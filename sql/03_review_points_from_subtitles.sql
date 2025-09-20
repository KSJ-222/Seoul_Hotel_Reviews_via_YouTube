-- sql/03_review_points_from_subtitles.sql
-- Purpose: From full subtitles, extract structured hotel review points via LLM (JSON) and parse into rows.
-- Inputs:  <PROJECT_ID>.youtube_raw.subtitles_full, <PROJECT_ID>.youtube_raw.videos
-- Outputs: <PROJECT_ID>.youtube_proc.review_json (LLM raw JSON), <PROJECT_ID>.youtube_proc.video_review_points_raw (parsed)
-- Deps:    BigQuery AI connection <PROJECT_ID>.asia-northeast3.llm_conn; endpoint gemini-2.5-flash
-- Run:     Invoke-BqSqlFile .\sql\03_review_points_from_subtitles.sql
-- Notes:   Incremental & idempotent. Skips videos already parsed.

-- Toggles
DECLARE _TEST BOOL DEFAULT FALSE;                 -- TRUE: process only a small sample
DECLARE _N    INT64 DEFAULT 5;                    -- sample size if _TEST = TRUE
DECLARE _LANGS ARRAY<STRING> DEFAULT ['en','ko']; -- [] disables language filter
DECLARE _VIDEO_IDS ARRAY<STRING> DEFAULT [];      -- [] disables explicit video filter

-- Ensure target tables exist
CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_proc.review_json` (
  video_id   STRING,
  lang       STRING,
  json_text  STRING,  -- AI.GENERATE().result (raw JSON)
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_proc.video_review_points_raw` (
  video_id       STRING,
  lang           STRING,
  hotel_norm     STRING,
  brand          STRING,
  city           STRING,
  country        STRING,
  aspect         STRING,
  sentiment      STRING,
  review_summary STRING,
  evidence_quote STRING,
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Source candidates from subtitles_full (apply filters and optional sampling)
CREATE TEMP TABLE _src AS
SELECT
  v.video_id,
  sf.lang,
  COALESCE(v.title, '') AS title,
  COALESCE(v.description, '') AS description,
  COALESCE(ARRAY_TO_STRING(v.tags, ', '), '') AS tags_str,
  sf.full_text,
  COALESCE(v.view_count, 0) AS view_count,
  COALESCE(v.published_at, TIMESTAMP '1970-01-01') AS published_at
FROM `<PROJECT_ID>.youtube_raw.subtitles_full` AS sf
JOIN `<PROJECT_ID>.youtube_raw.videos` AS v USING (video_id)
WHERE sf.full_text IS NOT NULL
  AND LENGTH(sf.full_text) > 50
  AND (ARRAY_LENGTH(_LANGS) = 0 OR sf.lang IN UNNEST(_LANGS))
  AND (ARRAY_LENGTH(_VIDEO_IDS) = 0 OR v.video_id IN UNNEST(_VIDEO_IDS))
QUALIFY CASE WHEN _TEST
  THEN ROW_NUMBER() OVER (ORDER BY view_count DESC, published_at DESC) <= _N
  ELSE TRUE
END;

-- Pending keys to process (skip if already parsed)
CREATE TEMP TABLE _pending AS
SELECT s.video_id, s.lang, s.title, s.description, s.tags_str, s.full_text
FROM _src AS s
LEFT JOIN `<PROJECT_ID>.youtube_proc.video_review_points_raw` AS vr
  ON vr.video_id = s.video_id AND vr.lang = s.lang
WHERE vr.video_id IS NULL;

-- LLM calls only for missing review_json rows
INSERT INTO `<PROJECT_ID>.youtube_proc.review_json` (video_id, lang, json_text)
SELECT
  p.video_id,
  p.lang,
  (
    AI.GENERATE(
      CONCAT(
        'You are extracting structured HOTEL REVIEWS from a YouTube video transcript.',
        ' Return STRICT JSON exactly as:',
        ' {"hotels":[{"hotel_norm":string,"brand":string,"city":string,"country":string,',
        '  "reviews":[{"aspect":string,"sentiment":"positive|negative|mixed","summary":string,"evidence_quote":string}]}]}',
        '\nRules:',
        '\n- evidence_quote MUST be a literal 6 to 20 word snippet that actually EXISTS in the transcript.',
        '\n- Output ONLY JSON. No markdown, no code fences.',
        '\n- If none, return {"hotels":[]}.',
        '\n\nVIDEO META\nTitle: ', p.title,
        '\nDescription: ', p.description,
        '\nTags: ', p.tags_str,
        '\n\nFULL TRANSCRIPT (truncated to 100k chars):\n',
        SUBSTR(COALESCE(p.full_text, ''), 1, 100000)
      ),
      connection_id => '<PROJECT_ID>.asia-northeast3.llm_conn',
      endpoint      => 'gemini-2.5-flash',
      model_params  => JSON '{"generation_config":{"response_mime_type":"application/json","temperature":0,"candidate_count":1}}'
    )
  ).result AS json_text
FROM _pending AS p
LEFT JOIN `<PROJECT_ID>.youtube_proc.review_json` AS r
  ON r.video_id = p.video_id AND r.lang = p.lang
WHERE r.video_id IS NULL;

-- Parse JSON for pending keys (string JSON_EXTRACT_* only)
CREATE TEMP TABLE _to_parse AS
SELECT p.video_id, p.lang
FROM _pending AS p
JOIN `<PROJECT_ID>.youtube_proc.review_json` AS r
  ON r.video_id = p.video_id AND r.lang = p.lang;

CREATE TEMP TABLE _parsed AS
WITH src AS (
  SELECT r.video_id, r.lang, r.json_text
  FROM `<PROJECT_ID>.youtube_proc.review_json` AS r
  JOIN _to_parse USING (video_id, lang)
),
hotels AS (
  SELECT
    video_id,
    lang,
    JSON_EXTRACT_ARRAY(json_text, '$.hotels') AS hotels_arr
  FROM src
),
flat_h AS (
  SELECT
    video_id,
    lang,
    h_str
  FROM hotels, UNNEST(hotels_arr) AS h_str
  WHERE hotels_arr IS NOT NULL
),
flat_r AS (
  SELECT
    video_id,
    lang,
    JSON_EXTRACT_SCALAR(h_str, '$.hotel_norm') AS hotel_norm,
    JSON_EXTRACT_SCALAR(h_str, '$.brand')      AS brand,
    JSON_EXTRACT_SCALAR(h_str, '$.city')       AS city,
    JSON_EXTRACT_SCALAR(h_str, '$.country')    AS country,
    JSON_EXTRACT_ARRAY(h_str, '$.reviews')     AS reviews_arr
  FROM flat_h
),
flat AS (
  SELECT
    video_id,
    lang,
    hotel_norm,
    brand,
    city,
    country,
    JSON_EXTRACT_SCALAR(r_str, '$.aspect')         AS aspect,
    JSON_EXTRACT_SCALAR(r_str, '$.sentiment')      AS sentiment,
    JSON_EXTRACT_SCALAR(r_str, '$.summary')        AS review_summary,
    JSON_EXTRACT_SCALAR(r_str, '$.evidence_quote') AS evidence_quote
  FROM flat_r, UNNEST(reviews_arr) AS r_str
  WHERE reviews_arr IS NOT NULL
)
SELECT *
FROM flat
WHERE COALESCE(hotel_norm, '') <> '';

-- Insert parsed rows (idempotent per run due to _pending filter)
INSERT INTO `<PROJECT_ID>.youtube_proc.video_review_points_raw`
  (video_id, lang, hotel_norm, brand, city, country, aspect, sentiment, review_summary, evidence_quote)
SELECT
  video_id, lang, hotel_norm, brand, city, country, aspect, sentiment, review_summary, evidence_quote
FROM _parsed;

-- Quick counts
SELECT
  (SELECT COUNT(*) FROM _pending) AS pending_keys,
  (SELECT COUNT(*) FROM _parsed)  AS inserted_rows;
