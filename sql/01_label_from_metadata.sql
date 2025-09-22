-- sql/01_label_from_metadata.sql
-- Purpose: Label each video with (A) hotel-related content and (B) sponsored/paid review using only metadata (title/description/tags).
-- Inputs:  <PROJECT_ID>.youtube_raw.videos
-- Outputs: Create hotel_label_meta, sponsored_label_meta; Upserts into <PROJECT_ID>.youtube_proc.hotel_label_meta and <PROJECT_ID>.youtube_proc.sponsored_label_meta; updates judged_at on re-label.
-- Deps:    BigQuery AI.GENERATE_BOOL; LLM connection <PROJECT_ID>.asia-northeast3.llm_conn; datasets/tables from 00_schema.sql.
-- Run:     Invoke-BqSqlFile .\sql\01_label_from_metadata.sql
-- Notes:   Idempotent; skips AI calls for already-labeled videos via NOT EXISTS filters; conservative sponsored policy.

-- Datasets
CREATE SCHEMA IF NOT EXISTS `<PROJECT_ID>.youtube_proc` OPTIONS (location = "asia-northeast3");

-- Labels inferred from metadata
CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_proc.hotel_label_meta` (
  video_id STRING,
  is_hotel_pred BOOL,
  judged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_proc.sponsored_label_meta` (
  video_id STRING,
  is_paid_ad_pred BOOL,
  judged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 1) Hotel label (metadata only: title/description/tags)
MERGE `<PROJECT_ID>.youtube_proc.hotel_label_meta` AS T
USING (
  SELECT
    v.video_id,
    COALESCE(
      AI.GENERATE_BOOL(
        CONCAT(
          'Decide if this YouTube video is primarily about a hotel (review/tour/stay). ',
          'Answer strictly true or false. True only if clearly hotel-focused.',
          '\nTitle: ', IFNULL(v.title, ''),
          '\nDescription: ', IFNULL(v.description, ''),
          '\nTags: ', IFNULL(TO_JSON_STRING(v.tags), '[]')
        ),
        connection_id => '<PROJECT_ID>.asia-northeast3.llm_conn',
        endpoint      => 'gemini-2.5-flash'
      ).result,
      FALSE
    ) AS is_hotel_pred
  FROM `<PROJECT_ID>.youtube_raw.videos` AS v
  WHERE NOT EXISTS (
    SELECT 1
    FROM `<PROJECT_ID>.youtube_proc.hotel_label_meta` AS h
    WHERE h.video_id = v.video_id
      AND h.is_hotel_pred IS NOT NULL
  )
) AS S
ON T.video_id = S.video_id
WHEN MATCHED THEN UPDATE SET
  T.is_hotel_pred = S.is_hotel_pred,
  T.judged_at     = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (video_id, is_hotel_pred)
VALUES (S.video_id, S.is_hotel_pred);

-- 2) Sponsored/paid label (metadata only: title/description/tags)
MERGE `<PROJECT_ID>.youtube_proc.sponsored_label_meta` AS T
USING (
  SELECT
    v.video_id,
    COALESCE(
      AI.GENERATE_BOOL(
        CONCAT(
          'Decide if this YouTube video is a paid/sponsored/advertisement review based only on its metadata. ',
          'Return true only if sponsorship is clearly implied (e.g., "sponsored", "ad", "thanks to X for sponsoring", ',
          '"provided by", affiliate disclaimer, or local-language equivalents). Be conservative to avoid false positives.',
          '\nTitle: ', IFNULL(v.title, ''),
          '\nDescription: ', IFNULL(v.description, ''),
          '\nTags: ', IFNULL(TO_JSON_STRING(v.tags), '[]')
        ),
        connection_id => '<PROJECT_ID>.asia-northeast3.llm_conn',
        endpoint      => 'gemini-2.5-flash'
      ).result,
      FALSE
    ) AS is_paid_ad_pred
  FROM `<PROJECT_ID>.youtube_raw.videos` AS v
  WHERE NOT EXISTS (
    SELECT 1
    FROM `<PROJECT_ID>.youtube_proc.sponsored_label_meta` AS s
    WHERE s.video_id = v.video_id
      AND s.is_paid_ad_pred IS NOT NULL
  )
) AS S
ON T.video_id = S.video_id
WHEN MATCHED THEN UPDATE SET
  T.is_paid_ad_pred = S.is_paid_ad_pred,
  T.judged_at       = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (video_id, is_paid_ad_pred)
VALUES (S.video_id, S.is_paid_ad_pred);
