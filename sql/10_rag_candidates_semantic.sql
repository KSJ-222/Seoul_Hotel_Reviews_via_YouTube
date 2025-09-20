-- sql/10_rag_candidates_semantic.sql
-- Purpose: TVF for semantic retrieval over review_points_embeddings with user filters; returns ranked candidates.
-- Inputs:  <PROJECT_ID>.youtube_proc.review_points_embeddings, <PROJECT_ID>.youtube_raw.videos, <PROJECT_ID>.youtube_raw.subtitles_full
-- Outputs: <PROJECT_ID>.youtube_proc.rag_candidates_semantic(...) (table-valued function)
-- Deps:    BigQuery remote model <PROJECT_ID>.youtube_proc.text_multi_emb; optional vector index on review_points_embeddings(embedding)
-- Run:     Invoke-BqSqlFile .\sql\10_rag_candidates_semantic.sql
-- Notes:   Coalesces NULL params to safe defaults; uses 4-arg VECTOR_SEARCH; LLM hard filter (specific-hotel OR general relevance); VS pool top_k=80.

CREATE OR REPLACE TABLE FUNCTION `<PROJECT_ID>`.youtube_proc.rag_candidates_semantic(
  q_text STRING,
  lang_filter STRING,   -- 'ALL' | 'en' | 'ko' | NULL→ALL
  exclude_paid_ads BOOL,
  min_views INT64,      -- NULL→0
  min_subs INT64,       -- NULL→0
  top_k INT64           -- NULL→5
)
RETURNS TABLE<
  video_id STRING,
  channel_title STRING,
  video_title STRING,
  hotel_norm STRING,
  aspect STRING,
  sentiment STRING,
  review_summary STRING,
  yt_link STRING,
  evidence_sec INT64,
  sim FLOAT64,
  pop FLOAT64,
  score FLOAT64
>
AS (
  WITH defaults AS (
    SELECT 5 AS def_top_k, 0 AS def_min_views, 0 AS def_min_subs
  ),
  q AS (
    SELECT ml_generate_embedding_result AS qvec,
           COALESCE(top_k,     (SELECT def_top_k     FROM defaults)) AS K,
           COALESCE(min_views, (SELECT def_min_views FROM defaults)) AS MV,
           COALESCE(min_subs,  (SELECT def_min_subs  FROM defaults)) AS MS,
           COALESCE(lang_filter, 'ALL') AS LF,
           COALESCE(exclude_paid_ads, FALSE) AS EPA
    FROM ML.GENERATE_EMBEDDING(
      MODEL `<PROJECT_ID>`.youtube_proc.text_multi_emb,
      (SELECT q_text AS content)
    )
  ),
  -- Vector search pool (top 80) filtered by language
  vs AS (
    SELECT
      r.base.video_id,
      r.base.lang,
      r.base.hotel_norm,
      r.base.brand,
      r.base.city,
      r.base.country,
      r.base.aspect,
      r.base.sentiment,
      r.base.review_summary,
      r.base.evidence_sec,
      r.base.video_title,
      r.base.channel_title,
      r.base.view_count,
      r.base.channel_subs,
      r.distance
    FROM q,
    VECTOR_SEARCH(
      (
        SELECT
          embedding, video_id, lang, default_lang,
          hotel_norm, brand, city, country,
          aspect, sentiment, review_summary,
          evidence_sec, video_title, channel_title,
          view_count, channel_subs
        FROM `<PROJECT_ID>`.youtube_proc.review_points_embeddings
        WHERE (
          (SELECT LF FROM q) = 'ALL'
          OR LOWER(SPLIT(COALESCE(default_lang, ''), '-')[OFFSET(0)]) = LOWER((SELECT LF FROM q))
        )
      ),
      'embedding',
      (SELECT 1 AS qid, q.qvec AS embedding FROM q),
      'embedding',
      top_k => 80,
      distance_type => 'COSINE'
    ) AS r
  ),

  -- Paid-ad flag from curated label table
  ad_flags AS (
    SELECT
      v.video_id,
      LOGICAL_OR(COALESCE(s.is_paid_ad_pred, FALSE)) AS is_ad
    FROM `<PROJECT_ID>`.youtube_raw.videos v
    LEFT JOIN `<PROJECT_ID>`.youtube_proc.sponsored_label_meta s USING (video_id)
    GROUP BY v.video_id
  ),

  -- LLM hard filter (generalized):
  -- - If QUESTION names a specific hotel/brand: TRUE only when PASSAGE is the same property/brand (minor name variations allowed)
  -- - If QUESTION does NOT name a specific hotel/brand: TRUE when PASSAGE meaningfully answers the asked aspect(s) for a Seoul hotel
  llm_checked AS (
    SELECT
      v.*,
      COALESCE(
        AI.GENERATE_BOOL(
          CONCAT(
            'Strictly return true or false.', '\n',
            'Task: Decide if PASSAGE is an appropriate answer to QUESTION for a Seoul-hotel reviews RAG.', '\n',
            '- If QUESTION names a specific hotel/brand, return true ONLY if PASSAGE clearly refers to the SAME property/brand ',
            '(minor name variations like "Four Seasons Seoul" vs "Four Seasons Hotel Seoul" count as same).', '\n',
            '- If QUESTION does NOT name a specific hotel/brand, return true if PASSAGE meaningfully addresses the requested aspect(s) ',
            '(e.g., views, staff friendliness, breakfast, location, etc.) for a Seoul hotel.', '\n',
            '- Return false if PASSAGE is about a different property when a specific one was asked, or if it is off-topic.', '\n\n',
            'QUESTION: ', COALESCE(q_text,''), '\n\n',
            'PASSAGE FIELDS:', '\n',
            '  hotel_norm=', COALESCE(v.hotel_norm,''), '\n',
            '  brand=',      COALESCE(v.brand,''), '\n',
            '  city=',       COALESCE(v.city,''),  '\n',
            '  country=',    COALESCE(v.country,''), '\n',
            '  aspect=',     COALESCE(v.aspect,''), '\n',
            '  review_summary=', COALESCE(v.review_summary,''), '\n',
            '  video_title=', COALESCE(v.video_title,'')
          ),
          connection_id => '<PROJECT_ID>.asia-northeast3.llm_conn',
          endpoint      => 'gemini-2.5-flash',
          model_params  => JSON '{"generation_config":{"temperature":0,"candidate_count":1}}'
        ).result,
        FALSE
      ) AS is_ok
    FROM vs AS v
  ),

  scored AS (
    SELECT
      c.video_id, c.channel_title, c.video_title, c.hotel_norm, c.aspect, c.sentiment,
      c.review_summary, c.evidence_sec,
      (1.0 - c.distance) AS sim,
      (LOG10(1 + COALESCE(c.view_count,0)) + LOG10(1 + COALESCE(c.channel_subs,0))) AS pop,
      (1.0 - c.distance)
        + 0.15 * (LOG10(1 + COALESCE(c.view_count,0)) + LOG10(1 + COALESCE(c.channel_subs,0))) AS score,
      c.is_ok,
      a.is_ad
    FROM llm_checked AS c
    LEFT JOIN ad_flags AS a USING (video_id)
    WHERE COALESCE(c.view_count,0)   >= (SELECT MV FROM q)
      AND COALESCE(c.channel_subs,0) >= (SELECT MS FROM q)
      AND (NOT (SELECT EPA FROM q) OR NOT COALESCE(a.is_ad, FALSE))
  )

  SELECT
    video_id, channel_title, video_title, hotel_norm, aspect, sentiment,
    review_summary,
    CONCAT('https://youtu.be/', video_id,
           CASE WHEN evidence_sec IS NULL THEN '' ELSE CONCAT('?t=', CAST(evidence_sec AS STRING), 's') END) AS yt_link,
    evidence_sec, sim, pop, score
  FROM scored
  QUALIFY ROW_NUMBER() OVER (ORDER BY is_ok DESC, score DESC) <= (SELECT K FROM q)
);
