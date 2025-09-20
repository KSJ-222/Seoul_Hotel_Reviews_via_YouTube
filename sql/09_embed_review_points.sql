-- sql/09_embed_review_points.sql
-- Purpose: Generate embeddings for finalized review points and append to review_points_embeddings; (TODO) optionally create a vector index when row count ≥ 5000.
-- Inputs:  <PROJECT_ID>.youtube_proc.video_review_points, <PROJECT_ID>.youtube_raw.videos, <PROJECT_ID>.youtube_raw.channels
-- Outputs: <PROJECT_ID>.youtube_proc.review_points_embeddings; optional <PROJECT_ID>.youtube_proc.vidx_review_points_embeddings
-- Deps:    BigQuery remote model <PROJECT_ID>.youtube_proc.text_multi_emb
-- Run:     Invoke-BqSqlFile .\sql\09_embed_review_points.sql
-- Notes:   Incremental insert; conditional index creation.

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>`.youtube_proc.review_points_embeddings (
  video_id STRING,
  lang STRING,
  default_lang STRING,
  hotel_norm STRING,
  brand STRING,
  city STRING,
  country STRING,
  aspect STRING,
  sentiment STRING,
  review_summary STRING,
  evidence_quote STRING,
  evidence_sec INT64,
  video_title STRING,
  channel_title STRING,
  view_count INT64,
  channel_subs INT64,
  combined_text STRING,
  embedding ARRAY<FLOAT64>,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO `<PROJECT_ID>`.youtube_proc.review_points_embeddings (
  video_id, lang, default_lang, hotel_norm, brand, city, country, aspect, sentiment,
  review_summary, evidence_quote, evidence_sec, video_title, channel_title,
  view_count, channel_subs, combined_text, embedding
)
WITH src AS (
  SELECT
    r.video_id, r.lang, v.default_lang, r.hotel_norm, r.brand, r.city, r.country, r.aspect, r.sentiment,
    r.review_summary, r.evidence_quote, r.evidence_sec,
    v.title AS video_title, c.channel_title,
    v.view_count, c.channel_subs
  FROM `<PROJECT_ID>`.youtube_proc.video_review_points r
  JOIN `<PROJECT_ID>`.youtube_raw.videos v USING (video_id)
  JOIN `<PROJECT_ID>`.youtube_raw.channels c ON v.channel_id = c.channel_id
),
pending AS (
  SELECT s.*
  FROM src s
  LEFT JOIN `<PROJECT_ID>`.youtube_proc.review_points_embeddings e
    ON e.video_id = s.video_id AND e.lang = s.lang
   AND e.hotel_norm = s.hotel_norm AND e.aspect = s.aspect
   AND e.evidence_quote = s.evidence_quote
  WHERE e.video_id IS NULL
),
to_embed AS (
  SELECT
    CONCAT('[Hotel] ', COALESCE(hotel_norm,''),
           ' [City] ', COALESCE(city,''),
           ' [Aspect] ', COALESCE(aspect,''), ': ', COALESCE(review_summary,''),
           ' | Quote: ', COALESCE(evidence_quote,''),
           ' | Title: ', COALESCE(video_title,'')) AS content,
    *
  FROM pending
)
SELECT
  video_id, lang, default_lang, hotel_norm, brand, city, country, aspect, sentiment,
  review_summary, evidence_quote, evidence_sec, video_title, channel_title,
  view_count, channel_subs,
  content AS combined_text,
  ml_generate_embedding_result AS embedding
FROM ML.GENERATE_EMBEDDING(
  MODEL `<PROJECT_ID>`.youtube_proc.text_multi_emb,
  (
    SELECT content, video_id, lang, default_lang, hotel_norm, brand, city, country, aspect, sentiment,
           review_summary, evidence_quote, evidence_sec, video_title, channel_title,
           view_count, channel_subs
    FROM to_embed
  )
);