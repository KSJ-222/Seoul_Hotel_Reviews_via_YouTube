-- sql/06_embed_chunks.sql
-- Purpose: Embed new transcript chunks and append to youtube_proc.chunk_embeddings.
-- Inputs:  <PROJECT_ID>.youtube_proc.chunks, <PROJECT_ID>.youtube_proc.text_multi_emb (remote model)
-- Outputs: <PROJECT_ID>.youtube_proc.chunk_embeddings
-- Deps:    BigQuery remote model <PROJECT_ID>.youtube_proc.text_multi_emb (text-multilingual-embedding-002)
-- Run:     Invoke-BqSqlFile .\sql\06_embed_chunks.sql
-- Notes:   Incremental & idempotent. Inserts only chunks not yet embedded.

CREATE SCHEMA IF NOT EXISTS `<PROJECT_ID>.youtube_proc`;

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_proc.chunk_embeddings` (
  video_id    STRING,
  lang        STRING,
  chunk_id    INT64,
  start_sec   INT64,
  end_sec     INT64,
  chunk_text  STRING,
  embedding   ARRAY<FLOAT64>,
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO `<PROJECT_ID>.youtube_proc.chunk_embeddings`
  (video_id, lang, chunk_id, start_sec, end_sec, chunk_text, embedding)
WITH pending AS (
  SELECT
    c.video_id, c.lang, c.chunk_id, c.start_sec, c.end_sec, c.chunk_text
  FROM `<PROJECT_ID>.youtube_proc.chunks` AS c
  LEFT JOIN `<PROJECT_ID>.youtube_proc.chunk_embeddings` AS e
    ON e.video_id = c.video_id
   AND e.lang     = c.lang
   AND e.chunk_id = c.chunk_id
  WHERE e.video_id IS NULL
    AND c.chunk_text IS NOT NULL
    AND LENGTH(c.chunk_text) > 0
)
SELECT
  video_id,
  lang,
  chunk_id,
  start_sec,
  end_sec,
  chunk_text,
  ml_generate_embedding_result AS embedding
FROM ML.GENERATE_EMBEDDING(
  MODEL `<PROJECT_ID>.youtube_proc.text_multi_emb`,
  (
    SELECT
      chunk_text AS content,  -- required input column
      video_id, lang, chunk_id, start_sec, end_sec, chunk_text
    FROM pending
  )
);
