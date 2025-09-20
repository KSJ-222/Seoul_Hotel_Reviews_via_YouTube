-- sql/07_create_chunk_vector_index.sql
-- Purpose: Create a COSINE IVF vector index on chunk embeddings for faster ANN queries.
-- Inputs:  <PROJECT_ID>.youtube_proc.chunk_embeddings
-- Outputs: <PROJECT_ID>.youtube_proc.vidx_chunk_embeddings (vector index on embedding)
-- Deps:    BigQuery VECTOR INDEX feature; table and index must be in the same region
-- Run:     Invoke-BqSqlFile .\sql\07_create_chunk_vector_index.sql
-- Notes:   Requires sufficient rows in the source table; drop & recreate if schema changes.

CREATE OR REPLACE VECTOR INDEX `<PROJECT_ID>.youtube_proc.vidx_chunk_vector`
ON `<PROJECT_ID>.youtube_proc.chunk_embeddings` (embedding)
STORING (
  video_id,
  lang,
  chunk_id,
  start_sec,
  end_sec,
  chunk_text
)
OPTIONS (
  distance_type = 'COSINE',
  index_type    = 'IVF'
);
