-- sql/05_create_remote_embedding_model.sql
-- Purpose: Create a multilingual text-embedding remote model for use in BigQuery (Korean/English supported).
-- Inputs:  (none)
-- Outputs: <PROJECT_ID>.youtube_proc.text_multi_emb (remote model)
-- Deps:    BigQuery connection <PROJECT_ID>.asia-northeast3.llm_conn; endpoint text-multilingual-embedding-002
-- Run:     Invoke-BqSqlFile .\sql\05_create_remote_embedding_model.sql
-- Notes:   Idempotent (CREATE OR REPLACE). Keep connection & dataset in asia-northeast3.

CREATE OR REPLACE MODEL `<PROJECT_ID>.youtube_proc.text_multi_emb`
REMOTE WITH CONNECTION `<PROJECT_ID>.asia-northeast3.llm_conn`
OPTIONS (
  ENDPOINT = 'text-multilingual-embedding-002'
);
