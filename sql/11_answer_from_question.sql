-- sql/11_answer_from_question.sql
-- Purpose: Return a concise summary + 4-column citation table for a user question using rag_candidates_semantic with filters.
-- Inputs:  <PROJECT_ID>.youtube_proc.rag_candidates_semantic (TVF)
-- Outputs: Stored procedure <PROJECT_ID>.youtube_proc.answer_from_question_auto that returns a single-row 'markdown' column
-- Deps:    BigQuery AI connection <PROJECT_ID>.asia-northeast3.llm_conn; endpoint gemini-2.5-flash
-- Run:     Invoke-BqSqlFile .\sql\11_answer_from_question.sql
-- Notes:   CREATE OR REPLACE PROCEDURE; emits text in the question’s language.

CREATE OR REPLACE PROCEDURE `<PROJECT_ID>`.youtube_proc.answer_from_question_auto(
  q_text STRING,
  lang_filter STRING,
  exclude_paid_ads BOOL,
  min_views INT64,   -- allow NULL
  min_subs INT64,    -- allow NULL
  top_k INT64        -- allow NULL
)
BEGIN
  DECLARE NL STRING DEFAULT CHR(10);
  DECLARE _cnt INT64 DEFAULT 0;
  DECLARE _bullets STRING;
  DECLARE summary STRING;

  CREATE TEMP TABLE _cand AS
  SELECT * FROM `<PROJECT_ID>`.youtube_proc.rag_candidates_semantic(
    q_text, lang_filter, exclude_paid_ads, min_views, min_subs, top_k
  );

  SET _cnt = (SELECT COUNT(*) FROM _cand);
  IF _cnt = 0 THEN
    SELECT CONCAT(
      'Summary', NL,
      'No review candidates matched your question or filters.', NL, NL,
      'Citation', NL,
      '| Review | Link | Video title | Channel |', NL,
      '|---|---|---|---|', NL
    ) AS markdown;
    RETURN;
  END IF;

  CREATE TEMP TABLE _rows AS
  SELECT
    channel_title,
    video_title,
    hotel_norm,
    aspect,
    sentiment,
    review_summary,
    yt_link,
    score
  FROM _cand
  ORDER BY score DESC;

  SET _bullets = (
    SELECT STRING_AGG(CONCAT('- ', hotel_norm, ' — ', aspect, ' (', sentiment, '): ', review_summary), NL)
    FROM _rows
  );

  SET summary = (
    SELECT (
      AI.GENERATE(
        CONCAT(
          'Answer the user\'s question in the SAME language as the question (detect automatically). ',
          'Write 1–2 concise sentences, focusing on key takeaways. ',
          'Do not fabricate; use only the bullets below.', NL,
          'Question: ', q_text, NL, NL,
          'Bullets:', NL, COALESCE(_bullets, '')
        ),
        connection_id => '<PROJECT_ID>.asia-northeast3.llm_conn',
        endpoint      => 'gemini-2.5-flash',
        model_params  => JSON '{"generation_config":{"temperature":0}}'
      )
    ).result
  );

  SELECT CONCAT(
    'Summary', NL, COALESCE(summary, ''), NL, NL,
    'Citation', NL,
    '| Review | Link | Video title | Channel |', NL,
    '|---|---|---|---|', NL,
    (
      SELECT STRING_AGG(
        CONCAT('| ', hotel_norm, ' — ', aspect, ': ', review_summary,
               ' | ', yt_link,
               ' | ', video_title,
               ' | ', channel_title, ' |'),
        NL
      ) FROM _rows
    ),
    NL
  ) AS markdown;
END;
