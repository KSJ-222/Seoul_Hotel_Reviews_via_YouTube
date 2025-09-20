-- sql/04_subtitles_to_chunks.sql
-- Purpose: Build 30s transcript chunks (15s stride) per video/lang from subtitle segments with basic video/channel metadata.
-- Inputs:  <PROJECT_ID>.youtube_raw.subtitles_segments, <PROJECT_ID>.youtube_raw.videos, <PROJECT_ID>.youtube_raw.channels
-- Outputs: <PROJECT_ID>.youtube_proc.chunks
-- Deps:    BigQuery (no external connections)
-- Run:     Invoke-BqSqlFile .\sql\04_subtitles_to_chunks.sql
-- Notes:   Incremental & idempotent. Skips (video_id, lang) pairs already chunked.

CREATE SCHEMA IF NOT EXISTS `<PROJECT_ID>.youtube_proc`;

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_proc.chunks` (
  channel_id    STRING,
  channel_title STRING,
  video_id      STRING,
  title         STRING,
  lang          STRING,
  chunk_id      INT64,
  start_sec     INT64,
  end_sec       INT64,
  chunk_text    STRING,
  view_count    INT64,
  channel_subs  INT64
);

INSERT INTO `<PROJECT_ID>.youtube_proc.chunks`
  (channel_id, channel_title, video_id, title, lang,
   chunk_id, start_sec, end_sec, chunk_text,
   view_count, channel_subs)
WITH base AS (
  SELECT
    v.video_id,
    v.channel_id,
    c.channel_title,
    v.title,
    v.duration_sec,
    v.view_count,
    c.channel_subs
  FROM `<PROJECT_ID>.youtube_raw.videos`   AS v
  JOIN `<PROJECT_ID>.youtube_raw.channels` AS c
    ON v.channel_id = c.channel_id
),
langs AS (
  SELECT DISTINCT video_id, lang
  FROM `<PROJECT_ID>.youtube_raw.subtitles_segments`
),
pending AS (
  -- Skip (video_id, lang) that already exist in chunks
  SELECT l.video_id, l.lang
  FROM langs AS l
  LEFT JOIN `<PROJECT_ID>.youtube_proc.chunks` AS ch
    ON ch.video_id = l.video_id
   AND ch.lang     = l.lang
  WHERE ch.video_id IS NULL
),
grid AS (
  -- Sliding windows: 30s window, 15s stride
  SELECT
    b.video_id,
    p.lang,
    s       AS start_sec,
    s + 30  AS end_sec
  FROM base AS b
  JOIN pending AS p USING (video_id)
  JOIN UNNEST(GENERATE_ARRAY(0, CAST(FLOOR(b.duration_sec) AS INT64), 15)) AS s
),
joined AS (
  -- Collect overlapping subtitle lines into each window
  SELECT
    g.video_id,
    g.lang,
    g.start_sec,
    g.end_sec,
    STRING_AGG(s.text, ' ' ORDER BY s.start_sec) AS chunk_text
  FROM grid AS g
  JOIN `<PROJECT_ID>.youtube_raw.subtitles_segments` AS s
    ON s.video_id = g.video_id
   AND s.lang     = g.lang
   AND s.start_sec < g.end_sec
   AND (s.start_sec + s.dur_sec) > g.start_sec
  GROUP BY 1,2,3,4
),
rows_to_insert AS (
  SELECT
    b.channel_id,
    b.channel_title,
    j.video_id,
    v.title,
    j.lang,
    ROW_NUMBER() OVER (PARTITION BY j.video_id, j.lang ORDER BY j.start_sec) AS chunk_id,
    j.start_sec,
    j.end_sec,
    j.chunk_text,
    b.view_count,
    b.channel_subs
  FROM joined AS j
  JOIN base  AS b USING (video_id)
  JOIN `<PROJECT_ID>.youtube_raw.videos` AS v USING (video_id)
)
SELECT
  channel_id, channel_title, video_id, title, lang,
  chunk_id, start_sec, end_sec, chunk_text,
  view_count, channel_subs
FROM rows_to_insert;
