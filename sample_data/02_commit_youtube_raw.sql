-- sample_data/02_commit_youtube_raw.sql
-- Purpose: Commit staged JSONL rows into final youtube_raw tables (cast/reshape + dedupe), then drop staging tables.
-- Inputs:  <PROJECT_ID>.youtube_stage.{channels_stage,videos_stage,subtitles_segments_stage,subtitles_full_stage}
-- Outputs: <PROJECT_ID>.youtube_raw.{channels,videos,subtitles_segments,subtitles_full}
-- Deps:    Datasets/tables prepared by 01_prepare_youtube_raw.sql; BigQuery permissions to TRUNCATE/INSERT/DROP.
-- Run:     Invoke-BqSqlFile .\sample_data\02_commit_youtube_raw.sql
-- Notes:   Safe to re-run (final tables are truncated). No QUALIFY; use WHERE rn = 1. <PROJECT_ID> is replaced by $env:GCP_PROJECT_ID.


-- 0) Initialization
TRUNCATE TABLE `<PROJECT_ID>.youtube_raw.channels`;
TRUNCATE TABLE `<PROJECT_ID>.youtube_raw.videos`;
TRUNCATE TABLE `<PROJECT_ID>.youtube_raw.subtitles_segments`;
TRUNCATE TABLE `<PROJECT_ID>.youtube_raw.subtitles_full`;

-- Helper: RFC3339 → TIMESTAMP (fallback to SAFE_CAST)
CREATE TEMP FUNCTION PARSE_RFC3339(s STRING) AS (
  COALESCE(
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', s),
    SAFE_CAST(s AS TIMESTAMP)
  )
);

-- 1) channels
INSERT INTO `<PROJECT_ID>.youtube_raw.channels` (
  channel_id, channel_title, channel_subs, country, uploads_playlist, created_at
)
WITH c AS (
  SELECT
    channel_id,
    channel_title,
    channel_subs,
    country,
    uploads_playlist,
    PARSE_RFC3339(NULLIF(created_at, '')) AS created_at_ts,
    ROW_NUMBER() OVER (
      PARTITION BY channel_id
      ORDER BY PARSE_RFC3339(NULLIF(created_at, '')) DESC
    ) AS rn
  FROM `<PROJECT_ID>.youtube_stage.channels_stage`
)
SELECT
  channel_id,
  channel_title,
  SAFE_CAST(NULLIF(channel_subs, '') AS INT64) AS channel_subs,
  country,
  uploads_playlist,
  COALESCE(created_at_ts, CURRENT_TIMESTAMP()) AS created_at
FROM c
WHERE rn = 1;

-- 2) videos
INSERT INTO `<PROJECT_ID>.youtube_raw.videos` (
  video_id, channel_id, title, description, published_at,
  view_count, like_count, tags, default_lang, duration_sec
)
WITH v AS (
  SELECT
    video_id,
    channel_id,
    title,
    description,
    PARSE_RFC3339(NULLIF(published_at, '')) AS published_at_ts,
    SAFE_CAST(NULLIF(view_count, '') AS INT64) AS view_count_i,
    SAFE_CAST(NULLIF(like_count, '') AS INT64) AS like_count_i,
    tags,
    NULLIF(default_lang, '') AS default_lang_s,
    SAFE_CAST(NULLIF(duration_sec, '') AS FLOAT64) AS duration_f,
    ROW_NUMBER() OVER (
      PARTITION BY video_id
      ORDER BY PARSE_RFC3339(NULLIF(published_at, '')) DESC,
               SAFE_CAST(NULLIF(view_count,'') AS INT64) DESC
    ) AS rn
  FROM `<PROJECT_ID>.youtube_stage.videos_stage`
)
SELECT
  video_id,
  channel_id,
  title,
  description,
  published_at_ts AS published_at,
  view_count_i     AS view_count,
  like_count_i     AS like_count,
  tags,
  default_lang_s   AS default_lang,
  duration_f       AS duration_sec
FROM v
WHERE rn = 1;

-- 3) subtitles_segments
INSERT INTO `<PROJECT_ID>.youtube_raw.subtitles_segments` (
  video_id, lang, idx, start_sec, dur_sec, text
)
SELECT
  video_id,
  NULLIF(lang, '') AS lang,
  SAFE_CAST(NULLIF(idx, '') AS INT64) AS idx,
  SAFE_CAST(NULLIF(start_sec, '') AS FLOAT64) AS start_sec,
  SAFE_CAST(NULLIF(dur_sec, '') AS FLOAT64) AS dur_sec,
  text
FROM `<PROJECT_ID>.youtube_stage.subtitles_segments_stage`
WHERE video_id IS NOT NULL;

-- 4) subtitles_full (pick longest full_text per video_id,lang)
INSERT INTO `<PROJECT_ID>.youtube_raw.subtitles_full` (
  video_id, lang, full_text
)
WITH f AS (
  SELECT
    video_id,
    NULLIF(lang, '') AS lang,
    full_text,
    ROW_NUMBER() OVER (
      PARTITION BY video_id, NULLIF(lang,'')
      ORDER BY LENGTH(COALESCE(full_text, '')) DESC
    ) AS rn
  FROM `<PROJECT_ID>.youtube_stage.subtitles_full_stage`
)
SELECT
  video_id,
  lang,
  full_text
FROM f
WHERE rn = 1;

-- 5) cleanup staging
DROP TABLE IF EXISTS `<PROJECT_ID>.youtube_stage.channels_stage`;
DROP TABLE IF EXISTS `<PROJECT_ID>.youtube_stage.videos_stage`;
DROP TABLE IF EXISTS `<PROJECT_ID>.youtube_stage.subtitles_segments_stage`;
DROP TABLE IF EXISTS `<PROJECT_ID>.youtube_stage.subtitles_full_stage`;

-- 6) sanity check
SELECT 'channels'  AS table, COUNT(*) AS row_count FROM `<PROJECT_ID>.youtube_raw.channels`
UNION ALL SELECT 'videos',   COUNT(*) FROM `<PROJECT_ID>.youtube_raw.videos`
UNION ALL SELECT 'segments', COUNT(*) FROM `<PROJECT_ID>.youtube_raw.subtitles_segments`
UNION ALL SELECT 'full',     COUNT(*) FROM `<PROJECT_ID>.youtube_raw.subtitles_full`;
