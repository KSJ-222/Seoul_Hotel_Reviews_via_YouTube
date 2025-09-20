-- sample_data/02_commit_youtube_raw.sql
-- Purpose: Commit staged JSONL rows into final youtube_raw tables (cast/reshape + dedupe) and optionally drop staging tables.
-- Inputs:  <PROJECT_ID>.youtube_stage.{channels_stage,videos_stage,subtitles_segments_stage,subtitles_full_stage}
-- Outputs: <PROJECT_ID>.youtube_raw.{channels,videos,subtitles_segments,subtitles_full}
-- Deps:    Datasets/tables prepared by 01_prepare_youtube_raw.sql; <PROJECT_ID> replaced by $env:GCP_PROJECT_ID at run time.
-- Run:     Invoke-BqSqlFile .\sample_data\02_commit_youtube_raw.sql
-- Notes:   Idempotent-ish: final tables are truncated before insert; best rows picked via ROW_NUMBER(); staging tables dropped at the end.

-- 0) Initialization (safe to re-run)
TRUNCATE TABLE `<PROJECT_ID>.youtube_raw.channels`;
TRUNCATE TABLE `<PROJECT_ID>.youtube_raw.videos`;
TRUNCATE TABLE `<PROJECT_ID>.youtube_raw.subtitles_segments`;
TRUNCATE TABLE `<PROJECT_ID>.youtube_raw.subtitles_full`;

-- 1) channels
INSERT INTO `<PROJECT_ID>.youtube_raw.channels`
  (channel_id, channel_title, channel_subs, country, uploads_playlist, created_at)
SELECT
  channel_id,
  channel_title,
  SAFE_CAST(channel_subs AS INT64),
  country,
  uploads_playlist,
  COALESCE(
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S %Z', created_at),
    SAFE.PARSE_TIMESTAMP('%FT%T%E*EZ', created_at),
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', created_at),
    CURRENT_TIMESTAMP()
  ) AS created_at
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY channel_id
           ORDER BY COALESCE(SAFE_CAST(channel_subs AS INT64), 0) DESC,
                    COALESCE(
                      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S %Z', created_at),
                      SAFE.PARSE_TIMESTAMP('%FT%T%E*EZ', created_at),
                      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', created_at),
                      TIMESTAMP '1970-01-01'
                    ) DESC
         ) AS rn
  FROM `<PROJECT_ID>.youtube_stage.channels_stage`
)
WHERE rn = 1;

-- 2) videos
INSERT INTO `<PROJECT_ID>.youtube_raw.videos`
  (video_id, channel_id, title, description, published_at, view_count, like_count, tags, default_lang, duration_sec)
SELECT
  video_id,
  channel_id,
  title,
  description,
  COALESCE(
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S %Z', published_at),
    SAFE.PARSE_TIMESTAMP('%FT%T%E*EZ', published_at),
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', published_at)
  ) AS published_at,
  SAFE_CAST(view_count AS INT64),
  SAFE_CAST(like_count AS INT64),
  tags,
  default_lang,
  SAFE_CAST(duration_sec AS FLOAT64)
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY video_id
           ORDER BY COALESCE(
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S %Z', published_at),
                    SAFE.PARSE_TIMESTAMP('%FT%T%E*EZ', published_at),
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', published_at),
                    TIMESTAMP '1970-01-01'
                  ) DESC
         ) AS rn
  FROM `<PROJECT_ID>.youtube_stage.videos_stage`
)
WHERE rn = 1;

-- 3) subtitles_segments
INSERT INTO `<PROJECT_ID>.youtube_raw.subtitles_segments`
  (video_id, lang, idx, start_sec, dur_sec, text)
SELECT
  video_id,
  lang,
  SAFE_CAST(idx AS INT64),
  SAFE_CAST(start_sec AS FLOAT64),
  SAFE_CAST(dur_sec  AS FLOAT64),
  text
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY video_id, lang, idx
           ORDER BY COALESCE(SAFE_CAST(start_sec AS FLOAT64), 0.0) ASC
         ) AS rn
  FROM `<PROJECT_ID>.youtube_stage.subtitles_segments_stage`
)
WHERE rn = 1;

-- 4) subtitles_full
INSERT INTO `<PROJECT_ID>.youtube_raw.subtitles_full` (video_id, lang, full_text)
SELECT video_id, lang, full_text
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY video_id, lang
           ORDER BY LENGTH(COALESCE(full_text,'')) DESC
         ) AS rn
  FROM `<PROJECT_ID>.youtube_stage.subtitles_full_stage`
)
WHERE rn = 1;

-- 5) Optional: cleanup staging
DROP TABLE IF EXISTS `<PROJECT_ID>.youtube_stage.channels_stage`;
DROP TABLE IF EXISTS `<PROJECT_ID>.youtube_stage.videos_stage`;
DROP TABLE IF EXISTS `<PROJECT_ID>.youtube_stage.subtitles_segments_stage`;
DROP TABLE IF EXISTS `<PROJECT_ID>.youtube_stage.subtitles_full_stage`;

-- 6) Sanity check
SELECT 'channels' AS table, COUNT(*) AS rows FROM `<PROJECT_ID>.youtube_raw.channels`
UNION ALL SELECT 'videos', COUNT(*) FROM `<PROJECT_ID>.youtube_raw.videos`
UNION ALL SELECT 'segments', COUNT(*) FROM `<PROJECT_ID>.youtube_raw.subtitles_segments`
UNION ALL SELECT 'full', COUNT(*) FROM `<PROJECT_ID>.youtube_raw.subtitles_full`;
