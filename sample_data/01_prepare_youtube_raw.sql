-- sample_data/01_prepare_youtube_raw.sql
-- Purpose: Prepare raw and staging datasets plus base tables to receive local JSONL loads for YouTube ingestion.
-- Inputs:  None (this script initializes empty datasets/tables).
-- Outputs: <PROJECT_ID>.youtube_raw.{channels,videos,subtitles_segments,subtitles_full} and <PROJECT_ID>.youtube_stage.* staging tables.
-- Deps:    BigQuery permissions to CREATE SCHEMA/TABLE; <PROJECT_ID> is replaced by $env:GCP_PROJECT_ID at run time.
-- Run:     Invoke-BqSqlFile .\sample_data\01_prepare_youtube_raw.sql
-- Notes:   Idempotent; safe to re-run.

-- <PROJECT_ID> is replaced by PowerShell
CREATE SCHEMA IF NOT EXISTS `<PROJECT_ID>.youtube_raw`;
CREATE SCHEMA IF NOT EXISTS `<PROJECT_ID>.youtube_stage`;

-- Final tables
CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_raw.channels` (
  channel_id STRING,
  channel_title STRING,
  channel_subs INT64,
  country STRING,
  uploads_playlist STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (channel_id) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_raw.videos` (
  video_id STRING,
  channel_id STRING,
  title STRING,
  description STRING,
  published_at TIMESTAMP,
  view_count INT64,
  like_count INT64,
  tags ARRAY<STRING>,
  default_lang STRING,
  duration_sec FLOAT64,
  PRIMARY KEY (video_id) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_raw.subtitles_segments` (
  video_id STRING,
  lang STRING,
  idx INT64,
  start_sec FLOAT64,
  dur_sec FLOAT64,
  text STRING
);

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_raw.subtitles_full` (
  video_id STRING,
  lang STRING,
  full_text STRING
);

-- Staging (loose schema to accept raw local JSONL as-is)
CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_stage.channels_stage` (
  channel_id STRING, channel_title STRING, channel_subs STRING,
  country STRING, uploads_playlist STRING, created_at STRING
);

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_stage.videos_stage` (
  video_id STRING, channel_id STRING, title STRING, description STRING,
  published_at STRING, view_count STRING, like_count STRING,
  tags ARRAY<STRING>, default_lang STRING, duration_sec STRING
);

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_stage.subtitles_segments_stage` (
  video_id STRING, lang STRING, idx STRING, start_sec STRING, dur_sec STRING, text STRING
);

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_stage.subtitles_full_stage` (
  video_id STRING, lang STRING, full_text STRING
);
