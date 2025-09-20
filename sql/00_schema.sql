-- sql/00_schema.sql
-- Purpose: Create raw and processed datasets and base tables for YouTube ingestion.
-- Inputs:  None (this script initializes datasets and empty base tables).
-- Outputs: Schemas <PROJECT_ID>.youtube_raw, <PROJECT_ID>.youtube_proc; tables: channels, videos, subtitles_segments, subtitles_full, hotel_label_meta, sponsored_label_meta.
-- Deps:    BigQuery permissions to CREATE SCHEMA/TABLE; location=asia-northeast3; <PROJECT_ID> replaced by $env:GCP_PROJECT_ID at run time.
-- Run:     Invoke-BqSqlFile .\sql\00_schema.sql
-- Notes:   Idempotent; safe to re-run.

-- Datasets
CREATE SCHEMA IF NOT EXISTS `<PROJECT_ID>.youtube_raw`  OPTIONS (location = "asia-northeast3");
CREATE SCHEMA IF NOT EXISTS `<PROJECT_ID>.youtube_proc` OPTIONS (location = "asia-northeast3");

-- Channels
CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_raw.channels` (
  channel_id STRING,
  channel_title STRING,
  channel_subs INT64,
  country STRING,
  uploads_playlist STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (channel_id) NOT ENFORCED
);

-- Videos (includes duration_sec)
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

-- Subtitles
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

-- Labels inferred from metadata
CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_proc.hotel_label_meta` (
  video_id STRING,
  is_hotel_pred BOOL,
  judged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.youtube_proc.sponsored_label_meta` (
  video_id STRING,
  is_paid_ad_pred BOOL,
  judged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
