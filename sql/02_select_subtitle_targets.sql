-- sql/02_select_subtitle_targets.sql
-- Purpose: Select videos that (a) are hotel-focused, (b) have no stored subtitles yet, and (c) are longer than 60 seconds.
-- Inputs:  <PROJECT_ID>.youtube_raw.videos, <PROJECT_ID>.youtube_proc.hotel_label_meta,
--          <PROJECT_ID>.youtube_raw.subtitles_full, <PROJECT_ID>.youtube_raw.subtitles_segments
-- Outputs: Creates or replaces <PROJECT_ID>.youtube_proc.to_fetch_subs with the list of video_id to fetch subtitles for.
-- Deps:    Schemas/tables from 00_schema.sql; populated videos and hotel_label_meta.
-- Run:     Invoke-BqSqlFile .\sql\02_select_subtitle_targets.sql
-- Notes:   Idempotent; re-running refreshes the target list based on current labels and subtitle presence.

CREATE OR REPLACE TABLE `<PROJECT_ID>.youtube_proc.to_fetch_subs` AS
SELECT
  v.video_id
FROM `<PROJECT_ID>`.youtube_raw.videos AS v
JOIN `<PROJECT_ID>`.youtube_proc.hotel_label_meta AS h
  ON h.video_id = v.video_id
LEFT JOIN `<PROJECT_ID>`.youtube_raw.subtitles_full AS sf
  ON sf.video_id = v.video_id
LEFT JOIN `<PROJECT_ID>`.youtube_raw.subtitles_segments AS ss
  ON ss.video_id = v.video_id
WHERE h.is_hotel_pred = TRUE
  AND sf.video_id IS NULL
  AND ss.video_id IS NULL
  AND COALESCE(v.duration_sec, 0) > 60;
