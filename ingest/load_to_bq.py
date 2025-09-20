# ingest/load_to_bq.py
"""
JSONL → BigQuery upsert (MERGE) utilities.

Usage:
  python -m ingest.load_to_bq --target channels
  python -m ingest.load_to_bq --target videos
  python -m ingest.load_to_bq --target subtitles

Environment:
  GCP_PROJECT_ID          Project ID
  BQ_DATASET_RAW          Target dataset (default: youtube_raw)
  BQ_LOCATION             BigQuery location (default: asia-northeast3)

Inputs (from ./out/):
  channels.jsonl, videos.jsonl, subtitles_segments.jsonl, subtitles_full.jsonl
"""

import argparse
import json
import os
import uuid
from typing import List, Dict

from dotenv import find_dotenv, load_dotenv
from google.cloud import bigquery

load_dotenv(find_dotenv())

PROJECT = os.getenv("GCP_PROJECT_ID")
RAW = os.getenv("BQ_DATASET_RAW", "youtube_raw")
LOC = os.getenv("BQ_LOCATION", "asia-northeast3")


def _bq() -> bigquery.Client:
    """Create a BigQuery client bound to the configured location."""
    return bigquery.Client(project=PROJECT, location=LOC)


def _read_jsonl(path: str) -> List[Dict]:
    """Read a JSONL file into a list of dicts; return [] if missing/empty."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _load_staging(rows: List[Dict]) -> str:
    """Load rows into a temporary staging table and return its full name."""
    stg = f"{PROJECT}.{RAW}._stg_{uuid.uuid4().hex[:8]}"
    _bq().create_table(bigquery.Table(stg), exists_ok=True)
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )
    job = _bq().load_table_from_json(rows, stg, job_config=cfg)
    job.result()
    return stg


def _merge_channels(stg: str) -> None:
    """Merge staging rows into youtube_raw.channels."""
    sql = f"""
    MERGE `{PROJECT}.{RAW}.channels` T
    USING `{stg}` S
    ON T.channel_id = S.channel_id
    WHEN MATCHED THEN UPDATE SET
      T.channel_title   = S.channel_title,
      T.uploads_playlist = S.uploads_playlist,
      T.channel_subs    = S.channel_subs,
      T.country         = S.country
    WHEN NOT MATCHED THEN INSERT (channel_id, channel_title, uploads_playlist, channel_subs, country)
      VALUES (S.channel_id, S.channel_title, S.uploads_playlist, S.channel_subs, S.country)
    """
    _bq().query(sql).result()


def _merge_videos(stg: str) -> None:
    """Merge staging rows into youtube_raw.videos."""
    sql = f"""
    MERGE `{PROJECT}.{RAW}.videos` T
    USING `{stg}` S
    ON T.video_id = S.video_id
    WHEN MATCHED THEN UPDATE SET
      T.channel_id   = S.channel_id,
      T.title        = S.title,
      T.description  = S.description,
      T.published_at = TIMESTAMP(S.published_at),
      T.view_count   = S.view_count,
      T.like_count   = S.like_count,
      T.tags         = S.tags,
      T.default_lang = S.default_lang,
      T.duration_sec = S.duration_sec
    WHEN NOT MATCHED THEN INSERT
      (video_id, channel_id, title, description, published_at, view_count, like_count, tags, default_lang, duration_sec)
      VALUES
      (S.video_id, S.channel_id, S.title, S.description, TIMESTAMP(S.published_at), S.view_count, S.like_count, S.tags, S.default_lang, S.duration_sec)
    """
    _bq().query(sql).result()


def _merge_subtitles(seg_stg: str | None, full_stg: str | None) -> None:
    """Merge staging rows into subtitles tables (segments/full)."""
    if seg_stg:
        _bq().query(
            f"""
            MERGE `{PROJECT}.{RAW}.subtitles_segments` T
            USING `{seg_stg}` S
            ON  T.video_id = S.video_id AND T.lang = S.lang AND T.idx = S.idx
            WHEN NOT MATCHED THEN
              INSERT (video_id, lang, idx, start_sec, dur_sec, text)
              VALUES (S.video_id, S.lang, S.idx, S.start_sec, S.dur_sec, S.text)
            """
        ).result()
    if full_stg:
        _bq().query(
            f"""
            MERGE `{PROJECT}.{RAW}.subtitles_full` T
            USING `{full_stg}` S
            ON  T.video_id = S.video_id AND T.lang = S.lang
            WHEN MATCHED THEN
              UPDATE SET T.full_text = S.full_text
            WHEN NOT MATCHED THEN
              INSERT (video_id, lang, full_text)
              VALUES (S.video_id, S.lang, S.full_text)
            """
        ).result()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True, choices=["channels", "videos", "subtitles"])
    args = ap.parse_args()

    if args.target == "channels":
        rows = _read_jsonl("out/channels.jsonl")
        if not rows:
            print("no channels.jsonl")
            return
        stg = _load_staging(rows)
        _merge_channels(stg)
        _bq().delete_table(stg, not_found_ok=True)
        print("channels upserted")

    elif args.target == "videos":
        rows = _read_jsonl("out/videos.jsonl")
        if not rows:
            print("no videos.jsonl (or all existed)")
            return
        stg = _load_staging(rows)
        _merge_videos(stg)
        _bq().delete_table(stg, not_found_ok=True)
        print("videos upserted")

    elif args.target == "subtitles":
        seg = _read_jsonl("out/subtitles_segments.jsonl")
        full = _read_jsonl("out/subtitles_full.jsonl")
        if not seg and not full:
            print("no subtitles files")
            return
        seg_stg = _load_staging(seg) if seg else None
        full_stg = _load_staging(full) if full else None
        _merge_subtitles(seg_stg, full_stg)
        if seg_stg:
            _bq().delete_table(seg_stg, not_found_ok=True)
        if full_stg:
            _bq().delete_table(full_stg, not_found_ok=True)
        print("subtitles upserted")


if __name__ == "__main__":
    main()
