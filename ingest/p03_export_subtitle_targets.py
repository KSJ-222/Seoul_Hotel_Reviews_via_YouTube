# ingest/p03_export_subtitle_targets.py
"""
Export target video_ids from BigQuery to ./out/targets.jsonl.

Purpose:
  Read video IDs from <PROJECT>.<BQ_DATASET_PROC>.to_fetch_subs and
  write them to a JSONL file for downstream subtitle fetching.

Usage:
  python -m ingest.p03_export_subtitle_targets

Environment:
  GCP_PROJECT_ID   GCP project ID
  BQ_DATASET_PROC  BigQuery dataset for processed tables (default: youtube_proc)
  BQ_LOCATION      BigQuery location (default: asia-northeast3)

Output:
  out/targets.jsonl  (one {"video_id": "..."} per line)
"""

import json
import os
import pathlib

from dotenv import find_dotenv, load_dotenv
from google.cloud import bigquery

load_dotenv(find_dotenv())

PROJECT = os.getenv("GCP_PROJECT_ID")
DATASET_PROC = os.getenv("BQ_DATASET_PROC", "youtube_proc")
LOCATION = os.getenv("BQ_LOCATION", "asia-northeast3")


def main() -> None:
    if not PROJECT:
        raise SystemExit("GCP_PROJECT_ID not set")

    client = bigquery.Client(project=PROJECT, location=LOCATION)
    sql = f"SELECT video_id FROM `{PROJECT}.{DATASET_PROC}.to_fetch_subs` ORDER BY video_id"
    rows = list(client.query(sql).result())

    out_dir = pathlib.Path("out")
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "targets.jsonl"

    with out_path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps({"video_id": r["video_id"]}, ensure_ascii=False) + "\n")

    print(f"✅ wrote {len(rows)} ids → {out_path}")


if __name__ == "__main__":
    main()
