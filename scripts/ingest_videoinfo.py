from __future__ import annotations
import sys, datetime
from typing import List
from dotenv import load_dotenv, find_dotenv
from pytz import timezone

from src.yt_ingest.bq_client import ensure_tables, upsert_video_info
from src.yt_ingest.yt_meta import fetch_video_metadata

load_dotenv(find_dotenv())
TZ = timezone("Asia/Bangkok")

def main(video_ids: List[str]):
    ensure_tables()
    rows = fetch_video_metadata(video_ids)
    upsert_video_info(rows)

if __name__ == "__main__":
    # 사용: python -m scripts.ingest_info VIDEO_ID1 VIDEO_ID2 ...
    vids = sys.argv[1:] or ["sGriCACl7fg"]  # 예시
    main(vids)
