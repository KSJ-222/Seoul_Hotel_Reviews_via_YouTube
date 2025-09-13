from __future__ import annotations
import sys
from typing import List
from dotenv import load_dotenv, find_dotenv

import os 
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from src.yt_ingest.bq_client import ensure_tables, upsert_subtitles_raw
from src.yt_ingest.yt_subtitles import get_subtitles

load_dotenv(find_dotenv())

def main(video_ids: List[str]):
    ensure_tables()
    rows = [ get_subtitles(v) for v in video_ids ]
    upsert_subtitles_raw(rows, dedupe_on="video_id,subtitle_lang")

if __name__ == "__main__":
    # 사용: python -m scripts.ingest_subtitles VIDEO_ID1 VIDEO_ID2 ...
    vids = sys.argv[1:] or ["juLp1JUOGck"]  # 예시
    main(vids)
