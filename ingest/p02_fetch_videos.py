# ingest/p02_fetch_videos.py
"""
Fetch video IDs from each channel's uploads playlist, then fetch video metadata.

Flow:
  channels.uploads_playlist → playlistItems.list → videos.list

Outputs:
  - out/playlist_items.jsonl  (one {"video_id": "..."} per line)
  - out/videos.jsonl          (video metadata incl. duration_sec)

Env:
  YT_API_KEY, GCP_PROJECT_ID, BQ_LOCATION (default: asia-northeast3), BQ_DATASET_RAW (default: youtube_raw)
"""

import argparse
import json
import os
import pathlib
import re
from typing import Iterable, List, Optional, Set

import requests
from dotenv import find_dotenv, load_dotenv
from google.cloud import bigquery

from ingest.common import backoff_sequence, jitter_sleep, write_jsonl

load_dotenv(find_dotenv())

API_KEY = os.getenv("YT_API_KEY")
YOUTUBE_API = "https://www.googleapis.com/youtube/v3"
PROJECT = os.getenv("GCP_PROJECT_ID")
LOCATION = os.getenv("BQ_LOCATION", "asia-northeast3")
DATASET_RAW = os.getenv("BQ_DATASET_RAW", "youtube_raw")


def _bq() -> bigquery.Client:
    """Return a BigQuery client bound to the configured location."""
    return bigquery.Client(project=PROJECT, location=LOCATION)


def get_upload_playlists_from_db() -> List[str]:
    """Read uploads playlist IDs from youtube_raw.channels."""
    table = f"{PROJECT}.{DATASET_RAW}.channels"
    sql = f"SELECT uploads_playlist FROM `{table}` WHERE uploads_playlist IS NOT NULL"
    try:
        rows = list(_bq().query(sql).result())
        return [r["uploads_playlist"] for r in rows if r["uploads_playlist"]]
    except Exception as e:
        print(f"failed to read channels from BigQuery: {e}")
        return []


def get_existing_video_ids() -> Set[str]:
    """Return a set of video_ids already present in youtube_raw.videos."""
    table = f"{PROJECT}.{DATASET_RAW}.videos"
    try:
        rows = _bq().query(f"SELECT video_id FROM `{table}`").result()
        return {r["video_id"] for r in rows}
    except Exception:
        return set()


def playlist_items(playlist_id: str) -> Iterable[str]:
    """Yield video IDs from a playlist via playlistItems.list (handles paging)."""
    url = f"{YOUTUBE_API}/playlistItems"
    token: Optional[str] = None
    while True:
        params = {
            "part": "contentDetails",
            "playlistId": playlist_id,
            "maxResults": 50,
            "key": API_KEY,
        }
        if token:
            params["pageToken"] = token

        for delay in backoff_sequence():  # 1s, 2s, 4s (capped)
            try:
                r = requests.get(url, params=params, timeout=30)
                if r.status_code == 429:
                    jitter_sleep(delay, 0.5)
                    continue
                r.raise_for_status()
                data = r.json()
                for it in data.get("items", []):
                    yield it["contentDetails"]["videoId"]
                token = data.get("nextPageToken")
                break
            except requests.RequestException:
                jitter_sleep(delay, 0.5)

        if not token:
            return


_ISO_DUR = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<h>\d+)H)?(?:(?P<m>\d+)M)?(?:(?P<s>\d+(?:\.\d+)?)S)?)?$"
)


def iso8601_to_seconds(s: Optional[str]) -> float:
    """Parse an ISO-8601 duration (e.g., PT1H2M3S) into seconds."""
    if not s:
        return 0.0
    m = _ISO_DUR.match(s)
    if not m:
        return 0.0
    days = float(m.group("days") or 0)
    h = float(m.group("h") or 0)
    m_ = float(m.group("m") or 0)
    s_ = float(m.group("s") or 0)
    return days * 86400 + h * 3600 + m_ * 60 + s_


def videos_list(ids: List[str]) -> Iterable[dict]:
    """Yield video metadata rows for the given video ID list via videos.list."""
    url = f"{YOUTUBE_API}/videos"
    batch = 50
    for i in range(0, len(ids), batch):
        chunk = ids[i : i + batch]
        params = {
            "part": "snippet,statistics,contentDetails,status,topicDetails",
            "id": ",".join(chunk),
            "key": API_KEY,
            "maxResults": 50,
        }
        for delay in backoff_sequence():
            try:
                r = requests.get(url, params=params, timeout=30)
                if r.status_code == 429:
                    jitter_sleep(delay, 0.5)
                    continue
                r.raise_for_status()
                data = r.json()
                for it in data.get("items", []):
                    sn = it.get("snippet", {}) or {}
                    st = it.get("statistics", {}) or {}
                    cd = it.get("contentDetails", {}) or {}
                    dur_iso = cd.get("duration")
                    yield {
                        "video_id": it["id"],
                        "channel_id": sn.get("channelId"),
                        "title": sn.get("title"),
                        "description": sn.get("description"),
                        "published_at": sn.get("publishedAt"),
                        "view_count": int(st.get("viewCount", "0") or 0),
                        "like_count": int(st.get("LikeCount", "0") or st.get("likeCount", "0") or 0),
                        "tags": sn.get("tags", []),
                        "default_lang": sn.get("defaultAudioLanguage") or sn.get("defaultLanguage"),
                        "duration_sec": iso8601_to_seconds(dur_iso),
                    }
                break
            except requests.RequestException:
                jitter_sleep(delay, 0.5)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out_ids", default="out/playlist_items.jsonl")
    parser.add_argument("--out_videos", default="out/videos.jsonl")
    args = parser.parse_args()

    if not API_KEY:
        raise SystemExit("YT_API_KEY not set")
    if not PROJECT:
        raise SystemExit("GCP_PROJECT_ID not set")

    uploads = get_upload_playlists_from_db()
    if not uploads:
        print("no uploads playlists in BigQuery (youtube_raw.channels)")
        pathlib.Path("out").mkdir(exist_ok=True)
        write_jsonl(args.out_ids, [])
        write_jsonl(args.out_videos, [])
        return

    vids: List[str] = []
    for pl in uploads:
        vids.extend(list(playlist_items(pl)))
    vids = list(dict.fromkeys(vids))  # de-duplicate while preserving order

    pathlib.Path("out").mkdir(exist_ok=True)
    write_jsonl(args.out_ids, [{"video_id": v} for v in vids])
    print(f"✅ playlist items: {len(vids)} → {args.out_ids}")

    existing = get_existing_video_ids()
    to_fetch = [v for v in vids if v not in existing]
    if not to_fetch:
        print("All videos already present in BigQuery; skipping videos.list")
        write_jsonl(args.out_videos, [])
        return

    rows = list(videos_list(to_fetch))
    write_jsonl(args.out_videos, rows)
    print(f"✅ videos meta (new only): {len(rows)} → {args.out_videos}")


if __name__ == "__main__":
    main()
