from __future__ import annotations
import os, hashlib, datetime
from typing import List, Dict, Any
from dotenv import load_dotenv, find_dotenv
from googleapiclient.discovery import build
from pytz import timezone

load_dotenv(find_dotenv())
TZ = timezone("Asia/Bangkok")

def _hash_for_meta(item: Dict[str, Any]) -> str:
    # 변경감지용 간단 해시
    import json
    s = json.dumps(item, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def fetch_video_metadata(video_ids: List[str]) -> List[Dict[str, Any]]:
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY missing in .env")

    yt = build("youtube", "v3", developerKey=api_key)
    parts = ",".join(["snippet","contentDetails","status","topicDetails","recordingDetails","localizations"])
    out: List[Dict[str, Any]] = []

    # 50개 배치
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        resp = yt.videos().list(part=parts, id=",".join(batch), maxResults=50).execute()
        for it in resp.get("items", []):
            snippet = it.get("snippet", {})
            content = it.get("contentDetails", {})
            tags = snippet.get("tags") or []
            row = {
                "video_id": it["id"],
                "channel_id": snippet.get("channelId"),
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "published_at": snippet.get("publishedAt"),
                "tags": tags,
                "category_id": snippet.get("categoryId"),
                "duration_iso8601": content.get("duration"),
                "default_language": snippet.get("defaultLanguage"),
                "default_audio_language": snippet.get("defaultAudioLanguage"),
                "caption_available": True if (content.get("caption") == "true") else False,
                "source_api_version": "youtube_v3",
                "meta_hash": _hash_for_meta(it),
                "ingested_at": datetime.datetime.now(tz=TZ).isoformat(),
            }
            out.append(row)
    return out
