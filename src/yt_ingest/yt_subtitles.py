from __future__ import annotations
import os, json, re, datetime, hashlib
from typing import Dict, Any, List, Optional, Tuple
import requests
from yt_dlp import YoutubeDL
from dotenv import load_dotenv, find_dotenv
from pytz import timezone

load_dotenv(find_dotenv())
TZ = timezone("Asia/Bangkok")

def _now_iso() -> str:
    return datetime.datetime.now(tz=TZ).isoformat()

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _extract_info(video_id: str) -> Dict[str, Any]:
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "writesubtitles": True,
        "writeautomaticsub": True,
        "subtitlesformat": "json3",
    }
    with YoutubeDL(ydl_opts) as ydl:
        return ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)

def _pick_json3_track(info: Dict[str, Any], langs: List[str]) -> Tuple[str, str, str]:
    """
    선호 언어 순서대로 '수동 → 자동' 자막 트랙의 json3 URL 선택.
    반환: (url, lang, source) / 실패 시 ("", "", "")
    """
    subs = info.get("subtitles") or {}
    autos = info.get("automatic_captions") or {}

    # 수동
    for lang in langs:
        tracks = subs.get(lang, [])
        entry = next((t for t in tracks if t.get("ext") == "json3"), None)
        if entry and entry.get("url"):
            return entry["url"], lang, "manual"

    # 자동
    for lang in langs:
        tracks = autos.get(lang, [])
        entry = next((t for t in tracks if t.get("ext") == "json3"), None)
        if entry and entry.get("url"):
            return entry["url"], lang, "auto"

    return "", "", ""

def _download(url: str) -> Dict[str, Any]:
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return json.loads(r.text)

def _events_to_snippets(json3: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    json3의 events → [{idx,start,end,duration,text}]
    """
    out: List[Dict[str, Any]] = []
    events = json3.get("events", []) or []
    idx = 0
    for e in events:
        segs = e.get("segs")
        tstart = e.get("tStartMs")
        if not segs or tstart is None:
            continue
        start = float(tstart) / 1000.0
        dur = float(e.get("dDurationMs", 0)) / 1000.0
        end = start + dur
        text = "".join(seg.get("utf8", "") for seg in segs).strip()
        if text:
            out.append({"idx": idx, "start": start, "end": end, "duration": dur, "text": text})
            idx += 1
    return out

def get_subtitles(
    video_id: str,
    pref_langs: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    영어 우선, 없으면 한국어. 선택된 언어의 전체 텍스트 + 타임라인(snippets) 반환.
    실패 시 errors에 이유 기록.
    """
    if pref_langs is None:
        pref_langs = ["en", "ko"]

    errors: List[str] = []
    try:
        info = _extract_info(video_id)
    except Exception as e:
        return {
            "video_id": video_id, "subtitle_lang": None, "subtitle_text": None, "source": None,
            "snippet_count": 0, "snippets": [], "errors": [f"extract_failed: {e}"], "ingested_at": _now_iso()
        }

    url, lang, source = _pick_json3_track(info, pref_langs)
    if not url:
        return {
            "video_id": video_id, "subtitle_lang": None, "subtitle_text": None, "source": None,
            "snippet_count": 0, "snippets": [], "errors": ["no_suitable_track"], "ingested_at": _now_iso()
        }

    try:
        data = _download(url)
        snippets = _events_to_snippets(data)
        full_text = " ".join(s["text"] for s in snippets) if snippets else None
        return {
            "video_id": video_id,
            "subtitle_lang": lang,
            "subtitle_text": full_text,
            "source": source,
            "snippet_count": len(snippets),
            "snippets": snippets,
            "errors": errors,
            "ingested_at": _now_iso(),
        }
    except Exception as e:
        return {
            "video_id": video_id, "subtitle_lang": lang, "subtitle_text": None, "source": source,
            "snippet_count": 0, "snippets": [], "errors": [f"download_or_parse_failed: {e}"],
            "ingested_at": _now_iso()
        }

if __name__ == "__main__":
    video_id = "juLp1JUOGck"
    result = get_subtitles(video_id, pref_langs=["en", "ko"])
    print(result)