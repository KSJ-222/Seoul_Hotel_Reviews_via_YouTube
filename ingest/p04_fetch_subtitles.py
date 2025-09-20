# ingest/p04_fetch_subtitles.py
"""
Robust subtitle fetcher (yt-dlp JSON3 first, WebVTT fallback).

Strategy:
  1) Try yt-dlp Python API with subtitlesformat=json3 → parse JSON3 events
  2) Fallback: use info dict subtitle URLs (json3 / vtt / srv*) → download & parse
  3) Language selection: prefer exact (xx), then variants (xx-*), then auto (a.xx). Fallback lang is used if preferred is unavailable.

Usage:
  python -m ingest.p04_fetch_subtitles \
    --video_ids_file out/targets.jsonl \
    --lang en --fallback ko \
    [--cookies cookies.txt] [--debug]

Outputs:
  out/subtitles_segments.jsonl  # [{"video_id","lang","idx","start_sec","dur_sec","text"}, ...]
  out/subtitles_full.jsonl      # [{"video_id","lang","full_text"}, ...]
"""

import argparse
import json
import os
import re
import time
from http.cookiejar import MozillaCookieJar
from typing import Dict, List, Optional, Tuple

import requests

from ingest.common import backoff_sequence, jitter_sleep, write_jsonl


def parse_json3_events(json3_obj) -> Tuple[List[dict], str]:
    """Parse YouTube JSON3 caption events into segments and full text."""
    segs, full = [], []
    idx = 0
    for ev in (json3_obj or {}).get("events", []):
        seg_list = ev.get("segs") or []
        if not seg_list:
            continue
        text = "".join(s.get("utf8", "") for s in seg_list).replace("\n", " ").strip()
        if not text:
            continue
        start = ev.get("tStartMs", 0) / 1000.0
        dur = ev.get("dDurationMs", 0) / 1000.0
        segs.append((idx, start, dur, text))
        idx += 1
        full.append(text)
    return (
        [{"idx": i, "start_sec": st, "dur_sec": du, "text": tx} for (i, st, du, tx) in segs],
        " ".join(full),
    )


_VTT_TS = re.compile(
    r"^(?P<h>\d{2}):(?P<m>\d{2}):(?P<s>\d{2}\.\d{3})\s-->\s(?P<h2>\d{2}):(?P<m2>\d{2}):(?P<s2>\d{2}\.\d{3})"
)


def _hms_to_sec(h: str, m: str, s_str: str) -> float:
    s = float(s_str)
    return int(h) * 3600 + int(m) * 60 + s


def parse_webvtt(vtt_text: str) -> Tuple[List[dict], str]:
    """Parse a WebVTT string into segments and full text."""
    lines = [ln.strip("\ufeff ").rstrip() for ln in vtt_text.splitlines()]
    segs, full = [], []
    idx = 0
    i = 0
    while i < len(lines):
        ln = lines[i]
        if "-->" in ln:
            m = _VTT_TS.search(ln)
            if m:
                start = _hms_to_sec(m.group("h"), m.group("m"), m.group("s"))
                end = _hms_to_sec(m.group("h2"), m.group("m2"), m.group("s2"))
                i += 1
                text_lines: List[str] = []
                while i < len(lines) and lines[i] and "-->" not in lines[i]:
                    if not lines[i].startswith("NOTE"):
                        text_lines.append(lines[i])
                    i += 1
                text = " ".join(text_lines).replace("\n", " ").strip()
                if text:
                    segs.append(
                        {"idx": idx, "start_sec": start, "dur_sec": max(0.0, end - start), "text": text}
                    )
                    idx += 1
                    full.append(text)
                continue
        i += 1
    return segs, " ".join(full)


def best_lang_key(available: Dict[str, list], pref: str, fb: str) -> Optional[str]:
    """Choose the best matching language key from available captions."""
    order = [pref, f"{pref}-", f"a.{pref}", fb, f"{fb}-", f"a.{fb}"]
    keys = list(available.keys())
    for pat in order:
        if pat.endswith("-"):
            for k in keys:
                if k.startswith(pat):
                    return k
        else:
            if pat in available:
                return pat
    return None


def build_session(cookies_path: Optional[str] = None) -> requests.Session:
    """Create a requests session with optional Netscape-format cookies."""
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "en-US,en;q=0.9,ko;q=0.8",
        }
    )
    if cookies_path and os.path.exists(cookies_path):
        jar = MozillaCookieJar()
        try:
            jar.load(cookies_path, ignore_discard=True, ignore_expires=False)
            for c in jar:
                s.cookies.set_cookie(c)
        except Exception:
            pass
    return s


def download_text(url: str, session: requests.Session, timeout: int = 30) -> Optional[str]:
    """GET a text resource with simple retry/backoff on transient errors."""
    for delay in backoff_sequence():
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code in (429, 500, 503):
                jitter_sleep(delay, 0.5)
                continue
            r.raise_for_status()
            return r.text
        except requests.RequestException:
            jitter_sleep(delay, 0.5)
    return None


def fetch_subs_for_video(
    vid: str, pref: str, fb: str, cookies_path: Optional[str] = None, debug: bool = False
) -> Tuple[List[dict], Optional[dict]]:
    """
    Retrieve subtitles for a single video ID.
    Returns (segments, full) where:
      segments: list of {"video_id","lang","idx","start_sec","dur_sec","text"}
      full:     {"video_id","lang","full_text"} or None
    """
    segments: List[dict] = []
    full: Optional[dict] = None

    # A) Try yt-dlp Python API with JSON3
    try:
        from yt_dlp import YoutubeDL

        ydl_opts = {
            "skip_download": True,
            "writesubtitles": True,
            "writeautomaticsub": True,
            "subtitlesformat": "json3",
            "subtitleslangs": [pref, fb, f"{pref}-", f"{fb}-", f"a.{pref}", f"a.{fb}"],
            "quiet": True,
            "no_warnings": True,
        }
        if cookies_path and os.path.exists(cookies_path):
            ydl_opts["cookiefile"] = cookies_path

        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={vid}", download=False)

        rs = (info or {}).get("requested_subtitles") or {}
        if rs:
            k = best_lang_key(rs, pref, fb) or (list(rs.keys())[0] if rs else None)
            data = k and rs[k].get("data")
            if data:
                evs, full_text = parse_json3_events(data)
                if evs:
                    segments = [dict(video_id=vid, lang=k, **e) for e in evs]
                    full = dict(video_id=vid, lang=k, full_text=full_text[:1_000_000])
                    if debug:
                        print(f"[{vid}] JSON3 via yt-dlp API ok: lang={k}, segs={len(evs)}")
                    return segments, full
        if debug:
            print(f"[{vid}] JSON3 via yt-dlp API not found; trying fallback")
    except Exception as e:
        if debug:
            print(f"[{vid}] yt-dlp API failed: {e}; trying fallback")

    # B) Fallback: use info dict subtitle URLs (json3 / vtt / srv*)
    try:
        from yt_dlp import YoutubeDL

        ydl_opts2 = {"quiet": True, "no_warnings": True}
        if cookies_path and os.path.exists(cookies_path):
            ydl_opts2["cookiefile"] = cookies_path

        with YoutubeDL(ydl_opts2) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={vid}", download=False)

        subs = (info or {}).get("subtitles") or {}
        autos = (info or {}).get("automatic_captions") or {}
        pool = subs if subs else autos
        if not pool:
            if debug:
                print(f"[{vid}] no subtitles/automatic_captions in info")
            return [], None

        k = best_lang_key(pool, pref, fb) or (list(pool.keys())[0] if pool else None)
        if not k:
            if debug:
                print(f"[{vid}] no matching lang key in {list(pool.keys())[:5]} ...")
            return [], None

        entries = pool.get(k) or []
        url_json3 = next((e.get("url") for e in entries if e.get("ext") == "json3"), None)
        url_vtt = next((e.get("url") for e in entries if e.get("ext") in ("vtt", "ttml", "srv3", "srv1")), None)

        sess = build_session(cookies_path)

        if url_json3:
            txt = download_text(url_json3, sess)
            if txt:
                try:
                    data = json.loads(txt)
                    evs, full_text = parse_json3_events(data)
                    if evs:
                        segments = [dict(video_id=vid, lang=k, **e) for e in evs]
                        full = dict(video_id=vid, lang=k, full_text=full_text[:1_000_000])
                        if debug:
                            print(f"[{vid}] JSON3 via URL ok: lang={k}, segs={len(evs)}")
                        return segments, full
                except Exception:
                    pass

        if url_vtt:
            txt = download_text(url_vtt, sess)
            if txt:
                evs, full_text = parse_webvtt(txt)
                if evs:
                    segments = [dict(video_id=vid, lang=k, **e) for e in evs]
                    full = dict(video_id=vid, lang=k, full_text=full_text[:1_000_000])
                    if debug:
                        print(f"[{vid}] VTT fallback ok: lang={k}, segs={len(evs)}")
                    return segments, full

        if debug:
            print(f"[{vid}] no usable caption URLs (json3/vtt)")
        return [], None

    except Exception as e:
        if debug:
            print(f"[{vid}] fallback failed: {e}")
        return [], None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--video_ids_file", required=True, help="Path to JSONL file with {'video_id': ...}")
    ap.add_argument("--lang", default="en", help="Preferred caption language code (e.g., en)")
    ap.add_argument("--fallback", default="ko", help="Fallback language code (e.g., ko)")
    ap.add_argument("--out_segments", default="out/subtitles_segments.jsonl")
    ap.add_argument("--out_full", default="out/subtitles_full.jsonl")
    ap.add_argument("--cookies", default=None, help="Path to cookies.txt (Netscape/Mozilla format)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    ids = [
        json.loads(l)["video_id"]
        for l in open(args.video_ids_file, "r", encoding="utf-8")
        if l.strip()
    ]
    pref, fb = args.lang.lower(), args.fallback.lower()

    seg_rows: List[dict] = []
    full_rows: List[dict] = []
    for vid in ids:
        s, f = fetch_subs_for_video(vid, pref, fb, cookies_path=args.cookies, debug=args.debug)
        if s:
            seg_rows.extend(s)
            if f:
                full_rows.append(f)
        time.sleep(0.4)  # brief pause between videos

    write_jsonl(args.out_segments, seg_rows)
    write_jsonl(args.out_full, full_rows)
    print(f"✅ segments: {len(seg_rows)}  → {args.out_segments}")
    print(f"✅ full:     {len(full_rows)} → {args.out_full}")


if __name__ == "__main__":
    main()
