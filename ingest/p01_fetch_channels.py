# ingest/p01_fetch_channels.py
"""
Fetch channel metadata via YouTube Data API v3 and save as JSONL.

Usage (PowerShell):
  $env:YT_API_KEY="YOUR_API_KEY"
  python -m ingest.p01_fetch_channels

Optional:
  python -m ingest.p01_fetch_channels --out .\out\channels.jsonl
  python -m ingest.p01_fetch_channels --channels "UCxxxx,UCyyyy"
"""

import argparse
import os
from typing import Dict, List

import requests
from dotenv import find_dotenv, load_dotenv

from ingest.common import backoff_sequence, jitter_sleep, write_jsonl

load_dotenv(find_dotenv())

API_KEY = os.getenv("YT_API_KEY")
YOUTUBE_API = "https://www.googleapis.com/youtube/v3"

# ---------------- Hardcoded channel IDs ----------------
HARDCODED_CHANNELS: List[str] = [
    s.strip() for s in """
UC1OA0PWgSL3boEj-bkbnj1A,UCYxsXxbjJO1YYa9yQ3lKC8w,UCfRJ9Hw-WhXwVPiagNhNHTw,UCm8vIT4tBhCI5EghEjIxo4A,UCldwUablgk8KPcFPQlKnB-w,UCk8b-Sml9_tR_hquipaMdGg,UC78Z8NqjKy2_hO7cv6zw17A,UCOYMhBR9_wKh-zkXPQ8W1cw,UCSei4YHej33B6XJu9LGsEFQ,UC50xPFPma-nCS5GZpAFukpQ,UCWmP5RJpbngL-5SgrMxE79A,UCQe6loP6voLEoKl0zREAeZQ,UCMZElq8mEULCmaj46lKWZOA,UCATN1EjzEv4IURye1m22Jog,UCoxHJfORwojUierMJc7q5kw,UC1qCJewkzvgEUWfPV4uvmuQ,UCkidqWkzkXl1f68anc4M6bw,UCRbuIba6WS1HOdlKxYiGcYA,UCerSFrrU_BvhbYY8_k7uGtQ,UCE11CSQA7Oj6q0ocFFO7l5w,UCPtVpwZeQNMDNtNRSNIQ76A,UCtY8mzwz9LY0zLxkrlNzoKA,UCLZriRGWJrVSSliMhkFCqJQ,UC02M40JN1Yr9N76HqkU6Ahg,UC3fS6cBq4JcJX4CIYcKhtKw,UCGsIuZKpWFSOp2FEoySWTWw,UClOI5S5ALqsvoI7Ul-M4ILA,UCy5PAemuM6fc6fy0kw8ZESg,UC4USf1GhahCaVCcbU2iw8rQ,UCqjqrbtPOPRXjVV5hKQVmKA,UCGRSbidwu0DrbQIIRYodY_Q,UCBO8UwABkuqFSxaxG3fxULw,UCAycjZAX7wCywHs2pAZoIcg,UClOxldJH1i0G4xg8VceyBZg,UCqChwvEGqFP8w3Jyi-UCaEQ,UCPv1IXIINpUdjMuxdD88gMA,UCthrY5mLeptivP_thDUwMTg,UCqPYzU6fDBZpGEqjcMPRHOA,UC909kguZ7l5W7bDQUhrA-ow,UCPHn3YViX_dylhDRGPg7Tug,UCG7apKe5csRaLSj5WIy6VWQ,UCaD2ODkWCwfDNaxKT_egYFg,UCvSTKxe2O_MpzVXwUKED5Rw,UCQK8_ejmTrRqyEsYDTHoHaQ,UCJ1QpR5y4Ch9glYpOiwrPyA,UCEqSijKE5qlS8kIvWmShYpw,UCcQ2_xIrN4ZHMP-aDVW6W1A,UCUw5TY15KNfZoYvwTXniIUg,UC4qBrLQgM_WIrY4ImuSNRPA,UCIdy0QGEdS29Tarcgwpuh8g,UCFhZXwKB4Q_a4RdO-TJ0dwA,UCnS3ASjhPAuPao5rY5xy7DA
""".strip().split(",") if s.strip()
]
# ------------------------------------------------------


def _chunked(xs: List[str], n: int) -> List[List[str]]:
    return [xs[i : i + n] for i in range(0, len(xs), n)]


def channels_list(ids: List[str]) -> List[Dict]:
    """Fetch metadata for up to 50 channel IDs."""
    url = f"{YOUTUBE_API}/channels"
    params = {
        "part": "snippet,statistics,contentDetails",
        "id": ",".join(ids),
        "key": API_KEY,
        "maxResults": 50,
    }
    rows: List[Dict] = []
    for delay in backoff_sequence():  # 1s, 2s, 4s (capped)
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 429:
                jitter_sleep(delay, 0.5)
                continue
            r.raise_for_status()
            data = r.json()
            for it in data.get("items", []):
                rows.append(
                    {
                        "channel_id": it["id"],
                        "channel_title": it["snippet"].get("title"),
                        "channel_subs": int(
                            it.get("statistics", {}).get("subscriberCount", "0") or 0
                        ),
                        "country": it["snippet"].get("country"),
                        "uploads_playlist": it["contentDetails"]["relatedPlaylists"]["uploads"],
                    }
                )
            break
        except requests.RequestException:
            jitter_sleep(delay, 0.5)
    return rows


def main() -> None:
    if not API_KEY:
        raise RuntimeError(
            "YT_API_KEY not set. Put it in your environment or .env file."
        )

    ap = argparse.ArgumentParser()
    ap.add_argument("--channels", required=False, help="Comma-separated channel IDs")
    ap.add_argument("--out", default="out/channels.jsonl")
    args = ap.parse_args()

    # Prefer CLI input if provided; else use hardcoded list
    if args.channels:
        ids = [s.strip() for s in args.channels.split(",") if s.strip()]
    else:
        ids = HARDCODED_CHANNELS

    # Deduplicate while preserving order
    seen = {}
    ids = [seen.setdefault(x, x) for x in ids if x not in seen]

    # Ensure output directory exists
    out_dir = os.path.dirname(args.out) or "."
    os.makedirs(out_dir, exist_ok=True)

    # Call the API in batches of 50
    all_rows: List[Dict] = []
    for batch in _chunked(ids, 50):
        all_rows.extend(channels_list(batch))

    write_jsonl(args.out, all_rows)
    print(f"✅ channels: {len(all_rows)} → {args.out}")


if __name__ == "__main__":
    main()
