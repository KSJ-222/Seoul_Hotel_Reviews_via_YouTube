# ingest/common.py
"""
Utility helpers for ingestion scripts.

Functions:
- write_jsonl(path, rows): Write an iterable of dicts to a JSON Lines file.
- jitter_sleep(base_seconds, jitter_span_seconds): Sleep for base + random jitter.
- backoff_sequence(max_tries, base, cap, factor): Yield exponential backoff delays.

These helpers are used across fetch/load scripts to standardize I/O and retry timing.
"""

import json
import os
import random
import time
from typing import Iterable, Iterator


def write_jsonl(path: str, rows: Iterable[dict]) -> None:
    """Write records to a JSONL file, creating parent directories if needed."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def jitter_sleep(base_seconds: float, jitter_span_seconds: float) -> None:
    """Sleep for `base_seconds` plus a uniform random jitter in [0, jitter_span_seconds]."""
    time.sleep(base_seconds + random.random() * max(jitter_span_seconds, 0.0))


def backoff_sequence(
    max_tries: int = 3, base: float = 1.0, cap: float = 16.0, factor: float = 2.0
) -> Iterator[float]:
    """
    Yield an exponential backoff sequence of delays (in seconds), capped by `cap`.

    Example (defaults): 1s, 2s, 4s
    """
    delay = base
    for _ in range(max_tries):
        yield min(delay, cap)
        delay = min(delay * factor, cap)
