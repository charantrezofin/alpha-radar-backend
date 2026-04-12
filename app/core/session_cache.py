"""
Session cache utility -- persists last successful API responses to disk
so that data from the last trading session is available when market is closed.

Cache files are stored in settings.DATA_DIR as session_{key}.json.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from app.config import settings

logger = logging.getLogger("alpha_radar.core.session_cache")


def is_market_hours() -> bool:
    """Return True if current time is within market hours (9:15 AM - 3:30 PM IST, weekdays)."""
    now = datetime.now(settings.TIMEZONE)
    # Weekday check: Mon=0, Fri=4
    if now.weekday() >= 5:
        return False
    total_mins = now.hour * 60 + now.minute
    # 9:15 = 555, 15:30 = 930
    return 555 <= total_mins <= 930


def _cache_path(key: str) -> Path:
    """Return the file path for a given cache key."""
    return settings.DATA_DIR / f"session_{key}.json"


def save_session(key: str, data: Any) -> None:
    """
    Save data to a session cache file.
    Writes to data/session_{key}.json with a timestamp.
    """
    try:
        settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "data": data,
            "timestamp": datetime.now(settings.TIMEZONE).isoformat(),
            "savedAt": time.time(),
        }
        path = _cache_path(key)
        path.write_text(json.dumps(payload, default=str), encoding="utf-8")
        logger.debug("[session_cache] Saved %s (%d bytes)", key, path.stat().st_size)
    except Exception as exc:
        logger.warning("[session_cache] Failed to save %s: %s", key, exc)


def load_session(key: str) -> dict | None:
    """
    Load data from a session cache file.
    Returns the cached dict with 'data', 'timestamp', 'savedAt' keys,
    or None if no cache exists.
    """
    try:
        path = _cache_path(key)
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload
    except Exception as exc:
        logger.warning("[session_cache] Failed to load %s: %s", key, exc)
        return None


def get_cached_or_none(key: str) -> tuple[Any | None, str | None]:
    """
    Convenience: load session and return (data, timestamp) or (None, None).
    """
    cached = load_session(key)
    if cached is None:
        return None, None
    return cached.get("data"), cached.get("timestamp")
