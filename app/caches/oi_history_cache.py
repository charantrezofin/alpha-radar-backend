"""
Open Interest history cache.

Stores periodic OI snapshots for each index (nifty, banknifty, etc.)
throughout the trading day.  Max 30 snapshots per day per index.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any, Optional

from app.config import settings

logger = logging.getLogger("alpha_radar.cache.oi_history")

# Valid index keys
VALID_KEYS = ("nifty", "banknifty", "finnifty", "midcpnifty")

_history: dict[str, list[dict[str, Any]]] = {k: [] for k in VALID_KEYS}
_history_date: Optional[str] = None
_lock = threading.Lock()

MAX_SNAPSHOTS_PER_DAY = 30


def _ensure_today() -> None:
    global _history_date
    today = datetime.now(settings.TIMEZONE).strftime("%Y-%m-%d")
    if _history_date != today:
        with _lock:
            for k in VALID_KEYS:
                _history[k] = []
            _history_date = today


def take_snapshot(index: str, chain_data: dict[str, Any]) -> None:
    """
    Record an OI snapshot for *index*.

    ``chain_data`` should contain the processed options chain data
    (total CE OI, total PE OI, PCR, max pain, etc.).
    """
    key = index.lower().replace(" ", "")
    if key not in VALID_KEYS:
        logger.warning("Unknown OI index key: %s", key)
        return

    _ensure_today()

    snapshot = {
        "timestamp": datetime.now(settings.TIMEZONE).isoformat(),
        **chain_data,
    }

    with _lock:
        history = _history[key]
        if len(history) >= MAX_SNAPSHOTS_PER_DAY:
            history.pop(0)  # drop oldest
        history.append(snapshot)

    logger.debug("OI snapshot for %s (%d total)", key, len(_history[key]))


def get_history(index: str) -> list[dict[str, Any]]:
    """Return all snapshots for *index* today."""
    _ensure_today()
    key = index.lower().replace(" ", "")
    with _lock:
        return list(_history.get(key, []))


def clear() -> None:
    """Clear all OI history."""
    global _history_date
    with _lock:
        for k in VALID_KEYS:
            _history[k] = []
        _history_date = None


def size(index: Optional[str] = None) -> int:
    """Return snapshot count for a specific index or total across all."""
    if index:
        key = index.lower().replace(" ", "")
        return len(_history.get(key, []))
    return sum(len(v) for v in _history.values())
