"""
FII/DII activity cache.

Fetches FII & DII buy/sell data from the NSE API.  Uses a 15-minute
TTL and persists to disk so stale data can be served on API failures.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from app.core.cache import Cache

logger = logging.getLogger("alpha_radar.cache.fii_dii")

_cache = Cache(
    name="fii_dii",
    ttl=15 * 60,  # 15 minutes
    persist_path=".fiidii_cache.json",
)

_CACHE_KEY = "latest"


def get_fii_dii() -> Optional[dict[str, Any]]:
    """Return the latest FII/DII data, or ``None`` if not loaded."""
    return _cache.get(_CACHE_KEY)


def clear() -> None:
    _cache.clear()


async def load_fii_dii() -> Optional[dict[str, Any]]:
    """
    Fetch FII/DII data from NSE and update the cache.

    Returns the fetched data dict, or falls back to stale disk data on error.
    """
    # If cache is still fresh, return it
    cached = _cache.get(_CACHE_KEY)
    if cached is not None:
        return cached

    from app.data.nse_fetcher import fetch_fii_dii

    try:
        data = await fetch_fii_dii()
        if data:
            _cache.set(_CACHE_KEY, data)
            _cache.save_to_disk()
            logger.info("FII/DII cache updated")
            return data
    except Exception:
        logger.exception("Failed to fetch FII/DII data from NSE")

    # Try restoring stale data from disk
    if _cache.load_from_disk():
        stale = _cache.get(_CACHE_KEY)
        if stale:
            logger.info("Serving stale FII/DII data from disk")
            return {**stale, "stale": True}

    return None
