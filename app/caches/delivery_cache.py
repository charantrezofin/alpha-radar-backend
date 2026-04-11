"""
Delivery percentage cache.

Loads NSE delivery data (CSV) and caches the delivery-to-traded
percentage for each symbol.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from app.config import settings
from app.core.cache import Cache

logger = logging.getLogger("alpha_radar.cache.delivery")

_cache = Cache(name="delivery", ttl=None)
_loaded_date: Optional[str] = None


def get_delivery_pct(symbol: str) -> Optional[float]:
    """Return delivery percentage for *symbol*, or ``None``."""
    return _cache.get(symbol)


def get_all() -> dict[str, float]:
    return _cache.get_all()


def clear() -> None:
    global _loaded_date
    _cache.clear()
    _loaded_date = None


def size() -> int:
    return _cache.size()


async def load_delivery_data() -> int:
    """
    Fetch and parse the NSE delivery CSV, populating the cache.

    Returns the number of symbols cached.
    """
    global _loaded_date
    today = datetime.now(settings.TIMEZONE).strftime("%Y-%m-%d")

    if _loaded_date == today and _cache.size() > 0:
        logger.info("Delivery cache already loaded for today")
        return _cache.size()

    from app.data.nse_fetcher import fetch_delivery_csv

    try:
        data = await fetch_delivery_csv()
    except Exception:
        logger.exception("Failed to fetch delivery CSV")
        return 0

    if not data:
        logger.warning("Delivery CSV returned no data")
        return 0

    for symbol, pct in data.items():
        _cache.set(symbol, pct)

    _loaded_date = today
    logger.info("Delivery cache loaded: %d symbols", _cache.size())
    return _cache.size()
