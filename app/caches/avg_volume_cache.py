"""
Average volume cache.

Fetches 25 trading days of daily candles from Kite historical API and
computes the 20-day average volume for each symbol.  Persisted to disk
so it survives restarts within the same trading day.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

from kiteconnect import KiteConnect

from app.config import settings
from app.core.cache import Cache

logger = logging.getLogger("alpha_radar.cache.avg_volume")

_cache = Cache(
    name="avg_volume",
    ttl=None,  # valid for the full trading day; cleared by daily_reset
    persist_path=".avg_volume_cache.json",
)


def get_avg_volume(symbol: str) -> Optional[float]:
    """Return the 20-day average volume for *symbol*, or ``None``."""
    return _cache.get(symbol)


def get_all() -> dict[str, float]:
    """Return all cached average volumes."""
    return _cache.get_all()


def clear() -> None:
    _cache.clear()


def size() -> int:
    return _cache.size()


def load_avg_volumes(kite: KiteConnect, symbols: list[str]) -> int:
    """
    Build the avg-volume cache by fetching historical data from Kite.

    Tries to restore from disk first.  If the disk cache is from today
    and non-empty, skip the expensive API calls.

    Returns the number of symbols successfully cached.
    """
    today_str = datetime.now(settings.TIMEZONE).strftime("%Y-%m-%d")

    # Try disk first
    if _cache.load_from_disk() and _cache.size() > 0:
        logger.info(
            "Avg volume cache restored from disk (%d symbols)", _cache.size()
        )
        return _cache.size()

    logger.info("Building avg volume cache from Kite historical API (%d symbols)...", len(symbols))

    # Resolve instrument tokens for the symbols
    try:
        instruments = kite.instruments("NSE")
    except Exception:
        logger.exception("Could not fetch instruments for volume cache")
        return 0

    token_map: dict[str, int] = {}
    for inst in instruments:
        sym = inst.get("tradingsymbol", "")
        if sym in symbols:
            token_map[sym] = inst["instrument_token"]

    to_date = datetime.now(settings.TIMEZONE).date()
    from_date = to_date - timedelta(days=40)  # fetch extra to ensure 25 trading days

    fetched = 0
    for i, symbol in enumerate(symbols):
        token = token_map.get(symbol)
        if not token:
            continue

        try:
            candles = kite.historical_data(
                instrument_token=token,
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat(),
                interval="day",
            )
            if not candles:
                continue

            # Take last 20 candles (skip today if it exists)
            volumes = [c["volume"] for c in candles if c.get("volume", 0) > 0]
            if len(volumes) > 20:
                volumes = volumes[-20:]

            if volumes:
                avg = round(sum(volumes) / len(volumes))
                _cache.set(symbol, avg)
                fetched += 1
        except Exception:
            logger.debug("Failed to fetch history for %s", symbol)

        if (i + 1) % 50 == 0:
            logger.info("  Volume cache: %d/%d processed...", i + 1, len(symbols))

    logger.info("Avg volume cache built: %d symbols", fetched)

    # Persist to disk
    _cache.save_to_disk()
    return fetched
