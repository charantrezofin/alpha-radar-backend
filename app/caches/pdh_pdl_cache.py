"""
Previous Day High / Previous Day Low cache.

Fetches yesterday's daily candle for each symbol from Kite and stores
the high (PDH) and low (PDL) values.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

from kiteconnect import KiteConnect

from app.config import settings
from app.core.cache import Cache

logger = logging.getLogger("alpha_radar.cache.pdh_pdl")

_pdh_cache = Cache(name="pdh", ttl=None)
_pdl_cache = Cache(name="pdl", ttl=None)

_loaded_date: Optional[str] = None


def get_pdh(symbol: str) -> Optional[float]:
    """Return Previous Day High for *symbol*."""
    return _pdh_cache.get(symbol)


def get_pdl(symbol: str) -> Optional[float]:
    """Return Previous Day Low for *symbol*."""
    return _pdl_cache.get(symbol)


def get_all_pdh() -> dict[str, float]:
    return _pdh_cache.get_all()


def get_all_pdl() -> dict[str, float]:
    return _pdl_cache.get_all()


def clear() -> None:
    global _loaded_date
    _pdh_cache.clear()
    _pdl_cache.clear()
    _loaded_date = None


def size() -> int:
    return _pdh_cache.size()


def load_pdh_pdl(kite: KiteConnect, symbols: list[str]) -> int:
    """
    Fetch yesterday's candle for each symbol and cache PDH / PDL.

    Returns the number of symbols successfully cached.
    """
    global _loaded_date
    today = datetime.now(settings.TIMEZONE).strftime("%Y-%m-%d")

    if _loaded_date == today and _pdh_cache.size() > 0:
        logger.info("PDH/PDL cache already loaded for today")
        return _pdh_cache.size()

    logger.info("Loading PDH/PDL cache (%d symbols)...", len(symbols))

    try:
        instruments = kite.instruments("NSE")
    except Exception:
        logger.exception("Could not fetch instruments for PDH cache")
        return 0

    token_map: dict[str, int] = {}
    for inst in instruments:
        sym = inst.get("tradingsymbol", "")
        if sym in symbols:
            token_map[sym] = inst["instrument_token"]

    to_date = datetime.now(settings.TIMEZONE).date()
    from_date = to_date - timedelta(days=10)  # enough to find the last trading day

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

            # The last candle before today
            prev_candles = [
                c for c in candles
                if str(c["date"])[:10] < today
            ]
            if not prev_candles:
                continue

            last = prev_candles[-1]
            _pdh_cache.set(symbol, last["high"])
            _pdl_cache.set(symbol, last["low"])
            fetched += 1
        except Exception:
            logger.debug("Failed to fetch PDH for %s", symbol)

        if (i + 1) % 50 == 0:
            logger.info("  PDH cache: %d/%d processed...", i + 1, len(symbols))

    _loaded_date = today
    logger.info("PDH/PDL cache loaded: %d symbols", fetched)
    return fetched
