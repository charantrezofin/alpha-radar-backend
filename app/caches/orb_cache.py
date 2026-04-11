"""
Opening Range Breakout (ORB) cache.

Loads the first 15-minute and 30-minute candle data after market open
to establish the ORB high/low levels for each symbol.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from kiteconnect import KiteConnect

from app.config import settings
from app.core.cache import Cache

logger = logging.getLogger("alpha_radar.cache.orb")


@dataclass
class ORBState:
    """ORB levels for a single symbol."""
    orb_high_15: Optional[float] = None
    orb_low_15: Optional[float] = None
    orb_high_30: Optional[float] = None
    orb_low_30: Optional[float] = None
    loaded_at_15: Optional[str] = None
    loaded_at_30: Optional[str] = None


_cache = Cache(name="orb", ttl=None)
_cache_date: Optional[str] = None


def get_orb(symbol: str, orb_type: str = "15") -> Optional[ORBState]:
    """
    Return ORB state for *symbol*.

    ``orb_type`` is informational only -- the full ORBState is always returned.
    """
    return _cache.get(symbol)


def get_all() -> dict[str, ORBState]:
    return _cache.get_all()


def clear() -> None:
    global _cache_date
    _cache.clear()
    _cache_date = None


def size() -> int:
    return _cache.size()


def _ensure_today() -> str:
    global _cache_date
    today = datetime.now(settings.TIMEZONE).strftime("%Y-%m-%d")
    if _cache_date != today:
        _cache.clear()
        _cache_date = today
    return today


def load_orb15(kite: KiteConnect, symbols: list[str]) -> int:
    """
    Load the 15-min ORB (09:15-09:30 candle) for each symbol.

    Returns the number of symbols successfully loaded.
    """
    today = _ensure_today()
    logger.info("Loading ORB-15 cache (%d symbols)...", len(symbols))

    try:
        instruments = kite.instruments("NSE")
    except Exception:
        logger.exception("Could not fetch instruments for ORB cache")
        return 0

    token_map: dict[str, int] = {}
    for inst in instruments:
        sym = inst.get("tradingsymbol", "")
        if sym in symbols:
            token_map[sym] = inst["instrument_token"]

    from_dt = f"{today} 09:15:00"
    to_dt = f"{today} 09:30:00"
    now_str = datetime.now(settings.TIMEZONE).strftime("%H:%M:%S")

    fetched = 0
    for i, symbol in enumerate(symbols):
        token = token_map.get(symbol)
        if not token:
            continue
        try:
            candles = kite.historical_data(
                instrument_token=token,
                from_date=from_dt,
                to_date=to_dt,
                interval="15minute",
            )
            if not candles:
                continue

            state: ORBState = _cache.get(symbol) or ORBState()
            state.orb_high_15 = candles[0]["high"]
            state.orb_low_15 = candles[0]["low"]
            state.loaded_at_15 = now_str
            _cache.set(symbol, state)
            fetched += 1
        except Exception:
            logger.debug("Failed to fetch ORB-15 for %s", symbol)

        if (i + 1) % 50 == 0:
            logger.info("  ORB-15: %d/%d processed...", i + 1, len(symbols))

    logger.info("ORB-15 cache loaded: %d symbols", fetched)
    return fetched


def load_orb30(kite: KiteConnect, symbols: list[str]) -> int:
    """
    Load the 30-min ORB (09:15-09:45, combining two 15-min candles).

    Returns the number of symbols successfully loaded.
    """
    today = _ensure_today()
    logger.info("Loading ORB-30 cache (%d symbols)...", len(symbols))

    try:
        instruments = kite.instruments("NSE")
    except Exception:
        logger.exception("Could not fetch instruments for ORB cache")
        return 0

    token_map: dict[str, int] = {}
    for inst in instruments:
        sym = inst.get("tradingsymbol", "")
        if sym in symbols:
            token_map[sym] = inst["instrument_token"]

    from_dt = f"{today} 09:15:00"
    to_dt = f"{today} 09:45:00"
    now_str = datetime.now(settings.TIMEZONE).strftime("%H:%M:%S")

    fetched = 0
    for i, symbol in enumerate(symbols):
        token = token_map.get(symbol)
        if not token:
            continue
        try:
            candles = kite.historical_data(
                instrument_token=token,
                from_date=from_dt,
                to_date=to_dt,
                interval="15minute",
            )
            if not candles:
                continue

            state: ORBState = _cache.get(symbol) or ORBState()

            if len(candles) >= 2:
                state.orb_high_30 = max(candles[0]["high"], candles[1]["high"])
                state.orb_low_30 = min(candles[0]["low"], candles[1]["low"])
            else:
                state.orb_high_30 = candles[0]["high"]
                state.orb_low_30 = candles[0]["low"]

            state.loaded_at_30 = now_str
            _cache.set(symbol, state)
            fetched += 1
        except Exception:
            logger.debug("Failed to fetch ORB-30 for %s", symbol)

        if (i + 1) % 50 == 0:
            logger.info("  ORB-30: %d/%d processed...", i + 1, len(symbols))

    logger.info("ORB-30 cache loaded: %d symbols", fetched)
    return fetched
