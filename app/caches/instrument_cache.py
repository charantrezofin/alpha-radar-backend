"""
Instrument master cache.

Loads the NSE, NFO, and BFO instrument lists from Kite and provides
fast lookup by symbol or token.  1-hour TTL so it refreshes during long
trading sessions.
"""

from __future__ import annotations

import logging
from typing import Optional

from kiteconnect import KiteConnect

from app.core.cache import Cache

logger = logging.getLogger("alpha_radar.cache.instruments")

_cache = Cache(name="instruments", ttl=3600)  # 1-hour TTL

# Internal cache keys
_KEY_NSE = "_nse"
_KEY_NFO = "_nfo"
_KEY_BFO = "_bfo"
_KEY_TOKEN_MAP = "_token_map"


def load_instruments(kite: KiteConnect) -> int:
    """
    Fetch NSE + NFO + BFO instruments from Kite and populate the cache.

    Returns the total number of instruments loaded.
    """
    total = 0

    for exchange, key in [("NSE", _KEY_NSE), ("NFO", _KEY_NFO), ("BFO", _KEY_BFO)]:
        try:
            instruments = kite.instruments(exchange)
            _cache.set(key, instruments)
            total += len(instruments)
            logger.info("Loaded %d instruments from %s", len(instruments), exchange)
        except Exception:
            logger.exception("Failed to load %s instruments", exchange)
            _cache.set(key, [])

    # Build token lookup map:  tradingsymbol -> instrument_token
    token_map: dict[str, int] = {}
    for key in (_KEY_NSE, _KEY_NFO, _KEY_BFO):
        for inst in (_cache.get(key) or []):
            sym = inst.get("tradingsymbol", "")
            if sym:
                token_map[sym] = inst["instrument_token"]

    _cache.set(_KEY_TOKEN_MAP, token_map)
    logger.info("Instrument token map built: %d entries", len(token_map))
    return total


def get_nse_instruments() -> list[dict]:
    """Return all NSE instruments."""
    return _cache.get(_KEY_NSE) or []


def get_nfo_instruments() -> list[dict]:
    """Return all NFO (F&O) instruments."""
    return _cache.get(_KEY_NFO) or []


def get_bfo_instruments() -> list[dict]:
    """Return all BFO (BSE F&O) instruments."""
    return _cache.get(_KEY_BFO) or []


def get_instrument_token(symbol: str) -> Optional[int]:
    """Look up the instrument token for a trading symbol."""
    token_map = _cache.get(_KEY_TOKEN_MAP)
    if token_map is None:
        return None
    return token_map.get(symbol)


def get_all_tokens() -> dict[str, int]:
    """Return the full symbol -> token mapping."""
    return _cache.get(_KEY_TOKEN_MAP) or {}


def clear() -> None:
    _cache.clear()


def size() -> int:
    """Return total instruments across all exchanges."""
    total = 0
    for key in (_KEY_NSE, _KEY_NFO, _KEY_BFO):
        data = _cache.get(key)
        if data:
            total += len(data)
    return total
