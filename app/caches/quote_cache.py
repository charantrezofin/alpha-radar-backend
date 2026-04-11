"""
Real-time quote cache with a 15-second TTL.

Stores the latest Kite quote data keyed by symbol.
"""

from __future__ import annotations

import logging
from typing import Optional

from app.core.cache import Cache
from app.models.common import SymbolQuote

logger = logging.getLogger("alpha_radar.cache.quote")

_cache = Cache(name="quote", ttl=15)  # 15-second TTL


def set_quotes(symbol_quotes: dict[str, dict]) -> None:
    """
    Bulk-update the quote cache.

    ``symbol_quotes`` maps symbol names to raw Kite quote dicts.
    """
    for symbol, raw in symbol_quotes.items():
        _cache.set(symbol, raw)


def get_quote(symbol: str) -> Optional[dict]:
    """Return the cached quote for *symbol*, or ``None`` if expired/missing."""
    return _cache.get(symbol)


def get_all() -> dict[str, dict]:
    """Return all non-expired cached quotes."""
    return _cache.get_all()


def clear() -> None:
    _cache.clear()


def size() -> int:
    return _cache.size()
