"""
Kite WebSocket ticker wrapper.

Manages the KiteTicker connection, auto-subscribes to index tokens on
connect, and forwards ticks to a registered callback.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

from kiteconnect import KiteTicker as _KiteTicker

from app.config import settings
from app.core.kite_client import kite_state

logger = logging.getLogger("alpha_radar.ticker")

# Well-known index instrument tokens
INDEX_TOKENS: list[int] = [
    256265,   # NIFTY 50
    260105,   # BANK NIFTY
    265,      # SENSEX
    257801,   # FINNIFTY
]

# Module-level state
_ticker: Optional[_KiteTicker] = None
_subscribed_tokens: set[int] = set()
_on_tick_callback: Optional[Callable] = None


def init_ticker(on_tick: Callable[[list[dict]], None]) -> _KiteTicker:
    """
    Create and connect the KiteTicker WebSocket.

    Parameters
    ----------
    on_tick : callable
        Function that receives a list of tick dicts on every tick event.

    Returns
    -------
    KiteTicker instance (already connecting).
    """
    global _ticker, _on_tick_callback

    _on_tick_callback = on_tick

    access_token = kite_state.access_token
    if not access_token:
        raise RuntimeError("Cannot start ticker: Kite access token is not set")

    _ticker = _KiteTicker(
        api_key=settings.KITE_API_KEY,
        access_token=access_token,
    )

    _ticker.on_connect = _on_connect
    _ticker.on_ticks = _on_ticks
    _ticker.on_close = _on_close
    _ticker.on_error = _on_error
    _ticker.on_reconnect = _on_reconnect

    # Pre-register index tokens so they are subscribed immediately on connect
    for t in INDEX_TOKENS:
        _subscribed_tokens.add(t)

    logger.info("Starting KiteTicker (pre-subscribed tokens: %d)", len(_subscribed_tokens))
    _ticker.connect(threaded=True)
    return _ticker


# ------------------------------------------------------------------
# Public subscribe / unsubscribe
# ------------------------------------------------------------------

def subscribe(tokens: list[int]) -> None:
    """Subscribe to additional instrument tokens."""
    for t in tokens:
        _subscribed_tokens.add(t)
    if _ticker and _is_connected():
        _ticker.subscribe(tokens)
        _ticker.set_mode(_ticker.MODE_FULL, tokens)
        logger.info("Subscribed %d tokens (total: %d)", len(tokens), len(_subscribed_tokens))


def unsubscribe(tokens: list[int]) -> None:
    """Unsubscribe from instrument tokens."""
    for t in tokens:
        _subscribed_tokens.discard(t)
    if _ticker and _is_connected():
        _ticker.unsubscribe(tokens)
        logger.info("Unsubscribed %d tokens (total: %d)", len(tokens), len(_subscribed_tokens))


def get_subscribed_tokens() -> list[int]:
    """Return a copy of all currently tracked tokens."""
    return list(_subscribed_tokens)


def stop_ticker() -> None:
    """Gracefully close the ticker connection."""
    global _ticker
    if _ticker:
        try:
            _ticker.close()
        except Exception:
            pass
        _ticker = None
        logger.info("KiteTicker stopped")


# ------------------------------------------------------------------
# Internal callbacks
# ------------------------------------------------------------------

def _is_connected() -> bool:
    return _ticker is not None and getattr(_ticker, "is_connected", lambda: False)()


def _on_connect(ws, response):  # noqa: ARG001
    logger.info("Connected to Kite WebSocket")
    if _subscribed_tokens:
        tokens = list(_subscribed_tokens)
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)
        logger.info("Auto-subscribed %d tokens on connect", len(tokens))


def _on_ticks(ws, ticks):  # noqa: ARG001
    if _on_tick_callback:
        try:
            _on_tick_callback(ticks)
        except Exception:
            logger.exception("Error in tick callback")


def _on_close(ws, code, reason):  # noqa: ARG001
    logger.warning("Kite WebSocket closed (code=%s reason=%s)", code, reason)


def _on_error(ws, code, reason):  # noqa: ARG001
    logger.error("Kite WebSocket error (code=%s reason=%s)", code, reason)


def _on_reconnect(ws, attempts_count):  # noqa: ARG001
    logger.info("Kite WebSocket reconnecting... attempt %d", attempts_count)
