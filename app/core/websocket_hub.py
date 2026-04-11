"""
WebSocket hub for broadcasting real-time tick data to browser clients.

Mirrors the tradingdesk client-hub.ts behaviour:
- All clients receive index / equity ticks.
- Options ticks are sent only to clients that have subscribed (ref-counted).
- Latest ticks are cached for instant delivery on new connections.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import WebSocket

logger = logging.getLogger("alpha_radar.ws_hub")


class WebSocketHub:
    """Manages connected WebSocket clients and tick broadcasting."""

    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        # ws -> set of options tokens the client has subscribed to
        self._client_subscriptions: dict[WebSocket, set[int]] = {}
        # token -> number of clients subscribed to it
        self._options_ref_count: dict[int, int] = {}
        # token -> latest formatted tick
        self._latest_ticks: dict[int, dict] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self, ws: WebSocket) -> None:
        """Accept and register a new client."""
        await ws.accept()
        async with self._lock:
            self._clients.add(ws)
            self._client_subscriptions[ws] = set()

        count = len(self._clients)
        logger.info("WS client connected (%d total)", count)

        # Send welcome
        await self._send_json(ws, {"type": "connected", "data": {"clients": count}})

        # Send cached ticks immediately
        if self._latest_ticks:
            await self._send_json(
                ws,
                {"type": "tick", "data": list(self._latest_ticks.values())},
            )

    async def disconnect(self, ws: WebSocket) -> None:
        """Clean up when a client disconnects."""
        async with self._lock:
            # Clean up option subscriptions
            subs = self._client_subscriptions.pop(ws, set())
            tokens_to_remove: list[int] = []
            for t in subs:
                count = self._options_ref_count.get(t, 1) - 1
                if count <= 0:
                    self._options_ref_count.pop(t, None)
                    tokens_to_remove.append(t)
                else:
                    self._options_ref_count[t] = count

            self._clients.discard(ws)

        if tokens_to_remove:
            # Lazy import to avoid circular dependency
            from app.core.kite_ticker import unsubscribe
            unsubscribe(tokens_to_remove)
            logger.info("Auto-unsubscribed %d orphan option tokens", len(tokens_to_remove))

        logger.info("WS client disconnected (%d total)", len(self._clients))

    # ------------------------------------------------------------------
    # Client message handling
    # ------------------------------------------------------------------

    async def handle_message(self, ws: WebSocket, raw: str) -> None:
        """Process an incoming message from a client."""
        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return

        msg_type = msg.get("type")

        if msg_type == "subscribe_options" and isinstance(msg.get("tokens"), list):
            await self._subscribe_options(ws, msg["tokens"])
        elif msg_type == "unsubscribe_options" and isinstance(msg.get("tokens"), list):
            await self._unsubscribe_options(ws, msg["tokens"])
        elif msg_type == "ping":
            await self._send_json(ws, {"type": "pong"})

    # ------------------------------------------------------------------
    # Options subscription (ref-counted)
    # ------------------------------------------------------------------

    async def _subscribe_options(self, ws: WebSocket, tokens: list[int]) -> None:
        async with self._lock:
            client_subs = self._client_subscriptions.get(ws)
            if client_subs is None:
                return

            new_tokens: list[int] = []
            for t in tokens:
                client_subs.add(t)
                count = self._options_ref_count.get(t, 0) + 1
                self._options_ref_count[t] = count
                if count == 1:
                    new_tokens.append(t)

        if new_tokens:
            from app.core.kite_ticker import subscribe
            subscribe(new_tokens)
            logger.info(
                "Subscribed %d new option tokens (total options: %d)",
                len(new_tokens),
                len(self._options_ref_count),
            )

        # Send cached ticks for requested tokens
        cached = [self._latest_ticks[t] for t in tokens if t in self._latest_ticks]
        if cached:
            await self._send_json(ws, {"type": "options_tick", "data": cached})

    async def _unsubscribe_options(self, ws: WebSocket, tokens: list[int]) -> None:
        async with self._lock:
            client_subs = self._client_subscriptions.get(ws)
            if client_subs is None:
                return

            tokens_to_remove: list[int] = []
            for t in tokens:
                client_subs.discard(t)
                count = self._options_ref_count.get(t, 1) - 1
                if count <= 0:
                    self._options_ref_count.pop(t, None)
                    tokens_to_remove.append(t)
                else:
                    self._options_ref_count[t] = count

        if tokens_to_remove:
            from app.core.kite_ticker import unsubscribe
            unsubscribe(tokens_to_remove)

    # ------------------------------------------------------------------
    # Broadcasting
    # ------------------------------------------------------------------

    def broadcast_ticks(self, ticks: list[dict]) -> None:
        """
        Format raw Kite ticks and broadcast.

        Called from the synchronous ticker callback thread -- schedules the
        async broadcast on the running event loop.
        """
        if not self._clients:
            return

        formatted = [self._format_tick(t) for t in ticks]

        # Cache latest
        for t in formatted:
            self._latest_ticks[t["token"]] = t

        # Split options vs regular
        options_tokens = set(self._options_ref_count.keys())
        options_ticks = [t for t in formatted if t["token"] in options_tokens]
        regular_ticks = [t for t in formatted if t["token"] not in options_tokens]

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._async_broadcast(regular_ticks, options_ticks), loop
                )
            else:
                loop.run_until_complete(
                    self._async_broadcast(regular_ticks, options_ticks)
                )
        except RuntimeError:
            # No running event loop -- best-effort skip
            logger.debug("No event loop available for broadcast")

    async def _async_broadcast(
        self,
        regular_ticks: list[dict],
        options_ticks: list[dict],
    ) -> None:
        stale: list[WebSocket] = []

        # Broadcast regular ticks to ALL clients
        if regular_ticks:
            message = json.dumps({"type": "tick", "data": regular_ticks})
            for ws in list(self._clients):
                try:
                    await ws.send_text(message)
                except Exception:
                    stale.append(ws)

        # Broadcast options ticks only to subscribed clients
        if options_ticks:
            for ws, subs in list(self._client_subscriptions.items()):
                if not subs:
                    continue
                relevant = [t for t in options_ticks if t["token"] in subs]
                if relevant:
                    try:
                        await ws.send_text(
                            json.dumps({"type": "options_tick", "data": relevant})
                        )
                    except Exception:
                        if ws not in stale:
                            stale.append(ws)

        # Clean up stale connections
        for ws in stale:
            await self.disconnect(ws)

    # ------------------------------------------------------------------
    # Tick helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _format_tick(t: dict) -> dict:
        """Transform a raw Kite tick dict into the browser-friendly format."""
        ohlc = t.get("ohlc", {})
        close_price = ohlc.get("close", 0)
        last_price = t.get("last_price", 0)
        change = last_price - close_price if close_price else 0
        change_pct = (change / close_price * 100) if close_price else 0

        ts = t.get("exchange_timestamp")
        if ts is not None:
            try:
                ts = ts.timestamp() * 1000  # ms epoch
            except (AttributeError, TypeError):
                ts = None

        return {
            "token": t.get("instrument_token", 0),
            "tradingSymbol": t.get("tradingsymbol", ""),
            "last": last_price,
            "open": ohlc.get("open", 0),
            "high": ohlc.get("high", 0),
            "low": ohlc.get("low", 0),
            "close": close_price,
            "volume": t.get("volume_traded", t.get("volume", 0)),
            "change": round(change, 2),
            "changePercent": round(change_pct, 2),
            "oi": t.get("oi", 0),
            "oiDayHigh": t.get("oi_day_high", 0),
            "oiDayLow": t.get("oi_day_low", 0),
            "timestamp": ts,
        }

    def get_latest_tick(self, token: int) -> dict | None:
        return self._latest_ticks.get(token)

    def get_all_latest_ticks(self) -> list[dict]:
        return list(self._latest_ticks.values())

    @property
    def client_count(self) -> int:
        return len(self._clients)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _send_json(ws: WebSocket, data: Any) -> None:
        try:
            await ws.send_json(data)
        except Exception:
            pass


# Module-level singleton
ws_hub = WebSocketHub()
