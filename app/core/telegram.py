"""
Async Telegram message sender using httpx.

All messages are sent to the chat configured in ``settings.TELEGRAM_CHAT_ID``.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

from app.config import settings

logger = logging.getLogger("alpha_radar.telegram")

_BASE_URL = "https://api.telegram.org/bot{token}/sendMessage"


async def send_message(text: str, parse_mode: str = "HTML") -> Optional[dict]:
    """
    Send a text message to the configured Telegram chat.

    Returns the Telegram API response dict, or ``None`` on failure.
    """
    if not settings.TELEGRAM_TOKEN or not settings.TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured -- message not sent")
        return None

    url = _BASE_URL.format(token=settings.TELEGRAM_TOKEN)
    payload = {
        "chat_id": settings.TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("ok"):
                logger.error("Telegram API returned not-ok: %s", data)
            return data
    except httpx.HTTPError as exc:
        logger.error("Telegram send failed: %s", exc)
        return None


# ------------------------------------------------------------------
# Pre-formatted alert helpers
# ------------------------------------------------------------------

async def send_combo_surge_alert(
    symbol: str,
    price: float,
    sector: str,
    vol_ratio: float,
) -> Optional[dict]:
    """Send a bullish volume + PDH surge combo alert."""
    text = (
        f"<b>COMBO SURGE ALERT</b>\n\n"
        f"<b>{symbol}</b> ({sector})\n"
        f"Price: {price:,.2f}\n"
        f"Volume Ratio: {vol_ratio:.1f}x avg\n\n"
        f"#ComboSurge #Bull"
    )
    return await send_message(text)


async def send_combo_sell_alert(
    symbol: str,
    price: float,
    sector: str,
    vol_ratio: float,
) -> Optional[dict]:
    """Send a bearish volume + PDL break combo alert."""
    text = (
        f"<b>COMBO SELL ALERT</b>\n\n"
        f"<b>{symbol}</b> ({sector})\n"
        f"Price: {price:,.2f}\n"
        f"Volume Ratio: {vol_ratio:.1f}x avg\n\n"
        f"#ComboSell #Bear"
    )
    return await send_message(text)


async def send_orb_break_alert(
    symbol: str,
    orb_type: str,
    price: float,
    direction: str,
) -> Optional[dict]:
    """Send an Opening Range Breakout alert."""
    emoji = "\u2b06\ufe0f" if direction == "up" else "\u2b07\ufe0f"
    text = (
        f"<b>ORB BREAK {emoji}</b>\n\n"
        f"<b>{symbol}</b>\n"
        f"ORB Type: {orb_type}\n"
        f"Price: {price:,.2f}\n"
        f"Direction: {direction.upper()}\n\n"
        f"#ORB #{orb_type.upper()}"
    )
    return await send_message(text)


async def send_eod_report(report_data: dict[str, Any]) -> Optional[dict]:
    """
    Send an end-of-day summary report.

    ``report_data`` is expected to contain keys like ``top_gainers``,
    ``top_losers``, ``signals_fired``, ``index_close``, etc.
    """
    lines = ["<b>END OF DAY REPORT</b>\n"]

    # Index summary
    indices = report_data.get("indices", {})
    if indices:
        lines.append("<b>Indices:</b>")
        for name, info in indices.items():
            change = info.get("change", 0)
            pct = info.get("changePct", 0)
            sign = "+" if change >= 0 else ""
            lines.append(f"  {name}: {info.get('close', 0):,.2f} ({sign}{pct:.2f}%)")
        lines.append("")

    # Signals fired
    signals_count = report_data.get("signals_fired", 0)
    lines.append(f"<b>Signals Fired:</b> {signals_count}")

    # Top gainers
    gainers = report_data.get("top_gainers", [])
    if gainers:
        lines.append("\n<b>Top Gainers:</b>")
        for g in gainers[:5]:
            lines.append(f"  {g['symbol']}: +{g['changePct']:.2f}%")

    # Top losers
    losers = report_data.get("top_losers", [])
    if losers:
        lines.append("\n<b>Top Losers:</b>")
        for lo in losers[:5]:
            lines.append(f"  {lo['symbol']}: {lo['changePct']:.2f}%")

    lines.append("\n#EOD #AlphaRadar")
    return await send_message("\n".join(lines))
