"""
History routes -- OI history, index candles, last session, EOD report.

Ported from Alpha-Radar-backend/server.js
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException
from kiteconnect import KiteConnect

from app.config import settings
from app.dependencies import get_kite, require_premium
from app.routes.options import INDEX_CONFIG

logger = logging.getLogger("alpha_radar.routes.history")

router = APIRouter(prefix="/api", tags=["history"])

# ── OI History snapshots (in-memory, per day) ────────────────────────────────
_oi_history: dict[str, list[dict]] = {
    "nifty": [], "banknifty": [], "finnifty": [], "midcpnifty": [],
}

# ── Last session cache ───────────────────────────────────────────────────────
LAST_SESSION_FILE = settings.DATA_DIR / "last_session.json"
_last_session_cache: dict | None = None

if LAST_SESSION_FILE.exists():
    try:
        _last_session_cache = json.loads(LAST_SESSION_FILE.read_text())
    except Exception:
        pass


def save_last_session(
    quote_cache: dict[str, dict] | None = None,
    signal_log: list[dict] | None = None,
) -> dict | None:
    """
    Build and persist end-of-day session summary.
    Called from cron or manually via /api/eod-report.
    """
    global _last_session_cache

    try:
        all_stocks: list[dict] = []
        if quote_cache:
            seen: set[str] = set()
            for key, cached in quote_cache.items():
                stocks = cached.get("data", {}).get("stocks", [])
                for s in stocks:
                    sym = s.get("symbol", "")
                    if sym and sym not in seen:
                        seen.add(sym)
                        all_stocks.append(s)

        if not all_stocks:
            return None

        top_gainers = sorted(all_stocks, key=lambda s: s.get("changePct", 0), reverse=True)[:5]
        top_losers = sorted(all_stocks, key=lambda s: s.get("changePct", 0))[:5]
        top_scores = sorted(all_stocks, key=lambda s: s.get("buyingScore", 0), reverse=True)[:5]

        advances = sum(1 for s in all_stocks if s.get("changePct", 0) > 0.05)
        declines = sum(1 for s in all_stocks if s.get("changePct", 0) < -0.05)
        unchanged = sum(1 for s in all_stocks if abs(s.get("changePct", 0)) <= 0.05)
        breakouts = sum(1 for s in all_stocks if s.get("isBreakout"))
        strong_buys = sum(1 for s in all_stocks if s.get("buyingScore", 0) >= 65)

        def _slim(s: dict) -> dict:
            return {
                "symbol": s.get("symbol"),
                "changePct": s.get("changePct"),
                "ltp": s.get("ltp"),
                "buyingScore": s.get("buyingScore"),
                "volRatio": s.get("volRatio"),
            }

        signals = signal_log[:20] if signal_log else []
        slim_signals = [
            {
                "symbol": s.get("symbol"),
                "signalType": s.get("signalType"),
                "price": s.get("price"),
                "score": s.get("score"),
                "time": s.get("time"),
                "volRatio": s.get("volRatio"),
            }
            for s in signals
        ]

        session = {
            "date": datetime.now(settings.TIMEZONE).strftime("%A, %d %b %Y"),
            "savedAt": datetime.now(settings.TIMEZONE).isoformat(),
            "breadth": {
                "advances": advances,
                "declines": declines,
                "unchanged": unchanged,
                "breakouts": breakouts,
                "strongBuys": strong_buys,
                "total": len(all_stocks),
            },
            "topGainers": [_slim(s) for s in top_gainers],
            "topLosers": [_slim(s) for s in top_losers],
            "topScores": [_slim(s) for s in top_scores],
            "signals": slim_signals,
            "signalCount": len(signal_log) if signal_log else 0,
        }

        settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
        LAST_SESSION_FILE.write_text(json.dumps(session))
        _last_session_cache = session
        logger.info("Last session saved")
        return session

    except Exception as exc:
        logger.warning("Could not save last session: %s", exc)
        return None


def append_oi_snapshot(index_key: str, snapshot: dict) -> None:
    """Add an OI snapshot to the in-memory history. Called from cron tasks."""
    if index_key in _oi_history:
        _oi_history[index_key].append(snapshot)
        if len(_oi_history[index_key]) > 30:
            _oi_history[index_key].pop(0)


# ── GET /api/oi-history/{index} ──────────────────────────────────────────────
@router.get("/oi-history/{index}")
async def oi_history(
    index: str,
    user: dict = Depends(require_premium),
) -> dict:
    """Return OI snapshots for the day (premium only)."""
    key = index.lower()
    return {"history": _oi_history.get(key, []), "index": key}


# ── GET /api/index-candles/{index} ───────────────────────────────────────────
@router.get("/index-candles/{index}")
async def index_candles(
    index: str,
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(require_premium),
) -> dict:
    """Return 5-minute historical candles for an index (premium only)."""
    key = index.lower()
    cfg = INDEX_CONFIG.get(key)
    if not cfg or not cfg.get("histToken"):
        raise HTTPException(status_code=400, detail="Unknown index")

    try:
        today = datetime.now(settings.TIMEZONE)
        candles = kite.historical_data(cfg["histToken"], today, today, "5minute")
        return {"candles": candles or [], "index": key}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /api/last-session ────────────────────────────────────────────────────
@router.get("/last-session")
async def last_session(user: dict = Depends(require_premium)) -> dict | None:
    """Return yesterday's saved session data (premium only)."""
    return _last_session_cache


# ── GET /api/eod-report ──────────────────────────────────────────────────────
@router.get("/eod-report")
async def eod_report(
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(require_premium),
) -> dict:
    """Trigger EOD report generation and send to Telegram."""
    try:
        now = datetime.now(settings.TIMEZONE)
        date_str = now.strftime("%A, %d %b %Y")

        # Build report text
        try:
            from app.caches import live_tracker, orb_detector
            orb_state = orb_detector.get_state()
            orb30_list = list(orb_state.get("orb_break_30", set()))[:5]
            orb15_list = [s for s in orb_state.get("orb_break_15", set()) if s not in orb_state.get("orb_break_30", set())][:3]
            surge_list = list(live_tracker.combo_surge)[:3]
        except ImportError:
            orb30_list = []
            orb15_list = []
            surge_list = []

        try:
            from app.routes.signals import _signal_log
            signal_log = _signal_log
        except ImportError:
            signal_log = []

        wins = sum(1 for s in signal_log if s.get("outcome") == "WIN")
        losses = sum(1 for s in signal_log if s.get("outcome") == "LOSS")
        total = len(signal_log)
        win_rate = round(wins / (wins + losses) * 100) if (wins + losses) > 0 else None

        lines = [
            "<b>ALPHA RADAR -- EOD REPORT</b>",
            f"{date_str}",
            "",
            f"<b>ORB30 Breaks ({len(orb30_list)})</b>",
            "\n".join(f"  - {s}" for s in orb30_list) if orb30_list else "  None today",
            "",
            f"<b>ORB15 Breaks ({len(orb15_list)})</b>",
            "\n".join(f"  - {s}" for s in orb15_list) if orb15_list else "  None today",
            "",
            f"<b>Combo Surges ({len(surge_list)})</b>",
            "\n".join(f"  - {s}" for s in surge_list) if surge_list else "  None today",
            "",
            "<b>Signal Log</b>",
            f"  Total: {total} | Win: {wins} | Loss: {losses}",
            f"  Win Rate: {win_rate}%" if win_rate is not None else "  Win Rate: Not yet marked",
        ]
        report = "\n".join(lines)

        # Send to Telegram
        if settings.TELEGRAM_TOKEN and settings.TELEGRAM_CHAT_ID:
            try:
                url = f"https://api.telegram.org/bot{settings.TELEGRAM_TOKEN}/sendMessage"
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await client.post(url, json={
                        "chat_id": settings.TELEGRAM_CHAT_ID,
                        "text": report,
                        "parse_mode": "HTML",
                    })
                logger.info("EOD report sent to Telegram")
            except Exception as exc:
                logger.warning("Telegram send failed: %s", exc)

        return {"success": True, "message": "EOD report sent to Telegram", "preview": report}

    except Exception as exc:
        logger.exception("EOD report error")
        raise HTTPException(status_code=500, detail="Could not generate report")
