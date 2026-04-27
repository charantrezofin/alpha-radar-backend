"""
Market Pulse route -- real-time signal classification for stock screening.

Ported from tradingdesk/apps/gateway/src/routes/pulse.routes.ts
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from kiteconnect import KiteConnect
from pydantic import BaseModel

from app.config import settings
from app.core.session_cache import save_session, load_session, is_market_hours
from app.dependencies import get_kite
from app.engines import compute_pulse

logger = logging.getLogger("alpha_radar.routes.pulse")

router = APIRouter(prefix="/api", tags=["pulse"])

# ── Daily stats cache ────────────────────────────────────────────────────────
_stats_cache: dict[str, dict] = {}
_stats_cache_date: str = ""


class PulseBody(BaseModel):
    symbols: list[str]


def _ensure_daily_stats(kite: KiteConnect, symbols: list[str]) -> None:
    """Compute daily stats (avg volume, avg range, PDH/PDL) once per day."""
    global _stats_cache, _stats_cache_date

    today = datetime.now(settings.TIMEZONE).strftime("%Y-%m-%d")
    if _stats_cache_date == today and len(_stats_cache) > 0:
        return

    logger.info("[pulse] Computing daily stats for %d stocks...", len(symbols))
    new_cache: dict[str, dict] = {}

    batch_size = 5
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i: i + batch_size]
        for sym in batch:
            try:
                ltp_data = kite.ltp([f"NSE:{sym}"])
                entry = ltp_data.get(f"NSE:{sym}")
                if not entry:
                    continue

                token = entry["instrument_token"]
                to_date = datetime.now(settings.TIMEZONE)
                from_date = to_date - timedelta(days=20)

                bars = kite.historical_data(token, from_date, to_date, "day")
                if not bars or len(bars) < 5:
                    continue

                # Last 10 completed bars (exclude today)
                completed = bars[-11:-1] if len(bars) >= 11 else bars[:-1]
                if len(completed) < 5:
                    continue

                avg_volume = sum(b["volume"] for b in completed) / len(completed)
                ranges = [b["high"] - b["low"] for b in completed]
                avg_range = sum(ranges) / len(ranges)
                avg_close = sum(b["close"] for b in completed) / len(completed)
                avg_range_pct = (avg_range / avg_close * 100) if avg_close > 0 else 0

                prev_day = completed[-1]
                new_cache[sym] = {
                    "avg_volume": avg_volume,
                    "avg_range": avg_range,
                    "avg_range_pct": avg_range_pct,
                    "pdh": prev_day["high"],
                    "pdl": prev_day["low"],
                    "pdc": prev_day["close"],
                }
            except Exception:
                pass

        if i + batch_size < len(symbols):
            time.sleep(0.35)

    _stats_cache = new_cache
    _stats_cache_date = today
    logger.info("[pulse] Daily stats cached for %d stocks", len(new_cache))


# ── POST /api/pulse ──────────────────────────────────────────────────────────
@router.post("/pulse")
async def market_pulse(
    body: PulseBody,
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """Compute pulse for each symbol, return beacon + boost lists."""
    if not body.symbols:
        raise HTTPException(status_code=400, detail="symbols array required")

    try:
        _ensure_daily_stats(kite, body.symbols)

        # Fetch live quotes
        instruments = [f"NSE:{s}" for s in body.symbols]
        all_quotes: dict = {}
        for i in range(0, len(instruments), 200):
            q = kite.quote(instruments[i: i + 200])
            all_quotes.update(q)
            if i + 200 < len(instruments):
                time.sleep(0.35)

        results: list[dict] = []
        for sym in body.symbols:
            q = all_quotes.get(f"NSE:{sym}")
            if not q or not q.get("last_price"):
                continue

            stats = _stats_cache.get(sym)
            ltp = q["last_price"]
            prev_close = q.get("ohlc", {}).get("close", ltp) or ltp
            change_pct = ((ltp - prev_close) / prev_close * 100) if prev_close > 0 else 0
            today_high = q.get("ohlc", {}).get("high", ltp) or ltp
            today_low = q.get("ohlc", {}).get("low", ltp) or ltp
            today_range = today_high - today_low
            today_volume = q.get("volume", 0) or 0
            open_ = q.get("ohlc", {}).get("open", ltp) or ltp

            # R.Factor
            r_factor = (today_range / stats["avg_range"]) if stats and stats["avg_range"] > 0 else (1 if today_range > 0 else 0)
            vol_ratio = (today_volume / stats["avg_volume"]) if stats and stats["avg_volume"] > 0 else 1
            signal_pct = abs(change_pct) * (max(vol_ratio, 0.5) ** 0.5)

            pdh_breakout = stats and ltp > stats["pdh"] and change_pct > 0
            pdl_breakdown = stats and ltp < stats["pdl"] and change_pct < 0

            # Classification
            signal = "NEUTRAL"
            if change_pct > 1 and vol_ratio > 0.8:
                signal = "BULL"
            elif change_pct > 0.5 and vol_ratio > 1.2:
                signal = "BULL"
            elif change_pct > 2:
                signal = "BULL"
            elif change_pct < -1 and vol_ratio > 0.8:
                signal = "BEAR"
            elif change_pct < -0.5 and vol_ratio > 1.2:
                signal = "BEAR"
            elif change_pct < -2:
                signal = "BEAR"

            if abs(change_pct) < 0.3 and r_factor < 0.5:
                continue

            results.append({
                "symbol": sym,
                "ltp": ltp,
                "open": open_,
                "high": today_high,
                "low": today_low,
                "prevClose": prev_close,
                "changePct": round(change_pct, 2),
                "change": round(ltp - prev_close, 2),
                "volume": today_volume,
                "volRatio": round(vol_ratio, 2),
                "rFactor": round(r_factor, 2),
                "signalPct": round(signal_pct, 2),
                "signal": signal,
                "pdhBreakout": bool(pdh_breakout),
                "pdlBreakdown": bool(pdl_breakdown),
                "pdh": stats["pdh"] if stats else 0,
                "pdl": stats["pdl"] if stats else 0,
            })

            # Validation tracker — fire on PDH breakout or PDL breakdown only
            # (the dedupe inside log_signal_fire handles repeat firing)
            try:
                from app.services.signal_validator import log_signal_fire, compute_market_context
                if pdh_breakout and signal == "BULL":
                    log_signal_fire(
                        symbol=sym, signal_type="PULSE_BULL",
                        trigger_price=ltp, strength=round(signal_pct, 2),
                        direction="BULLISH",
                        confidence="STRONG" if signal_pct > 3 else "MODERATE",
                        category="stock",
                        metadata={
                            "change_pct": round(change_pct, 2),
                            "vol_ratio": round(vol_ratio, 2),
                            "r_factor": round(r_factor, 2),
                            "pdh": stats["pdh"] if stats else 0,
                        },
                        context=compute_market_context(),
                    )
                elif pdl_breakdown and signal == "BEAR":
                    log_signal_fire(
                        symbol=sym, signal_type="PULSE_BEAR",
                        trigger_price=ltp, strength=round(signal_pct, 2),
                        direction="BEARISH",
                        confidence="STRONG" if signal_pct > 3 else "MODERATE",
                        category="stock",
                        metadata={
                            "change_pct": round(change_pct, 2),
                            "vol_ratio": round(vol_ratio, 2),
                            "r_factor": round(r_factor, 2),
                            "pdl": stats["pdl"] if stats else 0,
                        },
                        context=compute_market_context(),
                    )
            except Exception:
                logger.debug("[pulse] signal_validator log failed for %s", sym, exc_info=True)

        beacon_list = sorted(
            [r for r in results if r["signal"] != "NEUTRAL"],
            key=lambda r: r["signalPct"],
            reverse=True,
        )
        boost_list = sorted(results, key=lambda r: r["rFactor"], reverse=True)

        response = {
            "success": True,
            "beacon": beacon_list,
            "boost": boost_list,
            "summary": {
                "total": len(results),
                "bullish": sum(1 for r in results if r["signal"] == "BULL"),
                "bearish": sum(1 for r in results if r["signal"] == "BEAR"),
                "neutral": sum(1 for r in results if r["signal"] == "NEUTRAL"),
            },
            "timestamp": int(time.time() * 1000),
            "statsCached": len(_stats_cache),
        }

        # Session cache: save if we got meaningful data
        if beacon_list or boost_list:
            save_session("pulse", response)

        return response

    except Exception as exc:
        logger.exception("[pulse] Error")
        # Serve cached data when market is closed
        if not is_market_hours():
            cached = load_session("pulse")
            if cached:
                resp = cached["data"]
                resp["cached"] = True
                resp["cachedAt"] = cached.get("timestamp")
                return resp
        raise HTTPException(status_code=500, detail=str(exc))
