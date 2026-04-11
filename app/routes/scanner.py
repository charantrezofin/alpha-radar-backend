"""
Scanner routes -- CPR, patterns, VCP, volume profile, NR/squeeze, swing.

Each takes {symbols: [list]} in body, fetches historical data from Kite,
passes to the appropriate CPR engine module.
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
from app.dependencies import get_current_user, get_kite

logger = logging.getLogger("alpha_radar.routes.scanner")

router = APIRouter(prefix="/api/scanner", tags=["scanner"])


# ── Request model ────────────────────────────────────────────────────────────
class ScannerBody(BaseModel):
    symbols: list[str]
    timeframe: str = "daily"
    days: int = 90


# ── Helpers ──────────────────────────────────────────────────────────────────

def _fetch_historical_for_symbols(
    kite: KiteConnect,
    symbols: list[str],
    days: int = 90,
    interval: str = "day",
) -> dict[str, list[dict]]:
    """
    Fetch daily OHLC from Kite for each symbol.
    Returns {symbol: [candle_dicts...]}.
    """
    # Build instrument token map
    try:
        nse_instruments = kite.instruments("NSE")
    except Exception:
        nse_instruments = []

    token_map: dict[str, int] = {}
    for inst in nse_instruments:
        if inst.get("instrument_type") == "EQ" and inst.get("segment") == "NSE":
            token_map[inst["tradingsymbol"]] = inst["instrument_token"]

    to_date = datetime.now(settings.TIMEZONE)
    from_date = to_date - timedelta(days=days + 10)  # Extra buffer for weekends
    fmt = lambda d: d.strftime("%Y-%m-%d")

    result: dict[str, list[dict]] = {}
    batch_size = 10
    delay = 0.4

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i: i + batch_size]
        for symbol in batch:
            token = token_map.get(symbol)
            if not token:
                continue
            try:
                candles = kite.historical_data(token, interval, fmt(from_date), fmt(to_date))
                if candles and len(candles) >= 5:
                    result[symbol] = [
                        {
                            "date": str(c["date"]),
                            "open": c["open"],
                            "high": c["high"],
                            "low": c["low"],
                            "close": c["close"],
                            "volume": c.get("volume", 0),
                        }
                        for c in candles
                    ]
            except Exception:
                pass
        if i + batch_size < len(symbols):
            time.sleep(delay)

    return result


# ── POST /api/scanner/cpr ────────────────────────────────────────────────────
@router.post("/cpr")
async def scan_cpr(
    body: ScannerBody,
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(get_current_user),
) -> dict:
    """Fetch OHLC for symbols, run CPR multi-timeframe scan."""
    try:
        ohlc_data = _fetch_historical_for_symbols(kite, body.symbols, body.days)

        results: list[dict] = []
        try:
            from app.engines.cpr import scan_cpr as cpr_engine
            for symbol, candles in ohlc_data.items():
                try:
                    result = cpr_engine(symbol, candles)
                    if result:
                        results.append(result.__dict__ if hasattr(result, "__dict__") else result)
                except Exception:
                    pass
        except ImportError:
            # Engine not yet available -- return raw OHLC with placeholder
            for symbol, candles in ohlc_data.items():
                if candles:
                    last = candles[-1]
                    prev = candles[-2] if len(candles) >= 2 else last
                    pivot = (prev["high"] + prev["low"] + prev["close"]) / 3
                    bc = (prev["high"] + prev["low"]) / 2
                    tc = 2 * pivot - bc
                    results.append({
                        "symbol": symbol,
                        "score": 0,
                        "direction": "NEUTRAL",
                        "alertTier": "LOW",
                        "cprLevels": {
                            "pivot": round(pivot, 2),
                            "bc": round(min(bc, tc), 2),
                            "tc": round(max(bc, tc), 2),
                        },
                        "cprWidth": round(abs(tc - bc), 2),
                    })

        return {"success": True, "results": results, "count": len(results), "timestamp": int(time.time() * 1000)}

    except Exception as exc:
        logger.exception("CPR scan error")
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /api/scanner/patterns ───────────────────────────────────────────────
@router.post("/patterns")
async def scan_patterns(
    body: ScannerBody,
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(get_current_user),
) -> dict:
    """Fetch OHLC, run pattern detection."""
    try:
        ohlc_data = _fetch_historical_for_symbols(kite, body.symbols, body.days)

        results: list[dict] = []
        try:
            from app.engines.cpr import detect_patterns as pattern_engine
            for symbol, candles in ohlc_data.items():
                try:
                    result = pattern_engine(symbol, candles, body.timeframe)
                    if result:
                        results.append(result.__dict__ if hasattr(result, "__dict__") else result)
                except Exception:
                    pass
        except ImportError:
            for symbol in ohlc_data:
                results.append({"symbol": symbol, "patterns": [], "timeframe": body.timeframe})

        return {"success": True, "results": results, "count": len(results), "timestamp": int(time.time() * 1000)}

    except Exception as exc:
        logger.exception("Pattern scan error")
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /api/scanner/vcp ───────────────────────────────────────────────────
@router.post("/vcp")
async def scan_vcp(
    body: ScannerBody,
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(get_current_user),
) -> dict:
    """Fetch OHLC, run VCP (Volatility Contraction Pattern) detection."""
    try:
        ohlc_data = _fetch_historical_for_symbols(kite, body.symbols, max(body.days, 120))

        results: list[dict] = []
        try:
            from app.engines.cpr import detect_vcp as vcp_engine
            for symbol, candles in ohlc_data.items():
                try:
                    result = vcp_engine(symbol, candles)
                    if result:
                        results.append(result.__dict__ if hasattr(result, "__dict__") else result)
                except Exception:
                    pass
        except ImportError:
            for symbol in ohlc_data:
                results.append({"symbol": symbol, "score": 0, "stage": "STAGE_2", "contractions": 0})

        return {"success": True, "results": results, "count": len(results), "timestamp": int(time.time() * 1000)}

    except Exception as exc:
        logger.exception("VCP scan error")
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /api/scanner/volume-profile ────────────────────────────────────────
@router.post("/volume-profile")
async def scan_volume_profile(
    body: ScannerBody,
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(get_current_user),
) -> dict:
    """Fetch OHLC, run volume profile analysis."""
    try:
        ohlc_data = _fetch_historical_for_symbols(kite, body.symbols, body.days)

        results: list[dict] = []
        try:
            from app.engines.cpr import analyze_volume_profile as vp_engine
            for symbol, candles in ohlc_data.items():
                try:
                    result = vp_engine(symbol, candles)
                    if result:
                        results.append(result.__dict__ if hasattr(result, "__dict__") else result)
                except Exception:
                    pass
        except ImportError:
            for symbol in ohlc_data:
                results.append({"symbol": symbol, "poc": 0, "vah": 0, "val": 0, "signal": "AT_POC"})

        return {"success": True, "results": results, "count": len(results), "timestamp": int(time.time() * 1000)}

    except Exception as exc:
        logger.exception("Volume profile scan error")
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /api/scanner/nr-squeeze ────────────────────────────────────────────
@router.post("/nr-squeeze")
async def scan_nr_squeeze(
    body: ScannerBody,
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(get_current_user),
) -> dict:
    """Fetch OHLC, run NR4/NR7 squeeze detection."""
    try:
        ohlc_data = _fetch_historical_for_symbols(kite, body.symbols, body.days)

        results: list[dict] = []
        try:
            from app.engines.cpr import detect_nr_squeeze as nr_engine
            for symbol, candles in ohlc_data.items():
                try:
                    result = nr_engine(symbol, candles)
                    if result:
                        results.append(result.__dict__ if hasattr(result, "__dict__") else result)
                except Exception:
                    pass
        except ImportError:
            for symbol in ohlc_data:
                results.append({"symbol": symbol, "squeezeType": None, "score": 0, "bias": "NEUTRAL"})

        return {"success": True, "results": results, "count": len(results), "timestamp": int(time.time() * 1000)}

    except Exception as exc:
        logger.exception("NR squeeze scan error")
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /api/scanner/swing ─────────────────────────────────────────────────
@router.post("/swing")
async def scan_swing(
    body: ScannerBody,
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(get_current_user),
) -> dict:
    """Fetch OHLC, run swing signal detector."""
    try:
        ohlc_data = _fetch_historical_for_symbols(kite, body.symbols, max(body.days, 60))

        results: list[dict] = []
        try:
            from app.engines.cpr import detect_swing as swing_engine
            for symbol, candles in ohlc_data.items():
                try:
                    result = swing_engine(symbol, candles)
                    if result:
                        results.append(result.__dict__ if hasattr(result, "__dict__") else result)
                except Exception:
                    pass
        except ImportError:
            for symbol in ohlc_data:
                results.append({"symbol": symbol, "strategy": None, "direction": "NEUTRAL", "strength": "WEAK"})

        return {"success": True, "results": results, "count": len(results), "timestamp": int(time.time() * 1000)}

    except Exception as exc:
        logger.exception("Swing scan error")
        raise HTTPException(status_code=500, detail=str(exc))
