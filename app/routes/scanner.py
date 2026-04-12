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

from fastapi import APIRouter, Depends, HTTPException, Request
from kiteconnect import KiteConnect
from pydantic import BaseModel

from app.config import settings
from app.dependencies import get_current_user, get_kite

logger = logging.getLogger("alpha_radar.routes.scanner")

router = APIRouter(prefix="/api/scanner", tags=["scanner"])


# ── Request models ───────────────────────────────────────────────────────────
class ScannerBody(BaseModel):
    symbols: list[str]
    timeframe: str = "daily"
    days: int = 90


class CPRSymbolData(BaseModel):
    symbol: str
    daily: list[dict]
    weekly: list[dict] | None = None
    monthly: list[dict] | None = None
    current_price: float | None = None


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
    request: Request,
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(get_current_user),
) -> dict:
    """
    CPR multi-timeframe scan.
    Accepts body as either:
      - {symbols: ["SYM1", ...]} → backend fetches OHLC
      - [{symbol, daily, weekly?, monthly?, current_price?}, ...] → pre-fetched OHLC from frontend
    """
    body_data = await request.json()

    try:
        results: list[dict] = []

        # Detect format: {symbols: [{symbol, daily, ...}]} or {symbols: ["SYM1", ...]}
        items = body_data if isinstance(body_data, list) else body_data.get("symbols", [])

        # Check if items are pre-fetched OHLC objects or plain symbol strings
        has_ohlc = items and isinstance(items[0], dict) and "daily" in items[0]

        if has_ohlc:
            # Frontend sends pre-fetched OHLC data
            for item in items:
                symbol = item.get("symbol", "")
                daily = item.get("daily", [])
                weekly = item.get("weekly") or []
                monthly = item.get("monthly") or []
                current_price = item.get("current_price")

                if not daily or len(daily) < 5:
                    continue

                try:
                    from app.engines.cpr.cpr_calculator import compute_cpr_series, analyse_cpr_sequence
                    from app.engines.cpr.pema_calculator import compute_pema
                    from app.engines.cpr.signal_scorer import score_symbol
                    import pandas as pd

                    def _to_df(candles):
                        if not candles:
                            return pd.DataFrame()
                        df = pd.DataFrame(candles)
                        for col in ["open", "high", "low", "close"]:
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                        if "volume" in df.columns:
                            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
                        if "date" in df.columns:
                            df["date"] = pd.to_datetime(df["date"])
                        return df

                    daily_df = _to_df(daily)
                    weekly_df = _to_df(weekly) if weekly else pd.DataFrame()
                    monthly_df = _to_df(monthly) if monthly else pd.DataFrame()

                    result = score_symbol(
                        symbol=symbol,
                        daily=daily_df,
                        weekly=weekly_df if not weekly_df.empty else None,
                        monthly=monthly_df if not monthly_df.empty else None,
                        intraday=None,
                        current_price=current_price or (float(daily_df.iloc[-1]["close"]) if len(daily_df) > 0 else 0),
                    )
                    if result:
                        results.append(result if isinstance(result, dict) else result.__dict__ if hasattr(result, "__dict__") else {"symbol": symbol})

                except ImportError:
                    # Fallback: compute basic CPR levels
                    if daily and len(daily) >= 2:
                        prev = daily[-2]
                        h, l, c = float(prev["high"]), float(prev["low"]), float(prev["close"])
                        pivot = (h + l + c) / 3
                        bc = (h + l) / 2
                        tc = 2 * pivot - bc
                        ltp = current_price or float(daily[-1]["close"])
                        width = abs(tc - bc)
                        width_pct = (width / ltp * 100) if ltp else 0
                        results.append({
                            "symbol": symbol,
                            "score": 0,
                            "direction": "NEUTRAL",
                            "alertTier": "None",
                            "cprLevels": {"pivot": round(pivot, 2), "bc": round(min(bc, tc), 2), "tc": round(max(bc, tc), 2)},
                            "cprWidth": round(width, 2),
                            "cprWidthPct": round(width_pct, 2),
                            "ltp": ltp,
                        })
                except Exception as e:
                    logger.debug("CPR scan failed for %s: %s", symbol, e)

        else:
            # Plain symbol strings — backend fetches OHLC
            symbols = [s if isinstance(s, str) else s.get("symbol", "") for s in items]
            symbols = [s for s in symbols if s]
            ohlc_data = _fetch_historical_for_symbols(kite, symbols, body_data.get("days", 120))
            for symbol, candles in ohlc_data.items():
                if candles and len(candles) >= 2:
                    prev = candles[-2]
                    pivot = (prev["high"] + prev["low"] + prev["close"]) / 3
                    bc = (prev["high"] + prev["low"]) / 2
                    tc = 2 * pivot - bc
                    results.append({
                        "symbol": symbol,
                        "score": 0,
                        "direction": "NEUTRAL",
                        "cprLevels": {"pivot": round(pivot, 2), "bc": round(min(bc, tc), 2), "tc": round(max(bc, tc), 2)},
                        "cprWidth": round(abs(tc - bc), 2),
                    })

        results.sort(key=lambda r: abs(r.get("score", 0)), reverse=True)
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
