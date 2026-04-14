"""
Institutional Buying scanner route.

Scans the F&O universe for multi-day accumulation footprints using 6 months
of daily OHLCV data and the 5-pillar scoring engine in
``app/engines/institutional_buying.py``.

Liquidity gate: 20-day avg daily turnover > Rs. 50 Cr.
Classification: 80-100 = 5-star, 65-79 = 4-star, 50-64 = 3-star.

Premium only. 60-minute cache; background refresh when stale.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from kiteconnect import KiteConnect

from app.dependencies import get_kite, require_premium
from app.engines.institutional_buying import score_stock, build_sector_clusters

logger = logging.getLogger("alpha_radar.routes.institutional")

router = APIRouter(prefix="/api", tags=["institutional"])

# ── Cache ────────────────────────────────────────────────────────────────────
_inst_cache: Dict[str, Any] | None = None
_inst_cache_ts: float = 0
_inst_scanning: bool = False
_INST_TTL = 3600  # 60 min
_STALE_AFTER = 1800  # 30 min -> trigger background refresh


# ── Helpers ─────────────────────────────────────────────────────────────────
def _get_fno_stocks(kite: KiteConnect) -> List[str]:
    """Derive F&O stock list from NFO-FUT instruments (same pattern as /api/fno/stocks)."""
    try:
        from app.routes.fno import _get_nfo_instruments  # reuse cached fetch
        instruments = _get_nfo_instruments(kite)
    except Exception:
        instruments = kite.instruments("NFO")
    stock_set: set[str] = set()
    for i in instruments:
        if i.get("instrument_type") == "FUT" and i.get("name") and i.get("segment") == "NFO-FUT":
            stock_set.add(i["name"])
    indices = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}
    return sorted(stock_set - indices)


def _get_instrument_token(kite: KiteConnect, symbol: str,
                          cache: Dict[str, int]) -> Optional[int]:
    if symbol in cache:
        return cache[symbol]
    try:
        nse = kite.instruments("NSE")
        for inst in nse:
            if inst.get("tradingsymbol") == symbol and inst.get("segment") == "NSE":
                cache[symbol] = inst["instrument_token"]
                return cache[symbol]
    except Exception:
        logger.exception("Failed fetching NSE instruments")
    return None


def _fetch_candles(kite: KiteConnect, token: int, days: int = 180
                   ) -> List[Dict[str, float]]:
    to = datetime.now()
    fr = to - timedelta(days=days)
    raw = kite.historical_data(token, fr, to, "day")
    out: List[Dict[str, float]] = []
    for r in raw:
        out.append({
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low": float(r["low"]),
            "close": float(r["close"]),
            "volume": int(r.get("volume") or 0),
        })
    return out


def _run_scan_sync(kite: KiteConnect) -> Dict[str, Any]:
    """Blocking scan; call from a thread via asyncio.to_thread."""
    started = time.time()
    symbols = _get_fno_stocks(kite)
    logger.info("Institutional scan: %d F&O stocks", len(symbols))

    token_cache: Dict[str, int] = {}
    # Pre-load NSE instruments once
    try:
        nse = kite.instruments("NSE")
        for inst in nse:
            if inst.get("segment") == "NSE" and inst.get("instrument_type") == "EQ":
                token_cache[inst["tradingsymbol"]] = inst["instrument_token"]
    except Exception:
        logger.exception("Failed preloading NSE instruments")

    qualifying: List[Dict[str, Any]] = []
    filtered_count = 0
    error_count = 0

    for idx, sym in enumerate(symbols, start=1):
        if idx % 25 == 0:
            logger.info("  ...scanning %d/%d", idx, len(symbols))
        tok = token_cache.get(sym)
        if not tok:
            error_count += 1
            continue
        try:
            candles = _fetch_candles(kite, tok, days=180)
            res = score_stock(sym, candles)
            if res is None:
                continue
            if res.get("filtered"):
                filtered_count += 1
                continue
            qualifying.append(res)
        except Exception as exc:  # per-symbol error; keep going
            error_count += 1
            logger.debug("Scan error on %s: %s", sym, exc)
        # light rate-limit courtesy
        time.sleep(0.08)

    clusters = build_sector_clusters(qualifying)
    cluster_sectors = {c["sector"] for c in clusters}
    for r in qualifying:
        if r["sector"] in cluster_sectors and "SECTOR_CLUSTER" not in r["alert_flags"]:
            r["alert_flags"].append("SECTOR_CLUSTER")

    qualifying.sort(key=lambda r: r["score"], reverse=True)

    elapsed = round(time.time() - started, 1)
    return {
        "stocks": qualifying,
        "clusters": clusters,
        "stats": {
            "scanned": len(symbols),
            "filtered_liquidity": filtered_count,
            "qualifying": len(qualifying),
            "errors": error_count,
            "five_star": sum(1 for s in qualifying if s["stars"] == 5),
            "four_star": sum(1 for s in qualifying if s["stars"] == 4),
            "three_star": sum(1 for s in qualifying if s["stars"] == 3),
            "elapsed_sec": elapsed,
        },
        "timestamp": int(time.time() * 1000),
    }


async def _background_refresh(kite: KiteConnect) -> None:
    global _inst_cache, _inst_cache_ts, _inst_scanning
    if _inst_scanning:
        return
    _inst_scanning = True
    try:
        result = await asyncio.to_thread(_run_scan_sync, kite)
        _inst_cache = result
        _inst_cache_ts = time.time()
        logger.info("Institutional scan complete: %d qualifying, %d clusters",
                    result["stats"]["qualifying"], len(result["clusters"]))
    except Exception:
        logger.exception("Background institutional scan failed")
    finally:
        _inst_scanning = False


# ── GET /api/institutional-buying ───────────────────────────────────────────
@router.get("/institutional-buying")
async def institutional_buying(
    background: BackgroundTasks,
    refresh: bool = Query(False, description="Force a fresh scan"),
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(require_premium),
) -> Dict[str, Any]:
    """Scan the F&O universe for institutional accumulation (premium only)."""
    global _inst_cache, _inst_cache_ts

    now = time.time()
    age = now - _inst_cache_ts if _inst_cache else None

    # Force refresh
    if refresh:
        try:
            result = await asyncio.to_thread(_run_scan_sync, kite)
            _inst_cache = result
            _inst_cache_ts = time.time()
            return {**result, "cached": False, "scanning": False}
        except Exception as exc:
            logger.exception("Forced institutional scan failed")
            raise HTTPException(status_code=500, detail=str(exc))

    # Fresh cache
    if _inst_cache and age is not None and age < _STALE_AFTER:
        return {**_inst_cache, "cached": True, "age_seconds": int(age), "scanning": _inst_scanning}

    # Stale but valid -> serve stale, trigger background refresh
    if _inst_cache and age is not None and age < _INST_TTL:
        background.add_task(_background_refresh, kite)
        return {**_inst_cache, "cached": True, "stale": True,
                "age_seconds": int(age), "scanning": True}

    # No cache or expired -> run synchronously
    try:
        result = await asyncio.to_thread(_run_scan_sync, kite)
        _inst_cache = result
        _inst_cache_ts = time.time()
        return {**result, "cached": False, "scanning": False}
    except Exception as exc:
        logger.exception("Institutional scan failed")
        raise HTTPException(status_code=500, detail=str(exc))
