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

from app.dependencies import get_kite
from app.engines.institutional_buying import score_stock, build_sector_clusters

logger = logging.getLogger("alpha_radar.routes.institutional")

router = APIRouter(prefix="/api", tags=["institutional"])

# ── Cache (keyed by universe) ────────────────────────────────────────────────
_inst_cache: Dict[str, Dict[str, Any]] = {}
_inst_cache_ts: Dict[str, float] = {}
_inst_scanning: Dict[str, bool] = {}
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


def _get_nifty500(kite: KiteConnect) -> List[str]:
    """Nifty 500 from the static cache if available, else fallback to full F&O + NSE EQ filter."""
    try:
        from app.caches import stock_universes  # type: ignore
        return list(stock_universes.NIFTY500_STOCKS)
    except Exception:
        return _get_all_nse_eq(kite)


def _get_all_nse_eq(kite: KiteConnect) -> List[str]:
    """All NSE equity symbols (tradeable EQ segment). Large universe (~2000)."""
    try:
        nse = kite.instruments("NSE")
    except Exception:
        return []
    return sorted({
        i["tradingsymbol"] for i in nse
        if i.get("segment") == "NSE" and i.get("instrument_type") == "EQ"
    })


def _resolve_universe(kite: KiteConnect, universe: str) -> List[str]:
    u = (universe or "").lower()
    if u == "fno":
        return _get_fno_stocks(kite)
    if u in ("nifty500", "n500", "500"):
        return _get_nifty500(kite)
    if u in ("all", "nse"):
        return _get_all_nse_eq(kite)
    # default
    return _get_nifty500(kite)


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


def _run_scan_sync(kite: KiteConnect, universe: str = "nifty500") -> Dict[str, Any]:
    """Blocking scan; call from a thread via asyncio.to_thread."""
    started = time.time()
    symbols = _resolve_universe(kite, universe)
    logger.info("Institutional scan: %d stocks (universe=%s)", len(symbols), universe)

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


async def _background_refresh(kite: KiteConnect, universe: str) -> None:
    if _inst_scanning.get(universe):
        return
    _inst_scanning[universe] = True
    try:
        result = await asyncio.to_thread(_run_scan_sync, kite, universe)
        _inst_cache[universe] = result
        _inst_cache_ts[universe] = time.time()
        logger.info("Institutional scan complete [%s]: %d qualifying, %d clusters",
                    universe, result["stats"]["qualifying"], len(result["clusters"]))
    except Exception:
        logger.exception("Background institutional scan failed")
    finally:
        _inst_scanning[universe] = False


# ── GET /api/institutional-buying ───────────────────────────────────────────
@router.get("/institutional-buying")
async def institutional_buying(
    background: BackgroundTasks,
    refresh: bool = Query(False, description="Force a fresh scan"),
    universe: str = Query("nifty500", description="fno | nifty500 | all"),
    kite: KiteConnect = Depends(get_kite),
) -> Dict[str, Any]:
    """Scan the selected universe for institutional accumulation."""
    u = (universe or "nifty500").lower()
    if u not in ("fno", "nifty500", "all"):
        u = "nifty500"

    now = time.time()
    cached = _inst_cache.get(u)
    ts = _inst_cache_ts.get(u, 0)
    age = now - ts if cached else None

    # Force refresh
    if refresh:
        try:
            result = await asyncio.to_thread(_run_scan_sync, kite, u)
            _inst_cache[u] = result
            _inst_cache_ts[u] = time.time()
            return {**result, "universe": u, "cached": False, "scanning": False}
        except Exception as exc:
            logger.exception("Forced institutional scan failed")
            raise HTTPException(status_code=500, detail=str(exc))

    # Fresh cache
    if cached and age is not None and age < _STALE_AFTER:
        return {**cached, "universe": u, "cached": True,
                "age_seconds": int(age), "scanning": _inst_scanning.get(u, False)}

    # Stale but valid -> serve stale, trigger background refresh
    if cached and age is not None and age < _INST_TTL:
        background.add_task(_background_refresh, kite, u)
        return {**cached, "universe": u, "cached": True, "stale": True,
                "age_seconds": int(age), "scanning": True}

    # No cache or expired -> run synchronously
    try:
        result = await asyncio.to_thread(_run_scan_sync, kite, u)
        _inst_cache[u] = result
        _inst_cache_ts[u] = time.time()
        return {**result, "universe": u, "cached": False, "scanning": False}
    except Exception as exc:
        logger.exception("Institutional scan failed")
        raise HTTPException(status_code=500, detail=str(exc))
