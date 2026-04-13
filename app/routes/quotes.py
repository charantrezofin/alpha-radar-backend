"""
Quote routes -- batch quotes, indices, instruments list, historical, LTP, intraday.

Ported from:
  - Alpha-Radar-backend/server.js (quotes-batch, indices, instruments-list)
  - tradingdesk/apps/gateway/src/routes/quotes.routes.ts
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from kiteconnect import KiteConnect
from pydantic import BaseModel
from supabase import Client

from app.config import settings
from app.core.kite_client import kite_state
from app.core.session_cache import save_session, load_session, is_market_hours
from app.dependencies import get_current_user, get_kite, get_supabase
from app.engines import compute_buying_score, compute_bear_score

logger = logging.getLogger("alpha_radar.routes.quotes")

router = APIRouter(prefix="/api", tags=["quotes"])

# ---------------------------------------------------------------------------
# Caches
# ---------------------------------------------------------------------------
_quote_cache: dict[str, dict] = {}
_QUOTE_CACHE_TTL = 15  # seconds

_instruments_list_cache: list[dict] | None = None
_instruments_list_ts: float = 0
_INSTRUMENTS_LIST_TTL = 86400  # 24 hours

# Index symbols
# Row 1: Core indices (always shown)
INDEX_SYMBOLS = {
    "NIFTY50": "NSE:NIFTY 50",
    "SENSEX": "BSE:SENSEX",
    "BANKNIFTY": "NSE:NIFTY BANK",
    "VIX": "NSE:INDIA VIX",
    "MIDCAP": "NSE:NIFTY MIDCAP 100",
    "SMALLCAP": "NSE:NIFTY SMLCAP 100",
}
# Hot/Cold indices are added dynamically from sector performance
_index_sparklines: dict[str, list[float]] = {k: [] for k in INDEX_SYMBOLS}
_MAX_SPARK = 48
_index_cache: dict | None = None
_index_cache_ts: float = 0


# ---------------------------------------------------------------------------
# Index 1-month returns cache
# ---------------------------------------------------------------------------
_month_returns_cache: dict[str, float] = {}
_month_returns_ts: float = 0
_MONTH_RETURNS_TTL = 3600  # 1 hour


def _get_index_month_returns(kite: KiteConnect) -> dict[str, float]:
    """Compute 1-month returns for all indices. Cached for 1 hour."""
    global _month_returns_cache, _month_returns_ts

    if _month_returns_cache and time.time() - _month_returns_ts < _MONTH_RETURNS_TTL:
        return _month_returns_cache

    results: dict[str, float] = {}
    now = datetime.now(settings.TIMEZONE)
    from_date = now - timedelta(days=35)

    for key, kite_sym in INDEX_SYMBOLS.items():
        try:
            ltp_data = kite.ltp([kite_sym])
            entry = ltp_data.get(kite_sym)
            if not entry:
                continue
            token = entry["instrument_token"]
            current_price = entry["last_price"]

            candles = kite.historical_data(token, from_date, now, "day")
            if candles and len(candles) >= 2:
                price_30d = candles[0]["close"]
                month_ret = round(((current_price - price_30d) / price_30d) * 100, 2) if price_30d > 0 else 0
                results[key] = month_ret
        except Exception:
            results[key] = 0

    _month_returns_cache = results
    _month_returns_ts = time.time()
    logger.info("[indices] Computed 1-month returns for %d indices", len(results))
    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_avg_volume_cache() -> dict[str, float]:
    """Return the avg-volume cache from the global caches module."""
    try:
        from app.caches import avg_volume_cache
        return avg_volume_cache.get_all() if hasattr(avg_volume_cache, "get_all") else {}
    except ImportError:
        return {}


def _get_pdh_cache() -> dict[str, float]:
    try:
        from app.caches import pdh_cache
        return pdh_cache.get_all() if hasattr(pdh_cache, "get_all") else {}
    except ImportError:
        return {}


def _get_pdl_cache() -> dict[str, float]:
    try:
        from app.caches import pdl_cache
        return pdl_cache.get_all() if hasattr(pdl_cache, "get_all") else {}
    except ImportError:
        return {}


def _get_delivery_cache() -> dict[str, float]:
    try:
        from app.caches import delivery_cache
        return delivery_cache.get_all() if hasattr(delivery_cache, "get_all") else {}
    except ImportError:
        return {}


def _get_orb_cache() -> dict[str, dict]:
    try:
        from app.caches import orb_cache
        return orb_cache.get_all() if hasattr(orb_cache, "get_all") else {}
    except ImportError:
        return {}


def _check_premium(request: Request, sb: Client) -> bool:
    """Check if the current user is premium. Non-throwing."""
    try:
        user_id = getattr(request.state, "user_id", None)
        if not user_id:
            return False
        result = (
            sb.table("subscriptions")
            .select("plan, status, current_period_end")
            .eq("user_id", user_id)
            .single()
            .execute()
        )
        sub = result.data
        if not sub:
            return False
        if sub.get("plan") != "premium" or sub.get("status") not in ("active", "trialing"):
            return False
        period_end = sub.get("current_period_end")
        if period_end:
            from datetime import timezone
            end_dt = datetime.fromisoformat(period_end.replace("Z", "+00:00"))
            if end_dt < datetime.now(timezone.utc):
                return False
        return True
    except Exception:
        return False


async def _load_subscription(request: Request, sb: Client) -> bool:
    """Lightweight check -- returns True if premium."""
    admin_header = request.headers.get("X-Admin-Secret", "")
    if admin_header and admin_header == settings.ADMIN_SECRET:
        return True
    return _check_premium(request, sb)


def _fetch_quotes_batched(kite: KiteConnect, symbols: list[str], batch_size: int = 500) -> dict:
    """Fetch Kite quotes in batches of ``batch_size``."""
    all_quotes: dict = {}
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i: i + batch_size]
        q = kite.quote(batch)
        all_quotes.update(q)
    return all_quotes


# ---------------------------------------------------------------------------
# POST body model
# ---------------------------------------------------------------------------
class BatchQuoteBody(BaseModel):
    symbols: list[str]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

# ── GET /api/quotes — lightweight quote fetch (used by heatmap, etc.) ────────
@router.get("/quotes")
async def quotes_simple(
    symbols: str = Query("", description="Comma-separated NSE symbols"),
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """Simple quote fetch without scoring — returns raw Kite data."""
    if not symbols:
        return {"success": True, "data": {}}
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
    try:
        kite_symbols = [f"NSE:{s}" for s in sym_list]
        data = kite.quote(kite_symbols)
        return {"success": True, "data": data}
    except Exception as exc:
        return {"success": False, "data": {}, "error": str(exc)}


# ── GET /api/quotes-batch ────────────────────────────────────────────────────
@router.get("/quotes-batch")
async def quotes_batch(
    request: Request,
    symbols: str = Query("", description="Comma-separated NSE symbols"),
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
) -> dict:
    """
    Fetch real-time quotes for a list of symbols, enriched with buying/bear scores.

    Free tier: max 15 symbols (top by buyingScore).
    Premium: up to 500 symbols.
    """
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        return {"stocks": []}

    # Cap at 500 regardless
    symbol_list = symbol_list[:500]

    is_premium = await _load_subscription(request, sb)
    max_symbols = settings.PREMIUM_TIER_LIMITS.quotes_batch_max if is_premium else settings.FREE_TIER_LIMITS.quotes_batch_max

    # Cache check (15s TTL, keyed on first 5 symbols)
    cache_key = "-".join(symbol_list[:5])
    cached = _quote_cache.get(cache_key)
    if cached and time.time() - cached["ts"] < _QUOTE_CACHE_TTL:
        return cached["data"]

    try:
        # Fetch from Kite
        kite_symbols = [f"NSE:{s}" for s in symbol_list]
        quotes = _fetch_quotes_batched(kite, kite_symbols)

        # BSE fallback for missing symbols
        missing = [s for s in symbol_list if f"NSE:{s}" not in quotes]
        if missing:
            try:
                bse_quotes = _fetch_quotes_batched(kite, [f"BSE:{s}" for s in missing])
                for s in missing:
                    if f"BSE:{s}" in bse_quotes:
                        quotes[f"NSE:{s}"] = bse_quotes[f"BSE:{s}"]
            except Exception:
                pass

        avg_vol = _get_avg_volume_cache()
        pdh_data = _get_pdh_cache()
        pdl_data = _get_pdl_cache()
        delivery_data = _get_delivery_cache()
        orb_data = _get_orb_cache()

        stocks: list[dict] = []
        for symbol in symbol_list:
            q = quotes.get(f"NSE:{symbol}")
            if not q:
                continue

            ltp = q.get("last_price", 0)
            prev_close = q.get("ohlc", {}).get("close", ltp) or ltp
            open_ = q.get("ohlc", {}).get("open", ltp) or ltp
            high = q.get("ohlc", {}).get("high", ltp) or ltp
            low = q.get("ohlc", {}).get("low", ltp) or ltp
            volume = q.get("volume", 0) or 0
            delivery = delivery_data.get(symbol, 0)

            scored = compute_buying_score(
                symbol=symbol, ltp=ltp, prev_close=prev_close,
                open_=open_, high=high, low=low, volume=volume,
                avg_volume=avg_vol.get(symbol, 0),
                pdh=pdh_data.get(symbol, 0),
                delivery=delivery,
            )
            bear_scored = compute_bear_score(
                symbol=symbol, ltp=ltp, prev_close=prev_close,
                open_=open_, high=high, low=low, volume=volume,
                avg_volume=avg_vol.get(symbol, 0),
                pdl=pdl_data.get(symbol, 0),
                delivery=delivery,
            )

            # Update live signal trackers (if available)
            try:
                from app.caches import live_tracker, bear_tracker, orb_detector
                live_tracker.update(symbol, ltp, pdh_data.get(symbol, 0), scored.vol_ratio)
                bear_tracker.update(symbol, ltp, pdl_data.get(symbol, 0), bear_scored.vol_ratio_bear)
                orb_detector.update(symbol, ltp)
            except ImportError:
                pass

            orb = orb_data.get(symbol)
            stocks.append({
                "symbol": symbol,
                "ltp": round(ltp, 2),
                "prevClose": round(prev_close, 2),
                "open": round(open_, 2),
                "high": round(high, 2),
                "low": round(low, 2),
                "change": round(ltp - prev_close, 2),
                "changePct": scored.change_pct,
                "volume": volume,
                "avgVolume": avg_vol.get(symbol),
                "volRatio": scored.vol_ratio,
                "buyingScore": scored.buying_score,
                "scoreBreakdown": {
                    "vol": scored.vol_score,
                    "pdh": scored.pdh_score,
                    "momentum": scored.momentum_score,
                    "range": scored.range_pos_score,
                    "delivery": scored.delivery_score,
                },
                "isBreakout": scored.is_breakout,
                "pdh": pdh_data.get(symbol),
                "bearScore": bear_scored.bear_score,
                "bearScoreBreakdown": {
                    "vol": bear_scored.vol_score_bear,
                    "pdl": bear_scored.pdl_score,
                    "momentum": bear_scored.momentum_score_bear,
                    "range": bear_scored.range_pos_score_bear,
                    "delivery": bear_scored.delivery_score_bear,
                },
                "isPDLBreak": bear_scored.is_pdl_break,
                "pdl": pdl_data.get(symbol),
                "delivery": delivery or None,
                "orb": {
                    "high15": orb.get("orb_high_15") if orb else None,
                    "low15": orb.get("orb_low_15") if orb else None,
                    "high30": orb.get("orb_high_30") if orb else None,
                    "low30": orb.get("orb_low_30") if orb else None,
                } if orb else None,
                "lastUpdated": int(time.time() * 1000),
            })

        # Freemium gating: free users see top N by buyingScore
        visible_stocks = stocks
        if not is_premium and len(stocks) > max_symbols:
            sorted_stocks = sorted(stocks, key=lambda s: s["buyingScore"], reverse=True)
            visible_stocks = sorted_stocks[:max_symbols]

        result = {
            "stocks": visible_stocks,
            "totalCount": len(stocks),
            "visibleCount": len(visible_stocks),
            "tier": "premium" if is_premium else "free",
            "limited": not is_premium and len(stocks) > max_symbols,
            "timestamp": int(time.time() * 1000),
        }

        # Cache full result
        if is_premium or len(symbol_list) <= max_symbols:
            _quote_cache[cache_key] = {
                "data": {
                    "stocks": stocks,
                    "totalCount": len(stocks),
                    "visibleCount": len(stocks),
                    "tier": "premium",
                    "limited": False,
                    "timestamp": int(time.time() * 1000),
                },
                "ts": time.time(),
            }

        return result

    except Exception as exc:
        logger.exception("Quote batch error")
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /api/indices (also aliased as /api/quotes/indices) ──────────────────
@router.get("/quotes/indices")
@router.get("/indices")
async def indices(
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """Fetch live index quotes with rolling sparklines."""
    global _index_cache, _index_cache_ts

    if _index_cache and time.time() - _index_cache_ts < 8:
        return _index_cache

    # If market is closed and no in-memory cache, try session cache
    if not is_market_hours() and not _index_cache:
        cached = load_session("indices")
        if cached:
            resp = cached["data"]
            resp["cached"] = True
            resp["cachedAt"] = cached.get("timestamp")
            _index_cache = resp
            _index_cache_ts = time.time()
            return resp

    try:
        q = kite.quote(list(INDEX_SYMBOLS.values()))
        index_list: list[dict] = []

        # Fetch 1-month returns for all indices (cached separately)
        month_returns = _get_index_month_returns(kite)

        for key, kite_sym in INDEX_SYMBOLS.items():
            d = q.get(kite_sym)
            if not d:
                continue
            ltp = d.get("last_price", 0)
            prev = d.get("ohlc", {}).get("close", ltp) or ltp
            chg_pct = round(((ltp - prev) / prev) * 100, 2) if prev > 0 else 0
            chg_abs = round(ltp - prev, 2)

            _index_sparklines[key].append(round(ltp, 2))
            if len(_index_sparklines[key]) > _MAX_SPARK:
                _index_sparklines[key].pop(0)

            index_list.append({
                "key": key,
                "ltp": round(ltp, 2),
                "prev": round(prev, 2),
                "chg": chg_pct,
                "chgAbs": chg_abs,
                "open": d.get("ohlc", {}).get("open"),
                "high": d.get("ohlc", {}).get("high"),
                "low": d.get("ohlc", {}).get("low"),
                "monthReturn": month_returns.get(key, 0),
                "sparkline": list(_index_sparklines[key]),
            })

        result = {"indices": index_list, "timestamp": int(time.time() * 1000)}

        # Session cache: save if we got meaningful data
        if index_list and any(i.get("ltp", 0) > 0 for i in index_list):
            save_session("indices", result)

        _index_cache = result
        _index_cache_ts = time.time()
        return result

    except Exception as exc:
        logger.exception("Index quotes error")
        # Serve cached data when market is closed
        if not is_market_hours():
            cached = load_session("indices")
            if cached:
                resp = cached["data"]
                resp["cached"] = True
                resp["cachedAt"] = cached.get("timestamp")
                return resp
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /api/instruments-list ────────────────────────────────────────────────
@router.get("/instruments-list")
async def instruments_list(kite: KiteConnect = Depends(get_kite)) -> list[dict]:
    """
    Return a merged NSE + BSE equity instrument list for autocomplete search.
    Cached for 24 hours.
    """
    global _instruments_list_cache, _instruments_list_ts

    if _instruments_list_cache and time.time() - _instruments_list_ts < _INSTRUMENTS_LIST_TTL:
        return _instruments_list_cache

    try:
        nse_instruments = kite.instruments("NSE")
        bse_instruments: list = []
        try:
            bse_instruments = kite.instruments("BSE")
        except Exception:
            pass

        stock_map: dict[str, dict] = {}

        # BSE first (lower priority)
        for inst in bse_instruments:
            if inst.get("instrument_type") == "EQ" and inst.get("segment") == "BSE":
                sym = inst.get("tradingsymbol", "")
                if "-" not in sym:
                    stock_map[sym] = {
                        "symbol": sym,
                        "name": inst.get("name", sym),
                        "exchange": "BSE",
                    }

        # NSE overlay (higher priority)
        for inst in nse_instruments:
            if inst.get("instrument_type") == "EQ" and inst.get("segment") == "NSE":
                sym = inst.get("tradingsymbol", "")
                if "-" not in sym:
                    stock_map[sym] = {
                        "symbol": sym,
                        "name": inst.get("name", sym),
                        "exchange": "NSE",
                    }

        result = sorted(stock_map.values(), key=lambda x: x["symbol"])
        _instruments_list_cache = result
        _instruments_list_ts = time.time()
        logger.info("Instruments list built: %d stocks (NSE + BSE)", len(result))
        return result

    except Exception as exc:
        logger.exception("Instruments list error")
        return []


# ── GET /api/quotes/history ──────────────────────────────────────────────────
@router.get("/quotes/history")
async def quote_history(
    symbol: str = Query(..., description="NSE symbol"),
    interval: str = Query("day", description="Kite candle interval"),
    days: int = Query(30, description="Number of days of history"),
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """Fetch historical OHLC candles for a symbol."""
    try:
        ltp_data = kite.ltp([f"NSE:{symbol}"])
        entry = ltp_data.get(f"NSE:{symbol}")
        if not entry:
            raise HTTPException(status_code=404, detail="Symbol not found")

        token = entry["instrument_token"]
        to_date = datetime.now(settings.TIMEZONE)
        from_date = to_date - timedelta(days=days)

        # Kite doesn't support "month" or "monthly" interval — fetch daily and resample
        is_monthly = interval in ("month", "monthly")
        kite_interval = "day" if is_monthly else interval

        candles = kite.historical_data(token, from_date, to_date, kite_interval)

        if is_monthly and candles:
            # Resample daily candles to monthly OHLCV
            import pandas as pd
            df = pd.DataFrame(candles)
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
            monthly = df.resample("ME").agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }).dropna()
            candles = [
                {
                    "date": str(idx),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["volume"]),
                }
                for idx, row in monthly.iterrows()
            ]
            return {"success": True, "symbol": symbol, "data": candles}

        return {
            "success": True,
            "symbol": symbol,
            "data": [
                {
                    "date": str(bar["date"]),
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar.get("volume", 0),
                }
                for bar in candles
            ],
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /api/quotes/ltp ─────────────────────────────────────────────────────
@router.get("/quotes/ltp")
async def quote_ltp(
    symbols: str = Query(..., description="Comma-separated NSE symbols"),
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """Lightweight last-traded-price only."""
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        raise HTTPException(status_code=400, detail="symbols required")

    try:
        instruments = [f"NSE:{s}" for s in symbol_list]
        ltp_data = kite.ltp(instruments)
        return {"success": True, "data": ltp_data}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /api/quotes/batch ───────────────────────────────────────────────────
@router.post("/quotes/batch")
async def quote_batch_post(
    body: BatchQuoteBody,
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """Batch quote fetch via POST body (avoids URL length limits)."""
    if not body.symbols:
        raise HTTPException(status_code=400, detail="symbols array required")

    try:
        all_quotes: dict = {}
        chunk_size = 250
        for i in range(0, len(body.symbols), chunk_size):
            chunk = body.symbols[i: i + chunk_size]
            instruments = [f"NSE:{s}" for s in chunk]
            q = kite.quote(instruments)
            all_quotes.update(q)

        return {"success": True, "data": all_quotes}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /api/quotes/intraday ────────────────────────────────────────────────
@router.get("/quotes/intraday")
async def quote_intraday(
    symbol: str = Query(..., description="NSE symbol or index name"),
    interval: str = Query("5minute", description="Candle interval"),
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """Intraday candles for today's session."""
    try:
        ltp_data = kite.ltp([f"NSE:{symbol}"])
        entry = ltp_data.get(f"NSE:{symbol}")
        if not entry:
            raise HTTPException(status_code=404, detail="Symbol not found")

        token = entry["instrument_token"]
        now = datetime.now(settings.TIMEZONE)
        market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)

        candles = kite.historical_data(token, market_open, now, interval)

        return {
            "success": True,
            "symbol": symbol,
            "interval": interval,
            "data": [
                {
                    "time": str(bar["date"]),
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar.get("volume", 0),
                }
                for bar in candles
            ],
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /api/quotes/sectors ────────────────────────────────────────────────
@router.get("/quotes/sectors")
async def quote_sectors(
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """Sector index quotes for the sector heatmap."""
    SECTOR_INDICES = [
        "NIFTY IT", "NIFTY BANK", "NIFTY PRIVATE BANK", "NIFTY PSU BANK",
        "NIFTY FIN SERVICE", "NIFTY AUTO", "NIFTY PHARMA", "NIFTY HEALTHCARE INDEX",
        "NIFTY METAL", "NIFTY FMCG", "NIFTY MEDIA", "NIFTY REALTY",
        "NIFTY ENERGY", "NIFTY OIL & GAS", "NIFTY COMMODITIES",
        "NIFTY INFRASTRUCTURE", "NIFTY CPSE", "NIFTY MNC",
        "NIFTY INDIA DEFENCE", "NIFTY CAPITAL MKT",
    ]
    try:
        symbols = [f"NSE:{s}" for s in SECTOR_INDICES]
        q = kite.quote(symbols)
        results = []
        for sym in SECTOR_INDICES:
            key = f"NSE:{sym}"
            d = q.get(key)
            if not d:
                continue
            ohlc = d.get("ohlc", {})
            last = d.get("last_price", 0)
            prev = ohlc.get("close", last)
            change = last - prev
            change_pct = (change / prev * 100) if prev else 0
            results.append({
                "symbol": sym,
                "name": sym.replace("NIFTY ", ""),
                "last": last,
                "change": round(change, 2),
                "changePct": round(change_pct, 2),
                "open": ohlc.get("open", 0),
                "high": ohlc.get("high", 0),
                "low": ohlc.get("low", 0),
                "close": prev,
            })
        return {"sectors": results}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Hot/Cold sector indices cache
# ---------------------------------------------------------------------------
_hot_cold_cache: dict | None = None
_hot_cold_cache_ts: float = 0
_HOT_COLD_TTL = 3600  # 1 hour


# ── GET /api/hot-cold-indices ─────────────────────────────────────────────
@router.get("/hot-cold-indices")
async def hot_cold_indices(
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """
    Return the hottest and coldest NSE sector index by 1-month return.
    Cached for 1 hour.
    """
    global _hot_cold_cache, _hot_cold_cache_ts

    if _hot_cold_cache and time.time() - _hot_cold_cache_ts < _HOT_COLD_TTL:
        return _hot_cold_cache

    HOT_COLD_SECTOR_INDICES = [
        "NIFTY IT", "NIFTY BANK", "NIFTY AUTO", "NIFTY PHARMA",
        "NIFTY METAL", "NIFTY FMCG", "NIFTY REALTY", "NIFTY ENERGY",
        "NIFTY INFRA", "NIFTY PSU BANK", "NIFTY MEDIA",
        "NIFTY PRIVATE BANK", "NIFTY FIN SERVICE",
    ]

    try:
        # 1. Get current quotes for all sector indices
        kite_symbols = [f"NSE:{s}" for s in HOT_COLD_SECTOR_INDICES]
        quotes = kite.quote(kite_symbols)

        # 2. For each, get 30-day historical data and compute 1-month return
        now = datetime.now(settings.TIMEZONE)
        from_date = now - timedelta(days=35)  # extra buffer for non-trading days

        results = []
        for sym_name in HOT_COLD_SECTOR_INDICES:
            kite_sym = f"NSE:{sym_name}"
            q = quotes.get(kite_sym)
            if not q:
                continue

            last_price = q.get("last_price", 0)
            if last_price <= 0:
                continue

            ohlc = q.get("ohlc", {})
            prev_close = ohlc.get("close", last_price) or last_price
            today_change_pct = round(((last_price - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0

            # Fetch 30-day historical data
            try:
                token = q.get("instrument_token")
                if not token:
                    ltp_data = kite.ltp([kite_sym])
                    entry = ltp_data.get(kite_sym)
                    if entry:
                        token = entry["instrument_token"]
                if not token:
                    continue

                candles = kite.historical_data(token, from_date, now, "day")
                if not candles or len(candles) < 2:
                    continue

                # Price ~30 days ago (first candle in the range)
                price_30d_ago = candles[0]["close"]
                month_return = round(((last_price - price_30d_ago) / price_30d_ago) * 100, 2) if price_30d_ago > 0 else 0

                results.append({
                    "name": sym_name,
                    "symbol": kite_sym,
                    "last": round(last_price, 2),
                    "monthReturn": month_return,
                    "changePct": today_change_pct,
                })
            except Exception as e:
                logger.warning("Hot/cold historical fetch failed for %s: %s", sym_name, e)
                continue

        if not results:
            raise HTTPException(status_code=500, detail="No sector data available")

        # 3. Sort by 1-month return
        results.sort(key=lambda x: x["monthReturn"], reverse=True)
        hot = results[0]
        cold = results[-1]

        response = {
            "hot": hot,
            "cold": cold,
            "timestamp": int(time.time() * 1000),
        }

        _hot_cold_cache = response
        _hot_cold_cache_ts = time.time()
        return response

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Hot/cold indices error")
        # Return cached data if available
        if _hot_cold_cache:
            return _hot_cold_cache
        raise HTTPException(status_code=500, detail=str(exc))
