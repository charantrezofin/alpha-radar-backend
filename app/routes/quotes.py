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
INDEX_SYMBOLS = {
    "NIFTY50": "NSE:NIFTY 50",
    "BANKNIFTY": "NSE:NIFTY BANK",
    "FINNIFTY": "NSE:NIFTY FIN SERVICE",
    "MIDCAP": "NSE:NIFTY MID SELECT",
    "VIX": "NSE:INDIA VIX",
    "SENSEX": "BSE:SENSEX",
}
_index_sparklines: dict[str, list[float]] = {k: [] for k in INDEX_SYMBOLS}
_MAX_SPARK = 48
_index_cache: dict | None = None
_index_cache_ts: float = 0


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


# ── GET /api/indices ─────────────────────────────────────────────────────────
@router.get("/indices")
async def indices(
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(get_current_user),
) -> dict:
    """Fetch live index quotes with rolling sparklines."""
    global _index_cache, _index_cache_ts

    if _index_cache and time.time() - _index_cache_ts < 8:
        return _index_cache

    try:
        q = kite.quote(list(INDEX_SYMBOLS.values()))
        index_list: list[dict] = []

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
                "sparkline": list(_index_sparklines[key]),
            })

        result = {"indices": index_list, "timestamp": int(time.time() * 1000)}
        _index_cache = result
        _index_cache_ts = time.time()
        return result

    except Exception as exc:
        logger.exception("Index quotes error")
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

        fmt = lambda d: d.strftime("%Y-%m-%d")
        candles = kite.historical_data(token, interval, fmt(from_date), fmt(to_date))

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

        fmt = lambda d: d.strftime("%Y-%m-%d %H:%M:%S")
        candles = kite.historical_data(token, interval, fmt(market_open), fmt(now))

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
