"""
52-Week Low Bounce scanner route.

Scans Nifty 500 stocks for accumulation near 52-week lows:
  - Volume >= 2x 20-day avg
  - Green today (changePct > 0)
  - Price recovering (ltp > open)
  - buyingScore >= 35

Premium only. 5-minute cache.

Ported from Alpha-Radar-backend/server.js (lines 912-1017)
"""

from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from kiteconnect import KiteConnect

from app.dependencies import get_kite, require_premium
from app.engines import compute_buying_score, detect_52w_bounce

logger = logging.getLogger("alpha_radar.routes.bounce")

router = APIRouter(prefix="/api", tags=["bounce"])

# ── Cache ────────────────────────────────────────────────────────────────────
_bounce_cache: dict | None = None
_bounce_cache_ts: float = 0
_BOUNCE_TTL = 300  # 5 minutes


def _get_nifty500() -> list[str]:
    """Return the Nifty 500 universe. Imported lazily to avoid circular deps."""
    try:
        from app.caches import stock_universes
        return stock_universes.NIFTY500_STOCKS
    except ImportError:
        # Fallback minimal list
        return [
            "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
            "SBIN", "BAJFINANCE", "BHARTIARTL", "KOTAKBANK", "ITC", "LT",
            "AXISBANK", "MARUTI", "SUNPHARMA", "TATAMOTORS", "WIPRO",
        ]


# ── GET /api/52w-bounce ──────────────────────────────────────────────────────
@router.get("/52w-bounce")
async def bounce_52w(
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(require_premium),
) -> dict:
    """Scan Nifty 500 for 52-week low bounce candidates (premium only)."""
    global _bounce_cache, _bounce_cache_ts

    if _bounce_cache and time.time() - _bounce_cache_ts < _BOUNCE_TTL:
        return _bounce_cache

    try:
        nifty500 = _get_nifty500()
        kite_symbols = [f"NSE:{s}" for s in nifty500]

        # Batch fetch
        all_quotes: dict = {}
        for i in range(0, len(kite_symbols), 500):
            q = kite.quote(kite_symbols[i: i + 500])
            all_quotes.update(q)

        # Load caches
        try:
            from app.caches import avg_volume_cache, delivery_cache
            avg_vol = avg_volume_cache.get_all() if hasattr(avg_volume_cache, "get_all") else {}
            delivery_data = delivery_cache.get_all() if hasattr(delivery_cache, "get_all") else {}
        except ImportError:
            avg_vol = {}
            delivery_data = {}

        bounce_stocks: list[dict] = []
        for symbol in nifty500:
            q = all_quotes.get(f"NSE:{symbol}")
            if not q:
                continue

            ltp = q.get("last_price", 0)
            prev_close = q.get("ohlc", {}).get("close", ltp) or ltp
            open_ = q.get("ohlc", {}).get("open", ltp) or ltp
            high = q.get("ohlc", {}).get("high", ltp) or ltp
            low = q.get("ohlc", {}).get("low", ltp) or ltp
            volume = q.get("volume", 0) or 0
            avg_volume = avg_vol.get(symbol, 0)
            delivery = delivery_data.get(symbol, 0)

            if avg_volume == 0:
                continue
            vol_ratio = volume / avg_volume
            change_pct = ((ltp - prev_close) / prev_close * 100) if prev_close > 0 else 0

            # Criteria
            if vol_ratio < 2:
                continue
            if change_pct <= 0:
                continue
            if ltp <= open_:
                continue

            scored = compute_buying_score(
                symbol=symbol, ltp=ltp, prev_close=prev_close,
                open_=open_, high=high, low=low, volume=volume,
                avg_volume=avg_volume, pdh=0, delivery=delivery,
            )
            if scored.buying_score < 35:
                continue

            bounce_stocks.append({
                "symbol": symbol,
                "ltp": round(ltp, 2),
                "prevClose": round(prev_close, 2),
                "open": round(open_, 2),
                "high": round(high, 2),
                "low": round(low, 2),
                "changePct": round(change_pct, 2),
                "volume": volume,
                "avgVolume": avg_volume,
                "volRatio": round(vol_ratio, 2),
                "buyingScore": scored.buying_score,
                "isBreakout": scored.is_breakout,
                "delivery": delivery or None,
                "signalType": "52W_BOUNCE",
            })

        bounce_stocks.sort(key=lambda s: s["volRatio"], reverse=True)

        result = {"stocks": bounce_stocks, "count": len(bounce_stocks), "timestamp": int(time.time() * 1000)}
        _bounce_cache = result
        _bounce_cache_ts = time.time()
        return result

    except Exception as exc:
        logger.exception("52W bounce error")
        raise HTTPException(status_code=500, detail=str(exc))
