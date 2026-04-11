"""
User routes -- tier info, tier config, watchlist management.

Ported from Alpha-Radar-backend/server.js
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Path as PathParam, Request
from kiteconnect import KiteConnect
from pydantic import BaseModel
from supabase import Client

from app.config import settings
from app.dependencies import get_current_user, get_kite, get_supabase

logger = logging.getLogger("alpha_radar.routes.user")

router = APIRouter(prefix="/api", tags=["user"])

# ── Watchlist persistence ────────────────────────────────────────────────────
WATCHLIST_FILE = settings.DATA_DIR / "watchlist.json"
_watchlist_data: dict = {"symbols": [], "lists": {"default": []}}

if WATCHLIST_FILE.exists():
    try:
        _watchlist_data = json.loads(WATCHLIST_FILE.read_text())
    except Exception:
        pass


def _save_watchlist() -> None:
    try:
        settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
        WATCHLIST_FILE.write_text(json.dumps(_watchlist_data))
    except Exception:
        pass


# ── Free tier limits (for frontend) ─────────────────────────────────────────
FREE_TIER_LIMITS = {
    "quotesBatchMax": settings.FREE_TIER_LIMITS.quotes_batch_max,
    "signalLogMax": settings.FREE_TIER_LIMITS.signal_log_max,
}


# ── GET /api/user-tier ───────────────────────────────────────────────────────
@router.get("/user-tier")
async def user_tier(
    request: Request,
    user: dict = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
) -> dict:
    """Return current user's tier (free/premium) + limits."""
    # Check admin bypass
    admin_header = request.headers.get("X-Admin-Secret", "")
    if admin_header and admin_header == settings.ADMIN_SECRET:
        return {"tier": "premium", "plan": "premium", "status": "active", "expiresAt": None, "limits": None}

    try:
        result = (
            sb.table("subscriptions")
            .select("plan, status, current_period_end")
            .eq("user_id", user["id"])
            .single()
            .execute()
        )
        sub = result.data
    except Exception:
        sub = None

    if not sub:
        return {"tier": "free", "plan": "free", "status": "active", "expiresAt": None, "limits": FREE_TIER_LIMITS}

    plan = sub.get("plan", "free")
    status = sub.get("status", "active")
    is_premium = plan == "premium" and status in ("active", "trialing")

    # Check expiry
    if is_premium and sub.get("current_period_end"):
        try:
            end_dt = datetime.fromisoformat(sub["current_period_end"].replace("Z", "+00:00"))
            if end_dt < datetime.now(timezone.utc):
                is_premium = False
        except (ValueError, TypeError):
            pass

    return {
        "tier": "premium" if is_premium else "free",
        "plan": plan,
        "status": status,
        "expiresAt": sub.get("current_period_end"),
        "limits": None if is_premium else FREE_TIER_LIMITS,
    }


# ── GET /api/tier-config ────────────────────────────────────────────────────
@router.get("/tier-config")
async def tier_config() -> dict:
    """Return feature matrix for UI gating (no auth required)."""
    return {
        "free": {
            "features": [
                "sector-scope", "market-status", "symbols", "expiry",
                "breadth", "fii-dii", "indices", "top-movers",
                "quotes-batch", "live-signals", "watchlist", "signal-log",
            ],
            "limits": FREE_TIER_LIMITS,
        },
        "premium": {
            "price": 6999,
            "currency": "INR",
            "period": "yearly",
            "features": [
                "quotes-batch-full", "52w-bounce", "oi-intelligence",
                "oi-history", "index-candles", "apex-ohlc", "apex-pcr",
                "apex-moneyflux", "pe-radar", "delivery", "signal-log-full",
                "orb-signals", "sector-momentum", "positions", "holdings",
                "last-session", "eod-report",
            ],
        },
    }


# ── GET /api/watchlist ───────────────────────────────────────────────────────
@router.get("/watchlist")
async def get_watchlist(
    user: dict = Depends(get_current_user),
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """Get user's watchlist with live quotes."""
    syms = _watchlist_data.get("symbols", [])
    if not syms:
        return {**_watchlist_data, "quotes": []}

    try:
        kite_syms = [f"NSE:{s}" for s in syms]
        all_q: dict = {}
        for i in range(0, len(kite_syms), 500):
            q = kite.quote(kite_syms[i: i + 500])
            all_q.update(q)

        # Load caches for scores
        try:
            from app.caches import pdh_cache, pdl_cache
            pdh_data = pdh_cache.get_all() if hasattr(pdh_cache, "get_all") else {}
            pdl_data = pdl_cache.get_all() if hasattr(pdl_cache, "get_all") else {}
        except ImportError:
            pdh_data = {}
            pdl_data = {}

        try:
            from app.caches import avg_volume_cache
            avg_vol = avg_volume_cache.get_all() if hasattr(avg_volume_cache, "get_all") else {}
        except ImportError:
            avg_vol = {}

        from app.engines import compute_buying_score, compute_bear_score

        quotes: list[dict] = []
        for s in syms:
            q = all_q.get(f"NSE:{s}")
            if not q:
                quotes.append({"symbol": s})
                continue
            ltp = q.get("last_price", 0)
            prev = q.get("ohlc", {}).get("close", ltp) or ltp
            chg = round(((ltp - prev) / prev * 100), 2) if prev > 0 else 0

            scored = compute_buying_score(
                symbol=s, ltp=ltp, prev_close=prev,
                open_=q.get("ohlc", {}).get("open", ltp) or ltp,
                high=q.get("ohlc", {}).get("high", ltp) or ltp,
                low=q.get("ohlc", {}).get("low", ltp) or ltp,
                volume=q.get("volume", 0) or 0,
                avg_volume=avg_vol.get(s, 0), pdh=pdh_data.get(s, 0), delivery=0,
            )
            bear_scored = compute_bear_score(
                symbol=s, ltp=ltp, prev_close=prev,
                open_=q.get("ohlc", {}).get("open", ltp) or ltp,
                high=q.get("ohlc", {}).get("high", ltp) or ltp,
                low=q.get("ohlc", {}).get("low", ltp) or ltp,
                volume=q.get("volume", 0) or 0,
                avg_volume=avg_vol.get(s, 0), pdl=pdl_data.get(s, 0), delivery=0,
            )
            quotes.append({
                "symbol": s,
                "ltp": round(ltp, 2),
                "chg": chg,
                "volume": q.get("volume", 0) or 0,
                "buyingScore": scored.buying_score,
                "bearScore": bear_scored.bear_score,
                "pdh": pdh_data.get(s),
                "pdl": pdl_data.get(s),
                "volRatio": scored.vol_ratio,
            })

        return {**_watchlist_data, "quotes": quotes}

    except Exception:
        return {**_watchlist_data, "quotes": []}


# ── POST /api/watchlist/add ──────────────────────────────────────────────────
class WatchlistAddBody(BaseModel):
    symbol: str


@router.post("/watchlist/add")
async def add_to_watchlist(
    body: WatchlistAddBody,
    user: dict = Depends(get_current_user),
) -> dict:
    """Add a symbol to the watchlist."""
    sym = body.symbol.upper().strip().replace("NSE:", "")
    if not sym:
        raise HTTPException(status_code=400, detail="symbol required")

    if sym not in _watchlist_data["symbols"]:
        _watchlist_data["symbols"].append(sym)
        _save_watchlist()

    return {"success": True, "symbols": _watchlist_data["symbols"]}


# ── DELETE /api/watchlist/{symbol} ───────────────────────────────────────────
@router.delete("/watchlist/{symbol}")
async def remove_from_watchlist(
    symbol: str = PathParam(..., description="Symbol to remove"),
    user: dict = Depends(get_current_user),
) -> dict:
    """Remove a symbol from the watchlist."""
    sym = symbol.upper()
    _watchlist_data["symbols"] = [s for s in _watchlist_data["symbols"] if s != sym]
    _save_watchlist()
    return {"success": True, "symbols": _watchlist_data["symbols"]}
