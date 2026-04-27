"""
Admin route for the signal validation tracker.

GET /api/admin/signal-stats?days=15 — per-signal-type win/loss aggregation
GET /api/admin/signal-fires?limit=100 — recent raw fires
GET /api/admin/run-outcome-check?horizon=15m — manual trigger (debug)

All routes require premium (which has admin bypass via X-Admin-Secret or
localhost dev user).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from kiteconnect import KiteConnect

from app.config import settings
from app.dependencies import get_kite, get_supabase, require_premium
from app.services.signal_validator import (
    HORIZONS,
    check_outcomes_for_horizon,
    get_signal_stats,
)

logger = logging.getLogger("alpha_radar.routes.signal_stats")

router = APIRouter(prefix="/api/admin", tags=["admin"])


@router.get("/signal-stats")
async def signal_stats(
    days: int = Query(15, ge=1, le=90, description="Lookback window in days"),
    user: dict = Depends(require_premium),
) -> dict:
    """Per-signal-type, per-horizon win/loss aggregation."""
    return {
        "success": True,
        "as_of": datetime.now(settings.TIMEZONE).isoformat(),
        **get_signal_stats(days=days),
    }


@router.get("/signal-fires")
async def signal_fires(
    limit: int = Query(100, ge=1, le=1000),
    symbol: Optional[str] = Query(None),
    signal_type: Optional[str] = Query(None),
    days: int = Query(15, ge=1, le=90),
    user: dict = Depends(require_premium),
    sb=Depends(get_supabase),
) -> dict:
    """Raw recent signal fires + their outcomes (for debugging / spot-checks)."""
    cutoff = (
        datetime.now(settings.TIMEZONE) - timedelta(days=days)
    ).isoformat()

    try:
        query = (
            sb.table("signal_fires")
            .select("*, signal_outcomes(horizon, status, return_pct, mfe_pct, mae_pct)")
            .gte("fired_at", cutoff)
            .order("fired_at", desc=True)
            .limit(limit)
        )
        if symbol:
            query = query.eq("symbol", symbol.upper())
        if signal_type:
            query = query.eq("signal_type", signal_type.upper())
        result = query.execute()
    except Exception as exc:
        logger.exception("[admin] signal-fires query failed")
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "success": True,
        "count": len(result.data or []),
        "fires": result.data or [],
    }


@router.post("/run-outcome-check")
async def run_outcome_check(
    horizon: str = Query(..., description="15m | 1h | eod | next_day_eod"),
    user: dict = Depends(require_premium),
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """Manually trigger an outcome check (useful for debugging the cron jobs)."""
    if horizon not in HORIZONS:
        raise HTTPException(
            status_code=400,
            detail=f"horizon must be one of {HORIZONS}",
        )
    summary = check_outcomes_for_horizon(horizon, kite)
    return {"success": True, "horizon": horizon, **summary}
