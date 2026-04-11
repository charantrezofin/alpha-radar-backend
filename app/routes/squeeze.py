"""
Squeeze detection route -- monitors OI compression near ATM for explosive moves.

Ported from tradingdesk/apps/gateway/src/routes/squeeze.routes.ts
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path
from kiteconnect import KiteConnect

from app.config import settings
from app.dependencies import get_kite
from app.engines import SqueezeMonitor, OISnapshot
from app.routes.options import INDEX_CONFIG, get_options_chain

logger = logging.getLogger("alpha_radar.routes.squeeze")

router = APIRouter(prefix="/api", tags=["squeeze"])

# ── Per-index squeeze monitors ───────────────────────────────────────────────
_monitors: dict[str, SqueezeMonitor] = {}


def _get_monitor(index_key: str) -> SqueezeMonitor:
    """Get or create a squeeze monitor for the given index."""
    today = datetime.now(settings.TIMEZONE).strftime("%Y-%m-%d")
    if index_key not in _monitors or _monitors[index_key].day_date != today:
        _monitors[index_key] = SqueezeMonitor(index=index_key, day_date=today)
    return _monitors[index_key]


# ── GET /api/squeeze/{index} ─────────────────────────────────────────────────
@router.get("/squeeze/{index}")
async def squeeze_status(
    index: str = Path(..., description="Index key: nifty, banknifty, etc."),
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """Return squeeze status with alerts, score, phase, and OI snapshots."""
    key = index.lower()
    if key not in INDEX_CONFIG:
        raise HTTPException(
            status_code=400,
            detail={"error": "Unknown index", "available": list(INDEX_CONFIG.keys())},
        )

    try:
        chain_data = get_options_chain(kite, key)
        monitor = _get_monitor(key)
        cfg = INDEX_CONFIG[key]
        now = int(time.time() * 1000)

        # Take a snapshot if >2 minutes since last one
        if now - monitor.last_snapshot_time > 120_000:
            chain = chain_data["chain"]
            spot = chain_data["spot"]
            analytics = chain_data["analytics"]
            strike_step = cfg["strikeStep"]

            # Find ATM strike
            atm = min(chain, key=lambda s: abs(s["strike"] - spot)) if chain else None

            # Near ATM = +/-2 strikes
            near_atm = [
                s for s in chain
                if atm and abs(s["strike"] - atm["strike"]) <= strike_step * 2
            ] if atm else []

            # Track running high/low
            prev_highs = [s.spot_high for s in monitor.snapshots] if monitor.snapshots else [spot]
            prev_lows = [s.spot_low for s in monitor.snapshots] if monitor.snapshots else [spot]
            spot_high = max(spot, max(prev_highs) if prev_highs else spot)
            spot_low = min(spot, min(prev_lows) if prev_lows else spot)

            snapshot = OISnapshot(
                time=now,
                time_str=datetime.now(settings.TIMEZONE).strftime("%H:%M"),
                spot=spot,
                total_call_oi=analytics["totalCallOI"],
                total_put_oi=analytics["totalPutOI"],
                pcr=analytics["pcr"],
                atm_call_oi=atm["call"]["oi"] if atm and atm.get("call") else 0,
                atm_put_oi=atm["put"]["oi"] if atm and atm.get("put") else 0,
                near_atm_call_oi=sum(s["call"]["oi"] for s in near_atm if s.get("call")),
                near_atm_put_oi=sum(s["put"]["oi"] for s in near_atm if s.get("put")),
                spot_high=spot_high,
                spot_low=spot_low,
                range=spot_high - spot_low,
            )

            monitor.snapshots.append(snapshot)
            monitor.last_snapshot_time = now
            if len(monitor.snapshots) > 200:
                monitor.snapshots = monitor.snapshots[-200:]

        # Run squeeze detection
        squeeze_result = monitor.detect(
            chain_data["chain"],
            chain_data["spot"],
            cfg["strikeStep"],
            chain_data["analytics"],
        )

        result_dict = squeeze_result.__dict__ if hasattr(squeeze_result, "__dict__") else dict(squeeze_result)
        # Serialize alerts
        if "alerts" in result_dict:
            result_dict["alerts"] = [
                a.__dict__ if hasattr(a, "__dict__") else a
                for a in result_dict["alerts"]
            ]

        return {
            "success": True,
            "index": key,
            "name": cfg["name"],
            "spot": chain_data["spot"],
            **result_dict,
            "snapshotCount": len(monitor.snapshots),
            "snapshots": [
                s.__dict__ if hasattr(s, "__dict__") else s
                for s in monitor.snapshots[-20:]
            ],
            "timestamp": now,
        }

    except Exception as exc:
        logger.exception("[squeeze] Error for %s", key)
        raise HTTPException(status_code=500, detail=str(exc))
