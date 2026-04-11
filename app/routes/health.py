"""
Health-check route.
"""

from __future__ import annotations

import time
from datetime import datetime

from fastapi import APIRouter

from app.config import settings
from app.core.kite_client import kite_state

router = APIRouter(tags=["health"])

_START_TIME = time.monotonic()


@router.get("/api/health")
async def health() -> dict:
    """Return service health status."""
    uptime_seconds = round(time.monotonic() - _START_TIME, 1)
    return {
        "status": "ok",
        "authenticated": kite_state.is_connected,
        "uptime": uptime_seconds,
        "timestamp": datetime.now(settings.TIMEZONE).isoformat(),
    }
