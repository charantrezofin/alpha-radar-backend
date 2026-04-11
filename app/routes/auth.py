"""
Kite Connect authentication routes.

Handles OAuth login flow: generate login URL, exchange request_token,
persist session, and check auth status.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse

from app.config import settings
from app.core.kite_client import kite_state

logger = logging.getLogger("alpha_radar.routes.auth")

router = APIRouter(prefix="/api/auth", tags=["auth"])

KITE_TOKEN_PATH = settings.DATA_DIR / "kite_token.json"


def _save_token_to_disk(access_token: str, user_name: str) -> None:
    """Persist Kite access token so it survives server restarts."""
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(settings.TIMEZONE).strftime("%Y-%m-%d")
    payload = {
        "access_token": access_token,
        "date": today,
        "user": user_name,
    }
    KITE_TOKEN_PATH.write_text(json.dumps(payload))
    logger.info("Kite token saved to disk for %s", user_name)


# ── GET /api/auth/login-url ──────────────────────────────────────────────────
@router.get("/login-url")
async def login_url() -> dict:
    """Return the Kite Connect OAuth login URL."""
    return {"url": kite_state.get_login_url()}


# ── GET /api/auth/callback ───────────────────────────────────────────────────
@router.get("/callback")
async def callback(request_token: str = Query(..., description="Kite OAuth request_token")):
    """
    Exchange the request_token for an access_token, persist it,
    and redirect the user to the frontend dashboard.
    """
    if not request_token:
        return JSONResponse(status_code=400, content={"error": "No request_token"})

    try:
        session = kite_state.generate_session(request_token)
        user_name = session.get("user_name", "unknown")
        access_token = session.get("access_token", "")

        _save_token_to_disk(access_token, user_name)
        logger.info("Kite login successful for %s", user_name)

        # Redirect to frontend with success flag
        redirect_url = settings.AUTH_REDIRECT_URL
        return RedirectResponse(url=f"{redirect_url}/alpha-login?auth=success")

    except Exception as exc:
        logger.exception("Kite auth callback failed")
        return JSONResponse(status_code=500, content={"error": str(exc)})


# ── GET /api/auth/status ─────────────────────────────────────────────────────
@router.get("/status")
async def status() -> dict:
    """Return current Kite authentication status."""
    token_date: str | None = None
    user: str | None = None

    if KITE_TOKEN_PATH.exists():
        try:
            data = json.loads(KITE_TOKEN_PATH.read_text())
            token_date = data.get("date")
            user = data.get("user")
        except Exception:
            pass

    return {
        "authenticated": kite_state.is_connected,
        "user": user,
        "tokenDate": token_date,
        "loginUrl": kite_state.get_login_url(),
    }
