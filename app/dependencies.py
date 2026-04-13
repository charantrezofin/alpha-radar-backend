"""
FastAPI dependency injection functions.

Usage in routers:
    from app.dependencies import get_kite, get_supabase, get_current_user, require_premium

    @router.get("/something")
    async def something(kite=Depends(get_kite), user=Depends(get_current_user)):
        ...
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import Depends, HTTPException, Request, status
from kiteconnect import KiteConnect
from supabase import Client

from app.config import settings
from app.core.kite_client import kite_state
from app.core.supabase_client import get_supabase_client

logger = logging.getLogger("alpha_radar.deps")


# ---------------------------------------------------------------------------
# Kite
# ---------------------------------------------------------------------------
def get_kite() -> KiteConnect:
    """
    Return the active KiteConnect instance.
    Raises 503 if no access token has been set yet.
    """
    if not kite_state.is_connected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kite is not connected. Please authenticate via /api/auth/kite/login.",
        )
    return kite_state.kite


# ---------------------------------------------------------------------------
# Supabase
# ---------------------------------------------------------------------------
def get_supabase() -> Client:
    """Return the Supabase service-role client."""
    try:
        return get_supabase_client()
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------
def _extract_bearer_token(request: Request) -> str:
    """Pull the Bearer token from the Authorization header."""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or malformed Authorization header",
        )
    return auth_header[7:]


async def _verify_supabase_jwt(token: str) -> dict[str, Any]:
    """
    Verify a Supabase-issued JWT by calling the Supabase Auth /user endpoint.
    Returns the user object if valid.
    """
    if not settings.SUPABASE_URL:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Supabase is not configured",
        )
    url = f"{settings.SUPABASE_URL}/auth/v1/user"
    headers = {
        "Authorization": f"Bearer {token}",
        "apikey": settings.SUPABASE_ANON_KEY,
    }
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
    if resp.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )
    return resp.json()


# ---------------------------------------------------------------------------
# Current user dependency
# ---------------------------------------------------------------------------
_DEV_USER = {
    "id": "dev-local-user",
    "email": "dev@localhost",
    "raw": {},
    "is_dev": True,
}


def _is_localhost(request: Request) -> bool:
    """Check if the request originates from localhost."""
    host = request.headers.get("host", "")
    origin = request.headers.get("origin", "")
    return "localhost" in host or "127.0.0.1" in host or "localhost" in origin


async def get_current_user(request: Request) -> dict[str, Any]:
    """
    Validate the JWT from the request and return a dict with at least
    ``id`` (user UUID) and ``email``.

    If no token is provided, returns a guest user for read-only access.
    This allows the platform to work without requiring login for viewing data.
    Write operations (watchlist, feedback, subscription) should use require_authenticated.
    """
    auth_header = request.headers.get("Authorization", "")

    # Allow unauthenticated access — return guest user for read-only endpoints
    if not auth_header.startswith("Bearer "):
        logger.debug("No auth token — using guest user for read-only access")
        return _DEV_USER

    token = _extract_bearer_token(request)
    user = await _verify_supabase_jwt(token)
    user_id = user.get("id")
    email = user.get("email")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not determine user identity from token",
        )
    return {"id": user_id, "email": email, "raw": user}


# ---------------------------------------------------------------------------
# Premium guard dependency
# ---------------------------------------------------------------------------
async def require_premium(
    request: Request,
    sb: Client = Depends(get_supabase),
) -> dict[str, Any]:
    """
    Validate JWT **and** ensure the user has an active premium subscription.
    Returns the same user dict as ``get_current_user``.
    """
    user = await get_current_user(request)
    user_id: str = user["id"]

    # Localhost dev user — grant premium for development
    if user.get("is_dev"):
        user["is_premium"] = True
        user["is_admin"] = True
        return user

    # Check admin bypass
    admin_header = request.headers.get("X-Admin-Secret", "")
    if admin_header and admin_header == settings.ADMIN_SECRET:
        user["is_premium"] = True
        user["is_admin"] = True
        return user

    # Query subscription status from Supabase
    try:
        result = (
            sb.table("subscriptions")
            .select("plan, status, current_period_end")
            .eq("user_id", user_id)
            .single()
            .execute()
        )
        sub = result.data
    except Exception:
        sub = None

    if not sub:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Premium subscription required",
        )

    plan = sub.get("plan", "free")
    sub_status = sub.get("status", "")
    period_end = sub.get("current_period_end")

    is_active = (
        plan == "premium"
        and sub_status in ("active", "trialing")
    )

    # Also check that the period hasn't expired
    if is_active and period_end:
        try:
            end_dt = datetime.fromisoformat(period_end.replace("Z", "+00:00"))
            if end_dt < datetime.now(timezone.utc):
                is_active = False
        except (ValueError, TypeError):
            pass

    if not is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Premium subscription required",
        )

    user["is_premium"] = True
    user["is_admin"] = False
    return user
