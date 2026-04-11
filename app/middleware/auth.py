"""
Authentication middleware -- validates Supabase JWT tokens.

Provides FastAPI ``Depends``-compatible callables that extract and verify
the Bearer token from the Authorization header using Supabase Auth.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import Depends, HTTPException, Request, status

from app.core.supabase_client import get_supabase_client

logger = logging.getLogger("alpha_radar.middleware.auth")


def _extract_bearer_token(request: Request) -> str:
    """Extract the JWT from the ``Authorization: Bearer <token>`` header."""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid authorization header",
        )
    return auth_header[7:]


async def authenticate_user(request: Request) -> dict[str, Any]:
    """
    Validate the Supabase JWT and return the user dict.

    Usage::

        @router.get("/protected")
        async def protected(user: dict = Depends(authenticate_user)):
            ...

    The returned dict contains at least ``id`` (UUID) and ``email``.
    """
    token = _extract_bearer_token(request)

    sb = get_supabase_client()
    try:
        resp = sb.auth.get_user(token)
        user = resp.user
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Auth validation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed",
        ) from exc

    return {
        "id": user.id,
        "email": user.email,
        "raw": user,
    }
