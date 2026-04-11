"""
Subscription middleware -- checks user plan and premium status.

Provides FastAPI ``Depends``-compatible callables for:
- Loading subscription info (non-blocking, defaults to free).
- Requiring a premium subscription (raises 403 if not premium).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import Depends, HTTPException, Request, status

from app.core.supabase_client import get_supabase_client
from app.middleware.auth import authenticate_user

logger = logging.getLogger("alpha_radar.middleware.subscription")


@dataclass
class SubscriptionInfo:
    """Subscription state for the current request."""
    plan: str = "free"
    status: str = "active"
    current_period_end: Optional[str] = None
    expired_at: Optional[str] = None
    is_premium: bool = False


def _is_expired(period_end: Optional[str]) -> bool:
    """Return ``True`` if *current_period_end* is in the past."""
    if not period_end:
        return False
    try:
        end_dt = datetime.fromisoformat(period_end.replace("Z", "+00:00"))
        return end_dt < datetime.now(timezone.utc)
    except (ValueError, TypeError):
        return False


async def load_subscription(
    request: Request,
    user: dict[str, Any] = Depends(authenticate_user),
) -> SubscriptionInfo:
    """
    Load subscription info for the authenticated user.

    Never raises -- defaults to free tier on any error.
    Use as a dependency when you want to know the tier but not gate on it.

    Usage::

        @router.get("/data")
        async def data(sub: SubscriptionInfo = Depends(load_subscription)):
            if sub.is_premium:
                ...
    """
    user_id = user.get("id")
    if not user_id:
        return SubscriptionInfo()

    sb = get_supabase_client()
    try:
        result = (
            sb.table("subscriptions")
            .select("plan, status, current_period_end")
            .eq("user_id", user_id)
            .single()
            .execute()
        )
        data = result.data
    except Exception:
        logger.debug("Subscription query failed for user %s, defaulting to free", user_id)
        return SubscriptionInfo()

    if not data:
        return SubscriptionInfo()

    plan = data.get("plan", "free")
    sub_status = data.get("status", "active")
    period_end = data.get("current_period_end")

    if plan == "premium" and sub_status == "active":
        if _is_expired(period_end):
            return SubscriptionInfo(
                plan="free",
                status="expired",
                expired_at=period_end,
            )
        return SubscriptionInfo(
            plan="premium",
            status="active",
            current_period_end=period_end,
            is_premium=True,
        )

    return SubscriptionInfo(plan="free", status=sub_status)


async def require_premium(
    request: Request,
    user: dict[str, Any] = Depends(authenticate_user),
) -> dict[str, Any]:
    """
    Validate that the user has an active premium subscription.

    Raises HTTP 403 if not premium.  Returns the user dict (enriched with
    ``is_premium`` and ``is_admin`` flags) on success.

    Usage::

        @router.get("/premium-only")
        async def premium_only(user: dict = Depends(require_premium)):
            ...
    """
    from app.config import settings

    user_id = user.get("id")

    # Admin bypass
    admin_header = request.headers.get("X-Admin-Secret", "")
    if admin_header and admin_header == settings.ADMIN_SECRET:
        user["is_premium"] = True
        user["is_admin"] = True
        return user

    sb = get_supabase_client()
    try:
        result = (
            sb.table("subscriptions")
            .select("plan, status, current_period_end")
            .eq("user_id", user_id)
            .single()
            .execute()
        )
        data = result.data
    except Exception:
        data = None

    if not data:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "No subscription found",
                "upgrade": True,
                "tier": "free",
                "message": "This feature requires a Premium subscription. Upgrade to unlock full access.",
            },
        )

    plan = data.get("plan", "free")
    sub_status = data.get("status", "")
    period_end = data.get("current_period_end")

    if plan != "premium" or sub_status != "active":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "Premium subscription required",
                "upgrade": True,
                "tier": "free",
                "currentPlan": plan,
                "status": sub_status,
                "message": "This feature requires a Premium subscription. Upgrade to unlock full access.",
            },
        )

    if _is_expired(period_end):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "Subscription expired",
                "upgrade": True,
                "tier": "free",
                "expiredAt": period_end,
                "message": "Your Premium subscription has expired. Renew to continue accessing premium features.",
            },
        )

    user["is_premium"] = True
    user["is_admin"] = False
    return user


def is_premium(sub: SubscriptionInfo) -> bool:
    """Convenience helper to check premium status."""
    return sub.is_premium
