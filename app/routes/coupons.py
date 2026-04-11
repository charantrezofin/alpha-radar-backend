"""
Coupon routes -- generate, list, redeem, revert coupon codes.

Ported from Alpha-Radar-backend/routes/coupons.js
"""

from __future__ import annotations

import logging
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Request
from pydantic import BaseModel
from supabase import Client

from app.config import settings
from app.dependencies import get_current_user, get_supabase

logger = logging.getLogger("alpha_radar.routes.coupons")

router = APIRouter(prefix="/api/coupons", tags=["coupons"])

# ── Helpers ──────────────────────────────────────────────────────────────────
COUPON_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # No 0/O/1/I confusion


def _generate_code() -> str:
    """Generate a coupon code in format AR-XXXX-XXXX."""
    part1 = "".join(secrets.choice(COUPON_CHARS) for _ in range(4))
    part2 = "".join(secrets.choice(COUPON_CHARS) for _ in range(4))
    return f"AR-{part1}-{part2}"


def _require_admin(request: Request) -> None:
    """Check X-Admin-Secret header."""
    secret = request.headers.get("X-Admin-Secret", "")
    if not secret or secret != settings.ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Admin access required")


# ── Request models ───────────────────────────────────────────────────────────
class GenerateBody(BaseModel):
    count: int = 5
    duration_days: int = 90
    label: str = ""


class RedeemBody(BaseModel):
    code: str


# ── POST /api/coupons/generate ───────────────────────────────────────────────
@router.post("/generate")
async def generate_coupons(
    body: GenerateBody,
    request: Request,
    sb: Client = Depends(get_supabase),
) -> dict:
    """Admin: generate a batch of unique coupon codes."""
    _require_admin(request)

    batch_size = min(body.count, 100)
    codes: list[str] = []
    for _ in range(batch_size):
        code = _generate_code()
        attempts = 0
        while code in codes and attempts < 10:
            code = _generate_code()
            attempts += 1
        codes.append(code)

    label = body.label or f"Batch {datetime.now(settings.TIMEZONE).strftime('%d/%m/%Y')}"
    rows = [{"code": c, "duration_days": body.duration_days, "label": label} for c in codes]

    try:
        result = sb.table("coupons").insert(rows).select().execute()
        data = result.data or []
        return {
            "generated": len(data),
            "codes": [d["code"] for d in data],
            "label": label,
            "duration_days": body.duration_days,
        }
    except Exception as exc:
        logger.exception("Coupon generate error")
        raise HTTPException(status_code=500, detail="Failed to generate coupons")


# ── GET /api/coupons/list ────────────────────────────────────────────────────
@router.get("/list")
async def list_coupons(
    request: Request,
    sb: Client = Depends(get_supabase),
) -> dict:
    """Admin: list all coupons with usage info."""
    _require_admin(request)

    try:
        result = sb.table("coupons").select("*").order("created_at", desc=True).execute()
        data = result.data or []
        unused = [c for c in data if not c.get("used_by")]
        used = [c for c in data if c.get("used_by")]

        return {
            "total": len(data),
            "unused": len(unused),
            "used": len(used),
            "coupons": data,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to fetch coupons")


# ── POST /api/coupons/redeem ────────────────────────────────────────────────
@router.post("/redeem")
async def redeem_coupon(
    body: RedeemBody,
    user: dict = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
) -> dict:
    """User: redeem a coupon code to activate premium."""
    if not body.code or not isinstance(body.code, str):
        raise HTTPException(status_code=400, detail="Please enter a coupon code")

    clean_code = body.code.strip().upper()

    # Check existing premium
    try:
        sub_result = (
            sb.table("subscriptions")
            .select("plan, status, current_period_end")
            .eq("user_id", user["id"])
            .single()
            .execute()
        )
        sub = sub_result.data
    except Exception:
        sub = None

    if sub and sub.get("plan") == "premium" and sub.get("status") == "active" and sub.get("current_period_end"):
        try:
            end_dt = datetime.fromisoformat(sub["current_period_end"].replace("Z", "+00:00"))
            if end_dt > datetime.now(timezone.utc):
                raise HTTPException(status_code=400, detail="You already have an active premium subscription")
        except (ValueError, TypeError):
            pass

    # Find coupon
    try:
        coupon_result = sb.table("coupons").select("*").eq("code", clean_code).single().execute()
        coupon = coupon_result.data
    except Exception:
        coupon = None

    if not coupon:
        raise HTTPException(status_code=404, detail="Invalid coupon code")

    if coupon.get("used_by"):
        raise HTTPException(status_code=400, detail="This coupon has already been used")

    # Mark as used
    try:
        sb.table("coupons").update({
            "used_by": user["id"],
            "used_by_email": user.get("email", ""),
            "used_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", coupon["id"]).execute()
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to redeem coupon")

    # Activate premium
    now = datetime.now(timezone.utc)
    expiry = now + timedelta(days=coupon["duration_days"])

    try:
        sb.table("subscriptions").update({
            "plan": "premium",
            "status": "active",
            "current_period_start": now.isoformat(),
            "current_period_end": expiry.isoformat(),
            "updated_at": now.isoformat(),
        }).eq("user_id", user["id"]).execute()
    except Exception as exc:
        logger.exception("Failed to activate premium via coupon")
        raise HTTPException(status_code=500, detail="Failed to activate premium")

    logger.info("Coupon %s redeemed by %s -- premium until %s", clean_code, user.get("email"), expiry.strftime("%Y-%m-%d"))

    return {
        "success": True,
        "plan": "premium",
        "expiresAt": expiry.isoformat(),
        "duration_days": coupon["duration_days"],
    }


# ── POST /api/coupons/{id}/revert ───────────────────────────────────────────
@router.post("/{coupon_id}/revert")
async def revert_coupon(
    coupon_id: str = Path(..., alias="coupon_id"),
    request: Request = None,
    sb: Client = Depends(get_supabase),
) -> dict:
    """Admin: revert a used coupon (mark unused, downgrade user to free)."""
    _require_admin(request)

    try:
        coupon_result = sb.table("coupons").select("*").eq("id", coupon_id).single().execute()
        coupon = coupon_result.data
    except Exception:
        coupon = None

    if not coupon:
        raise HTTPException(status_code=404, detail="Coupon not found")
    if not coupon.get("used_by"):
        raise HTTPException(status_code=400, detail="Coupon has not been used")

    # Revert user subscription
    try:
        sb.table("subscriptions").update({
            "plan": "free",
            "status": "active",
            "current_period_start": None,
            "current_period_end": None,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).eq("user_id", coupon["used_by"]).execute()
    except Exception:
        pass

    # Reset coupon
    try:
        sb.table("coupons").update({
            "used_by": None,
            "used_by_email": None,
            "used_at": None,
        }).eq("id", coupon_id).execute()
    except Exception:
        pass

    logger.info("Coupon %s reverted -- %s downgraded to free", coupon["code"], coupon.get("used_by_email"))
    return {"success": True, "revertedUser": coupon.get("used_by_email")}


# ── DELETE /api/coupons/{id} ──────────────────────────────────────────────
@router.delete("/{coupon_id}")
async def delete_coupon(
    coupon_id: str = Path(..., alias="coupon_id"),
    request: Request = None,
    sb: Client = Depends(get_supabase),
) -> dict:
    """Admin: delete an unused coupon."""
    _require_admin(request)

    try:
        coupon_result = sb.table("coupons").select("*").eq("id", coupon_id).single().execute()
        coupon = coupon_result.data
    except Exception:
        coupon = None

    if not coupon:
        raise HTTPException(status_code=404, detail="Coupon not found")
    if coupon.get("used_by"):
        raise HTTPException(status_code=400, detail="Cannot delete a used coupon. Revert it first.")

    try:
        sb.table("coupons").delete().eq("id", coupon_id).execute()
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to delete coupon")

    logger.info("Coupon %s deleted", coupon["code"])
    return {"success": True, "deleted": coupon["code"]}
