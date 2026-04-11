"""
Subscription routes -- Razorpay payment integration.

Ported from Alpha-Radar-backend/routes/subscription.js
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from supabase import Client

from app.config import settings
from app.dependencies import get_current_user, get_supabase

logger = logging.getLogger("alpha_radar.routes.subscription")

router = APIRouter(prefix="/api/subscription", tags=["subscription"])

# ── Constants ────────────────────────────────────────────────────────────────
AMOUNT_PAISE = 699900  # Rs 6,999
PLAN_DURATION_DAYS = 365


def _get_razorpay():
    """Lazy import razorpay client."""
    if not settings.RAZORPAY_KEY_ID or not settings.RAZORPAY_KEY_SECRET:
        return None
    try:
        import razorpay
        return razorpay.Client(auth=(settings.RAZORPAY_KEY_ID, settings.RAZORPAY_KEY_SECRET))
    except ImportError:
        logger.warning("razorpay package not installed")
        return None


# ── GET /api/subscription/status ─────────────────────────────────────────────
@router.get("/status")
async def subscription_status(
    user: dict = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
) -> dict:
    """Current subscription info."""
    try:
        result = (
            sb.table("subscriptions")
            .select("*")
            .eq("user_id", user["id"])
            .single()
            .execute()
        )
        data = result.data
    except Exception:
        data = None

    if not data:
        return {"plan": "free", "status": "active"}

    # Check if premium has expired
    if data.get("plan") == "premium" and data.get("status") == "active" and data.get("current_period_end"):
        try:
            end_dt = datetime.fromisoformat(data["current_period_end"].replace("Z", "+00:00"))
            if end_dt < datetime.now(timezone.utc):
                sb.table("subscriptions").update({
                    "status": "expired",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }).eq("user_id", user["id"]).execute()
                return {"plan": "premium", "status": "expired", "currentPeriodEnd": data["current_period_end"]}
        except (ValueError, TypeError):
            pass

    return {
        "plan": data.get("plan", "free"),
        "status": data.get("status", "active"),
        "currentPeriodEnd": data.get("current_period_end"),
    }


# ── POST /api/subscription/create ───────────────────────────────────────────
@router.post("/create")
async def create_order(
    user: dict = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
) -> dict:
    """Create a Razorpay order for premium subscription."""
    rp = _get_razorpay()
    if not rp:
        raise HTTPException(status_code=500, detail="Payment system not configured")

    # Check existing active subscription
    try:
        result = sb.table("subscriptions").select("*").eq("user_id", user["id"]).single().execute()
        existing = result.data
    except Exception:
        existing = None

    if existing and existing.get("plan") == "premium" and existing.get("status") == "active":
        end = existing.get("current_period_end")
        if end:
            try:
                end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
                if end_dt > datetime.now(timezone.utc):
                    raise HTTPException(status_code=400, detail="Already subscribed")
            except (ValueError, TypeError):
                pass

    try:
        order = rp.order.create({
            "amount": AMOUNT_PAISE,
            "currency": "INR",
            "receipt": f"alpha_{user['id'][:8]}_{int(time.time())}",
            "notes": {
                "supabase_user_id": user["id"],
                "plan": "premium_yearly",
                "email": user.get("email", ""),
            },
        })

        # Save order ID
        try:
            sb.table("subscriptions").update({
                "razorpay_subscription_id": order["id"],
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }).eq("user_id", user["id"]).execute()
        except Exception:
            pass

        return {
            "orderId": order["id"],
            "amount": AMOUNT_PAISE,
            "currency": "INR",
            "razorpayKeyId": settings.RAZORPAY_KEY_ID,
        }
    except Exception as exc:
        logger.exception("Order create error")
        raise HTTPException(status_code=500, detail="Failed to create payment order")


# ── POST /api/subscription/verify ────────────────────────────────────────────
class VerifyBody(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str


@router.post("/verify")
async def verify_payment(
    body: VerifyBody,
    user: dict = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
) -> dict:
    """Verify Razorpay payment signature and activate premium."""
    # Verify signature
    expected = hmac.new(
        key=settings.RAZORPAY_KEY_SECRET.encode(),
        msg=f"{body.razorpay_order_id}|{body.razorpay_payment_id}".encode(),
        digestmod=hashlib.sha256,
    ).hexdigest()

    if expected != body.razorpay_signature:
        raise HTTPException(status_code=400, detail="Invalid payment signature")

    try:
        now = datetime.now(timezone.utc)
        expiry = now + timedelta(days=PLAN_DURATION_DAYS)

        sb.table("subscriptions").update({
            "plan": "premium",
            "status": "active",
            "razorpay_subscription_id": body.razorpay_order_id,
            "razorpay_customer_id": body.razorpay_payment_id,
            "current_period_start": now.isoformat(),
            "current_period_end": expiry.isoformat(),
            "updated_at": now.isoformat(),
        }).eq("user_id", user["id"]).execute()

        logger.info("Premium activated for user %s until %s", user["id"], expiry.strftime("%Y-%m-%d"))
        return {"success": True, "plan": "premium", "expiresAt": expiry.isoformat()}
    except Exception as exc:
        logger.exception("Payment verify error")
        raise HTTPException(status_code=500, detail="Payment verification failed")


# ── POST /api/subscription/webhook ───────────────────────────────────────────
@router.post("/webhook")
async def razorpay_webhook(
    request: Request,
    sb: Client = Depends(get_supabase),
) -> dict:
    """Razorpay webhook handler for payment.captured and payment.failed."""
    body_bytes = await request.body()
    body_str = body_bytes.decode("utf-8")

    # Verify webhook signature if secret is configured
    webhook_secret = settings.RAZORPAY_WEBHOOK_SECRET
    if webhook_secret:
        signature = request.headers.get("x-razorpay-signature", "")
        expected = hmac.new(
            key=webhook_secret.encode(),
            msg=body_str.encode(),
            digestmod=hashlib.sha256,
        ).hexdigest()
        if signature != expected:
            logger.warning("Webhook signature mismatch")
            raise HTTPException(status_code=400, detail="Invalid webhook signature")

    import json
    try:
        payload = json.loads(body_str)
    except Exception:
        return {"received": True}

    event = payload.get("event", "")
    logger.info("Razorpay webhook: %s", event)

    try:
        if event == "payment.captured":
            payment = payload.get("payload", {}).get("payment", {}).get("entity", {})
            user_id = payment.get("notes", {}).get("supabase_user_id")
            if user_id:
                now = datetime.now(timezone.utc)
                expiry = now + timedelta(days=PLAN_DURATION_DAYS)
                sb.table("subscriptions").update({
                    "plan": "premium",
                    "status": "active",
                    "current_period_start": now.isoformat(),
                    "current_period_end": expiry.isoformat(),
                    "updated_at": now.isoformat(),
                }).eq("user_id", user_id).execute()
                logger.info("Webhook: Premium activated for %s", user_id)

        elif event == "payment.failed":
            payment = payload.get("payload", {}).get("payment", {}).get("entity", {})
            user_id = payment.get("notes", {}).get("supabase_user_id")
            if user_id:
                logger.info("Webhook: Payment failed for %s", user_id)

    except Exception as exc:
        logger.exception("Webhook processing error")

    return {"received": True}
