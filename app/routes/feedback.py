"""
Feedback routes -- submit and list user feedback.

Ported from Alpha-Radar-backend/routes/feedback.js
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from supabase import Client

from app.config import settings
from app.dependencies import get_current_user, get_supabase

logger = logging.getLogger("alpha_radar.routes.feedback")

router = APIRouter(prefix="/api/feedback", tags=["feedback"])

VALID_CATEGORIES = {"general", "feature_request", "bug", "improvement", "missing_tool"}


def _require_admin(request: Request) -> None:
    """Check X-Admin-Secret header."""
    secret = request.headers.get("X-Admin-Secret", "")
    if not secret or secret != settings.ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Admin access required")


# ── Request model ────────────────────────────────────────────────────────────
class FeedbackBody(BaseModel):
    category: str = "general"
    message: str = Field(..., min_length=5, max_length=2000)
    rating: Optional[int] = Field(None, ge=1, le=5)


# ── POST /api/feedback ───────────────────────────────────────────────────────
@router.post("/")
async def submit_feedback(
    body: FeedbackBody,
    user: dict = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
) -> dict:
    """Submit user feedback."""
    cat = body.category if body.category in VALID_CATEGORIES else "general"
    message = body.message.strip()[:2000]

    try:
        sb.table("feedback").insert({
            "user_id": user["id"],
            "user_email": user.get("email", ""),
            "user_name": user.get("raw", {}).get("user_metadata", {}).get("full_name", user.get("email", "")),
            "category": cat,
            "message": message,
            "rating": body.rating if body.rating and 1 <= body.rating <= 5 else None,
        }).execute()

        logger.info("Feedback from %s: [%s] %s...", user.get("email"), cat, message[:80])
        return {"success": True}
    except Exception as exc:
        logger.exception("Feedback submit error")
        raise HTTPException(status_code=500, detail="Failed to submit feedback")


# ── GET /api/feedback/list ───────────────────────────────────────────────────
@router.get("/list")
async def list_feedback(
    request: Request,
    sb: Client = Depends(get_supabase),
) -> dict:
    """Admin: list all feedback."""
    _require_admin(request)

    try:
        result = (
            sb.table("feedback")
            .select("*")
            .order("created_at", desc=True)
            .limit(200)
            .execute()
        )
        data = result.data or []
        return {"feedback": data, "total": len(data)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to fetch feedback")
