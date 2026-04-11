"""
Pydantic models for user profiles, subscriptions, coupons, and feedback.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class PlanEnum(str, Enum):
    FREE = "free"
    PREMIUM = "premium"


class StatusEnum(str, Enum):
    ACTIVE = "active"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    TRIALING = "trialing"


class SubscriptionInfo(BaseModel):
    """User's current subscription state."""
    plan: PlanEnum = PlanEnum.FREE
    status: StatusEnum = StatusEnum.ACTIVE
    currentPeriodEnd: Optional[datetime] = None
    isAdmin: bool = False


class UserProfile(BaseModel):
    """Public-facing user profile."""
    id: str
    email: str
    name: Optional[str] = None
    subscription: SubscriptionInfo = Field(default_factory=SubscriptionInfo)


class CouponInfo(BaseModel):
    """Promotional coupon record."""
    code: str
    durationDays: int = Field(..., ge=1)
    label: Optional[str] = None
    usedBy: Optional[str] = None
    usedAt: Optional[datetime] = None


class FeedbackCategoryEnum(str, Enum):
    BUG = "bug"
    FEATURE = "feature"
    GENERAL = "general"
    COMPLAINT = "complaint"


class FeedbackEntry(BaseModel):
    """User-submitted feedback."""
    category: FeedbackCategoryEnum = FeedbackCategoryEnum.GENERAL
    message: str = Field(..., min_length=1, max_length=2000)
    rating: Optional[int] = Field(None, ge=1, le=5)
