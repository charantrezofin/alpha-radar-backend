"""
Application configuration loaded from environment variables.
"""

import os
from dataclasses import dataclass, field
from datetime import time
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class TierLimits:
    """Rate/usage limits for a subscription tier."""
    quotes_batch_max: int
    signal_log_max: int


@dataclass(frozen=True)
class Settings:
    """Central application settings sourced from environment variables."""

    # --- Kite Connect ---
    KITE_API_KEY: str = os.getenv("KITE_API_KEY", "")
    KITE_API_SECRET: str = os.getenv("KITE_API_SECRET", "")

    # --- Supabase ---
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_SERVICE_ROLE_KEY: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    SUPABASE_ANON_KEY: str = os.getenv("SUPABASE_ANON_KEY", "")

    # --- Razorpay ---
    RAZORPAY_KEY_ID: str = os.getenv("RAZORPAY_KEY_ID", "")
    RAZORPAY_KEY_SECRET: str = os.getenv("RAZORPAY_KEY_SECRET", "")
    RAZORPAY_PLAN_ID: str = os.getenv("RAZORPAY_PLAN_ID", "")
    RAZORPAY_WEBHOOK_SECRET: str = os.getenv("RAZORPAY_WEBHOOK_SECRET", "")

    # --- Domain ---
    DOMAIN: str = os.getenv("DOMAIN", "localhost")

    # --- Telegram ---
    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # --- Admin ---
    ADMIN_SECRET: str = os.getenv("ADMIN_SECRET", "")

    # --- Server ---
    PORT: int = int(os.getenv("PORT", "8000"))
    CORS_ORIGINS: list[str] = field(default_factory=lambda: [
        o.strip()
        for o in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")
        if o.strip()
    ])
    AUTH_REDIRECT_URL: str = os.getenv("AUTH_REDIRECT_URL", "http://localhost:3000")

    # --- Storage ---
    DATA_DIR: Path = Path(os.getenv("DATA_DIR", "./data"))

    # --- Market hours (IST) ---
    TIMEZONE: ZoneInfo = field(default_factory=lambda: ZoneInfo("Asia/Kolkata"))
    MARKET_OPEN: time = time(9, 15)
    MARKET_CLOSE: time = time(15, 30)

    # --- Tier limits ---
    FREE_TIER_LIMITS: TierLimits = TierLimits(quotes_batch_max=15, signal_log_max=5)
    PREMIUM_TIER_LIMITS: TierLimits = TierLimits(quotes_batch_max=500, signal_log_max=999)

    def tier_limits(self, is_premium: bool) -> TierLimits:
        """Return the appropriate tier limits."""
        return self.PREMIUM_TIER_LIMITS if is_premium else self.FREE_TIER_LIMITS


# Singleton used throughout the application
settings = Settings()
