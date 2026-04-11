"""
Singleton Supabase client.

Provides both the service-role client (for server-side operations)
and a helper to verify JWTs issued by Supabase Auth.
"""

from __future__ import annotations

import logging
from typing import Optional

from supabase import Client, create_client

from app.config import settings

logger = logging.getLogger("alpha_radar.supabase")

_client: Optional[Client] = None


def get_supabase_client() -> Client:
    """Return (and lazily initialise) the Supabase service-role client."""
    global _client
    if _client is None:
        if not settings.SUPABASE_URL or not settings.SUPABASE_SERVICE_ROLE_KEY:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in environment"
            )
        _client = create_client(
            settings.SUPABASE_URL,
            settings.SUPABASE_SERVICE_ROLE_KEY,
        )
        logger.info("Supabase client initialised for %s", settings.SUPABASE_URL)
    return _client
