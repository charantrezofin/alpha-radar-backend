"""
Singleton Kite Connect client wrapper.

Manages the KiteConnect instance and its access-token lifecycle.
"""

from __future__ import annotations

import logging
from typing import Optional

from kiteconnect import KiteConnect

from app.config import settings

logger = logging.getLogger("alpha_radar.kite")


class KiteState:
    """Thread-safe holder for the KiteConnect session."""

    def __init__(self) -> None:
        self._kite = KiteConnect(api_key=settings.KITE_API_KEY)
        self._access_token: Optional[str] = None

    @property
    def kite(self) -> KiteConnect:
        return self._kite

    @property
    def access_token(self) -> Optional[str]:
        return self._access_token

    @property
    def is_connected(self) -> bool:
        return self._access_token is not None

    def set_access_token(self, token: str) -> None:
        """Apply the access token to the KiteConnect instance."""
        self._access_token = token
        self._kite.set_access_token(token)
        logger.info("Kite access token set")

    def clear(self) -> None:
        """Invalidate the current session."""
        self._access_token = None
        self._kite = KiteConnect(api_key=settings.KITE_API_KEY)
        logger.info("Kite session cleared")

    def get_login_url(self) -> str:
        """Return the Kite login URL for the user to authenticate."""
        return self._kite.login_url()

    def generate_session(self, request_token: str) -> dict:
        """
        Exchange a request_token for an access_token and persist it.
        Returns the full session dict from Kite.
        """
        session = self._kite.generate_session(
            request_token,
            api_secret=settings.KITE_API_SECRET,
        )
        token = session["access_token"]
        self.set_access_token(token)
        return session


# Module-level singleton
kite_state = KiteState()
