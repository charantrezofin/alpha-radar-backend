"""
Generic thread-safe cache with TTL and optional disk persistence.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Optional

from app.config import settings

logger = logging.getLogger("alpha_radar.cache")


class Cache:
    """
    In-memory key-value cache with optional TTL expiry and disk persistence.

    Parameters
    ----------
    name : str
        Human-readable name (used in logs).
    ttl : float | None
        Time-to-live in seconds. ``None`` means entries never expire.
    persist_path : str | None
        Relative filename inside ``settings.DATA_DIR`` for JSON persistence.
        ``None`` disables disk persistence.
    """

    def __init__(
        self,
        name: str,
        ttl: Optional[float] = None,
        persist_path: Optional[str] = None,
    ) -> None:
        self.name = name
        self.ttl = ttl
        self.persist_path = persist_path
        self._store: dict[str, Any] = {}
        self._timestamps: dict[str, float] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def get(self, key: str) -> Any | None:
        """Return the cached value or ``None`` if missing / expired."""
        with self._lock:
            if key not in self._store:
                return None
            if self._is_expired(key):
                del self._store[key]
                del self._timestamps[key]
                return None
            return self._store[key]

    def set(self, key: str, value: Any) -> None:
        """Store a value under *key* with the current timestamp."""
        with self._lock:
            self._store[key] = value
            self._timestamps[key] = time.time()

    def has(self, key: str) -> bool:
        """Check if *key* exists and is not expired."""
        return self.get(key) is not None

    def delete(self, key: str) -> None:
        """Remove a single key."""
        with self._lock:
            self._store.pop(key, None)
            self._timestamps.pop(key, None)

    def clear(self) -> None:
        """Drop all entries."""
        with self._lock:
            self._store.clear()
            self._timestamps.clear()
        logger.debug("Cache '%s' cleared", self.name)

    def keys(self) -> list[str]:
        """Return non-expired keys."""
        with self._lock:
            return [k for k in list(self._store) if not self._is_expired(k)]

    def values(self) -> list[Any]:
        """Return all non-expired values."""
        with self._lock:
            return [
                self._store[k]
                for k in list(self._store)
                if not self._is_expired(k)
            ]

    def items(self) -> list[tuple[str, Any]]:
        """Return all non-expired (key, value) pairs."""
        with self._lock:
            return [
                (k, self._store[k])
                for k in list(self._store)
                if not self._is_expired(k)
            ]

    def size(self) -> int:
        return len(self.keys())

    def get_all(self) -> dict[str, Any]:
        """Return a shallow copy of all non-expired entries."""
        return dict(self.items())

    def set_many(self, data: dict[str, Any]) -> None:
        """Bulk-set multiple key-value pairs."""
        now = time.time()
        with self._lock:
            for k, v in data.items():
                self._store[k] = v
                self._timestamps[k] = now

    # ------------------------------------------------------------------
    # Disk persistence
    # ------------------------------------------------------------------

    def save_to_disk(self) -> None:
        """Persist current cache contents to a JSON file."""
        if not self.persist_path:
            return
        filepath = settings.DATA_DIR / self.persist_path
        try:
            settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
            payload = {
                "name": self.name,
                "saved_at": time.time(),
                "data": self._store,
            }
            filepath.write_text(json.dumps(payload, default=str))
            logger.info("Cache '%s' saved to %s (%d entries)", self.name, filepath, len(self._store))
        except Exception:
            logger.exception("Failed to save cache '%s' to disk", self.name)

    def load_from_disk(self) -> bool:
        """
        Restore cache contents from disk. Returns ``True`` if successful.
        """
        if not self.persist_path:
            return False
        filepath = settings.DATA_DIR / self.persist_path
        if not filepath.exists():
            return False
        try:
            payload = json.loads(filepath.read_text())
            data = payload.get("data", {})
            now = time.time()
            with self._lock:
                self._store = data
                self._timestamps = {k: now for k in data}
            logger.info(
                "Cache '%s' loaded from disk (%d entries)", self.name, len(data)
            )
            return True
        except Exception:
            logger.exception("Failed to load cache '%s' from disk", self.name)
            return False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _is_expired(self, key: str) -> bool:
        if self.ttl is None:
            return False
        ts = self._timestamps.get(key, 0.0)
        return (time.time() - ts) > self.ttl
