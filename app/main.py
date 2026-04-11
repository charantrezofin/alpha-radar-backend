"""
Alpha Radar backend -- FastAPI application entry point.
"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings

logger = logging.getLogger("alpha_radar")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

# ---------------------------------------------------------------------------
# Kite session persistence helpers
# ---------------------------------------------------------------------------
KITE_TOKEN_PATH = settings.DATA_DIR / "kite_token.json"


def _load_saved_kite_token() -> None:
    """Attempt to restore a previously saved Kite access token."""
    if not KITE_TOKEN_PATH.exists():
        logger.info("No saved Kite token found at %s", KITE_TOKEN_PATH)
        return
    try:
        data = json.loads(KITE_TOKEN_PATH.read_text())
        access_token: str = data.get("access_token", "")
        saved_date: str = data.get("date", "")
        today = datetime.now(settings.TIMEZONE).strftime("%Y-%m-%d")
        if access_token and saved_date == today:
            # Lazy import to avoid circular deps at module level
            from app.core.kite_client import kite_state  # noqa: WPS433
            kite_state.set_access_token(access_token)
            logger.info("Restored Kite access token from %s", saved_date)
        else:
            logger.info("Saved Kite token is stale (%s vs %s), skipping", saved_date, today)
    except Exception:
        logger.exception("Failed to load saved Kite token")


def _save_kite_token() -> None:
    """Persist the current Kite access token to disk so it survives restarts."""
    try:
        from app.core.kite_client import kite_state  # noqa: WPS433
        token = kite_state.access_token
        if not token:
            return
        settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
        today = datetime.now(settings.TIMEZONE).strftime("%Y-%m-%d")
        KITE_TOKEN_PATH.write_text(json.dumps({"access_token": token, "date": today}))
        logger.info("Saved Kite access token to disk")
    except Exception:
        logger.exception("Failed to save Kite token")


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup / shutdown lifecycle."""
    # -- startup --
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Data directory ensured at %s", settings.DATA_DIR.resolve())
    _load_saved_kite_token()
    logger.info("Alpha Radar backend started")
    yield
    # -- shutdown --
    _save_kite_token()
    logger.info("Alpha Radar backend stopped")


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Alpha Radar API",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------
@app.get("/api/health")
async def health() -> dict:
    """Health-check endpoint."""
    from app.core.kite_client import kite_state  # noqa: WPS433
    return {
        "status": "ok",
        "kiteConnected": kite_state.is_connected,
        "timestamp": datetime.now(settings.TIMEZONE).isoformat(),
    }


# ---------------------------------------------------------------------------
# WebSocket hub -- delegates to core.websocket_hub for full functionality
# ---------------------------------------------------------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """Real-time market data WebSocket with options subscription support."""
    from app.core.websocket_hub import ws_hub  # noqa: WPS433

    await ws_hub.connect(websocket)
    try:
        while True:
            raw = await websocket.receive_text()
            await ws_hub.handle_message(websocket, raw)
    except WebSocketDisconnect:
        ws_hub.disconnect(websocket)


# ---------------------------------------------------------------------------
# Mount routers (imported lazily so missing modules don't break startup)
# ---------------------------------------------------------------------------
def _mount_routers() -> None:
    """Import and include all API routers."""
    import importlib

    router_modules = [
        "app.routes.auth",
        "app.routes.health",
        "app.routes.quotes",
        "app.routes.options",
        "app.routes.signals",
        "app.routes.scanner",
        "app.routes.pulse",
        "app.routes.squeeze",
        "app.routes.bounce",
        "app.routes.market",
        "app.routes.fno",
        "app.routes.subscription",
        "app.routes.coupons",
        "app.routes.feedback",
        "app.routes.user",
        "app.routes.history",
    ]
    for module_path in router_modules:
        try:
            mod = importlib.import_module(module_path)
            router = getattr(mod, "router")
            app.include_router(router)
            logger.info("✅ Mounted %s", module_path)
        except ModuleNotFoundError:
            logger.warning("⚠️  Router module %s not found -- skipped", module_path)
        except Exception:
            logger.exception("❌ Failed to mount %s", module_path)


_mount_routers()
