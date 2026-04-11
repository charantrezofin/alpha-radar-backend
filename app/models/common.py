"""
Shared / common Pydantic models used across the application.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class OHLCBar(BaseModel):
    """Single OHLCV candle bar."""
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int = 0


class SymbolQuote(BaseModel):
    """Real-time quote for a single instrument."""
    symbol: str = Field(..., description="NSE symbol, e.g. RELIANCE")
    tradingSymbol: str = Field(..., description="Exchange-prefixed symbol, e.g. NSE:RELIANCE")
    instrumentToken: int
    last: float = Field(..., description="Last traded price")
    open: float
    high: float
    low: float
    close: float = Field(..., description="Previous close")
    volume: int = 0
    change: float = 0.0
    changePct: float = 0.0
    oi: int = 0
    timestamp: Optional[datetime] = None
    sector: Optional[str] = None


class MarketStatusEnum(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


class MarketStatus(BaseModel):
    """Current market open/closed status."""
    status: MarketStatusEnum
    timestamp: datetime


class IndexTick(BaseModel):
    """Tick-level data for a market index."""
    symbol: str = Field(..., description="Short key, e.g. nifty")
    name: str = Field(..., description="Display name, e.g. NIFTY 50")
    last: float
    change: float = 0.0
    changePct: float = 0.0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    timestamp: Optional[datetime] = None
