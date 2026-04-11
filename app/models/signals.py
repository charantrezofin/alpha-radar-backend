"""
Pydantic models for signal logging, ORB state, and live signal summaries.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class OutcomeEnum(str, Enum):
    WIN = "WIN"
    LOSS = "LOSS"
    SKIP = "SKIP"


class SignalLogEntry(BaseModel):
    """Persisted log of a generated signal and its outcome."""
    symbol: str
    signalType: str = Field(..., description="e.g. PDH_CROSS, COMBO_SURGE, PDL_BREAK")
    sector: Optional[str] = None
    entryPrice: float
    entryTime: datetime
    exitPrice: Optional[float] = None
    exitPct: Optional[float] = None
    outcome: Optional[OutcomeEnum] = None


class ORBState(BaseModel):
    """Opening Range Breakout state for a symbol."""
    orbHigh: float = 0.0
    orbLow: float = 0.0
    nearOrb: bool = False
    orbBreak: bool = False
    orbBreakDown: bool = False


class LiveSignalSummary(BaseModel):
    """Aggregated live-signal counters for the current session."""
    pdhCrossCount: int = 0
    comboSurgeCount: int = 0
    pdlCrossCount: int = 0
    comboSellCount: int = 0
    firstCrossTime: Optional[datetime] = None
    firstComboTime: Optional[datetime] = None
    firstPDLTime: Optional[datetime] = None
    firstComboSellTime: Optional[datetime] = None
