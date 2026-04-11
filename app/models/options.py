"""
Pydantic models for options chain analysis, OI signals, and recommendations.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Option chain primitives
# ---------------------------------------------------------------------------
class OptionData(BaseModel):
    """Single option contract quote."""
    tradingSymbol: str
    instrumentToken: int
    last: float = 0.0
    volume: int = 0
    oi: int = 0
    oiChange: int = 0
    iv: float = 0.0
    bidQty: int = 0
    askQty: int = 0


class OptionStrike(BaseModel):
    """A strike row in the option chain."""
    strike: float
    ce: Optional[OptionData] = None
    pe: Optional[OptionData] = None


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------
class OptionsAnalytics(BaseModel):
    """Aggregate analytics derived from the option chain."""
    pcr: float = Field(0.0, description="Put-Call Ratio (OI-based)")
    pcrSentiment: str = Field("Neutral", description="Bullish / Bearish / Neutral")
    maxPainStrike: float = 0.0
    totalCeOI: int = 0
    totalPeOI: int = 0
    topOICalls: list[dict] = Field(default_factory=list)
    topOIPuts: list[dict] = Field(default_factory=list)
    atmIV: float = 0.0


# ---------------------------------------------------------------------------
# OI signal breakdown and composite signal
# ---------------------------------------------------------------------------
class OISignalBreakdown(BaseModel):
    """Individual component scores that compose the OI signal."""
    momentum: float = 0.0
    pcr: float = 0.0
    oiChange: float = 0.0
    maxPain: float = 0.0
    ivSkew: float = 0.0
    oiUnwinding: float = 0.0
    pricePosition: float = 0.0


class DirectionEnum(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"


class OptionTypeEnum(str, Enum):
    CE = "CE"
    PE = "PE"


class ActionEnum(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class SignalRecommendation(BaseModel):
    """Recommended option trade based on OI signal analysis."""
    strike: float
    optionType: OptionTypeEnum
    tradingsymbol: str
    entry: float
    stopLoss: float
    target1: float
    target2: float
    iv: float = 0.0
    oi: int = 0
    volume: int = 0


class CashRecommendation(BaseModel):
    """Cash / equity trade recommendation alongside the OI signal."""
    action: ActionEnum
    entry: float
    stopLoss: float
    target1: float
    target2: float


class CategoryEnum(str, Enum):
    STRONG = "STRONG"
    MODERATE = "MODERATE"
    WEAK = "WEAK"


class OISignal(BaseModel):
    """Composite OI-based directional signal for an index."""
    symbol: str
    category: CategoryEnum
    direction: DirectionEnum
    confidence: float = Field(0.0, ge=0, le=100)
    score: float = 0.0
    scoreBreakdown: OISignalBreakdown = Field(default_factory=OISignalBreakdown)
    recommendation: Optional[SignalRecommendation] = None
    cashRecommendation: Optional[CashRecommendation] = None
    reasons: list[str] = Field(default_factory=list)
    timestamp: Optional[datetime] = None
