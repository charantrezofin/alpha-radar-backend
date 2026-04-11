"""
Pydantic models for scanner results (CPR, patterns, VCP, NR, volume profile, swing).
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# CPR (Central Pivot Range)
# ---------------------------------------------------------------------------
class CPRLevels(BaseModel):
    """Standard pivot, BC/TC, and support/resistance levels."""
    pivot: float
    bc: float = Field(..., description="Bottom Central Pivot")
    tc: float = Field(..., description="Top Central Pivot")
    r1: float = 0.0
    r2: float = 0.0
    r3: float = 0.0
    s1: float = 0.0
    s2: float = 0.0
    s3: float = 0.0


class DirectionEnum(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"


class AlertTierEnum(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class CPRWidthClassEnum(str, Enum):
    NARROW = "NARROW"
    MEDIUM = "MEDIUM"
    WIDE = "WIDE"


class CPRResult(BaseModel):
    """Scanner output for CPR-based analysis."""
    symbol: str
    score: float = Field(0, ge=0, le=100)
    direction: DirectionEnum = DirectionEnum.NEUTRAL
    alertTier: AlertTierEnum = AlertTierEnum.LOW
    cprLevels: CPRLevels
    cprWidth: float = 0.0
    cprWidthClass: CPRWidthClassEnum = CPRWidthClassEnum.MEDIUM
    pemaStack: str = ""
    pemaSlope: str = ""
    entry: float = 0.0
    sl: float = 0.0
    target1: float = 0.0
    target2: float = 0.0
    qty: int = 0


# ---------------------------------------------------------------------------
# Pattern recognition
# ---------------------------------------------------------------------------
class PatternResult(BaseModel):
    """Detected chart patterns on a symbol."""
    symbol: str
    patterns: list[dict] = Field(default_factory=list, description="List of detected pattern dicts")
    timeframe: str = "daily"


# ---------------------------------------------------------------------------
# VCP (Volatility Contraction Pattern)
# ---------------------------------------------------------------------------
class VCPStageEnum(str, Enum):
    STAGE_1 = "STAGE_1"
    STAGE_2 = "STAGE_2"
    STAGE_3 = "STAGE_3"
    STAGE_4 = "STAGE_4"


class VCPResult(BaseModel):
    """VCP scanner result."""
    symbol: str
    score: float = Field(0, ge=0, le=100)
    stage: VCPStageEnum = VCPStageEnum.STAGE_2
    contractions: int = 0
    trendTemplate: bool = False
    pivot: float = 0.0
    rs: float = Field(0, description="Relative strength vs. benchmark")


# ---------------------------------------------------------------------------
# NR (Narrow Range) squeeze
# ---------------------------------------------------------------------------
class SqueezeTypeEnum(str, Enum):
    NR4 = "NR4"
    NR7 = "NR7"
    INSIDE_BAR = "INSIDE_BAR"


class NRResult(BaseModel):
    """Narrow-range / squeeze scanner result."""
    symbol: str
    squeezeType: SqueezeTypeEnum
    score: float = Field(0, ge=0, le=100)
    bias: DirectionEnum = DirectionEnum.NEUTRAL
    buyAbove: float = 0.0
    sellBelow: float = 0.0
    sl: float = 0.0
    target: float = 0.0


# ---------------------------------------------------------------------------
# Volume Profile
# ---------------------------------------------------------------------------
class VolumeProfileSignalEnum(str, Enum):
    ABOVE_POC = "ABOVE_POC"
    BELOW_POC = "BELOW_POC"
    AT_POC = "AT_POC"
    ABOVE_VAH = "ABOVE_VAH"
    BELOW_VAL = "BELOW_VAL"


class VolumeProfileResult(BaseModel):
    """Volume-profile analysis result."""
    symbol: str
    poc: float = Field(..., description="Point of Control")
    vah: float = Field(..., description="Value Area High")
    val: float = Field(..., description="Value Area Low")
    pricePosition: str = ""
    signal: VolumeProfileSignalEnum = VolumeProfileSignalEnum.AT_POC


# ---------------------------------------------------------------------------
# Swing signal
# ---------------------------------------------------------------------------
class SwingStrategyEnum(str, Enum):
    EMA_PULLBACK = "EMA_PULLBACK"
    BREAKOUT = "BREAKOUT"
    REVERSAL = "REVERSAL"
    TREND_FOLLOW = "TREND_FOLLOW"


class StrengthEnum(str, Enum):
    STRONG = "STRONG"
    MODERATE = "MODERATE"
    WEAK = "WEAK"


class SwingSignal(BaseModel):
    """Swing-trading signal."""
    symbol: str
    strategy: SwingStrategyEnum
    direction: DirectionEnum
    strength: StrengthEnum = StrengthEnum.MODERATE
    entry: float = 0.0
    sl: float = 0.0
    target: float = 0.0
