"""
Pydantic models for buying-score and bear-score computations.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class BuyingScoreResult(BaseModel):
    """Composite buying (bullish) score for a stock."""
    buyingScore: float = Field(..., ge=0, le=100, description="Aggregate bullish score 0-100")
    volScore: float = Field(0, description="Volume component score")
    pdhScore: float = Field(0, description="Previous-day-high proximity/cross score")
    momentumScore: float = Field(0, description="Intraday momentum score")
    rangePosScore: float = Field(0, description="Position within day's range score")
    deliveryScore: float = Field(0, description="Delivery percentage score")
    volRatio: float = Field(0, description="Current volume / average volume ratio")
    changePct: float = Field(0, description="Percentage change from previous close")
    isBreakout: bool = Field(False, description="True if price crossed PDH")
    pdh: float = Field(0, description="Previous day high")
    avgVolume: float = Field(0, description="20-day average volume")


class BearScoreResult(BaseModel):
    """Composite bearish score for a stock."""
    bearScore: float = Field(..., ge=0, le=100, description="Aggregate bearish score 0-100")
    volScore: float = Field(0, description="Volume component score")
    pdlScore: float = Field(0, description="Previous-day-low proximity/break score")
    momentumScore: float = Field(0, description="Intraday negative momentum score")
    rangePosScore: float = Field(0, description="Position within day's range score (inverted)")
    deliveryScore: float = Field(0, description="Delivery percentage score")
    volRatio: float = Field(0, description="Current volume / average volume ratio")
    changePct: float = Field(0, description="Percentage change from previous close")
    isPDLBreak: bool = Field(False, description="True if price broke below PDL")
    pdl: float = Field(0, description="Previous day low")
    avgVolume: float = Field(0, description="20-day average volume")
