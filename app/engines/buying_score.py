"""
Buying Score Engine — 5-component scoring system for BUY conviction.

Components (max 100):
1. VOL_SCORE   (0-35): How much today's volume exceeds the 20-day avg
2. PDH_SCORE   (0-25): Is price breaking yesterday's high, and by how much?
3. MOMENTUM    (0-20): % change vs prev close, capped at 3%
4. RANGE_POS   (0-15): Where is LTP within today's high-low range?
5. DELIVERY    (0-5):  Bonus if delivery % is high (real buying)

isBreakout: ltp >= pdh * 1.001 AND volRatio >= 1.5
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class BuyingScoreResult:
    buying_score: int
    vol_score: int
    pdh_score: int
    momentum_score: int
    range_pos_score: int
    delivery_score: int
    vol_ratio: float
    change_pct: float
    is_breakout: bool
    pdh: Optional[float]
    avg_volume: Optional[float]


def compute_buying_score(
    ltp: float,
    open: float,
    high: float,
    low: float,
    close: float,
    volume: int,
    prev_close: float,
    pdh: float,
    avg_volume: float,
    delivery_pct: Optional[float] = None,
) -> BuyingScoreResult:
    """
    Port of ``computeScore`` from Alpha-Radar-backend/server.js (lines 645-697).
    """
    change_pct = ((ltp - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
    vol_ratio = round(volume / avg_volume, 2) if avg_volume > 0 else 1.0

    # 1. Volume score (0-35)
    vol_score = 0.0
    if avg_volume > 0:
        vol_score = (vol_ratio - 1) * (35 / 3)  # 4x = 35pts
        vol_score = max(0.0, min(35.0, vol_score))

    # 2. PDH score (0-25)
    pdh_score = 0
    if pdh > 0 and ltp >= pdh:
        pct = ((ltp - pdh) / pdh) * 100
        pdh_score = 25 if pct >= 1 else (15 if pct >= 0.5 else 10)

    # 3. Momentum score (0-20)
    momentum_score = min(20.0, (change_pct / 3) * 20) if change_pct > 0 else 0.0

    # 4. Range position score (0-15)
    range_ = high - low
    range_pos_score = 0
    if range_ > 0:
        pos = (ltp - low) / range_  # 0 = at low, 1 = at high
        range_pos_score = 15 if pos >= 0.75 else (8 if pos >= 0.5 else 0)

    # 5. Delivery bonus (0-5)
    delivery = delivery_pct if delivery_pct is not None else 0.0
    delivery_score = 5 if delivery >= 60 else (2 if delivery >= 40 else 0)

    total = round(min(100, vol_score + pdh_score + momentum_score + range_pos_score + delivery_score))

    # Breakout: must cross PDH + have volume confirmation (>=1.5x avg)
    is_breakout = pdh > 0 and ltp >= pdh * 1.001 and vol_ratio >= 1.5

    return BuyingScoreResult(
        buying_score=total,
        vol_score=round(vol_score),
        pdh_score=pdh_score,
        momentum_score=round(momentum_score),
        range_pos_score=range_pos_score,
        delivery_score=delivery_score,
        vol_ratio=vol_ratio,
        change_pct=round(change_pct, 2),
        is_breakout=is_breakout,
        pdh=pdh or None,
        avg_volume=avg_volume or None,
    )
