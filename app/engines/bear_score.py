"""
Bear Score Engine — mirror of buying score for SHORT/SELL conviction.

Components (max 100):
1. VOL_SCORE   (0-35): High volume on a DOWN move = conviction selling
2. PDL_SCORE   (0-25): Price broke below Previous Day Low
3. MOMENTUM    (0-20): % decline vs prev close, capped at 3%
4. RANGE_POS   (0-15): Price in BOTTOM 25% of today's range = 15pts
5. DELIVERY    (0-5):  Bonus if delivery % high (real selling)

isPDLBreak: ltp <= pdl * 0.999 AND volRatio >= 1.5
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class BearScoreResult:
    bear_score: int
    vol_score_bear: int
    pdl_score: int
    momentum_score_bear: int
    range_pos_score_bear: int
    delivery_score_bear: int
    vol_ratio_bear: float
    change_pct: float
    is_pdl_break: bool
    pdl: Optional[float]


def compute_bear_score(
    ltp: float,
    open: float,
    high: float,
    low: float,
    close: float,
    volume: int,
    prev_close: float,
    pdl: float,
    avg_volume: float,
    delivery_pct: Optional[float] = None,
) -> BearScoreResult:
    """
    Port of ``computeBearScore`` from Alpha-Radar-backend/server.js (lines 576-619).
    Only scores negative-change stocks.
    """
    change_pct = ((ltp - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
    vol_ratio = round(volume / avg_volume, 2) if avg_volume > 0 else 1.0

    # If stock is green or flat, return zero bear score
    if change_pct >= 0:
        return BearScoreResult(
            bear_score=0,
            vol_score_bear=0,
            pdl_score=0,
            momentum_score_bear=0,
            range_pos_score_bear=0,
            delivery_score_bear=0,
            vol_ratio_bear=vol_ratio,
            change_pct=round(change_pct, 2),
            is_pdl_break=False,
            pdl=pdl or None,
        )

    # 1. Volume score (0-35)
    vol_score = 0.0
    if avg_volume > 0:
        vol_score = max(0.0, min(35.0, (vol_ratio - 1) * (35 / 3)))

    # 2. PDL score (0-25)
    pdl_score = 0
    if pdl > 0 and ltp <= pdl:
        pct = ((pdl - ltp) / pdl) * 100
        pdl_score = 25 if pct >= 1 else (15 if pct >= 0.5 else 10)

    # 3. Momentum score (0-20) — uses absolute change pct
    abs_pct = abs(change_pct)
    momentum_score = min(20.0, (abs_pct / 3) * 20)

    # 4. Range position score (0-15) — bottom 25% = 15pts
    range_ = high - low
    range_pos_score = 0
    if range_ > 0:
        pos = (ltp - low) / range_
        range_pos_score = 15 if pos <= 0.25 else (8 if pos <= 0.5 else 0)

    # 5. Delivery bonus (0-5)
    delivery = delivery_pct if delivery_pct is not None else 0.0
    delivery_score = 5 if delivery >= 60 else (2 if delivery >= 40 else 0)

    total = round(min(100, vol_score + pdl_score + momentum_score + range_pos_score + delivery_score))

    # PDL Break: price below pdl * 0.999 AND volume >= 1.5x avg
    is_pdl_break = pdl > 0 and ltp <= pdl * 0.999 and vol_ratio >= 1.5

    return BearScoreResult(
        bear_score=total,
        vol_score_bear=round(vol_score),
        pdl_score=pdl_score,
        momentum_score_bear=round(momentum_score),
        range_pos_score_bear=range_pos_score,
        delivery_score_bear=delivery_score,
        vol_ratio_bear=vol_ratio,
        change_pct=round(change_pct, 2),
        is_pdl_break=is_pdl_break,
        pdl=pdl or None,
    )
