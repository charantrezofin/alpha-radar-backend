"""
Futures Pre-Screen Engine -- 4-component scoring for F&O stock pre-filtering.

Components:
1. Price Momentum  (+/-30): Based on changePct thresholds
2. Futures Basis   (+/-15): Premium/discount of futures vs spot
3. OI Build        (+/-25): OI change with price confirmation
4. Volume Spike    (+/-10): High volume/OI ratio

Ported from tradingdesk/apps/gateway/src/routes/signals.routes.ts (lines 74-147)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Literal, Optional


def _format_oi(oi: float) -> str:
    abs_oi = abs(oi)
    if abs_oi >= 10_000_000:
        return f"{oi / 10_000_000:.1f}Cr"
    if abs_oi >= 100_000:
        return f"{oi / 100_000:.1f}L"
    if abs_oi >= 1_000:
        return f"{oi / 1_000:.1f}K"
    return str(int(oi))


@dataclass
class FuturesPreScreen:
    name: str
    spot: float
    fut_price: float
    change: float
    change_pct: float
    oi: int
    oi_change: int
    volume: int
    pre_score: int
    pre_direction: Literal["BULLISH", "BEARISH", "NEUTRAL"]
    pre_reasons: List[str] = field(default_factory=list)


def prescreen_stock(
    name: str,
    spot: float,
    fut_price: float,
    change_pct: float,
    oi: int,
    oi_change: int,
    volume: int,
) -> FuturesPreScreen:
    """
    Port of ``preScreenStock`` from signals.routes.ts.

    Parameters
    ----------
    name : str
        Stock symbol name.
    spot : float
        Spot price.
    fut_price : float
        Futures last traded price.
    change_pct : float
        Percentage change of the futures price from previous close.
    oi : int
        Current open interest.
    oi_change : int
        Change in OI (oi - oi_day_low).
    volume : int
        Today's futures volume.
    """
    pre_score = 0
    reasons: List[str] = []

    # 1. Price momentum (+/-30)
    if change_pct >= 3:
        pre_score += 30
        reasons.append(f"Strong rally +{change_pct:.1f}%")
    elif change_pct >= 1.5:
        pre_score += 20
        reasons.append(f"Bullish move +{change_pct:.1f}%")
    elif change_pct >= 0.5:
        pre_score += 10
    elif change_pct <= -3:
        pre_score -= 30
        reasons.append(f"Sharp fall {change_pct:.1f}%")
    elif change_pct <= -1.5:
        pre_score -= 20
        reasons.append(f"Bearish move {change_pct:.1f}%")
    elif change_pct <= -0.5:
        pre_score -= 10

    # 2. Futures premium/discount (+/-15)
    if spot > 0:
        basis_pct = ((fut_price - spot) / spot) * 100
        if basis_pct > 0.5:
            pre_score += 15
            reasons.append(f"Futures premium +{basis_pct:.2f}%")
        elif basis_pct > 0.2:
            pre_score += 8
        elif basis_pct < -0.5:
            pre_score -= 15
            reasons.append(f"Futures discount {basis_pct:.2f}%")
        elif basis_pct < -0.2:
            pre_score -= 8

    # 3. OI build with price confirmation (+/-25)
    if oi_change > 0 and change_pct > 0.3:
        pre_score += 25
        reasons.append(f"Long buildup -- OI +{_format_oi(oi_change)} with price up")
    elif oi_change > 0 and change_pct < -0.3:
        pre_score -= 25
        reasons.append(f"Short buildup -- OI +{_format_oi(oi_change)} with price down")
    elif oi_change < 0 and change_pct > 0.3:
        pre_score += 15
        reasons.append(f"Short covering -- OI {_format_oi(oi_change)} with price up")
    elif oi_change < 0 and change_pct < -0.3:
        pre_score -= 15
        reasons.append(f"Long unwinding -- OI {_format_oi(oi_change)} with price down")

    # 4. Volume spike (+/-10)
    if volume > 0 and oi > 0 and volume / oi > 0.5:
        pre_score += 10 if change_pct > 0 else -10
        reasons.append("High volume/OI ratio -- strong conviction")

    # Compute change from change_pct and fut_price
    # change = fut_price - close, and change_pct = change/close * 100
    # so close = fut_price / (1 + change_pct/100), change = fut_price - close
    if change_pct != 0:
        close_approx = fut_price / (1 + change_pct / 100)
        change = fut_price - close_approx
    else:
        change = 0.0

    pre_direction: Literal["BULLISH", "BEARISH", "NEUTRAL"] = (
        "BULLISH" if pre_score > 15 else ("BEARISH" if pre_score < -15 else "NEUTRAL")
    )

    return FuturesPreScreen(
        name=name,
        spot=spot,
        fut_price=fut_price,
        change=round(change, 2),
        change_pct=round(change_pct, 2),
        oi=oi,
        oi_change=oi_change,
        volume=volume,
        pre_score=pre_score,
        pre_direction=pre_direction,
        pre_reasons=reasons,
    )
