"""
52-Week Low Bounce Scanner -- detects accumulation near 52-week lows.

Criteria:
  1. Volume surge: 2x avg or more
  2. Green today (changePct > 0)
  3. Price recovering (ltp > open -- trading above open)
  4. Has avg volume data (otherwise skip)
  5. Buying score >= 35

Ported from Alpha-Radar-backend/server.js (lines 912-1018)
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .buying_score import compute_buying_score


def detect_52w_bounce(
    symbol: str,
    ltp: float,
    open_: float,
    high: float,
    low: float,
    prev_close: float,
    volume: int,
    avg_volume: float,
    change_pct: float,
    delivery_pct: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    """
    Detect a potential bounce from near 52-week low territory.

    Parameters
    ----------
    symbol : str
        Stock symbol.
    ltp : float
        Last traded price.
    open_ : float
        Today's open price.
    high : float
        Today's high.
    low : float
        Today's low.
    prev_close : float
        Previous close.
    volume : int
        Today's volume.
    avg_volume : float
        20-day average volume.
    change_pct : float
        Percentage change from previous close.
    delivery_pct : float, optional
        Delivery percentage.

    Returns
    -------
    dict or None
        Bounce signal dict if criteria met, else None.
    """
    # Must have avg volume data
    if avg_volume == 0:
        return None

    vol_ratio = volume / avg_volume

    # Criteria checks
    if vol_ratio < 2:
        return None
    if change_pct <= 0:
        return None
    if ltp <= open_:
        return None

    # Compute buying score
    scored = compute_buying_score(
        ltp=ltp,
        open=open_,
        high=high,
        low=low,
        close=prev_close,
        volume=volume,
        prev_close=prev_close,
        pdh=0,  # PDH not used for bounce detection filter
        avg_volume=avg_volume,
        delivery_pct=delivery_pct or 0,
    )

    if scored.buying_score < 35:
        return None

    return {
        "symbol": symbol,
        "ltp": round(ltp, 2),
        "prev_close": round(prev_close, 2),
        "open": round(open_, 2),
        "high": round(high, 2),
        "low": round(low, 2),
        "change_pct": round(change_pct, 2),
        "volume": volume,
        "avg_volume": avg_volume,
        "vol_ratio": round(vol_ratio, 2),
        "buying_score": scored.buying_score,
        "is_breakout": scored.is_breakout,
        "pdh": scored.pdh,
        "delivery": delivery_pct,
        "signal_type": "52W_BOUNCE",
        "score_breakdown": {
            "vol_score": scored.vol_score,
            "pdh_score": scored.pdh_score,
            "momentum_score": scored.momentum_score,
            "range_pos_score": scored.range_pos_score,
            "delivery_score": scored.delivery_score,
        },
    }
