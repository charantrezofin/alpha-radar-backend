"""
Market Pulse Engine -- real-time signal classification for stock screening.

Computes:
  - rFactor:   today's range / avg 10-day range (range expansion detection)
  - volRatio:  today's volume / avg 10-day volume
  - signalPct: |changePct| * sqrt(volRatio) (dampened momentum)
  - signal:    BULL / BEAR / NEUTRAL classification
  - PDH breakout / PDL breakdown detection

Ported from tradingdesk/apps/gateway/src/routes/pulse.routes.ts
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal, Optional


@dataclass
class PulseResult:
    symbol: str
    ltp: float
    open: float
    high: float
    low: float
    prev_close: float
    change_pct: float
    change: float
    volume: int
    vol_ratio: float
    r_factor: float
    signal_pct: float
    signal: Literal["BULL", "BEAR", "NEUTRAL"]
    pdh_breakout: bool
    pdl_breakdown: bool
    pdh: float
    pdl: float


def compute_pulse(
    symbol: str,
    ltp: float,
    open_: float,
    high: float,
    low: float,
    volume: int,
    change_pct: float,
    avg_volume: float,
    avg_range: float,
    pdh: float,
    pdl: float,
) -> Optional[PulseResult]:
    """
    Port of pulse computation from pulse.routes.ts.

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
    volume : int
        Today's volume.
    change_pct : float
        Percentage change from previous close.
    avg_volume : float
        10-day average volume.
    avg_range : float
        10-day average daily range (high - low).
    pdh : float
        Previous day high.
    pdl : float
        Previous day low.

    Returns
    -------
    PulseResult or None
        Pulse signal; None if stock has too little movement to be actionable.
    """
    prev_close = ltp / (1 + change_pct / 100) if change_pct != 0 else ltp
    today_range = high - low

    # R.Factor = today's range / avg 10-day range
    if avg_range > 0:
        r_factor = today_range / avg_range
    elif today_range > 0:
        r_factor = 1.0
    else:
        r_factor = 0.0

    # Volume ratio
    vol_ratio = volume / avg_volume if avg_volume > 0 else 1.0

    # Signal % = change% weighted by sqrt of volume ratio (dampened)
    signal_pct = abs(change_pct) * math.sqrt(max(vol_ratio, 0.5))

    # PDH breakout / PDL breakdown
    pdh_breakout = pdh > 0 and ltp > pdh and change_pct > 0
    pdl_breakdown = pdl > 0 and ltp < pdl and change_pct < 0

    # Signal classification
    signal: Literal["BULL", "BEAR", "NEUTRAL"] = "NEUTRAL"
    if change_pct > 1 and vol_ratio > 0.8:
        signal = "BULL"
    elif change_pct > 0.5 and vol_ratio > 1.2:
        signal = "BULL"
    elif change_pct > 2:
        signal = "BULL"
    elif change_pct < -1 and vol_ratio > 0.8:
        signal = "BEAR"
    elif change_pct < -0.5 and vol_ratio > 1.2:
        signal = "BEAR"
    elif change_pct < -2:
        signal = "BEAR"

    # Filter: only include stocks with some movement
    if abs(change_pct) < 0.3 and r_factor < 0.5:
        return None

    return PulseResult(
        symbol=symbol,
        ltp=ltp,
        open=open_,
        high=high,
        low=low,
        prev_close=round(prev_close, 2),
        change_pct=round(change_pct, 2),
        change=round(ltp - prev_close, 2),
        volume=volume,
        vol_ratio=round(vol_ratio, 2),
        r_factor=round(r_factor, 2),
        signal_pct=round(signal_pct, 2),
        signal=signal,
        pdh_breakout=pdh_breakout,
        pdl_breakdown=pdl_breakdown,
        pdh=pdh,
        pdl=pdl,
    )
