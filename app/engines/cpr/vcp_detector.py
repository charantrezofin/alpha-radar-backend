"""
engine/vcp_detector.py
=======================
Volume Contraction Pattern (VCP) detector — Mark Minervini's SEPA methodology.

Identifies stocks forming tightening price contractions with declining volume,
signaling institutional accumulation before a breakout.

Scoring criteria:
1. Trend Template (must pass to qualify)
2. Contraction detection (2-5 successive tighter pullbacks)
3. Volume dry-up through each contraction
4. Pivot proximity (how close to breakout point)
5. Relative strength vs index

Returns VCPResult with stage, contractions, pivot, and readiness score.
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Optional
from .config import cfg


@dataclass
class Contraction:
    """A single contraction (pullback) within the VCP."""
    number: int              # 1st, 2nd, 3rd contraction
    start_idx: int
    end_idx: int
    high_price: float        # Swing high before pullback
    low_price: float         # Swing low of pullback
    depth_pct: float         # Pullback depth as % of high
    avg_volume: float        # Average volume during this contraction
    duration_bars: int       # How many bars this contraction lasted


@dataclass
class VCPResult:
    """Complete VCP analysis for a single symbol."""
    symbol: str
    current_price: float

    # Trend template checks
    trend_template_pass: bool
    price_above_150ma: bool
    price_above_200ma: bool
    ma150_above_200ma: bool
    ma200_rising: bool
    pct_from_52w_high: float    # Negative = below high
    pct_from_52w_low: float     # Positive = above low

    # VCP detection
    vcp_detected: bool
    num_contractions: int
    contractions: list[Contraction]
    volume_declining: bool       # Volume decreasing across contractions
    tightening: bool             # Each contraction shallower than previous

    # Pivot & breakout
    pivot_price: float           # Breakout level
    pivot_distance_pct: float    # How far price is from pivot (negative = below)
    pivot_tight_range_pct: float # Width of the tight area near pivot

    # Relative strength
    rs_vs_index: float           # Price change % - index change % (higher = stronger)

    # Overall score (0-100)
    vcp_score: int
    stage: str                   # "Setting Up", "Near Pivot", "Breakout", "Not Ready"
    notes: list[str]


def compute_sma(series: pd.Series, period: int) -> pd.Series:
    """Simple moving average."""
    return series.rolling(window=period, min_periods=period).mean()


def find_contractions(
    df: pd.DataFrame,
    lookback: int = 120,
    swing_window: int = 8,
) -> list[Contraction]:
    """
    Detect successive contractions (pullbacks) in price action.

    Algorithm:
    1. Find swing highs and swing lows in the lookback period
    2. Pair each swing high with the following swing low = one contraction
    3. Return list of contractions ordered by time
    """
    data = df.tail(lookback).copy().reset_index(drop=True)
    highs = data["high"].values
    lows = data["low"].values
    n = len(data)

    # Find swing highs
    swing_highs = []
    for i in range(swing_window, n - swing_window):
        window = highs[i - swing_window: i + swing_window + 1]
        if highs[i] == max(window) and list(window).count(highs[i]) == 1:
            swing_highs.append((i, float(highs[i])))

    # Find swing lows
    swing_lows = []
    for i in range(swing_window, n - swing_window):
        window = lows[i - swing_window: i + swing_window + 1]
        if lows[i] == min(window) and list(window).count(lows[i]) == 1:
            swing_lows.append((i, float(lows[i])))

    if len(swing_highs) < 2 or len(swing_lows) < 1:
        return []

    # Pair swing highs with following swing lows to form contractions
    contractions = []
    contraction_num = 0

    for hi_idx, hi_price in swing_highs:
        # Find the next swing low after this high
        next_lows = [(li, lp) for li, lp in swing_lows if li > hi_idx]
        if not next_lows:
            continue

        lo_idx, lo_price = next_lows[0]
        depth_pct = ((hi_price - lo_price) / hi_price) * 100

        # Only count meaningful pullbacks (> 2% depth)
        if depth_pct < 2:
            continue

        contraction_num += 1
        avg_vol = float(data["volume"].iloc[hi_idx:lo_idx + 1].mean()) if lo_idx > hi_idx else 0

        contractions.append(Contraction(
            number=contraction_num,
            start_idx=hi_idx,
            end_idx=lo_idx,
            high_price=hi_price,
            low_price=lo_price,
            depth_pct=round(depth_pct, 2),
            avg_volume=avg_vol,
            duration_bars=lo_idx - hi_idx,
        ))

    return contractions


def analyze_vcp(
    df: pd.DataFrame,
    symbol: str,
    index_change_pct: float = 0.0,
    lookback: int = 120,
) -> VCPResult:
    """
    Full VCP analysis for a single stock.

    Args:
        df: OHLC DataFrame with at least 200 rows for MA calculation
        symbol: Stock symbol
        index_change_pct: Nifty 50 change % over same period (for RS calc)
        lookback: Bars to look back for contraction detection
    """
    if len(df) < 50:
        return _empty_result(symbol, 0, "Not enough data")

    close = df["close"].values
    current_price = float(close[-1])
    notes: list[str] = []

    # ── 1. Trend Template ────────────────────────────────────────────────

    ma50 = compute_sma(df["close"], 50)
    ma150 = compute_sma(df["close"], 150) if len(df) >= 150 else pd.Series([np.nan] * len(df))
    ma200 = compute_sma(df["close"], 200) if len(df) >= 200 else pd.Series([np.nan] * len(df))

    ma50_val = float(ma50.iloc[-1]) if not pd.isna(ma50.iloc[-1]) else current_price
    ma150_val = float(ma150.iloc[-1]) if not pd.isna(ma150.iloc[-1]) else current_price
    ma200_val = float(ma200.iloc[-1]) if not pd.isna(ma200.iloc[-1]) else current_price

    price_above_150 = current_price > ma150_val
    price_above_200 = current_price > ma200_val
    ma150_above_200 = ma150_val > ma200_val

    # 200 MA rising — check if current > 1 month ago
    ma200_1m_ago = float(ma200.iloc[-22]) if len(ma200) >= 22 and not pd.isna(ma200.iloc[-22]) else ma200_val
    ma200_rising = ma200_val > ma200_1m_ago

    trend_template_pass = price_above_150 and price_above_200 and ma150_above_200 and ma200_rising

    if trend_template_pass:
        notes.append("Trend template: PASS — price > 150 MA > 200 MA, 200 MA rising")
    else:
        reasons = []
        if not price_above_150: reasons.append("below 150 MA")
        if not price_above_200: reasons.append("below 200 MA")
        if not ma150_above_200: reasons.append("150 MA < 200 MA")
        if not ma200_rising: reasons.append("200 MA not rising")
        notes.append(f"Trend template: FAIL — {', '.join(reasons)}")

    # 52-week high/low position
    high_52w = float(df["high"].tail(252).max()) if len(df) >= 252 else float(df["high"].max())
    low_52w = float(df["low"].tail(252).min()) if len(df) >= 252 else float(df["low"].min())
    pct_from_high = ((current_price - high_52w) / high_52w) * 100
    pct_from_low = ((current_price - low_52w) / low_52w) * 100

    within_25_of_high = pct_from_high >= -25
    above_30_from_low = pct_from_low >= 30

    if within_25_of_high:
        notes.append(f"Within {abs(pct_from_high):.1f}% of 52-week high")
    if above_30_from_low:
        notes.append(f"{pct_from_low:.0f}% above 52-week low — strong base")

    # ── 2. Contraction Detection ─────────────────────────────────────────

    contractions = find_contractions(df, lookback=lookback)

    # Check tightening — each contraction shallower than previous
    tightening = False
    if len(contractions) >= 2:
        depths = [c.depth_pct for c in contractions]
        # At least the last 2-3 contractions should be tightening
        recent = depths[-min(4, len(depths)):]
        tightening = all(recent[i] <= recent[i - 1] * 1.1 for i in range(1, len(recent)))

        if tightening:
            depth_str = " → ".join([f"{d:.0f}%" for d in recent])
            notes.append(f"Tightening contractions: {depth_str}")

    # Check volume declining across contractions
    volume_declining = False
    if len(contractions) >= 2:
        vols = [c.avg_volume for c in contractions if c.avg_volume > 0]
        if len(vols) >= 2:
            recent_vols = vols[-min(4, len(vols)):]
            volume_declining = all(
                recent_vols[i] <= recent_vols[i - 1] * 1.15
                for i in range(1, len(recent_vols))
            )
            if volume_declining:
                notes.append("Volume declining through contractions — supply drying up")

    vcp_detected = (
        len(contractions) >= 2
        and tightening
        and trend_template_pass
    )

    # ── 3. Pivot & Breakout Level ────────────────────────────────────────

    # Pivot = the high of the last/tightest contraction
    if contractions:
        last_contraction = contractions[-1]
        pivot_price = last_contraction.high_price

        # Tight range = last contraction's range
        pivot_tight_range = last_contraction.depth_pct

        # Distance to pivot
        pivot_distance_pct = ((current_price - pivot_price) / pivot_price) * 100
    else:
        # Fallback — use recent high as pivot
        recent_high = float(df["high"].tail(20).max())
        pivot_price = recent_high
        pivot_tight_range = 0
        pivot_distance_pct = ((current_price - pivot_price) / pivot_price) * 100

    # ── 4. Relative Strength ─────────────────────────────────────────────

    price_change_3m = 0.0
    if len(df) >= 63:
        price_3m_ago = float(close[-63])
        price_change_3m = ((current_price - price_3m_ago) / price_3m_ago) * 100

    rs_vs_index = round(price_change_3m - index_change_pct, 2)

    if rs_vs_index > 10:
        notes.append(f"Strong RS: +{rs_vs_index:.1f}% vs index — outperformer")
    elif rs_vs_index > 0:
        notes.append(f"Positive RS: +{rs_vs_index:.1f}% vs index")

    # ── 5. Scoring (0-100) ───────────────────────────────────────────────

    score = 0

    # Trend template (25 pts)
    if trend_template_pass:
        score += 25
    elif price_above_200:
        score += 10  # Partial credit

    # 52-week position (10 pts)
    if within_25_of_high:
        score += 5
    if above_30_from_low:
        score += 5

    # Contractions (25 pts)
    if len(contractions) >= 3:
        score += 15
    elif len(contractions) >= 2:
        score += 10

    if tightening:
        score += 10

    # Volume dry-up (15 pts)
    if volume_declining:
        score += 15

    # Pivot proximity (15 pts)
    if pivot_distance_pct >= -2 and pivot_distance_pct <= 2:
        score += 15
        notes.append(f"Near pivot at {pivot_price:.2f} — {abs(pivot_distance_pct):.1f}% away")
    elif pivot_distance_pct >= -5:
        score += 8
    elif pivot_distance_pct > 0:
        score += 12
        notes.append(f"Breaking out above pivot {pivot_price:.2f}")

    # Relative strength (10 pts)
    if rs_vs_index > 15:
        score += 10
    elif rs_vs_index > 5:
        score += 6
    elif rs_vs_index > 0:
        score += 3

    # Determine stage
    if pivot_distance_pct > 1:
        stage = "Breakout"
    elif pivot_distance_pct >= -3:
        stage = "Near Pivot"
    elif vcp_detected:
        stage = "Setting Up"
    else:
        stage = "Not Ready"

    return VCPResult(
        symbol=symbol,
        current_price=current_price,
        trend_template_pass=trend_template_pass,
        price_above_150ma=price_above_150,
        price_above_200ma=price_above_200,
        ma150_above_200ma=ma150_above_200,
        ma200_rising=ma200_rising,
        pct_from_52w_high=round(pct_from_high, 2),
        pct_from_52w_low=round(pct_from_low, 2),
        vcp_detected=vcp_detected,
        num_contractions=len(contractions),
        contractions=contractions,
        volume_declining=volume_declining,
        tightening=tightening,
        pivot_price=round(pivot_price, 2),
        pivot_distance_pct=round(pivot_distance_pct, 2),
        pivot_tight_range_pct=round(pivot_tight_range, 2) if contractions else 0,
        rs_vs_index=rs_vs_index,
        vcp_score=min(100, score),
        stage=stage,
        notes=notes,
    )


def _empty_result(symbol: str, price: float, reason: str) -> VCPResult:
    return VCPResult(
        symbol=symbol, current_price=price,
        trend_template_pass=False, price_above_150ma=False,
        price_above_200ma=False, ma150_above_200ma=False,
        ma200_rising=False, pct_from_52w_high=0, pct_from_52w_low=0,
        vcp_detected=False, num_contractions=0, contractions=[],
        volume_declining=False, tightening=False,
        pivot_price=0, pivot_distance_pct=0, pivot_tight_range_pct=0,
        rs_vs_index=0, vcp_score=0, stage="Not Ready",
        notes=[reason],
    )


def vcp_result_to_dict(r: VCPResult) -> dict:
    """Serialize for JSON response."""
    return {
        "symbol": r.symbol,
        "current_price": r.current_price,
        "vcp_score": r.vcp_score,
        "stage": r.stage,
        "vcp_detected": r.vcp_detected,
        "trend_template_pass": r.trend_template_pass,
        "price_above_150ma": r.price_above_150ma,
        "price_above_200ma": r.price_above_200ma,
        "ma150_above_200ma": r.ma150_above_200ma,
        "ma200_rising": r.ma200_rising,
        "pct_from_52w_high": r.pct_from_52w_high,
        "pct_from_52w_low": r.pct_from_52w_low,
        "num_contractions": r.num_contractions,
        "contractions": [
            {
                "number": c.number,
                "depth_pct": c.depth_pct,
                "high": c.high_price,
                "low": c.low_price,
                "duration": c.duration_bars,
            }
            for c in r.contractions
        ],
        "volume_declining": r.volume_declining,
        "tightening": r.tightening,
        "pivot_price": r.pivot_price,
        "pivot_distance_pct": r.pivot_distance_pct,
        "pivot_tight_range_pct": r.pivot_tight_range_pct,
        "rs_vs_index": r.rs_vs_index,
        "notes": r.notes,
    }
