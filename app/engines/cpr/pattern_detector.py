"""
engine/pattern_detector.py
===========================
Detects chart patterns from OHLC DataFrames using swing high/low
detection and trendline fitting.

Patterns detected:
- Ascending Triangle, Descending Triangle, Symmetrical Triangle
- Rectangle (range-bound)
- Double Top, Double Bottom
- Head & Shoulders, Inverse Head & Shoulders
- Bull Flag, Bear Flag
- Rising Wedge, Falling Wedge

Each detector returns PatternResult with confidence (0-100), direction,
entry/SL/target prices.
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from enum import Enum
from typing import Optional
from .config import cfg

# Verify cfg has pattern attributes (added in latest config.py)
if not hasattr(cfg, 'PATTERN_CONFIDENCE_THRESHOLD'):
    cfg.PATTERN_CONFIDENCE_THRESHOLD = 40
    cfg.SWING_WINDOW_DAILY = 5
    cfg.SWING_WINDOW_WEEKLY = 3
    cfg.SWING_WINDOW_MONTHLY = 2


class PatternType(str, Enum):
    ASCENDING_TRIANGLE = "Ascending Triangle"
    DESCENDING_TRIANGLE = "Descending Triangle"
    SYMMETRICAL_TRIANGLE = "Symmetrical Triangle"
    RECTANGLE = "Rectangle"
    DOUBLE_TOP = "Double Top"
    DOUBLE_BOTTOM = "Double Bottom"
    HEAD_AND_SHOULDERS = "Head & Shoulders"
    INVERSE_HEAD_AND_SHOULDERS = "Inverse H&S"
    BULL_FLAG = "Bull Flag"
    BEAR_FLAG = "Bear Flag"
    RISING_WEDGE = "Rising Wedge"
    FALLING_WEDGE = "Falling Wedge"


class BreakoutDirection(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"


@dataclass
class PatternResult:
    pattern: PatternType
    confidence: int  # 0-100
    direction: BreakoutDirection
    entry_price: float
    stop_loss: float
    target: float
    risk_reward: float
    pattern_start_idx: int
    pattern_end_idx: int
    support_level: float
    resistance_level: float
    notes: str


# ── Swing Detection ──────────────────────────────────────────────────────────

def find_swing_highs(df: pd.DataFrame, window: int = 5) -> list[tuple[int, float]]:
    """Find local maxima in 'high' column."""
    highs = df["high"].values
    swings = []
    for i in range(window, len(highs) - window):
        local = highs[i - window: i + window + 1]
        if highs[i] == max(local) and list(local).count(highs[i]) == 1:
            swings.append((i, float(highs[i])))
    return swings


def find_swing_lows(df: pd.DataFrame, window: int = 5) -> list[tuple[int, float]]:
    """Find local minima in 'low' column."""
    lows = df["low"].values
    swings = []
    for i in range(window, len(lows) - window):
        local = lows[i - window: i + window + 1]
        if lows[i] == min(local) and list(local).count(lows[i]) == 1:
            swings.append((i, float(lows[i])))
    return swings


# ── Trendline Fitting ────────────────────────────────────────────────────────

def fit_trendline(points: list[tuple[int, float]]) -> tuple[float, float, float]:
    """
    Fit linear regression to (index, price) points.
    Returns (slope, intercept, r_squared).
    """
    if len(points) < 2:
        return (0.0, 0.0, 0.0)
    x = np.array([p[0] for p in points], dtype=float)
    y = np.array([p[1] for p in points], dtype=float)
    coeffs = np.polyfit(x, y, 1)
    slope, intercept = float(coeffs[0]), float(coeffs[1])
    y_pred = slope * x + intercept
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = float(1 - (ss_res / ss_tot)) if ss_tot > 0 else 0
    return (slope, intercept, max(0, r_squared))


def trendline_value_at(slope: float, intercept: float, idx: int) -> float:
    return slope * idx + intercept


# ── Confidence Scoring ───────────────────────────────────────────────────────

def compute_confidence(
    r_sq_upper: float,
    r_sq_lower: float,
    n_touches_upper: int,
    n_touches_lower: int,
    completeness: float,  # 0-1
    volume_declining: bool = False,
) -> int:
    """
    Compute pattern confidence (0-100).
    - Trendline fit R-squared: 30 pts max
    - Touch count: 25 pts max
    - Completeness: 25 pts max
    - Volume: 20 pts max
    """
    fit_score = int(((r_sq_upper + r_sq_lower) / 2) * 30)
    touch_score = min(25, (n_touches_upper + n_touches_lower - 2) * 6)
    comp_score = int(completeness * 25)
    vol_score = 15 if volume_declining else 5
    return min(100, fit_score + touch_score + comp_score + vol_score)


def is_volume_declining(df: pd.DataFrame, lookback: int = 20) -> bool:
    """Check if volume trend is declining (common in consolidation patterns)."""
    if "volume" not in df.columns or len(df) < lookback:
        return False
    vol = df["volume"].tail(lookback).values
    if len(vol) < 10:
        return False
    first_half = np.mean(vol[:len(vol) // 2])
    second_half = np.mean(vol[len(vol) // 2:])
    return second_half < first_half * 0.85


# ── Pattern Detectors ────────────────────────────────────────────────────────

def detect_triangles(
    df: pd.DataFrame,
    swing_highs: list[tuple[int, float]],
    swing_lows: list[tuple[int, float]],
    avg_price: float,
) -> list[PatternResult]:
    """Detect ascending, descending, and symmetrical triangles."""
    results = []
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return results

    # Use last 4-6 swings for each
    recent_highs = swing_highs[-5:]
    recent_lows = swing_lows[-5:]

    upper_slope, upper_int, upper_r2 = fit_trendline(recent_highs)
    lower_slope, lower_int, lower_r2 = fit_trendline(recent_lows)

    # Normalize slopes by price
    norm_upper = upper_slope / avg_price if avg_price > 0 else 0
    norm_lower = lower_slope / avg_price if avg_price > 0 else 0

    flat_threshold = 0.0008  # 0.08% per bar

    last_idx = len(df) - 1
    current_price = float(df["close"].iloc[-1])
    upper_at_end = trendline_value_at(upper_slope, upper_int, last_idx)
    lower_at_end = trendline_value_at(lower_slope, lower_int, last_idx)
    vol_declining = is_volume_declining(df)

    # Check convergence
    first_idx = min(recent_highs[0][0], recent_lows[0][0])
    upper_at_start = trendline_value_at(upper_slope, upper_int, first_idx)
    lower_at_start = trendline_value_at(lower_slope, lower_int, first_idx)
    width_start = upper_at_start - lower_at_start
    width_end = upper_at_end - lower_at_end

    if width_start <= 0 or width_end <= 0:
        return results

    completeness = max(0, min(1, 1 - (width_end / width_start)))

    # Ascending Triangle: flat top, rising bottom
    if abs(norm_upper) < flat_threshold and norm_lower > flat_threshold * 0.5:
        conf = compute_confidence(upper_r2, lower_r2, len(recent_highs), len(recent_lows), completeness, vol_declining)
        if conf >= cfg.PATTERN_CONFIDENCE_THRESHOLD:
            breakout_level = upper_at_end
            target = breakout_level + (width_start * 0.7)
            sl = lower_at_end
            rr = (target - breakout_level) / (breakout_level - sl) if breakout_level > sl else 0
            results.append(PatternResult(
                pattern=PatternType.ASCENDING_TRIANGLE,
                confidence=conf,
                direction=BreakoutDirection.BULLISH,
                entry_price=round(breakout_level, 2),
                stop_loss=round(sl, 2),
                target=round(target, 2),
                risk_reward=round(rr, 2),
                pattern_start_idx=first_idx,
                pattern_end_idx=last_idx,
                support_level=round(lower_at_end, 2),
                resistance_level=round(upper_at_end, 2),
                notes=f"Flat resistance at {upper_at_end:.0f}, rising support. Breakout target {target:.0f}",
            ))

    # Descending Triangle: falling top, flat bottom
    if norm_upper < -flat_threshold * 0.5 and abs(norm_lower) < flat_threshold:
        conf = compute_confidence(upper_r2, lower_r2, len(recent_highs), len(recent_lows), completeness, vol_declining)
        if conf >= cfg.PATTERN_CONFIDENCE_THRESHOLD:
            breakout_level = lower_at_end
            target = breakout_level - (width_start * 0.7)
            sl = upper_at_end
            rr = (breakout_level - target) / (sl - breakout_level) if sl > breakout_level else 0
            results.append(PatternResult(
                pattern=PatternType.DESCENDING_TRIANGLE,
                confidence=conf,
                direction=BreakoutDirection.BEARISH,
                entry_price=round(breakout_level, 2),
                stop_loss=round(sl, 2),
                target=round(target, 2),
                risk_reward=round(rr, 2),
                pattern_start_idx=first_idx,
                pattern_end_idx=last_idx,
                support_level=round(lower_at_end, 2),
                resistance_level=round(upper_at_end, 2),
                notes=f"Falling resistance, flat support at {lower_at_end:.0f}. Breakdown target {target:.0f}",
            ))

    # Symmetrical Triangle: upper falling, lower rising
    if norm_upper < -flat_threshold * 0.3 and norm_lower > flat_threshold * 0.3:
        conf = compute_confidence(upper_r2, lower_r2, len(recent_highs), len(recent_lows), completeness, vol_declining)
        if conf >= cfg.PATTERN_CONFIDENCE_THRESHOLD:
            # Direction determined by trend before triangle
            pre_trend = float(df["close"].iloc[first_idx]) - float(df["close"].iloc[max(0, first_idx - 20)])
            direction = BreakoutDirection.BULLISH if pre_trend > 0 else BreakoutDirection.BEARISH

            if direction == BreakoutDirection.BULLISH:
                entry = round(upper_at_end, 2)
                sl = round(lower_at_end, 2)
                target = round(entry + width_start * 0.6, 2)
            else:
                entry = round(lower_at_end, 2)
                sl = round(upper_at_end, 2)
                target = round(entry - width_start * 0.6, 2)

            rr = abs(target - entry) / abs(sl - entry) if abs(sl - entry) > 0 else 0
            results.append(PatternResult(
                pattern=PatternType.SYMMETRICAL_TRIANGLE,
                confidence=conf,
                direction=direction,
                entry_price=entry,
                stop_loss=sl,
                target=target,
                risk_reward=round(rr, 2),
                pattern_start_idx=first_idx,
                pattern_end_idx=last_idx,
                support_level=round(lower_at_end, 2),
                resistance_level=round(upper_at_end, 2),
                notes=f"Converging trendlines. Bias: {direction.value} based on prior trend",
            ))

    return results


def detect_rectangle(
    df: pd.DataFrame,
    swing_highs: list[tuple[int, float]],
    swing_lows: list[tuple[int, float]],
    avg_price: float,
) -> Optional[PatternResult]:
    """Detect range-bound rectangle pattern."""
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return None

    recent_highs = swing_highs[-4:]
    recent_lows = swing_lows[-4:]

    upper_slope, upper_int, upper_r2 = fit_trendline(recent_highs)
    lower_slope, lower_int, lower_r2 = fit_trendline(recent_lows)

    norm_upper = abs(upper_slope / avg_price) if avg_price > 0 else 1
    norm_lower = abs(lower_slope / avg_price) if avg_price > 0 else 1

    flat_threshold = 0.0006

    if norm_upper > flat_threshold or norm_lower > flat_threshold:
        return None

    # Both lines are approximately flat
    resistance = np.mean([h[1] for h in recent_highs])
    support = np.mean([l[1] for l in recent_lows])
    range_width = resistance - support

    if range_width <= 0 or (range_width / avg_price) < 0.01:
        return None

    current_price = float(df["close"].iloc[-1])
    vol_declining = is_volume_declining(df)
    completeness = min(1.0, len(recent_highs) / 4)

    conf = compute_confidence(upper_r2, lower_r2, len(recent_highs), len(recent_lows), completeness, vol_declining)
    if conf < cfg.PATTERN_CONFIDENCE_THRESHOLD:
        return None

    # Bias based on position within range
    mid = (resistance + support) / 2
    if current_price > mid:
        direction = BreakoutDirection.BULLISH
        entry = round(resistance, 2)
        sl = round(mid, 2)
        target = round(resistance + range_width, 2)
    else:
        direction = BreakoutDirection.BEARISH
        entry = round(support, 2)
        sl = round(mid, 2)
        target = round(support - range_width, 2)

    rr = abs(target - entry) / abs(sl - entry) if abs(sl - entry) > 0 else 0

    return PatternResult(
        pattern=PatternType.RECTANGLE,
        confidence=conf,
        direction=direction,
        entry_price=entry,
        stop_loss=sl,
        target=target,
        risk_reward=round(rr, 2),
        pattern_start_idx=recent_lows[0][0],
        pattern_end_idx=len(df) - 1,
        support_level=round(support, 2),
        resistance_level=round(resistance, 2),
        notes=f"Range {support:.0f}-{resistance:.0f} ({(range_width/avg_price*100):.1f}% width)",
    )


def detect_double_top(
    df: pd.DataFrame,
    swing_highs: list[tuple[int, float]],
    swing_lows: list[tuple[int, float]],
) -> Optional[PatternResult]:
    """Detect double top reversal pattern."""
    if len(swing_highs) < 2 or len(swing_lows) < 1:
        return None

    # Check last 2 swing highs
    peak1_idx, peak1 = swing_highs[-2]
    peak2_idx, peak2 = swing_highs[-1]

    # Peaks must be within 2% of each other
    if abs(peak1 - peak2) / max(peak1, peak2) > 0.02:
        return None

    # Must be separated by at least 8 bars
    if peak2_idx - peak1_idx < 8:
        return None

    # Find the trough between peaks (neckline)
    troughs_between = [s for s in swing_lows if peak1_idx < s[0] < peak2_idx]
    if not troughs_between:
        return None

    neckline = min(troughs_between, key=lambda s: s[1])[1]
    peak_avg = (peak1 + peak2) / 2
    pattern_height = peak_avg - neckline

    if pattern_height <= 0 or (pattern_height / peak_avg) < 0.01:
        return None

    current_price = float(df["close"].iloc[-1])
    # Pattern is more valid if price is near or below neckline
    completeness = min(1.0, max(0.3, (peak_avg - current_price) / pattern_height))

    r2_approx = 1 - abs(peak1 - peak2) / max(peak1, peak2)
    conf = compute_confidence(r2_approx, 0.8, 2, len(troughs_between), completeness)
    if conf < cfg.PATTERN_CONFIDENCE_THRESHOLD:
        return None

    entry = round(neckline, 2)
    sl = round(peak_avg * 1.005, 2)
    target = round(neckline - pattern_height, 2)
    rr = abs(entry - target) / abs(sl - entry) if abs(sl - entry) > 0 else 0

    return PatternResult(
        pattern=PatternType.DOUBLE_TOP,
        confidence=conf,
        direction=BreakoutDirection.BEARISH,
        entry_price=entry,
        stop_loss=sl,
        target=target,
        risk_reward=round(rr, 2),
        pattern_start_idx=peak1_idx,
        pattern_end_idx=peak2_idx,
        support_level=round(neckline, 2),
        resistance_level=round(peak_avg, 2),
        notes=f"Two peaks at ~{peak_avg:.0f}, neckline at {neckline:.0f}",
    )


def detect_double_bottom(
    df: pd.DataFrame,
    swing_highs: list[tuple[int, float]],
    swing_lows: list[tuple[int, float]],
) -> Optional[PatternResult]:
    """Detect double bottom reversal pattern."""
    if len(swing_lows) < 2 or len(swing_highs) < 1:
        return None

    trough1_idx, trough1 = swing_lows[-2]
    trough2_idx, trough2 = swing_lows[-1]

    if abs(trough1 - trough2) / min(trough1, trough2) > 0.02:
        return None
    if trough2_idx - trough1_idx < 8:
        return None

    peaks_between = [s for s in swing_highs if trough1_idx < s[0] < trough2_idx]
    if not peaks_between:
        return None

    neckline = max(peaks_between, key=lambda s: s[1])[1]
    trough_avg = (trough1 + trough2) / 2
    pattern_height = neckline - trough_avg

    if pattern_height <= 0 or (pattern_height / trough_avg) < 0.01:
        return None

    current_price = float(df["close"].iloc[-1])
    completeness = min(1.0, max(0.3, (current_price - trough_avg) / pattern_height))

    r2_approx = 1 - abs(trough1 - trough2) / max(trough1, trough2)
    conf = compute_confidence(0.8, r2_approx, len(peaks_between), 2, completeness)
    if conf < cfg.PATTERN_CONFIDENCE_THRESHOLD:
        return None

    entry = round(neckline, 2)
    sl = round(trough_avg * 0.995, 2)
    target = round(neckline + pattern_height, 2)
    rr = abs(target - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0

    return PatternResult(
        pattern=PatternType.DOUBLE_BOTTOM,
        confidence=conf,
        direction=BreakoutDirection.BULLISH,
        entry_price=entry,
        stop_loss=sl,
        target=target,
        risk_reward=round(rr, 2),
        pattern_start_idx=trough1_idx,
        pattern_end_idx=trough2_idx,
        support_level=round(trough_avg, 2),
        resistance_level=round(neckline, 2),
        notes=f"Two troughs at ~{trough_avg:.0f}, neckline at {neckline:.0f}",
    )


def detect_head_and_shoulders(
    df: pd.DataFrame,
    swing_highs: list[tuple[int, float]],
    swing_lows: list[tuple[int, float]],
) -> list[PatternResult]:
    """Detect head & shoulders and inverse H&S patterns."""
    results = []

    # Regular H&S (bearish reversal)
    if len(swing_highs) >= 3 and len(swing_lows) >= 2:
        for i in range(len(swing_highs) - 2):
            ls_idx, ls = swing_highs[i]
            h_idx, head = swing_highs[i + 1]
            rs_idx, rs = swing_highs[i + 2]

            # Head must be highest
            if head <= ls or head <= rs:
                continue

            # Shoulders within 5% of each other
            if abs(ls - rs) / max(ls, rs) > 0.05:
                continue

            # Find troughs between peaks for neckline
            t1 = [s for s in swing_lows if ls_idx < s[0] < h_idx]
            t2 = [s for s in swing_lows if h_idx < s[0] < rs_idx]
            if not t1 or not t2:
                continue

            neckline = (min(t1, key=lambda s: s[1])[1] + min(t2, key=lambda s: s[1])[1]) / 2
            pattern_height = head - neckline

            if pattern_height <= 0 or (pattern_height / head) < 0.02:
                continue

            current_price = float(df["close"].iloc[-1])
            completeness = min(1.0, max(0.3, (head - current_price) / pattern_height))

            conf = min(85, 50 + int(completeness * 20) + (5 if abs(ls - rs) / max(ls, rs) < 0.02 else 0))
            if conf < cfg.PATTERN_CONFIDENCE_THRESHOLD:
                continue

            entry = round(neckline, 2)
            sl = round(rs * 1.005, 2)
            target = round(neckline - pattern_height, 2)
            rr = abs(entry - target) / abs(sl - entry) if abs(sl - entry) > 0 else 0

            results.append(PatternResult(
                pattern=PatternType.HEAD_AND_SHOULDERS,
                confidence=conf,
                direction=BreakoutDirection.BEARISH,
                entry_price=entry,
                stop_loss=sl,
                target=target,
                risk_reward=round(rr, 2),
                pattern_start_idx=ls_idx,
                pattern_end_idx=rs_idx,
                support_level=round(neckline, 2),
                resistance_level=round(head, 2),
                notes=f"Head at {head:.0f}, shoulders at ~{(ls+rs)/2:.0f}, neckline {neckline:.0f}",
            ))
            break  # Only report the most recent one

    # Inverse H&S (bullish reversal)
    if len(swing_lows) >= 3 and len(swing_highs) >= 2:
        for i in range(len(swing_lows) - 2):
            ls_idx, ls = swing_lows[i]
            h_idx, head = swing_lows[i + 1]
            rs_idx, rs = swing_lows[i + 2]

            if head >= ls or head >= rs:
                continue
            if abs(ls - rs) / min(ls, rs) > 0.05:
                continue

            t1 = [s for s in swing_highs if ls_idx < s[0] < h_idx]
            t2 = [s for s in swing_highs if h_idx < s[0] < rs_idx]
            if not t1 or not t2:
                continue

            neckline = (max(t1, key=lambda s: s[1])[1] + max(t2, key=lambda s: s[1])[1]) / 2
            pattern_height = neckline - head

            if pattern_height <= 0 or (pattern_height / head) < 0.02:
                continue

            current_price = float(df["close"].iloc[-1])
            completeness = min(1.0, max(0.3, (current_price - head) / pattern_height))

            conf = min(85, 50 + int(completeness * 20) + (5 if abs(ls - rs) / min(ls, rs) < 0.02 else 0))
            if conf < cfg.PATTERN_CONFIDENCE_THRESHOLD:
                continue

            entry = round(neckline, 2)
            sl = round(rs * 0.995, 2)
            target = round(neckline + pattern_height, 2)
            rr = abs(target - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0

            results.append(PatternResult(
                pattern=PatternType.INVERSE_HEAD_AND_SHOULDERS,
                confidence=conf,
                direction=BreakoutDirection.BULLISH,
                entry_price=entry,
                stop_loss=sl,
                target=target,
                risk_reward=round(rr, 2),
                pattern_start_idx=ls_idx,
                pattern_end_idx=rs_idx,
                support_level=round(head, 2),
                resistance_level=round(neckline, 2),
                notes=f"Inv H&S: head at {head:.0f}, neckline {neckline:.0f}",
            ))
            break

    return results


def detect_flags(
    df: pd.DataFrame,
    swing_highs: list[tuple[int, float]],
    swing_lows: list[tuple[int, float]],
    avg_price: float,
) -> list[PatternResult]:
    """Detect bull and bear flag patterns."""
    results = []
    if len(df) < 30:
        return results

    # Look for a sharp pole followed by a consolidation channel
    close = df["close"].values
    n = len(close)

    # Check the last 40 bars for a pole + flag structure
    for pole_len in range(5, 15):
        flag_start = n - 25  # Approximate
        pole_end = flag_start
        pole_start = pole_end - pole_len

        if pole_start < 0 or flag_start >= n:
            continue

        pole_move = close[pole_end] - close[pole_start]
        pole_pct = abs(pole_move) / close[pole_start] * 100

        if pole_pct < 4:  # Need at least 4% move for the pole
            continue

        # Flag is the consolidation after the pole (last 10-20 bars)
        flag_data = df.iloc[pole_end:].copy()
        if len(flag_data) < 8:
            continue

        flag_highs = find_swing_highs(flag_data, window=2)
        flag_lows = find_swing_lows(flag_data, window=2)

        if len(flag_highs) < 2 or len(flag_lows) < 2:
            continue

        flag_upper_slope, _, flag_upper_r2 = fit_trendline(flag_highs)
        flag_lower_slope, _, flag_lower_r2 = fit_trendline(flag_lows)

        # Flag retracement should be small (< 38.2% of pole)
        flag_range = max(flag_data["high"]) - min(flag_data["low"])
        retracement = flag_range / abs(pole_move) if abs(pole_move) > 0 else 1

        if retracement > 0.5:
            continue

        # Bull flag: pole up, flag drifting down (both slopes negative)
        if pole_move > 0 and flag_upper_slope < 0 and flag_lower_slope < 0:
            conf = min(80, 45 + int((1 - retracement) * 20) + int(flag_upper_r2 * 10))
            if conf < cfg.PATTERN_CONFIDENCE_THRESHOLD:
                continue

            entry = round(float(max(flag_data["high"])), 2)
            sl = round(float(min(flag_data["low"])), 2)
            target = round(entry + abs(pole_move) * 0.7, 2)
            rr = (target - entry) / (entry - sl) if entry > sl else 0

            results.append(PatternResult(
                pattern=PatternType.BULL_FLAG,
                confidence=conf,
                direction=BreakoutDirection.BULLISH,
                entry_price=entry,
                stop_loss=sl,
                target=target,
                risk_reward=round(rr, 2),
                pattern_start_idx=pole_start,
                pattern_end_idx=n - 1,
                support_level=sl,
                resistance_level=entry,
                notes=f"Pole +{pole_pct:.1f}%, flag retrace {retracement*100:.0f}%",
            ))
            break

        # Bear flag: pole down, flag drifting up
        if pole_move < 0 and flag_upper_slope > 0 and flag_lower_slope > 0:
            conf = min(80, 45 + int((1 - retracement) * 20) + int(flag_lower_r2 * 10))
            if conf < cfg.PATTERN_CONFIDENCE_THRESHOLD:
                continue

            entry = round(float(min(flag_data["low"])), 2)
            sl = round(float(max(flag_data["high"])), 2)
            target = round(entry - abs(pole_move) * 0.7, 2)
            rr = (entry - target) / (sl - entry) if sl > entry else 0

            results.append(PatternResult(
                pattern=PatternType.BEAR_FLAG,
                confidence=conf,
                direction=BreakoutDirection.BEARISH,
                entry_price=entry,
                stop_loss=sl,
                target=target,
                risk_reward=round(rr, 2),
                pattern_start_idx=pole_start,
                pattern_end_idx=n - 1,
                support_level=entry,
                resistance_level=sl,
                notes=f"Pole {pole_pct:.1f}%, flag retrace {retracement*100:.0f}%",
            ))
            break

    return results


def detect_wedges(
    df: pd.DataFrame,
    swing_highs: list[tuple[int, float]],
    swing_lows: list[tuple[int, float]],
    avg_price: float,
) -> list[PatternResult]:
    """Detect rising and falling wedge patterns."""
    results = []
    if len(swing_highs) < 3 or len(swing_lows) < 3:
        return results

    recent_highs = swing_highs[-4:]
    recent_lows = swing_lows[-4:]

    upper_slope, upper_int, upper_r2 = fit_trendline(recent_highs)
    lower_slope, lower_int, lower_r2 = fit_trendline(recent_lows)

    norm_upper = upper_slope / avg_price if avg_price > 0 else 0
    norm_lower = lower_slope / avg_price if avg_price > 0 else 0

    # Both must move in same direction and converge
    last_idx = len(df) - 1
    first_idx = min(recent_highs[0][0], recent_lows[0][0])

    width_start = trendline_value_at(upper_slope, upper_int, first_idx) - trendline_value_at(lower_slope, lower_int, first_idx)
    width_end = trendline_value_at(upper_slope, upper_int, last_idx) - trendline_value_at(lower_slope, lower_int, last_idx)

    if width_start <= 0 or width_end <= 0 or width_end >= width_start:
        return results

    convergence = 1 - (width_end / width_start)
    vol_declining = is_volume_declining(df)

    # Rising wedge: both slopes positive, converging (bearish)
    if norm_upper > 0.0003 and norm_lower > 0.0003 and norm_upper < norm_lower * 2:
        conf = compute_confidence(upper_r2, lower_r2, len(recent_highs), len(recent_lows), convergence, vol_declining)
        if conf >= cfg.PATTERN_CONFIDENCE_THRESHOLD:
            lower_at_end = trendline_value_at(lower_slope, lower_int, last_idx)
            upper_at_end = trendline_value_at(upper_slope, upper_int, last_idx)
            entry = round(lower_at_end, 2)
            sl = round(upper_at_end, 2)
            target = round(entry - width_start * 0.6, 2)
            rr = abs(entry - target) / abs(sl - entry) if abs(sl - entry) > 0 else 0

            results.append(PatternResult(
                pattern=PatternType.RISING_WEDGE,
                confidence=conf,
                direction=BreakoutDirection.BEARISH,
                entry_price=entry,
                stop_loss=sl,
                target=target,
                risk_reward=round(rr, 2),
                pattern_start_idx=first_idx,
                pattern_end_idx=last_idx,
                support_level=entry,
                resistance_level=sl,
                notes=f"Rising wedge converging — bearish breakdown expected",
            ))

    # Falling wedge: both slopes negative, converging (bullish)
    if norm_upper < -0.0003 and norm_lower < -0.0003 and abs(norm_upper) < abs(norm_lower) * 2:
        conf = compute_confidence(upper_r2, lower_r2, len(recent_highs), len(recent_lows), convergence, vol_declining)
        if conf >= cfg.PATTERN_CONFIDENCE_THRESHOLD:
            upper_at_end = trendline_value_at(upper_slope, upper_int, last_idx)
            lower_at_end = trendline_value_at(lower_slope, lower_int, last_idx)
            entry = round(upper_at_end, 2)
            sl = round(lower_at_end, 2)
            target = round(entry + width_start * 0.6, 2)
            rr = abs(target - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0

            results.append(PatternResult(
                pattern=PatternType.FALLING_WEDGE,
                confidence=conf,
                direction=BreakoutDirection.BULLISH,
                entry_price=entry,
                stop_loss=sl,
                target=target,
                risk_reward=round(rr, 2),
                pattern_start_idx=first_idx,
                pattern_end_idx=last_idx,
                support_level=round(lower_at_end, 2),
                resistance_level=round(upper_at_end, 2),
                notes=f"Falling wedge converging — bullish breakout expected",
            ))

    return results


# ── Main Scanner ─────────────────────────────────────────────────────────────

def scan_patterns(
    df: pd.DataFrame,
    lookback: int = 60,
    swing_window: int = 5,
    min_confidence: int = 40,
) -> list[PatternResult]:
    """
    Run all pattern detectors on a single OHLC DataFrame.
    Returns list of detected patterns sorted by confidence descending.
    """
    if len(df) < 20:
        return []

    df_slice = df.tail(lookback).copy().reset_index(drop=True)
    avg_price = float(df_slice["close"].mean())

    swing_highs = find_swing_highs(df_slice, window=swing_window)
    swing_lows = find_swing_lows(df_slice, window=swing_window)

    patterns: list[PatternResult] = []

    # Run all detectors
    try:
        patterns.extend(detect_triangles(df_slice, swing_highs, swing_lows, avg_price))
    except Exception:
        pass

    try:
        rect = detect_rectangle(df_slice, swing_highs, swing_lows, avg_price)
        if rect:
            patterns.append(rect)
    except Exception:
        pass

    try:
        dt = detect_double_top(df_slice, swing_highs, swing_lows)
        if dt:
            patterns.append(dt)
    except Exception:
        pass

    try:
        db = detect_double_bottom(df_slice, swing_highs, swing_lows)
        if db:
            patterns.append(db)
    except Exception:
        pass

    try:
        patterns.extend(detect_head_and_shoulders(df_slice, swing_highs, swing_lows))
    except Exception:
        pass

    try:
        patterns.extend(detect_flags(df_slice, swing_highs, swing_lows, avg_price))
    except Exception:
        pass

    try:
        patterns.extend(detect_wedges(df_slice, swing_highs, swing_lows, avg_price))
    except Exception:
        pass

    # Filter and sort
    patterns = [p for p in patterns if p.confidence >= min_confidence]
    patterns.sort(key=lambda p: p.confidence, reverse=True)

    return patterns


def pattern_to_dict(p: PatternResult) -> dict:
    """Serialize PatternResult for JSON response."""
    return {
        "pattern": p.pattern.value,
        "confidence": p.confidence,
        "direction": p.direction.value,
        "entry_price": p.entry_price,
        "stop_loss": p.stop_loss,
        "target": p.target,
        "risk_reward": p.risk_reward,
        "support_level": p.support_level,
        "resistance_level": p.resistance_level,
        "notes": p.notes,
    }
