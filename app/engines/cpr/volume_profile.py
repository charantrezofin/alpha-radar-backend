"""
engine/volume_profile.py
=========================
Volume Profile analysis — computes Point of Control (POC), Value Area High (VAH),
and Value Area Low (VAL) from OHLC+Volume data.

POC  = Price level with highest traded volume (institutional interest)
VAH  = Upper boundary of 70% volume area (resistance)
VAL  = Lower boundary of 70% volume area (support)

When combined with CPR:
- POC near CPR pivot = strong confluence (high probability reversal/breakout)
- Price above VAH = bullish (broken above value)
- Price below VAL = bearish (broken below value)
- Price between VAL-VAH = range-bound (fair value)

Supports daily, weekly, monthly profiles and developing (intraday) profiles.
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional
from .config import cfg


@dataclass
class VolumeProfileResult:
    """Volume Profile analysis for a single symbol."""
    symbol: str
    current_price: float
    timeframe: str  # "daily_composite", "weekly", "monthly"

    # Core levels
    poc: float              # Point of Control — highest volume price
    vah: float              # Value Area High (70% boundary)
    val: float              # Value Area Low (70% boundary)
    poc_volume: float       # Volume at POC level

    # Price position
    price_vs_profile: str   # "above_value", "in_value", "below_value", "at_poc"
    distance_to_poc_pct: float  # How far price is from POC

    # Volume distribution
    total_volume: float
    value_area_pct: float   # Actual % captured (should be ~70%)
    profile_high: float     # Highest price in profile
    profile_low: float      # Lowest price in profile

    # CPR confluence (if CPR data provided)
    poc_near_cpr_pivot: bool    # POC within 0.5% of CPR pivot
    vah_near_cpr_tc: bool       # VAH near CPR top
    val_near_cpr_bc: bool       # VAL near CPR bottom

    # Signal
    signal: str             # "bullish", "bearish", "neutral", "confluence"
    signal_strength: int    # 0-100
    notes: list[str]


def compute_volume_profile(
    df: pd.DataFrame,
    num_bins: int = 50,
    value_area_pct: float = 0.70,
) -> tuple[float, float, float, dict]:
    """
    Compute volume profile from OHLC+Volume data.

    Distributes each bar's volume across its price range (high-low) equally,
    then aggregates into price bins.

    Returns: (poc, vah, val, bin_volumes_dict)
    """
    if df.empty or len(df) < 5:
        return (0, 0, 0, {})

    price_high = float(df["high"].max())
    price_low = float(df["low"].min())
    total_range = price_high - price_low

    if total_range <= 0:
        mid = float(df["close"].mean())
        return (mid, mid, mid, {})

    bin_size = total_range / num_bins
    bins = np.zeros(num_bins)

    # Distribute each bar's volume across its range
    for _, row in df.iterrows():
        bar_high = row["high"]
        bar_low = row["low"]
        bar_vol = row.get("volume", 0)

        if bar_vol <= 0 or bar_high <= bar_low:
            continue

        low_bin = max(0, int((bar_low - price_low) / bin_size))
        high_bin = min(num_bins - 1, int((bar_high - price_low) / bin_size))

        if high_bin <= low_bin:
            bins[low_bin] += bar_vol
        else:
            vol_per_bin = bar_vol / (high_bin - low_bin + 1)
            for b in range(low_bin, high_bin + 1):
                bins[b] += vol_per_bin

    # POC = bin with highest volume
    poc_bin = int(np.argmax(bins))
    poc = price_low + (poc_bin + 0.5) * bin_size
    poc_volume = float(bins[poc_bin])

    # Value Area — expand from POC until 70% of total volume captured
    total_vol = float(np.sum(bins))
    if total_vol <= 0:
        return (poc, poc, poc, {})

    target_vol = total_vol * value_area_pct
    captured_vol = float(bins[poc_bin])
    lower_idx = poc_bin
    upper_idx = poc_bin

    while captured_vol < target_vol:
        # Look one bin up and one bin down, add the larger one
        can_go_up = upper_idx < num_bins - 1
        can_go_down = lower_idx > 0

        if not can_go_up and not can_go_down:
            break

        up_vol = float(bins[upper_idx + 1]) if can_go_up else -1
        down_vol = float(bins[lower_idx - 1]) if can_go_down else -1

        if up_vol >= down_vol:
            upper_idx += 1
            captured_vol += up_vol
        else:
            lower_idx -= 1
            captured_vol += down_vol

    vah = price_low + (upper_idx + 1) * bin_size
    val = price_low + lower_idx * bin_size

    # Build bin volumes dict for reference
    bin_volumes = {}
    for i in range(num_bins):
        if bins[i] > 0:
            price = price_low + (i + 0.5) * bin_size
            bin_volumes[round(price, 2)] = round(float(bins[i]), 0)

    return (round(poc, 2), round(vah, 2), round(val, 2), bin_volumes)


def analyze_volume_profile(
    df: pd.DataFrame,
    symbol: str,
    lookback: int = 30,
    cpr_pivot: float = 0,
    cpr_tc: float = 0,
    cpr_bc: float = 0,
    timeframe: str = "daily_composite",
) -> VolumeProfileResult:
    """
    Full volume profile analysis for a single symbol.

    Args:
        df: OHLC+Volume DataFrame
        symbol: Stock symbol
        lookback: Number of bars to build profile from
        cpr_pivot/tc/bc: CPR levels for confluence check
        timeframe: Label for the profile period
    """
    data = df.tail(lookback).copy()
    if data.empty or len(data) < 5:
        return _empty_vp_result(symbol, 0, timeframe)

    current_price = float(data["close"].iloc[-1])
    poc, vah, val, _ = compute_volume_profile(data)

    if poc == 0:
        return _empty_vp_result(symbol, current_price, timeframe)

    total_volume = float(data["volume"].sum())
    profile_high = float(data["high"].max())
    profile_low = float(data["low"].min())

    # Price position
    distance_to_poc = ((current_price - poc) / poc) * 100 if poc > 0 else 0

    if current_price > vah * 1.002:
        price_vs = "above_value"
    elif current_price < val * 0.998:
        price_vs = "below_value"
    elif abs(current_price - poc) / poc < 0.003:
        price_vs = "at_poc"
    else:
        price_vs = "in_value"

    # CPR confluence
    poc_near_pivot = abs(poc - cpr_pivot) / poc < 0.005 if cpr_pivot > 0 and poc > 0 else False
    vah_near_tc = abs(vah - cpr_tc) / vah < 0.005 if cpr_tc > 0 and vah > 0 else False
    val_near_bc = abs(val - cpr_bc) / val < 0.005 if cpr_bc > 0 and val > 0 else False

    # Signal
    notes = []
    strength = 0

    if price_vs == "above_value":
        signal = "bullish"
        strength += 25
        notes.append(f"Price above Value Area High {vah:.2f} — bullish breakout")
    elif price_vs == "below_value":
        signal = "bearish"
        strength += 25
        notes.append(f"Price below Value Area Low {val:.2f} — bearish breakdown")
    elif price_vs == "at_poc":
        signal = "neutral"
        strength += 15
        notes.append(f"Price at POC {poc:.2f} — high volume node, expect reaction")
    else:
        signal = "neutral"
        strength += 10
        notes.append(f"Price in value area ({val:.2f} - {vah:.2f}) — range-bound")

    # POC as support/resistance
    if current_price > poc and distance_to_poc < 2:
        strength += 15
        notes.append(f"POC {poc:.2f} acting as support below")
    elif current_price < poc and abs(distance_to_poc) < 2:
        strength += 15
        notes.append(f"POC {poc:.2f} acting as resistance above")

    # CPR confluence bonus
    if poc_near_pivot:
        strength += 20
        signal = "confluence"
        notes.append(f"POC near CPR Pivot — strong confluence level")
    if vah_near_tc:
        strength += 10
        notes.append(f"VAH aligns with CPR Top — double resistance")
    if val_near_bc:
        strength += 10
        notes.append(f"VAL aligns with CPR Bottom — double support")

    # Value area width
    va_width_pct = ((vah - val) / poc) * 100 if poc > 0 else 0
    if va_width_pct < 2:
        strength += 10
        notes.append(f"Tight value area ({va_width_pct:.1f}%) — compression, expect expansion")
    elif va_width_pct > 5:
        notes.append(f"Wide value area ({va_width_pct:.1f}%) — high volatility range")

    actual_va_pct = 70.0  # Approximate

    return VolumeProfileResult(
        symbol=symbol,
        current_price=current_price,
        timeframe=timeframe,
        poc=poc,
        vah=vah,
        val=val,
        poc_volume=0,
        price_vs_profile=price_vs,
        distance_to_poc_pct=round(distance_to_poc, 2),
        total_volume=total_volume,
        value_area_pct=actual_va_pct,
        profile_high=round(profile_high, 2),
        profile_low=round(profile_low, 2),
        poc_near_cpr_pivot=poc_near_pivot,
        vah_near_cpr_tc=vah_near_tc,
        val_near_cpr_bc=val_near_bc,
        signal=signal,
        signal_strength=min(100, strength),
        notes=notes,
    )


def _empty_vp_result(symbol: str, price: float, tf: str) -> VolumeProfileResult:
    return VolumeProfileResult(
        symbol=symbol, current_price=price, timeframe=tf,
        poc=0, vah=0, val=0, poc_volume=0,
        price_vs_profile="unknown", distance_to_poc_pct=0,
        total_volume=0, value_area_pct=0,
        profile_high=0, profile_low=0,
        poc_near_cpr_pivot=False, vah_near_cpr_tc=False, val_near_cpr_bc=False,
        signal="neutral", signal_strength=0, notes=["Insufficient data"],
    )


def vp_result_to_dict(r: VolumeProfileResult) -> dict:
    return {
        "symbol": r.symbol,
        "current_price": r.current_price,
        "timeframe": r.timeframe,
        "poc": r.poc,
        "vah": r.vah,
        "val": r.val,
        "price_vs_profile": r.price_vs_profile,
        "distance_to_poc_pct": r.distance_to_poc_pct,
        "total_volume": r.total_volume,
        "profile_high": r.profile_high,
        "profile_low": r.profile_low,
        "poc_near_cpr_pivot": bool(r.poc_near_cpr_pivot),
        "vah_near_cpr_tc": bool(r.vah_near_cpr_tc),
        "val_near_cpr_bc": bool(r.val_near_cpr_bc),
        "signal": r.signal,
        "signal_strength": r.signal_strength,
        "notes": r.notes,
    }
