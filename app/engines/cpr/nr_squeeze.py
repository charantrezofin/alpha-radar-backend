"""
engine/nr_squeeze.py
=====================
NR4/NR7 Squeeze detector — identifies stocks with the narrowest
range in 4 or 7 days, signaling a volatility contraction about to expand.

NR4 = Today's range (high - low) is the narrowest in the last 4 bars
NR7 = Today's range is the narrowest in the last 7 bars

When NR4 or NR7 occurs:
- Volatility is compressing → big move incoming
- Direction determined by: trend, volume, and where price closes within the range
- Popular with Mumbai swing traders for next-day breakout entries

Entry: Buy above NR bar high (bullish) or sell below NR bar low (bearish)
SL: Opposite end of the NR bar
Target: 1:2 or 1:3 RR from entry

Additional filters:
- Inside Bar (IB): Today's high < yesterday's high AND today's low > yesterday's low
- NR + IB = strongest squeeze signal
- Bollinger Band squeeze: BB width at multi-day low confirms compression
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional
from .config import cfg


@dataclass
class NRSqueezeResult:
    """NR4/NR7 squeeze analysis for a single symbol."""
    symbol: str
    current_price: float

    # NR detection
    is_nr4: bool            # Narrowest range in 4 bars
    is_nr7: bool            # Narrowest range in 7 bars
    is_inside_bar: bool     # Today's range inside yesterday's range
    nr_bar_high: float      # High of the NR bar
    nr_bar_low: float       # Low of the NR bar
    nr_bar_range: float     # Range of NR bar (high - low)
    nr_bar_range_pct: float # Range as % of price
    avg_range_7d: float     # Average 7-day range for comparison

    # Bollinger squeeze
    bb_squeeze: bool        # BB width at 20-day low
    bb_width_pct: float     # Current BB width as % of price
    bb_width_percentile: float  # Where current width ranks (0-100, lower = tighter)

    # Directional bias
    direction: str          # "bullish", "bearish", "neutral"
    close_position: float   # Where price closed within the NR range (0=low, 1=high)
    trend_bias: str         # "up", "down", "flat" based on 20 EMA
    volume_signal: str      # "dry" (low vol confirms squeeze), "spike" (breakout starting)

    # Trade levels
    buy_above: float        # Long entry = NR high + buffer
    sell_below: float       # Short entry = NR low - buffer
    long_sl: float          # SL for long = NR low
    short_sl: float         # SL for short = NR high
    long_target: float      # 1:2 RR target for long
    short_target: float     # 1:2 RR target for short

    # Score
    squeeze_score: int      # 0-100
    squeeze_type: str       # "NR7+IB" (best), "NR7", "NR4+IB", "NR4", "None"
    notes: list[str]


def analyze_nr_squeeze(
    df: pd.DataFrame,
    symbol: str,
) -> NRSqueezeResult:
    """
    Full NR4/NR7 + Inside Bar + BB Squeeze analysis.
    """
    if len(df) < 20:
        return _empty_nr_result(symbol, 0)

    current_price = float(df["close"].iloc[-1])
    notes: list[str] = []

    # ── Range calculation ────────────────────────────────────────────────
    df = df.copy()
    df["range"] = df["high"] - df["low"]
    ranges = df["range"].values

    today_range = float(ranges[-1])
    today_high = float(df["high"].iloc[-1])
    today_low = float(df["low"].iloc[-1])
    today_close = float(df["close"].iloc[-1])
    yesterday_high = float(df["high"].iloc[-2])
    yesterday_low = float(df["low"].iloc[-2])

    # NR4: narrowest in last 4
    is_nr4 = today_range <= min(ranges[-4:]) if len(ranges) >= 4 else False

    # NR7: narrowest in last 7
    is_nr7 = today_range <= min(ranges[-7:]) if len(ranges) >= 7 else False

    # Inside Bar: today's range completely inside yesterday's
    is_inside_bar = today_high < yesterday_high and today_low > yesterday_low

    # Average range
    avg_range_7d = float(np.mean(ranges[-7:])) if len(ranges) >= 7 else today_range
    nr_range_pct = (today_range / current_price * 100) if current_price > 0 else 0

    if is_nr7:
        notes.append(f"NR7 — narrowest range in 7 days ({nr_range_pct:.2f}%)")
    elif is_nr4:
        notes.append(f"NR4 — narrowest range in 4 days ({nr_range_pct:.2f}%)")

    if is_inside_bar:
        notes.append("Inside Bar — range within previous day's range")

    # ── Bollinger Band squeeze ───────────────────────────────────────────
    close_series = df["close"]
    bb_period = 20
    bb_std = 2

    if len(close_series) >= bb_period:
        sma = close_series.rolling(bb_period).mean()
        std = close_series.rolling(bb_period).std()
        bb_upper = sma + bb_std * std
        bb_lower = sma - bb_std * std
        bb_width = ((bb_upper - bb_lower) / sma * 100).dropna()

        current_bb_width = float(bb_width.iloc[-1]) if len(bb_width) > 0 else 0

        # Check if BB width is at 20-day low
        recent_widths = bb_width.tail(20).values
        bb_squeeze = current_bb_width <= min(recent_widths) * 1.05 if len(recent_widths) >= 10 else False

        # Percentile rank
        all_widths = bb_width.values
        bb_percentile = float(np.sum(all_widths <= current_bb_width) / len(all_widths) * 100) if len(all_widths) > 0 else 50
    else:
        current_bb_width = 0
        bb_squeeze = False
        bb_percentile = 50

    if bb_squeeze:
        notes.append(f"Bollinger Band squeeze — width at {bb_percentile:.0f}th percentile")

    # ── Directional bias ─────────────────────────────────────────────────
    # Where price closed within today's range
    if today_range > 0:
        close_position = (today_close - today_low) / today_range
    else:
        close_position = 0.5

    # EMA 20 trend
    if len(close_series) >= 20:
        ema20 = close_series.ewm(span=20, adjust=False).mean()
        ema20_val = float(ema20.iloc[-1])
        ema20_prev = float(ema20.iloc[-5]) if len(ema20) >= 5 else ema20_val

        if ema20_val > ema20_prev * 1.001:
            trend_bias = "up"
        elif ema20_val < ema20_prev * 0.999:
            trend_bias = "down"
        else:
            trend_bias = "flat"
    else:
        trend_bias = "flat"

    # Volume signal
    if "volume" in df.columns and len(df) >= 10:
        avg_vol = float(df["volume"].tail(10).mean())
        today_vol = float(df["volume"].iloc[-1])
        vol_ratio = today_vol / avg_vol if avg_vol > 0 else 1

        if vol_ratio < 0.7:
            volume_signal = "dry"
            notes.append(f"Volume dry-up ({vol_ratio:.1f}x avg) — confirms squeeze")
        elif vol_ratio > 1.5:
            volume_signal = "spike"
            notes.append(f"Volume spike ({vol_ratio:.1f}x avg) — breakout attempt")
        else:
            volume_signal = "normal"
    else:
        volume_signal = "normal"

    # Direction
    if close_position > 0.65 and trend_bias == "up":
        direction = "bullish"
        notes.append("Bullish bias — closed in upper range, uptrend")
    elif close_position < 0.35 and trend_bias == "down":
        direction = "bearish"
        notes.append("Bearish bias — closed in lower range, downtrend")
    elif trend_bias == "up":
        direction = "bullish"
    elif trend_bias == "down":
        direction = "bearish"
    else:
        direction = "neutral"

    # ── Trade levels ─────────────────────────────────────────────────────
    buffer = today_range * 0.1  # 10% of NR range as buffer
    buy_above = round(today_high + buffer, 2)
    sell_below = round(today_low - buffer, 2)
    long_sl = round(today_low, 2)
    short_sl = round(today_high, 2)

    long_risk = buy_above - long_sl
    short_risk = short_sl - sell_below
    long_target = round(buy_above + long_risk * 2, 2)
    short_target = round(sell_below - short_risk * 2, 2)

    # ── Scoring ──────────────────────────────────────────────────────────
    score = 0

    # NR type (30 pts)
    if is_nr7 and is_inside_bar:
        score += 30
        squeeze_type = "NR7+IB"
    elif is_nr7:
        score += 25
        squeeze_type = "NR7"
    elif is_nr4 and is_inside_bar:
        score += 20
        squeeze_type = "NR4+IB"
    elif is_nr4:
        score += 15
        squeeze_type = "NR4"
    else:
        squeeze_type = "None"

    # BB squeeze confirmation (20 pts)
    if bb_squeeze:
        score += 20
    elif bb_percentile < 30:
        score += 10

    # Volume dry-up (15 pts)
    if volume_signal == "dry":
        score += 15
    elif volume_signal == "spike":
        score += 5

    # Close position clarity (15 pts)
    if close_position > 0.7 or close_position < 0.3:
        score += 15  # Clear directional close
    elif close_position > 0.6 or close_position < 0.4:
        score += 8

    # Trend alignment (10 pts)
    if (direction == "bullish" and trend_bias == "up") or \
       (direction == "bearish" and trend_bias == "down"):
        score += 10

    # Range compression ratio (10 pts)
    if avg_range_7d > 0:
        compression = today_range / avg_range_7d
        if compression < 0.5:
            score += 10
            notes.append(f"Range compressed to {compression:.0%} of avg — extreme squeeze")
        elif compression < 0.7:
            score += 5

    return NRSqueezeResult(
        symbol=symbol,
        current_price=current_price,
        is_nr4=is_nr4,
        is_nr7=is_nr7,
        is_inside_bar=is_inside_bar,
        nr_bar_high=round(today_high, 2),
        nr_bar_low=round(today_low, 2),
        nr_bar_range=round(today_range, 2),
        nr_bar_range_pct=round(nr_range_pct, 2),
        avg_range_7d=round(avg_range_7d, 2),
        bb_squeeze=bb_squeeze,
        bb_width_pct=round(current_bb_width, 2),
        bb_width_percentile=round(bb_percentile, 1),
        direction=direction,
        close_position=round(close_position, 2),
        trend_bias=trend_bias,
        volume_signal=volume_signal,
        buy_above=buy_above,
        sell_below=sell_below,
        long_sl=long_sl,
        short_sl=short_sl,
        long_target=long_target,
        short_target=short_target,
        squeeze_score=min(100, score),
        squeeze_type=squeeze_type,
        notes=notes,
    )


def _empty_nr_result(symbol: str, price: float) -> NRSqueezeResult:
    return NRSqueezeResult(
        symbol=symbol, current_price=price,
        is_nr4=False, is_nr7=False, is_inside_bar=False,
        nr_bar_high=0, nr_bar_low=0, nr_bar_range=0,
        nr_bar_range_pct=0, avg_range_7d=0,
        bb_squeeze=False, bb_width_pct=0, bb_width_percentile=50,
        direction="neutral", close_position=0.5, trend_bias="flat",
        volume_signal="normal",
        buy_above=0, sell_below=0, long_sl=0, short_sl=0,
        long_target=0, short_target=0,
        squeeze_score=0, squeeze_type="None", notes=["Insufficient data"],
    )


def nr_result_to_dict(r: NRSqueezeResult) -> dict:
    return {
        "symbol": r.symbol,
        "current_price": float(r.current_price),
        "is_nr4": bool(r.is_nr4),
        "is_nr7": bool(r.is_nr7),
        "is_inside_bar": bool(r.is_inside_bar),
        "nr_bar_high": float(r.nr_bar_high),
        "nr_bar_low": float(r.nr_bar_low),
        "nr_bar_range": float(r.nr_bar_range),
        "nr_bar_range_pct": float(r.nr_bar_range_pct),
        "avg_range_7d": float(r.avg_range_7d),
        "bb_squeeze": bool(r.bb_squeeze),
        "bb_width_pct": float(r.bb_width_pct),
        "bb_width_percentile": float(r.bb_width_percentile),
        "direction": r.direction,
        "close_position": r.close_position,
        "trend_bias": r.trend_bias,
        "volume_signal": r.volume_signal,
        "buy_above": r.buy_above,
        "sell_below": r.sell_below,
        "long_sl": r.long_sl,
        "short_sl": r.short_sl,
        "long_target": r.long_target,
        "short_target": r.short_target,
        "squeeze_score": r.squeeze_score,
        "squeeze_type": r.squeeze_type,
        "notes": r.notes,
    }
