"""
engine/pema_calculator.py
==========================
Computes the three Pivot EMAs (13, 34, 55) used as the trend filter
in the CPR swing strategy.

Key outputs:
- Current values of fast (13), mid (34), slow (55) EMA
- Whether they are stacked bullishly (13 > 34 > 55) or bearishly (55 > 34 > 13)
- Whether all three are sloping in the same direction (upward/downward)
- The PEMA zone (range between 13 and 34 EMA) — the pullback target zone

Uses pandas EWM (exponential weighted mean) with adjust=False to match
the standard EMA calculation used by TradingView and Kite charts.
"""

from dataclasses import dataclass
from enum import Enum
import pandas as pd
import numpy as np
from .config import cfg


# ─── Enums ───────────────────────────────────────────────────────────────────

class PEMAStack(str, Enum):
    BULLISH  = "bullish"    # 13 > 34 > 55  — all stacked for longs
    BEARISH  = "bearish"    # 55 > 34 > 13  — all stacked for shorts
    TANGLED  = "tangled"    # Mixed — avoid trading, no clear trend
    UNKNOWN  = "unknown"    # Not enough data


class PEMASlope(str, Enum):
    RISING   = "rising"     # All three EMAs pointing upward
    FALLING  = "falling"    # All three EMAs pointing downward
    MIXED    = "mixed"      # Mixed slopes — trend not mature


# ─── Data class ──────────────────────────────────────────────────────────────

@dataclass
class PEMAResult:
    """Complete PEMA analysis for a single symbol on a single timeframe."""

    # Current EMA values (most recent candle)
    fast: float        # 13-period EMA
    mid: float         # 34-period EMA
    slow: float        # 55-period EMA

    # Previous candle EMA values (for slope calculation)
    fast_prev: float
    mid_prev: float
    slow_prev: float

    # Stack assessment
    stack: PEMAStack

    # Slope assessment (based on last N candles)
    slope: PEMASlope

    # Slope angles (approximate — change per candle as % of price)
    fast_slope_pct: float   # Positive = rising, negative = falling
    mid_slope_pct: float
    slow_slope_pct: float

    # PEMA pullback zone — price should dip INTO this range for entry
    pullback_zone_upper: float   # = fast (13 EMA)
    pullback_zone_lower: float   # = mid  (34 EMA)

    # Signal flags for scoring
    is_bullish_stack: bool   # True if stack == BULLISH
    is_bearish_stack: bool   # True if stack == BEARISH
    is_rising: bool          # True if slope == RISING
    is_falling: bool         # True if slope == FALLING

    # Price vs PEMA zone
    price_in_pullback_zone: bool   # Price between 13 and 34 EMA
    price_below_fast_ema: bool     # Price touched/crossed below 13 EMA (pullback signal for longs)
    price_above_fast_ema: bool     # Price touched/crossed above 13 EMA (pullback signal for shorts)


# ─── Core computation ────────────────────────────────────────────────────────

def compute_pema(
    df: pd.DataFrame,
    current_price: float,
    fast: int = None,
    mid: int = None,
    slow: int = None,
) -> PEMAResult:
    """
    Compute PEMA values for a single symbol on a single timeframe.

    Args:
        df:             OHLC DataFrame sorted oldest → newest.
                        Must have 'close' column and at least 55 rows.
        current_price:  Latest price (CMP) for zone comparisons.
        fast/mid/slow:  EMA periods (defaults from config: 13, 34, 55).

    Returns:
        PEMAResult with all values and flags populated.
    """
    fast = fast or cfg.PEMA_FAST    # 13
    mid  = mid  or cfg.PEMA_MID     # 34
    slow = slow or cfg.PEMA_SLOW    # 55

    if df.empty or len(df) < slow + 5:
        raise ValueError(
            f"Need at least {slow + 5} candles for reliable PEMA computation. "
            f"Got {len(df)}. Increase LOOKBACK in config."
        )

    close = df["close"].astype(float)

    # Compute EMAs using pandas EWM (adjust=False = standard EMA, matches TradingView)
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_mid  = close.ewm(span=mid,  adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()

    # Current values (last row)
    fast_now  = round(ema_fast.iloc[-1], 2)
    mid_now   = round(ema_mid.iloc[-1],  2)
    slow_now  = round(ema_slow.iloc[-1], 2)

    # Previous candle values (second to last) for slope
    fast_prev = round(ema_fast.iloc[-2], 2)
    mid_prev  = round(ema_mid.iloc[-2],  2)
    slow_prev = round(ema_slow.iloc[-2], 2)

    # ─── Stack assessment ─────────────────────────────────────────────
    stack = _assess_stack(fast_now, mid_now, slow_now)

    # ─── Slope assessment (using last 3 candles for stability) ────────
    slope, fast_slope_pct, mid_slope_pct, slow_slope_pct = _assess_slope(
        ema_fast, ema_mid, ema_slow, lookback=3
    )

    # ─── Price vs PEMA zone ───────────────────────────────────────────
    # For LONG: pullback zone = price dips to between fast (13) and mid (34) EMA
    # For SHORT: inverse — price rallies to between fast (13) and mid (34) EMA
    zone_upper = fast_now
    zone_lower = mid_now

    price_in_zone         = zone_lower <= current_price <= zone_upper
    price_below_fast      = current_price < fast_now
    price_above_fast      = current_price > fast_now

    return PEMAResult(
        fast=fast_now,
        mid=mid_now,
        slow=slow_now,
        fast_prev=fast_prev,
        mid_prev=mid_prev,
        slow_prev=slow_prev,
        stack=stack,
        slope=slope,
        fast_slope_pct=fast_slope_pct,
        mid_slope_pct=mid_slope_pct,
        slow_slope_pct=slow_slope_pct,
        pullback_zone_upper=zone_upper,
        pullback_zone_lower=zone_lower,
        is_bullish_stack=(stack == PEMAStack.BULLISH),
        is_bearish_stack=(stack == PEMAStack.BEARISH),
        is_rising=(slope == PEMASlope.RISING),
        is_falling=(slope == PEMASlope.FALLING),
        price_in_pullback_zone=price_in_zone,
        price_below_fast_ema=price_below_fast,
        price_above_fast_ema=price_above_fast,
    )


def _assess_stack(fast: float, mid: float, slow: float) -> PEMAStack:
    """Determine EMA stack configuration."""
    if fast > mid > slow:
        return PEMAStack.BULLISH
    elif slow > mid > fast:
        return PEMAStack.BEARISH
    else:
        return PEMAStack.TANGLED


def _assess_slope(
    ema_fast: pd.Series,
    ema_mid: pd.Series,
    ema_slow: pd.Series,
    lookback: int = 3,
) -> tuple[PEMASlope, float, float, float]:
    """
    Assess slope direction using last N candles.
    Returns (slope_enum, fast_slope_pct, mid_slope_pct, slow_slope_pct).
    Slope pct = (current - N periods ago) / N periods ago * 100
    """
    if len(ema_fast) < lookback + 1:
        return PEMASlope.MIXED, 0.0, 0.0, 0.0

    def slope_pct(series: pd.Series) -> float:
        old = series.iloc[-(lookback + 1)]
        now = series.iloc[-1]
        if old == 0:
            return 0.0
        return round(((now - old) / old) * 100, 4)

    fs = slope_pct(ema_fast)
    ms = slope_pct(ema_mid)
    ss = slope_pct(ema_slow)

    all_rising  = fs > 0 and ms > 0 and ss > 0
    all_falling = fs < 0 and ms < 0 and ss < 0

    if all_rising:
        slope = PEMASlope.RISING
    elif all_falling:
        slope = PEMASlope.FALLING
    else:
        slope = PEMASlope.MIXED

    return slope, fs, ms, ss


# ─── 15-min entry trigger detection ──────────────────────────────────────────

def detect_15min_long_trigger(
    df_15min: pd.DataFrame,
    pema: PEMAResult,
) -> dict:
    """
    Checks the last two 15-min candles for the LONG entry trigger:
    1. Previous candle: price touched or dipped below the 13 PEMA (pullback to zone)
    2. Current candle: price closed back ABOVE the 13 PEMA (re-entry)

    This is the exact entry described in the strategy:
    "Price pulls back to 13 or 34 PEMA, then closes back above 13 PEMA → ENTRY"

    Returns a dict with:
        triggered: bool
        trigger_type: 'fast_touch' | '34_touch' | None
        entry_price: float (close of trigger candle)
        candle_date: timestamp
    """
    if df_15min is None or len(df_15min) < 3:
        return {"triggered": False, "trigger_type": None, "entry_price": None, "candle_date": None}

    # Recompute 15-min PEMAs inline on the 15-min frame
    close = df_15min["close"].astype(float)
    ema_fast_series = close.ewm(span=cfg.PEMA_FAST, adjust=False).mean()
    ema_mid_series  = close.ewm(span=cfg.PEMA_MID,  adjust=False).mean()

    # Last three candles
    candle_now  = df_15min.iloc[-1]
    candle_prev = df_15min.iloc[-2]

    fast_now  = ema_fast_series.iloc[-1]
    fast_prev = ema_fast_series.iloc[-2]
    mid_prev  = ema_mid_series.iloc[-2]

    prev_low   = float(candle_prev["low"])
    prev_close = float(candle_prev["close"])
    now_close  = float(candle_now["close"])

    # Condition A: previous candle touched/crossed fast EMA, current closed above it
    touched_fast = prev_low <= fast_prev or prev_close <= fast_prev
    recrossed_fast = now_close > fast_now

    # Condition B: previous candle dipped to mid (34) EMA zone
    touched_mid = prev_low <= float(ema_mid_series.iloc[-2])

    triggered = recrossed_fast and (touched_fast or touched_mid)
    trigger_type = None
    if triggered:
        trigger_type = "34_touch" if touched_mid and not touched_fast else "fast_touch"

    return {
        "triggered": triggered,
        "trigger_type": trigger_type,
        "entry_price": round(now_close, 2) if triggered else None,
        "candle_date": candle_now.get("date", candle_now.name) if triggered else None,
        "fast_ema_now": round(fast_now, 2),
        "mid_ema": round(float(ema_mid_series.iloc[-1]), 2),
    }


def detect_15min_short_trigger(
    df_15min: pd.DataFrame,
    pema: PEMAResult,
) -> dict:
    """
    Mirror of detect_15min_long_trigger for SHORT:
    1. Previous candle: price rallied to touch/cross above 13 PEMA
    2. Current candle: price closed back BELOW 13 PEMA → SHORT ENTRY
    """
    if df_15min is None or len(df_15min) < 3:
        return {"triggered": False, "trigger_type": None, "entry_price": None, "candle_date": None}

    close = df_15min["close"].astype(float)
    ema_fast_series = close.ewm(span=cfg.PEMA_FAST, adjust=False).mean()
    ema_mid_series  = close.ewm(span=cfg.PEMA_MID,  adjust=False).mean()

    candle_now  = df_15min.iloc[-1]
    candle_prev = df_15min.iloc[-2]

    fast_now  = ema_fast_series.iloc[-1]
    fast_prev = ema_fast_series.iloc[-2]

    prev_high  = float(candle_prev["high"])
    prev_close = float(candle_prev["close"])
    now_close  = float(candle_now["close"])

    touched_fast  = prev_high >= fast_prev or prev_close >= fast_prev
    touched_mid   = prev_high >= float(ema_mid_series.iloc[-2])
    recrossed_fast = now_close < fast_now

    triggered = recrossed_fast and (touched_fast or touched_mid)
    trigger_type = None
    if triggered:
        trigger_type = "34_touch" if touched_mid and not touched_fast else "fast_touch"

    return {
        "triggered": triggered,
        "trigger_type": trigger_type,
        "entry_price": round(now_close, 2) if triggered else None,
        "candle_date": candle_now.get("date", candle_now.name) if triggered else None,
        "fast_ema_now": round(fast_now, 2),
        "mid_ema": round(float(ema_mid_series.iloc[-1]), 2),
    }


# ─── Quick test ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pandas as pd
    import numpy as np

    np.random.seed(42)
    n = 80
    prices = 1000 + np.cumsum(np.random.randn(n) * 5)
    df = pd.DataFrame({
        "date":   pd.date_range("2024-08-01", periods=n, freq="B"),
        "open":   prices - np.abs(np.random.randn(n) * 2),
        "high":   prices + np.abs(np.random.randn(n) * 4),
        "low":    prices - np.abs(np.random.randn(n) * 4),
        "close":  prices,
        "volume": np.random.randint(100000, 500000, n),
    })

    current_price = float(df["close"].iloc[-1])
    result = compute_pema(df, current_price)

    print(f"\n=== PEMA Result ===")
    print(f"  Fast (13): {result.fast}    Prev: {result.fast_prev}   Slope: {result.fast_slope_pct:+.4f}%")
    print(f"  Mid  (34): {result.mid}    Prev: {result.mid_prev}   Slope: {result.mid_slope_pct:+.4f}%")
    print(f"  Slow (55): {result.slow}   Prev: {result.slow_prev}  Slope: {result.slow_slope_pct:+.4f}%")
    print(f"  Stack    : {result.stack.value}")
    print(f"  Slope    : {result.slope.value}")
    print(f"  Bullish  : {result.is_bullish_stack}  Rising: {result.is_rising}")
    print(f"  Price    : {current_price}  In pullback zone: {result.price_in_pullback_zone}")
    print(f"  Zone     : {result.pullback_zone_lower} – {result.pullback_zone_upper}")
