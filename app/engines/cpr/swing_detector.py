"""
engine/swing_detector.py
=========================
Swing Spectrum — detects 4 swing trading strategies from daily OHLC data:

1. 10 Day Breakout (10D BO) — price breaking above/below 10-day high/low
2. 50 Day Breakout (50D BO) — price breaking above/below 50-day high/low
3. Reversal Radar — stocks reversing after multi-day decline/rally
4. Channel Breakout (Channel BO) — Donchian Channel breakout (20-day)
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional


@dataclass
class SwingSignal:
    symbol: str
    strategy: str           # "10D_BO", "50D_BO", "REVERSAL", "CHANNEL_BO"
    signal: str             # "BULL" or "BEAR"
    current_price: float
    change_pct: float
    trigger_date: str       # Date when signal triggered
    breakout_level: float   # The level that was broken
    stop_loss: float
    target: float
    strength: float         # 0-100 signal strength
    notes: str


def detect_breakout(df: pd.DataFrame, symbol: str, period: int, label: str) -> Optional[SwingSignal]:
    """Detect N-day high/low breakout."""
    if len(df) < period + 1:
        return None

    current = df.iloc[-1]
    lookback = df.iloc[-(period + 1):-1]  # Last N days excluding today

    high_n = float(lookback["high"].max())
    low_n = float(lookback["low"].min())
    close = float(current["close"])
    prev_close = float(df.iloc[-2]["close"])
    change_pct = ((close - prev_close) / prev_close) * 100 if prev_close > 0 else 0
    today_date = str(current["date"])[:10] if "date" in current else ""

    # Average range for SL/target calculation
    avg_range = float(lookback["high"].mean() - lookback["low"].mean())

    # BULL: close above N-day high
    if close > high_n:
        sl = max(float(current["low"]), high_n - avg_range * 0.5)
        risk = close - sl
        target = close + risk * 2
        strength = min(100, 40 + ((close - high_n) / high_n * 100) * 20 + abs(change_pct) * 5)

        return SwingSignal(
            symbol=symbol, strategy=label, signal="BULL",
            current_price=round(close, 2), change_pct=round(change_pct, 2),
            trigger_date=today_date, breakout_level=round(high_n, 2),
            stop_loss=round(sl, 2), target=round(target, 2),
            strength=round(min(100, strength)),
            notes=f"Broke above {period}-day high {high_n:.2f}",
        )

    # BEAR: close below N-day low
    if close < low_n:
        sl = min(float(current["high"]), low_n + avg_range * 0.5)
        risk = sl - close
        target = close - risk * 2
        strength = min(100, 40 + ((low_n - close) / low_n * 100) * 20 + abs(change_pct) * 5)

        return SwingSignal(
            symbol=symbol, strategy=label, signal="BEAR",
            current_price=round(close, 2), change_pct=round(change_pct, 2),
            trigger_date=today_date, breakout_level=round(low_n, 2),
            stop_loss=round(sl, 2), target=round(target, 2),
            strength=round(min(100, strength)),
            notes=f"Broke below {period}-day low {low_n:.2f}",
        )

    return None


def detect_reversal(df: pd.DataFrame, symbol: str) -> Optional[SwingSignal]:
    """
    Reversal Radar — detects stocks reversing after multi-day moves.

    Bullish reversal: stock declined 3+ days, then today closes in upper 60% of range
    Bearish reversal: stock rallied 3+ days, then today closes in lower 40% of range
    """
    if len(df) < 10:
        return None

    current = df.iloc[-1]
    close = float(current["close"])
    high = float(current["high"])
    low = float(current["low"])
    today_range = high - low
    prev_close = float(df.iloc[-2]["close"])
    change_pct = ((close - prev_close) / prev_close) * 100 if prev_close > 0 else 0
    today_date = str(current["date"])[:10] if "date" in current else ""

    if today_range <= 0:
        return None

    close_position = (close - low) / today_range  # 0 = closed at low, 1 = closed at high

    # Check last 5 days for consecutive decline/rally
    last_5 = df.iloc[-6:-1]
    changes = []
    for i in range(1, len(last_5)):
        c = (float(last_5.iloc[i]["close"]) - float(last_5.iloc[i - 1]["close"])) / float(last_5.iloc[i - 1]["close"]) * 100
        changes.append(c)

    declining_days = sum(1 for c in changes if c < -0.3)
    rallying_days = sum(1 for c in changes if c > 0.3)
    total_decline = sum(c for c in changes if c < 0)
    total_rally = sum(c for c in changes if c > 0)

    avg_range_10d = float((df.iloc[-10:]["high"] - df.iloc[-10:]["low"]).mean())

    # Bullish reversal: declining trend + today closes strong
    if declining_days >= 3 and close_position > 0.6 and change_pct > 0:
        sl = round(low - avg_range_10d * 0.3, 2)
        risk = close - sl
        target = round(close + risk * 2, 2)
        strength = min(100, 35 + declining_days * 8 + close_position * 20 + abs(total_decline) * 3)

        return SwingSignal(
            symbol=symbol, strategy="REVERSAL", signal="BULL",
            current_price=round(close, 2), change_pct=round(change_pct, 2),
            trigger_date=today_date,
            breakout_level=round(float(last_5["low"].min()), 2),
            stop_loss=sl, target=target,
            strength=round(min(100, strength)),
            notes=f"Bullish reversal after {declining_days} declining days ({total_decline:.1f}%)",
        )

    # Bearish reversal: rallying trend + today closes weak
    if rallying_days >= 3 and close_position < 0.4 and change_pct < 0:
        sl = round(high + avg_range_10d * 0.3, 2)
        risk = sl - close
        target = round(close - risk * 2, 2)
        strength = min(100, 35 + rallying_days * 8 + (1 - close_position) * 20 + abs(total_rally) * 3)

        return SwingSignal(
            symbol=symbol, strategy="REVERSAL", signal="BEAR",
            current_price=round(close, 2), change_pct=round(change_pct, 2),
            trigger_date=today_date,
            breakout_level=round(float(last_5["high"].max()), 2),
            stop_loss=sl, target=target,
            strength=round(min(100, strength)),
            notes=f"Bearish reversal after {rallying_days} rally days (+{total_rally:.1f}%)",
        )

    return None


def detect_channel_breakout(df: pd.DataFrame, symbol: str, period: int = 20) -> Optional[SwingSignal]:
    """
    Donchian Channel Breakout — price breaking out of N-day channel.
    Channel = highest high and lowest low of last N days.
    """
    if len(df) < period + 1:
        return None

    current = df.iloc[-1]
    lookback = df.iloc[-(period + 1):-1]

    channel_high = float(lookback["high"].max())
    channel_low = float(lookback["low"].min())
    channel_mid = (channel_high + channel_low) / 2
    channel_width = channel_high - channel_low

    close = float(current["close"])
    prev_close = float(df.iloc[-2]["close"])
    change_pct = ((close - prev_close) / prev_close) * 100 if prev_close > 0 else 0
    today_date = str(current["date"])[:10] if "date" in current else ""

    if channel_width <= 0:
        return None

    # BULL: close breaks above channel high
    if close > channel_high:
        sl = round(channel_mid, 2)
        risk = close - sl
        target = round(close + channel_width, 2)  # Target = channel width projected
        strength = min(100, 45 + ((close - channel_high) / channel_high * 100) * 15 + abs(change_pct) * 5)

        return SwingSignal(
            symbol=symbol, strategy="CHANNEL_BO", signal="BULL",
            current_price=round(close, 2), change_pct=round(change_pct, 2),
            trigger_date=today_date,
            breakout_level=round(channel_high, 2),
            stop_loss=sl, target=target,
            strength=round(min(100, strength)),
            notes=f"Broke above {period}-day channel {channel_high:.2f} (width {channel_width:.0f})",
        )

    # BEAR: close breaks below channel low
    if close < channel_low:
        sl = round(channel_mid, 2)
        risk = sl - close
        target = round(close - channel_width, 2)
        strength = min(100, 45 + ((channel_low - close) / channel_low * 100) * 15 + abs(change_pct) * 5)

        return SwingSignal(
            symbol=symbol, strategy="CHANNEL_BO", signal="BEAR",
            current_price=round(close, 2), change_pct=round(change_pct, 2),
            trigger_date=today_date,
            breakout_level=round(channel_low, 2),
            stop_loss=sl, target=target,
            strength=round(min(100, strength)),
            notes=f"Broke below {period}-day channel {channel_low:.2f} (width {channel_width:.0f})",
        )

    return None


def scan_swing_spectrum(df: pd.DataFrame, symbol: str) -> list[SwingSignal]:
    """Run all 4 swing strategies on a single stock. Returns list of triggered signals."""
    signals = []

    # 10-Day Breakout
    sig = detect_breakout(df, symbol, 10, "10D_BO")
    if sig:
        signals.append(sig)

    # 50-Day Breakout
    sig = detect_breakout(df, symbol, 50, "50D_BO")
    if sig:
        signals.append(sig)

    # Reversal Radar
    sig = detect_reversal(df, symbol)
    if sig:
        signals.append(sig)

    # Channel Breakout (20-day Donchian)
    sig = detect_channel_breakout(df, symbol, 20)
    if sig:
        signals.append(sig)

    return signals


def swing_signal_to_dict(s: SwingSignal) -> dict:
    return {
        "symbol": s.symbol,
        "strategy": s.strategy,
        "signal": s.signal,
        "current_price": s.current_price,
        "change_pct": s.change_pct,
        "trigger_date": s.trigger_date,
        "breakout_level": s.breakout_level,
        "stop_loss": s.stop_loss,
        "target": s.target,
        "strength": s.strength,
        "notes": s.notes,
    }
