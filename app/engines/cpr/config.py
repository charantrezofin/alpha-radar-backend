"""
Standalone config for CPR Engine — decoupled from CPR_Scanner's config.
Contains only the constants needed by the engine calculators.
"""
from dataclasses import dataclass


@dataclass
class Config:
    # CPR width thresholds (% of price)
    CPR_EXTREMELY_NARROW_PCT: float = 0.20
    CPR_NARROW_PCT: float = 0.50
    CPR_WIDE_PCT: float = 1.00

    # Minimum ascending/descending CPRs for long/short validation
    MIN_ASCENDING_CPRS: int = 2

    # PEMA periods
    PEMA_FAST: int = 13
    PEMA_MEDIUM: int = 34
    PEMA_MID: int = 34  # alias used by pema_calculator
    PEMA_SLOW: int = 55

    # Signal scoring
    SCORE_A_PLUS_MIN: int = 80
    SCORE_A_MIN: int = 60
    SCORE_B_MIN: int = 40
    SCORE_WATCHLIST_MIN: int = 20

    # Risk management
    RISK_PER_TRADE_PCT: float = 1.0  # 1% risk per trade

    # Pattern detection
    PATTERN_MIN_LOOKBACK: int = 60
    PATTERN_CONFIDENCE_THRESHOLD: int = 40
    SWING_WINDOW_DAILY: int = 5
    SWING_WINDOW_WEEKLY: int = 3
    SWING_WINDOW_MONTHLY: int = 2

    # Timeframe lookbacks
    LOOKBACK_MONTHLY: int = 24
    LOOKBACK_WEEKLY: int = 60
    LOOKBACK_DAILY: int = 120
    LOOKBACK_15MIN: int = 200

    # Market hours (IST)
    MARKET_OPEN_HOUR: int = 9
    MARKET_OPEN_MINUTE: int = 15
    MARKET_CLOSE_HOUR: int = 15
    MARKET_CLOSE_MINUTE: int = 30


cfg = Config()
