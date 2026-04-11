"""
engine/cpr_calculator.py
=========================
Computes CPR levels (TC, BC, P) for any timeframe given an OHLC DataFrame.
Also detects ascending/descending CPR sequences and classifies CPR direction.

Inputs:  pandas DataFrame with columns [date, open, high, low, close, volume]
Outputs: CPRResult dataclass per candle, CPRSequence for direction detection

All formulas are exact matches from the Pivot Boss methodology:
    BC  = (High + Low) / 2
    P   = (High + Low + Close) / 3
    TC  = P + (P - BC)   →  equivalently: (High + Low + 2×Close) / 2 - BC
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional
import pandas as pd
import numpy as np
from .config import cfg


# ─── Enums ───────────────────────────────────────────────────────────────────

class CPRDirection(str, Enum):
    ASCENDING  = "ascending"    # CPR moved higher than previous period
    DESCENDING = "descending"   # CPR moved lower than previous period
    PARALLEL   = "parallel"     # CPR at roughly same level (within tolerance)
    UNKNOWN    = "unknown"      # Not enough data yet


class CPRWidth(str, Enum):
    EXTREMELY_NARROW = "extremely_narrow"   # < 0.20% of price  → A+
    NARROW           = "narrow"             # 0.20–0.50%        → A
    WIDE             = "wide"               # 0.50–1.00%        → B
    EXTREMELY_WIDE   = "extremely_wide"     # > 1.00%           → avoid


# ─── Data classes ────────────────────────────────────────────────────────────

@dataclass
class CPRLevel:
    """CPR levels for a single completed candle (to be plotted on the NEXT period)."""
    date: pd.Timestamp          # The candle date these levels were computed FROM
    high: float
    low: float
    close: float
    bc: float                   # Bottom of CPR
    pivot: float                # Central pivot P
    tc: float                   # Top of CPR
    width_pts: float            # TC - BC in price points
    width_pct: float            # (TC - BC) / close * 100  — used for classification
    width_class: CPRWidth
    direction: CPRDirection = CPRDirection.UNKNOWN   # filled in after sequence analysis


@dataclass
class CPRSequenceResult:
    """
    Result of analysing N consecutive CPR levels for directional trend.
    Returned by analyse_cpr_sequence().
    """
    n_periods: int                        # How many periods analysed
    n_ascending: int                      # Count of ascending CPR moves
    n_descending: int                     # Count of descending CPR moves
    n_parallel: int
    consecutive_ascending: int            # Current streak of ascending CPRs
    consecutive_descending: int           # Current streak of descending CPRs
    overall_direction: CPRDirection       # Dominant direction
    is_long_valid: bool                   # True if ≥ MIN_ASCENDING_CPRS consecutive ascending
    is_short_valid: bool                  # True if ≥ MIN_ASCENDING_CPRS consecutive descending
    latest_cpr: Optional[CPRLevel]        # Most recent CPR (the one active NOW)
    previous_cpr: Optional[CPRLevel]      # One period back


# ─── Core computation ────────────────────────────────────────────────────────

def compute_cpr_level(
    date: pd.Timestamp,
    high: float,
    low: float,
    close: float,
) -> CPRLevel:
    """
    Compute CPR levels from a single period's High/Low/Close.
    These levels apply to the NEXT period.
    """
    bc    = (high + low) / 2
    pivot = (high + low + close) / 3
    tc    = pivot + (pivot - bc)

    width_pts = tc - bc
    width_pct = (width_pts / close) * 100 if close > 0 else 0.0
    width_class = _classify_width(width_pct)

    return CPRLevel(
        date=date,
        high=high,
        low=low,
        close=close,
        bc=round(bc, 2),
        pivot=round(pivot, 2),
        tc=round(tc, 2),
        width_pts=round(width_pts, 2),
        width_pct=round(width_pct, 4),
        width_class=width_class,
    )


def _classify_width(width_pct: float) -> CPRWidth:
    """Classify CPR width percentage into the four categories."""
    if width_pct < cfg.CPR_EXTREMELY_NARROW_PCT:
        return CPRWidth.EXTREMELY_NARROW
    elif width_pct < cfg.CPR_NARROW_PCT:
        return CPRWidth.NARROW
    elif width_pct < cfg.CPR_WIDE_PCT:
        return CPRWidth.WIDE
    else:
        return CPRWidth.EXTREMELY_WIDE


# ─── Batch computation from DataFrame ────────────────────────────────────────

def compute_cpr_series(df: pd.DataFrame) -> list[CPRLevel]:
    """
    Given a full OHLC DataFrame (sorted oldest → newest), compute CPR levels
    for every row. Each CPRLevel represents levels derived FROM that candle
    (i.e., levels that will be ACTIVE in the next period).

    Required DataFrame columns: date, high, low, close
    'date' can be the index or a column.
    """
    if df.empty:
        return []

    # Normalise: ensure date is a column
    df = df.copy()
    if df.index.name == "date" or isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()

    df = df.sort_values("date").reset_index(drop=True)

    levels: list[CPRLevel] = []
    for _, row in df.iterrows():
        level = compute_cpr_level(
            date=row["date"],
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
        )
        levels.append(level)

    return levels


def compute_cpr_direction(levels: list[CPRLevel]) -> list[CPRLevel]:
    """
    Assign direction to each CPRLevel by comparing its pivot to the previous period's pivot.
    Parallel tolerance: within 0.10% of price.
    Mutates the input list in-place and also returns it.
    """
    PARALLEL_TOLERANCE_PCT = 0.10

    for i, level in enumerate(levels):
        if i == 0:
            level.direction = CPRDirection.UNKNOWN
            continue

        prev = levels[i - 1]
        change_pct = ((level.pivot - prev.pivot) / prev.pivot) * 100

        if abs(change_pct) < PARALLEL_TOLERANCE_PCT:
            level.direction = CPRDirection.PARALLEL
        elif change_pct > 0:
            level.direction = CPRDirection.ASCENDING
        else:
            level.direction = CPRDirection.DESCENDING

    return levels


# ─── Sequence analysis ───────────────────────────────────────────────────────

def analyse_cpr_sequence(levels: list[CPRLevel]) -> CPRSequenceResult:
    """
    Given a list of CPRLevels (with direction already assigned), analyse the
    sequence to determine:
    - Current consecutive ascending/descending streak
    - Whether the long or short condition is satisfied
    - Overall dominant direction
    """
    if not levels:
        return CPRSequenceResult(
            n_periods=0, n_ascending=0, n_descending=0, n_parallel=0,
            consecutive_ascending=0, consecutive_descending=0,
            overall_direction=CPRDirection.UNKNOWN,
            is_long_valid=False, is_short_valid=False,
            latest_cpr=None, previous_cpr=None
        )

    # Only count levels with known direction
    known = [l for l in levels if l.direction != CPRDirection.UNKNOWN]

    n_asc  = sum(1 for l in known if l.direction == CPRDirection.ASCENDING)
    n_desc = sum(1 for l in known if l.direction == CPRDirection.DESCENDING)
    n_par  = sum(1 for l in known if l.direction == CPRDirection.PARALLEL)

    # Consecutive streak counting from the most recent period backwards
    consec_asc  = 0
    consec_desc = 0

    for level in reversed(known):
        if level.direction == CPRDirection.ASCENDING:
            if consec_desc == 0:   # Only count if no descending break yet
                consec_asc += 1
        elif level.direction == CPRDirection.DESCENDING:
            if consec_asc == 0:
                consec_desc += 1
        else:
            break  # Parallel breaks the streak

    # Overall direction: whichever has more recent consecutive moves
    if consec_asc > 0:
        overall = CPRDirection.ASCENDING
    elif consec_desc > 0:
        overall = CPRDirection.DESCENDING
    else:
        overall = CPRDirection.PARALLEL

    return CPRSequenceResult(
        n_periods=len(levels),
        n_ascending=n_asc,
        n_descending=n_desc,
        n_parallel=n_par,
        consecutive_ascending=consec_asc,
        consecutive_descending=consec_desc,
        overall_direction=overall,
        is_long_valid=(consec_asc >= cfg.MIN_ASCENDING_CPRS),
        is_short_valid=(consec_desc >= cfg.MIN_ASCENDING_CPRS),
        latest_cpr=levels[-1] if levels else None,
        previous_cpr=levels[-2] if len(levels) >= 2 else None,
    )


# ─── Single-symbol convenience function ──────────────────────────────────────

def get_cpr_analysis(df: pd.DataFrame) -> tuple[list[CPRLevel], CPRSequenceResult]:
    """
    Full pipeline: compute CPR for all rows → assign directions → analyse sequence.
    Returns (levels_list, sequence_result).
    Callers typically only need sequence_result for scoring.
    """
    levels = compute_cpr_series(df)
    levels = compute_cpr_direction(levels)
    sequence = analyse_cpr_sequence(levels)
    return levels, sequence


# ─── Price vs CPR check ──────────────────────────────────────────────────────

def is_price_above_cpr(current_price: float, latest_level: CPRLevel) -> bool:
    """True if current price is above the TC (top of CPR band). Required for longs."""
    return current_price > latest_level.tc


def is_price_below_cpr(current_price: float, latest_level: CPRLevel) -> bool:
    """True if current price is below the BC (bottom of CPR band). Required for shorts."""
    return current_price < latest_level.bc


def price_vs_cpr(current_price: float, latest_level: CPRLevel) -> str:
    """Returns 'above', 'inside', or 'below' relative to CPR band."""
    if current_price > latest_level.tc:
        return "above"
    elif current_price < latest_level.bc:
        return "below"
    else:
        return "inside"


# ─── Quick test ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pandas as pd

    # Simulate 5 monthly candles with ascending CPRs
    test_data = pd.DataFrame({
        "date":  pd.date_range("2025-10-01", periods=5, freq="ME"),
        "high":  [1100, 1150, 1200, 1260, 1310],
        "low":   [1000, 1050, 1090, 1140, 1190],
        "close": [1080, 1120, 1170, 1230, 1290],
    })

    levels, seq = get_cpr_analysis(test_data)

    print("\n=== CPR Levels ===")
    for l in levels:
        print(f"  {l.date.date()}  BC={l.bc}  P={l.pivot}  TC={l.tc}  "
              f"Width={l.width_pct:.2f}%  [{l.width_class.value}]  Dir={l.direction.value}")

    print(f"\n=== Sequence Analysis ===")
    print(f"  Consecutive ascending : {seq.consecutive_ascending}")
    print(f"  Is long valid         : {seq.is_long_valid}")
    print(f"  Overall direction     : {seq.overall_direction.value}")
    print(f"  Latest CPR TC         : {seq.latest_cpr.tc}")
    print(f"  Latest width class    : {seq.latest_cpr.width_class.value}")


# ─── Forward-looking: project next-period CPR ────────────────────────────────

@dataclass
class ProjectedCPR:
    """
    Estimated CPR for the NEXT period based on current period's partial data.
    Used by the Breakout Radar to identify upcoming narrow-CPR setups.
    """
    source_period: str              # e.g. "2025-03" (the period we're projecting FROM)
    target_period: str              # e.g. "2025-04" (the period this CPR will be active in)
    projected_bc: float
    projected_pivot: float
    projected_tc: float
    projected_width_pct: float
    projected_width_class: CPRWidth
    current_month_high: float       # H so far this period
    current_month_low: float        # L so far this period
    current_close: float            # Last close used as proxy for period close
    confidence: str                 # "high" / "medium" / "low"
    # confidence = high if >= 15 trading days in period, medium 8–14, low < 8


def project_next_period_cpr(
    df_daily: pd.DataFrame,
    timeframe: str = "monthly",
) -> Optional["ProjectedCPR"]:
    """
    Project the CPR that will be ACTIVE in the next period (e.g. April)
    by using the current (incomplete) period's H/L/C data so far.

    Logic:
        - For monthly: aggregate all daily candles in the current calendar month
          → compute H, L, close-of-last-candle → apply CPR formula
        - For weekly: same but for current ISO week

    This is inherently an estimate — the more of the period has elapsed,
    the more reliable the projection.

    Returns None if there are no candles in the current period yet.
    """
    if df_daily is None or df_daily.empty:
        return None

    df = df_daily.copy()
    if df.index.name == "date" or isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    last_date = df["date"].iloc[-1]

    if timeframe == "monthly":
        # Current month's candles
        mask = (df["date"].dt.year == last_date.year) & \
               (df["date"].dt.month == last_date.month)
        period_df = df[mask]

        source_period = last_date.strftime("%Y-%m")
        # Next month
        if last_date.month == 12:
            target_period = f"{last_date.year + 1}-01"
        else:
            target_period = f"{last_date.year}-{last_date.month + 1:02d}"

    elif timeframe == "weekly":
        # Current ISO week's candles
        mask = (df["date"].dt.isocalendar().year == last_date.isocalendar().year) & \
               (df["date"].dt.isocalendar().week == last_date.isocalendar().week)
        period_df = df[mask]

        source_period = f"{last_date.isocalendar().year}-W{last_date.isocalendar().week:02d}"
        next_week = last_date + pd.Timedelta(weeks=1)
        target_period = f"{next_week.isocalendar().year}-W{next_week.isocalendar().week:02d}"
    else:
        return None

    if period_df.empty:
        return None

    # Aggregate the partial period
    period_high  = float(period_df["high"].max())
    period_low   = float(period_df["low"].min())
    period_close = float(period_df["close"].iloc[-1])
    n_days = len(period_df)

    # Compute projected CPR
    bc    = (period_high + period_low) / 2
    pivot = (period_high + period_low + period_close) / 3
    tc    = pivot + (pivot - bc)

    width_pts = tc - bc
    width_pct = (width_pts / period_close) * 100 if period_close > 0 else 0.0
    width_class = _classify_width(width_pct)

    # Confidence based on how much of the period has elapsed
    if timeframe == "monthly":
        confidence = "high" if n_days >= 15 else ("medium" if n_days >= 8 else "low")
    else:
        confidence = "high" if n_days >= 4 else ("medium" if n_days >= 2 else "low")

    return ProjectedCPR(
        source_period=source_period,
        target_period=target_period,
        projected_bc=round(bc, 2),
        projected_pivot=round(pivot, 2),
        projected_tc=round(tc, 2),
        projected_width_pct=round(width_pct, 4),
        projected_width_class=width_class,
        current_month_high=round(period_high, 2),
        current_month_low=round(period_low, 2),
        current_close=round(period_close, 2),
        confidence=confidence,
    )


def get_breakout_radar_score(
    df_daily: pd.DataFrame,
    df_weekly: pd.DataFrame,
    current_price: float,
) -> dict:
    """
    Compute a forward-looking "breakout probability" score for the next month.
    Used by the Breakout Radar tab.

    Returns dict with:
        projected_monthly_cpr  : ProjectedCPR
        projected_weekly_cpr   : ProjectedCPR
        monthly_seq            : CPRSequenceResult (historical trend)
        weekly_seq             : CPRSequenceResult
        breakout_score         : 0–5 (higher = more likely breakout next period)
        direction              : "LONG" / "SHORT" / "NEUTRAL"
        tier                   : "A+" / "B" / "Watch" / "Skip"
        reasons                : list of strings explaining the score
    """
    reasons = []
    score = 0
    direction = "NEUTRAL"

    # 1. Project next month's CPR
    proj_monthly = project_next_period_cpr(df_daily, timeframe="monthly")
    proj_weekly  = project_next_period_cpr(df_weekly if df_weekly is not None else df_daily,
                                           timeframe="weekly")

    # 2. Historical CPR trend (are we in an ascending sequence?)
    _, monthly_seq = get_cpr_analysis(df_daily.resample("ME", on="date").agg(
        {"high": "max", "low": "min", "close": "last", "open": "first"}
    ).dropna().reset_index()) if not df_daily.empty else ([], None)

    _, weekly_seq  = get_cpr_analysis(df_daily.resample("W-FRI", on="date").agg(
        {"high": "max", "low": "min", "close": "last", "open": "first"}
    ).dropna().reset_index()) if not df_daily.empty else ([], None)

    _, daily_seq   = get_cpr_analysis(df_daily) if not df_daily.empty else ([], None)

    # Score: projected monthly CPR is narrow
    if proj_monthly:
        if proj_monthly.projected_width_class == CPRWidth.EXTREMELY_NARROW:
            score += 2
            reasons.append(f"Projected April CPR extremely narrow ({proj_monthly.projected_width_pct:.2f}%) — A+ setup forming")
        elif proj_monthly.projected_width_class == CPRWidth.NARROW:
            score += 1
            reasons.append(f"Projected April CPR narrow ({proj_monthly.projected_width_pct:.2f}%)")

    # Score: current monthly ascending streak
    if monthly_seq and monthly_seq.consecutive_ascending >= 3:
        score += 2
        direction = "LONG"
        reasons.append(f"Monthly CPR ascending {monthly_seq.consecutive_ascending} consecutive periods")
    elif monthly_seq and monthly_seq.consecutive_ascending >= 2:
        score += 1
        direction = "LONG"
        reasons.append(f"Monthly CPR ascending {monthly_seq.consecutive_ascending} periods")
    elif monthly_seq and monthly_seq.consecutive_descending >= 2:
        direction = "SHORT"
        reasons.append(f"Monthly CPR descending {monthly_seq.consecutive_descending} periods — short setup")

    # Score: weekly CPR alignment
    if weekly_seq:
        if direction == "LONG" and weekly_seq.consecutive_ascending >= 2:
            score += 1
            reasons.append("Weekly CPR also ascending — multi-TF alignment")
        elif direction == "SHORT" and weekly_seq.consecutive_descending >= 2:
            score += 1
            reasons.append("Weekly CPR also descending — multi-TF alignment")

    # Tier
    if score >= 4:
        tier = "A+"
    elif score >= 3:
        tier = "B"
    elif score >= 2:
        tier = "Watch"
    else:
        tier = "Skip"

    if not reasons:
        reasons.append("No strong CPR alignment forming for next period")

    return {
        "projected_monthly_cpr": proj_monthly,
        "projected_weekly_cpr":  proj_weekly,
        "monthly_seq":           monthly_seq,
        "weekly_seq":            weekly_seq,
        "daily_seq":             daily_seq,
        "breakout_score":        score,
        "direction":             direction,
        "tier":                  tier,
        "reasons":               reasons,
    }
