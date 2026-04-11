"""
engine/signal_scorer.py
========================
Scores each stock across all four timeframes and produces a final
SignalResult with alert tier, trade direction, and all parameters
needed for the dashboard and alert messages.

Scoring per timeframe (max 3 pts each, max 12 total):
    1 pt → CPR direction correct (≥2 consecutive ascending for long)
    1 pt → PEMA stack correct (13>34>55 for long, rising slope)
    1 pt → CPR is narrow (< 0.5% of price) — the A+ bonus

Alert tiers:
    A+  → score 10–12 (all TFs aligned + narrow daily + PEMA stacked)
    B   → score  7–9  (3 of 4 TFs aligned)
    Watch → score 4–6 (monthly + weekly aligned)
    None → score 0–3  (filtered out)
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from datetime import datetime

from .cpr_calculator import (
    CPRSequenceResult, CPRLevel, CPRWidth, CPRDirection,
    is_price_above_cpr, is_price_below_cpr, price_vs_cpr
)
from .pema_calculator import PEMAResult, PEMAStack, PEMASlope
from .config import cfg


# ─── Enums ───────────────────────────────────────────────────────────────────

class AlertTier(str, Enum):
    A_PLUS  = "A+"
    B       = "B"
    WATCH   = "Watch"
    NONE    = "None"


class TradeDirection(str, Enum):
    LONG    = "LONG"
    SHORT   = "SHORT"
    NEUTRAL = "NEUTRAL"   # Conflicting signals — no trade


# ─── Per-timeframe score ──────────────────────────────────────────────────────

@dataclass
class TimeframeScore:
    """Score breakdown for a single timeframe."""
    name: str                    # 'monthly', 'weekly', 'daily', '15min'
    cpr_direction_score: int     # 0 or 1
    pema_stack_score: int        # 0 or 1
    narrow_cpr_score: int        # 0 or 1
    total: int                   # 0–3

    # Detail for dashboard display
    cpr_direction: CPRDirection
    cpr_width_class: CPRWidth
    pema_stack: PEMAStack
    pema_slope: PEMASlope
    consecutive_ascending: int
    consecutive_descending: int
    price_vs_cpr: str            # 'above', 'inside', 'below'
    is_valid_for_long: bool
    is_valid_for_short: bool


# ─── Full signal result ───────────────────────────────────────────────────────

@dataclass
class SignalResult:
    """Complete signal output for one symbol."""
    symbol: str
    scanned_at: datetime
    current_price: float

    # Per-timeframe breakdowns
    monthly: TimeframeScore
    weekly:  TimeframeScore
    daily:   TimeframeScore
    intraday: TimeframeScore    # 15-min

    # Aggregate
    total_score: int            # 0–12
    alert_tier: AlertTier
    direction: TradeDirection

    # Trade parameters (only populated when alert_tier is A+ or B)
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    target_1r2: Optional[float] = None   # 1:2 RR target
    target_1r3: Optional[float] = None   # 1:3 RR target
    sl_distance: Optional[float] = None
    risk_1pct_qty: Optional[int] = None  # Qty for 1% risk on ₹1Cr capital

    # 15-min trigger detail
    trigger_fired: bool = False
    trigger_type: Optional[str] = None   # 'fast_touch' or '34_touch'

    # CPR confluence notes
    is_aplus_bonus: bool = False         # True if narrow daily CPR + all 4 TFs aligned
    notes: list[str] = field(default_factory=list)

    def to_alert_string(self) -> str:
        """Format for Telegram/WhatsApp message."""
        dir_arrow = "↑ LONG" if self.direction == TradeDirection.LONG else "↓ SHORT"
        lines = [
            f"{'🟢' if self.alert_tier == AlertTier.A_PLUS else '🟡'} "
            f"{self.alert_tier.value} {dir_arrow} | {self.symbol}",
            f"Score: {self.total_score}/12",
            f"CMP: ₹{self.current_price}",
        ]
        if self.entry_price:
            lines += [
                f"Entry  : ₹{self.entry_price}",
                f"SL     : ₹{self.stop_loss}  (dist: ₹{self.sl_distance})",
                f"Target : ₹{self.target_1r2} (1:2) | ₹{self.target_1r3} (1:3)",
                f"Qty (1% risk ₹1Cr): {self.risk_1pct_qty} shares",
            ]
        lines += [
            f"M: {self.monthly.cpr_direction.value} {self.monthly.pema_stack.value}  "
            f"W: {self.weekly.cpr_direction.value} {self.weekly.pema_stack.value}  "
            f"D: {self.daily.cpr_width_class.value}  "
            f"15m: {'Triggered ✅' if self.trigger_fired else 'Waiting'}",
        ]
        if self.notes:
            lines.append("Note: " + " | ".join(self.notes))
        return "\n".join(lines)


# ─── Scoring helpers ─────────────────────────────────────────────────────────

def _score_timeframe(
    name: str,
    seq: CPRSequenceResult,
    pema: PEMAResult,
    current_price: float,
    direction: TradeDirection,
) -> TimeframeScore:
    """
    Score a single timeframe (0–3 points).
    Direction determines which conditions we check.
    """
    latest_cpr = seq.latest_cpr

    # 1. CPR direction score
    if direction == TradeDirection.LONG:
        cpr_dir_score = 1 if seq.is_long_valid else 0
    else:
        cpr_dir_score = 1 if seq.is_short_valid else 0

    # 2. PEMA stack score
    if direction == TradeDirection.LONG:
        pema_score = 1 if (pema.is_bullish_stack and pema.is_rising) else 0
    else:
        pema_score = 1 if (pema.is_bearish_stack and pema.is_falling) else 0

    # 3. Narrow CPR score
    narrow_score = 0
    if latest_cpr:
        if latest_cpr.width_class in (CPRWidth.EXTREMELY_NARROW, CPRWidth.NARROW):
            narrow_score = 1

    total = cpr_dir_score + pema_score + narrow_score

    # Direction validity
    is_long_valid = (
        seq.is_long_valid
        and pema.is_bullish_stack
        and (latest_cpr is not None and is_price_above_cpr(current_price, latest_cpr))
    )
    is_short_valid = (
        seq.is_short_valid
        and pema.is_bearish_stack
        and (latest_cpr is not None and is_price_below_cpr(current_price, latest_cpr))
    )

    return TimeframeScore(
        name=name,
        cpr_direction_score=cpr_dir_score,
        pema_stack_score=pema_score,
        narrow_cpr_score=narrow_score,
        total=total,
        cpr_direction=seq.overall_direction,
        cpr_width_class=latest_cpr.width_class if latest_cpr else CPRWidth.EXTREMELY_WIDE,
        pema_stack=pema.stack,
        pema_slope=pema.slope,
        consecutive_ascending=seq.consecutive_ascending,
        consecutive_descending=seq.consecutive_descending,
        price_vs_cpr=price_vs_cpr(current_price, latest_cpr) if latest_cpr else "unknown",
        is_valid_for_long=is_long_valid,
        is_valid_for_short=is_short_valid,
    )


def _determine_direction(
    monthly: TimeframeScore,
    weekly: TimeframeScore,
    daily: TimeframeScore,
    intraday: TimeframeScore,
) -> TradeDirection:
    """
    Determine overall trade direction from timeframe votes.
    Requires at least monthly + weekly to agree. If conflicting, NEUTRAL.
    """
    long_votes  = sum(1 for tf in [monthly, weekly, daily, intraday] if tf.is_valid_for_long)
    short_votes = sum(1 for tf in [monthly, weekly, daily, intraday] if tf.is_valid_for_short)

    # Require monthly + weekly to agree at minimum
    monthly_weekly_long  = monthly.is_valid_for_long  and weekly.is_valid_for_long
    monthly_weekly_short = monthly.is_valid_for_short and weekly.is_valid_for_short

    if monthly_weekly_long and long_votes >= short_votes:
        return TradeDirection.LONG
    elif monthly_weekly_short and short_votes > long_votes:
        return TradeDirection.SHORT
    else:
        return TradeDirection.NEUTRAL


def _classify_alert(score: int) -> AlertTier:
    if score >= cfg.SCORE_A_PLUS_MIN:
        return AlertTier.A_PLUS
    elif score >= cfg.SCORE_B_MIN:
        return AlertTier.B
    elif score >= cfg.SCORE_WATCHLIST_MIN:
        return AlertTier.WATCH
    else:
        return AlertTier.NONE


def _compute_trade_params(
    current_price: float,
    daily_seq: CPRSequenceResult,
    direction: TradeDirection,
    capital: float = 10_000_000,   # ₹1 Cr default
) -> dict:
    """
    Compute entry, SL, targets and position sizing.
    Entry  = current_price (trigger candle close)
    SL     = daily BC (for long) or daily TC (for short)
    Target = 1:2 and 1:3 from entry
    Qty    = risk amount / SL distance
    """
    result = {}
    latest = daily_seq.latest_cpr
    if not latest:
        return result

    risk_amount = capital * (cfg.RISK_PER_TRADE_PCT / 100)  # ₹1,00,000 for ₹1Cr

    if direction == TradeDirection.LONG:
        sl = latest.bc       # Below bottom of CPR band
        if sl >= current_price:
            return result    # Invalid — SL above entry
        sl_dist = round(current_price - sl, 2)
        t1r2 = round(current_price + (sl_dist * 2), 2)
        t1r3 = round(current_price + (sl_dist * 3), 2)
    else:
        sl = latest.tc       # Above top of CPR band
        if sl <= current_price:
            return result
        sl_dist = round(sl - current_price, 2)
        t1r2 = round(current_price - (sl_dist * 2), 2)
        t1r3 = round(current_price - (sl_dist * 3), 2)

    if sl_dist <= 0:
        return result

    qty = int(risk_amount / sl_dist)

    result = {
        "entry_price": round(current_price, 2),
        "stop_loss": round(sl, 2),
        "sl_distance": sl_dist,
        "target_1r2": t1r2,
        "target_1r3": t1r3,
        "risk_1pct_qty": qty,
    }
    return result


# ─── Main scoring function ────────────────────────────────────────────────────

def score_symbol(
    symbol: str,
    current_price: float,
    monthly_seq: CPRSequenceResult,
    weekly_seq: CPRSequenceResult,
    daily_seq: CPRSequenceResult,
    monthly_pema: PEMAResult,
    weekly_pema: PEMAResult,
    daily_pema: PEMAResult,
    intraday_pema: PEMAResult,
    intraday_seq: CPRSequenceResult,
    trigger_15min: dict,
    capital: float = 10_000_000,
) -> SignalResult:
    """
    Master scoring function — call this for each symbol every scan cycle.
    Returns a complete SignalResult ready for dashboard + alerts.
    """
    # Step 1: Determine dominant direction from monthly + weekly
    # FIX: also require price to be on the correct side of the daily CPR
    # Previously, stocks trading BELOW their daily CPR could score LONG
    monthly_long_poss  = monthly_seq.is_long_valid  and monthly_pema.is_bullish_stack
    monthly_short_poss = monthly_seq.is_short_valid and monthly_pema.is_bearish_stack
    weekly_long_poss   = weekly_seq.is_long_valid   and weekly_pema.is_bullish_stack
    weekly_short_poss  = weekly_seq.is_short_valid  and weekly_pema.is_bearish_stack

    # Price vs daily CPR gate — price must confirm direction before assigning it
    daily_cpr = daily_seq.latest_cpr
    price_above_daily_cpr = daily_cpr is not None and is_price_above_cpr(current_price, daily_cpr)
    price_below_daily_cpr = daily_cpr is not None and is_price_below_cpr(current_price, daily_cpr)

    if monthly_long_poss and weekly_long_poss and price_above_daily_cpr:
        direction = TradeDirection.LONG
    elif monthly_short_poss and weekly_short_poss and price_below_daily_cpr:
        direction = TradeDirection.SHORT
    else:
        direction = TradeDirection.NEUTRAL

    # Step 2: Score each timeframe against determined direction
    m_score = _score_timeframe("monthly",  monthly_seq,  monthly_pema,  current_price, direction)
    w_score = _score_timeframe("weekly",   weekly_seq,   weekly_pema,   current_price, direction)
    d_score = _score_timeframe("daily",    daily_seq,    daily_pema,    current_price, direction)
    i_score = _score_timeframe("15min",    intraday_seq, intraday_pema, current_price, direction)

    # Step 3: Total score and alert tier
    total = m_score.total + w_score.total + d_score.total + i_score.total
    tier  = _classify_alert(total)

    # Step 4: 15-min trigger
    trigger_fired = trigger_15min.get("triggered", False)
    trigger_type  = trigger_15min.get("trigger_type")
    if trigger_fired:
        pass  # Trigger already fired → keep score as-is

    # Step 5: A+ bonus check
    daily_latest = daily_seq.latest_cpr
    is_aplus = (
        tier == AlertTier.A_PLUS
        and daily_latest is not None
        and daily_latest.width_class in (CPRWidth.EXTREMELY_NARROW, CPRWidth.NARROW)
        and d_score.pema_stack_score == 1
    )

    # Step 6: Trade parameters
    notes = []
    entry_price = None
    stop_loss   = None
    target_1r2  = None
    target_1r3  = None
    sl_distance = None
    qty         = None

    if tier in (AlertTier.A_PLUS, AlertTier.B) and direction != TradeDirection.NEUTRAL:
        params = _compute_trade_params(current_price, daily_seq, direction, capital)
        if params:
            entry_price = params["entry_price"]
            stop_loss   = params["stop_loss"]
            sl_distance = params["sl_distance"]
            target_1r2  = params["target_1r2"]
            target_1r3  = params["target_1r3"]
            qty         = params["risk_1pct_qty"]

    # Notes for dashboard
    if direction == TradeDirection.NEUTRAL:
        notes.append("Monthly/weekly not aligned — no direction bias")
    if daily_latest and daily_latest.width_class == CPRWidth.EXTREMELY_WIDE:
        notes.append("Daily CPR extremely wide — avoid or reduce size significantly")
    if daily_latest and daily_latest.width_class == CPRWidth.WIDE:
        notes.append("Daily CPR wide — reduce position size")
    if d_score.pema_stack == PEMAStack.TANGLED:
        notes.append("Daily PEMAs tangled — trend not mature, wait")
    if is_aplus:
        notes.append("A+ bonus: narrow CPR + full alignment")

    return SignalResult(
        symbol=symbol,
        scanned_at=datetime.now(),
        current_price=current_price,
        monthly=m_score,
        weekly=w_score,
        daily=d_score,
        intraday=i_score,
        total_score=total,
        alert_tier=tier,
        direction=direction,
        entry_price=entry_price,
        stop_loss=stop_loss,
        target_1r2=target_1r2,
        target_1r3=target_1r3,
        sl_distance=sl_distance,
        risk_1pct_qty=qty,
        trigger_fired=trigger_fired,
        trigger_type=trigger_type,
        is_aplus_bonus=is_aplus,
        notes=notes,
    )


# ─── Batch scorer ────────────────────────────────────────────────────────────

def score_all_symbols(symbol_data: list[dict], capital: float = 10_000_000) -> list[SignalResult]:
    """
    Score a list of symbols. Each dict in symbol_data must contain:
        symbol, current_price,
        monthly_seq, weekly_seq, daily_seq, intraday_seq  (CPRSequenceResult)
        monthly_pema, weekly_pema, daily_pema, intraday_pema  (PEMAResult)
        trigger_15min  (dict from detect_15min_long/short_trigger)

    Returns list sorted by total_score descending.
    """
    results = []
    for data in symbol_data:
        try:
            result = score_symbol(capital=capital, **data)
            if result.alert_tier != AlertTier.NONE:
                results.append(result)
        except Exception as e:
            print(f"[SCORER] Error scoring {data.get('symbol', '?')}: {e}")

    results.sort(key=lambda r: r.total_score, reverse=True)
    return results
