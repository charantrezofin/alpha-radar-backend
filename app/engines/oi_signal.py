"""
OI Signal Engine -- analyses options chain + price data to generate
directional trade signals with CE/PE and cash recommendations.

7-component scoring: Momentum, PCR, OI Change, Max Pain, IV Skew, OI Unwinding, Price Position
Score range: -100 (strong bearish) to +100 (strong bullish)

Ported from tradingdesk/apps/gateway/src/scoring/oi-signal.ts
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, List, Literal, Optional


# ── Types ────────────────────────────────────────────────────────────────────


@dataclass
class OptionLeg:
    token: Optional[int] = None
    oi: int = 0
    oi_change: int = 0
    volume: int = 0
    ltp: float = 0.0
    iv: Optional[float] = None
    tradingsymbol: str = ""
    bid_qty: int = 0
    ask_qty: int = 0
    lot_size: Optional[int] = None


@dataclass
class ChainRow:
    strike: float
    call: Optional[OptionLeg] = None
    put: Optional[OptionLeg] = None


@dataclass
class ChainAnalytics:
    pcr: float
    pcr_sentiment: str
    max_pain_strike: float
    max_pain_distance: float
    total_call_oi: int
    total_put_oi: int
    resistance: List[float] = field(default_factory=list)
    support: List[float] = field(default_factory=list)
    atm_iv: float = 0.0


@dataclass
class SignalRecommendation:
    strike: float
    option_type: Literal["CE", "PE"]
    tradingsymbol: str
    entry: float
    stop_loss: float
    target1: float
    target2: float
    iv: Optional[float]
    oi: int
    volume: int
    lot_size: Optional[int] = None


@dataclass
class CashRecommendation:
    action: Literal["BUY", "SELL"]
    entry: float
    stop_loss: float
    target1: float
    target2: float


@dataclass
class ScoreBreakdown:
    momentum: int
    pcr: int
    oi_change: int
    max_pain: int
    iv_skew: int
    oi_unwinding: int
    price_position: int


@dataclass
class OISignal:
    symbol: str
    category: Literal["index", "stock"]
    direction: Literal["BULLISH", "BEARISH", "NEUTRAL"]
    confidence: Literal["STRONG", "MODERATE", "WEAK"]
    score: int
    recommendation: Optional[SignalRecommendation]
    cash_recommendation: Optional[CashRecommendation]
    analytics: dict
    score_breakdown: ScoreBreakdown
    reasons: List[str]
    timestamp: int


@dataclass
class IndexConfig:
    name: str
    strike_step: float
    expiry: str  # expiry date string


# ── Helpers ──────────────────────────────────────────────────────────────────


def _clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def _format_oi(oi: float) -> str:
    abs_oi = abs(oi)
    if abs_oi >= 10_000_000:
        return f"{oi / 10_000_000:.1f}Cr"
    if abs_oi >= 100_000:
        return f"{oi / 100_000:.1f}L"
    if abs_oi >= 1_000:
        return f"{oi / 1_000:.1f}K"
    return str(int(oi))


def _days_until(expiry_str: str) -> int:
    try:
        expiry = datetime.strptime(expiry_str, "%Y-%m-%d")
    except ValueError:
        # Try alternative formats
        try:
            expiry = datetime.strptime(expiry_str, "%a %b %d %Y")
        except ValueError:
            return 5  # default fallback
    now = datetime.now()
    delta = (expiry - now).days
    return max(0, math.ceil(delta))


# ── Scoring Components ───────────────────────────────────────────────────────


def _score_momentum(change_pct: float, reasons: List[str]) -> int:
    if change_pct >= 3:
        reasons.append(f"Strong rally +{change_pct:.1f}% -- aggressive buying")
        return 20
    if change_pct >= 1.5:
        reasons.append(f"Solid up move +{change_pct:.1f}%")
        return 15
    if change_pct >= 0.5:
        return 8
    if change_pct >= 0.2:
        return 3
    if change_pct <= -3:
        reasons.append(f"Sharp sell-off {change_pct:.1f}% -- panic selling")
        return -20
    if change_pct <= -1.5:
        reasons.append(f"Bearish decline {change_pct:.1f}%")
        return -15
    if change_pct <= -0.5:
        return -8
    if change_pct <= -0.2:
        return -3
    return 0


def _score_pcr(pcr: float, reasons: List[str]) -> int:
    if pcr >= 1.5:
        reasons.append(f"PCR {pcr} -- extreme PE writing, strong bullish")
        return 20
    if pcr >= 1.3:
        reasons.append(f"PCR {pcr} -- heavy PE writing, bullish")
        return 15
    if pcr >= 1.1:
        reasons.append(f"PCR {pcr} -- moderately bullish")
        return 8
    if pcr >= 0.9:
        return 0
    if pcr >= 0.7:
        reasons.append(f"PCR {pcr} -- moderately bearish")
        return -8
    if pcr >= 0.5:
        reasons.append(f"PCR {pcr} -- heavy CE writing, bearish")
        return -15
    reasons.append(f"PCR {pcr} -- extreme CE writing, strong bearish")
    return -20


def _score_oi_change(
    chain: List[ChainRow],
    spot: float,
    strike_step: float,
    reasons: List[str],
) -> int:
    near_strikes = [r for r in chain if abs(r.strike - spot) <= strike_step * 5]
    if not near_strikes:
        return 0

    ce_oi_change = sum((r.call.oi_change if r.call else 0) for r in near_strikes)
    pe_oi_change = sum((r.put.oi_change if r.put else 0) for r in near_strikes)

    if pe_oi_change > 0 and ce_oi_change > 0:
        ratio = pe_oi_change / ce_oi_change
        if ratio > 1.5:
            reasons.append(
                f"Fresh PE writing {_format_oi(pe_oi_change)} >> CE {_format_oi(ce_oi_change)} -- support building"
            )
            return 15
        if ratio > 1.2:
            return 8
        if ratio < 0.67:
            reasons.append(
                f"Fresh CE writing {_format_oi(ce_oi_change)} >> PE {_format_oi(pe_oi_change)} -- resistance building"
            )
            return -15
        if ratio < 0.83:
            return -8
    elif pe_oi_change > 0 and ce_oi_change <= 0:
        reasons.append(f"PE writing {_format_oi(pe_oi_change)} with CE unwinding -- bullish")
        return 15
    elif ce_oi_change > 0 and pe_oi_change <= 0:
        reasons.append(f"CE writing {_format_oi(ce_oi_change)} with PE unwinding -- bearish")
        return -15
    return 0


def _score_max_pain(
    spot: float,
    max_pain_strike: float,
    expiry_str: str,
    reasons: List[str],
) -> int:
    dist_pct = ((spot - max_pain_strike) / spot) * 100
    dte = _days_until(expiry_str)

    weight = 1.0 if dte <= 2 else (0.6 if dte <= 5 else 0.25)

    raw = 0
    if abs(dist_pct) > 1:
        raw = -12 if dist_pct > 0 else 12
        if dte <= 3:
            direction = "above" if dist_pct > 0 else "below"
            pull = "strong" if dte <= 2 else "moderate"
            reasons.append(
                f"Spot {abs(dist_pct):.1f}% {direction} max pain {max_pain_strike} "
                f"-- pull {pull} ({dte}d to expiry)"
            )
    elif abs(dist_pct) > 0.3:
        raw = -6 if dist_pct > 0 else 6

    return round(raw * weight)


def _score_iv_skew(chain: List[ChainRow], spot: float, reasons: List[str]) -> int:
    if not chain:
        return 0
    atm = min(chain, key=lambda r: abs(r.strike - spot))
    if not atm.call or not atm.put or atm.call.iv is None or atm.put.iv is None:
        return 0

    skew = atm.put.iv - atm.call.iv
    if skew > 3:
        reasons.append(f"Put IV {atm.put.iv}% > Call IV {atm.call.iv}% -- fear premium")
        return -5
    if skew > 1:
        return -3
    if skew < -3:
        reasons.append(f"Call IV {atm.call.iv}% > Put IV {atm.put.iv}% -- call demand")
        return 5
    if skew < -1:
        return 3
    return 0


def _score_oi_unwinding(
    chain: List[ChainRow],
    resistance: List[float],
    support: List[float],
    reasons: List[str],
) -> int:
    score = 0

    for strike in resistance:
        row = next((r for r in chain if r.strike == strike), None)
        if row and row.call and row.call.oi_change < 0:
            score += 5
            reasons.append(
                f"CE unwinding at {strike} ({_format_oi(abs(row.call.oi_change))}) -- resistance weakening"
            )

    for strike in support:
        row = next((r for r in chain if r.strike == strike), None)
        if row and row.put and row.put.oi_change < 0:
            score -= 5
            reasons.append(
                f"PE unwinding at {strike} ({_format_oi(abs(row.put.oi_change))}) -- support weakening"
            )

    return int(_clamp(score, -12, 12))


def _score_price_position(
    spot: float,
    resistance: List[float],
    support: List[float],
    reasons: List[str],
) -> int:
    nearest_resistance = min(resistance) if resistance else 0
    nearest_support = max(support) if support else 0

    if nearest_resistance and spot > nearest_resistance * 1.003:
        reasons.append(f"Spot above resistance {nearest_resistance} -- breakout confirmed")
        return 12
    if nearest_resistance and spot > nearest_resistance * 0.997:
        reasons.append(f"Spot testing resistance at {nearest_resistance}")
        return -5
    if nearest_support and spot < nearest_support * 0.997:
        reasons.append(f"Spot below support {nearest_support} -- breakdown")
        return -12
    if nearest_support and spot < nearest_support * 1.003:
        reasons.append(f"Spot holding support at {nearest_support}")
        return 5
    return 0


# ── Strike Selection ─────────────────────────────────────────────────────────


def _select_strike(
    chain: List[ChainRow],
    spot: float,
    direction: Literal["BULLISH", "BEARISH"],
    strike_step: float,
    atm_iv: float,
) -> Optional[SignalRecommendation]:
    if not chain:
        return None

    atm_strike = min(chain, key=lambda r: abs(r.strike - spot)).strike

    if direction == "BULLISH":
        candidates = [atm_strike, atm_strike + strike_step, atm_strike + 2 * strike_step]
    else:
        candidates = [atm_strike, atm_strike - strike_step, atm_strike - 2 * strike_step]

    option_type: Literal["CE", "PE"] = "CE" if direction == "BULLISH" else "PE"

    best_score = -1
    best_option: Optional[OptionLeg] = None
    best_strike: float = 0

    for strike in candidates:
        row = next((r for r in chain if r.strike == strike), None)
        if not row:
            continue
        opt = row.call if option_type == "CE" else row.put
        if not opt or opt.ltp <= 0:
            continue

        score = 0
        if opt.oi > 100_000:
            score += 3
        elif opt.oi > 50_000:
            score += 2
        elif opt.oi > 10_000:
            score += 1

        if opt.iv is not None and atm_iv > 0 and opt.iv <= atm_iv * 1.1:
            score += 2
        elif opt.iv is not None and atm_iv > 0 and opt.iv <= atm_iv * 1.2:
            score += 1

        if opt.bid_qty > 0 and opt.ask_qty > 0:
            score += 1
        if opt.volume > 50_000:
            score += 2
        elif opt.volume > 10_000:
            score += 1

        if strike == atm_strike:
            score += 3
        elif abs(strike - atm_strike) == strike_step:
            score += 1

        if score > best_score:
            best_score = score
            best_option = opt
            best_strike = strike

    if best_option is None:
        return None

    entry = best_option.ltp
    risk_amount = entry * 0.30

    return SignalRecommendation(
        strike=best_strike,
        option_type=option_type,
        tradingsymbol=best_option.tradingsymbol,
        entry=round(entry, 2),
        stop_loss=round(entry - risk_amount, 2),
        target1=round(entry + risk_amount * 1.5, 2),
        target2=round(entry + risk_amount * 2, 2),
        iv=best_option.iv,
        oi=best_option.oi,
        volume=best_option.volume,
        lot_size=best_option.lot_size,
    )


# ── Cash Recommendation ──────────────────────────────────────────────────────


def _compute_cash_recommendation(
    spot: float,
    direction: Literal["BULLISH", "BEARISH"],
    support: List[float],
    resistance: List[float],
) -> CashRecommendation:
    nearest_support = max(support) if support else spot * 0.98
    nearest_resistance = min(resistance) if resistance else spot * 1.02

    if direction == "BULLISH":
        sl = min(nearest_support, spot * 0.985)
        risk = spot - sl
        return CashRecommendation(
            action="BUY",
            entry=round(spot, 2),
            stop_loss=round(sl, 2),
            target1=round(spot + risk * 1.5, 2),
            target2=round(spot + risk * 2, 2),
        )
    else:
        sl = max(nearest_resistance, spot * 1.015)
        risk = sl - spot
        return CashRecommendation(
            action="SELL",
            entry=round(spot, 2),
            stop_loss=round(sl, 2),
            target1=round(spot - risk * 1.5, 2),
            target2=round(spot - risk * 2, 2),
        )


# ── Main Signal Computation ──────────────────────────────────────────────────


def compute_oi_signal(
    symbol: str,
    category: Literal["index", "stock"],
    spot: float,
    prev_close: float,
    expiry: str,
    strike_step: float,
    chain: List[ChainRow],
    analytics: ChainAnalytics,
) -> OISignal:
    """
    Port of ``computeOISignal`` from oi-signal.ts.
    Combines all 7 scoring components and produces trade recommendations.
    """
    reasons: List[str] = []

    change_pct = ((spot - prev_close) / prev_close * 100) if prev_close > 0 else 0.0

    # 7 scoring components
    momentum_score = _score_momentum(change_pct, reasons)
    pcr_score = _score_pcr(analytics.pcr, reasons)
    oi_change_score = _score_oi_change(chain, spot, strike_step, reasons)
    max_pain_score = _score_max_pain(spot, analytics.max_pain_strike, expiry, reasons)
    iv_skew_score = _score_iv_skew(chain, spot, reasons)
    unwinding_score = _score_oi_unwinding(chain, analytics.resistance, analytics.support, reasons)
    position_score = _score_price_position(spot, analytics.resistance, analytics.support, reasons)

    total_score = int(
        _clamp(
            momentum_score + pcr_score + oi_change_score + max_pain_score + iv_skew_score + unwinding_score + position_score,
            -100,
            100,
        )
    )

    # Classify
    direction: Literal["BULLISH", "BEARISH", "NEUTRAL"] = (
        "BULLISH" if total_score > 10 else ("BEARISH" if total_score < -10 else "NEUTRAL")
    )
    abs_score = abs(total_score)
    confidence: Literal["STRONG", "MODERATE", "WEAK"] = (
        "STRONG" if abs_score > 45 else ("MODERATE" if abs_score > 20 else "WEAK")
    )

    # Options recommendation
    recommendation: Optional[SignalRecommendation] = None
    if direction != "NEUTRAL":
        recommendation = _select_strike(chain, spot, direction, strike_step, analytics.atm_iv)

    # Cash recommendation
    cash_recommendation: Optional[CashRecommendation] = None
    if direction != "NEUTRAL":
        cash_recommendation = _compute_cash_recommendation(
            spot, direction, analytics.support, analytics.resistance
        )

    return OISignal(
        symbol=symbol,
        category=category,
        direction=direction,
        confidence=confidence,
        score=total_score,
        recommendation=recommendation,
        cash_recommendation=cash_recommendation,
        analytics={
            "pcr": analytics.pcr,
            "pcr_sentiment": analytics.pcr_sentiment,
            "max_pain_strike": analytics.max_pain_strike,
            "max_pain_distance": analytics.max_pain_distance,
            "atm_iv": analytics.atm_iv,
            "spot": spot,
            "prev_close": round(prev_close, 2),
            "change_pct": round(change_pct, 2),
            "expiry": expiry,
        },
        score_breakdown=ScoreBreakdown(
            momentum=momentum_score,
            pcr=pcr_score,
            oi_change=oi_change_score,
            max_pain=max_pain_score,
            iv_skew=iv_skew_score,
            oi_unwinding=unwinding_score,
            price_position=position_score,
        ),
        reasons=reasons,
        timestamp=int(time.time() * 1000),
    )
