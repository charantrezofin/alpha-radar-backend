"""
Accumulation scanner -- detects PRE-MOVE institutional accumulation footprints.

Counterpart to ``institutional_buying.py`` (which detects POST-move patterns).
This engine looks for stocks that look like they're being quietly accumulated
BEFORE the breakout, not after it.

Five pillars (0-100 total):

  1. Volume Divergence       (35 pts)  Green-day volume >> red-day volume
                                       while price is roughly flat
  2. Range Contraction       (25 pts)  Recent 10-day range tightening relative
                                       to the prior 30-day range; tight bases
                                       precede breakouts
  3. Coiling Near Resistance (15 pts)  Price pinned within a few % of the recent
                                       high for many days
  4. Down-Day Resilience     (15 pts)  Red days get bought back within 1-2 days
  5. Higher Lows             (10 pts)  Second half of the window has higher lows
                                       than the first half (absorption)

Liquidity gate: 20-day avg daily turnover must be > Rs. 50 Cr (same as
post-move engine). Classification: 80-100 = 5-star, 65-79 = 4-star, 50-64 = 3-star.

Key difference from post-move engine: this engine REWARDS flat price action.
Stocks already up >8% in the look-back window are penalised because at that
point the move has likely started.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# Reuse the sector map from the post-move engine -- single source of truth.
from app.engines.institutional_buying import SECTOR_MAP, _avg_turnover_cr

WINDOW_DAYS = 20             # primary look-back for divergence + resilience + higher-lows
HIGH_LOOKBACK_DAYS = 60      # for resistance reference
CONTRACTION_REF_DAYS = 30    # older window to compare current 10-day range against


def _sector_of(symbol: str) -> str:
    return SECTOR_MAP.get(symbol.upper(), "Other")


# ── Dataclasses ─────────────────────────────────────────────────────────────
@dataclass
class AccumPillarScores:
    volume_divergence: int
    range_contraction: int
    coil_near_resistance: int
    resilience: int
    higher_lows: int

    @property
    def total(self) -> int:
        return (self.volume_divergence + self.range_contraction
                + self.coil_near_resistance + self.resilience + self.higher_lows)


@dataclass
class AccumDetails:
    green_red_vol_ratio: float
    price_drift_pct: float
    range_contraction_ratio: float
    range_pct_of_price: float
    distance_from_high_pct: float
    days_near_high: int
    red_day_recovery_rate: float
    higher_lows_pct: float


@dataclass
class AccumulationResult:
    symbol: str
    sector: str
    score: int
    stars: int
    current_price: float
    pillar_scores: AccumPillarScores
    details: AccumDetails
    avg_turnover_cr: float
    alert_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = self.details
        p = self.pillar_scores
        return {
            "symbol": self.symbol,
            "sector": self.sector,
            "score": self.score,
            "stars": self.stars,
            "current_price": round(self.current_price, 2),
            "pillar_scores": {
                "volume_divergence": p.volume_divergence,
                "range_contraction": p.range_contraction,
                "coil_near_resistance": p.coil_near_resistance,
                "resilience": p.resilience,
                "higher_lows": p.higher_lows,
            },
            "details": {
                "green_red_vol_ratio": round(d.green_red_vol_ratio, 2),
                "price_drift_pct": round(d.price_drift_pct, 2),
                "range_contraction_ratio": round(d.range_contraction_ratio, 2),
                "range_pct_of_price": round(d.range_pct_of_price, 2),
                "distance_from_high_pct": round(d.distance_from_high_pct, 2),
                "days_near_high": d.days_near_high,
                "red_day_recovery_rate": round(d.red_day_recovery_rate, 2),
                "higher_lows_pct": round(d.higher_lows_pct, 2),
            },
            "avg_turnover_cr": round(self.avg_turnover_cr, 2),
            "alert_flags": self.alert_flags,
        }


# ── Pillar 1: Volume Divergence ─────────────────────────────────────────────
def _score_volume_divergence(candles: List[Dict[str, float]]
                             ) -> Tuple[int, float, float]:
    """
    Asymmetric volume on green vs red days, scaled down if price is already
    trending. Returns (score, green_red_ratio, price_drift_pct).
    """
    if len(candles) < WINDOW_DAYS:
        return 0, 0.0, 0.0
    window = candles[-WINDOW_DAYS:]

    green_vols = [c["volume"] for c in window if c["close"] > c["open"]]
    red_vols = [c["volume"] for c in window if c["close"] < c["open"]]
    if len(green_vols) < 5 or len(red_vols) < 3:
        return 0, 0.0, 0.0

    avg_green = sum(green_vols) / len(green_vols)
    avg_red = sum(red_vols) / len(red_vols)
    if avg_red <= 0:
        return 0, 0.0, 0.0

    ratio = avg_green / avg_red

    # Price drift over the same window — flat = good, big drift = signal degraded
    open_first = window[0]["open"] or window[0]["close"]
    close_last = window[-1]["close"]
    drift_pct = ((close_last - open_first) / open_first * 100) if open_first > 0 else 0

    # Drift dampener: full credit between -3% and +5%, fading to 0 at +/- 10%
    if -3.0 <= drift_pct <= 5.0:
        drift_mult = 1.0
    elif drift_pct < -3.0:
        drift_mult = max(0.0, 1.0 - (abs(drift_pct) - 3.0) / 7.0)
    else:  # drift_pct > 5
        drift_mult = max(0.0, 1.0 - (drift_pct - 5.0) / 5.0)

    if ratio >= 2.0:
        base = 35
    elif ratio >= 1.5:
        base = 25
    elif ratio >= 1.2:
        base = 15
    elif ratio >= 1.0:
        base = 5
    else:
        base = 0

    return int(round(base * drift_mult)), ratio, drift_pct


# ── Pillar 2: Range Contraction ─────────────────────────────────────────────
def _score_range_contraction(candles: List[Dict[str, float]]
                             ) -> Tuple[int, float, float]:
    """
    Compare recent 10-day high-low range to the prior 30-day range.
    Tighter recent range relative to prior = base forming.
    Returns (score, contraction_ratio, range_pct_of_price).
    """
    if len(candles) < 40:
        return 0, 0.0, 0.0
    recent_10 = candles[-10:]
    prior_30 = candles[-40:-10]

    recent_range = max(c["high"] for c in recent_10) - min(c["low"] for c in recent_10)
    prior_range = max(c["high"] for c in prior_30) - min(c["low"] for c in prior_30)
    if prior_range <= 0:
        return 0, 0.0, 0.0

    ratio = recent_range / prior_range
    avg_close = sum(c["close"] for c in recent_10) / len(recent_10)
    range_pct = (recent_range / avg_close * 100) if avg_close > 0 else 100

    if ratio <= 0.4 and range_pct <= 4:
        return 25, ratio, range_pct
    if ratio <= 0.5 and range_pct <= 6:
        return 18, ratio, range_pct
    if ratio <= 0.6 and range_pct <= 8:
        return 10, ratio, range_pct
    return 0, ratio, range_pct


# ── Pillar 3: Coiling Near Resistance ───────────────────────────────────────
def _score_coil_near_resistance(candles: List[Dict[str, float]]
                                ) -> Tuple[int, float, int]:
    """
    Stock pinned within a few % of the recent high.
    Returns (score, distance_from_high_pct, days_near_high).
    """
    if len(candles) < HIGH_LOOKBACK_DAYS + 10:
        return 0, 0.0, 0
    # Resistance reference: highest high in the look-back excluding the
    # very recent 5 days (so we measure approach, not the high itself).
    ref_window = candles[-HIGH_LOOKBACK_DAYS:-5]
    if not ref_window:
        return 0, 0.0, 0
    recent_high = max(c["high"] for c in ref_window)
    if recent_high <= 0:
        return 0, 0.0, 0

    last_close = candles[-1]["close"]
    distance_pct = (recent_high - last_close) / recent_high * 100

    last_10 = candles[-10:]
    threshold = recent_high * 0.97
    days_near = sum(1 for c in last_10 if c["close"] >= threshold)

    if distance_pct <= 2.0 and days_near >= 5:
        return 15, distance_pct, days_near
    if distance_pct <= 4.0 and days_near >= 3:
        return 10, distance_pct, days_near
    if distance_pct <= 6.0 and days_near >= 2:
        return 5, distance_pct, days_near
    return 0, distance_pct, days_near


# ── Pillar 4: Down-Day Resilience ───────────────────────────────────────────
def _score_resilience(candles: List[Dict[str, float]]
                      ) -> Tuple[int, float]:
    """
    On red days in the window, did price recover within 1-2 days?
    Returns (score, recovery_rate).
    """
    if len(candles) < WINDOW_DAYS + 2:
        return 0, 0.0
    window = candles[-WINDOW_DAYS:]
    # Need 2 days of forward visibility, so consider only first WINDOW-2
    eligible = window[:-2]

    red_count = 0
    recovered = 0
    for i, c in enumerate(eligible):
        if c["close"] >= c["open"]:
            continue
        red_count += 1
        next1 = window[i + 1]
        next2 = window[i + 2]
        if next1["close"] > c["close"] or next2["close"] > c["close"]:
            recovered += 1

    if red_count < 4:
        return 0, 0.0
    rate = recovered / red_count
    if rate >= 0.75:
        return 15, rate
    if rate >= 0.60:
        return 10, rate
    if rate >= 0.50:
        return 5, rate
    return 0, rate


# ── Pillar 5: Higher Lows (absorption) ──────────────────────────────────────
def _score_higher_lows(candles: List[Dict[str, float]],
                       price_drift_pct: float
                       ) -> Tuple[int, float]:
    """
    Compare avg low of first half of window to second half. Rising lows
    on roughly flat price = institutions absorbing on dips.
    Returns (score, higher_lows_pct).
    """
    if len(candles) < WINDOW_DAYS:
        return 0, 0.0
    window = candles[-WINDOW_DAYS:]
    half = len(window) // 2
    first = window[:half]
    second = window[half:]

    avg_first = sum(c["low"] for c in first) / len(first)
    avg_second = sum(c["low"] for c in second) / len(second)
    if avg_first <= 0:
        return 0, 0.0
    diff_pct = (avg_second - avg_first) / avg_first * 100

    # Don't reward higher lows if price is trending hard (then it's just trend, not absorption)
    if price_drift_pct > 6.0:
        return 0, diff_pct

    if diff_pct >= 3.0:
        return 10, diff_pct
    if diff_pct >= 1.5:
        return 6, diff_pct
    if diff_pct >= 0.0:
        return 3, diff_pct
    return 0, diff_pct


# ── Classification ──────────────────────────────────────────────────────────
def _stars_for(score: int) -> int:
    if score >= 80:
        return 5
    if score >= 65:
        return 4
    if score >= 50:
        return 3
    return 0


# ── Public API ──────────────────────────────────────────────────────────────
def score_stock_accumulation(symbol: str,
                              candles: List[Dict[str, float]],
                              min_turnover_cr: float = 50.0,
                              ) -> Optional[Dict[str, Any]]:
    """
    Score a single stock for PRE-MOVE accumulation.

    ``candles`` is a list of dicts with keys: open, high, low, close, volume
    (oldest -> newest). At least ~70 bars recommended (we need 60 for
    resistance reference + 10 recent).

    Returns:
      - {"filtered": True, "reason": ...} if liquidity fails / insufficient data
      - None if the score is below 50 (don't display)
      - dict (from AccumulationResult.to_dict) with added filtered=False otherwise
    """
    if not candles or len(candles) < 70:
        return {"filtered": True, "reason": "insufficient_data", "symbol": symbol}

    turnover = _avg_turnover_cr(candles)
    if turnover < min_turnover_cr:
        return {"filtered": True, "reason": "low_liquidity",
                "symbol": symbol, "avg_turnover_cr": round(turnover, 2)}

    p_vol, ratio, drift_pct = _score_volume_divergence(candles)
    p_range, contraction, range_pct = _score_range_contraction(candles)
    p_coil, dist_high, days_near = _score_coil_near_resistance(candles)
    p_res, recovery_rate = _score_resilience(candles)
    p_hl, hl_pct = _score_higher_lows(candles, drift_pct)

    pillars = AccumPillarScores(
        volume_divergence=p_vol,
        range_contraction=p_range,
        coil_near_resistance=p_coil,
        resilience=p_res,
        higher_lows=p_hl,
    )
    total = pillars.total
    if total < 50:
        return None

    flags: List[str] = []
    if ratio >= 2.0:
        flags.append("VOL_DIV_2X")
    elif ratio >= 1.5:
        flags.append("VOL_DIV_1.5X")
    if contraction <= 0.4 and range_pct <= 4:
        flags.append("TIGHT_BASE")
    if dist_high <= 2.0 and days_near >= 5:
        flags.append("AT_RESISTANCE")
    if recovery_rate >= 0.75:
        flags.append("RESILIENT")
    if abs(drift_pct) <= 3:
        flags.append("FLAT_PRICE")  # signal that this is genuinely pre-move

    result = AccumulationResult(
        symbol=symbol,
        sector=_sector_of(symbol),
        score=total,
        stars=_stars_for(total),
        current_price=candles[-1]["close"],
        pillar_scores=pillars,
        details=AccumDetails(
            green_red_vol_ratio=ratio,
            price_drift_pct=drift_pct,
            range_contraction_ratio=contraction,
            range_pct_of_price=range_pct,
            distance_from_high_pct=dist_high,
            days_near_high=days_near,
            red_day_recovery_rate=recovery_rate,
            higher_lows_pct=hl_pct,
        ),
        avg_turnover_cr=turnover,
        alert_flags=flags,
    )
    out = result.to_dict()
    out["filtered"] = False
    return out


def build_accumulation_clusters(results: List[Dict[str, Any]],
                                min_stocks: int = 3
                                ) -> List[Dict[str, Any]]:
    """
    Group accumulation results by sector. Lower min_stocks (3) than post-move
    engine because pre-move is a rarer signal — even 3 stocks coiling in the
    same sector is meaningful.
    """
    by_sector: Dict[str, List[Dict[str, Any]]] = {}
    for r in results:
        if r.get("filtered"):
            continue
        if r.get("score", 0) < 50:
            continue
        by_sector.setdefault(r["sector"], []).append(r)

    clusters: List[Dict[str, Any]] = []
    for sector, stocks in by_sector.items():
        if len(stocks) >= min_stocks:
            stocks_sorted = sorted(stocks, key=lambda s: s["score"], reverse=True)
            clusters.append({
                "sector": sector,
                "count": len(stocks),
                "avg_score": round(sum(s["score"] for s in stocks) / len(stocks), 1),
                "symbols": [s["symbol"] for s in stocks_sorted],
            })
    clusters.sort(key=lambda c: (c["count"], c["avg_score"]), reverse=True)
    return clusters
