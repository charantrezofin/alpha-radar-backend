"""
Institutional Buying Scanner -- detects multi-day accumulation footprints
in the F&O universe using 6 months of daily OHLCV data.

Scoring (0-100, 5 pillars):
  1. Back-to-Back Buying      (30 pts)  Consecutive green-day count
  2. Volume Surge Quality     (25 pts)  Multi-day avg volume ratio + confirmation
  3. Move Magnitude & Speed   (20 pts)  Size of the low->high leg and how fast
  4. Candle Quality           (15 pts)  Avg close-within-range on up days
  5. Base Quality After Move  (10 pts)  Retracement / consolidation tightness

Liquidity gate: 20-day avg daily turnover must be > Rs. 50 Crore.

Classification:
  80-100  -> 5-star
  65-79   -> 4-star
  50-64   -> 3-star
  <50     -> filtered out

Sector cluster alert: 4+ stocks from the same sector with score >= 50.

This engine is pure data -> data; the route in app/routes/institutional.py
is responsible for fetching the 6-month daily candles from Kite and feeding
them in.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# ── Sector map for the F&O universe ─────────────────────────────────────────
# Best-effort mapping; symbols not listed fall into "Other".
SECTOR_MAP: Dict[str, str] = {
    # Banks
    "HDFCBANK": "Banks", "ICICIBANK": "Banks", "SBIN": "Banks", "AXISBANK": "Banks",
    "KOTAKBANK": "Banks", "INDUSINDBK": "Banks", "BANKBARODA": "Banks",
    "PNB": "Banks", "FEDERALBNK": "Banks", "IDFCFIRSTB": "Banks", "AUBANK": "Banks",
    "BANDHANBNK": "Banks", "RBLBANK": "Banks", "CANBK": "Banks",
    # NBFC / Finance
    "BAJFINANCE": "NBFC", "BAJAJFINSV": "NBFC", "CHOLAFIN": "NBFC",
    "SBICARD": "NBFC", "MUTHOOTFIN": "NBFC", "MANAPPURAM": "NBFC",
    "LICHSGFIN": "NBFC", "PFC": "NBFC", "RECLTD": "NBFC", "IIFL": "NBFC",
    "SHRIRAMFIN": "NBFC", "POONAWALLA": "NBFC",
    # IT
    "TCS": "IT", "INFY": "IT", "WIPRO": "IT", "HCLTECH": "IT",
    "TECHM": "IT", "LTIM": "IT", "MPHASIS": "IT", "PERSISTENT": "IT",
    "COFORGE": "IT", "LTTS": "IT",
    # Auto
    "MARUTI": "Auto", "TATAMOTORS": "Auto", "M&M": "Auto", "BAJAJ-AUTO": "Auto",
    "HEROMOTOCO": "Auto", "EICHERMOT": "Auto", "TVSMOTOR": "Auto",
    "ASHOKLEY": "Auto", "MOTHERSON": "Auto", "BOSCHLTD": "Auto",
    "BALKRISIND": "Auto", "MRF": "Auto", "APOLLOTYRE": "Auto", "EXIDEIND": "Auto",
    # Pharma
    "SUNPHARMA": "Pharma", "DRREDDY": "Pharma", "CIPLA": "Pharma",
    "DIVISLAB": "Pharma", "LUPIN": "Pharma", "AUROPHARMA": "Pharma",
    "TORNTPHARM": "Pharma", "ALKEM": "Pharma", "BIOCON": "Pharma",
    "LAURUSLABS": "Pharma", "ZYDUSLIFE": "Pharma", "GLENMARK": "Pharma",
    "ABBOTINDIA": "Pharma", "SYNGENE": "Pharma", "IPCALAB": "Pharma",
    # FMCG
    "HINDUNILVR": "FMCG", "ITC": "FMCG", "NESTLEIND": "FMCG",
    "BRITANNIA": "FMCG", "DABUR": "FMCG", "MARICO": "FMCG",
    "GODREJCP": "FMCG", "COLPAL": "FMCG", "TATACONSUM": "FMCG",
    "UBL": "FMCG", "UNITDSPR": "FMCG", "VBL": "FMCG",
    # Metals
    "TATASTEEL": "Metals", "JSWSTEEL": "Metals", "HINDALCO": "Metals",
    "VEDL": "Metals", "SAIL": "Metals", "JINDALSTEL": "Metals",
    "NMDC": "Metals", "HINDCOPPER": "Metals", "NATIONALUM": "Metals",
    "APLAPOLLO": "Metals", "RATNAMANI": "Metals",
    # Energy / Oil & Gas
    "RELIANCE": "Energy", "ONGC": "Energy", "IOC": "Energy",
    "BPCL": "Energy", "HPCL": "Energy", "GAIL": "Energy", "OIL": "Energy",
    "PETRONET": "Gas", "IGL": "Gas", "GUJGASLTD": "Gas", "MGL": "Gas",
    # Power / Utilities
    "NTPC": "Power", "POWERGRID": "Power", "TATAPOWER": "Power",
    "ADANIPOWER": "Power", "JSWENERGY": "Power", "TORNTPOWER": "Power",
    "NHPC": "Power", "SJVN": "Power",
    # Cement
    "ULTRACEMCO": "Cement", "SHREECEM": "Cement", "GRASIM": "Cement",
    "AMBUJACEM": "Cement", "ACC": "Cement", "DALBHARAT": "Cement",
    "RAMCOCEM": "Cement", "JKCEMENT": "Cement",
    # Telecom
    "BHARTIARTL": "Telecom", "IDEA": "Telecom", "INDUSTOWER": "Telecom",
    # Chemicals
    "PIDILITIND": "Chemicals", "SRF": "Chemicals", "UPL": "Chemicals",
    "AARTIIND": "Chemicals", "DEEPAKNTR": "Chemicals", "PIIND": "Chemicals",
    "TATACHEM": "Chemicals", "NAVINFLUOR": "Chemicals", "ATUL": "Chemicals",
    "COROMANDEL": "Chemicals", "BALRAMCHIN": "Chemicals", "CHAMBLFERT": "Chemicals",
    # Infra / Capital Goods
    "LT": "Infra", "SIEMENS": "Infra", "ABB": "Infra", "BEL": "Defence",
    "HAL": "Defence", "BHEL": "Defence", "MAZDOCK": "Defence",
    "CUMMINSIND": "Infra", "HAVELLS": "Infra", "POLYCAB": "Infra",
    "KEI": "Infra", "ASTRAL": "Infra", "BLUESTARCO": "Infra",
    "GMRAIRPORT": "Infra", "GMRP&UI": "Infra", "IRB": "Infra",
    # Realty
    "DLF": "Realty", "GODREJPROP": "Realty", "OBEROIRLTY": "Realty",
    "PRESTIGE": "Realty", "LODHA": "Realty", "BRIGADE": "Realty", "PHOENIXLTD": "Realty",
    # Consumer / Retail
    "TITAN": "Consumer", "TRENT": "Consumer", "PAGEIND": "Consumer",
    "DMART": "Consumer", "VOLTAS": "Consumer", "DIXON": "Consumer",
    "BATAINDIA": "Consumer", "RELAXO": "Consumer", "JUBLFOOD": "Consumer",
    # Internet / New-age
    "ZOMATO": "Internet", "PAYTM": "Internet", "NYKAA": "Internet",
    "POLICYBZR": "Internet", "IRCTC": "Internet", "DELHIVERY": "Internet",
    # Insurance
    "SBILIFE": "Insurance", "HDFCLIFE": "Insurance", "ICICIPRULI": "Insurance",
    "ICICIGI": "Insurance", "LICI": "Insurance", "MAXHEALTH": "Healthcare",
    "APOLLOHOSP": "Healthcare",
    # Diversified / Conglomerate / Other
    "ADANIENT": "Conglomerate", "ADANIPORTS": "Ports", "ADANIGREEN": "Power",
    "HDFCAMC": "AMC", "NAM-INDIA": "AMC", "MCX": "Financials",
    "BSE": "Financials", "CDSL": "Financials", "CAMS": "Financials",
    "CONCOR": "Logistics", "GUJFLUORO": "Chemicals",
    "INDIGO": "Aviation",
}


def _sector_of(symbol: str) -> str:
    return SECTOR_MAP.get(symbol.upper(), "Other")


# ── Dataclasses ─────────────────────────────────────────────────────────────
@dataclass
class PillarScores:
    back_to_back: int
    volume_surge: int
    magnitude_speed: int
    candle_quality: int
    base_quality: int

    @property
    def total(self) -> int:
        return (self.back_to_back + self.volume_surge + self.magnitude_speed
                + self.candle_quality + self.base_quality)


@dataclass
class MoveDetails:
    move_start_idx: int
    move_end_idx: int
    move_low: float
    move_high: float
    move_pct: float
    days_taken: int
    consecutive_green: int
    avg_vol_ratio_during_move: float


@dataclass
class InstitutionalResult:
    symbol: str
    sector: str
    score: int
    stars: int
    current_price: float
    pillar_scores: PillarScores
    move_details: MoveDetails
    avg_turnover_cr: float
    alert_flags: List[str] = field(default_factory=list)
    base_retrace_pct: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        m = self.move_details
        p = self.pillar_scores
        return {
            "symbol": self.symbol,
            "sector": self.sector,
            "score": self.score,
            "stars": self.stars,
            "current_price": round(self.current_price, 2),
            "pillar_scores": {
                "back_to_back": p.back_to_back,
                "volume_surge": p.volume_surge,
                "magnitude_speed": p.magnitude_speed,
                "candle_quality": p.candle_quality,
                "base_quality": p.base_quality,
            },
            "move_details": {
                "move_low": round(m.move_low, 2),
                "move_high": round(m.move_high, 2),
                "move_pct": round(m.move_pct, 2),
                "days_taken": m.days_taken,
                "consecutive_green": m.consecutive_green,
                "avg_vol_ratio_during_move": round(m.avg_vol_ratio_during_move, 2),
            },
            "avg_turnover_cr": round(self.avg_turnover_cr, 2),
            "base_retrace_pct": round(self.base_retrace_pct, 2),
            "alert_flags": self.alert_flags,
        }


# ── Move detection ──────────────────────────────────────────────────────────
def _find_recent_up_move(candles: List[Dict[str, float]],
                         lookback: int = 60,
                         min_move_pct: float = 10.0
                         ) -> Optional[Tuple[int, int, float, float]]:
    """
    Find the most recent low -> high leg in the last ``lookback`` candles.
    Returns (low_idx, high_idx, low_price, high_price) or None.
    """
    if len(candles) < 10:
        return None
    window = candles[-lookback:] if len(candles) > lookback else candles
    offset = len(candles) - len(window)

    # Find the highest high in the window
    high_local_idx = 0
    for i, c in enumerate(window):
        if c["high"] > window[high_local_idx]["high"]:
            high_local_idx = i

    # Find the lowest low BEFORE the high, within the window
    if high_local_idx == 0:
        return None
    low_local_idx = 0
    for i in range(0, high_local_idx):
        if window[i]["low"] < window[low_local_idx]["low"]:
            low_local_idx = i

    low_price = window[low_local_idx]["low"]
    high_price = window[high_local_idx]["high"]
    if low_price <= 0:
        return None
    move_pct = (high_price - low_price) / low_price * 100
    if move_pct < min_move_pct:
        return None

    return (offset + low_local_idx, offset + high_local_idx, low_price, high_price)


def _count_consecutive_green(candles: List[Dict[str, float]], end_idx: int) -> int:
    """
    Walk backwards from end_idx counting green days (close>open).
    Allow one red day with body <= 1% of close as interruption (does not reset,
    but does not increment either).
    """
    count = 0
    i = end_idx
    tolerated = 0
    while i >= 0:
        c = candles[i]
        if c["close"] > c["open"]:
            count += 1
            i -= 1
            continue
        # red day
        body_pct = abs(c["close"] - c["open"]) / c["close"] * 100 if c["close"] > 0 else 100
        if body_pct <= 1.0 and tolerated < 1 and count >= 2:
            tolerated += 1
            i -= 1
            continue
        break
    return count


# ── Pillar scoring ──────────────────────────────────────────────────────────
def _score_back_to_back(consecutive_green: int) -> int:
    if consecutive_green >= 5:
        return 30
    if consecutive_green == 4:
        return 20
    if consecutive_green == 3:
        return 10
    return 0


def _score_volume_surge(candles: List[Dict[str, float]],
                        move_start: int, move_end: int) -> Tuple[int, float, int]:
    """
    Volume ratio of move window vs 20-day avg preceding the move.
    Returns (score, avg_ratio_during_move, days_with_high_volume).
    """
    if move_start < 20:
        return 0, 0.0, 0
    prev_20 = candles[move_start - 20: move_start]
    if not prev_20:
        return 0, 0.0, 0
    avg_prev_vol = sum(c["volume"] for c in prev_20) / len(prev_20)
    if avg_prev_vol <= 0:
        return 0, 0.0, 0

    move_slice = candles[move_start: move_end + 1]
    if not move_slice:
        return 0, 0.0, 0
    ratios = [c["volume"] / avg_prev_vol for c in move_slice]
    avg_ratio = sum(ratios) / len(ratios)
    high_vol_days = sum(1 for r in ratios if r >= 3.0)

    if avg_ratio >= 8:
        pts = 25
    elif avg_ratio >= 5:
        pts = 18
    elif avg_ratio >= 3:
        pts = 10
    else:
        pts = 0

    if high_vol_days >= 3 and pts > 0:
        pts = min(25, pts + 5)

    return pts, avg_ratio, high_vol_days


def _score_magnitude_speed(move_pct: float, days_taken: int) -> int:
    if move_pct >= 50:
        base = 20
    elif move_pct >= 30:
        base = 14
    elif move_pct >= 20:
        base = 8
    else:
        base = 0

    if base == 0:
        return 0
    if days_taken < 7:
        base = min(20, base + 3)
    elif days_taken > 20:
        base = max(0, base - 5)
    return base


def _score_candle_quality(candles: List[Dict[str, float]],
                          move_start: int, move_end: int) -> int:
    move_slice = [c for c in candles[move_start: move_end + 1]
                  if c["close"] > c["open"]]
    if not move_slice:
        return 0
    strengths: List[float] = []
    for c in move_slice:
        rng = c["high"] - c["low"]
        if rng <= 0:
            continue
        strengths.append((c["close"] - c["low"]) / rng)
    if not strengths:
        return 0
    avg = sum(strengths) / len(strengths)
    if avg >= 0.80:
        return 15
    if avg >= 0.65:
        return 10
    if avg >= 0.50:
        return 5
    return 0


def _score_base_quality(candles: List[Dict[str, float]],
                        move_end: int, move_low: float, move_high: float
                        ) -> Tuple[int, float, bool]:
    """
    After the move, how tight is the consolidation? Look at candles after
    move_end (up to 10 bars). Retrace = (move_high - lowest_post_low)/
    (move_high - move_low).
    Returns (score, retrace_pct, in_top_quarter).
    """
    post = candles[move_end + 1: move_end + 11]
    if not post:
        # Still at the high -> strong
        return 10, 0.0, True
    lowest_post = min(c["low"] for c in post)
    move_range = max(1e-9, move_high - move_low)
    retrace = (move_high - lowest_post) / move_range * 100

    if retrace < 10:
        pts = 10
    elif retrace < 25:
        pts = 7
    elif retrace < 40:
        pts = 3
    else:
        pts = 0

    last_close = candles[-1]["close"]
    top_quarter_threshold = move_high - 0.25 * move_range
    in_top_quarter = last_close >= top_quarter_threshold
    if in_top_quarter and pts > 0:
        pts = min(10, pts + 2)

    return pts, retrace, in_top_quarter


# ── Liquidity ───────────────────────────────────────────────────────────────
def _avg_turnover_cr(candles: List[Dict[str, float]], window: int = 20) -> float:
    if len(candles) < window:
        return 0.0
    last = candles[-window:]
    total = sum(c["close"] * c["volume"] for c in last)
    return total / window / 1e7  # Rs. Cr


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
def score_stock(symbol: str,
                candles: List[Dict[str, float]],
                min_turnover_cr: float = 50.0,
                ) -> Optional[Dict[str, Any]]:
    """
    Score a single stock's 6-month daily candle series.

    ``candles`` is a list of dicts with keys: open, high, low, close, volume
    (oldest -> newest). At least ~60 bars recommended.

    Returns:
      - {"filtered": True, "reason": ...} if liquidity fails,
      - None if the score is below 50 (don't display),
      - dict (from InstitutionalResult.to_dict) with added filtered=False otherwise.
    """
    if not candles or len(candles) < 30:
        return {"filtered": True, "reason": "insufficient_data", "symbol": symbol}

    turnover = _avg_turnover_cr(candles)
    if turnover < min_turnover_cr:
        return {"filtered": True, "reason": "low_liquidity",
                "symbol": symbol, "avg_turnover_cr": round(turnover, 2)}

    move = _find_recent_up_move(candles)
    if move is None:
        return None
    low_idx, high_idx, low_price, high_price = move
    days_taken = high_idx - low_idx
    move_pct = (high_price - low_price) / low_price * 100

    consecutive_green = _count_consecutive_green(candles, high_idx)

    p_b2b = _score_back_to_back(consecutive_green)
    p_vol, avg_vol_ratio, high_vol_days = _score_volume_surge(candles, low_idx, high_idx)
    p_mag = _score_magnitude_speed(move_pct, days_taken)
    p_cndl = _score_candle_quality(candles, low_idx, high_idx)
    p_base, retrace_pct, in_top_q = _score_base_quality(candles, high_idx, low_price, high_price)

    pillars = PillarScores(
        back_to_back=p_b2b,
        volume_surge=p_vol,
        magnitude_speed=p_mag,
        candle_quality=p_cndl,
        base_quality=p_base,
    )
    total = pillars.total
    if total < 50:
        return None

    flags: List[str] = []
    if avg_vol_ratio >= 10:
        flags.append("VOLUME_10X")
    elif avg_vol_ratio >= 5:
        flags.append("VOLUME_5X")
    if retrace_pct < 10:
        flags.append("TIGHT_BASE")
    if in_top_q:
        flags.append("TOP_QUARTER")
    if days_taken < 7 and move_pct >= 20:
        flags.append("FAST_MOVE")

    result = InstitutionalResult(
        symbol=symbol,
        sector=_sector_of(symbol),
        score=total,
        stars=_stars_for(total),
        current_price=candles[-1]["close"],
        pillar_scores=pillars,
        move_details=MoveDetails(
            move_start_idx=low_idx,
            move_end_idx=high_idx,
            move_low=low_price,
            move_high=high_price,
            move_pct=move_pct,
            days_taken=days_taken,
            consecutive_green=consecutive_green,
            avg_vol_ratio_during_move=avg_vol_ratio,
        ),
        avg_turnover_cr=turnover,
        alert_flags=flags,
        base_retrace_pct=retrace_pct,
    )
    out = result.to_dict()
    out["filtered"] = False
    return out


def build_sector_clusters(results: List[Dict[str, Any]],
                          min_stocks: int = 4
                          ) -> List[Dict[str, Any]]:
    """Group results by sector; return sectors with >= min_stocks entries."""
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
