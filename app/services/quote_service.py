"""
Quote Service -- orchestrates the quote-batch flow.

Fetches live quotes from Kite in batches, enriches with cached data
(avg volume, PDH/PDL, delivery), computes buying/bear scores, and
tracks live signals (PDH cross, combo surge, ORB breakouts).

Ported from Alpha-Radar-backend/server.js ``/api/quotes-batch`` handler
(lines 774-910).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import asdict, dataclass
from typing import Any, Optional

from kiteconnect import KiteConnect

from app.caches import avg_volume_cache, pdh_pdl_cache, quote_cache
from app.engines.buying_score import BuyingScoreResult, compute_buying_score
from app.engines.bear_score import BearScoreResult, compute_bear_score
from app.engines.live_signal_tracker import LiveSignalTracker
from app.engines.orb_detector import ORBDetector

logger = logging.getLogger("alpha_radar.services.quote")

# ---------------------------------------------------------------------------
# Module-level stateful trackers (one instance per process, reset daily)
# ---------------------------------------------------------------------------
_signal_tracker = LiveSignalTracker()
_orb_detector = ORBDetector()


def get_signal_tracker() -> LiveSignalTracker:
    """Return the module-level LiveSignalTracker singleton."""
    return _signal_tracker


def get_orb_detector() -> ORBDetector:
    """Return the module-level ORBDetector singleton."""
    return _orb_detector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BATCH_SIZE = 250
_BATCH_DELAY_S = 0.4  # 400ms between batches


async def _fetch_quotes_batched(
    kite: KiteConnect,
    kite_symbols: list[str],
    batch_size: int = _BATCH_SIZE,
    delay: float = _BATCH_DELAY_S,
) -> dict[str, dict]:
    """
    Fetch quotes from Kite in batches, respecting rate limits.

    Parameters
    ----------
    kite : KiteConnect
        Authenticated Kite client.
    kite_symbols : list[str]
        Exchange-prefixed symbols, e.g. ``["NSE:RELIANCE", "NSE:TCS"]``.
    batch_size : int
        Maximum symbols per API call (Kite limit ~500, we use 250).
    delay : float
        Seconds to wait between successive batch calls.

    Returns
    -------
    dict
        Merged quote dict keyed by exchange-prefixed symbol.
    """
    all_quotes: dict[str, dict] = {}

    for i in range(0, len(kite_symbols), batch_size):
        batch = kite_symbols[i : i + batch_size]
        try:
            quotes = await asyncio.to_thread(kite.quote, batch)
            all_quotes.update(quotes)
        except Exception:
            logger.warning("Quote batch failed for symbols %d-%d", i, i + len(batch))

        if i + batch_size < len(kite_symbols):
            await asyncio.sleep(delay)

    return all_quotes


# ---------------------------------------------------------------------------
# Core: fetch_enriched_quotes
# ---------------------------------------------------------------------------


@dataclass
class EnrichedQuote:
    """Single enriched quote with scores and signals."""
    symbol: str
    ltp: float
    prev_close: float
    open: float
    high: float
    low: float
    change: float
    change_pct: float
    volume: int
    avg_volume: Optional[float]
    vol_ratio: float
    # Buy side
    buying_score: int
    score_breakdown: dict
    is_breakout: bool
    pdh: Optional[float]
    # Bear side
    bear_score: int
    bear_score_breakdown: dict
    is_pdl_break: bool
    pdl: Optional[float]
    # Extras
    delivery: Optional[float]
    orb: Optional[dict]
    signals: dict
    last_updated: int


async def fetch_enriched_quotes(
    kite: KiteConnect,
    symbols: list[str],
    delivery_data: Optional[dict[str, float]] = None,
) -> list[dict]:
    """
    Fetch live quotes and enrich with scores + signal tracking.

    Parameters
    ----------
    kite : KiteConnect
        Authenticated Kite client.
    symbols : list[str]
        Plain NSE symbols, e.g. ``["RELIANCE", "TCS", ...]``.
    delivery_data : dict, optional
        Symbol -> delivery percentage mapping.

    Returns
    -------
    list[dict]
        List of enriched quote dicts, one per successfully quoted symbol.
    """
    if not symbols:
        return []

    delivery_data = delivery_data or {}

    # 1. Fetch live quotes in batches ----------------------------------------
    kite_symbols = [f"NSE:{s}" for s in symbols]
    all_quotes = await _fetch_quotes_batched(kite, kite_symbols)

    # BSE fallback for symbols missing on NSE
    missing = [s for s in symbols if f"NSE:{s}" not in all_quotes]
    if missing:
        try:
            bse_symbols = [f"BSE:{s}" for s in missing]
            bse_quotes = await asyncio.to_thread(kite.quote, bse_symbols)
            for s in missing:
                bse_key = f"BSE:{s}"
                if bse_key in bse_quotes:
                    all_quotes[f"NSE:{s}"] = bse_quotes[bse_key]
        except Exception:
            pass  # BSE fallback is best-effort

    # 2. Enrich each symbol --------------------------------------------------
    enriched: list[dict] = []
    now_ms = int(time.time() * 1000)

    for symbol in symbols:
        q = all_quotes.get(f"NSE:{symbol}")
        if not q:
            continue

        try:
            ltp = q.get("last_price", 0.0)
            ohlc = q.get("ohlc", {})
            prev_close = ohlc.get("close", ltp)
            open_ = ohlc.get("open", ltp)
            high = ohlc.get("high", ltp)
            low = ohlc.get("low", ltp)
            volume = q.get("volume", 0)

            avg_vol = avg_volume_cache.get_avg_volume(symbol) or 0.0
            pdh = pdh_pdl_cache.get_pdh(symbol) or 0.0
            pdl = pdh_pdl_cache.get_pdl(symbol) or 0.0
            delivery = delivery_data.get(symbol)

            # Buying score
            buy_result: BuyingScoreResult = compute_buying_score(
                ltp=ltp,
                open=open_,
                high=high,
                low=low,
                close=prev_close,
                volume=volume,
                prev_close=prev_close,
                pdh=pdh,
                avg_volume=avg_vol,
                delivery_pct=delivery,
            )

            # Bear score
            bear_result: BearScoreResult = compute_bear_score(
                ltp=ltp,
                open=open_,
                high=high,
                low=low,
                close=prev_close,
                volume=volume,
                prev_close=prev_close,
                pdl=pdl,
                avg_volume=avg_vol,
                delivery_pct=delivery,
            )

            # Live signal tracking (PDH cross, combo surge, PDL cross)
            tick_signal = _signal_tracker.track_tick(
                symbol=symbol,
                ltp=ltp,
                volume=volume,
                avg_volume=avg_vol,
                pdh=pdh,
                pdl=pdl,
            )

            # ORB tracking
            orb_status = _orb_detector.check_orb_status(symbol, ltp)

            # Update quote cache
            quote_cache.set_quotes({symbol: q})

            enriched.append({
                "symbol": symbol,
                "ltp": round(ltp, 2),
                "prevClose": round(prev_close, 2),
                "open": round(open_, 2),
                "high": round(high, 2),
                "low": round(low, 2),
                "change": round(ltp - prev_close, 2),
                "changePct": buy_result.change_pct,
                "volume": volume,
                "avgVolume": buy_result.avg_volume,
                "volRatio": buy_result.vol_ratio,
                # Buy side
                "buyingScore": buy_result.buying_score,
                "scoreBreakdown": {
                    "vol": buy_result.vol_score,
                    "pdh": buy_result.pdh_score,
                    "momentum": buy_result.momentum_score,
                    "range": buy_result.range_pos_score,
                    "delivery": buy_result.delivery_score,
                },
                "isBreakout": buy_result.is_breakout,
                "pdh": buy_result.pdh,
                # Bear side
                "bearScore": bear_result.bear_score,
                "bearScoreBreakdown": {
                    "vol": bear_result.vol_score_bear,
                    "pdl": bear_result.pdl_score,
                    "momentum": bear_result.momentum_score_bear,
                    "range": bear_result.range_pos_score_bear,
                    "delivery": bear_result.delivery_score_bear,
                },
                "isPDLBreak": bear_result.is_pdl_break,
                "pdl": bear_result.pdl,
                # Extras
                "delivery": delivery,
                "orb": {
                    "high15": orb_status.orb_high_15,
                    "low15": orb_status.orb_low_15,
                    "high30": orb_status.orb_high_30,
                    "low30": orb_status.orb_low_30,
                    "nearOrb15": orb_status.near_orb15,
                    "orbBreak15": orb_status.orb_break15,
                    "nearOrb30": orb_status.near_orb30,
                    "orbBreak30": orb_status.orb_break30,
                    "orbBreakDown15": orb_status.orb_break_down15,
                    "orbBreakDown30": orb_status.orb_break_down30,
                } if orb_status.orb_high_15 or orb_status.orb_high_30 else None,
                "signals": {
                    "pdhCrossed": tick_signal.pdh_crossed,
                    "comboSurge": tick_signal.combo_surge,
                    "pdlCrossed": tick_signal.pdl_crossed,
                    "comboSell": tick_signal.combo_sell,
                    "firstCrossTime": tick_signal.first_cross_time,
                    "firstComboTime": tick_signal.first_combo_time,
                    "firstPDLTime": tick_signal.first_pdl_time,
                    "firstComboSellTime": tick_signal.first_combo_sell_time,
                },
                "lastUpdated": now_ms,
            })
        except Exception:
            logger.debug("Error enriching %s", symbol, exc_info=True)

    return enriched


# ---------------------------------------------------------------------------
# Top movers
# ---------------------------------------------------------------------------


def get_top_movers(quotes: list[dict], top_n: int = 8) -> dict:
    """
    Extract top gainers, losers, and strong buys from enriched quotes.

    Parameters
    ----------
    quotes : list[dict]
        Output of ``fetch_enriched_quotes``.
    top_n : int
        How many entries per category.

    Returns
    -------
    dict
        ``{gainers, losers, strongBuys}`` each a list of top_n items.
    """
    valid = [q for q in quotes if q.get("changePct") is not None]

    gainers = sorted(valid, key=lambda q: q["changePct"], reverse=True)[:top_n]
    losers = sorted(valid, key=lambda q: q["changePct"])[:top_n]
    strong_buys = sorted(valid, key=lambda q: q.get("buyingScore", 0), reverse=True)[:top_n]

    return {
        "gainers": gainers,
        "losers": losers,
        "strongBuys": strong_buys,
    }
