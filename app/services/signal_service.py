"""
Signal Service -- orchestrates the 2-phase OI signal system.

Phase 1 (Pre-screen): Batch-fetch all F&O futures + spot quotes,
                       compute futures pre-screen score, filter top movers.
Phase 2 (Deep analysis): For top stocks, build options chain and compute
                          full OI signal with futures boost.

Ported from tradingdesk/apps/gateway/src/routes/signals.routes.ts.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Literal, Optional

from kiteconnect import KiteConnect

from app.caches import instrument_cache
from app.data.index_config import INDEX_CONFIG, get_all_index_keys
from app.data.stock_universes import FNO_STOCKS
from app.engines.futures_prescreen import FuturesPreScreen, prescreen_stock
from app.engines.oi_signal import (
    ChainAnalytics,
    ChainRow,
    OISignal,
    compute_oi_signal,
)
from app.services.options_service import get_options_chain, get_stock_options_chain

logger = logging.getLogger("alpha_radar.services.signal")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_FUTURES_BATCH_SIZE = 200
_FUTURES_BATCH_DELAY_S = 0.35   # 350ms between futures batches
_DEEP_BATCH_SIZE = 2
_DEEP_BATCH_DELAY_S = 0.8       # 800ms between deep analysis batches
_INDEX_DELAY_S = 0.5             # 500ms between index signal calls
_PRESCREEN_MIN_SCORE = 10        # |preScore| >= 10 to qualify for deep analysis

# Index names to exclude from stock futures
_INDEX_NAMES = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _fetch_quotes_batched(
    kite: KiteConnect,
    symbols: list[str],
    batch_size: int,
    delay: float,
) -> dict[str, dict]:
    """Fetch Kite quotes in batches with rate-limit pauses."""
    all_quotes: dict[str, dict] = {}
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        try:
            quotes = await asyncio.to_thread(kite.quote, batch)
            all_quotes.update(quotes)
        except Exception:
            logger.warning("Signal quote batch failed (%d-%d)", i, i + len(batch))
        if i + batch_size < len(symbols):
            await asyncio.sleep(delay)
    return all_quotes


# ---------------------------------------------------------------------------
# Index Signals
# ---------------------------------------------------------------------------


async def get_index_signals(kite: KiteConnect) -> list[dict]:
    """
    Compute OI signals for all configured indices.

    Processes indices sequentially with a small delay between each
    to avoid overwhelming the Kite API.

    Parameters
    ----------
    kite : KiteConnect
        Authenticated Kite client.

    Returns
    -------
    list[dict]
        OISignal results (as dicts) sorted by ``|score|`` descending.
    """
    signals: list[OISignal] = []

    for key in get_all_index_keys():
        try:
            data = await get_options_chain(kite, key)

            # Build ChainAnalytics from the analytics dict
            analytics = _dict_to_chain_analytics(data["analytics"])

            signal = compute_oi_signal(
                symbol=data["name"],
                category="index",
                spot=data["spot"],
                prev_close=data["prevClose"],
                expiry=data["expiry"],
                strike_step=data["strikeStep"],
                chain=data["chain"],
                analytics=analytics,
            )
            signals.append(signal)
        except Exception as exc:
            logger.error("Index %s signal failed: %s", key, exc)

        await asyncio.sleep(_INDEX_DELAY_S)

    signals.sort(key=lambda s: abs(s.score), reverse=True)
    return [_oi_signal_to_dict(s) for s in signals]


# ---------------------------------------------------------------------------
# Stock Signals (2-Phase)
# ---------------------------------------------------------------------------


async def get_stock_signals(
    kite: KiteConnect,
    deep_count: int = 20,
) -> dict:
    """
    2-phase stock signal generation.

    Phase 1 -- Futures pre-screening:
        Fetch all F&O futures (nearest month) and spot quotes in bulk.
        Compute pre-screen score for each. Filter by |preScore| >= threshold.

    Phase 2 -- Deep analysis:
        For top ``deep_count`` stocks, build options chain and compute
        full OI signal. Boost score by 30% of pre-screen score.

    Parameters
    ----------
    kite : KiteConnect
        Authenticated Kite client.
    deep_count : int
        Maximum number of stocks for deep analysis (default 20, max 50).

    Returns
    -------
    dict
        ``{signals, preScreened, summary, preScreenSummary}``
    """
    deep_count = min(deep_count, 50)

    # ── Phase 1: Futures pre-screening ──────────────────────────────────────
    logger.info("Phase 1: Pre-screening F&O stocks...")

    instruments = instrument_cache.get_nfo_instruments()
    now = datetime.now()

    # Find nearest-month futures for each stock
    futures = [
        i for i in instruments
        if i.get("instrument_type") == "FUT"
        and _parse_expiry_safe(i.get("expiry")) is not None
        and _parse_expiry_safe(i.get("expiry")) > now
    ]
    futures.sort(key=lambda i: _parse_expiry_safe(i.get("expiry")) or now)

    nearest_fut: dict[str, dict] = {}
    for f in futures:
        name = f.get("name", "")
        if name and name not in nearest_fut:
            nearest_fut[name] = f

    # Exclude indices
    stock_futures = {
        name: inst
        for name, inst in nearest_fut.items()
        if name not in _INDEX_NAMES
    }

    # Batch-fetch ALL futures + spot quotes
    fut_symbols = [f"NFO:{inst['tradingsymbol']}" for inst in stock_futures.values()]
    spot_symbols = [f"NSE:{name}" for name in stock_futures]
    all_symbols = fut_symbols + spot_symbols

    all_quotes = await _fetch_quotes_batched(
        kite, all_symbols, _FUTURES_BATCH_SIZE, _FUTURES_BATCH_DELAY_S
    )

    # Pre-screen each stock
    pre_screened: list[dict] = []
    for name, inst in stock_futures.items():
        try:
            fut_quote = all_quotes.get(f"NFO:{inst['tradingsymbol']}", {})
            spot_quote = all_quotes.get(f"NSE:{name}", {})

            spot_price = spot_quote.get("last_price") or fut_quote.get("last_price", 0)
            spot_prev_close = spot_quote.get("ohlc", {}).get("close", 0)

            fut_last = fut_quote.get("last_price", 0)
            fut_close = fut_quote.get("ohlc", {}).get("close", fut_last)
            change_pct = ((fut_last - fut_close) / fut_close * 100) if fut_close > 0 else 0

            oi = fut_quote.get("oi", 0)
            oi_low = fut_quote.get("oi_day_low", oi)
            oi_change = oi - oi_low
            volume = fut_quote.get("volume", 0)

            if oi <= 0:
                continue

            result = prescreen_stock(
                name=name,
                spot=spot_price,
                fut_price=fut_last,
                change_pct=change_pct,
                oi=oi,
                oi_change=oi_change,
                volume=volume,
            )

            pre_screened.append({
                "result": result,
                "spot": spot_price,
                "spotPrevClose": spot_prev_close,
            })
        except Exception:
            logger.debug("Pre-screen failed for %s", name, exc_info=True)

    # Sort by |preScore| descending
    pre_screened.sort(key=lambda x: abs(x["result"].pre_score), reverse=True)
    logger.info("Phase 1 done: %d stocks pre-screened", len(pre_screened))

    # ── Phase 2: Deep analysis ──────────────────────────────────────────────
    top_stocks = [
        s for s in pre_screened
        if abs(s["result"].pre_score) >= _PRESCREEN_MIN_SCORE
    ][:deep_count]

    logger.info("Phase 2: Deep analysis for %d stocks...", len(top_stocks))

    deep_signals: list[OISignal] = []

    for i in range(0, len(top_stocks), _DEEP_BATCH_SIZE):
        batch = top_stocks[i : i + _DEEP_BATCH_SIZE]

        tasks = [
            _deep_analyze_stock(
                kite=kite,
                stock_name=s["result"].name,
                spot=s["spot"],
                prev_close=s["spotPrevClose"],
                pre_score=s["result"].pre_score,
                pre_reasons=s["result"].pre_reasons,
            )
            for s in batch
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, OISignal):
                deep_signals.append(r)
            elif isinstance(r, Exception):
                logger.debug("Deep analysis failed: %s", r)

        if i + _DEEP_BATCH_SIZE < len(top_stocks):
            await asyncio.sleep(_DEEP_BATCH_DELAY_S)

    deep_signals.sort(key=lambda s: abs(s.score), reverse=True)
    logger.info("Phase 2 done: %d deep signals generated", len(deep_signals))

    # Build response
    signals_out = [_oi_signal_to_dict(s) for s in deep_signals]
    pre_screen_out = [_prescreen_to_dict(s["result"]) for s in pre_screened]

    summary = _build_summary(deep_signals)
    pre_screen_summary = {
        "total": len(pre_screened),
        "bullish": sum(1 for s in pre_screened if s["result"].pre_direction == "BULLISH"),
        "bearish": sum(1 for s in pre_screened if s["result"].pre_direction == "BEARISH"),
        "deepAnalyzed": len(deep_signals),
    }

    return {
        "signals": signals_out,
        "preScreened": pre_screen_out,
        "summary": summary,
        "preScreenSummary": pre_screen_summary,
    }


# ---------------------------------------------------------------------------
# Combined signal dispatch
# ---------------------------------------------------------------------------


async def get_all_signals(
    kite: KiteConnect,
    category: str = "all",
    deep_count: int = 20,
) -> dict:
    """
    Dispatch to index and/or stock signals based on category.

    Parameters
    ----------
    kite : KiteConnect
        Authenticated Kite client.
    category : str
        ``"indices"``, ``"stocks"``, or ``"all"`` (default).
    deep_count : int
        Max stocks for deep analysis.

    Returns
    -------
    dict
        ``{success, signals, preScreened, summary, preScreenSummary, timestamp}``
    """
    cat = category.lower().strip()

    if cat == "indices":
        signals = await get_index_signals(kite)
        return {
            "success": True,
            "signals": signals,
            "summary": _build_summary_from_dicts(signals),
            "timestamp": int(time.time() * 1000),
        }

    if cat == "stocks":
        result = await get_stock_signals(kite, deep_count)
        return {
            "success": True,
            **result,
            "timestamp": int(time.time() * 1000),
        }

    # Default: all -- fetch indices first (fast), then stocks
    index_signals = await get_index_signals(kite)
    stock_result = await get_stock_signals(kite, deep_count)

    all_signals = index_signals + stock_result["signals"]
    all_signals.sort(key=lambda s: abs(s.get("score", 0)), reverse=True)

    return {
        "success": True,
        "signals": all_signals,
        "preScreened": stock_result.get("preScreened", []),
        "summary": _build_summary_from_dicts(all_signals),
        "preScreenSummary": stock_result.get("preScreenSummary", {}),
        "timestamp": int(time.time() * 1000),
    }


# ---------------------------------------------------------------------------
# Deep analysis for a single stock
# ---------------------------------------------------------------------------


async def _deep_analyze_stock(
    kite: KiteConnect,
    stock_name: str,
    spot: float,
    prev_close: float,
    pre_score: int,
    pre_reasons: list[str],
) -> OISignal:
    """
    Perform deep OI analysis on a single stock: build options chain,
    compute OI signal, and boost with futures pre-score.
    """
    chain_data = await get_stock_options_chain(kite, stock_name)

    chain: list[ChainRow] = chain_data["chain"]
    if len(chain) < 3:
        raise ValueError(f"Too few strikes for {stock_name}")

    analytics = _dict_to_chain_analytics(chain_data["analytics"])

    signal = compute_oi_signal(
        symbol=stock_name,
        category="stock",
        spot=spot,
        prev_close=prev_close,
        expiry=chain_data["expiry"],
        strike_step=chain_data["strikeStep"],
        chain=chain,
        analytics=analytics,
    )

    # Blend futures pre-score for conviction boost (30%)
    fut_boost = round(pre_score * 0.3)
    boosted_score = max(-100, min(100, signal.score + fut_boost))

    # Merge pre-screen reasons
    for r in pre_reasons:
        if r not in signal.reasons:
            signal.reasons.append(r)

    # Re-classify after boost
    signal.score = boosted_score
    signal.direction = (
        "BULLISH" if boosted_score > 15
        else ("BEARISH" if boosted_score < -15 else "NEUTRAL")
    )
    signal.confidence = (
        "STRONG" if abs(boosted_score) > 50
        else ("MODERATE" if abs(boosted_score) > 25 else "WEAK")
    )

    return signal


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------


def _dict_to_chain_analytics(d: dict) -> ChainAnalytics:
    """Convert an analytics dict back to a ChainAnalytics dataclass."""
    return ChainAnalytics(
        pcr=d.get("pcr", 0),
        pcr_sentiment=d.get("pcrSentiment", "NEUTRAL"),
        max_pain_strike=d.get("maxPainStrike", 0),
        max_pain_distance=d.get("maxPainDistance", 0),
        total_call_oi=d.get("totalCallOI", 0),
        total_put_oi=d.get("totalPutOI", 0),
        resistance=d.get("resistance", []),
        support=d.get("support", []),
        atm_iv=d.get("atmIV", 0),
    )


def _oi_signal_to_dict(s: OISignal) -> dict:
    """Convert an OISignal dataclass to a JSON-serializable dict."""
    rec = None
    if s.recommendation:
        rec = {
            "strike": s.recommendation.strike,
            "optionType": s.recommendation.option_type,
            "tradingsymbol": s.recommendation.tradingsymbol,
            "entry": s.recommendation.entry,
            "stopLoss": s.recommendation.stop_loss,
            "target1": s.recommendation.target1,
            "target2": s.recommendation.target2,
            "iv": s.recommendation.iv,
            "oi": s.recommendation.oi,
            "volume": s.recommendation.volume,
            "lotSize": s.recommendation.lot_size,
        }

    cash_rec = None
    if s.cash_recommendation:
        cash_rec = {
            "action": s.cash_recommendation.action,
            "entry": s.cash_recommendation.entry,
            "stopLoss": s.cash_recommendation.stop_loss,
            "target1": s.cash_recommendation.target1,
            "target2": s.cash_recommendation.target2,
        }

    return {
        "symbol": s.symbol,
        "category": s.category,
        "direction": s.direction,
        "confidence": s.confidence,
        "score": s.score,
        "recommendation": rec,
        "cashRecommendation": cash_rec,
        "analytics": s.analytics,
        "scoreBreakdown": {
            "momentum": s.score_breakdown.momentum,
            "pcr": s.score_breakdown.pcr,
            "oiChange": s.score_breakdown.oi_change,
            "maxPain": s.score_breakdown.max_pain,
            "ivSkew": s.score_breakdown.iv_skew,
            "oiUnwinding": s.score_breakdown.oi_unwinding,
            "pricePosition": s.score_breakdown.price_position,
        },
        "reasons": s.reasons,
        "timestamp": s.timestamp,
    }


def _prescreen_to_dict(p: FuturesPreScreen) -> dict:
    """Convert FuturesPreScreen to API-friendly dict."""
    return {
        "symbol": p.name,
        "spot": p.spot,
        "futPrice": p.fut_price,
        "changePct": p.change_pct,
        "oi": p.oi,
        "oiChange": p.oi_change,
        "volume": p.volume,
        "preScore": p.pre_score,
        "preDirection": p.pre_direction,
        "preReasons": p.pre_reasons,
    }


def _build_summary(signals: list[OISignal]) -> dict:
    """Build signal summary counts."""
    return {
        "bullish": sum(1 for s in signals if s.direction == "BULLISH"),
        "bearish": sum(1 for s in signals if s.direction == "BEARISH"),
        "neutral": sum(1 for s in signals if s.direction == "NEUTRAL"),
        "strongSignals": sum(1 for s in signals if s.confidence == "STRONG"),
        "total": len(signals),
    }


def _build_summary_from_dicts(signals: list[dict]) -> dict:
    """Build signal summary from dict-form signals."""
    return {
        "bullish": sum(1 for s in signals if s.get("direction") == "BULLISH"),
        "bearish": sum(1 for s in signals if s.get("direction") == "BEARISH"),
        "neutral": sum(1 for s in signals if s.get("direction") == "NEUTRAL"),
        "strongSignals": sum(1 for s in signals if s.get("confidence") == "STRONG"),
        "total": len(signals),
    }


def _parse_expiry_safe(val: Any) -> Optional[datetime]:
    """Parse expiry without raising, returning None on failure."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        for fmt in ("%Y-%m-%d", "%a %b %d %Y", "%d %b %Y"):
            try:
                return datetime.strptime(val, fmt)
            except ValueError:
                continue
    try:
        return datetime(val.year, val.month, val.day)
    except Exception:
        return None
