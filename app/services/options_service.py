"""
Options Service -- orchestrates options chain building for indices and stocks.

Fetches spot prices, filters instruments to nearest expiry, builds the strike
map with CE/PE legs, and computes chain analytics (PCR, max pain, ATM IV,
OI resistance/support).

Ported from tradingdesk/apps/gateway/src/routes/options.routes.ts
``getOptionsChain`` function.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime
from typing import Any, Optional

from kiteconnect import KiteConnect

from app.caches import instrument_cache
from app.data.index_config import INDEX_CONFIG, get_index_config
from app.engines.oi_signal import ChainAnalytics, ChainRow, OptionLeg

logger = logging.getLogger("alpha_radar.services.options")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_QUOTE_BATCH_SIZE = 150      # Kite can be strict; keep batches small
_QUOTE_BATCH_DELAY_S = 0.4   # 400ms between quote batches
_STRIKE_RANGE_PCT = 0.15     # +/-15% of spot for strike filtering


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _fetch_quotes_batched(
    kite: KiteConnect,
    symbols: list[str],
    batch_size: int = _QUOTE_BATCH_SIZE,
    delay: float = _QUOTE_BATCH_DELAY_S,
) -> dict[str, dict]:
    """Fetch Kite quotes in batches with rate-limit pauses."""
    all_quotes: dict[str, dict] = {}
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        try:
            quotes = await asyncio.to_thread(kite.quote, batch)
            all_quotes.update(quotes)
        except Exception:
            logger.warning("Options quote batch failed (%d-%d)", i, i + len(batch))
        if i + batch_size < len(symbols):
            await asyncio.sleep(delay)
    return all_quotes


def _nearest_expiry_instruments(
    instruments: list[dict],
    name: str,
    now: datetime,
) -> tuple[list[dict], Optional[datetime], list[str]]:
    """
    Filter option instruments for *name* to the nearest future expiry.

    Returns (chain_instruments, expiry_date, all_expiry_strings).
    """
    options = [
        i for i in instruments
        if i.get("name") == name and i.get("instrument_type") in ("CE", "PE")
    ]

    # Group by expiry
    expiry_map: dict[str, dict] = {}
    for inst in options:
        raw_expiry = inst.get("expiry")
        if not raw_expiry:
            continue
        d = raw_expiry if isinstance(raw_expiry, datetime) else _parse_expiry(raw_expiry)
        if d is None or d <= now:
            continue
        key = d.strftime("%Y-%m-%d")
        if key not in expiry_map:
            expiry_map[key] = {"date": d, "instruments": []}
        expiry_map[key]["instruments"].append(inst)

    if not expiry_map:
        return [], None, []

    sorted_expiries = sorted(expiry_map.values(), key=lambda e: e["date"])
    all_expiry_strs = [e["date"].strftime("%Y-%m-%d") for e in sorted_expiries]

    nearest = sorted_expiries[0]
    return nearest["instruments"], nearest["date"], all_expiry_strs


def _parse_expiry(val: Any) -> Optional[datetime]:
    """Parse expiry from string or datetime."""
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        for fmt in ("%Y-%m-%d", "%a %b %d %Y", "%d %b %Y"):
            try:
                return datetime.strptime(val, fmt)
            except ValueError:
                continue
    # Try date object
    try:
        return datetime(val.year, val.month, val.day)
    except Exception:
        return None


def _build_chain_and_analytics(
    chain_instruments: list[dict],
    all_quotes: dict[str, dict],
    exchange: str,
    spot: float,
) -> tuple[list[ChainRow], dict, float]:
    """
    Build the strike map and compute analytics from instrument list and quotes.

    Returns (chain_rows, analytics_dict, strike_step).
    """
    strike_map: dict[float, dict[str, Optional[OptionLeg]]] = {}

    for inst in chain_instruments:
        strike = inst.get("strike", 0)
        if strike <= 0:
            continue
        if strike not in strike_map:
            strike_map[strike] = {"call": None, "put": None}

        q_key = f"{exchange}:{inst['tradingsymbol']}"
        q = all_quotes.get(q_key)
        if not q:
            continue

        oi = q.get("oi", 0)
        oi_low = q.get("oi_day_low", oi)
        oi_change = oi - oi_low

        leg = OptionLeg(
            token=inst.get("instrument_token"),
            oi=oi,
            oi_change=oi_change,
            volume=q.get("volume", 0),
            ltp=q.get("last_price", 0.0),
            iv=round(q["implied_volatility"], 1) if q.get("implied_volatility") else None,
            tradingsymbol=inst["tradingsymbol"],
            bid_qty=(q.get("depth", {}).get("buy", [{}])[0].get("quantity", 0)),
            ask_qty=(q.get("depth", {}).get("sell", [{}])[0].get("quantity", 0)),
            lot_size=inst.get("lot_size"),
        )

        side = "call" if inst.get("instrument_type") == "CE" else "put"
        strike_map[strike][side] = leg

    # Build ChainRow list (only rows with both CE and PE)
    chain: list[ChainRow] = []
    for strike in sorted(strike_map):
        entry = strike_map[strike]
        if entry["call"] and entry["put"]:
            chain.append(ChainRow(strike=strike, call=entry["call"], put=entry["put"]))

    if not chain:
        return [], {}, 0

    # Strike step (gap between consecutive strikes)
    strike_step = chain[1].strike - chain[0].strike if len(chain) >= 2 else 50

    # --- Analytics ---
    total_call_oi = sum(r.call.oi for r in chain if r.call)
    total_put_oi = sum(r.put.oi for r in chain if r.put)
    pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 0.0

    # Max Pain
    min_loss = float("inf")
    max_pain_strike = chain[0].strike
    for pivot in chain:
        total_loss = 0.0
        for row in chain:
            if row.strike < pivot.strike:
                total_loss += (pivot.strike - row.strike) * (row.call.oi if row.call else 0)
            if row.strike > pivot.strike:
                total_loss += (row.strike - pivot.strike) * (row.put.oi if row.put else 0)
        if total_loss < min_loss:
            min_loss = total_loss
            max_pain_strike = pivot.strike

    # Top OI strikes (resistance = top call OI, support = top put OI)
    resistance = [
        r.strike
        for r in sorted(chain, key=lambda r: r.call.oi if r.call else 0, reverse=True)[:3]
    ]
    support = [
        r.strike
        for r in sorted(chain, key=lambda r: r.put.oi if r.put else 0, reverse=True)[:3]
    ]

    # ATM IV
    atm_row = min(chain, key=lambda r: abs(r.strike - spot))
    atm_iv = 0.0
    if atm_row:
        ce_iv = atm_row.call.iv if atm_row.call and atm_row.call.iv else 0.0
        pe_iv = atm_row.put.iv if atm_row.put and atm_row.put.iv else 0.0
        atm_iv = round((ce_iv + pe_iv) / 2, 1) if (ce_iv or pe_iv) else 0.0

    pcr_sentiment = "BULLISH" if pcr > 1.3 else ("BEARISH" if pcr < 0.7 else "NEUTRAL")

    analytics = {
        "pcr": pcr,
        "pcrSentiment": pcr_sentiment,
        "maxPainStrike": max_pain_strike,
        "maxPainDistance": round(spot - max_pain_strike),
        "totalCallOI": total_call_oi,
        "totalPutOI": total_put_oi,
        "resistance": resistance,
        "support": support,
        "atmIV": atm_iv,
    }

    return chain, analytics, strike_step


# ---------------------------------------------------------------------------
# Public API: Index options chain
# ---------------------------------------------------------------------------


async def get_options_chain(kite: KiteConnect, index_key: str) -> dict:
    """
    Build the full options chain for an index (nifty, banknifty, etc.).

    Parameters
    ----------
    kite : KiteConnect
        Authenticated Kite client.
    index_key : str
        Key from INDEX_CONFIG (e.g. ``"nifty"``, ``"banknifty"``).

    Returns
    -------
    dict
        ``{index, name, spot, prevClose, expiry, expiries, lotSize,
        strikeStep, chain, analytics, timestamp}``

    Raises
    ------
    ValueError
        If index_key is unknown or spot price cannot be fetched.
    """
    cfg = get_index_config(index_key)
    if cfg is None:
        raise ValueError(f"Unknown index: {index_key}")

    underlying = cfg["underlying"]
    exchange = cfg["exchange"]
    strike_step_config = cfg["strikeStep"]
    lot_size = cfg["lotSize"]
    name = index_key.upper()
    if index_key == "nifty":
        name = "NIFTY"
    elif index_key == "banknifty":
        name = "BANKNIFTY"
    elif index_key == "finnifty":
        name = "FINNIFTY"
    elif index_key == "midcpnifty":
        name = "MIDCPNIFTY"
    elif index_key == "sensex":
        name = "SENSEX"

    # 1. Fetch spot price
    spot, prev_close = await _fetch_spot(kite, underlying)
    if not spot:
        raise ValueError(f"Could not fetch spot price for {name}")

    # 2. Get instruments from cache
    instruments = (
        instrument_cache.get_bfo_instruments()
        if exchange == "BFO"
        else instrument_cache.get_nfo_instruments()
    )

    now = datetime.now()
    chain_instruments, expiry_date, all_expiries = _nearest_expiry_instruments(
        instruments, name, now
    )
    if not chain_instruments:
        raise ValueError(f"No option instruments found for {name}")

    # 3. Filter strikes to +/-15% of spot
    chain_instruments = [
        i for i in chain_instruments
        if i.get("strike", 0) >= spot * (1 - _STRIKE_RANGE_PCT)
        and i.get("strike", 0) <= spot * (1 + _STRIKE_RANGE_PCT)
    ]

    # 4. Fetch quotes in batches
    tokens = [f"{exchange}:{i['tradingsymbol']}" for i in chain_instruments]
    all_quotes = await _fetch_quotes_batched(kite, tokens)

    # 5. Build chain + analytics
    chain, analytics, strike_step = _build_chain_and_analytics(
        chain_instruments, all_quotes, exchange, spot
    )

    expiry_str = expiry_date.strftime("%a %b %d %Y") if expiry_date else ""

    return {
        "index": index_key,
        "name": name,
        "spot": round(spot, 2),
        "prevClose": round(prev_close, 2),
        "expiry": expiry_str,
        "expiries": all_expiries,
        "lotSize": lot_size,
        "strikeStep": strike_step or strike_step_config,
        "chain": chain,
        "analytics": analytics,
        "timestamp": int(time.time() * 1000),
    }


# ---------------------------------------------------------------------------
# Public API: Stock options chain
# ---------------------------------------------------------------------------


async def get_stock_options_chain(
    kite: KiteConnect,
    symbol: str,
    expiry: Optional[str] = None,
) -> dict:
    """
    Build the full options chain for an individual F&O stock.

    Parameters
    ----------
    kite : KiteConnect
        Authenticated Kite client.
    symbol : str
        F&O stock symbol, e.g. ``"RELIANCE"``.
    expiry : str, optional
        Specific expiry date (``"YYYY-MM-DD"``). Defaults to nearest.

    Returns
    -------
    dict
        Same shape as ``get_options_chain`` but for a stock.
    """
    # Spot price
    spot, prev_close = await _fetch_spot(kite, f"NSE:{symbol}")
    if not spot:
        raise ValueError(f"Could not fetch spot price for {symbol}")

    # NFO instruments for this stock
    instruments = instrument_cache.get_nfo_instruments()
    now = datetime.now()

    if expiry:
        # Filter to specific expiry
        target_date = _parse_expiry(expiry)
        if not target_date:
            raise ValueError(f"Invalid expiry format: {expiry}")

        chain_instruments = [
            i for i in instruments
            if i.get("name") == symbol
            and i.get("instrument_type") in ("CE", "PE")
            and _expiry_matches(i.get("expiry"), target_date)
        ]
        expiry_date = target_date
        all_expiries = [expiry]
    else:
        chain_instruments, expiry_date, all_expiries = _nearest_expiry_instruments(
            instruments, symbol, now
        )

    if not chain_instruments:
        raise ValueError(f"No option instruments found for {symbol}")

    # Filter strikes to +/-15% of spot
    chain_instruments = [
        i for i in chain_instruments
        if i.get("strike", 0) >= spot * (1 - _STRIKE_RANGE_PCT)
        and i.get("strike", 0) <= spot * (1 + _STRIKE_RANGE_PCT)
    ]

    # Fetch quotes
    tokens = [f"NFO:{i['tradingsymbol']}" for i in chain_instruments]
    all_quotes = await _fetch_quotes_batched(kite, tokens)

    chain, analytics, strike_step = _build_chain_and_analytics(
        chain_instruments, all_quotes, "NFO", spot
    )

    expiry_str = expiry_date.strftime("%a %b %d %Y") if expiry_date else ""

    # Lot size from first instrument
    lot_size = chain_instruments[0].get("lot_size", 1) if chain_instruments else 1

    return {
        "symbol": symbol,
        "name": symbol,
        "spot": round(spot, 2),
        "prevClose": round(prev_close, 2),
        "expiry": expiry_str,
        "expiries": all_expiries,
        "lotSize": lot_size,
        "strikeStep": strike_step,
        "chain": chain,
        "analytics": analytics,
        "timestamp": int(time.time() * 1000),
    }


# ---------------------------------------------------------------------------
# Internal: spot price fetching
# ---------------------------------------------------------------------------


async def _fetch_spot(kite: KiteConnect, underlying: str) -> tuple[float, float]:
    """
    Fetch spot price and prev close for an underlying symbol.

    Returns (spot, prev_close). Falls back to LTP if quote fails.
    """
    spot = 0.0
    prev_close = 0.0

    try:
        raw = await asyncio.to_thread(kite.quote, [underlying])
        q = raw.get(underlying, {})
        spot = q.get("last_price", 0.0)
        prev_close = q.get("ohlc", {}).get("close", 0.0)
    except Exception:
        logger.debug("Spot quote failed for %s, trying LTP", underlying)

    if not spot:
        try:
            raw = await asyncio.to_thread(kite.ltp, [underlying])
            spot = raw.get(underlying, {}).get("last_price", 0.0)
        except Exception:
            logger.warning("LTP fallback also failed for %s", underlying)

    return spot, prev_close


def _expiry_matches(raw_expiry: Any, target: datetime) -> bool:
    """Check if an instrument's expiry matches the target date."""
    d = _parse_expiry(raw_expiry)
    if d is None:
        return False
    return d.date() == target.date()
