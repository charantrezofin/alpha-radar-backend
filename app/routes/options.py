"""
Options chain routes -- index options chain with analytics.

Ported from tradingdesk/apps/gateway/src/routes/options.routes.ts
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path
from kiteconnect import KiteConnect

from app.core.session_cache import save_session, load_session, is_market_hours
from app.dependencies import get_kite

logger = logging.getLogger("alpha_radar.routes.options")

router = APIRouter(prefix="/api/options", tags=["options"])

# ── Index configurations ─────────────────────────────────────────────────────
INDEX_CONFIG: dict[str, dict[str, Any]] = {
    "nifty": {
        "name": "NIFTY",
        "underlying": "NSE:NIFTY 50",
        "exchange": "NFO",
        "lotSize": 75,
        "strikeStep": 50,
        "histToken": 256265,
    },
    "banknifty": {
        "name": "BANKNIFTY",
        "underlying": "NSE:NIFTY BANK",
        "exchange": "NFO",
        "lotSize": 15,
        "strikeStep": 100,
        "histToken": 260105,
    },
    "finnifty": {
        "name": "FINNIFTY",
        "underlying": "NSE:NIFTY FIN SERVICE",
        "exchange": "NFO",
        "lotSize": 40,
        "strikeStep": 50,
        "histToken": 257801,
    },
    "midcpnifty": {
        "name": "MIDCPNIFTY",
        "underlying": "NSE:NIFTY MIDCAP SELECT",
        "exchange": "NFO",
        "lotSize": 75,
        "strikeStep": 25,
        "histToken": 288009,
    },
    "sensex": {
        "name": "SENSEX",
        "underlying": "BSE:SENSEX",
        "exchange": "BFO",
        "lotSize": 10,
        "strikeStep": 100,
        "histToken": 265,
    },
}

# ── Instrument caches ────────────────────────────────────────────────────────
_nfo_instruments: list[dict] | None = None
_nfo_instruments_ts: float = 0
_bfo_instruments: list[dict] | None = None
_bfo_instruments_ts: float = 0

# ── OI cache (60s TTL) ──────────────────────────────────────────────────────
_oi_cache: dict[str, dict[str, Any]] = {}


def _get_instruments(kite: KiteConnect, index_key: str) -> tuple[list[dict], str]:
    """Return (instruments, exchange) for the given index, with 1-hour caching."""
    global _nfo_instruments, _nfo_instruments_ts, _bfo_instruments, _bfo_instruments_ts

    cfg = INDEX_CONFIG[index_key]
    exchange = cfg["exchange"]

    if exchange == "BFO":
        if not _bfo_instruments or time.time() - _bfo_instruments_ts > 3600:
            _bfo_instruments = kite.instruments("BFO")
            _bfo_instruments_ts = time.time()
        return _bfo_instruments, exchange
    else:
        if not _nfo_instruments or time.time() - _nfo_instruments_ts > 3600:
            _nfo_instruments = kite.instruments("NFO")
            _nfo_instruments_ts = time.time()
        return _nfo_instruments, exchange


def get_options_chain(kite: KiteConnect, index_key: str) -> dict[str, Any]:
    """
    Build and return a full options chain with analytics for the given index.
    This function is also called from signals.py and squeeze.py.
    """
    cfg = INDEX_CONFIG.get(index_key)
    if not cfg:
        raise ValueError(f"Unknown index: {index_key}")

    # Fetch spot price
    spot_quote = kite.quote([cfg["underlying"]])
    sq = spot_quote.get(cfg["underlying"], {})
    spot = sq.get("last_price")
    prev_close = sq.get("ohlc", {}).get("close", 0) or 0

    if not spot:
        ltp_data = kite.ltp([cfg["underlying"]])
        spot = ltp_data.get(cfg["underlying"], {}).get("last_price")
    if not spot:
        raise ValueError(f"Could not fetch spot price for {cfg['name']}")

    instruments, exchange = _get_instruments(kite, index_key)

    # Filter to this index's options (not futures)
    index_instruments = [
        i for i in instruments
        if i.get("name") == cfg["name"] and i.get("instrument_type") != "FUT"
    ]

    now = datetime.now()

    # Group by expiry
    expiry_map: dict[str, dict] = {}
    for inst in index_instruments:
        expiry = inst.get("expiry")
        if not expiry:
            continue
        d = datetime(expiry.year, expiry.month, expiry.day) if hasattr(expiry, "year") else datetime.fromisoformat(str(expiry))
        if d <= now:
            continue
        key = d.strftime("%Y-%m-%d")
        if key not in expiry_map:
            expiry_map[key] = {"date": d, "instruments": []}
        expiry_map[key]["instruments"].append(inst)

    sorted_expiries = sorted(expiry_map.values(), key=lambda x: x["date"])
    if not sorted_expiries:
        raise ValueError(f"No expiries for {cfg['name']}")

    # Nearest expiry -- filter to strikes within +/-15% of spot
    chain_instruments = [
        i for i in sorted_expiries[0]["instruments"]
        if i["strike"] >= spot * 0.85 and i["strike"] <= spot * 1.15
    ]
    nearest_expiry = sorted_expiries[0]["date"].strftime("%Y-%m-%d")
    all_expiries = [e["date"].strftime("%Y-%m-%d") for e in sorted_expiries]

    # Fetch quotes in batches of 150
    tokens = [f"{exchange}:{i['tradingsymbol']}" for i in chain_instruments]
    all_quotes: dict = {}
    for i in range(0, len(tokens), 150):
        try:
            q = kite.quote(tokens[i: i + 150])
            all_quotes.update(q)
        except Exception as exc:
            logger.error("[options] %s: batch failed: %s", cfg["name"], exc)
        if i + 150 < len(tokens):
            time.sleep(0.4)

    # Build strike map
    strike_map: dict[float, dict] = {}
    for inst in chain_instruments:
        strike = inst["strike"]
        if strike not in strike_map:
            strike_map[strike] = {"strike": strike, "call": None, "put": None}

        q = all_quotes.get(f"{exchange}:{inst['tradingsymbol']}")
        if not q:
            continue

        oi = q.get("oi", 0) or 0
        oi_change = oi - (q.get("oi_day_low", oi) or oi)
        data = {
            "token": inst["instrument_token"],
            "oi": oi,
            "oiChange": oi_change,
            "volume": q.get("volume", 0) or 0,
            "ltp": q.get("last_price", 0) or 0,
            "iv": round(q["implied_volatility"], 1) if q.get("implied_volatility") else None,
            "tradingsymbol": inst["tradingsymbol"],
            "bidQty": (q.get("depth", {}).get("buy", [{}])[0].get("quantity", 0)) if q.get("depth") else 0,
            "askQty": (q.get("depth", {}).get("sell", [{}])[0].get("quantity", 0)) if q.get("depth") else 0,
        }

        if inst["instrument_type"] == "CE":
            strike_map[strike]["call"] = data
        elif inst["instrument_type"] == "PE":
            strike_map[strike]["put"] = data

    chain = sorted(
        [s for s in strike_map.values() if s["call"] and s["put"]],
        key=lambda s: s["strike"],
    )

    # Analytics
    total_call_oi = sum(s["call"]["oi"] for s in chain)
    total_put_oi = sum(s["put"]["oi"] for s in chain)
    pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 0

    # Max Pain
    min_loss = float("inf")
    max_pain_strike = chain[0]["strike"] if chain else 0
    for pivot in chain:
        total_loss = 0
        for row in chain:
            if row["strike"] < pivot["strike"]:
                total_loss += (pivot["strike"] - row["strike"]) * (row["call"]["oi"])
            if row["strike"] > pivot["strike"]:
                total_loss += (row["strike"] - pivot["strike"]) * (row["put"]["oi"])
        if total_loss < min_loss:
            min_loss = total_loss
            max_pain_strike = pivot["strike"]

    # Top OI strikes
    top_call_strikes = [
        s["strike"] for s in sorted(chain, key=lambda s: s["call"]["oi"], reverse=True)[:3]
    ]
    top_put_strikes = [
        s["strike"] for s in sorted(chain, key=lambda s: s["put"]["oi"], reverse=True)[:3]
    ]

    # ATM IV
    atm_strike = min(chain, key=lambda s: abs(s["strike"] - spot)) if chain else None
    atm_iv = 0.0
    if atm_strike:
        ce_iv = atm_strike["call"]["iv"] or 0
        pe_iv = atm_strike["put"]["iv"] or 0
        atm_iv = (ce_iv + pe_iv) / 2

    return {
        "index": index_key,
        "name": cfg["name"],
        "spot": round(spot, 2),
        "prevClose": round(prev_close, 2),
        "expiry": nearest_expiry,
        "expiries": all_expiries,
        "lotSize": cfg["lotSize"],
        "strikeStep": cfg["strikeStep"],
        "chain": chain,
        "analytics": {
            "pcr": pcr,
            "pcrSentiment": "BULLISH" if pcr > 1.3 else ("BEARISH" if pcr < 0.7 else "NEUTRAL"),
            "maxPainStrike": max_pain_strike,
            "maxPainDistance": round(spot - max_pain_strike),
            "totalCallOI": total_call_oi,
            "totalPutOI": total_put_oi,
            "resistance": top_call_strikes,
            "support": top_put_strikes,
            "atmIV": round(atm_iv, 1),
        },
        "timestamp": int(time.time() * 1000),
    }


# ── GET /api/options/ ────────────────────────────────────────────────────────
@router.get("/")
async def list_indices() -> dict:
    """List available index options."""
    return {
        "success": True,
        "indices": [
            {
                "key": key,
                "name": cfg["name"],
                "lotSize": cfg["lotSize"],
                "strikeStep": cfg["strikeStep"],
            }
            for key, cfg in INDEX_CONFIG.items()
        ],
    }


# ── GET /api/options/{index} ─────────────────────────────────────────────────
@router.get("/{index}")
async def options_chain(
    index: str = Path(..., description="Index key: nifty, banknifty, etc."),
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """
    Full options chain + analytics for the specified index.
    60-second cache per index.
    """
    key = index.lower()
    if key not in INDEX_CONFIG:
        raise HTTPException(
            status_code=400,
            detail={"error": "Unknown index", "available": list(INDEX_CONFIG.keys())},
        )

    session_key = f"options_{key}"

    # Return cache if <60s old
    cached = _oi_cache.get(key)
    if cached and time.time() - cached["ts"] < 60:
        return {"success": True, **cached["data"], "cached": True}

    # If market is closed and no in-memory cache, try session cache
    if not is_market_hours() and not cached:
        session_cached = load_session(session_key)
        if session_cached:
            resp = {"success": True, **session_cached["data"], "cached": True, "cachedAt": session_cached.get("timestamp")}
            return resp

    try:
        data = get_options_chain(kite, key)
        _oi_cache[key] = {"ts": time.time(), "data": data}

        # Session cache: save if we got meaningful chain data
        if data.get("chain") and len(data["chain"]) > 0:
            save_session(session_key, data)

        return {"success": True, **data}
    except Exception as exc:
        logger.exception("[options] Error for %s", key)
        # Serve cached data when market is closed
        if not is_market_hours():
            session_cached = load_session(session_key)
            if session_cached:
                resp = {"success": True, **session_cached["data"], "cached": True, "cachedAt": session_cached.get("timestamp")}
                return resp
        raise HTTPException(status_code=500, detail=str(exc))
