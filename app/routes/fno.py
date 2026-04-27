"""
F&O routes -- list F&O stocks, stock options chain, active futures.

Ported from tradingdesk/apps/gateway/src/routes/fno.routes.ts
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from kiteconnect import KiteConnect

from app.dependencies import get_kite

logger = logging.getLogger("alpha_radar.routes.fno")

router = APIRouter(prefix="/api/fno", tags=["fno"])

# ── NFO instruments cache ────────────────────────────────────────────────────
_nfo_instruments: list[dict] | None = None
_nfo_instruments_ts: float = 0


def _get_nfo_instruments(kite: KiteConnect) -> list[dict]:
    global _nfo_instruments, _nfo_instruments_ts
    if not _nfo_instruments or time.time() - _nfo_instruments_ts > 3600:
        _nfo_instruments = kite.instruments("NFO")
        _nfo_instruments_ts = time.time()
    return _nfo_instruments


# ── GET /api/fno/stocks ──────────────────────────────────────────────────────
@router.get("/stocks")
async def fno_stocks(kite: KiteConnect = Depends(get_kite)) -> dict:
    """List all stocks with active F&O contracts."""
    try:
        instruments = _get_nfo_instruments(kite)
        stock_set: set[str] = set()
        for i in instruments:
            if i.get("instrument_type") == "FUT" and i.get("name") and i.get("segment") == "NFO-FUT":
                stock_set.add(i["name"])

        index_names = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}
        stocks = sorted(stock_set - index_names)

        return {"success": True, "count": len(stocks), "stocks": stocks}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /api/fno/chain/{symbol} ──────────────────────────────────────────────
@router.get("/chain/{symbol}")
async def fno_chain(
    symbol: str = Path(..., description="Stock symbol, e.g. RELIANCE"),
    expiry: str | None = Query(None, description="Expiry date YYYY-MM-DD"),
    kite: KiteConnect = Depends(get_kite),
) -> dict:
    """Options chain for a specific F&O stock."""
    symbol = symbol.upper()
    try:
        # Spot price
        spot_quote = kite.quote([f"NSE:{symbol}"])
        sq = spot_quote.get(f"NSE:{symbol}", {})
        spot = sq.get("last_price")
        if not spot:
            raise HTTPException(status_code=404, detail=f"Symbol not found: {symbol}")

        instruments = _get_nfo_instruments(kite)
        stock_opts = [i for i in instruments if i.get("name") == symbol and i.get("instrument_type") != "FUT"]
        if not stock_opts:
            raise HTTPException(status_code=404, detail=f"No F&O contracts for {symbol}")

        now = datetime.now()
        expiry_map: dict[str, dict] = {}
        for inst in stock_opts:
            exp = inst.get("expiry")
            if not exp:
                continue
            d = datetime(exp.year, exp.month, exp.day) if hasattr(exp, "year") else datetime.fromisoformat(str(exp))
            if d <= now:
                continue
            key = d.strftime("%Y-%m-%d")
            if key not in expiry_map:
                expiry_map[key] = {"date": d, "instruments": []}
            expiry_map[key]["instruments"].append(inst)

        sorted_expiries = sorted(expiry_map.values(), key=lambda x: x["date"])
        if not sorted_expiries:
            raise HTTPException(status_code=404, detail=f"No active expiries for {symbol}")

        # Select expiry
        if expiry:
            selected = next((e for e in sorted_expiries if e["date"].strftime("%Y-%m-%d") == expiry), sorted_expiries[0])
        else:
            selected = sorted_expiries[0]

        chain_instruments = selected["instruments"]
        all_expiries = [e["date"].strftime("%Y-%m-%d") for e in sorted_expiries]

        # Fetch quotes in batches
        tokens = [f"NFO:{i['tradingsymbol']}" for i in chain_instruments]
        all_quotes: dict = {}
        for i in range(0, len(tokens), 200):
            q = kite.quote(tokens[i: i + 200])
            all_quotes.update(q)
            if i + 200 < len(tokens):
                time.sleep(0.35)

        # Build chain
        strike_map: dict[float, dict] = {}
        for inst in chain_instruments:
            strike = inst["strike"]
            if strike not in strike_map:
                strike_map[strike] = {"strike": strike, "call": None, "put": None}
            q = all_quotes.get(f"NFO:{inst['tradingsymbol']}")
            if not q:
                continue

            oi = q.get("oi", 0) or 0
            oi_change = oi - (q.get("oi_day_low", oi) or oi)
            data = {
                "oi": oi,
                "oiChange": oi_change,
                "volume": q.get("volume", 0) or 0,
                "ltp": q.get("last_price", 0) or 0,
                "iv": round(q["implied_volatility"], 1) if q.get("implied_volatility") else None,
                "tradingsymbol": inst["tradingsymbol"],
                "lotSize": inst.get("lot_size", 0),
            }
            if inst["instrument_type"] == "CE":
                strike_map[strike]["call"] = data
            elif inst["instrument_type"] == "PE":
                strike_map[strike]["put"] = data

        chain = sorted(
            [s for s in strike_map.values() if s["call"] or s["put"]],
            key=lambda s: s["strike"],
        )

        # Analytics
        full_chain = [s for s in chain if s["call"] and s["put"]]
        total_call_oi = sum(s["call"]["oi"] for s in full_chain)
        total_put_oi = sum(s["put"]["oi"] for s in full_chain)
        pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 0

        min_loss = float("inf")
        max_pain_strike = full_chain[0]["strike"] if full_chain else 0
        for pivot in full_chain:
            total_loss = 0
            for row in full_chain:
                if row["strike"] < pivot["strike"]:
                    total_loss += (pivot["strike"] - row["strike"]) * row["call"]["oi"]
                if row["strike"] > pivot["strike"]:
                    total_loss += (row["strike"] - pivot["strike"]) * row["put"]["oi"]
            if total_loss < min_loss:
                min_loss = total_loss
                max_pain_strike = pivot["strike"]

        return {
            "success": True,
            "symbol": symbol,
            "spot": round(spot, 2),
            "expiry": selected["date"].strftime("%Y-%m-%d"),
            "expiries": all_expiries,
            "lotSize": chain_instruments[0].get("lot_size", 0) if chain_instruments else 0,
            "chain": chain,
            "analytics": {
                "pcr": pcr,
                "pcrSentiment": "BULLISH" if pcr > 1.3 else ("BEARISH" if pcr < 0.7 else "NEUTRAL"),
                "maxPainStrike": max_pain_strike,
                "totalCallOI": total_call_oi,
                "totalPutOI": total_put_oi,
            },
            "timestamp": int(time.time() * 1000),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("[fno] Chain error for %s", symbol)
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /api/fno/futures ─────────────────────────────────────────────────────
@router.get("/futures")
async def fno_futures(kite: KiteConnect = Depends(get_kite)) -> dict:
    """Active nearest futures with quotes for all F&O stocks."""
    try:
        instruments = _get_nfo_instruments(kite)
        now = datetime.now()

        futures = sorted(
            [i for i in instruments if i.get("instrument_type") == "FUT" and _exp_date(i) > now],
            key=lambda i: _exp_date(i),
        )

        nearest_fut: dict[str, dict] = {}
        for f in futures:
            name = f.get("name", "")
            if name not in nearest_fut:
                nearest_fut[name] = f

        symbols = [f"NFO:{inst['tradingsymbol']}" for inst in nearest_fut.values()]

        all_quotes: dict = {}
        for i in range(0, len(symbols), 200):
            q = kite.quote(symbols[i: i + 200])
            all_quotes.update(q)
            if i + 200 < len(symbols):
                time.sleep(0.35)

        result = []
        for name, inst in nearest_fut.items():
            q = all_quotes.get(f"NFO:{inst['tradingsymbol']}", {})
            ltp = q.get("last_price", 0)
            close = q.get("ohlc", {}).get("close", ltp) or ltp
            change_pct = round(((ltp - close) / close * 100), 2) if close > 0 else 0

            oi = q.get("oi", 0) or 0
            oi_day_low = q.get("oi_day_low", 0) or 0
            oi_change = oi - oi_day_low if oi_day_low > 0 else 0
            oi_change_pct = round((oi_change / oi_day_low) * 100, 2) if oi_day_low > 0 else 0

            result.append({
                "name": name,
                "tradingsymbol": inst["tradingsymbol"],
                "expiry": _exp_date(inst).strftime("%Y-%m-%d"),
                "lotSize": inst.get("lot_size", 0),
                "last": ltp,
                "volume": q.get("volume", 0) or 0,
                "oi": oi,
                "oiDayLow": oi_day_low,
                "oiChange": oi_change,
                "oiChangePercent": oi_change_pct,
                "change": q.get("net_change", 0) or 0,
                "changePercent": change_pct,
            })

        result.sort(key=lambda x: x["name"])
        return {"success": True, "count": len(result), "data": result}

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


def _exp_date(inst: dict) -> datetime:
    exp = inst.get("expiry")
    if hasattr(exp, "year"):
        return datetime(exp.year, exp.month, exp.day)
    return datetime.fromisoformat(str(exp))
