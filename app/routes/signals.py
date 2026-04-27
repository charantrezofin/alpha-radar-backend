"""
OI-based signal routes -- index signals, stock signals, live signals, ORB, signal log.

Ported from:
  - tradingdesk/apps/gateway/src/routes/signals.routes.ts
  - Alpha-Radar-backend/server.js (live-signals, orb-signals, signal-log, signal-outcome)
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from kiteconnect import KiteConnect
from pydantic import BaseModel
from supabase import Client

from app.config import settings
from app.core.session_cache import save_session, load_session, is_market_hours
from app.dependencies import get_current_user, get_kite, get_supabase, require_premium
from app.engines import (
    compute_oi_signal,
    prescreen_stock,
    ChainRow,
    OptionLeg,
    ChainAnalytics,
)
from app.routes.options import INDEX_CONFIG, get_options_chain

logger = logging.getLogger("alpha_radar.routes.signals")

router = APIRouter(prefix="/api/signals", tags=["signals"])

# ── Caches ────────────────────────────────────────────────────────────��──────
_signal_cache: dict[str, dict[str, Any]] = {}

# ── In-memory signal log ──────────────────────────────────────────────────���──
_signal_log: list[dict] = []
_MAX_SIGNAL_LOG = 500


def _log_signal(symbol: str, signal_type: str, price: float, score: float, vol_ratio: float, change_pct: float) -> None:
    """Append a signal to the in-memory log."""
    _signal_log.insert(0, {
        "id": int(time.time() * 1000) + hash(symbol) % 1000,
        "time": datetime.now(settings.TIMEZONE).strftime("%H:%M:%S"),
        "timestamp": int(time.time() * 1000),
        "symbol": symbol,
        "signalType": signal_type,
        "price": price,
        "score": score,
        "volRatio": vol_ratio,
        "changePct": change_pct,
        "outcome": None,
    })
    if len(_signal_log) > _MAX_SIGNAL_LOG:
        _signal_log.pop()


# ── NFO instruments cache ────────────────────────────────────────────────────
_nfo_instruments: list[dict] | None = None
_nfo_instruments_ts: float = 0


def _get_nfo_instruments(kite: KiteConnect) -> list[dict]:
    global _nfo_instruments, _nfo_instruments_ts
    if not _nfo_instruments or time.time() - _nfo_instruments_ts > 3600:
        _nfo_instruments = kite.instruments("NFO")
        _nfo_instruments_ts = time.time()
    return _nfo_instruments


# ── Index signals ────────────────────────────────────────────────────────────

def _get_index_signals(kite: KiteConnect) -> list[dict]:
    """Fetch options chain for each index and compute OI signal."""
    cache_key = "indices"
    cached = _signal_cache.get(cache_key)
    if cached and time.time() - cached["ts"] < 60:
        return cached["data"]

    signals: list[dict] = []
    for key, cfg in INDEX_CONFIG.items():
        try:
            data = get_options_chain(kite, key)

            # Build ChainRow objects for OI signal engine
            chain_rows = []
            for row in data["chain"]:
                call_leg = OptionLeg(
                    oi=row["call"]["oi"],
                    oi_change=row["call"]["oiChange"],
                    volume=row["call"]["volume"],
                    ltp=row["call"]["ltp"],
                    iv=row["call"]["iv"],
                )
                put_leg = OptionLeg(
                    oi=row["put"]["oi"],
                    oi_change=row["put"]["oiChange"],
                    volume=row["put"]["volume"],
                    ltp=row["put"]["ltp"],
                    iv=row["put"]["iv"],
                )
                chain_rows.append(ChainRow(strike=row["strike"], call=call_leg, put=put_leg))

            pcr_val = data["analytics"]["pcr"]
            spot_val = data["spot"]
            max_pain_val = data["analytics"]["maxPainStrike"]
            pcr_sent = "BULLISH" if pcr_val > 1.3 else "BEARISH" if pcr_val < 0.7 else "NEUTRAL"
            max_pain_dist = ((spot_val - max_pain_val) / spot_val * 100) if spot_val else 0

            analytics = ChainAnalytics(
                pcr=pcr_val,
                pcr_sentiment=pcr_sent,
                max_pain_strike=max_pain_val,
                max_pain_distance=round(max_pain_dist, 2),
                total_call_oi=data["analytics"]["totalCallOI"],
                total_put_oi=data["analytics"]["totalPutOI"],
                resistance=data["analytics"].get("topOICalls", []),
                support=data["analytics"].get("topOIPuts", []),
                atm_iv=data["analytics"]["atmIV"],
            )

            signal = compute_oi_signal(
                symbol=data["name"],
                category="index",
                spot=spot_val,
                prev_close=data.get("prevClose", 0),
                expiry=data["expiry"],
                strike_step=data["strikeStep"],
                chain=chain_rows,
                analytics=analytics,
            )
            signals.append(signal.__dict__ if hasattr(signal, "__dict__") else signal)
        except Exception as exc:
            logger.error("[signals] Index %s failed: %s", key, exc)

        time.sleep(0.5)  # Rate limit between indices

    signals.sort(key=lambda s: abs(s.get("score", 0)), reverse=True)
    _signal_cache[cache_key] = {"ts": time.time(), "data": signals}
    return signals


# ── Stock signals (2-phase) ──────────────────────────────────────────────────

def _get_stock_signals(kite: KiteConnect, deep_limit: int = 20) -> dict:
    """
    Phase 1: Batch-fetch all futures + spot quotes, pre-screen.
    Phase 2: Deep OI analysis on top N stocks.
    """
    cache_key = f"stocks_v2_{deep_limit}"
    cached = _signal_cache.get(cache_key)
    if cached and time.time() - cached["ts"] < 90:
        return cached["data"]

    instruments = _get_nfo_instruments(kite)
    now = datetime.now()

    # Get nearest month futures
    futures = sorted(
        [i for i in instruments if i.get("instrument_type") == "FUT" and _expiry_date(i) > now],
        key=lambda i: _expiry_date(i),
    )
    nearest_fut: dict[str, dict] = {}
    for f in futures:
        name = f.get("name", "")
        if name not in nearest_fut:
            nearest_fut[name] = f

    index_names = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"}
    stock_futures = [(name, inst) for name, inst in nearest_fut.items() if name not in index_names]

    # Phase 1: batch fetch
    logger.info("[signals] Phase 1: Pre-screening %d F&O stocks", len(stock_futures))
    fut_symbols = [f"NFO:{inst['tradingsymbol']}" for _, inst in stock_futures]
    spot_symbols = [f"NSE:{name}" for name, _ in stock_futures]
    all_symbols = fut_symbols + spot_symbols

    all_quotes: dict = {}
    for i in range(0, len(all_symbols), 400):
        try:
            q = kite.quote(all_symbols[i: i + 400])
            all_quotes.update(q)
        except Exception:
            pass
        if i + 400 < len(all_symbols):
            time.sleep(0.4)

    pre_screened: list[dict] = []
    for name, inst in stock_futures:
        fut_quote = all_quotes.get(f"NFO:{inst['tradingsymbol']}", {})
        spot_quote = all_quotes.get(f"NSE:{name}", {})
        spot_price = spot_quote.get("last_price") or fut_quote.get("last_price", 0)
        spot_prev_close = spot_quote.get("ohlc", {}).get("close", 0) or 0

        fut_last = fut_quote.get("last_price", 0)
        fut_close = fut_quote.get("ohlc", {}).get("close", 0) or 0
        fut_oi = fut_quote.get("oi", 0) or 0
        fut_oi_day_low = fut_quote.get("oi_day_low", 0) or 0
        fut_volume = fut_quote.get("volume", 0) or 0
        change_pct = ((fut_last - fut_close) / fut_close * 100) if fut_close else 0

        result = prescreen_stock(
            name=name,
            spot=spot_price,
            fut_price=fut_last,
            change_pct=change_pct,
            oi=fut_oi,
            oi_change=fut_oi - fut_oi_day_low,
            volume=fut_volume,
        )
        if result.oi > 0:
            pre_screened.append(result.__dict__ if hasattr(result, "__dict__") else result)

    pre_screened.sort(key=lambda s: abs(s.get("pre_score", 0)), reverse=True)

    # Phase 2: deep analysis on top N
    top_stocks = [s for s in pre_screened if abs(s.get("pre_score", 0)) >= 10][:deep_limit]
    logger.info("[signals] Phase 2: Deep analysis for %d stocks", len(top_stocks))

    deep_signals: list[dict] = []
    for stock in top_stocks:
        try:
            stock_name = stock.get("name", "")
            stock_spot = stock.get("spot", 0)
            stock_prev_close = stock.get("spot_prev_close", 0)

            # Build options chain for this stock
            stock_opts = [
                i for i in instruments
                if i.get("name") == stock_name and i.get("instrument_type") != "FUT"
            ]
            if not stock_opts:
                continue

            # Get nearest expiry
            expiry_map: dict[str, dict] = {}
            for inst in stock_opts:
                expiry = inst.get("expiry")
                if not expiry:
                    continue
                d = _expiry_date_raw(expiry)
                if d <= now:
                    continue
                key = d.strftime("%Y-%m-%d")
                if key not in expiry_map:
                    expiry_map[key] = {"date": d, "instruments": []}
                expiry_map[key]["instruments"].append(inst)

            sorted_expiries = sorted(expiry_map.values(), key=lambda x: x["date"])
            if not sorted_expiries:
                continue

            chain_instruments = sorted_expiries[0]["instruments"]

            # Fetch option quotes
            tokens = [f"NFO:{i['tradingsymbol']}" for i in chain_instruments]
            opt_quotes: dict = {}
            for i in range(0, len(tokens), 500):
                try:
                    q = kite.quote(tokens[i: i + 500])
                    opt_quotes.update(q)
                except Exception:
                    pass
                if i + 500 < len(tokens):
                    time.sleep(0.4)

            # Build chain
            strike_map: dict[float, dict] = {}
            for inst in chain_instruments:
                strike = inst["strike"]
                if strike not in strike_map:
                    strike_map[strike] = {"strike": strike, "call": None, "put": None}
                q = opt_quotes.get(f"NFO:{inst['tradingsymbol']}")
                if not q:
                    continue
                oi = q.get("oi", 0) or 0
                oi_change = oi - (q.get("oi_day_low", oi) or oi)
                side = "call" if inst["instrument_type"] == "CE" else "put"
                strike_map[strike][side] = {
                    "oi": oi,
                    "oiChange": oi_change,
                    "volume": q.get("volume", 0) or 0,
                    "ltp": q.get("last_price", 0) or 0,
                    "iv": round(q["implied_volatility"], 1) if q.get("implied_volatility") else None,
                    "tradingsymbol": inst["tradingsymbol"],
                }

            chain = sorted(
                [s for s in strike_map.values() if s["call"] and s["put"]],
                key=lambda s: s["strike"],
            )
            if len(chain) < 3:
                continue

            # Analytics
            total_call_oi = sum(s["call"]["oi"] for s in chain)
            total_put_oi = sum(s["put"]["oi"] for s in chain)
            pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 0

            # Max pain
            min_loss = float("inf")
            max_pain_strike = chain[0]["strike"]
            for pivot in chain:
                total_loss = 0
                for row in chain:
                    if row["strike"] < pivot["strike"]:
                        total_loss += (pivot["strike"] - row["strike"]) * row["call"]["oi"]
                    if row["strike"] > pivot["strike"]:
                        total_loss += (row["strike"] - pivot["strike"]) * row["put"]["oi"]
                if total_loss < min_loss:
                    min_loss = total_loss
                    max_pain_strike = pivot["strike"]

            atm = min(chain, key=lambda s: abs(s["strike"] - stock_spot))
            atm_iv = ((atm["call"]["iv"] or 0) + (atm["put"]["iv"] or 0)) / 2
            strike_step = chain[1]["strike"] - chain[0]["strike"] if len(chain) >= 2 else 50

            chain_rows = [
                ChainRow(
                    strike=row["strike"],
                    call=OptionLeg(oi=row["call"]["oi"], oi_change=row["call"]["oiChange"],
                                   volume=row["call"]["volume"], ltp=row["call"]["ltp"], iv=row["call"]["iv"]),
                    put=OptionLeg(oi=row["put"]["oi"], oi_change=row["put"]["oiChange"],
                                  volume=row["put"]["volume"], ltp=row["put"]["ltp"], iv=row["put"]["iv"]),
                )
                for row in chain
            ]
            pcr_sent = "BULLISH" if pcr > 1.3 else "BEARISH" if pcr < 0.7 else "NEUTRAL"
            mp_dist = ((stock_spot - max_pain_strike) / stock_spot * 100) if stock_spot else 0
            analytics = ChainAnalytics(
                pcr=pcr, pcr_sentiment=pcr_sent,
                max_pain_strike=max_pain_strike, max_pain_distance=round(mp_dist, 2),
                total_call_oi=total_call_oi, total_put_oi=total_put_oi, atm_iv=round(atm_iv, 1),
            )

            signal = compute_oi_signal(
                symbol=stock_name, category="stock",
                spot=stock_spot, prev_close=stock_prev_close,
                expiry=sorted_expiries[0]["date"].strftime("%Y-%m-%d"),
                strike_step=strike_step, chain=chain_rows, analytics=analytics,
            )

            # Blend futures pre-score
            pre_score = stock.get("pre_score", 0)
            fut_boost = round(pre_score * 0.3)
            sig_dict = signal.__dict__ if hasattr(signal, "__dict__") else dict(signal)
            sig_dict["score"] = max(-100, min(100, sig_dict.get("score", 0) + fut_boost))
            for r in stock.get("pre_reasons", []):
                reasons = sig_dict.get("reasons", [])
                if r not in reasons:
                    reasons.append(r)
            sig_dict["direction"] = "BULLISH" if sig_dict["score"] > 15 else ("BEARISH" if sig_dict["score"] < -15 else "NEUTRAL")
            sig_dict["confidence"] = "STRONG" if abs(sig_dict["score"]) > 50 else ("MODERATE" if abs(sig_dict["score"]) > 25 else "WEAK")
            deep_signals.append(sig_dict)

            # Validation tracker — log STRONG/MODERATE fires (skip WEAK and NEUTRAL)
            if abs(sig_dict["score"]) >= 25 and sig_dict["direction"] != "NEUTRAL":
                try:
                    from app.services.signal_validator import log_signal_fire, compute_market_context
                    sig_type = "OI_BULLISH" if sig_dict["direction"] == "BULLISH" else "OI_BEARISH"
                    log_signal_fire(
                        symbol=stock_name,
                        signal_type=sig_type,
                        trigger_price=stock_spot,
                        strength=sig_dict["score"],
                        direction=sig_dict["direction"],
                        confidence=sig_dict["confidence"],
                        category="stock",
                        metadata={
                            "pcr": pcr,
                            "max_pain_strike": max_pain_strike,
                            "max_pain_distance_pct": round(mp_dist, 2),
                            "atm_iv": round(atm_iv, 1),
                            "pre_score": pre_score,
                            "reasons": sig_dict.get("reasons", []),
                        },
                        context=compute_market_context(),
                    )
                except Exception:
                    logger.exception("[signals] signal_validator log failed for %s", stock_name)

        except Exception as exc:
            logger.error("[signals] Deep analysis failed for %s: %s", stock.get("name"), exc)

        time.sleep(0.8)

    deep_signals.sort(key=lambda s: abs(s.get("score", 0)), reverse=True)

    result = {"deepSignals": deep_signals, "preScreened": pre_screened}
    if deep_signals or pre_screened:
        _signal_cache[cache_key] = {"ts": time.time(), "data": result}
    return result


def _expiry_date(inst: dict) -> datetime:
    exp = inst.get("expiry")
    if hasattr(exp, "year"):
        return datetime(exp.year, exp.month, exp.day)
    return datetime.fromisoformat(str(exp))


def _expiry_date_raw(exp: Any) -> datetime:
    if hasattr(exp, "year"):
        return datetime(exp.year, exp.month, exp.day)
    return datetime.fromisoformat(str(exp))


def _build_summary(signals: list[dict]) -> dict:
    return {
        "bullish": sum(1 for s in signals if s.get("direction") == "BULLISH"),
        "bearish": sum(1 for s in signals if s.get("direction") == "BEARISH"),
        "neutral": sum(1 for s in signals if s.get("direction") == "NEUTRAL"),
        "strongSignals": sum(1 for s in signals if s.get("confidence") == "STRONG"),
        "total": len(signals),
    }


# ── Routes ────────────────────────────────��──────────────────────────────────

# ── GET /api/signals/ ─────────────────────��──────────────────────────────────
@router.get("/")
async def get_signals(
    category: str = Query("all", description="indices|stocks|all"),
    deep: int = Query(20, description="Max stocks for deep analysis", le=50),
    symbol: Optional[str] = Query(None, description="Filter by specific symbol"),
    kite: KiteConnect = Depends(get_kite),
    user: dict = Depends(get_current_user),
) -> dict:
    """
    OI-based directional signals.
    - indices: sequential fetch of options chain per index
    - stocks: 2-phase (futures prescreen then deep options on top N)
    """
    cache_key = f"signals_{category}"
    try:
        if category == "indices":
            signals = _get_index_signals(kite)
            response = {"success": True, "signals": signals, "summary": _build_summary(signals), "timestamp": int(time.time() * 1000)}
            if signals:
                save_session(cache_key, response)
            elif not is_market_hours():
                cached = load_session(cache_key)
                if cached:
                    resp = cached["data"]
                    resp["cached"] = True
                    resp["cachedAt"] = cached.get("timestamp")
                    return resp
            return response

        if category == "stocks":
            result = _get_stock_signals(kite, deep)
            response = {
                "success": True,
                "signals": result["deepSignals"],
                "preScreened": result["preScreened"],
                "summary": _build_summary(result["deepSignals"]),
                "timestamp": int(time.time() * 1000),
            }
            if result["deepSignals"] or result["preScreened"]:
                save_session(cache_key, response)
            elif not is_market_hours():
                cached = load_session(cache_key)
                if cached:
                    resp = cached["data"]
                    resp["cached"] = True
                    resp["cachedAt"] = cached.get("timestamp")
                    return resp
            return response

        # Default: all
        index_signals = _get_index_signals(kite)
        stock_result = _get_stock_signals(kite, deep)
        all_signals = sorted(
            index_signals + stock_result["deepSignals"],
            key=lambda s: abs(s.get("score", 0)),
            reverse=True,
        )
        response = {
            "success": True,
            "signals": all_signals,
            "preScreened": stock_result["preScreened"],
            "summary": _build_summary(all_signals),
            "timestamp": int(time.time() * 1000),
        }
        if all_signals:
            save_session(cache_key, response)
        elif not is_market_hours():
            cached = load_session(cache_key)
            if cached:
                resp = cached["data"]
                resp["cached"] = True
                resp["cachedAt"] = cached.get("timestamp")
                return resp
        return response

    except Exception as exc:
        logger.exception("[signals] Error")
        # On error, try serving cached data if market is closed
        if not is_market_hours():
            cached = load_session(cache_key)
            if cached:
                resp = cached["data"]
                resp["cached"] = True
                resp["cachedAt"] = cached.get("timestamp")
                return resp
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /api/signals/live-signals ────────────────────────────────────────────
@router.get("/live-signals")
async def live_signals(user: dict = Depends(get_current_user)) -> dict:
    """PDH/PDL cross counts and combo surge counts from live tracker."""
    from app.core.session_cache import save_session, load_session, is_market_hours

    try:
        from app.caches import live_tracker, bear_tracker
        result = {
            "pdhCrossed": list(live_tracker.pdh_crossed),
            "comboSurge": list(live_tracker.combo_surge),
            "firstCrossTime": dict(live_tracker.first_cross_time),
            "firstComboTime": dict(live_tracker.first_combo_time),
            "pdhCrossCount": len(live_tracker.pdh_crossed),
            "comboSurgeCount": len(live_tracker.combo_surge),
            "pdlCrossed": list(bear_tracker.pdl_crossed),
            "comboSell": list(bear_tracker.combo_sell),
            "firstPDLTime": dict(bear_tracker.first_pdl_time),
            "firstComboSellTime": dict(bear_tracker.first_combo_sell_time),
            "pdlCrossCount": len(bear_tracker.pdl_crossed),
            "comboSellCount": len(bear_tracker.combo_sell),
            "timestamp": int(time.time() * 1000),
        }

        # Cache if we have meaningful data
        if result["pdhCrossCount"] > 0 or result["pdlCrossCount"] > 0:
            save_session("live_signals", result)

        return result
    except ImportError:
        # Trackers not initialized — serve cached if market closed
        if not is_market_hours():
            cached = load_session("live_signals")
            if cached:
                resp = cached["data"]
                resp["cached"] = True
                resp["cachedAt"] = cached.get("timestamp")
                return resp

        return {
            "pdhCrossed": [], "comboSurge": [],
            "firstCrossTime": {}, "firstComboTime": {},
            "pdhCrossCount": 0, "comboSurgeCount": 0,
            "pdlCrossed": [], "comboSell": [],
            "firstPDLTime": {}, "firstComboSellTime": {},
            "pdlCrossCount": 0, "comboSellCount": 0,
            "timestamp": int(time.time() * 1000),
        }


# ── GET /api/signals/orb-signals ─────────────────────────────────────────────
@router.get("/orb-signals")
async def orb_signals(user: dict = Depends(require_premium)) -> dict:
    """ORB15/ORB30 breakout status (premium only)."""
    try:
        from app.caches import orb_detector
        state = orb_detector.get_state()

        # Consume new breaks (one-time notification)
        new_breaks_15 = list(state.get("new_breaks_15", []))
        new_breaks_30 = list(state.get("new_breaks_30", []))
        new_breaks_down_15 = list(state.get("new_breaks_down_15", []))
        new_breaks_down_30 = list(state.get("new_breaks_down_30", []))
        orb_detector.clear_new_breaks()

        result = {
            "nearOrb15": list(state.get("near_orb_15", [])),
            "orbBreak15": list(state.get("orb_break_15", [])),
            "nearOrb30": list(state.get("near_orb_30", [])),
            "orbBreak30": list(state.get("orb_break_30", [])),
            "breakTime15": state.get("break_time_15", {}),
            "breakTime30": state.get("break_time_30", {}),
            "newBreaks15": new_breaks_15,
            "newBreaks30": new_breaks_30,
            "orbBreakDown15": list(state.get("orb_break_down_15", [])),
            "orbBreakDown30": list(state.get("orb_break_down_30", [])),
            "breakDownTime15": state.get("break_down_time_15", {}),
            "breakDownTime30": state.get("break_down_time_30", {}),
            "newBreaksDown15": new_breaks_down_15,
            "newBreaksDown30": new_breaks_down_30,
            "orbLoaded": state.get("loaded_count", 0),
            "timestamp": int(time.time() * 1000),
        }

        # Cache if we have ORB data
        if result.get("orbLoaded", 0) > 0:
            from app.core.session_cache import save_session
            save_session("orb_signals", result)

        return result
    except ImportError:
        # Serve cached ORB data when market closed
        from app.core.session_cache import load_session, is_market_hours
        if not is_market_hours():
            cached = load_session("orb_signals")
            if cached:
                resp = cached["data"]
                resp["cached"] = True
                resp["cachedAt"] = cached.get("timestamp")
                return resp
        return {"orbLoaded": 0, "timestamp": int(time.time() * 1000)}


# ── GET /api/signals/signal-log ──────────────────────────────────────────────
@router.get("/signal-log")
async def signal_log(
    request: Request,
    user: dict = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
) -> dict:
    """All signal entries. Free: last 5, Premium: all."""
    from app.routes.quotes import _load_subscription
    is_premium = await _load_subscription(request, sb)

    if is_premium:
        return {"signals": _signal_log, "count": len(_signal_log), "tier": "premium"}

    max_visible = settings.FREE_TIER_LIMITS.signal_log_max
    limited = _signal_log[-max_visible:]
    return {
        "signals": limited,
        "count": len(_signal_log),
        "visibleCount": len(limited),
        "tier": "free",
        "limited": len(_signal_log) > max_visible,
    }


# ── POST /api/signals/signal-outcome ────────────────────────────────────────
class SignalOutcomeBody(BaseModel):
    id: int | float
    outcome: str  # WIN, LOSS, SKIP
    exitPrice: Optional[float] = None


@router.post("/signal-outcome")
async def signal_outcome(
    body: SignalOutcomeBody,
    user: dict = Depends(get_current_user),
) -> dict:
    """Mark WIN/LOSS/SKIP outcome for a signal."""
    signal = next((s for s in _signal_log if s["id"] == body.id), None)
    if not signal:
        raise HTTPException(status_code=404, detail="Signal not found")

    signal["outcome"] = body.outcome
    signal["exitPrice"] = body.exitPrice
    if body.exitPrice and signal.get("price"):
        signal["pnlPct"] = round((body.exitPrice - signal["price"]) / signal["price"] * 100, 2)

    return {"success": True, "signal": signal}
