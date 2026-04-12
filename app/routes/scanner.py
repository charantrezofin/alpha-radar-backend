"""
Scanner routes -- CPR, patterns, VCP, volume profile, NR/squeeze, swing.

Each endpoint accepts pre-fetched OHLC data from the frontend:
    POST body: { symbols: [ {symbol, daily, weekly?, monthly?, ...}, ... ] }

The frontend fetches OHLC from /api/quotes/history for each symbol,
then POSTs the data here for analysis by the CPR engine modules.
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from app.dependencies import get_current_user

logger = logging.getLogger("alpha_radar.routes.scanner")

router = APIRouter(prefix="/api/scanner", tags=["scanner"])


# ── Shared helpers ──────────────────────────────────────────────────────────

def _to_df(candles: list[dict] | None) -> pd.DataFrame:
    """Convert a list of OHLC candle dicts to a pandas DataFrame."""
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles)
    for col in ["open", "high", "low", "close"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def _parse_symbols_payload(body_data: dict | list) -> list[dict]:
    """
    Extract the list of symbol items from the request body.
    Handles both {symbols: [...]} and bare [...] formats.
    """
    if isinstance(body_data, list):
        return body_data
    return body_data.get("symbols", [])


def _make_response(results: list[dict], **extra) -> dict:
    return {
        "success": True,
        "results": results,
        "count": len(results),
        "timestamp": int(time.time() * 1000),
        **extra,
    }


# ── POST /api/scanner/cpr ──────────────────────────────────────────────────

@router.post("/cpr")
async def scan_cpr(
    request: Request,
    user: dict = Depends(get_current_user),
) -> dict:
    """
    CPR multi-timeframe scan.

    Frontend sends:
        { symbols: [
            { symbol, daily: [...candles], weekly?: [...], monthly?: [...], current_price? },
            ...
        ] }

    Returns:
        { results: [ { symbol, total_score, alert_tier, direction, monthly, weekly, daily, ... } ] }
    """
    body_data = await request.json()
    results: list[dict] = []

    try:
        items = _parse_symbols_payload(body_data)

        for item in items:
            if isinstance(item, str):
                # Plain symbol string -- skip, we need OHLC data
                continue

            symbol = item.get("symbol", "")
            daily = item.get("daily", [])
            weekly = item.get("weekly") or []
            monthly = item.get("monthly") or []
            current_price = item.get("current_price")

            if not daily or len(daily) < 5:
                continue

            try:
                from app.engines.cpr.cpr_calculator import (
                    get_cpr_analysis, CPRSequenceResult, CPRDirection,
                )
                from app.engines.cpr.pema_calculator import (
                    compute_pema, PEMAResult, PEMAStack, PEMASlope,
                )
                from app.engines.cpr.signal_scorer import score_symbol

                daily_df = _to_df(daily)
                weekly_df = _to_df(weekly) if weekly else pd.DataFrame()
                monthly_df = _to_df(monthly) if monthly else pd.DataFrame()

                price = current_price or float(daily_df.iloc[-1]["close"])

                def _empty_seq():
                    return CPRSequenceResult(
                        n_periods=0, n_ascending=0, n_descending=0, n_parallel=0,
                        consecutive_ascending=0, consecutive_descending=0,
                        overall_direction=CPRDirection.UNKNOWN,
                        is_long_valid=False, is_short_valid=False,
                        latest_cpr=None, previous_cpr=None,
                    )

                def _empty_pema():
                    return PEMAResult(
                        fast=0, mid=0, slow=0,
                        fast_prev=0, mid_prev=0, slow_prev=0,
                        stack=PEMAStack.TANGLED, slope=PEMASlope.MIXED,
                        fast_slope_pct=0, mid_slope_pct=0, slow_slope_pct=0,
                        pullback_zone_upper=0, pullback_zone_lower=0,
                        is_bullish_stack=False, is_bearish_stack=False,
                        is_rising=False, is_falling=False,
                        price_in_pullback_zone=False,
                        price_below_fast_ema=False, price_above_fast_ema=False,
                    )

                # Compute CPR sequence for each timeframe
                if len(daily_df) >= 5:
                    _, daily_seq = get_cpr_analysis(daily_df)
                else:
                    daily_seq = _empty_seq()

                if not weekly_df.empty and len(weekly_df) >= 3:
                    _, weekly_seq = get_cpr_analysis(weekly_df)
                else:
                    weekly_seq = _empty_seq()

                if not monthly_df.empty and len(monthly_df) >= 3:
                    _, monthly_seq = get_cpr_analysis(monthly_df)
                else:
                    monthly_seq = _empty_seq()

                # Compute PEMA for each timeframe (needs >= 55 rows for the slow EMA)
                daily_pema = compute_pema(daily_df, price) if len(daily_df) >= 55 else _empty_pema()
                weekly_pema = compute_pema(weekly_df, price) if not weekly_df.empty and len(weekly_df) >= 55 else _empty_pema()
                monthly_pema = compute_pema(monthly_df, price) if not monthly_df.empty and len(monthly_df) >= 55 else _empty_pema()

                # Intraday placeholders (we don't have intraday data from this scanner)
                intraday_seq = _empty_seq()
                intraday_pema = _empty_pema()

                signal = score_symbol(
                    symbol=symbol,
                    current_price=price,
                    monthly_seq=monthly_seq,
                    weekly_seq=weekly_seq,
                    daily_seq=daily_seq,
                    monthly_pema=monthly_pema,
                    weekly_pema=weekly_pema,
                    daily_pema=daily_pema,
                    intraday_pema=intraday_pema,
                    intraday_seq=intraday_seq,
                    trigger_15min={"triggered": False},
                )

                # Serialize SignalResult to dict for JSON
                if signal:
                    result_dict = _signal_result_to_dict(signal)
                    results.append(result_dict)

            except Exception as e:
                logger.debug("CPR engine failed for %s: %s, falling back to basic", symbol, e)
                # Fallback: compute basic CPR levels
                try:
                    if daily and len(daily) >= 2:
                        prev = daily[-2]
                        h, l, c = float(prev["high"]), float(prev["low"]), float(prev["close"])
                        pivot = (h + l + c) / 3
                        bc = (h + l) / 2
                        tc = 2 * pivot - bc
                        ltp = current_price or float(daily[-1]["close"])
                        width = abs(tc - bc)
                        width_pct = (width / ltp * 100) if ltp else 0
                        results.append({
                            "symbol": symbol,
                            "current_price": ltp,
                            "total_score": 0,
                            "alert_tier": "None",
                            "direction": "NEUTRAL",
                            "monthly": _empty_tf_detail("monthly"),
                            "weekly": _empty_tf_detail("weekly"),
                            "daily": _empty_tf_detail("daily"),
                            "entry_price": None,
                            "stop_loss": None,
                            "target_1r2": None,
                            "target_1r3": None,
                            "notes": [f"CPR: P={pivot:.2f} BC={min(bc,tc):.2f} TC={max(bc,tc):.2f} Width={width_pct:.2f}%"],
                        })
                except Exception:
                    pass

        results.sort(key=lambda r: abs(r.get("total_score", 0)), reverse=True)
        return _make_response(results)

    except Exception as exc:
        logger.exception("CPR scan error")
        raise HTTPException(status_code=500, detail=str(exc))


def _empty_tf_detail(name: str) -> dict:
    """Return an empty timeframe detail dict matching the frontend interface."""
    return {
        "name": name,
        "total": 0,
        "cpr_direction": "unknown",
        "cpr_width_class": "normal",
        "pema_stack": "tangled",
        "pema_slope": "flat",
        "consecutive_ascending": 0,
        "consecutive_descending": 0,
        "price_vs_cpr": "inside",
        "is_valid_for_long": False,
        "is_valid_for_short": False,
    }


def _signal_result_to_dict(sr) -> dict:
    """Convert a SignalResult dataclass to a JSON-serializable dict matching the frontend."""
    def _tf_to_dict(tf) -> dict:
        return {
            "name": tf.name if hasattr(tf, "name") else "",
            "total": tf.total if hasattr(tf, "total") else 0,
            "cpr_direction": tf.cpr_direction.value if hasattr(tf.cpr_direction, "value") else str(tf.cpr_direction),
            "cpr_width_class": tf.cpr_width_class.value if hasattr(tf.cpr_width_class, "value") else str(tf.cpr_width_class),
            "pema_stack": tf.pema_stack.value if hasattr(tf.pema_stack, "value") else str(tf.pema_stack),
            "pema_slope": tf.pema_slope.value if hasattr(tf.pema_slope, "value") else str(tf.pema_slope),
            "consecutive_ascending": tf.consecutive_ascending if hasattr(tf, "consecutive_ascending") else 0,
            "consecutive_descending": tf.consecutive_descending if hasattr(tf, "consecutive_descending") else 0,
            "price_vs_cpr": tf.price_vs_cpr if hasattr(tf, "price_vs_cpr") else "inside",
            "is_valid_for_long": tf.is_valid_for_long if hasattr(tf, "is_valid_for_long") else False,
            "is_valid_for_short": tf.is_valid_for_short if hasattr(tf, "is_valid_for_short") else False,
        }

    return {
        "symbol": sr.symbol,
        "current_price": sr.current_price,
        "total_score": sr.total_score,
        "alert_tier": sr.alert_tier.value if hasattr(sr.alert_tier, "value") else str(sr.alert_tier),
        "direction": sr.direction.value if hasattr(sr.direction, "value") else str(sr.direction),
        "monthly": _tf_to_dict(sr.monthly),
        "weekly": _tf_to_dict(sr.weekly),
        "daily": _tf_to_dict(sr.daily),
        "entry_price": sr.entry_price,
        "stop_loss": sr.stop_loss,
        "target_1r2": sr.target_1r2,
        "target_1r3": sr.target_1r3,
        "notes": sr.notes if hasattr(sr, "notes") else [],
    }


# ── POST /api/scanner/patterns ─────────────────────────────────────────────

@router.post("/patterns")
async def scan_patterns(
    request: Request,
    user: dict = Depends(get_current_user),
) -> dict:
    """
    Pattern detection scan.

    Frontend sends:
        { symbols: [
            { symbol, daily: [...candles], weekly?: [...], lookback?: 60, min_confidence?: 40 },
            ...
        ] }

    Returns:
        { results: [ { symbol, current_price, pattern_count, patterns: [...] } ] }
    """
    body_data = await request.json()
    results: list[dict] = []

    try:
        items = _parse_symbols_payload(body_data)

        for item in items:
            if isinstance(item, str):
                continue

            symbol = item.get("symbol", "")
            daily = item.get("daily", [])
            weekly = item.get("weekly")
            lookback = item.get("lookback", 60)
            min_confidence = item.get("min_confidence", 40)

            if not daily or len(daily) < 20:
                continue

            try:
                from app.engines.cpr.pattern_detector import scan_patterns as engine_scan_patterns, pattern_to_dict

                daily_df = _to_df(daily)
                current_price = float(daily_df.iloc[-1]["close"])

                # Scan daily patterns
                all_patterns = []
                detected = engine_scan_patterns(
                    daily_df,
                    lookback=lookback,
                    min_confidence=min_confidence,
                )
                for p in detected:
                    d = pattern_to_dict(p)
                    d["timeframe"] = "daily"
                    all_patterns.append(d)

                # Scan weekly patterns if provided
                if weekly:
                    weekly_df = _to_df(weekly)
                    if len(weekly_df) >= 20:
                        weekly_detected = engine_scan_patterns(
                            weekly_df,
                            lookback=min(lookback, len(weekly_df)),
                            swing_window=3,
                            min_confidence=min_confidence,
                        )
                        for p in weekly_detected:
                            d = pattern_to_dict(p)
                            d["timeframe"] = "weekly"
                            all_patterns.append(d)

                # Sort by confidence descending
                all_patterns.sort(key=lambda x: x.get("confidence", 0), reverse=True)

                results.append({
                    "symbol": symbol,
                    "current_price": current_price,
                    "pattern_count": len(all_patterns),
                    "patterns": all_patterns,
                })

            except Exception as e:
                logger.debug("Pattern scan failed for %s: %s, using fallback", symbol, e)
                # Fallback: return empty patterns for this symbol
                try:
                    daily_df = _to_df(daily)
                    current_price = float(daily_df.iloc[-1]["close"]) if len(daily_df) > 0 else 0
                    results.append({
                        "symbol": symbol,
                        "current_price": current_price,
                        "pattern_count": 0,
                        "patterns": [],
                    })
                except Exception:
                    pass

        return _make_response(results)

    except Exception as exc:
        logger.exception("Pattern scan error")
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /api/scanner/vcp ──────────────────────────────────────────────────

@router.post("/vcp")
async def scan_vcp(
    request: Request,
    user: dict = Depends(get_current_user),
) -> dict:
    """
    VCP (Volatility Contraction Pattern) scan.

    Frontend sends:
        { symbols: [
            { symbol, daily: [...candles], index_change_pct? },
            ...
          ],
          index_change_pct?: float
        }

    Returns:
        { results: [ { symbol, current_price, vcp_score, stage, ... } ] }
    """
    body_data = await request.json()
    results: list[dict] = []

    try:
        items = _parse_symbols_payload(body_data)
        # Top-level index_change_pct (sent by frontend)
        top_index_change = body_data.get("index_change_pct", 0) if isinstance(body_data, dict) else 0

        for item in items:
            if isinstance(item, str):
                continue

            symbol = item.get("symbol", "")
            daily = item.get("daily", [])
            index_change_pct = item.get("index_change_pct", top_index_change)

            if not daily or len(daily) < 50:
                continue

            try:
                from app.engines.cpr.vcp_detector import analyze_vcp, vcp_result_to_dict

                daily_df = _to_df(daily)

                vcp = analyze_vcp(
                    df=daily_df,
                    symbol=symbol,
                    index_change_pct=index_change_pct,
                )
                results.append(vcp_result_to_dict(vcp))

            except Exception as e:
                logger.debug("VCP scan failed for %s: %s, using fallback", symbol, e)
                # Fallback: return basic result
                try:
                    daily_df = _to_df(daily)
                    current_price = float(daily_df.iloc[-1]["close"]) if len(daily_df) > 0 else 0
                    results.append({
                        "symbol": symbol,
                        "current_price": current_price,
                        "vcp_score": 0,
                        "stage": "Not Ready",
                        "vcp_detected": False,
                        "trend_template_pass": False,
                        "price_above_150ma": False,
                        "price_above_200ma": False,
                        "ma150_above_200ma": False,
                        "ma200_rising": False,
                        "pct_from_52w_high": 0,
                        "pct_from_52w_low": 0,
                        "num_contractions": 0,
                        "contractions": [],
                        "volume_declining": False,
                        "tightening": False,
                        "pivot_price": current_price,
                        "pivot_distance_pct": 0,
                        "pivot_tight_range_pct": 0,
                        "rs_vs_index": 0,
                        "notes": [f"VCP analysis fallback: {e}"],
                    })
                except Exception:
                    pass

        # Sort by vcp_score descending
        results.sort(key=lambda r: r.get("vcp_score", 0), reverse=True)
        return _make_response(results)

    except Exception as exc:
        logger.exception("VCP scan error")
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /api/scanner/nr-squeeze ───────────────────────────────────────────

@router.post("/nr-squeeze")
async def scan_nr_squeeze(
    request: Request,
    user: dict = Depends(get_current_user),
) -> dict:
    """
    NR4/NR7 + Inside Bar + BB Squeeze scan.

    Frontend sends:
        { symbols: [
            { symbol, daily: [...candles] },
            ...
        ] }

    Returns:
        { results: [ { symbol, current_price, is_nr4, is_nr7, squeeze_score, ... } ] }
    """
    body_data = await request.json()
    results: list[dict] = []

    try:
        items = _parse_symbols_payload(body_data)

        for item in items:
            if isinstance(item, str):
                continue

            symbol = item.get("symbol", "")
            daily = item.get("daily", [])

            if not daily or len(daily) < 10:
                continue

            try:
                from app.engines.cpr.nr_squeeze import analyze_nr_squeeze, nr_result_to_dict

                daily_df = _to_df(daily)

                nr = analyze_nr_squeeze(
                    df=daily_df,
                    symbol=symbol,
                )
                results.append(nr_result_to_dict(nr))

            except Exception as e:
                logger.debug("NR squeeze failed for %s: %s, using fallback", symbol, e)
                try:
                    daily_df = _to_df(daily)
                    current_price = float(daily_df.iloc[-1]["close"]) if len(daily_df) > 0 else 0
                    last_bar = daily[-1]
                    bar_high = float(last_bar.get("high", current_price))
                    bar_low = float(last_bar.get("low", current_price))
                    bar_range = bar_high - bar_low
                    results.append({
                        "symbol": symbol,
                        "current_price": current_price,
                        "is_nr4": False,
                        "is_nr7": False,
                        "is_inside_bar": False,
                        "nr_bar_high": bar_high,
                        "nr_bar_low": bar_low,
                        "nr_bar_range": round(bar_range, 2),
                        "nr_bar_range_pct": round(bar_range / current_price * 100, 2) if current_price else 0,
                        "avg_range_7d": 0,
                        "bb_squeeze": False,
                        "bb_width_pct": 0,
                        "bb_width_percentile": 50,
                        "direction": "neutral",
                        "close_position": 0.5,
                        "trend_bias": "flat",
                        "volume_signal": "normal",
                        "buy_above": bar_high,
                        "sell_below": bar_low,
                        "long_sl": bar_low,
                        "short_sl": bar_high,
                        "long_target": round(bar_high + bar_range * 2, 2),
                        "short_target": round(bar_low - bar_range * 2, 2),
                        "squeeze_score": 0,
                        "squeeze_type": "None",
                        "notes": [f"NR analysis fallback: {e}"],
                    })
                except Exception:
                    pass

        # Sort by squeeze_score descending
        results.sort(key=lambda r: r.get("squeeze_score", 0), reverse=True)
        return _make_response(results)

    except Exception as exc:
        logger.exception("NR squeeze scan error")
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /api/scanner/volume-profile ───────────────────────────────────────

@router.post("/volume-profile")
async def scan_volume_profile(
    request: Request,
    user: dict = Depends(get_current_user),
) -> dict:
    """
    Volume Profile scan.

    Frontend sends:
        { symbols: [
            { symbol, daily: [...candles], lookback?: 30 },
            ...
        ] }

    Returns:
        { results: [ { symbol, current_price, poc, vah, val, signal, ... } ] }
    """
    body_data = await request.json()
    results: list[dict] = []

    try:
        items = _parse_symbols_payload(body_data)

        for item in items:
            if isinstance(item, str):
                continue

            symbol = item.get("symbol", "")
            daily = item.get("daily", [])
            lookback = item.get("lookback", 30)

            if not daily or len(daily) < 10:
                continue

            try:
                from app.engines.cpr.volume_profile import analyze_volume_profile, vp_result_to_dict
                from app.engines.cpr.cpr_calculator import compute_cpr_level

                daily_df = _to_df(daily)
                current_price = float(daily_df.iloc[-1]["close"])

                # Compute CPR pivot for confluence check
                if len(daily_df) >= 2:
                    prev = daily_df.iloc[-2]
                    cpr = compute_cpr_level(
                        date=prev.get("date", pd.Timestamp.now()),
                        high=float(prev["high"]),
                        low=float(prev["low"]),
                        close=float(prev["close"]),
                    )
                    cpr_pivot = cpr.pivot
                    cpr_tc = cpr.tc
                    cpr_bc = cpr.bc
                else:
                    cpr_pivot = cpr_tc = cpr_bc = 0

                vp = analyze_volume_profile(
                    df=daily_df,
                    symbol=symbol,
                    lookback=lookback,
                    cpr_pivot=cpr_pivot,
                    cpr_tc=cpr_tc,
                    cpr_bc=cpr_bc,
                )
                results.append(vp_result_to_dict(vp))

            except Exception as e:
                logger.debug("Volume profile failed for %s: %s, using fallback", symbol, e)
                try:
                    daily_df = _to_df(daily)
                    current_price = float(daily_df.iloc[-1]["close"]) if len(daily_df) > 0 else 0
                    results.append({
                        "symbol": symbol,
                        "current_price": current_price,
                        "poc": current_price,
                        "vah": current_price,
                        "val": current_price,
                        "price_vs_profile": "at_poc",
                        "distance_to_poc_pct": 0,
                        "total_volume": 0,
                        "profile_high": current_price,
                        "profile_low": current_price,
                        "poc_near_cpr_pivot": False,
                        "vah_near_cpr_tc": False,
                        "val_near_cpr_bc": False,
                        "signal": "neutral",
                        "signal_strength": 0,
                        "notes": [f"Volume profile fallback: {e}"],
                    })
                except Exception:
                    pass

        # Sort by signal_strength descending
        results.sort(key=lambda r: r.get("signal_strength", 0), reverse=True)
        return _make_response(results)

    except Exception as exc:
        logger.exception("Volume profile scan error")
        raise HTTPException(status_code=500, detail=str(exc))


# ── POST /api/scanner/swing ────────────────────────────────────────────────

@router.post("/swing")
async def scan_swing(
    request: Request,
    user: dict = Depends(get_current_user),
) -> dict:
    """
    Swing signal detection scan.

    Frontend sends:
        { symbols: [
            { symbol, daily: [...candles] },
            ...
        ] }

    Returns:
        {
          count: N,
          summary: { total, bullish, bearish },
          strategies: {
            "10D_BO": [...signals],
            "50D_BO": [...signals],
            "REVERSAL": [...signals],
            "CHANNEL_BO": [...signals],
          }
        }
    """
    body_data = await request.json()

    strategies: dict[str, list[dict]] = {
        "10D_BO": [],
        "50D_BO": [],
        "REVERSAL": [],
        "CHANNEL_BO": [],
    }

    try:
        items = _parse_symbols_payload(body_data)

        for item in items:
            if isinstance(item, str):
                continue

            symbol = item.get("symbol", "")
            daily = item.get("daily", [])

            if not daily or len(daily) < 15:
                continue

            try:
                from app.engines.cpr.swing_detector import scan_swing_spectrum, swing_signal_to_dict

                daily_df = _to_df(daily)

                signals = scan_swing_spectrum(daily_df, symbol)
                for sig in signals:
                    d = swing_signal_to_dict(sig)
                    strategy_key = d.get("strategy", "")
                    if strategy_key in strategies:
                        strategies[strategy_key].append(d)

            except Exception as e:
                logger.debug("Swing scan failed for %s: %s", symbol, e)
                # Fallback: basic breakout detection
                try:
                    daily_df = _to_df(daily)
                    if len(daily_df) >= 11:
                        current = daily_df.iloc[-1]
                        close = float(current["close"])
                        prev_close = float(daily_df.iloc[-2]["close"])
                        change_pct = ((close - prev_close) / prev_close * 100) if prev_close else 0
                        high_10d = float(daily_df["high"].iloc[-11:-1].max())
                        low_10d = float(daily_df["low"].iloc[-11:-1].min())

                        if close > high_10d:
                            strategies["10D_BO"].append({
                                "symbol": symbol,
                                "strategy": "10D_BO",
                                "signal": "BULL",
                                "current_price": round(close, 2),
                                "change_pct": round(change_pct, 2),
                                "trigger_date": str(current.get("date", ""))[:10],
                                "breakout_level": round(high_10d, 2),
                                "stop_loss": round(float(current["low"]), 2),
                                "target": round(close + (close - float(current["low"])) * 2, 2),
                                "strength": 50,
                                "notes": f"Broke above 10-day high {high_10d:.2f}",
                            })
                        elif close < low_10d:
                            strategies["10D_BO"].append({
                                "symbol": symbol,
                                "strategy": "10D_BO",
                                "signal": "BEAR",
                                "current_price": round(close, 2),
                                "change_pct": round(change_pct, 2),
                                "trigger_date": str(current.get("date", ""))[:10],
                                "breakout_level": round(low_10d, 2),
                                "stop_loss": round(float(current["high"]), 2),
                                "target": round(close - (float(current["high"]) - close) * 2, 2),
                                "strength": 50,
                                "notes": f"Broke below 10-day low {low_10d:.2f}",
                            })
                except Exception:
                    pass

        # Flatten all signals for count/summary
        all_signals = []
        for signals in strategies.values():
            all_signals.extend(signals)

        bullish = sum(1 for s in all_signals if s.get("signal") == "BULL")
        bearish = sum(1 for s in all_signals if s.get("signal") == "BEAR")

        return {
            "count": len(all_signals),
            "summary": {
                "total": len(all_signals),
                "bullish": bullish,
                "bearish": bearish,
            },
            "strategies": strategies,
        }

    except Exception as exc:
        logger.exception("Swing scan error")
        raise HTTPException(status_code=500, detail=str(exc))
