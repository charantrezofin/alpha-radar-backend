"""
Signal Validation Tracker — services for the 15-day shadow-mode experiment.

Logs every signal fire to Supabase (signal_fires table) and provides outcome-
check routines for the scheduled jobs. Outcomes are written to signal_outcomes
at +15m / +1h / EOD / next-day-EOD horizons.

This module is import-safe: if Supabase isn't configured, log_signal_fire
silently no-ops so callers don't crash.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Optional

from kiteconnect import KiteConnect

from app.config import settings
from app.core.supabase_client import get_supabase_client

logger = logging.getLogger("alpha_radar.signal_validator")

# ── Configuration ───────────────────────────────────────────────────────────
HORIZONS = ["15m", "1h", "eod", "next_day_eod"]

HORIZON_MINUTES = {
    "15m": 15,
    "1h": 60,
}

# Asymmetric win/loss thresholds — covers ~0.1-0.15% slippage + brokerage
WIN_LOSS_THRESHOLDS = {
    "15m":          {"win": 0.5, "loss": -0.3},
    "1h":           {"win": 1.0, "loss": -0.5},
    "eod":          {"win": 1.5, "loss": -0.8},
    "next_day_eod": {"win": 2.0, "loss": -1.0},
}

# Don't log the same (symbol, signal_type, direction) within this window
DEDUPE_COOLDOWN_MINUTES = 30

# Token cache to avoid repeated kite.ltp lookups
_token_cache: dict[str, int] = {}


# ── Public: log a signal fire ───────────────────────────────────────────────

def log_signal_fire(
    *,
    symbol: str,
    signal_type: str,
    trigger_price: float,
    strength: float,
    direction: str,
    confidence: str | None = None,
    category: str = "stock",
    metadata: dict | None = None,
    context: dict | None = None,
) -> Optional[str]:
    """
    Persist a signal fire and seed PENDING outcome rows for all horizons.

    Returns the fire_id (UUID string) on success, None if Supabase isn't
    configured, the signal direction is NEUTRAL, or the fire was deduped.
    """
    if direction not in ("BULLISH", "BEARISH"):
        return None  # nothing to validate

    try:
        sb = get_supabase_client()
    except Exception:
        logger.debug("signal_validator: Supabase not configured — skipping log")
        return None

    # Dedupe: skip if same setup logged recently
    try:
        cutoff = (
            datetime.now(settings.TIMEZONE)
            - timedelta(minutes=DEDUPE_COOLDOWN_MINUTES)
        ).isoformat()
        existing = (
            sb.table("signal_fires")
            .select("id")
            .eq("symbol", symbol)
            .eq("signal_type", signal_type)
            .eq("direction", direction)
            .gte("fired_at", cutoff)
            .limit(1)
            .execute()
        )
        if existing.data:
            return None
    except Exception:
        # If dedupe check fails, still proceed (better to over-log than miss)
        logger.debug("signal_validator: dedupe check failed", exc_info=True)

    fired_at = datetime.now(settings.TIMEZONE)

    try:
        result = (
            sb.table("signal_fires")
            .insert(
                {
                    "signal_type": signal_type,
                    "symbol": symbol,
                    "category": category,
                    "fired_at": fired_at.isoformat(),
                    "trigger_price": trigger_price,
                    "strength": strength,
                    "direction": direction,
                    "confidence": confidence,
                    "metadata": metadata or {},
                    "context": context or {},
                }
            )
            .execute()
        )
    except Exception:
        logger.exception(
            "signal_validator: insert failed for %s/%s", symbol, signal_type
        )
        return None

    if not result.data:
        return None

    fire_id = result.data[0]["id"]

    # Seed pending outcome rows for each horizon
    try:
        sb.table("signal_outcomes").insert(
            [
                {"signal_fire_id": fire_id, "horizon": h, "status": "PENDING"}
                for h in HORIZONS
            ]
        ).execute()
    except Exception:
        logger.exception(
            "signal_validator: failed to seed outcomes for %s", fire_id
        )

    logger.info(
        "[signal_validator] logged fire %s for %s %s %s (strength=%.1f)",
        fire_id, symbol, signal_type, direction, strength,
    )
    return fire_id


# ── Market context snapshot ─────────────────────────────────────────────────

def compute_market_context() -> dict:
    """
    Best-effort snapshot of market state. Returns whatever's available.
    For v1 we just capture pulse regime; breadth/VIX added in a later iteration.
    """
    ctx: dict[str, Any] = {}
    try:
        # Lazy import — pulse cache may not exist on all deployments
        from app.caches import market_pulse_cache  # type: ignore
        pulse = getattr(market_pulse_cache, "current_pulse", None)
        if pulse:
            ctx["regime"] = pulse.get("signal", "NEUTRAL")
    except Exception:
        pass
    return ctx


# ── Outcome checks ──────────────────────────────────────────────────────────

def _resolve_token(kite: KiteConnect, symbol: str) -> Optional[int]:
    """Resolve NSE symbol to instrument token, cached in-process."""
    if symbol in _token_cache:
        return _token_cache[symbol]
    try:
        ltp_data = kite.ltp([f"NSE:{symbol}"])
        token = ltp_data[f"NSE:{symbol}"]["instrument_token"]
        _token_cache[symbol] = token
        return token
    except Exception:
        return None


def _classify_outcome(direction: str, return_pct: float, horizon: str) -> str:
    """Classify a signed return as WIN / LOSS / FLAT for the given horizon."""
    thresholds = WIN_LOSS_THRESHOLDS[horizon]
    # For BEARISH signals a price drop is a "win" — flip the sign
    signed_return = return_pct if direction == "BULLISH" else -return_pct
    if signed_return >= thresholds["win"]:
        return "WIN"
    if signed_return <= thresholds["loss"]:
        return "LOSS"
    return "FLAT"


def _horizon_target_dt(fired_at: datetime, horizon: str) -> datetime:
    """When should this horizon be evaluated?"""
    if horizon in HORIZON_MINUTES:
        target = fired_at + timedelta(minutes=HORIZON_MINUTES[horizon])
        # Cap intraday horizons at market close
        market_close = fired_at.replace(hour=15, minute=30, second=0, microsecond=0)
        if target > market_close:
            target = market_close
        return target
    if horizon == "eod":
        return fired_at.replace(hour=15, minute=30, second=0, microsecond=0)
    if horizon == "next_day_eod":
        return (fired_at + timedelta(days=1)).replace(
            hour=15, minute=30, second=0, microsecond=0
        )
    raise ValueError(f"unknown horizon: {horizon}")


def _fetch_window(
    kite: KiteConnect,
    token: int,
    from_dt: datetime,
    to_dt: datetime,
    interval: str = "5minute",
) -> list[dict]:
    """Fetch OHLC candles between from_dt and to_dt."""
    try:
        return kite.historical_data(token, from_dt, to_dt, interval) or []
    except Exception:
        logger.debug(
            "signal_validator: historical_data failed for token=%s", token,
            exc_info=True,
        )
        return []


def check_outcomes_for_horizon(horizon: str, kite: KiteConnect) -> dict:
    """
    Find PENDING outcomes for this horizon whose target time has passed,
    fetch price data, compute returns, and update rows.

    Returns a summary dict {checked, updated, skipped, errors}.
    """
    if horizon not in HORIZONS:
        raise ValueError(f"unknown horizon: {horizon}")

    summary = {"checked": 0, "updated": 0, "skipped": 0, "errors": 0}

    try:
        sb = get_supabase_client()
    except Exception:
        logger.debug("signal_validator: Supabase not configured")
        return summary

    try:
        result = (
            sb.table("signal_outcomes")
            .select(
                "id, horizon, signal_fire_id, "
                "signal_fires(symbol, fired_at, direction, trigger_price)"
            )
            .eq("horizon", horizon)
            .eq("status", "PENDING")
            .limit(500)
            .execute()
        )
        rows = result.data or []
    except Exception:
        logger.exception(
            "signal_validator: failed to fetch pending outcomes for %s", horizon
        )
        return summary

    now = datetime.now(settings.TIMEZONE)

    for row in rows:
        summary["checked"] += 1
        fire = row.get("signal_fires") or {}
        symbol = fire.get("symbol")
        direction = fire.get("direction")
        trigger_price = fire.get("trigger_price")
        fired_at_str = fire.get("fired_at")

        if not (symbol and direction and trigger_price and fired_at_str):
            summary["skipped"] += 1
            continue

        try:
            fired_at = datetime.fromisoformat(
                fired_at_str.replace("Z", "+00:00")
            ).astimezone(settings.TIMEZONE)
        except Exception:
            summary["errors"] += 1
            continue

        target_dt = _horizon_target_dt(fired_at, horizon)
        if now < target_dt:
            summary["skipped"] += 1
            continue

        token = _resolve_token(kite, symbol)
        if not token:
            summary["errors"] += 1
            continue

        # Window to query for entry/exit/high/low
        if horizon == "next_day_eod":
            from_dt = (fired_at + timedelta(days=1)).replace(
                hour=9, minute=15, second=0, microsecond=0
            )
            to_dt = from_dt.replace(hour=15, minute=30)
        elif horizon == "eod":
            from_dt = fired_at + timedelta(minutes=1)
            to_dt = fired_at.replace(hour=15, minute=30, second=0, microsecond=0)
        else:  # 15m, 1h
            from_dt = fired_at + timedelta(minutes=1)
            to_dt = target_dt

        if to_dt <= from_dt:
            summary["skipped"] += 1
            continue

        candles = _fetch_window(kite, token, from_dt, to_dt, "5minute")
        if not candles:
            summary["errors"] += 1
            continue

        entry_price = candles[0]["open"]
        exit_price = candles[-1]["close"]
        high_during = max(c["high"] for c in candles)
        low_during = min(c["low"] for c in candles)
        return_pct = (
            round((exit_price - entry_price) / entry_price * 100, 3)
            if entry_price > 0
            else 0
        )

        if direction == "BULLISH":
            mfe = round((high_during - entry_price) / entry_price * 100, 3)
            mae = round((low_during - entry_price) / entry_price * 100, 3)
        else:  # BEARISH — favorable means price went DOWN
            mfe = round((entry_price - low_during) / entry_price * 100, 3)
            mae = round((entry_price - high_during) / entry_price * 100, 3)

        status = _classify_outcome(direction, return_pct, horizon)

        try:
            sb.table("signal_outcomes").update(
                {
                    "status": status,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "high_during": high_during,
                    "low_during": low_during,
                    "return_pct": return_pct,
                    "mfe_pct": mfe,
                    "mae_pct": mae,
                    "checked_at": now.isoformat(),
                }
            ).eq("id", row["id"]).execute()
            summary["updated"] += 1
        except Exception:
            logger.exception(
                "signal_validator: update failed for outcome %s", row["id"]
            )
            summary["errors"] += 1

    if summary["updated"] > 0 or summary["errors"] > 0:
        logger.info(
            "[signal_validator] horizon=%s checked=%d updated=%d skipped=%d errors=%d",
            horizon, summary["checked"], summary["updated"],
            summary["skipped"], summary["errors"],
        )

    return summary


# ── Stats query (used by /api/admin/signal-stats) ───────────────────────────

def get_signal_stats(days: int = 15) -> dict:
    """
    Aggregate win/loss stats per signal_type per horizon over the last N days.
    """
    summary: dict = {"days": days, "by_signal_type": {}}

    try:
        sb = get_supabase_client()
    except Exception:
        return summary

    cutoff = (
        datetime.now(settings.TIMEZONE) - timedelta(days=days)
    ).isoformat()

    try:
        rows_result = (
            sb.table("signal_outcomes")
            .select(
                "status, horizon, return_pct, mfe_pct, mae_pct, "
                "signal_fires!inner(signal_type, fired_at, direction)"
            )
            .gte("signal_fires.fired_at", cutoff)
            .limit(10000)
            .execute()
        )
        rows = rows_result.data or []
    except Exception:
        logger.exception("signal_validator: stats query failed")
        return summary

    agg: dict = {}
    for r in rows:
        fire = r.get("signal_fires") or {}
        st = fire.get("signal_type") or "UNKNOWN"
        h = r.get("horizon")
        status = r.get("status")
        ret = r.get("return_pct") or 0
        mfe = r.get("mfe_pct") or 0
        mae = r.get("mae_pct") or 0

        bucket = agg.setdefault(st, {}).setdefault(
            h,
            {
                "total": 0, "resolved": 0, "pending": 0,
                "wins": 0, "losses": 0, "flats": 0,
                "sum_return": 0.0, "sum_mfe": 0.0, "sum_mae": 0.0,
            },
        )
        bucket["total"] += 1
        if status in ("WIN", "LOSS", "FLAT"):
            bucket["resolved"] += 1
            bucket["sum_return"] += ret
            bucket["sum_mfe"] += mfe
            bucket["sum_mae"] += mae
            if status == "WIN":
                bucket["wins"] += 1
            elif status == "LOSS":
                bucket["losses"] += 1
            else:
                bucket["flats"] += 1
        else:
            bucket["pending"] += 1

    for st, horizons in agg.items():
        out_h = {}
        for h, b in horizons.items():
            r_count = b["resolved"]
            wl_count = b["wins"] + b["losses"]
            out_h[h] = {
                "total": b["total"],
                "resolved": r_count,
                "pending": b["pending"],
                "wins": b["wins"],
                "losses": b["losses"],
                "flats": b["flats"],
                "win_rate": round(b["wins"] / r_count * 100, 1) if r_count > 0 else None,
                "win_rate_excl_flat": round(b["wins"] / wl_count * 100, 1) if wl_count > 0 else None,
                "avg_return_pct": round(b["sum_return"] / r_count, 2) if r_count > 0 else None,
                "avg_mfe_pct": round(b["sum_mfe"] / r_count, 2) if r_count > 0 else None,
                "avg_mae_pct": round(b["sum_mae"] / r_count, 2) if r_count > 0 else None,
            }
        summary["by_signal_type"][st] = out_h

    return summary


# ── Daily digest (sent via Telegram) ────────────────────────────────────────

def _format_pct(v: Optional[float], with_sign: bool = False) -> str:
    if v is None:
        return "—"
    sign = "+" if (with_sign and v >= 0) else ""
    return f"{sign}{v:.1f}%"


def _signal_distinct_fires(stats: dict) -> int:
    """Count distinct fires (each fire shows up under all 4 horizons in stats)."""
    total = 0
    for horizons in stats.get("by_signal_type", {}).values():
        if "15m" in horizons:
            total += horizons["15m"]["total"]
    return total


def build_daily_digest_text() -> str:
    """
    Compose the HTML body for the daily Telegram digest.
    Compares yesterday (1d) vs the trailing week (7d) at the 1h horizon.
    """
    today = datetime.now(settings.TIMEZONE).strftime("%d %b %Y")
    day_stats = get_signal_stats(days=1)
    week_stats = get_signal_stats(days=7)

    day_fires = _signal_distinct_fires(day_stats)
    week_fires = _signal_distinct_fires(week_stats)

    lines: list[str] = []
    lines.append("<b>Signal Validator — Daily Digest</b>")
    lines.append(f"<i>{today}</i>")
    lines.append("")

    if day_fires == 0:
        lines.append("<b>No signals fired in the last 24h.</b>")
        lines.append(
            "Either market was closed, validator hadn't been deployed, "
            "or no engine produced a STRONG/MODERATE setup."
        )
        lines.append(f"\n7-day total: <b>{week_fires}</b> fires")
        lines.append("\n#AlphaRadar #SignalValidator")
        return "\n".join(lines)

    lines.append(f"<b>Yesterday's activity:</b> {day_fires} distinct fires")
    lines.append("")

    # Per-engine performance at 1h horizon
    rows: list[tuple[str, Optional[float], Optional[float], int, int]] = []
    for st, horizons in day_stats.get("by_signal_type", {}).items():
        h1 = horizons.get("1h")
        if not h1:
            continue
        rows.append((
            st,
            h1.get("win_rate"),
            h1.get("avg_return_pct"),
            h1.get("resolved", 0),
            h1.get("total", 0),
        ))

    # Sort by win rate desc, with None last
    rows.sort(key=lambda r: (r[1] is None, -(r[1] or 0)))

    if rows:
        lines.append("<b>1-hour horizon, by signal type:</b>")
        for st, wr, ret, resolved, total in rows:
            wr_s = _format_pct(wr)
            ret_s = _format_pct(ret, with_sign=True)
            sample = f"{resolved}/{total}" if resolved < total else f"{total}"
            lines.append(f"  {st}: <b>{wr_s}</b> win, {ret_s} avg ({sample})")
        lines.append("")

    # 7-day rollup at 1h
    week_resolved = 0
    week_wins = 0
    for horizons in week_stats.get("by_signal_type", {}).values():
        h1 = horizons.get("1h")
        if h1:
            week_resolved += h1.get("resolved", 0)
            week_wins += h1.get("wins", 0)
    week_winrate = (week_wins / week_resolved * 100) if week_resolved > 0 else 0
    lines.append(
        f"<b>7-day rollup (1h):</b> {week_fires} fires, "
        f"{week_resolved} resolved, "
        f"<b>{week_winrate:.1f}%</b> win rate"
    )

    lines.append("")
    lines.append("Full dashboard: /admin/signals")
    lines.append("")
    lines.append("#AlphaRadar #SignalValidator")
    return "\n".join(lines)


async def send_daily_digest() -> dict:
    """
    Build and send the daily Telegram digest.
    Returns a status dict {sent, message_chars, telegram_ok}.
    """
    from app.core.telegram import send_message  # local import — avoids circular

    text = build_daily_digest_text()
    result = await send_message(text)
    return {
        "sent": bool(result and result.get("ok")),
        "message_chars": len(text),
        "telegram_ok": bool(result and result.get("ok")),
    }
