"""
Squeeze Monitor -- detects compression / coiling in index options before big moves.

6 detection components:
1. Range Compression  : today's range vs expected range by time-of-day
2. OI Buildup Near ATM: heavy OI at strikes near spot = squeeze fuel
3. OI Change Rate     : fast OI drop on one side = positions closing = squeeze starting
4. PCR Trend          : rising/falling PCR over recent snapshots
5. Spot Move Detection: sharp spot move = squeeze firing
6. Net OI Direction   : which side is dominating OI changes since open

Squeeze phases: calm -> building -> ready -> firing

Ported from tradingdesk/apps/gateway/src/routes/squeeze.routes.ts
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional


# ── Types ────────────────────────────────────────────────────────────────────


@dataclass
class OISnapshot:
    time: int               # timestamp ms
    time_str: str           # "09:15", "09:20" etc
    spot: float
    total_call_oi: int
    total_put_oi: int
    pcr: float
    atm_call_oi: int
    atm_put_oi: int
    near_atm_call_oi: int  # Sum of CE OI at ATM +/- 2 strikes
    near_atm_put_oi: int
    spot_high: float
    spot_low: float
    range: float            # high - low so far


@dataclass
class SqueezeAlert:
    type: Literal[
        "COMPRESSION", "OI_BUILDUP", "OI_UNWINDING",
        "VOLUME_SPIKE", "SQUEEZE_READY", "SQUEEZE_FIRING",
    ]
    severity: Literal["HIGH", "MEDIUM", "LOW"]
    direction: Literal["BULLISH", "BEARISH", "NEUTRAL"]
    message: str
    value: float
    threshold: float


@dataclass
class SqueezeResult:
    alerts: List[SqueezeAlert]
    squeeze_score: int              # 0-100
    squeeze_phase: str              # "calm", "building", "ready", "firing"
    range_compression: float
    oi_buildup_ratio: float
    pcr_trend: str                  # "rising", "falling", "stable"
    net_oi_direction: str           # "bullish", "bearish", "neutral"


# ── Helpers ──────────────────────────────────────────────────────────────────


def _format_oi(v: float) -> str:
    abs_v = abs(v)
    if abs_v >= 10_000_000:
        return f"{v / 10_000_000:.1f}Cr"
    if abs_v >= 100_000:
        return f"{v / 100_000:.1f}L"
    if abs_v >= 1_000:
        return f"{v / 1_000:.1f}K"
    return str(round(v))


# ── Squeeze Detection Logic ─────────────────────────────────────────────────


def _detect_squeeze(snapshots: List[OISnapshot]) -> SqueezeResult:
    """
    Analyse a series of OI snapshots and return squeeze status.
    """
    alerts: List[SqueezeAlert] = []
    squeeze_score = 0

    # Need at least 2 snapshots for comparison
    if len(snapshots) < 2:
        return SqueezeResult(
            alerts=[SqueezeAlert(
                type="COMPRESSION", severity="LOW", direction="NEUTRAL",
                message="Gathering data... need more snapshots",
                value=0, threshold=0,
            )],
            squeeze_score=0,
            squeeze_phase="calm",
            range_compression=0,
            oi_buildup_ratio=0,
            pcr_trend="stable",
            net_oi_direction="neutral",
        )

    latest = snapshots[-1]
    prev = snapshots[-2]
    first = snapshots[0]

    # ── 1. Range Compression ─────────────────────────────────────────────
    minutes_since_open = (latest.time - first.time) / 60_000
    expected_range = latest.spot * 0.015 * min(1, minutes_since_open / 375)  # ~1.5% full day range
    actual_range = latest.range
    range_compression = actual_range / expected_range if expected_range > 0 else 1

    if range_compression < 0.4 and minutes_since_open > 60:
        squeeze_score += 25
        alerts.append(SqueezeAlert(
            type="COMPRESSION", severity="HIGH", direction="NEUTRAL",
            message=(
                f"Range compressed: {actual_range:.0f} pts "
                f"({range_compression * 100:.0f}% of expected) -- coiling tight"
            ),
            value=range_compression, threshold=0.4,
        ))
    elif range_compression < 0.6 and minutes_since_open > 45:
        squeeze_score += 15
        alerts.append(SqueezeAlert(
            type="COMPRESSION", severity="MEDIUM", direction="NEUTRAL",
            message=(
                f"Range narrow: {actual_range:.0f} pts "
                f"({range_compression * 100:.0f}% of expected)"
            ),
            value=range_compression, threshold=0.6,
        ))

    # ── 2. OI Buildup Near ATM ───────────────────────────────────────────
    total_oi = latest.total_call_oi + latest.total_put_oi
    near_atm_oi = latest.near_atm_call_oi + latest.near_atm_put_oi
    oi_buildup_ratio = near_atm_oi / total_oi if total_oi > 0 else 0

    if oi_buildup_ratio > 0.35:
        squeeze_score += 20
        alerts.append(SqueezeAlert(
            type="OI_BUILDUP", severity="HIGH", direction="NEUTRAL",
            message=(
                f"Heavy OI buildup near ATM: {oi_buildup_ratio * 100:.0f}% of total OI "
                f"within +/-2 strikes -- squeeze fuel loaded"
            ),
            value=oi_buildup_ratio, threshold=0.35,
        ))
    elif oi_buildup_ratio > 0.25:
        squeeze_score += 10
        alerts.append(SqueezeAlert(
            type="OI_BUILDUP", severity="MEDIUM", direction="NEUTRAL",
            message=(
                f"Moderate OI near ATM: {oi_buildup_ratio * 100:.0f}% of total OI "
                f"within +/-2 strikes"
            ),
            value=oi_buildup_ratio, threshold=0.25,
        ))

    # ── 3. OI Change Rate (Unwinding Detection) ─────────────────────────
    call_oi_change = latest.total_call_oi - prev.total_call_oi
    put_oi_change = latest.total_put_oi - prev.total_put_oi

    if abs(call_oi_change) > latest.total_call_oi * 0.02:
        if call_oi_change < 0:
            squeeze_score += 15
            alerts.append(SqueezeAlert(
                type="OI_UNWINDING", severity="HIGH", direction="BULLISH",
                message=(
                    f"CE OI dropping fast ({_format_oi(call_oi_change)}) "
                    f"-- bears covering, bullish squeeze"
                ),
                value=abs(call_oi_change),
                threshold=latest.total_call_oi * 0.02,
            ))
        else:
            squeeze_score += 5
            alerts.append(SqueezeAlert(
                type="OI_BUILDUP", severity="LOW", direction="BEARISH",
                message=(
                    f"CE OI building ({_format_oi(call_oi_change)}) "
                    f"-- new resistance being written"
                ),
                value=call_oi_change, threshold=0,
            ))

    if abs(put_oi_change) > latest.total_put_oi * 0.02:
        if put_oi_change < 0:
            squeeze_score += 15
            alerts.append(SqueezeAlert(
                type="OI_UNWINDING", severity="HIGH", direction="BEARISH",
                message=(
                    f"PE OI dropping fast ({_format_oi(put_oi_change)}) "
                    f"-- bulls exiting, bearish squeeze"
                ),
                value=abs(put_oi_change),
                threshold=latest.total_put_oi * 0.02,
            ))
        else:
            squeeze_score += 5
            alerts.append(SqueezeAlert(
                type="OI_BUILDUP", severity="LOW", direction="BULLISH",
                message=(
                    f"PE OI building ({_format_oi(put_oi_change)}) "
                    f"-- new support being written"
                ),
                value=put_oi_change, threshold=0,
            ))

    # ── 4. PCR Trend ─────────────────────────────────────────────────────
    pcr_trend = "stable"
    if len(snapshots) >= 3:
        recent_pcrs = [s.pcr for s in snapshots[-5:]]
        pcr_start = recent_pcrs[0]
        pcr_end = recent_pcrs[-1]
        pcr_change = pcr_end - pcr_start

        if pcr_change > 0.1:
            pcr_trend = "rising"
            squeeze_score += 5
            alerts.append(SqueezeAlert(
                type="OI_BUILDUP", severity="LOW", direction="BULLISH",
                message=(
                    f"PCR rising ({pcr_start:.2f} -> {pcr_end:.2f}) "
                    f"-- PE writing increasing, bullish"
                ),
                value=pcr_change, threshold=0.1,
            ))
        elif pcr_change < -0.1:
            pcr_trend = "falling"
            squeeze_score += 5
            alerts.append(SqueezeAlert(
                type="OI_BUILDUP", severity="LOW", direction="BEARISH",
                message=(
                    f"PCR falling ({pcr_start:.2f} -> {pcr_end:.2f}) "
                    f"-- CE writing increasing, bearish"
                ),
                value=abs(pcr_change), threshold=0.1,
            ))

    # ── 5. Spot Move Detection (is squeeze firing?) ──────────────────────
    spot_change = latest.spot - prev.spot
    spot_change_pct = abs(spot_change / prev.spot) * 100 if prev.spot > 0 else 0

    if spot_change_pct > 0.3:
        squeeze_score += 20
        direction: Literal["BULLISH", "BEARISH"] = "BULLISH" if spot_change > 0 else "BEARISH"
        sign = "+" if spot_change > 0 else ""
        alerts.append(SqueezeAlert(
            type="SQUEEZE_FIRING", severity="HIGH", direction=direction,
            message=(
                f"Squeeze FIRING! Spot moved {sign}{spot_change:.0f} pts "
                f"({spot_change_pct:.1f}%) since last snapshot"
            ),
            value=spot_change_pct, threshold=0.3,
        ))

    # ── 6. Net OI Direction ──────────────────────────────────────────────
    net_call_change = latest.total_call_oi - first.total_call_oi
    net_put_change = latest.total_put_oi - first.total_put_oi
    net_oi_direction = "neutral"
    if net_put_change > net_call_change * 1.3:
        net_oi_direction = "bullish"
    elif net_call_change > net_put_change * 1.3:
        net_oi_direction = "bearish"

    # ── Squeeze Phase ────────────────────────────────────────────────────
    squeeze_score = min(100, squeeze_score)

    if squeeze_score >= 60:
        squeeze_phase = "firing"
    elif squeeze_score >= 40:
        squeeze_phase = "ready"
    elif squeeze_score >= 20:
        squeeze_phase = "building"
    else:
        squeeze_phase = "calm"

    # Add master alert for high scores
    if squeeze_score >= 40:
        master_dir: Literal["BULLISH", "BEARISH", "NEUTRAL"] = (
            "BULLISH" if net_oi_direction == "bullish"
            else ("BEARISH" if net_oi_direction == "bearish" else "NEUTRAL")
        )
        alerts.insert(0, SqueezeAlert(
            type="SQUEEZE_READY", severity="HIGH",
            direction=master_dir,
            message=(
                f"Squeeze {squeeze_phase.upper()}! Score {squeeze_score}/100 "
                f"-- expect 100-150pt move. Direction bias: {net_oi_direction}"
            ),
            value=squeeze_score, threshold=40,
        ))

    return SqueezeResult(
        alerts=alerts,
        squeeze_score=squeeze_score,
        squeeze_phase=squeeze_phase,
        range_compression=range_compression,
        oi_buildup_ratio=oi_buildup_ratio,
        pcr_trend=pcr_trend,
        net_oi_direction=net_oi_direction,
    )


# ── Stateful Monitor ────────────────────────────────────────────────────────


class SqueezeMonitor:
    """
    Maintains per-index OI snapshot history and runs squeeze detection.

    Usage:
        monitor = SqueezeMonitor()
        monitor.take_snapshot("nifty", spot, chain_data, analytics, strike_step)
        result = monitor.get_squeeze_status("nifty")
    """

    MAX_SNAPSHOTS = 200
    MIN_SNAPSHOT_INTERVAL_MS = 120_000  # 2 minutes

    def __init__(self) -> None:
        self._states: Dict[str, _SqueezeState] = {}

    def _get_or_create(self, index: str) -> _SqueezeState:
        today = datetime.now().strftime("%Y-%m-%d")
        if index not in self._states or self._states[index].day_date != today:
            self._states[index] = _SqueezeState(
                index=index, snapshots=[], last_snapshot_time=0, day_date=today,
            )
        return self._states[index]

    def take_snapshot(
        self,
        index: str,
        spot: float,
        chain: List[Any],
        analytics: Any,
        strike_step: float,
    ) -> bool:
        """
        Record an OI snapshot if enough time has passed since the last one.

        Parameters
        ----------
        index : str
            Index key (e.g. "nifty").
        spot : float
            Current spot price.
        chain : list
            Options chain rows (dicts or ChainRow objects with strike, call, put).
        analytics : object
            Must have total_call_oi, total_put_oi, pcr attributes.
        strike_step : float
            Distance between strikes.

        Returns
        -------
        bool
            True if a new snapshot was recorded.
        """
        state = self._get_or_create(index)
        now = int(time.time() * 1000)

        if now - state.last_snapshot_time < self.MIN_SNAPSHOT_INTERVAL_MS:
            return False

        # Find ATM strike
        if not chain:
            return False

        atm = min(chain, key=lambda r: abs(_get_strike(r) - spot))
        atm_strike = _get_strike(atm)

        # Near ATM = +/- 2 strikes
        near_atm = [r for r in chain if abs(_get_strike(r) - atm_strike) <= strike_step * 2]

        near_atm_call_oi = sum(_get_call_oi(r) for r in near_atm)
        near_atm_put_oi = sum(_get_put_oi(r) for r in near_atm)

        # Spot high/low tracking
        prev_highs = [s.spot_high for s in state.snapshots if s.spot_high] + [spot]
        prev_lows = [s.spot_low for s in state.snapshots if s.spot_low] + [spot]
        spot_high = max(prev_highs)
        spot_low = min(prev_lows)

        total_call_oi = getattr(analytics, "total_call_oi", 0) if hasattr(analytics, "total_call_oi") else analytics.get("total_call_oi", 0)
        total_put_oi = getattr(analytics, "total_put_oi", 0) if hasattr(analytics, "total_put_oi") else analytics.get("total_put_oi", 0)
        pcr = getattr(analytics, "pcr", 0) if hasattr(analytics, "pcr") else analytics.get("pcr", 0)

        snapshot = OISnapshot(
            time=now,
            time_str=datetime.now().strftime("%H:%M"),
            spot=spot,
            total_call_oi=total_call_oi,
            total_put_oi=total_put_oi,
            pcr=pcr,
            atm_call_oi=_get_call_oi(atm),
            atm_put_oi=_get_put_oi(atm),
            near_atm_call_oi=near_atm_call_oi,
            near_atm_put_oi=near_atm_put_oi,
            spot_high=spot_high,
            spot_low=spot_low,
            range=spot_high - spot_low,
        )

        state.snapshots.append(snapshot)
        state.last_snapshot_time = now

        # Keep max snapshots
        if len(state.snapshots) > self.MAX_SNAPSHOTS:
            state.snapshots = state.snapshots[-self.MAX_SNAPSHOTS:]

        return True

    def get_squeeze_status(self, index: str) -> SqueezeResult:
        """Run squeeze detection on accumulated snapshots for an index."""
        state = self._get_or_create(index)
        return _detect_squeeze(state.snapshots)

    def get_snapshots(self, index: str, last_n: int = 20) -> List[OISnapshot]:
        """Return the last N snapshots for charting."""
        state = self._get_or_create(index)
        return state.snapshots[-last_n:]

    @property
    def snapshot_count(self) -> Dict[str, int]:
        return {k: len(v.snapshots) for k, v in self._states.items()}


# ── Internal State ───────────────────────────────────────────────────────────


@dataclass
class _SqueezeState:
    index: str
    snapshots: List[OISnapshot]
    last_snapshot_time: int
    day_date: str


# ── Chain row accessor helpers (support both dict and dataclass) ─────────────


def _get_strike(row: Any) -> float:
    if isinstance(row, dict):
        return row.get("strike", 0)
    return getattr(row, "strike", 0)


def _get_call_oi(row: Any) -> int:
    if isinstance(row, dict):
        call = row.get("call")
        if isinstance(call, dict):
            return call.get("oi", 0)
        return getattr(call, "oi", 0) if call else 0
    call = getattr(row, "call", None)
    if call is None:
        return 0
    return getattr(call, "oi", 0) if not isinstance(call, dict) else call.get("oi", 0)


def _get_put_oi(row: Any) -> int:
    if isinstance(row, dict):
        put = row.get("put")
        if isinstance(put, dict):
            return put.get("oi", 0)
        return getattr(put, "oi", 0) if put else 0
    put = getattr(row, "put", None)
    if put is None:
        return 0
    return getattr(put, "oi", 0) if not isinstance(put, dict) else put.get("oi", 0)
