"""
Live Signal Tracker -- tracks PDH/PDL cross and combo surge/sell intraday.

Bullish signals:
  - pdhCrossed : price crossed above PDH (ltp >= pdh)
  - comboSurge : PDH cross AND volume >= 1.5x avg simultaneously

Bearish signals:
  - pdlCrossed : price crossed below PDL (ltp <= pdl)
  - comboSell  : PDL cross AND volume >= 1.5x avg simultaneously

All signals latch (once triggered, stay true for the day).
Resets daily at 8:30am.

Ported from Alpha-Radar-backend/server.js (lines 350-403)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Set


@dataclass
class TickSignal:
    """Result of processing a single tick for a symbol."""
    pdh_crossed: bool = False
    pdh_first_cross: bool = False          # True only the first time
    combo_surge: bool = False
    combo_surge_first: bool = False        # True only the first time
    pdl_crossed: bool = False
    pdl_first_cross: bool = False
    combo_sell: bool = False
    combo_sell_first: bool = False
    first_cross_time: Optional[str] = None
    first_combo_time: Optional[str] = None
    first_pdl_time: Optional[str] = None
    first_combo_sell_time: Optional[str] = None


class LiveSignalTracker:
    """
    Stateful tracker for PDH/PDL cross and combo surge/sell signals.
    Maintains per-symbol latched state for the trading day.
    """

    def __init__(self, on_alert: Optional[Callable[[str, str, Dict[str, Any]], None]] = None) -> None:
        """
        Parameters
        ----------
        on_alert : callable, optional
            Callback fired on new signals: ``on_alert(alert_type, symbol, details)``.
            alert_type is one of "PDH_CROSS", "COMBO_SURGE", "PDL_CROSS", "COMBO_SELL".
        """
        self._on_alert = on_alert

        # Bullish
        self._pdh_crossed: Set[str] = set()
        self._combo_surge: Set[str] = set()
        self._first_cross_time: Dict[str, str] = {}
        self._first_combo_time: Dict[str, str] = {}

        # Bearish
        self._pdl_crossed: Set[str] = set()
        self._combo_sell: Set[str] = set()
        self._first_pdl_time: Dict[str, str] = {}
        self._first_combo_sell_time: Dict[str, str] = {}

        self._last_reset_date: Optional[str] = None

    def _maybe_reset(self) -> None:
        """Reset all state if we've crossed into a new trading day."""
        today = datetime.now().strftime("%Y-%m-%d")
        if self._last_reset_date != today:
            self._pdh_crossed.clear()
            self._combo_surge.clear()
            self._first_cross_time.clear()
            self._first_combo_time.clear()
            self._pdl_crossed.clear()
            self._combo_sell.clear()
            self._first_pdl_time.clear()
            self._first_combo_sell_time.clear()
            self._last_reset_date = today

    def track_tick(
        self,
        symbol: str,
        ltp: float,
        volume: int,
        avg_volume: float,
        pdh: float,
        pdl: float,
        sector: str = "",
    ) -> TickSignal:
        """
        Process a single price tick and update signal state.

        Parameters
        ----------
        symbol : str
            Stock symbol.
        ltp : float
            Last traded price.
        volume : int
            Today's cumulative volume.
        avg_volume : float
            20-day average volume.
        pdh : float
            Previous day high.
        pdl : float
            Previous day low.
        sector : str, optional
            Sector name (passed through to alert callback).

        Returns
        -------
        TickSignal
            Current signal state for this symbol.
        """
        self._maybe_reset()

        vol_ratio = round(volume / avg_volume, 2) if avg_volume > 0 else 0.0
        now_str = datetime.now().strftime("%H:%M:%S")

        result = TickSignal()

        # ── Bullish: PDH cross ──
        if pdh and pdh > 0 and ltp:
            if ltp >= pdh:
                result.pdh_crossed = True
                if symbol not in self._pdh_crossed:
                    self._pdh_crossed.add(symbol)
                    self._first_cross_time[symbol] = now_str
                    result.pdh_first_cross = True
                    if self._on_alert:
                        self._on_alert("PDH_CROSS", symbol, {
                            "ltp": ltp, "pdh": pdh, "time": now_str, "sector": sector,
                        })

                # Combo surge: PDH + volume >= 1.5x avg
                if vol_ratio >= 1.5:
                    result.combo_surge = True
                    if symbol not in self._combo_surge:
                        self._combo_surge.add(symbol)
                        self._first_combo_time[symbol] = now_str
                        result.combo_surge_first = True
                        if self._on_alert:
                            self._on_alert("COMBO_SURGE", symbol, {
                                "ltp": ltp, "pdh": pdh, "vol_ratio": vol_ratio,
                                "time": now_str, "sector": sector,
                            })

        # ── Bearish: PDL cross ──
        if pdl and pdl > 0 and ltp:
            if ltp <= pdl:
                result.pdl_crossed = True
                if symbol not in self._pdl_crossed:
                    self._pdl_crossed.add(symbol)
                    self._first_pdl_time[symbol] = now_str
                    result.pdl_first_cross = True
                    if self._on_alert:
                        self._on_alert("PDL_CROSS", symbol, {
                            "ltp": ltp, "pdl": pdl, "time": now_str, "sector": sector,
                        })

                # Combo sell: PDL + volume >= 1.5x avg
                if vol_ratio >= 1.5:
                    result.combo_sell = True
                    if symbol not in self._combo_sell:
                        self._combo_sell.add(symbol)
                        self._first_combo_sell_time[symbol] = now_str
                        result.combo_sell_first = True
                        if self._on_alert:
                            self._on_alert("COMBO_SELL", symbol, {
                                "ltp": ltp, "pdl": pdl, "vol_ratio": vol_ratio,
                                "time": now_str, "sector": sector,
                            })

        result.first_cross_time = self._first_cross_time.get(symbol)
        result.first_combo_time = self._first_combo_time.get(symbol)
        result.first_pdl_time = self._first_pdl_time.get(symbol)
        result.first_combo_sell_time = self._first_combo_sell_time.get(symbol)

        return result

    def get_state_snapshot(self) -> Dict[str, Any]:
        """Return a serializable snapshot of all tracked signals."""
        return {
            "pdh_crossed": sorted(self._pdh_crossed),
            "combo_surge": sorted(self._combo_surge),
            "first_cross_time": dict(self._first_cross_time),
            "first_combo_time": dict(self._first_combo_time),
            "pdl_crossed": sorted(self._pdl_crossed),
            "combo_sell": sorted(self._combo_sell),
            "first_pdl_time": dict(self._first_pdl_time),
            "first_combo_sell_time": dict(self._first_combo_sell_time),
        }
