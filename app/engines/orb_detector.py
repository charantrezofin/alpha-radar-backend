"""
ORB (Opening Range Breakout) Detector -- tracks ORB15 and ORB30 breakouts.

Opening Range = first 15-min candle (9:15-9:30) and first 30-min (9:15-9:45).
ORB high/low are locked for the full trading day -- never recalculated.

Tracks 6 signal types per symbol:
  - nearOrb15  : within 0.5% below ORB15 high (dynamic -- cleared if price falls back)
  - orbBreak15 : ltp crossed ORB15 high (latched -- stays true once fired)
  - nearOrb30  : within 0.5% below ORB30 high (dynamic)
  - orbBreak30 : ltp crossed ORB30 high (latched)
  - orbBreakDown15 : ltp crossed below ORB15 low (latched)
  - orbBreakDown30 : ltp crossed below ORB30 low (latched)

Ported from Alpha-Radar-backend/server.js (lines 404-562)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set


NEAR_THRESHOLD = 0.005  # 0.5% below ORB high = "coiling"


@dataclass
class ORBLevel:
    orb_high_15: Optional[float] = None
    orb_low_15: Optional[float] = None
    orb_high_30: Optional[float] = None
    orb_low_30: Optional[float] = None
    loaded_at_15: Optional[str] = None
    loaded_at_30: Optional[str] = None


@dataclass
class ORBStatus:
    near_orb15: bool = False
    orb_break15: bool = False
    near_orb30: bool = False
    orb_break30: bool = False
    orb_break_down15: bool = False
    orb_break_down30: bool = False
    break_time_15: Optional[str] = None
    break_time_30: Optional[str] = None
    break_down_time_15: Optional[str] = None
    break_down_time_30: Optional[str] = None
    orb_high_15: Optional[float] = None
    orb_low_15: Optional[float] = None
    orb_high_30: Optional[float] = None
    orb_low_30: Optional[float] = None


class ORBDetector:
    """
    Stateful ORB tracker that maintains per-symbol ORB levels
    and breakout/breakdown state for the trading day.
    """

    def __init__(self) -> None:
        self._orb_cache: Dict[str, ORBLevel] = {}
        self._cache_date: Optional[str] = None

        # Bullish breaks (latched)
        self._orb_break_15: Set[str] = set()
        self._orb_break_30: Set[str] = set()
        self._break_time_15: Dict[str, str] = {}
        self._break_time_30: Dict[str, str] = {}

        # Near ORB (dynamic)
        self._near_orb_15: Set[str] = set()
        self._near_orb_30: Set[str] = set()

        # Bearish breaks (latched)
        self._orb_break_down_15: Set[str] = set()
        self._orb_break_down_30: Set[str] = set()
        self._break_down_time_15: Dict[str, str] = {}
        self._break_down_time_30: Dict[str, str] = {}

        # New breaks since last poll (for push notifications)
        self._new_breaks_15: Set[str] = set()
        self._new_breaks_30: Set[str] = set()
        self._new_breaks_down_15: Set[str] = set()
        self._new_breaks_down_30: Set[str] = set()

    def _reset_if_new_day(self) -> None:
        today = datetime.now().strftime("%Y-%m-%d")
        if self._cache_date != today:
            self._orb_cache.clear()
            self._cache_date = today
            self._orb_break_15.clear()
            self._orb_break_30.clear()
            self._break_time_15.clear()
            self._break_time_30.clear()
            self._near_orb_15.clear()
            self._near_orb_30.clear()
            self._orb_break_down_15.clear()
            self._orb_break_down_30.clear()
            self._break_down_time_15.clear()
            self._break_down_time_30.clear()
            self._new_breaks_15.clear()
            self._new_breaks_30.clear()
            self._new_breaks_down_15.clear()
            self._new_breaks_down_30.clear()

    def load_orb15(self, symbol: str, candles: List[Dict[str, Any]]) -> None:
        """
        Load ORB15 levels from the first 15-min candle.

        Parameters
        ----------
        symbol : str
            Stock symbol.
        candles : list
            List of 15-minute candle dicts with 'high' and 'low' keys.
            Expects at least 1 candle (the 9:15-9:30 bar).
        """
        self._reset_if_new_day()
        if not candles:
            return

        if symbol not in self._orb_cache:
            self._orb_cache[symbol] = ORBLevel()

        orb = self._orb_cache[symbol]
        orb.orb_high_15 = candles[0]["high"]
        orb.orb_low_15 = candles[0]["low"]
        orb.loaded_at_15 = datetime.now().strftime("%H:%M:%S")

    def load_orb30(self, symbol: str, candles: List[Dict[str, Any]]) -> None:
        """
        Load ORB30 levels from first two 15-min candles combined.

        Parameters
        ----------
        symbol : str
            Stock symbol.
        candles : list
            List of 15-minute candle dicts. Needs >= 2 for proper ORB30.
            Falls back to single candle if only 1 available.
        """
        self._reset_if_new_day()
        if not candles:
            return

        if symbol not in self._orb_cache:
            self._orb_cache[symbol] = ORBLevel()

        orb = self._orb_cache[symbol]
        if len(candles) >= 2:
            orb.orb_high_30 = max(candles[0]["high"], candles[1]["high"])
            orb.orb_low_30 = min(candles[0]["low"], candles[1]["low"])
        else:
            # Fallback: only 1 candle available
            orb.orb_high_30 = candles[0]["high"]
            orb.orb_low_30 = candles[0]["low"]
        orb.loaded_at_30 = datetime.now().strftime("%H:%M:%S")

    def clear_new_breaks(self) -> None:
        """Clear the new-break sets after they have been consumed (e.g., sent as alerts)."""
        self._new_breaks_15.clear()
        self._new_breaks_30.clear()
        self._new_breaks_down_15.clear()
        self._new_breaks_down_30.clear()

    def check_orb_status(self, symbol: str, ltp: float) -> ORBStatus:
        """
        Check and update ORB breakout/breakdown state for a symbol.

        Parameters
        ----------
        symbol : str
            Stock symbol.
        ltp : float
            Last traded price.

        Returns
        -------
        ORBStatus
            Current ORB status for the symbol.
        """
        self._reset_if_new_day()
        orb = self._orb_cache.get(symbol)

        status = ORBStatus()
        if not orb or not ltp:
            return status

        status.orb_high_15 = orb.orb_high_15
        status.orb_low_15 = orb.orb_low_15
        status.orb_high_30 = orb.orb_high_30
        status.orb_low_30 = orb.orb_low_30

        now_str = datetime.now().strftime("%H:%M:%S")

        # ── ORB 15 ──
        if orb.orb_high_15:
            near_15 = ltp >= orb.orb_high_15 * (1 - NEAR_THRESHOLD) and ltp < orb.orb_high_15
            if near_15:
                self._near_orb_15.add(symbol)
            else:
                self._near_orb_15.discard(symbol)
            status.near_orb15 = near_15

            if ltp >= orb.orb_high_15 and symbol not in self._orb_break_15:
                self._orb_break_15.add(symbol)
                self._near_orb_15.discard(symbol)
                self._break_time_15[symbol] = now_str
                self._new_breaks_15.add(symbol)

            status.orb_break15 = symbol in self._orb_break_15
            status.break_time_15 = self._break_time_15.get(symbol)

        # ── ORB 30 ──
        if orb.orb_high_30:
            near_30 = ltp >= orb.orb_high_30 * (1 - NEAR_THRESHOLD) and ltp < orb.orb_high_30
            if near_30:
                self._near_orb_30.add(symbol)
            else:
                self._near_orb_30.discard(symbol)
            status.near_orb30 = near_30

            if ltp >= orb.orb_high_30 and symbol not in self._orb_break_30:
                self._orb_break_30.add(symbol)
                self._near_orb_30.discard(symbol)
                self._break_time_30[symbol] = now_str
                self._new_breaks_30.add(symbol)

            status.orb_break30 = symbol in self._orb_break_30
            status.break_time_30 = self._break_time_30.get(symbol)

        # ── ORB 15 DOWN BREAK ──
        if orb.orb_low_15:
            if ltp <= orb.orb_low_15 and symbol not in self._orb_break_down_15:
                self._orb_break_down_15.add(symbol)
                self._break_down_time_15[symbol] = now_str
                self._new_breaks_down_15.add(symbol)

            status.orb_break_down15 = symbol in self._orb_break_down_15
            status.break_down_time_15 = self._break_down_time_15.get(symbol)

        # ── ORB 30 DOWN BREAK ──
        if orb.orb_low_30:
            if ltp <= orb.orb_low_30 and symbol not in self._orb_break_down_30:
                self._orb_break_down_30.add(symbol)
                self._break_down_time_30[symbol] = now_str
                self._new_breaks_down_30.add(symbol)

            status.orb_break_down30 = symbol in self._orb_break_down_30
            status.break_down_time_30 = self._break_down_time_30.get(symbol)

        return status

    @property
    def new_breaks_15(self) -> Set[str]:
        return self._new_breaks_15

    @property
    def new_breaks_30(self) -> Set[str]:
        return self._new_breaks_30

    @property
    def new_breaks_down_15(self) -> Set[str]:
        return self._new_breaks_down_15

    @property
    def new_breaks_down_30(self) -> Set[str]:
        return self._new_breaks_down_30

    def get_all_breaks(self) -> Dict[str, Any]:
        """Return a snapshot of all current ORB states for API responses."""
        return {
            "orb_break_15": sorted(self._orb_break_15),
            "orb_break_30": sorted(self._orb_break_30),
            "near_orb_15": sorted(self._near_orb_15),
            "near_orb_30": sorted(self._near_orb_30),
            "orb_break_down_15": sorted(self._orb_break_down_15),
            "orb_break_down_30": sorted(self._orb_break_down_30),
            "break_time_15": dict(self._break_time_15),
            "break_time_30": dict(self._break_time_30),
            "break_down_time_15": dict(self._break_down_time_15),
            "break_down_time_30": dict(self._break_down_time_30),
        }
