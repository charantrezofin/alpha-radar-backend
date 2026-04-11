# app/engines/cpr/__init__.py
"""
CPR Engine — Central Pivot Range analysis engine.

Re-exports main classes and functions from all submodules for convenient access.
"""

from .config import cfg, Config

from .cpr_calculator import (
    CPRDirection, CPRWidth, CPRLevel, CPRSequenceResult, ProjectedCPR,
    compute_cpr_level, compute_cpr_series, compute_cpr_direction,
    analyse_cpr_sequence, get_cpr_analysis,
    is_price_above_cpr, is_price_below_cpr, price_vs_cpr,
    project_next_period_cpr, get_breakout_radar_score,
)

from .pema_calculator import (
    PEMAStack, PEMASlope, PEMAResult,
    compute_pema,
    detect_15min_long_trigger, detect_15min_short_trigger,
)

from .signal_scorer import (
    AlertTier, TradeDirection, TimeframeScore, SignalResult,
    score_symbol, score_all_symbols,
)

from .pattern_detector import (
    PatternType, BreakoutDirection, PatternResult,
    scan_patterns, pattern_to_dict,
    find_swing_highs, find_swing_lows,
)

from .vcp_detector import (
    Contraction, VCPResult,
    analyze_vcp, vcp_result_to_dict,
)

from .nr_squeeze import (
    NRSqueezeResult,
    analyze_nr_squeeze, nr_result_to_dict,
)

from .swing_detector import (
    SwingSignal,
    scan_swing_spectrum, swing_signal_to_dict,
    detect_breakout, detect_reversal, detect_channel_breakout,
)

from .volume_profile import (
    VolumeProfileResult,
    compute_volume_profile, analyze_volume_profile, vp_result_to_dict,
)

__all__ = [
    # Config
    "cfg", "Config",
    # CPR Calculator
    "CPRDirection", "CPRWidth", "CPRLevel", "CPRSequenceResult", "ProjectedCPR",
    "compute_cpr_level", "compute_cpr_series", "compute_cpr_direction",
    "analyse_cpr_sequence", "get_cpr_analysis",
    "is_price_above_cpr", "is_price_below_cpr", "price_vs_cpr",
    "project_next_period_cpr", "get_breakout_radar_score",
    # PEMA Calculator
    "PEMAStack", "PEMASlope", "PEMAResult",
    "compute_pema", "detect_15min_long_trigger", "detect_15min_short_trigger",
    # Signal Scorer
    "AlertTier", "TradeDirection", "TimeframeScore", "SignalResult",
    "score_symbol", "score_all_symbols",
    # Pattern Detector
    "PatternType", "BreakoutDirection", "PatternResult",
    "scan_patterns", "pattern_to_dict",
    "find_swing_highs", "find_swing_lows",
    # VCP Detector
    "Contraction", "VCPResult",
    "analyze_vcp", "vcp_result_to_dict",
    # NR Squeeze
    "NRSqueezeResult",
    "analyze_nr_squeeze", "nr_result_to_dict",
    # Swing Detector
    "SwingSignal",
    "scan_swing_spectrum", "swing_signal_to_dict",
    "detect_breakout", "detect_reversal", "detect_channel_breakout",
    # Volume Profile
    "VolumeProfileResult",
    "compute_volume_profile", "analyze_volume_profile", "vp_result_to_dict",
]
