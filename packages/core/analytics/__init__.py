"""
Analytics module for prediction market analysis.

Exports:
- metrics: Core calculation functions for scoring and detection
- MoverScorer: Unified scorer class for ranking movers
"""

from packages.core.analytics.metrics import (
    calculate_move_pp,
    calculate_pct_change,
    calculate_quality_score,
    calculate_volume_spike_ratio,
    classify_volume_spike,
    calculate_composite_score,
    is_significant_event,
    get_alert_severity_thresholds,
    classify_alert_severity,
    should_suppress_settlement_snap,
    alert_volume_threshold_multiplier,
    calculate_price_velocity,
    MoverScorer,
    default_mover_scorer,
)

__all__ = [
    "calculate_move_pp",
    "calculate_pct_change",
    "calculate_quality_score",
    "calculate_volume_spike_ratio",
    "classify_volume_spike",
    "calculate_composite_score",
    "is_significant_event",
    "get_alert_severity_thresholds",
    "classify_alert_severity",
    "should_suppress_settlement_snap",
    "alert_volume_threshold_multiplier",
    "calculate_price_velocity",
    "MoverScorer",
    "default_mover_scorer",
]



