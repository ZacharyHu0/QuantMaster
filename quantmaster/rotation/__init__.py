"""Market breadth and sector/theme rotation analytics."""

from quantmaster.rotation.analytics import (
    analyze_group_rotation,
    compute_market_structure,
    compute_market_temperature,
    compute_trend_matrices,
    estimate_etf_flows,
)

__all__ = [
    "analyze_group_rotation",
    "compute_market_structure",
    "compute_market_temperature",
    "compute_trend_matrices",
    "estimate_etf_flows",
]
