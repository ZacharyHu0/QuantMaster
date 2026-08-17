"""Shared signal result contract used by strategies and decision engines."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class SignalBundle:
    """Attributable signal payload compatible with the strategy protocol."""

    weights: pd.DataFrame
    scores: pd.DataFrame | None = None
    confidence: pd.DataFrame | None = None
    degraded: pd.DataFrame | None = None
    contributions: dict[str, pd.DataFrame] = field(default_factory=dict)
    intentional_flat: pd.Series | None = None
    target_exposure: pd.Series | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
