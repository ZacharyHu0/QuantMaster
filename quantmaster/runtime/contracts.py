"""Shared strict input contracts for API payloads and immutable specifications."""

from __future__ import annotations

import math
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator


def reject_nonfinite(value: Any, path: str = "payload") -> Any:
    """Reject NaN and infinities recursively, including values hidden in Any fields."""
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{path} 不能包含 NaN 或 Infinity")
    if isinstance(value, Decimal) and not value.is_finite():
        raise ValueError(f"{path} 不能包含 NaN 或 Infinity")
    if isinstance(value, Mapping):
        for key, item in value.items():
            reject_nonfinite(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple, set, frozenset)):
        for index, item in enumerate(value):
            reject_nonfinite(item, f"{path}[{index}]")
    return value


class ContractModel(BaseModel):
    """Default-deny model shared by external and persisted JSON contracts."""

    model_config = ConfigDict(
        extra="forbid", allow_inf_nan=False, str_strip_whitespace=True,
    )

    @model_validator(mode="before")
    @classmethod
    def reject_nonfinite_values(cls, value: Any) -> Any:
        return reject_nonfinite(value)
