"""Quality-assessment seam shared by the registry and reference-market reader."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_assessor: Callable[..., Any] | None = None
_coverage: Callable[..., Any] | None = None


def register_frame_quality(
    assessor: Callable[..., Any], coverage: Callable[..., Any],
) -> None:
    global _assessor, _coverage
    _assessor = assessor
    _coverage = coverage


def assess_daily_frame(*args: Any, **kwargs: Any) -> Any:
    if _assessor is None:
        raise RuntimeError("行情质量评估器尚未注册")
    return _assessor(*args, **kwargs)


def covers_requested_range(*args: Any, **kwargs: Any) -> Any:
    if _coverage is None:
        raise RuntimeError("行情质量评估器尚未注册")
    return _coverage(*args, **kwargs)
