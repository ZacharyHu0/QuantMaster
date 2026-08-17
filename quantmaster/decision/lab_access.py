"""Explicit access seam for decision consumers of Lab-owned capabilities."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_predict_panel: Callable[..., Any] | None = None


def register_predict_panel(factory: Callable[..., Any]) -> None:
    global _predict_panel
    _predict_panel = factory


def predict_panel(*args: Any, **kwargs: Any) -> Any:
    if _predict_panel is None:
        raise RuntimeError("Quant Lab 模型推理器尚未注册")
    return _predict_panel(*args, **kwargs)
