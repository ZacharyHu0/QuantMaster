"""Callback seam for instrument validation that needs the market registry."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_refresh_history: Callable[..., Any] | None = None


def register_history_refresh(callback: Callable[..., Any]) -> None:
    global _refresh_history
    _refresh_history = callback


def refresh_history(*args: Any, **kwargs: Any) -> Any:
    if _refresh_history is None:
        raise RuntimeError("行情注册表尚未注册")
    return _refresh_history(*args, **kwargs)
