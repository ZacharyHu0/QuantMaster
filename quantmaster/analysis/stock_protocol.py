"""Stable seam for the optional evidence-backed stock research protocol."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_runner: Callable[..., dict[str, Any]] | None = None


def register_stock_analysis_v2(runner: Callable[..., dict[str, Any]]) -> None:
    global _runner
    _runner = runner


def run_stock_analysis_v2(service: Any, query: str, **kwargs: Any) -> dict[str, Any]:
    if _runner is None:
        raise RuntimeError("个股研究协议尚未注册")
    return _runner(service, query, **kwargs)
