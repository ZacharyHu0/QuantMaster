"""Dependency-inversion seam for data readers that consume research evidence."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

_lake_factory: Callable[[str | Path | None], Any] | None = None
_repair_handler: Callable[[dict[str, Any]], dict[str, Any]] | None = None
_engine_factory: Callable[[Any], Any] | None = None


def register_research_lake(factory: Callable[[str | Path | None], Any]) -> None:
    global _lake_factory
    _lake_factory = factory


def research_lake(root: str | Path | None = None) -> Any:
    if _lake_factory is None:
        raise RuntimeError("研究湖尚未注册")
    return _lake_factory(root)


def register_research_repair_handler(
    handler: Callable[[dict[str, Any]], dict[str, Any]],
) -> None:
    global _repair_handler
    _repair_handler = handler


def research_repair_handler() -> Callable[[dict[str, Any]], dict[str, Any]] | None:
    return _repair_handler


def register_research_engine(factory: Callable[[Any], Any]) -> None:
    global _engine_factory
    _engine_factory = factory


def research_engine(lake: Any) -> Any:
    if _engine_factory is None:
        raise RuntimeError("研究引擎尚未注册")
    return _engine_factory(lake)
