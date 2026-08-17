"""Provider seam for point-in-time index membership refreshes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_source_factory: Callable[[], Any] | None = None


def register_index_source(factory: Callable[[], Any]) -> None:
    global _source_factory
    _source_factory = factory


def index_source() -> Any:
    if _source_factory is None:
        raise RuntimeError("指数成分数据源尚未注册")
    return _source_factory()
