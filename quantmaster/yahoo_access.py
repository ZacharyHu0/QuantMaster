"""Optional Yahoo SDK seam for instrument search."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_loader: Callable[[], Any] | None = None


def register_yahoo_loader(loader: Callable[[], Any]) -> None:
    global _loader
    _loader = loader


def yahoo_loader() -> Any:
    if _loader is None:
        raise RuntimeError("Yahoo 数据源尚未注册")
    return _loader()
