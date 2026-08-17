"""Configuration-manager constructor seam for data-root migration."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_factory: Callable[[], Any] | None = None


def register_config_manager(factory: Callable[[], Any]) -> None:
    global _factory
    _factory = factory


def new_config_manager() -> Any:
    if _factory is None:
        raise RuntimeError("设置管理器尚未注册")
    return _factory()
