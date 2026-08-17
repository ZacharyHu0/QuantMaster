"""Registered optional rotation provider implementation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_provider_factory: Callable[..., Any] | None = None


def register_rotation_provider(factory: Callable[..., Any]) -> None:
    global _provider_factory
    _provider_factory = factory


def rotation_provider() -> Callable[..., Any]:
    if _provider_factory is None:
        raise RuntimeError("轮动数据提供器尚未注册")
    return _provider_factory
