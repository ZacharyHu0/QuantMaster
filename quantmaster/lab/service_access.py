"""Composition seam between Lab lifecycle jobs and the Lab service."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_service_factory: Callable[..., Any] | None = None


def register_lab_service_factory(factory: Callable[..., Any]) -> None:
    global _service_factory
    _service_factory = factory


def get_lab_service(*, read_only: bool = False) -> Any:
    if _service_factory is None:
        raise RuntimeError("Quant Lab service composition root尚未注册")
    return _service_factory(read_only=read_only)
