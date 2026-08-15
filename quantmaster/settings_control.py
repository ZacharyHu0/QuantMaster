"""Private seam between settings HTTP handlers and durable settings jobs."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from quantmaster import settings as _settings  # noqa: F401
from quantmaster.data.migration import migration_manager

_settings_manager: Any | None = migration_manager.config_manager
_apply_runtime: Callable[[dict[str, Any]], dict[str, Any]] | None = None


def register_settings_control(
    manager: Any,
    apply_runtime: Callable[[dict[str, Any]], dict[str, Any]],
) -> None:
    global _settings_manager, _apply_runtime
    _settings_manager = manager
    _apply_runtime = apply_runtime


def settings_manager() -> Any:
    if _settings_manager is None:
        raise RuntimeError("设置运行时尚未注册")
    return _settings_manager


def apply_runtime(saved: dict[str, Any]) -> dict[str, Any]:
    if _apply_runtime is None:
        raise RuntimeError("设置运行时尚未注册")
    return _apply_runtime(saved)
