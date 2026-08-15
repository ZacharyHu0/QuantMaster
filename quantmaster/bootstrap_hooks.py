"""Registries used by the composition root without importing Web modules."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_worker_hooks: tuple[
    Any,
    Callable[[], Any],
    Callable[[], None],
    Callable[[], None],
    Callable[[], None],
] | None = None


def register_server_worker_hooks(
    *,
    settings_manager: Any,
    get_settings_jobs: Callable[[], Any],
    shutdown_settings_jobs: Callable[[], None],
    start_diagnostics_sampler: Callable[[], None],
    stop_diagnostics_sampler: Callable[[], None],
) -> None:
    global _worker_hooks
    _worker_hooks = (
        settings_manager,
        get_settings_jobs,
        shutdown_settings_jobs,
        start_diagnostics_sampler,
        stop_diagnostics_sampler,
    )


def server_worker_hooks() -> tuple[
    Any, Callable[[], Any], Callable[[], None], Callable[[], None], Callable[[], None]
]:
    if _worker_hooks is None:
        raise RuntimeError("后台 Worker 的服务器组件尚未注册")
    return _worker_hooks


def server_worker_hooks_registered() -> bool:
    return _worker_hooks is not None
