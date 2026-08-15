"""Narrow command-to-runtime seam; it contains no concrete runtime import."""

from __future__ import annotations

from collections.abc import Callable

_reload_jobs: Callable[[], None] | None = None


def register_reload_jobs(callback: Callable[[], None]) -> None:
    global _reload_jobs
    _reload_jobs = callback


def reload_jobs() -> None:
    if _reload_jobs is None:
        raise RuntimeError("自动化运行时尚未注册任务重载入口")
    _reload_jobs()
