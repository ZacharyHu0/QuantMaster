"""Callback seam for integrity readers that enqueue durable repairs."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_enqueue: Callable[..., dict[str, Any] | None] | None = None
_quarantine: Callable[..., dict[str, Any]] | None = None
_resolve: Callable[..., dict[str, Any] | None] | None = None


def register_repair_access(
    enqueue: Callable[..., dict[str, Any] | None],
    quarantine: Callable[..., dict[str, Any]],
    resolve: Callable[..., dict[str, Any] | None],
) -> None:
    global _enqueue, _quarantine, _resolve
    _enqueue = enqueue
    _quarantine = quarantine
    _resolve = resolve


def enqueue_repair(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
    if _enqueue is None:
        raise RuntimeError("修复队列尚未注册")
    return _enqueue(*args, **kwargs)


def quarantine_file(*args: Any, **kwargs: Any) -> dict[str, Any]:
    if _quarantine is None:
        raise RuntimeError("修复隔离器尚未注册")
    return _quarantine(*args, **kwargs)


def resolve_repair(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
    if _resolve is None:
        raise RuntimeError("修复队列尚未注册")
    return _resolve(*args, **kwargs)
