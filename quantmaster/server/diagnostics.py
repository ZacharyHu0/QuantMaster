"""Bounded, cached operational diagnostics kept off the liveness path."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any

from quantmaster.server.problems import collect_health_report, make_problem

_TTL_SECONDS = 5.0
_WAIT_SECONDS = 2.0
_lock = threading.Lock()
_ready = threading.Event()
_refreshing = False
_cached: dict[str, Any] | None = None
_cached_at = 0.0


def invalidate_diagnostics() -> None:
    global _cached_at
    with _lock:
        _cached_at = 0.0


def _refresh() -> None:
    global _cached, _cached_at, _refreshing
    try:
        report = collect_health_report()
    except Exception as exc:  # final diagnostic boundary: never break liveness/readiness
        report = {
            "level": "warning",
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "issues": [make_problem(
                "diagnostics_failed",
                severity="warning",
                source="后台状态",
                title="完整诊断暂不可用",
                message=str(exc)[:300] or "诊断任务未完成",
                action="稍后重试并查看服务日志。",
            )],
        }
    with _lock:
        _cached = report
        _cached_at = time.monotonic()
        _refreshing = False
        _ready.set()


def _start_refresh() -> None:
    global _refreshing
    with _lock:
        if _refreshing:
            return
        _refreshing = True
        _ready.clear()
    threading.Thread(target=_refresh, name="qm-diagnostics", daemon=True).start()


def diagnostics(*, wait_for_first: bool = True) -> dict[str, Any]:
    with _lock:
        cached = _cached
        fresh = cached is not None and time.monotonic() - _cached_at < _TTL_SECONDS
    if not fresh:
        _start_refresh()
    if cached is None and wait_for_first:
        _ready.wait(_WAIT_SECONDS)
        with _lock:
            cached = _cached
    if cached is not None:
        result = dict(cached)
        result["refreshing"] = not fresh
        return result
    return {
        "level": "warning",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "refreshing": True,
        "issues": [make_problem(
            "diagnostics_pending",
            severity="info",
            source="后台状态",
            title="完整诊断仍在运行",
            message="诊断超过快速响应预算，结果将在后台缓存。",
            action="稍后刷新后台状态。",
        )],
    }
