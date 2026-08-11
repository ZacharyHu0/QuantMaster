"""Bounded, cached operational diagnostics kept off the liveness path."""

from __future__ import annotations

import logging
import threading
import time
from datetime import UTC, datetime
from typing import Any

from quantmaster.server.problems import collect_health_report, make_problem

_TTL_SECONDS = 5.0
_WAIT_SECONDS = 2.0
_lock = threading.Lock()
_ready = threading.Event()
_refreshing = False
_cached: dict[str, Any] | None = None
_cached_at = 0.0
_sampler_stop = threading.Event()
_sampler: threading.Thread | None = None
logger = logging.getLogger(__name__)


def invalidate_diagnostics() -> None:
    global _cached_at
    with _lock:
        _cached_at = 0.0


def _refresh() -> None:
    global _cached, _cached_at, _refreshing
    try:
        report = collect_health_report()
        from quantmaster.operational_diagnostics import safe_operational_metrics

        report["components"] = safe_operational_metrics()
    except Exception:  # final diagnostic boundary: never break liveness/readiness
        logger.warning("完整诊断收集失败", exc_info=True)
        report = {
            "level": "warning",
            "checked_at": datetime.now(UTC).isoformat(),
            "issues": [make_problem(
                "diagnostics_failed",
                severity="warning",
                source="后台状态",
                title="完整诊断暂不可用",
                message="诊断任务未完成，请查看本机日志",
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


def start_diagnostics_sampler(interval_seconds: float = _TTL_SECONDS) -> None:
    """Refresh expensive diagnostics from the runtime worker, never a page GET."""

    global _sampler
    with _lock:
        if _sampler is not None and _sampler.is_alive():
            return
        _sampler_stop.clear()

        def run() -> None:
            while not _sampler_stop.is_set():
                _start_refresh()
                _sampler_stop.wait(max(1.0, float(interval_seconds)))

        _sampler = threading.Thread(
            target=run,
            name="qm-diagnostics-sampler",
            daemon=True,
        )
        _sampler.start()


def stop_diagnostics_sampler() -> None:
    global _sampler
    _sampler_stop.set()
    with _lock:
        sampler = _sampler
        _sampler = None
    if sampler is not None:
        sampler.join(timeout=1.0)


def diagnostics(*, wait_for_first: bool = True, refresh: bool = True) -> dict[str, Any]:
    with _lock:
        cached = _cached
        fresh = cached is not None and time.monotonic() - _cached_at < _TTL_SECONDS
    if not fresh and refresh:
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
        "checked_at": datetime.now(UTC).isoformat(),
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
