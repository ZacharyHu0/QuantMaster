"""Bounded, cached operational diagnostics kept off the liveness path."""

from __future__ import annotations

import logging
import re
import threading
import time
from datetime import UTC, datetime
from typing import Any

from quantmaster.logging_config import redact_sensitive_text
from quantmaster.server.problems import collect_health_report, make_problem

_TTL_SECONDS = 5.0
_WAIT_SECONDS = 2.0
_REFRESH_TIMEOUT_SECONDS = 10.0
_lock = threading.Lock()
_ready = threading.Event()
_refreshing = False
_cached: dict[str, Any] | None = None
_cached_at = 0.0
_sampler_stop = threading.Event()
_sampler: threading.Thread | None = None
_refresh_thread: threading.Thread | None = None
_refresh_generation = 0
logger = logging.getLogger(__name__)
_problem_history: dict[str, dict[str, Any]] = {}
_recovered_history: list[dict[str, Any]] = []
_MAX_RECOVERED = 20
_SECRET_FIELD = re.compile(r"(?i)(?:token|secret|password|authorization|header|ticket|credential)")


def invalidate_diagnostics() -> None:
    global _cached_at
    with _lock:
        _cached_at = 0.0


def _refresh(generation: int | None = None) -> None:
    global _cached, _cached_at, _refreshing
    if generation is None:
        with _lock:
            generation = _refresh_generation
    try:
        report = collect_health_report()
        from quantmaster.runtime.llm import get_llm_execution_coordinator
        from quantmaster.server.operational_diagnostics import safe_operational_metrics

        report["components"] = safe_operational_metrics()
        from quantmaster.server.cache_observability import collect_cache_observability

        report["cache"] = collect_cache_observability()
        llm = get_llm_execution_coordinator().diagnostics()
        from quantmaster.ai.llm import llm_gate_status, news_llm_gate_status

        llm["scopes"]["global"]["gate"] = llm_gate_status()
        llm["scopes"]["news"]["gate"] = news_llm_gate_status()
        report["llm"] = llm
        from quantmaster.server.readiness import runtime_status

        runtime = runtime_status()
        try:
            storage = dict(runtime.get("storage") or {})
            # This is a public status endpoint, not a sidecar debug dump.
            # Keep only an explicit non-secret allowlist.
            runtime["free_stockdb"] = {
                "state": "running" if storage.get("active_writers") else "stopped",
                "managed": bool(storage.get("active_writers")),
                "update_result": (
                    "failed" if storage.get("last_error") else "unknown"
                ),
            }
        except (OSError, RuntimeError, TypeError, ValueError):
            runtime["free_stockdb"] = {
                "state": "degraded", "message": "状态读取失败",
            }
        report["runtime"] = runtime
        readiness = runtime["readiness"]
        if not readiness["storage_ready"]:
            report.setdefault("issues", []).append(make_problem(
                "core_storage_unavailable",
                severity="error",
                source="本地存储",
                title="核心本地存储不可用",
                message="配置的数据目录尚不可访问，Web 不能安全处理本地数据。",
                action="检查数据目录权限或恢复磁盘后重启 QuantMaster。",
                blocking=True,
                problem_id="readiness:storage",
                correlation_id="readiness-storage",
            ))
        _decorate_problem_history(report)
        report = _redact_report(report)
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
        _decorate_problem_history(report)
    with _lock:
        if generation != _refresh_generation:
            return
        _cached = report
        _cached_at = time.monotonic()
        _refreshing = False
        _ready.set()


def _expire_refresh(generation: int) -> None:
    """Publish a bounded failure without letting a hung probe block health."""

    global _cached, _cached_at, _refreshing, _refresh_generation
    expired = False
    with _lock:
        if generation != _refresh_generation or not _refreshing:
            return
        _refresh_generation += 1
        _refreshing = False
        _cached = {
            "level": "warning",
            "checked_at": datetime.now(UTC).isoformat(),
            "refreshing": False,
            "issues": [make_problem(
                "diagnostics_timeout",
                severity="warning",
                source="后台状态",
                title="完整诊断超时",
                message="诊断任务超过时间预算，已记录失败并继续提供健康检查。",
                action="稍后重试并查看服务日志。",
            )],
        }
        _cached_at = time.monotonic()
        _ready.set()
        expired = True
    if expired:
        logger.warning("完整诊断刷新超时 generation=%s", generation)


def _decorate_problem_history(report: dict[str, Any]) -> None:
    """Attach stable, non-secret occurrence metadata for the status drawer."""

    checked_at = str(report.get("checked_at") or datetime.now(UTC).isoformat())
    active: set[str] = set()
    for item in report.get("issues") or []:
        if not isinstance(item, dict):
            continue
        dimensions = (
            item.get("id"), item.get("event"), item.get("component"), item.get("code"),
            item.get("item_type"), item.get("item_id"), item.get("symbol"),
            item.get("business_date"), item.get("security_event_kind"),
        )
        key = "|".join(str(value or "") for value in dimensions)
        active.add(key)
        previous = _problem_history.get(key)
        if previous is None:
            previous = {
                "first_seen_at": checked_at, "suppressed_count": 0,
                "problem": dict(item),
            }
        else:
            previous["suppressed_count"] = int(previous["suppressed_count"]) + 1
        previous["last_seen_at"] = checked_at
        previous["first_seen"] = previous["first_seen_at"]
        previous["last_seen"] = previous["last_seen_at"]
        previous["consecutive_count"] = int(previous["suppressed_count"]) + 1
        previous["affected_count"] = max(
            int(previous.get("affected_count") or 0), len(item.get("items") or []) or 1,
        )
        _problem_history[key] = previous
        item.update({key: value for key, value in previous.items() if key != "problem"})
        item.setdefault("diagnostic_id", str(item.get("correlation_id") or item.get("id") or ""))
    for key in set(_problem_history) - active:
        history = _problem_history.pop(key)
        recovered = dict(history.get("problem") or {})
        recovered.update({name: value for name, value in history.items() if name != "problem"})
        recovered.update({"state": "recovered", "recovered_at": checked_at})
        _recovered_history.insert(0, recovered)
    del _recovered_history[_MAX_RECOVERED:]
    report["recent_recovered"] = [dict(item) for item in _recovered_history]


def _redact_report(value: Any) -> Any:
    """Never turn an internal component snapshot into a credential leak."""

    if isinstance(value, str):
        return redact_sensitive_text(value)
    if isinstance(value, dict):
        return {
            str(key): "***" if _SECRET_FIELD.search(str(key)) else _redact_report(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_report(item) for item in value]
    if isinstance(value, tuple):
        return [_redact_report(item) for item in value]
    return value


def _start_refresh() -> None:
    global _refreshing, _refresh_thread, _refresh_generation
    with _lock:
        if _refreshing:
            return
        if _refresh_thread is not None and _refresh_thread.is_alive():
            return
        _refreshing = True
        _refresh_generation += 1
        generation = _refresh_generation
        _ready.clear()
        _refresh_thread = threading.Thread(
            target=_refresh, args=(generation,), name="qm-diagnostics", daemon=True,
        )
        _refresh_thread.start()
    timer = threading.Timer(
        _REFRESH_TIMEOUT_SECONDS, _expire_refresh, args=(generation,),
    )
    timer.daemon = True
    timer.start()


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
