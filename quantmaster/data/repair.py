"""Rate-limited repair handlers backed by the unified job lifecycle."""

from __future__ import annotations

import builtins
import hashlib
import logging
import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from quantmaster.config import get_config
from quantmaster.data.repair_access import register_repair_access
from quantmaster.runtime.jobs import (
    JobContext,
    JobOutcome,
    UnifiedJobRuntime,
    UnifiedJobStore,
)
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.trading_sessions import market_date

logger = logging.getLogger(__name__)
RepairHandler = Callable[[dict[str, Any]], dict[str, Any] | None]
DATA_REPAIR_TASK_TYPE = "data.repair"
REPAIR_RESULT_KIND = "data.repair.result"
REPAIR_FAILURE_CHECKPOINT = "data.repair.failure"


def _canonical(value: dict[str, Any]) -> str:
    return strict_json_dumps(value, sort_keys=True)


def _idempotency_key(kind: str, target: str) -> str:
    return hashlib.sha256(f"{kind}\0{target}".encode()).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def quarantine_file(
    path: str | Path,
    *,
    category: str,
    target: str,
    reason: str,
) -> dict[str, Any] | None:
    """Move an original aside atomically and persist an audit manifest beside it."""

    source = Path(path).resolve()
    if not source.is_file():
        return None
    root = get_config().data_root.resolve()
    quarantine = root / "quarantine" / category / market_date().isoformat()
    quarantine.mkdir(parents=True, exist_ok=True)
    content_sha256 = _file_sha256(source)
    file_size = source.stat().st_size
    destination = quarantine / f"{source.name}.{uuid.uuid4().hex}.quarantine"
    os.replace(source, destination)
    _sync_directory(source.parent)
    _sync_directory(quarantine)
    manifest = {
        "schema_version": 1,
        "category": category,
        "target": target,
        "reason": reason,
        "original_path": str(source),
        "quarantine_path": str(destination),
        "content_sha256": content_sha256,
        "file_size": file_size,
        "quarantined_at": time.time(),
    }
    manifest_path = destination.with_suffix(destination.suffix + ".json")
    manifest_path.write_text(_canonical(manifest), encoding="utf-8")
    with manifest_path.open("rb+") as stream:
        os.fsync(stream.fileno())
    _sync_directory(quarantine)
    return manifest


class DataRepairManager:
    """Own repair admission and handlers while the kernel owns lifecycle state."""

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        read_only: bool = False,
        runtime: UnifiedJobRuntime | None = None,
    ) -> None:
        self._explicit_path = Path(path) if path is not None else None
        self.read_only = bool(read_only)
        self._runtime = runtime
        self._fixed_runtime = runtime is not None
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._wakeup = threading.Event()
        self._workers: list[threading.Thread] = []
        self._handlers: dict[str, RepairHandler] = {}
        self._register_builtin_handlers()
        if runtime is not None:
            runtime.register(DATA_REPAIR_TASK_TYPE, self._handle)

    def _path(self) -> Path:
        return self._explicit_path or get_config().data_root / "jobs.sqlite"

    def _ensure_runtime(self) -> UnifiedJobRuntime:
        if self.read_only:
            raise RuntimeError("只读修复投影不能执行任务")
        path = self._path()
        with self._lock:
            if self._runtime is not None:
                same_root = self._runtime.store.path.resolve() == path.resolve()
                if self._fixed_runtime or same_root:
                    return self._runtime
                if not self._runtime.idle:
                    raise RuntimeError("数据修复仍在旧数据目录运行，拒绝切换任务账本")
                self._runtime.stop()
            self._runtime = UnifiedJobRuntime(
                UnifiedJobStore(path),
                max_workers=max(1, min(int(get_config().data.repair_max_workers), 8)),
                dispatch=False,
            )
            self._runtime.register(DATA_REPAIR_TASK_TYPE, self._handle)
            return self._runtime

    def _read_store(self) -> UnifiedJobStore:
        if self._runtime is not None:
            if self._fixed_runtime or self._runtime.store.path.resolve() == self._path().resolve():
                return self._runtime.store
        return UnifiedJobStore(self._path(), read_only=True)

    def register_handler(self, kind: str, handler: RepairHandler) -> None:
        self._handlers[str(kind)] = handler

    def _register_builtin_handlers(self) -> None:
        self.register_handler("bar", self._repair_bar)
        self.register_handler("api_cache", self._repair_api_cache)
        from quantmaster.data.research_access import research_repair_handler

        handler = research_repair_handler()
        if handler is not None:
            self.register_handler("research_partition", handler)

    @staticmethod
    def _business_key(kind: str, target: str) -> str:
        return f"repair:{_idempotency_key(kind, target)}"

    def enqueue(
        self,
        kind: str,
        target: str,
        *,
        reason: str,
        spec: dict[str, Any],
        source: str = "unknown",
    ) -> dict[str, Any]:
        runtime = self._ensure_runtime()
        key = self._business_key(kind, target)
        existing = runtime.store.find_business_job(DATA_REPAIR_TASK_TYPE, key)
        if existing is None:
            existing, _created = runtime.store.submit(
                DATA_REPAIR_TASK_TYPE,
                {
                    "kind": str(kind),
                    "target": str(target),
                    "source": str(source),
                    "reason": str(reason),
                    "repair_spec": dict(spec),
                },
                business_key=key,
                max_attempts=max(1, int(get_config().data.repair_max_attempts)),
                deadline_seconds=600,
            )
        runtime.store.append_event(
            str(existing["id"]), "data_repair_evidence",
            {"reason": str(reason)[:1000], "source": str(source)[:100]},
        )
        self._wakeup.set()
        return self.get(str(existing["id"]))

    @staticmethod
    def _latest_reason(store: UnifiedJobStore, job: dict[str, Any]) -> str:
        events = store.events(str(job["id"]), 0, 2000)
        for event in reversed(events):
            if event["type"] == "data_repair_evidence":
                return str((event.get("payload") or {}).get("reason") or "")
        return str((job.get("spec") or {}).get("reason") or "")

    def _project(self, store: UnifiedJobStore, job: dict[str, Any]) -> dict[str, Any]:
        if str(job.get("type")) != DATA_REPAIR_TASK_TYPE:
            raise KeyError(str(job.get("id") or ""))
        spec = dict(job["spec"])
        artifact = store.latest_artifact(str(job["id"]), REPAIR_RESULT_KIND)
        payload = dict(artifact["payload"]) if artifact else {}
        failure = store.checkpoint(
            str(job["id"]), REPAIR_FAILURE_CHECKPOINT, str(job["spec_hash"]),
        ) or {}
        value = UnifiedJobRuntime.public(job)
        value.update({
            "kind": spec.get("kind"),
            "target": spec.get("target"),
            "source": spec.get("source"),
            "reason": self._latest_reason(store, job),
            "spec": dict(spec.get("repair_spec") or {}),
            "result": dict(payload.get("result") or {}),
            "outcome": str(payload.get("outcome") or ""),
            "last_error": str(failure.get("error") or job.get("detail") or ""),
            "next_run": float(job.get("next_retry_at") or 0),
            "completed_at": str(job.get("finished_at") or ""),
        })
        return value

    def get(self, repair_id: str) -> dict[str, Any]:
        try:
            store = self._read_store()
            return self._project(store, store.get(repair_id))
        except (FileNotFoundError, sqlite3.Error) as exc:
            raise KeyError(repair_id) from exc

    def list(self, *, status: str = "", limit: int = 100) -> list[dict[str, Any]]:
        try:
            store = self._read_store()
            values = [
                self._project(store, job)
                for job in store.list(limit, job_type=DATA_REPAIR_TASK_TYPE)
            ]
        except (FileNotFoundError, sqlite3.Error):
            return []
        return [value for value in values if not status or value["status"] == status]

    def events(self, repair_id: str, after: int = 0) -> builtins.list[dict[str, Any]]:
        store = self._read_store()
        self._project(store, store.get(repair_id))
        return store.events(repair_id, after, 2000)

    def retry(self, repair_id: str) -> dict[str, Any]:
        runtime = self._ensure_runtime()
        self._project(runtime.store, runtime.store.get(repair_id))
        runtime.store.retry(repair_id)
        self._wakeup.set()
        return self.get(repair_id)

    def resolve(
        self, kind: str, target: str, *, result: dict[str, Any],
    ) -> dict[str, Any] | None:
        runtime = self._ensure_runtime()
        job = runtime.store.find_business_job(
            DATA_REPAIR_TASK_TYPE, self._business_key(kind, target),
        )
        if job is None:
            return None
        if job["status"] == "completed":
            return self.get(str(job["id"]))
        if job["status"] in {"running", "cancelling"}:
            return self.get(str(job["id"]))
        artifact = runtime.store.write_artifact(
            str(job["id"]), REPAIR_RESULT_KIND,
            {"schema_version": "1.0", "outcome": "resolved_by_validation", "result": result},
            {"schema_version": "1.0", "lineage": {"spec_hash": job["spec_hash"]}},
        )
        runtime.store.complete_from_evidence(
            str(job["id"]),
            JobOutcome("completed", "独立完整性检查已确认修复", str(artifact["id"])),
        )
        return self.get(str(job["id"]))

    def cancel(self, repair_id: str) -> dict[str, Any]:
        runtime = self._ensure_runtime()
        self._project(runtime.store, runtime.store.get(repair_id))
        runtime.store.cancel(repair_id)
        return self.get(repair_id)

    def _budget_used(self, store: UnifiedJobStore, source: str) -> int:
        today = market_date()
        used = 0
        for job in store.list(1000, job_type=DATA_REPAIR_TASK_TYPE):
            if str((job.get("spec") or {}).get("source") or "") != source:
                continue
            used += sum(
                event["type"] == "job_started"
                and datetime.fromisoformat(str(event["created_at"])).astimezone(
                    ZoneInfo("Asia/Shanghai")
                ).date() == today
                for event in store.events(str(job["id"]), 0, 2000)
            )
        return used

    def _next_due(self, store: UnifiedJobStore) -> dict[str, Any] | None:
        now = time.time()
        budget = max(0, int(get_config().data.repair_daily_budget))
        jobs = sorted(
            store.list(1000, job_type=DATA_REPAIR_TASK_TYPE),
            key=lambda item: str(item.get("created_at") or ""),
        )
        for job in jobs:
            if job["status"] not in {"queued", "interrupted"}:
                continue
            if float(job.get("next_retry_at") or 0) > now:
                continue
            source = str((job.get("spec") or {}).get("source") or "unknown")
            if budget and self._budget_used(store, source) >= budget:
                continue
            return job
        return None

    def run_one(self) -> dict[str, Any] | None:
        runtime = self._ensure_runtime()
        job = self._next_due(runtime.store)
        if job is None:
            return None
        runtime.dispatch_job(str(job["id"]))
        runtime.wait(str(job["id"]), timeout=610.0)
        return self.get(str(job["id"]))

    def _handle(self, context: JobContext, spec: dict[str, Any]) -> JobOutcome:
        kind = str(spec["kind"])
        target = str(spec["target"])
        item: dict[str, Any] = {
            "id": context.job_id,
            "kind": kind,
            "target": target,
            "source": str(spec.get("source") or "unknown"),
            "reason": self._latest_reason(context.store, context.store.get(context.job_id)),
            "spec": dict(spec.get("repair_spec") or {}),
            "attempt": context.attempt,
        }
        handler = self._handlers.get(kind)
        try:
            if handler is None:
                raise RuntimeError(f"没有 {kind} 修复处理器")
            context.progress(5, "验证修复目标", target)
            result = handler(item) or {}
            context.ensure_active()
        except Exception as exc:
            from quantmaster.logging_config import redact_sensitive_text

            logger.exception(
                "数据修复 handler 失败 job=%s kind=%s", context.job_id, kind,
            )
            detail = f"{type(exc).__name__}: {redact_sensitive_text(exc)}"[:1000]
            context.write_checkpoint(
                REPAIR_FAILURE_CHECKPOINT, context.spec_hash,
                {"schema_version": "1.0", "error": detail},
            )
            base = max(0.01, float(get_config().data.repair_retry_backoff))
            delay = min(base * (2 ** max(0, context.attempt - 1)), 86400.0)
            return JobOutcome("failed", detail, retry_delay_seconds=delay)
        outcome = str(result.get("state") or "completed")
        artifact = context.write_artifact(
            REPAIR_RESULT_KIND,
            {"schema_version": "1.0", "outcome": outcome, "result": result},
            {"schema_version": "1.0", "lineage": {"spec_hash": context.spec_hash}},
        )
        context.emit("data_repair_completed", {"outcome": outcome})
        return JobOutcome("completed", "数据修复已完成", str(artifact["id"]))

    def start(self) -> None:
        if self.read_only:
            return
        self._ensure_runtime()
        with self._lock:
            if self._workers:
                return
            self._stop.clear()
            count = max(1, min(int(get_config().data.repair_max_workers), 8))
            for index in range(count):
                worker = threading.Thread(
                    target=self._loop, name=f"data-repair-{index + 1}", daemon=True,
                )
                self._workers.append(worker)
                worker.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            runtime = self._ensure_runtime()
            job = self._next_due(runtime.store)
            if job is None:
                self._wakeup.wait(0.75)
                self._wakeup.clear()
                continue
            runtime.dispatch_job(str(job["id"]))
            self._wakeup.wait(0.05)
            self._wakeup.clear()

    def shutdown(self, timeout: float = 10.0) -> None:
        self._stop.set()
        self._wakeup.set()
        with self._lock:
            workers, self._workers = self._workers, []
            runtime = self._runtime
        per_worker = max(0.05, timeout / max(1, len(workers)))
        for worker in workers:
            worker.join(per_worker)
        if runtime is not None:
            runtime.stop(deadline_seconds=timeout)

    @staticmethod
    def _repair_bar(item: dict[str, Any]) -> dict[str, Any]:
        from quantmaster.data.registry import RefreshMode, refresh_history
        from quantmaster.data.storage import BarStore

        spec = item["spec"]
        root = Path(spec["root"]).resolve()
        symbol = str(spec["symbol"])
        store = BarStore(root=root)
        start = str(spec.get("start") or "1990-01-01")
        end = str(spec.get("end") or market_date())
        with store.lock(symbol):
            target = store.path_for_repair(symbol)
            quarantine = quarantine_file(
                target, category="bars", target=symbol, reason=str(item["reason"]),
            )
            envelope = refresh_history(
                symbol, start, end, store=store, mode=RefreshMode.FULL,
                work_class="maintenance",
            )
            frame = envelope.require_data()
            result = store.read(symbol, enqueue_repair=False)
        if result.status != "ready" or frame.empty or envelope.quality.status != "verified":
            raise RuntimeError(f"重拉后完整性仍异常: {result.status}: {result.reason}")
        return {
            "rows": len(frame), "content_sha256": result.content_sha256,
            "quarantine": quarantine,
        }

    @staticmethod
    def _repair_api_cache(item: dict[str, Any]) -> dict[str, Any]:
        import pandas as pd

        spec = item["spec"]
        root = Path(spec["root"]).resolve()
        target = Path(spec["path"]).resolve()
        try:
            target.relative_to(root)
        except ValueError as exc:
            raise RuntimeError("接口缓存修复目标越出声明目录") from exc
        quarantine = spec.get("quarantine")
        if not target.exists():
            return {
                "state": "quarantined", "replacement": "not_available",
                "quarantine": quarantine,
            }
        try:
            frame = pd.read_parquet(target)
        except (ImportError, OSError, TypeError, ValueError) as exc:
            quarantine_file(
                target, category="api-cache", target=str(item["target"]),
                reason=f"替换缓存仍不可读: {type(exc).__name__}: {exc}",
            )
            raise RuntimeError("替换后的接口缓存仍不可读") from exc
        return {"state": "replaced", "rows": len(frame), "quarantine": quarantine}

_MANAGER: DataRepairManager | None = None
_MANAGER_LOCK = threading.Lock()


def get_data_repair_manager() -> DataRepairManager:
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None:
            _MANAGER = DataRepairManager()
        return _MANAGER


def reset_data_repair_manager_for_tests() -> None:
    global _MANAGER
    with _MANAGER_LOCK:
        manager, _MANAGER = _MANAGER, None
    if manager is not None:
        manager.shutdown(timeout=1.0)


def enqueue_repair(
    kind: str,
    target: str,
    *,
    reason: str,
    spec: dict[str, Any],
    source: str = "unknown",
) -> dict[str, Any] | None:
    if not get_config().data.repair_enabled:
        return None
    return get_data_repair_manager().enqueue(
        kind, target, reason=reason, spec=spec, source=source,
    )


def resolve_repair(
    kind: str, target: str, *, result: dict[str, Any],
) -> dict[str, Any] | None:
    return get_data_repair_manager().resolve(kind, target, result=result)


register_repair_access(enqueue_repair, quarantine_file, resolve_repair)
