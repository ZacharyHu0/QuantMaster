"""Quant Lab domain work backed by the unified job lifecycle."""

from __future__ import annotations

import builtins
import os
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from quantmaster.config import get_config
from quantmaster.lab.errors import classify_lab_error
from quantmaster.lab.preflight import require_runnable
from quantmaster.runtime.jobs import (
    ACTIVE_STATUSES,
    JobContext,
    JobOutcome,
    UnifiedJobRuntime,
    UnifiedJobStore,
)

LAB_KINDS = frozenset({
    "prepare_data",
    "validate",
    "discover_genetic",
    "discover_llm",
    "discover_python",
    "optimize",
    "bias_audit",
    "research_cycle",
    "shadow_score",
})
LAB_JOB_TYPES = frozenset(f"lab.{kind}" for kind in LAB_KINDS)
LAB_RESULT_KIND = "lab.worker.result"
LAB_PROGRESS_CHECKPOINT = "lab.worker.progress"
LAB_ALGORITHM_VERSION = "quant-lab-v2"


def _jobs_path() -> Path:
    return get_config().data_root / "jobs.sqlite"


def _lab_path() -> Path:
    return get_config().data_root / "lab.sqlite"


class LabJobManager:
    """Own Lab admission/projection while UnifiedJobRuntime owns lifecycle."""

    def __init__(self, service: Any | None = None, runtime: UnifiedJobRuntime | None = None):
        if service is None:
            from quantmaster.lab.service import get_lab_service

            service = get_lab_service()
        self.service: Any = service
        try:
            cast(Any, self.service)._job_manager = self
        except AttributeError:
            pass
        self._path = _jobs_path()
        self._runtime = runtime
        self._fixed_runtime = runtime is not None
        self._lock = threading.RLock()
        self._resource_gates = {
            "gpu": threading.BoundedSemaphore(
                max(1, int(get_config().lab.gpu_max_concurrent_jobs)),
            ),
            "external": threading.BoundedSemaphore(1),
            "io": threading.BoundedSemaphore(1),
        }
        if runtime is not None:
            self._register(runtime)

    @staticmethod
    def _owns_runtime() -> bool:
        return os.environ.get("QM_WEB_PROCESS") != "1"

    def _register(self, runtime: UnifiedJobRuntime) -> None:
        for job_type in LAB_JOB_TYPES:
            runtime.register(job_type, self._handle)

    def _ensure_runtime(self) -> UnifiedJobRuntime:
        with self._lock:
            if self._runtime is not None:
                if not self._fixed_runtime and self._runtime.snapshot()["status"] == "stopped":
                    self._runtime = None
                else:
                    return self._runtime
            self._runtime = UnifiedJobRuntime(
                UnifiedJobStore(self._path),
                max_workers=min(4, max(1, int(get_config().lab.max_workers))),
                dispatch=self._owns_runtime(),
            )
            self._register(self._runtime)
            return self._runtime

    def _read_store(self) -> UnifiedJobStore:
        runtime = self._runtime
        if runtime is not None and (
            self._fixed_runtime or runtime.store.path.resolve() == self._path.resolve()
        ):
            return runtime.store
        return UnifiedJobStore(self._path, read_only=True)

    def find_business(self, business_key: str) -> dict[str, Any] | None:
        if not business_key:
            return None
        try:
            store = self._read_store()
            for job_type in LAB_JOB_TYPES:
                value = store.find_business_job(job_type, business_key)
                if value is not None:
                    return self._project(store, value)
        except (FileNotFoundError, sqlite3.Error):
            return None
        return None

    def submit(
        self,
        kind: str,
        params: dict[str, Any],
        *,
        preflight: dict[str, Any] | None = None,
        dataset_id: str = "",
        business_key: str = "",
    ) -> dict[str, Any]:
        if kind not in LAB_KINDS:
            raise ValueError(f"未知研究任务: {kind}")
        admission = dict(preflight or {})
        spec = {
            "kind": kind,
            "params": dict(params),
            "preflight": admission,
            "dataset_id": str(dataset_id),
            "resource_class": str(admission.get("resource_class") or "cpu"),
        }
        runtime = self._ensure_runtime()
        job, _created = runtime.submit(
            f"lab.{kind}",
            spec,
            business_key=str(business_key),
            algorithm_version=LAB_ALGORITHM_VERSION,
            deadline_seconds=3600,
            max_attempts=8,
            llm_scope="global" if kind in {"discover_llm", "discover_python"} else "",
        )
        return self._project(runtime.store, job)

    @staticmethod
    def _outcome(result: dict[str, Any]) -> tuple[str, str, str]:
        warnings = result.get("warnings")
        warnings = warnings if isinstance(warnings, list) else []
        if result.get("cancelled"):
            return "cancelled", "cancelled", "研究任务已取消"
        if result.get("paused"):
            return "interrupted", "paused", "研究已暂停，可从已提交 checkpoint 恢复"
        if warnings:
            first = warnings[0]
            detail = str(first.get("message") if isinstance(first, dict) else first)
            return "completed_with_errors", "completed_with_warnings", detail
        return "completed", "completed", "研究任务已完成"

    def _artifact(
        self,
        context: JobContext,
        kind: str,
        outcome: str,
        result: dict[str, Any],
        *,
        error_info: dict[str, Any] | None = None,
        telemetry: dict[str, Any] | None = None,
        persist_domain: bool = True,
    ) -> dict[str, Any]:
        payload = {
            "schema_version": "1.0",
            "kind": kind,
            "outcome": outcome,
            "result": result,
            "error_info": dict(error_info or {}),
            "telemetry": dict(telemetry or result.get("telemetry") or {}),
        }
        artifact = context.write_artifact(
            LAB_RESULT_KIND,
            payload,
            {
                "schema_version": "1.0",
                "lineage": {"spec_hash": context.spec_hash, "kind": kind},
            },
        )
        if persist_domain:
            self.service.store.save_worker_result(
                context.job_id,
                context.attempt,
                kind,
                outcome,
                result,
                error_info=error_info,
                telemetry=telemetry or result.get("telemetry") or {},
            )
        return artifact

    def _committed_result(self, context: JobContext, kind: str) -> JobOutcome | None:
        value = self.service.store.worker_result(context.job_id)
        if value is None or str(value.get("outcome") or "") not in {
            "completed", "completed_with_warnings",
        }:
            return None
        result = dict(value.get("result") or {})
        outcome = str(value["outcome"])
        artifact = self._artifact(
            context,
            kind,
            outcome,
            result,
            error_info=dict(value.get("error_info") or {}),
            telemetry=dict(value.get("telemetry") or {}),
            persist_domain=False,
        )
        runtime_status = "completed_with_errors" if outcome == "completed_with_warnings" else "completed"
        return JobOutcome(runtime_status, "已复用已提交的 Lab worker result", str(artifact["id"]))

    def _handle(self, context: JobContext, spec: dict[str, Any]) -> JobOutcome:
        kind = str(spec.get("kind") or "")
        if (
            kind not in LAB_KINDS
            or str(context.store.get(context.job_id).get("type") or "") != f"lab.{kind}"
        ):
            raise ValueError("Lab job type 与 immutable spec 不一致")
        committed = self._committed_result(context, kind)
        if committed is not None:
            context.emit("lab_result_reused", {"kind": kind})
            return committed
        params = dict(spec.get("params") or {})
        admission = dict(spec.get("preflight") or {})
        gate = self._resource_gates.get(str(spec.get("resource_class") or "cpu"))
        acquired = False

        def progress(
            value: int,
            phase: str,
            detail: str = "",
            *,
            event_type: str = "progress",
            metadata: dict[str, Any] | None = None,
        ) -> None:
            context.ensure_active()
            context.progress(value, phase, detail)
            if event_type != "progress" or metadata:
                payload = {
                    "progress": max(0, min(100, int(value))),
                    "phase": str(phase),
                    "detail": str(detail),
                    **dict(metadata or {}),
                }
                context.emit(event_type, payload)
                if event_type == "partition_checkpoint":
                    context.write_checkpoint(
                        LAB_PROGRESS_CHECKPOINT, context.spec_hash,
                        {"schema_version": "1.0", "type": event_type, **payload},
                    )

        try:
            if gate is not None:
                while not gate.acquire(timeout=0.1):
                    context.ensure_active()
                acquired = True
            current_admission = self.service.preflight(kind, params)
            require_runnable(current_admission)
            if admission != current_admission:
                context.emit("lab_preflight_refreshed", {
                    "kind": kind,
                    "previous_state": str(admission.get("state") or ""),
                    "current_state": str(current_admission.get("state") or ""),
                })
            result = self.service.run_job(
                {"id": context.job_id, "kind": kind, "params": params},
                progress=progress,
                cancelled=context.cancelled,
            )
            if not isinstance(result, dict):
                raise TypeError("Lab worker result 必须是 JSON 对象")
            context.ensure_active()
            status, outcome, detail = self._outcome(result)
            artifact = self._artifact(context, kind, outcome, result)
            context.emit("lab_domain_outcome", {"kind": kind, "outcome": outcome})
            return JobOutcome(status, detail, str(artifact["id"]))
        except InterruptedError:
            raise
        except (
            ArithmeticError, AttributeError, ImportError, LookupError, OSError,
            RuntimeError, sqlite3.Error, TypeError, ValueError,
        ) as exc:
            failure = classify_lab_error(exc)
            error_info = failure.to_dict()
            artifact = self._artifact(
                context, kind, "failed", {}, error_info=error_info,
            )
            context.emit(
                "lab_diagnostic",
                {"kind": kind, "diagnostic_code": failure.code, "error": failure.message},
            )
            return JobOutcome("failed", failure.message, str(artifact["id"]))
        finally:
            if acquired and gate is not None:
                gate.release()

    @staticmethod
    def _result_payload(store: UnifiedJobStore, job: dict[str, Any]) -> dict[str, Any]:
        artifact = None
        artifact_id = str(job.get("result_artifact_id") or "")
        if artifact_id:
            try:
                artifact = store.artifact(artifact_id)
            except (KeyError, RuntimeError, ValueError):
                artifact = None
        if artifact is None:
            artifact = store.latest_artifact(str(job["id"]), LAB_RESULT_KIND)
        payload = artifact.get("payload") if isinstance(artifact, dict) else None
        return dict(payload) if isinstance(payload, dict) else {}

    @classmethod
    def _project(
        cls, store: UnifiedJobStore, job: dict[str, Any], *, summary: bool = False,
    ) -> dict[str, Any]:
        job_type = str(job.get("type") or "")
        if job_type not in LAB_JOB_TYPES:
            raise KeyError(str(job.get("id") or ""))
        spec = dict(job.get("spec") or {})
        kind = str(spec.get("kind") or job_type.removeprefix("lab."))
        payload = {} if summary else cls._result_payload(store, job)
        public = UnifiedJobRuntime.public(job)
        error_info = dict(payload.get("error_info") or {})
        checkpoint = store.checkpoint(
            str(job["id"]), LAB_PROGRESS_CHECKPOINT, str(job.get("spec_hash") or ""),
        )
        public.update({
            "kind": kind,
            "params": {} if summary else dict(spec.get("params") or {}),
            "preflight": {} if summary else dict(spec.get("preflight") or {}),
            "dataset_id": str(spec.get("dataset_id") or ""),
            "resource_class": str(spec.get("resource_class") or "cpu"),
            "result": {} if summary else dict(payload.get("result") or {}),
            "outcome": str(payload.get("outcome") or public.get("outcome") or ""),
            "error_info": error_info,
            "error_code": str(error_info.get("code") or ""),
            "error": str(error_info.get("message") or ""),
            "telemetry": {} if summary else dict(payload.get("telemetry") or {}),
            "checkpoint": {} if summary else dict(checkpoint or {}),
            "worker": str(job.get("owner") or ""),
        })
        return public

    def get(self, job_id: str) -> dict[str, Any]:
        store = self._read_store()
        return self._project(store, store.get(job_id))

    def list(
        self,
        limit: int = 50,
        *,
        status: str | None = None,
        kind: str | None = None,
        cursor: str | None = None,
        offset: int = 0,
        summary: bool = False,
    ) -> list[dict[str, Any]]:
        store = self._read_store()
        values = [job for job in store.list(1000) if str(job.get("type") or "") in LAB_JOB_TYPES]
        if cursor:
            positions = [index for index, value in enumerate(values) if value["id"] == cursor]
            if positions:
                values = values[positions[0] + 1:]
        projected = [self._project(store, value, summary=summary) for value in values]
        if status:
            projected = [
                value for value in projected
                if value["status"] == status or value.get("outcome") == status
            ]
        if kind:
            projected = [value for value in projected if value["kind"] == kind]
        start = max(0, int(offset))
        return projected[start:start + max(1, min(500, int(limit)))]

    def events(
        self, job_id: str, after: int = 0, limit: int = 500,
    ) -> builtins.list[dict[str, Any]]:
        store = self._read_store()
        self._project(store, store.get(job_id), summary=True)
        return store.events(job_id, after, limit)

    def cancel(self, job_id: str) -> dict[str, Any]:
        runtime = self._ensure_runtime()
        self._project(runtime.store, runtime.store.get(job_id), summary=True)
        return self._project(runtime.store, runtime.store.cancel(job_id))

    def retry(self, job_id: str) -> dict[str, Any]:
        runtime = self._ensure_runtime()
        self._project(runtime.store, runtime.store.get(job_id), summary=True)
        return self._project(runtime.store, runtime.retry(job_id))

    def start(self) -> None:
        if self._owns_runtime():
            self._ensure_runtime().start()

    def pause(self) -> None:
        runtime = self._runtime
        if runtime is not None:
            runtime.pause()

    def resume(self) -> None:
        if self._owns_runtime():
            self._ensure_runtime().resume()

    def shutdown(self, timeout: float = 10.0) -> None:
        runtime = self._runtime
        if runtime is not None:
            runtime.stop(deadline_seconds=timeout)

    def snapshot(self) -> dict[str, Any]:
        runtime = self._runtime
        return runtime.snapshot() if runtime is not None else {
            "status": "disabled", "accepting": False, "active_tasks": 0,
        }

    def active_job_ids(self) -> builtins.list[str]:
        runtime = self._runtime
        if runtime is None:
            return []
        owner = runtime.identity.value
        return sorted(
            str(job["id"])
            for job in runtime.store.list(1000)
            if str(job.get("type") or "") in LAB_JOB_TYPES
            and str(job.get("owner") or "") == owner
            and str(job.get("status") or "") in {"running", "cancelling"}
        )

    def live_job_ids(self) -> set[str]:
        runtime = self._ensure_runtime()
        runtime.store.recover_expired()
        return {
            str(job["id"])
            for job in runtime.store.list(1000)
            if str(job.get("type") or "") in LAB_JOB_TYPES
            and str(job.get("status") or "") in ACTIVE_STATUSES
        }

    def scheduled_usage_hours(self) -> float:
        now = datetime.now(UTC)
        total = 0.0
        try:
            store = self._read_store()
            jobs = store.list(1000)
        except (FileNotFoundError, sqlite3.Error):
            return 0.0
        for job in jobs:
            if str(job.get("type") or "") not in LAB_JOB_TYPES:
                continue
            params = dict((job.get("spec") or {}).get("params") or {})
            if not params.get("_scheduled") or not job.get("started_at"):
                continue
            try:
                started = datetime.fromisoformat(str(job["started_at"]))
                finished = (
                    datetime.fromisoformat(str(job["finished_at"]))
                    if job.get("finished_at") else now
                )
            except ValueError:
                continue
            if started.astimezone(UTC).date() != now.date():
                continue
            total += max(0.0, (finished - started).total_seconds() / 3600)
        return total

    def overview(self) -> dict[str, Any]:
        try:
            jobs = self.list(500, summary=True)
        except (FileNotFoundError, sqlite3.Error):
            jobs = []
        statuses: dict[str, int] = {}
        for job in jobs:
            key = str(job.get("outcome") or job.get("status") or "unknown")
            statuses[key] = statuses.get(key, 0) + 1
        return {
            "job_statuses": statuses,
            "active_jobs": sum(str(job.get("status") or "") in ACTIVE_STATUSES for job in jobs),
        }


_MANAGERS: dict[str, LabJobManager] = {}
_MANAGERS_LOCK = threading.Lock()


def get_lab_job_manager() -> LabJobManager:
    key = str(get_config().data_root.resolve())
    with _MANAGERS_LOCK:
        manager = _MANAGERS.get(key)
        if manager is None:
            manager = LabJobManager()
            _MANAGERS[key] = manager
        return manager


def read_lab_job(job_id: str) -> dict[str, Any]:
    store = UnifiedJobStore(_jobs_path(), read_only=True)
    return LabJobManager._project(store, store.get(job_id))


def list_lab_jobs(
    limit: int = 50,
    *,
    status: str | None = None,
    kind: str | None = None,
    cursor: str | None = None,
    offset: int = 0,
    summary: bool = False,
) -> list[dict[str, Any]]:
    manager = LabJobManager.__new__(LabJobManager)
    manager._path = _jobs_path()
    manager._runtime = None
    manager._fixed_runtime = False
    manager._lock = threading.RLock()
    manager._resource_gates = {}
    try:
        return manager.list(
            limit,
            status=status,
            kind=kind,
            cursor=cursor,
            offset=offset,
            summary=summary,
        )
    except (FileNotFoundError, sqlite3.Error):
        return []


def lab_job_events(job_id: str, after: int = 0, limit: int = 500) -> list[dict[str, Any]]:
    store = UnifiedJobStore(_jobs_path(), read_only=True)
    LabJobManager._project(store, store.get(job_id), summary=True)
    return store.events(job_id, after, limit)


def lab_job_overview() -> dict[str, Any]:
    try:
        jobs = list_lab_jobs(500, summary=True)
    except (FileNotFoundError, sqlite3.Error):
        jobs = []
    statuses: dict[str, int] = {}
    for job in jobs:
        key = str(job.get("outcome") or job.get("status") or "unknown")
        statuses[key] = statuses.get(key, 0) + 1
    return {
        "job_statuses": statuses,
        "active_jobs": sum(str(job.get("status") or "") in ACTIVE_STATUSES for job in jobs),
    }


def shutdown_lab_job_managers(timeout: float = 10.0) -> None:
    with _MANAGERS_LOCK:
        managers = list(_MANAGERS.values())
    per_manager = max(0.05, timeout / max(1, len(managers)))
    for manager in managers:
        manager.shutdown(per_manager)
