"""Research Lake domain work backed by the unified job lifecycle."""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from pathlib import Path
from typing import Any

from quantmaster.config import get_config
from quantmaster.research.contracts import ExecutionPlan, RunManifest, utc_now
from quantmaster.research.engine import ResearchEngine
from quantmaster.research.kernel import Kernel
from quantmaster.runtime.jobs import (
    ACTIVE_STATUSES,
    JobContext,
    JobOutcome,
    UnifiedJobRuntime,
    UnifiedJobStore,
)

logger = logging.getLogger(__name__)

RESEARCH_TASK_TYPE = "research.lake"
RESEARCH_CHECKPOINT = "research.lake.progress"
RESEARCH_RESULT_KIND = "research.lake.result"


def _jobs_path() -> Path:
    return get_config().data_root / "jobs.sqlite"


class ResearchJobManager:
    """Own research planning and projection while the runtime owns lifecycle."""

    def __init__(
        self,
        engine: ResearchEngine | None = None,
        runtime: UnifiedJobRuntime | None = None,
    ) -> None:
        self.engine = engine or ResearchEngine()
        self._runtime = runtime
        self._fixed_runtime = runtime is not None
        self._path = self.engine.lake.root.parent / "jobs.sqlite"
        self._lock = threading.RLock()
        if runtime is not None:
            runtime.register(RESEARCH_TASK_TYPE, self._handle)

    @staticmethod
    def _owns_runtime() -> bool:
        return os.environ.get("QM_WEB_PROCESS") != "1"

    def _ensure_runtime(self) -> UnifiedJobRuntime:
        with self._lock:
            if self._runtime is not None:
                if self._fixed_runtime or self._runtime.store.path.resolve() == self._path.resolve():
                    return self._runtime
                if not self._runtime.idle:
                    raise RuntimeError("研究任务仍在旧数据目录运行，拒绝切换任务账本")
                self._runtime.stop()
            self._runtime = UnifiedJobRuntime(
                UnifiedJobStore(self._path), max_workers=1, dispatch=self._owns_runtime(),
            )
            self._runtime.register(RESEARCH_TASK_TYPE, self._handle)
            return self._runtime

    def _read_store(self) -> UnifiedJobStore:
        if self._runtime is not None:
            if self._fixed_runtime or self._runtime.store.path.resolve() == self._path.resolve():
                return self._runtime.store
        return UnifiedJobStore(self._path, read_only=True)

    def create(self, plan: ExecutionPlan, mode: str = "historical") -> dict[str, Any]:
        if plan.capability_blocks:
            detail = "；".join(
                f"{item['dataset_id']}: {item['detail']}" for item in plan.capability_blocks
            )
            raise ValueError(f"计划存在能力阻塞：{detail}")
        runtime = self._ensure_runtime()
        active = next(
            (
                job for job in runtime.store.list(200, job_type=RESEARCH_TASK_TYPE)
                if str(job["status"]) in ACTIVE_STATUSES
            ),
            None,
        )
        if active is not None:
            raise ValueError(f"已有研究数据任务正在运行：{active['id']}")
        job, _created = runtime.store.submit(
            RESEARCH_TASK_TYPE,
            {"mode": str(mode), "plan": plan.to_dict()},
            deadline_seconds=3600,
            max_attempts=8,
            algorithm_version="research-lake-v2",
        )
        if self._owns_runtime():
            runtime.start()
        return self.get(str(job["id"]))

    @staticmethod
    def _initial_state(context: JobContext, spec: dict[str, Any]) -> dict[str, Any]:
        previous = context.store.latest_artifact(context.job_id, RESEARCH_RESULT_KIND)
        if context.attempt > 1 and previous:
            payload = dict(previous["payload"])
            failed_indexes = [
                int(item["task_index"])
                for item in payload.get("failures") or ()
                if isinstance(item, dict) and isinstance(item.get("task_index"), int)
            ]
            if failed_indexes:
                manifest = dict(payload.get("manifest") or {})
                return {
                    "schema_version": "1.0",
                    "task_indexes": failed_indexes,
                    "next_index": 0,
                    "total": len(failed_indexes),
                    "succeeded": 0,
                    "failed": 0,
                    "failures": [],
                    "current_task": "",
                    "manifest": manifest,
                    "outcome": "",
                }
        checkpoint = context.load_checkpoint(RESEARCH_CHECKPOINT, context.spec_hash)
        if checkpoint:
            return dict(checkpoint)
        tasks = list((spec.get("plan") or {}).get("tasks") or ())
        return {
            "schema_version": "1.0",
            "task_indexes": list(range(len(tasks))),
            "next_index": 0,
            "total": len(tasks),
            "succeeded": 0,
            "failed": 0,
            "failures": [],
            "current_task": "",
            "manifest": {},
            "outcome": "",
        }

    @staticmethod
    def _checkpoint_manifest(
        run_id: str,
        plan: ExecutionPlan,
        attempt: int,
        started_at: str,
        inputs: list[dict[str, Any]],
        outputs: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "plan_hash": plan.plan_hash,
            "status": "running",
            "started_at": started_at,
            "input_partitions": inputs,
            "output_partitions": outputs,
            "attempt": attempt,
        }

    def _handle(self, context: JobContext, spec: dict[str, Any]) -> JobOutcome:
        plan = ExecutionPlan.from_dict(dict(spec["plan"]))
        kernel = Kernel(plan.backend)
        state = self._initial_state(context, spec)
        manifest_state = dict(state.get("manifest") or {})
        inputs: list[dict[str, Any]] = list(manifest_state.get("input_partitions") or ())
        outputs: list[dict[str, Any]] = list(manifest_state.get("output_partitions") or ())
        started_at = str(manifest_state.get("started_at") or utc_now())
        run_id = (
            context.job_id
            if context.attempt == 1
            else f"{context.job_id}.attempt-{context.attempt}"
        )
        task_indexes = [int(item) for item in state.get("task_indexes") or ()]
        failures = list(state.get("failures") or ())

        while int(state["next_index"]) < len(task_indexes):
            context.ensure_active()
            cursor = int(state["next_index"])
            task_index = task_indexes[cursor]
            if task_index < 0 or task_index >= len(plan.tasks):
                raise RuntimeError(f"任务索引越界：{task_index}")
            task = plan.tasks[task_index]
            state["current_task"] = task.key
            context.progress(
                round(100 * cursor / max(1, len(task_indexes))),
                "执行研究计划",
                task.key,
            )
            try:
                records = self.engine.execute_task(
                    plan, task, kernel=kernel, run_id=run_id,
                )
                context.ensure_active()
                (inputs if task.kind == "sync" else outputs).extend(records)
                state["succeeded"] = int(state["succeeded"]) + 1
                context.emit(
                    "research_task_completed",
                    {"task_index": task_index, "task": task.key},
                )
            except Exception as exc:
                from quantmaster.logging_config import redact_sensitive_text

                logger.exception(
                    "研究计划任务失败 job=%s task=%s", context.job_id, task.key,
                )
                failure = {
                    "task_index": task_index,
                    "task": task.to_dict(),
                    "error": redact_sensitive_text(exc)[:500],
                }
                failures.append(failure)
                state["failed"] = int(state["failed"]) + 1
                context.emit(
                    "research_task_failed",
                    {"task_index": task_index, "task": task.key, "error": failure["error"]},
                )
            state["next_index"] = cursor + 1
            state["current_task"] = ""
            state["failures"] = failures
            state["manifest"] = self._checkpoint_manifest(
                run_id,
                plan,
                context.attempt,
                started_at,
                inputs,
                outputs,
            )
            context.write_checkpoint(RESEARCH_CHECKPOINT, context.spec_hash, state)
            context.completed_unit(
                f"已完成 {state['next_index']}/{len(task_indexes)} 个研究任务"
            )

        diagnostics: list[dict[str, Any]] = []
        if not failures:
            try:
                diagnostics = self.engine._emit_diagnostics(plan, run_id)
            except Exception as exc:
                from quantmaster.logging_config import redact_sensitive_text

                logger.exception("研究诊断发布失败 job=%s", context.job_id)
                failures.append({
                    "task": "diagnostics",
                    "error": redact_sensitive_text(exc)[:500],
                })
        domain_status = "completed_with_errors" if failures else "completed"
        manifest = RunManifest(
            run_id=run_id,
            plan_hash=plan.plan_hash,
            status=domain_status,
            backend_requested=plan.backend,
            backend_used=kernel.backend_used,
            started_at=started_at,
            finished_at=utc_now(),
            input_partitions=tuple(inputs),
            output_partitions=tuple(outputs),
            warnings=tuple(filter(None, (*plan.warnings, kernel.fallback_reason))),
        ).to_dict()
        manifest["diagnostics"] = diagnostics
        manifest["attempt"] = context.attempt
        self.engine.lake.catalog.save_run(manifest)
        self.engine.lake.write_run_files(run_id, manifest)

        outcome = "completed_with_warnings" if failures else "completed"
        state.update({
            "total": len(task_indexes),
            "failures": failures,
            "failed": len(failures),
            "current_task": "",
            "manifest": manifest,
            "outcome": outcome,
        })
        artifact = context.write_artifact(
            RESEARCH_RESULT_KIND,
            state,
            {
                "schema_version": "1.0",
                "lineage": {"spec_hash": context.spec_hash, "plan_hash": plan.plan_hash},
            },
        )
        context.emit(
            "research_job_completed",
            {"outcome": outcome, "failed": len(failures)},
        )
        return JobOutcome("completed", "研究计划已完成", str(artifact["id"]))

    @staticmethod
    def _state(store: UnifiedJobStore, job: dict[str, Any]) -> dict[str, Any]:
        artifact = store.latest_artifact(str(job["id"]), RESEARCH_RESULT_KIND)
        if artifact:
            return dict(artifact["payload"])
        checkpoint = store.checkpoint(
            str(job["id"]), RESEARCH_CHECKPOINT, str(job["spec_hash"]),
        )
        return dict(checkpoint or {})

    @classmethod
    def _project(cls, store: UnifiedJobStore, job: dict[str, Any]) -> dict[str, Any]:
        if str(job.get("type")) != RESEARCH_TASK_TYPE:
            raise KeyError(str(job.get("id") or ""))
        spec = dict(job["spec"])
        plan = dict(spec.get("plan") or {})
        state = cls._state(store, job)
        task_indexes = list(state.get("task_indexes") or range(len(plan.get("tasks") or ())))
        failures = list(state.get("failures") or ())
        value = UnifiedJobRuntime.public(job)
        value.update({
            "mode": str(spec.get("mode") or "historical"),
            "plan": plan,
            "next_index": int(state.get("next_index") or 0),
            "total": int(state.get("total") or len(task_indexes)),
            "succeeded": int(state.get("succeeded") or 0),
            "failed": int(state.get("failed") or len(failures)),
            "current_task": str(state.get("current_task") or ""),
            "failures": failures,
            "manifest": dict(state.get("manifest") or {}),
            "task_indexes": task_indexes,
            "outcome": str(state.get("outcome") or ""),
            "active": str(job["status"]) in {"queued", "running", "cancelling"},
        })
        return value

    @staticmethod
    def public(value: dict[str, Any]) -> dict[str, Any]:
        result = dict(value)
        plan = dict(result.pop("plan", {}) or {})
        result["plan_summary"] = {
            key: plan.get(key)
            for key in (
                "id",
                "start",
                "end",
                "asset_classes",
                "frequency",
                "datasets",
                "backend",
                "estimated_rows",
                "estimated_bytes",
                "plan_hash",
            )
        }
        result["plan_summary"]["selected_specs"] = len(plan.get("selected_specs") or ())
        result["plan_summary"]["tasks"] = len(plan.get("tasks") or ())
        return result

    def get(self, job_id: str) -> dict[str, Any]:
        try:
            store = self._read_store()
            return self._project(store, store.get(job_id))
        except (FileNotFoundError, sqlite3.Error) as exc:
            raise KeyError(job_id) from exc

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        try:
            store = self._read_store()
            return [
                self._project(store, job)
                for job in store.list(limit, job_type=RESEARCH_TASK_TYPE)
            ]
        except (FileNotFoundError, sqlite3.Error):
            return []

    def events(self, job_id: str, after: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        store = self._read_store()
        self._project(store, store.get(job_id))
        return store.events(job_id, after, limit)

    def wait(self, job_id: str, poll_seconds: float = 0.1) -> dict[str, Any]:
        while True:
            value = self.get(job_id)
            if value["status"] not in {"queued", "running", "cancelling"}:
                return value
            threading.Event().wait(max(0.01, poll_seconds))

    def cancel(self, job_id: str) -> dict[str, Any]:
        runtime = self._ensure_runtime()
        self._project(runtime.store, runtime.store.get(job_id))
        return self._project(runtime.store, runtime.store.cancel(job_id))

    def resume(self, job_id: str) -> dict[str, Any]:
        runtime = self._ensure_runtime()
        source = self._project(runtime.store, runtime.store.get(job_id))
        retryable = source["status"] in {"failed", "cancelled", "interrupted"}
        retryable = retryable or source.get("outcome") == "completed_with_warnings"
        if not retryable:
            raise ValueError("当前任务不能续跑")
        return self._project(runtime.store, runtime.retry(job_id))

    def start(self) -> None:
        if self._owns_runtime():
            self._ensure_runtime().start()

    def shutdown(self, timeout: float = 10.0) -> None:
        with self._lock:
            runtime = self._runtime
        if runtime is not None:
            runtime.stop(deadline_seconds=timeout)


def read_research_job(job_id: str) -> dict[str, Any]:
    store = UnifiedJobStore(_jobs_path(), read_only=True)
    return ResearchJobManager._project(store, store.get(job_id))


def list_research_jobs(limit: int = 50) -> list[dict[str, Any]]:
    try:
        store = UnifiedJobStore(_jobs_path(), read_only=True)
        return [
            ResearchJobManager._project(store, job)
            for job in store.list(limit, job_type=RESEARCH_TASK_TYPE)
        ]
    except (FileNotFoundError, sqlite3.Error):
        return []


def research_job_events(
    job_id: str, after: int = 0, limit: int = 500,
) -> list[dict[str, Any]]:
    store = UnifiedJobStore(_jobs_path(), read_only=True)
    ResearchJobManager._project(store, store.get(job_id))
    return store.events(job_id, after, limit)


_MANAGERS: dict[str, ResearchJobManager] = {}
_MANAGERS_LOCK = threading.Lock()


def get_research_job_manager() -> ResearchJobManager:
    """Keep one manager per hot-swappable data root without import-time writes."""

    key = str(get_config().data_root.resolve())
    with _MANAGERS_LOCK:
        return _MANAGERS.setdefault(key, ResearchJobManager())


def shutdown_research_job_managers(timeout: float = 10.0) -> None:
    with _MANAGERS_LOCK:
        managers = list(_MANAGERS.values())
    per_manager = max(0.05, timeout / max(1, len(managers)))
    for manager in managers:
        manager.shutdown(per_manager)
