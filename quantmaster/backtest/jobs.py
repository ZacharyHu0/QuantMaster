"""Backtest admission and projections backed by the unified job lifecycle."""

from __future__ import annotations

import builtins
import os
import sqlite3
import threading
from typing import Any

from quantmaster.backtest.spec import BacktestSpec, pin_decision_strategy, preflight_strategy
from quantmaster.backtest.workbench import BacktestService, BacktestStore
from quantmaster.config import get_config
from quantmaster.runtime.jobs import (
    JobContext,
    JobOutcome,
    ProcessJobContext,
    UnifiedJobRuntime,
    UnifiedJobStore,
)
from quantmaster.runtime.problems import OperationProblem

BACKTEST_TASK_TYPE = "backtest.run"
BACKTEST_RESULT_KIND = "backtest.result"
BACKTEST_ALGORITHM_VERSION = "backtest-v2"


def _result_payload(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "name": str(result["name"]),
        "spec": dict(result["spec"]),
        "outcome": str(result["outcome"]),
        "manifest": dict(result.get("manifest") or {}),
        "summary": dict(result.get("summary") or {}),
        "artifact": dict(result.get("artifact") or {}),
        "diagnostic": dict(result.get("diagnostic") or {}),
    }


def _publish_result(
    context: JobContext | ProcessJobContext,
    domain_store: BacktestStore,
    result: dict[str, Any],
    *,
    persist_domain: bool,
) -> dict[str, Any]:
    payload = _result_payload(result)
    if persist_domain:
        domain_store.save_result(
            context.job_id,
            context.attempt,
            name=payload["name"],
            spec=payload["spec"],
            outcome=payload["outcome"],
            manifest=payload["manifest"],
            summary=payload["summary"],
            artifact=payload["artifact"],
            diagnostic=payload["diagnostic"],
        )
    return context.write_artifact(
        BACKTEST_RESULT_KIND,
        payload,
        {
            "schema_version": "1.0",
            "lineage": {
                "spec_hash": context.spec_hash,
                "config_hash": str(payload["spec"].get("config_hash") or ""),
            },
        },
    )


def _committed_result(
    context: JobContext | ProcessJobContext,
    domain_store: BacktestStore,
) -> JobOutcome | None:
    values = domain_store.results(context.job_id)
    completed = next(
        (
            value for value in reversed(values)
            if str(value.get("outcome") or "") in {"completed", "completed_with_warnings"}
        ),
        None,
    )
    if completed is None:
        return None
    restored = domain_store.result(
        context.job_id,
        attempt=int(completed["attempt"]),
        include_artifact=True,
    )
    if restored is None:
        return None
    artifact = _publish_result(context, domain_store, restored, persist_domain=False)
    status = (
        "completed_with_errors"
        if restored["outcome"] == "completed_with_warnings"
        else "completed"
    )
    context.emit("backtest_result_reused", {"source_attempt": int(restored["attempt"])})
    return JobOutcome(status, "已复用已提交的回测领域结果", str(artifact["id"]))


def _execute(
    context: JobContext | ProcessJobContext,
    immutable_spec: dict[str, Any],
    *,
    service: BacktestService,
    domain_store: BacktestStore,
) -> JobOutcome:
    job = context.store.get(context.job_id)
    if str(job.get("type") or "") != BACKTEST_TASK_TYPE:
        raise ValueError("回测 job type 与 immutable spec 不一致")
    if dict(job.get("spec") or {}) != dict(immutable_spec):
        raise ValueError("回测执行参数与 immutable spec 不一致")
    committed = _committed_result(context, domain_store)
    if committed is not None:
        return committed

    name = str(immutable_spec.get("name") or "回测")
    config = dict(immutable_spec.get("config") or {})
    spec = BacktestSpec.model_validate(config)

    def progress(value: int, phase: str, detail: str = "") -> None:
        context.progress(value, phase, detail)

    try:
        manifest, payload = service.run(
            context.job_id,
            name,
            spec,
            progress=progress,
            cancelled=context.cancelled,
        )
        context.ensure_active()
        warnings = manifest.get("warnings")
        warnings = warnings if isinstance(warnings, list) else []
        outcome = "completed_with_warnings" if warnings else "completed"
        result = {
            "name": name,
            "spec": immutable_spec,
            "outcome": outcome,
            "manifest": manifest,
            "summary": dict(payload.get("summary") or {}),
            "artifact": dict(payload.get("artifact") or {}),
            "diagnostic": {},
        }
        artifact = _publish_result(context, domain_store, result, persist_domain=True)
        context.emit("backtest_domain_outcome", {"outcome": outcome})
        return JobOutcome(
            "completed_with_errors" if warnings else "completed",
            "回测完成，但含数据质量提示" if warnings else "回测完成",
            str(artifact["id"]),
        )
    except InterruptedError:
        raise
    except OperationProblem as exc:
        can_continue = bool(exc.problem.get("can_continue"))
        outcome = "needs_confirmation" if can_continue else "failed"
        summary = {
            "problem": dict(exc.problem),
            "data_quality": dict(exc.data_quality or {}),
        }
        diagnostic = {
            "code": str(exc.problem.get("code") or "backtest_blocked"),
            "message": str(exc.problem.get("message") or "回测被数据门禁阻止"),
            "can_continue": can_continue,
        }
        result = {
            "name": name,
            "spec": immutable_spec,
            "outcome": outcome,
            "manifest": {},
            "summary": summary,
            "artifact": {},
            "diagnostic": diagnostic,
        }
        artifact = _publish_result(context, domain_store, result, persist_domain=True)
        context.emit("backtest_diagnostic", diagnostic)
        return JobOutcome("failed", diagnostic["message"], str(artifact["id"]))
    except (
        ArithmeticError,
        AttributeError,
        ImportError,
        LookupError,
        OSError,
        RuntimeError,
        sqlite3.Error,
        TypeError,
        ValueError,
    ) as exc:
        diagnostic = {
            "code": "backtest_execution_failed",
            "message": str(exc)[:1500],
        }
        result = {
            "name": name,
            "spec": immutable_spec,
            "outcome": "failed",
            "manifest": {},
            "summary": {},
            "artifact": {},
            "diagnostic": diagnostic,
        }
        artifact = _publish_result(context, domain_store, result, persist_domain=True)
        context.emit("backtest_diagnostic", diagnostic)
        return JobOutcome("failed", diagnostic["message"], str(artifact["id"]))


def run_backtest_job(
    context: ProcessJobContext,
    immutable_spec: dict[str, Any],
) -> JobOutcome:
    """Importable compute-child entrypoint for CPU-isolated backtests."""

    root = context.store.path.parent
    return _execute(
        context,
        immutable_spec,
        service=BacktestService(),
        domain_store=BacktestStore(root / "backtests.sqlite", root / "backtests"),
    )


class BacktestJobManager:
    """Own backtest admission/projection while UnifiedJobRuntime owns lifecycle."""

    def __init__(
        self,
        store: BacktestStore | None = None,
        service: BacktestService | None = None,
        runtime: UnifiedJobRuntime | None = None,
    ):
        root = runtime.store.path.parent if runtime is not None else get_config().data_root
        self._jobs_path = runtime.store.path if runtime is not None else root / "jobs.sqlite"
        self._domain_path = store.path if store is not None else root / "backtests.sqlite"
        self._artifact_root = store.artifact_root if store is not None else root / "backtests"
        self._store = store
        self.service = service or BacktestService()
        self._runtime = runtime
        self._fixed_runtime = runtime is not None
        self._lock = threading.RLock()
        if runtime is not None:
            self._register(runtime)

    @staticmethod
    def _owns_runtime() -> bool:
        return os.environ.get("QM_WEB_PROCESS") != "1"

    def _register(self, runtime: UnifiedJobRuntime) -> None:
        runtime.register(
            BACKTEST_TASK_TYPE,
            self._handle,
            process_entrypoint="quantmaster.backtest.jobs:run_backtest_job",
        )

    def _ensure_runtime(self) -> UnifiedJobRuntime:
        with self._lock:
            if self._runtime is not None:
                if not self._fixed_runtime and self._runtime.snapshot()["status"] == "stopped":
                    self._runtime = None
                else:
                    return self._runtime
            self._runtime = UnifiedJobRuntime(
                UnifiedJobStore(self._jobs_path),
                max_workers=1,
                dispatch=self._owns_runtime(),
            )
            self._register(self._runtime)
            return self._runtime

    def _read_job_store(self) -> UnifiedJobStore:
        runtime = self._runtime
        if runtime is not None and runtime.store.path.resolve() == self._jobs_path.resolve():
            return runtime.store
        return UnifiedJobStore(self._jobs_path, read_only=True)

    def _domain_store(self, *, write: bool = False) -> BacktestStore | None:
        if self._store is not None and (not write or not self._store.read_only):
            return self._store
        if not write and not self._domain_path.is_file():
            return None
        self._store = BacktestStore(
            self._domain_path,
            self._artifact_root,
            read_only=not write,
        )
        return self._store

    def _handle(self, context: JobContext, immutable_spec: dict[str, Any]) -> JobOutcome:
        domain_store = self._domain_store(write=True)
        if domain_store is None:  # pragma: no cover - write always creates the store
            raise RuntimeError("回测领域结果库不可用")
        return _execute(
            context,
            immutable_spec,
            service=self.service,
            domain_store=domain_store,
        )

    def enqueue(self, spec: BacktestSpec) -> dict[str, Any]:
        preflight_strategy(spec)
        strategy = pin_decision_strategy(spec.strategy, spec.universe)
        if strategy is not spec.strategy:
            spec = spec.model_copy(update={"strategy": strategy})
        name = spec.name.strip() or f"{spec.strategy.kind} · {spec.universe} · {spec.start}"
        immutable_spec = {
            "name": name,
            "config": spec.model_dump(mode="json"),
            "config_hash": spec.snapshot_hash,
        }
        runtime = self._ensure_runtime()
        job, _created = runtime.submit(
            BACKTEST_TASK_TYPE,
            immutable_spec,
            algorithm_version=BACKTEST_ALGORITHM_VERSION,
            deadline_seconds=3600,
            max_attempts=8,
        )
        return self._project(runtime.store, job)

    def _domain_result(
        self,
        job: dict[str, Any],
        *,
        include_artifact: bool,
    ) -> dict[str, Any] | None:
        store = self._domain_store()
        if store is None:
            return None
        return store.result(
            str(job["id"]),
            attempt=int(job.get("attempt") or 1),
            include_artifact=include_artifact,
        ) or store.result(str(job["id"]), include_artifact=include_artifact)

    def _project(
        self,
        job_store: UnifiedJobStore,
        job: dict[str, Any],
        *,
        include_artifact: bool = False,
    ) -> dict[str, Any]:
        if str(job.get("type") or "") != BACKTEST_TASK_TYPE:
            raise KeyError(str(job.get("id") or ""))
        immutable_spec = dict(job.get("spec") or {})
        config = dict(immutable_spec.get("config") or {})
        result = self._domain_result(job, include_artifact=include_artifact)
        public = UnifiedJobRuntime.public(job)
        diagnostic = dict((result or {}).get("diagnostic") or {})
        public.update({
            "name": str(immutable_spec.get("name") or "回测"),
            "config": config,
            "config_hash": str(immutable_spec.get("config_hash") or ""),
            "manifest": dict((result or {}).get("manifest") or {}),
            "result": dict((result or {}).get("summary") or {}),
            "error": str(diagnostic.get("message") or ""),
            "diagnostic": diagnostic,
            "worker": str(job.get("owner") or ""),
            "legacy_read_only": config.get("strategy", {}).get("kind") == "swing",
            "outcome": str((result or {}).get("outcome") or public.get("outcome") or ""),
        })
        if include_artifact and result is not None:
            public["artifact"] = dict(result.get("artifact") or {})
        return public

    def get(self, job_id: str, *, include_artifact: bool = False) -> dict[str, Any]:
        store = self._read_job_store()
        return self._project(store, store.get(job_id), include_artifact=include_artifact)

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        store = self._read_job_store()
        jobs = [
            value for value in store.list(1000, job_type=BACKTEST_TASK_TYPE)
            if str(value.get("type") or "") == BACKTEST_TASK_TYPE
        ]
        return [self._project(store, value) for value in jobs[:max(1, min(200, int(limit)))]]

    def events(
        self,
        job_id: str,
        after: int = 0,
        limit: int = 500,
    ) -> builtins.list[dict[str, Any]]:
        store = self._read_job_store()
        self._project(store, store.get(job_id))
        return store.events(job_id, after, limit)

    def cancel(self, job_id: str) -> dict[str, Any]:
        runtime = self._ensure_runtime()
        self._project(runtime.store, runtime.store.get(job_id))
        return self._project(runtime.store, runtime.store.cancel(job_id))

    def retry(self, job_id: str) -> dict[str, Any]:
        runtime = self._ensure_runtime()
        current = self._project(runtime.store, runtime.store.get(job_id))
        if current["legacy_read_only"]:
            raise ValueError("旧 Swing 回测仅供历史查看，不能重试")
        return self._project(runtime.store, runtime.retry(job_id))

    def compare(self, run_ids: list[str]) -> dict[str, Any]:
        unique = list(dict.fromkeys(run_ids))
        if not 2 <= len(unique) <= 4:
            raise ValueError("请选择 2–4 个回测进行比较")
        runs = []
        for run_id in unique:
            run = self.get(run_id, include_artifact=True)
            artifact = run.get("artifact")
            if run["status"] != "completed" or not isinstance(artifact, dict) or not artifact:
                raise ValueError(f"回测 {run['name']} 尚未完成")
            runs.append({
                "id": run_id,
                "name": run["name"],
                "config": run["config"],
                "metrics": artifact["metrics"],
                "nav": artifact["nav"],
                "warnings": artifact.get("manifest", {}).get("warnings", []),
            })
        return {"runs": runs}

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

    @property
    def idle(self) -> bool:
        runtime = self._runtime
        return runtime is None or runtime.idle

    def snapshot(self) -> dict[str, Any]:
        runtime = self._runtime
        return runtime.snapshot() if runtime is not None else {
            "status": "disabled",
            "accepting": False,
            "active_tasks": 0,
        }


_MANAGERS: dict[str, BacktestJobManager] = {}
_MANAGERS_LOCK = threading.Lock()


def get_backtest_job_manager() -> BacktestJobManager:
    key = str(get_config().data_root.resolve())
    with _MANAGERS_LOCK:
        return _MANAGERS.setdefault(key, BacktestJobManager())


def read_backtest_job(job_id: str, *, include_artifact: bool = False) -> dict[str, Any]:
    manager = BacktestJobManager.__new__(BacktestJobManager)
    root = get_config().data_root
    manager._jobs_path = root / "jobs.sqlite"
    manager._domain_path = root / "backtests.sqlite"
    manager._artifact_root = root / "backtests"
    manager._store = None
    manager._runtime = None
    manager._fixed_runtime = False
    manager._lock = threading.RLock()
    store = UnifiedJobStore(manager._jobs_path, read_only=True)
    return manager._project(store, store.get(job_id), include_artifact=include_artifact)


def list_backtest_jobs(limit: int = 50) -> list[dict[str, Any]]:
    manager = BacktestJobManager.__new__(BacktestJobManager)
    root = get_config().data_root
    manager._jobs_path = root / "jobs.sqlite"
    manager._domain_path = root / "backtests.sqlite"
    manager._artifact_root = root / "backtests"
    manager._store = None
    manager._runtime = None
    manager._fixed_runtime = False
    manager._lock = threading.RLock()
    try:
        return manager.list(limit)
    except (FileNotFoundError, sqlite3.Error):
        return []


def backtest_job_events(job_id: str, after: int = 0, limit: int = 500) -> list[dict[str, Any]]:
    store = UnifiedJobStore(get_config().data_root / "jobs.sqlite", read_only=True)
    job = store.get(job_id)
    if str(job.get("type") or "") != BACKTEST_TASK_TYPE:
        raise KeyError(job_id)
    return store.events(job_id, after, limit)


def shutdown_backtest_job_managers(timeout: float = 10.0) -> None:
    with _MANAGERS_LOCK:
        managers = list(_MANAGERS.values())
        _MANAGERS.clear()
    for manager in managers:
        manager.shutdown(timeout)
