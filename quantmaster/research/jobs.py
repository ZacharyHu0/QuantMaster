"""Persistent, cancellable background jobs for research data and computation plans."""

from __future__ import annotations

import threading
import uuid
from typing import Any

from quantmaster.research.contracts import ExecutionPlan, RunManifest, utc_now
from quantmaster.research.engine import ResearchEngine
from quantmaster.research.kernel import Kernel


class ResearchJobManager:
    def __init__(self, engine: ResearchEngine | None = None):
        self.engine = engine or ResearchEngine()
        self.catalog = self.engine.lake.catalog
        self.catalog.recover_interrupted_jobs()
        self._lock = threading.RLock()
        self._threads: dict[str, threading.Thread] = {}

    def create(self, plan: ExecutionPlan, mode: str = "historical") -> dict[str, Any]:
        if plan.capability_blocks:
            detail = "；".join(
                f"{item['dataset_id']}: {item['detail']}" for item in plan.capability_blocks
            )
            raise ValueError(f"计划存在能力阻塞：{detail}")
        with self._lock:
            active = next((
                item for item in self.catalog.jobs(100)
                if item["status"] in {"running", "cancelling"}
            ), None)
            if active:
                raise ValueError(f"已有研究数据任务正在运行：{active['id']}")
            job_id = uuid.uuid4().hex
            payload = plan.to_dict()
            payload["id"] = job_id
            self.catalog.create_job(job_id, mode, payload)
            self._start(job_id)
        return self.get(job_id)

    def _start(self, job_id: str) -> None:
        current = self._threads.get(job_id)
        if current and current.is_alive():
            return
        thread = threading.Thread(
            target=self._run, args=(job_id,), name=f"research-job-{job_id[:8]}", daemon=True,
        )
        self._threads[job_id] = thread
        thread.start()

    def _run(self, job_id: str) -> None:
        job = self.catalog.job(job_id)
        if not job:
            return
        plan = ExecutionPlan.from_dict(job["plan"])
        kernel = Kernel(plan.backend)
        failures = list(job["failures"])
        inputs: list[dict[str, Any]] = list(job["manifest"].get("input_partitions") or ())
        outputs: list[dict[str, Any]] = list(job["manifest"].get("output_partitions") or ())
        started_at = str(job["manifest"].get("started_at") or utc_now())
        self.catalog.update_job(job_id, manifest_json={
            "run_id": job_id, "plan_hash": plan.plan_hash, "status": "running",
            "started_at": started_at, "input_partitions": inputs,
            "output_partitions": outputs,
        })
        while True:
            job = self.catalog.job(job_id)
            if not job:
                return
            index = int(job["next_index"])
            if job["cancel_requested"]:
                self.catalog.update_job(
                    job_id, status="cancelled", current_task="",
                    manifest_json={
                        "run_id": job_id, "plan_hash": plan.plan_hash, "status": "cancelled",
                        "started_at": started_at, "finished_at": utc_now(),
                        "input_partitions": inputs,
                        "output_partitions": outputs,
                    },
                )
                return
            if index >= len(plan.tasks):
                status = "completed_with_errors" if failures else "completed"
                diagnostics = []
                if not failures:
                    try:
                        diagnostics = self.engine._emit_diagnostics(plan, job_id)
                    except Exception as exc:
                        failures.append({"task": "diagnostics", "error": str(exc)[:500]})
                        status = "completed_with_errors"
                manifest = RunManifest(
                    run_id=job_id, plan_hash=plan.plan_hash, status=status,
                    backend_requested=plan.backend, backend_used=kernel.backend_used,
                    started_at=started_at, finished_at=utc_now(),
                    input_partitions=tuple(inputs),
                    output_partitions=tuple(outputs),
                    warnings=tuple(filter(None, (*plan.warnings, kernel.fallback_reason))),
                ).to_dict()
                manifest["diagnostics"] = diagnostics
                self.engine.lake.catalog.save_run(manifest)
                self.engine.lake.write_run_files(job_id, manifest)
                self.catalog.update_job(
                    job_id, status=status, current_task="", failures_json=failures,
                    manifest_json=manifest,
                )
                return
            task = plan.tasks[index]
            self.catalog.update_job(job_id, current_task=task.key)
            try:
                records = self.engine.execute_task(
                    plan, task, kernel=kernel, run_id=job_id,
                )
                (inputs if task.kind == "sync" else outputs).extend(records)
                self.catalog.update_job(
                    job_id, next_index=index + 1, succeeded=int(job["succeeded"]) + 1,
                    current_task="", manifest_json={
                        "run_id": job_id, "plan_hash": plan.plan_hash, "status": "running",
                        "started_at": started_at, "input_partitions": inputs,
                        "output_partitions": outputs,
                    },
                )
            except Exception as exc:
                failures.append({"task": task.to_dict(), "error": str(exc)[:500]})
                self.catalog.update_job(
                    job_id, next_index=index + 1, failed=int(job["failed"]) + 1,
                    current_task="", failures_json=failures,
                )

    def get(self, job_id: str) -> dict[str, Any]:
        value = self.catalog.job(job_id)
        if value is None:
            raise KeyError(job_id)
        value["active"] = bool(
            self._threads.get(job_id) and self._threads[job_id].is_alive()
        )
        return value

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        return [self.get(item["id"]) for item in self.catalog.jobs(limit)]

    @staticmethod
    def public(value: dict[str, Any]) -> dict[str, Any]:
        result = dict(value)
        plan = result.pop("plan", {})
        result["plan_summary"] = {
            key: plan.get(key) for key in (
                "id", "start", "end", "asset_classes", "frequency", "datasets",
                "backend", "estimated_rows", "estimated_bytes", "plan_hash",
            )
        }
        result["plan_summary"]["selected_specs"] = len(plan.get("selected_specs") or ())
        result["plan_summary"]["tasks"] = len(plan.get("tasks") or ())
        return result

    def wait(self, job_id: str, poll_seconds: float = 0.1) -> dict[str, Any]:
        while True:
            value = self.get(job_id)
            if value["status"] not in {"running", "cancelling"}:
                return value
            threading.Event().wait(max(0.01, poll_seconds))

    def cancel(self, job_id: str) -> dict[str, Any]:
        job = self.get(job_id)
        if job["status"] not in {"running", "cancelling"}:
            raise ValueError("当前任务不能取消")
        self.catalog.update_job(job_id, status="cancelling", cancel_requested=1)
        return self.get(job_id)

    def resume(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            job = self.get(job_id)
            if job["status"] not in {"cancelled", "interrupted", "completed_with_errors"}:
                raise ValueError("当前任务不能续跑")
            if job["status"] == "completed_with_errors":
                failed_tasks = [
                    item["task"] for item in job["failures"]
                    if isinstance(item.get("task"), dict)
                ]
                if not failed_tasks:
                    raise ValueError("没有可重试的数据任务")
                plan = dict(job["plan"])
                plan["tasks"] = failed_tasks
                self.catalog.update_job(
                    job_id, status="running", next_index=0, succeeded=0, failed=0,
                    cancel_requested=0, failures_json=[], current_task="",
                )
                with self.catalog._connect() as connection:
                    from quantmaster.research.contracts import canonical_json

                    connection.execute(
                        "UPDATE research_jobs SET plan_json=?,total=? WHERE id=?",
                        (canonical_json(plan), len(failed_tasks), job_id),
                    )
            else:
                self.catalog.update_job(
                    job_id, status="running", cancel_requested=0, current_task="",
                )
            self._start(job_id)
        return self.get(job_id)


_MANAGERS: dict[str, ResearchJobManager] = {}
_MANAGERS_LOCK = threading.Lock()


def get_research_job_manager() -> ResearchJobManager:
    """Keep one manager per hot-swappable data root without mutating at import time."""
    from quantmaster.config import get_config

    key = str((get_config().data_root / "research_lake").resolve())
    with _MANAGERS_LOCK:
        return _MANAGERS.setdefault(key, ResearchJobManager())
