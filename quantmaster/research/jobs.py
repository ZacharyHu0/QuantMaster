"""Leased, cancellable background jobs for research execution plans."""

from __future__ import annotations

import logging
import threading
import uuid
from typing import Any

from quantmaster.research.contracts import ExecutionPlan, RunManifest, utc_now
from quantmaster.research.engine import ResearchEngine
from quantmaster.research.kernel import Kernel
from quantmaster.runtime.jobs import WorkerIdentity

logger = logging.getLogger(__name__)


class ResearchJobManager:
    """One process worker; SQLite leases coordinate all other processes."""

    def __init__(self, engine: ResearchEngine | None = None):
        self.engine = engine or ResearchEngine()
        self.catalog = self.engine.lake.catalog
        self.identity = WorkerIdentity.create("research")
        self.catalog.recover_interrupted_jobs()
        self._lock = threading.RLock()
        self._threads: dict[str, threading.Thread] = {}
        self._stop = threading.Event()
        self._accepting = True

    def create(self, plan: ExecutionPlan, mode: str = "historical") -> dict[str, Any]:
        if plan.capability_blocks:
            detail = "；".join(
                f"{item['dataset_id']}: {item['detail']}" for item in plan.capability_blocks
            )
            raise ValueError(f"计划存在能力阻塞：{detail}")
        with self._lock:
            if not self._accepting:
                raise RuntimeError("研究任务执行器正在停止，暂不接受新任务")
            job_id = uuid.uuid4().hex
            self.catalog.create_job(job_id, mode, plan.to_dict())
            self._start(job_id)
        return self.get(job_id)

    def start(self) -> None:
        """Allow a manager to be reused across repeated ASGI lifespan cycles."""
        with self._lock:
            if any(thread.is_alive() for thread in self._threads.values()):
                self._accepting = True
                return
            self._stop.clear()
            self._accepting = True
            self.catalog.recover_interrupted_jobs()

    def _start(self, job_id: str) -> None:
        current = self._threads.get(job_id)
        if current and current.is_alive():
            return
        thread = threading.Thread(
            target=self._run, args=(job_id,), name=f"research-job-{job_id[:8]}", daemon=True,
        )
        self._threads[job_id] = thread
        thread.start()

    def _heartbeat(self, job_id: str, stop: threading.Event, alive: threading.Event) -> None:
        while not stop.wait(5.0):
            if self.catalog.heartbeat_job(job_id, self.identity.value):
                continue
            alive.clear()
            logger.warning("研究任务租约已丢失 job=%s owner=%s", job_id, self.identity.value)
            return

    def _owned_update(self, job_id: str, **changes: Any) -> dict[str, Any]:
        return self.catalog.update_job(
            job_id, expected_owner=self.identity.value, **changes,
        )

    def _interrupt(self, job_id: str, attempt: int, reason: str) -> None:
        try:
            self._owned_update(
                job_id, status="interrupted", current_task="", owner="", lease_expires=0,
            )
            self.catalog.append_job_event(job_id, attempt, {
                "type": "interrupted", "reason": reason,
            })
        except RuntimeError:
            pass

    def _run(self, job_id: str) -> None:
        if not self.catalog.claim_job(job_id, self.identity.value):
            return
        job = self.catalog.job(job_id)
        if not job:
            return
        attempt = int(job["attempt"])
        heartbeat_stop = threading.Event()
        lease_alive = threading.Event()
        lease_alive.set()
        heartbeat = threading.Thread(
            target=self._heartbeat,
            args=(job_id, heartbeat_stop, lease_alive),
            name=f"research-heartbeat-{job_id[:8]}",
            daemon=True,
        )
        heartbeat.start()
        try:
            self._execute(job_id, attempt, lease_alive)
        except RuntimeError as exc:
            if "租约已丢失" not in str(exc):
                logger.exception("研究任务运行时失败 job=%s", job_id)
                self._interrupt(job_id, attempt, str(exc)[:300])
        except Exception as exc:
            logger.exception("研究任务意外失败 job=%s", job_id)
            try:
                self._owned_update(
                    job_id, status="failed", current_task="", owner="", lease_expires=0,
                    failures_json=[{"error": str(exc)[:500]}],
                )
                self.catalog.append_job_event(job_id, attempt, {
                    "type": "failed", "error": str(exc)[:500],
                })
            except RuntimeError:
                pass
        finally:
            heartbeat_stop.set()
            heartbeat.join(timeout=1.0)

    def _execute(self, job_id: str, attempt: int, lease_alive: threading.Event) -> None:
        job = self.catalog.job(job_id)
        if not job:
            return
        plan = ExecutionPlan.from_dict(job["plan"])
        kernel = Kernel(plan.backend)
        failures = list(job["failures"])
        inputs: list[dict[str, Any]] = list(job["manifest"].get("input_partitions") or ())
        outputs: list[dict[str, Any]] = list(job["manifest"].get("output_partitions") or ())
        started_at = str(job["manifest"].get("started_at") or utc_now())
        self._owned_update(job_id, manifest_json={
            "run_id": job_id, "plan_hash": plan.plan_hash, "status": "running",
            "started_at": started_at, "input_partitions": inputs,
            "output_partitions": outputs, "attempt": attempt,
        })
        while True:
            if self._stop.is_set():
                self._interrupt(job_id, attempt, "process_shutdown")
                return
            if not lease_alive.is_set():
                return
            job = self.catalog.job(job_id)
            if not job or job.get("owner") != self.identity.value:
                return
            cursor = int(job["next_index"])
            task_indexes = [int(item) for item in job.get("task_indexes") or ()]
            if job["cancel_requested"]:
                self._owned_update(
                    job_id, status="cancelled", current_task="", owner="", lease_expires=0,
                    manifest_json={
                        "run_id": job_id, "plan_hash": plan.plan_hash, "status": "cancelled",
                        "started_at": started_at, "finished_at": utc_now(),
                        "input_partitions": inputs, "output_partitions": outputs,
                        "attempt": attempt,
                    },
                )
                self.catalog.append_job_event(job_id, attempt, {"type": "cancelled"})
                return
            if cursor >= len(task_indexes):
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
                    input_partitions=tuple(inputs), output_partitions=tuple(outputs),
                    warnings=tuple(filter(None, (*plan.warnings, kernel.fallback_reason))),
                ).to_dict()
                manifest["diagnostics"] = diagnostics
                manifest["attempt"] = attempt
                self.engine.lake.catalog.save_run(manifest)
                self.engine.lake.write_run_files(job_id, manifest)
                self._owned_update(
                    job_id, status=status, current_task="", failures_json=failures,
                    manifest_json=manifest, owner="", lease_expires=0,
                )
                self.catalog.append_job_event(job_id, attempt, {
                    "type": status, "failed": len(failures),
                })
                return
            task_index = task_indexes[cursor]
            if task_index < 0 or task_index >= len(plan.tasks):
                raise RuntimeError(f"任务索引越界：{task_index}")
            task = plan.tasks[task_index]
            self._owned_update(job_id, current_task=task.key)
            try:
                records = self.engine.execute_task(plan, task, kernel=kernel, run_id=job_id)
                if not lease_alive.is_set():
                    return
                (inputs if task.kind == "sync" else outputs).extend(records)
                self._owned_update(
                    job_id, next_index=cursor + 1, succeeded=int(job["succeeded"]) + 1,
                    current_task="", manifest_json={
                        "run_id": job_id, "plan_hash": plan.plan_hash, "status": "running",
                        "started_at": started_at, "input_partitions": inputs,
                        "output_partitions": outputs, "attempt": attempt,
                    },
                )
                self.catalog.append_job_event(job_id, attempt, {
                    "type": "task_completed", "task_index": task_index, "task": task.key,
                })
            except Exception as exc:
                failures.append({
                    "task_index": task_index, "task": task.to_dict(), "error": str(exc)[:500],
                })
                self._owned_update(
                    job_id, next_index=cursor + 1, failed=int(job["failed"]) + 1,
                    current_task="", failures_json=failures,
                )
                self.catalog.append_job_event(job_id, attempt, {
                    "type": "task_failed", "task_index": task_index,
                    "task": task.key, "error": str(exc)[:500],
                })

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
        result.pop("owner", None)
        result.pop("lease_expires", None)
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
            if value["status"] not in {"queued", "running", "cancelling"}:
                return value
            self._stop.wait(max(0.01, poll_seconds))

    def cancel(self, job_id: str) -> dict[str, Any]:
        job = self.get(job_id)
        if job["status"] == "queued":
            self.catalog.update_job(
                job_id, status="cancelled", cancel_requested=1, current_task="",
            )
        elif job["status"] in {"running", "cancelling"}:
            self.catalog.update_job(job_id, status="cancelling", cancel_requested=1)
        else:
            raise ValueError("当前任务不能取消")
        return self.get(job_id)

    def resume(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            if not self._accepting:
                raise RuntimeError("研究任务执行器正在停止，暂不能续跑")
            self.catalog.resume_job(job_id)
            self._start(job_id)
        return self.get(job_id)

    def shutdown(self, timeout: float = 10.0) -> None:
        with self._lock:
            self._accepting = False
            self._stop.set()
            threads = list(self._threads.values())
        per_thread = max(0.05, timeout / max(1, len(threads)))
        for thread in threads:
            thread.join(timeout=per_thread)
        self.catalog.interrupt_owned(self.identity.value)


_MANAGERS: dict[str, ResearchJobManager] = {}
_MANAGERS_LOCK = threading.Lock()


def get_research_job_manager() -> ResearchJobManager:
    """Keep one manager per hot-swappable data root without mutating at import time."""
    from quantmaster.config import get_config

    key = str((get_config().data_root / "research_lake").resolve())
    with _MANAGERS_LOCK:
        return _MANAGERS.setdefault(key, ResearchJobManager())


def shutdown_research_job_managers(timeout: float = 10.0) -> None:
    with _MANAGERS_LOCK:
        managers = list(_MANAGERS.values())
    per_manager = max(0.05, timeout / max(1, len(managers)))
    for manager in managers:
        manager.shutdown(per_manager)
