from __future__ import annotations

import threading
from typing import Any

from quantmaster.config import get_config
from quantmaster.rotation.etf_research import get_etf_research_service
from quantmaster.runtime.jobs import ACTIVE_STATUSES, JobContext, JobOutcome, UnifiedJobRuntime, UnifiedJobStore

TASK_TYPE = "rotation.etf.scan"


class EtfResearchJobs:
    def __init__(self, runtime: UnifiedJobRuntime | None = None):
        self.runtime = runtime or UnifiedJobRuntime(
            UnifiedJobStore(get_config().data_root / "jobs.sqlite"), max_workers=1,
        )
        self._submit_lock = threading.Lock()
        self.runtime.register(TASK_TYPE, self._handle)

    @staticmethod
    def _handle(context: JobContext, spec: dict[str, Any]) -> JobOutcome:
        service = get_etf_research_service()
        try:
            snapshot = service.scan(
                as_of=str(spec.get("as_of") or ""), progress=context.progress,
                cancelled=context.cancelled,
            )
        except (InterruptedError, OSError, RuntimeError, TypeError, ValueError, AttributeError) as exc:
            service.store.record_failure(str(exc) or exc.__class__.__name__)
            raise
        artifact = context.write_artifact(
            "rotation.etf.snapshot", snapshot.to_dict(), {
                "schema_version": snapshot.schema_version,
                "lineage": {
                    "snapshot_id": snapshot.snapshot_id, "ingest_id": snapshot.ingest_id,
                    "artifact_id": snapshot.artifact_id, "input_hash": snapshot.input_hash,
                },
            },
        )
        return JobOutcome("completed", "ETF 研究快照已发布", artifact["id"])

    def submit(self, *, as_of: str = "") -> tuple[dict[str, Any], bool]:
        spec = {"as_of": as_of}
        with self._submit_lock:
            for existing in self.runtime.store.list(1000, job_type=TASK_TYPE):
                if existing.get("status") in ACTIVE_STATUSES and existing.get("spec") == spec:
                    return existing, False
            return self.runtime.submit(
                TASK_TYPE, spec, idempotency_key="", deadline_seconds=3600, max_attempts=2,
            )

    def get(self, job_id: str) -> dict[str, Any]:
        value = self.runtime.store.get(job_id)
        if str(value.get("type") or "") != TASK_TYPE:
            raise KeyError(job_id)
        return value

    def public(self, value: dict[str, Any]) -> dict[str, Any]:
        return self.runtime.public(value)

    def start(self) -> None:
        self.runtime.start()

    def pause(self) -> None:
        self.runtime.pause()

    def resume(self) -> None:
        self.runtime.resume()

    def stop(self) -> None:
        self.runtime.stop()

    @property
    def idle(self) -> bool:
        return self.runtime.idle


_lock = threading.Lock()
_instance: EtfResearchJobs | None = None


def get_etf_research_jobs() -> EtfResearchJobs:
    global _instance
    with _lock:
        if _instance is None:
            _instance = EtfResearchJobs()
        return _instance


def shutdown_etf_research_jobs() -> None:
    global _instance
    with _lock:
        value, _instance = _instance, None
    if value is not None:
        value.stop()
