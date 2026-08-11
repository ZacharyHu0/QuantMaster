from __future__ import annotations

import threading
from typing import Any

from quantmaster.after_close.models import SCORE_VERSION
from quantmaster.after_close.service import get_after_close_service
from quantmaster.after_close.store import AfterCloseStore
from quantmaster.automation.runtime import get_runtime
from quantmaster.config import get_config
from quantmaster.runtime.derived import DerivedArtifactCatalog
from quantmaster.runtime.jobs import (
    JobContext,
    JobOutcome,
    UnifiedJobRuntime,
    UnifiedJobStore,
)
from quantmaster.trading_sessions import market_date

TASK_TYPE = "after_close.scan"


def run_after_close_job(context: JobContext, spec: dict[str, Any]) -> JobOutcome:
    """Pure, spawn-safe handler for the heavy after-close calculation.

    The callable intentionally reconstructs its service in the compute child.
    The parent ``UnifiedJobRuntime`` keeps the lease and commits the terminal
    state, while this function can only use the fenced context for progress and
    immutable artifacts.
    """

    try:
        snapshot = get_after_close_service().scan(
            as_of=str(spec.get("as_of") or ""),
            force=bool(spec.get("force")),
            progress=context.progress,
            cancelled=context.cancelled,
        )
    except InterruptedError:
        raise
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        AfterCloseJobs._notify_failure(context.job_id, exc)
        raise
    artifact = context.write_artifact(
        "after_close.snapshot",
        snapshot.to_dict(),
        {
            "schema_version": snapshot.schema_version,
            "lineage": {
                "snapshot_id": snapshot.snapshot_id,
                "input_hash": snapshot.input_hash,
                "score_version": snapshot.score_version,
            },
        },
    )
    AfterCloseJobs._notify_success(context.job_id, snapshot)
    return JobOutcome("completed", "盘后研究快照已发布", artifact["id"])


class AfterCloseJobs:
    def __init__(self, runtime: UnifiedJobRuntime | None = None):
        self.runtime = runtime or UnifiedJobRuntime(
            UnifiedJobStore(get_config().data_root / "jobs.sqlite"), max_workers=1,
        )
        self.runtime.register(
            TASK_TYPE,
            self._handle,
            process_entrypoint="quantmaster.after_close.jobs:run_after_close_job",
        )

    def _handle(self, context: JobContext, spec: dict[str, Any]) -> JobOutcome:
        return run_after_close_job(context, spec)

    @staticmethod
    def input_fingerprint(*, as_of: str, force: bool) -> tuple[str, str]:
        """Use catalog generations before admitting a costly after-close scan."""

        if force:
            # A deliberate force/rebuild request is maintenance work; it must
            # not be silently satisfied from a previous normal artifact.
            try:
                return "", AfterCloseStore().active_score_version()
            except (OSError, RuntimeError, TypeError, ValueError):
                return "", SCORE_VERSION
        try:
            catalog = DerivedArtifactCatalog()
            generations = [
                *catalog.source_generations("stockdb.ingest.stock"),
                *catalog.source_generations("instrument_catalog"),
                *catalog.source_generations("index_membership.csi800"),
            ]
            if not generations:
                return "", AfterCloseStore().active_score_version()
            cfg = get_config().data
            score_version = AfterCloseStore().active_score_version()
            target = str(as_of or market_date().isoformat())
            return (
                catalog.input_fingerprint(
                    schema_version=2,
                    algorithm_version=score_version,
                    parameters={
                        "task": TASK_TYPE,
                        "as_of": target,
                        "include_bj": bool(cfg.after_close_include_bj),
                        "min_listing_sessions": int(cfg.after_close_min_listing_sessions),
                        "min_avg_amount": float(cfg.after_close_min_avg_amount),
                        "candidate_limit": int(cfg.after_close_candidate_limit),
                    },
                    source_generations=generations,
                ),
                score_version,
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            return "", SCORE_VERSION

    @staticmethod
    def _notify_success(job_id: str, snapshot) -> None:
        cfg = get_config()
        if not cfg.data.after_close_notify or not cfg.automation.enabled:
            return
        from quantmaster.automation.models import AlertEvent, stable_hash

        sectors = [item for item in snapshot.sectors if item.level == "L1"][:3]
        get_runtime().service.process_event(AlertEvent(
            kind="after_close_snapshot", score=0, severity="info",
            data_as_of=snapshot.as_of_date,
            symbols=[item.symbol for item in snapshot.candidates[:10]],
            evidence=[
                f"正式快照 {snapshot.snapshot_id[:12]}",
                "领先板块：" + "、".join(item.name for item in sectors),
                f"研究候选 {len(snapshot.candidates)} 只；不构成买卖建议",
            ],
            dedupe_key=stable_hash({"after_close": snapshot.snapshot_id}),
            payload={
                "title": f"盘后研究已更新 · {snapshot.as_of_date}",
                "snapshot_id": snapshot.snapshot_id, "job_id": job_id,
            },
        ))

    @staticmethod
    def _notify_failure(job_id: str, exc: Exception) -> None:
        cfg = get_config()
        if not cfg.data.after_close_notify or not cfg.automation.enabled:
            return
        from datetime import UTC, datetime

        from quantmaster.automation.models import AlertEvent, stable_hash
        from quantmaster.automation.runtime import get_runtime

        message = str(exc).strip()[:500] or "数据完整性门未通过"
        get_runtime().service.process_event(AlertEvent(
            kind="task_failure", score=100, severity="warning",
            data_as_of=datetime.now(UTC).isoformat(), evidence=[
                "未发布伪新结果，继续沿用上一正式盘后快照", message,
                f"运行编号 {job_id[:10]}",
            ],
            dedupe_key=stable_hash({
                "after_close_failure": True,
                "day": market_date().strftime("%Y%m%d"), "message": message,
            }),
            payload={"title": "盘后研究未更新", "job_id": job_id},
        ))

    def submit(self, *, as_of: str = "", force: bool = False) -> tuple[dict, bool]:
        spec = {"as_of": as_of, "force": bool(force)}
        fingerprint, score_version = self.input_fingerprint(as_of=as_of, force=force)
        return self.runtime.submit(
            TASK_TYPE,
            spec,
            input_fingerprint=fingerprint,
            algorithm_version=score_version,
            deadline_seconds=3600,
            max_attempts=2,
        )

    def list(self, limit: int = 50) -> list[dict]:
        return self.runtime.store.list(limit, job_type=TASK_TYPE)

    def get(self, job_id: str) -> dict:
        value = self.runtime.store.get(job_id)
        if str(value.get("type") or "") != TASK_TYPE:
            raise KeyError(job_id)
        return value

    def public(self, value: dict) -> dict:
        public = self.runtime.public(value)
        spec = value.get("spec") if isinstance(value.get("spec"), dict) else {}
        target = str(spec.get("as_of") or "")
        completed = ""
        if str(value.get("status") or "") == "completed":
            artifact = self.runtime.store.latest_artifact(
                str(value.get("id") or ""), "after_close.snapshot",
            )
            payload = (artifact or {}).get("payload") if artifact else {}
            if isinstance(payload, dict):
                completed = str(payload.get("as_of_date") or "")
        public.update({
            "target_as_of": target,
            "completed_as_of": completed,
            "failure_reason": str(value.get("detail") or "")[:1000]
            if str(value.get("status") or "") in {"failed", "interrupted"} else "",
        })
        return public

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
_instance: AfterCloseJobs | None = None


def get_after_close_jobs() -> AfterCloseJobs:
    global _instance
    with _lock:
        if _instance is None:
            _instance = AfterCloseJobs()
        return _instance


def shutdown_after_close_jobs() -> None:
    global _instance
    with _lock:
        value, _instance = _instance, None
    if value is not None:
        value.stop()
