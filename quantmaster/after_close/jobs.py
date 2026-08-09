from __future__ import annotations

import threading
from typing import Any

from quantmaster.after_close.service import get_after_close_service
from quantmaster.automation.runtime import get_runtime
from quantmaster.config import get_config
from quantmaster.runtime.jobs import (
    ACTIVE_STATUSES,
    JobContext,
    JobOutcome,
    UnifiedJobRuntime,
    UnifiedJobStore,
)
from quantmaster.trading_sessions import market_date

TASK_TYPE = "after_close.scan"


class AfterCloseJobs:
    def __init__(self, runtime: UnifiedJobRuntime | None = None):
        self.runtime = runtime or UnifiedJobRuntime(
            UnifiedJobStore(get_config().data_root / "jobs.sqlite"), max_workers=1,
        )
        self._submit_lock = threading.Lock()
        self.runtime.register(TASK_TYPE, self._handle)

    def _handle(self, context: JobContext, spec: dict[str, Any]) -> JobOutcome:
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
            self._notify_failure(context.job_id, exc)
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
        self._notify_success(context.job_id, snapshot)
        return JobOutcome("completed", "盘后研究快照已发布", artifact["id"])

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
        # 只合并仍在执行的同规格扫描。持久幂等键会永久绑定首个任务，导致
        # 当天一次失败后，页面后续点击始终取回旧失败任务，甚至跨交易日也
        # 无法创建新扫描。盘后快照自身已有输入哈希保证结果不可变，因此任务
        # 层只需要防止并发重复提交，终态任务必须允许重新运行。
        with self._submit_lock:
            for existing in self.runtime.store.list(1000, job_type=TASK_TYPE):
                if (
                    existing.get("status") in ACTIVE_STATUSES
                    and existing.get("spec") == spec
                ):
                    return existing, False
            return self.runtime.submit(
                TASK_TYPE, spec, idempotency_key="",
                deadline_seconds=3600, max_attempts=3,
            )

    def list(self, limit: int = 50) -> list[dict]:
        return self.runtime.store.list(limit, job_type=TASK_TYPE)

    def get(self, job_id: str) -> dict:
        value = self.runtime.store.get(job_id)
        if str(value.get("type") or "") != TASK_TYPE:
            raise KeyError(job_id)
        return value

    def public(self, value: dict) -> dict:
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
