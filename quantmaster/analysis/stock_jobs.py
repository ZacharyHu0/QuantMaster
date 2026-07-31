"""Thin market.stock_analysis registration for the unified runtime job kernel."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from quantmaster.analysis.stock import StockAnalysisService
from quantmaster.analysis.stock_research import (
    DEEP_DEADLINE_SECONDS,
    DIMENSION_LABELS,
    DIMENSION_ORDER,
    QUICK_DEADLINE_SECONDS,
    REPORT_SCHEMA_VERSION,
    StockAnalysisSpec,
)
from quantmaster.runtime.jobs import (
    JobContext,
    JobOutcome,
    UnifiedJobRuntime,
)

STOCK_ANALYSIS_TASK_TYPE = "market.stock_analysis"


class StockAnalysisJobs:
    def __init__(
        self,
        runtime: UnifiedJobRuntime | None = None,
        *,
        service_factory: Callable[[], StockAnalysisService] = StockAnalysisService,
    ):
        self.runtime = runtime or UnifiedJobRuntime()
        self.service_factory = service_factory
        self.runtime.register(STOCK_ANALYSIS_TASK_TYPE, self._handle)

    def start(self) -> None:
        self.runtime.start()

    def stop(self) -> None:
        self.runtime.stop()

    def pause(self) -> None:
        self.runtime.pause()

    def resume(self) -> None:
        self.runtime.resume()

    @property
    def idle(self) -> bool:
        return self.runtime.idle

    @staticmethod
    def _spec(query: str, mode: str) -> dict[str, Any]:
        value = StockAnalysisSpec(query, mode)
        return {
            "type": STOCK_ANALYSIS_TASK_TYPE,
            "query": value.query,
            "mode": value.mode,
            "schema_version": value.schema_version,
        }

    def submit(
        self,
        query: str,
        mode: str = "deep",
        *,
        idempotency_key: str = "",
    ) -> tuple[dict[str, Any], bool]:
        return self.runtime.submit(
            STOCK_ANALYSIS_TASK_TYPE,
            self._spec(query, mode),
            idempotency_key=idempotency_key,
            deadline_seconds=(
                DEEP_DEADLINE_SECONDS if mode == "deep" else QUICK_DEADLINE_SECONDS
            ),
        )

    @staticmethod
    def _event_progress(
        event_type: str,
        payload: dict[str, Any],
        current: int,
    ) -> tuple[int, str, str]:
        dimension = str(payload.get("dimension") or "")
        label = DIMENSION_LABELS.get(dimension, ("", dimension or "证据"))[1]
        if event_type == "analysis_started":
            return 3, "确认标的", "任务规格已锁定"
        if event_type == "evidence_collection_started":
            return 8, "联网取证", "六类证据并发采集"
        if event_type == "evidence_search_started":
            return max(current, 18), "联网搜索", f"第 {payload.get('round') or 1} 轮受限搜索"
        if event_type == "evidence_collection_completed":
            return 28, "证据归一化", f"已归一化 {payload.get('evidence_count') or 0} 条证据"
        if event_type == "dimension_started":
            stage = "规则计算" if payload.get("stage") == "rules" else "模型复核"
            return max(current, 30), f"{label}{stage}", "仅允许引用本维 evidence ID"
        if event_type == "dimension_audit_started":
            return max(current, 36), f"{label}反方审查", "主动寻找反例、遗漏和时点错配"
        if event_type in {"dimension_completed", "dimension_degraded"}:
            completed = max(1, min(len(DIMENSION_ORDER), int(payload.get("completed") or 1)))
            state = "降级交付" if event_type.endswith("degraded") else "完成"
            return 28 + completed * 10, f"{label}{state}", f"六维已交付 {completed}/{len(DIMENSION_ORDER)}"
        if event_type == "final_review_started":
            return 92, "交叉复核", "检查证据冲突、时点和空白"
        if event_type == "deep_final_review_started":
            return 95, "深度证伪终审", "核查未知项、催化剂和结论失效条件"
        if event_type == "final_review_completed":
            return 98, "终审完成", "正在保存可核查报告"
        if event_type == "analysis_completed":
            return 99, "保存报告", "完整报告已生成"
        return current, "分析进行中", str(payload.get("message") or "")[:300]

    def _handle(self, context: JobContext, persisted_spec: dict[str, Any]) -> JobOutcome:
        spec = StockAnalysisSpec(
            str(persisted_spec["query"]),
            str(persisted_spec.get("mode") or "deep"),
        )
        if spec.hash != context.spec_hash:
            raise ValueError("持久任务规格哈希与分析规格不一致")
        current_progress = 1

        def emit(event_type: str, payload: dict[str, Any]) -> None:
            nonlocal current_progress
            event_payload = dict(payload)
            if event_type == "analysis_completed" and "report" in event_payload:
                report = event_payload.pop("report") or {}
                event_payload["completion_status"] = (report.get("research") or {}).get(
                    "completion_status"
                ) or "completed"
            context.emit(event_type, event_payload)
            current_progress, phase, detail = self._event_progress(
                event_type,
                event_payload,
                current_progress,
            )
            context.progress(current_progress, phase, detail)

        service = self.service_factory()
        report = service.analyze_v2(
            spec.query,
            mode=spec.mode,
            emit=emit,
            artifact_writer=context.write_artifact,
            checkpoint_loader=context.load_checkpoint,
            checkpoint_writer=context.write_checkpoint,
            deadline_seconds=context.deadline_seconds,
            cancelled=context.cancelled,
        )
        artifact = context.store.latest_artifact(context.job_id, "stock_analysis.report")
        if artifact is None:
            raise RuntimeError("最终报告产物未提交")
        status = str((report.get("research") or {}).get("completion_status") or "completed")
        if status not in {"completed", "completed_with_errors"}:
            status = "completed_with_errors"
        return JobOutcome(status, "六维分析已完成", str(artifact["id"]))

    def _job(self, job_id: str) -> dict[str, Any]:
        job = self.runtime.store.get(job_id)
        if job["type"] != STOCK_ANALYSIS_TASK_TYPE:
            raise KeyError(job_id)
        return job

    def public_job(self, job_id: str) -> dict[str, Any]:
        return self.runtime.public(self._job(job_id))

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        return [
            self.runtime.public(value)
            for value in self.runtime.store.list(limit, job_type=STOCK_ANALYSIS_TASK_TYPE)
        ]

    def events(self, job_id: str, after: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        self._job(job_id)
        return self.runtime.store.events(job_id, after, limit)

    def cancel(self, job_id: str) -> dict[str, Any]:
        self._job(job_id)
        return self.runtime.public(self.runtime.store.cancel(job_id))

    def retry(self, job_id: str) -> dict[str, Any]:
        self._job(job_id)
        return self.runtime.public(self.runtime.retry(job_id))

    def analysis(self, analysis_id: str) -> dict[str, Any]:
        job = self._job(analysis_id)
        public = self.runtime.public(job)
        dimensions: list[dict[str, Any]] = []
        for key in DIMENSION_ORDER:
            artifact = self.runtime.store.latest_artifact(
                analysis_id,
                f"stock_analysis.dimension.{key}",
            )
            if artifact:
                dimensions.append(dict(artifact["payload"].get("dimension") or {}))
        report_artifact = self.runtime.store.latest_artifact(
            analysis_id,
            "stock_analysis.report",
        )
        report_error = ""
        if report_artifact is None and str(public["status"]).startswith("completed"):
            report_error = "最终报告产物未通过完整性校验，已请求修复；可以安全重试任务"
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "analysis_id": analysis_id,
            "job_id": analysis_id,
            "status": public["status"],
            "progress": public["progress"],
            "phase": public["phase"],
            "attempt": public["attempt"],
            "dimensions": dimensions,
            "report": report_artifact["payload"] if report_artifact else None,
            "error": report_error,
        }


_lock = threading.Lock()
_instance: StockAnalysisJobs | None = None


def get_stock_analysis_jobs() -> StockAnalysisJobs:
    global _instance
    with _lock:
        if _instance is None:
            _instance = StockAnalysisJobs()
        return _instance


def shutdown_stock_analysis_jobs() -> None:
    global _instance
    with _lock:
        value, _instance = _instance, None
    if value is not None:
        value.stop()
