"""Durable cloud-copilot suggestions, isolated from HTTP handlers."""

from __future__ import annotations

import threading
from typing import Any

from quantmaster.runtime.jobs import JobContext, JobOutcome, UnifiedJobRuntime


class LabLLMJobs:
    """One narrow queue for the Lab's optional cloud suggestion path."""

    def __init__(self) -> None:
        self.runtime = UnifiedJobRuntime(max_workers=1)
        self.runtime.register("lab.cloud_suggestion", self._handle)

    def submit(
        self,
        version_id: str,
        sample_consent: bool,
        sample: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], bool]:
        # Samples are explicit, user-consented Lab payloads.  The job spec
        # deliberately contains neither a provider credential nor a prompt.
        return self.runtime.submit(
            "lab.cloud_suggestion",
            {
                "version_id": str(version_id),
                "sample_consent": bool(sample_consent),
                "sample": dict(sample or {}),
            },
            deadline_seconds=300,
            max_attempts=1,
            llm_scope="global",
        )

    @staticmethod
    def _handle(context: JobContext, spec: dict[str, Any]) -> JobOutcome:
        from quantmaster.lab.service import get_lab_service

        context.progress(5, "生成云端建议", "已取得 LLM 执行租约")
        result = get_lab_service().suggest_revision(
            str(spec["version_id"]),
            use_cloud=True,
            sample_consent=bool(spec.get("sample_consent")),
            sample=dict(spec.get("sample") or {}),
        )
        context.ensure_active()
        artifact = context.write_artifact(
            "lab.cloud_suggestion",
            {"schema_version": "1.0", "result": result},
            {"schema_version": "1.0", "lineage": {"version_id": str(spec["version_id"])}},
        )
        return JobOutcome("completed", "云端建议已生成", artifact["id"])


_instance: LabLLMJobs | None = None
_lock = threading.Lock()


def get_lab_llm_jobs() -> LabLLMJobs:
    global _instance
    with _lock:
        if _instance is None:
            _instance = LabLLMJobs()
        return _instance


def shutdown_lab_llm_jobs() -> None:
    global _instance
    with _lock:
        value, _instance = _instance, None
    if value is not None:
        value.runtime.stop()
