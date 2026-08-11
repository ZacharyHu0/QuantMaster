"""Unified, process-isolated runtime jobs for remote news acquisition."""

from __future__ import annotations

import hashlib
import threading
import time
from typing import Any

from quantmaster.ai.crawler import AICrawler, NewsStore
from quantmaster.ai.news_sources import NewsSourceStore
from quantmaster.config import get_config
from quantmaster.runtime.jobs import JobContext, JobOutcome, UnifiedJobRuntime, UnifiedJobStore
from quantmaster.runtime.json import strict_json_dumps

TASK_TYPE = "news.crawl"
ALGORITHM_VERSION = "QM_NEWS_CRAWL_V3"


def _canonical_source_state(
    *, sources: list[str] | None, group: str | None,
) -> tuple[list[dict[str, Any]], int]:
    """Read the small local source/state tables without fetching a provider."""

    store = NewsSourceStore(read_only=True)
    selected = store.list(enabled=True, group_name=group)
    allowed = set(str(value) for value in sources or ())
    if allowed:
        selected = [item for item in selected if str(item.get("id") or "") in allowed]
    fields = (
        "id", "enabled", "group_name", "updated_at", "watermark", "pending_watermark",
        "latest_published_at", "last_success_at", "last_status", "health",
    )
    compact = [
        {field: item.get(field) for field in fields}
        for item in sorted(selected, key=lambda item: str(item.get("id") or ""))
    ]
    return compact, NewsStore(read_only=True).max_id()


def _input_fingerprint(spec: dict[str, Any]) -> str:
    """Coalesce duplicate clicks while retaining a bounded remote freshness probe.

    Remote news can change without a local generation.  Its source watermark
    and a five-minute polling window are therefore part of the canonical input
    boundary.  Within the window repeated clicks share one task; after it, a
    new conditional provider probe is permitted.  No page GET performs either
    operation.
    """

    try:
        sources = [str(value) for value in spec.get("sources") or ()]
        group = str(spec.get("group") or "") or None
        source_state, max_id = _canonical_source_state(sources=sources or None, group=group)
        payload = {
            "schema_version": 1,
            "algorithm_version": ALGORITHM_VERSION,
            "sources": sources,
            "group": group or "",
            "limit": int(spec.get("limit") or 30),
            "skip_llm": bool(spec.get("skip_llm")),
            "source_state": source_state,
            "news_max_id": max_id,
            # External evidence is intentionally rechecked on a bounded
            # cadence; this is a freshness policy, never a substitute for a
            # local content generation.
            "remote_probe_window": int(time.time() // 300),
        }
        return hashlib.sha256(
            strict_json_dumps(payload, sort_keys=True).encode("utf-8"),
        ).hexdigest()
    except (OSError, RuntimeError, TypeError, ValueError):
        # An unavailable local catalog must not create a false completed hit.
        return ""


def run_news_crawl_job(context: JobContext, spec: dict[str, Any]) -> JobOutcome:
    """Spawn-safe crawler handler; the parent runtime owns the lease."""

    context.progress(2, "检查本地资讯目录", "确认来源、水位和待标注队列")
    result = AICrawler().run(
        sources=[str(value) for value in spec.get("sources") or ()] or None,
        group=str(spec.get("group") or "") or None,
        limit=max(1, min(100, int(spec.get("limit") or 30))),
        skip_llm=bool(spec.get("skip_llm")),
    )
    context.ensure_active()
    context.progress(96, "发布资讯结果", "写入不可变任务产物")
    artifact = context.write_artifact(
        "news.crawl.result",
        result,
        {
            "schema_version": "1.0",
            "lineage": {
                "algorithm_version": ALGORITHM_VERSION,
                "input_fingerprint": context.input_fingerprint,
                "max_news_id": max((result.get("new_ids") or [0]), default=0),
            },
        },
    )
    errors = result.get("errors") or {}
    detail = (
        f"资讯采集完成：抓取 {int(result.get('fetched') or 0)} 条，新增 "
        f"{int(result.get('saved') or 0)} 条"
    )
    if errors:
        detail += f"；{len(errors)} 个来源降级"
    return JobOutcome("completed", detail, artifact["id"])


class NewsJobs:
    def __init__(self, runtime: UnifiedJobRuntime | None = None):
        self.runtime = runtime or UnifiedJobRuntime(
            UnifiedJobStore(get_config().data_root / "jobs.sqlite"), max_workers=1,
        )
        self.runtime.register(
            TASK_TYPE,
            run_news_crawl_job,
            process_entrypoint="quantmaster.ai.news_jobs:run_news_crawl_job",
        )

    def submit(
        self,
        *,
        sources: list[str] | None = None,
        group: str | None = None,
        limit: int = 30,
        skip_llm: bool = False,
    ) -> tuple[dict[str, Any], bool]:
        spec = {
            "sources": list(dict.fromkeys(str(value) for value in sources or ())),
            "group": str(group or ""),
            "limit": max(1, min(100, int(limit))),
            "skip_llm": bool(skip_llm),
        }
        return self.runtime.submit(
            TASK_TYPE,
            spec,
            input_fingerprint=_input_fingerprint(spec),
            algorithm_version=ALGORITHM_VERSION,
            deadline_seconds=3600,
            max_attempts=2,
        )

    def get(self, job_id: str) -> dict[str, Any]:
        value = self.runtime.store.get(job_id)
        if str(value.get("type") or "") != TASK_TYPE:
            raise KeyError(job_id)
        return value

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        return self.runtime.store.list(limit, job_type=TASK_TYPE)

    def public(self, value: dict[str, Any]) -> dict[str, Any]:
        public = self.runtime.public(value)
        artifact_id = str(value.get("result_artifact_id") or "")
        if artifact_id:
            try:
                artifact = self.runtime.store.artifact(artifact_id)
            except (KeyError, RuntimeError, ValueError):
                artifact = None
            if isinstance(artifact, dict) and isinstance(artifact.get("payload"), dict):
                public["result"] = dict(artifact["payload"])
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
_instance: NewsJobs | None = None


def get_news_jobs() -> NewsJobs:
    global _instance
    with _lock:
        if _instance is None:
            _instance = NewsJobs()
        return _instance


def shutdown_news_jobs() -> None:
    global _instance
    with _lock:
        value, _instance = _instance, None
    if value is not None:
        value.stop()
