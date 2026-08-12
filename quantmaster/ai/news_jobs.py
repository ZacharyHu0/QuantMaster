"""Unified, process-isolated runtime jobs for remote news acquisition."""

from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
from typing import Any

from quantmaster.ai.crawler import AICrawler, NewsStore
from quantmaster.ai.news_sources import NewsSourceStore
from quantmaster.config import get_config
from quantmaster.runtime.jobs import JobContext, JobOutcome, UnifiedJobRuntime, UnifiedJobStore
from quantmaster.runtime.json import strict_json_dumps

CRAWL_TASK_TYPE = "news.crawl"
SOURCE_RUN_TASK_TYPE = "news.source_run"
REANALYZE_TASK_TYPE = "news.reanalyze"
TASK_TYPE = CRAWL_TASK_TYPE  # stable import for existing callers
_TASK_TYPES = frozenset({CRAWL_TASK_TYPE, SOURCE_RUN_TASK_TYPE, REANALYZE_TASK_TYPE})
ALGORITHM_VERSION = "QM_NEWS_CRAWL_V4"


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
        cancelled=context.cancelled,
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


def run_news_source_job(context: JobContext, spec: dict[str, Any]) -> JobOutcome:
    """Source-specific crawl keeps its own durable task identity."""
    return run_news_crawl_job(context, spec)


def run_news_reanalyze_job(context: JobContext, spec: dict[str, Any]) -> JobOutcome:
    """Reanalyze in the worker, never via a request-held stream."""
    context.progress(2, "准备资讯重分析", "申领待标注资讯")
    context.ensure_active()
    crawler = AICrawler()
    ids = [int(value) for value in spec.get("ids") or ()] or None
    limit = max(1, min(1000, int(spec.get("limit") or 100)))
    batch_size = max(1, min(10, int(spec.get("batch_size") or 5)))
    mode = str(spec.get("mode") or "pending")
    if mode not in {"pending", "failed", "dead_letter"}:
        raise ValueError("未知资讯重分析模式")
    if mode == "pending" and ids is not None:
        reset = crawler.store.reset_analysis(ids)
    else:
        reset = 0
    selected_limit = None if mode in {"failed", "dead_letter"} and ids is None else limit
    result = crawler.enrich_pending(
        ids=ids,
        limit=selected_limit,
        batch_size=batch_size,
        mode=mode,  # type: ignore[arg-type]
        manual=mode in {"failed", "dead_letter"},
        cancelled=context.cancelled,
    )
    context.ensure_active()
    payload = {"status": "ok", "reset": reset, **result, "mode": mode}
    context.progress(96, "发布重分析结果", "写入不可变任务产物")
    artifact = context.write_artifact(
        "news.reanalyze.result",
        payload,
        {"schema_version": "1.0", "lineage": {"algorithm_version": ALGORITHM_VERSION}},
    )
    return JobOutcome("completed", f"资讯重分析完成：{int(result.get('completed') or 0)} 条", artifact["id"])


class NewsJobs:
    def __init__(self, runtime: UnifiedJobRuntime | None = None):
        self.runtime = runtime or UnifiedJobRuntime(
            UnifiedJobStore(get_config().data_root / "jobs.sqlite"), max_workers=1,
        )
        self.runtime.register(
            CRAWL_TASK_TYPE,
            run_news_crawl_job,
            process_entrypoint="quantmaster.ai.news_jobs:run_news_crawl_job",
        )
        self.runtime.register(
            SOURCE_RUN_TASK_TYPE,
            run_news_source_job,
            process_entrypoint="quantmaster.ai.news_jobs:run_news_source_job",
        )
        self.runtime.register(
            REANALYZE_TASK_TYPE,
            run_news_reanalyze_job,
            process_entrypoint="quantmaster.ai.news_jobs:run_news_reanalyze_job",
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
            CRAWL_TASK_TYPE,
            spec,
            input_fingerprint=_input_fingerprint(spec),
            algorithm_version=ALGORITHM_VERSION,
            deadline_seconds=3600,
            max_attempts=2,
            llm_scope="" if skip_llm else "news",
        )

    def submit_source_run(
        self, source_id: str, *, skip_llm: bool = False,
    ) -> tuple[dict[str, Any], bool]:
        spec = {
            "sources": [str(source_id)],
            "group": "",
            "limit": 30,
            "skip_llm": bool(skip_llm),
            "source_run": True,
        }
        return self.runtime.submit(
            SOURCE_RUN_TASK_TYPE,
            spec,
            input_fingerprint=_input_fingerprint(spec),
            algorithm_version=ALGORITHM_VERSION,
            deadline_seconds=3600,
            max_attempts=2,
            llm_scope="" if skip_llm else "news",
        )

    def submit_reanalyze(self, spec: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        normalized = {
            "ids": [int(value) for value in spec.get("ids") or ()],
            "limit": max(1, min(1000, int(spec.get("limit") or 100))),
            "batch_size": max(1, min(10, int(spec.get("batch_size") or 5))),
            "mode": str(spec.get("mode") or "pending"),
        }
        try:
            max_news_id = NewsStore(read_only=True).max_id()
        except (FileNotFoundError, OSError, RuntimeError, ValueError, sqlite3.Error):
            # A cold cache is still a valid asynchronous request.  The worker
            # owns any first-write initialization and will report an empty
            # reanalysis result rather than making the HTTP route fail.
            max_news_id = 0
        return self.runtime.submit(
            REANALYZE_TASK_TYPE,
            normalized,
            input_fingerprint=hashlib.sha256(
                strict_json_dumps({
                    "spec": normalized,
                    "max_news_id": max_news_id,
                }, sort_keys=True).encode("utf-8"),
            ).hexdigest(),
            algorithm_version="QM_NEWS_REANALYZE_V1",
            deadline_seconds=3600,
            max_attempts=2,
            llm_scope="news",
        )

    def get(self, job_id: str) -> dict[str, Any]:
        value = self.runtime.store.get(job_id)
        if str(value.get("type") or "") not in _TASK_TYPES:
            raise KeyError(job_id)
        return value

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        return [
            value for value in self.runtime.store.list(max(1, min(1000, int(limit) * 3)))
            if str(value.get("type") or "") in _TASK_TYPES
        ][:limit]

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
