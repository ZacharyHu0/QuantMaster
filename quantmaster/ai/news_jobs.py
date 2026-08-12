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
    batch_size = max(1, min(50, int(spec.get("batch_size") or 10)))
    mode = str(spec.get("mode") or "pending")
    if mode not in {"pending", "failed", "dead_letter"}:
        raise ValueError("未知资讯重分析模式")
    if mode == "pending" and ids is not None:
        reset = crawler.store.reset_analysis(ids)
    else:
        reset = 0
    selected_limit = None if mode in {"failed", "dead_letter"} and ids is None else limit
    result: dict[str, Any] = {
        "processed": 0, "completed": 0, "failed": 0,
        "retry_scheduled": 0, "dead_letter": 0,
        "failure_details": [], "completed_ids": [], "claimed": 0,
    }
    for event in crawler.enrich_pending_events(
        ids=ids,
        limit=selected_limit,
        batch_size=batch_size,
        mode=mode,  # type: ignore[arg-type]
        manual=mode in {"failed", "dead_letter"},
        cancelled=context.cancelled,
    ):
        context.ensure_active()
        event_type = str(event.get("type") or "")
        if event_type == "start":
            total = int(event.get("total") or 0)
            context.progress(
                4, "准备资讯重分析",
                f"已选取 {total} 条可处理资讯" if total else "没有可处理的资讯",
            )
        elif event_type == "batch":
            processed = int(event.get("processed") or 0)
            total = max(1, int(event.get("total") or 0))
            completed = int(event.get("completed") or 0)
            failed = int(event.get("failed") or 0)
            context.progress(
                min(95, 4 + (91 * processed // total)),
                "正在标注资讯",
                f"已处理 {processed}/{total} 条，成功 {completed} 条，失败 {failed} 条",
            )
        elif event_type == "complete":
            result = {key: value for key, value in event.items() if key != "type"}
    context.ensure_active()
    payload = {"status": "ok", "reset": reset, **result, "mode": mode}
    context.progress(96, "发布重分析结果", "写入不可变任务产物")
    artifact = context.write_artifact(
        "news.reanalyze.result",
        payload,
        {"schema_version": "1.0", "lineage": {"algorithm_version": ALGORITHM_VERSION}},
    )
    completed = int(result.get("completed") or 0)
    failed = int(result.get("failed") or 0)
    processed = int(result.get("processed") or 0)
    if processed == 0:
        detail = "资讯重分析完成：没有可处理的资讯"
    else:
        detail = f"资讯重分析完成：成功 {completed} 条，失败 {failed} 条"
    status = "completed_with_errors" if failed else "completed"
    return JobOutcome(status, detail, artifact["id"])


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
        news_config = get_config().news
        normalized = {
            "ids": [int(value) for value in spec.get("ids") or ()],
            "limit": max(1, min(1000, int(
                spec.get("limit") or news_config.annotation_items_per_run
            ))),
            "batch_size": max(1, min(50, int(
                spec.get("batch_size") or news_config.annotation_batch_size
            ))),
            "mode": str(spec.get("mode") or "pending"),
        }
        return self.runtime.submit(
            REANALYZE_TASK_TYPE,
            normalized,
            # A manual queue action may change only analysis_status while the
            # maximum news id remains stable.  Keep active singleflight by
            # spec_hash, but never reuse a terminal artifact for a new click.
            input_fingerprint="",
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
