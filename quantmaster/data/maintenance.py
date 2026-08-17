"""Incremental market-data refreshes backed by the unified job lifecycle."""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal

from quantmaster.config import get_config
from quantmaster.data.registry import RefreshMode, refresh_history
from quantmaster.data.storage import BarStore
from quantmaster.runtime.jobs import (
    JobContext,
    JobOutcome,
    UnifiedJobRuntime,
    UnifiedJobStore,
)
from quantmaster.trading_sessions import market_date

RefreshScope = Literal["market", "universe", "all_cached"]
DATA_REFRESH_TASK_TYPE = "data.refresh"
REFRESH_RESULT_KIND = "data.refresh.result"
REFRESH_CHECKPOINT = "data.refresh.progress"
logger = logging.getLogger(__name__)


def market_symbols() -> list[str]:
    from quantmaster.data.akshare_source import A_SHARE_INDEXES, FUTURES_MAIN
    from quantmaster.data.reference_catalog import GLOBAL_REFS

    return list(dict.fromkeys([
        *A_SHARE_INDEXES,
        *(symbol for symbol in GLOBAL_REFS if not symbol.endswith((".CONTINUOUS", ".FX"))),
        *(symbol for symbol in FUTURES_MAIN if not symbol.startswith("IF")),
        *(symbol for symbol in GLOBAL_REFS if symbol.endswith((".CONTINUOUS", ".FX"))),
    ]))


class DataRefreshManager:
    """Plan refresh work and project its domain result from one runtime ledger."""

    MAX_PARALLEL_SYMBOLS = 8

    def __init__(self, runtime: UnifiedJobRuntime | None = None) -> None:
        self._lock = threading.RLock()
        self._runtime = runtime
        self._fixed_runtime = runtime is not None

    @staticmethod
    def _owns_runtime() -> bool:
        return os.environ.get("QM_WEB_PROCESS") != "1"

    @staticmethod
    def _path() -> Path:
        return get_config().data_root / "jobs.sqlite"

    def _ensure_runtime(self) -> UnifiedJobRuntime:
        path = self._path()
        with self._lock:
            if self._runtime is not None:
                same_root = self._runtime.store.path.resolve() == path.resolve()
                if self._fixed_runtime or same_root:
                    return self._runtime
                if not self._runtime.idle:
                    raise RuntimeError("行情刷新仍在旧数据目录运行，拒绝切换任务账本")
                self._runtime.stop()
            self._runtime = UnifiedJobRuntime(
                UnifiedJobStore(path), max_workers=self.MAX_PARALLEL_SYMBOLS,
                dispatch=self._owns_runtime(),
            )
            self._runtime.register(DATA_REFRESH_TASK_TYPE, self._handle)
            return self._runtime

    def _read_store(self) -> UnifiedJobStore:
        if self._runtime is not None:
            if self._fixed_runtime or self._runtime.store.path.resolve() == self._path().resolve():
                return self._runtime.store
        return UnifiedJobStore(self._path(), read_only=True)

    def initialize(self) -> None:
        """Publish the shared schema only from the runtime-worker path."""

        self._ensure_runtime()

    @staticmethod
    def _resolve_symbols(scope: RefreshScope, universe: str, start: str, end: str) -> list[str]:
        if scope == "market":
            return market_symbols()
        if scope == "all_cached":
            return BarStore().symbols()
        if not universe:
            raise ValueError("指定候选刷新需要选择候选")
        if universe.lower() == "csi800":
            from quantmaster.data.schema_access import schema_target

            membership = schema_target("membership_loader")(start, end)
            return sorted(symbol for symbol in membership if membership[symbol].any())
        from quantmaster.data.universe import load_universe

        return load_universe(universe)

    def _plan(
        self, scope: RefreshScope, universe: str = "", start: str = "",
    ) -> tuple[dict[str, Any], list[str]]:
        end = market_date().isoformat()
        if scope == "market":
            start = str(market_date() - timedelta(days=365))
        elif not start:
            start = get_config().lab.start
        try:
            start_date = date.fromisoformat(start)
        except ValueError:
            raise ValueError("刷新起始日期必须是 YYYY-MM-DD") from None
        if start_date > market_date():
            raise ValueError("刷新起始日期不能晚于今天")
        symbols = self._resolve_symbols(scope, universe, start, end)
        from quantmaster.data.resilience import PROVIDER_HEALTH

        health = PROVIDER_HEALTH.status()
        unhealthy = [
            lane for lane, item in health.items()
            if item.get("state") != "closed"
            and float(item.get("open_until") or 0) > time.time()
        ]
        return {
            "scope": scope,
            "universe": universe,
            "start": start,
            "end": end,
            "total": len(symbols),
            "unhealthy_sources": unhealthy,
            "message": (
                f"将增量同步 {len(symbols)} 个日线标的；"
                "已缓存标的只请求尾部重叠区间，未缓存标的才按起始日期初始化"
            ),
        }, symbols

    def preview(
        self, scope: RefreshScope, universe: str = "", start: str = "",
    ) -> dict[str, Any]:
        preview, _symbols = self._plan(scope, universe, start)
        return preview

    def create(
        self, scope: RefreshScope, universe: str = "", start: str = "",
    ) -> dict[str, Any]:
        preview, symbols = self._plan(scope, universe, start)
        runtime = self._ensure_runtime()
        job, created = runtime.store.submit(
            DATA_REFRESH_TASK_TYPE,
            {
                "scope": scope,
                "universe": universe,
                "start": preview["start"],
                "end": preview["end"],
                "symbols": symbols,
            },
            deadline_seconds=3600,
            max_attempts=8,
        )
        if created and self._owns_runtime():
            self._start(str(job["id"]))
        value = self.get(str(job["id"]))
        value.update(created=created, coalesced=not created)
        return value

    def _start(self, _job_id: str) -> None:
        self._ensure_runtime().start()

    def _run(self, job_id: str) -> None:
        """Execute one queued fixture synchronously through the kernel."""

        runtime = self._ensure_runtime()
        runtime.dispatch_job(job_id)
        runtime.wait(job_id, timeout=30.0)

    @staticmethod
    def _publish_market_snapshot() -> None:
        try:
            from quantmaster.data.schema_access import schema_target

            schema_target("market_overview_publisher")()
        except (OSError, RuntimeError, ValueError, TypeError):
            logger.warning("数据刷新后发布市场快照失败", exc_info=True)

    @staticmethod
    def _initial_state(context: JobContext, spec: dict[str, Any]) -> dict[str, Any]:
        previous = context.store.latest_artifact(context.job_id, REFRESH_RESULT_KIND)
        if context.attempt > 1 and previous:
            payload = dict(previous["payload"])
            retry_symbols = [str(item["symbol"]) for item in payload.get("failures") or ()]
            if retry_symbols:
                return {
                    "schema_version": "1.0",
                    "original_symbols": list(payload.get("original_symbols") or spec["symbols"]),
                    "symbols": retry_symbols,
                    "next_index": 0,
                    "succeeded": 0,
                    "failures": [],
                    "current_symbol": "",
                }
        checkpoint = context.load_checkpoint(REFRESH_CHECKPOINT, context.spec_hash)
        if checkpoint:
            return dict(checkpoint)
        return {
            "schema_version": "1.0",
            "original_symbols": list(spec["symbols"]),
            "symbols": list(spec["symbols"]),
            "next_index": 0,
            "succeeded": 0,
            "failures": [],
            "current_symbol": "",
        }

    @staticmethod
    def _refresh_one(store: BarStore, symbol: str, start: str, end: str) -> str:
        try:
            envelope = refresh_history(
                symbol, start, end, store=store,
                mode=RefreshMode.INCREMENTAL, work_class="maintenance",
            )
            envelope.require_data()
            if envelope.quality.status != "verified":
                return "；".join(envelope.quality.issues) or "行情证据仍为降级状态"
        except Exception as exc:
            from quantmaster.logging_config import redact_sensitive_text

            return redact_sensitive_text(exc)[:300]
        return ""

    def _handle(self, context: JobContext, spec: dict[str, Any]) -> JobOutcome:
        state = self._initial_state(context, spec)
        store = BarStore()
        symbols = [str(symbol) for symbol in state["symbols"]]
        while int(state["next_index"]) < len(symbols):
            context.ensure_active()
            index = int(state["next_index"])
            batch = symbols[index:index + self.MAX_PARALLEL_SYMBOLS]
            plans: list[tuple[str, str]] = []
            for symbol in batch:
                coverage = store.coverage(symbol)
                start = coverage[0] if spec["scope"] == "all_cached" and coverage else spec["start"]
                plans.append((symbol, str(start)))
            state["current_symbol"] = f"正在并行同步 {len(plans)} 个标的"
            context.progress(
                round(100 * index / max(1, len(symbols))),
                "同步行情",
                str(state["current_symbol"]),
            )
            errors = [""] * len(plans)
            with ThreadPoolExecutor(
                max_workers=len(plans), thread_name_prefix=f"data-refresh-{context.job_id[-8:]}",
            ) as executor:
                futures = {
                    executor.submit(self._refresh_one, store, symbol, start, str(spec["end"])): offset
                    for offset, (symbol, start) in enumerate(plans)
                }
                for future in as_completed(futures):
                    errors[futures[future]] = future.result()
            context.ensure_active()
            for (symbol, _start), error in zip(plans, errors, strict=True):
                if error:
                    state["failures"].append({"symbol": symbol, "error": error})
                else:
                    state["succeeded"] = int(state["succeeded"]) + 1
            state["next_index"] = index + len(plans)
            state["current_symbol"] = ""
            context.write_checkpoint(REFRESH_CHECKPOINT, context.spec_hash, state)
            context.completed_unit(f"已同步 {state['next_index']}/{len(symbols)} 个标的")
        failures = list(state["failures"])
        outcome = "completed_with_warnings" if failures else "completed"
        result = {
            **state,
            "outcome": outcome,
            "total": len(symbols),
            "failed": len(failures),
        }
        artifact = context.write_artifact(
            REFRESH_RESULT_KIND,
            result,
            {"schema_version": "1.0", "lineage": {"spec_hash": context.spec_hash}},
        )
        context.emit("data_refresh_completed", {"outcome": outcome, "failed": len(failures)})
        self._publish_market_snapshot()
        return JobOutcome("completed", "行情刷新已完成", str(artifact["id"]))

    @staticmethod
    def _state(store: UnifiedJobStore, job: dict[str, Any]) -> dict[str, Any]:
        artifact = store.latest_artifact(str(job["id"]), REFRESH_RESULT_KIND)
        if artifact:
            return dict(artifact["payload"])
        checkpoint = store.checkpoint(
            str(job["id"]), REFRESH_CHECKPOINT, str(job["spec_hash"]),
        )
        return dict(checkpoint or {})

    def _project(self, store: UnifiedJobStore, job: dict[str, Any]) -> dict[str, Any]:
        if str(job.get("type")) != DATA_REFRESH_TASK_TYPE:
            raise KeyError(str(job.get("id") or ""))
        spec = dict(job["spec"])
        state = self._state(store, job)
        symbols = list(state.get("symbols") or spec.get("symbols") or ())
        failures = list(state.get("failures") or ())
        value = UnifiedJobRuntime.public(job)
        value.update({
            "scope": spec.get("scope"),
            "universe_name": spec.get("universe") or "",
            "start_date": spec.get("start"),
            "end_date": spec.get("end"),
            "next_index": int(state.get("next_index") or 0),
            "total": int(state.get("total") or len(symbols)),
            "succeeded": int(state.get("succeeded") or 0),
            "failed": int(state.get("failed") or len(failures)),
            "failures": failures[-200:],
            "current_symbol": str(state.get("current_symbol") or ""),
            "outcome": str(state.get("outcome") or ""),
        })
        return value

    def get(self, job_id: str) -> dict[str, Any]:
        try:
            store = self._read_store()
            return self._project(store, store.get(job_id))
        except (FileNotFoundError, sqlite3.Error) as exc:
            raise KeyError(job_id) from exc

    def latest(self) -> dict[str, Any] | None:
        values = self.list(1)
        return values[0] if values else None

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        try:
            store = self._read_store()
            return [
                self._project(store, job)
                for job in store.list(limit, job_type=DATA_REFRESH_TASK_TYPE)
            ]
        except (FileNotFoundError, sqlite3.Error):
            return []

    @property
    def active(self) -> bool:
        return any(job["status"] in {"queued", "running", "cancelling", "interrupted"}
                   for job in self.list(200))

    def cancel(self, job_id: str) -> dict[str, Any]:
        runtime = self._ensure_runtime()
        self._project(runtime.store, runtime.store.get(job_id))
        return self._project(runtime.store, runtime.store.cancel(job_id))

    def resume(self, job_id: str) -> dict[str, Any]:
        runtime = self._ensure_runtime()
        source = self._project(runtime.store, runtime.store.get(job_id))
        retryable = source["status"] in {"failed", "cancelled", "interrupted"}
        retryable = retryable or source.get("outcome") == "completed_with_warnings"
        if not retryable:
            raise ValueError("当前任务不能续跑")
        return self._project(runtime.store, runtime.retry(job_id))

    def events(self, job_id: str, after: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        store = self._read_store()
        self._project(store, store.get(job_id))
        return store.events(job_id, after, limit)

    def start(self) -> None:
        if self._owns_runtime():
            self._ensure_runtime().start()

    def shutdown(self, timeout: float = 10.0) -> None:
        with self._lock:
            runtime = self._runtime
        if runtime is not None:
            runtime.stop(deadline_seconds=timeout)


data_refresh_manager = DataRefreshManager()
