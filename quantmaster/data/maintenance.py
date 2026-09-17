"""Incremental market-data refreshes backed by the unified job lifecycle."""

from __future__ import annotations

import builtins
import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from quantmaster.config import get_config
from quantmaster.data.base import MarketDataUnavailable
from quantmaster.data.registry import RefreshMode, refresh_history
from quantmaster.data.stockdb_state import stockdb_wait_reason
from quantmaster.data.storage import BarStore
from quantmaster.runtime.jobs import (
    JobContext,
    JobOutcome,
    UnifiedJobRuntime,
    UnifiedJobStore,
)
from quantmaster.trading_sessions import market_date, market_now

RefreshScope = Literal["market", "universe", "all_cached"]
DATA_REFRESH_TASK_TYPE = "data.refresh"
REFRESH_RESULT_KIND = "data.refresh.result"
REFRESH_CHECKPOINT = "data.refresh.progress"
REFRESH_SCHEMA = "3.0"
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

    @staticmethod
    def _stockdb_wait_reason() -> str:
        reason = stockdb_wait_reason()
        if reason:
            return reason
        from quantmaster.data.resilience import PROVIDER_HEALTH

        # A pre-upgrade outage may already have opened the circuit. Preserve
        # that evidence and let its existing cooldown expire before resuming.
        health = PROVIDER_HEALTH.status("free-stockdb").get("free-stockdb", {})
        if health.get("state") != "closed" and float(health.get("open_until") or 0) > time.time():
            return "StockDB 仍在已有故障冷却期，等待冷却结束后自动继续；未重置健康记录"
        return ""

    @staticmethod
    def _uses_stockdb(symbol: str) -> bool:
        from quantmaster.data.registry import _factories
        from quantmaster.market_capabilities import guess_market

        try:
            market = guess_market(symbol)
        except ValueError:
            return False  # Invalid identities still reach the ordinary validation path.
        ordered = _factories().get(market, [])
        return bool(ordered and ordered[0].name == "free-stockdb")

    @staticmethod
    def _waiting(reason: str) -> JobOutcome:
        return JobOutcome("interrupted", reason, retry_delay_seconds=60, waiting_on="stockdb_update")

    def __init__(self, runtime: UnifiedJobRuntime | None = None) -> None:
        self._lock = threading.RLock()
        self._runtime = runtime
        self._fixed_runtime = runtime is not None
        self._stop = threading.Event()
        self._scheduler: threading.Thread | None = None

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
                UnifiedJobStore(path), max_workers=1,
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

            membership = schema_target("membership_loader", start, end)
            return sorted(symbol for symbol in membership if membership[symbol].any())
        from quantmaster.data.universe import load_universe

        return load_universe(universe)

    def preview(
        self, scope: RefreshScope, universe: str = "", start: str = "",
    ) -> dict[str, Any]:
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
        if scope == "universe" and not universe:
            raise ValueError("指定候选刷新需要选择候选")
        # Preview is a local estimate, never a supplier discovery request.
        symbols: list[str] | None = None
        if scope == "market":
            symbols = market_symbols()
        elif scope == "universe" and universe.lower() != "csi800":
            from quantmaster.data.universe import load_universe

            symbols = load_universe(universe)
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
            "total": len(symbols) if symbols is not None else None,
            "planning_required": symbols is None,
            "unhealthy_sources": unhealthy,
            "message": (
                (f"将增量同步约 {len(symbols)} 个日线标的；" if symbols is not None
                 else "提交后在后台核验成分证据并确定标的数量；")
                + "优先复用本地缓存，只补齐缺失或过期区间"
            ),
        }

    def create(
        self, scope: RefreshScope, universe: str = "", start: str = "",
    ) -> dict[str, Any]:
        preview = self.preview(scope, universe, start)
        with self._lock:
            return self._submit(scope, universe, str(preview["start"]), str(preview["end"]))

    @staticmethod
    def _fingerprint(symbols: list[str]) -> str:
        from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

        metadata = BarStore(read_only=True).metadata_many(symbols) if symbols else {}
        cfg = get_config().data
        # Bound reuse by a short freshness epoch as well as actual local inputs.
        # A worker restart must not turn an old success into permanent freshness.
        inputs = {
            "epoch": int(time.time() // min(3600, max(60, cfg.cache_days * 86400))),
            "day": str(market_date()),
            "after_close": (market_now().hour, market_now().minute) >= (15, 30),
            "stockdb": free_stockdb_runtime._data_fingerprint(get_config().free_stockdb_root),
            "bars": metadata,
            "source_config": asdict(cfg),
            "membership": DataRefreshManager._membership_fingerprint(),
        }
        return hashlib.sha256(json.dumps(inputs, sort_keys=True, default=str).encode()).hexdigest()

    @staticmethod
    def _membership_fingerprint() -> list[tuple[str, int, int]]:
        root = get_config().data_root
        paths = [
            *root.joinpath("universe").glob("*.json"),
            *root.joinpath("research_lake", "raw", "stock", "1d", "csi800_membership").glob("**/*.parquet"),
        ]
        values = []
        for path in sorted(paths):
            try:
                info = path.stat()
            except FileNotFoundError:
                continue
            values.append((str(path.relative_to(root)), info.st_size, info.st_mtime_ns))
        return values

    def _submit(
        self, scope: str, universe: str, start: str, end: str, symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        runtime = self._ensure_runtime()
        spec: dict[str, Any] = {
            "scope": scope, "universe": universe, "start": start, "end": end,
            "refresh_schema": REFRESH_SCHEMA,
        }
        if symbols is not None:
            spec["symbols"] = sorted(set(symbols))
        if scope == "universe" and universe.lower() != "csi800":
            from quantmaster.data.universe import load_universe_snapshot

            snapshot = load_universe_snapshot(universe)
            spec["universe_revision"] = snapshot.content_hash
            spec["symbols"] = sorted(snapshot.symbols)
        existing = self._reusable(runtime.store, spec)
        if existing:
            value = self.get(str(existing["id"]))
            value.update(created=False, coalesced=True, reused=existing["status"] == "completed")
            return value
        job, created = runtime.store.submit(
            DATA_REFRESH_TASK_TYPE,
            spec,
            algorithm_version=REFRESH_SCHEMA,
            deadline_seconds=3600,
            max_attempts=8,
        )
        if created and self._owns_runtime():
            self._start(str(job["id"]))
        value = self.get(str(job["id"]))
        value.update(created=created, coalesced=not created)
        return value

    def _reusable(
        self, store: UnifiedJobStore, spec: dict[str, Any],
    ) -> dict[str, Any] | None:
        for job in store.list(200, job_type=DATA_REFRESH_TASK_TYPE):
            if job["spec"] != spec:
                continue
            if job["status"] in {"queued", "running", "cancelling", "interrupted"}:
                return job
            if job["status"] == "completed":
                artifact = store.latest_artifact(job["id"], REFRESH_RESULT_KIND)
                symbols = list(artifact["payload"].get("original_symbols") or ()) if artifact else []
                if artifact and artifact["payload"].get("local_fingerprint") == self._fingerprint(symbols):
                    return job
        return None

    def _start(self, _job_id: str) -> None:
        with self._lock:
            runtime = self._ensure_runtime()
            if not runtime.stopping:
                runtime.start()

    def _run(self, job_id: str) -> None:
        """Execute one queued fixture synchronously through the kernel."""

        runtime = self._ensure_runtime()
        runtime.dispatch_job(job_id)
        runtime.wait(job_id, timeout=30.0)

    @staticmethod
    def _publish_market_snapshot() -> None:
        try:
            from quantmaster.data.schema_access import schema_target

            schema_target("market_overview_publisher")
        except (OSError, RuntimeError, ValueError, TypeError):
            logger.warning("数据刷新后发布市场快照失败", exc_info=True)

    def _initial_state(self, context: JobContext, spec: dict[str, Any]) -> dict[str, Any]:
        if spec.get("refresh_schema") != REFRESH_SCHEMA:
            raise ValueError("REFRESH_SCHEMA_UNSUPPORTED: 请新建刷新任务，旧任务证据不支持续跑")
        checkpoint = context.load_checkpoint(REFRESH_CHECKPOINT, context.spec_hash)
        if checkpoint and checkpoint.get("schema_version") != REFRESH_SCHEMA:
            raise ValueError("REFRESH_SCHEMA_UNSUPPORTED: 检查点版本不支持续跑")
        previous = context.store.latest_artifact(context.job_id, REFRESH_RESULT_KIND)
        previous_attempt = int(previous["payload"].get("attempt", 0)) if previous else 0
        if checkpoint and int(checkpoint.get("attempt", 0)) > previous_attempt:
            return dict(checkpoint)
        if context.attempt > 1 and previous:
            payload = dict(previous["payload"])
            retry_symbols = [str(item["symbol"]) for item in payload.get("failures") or ()
                             if item.get("retryable")]
            if retry_symbols:
                return {
                    "schema_version": REFRESH_SCHEMA,
                    "attempt": context.attempt,
                    "original_symbols": list(payload["original_symbols"]),
                    "symbols": retry_symbols,
                    "next_index": 0,
                    "succeeded": 0,
                    "failures": [],
                    "current_symbol": "",
                    "completed_symbols": [],
                    "blocked_failures": [item for item in payload.get("failures") or ()
                                         if not item.get("retryable")],
                }
        if checkpoint:
            return dict(checkpoint)
        context.progress(0, "规划刷新", "正在核验候选成分证据；可取消")
        symbols = spec.get("symbols")
        if symbols is None:
            symbols = self._resolve_symbols(spec["scope"], spec["universe"], spec["start"], spec["end"])
        context.ensure_active()
        if not symbols:
            raise ValueError("REFRESH_MEMBERSHIP_EVIDENCE_MISSING: 刷新范围没有有效标的证据")
        return {
            "schema_version": REFRESH_SCHEMA,
            "attempt": context.attempt,
            "original_symbols": sorted(set(symbols)),
            "symbols": sorted(set(symbols)),
            "next_index": 0,
            "succeeded": 0,
            "failures": [],
            "current_symbol": "",
            "completed_symbols": [],
            "blocked_failures": [],
        }

    @staticmethod
    def _refresh_one(store: BarStore, symbol: str, start: str, end: str) -> dict[str, Any] | None:
        try:
            envelope = refresh_history(
                symbol, start, end, store=store,
                mode=RefreshMode.AUTO, work_class="maintenance",
            )
            envelope.require_data()
            if envelope.quality.status != "verified":
                return DataRefreshManager._failure(MarketDataUnavailable(envelope.quality))
        except Exception as exc:
            return DataRefreshManager._failure(exc)
        return None

    def _refresh_guarded(self, store: BarStore, symbol: str, start: str, end: str) -> dict[str, Any] | None:
        if self._uses_stockdb(symbol) and self._stockdb_wait_reason():
            return {"waiting_on": "stockdb_update"}
        error = self._refresh_one(store, symbol, start, end)
        # A bounded in-flight request may meet the service stopping. Retry it
        # after recovery instead of recording planned downtime as symbol failure.
        if error and self._uses_stockdb(symbol) and self._stockdb_wait_reason():
            return {"waiting_on": "stockdb_update"}
        return error

    @staticmethod
    def _failure(exc: Exception) -> dict[str, Any]:
        from quantmaster.data.resilience import classify_provider_failure
        from quantmaster.logging_config import redact_sensitive_text

        code = classify_provider_failure(exc)
        if isinstance(exc, MarketDataUnavailable) and code == "transient_upstream":
            code = "data_incomplete" if exc.quality.stale else "evidence_missing"
        retryable = code in {
            "transient_network", "transient_upstream", "rate_limit", "empty_response", "data_incomplete",
        }
        if isinstance(exc, MarketDataUnavailable) and any(marker in str(exc) for marker in (
            "单位", "unit_", "factor_contract", "continuous_contract", "MARKET_SESSION_UNSUPPORTED",
            "证券主数据", "semantics_", "来源契约",
        )):
            code, retryable = "evidence_missing", False
        retryable = retryable or code.endswith("_upstream") or code == "upstream_5xx"
        return {"error": redact_sensitive_text(exc)[:300], "code": code, "retryable": retryable}

    def _execute(self, context: JobContext, spec: dict[str, Any], state: dict[str, Any]) -> str:
        state["attempt"] = context.attempt
        store = BarStore()
        completed = set(state["completed_symbols"])
        remaining = iter(symbol for symbol in state["symbols"] if symbol not in completed)
        waiting = ""
        with ThreadPoolExecutor(max_workers=self.MAX_PARALLEL_SYMBOLS,
                                thread_name_prefix="data-refresh") as executor:
            pending: dict[Future[dict[str, Any] | None], str] = {}
            exhausted = False
            while pending or not exhausted:
                context.ensure_active()
                while not exhausted and len(pending) < self.MAX_PARALLEL_SYMBOLS:
                    context.ensure_active()
                    symbol = next(remaining, None)
                    if symbol is None:
                        exhausted = True
                        break
                    reason = self._stockdb_wait_reason() if self._uses_stockdb(symbol) else ""
                    if reason:
                        waiting = reason
                        continue
                    coverage = store.coverage(symbol)
                    start = coverage[0] if spec["scope"] == "all_cached" and coverage else spec["start"]
                    future = executor.submit(self._refresh_guarded, store, symbol, start, spec["end"])
                    pending[future] = symbol
                done, _ = wait(pending, timeout=0.2, return_when=FIRST_COMPLETED)
                for future in done:
                    symbol = pending.pop(future)
                    error = future.result()
                    if error and error.get("waiting_on"):
                        waiting = self._stockdb_wait_reason() or "StockDB 已切换更新阶段，等待重新规划"
                        continue
                    if error:
                        state["failures"].append({"symbol": symbol, **error})
                    else:
                        state["succeeded"] += 1
                    state["completed_symbols"].append(symbol)
                    state["next_index"] = len(state["completed_symbols"])
                    state["current_symbol"] = ", ".join(pending.values())
                    context.write_checkpoint(REFRESH_CHECKPOINT, context.spec_hash, state)
                    context.progress(round(100 * state["next_index"] / max(1, len(state["symbols"]))),
                                     "同步行情", f"已完成 {state['next_index']}/{len(state['symbols'])}")
                    context.completed_unit(symbol)
        if not waiting and any(self._uses_stockdb(symbol) for symbol in state["symbols"]):
            waiting = self._stockdb_wait_reason()
        return waiting

    def _handle(self, context: JobContext, spec: dict[str, Any]) -> JobOutcome:
        reason = self._stockdb_wait_reason()
        if reason and spec.get("universe", "").lower() == "csi800":
            return self._waiting(reason)  # Membership planning itself needs StockDB evidence.
        state = self._initial_state(context, spec)
        if state.pop("stockdb_waiting", False) and not reason:
            # The updater changes local inputs. Recheck affected earlier successes
            # before publishing a fingerprint for the new accepted generation.
            state["completed_symbols"] = [
                symbol for symbol in state["completed_symbols"] if not self._uses_stockdb(symbol)
            ]
            state["failures"] = [item for item in state["failures"] if not self._uses_stockdb(item["symbol"])]
            state["succeeded"] = len(state["completed_symbols"]) - len(state["failures"])
            state["next_index"] = len(state["completed_symbols"])
        elif reason:
            state["stockdb_waiting"] = True
        context.write_checkpoint(REFRESH_CHECKPOINT, context.spec_hash, state)
        symbols = [str(symbol) for symbol in state["symbols"]]
        waiting = self._execute(context, spec, state)
        if waiting:
            state["stockdb_waiting"] = True
            context.write_checkpoint(REFRESH_CHECKPOINT, context.spec_hash, state)
            return self._waiting(waiting)
        failures = [*state["blocked_failures"], *state["failures"]]
        outcome = "completed_with_warnings" if failures else "completed"
        result = {
            **state,
            "failures": failures,
            "current_symbol": "",
            "outcome": outcome,
            "total": len(symbols) + len(state["blocked_failures"]),
            "next_index": len(symbols) + len(state["blocked_failures"]),
            "failed": len(failures),
            "local_fingerprint": self._fingerprint(list(state["original_symbols"])),
        }
        artifact = context.write_artifact(
            REFRESH_RESULT_KIND,
            result,
            {"schema_version": REFRESH_SCHEMA, "lineage": {"spec_hash": context.spec_hash}},
        )
        context.emit("data_refresh_completed", {"outcome": outcome, "failed": len(failures)})
        self._publish_market_snapshot()
        return JobOutcome("completed", "行情刷新已完成", str(artifact["id"]))

    @staticmethod
    def _state(store: UnifiedJobStore, job: dict[str, Any]) -> dict[str, Any]:
        artifact = store.latest_artifact(str(job["id"]), REFRESH_RESULT_KIND)
        if artifact and job["status"] == "completed":
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
            "total_known": bool(state or "symbols" in spec),
            "planning": not state and "symbols" not in spec and job["status"] in {"queued", "running"},
            "succeeded": int(state.get("succeeded") or 0),
            "failed": int(state.get("failed") or len(failures)),
            "failures": failures[-200:],
            "current_symbol": str(state.get("current_symbol") or ""),
            "outcome": str(state.get("outcome") or ""),
            "waiting_on": str(job.get("waiting_on") or ""),
            "next_retry_at": float(job.get("next_retry_at") or 0),
        })
        value["can_retry"] = not value["waiting_on"] and bool(value["can_retry"]) and (
            job["status"] in {"failed", "cancelled", "interrupted"}
            or any(item.get("retryable") for item in failures)
        ) and spec.get("refresh_schema") == REFRESH_SCHEMA
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
        if not source["can_retry"]:
            raise ValueError("当前任务不能续跑")
        return self._project(runtime.store, runtime.retry(job_id))

    def events(self, job_id: str, after: int = 0, limit: int = 500) -> builtins.list[dict[str, Any]]:
        store = self._read_store()
        self._project(store, store.get(job_id))
        return store.events(job_id, after, limit)

    def start(self) -> None:
        if self._owns_runtime():
            self._ensure_runtime().start()
            if os.environ.get("QM_WORKER_SUPERVISOR") == "1":
                with self._lock:
                    if self._scheduler is None or not self._scheduler.is_alive():
                        self._stop.clear()
                        self._scheduler = threading.Thread(
                            target=self._maintain, name="data-maintenance-intake", daemon=True,
                        )
                        self._scheduler.start()

    def maintain_consumed(self) -> builtins.list[dict[str, Any]]:
        """Worker-only intake; page reads remain strictly local and read-only."""
        cfg = get_config()
        if not cfg.data.repair_enabled:
            return []
        jobs = [self.create("market")]
        if cfg.automation.watchlist:
            jobs.append(self._submit(
                "watchlist", "", str(market_date() - timedelta(days=365)),
                str(market_date()), cfg.automation.watchlist,
            ))
        universes = {cfg.automation.primary_universe}
        if cfg.lab.enabled:
            universes.add(cfg.lab.universe)
        for universe in sorted(universes - {""}):
            try:
                jobs.append(self.create("universe", universe=universe))
            except (OSError, RuntimeError, TypeError, ValueError):
                logger.warning("消费范围暂不可用，保留缺口并等待下次检查: %s", universe, exc_info=True)
        return jobs

    def _maintain(self) -> None:
        while not self._stop.is_set():
            try:
                # Existing automatic-maintenance switch also bounds intake.
                if get_config().data.repair_enabled and not self._ensure_runtime().stopping:
                    for job in self.maintain_consumed():
                        if self._retry_due(job):
                            self.resume(str(job["id"]))
            except (OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
                logger.warning("数据自动维护接纳失败，稍后重试", exc_info=True)
            self._stop.wait(60)

    @staticmethod
    def _retry_due(job: dict[str, Any]) -> bool:
        if not job.get("can_retry") or not job.get("finished_at"):
            return False
        finished = datetime.fromisoformat(str(job["finished_at"]))
        delay = min(3600, 60 * 2 ** max(0, int(job.get("attempt") or 1) - 1))
        return (datetime.now(UTC) - finished).total_seconds() >= delay

    @property
    def idle(self) -> bool:
        runtime = self._runtime
        return runtime is None or runtime.idle

    def pause(self) -> None:
        with self._lock:
            runtime = self._runtime
            if runtime is not None:
                runtime.pause()

    def shutdown(self, timeout: float = 10.0) -> None:
        self._stop.set()
        scheduler = self._scheduler
        if scheduler is not None:
            scheduler.join(timeout=timeout)
        with self._lock:
            runtime = self._runtime
        if runtime is not None:
            runtime.stop(deadline_seconds=timeout)


data_refresh_manager = DataRefreshManager()
