"""Explicitly consented, single-symbol vendor-online experiments."""

from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.free_stockdb_source import FreeStockDBSource

_SYMBOL = re.compile(r"^\d{6}(?:\.(?:SH|SZ|BJ))?$")
_STAT_DATE = re.compile(r"^\d{4}(?:q[1-4]|-\d{2}-\d{2})$", re.IGNORECASE)
_FUNDAMENTAL_DATASETS = frozenset({"cash_flow", "income", "balance", "valuation"})
_REMOTE_ERRORS = (
    OSError, RuntimeError, TypeError, ValueError, AttributeError, KeyError, ImportError,
)


class StockDBExperimentalOnline:
    _file_guard = threading.RLock()
    _remote_slots = threading.BoundedSemaphore(2)

    def __init__(self, source: FreeStockDBSource | None = None, root: str | Path | None = None):
        self.source = source or FreeStockDBSource()
        self.root = Path(root or (get_config().data_root / "stockdb-experimental")).resolve()
        self._lock = self._file_guard

    def _read_state(self, name: str, default: dict[str, Any]) -> dict[str, Any]:
        try:
            value = json.loads((self.root / name).read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else dict(default)
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            return dict(default)

    def _write_state(self, name: str, value: dict[str, Any]) -> None:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(f"{path.suffix}.{threading.get_ident()}.tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)

    def _audit(
        self, *, kind: str, symbol: str, params: dict[str, Any], outcome: str,
        cached: bool = False, error: str = "",
    ) -> None:
        row = {
            "at": datetime.now(UTC).isoformat(timespec="seconds"), "kind": kind,
            "symbol": symbol, "params": params, "outcome": outcome,
            "cached": cached, "error": error[:500], "remote": True,
            "upstream": "vendor-declared-unverified",
            "upstream_evidence": "not_provided",
            "distribution": "free-stockdb-online",
        }
        with self._lock:
            path = self.root / "audit.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    def _circuit_before(self) -> None:
        with self._lock:
            state = self._read_state("circuit.json", {"failures": 0, "open_until": 0})
        remaining = float(state.get("open_until") or 0) - time.time()
        if remaining > 0:
            raise RuntimeError(f"free-stockdb 在线实验接口熔断中，约 {int(remaining) + 1} 秒后重试")

    def _circuit_result(self, success: bool) -> None:
        with self._lock:
            state = self._read_state("circuit.json", {"failures": 0, "open_until": 0})
            if success:
                state = {"failures": 0, "open_until": 0, "updated_at": time.time()}
            else:
                failures = int(state.get("failures") or 0) + 1
                state = {
                    "failures": failures,
                    "open_until": time.time() + 300 if failures >= 3 else 0,
                    "updated_at": time.time(),
                }
            self._write_state("circuit.json", state)

    @contextmanager
    def _remote_call(self, kind: str):
        if not self._remote_slots.acquire(blocking=False):
            raise RuntimeError("free-stockdb 在线实验接口并发已满")
        try:
            self._circuit_before()
            self._consume_quota(kind)
            try:
                yield
            except _REMOTE_ERRORS:
                self._circuit_result(False)
                raise
            else:
                self._circuit_result(True)
        finally:
            self._remote_slots.release()

    def _consume_quota(self, kind: str) -> None:
        cfg = get_config().data
        with self._lock:
            path = self.root / "quota.json"
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
            except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
                state = {"date": str(date.today()), "count": 0, "by_kind": {}}
            if state.get("date") != str(date.today()):
                state = {"date": str(date.today()), "count": 0, "by_kind": {}}
            if int(state.get("count") or 0) >= cfg.free_stockdb_experimental_daily_quota:
                raise RuntimeError("free-stockdb 在线实验接口已达到今日配额")
            state["count"] = int(state.get("count") or 0) + 1
            by_kind = dict(state.get("by_kind") or {})
            by_kind[kind] = int(by_kind.get(kind) or 0) + 1
            state["by_kind"] = by_kind
            path.parent.mkdir(parents=True, exist_ok=True)
            self._write_state("quota.json", state)

    def _cached(self, key: str, ttl: int, fetch) -> tuple[Any, bool, str]:
        digest = hashlib.sha256(key.encode()).hexdigest()
        path = self.root / "cache" / f"{digest}.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            if time.time() - float(value.get("fetched_epoch") or 0) <= ttl:
                return value["data"], True, str(value.get("fetched_at") or "")
        except (FileNotFoundError, OSError, ValueError, KeyError, json.JSONDecodeError):
            pass
        data = fetch()
        fetched_at = pd.Timestamp.now(tz="UTC").isoformat()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "fetched_epoch": time.time(), "fetched_at": fetched_at, "data": data,
        }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return data, False, fetched_at

    @staticmethod
    def _code(symbol: str) -> str:
        value = symbol.strip().upper()
        if _SYMBOL.fullmatch(value) is None:
            raise ValueError("实验接口一次只接受一只沪深北六位代码")
        return value.partition(".")[0]

    def tick(self, symbol: str, *, count: int = 1) -> dict[str, Any]:
        cfg = get_config().data
        if not cfg.free_stockdb_experimental_tick_enabled:
            raise PermissionError("free-stockdb 在线 Tick 实验开关未启用")
        code = self._code(symbol)
        count = max(1, min(int(count), 20))

        def fetch():
            with self._remote_call("tick"):
                module = self.source._load_sdk_module()
                function = module.__dict__.get("get_last_tick")
                if not callable(function):
                    raise RuntimeError("当前 SDK 未暴露在线 Tick 接口")
                value = function(code, count=count)
            if isinstance(value, pd.DataFrame):
                return value.replace({float("inf"): None, float("-inf"): None}).where(
                    pd.notna(value), None,
                ).to_dict("records")
            return value

        try:
            data, cached, fetched_at = self._cached(f"tick:{code}:{count}", 5, fetch)
        except _REMOTE_ERRORS as exc:
            self._audit(kind="tick", symbol=symbol.upper(), params={"count": count},
                        outcome="failed", error=str(exc))
            raise
        self._audit(kind="tick", symbol=symbol.upper(), params={"count": count},
                    outcome="success", cached=cached)
        return {
            "symbol": symbol.upper(), "data": data, "remote": True, "cached": cached,
            "provider": "tushare-via-free-stockdb-online", "fetched_at": fetched_at,
            "staleness_seconds": 5, "research_only": True,
        }

    def fundamentals(self, symbol: str, *, dataset: str, stat_date: str) -> dict[str, Any]:
        cfg = get_config().data
        if not cfg.free_stockdb_experimental_fundamentals_enabled:
            raise PermissionError("free-stockdb 在线财务实验开关未启用")
        code = self._code(symbol)
        dataset = dataset.strip().lower()
        if dataset not in _FUNDAMENTAL_DATASETS:
            raise ValueError("财务数据集仅允许 cash_flow/income/balance/valuation")
        if _STAT_DATE.fullmatch(stat_date.strip()) is None:
            raise ValueError("stat_date 仅允许 YYYYqN 或 YYYY-MM-DD")

        def fetch():
            with self._remote_call("fundamentals"):
                module = self.source._load_sdk_module()
                namespace = module.__dict__
                table = namespace.get(dataset)
                query = namespace.get("query")
                function = namespace.get("get_fundamentals")
                if table is None or not callable(query) or not callable(function):
                    raise RuntimeError("当前 SDK 未暴露白名单财务接口")
                exchange = "XSHG" if code.startswith(("5", "6", "9")) else "XSHE"
                value = function(
                    query(table).filter(table.code == f"{code}.{exchange}"), statDate=stat_date,
                )
            frame = value if isinstance(value, pd.DataFrame) else pd.DataFrame(value)
            return frame.where(pd.notna(frame), None).to_dict("records")

        key = f"fundamentals:{dataset}:{code}:{stat_date.lower()}"
        params = {"dataset": dataset, "stat_date": stat_date}
        try:
            data, cached, fetched_at = self._cached(key, 86400, fetch)
        except _REMOTE_ERRORS as exc:
            self._audit(kind="fundamentals", symbol=symbol.upper(), params=params,
                        outcome="failed", error=str(exc))
            raise
        self._audit(kind="fundamentals", symbol=symbol.upper(), params=params,
                    outcome="success", cached=cached)
        return {
            "symbol": symbol.upper(), "dataset": dataset, "stat_date": stat_date,
            "data": data, "remote": True, "cached": cached,
            "provider": "tushare-via-free-stockdb-online", "fetched_at": fetched_at,
            "staleness_seconds": 86400, "research_only": True,
        }

    def status(self) -> dict[str, Any]:
        cfg = get_config().data
        quota = self._read_state("quota.json", {"date": str(date.today()), "count": 0})
        circuit = self._read_state("circuit.json", {"failures": 0, "open_until": 0})
        return {
            "tick_enabled": cfg.free_stockdb_experimental_tick_enabled,
            "fundamentals_enabled": cfg.free_stockdb_experimental_fundamentals_enabled,
            "daily_quota": cfg.free_stockdb_experimental_daily_quota,
            "quota": quota, "circuit": circuit, "max_concurrency": 2,
            "audit_path": str(self.root / "audit.jsonl"),
        }
