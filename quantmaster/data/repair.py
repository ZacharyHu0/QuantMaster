"""Durable, rate-limited repair queue for rebuildable local data artifacts."""

from __future__ import annotations

import builtins
import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

from quantmaster.config import get_config
from quantmaster.runtime.jobs import WorkerIdentity, lease_deadline
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.sqlite import connect_sqlite

logger = logging.getLogger(__name__)

RepairHandler = Callable[[dict[str, Any]], dict[str, Any] | None]


def _canonical(value: dict[str, Any]) -> str:
    return strict_json_dumps(value, sort_keys=True)


def _idempotency_key(kind: str, target: str) -> str:
    return hashlib.sha256(f"{kind}\0{target}".encode()).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sync_directory(path: Path) -> None:
    """Best-effort directory fsync; Windows does not expose it through ``os.open``."""
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def quarantine_file(
    path: str | Path,
    *,
    category: str,
    target: str,
    reason: str,
) -> dict[str, Any] | None:
    """Move an original aside atomically and persist an audit manifest beside it."""
    source = Path(path).resolve()
    if not source.is_file():
        return None
    root = get_config().data_root.resolve()
    quarantine = root / "quarantine" / category / date.today().isoformat()
    quarantine.mkdir(parents=True, exist_ok=True)
    content_sha256 = _file_sha256(source)
    file_size = source.stat().st_size
    token = uuid.uuid4().hex
    destination = quarantine / f"{source.name}.{token}.quarantine"
    os.replace(source, destination)
    _sync_directory(source.parent)
    _sync_directory(quarantine)
    manifest = {
        "schema_version": 1,
        "category": category,
        "target": target,
        "reason": reason,
        "original_path": str(source),
        "quarantine_path": str(destination),
        "content_sha256": content_sha256,
        "file_size": file_size,
        "quarantined_at": time.time(),
    }
    manifest_path = destination.with_suffix(destination.suffix + ".json")
    manifest_path.write_text(_canonical(manifest), encoding="utf-8")
    with manifest_path.open("rb+") as stream:
        os.fsync(stream.fileno())
    _sync_directory(quarantine)
    return manifest


class DataRepairManager:
    """Persistent repair scheduler with leases, backoff, budgets and audit events."""

    def __init__(self, path: str | Path | None = None) -> None:
        self._explicit_path = Path(path) if path is not None else None
        self.identity = WorkerIdentity.create("data-repair")
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._wakeup = threading.Event()
        self._workers: list[threading.Thread] = []
        self._handlers: dict[str, RepairHandler] = {}
        self._initialized: set[str] = set()
        self._register_builtin_handlers()
        self._ensure_schema()

    def _path(self) -> Path:
        return self._explicit_path or get_config().data_root / "data_repairs.sqlite"

    def _conn(self) -> sqlite3.Connection:
        connection = connect_sqlite(self._path(), row_factory=True)
        self._initialize(connection)
        return connection

    def _ensure_schema(self) -> None:
        with self._conn():
            pass

    def _initialize(self, connection: sqlite3.Connection) -> None:
        key = str(self._path().resolve())
        with self._lock:
            if key in self._initialized:
                return
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS data_repairs (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    target TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    next_run REAL NOT NULL,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    owner TEXT NOT NULL DEFAULT '',
                    lease_expires REAL NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    result_json TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    completed_at REAL NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_data_repairs_due
                    ON data_repairs(status,next_run,created_at);
                CREATE TABLE IF NOT EXISTS data_repair_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    repair_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    event_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    FOREIGN KEY(repair_id) REFERENCES data_repairs(id)
                );
                CREATE TABLE IF NOT EXISTS data_repair_budget (
                    day TEXT NOT NULL,
                    source TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    PRIMARY KEY(day,source)
                );
                """
            )
            columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(data_repairs)"
                ).fetchall()
            }
            if "cancel_requested" not in columns:
                connection.execute(
                    "ALTER TABLE data_repairs ADD COLUMN "
                    "cancel_requested INTEGER NOT NULL DEFAULT 0"
                )
            now = time.time()
            connection.execute(
                "UPDATE data_repairs SET status='queued',owner='',lease_expires=0,"
                "next_run=MIN(next_run,?),updated_at=? WHERE status='running' "
                "AND lease_expires<=?",
                (now, now, now),
            )
            connection.commit()
            self._initialized.add(key)

    def register_handler(self, kind: str, handler: RepairHandler) -> None:
        self._handlers[str(kind)] = handler

    def _register_builtin_handlers(self) -> None:
        self.register_handler("bar", self._repair_bar)
        self.register_handler("api_cache", self._repair_api_cache)
        self.register_handler("research_partition", self._repair_research_partition)

    def enqueue(
        self,
        kind: str,
        target: str,
        *,
        reason: str,
        spec: dict[str, Any],
        source: str = "unknown",
    ) -> dict[str, Any]:
        """Create one active repair per logical target without rewriting its specification."""
        now = time.time()
        key = _idempotency_key(kind, target)
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM data_repairs WHERE idempotency_key=?", (key,),
            ).fetchone()
            if row is None:
                repair_id = uuid.uuid4().hex
                maximum = max(1, int(get_config().data.repair_max_attempts))
                connection.execute(
                    "INSERT INTO data_repairs "
                    "(id,kind,target,idempotency_key,source,status,reason,spec_json,"
                    "max_attempts,next_run,created_at,updated_at) "
                    "VALUES (?,?,?,?,?,'queued',?,?,?,?,?,?)",
                    (
                        repair_id, kind, target, key, source, reason, _canonical(spec),
                        maximum, now, now, now,
                    ),
                )
                self._event(connection, repair_id, 0, {"type": "queued", "reason": reason})
            else:
                repair_id = str(row["id"])
                if row["status"] in {"completed", "quarantined"}:
                    return self._decode(row)
                connection.execute(
                    "UPDATE data_repairs SET reason=?,updated_at=? WHERE id=?",
                    (reason, now, repair_id),
                )
        self._wakeup.set()
        return self.get(repair_id)

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        repair_id: str,
        attempt: int,
        payload: dict[str, Any],
    ) -> None:
        connection.execute(
            "INSERT INTO data_repair_events(repair_id,attempt,event_json,created_at) "
            "VALUES (?,?,?,?)",
            (repair_id, attempt, _canonical(payload), time.time()),
        )

    @staticmethod
    def _decode(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["spec"] = json.loads(value.pop("spec_json"))
        value["result"] = json.loads(value.pop("result_json"))
        value.pop("idempotency_key", None)
        return value

    def get(self, repair_id: str) -> dict[str, Any]:
        with self._conn() as connection:
            row = connection.execute(
                "SELECT * FROM data_repairs WHERE id=?", (repair_id,),
            ).fetchone()
        if row is None:
            raise KeyError(repair_id)
        return self._decode(row)

    def list(self, *, status: str = "", limit: int = 100) -> list[dict[str, Any]]:
        query = "SELECT * FROM data_repairs"
        params: builtins.list[Any] = []
        if status:
            query += " WHERE status=?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(max(1, min(int(limit), 1000)))
        with self._conn() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._decode(row) for row in rows]

    def events(self, repair_id: str, after: int = 0) -> builtins.list[dict[str, Any]]:
        with self._conn() as connection:
            rows = connection.execute(
                "SELECT seq,attempt,event_json,created_at FROM data_repair_events "
                "WHERE repair_id=? AND seq>? ORDER BY seq LIMIT 2000",
                (repair_id, max(0, int(after))),
            ).fetchall()
        return [
            {
                "seq": row["seq"], "attempt": row["attempt"],
                "created_at": row["created_at"], **json.loads(row["event_json"]),
            }
            for row in rows
        ]

    def retry(self, repair_id: str) -> dict[str, Any]:
        now = time.time()
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT status,attempt FROM data_repairs WHERE id=?", (repair_id,),
            ).fetchone()
            if row is None:
                raise KeyError(repair_id)
            if row["status"] not in {"failed", "quarantined", "cancelled"}:
                raise ValueError("只有失败、已隔离或已取消的修复任务可以重试")
            attempt = int(row["attempt"])
            connection.execute(
                "UPDATE data_repairs SET status='queued',next_run=?,owner='',"
                "lease_expires=0,last_error='',completed_at=0,cancel_requested=0,"
                "updated_at=? WHERE id=?",
                (now, now, repair_id),
            )
            self._event(connection, repair_id, attempt, {"type": "retried"})
        self._wakeup.set()
        return self.get(repair_id)

    def resolve(
        self, kind: str, target: str, *, result: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Close a stale repair after an independent integrity check succeeds."""
        now = time.time()
        key = _idempotency_key(kind, target)
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM data_repairs WHERE idempotency_key=?", (key,),
            ).fetchone()
            if row is None:
                return None
            status = str(row["status"])
            if status == "completed":
                return self._decode(row)
            if status not in {"queued", "failed", "cancelled", "quarantined"}:
                return self._decode(row)
            repair_id = str(row["id"])
            attempt = int(row["attempt"])
            connection.execute(
                "UPDATE data_repairs SET status='completed',owner='',lease_expires=0,"
                "result_json=?,last_error='',completed_at=?,updated_at=?,"
                "cancel_requested=0 WHERE id=?",
                (_canonical(result), now, now, repair_id),
            )
            self._event(connection, repair_id, attempt, {
                "type": "resolved_by_validation", "result": result,
            })
        return self.get(repair_id)

    def cancel(self, repair_id: str) -> dict[str, Any]:
        now = time.time()
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT status,attempt FROM data_repairs WHERE id=?", (repair_id,),
            ).fetchone()
            if row is None:
                raise KeyError(repair_id)
            status = str(row["status"])
            if status == "queued":
                connection.execute(
                    "UPDATE data_repairs SET status='cancelled',cancel_requested=1,"
                    "updated_at=? WHERE id=?", (now, repair_id),
                )
                event = "cancelled"
            elif status == "running":
                connection.execute(
                    "UPDATE data_repairs SET status='cancelling',cancel_requested=1,"
                    "updated_at=? WHERE id=?", (now, repair_id),
                )
                event = "cancel_requested"
            elif status in {"cancelling", "cancelled"}:
                return self.get(repair_id)
            else:
                raise ValueError("当前修复任务不能取消")
            self._event(connection, repair_id, int(row["attempt"]), {"type": event})
        return self.get(repair_id)

    def _claim(self) -> dict[str, Any] | None:
        now = time.time()
        today = date.today().isoformat()
        budget = max(0, int(get_config().data.repair_daily_budget))
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT * FROM data_repairs WHERE status='queued' AND cancel_requested=0 "
                "AND next_run<=? "
                "ORDER BY next_run,created_at LIMIT 100",
                (now,),
            ).fetchall()
            selected: sqlite3.Row | None = None
            for row in rows:
                used = connection.execute(
                    "SELECT attempts FROM data_repair_budget WHERE day=? AND source=?",
                    (today, row["source"]),
                ).fetchone()
                if budget and int(used[0] if used else 0) >= budget:
                    continue
                selected = row
                break
            if selected is None:
                return None
            repair_id = str(selected["id"])
            attempt = int(selected["attempt"]) + 1
            changed = connection.execute(
                "UPDATE data_repairs SET status='running',attempt=?,owner=?,lease_expires=?,"
                "updated_at=? WHERE id=? AND status='queued'",
                (attempt, self.identity.value, lease_deadline(120), now, repair_id),
            ).rowcount
            if not changed:
                return None
            connection.execute(
                "INSERT INTO data_repair_budget(day,source,attempts) VALUES (?,?,1) "
                "ON CONFLICT(day,source) DO UPDATE SET attempts=attempts+1",
                (today, selected["source"]),
            )
            self._event(connection, repair_id, attempt, {
                "type": "claimed", "owner": self.identity.value,
            })
            selected = connection.execute(
                "SELECT * FROM data_repairs WHERE id=?", (repair_id,),
            ).fetchone()
        return self._decode(selected)

    def run_one(self) -> dict[str, Any] | None:
        """Claim and execute one due item; exposed for deterministic maintenance/tests."""
        item = self._claim()
        if item is None:
            return None
        handler = self._handlers.get(str(item["kind"]))
        try:
            if handler is None:
                raise RuntimeError(f"没有 {item['kind']} 修复处理器")
            result = handler(item) or {}
        except Exception as exc:
            logger.exception("数据修复失败 repair=%s target=%s", item["id"], item["target"])
            self._finish_failure(item, exc)
        else:
            self._finish_success(item, result)
        return self.get(str(item["id"]))

    def _finish_success(self, item: dict[str, Any], result: dict[str, Any]) -> None:
        now = time.time()
        with self._conn() as connection:
            connection.execute(
                "UPDATE data_repairs SET status='completed',owner='',lease_expires=0,"
                "result_json=?,last_error='',completed_at=?,updated_at=? "
                "WHERE id=? AND owner=? AND status IN ('running','cancelling')",
                (_canonical(result), now, now, item["id"], self.identity.value),
            )
            self._event(connection, str(item["id"]), int(item["attempt"]), {
                "type": "completed", "result": result,
                "cancel_arrived_after_claim": bool(connection.execute(
                    "SELECT cancel_requested FROM data_repairs WHERE id=?", (item["id"],),
                ).fetchone()[0]),
            })

    def _finish_failure(self, item: dict[str, Any], error: Exception) -> None:
        now = time.time()
        attempt = int(item["attempt"])
        exhausted = attempt >= int(item["max_attempts"])
        base = max(0.01, float(get_config().data.repair_retry_backoff))
        delay = min(base * (2 ** max(0, attempt - 1)), 86400.0)
        status = "failed" if exhausted else "queued"
        with self._conn() as connection:
            cancelled = bool(connection.execute(
                "SELECT cancel_requested FROM data_repairs WHERE id=?", (item["id"],),
            ).fetchone()[0])
            if cancelled:
                status = "cancelled"
            connection.execute(
                "UPDATE data_repairs SET status=?,owner='',lease_expires=0,last_error=?,"
                "next_run=?,updated_at=? WHERE id=? AND owner=?",
                (
                    status, f"{type(error).__name__}: {error}"[:2000],
                    0 if cancelled else now + delay,
                    now, item["id"], self.identity.value,
                ),
            )
            self._event(connection, str(item["id"]), attempt, {
                "type": status, "error": f"{type(error).__name__}: {error}",
                "retry_at": 0 if exhausted or cancelled else now + delay,
            })

    def start(self) -> None:
        with self._lock:
            if self._workers:
                return
            self._stop.clear()
            count = max(1, min(int(get_config().data.repair_max_workers), 8))
            for index in range(count):
                thread = threading.Thread(
                    target=self._loop,
                    name=f"data-repair-{index + 1}",
                    daemon=True,
                )
                self._workers.append(thread)
                thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            if self.run_one() is None:
                self._wakeup.wait(5.0)
                self._wakeup.clear()

    def shutdown(self, timeout: float = 10.0) -> None:
        self._stop.set()
        self._wakeup.set()
        with self._lock:
            workers, self._workers = self._workers, []
        per_worker = max(0.05, timeout / max(1, len(workers)))
        for worker in workers:
            worker.join(per_worker)
        with self._conn() as connection:
            connection.execute(
                "UPDATE data_repairs SET status='queued',owner='',lease_expires=0,"
                "next_run=?,updated_at=? WHERE owner=? AND status IN ('running','cancelling')",
                (time.time(), time.time(), self.identity.value),
            )

    @staticmethod
    def _repair_bar(item: dict[str, Any]) -> dict[str, Any]:
        from quantmaster.data.registry import RefreshMode, load_history
        from quantmaster.data.storage import BarStore

        spec = item["spec"]
        root = Path(spec["root"]).resolve()
        symbol = str(spec["symbol"])
        store = BarStore(root=root)
        start = str(spec.get("start") or "1990-01-01")
        end = str(spec.get("end") or date.today())
        with store.lock(symbol):
            target = store.path_for_repair(symbol)
            quarantine = quarantine_file(
                target, category="bars", target=symbol, reason=str(item["reason"]),
            )
            frame = load_history(
                symbol, start, end, store=store, refresh=RefreshMode.FULL,
                priority="maintenance",
            )
            result = store.read(symbol, enqueue_repair=False)
        if result.status != "ready" or frame.empty:
            raise RuntimeError(f"重拉后完整性仍异常: {result.status}: {result.reason}")
        return {
            "rows": len(frame), "content_sha256": result.content_sha256,
            "quarantine": quarantine,
        }

    @staticmethod
    def _repair_api_cache(item: dict[str, Any]) -> dict[str, Any]:
        """Validate a replacement written after a corrupt endpoint cache was isolated."""
        import pandas as pd

        spec = item["spec"]
        root = Path(spec["root"]).resolve()
        target = Path(spec["path"]).resolve()
        try:
            target.relative_to(root)
        except ValueError as exc:
            raise RuntimeError("接口缓存修复目标越出声明目录") from exc
        quarantine = spec.get("quarantine")
        if not target.exists():
            return {
                "state": "quarantined",
                "replacement": "not_available",
                "quarantine": quarantine,
            }
        try:
            frame = pd.read_parquet(target)
        except (ImportError, OSError, TypeError, ValueError) as exc:
            quarantine_file(
                target,
                category="api-cache",
                target=str(item["target"]),
                reason=f"替换缓存仍不可读: {type(exc).__name__}: {exc}",
            )
            raise RuntimeError("替换后的接口缓存仍不可读") from exc
        return {
            "state": "replaced",
            "rows": len(frame),
            "quarantine": quarantine,
        }

    @staticmethod
    def _repair_research_partition(item: dict[str, Any]) -> dict[str, Any]:
        from quantmaster.research.contracts import ArtifactKind, AssetClass, Frequency
        from quantmaster.research.engine import ResearchEngine
        from quantmaster.research.lake import ResearchLake

        spec = item["spec"]
        lake = ResearchLake(spec["root"])
        metadata = dict(spec["metadata"])
        key = str(metadata["partition_key"])
        owner = f"repair:{item['id']}"
        if not lake.catalog.claim(key, owner):
            raise RuntimeError(f"研究分区正在由其他任务写入: {key}")
        try:
            target = lake.path_for_repair(metadata)
            quarantine = quarantine_file(
                target,
                category="research",
                target=key,
                reason=str(item["reason"]),
            )
            lake.catalog.delete_partition(key)
        finally:
            lake.catalog.release(key, owner)
        trade_date = str(metadata["trade_date"])
        kind = ArtifactKind(str(metadata["kind"]))
        asset = AssetClass(str(metadata["asset_class"]))
        frequency = Frequency(str(metadata["frequency"]))
        if frequency != Frequency.DAILY:
            raise RuntimeError("目前只允许自动重建日频研究分区")
        if (
            kind == ArtifactKind.RAW
            and asset == AssetClass.STOCK
            and str(metadata["dataset_id"]) == "stock_bars"
        ):
            lake.materialize_bar_store(None, trade_date, trade_date, asset_class=asset)
            rebuilt = lake.catalog.partition(
                kind, asset, frequency, str(metadata["dataset_id"]), trade_date,
            )
            if rebuilt is not None:
                lake.validate_partition(rebuilt, enqueue_repair=False)
                return {
                    "partition_key": metadata["partition_key"],
                    "quarantine": quarantine,
                    "source": "barstore",
                }
        engine = ResearchEngine(lake=lake)
        spec_ids = list((metadata.get("spec_versions") or {}).keys())
        if kind in {ArtifactKind.FACTOR, ArtifactKind.LABEL, ArtifactKind.RISK, ArtifactKind.MODEL}:
            if not spec_ids:
                raise RuntimeError("研究分区缺少可执行规格血缘")
            plan = engine.plan(
                trade_date, trade_date, asset_classes=[asset], spec_ids=spec_ids,
                mode="historical",
            )
        else:
            plan = engine.plan(
                trade_date, trade_date, asset_classes=[asset],
                datasets=[str(metadata["dataset_id"])], mode="historical",
            )
        engine.execute(plan)
        rebuilt = lake.catalog.partition(
            kind, asset, frequency, str(metadata["dataset_id"]), trade_date,
        )
        if rebuilt is None:
            raise RuntimeError("研究任务结束后目标分区仍不存在")
        lake.validate_partition(rebuilt, enqueue_repair=False)
        return {"partition_key": metadata["partition_key"], "quarantine": quarantine}


_MANAGER: DataRepairManager | None = None
_MANAGER_LOCK = threading.Lock()


def get_data_repair_manager() -> DataRepairManager:
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None:
            _MANAGER = DataRepairManager()
        return _MANAGER


def reset_data_repair_manager_for_tests() -> None:
    global _MANAGER
    with _MANAGER_LOCK:
        manager, _MANAGER = _MANAGER, None
    if manager is not None:
        manager.shutdown(timeout=1.0)


def enqueue_repair(
    kind: str,
    target: str,
    *,
    reason: str,
    spec: dict[str, Any],
    source: str = "unknown",
) -> dict[str, Any] | None:
    if not get_config().data.repair_enabled:
        return None
    return get_data_repair_manager().enqueue(
        kind, target, reason=reason, spec=spec, source=source,
    )


def resolve_repair(
    kind: str, target: str, *, result: dict[str, Any],
) -> dict[str, Any] | None:
    """Reconcile a queued or terminal repair with independently validated data."""
    return get_data_repair_manager().resolve(kind, target, result=result)
