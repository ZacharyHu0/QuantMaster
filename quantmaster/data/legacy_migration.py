"""Durable one-shot migration runner for retired local data contracts."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from quantmaster.config import get_config
from quantmaster.data.migration import backup_sqlite_tree, validate_backup_tree
from quantmaster.runtime.maintenance import MaintenanceLease, maintenance_barrier
from quantmaster.runtime.sqlite import connect_sqlite


class LegacyMigrationError(RuntimeError):
    """A migration cannot proceed without weakening its evidence boundary."""


@dataclass(frozen=True)
class MigrationRecord:
    record_key: str
    outcome: str
    diagnostic_code: str = ""
    unknown_fields: tuple[str, ...] = ()
    detail: str = ""


class DomainMigrator(Protocol):
    name: str

    def inspect(self, root: Path) -> Iterable[MigrationRecord]: ...

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]: ...

    def rollback(self, root: Path, backup_root: Path) -> None: ...


@dataclass(frozen=True)
class OfflineMaintenanceEvidence:
    confirmed_root: Path
    writer_stopped: bool
    evidence: str


class _ProcessLease:
    """OS-released cross-process lease; a stale file is not mistaken for a held lock."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = path.open("a+b")
        self._handle.seek(0)
        if self._handle.read(1) == b"":
            self._handle.write(b"0")
            self._handle.flush()
        self._handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:  # pragma: no cover - Windows is the supported desktop runtime
                import fcntl
                fcntl.flock(  # type: ignore[attr-defined]
                    self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB,  # type: ignore[attr-defined]
                )
        except (OSError, BlockingIOError) as exc:
            self._handle.close()
            raise LegacyMigrationError("另一个进程正在执行离线迁移") from exc

    def close(self) -> None:
        if self._handle.closed:
            return
        self._handle.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:  # pragma: no cover
            import fcntl
            fcntl.flock(  # type: ignore[attr-defined]
                self._handle.fileno(), fcntl.LOCK_UN,  # type: ignore[attr-defined]
            )
        self._handle.close()


_MIGRATORS: dict[str, DomainMigrator] = {}


def register_migrator(migrator: DomainMigrator) -> None:
    if not migrator.name or migrator.name in _MIGRATORS:
        raise ValueError(f"重复或无效的迁移类型：{migrator.name!r}")
    _MIGRATORS[migrator.name] = migrator


def registered_migrations() -> tuple[str, ...]:
    register_builtin_migrations()
    return tuple(sorted(_MIGRATORS))


def register_builtin_migrations() -> None:
    """Load domain adapters only when migration management is explicitly used."""
    from quantmaster.after_close.migration import after_close_legacy_migrator
    from quantmaster.ai.news_migration import news_contract_migrator
    from quantmaster.automation.migration import automation_contract_migrator
    from quantmaster.backtest.paper_legacy_migration import PaperLegacyMigrator
    from quantmaster.data.legacy_migrations import market_data_legacy_migrator
    from quantmaster.data.remaining_schema_migration import remaining_schema_migrator
    from quantmaster.data.startup_schema_migration import startup_schema_migrator
    from quantmaster.data.store_schema_migration import store_schema_migrator
    from quantmaster.decision.migration import decision_legacy_migrator

    for migrator in (
        market_data_legacy_migrator,
        decision_legacy_migrator,
        after_close_legacy_migrator,
        news_contract_migrator,
        automation_contract_migrator,
        PaperLegacyMigrator(),
        startup_schema_migrator,
        store_schema_migrator,
        remaining_schema_migrator,
    ):
        existing = _MIGRATORS.get(migrator.name)
        if existing is None:
            register_migrator(migrator)
        elif type(existing) is not type(migrator):
            raise ValueError(f"迁移类型冲突：{migrator.name}")


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class LegacyMigrationManager:
    """Persist migration progress so interruption never requires format guessing."""

    ACTIVE = frozenset({"queued", "backing_up", "running", "pausing", "rolling_back"})
    TERMINAL = frozenset({"completed", "failed", "cancelled", "rolled_back"})

    def __init__(
        self,
        root: str | Path | None = None,
        *,
        backup: Callable[[Path, Path], None] | None = None,
        offline_evidence: OfflineMaintenanceEvidence | None = None,
    ) -> None:
        self.root = Path(root or get_config().data_root).resolve()
        self.state_path = self.root / "legacy_contract_migrations.sqlite"
        self.backup_root = self.root / "backups" / "legacy-contracts"
        self._backup = backup or self._backup_sqlite_files
        self._offline_evidence = offline_evidence
        self._lock = threading.RLock()
        self._threads: dict[str, threading.Thread] = {}
        self._leases: dict[str, MaintenanceLease] = {}
        self._pause_requests: set[str] = set()
        self._initialized = False

    def _conn(self) -> sqlite3.Connection:
        connection = connect_sqlite(self.state_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        if self._initialized:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        with self._conn() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS migration_runs (
                    id TEXT PRIMARY KEY,
                    domain TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('dry_run','apply')),
                    status TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    total INTEGER NOT NULL DEFAULT 0,
                    checked INTEGER NOT NULL DEFAULT 0,
                    converted INTEGER NOT NULL DEFAULT 0,
                    blank INTEGER NOT NULL DEFAULT 0,
                    review INTEGER NOT NULL DEFAULT 0,
                    conflicts INTEGER NOT NULL DEFAULT 0,
                    last_key TEXT NOT NULL DEFAULT '',
                    last_batch INTEGER NOT NULL DEFAULT 0,
                    write_paused INTEGER NOT NULL DEFAULT 0,
                    estimated_remaining_seconds INTEGER,
                    backup_path TEXT NOT NULL DEFAULT '',
                    diagnostic_code TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    finished_at TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_migration
                    ON migration_runs((1))
                    WHERE status IN ('queued','backing_up','running','pausing','rolling_back');
                CREATE TABLE IF NOT EXISTS migration_audit (
                    run_id TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    batch INTEGER NOT NULL,
                    record_key TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    diagnostic_code TEXT NOT NULL DEFAULT '',
                    unknown_fields_json TEXT NOT NULL DEFAULT '[]',
                    detail TEXT NOT NULL DEFAULT '',
                    recorded_at TEXT NOT NULL,
                    PRIMARY KEY(run_id,record_key),
                    FOREIGN KEY(run_id) REFERENCES migration_runs(id)
                );
                """
            )
            connection.execute(
                "UPDATE migration_runs SET status='paused',phase='进程中断，可从最近批次续跑',"
                "write_paused=0,diagnostic_code='process_interrupted',updated_at=? "
                "WHERE status IN ('queued','backing_up','running','pausing','rolling_back')",
                (utc_now(),),
            )
        self._initialized = True

    def create(self, domain: str, *, mode: str = "dry_run", batch_size: int = 250) -> dict:
        register_builtin_migrations()
        if domain not in _MIGRATORS:
            raise LegacyMigrationError(f"未知迁移类型：{domain}")
        if mode not in {"dry_run", "apply"}:
            raise LegacyMigrationError("mode 仅支持 dry_run/apply")
        if not 1 <= int(batch_size) <= 5000:
            raise LegacyMigrationError("batch_size 必须在 1..5000")
        if mode == "apply":
            self._require_offline_evidence()
        self._initialize()
        with self._conn() as connection:
            active = connection.execute(
                "SELECT id FROM migration_runs WHERE status IN "
                "('queued','backing_up','running','pausing','rolling_back') LIMIT 1"
            ).fetchone()
        if active:
            raise LegacyMigrationError("已有历史合同迁移正在运行")
        run_id, now = uuid.uuid4().hex, utc_now()
        try:
            with self._conn() as connection:
                connection.execute(
                    "INSERT INTO migration_runs(id,domain,mode,status,phase,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (run_id, domain, mode, "queued", "等待开始", now, now),
                )
        except sqlite3.IntegrityError as exc:
            raise LegacyMigrationError("已有历史合同迁移正在运行") from exc
        thread = threading.Thread(
            target=self._run, args=(run_id, int(batch_size)), daemon=True,
            name=f"legacy-migration-{domain}",
        )
        with self._lock:
            self._threads[run_id] = thread
        thread.start()
        return self.get(run_id)

    def get(self, run_id: str) -> dict:
        if not self.state_path.is_file():
            raise KeyError(run_id)
        with self._conn() as connection:
            row = connection.execute(
                "SELECT * FROM migration_runs WHERE id=?", (run_id,),
            ).fetchone()
        if row is None:
            raise KeyError(run_id)
        value = dict(row)
        value["write_paused"] = bool(value["write_paused"])
        value["maintenance_mode"] = (
            "offline_writer_stop_verified" if value["write_paused"] else "not_active"
        )
        with self._lock:
            pause_requested = run_id in self._pause_requests
        if pause_requested and value["status"] in {"queued", "backing_up", "running"}:
            value["status"] = "pausing"
            value["phase"] = "正在安全暂停"
        value["unknown_results"] = self.unknown_results(run_id)
        return value

    def latest(self) -> dict | None:
        if not self.state_path.is_file():
            return None
        with self._conn() as connection:
            row = connection.execute(
                "SELECT id FROM migration_runs ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        return self.get(str(row[0])) if row else None

    def unknown_results(self, run_id: str, limit: int = 50) -> list[dict]:
        if not self.state_path.is_file():
            return []
        with self._conn() as connection:
            rows = connection.execute(
                "SELECT record_key,diagnostic_code,unknown_fields_json,detail,batch "
                "FROM migration_audit WHERE run_id=? AND outcome IN ('blank','review','conflict') "
                "ORDER BY batch,record_key LIMIT ?", (run_id, max(1, min(limit, 500))),
            ).fetchall()
        return [
            {
                "record_key": row[0],
                "diagnostic_code": row[1],
                "unknown_fields": json.loads(row[2]),
                "detail": row[3],
                "batch": row[4],
            }
            for row in rows
        ]

    def pause(self, run_id: str) -> dict:
        self._initialize()
        task = self.get(run_id)
        if task["status"] in {"paused", *self.TERMINAL}:
            return task
        if task["status"] not in {"queued", "backing_up", "running"}:
            raise LegacyMigrationError("迁移当前无法暂停")
        with self._lock:
            self._pause_requests.add(run_id)
        task["status"] = "pausing"
        task["phase"] = "正在安全暂停"
        return task

    def resume(self, run_id: str, *, batch_size: int = 250) -> dict:
        self._initialize()
        task = self.get(run_id)
        if task["mode"] == "apply":
            self._require_offline_evidence()
        with self._conn() as connection:
            changed = connection.execute(
                "UPDATE migration_runs SET status='queued',phase='等待续跑',error='',"
                "diagnostic_code='',updated_at=? "
                "WHERE id=? AND status IN ('paused','failed')",
                (utc_now(), run_id),
            ).rowcount
        if not changed:
            raise LegacyMigrationError("只有已暂停或失败的迁移可以续跑")
        thread = threading.Thread(
            target=self._run, args=(run_id, int(batch_size)), daemon=True,
            name=f"legacy-migration-resume-{run_id[:8]}",
        )
        with self._lock:
            self._threads[run_id] = thread
        thread.start()
        return self.get(run_id)

    def rollback(self, run_id: str) -> dict:
        self._initialize()
        task = self.get(run_id)
        self._require_offline_evidence()
        if task["mode"] != "apply" or task["status"] not in {"completed", "failed", "paused"}:
            raise LegacyMigrationError("只有 completed/failed/paused 且有备份的 apply 迁移可以回滚")
        backup_path = Path(task["backup_path"])
        if not backup_path.is_dir():
            raise LegacyMigrationError("迁移备份不存在，拒绝回滚")
        validate_backup_tree(backup_path)
        process_lease = self._process_lease()
        lease = maintenance_barrier.enter(f"legacy_migration_rollback:{task['domain']}", timeout=30)
        try:
            with maintenance_barrier.authorize(lease):
                self._set(run_id, status="rolling_back", phase="从可恢复备份回滚", write_paused=1)
                _MIGRATORS[task["domain"]].rollback(self.root, backup_path)
                self._set(
                    run_id, status="rolled_back", phase="已回滚", write_paused=0,
                    finished_at=utc_now(),
                )
        except (OSError, RuntimeError, sqlite3.Error, ValueError) as exc:
            with maintenance_barrier.authorize(lease):
                self._set(
                    run_id, status="failed", phase="回滚失败", diagnostic_code="rollback_failed",
                    error=str(exc), write_paused=0,
                )
            raise
        finally:
            maintenance_barrier.exit(lease)
            process_lease.close()
        return self.get(run_id)

    def _run(self, run_id: str, batch_size: int) -> None:
        task = self.get(run_id)
        migrator = _MIGRATORS[task["domain"]]
        lease: MaintenanceLease | None = None
        process_lease: _ProcessLease | None = None
        started = time.monotonic()
        try:
            if task["mode"] == "apply":
                lease, process_lease = self._run_apply(
                    run_id, task, migrator, batch_size, started,
                )
                return
            else:
                total = sum(1 for _ in migrator.inspect(self.root))
                self._set(
                    run_id, status="running", phase="只读检查历史记录", total=total,
                )
                records = (
                    record for record in migrator.inspect(self.root)
                    if record.record_key > str(task["last_key"])
                )
                iterator = lambda after: self._take_after(records, batch_size)  # noqa: E731
            batch = int(task["last_batch"])
            after = str(task["last_key"])
            while True:
                if self._pause_requested(run_id):
                    self._set(run_id, status="paused", phase="已安全暂停", write_paused=0)
                    return
                values = list(iterator(after))
                if not values:
                    break
                batch += 1
                self._record_batch(run_id, task["domain"], batch, values, started)
                after = values[-1].record_key
                if len(values) < batch_size:
                    break
            finished = utc_now()
            self._set(
                run_id, status="completed", phase="迁移完成" if task["mode"] == "apply" else "检查完成",
                write_paused=0, estimated_remaining_seconds=0, finished_at=finished,
            )
        except (OSError, RuntimeError, sqlite3.Error, ValueError) as exc:
            failure = {
                "status": "failed", "phase": "迁移失败，可从最近批次续跑",
                "diagnostic_code": "migration_failed", "error": str(exc),
                "write_paused": 0,
            }
            if lease is None:
                self._set(run_id, **failure)
            else:
                with maintenance_barrier.authorize(lease):
                    self._set(run_id, **failure)
        finally:
            with self._lock:
                self._pause_requests.discard(run_id)
            if lease is not None:
                try:
                    maintenance_barrier.exit(lease)
                finally:
                    with self._lock:
                        self._leases.pop(run_id, None)
            if process_lease is not None:
                process_lease.close()

    def _run_apply(
        self, run_id: str, task: dict, migrator: DomainMigrator,
        batch_size: int, started: float,
    ) -> tuple[MaintenanceLease, _ProcessLease]:
        self._require_offline_evidence()
        process_lease = self._process_lease()
        lease = maintenance_barrier.enter(f"legacy_migration:{task['domain']}", timeout=30)
        try:
            with maintenance_barrier.authorize(lease):
                with self._lock:
                    self._leases[run_id] = lease
                backup = (
                    Path(task["backup_path"])
                    if task["backup_path"] else self.backup_root / run_id
                )
                if backup.is_dir():
                    validate_backup_tree(backup)
                else:
                    self._set(
                        run_id, status="backing_up", phase="创建可恢复备份",
                        write_paused=1, backup_path=str(backup),
                    )
                    self._backup(self.root, backup)
                    validate_backup_tree(backup)
                total = int(task["total"] or 0) or sum(1 for _ in migrator.inspect(self.root))
                self._set(run_id, status="running", phase="分批转换历史记录", total=total)
                self._apply_batches(run_id, task, migrator, batch_size, started)
        except (OSError, RuntimeError, sqlite3.Error, ValueError):
            maintenance_barrier.exit(lease)
            process_lease.close()
            with self._lock:
                self._leases.pop(run_id, None)
            raise
        return lease, process_lease

    def _apply_batches(
        self, run_id: str, task: dict, migrator: DomainMigrator, batch_size: int,
        started: float,
    ) -> None:
        batch = int(task["last_batch"])
        after = str(task["last_key"])
        while True:
            if self._pause_requested(run_id):
                self._set(run_id, status="paused", phase="已安全暂停", write_paused=0)
                return
            values = list(migrator.migrate_batch(
                self.root, after_key=after, limit=batch_size,
            ))
            if not values:
                break
            batch += 1
            self._record_batch(run_id, task["domain"], batch, values, started)
            after = values[-1].record_key
            if len(values) < batch_size:
                break
        self._set(
            run_id, status="completed", phase="迁移完成", write_paused=0,
            estimated_remaining_seconds=0, finished_at=utc_now(),
        )

    @staticmethod
    def _take_after(records: Iterable[MigrationRecord], limit: int) -> list[MigrationRecord]:
        result: list[MigrationRecord] = []
        for record in records:
            result.append(record)
            if len(result) >= limit:
                break
        return result

    def _record_batch(
        self, run_id: str, domain: str, batch: int,
        records: list[MigrationRecord], started: float,
    ) -> None:
        valid = {"converted", "blank", "review", "conflict", "unchanged"}
        if any(record.outcome not in valid for record in records):
            raise LegacyMigrationError("domain migrator 返回未知 outcome")
        now = utc_now()
        with self._conn() as connection:
            for record in records:
                connection.execute(
                    "INSERT OR REPLACE INTO migration_audit "
                    "(run_id,domain,batch,record_key,outcome,diagnostic_code,"
                    "unknown_fields_json,detail,recorded_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        run_id, domain, batch, record.record_key, record.outcome,
                        record.diagnostic_code,
                        json.dumps(record.unknown_fields, ensure_ascii=False),
                        record.detail, now,
                    ),
                )
            totals = connection.execute(
                "SELECT COUNT(*),"
                "SUM(outcome='converted'),SUM(outcome='blank'),SUM(outcome='review'),"
                "SUM(outcome='conflict') FROM migration_audit WHERE run_id=?",
                (run_id,),
            ).fetchone()
            checked = int(totals[0] or 0)
            total_row = connection.execute(
                "SELECT total FROM migration_runs WHERE id=?", (run_id,),
            ).fetchone()
            total = int(total_row[0] or 0)
            elapsed = max(0.001, time.monotonic() - started)
            estimated = round(elapsed / checked * max(0, total - checked)) if checked else None
            connection.execute(
                "UPDATE migration_runs SET checked=?,converted=?,blank=?,review=?,conflicts=?,"
                "last_key=?,last_batch=?,updated_at=?,estimated_remaining_seconds=? WHERE id=?",
                (
                    checked, int(totals[1] or 0), int(totals[2] or 0),
                    int(totals[3] or 0), int(totals[4] or 0), records[-1].record_key,
                    batch, now, estimated, run_id,
                ),
            )

    def _pause_requested(self, run_id: str) -> bool:
        with self._lock:
            return run_id in self._pause_requests

    def _set(self, run_id: str, **values: Any) -> None:
        if not values:
            return
        allowed = {
            "status", "phase", "total", "write_paused", "estimated_remaining_seconds",
            "backup_path", "diagnostic_code", "error", "finished_at",
        }
        if not set(values) <= allowed:
            raise ValueError("未知 migration_runs 字段")
        values["updated_at"] = utc_now()
        assignments = ",".join(f"{name}=?" for name in values)
        with self._conn() as connection:
            connection.execute(
                f"UPDATE migration_runs SET {assignments} WHERE id=?",
                (*values.values(), run_id),
            )

    @staticmethod
    def _backup_sqlite_files(root: Path, target: Path) -> None:
        task_domain = ""
        # target is <backup-root>/<run-id>; the manager resolves the run below.
        state = root / "legacy_contract_migrations.sqlite"
        if state.is_file():
            with connect_sqlite(state, read_only=True) as connection:
                row = connection.execute(
                    "SELECT domain FROM migration_runs WHERE id=?", (target.name,),
                ).fetchone()
                task_domain = str(row[0]) if row else ""
        migrator = _MIGRATORS.get(task_domain)
        extras = tuple(getattr(migrator, "backup_paths", ())) if migrator else ()
        backup_sqlite_tree(
            root, target, exclude={"legacy_contract_migrations.sqlite"}, extra_paths=extras,
        )

    def _require_offline_evidence(self) -> None:
        evidence = self._offline_evidence
        if (
            evidence is None
            or evidence.confirmed_root.resolve() != self.root
            or not evidence.writer_stopped
            or not evidence.evidence.strip()
        ):
            raise LegacyMigrationError(
                "apply/resume/rollback 仅允许离线维护：需精确 data root、已停写证据与跨进程 lease"
            )

    def _process_lease(self) -> _ProcessLease:
        return _ProcessLease(self.root / ".legacy-contract-maintenance.lock")


legacy_migration_manager = LegacyMigrationManager()
