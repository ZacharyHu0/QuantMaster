"""One-shot migration of data-domain lifecycle ledgers into UnifiedJobStore."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from contextlib import closing
from pathlib import Path
from typing import Any

from quantmaster.data.legacy_migration import MigrationRecord
from quantmaster.data.maintenance import (
    DATA_REFRESH_TASK_TYPE,
    REFRESH_CHECKPOINT,
    REFRESH_RESULT_KIND,
)
from quantmaster.data.repair import (
    DATA_REPAIR_TASK_TYPE,
    REPAIR_FAILURE_CHECKPOINT,
    REPAIR_RESULT_KIND,
    _idempotency_key,
)
from quantmaster.runtime.jobs import UnifiedJobStore
from quantmaster.runtime.sqlite import connect_sqlite

_REFRESH_TABLES = {"refresh_jobs", "refresh_failures", "refresh_events", "sqlite_sequence"}
_REFRESH_REQUIRED = {
    "id", "status", "scope", "universe_name", "start_date", "end_date",
    "symbols_json", "next_index", "total", "succeeded", "failed", "failures_json",
    "current_symbol", "cancel_requested", "created_at", "updated_at", "attempt",
    "original_symbols_json",
}
_REFRESH_OPTIONAL = {"owner", "lease_expires", "heartbeat_at"}
_REPAIR_TABLES = {"data_repairs", "data_repair_events", "data_repair_budget", "sqlite_sequence"}
_REPAIR_REQUIRED = {
    "id", "kind", "target", "idempotency_key", "source", "status", "reason",
    "spec_json", "attempt", "max_attempts", "next_run", "cancel_requested", "owner",
    "lease_expires", "last_error", "result_json", "created_at", "updated_at",
    "completed_at",
}


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0]) for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _content_conflicts(connection: sqlite3.Connection, domain: str) -> tuple[str, ...]:
    conflicts: set[str] = set()
    if domain == "refresh":
        statuses = {
            "queued", "running", "cancelling", "interrupted", "cancelled",
            "completed", "completed_with_errors",
        }
        rows = connection.execute(
            "SELECT id,status,symbols_json,original_symbols_json,failures_json FROM refresh_jobs"
        )
        json_fields = ("symbols_json", "original_symbols_json", "failures_json")
        expected = list
    else:
        statuses = {"queued", "running", "cancelling", "failed", "quarantined", "cancelled", "completed"}
        rows = connection.execute(
            "SELECT id,status,spec_json,result_json FROM data_repairs"
        )
        json_fields = ("spec_json", "result_json")
        expected = dict
    for row in rows:
        values = dict(row)
        if str(values["status"]) not in statuses:
            conflicts.add(f"status:{values['status']}")
        for field in json_fields:
            try:
                decoded = json.loads(str(values[field]))
            except (json.JSONDecodeError, TypeError):
                decoded = None
            if not isinstance(decoded, expected):
                conflicts.add(f"{values['id']}:{field}")
    return tuple(sorted(conflicts))


def _probe(path: Path, domain: str) -> tuple[str, tuple[str, ...]]:
    if not path.is_file():
        return "absent", ()
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        tables = _tables(connection)
        if domain == "refresh":
            if "refresh_jobs" not in tables:
                return ("retired", ()) if not (tables - {"sqlite_sequence"}) else (
                    "conflict", tuple(sorted(tables - {"sqlite_sequence"})),
                )
            unknown_tables = tables - _REFRESH_TABLES
            columns = _columns(connection, "refresh_jobs")
            unknown = unknown_tables | (columns - _REFRESH_REQUIRED - _REFRESH_OPTIONAL)
            missing = _REFRESH_REQUIRED - columns
        else:
            if "data_repairs" not in tables:
                return ("retired", ()) if not (tables - {"sqlite_sequence"}) else (
                    "conflict", tuple(sorted(tables - {"sqlite_sequence"})),
                )
            unknown_tables = tables - _REPAIR_TABLES
            columns = _columns(connection, "data_repairs")
            unknown = unknown_tables | (columns - _REPAIR_REQUIRED)
            missing = _REPAIR_REQUIRED - columns
        content = _content_conflicts(connection, domain) if not unknown and not missing else ()
    evidence = tuple(sorted(unknown | missing | set(content)))
    return ("conflict", evidence) if evidence else ("upgrade", ())


def _record(key: str, status: str, unknown: tuple[str, ...] = ()) -> MigrationRecord:
    if status == "conflict":
        return MigrationRecord(
            key, "conflict", "data_job_schema_unclassified", unknown,
            f"{key} 含未知 lifecycle schema，拒绝写入",
        )
    return MigrationRecord(
        key, "review" if status == "upgrade" else "converted",
        "data_job_lifecycle_migration_required" if status == "upgrade" else "data_job_migrated",
        (), f"{key} lifecycle {'需要迁移' if status == 'upgrade' else '已迁移'}",
    )


def _json_object(raw: Any, field: str) -> dict[str, Any]:
    value = json.loads(str(raw or "{}"))
    if not isinstance(value, dict):
        raise ValueError(f"{field} 不是 JSON 对象")
    return value


def _json_list(raw: Any, field: str) -> list[Any]:
    value = json.loads(str(raw or "[]"))
    if not isinstance(value, list):
        raise ValueError(f"{field} 不是 JSON 数组")
    return value


def _legacy_events(
    rows: Iterable[sqlite3.Row], *, prefix: str, claimed_as_started: bool = False,
) -> list[dict[str, Any]]:
    events = []
    for offset, row in enumerate(rows, start=1):
        payload = _json_object(row["event_json"], "event_json")
        legacy_type = str(payload.pop("type", "event"))
        event_type = "job_started" if claimed_as_started and legacy_type == "claimed" else (
            f"legacy_{prefix}_{legacy_type}"
        )
        events.append({
            "seq": offset,
            "attempt": max(1, int(row["attempt"] or 1)),
            "type": event_type,
            "payload": {"legacy_type": legacy_type, **payload},
            "created_at": row["created_at"],
        })
    return events


def _refresh_failures(connection: sqlite3.Connection, row: sqlite3.Row) -> list[dict[str, str]]:
    failures = connection.execute(
        "SELECT symbol,error FROM refresh_failures WHERE job_id=? AND attempt=? ORDER BY id",
        (row["id"], int(row["attempt"] or 1)),
    ).fetchall()
    if failures:
        return [{"symbol": str(item[0]), "error": str(item[1])} for item in failures]
    return [dict(item) for item in _json_list(row["failures_json"], "failures_json")]


def _refresh_record(
    connection: sqlite3.Connection, row: sqlite3.Row,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    status = str(row["status"])
    statuses = {
        "queued": "queued", "running": "interrupted", "cancelling": "interrupted",
        "interrupted": "interrupted", "cancelled": "cancelled",
        "completed": "completed", "completed_with_errors": "completed",
    }
    if status not in statuses:
        raise ValueError(f"未知 refresh status: {status}")
    original = [str(value) for value in _json_list(
        row["original_symbols_json"], "original_symbols_json",
    )]
    symbols = [str(value) for value in _json_list(row["symbols_json"], "symbols_json")]
    failures = _refresh_failures(connection, row)
    state = {
        "schema_version": "1.0", "original_symbols": original or symbols,
        "symbols": symbols, "next_index": int(row["next_index"]),
        "succeeded": int(row["succeeded"]), "failures": failures,
        "current_symbol": str(row["current_symbol"] or ""),
    }
    artifacts = [{
        "kind": f"checkpoint.{REFRESH_CHECKPOINT}", "checkpoint_key": REFRESH_CHECKPOINT,
        "payload": state, "attempt": row["attempt"], "created_at": row["updated_at"],
    }]
    if status in {"completed", "completed_with_errors"}:
        outcome = "completed_with_warnings" if failures else "completed"
        artifacts.append({
            "kind": REFRESH_RESULT_KIND, "result": True,
            "payload": {
                **state, "outcome": outcome, "total": int(row["total"]),
                "failed": len(failures),
            },
            "attempt": row["attempt"], "created_at": row["updated_at"],
        })
    events = _legacy_events(connection.execute(
        "SELECT attempt,event_json,created_at FROM refresh_events "
        "WHERE job_id=? ORDER BY seq", (row["id"],),
    ).fetchall(), prefix="data_refresh")
    record = {
        "id": str(row["id"]), "type": DATA_REFRESH_TASK_TYPE,
        "spec": {
            "scope": str(row["scope"]), "universe": str(row["universe_name"]),
            "start": str(row["start_date"]), "end": str(row["end_date"]),
            "symbols": original or symbols,
        },
        "status": statuses[status],
        "progress": round(100 * int(row["next_index"]) / max(1, int(row["total"]))),
        "phase": "等待恢复" if status in {"running", "cancelling"} else "",
        "detail": "从旧数据刷新 lifecycle 迁移",
        "attempt": max(1, int(row["attempt"] or 1)), "max_attempts": 8,
        "cancel_requested": bool(row["cancel_requested"]),
        "created_at": row["created_at"], "updated_at": row["updated_at"],
        "finished_at": row["updated_at"] if statuses[status] in {"completed", "cancelled"} else "",
        "deadline_seconds": 3600,
    }
    return record, events, artifacts


def _repair_record(
    connection: sqlite3.Connection, row: sqlite3.Row,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    status = str(row["status"])
    statuses = {
        "queued": "queued", "running": "interrupted", "cancelling": "interrupted",
        "failed": "failed", "quarantined": "completed", "cancelled": "cancelled",
        "completed": "completed",
    }
    if status not in statuses:
        raise ValueError(f"未知 repair status: {status}")
    spec = {
        "kind": str(row["kind"]), "target": str(row["target"]),
        "source": str(row["source"]), "reason": str(row["reason"]),
        "repair_spec": _json_object(row["spec_json"], "spec_json"),
    }
    artifacts: list[dict[str, Any]] = []
    if row["last_error"]:
        artifacts.append({
            "kind": f"checkpoint.{REPAIR_FAILURE_CHECKPOINT}",
            "checkpoint_key": REPAIR_FAILURE_CHECKPOINT,
            "payload": {"schema_version": "1.0", "error": str(row["last_error"])},
            "attempt": max(1, int(row["attempt"] or 1)), "created_at": row["updated_at"],
        })
    if status in {"completed", "quarantined"}:
        artifacts.append({
            "kind": REPAIR_RESULT_KIND, "result": True,
            "payload": {
                "schema_version": "1.0",
                "outcome": "quarantined" if status == "quarantined" else "completed",
                "result": _json_object(row["result_json"], "result_json"),
            },
            "attempt": max(1, int(row["attempt"] or 1)), "created_at": row["updated_at"],
        })
    events = _legacy_events(connection.execute(
        "SELECT attempt,event_json,created_at FROM data_repair_events "
        "WHERE repair_id=? ORDER BY seq", (row["id"],),
    ).fetchall(), prefix="data_repair", claimed_as_started=True)
    record = {
        "id": str(row["id"]), "type": DATA_REPAIR_TASK_TYPE, "spec": spec,
        "business_key": f"repair:{_idempotency_key(str(row['kind']), str(row['target']))}",
        "status": statuses[status], "phase": "等待恢复" if status == "running" else "",
        "detail": str(row["last_error"] or ""),
        "attempt": max(1, int(row["attempt"] or 1)),
        "max_attempts": max(1, int(row["max_attempts"])),
        "next_retry_at": float(row["next_run"] or 0),
        "cancel_requested": bool(row["cancel_requested"]),
        "created_at": row["created_at"], "updated_at": row["updated_at"],
        "finished_at": row["completed_at"] if statuses[status] in {"completed", "cancelled"} else "",
        "deadline_seconds": 600,
    }
    return record, events, artifacts


def _migrate(path: Path, store: UnifiedJobStore, domain: str) -> None:
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        table = "refresh_jobs" if domain == "refresh" else "data_repairs"
        rows = connection.execute(f"SELECT * FROM {table} ORDER BY created_at,id").fetchall()
        converted = [
            (_refresh_record(connection, row) if domain == "refresh" else _repair_record(connection, row))
            for row in rows
        ]
    for record, events, artifacts in converted:
        store.import_legacy_job(record, events=events, artifacts=artifacts)
    for record, _events, _artifacts in converted:
        store.get(str(record["id"]))
    with closing(connect_sqlite(path)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        tables = (
            ("refresh_events", "refresh_failures", "refresh_jobs")
            if domain == "refresh"
            else ("data_repair_events", "data_repair_budget", "data_repairs")
        )
        for table in tables:
            connection.execute(f"DROP TABLE IF EXISTS {table}")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    for suffix in ("-wal", "-shm", "-journal"):
        path.with_name(path.name + suffix).unlink(missing_ok=True)
    path.unlink()


class DataJobLegacyMigrator:
    name = "data-jobs"
    backup_paths = ("data_refresh.sqlite", "data_repairs.sqlite", "jobs.sqlite")
    _targets = (("data-refresh", "data_refresh.sqlite", "refresh"),
                ("data-repair", "data_repairs.sqlite", "repair"))

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        records = []
        for key, filename, domain in self._targets:
            status, unknown = _probe(root / filename, domain)
            if status not in {"absent", "retired"}:
                records.append(_record(key, status, unknown))
        return tuple(records)

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        records = []
        for key, filename, domain in self._targets:
            if key <= after_key:
                continue
            path = root / filename
            status, unknown = _probe(path, domain)
            if status in {"absent", "retired"}:
                continue
            if status == "conflict":
                records.append(_record(key, status, unknown))
            else:
                _migrate(path, UnifiedJobStore(root / "jobs.sqlite"), domain)
                records.append(_record(key, "converted"))
            if len(records) >= max(1, int(limit)):
                break
        return tuple(records)

    def rollback(self, root: Path, backup_root: Path) -> None:
        from quantmaster.data.migration import restore_backup_path

        for relative in self.backup_paths:
            restore_backup_path(root, backup_root, relative)


data_job_legacy_migrator = DataJobLegacyMigrator()
