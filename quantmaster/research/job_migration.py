"""One-shot migration of Research Lake lifecycle rows to UnifiedJobStore."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from contextlib import closing
from pathlib import Path
from typing import Any

from quantmaster.data.legacy_migration import MigrationRecord
from quantmaster.research.catalog import RESEARCH_SCHEMA_VERSION, ResearchCatalog
from quantmaster.research.contracts import ExecutionPlan
from quantmaster.research.jobs import (
    RESEARCH_CHECKPOINT,
    RESEARCH_RESULT_KIND,
    RESEARCH_TASK_TYPE,
)
from quantmaster.runtime.jobs import UnifiedJobStore
from quantmaster.runtime.sqlite import connect_sqlite

_CATALOG = Path("research_lake") / "_meta" / "catalog.sqlite"
_DOMAIN_TABLES = {
    "research_specs",
    "research_partitions",
    "research_runs",
    "research_leases",
    "research_partition_intents",
    "research_capabilities",
}
_LEGACY_TABLES = {"research_jobs", "research_job_events"}
_JOB_COLUMNS = {
    "id",
    "status",
    "mode",
    "plan_json",
    "next_index",
    "total",
    "succeeded",
    "failed",
    "cancel_requested",
    "current_task",
    "failures_json",
    "manifest_json",
    "created_at",
    "updated_at",
    "owner",
    "lease_expires",
    "heartbeat_at",
    "attempt",
    "task_indexes_json",
}
_EVENT_COLUMNS = {"seq", "job_id", "attempt", "event_json", "created_at"}
_STATUSES = {
    "queued",
    "running",
    "cancelling",
    "interrupted",
    "cancelled",
    "completed",
    "completed_with_errors",
    "failed",
}


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


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


def _validate_partition_links(
    connection: sqlite3.Connection,
    job_id: str,
    manifest: dict[str, Any],
) -> set[str]:
    conflicts: set[str] = set()
    for field in ("input_partitions", "output_partitions"):
        values = manifest.get(field) or []
        if not isinstance(values, list):
            conflicts.add(f"{job_id}:{field}")
            continue
        for index, value in enumerate(values):
            if not isinstance(value, dict) or not str(value.get("partition_key") or ""):
                conflicts.add(f"{job_id}:{field}:{index}:partition_key")
                continue
            exists = connection.execute(
                "SELECT 1 FROM research_partitions WHERE partition_key=?",
                (str(value["partition_key"]),),
            ).fetchone()
            if exists is None:
                conflicts.add(f"{job_id}:{field}:{index}:dangling")
    return conflicts


def _job_content_conflicts(connection: sqlite3.Connection, row: sqlite3.Row) -> set[str]:
    job_id = str(row["id"])
    status = str(row["status"])
    if status not in _STATUSES:
        return {f"status:{status}"}
    try:
        plan = _json_object(row["plan_json"], "plan_json")
        ExecutionPlan.from_dict(plan)
        failures = _json_list(row["failures_json"], "failures_json")
        manifest = _json_object(row["manifest_json"], "manifest_json")
        task_indexes = _json_list(row["task_indexes_json"], "task_indexes_json")
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return {f"{job_id}:json"}
    tasks = list(plan.get("tasks") or ())
    conflicts = set()
    if (
        any(not isinstance(item, dict) for item in failures)
        or any(not isinstance(item, int) or item < 0 or item >= len(tasks) for item in task_indexes)
        or int(row["next_index"]) < 0
        or int(row["next_index"]) > len(task_indexes)
        or int(row["total"]) != len(task_indexes)
    ):
        conflicts.add(f"{job_id}:progress")
    conflicts.update(_validate_partition_links(connection, job_id, manifest))
    if status in {"completed", "completed_with_errors"}:
        run = connection.execute(
            "SELECT manifest_json FROM research_runs WHERE run_id=?", (job_id,),
        ).fetchone()
        if run is None:
            conflicts.add(f"{job_id}:run_manifest")
        else:
            try:
                published = _json_object(run["manifest_json"], "run_manifest")
            except (TypeError, ValueError, json.JSONDecodeError):
                conflicts.add(f"{job_id}:run_manifest")
            else:
                if str(published.get("plan_hash") or "") != str(plan.get("plan_hash") or ""):
                    conflicts.add(f"{job_id}:plan_hash")
    return conflicts


def _event_content_conflicts(row: sqlite3.Row) -> set[str]:
    try:
        payload = _json_object(row["event_json"], "event_json")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {f"{row['job_id']}:event_json"}
    return set() if str(payload.get("type") or "") else {f"{row['job_id']}:event_type"}


def _content_conflicts(connection: sqlite3.Connection) -> tuple[str, ...]:
    conflicts: set[str] = set()
    for row in connection.execute("SELECT * FROM research_jobs ORDER BY created_at,id"):
        conflicts.update(_job_content_conflicts(connection, row))
    for row in connection.execute("SELECT job_id,event_json FROM research_job_events"):
        conflicts.update(_event_content_conflicts(row))
    return tuple(sorted(conflicts))


def _probe(path: Path) -> tuple[str, tuple[str, ...]]:
    if not path.is_file():
        return "absent", ()
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        tables = _tables(connection)
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if not (_LEGACY_TABLES & tables):
            unknown = (tables - _DOMAIN_TABLES - {"sqlite_sequence"}) | (
                _DOMAIN_TABLES - tables
            )
            if version == RESEARCH_SCHEMA_VERSION and not unknown:
                return "retired", ()
            return "conflict", tuple(sorted(unknown | {f"user_version:{version}"}))
        unknown_tables = tables - _DOMAIN_TABLES - _LEGACY_TABLES - {"sqlite_sequence"}
        missing_tables = (_DOMAIN_TABLES | _LEGACY_TABLES) - tables
        unknown_columns = (_columns(connection, "research_jobs") - _JOB_COLUMNS) | (
            _columns(connection, "research_job_events") - _EVENT_COLUMNS
        )
        missing_columns = (_JOB_COLUMNS - _columns(connection, "research_jobs")) | (
            _EVENT_COLUMNS - _columns(connection, "research_job_events")
        )
        schema_conflicts = (
            unknown_tables | missing_tables | unknown_columns | missing_columns
            | ({f"user_version:{version}"} if version != 1 else set())
        )
        content = _content_conflicts(connection) if not schema_conflicts else ()
    evidence = tuple(sorted(schema_conflicts | set(content)))
    return ("conflict", evidence) if evidence else ("upgrade", ())


def _record(status: str, unknown: tuple[str, ...] = ()) -> MigrationRecord:
    if status == "conflict":
        return MigrationRecord(
            "research-lake",
            "conflict",
            "research_job_schema_unclassified",
            unknown,
            "Research Lake 含未知 lifecycle 或悬空 provenance，拒绝写入",
        )
    return MigrationRecord(
        "research-lake",
        "review" if status == "upgrade" else "converted",
        (
            "research_job_lifecycle_migration_required"
            if status == "upgrade"
            else "research_job_migrated"
        ),
        (),
        f"Research Lake lifecycle {'需要迁移' if status == 'upgrade' else '已迁移'}",
    )


def _events(connection: sqlite3.Connection, job_id: str) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    rows = connection.execute(
        "SELECT attempt,event_json,created_at FROM research_job_events "
        "WHERE job_id=? ORDER BY seq",
        (job_id,),
    ).fetchall()
    for offset, row in enumerate(rows, start=1):
        payload = _json_object(row["event_json"], "event_json")
        legacy_type = str(payload.pop("type"))
        values.append({
            "seq": offset,
            "attempt": max(1, int(row["attempt"] or 1)),
            "type": "job_started" if legacy_type == "claimed" else f"legacy_research_{legacy_type}",
            "payload": {"legacy_type": legacy_type, **payload},
            "created_at": row["created_at"],
        })
    return values


def _convert(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    status = str(row["status"])
    statuses = {
        "queued": "queued",
        "running": "interrupted",
        "cancelling": "interrupted",
        "interrupted": "interrupted",
        "cancelled": "cancelled",
        "completed": "completed",
        "completed_with_errors": "completed",
        "failed": "failed",
    }
    if status not in statuses:
        raise ValueError(f"未知 research status: {status}")
    plan = _json_object(row["plan_json"], "plan_json")
    failures = [dict(item) for item in _json_list(row["failures_json"], "failures_json")]
    manifest = _json_object(row["manifest_json"], "manifest_json")
    task_indexes = [int(item) for item in _json_list(
        row["task_indexes_json"], "task_indexes_json",
    )]
    outcome = "completed_with_warnings" if status == "completed_with_errors" else (
        "completed" if status == "completed" else ""
    )
    state = {
        "schema_version": "1.0",
        "task_indexes": task_indexes,
        "next_index": int(row["next_index"]),
        "total": int(row["total"]),
        "succeeded": int(row["succeeded"]),
        "failed": int(row["failed"]),
        "failures": failures,
        "current_task": str(row["current_task"] or ""),
        "manifest": manifest,
        "outcome": outcome,
    }
    attempt = max(1, int(row["attempt"] or 1))
    artifacts: list[dict[str, Any]] = [{
        "kind": f"checkpoint.{RESEARCH_CHECKPOINT}",
        "checkpoint_key": RESEARCH_CHECKPOINT,
        "payload": state,
        "attempt": attempt,
        "created_at": row["updated_at"],
    }]
    if status in {"completed", "completed_with_errors"}:
        artifacts.append({
            "kind": RESEARCH_RESULT_KIND,
            "result": True,
            "payload": state,
            "attempt": attempt,
            "created_at": row["updated_at"],
        })
    record = {
        "id": str(row["id"]),
        "type": RESEARCH_TASK_TYPE,
        "spec": {"mode": str(row["mode"]), "plan": plan},
        "algorithm_version": "research-lake-v2",
        "status": statuses[status],
        "progress": round(100 * int(row["next_index"]) / max(1, int(row["total"]))),
        "phase": "等待恢复" if status in {"running", "cancelling"} else "",
        "detail": "从旧 Research Lake lifecycle 迁移",
        "attempt": attempt,
        "max_attempts": 8,
        "cancel_requested": bool(row["cancel_requested"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "finished_at": (
            row["updated_at"] if statuses[status] in {"completed", "cancelled"} else ""
        ),
        "deadline_seconds": 3600,
    }
    return record, _events(connection, str(row["id"])), artifacts


def _migrate(path: Path, store: UnifiedJobStore) -> None:
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        converted = [
            _convert(connection, row)
            for row in connection.execute(
                "SELECT * FROM research_jobs ORDER BY created_at,id"
            ).fetchall()
        ]
    for record, events, artifacts in converted:
        store.import_legacy_job(record, events=events, artifacts=artifacts)
    for record, _events_value, _artifacts in converted:
        store.get(str(record["id"]))
    with closing(connect_sqlite(path, row_factory=True)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("DROP TABLE research_job_events")
        connection.execute("DROP TABLE research_jobs")
        connection.execute(f"PRAGMA user_version={RESEARCH_SCHEMA_VERSION}")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    ResearchCatalog(path, read_only=True)


class ResearchJobLegacyMigrator:
    name = "research-jobs"
    backup_paths = (_CATALOG.as_posix(), "jobs.sqlite")

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        status, unknown = _probe(root / _CATALOG)
        if status in {"absent", "retired"}:
            return ()
        return (_record(status, unknown),)

    def migrate_batch(
        self,
        root: Path,
        *,
        after_key: str,
        limit: int,
    ) -> Iterable[MigrationRecord]:
        if after_key >= "research-lake" or int(limit) < 1:
            return ()
        path = root / _CATALOG
        status, unknown = _probe(path)
        if status in {"absent", "retired"}:
            return ()
        if status == "conflict":
            return (_record(status, unknown),)
        _migrate(path, UnifiedJobStore(root / "jobs.sqlite"))
        return (_record("converted"),)

    def rollback(self, root: Path, backup_root: Path) -> None:
        from quantmaster.data.migration import restore_backup_path

        for relative in self.backup_paths:
            restore_backup_path(root, backup_root, relative)


research_job_legacy_migrator = ResearchJobLegacyMigrator()
