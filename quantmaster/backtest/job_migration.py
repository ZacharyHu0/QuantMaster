"""One-shot migration of backtest lifecycle rows to UnifiedJobStore."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from collections.abc import Iterable
from contextlib import closing
from pathlib import Path
from typing import Any

from quantmaster.backtest.jobs import (
    BACKTEST_ALGORITHM_VERSION,
    BACKTEST_RESULT_KIND,
    BACKTEST_TASK_TYPE,
)
from quantmaster.backtest.spec import BacktestSpec, canonical_json
from quantmaster.backtest.workbench import BACKTEST_SCHEMA_VERSION, BacktestStore
from quantmaster.data.legacy_migration import MigrationRecord
from quantmaster.runtime.jobs import UnifiedJobStore
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.sqlite import connect_sqlite

_DATABASE = Path("backtests.sqlite")
_ARTIFACT_ROOT = Path("backtests")
_LEGACY_CORE = {"backtest_runs", "backtest_events"}
_LEGACY_TABLES = _LEGACY_CORE | {"backtest_store_meta"}
_CURRENT_TABLES = {"backtest_results", "backtest_store_meta"}
_RUN_COLUMNS = {
    "id", "name", "status", "config_json", "config_hash", "manifest_json",
    "result_json", "artifact_path", "progress", "phase", "detail", "error",
    "cancel_requested", "worker", "created_at", "started_at", "heartbeat_at",
    "finished_at",
}
_EVENT_COLUMNS = {"seq", "run_id", "event_json", "created_at"}
_RESULT_COLUMNS = {
    "job_id", "attempt", "name", "spec_json", "spec_hash", "outcome",
    "manifest_json", "summary_json", "diagnostic_json", "artifact_path",
    "content_hash", "created_at",
}
_STATUSES = {
    "queued", "running", "interrupted", "cancelled", "completed", "failed",
    "needs_confirmation",
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


def _artifact(root: Path, raw: Any) -> tuple[Path, dict[str, Any]]:
    value = str(raw or "")
    if not value:
        raise FileNotFoundError("artifact_path")
    candidate = Path(value)
    boundary = root.resolve()
    candidates = (
        (candidate.resolve(),)
        if candidate.is_absolute()
        else ((root / candidate).resolve(), (root.parent / candidate).resolve())
    )
    resolved = next(
        (
            path for path in candidates
            if path.is_relative_to(boundary) and path.is_file()
        ),
        None,
    )
    if resolved is None:
        raise FileNotFoundError(value)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("artifact 不是 JSON 对象")
    return resolved, payload


def _provenance_conflicts(
    job_id: str,
    config_hash: str,
    manifest: dict[str, Any],
    artifact: dict[str, Any],
) -> set[str]:
    conflicts: set[str] = set()
    if str(manifest.get("config_hash") or "") != config_hash:
        conflicts.add(f"{job_id}:config_hash")
    if not isinstance(manifest.get("data_quality"), dict) or not manifest["data_quality"]:
        conflicts.add(f"{job_id}:data_quality")
    if not isinstance(manifest.get("strategy_snapshot"), dict) or not manifest["strategy_snapshot"]:
        conflicts.add(f"{job_id}:strategy_snapshot")
    published = artifact.get("manifest")
    if not isinstance(published, dict) or published != manifest:
        conflicts.add(f"{job_id}:artifact_manifest")
    if not isinstance(artifact.get("metrics"), dict):
        conflicts.add(f"{job_id}:metrics")
    if not isinstance(artifact.get("trades"), list):
        conflicts.add(f"{job_id}:trades")
    return conflicts


def _content_conflicts(root: Path, connection: sqlite3.Connection) -> tuple[str, ...]:
    conflicts: set[str] = set()
    rows = connection.execute("SELECT * FROM backtest_runs ORDER BY created_at,id").fetchall()
    jobs = {str(row["id"]): row for row in rows}
    for job_id, row in jobs.items():
        status = str(row["status"])
        if status not in _STATUSES:
            conflicts.add(f"status:{status}")
            continue
        try:
            config = _json_object(row["config_json"], "config_json")
            manifest = _json_object(row["manifest_json"], "manifest_json")
            result = _json_object(row["result_json"], "result_json")
        except (TypeError, ValueError, json.JSONDecodeError):
            conflicts.add(f"{job_id}:json")
            continue
        strategy_kind = str((config.get("strategy") or {}).get("kind") or "")
        if strategy_kind != "swing":
            try:
                validated = BacktestSpec.model_validate(config)
            except (TypeError, ValueError):
                conflicts.add(f"{job_id}:spec")
            else:
                if str(row["config_hash"] or "") != validated.snapshot_hash:
                    conflicts.add(f"{job_id}:spec_hash")
        elif not config.get("universe") or not config.get("start"):
            conflicts.add(f"{job_id}:legacy_spec")
        if status == "running" and not all(
            str(row[field] or "") for field in ("worker", "started_at", "heartbeat_at")
        ):
            conflicts.add(f"{job_id}:lease_evidence")
        if status != "running" and str(row["worker"] or ""):
            conflicts.add(f"{job_id}:orphan_worker")
        if bool(row["cancel_requested"]) and status not in {
            "running", "interrupted", "cancelled",
        }:
            conflicts.add(f"{job_id}:cancel_status")
        if status == "completed":
            if not manifest or not result:
                conflicts.add(f"{job_id}:result")
                continue
            try:
                _path, artifact = _artifact(root / _ARTIFACT_ROOT, row["artifact_path"])
            except (FileNotFoundError, OSError, TypeError, ValueError, json.JSONDecodeError):
                conflicts.add(f"{job_id}:artifact")
            else:
                conflicts.update(_provenance_conflicts(
                    job_id, str(row["config_hash"]), manifest, artifact,
                ))
        if status in {"failed", "needs_confirmation"} and not result and not str(row["error"]):
            conflicts.add(f"{job_id}:failure_result")
    for row in connection.execute("SELECT run_id,event_json FROM backtest_events"):
        job_id = str(row["run_id"])
        if job_id not in jobs:
            conflicts.add(f"event:{job_id}:dangling_job")
        try:
            payload = _json_object(row["event_json"], "event_json")
        except (TypeError, ValueError, json.JSONDecodeError):
            conflicts.add(f"{job_id}:event_json")
            continue
        if not str(payload.get("type") or ""):
            conflicts.add(f"{job_id}:event_type")
    return tuple(sorted(conflicts))


def _target_conflicts(root: Path, rows: Iterable[sqlite3.Row]) -> tuple[str, ...]:
    path = root / "jobs.sqlite"
    if not path.is_file():
        return ()
    conflicts: set[str] = set()
    try:
        store = UnifiedJobStore(path, read_only=True)
        for row in rows:
            job_id = str(row["id"])
            try:
                existing = store.get(job_id)
            except KeyError:
                continue
            config = _json_object(row["config_json"], "config_json")
            expected = {
                "name": str(row["name"]),
                "config": config,
                "config_hash": str(row["config_hash"]),
            }
            if (
                str(existing.get("type") or "") != BACKTEST_TASK_TYPE
                or dict(existing.get("spec") or {}) != expected
            ):
                conflicts.add(f"{job_id}:target_collision")
    except (FileNotFoundError, sqlite3.Error, ValueError):
        conflicts.add("jobs.sqlite:unclassified")
    return tuple(sorted(conflicts))


def _probe(root: Path) -> tuple[str, tuple[str, ...]]:
    path = root / _DATABASE
    if not path.is_file():
        return "absent", ()
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        tables = _tables(connection)
        row = connection.execute(
            "SELECT value FROM backtest_store_meta WHERE key='schema_version'"
        ).fetchone() if "backtest_store_meta" in tables else None
        version = str(row[0]) if row is not None else ""
        if "backtest_results" in tables and not ({"backtest_runs", "backtest_events"} & tables):
            schema = (
                tables - _CURRENT_TABLES - {"sqlite_sequence"}
                | _CURRENT_TABLES - tables
                | _columns(connection, "backtest_results") ^ _RESULT_COLUMNS
            )
            if version != str(BACKTEST_SCHEMA_VERSION):
                schema.add(f"schema_version:{version}")
            return ("retired", ()) if not schema else ("conflict", tuple(sorted(schema)))
        schema = tables - _LEGACY_TABLES - {"sqlite_sequence"} | _LEGACY_CORE - tables
        if version not in {"", "1"}:
            schema.add(f"schema_version:{version}")
        if not schema:
            schema |= _columns(connection, "backtest_runs") ^ _RUN_COLUMNS
            schema |= _columns(connection, "backtest_events") ^ _EVENT_COLUMNS
        content = _content_conflicts(root, connection) if not schema else ()
        rows = connection.execute("SELECT * FROM backtest_runs").fetchall() if not schema else ()
    evidence = set(schema) | set(content)
    if not evidence:
        evidence.update(_target_conflicts(root, rows))
    return ("conflict", tuple(sorted(evidence))) if evidence else ("upgrade", ())


def _record(status: str, unknown: tuple[str, ...] = ()) -> MigrationRecord:
    if status == "conflict":
        return MigrationRecord(
            "backtests",
            "conflict",
            "backtest_job_schema_unclassified",
            unknown,
            "回测账本含未知 lifecycle、缺失 provenance、悬空 artifact 或目标冲突，拒绝写入",
        )
    return MigrationRecord(
        "backtests",
        "review" if status == "upgrade" else "converted",
        "backtest_job_lifecycle_migration_required" if status == "upgrade" else "backtest_job_migrated",
        (),
        f"回测 lifecycle {'需要迁移' if status == 'upgrade' else '已迁移'}",
    )


def _events(connection: sqlite3.Connection, job_id: str) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    rows = connection.execute(
        "SELECT event_json,created_at FROM backtest_events WHERE run_id=? ORDER BY seq",
        (job_id,),
    ).fetchall()
    for offset, row in enumerate(rows, start=1):
        payload = _json_object(row["event_json"], "event_json")
        legacy_type = str(payload.pop("type"))
        values.append({
            "seq": offset,
            "attempt": 1,
            "type": "job_started" if legacy_type == "claimed" else f"legacy_backtest_{legacy_type}",
            "payload": {"legacy_type": legacy_type, **payload},
            "created_at": row["created_at"],
        })
    return values


def _legacy_artifact(root: Path, row: sqlite3.Row) -> dict[str, Any]:
    if not str(row["artifact_path"] or ""):
        return {}
    _path, artifact = _artifact(root / _ARTIFACT_ROOT, row["artifact_path"])
    return artifact


def _convert(
    root: Path,
    connection: sqlite3.Connection,
    row: sqlite3.Row,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any] | None]:
    legacy_status = str(row["status"])
    config = _json_object(row["config_json"], "config_json")
    manifest = _json_object(row["manifest_json"], "manifest_json")
    summary = _json_object(row["result_json"], "result_json")
    swing = str((config.get("strategy") or {}).get("kind") or "") == "swing"
    statuses = {
        "queued": "queued",
        "running": "interrupted",
        "interrupted": "interrupted",
        "cancelled": "cancelled",
        "completed": "completed",
        "failed": "failed",
        "needs_confirmation": "failed",
    }
    if legacy_status not in statuses:
        raise ValueError(f"未知 backtest status: {legacy_status}")
    status = (
        "cancelled"
        if swing and legacy_status in {"queued", "running", "interrupted"}
        else statuses[legacy_status]
    )
    immutable_spec = {
        "name": str(row["name"]),
        "config": config,
        "config_hash": str(row["config_hash"]),
    }
    record = {
        "id": str(row["id"]),
        "type": BACKTEST_TASK_TYPE,
        "spec": immutable_spec,
        "algorithm_version": f"{BACKTEST_ALGORITHM_VERSION}-legacy",
        "status": status,
        "progress": int(row["progress"] or 0),
        "phase": "旧 Swing 执行器已移除" if swing and status == "cancelled" else str(row["phase"] or ""),
        "detail": str(row["detail"] or row["error"] or ""),
        "attempt": 1,
        "max_attempts": 8,
        "cancel_requested": bool(row["cancel_requested"]) or (swing and status == "cancelled"),
        "diagnostic_code": "needs_confirmation" if legacy_status == "needs_confirmation" else "",
        "created_at": row["created_at"],
        "updated_at": row["heartbeat_at"] or row["finished_at"] or row["created_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"] or (row["heartbeat_at"] if status == "cancelled" else ""),
        "deadline_seconds": 3600,
    }
    domain_result = None
    if legacy_status in {"completed", "failed", "cancelled", "needs_confirmation"} or status == "cancelled":
        problem = summary.get("problem") if isinstance(summary.get("problem"), dict) else {}
        diagnostic = {
            "code": (
                str(problem.get("code") or "needs_confirmation")
                if legacy_status == "needs_confirmation"
                else str(problem.get("code") or "backtest_execution_failed")
                if legacy_status == "failed"
                else ""
            ),
            "message": str(row["error"] or row["detail"] or ""),
        }
        domain_outcome = "needs_confirmation" if legacy_status == "needs_confirmation" else status
        if legacy_status == "completed" and manifest.get("warnings"):
            domain_outcome = "completed_with_warnings"
        domain_result = {
            "job_id": str(row["id"]),
            "attempt": 1,
            "name": str(row["name"]),
            "spec": immutable_spec,
            "outcome": domain_outcome,
            "manifest": manifest,
            "summary": summary,
            "artifact": _legacy_artifact(root, row),
            "diagnostic": diagnostic,
            "created_at": row["finished_at"] or row["heartbeat_at"] or row["created_at"],
        }
    return record, _events(connection, str(row["id"])), domain_result


def _digest(value: Any) -> str:
    return hashlib.sha256(strict_json_dumps(value, sort_keys=True).encode("utf-8")).hexdigest()


def _prepare_domain_artifact(
    artifact_root: Path,
    result: dict[str, Any],
) -> tuple[str, str]:
    envelope = {
        "schema_version": "1.0",
        "job_id": result["job_id"],
        "attempt": result["attempt"],
        "name": result["name"],
        "spec": result["spec"],
        "outcome": result["outcome"],
        "manifest": result["manifest"],
        "summary": result["summary"],
        "artifact": result["artifact"],
        "diagnostic": result["diagnostic"],
    }
    digest = _digest(envelope)
    helper = BacktestStore.__new__(BacktestStore)
    helper.artifact_root = artifact_root
    relative = helper._relative_artifact(result["job_id"], result["attempt"], digest)
    destination = artifact_root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.is_file():
        descriptor, temp_name = tempfile.mkstemp(
            prefix=".backtest-migration.", suffix=".tmp", dir=destination.parent,
        )
        temp = Path(temp_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(strict_json_dumps(result["artifact"]))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, destination)
        finally:
            temp.unlink(missing_ok=True)
    elif _digest(json.loads(destination.read_text(encoding="utf-8"))) != _digest(result["artifact"]):
        raise ValueError("迁移目标 artifact 内容冲突")
    return relative.as_posix(), digest


def _runtime_artifact(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": BACKTEST_RESULT_KIND,
        "result": True,
        "payload": {
            "schema_version": "1.0",
            "name": result["name"],
            "spec": result["spec"],
            "outcome": result["outcome"],
            "manifest": result["manifest"],
            "summary": result["summary"],
            "artifact": result["artifact"],
            "diagnostic": result["diagnostic"],
        },
        "attempt": result["attempt"],
        "created_at": result["created_at"],
    }


def _rewrite_domain(
    path: Path,
    rows: list[tuple[dict[str, Any], str, str]],
) -> None:
    with closing(connect_sqlite(path, row_factory=True)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("DROP TABLE backtest_events")
        connection.execute("DROP TABLE backtest_runs")
        connection.execute("DROP INDEX IF EXISTS idx_backtest_status")
        connection.execute("DROP INDEX IF EXISTS idx_backtest_events")
        connection.execute("DROP TABLE backtest_store_meta")
        connection.executescript("""
            CREATE TABLE backtest_results (
                job_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                name TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                spec_hash TEXT NOT NULL,
                outcome TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                diagnostic_json TEXT NOT NULL,
                artifact_path TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(job_id,attempt));
            CREATE INDEX idx_backtest_results_created
                ON backtest_results(created_at DESC,job_id,attempt);
            CREATE TABLE backtest_store_meta (
                key TEXT PRIMARY KEY,value TEXT NOT NULL);
        """)
        for result, relative, digest in rows:
            spec_json = canonical_json(result["spec"])
            connection.execute(
                "INSERT INTO backtest_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    result["job_id"], result["attempt"], result["name"], spec_json,
                    hashlib.sha256(spec_json.encode("utf-8")).hexdigest(), result["outcome"],
                    canonical_json(result["manifest"]), canonical_json(result["summary"]),
                    canonical_json(result["diagnostic"]), relative, digest,
                    result["created_at"],
                ),
            )
        connection.execute(
            "INSERT INTO backtest_store_meta(key,value) VALUES ('schema_version',?)",
            (str(BACKTEST_SCHEMA_VERSION),),
        )
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")


def _migrate(root: Path, store: UnifiedJobStore) -> None:
    path = root / _DATABASE
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        converted = [
            _convert(root, connection, row)
            for row in connection.execute(
                "SELECT * FROM backtest_runs ORDER BY created_at,id"
            ).fetchall()
        ]
    domain_rows: list[tuple[dict[str, Any], str, str]] = []
    for record, events, result in converted:
        artifacts = [_runtime_artifact(result)] if result is not None else []
        store.import_legacy_job(record, events=events, artifacts=artifacts)
        if result is not None:
            relative, digest = _prepare_domain_artifact(root / _ARTIFACT_ROOT, result)
            domain_rows.append((result, relative, digest))
    for record, _events_value, _result in converted:
        store.get(str(record["id"]))
    if len(store.list(1000, job_type=BACKTEST_TASK_TYPE)) < len(converted):
        raise RuntimeError("回测 lifecycle 导入条数不守恒")
    _rewrite_domain(path, domain_rows)
    domain = BacktestStore(path, root / _ARTIFACT_ROOT, read_only=True)
    migrated_results = sum(
        len(domain.results(str(record["id"])))
        for record, _events_value, _result in converted
    )
    if migrated_results != len(domain_rows):
        raise RuntimeError("回测领域结果迁移条数不守恒")


class BacktestJobLegacyMigrator:
    name = "backtest-jobs"
    backup_paths = (_DATABASE.as_posix(), _ARTIFACT_ROOT.as_posix(), "jobs.sqlite")

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        status, unknown = _probe(root)
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
        if after_key >= "backtests" or int(limit) < 1:
            return ()
        status, unknown = _probe(root)
        if status in {"absent", "retired"}:
            return ()
        if status == "conflict":
            return (_record(status, unknown),)
        _migrate(root, UnifiedJobStore(root / "jobs.sqlite"))
        return (_record("converted"),)

    def rollback(self, root: Path, backup_root: Path) -> None:
        from quantmaster.data.migration import restore_backup_path

        for relative in self.backup_paths:
            restore_backup_path(root, backup_root, relative)


backtest_job_legacy_migrator = BacktestJobLegacyMigrator()
