"""One-shot migration of Quant Lab lifecycle rows to UnifiedJobStore."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from contextlib import closing
from pathlib import Path
from typing import Any

from quantmaster.data.legacy_migration import MigrationRecord
from quantmaster.lab.jobs import (
    LAB_ALGORITHM_VERSION,
    LAB_JOB_TYPES,
    LAB_KINDS,
    LAB_PROGRESS_CHECKPOINT,
    LAB_RESULT_KIND,
)
from quantmaster.lab.models import canonical_json, content_hash
from quantmaster.lab.store import LAB_SCHEMA_VERSION, LabStore
from quantmaster.runtime.jobs import UnifiedJobStore
from quantmaster.runtime.paths import confined_path
from quantmaster.runtime.sqlite import connect_sqlite

_LAB = Path("lab.sqlite")
_DOMAIN_TABLES = {
    "factor_definitions",
    "factor_versions",
    "validation_reports",
    "approvals",
    "deployments",
    "deployment_evidence",
    "dataset_snapshots",
    "experiments",
    "copilot_suggestions",
    "optimization_studies",
    "bias_audits",
    "mining_runs",
    "mining_candidates",
    "lab_worker_results",
    "lab_publications",
    "lab_publication_events",
    "research_cycles",
    "strategy_candidates",
    "shadow_signals",
    "promotion_events",
}
_LEGACY_TABLES = {"lab_jobs", "lab_job_events", "lab_schedule_slots"}
_JOB_COLUMNS = {
    "id",
    "kind",
    "status",
    "params_json",
    "result_json",
    "dataset_id",
    "resource_class",
    "preflight_json",
    "progress",
    "phase",
    "detail",
    "error",
    "error_code",
    "error_json",
    "telemetry_json",
    "cancel_requested",
    "worker",
    "llm_scope",
    "llm_revision",
    "cancellation_reason",
    "created_at",
    "started_at",
    "heartbeat_at",
    "finished_at",
}
_EVENT_COLUMNS = {"seq", "job_id", "event_json", "created_at"}
_SLOT_COLUMNS = {"slot", "created_at"}
_RESULT_COLUMNS = {
    "job_id", "attempt", "kind", "outcome", "result_json", "error_json",
    "telemetry_json", "content_hash", "created_at",
}
_STATUSES = {
    "queued",
    "running",
    "cancelling",
    "interrupted",
    "paused",
    "cancelled",
    "completed",
    "completed_with_warnings",
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


def _table_job_links(
    connection: sqlite3.Connection,
    jobs: dict[str, sqlite3.Row],
    table: str,
    expected_kind: str,
) -> set[str]:
    conflicts: set[str] = set()
    for row in connection.execute(f"SELECT id,job_id FROM {table}"):
        job_id = str(row["job_id"] or "")
        if not job_id:
            continue
        job = jobs.get(job_id)
        if job is None:
            conflicts.add(f"{table}:{row['id']}:dangling_job")
        elif str(job["kind"]) != expected_kind:
            conflicts.add(f"{table}:{row['id']}:job_kind")
    return conflicts


def _job_domain_links(
    connection: sqlite3.Connection,
    jobs: dict[str, sqlite3.Row],
) -> set[str]:
    conflicts: set[str] = set()
    for job_id, row in jobs.items():
        try:
            params = _json_object(row["params_json"], "params_json")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        kind = str(row["kind"])
        if kind == "optimize":
            study_id = str(params.get("study_id") or "")
            link = connection.execute(
                "SELECT job_id FROM optimization_studies WHERE id=?", (study_id,),
            ).fetchone()
            if link is None or str(link["job_id"] or "") not in {"", job_id}:
                conflicts.add(f"{job_id}:dangling_study")
        if kind == "discover_python":
            run_id = str(params.get("run_id") or "")
            link = connection.execute(
                "SELECT job_id FROM mining_runs WHERE id=?", (run_id,),
            ).fetchone()
            if link is None or str(link["job_id"] or "") not in {"", job_id}:
                conflicts.add(f"{job_id}:dangling_mining_run")
    return conflicts


def _domain_foreign_links(connection: sqlite3.Connection) -> set[str]:
    conflicts: set[str] = set()
    for row in connection.execute("SELECT id,experiment_id FROM optimization_studies"):
        experiment_id = str(row["experiment_id"] or "")
        if experiment_id and connection.execute(
            "SELECT 1 FROM experiments WHERE id=?", (experiment_id,),
        ).fetchone() is None:
            conflicts.add(f"optimization_studies:{row['id']}:dangling_experiment")
    for row in connection.execute("SELECT id,run_id,version_id FROM mining_candidates"):
        if connection.execute(
            "SELECT 1 FROM mining_runs WHERE id=?", (str(row["run_id"]),),
        ).fetchone() is None:
            conflicts.add(f"mining_candidates:{row['id']}:dangling_run")
        version_id = str(row["version_id"] or "")
        if version_id and connection.execute(
            "SELECT 1 FROM factor_versions WHERE id=?", (version_id,),
        ).fetchone() is None:
            conflicts.add(f"mining_candidates:{row['id']}:dangling_version")
    return conflicts


def _domain_links(connection: sqlite3.Connection, jobs: dict[str, sqlite3.Row]) -> set[str]:
    return (
        _table_job_links(connection, jobs, "optimization_studies", "optimize")
        | _table_job_links(connection, jobs, "mining_runs", "discover_python")
        | _job_domain_links(connection, jobs)
        | _domain_foreign_links(connection)
    )


def _artifact_path(root: Path, raw: Any) -> Path:
    value = str(raw or "")
    candidate = Path(value)
    if candidate.is_absolute():
        boundary = root.resolve()
        resolved = candidate.resolve()
        if not resolved.is_relative_to(boundary):
            raise ValueError("artifact 路径越出数据目录")
    else:
        resolved = confined_path(root, value, label="Lab artifact")
    if not resolved.is_file():
        raise FileNotFoundError(value)
    return resolved


def _trial_evidence_conflicts(
    study_id: str,
    result: dict[str, Any],
) -> set[str]:
    conflicts: set[str] = set()
    trials = result.get("trials")
    numbers: set[int] = set()
    if trials is not None:
        if not isinstance(trials, list):
            conflicts.add(f"optimization_studies:{study_id}:trials")
        else:
            for offset, trial in enumerate(trials):
                if not isinstance(trial, dict):
                    conflicts.add(f"optimization_studies:{study_id}:trial:{offset}")
                    continue
                number = trial.get("number")
                if isinstance(number, bool) or not isinstance(number, int) or number < 0:
                    conflicts.add(f"optimization_studies:{study_id}:trial:{offset}:number")
                elif number in numbers:
                    conflicts.add(f"optimization_studies:{study_id}:trial:{number}:duplicate")
                else:
                    numbers.add(number)
    recommended = result.get("recommended")
    if recommended is not None:
        if not isinstance(recommended, dict) or recommended.get("number") not in numbers:
            conflicts.add(f"optimization_studies:{study_id}:dangling_trial")
    return conflicts


def _nested_artifact_paths(
    study_id: str,
    result: dict[str, Any],
) -> tuple[set[str], list[tuple[str, Any]]]:
    conflicts: set[str] = set()
    paths: list[tuple[str, Any]] = []
    folds = result.get("fold_artifacts")
    if folds is not None:
        if not isinstance(folds, list):
            conflicts.add(f"optimization_studies:{study_id}:fold_artifacts")
        else:
            for offset, fold in enumerate(folds):
                if not isinstance(fold, dict) or not fold.get("artifact"):
                    conflicts.add(f"optimization_studies:{study_id}:fold_artifact:{offset}")
                else:
                    paths.append((f"fold_artifact:{offset}", fold["artifact"]))
    live = result.get("live_artifact")
    if live is not None:
        if not isinstance(live, dict) or not live.get("artifact"):
            conflicts.add(f"optimization_studies:{study_id}:live_artifact")
        else:
            paths.append(("live_artifact", live["artifact"]))
    if result.get("candidate") and not all(
        result.get(field) for field in ("prediction_artifact", "manifest", "fold_artifacts")
    ):
        conflicts.add(f"optimization_studies:{study_id}:candidate_artifacts")
    return conflicts, paths


def _artifact_evidence_conflicts(
    root: Path,
    study_id: str,
    result: dict[str, Any],
) -> set[str]:
    conflicts, paths = _nested_artifact_paths(study_id, result)
    for field in ("prediction_artifact", "manifest"):
        if result.get(field):
            paths.append((field, result[field]))
    for label, value in paths:
        try:
            _artifact_path(root, value)
        except (FileNotFoundError, OSError, ValueError):
            conflicts.add(f"optimization_studies:{study_id}:{label}:dangling")
    return conflicts


def _study_evidence_conflicts(
    root: Path,
    study_id: str,
    result: dict[str, Any],
) -> set[str]:
    return (
        _trial_evidence_conflicts(study_id, result)
        | _artifact_evidence_conflicts(root, study_id, result)
    )


def _mining_artifact_conflicts(
    root: Path,
    candidate_id: str,
    artifact: dict[str, Any],
) -> set[str]:
    if not artifact:
        return set()
    conflicts: set[str] = set()
    for field in ("manifest", "source"):
        try:
            _artifact_path(root, artifact.get(field))
        except (FileNotFoundError, OSError, ValueError):
            conflicts.add(f"mining_candidates:{candidate_id}:{field}:dangling")
    return conflicts


def _content_conflicts(root: Path, connection: sqlite3.Connection) -> tuple[str, ...]:
    conflicts: set[str] = set()
    rows = connection.execute("SELECT * FROM lab_jobs ORDER BY created_at,id").fetchall()
    jobs = {str(row["id"]): row for row in rows}
    for job_id, row in jobs.items():
        status = str(row["status"])
        kind = str(row["kind"])
        if status not in _STATUSES:
            conflicts.add(f"status:{status}")
        if kind not in LAB_KINDS:
            conflicts.add(f"kind:{kind}")
        for field in (
            "params_json", "result_json", "preflight_json", "error_json", "telemetry_json",
        ):
            try:
                _json_object(row[field], field)
            except (TypeError, ValueError, json.JSONDecodeError):
                conflicts.add(f"{job_id}:{field}")
        if status in {"running", "cancelling"} and not all(
            str(row[field] or "") for field in ("worker", "started_at", "heartbeat_at")
        ):
            conflicts.add(f"{job_id}:lease_evidence")
        if bool(row["cancel_requested"]) and status not in {
            "running", "cancelling", "interrupted", "cancelled",
        }:
            conflicts.add(f"{job_id}:cancel_status")
        if kind in {"discover_llm", "discover_python"}:
            scope = str(row["llm_scope"] or "")
            revision = str(row["llm_revision"] or "")
            manual = status == "interrupted" and str(row["phase"] or "") == "需要手动重试"
            if not manual and (not scope or not revision):
                conflicts.add(f"{job_id}:llm_revision")
    for row in connection.execute("SELECT job_id,event_json FROM lab_job_events"):
        job_id = str(row["job_id"])
        if job_id not in jobs:
            conflicts.add(f"event:{job_id}:dangling_job")
        try:
            payload = _json_object(row["event_json"], "event_json")
        except (TypeError, ValueError, json.JSONDecodeError):
            conflicts.add(f"{job_id}:event_json")
            continue
        if not str(payload.get("type") or ""):
            conflicts.add(f"{job_id}:event_type")
    for table, fields in (
        ("optimization_studies", ("config_json", "result_json")),
        ("mining_runs", ("config_json", "split_json", "result_json")),
        ("mining_candidates", ("proposal_json", "metrics_json", "artifact_json")),
    ):
        for row in connection.execute(f"SELECT * FROM {table}"):
            for field in fields:
                try:
                    payload = _json_object(row[field], field)
                except (TypeError, ValueError, json.JSONDecodeError):
                    conflicts.add(f"{table}:{row['id']}:{field}")
                    continue
                if table == "optimization_studies" and field == "result_json":
                    conflicts.update(_study_evidence_conflicts(root, str(row["id"]), payload))
                if table == "mining_candidates" and field == "artifact_json":
                    conflicts.update(_mining_artifact_conflicts(root, str(row["id"]), payload))
    conflicts.update(_domain_links(connection, jobs))
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
            expected_spec = {
                "kind": str(row["kind"]),
                "params": _json_object(row["params_json"], "params_json"),
                "preflight": _json_object(row["preflight_json"], "preflight_json"),
                "dataset_id": str(row["dataset_id"] or ""),
                "resource_class": str(row["resource_class"] or "cpu"),
            }
            if (
                str(existing.get("type") or "") != f"lab.{row['kind']}"
                or dict(existing.get("spec") or {}) != expected_spec
            ):
                conflicts.add(f"{job_id}:target_collision")
    except (FileNotFoundError, sqlite3.Error, ValueError):
        conflicts.add("jobs.sqlite:unclassified")
    return tuple(sorted(conflicts))


def _probe(root: Path) -> tuple[str, tuple[str, ...]]:
    path = root / _LAB
    if not path.is_file():
        return "absent", ()
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        tables = _tables(connection)
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        legacy = _LEGACY_TABLES & tables
        if not legacy:
            unknown = (tables - _DOMAIN_TABLES - {"sqlite_sequence"}) | (
                _DOMAIN_TABLES - tables
            )
            if version == LAB_SCHEMA_VERSION and not unknown:
                return "retired", ()
            return "conflict", tuple(sorted(unknown | {f"user_version:{version}"}))
        expected_domain = _DOMAIN_TABLES - ({"lab_worker_results"} if version == 11 else set())
        schema_conflicts = (
            tables - expected_domain - _LEGACY_TABLES - {"sqlite_sequence"}
        ) | (expected_domain - tables) | (_LEGACY_TABLES - tables)
        if version not in {11, LAB_SCHEMA_VERSION}:
            schema_conflicts.add(f"user_version:{version}")
        if not schema_conflicts:
            schema_conflicts |= _columns(connection, "lab_jobs") ^ _JOB_COLUMNS
            schema_conflicts |= _columns(connection, "lab_job_events") ^ _EVENT_COLUMNS
            schema_conflicts |= _columns(connection, "lab_schedule_slots") ^ _SLOT_COLUMNS
            if "lab_worker_results" in tables:
                schema_conflicts |= _columns(connection, "lab_worker_results") ^ _RESULT_COLUMNS
        content = _content_conflicts(root, connection) if not schema_conflicts else ()
        rows = connection.execute("SELECT * FROM lab_jobs").fetchall() if not schema_conflicts else ()
    evidence = set(schema_conflicts) | set(content)
    if not evidence:
        evidence.update(_target_conflicts(root, rows))
    return ("conflict", tuple(sorted(evidence))) if evidence else ("upgrade", ())


def _record(status: str, unknown: tuple[str, ...] = ()) -> MigrationRecord:
    if status == "conflict":
        return MigrationRecord(
            "quant-lab",
            "conflict",
            "lab_job_schema_unclassified",
            unknown,
            "Quant Lab 含未知 lifecycle、冲突 lease 或悬空领域关联，拒绝写入",
        )
    return MigrationRecord(
        "quant-lab",
        "review" if status == "upgrade" else "converted",
        "lab_job_lifecycle_migration_required" if status == "upgrade" else "lab_job_migrated",
        (),
        f"Quant Lab lifecycle {'需要迁移' if status == 'upgrade' else '已迁移'}",
    )


def _events(connection: sqlite3.Connection, job_id: str) -> list[dict[str, Any]]:
    result = []
    rows = connection.execute(
        "SELECT seq,event_json,created_at FROM lab_job_events WHERE job_id=? ORDER BY seq",
        (job_id,),
    ).fetchall()
    for offset, row in enumerate(rows, start=1):
        payload = _json_object(row["event_json"], "event_json")
        legacy_type = str(payload.pop("type"))
        result.append({
            "seq": offset,
            "attempt": 1,
            "type": f"legacy_lab_{legacy_type}",
            "payload": {"legacy_type": legacy_type, **payload},
            "created_at": row["created_at"],
        })
    return result


def _convert(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any] | None]:
    legacy_status = str(row["status"])
    statuses = {
        "queued": "queued",
        "running": "interrupted",
        "cancelling": "interrupted",
        "interrupted": "interrupted",
        "paused": "interrupted",
        "cancelled": "cancelled",
        "completed": "completed",
        "completed_with_warnings": "completed",
        "failed": "failed",
    }
    if legacy_status not in statuses:
        raise ValueError(f"未知 Lab status: {legacy_status}")
    kind = str(row["kind"])
    params = _json_object(row["params_json"], "params_json")
    result = _json_object(row["result_json"], "result_json")
    preflight = _json_object(row["preflight_json"], "preflight_json")
    error_info = _json_object(row["error_json"], "error_json")
    telemetry = _json_object(row["telemetry_json"], "telemetry_json")
    outcome = (
        "completed_with_warnings" if legacy_status == "completed_with_warnings"
        else "paused" if legacy_status == "paused"
        else legacy_status if legacy_status in {"completed", "failed", "cancelled"}
        else ""
    )
    events = _events(connection, str(row["id"]))
    checkpoint = next(
        (
            {"schema_version": "1.0", "type": "partition_checkpoint", **event["payload"]}
            for event in reversed(events)
            if event["payload"].get("legacy_type") == "partition_checkpoint"
        ),
        None,
    )
    artifacts: list[dict[str, Any]] = []
    if checkpoint is not None:
        artifacts.append({
            "kind": f"checkpoint.{LAB_PROGRESS_CHECKPOINT}",
            "checkpoint_key": LAB_PROGRESS_CHECKPOINT,
            "payload": checkpoint,
            "attempt": 1,
            "created_at": row["heartbeat_at"] or row["created_at"],
        })
    domain_result = None
    if outcome:
        payload = {
            "schema_version": "1.0",
            "kind": kind,
            "outcome": outcome,
            "result": result,
            "error_info": error_info,
            "telemetry": telemetry,
        }
        artifacts.append({
            "kind": LAB_RESULT_KIND,
            "result": True,
            "payload": payload,
            "attempt": 1,
            "created_at": row["finished_at"] or row["created_at"],
        })
        domain_result = {
            "job_id": str(row["id"]),
            "attempt": 1,
            "kind": kind,
            "outcome": outcome,
            "result": result,
            "error_info": error_info,
            "telemetry": telemetry,
            "created_at": row["finished_at"] or row["created_at"],
        }
    record = {
        "id": str(row["id"]),
        "type": f"lab.{kind}",
        "spec": {
            "kind": kind,
            "params": params,
            "preflight": preflight,
            "dataset_id": str(row["dataset_id"] or ""),
            "resource_class": str(row["resource_class"] or "cpu"),
        },
        "algorithm_version": LAB_ALGORITHM_VERSION,
        "status": statuses[legacy_status],
        "progress": int(row["progress"] or 0),
        "phase": "等待恢复" if legacy_status in {"running", "cancelling"} else str(row["phase"]),
        "detail": str(row["detail"] or row["error"] or ""),
        "attempt": 1,
        "max_attempts": 8,
        "cancel_requested": bool(row["cancel_requested"]),
        "llm_scope": str(row["llm_scope"] or ""),
        "llm_revision": str(row["llm_revision"] or ""),
        "diagnostic_code": str(row["error_code"] or ""),
        "created_at": row["created_at"],
        "updated_at": row["heartbeat_at"] or row["finished_at"] or row["created_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"] if statuses[legacy_status] in {
            "completed", "cancelled", "failed",
        } else "",
        "deadline_seconds": 3600,
    }
    return record, events, artifacts, domain_result


def _result_ddl(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE IF NOT EXISTS lab_worker_results ("
        "job_id TEXT NOT NULL,attempt INTEGER NOT NULL,kind TEXT NOT NULL,outcome TEXT NOT NULL,"
        "result_json TEXT NOT NULL,error_json TEXT NOT NULL DEFAULT '{}',"
        "telemetry_json TEXT NOT NULL DEFAULT '{}',content_hash TEXT NOT NULL,"
        "created_at TEXT NOT NULL,PRIMARY KEY(job_id,attempt))"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_lab_worker_results_kind "
        "ON lab_worker_results(kind,created_at DESC)"
    )


def _migrate(root: Path) -> None:
    path = root / _LAB
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        converted = [
            _convert(connection, row)
            for row in connection.execute(
                "SELECT * FROM lab_jobs ORDER BY created_at,id"
            ).fetchall()
        ]
    store = UnifiedJobStore(root / "jobs.sqlite")
    for record, events, artifacts, _domain_result in converted:
        store.import_legacy_job(record, events=events, artifacts=artifacts)
    for record, events, artifacts, _domain_result in converted:
        imported = store.get(str(record["id"]))
        if str(imported["type"]) not in LAB_JOB_TYPES:
            raise ValueError(f"Lab job 导入类型不守恒: {record['id']}")
        if len(store.events(str(record["id"]), 0, 2000)) < len(events):
            raise ValueError(f"Lab job event 导入数量不守恒: {record['id']}")
        if any(item.get("result") for item in artifacts) and not imported["result_artifact_id"]:
            raise ValueError(f"Lab worker result 导入缺失: {record['id']}")
    with closing(connect_sqlite(path, row_factory=True)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        _result_ddl(connection)
        for _record_value, _events_value, _artifacts_value, domain_result in converted:
            if domain_result is None:
                continue
            digest = content_hash({
                "kind": domain_result["kind"],
                "outcome": domain_result["outcome"],
                "result": domain_result["result"],
                "error_info": domain_result["error_info"],
                "telemetry": domain_result["telemetry"],
            })
            existing = connection.execute(
                "SELECT content_hash FROM lab_worker_results WHERE job_id=? AND attempt=?",
                (domain_result["job_id"], domain_result["attempt"]),
            ).fetchone()
            if existing is not None and str(existing["content_hash"]) != digest:
                raise ValueError(f"Lab worker result 冲突: {domain_result['job_id']}")
            connection.execute(
                "INSERT OR IGNORE INTO lab_worker_results "
                "(job_id,attempt,kind,outcome,result_json,error_json,telemetry_json,"
                "content_hash,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    domain_result["job_id"],
                    domain_result["attempt"],
                    domain_result["kind"],
                    domain_result["outcome"],
                    canonical_json(domain_result["result"]),
                    canonical_json(domain_result["error_info"]),
                    canonical_json(domain_result["telemetry"]),
                    digest,
                    domain_result["created_at"],
                ),
            )
        connection.execute("DROP TABLE lab_job_events")
        connection.execute("DROP TABLE lab_jobs")
        connection.execute("DROP TABLE lab_schedule_slots")
        connection.execute(f"PRAGMA user_version={LAB_SCHEMA_VERSION}")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    LabStore(path, read_only=True)


class LabJobLegacyMigrator:
    name = "lab-jobs"
    backup_paths = ("lab.sqlite", "jobs.sqlite")

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
        if after_key >= "quant-lab" or int(limit) < 1:
            return ()
        status, unknown = _probe(root)
        if status in {"absent", "retired"}:
            return ()
        if status == "conflict":
            return (_record(status, unknown),)
        _migrate(root)
        return (_record("converted"),)

    def rollback(self, root: Path, backup_root: Path) -> None:
        from quantmaster.data.migration import restore_backup_path

        for relative in self.backup_paths:
            restore_backup_path(root, backup_root, relative)


lab_job_legacy_migrator = LabJobLegacyMigrator()
