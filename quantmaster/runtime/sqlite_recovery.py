"""Explicit, auditable orchestration for a confirmed SQLite instance repair."""

from __future__ import annotations

import json
import os
import re
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from quantmaster.runtime.sqlite import connect_sqlite_recovery
from quantmaster.runtime.storage_governance import (
    InstanceRepairTarget,
    StorageBoundaryError,
    diagnose_sqlite,
    validate_instance_repair_target,
)


@dataclass(frozen=True)
class FileIdentity:
    device: int
    inode: int
    size: int
    modified_ns: int


@dataclass(frozen=True)
class WriterIdentity:
    pid: int
    executable: Path
    started: int


@dataclass(frozen=True)
class RecoveryEvidence:
    maintenance_confirmed: bool
    writer_stopped: bool
    method: str
    observed_at: str


@dataclass(frozen=True)
class RecoveryPlan:
    operation_id: str
    target: InstanceRepairTarget
    target_identity: FileIdentity
    owner_marker: Path
    writer: WriterIdentity
    evidence: RecoveryEvidence
    backup_directory: Path
    ledger: Path


class RecoveryGuard(Protocol):
    def stat_identity(self, path: Path) -> FileIdentity: ...

    def owner_marker(self, path: Path) -> Mapping[str, Any]: ...

    def process_identity(self, pid: int) -> Mapping[str, Any] | None: ...


class LocalRecoveryGuard:
    """Read-only operating-system observations; it never stops a process."""

    def stat_identity(self, path: Path) -> FileIdentity:
        stat = path.stat()
        return FileIdentity(stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)

    def owner_marker(self, path: Path) -> Mapping[str, Any]:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise StorageBoundaryError("owner marker 格式无效")
        return value

    def process_identity(self, pid: int) -> Mapping[str, Any] | None:
        from quantmaster.data.free_stockdb_runtime import FreeStockDBRuntime

        return FreeStockDBRuntime._process_identity(pid)


def capture_file_identity(path: str | Path, guard: RecoveryGuard | None = None) -> FileIdentity:
    return (guard or LocalRecoveryGuard()).stat_identity(Path(path).resolve())


def _plan_record(plan: RecoveryPlan) -> dict[str, Any]:
    return {
        "operation_id": plan.operation_id,
        "database": str(plan.target.database.resolve()),
        "instance_root": str(plan.target.instance_root.resolve()),
        "target_identity": asdict(plan.target_identity),
        "owner_marker": str(plan.owner_marker.resolve()),
        "writer": {
            "pid": plan.writer.pid,
            "executable": str(plan.writer.executable.resolve()),
            "started": plan.writer.started,
        },
        "evidence": asdict(plan.evidence),
        "backup_directory": str(plan.backup_directory.resolve()),
    }


def _marker_identity(marker: Mapping[str, Any]) -> tuple[int, str, int]:
    process = marker.get("process")
    if not isinstance(process, Mapping):
        raise StorageBoundaryError("owner marker 缺少 writer process identity")
    return (
        int(process.get("pid") or 0),
        str(process.get("image") or marker.get("executable") or ""),
        int(process.get("created") or 0),
    )


def validate_recovery_plan(
    plan: RecoveryPlan,
    guard: RecoveryGuard,
    *,
    target_validator: Callable[[InstanceRepairTarget], Path] = validate_instance_repair_target,
) -> Path:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{7,79}", plan.operation_id):
        raise StorageBoundaryError("operation_id 格式无效")
    database = target_validator(plan.target)
    if not plan.evidence.maintenance_confirmed or not plan.evidence.writer_stopped:
        raise StorageBoundaryError("缺少 maintenance confirmed / writer stopped 明确证据")
    if not plan.evidence.method.strip() or not plan.evidence.observed_at.strip():
        raise StorageBoundaryError("停写证据必须记录方法和观察时间")
    marker_path = plan.owner_marker.expanduser().resolve()
    if not marker_path.is_file() or not marker_path.is_relative_to(plan.target.instance_root.resolve()):
        raise StorageBoundaryError("owner marker 不存在或越出实例根")
    marker = guard.owner_marker(marker_path)
    expected = (
        plan.writer.pid, str(plan.writer.executable.resolve()).casefold(), plan.writer.started,
    )
    marker_pid, marker_image, marker_started = _marker_identity(marker)
    observed = (marker_pid, str(Path(marker_image).resolve()).casefold(), marker_started)
    if observed != expected:
        raise StorageBoundaryError("owner marker 与 expected PID/executable/start identity 不一致")
    current = guard.process_identity(plan.writer.pid)
    if current is not None and (
        int(current.get("created") or 0) == plan.writer.started
        and str(Path(str(current.get("image") or "")).resolve()).casefold() == expected[1]
    ):
        raise StorageBoundaryError("writer stopped 证据与当前活跃进程冲突")
    if guard.stat_identity(database) != plan.target_identity:
        raise StorageBoundaryError("目标 stat identity 已变化，拒绝继续以防 TOCTOU")
    return database


def _ledger_connection(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = connect_sqlite_recovery(path)
    connection.execute(
        "CREATE TABLE IF NOT EXISTS recovery_operations ("
        "operation_id TEXT PRIMARY KEY,status TEXT NOT NULL,plan_json TEXT NOT NULL,"
        "audit_json TEXT NOT NULL DEFAULT '{}',updated_at TEXT NOT NULL)"
    )
    return connection


def execute_recovery_plan(
    plan: RecoveryPlan,
    migrate: Callable[[sqlite3.Connection], None],
    *,
    guard: RecoveryGuard,
    target_validator: Callable[[InstanceRepairTarget], Path] = validate_instance_repair_target,
) -> dict[str, Any]:
    """Execute one guarded repair; completed operation IDs are durable no-ops."""

    # Resolve only through the caller's explicit validation seam before reading
    # the independent ledger.  A completed operation is a true no-op even
    # though its own successful migration changed the database stat identity.
    database = target_validator(plan.target)
    plan_json = json.dumps(_plan_record(plan), ensure_ascii=False, sort_keys=True)
    ledger_path = plan.ledger.expanduser().resolve()
    if ledger_path == database or not ledger_path.is_relative_to(plan.backup_directory.resolve()):
        raise StorageBoundaryError("recovery ledger 必须位于独立备份目录")
    now = datetime.now(UTC).isoformat()
    with _ledger_connection(ledger_path) as ledger:
        row = ledger.execute(
            "SELECT status,plan_json,audit_json FROM recovery_operations WHERE operation_id=?",
            (plan.operation_id,),
        ).fetchone()
        if row:
            if str(row[1]) != plan_json:
                raise StorageBoundaryError("operation_id 已绑定另一 recovery plan")
            if str(row[0]) == "completed":
                return {**json.loads(str(row[2])), "noop": True}
            if str(row[0]) == "running":
                raise StorageBoundaryError("相同 recovery operation 正在执行")
        ledger.execute(
            "INSERT INTO recovery_operations(operation_id,status,plan_json,updated_at) "
            "VALUES(?,?,?,?) ON CONFLICT(operation_id) DO UPDATE SET status=excluded.status,"
            "updated_at=excluded.updated_at",
            (plan.operation_id, "running", plan_json, now),
        )

    validate_recovery_plan(plan, guard, target_validator=target_validator)
    before = diagnose_sqlite(database)
    if before.get("diagnostic_code") != "OK":
        raise sqlite3.DatabaseError(f"修复前健康检查失败: {before.get('diagnostic_code')}")
    # Revalidate after diagnosis and immediately before any write/backup.
    validate_recovery_plan(plan, guard, target_validator=target_validator)
    backup = plan.backup_directory.resolve() / f"{database.name}.{plan.operation_id}.bak"
    partial = backup.with_suffix(backup.suffix + ".partial")
    if not backup.exists():
        with connect_sqlite_recovery(database, read_only=True) as source:
            with connect_sqlite_recovery(partial) as destination:
                source.backup(destination)
                if destination.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise sqlite3.DatabaseError("备份健康检查失败")
        with partial.open("rb+") as stream:
            os.fsync(stream.fileno())
        os.replace(partial, backup)
    validate_recovery_plan(plan, guard, target_validator=target_validator)

    with connect_sqlite_recovery(database) as connection:
        tables = [
            str(row[0]) for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        ]
        before_counts = {
            table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for table in tables
        }
        start_changes = connection.total_changes
        connection.execute("BEGIN IMMEDIATE")
        # The managed connection context rolls back on every exceptional exit,
        # including caller-defined migration exceptions.  Keep the exception
        # untouched for the recovery operator instead of broadly catching it.
        migrate(connection)
        connection.commit()
        after_counts = {
            table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for table in tables
        }
        changes = connection.total_changes - start_changes
    after = diagnose_sqlite(database)
    if after.get("diagnostic_code") != "OK":
        raise sqlite3.DatabaseError(f"修复后健康检查失败: {after.get('diagnostic_code')}")
    audit = {
        "operation_id": plan.operation_id, "database": str(database), "backup": str(backup),
        "changes": changes, "row_counts_before": before_counts,
        "row_counts_after": after_counts, "before": before, "after": after,
        "completed_at": datetime.now(UTC).isoformat(), "noop": False,
    }
    with _ledger_connection(ledger_path) as ledger:
        ledger.execute(
            "UPDATE recovery_operations SET status='completed',audit_json=?,updated_at=? "
            "WHERE operation_id=? AND status='running'",
            (json.dumps(audit, ensure_ascii=False, sort_keys=True), audit["completed_at"], plan.operation_id),
        )
    return audit
