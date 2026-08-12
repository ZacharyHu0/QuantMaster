from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from quantmaster.runtime.sqlite_recovery import (
    FileIdentity,
    RecoveryEvidence,
    RecoveryPlan,
    StorageBoundaryError,
    WriterIdentity,
    capture_file_identity,
    execute_recovery_plan,
    validate_recovery_plan,
)
from quantmaster.runtime.storage_governance import InstanceRepairTarget


class Guard:
    def __init__(self, marker, *, process=None):
        self.marker = marker
        self.process = process
        self.identity_override = None

    def stat_identity(self, path):
        if self.identity_override is not None:
            return self.identity_override
        stat = path.stat()
        return FileIdentity(stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)

    def owner_marker(self, _path):
        return self.marker

    def process_identity(self, _pid):
        return self.process


def make_plan(tmp_path: Path, guard: Guard, operation_id="repair-op-0001"):
    root = tmp_path / "runtime-instance"
    root.mkdir()
    database = root / "app.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE settings(name TEXT PRIMARY KEY,value TEXT)")
    executable = root / "writer.exe"
    executable.touch()
    marker = root / "owner.json"
    marker.write_text("{}", encoding="utf-8")
    backups = tmp_path / "backups"
    backups.mkdir()
    writer = WriterIdentity(4321, executable.resolve(), 123456)
    guard.marker = {
        "root": str(root.resolve()),
        "executable": str(executable.resolve()),
        "process": {
            "pid": writer.pid, "image": str(writer.executable), "created": writer.started,
        },
    }
    target = InstanceRepairTarget(
        database.resolve(), root.resolve(), str(database.resolve()), maintenance_confirmed=True,
    )
    plan = RecoveryPlan(
        operation_id, target, capture_file_identity(database, guard), marker.resolve(), writer,
        RecoveryEvidence(True, True, "service-maintenance-drain", "2026-08-13T00:00:00Z"),
        backups.resolve(), (backups / "recovery-ledger.sqlite").resolve(),
    )
    return plan, database


def isolated_validator(test_root: Path):
    boundary = test_root.resolve()

    def validate(target):
        """Test-only seam: never discovers or accepts a real instance path."""
        database = target.database.resolve()
        assert database.is_relative_to(target.instance_root.resolve())
        assert database.is_relative_to(boundary)
        return database

    return validate


def test_plan_rejects_changed_target_identity_and_live_expected_writer(tmp_path):
    guard = Guard({})
    plan, _database = make_plan(tmp_path, guard)
    validator = isolated_validator(tmp_path)
    guard.identity_override = replace(plan.target_identity, size=plan.target_identity.size + 1)
    with pytest.raises(StorageBoundaryError, match="TOCTOU"):
        validate_recovery_plan(plan, guard, target_validator=validator)

    guard.identity_override = plan.target_identity
    guard.process = {
        "pid": plan.writer.pid, "image": str(plan.writer.executable),
        "created": plan.writer.started,
    }
    with pytest.raises(StorageBoundaryError, match="活跃进程"):
        validate_recovery_plan(plan, guard, target_validator=validator)


def test_plan_requires_owner_and_explicit_stop_evidence(tmp_path):
    guard = Guard({})
    plan, _database = make_plan(tmp_path, guard)
    validator = isolated_validator(tmp_path)
    bad_marker = {**guard.marker, "process": {**guard.marker["process"], "created": 999}}
    guard.marker = bad_marker
    with pytest.raises(StorageBoundaryError, match="owner marker"):
        validate_recovery_plan(plan, guard, target_validator=validator)

    guard.marker = {
        **bad_marker, "process": {**bad_marker["process"], "created": plan.writer.started},
    }
    plan = replace(plan, evidence=replace(plan.evidence, writer_stopped=False))
    with pytest.raises(StorageBoundaryError, match="明确证据"):
        validate_recovery_plan(plan, guard, target_validator=validator)


def test_execute_is_audited_idempotent_and_rejects_operation_rebinding(tmp_path):
    guard = Guard({})
    plan, database = make_plan(tmp_path, guard)
    validator = isolated_validator(tmp_path)
    calls = []

    def migrate(connection):
        calls.append("called")
        connection.execute(
            "INSERT INTO settings VALUES('mode','safe') "
            "ON CONFLICT(name) DO UPDATE SET value=excluded.value"
        )

    first = execute_recovery_plan(
        plan, migrate, guard=guard, target_validator=validator,
    )
    second = execute_recovery_plan(
        plan, migrate, guard=guard, target_validator=validator,
    )

    assert calls == ["called"]
    assert first["noop"] is False and second["noop"] is True
    assert first["row_counts_before"] == {"settings": 0}
    assert first["row_counts_after"] == {"settings": 1}
    assert Path(first["backup"]).is_file()
    assert not list(plan.backup_directory.glob("*.partial"))
    with sqlite3.connect(plan.ledger) as ledger:
        status, audit = ledger.execute(
            "SELECT status,audit_json FROM recovery_operations WHERE operation_id=?",
            (plan.operation_id,),
        ).fetchone()
    assert status == "completed"
    assert json.loads(audit)["row_counts_after"] == {"settings": 1}
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT value FROM settings").fetchone()[0] == "safe"

    other_root = tmp_path / "other-instance"
    other_root.mkdir()
    other_database = other_root / "other.sqlite"
    with sqlite3.connect(other_database) as connection:
        connection.execute("CREATE TABLE values_v1(value TEXT)")
    rebound = replace(
        plan,
        target=InstanceRepairTarget(
            other_database.resolve(), other_root.resolve(), str(other_database.resolve()),
            maintenance_confirmed=True,
        ),
        target_identity=capture_file_identity(other_database),
    )
    # Validation happens before ledger lookup, so bind a matching isolated guard marker.
    rebound_marker = other_root / "owner.json"
    rebound_marker.write_text("{}", encoding="utf-8")
    rebound_executable = other_root / "writer.exe"
    rebound_executable.touch()
    rebound_writer = replace(plan.writer, executable=rebound_executable.resolve())
    rebound = replace(rebound, owner_marker=rebound_marker.resolve(), writer=rebound_writer)
    guard.marker = {
        "process": {
            "pid": rebound_writer.pid, "image": str(rebound_writer.executable),
            "created": rebound_writer.started,
        },
    }
    with pytest.raises(StorageBoundaryError, match="另一 recovery plan"):
        execute_recovery_plan(
            rebound, migrate, guard=guard, target_validator=validator,
        )


def test_execute_rolls_back_arbitrary_migration_exception(tmp_path):
    guard = Guard({})
    plan, database = make_plan(tmp_path, guard, operation_id="repair-op-rollback")

    class MigrationStopped(RuntimeError):
        pass

    def migrate(connection):
        connection.execute("INSERT INTO settings VALUES('mode','unsafe')")
        raise MigrationStopped("operator migration stopped")

    with pytest.raises(MigrationStopped, match="operator migration stopped"):
        execute_recovery_plan(
            plan, migrate, guard=guard, target_validator=isolated_validator(tmp_path),
        )

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT * FROM settings").fetchall() == []
    assert (plan.backup_directory / f"{database.name}.{plan.operation_id}.bak").is_file()
