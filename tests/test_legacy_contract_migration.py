from __future__ import annotations

import threading
import time
import uuid
from pathlib import Path

import pytest

from quantmaster.data.legacy_migration import (
    LegacyMigrationError,
    LegacyMigrationManager,
    MigrationRecord,
    register_migrator,
    registered_migrations,
)


class FixtureMigrator:
    name = "fixture-contract"

    def __init__(self) -> None:
        self.records = [
            MigrationRecord("001", "converted"),
            MigrationRecord(
                "002", "blank", "optional_semantics_ambiguous", ("market", "adjustment"),
                "原记录未声明市场和复权语义",
            ),
            MigrationRecord("003", "conflict", "identity_conflict"),
        ]
        self.rolled_back = False

    def inspect(self, root: Path):
        return iter(self.records)

    def migrate_batch(self, root: Path, *, after_key: str, limit: int):
        values = [item for item in self.records if item.record_key > after_key]
        return iter(values[:limit])

    def rollback(self, root: Path, backup_root: Path) -> None:
        assert backup_root.is_dir()
        self.rolled_back = True


@pytest.fixture
def fixture_migrator():
    value = FixtureMigrator()
    value.name = f"fixture-contract-{uuid.uuid4().hex}"
    register_migrator(value)
    return value


def wait_finished(manager: LegacyMigrationManager, run_id: str) -> dict:
    for _ in range(200):
        task = manager.get(run_id)
        if task["status"] not in manager.ACTIVE:
            return task
        time.sleep(0.01)
    raise AssertionError(manager.get(run_id))


def test_status_read_does_not_create_state_database(tmp_path):
    manager = LegacyMigrationManager(tmp_path)
    assert manager.latest() is None
    assert not manager.state_path.exists()


def test_builtin_domain_migrations_are_registered_without_touching_data(tmp_path):
    assert {
        "market_data", "decision", "after_close", "news", "automation-contract-v9",
        "paper-ledger",
    } <= set(registered_migrations())
    assert not (tmp_path / "legacy_contract_migrations.sqlite").exists()


def test_dry_run_persists_counts_and_specific_unknown_evidence(tmp_path, fixture_migrator):
    manager = LegacyMigrationManager(tmp_path)
    task = manager.create(fixture_migrator.name, mode="dry_run", batch_size=2)
    result = wait_finished(manager, task["id"])
    assert result["status"] == "completed"
    assert result["total"] == 3
    assert result["checked"] == 3
    assert result["converted"] == 1
    assert result["blank"] == 1
    assert result["conflicts"] == 1
    unknown = {item["record_key"]: item for item in result["unknown_results"]}
    assert unknown["002"]["diagnostic_code"] == "optional_semantics_ambiguous"
    assert unknown["002"]["unknown_fields"] == ["market", "adjustment"]


def test_apply_backs_up_before_batches_and_can_rollback(tmp_path, fixture_migrator):
    database = tmp_path / "source.sqlite"
    import sqlite3

    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE evidence(value TEXT)")
        connection.execute("INSERT INTO evidence VALUES('before')")
    manager = LegacyMigrationManager(tmp_path)
    task = manager.create(fixture_migrator.name, mode="apply", batch_size=2)
    result = wait_finished(manager, task["id"])
    assert result["status"] == "completed"
    assert result["write_paused"] is False
    backup = Path(result["backup_path"]) / "source.sqlite"
    assert backup.is_file()
    rolled_back = manager.rollback(task["id"])
    assert rolled_back["status"] == "rolled_back"
    assert fixture_migrator.rolled_back is True


def test_only_one_active_run_and_resume_from_last_batch(tmp_path, fixture_migrator, monkeypatch):
    manager = LegacyMigrationManager(tmp_path, backup=lambda *_: time.sleep(0.1))
    first = manager.create(fixture_migrator.name, mode="apply", batch_size=1)
    with pytest.raises(LegacyMigrationError, match="已有"):
        manager.create(fixture_migrator.name, mode="dry_run")
    wait_finished(manager, first["id"])


def test_pause_request_crosses_maintenance_write_fence(tmp_path, fixture_migrator):
    backup_started = threading.Event()
    release_backup = threading.Event()

    def blocking_backup(*_):
        backup_started.set()
        assert release_backup.wait(2)

    manager = LegacyMigrationManager(tmp_path, backup=blocking_backup)
    task = manager.create(fixture_migrator.name, mode="apply", batch_size=1)
    assert backup_started.wait(2)
    pausing = manager.pause(task["id"])
    assert pausing["status"] == "pausing"
    release_backup.set()
    result = wait_finished(manager, task["id"])
    assert result["status"] == "paused"
    assert result["write_paused"] is False


def test_new_manager_recovers_interrupted_run_as_resumable(tmp_path, fixture_migrator):
    import sqlite3

    manager = LegacyMigrationManager(tmp_path)
    manager._initialize()
    now = "2026-01-01T00:00:00+00:00"
    with sqlite3.connect(manager.state_path) as connection:
        connection.execute(
            "INSERT INTO migration_runs(id,domain,mode,status,phase,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?)",
            ("interrupted", fixture_migrator.name, "apply", "running", "批次 2", now, now),
        )
    recovered = LegacyMigrationManager(tmp_path)
    recovered._initialize()
    task = recovered.get("interrupted")
    assert task["status"] == "paused"
    assert task["diagnostic_code"] == "process_interrupted"


def test_resume_reuses_backup_and_continues_after_last_key(tmp_path, fixture_migrator):
    backups = []

    def backup(_root, target):
        target.mkdir(parents=True)
        backups.append(target)

    manager = LegacyMigrationManager(tmp_path, backup=backup)
    manager._initialize()
    backup = manager.backup_root / "resume"
    backup.mkdir(parents=True)
    now = "2026-01-01T00:00:00+00:00"
    import sqlite3

    with sqlite3.connect(manager.state_path) as connection:
        connection.execute(
            "INSERT INTO migration_runs(id,domain,mode,status,phase,total,checked,last_key,last_batch,"
            "backup_path,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            ("resume", fixture_migrator.name, "apply", "paused", "已暂停", 3, 1, "001", 1,
             str(backup), now, now),
        )
        connection.execute(
            "INSERT INTO migration_audit(run_id,domain,batch,record_key,outcome,recorded_at) "
            "VALUES(?,?,?,?,?,?)",
            ("resume", fixture_migrator.name, 1, "001", "converted", now),
        )
    result = wait_finished(manager, manager.resume("resume", batch_size=1)["id"])
    assert result["status"] == "completed"
    assert result["checked"] == 3
    assert result["last_batch"] == 3
    assert backups == []


def test_unknown_domain_is_rejected_without_creating_state(tmp_path):
    manager = LegacyMigrationManager(tmp_path)
    with pytest.raises(LegacyMigrationError, match="未知迁移类型"):
        manager.create("unknown")
    assert not manager.state_path.exists()


def test_maintenance_write_authorization_is_lease_and_thread_scoped(tmp_path):
    import sqlite3

    from quantmaster.runtime.maintenance import MaintenanceActiveError, maintenance_barrier
    from quantmaster.runtime.sqlite import connect_sqlite

    database = tmp_path / "scope.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE values_table(value TEXT)")
    lease = maintenance_barrier.enter("test-authorized-migration")
    try:
        with connect_sqlite(database) as connection:
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                connection.execute("INSERT INTO values_table VALUES('blocked')")
        errors = []

        def other_thread() -> None:
            try:
                with connect_sqlite(database) as connection:
                    connection.execute("INSERT INTO values_table VALUES('wrong-thread')")
            except sqlite3.OperationalError as exc:
                errors.append(str(exc))

        with maintenance_barrier.authorize(lease):
            worker = threading.Thread(target=other_thread)
            worker.start()
            worker.join()
            with connect_sqlite(database) as connection:
                connection.execute("INSERT INTO values_table VALUES('authorized')")
        assert errors and "readonly" in errors[0]
        with pytest.raises(MaintenanceActiveError, match="租约"):
            from quantmaster.runtime.maintenance import MaintenanceLease

            with maintenance_barrier.authorize(MaintenanceLease("wrong", "test")):
                pass
    finally:
        maintenance_barrier.exit(lease)
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT value FROM values_table").fetchall() == [("authorized",)]
