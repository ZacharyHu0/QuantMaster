from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from quantmaster.runtime.storage_governance import (
    InstanceRepairTarget,
    StorageBoundaryError,
    StorageRequest,
    classify_sqlite_error,
    diagnose_sqlite,
    repair_instance_database,
    resolve_storage,
    validate_instance_repair_target,
)


def test_resolver_rejects_relative_workspace_and_test_writes_without_task(tmp_path):
    with pytest.raises(StorageBoundaryError, match="绝对路径"):
        resolve_storage(StorageRequest(Path("relative"), "test", "database", "writable"))
    with pytest.raises(StorageBoundaryError, match="task worktree"):
        resolve_storage(StorageRequest(
            tmp_path.resolve(), "test", "database", "writable", test_context=True,
        ))


def test_resolver_keeps_concurrent_worktrees_disjoint(tmp_path):
    workspace = tmp_path.resolve()
    first = workspace / ".worktrees" / "alpha"
    second = workspace / ".worktrees" / "beta"
    first.mkdir(parents=True)
    second.mkdir(parents=True)

    left = resolve_storage(StorageRequest(
        workspace, "tests", "provider-cache", "writable", first, True,
    ))
    right = resolve_storage(StorageRequest(
        workspace, "tests", "provider-cache", "writable", second, True,
    ))

    assert left.path != right.path
    assert left.path.is_relative_to(workspace / ".artifacts" / "worktrees" / "alpha")
    assert right.path.is_relative_to(workspace / ".artifacts" / "worktrees" / "beta")


def test_resolver_rejects_worktree_outside_registered_layout(tmp_path):
    workspace = tmp_path.resolve()
    outside = workspace / "other" / "feature"
    outside.mkdir(parents=True)
    with pytest.raises(StorageBoundaryError, match=r"workspace/.worktrees"):
        resolve_storage(StorageRequest(
            workspace, "tests", "runtime", "writable", outside, True,
        ))


@pytest.mark.parametrize(
    ("message", "code"),
    [
        ("database is locked", "SQLITE_LOCKED"),
        ("attempt to write a readonly database", "SQLITE_READ_ONLY"),
        ("database or disk is full", "STORAGE_SPACE_INSUFFICIENT"),
        ("database disk image is malformed", "SQLITE_CORRUPT"),
        ("unable to open database file", "SQLITE_OPEN_FAILED"),
        ("disk I/O error", "SQLITE_IO_ERROR"),
    ],
)
def test_classify_sqlite_errors(message, code):
    assert classify_sqlite_error(sqlite3.OperationalError(message)) == code


def test_diagnose_sqlite_is_read_only_and_reports_wal(tmp_path):
    database = tmp_path / "healthy.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE samples(value TEXT)")
        connection.execute("INSERT INTO samples VALUES ('ok')")
        connection.execute("PRAGMA user_version=7")
    before = database.stat().st_mtime_ns

    report = diagnose_sqlite(database.resolve())

    assert report["diagnostic_code"] == "OK"
    assert report["read_only"] is True
    assert report["schema_version"] == 7
    assert report["quick_check"] == ["ok"]
    assert database.stat().st_mtime_ns == before


def test_diagnose_missing_database_does_not_create_it(tmp_path):
    database = (tmp_path / "missing.sqlite").resolve()
    report = diagnose_sqlite(database)
    assert report["diagnostic_code"] == "SQLITE_MISSING"
    assert not database.exists()


def _repair_target(database: Path, root: Path, **changes) -> InstanceRepairTarget:
    values = {
        "database": database.resolve(), "instance_root": root.resolve(),
        "confirmation": str(database.resolve()), "maintenance_confirmed": True,
    }
    values.update(changes)
    return InstanceRepairTarget(**values)


def test_instance_repair_requires_exact_target_and_rejects_test_or_dry_run(tmp_path):
    root = tmp_path / "instance"
    root.mkdir()
    database = root / "app.sqlite"
    database.touch()
    with pytest.raises(StorageBoundaryError, match="逐字确认"):
        validate_instance_repair_target(InstanceRepairTarget(
            database, root, "wrong", maintenance_confirmed=True,
        ))
    with pytest.raises(StorageBoundaryError, match="fixture"):
        validate_instance_repair_target(_repair_target(database, root, test_context=True))
    with pytest.raises(StorageBoundaryError, match="dry-run"):
        validate_instance_repair_target(_repair_target(database, root, dry_run=True))


def test_instance_repair_rejects_active_writer_or_missing_maintenance(tmp_path):
    root = tmp_path / "instance"
    root.mkdir()
    database = root / "app.sqlite"
    database.touch()
    with pytest.raises(StorageBoundaryError, match="维护屏障"):
        validate_instance_repair_target(InstanceRepairTarget(
            database.resolve(), root.resolve(), str(database.resolve()),
        ))
    with pytest.raises(StorageBoundaryError, match="活跃写入者"):
        validate_instance_repair_target(_repair_target(database, root, writer_active=True))


def test_instance_repair_backup_transaction_and_idempotence(tmp_path, monkeypatch):
    root = tmp_path / "instance"
    root.mkdir()
    database = root / "app.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE settings(name TEXT PRIMARY KEY, value TEXT)")
    target = _repair_target(database, root)
    monkeypatch.setattr(
        "quantmaster.runtime.storage_governance.validate_instance_repair_target",
        lambda _target: database.resolve(),
    )
    backups = tmp_path / "backups"

    def migrate(connection):
        connection.execute(
            "INSERT INTO settings(name,value) VALUES('mode','safe') "
            "ON CONFLICT(name) DO UPDATE SET value=excluded.value"
        )

    first = repair_instance_database(target, migrate, backup_directory=backups.resolve())
    second = repair_instance_database(target, migrate, backup_directory=backups.resolve())

    assert Path(first["backup"]).is_file()
    assert Path(second["backup"]).is_file()
    assert first["row_counts_before"] == {"settings": 0}
    assert first["row_counts_after"] == {"settings": 1}
    assert not list(backups.glob("*.partial"))
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT name,value FROM settings").fetchall() == [("mode", "safe")]


def test_instance_repair_rolls_back_and_keeps_verified_backup(tmp_path, monkeypatch):
    root = tmp_path / "instance"
    root.mkdir()
    database = root / "app.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE values_v1(value TEXT)")
        connection.execute("INSERT INTO values_v1 VALUES('before')")
    target = _repair_target(database, root)
    monkeypatch.setattr(
        "quantmaster.runtime.storage_governance.validate_instance_repair_target",
        lambda _target: database.resolve(),
    )
    backups = (tmp_path / "backups").resolve()

    def fail(connection):
        connection.execute("UPDATE values_v1 SET value='after'")
        raise RuntimeError("stop")

    with pytest.raises(RuntimeError, match="stop"):
        repair_instance_database(target, fail, backup_directory=backups)

    assert len(list(backups.glob("*.bak"))) == 1
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT value FROM values_v1").fetchone()[0] == "before"
