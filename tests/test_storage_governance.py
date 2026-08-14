from __future__ import annotations

import os
import sqlite3
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from quantmaster.runtime.storage_governance import (
    InstanceRepairTarget,
    StorageBoundaryError,
    StorageRequest,
    classify_sqlite_error,
    create_inheriting_temporary_directory,
    diagnose_sqlite,
    repair_instance_database,
    resolve_storage,
    validate_instance_repair_target,
)


def _windows_os() -> SimpleNamespace:
    return SimpleNamespace(
        name="nt", access=os.access, R_OK=os.R_OK, W_OK=os.W_OK, environ=os.environ,
    )


def test_acl_inspection_retries_one_transient_timeout(monkeypatch, tmp_path):
    from quantmaster.runtime import storage_governance

    calls = 0

    def run(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])
        return subprocess.CompletedProcess(
            args[0], 0, stdout="OWNER=test-owner\nINHERITED=True\n", stderr="",
        )

    monkeypatch.setattr(storage_governance, "os", _windows_os())
    monkeypatch.setattr(storage_governance.subprocess, "run", run)

    status = storage_governance.inspect_acl(tmp_path)

    assert calls == 2
    assert status.owner == "test-owner"
    assert status.inherited is True
    assert status.error == ""


def test_acl_inspection_reports_both_timeouts_as_unavailable(monkeypatch, tmp_path):
    from quantmaster.runtime import storage_governance

    calls = 0

    def timeout(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    monkeypatch.setattr(storage_governance, "os", _windows_os())
    monkeypatch.setattr(storage_governance.subprocess, "run", timeout)

    with pytest.raises(RuntimeError, match=r"无法验证.*attempt 1/2 TimeoutExpired.*attempt 2/2"):
        storage_governance.prepare_writable_directory(tmp_path / "acl-timeout")

    assert calls == 2


def test_acl_inspection_distinguishes_confirmed_non_inheritance(monkeypatch, tmp_path):
    from quantmaster.runtime import storage_governance

    monkeypatch.setattr(storage_governance, "os", _windows_os())
    monkeypatch.setattr(
        storage_governance,
        "inspect_acl",
        lambda path: storage_governance.ACLStatus(
            str(path), "test-owner", False, True, True,
        ),
    )

    with pytest.raises(PermissionError, match="目录未保留 Windows ACL 继承"):
        storage_governance.prepare_writable_directory(tmp_path / "protected-acl")


def test_temporary_directory_keeps_parent_inheritance_contract(monkeypatch, tmp_path):
    from quantmaster.runtime import storage_governance

    parent = tmp_path / "pytest-run" / "test-case" / "lab_evidence"
    inspected = []
    original = storage_governance.prepare_writable_directory

    def inspect(path, **kwargs):
        result = original(path, **kwargs)
        inspected.append(Path(path).resolve())
        return result

    monkeypatch.setattr(storage_governance, "prepare_writable_directory", inspect)
    staged = create_inheriting_temporary_directory(parent, prefix=".dataset-")
    published = parent / "published"
    staged.replace(published)

    assert inspected == [parent.resolve(), staged.resolve()]
    assert published.is_dir()
    assert published.parent == parent.resolve()


def test_temporary_directory_removes_acl_rejected_candidate(monkeypatch, tmp_path):
    from quantmaster.runtime import storage_governance

    parent = tmp_path / "pytest-run" / "test-case" / "lab_evidence"
    original = storage_governance.prepare_writable_directory
    calls = 0

    def reject_candidate(path, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return original(path, **kwargs)
        raise PermissionError("目录未保留 Windows ACL 继承")

    monkeypatch.setattr(storage_governance, "prepare_writable_directory", reject_candidate)
    with pytest.raises(PermissionError, match="ACL"):
        create_inheriting_temporary_directory(parent, prefix=".dataset-")

    assert parent.is_dir()
    assert list(parent.iterdir()) == []


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
