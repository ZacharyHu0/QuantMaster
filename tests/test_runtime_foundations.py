"""Concurrency and recovery contracts for shared runtime foundations."""

from __future__ import annotations

import sqlite3
import subprocess
from concurrent.futures import ThreadPoolExecutor

import pytest

from quantmaster.runtime.sqlite import connect_sqlite, migrate_schema


def test_concurrent_first_connections_enable_wal_once_without_locking(tmp_path):
    path = tmp_path / "runtime.sqlite"

    def initialize(index: int) -> tuple[str, int]:
        with connect_sqlite(path) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS writes (id INTEGER PRIMARY KEY, value INTEGER)"
            )
            connection.execute("INSERT INTO writes(value) VALUES (?)", (index,))
            mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        return str(mode).lower(), index

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(initialize, range(24)))

    assert {mode for mode, _ in results} == {"wal"}
    with connect_sqlite(path, row_factory=True) as connection:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM writes").fetchone()[0] == 24


def test_schema_migration_rolls_back_version_and_content_together(tmp_path):
    path = tmp_path / "migrations.sqlite"
    with connect_sqlite(path) as connection:
        assert migrate_schema(connection, [
            (1, lambda conn: conn.execute("CREATE TABLE values_v1 (value TEXT)")),
        ]) == 1

        def broken(conn: sqlite3.Connection) -> None:
            conn.execute("INSERT INTO values_v1(value) VALUES ('must-rollback')")
            raise RuntimeError("injected migration failure")

        with pytest.raises(RuntimeError, match="injected"):
            migrate_schema(connection, [(2, broken)])
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM values_v1").fetchone()[0] == 0


def test_process_start_retries_only_transient_windows_errors(monkeypatch):
    from quantmaster.runtime import process

    calls = 0

    def flaky_run(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls < 3:
            error = PermissionError("temporarily denied")
            error.winerror = 5
            raise error
        return subprocess.CompletedProcess(args[0], 0, stdout="ready")

    monkeypatch.setattr(process.subprocess, "run", flaky_run)
    monkeypatch.setattr(process.time, "sleep", lambda _: None)

    result = process.run_process(["python", "-V"])

    assert calls == 3
    assert result.stdout == "ready"


def test_process_start_does_not_retry_permanent_errors(monkeypatch):
    from quantmaster.runtime import process

    calls = 0

    def missing_run(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise FileNotFoundError("missing executable")

    monkeypatch.setattr(process.subprocess, "run", missing_run)
    with pytest.raises(FileNotFoundError):
        process.run_process(["missing"])
    assert calls == 1
