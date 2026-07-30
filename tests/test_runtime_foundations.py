"""Concurrency and recovery contracts for shared runtime foundations."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.json import StrictJSONResponse, strict_json_dumps
from quantmaster.runtime.maintenance import (
    MaintenanceActiveError,
    MaintenanceBarrier,
    MaintenanceParticipant,
    maintenance_barrier,
)
from quantmaster.runtime.process import ProcessLimitError, ProcessLimits, run_restricted_process
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


def test_maintenance_barrier_drains_freezes_and_resumes_in_reverse_order():
    barrier = MaintenanceBarrier()
    events = []
    idle = {"one": False, "two": False}

    for name in ("one", "two"):
        barrier.register(MaintenanceParticipant(
            name=name,
            drain=lambda name=name: (events.append(f"drain:{name}"), idle.__setitem__(name, True)),
            resume=lambda name=name: events.append(f"resume:{name}"),
            idle=lambda name=name: idle[name],
        ))

    lease = barrier.enter("test", timeout=1)
    assert barrier.frozen
    with pytest.raises(MaintenanceActiveError):
        barrier.require_writable()
    barrier.exit(lease)

    assert events == ["drain:one", "drain:two", "resume:two", "resume:one"]
    assert not barrier.active


def test_frozen_barrier_makes_existing_sqlite_connections_read_only(tmp_path):
    path = tmp_path / "frozen.sqlite"
    with connect_sqlite(path) as connection:
        connection.execute("CREATE TABLE values_v1 (value TEXT)")
        connection.execute("INSERT INTO values_v1 VALUES ('before')")

    lease = maintenance_barrier.enter("test_sqlite_freeze")
    try:
        with connect_sqlite(path) as connection:
            assert connection.execute("SELECT value FROM values_v1").fetchone()[0] == "before"
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                connection.execute("INSERT INTO values_v1 VALUES ('during')")
        with pytest.raises(MaintenanceActiveError):
            connect_sqlite(tmp_path / "new.sqlite")
    finally:
        maintenance_barrier.exit(lease)

    with connect_sqlite(path) as connection:
        connection.execute("INSERT INTO values_v1 VALUES ('after')")
        assert connection.execute("SELECT COUNT(*) FROM values_v1").fetchone()[0] == 2


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


def test_restricted_process_rejects_excess_output():
    with pytest.raises(ProcessLimitError, match="输出超过"):
        run_restricted_process(
            [sys.executable, "-c", "print('x' * 10000)"],
            limits=ProcessLimits(
                memory_bytes=256 * 1024 * 1024,
                cpu_seconds=5,
                output_bytes=1024,
            ),
            timeout=5,
        )


@pytest.mark.skipif(os.name != "nt", reason="Windows Job Object contract")
def test_windows_restricted_process_cannot_create_child_process():
    result = run_restricted_process(
        [
            sys.executable,
            "-c",
            "import subprocess,sys; subprocess.run([sys.executable,'-c','print(1)'],check=True)",
        ],
        limits=ProcessLimits(
            memory_bytes=256 * 1024 * 1024,
            cpu_seconds=5,
            output_bytes=64 * 1024,
            max_processes=1,
        ),
        timeout=5,
    )
    assert result.returncode != 0


def test_strict_json_boundary_converts_nonfinite_values_to_null():
    app = FastAPI(default_response_class=StrictJSONResponse)

    @app.get("/values")
    def values():
        return {"nan": float("nan"), "values": [float("inf"), float("-inf"), 1.0]}

    response = TestClient(app).get("/values")

    assert response.status_code == 200
    assert response.json() == {"nan": None, "values": [None, None, 1.0]}
    encoded = strict_json_dumps({
        "value": float("nan"), "decimal": Decimal("Infinity"),
        "overflow": Decimal("1e10000"), "finite": Decimal("1.25"),
    })
    assert encoded == '{"value":null,"decimal":null,"overflow":null,"finite":1.25}'
    assert "NaN" not in encoded and "Infinity" not in encoded


def test_contract_model_rejects_extra_and_nested_nonfinite_values():
    class Payload(ContractModel):
        options: dict

    with pytest.raises(ValidationError) as nonfinite:
        Payload.model_validate({"options": {"nested": [1, float("nan")]}})
    assert nonfinite.value.errors()[0]["type"] == "value_error"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        Payload.model_validate({"options": {}, "unknown": True})
