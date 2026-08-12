from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from quantmaster.server.app import app
from quantmaster.server.storage_status import storage_status


def _control_database(path: Path) -> None:
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TABLE runtime_state (
                singleton INTEGER PRIMARY KEY, payload_json TEXT, updated_at REAL);
            CREATE TABLE commands (
                action TEXT, status TEXT, completed_at REAL, result_json TEXT,
                created_at REAL);
            INSERT INTO runtime_state VALUES(1, '{"update_result":"success"}', 1000);
            INSERT INTO commands VALUES('update', 'completed', 1100,
                '{"success":true}', 1000);
            INSERT INTO commands VALUES('refresh', 'running', 0, '{}', 1200);
        """)


def test_storage_status_is_read_only_redacted_and_reports_schema(
    isolated_config, monkeypatch, tmp_path,
):
    root = tmp_path / "private-user" / "runtime" / "free-stockdb"
    control = root / ".quantmaster-control.sqlite"
    _control_database(control)
    isolated_config.data.free_stockdb_root = str(root)
    monkeypatch.delenv("QM_FREE_STOCKDB_CONTROL_PATH", raising=False)
    monkeypatch.setattr(
        "quantmaster.server.storage_status._owner_writer_count", lambda _root: 0,
    )
    before = control.stat()

    result = storage_status()

    after = control.stat()
    assert result == {
        **result,
        "purpose": "核心数据与 StockDB 控制状态",
        "instance": "configured-runtime",
        "access": "read-write",
        "diagnostic_access": "read-only",
        "display_path": "<configured-instance>/.quantmaster-control.sqlite",
        "database": ".quantmaster-control.sqlite",
        "journal_mode": "delete",
        "wal_present": False,
        "shm_present": False,
        "journal_present": False,
        "active_writers": 0,
        "affected_tasks": ["refresh"],
        "diagnostic_code": "SQLITE_OK",
        "quick_check": "ok",
    }
    assert result["free_bytes"] > 0
    assert result["estimated_bytes"] == control.stat().st_size
    assert result["last_success_at"]
    assert str(tmp_path) not in json.dumps(result)
    assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
    assert not control.with_name(f"{control.name}-wal").exists()
    assert not control.with_name(f"{control.name}-shm").exists()


def test_storage_status_reports_wal_without_consuming_sidecars(
    isolated_config, monkeypatch, tmp_path,
):
    root = tmp_path / "stockdb"
    control = root / ".quantmaster-control.sqlite"
    _control_database(control)
    wal = control.with_name(f"{control.name}-wal")
    shm = control.with_name(f"{control.name}-shm")
    wal.write_bytes(b"")
    shm.write_bytes(b"shared-memory")
    isolated_config.data.free_stockdb_root = str(root)
    monkeypatch.delenv("QM_FREE_STOCKDB_CONTROL_PATH", raising=False)
    monkeypatch.setattr(
        "quantmaster.server.storage_status._owner_writer_count", lambda _root: 1,
    )
    monkeypatch.setattr(
        "quantmaster.server.storage_status.connect_sqlite_diagnostic",
        lambda _path: (_ for _ in ()).throw(AssertionError("database was opened")),
    )
    before = {path: path.read_bytes() for path in (control, wal, shm)}

    result = storage_status()

    assert result["status"] == "degraded"
    assert result["wal_present"] is True
    assert result["shm_present"] is True
    assert result["active_writers"] == 1
    assert result["diagnostic_code"] == "WAL_REQUIRES_QUIESCENT_CHECK"
    assert {path: path.read_bytes() for path in (control, wal, shm)} == before


def test_storage_status_resolves_relative_stockdb_root_from_config_not_cwd(
    isolated_config, monkeypatch, tmp_path,
):
    config_root = tmp_path / "configured-workspace"
    unrelated_cwd = tmp_path / "unrelated-cwd"
    root = config_root / "runtime" / "free-stockdb"
    control = root / ".quantmaster-control.sqlite"
    _control_database(control)
    unrelated_cwd.mkdir()
    isolated_config.workspace_root = config_root.resolve()
    isolated_config.config_path = None
    isolated_config.data.free_stockdb_root = "runtime/free-stockdb"
    monkeypatch.chdir(unrelated_cwd)
    monkeypatch.delenv("QM_FREE_STOCKDB_CONTROL_PATH", raising=False)
    monkeypatch.setattr(
        "quantmaster.server.storage_status._owner_writer_count", lambda _root: 0,
    )

    result = storage_status()

    assert result["diagnostic_code"] == "SQLITE_OK"
    assert result["database"] == ".quantmaster-control.sqlite"
    assert result["display_path"] == "<configured-instance>/.quantmaster-control.sqlite"
    assert not (unrelated_cwd / "runtime" / "free-stockdb").exists()


def test_runtime_status_exposes_storage_contract_without_absolute_path(
    isolated_config, monkeypatch,
):
    from quantmaster.server.readiness import runtime_status

    # Patch the source function imported inside runtime_status.
    monkeypatch.setattr(
        "quantmaster.server.storage_status.storage_status",
        lambda: {
            "status": "ready", "purpose": "runtime", "instance": "test",
            "access": "read-write", "display_path": "<instance>/control.sqlite",
            "free_bytes": 1, "estimated_bytes": 2, "database": "control.sqlite",
            "journal_mode": "wal", "wal_present": True, "active_writers": 1,
            "last_success_at": "", "last_error": "", "affected_tasks": [],
            "diagnostic_code": "ACTIVE_WRITER",
        },
    )

    storage = runtime_status()["storage"]

    assert storage["display_path"] == "<instance>/control.sqlite"
    assert "data_root" not in storage
    assert set((
        "purpose", "instance", "access", "free_bytes", "estimated_bytes",
        "database", "journal_mode", "wal_present", "active_writers",
        "last_success_at", "last_error", "affected_tasks", "diagnostic_code",
    )) <= storage.keys()


def test_diagnostics_api_exposes_only_redacted_storage_contract(
    isolated_config, monkeypatch, tmp_path,
):
    from quantmaster.server import diagnostics as diagnostics_module

    private = str(tmp_path / "private-user")
    projection = {
        "status": "degraded", "purpose": "runtime-control",
        "instance": "configured-runtime", "access": "read-write",
        "diagnostic_access": "read-only",
        "display_path": "<configured-instance>/control.sqlite",
        "free_bytes": 10, "estimated_bytes": 20,
        "database": "control.sqlite", "journal_mode": "delete",
        "wal_present": False, "shm_present": False, "journal_present": False,
        "active_writers": 1, "last_success_at": "2026-08-13T00:00:00+00:00",
        "last_error": "只读检查待复验", "affected_tasks": ["update"],
        "diagnostic_code": "ACTIVE_WRITER", "quick_check": "ok",
    }
    monkeypatch.setattr(
        "quantmaster.server.storage_status.storage_status", lambda: dict(projection),
    )
    diagnostics_module._cached = None
    diagnostics_module._refresh()

    payload = TestClient(app).get("/api/v1/diagnostics").json()
    storage = payload["runtime"]["storage"]

    assert storage == projection
    assert private not in json.dumps(payload, ensure_ascii=False)
    assert "data_root" not in storage
