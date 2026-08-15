from __future__ import annotations

import json
import sqlite3
from contextlib import closing

from quantmaster.data.job_migration import DataJobLegacyMigrator
from quantmaster.data.maintenance import DataRefreshManager
from quantmaster.data.migration import backup_sqlite_tree
from quantmaster.data.repair import DataRepairManager
from quantmaster.runtime.jobs import UnifiedJobRuntime, UnifiedJobStore


def _legacy_refresh(path) -> None:
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript("""
            CREATE TABLE refresh_jobs (
                id TEXT PRIMARY KEY,status TEXT NOT NULL,scope TEXT NOT NULL,
                universe_name TEXT NOT NULL,start_date TEXT NOT NULL,end_date TEXT NOT NULL,
                symbols_json TEXT NOT NULL,next_index INTEGER NOT NULL,total INTEGER NOT NULL,
                succeeded INTEGER NOT NULL,failed INTEGER NOT NULL,failures_json TEXT NOT NULL,
                current_symbol TEXT NOT NULL,cancel_requested INTEGER NOT NULL,
                created_at REAL NOT NULL,updated_at REAL NOT NULL,owner TEXT NOT NULL,
                lease_expires REAL NOT NULL,heartbeat_at REAL NOT NULL,attempt INTEGER NOT NULL,
                original_symbols_json TEXT NOT NULL
            );
            CREATE TABLE refresh_failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,symbol TEXT NOT NULL,error TEXT NOT NULL
            );
            CREATE TABLE refresh_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,event_json TEXT NOT NULL,created_at REAL NOT NULL
            );
        """)
        connection.execute(
            "INSERT INTO refresh_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "refresh-1", "completed_with_errors", "market", "", "2026-01-01",
                "2026-08-15", json.dumps(["600000.SH", "000001.SZ"]), 2, 2, 1, 1,
                "[]", "", 0, 1_700_000_000.0, 1_700_000_100.0, "", 0.0, 0.0, 2,
                json.dumps(["600000.SH", "000001.SZ"]),
            ),
        )
        connection.execute(
            "INSERT INTO refresh_failures(job_id,attempt,symbol,error) VALUES (?,?,?,?)",
            ("refresh-1", 2, "000001.SZ", "offline"),
        )
        connection.execute(
            "INSERT INTO refresh_events(job_id,attempt,event_json,created_at) VALUES (?,?,?,?)",
            ("refresh-1", 2, '{"type":"completed_with_errors","failed":1}', 1_700_000_100.0),
        )
        connection.commit()


def _legacy_repairs(path) -> None:
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript("""
            CREATE TABLE data_repairs (
                id TEXT PRIMARY KEY,kind TEXT NOT NULL,target TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,source TEXT NOT NULL,status TEXT NOT NULL,
                reason TEXT NOT NULL,spec_json TEXT NOT NULL,attempt INTEGER NOT NULL,
                max_attempts INTEGER NOT NULL,next_run REAL NOT NULL,
                cancel_requested INTEGER NOT NULL,owner TEXT NOT NULL,lease_expires REAL NOT NULL,
                last_error TEXT NOT NULL,result_json TEXT NOT NULL,created_at REAL NOT NULL,
                updated_at REAL NOT NULL,completed_at REAL NOT NULL
            );
            CREATE TABLE data_repair_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,repair_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,event_json TEXT NOT NULL,created_at REAL NOT NULL
            );
            CREATE TABLE data_repair_budget (
                day TEXT NOT NULL,source TEXT NOT NULL,attempts INTEGER NOT NULL,
                PRIMARY KEY(day,source)
            );
        """)
        connection.execute(
            "INSERT INTO data_repairs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "repair-1", "api_cache", "cache-key", "legacy-key", "provider",
                "quarantined", "hash mismatch", '{"root":"cache","path":"cache/item"}',
                1, 3, 0.0, 0, "", 0.0, "", '{"state":"quarantined"}',
                1_700_000_000.0, 1_700_000_100.0, 1_700_000_100.0,
            ),
        )
        connection.execute(
            "INSERT INTO data_repair_events(repair_id,attempt,event_json,created_at) "
            "VALUES (?,?,?,?)",
            ("repair-1", 1, '{"type":"claimed","owner":"legacy"}', 1_700_000_010.0),
        )
        connection.commit()


def test_data_job_migration_preserves_refresh_and_repair_domain_facts(tmp_path):
    _legacy_refresh(tmp_path / "data_refresh.sqlite")
    _legacy_repairs(tmp_path / "data_repairs.sqlite")
    migrator = DataJobLegacyMigrator()

    assert [record.record_key for record in migrator.inspect(tmp_path)] == [
        "data-refresh", "data-repair",
    ]
    assert [record.outcome for record in migrator.migrate_batch(
        tmp_path, after_key="", limit=2,
    )] == ["converted", "converted"]

    store = UnifiedJobStore(tmp_path / "jobs.sqlite", read_only=True)
    refresh = DataRefreshManager(UnifiedJobRuntime(store, dispatch=False)).get("refresh-1")
    repair = DataRepairManager(runtime=UnifiedJobRuntime(store, dispatch=False)).get("repair-1")
    assert refresh["status"] == "completed"
    assert refresh["outcome"] == "completed_with_warnings"
    assert refresh["failures"] == [{"symbol": "000001.SZ", "error": "offline"}]
    assert repair["status"] == "completed"
    assert repair["outcome"] == "quarantined"
    assert repair["result"] == {"state": "quarantined"}
    assert any(event["type"] == "job_started" for event in store.events("repair-1"))
    assert list(migrator.inspect(tmp_path)) == []
    assert not (tmp_path / "data_refresh.sqlite").exists()
    assert not (tmp_path / "data_repairs.sqlite").exists()


def test_data_job_migration_rejects_unknown_schema_before_writing(tmp_path):
    _legacy_repairs(tmp_path / "data_repairs.sqlite")
    with closing(sqlite3.connect(tmp_path / "data_repairs.sqlite")) as connection:
        connection.execute("ALTER TABLE data_repairs ADD COLUMN mystery TEXT")
        connection.commit()

    records = list(DataJobLegacyMigrator().migrate_batch(
        tmp_path, after_key="", limit=2,
    ))

    assert len(records) == 1
    assert records[0].outcome == "conflict"
    assert records[0].unknown_fields == ("mystery",)
    assert not (tmp_path / "jobs.sqlite").exists()
    with closing(sqlite3.connect(tmp_path / "data_repairs.sqlite")) as connection:
        assert connection.execute("SELECT COUNT(*) FROM data_repairs").fetchone()[0] == 1


def test_data_job_migration_rejects_unknown_status_before_writing(tmp_path):
    _legacy_repairs(tmp_path / "data_repairs.sqlite")
    with closing(sqlite3.connect(tmp_path / "data_repairs.sqlite")) as connection:
        connection.execute("UPDATE data_repairs SET status='future_state'")
        connection.commit()

    records = list(DataJobLegacyMigrator().migrate_batch(
        tmp_path, after_key="", limit=2,
    ))

    assert records[0].outcome == "conflict"
    assert records[0].unknown_fields == ("status:future_state",)
    assert not (tmp_path / "jobs.sqlite").exists()
    assert (tmp_path / "data_repairs.sqlite").is_file()


def test_data_job_migration_backup_rolls_back_without_losing_legacy_rows(tmp_path):
    root = tmp_path / "data"
    root.mkdir(exist_ok=True)
    _legacy_refresh(root / "data_refresh.sqlite")
    backup = tmp_path / "backup"
    migrator = DataJobLegacyMigrator()
    backup_sqlite_tree(root, backup, extra_paths=migrator.backup_paths)
    list(migrator.migrate_batch(root, after_key="", limit=2))

    migrator.rollback(root, backup)

    assert not (root / "jobs.sqlite").exists()
    with closing(sqlite3.connect(root / "data_refresh.sqlite")) as connection:
        row = connection.execute("SELECT id,status FROM refresh_jobs").fetchone()
    assert row == ("refresh-1", "completed_with_errors")
