from __future__ import annotations

import gc
import sqlite3

import pytest

from quantmaster.backtest.paper_accounts import (
    PaperSchemaMigrationRequired,
    PaperStore,
)
from quantmaster.backtest.workbench import (
    BacktestSchemaMigrationRequired,
    BacktestStore,
)
from quantmaster.data.startup_schema_migration import StartupSchemaMigrator
from quantmaster.runtime.jobs import JobSchemaMigrationRequired, UnifiedJobStore


def _strip_current_markers(root):
    with sqlite3.connect(root / "jobs.sqlite") as connection:
        connection.execute("DROP TABLE runtime_store_meta")
    with sqlite3.connect(root / "backtests.sqlite") as connection:
        connection.execute("DROP TABLE backtest_store_meta")
    with sqlite3.connect(root / "paper.sqlite") as connection:
        connection.execute("PRAGMA user_version=4")


def _legacy_databases(root):
    UnifiedJobStore(root / "jobs.sqlite")
    BacktestStore(root / "backtests.sqlite", root / "backtests")
    PaperStore(root / "paper.sqlite", root / "paper_accounts")
    _strip_current_markers(root)


def test_existing_store_constructors_are_read_only_strict(tmp_path):
    _legacy_databases(tmp_path)

    with pytest.raises(JobSchemaMigrationRequired):
        UnifiedJobStore(tmp_path / "jobs.sqlite")
    with pytest.raises(BacktestSchemaMigrationRequired):
        BacktestStore(tmp_path / "backtests.sqlite", tmp_path / "backtests")
    with pytest.raises(PaperSchemaMigrationRequired):
        PaperStore(tmp_path / "paper.sqlite", tmp_path / "paper_accounts")

    with sqlite3.connect(tmp_path / "jobs.sqlite") as connection:
        assert "runtime_store_meta" not in {
            row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    with sqlite3.connect(tmp_path / "paper.sqlite") as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 4


def test_startup_schema_migrator_inspects_applies_and_is_idempotent(tmp_path):
    _legacy_databases(tmp_path)
    migrator = StartupSchemaMigrator()

    inspected = list(migrator.inspect(tmp_path))
    assert [record.record_key for record in inspected] == [
        "schema:backtests", "schema:jobs", "schema:paper",
    ]
    assert {record.outcome for record in inspected} == {"review"}
    assert {record.diagnostic_code for record in inspected} == {
        "startup_schema_upgrade_required"
    }
    first = list(migrator.migrate_batch(tmp_path, after_key="", limit=2))
    second = list(migrator.migrate_batch(
        tmp_path, after_key=first[-1].record_key, limit=2,
    ))
    assert [record.outcome for record in [*first, *second]] == [
        "converted", "converted", "converted",
    ]
    assert list(migrator.migrate_batch(
        tmp_path, after_key=second[-1].record_key, limit=2,
    )) == []
    assert list(migrator.inspect(tmp_path)) == []

    UnifiedJobStore(tmp_path / "jobs.sqlite")
    BacktestStore(tmp_path / "backtests.sqlite", tmp_path / "backtests")
    PaperStore(tmp_path / "paper.sqlite", tmp_path / "paper_accounts")


def test_startup_schema_rollback_restores_pre_migration_versions(tmp_path):
    _legacy_databases(tmp_path)
    backup = tmp_path / "backup"
    migrator = StartupSchemaMigrator()
    from quantmaster.data.migration import backup_sqlite_tree

    backup_sqlite_tree(tmp_path, backup, extra_paths=migrator.backup_paths)
    list(migrator.migrate_batch(tmp_path, after_key="", limit=3))
    gc.collect()

    migrator.rollback(tmp_path, backup)

    with pytest.raises(JobSchemaMigrationRequired):
        UnifiedJobStore(tmp_path / "jobs.sqlite")
    with pytest.raises(BacktestSchemaMigrationRequired):
        BacktestStore(tmp_path / "backtests.sqlite", tmp_path / "backtests")
    with pytest.raises(PaperSchemaMigrationRequired):
        PaperStore(tmp_path / "paper.sqlite", tmp_path / "paper_accounts")


def test_current_schema_damage_is_reported_and_never_falls_back(tmp_path):
    PaperStore(tmp_path / "paper.sqlite", tmp_path / "paper_accounts")
    with sqlite3.connect(tmp_path / "paper.sqlite") as connection:
        connection.execute("ALTER TABLE paper_auto_runs DROP COLUMN failure_code")

    with pytest.raises(PaperSchemaMigrationRequired):
        PaperStore(tmp_path / "paper.sqlite", tmp_path / "paper_accounts")
    migrator = StartupSchemaMigrator()
    inspected = list(migrator.inspect(tmp_path))
    assert [(record.outcome, record.diagnostic_code, record.unknown_fields) for record in inspected] == [
        ("conflict", "current_paper_schema_corrupt", ("failure_code",))
    ]
    applied = list(migrator.migrate_batch(tmp_path, after_key="", limit=3))
    assert [(record.outcome, record.diagnostic_code) for record in applied] == [
        ("conflict", "current_paper_schema_corrupt")
    ]
    with pytest.raises(PaperSchemaMigrationRequired):
        PaperStore(tmp_path / "paper.sqlite", tmp_path / "paper_accounts")


def test_unknown_database_is_not_guessed_as_a_jobs_generation(tmp_path):
    with sqlite3.connect(tmp_path / "jobs.sqlite") as connection:
        connection.execute("CREATE TABLE unrelated_payload(value TEXT)")
        connection.execute("INSERT INTO unrelated_payload VALUES ('preserve-me')")

    migrator = StartupSchemaMigrator()
    record = next(iter(migrator.inspect(tmp_path)))
    assert (record.outcome, record.diagnostic_code) == (
        "conflict", "jobs_schema_generation_unclassified",
    )
    assert "runtime_jobs" in record.unknown_fields
    applied = list(migrator.migrate_batch(tmp_path, after_key="", limit=1))
    assert applied[0].outcome == "conflict"
    with sqlite3.connect(tmp_path / "jobs.sqlite") as connection:
        assert connection.execute("SELECT value FROM unrelated_payload").fetchone()[0] == (
            "preserve-me"
        )
        assert "runtime_jobs" not in {
            row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
