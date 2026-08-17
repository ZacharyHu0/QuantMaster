from __future__ import annotations

import gc
import sqlite3

import pytest

from quantmaster.data.migration import StoreSchemaMigrator, backup_sqlite_tree
from quantmaster.lab.store import LabSchemaMigrationRequired, LabStore
from quantmaster.rotation.store import RotationSchemaMigrationRequired, RotationStore


def _legacy_stores(root):
    LabStore(root / "lab.sqlite")
    RotationStore(root / "rotation")
    with sqlite3.connect(root / "lab.sqlite") as connection:
        connection.execute("PRAGMA user_version=10")
    with sqlite3.connect(root / "rotation" / "cache.sqlite") as connection:
        connection.execute("PRAGMA user_version=5")


def test_existing_store_constructors_are_strict_and_do_not_upgrade(tmp_path):
    _legacy_stores(tmp_path)

    with pytest.raises(LabSchemaMigrationRequired):
        LabStore(tmp_path / "lab.sqlite")
    with pytest.raises(RotationSchemaMigrationRequired):
        RotationStore(tmp_path / "rotation")

    with sqlite3.connect(tmp_path / "lab.sqlite") as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 10
    with sqlite3.connect(tmp_path / "rotation" / "cache.sqlite") as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 5


def test_explicit_store_migrator_applies_and_is_idempotent(tmp_path):
    _legacy_stores(tmp_path)
    migrator = StoreSchemaMigrator()

    inspected = list(migrator.inspect(tmp_path))
    assert [record.record_key for record in inspected] == [
        "schema:lab", "schema:rotation-cache",
    ]
    assert {record.outcome for record in inspected} == {"review"}
    first = list(migrator.migrate_batch(tmp_path, after_key="", limit=1))
    second = list(migrator.migrate_batch(
        tmp_path, after_key=first[-1].record_key, limit=2,
    ))
    assert [record.outcome for record in [*first, *second]] == ["converted", "converted"]
    assert list(migrator.inspect(tmp_path)) == []

    LabStore(tmp_path / "lab.sqlite")
    RotationStore(tmp_path / "rotation")


def test_unknown_store_schema_is_conflict_and_never_guessed(tmp_path):
    with sqlite3.connect(tmp_path / "lab.sqlite") as connection:
        connection.execute("CREATE TABLE unrelated_payload(value TEXT)")
        connection.execute("INSERT INTO unrelated_payload VALUES ('preserve-me')")

    migrator = StoreSchemaMigrator()
    record = next(iter(migrator.inspect(tmp_path)))
    assert (record.outcome, record.diagnostic_code) == (
        "conflict", "lab_schema_generation_unclassified",
    )
    assert "factor_definitions" in record.unknown_fields
    applied = list(migrator.migrate_batch(tmp_path, after_key="", limit=1))
    assert applied[0].outcome == "conflict"
    with sqlite3.connect(tmp_path / "lab.sqlite") as connection:
        assert connection.execute("SELECT value FROM unrelated_payload").fetchone()[0] == (
            "preserve-me"
        )


def test_current_schema_damage_is_conflict_not_legacy_fallback(tmp_path):
    LabStore(tmp_path / "lab.sqlite")
    with sqlite3.connect(tmp_path / "lab.sqlite") as connection:
        connection.execute("ALTER TABLE lab_worker_results DROP COLUMN telemetry_json")

    with pytest.raises(LabSchemaMigrationRequired):
        LabStore(tmp_path / "lab.sqlite")
    record = next(iter(StoreSchemaMigrator().inspect(tmp_path)))
    assert record.outcome == "conflict"
    assert record.diagnostic_code == "current_lab_schema_corrupt"


def test_store_schema_rollback_restores_pre_migration_versions(tmp_path):
    _legacy_stores(tmp_path)
    migrator = StoreSchemaMigrator()
    backup = tmp_path / "backup"
    backup_sqlite_tree(tmp_path, backup, extra_paths=migrator.backup_paths)
    list(migrator.migrate_batch(tmp_path, after_key="", limit=3))
    gc.collect()

    migrator.rollback(tmp_path, backup)

    with pytest.raises(LabSchemaMigrationRequired):
        LabStore(tmp_path / "lab.sqlite")
    with pytest.raises(RotationSchemaMigrationRequired):
        RotationStore(tmp_path / "rotation")
