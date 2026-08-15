"""Explicit one-shot upgrades retired from Lab and rotation constructors."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from quantmaster.data.legacy_migration import MigrationRecord
from quantmaster.runtime.sqlite import connect_sqlite


@dataclass(frozen=True)
class _SchemaTarget:
    key: str
    path: Callable[[Path], Path]
    current_version: int
    core_tables: frozenset[str]


LAB = _SchemaTarget(
    "lab", lambda root: root / "lab.sqlite", 12,
    frozenset({
        "factor_definitions", "factor_versions", "lab_worker_results", "deployments",
    }),
)
ROTATION_CACHE = _SchemaTarget(
    "rotation-cache", lambda root: root / "rotation" / "cache.sqlite", 6,
    frozenset({"snapshots", "taxonomy_nodes", "theme_catalog", "runtime_state"}),
)
ROTATION_PREFERENCES = _SchemaTarget(
    "rotation-preferences", lambda root: root / "rotation" / "preferences.sqlite", 1,
    frozenset({"preferences"}),
)


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0]) for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }


def _probe(target: _SchemaTarget, path: Path) -> tuple[str, str, tuple[str, ...]]:
    with closing(connect_sqlite(path, read_only=True)) as connection:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        tables = _tables(connection)
        missing_columns: set[str] = set()
        if target is LAB and "factor_definitions" in tables:
            definitions = {
                str(row[1]) for row in connection.execute(
                    "PRAGMA table_info(factor_definitions)"
                )
            }
            missing_columns |= {"name_key"} - definitions
            if "lab_jobs" in tables:
                jobs = {
                    str(row[1]) for row in connection.execute("PRAGMA table_info(lab_jobs)")
                }
                missing_columns |= {
                    "error_code", "error_json", "telemetry_json", "cancellation_reason",
                } - jobs
            if "lab_worker_results" in tables:
                results = {
                    str(row[1])
                    for row in connection.execute("PRAGMA table_info(lab_worker_results)")
                }
                missing_columns |= {
                    "job_id", "attempt", "kind", "outcome", "result_json", "error_json",
                    "telemetry_json", "content_hash", "created_at",
                } - results
        elif target is ROTATION_CACHE and "snapshot_items" in tables:
            items = {
                str(row[1]) for row in connection.execute(
                    "PRAGMA table_info(snapshot_items)"
                )
            }
            missing_columns |= {
                "flow_1", "flow_3", "flow_5", "flow_20", "daily_flow",
                "grade_1", "grade_3", "grade_5", "grade_20",
            } - items
    if version == target.current_version:
        missing = tuple(sorted(target.core_tables - tables | missing_columns))
        return (
            ("current", "", ()) if not missing
            else ("conflict", f"current_{target.key}_schema_corrupt", missing)
        )
    if version < 0 or version > target.current_version:
        return ("conflict", f"{target.key}_schema_version_unclassified", ("user_version",))
    missing = tuple(sorted(target.core_tables - tables))
    if missing:
        return ("conflict", f"{target.key}_schema_generation_unclassified", missing)
    return ("upgrade", "store_schema_upgrade_required", ())


def _record(
    target: _SchemaTarget, status: str, diagnostic: str, fields: tuple[str, ...],
    *, applied: bool = False,
) -> MigrationRecord:
    return MigrationRecord(
        record_key=f"schema:{target.key}",
        outcome="converted" if applied else "review" if status == "upgrade" else "conflict",
        diagnostic_code="store_schema_upgraded" if applied else diagnostic,
        unknown_fields=fields,
        detail=(
            f"{target.key} schema 已显式升级"
            if applied else f"{target.key} schema 需显式升级或人工确认"
        ),
    )


class StoreSchemaMigrator:
    name = "store-schemas"
    backup_paths = (
        "lab.sqlite", "rotation/cache.sqlite", "rotation/preferences.sqlite",
    )
    targets = (LAB, ROTATION_CACHE, ROTATION_PREFERENCES)

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        records: list[MigrationRecord] = []
        for target in self.targets:
            path = target.path(root)
            if not path.is_file():
                continue
            status, diagnostic, fields = _probe(target, path)
            if status != "current":
                records.append(_record(target, status, diagnostic, fields))
        return tuple(records)

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        selected = [
            target for target in self.targets
            if f"schema:{target.key}" > after_key and target.path(root).is_file()
            and _probe(target, target.path(root))[0] != "current"
        ][:max(1, int(limit))]
        records: list[MigrationRecord] = []
        for target in selected:
            status, diagnostic, fields = _probe(target, target.path(root))
            if status == "upgrade":
                self._upgrade(root, target)
                records.append(_record(target, status, diagnostic, fields, applied=True))
            else:
                records.append(_record(target, status, diagnostic, fields))
        return tuple(records)

    @staticmethod
    def _upgrade(root: Path, target: _SchemaTarget) -> None:
        if target is LAB:
            StoreSchemaMigrator._upgrade_lab(root)
            return
        StoreSchemaMigrator._upgrade_rotation(root, target)

    @staticmethod
    def _upgrade_lab(root: Path) -> None:
        from quantmaster.lab.store import LabStore

        store = LabStore.__new__(LabStore)
        store.path = LAB.path(root)
        store.read_only = False
        store._migrate_legacy_schema()

    @staticmethod
    def _upgrade_rotation(root: Path, target: _SchemaTarget) -> None:
        from quantmaster.rotation.store import RotationStore

        rotation = RotationStore.__new__(RotationStore)
        rotation.root = root / "rotation"
        rotation.read_only = False
        rotation.cache_path = ROTATION_CACHE.path(root)
        rotation.preferences_path = ROTATION_PREFERENCES.path(root)
        if target is ROTATION_CACHE:
            with rotation._cache() as connection:
                from quantmaster.runtime.sqlite import migrate_schema

                migrate_schema(connection, (
                    (1, rotation._cache_v1), (2, rotation._cache_v2),
                    (3, rotation._cache_v3), (4, rotation._cache_v4),
                    (5, rotation._cache_v5), (6, rotation._cache_v6),
                ))
        else:
            with rotation._preferences() as connection:
                from quantmaster.runtime.sqlite import migrate_schema

                migrate_schema(connection, ((1, rotation._preferences_v1),))

    def rollback(self, root: Path, backup_root: Path) -> None:
        from quantmaster.data.migration import restore_backup_path

        for relative in self.backup_paths:
            restore_backup_path(root, backup_root, relative)


store_schema_migrator = StoreSchemaMigrator()
