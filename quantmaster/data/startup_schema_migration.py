"""One-shot schema upgrades retired from normal store construction."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from contextlib import closing
from pathlib import Path

from quantmaster.data.migration_contracts import MigrationRecord
from quantmaster.runtime.sqlite import connect_sqlite

_DOMAINS = (
    ("backtests", "backtests.sqlite"),
    ("jobs", "jobs.sqlite"),
    ("paper", "paper.sqlite"),
)


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _probe_jobs(connection: sqlite3.Connection) -> tuple[str, str, tuple[str, ...]]:
    tables = _tables(connection)
    core = {
        "runtime_jobs", "runtime_job_events", "runtime_job_artifacts",
        "runtime_artifact_repairs",
    }
    required_job_columns = {
        "business_key", "input_fingerprint", "algorithm_version", "lease_token",
        "llm_scope", "llm_revision", "cancellation_reason", "trigger_count",
        "coalesced_count", "last_trigger_at", "next_retry_at", "waiting_on",
        "diagnostic_code", "last_completed_unit_at",
    }
    required_artifact_columns = {"external_path", "payload_bytes"}
    row = connection.execute(
        "SELECT value FROM runtime_store_meta WHERE key='schema_version'"
    ).fetchone() if "runtime_store_meta" in tables else None
    if row is not None and str(row[0]) == "1":
        missing = tuple(sorted(
            (core | {"runtime_store_meta"}) - tables
            | required_job_columns - _columns(connection, "runtime_jobs")
            | required_artifact_columns - _columns(connection, "runtime_job_artifacts")
        ))
        return (
            ("current", "", ()) if not missing
            else ("conflict", "current_jobs_schema_corrupt", missing)
        )
    if "runtime_store_meta" in tables:
        return ("conflict", "jobs_schema_version_unclassified", ("schema_version",))
    missing_core = tuple(sorted(core - tables))
    if missing_core:
        return ("conflict", "jobs_schema_generation_unclassified", missing_core)
    return ("upgrade", "startup_schema_upgrade_required", ())


def _probe_backtests(connection: sqlite3.Connection) -> tuple[str, str, tuple[str, ...]]:
    tables = _tables(connection)
    core = {"backtest_runs", "backtest_events"}
    row = connection.execute(
        "SELECT value FROM backtest_store_meta WHERE key='schema_version'"
    ).fetchone() if "backtest_store_meta" in tables else None
    if row is not None and str(row[0]) == "1":
        missing = tuple(sorted((core | {"backtest_store_meta"}) - tables))
        return (
            ("current", "", ()) if not missing
            else ("conflict", "current_backtests_schema_corrupt", missing)
        )
    if "backtest_store_meta" in tables:
        return ("conflict", "backtests_schema_version_unclassified", ("schema_version",))
    missing_core = tuple(sorted(core - tables))
    if missing_core:
        return ("conflict", "backtests_schema_generation_unclassified", missing_core)
    return ("upgrade", "startup_schema_upgrade_required", ())


def _probe_paper(connection: sqlite3.Connection) -> tuple[str, str, tuple[str, ...]]:
    from quantmaster.schema_access import schema_target

    version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    paper_schema_version = int(schema_target("paper_schema_version"))
    tables = _tables(connection)
    core = {"paper_accounts", "paper_cycles", "paper_orders", "paper_auto_runs"}
    current_tables = core | {"paper_legacy_imports"}
    account_columns = {
        "strategy_warning", "runtime_warning", "strategy_effective_after",
    }
    run_columns = {"lease_token", "heartbeat_at", "failure_code"}
    if version == paper_schema_version:
        missing = tuple(sorted(
            current_tables - tables
            | account_columns - _columns(connection, "paper_accounts")
            | run_columns - _columns(connection, "paper_auto_runs")
        ))
        return (
            ("current", "", ()) if not missing
            else ("conflict", "current_paper_schema_corrupt", missing)
        )
    if version not in range(paper_schema_version):
        return ("conflict", "paper_schema_version_unclassified", ("user_version",))
    missing_core = tuple(sorted(core - tables))
    if missing_core:
        return ("conflict", "paper_schema_generation_unclassified", missing_core)
    return ("upgrade", "startup_schema_upgrade_required", ())


def _probe(domain: str, connection: sqlite3.Connection) -> tuple[str, str, tuple[str, ...]]:
    return {
        "jobs": _probe_jobs,
        "backtests": _probe_backtests,
        "paper": _probe_paper,
    }[domain](connection)


def _record(
    domain: str, *, outcome: str, diagnostic_code: str = "",
    unknown_fields: tuple[str, ...] = (),
) -> MigrationRecord:
    converted = outcome == "converted"
    return MigrationRecord(
        record_key=f"schema:{domain}",
        outcome=outcome,
        diagnostic_code=diagnostic_code or (
            "startup_schema_upgraded" if converted else "startup_schema_upgrade_required"
        ),
        unknown_fields=unknown_fields,
        detail=(
            f"{domain} schema 已显式升级" if converted
            else f"{domain} schema 需要显式升级或人工确认"
        ),
    )


def _upgrade(domain: str, path: Path, root: Path) -> None:
    if domain == "jobs":
        from quantmaster.runtime.jobs import UnifiedJobStore

        store = UnifiedJobStore.__new__(UnifiedJobStore)
        store.path = path
        store.read_only = False
        store.artifacts_root = root / "derived" / "job-artifacts"
        store._migrate_legacy_schema()
    elif domain == "backtests":
        from quantmaster.schema_access import schema_factory

        store_type = schema_factory("backtest_store")
        store = store_type.__new__(store_type)
        store.path = path
        store.read_only = False
        store.artifact_root = root / "backtests"
        store._migrate_legacy_schema()
    else:
        from quantmaster.schema_access import schema_target

        schema_target("paper_store").migrate_legacy_database(path, root / "paper_accounts")


class StartupSchemaMigrator:
    name = "startup-schemas"
    backup_paths = ("backtests.sqlite", "jobs.sqlite", "paper.sqlite")

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        records: list[MigrationRecord] = []
        for domain, filename in _DOMAINS:
            path = root / filename
            if not path.is_file():
                continue
            with closing(connect_sqlite(path, read_only=True)) as connection:
                status, diagnostic, unknown_fields = _probe(domain, connection)
                if status != "current":
                    records.append(_record(
                        domain, outcome="review" if status == "upgrade" else "conflict",
                        diagnostic_code=diagnostic, unknown_fields=unknown_fields,
                    ))
        return tuple(records)

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        selected: list[tuple[str, str]] = []
        for domain, filename in _DOMAINS:
            path = root / filename
            if f"schema:{domain}" <= after_key or not path.is_file():
                continue
            with closing(connect_sqlite(path, read_only=True)) as connection:
                status, diagnostic, unknown_fields = _probe(domain, connection)
                if status != "current":
                    selected.append((domain, filename))
            if len(selected) >= max(1, int(limit)):
                break
        records: list[MigrationRecord] = []
        for domain, filename in selected:
            path = root / filename
            with closing(connect_sqlite(path, read_only=True)) as connection:
                status, diagnostic, unknown_fields = _probe(domain, connection)
            if status == "upgrade":
                _upgrade(domain, path, root)
                records.append(_record(domain, outcome="converted"))
            else:
                records.append(_record(
                    domain, outcome="conflict", diagnostic_code=diagnostic,
                    unknown_fields=unknown_fields,
                ))
        return tuple(records)

    def rollback(self, root: Path, backup_root: Path) -> None:
        from quantmaster.data.migration import restore_backup_path

        for _domain, filename in _DOMAINS:
            restore_backup_path(root, backup_root, filename)


startup_schema_migrator = StartupSchemaMigrator()
