"""One-shot upgrades for stores that no longer migrate while being opened."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterable
from contextlib import closing
from pathlib import Path

from quantmaster.data.legacy_migration import MigrationRecord
from quantmaster.runtime.sqlite import connect_sqlite


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _probe(
    path: Path, core: set[str], current_columns: dict[str, set[str]], current_version: int,
) -> tuple[str, str, tuple[str, ...]]:
    with closing(connect_sqlite(path, read_only=True, row_factory=True)) as connection:
        tables = _tables(connection)
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        missing_tables = core - tables
        missing_columns = set().union(*(
            columns - _columns(connection, table)
            for table, columns in current_columns.items()
        ))
    if version == current_version and not missing_tables and not missing_columns:
        return "current", "", ()
    if version == 0 and not missing_tables:
        return "upgrade", "remaining_schema_upgrade_required", ()
    unknown = tuple(sorted(missing_tables | missing_columns | {"user_version"}))
    return "conflict", "remaining_schema_generation_unclassified", unknown


def _record(key: str, status: str, diagnostic: str, unknown: tuple[str, ...]) -> MigrationRecord:
    return MigrationRecord(
        record_key=key,
        outcome={"upgrade": "review", "conflict": "conflict"}.get(status, "converted"),
        diagnostic_code=diagnostic or "remaining_schema_upgraded",
        unknown_fields=unknown,
        detail=f"{key} schema {'已显式升级' if status == 'converted' else '需要升级或人工确认'}",
    )


class RemainingSchemaMigrator:
    name = "remaining-schemas"
    backup_paths = (
        "source_health.sqlite", "tushare_rate.sqlite", "bars", "fundamentals",
        "pit_execution", "ledger_default.sqlite", "ledger_paper.sqlite",
        "paper_accounts",
    )

    @staticmethod
    def _targets(root: Path) -> list[tuple[str, Path, Callable[[], None], tuple]]:
        from quantmaster.data.resilience import ProviderHealthStore, TushareRateLimiter
        from quantmaster.data.storage import BarStore
        from quantmaster.portfolio.ledger import Ledger

        targets: list[tuple[str, Path, Callable[[], None], tuple]] = []
        provider_columns = {
            "failure_class", "config_revision", "probe_started", "retry_after",
            "diagnostic_code",
        }
        fixed = (
            ("provider-health", root / "source_health.sqlite",
             lambda: ProviderHealthStore.migrate_legacy_database(root / "source_health.sqlite"),
             ({"source_health"}, {"source_health": provider_columns}, 4)),
            ("tushare-rate", root / "tushare_rate.sqlite",
             lambda: TushareRateLimiter.migrate_legacy_database(root / "tushare_rate.sqlite"),
             ({"rate_state"}, {"rate_state": {"name", "next_call"}}, 1)),
        )
        targets.extend(fixed)
        bar_roots = [root / "bars", root / "fundamentals", root / "pit_execution"]
        bar_roots += [root / "bars" / "intraday" / value for value in ("1m", "5m", "15m", "30m", "60m")]
        for bar_root in bar_roots:
            key = f"bars:{bar_root.relative_to(root).as_posix()}"
            bar_columns = {
                "coverage_start", "coverage_end", "checked_at", "last_source",
                "last_status", "content_sha256", "row_count", "file_size",
                "file_mtime_ns", "quality_json", "source_chain_json",
                "observed_start", "observed_end",
            }
            targets.append((
                key, bar_root / "meta.sqlite",
                lambda value=bar_root: BarStore.migrate_legacy_database(value),
                ({"bar_meta"}, {"bar_meta": bar_columns}, 1),
            ))
        ledgers = [root / "ledger_default.sqlite", root / "ledger_paper.sqlite"]
        accounts = root / "paper_accounts"
        if accounts.is_dir():
            ledgers.extend(sorted(accounts.glob("*/ledger.sqlite")))
        for path in ledgers:
            key = f"ledger:{path.relative_to(root).as_posix()}"
            ledger_columns = {
                "trades": {"import_batch", "fingerprint", "idempotency_key"},
                "cashflows": {"idempotency_key"},
            }
            targets.append((
                key, path, lambda value=path: Ledger.migrate_legacy_database(value),
                ({"trades", "cashflows"}, ledger_columns, 1),
            ))
        return sorted(targets, key=lambda item: item[0])

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        records = []
        for key, path, _upgrade, probe_args in self._targets(root):
            if not path.is_file():
                continue
            status, diagnostic, unknown = _probe(path, *probe_args)
            if status != "current":
                records.append(_record(key, status, diagnostic, unknown))
        return tuple(records)

    def migrate_batch(self, root: Path, *, after_key: str, limit: int) -> Iterable[MigrationRecord]:
        records = []
        for key, path, upgrade, probe_args in self._targets(root):
            if key <= after_key or not path.is_file():
                continue
            status, diagnostic, unknown = _probe(path, *probe_args)
            if status == "current":
                continue
            if status == "upgrade":
                upgrade()
                records.append(_record(key, "converted", "", ()))
            else:
                records.append(_record(key, status, diagnostic, unknown))
            if len(records) >= max(1, int(limit)):
                break
        return tuple(records)

    def rollback(self, root: Path, backup_root: Path) -> None:
        from quantmaster.data.migration import restore_backup_path, validate_backup_tree

        manifest = validate_backup_tree(backup_root)
        prefixes = ("bars/", "fundamentals/", "pit_execution/", "paper_accounts/")
        exact = {
            "source_health.sqlite", "tushare_rate.sqlite", "ledger_default.sqlite",
            "ledger_paper.sqlite",
        }
        for entry in manifest["entries"]:
            relative = str(entry["path"])
            if relative in exact or any(relative.startswith(prefix) for prefix in prefixes):
                restore_backup_path(root, backup_root, relative)


remaining_schema_migrator = RemainingSchemaMigrator()
