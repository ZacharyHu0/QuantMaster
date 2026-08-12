"""Explicit, evidence-bounded migration for retired news database contracts."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from contextlib import closing
from pathlib import Path

from quantmaster.ai.crawler import _normalize_sectors
from quantmaster.ai.news_sources import NewsSourceStore
from quantmaster.ai.news_storage import (
    NEWS_SCHEMA_VERSION,
    migrate_legacy_news_schema,
    require_current_news_schema,
)
from quantmaster.data.legacy_migration import MigrationRecord
from quantmaster.runtime.sqlite import connect_sqlite

NEWS_ARCHIVE_TABLES = (
    "news_revisions_legacy_v3",
    "news_analysis_sectors_legacy_v3",
    "news_analysis_symbols_legacy_v3",
    "news_legacy_v3",
)
_ARCHIVE_KEYS = {
    table: f"archive:{index:02d}:{table}"
    for index, table in enumerate(NEWS_ARCHIVE_TABLES, start=1)
}

_OPTIONAL_EVIDENCE_FIELDS = (
    "content_scope",
    "source_id",
    "fingerprint",
    "content_hash",
    "first_seen_at",
    "last_seen_at",
    "fetched_at",
    "published_at_epoch",
    "content_version_at",
    "analysis_updated_at",
    "factor_importance_score",
    "factor_weight_at_analysis",
)


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    if table not in {"news", "news_store_meta"}:
        raise ValueError("invalid news migration table")
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _schema_version(connection: sqlite3.Connection) -> int:
    if "news_store_meta" not in _tables(connection):
        return 0
    row = connection.execute(
        "SELECT value FROM news_store_meta WHERE key='schema_version'"
    ).fetchone()
    try:
        return int(row[0]) if row else 0
    except (TypeError, ValueError) as exc:
        raise RuntimeError("news_schema_version_invalid") from exc


def _archive_counts(connection: sqlite3.Connection) -> dict[str, int]:
    tables = _tables(connection)
    return {
        table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
        for table in NEWS_ARCHIVE_TABLES
        if table in tables
    }


def _archive_record(table: str, count: int, *, applied: bool) -> MigrationRecord:
    return MigrationRecord(
        record_key=_ARCHIVE_KEYS[table],
        outcome="converted",
        diagnostic_code=(
            "news_archive_retired" if applied else "news_archive_retirement_required"
        ),
        detail=f"table={table}; row_count={count}",
    )


def _active_runner_backup(root: Path, expected: dict[str, int]) -> Path | None:
    state = root / "legacy_contract_migrations.sqlite"
    if not state.is_file():
        return None
    try:
        with closing(connect_sqlite(state, row_factory=True, read_only=True)) as connection:
            row = connection.execute(
                "SELECT backup_path FROM migration_runs WHERE domain='news' "
                "AND mode='apply' AND status IN ('backing_up','running','pausing') "
                "ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        backup_root = Path(str(row[0])) if row and row[0] else None
        if backup_root is None:
            return None
        from quantmaster.data.migration import validate_backup_tree

        validate_backup_tree(backup_root)
        candidate = backup_root / "news.sqlite"
        if candidate is None or not candidate.is_file():
            return None
        with closing(connect_sqlite(candidate, read_only=True)) as connection:
            counts = _archive_counts(connection)
    except (OSError, RuntimeError, sqlite3.Error, ValueError):
        return None
    return candidate if all(counts.get(table) == count for table, count in expected.items()) else None


def _invalid_dimension(row: sqlite3.Row, columns: set[str]) -> MigrationRecord | None:
    for field in ("symbols", "sectors"):
        raw = row[field] if field in columns else None
        if raw in {None, ""}:
            continue
        try:
            decoded = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            code = f"news_{field}_json_invalid"
        else:
            if isinstance(decoded, list):
                continue
            code = f"news_{field}_shape_invalid"
        return MigrationRecord(
            record_key=f"news:{int(row['id']):020d}", outcome="conflict",
            diagnostic_code=code, unknown_fields=(field,),
            detail=f"资讯 {row['id']} 的 {field} 不是当前 JSON 数组；拒绝 decoder fallback",
        )
    return None


def _record(row: sqlite3.Row, columns: set[str]) -> MigrationRecord:
    unknown: list[str] = []
    for field in _OPTIONAL_EVIDENCE_FIELDS:
        if field not in columns or row[field] in {None, "", 0}:
            unknown.append(field)
    invalid = _invalid_dimension(row, columns)
    if invalid is not None:
        return invalid
    analysis_version = row["analysis_version"] if "analysis_version" in columns else None
    if analysis_version in {None, 0, 1, "", "1"}:
        for field in ("factor_importance_score", "factor_weight_at_analysis"):
            if field not in unknown:
                unknown.append(field)
    if unknown:
        return MigrationRecord(
            record_key=f"news:{int(row['id']):020d}",
            outcome="blank",
            diagnostic_code="news_optional_evidence_unavailable",
            unknown_fields=tuple(unknown),
            detail="旧记录只保留原行可确认字段；未用正文、标题、当前来源或当前行业映射补事实",
        )
    return MigrationRecord(
        record_key=f"news:{int(row['id']):020d}",
        outcome="unchanged",
        detail="记录已经满足当前资讯合同",
    )


def _retire_archive_batch(
    root: Path, connection: sqlite3.Connection, archives: dict[str, int],
    after_key: str, limit: int,
) -> tuple[MigrationRecord, ...]:
    if _active_runner_backup(root, archives) is None:
        raise RuntimeError(
            "news_archive_backup_missing: 拒绝在 runner 可恢复备份外退休归档表"
        )
    selected = [
        table for table in NEWS_ARCHIVE_TABLES
        if table in archives and _ARCHIVE_KEYS[table] > after_key
    ][:max(1, int(limit))]
    if not selected:
        return ()
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute("BEGIN IMMEDIATE")
    try:
        for table in selected:
            connection.execute(f'DROP TABLE "{table}"')
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise RuntimeError("news_archive_retirement_fk_conflict")
        connection.commit()
    except (RuntimeError, sqlite3.Error):
        connection.rollback()
        raise
    finally:
        connection.execute("PRAGMA foreign_keys=ON")
    return tuple(_archive_record(table, archives[table], applied=True) for table in selected)


class NewsContractMigrator:
    """Migrate current rows, then retire only the four exact v3 archive tables."""

    name = "news"
    backup_paths = ("news.sqlite",)

    @staticmethod
    def _path(root: Path) -> Path:
        return root / "news.sqlite"

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        path = self._path(root)
        if not path.is_file():
            return ()
        with closing(connect_sqlite(path, row_factory=True, read_only=True)) as connection:
            tables = _tables(connection)
            if "news" not in tables:
                return (
                    MigrationRecord(
                        "schema", "conflict", "news_table_missing", (),
                        "news.sqlite 不含 news 表；拒绝猜测其它表为当前语料",
                    ),
                )
            current = _schema_version(connection)
            if current > NEWS_SCHEMA_VERSION:
                return (
                    MigrationRecord(
                        "schema", "conflict", "news_schema_newer_than_runtime", (),
                        f"数据库版本 {current} 高于当前 {NEWS_SCHEMA_VERSION}",
                    ),
                )
            archives = _archive_counts(connection)
            if archives:
                return tuple(
                    _archive_record(table, archives[table], applied=False)
                    for table in NEWS_ARCHIVE_TABLES if table in archives
                )
            columns = _columns(connection, "news")
            rows = connection.execute("SELECT * FROM news ORDER BY id").fetchall()
            return tuple(_record(row, columns) for row in rows)

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        path = self._path(root)
        if not path.is_file():
            return ()
        needs_source_schema = False
        with closing(connect_sqlite(path, row_factory=True)) as connection:
            current = _schema_version(connection)
            if current > NEWS_SCHEMA_VERSION:
                raise RuntimeError("news_schema_newer_than_runtime")
            if current < NEWS_SCHEMA_VERSION:
                migrate_legacy_news_schema(
                    connection, normalize_sectors=_normalize_sectors,
                )
                needs_source_schema = True
        if needs_source_schema:
            # Source DDL belongs to this explicit migration, never store construction.
            NewsSourceStore(path, initialize=True)
        with closing(connect_sqlite(path, row_factory=True)) as connection:
            archives = _archive_counts(connection)
            if archives:
                return _retire_archive_batch(root, connection, archives, after_key, limit)
            if after_key.startswith("archive:"):
                return ()
            require_current_news_schema(connection)
            columns = _columns(connection, "news")
            last_id = 0
            if after_key.startswith("news:"):
                last_id = int(after_key.partition(":")[2])
            rows = connection.execute(
                "SELECT * FROM news WHERE id>? ORDER BY id LIMIT ?",
                (last_id, max(1, int(limit))),
            ).fetchall()
            return tuple(_record(row, columns) for row in rows)

    def rollback(self, root: Path, backup_root: Path) -> None:
        source = backup_root / "news.sqlite"
        if not source.is_file():
            raise FileNotFoundError("资讯迁移备份不存在")
        from quantmaster.data.migration import restore_backup_path

        restore_backup_path(root, backup_root, "news.sqlite")


news_contract_migrator = NewsContractMigrator()
