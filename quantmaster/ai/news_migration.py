"""Explicit, evidence-bounded migration for retired news database contracts."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from quantmaster.ai.crawler import _normalize_sectors
from quantmaster.ai.news_sources import NewsSourceStore
from quantmaster.ai.news_storage import (
    NEWS_SCHEMA_VERSION,
    migrate_legacy_news_schema,
    require_current_news_schema,
)
from quantmaster.runtime.sqlite import connect_sqlite

try:
    from quantmaster.data.legacy_migration import MigrationRecord
except ModuleNotFoundError:  # The shared runner is integrated as an independent task.
    @dataclass(frozen=True)
    class MigrationRecord:  # type: ignore[no-redef]
        record_key: str
        outcome: str
        diagnostic_code: str = ""
        unknown_fields: tuple[str, ...] = ()
        detail: str = ""


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


class NewsContractMigrator:
    """Migrate only ``news.sqlite`` current rows; archives are never counted twice."""

    name = "news"

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
                    connection, industry_map={}, normalize_sectors=_normalize_sectors,
                )
                needs_source_schema = True
        if needs_source_schema:
            # Source DDL belongs to this explicit migration, never store construction.
            NewsSourceStore(path, initialize=True)
        with closing(connect_sqlite(path, row_factory=True)) as connection:
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
        destination = self._path(root)
        with closing(connect_sqlite(source, read_only=True)) as backup:
            with closing(connect_sqlite(destination)) as current:
                backup.backup(current)


news_contract_migrator = NewsContractMigrator()
