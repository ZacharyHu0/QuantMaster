"""Versioned news storage and SQL-side analytics.

The authoritative ``news`` rows remain the source of truth.  The symbol and
sector tables in this module are rebuildable projections used to keep overview
queries bounded in both memory and Python work.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from collections.abc import Callable, Sequence
from typing import Any

from quantmaster.ai.news_contracts import (
    article_evidence_binding_hash,
    news_content_hash,
    read_raw_evidence,
)

NEWS_SCHEMA_VERSION = 8


class NewsSchemaMigrationRequired(RuntimeError):
    """The database is not the current contract and needs the one-shot migrator."""

_NEWS_COLUMNS = {
    "importance_score": "REAL DEFAULT 0",
    # v5 separates immutable, point-in-time factor evidence from mutable
    # portfolio/watchlist alert context.  Legacy importance cannot be promoted
    # into either formal factor field because neither its analysis-time source
    # weight nor its pre-context value can be proven.
    "factor_importance_score": "REAL DEFAULT NULL",
    "factor_weight_at_analysis": "REAL DEFAULT NULL",
    "alert_importance_score": "REAL NOT NULL DEFAULT 0",
    "scope": "TEXT DEFAULT ''",
    "urgency": "TEXT DEFAULT ''",
    "confidence": "REAL DEFAULT 0",
    "sectors": "TEXT DEFAULT '[]'",
    "fingerprint": "TEXT DEFAULT NULL",
    "is_official": "INTEGER DEFAULT 0",
    "content_scope": "TEXT DEFAULT NULL",
    "source_id": "TEXT DEFAULT NULL",
    "content_hash": "TEXT DEFAULT NULL",
    "first_seen_at": "REAL DEFAULT NULL",
    "last_seen_at": "REAL DEFAULT NULL",
    "raw_cache_key": "TEXT DEFAULT ''",
    "evidence_binding_hash": "TEXT DEFAULT ''",
    "ingest_window_id": "TEXT DEFAULT ''",
    "ingest_batch_id": "TEXT DEFAULT ''",
    "analysis_status": "TEXT DEFAULT NULL",
    "analysis_attempts": "INTEGER DEFAULT 0",
    "analysis_error": "TEXT DEFAULT ''",
    "analysis_version": "INTEGER DEFAULT NULL",
    "next_retry_at": "REAL DEFAULT 0",
    "parser_version": "TEXT DEFAULT '1'",
    "analysis_recovery_count": "INTEGER DEFAULT 0",
    "last_failure_code": "TEXT DEFAULT ''",
    "analysis_updated_at": "REAL DEFAULT NULL",
    "content_version_at": "REAL DEFAULT NULL",
    "published_at_epoch": "REAL DEFAULT NULL",
    "fetched_at": "REAL DEFAULT NULL",
    "provider_item_id": "TEXT DEFAULT ''",
}


def news_fingerprint(
    source: str, title: str, url: str, published_at: str, provider_item_id: str = "",
) -> str:
    normalized_title = re.sub(r"\W+", "", title.casefold())
    identity = provider_item_id.strip() or (
        f"{url.strip().lower()}|{normalized_title}|{published_at.strip()}"
    )
    return hashlib.sha256(f"{source}|{identity}".encode()).hexdigest()


def _decode_list(value: Any) -> list[str]:
    try:
        decoded = json.loads(value or "[]")
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(decoded, list):
        return []
    return list(dict.fromkeys(str(item).strip() for item in decoded if str(item).strip()))


def replace_news_dimensions(
    connection: sqlite3.Connection,
    news_id: int,
    symbols: Sequence[Any],
    sectors: Sequence[Any],
    *, normalize_sectors: Callable[[list[Any]], list[str]],
) -> None:
    """Replace formal dimensions only from values frozen in the news row.

    The crawler freezes dimensions in the analysis transaction.  Current
    symbol classifications are never accepted by this historical write path.
    """
    normalized_symbols = list(dict.fromkeys(
        str(symbol).strip() for symbol in symbols if str(symbol).strip()
    ))
    normalized_sectors = normalize_sectors(list(sectors))
    connection.execute("DELETE FROM news_analysis_symbols WHERE news_id=?", (news_id,))
    connection.execute("DELETE FROM news_analysis_sectors WHERE news_id=?", (news_id,))
    connection.executemany(
        "INSERT INTO news_analysis_symbols(news_id,symbol) VALUES (?,?)",
        ((news_id, symbol) for symbol in normalized_symbols),
    )
    connection.executemany(
        "INSERT INTO news_analysis_sectors(news_id,sector) VALUES (?,?)",
        ((news_id, sector) for sector in normalized_sectors),
    )


def _create_news_table(connection: sqlite3.Connection, table_name: str) -> None:
    if table_name != "news":
        raise ValueError("无效的资讯表名")
    connection.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,source TEXT,title TEXT,content TEXT,"
        "url TEXT,published_at TEXT,symbols TEXT,sectors TEXT,event_type TEXT,"
        "sentiment REAL,summary TEXT,"
        "created_at REAL,importance_score REAL DEFAULT 0,"
        "factor_importance_score REAL DEFAULT NULL,"
        "factor_weight_at_analysis REAL DEFAULT NULL,"
        "alert_importance_score REAL NOT NULL DEFAULT 0,scope TEXT DEFAULT '',"
        "urgency TEXT DEFAULT '',confidence REAL DEFAULT 0,fingerprint TEXT DEFAULT '',"
        "is_official INTEGER DEFAULT 0,content_scope TEXT DEFAULT 'unknown',"
        "source_id TEXT DEFAULT '',"
        "content_hash TEXT DEFAULT '',first_seen_at REAL DEFAULT 0,last_seen_at REAL DEFAULT 0,"
        "raw_cache_key TEXT DEFAULT '',analysis_status TEXT DEFAULT 'pending',"
        "evidence_binding_hash TEXT DEFAULT '',"
        "ingest_window_id TEXT DEFAULT '',ingest_batch_id TEXT DEFAULT '',"
        "analysis_attempts INTEGER DEFAULT 0,analysis_error TEXT DEFAULT '',"
        "analysis_version INTEGER DEFAULT 1,next_retry_at REAL DEFAULT 0,"
        "parser_version TEXT DEFAULT '1',analysis_recovery_count INTEGER DEFAULT 0,"
        "last_failure_code TEXT DEFAULT '',analysis_updated_at REAL DEFAULT 0,"
        "content_version_at REAL DEFAULT 0,"
        "published_at_epoch REAL DEFAULT 0,fetched_at REAL DEFAULT 0,provider_item_id TEXT DEFAULT ''"
        ")"
    )


def _create_news_schema(connection: sqlite3.Connection, *, legacy: bool) -> None:
    _create_news_table(connection, "news")
    if legacy:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(news)")}
        for name, sql_type in _NEWS_COLUMNS.items():
            if name not in columns:
                connection.execute(f"ALTER TABLE news ADD COLUMN {name} {sql_type}")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS news_analysis_symbols ("
        "news_id INTEGER NOT NULL REFERENCES news(id) ON DELETE CASCADE,"
        "symbol TEXT NOT NULL,PRIMARY KEY(news_id,symbol))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS news_analysis_sectors ("
        "news_id INTEGER NOT NULL REFERENCES news(id) ON DELETE CASCADE,"
        "sector TEXT NOT NULL,PRIMARY KEY(news_id,sector))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS news_revisions ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,news_id INTEGER NOT NULL "
        "REFERENCES news(id) ON DELETE CASCADE,revision_number INTEGER NOT NULL,"
        "title TEXT NOT NULL,content TEXT NOT NULL,content_hash TEXT NOT NULL,"
        "raw_cache_key TEXT NOT NULL DEFAULT '',fetched_at REAL NOT NULL DEFAULT 0,"
        "evidence_binding_hash TEXT NOT NULL DEFAULT '',"
        "recorded_at REAL NOT NULL,UNIQUE(news_id,revision_number))"
    )
    revision_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(news_revisions)")
    }
    if "evidence_binding_hash" not in revision_columns:
        connection.execute(
            "ALTER TABLE news_revisions ADD COLUMN "
            "evidence_binding_hash TEXT NOT NULL DEFAULT ''"
        )
    # The workbench reads these compact payloads directly.  They are rebuilt
    # by the writer after ingest/annotation, never lazily by a Web GET, so a
    # page view cannot re-run evidence verification and window aggregation.
    connection.execute(
        "CREATE TABLE IF NOT EXISTS news_dashboard_materializations ("
        "kind TEXT NOT NULL,window_days INTEGER NOT NULL,"
        "input_fingerprint TEXT NOT NULL,snapshot_id TEXT NOT NULL,"
        "payload_json TEXT NOT NULL,generated_at REAL NOT NULL,"
        "PRIMARY KEY(kind,window_days))"
    )
    if legacy:
        return
    statements = (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_news_fingerprint_unique_v4 "
        "ON news(fingerprint) WHERE fingerprint<>''",
        "CREATE INDEX IF NOT EXISTS idx_news_recent_v4 ON news(id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_news_source_v4 ON news(source_id,id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_news_analysis_v4 "
        "ON news(analysis_status,next_retry_at,id)",
        "CREATE INDEX IF NOT EXISTS idx_news_seen_v4 ON news(first_seen_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_news_published_v4 "
        "ON news(published_at_epoch DESC,id DESC)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_news_provider_item_unique_v4 "
        "ON news(source_id,provider_item_id) WHERE provider_item_id<>''",
        "CREATE INDEX IF NOT EXISTS idx_news_stats_v4 "
        "ON news(analysis_status,first_seen_at,content_version_at,analysis_updated_at,confidence)",
        "CREATE INDEX IF NOT EXISTS idx_news_ingest_window_v7 "
        "ON news(ingest_window_id,ingest_batch_id)",
        "CREATE INDEX IF NOT EXISTS idx_news_revisions_news_v4 "
        "ON news_revisions(news_id,revision_number DESC)",
        "CREATE INDEX IF NOT EXISTS idx_news_symbol_value_v4 "
        "ON news_analysis_symbols(symbol,news_id)",
        "CREATE INDEX IF NOT EXISTS idx_news_sector_value_v4 "
        "ON news_analysis_sectors(sector,news_id)",
        "CREATE INDEX IF NOT EXISTS idx_news_dashboard_generated_v8 "
        "ON news_dashboard_materializations(generated_at DESC)",
    )
    for statement in statements:
        connection.execute(statement)


def _rebuild_legacy_title_identity_table(connection: sqlite3.Connection) -> None:
    """Rebuild v3 in-transaction; the runner backup is the sole legacy archive."""
    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='news'"
    ).fetchone()
    normalized = re.sub(r"\s+", "", str(row[0] if row else "").casefold())
    if "unique(source,title,published_at)" not in normalized:
        return
    staging_names = (
        "news_migration_v3",
        "news_analysis_symbols_migration_v3",
        "news_analysis_sectors_migration_v3",
        "news_revisions_migration_v3",
    )
    placeholders = ",".join("?" for _ in staging_names)
    conflicts = connection.execute(
        f"SELECT name FROM sqlite_master WHERE name IN ({placeholders})",
        staging_names,
    ).fetchall()
    if conflicts:
        names = ", ".join(str(item[0]) for item in conflicts)
        raise RuntimeError(f"资讯 v3 迁移发现 staging 表冲突：{names}")

    connection.execute("ALTER TABLE news RENAME TO news_migration_v3")
    connection.execute(
        "ALTER TABLE news_analysis_symbols "
        "RENAME TO news_analysis_symbols_migration_v3"
    )
    connection.execute(
        "ALTER TABLE news_analysis_sectors "
        "RENAME TO news_analysis_sectors_migration_v3"
    )
    connection.execute("ALTER TABLE news_revisions RENAME TO news_revisions_migration_v3")
    _create_news_schema(connection, legacy=False)
    column_names = [
        str(info[1]) for info in connection.execute("PRAGMA table_info(news)")
    ]
    columns_sql = ",".join(f'"{name}"' for name in column_names)
    connection.execute(
        f"INSERT INTO news({columns_sql}) "
        f"SELECT {columns_sql} FROM news_migration_v3"
    )
    connection.execute(
        "INSERT INTO news_analysis_symbols(news_id,symbol) "
        "SELECT news_id,symbol FROM news_analysis_symbols_migration_v3"
    )
    connection.execute(
        "INSERT INTO news_analysis_sectors(news_id,sector) "
        "SELECT news_id,sector FROM news_analysis_sectors_migration_v3"
    )
    connection.execute(
        "INSERT INTO news_revisions("
        "id,news_id,revision_number,title,content,content_hash,raw_cache_key,"
        "fetched_at,evidence_binding_hash,recorded_at) "
        "SELECT id,news_id,revision_number,title,content,"
        "content_hash,raw_cache_key,fetched_at,'',recorded_at "
        "FROM news_revisions_migration_v3 ORDER BY id"
    )
    for name in reversed(staging_names):
        connection.execute(f"DROP TABLE {name}")


def _rebuild_dimensions(
    connection: sqlite3.Connection,
    *,
    normalize_sectors: Callable[[list[Any]], list[str]],
) -> None:
    connection.execute("DELETE FROM news_analysis_symbols")
    connection.execute("DELETE FROM news_analysis_sectors")
    rows = connection.execute("SELECT id,symbols,sectors FROM news ORDER BY id").fetchall()
    for row in rows:
        replace_news_dimensions(
            connection,
            int(row["id"]),
            _decode_list(row["symbols"]),
            _decode_list(row["sectors"]),
            normalize_sectors=normalize_sectors,
        )


def initialize_news_schema(connection: sqlite3.Connection) -> None:
    """Create the sole current schema for a brand-new database."""
    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        "CREATE TABLE news_store_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL)"
    )
    _create_news_schema(connection, legacy=False)
    connection.execute(
        "INSERT INTO news_store_meta(key,value) VALUES ('schema_version',?)",
        (str(NEWS_SCHEMA_VERSION),),
    )


def require_current_news_schema(connection: sqlite3.Connection) -> None:
    """Validate without DDL, backfills, format guessing, or decoder fallback."""
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    required_tables = {
        "news", "news_store_meta", "news_analysis_symbols",
        "news_analysis_sectors", "news_revisions", "news_dashboard_materializations",
    }
    missing_tables = sorted(required_tables - tables)
    if missing_tables:
        raise NewsSchemaMigrationRequired(
            "资讯数据库缺少当前表，需先执行一次性迁移：" + ",".join(missing_tables)
        )
    retired_names = {
        "news_legacy_v3",
        "news_analysis_symbols_legacy_v3",
        "news_analysis_sectors_legacy_v3",
        "news_revisions_legacy_v3",
    }
    retired = sorted(retired_names & tables)
    if retired:
        raise NewsSchemaMigrationRequired(
            "资讯当前库仍含退休归档表，需迁移至外部备份：" + ",".join(retired)
        )
    row = connection.execute(
        "SELECT value FROM news_store_meta WHERE key='schema_version'"
    ).fetchone()
    try:
        current = int(row[0]) if row else 0
    except (TypeError, ValueError) as exc:
        raise NewsSchemaMigrationRequired("资讯 schema_version 非法") from exc
    if current != NEWS_SCHEMA_VERSION:
        raise NewsSchemaMigrationRequired(
            f"资讯数据库版本 {current} 不是当前版本 {NEWS_SCHEMA_VERSION}，需先执行一次性迁移"
        )
    columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(news)")}
    required_columns = {
        "id", "source", "title", "content", "url", "published_at", "symbols",
        "sectors", "event_type", "sentiment", "summary", "created_at", *_NEWS_COLUMNS,
    }
    missing_columns = sorted(required_columns - columns)
    if missing_columns:
        raise NewsSchemaMigrationRequired(
            "资讯数据库缺少当前字段，需先执行一次性迁移：" + ",".join(missing_columns)
        )


def migrate_legacy_news_schema(
    connection: sqlite3.Connection,
    *,
    normalize_sectors: Callable[[list[Any]], list[str]],
) -> None:
    """Explicit one-shot migration using only facts persisted in the old database."""
    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS news_store_meta ("
        "key TEXT PRIMARY KEY,value TEXT NOT NULL)"
    )
    row = connection.execute(
        "SELECT value FROM news_store_meta WHERE key='schema_version'"
    ).fetchone()
    current = int(row[0]) if row else 0
    if current > NEWS_SCHEMA_VERSION:
        raise RuntimeError(
            f"资讯数据库版本 {current} 高于当前支持版本 {NEWS_SCHEMA_VERSION}"
        )
    _create_news_schema(connection, legacy=current < NEWS_SCHEMA_VERSION)
    if current < NEWS_SCHEMA_VERSION:
        _rebuild_legacy_title_identity_table(connection)
        _create_news_schema(connection, legacy=False)
        _rebuild_dimensions(
            connection,
            normalize_sectors=normalize_sectors,
        )
        connection.execute(
            "INSERT INTO news_store_meta(key,value) VALUES ('schema_version',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(NEWS_SCHEMA_VERSION),),
        )
    connection.commit()


def _decay_weight(published_at_epoch: Any, now: Any, halflife_days: Any) -> float:
    try:
        age_days = max(0.0, (float(now) - float(published_at_epoch)) / 86400.0)
        return math.pow(0.5, age_days / max(0.01, float(halflife_days)))
    except (TypeError, ValueError, OverflowError):
        return 0.0


def register_news_raw_verifier(connection: sqlite3.Connection) -> None:
    """Register the fail-closed raw-evidence verifier for formal factor SQL."""
    database_path = ""
    for row in connection.execute("PRAGMA database_list"):
        if str(row[1]) == "main":
            database_path = str(row[2] or "")
            break

    def valid(raw_cache_key: Any) -> int:
        if not database_path:
            return 0
        return int(read_raw_evidence(database_path, str(raw_cache_key or "")) is not None)

    def binding_valid(
        source_id: Any,
        raw_cache_key: Any,
        url: Any,
        provider_item_id: Any,
        title: Any,
        content: Any,
        published_at: Any,
        published_at_epoch: Any,
        content_scope: Any,
        parser_version: Any,
        content_hash: Any,
        binding_hash: Any,
    ) -> int:
        try:
            expected_content_hash = news_content_hash(str(content or ""), str(title or ""))
            if expected_content_hash != str(content_hash or ""):
                return 0
            expected_binding = article_evidence_binding_hash(
                source_id=str(source_id or ""),
                raw_cache_key=str(raw_cache_key or ""),
                url=str(url or ""),
                provider_item_id=str(provider_item_id or ""),
                title=str(title or ""),
                content=str(content or ""),
                published_at=str(published_at or ""),
                published_at_epoch=float(published_at_epoch or 0.0),
                content_scope=str(content_scope or ""),
                parser_version=str(parser_version or ""),
            )
        except (TypeError, ValueError, OverflowError):
            return 0
        return int(expected_binding == str(binding_hash or ""))

    connection.create_function("qm_news_raw_valid", 1, valid)
    connection.create_function(
        "qm_news_article_evidence_valid", 12, binding_valid, deterministic=True,
    )


_NEWS_STATS_SQL = """
WITH base AS (
    SELECT n.id,n.content_hash,n.published_at_epoch,n.sentiment,
           n.factor_weight_at_analysis * n.confidence
               * n.factor_importance_score / 100.0
               AS quality_weight
      FROM news n
      LEFT JOIN news_sources s ON s.id=n.source_id
     WHERE n.published_at_epoch>=? AND n.published_at_epoch<=?
       AND n.first_seen_at>0 AND n.first_seen_at<=?
       AND n.content_version_at>0 AND n.content_version_at<=?
       AND n.analysis_updated_at>0 AND n.analysis_updated_at<=?
       AND n.analysis_status='complete' AND n.confidence>=?
       AND n.factor_importance_score>0 AND n.factor_importance_score<=100
       AND n.factor_weight_at_analysis>0 AND n.factor_weight_at_analysis<=3
       AND n.content_scope IN ('full_text','full_article','feed_summary')
       AND n.is_official=1 AND COALESCE(s.is_official,0)=1
       AND COALESCE(s.built_in,0)=1
       AND n.raw_cache_key<>'' AND qm_news_raw_valid(n.raw_cache_key)=1
       AND n.evidence_binding_hash<>''
       AND n.ingest_window_id<>'' AND n.ingest_batch_id<>''
       AND qm_news_article_evidence_valid(
           n.source_id,n.raw_cache_key,n.url,n.provider_item_id,n.title,n.content,
           n.published_at,n.published_at_epoch,n.content_scope,n.parser_version,
           n.content_hash,n.evidence_binding_hash)=1
       AND EXISTS (
           SELECT 1 FROM news_raw_manifest h
            WHERE h.source_id=n.source_id AND h.raw_cache_key=n.raw_cache_key
       )
       AND EXISTS (
           SELECT 1 FROM news_article_evidence_manifest e
            WHERE e.binding_hash=n.evidence_binding_hash
              AND e.source_id=n.source_id AND e.raw_cache_key=n.raw_cache_key
              AND e.article_url=n.url AND e.provider_item_id=n.provider_item_id
              AND e.content_hash=n.content_hash AND e.title=n.title
              AND e.content=n.content AND e.published_at=n.published_at
              AND e.published_at_epoch=n.published_at_epoch
              AND e.content_scope=n.content_scope
              AND e.parser_version=n.parser_version
       )
       AND EXISTS (
           SELECT 1 FROM news_ingest_windows w
           JOIN news_ingest_batches b ON b.window_id=w.window_id
           JOIN news_ingest_batch_articles ba ON ba.batch_id=b.batch_id
            WHERE w.window_id=n.ingest_window_id AND w.source_id=n.source_id
              AND w.status='complete' AND w.completed_batch_id<>''
              AND b.batch_id=n.ingest_batch_id AND b.source_id=n.source_id
              AND ba.evidence_binding_hash=n.evidence_binding_hash
              AND ba.source_id=n.source_id AND ba.provider_item_id=n.provider_item_id
              AND ba.raw_cache_key=n.raw_cache_key
       )
), ranked AS (
    SELECT *,ROW_NUMBER() OVER (
        PARTITION BY COALESCE(NULLIF(content_hash,''),'id:' || id),
                     strftime('%Y-%m-%d',published_at_epoch,'unixepoch','+8 hours')
        ORDER BY quality_weight DESC,id
    ) AS duplicate_rank
      FROM base
), selected AS MATERIALIZED (
    SELECT id,published_at_epoch,sentiment,quality_weight,
           quality_weight * qm_news_decay(published_at_epoch,?,?) AS current_weight
      FROM ranked
     WHERE duplicate_rank=1 AND quality_weight>0
), daily AS (
    SELECT strftime('%Y-%m-%d',published_at_epoch,'unixepoch','+8 hours') AS item_key,
           SUM(sentiment*quality_weight) AS weighted_score,
           SUM(quality_weight) AS total_weight,COUNT(*) AS event_count
      FROM selected GROUP BY item_key
), market AS (
    SELECT SUM(sentiment*current_weight) AS weighted_score,
           SUM(current_weight) AS total_weight,COUNT(*) AS event_count,
           SUM(sentiment>0.15) AS positive,SUM(sentiment< -0.15) AS negative
      FROM selected
), sectors AS (
    SELECT d.sector AS item_key,SUM(s.sentiment*s.current_weight) AS weighted_score,
           SUM(s.current_weight) AS total_weight,COUNT(*) AS event_count,
           SUM(s.sentiment>0.15) AS positive,SUM(s.sentiment< -0.15) AS negative
      FROM selected s JOIN news_analysis_sectors d ON d.news_id=s.id
     GROUP BY d.sector
), symbols AS (
    SELECT d.symbol AS item_key,COUNT(*) AS event_count
      FROM selected s JOIN news_analysis_symbols d ON d.news_id=s.id
     GROUP BY d.symbol ORDER BY event_count DESC,d.symbol LIMIT 24
)
SELECT 'market' AS item_type,'' AS item_key,weighted_score,total_weight,event_count,
       positive,negative FROM market
UNION ALL
SELECT 'daily',item_key,weighted_score,total_weight,event_count,0,0 FROM daily
UNION ALL
SELECT 'sector',item_key,weighted_score,total_weight,event_count,positive,negative FROM sectors
UNION ALL
SELECT 'symbol',item_key,0,0,event_count,0,0 FROM symbols
"""


def aggregate_news_stats(
    connection: sqlite3.Connection,
    *,
    cutoff: float,
    until: float,
    minimum_confidence: float,
    now: float,
    halflife_days: float,
    knowledge_until: float | None = None,
) -> list[dict[str, Any]]:
    """Aggregate events by publication time and evidence visibility separately.

    ``until`` is the event/publication cutoff.  ``knowledge_until`` controls when
    the raw article, content version, and analysis must have become visible.  It
    defaults to ``until`` to preserve strict point-in-time callers.
    """
    visible_until = until if knowledge_until is None else float(knowledge_until)
    if not math.isfinite(visible_until) or visible_until <= 0:
        raise ValueError("资讯证据可见截止时间必须是有效时间戳")
    connection.create_function("qm_news_decay", 3, _decay_weight, deterministic=True)
    register_news_raw_verifier(connection)
    rows = connection.execute(
        _NEWS_STATS_SQL,
        (
            cutoff, until, visible_until, visible_until, visible_until, minimum_confidence,
            now, halflife_days,
        ),
    ).fetchall()
    return [dict(row) for row in rows]


_NEWS_EVENT_FOCUS_SQL = """
WITH base AS (
    SELECT n.id,n.content_hash,n.published_at_epoch,
           n.factor_weight_at_analysis * n.confidence
               * n.factor_importance_score / 100.0
               AS quality_weight
      FROM news n
      LEFT JOIN news_sources s ON s.id=n.source_id
     WHERE n.published_at_epoch>=? AND n.published_at_epoch<=?
       AND n.first_seen_at>0 AND n.first_seen_at<=?
       AND n.content_version_at>0 AND n.content_version_at<=?
       AND n.analysis_updated_at>0 AND n.analysis_updated_at<=?
       AND n.analysis_status='complete' AND n.confidence>=?
       AND n.factor_importance_score>0 AND n.factor_importance_score<=100
       AND n.factor_weight_at_analysis>0 AND n.factor_weight_at_analysis<=3
       AND n.content_scope IN ('full_text','full_article','feed_summary')
       AND n.is_official=1 AND COALESCE(s.is_official,0)=1
       AND COALESCE(s.built_in,0)=1
       AND n.raw_cache_key<>'' AND qm_news_raw_valid(n.raw_cache_key)=1
       AND n.evidence_binding_hash<>''
       AND n.ingest_window_id<>'' AND n.ingest_batch_id<>''
       AND qm_news_article_evidence_valid(
           n.source_id,n.raw_cache_key,n.url,n.provider_item_id,n.title,n.content,
           n.published_at,n.published_at_epoch,n.content_scope,n.parser_version,
           n.content_hash,n.evidence_binding_hash)=1
       AND EXISTS (
           SELECT 1 FROM news_raw_manifest h
            WHERE h.source_id=n.source_id AND h.raw_cache_key=n.raw_cache_key
       )
       AND EXISTS (
           SELECT 1 FROM news_article_evidence_manifest e
            WHERE e.binding_hash=n.evidence_binding_hash
              AND e.source_id=n.source_id AND e.raw_cache_key=n.raw_cache_key
              AND e.article_url=n.url AND e.provider_item_id=n.provider_item_id
              AND e.content_hash=n.content_hash AND e.title=n.title
              AND e.content=n.content AND e.published_at=n.published_at
              AND e.published_at_epoch=n.published_at_epoch
              AND e.content_scope=n.content_scope
              AND e.parser_version=n.parser_version
       )
       AND EXISTS (
           SELECT 1 FROM news_ingest_windows w
           JOIN news_ingest_batches b ON b.window_id=w.window_id
           JOIN news_ingest_batch_articles ba ON ba.batch_id=b.batch_id
            WHERE w.window_id=n.ingest_window_id AND w.source_id=n.source_id
              AND w.status='complete' AND w.completed_batch_id<>''
              AND b.batch_id=n.ingest_batch_id AND b.source_id=n.source_id
              AND ba.evidence_binding_hash=n.evidence_binding_hash
              AND ba.source_id=n.source_id AND ba.provider_item_id=n.provider_item_id
              AND ba.raw_cache_key=n.raw_cache_key
       )
), ranked AS (
    SELECT *,ROW_NUMBER() OVER (
        PARTITION BY COALESCE(NULLIF(content_hash,''),'id:' || id),
                     strftime('%Y-%m-%d',published_at_epoch,'unixepoch','+8 hours')
        ORDER BY quality_weight DESC,id
    ) AS duplicate_rank
      FROM base
), selected AS MATERIALIZED (
    SELECT id FROM ranked WHERE duplicate_rank=1 AND quality_weight>0
)
SELECT d.symbol AS symbol,COUNT(*) AS event_count
  FROM selected s JOIN news_analysis_symbols d ON d.news_id=s.id
 GROUP BY d.symbol
 ORDER BY event_count DESC,d.symbol
 LIMIT 24
"""


def aggregate_news_event_focus(
    connection: sqlite3.Connection,
    *,
    cutoff: float,
    until: float,
    minimum_confidence: float,
) -> list[dict[str, Any]]:
    """Return the bounded, quality-filtered symbol focus without other analytics."""
    register_news_raw_verifier(connection)
    rows = connection.execute(
        _NEWS_EVENT_FOCUS_SQL,
        (cutoff, until, until, until, until, minimum_confidence),
    ).fetchall()
    return [dict(row) for row in rows]
