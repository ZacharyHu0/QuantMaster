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
from collections.abc import Callable, Mapping, Sequence
from typing import Any

NEWS_SCHEMA_VERSION = 1

_NEWS_COLUMNS = {
    "importance_score": "REAL DEFAULT 0",
    "scope": "TEXT DEFAULT ''",
    "urgency": "TEXT DEFAULT ''",
    "confidence": "REAL DEFAULT 0",
    "sectors": "TEXT DEFAULT '[]'",
    "fingerprint": "TEXT DEFAULT ''",
    "is_official": "INTEGER DEFAULT 0",
    "source_id": "TEXT DEFAULT ''",
    "content_hash": "TEXT DEFAULT ''",
    "first_seen_at": "REAL DEFAULT 0",
    "last_seen_at": "REAL DEFAULT 0",
    "raw_cache_key": "TEXT DEFAULT ''",
    "analysis_status": "TEXT DEFAULT 'pending'",
    "analysis_attempts": "INTEGER DEFAULT 0",
    "analysis_error": "TEXT DEFAULT ''",
    "analysis_version": "INTEGER DEFAULT 1",
    "next_retry_at": "REAL DEFAULT 0",
    "parser_version": "TEXT DEFAULT '1'",
    "analysis_recovery_count": "INTEGER DEFAULT 0",
    "last_failure_code": "TEXT DEFAULT ''",
    "analysis_updated_at": "REAL DEFAULT 0",
}


def news_fingerprint(source: str, title: str, url: str, published_at: str) -> str:
    normalized_title = re.sub(r"\W+", "", title.casefold())
    identity = f"{url.strip().lower()}|{normalized_title}|{published_at.strip()}"
    return hashlib.sha256(f"{source}|{identity}".encode()).hexdigest()


def news_content_hash(content: str, title: str) -> str:
    text = re.sub(r"\s+", "", (content or title).casefold())
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _decode_list(value: Any) -> list[str]:
    try:
        decoded = json.loads(value or "[]")
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(decoded, list):
        return []
    return list(dict.fromkeys(str(item).strip() for item in decoded if str(item).strip()))


def _industry_map_hash(industry_map: Mapping[str, str]) -> str:
    payload = json.dumps(
        sorted((str(symbol), str(sector)) for symbol, sector in industry_map.items()),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def replace_news_dimensions(
    connection: sqlite3.Connection,
    news_id: int,
    symbols: Sequence[Any],
    sectors: Sequence[Any],
    *,
    industry_map: Mapping[str, str],
    normalize_sectors: Callable[[list[Any]], list[str]],
) -> None:
    """Replace one news row's rebuildable symbol and sector projections."""
    normalized_symbols = list(dict.fromkeys(
        str(symbol).strip() for symbol in symbols if str(symbol).strip()
    ))
    normalized_sectors = normalize_sectors([
        *sectors,
        *(industry_map.get(symbol, "") for symbol in normalized_symbols),
    ])
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


def _create_news_schema(connection: sqlite3.Connection, *, legacy: bool) -> None:
    connection.execute(
        "CREATE TABLE IF NOT EXISTS news ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,source TEXT,title TEXT,content TEXT,"
        "url TEXT,published_at TEXT,symbols TEXT,sectors TEXT,event_type TEXT,"
        "sentiment REAL,summary TEXT,"
        "created_at REAL,importance_score REAL DEFAULT 0,scope TEXT DEFAULT '',"
        "urgency TEXT DEFAULT '',confidence REAL DEFAULT 0,fingerprint TEXT DEFAULT '',"
        "is_official INTEGER DEFAULT 0,source_id TEXT DEFAULT '',"
        "content_hash TEXT DEFAULT '',first_seen_at REAL DEFAULT 0,last_seen_at REAL DEFAULT 0,"
        "raw_cache_key TEXT DEFAULT '',analysis_status TEXT DEFAULT 'pending',"
        "analysis_attempts INTEGER DEFAULT 0,analysis_error TEXT DEFAULT '',"
        "analysis_version INTEGER DEFAULT 1,next_retry_at REAL DEFAULT 0,"
        "parser_version TEXT DEFAULT '1',analysis_recovery_count INTEGER DEFAULT 0,"
        "last_failure_code TEXT DEFAULT '',analysis_updated_at REAL DEFAULT 0,"
        "UNIQUE(source,title,published_at))"
    )
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
    statements = (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_news_fingerprint_unique "
        "ON news(fingerprint) WHERE fingerprint<>''",
        "CREATE INDEX IF NOT EXISTS idx_news_recent ON news(id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_news_source ON news(source_id,id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_news_analysis "
        "ON news(analysis_status,next_retry_at,id)",
        "CREATE INDEX IF NOT EXISTS idx_news_seen ON news(first_seen_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_news_stats "
        "ON news(analysis_status,first_seen_at,confidence)",
        "CREATE INDEX IF NOT EXISTS idx_news_symbol_value "
        "ON news_analysis_symbols(symbol,news_id)",
        "CREATE INDEX IF NOT EXISTS idx_news_sector_value "
        "ON news_analysis_sectors(sector,news_id)",
    )
    for statement in statements:
        connection.execute(statement)


def _backfill_news_core(connection: sqlite3.Connection) -> None:
    connection.execute("UPDATE news SET source_id=source WHERE source_id='' OR source_id IS NULL")
    connection.execute("UPDATE news SET first_seen_at=created_at WHERE first_seen_at=0")
    connection.execute("UPDATE news SET last_seen_at=created_at WHERE last_seen_at=0")
    connection.execute(
        "UPDATE news SET analysis_status='dead_letter' "
        "WHERE analysis_status='failed' AND analysis_attempts>=3"
    )
    connection.execute(
        "UPDATE news SET analysis_updated_at=last_seen_at "
        "WHERE analysis_status='complete' AND analysis_updated_at=0"
    )
    rows = connection.execute(
        "SELECT id,source,title,content,url,published_at,fingerprint,summary,confidence,"
        "symbols,analysis_status FROM news"
    ).fetchall()
    updates: list[tuple[str, str, str, int]] = []
    for row in rows:
        fingerprint = row["fingerprint"] or news_fingerprint(
            row["source"] or "", row["title"] or "", row["url"] or "",
            row["published_at"] or "",
        )
        content_hash = news_content_hash(row["content"] or "", row["title"] or "")
        status = row["analysis_status"] or "pending"
        has_analysis = (
            row["summary"] or row["confidence"]
            or row["symbols"] not in {"", "[]", None}
        )
        if status == "pending" and has_analysis:
            status = "complete"
        updates.append((fingerprint, content_hash, status, int(row["id"])))
    connection.executemany(
        "UPDATE news SET fingerprint=?,content_hash=?,analysis_status=? WHERE id=?",
        updates,
    )


def _rebuild_dimensions(
    connection: sqlite3.Connection,
    *,
    industry_map: Mapping[str, str],
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
            industry_map=industry_map,
            normalize_sectors=normalize_sectors,
        )


def migrate_news_schema(
    connection: sqlite3.Connection,
    *,
    industry_map: Mapping[str, str],
    normalize_sectors: Callable[[list[Any]], list[str]],
) -> None:
    """Migrate once, then keep normal store construction independent of row count."""
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
    expected_map_hash = _industry_map_hash(industry_map)
    stored_map = connection.execute(
        "SELECT value FROM news_store_meta WHERE key='industry_map_hash'"
    ).fetchone()
    if current < NEWS_SCHEMA_VERSION:
        _backfill_news_core(connection)
        _rebuild_dimensions(
            connection,
            industry_map=industry_map,
            normalize_sectors=normalize_sectors,
        )
        connection.execute(
            "INSERT INTO news_store_meta(key,value) VALUES ('schema_version',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(NEWS_SCHEMA_VERSION),),
        )
    elif not stored_map or stored_map[0] != expected_map_hash:
        _rebuild_dimensions(
            connection,
            industry_map=industry_map,
            normalize_sectors=normalize_sectors,
        )
    connection.execute(
        "INSERT INTO news_store_meta(key,value) VALUES ('industry_map_hash',?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (expected_map_hash,),
    )


def _decay_weight(first_seen_at: Any, now: Any, halflife_days: Any) -> float:
    try:
        age_days = max(0.0, (float(now) - float(first_seen_at)) / 86400.0)
        return math.pow(0.5, age_days / max(0.01, float(halflife_days)))
    except (TypeError, ValueError, OverflowError):
        return 0.0


_NEWS_STATS_SQL = """
WITH base AS (
    SELECT n.id,n.content_hash,n.first_seen_at,n.sentiment,
           COALESCE(s.factor_weight,1) * n.confidence * n.importance_score / 100.0
               AS quality_weight
      FROM news n
      LEFT JOIN news_sources s ON s.id=n.source_id
     WHERE n.first_seen_at>=? AND n.analysis_status='complete' AND n.confidence>=?
), ranked AS (
    SELECT *,ROW_NUMBER() OVER (
        PARTITION BY COALESCE(NULLIF(content_hash,''),'id:' || id)
        ORDER BY quality_weight DESC,id
    ) AS duplicate_rank
      FROM base
), selected AS MATERIALIZED (
    SELECT id,first_seen_at,sentiment,quality_weight,
           quality_weight * qm_news_decay(first_seen_at,?,?) AS current_weight
      FROM ranked
     WHERE duplicate_rank=1 AND quality_weight>0
), daily AS (
    SELECT strftime('%Y-%m-%d',first_seen_at,'unixepoch','+8 hours') AS item_key,
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
    minimum_confidence: float,
    now: float,
    halflife_days: float,
) -> list[dict[str, Any]]:
    connection.create_function("qm_news_decay", 3, _decay_weight, deterministic=True)
    rows = connection.execute(
        _NEWS_STATS_SQL,
        (cutoff, minimum_confidence, now, halflife_days),
    ).fetchall()
    return [dict(row) for row in rows]
