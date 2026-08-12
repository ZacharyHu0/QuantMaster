from __future__ import annotations

import sqlite3

import pytest

from quantmaster.ai.crawler import NewsItem, NewsStore
from quantmaster.ai.news_migration import NewsContractMigrator
from quantmaster.ai.news_storage import NewsSchemaMigrationRequired


def _legacy_database(path, *, symbols="[]", sectors="[]"):
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TABLE news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,title TEXT,content TEXT,url TEXT,published_at TEXT,
                symbols TEXT,sectors TEXT,event_type TEXT,sentiment REAL,summary TEXT,
                created_at REAL,UNIQUE(source,title,published_at));
            CREATE TABLE news_store_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
            INSERT INTO news_store_meta(key,value) VALUES ('schema_version','3');
        """)
        connection.execute(
            "INSERT INTO news(source,title,content,url,published_at,symbols,sectors,"
            "event_type,sentiment,summary,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "pboc", "同标题", "原始正文", "https://example.test/one", "2026-08-09",
                symbols, sectors, "其他", 0.2, "原摘要", 1786240800,
            ),
        )


def test_store_construction_does_not_migrate_old_schema(tmp_path):
    path = tmp_path / "news.sqlite"
    _legacy_database(path)

    with pytest.raises(NewsSchemaMigrationRequired):
        NewsStore(path)

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT value FROM news_store_meta WHERE key='schema_version'"
        ).fetchone()[0] == "3"
        assert "source_id" not in {
            row[1] for row in connection.execute("PRAGMA table_info(news)")
        }


def test_explicit_migration_preserves_row_fields_and_leaves_unknowns_blank(tmp_path):
    path = tmp_path / "news.sqlite"
    _legacy_database(path, symbols='["600000.SH"]', sectors='["银行"]')
    migrator = NewsContractMigrator()

    inspected = list(migrator.inspect(tmp_path))
    assert len(inspected) == 1
    assert inspected[0].outcome == "blank"
    assert "content_scope" in inspected[0].unknown_fields
    assert "source_id" in inspected[0].unknown_fields

    migrated = list(migrator.migrate_batch(tmp_path, after_key="", limit=20))
    assert migrated[0].diagnostic_code == "news_optional_evidence_unavailable"
    store = NewsStore(path)
    with store._conn() as connection:
        row = connection.execute(
            "SELECT source,title,content,symbols,sectors,source_id,content_scope,"
            "fingerprint,content_hash,factor_importance_score,factor_weight_at_analysis "
            "FROM news"
        ).fetchone()
    assert tuple(row[:5]) == (
        "pboc", "同标题", "原始正文", '["600000.SH"]', '["银行"]',
    )
    assert all(value in {None, "", 0} for value in row[5:])
    with sqlite3.connect(path) as connection:
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
    assert not any("legacy" in name or "migration_v3" in name for name in tables)


def test_current_store_rejects_permanent_legacy_archive_tables(tmp_path):
    path = tmp_path / "news.sqlite"
    NewsStore(path)
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE news_legacy_v3(id INTEGER)")

    with pytest.raises(NewsSchemaMigrationRequired, match="退休归档表"):
        NewsStore(path)


def test_migration_is_idempotent_and_current_title_collision_is_not_identity(tmp_path):
    path = tmp_path / "news.sqlite"
    _legacy_database(path)
    migrator = NewsContractMigrator()
    assert list(migrator.migrate_batch(tmp_path, after_key="", limit=20))
    assert list(migrator.migrate_batch(tmp_path, after_key="news:00000000000000000001", limit=20)) == []

    store = NewsStore(path)
    assert store.save([
        NewsItem(
            source="pboc", title="同标题", content="新正文", published_at="2026-08-09",
            provider_item_id="distinct-current-id",
        )
    ]) == 1
    with store._conn() as connection:
        assert connection.execute("SELECT COUNT(*) FROM news").fetchone()[0] == 2


@pytest.mark.parametrize("field,value", [("symbols", "not-json"), ("sectors", "{}")])
def test_damaged_current_shape_is_conflict_not_decoder_fallback(tmp_path, field, value):
    path = tmp_path / "news.sqlite"
    values = {"symbols": "[]", "sectors": "[]"}
    values[field] = value
    _legacy_database(path, **values)

    record = next(iter(NewsContractMigrator().inspect(tmp_path)))

    assert record.outcome == "conflict"
    assert record.diagnostic_code == f"news_{field}_" + (
        "json_invalid" if value == "not-json" else "shape_invalid"
    )
