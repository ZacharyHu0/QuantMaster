from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from quantmaster.ai.crawler import NewsItem, NewsStore
from quantmaster.ai.news_storage import NewsSchemaMigrationRequired
from quantmaster.data.migration import NEWS_ARCHIVE_TABLES, NewsContractMigrator


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


def _archive_database(path: Path, counts=(1, 2, 3, 4)) -> None:
    NewsStore(path)
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        for table, count in zip(NEWS_ARCHIVE_TABLES, counts, strict=True):
            connection.execute(f'CREATE TABLE "{table}"(id INTEGER PRIMARY KEY)')
            connection.executemany(
                f'INSERT INTO "{table}"(id) VALUES (?)',
                ((index,) for index in range(1, count + 1)),
            )


def _runner_backup(root: Path, run_id: str = "run") -> Path:
    backup_root = root / "backups" / "legacy-contracts" / run_id
    from quantmaster.data.migration import backup_sqlite_tree

    backup_sqlite_tree(
        root, backup_root, exclude={"legacy_contract_migrations.sqlite"},
        extra_paths=("news.sqlite",),
    )
    destination = backup_root / "news.sqlite"
    with sqlite3.connect(root / "legacy_contract_migrations.sqlite") as connection:
        connection.execute(
            "CREATE TABLE migration_runs (domain TEXT,mode TEXT,status TEXT,"
            "backup_path TEXT,created_at TEXT)"
        )
        connection.execute(
            "INSERT INTO migration_runs VALUES ('news','apply','running',?,'2026-08-13')",
            (str(destination.parent),),
        )
    return destination


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


def test_archive_dry_run_reports_each_exact_table_and_does_not_count_current_rows(tmp_path):
    path = tmp_path / "news.sqlite"
    _archive_database(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO news(source,title,content) VALUES ('current','current','current')"
        )

    records = list(NewsContractMigrator().inspect(tmp_path))

    assert len(records) == 4
    assert [record.record_key.rpartition(":")[2] for record in records] == list(
        NEWS_ARCHIVE_TABLES
    )
    assert [record.detail for record in records] == [
        f"table={table}; row_count={count}"
        for table, count in zip(NEWS_ARCHIVE_TABLES, (1, 2, 3, 4), strict=True)
    ]
    with sqlite3.connect(path) as connection:
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        assert connection.execute("SELECT COUNT(*) FROM news").fetchone()[0] == 1
    assert set(NEWS_ARCHIVE_TABLES) <= tables


def test_archive_apply_requires_runner_backup_and_is_transactional_and_idempotent(tmp_path):
    path = tmp_path / "news.sqlite"
    _archive_database(path)
    migrator = NewsContractMigrator()
    with pytest.raises(RuntimeError, match="news_archive_backup_missing"):
        list(migrator.migrate_batch(tmp_path, after_key="", limit=4))
    _runner_backup(tmp_path)

    first = list(migrator.migrate_batch(tmp_path, after_key="", limit=2))
    assert [record.diagnostic_code for record in first] == [
        "news_archive_retired", "news_archive_retired",
    ]
    assert [record.detail for record in first] == [
        "table=news_revisions_legacy_v3; row_count=1",
        "table=news_analysis_sectors_legacy_v3; row_count=2",
    ]
    second = list(migrator.migrate_batch(
        tmp_path, after_key=first[-1].record_key, limit=2,
    ))
    assert len(second) == 2
    assert list(migrator.migrate_batch(
        tmp_path, after_key=second[-1].record_key, limit=2,
    )) == []
    with sqlite3.connect(path) as connection:
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    assert not set(NEWS_ARCHIVE_TABLES) & tables
    NewsStore(path)


def test_archive_rollback_restores_exact_tables_and_counts(tmp_path):
    path = tmp_path / "news.sqlite"
    _archive_database(path)
    backup_root = _runner_backup(tmp_path).parent
    migrator = NewsContractMigrator()
    records = list(migrator.migrate_batch(tmp_path, after_key="", limit=4))
    assert len(records) == 4
    # SQLite restore is an offline operation: close any cached NewsStore handle
    # created by the fixture before replacing the database and its sidecars.
    import gc

    gc.collect()

    migrator.rollback(tmp_path, backup_root)

    with sqlite3.connect(path) as connection:
        assert [
            connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            for table in NEWS_ARCHIVE_TABLES
        ] == [1, 2, 3, 4]
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
