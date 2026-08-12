from __future__ import annotations

import json
import sqlite3

import pandas as pd

from quantmaster.data.legacy_migrations import (
    MarketDataLegacyMigrator,
    migrate_bar_filenames,
    migrate_index_membership,
    migrate_industry_current_projection,
    migrate_instrument_names,
)


def _catalog(path, rows):
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE instruments(symbol TEXT PRIMARY KEY,name TEXT)")
        connection.executemany("INSERT INTO instruments VALUES (?,?)", rows)


def test_bar_migration_dry_run_idempotent_and_collision_isolated(tmp_path):
    bars = tmp_path / "bars"
    bars.mkdir()
    with sqlite3.connect(bars / "meta.sqlite") as connection:
        connection.execute("CREATE TABLE bar_meta(symbol TEXT PRIMARY KEY)")
        connection.executemany(
            "INSERT INTO bar_meta VALUES (?)", [("HG=F.US",), ("HG#F.US",)],
        )
    legacy = bars / "HG_F.US.parquet"
    legacy.write_bytes(b"old")

    dry = migrate_bar_filenames(tmp_path, dry_run=True)
    assert dry[0]["outcome"] == "conflict"
    assert legacy.is_file()
    first = migrate_bar_filenames(tmp_path)
    second = migrate_bar_filenames(tmp_path)
    assert first[0]["diagnostic_code"] == "bar_symbol_collision"
    assert second[0]["diagnostic_code"] == "bar_filename_isolated"
    assert not legacy.exists()


def test_instrument_migration_fills_only_existing_empty_names(tmp_path):
    _catalog(tmp_path / "security_master.sqlite", [
        ("600000.SH", ""), ("600519.SH", "贵州茅台"),
    ])
    (tmp_path / "stock_names.json").write_text(json.dumps({"names": {
        "600000.SH": "浦发银行", "600519.SH": "旧茅台", "000001.SZ": "平安银行",
    }}), encoding="utf-8")

    dry = migrate_instrument_names(tmp_path, dry_run=True)
    with sqlite3.connect(tmp_path / "security_master.sqlite") as connection:
        assert connection.execute(
            "SELECT name FROM instruments WHERE symbol='600000.SH'"
        ).fetchone()[0] == ""
    results = migrate_instrument_names(tmp_path)
    outcomes = {item["record_key"]: item["outcome"] for item in results}
    assert outcomes == {
        "instrument:000001.SZ": "blank",
        "instrument:600000.SH": "converted",
        "instrument:600519.SH": "unchanged",
    }
    assert any(item["outcome"] == "converted" for item in dry)


def test_unknown_instrument_and_index_formats_are_not_guessed(tmp_path):
    (tmp_path / "stock_names.json").write_text('{"symbols": {}}', encoding="utf-8")
    instrument = migrate_instrument_names(tmp_path)
    assert instrument[0]["diagnostic_code"] == "instrument_names_unknown_format"

    cache = tmp_path / "api_cache" / "tushare"
    cache.mkdir(parents=True)
    pd.DataFrame({"foo": [1]}).to_parquet(cache / "index_weight-old.parquet")
    index = migrate_index_membership(tmp_path)
    assert index[0]["diagnostic_code"] == "index_membership_unknown_format"
    assert not (tmp_path / "migration_quarantine").exists()


def test_index_migration_keeps_common_columns_but_leaves_times_empty(tmp_path):
    cache = tmp_path / "api_cache" / "tushare"
    cache.mkdir(parents=True)
    source = cache / "index_weight-old.parquet"
    pd.DataFrame({
        "index_code": ["000300.SH"], "con_code": ["600000.SH"],
        "trade_date": ["2024-01-02"], "weight": [1.0], "mystery": [9],
    }).to_parquet(source)

    dry = migrate_index_membership(tmp_path, dry_run=True)
    assert dry[0]["outcome"] == "blank"
    assert dry[0]["unknown_fields"] == ["mystery"]
    migrate_index_membership(tmp_path)
    target = (
        tmp_path / "migration_quarantine" / "market_data" /
        "index_membership" / source.name
    )
    isolated = pd.read_parquet(target)
    assert isolated["published_at"].isna().all()
    assert isolated["acquired_at"].isna().all()


def test_industry_requires_exact_shape_and_is_current_only(tmp_path):
    path = tmp_path / "industry_map.json"
    path.write_text('{"updated_at": 1, "mapping": {}}', encoding="utf-8")
    assert migrate_industry_current_projection(tmp_path)[0]["outcome"] == "blank"
    path.write_text(
        '{"updated_at": 1, "mapping": {"600000.sh": "银行"}}', encoding="utf-8",
    )
    before = path.read_text(encoding="utf-8")
    assert migrate_industry_current_projection(tmp_path, dry_run=True)[0]["outcome"] == "converted"
    assert path.read_text(encoding="utf-8") == before
    migrate_industry_current_projection(tmp_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload == {
        "schema_version": 3, "projection": "current_only", "updated_at": 1.0,
        "mapping": {"600000.SH": "银行"},
    }
    assert migrate_industry_current_projection(tmp_path)[0]["outcome"] == "unchanged"


def test_adapter_empty_root_and_batch_resume(tmp_path):
    adapter = MarketDataLegacyMigrator()
    assert list(adapter.inspect(tmp_path)) == []
    assert list(adapter.migrate_batch(tmp_path, "", 10)) == []
