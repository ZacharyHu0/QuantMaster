"""数据迁移与候选管理测试。"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import ClassVar

import pandas as pd
import pytest

from quantmaster.config import Config, set_config
from quantmaster.data.base import BarDataEnvelope, BarDataQuality
from quantmaster.data.migration import DataMigrationManager, MigrationError
from quantmaster.data.universe import (
    delete_universe,
    list_universes,
    load_universe,
    normalize_symbols,
    rename_universe,
    save_universe,
)


class RootSwitcher:
    def __init__(self):
        self.target = None

    def update_data_root(self, target):
        self.target = str(target)


def test_symbol_and_universe_validation(tmp_path):
    cfg = Config()
    cfg.data.root = str(tmp_path)
    set_config(cfg)
    assert normalize_symbols([" 600519 ", "600519.sh", "000001", "430047"] ) == [
        "600519.SH", "000001.SZ", "430047.BJ",
    ]
    save_universe("核心_pool", ["600519", "000001.sz", "600519.SH"])
    assert load_universe("核心_pool") == ["600519.SH", "000001.SZ"]
    assert any(item["name"] == "核心_pool" for item in list_universes())
    rename_universe("核心_pool", "renamed")
    delete_universe("renamed")
    with pytest.raises(ValueError):
        save_universe("../escape", ["600519"])
    with pytest.raises(ValueError, match="只读"):
        save_universe("demo", ["600519"])
    with pytest.raises(ValueError, match="只读"):
        save_universe("csi800", ["600519"])


def test_canonical_universe_symbols_use_one_batched_master_lookup(monkeypatch):
    from quantmaster.data import instruments

    class Entry:
        def __init__(self, symbol):
            self.symbol = symbol

    class Store:
        instances = 0
        batches: ClassVar[list[list[str]]] = []

        def __init__(self):
            type(self).instances += 1

        def get_many(self, symbols):
            values = list(symbols)
            type(self).batches.append(values)
            return {value: Entry(value) for value in values}

        def get(self, symbol):  # pragma: no cover - 规范代码不应退回单项查询
            raise AssertionError(f"unexpected single lookup: {symbol}")

        def resolve(self, symbol):  # pragma: no cover - 规范代码不应进入模糊搜索
            raise AssertionError(f"unexpected fuzzy lookup: {symbol}")

    monkeypatch.setattr(instruments, "InstrumentStore", Store)

    assert normalize_symbols(["600519.SH", "000001.SZ", "600519.SH"]) == [
        "600519.SH", "000001.SZ",
    ]
    assert Store.instances == 1
    assert Store.batches == [["600519.SH", "000001.SZ", "600519.SH"]]


def test_copy_migration_keeps_source_and_switches_only_after_verify(tmp_path):
    source, target = tmp_path / "source", tmp_path / "target"
    source.mkdir()
    (source / "bars").mkdir()
    (source / "bars" / "sample.bin").write_bytes(b"abc" * 1000)
    cfg = Config()
    cfg.data.root = str(source)
    set_config(cfg)
    switcher = RootSwitcher()
    manager = DataMigrationManager(switcher)
    task = manager.create(target, "copy")
    for _ in range(100):
        result = manager.get(task["id"])
        if result["status"] not in {"pending", "running", "cancelling"}:
            break
        time.sleep(0.02)
    assert result["status"] == "completed", result.get("error") or result
    assert (source / "bars" / "sample.bin").is_file()
    assert (target / "bars" / "sample.bin").read_bytes() == b"abc" * 1000
    assert switcher.target == str(target.resolve())


def test_migration_uses_sqlite_consistent_backup(tmp_path):
    source, target = tmp_path / "source", tmp_path / "target"
    source.mkdir()
    database = source / "ledger.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE entries (value TEXT)")
        connection.execute("INSERT INTO entries VALUES ('kept')")
    cfg = Config()
    cfg.data.root = str(source)
    set_config(cfg)
    manager = DataMigrationManager(RootSwitcher())
    task = manager.create(target, "copy")
    for _ in range(100):
        result = manager.get(task["id"])
        if result["status"] not in {"pending", "running", "cancelling"}:
            break
        time.sleep(0.02)
    assert result["status"] == "completed", result.get("error") or result
    with sqlite3.connect(target / "ledger.sqlite") as connection:
        assert connection.execute("SELECT value FROM entries").fetchone() == ("kept",)


def test_migration_aborts_if_source_changes_during_copy(tmp_path, monkeypatch):
    from quantmaster.data import migration

    source, target = tmp_path / "source", tmp_path / "target"
    source.mkdir()
    (source / "bars.parquet").write_bytes(b"stable")
    cfg = Config()
    cfg.data.root = str(source)
    set_config(cfg)
    switcher = RootSwitcher()
    manager = DataMigrationManager(switcher)
    original_copy = migration.shutil.copy2

    def copy_then_mutate(src, dst):
        result = original_copy(src, dst)
        (source / "late-write.json").write_text("changed", encoding="utf-8")
        return result

    monkeypatch.setattr(migration.shutil, "copy2", copy_then_mutate)
    task = manager.create(target, "copy")
    for _ in range(100):
        result = manager.get(task["id"])
        if result["status"] not in {"pending", "running", "cancelling"}:
            break
        time.sleep(0.02)

    assert result["status"] == "failed"
    assert "迁移期间发生写入" in result["error"]
    assert switcher.target is None
    assert not target.exists()


def test_migration_preflight_rejects_nested_and_nonempty(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    cfg = Config()
    cfg.data.root = str(source)
    set_config(cfg)
    manager = DataMigrationManager(RootSwitcher())
    with pytest.raises(MigrationError, match="绝对路径"):
        manager.create("relative-data", "copy")
    with pytest.raises(MigrationError, match="嵌套"):
        manager.create(source / "nested", "copy")
    target = tmp_path / "occupied"
    target.mkdir()
    (target / "keep.txt").write_text("keep", encoding="utf-8")
    with pytest.raises(MigrationError, match="空目录"):
        manager.create(target, "copy")


def test_switch_only_accepts_existing_data_directory(tmp_path):
    source, target = tmp_path / "source", tmp_path / "existing"
    source.mkdir()
    target.mkdir()
    (target / "ledger.sqlite").write_bytes(b"existing")
    cfg = Config()
    cfg.data.root = str(source)
    set_config(cfg)
    switcher = RootSwitcher()
    manager = DataMigrationManager(switcher)
    task = manager.create(target, "switch")
    for _ in range(100):
        result = manager.get(task["id"])
        if result["status"] not in {"pending", "running", "cancelling"}:
            break
        time.sleep(0.01)
    assert result["status"] == "completed"
    assert switcher.target == str(target.resolve())


def test_incremental_refresh_job_is_persistent_and_retries_only_failures(
    isolated_config, monkeypatch,
):
    from quantmaster.data.maintenance import DataRefreshManager
    from quantmaster.data.registry import RefreshMode

    manager = DataRefreshManager()
    symbols = ["600000.SH", "000001.SZ"]
    monkeypatch.setattr(manager, "_resolve_symbols", lambda *args: symbols)
    monkeypatch.setattr(manager, "_start", lambda job_id: None)
    calls = []

    def fake_load(symbol, start, end, **kwargs):
        calls.append((symbol, start, end, kwargs))
        return BarDataEnvelope(
            data=pd.DataFrame({"close": [1.0]}, index=pd.to_datetime([end])),
            quality=BarDataQuality(
                status="verified",
                requested_start=start,
                requested_end=end,
                observed_start=end,
                observed_end=end,
                coverage_ratio=1.0,
                sources=("fixture",),
                timezone="Asia/Shanghai",
                adjustment="qfq",
                requested_symbols=(symbol,),
                observed_symbols=(symbol,),
            ),
            provenance=({"source": "fixture"},),
        )

    monkeypatch.setattr("quantmaster.data.maintenance.load_history", fake_load)
    job = manager.create("market")
    assert job["status"] == "queued"
    assert manager.latest()["id"] == job["id"]

    manager._run(job["id"])
    completed = manager.get(job["id"])
    assert completed["status"] == "completed"
    assert [item[0] for item in calls] == symbols
    assert all(item[3]["refresh"] == RefreshMode.INCREMENTAL for item in calls)
    assert all(item[3]["priority"] == "maintenance" for item in calls)

    # 已结束任务中的失败项续跑时只重排失败标的，不重复成功标的。
    with manager._conn() as conn:
        conn.execute(
            "UPDATE refresh_jobs SET status='completed_with_errors',failed=1,"
            "failures_json=?,next_index=total WHERE id=?",
            ('[{"symbol":"000001.SZ","error":"offline"}]', job["id"]),
        )
    resumed = manager.resume(job["id"])
    assert resumed["status"] == "queued"
    assert resumed["total"] == 1
    assert resumed["next_index"] == 0
    assert resumed["attempt"] == 2
    with manager._conn() as conn:
        original = json.loads(conn.execute(
            "SELECT original_symbols_json FROM refresh_jobs WHERE id=?", (job["id"],)
        ).fetchone()[0])
    assert original == symbols


def test_refresh_manager_creates_schema_after_hot_root_switch(isolated_config, tmp_path):
    from quantmaster.data.maintenance import DataRefreshManager

    manager = DataRefreshManager()
    isolated_config.data.root = str(tmp_path / "switched")
    assert manager.latest() is None
    assert (isolated_config.data_root / "data_refresh.sqlite").exists()


def test_refresh_manager_only_recovers_expired_foreign_lease(isolated_config, monkeypatch):
    from quantmaster.data.maintenance import DataRefreshManager

    first = DataRefreshManager()
    second = DataRefreshManager()
    monkeypatch.setattr(first, "_resolve_symbols", lambda *args: ["600000.SH"])
    monkeypatch.setattr(first, "_start", lambda job_id: None)
    job = first.create("market")
    with first._conn() as conn:
        conn.execute(
            "UPDATE refresh_jobs SET status='running',owner=?,lease_expires=? WHERE id=?",
            (first.identity.value, time.time() + 60, job["id"]),
        )

    second._initialized_roots.clear()
    with second._conn():
        pass
    assert second.get(job["id"])["status"] == "running"

    with first._conn() as conn:
        conn.execute("UPDATE refresh_jobs SET lease_expires=0 WHERE id=?", (job["id"],))
    second._initialized_roots.clear()
    with second._conn():
        pass
    assert second.get(job["id"])["status"] == "interrupted"
