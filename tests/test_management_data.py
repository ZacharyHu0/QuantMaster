"""数据迁移与股票池管理测试。"""

from __future__ import annotations

import sqlite3
import time

import pytest

from quantmaster.config import Config, set_config
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
    assert result["status"] == "completed", result
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
    assert result["status"] == "completed", result
    with sqlite3.connect(target / "ledger.sqlite") as connection:
        assert connection.execute("SELECT value FROM entries").fetchone() == ("kept",)


def test_migration_preflight_rejects_nested_and_nonempty(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    cfg = Config()
    cfg.data.root = str(source)
    set_config(cfg)
    manager = DataMigrationManager(RootSwitcher())
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
