"""数据迁移与候选管理测试。"""

from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pandas as pd
import pytest

from quantmaster.config import Config, set_config
from quantmaster.data import migration as data_migration
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


def test_refresh_slow_planning_is_durable_coalesced_and_cancellable(isolated_config, monkeypatch):
    from quantmaster.data.maintenance import DataRefreshManager
    from quantmaster.runtime.worker_ipc import RuntimeCommandServer, call_worker_command

    isolated_config.data.free_stockdb_managed = False
    manager = DataRefreshManager()
    manager.initialize()
    started, release = threading.Event(), threading.Event()
    discoveries, refreshed = [], []

    def resolve(*_args):
        discoveries.append(1)
        started.set()
        assert release.wait(10)
        return ["A"]

    def handler(operation, payload):
        if operation == "create":
            return manager.create("universe", "csi800")
        if operation == "cancel":
            return manager.cancel(payload["id"])
        return {"accepted": True}

    monkeypatch.setattr(manager, "_resolve_symbols", resolve)
    monkeypatch.setattr(manager, "_refresh_one", lambda *args: refreshed.append(1))
    server = RuntimeCommandServer(handler, root=isolated_config.data_root)
    server.start()
    try:
        preview = manager.preview("universe", "csi800")
        assert preview["total"] is None
        assert discoveries == []
        job = call_worker_command("create", root=server.root)
        assert started.wait(3)
        assert manager.get(job["id"])["phase"] == "规划刷新"
        assert DataRefreshManager().get(job["id"])["status"] == "running"
        assert call_worker_command("create", root=server.root)["id"] == job["id"]
        assert call_worker_command("control", root=server.root) == {"accepted": True}
        assert call_worker_command("cancel", {"id": job["id"]}, root=server.root)["status"] == "cancelling"
        assert discoveries == [1]
    finally:
        release.set()
        manager._ensure_runtime().wait(job["id"], timeout=5)
        server.stop()
        manager.shutdown()
    assert manager.get(job["id"])["status"] == "cancelled"
    assert refreshed == []


def test_refresh_all_false_membership_fails_with_durable_evidence(isolated_config, monkeypatch):
    from quantmaster.data import schema_access
    from quantmaster.data.maintenance import DataRefreshManager

    isolated_config.data.free_stockdb_managed = False
    manager = DataRefreshManager()
    manager.initialize()
    monkeypatch.setattr(manager, "_start", lambda _: None)
    monkeypatch.setattr(schema_access, "_factories", {})
    schema_access.register_membership_loader(
        lambda *_: pd.DataFrame({"A": [False], "B": [False]}),
    )
    job = manager.create("universe", "csi800")
    manager._run(job["id"])
    failed = DataRefreshManager().get(job["id"])
    assert failed["status"] == "failed"
    assert "REFRESH_MEMBERSHIP_EVIDENCE_MISSING" in failed["detail"]
    assert failed["outcome"] != "completed"
    assert failed["can_retry"]
    manager.shutdown()


def test_refresh_calls_registered_membership_loader_once(monkeypatch):
    from quantmaster.data import schema_access
    from quantmaster.data.maintenance import DataRefreshManager

    monkeypatch.setattr(schema_access, "_factories", {})
    calls = []

    def membership(start, end):
        calls.append((start, end))
        return pd.DataFrame({"600000.SH": [True], "000001.SZ": [False]})

    schema_access.register_membership_loader(membership)
    assert DataRefreshManager._resolve_symbols(
        "universe", "csi800", "2026-09-01", "2026-09-15",
    ) == ["600000.SH"]
    assert calls == [("2026-09-01", "2026-09-15")]


def test_refresh_publication_accepts_non_callable_result(monkeypatch, caplog):
    from quantmaster.data import schema_access
    from quantmaster.data.maintenance import DataRefreshManager

    monkeypatch.setattr(schema_access, "_factories", {})
    calls = []
    schema_access.register_market_overview_publisher(lambda: calls.append("published"))
    DataRefreshManager._publish_market_snapshot()
    assert calls == ["published"]
    assert "数据刷新后发布市场快照失败" not in caplog.text


def test_data_migration_preflight_is_side_effect_free_and_reports_capacity(
    tmp_path, monkeypatch,
):
    source = tmp_path / "source"
    source.mkdir()
    database = source / "ledger.sqlite"
    parquet = source / "bars.parquet"
    sidecar = source / "ledger.sqlite-wal"
    database.write_bytes(b"sqlite")
    parquet.write_bytes(b"parquet")
    sidecar.write_bytes(b"excluded-sidecar")
    target = tmp_path / "missing-parent" / "target"
    free_bytes = 64 * 1024 * 1024
    usage_paths = []
    monkeypatch.setattr(
        data_migration.shutil,
        "disk_usage",
        lambda path: usage_paths.append(Path(path)) or SimpleNamespace(free=free_bytes),
    )
    original_open = Path.open

    def reject_payload_open(path, *args, **kwargs):
        if path in {database, parquet, sidecar}:
            raise AssertionError(f"preflight opened payload: {path}")
        return original_open(path, *args, **kwargs)

    def reject_sqlite_open(*args, **kwargs):
        raise AssertionError("preflight opened SQLite")

    def reject_mkdir(*args, **kwargs):
        raise AssertionError("preflight created a directory")

    monkeypatch.setattr(Path, "open", reject_payload_open)
    monkeypatch.setattr(Path, "mkdir", reject_mkdir)
    monkeypatch.setattr(data_migration.sqlite3, "connect", reject_sqlite_open)

    result = data_migration.preflight_data_root_migration(source, target, "copy")

    assert result.source == source.resolve()
    assert result.target == target.resolve()
    assert result.mode == "copy"
    assert result.file_count == 2
    assert result.total_bytes == len(b"sqliteparquet")
    assert result.required_bytes == result.total_bytes + 16 * 1024 * 1024
    assert result.free_bytes == free_bytes
    assert usage_paths == [tmp_path.resolve()]
    assert not target.parent.exists()
    with pytest.raises(FrozenInstanceError):
        result.total_bytes = 0
    monkeypatch.setattr(
        data_migration.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(free=result.required_bytes - 1),
    )
    with pytest.raises(MigrationError, match="剩余空间不足"):
        data_migration.preflight_data_root_migration(source, target, "copy")


def test_switch_preflight_does_not_probe_disk_capacity(tmp_path, monkeypatch):
    source, target = tmp_path / "source", tmp_path / "existing"
    source.mkdir()
    target.mkdir()

    def reject_disk_usage(path):
        raise AssertionError(f"switch preflight probed disk capacity: {path}")

    monkeypatch.setattr(data_migration.shutil, "disk_usage", reject_disk_usage)

    result = data_migration.preflight_data_root_migration(source, target, "switch")

    assert result.required_bytes == 0
    assert result.free_bytes is None


def test_data_migration_preflight_preserves_fail_closed_boundaries(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    payload = source / "payload.parquet"
    payload.write_bytes(b"payload")
    target = tmp_path / "target"

    with pytest.raises(MigrationError, match="绝对路径"):
        data_migration.preflight_data_root_migration("relative", target, "copy")
    with pytest.raises(MigrationError, match="嵌套"):
        data_migration.preflight_data_root_migration(source, source / "nested", "copy")
    with pytest.raises(MigrationError, match="mode"):
        data_migration.preflight_data_root_migration(source, target, "move")

    original_is_symlink = Path.is_symlink
    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda path: path == payload or original_is_symlink(path),
    )
    with pytest.raises(MigrationError, match="符号链接"):
        data_migration.preflight_data_root_migration(source, target, "copy")


def test_data_migration_manager_reuses_public_preflight(tmp_path, monkeypatch):
    source, target = tmp_path / "source", tmp_path / "existing"
    source.mkdir()
    target.mkdir()
    cfg = Config()
    cfg.data.root = str(source)
    set_config(cfg)
    calls = []
    original = data_migration.preflight_data_root_migration

    def tracked_preflight(source_path, target_path, mode):
        calls.append((source_path, target_path, mode))
        return original(source_path, target_path, mode)

    monkeypatch.setattr(data_migration, "preflight_data_root_migration", tracked_preflight)
    manager = DataMigrationManager(RootSwitcher())
    task = manager.create(target, "switch")
    for _ in range(100):
        result = manager.get(task["id"])
        if result["status"] not in {"pending", "running", "cancelling"}:
            break
        time.sleep(0.01)

    assert result["status"] == "completed"
    assert calls == [(str(source), target, "switch")]


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


def test_universe_members_uses_one_batched_master_lookup(monkeypatch):
    from quantmaster.data import instruments
    from quantmaster.server import management

    class Entry:
        def __init__(self, symbol):
            self.name = f"名称-{symbol}"
            self.market = "cn"
            self.exchange = "SSE"
            self.asset_type = "stock"
            self.status = "active"
            self.source = "fixture"

    class Store:
        instances = 0
        batches: ClassVar[list[list[str]]] = []

        def __init__(self, *, read_only):
            assert read_only is True
            type(self).instances += 1

        def get_many(self, symbols):
            values = list(symbols)
            type(self).batches.append(values)
            return {value.upper(): Entry(value) for value in values}

        def get(self, symbol):  # pragma: no cover - 不应退回单项查询
            raise AssertionError(f"unexpected single lookup: {symbol}")

    monkeypatch.setattr(instruments, "InstrumentStore", Store)

    members = management._universe_members(["600519.SH", "000001.SZ", "600519.SH"])

    assert Store.instances == 1
    assert Store.batches == [["600519.SH", "000001.SZ", "600519.SH"]]
    assert [item["symbol"] for item in members] == ["600519.SH", "000001.SZ", "600519.SH"]
    assert [item["name"] for item in members] == [
        "名称-600519.SH", "名称-000001.SZ", "名称-600519.SH",
    ]


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

    isolated_config.data.free_stockdb_managed = False
    manager = DataRefreshManager()
    manager.initialize()
    symbols = ["600000.SH", "000001.SZ"]
    monkeypatch.setattr(manager, "_resolve_symbols", lambda *args: symbols)
    monkeypatch.setattr(manager, "_start", lambda job_id: None)
    calls = []
    fail_once = {"000001.SZ"}

    def fake_load(symbol, start, end, **kwargs):
        calls.append((symbol, start, end, kwargs))
        if symbol in fail_once:
            fail_once.remove(symbol)
            raise OSError("offline")
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

    monkeypatch.setattr("quantmaster.data.maintenance.refresh_history", fake_load)
    job = manager.create("market")
    assert job["status"] == "queued"
    assert manager.latest()["id"] == job["id"]

    manager._run(job["id"])
    completed = manager.get(job["id"])
    assert completed["status"] == "completed"
    assert completed["outcome"] == "completed_with_warnings"
    assert completed["failed"] == 1
    assert completed["can_retry"] is True
    assert {item[0] for item in calls} == set(symbols)
    assert all(item[3]["mode"] == RefreshMode.AUTO for item in calls)
    assert all(item[3]["work_class"] == "maintenance" for item in calls)

    # 领域 outcome 保留失败标的；统一 lifecycle retry 只重跑失败项。
    resumed = manager.resume(job["id"])
    assert resumed["status"] == "queued"
    assert resumed["attempt"] == 2
    manager._run(job["id"])
    retried = manager.get(job["id"])
    assert retried["status"] == "completed"
    assert retried["outcome"] == "completed"
    assert retried["total"] == 1
    assert retried["can_retry"] is False
    assert [item[0] for item in calls].count("600000.SH") == 1
    assert [item[0] for item in calls].count("000001.SZ") == 2


def test_incremental_refresh_job_limits_per_job_parallelism(isolated_config, monkeypatch):
    from quantmaster.data.maintenance import DataRefreshManager

    isolated_config.data.free_stockdb_managed = False
    manager = DataRefreshManager()
    manager.initialize()
    symbols = [f"{index:06d}.SZ" for index in range(16)]
    monkeypatch.setattr(manager, "_resolve_symbols", lambda *args: symbols)
    monkeypatch.setattr(manager, "_start", lambda job_id: None)
    monkeypatch.setattr(manager, "_publish_market_snapshot", lambda: None)

    active = 0
    peak_active = 0
    lock = threading.Lock()
    second_worker_started = threading.Event()

    def fake_load(symbol, start, end, **kwargs):
        nonlocal active, peak_active
        with lock:
            active += 1
            peak_active = max(peak_active, active)
            if active >= 2:
                second_worker_started.set()
        try:
            assert second_worker_started.wait(timeout=2.0)
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
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr("quantmaster.data.maintenance.refresh_history", fake_load)
    job = manager.create("market")
    manager._run(job["id"])

    completed = manager.get(job["id"])
    assert completed["status"] == "completed"
    assert completed["next_index"] == len(symbols)
    assert peak_active > 1
    assert peak_active <= manager.MAX_PARALLEL_SYMBOLS


def test_refresh_manager_creates_schema_after_hot_root_switch(isolated_config, tmp_path):
    from quantmaster.data.maintenance import DataRefreshManager

    manager = DataRefreshManager()
    isolated_config.data.root = str(tmp_path / "switched")
    # Web/status construction is inert after a root change; only the
    # runtime-worker startup path is allowed to publish the task schema.
    assert manager.latest() is None
    assert not (isolated_config.data_root / "jobs.sqlite").exists()
    manager.initialize()
    assert (isolated_config.data_root / "jobs.sqlite").exists()
    assert not (isolated_config.data_root / "data_refresh.sqlite").exists()


def test_refresh_manager_only_recovers_expired_foreign_lease(isolated_config, monkeypatch):
    from quantmaster.data.maintenance import DataRefreshManager

    first = DataRefreshManager()
    second = DataRefreshManager()
    first.initialize()
    second.initialize()
    monkeypatch.setattr(first, "_resolve_symbols", lambda *args: ["600000.SH"])
    monkeypatch.setattr(first, "_start", lambda job_id: None)
    job = first.create("market")
    store = first._ensure_runtime().store
    assert store.claim(job["id"], "fixture-owner", lease_seconds=60)
    second.initialize()
    assert second.get(job["id"])["status"] == "running"

    future = time.time() + 120
    monkeypatch.setattr("quantmaster.runtime.jobs.time.time", lambda: future)
    second._ensure_runtime().store.recover_expired()
    assert second.get(job["id"])["status"] == "interrupted"


def test_refresh_slow_symbol_does_not_block_checkpoint_or_next_symbol(isolated_config, monkeypatch):
    from quantmaster.data.maintenance import REFRESH_CHECKPOINT, DataRefreshManager

    manager = DataRefreshManager()
    manager.initialize()
    monkeypatch.setattr(manager, "MAX_PARALLEL_SYMBOLS", 2)
    monkeypatch.setattr(manager, "_resolve_symbols", lambda *args: ["A", "B", "C"])
    monkeypatch.setattr(manager, "_start", lambda _: None)
    monkeypatch.setattr(manager, "_publish_market_snapshot", lambda: None)
    release_a, started_c = threading.Event(), threading.Event()

    def refresh(store, symbol, start, end):
        if symbol == "A":
            assert release_a.wait(10)
        if symbol == "C":
            started_c.set()
        return None

    monkeypatch.setattr(manager, "_refresh_one", refresh)
    job = manager.create("market")
    runtime = manager._ensure_runtime()
    runtime.dispatch_job(job["id"])
    try:
        assert started_c.wait(5), "C must start while A is still blocked"
        spec_hash = runtime.store.get(job["id"])["spec_hash"]
        checkpoint = runtime.store.checkpoint(job["id"], REFRESH_CHECKPOINT, spec_hash)
        assert "B" in checkpoint["completed_symbols"]
        assert "A" not in checkpoint["completed_symbols"]
        assert manager.get(job["id"])["next_index"] >= 1
    finally:
        release_a.set()
        runtime.wait(job["id"], timeout=10)
        manager.shutdown()
    assert manager.get(job["id"])["succeeded"] == 3


def test_refresh_resume_skips_durable_success_and_rejects_legacy_checkpoint(isolated_config, monkeypatch):
    from quantmaster.data.maintenance import REFRESH_SCHEMA, DataRefreshManager

    saved = {"schema_version": REFRESH_SCHEMA, "symbols": ["A", "B", "C"],
             "completed_symbols": ["B"], "next_index": 1, "succeeded": 1,
             "failures": [], "blocked_failures": []}
    context = SimpleNamespace(
        attempt=1, spec_hash="fixture", ensure_active=lambda: None,
        write_checkpoint=lambda *args: None, progress=lambda *args: None,
        completed_unit=lambda *args: None,
    )
    calls = []
    manager = DataRefreshManager()
    monkeypatch.setattr(manager, "_refresh_one", lambda store, symbol, *args: calls.append(symbol))
    manager._execute(context, {"scope": "market", "start": "2024-01-01", "end": "2024-02-01"}, saved)
    assert set(calls) == {"A", "C"}
    assert saved["succeeded"] == 3
    with pytest.raises(ValueError, match="REFRESH_SCHEMA_UNSUPPORTED"):
        manager._initial_state(context, {"symbols": ["A"]})
    context.attempt = 1
    context.job_id = "fixture"
    context.store = SimpleNamespace(latest_artifact=lambda *args: None)
    context.load_checkpoint = lambda *args: {"schema_version": "1.0", "next_index": 1}
    with pytest.raises(ValueError, match="REFRESH_SCHEMA_UNSUPPORTED"):
        manager._initial_state(context, {"refresh_schema": REFRESH_SCHEMA})
    # A retry interrupted after saving B resumes that checkpoint, not the older failure result.
    context.attempt = 3
    saved["attempt"] = 2
    context.load_checkpoint = lambda *args: saved
    context.store.latest_artifact = lambda *args: {"payload": {
        "attempt": 1, "failures": [{"symbol": "B", "retryable": True}],
    }}
    assert manager._initial_state(context, {"refresh_schema": REFRESH_SCHEMA}) == saved


def test_refresh_reuses_result_without_hiding_evidence_failure(isolated_config, monkeypatch):
    from quantmaster.data.maintenance import DataRefreshManager

    manager = DataRefreshManager()
    manager.initialize()
    generation = ["first"]
    calls = []
    monkeypatch.setattr(manager, "_fingerprint", lambda symbols: generation[0])
    monkeypatch.setattr(manager, "_resolve_symbols", lambda *args: ["A"])
    monkeypatch.setattr(manager, "_start", lambda _: None)
    monkeypatch.setattr(manager, "_publish_market_snapshot", lambda: None)

    def refresh(*args):
        calls.append(1)
        generation[0] = "after-local-write"
        return {"error": "unit evidence missing", "code": "evidence_missing", "retryable": False}

    monkeypatch.setattr(manager, "_refresh_one", refresh)
    job = manager.create("market")
    # Intermediate local writes do not create a duplicate active task.
    generation[0] = "during-write"
    assert manager.create("market")["id"] == job["id"]
    manager._run(job["id"])
    reused = manager.create("market")
    assert reused["id"] == job["id"]
    assert reused["reused"] is True
    assert reused["outcome"] == "completed_with_warnings"
    assert reused["can_retry"] is False
    assert calls == [1]
    with pytest.raises(ValueError, match="不能续跑"):
        manager.resume(job["id"])
    generation[0] = "new-source-generation"
    assert manager.create("market")["id"] != job["id"]
    manager.shutdown()


def test_refresh_failure_classification_preserves_quality_gate():
    from quantmaster.data.base import MarketDataUnavailable
    from quantmaster.data.maintenance import DataRefreshManager

    blocked = MarketDataUnavailable(BarDataQuality(
        status="degraded", requested_start="2024-01-01", requested_end="2024-02-01",
        issues=("continuous_contract_unconfirmed",),
    ))
    assert DataRefreshManager._failure(blocked)["retryable"] is False
    assert DataRefreshManager._failure(ConnectionError("offline"))["retryable"] is True
    assert DataRefreshManager._failure(PermissionError("permission denied"))["retryable"] is False


def test_refresh_worker_intake_tracks_consumed_scopes(isolated_config, monkeypatch):
    from quantmaster.data.maintenance import DataRefreshManager

    isolated_config.automation.watchlist = ["600000.SH"]
    isolated_config.automation.primary_universe = "selected"
    isolated_config.lab.enabled = True
    isolated_config.lab.universe = "research"
    calls = []
    manager = DataRefreshManager()
    monkeypatch.setattr(manager, "create", lambda scope, **kw: calls.append((scope, kw)) or {})
    monkeypatch.setattr(manager, "_submit", lambda *args: calls.append(args) or {})
    assert len(manager.maintain_consumed()) == 4
    assert ("market", {}) in calls
    assert ("universe", {"universe": "selected"}) in calls
    assert ("universe", {"universe": "research"}) in calls
    assert any(call[0] == "watchlist" for call in calls)


def test_ordinary_refresh_reuses_fresh_cache_without_provider_calls(isolated_config, monkeypatch):
    from quantmaster.data import registry
    from quantmaster.data.maintenance import DataRefreshManager
    from quantmaster.data.storage import BarStore
    from quantmaster.trading_sessions import market_date

    end = str(market_date())
    store = BarStore()
    data = pd.DataFrame({"open": [10.0], "high": [11.0], "low": [9.0],
                         "close": [10.0], "volume": [100.0]}, index=pd.to_datetime([end]))
    store.put("600000.SH", data, request_start=end, request_end=end)

    provider_calls = []

    def reject_fetch(*args, **kwargs):
        provider_calls.append(1)
        raise AssertionError("fresh complete cache must not acquire a provider")

    monkeypatch.setattr(registry, "_fetch_segment", reject_fetch)
    monkeypatch.setattr(registry, "_full_refresh", reject_fetch)
    # Quality may remain degraded; the point is zero provider work, not fabricated eligibility.
    DataRefreshManager._refresh_one(store, "600000.SH", end, end)
    DataRefreshManager._refresh_one(store, "600000.SH", end, end)
    assert provider_calls == []


def test_refresh_fingerprint_expires_and_tracks_local_source_changes(isolated_config, monkeypatch):
    from quantmaster.data import maintenance
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    clock = [7200.0]
    generation = [()]
    monkeypatch.setattr(maintenance.time, "time", lambda: clock[0])
    monkeypatch.setattr(free_stockdb_runtime, "_data_fingerprint", lambda root: generation[0])
    first = maintenance.DataRefreshManager._fingerprint(["A"])
    assert maintenance.DataRefreshManager._fingerprint(["A"]) == first
    generation[0] = (("data/current", 100, 200),)
    changed = maintenance.DataRefreshManager._fingerprint(["A"])
    assert changed != first
    clock[0] += 3600
    assert maintenance.DataRefreshManager._fingerprint(["A"]) != changed


def test_refresh_cancel_stops_dispatch_and_retry_has_backoff(isolated_config, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from quantmaster.data.maintenance import DataRefreshManager

    manager = DataRefreshManager()
    monkeypatch.setattr(manager, "MAX_PARALLEL_SYMBOLS", 1)
    calls = []
    cancelled = [False]

    def active():
        if cancelled[0]:
            raise InterruptedError("cancelled")

    def checkpoint(*args):
        cancelled[0] = True

    context = SimpleNamespace(
        attempt=1, spec_hash="fixture", ensure_active=active, write_checkpoint=checkpoint,
        progress=lambda *args: None, completed_unit=lambda *args: None,
    )
    state = {"symbols": ["A", "B"], "completed_symbols": [], "succeeded": 0, "failures": []}
    monkeypatch.setattr(manager, "_refresh_one", lambda store, symbol, *args: calls.append(symbol))
    with pytest.raises(InterruptedError):
        manager._execute(context, {"scope": "market", "start": "2024-01-01", "end": "2024-02-01"}, state)
    assert calls == ["A"]
    assert state["completed_symbols"] == ["A"]
    now = datetime.now(UTC)
    assert not manager._retry_due({"can_retry": True, "finished_at": now.isoformat(), "attempt": 1})
    older = (now - timedelta(seconds=65)).isoformat()
    assert manager._retry_due({"can_retry": True, "finished_at": older, "attempt": 1})
    assert not manager._retry_due({"can_retry": True, "finished_at": older, "attempt": 2})
    assert not manager._retry_due({"can_retry": False, "finished_at": older, "attempt": 1})
