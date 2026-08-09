from __future__ import annotations

import time
from pathlib import Path

import pandas as pd

from quantmaster.data import registry
from quantmaster.data.base import DataSource, Market
from quantmaster.data.repair import DataRepairManager
from quantmaster.data.storage import BarStore
from quantmaster.research.contracts import ArtifactKind, AssetClass, Frequency
from quantmaster.research.lake import ResearchDataIntegrityError, ResearchLake


def test_repair_queue_deduplicates_and_keeps_immutable_spec(
    tmp_path, isolated_config,
):
    manager = DataRepairManager(tmp_path / "repairs.sqlite")
    first = manager.enqueue(
        "example", "same-target", reason="first", spec={"version": 1}, source="test",
    )
    second = manager.enqueue(
        "example", "same-target", reason="new evidence", spec={"version": 2}, source="test",
    )

    assert first["id"] == second["id"]
    assert second["spec"] == {"version": 1}
    assert second["reason"] == "new evidence"
    assert len(manager.list()) == 1


def test_repair_queue_applies_backoff_and_daily_source_budget(
    tmp_path, isolated_config,
):
    isolated_config.data.repair_daily_budget = 1
    isolated_config.data.repair_retry_backoff = 30
    manager = DataRepairManager(tmp_path / "repairs.sqlite")
    manager.register_handler("broken", lambda _item: (_ for _ in ()).throw(OSError("offline")))
    first = manager.enqueue("broken", "one", reason="bad", spec={}, source="provider")
    manager.enqueue("broken", "two", reason="bad", spec={}, source="provider")

    failed = manager.run_one()
    assert failed is not None
    assert failed["id"] == first["id"]
    assert failed["status"] == "queued"
    assert failed["next_run"] > time.time()
    assert "OSError: offline" in failed["last_error"]
    assert manager.run_one() is None


def test_corrupt_bar_is_quarantined_refetched_and_audited(
    tmp_path, isolated_config, monkeypatch,
):
    from quantmaster.data import repair

    manager = DataRepairManager(tmp_path / "repairs.sqlite")
    monkeypatch.setattr(repair, "_MANAGER", manager)
    store = BarStore(root=tmp_path / "bars")
    dates = pd.bdate_range("2024-01-02", "2024-01-12")
    original = pd.DataFrame({
        "open": 10.0, "high": 10.0, "low": 10.0,
        "close": 10.0, "volume": 100.0,
    }, index=dates)
    store.put("600000.SH", original)
    original.assign(close=99.0).to_parquet(store.path_for_repair("600000.SH"))

    read = store.read("600000.SH")
    assert read.status == "corrupt"
    assert read.frame is None
    jobs = manager.list()
    assert len(jobs) == 1
    assert jobs[0]["kind"] == "bar"

    class HealthySource(DataSource):
        name = "healthy"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            return original.copy()

    monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [HealthySource]})
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda _start, _end: (pd.DatetimeIndex(dates), "test-calendar"),
    )
    monkeypatch.setattr(
        registry,
        "_unit_contract",
        lambda _symbol: (
            (
                ("open", "CNY/share"), ("high", "CNY/share"),
                ("low", "CNY/share"), ("close", "CNY/share"),
                ("volume", "share"), ("amount", "CNY"),
            ),
            "",
        ),
    )
    completed = manager.run_one()

    assert completed is not None
    assert completed["status"] == "completed"
    assert completed["result"]["rows"] == len(original)
    assert completed["result"]["quarantine"]["original_path"].endswith("600000.SH.parquet")
    assert len(completed["result"]["quarantine"]["content_sha256"]) == 64
    assert completed["result"]["quarantine"]["file_size"] > 0
    quarantine = isolated_config.data_root / "quarantine" / "bars"
    assert len(list(quarantine.rglob("*.quarantine"))) == 1
    assert len(list(quarantine.rglob("*.quarantine.json"))) == 1
    assert store.read("600000.SH", enqueue_repair=False).status == "ready"
    assert [event["type"] for event in manager.events(completed["id"])] == [
        "queued", "claimed", "completed",
    ]


def test_legacy_bar_filename_is_migrated_and_failed_repair_reconciled(
    tmp_path, isolated_config, monkeypatch,
):
    from quantmaster.data import repair

    isolated_config.data.repair_max_attempts = 1
    manager = DataRepairManager(tmp_path / "repairs.sqlite")
    manager.register_handler(
        "bar", lambda _item: (_ for _ in ()).throw(OSError("offline")),
    )
    monkeypatch.setattr(repair, "_MANAGER", manager)
    store = BarStore(root=isolated_config.data_root / "bars")
    symbol = "HG=F.US"
    dates = pd.bdate_range("2024-01-02", "2024-01-12")
    original = pd.DataFrame({"close": 10.0, "volume": 100.0}, index=dates)
    store.put(symbol, original)
    current = store.path_for_repair(symbol)
    legacy = current.with_name("HG_F.US.parquet")
    current.replace(legacy)
    store.mark_status(symbol, "corrupt")
    target = f"{store.root.resolve()}::{symbol}"
    queued = manager.enqueue(
        "bar", target, reason="cataloged bar file is missing",
        spec={"root": str(store.root.resolve()), "symbol": symbol}, source="test",
    )
    failed = manager.run_one()

    assert failed is not None and failed["status"] == "failed"
    result = store.read(symbol)
    resolved = manager.get(queued["id"])

    assert result.status == "ready"
    assert result.frame is not None and len(result.frame) == len(original)
    assert current.is_file() and not legacy.exists()
    assert store.metadata(symbol)["last_status"] == "ready"
    assert resolved["status"] == "completed"
    assert resolved["result"]["reason"] == "legacy_filename_migrated"
    assert manager.events(queued["id"])[-1]["type"] == "resolved_by_validation"


def test_research_integrity_failure_enqueues_one_repair(
    tmp_path, isolated_config, monkeypatch,
):
    from quantmaster.data import repair

    manager = DataRepairManager(tmp_path / "repairs.sqlite")
    monkeypatch.setattr(repair, "_MANAGER", manager)
    lake = ResearchLake(tmp_path / "lake")
    frame = pd.DataFrame({
        "trade_date": ["2024-01-02"], "symbol": ["600000.SH"], "close": [10.0],
    })
    lake.write_partition(
        ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
        "stock_bars", "2024-01-02", frame,
    )
    path = lake.partition_path(
        ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
        "stock_bars", "2024-01-02",
    )
    frame.assign(close=99.0).to_parquet(path, index=False)

    for _ in range(2):
        try:
            lake.read_partition(
                ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
                "stock_bars", "2024-01-02",
            )
        except ResearchDataIntegrityError:
            pass

    jobs = manager.list()
    assert len(jobs) == 1
    assert jobs[0]["kind"] == "research_partition"
    assert jobs[0]["spec"]["metadata"]["content_sha256"]


def test_corrupt_endpoint_cache_is_quarantined_and_uses_global_repair_queue(
    tmp_path, isolated_config, monkeypatch,
):
    from quantmaster.data import repair
    from quantmaster.data.resilience import EndpointFrameCache

    manager = DataRepairManager(tmp_path / "repairs.sqlite")
    monkeypatch.setattr(repair, "_MANAGER", manager)
    cache = EndpointFrameCache("akshare_stock_research", root=tmp_path / "api-cache")
    endpoint = "stock_financial_abstract"
    params = {"symbol": "600519"}
    path = cache.path_for(endpoint, params)
    path.write_bytes(b"not-a-parquet-file")

    assert cache.get(endpoint, params, ttl_days=7) is None
    assert not path.exists()
    jobs = manager.list()
    assert len(jobs) == 1
    assert jobs[0]["kind"] == "api_cache"
    manifest = jobs[0]["spec"]["quarantine"]
    assert manifest["content_sha256"]
    assert Path(manifest["quarantine_path"]).is_file()

    replacement = pd.DataFrame({"metric": [1.0]})
    cache.put(endpoint, params, replacement)
    completed = manager.run_one()
    assert completed is not None
    assert completed["status"] == "completed"
    assert completed["result"]["state"] == "replaced"
    assert completed["result"]["rows"] == 1
