"""Repeated local failures must not invalidate overlapping completed intake."""

import json
from datetime import date, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

from quantmaster.data import maintenance
from quantmaster.data.storage import BarStore


def _seed(store, symbol):
    store.put(symbol, pd.DataFrame(
        {"close": [10.0]}, index=pd.to_datetime(["2026-09-17"]),
    ), source="free-stockdb", quality={
        "status": "degraded", "stale": False, "coverage_ratio": 0.5,
        "requested_start": "2015-01-01", "requested_end": "2026-09-17",
        "observed_start": "2015-05-26", "observed_end": "2026-09-17",
        "formal_eligible": False,
    })


def test_repeated_failed_status_preserves_inputs_and_real_changes_invalidate(
    isolated_config, monkeypatch,
):
    isolated_config.data.free_stockdb_root = str(isolated_config.data_root / "stockdb")
    store = BarStore()
    _seed(store, "A")
    store.mark_status("A", "stale")
    before = store.metadata("A")
    fingerprint = maintenance.DataRefreshManager._fingerprint(["A"])
    statements = []
    connect = store._conn

    def traced():
        connection = connect()
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(store, "_conn", traced)
    for _ in range(3):
        store.mark_status("A", "stale")
    assert not any(sql.startswith("UPDATE") for sql in statements)
    assert sum(sql.startswith("SELECT quality_json") for sql in statements) == 3
    assert store.metadata("A") == before
    assert maintenance.DataRefreshManager._fingerprint(["A"]) == fingerprint
    assert json.loads(before["quality_json"])["formal_eligible"] is False

    # Source/status changes remain evidence, even with identical bar bytes.
    store.mark_status("A", "stale", source="other-local-source")
    assert maintenance.DataRefreshManager._fingerprint(["A"]) != fingerprint
    changed = store.metadata("A")
    assert len(json.loads(changed["source_chain_json"])) == 3
    assert changed["checked_at"] == before["checked_at"]
    fingerprint = maintenance.DataRefreshManager._fingerprint(["A"])
    store.mark_status("A", "refresh_failed")
    assert maintenance.DataRefreshManager._fingerprint(["A"]) != fingerprint

    # A successful check still renews freshness and quality through mark_checked.
    store.mark_checked("A", "2015-01-01", "2026-09-17", quality={"status": "verified"})
    assert store.metadata("A")["checked_at"] >= before["checked_at"]
    assert store.quality("A") == {"status": "verified"}


def test_overlapping_automatic_intake_reuses_unchanged_failed_inputs(
    isolated_config, monkeypatch,
):
    cfg = isolated_config
    cfg.data.free_stockdb_root = str(cfg.data_root / "stockdb")
    cfg.automation.primary_universe = "hs300"
    cfg.automation.watchlist = []
    cfg.lab.enabled = True
    cfg.lab.universe = "csi800"
    store = BarStore()
    for symbol in ("A", "B", "C"):
        _seed(store, symbol)
    monkeypatch.setattr(maintenance, "market_symbols", lambda: ["C"])
    monkeypatch.setattr("quantmaster.data.universe.load_universe", lambda _: ["A"])
    monkeypatch.setattr("quantmaster.data.universe.load_universe_snapshot", lambda _: SimpleNamespace(
        content_hash="members-1", symbols=["A"],
    ))
    manager = maintenance.DataRefreshManager()
    monkeypatch.setattr(manager, "_resolve_symbols", lambda scope, *args: (
        ["C"] if scope == "market" else ["A", "B"]
    ))
    monkeypatch.setattr(manager, "_stockdb_wait_reason", lambda: "")
    monkeypatch.setattr(manager, "_uses_stockdb", lambda _: False)
    monkeypatch.setattr(manager, "_start", lambda _: None)
    monkeypatch.setattr(manager, "_publish_market_snapshot", lambda: None)
    calls = []

    def refresh(bar_store, symbol, *args):
        calls.append(symbol)
        bar_store.mark_status(symbol, "stale")
        return {"error": "historical evidence missing", "code": "evidence_missing", "retryable": False}

    monkeypatch.setattr(manager, "_refresh_one", refresh)
    try:
        first = manager.maintain_consumed()
        for job in first:
            manager._run(job["id"])
        assert sorted(calls) == ["A", "A", "B", "C"]
        for _ in range(3):
            reused = manager.maintain_consumed()
            assert [job["id"] for job in reused] == [job["id"] for job in first]
            assert all(job["reused"] and not job["can_retry"] for job in reused)
            assert all(job["failed"] == job["total"] for job in reused)
        assert len(calls) == 4  # No additional bar reads/fetches in stable intake rounds.
        before = maintenance.DataRefreshManager._fingerprint(["A"])
        store.put("A", pd.DataFrame({"close": [11.0]}, index=pd.to_datetime(["2026-09-17"])))
        assert maintenance.DataRefreshManager._fingerprint(["A"]) != before
        assert manager.maintain_consumed()[1]["id"] != first[1]["id"]
    finally:
        manager.shutdown()


@pytest.mark.parametrize("changed_input", ["source", "config", "membership", "day", "quality"])
def test_real_input_changes_still_invalidate(isolated_config, monkeypatch, changed_input):
    root = isolated_config.data_root
    isolated_config.data.free_stockdb_root = str(root / "stockdb")
    store = BarStore()
    _seed(store, "A")
    store.mark_status("A", "stale")
    today = date(2026, 9, 17)
    monkeypatch.setattr(maintenance, "market_date", lambda: today)
    original = maintenance.DataRefreshManager._fingerprint(["A"])
    if changed_input == "source":
        source = root / "stockdb" / "data"
        source.mkdir(parents=True)
        (source / "generation").write_text("new local data", encoding="utf-8")
    elif changed_input == "config":
        isolated_config.data.cache_days += 1
    elif changed_input == "membership":
        membership = root / "universe"
        membership.mkdir()
        (membership / "selected.json").write_text('{"symbols": ["A", "B"]}', encoding="utf-8")
    elif changed_input == "day":
        monkeypatch.setattr(maintenance, "market_date", lambda: today + timedelta(days=1))
    else:
        store.mark_checked("A", "2015-01-01", str(today), quality={"formal_eligible": True})
    assert maintenance.DataRefreshManager._fingerprint(["A"]) != original


def test_refresh_algorithm_change_rejects_old_failures_and_checkpoints(isolated_config, monkeypatch):
    manager = maintenance.DataRefreshManager()
    current = maintenance.REFRESH_SCHEMA
    assert current != '3.0'
    monkeypatch.setattr(manager, '_start', lambda _: None)
    monkeypatch.setattr(manager, '_stockdb_wait_reason', lambda: '')
    monkeypatch.setattr(manager, '_uses_stockdb', lambda _: False)
    monkeypatch.setattr(manager, '_publish_market_snapshot', lambda: None)
    monkeypatch.setattr(manager, '_fingerprint', lambda _: 'unchanged-inputs-same-hour')
    calls = []

    def refresh(*args):
        calls.append(maintenance.REFRESH_SCHEMA)
        return {'error': 'provider resources unavailable', 'code': 'evidence_missing', 'retryable': False}

    monkeypatch.setattr(manager, '_refresh_one', refresh)
    try:
        monkeypatch.setattr(maintenance, 'REFRESH_SCHEMA', '3.0')
        old = manager._submit('market', '', '2026-09-01', '2026-09-17', ['TEST.US'])
        manager._run(old['id'])
        monkeypatch.setattr(maintenance, 'REFRESH_SCHEMA', current)
        new = manager._submit('market', '', '2026-09-01', '2026-09-17', ['TEST.US'])
        assert new['id'] != old['id'] and new['created']
        manager._run(new['id'])
        repeated = manager._submit('market', '', '2026-09-01', '2026-09-17', ['TEST.US'])
        assert repeated['id'] == new['id'] and repeated['reused']
        assert calls == ['3.0', current]
        context = SimpleNamespace(spec_hash='same', load_checkpoint=lambda *args: {'schema_version': '3.0'})
        with pytest.raises(ValueError, match='REFRESH_SCHEMA_UNSUPPORTED'):
            manager._initial_state(context, {'refresh_schema': current})
        with pytest.raises(ValueError, match='REFRESH_SCHEMA_UNSUPPORTED'):
            manager._initial_state(context, {'refresh_schema': '3.0'})
    finally:
        manager.shutdown()
