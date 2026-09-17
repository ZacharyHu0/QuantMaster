"""Offline contracts for explicit, atomic same-source history recovery."""

from __future__ import annotations

import json
import sqlite3

import pandas as pd
import pytest

from quantmaster.data import registry
from quantmaster.data.base import HistoryRepairError
from quantmaster.data.resilience import EndpointFrameCache, ProviderContractChanged
from quantmaster.data.storage import BarStore
from quantmaster.data.tushare_source import TushareSource
from quantmaster.lab.errors import classify_lab_error

SYMBOL = "000938.SZ"
START, OLD_END, END = "2024-01-02", "2024-01-12", "2024-01-19"


@pytest.fixture
def repair_setup(tmp_path, isolated_config, monkeypatch):
    from quantmaster.data.instruments import InstrumentStore

    InstrumentStore().upsert(
        [
            {
                "symbol": SYMBOL,
                "code": "000938",
                "market": "CN",
                "asset_type": "stock",
                "exchange": "SZ",
                "currency": "CNY",
            }
        ],
        source="test",
        source_priority=100,
    )
    days = pd.bdate_range(START, END)
    raw = pd.DataFrame(
        {
            "ts_code": SYMBOL,
            "trade_date": days.strftime("%Y%m%d"),
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.0,
            "vol": 100.0,
            "amount": 1000.0,
        }
    )
    factors = raw[["ts_code", "trade_date"]].assign(adj_factor=2.0)
    factors.loc[:4, "adj_factor"] = 1.0
    calls = []
    state = {"fault": "", "cancelled": False}

    class FakePro:
        def daily(self, **params):
            return self.response("daily", raw, params)

        def adj_factor(self, **params):
            return self.response("adj_factor", factors, params)

        def response(self, endpoint, data, params):
            calls.append((endpoint, params["start_date"], params["end_date"]))
            full = params["start_date"] == START.replace("-", "")
            if full and state["fault"] == "network":
                raise OSError("offline")
            value = data.loc[data.trade_date.between(params["start_date"], params["end_date"])].copy()
            if full and state["fault"] == "missing_old":
                value = value.iloc[1:]
            if full and state["fault"] == "missing_gap":
                value = value[value.trade_date != "20240118"]
            if full and state["fault"] == "identity":
                value["ts_code"] = "002558.SZ"
            if full and state["fault"] == "factor" and endpoint == "adj_factor":
                value = value.iloc[1:]
            if full and state["fault"] == "cancel":
                state["cancelled"] = True
            return value

    cache = EndpointFrameCache("tushare", root=tmp_path / "endpoint")
    source = TushareSource(cache)
    source._api = FakePro()
    monkeypatch.setattr(
        "quantmaster.data.tushare_source.provider_call", lambda lane, key, fetch, **kw: fetch()
    )
    monkeypatch.setattr("quantmaster.data.tushare_source.TUSHARE_LIMITER.wait", lambda: None)
    monkeypatch.setattr(registry, "_request_factories", lambda **kw: {registry.Market.CN: [lambda: source]})
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda start, end: (days[(days >= start) & (days <= end)], "test-calendar"),
    )
    store = BarStore(tmp_path / "bars")
    old = TushareSource._normalize_market_frame(raw).loc[START:OLD_END]
    old.attrs.update({"adjustment": "qfq", "adjustment_anchor_date": OLD_END})
    quality = registry._assess_daily_frame(old, START, OLD_END, symbol=SYMBOL, source="free-stockdb")
    store.put(
        SYMBOL,
        old,
        replace=True,
        request_start=START,
        request_end=OLD_END,
        source="free-stockdb",
        quality=quality.to_dict(),
    )
    return store, source, calls, state, old


def repair(store, **kwargs):
    return registry.refresh_history(
        SYMBOL,
        "2024-01-15",
        END,
        store=store,
        mode="incremental",
        source_name="tushare",
        repair_incompatible=True,
        **kwargs,
    )


def test_conflicting_qfq_rebuild_keeps_old_history_and_truthful_evidence(repair_setup):
    store, source, calls, _, old = repair_setup
    result = repair(store)
    saved = store.get(SYMBOL)
    assert saved.index.min() == old.index.min()
    assert old.index.difference(saved.index).empty
    assert saved.loc[START, "close"] == 5.0
    assert saved.loc[END, "close"] == 10.0
    assert len(calls) == 4  # bounded gap probe plus required consistent full range
    assert calls[-2:] == [("daily", "20240102", "20240119"), ("adj_factor", "20240102", "20240119")]
    meta = store.metadata(SYMBOL)
    assert meta["coverage_start"] == START
    assert meta["coverage_end"] == END
    assert meta["last_source"] == "tushare"
    chain = json.loads(meta["source_chain_json"])
    assert len(chain) == 1 and chain[0]["operation"] == "full_replace"
    assert result.quality.semantics.price_type.value == "forward_adjusted"
    assert result.quality.semantics.adjustment_anchor_date == END
    assert result.quality.semantics.factor_coverage == "complete"
    assert result.quality.semantics.adjustment_company_actions == ""
    assert result.quality.status == "degraded"
    assert not result.quality.formal_eligible
    pd.testing.assert_frame_equal(source.cached_daily(SYMBOL, START, END), saved)


@pytest.mark.parametrize("fault", ["missing_old", "missing_gap", "network", "identity", "factor", "cancel"])
def test_failed_or_cancelled_rebuild_preserves_bytes_and_metadata(repair_setup, fault):
    store, _, _, state, old = repair_setup
    original = store._path(SYMBOL).read_bytes()
    metadata = store.metadata(SYMBOL)
    state["fault"] = fault
    expected = InterruptedError if fault == "cancel" else HistoryRepairError
    with pytest.raises(expected):
        repair(store, cancelled=lambda: state["cancelled"])
    assert store._path(SYMBOL).read_bytes() == original
    assert store.metadata(SYMBOL) == metadata
    pd.testing.assert_frame_equal(store.get(SYMBOL), old)


def test_rebuild_reuses_trusted_full_source_cache(repair_setup):
    store, source, calls, _, _ = repair_setup
    source.daily(SYMBOL, START, END)
    calls.clear()
    repair(store)
    assert calls == []  # full trusted source cache wins before any network probe
    assert not any(left == "20240102" for _, left, _ in calls)


def test_compatible_source_stays_incremental(repair_setup):
    store, source, calls, _, _ = repair_setup
    full = source.daily(SYMBOL, START, END)
    old = full.loc[:OLD_END]
    quality = registry._assess_daily_frame(old, START, OLD_END, symbol=SYMBOL, source="tushare")
    store.put(SYMBOL, old, replace=True, replace_coverage=True, source="tushare", quality=quality.to_dict())
    source.cache = EndpointFrameCache("tushare", root=store.root.parent / "compatible-endpoint")
    calls.clear()
    repair(store)
    assert len(calls) == 2
    assert store.get(SYMBOL).index.min() == pd.Timestamp(START)
    assert json.loads(store.metadata(SYMBOL)["source_chain_json"])[-1]["operation"] == "incremental_replace"


@pytest.mark.parametrize("fault", ["rename", "metadata", "cancel_at_commit"])
def test_atomic_publication_failure_preserves_old_generation(repair_setup, monkeypatch, fault):
    store, _, _, state, _ = repair_setup
    original = store._path(SYMBOL).read_bytes()
    metadata = store.metadata(SYMBOL)
    if fault == "metadata":
        monkeypatch.setattr(
            store,
            "_commit_metadata",
            lambda *a, **kw: (_ for _ in ()).throw(sqlite3.OperationalError("blocked")),
        )
    elif fault == "rename":
        import quantmaster.data.storage as storage

        original_replace = storage.os.replace

        def denied(src, dst):
            if str(src).endswith(".parquet.tmp") and str(dst) == str(store._path(SYMBOL)):
                raise PermissionError("blocked")
            return original_replace(src, dst)

        monkeypatch.setattr(storage.os, "replace", denied)
    else:
        original_parquet = pd.DataFrame.to_parquet

        def cancel(frame, *args, **kwargs):
            result = original_parquet(frame, *args, **kwargs)
            if str(args[0]).endswith(".parquet.tmp") and str(store.root) in str(args[0]):
                state["cancelled"] = True
            return result

        monkeypatch.setattr(pd.DataFrame, "to_parquet", cancel)
    with pytest.raises(InterruptedError if fault == "cancel_at_commit" else HistoryRepairError):
        repair(store, cancelled=lambda: state["cancelled"])
    assert store._path(SYMBOL).read_bytes() == original
    assert store.metadata(SYMBOL) == metadata
    with store._conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM bar_write_intents").fetchone()[0] == 0


@pytest.mark.parametrize("bad", [0.0, -1.0, float("nan"), float("inf")])
def test_tushare_rejects_invalid_factor_before_normalization(repair_setup, bad):
    _, source, _, _, _ = repair_setup
    raw = pd.DataFrame(
        {
            "ts_code": [SYMBOL],
            "trade_date": ["20240102"],
            "open": [10],
            "high": [11],
            "low": [9],
            "close": [10],
            "vol": [1],
            "amount": [1],
        }
    )
    factors = raw[["ts_code", "trade_date"]].assign(adj_factor=bad)
    with pytest.raises(ProviderContractChanged, match="复权因子"):
        source._qfq_frame(SYMBOL, START, END, raw, factors)


def test_repair_failure_has_actionable_nonretryable_lab_classification():
    failure = classify_lab_error(HistoryRepairError(SYMBOL, "因子证据不足"))
    assert failure.code == "DATA_HISTORY_REBUILD_REQUIRED"
    assert not failure.retryable
    assert failure.action and failure.context == {"symbol": SYMBOL}


@pytest.mark.parametrize("missing", ["instrument", "units", "calendar", "numeric"])
def test_insufficient_evidence_keeps_original_cache(repair_setup, monkeypatch, missing):
    store, source, _, _, _ = repair_setup
    original = store._path(SYMBOL).read_bytes()
    metadata = store.metadata(SYMBOL)
    if missing == "units":
        monkeypatch.setattr(
            registry, "_unit_contract", lambda symbol: ((("close", "unknown"),), "unknown units")
        )
    elif missing == "calendar":
        monkeypatch.setattr(registry, "_local_sessions", lambda *args: (pd.DatetimeIndex([]), "unavailable"))
    else:
        daily = source.daily

        def incomplete(*args):
            frame = daily(*args)
            if missing == "instrument":
                frame.attrs.pop("instrument")
            else:
                frame.iloc[0, frame.columns.get_loc("close")] = float("nan")
            return frame

        monkeypatch.setattr(source, "daily", incomplete)
    with pytest.raises(HistoryRepairError):
        repair(store)
    assert store._path(SYMBOL).read_bytes() == original
    assert store.metadata(SYMBOL) == metadata


def test_ordinary_increment_still_refuses_conflicting_semantics(repair_setup):
    from quantmaster.data.semantics import SemanticContractError

    store, _, _, _, _ = repair_setup
    original = store._path(SYMBOL).read_bytes()
    with pytest.raises(SemanticContractError, match="adjustment_anchor_date"):
        registry.refresh_history(
            SYMBOL, "2024-01-15", END, store=store, mode="incremental", source_name="tushare"
        )
    assert store._path(SYMBOL).read_bytes() == original


def test_lab_terminal_retains_rebuild_classification(tmp_path, monkeypatch):
    from quantmaster.lab.errors import LabError
    from quantmaster.lab.service import LabService
    from quantmaster.lab.store import LabStore

    service = LabService(LabStore(tmp_path / "lab.sqlite"))
    plan = {
        "membership_missing": False,
        "repair_symbol_count": 1,
        "gaps": [{"symbol": SYMBOL, "segments": [{"start": START, "end": END, "kind": "critical"}]}],
    }
    monkeypatch.setattr("quantmaster.lab.service.dataset_repair_plan", lambda *args: plan)
    monkeypatch.setattr(service, "_prepare_data_space_check", lambda *args: {})

    def failed(*args, **kwargs):
        assert kwargs["repair_incompatible"] is True
        assert "cancelled" in kwargs
        raise HistoryRepairError(SYMBOL, "factor evidence missing")

    monkeypatch.setattr("quantmaster.data.refresh_history", failed)
    events = []
    with pytest.raises(LabError) as result:
        service.prepare_data(
            universe="test",
            start=START,
            end=END,
            provider="tushare",
            progress=lambda *args, **kwargs: events.append(kwargs),
        )
    assert result.value.code == "DATA_HISTORY_REBUILD_REQUIRED"
    assert not result.value.retryable
    assert any(event.get("metadata", {}).get("diagnostic_code") == result.value.code for event in events)


def test_qfq_anchor_uses_latest_factor_even_without_price_on_anchor_date(repair_setup):
    _, source, _, _, _ = repair_setup
    raw = pd.DataFrame(
        {
            "ts_code": [SYMBOL],
            "trade_date": ["20240102"],
            "open": [10],
            "high": [11],
            "low": [9],
            "close": [10],
            "vol": [1],
            "amount": [1],
        }
    )
    factors = pd.DataFrame(
        {"ts_code": [SYMBOL, SYMBOL], "trade_date": ["20240102", "20240103"], "adj_factor": [1.0, 2.0]}
    )
    result = source._qfq_frame(SYMBOL, START, END, raw, factors)
    assert result.loc[START, "close"] == 5.0
    assert result.attrs["adjustment_anchor_date"] == "2024-01-03"


def test_committed_repair_is_not_reported_failed_for_backup_cleanup(repair_setup, monkeypatch):
    from pathlib import Path

    store, _, _, _, _ = repair_setup
    unlink = Path.unlink

    def blocked_backup(path, *args, **kwargs):
        if path.suffix == ".bak":
            raise PermissionError("backup cleanup blocked")
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", blocked_backup)
    result = repair(store)
    assert result.data.index.max() == pd.Timestamp(END)
    assert store.metadata(SYMBOL)["last_source"] == "tushare"
    assert store.get(SYMBOL).loc[START, "close"] == 5.0
