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
    state = {"fault": "", "cancelled": False, "request_factories": registry._request_factories}

    class FakePro:
        def trade_cal(self, **params):
            dates = pd.date_range(params["start_date"], params["end_date"])
            return pd.DataFrame({
                "exchange": "SSE", "cal_date": dates.strftime("%Y%m%d"),
                "is_open": dates.isin(days).astype(int), "pretrade_date": "",
            })

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
            if full and (state["fault"] == "missing_old" or (
                state["fault"] == "factor" and endpoint == "adj_factor"
            )):
                value = value.iloc[1:]
            if full and state["fault"] == "missing_gap":
                value = value[value.trade_date != "20240118"]
            if full and state["fault"] == "identity":
                value["ts_code"] = "002558.SZ"
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


def test_explicit_repair_uses_official_calendar_beyond_local_snapshot(
    repair_setup, monkeypatch,
):
    store, _, _, _, _ = repair_setup
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda start, end: (pd.DatetimeIndex([]), "published-calendar"),
    )

    result = repair(store)

    assert result.data.index.max() == pd.Timestamp(END)
    assert store.get(SYMBOL).index.max() == pd.Timestamp(END)


def test_maintenance_reuses_repaired_tushare_cache_without_formal_upgrade(repair_setup, monkeypatch):
    from quantmaster.data.maintenance import DataRefreshManager

    store, _, calls, _, _ = repair_setup
    repair(store)
    calls.clear()
    monkeypatch.setattr(registry, "_request_factories", lambda **kw: pytest.fail("fresh local repair"))
    for _ in range(2):
        outcome = DataRefreshManager._refresh_one(store, SYMBOL, START, END)
        assert outcome and outcome["code"] == "formal_evidence_missing"
        assert outcome["formal_eligible"] is False and "warning" in outcome
    assert not calls
    envelope = registry.read_history(SYMBOL, START, END, store)
    assert envelope.quality.semantics.factor_coverage == "complete"
    assert not envelope.quality.formal_eligible


@pytest.mark.parametrize("missing", ["instrument", "factor_coverage", "adjustment_provider_definition"])
def test_maintenance_rejects_incomplete_repaired_source_contract(repair_setup, monkeypatch, missing):
    from quantmaster.data.maintenance import DataRefreshManager

    store, _, _, _, _ = repair_setup
    repair(store)
    frame = store.get(SYMBOL)
    frame.attrs.pop(missing)
    monkeypatch.setattr(store, "get", lambda symbol: frame)
    outcome = DataRefreshManager._refresh_one(store, SYMBOL, START, END)
    assert outcome and "error" in outcome and "warning" not in outcome


@pytest.mark.parametrize("fault", ["missing_old", "missing_gap", "network", "identity", "factor", "cancel"])
def test_failed_or_cancelled_rebuild_preserves_bytes_and_metadata(
    repair_setup, monkeypatch, fault,
):
    store, _, _, state, old = repair_setup
    original = store._path(SYMBOL).read_bytes()
    metadata = store.metadata(SYMBOL)
    state["fault"] = fault
    if fault == "missing_gap":
        monkeypatch.setattr(
            "quantmaster.data.instrument_snapshots.load_or_fetch_suspension_snapshot",
            lambda *args: {"full_day_symbols": []},
        )
    expected = InterruptedError if fault == "cancel" else HistoryRepairError
    with pytest.raises(expected):
        repair(store, cancelled=lambda: state["cancelled"])
    assert store._path(SYMBOL).read_bytes() == original
    assert store.metadata(SYMBOL) == metadata
    pd.testing.assert_frame_equal(store.get(SYMBOL), old)


def test_explicit_repair_accepts_verified_full_day_suspension(repair_setup, monkeypatch):
    store, source, _, state, _ = repair_setup
    state["fault"] = "missing_gap"
    requested = []
    published = pd.bdate_range(START, "2024-01-17")
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda start, end: (published[(published >= start) & (published <= end)], "published-calendar"),
    )

    def suspension_snapshot(selected, day):
        assert selected is source
        requested.append(day)
        return {"full_day_symbols": [SYMBOL]}

    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_or_fetch_suspension_snapshot",
        suspension_snapshot,
    )

    result = repair(store)

    assert requested == ["2024-01-18"]
    assert pd.Timestamp("2024-01-18") not in result.data.index
    assert result.data.index.max() == pd.Timestamp(END)


def test_explicit_repair_cancels_between_suspension_checks(repair_setup, monkeypatch):
    store, source, _, _, _ = repair_setup
    daily = source.daily
    cancelled = False
    requested = []
    original = store._path(SYMBOL).read_bytes()
    metadata = store.metadata(SYMBOL)
    published = pd.bdate_range(START, "2024-01-16")

    def missing_suspended_days(*args):
        return daily(*args).drop(pd.to_datetime(["2024-01-17", "2024-01-18"]), errors="ignore")

    def suspension_snapshot(selected, day):
        nonlocal cancelled
        assert selected is source
        requested.append(day)
        cancelled = True
        return {"full_day_symbols": [SYMBOL]}

    monkeypatch.setattr(source, "daily", missing_suspended_days)
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda start, end: (published[(published >= start) & (published <= end)], "published-calendar"),
    )
    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_or_fetch_suspension_snapshot",
        suspension_snapshot,
    )

    with pytest.raises(InterruptedError):
        repair(store, cancelled=lambda: cancelled)

    assert requested == ["2024-01-17"]
    assert store._path(SYMBOL).read_bytes() == original
    assert store.metadata(SYMBOL) == metadata


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
        monkeypatch.setattr(
            source,
            "trade_calendar",
            lambda *args: (_ for _ in ()).throw(RuntimeError("official calendar unavailable")),
        )
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


@pytest.mark.parametrize("status", ["stale", "refresh_failed", "degraded"])
def test_auto_does_not_reuse_checked_but_missing_tail(repair_setup, monkeypatch, status):
    store, _, calls, _, old = repair_setup
    # Reproduce the installed cache: requested coverage includes the absent tail,
    # checked_at is recent, old bytes have no trustworthy qfq contract.
    store.put(SYMBOL, old, replace=True, replace_coverage=True, source="tushare",
              request_start=START, request_end=END)
    store.mark_status(SYMBOL, status)
    monkeypatch.setattr(registry, "market_date", lambda: pd.Timestamp(END).date())
    result = registry.refresh_history(SYMBOL, START, END, store=store, mode="auto")
    assert calls
    assert result.data.index.max() == pd.Timestamp(END)
    assert old.index.difference(result.data.index).empty
    assert not result.quality.stale
    assert not result.quality.formal_eligible
    assert result.data.attrs["factor_coverage"] == "complete"
    calls.clear()
    before = store.metadata(SYMBOL)
    registry.refresh_history(SYMBOL, START, END, store=store, mode="auto")
    assert calls == []
    assert store.metadata(SYMBOL) == before


def _auto_sources(monkeypatch, source, native):
    class Local:
        name = "free-stockdb"

        def daily(self, symbol, start, end):
            return native.loc[start:end].copy()

    monkeypatch.setattr(registry, "_request_factories", lambda **kw: {
        registry.Market.CN: [lambda: source] if kw.get("provider") == "tushare"
        else [Local, lambda: source],
    })


@pytest.mark.parametrize("fault", ["", "old_date", "tail", "raw", "identity", "unit"])
def test_auto_tries_complete_local_before_same_source_repair(repair_setup, monkeypatch, fault):
    store, source, calls, _, old = repair_setup
    store.put(SYMBOL, old, replace=True, replace_coverage=True, source="tushare",
              request_start=START, request_end=END)
    store.mark_status(SYMBOL, "stale")
    native = old.reindex(pd.bdate_range(START, END)).ffill()
    native.index.name = "date"
    native.attrs = {
        "instrument": SYMBOL, "adjustment": "qfq", "provider_interface": "stock_sdk:daily",
        "unit_status": "verified_local_stockdb_schema_v1",
        "units": dict(registry._unit_contract(SYMBOL)[0]), "factor_coverage": "unconfirmed",
    }
    if fault == "old_date":
        native = native.iloc[1:]
    elif fault == "tail":
        native = native.iloc[:-1]
    elif fault == "raw":
        native.attrs["adjustment"] = "raw"
    elif fault == "identity":
        native.attrs["instrument"] = "600000.SH"
    elif fault == "unit":
        native.attrs["unit_status"] = "unknown"
    _auto_sources(monkeypatch, source, native)
    result = registry.refresh_history(SYMBOL, START, END, store=store, mode="auto")
    assert result.data.index.max() == pd.Timestamp(END)
    assert old.index.difference(result.data.index).empty
    assert not result.quality.stale and not result.quality.formal_eligible
    assert bool(calls) == bool(fault)
    assert store.metadata(SYMBOL)["last_source"] == ("tushare" if fault else "free-stockdb")


@pytest.mark.parametrize("fault", ["missing_old", "missing_gap", "network", "factor", "cancel"])
def test_auto_failed_repair_preserves_entire_old_generation(repair_setup, monkeypatch, fault):
    store, source, _, state, old = repair_setup
    store.put(SYMBOL, old, replace=True, replace_coverage=True, source="tushare",
              request_start=START, request_end=END)
    store.mark_status(SYMBOL, "stale")
    _auto_sources(monkeypatch, source, old)
    original = store._path(SYMBOL).read_bytes()
    metadata = store.metadata(SYMBOL)
    state["fault"] = fault
    if fault in {"network", "cancel", "factor"}:
        with pytest.raises(InterruptedError if fault == "cancel" else RuntimeError):
            registry.refresh_history(SYMBOL, START, END, store=store, mode="auto",
                                     cancelled=lambda: state["cancelled"])
    else:
        with pytest.raises(HistoryRepairError):
            registry.refresh_history(SYMBOL, START, END, store=store, mode="auto")
    assert store._path(SYMBOL).read_bytes() == original
    assert store.metadata(SYMBOL) == metadata


@pytest.mark.parametrize("end,now,last", [
    ("2024-01-21", "2024-01-21T20:00:00+08:00", END),
    (END, "2024-01-19T10:00:00+08:00", "2024-01-18"),
])
def test_auto_keeps_weekend_and_unclosed_session_cache(repair_setup, monkeypatch, end, now, last):
    store, source, calls, _, _ = repair_setup
    frame = source.daily(SYMBOL, START, END).loc[:last]
    quality = registry._assess_daily_frame(frame, START, last, symbol=SYMBOL, source="tushare")
    store.put(SYMBOL, frame, replace=True, replace_coverage=True, source="tushare",
              request_start=START, request_end=end, quality=quality.to_dict())
    monkeypatch.setattr(registry, "market_now", lambda: pd.Timestamp(now).to_pydatetime())
    monkeypatch.setattr(registry, "market_date", lambda: pd.Timestamp(now).date())
    calls.clear()
    before = store.metadata(SYMBOL)
    registry.refresh_history(SYMBOL, START, end, store=store, mode="auto")
    assert not calls
    assert store.metadata(SYMBOL) == before


def test_auto_respects_disabled_tushare(repair_setup, monkeypatch):
    store, _, calls, state, old = repair_setup
    store.put(SYMBOL, old, replace=True, replace_coverage=True, source="tushare")
    store.mark_status(SYMBOL, "stale")
    # Undo the fixture's factory stub: exercise the real configured switches.
    monkeypatch.setattr(registry, "_request_factories", state["request_factories"])
    registry.get_config().data.tushare_enabled = False
    monkeypatch.setattr("quantmaster.data.free_stockdb_source.FreeStockDBSource.daily", lambda *a: old)
    assert all(factory.name != "tushare" for factory in registry._request_factories(
        priority="maintenance", allow_online=False,
    )[registry.Market.CN])
    original, meta = store._path(SYMBOL).read_bytes(), store.metadata(SYMBOL)
    registry.refresh_history(SYMBOL, START, END, store=store, mode="auto")
    assert not calls
    assert store._path(SYMBOL).read_bytes() == original
    assert store.metadata(SYMBOL) == meta


def test_auto_missing_tail_reports_rejection_without_rewriting_cache(repair_setup, monkeypatch):
    from quantmaster.data.maintenance import DataRefreshManager

    store, source, _, _, old = repair_setup
    old.attrs = {}
    quality = registry._assess_daily_frame(old, START, OLD_END, symbol=SYMBOL, source="tushare")
    store.put(SYMBOL, old, replace=True, replace_coverage=True, source="tushare",
              request_start=START, request_end=END, quality=quality.to_dict())
    store.mark_status(SYMBOL, "stale")
    daily = source.daily
    monkeypatch.setattr(source, "daily", lambda *args: daily(*args).loc[:OLD_END])
    before, original = store.metadata(SYMBOL), store._path(SYMBOL).read_bytes()
    outcome = DataRefreshManager._refresh_one(store, SYMBOL, START, END)
    assert outcome["code"] == "history_repair_rejected"
    assert "响应质量校验失败" in outcome["error"]
    assert store.metadata(SYMBOL) == before
    assert store._path(SYMBOL).read_bytes() == original


def _damage_repair_candidate(frame, days, fault):
    if fault == 'old_date':
        frame = frame.drop(days[35])
    elif fault == 'head':
        frame = frame.drop(days[2])
    elif fault in {'tail', 'calendar_sparse', 'ingest_sparse'}:
        frame = frame.drop(days[70])
    elif fault == 'numeric':
        frame.loc[days[70], 'close'] = -1
    elif fault == 'amount':
        frame.loc[days[70], 'amount'] = -1
    elif fault == 'identity':
        frame.attrs['instrument'] = '600000.SH'
    elif fault == 'factor':
        frame.attrs['factor_coverage'] = 'unconfirmed'
    return frame

@pytest.mark.parametrize('fault', [
    '', 'old_date', 'head', 'tail', 'numeric', 'amount', 'identity', 'factor', 'cancel',
    'calendar', 'calendar_sparse', 'ingest_sparse', 'weekend',
])
def test_auto_existing_gap_only_allows_safe_extensions(repair_setup, monkeypatch, fault):
    from quantmaster.data.maintenance import DataRefreshManager

    store, source, _, _, _ = repair_setup
    days = pd.bdate_range('2024-01-02', periods=79)
    frame = source.daily(SYMBOL, START, END).reindex(days).ffill()
    frame.attrs['adjustment_anchor_date'] = str(days[-1].date())
    frame = frame.drop(days[40])
    old = frame.loc[days[25]:days[65]].copy()
    old.attrs['adjustment_anchor_date'] = str(days[65].date())
    end = str((days[-1] + pd.Timedelta(days=2 if fault == 'weekend' else 0)).date())
    quality = registry._assess_daily_frame(old, str(days[25].date()), str(days[65].date()),
                                         symbol=SYMBOL, source='tushare')
    store.put(SYMBOL, old, replace=True, replace_coverage=True, source='tushare',
              request_start=START, request_end=end, quality=quality.to_dict())
    store.mark_status(SYMBOL, 'stale')
    frame = _damage_repair_candidate(frame, days, fault)
    monkeypatch.setattr(source, 'cached_daily', lambda *args: frame.copy())
    monkeypatch.setattr(source, 'daily', lambda *args: pytest.fail('unexpected provider fetch'))
    def unavailable_calendar(*args):
        if fault == 'calendar':
            raise HistoryRepairError(SYMBOL, '独立日历证据不足')
        return days
    monkeypatch.setattr(source, 'trade_calendar', unavailable_calendar)
    monkeypatch.setattr(registry, 'market_date', lambda: days[-1].date())
    monkeypatch.setattr(registry, '_local_sessions', lambda left, right: (
        pd.DatetimeIndex([]) if fault == 'calendar' and left > days[65]
        else days[(days >= left) & (days <= right)], 'test-calendar'))
    if fault in {'calendar_sparse', 'ingest_sparse'}:
        monkeypatch.setattr(registry, '_local_sessions', lambda left, right: (
            days[(days >= left) & (days <= right) & (days != days[70])],
            'research_lake' if fault == 'calendar_sparse' else 'stockdb-ingest:tushare:trade_cal'))
    monkeypatch.setattr(
        'quantmaster.data.instrument_snapshots.load_or_fetch_suspension_snapshot',
        lambda *args: {'full_day_symbols': []},
    )
    original, metadata = store._path(SYMBOL).read_bytes(), store.metadata(SYMBOL)
    if fault == 'head':
        with pytest.raises(HistoryRepairError):
            registry.refresh_history(SYMBOL, START, end, store=store, source_name='tushare',
                                     repair_incompatible=True)
        assert store._path(SYMBOL).read_bytes() == original
        assert store.metadata(SYMBOL) == metadata
    if fault not in {'', 'head', 'weekend'}:
        with pytest.raises(InterruptedError if fault == 'cancel' else HistoryRepairError):
            registry.refresh_history(SYMBOL, START, end, store=store, mode='auto',
                                     cancelled=lambda: fault == 'cancel')
        assert store._path(SYMBOL).read_bytes() == original
        assert store.metadata(SYMBOL) == metadata
    else:
        result = registry.refresh_history(SYMBOL, START, end, store=store, mode='auto')
        assert old.index.difference(result.data.index).empty
        assert days[40] not in result.data.index
        assert result.quality.partial and result.quality.coverage_ratio < 1
        assert not result.quality.stale and not result.quality.formal_eligible
        assert store.get(SYMBOL).index.min() == old.index.min()
        monkeypatch.setattr(registry, '_request_factories', lambda **kw: pytest.fail('repeat source'))
        outcome = DataRefreshManager._refresh_one(store, SYMBOL, START, end)
        assert outcome['code'] == 'historical_start_incomplete'

@pytest.mark.parametrize('fault', ['', 'missing_day', 'duplicate', 'exchange', 'flag'])
def test_repair_calendar_proves_closed_days_and_rejects_partial_evidence(repair_setup, monkeypatch, fault):
    from quantmaster.data.resilience import ProviderContractChanged

    _, source, _, _, _ = repair_setup
    frame = pd.DataFrame({
        'exchange': ['SSE', 'SSE'], 'cal_date': ['20240106', '20240107'], 'is_open': [0, 0],
    })
    if fault == 'missing_day':
        frame = frame.iloc[:1]
    elif fault == 'duplicate':
        frame.loc[1, 'cal_date'] = '20240106'
    elif fault == 'exchange':
        frame.loc[1, 'exchange'] = 'SZSE'
    elif fault == 'flag':
        frame.loc[1, 'is_open'] = 2
    monkeypatch.setattr(source, '_call', lambda *args, **kw: frame)
    if fault:
        with pytest.raises(ProviderContractChanged):
            source.trade_calendar('2024-01-06', '2024-01-07')
    else:
        assert source.trade_calendar('2024-01-06', '2024-01-07').empty


@pytest.mark.parametrize("fault", ["", "stale", "calendar", "expected", "tail", "units", "stamp"])
def test_maintenance_closed_day_target_keeps_stockdb_evidence_gates(isolated_config, monkeypatch, fault):
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from quantmaster.data import maintenance
    from quantmaster.data.base import BarDataEnvelope, BarDataQuality

    accepted_at = datetime(2026, 9, 18, 10, tzinfo=UTC)
    acceptance = SimpleNamespace(session="2026-09-18", updated_at=accepted_at)
    monkeypatch.setattr(maintenance, "read_stockdb_session_acceptance", lambda _: acceptance)
    frame = pd.DataFrame({"close": [10.0]}, index=pd.to_datetime(["2026-09-18"]))
    frame.attrs.update(
        stockdb_accepted_session="2026-09-18",
        stockdb_accepted_at=accepted_at.isoformat() if fault != "stamp" else "old",
        unit_status="verified_local_stockdb_schema_v1",
    )
    quality = BarDataQuality(
        "degraded", "2026-09-01", "2026-09-19",
        observed_end="2026-09-17" if fault == "tail" else "2026-09-18",
        expected_session="" if fault == "expected" else "2026-09-18",
        freshness_state="stale" if fault == "stale" else "fresh",
        stale=fault == "stale",
        calendar_source="unavailable" if fault == "calendar" else "tushare:trade_cal",
        sources=("free-stockdb",), coverage_ratio=1.0,
        units=(("close", "unknown" if fault == "units" else "CNY/share"),),
        semantic_diagnostic_code="factor_contract_incomplete",
    )
    envelope = BarDataEnvelope(frame, quality, ())
    assert maintenance.DataRefreshManager._prepared_with_formal_gaps(
        envelope, SYMBOL, "2026-09-01", "2026-09-19",
    ) is (not fault)
    assert not quality.formal_eligible
