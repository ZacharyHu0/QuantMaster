"""Generation/DAG regression checks for normal local refreshes."""

from __future__ import annotations

import numpy as np
import pandas as pd

from quantmaster.rotation.contracts import RotationJobSpec
from quantmaster.rotation.service import ALGORITHM_VERSION, RotationService
from quantmaster.rotation.store import RotationStore
from quantmaster.runtime.jobs import UnifiedJobStore


def _local_state() -> dict:
    return {
        "generations": [
            {
                "source": "bar_store.stock_bars",
                "partition_key": "600000.SH",
                "generation": 1,
                "content_id": "bars-v1",
                "coverage_start": "2026-08-01",
                "coverage_end": "2026-08-10",
            },
            {
                "source": "instrument_catalog",
                "partition_key": "cn-listed-stock",
                "generation": 1,
                "content_id": "instruments-v1",
            },
        ],
        "as_of": "2026-08-10",
        "source": "bar_store",
        "expected_count": 1,
        "available": True,
    }


def _snapshot(kind: str, fingerprint: str) -> dict:
    return {
        "meta": {
            "snapshot_id": f"{kind}-snapshot",
            "schema_version": 2,
            "algorithm_version": ALGORITHM_VERSION,
            "input_fingerprint": fingerprint,
            "as_of": "2026-08-10",
            "generated_at": "2026-08-10T10:00:00+00:00",
            "quality": {"status": "complete", "issues": []},
        },
        "data": {"as_of": "2026-08-10", "items": []},
    }


def test_node_generations_invalidate_only_their_dependency_cut(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    service = RotationService(store, UnifiedJobStore(tmp_path / "jobs.sqlite"))
    state = _local_state()
    service._local_input_state = lambda: state  # type: ignore[method-assign]
    store.replace_taxonomy_nodes([
        {"code": "801080.SI", "name": "电子", "level": "L1", "members": ["600000.SH"]},
    ])
    store.replace_themes([
        {"code": "BK001", "name": "题材", "members": ["600000.SH"], "as_of": "2026-08-10"},
    ])
    store.save_etf_observations(pd.DataFrame([
        {"trade_date": "2026-08-10", "symbol": "510300.SH", "shares": 100.0},
    ]))

    spec = RotationJobSpec(scope="all", source="local")
    before = service.snapshot_input_fingerprints(spec)
    store.replace_themes([
        {"code": "BK001", "name": "题材", "members": ["600000.SH", "600001.SH"], "as_of": "2026-08-10"},
    ])
    after_theme = service.snapshot_input_fingerprints(spec)
    assert after_theme["themes"] != before["themes"]
    assert after_theme["industries"] == before["industries"]
    assert after_theme["temperature"] == before["temperature"]

    store.save_etf_observations(pd.DataFrame([
        {"trade_date": "2026-08-10", "symbol": "510300.SH", "shares": 101.0},
    ]))
    after_etf = service.snapshot_input_fingerprints(spec)
    assert after_etf["temperature"] != after_theme["temperature"]
    assert after_etf["etf_flows"] != after_theme["etf_flows"]
    assert after_etf["themes"] == after_theme["themes"]
    assert after_etf["industries"] == after_theme["industries"]


def test_matching_published_nodes_short_circuit_before_matrix_read(tmp_path, monkeypatch):
    store = RotationStore(tmp_path / "rotation")
    service = RotationService(store, UnifiedJobStore(tmp_path / "jobs.sqlite"))
    state = _local_state()
    service._local_input_state = lambda: state  # type: ignore[method-assign]
    monkeypatch.setattr(
        "quantmaster.rotation.service._expected_market_session", lambda: "2026-08-10",
    )
    spec = RotationJobSpec(scope="close", source="local")
    fingerprints = service.snapshot_input_fingerprints(spec)
    store.save_snapshots({kind: _snapshot(kind, fingerprint) for kind, fingerprint in fingerprints.items()})

    def should_not_read(**_kwargs):
        raise AssertionError("no-op refresh must not load the market matrix")

    service.loader.market_matrices = should_not_read
    result = service.build(spec, progress=lambda *_: None, cancelled=lambda: False)

    assert result["outcome"] == "unchanged"
    assert result["computed"] == []


def test_etf_generation_only_recomputes_temperature_and_etf_nodes(tmp_path, monkeypatch):
    """An ETF revision must not rerun industry/theme aggregation loops."""

    store = RotationStore(tmp_path / "rotation")
    service = RotationService(store, UnifiedJobStore(tmp_path / "jobs.sqlite"))
    state = _local_state()
    service._local_input_state = lambda: state  # type: ignore[method-assign]
    dates = pd.bdate_range("2026-03-02", periods=100)
    symbols = [f"{600000 + index:06d}.SH" for index in range(40)]
    returns = np.random.default_rng(11).normal(0.0004, 0.012, (len(dates), len(symbols)))
    values = 20 * np.exp(np.cumsum(returns, axis=0))
    close = pd.DataFrame(values, index=dates, columns=symbols)
    amount = close * 800_000
    names = {symbol: f"股票{index}" for index, symbol in enumerate(symbols)}
    market_reads: list[str] = []

    def load_values(*, progress, cancelled):
        assert not cancelled()
        market_reads.append("read")
        return close, amount, names, len(symbols), ["test:local"]

    service.loader.market_matrices = load_values
    monkeypatch.setattr(
        "quantmaster.rotation.service._expected_market_session",
        lambda: str(dates[-1].date()),
    )
    industry_names = ("电子", "计算机", "机械设备", "医药生物")
    monkeypatch.setattr(
        "quantmaster.rotation.service.load_cached_industry_map",
        lambda: {
            symbol: industry_names[index // 10]
            for index, symbol in enumerate(symbols)
        },
    )
    store.replace_themes([{
        "code": "BK1001", "name": "测试题材", "members": symbols[:16],
        "as_of": str(dates[-1].date()),
    }])
    store.save_etf_observations(pd.DataFrame([
        {"trade_date": dates[-2], "symbol": "510300.SH", "shares": 100, "nav": 4.0},
        {"trade_date": dates[-1], "symbol": "510300.SH", "shares": 105, "nav": 4.1},
    ]))
    spec = RotationJobSpec(scope="all", source="local")
    service.build(spec, progress=lambda *_: None, cancelled=lambda: False)
    before_industries = service.snapshot_header("industries")["meta"]["snapshot_id"]
    before_themes = service.snapshot_header("themes")["meta"]["snapshot_id"]

    store.save_etf_observations(pd.DataFrame([
        {"trade_date": dates[-2], "symbol": "510300.SH", "shares": 100, "nav": 4.0},
        {"trade_date": dates[-1], "symbol": "510300.SH", "shares": 110, "nav": 4.1},
    ]))
    from quantmaster.rotation import service as service_module

    real_group_analysis = service_module.analyze_group_rotation
    group_calls: list[str] = []

    def no_group_rebuild(*args, **kwargs):
        group_calls.append("called")
        return real_group_analysis(*args, **kwargs)

    monkeypatch.setattr(service_module, "analyze_group_rotation", no_group_rebuild)
    result = service.build(spec, progress=lambda *_: None, cancelled=lambda: False)

    assert set(result["computed"]) == {"temperature", "etf_flows"}
    assert market_reads == ["read", "read"]  # temperature still needs the shared tail matrix
    assert group_calls == []
    assert service.snapshot_header("industries")["meta"]["snapshot_id"] == before_industries
    assert service.snapshot_header("themes")["meta"]["snapshot_id"] == before_themes
