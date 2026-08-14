from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from quantmaster.backtest.spec import BacktestSpec, LabVersionStrategySpec
from quantmaster.cli import build_parser
from quantmaster.config import Config, set_config
from quantmaster.data.research import PitDataStore, ResearchDataBundle, load_research_bundle
from quantmaster.data.tushare_source import TushareSource
from quantmaster.lab.multihorizon import (
    apply_probability_calibrators,
    fit_multi_fold,
    fit_probability_calibrators,
    fold_positions,
    make_multi_horizon_samples,
    predictions_to_frame,
)
from quantmaster.lab.optimization import OptimizationRunner, evaluate_predictions
from quantmaster.lab.research import (
    FeatureSetSpec,
    OptimizationSpec,
    TimeFold,
    WalkForwardSpec,
    benjamini_hochberg_family,
    walk_forward_folds,
)
from quantmaster.lab.store import LabStore


@pytest.fixture(autouse=True)
def reset_config():
    yield
    set_config(None)


def _panel(days: int = 300, symbols: int = 8) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(2026)
    index = pd.bdate_range("2022-01-03", periods=days)
    columns = [f"S{number}" for number in range(symbols)]
    close = pd.DataFrame(
        100 * np.exp(np.cumsum(rng.normal(0.0002, 0.012, (days, symbols)), axis=0)),
        index=index,
        columns=columns,
    )
    volume = pd.DataFrame(rng.lognormal(10, 0.5, close.shape), index=index, columns=columns)
    return {
        "open": close * (1 + rng.normal(0, 0.001, close.shape)),
        "high": close * 1.015,
        "low": close * 0.985,
        "close": close,
        "volume": volume,
        "amount": volume * close,
    }


def test_walk_forward_keeps_development_before_sealed_holdout():
    dates = pd.bdate_range("2015-01-05", periods=1400)
    protocol = WalkForwardSpec()
    folds, sealed = walk_forward_folds(dates, protocol)

    assert len(folds) == 4
    assert sealed.sealed is True
    assert all(pd.Timestamp(fold.train_end) < pd.Timestamp(fold.test_start) for fold in folds)
    assert pd.Timestamp(folds[-1].test_end) < pd.Timestamp(sealed.test_start)
    assert len(pd.bdate_range(sealed.test_start, sealed.test_end)) >= 252


def test_family_fdr_is_monotone_in_original_hypothesis_family():
    adjusted = benjamini_hochberg_family([0.04, 0.001, 0.02, 0.4])
    assert adjusted[1] <= adjusted[2] <= adjusted[0] <= adjusted[3]
    assert all(0 <= value <= 1 for value in adjusted)


def test_shared_ridge_emits_all_horizons_and_oof_calibration(tmp_path):
    panel = _panel()
    samples = make_multi_horizon_samples(
        panel,
        sequence_length=20,
        feature_spec=FeatureSetSpec(
            groups=("price_volume_v2",), minimum_coverage=0.75,
        ),
    )
    dates = panel["close"].index
    fold = TimeFold(
        "unit-oof",
        train_start=dates[125].strftime("%Y-%m-%d"),
        train_end=dates[215].strftime("%Y-%m-%d"),
        test_start=dates[225].strftime("%Y-%m-%d"),
        test_end=dates[255].strftime("%Y-%m-%d"),
    )
    train, valid = fold_positions(samples, fold)
    fitted = fit_multi_fold(
        "ridge", samples, train, valid,
        artifact_path=tmp_path / "shared-ridge.npz",
        config={"alpha": 1.0}, roundtrip_cost=0.002,
    )
    frame = predictions_to_frame(samples, valid, fitted["_predictions"])

    assert set(frame["horizon"]) == {1, 3, 5, 7, 10, 20, 30}
    assert fitted["artifact_sha256"]
    calibrators = fit_probability_calibrators(frame, roundtrip_cost=0.002)
    calibrated = apply_probability_calibrators(frame, calibrators)
    assert set(calibrators) == {"1", "3", "5", "7", "10", "20", "30"}
    assert calibrated[["probability_up", "probability_net_positive"]].ge(0).all().all()
    assert calibrated[["probability_up", "probability_net_positive"]].le(1).all().all()
    metrics = evaluate_predictions(calibrated, top_n=3, roundtrip_cost=0.002)
    assert set(metrics["horizons"]) == {"1", "3", "5", "7", "10", "20", "30"}


def test_research_bundle_production_gate_rejects_legacy_approximations():
    bundle = ResearchDataBundle.from_legacy_panel(_panel(days=30, symbols=3))
    sandbox = bundle.validate("sandbox")
    assert sandbox["tier"] == "sandbox"
    assert bundle.backtest_panel()["execution_open"] is not None
    with pytest.raises(ValueError, match="生产研究数据门禁未通过"):
        bundle.validate("production")


def test_research_daily_separates_stable_signal_and_suspension_interval(monkeypatch):
    monkeypatch.setattr("quantmaster.data.tushare_source._instrument_type", lambda symbol: "stock")
    source = TushareSource()
    frames = {
        "daily": pd.DataFrame({
            "ts_code": ["600000.SH", "600000.SH"],
            "trade_date": ["20240104", "20240102"],
            "open": [5.0, 10.0], "high": [5.2, 10.2], "low": [4.8, 9.8],
            "close": [5.0, 10.0], "vol": [20.0, 10.0], "amount": [2.0, 1.0],
        }),
        "adj_factor": pd.DataFrame({
            "trade_date": ["20240104", "20240102"], "adj_factor": [2.0, 1.0],
        }),
        "stk_limit": pd.DataFrame({
            "trade_date": ["20240104", "20240102"],
            "up_limit": [5.5, 11.0], "down_limit": [4.5, 9.0],
        }),
        "suspend_d": pd.DataFrame({
            "trade_date": ["20240103"], "suspend_type": ["S"],
        }),
        "trade_cal": pd.DataFrame({
            "cal_date": ["20240102", "20240103", "20240104"],
            "is_open": [1, 1, 1],
        }),
    }
    source._call = lambda endpoint, ttl, **params: frames[endpoint].copy()  # type: ignore[method-assign]

    result = source.research_daily("600000.SH", "2024-01-02", "2024-01-04")

    assert result["raw"].loc["2024-01-04", "close"] == 5.0
    assert result["signal"].loc["2024-01-04", "close"] == 10.0
    assert bool(result["suspended"].loc["2024-01-03", "suspended"]) is True


def test_production_pit_store_reuses_complete_symbol_inputs(tmp_path):
    class Source:
        def __init__(self):
            self.calls = 0
            self.requested = []

        def trade_calendar(self, start, end):
            return pd.date_range(start, end, freq="D")

        def research_daily(self, symbol, start, end, *, calendar=None):
            self.calls += 1
            self.requested.append((start, end))
            assert calendar is not None
            raw = pd.DataFrame({
                "open": range(10, 10 + len(calendar)),
                "high": range(11, 11 + len(calendar)),
                "low": range(9, 9 + len(calendar)),
                "close": range(10, 10 + len(calendar)),
                "volume": 1000, "amount": 10000,
            }, index=calendar, dtype=float)
            signal = raw.copy()
            signal[["open", "high", "low", "close"]] *= 2
            return {
                "signal": signal,
                "raw": raw,
                "adj_factor": pd.DataFrame({"adj_factor": 2.0}, index=calendar),
                "limits": pd.DataFrame({"up_limit": raw["close"] * 1.1,
                                        "down_limit": raw["close"] * 0.9}),
                "suspended": pd.DataFrame({"suspended": False}, index=calendar),
            }

    store = PitDataStore(tmp_path / "pit")
    source = Source()
    dates = pd.date_range("2024-01-02", "2024-01-04", freq="D")
    membership = pd.DataFrame({"600000.SH": True}, index=dates)
    first = load_research_bundle(
        ["600000.SH"], "2024-01-02", "2024-01-04", membership=membership,
        source=source, store=store,
    )
    second = load_research_bundle(
        ["600000.SH"], "2024-01-02", "2024-01-04", membership=membership,
        source=source, store=store,
    )

    assert source.calls == 1
    assert first.manifest_hash == second.manifest_hash
    assert first.manifest["pit_cache"]["downloaded"] == 1
    assert second.manifest["pit_cache"] == {
        "dataset": "pit_execution/v1", "hits": 1, "downloaded": 0,
        "estimated_requests": 0,
    }
    pd.testing.assert_frame_equal(first.execution["raw_close"], second.execution["raw_close"])

    extended_dates = pd.date_range("2024-01-02", "2024-01-05", freq="D")
    extended_membership = pd.DataFrame({"600000.SH": True}, index=extended_dates)
    load_research_bundle(
        ["600000.SH"], "2024-01-02", "2024-01-05", membership=extended_membership,
        source=source, store=store,
    )
    assert source.calls == 2
    assert source.requested[-1] == ("2024-01-05", "2024-01-05")


def test_study_ledger_persists_protocol_and_resume_state(tmp_path):
    cfg = Config()
    cfg.data.root = str(tmp_path)
    set_config(cfg)
    store = LabStore(tmp_path / "lab.sqlite")
    spec = OptimizationSpec(
        end="2026-07-28", models=("ridge",), budget_hours=0.1, max_trials=2,
    )
    created = store.create_study(spec.to_dict())
    paused = store.update_study(
        created["id"], status="paused", result={"sealed_completed_blocks": 3},
    )

    assert paused["config_hash"] == spec.config_hash
    assert paused["config"]["protocol"]["sealed_holdout"] == 252
    assert paused["result"]["sealed_completed_blocks"] == 3


def test_study_rest_api_and_cli_expose_the_same_research_controls(tmp_path, monkeypatch):
    cfg = Config()
    cfg.data.root = str(tmp_path)
    cfg.lab.enabled = False
    set_config(cfg)
    from quantmaster.server.app import app

    with TestClient(app) as client:
        client.headers["X-CSRF-Token"] = client.get(
            "/api/v1/session",
        ).json()["csrf_token"]
        created = client.post("/api/v1/lab/studies", json={
            "universe": "csi800", "start": "2015-01-01", "end": "2026-07-28",
            "models": ["ridge"], "budget_hours": 0.5, "max_trials": 2,
        })
        assert created.status_code == 409
        assert created.json()["error"]["code"] == "DATASET_MISSING"
        from quantmaster.lab.service import get_lab_service

        monkeypatch.setattr(get_lab_service(), "preflight", lambda *_args, **_kwargs: {
            "runnable": True, "state": "ready", "resource_class": "cpu",
            "blockers": [], "warnings": [], "dataset": {},
        })
        created = client.post("/api/v1/lab/studies", json={
            "universe": "csi800", "start": "2015-01-01", "end": "2026-07-28",
            "models": ["ridge"], "budget_hours": 0.5, "max_trials": 2,
        })
        assert created.status_code == 202
        study = created.json()
        assert study["status"] == "queued"
        assert study["job_id"]
        detail = client.get(f"/api/v1/lab/studies/{study['id']}")
        assert detail.status_code == 200
        assert detail.json()["config"]["protocol"]["train_window"] == 756
        assert client.get("/api/v1/lab/studies").json()["items"][0]["id"] == study["id"]

    parsed = build_parser().parse_args([
        "lab", "optimize", "--models", "ridge", "--budget-hours", "0.5",
    ])
    assert parsed.lab_cmd == "optimize"
    assert parsed.models == "ridge"


def test_backtest_spec_accepts_fixed_lab_oof_version():
    spec = BacktestSpec(
        strategy=LabVersionStrategySpec(version_id="a" * 32, horizon=5),
        universe="csi800", start="2024-01-02", end="2025-01-02",
        research_tier="production",
    )
    assert spec.strategy.kind == "lab_version"
    assert spec.strategy.horizon == 5


def test_optuna_runner_persists_a_ridge_baseline_and_reuses_sealed_blocks(
    tmp_path, monkeypatch,
):
    pytest.importorskip("optuna")
    monkeypatch.setattr(
        "quantmaster.lab.optimization.feasibility",
        lambda metrics, spec: {"feasible": True, "failures": [], "sign_ratio": 1.0},
    )
    cfg = Config()
    cfg.data.root = str(tmp_path / "data")
    set_config(cfg)
    panel = _panel()
    protocol = WalkForwardSpec(
        train_window=120, retrain_every=10, sealed_holdout=20,
        purge_gap=30, development_folds=2, fold_test_days=10,
    )
    spec = OptimizationSpec(
        universe="demo", start=str(panel["close"].index.min().date()),
        end=str(panel["close"].index.max().date()), models=("ridge",),
        budget_hours=0.05, max_trials=1, top_n=3, sequence_length=20,
        research_tier="sandbox", protocol=protocol,
        features=FeatureSetSpec(groups=("price_volume_v2",), minimum_coverage=0.75),
    )

    runner = OptimizationRunner(tmp_path / "artifacts")
    result = runner.run("study-unit", spec, panel)

    assert result["status"] == "completed"
    assert result["candidate"] is True
    assert len(result["trials"]) == 1
    assert result["trials"][0]["params"]["model"] == "ridge"
    assert (tmp_path / "artifacts" / "study-unit" / "optuna.sqlite").is_file()
    assert (tmp_path / "artifacts" / "study-unit" / "sealed_predictions.parquet").is_file()

    def unexpected_fit(*args, **kwargs):
        raise AssertionError("恢复 Study 时不应重训已验证且哈希匹配的密封块")

    monkeypatch.setattr("quantmaster.lab.optimization.fit_multi_fold", unexpected_fit)
    resumed = runner.run("study-unit", spec, panel)
    assert resumed["status"] == "completed"
    assert resumed["prediction_sha256"] == result["prediction_sha256"]


def test_multi_horizon_sample_store_uses_shared_cube_and_compact_metadata(tmp_path):
    panel = _panel(days=180, symbols=4)
    root = tmp_path / "sample-store-v4"

    samples = make_multi_horizon_samples(
        panel,
        sequence_length=20,
        feature_spec=FeatureSetSpec(
            groups=("price_volume_v2",), minimum_coverage=0.75,
        ),
        storage_dir=root,
    )

    assert isinstance(samples.values.cube, np.memmap)
    assert isinstance(samples.metadata.date_positions, np.memmap)
    assert (root / "feature-cube.npy").is_file()
    assert not (root / "features.npy").exists()
    assert len(samples.metadata) == len(samples.values)
    assert samples.values[0].shape == (20, len(samples.feature_names))
    first = samples.metadata[0]
    assert first["symbol"] in panel["close"].columns
    assert set(first["target_dates"]) == {"1", "3", "5", "7", "10", "20", "30"}
