"""牛熊/趋势状态与 1-7 日选股决策。"""

import numpy as np
import pandas as pd

from quantmaster.backtest.strategy import SwingStrategy
from quantmaster.decision import (
    HybridDecisionStrategy,
    adaptive_rule_score_panel,
    daily_selection,
    hybrid_daily_selection,
    market_exposure,
    resolve_policy,
    swing_score_panel,
)
from quantmaster.decision.storage import DecisionStore
from quantmaster.market import analyze_bars, analyze_market, analyze_sectors


def test_uptrend_regime_has_macd_and_probabilistic_future():
    dates = pd.bdate_range("2024-01-02", periods=120)
    close = pd.Series(
        100 * np.exp(np.linspace(0, 0.35, len(dates)) + 0.01 * np.sin(np.arange(len(dates)))),
        index=dates,
    )
    bars = pd.DataFrame({
        "close": close, "volume": 1e6 * (1 + 0.1 * np.sin(np.arange(len(dates)) / 3)),
        "amount": close * 1e6,
    })
    report = analyze_bars(bars)
    assert report["current"]["state"] in {"up", "strong_up"}
    assert report["current"]["macd"] is not None
    assert [row["horizon_days"] for row in report["future"]] == [1, 3, 5, 7]
    assert all(0 <= row["probability_up"] <= 1 for row in report["future"])
    assert all(row["samples"] > 80 for row in report["forecast_validation"])
    assert all(0 <= row["direction_accuracy"] <= 1 for row in report["forecast_validation"])
    assert all(0 <= row["brier_score"] <= 1 for row in report["forecast_validation"])
    assert "确定" in report["forecast_note"]


def test_market_and_sector_views(panel):
    report = analyze_market(panel)
    assert 0 <= report["current"]["bull_score"] <= 100
    assert 0 <= report["current"]["advance_ratio"] <= 1
    assert 0 <= report["current"]["rsi_14"] <= 100
    symbols = list(panel["close"].columns)
    mapping = {symbol: "行业A" if i < 4 else "行业B" for i, symbol in enumerate(symbols)}
    sectors = analyze_sectors(panel, mapping)
    assert set(sectors["sector"]) == {"行业A", "行业B"}
    assert sectors["bull_score"].between(0, 100).all()
    assert sectors["rsi_14"].between(0, 100).all()


def test_swing_scores_have_no_forward_dependency(panel):
    cutoff = panel["close"].index[90]
    original = swing_score_panel(panel).loc[cutoff]
    changed = {name: values.copy() for name, values in panel.items()}
    for values in changed.values():
        values.loc[values.index > cutoff] *= 50
    mutated = swing_score_panel(changed).loc[cutoff]
    pd.testing.assert_series_equal(original, mutated)


def test_hybrid_adaptive_scores_have_no_forward_dependency(panel):
    cutoff = panel["close"].index[110]
    original = adaptive_rule_score_panel(panel, horizon=5).loc[cutoff]
    changed = {name: values.copy() for name, values in panel.items()}
    for values in changed.values():
        values.loc[values.index > cutoff] *= 25
    mutated = adaptive_rule_score_panel(changed, horizon=5).loc[cutoff]
    pd.testing.assert_series_equal(original, mutated)


def test_hybrid_profiles_snapshot_and_storage_are_reproducible(panel, tmp_path):
    from quantmaster.lab.store import LabStore

    lab = LabStore(tmp_path / "lab.sqlite")
    symbols = list(panel["close"].columns)
    industries = {symbol: f"行业{index % 3}" for index, symbol in enumerate(symbols)}
    risk_policy = resolve_policy(
        "demo", 3, "risk_adjusted", symbols=symbols, store=lab,
    )
    stable_policy = resolve_policy(
        "demo", 3, "stable", symbols=symbols, store=lab,
    )
    risk = hybrid_daily_selection(
        panel, top_n=4, horizon=3, profile="risk_adjusted", universe="demo",
        industry_map=industries, policy_snapshot=risk_policy,
    )
    stable = hybrid_daily_selection(
        panel, top_n=4, horizon=3, profile="stable", universe="demo",
        industry_map=industries, policy_snapshot=stable_policy,
    )
    assert risk["model_version"].startswith("hybrid-v2:risk_adjusted:")
    assert stable["model_version"].startswith("hybrid-v2:stable:")
    assert stable["recommended_exposure"] <= risk["recommended_exposure"]
    assert all(0 <= item["probability_up"] <= 1 for item in risk["picks"])
    assert all("component_scores" in item for item in risk["picks"])

    store = DecisionStore(tmp_path / "hybrid.sqlite")
    store.save(risk, "demo")
    store.save(stable, "demo")
    assert len(store.history("demo")) == 2
    assert len(store.history("demo", profile="stable")) == 1


def test_hybrid_strategy_uses_profile_risk_limits(panel, tmp_path):
    from quantmaster.lab.store import LabStore

    policy = resolve_policy(
        "demo", 5, "stable", symbols=list(panel["close"].columns),
        store=LabStore(tmp_path / "lab.sqlite"),
    )
    weights = HybridDecisionStrategy(
        top_n=4, holding_days=5, profile="stable", universe="demo",
        policy_snapshot=policy,
    ).target_weights(panel)
    signals = weights.dropna(how="all")
    assert len(signals) > 10
    assert (signals.sum(axis=1) <= 0.65 + 1e-9).all()


def test_profile_constraints_survive_missing_factor_component():
    class OnlyMlStore:
        @staticmethod
        def active_deployments():
            return [{
                "id": "deployment", "version_id": "learned", "universe": "demo",
                "horizon": 3, "profile": "all", "scope": "exact", "role": "ml",
                "created_at": "2026-07-27T00:00:00+00:00",
            }]

        @staticmethod
        def version(version_id):
            return {
                "id": version_id, "name": "ML Champion", "status": "approved",
                "content_hash": "hash", "validation": {},
                "spec": {"kind": "learned", "model": {"manifest": "unused.json"}},
            }

    risk = resolve_policy("demo", 3, "risk_adjusted", store=OnlyMlStore())
    short = resolve_policy("demo", 3, "short_term", store=OnlyMlStore())
    assert risk["components"][1]["weight"] <= 0.30
    assert short["components"][1]["weight"] <= 0.45
    assert risk["components"][0]["weight"] >= 0.35
    assert short["components"][0]["weight"] >= 0.25


def test_daily_selection_and_swing_strategy(panel):
    symbols = list(panel["close"].columns)
    names = {symbol: f"股票{i}" for i, symbol in enumerate(symbols)}
    report = daily_selection(panel, top_n=4, horizon=5, name_map=names)
    assert report["holding_horizon_days"] == 5
    assert len(report["picks"]) == 4
    assert all(pick["name"] == names[pick["symbol"]] for pick in report["picks"])
    assert all(0 <= pick["score"] <= 100 for pick in report["picks"])
    assert all(0.03 <= pick["stop_loss"] <= 0.10 for pick in report["picks"])

    weights = SwingStrategy(top_n=4, holding_days=5).target_weights(panel)
    signals = weights.dropna(how="all")
    assert len(signals) > 10
    assert (signals.sum(axis=1) <= 1.0 + 1e-9).all()
    positions = np.flatnonzero(weights.notna().any(axis=1).to_numpy())
    assert (np.diff(positions) == 5).all()


def test_bear_market_reduces_exposure(panel):
    bearish = {name: values.copy() for name, values in panel.items()}
    decay = np.exp(-np.linspace(0, 1.0, len(panel["close"])))
    for field in ("open", "high", "low", "close"):
        bearish[field] = bearish[field].mul(decay, axis=0)
    assert market_exposure(bearish).iloc[-1] == 0.30


def test_decision_snapshots_are_reproducible(panel, tmp_path):
    store = DecisionStore(tmp_path / "decisions.sqlite")
    report = daily_selection(panel, top_n=3, horizon=3)
    store.save(report, "demo")
    store.save(report, "demo")
    assert store.latest("demo") == report
    assert len(store.history("demo")) == 1
    assert len(store.history("demo", horizon=3)) == 1
    assert store.history("demo", horizon=1) == []
    assert store.history("other") == []


def test_swing_strategy_runs_through_a_share_backtest(panel):
    from quantmaster.backtest import BacktestConfig, run_backtest

    strategy = SwingStrategy(top_n=4, holding_days=3)
    result = run_backtest(
        panel, strategy.target_weights(panel),
        BacktestConfig(initial_capital=100_000, allow_fractional=True))
    assert result.trades
    assert result.nav.notna().all()
    assert result.metrics["total_return"] == round(float(result.nav.iloc[-1] - 1), 4)
