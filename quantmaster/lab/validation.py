"""统一、可审计的因子验证与晋级门槛。"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.factors.analysis import (
    annualize,
    forward_returns,
    information_coefficient,
    quantile_backtest,
    top_quantile_turnover,
)
from quantmaster.lab.horizons import SUPPORTED_HORIZONS
from quantmaster.lab.research import WalkForwardSpec, walk_forward_folds
from quantmaster.lab.robustness import (
    monte_carlo_block_bootstrap,
    parameter_sensitivity,
    penetration_analysis,
    robustness_summary,
    walk_forward_robustness,
)
from quantmaster.lab.strategy import (
    atomic_horizon_gate,
    execute_daily_targets,
    moving_block_return_interval,
)


def _finite(value: float, default: float = 0.0) -> float:
    return round(float(value), 6) if np.isfinite(value) else default


def _p_value(mean: float, std: float, count: int) -> float:
    if count < 2 or std <= 0:
        return 1.0
    z_score = abs(mean) / (std / math.sqrt(count))
    return min(1.0, math.erfc(z_score / math.sqrt(2.0)))


def benjamini_hochberg(p_values: list[float]) -> list[float]:
    """Benjamini-Hochberg FDR 校正，返回与输入顺序一致的 q-value。"""
    if not p_values:
        return []
    count = len(p_values)
    ordered = sorted(enumerate(p_values), key=lambda item: item[1])
    adjusted = [1.0] * count
    running = 1.0
    for rank, (index, value) in reversed(list(enumerate(ordered, start=1))):
        running = min(running, float(value) * count / rank)
        adjusted[index] = min(1.0, running)
    return adjusted


def _walk_forward_ic(
    values: pd.DataFrame,
    close: pd.DataFrame,
    horizon: int,
    protocol: WalkForwardSpec,
) -> tuple[pd.Series, list[dict], int, pd.Series, dict[str, Any]]:
    fwd = forward_returns(close, periods=horizon)
    raw_ic = information_coefficient(values, fwd)
    maximum_horizon = max(protocol.horizons)
    if len(close.index) <= maximum_horizon:
        raise ValueError("行情长度不足以生成成熟标签")
    maturity_cutoff = pd.Timestamp(close.index[-maximum_horizon - 1])
    raw_ic = raw_ic.loc[raw_ic.index <= maturity_cutoff]
    folds, sealed = walk_forward_folds(raw_ic.index, protocol)
    first = folds[0]
    discovery = raw_ic.loc[first.train_start:first.train_end]
    direction = 1 if float(discovery.mean()) >= 0 else -1
    oriented = raw_ic * direction
    reports, oos = [], []

    def fold_report(fold, number: int) -> tuple[pd.Series, dict[str, Any]]:
        train = oriented.loc[fold.train_start:fold.train_end]
        test = oriented.loc[fold.test_start:fold.test_end]
        train_mean = float(train.mean()) if len(train) else 0.0
        test_mean = float(test.mean()) if len(test) else 0.0
        retention = abs(test_mean) / abs(train_mean) if abs(train_mean) > 1e-12 else 0.0
        return test, {
            "fold": number,
            "name": fold.name,
            "sealed": bool(fold.sealed),
            "train_start": fold.train_start,
            "train_end": fold.train_end,
            "test_start": fold.test_start,
            "test_end": fold.test_end,
            "train_days": len(train),
            "test_days": len(test),
            "train_rank_ic": _finite(train_mean),
            "rank_ic": _finite(test_mean),
            "icir": _finite(test.mean() / test.std()) if test.std() > 0 else 0.0,
            "retention": _finite(retention),
        }

    for number, fold in enumerate(folds, start=1):
        test, report = fold_report(fold, number)
        oos.append(test)
        reports.append(report)
    _sealed_values, sealed_report = fold_report(sealed, len(folds) + 1)
    combined_oos = pd.concat(oos).groupby(level=0, sort=True).mean()
    return combined_oos, reports, direction, discovery * direction, sealed_report


def _correlation_with_existing(
    values: pd.DataFrame,
    approved_values: dict[str, pd.DataFrame] | None,
) -> tuple[float, str]:
    if not approved_values:
        return 0.0, ""
    target = values.rank(axis=1)
    best_value, best_name = 0.0, ""
    for name, other in approved_values.items():
        left, right = target.align(other.rank(axis=1), join="inner")
        value = float(left.corrwith(right, axis=1).mean())
        if np.isfinite(value) and abs(value) > abs(best_value):
            best_value, best_name = value, name
    return _finite(best_value), best_name


def validate_factor_values(
    factor_values: pd.DataFrame,
    close: pd.DataFrame,
    *,
    name: str = "factor",
    horizons: tuple[int, ...] = SUPPORTED_HORIZONS,
    protocol: WalkForwardSpec | None = None,
    membership: pd.DataFrame | None = None,
    approved_values: dict[str, pd.DataFrame] | None = None,
    research_quality: str = "production",
    panel: dict[str, pd.DataFrame] | None = None,
    parameter_variants: dict[str, pd.DataFrame] | None = None,
    robustness_seed: int = 42,
    open_prices: pd.DataFrame | None = None,
    essential_only: bool = False,
) -> dict[str, Any]:
    """对表达式、遗传、LLM 和学习型因子使用同一套验证口径。"""
    protocol = protocol or WalkForwardSpec.from_lab_config(
        get_config().lab, horizons=horizons,
    )
    if tuple(protocol.horizons) != tuple(horizons):
        raise ValueError("滚动协议 horizons 必须与本次验证周期完全一致")
    values, prices = factor_values.align(close, join="inner")
    if membership is not None:
        member_mask = membership.reindex(index=values.index, columns=values.columns).fillna(False)
        values = values.where(member_mask)
        prices = prices.where(member_mask)
    denominator = max(1, int(prices.notna().sum().sum()))
    coverage = float((values.notna() & prices.notna()).sum().sum()) / denominator
    if values.empty or prices.empty:
        raise ValueError("因子值或收盘价为空")

    max_corr, max_corr_name = _correlation_with_existing(values, approved_values)
    trade = get_config().trade
    one_way_cost = trade.commission_rate + trade.transfer_fee_rate + trade.slippage
    sell_extra = trade.stamp_tax_rate
    horizon_reports: dict[str, dict] = {}
    essential_execution_cache: dict[int, dict[str, Any]] = {}
    for horizon in horizons:
        oos, fold_reports, direction, discovery, sealed_report = _walk_forward_ic(
            values, prices, horizon, protocol,
        )
        oriented = values * direction
        daily, _nav = quantile_backtest(oriented, prices, quantiles=5, periods=horizon)
        long_short = daily["Q5"] - daily["Q1"]
        turnover = top_quantile_turnover(oriented, quantiles=5) / max(1, horizon)
        estimated_cost = turnover * (2 * one_way_cost + sell_extra)
        net_long_short = long_short - estimated_cost
        gross_edge = max(0.0, float(long_short.mean()))
        edge_cost_ratio = gross_edge / estimated_cost if estimated_cost > 0 else 999.0
        mean, std = float(oos.mean()), float(oos.std())
        is_mean = float(discovery.mean())
        retention = abs(mean) / abs(is_mean) if abs(is_mean) > 1e-12 else 0.0
        p_value = _p_value(mean, std, len(oos))
        fold_same_sign = sum(1 for item in fold_reports if item["rank_ic"] > 0) / len(fold_reports)
        quantile_annual = {column: annualize(daily[column]) for column in daily}
        annual_values = np.array(list(quantile_annual.values()), dtype=float)
        monotonicity = (
            float(np.corrcoef(np.arange(1, len(annual_values) + 1), annual_values)[0, 1])
            if np.std(annual_values) > 0 else 0.0
        )
        wfa = walk_forward_robustness(fold_reports)
        if essential_only:
            robustness = {
                "passed": bool(wfa.get("passed", True)),
                "failed_tests": [] if wfa.get("passed", True) else ["walk_forward"],
                "essential_only": True,
                "walk_forward": wfa,
            }
        else:
            monte_carlo = monte_carlo_block_bootstrap(
                oos,
                net_long_short,
                horizon=horizon,
                seed=robustness_seed,
            )
            parameter_report = parameter_sensitivity(
                values,
                parameter_variants,
                prices,
                horizon=horizon,
                direction=direction,
                oos_index=oos.index,
            )
            penetration = penetration_analysis(
                oriented,
                prices,
                oos,
                horizon=horizon,
                panel=panel,
            )
            robustness = robustness_summary(
                monte_carlo=monte_carlo,
                parameter_report=parameter_report,
                walk_forward=wfa,
                penetration=penetration,
            )
        execution: dict[str, Any] = {}
        execution_bootstrap: dict[str, Any] = {}
        if open_prices is not None:
            execution_result = essential_execution_cache.get(direction)
            if execution_result is None or not essential_only:
                execution_index = oos.index.intersection(oriented.index)
                execution_panel = {
                    key: frame.reindex(index=execution_index, columns=oriented.columns)
                    for key, frame in (panel or {}).items()
                    if isinstance(frame, pd.DataFrame)
                }
                execution_panel["open"] = open_prices.reindex(
                    index=execution_index, columns=oriented.columns,
                )
                execution_result = execute_daily_targets(
                    oriented.reindex(execution_index), execution_panel,
                    horizon=horizon, top_n=12, cap_weight=0.10,
                )
                if essential_only:
                    essential_execution_cache[direction] = execution_result
            execution = execution_result["metrics"]
            if not essential_only:
                execution_bootstrap = moving_block_return_interval(
                    execution_result["daily_excess"], block_days=max(20, 2 * horizon),
                    seed=robustness_seed + horizon,
                )
        horizon_reports[str(horizon)] = {
            "horizon": horizon,
            "direction": direction,
            "oos_days": len(oos),
            "oos_rank_ic": _finite(mean),
            "oos_ic_std": _finite(std),
            "oos_icir": _finite(mean / std) if std > 0 else 0.0,
            "is_rank_ic": _finite(is_mean),
            "retention": _finite(retention),
            "positive_ratio": _finite((oos > 0).mean()),
            "fold_same_sign": _finite(fold_same_sign),
            "p_value": _finite(p_value, 1.0),
            "q_value": _finite(p_value, 1.0),
            "turnover_daily": _finite(turnover),
            "edge_cost_ratio": _finite(edge_cost_ratio),
            "long_short_annual": _finite(annualize(long_short)),
            "top_annual": _finite(quantile_annual.get("Q5", 0.0)),
            "monotonicity": _finite(monotonicity),
            "folds": fold_reports,
            "sealed": sealed_report,
            "robustness": robustness,
            "execution": execution,
            "bootstrap": execution_bootstrap,
        }

    q_values = benjamini_hochberg([item["p_value"] for item in horizon_reports.values()])
    for item, q_value in zip(horizon_reports.values(), q_values, strict=True):
        item["q_value"] = _finite(q_value, 1.0)
        item["gates"] = atomic_horizon_gate(
            item, coverage=coverage, research_quality=research_quality,
        )

    def score(item: dict) -> float:
        novelty = max(0.0, 1.0 - abs(max_corr))
        return round(100 * (
            0.25 * min(1.0, abs(item["oos_rank_ic"]) / 0.05)
            + 0.15 * min(1.0, abs(item["oos_icir"]) / 0.5)
            + 0.15 * item["fold_same_sign"]
            + 0.15 * min(1.0, max(0.0, item["long_short_annual"]) / 0.20)
            + 0.10 * max(0.0, item["monotonicity"])
            + 0.10 * min(1.0, coverage / 0.9)
            + 0.10 * novelty
        ), 2)

    for item in horizon_reports.values():
        item["candidate_score"] = score(item)
    best = max(horizon_reports.values(), key=lambda item: item["candidate_score"])

    eligible_horizons = [
        int(item["horizon"]) for item in horizon_reports.values()
        if item["gates"]["passed"]
    ]
    common_hard = list(best["gates"]["hard_failures"])
    common_soft = [] if eligible_horizons else ["没有任何具体周期通过原子因子门槛"]
    best_robustness = best["robustness"]

    return {
        "factor": name,
        "protocol": protocol.to_dict(),
        "coverage": _finite(coverage),
        "max_existing_correlation": max_corr,
        "max_existing_factor": max_corr_name,
        "best_horizon": best["horizon"],
        "candidate_score": best["candidate_score"],
        "eligible_horizons": eligible_horizons,
        "horizons": horizon_reports,
        "robustness": best_robustness,
        "gates": {
            "passed": bool(eligible_horizons) and not common_hard,
            "hard_failures": common_hard,
            "soft_failures": common_soft,
            "override_allowed": not common_hard,
        },
    }
