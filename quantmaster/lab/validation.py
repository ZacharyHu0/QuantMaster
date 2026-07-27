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
    folds: int,
) -> tuple[pd.Series, list[dict], int, pd.Series]:
    fwd = forward_returns(close, periods=horizon)
    raw_ic = information_coefficient(values, fwd)
    if len(raw_ic) < max(80, folds * 20):
        raise ValueError(f"{horizon}日 IC 样本不足：仅 {len(raw_ic)} 个交易日")
    min_train = max(60, len(raw_ic) // 3)
    discovery = raw_ic.iloc[: max(1, min_train - horizon)]
    direction = 1 if float(discovery.mean()) >= 0 else -1
    oriented = raw_ic * direction
    remaining = np.arange(min_train, len(oriented))
    chunks = [chunk for chunk in np.array_split(remaining, folds) if len(chunk)]
    reports, oos = [], []
    for number, positions in enumerate(chunks, start=1):
        test_start = int(positions[0])
        train = oriented.iloc[: max(0, test_start - horizon)]
        test = oriented.iloc[positions]
        oos.append(test)
        reports.append({
            "fold": number,
            "train_start": str(train.index[0].date()) if len(train) else None,
            "train_end": str(train.index[-1].date()) if len(train) else None,
            "test_start": str(test.index[0].date()),
            "test_end": str(test.index[-1].date()),
            "train_days": len(train),
            "test_days": len(test),
            "rank_ic": _finite(test.mean()),
            "icir": _finite(test.mean() / test.std()) if test.std() > 0 else 0.0,
        })
    return pd.concat(oos), reports, direction, discovery * direction


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
    horizons: tuple[int, ...] = (1, 3, 5, 7),
    folds: int = 4,
    membership: pd.DataFrame | None = None,
    approved_values: dict[str, pd.DataFrame] | None = None,
    research_quality: str = "production",
) -> dict[str, Any]:
    """对表达式、遗传、LLM 和学习型因子使用同一套验证口径。"""
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
    for horizon in horizons:
        oos, fold_reports, direction, discovery = _walk_forward_ic(
            values, prices, horizon, folds
        )
        oriented = values * direction
        daily, _nav = quantile_backtest(oriented, prices, quantiles=5, periods=horizon)
        long_short = daily["Q5"] - daily["Q1"]
        turnover = top_quantile_turnover(oriented, quantiles=5) / max(1, horizon)
        estimated_cost = turnover * (2 * one_way_cost + sell_extra)
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
        }

    q_values = benjamini_hochberg([item["p_value"] for item in horizon_reports.values()])
    for item, q_value in zip(horizon_reports.values(), q_values, strict=True):
        item["q_value"] = _finite(q_value, 1.0)

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

    hard_failures = []
    if research_quality != "production":
        hard_failures.append("候选不是 point-in-time 生产级快照")
    if coverage < 0.70:
        hard_failures.append(f"因子覆盖率 {coverage:.1%} 低于 70%")
    if best["oos_days"] < 252:
        hard_failures.append(f"样本外仅 {best['oos_days']} 个交易日，少于 252 日")
    soft_failures = []
    if abs(best["oos_rank_ic"]) < 0.02:
        soft_failures.append("样本外 |RankIC| 低于 0.02")
    if abs(best["oos_icir"]) < 0.20:
        soft_failures.append("样本外 |ICIR| 低于 0.20")
    if best["retention"] < 0.50:
        soft_failures.append("样本外 IC 保留率低于 50%")
    if best["fold_same_sign"] < 0.75:
        soft_failures.append("少于 3/4 walk-forward 分段同号")
    if best["q_value"] > 0.10:
        soft_failures.append("多重检验校正 q-value 高于 0.10")
    if best["edge_cost_ratio"] < 2.0:
        soft_failures.append("估算毛收益不足交易成本的 2 倍")
    if abs(max_corr) >= 0.70:
        soft_failures.append(f"与生产因子 {max_corr_name} 的相关性达到 {max_corr:.2f}")

    return {
        "factor": name,
        "coverage": _finite(coverage),
        "max_existing_correlation": max_corr,
        "max_existing_factor": max_corr_name,
        "best_horizon": best["horizon"],
        "candidate_score": best["candidate_score"],
        "horizons": horizon_reports,
        "gates": {
            "passed": not hard_failures and not soft_failures,
            "hard_failures": hard_failures,
            "soft_failures": soft_failures,
            "override_allowed": not hard_failures,
        },
    }
