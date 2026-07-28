"""因子候选的鲁棒性压力测试。

所有测试只消费已经冻结的因子值与行情，不参与候选搜索或参数选择。结果采用
可序列化字典，便于写入研究账本并在 Web/CLI 中复验。
"""

from __future__ import annotations

import ast
import copy
import math
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.factors.analysis import forward_returns, information_coefficient
from quantmaster.factors.base import parse_expression

_WINDOW_ARGUMENTS: dict[str, tuple[int, ...]] = {
    "delay": (1,),
    "delta": (1,),
    "pct_change": (1,),
    "ts_mean": (1,),
    "ts_std": (1,),
    "ts_min": (1,),
    "ts_max": (1,),
    "ts_sum": (1,),
    "ts_rank": (1,),
    "ts_zscore": (1,),
    "ts_corr": (2,),
    "ema": (1,),
}


def _finite(value: float, default: float = 0.0) -> float:
    return round(float(value), 6) if np.isfinite(value) else default


def _percentile(values: np.ndarray, level: float) -> float:
    return _finite(float(np.percentile(values, level))) if values.size else 0.0


def expression_parameter_variants(
    expression: str, *, scales: tuple[float, ...] = (0.8, 1.2), limit: int = 8,
) -> dict[str, str]:
    """仅扰动白名单时序算子的窗口常量，不触碰系数或幂次常量。"""
    tree = parse_expression(expression)
    targets: list[tuple[int, str, int, float, int]] = []
    operator_counts: dict[str, int] = {}
    call_number = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        operator_counts[node.func.id] = operator_counts.get(node.func.id, 0) + 1
        indices = _WINDOW_ARGUMENTS.get(node.func.id, ())
        for argument_index in indices:
            if argument_index >= len(node.args):
                continue
            argument = node.args[argument_index]
            if isinstance(argument, ast.Constant) and isinstance(argument.value, (int, float)):
                value = float(argument.value)
                if value >= 2:
                    targets.append((
                        call_number,
                        node.func.id,
                        argument_index,
                        value,
                        operator_counts[node.func.id],
                    ))
        call_number += 1

    variants: dict[str, str] = {}
    for target_call, operator, argument_index, original, occurrence in targets:
        for scale in scales:
            replacement = max(1, round(original * scale))
            if replacement == int(original):
                continue
            candidate = copy.deepcopy(tree)
            current_call = 0
            for node in ast.walk(candidate):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
                    continue
                if current_call == target_call:
                    node.args[argument_index] = ast.Constant(value=replacement)
                    break
                current_call += 1
            ast.fix_missing_locations(candidate)
            label = f"{operator}#{occurrence}:{int(original)}→{replacement}"
            variants[label] = ast.unparse(candidate)
            if len(variants) >= max(1, int(limit)):
                return variants
    return variants


def monte_carlo_block_bootstrap(
    daily_ic: pd.Series,
    net_returns: pd.Series | None = None,
    *,
    horizon: int = 1,
    paths: int = 500,
    block_days: int = 20,
    seed: int = 42,
) -> dict[str, Any]:
    """对时序依赖友好的循环移动区块 bootstrap。"""
    ic = pd.Series(daily_ic, dtype=float).replace([np.inf, -np.inf], np.nan).dropna()
    if len(ic) < 40:
        return {
            "available": False, "passed": False, "paths": 0,
            "reason": f"有效 IC 仅 {len(ic)} 日，少于 Monte Carlo 最低 40 日",
        }
    paths = max(100, int(paths))
    block_days = min(len(ic), max(int(block_days), int(horizon) * 2, 5))
    blocks = math.ceil(len(ic) / block_days)
    rng = np.random.default_rng(int(seed) + int(horizon) * 1009)
    starts = rng.integers(0, len(ic), size=(paths, blocks, 1))
    offsets = np.arange(block_days, dtype=int).reshape(1, 1, -1)
    indices = ((starts + offsets) % len(ic)).reshape(paths, -1)[:, : len(ic)]
    ic_paths = ic.to_numpy(dtype=float)[indices]
    ic_means = ic_paths.mean(axis=1)
    ic_stds = ic_paths.std(axis=1, ddof=1)
    icirs = np.divide(
        ic_means, ic_stds, out=np.zeros_like(ic_means), where=ic_stds > 1e-12,
    )

    result: dict[str, Any] = {
        "available": True,
        "method": "circular_moving_block_bootstrap",
        "seed": int(seed),
        "paths": paths,
        "block_days": block_days,
        "observations": len(ic),
        "thresholds": {
            "probability_positive_ic": 0.90,
            "probability_positive_net": 0.75,
        },
        "ic_mean_ci_95": [_percentile(ic_means, 2.5), _percentile(ic_means, 97.5)],
        "icir_ci_95": [_percentile(icirs, 2.5), _percentile(icirs, 97.5)],
        "probability_positive_ic": _finite((ic_means > 0).mean()),
    }

    probability_positive_net = None
    if net_returns is not None:
        net = pd.Series(net_returns, dtype=float).reindex(ic.index).replace(
            [np.inf, -np.inf], np.nan,
        ).fillna(0.0)
        net_paths = net.to_numpy(dtype=float)[indices]
        clipped = np.clip(net_paths, -0.999, None)
        # quantile_backtest 的 periods 控制调仓频率，但这里仍是逐日收益序列。
        annual_exponent = 244 / len(net)
        annual = np.prod(1 + clipped, axis=1) ** annual_exponent - 1
        probability_positive_net = float((annual > 0).mean())
        result.update({
            "net_annual_ci_95": [_percentile(annual, 2.5), _percentile(annual, 97.5)],
            "probability_positive_net": _finite(probability_positive_net),
        })

    ic_pass = float(result["probability_positive_ic"]) >= 0.90
    net_pass = probability_positive_net is None or probability_positive_net >= 0.75
    reasons = []
    if not ic_pass:
        reasons.append("Monte Carlo 中正向 IC 概率低于 90%")
    if not net_pass:
        reasons.append("Monte Carlo 中扣费后正年化概率低于 75%")
    result["passed"] = ic_pass and net_pass
    result["reasons"] = reasons
    return result


def walk_forward_robustness(folds: list[dict[str, Any]]) -> dict[str, Any]:
    """汇总 expanding walk-forward 的同号性、衰减和最差折。"""
    usable = [item for item in folds if int(item.get("test_days") or 0) > 0]
    if not usable:
        return {"available": False, "passed": False, "reason": "没有可用 WFA 折叠"}
    test_ic = np.asarray([float(item.get("rank_ic") or 0.0) for item in usable])
    retentions = np.asarray([
        float(item.get("retention") or 0.0) for item in usable
        if item.get("retention") is not None
    ])
    sign_consistency = float((test_ic > 0).mean())
    median_retention = float(np.median(retentions)) if retentions.size else 0.0
    dispersion = float(np.std(test_ic, ddof=1)) if len(test_ic) > 1 else 0.0
    passed = sign_consistency >= 0.75 and median_retention >= 0.35 and float(test_ic.min()) > -0.02
    reasons = []
    if sign_consistency < 0.75:
        reasons.append("少于 3/4 WFA 折叠保持正向")
    if median_retention < 0.35:
        reasons.append("WFA 中位 IC 保留率低于 35%")
    if float(test_ic.min()) <= -0.02:
        reasons.append("最差 WFA 折叠 RankIC 不高于 -0.02")
    return {
        "available": True, "passed": passed, "fold_count": len(usable),
        "thresholds": {
            "sign_consistency": 0.75,
            "median_retention": 0.35,
            "worst_fold_rank_ic": -0.02,
        },
        "sign_consistency": _finite(sign_consistency),
        "median_retention": _finite(median_retention),
        "worst_fold_rank_ic": _finite(test_ic.min()),
        "fold_rank_ic_dispersion": _finite(dispersion),
        "reasons": reasons,
    }


def parameter_sensitivity(
    baseline: pd.DataFrame,
    variants: dict[str, pd.DataFrame] | None,
    close: pd.DataFrame,
    *,
    horizon: int,
    direction: int,
    oos_index: pd.Index,
) -> dict[str, Any]:
    """检验参数邻域是否形成平台，而非单点尖峰。"""
    if not variants:
        return {
            "available": False, "applicable": False, "passed": True,
            "reason": "因子没有可扰动的显式窗口参数",
            "tested_variants": 0,
        }
    forward = forward_returns(close, periods=horizon)
    baseline_ic = information_coefficient(baseline, forward).reindex(oos_index) * direction
    baseline_mean = float(baseline_ic.dropna().mean()) if baseline_ic.notna().any() else 0.0
    rows = []
    for label, values in variants.items():
        aligned = values.reindex(index=baseline.index, columns=baseline.columns)
        daily_ic = information_coefficient(aligned, forward).reindex(oos_index) * direction
        mean = float(daily_ic.dropna().mean()) if daily_ic.notna().any() else 0.0
        retention = abs(mean) / max(abs(baseline_mean), 1e-12)
        left, right = baseline.rank(axis=1).align(aligned.rank(axis=1), join="inner")
        correlation = float(left.corrwith(right, axis=1).replace(
            [np.inf, -np.inf], np.nan,
        ).mean())
        rows.append({
            "variant": str(label), "rank_ic": _finite(mean),
            "retention": _finite(retention), "same_sign": bool(mean > 0),
            "factor_rank_correlation": _finite(correlation),
        })
    same_sign_ratio = float(np.mean([item["same_sign"] for item in rows]))
    median_retention = float(np.median([item["retention"] for item in rows]))
    worst_retention = float(min(item["retention"] for item in rows))
    median_correlation = float(np.median([item["factor_rank_correlation"] for item in rows]))
    passed = (
        same_sign_ratio >= 0.75 and median_retention >= 0.65
        and worst_retention >= 0.35 and median_correlation >= 0.60
    )
    reasons = []
    if same_sign_ratio < 0.75:
        reasons.append("参数邻域同号比例低于 75%")
    if median_retention < 0.65:
        reasons.append("参数邻域中位 IC 保留率低于 65%")
    if worst_retention < 0.35:
        reasons.append("参数邻域最差 IC 保留率低于 35%")
    if median_correlation < 0.60:
        reasons.append("参数邻域因子排序中位相关性低于 0.60")
    return {
        "available": True, "applicable": True, "passed": passed,
        "thresholds": {
            "same_sign_ratio": 0.75,
            "median_retention": 0.65,
            "worst_retention": 0.35,
            "median_factor_rank_correlation": 0.60,
        },
        "baseline_rank_ic": _finite(baseline_mean), "tested_variants": len(rows),
        "same_sign_ratio": _finite(same_sign_ratio),
        "median_retention": _finite(median_retention),
        "worst_retention": _finite(worst_retention),
        "median_factor_rank_correlation": _finite(median_correlation),
        "variants": rows, "reasons": reasons,
    }


def _slice_ic(
    factor: pd.DataFrame, forward: pd.DataFrame, mask: pd.DataFrame,
) -> pd.Series:
    return information_coefficient(factor.where(mask), forward.where(mask)).dropna()


def penetration_analysis(
    factor: pd.DataFrame,
    close: pd.DataFrame,
    daily_ic: pd.Series,
    *,
    horizon: int,
    panel: dict[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    """穿透到年份、行情状态、流动性和个股贡献，识别集中伪稳健。"""
    ic = pd.Series(daily_ic, dtype=float).dropna()
    if len(ic) < 40:
        return {"available": False, "passed": False, "reason": "穿透分析有效日不足 40"}
    factor = factor.reindex(index=close.index, columns=close.columns)
    forward = forward_returns(close, periods=horizon)
    dates = ic.index.intersection(factor.index)

    years = []
    for year, values in ic.groupby(ic.index.year):
        if len(values) < 20:
            continue
        years.append({
            "year": int(year), "days": len(values), "rank_ic": _finite(values.mean()),
            "positive_ratio": _finite((values > 0).mean()),
        })
    positive_year_ratio = float(np.mean([item["rank_ic"] > 0 for item in years])) if years else 0.0
    worst_year = min((item["rank_ic"] for item in years), default=0.0)

    trailing_market = close.pct_change(60, fill_method=None).median(axis=1).reindex(dates)
    trailing_volatility = close.pct_change(fill_method=None).rolling(20).std().median(axis=1).reindex(dates)
    volatility_cut = float(trailing_volatility.median())
    regime_masks = {
        "uptrend": trailing_market >= 0,
        "downtrend": trailing_market < 0,
        "high_volatility": trailing_volatility >= volatility_cut,
        "normal_volatility": trailing_volatility < volatility_cut,
    }
    regimes = []
    for name, mask in regime_masks.items():
        values = ic.reindex(dates)[mask.fillna(False)]
        if len(values) < 20:
            continue
        regimes.append({
            "regime": name, "days": len(values), "rank_ic": _finite(values.mean()),
            "positive_ratio": _finite((values > 0).mean()),
        })
    regime_sign_ratio = float(np.mean([item["rank_ic"] > 0 for item in regimes])) if regimes else 0.0

    liquidity: dict[str, Any] = {
        "available": False, "passed": True,
        "reason": "快照没有 amount/volume，未执行流动性穿透",
    }
    source_name = ""
    source = None
    for field in ("amount", "volume"):
        if panel is not None and field in panel:
            source_name, source = field, panel[field]
            break
    if source is not None:
        trailing = source.reindex(index=close.index, columns=close.columns).rolling(20).mean()
        percentile = trailing.rank(axis=1, pct=True)
        buckets = []
        for name, mask in (("low", percentile <= 0.5), ("high", percentile > 0.5)):
            bucket_ic = _slice_ic(factor.loc[dates], forward.loc[dates], mask.loc[dates])
            buckets.append({
                "bucket": name, "days": len(bucket_ic), "rank_ic": _finite(bucket_ic.mean()),
                "positive_ratio": _finite((bucket_ic > 0).mean()) if len(bucket_ic) else 0.0,
            })
        liquid_sign_ratio = float(np.mean([
            item["rank_ic"] > 0 for item in buckets if item["days"] >= 20
        ])) if any(item["days"] >= 20 for item in buckets) else 0.0
        liquidity = {
            "available": True, "passed": liquid_sign_ratio >= 0.5,
            "field": source_name, "same_sign_ratio": _finite(liquid_sign_ratio),
            "buckets": buckets,
        }

    ranks = factor.loc[dates].rank(axis=1, pct=True)
    active_returns = forward.loc[dates].sub(forward.loc[dates].mean(axis=1), axis=0)
    selected = ranks >= 0.8
    contribution = active_returns.where(selected).div(selected.sum(axis=1).replace(0, np.nan), axis=0)
    by_symbol = contribution.abs().sum(axis=0, min_count=1).dropna().sort_values(ascending=False)
    total = float(by_symbol.sum())
    shares = by_symbol / total if total > 0 else by_symbol * 0.0
    top1_share = float(shares.iloc[:1].sum()) if len(shares) else 1.0
    top5_share = float(shares.iloc[:5].sum()) if len(shares) else 1.0
    squared_share_sum = float(np.square(shares).sum())
    effective_names = float(1 / squared_share_sum) if len(shares) and squared_share_sum > 0 else 0.0
    concentration_passed = top1_share <= 0.20 and top5_share <= 0.55 and effective_names >= 8
    concentration = {
        "passed": concentration_passed, "symbols": len(by_symbol),
        "top1_absolute_contribution_share": _finite(top1_share),
        "top5_absolute_contribution_share": _finite(top5_share),
        "effective_names": _finite(effective_names),
        "top_contributors": [
            {"symbol": str(symbol), "share": _finite(value)}
            for symbol, value in shares.iloc[:5].items()
        ],
    }

    temporal_passed = bool(years) and positive_year_ratio >= 0.5 and worst_year > -0.03
    regime_passed = len(regimes) >= 2 and regime_sign_ratio >= 0.5
    passed = temporal_passed and regime_passed and concentration_passed and bool(liquidity["passed"])
    reasons = []
    if not temporal_passed:
        reasons.append("年度穿透稳定性未通过")
    if not regime_passed:
        reasons.append("市场状态穿透稳定性未通过")
    if not concentration_passed:
        reasons.append("收益贡献过度集中于少数个股")
    if not liquidity["passed"]:
        reasons.append("高低流动性分层未保持稳定")
    return {
        "available": True, "passed": passed,
        "thresholds": {
            "positive_year_ratio": 0.50,
            "worst_year_rank_ic": -0.03,
            "regime_same_sign_ratio": 0.50,
            "liquidity_same_sign_ratio": 0.50,
            "top1_absolute_contribution_share": 0.20,
            "top5_absolute_contribution_share": 0.55,
            "effective_names": 8,
        },
        "time": {
            "passed": temporal_passed, "positive_year_ratio": _finite(positive_year_ratio),
            "worst_year_rank_ic": _finite(worst_year), "years": years,
        },
        "regimes": {
            "passed": regime_passed, "same_sign_ratio": _finite(regime_sign_ratio),
            "buckets": regimes,
        },
        "liquidity": liquidity,
        "concentration": concentration,
        "reasons": reasons,
    }


def robustness_summary(
    *,
    monte_carlo: dict[str, Any],
    parameter_report: dict[str, Any],
    walk_forward: dict[str, Any],
    penetration: dict[str, Any],
) -> dict[str, Any]:
    tests = {
        "monte_carlo": monte_carlo,
        "parameter_sensitivity": parameter_report,
        "walk_forward": walk_forward,
        "penetration": penetration,
    }
    failed = [name for name, report in tests.items() if not bool(report.get("passed"))]
    applicable = [name for name, report in tests.items() if report.get("applicable", True)]
    return {
        "schema_version": 1,
        "passed": not failed,
        "tests_passed": len(applicable) - len(failed),
        "tests_applicable": len(applicable),
        "failed_tests": failed,
        **tests,
    }
