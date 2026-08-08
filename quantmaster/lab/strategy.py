"""多因子组合、T+1 执行和交易动作的共享数值内核。"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.horizons import SUPPORTED_HORIZONS, require_supported_horizon

TRADING_DAYS = 244
ENSEMBLE_MIN_COMPONENTS = 3
ENSEMBLE_MAX_COMPONENTS = 8
MAX_COMPONENT_CORRELATION = 0.70


def atomic_horizon_gate(
    evidence: dict[str, Any], *, coverage: float, research_quality: str,
) -> dict[str, Any]:
    """Return an independently deployable gate for one exact horizon."""
    hard: list[str] = []
    soft: list[str] = []
    if research_quality != "production":
        hard.append("候选不是 point-in-time 生产级快照")
    if coverage < 0.70:
        hard.append(f"因子覆盖率 {coverage:.1%} 低于 70%")
    if int(evidence.get("oos_days") or 0) < 252:
        hard.append(f"样本外仅 {int(evidence.get('oos_days') or 0)} 个交易日，少于 252 日")
    if abs(float(evidence.get("oos_rank_ic") or evidence.get("rank_ic") or 0)) < 0.02:
        soft.append("样本外 |RankIC| 低于 0.02")
    if abs(float(evidence.get("oos_icir") or evidence.get("icir") or 0)) < 0.20:
        soft.append("样本外 |ICIR| 低于 0.20")
    if float(evidence.get("fold_same_sign") or 0) < 0.75:
        soft.append("少于 3/4 walk-forward 分段同号")
    if float(evidence.get("q_value", 1)) > 0.10:
        soft.append("多重检验校正 q-value 高于 0.10")
    if float(evidence.get("edge_cost_ratio") or 0) < 2.0:
        soft.append("估算毛收益不足交易成本的 2 倍")
    return {
        "passed": not hard and not soft,
        "hard_failures": hard,
        "soft_failures": soft,
        "override_allowed": not hard,
    }


def _blocked_trade(
    symbol: str, date: pd.Timestamp, side: str, panel: dict[str, pd.DataFrame],
) -> bool:
    suspended = panel.get("suspended")
    if suspended is not None:
        value = suspended.reindex(index=[date], columns=[symbol]).iloc[0, 0]
        if pd.notna(value) and bool(value):
            return True
    field = "up_limit" if side == "buy" else "down_limit"
    limit_frame = panel.get(field)
    open_prices = panel["open"]
    if limit_frame is not None:
        limit_value = limit_frame.reindex(index=[date], columns=[symbol]).iloc[0, 0]
        open_value = open_prices.reindex(index=[date], columns=[symbol]).iloc[0, 0]
        if pd.notna(limit_value) and pd.notna(open_value):
            tolerance = max(1e-6, abs(float(limit_value)) * 1e-6)
            if side == "buy" and float(open_value) >= float(limit_value) - tolerance:
                return True
            if side == "sell" and float(open_value) <= float(limit_value) + tolerance:
                return True
    return False


def target_weights(
    scores: pd.DataFrame, *, top_n: int = 12, cap_weight: float = 0.10,
    industry_map: dict[str, str] | None = None, industry_cap: float = 0.25,
) -> pd.DataFrame:
    """Convert cross-sectional scores to diversified daily target weights."""
    if not 10 <= top_n <= 15:
        raise ValueError("组合持仓数必须在 10–15 只之间")
    ranks = scores.rank(axis=1, ascending=False, method="first")
    selected = (ranks <= top_n) & scores.notna()
    if industry_map:
        selected.loc[:, :] = False
        maximum_per_industry = max(1, math.floor(top_n * industry_cap + 1e-9))
        for date, row in scores.iterrows():
            counts: dict[str, int] = {}
            chosen: list[str] = []
            for symbol in row.dropna().sort_values(ascending=False).index:
                industry = industry_map.get(str(symbol), "未知")
                if industry != "未知" and counts.get(industry, 0) >= maximum_per_industry:
                    continue
                chosen.append(str(symbol))
                counts[industry] = counts.get(industry, 0) + 1
                if len(chosen) == top_n:
                    break
            selected.loc[date, chosen] = True
    weights = selected.astype(float)
    weights = weights.div(weights.sum(axis=1).replace(0, np.nan), axis=0).fillna(0.0)
    return weights.clip(upper=cap_weight)


def execute_daily_targets(
    scores: pd.DataFrame,
    panel: dict[str, pd.DataFrame],
    *,
    horizon: int,
    top_n: int = 12,
    cap_weight: float = 0.10,
    benchmark_returns: pd.Series | None = None,
    industry_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Simulate T-close signals executed at T+1 open with directional costs.

    The forecast horizon changes the signal/evidence; target positions still roll every
    trading day.  Returns are next-open to following-open so no T close information is
    credited before it can be traded.
    """
    require_supported_horizon(horizon)
    open_prices = panel["open"].astype(float).sort_index()
    signal, opens = scores.align(open_prices, join="inner")
    desired = target_weights(
        signal, top_n=top_n, cap_weight=cap_weight, industry_map=industry_map,
    ).shift(1).fillna(0.0)
    dates = desired.index
    columns = desired.columns
    executed = pd.DataFrame(0.0, index=dates, columns=columns)
    costs = pd.Series(0.0, index=dates)
    turnover = pd.Series(0.0, index=dates)
    cfg = get_config().trade
    buy_rate = float(cfg.commission_rate + cfg.transfer_fee_rate + cfg.slippage)
    sell_rate = float(buy_rate + cfg.stamp_tax_rate)
    previous = pd.Series(0.0, index=columns)
    for date in dates:
        wanted = desired.loc[date].fillna(0.0)
        current = wanted.copy()
        changed = (wanted - previous).abs() > 1e-12
        for symbol in columns[changed.to_numpy()]:
            delta = float(wanted[symbol] - previous[symbol])
            side = "buy" if delta > 0 else "sell"
            if pd.isna(opens.at[date, symbol]) or _blocked_trade(symbol, date, side, panel):
                current[symbol] = previous[symbol]
        delta = current - previous
        buys = float(delta.clip(lower=0).sum())
        sells = float((-delta.clip(upper=0)).sum())
        costs.at[date] = buys * buy_rate + sells * sell_rate
        turnover.at[date] = 0.5 * float(delta.abs().sum())
        executed.loc[date] = current
        previous = current

    asset_returns = opens.shift(-1).div(opens).sub(1.0)
    gross = (executed * asset_returns).sum(axis=1, min_count=1).fillna(0.0)
    net = gross - costs
    if benchmark_returns is None:
        benchmark = asset_returns.mean(axis=1, skipna=True).fillna(0.0)
    else:
        benchmark = benchmark_returns.reindex(net.index).fillna(0.0)
    excess = net - benchmark
    nav = (1.0 + excess.clip(lower=-0.999)).cumprod()
    annual = float((1.0 + excess.clip(lower=-0.999)).prod() ** (TRADING_DAYS / max(1, len(excess))) - 1)
    std = float(excess.std(ddof=1))
    net_ir = float(excess.mean() / std * math.sqrt(TRADING_DAYS)) if std > 0 else 0.0
    max_drawdown = float((1.0 - nav / nav.cummax()).max()) if len(nav) else 0.0
    fold_returns = []
    for chunk in np.array_split(excess.to_numpy(float), 4):
        fold_returns.append(float(np.prod(1.0 + np.clip(chunk, -0.999, None)) - 1.0))
    return {
        "horizon": horizon,
        "daily_gross": gross,
        "daily_net": net,
        "daily_excess": excess,
        "nav": nav,
        "weights": executed,
        "costs": costs,
        "turnover_series": turnover,
        "metrics": {
            "net_information_ratio": net_ir,
            "net_annual_excess_return": annual,
            "max_drawdown": max_drawdown,
            "calmar": annual / max(max_drawdown, 1e-12),
            "turnover": float(turnover.mean()),
            "cost_annual": float(costs.mean() * TRADING_DAYS),
            "samples": len(excess),
            "positive_folds": int(sum(value > 0 for value in fold_returns)),
            "fold_net_returns": fold_returns,
        },
    }


def moving_block_return_interval(
    returns: pd.Series, *, block_days: int = 20, paths: int = 500, seed: int = 42,
) -> dict[str, Any]:
    values = pd.Series(returns, dtype=float).replace([np.inf, -np.inf], np.nan).dropna()
    if len(values) < 40:
        return {"available": False, "probability_positive": 0.0, "ci_95": [0.0, 0.0]}
    block_days = min(len(values), max(5, int(block_days)))
    blocks = math.ceil(len(values) / block_days)
    rng = np.random.default_rng(seed)
    starts = rng.integers(0, len(values), size=(paths, blocks, 1))
    offsets = np.arange(block_days).reshape(1, 1, -1)
    indices = ((starts + offsets) % len(values)).reshape(paths, -1)[:, : len(values)]
    sampled = values.to_numpy(float)[indices]
    annual = np.prod(1.0 + np.clip(sampled, -0.999, None), axis=1) ** (
        TRADING_DAYS / len(values)
    ) - 1.0
    return {
        "available": True,
        "method": "circular_moving_block_bootstrap",
        "paths": paths,
        "block_days": block_days,
        "probability_positive": float((annual > 0).mean()),
        "ci_95": [float(np.percentile(annual, 2.5)), float(np.percentile(annual, 97.5))],
    }


def cross_sectional_correlation(left: pd.DataFrame, right: pd.DataFrame) -> float:
    a, b = left.rank(axis=1).align(right.rank(axis=1), join="inner")
    daily = a.corrwith(b, axis=1).replace([np.inf, -np.inf], np.nan).dropna()
    return float(daily.median()) if len(daily) else 0.0


def ensemble_weights(
    candidates: list[dict[str, Any]], values: dict[str, pd.DataFrame], *, horizon: int,
) -> list[dict[str, Any]]:
    """Select 3–8 non-duplicate factors using development evidence only."""
    require_supported_horizon(horizon)
    ranked = sorted(
        candidates,
        key=lambda item: float(item.get("development_score") or 0),
        reverse=True,
    )
    selected: list[dict[str, Any]] = []
    for candidate in ranked:
        version_id = str(candidate["version_id"])
        if version_id not in values:
            continue
        duplicate = any(
            abs(cross_sectional_correlation(values[version_id], values[str(other["version_id"])]))
            >= MAX_COMPONENT_CORRELATION
            for other in selected
        )
        if duplicate:
            continue
        selected.append(candidate)
        if len(selected) == ENSEMBLE_MAX_COMPONENTS:
            break
    if len(selected) < ENSEMBLE_MIN_COMPONENTS:
        return []
    raw = np.asarray([max(1e-6, float(item.get("development_score") or 0)) for item in selected])
    optimized = raw / raw.sum()
    shrunk = 0.50 * optimized + 0.50 / len(selected)
    shrunk = np.maximum(shrunk, 0.05)
    shrunk /= shrunk.sum()
    for _ in range(len(shrunk) + 1):
        over = shrunk > 0.35 + 1e-12
        if not over.any():
            break
        excess = float((shrunk[over] - 0.35).sum())
        shrunk[over] = 0.35
        under = ~over
        room = np.maximum(0.0, 0.35 - shrunk[under])
        if room.sum() <= 0:
            break
        shrunk[under] += excess * room / room.sum()
    return [
        {"version_id": str(item["version_id"]), "weight": float(weight), "horizon": horizon}
        for item, weight in zip(selected, shrunk, strict=True)
    ]


def combine_scores(
    components: list[dict[str, Any]], values: dict[str, pd.DataFrame],
    *, reference_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Combine factor percentiles, optionally calibrated to a fixed reference pool."""
    combined: pd.DataFrame | None = None
    total = 0.0
    seen: set[str] = set()
    for component in components:
        version_id = str(component["version_id"])
        if version_id in seen or version_id not in values:
            continue
        seen.add(version_id)
        weight = float(component["weight"])
        frame = values[version_id]
        if reference_columns is None:
            ranked = frame.rank(axis=1, pct=True)
        else:
            reference = frame.reindex(columns=reference_columns)
            ranked = reference.rank(axis=1, pct=True).reindex(columns=frame.columns)
            extra_columns = [column for column in frame.columns if column not in reference.columns]
            for date in frame.index:
                sample = np.sort(reference.loc[date].dropna().to_numpy(float))
                if not len(sample):
                    continue
                extra = frame.loc[date, extra_columns].dropna()
                if len(extra):
                    ranked.loc[date, extra.index] = (
                        np.searchsorted(sample, extra.to_numpy(float), side="right") / len(sample)
                    )
        combined = ranked * weight if combined is None else combined.add(ranked * weight, fill_value=0)
        total += weight
    if combined is None or total <= 0:
        raise ValueError("组合没有可用且不重复的因子")
    return combined / total


def strategy_sealed_gate(
    metrics: dict[str, Any], bootstrap: dict[str, Any], *, baseline_calmar: float = 0.0,
) -> dict[str, Any]:
    failures: list[str] = []
    if float(metrics.get("net_information_ratio") or 0) < 0.5:
        failures.append("密封集净 IR 低于 0.5")
    if float(metrics.get("net_annual_excess_return") or 0) <= 0:
        failures.append("密封集年化扣费后超额收益不为正")
    if int(metrics.get("positive_folds") or 0) < 3:
        failures.append("少于 3/4 折扣费后收益为正")
    if float(bootstrap.get("probability_positive") or 0) < 0.75:
        failures.append("正净收益 bootstrap 概率低于 75%")
    if float(metrics.get("max_drawdown") or 1) > 0.25:
        failures.append("最大回撤高于 25%")
    if float(metrics.get("calmar") or 0) < baseline_calmar:
        failures.append("Calmar 低于规则基线")
    return {"passed": not failures, "failures": failures, "override_allowed": False}


def holding_actions(
    target: dict[str, float], current: dict[str, float], *, buffer: float = 0.01,
    evidence_valid: bool = True, confidence: dict[str, float] | None = None,
) -> list[dict[str, Any]]:
    """Translate target/current differences without forced exits on missing evidence."""
    result: list[dict[str, Any]] = []
    confidence = confidence or {}
    for symbol in sorted(set(target) | set(current)):
        wanted = max(0.0, float(target.get(symbol, 0.0)))
        held = max(0.0, float(current.get(symbol, 0.0)))
        delta = wanted - held
        valid = evidence_valid and float(confidence.get(symbol, 1.0)) >= 0.5
        if not valid:
            action = "review"
        elif held <= 0 and wanted > 0:
            action = "buy"
        elif abs(delta) <= buffer:
            action = "hold"
        elif delta > 0:
            action = "add"
        elif wanted > 0:
            action = "reduce"
        else:
            action = "exit"
        result.append({
            "symbol": symbol, "action": action,
            "current_weight": held, "target_weight": wanted, "difference": delta,
            "confidence": float(confidence.get(symbol, 1.0)),
            "invalidation": "数据不足或 CSI800 参考校准失效时转人工复核，不强制退出",
        })
    return result


def return_curve_points(strategies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build chart points from immutable per-horizon evidence only."""
    by_horizon: dict[int, dict[str, Any]] = {}
    for strategy in strategies:
        horizon = int(strategy.get("horizon") or 0)
        if horizon not in SUPPORTED_HORIZONS:
            continue
        evidence = strategy.get("sealed_evidence") or strategy.get("evidence") or {}
        metrics = evidence.get("metrics") or evidence
        bootstrap = evidence.get("bootstrap") or {}
        by_horizon[horizon] = {
            "horizon": horizon,
            "strategy_id": strategy.get("id", ""),
            "status": strategy.get("status", "historical_candidate"),
            "annual_net_excess_return": float(metrics.get("net_annual_excess_return") or 0),
            "ci_95": list(bootstrap.get("ci_95") or [0.0, 0.0]),
            "net_information_ratio": float(metrics.get("net_information_ratio") or 0),
            "max_drawdown": float(metrics.get("max_drawdown") or 0),
            "turnover": float(metrics.get("turnover") or 0),
            "cost_annual": float(metrics.get("cost_annual") or 0),
            "samples": int(metrics.get("samples") or 0),
            "shadow": strategy.get("shadow_summary") or {},
        }
    return [by_horizon.get(h, {"horizon": h, "missing": True}) for h in SUPPORTED_HORIZONS]
