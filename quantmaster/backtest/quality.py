"""Transport-neutral market-data and signal gates for backtest execution."""

from __future__ import annotations

from typing import Any

import pandas as pd

from quantmaster.runtime.problems import OperationProblem, Problem, make_problem


def _quality_problem(
    code: str,
    *,
    title: str,
    message: str,
    action: str,
    blocking: bool,
    can_continue: bool = False,
    items: list[object] | None = None,
) -> Problem:
    return make_problem(
        code, severity="warning" if can_continue else "error", source="策略回测",
        title=title, message=message, action=action, blocking=blocking,
        can_continue=can_continue, problem_id=f"backtest:{code}", items=items,
    )


def _raise_quality(status_code: int, problem: Problem, quality: dict[str, Any]) -> None:
    quality["status"] = "needs_confirmation" if problem["can_continue"] else "blocked"
    raise OperationProblem(status_code, problem, data_quality=quality)


def assess_panel_quality(
    panel: dict[str, pd.DataFrame], requested_symbols: list[str], *,
    minimum_symbols: int, allow_partial: bool,
) -> tuple[dict[str, Any], list[Problem]]:
    """Validate that a price panel can support an executable backtest."""
    requested = list(dict.fromkeys(requested_symbols))
    quality: dict[str, Any] = {
        "status": "complete", "requested_symbol_count": len(requested),
        "usable_symbol_count": 0, "missing_symbol_count": len(requested),
        "missing_symbols": requested[:20], "trading_days": 0,
        "actual_start": None, "actual_end": None, "valid_signal_dates": 0,
        "executable_signal_dates": 0, "selected_signals": 0,
        "executable_signals": 0, "benchmark_status": "not_checked",
    }
    warnings: list[Problem] = []
    missing_fields = [field for field in ("open", "close") if field not in panel]
    if missing_fields:
        _raise_quality(422, _quality_problem(
            "missing_price_fields", title="缺少回测必需价格",
            message=f"行情数据缺少 {', '.join(missing_fields)} 字段，无法计算真实成交与净值。",
            action="补齐开盘价和收盘价数据后重新回测。", blocking=True,
            items=missing_fields,
        ), quality)

    open_prices = panel["open"].replace([float("inf"), float("-inf")], pd.NA)
    close_prices = panel["close"].replace([float("inf"), float("-inf")], pd.NA)
    common_dates = open_prices.index.intersection(close_prices.index).sort_values()
    open_prices = open_prices.reindex(common_dates)
    close_prices = close_prices.reindex(common_dates)
    common_symbols = open_prices.columns.intersection(close_prices.columns)
    usable = [
        symbol for symbol in requested
        if symbol in common_symbols
        and int((open_prices[symbol].gt(0) & close_prices[symbol].gt(0)).sum()) >= 2
    ]
    valid_days = (
        open_prices.reindex(columns=usable).gt(0)
        & close_prices.reindex(columns=usable).gt(0)
    ).any(axis=1)
    dates = common_dates[valid_days]
    missing_symbols = [symbol for symbol in requested if symbol not in usable]
    quality.update({
        "usable_symbol_count": len(usable), "missing_symbol_count": len(missing_symbols),
        "missing_symbols": missing_symbols[:20], "trading_days": len(dates),
        "actual_start": str(dates[0].date()) if len(dates) else None,
        "actual_end": str(dates[-1].date()) if len(dates) else None,
    })
    if len(dates) < 2:
        _raise_quality(422, _quality_problem(
            "insufficient_trading_days", title="有效交易日不足",
            message=f"目前只有 {len(dates)} 个有效交易日，无法形成信号后的下一交易日成交。",
            action="扩大回测日期范围或刷新对应行情后重试。", blocking=True,
        ), quality)
    if len(usable) < minimum_symbols:
        _raise_quality(422, _quality_problem(
            "insufficient_usable_symbols", title="可用标的不足",
            message=f"策略需要至少 {minimum_symbols} 只标的，目前只有 {len(usable)} 只具备有效开收盘价。",
            action="减少选股数量，或补齐候选标的行情后重新回测。", blocking=True,
            items=missing_symbols,
        ), quality)

    reasons = []
    if missing_symbols:
        reasons.append(f"{len(missing_symbols)} 只候选缺少可用行情")
    if len(dates) < 20:
        reasons.append(f"有效区间仅 {len(dates)} 个交易日")
    if reasons:
        problem = _quality_problem(
            "partial_market_data", title="回测数据不完整",
            message="；".join(reasons) + "。继续会改变实际样本范围和选股结果。",
            action="建议先补齐数据；如已了解偏差，可仅用现有数据继续。",
            blocking=not allow_partial, can_continue=True, items=missing_symbols,
        )
        if not allow_partial:
            _raise_quality(409, problem, quality)
        warnings.append(problem)
        quality["status"] = "partial"
    return quality, warnings


def assess_signal_quality(
    panel: dict[str, pd.DataFrame], weights: pd.DataFrame, quality: dict[str, Any], *,
    allow_partial: bool,
    intentional_flat: pd.Series | None = None,
) -> list[Problem]:
    """Validate that signals can execute at a finite next-session open."""
    warnings: list[Problem] = []
    close = panel["close"]
    aligned = weights.reindex(index=close.index, columns=close.columns)
    positive = aligned.gt(0) & aligned.notna()
    flat = (
        intentional_flat.reindex(aligned.index).fillna(False).astype(bool)
        if intentional_flat is not None
        else pd.Series(False, index=aligned.index)
    )
    quality["intentional_flat_signal_dates"] = int(flat.sum())
    if int(positive.to_numpy().sum()) == 0:
        if flat.any():
            quality.update({
                "valid_signal_dates": int(flat.sum()),
                "executable_signal_dates": 0,
                "selected_signals": 0,
                "executable_signals": 0,
            })
            warnings.append(_quality_problem(
                "intentional_cash_backtest", title="策略按仓控保持现金",
                message="回测期间仓位计划明确选择空仓，没有把现金期误判为信号失败。",
                action="可查看仓控状态原因、市场基础仓位与合格标的数量。",
                blocking=False,
            ))
            return warnings
        _raise_quality(422, _quality_problem(
            "no_finite_signal", title="策略没有生成有效信号",
            message="当前数据与参数下没有任何正权重选股信号，继续计算只会得到无意义的空结果。",
            action="检查因子所需历史窗口、表达式和候选范围后重试。", blocking=True,
        ), quality)
    next_open = panel["open"].reindex(index=aligned.index, columns=aligned.columns).shift(-1)
    eligible = positive.copy()
    if len(eligible.index):
        eligible.iloc[-1] = False
    executable = eligible & next_open.gt(0)
    selected_count = int(eligible.to_numpy().sum())
    executable_count = int(executable.to_numpy().sum())
    quality.update({
        "valid_signal_dates": int(eligible.any(axis=1).sum()),
        "executable_signal_dates": int(executable.any(axis=1).sum()),
        "selected_signals": selected_count, "executable_signals": executable_count,
    })
    if flat.any():
        warnings.append(_quality_problem(
            "intentional_cash_periods", title="策略包含主动现金期",
            message=f"仓位计划在 {int(flat.sum())} 个调仓日明确保持空仓。",
            action="这是仓控结果，可结合状态原因复核。", blocking=False,
        ))
    if executable_count == 0:
        _raise_quality(422, _quality_problem(
            "no_executable_signal", title="信号无法成交",
            message="策略虽生成了选股信号，但下一交易日没有可用开盘价，无法模拟成交。",
            action="补齐信号后交易日的开盘价，或调整回测结束日期后重试。", blocking=True,
        ), quality)
    if executable_count < selected_count:
        missing = selected_count - executable_count
        problem = _quality_problem(
            "partial_execution_prices", title="部分信号缺少成交价",
            message=f"{selected_count} 个选股信号中有 {missing} 个缺少下一交易日开盘价，将被跳过。",
            action="建议补齐成交价；如已了解偏差，可跳过这些信号继续。",
            blocking=not allow_partial, can_continue=True,
        )
        if not allow_partial:
            _raise_quality(409, problem, quality)
        warnings.append(problem)
        quality["status"] = "partial"
    return warnings
