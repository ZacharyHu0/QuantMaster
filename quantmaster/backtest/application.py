"""Transport-neutral, auditable backtest execution."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict
from typing import Any

import pandas as pd

from quantmaster import __version__
from quantmaster.backtest.spec import BacktestSpec, LabVersionStrategySpec
from quantmaster.config import get_config
from quantmaster.trading_sessions import resolve_session_target


def _points(series: pd.Series | None) -> list[list[Any]]:
    if series is None:
        return []
    return [
        [pd.Timestamp(index).strftime("%Y-%m-%d"), round(float(value), 6)]
        for index, value in series.dropna().items()
    ]


def _formal_eligibility(
    spec: BacktestSpec,
    *,
    resolved_tier: str,
    universe_quality: str,
    data_quality: dict[str, Any],
    research_manifest: dict[str, Any],
    benchmark_required: bool,
    warnings: list[dict[str, Any]],
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if resolved_tier != "production":
        reasons.append("sandbox_research_tier")
    if universe_quality != "production":
        reasons.append("universe_not_pit")
    if isinstance(spec.strategy, LabVersionStrategySpec):
        reasons.append("lab_oof_result")
    if not research_manifest:
        reasons.append("missing_research_manifest")
    blocking_codes = {
        "market_data_degraded",
        "partial_market_data",
        "partial_execution_prices",
    }
    warning_codes = {str(item.get("code") or "") for item in warnings}
    if warning_codes & blocking_codes:
        reasons.append("incomplete_market_evidence")
    if data_quality.get("status") != "complete":
        reasons.append("data_quality_not_complete")
    market_contract = data_quality.get("market_contract") or {}
    if market_contract.get("formal_eligible") is False:
        reasons.append("market_contract_not_formal")
    benchmark_contract = data_quality.get("benchmark_contract") or {}
    if benchmark_required and (
        data_quality.get("benchmark_status") != "verified"
        or benchmark_contract.get("formal_eligible") is not True
    ):
        reasons.append("benchmark_evidence_not_verified")
    return not reasons, list(dict.fromkeys(reasons))


def _backtest_window(spec: BacktestSpec) -> str:
    if spec.end:
        return spec.end
    expectation = resolve_session_target()
    if not expectation.ready or not expectation.session:
        raise ValueError(f"默认回测截止交易日不可用：{expectation.reason}")
    return expectation.session


def _backtest_inputs(
    spec: BacktestSpec, end: str, panel: dict[str, pd.DataFrame] | None,
    membership: pd.DataFrame | None, resolved_tier: str, warnings: list[dict[str, Any]], checkpoint) -> tuple[
        dict[str, pd.DataFrame], pd.DataFrame | None, list[str], str, dict[str, Any], Any,
        bool,
    ]:
    from quantmaster import data as data_api
    from quantmaster.data.universe import load_universe
    from quantmaster.lab.dataset import load_csi800_membership

    if spec.universe.lower() == "csi800":
        if membership is None:
            membership = load_csi800_membership(spec.start, end)
        symbols = sorted(symbol for symbol in membership if membership[symbol].any())
        universe_quality = "production"
    else:
        symbols = load_universe(spec.universe, as_of=end)
        universe_quality = "sandbox"
        warnings.append({
            "code": "fixed_universe", "level": "warning",
            "message": "固定候选可能包含幸存者偏差；生产研究建议使用 csi800 历史成分。",
        })
    if not symbols:
        raise ValueError("候选中没有可回测标的")
    checkpoint(18, "加载行情", f"读取 {len(symbols)} 只标的的历史行情")
    provided_panel = panel is not None
    market_quality = None
    research_manifest: dict[str, Any] = {}
    if panel is None and resolved_tier == "production":
        from quantmaster.data.research import load_research_bundle

        def research_progress(done: int, total: int, symbol: str, success: bool, detail: str = "") -> None:
            request_estimate = max(0, total - done) * 4
            suffix = f" · {detail}" if detail else ""
            if detail == "下载 PIT 缺口":
                minutes = (request_estimate + 119) // 120
                suffix += f" · 余约 {request_estimate} 请求 / {minutes} 分钟（120/分）"
            checkpoint(
                18 + int(18 * done / max(1, total)), "加载原始成交/PIT约束",
                f"{done}/{total} · {symbol}{'' if success else ' 失败'}{suffix}",
            )

        bundle = load_research_bundle(
            symbols, spec.start, end, membership=membership, progress=research_progress,
        )
        panel = bundle.backtest_panel()
        research_manifest = {**bundle.manifest, "manifest_hash": bundle.manifest_hash}
    elif panel is None:
        market_envelope = data_api.refresh_panel(symbols, spec.start, end)
        panel = market_envelope.require_data()
        market_quality = market_envelope.quality
        warnings.append({
            "code": "sandbox_execution_approximation", "level": "warning",
            "message": "Sandbox 使用旧前复权缓存与代码板涨跌停近似，不能作为生产晋升证据。",
        })
    elif resolved_tier == "production":
        required = {"execution_open", "execution_close", "adj_factor", "up_limit", "down_limit", "suspended"}
        missing = sorted(required - set(panel))
        if missing:
            raise ValueError("production 回测缺少真实成交字段：" + "、".join(missing))
    return panel, membership, symbols, universe_quality, research_manifest, market_quality, provided_panel


def _backtest_signal(
    spec: BacktestSpec, panel: dict[str, pd.DataFrame], membership: pd.DataFrame | None,
    end: str, resolved_tier: str, data_quality: dict[str, Any], warnings: list[dict[str, Any]], checkpoint,
) -> tuple[Any, Any, Any]:
    from quantmaster.backtest.quality import assess_signal_quality
    from quantmaster.backtest.spec import build_strategy

    symbols = list(panel["close"].columns)
    checkpoint(38, "计算信号", "按策略快照生成目标权重")
    strategy = build_strategy(spec.strategy, symbols, spec.start, end, universe=spec.universe)
    signal_bundle = strategy.signal_bundle(panel, eligibility_mask=membership)
    if (
        resolved_tier == "production"
        and signal_bundle.metadata.get("prediction_source") == "rolling_oof"
        and signal_bundle.metadata.get("research_quality") != "production"
    ):
        raise ValueError("production 回测拒绝使用 Sandbox 训练的学习模型")
    weights = signal_bundle.weights
    if (
        membership is not None
        and signal_bundle.metadata.get("position_control") != "hybrid-position-control-v1"
    ):
        active = weights.notna().any(axis=1)
        totals = weights.loc[active].sum(axis=1).replace(0, float("nan"))
        weights.loc[active] = weights.loc[active].div(totals, axis=0).fillna(0.0)
    warnings.extend(assess_signal_quality(
        panel, weights, data_quality, allow_partial=spec.allow_partial,
        intentional_flat=signal_bundle.intentional_flat,
    ))
    return strategy, weights, signal_bundle


def _backtest_benchmark(
    spec: BacktestSpec, end: str, benchmark_close: pd.Series | None,
    data_quality: dict[str, Any], warnings: list[dict[str, Any]],
) -> tuple[pd.Series | None, bool]:
    from quantmaster import data as data_api

    required = bool(spec.benchmark) or benchmark_close is not None
    if benchmark_close is None and spec.benchmark:
        try:
            envelope = data_api.refresh_history(spec.benchmark, spec.start, end)
            benchmark_close = envelope.require_data()["close"]
            if benchmark_close.empty:
                raise ValueError("基准没有可用收盘价")
            data_quality["benchmark_status"] = envelope.quality.status
            data_quality["benchmark_contract"] = envelope.quality.to_dict()
        except Exception as exc:
            data_quality["benchmark_status"] = "unavailable"
            data_quality["status"] = "partial"
            warnings.append({
                "code": "benchmark_unavailable", "level": "warning",
                "message": f"基准 {spec.benchmark} 不可用，超额指标未计算：{exc}",
            })
    elif not spec.benchmark:
        data_quality["benchmark_status"] = "not_requested"
    return benchmark_close, required


def execute_backtest(
    spec: BacktestSpec,
    *,
    progress: Callable[[int, str, str], None] = lambda *_args: None,
    cancelled: Callable[[], bool] = lambda: False,
    panel: dict[str, pd.DataFrame] | None = None,
    membership: pd.DataFrame | None = None,
    benchmark_close: pd.Series | None = None,
    artifact_id: str = "",
    artifact_name: str = "",
) -> dict[str, dict[str, Any]]:
    """Run one backtest without choosing its CLI or persisted-job lifecycle."""
    from quantmaster.backtest.engine import BacktestConfig, run_backtest
    from quantmaster.backtest.quality import assess_panel_quality
    from quantmaster.backtest.report import full_report
    from quantmaster.backtest.spec import pin_decision_strategy, preflight_strategy
    from quantmaster.lab.dataset import create_snapshot
    from quantmaster.runtime.problems import OperationProblem, make_problem

    preflight_strategy(spec)
    strategy_spec = pin_decision_strategy(spec.strategy, spec.universe)
    if strategy_spec is not spec.strategy:
        spec = spec.model_copy(update={"strategy": strategy_spec})

    end = _backtest_window(spec)
    warnings: list[dict[str, Any]] = []
    resolved_tier = (
        "production" if spec.research_tier == "auto" and spec.universe.lower() == "csi800"
        else "sandbox" if spec.research_tier == "auto" else spec.research_tier
    )

    def checkpoint(value: int, phase: str, detail: str = "") -> None:
        if cancelled():
            raise InterruptedError("用户取消回测")
        progress(value, phase, detail)

    checkpoint(5, "准备候选", "解析固定候选或历史成分快照")
    (
        panel, membership, symbols, universe_quality, research_manifest,
        market_quality, provided_panel,
    ) = _backtest_inputs(
        spec, end, panel, membership, resolved_tier, warnings, checkpoint,
    )
    quality_symbols = list(panel.get("close", pd.DataFrame()).columns) if provided_panel else symbols
    data_quality, panel_warnings = assess_panel_quality(
        panel,
        quality_symbols,
        minimum_symbols=spec.strategy.top_n,
        allow_partial=spec.allow_partial,
    )
    if market_quality is not None:
        data_quality["market_contract"] = market_quality.to_dict()
        if market_quality.status == "degraded":
            data_quality["status"] = "partial"
            warnings.append({
                "code": "market_data_degraded", "level": "warning",
                "message": "行情证据已降级：" + "；".join(market_quality.issues),
            })
    warnings.extend(panel_warnings)
    close = panel["close"]
    symbols = list(close.columns)

    strategy, weights, signal_bundle = _backtest_signal(
        spec, panel, membership, end, resolved_tier, data_quality, warnings, checkpoint,
    )

    benchmark_close, benchmark_required = _backtest_benchmark(
        spec, end, benchmark_close, data_quality, warnings,
    )

    checkpoint(60, "模拟成交", "按 T 日收盘信号、T+1 日开盘与 A 股费用规则撮合")
    result = run_backtest(
        panel,
        weights,
        BacktestConfig(
            initial_capital=spec.initial_capital,
            stop_loss=spec.stop_loss,
            take_profit=spec.take_profit,
            research_tier=resolved_tier,
        ),
        benchmark_close=benchmark_close,
    )
    if not result.trades and not (
        signal_bundle.intentional_flat is not None
        and signal_bundle.intentional_flat.fillna(False).any()
        and not weights.gt(0).to_numpy().any()
    ):
        data_quality["status"] = "blocked"
        raise OperationProblem(
            422,
            make_problem(
                "no_valid_trades",
                source="策略回测",
                title="回测没有产生有效成交",
                message="所有信号均未形成可验证成交，不能把空净值曲线当作有效回测结果。",
                action="检查成交日价格、涨跌停限制、资金规模和策略信号后重试。",
                blocking=True,
                problem_id="backtest:no-valid-trades",
            ),
            data_quality=data_quality,
        )
    checkpoint(82, "生成报告", "汇总净值、回撤、成交与分期绩效")
    report = full_report(result)
    drawdown = result.nav / result.nav.cummax() - 1.0
    exposure = result.positions.sum(axis=1) / (result.nav * spec.initial_capital)
    snapshot = create_snapshot(
        spec.universe, spec.start, end, panel=panel, membership=membership,
    ).to_dict()
    trade_config = asdict(get_config().trade)
    formal_eligible, eligibility_reasons = _formal_eligibility(
        spec,
        resolved_tier=resolved_tier,
        universe_quality=universe_quality,
        data_quality=data_quality,
        research_manifest=research_manifest,
        benchmark_required=benchmark_required,
        warnings=warnings,
    )
    manifest = {
        "app_version": __version__,
        "config_hash": spec.snapshot_hash,
        "strategy_name": strategy.name,
        "strategy_snapshot": spec.strategy.model_dump(mode="json"),
        "universe": spec.universe,
        "universe_quality": universe_quality,
        "research_tier": resolved_tier,
        "formal_eligible": formal_eligible,
        "eligibility_reasons": eligibility_reasons,
        "date_range": {"requested": [spec.start, end], "actual": [
            pd.Timestamp(close.index.min()).strftime("%Y-%m-%d"),
            pd.Timestamp(close.index.max()).strftime("%Y-%m-%d"),
        ]},
        "symbol_count": len(symbols),
        "benchmark": spec.benchmark or "",
        "execution": "T close signal -> T+1 open execution",
        "trade_config": trade_config,
        "dataset": snapshot,
        "research_data": research_manifest,
        "data_quality": data_quality,
        "warnings": warnings,
    }
    position_history = [
        {
            "date": pd.Timestamp(date).strftime("%Y-%m-%d"),
            "values": {
                str(symbol): round(float(value), 2)
                for symbol, value in row.items() if float(value) > 0
            },
        }
        for date, row in result.positions.iterrows()
    ]
    artifact = {
        "id": artifact_id,
        "name": artifact_name or spec.name,
        "config": spec.model_dump(mode="json"),
        "manifest": manifest,
        "metrics": report["metrics"],
        "nav": _points(result.nav),
        "benchmark_nav": _points(result.benchmark_nav),
        "drawdown": _points(drawdown),
        "exposure": _points(exposure),
        "positions": position_history,
        "trades": [asdict(trade) for trade in result.trades],
        "blocked_orders": [asdict(order) for order in result.blocked_orders],
        "yearly": report["yearly"],
        "monthly": report["monthly"],
        "trade_stats": report["trade_stats"],
        "risk_diagnostics": report.get("risk_diagnostics", {}),
        "stress_tests": report.get("stress_tests", []),
        "trade_lifecycle": report.get("trade_lifecycle", {}),
        "attribution": {
            name: _points(
                values.reindex_like(weights).mul(weights.fillna(0.0)).sum(axis=1)
            )
            for name, values in signal_bundle.contributions.items()
        },
        "signal_metadata": signal_bundle.metadata,
    }
    summary = {
        "strategy": strategy.name,
        "metrics": report["metrics"],
        "trade_stats": report["trade_stats"],
        "warnings": warnings,
        "data_quality": data_quality,
        "research_tier": resolved_tier,
        "formal_eligible": formal_eligible,
        "nav_points": len(artifact["nav"]),
        "trade_count": len(result.trades),
        "blocked_order_count": len(result.blocked_orders),
    }
    checkpoint(96, "保存结果", "原子写入可复现实验产物")
    return {"manifest": manifest, "summary": summary, "artifact": artifact}
