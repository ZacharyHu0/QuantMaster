"""面向前端的统一问题协议、健康聚合与研究数据质量门禁。"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from quantmaster.logging_config import redact_sensitive_text

Problem = dict[str, Any]


def _clean(value: object, limit: int = 300) -> str:
    text = redact_sensitive_text(str(value or "").strip())
    return text if len(text) <= limit else f"{text[: limit - 1]}…"


def make_problem(
    code: str,
    *,
    severity: str = "error",
    source: str = "本地服务",
    title: str,
    message: str,
    action: str,
    blocking: bool = False,
    can_continue: bool = False,
    problem_id: str | None = None,
    items: list[object] | None = None,
    **context: object,
) -> Problem:
    """构造可稳定去重、可直接展示且不泄露敏感信息的问题对象。"""
    safe_severity = severity if severity in {"info", "warning", "error"} else "error"
    problem: Problem = {
        "id": problem_id or f"{source}:{code}",
        "code": _clean(code, 80),
        "severity": safe_severity,
        "source": _clean(source, 60),
        "title": _clean(title, 120),
        "message": _clean(message),
        "action": _clean(action),
        "blocking": bool(blocking),
        "can_continue": bool(can_continue),
    }
    if items:
        problem["items"] = [_clean(item, 100) for item in items[:20]]
    for key, value in context.items():
        if value is not None:
            problem[key] = value
    revision_payload = {
        key: value for key, value in problem.items()
        if key not in {"revision", "checked_at"}
    }
    problem["revision"] = hashlib.sha256(
        json.dumps(revision_payload, ensure_ascii=False, sort_keys=True, default=str).encode()
    ).hexdigest()[:12]
    return problem


class OperationProblem(Exception):
    """带 HTTP 状态和恢复语义的业务问题。"""

    def __init__(
        self,
        status_code: int,
        problem: Problem,
        *,
        data_quality: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(problem.get("message") or problem.get("title") or "操作未完成")
        self.status_code = status_code
        self.problem = problem
        self.data_quality = data_quality

    def response(self, error_id: str) -> dict[str, Any]:
        result: dict[str, Any] = {
            "detail": self.problem["message"],
            "problem": self.problem,
            "error_id": error_id,
        }
        if self.data_quality is not None:
            result["data_quality"] = self.data_quality
        return result


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
        code,
        severity="warning" if can_continue else "error",
        source="策略回测",
        title=title,
        message=message,
        action=action,
        blocking=blocking,
        can_continue=can_continue,
        problem_id=f"backtest:{code}",
        items=items,
    )


def _raise_quality(
    status_code: int,
    problem: Problem,
    quality: dict[str, Any],
) -> None:
    quality["status"] = "needs_confirmation" if problem["can_continue"] else "blocked"
    raise OperationProblem(status_code, problem, data_quality=quality)


def assess_panel_quality(
    panel: dict[str, pd.DataFrame],
    requested_symbols: list[str],
    *,
    minimum_symbols: int,
    allow_partial: bool,
) -> tuple[dict[str, Any], list[Problem]]:
    """检查价格面板能否支撑回测，并区分阻断与可确认的部分数据。"""
    requested = list(dict.fromkeys(requested_symbols))
    quality: dict[str, Any] = {
        "status": "complete",
        "requested_symbol_count": len(requested),
        "usable_symbol_count": 0,
        "missing_symbol_count": len(requested),
        "missing_symbols": requested[:20],
        "trading_days": 0,
        "actual_start": None,
        "actual_end": None,
        "valid_signal_dates": 0,
        "executable_signal_dates": 0,
        "selected_signals": 0,
        "executable_signals": 0,
        "benchmark_status": "not_checked",
    }
    warnings: list[Problem] = []

    missing_fields = [field for field in ("open", "close") if field not in panel]
    if missing_fields:
        problem = _quality_problem(
            "missing_price_fields",
            title="缺少回测必需价格",
            message=f"行情数据缺少 {', '.join(missing_fields)} 字段，无法计算真实成交与净值。",
            action="补齐开盘价和收盘价数据后重新回测。",
            blocking=True,
            items=missing_fields,
        )
        _raise_quality(422, problem, quality)

    open_prices = panel["open"].replace([float("inf"), float("-inf")], pd.NA)
    close_prices = panel["close"].replace([float("inf"), float("-inf")], pd.NA)
    common_dates = open_prices.index.intersection(close_prices.index).sort_values()
    open_prices = open_prices.reindex(common_dates)
    close_prices = close_prices.reindex(common_dates)
    common_symbols = open_prices.columns.intersection(close_prices.columns)
    usable: list[str] = []
    for symbol in requested:
        if symbol not in common_symbols:
            continue
        pairs = open_prices[symbol].gt(0) & close_prices[symbol].gt(0)
        if int(pairs.sum()) >= 2:
            usable.append(symbol)
    valid_days = (open_prices.reindex(columns=usable).gt(0) &
                  close_prices.reindex(columns=usable).gt(0)).any(axis=1)
    dates = common_dates[valid_days]
    missing_symbols = [symbol for symbol in requested if symbol not in usable]
    quality.update({
        "usable_symbol_count": len(usable),
        "missing_symbol_count": len(missing_symbols),
        "missing_symbols": missing_symbols[:20],
        "trading_days": len(dates),
        "actual_start": str(dates[0].date()) if len(dates) else None,
        "actual_end": str(dates[-1].date()) if len(dates) else None,
    })

    if len(dates) < 2:
        problem = _quality_problem(
            "insufficient_trading_days",
            title="有效交易日不足",
            message=f"目前只有 {len(dates)} 个有效交易日，无法形成信号后的下一交易日成交。",
            action="扩大回测日期范围或刷新对应行情后重试。",
            blocking=True,
        )
        _raise_quality(422, problem, quality)
    if len(usable) < minimum_symbols:
        problem = _quality_problem(
            "insufficient_usable_symbols",
            title="可用标的不足",
            message=f"策略需要至少 {minimum_symbols} 只标的，目前只有 {len(usable)} 只具备有效开收盘价。",
            action="减少选股数量，或补齐候选标的行情后重新回测。",
            blocking=True,
            items=missing_symbols,
        )
        _raise_quality(422, problem, quality)

    confirm_reasons: list[str] = []
    if missing_symbols:
        confirm_reasons.append(f"{len(missing_symbols)} 只候选缺少可用行情")
    if len(dates) < 20:
        confirm_reasons.append(f"有效区间仅 {len(dates)} 个交易日")
    if confirm_reasons:
        problem = _quality_problem(
            "partial_market_data",
            title="回测数据不完整",
            message="；".join(confirm_reasons) + "。继续会改变实际样本范围和选股结果。",
            action="建议先补齐数据；如已了解偏差，可仅用现有数据继续。",
            blocking=not allow_partial,
            can_continue=True,
            items=missing_symbols,
        )
        if not allow_partial:
            _raise_quality(409, problem, quality)
        warnings.append(problem)
        quality["status"] = "partial"
    return quality, warnings


def assess_signal_quality(
    panel: dict[str, pd.DataFrame],
    weights: pd.DataFrame,
    quality: dict[str, Any],
    *,
    allow_partial: bool,
) -> list[Problem]:
    """验证信号存在且能在下一交易日以有效开盘价执行。"""
    warnings: list[Problem] = []
    close = panel["close"]
    aligned = weights.reindex(index=close.index, columns=close.columns)
    positive = aligned.gt(0) & aligned.notna()
    if int(positive.to_numpy().sum()) == 0:
        problem = _quality_problem(
            "no_finite_signal",
            title="策略没有生成有效信号",
            message="当前数据与参数下没有任何正权重选股信号，继续计算只会得到无意义的空结果。",
            action="检查因子所需历史窗口、表达式和候选范围后重试。",
            blocking=True,
        )
        _raise_quality(422, problem, quality)

    next_open = panel["open"].reindex(index=aligned.index, columns=aligned.columns).shift(-1)
    eligible = positive.copy()
    if len(eligible.index):
        eligible.iloc[-1] = False
    executable = eligible & next_open.gt(0)
    selected_count = int(eligible.to_numpy().sum())
    executable_count = int(executable.to_numpy().sum())
    valid_dates = int(eligible.any(axis=1).sum())
    executable_dates = int(executable.any(axis=1).sum())
    quality.update({
        "valid_signal_dates": valid_dates,
        "executable_signal_dates": executable_dates,
        "selected_signals": selected_count,
        "executable_signals": executable_count,
    })
    if executable_count == 0:
        problem = _quality_problem(
            "no_executable_signal",
            title="信号无法成交",
            message="策略虽生成了选股信号，但下一交易日没有可用开盘价，无法模拟成交。",
            action="补齐信号后交易日的开盘价，或调整回测结束日期后重试。",
            blocking=True,
        )
        _raise_quality(422, problem, quality)
    if executable_count < selected_count:
        missing = selected_count - executable_count
        problem = _quality_problem(
            "partial_execution_prices",
            title="部分信号缺少成交价",
            message=f"{selected_count} 个选股信号中有 {missing} 个缺少下一交易日开盘价，将被跳过。",
            action="建议补齐成交价；如已了解偏差，可跳过这些信号继续。",
            blocking=not allow_partial,
            can_continue=True,
        )
        if not allow_partial:
            _raise_quality(409, problem, quality)
        warnings.append(problem)
        quality["status"] = "partial"
    return warnings


def _component_failure(name: str, exc: Exception) -> Problem:
    return make_problem(
        "health_probe_failed",
        severity="warning",
        source="后台状态",
        title=f"{name}状态暂不可用",
        message=_clean(exc) or "状态读取失败",
        action="稍后刷新后台状态；如持续出现，请查看服务端日志。",
        problem_id=f"health:{name}:probe",
    )


def collect_health_report() -> dict[str, Any]:
    """聚合全局后台健康与当前任务；任一子系统失败不影响其余状态。"""
    issues: list[Problem] = []

    try:
        from quantmaster.data.resilience import PROVIDER_HEALTH

        for lane, item in PROVIDER_HEALTH.status().items():
            state = item.get("state", "closed")
            if state == "closed":
                continue
            remaining = max(0, int(float(item.get("open_until") or 0) - datetime.now().timestamp()))
            issues.append(make_problem(
                "provider_circuit_open",
                severity="warning",
                source="行情数据源",
                title=f"{lane} 暂停请求",
                message=_clean(item.get("last_error")) or f"数据源处于 {state} 状态",
                action=(f"约 {remaining} 秒后系统会自动探测恢复。" if remaining
                        else "系统正在探测恢复，可稍后重试相关操作。"),
                problem_id=f"provider:{lane}",
                state=state,
            ))
    except Exception as exc:
        issues.append(_component_failure("行情数据源", exc))

    try:
        from quantmaster.data.maintenance import data_refresh_manager

        refresh = data_refresh_manager.latest()
        if refresh and refresh.get("status") in {
            "running", "cancelling", "interrupted", "completed_with_errors",
        }:
            status = str(refresh["status"])
            running = status in {"running", "cancelling"}
            failed = int(refresh.get("failed") or len(refresh.get("failures") or []))
            issues.append(make_problem(
                "data_refresh_status",
                severity="info" if running else "warning",
                source="数据刷新",
                title="行情正在增量同步" if running else "最近的数据同步未完整完成",
                message=(
                    f"进度 {refresh.get('progress', 0)}%，"
                    f"当前 {refresh.get('current_symbol') or '准备中'}"
                    if running else f"有 {failed} 个标的刷新失败或任务被中断。"
                ),
                action=("任务会在后台继续，可正常浏览其他页面。" if running
                        else "在设置中心查看失败项并重试。"),
                problem_id=f"refresh:{refresh.get('id', 'latest')}",
                job_status=status,
            ))
    except Exception as exc:
        issues.append(_component_failure("数据刷新", exc))

    try:
        from quantmaster.ai.news_sources import NewsSourceStore

        for source in NewsSourceStore().list(enabled=True):
            if source.get("last_status") != "failed":
                continue
            issues.append(make_problem(
                "news_source_failed",
                severity="warning",
                source="资讯来源",
                title=f"{source.get('name') or '资讯源'} 最近抓取失败",
                message=_clean(source.get("last_error")) or "来源未返回可用内容",
                action="在资讯来源设置中检测该来源，或稍后重新抓取。",
                problem_id=f"news-source:{source.get('id', 'unknown')}",
            ))
    except Exception as exc:
        issues.append(_component_failure("资讯来源", exc))

    try:
        from quantmaster.automation.runtime import get_runtime
        from quantmaster.config import get_config

        if get_config().automation.enabled:
            runtime = get_runtime()
            status = runtime.status()
            if status.get("status") == "degraded":
                issues.append(make_problem(
                    "automation_degraded",
                    severity="error",
                    source="自动任务",
                    title="自动化运行异常",
                    message="调度器或消息通道未按配置运行。",
                    action="打开自动化页面检查调度状态和通道配置。",
                    problem_id="automation:runtime",
                ))
            overview = runtime.service.overview()
            latest = (overview.get("recent_runs") or [None])[0]
            if latest and latest.get("status") == "failed":
                issues.append(make_problem(
                    "automation_run_failed",
                    severity="warning",
                    source="自动任务",
                    title=f"{latest.get('job_name') or '最近任务'}执行失败",
                    message=_clean(latest.get("error")) or "任务未完成",
                    action="在自动化页面查看任务运行记录并重试。",
                    problem_id=f"automation-run:{latest.get('id', 'latest')}",
                ))
    except Exception as exc:
        issues.append(_component_failure("自动任务", exc))

    try:
        from quantmaster.config import get_config
        from quantmaster.lab.store import LabStore
        from quantmaster.lab.worker import get_worker

        if get_config().lab.enabled:
            status = get_worker().status()
            if status.get("status") not in {"running", "draining"}:
                issues.append(make_problem(
                    "lab_worker_unavailable",
                    severity="error",
                    source="Quant Lab",
                    title="研究任务执行器未运行",
                    message="Quant Lab 已启用，但后台执行器当前不可用。",
                    action="重启本地服务；如仍失败，请查看服务端日志。",
                    problem_id="lab:worker",
                ))
            latest_jobs = LabStore().jobs(1)
            latest = latest_jobs[0] if latest_jobs else None
            if latest and latest.get("status") == "failed":
                issues.append(make_problem(
                    "lab_job_failed",
                    severity="warning",
                    source="Quant Lab",
                    title="最近的研究任务失败",
                    message=_clean(latest.get("error")) or "研究任务未完成",
                    action="打开 Quant Lab 查看任务事件并重新运行。",
                    problem_id=f"lab-job:{latest.get('id', 'latest')}",
                ))
            elif latest and latest.get("status") == "completed_with_warnings":
                warnings = (latest.get("result") or {}).get("warnings") or []
                warning = warnings[0] if warnings else {}
                message = warning.get("message") if isinstance(warning, dict) else str(warning)
                issues.append(make_problem(
                    "lab_job_partial",
                    severity="warning",
                    source="Quant Lab",
                    title="最近的研究任务部分完成",
                    message=_clean(message) or "已保留完成部分，仍有研究轮次未完成。",
                    action="打开 Quant Lab 查看已保存结果或按相同参数重新运行。",
                    problem_id=f"lab-job:{latest.get('id', 'latest')}",
                ))
    except Exception as exc:
        issues.append(_component_failure("Quant Lab", exc))

    try:
        from quantmaster.backtest.workbench import get_backtest_worker

        latest_runs = get_backtest_worker().service.store.list(1)
        latest = latest_runs[0] if latest_runs else None
        if latest and latest.get("status") in {"queued", "running", "interrupted"}:
            issues.append(make_problem(
                "backtest_running",
                severity="info",
                source="策略回测",
                title="回测任务正在执行",
                message=(latest.get("detail") or latest.get("phase") or "等待后台执行"),
                action="任务状态已保存在本地，可以继续浏览其他页面。",
                problem_id=f"backtest-run:{latest.get('id', 'latest')}",
            ))
        elif latest and latest.get("status") == "failed":
            stored = (latest.get("result") or {}).get("problem") or {}
            issues.append(make_problem(
                str(stored.get("code") or "backtest_failed"),
                severity=str(stored.get("severity") or "error"),
                source="策略回测",
                title=str(stored.get("title") or "最近的回测任务失败"),
                message=str(stored.get("message") or latest.get("error") or "回测未完成"),
                action=str(stored.get("action") or "打开回测工作台检查数据与参数后重试。"),
                blocking=bool(stored.get("blocking", True)),
                can_continue=bool(stored.get("can_continue", False)),
                problem_id=f"backtest-run:{latest.get('id', 'latest')}",
                items=stored.get("items") if isinstance(stored.get("items"), list) else None,
            ))
    except Exception as exc:
        issues.append(_component_failure("策略回测", exc))

    level = "error" if any(item["severity"] == "error" for item in issues) else (
        "warning" if any(item["severity"] == "warning" for item in issues) else "ok"
    )
    return {
        "level": level,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "issues": issues,
    }
