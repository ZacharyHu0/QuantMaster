"""面向前端的统一问题协议与健康状态聚合。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from quantmaster.backtest.quality import (
    assess_panel_quality as assess_panel_quality,
)
from quantmaster.backtest.quality import (
    assess_signal_quality as assess_signal_quality,
)
from quantmaster.logging_config import redact_sensitive_text
from quantmaster.runtime.problems import OperationProblem as OperationProblem
from quantmaster.runtime.problems import Problem, make_problem


def _clean(value: object, limit: int = 300) -> str:
    text = redact_sensitive_text(str(value or "").strip())
    return text if len(text) <= limit else f"{text[: limit - 1]}…"


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
