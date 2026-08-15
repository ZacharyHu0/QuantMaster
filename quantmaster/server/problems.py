"""面向前端的统一问题协议与健康状态聚合。"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from quantmaster.backtest.quality import (
    assess_panel_quality as assess_panel_quality,
)
from quantmaster.backtest.quality import (
    assess_signal_quality as assess_signal_quality,
)
from quantmaster.logging_config import redact_sensitive_text
from quantmaster.rotation.status import canonical_provider_status
from quantmaster.runtime.problems import OperationProblem as OperationProblem
from quantmaster.runtime.problems import Problem, make_problem

logger = logging.getLogger(__name__)


def _clean(value: object, limit: int = 300) -> str:
    text = redact_sensitive_text(str(value or "").strip())
    return text if len(text) <= limit else f"{text[: limit - 1]}…"


_PROVIDER_NAMES = {
    "free-stockdb": "本地 StockDB",
    "tushare": "Tushare",
    "akshare": "AKShare",
    "yfinance": "Yahoo Finance",
    "yahoo": "Yahoo Finance",
    "ths": "同花顺",
}


def _llm_action(category: str, *, retryable: bool, retry_status: str) -> str:
    actions = {
        "dns": "检查域名、DNS 和网络代理设置后重试。",
        "tcp": "确认模型服务监听地址和端口，检查防火墙后重试。",
        "tls": "检查 HTTPS 地址、证书链和系统时间；不要关闭证书验证。",
        "timeout": "检查服务负载与超时配置；当前请求可在退避后重试。",
        "authentication": "检查 API Key 是否有效；不要在此页面粘贴或记录密钥。",
        "authorization": "确认该密钥拥有当前模型或接口的访问权限。",
        "model_or_endpoint": "检查脱敏端点、API 协议和模型 ID 是否存在。",
        "request_contract": "网关不接受当前请求合同；检查模型协议和参数兼容性。",
        "rate_limit": "等待额度或限流窗口恢复，并降低并发或请求频率。",
        "upstream_gateway": "上游网关暂时异常；稍后重试或查看网关运行状态。",
        "response_contract": "上游响应不符合模型协议；检查网关兼容性。",
    }
    fallback = "查看本机服务日志并使用诊断请求码检索该次失败。"
    if retryable and retry_status:
        return f"{actions.get(category, fallback)} {retry_status}。"
    return actions.get(category, fallback)


def _llm_title(category: str) -> str:
    labels = {
        "dns": "模型服务 DNS 解析失败", "tcp": "模型服务连接被拒绝",
        "tls": "模型服务 TLS 证书失败", "timeout": "模型服务请求超时",
        "authentication": "模型服务鉴权失败", "authorization": "模型服务无访问权限",
        "model_or_endpoint": "模型或接口不存在", "request_contract": "模型请求合同不兼容",
        "rate_limit": "模型服务限流或额度不足", "upstream_gateway": "模型上游网关故障",
        "response_contract": "模型服务响应合同错误",
    }
    return labels.get(category, "模型服务请求失败")
_CAPABILITY_NAMES = {
    "index-cons": "指数成分",
    "csindex": "指数成分",
    "index_weight": "指数成分权重",
    "etf_basic": "基金目录",
    "eastmoney-spot": "实时行情",
}


def _provider_name(lane: object) -> str:
    raw = str(lane or "").strip()
    provider, _, capability = raw.partition(":")
    name = _PROVIDER_NAMES.get(provider.casefold(), provider or "行情数据源")
    purpose = _CAPABILITY_NAMES.get(capability.casefold(), "")
    return f"{name}（{purpose}）" if purpose else name


def _provider_failure_message(failure_class: str, *, disabled: bool) -> str:
    messages = {
        "permission_missing": "当前账号没有读取这项数据的权限。",
        "auth_invalid": "数据源的账号或密钥未通过验证。",
        "rate_limited": "在线数据源请求过于频繁，系统已暂停继续请求。",
        "capability_missing": "当前数据源不支持这项数据，系统已停止重复请求。",
        "network": "暂时无法连接在线数据源，系统将继续使用本地数据。",
        "5xx": "在线数据源暂时不可用，系统将继续使用本地数据。",
        "contract_changed": "在线数据合同已变化，系统已停止重复提交不兼容请求。",
    }
    return messages.get(
        failure_class,
        "数据源连续请求失败，系统已停止重复请求。"
        if disabled else "数据源暂时不可用，系统已暂停请求并保留本地数据。",
    )


def _component_failure(name: str) -> Problem:
    logger.warning("后台健康探针失败 component=%s", name, exc_info=True)
    return make_problem(
        "health_probe_failed",
        severity="warning",
        source="后台状态",
        title=f"{name}状态暂不可用",
        message="状态读取失败，请查看本机日志",
        action="稍后刷新后台状态；如持续出现，请查看服务端日志。",
        problem_id=f"health:{name}:probe",
    )


def collect_health_report() -> dict[str, Any]:
    """聚合全局后台健康与当前任务；任一子系统失败不影响其余状态。"""
    issues: list[Problem] = []

    # LLM issues are informational health projections only: unrelated jobs
    # remain visible and runnable even while a provider is degraded.
    try:
        from quantmaster.ai.llm import llm_provider_health

        for item in llm_provider_health():
            if item.get("status") not in {"degraded", "healthy"}:
                continue
            category = str(item.get("error_category") or "unknown")
            provider = _clean(item.get("provider") or "LLM", 40)
            model = _clean(item.get("model") or "未指定模型", 120)
            retry_status = _clean(item.get("retry_status") or "", 80)
            healthy = item.get("status") == "healthy"
            issues.append(make_problem(
                str(item.get("error_code") or "llm_healthy"), severity="info" if healthy else "warning",
                source="模型服务", title="模型服务运行正常" if healthy else _llm_title(category),
                message=(f"{provider} · {model}：最近请求成功" if healthy else
                         f"{provider} · {model}：{_clean(item.get('message') or '请求失败')}"),
                action="LLM 故障不会阻断不依赖模型的后台任务。" if healthy else _llm_action(
                    category, retryable=bool(item.get("retry_after_seconds")), retry_status=retry_status,
                ),
                problem_id=f"llm:{provider}:{model}", can_continue=True,
                provider=provider, endpoint=_clean(item.get("endpoint"), 220), model=model,
                occurred_at=item.get("occurred_at"), last_success_at=item.get("last_success_at"),
                diagnostic_id=_clean(item.get("last_request_id"), 80),
                error_category=category, http_status=item.get("http_status"),
                retry_status=retry_status, next_retry_at=item.get("next_retry_at"),
                response_summary=_clean(item.get("response_summary"), 180),
            ))
    except (ImportError, OSError, RuntimeError, TypeError, ValueError):
        issues.append(_component_failure("模型服务"))

    try:
        from quantmaster.data.resilience import PROVIDER_HEALTH

        for lane, item in PROVIDER_HEALTH.status().items():
            state = item.get("state", "closed")
            if state == "closed":
                continue
            remaining = max(0, int(float(item.get("open_until") or 0) - datetime.now().timestamp()))
            disabled = state == "disabled"
            raw_failure_class = str(item.get("failure_class") or "transient_upstream")
            failure_class = canonical_provider_status(raw_failure_class)
            provider_name = _provider_name(lane)
            diagnostic_code = _clean(item.get("diagnostic_code") or raw_failure_class, 80)
            issues.append(make_problem(
                "provider_disabled" if disabled else "provider_circuit_open",
                # Provider health is an independent operational projection.
                # The consuming data view decides whether missing data is a
                # warning/error; an upstream-only issue remains informational.
                severity="info",
                source="行情数据源",
                title=f"{provider_name}{'已停止自动请求' if disabled else '已暂停请求'}",
                message=_provider_failure_message(failure_class, disabled=disabled),
                action=(
                    "更新对应凭据或依赖后，在后台诊断中执行一次手工探测。"
                    if disabled else (
                        f"约 {remaining} 秒后系统会自动探测恢复。" if remaining
                        else "下一次相关请求会执行受控恢复探测。"
                    )
                ),
                problem_id=f"provider:{lane}",
                state=state,
                failure_class=failure_class,
                provider_status=failure_class,
                capability=str(lane).partition(":")[2] or str(lane),
                diagnostic_code=diagnostic_code,
                diagnostic_id=f"provider:{lane}:{diagnostic_code}",
                last_success=float(item.get("last_success") or 0),
                last_failure=float(item.get("last_failure") or 0),
                next_probe_at=float(item.get("next_probe_at") or 0),
                retry_after_at=float(item.get("retry_after_at") or 0),
                remote_failures=int(item.get("failures") or 0),
                local_blocks=int(item.get("suppressed") or 0),
                can_continue=True,
            ))
    except Exception:
        issues.append(_component_failure("行情数据源"))

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
    except Exception:
        issues.append(_component_failure("数据刷新"))

    try:
        from quantmaster.data.repair import get_data_repair_manager

        repairs = get_data_repair_manager().list(limit=200)
        failed_repairs = [item for item in repairs if item.get("status") == "failed"]
        pending_repairs = [
            item for item in repairs
            if item.get("status") in {"queued", "running", "cancelling"}
        ]
        if failed_repairs:
            issues.append(make_problem(
                "data_repair_failed",
                severity="warning",
                source="数据修复",
                title="部分数据修复已耗尽自动重试",
                message=f"{len(failed_repairs)} 个可重建数据目标仍未恢复。",
                action="在统一任务列表选择 repairs 查看错误并人工重试。",
                problem_id="data-repair:failed",
            ))
        elif pending_repairs:
            issues.append(make_problem(
                "data_repair_pending",
                severity="info",
                source="数据修复",
                title="数据完整性修复正在排队",
                message=f"{len(pending_repairs)} 个目标将按来源额度与退避策略修复。",
                action="系统会保留隔离原件并自动校验替换结果。",
                problem_id="data-repair:pending",
            ))
    except Exception:
        issues.append(_component_failure("数据修复"))

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
    except Exception:
        issues.append(_component_failure("资讯来源"))

    try:
        from quantmaster.ai.llm import web_search_capability_status

        search = web_search_capability_status()
        if search.get("supported") is False:
            issues.append(make_problem(
                "llm_web_search_unavailable",
                severity="warning",
                source="个股深度分析",
                title="模型网关不支持原生联网搜索",
                message=_clean(search.get("detail")) or "能力探测已确认当前网关不支持 Web Search。",
                action="系统会自动使用内置金融数据源；如需联网补证，可切换支持原生搜索的模型接口。",
                problem_id="stock-analysis:web-search",
            ))
    except (ImportError, RuntimeError, TypeError, ValueError):
        issues.append(_component_failure("个股联网搜索"))

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
    except Exception:
        issues.append(_component_failure("自动任务"))

    try:
        from quantmaster.config import get_config
        from quantmaster.lab.jobs import list_lab_jobs
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
            latest_jobs = list_lab_jobs(1)
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
            publications = LabStore().pending_publications(100, due_only=False)
            if publications:
                issues.append(make_problem(
                    "lab_publication_pending",
                    severity="warning",
                    source="Quant Lab",
                    title="模型训练已完成，数据发布仍在重试",
                    message=f"{len(publications)} 个模型预测 outbox 尚未发布完成。",
                    action="Lab worker 会幂等重试；训练版本与验证证据不受影响。",
                    problem_id="lab:publication-pending",
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
    except Exception:
        issues.append(_component_failure("Quant Lab"))

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
    except Exception:
        issues.append(_component_failure("策略回测"))

    level = "error" if any(item["severity"] == "error" for item in issues) else (
        "warning" if any(item["severity"] == "warning" for item in issues) else "ok"
    )
    return {
        "level": level,
        "checked_at": datetime.now(UTC).isoformat(),
        "issues": issues,
    }
