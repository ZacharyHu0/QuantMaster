"""Worker-owned settings application, independent of HTTP router construction."""

from __future__ import annotations

import logging
from collections.abc import Callable
from functools import partial
from typing import Any

from quantmaster.config import Config, get_config
from quantmaster.credentials import CredentialError
from quantmaster.server.settings_control import (
    SettingsApplyPending,
    register_settings_control,
    settings_manager,
)
from quantmaster.server.settings_runtime import public_state

logger = logging.getLogger(__name__)


def initialize_worker_settings(
    manager: Any, on_applied: Callable[[int, int], None] | None = None,
) -> None:
    """Wire durable settings handlers before the worker starts consuming jobs."""
    register_settings_control(manager, partial(_apply_runtime, on_applied=on_applied))


def _apply_free_stockdb(changed: list[str], result: dict[str, Any]) -> dict[str, Any]:
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    if not any(field.startswith("data.free_stockdb_") for field in changed):
        return {"status": "unchanged"}
    try:
        # This runs off the Web request path. Keep waiting on the same command
        # for a late owner acknowledgement, within the durable job's deadline.
        return free_stockdb_runtime.request_apply_config(changed, timeout=60.0)
    except (OSError, RuntimeError, ValueError):
        logger.warning("free-stockdb 托管运行时热应用失败", exc_info=True)
        result.setdefault("warnings", []).append("free-stockdb 设置已保存，但运行时热应用失败")
        return {"status": "degraded", "message": "托管运行时应用失败，请重启服务"}


def _report_apply_component(
    component: str, status: dict[str, Any], *, revision: int, generation: int,
    diagnostic_code: str, recommendation: str,
) -> None:
    from quantmaster.server.settings_runtime import report_component

    degraded = str(status.get("status") or "unchanged") == "degraded"
    report_component(
        settings_manager().path, component, revision=revision, generation=generation,
        status="failed" if degraded else "effective",
        error=str(status.get("message") or "") if degraded else "",
        diagnostic_code=diagnostic_code if degraded else "",
        recommendation=recommendation if degraded else "",
    )


def _apply_worker_components(
    changed: list[str], result: dict[str, Any], apply_status: dict[str, Any],
    *, revision: int, generation: int,
) -> None:
    from quantmaster.lab.worker import get_worker

    try:
        if "data.root" in changed:
            from quantmaster.automation.runtime import get_runtime

            active = get_runtime().start() if get_config().automation.enabled else False
            apply_status["automation"] = {
                "status": "applied" if active else "disabled"
                if not get_config().automation.enabled else "standby"
            }
        elif any(field.startswith("automation.") for field in changed):
            from quantmaster.automation.runtime import get_runtime

            apply_status["automation"] = get_runtime().apply_config(changed)
    except Exception:
        logger.warning("自动化运行时热应用失败", exc_info=True)
        apply_status["automation"] = {"status": "degraded", "message": "运行时热应用失败，请重启服务"}
        result.setdefault("warnings", []).append("自动化配置已保存，但运行时热应用失败")
    _report_apply_component(
        "automation", apply_status["automation"], revision=revision, generation=generation,
        diagnostic_code="automation_apply_failed", recommendation="重试应用；持续失败时重启服务",
    )
    try:
        if "data.root" in changed:
            worker = get_worker()
            if get_config().lab.enabled:
                worker.start()
                apply_status["lab"] = {"status": "applied"}
            else:
                apply_status["lab"] = {"status": "disabled"}
        elif any(field.startswith("lab.") for field in changed) or "automation.timezone" in changed:
            apply_status["lab"] = get_worker().apply_config(changed)
    except Exception:
        logger.warning("Quant Lab Worker 热应用失败", exc_info=True)
        apply_status["lab"] = {"status": "degraded", "message": "Worker 热应用失败，请重启服务"}
        result.setdefault("warnings", []).append("Quant Lab 配置已保存，但 Worker 热应用失败")
    _report_apply_component(
        "lab", apply_status["lab"], revision=revision, generation=generation,
        diagnostic_code="lab_apply_failed", recommendation="重试应用；持续失败时重启服务",
    )


def _report_remaining_components(
    llm_probe: dict[str, Any] | None, apply_status: dict[str, Any],
    *, revision: int, generation: int,
) -> None:
    from quantmaster.server.settings_runtime import report_component

    if llm_probe and str(llm_probe.get("status")) == "error":
        previous = public_state(settings_manager().path).get("components", {}).get("llm", {})
        report_component(
            settings_manager().path, "llm", revision=revision, generation=generation,
            status="failed", effective_revision=int(previous.get("effective_revision") or 0),
            error=str(llm_probe.get("message") or llm_probe.get("error") or "candidate probe failed"),
            diagnostic_code=str(llm_probe.get("diagnostic_id") or "llm_probe_failed"),
            recommendation="修改 provider/凭据后重试应用，或回滚已保存版本",
        )
        apply_status["llm"] = {
            "status": "degraded", "message": llm_probe.get("message"),
            "diagnostic_id": llm_probe.get("diagnostic_id"),
        }
    else:
        report_component(
            settings_manager().path, "llm", revision=revision,
            generation=generation, status="effective",
        )
    for component in ("data-clients", "scheduler"):
        report_component(
            settings_manager().path, component, revision=revision,
            generation=generation, status="effective",
        )


def _probe_candidate_llm(
    changed: list[str], candidate_runtime_config: Config, previous_runtime_config: Config,
) -> dict[str, Any] | None:
    llm_probe: dict[str, Any] | None = None
    if any(field.startswith("llm.") for field in changed):
        try:
            from quantmaster.server.settings_checks import list_llm_models
            from quantmaster.settings import document_from_config

            llm_probe = list_llm_models(
                document_from_config(candidate_runtime_config).llm,
                candidate_runtime_config.llm.api_key,
                isolated=True,
            )
            if str(llm_probe.get("status") or "warning") == "error":
                candidate_runtime_config.llm = previous_runtime_config.llm
        except (CredentialError, OSError, RuntimeError, TypeError, ValueError) as exc:
            logger.warning("LLM candidate probe failed", exc_info=True)
            llm_probe = {
                "status": "error",
                "message": "LLM 临时 client 探测失败",
                "diagnostic_id": "llm_probe_failed",
                "error": str(exc),
            }
            candidate_runtime_config.llm = previous_runtime_config.llm
    return llm_probe


def _apply_runtime(
    result: dict[str, Any], *, on_applied: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    """按变更字段热应用进程内服务；配置落盘成功不因联网状态回滚。"""
    from quantmaster.config import get_config as current_config
    from quantmaster.config import set_config
    from quantmaster.server.settings_runtime import report_component

    changed = list(result.get("changed_fields") or [])
    revision = int(result.get("config_revision") or 0)
    generation = int(result.get("generation") or 0)
    latest = int(settings_manager().public().get("config_revision") or 0)
    if revision < latest:
        result["apply_status"] = {"config": {"status": "superseded"}}
        result["runtime"] = public_state(settings_manager().path)
        return result
    # Runtime-worker owns its own process-local snapshot.  Reconcile from the
    # persisted source before applying components; never use the stale config
    # captured at worker startup.
    previous_runtime_config = current_config()
    candidate_runtime_config = settings_manager().load()
    llm_probe = _probe_candidate_llm(changed, candidate_runtime_config, previous_runtime_config)
    set_config(candidate_runtime_config)
    if on_applied is not None:
        on_applied(revision, generation)
    report_component(
        settings_manager().path, "runtime-worker", revision=revision,
        generation=generation, status="effective",
    )
    if result.get("restart_required"):
        server_previous = public_state(settings_manager().path).get("components", {}).get("server", {})
        report_component(
            settings_manager().path, "server", revision=revision,
            generation=generation, status="restart_required",
            effective_revision=int(server_previous.get("effective_revision") or 0),
            recommendation="重启 QuantMaster 后应用监听地址变更",
        )
    else:
        report_component(
            settings_manager().path, "server", revision=revision,
            generation=generation, status="effective",
        )
    apply_status: dict[str, Any] = {
        "config": {"status": "applied"},
        "automation": {"status": "unchanged"},
        "lab": {"status": "unchanged"},
        "server": {"status": "restart_required" if result.get("restart_required") else "applied"},
    }
    apply_status["free_stockdb"] = _apply_free_stockdb(changed, result)
    _apply_worker_components(
        changed, result, apply_status, revision=revision, generation=generation,
    )
    if "data.root" in changed:
        try:
            from quantmaster.backtest.jobs import get_backtest_job_manager
            from quantmaster.backtest.paper_automation import get_paper_automation_worker
            from quantmaster.data.maintenance import data_refresh_manager
            from quantmaster.research.jobs import get_research_job_manager

            data_refresh_manager.start()
            get_research_job_manager().start()
            get_backtest_job_manager().start()
            get_paper_automation_worker().start()
            apply_status["data_workers"] = {"status": "applied"}
        except Exception:
            logger.warning("数据目录切换后后台执行器恢复失败", exc_info=True)
            apply_status["data_workers"] = {
                "status": "degraded",
                "message": "后台执行器恢复失败，请重启服务",
            }
            result.setdefault("warnings", []).append("数据目录已切换，但部分后台执行器需要重启服务后恢复")
    stock_status = str(apply_status["free_stockdb"].get("status") or "unchanged")
    stock_failed = stock_status not in {"applied", "disabled", "unchanged"}
    report_component(
        settings_manager().path, "free-stockdb", revision=revision, generation=generation,
        status="pending" if stock_status == "queued" else "failed" if stock_failed else "effective",
        error=str(apply_status["free_stockdb"].get("message") or "") if stock_failed else "",
        diagnostic_code="free_stockdb_apply_unconfirmed" if stock_failed else "",
        recommendation="重试应用，等待托管进程确认" if stock_failed else "",
    )
    _report_remaining_components(
        llm_probe, apply_status, revision=revision, generation=generation,
    )
    result["apply_status"] = apply_status
    result["runtime"] = public_state(settings_manager().path)
    if stock_status == "queued":
        raise SettingsApplyPending("等待托管进程确认设置应用")
    if stock_failed or any(
        item.get("status") == "degraded" for item in apply_status.values()
    ):
        raise RuntimeError("设置已保存，但部分组件尚未确认应用；请重试应用")
    return result
