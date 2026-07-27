from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, SecretStr

from quantmaster.automation.runtime import get_runtime
from quantmaster.server.management import _require_csrf, _require_local

router = APIRouter()


def service():
    return get_runtime().service


def _error(exc: Exception) -> HTTPException:
    if isinstance(exc, PermissionError):
        return HTTPException(403, str(exc))
    if isinstance(exc, KeyError):
        return HTTPException(404, str(exc).strip("'"))
    return HTTPException(400, str(exc))


class PolicyIn(BaseModel):
    preset: Literal["conservative", "balanced", "sensitive"] = "balanced"
    overrides: dict[str, Any] = Field(default_factory=dict)
    enabled: bool | None = None


class ScheduleIn(BaseModel):
    action: Literal["pause", "resume", "reschedule"]
    schedule: dict[str, Any] | None = None


class FeishuConfigIn(BaseModel):
    app_id: str = Field(..., min_length=3, max_length=200)
    app_secret: SecretStr


@router.get("/api/automation/overview")
def automation_overview(request: Request) -> dict:
    _require_local(request)
    runtime = get_runtime()
    result = runtime.service.overview()
    result["runtime"] = (
        "running" if runtime.leader else "standby" if result["enabled"] else "disabled"
    )
    return result


@router.get("/api/automation/targets")
def automation_targets(request: Request) -> dict:
    _require_local(request)
    return {"targets": service().public_targets()}


@router.get("/api/automation/jobs")
def automation_jobs(request: Request) -> dict:
    _require_local(request)
    return {"jobs": service().store.jobs(), "runs": service().store.recent_runs(50)}


@router.get("/api/automation/audit")
def automation_audit(request: Request, limit: int = 100) -> dict:
    _require_local(request)
    return {"items": service().store.audit_entries(max(1, min(limit, 500)))}


@router.get("/api/automation/events")
def automation_events(request: Request, limit: int = 100) -> dict:
    _require_local(request)
    return {"items": service().store.recent_events(max(1, min(limit, 500)))}


@router.post("/api/automation/bindings/code")
def automation_binding_code(target_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return service().store.create_binding_code(target_id)
    except Exception as exc:
        raise _error(exc) from exc


@router.patch("/api/automation/targets/{target_id}/policy")
def automation_policy(target_id: str, value: PolicyIn, request: Request) -> dict:
    _require_csrf(request)
    try:
        target = service().update_policy(
            target_id, value.preset, value.overrides, value.enabled, "web")
        return service()._public_target(target)
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/api/automation/targets/{target_id}/test")
def automation_target_test(target_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return service().test_target(target_id)
    except Exception as exc:
        raise _error(exc) from exc


@router.patch("/api/automation/jobs/{name}")
def automation_job_update(name: str, value: ScheduleIn, request: Request) -> dict:
    _require_csrf(request)
    try:
        result = service().update_schedule(
            name, action=value.action, schedule=value.schedule, actor="web")
        get_runtime().reload_jobs()
        return result
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/api/automation/jobs/{name}/run")
def automation_job_run(name: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return service().run_task(name, actor="web")
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/api/automation/channels/weixin/login")
def weixin_login_start(request: Request) -> dict:
    """向腾讯微信 ClawBot iLink 接口申请扫码登录二维码。"""
    _require_csrf(request)
    try:
        return service().start_weixin_login()
    except Exception as exc:
        raise _error(exc) from exc


@router.get("/api/automation/channels/weixin/login/{session_id}")
def weixin_login_poll(session_id: str, request: Request, verify_code: str = "") -> dict:
    _require_csrf(request)
    try:
        result = service().poll_weixin_login(session_id, verify_code)
        if result.get("status") == "confirmed":
            get_runtime().start_channels()
        return result
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/api/automation/channels/feishu/config")
def feishu_config(value: FeishuConfigIn, request: Request) -> dict:
    """保存飞书企业自建应用 Bot 凭据；App Secret 只进入系统凭据库。"""
    _require_csrf(request)
    try:
        current = get_runtime()._channel_threads.get("feishu")
        restart_required = bool(current and current.is_alive())
        result = service().configure_feishu(value.app_id, value.app_secret.get_secret_value())
        if not restart_required:
            get_runtime().start_channels()
        return {**result, "restart_required": restart_required}
    except Exception as exc:
        raise _error(exc) from exc
