from __future__ import annotations

import sqlite3
import time
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import Field, SecretStr

from quantmaster.automation.runtime import get_runtime
from quantmaster.automation.store import AutomationStore
from quantmaster.config import get_config
from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.jobs import UnifiedJobStore
from quantmaster.runtime.worker import runtime_worker_status
from quantmaster.server.management import _require_csrf, _require_local

router = APIRouter()
_MISSING = object()


def service():
    return get_runtime().service


def _reader() -> AutomationStore:
    """Open the worker-owned automation ledger without a migration or seed."""

    return AutomationStore(read_only=True)


def _snapshot_unavailable(exc: Exception) -> HTTPException:
    return HTTPException(503, "automation_snapshot_unavailable")


def _read(call, *, default: Any = _MISSING):
    try:
        return call(_reader())
    except (FileNotFoundError, sqlite3.Error) as exc:
        if default is not _MISSING:
            return default() if callable(default) else default
        raise _snapshot_unavailable(exc) from None


def _public_target(value: dict[str, Any]) -> dict[str, Any]:
    result = {key: item for key, item in value.items() if key != "context_token"}
    result["has_context"] = bool(value.get("context_token"))
    return result


def _automation_runs() -> list[dict[str, Any]]:
    try:
        values = UnifiedJobStore(
            get_config().data_root / "jobs.sqlite", read_only=True,
        ).list(200)
    except (FileNotFoundError, sqlite3.Error):
        return []
    return [
        value for value in values
        if str(value.get("type") or "").startswith("automation.")
    ][:50]


def _cold_overview() -> dict[str, Any]:
    enabled = bool(get_config().automation.enabled)
    worker = runtime_worker_status()
    return {
        "enabled": enabled,
        "timezone": get_config().automation.timezone,
        "runtime": "disabled" if not enabled else "degraded",
        "runtime_detail": {**worker, "channels": {}, "source": "runtime-worker-heartbeat"},
        "channels": {
            "feishu": {"configured": False, "label": "飞书应用 Bot", "role": "primary"},
            "weixin": {"configured": False, "label": "腾讯微信 ClawBot", "role": "limited"},
        },
        "bot_accounts": [],
        "inbound": {
            "feishu": {"total": 0, "last_received_at": "", "direct": {}, "group": {}},
            "weixin": {"total": 0, "last_received_at": ""},
        },
        "targets": [],
        "jobs": [],
        "recent_runs": [],
        "recent_events": [],
        "snapshot": {"state": "degraded", "issues": ["automation_snapshot_unavailable"]},
    }


def _error(exc: Exception) -> HTTPException:
    if isinstance(exc, PermissionError):
        return HTTPException(403, str(exc))
    if isinstance(exc, KeyError):
        return HTTPException(404, str(exc).strip("'"))
    return HTTPException(400, str(exc))


class PolicyIn(ContractModel):
    preset: Literal["conservative", "balanced", "sensitive"] = "balanced"
    overrides: dict[str, Any] = Field(default_factory=dict)
    enabled: bool | None = None


class ScheduleIn(ContractModel):
    action: Literal["pause", "resume", "reschedule"]
    schedule: dict[str, Any] | None = None


class FeishuConfigIn(ContractModel):
    app_id: str = Field(..., min_length=3, max_length=200)
    app_secret: SecretStr


@router.get("/api/v1/automation/overview")
def automation_overview(request: Request) -> dict:
    _require_local(request)
    def project(store: AutomationStore) -> dict:
        targets = [_public_target(value) for value in store.targets()]
        accounts = [
            {key: value for key, value in account.items() if key != "secret_target"}
            for account in store.bot_accounts()
        ]
        worker = runtime_worker_status()
        enabled = bool(get_config().automation.enabled)
        runtime = "disabled" if not enabled else "running" if worker.get("available") else "degraded"
        return {
            "enabled": enabled,
            "timezone": get_config().automation.timezone,
            "runtime": runtime,
            "runtime_detail": {
                **worker,
                "channels": {},
                "source": "runtime-worker-heartbeat",
            },
            "channels": {
                "feishu": {
                    "configured": any(a.get("channel") == "feishu" for a in accounts),
                    "label": "飞书应用 Bot", "role": "primary",
                },
                "weixin": {
                    "configured": any(a.get("channel") == "weixin" for a in accounts),
                    "label": "腾讯微信 ClawBot", "role": "limited",
                },
            },
            "bot_accounts": accounts,
            "inbound": {
                "feishu": {
                    **store.inbound_status("feishu"),
                    "direct": store.inbound_status("feishu", "direct"),
                    "group": store.inbound_status("feishu", "group"),
                },
                "weixin": store.inbound_status("weixin"),
            },
            "targets": targets,
            "jobs": store.jobs(),
            "recent_runs": store.recent_runs(12),
            "recent_events": store.recent_events(12),
        }

    return _read(project, default=_cold_overview)


@router.get("/api/v1/automation/targets")
def automation_targets(request: Request) -> dict:
    _require_local(request)
    return {
        "targets": _read(
            lambda store: [_public_target(value) for value in store.targets()],
            default=[],
        )
    }


@router.get("/api/v1/automation/jobs")
def automation_jobs(request: Request) -> dict:
    _require_local(request)
    return {"jobs": _read(lambda store: store.jobs(), default=[]), "runs": _automation_runs()}


@router.get("/api/v1/automation/audit")
def automation_audit(request: Request, limit: int = 100) -> dict:
    _require_local(request)
    return {
        "items": _read(lambda store: store.audit_entries(max(1, min(limit, 500))), default=[])
    }


@router.get("/api/v1/automation/events")
def automation_events(request: Request, limit: int = 100) -> dict:
    _require_local(request)
    return {
        "items": _read(lambda store: store.recent_events(max(1, min(limit, 500))), default=[])
    }


@router.post("/api/v1/automation/bindings/code")
def automation_binding_code(target_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return service().create_binding(target_id)
    except Exception as exc:
        raise _error(exc) from exc


@router.get("/api/v1/automation/bindings/{action_id}")
def automation_binding_status(action_id: str, request: Request) -> dict:
    _require_csrf(request)
    def project(store: AutomationStore) -> dict:
        action = store.binding_action(action_id)
        if not action:
            raise KeyError("绑定会话不存在")
        target_id = str((action.get("payload") or {}).get("target_id") or "")
        target = store.target(target_id)
        if not target:
            raise KeyError("推送目标不存在")
        bound = bool(target.get("target") and target.get("account_id"))
        return {
            "id": action_id,
            "target_id": target_id,
            "status": "bound" if action.get("status") == "consumed" and bound else action.get("status"),
            "expires_at": action.get("expires_at"),
            "bound": bound,
            "inbound": store.inbound_status("feishu", str(target.get("chat_type") or "")),
        }

    try:
        return _read(project)
    except KeyError as exc:
        raise _error(exc) from None


@router.patch("/api/v1/automation/targets/{target_id}/policy")
def automation_policy(target_id: str, value: PolicyIn, request: Request) -> dict:
    _require_csrf(request)
    try:
        target = service().update_policy(
            target_id, value.preset, value.overrides, value.enabled, "web")
        return service()._public_target(target)
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/api/v1/automation/targets/{target_id}/test")
def automation_target_test(target_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return service().test_target(target_id)
    except Exception as exc:
        raise _error(exc) from exc


@router.patch("/api/v1/automation/jobs/{name}")
def automation_job_update(name: str, value: ScheduleIn, request: Request) -> dict:
    _require_csrf(request)
    try:
        result = service().update_schedule(
            name, action=value.action, schedule=value.schedule, actor="web")
        get_runtime().reload_jobs()
        return result
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/api/v1/automation/jobs/{name}/run")
def automation_job_run(name: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return service().run_task(
            name,
            actor="web",
            idempotency_key=str(request.headers.get("Idempotency-Key") or ""),
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/api/v1/automation/channels/weixin/login")
def weixin_login_start(request: Request) -> dict:
    """向腾讯微信 ClawBot iLink 接口申请扫码登录二维码。"""
    _require_csrf(request)
    try:
        return service().start_weixin_login()
    except Exception as exc:
        raise _error(exc) from exc


@router.get("/api/v1/automation/channels/weixin/login/{session_id}")
def weixin_login_poll(session_id: str, request: Request, verify_code: str = "") -> dict:
    _require_csrf(request)
    try:
        result = service().poll_weixin_login(session_id, verify_code)
        if result.get("status") == "confirmed":
            get_runtime().start_channels()
        return result
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/api/v1/automation/channels/feishu/config")
def feishu_config(value: FeishuConfigIn, request: Request) -> dict:
    """保存飞书企业自建应用 Bot 凭据；App Secret 只进入系统凭据库。"""
    _require_csrf(request)
    try:
        result = service().configure_feishu(value.app_id, value.app_secret.get_secret_value())
        runtime_status = get_runtime().restart_channel("feishu")
        return {
            **result,
            "runtime_status": runtime_status,
            "restart_required": False,
            "warnings": ([result["verification"]["message"]]
                         if result["verification"]["status"] == "warning" else []),
        }
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/api/v1/automation/channels/feishu/check")
def feishu_check(request: Request) -> dict:
    """返回实际凭据、运行时、长连接、入站事件与绑定五阶段状态。"""
    _require_csrf(request)
    runtime = get_runtime()
    account = service().store.bot_account("feishu")
    stages: dict[str, dict[str, Any]] = {}
    if not account:
        stages["credential"] = {"status": "error", "message": "尚未配置飞书应用"}
    else:
        try:
            app_id, secret = service().feishu.credentials_value()
            stages["credential"] = service().feishu.verify(app_id, secret)
        except Exception:
            stages["credential"] = {"status": "error", "message": "飞书凭据不可读取"}
    runtime_detail = runtime.status()
    stages["runtime"] = {
        "status": "success" if runtime_detail["status"] == "running" else "warning",
        "message": {
            "running": "自动化运行时已启动",
            "disabled": "自动化总开关关闭",
            "standby": "当前进程正在等待调度租约",
        }.get(runtime_detail["status"], "自动化运行时异常"),
    }
    channel_alive = bool(runtime_detail["channels"].get("feishu"))
    if account and runtime_detail["status"] == "running" and not channel_alive:
        runtime.restart_channel("feishu")
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            time.sleep(0.25)
            runtime_detail = runtime.status()
            channel_alive = bool(runtime_detail["channels"].get("feishu"))
            refreshed_status = (service().store.bot_account("feishu") or {}).get("status")
            if refreshed_status in {"listening", "degraded"} or not channel_alive:
                break
    refreshed = service().store.bot_account("feishu") or account or {}
    websocket_status = str(refreshed.get("status") or "")
    if websocket_status == "listening" and channel_alive:
        connection_status = "success"
    elif account and runtime_detail["status"] != "running":
        connection_status = "warning"
    elif websocket_status == "connecting" and channel_alive:
        connection_status = "warning"
    else:
        connection_status = "error"
    stages["websocket"] = {
        "status": connection_status,
        "message": {
            "connecting": "飞书长连接正在建立",
            "listening": "飞书长连接监听中",
            "degraded": refreshed.get("last_error") or "飞书长连接异常",
        }.get(websocket_status, "飞书长连接尚未启动" if runtime_detail["status"] == "running"
              else "凭据已配置；启用自动化后才会建立长连接"),
    }
    inbound = service().store.inbound_status("feishu")
    stages["event"] = {
        "status": "success" if inbound["total"] else "warning",
        "message": (f"已收到 {inbound['total']} 条消息事件" if inbound["total"] else
                    "长连接尚未收到消息事件；请检查事件订阅、权限和应用发布状态"),
        **inbound,
    }
    targets = service().public_targets()
    bound = [item for item in targets if item["channel"] == "feishu" and item.get("target")]
    stages["binding"] = {
        "status": "success" if bound else "warning",
        "message": f"已绑定 {len(bound)} 个飞书会话" if bound else "尚未绑定管理员私聊或提醒群",
    }
    statuses = {item["status"] for item in stages.values()}
    return {
        "status": "error" if "error" in statuses else
                  "warning" if "warning" in statuses else "success",
        "checked_at": datetime.now(UTC).isoformat(),
        "stages": stages,
    }


@router.delete("/api/v1/automation/channels/feishu/config")
def feishu_remove(request: Request) -> dict:
    _require_csrf(request)
    try:
        get_runtime().stop_channel("feishu")
        return service().remove_feishu()
    except Exception as exc:
        raise _error(exc) from exc
