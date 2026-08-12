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
from quantmaster.runtime.jobs import UnifiedJobRuntime, UnifiedJobStore
from quantmaster.runtime.worker import runtime_worker_status
from quantmaster.server.management import _require_csrf, _require_local

router = APIRouter()
_MISSING = object()
_ACTIVE_RUN_STATUSES = frozenset({"queued", "running", "cancelling", "interrupted"})
_FAILED_RUN_STATUSES = frozenset({"failed"})
_JOB_KINDS = {
    "intraday_monitor": "high_frequency_poll",
    "fast_news_scan": "time_window",
    "official_news_scan": "time_window",
    "periodic_news_scan": "time_window",
    "daily_close_pipeline": "daily",
    "news_digest": "daily",
    "news_dead_letter_recovery": "daily",
    "paper_rebalance_proposal": "daily",
}


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


def _iso_time(value: Any) -> str:
    if value in (None, "", 0, 0.0):
        return ""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), UTC).isoformat()
    return str(value)


def _seconds_between(start: Any, finish: Any = "") -> int:
    if not start:
        return 0
    try:
        left = datetime.fromisoformat(str(start)).timestamp()
        right = datetime.fromisoformat(str(finish)).timestamp() if finish else time.time()
    except (TypeError, ValueError):
        return 0
    return max(0, round(right - left))


def _count(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _automation_run_rows() -> list[dict[str, Any]]:
    try:
        values = UnifiedJobStore(
            get_config().data_root / "jobs.sqlite", read_only=True,
        ).list(200)
    except (FileNotFoundError, sqlite3.Error):
        return []
    return [
        UnifiedJobRuntime.public(value) for value in values
        if str(value.get("type") or "").startswith("automation.")
    ][:50]


def _public_automation_run(value: dict[str, Any]) -> dict[str, Any]:
    """Publish scheduler facts without exposing a job spec, owner or lease token."""

    status = str(value.get("status") or "unknown")
    started_at = _iso_time(value.get("started_at"))
    finished_at = _iso_time(value.get("finished_at"))
    heartbeat_at = _iso_time(value.get("heartbeat_at"))
    backoff_value = value.get("backoff") or {}
    backoff = backoff_value if isinstance(backoff_value, dict) else {}
    stalled_value = value.get("stalled") or {}
    stalled = stalled_value if isinstance(stalled_value, dict) else {}
    queue_value = value.get("queue") or {}
    queue = queue_value if isinstance(queue_value, dict) else {}
    next_retry_at = _iso_time(backoff.get("next_retry_at"))
    diagnostic_code = str(stalled.get("diagnostic_code") or "")[:80]
    job_id = str(value.get("id") or "")
    return {
        "domain": "automation",
        "id": job_id,
        "type": str(value.get("type") or "automation.job"),
        "status": status,
        "created": bool(value.get("created")),
        "coalesced": bool(value.get("coalesced")),
        "reused": bool(value.get("reused")),
        "progress": max(0, min(100, _count(value.get("progress")))),
        "phase": str(value.get("phase") or "")[:200],
        "detail": str(value.get("detail") or "")[:1000],
        "attempt": max(1, _count(value.get("attempt")) or 1),
        "created_at": _iso_time(value.get("created_at")),
        "updated_at": _iso_time(value.get("updated_at")),
        "started_at": started_at,
        "finished_at": finished_at,
        "heartbeat_at": heartbeat_at,
        "last_completed_unit_at": _iso_time(value.get("last_completed_unit_at")),
        "elapsed_seconds": _seconds_between(started_at, finished_at),
        "estimated_remaining_seconds": _count(value.get("estimated_remaining_seconds")),
        "queue": {
            "pending": _count(queue.get("pending")),
            "running": _count(queue.get("running")),
            "retry_wait": _count(queue.get("retry_wait")),
            "dead_letter": _count(queue.get("dead_letter")),
        },
        "coalesced_count": _count(value.get("coalesced_count")),
        "backoff": {
            "active": bool(backoff.get("active")),
            "reason": str(backoff.get("reason") or "")[:500],
            "waiting_on": str(backoff.get("waiting_on") or "")[:200],
            "next_retry_at": next_retry_at,
        },
        "stalled": {
            "is_stalled": bool(stalled.get("is_stalled")),
            "reason": str(stalled.get("reason") or "")[:500],
            "diagnostic_code": diagnostic_code,
            "observed_at": _iso_time(stalled.get("observed_at")),
            "waiting_on": str(stalled.get("waiting_on") or "")[:200],
        },
        "links": {
            "self": f"/api/v1/jobs/{job_id}",
            "events": f"/api/v1/jobs/{job_id}/events",
        },
    }


def _automation_runs() -> list[dict[str, Any]]:
    return [_public_automation_run(value) for value in _automation_run_rows()]


def _outbox_summary(store: AutomationStore, *, enabled: bool) -> dict[str, Any]:
    defaults = {
        "dispatcher_status": "disabled" if not enabled else "not_configured",
        "pending": 0, "leased": 0, "retry_wait": 0,
        "sent": 0, "dead_letter": 0, "next_retry_at": "",
    }
    try:
        accounts = store.bot_accounts("feishu")
        configured = any(
            str(item.get("status") or "") not in {"disabled", "not_configured"}
            and bool(item.get("account_id")) and bool(item.get("secret_target"))
            for item in accounts
        )
        with store._conn() as connection:  # read-only operational projection
            rows = connection.execute(
                "SELECT status,COUNT(*) AS count,MIN(next_attempt_at) AS next_attempt_at "
                "FROM delivery_attempts GROUP BY status"
            ).fetchall()
    except (FileNotFoundError, OSError, sqlite3.Error):
        return defaults
    mapping = {status: status for status in (
        "pending", "leased", "retry_wait", "sent", "dead_letter",
    )}
    retry_times: list[float] = []
    for row in rows:
        key = mapping.get(str(row["status"] or ""))
        if key:
            defaults[key] += _count(row["count"])
        if key == "retry_wait" and row["next_attempt_at"]:
            retry_times.append(float(row["next_attempt_at"]))
    defaults["dispatcher_status"] = (
        "disabled" if not enabled else "running" if configured else "not_configured"
    )
    if retry_times:
        defaults["next_retry_at"] = _iso_time(min(retry_times))
    return defaults


def _job_projection(
    templates: list[dict[str, Any]], runs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    projected: list[dict[str, Any]] = []
    for template in templates:
        name = str(template.get("name") or "")
        matching = [run for run in runs if str(run.get("type") or "") == f"automation.{name}"]
        active = [run for run in matching if str(run.get("status") or "") in _ACTIVE_RUN_STATUSES]
        selected = active[0] if active else matching[0] if matching else {}
        queue = selected.get("queue") or {}
        execution = {
            **selected,
            "active_job_id": str((active[0] if active else {}).get("id") or ""),
            "running_instances": sum(
                str(run.get("status") or "") in {"running", "cancelling"} for run in matching
            ),
            "queue": {
                "pending": sum(_count((run.get("queue") or {}).get("pending")) for run in active),
                "running": sum(_count((run.get("queue") or {}).get("running")) for run in active),
                "retry_wait": sum(_count((run.get("queue") or {}).get("retry_wait")) for run in active),
                "dead_letter": max(
                    [_count((run.get("queue") or {}).get("dead_letter")) for run in matching] or [0]
                ),
            } if active else queue,
            "coalesced_count": sum(_count(run.get("coalesced_count")) for run in matching),
        }
        projected.append({**template, "job_kind": _JOB_KINDS.get(name, "manual"), "execution": execution})
    return projected


def _queue_summary(runs: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "queued": sum(str(run.get("status") or "") == "queued" for run in runs),
        "running": sum(str(run.get("status") or "") in {"running", "cancelling"} for run in runs),
        "retry_wait": sum(bool((run.get("backoff") or {}).get("active")) for run in runs),
        "failed": sum(str(run.get("status") or "") in _FAILED_RUN_STATUSES for run in runs),
        "dead_letter": sum(_count((run.get("queue") or {}).get("dead_letter")) for run in runs),
        "coalesced_count": sum(_count(run.get("coalesced_count")) for run in runs),
    }


def _cold_overview() -> dict[str, Any]:
    enabled = bool(get_config().automation.enabled)
    worker = runtime_worker_status()
    return {
        "enabled": enabled,
        "timezone": get_config().automation.timezone,
        "runtime": {
            "status": "disabled" if not enabled else "degraded",
            "worker": worker,
        },
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
        "recent_events": [],
        "scheduler": {
            "status": "disabled" if not enabled else "unavailable",
            "managed_by": "runtime-worker", "worker_pid": None,
            "timezone": get_config().automation.timezone,
        },
        "queue_summary": _queue_summary([]),
        "outbox": _outbox_summary(AutomationStore(read_only=True), enabled=enabled),
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
        accounts = []
        for account in store.bot_accounts():
            if account["channel"] == "feishu":
                # The web process is a read-only observer; never retrieve the
                # credential just to render an overview.
                accounts.append({
                    "channel": "feishu", "status": str(account.get("status") or "not_configured"),
                    "app_id_present": bool(account.get("account_id")),
                    "app_id_suffix": str(account.get("account_id") or "")[-4:],
                    "app_secret_present": bool(account.get("secret_target")),
                    "last_validated_at": str(account.get("last_validated_at") or ""),
                    "updated_at": str(account.get("updated_at") or ""),
                    "last_error": "" if str(account.get("status") or "") == "connected"
                    else "请使用“检测连接”查看脱敏诊断。",
                })
            else:
                accounts.append({key: item for key, item in account.items() if key != "secret_target"})
        worker = runtime_worker_status()
        enabled = bool(get_config().automation.enabled)
        runtime = "disabled" if not enabled else "running" if worker.get("available") else "degraded"
        runs = _automation_runs()
        templates = store.jobs()
        return {
            "enabled": enabled,
            "timezone": get_config().automation.timezone,
            "runtime": {"status": runtime, "worker": worker},
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
            "jobs": _job_projection(templates, runs),
            "recent_events": store.recent_events(12),
            "scheduler": {
                "status": runtime,
                "managed_by": "runtime-worker",
                "worker_pid": worker.get("pid"),
                "timezone": get_config().automation.timezone,
                "checked_at": datetime.now(UTC).isoformat(),
            },
            "queue_summary": _queue_summary(runs),
            "outbox": _outbox_summary(store, enabled=enabled),
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
    runs = _automation_runs()
    jobs = _read(lambda store: _job_projection(store.jobs(), runs), default=[])
    return {"jobs": jobs, "runs": runs, "queue_summary": _queue_summary(runs)}


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
        result = service().run_task(
            name,
            actor="web",
            idempotency_key=str(request.headers.get("Idempotency-Key") or ""),
        )
        job_id = str(result.get("job_id") or result.get("run_id") or "")
        result.setdefault("coalesced", not bool(result.get("created", True)))
        result.setdefault("reused", False)
        result.setdefault("links", {
            "self": f"/api/v1/jobs/{job_id}",
            "events": f"/api/v1/jobs/{job_id}/events",
        })
        try:
            run = UnifiedJobStore(
                get_config().data_root / "jobs.sqlite", read_only=True,
            ).get(job_id)
        except (FileNotFoundError, KeyError, sqlite3.Error):
            return result
        public = _public_automation_run(run)
        result.update({
            "status": public["status"], "progress": public["progress"],
            "phase": public["phase"], "coalesced": result["coalesced"],
            "reused": bool(run.get("reused")), "links": public["links"],
        })
        return result
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
        result = get_runtime().replace_feishu(value.app_id, value.app_secret.get_secret_value())
        return {
            **result,
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
        stages["credential"] = {
            "status": "warning", "state": "not_configured",
            "message": "尚未配置飞书应用；不会启动 Bot 或重试。",
        }
    else:
        try:
            app_id, secret = service().feishu.credentials_value()
            stages["credential"] = service().feishu.verify(app_id, secret)
            state = str(stages["credential"].get("state") or "")
            service().store.set_bot_validation(
                "feishu", app_id, state or "invalid_credentials",
                "" if state == "connected" else str(stages["credential"].get("message") or ""),
            )
        except Exception:
            stages["credential"] = {
                "status": "warning", "state": "not_configured",
                "message": "飞书凭据不可读取；不会启动 Bot 或重试。",
            }
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
    credential_ready = stages["credential"].get("state") in {
        "connected", "network_error", "tls_error", "rate_limited",
    }
    refreshed = service().store.bot_account("feishu") or account or {}
    websocket_status = str(refreshed.get("status") or "")
    if not account or not credential_ready:
        connection_status = "warning"
    elif websocket_status == "connected" and channel_alive:
        connection_status = "success"
    elif account and runtime_detail["status"] != "running":
        connection_status = "warning"
    elif websocket_status == "connecting" and channel_alive:
        connection_status = "warning"
    else:
        connection_status = "error"
    stages["websocket"] = {
        "status": connection_status,
        "state": websocket_status or "not_configured",
        "message": {
            "connecting": "飞书长连接正在建立",
            "connected": "飞书长连接监听中",
            "invalid_credentials": "凭据无效；请更新 App ID / App Secret",
            "tls_error": "TLS 连接失败；请检查系统时间、证书和网络",
            "network_error": "飞书网络不可达；请检查网络后重试",
            "rate_limited": "飞书连接受限；请稍后重试",
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
