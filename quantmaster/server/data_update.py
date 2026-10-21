"""Application-level update orchestration over existing worker services."""
from __future__ import annotations

import time
from typing import Any

from quantmaster.config import get_config
from quantmaster.data.update import ACTIVE, UPDATE_STATE
from quantmaster.runtime.jobs import JobContext, JobOutcome


def consumed_symbols() -> list[str]:
    from quantmaster.portfolio.ledger import Ledger
    from quantmaster.portfolio.watchlist import AssetListStore

    assets = AssetListStore(read_only=True).all()
    return sorted({
        *(item["symbol"] for rows in assets.values() for item in rows),
        *(position.symbol for position in Ledger(read_only=True).positions() if position.shares > 0),
    })


def _source_step(state: dict[str, Any], target: str) -> dict[str, Any] | None:
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    status = free_stockdb_runtime.status()
    if str(status.get("validated_session") or "") >= target:
        warnings = (status.get("validation") or {}).get("warnings") or []
        return {
            "status": "partial" if warnings else "completed",
            "detail": "；".join(warnings) or "本地行情库已通过目标交易日验收，复用已有数据",
            "data_as_of": status["validated_session"],
        }
    if not state.get("stockdb_requested"):
        # Re-admission after a crash is safe: the mailbox coalesces updates.
        free_stockdb_runtime.request_update()
        state["stockdb_requested"] = True
        return None
    if status.get("update_result") not in {"failed", "manual_required"} and (
        status.get("phase") in {"queued", "stopping", "syncing", "restarting", "validating", "retry_wait"}
        or status.get("update_result") in {"queued", "running", "validating", "retry_wait"}
    ):
        return None
    return {
        "status": "failed",
        "detail": str(status.get("message") or "本地行情库尚未通过目标交易日验收"),
        "data_as_of": str(status.get("validated_session") or ""),
    }


def _dependency_results(manager: Any, children: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
    store = manager._read_store()
    results = []
    pending = False
    for child in children:
        job = store.get(child["id"])
        result = {**child, "status": job["status"], "detail": job.get("detail", "")}
        if child["kind"] == "行情缓存":
            projected = manager.get(child["id"])
            if projected.get("failed") or projected.get("warning_count"):
                result["status"] = "partial"
                result["detail"] = (
                    f"缺口 {projected.get('failed', 0)}，证据警告 {projected.get('warning_count', 0)}"
                )
        elif job["status"] == "completed" and job.get("result_artifact_id"):
            payload = store.artifact(job["result_artifact_id"])["payload"]
            if (
                payload.get("warnings") or payload.get("fresh") is False
                or payload.get("outcome") in {"partial", "unavailable", "degraded"}
                or (payload.get("staleness") or {}).get("stale")
            ):
                result["status"] = "partial"
                result["detail"] = "；".join(str(v) for v in payload.get("warnings", [])) or "快照仍过期"
        pending = pending or job["status"] in ACTIVE
        results.append(result)
    return results, pending


def _submit_analysis(state: dict[str, Any], spec: dict[str, Any]) -> None:
    from quantmaster.after_close.jobs import get_after_close_jobs
    from quantmaster.rotation.contracts import RotationJobSpec
    from quantmaster.rotation.service import get_rotation_worker

    rotation = get_rotation_worker()
    rotation.start()
    job = rotation.submit(RotationJobSpec(scope="all", source="auto", as_of=spec["target_session"]))
    state["children"].append({"id": job["id"], "kind": "市场与板块分析"})
    if spec.get("include_scan", get_config().data.after_close_enabled):
        job, _created = get_after_close_jobs().submit(as_of=spec["target_session"], force=False)
        state["children"].append({"id": job["id"], "kind": "盘后扫描"})


def _finish_update(
    context: JobContext, state: dict[str, Any], clean: bool, detail: str = "",
) -> JobOutcome:
    state["outcome"] = "completed" if clean else "partial"
    context.write_checkpoint(UPDATE_STATE, context.spec_hash, state)
    artifact = context.write_artifact(UPDATE_STATE, state, {"schema_version": "1"})
    return JobOutcome(
        "completed" if clean else "failed",
        detail or ("数据与分析已更新" if clean else "更新部分完成；保留可用结果，请查看各阶段缺口"),
        artifact["id"],
    )


def _wait_update(context: JobContext, state: dict[str, Any], detail: str) -> JobOutcome:
    context.write_checkpoint(UPDATE_STATE, context.spec_hash, state)
    context.progress(20 if "consumers" not in state["steps"] else 65, "更新数据", detail)
    return JobOutcome("interrupted", detail, retry_delay_seconds=30, waiting_on="data_dependencies")


def _prepare_source(context: JobContext, state: dict[str, Any], target: str) -> JobOutcome | None:
    steps = state["steps"]
    if "stockdb" in steps:
        return None
    source = _source_step(state, target)
    if source is None:
        return _wait_update(context, state, "本地行情库正在检查与更新；可离开页面，后台会继续")
    steps["stockdb"] = source
    context.write_checkpoint(UPDATE_STATE, context.spec_hash, state)
    if source["status"] == "failed":
        for stage in ("consumers", "analysis"):
            steps[stage] = {"status": "blocked", "detail": "行情库未通过验收，本次未启动；保留原有数据"}
        return _finish_update(
            context, state, False, "行情库更新未通过验收，后续阶段未执行；请查看来源状态",
        )
    return None


def run_update(context: JobContext, spec: dict[str, Any], manager: Any) -> JobOutcome:
    """Yield between dependencies, so child jobs can use the same worker pool."""
    state = context.load_checkpoint(UPDATE_STATE, context.spec_hash) or {
        "target_session": spec["target_session"], "steps": {}, "children": [],
        "plan_started_at": time.time(),
    }
    steps = state["steps"]
    if context.cancelled():
        return JobOutcome("cancelled", "已停止后续更新；已提交的共享数据任务继续运行")
    # Dependency yields create new runtime attempts; bound the whole plan as well.
    if time.time() - state.setdefault("plan_started_at", time.time()) >= context.deadline_seconds:
        for stage in ("stockdb", "consumers", "analysis"):
            if steps.get(stage, {}).get("status") not in {"completed", "partial"}:
                steps[stage] = {
                    "status": "blocked", "detail": "依赖等待已到期限；保留已有结果，可稍后重新更新",
                }
        return _finish_update(context, state, False, "更新等待已到期限；请查看来源和子任务状态后重试")
    waiting = _prepare_source(context, state, spec["target_session"])
    if waiting:
        return waiting
    if "consumers" not in steps:
        jobs = (manager.maintain_consumed(manual=True, end=spec["target_session"])
                if spec.get("include_cache", True) else [])
        state["children"] = [{"id": job["id"], "kind": "行情缓存"} for job in jobs]
        steps["consumers"] = {
            "status": "running",
            "detail": "补齐市场、自选、持仓和研究候选" if jobs else "缓存自动维护已关闭，本次跳过",
        }
        return _wait_update(context, state, "正在检查并补齐使用中的行情缓存")
    results, pending = _dependency_results(manager, state["children"])
    state["results"] = results
    if pending:
        return _wait_update(context, state, "后台任务仍在执行；进度断开不会取消更新")
    if "analysis" not in steps:
        steps["consumers"]["status"] = "completed" if all(
            r["status"] == "completed" for r in results
        ) else "partial"
        _submit_analysis(state, spec)
        steps["analysis"] = {"status": "running", "detail": "重算并发布市场、板块和盘后快照"}
        return _wait_update(context, state, "正在重算分析；完成后页面自动读取新结果")
    analysis_clean = all(r["status"] == "completed" for r in results if r["kind"] != "行情缓存")
    clean = steps["stockdb"]["status"] == "completed" and all(r["status"] == "completed" for r in results)
    steps["analysis"]["status"] = "completed" if analysis_clean else "partial"
    return _finish_update(context, state, clean)
