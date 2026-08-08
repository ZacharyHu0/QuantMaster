"""Offline-first admission checks for every Quant Lab operation."""

from __future__ import annotations

import importlib.util
import shutil
from datetime import date
from typing import Any

import pandas as pd

from quantmaster.config import get_config
from quantmaster.lab.dataset import inspect_local_dataset
from quantmaster.lab.errors import LabError
from quantmaster.lab.models import DataPolicy, ResourceClass

_DEEP_MODELS = {
    "mlp", "tcn", "gru", "transformer", "dae",
    "multi-transformer", "multi-tcn", "multi-gru",
}

_DATASET_SUMMARY_KEYS = {
    "universe", "start", "end", "as_of", "state", "symbol_count",
    "membership_source", "membership_hash", "bytes", "manifest_hash",
}


def resource_class_for(operation: str, params: dict[str, Any]) -> ResourceClass:
    if operation == "prepare_data":
        return ResourceClass.IO
    if operation in {"discover_llm", "discover_python"}:
        return ResourceClass.EXTERNAL
    model = str(params.get("model") or "")
    models = {str(item) for item in params.get("models") or []}
    if operation == "train" and model in _DEEP_MODELS:
        return ResourceClass.GPU
    if operation == "optimize" and models & _DEEP_MODELS:
        return ResourceClass.GPU
    return ResourceClass.CPU


def _blocker(code: str, message: str, action: str, **context: Any) -> dict[str, Any]:
    return {"code": code, "message": message, "action": action, "context": context}


def _compact_issue(issue: dict[str, Any], sample_limit: int) -> dict[str, Any]:
    value = dict(issue)
    context = dict(value.get("context") or {})
    sample = context.get("sample")
    if isinstance(sample, list):
        context["sample"] = sample[:sample_limit]
        if len(sample) > sample_limit:
            context["sample_omitted"] = len(sample) - sample_limit
    value["context"] = context
    return value


def compact_preflight(
    report: dict[str, Any], *, sample_limit: int = 3,
) -> dict[str, Any]:
    """Bound admission payloads so Doctor and first paint stay predictably small."""
    value = dict(report)
    value["blockers"] = [
        _compact_issue(item, sample_limit) for item in report.get("blockers") or []
    ]
    value["warnings"] = [
        _compact_issue(item, sample_limit) for item in report.get("warnings") or []
    ]
    dataset = dict(report.get("dataset") or {})
    summary = {key: dataset[key] for key in _DATASET_SUMMARY_KEYS if key in dataset}
    for name in ("missing", "coverage_gaps", "warmup_gaps"):
        items = dataset.get(name)
        summary[f"{name}_count"] = len(items) if isinstance(items, list) else 0
        if isinstance(items, list) and items:
            summary[f"{name}_sample"] = items[:sample_limit]
    value["dataset"] = summary
    return value


def run_preflight(operation: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a deterministic, network-free admission report."""
    from quantmaster.lab.ml import capabilities as ml_capabilities

    cfg = get_config()
    values = dict(params or {})
    resource_class = resource_class_for(operation, values)
    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    ml = ml_capabilities()
    requested_device = str(values.get("device") or cfg.lab.device or "auto")
    effective_device = "cpu"

    if operation in {"discover_llm", "discover_python"} and not cfg.llm.api_key:
        blockers.append(_blocker(
            "DEPENDENCY_MISSING", "AI 发现需要先配置 LLM",
            "在设置中心配置 LLM API 后重新预检", dependency="llm",
        ))
    if operation == "discover_python" and not cfg.lab.ai_python_mining_enabled:
        blockers.append(_blocker(
            "DEPENDENCY_MISSING", "受限 Python AutoMiner 尚未启用",
            "在设置中心启用 AI Python 挖掘", dependency="ai_python_mining",
        ))
    if operation == "optimize" and not importlib.util.find_spec("optuna"):
        blockers.append(_blocker(
            "DEPENDENCY_MISSING", "滚动优化需要 Optuna",
            "安装 quantmaster[ml] 后重试", dependency="optuna",
        ))
    model = str(values.get("model") or "")
    deep_requested = resource_class == ResourceClass.GPU or model in _DEEP_MODELS
    if operation == "train" and model == "ridge" and not ml.get("sklearn"):
        blockers.append(_blocker(
            "DEPENDENCY_MISSING", "Ridge 训练需要 scikit-learn",
            "安装 quantmaster[ml] 后重试", dependency="scikit-learn",
        ))
    if deep_requested:
        if not ml.get("torch"):
            blockers.append(_blocker(
                "DEPENDENCY_MISSING", "深度模型需要 PyTorch",
                "安装 CUDA 版 quantmaster[ml] 后重试", dependency="torch",
            ))
        cuda_ready = bool((ml.get("gpu") or {}).get("available"))
        if requested_device == "cuda" and not cuda_ready:
            blockers.append(_blocker(
                "CUDA_UNAVAILABLE", "已请求 CUDA，但当前 PyTorch 无法使用 NVIDIA GPU",
                "运行 qm lab doctor，安装官方 CUDA PyTorch 构建", requested="cuda",
            ))
        elif requested_device in {"auto", "cuda"} and cuda_ready:
            effective_device = "cuda:0"
        elif requested_device.startswith("cuda"):
            effective_device = requested_device

    universe = str(values.get("universe") or cfg.lab.universe)
    start = str(values.get("start") or cfg.lab.start)
    end = str(values.get("end") or date.today().isoformat())
    policy_value = str(values.get("data_policy") or cfg.lab.data_policy)
    try:
        policy = DataPolicy(policy_value)
    except ValueError:
        blockers.append(_blocker(
            "INVALID_REQUEST", f"未知数据策略: {policy_value}",
            "使用 local_only、prefer_local 或 refresh_missing",
        ))
        policy = DataPolicy.PREFER_LOCAL

    dataset: dict[str, Any] = {}
    needs_dataset = operation not in {"create_factor", "reject"}
    if needs_dataset:
        try:
            dataset = inspect_local_dataset(universe, start, end)
        except (OSError, RuntimeError, ValueError) as exc:
            dataset = {"state": "incomplete", "symbol_count": 0, "blockers": []}
            blockers.append(_blocker(
                "DATASET_MISSING", "无法检查本地研究数据",
                "运行 qm lab doctor 并修复本地数据池", reason=type(exc).__name__,
            ))
        if operation == "prepare_data":
            if (
                universe.lower() == "csi800"
                and not dataset.get("symbol_count")
                and not cfg.data.tushare_token
            ):
                blockers.append(_blocker(
                    "DEPENDENCY_MISSING", "首次准备 CSI800 成分需要 Tushare",
                    "配置 Tushare token，或导入已有 PIT 成分缓存", dependency="tushare",
                ))
        elif policy != DataPolicy.REFRESH_MISSING:
            blockers.extend(dataset.get("blockers") or [])
        warnings.extend(dataset.get("warnings") or [])
        if dataset.get("state") == "stale":
            stale = {
                "code": "DATASET_STALE",
                "message": f"本地快照截至 {dataset.get('as_of') or '未知日期'}",
                "action": "可继续研究；生产审批前请显式更新数据",
            }
            if operation in {"approve", "deploy"} or values.get("research_tier") == "production":
                blockers.append(stale)
            else:
                warnings.append(stale)

    symbol_count = int(dataset.get("symbol_count") or 0)
    try:
        sessions = max(1, len(pd.bdate_range(start, end)))
    except (TypeError, ValueError):
        sessions = 1
    feature_bytes = symbol_count * sessions * 48 * 4
    sample_count = max(0, symbol_count * max(0, sessions - int(values.get("sequence_length") or 20)))
    cache_root = cfg.data_root / "lab_cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    disk_free = shutil.disk_usage(cache_root).free
    disk_estimate = feature_bytes + sample_count * 12
    if disk_estimate > disk_free:
        blockers.append(_blocker(
            "MEMORY_BUDGET_EXCEEDED", "特征缓存所需磁盘空间不足",
            "缩短区间或清理 Quant Lab 缓存",
            required_bytes=disk_estimate, free_bytes=disk_free,
        ))

    state = "blocked" if blockers else "degraded" if warnings else "ready"
    report = {
        "operation": operation,
        "runnable": not blockers,
        "state": state,
        "resource_class": resource_class.value,
        "data_policy": policy.value,
        "blockers": blockers,
        "warnings": warnings,
        "estimate": {
            "symbols": symbol_count,
            "sessions": sessions,
            "samples": sample_count,
            "feature_bytes": feature_bytes,
            "disk_bytes": disk_estimate,
            "disk_free_bytes": disk_free,
        },
        "dataset": {
            key: value for key, value in dataset.items()
            if key not in {"bars", "membership_records", "symbols"}
        },
        "compute": {
            "requested_device": requested_device,
            "effective_device": effective_device,
            "gpu": ml.get("gpu") or {},
            "torch": ml.get("torch"),
            "torch_version": ml.get("torch_version", ""),
            "cuda_runtime": ml.get("cuda_runtime", ""),
        },
    }
    return compact_preflight(report, sample_limit=10)


def require_runnable(report: dict[str, Any]) -> None:
    if report.get("runnable"):
        return
    first = (report.get("blockers") or [{}])[0]
    raise LabError(
        str(first.get("code") or "PREFLIGHT_BLOCKED"),
        str(first.get("message") or "任务预检未通过"),
        action=str(first.get("action") or "修复阻塞项后重试"),
        retryable=True,
        context={"preflight": report},
        status_code=409,
    )
