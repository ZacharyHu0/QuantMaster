"""设置中心的只读连通性与可用性检测。"""

from __future__ import annotations

import importlib.util
import socket
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from quantmaster.settings import (
    DataSettings,
    LabSettings,
    LLMSettings,
    ServerSettings,
    normalize_api_base,
)


def _result(status: str, message: str, started: float, **details: Any) -> dict[str, Any]:
    return {
        "status": status,
        "message": message,
        "latency_ms": round((time.perf_counter() - started) * 1000),
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "details": details,
    }


def list_llm_models(settings: LLMSettings, api_key: str = "") -> dict[str, Any]:
    started = time.perf_counter()
    provider = settings.provider
    if provider in {"anthropic", "openai"} and not api_key:
        return _result("error", "尚未配置 API Key", started, models=[])
    headers: dict[str, str]
    if provider == "anthropic":
        base = normalize_api_base(provider, settings.base_url) or "https://api.anthropic.com/v1"
        url = f"{base.rstrip('/')}/models"
        headers = {"x-api-key": api_key, "anthropic-version": "2023-06-01"}
    elif provider == "openai":
        base = normalize_api_base(provider, settings.base_url) or "https://api.openai.com/v1"
        url = f"{base.rstrip('/')}/models"
        headers = {"Authorization": f"Bearer {api_key}"}
    else:
        base = normalize_api_base(provider, settings.base_url)
        url = f"{base.rstrip('/')}/models"
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    models: list[str] = []
    try:
        with httpx.Client(timeout=settings.timeout, follow_redirects=False) as client:
            for _ in range(20):
                response = client.get(url, headers=headers)
                if response.status_code in {401, 403}:
                    return _result("error", "API Key 无效或无权读取模型列表", started,
                                   http_status=response.status_code, models=[])
                if response.status_code == 404:
                    return _result("error", "模型列表地址不存在，请检查 API 根地址", started,
                                   http_status=404, models=[])
                response.raise_for_status()
                payload = response.json()
                for item in payload.get("data", payload.get("models", [])):
                    model_id = item.get("id") if isinstance(item, dict) else str(item)
                    if model_id:
                        models.append(str(model_id))
                if provider != "anthropic" or not payload.get("has_more"):
                    break
                last_id = payload.get("last_id")
                if not last_id:
                    break
                # Anthropic 使用 after_id 游标分页；保持用户原有 base URL。
                url = str(httpx.URL(f"{base.rstrip('/')}/models").copy_add_param(
                    "after_id", str(last_id)
                ))
        models = sorted(set(models), key=str.casefold)
        message = f"已读取 {len(models)} 个模型；列表不代表聊天接口兼容性"
        return _result("success", message, started, models=models,
                       selected_present=settings.model in models)
    except httpx.TimeoutException:
        return _result("warning", "请求超时；这不会阻止保存设置", started, models=[])
    except (httpx.HTTPError, ValueError) as exc:
        return _result("warning", f"联网检测失败：{type(exc).__name__}", started, models=[])


def check_tushare(token: str) -> dict[str, Any]:
    started = time.perf_counter()
    if not token:
        return _result("error", "尚未配置 Tushare Token", started)
    if importlib.util.find_spec("tushare") is None:
        return _result("error", "缺少 tushare 依赖，请安装 quantmaster[tushare]", started,
                       category="missing-dependency")
    try:
        import tushare as ts

        pro = ts.pro_api(token)
        frame = pro.trade_cal(exchange="SSE", start_date="20240102", end_date="20240103")
        return _result("success", f"Token 可用，返回 {len(frame)} 个交易日历记录", started)
    except Exception as exc:  # tushare 将服务端错误包装为通用 Exception
        text = str(exc).lower()
        if any(word in text for word in ("token", "invalid", "无效", "登录")):
            category, message, status = "invalid-token", "Token 无效", "error"
        elif any(word in text for word in ("积分", "权限", "permission", "privilege", "2000")):
            category, message, status = (
                "permission", "Token 有效但缺少 trade_cal 权限（当前接口要求 2000 积分）", "error"
            )
        else:
            category, message, status = "network", "Tushare 网络或服务检测失败；仍可保存", "warning"
        return _result(status, message, started, category=category)


def check_storage(data: DataSettings) -> dict[str, Any]:
    started = time.perf_counter()
    root = Path(data.root).expanduser().resolve()
    try:
        root.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix=".qm-write-test-", dir=root, delete=True) as handle:
            handle.write(b"quantmaster")
            handle.flush()
            __import__("os").fsync(handle.fileno())
        usage = __import__("shutil").disk_usage(root)
        return _result("success", "目录可写", started, path=str(root), free_bytes=usage.free)
    except OSError as exc:
        return _result("error", f"目录不可写：{exc.strerror or type(exc).__name__}", started,
                       path=str(root))


def check_data_sources(timeout: float = 8.0) -> dict[str, Any]:
    started = time.perf_counter()
    sources: dict[str, Any] = {}
    for package, url in (("akshare", "https://www.akshare.xyz"),
                         ("yfinance", "https://query1.finance.yahoo.com")):
        if importlib.util.find_spec(package) is None:
            sources[package] = {"status": "error", "message": "依赖未安装"}
            continue
        try:
            response = httpx.get(url, timeout=timeout, follow_redirects=True)
            sources[package] = {"status": "success" if response.status_code < 500 else "warning",
                                "message": f"网络 HTTP {response.status_code}"}
        except httpx.HTTPError:
            sources[package] = {"status": "warning", "message": "依赖已安装，但网络不可达"}
    overall = "success" if all(v["status"] == "success" for v in sources.values()) else "warning"
    return _result(overall, "数据源依赖与网络检测完成", started, sources=sources)


def check_server(settings: ServerSettings) -> dict[str, Any]:
    started = time.perf_counter()
    family = socket.AF_INET6 if ":" in settings.host else socket.AF_INET
    host = settings.host
    if host == "localhost":
        host, family = "127.0.0.1", socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        sock.bind((host, settings.port))
        status, message, available = "success", "host/port 合法且端口可用", True
    except OSError as exc:
        status, message, available = "warning", f"地址合法，但端口当前不可绑定：{exc.strerror or exc}", False
    finally:
        sock.close()
    return _result(status, message, started, host=settings.host,
                   port=settings.port, available=available)


def check_lab(settings: LabSettings, data: DataSettings, tushare_token: str) -> dict[str, Any]:
    """检测研究候选、PIT 数据权限和所选计算设备。"""
    started = time.perf_counter()
    checks: dict[str, Any] = {}
    universe = settings.universe
    if universe == "csi800":
        if not tushare_token:
            checks["universe"] = {
                "status": "warning", "message": "csi800 需要 Tushare Token 与 index_weight 权限",
            }
        elif importlib.util.find_spec("tushare") is None:
            checks["universe"] = {
                "status": "error", "message": "缺少 tushare 依赖",
            }
        else:
            try:
                import tushare as ts

                frame = ts.pro_api(tushare_token).index_weight(
                    index_code="000300.SH", start_date="20240301", end_date="20240331")
                checks["universe"] = {
                    "status": "success" if len(frame) else "warning",
                    "message": f"index_weight 可用，返回 {len(frame)} 条成分记录",
                }
            except Exception as exc:
                text = str(exc).lower()
                permission = any(word in text for word in (
                    "积分", "权限", "permission", "privilege", "2000"))
                checks["universe"] = {
                    "status": "error" if permission else "warning",
                    "message": "缺少 index_weight 权限" if permission else "PIT 成分联网检测失败",
                }
    elif universe == "demo":
        checks["universe"] = {"status": "success", "message": "内置 demo 候选可用"}
    else:
        path = Path(data.root).expanduser().resolve() / "universe" / f"{universe}.json"
        checks["universe"] = {
            "status": "success" if path.is_file() else "error",
            "message": "自定义候选文件可用" if path.is_file() else "自定义候选不存在",
        }

    torch_available = importlib.util.find_spec("torch") is not None
    sklearn_available = importlib.util.find_spec("sklearn") is not None
    device_status, device_message = "success", f"设备策略 {settings.device} 可用于新任务"
    if settings.device in {"cuda", "mps"} and not torch_available:
        device_status, device_message = "warning", "所选设备需要安装 PyTorch"
    elif settings.device in {"cuda", "mps"} and torch_available:
        try:
            import torch

            available = (torch.cuda.is_available() if settings.device == "cuda" else
                         bool(getattr(torch.backends, "mps", None) and
                              torch.backends.mps.is_available()))
            if not available:
                device_status, device_message = "warning", f"当前环境未检测到 {settings.device}"
        except Exception:
            device_status, device_message = "warning", "计算设备检测失败"
    checks["compute"] = {
        "status": device_status, "message": device_message,
        "torch": torch_available, "sklearn": sklearn_available,
    }
    statuses = {item["status"] for item in checks.values()}
    overall = "error" if "error" in statuses else "warning" if "warning" in statuses else "success"
    return _result(overall, "Quant Lab 研究环境检测完成", started, checks=checks)
