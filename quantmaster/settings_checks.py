"""设置中心的只读连通性与可用性检测。"""

from __future__ import annotations

import importlib.util
import logging
import os
import socket
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from quantmaster.settings import (
    DataSettings,
    LabSettings,
    LLMSettings,
    ServerSettings,
    normalize_api_base,
)

logger = logging.getLogger(__name__)


def _result(status: str, message: str, started: float, **details: Any) -> dict[str, Any]:
    from quantmaster.settings_runtime import diagnostic_id

    return {
        "status": status,
        "message": message,
        "latency_ms": round((time.perf_counter() - started) * 1000),
        "checked_at": datetime.now(UTC).isoformat(),
        "diagnostic_id": diagnostic_id("diag"),
        "details": details,
    }


def list_llm_models(
    settings: LLMSettings, api_key: str = "", *, isolated: bool = False,
) -> dict[str, Any]:
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

    from quantmaster.ai.llm import (
        LLMClient,
        LLMError,
        _api_error,
        llm_diagnostic_details,
        record_llm_failure,
    )
    from quantmaster.config import LLMConfig

    client = LLMClient(LLMConfig(
        provider=settings.provider,
        model=settings.model,
        api_key=api_key,
        base_url=settings.base_url,
        reasoning_effort=settings.reasoning_effort,
        max_tokens=settings.max_tokens,
        temperature=settings.temperature,
        timeout=settings.timeout,
        max_concurrency=settings.max_concurrency,
        queue_timeout=settings.queue_timeout,
    ), register_health=not isolated)
    models: list[str] = []
    try:
        for _ in range(20):
            response = client.guarded_get(
                url, headers=headers, timeout=settings.timeout, follow_redirects=False,
                record_health=not isolated,
            )
            if response.status_code in {401, 403}:
                error = _api_error("模型目录", response)
                if not isolated:
                    record_llm_failure(client.config, error)
                return _result("error", "API Key 无效或无权读取模型列表", started, models=[],
                               **llm_diagnostic_details(client.config, error))
            if response.status_code == 404:
                error = _api_error("模型目录", response)
                if not isolated:
                    record_llm_failure(client.config, error)
                return _result("error", "模型列表地址不存在，请检查 API 根地址", started, models=[],
                               **llm_diagnostic_details(client.config, error))
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
    except LLMError as exc:
        if not isolated:
            record_llm_failure(client.config, exc)
        return _result(
            "warning", "模型目录检测失败；这不会阻止保存设置", started, models=[],
            **llm_diagnostic_details(client.config, exc),
        )
    except (httpx.HTTPError, ValueError):
        error = LLMError("模型目录响应无法解析", code="response_contract_error",
                         retryable=True, category="response_contract")
        if not isolated:
            record_llm_failure(client.config, error)
        return _result("warning", "模型目录检测失败；这不会阻止保存设置", started, models=[],
                       **llm_diagnostic_details(client.config, error))


def check_llm_web_search(
    settings: LLMSettings, api_key: str = "", *, isolated: bool = False,
) -> dict[str, Any]:
    """Force a bounded native-search probe without exposing credentials or raw responses."""
    started = time.perf_counter()
    if settings.provider in {"anthropic", "openai"} and not api_key:
        return _result("error", "尚未配置 API Key", started, supported=False, sources=[])

    from quantmaster.ai.llm import (
        LLMClient,
        LLMError,
        llm_diagnostic_details,
        record_llm_failure,
        reset_web_search_capability,
    )
    from quantmaster.config import LLMConfig

    config = LLMConfig(
        provider=settings.provider,
        model=settings.model,
        api_key=api_key,
        base_url=settings.base_url,
        reasoning_effort=settings.reasoning_effort,
        max_tokens=settings.max_tokens,
        temperature=settings.temperature,
        timeout=settings.timeout,
        max_concurrency=settings.max_concurrency,
    )
    if not isolated:
        reset_web_search_capability(config)
    client = (
        LLMClient(config, register_health=False, isolated=True)
        if isolated else LLMClient(config)
    )
    try:
        results = client.web_search(
            "中国证监会 官方网站 A股 最新公告",
            timeout=min(30.0, settings.timeout),
            max_uses=1,
        )
    except LLMError as exc:
        if exc.code in {
            "read_timeout", "connect_timeout", "request_timeout", "queue_timeout", "network_error",
        }:
            message = "搜索请求超时或网络不可达"
        elif exc.code == "invalid_response":
            message = "网关返回了无法解析的搜索响应"
        elif exc.code == "response_failed":
            message = "搜索服务返回了失败事件"
        elif exc.code == "response_incomplete":
            message = "搜索响应未完整结束"
        else:
            message = "搜索能力检测失败"
        if not isolated:
            record_llm_failure(client.config, exc)
        return _result("warning", message, started, supported=None, sources=[],
                       **llm_diagnostic_details(client.config, exc))

    capability = client.web_search_status()
    supported = capability.get("supported") is True
    sources = [
        {"title": item.get("title", "")[:120], "url": item.get("url", "")[:2048]}
        for item in results[:5]
    ]
    if supported:
        message = f"原生联网搜索可用，本次返回 {len(results)} 个可引用来源"
        return _result("success", message, started, supported=True, sources=sources)
    return _result(
        "warning",
        "当前网关未通过原生搜索探测；5 分钟后会自动重新探测",
        started,
        supported=False,
        sources=[],
    )


def check_tushare(token: str) -> dict[str, Any]:
    started = time.perf_counter()
    if not token:
        return _result(
            "error", "尚未配置 Tushare Token", started,
            provider_status="auth_invalid", capability="provider_auth",
        )
    if importlib.util.find_spec("tushare") is None:
        return _result(
            "error", "缺少 tushare 依赖，请安装 quantmaster[tushare]", started,
            category="missing-dependency", provider_status="capability_missing",
            capability="sdk", capability_reason="dependency_missing",
        )
    try:
        import tushare as ts

        pro = ts.pro_api(token)
        frame = pro.trade_cal(exchange="SSE", start_date="20240102", end_date="20240103")
        from quantmaster.data.resilience import PROVIDER_HEALTH

        PROVIDER_HEALTH.success("tushare:trade_cal")
        return _result(
            "success", f"Token 可用，trade_cal 返回 {len(frame)} 个交易日历记录", started,
            provider_status="available", capability="trade_cal",
        )
    except Exception as exc:  # tushare 将服务端错误包装为通用 Exception
        from quantmaster.data.resilience import (
            PROVIDER_HEALTH,
            classify_provider_failure,
        )
        from quantmaster.rotation.status import canonical_provider_status

        failure = classify_provider_failure(exc)
        provider_status = canonical_provider_status(failure)
        PROVIDER_HEALTH.failure("tushare:trade_cal", exc, immediate=True)
        if provider_status == "auth_invalid":
            category, message, status = "invalid-token", "Token 无效", "error"
        elif provider_status == "permission_missing":
            category, message, status = (
                "permission", "Token 已被识别，但当前账号缺少 trade_cal 接口权限", "warning"
            )
        elif provider_status == "capability_missing":
            category, message, status = (
                "capability", "当前 tushare SDK 缺少 trade_cal 能力", "warning"
            )
        else:
            category, message, status = (
                provider_status, "Tushare 网络或服务检测失败；其他接口状态保持独立", "warning"
            )
        return _result(
            status, message, started, category=category,
            provider_status=provider_status, capability="trade_cal",
        )


def check_storage(data: DataSettings) -> dict[str, Any]:
    started = time.perf_counter()
    root = Path(data.root).expanduser().resolve()
    if not root.exists():
        parent = root.parent
        while not parent.exists() and parent != parent.parent:
            parent = parent.parent
        writable = os.access(parent, os.W_OK)
        return _result(
            "success" if writable else "error",
            "父目录可写；保存或迁移时才会创建候选目录" if writable else "父目录不可写",
            started,
            path=str(root),
            exists=False,
        )
    try:
        with tempfile.NamedTemporaryFile(prefix=".qm-write-test-", dir=root, delete=True) as handle:
            handle.write(b"quantmaster")
            handle.flush()
            __import__("os").fsync(handle.fileno())
        usage = __import__("shutil").disk_usage(root)
        return _result("success", "目录可写", started, path=str(root), free_bytes=usage.free)
    except OSError:
        logger.info("设置检查发现数据目录不可写", exc_info=True)
        return _result("error", "目录不可写，请检查本机权限", started, path=str(root))


def _check_free_stockdb(data: DataSettings | None, timeout: float) -> dict[str, Any]:
    if data is None:
        return {}
    try:
        from quantmaster.data.free_stockdb_source import FreeStockDBSource

        details = FreeStockDBSource(
            data.free_stockdb_url,
            min(float(timeout), data.free_stockdb_timeout),
            data.free_stockdb_sdk_path,
        ).probe()
        engine = str(details.get("engine") or "free-stockdb")
        return {
            "free-stockdb": {
                "status": "success", "message": f"本地服务可用（{engine}）",
                "engine": engine,
                "sdk_path": str(details.get("sdk_path") or ""),
                "service_url": str(details.get("service_url") or data.free_stockdb_url),
            },
        }
    except (httpx.HTTPError, ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
        return {
            "free-stockdb": {
                "status": "warning",
                "message": f"本地服务未运行，将自动降级（{type(exc).__name__}）",
            },
        }


def _check_free_stockdb_online(data: DataSettings | None, timeout: float) -> dict[str, Any]:
    if data is None:
        return {}
    if not data.free_stockdb_online_enabled:
        return {
            "free-stockdb-online": {
                "status": "success",
                "message": "已在设置中关闭",
                "category": "disabled-by-user",
            },
        }
    try:
        from quantmaster.data.free_stockdb_source import FreeStockDBOnlineSource

        details = FreeStockDBOnlineSource().probe()
        return {
            "free-stockdb-online": {
                "status": "success",
                "message": "公开测试服务可用（仅小批交互补缺）",
                "engine": str(details.get("engine") or "free-stockdb"),
                "service_url": str(details.get("service_url") or data.free_stockdb_online_url),
            },
        }
    except (httpx.HTTPError, ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
        return {
            "free-stockdb-online": {
                "status": "warning",
                "message": f"公开测试服务不可用，将保留本地数据（{type(exc).__name__}）",
                "category": "network",
            },
        }


def _connectivity_probes(
    data: DataSettings | None,
) -> tuple[dict[str, tuple[str, str]], dict[str, Any]]:
    probes = {
        "akshare": (
            "akshare:eastmoney",
            "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            "?secid=1.000001&klt=101&fqt=1&lmt=1&fields1=f1&fields2=f51,f52",
        ),
        "yfinance": (
            "yahoo",
            "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC"
            "?range=1d&interval=1d",
        ),
    }
    disabled: dict[str, Any] = {}
    switches = {
        "akshare": True if data is None else data.akshare_enabled,
        "yfinance": True if data is None else data.yfinance_enabled,
    }
    for provider, enabled in switches.items():
        if enabled:
            continue
        probes.pop(provider)
        disabled[provider] = {
            "status": "success", "message": "已在设置中关闭",
            "category": "disabled-by-user",
        }
    return probes, disabled


def check_data_sources(
    timeout: float = 8.0,
    data: DataSettings | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    probes, sources = _connectivity_probes(data)

    def probe(package: str, lane: str, url: str) -> tuple[str, dict[str, str]]:
        try:
            from quantmaster.data.resilience import provider_call

            def request_endpoint() -> httpx.Response:
                response = httpx.get(url, timeout=timeout, follow_redirects=True)
                if response.status_code >= 400:
                    response.raise_for_status()
                return response

            response = provider_call(
                lane, "settings-connectivity-probe",
                request_endpoint,
                probe=True,
            )
            return package, {
                "status": "success",
                "message": f"真实行情端点可达（HTTP {response.status_code}）",
            }
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            if code == 429:
                message, category, status = "真实行情端点限流（HTTP 429）", "rate-limit", "warning"
            elif code == 401:
                message, category, status = "真实行情端点认证失败（HTTP 401）", "authentication", "error"
            elif code == 403:
                message, category, status = "真实行情端点拒绝访问（HTTP 403）", "permission", "error"
            elif code >= 500:
                message, category, status = (
                    f"真实行情上游异常（HTTP {code}）", "upstream-5xx", "warning"
                )
            else:
                message, category, status = (
                    f"真实行情端点拒绝请求（HTTP {code}）", "http-error", "warning"
                )
            return package, {"status": status, "message": message, "category": category}
        except Exception as exc:
            return package, {
                "status": "warning",
                "message": f"依赖已安装，但行情网络不可达（{type(exc).__name__}）",
                "category": "network",
            }

    with ThreadPoolExecutor(max_workers=len(probes)) as pool:
        futures = []
        for package, (lane, url) in probes.items():
            if importlib.util.find_spec(package) is None:
                sources[package] = {
                    "status": "error",
                    "message": "可选数据依赖未安装；运行 uv sync --extra data 后重启",
                    "category": "missing-dependency",
                }
            else:
                futures.append(pool.submit(probe, package, lane, url))
        for future in as_completed(futures):
            package, result = future.result()
            sources[package] = result

    sources.update(_check_free_stockdb(data, timeout))
    sources.update(_check_free_stockdb_online(data, timeout))
    if data is not None and not data.tushare_enabled:
        sources["tushare"] = {
            "status": "success", "message": "已在设置中关闭",
            "category": "disabled-by-user",
        }

    from quantmaster.data.instruments import instrument_diagnostics
    from quantmaster.data.resilience import PROVIDER_HEALTH

    proxies: dict[str, str] = {}
    for name in ("HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY"):
        raw = os.environ.get(name) or os.environ.get(name.lower())
        if not raw:
            continue
        parsed = urlsplit(raw if "://" in raw else f"http://{raw}")
        host = parsed.hostname or "已配置"
        try:
            port = parsed.port
        except ValueError:
            port = None
        proxies[name] = f"{parsed.scheme}://{host}{f':{port}' if port else ''}"
    overall = "success" if all(v["status"] == "success" for v in sources.values()) else "warning"
    return _result(
        overall, "数据源依赖、真实行情端点与熔断状态检测完成", started,
        sources=sources, circuits=PROVIDER_HEALTH.status(), proxies=proxies,
        security_master=instrument_diagnostics(),
    )


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
    except OSError:
        logger.info("设置检查无法绑定服务端口", exc_info=True)
        status, message, available = "warning", "地址合法，但端口当前不可绑定", False
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
