"""模型发现协议测试：只读取列表，不发推理请求。"""

from __future__ import annotations

import threading
from typing import ClassVar

import httpx
import pytest

from quantmaster.ai.llm import LLMError
from quantmaster.server.settings_checks import (
    check_data_sources,
    check_lab,
    check_llm_web_search,
    check_storage,
    list_llm_models,
)
from quantmaster.settings import DataSettings, LabSettings, LLMSettings, normalize_api_base


def test_storage_draft_check_does_not_create_candidate_directory(tmp_path):
    target = tmp_path / "missing" / "data"

    result = check_storage(DataSettings(root=str(target)))

    assert result["status"] == "success"
    assert result["diagnostic_id"].startswith("diag-")
    assert result["details"]["exists"] is False
    assert not target.exists()


class FakeClient:
    responses: ClassVar[list] = []
    calls: ClassVar[list] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def get(self, url, headers, **_kwargs):
        self.calls.append((str(url), headers))
        return self.responses.pop(0)


def response(status, payload):
    request = httpx.Request("GET", "https://example.test/v1/models")
    return httpx.Response(status, json=payload, request=request)


def test_openai_model_list(monkeypatch):
    FakeClient.calls = []
    FakeClient.responses = [response(200, {"data": [{"id": "gpt-z"}, {"id": "gpt-a"}]})]
    monkeypatch.setattr("quantmaster.ai.llm.httpx.Client", FakeClient)
    result = list_llm_models(LLMSettings(provider="openai", model="gpt-a"), "secret")
    assert result["status"] == "success"
    assert result["details"]["models"] == ["gpt-a", "gpt-z"]
    assert FakeClient.calls[0][0] == "https://api.openai.com/v1/models"
    assert FakeClient.calls[0][1]["Authorization"] == "Bearer secret"


def test_anthropic_paginates_with_after_id(monkeypatch):
    FakeClient.calls = []
    FakeClient.responses = [
        response(200, {"data": [{"id": "claude-a"}], "has_more": True, "last_id": "cursor 1"}),
        response(200, {"data": [{"id": "claude-b"}], "has_more": False}),
    ]
    monkeypatch.setattr("quantmaster.ai.llm.httpx.Client", FakeClient)
    result = list_llm_models(LLMSettings(provider="anthropic", model="manual"), "secret")
    assert result["details"]["models"] == ["claude-a", "claude-b"]
    assert FakeClient.calls[1][0].endswith(("after_id=cursor%201", "after_id=cursor+1"))
    assert FakeClient.calls[0][1]["anthropic-version"] == "2023-06-01"
    assert result["details"]["selected_present"] is False


def test_compatible_gateway_allows_no_key(monkeypatch):
    FakeClient.calls = []
    FakeClient.responses = [response(200, {"models": [{"id": "local-model"}]})]
    monkeypatch.setattr("quantmaster.ai.llm.httpx.Client", FakeClient)
    settings = LLMSettings(provider="openai-compatible", model="local-model",
                           base_url="http://127.0.0.1:11434/v1/chat/completions")
    result = list_llm_models(settings)
    assert result["status"] == "success"
    assert FakeClient.calls[0] == ("http://127.0.0.1:11434/v1/models", {})


def test_unauthorized_and_legacy_endpoint_normalization(monkeypatch):
    FakeClient.calls = []
    FakeClient.responses = [response(401, {"error": "do not expose"})]
    monkeypatch.setattr("quantmaster.ai.llm.httpx.Client", FakeClient)
    result = list_llm_models(LLMSettings(provider="openai", model="x"), "bad-key")
    assert result["status"] == "error"
    assert result["details"]["http_status"] == 401
    assert "do not expose" not in str(result)
    assert normalize_api_base("openai-compatible", "https://gw.test/v1/models") == "https://gw.test/v1"


def test_web_search_check_forces_reprobe_and_returns_only_safe_sources(monkeypatch):
    reset = []

    class SearchClient:
        def __init__(self, config):
            self.config = config
            assert config.reasoning_effort == "high"

        def web_search(self, query, **kwargs):
            assert "证监会" in query
            return [{
                "title": "证监会公告",
                "url": "https://www.csrc.gov.cn/notice",
                "text": "internal excerpt",
            }]

        def web_search_status(self):
            return {"supported": True}

    monkeypatch.setattr("quantmaster.ai.llm.LLMClient", SearchClient)
    monkeypatch.setattr(
        "quantmaster.ai.llm.reset_web_search_capability",
        lambda config: reset.append(config),
    )
    result = check_llm_web_search(
        LLMSettings(
            provider="openai", model="gpt-search", reasoning_effort="high", timeout=45,
        ),
        "secret",
    )

    assert result["status"] == "success"
    assert result["details"]["supported"] is True
    assert result["details"]["sources"] == [{
        "title": "证监会公告", "url": "https://www.csrc.gov.cn/notice",
    }]
    assert len(reset) == 1
    assert "secret" not in str(result)


def test_web_search_draft_check_uses_fully_isolated_client(monkeypatch):
    reset = []
    constructed = []
    failures = []

    class SearchClient:
        def __init__(self, config, **kwargs):
            self.config = config
            constructed.append(kwargs)

        def web_search(self, _query, **_kwargs):
            return []

        def web_search_status(self):
            return {"supported": False}

    monkeypatch.setattr("quantmaster.ai.llm.LLMClient", SearchClient)
    monkeypatch.setattr(
        "quantmaster.ai.llm.reset_web_search_capability",
        lambda config: reset.append(config),
    )
    monkeypatch.setattr(
        "quantmaster.ai.llm.record_llm_failure",
        lambda config, error: failures.append((config, error)),
    )

    result = check_llm_web_search(
        LLMSettings(provider="openai", model="gpt-search"),
        "draft-secret",
        isolated=True,
    )

    assert result["status"] == "warning"
    assert constructed == [{"register_health": False, "isolated": True}]
    assert reset == []
    assert failures == []


def test_isolated_llm_client_keeps_gate_and_capability_local():
    from quantmaster.ai.llm import LLMClient, llm_gate_status, web_search_capability_status
    from quantmaster.config import LLMConfig

    config = LLMConfig(provider="openai", model="diagnostic", api_key="draft-secret")
    gate_before = llm_gate_status()
    capability_before = web_search_capability_status(config)
    client = LLMClient(config, register_health=False, isolated=True)

    client._remember_web_search(True, "diagnostic-only")

    assert client._request_gate() is not client.__class__(
        config, register_health=False, isolated=True,
    )._request_gate()
    assert client.web_search_status()["supported"] is True
    assert web_search_capability_status(config) == capability_before
    assert llm_gate_status() == gate_before


def test_web_search_check_requires_official_provider_key():
    result = check_llm_web_search(LLMSettings(provider="openai", model="gpt-search"))
    assert result["status"] == "error"
    assert result["details"]["supported"] is False


@pytest.mark.parametrize(("code", "expected_message"), [
    ("read_timeout", "搜索请求超时或网络不可达"),
    ("network_error", "搜索请求超时或网络不可达"),
    ("invalid_response", "网关返回了无法解析的搜索响应"),
    ("response_failed", "搜索服务返回了失败事件"),
    ("response_incomplete", "搜索响应未完整结束"),
    ("auth_error", "搜索能力检测失败"),
])
def test_web_search_check_classifies_errors_without_leaking_details(
    monkeypatch, code, expected_message,
):
    class SearchClient:
        def __init__(self, config):
            self.config = config

        def web_search(self, query, **kwargs):
            raise LLMError(
                "raw upstream error sk-secret-do-not-show",
                code=code,
                retryable=True,
            )

    monkeypatch.setattr("quantmaster.ai.llm.LLMClient", SearchClient)
    monkeypatch.setattr(
        "quantmaster.ai.llm.reset_web_search_capability",
        lambda config: None,
    )

    result = check_llm_web_search(
        LLMSettings(provider="openai", model="gpt-search"),
        "secret",
    )

    assert result["status"] == "warning"
    assert result["message"] == expected_message
    assert result["details"]["supported"] is None
    assert result["details"]["error_code"] == code
    assert "sk-secret-do-not-show" not in str(result)


def test_lab_check_reports_demo_and_missing_custom_pool(tmp_path):
    data = DataSettings(root=str(tmp_path))
    demo = check_lab(LabSettings(universe="demo", device="cpu"), data, "")
    assert demo["status"] == "success"
    assert demo["details"]["checks"]["universe"]["status"] == "success"

    missing = check_lab(LabSettings(universe="research_pool", device="cpu"), data, "")
    assert missing["status"] == "error"
    assert "不存在" in missing["details"]["checks"]["universe"]["message"]


def test_data_source_checks_use_real_endpoints_in_parallel_and_mask_proxy(monkeypatch):
    barrier = threading.Barrier(2, timeout=2)
    calls = []

    class ProbeResponse:
        status_code = 200

    def fake_get(url, **kwargs):
        calls.append(url)
        barrier.wait()
        return ProbeResponse()

    monkeypatch.setattr("quantmaster.server.settings_checks.importlib.util.find_spec", lambda name: object())
    monkeypatch.setattr("quantmaster.server.settings_checks.httpx.get", fake_get)
    monkeypatch.setattr(
        "quantmaster.data.resilience.provider_call",
        lambda lane, key, func, **kwargs: func(),
    )
    monkeypatch.setenv("HTTPS_PROXY", "http://secret-user:secret-pass@proxy.example:8080")
    result = check_data_sources(2)

    assert result["status"] == "success"
    assert len(calls) == 2
    assert any("eastmoney.com/api/qt/stock/kline/get" in url for url in calls)
    assert any("finance.yahoo.com/v8/finance/chart" in url for url in calls)
    assert result["details"]["proxies"]["HTTPS_PROXY"] == "http://proxy.example:8080"
    assert "secret-user" not in str(result) and "secret-pass" not in str(result)


@pytest.mark.parametrize(("http_status", "status", "message"), [
    (200, "success", "可达"),
    (304, "success", "可达"),
    (429, "warning", "限流"),
    (401, "error", "认证失败"),
    (403, "error", "拒绝访问"),
    (503, "warning", "上游异常"),
])
def test_data_source_check_classifies_http_status(
    monkeypatch, http_status, status, message,
):
    def fake_get(url, **_kwargs):
        return httpx.Response(http_status, request=httpx.Request("GET", url))

    monkeypatch.setattr("quantmaster.server.settings_checks.importlib.util.find_spec", lambda _name: object())
    monkeypatch.setattr("quantmaster.server.settings_checks.httpx.get", fake_get)
    monkeypatch.setattr(
        "quantmaster.data.resilience.provider_call",
        lambda _lane, _key, func, **_kwargs: func(),
    )

    result = check_data_sources(2)

    assert {item["status"] for item in result["details"]["sources"].values()} == {status}
    assert all(message in item["message"] for item in result["details"]["sources"].values())


def test_data_source_check_reports_connectivity_failure(monkeypatch):
    def fail_get(_url, **_kwargs):
        raise httpx.ConnectTimeout("timed out")

    monkeypatch.setattr("quantmaster.server.settings_checks.importlib.util.find_spec", lambda _name: object())
    monkeypatch.setattr("quantmaster.server.settings_checks.httpx.get", fail_get)
    monkeypatch.setattr(
        "quantmaster.data.resilience.provider_call",
        lambda _lane, _key, func, **_kwargs: func(),
    )

    result = check_data_sources(2)

    assert all(
        item == {
            "status": "warning",
            "message": "依赖已安装，但行情网络不可达（ConnectTimeout）",
            "category": "network",
        }
        for item in result["details"]["sources"].values()
    )


def test_free_stockdb_connection_failure_degrades_to_warning(monkeypatch):
    from quantmaster.server.settings_checks import _check_free_stockdb

    def fail_probe(_self):
        raise httpx.ConnectError("local stockdb refused the connection")

    monkeypatch.setattr(
        "quantmaster.data.free_stockdb_source.FreeStockDBSource.probe",
        fail_probe,
    )

    result = _check_free_stockdb(DataSettings(), 2)

    assert result["free-stockdb"]["status"] == "warning"
    assert "自动降级" in result["free-stockdb"]["message"]
    assert "ConnectError" in result["free-stockdb"]["message"]


def test_missing_optional_data_dependency_has_install_action(monkeypatch):
    monkeypatch.setattr("quantmaster.server.settings_checks.importlib.util.find_spec", lambda _name: None)

    result = check_data_sources(2)

    akshare = result["details"]["sources"]["akshare"]
    assert akshare["category"] == "missing-dependency"
    assert "uv sync --extra data" in akshare["message"]
