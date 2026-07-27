"""模型发现协议测试：只读取列表，不发推理请求。"""

from __future__ import annotations

from typing import ClassVar

import httpx

from quantmaster.settings import LLMSettings, normalize_api_base
from quantmaster.settings_checks import list_llm_models


class FakeClient:
    responses: ClassVar[list] = []
    calls: ClassVar[list] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def get(self, url, headers):
        self.calls.append((str(url), headers))
        return self.responses.pop(0)


def response(status, payload):
    request = httpx.Request("GET", "https://example.test/v1/models")
    return httpx.Response(status, json=payload, request=request)


def test_openai_model_list(monkeypatch):
    FakeClient.calls = []
    FakeClient.responses = [response(200, {"data": [{"id": "gpt-z"}, {"id": "gpt-a"}]})]
    monkeypatch.setattr("quantmaster.settings_checks.httpx.Client", FakeClient)
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
    monkeypatch.setattr("quantmaster.settings_checks.httpx.Client", FakeClient)
    result = list_llm_models(LLMSettings(provider="anthropic", model="manual"), "secret")
    assert result["details"]["models"] == ["claude-a", "claude-b"]
    assert FakeClient.calls[1][0].endswith(("after_id=cursor%201", "after_id=cursor+1"))
    assert FakeClient.calls[0][1]["anthropic-version"] == "2023-06-01"
    assert result["details"]["selected_present"] is False


def test_compatible_gateway_allows_no_key(monkeypatch):
    FakeClient.calls = []
    FakeClient.responses = [response(200, {"models": [{"id": "local-model"}]})]
    monkeypatch.setattr("quantmaster.settings_checks.httpx.Client", FakeClient)
    settings = LLMSettings(provider="openai-compatible", model="local-model",
                           base_url="http://127.0.0.1:11434/v1/chat/completions")
    result = list_llm_models(settings)
    assert result["status"] == "success"
    assert FakeClient.calls[0] == ("http://127.0.0.1:11434/v1/models", {})


def test_unauthorized_and_legacy_endpoint_normalization(monkeypatch):
    FakeClient.calls = []
    FakeClient.responses = [response(401, {"error": "do not expose"})]
    monkeypatch.setattr("quantmaster.settings_checks.httpx.Client", FakeClient)
    result = list_llm_models(LLMSettings(provider="openai", model="x"), "bad-key")
    assert result["status"] == "error"
    assert result["details"]["http_status"] == 401
    assert "do not expose" not in str(result)
    assert normalize_api_base("openai-compatible", "https://gw.test/v1/models") == "https://gw.test/v1"
