"""LLM 客户端测试（不触网：mock httpx）。"""

import httpx
import pytest

from quantmaster.ai.llm import LLMClient, LLMError, parse_json_reply
from quantmaster.config import LLMConfig


class TestParseJsonReply:
    def test_plain_json(self):
        assert parse_json_reply('{"a": 1}') == {"a": 1}

    def test_fenced_json(self):
        assert parse_json_reply('```json\n[{"x": 2}]\n```') == [{"x": 2}]

    def test_json_with_prose(self):
        text = '好的，结果如下：\n[{"expression": "rank(close)"}]\n希望有帮助。'
        assert parse_json_reply(text) == [{"expression": "rank(close)"}]

    def test_invalid_raises(self):
        with pytest.raises((LLMError, ValueError)):
            parse_json_reply("完全不是 JSON")


class TestLLMClient:
    def test_requires_api_key(self, monkeypatch):
        for var in ("ANTHROPIC_API_KEY", "OPENAI_API_KEY", "QM_LLM_API_KEY"):
            monkeypatch.delenv(var, raising=False)
        with pytest.raises(LLMError, match="API key"):
            LLMClient(LLMConfig(api_key=""))

    def test_anthropic_request_format(self, monkeypatch):
        captured = {}

        def fake_post(url, headers=None, json=None, timeout=None):
            captured.update(url=url, headers=headers, payload=json)
            return httpx.Response(
                200, json={"content": [{"type": "text", "text": "回复"}]},
                request=httpx.Request("POST", url),
            )

        monkeypatch.setattr(httpx, "post", fake_post)
        client = LLMClient(LLMConfig(provider="anthropic", api_key="sk-test",
                                     model="claude-sonnet-5"))
        reply = client.chat("你好", system="系统提示")
        assert reply == "回复"
        assert captured["headers"]["x-api-key"] == "sk-test"
        assert captured["payload"]["system"] == "系统提示"
        assert captured["payload"]["messages"][0]["role"] == "user"

    def test_openai_compatible_base_url(self, monkeypatch):
        captured = {}

        def fake_post(url, headers=None, json=None, timeout=None):
            captured["url"] = url
            return httpx.Response(
                200, json={"choices": [{"message": {"content": "ok"}}]},
                request=httpx.Request("POST", url),
            )

        monkeypatch.setattr(httpx, "post", fake_post)
        client = LLMClient(LLMConfig(provider="openai-compatible", api_key="sk",
                                     base_url="https://api.deepseek.com/v1"))
        assert client.chat("hi") == "ok"
        assert captured["url"] == "https://api.deepseek.com/v1/chat/completions"

    def test_error_status_raises(self, monkeypatch):
        def fake_post(url, **kwargs):
            return httpx.Response(429, text="rate limited",
                                  request=httpx.Request("POST", url))

        monkeypatch.setattr(httpx, "post", fake_post)
        client = LLMClient(LLMConfig(provider="openai", api_key="sk"))
        with pytest.raises(LLMError, match="429"):
            client.chat("hi")
