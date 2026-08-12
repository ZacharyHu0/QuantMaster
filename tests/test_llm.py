"""LLM 客户端测试（不触网：mock httpx）。"""

import threading

import httpx
import pytest

from quantmaster.ai.llm import (
    LLMClient,
    LLMError,
    _api_error,
    _HTTPClientPool,
    _LLMRequestGate,
    _transport_error,
    llm_diagnostic_details,
    parse_json_reply,
)
from quantmaster.config import LLMConfig


def test_llm_diagnostic_contract_redacts_endpoint_and_response_body():
    config = LLMConfig(
        provider="openai-compatible", model="private-model", api_key="sk-secret-value",
        base_url="https://gateway.example/v1?token=private-query",
    )
    response = httpx.Response(
        429, headers={"x-request-id": "upstream-correlation"},
        text='{"error":"Bearer secret-token prompt=private"}',
    )
    error = _api_error("OpenAI 协议", response)
    details = llm_diagnostic_details(config, error)

    assert error.code == "http_429"
    assert error.category == "rate_limit"
    assert details["diagnostic_id"] == "upstream-correlation"
    assert details["endpoint"] == "https://gateway.example/v1"
    assert "secret" not in repr(details).casefold()
    assert "private-query" not in repr(details)
    assert details["response_summary"] == "上游返回了错误响应（内容已隐藏）"


@pytest.mark.parametrize(("exception", "code", "category"), [
    (httpx.ReadTimeout("read"), "read_timeout", "timeout"),
    (httpx.ConnectTimeout("connect"), "connect_timeout", "timeout"),
    (httpx.WriteTimeout("write"), "write_timeout", "timeout"),
    (httpx.PoolTimeout("pool"), "pool_timeout", "timeout"),
])
def test_llm_transport_timeout_categories(exception, code, category):
    error = _transport_error(exception, 12)
    assert error.code == code
    assert error.category == category
    assert error.retryable is True
    assert error.request_id.startswith("llm-")


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

    def test_local_compatible_gateway_allows_missing_key(self):
        client = LLMClient(LLMConfig(
            provider="openai-compatible", api_key="", model="local",
            base_url="http://127.0.0.1:11434/v1",
        ))
        assert client.config.api_key == ""

    def test_anthropic_request_format(self, monkeypatch):
        captured = {}

        def fake_post(url, headers=None, json=None, timeout=None):
            captured.update(url=url, headers=headers, payload=json, timeout=timeout)
            return httpx.Response(
                200, json={"content": [{"type": "text", "text": "回复"}]},
                request=httpx.Request("POST", url),
            )

        monkeypatch.setattr(
            httpx.Client, "post",
            lambda _client, *args, **kwargs: fake_post(*args, **kwargs),
        )
        client = LLMClient(LLMConfig(provider="anthropic", api_key="sk-test",
                                     model="claude-sonnet-5"))
        reply = client.chat("你好", system="系统提示")
        assert reply == "回复"
        assert captured["headers"]["x-api-key"] == "sk-test"
        assert captured["payload"]["system"] == "系统提示"
        assert captured["payload"]["messages"][0]["role"] == "user"
        assert captured["payload"]["output_config"] == {"effort": "medium"}
        assert captured["timeout"].read == 60

    def test_per_request_read_timeout_and_timeout_error_are_structured(self, monkeypatch):
        captured = {}

        def fake_post(url, **kwargs):
            captured["timeout"] = kwargs["timeout"]
            raise httpx.ReadTimeout("slow", request=httpx.Request("POST", url))

        monkeypatch.setattr(
            httpx.Client, "post",
            lambda _client, *args, **kwargs: fake_post(*args, **kwargs),
        )
        client = LLMClient(LLMConfig(provider="openai", api_key="sk", timeout=60))
        with pytest.raises(LLMError) as caught:
            client.chat("hi", timeout=180)
        assert captured["timeout"].read == 180
        assert caught.value.code == "read_timeout"
        assert caught.value.retryable is True
        assert "180 秒" in str(caught.value)

    def test_openai_compatible_base_url(self, monkeypatch):
        captured = {}

        def fake_post(url, headers=None, json=None, timeout=None):
            captured.update(url=url, payload=json)
            return httpx.Response(
                200, json={"choices": [{"message": {"content": "ok"}}]},
                request=httpx.Request("POST", url),
            )

        monkeypatch.setattr(
            httpx.Client, "post",
            lambda _client, *args, **kwargs: fake_post(*args, **kwargs),
        )
        client = LLMClient(LLMConfig(provider="openai-compatible", api_key="sk",
                                     base_url="https://api.deepseek.com/v1",
                                     reasoning_effort="high"))
        assert client.chat("hi") == "ok"
        assert captured["url"] == "https://api.deepseek.com/v1/chat/completions"
        assert captured["payload"]["reasoning_effort"] == "high"

    def test_invalid_html_response_reports_safe_upstream_summary(self, monkeypatch):
        def fake_post(url, **kwargs):
            return httpx.Response(
                200,
                text=(
                    "<!doctype html><html><head><title>Sub2API - AI API Gateway</title>"
                    "</head><body>sk-secret-must-not-leak</body></html>"
                ),
                headers={"Content-Type": "text/html; charset=utf-8"},
                request=httpx.Request("POST", url),
            )

        monkeypatch.setattr(
            httpx.Client, "post",
            lambda _client, *args, **kwargs: fake_post(*args, **kwargs),
        )
        client = LLMClient(LLMConfig(
            provider="openai-compatible", base_url="https://gateway.test/v1",
        ))

        with pytest.raises(LLMError) as caught:
            client.chat("hi")

        message = str(caught.value)
        assert caught.value.code == "invalid_response"
        assert "HTTP 200" in message
        assert "text/html" in message
        assert "Sub2API - AI API Gateway" in message
        assert "secret-must-not-leak" not in message

    def test_root_gateway_falls_back_to_v1_after_html(self, monkeypatch):
        calls = []

        def fake_post(url, **kwargs):
            calls.append(url)
            if url.endswith("/v1/chat/completions"):
                return httpx.Response(
                    200, json={"choices": [{"message": {"content": "ok"}}]},
                    request=httpx.Request("POST", url),
                )
            return httpx.Response(
                200, text="<title>Gateway Console</title>",
                headers={"Content-Type": "text/html"},
                request=httpx.Request("POST", url),
            )

        monkeypatch.setattr(
            httpx.Client, "post",
            lambda _client, *args, **kwargs: fake_post(*args, **kwargs),
        )
        client = LLMClient(LLMConfig(
            provider="openai-compatible", base_url="https://gateway.test",
        ))

        assert client.chat("hi") == "ok"
        assert calls == [
            "https://gateway.test/chat/completions",
            "https://gateway.test/v1/chat/completions",
        ]

    def test_per_request_model_override(self, monkeypatch):
        captured = {}

        def fake_post(url, **kwargs):
            captured.update(kwargs["json"])
            return httpx.Response(
                200, json={"choices": [{"message": {"content": "{}"}}]},
                request=httpx.Request("POST", url),
            )

        monkeypatch.setattr(
            httpx.Client, "post",
            lambda _client, *args, **kwargs: fake_post(*args, **kwargs),
        )
        client = LLMClient(LLMConfig(provider="openai", api_key="sk", model="large"))
        assert client.chat_json("extract", model="small", reasoning_effort="minimal") == {}
        assert captured["model"] == "small"
        assert captured["reasoning_effort"] == "minimal"

    def test_error_status_raises(self, monkeypatch):
        def fake_post(url, **kwargs):
            return httpx.Response(429, text="rate limited", headers={"Retry-After": "75"},
                                  request=httpx.Request("POST", url))

        monkeypatch.setattr(
            httpx.Client, "post",
            lambda _client, *args, **kwargs: fake_post(*args, **kwargs),
        )
        client = LLMClient(LLMConfig(provider="openai", api_key="sk"))
        with pytest.raises(LLMError, match="429") as caught:
            client.chat("hi")
        assert caught.value.retryable is True
        assert caught.value.status_code == 429
        assert caught.value.code == "http_429"
        assert caught.value.retry_after == 75

    def test_global_concurrency_limit_serializes_clients(self, monkeypatch):
        import threading
        import time
        from concurrent.futures import ThreadPoolExecutor

        active = maximum = 0
        lock = threading.Lock()

        def fake_post(url, **kwargs):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            time.sleep(0.04)
            with lock:
                active -= 1
            return httpx.Response(
                200, json={"choices": [{"message": {"content": "ok"}}]},
                request=httpx.Request("POST", url),
            )

        monkeypatch.setattr(
            httpx.Client, "post",
            lambda _client, *args, **kwargs: fake_post(*args, **kwargs),
        )
        config = LLMConfig(provider="openai", api_key="sk", max_concurrency=1)
        clients = [LLMClient(config), LLMClient(config)]
        with ThreadPoolExecutor(max_workers=2) as executor:
            assert list(executor.map(lambda client: client.chat("hi"), clients)) == ["ok", "ok"]
        assert maximum == 1

    def test_news_concurrency_uses_an_independent_gate(
        self, monkeypatch, isolated_config,
    ):
        import threading
        import time
        from concurrent.futures import ThreadPoolExecutor

        isolated_config.news.annotation_max_concurrency = 2
        active = {"global": 0, "news": 0}
        maximum = {"global": 0, "news": 0, "combined": 0}
        lock = threading.Lock()

        def fake_post(url, **kwargs):
            scope = kwargs["json"]["model"]
            with lock:
                active[scope] += 1
                maximum[scope] = max(maximum[scope], active[scope])
                maximum["combined"] = max(maximum["combined"], sum(active.values()))
            time.sleep(0.05)
            with lock:
                active[scope] -= 1
            return httpx.Response(
                200, json={"choices": [{"message": {"content": "ok"}}]},
                request=httpx.Request("POST", url),
            )

        monkeypatch.setattr(
            httpx.Client, "post",
            lambda _client, *args, **kwargs: fake_post(*args, **kwargs),
        )
        global_config = LLMConfig(
            provider="openai", api_key="sk", model="global", max_concurrency=1,
        )
        news_config = LLMConfig(
            provider="openai", api_key="sk", model="news", max_concurrency=1,
        )
        clients = [
            LLMClient(global_config), LLMClient(global_config),
            LLMClient(news_config, concurrency_scope="news"),
            LLMClient(news_config, concurrency_scope="news"),
        ]
        with ThreadPoolExecutor(max_workers=4) as executor:
            assert list(executor.map(lambda client: client.chat("hi"), clients)) == [
                "ok", "ok", "ok", "ok",
            ]
        assert maximum == {"global": 1, "news": 2, "combined": 3}


def test_request_gate_is_fifo_and_queue_wait_is_bounded():
    import threading
    from concurrent.futures import ThreadPoolExecutor

    gate = _LLMRequestGate()
    order = []
    first_waiting = threading.Event()
    second_waiting = threading.Event()

    def waiter(name, ready):
        ready.set()
        with gate.slot(1, 5):
            order.append(name)

    with ThreadPoolExecutor(max_workers=2) as executor:
        with gate.slot(1, 5):
            first = executor.submit(waiter, "first", first_waiting)
            assert first_waiting.wait(1)
            second = executor.submit(waiter, "second", second_waiting)
            assert second_waiting.wait(1)
        first.result(timeout=2)
        second.result(timeout=2)
    assert order == ["first", "second"]
    assert gate.status() == {"active": 0, "waiting": 0, "timeout_count": 0}

    with gate.slot(1, 5), pytest.raises(TimeoutError, match="llm_queue_timeout"):
        gate.slot(1, 1).__enter__()
    assert gate.status()["timeout_count"] == 1


def test_http_pool_retires_hot_replaced_client_after_inflight_request(monkeypatch):
    clients = []

    class FakeClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.closed = 0
            clients.append(self)

        def close(self):
            self.closed += 1

    monkeypatch.setattr("quantmaster.ai.llm.httpx.Client", FakeClient)
    pool = _HTTPClientPool()
    with pool.client(("openai", "https://one")) as first:
        with pool.client(("openai", "https://two")) as second:
            assert first.closed == 0
            assert second.closed == 0
        assert first.closed == 0
    assert first.closed == 1
    pool.close()
    assert second.closed == 1


def test_http_pool_close_defers_inflight_client_until_context_exit(monkeypatch):
    clients = []

    class FakeClient:
        def __init__(self, **kwargs):
            self.closed = 0
            clients.append(self)

        def close(self):
            self.closed += 1

    monkeypatch.setattr("quantmaster.ai.llm.httpx.Client", FakeClient)
    pool = _HTTPClientPool()
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    failures = []

    def borrow_client():
        try:
            with pool.client(("openai", "https://one")):
                entered.set()
                assert release.wait(1)
        except BaseException as exc:  # Assert context-manager cleanup also succeeds.
            failures.append(exc)
        finally:
            finished.set()

    borrower = threading.Thread(target=borrow_client)
    borrower.start()
    assert entered.wait(1)

    closer = threading.Thread(target=pool.close)
    closer.start()
    closer.join(timeout=1)
    assert not closer.is_alive()
    assert clients[0].closed == 0

    release.set()
    assert finished.wait(1)
    borrower.join(timeout=1)
    assert not borrower.is_alive()
    assert failures == []
    assert clients[0].closed == 1
    pool.close()
    assert clients[0].closed == 1
