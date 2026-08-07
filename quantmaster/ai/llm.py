"""统一 LLM 客户端：一套接口同时兼容 Anthropic 与 OpenAI 协议。

直接用 httpx 调 REST API（不依赖官方 SDK，安装轻量）：
- provider = "anthropic"          Claude 系列（默认 claude-sonnet-5）
- provider = "openai"             OpenAI GPT 系列
- provider = "openai-compatible"  任何 OpenAI 协议网关：DeepSeek、Qwen(通义)、
                                  Moonshot(Kimi)、智谱 GLM、本地 Ollama/vLLM 等，
                                  设置 base_url 即可。

配置（config.yaml 或环境变量 QM_LLM_* / ANTHROPIC_API_KEY / OPENAI_API_KEY）：
    llm:
      provider: anthropic
      model: claude-sonnet-5
      api_key: sk-...
      base_url: ""        # openai-compatible 时必填，如 https://api.deepseek.com/v1
      reasoning_effort: medium
"""

from __future__ import annotations

import json
import re
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Literal

import httpx

from quantmaster.config import LLMConfig, get_config

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
OPENAI_URL = "https://api.openai.com/v1/chat/completions"

_WEB_SEARCH_CAPABILITIES: dict[tuple[str, str, str], dict[str, Any]] = {}
_WEB_SEARCH_CAPABILITIES_LOCK = threading.RLock()
_WEB_SEARCH_NEGATIVE_TTL_SECONDS = 300.0


class _HTTPClientPool:
    """Reference-counted shared clients, retired safely after config changes."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._current_key: tuple[str, str] | None = None
        self._current: httpx.Client | None = None
        self._references: dict[httpx.Client, int] = {}
        self._retired: set[httpx.Client] = set()

    @contextmanager
    def client(self, key: tuple[str, str]):
        close_now: list[httpx.Client] = []
        with self._lock:
            if self._current is None or self._current_key != key:
                if self._current is not None:
                    self._retired.add(self._current)
                self._current = httpx.Client(follow_redirects=True)
                self._current_key = key
            client = self._current
            self._references[client] = self._references.get(client, 0) + 1
            close_now = [
                value for value in self._retired
                if self._references.get(value, 0) == 0
            ]
            self._retired.difference_update(close_now)
        for value in close_now:
            value.close()
        try:
            yield client
        finally:
            should_close = False
            with self._lock:
                self._references[client] -= 1
                if self._references[client] == 0:
                    self._references.pop(client, None)
                    if client in self._retired:
                        self._retired.remove(client)
                        should_close = True
            if should_close:
                client.close()

    def close(self) -> None:
        with self._lock:
            clients = set(self._references) | self._retired
            if self._current is not None:
                clients.add(self._current)
            self._current = None
            self._current_key = None
            self._references.clear()
            self._retired.clear()
        for client in clients:
            client.close()


_HTTP_CLIENT_POOL = _HTTPClientPool()


def close_llm_http_clients() -> None:
    """Release pooled sockets during application shutdown and isolated tests."""
    _HTTP_CLIENT_POOL.close()


class _LLMRequestGate:
    """Process-wide FIFO request gate with bounded queue waits."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._active = 0
        self._waiting: deque[object] = deque()
        self._timeout_count = 0

    @contextmanager
    def slot(self, limit: int, queue_timeout: float):
        value = max(1, int(limit))
        timeout = max(1.0, min(300.0, float(queue_timeout)))
        ticket = object()
        with self._condition:
            self._waiting.append(ticket)
            deadline = time.monotonic() + timeout
            while self._waiting[0] is not ticket or self._active >= value:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._waiting.remove(ticket)
                    self._timeout_count += 1
                    self._condition.notify_all()
                    raise TimeoutError("llm_queue_timeout")
                self._condition.wait(remaining)
            self._waiting.popleft()
            self._active += 1
        try:
            yield
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()

    def status(self) -> dict[str, int]:
        with self._condition:
            return {
                "active": self._active,
                "waiting": len(self._waiting),
                "timeout_count": self._timeout_count,
            }


_LLM_REQUEST_GATE = _LLMRequestGate()
_NEWS_LLM_REQUEST_GATE = _LLMRequestGate()


def llm_gate_status() -> dict[str, int]:
    """Expose non-secret queue pressure for diagnostics."""
    return _LLM_REQUEST_GATE.status()


def news_llm_gate_status() -> dict[str, int]:
    """Expose the independently limited news-annotation queue pressure."""
    return _NEWS_LLM_REQUEST_GATE.status()


def _web_search_capability_key(config: LLMConfig) -> tuple[str, str, str]:
    return config.provider, config.base_url.rstrip("/"), config.model


def reset_web_search_capability(config: LLMConfig | None = None) -> None:
    """Forget one provider probe so an operator can retry after a gateway upgrade."""
    value = config or get_config().llm
    with _WEB_SEARCH_CAPABILITIES_LOCK:
        _WEB_SEARCH_CAPABILITIES.pop(_web_search_capability_key(value), None)


def web_search_capability_status(config: LLMConfig | None = None) -> dict[str, Any]:
    """Read optional native-search status without constructing a credentialed client."""
    value = config or get_config().llm
    key = _web_search_capability_key(value)
    with _WEB_SEARCH_CAPABILITIES_LOCK:
        cached = _WEB_SEARCH_CAPABILITIES.get(key)
        if (cached and cached.get("supported") is False and
                time.monotonic() - float(cached.get("checked_monotonic") or 0.0) >=
                _WEB_SEARCH_NEGATIVE_TTL_SECONDS):
            _WEB_SEARCH_CAPABILITIES.pop(key, None)
            cached = None
    public = dict(cached or {
        "supported": None, "detail": "尚未探测", "checked_at": "",
        "provider": value.provider,
    })
    public.pop("checked_monotonic", None)
    return public


class LLMError(RuntimeError):
    """可安全呈现给任务账本的结构化 LLM 错误。"""

    def __init__(
        self,
        message: str,
        *,
        code: str = "llm_error",
        retryable: bool = False,
        status_code: int | None = None,
        retry_after: float | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.status_code = status_code
        self.retry_after = retry_after


_RETRYABLE_STATUSES = {408, 425, 429, 500, 502, 503, 504}


def _retry_after(response: httpx.Response) -> float | None:
    raw = (response.headers.get("Retry-After") or "").strip()
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        try:
            target = parsedate_to_datetime(raw)
            if target.tzinfo is None:
                target = target.replace(tzinfo=UTC)
            return max(0.0, (target - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


def _request_timeout(read_timeout: float) -> httpx.Timeout:
    read_timeout = max(1.0, float(read_timeout))
    return httpx.Timeout(
        read=read_timeout,
        connect=min(15.0, read_timeout),
        write=min(30.0, read_timeout),
        pool=min(15.0, read_timeout),
    )


def _transport_error(exc: httpx.HTTPError, read_timeout: float) -> LLMError:
    if isinstance(exc, httpx.ReadTimeout):
        return LLMError(
            f"模型在 {read_timeout:.0f} 秒内未返回结果",
            code="read_timeout",
            retryable=True,
        )
    if isinstance(exc, httpx.ConnectTimeout):
        return LLMError("连接模型服务超时", code="connect_timeout", retryable=True)
    if isinstance(exc, httpx.TimeoutException):
        return LLMError("模型请求超时", code="request_timeout", retryable=True)
    return LLMError(
        f"无法连接模型服务：{type(exc).__name__}",
        code="network_error",
        retryable=True,
    )


def _api_error(provider: str, response: httpx.Response) -> LLMError:
    status = int(response.status_code)
    retryable = status in _RETRYABLE_STATUSES
    label = "暂时不可用" if retryable else "请求失败"
    detail = response.text.strip().replace("\n", " ")[:500]
    message = f"{provider} API {label}（HTTP {status}）"
    if detail:
        message += f"：{detail}"
    return LLMError(
        message,
        code=f"http_{status}",
        retryable=retryable,
        status_code=status,
        retry_after=_retry_after(response),
    )


class LLMClient:
    def __init__(
        self,
        config: LLMConfig | None = None,
        *,
        concurrency_scope: Literal["global", "news"] = "global",
    ):
        self._uses_runtime_config = config is None
        self.config = config or get_config().llm
        self._concurrency_scope = concurrency_scope
        if not self.config.api_key and self.config.provider != "openai-compatible":
            raise LLMError(
                "未配置 LLM API key。请设置环境变量 ANTHROPIC_API_KEY / OPENAI_API_KEY / "
                "QM_LLM_API_KEY，或在 config.yaml 的 llm.api_key 中配置。"
            )

    def _max_concurrency(self) -> int:
        if self._concurrency_scope == "news":
            return max(
                1, min(16, int(get_config().news.annotation_max_concurrency)),
            )
        config = get_config().llm if self._uses_runtime_config else self.config
        return max(1, int(config.max_concurrency))

    def _request_gate(self) -> _LLMRequestGate:
        if self._concurrency_scope == "news":
            return _NEWS_LLM_REQUEST_GATE
        return _LLM_REQUEST_GATE

    def _queue_timeout(self) -> float:
        config = get_config().llm if self._uses_runtime_config else self.config
        return max(1.0, min(300.0, float(config.queue_timeout)))

    def _post(self, url: str, **kwargs: Any) -> httpx.Response:
        try:
            with self._request_gate().slot(self._max_concurrency(), self._queue_timeout()):
                key = (self.config.provider, self.config.base_url.rstrip("/"))
                with _HTTP_CLIENT_POOL.client(key) as client:
                    return client.post(url, **kwargs)
        except TimeoutError as exc:
            raise LLMError(
                f"模型请求排队超过 {self._queue_timeout():.0f} 秒",
                code="queue_timeout", retryable=True,
            ) from exc

    # ---- 底层请求 ----

    def _request_anthropic(
        self, messages: list[dict], system: str | None, read_timeout: float,
        reasoning_effort: str, model: str,
    ) -> str:
        payload = {
            "model": model,
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature,
            "output_config": {"effort": reasoning_effort},
            "messages": messages,
        }
        if system:
            payload["system"] = system
        base = self.config.base_url.rstrip("/") if self.config.base_url else ANTHROPIC_URL
        if self.config.base_url and not base.endswith("/messages"):
            base += "/messages"
        try:
            response = self._post(
                base,
                headers={
                    "x-api-key": self.config.api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json=payload,
                timeout=_request_timeout(read_timeout),
            )
        except httpx.HTTPError as exc:
            raise _transport_error(exc, read_timeout) from exc
        if response.status_code != 200:
            raise _api_error("Anthropic", response)
        try:
            data = response.json()
        except ValueError as exc:
            raise LLMError(
                "Anthropic API 返回了无法解析的响应",
                code="invalid_response",
                retryable=True,
            ) from exc
        return "".join(block.get("text", "") for block in data.get("content", []))

    def _request_openai(
        self, messages: list[dict], system: str | None, read_timeout: float,
        reasoning_effort: str, model: str,
    ) -> str:
        if system:
            messages = [{"role": "system", "content": system}, *messages]
        url = OPENAI_URL
        if self.config.base_url:
            url = self.config.base_url.rstrip("/") + "/chat/completions"
        headers = ({"Authorization": f"Bearer {self.config.api_key}"}
                   if self.config.api_key else {})
        try:
            response = self._post(
                url,
                headers=headers,
                json={
                    "model": model,
                    "max_tokens": self.config.max_tokens,
                    "temperature": self.config.temperature,
                    "reasoning_effort": reasoning_effort,
                    "messages": messages,
                },
                timeout=_request_timeout(read_timeout),
            )
        except httpx.HTTPError as exc:
            raise _transport_error(exc, read_timeout) from exc
        if response.status_code != 200:
            raise _api_error("OpenAI 协议", response)
        try:
            data = response.json()
            return data["choices"][0]["message"]["content"]
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise LLMError(
                "OpenAI 协议 API 返回了无法解析的响应",
                code="invalid_response",
                retryable=True,
            ) from exc

    def _capability_key(self) -> tuple[str, str, str]:
        return _web_search_capability_key(self.config)

    def _remember_web_search(self, supported: bool, detail: str = "") -> None:
        with _WEB_SEARCH_CAPABILITIES_LOCK:
            _WEB_SEARCH_CAPABILITIES[self._capability_key()] = {
                "supported": bool(supported),
                "detail": str(detail)[:500],
                "checked_at": datetime.now(UTC).isoformat(),
                "checked_monotonic": time.monotonic(),
                "provider": self.config.provider,
            }

    def web_search_status(self) -> dict[str, Any]:
        """Return the process-cached optional search capability for diagnostics."""
        return web_search_capability_status(self.config)

    @staticmethod
    def _search_result(
        url: Any, title: Any = "", text: Any = "", published_at: Any = "",
    ) -> dict[str, str] | None:
        value = str(url or "").strip()
        if not re.match(r"^https?://", value, re.IGNORECASE):
            return None
        return {
            "url": value[:2048],
            "title": str(title or value)[:300],
            "text": str(text or "")[:1500],
            "published_at": str(published_at or "")[:80],
        }

    @classmethod
    def _openai_search_results(cls, payload: dict[str, Any]) -> list[dict[str, str]]:
        results: list[dict[str, str]] = []
        for item in payload.get("output") or []:
            if item.get("type") == "web_search_call":
                action = item.get("action") or {}
                for source in action.get("sources") or []:
                    result = cls._search_result(source.get("url"), source.get("title"))
                    if result:
                        results.append(result)
            if item.get("type") != "message":
                continue
            for content in item.get("content") or []:
                if content.get("type") != "output_text":
                    continue
                output_text = str(content.get("text") or "")
                for annotation in content.get("annotations") or []:
                    citation = annotation.get("url_citation") or annotation
                    if annotation.get("type") != "url_citation" and not citation.get("url"):
                        continue
                    start = int(citation.get("start_index") or 0)
                    end = int(citation.get("end_index") or 0)
                    excerpt = output_text[max(0, start - 180):min(len(output_text), end + 180)]
                    result = cls._search_result(
                        citation.get("url"), citation.get("title"), excerpt,
                    )
                    if result:
                        results.append(result)
        return _dedupe_search_results(results)

    @classmethod
    def _anthropic_search_results(cls, payload: dict[str, Any]) -> list[dict[str, str]]:
        results: list[dict[str, str]] = []
        for block in payload.get("content") or []:
            block_type = str(block.get("type") or "")
            if block_type in {"web_search_tool_result", "web_search_result"}:
                values = block.get("content") or block.get("results") or []
                if isinstance(values, dict):
                    values = values.get("results") or [values]
                for value in values:
                    if not isinstance(value, dict):
                        continue
                    result = cls._search_result(
                        value.get("url"), value.get("title"),
                        value.get("snippet") or value.get("content"),
                        value.get("page_age") or value.get("published_at"),
                    )
                    if result:
                        results.append(result)
            if block_type != "text":
                continue
            text = str(block.get("text") or "")
            for citation in block.get("citations") or []:
                result = cls._search_result(
                    citation.get("url"), citation.get("title"),
                    citation.get("cited_text") or text,
                    citation.get("page_age") or citation.get("published_at"),
                )
                if result:
                    results.append(result)
        return _dedupe_search_results(results)

    def _web_search_openai(
        self, query: str, *, timeout: float, max_uses: int,
    ) -> list[dict[str, str]]:
        base = self.config.base_url.rstrip("/") if self.config.base_url else "https://api.openai.com/v1"
        if base.endswith("/chat/completions"):
            base = base[:-len("/chat/completions")]
        elif base.endswith("/responses"):
            base = base[:-len("/responses")]
        url = base + "/responses"
        headers = ({"Authorization": f"Bearer {self.config.api_key}"}
                   if self.config.api_key else {})
        rich_payload = {
            "model": self.config.model,
            "input": query,
            "reasoning": {"effort": self.config.reasoning_effort},
            "tools": [{"type": "web_search", "search_context_size": "medium"}],
            "tool_choice": "auto",
            "max_tool_calls": max(1, min(3, int(max_uses))),
            "include": ["web_search_call.action.sources"],
        }
        minimal_payload = {
            "model": self.config.model,
            "input": query,
            "reasoning": {"effort": self.config.reasoning_effort},
            "tools": [{"type": "web_search"}],
        }
        response: Any = None
        for index, payload in enumerate((rich_payload, minimal_payload)):
            try:
                response = self._post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=_request_timeout(timeout),
                )
            except httpx.HTTPError as exc:
                raise _transport_error(exc, timeout) from exc
            if response.status_code == 200:
                break
            if index == 0 and response.status_code in {400, 415, 422}:
                continue
            raise _api_error("OpenAI Responses", response)
        if response is None or response.status_code != 200:
            raise LLMError("OpenAI Responses 搜索探测未返回结果", code="invalid_response")
        try:
            payload = response.json()
        except ValueError as exc:
            raise LLMError(
                "OpenAI Responses 返回了无法解析的搜索响应",
                code="invalid_response", retryable=True,
            ) from exc
        return self._openai_search_results(payload)

    def _web_search_anthropic(
        self, query: str, *, timeout: float, max_uses: int,
    ) -> list[dict[str, str]]:
        base = self.config.base_url.rstrip("/") if self.config.base_url else ANTHROPIC_URL
        if self.config.base_url and not base.endswith("/messages"):
            base += "/messages"
        headers = {
            "x-api-key": self.config.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        tool = {
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": max(1, min(3, int(max_uses))),
        }
        request_payload: dict[str, Any] = {
            "model": self.config.model,
            "max_tokens": self.config.max_tokens,
            "output_config": {"effort": self.config.reasoning_effort},
            "messages": [{"role": "user", "content": query}],
            "tools": [tool],
        }
        for _ in range(2):
            try:
                response = self._post(
                    base, headers=headers, json=request_payload,
                    timeout=_request_timeout(timeout),
                )
            except httpx.HTTPError as exc:
                raise _transport_error(exc, timeout) from exc
            if response.status_code != 200:
                raise _api_error("Anthropic Web Search", response)
            try:
                payload = response.json()
            except ValueError as exc:
                raise LLMError(
                    "Anthropic API 返回了无法解析的搜索响应",
                    code="invalid_response", retryable=True,
                ) from exc
            if payload.get("stop_reason") != "pause_turn":
                return self._anthropic_search_results(payload)
            request_payload["messages"] = [
                {"role": "user", "content": query},
                {"role": "assistant", "content": payload.get("content") or []},
            ]
        return self._anthropic_search_results(payload)

    def web_search(
        self, query: str, *, timeout: float = 30.0, max_uses: int = 1,
    ) -> list[dict[str, str]]:
        """Use the provider's native search when available; unsupported gateways degrade once."""
        value = str(query or "").strip()
        if not value:
            return []
        cached = self.web_search_status()
        if cached.get("supported") is False:
            return []
        try:
            if self.config.provider == "anthropic":
                results = self._web_search_anthropic(value, timeout=timeout, max_uses=max_uses)
            else:
                results = self._web_search_openai(value, timeout=timeout, max_uses=max_uses)
        except LLMError as exc:
            if exc.status_code in {400, 404, 405, 415, 422}:
                self._remember_web_search(
                    False,
                    f"原生搜索探测未通过（HTTP {exc.status_code}），5 分钟后自动重试",
                )
                return []
            raise
        self._remember_web_search(True, f"返回 {len(results)} 个可引用来源")
        return results

    # ---- 对外接口 ----

    def chat(
        self,
        prompt: str,
        system: str | None = None,
        history: list[dict] | None = None,
        *,
        timeout: float | None = None,
        reasoning_effort: str | None = None,
        model: str | None = None,
    ) -> str:
        """单轮/多轮对话，返回纯文本回复。"""
        messages = list(history or [])
        messages.append({"role": "user", "content": prompt})
        read_timeout = max(1.0, float(timeout or self.config.timeout))
        effort = str(reasoning_effort or self.config.reasoning_effort)
        selected_model = str(model or self.config.model).strip()
        if self.config.provider == "anthropic":
            return self._request_anthropic(
                messages, system, read_timeout, effort, selected_model,
            )
        return self._request_openai(
            messages, system, read_timeout, effort, selected_model,
        )

    def chat_json(
        self, prompt: str, system: str | None = None, *, timeout: float | None = None,
        reasoning_effort: str | None = None, model: str | None = None,
    ) -> dict | list:
        """要求模型输出 JSON 并解析（自动剥离 markdown 代码块围栏）。"""
        hint = "\n\n只输出合法的 JSON，不要输出任何其他文字、解释或 markdown 围栏。"
        text = self.chat(
            prompt + hint, system=system, timeout=timeout,
            reasoning_effort=reasoning_effort, model=model,
        )
        return parse_json_reply(text)


def parse_json_reply(text: str) -> dict | list:
    """尽力从 LLM 回复中抽出 JSON（容忍 ```json 围栏与前后杂质）。"""
    text = text.strip()
    if not text:
        raise LLMError("模型返回了空内容", code="empty_response", retryable=True)
    fence = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    if fence:
        text = fence.group(1).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        # 从第一个 { 或 [ 起截取到最后一个 } 或 ]
        match = re.search(r"([\[{].*[\]}])", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
        raise LLMError(
            f"模型未返回合法 JSON：{text[:200]}",
            code="invalid_json",
            retryable=True,
        ) from e


def _dedupe_search_results(values: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    results: list[dict[str, str]] = []
    for value in values:
        url = value.get("url", "").strip()
        key = url.rstrip("/").casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        results.append(value)
    return results


def chat(prompt: str, system: str | None = None) -> str:
    """便捷函数。"""
    return LLMClient().chat(prompt, system=system)
