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
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx

from quantmaster.config import LLMConfig, get_config

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
OPENAI_URL = "https://api.openai.com/v1/chat/completions"


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
                target = target.replace(tzinfo=timezone.utc)
            return max(0.0, (target - datetime.now(timezone.utc)).total_seconds())
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
    def __init__(self, config: LLMConfig | None = None):
        self.config = config or get_config().llm
        if not self.config.api_key and self.config.provider != "openai-compatible":
            raise LLMError(
                "未配置 LLM API key。请设置环境变量 ANTHROPIC_API_KEY / OPENAI_API_KEY / "
                "QM_LLM_API_KEY，或在 config.yaml 的 llm.api_key 中配置。"
            )

    # ---- 底层请求 ----

    def _request_anthropic(
        self, messages: list[dict], system: str | None, read_timeout: float,
    ) -> str:
        payload = {
            "model": self.config.model,
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature,
            "messages": messages,
        }
        if system:
            payload["system"] = system
        base = self.config.base_url.rstrip("/") if self.config.base_url else ANTHROPIC_URL
        if self.config.base_url and not base.endswith("/messages"):
            base += "/messages"
        try:
            response = httpx.post(
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
    ) -> str:
        if system:
            messages = [{"role": "system", "content": system}, *messages]
        url = OPENAI_URL
        if self.config.base_url:
            url = self.config.base_url.rstrip("/") + "/chat/completions"
        headers = ({"Authorization": f"Bearer {self.config.api_key}"}
                   if self.config.api_key else {})
        try:
            response = httpx.post(
                url,
                headers=headers,
                json={
                    "model": self.config.model,
                    "max_tokens": self.config.max_tokens,
                    "temperature": self.config.temperature,
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

    # ---- 对外接口 ----

    def chat(
        self,
        prompt: str,
        system: str | None = None,
        history: list[dict] | None = None,
        *,
        timeout: float | None = None,
    ) -> str:
        """单轮/多轮对话，返回纯文本回复。"""
        messages = list(history or [])
        messages.append({"role": "user", "content": prompt})
        read_timeout = max(1.0, float(timeout or self.config.timeout))
        if self.config.provider == "anthropic":
            return self._request_anthropic(messages, system, read_timeout)
        return self._request_openai(messages, system, read_timeout)

    def chat_json(
        self, prompt: str, system: str | None = None, *, timeout: float | None = None,
    ) -> dict | list:
        """要求模型输出 JSON 并解析（自动剥离 markdown 代码块围栏）。"""
        hint = "\n\n只输出合法的 JSON，不要输出任何其他文字、解释或 markdown 围栏。"
        text = self.chat(prompt + hint, system=system, timeout=timeout)
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


def chat(prompt: str, system: str | None = None) -> str:
    """便捷函数。"""
    return LLMClient().chat(prompt, system=system)
