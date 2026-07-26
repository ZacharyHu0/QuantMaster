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

import httpx

from quantmaster.config import LLMConfig, get_config

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
OPENAI_URL = "https://api.openai.com/v1/chat/completions"


class LLMError(RuntimeError):
    pass


class LLMClient:
    def __init__(self, config: LLMConfig | None = None):
        self.config = config or get_config().llm
        if not self.config.api_key:
            raise LLMError(
                "未配置 LLM API key。请设置环境变量 ANTHROPIC_API_KEY / OPENAI_API_KEY / "
                "QM_LLM_API_KEY，或在 config.yaml 的 llm.api_key 中配置。"
            )

    # ---- 底层请求 ----

    def _request_anthropic(self, messages: list[dict], system: str | None) -> str:
        payload = {
            "model": self.config.model,
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature,
            "messages": messages,
        }
        if system:
            payload["system"] = system
        base = self.config.base_url or ANTHROPIC_URL
        response = httpx.post(
            base,
            headers={
                "x-api-key": self.config.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=payload,
            timeout=self.config.timeout,
        )
        if response.status_code != 200:
            raise LLMError(f"Anthropic API 错误 {response.status_code}: {response.text[:500]}")
        data = response.json()
        return "".join(block.get("text", "") for block in data.get("content", []))

    def _request_openai(self, messages: list[dict], system: str | None) -> str:
        if system:
            messages = [{"role": "system", "content": system}, *messages]
        url = OPENAI_URL
        if self.config.base_url:
            url = self.config.base_url.rstrip("/") + "/chat/completions"
        response = httpx.post(
            url,
            headers={"Authorization": f"Bearer {self.config.api_key}"},
            json={
                "model": self.config.model,
                "max_tokens": self.config.max_tokens,
                "temperature": self.config.temperature,
                "messages": messages,
            },
            timeout=self.config.timeout,
        )
        if response.status_code != 200:
            raise LLMError(f"OpenAI 协议 API 错误 {response.status_code}: {response.text[:500]}")
        data = response.json()
        return data["choices"][0]["message"]["content"]

    # ---- 对外接口 ----

    def chat(self, prompt: str, system: str | None = None,
             history: list[dict] | None = None) -> str:
        """单轮/多轮对话，返回纯文本回复。"""
        messages = list(history or [])
        messages.append({"role": "user", "content": prompt})
        if self.config.provider == "anthropic":
            return self._request_anthropic(messages, system)
        return self._request_openai(messages, system)

    def chat_json(self, prompt: str, system: str | None = None) -> dict | list:
        """要求模型输出 JSON 并解析（自动剥离 markdown 代码块围栏）。"""
        hint = "\n\n只输出合法的 JSON，不要输出任何其他文字、解释或 markdown 围栏。"
        text = self.chat(prompt + hint, system=system)
        return parse_json_reply(text)


def parse_json_reply(text: str) -> dict | list:
    """尽力从 LLM 回复中抽出 JSON（容忍 ```json 围栏与前后杂质）。"""
    text = text.strip()
    fence = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    if fence:
        text = fence.group(1).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        # 从第一个 { 或 [ 起截取到最后一个 } 或 ]
        match = re.search(r"([\[{].*[\]}])", text, re.DOTALL)
        if match:
            return json.loads(match.group(1))
        raise LLMError(f"无法从 LLM 回复解析 JSON: {text[:200]}") from e


def chat(prompt: str, system: str | None = None) -> str:
    """便捷函数。"""
    return LLMClient().chat(prompt, system=system)
