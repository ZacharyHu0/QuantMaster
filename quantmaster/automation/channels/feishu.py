from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from collections.abc import Callable

from quantmaster.automation.models import ActorContext
from quantmaster.automation.store import AutomationStore
from quantmaster.config import get_config
from quantmaster.credentials import CredentialStore


class FeishuBotClient:
    """飞书企业自建应用 Bot：长连接收消息，OpenAPI 发送消息。"""

    def __init__(self, store: AutomationStore, credentials: CredentialStore | None = None):
        self.store = store
        self.credentials = credentials or CredentialStore()

    def configure(self, app_id: str, app_secret: str) -> dict:
        app_id, app_secret = app_id.strip(), app_secret.strip()
        if not app_id or not app_secret:
            raise ValueError("飞书 App ID 和 App Secret 均不能为空")
        secret_target = CredentialStore.feishu_target(app_id)
        self.credentials.set(secret_target, app_secret)
        account = self.store.save_bot_account(
            channel="feishu", account_id=app_id, base_url="https://open.feishu.cn",
            secret_target=secret_target, status="configured",
        )
        self.store.clear_channel_removal_marker("feishu")
        return account

    def bootstrap_legacy(self) -> dict | None:
        """仅在账号库为空时接纳旧 YAML/环境变量配置。"""
        if (self.store.bot_account("feishu") or
                self.store.channel_credentials_removed("feishu")):
            return None
        app_id = get_config().automation.feishu_app_id.strip()
        secret = os.environ.get("QM_FEISHU_APP_SECRET", "").strip()
        if not app_id or not secret:
            return None
        return self.store.save_bot_account(
            channel="feishu", account_id=app_id, base_url="https://open.feishu.cn",
            secret_target="", status="configured",
        )

    @staticmethod
    def verify(app_id: str, app_secret: str, timeout: float = 10.0) -> dict:
        """只验证应用凭据，不启动第二条长连接。"""
        import httpx

        started = time.perf_counter()
        try:
            response = httpx.post(
                "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                json={"app_id": app_id.strip(), "app_secret": app_secret.strip()},
                timeout=timeout,
            )
            payload = response.json()
            valid = response.is_success and int(payload.get("code", -1)) == 0
            return {
                "status": "success" if valid else "error",
                "message": "App ID / App Secret 有效" if valid else "App ID 或 App Secret 无效",
                "latency_ms": round((time.perf_counter() - started) * 1000),
            }
        except (httpx.HTTPError, ValueError):
            return {
                "status": "warning", "message": "飞书联网验证失败；凭据仍可保存后重试",
                "latency_ms": round((time.perf_counter() - started) * 1000),
            }

    def credentials_value(self) -> tuple[str, str]:
        account = self.store.bot_account("feishu")
        if account and account.get("secret_target"):
            app_id = account["account_id"]
            secret = self.credentials.get(account["secret_target"]) or ""
        else:
            app_id = (account or {}).get("account_id") or get_config().automation.feishu_app_id
            secret = os.environ.get("QM_FEISHU_APP_SECRET", "")
        if not app_id or not secret:
            raise RuntimeError("飞书应用 Bot 尚未配置 App ID/App Secret")
        return app_id, secret

    def _client(self):
        try:
            import lark_oapi as lark
        except ImportError as exc:
            raise RuntimeError("未安装 lark-oapi，无法使用飞书 Bot") from exc
        app_id, secret = self.credentials_value()
        return lark.Client.builder().app_id(app_id).app_secret(secret).build()

    def send(self, *, chat_id: str, text: str) -> None:
        self._send_message(chat_id=chat_id, msg_type="text", content={"text": text})

    def send_card(self, *, chat_id: str, card: dict) -> None:
        """发送飞书消息卡片；用于主通道的结构化告警。"""
        self._send_message(chat_id=chat_id, msg_type="interactive", content=card)

    def _send_message(self, *, chat_id: str, msg_type: str, content: dict) -> None:
        from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody

        request = CreateMessageRequest.builder().receive_id_type("chat_id").request_body(
            CreateMessageRequestBody.builder().receive_id(chat_id).msg_type(msg_type).content(
                json.dumps(content, ensure_ascii=False)).build()
        ).build()
        response = self._client().im.v1.message.create(request)
        if not response.success():
            raise RuntimeError(f"飞书发送失败 code={response.code}: {response.msg}")

    def listen_forever(self, on_message: Callable[[ActorContext, str], None],
                       stop_event: threading.Event) -> None:
        try:
            from lark_oapi.channel import FeishuChannel
            try:
                from lark_oapi.channel import PolicyConfig
            except ImportError:  # 兼容旧 SDK 及精简测试替身。
                PolicyConfig = None
        except ImportError as exc:
            raise RuntimeError("未安装 lark-oapi，无法启动飞书长连接") from exc
        app_id, secret = self.credentials_value()
        channel = None

        def bot_mention(mention) -> bool:
            identity = getattr(channel, "bot_identity", None) if channel is not None else None
            bot_open_id = str(getattr(identity, "open_id", "") or "")
            mention_open_id = str(getattr(mention, "open_id", "") or "")
            if bot_open_id:
                return bool(mention_open_id and mention_open_id == bot_open_id)
            bot_names = {"quantmaster"}
            identity_name = str(getattr(identity, "name", "") or "").strip().casefold()
            if identity_name:
                bot_names.add(identity_name)
            mention_name = str(getattr(mention, "name", "") or "").strip().casefold()
            return bool(mention_name and mention_name in bot_names)

        async def receive(message) -> None:
            if message.sender.is_bot:
                return
            message_id = str(message.message_id or "").strip()
            chat_type = "direct" if message.chat_type == "p2p" else "group"
            if not message_id or not self.store.claim_inbound(
                    "feishu", message_id, chat_type=chat_type, account_id=app_id):
                return
            text = str(message.content_text or "").strip()
            mentions = list(message.mentions or [])
            mentioned_bot = chat_type == "direct" or any(bot_mention(item) for item in mentions)
            for mention in mentions:
                if chat_type == "group" and not bot_mention(mention):
                    continue
                key = str(getattr(mention, "key", "") or "")
                name = str(getattr(mention, "name", "") or "")
                if key:
                    text = text.replace(key, "").strip()
                if name:
                    text = text.replace(f"@{name}", "").strip()
            if not text:
                return
            actor = ActorContext(
                channel="feishu", target=str(message.chat_id), account_id=app_id,
                chat_type=chat_type, sender_id=str(message.sender_id),
                sender_name=str(message.sender_name or ""),
                message_id=message_id,
                reply_to=str(getattr(getattr(message, "reply", None), "message_id", "") or ""),
                reply_text=str(getattr(getattr(message, "reply", None), "text", "") or ""),
            )
            bound = self.store.target_by_route("feishu", app_id, actor.target)
            if chat_type == "group" and bound:
                self.store.remember_conversation_message(
                    channel="feishu", account_id=app_id, chat_id=actor.target,
                    message_id=message_id, sender_id=actor.sender_id,
                    sender_name=actor.sender_name, text=text, mentioned_bot=mentioned_bot,
                    reply_to=actor.reply_to,
                )
            if chat_type == "group" and not mentioned_bot:
                return
            on_message(actor, text)

        async def serve() -> None:
            nonlocal channel
            # 允许普通群消息进入本地上下文缓存；是否回复由上面的真实 @ 检查决定。
            options = {"app_id": app_id, "app_secret": secret}
            if PolicyConfig is not None:
                options["policy"] = PolicyConfig(
                    require_mention=False, respond_to_mention_all=False)
            channel = FeishuChannel(**options)
            channel.on("message", receive)
            self.store.set_bot_status("feishu", app_id, "connecting")
            try:
                await channel.start_background(timeout=30)
                self.store.set_bot_status("feishu", app_id, "listening")
                await asyncio.to_thread(stop_event.wait)
            finally:
                await channel.stop_background()
                if stop_event.is_set():
                    self.store.set_bot_status("feishu", app_id, "configured")

        asyncio.run(serve())
