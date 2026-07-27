from __future__ import annotations

import asyncio
import json
import os
import threading
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
        return self.store.save_bot_account(
            channel="feishu", account_id=app_id, base_url="https://open.feishu.cn",
            secret_target=secret_target, status="configured",
        )

    def credentials_value(self) -> tuple[str, str]:
        env_app_id = os.environ.get("QM_FEISHU_APP_ID", "").strip()
        account = self.store.bot_account("feishu", env_app_id or None)
        if env_app_id:
            app_id = env_app_id
        elif account:
            app_id = account["account_id"]
        else:
            app_id = get_config().automation.feishu_app_id
            account = self.store.bot_account("feishu", app_id or None)
        secret = os.environ.get("QM_FEISHU_APP_SECRET", "")
        if not secret and account:
            secret = self.credentials.get(account["secret_target"]) or ""
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
        except ImportError as exc:
            raise RuntimeError("未安装 lark-oapi，无法启动飞书长连接") from exc
        app_id, secret = self.credentials_value()

        async def receive(message) -> None:
            if message.sender.is_bot:
                return
            message_id = str(message.message_id or "").strip()
            if not message_id or not self.store.claim_inbound("feishu", message_id):
                return
            chat_type = "direct" if message.chat_type == "p2p" else "group"
            if chat_type == "group" and not message.mentioned_bot:
                return
            text = str(message.content_text or "").strip()
            for mention in message.mentions or []:
                key = str(getattr(mention, "key", "") or "")
                if key:
                    text = text.replace(key, "").strip()
            if not text:
                return
            actor = ActorContext(
                channel="feishu", target=str(message.chat_id), account_id=app_id,
                chat_type=chat_type, sender_id=str(message.sender_id),
                sender_name=str(message.sender_name or ""),
            )
            on_message(actor, text)

        async def serve() -> None:
            channel = FeishuChannel(app_id=app_id, app_secret=secret)
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
