from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import time
from collections.abc import Callable
from typing import Literal, get_args

from quantmaster.automation.models import ActorContext
from quantmaster.automation.store import AutomationStore
from quantmaster.config import get_config
from quantmaster.credentials import CredentialError, CredentialStore
from quantmaster.logging_config import normalize_third_party_logger

logger = logging.getLogger(__name__)

FEISHU_STATES = {
    "disabled", "not_configured", "invalid_credentials", "tls_error",
    "network_error", "rate_limited", "connected",
}


def _safe_feishu_error(value: object) -> str:
    """Keep diagnostics actionable without putting authentication material in state."""
    message = str(value or "").strip()
    message = re.sub(r"(?i)(bearer\s+)[^\s,;&]+", r"\1***", message)
    message = re.sub(
        r"(?i)((?:app[_ -]?secret|token|authorization|ticket|cookie|header)\s*[=:]\s*)[^\s,;&]+",
        r"\1***", message,
    )
    return message[:500]


def _task_name(task: asyncio.Task) -> str:
    try:
        coroutine = task.get_coro()
        return str(
            getattr(coroutine, "__qualname__", "")
            or getattr(coroutine, "__name__", "")
        )
    except (AttributeError, RuntimeError):
        return ""


async def _drain_lark_ws_tasks(channel, timeout: float = 2.0) -> bool:
    """Drain lark-oapi 1.x private WS tasks before its loop is stopped.

    The SDK's public ``stop_background`` closes the socket and immediately
    stops its module-level loop.  Its ping, receive and ExpiringCache tasks are
    otherwise destroyed while pending.  Keep this compatibility boundary here
    instead of patching site-packages or hiding the resulting warnings.
    """
    ws = getattr(channel, "_ws_client", None)
    if ws is None:
        return False
    ws_loop = None
    try:
        from lark_oapi.ws import client as ws_client_module

        ws_loop = getattr(ws_client_module, "loop", None)
    except ImportError:
        return False
    if ws_loop is None or ws_loop.is_closed():
        return False

    # A normal local shutdown must never enter the SDK's reconnect loop.
    if hasattr(ws, "_auto_reconnect"):
        ws._auto_reconnect = False

    async def cancel_task(task: asyncio.Task) -> None:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    # ExpiringCache is constructed inside the SDK executor thread.  With no
    # event loop there, lark-oapi creates a separate orphan loop and schedules
    # _start_clear_cron on it; that task is therefore invisible to ws_loop.
    cache = getattr(ws, "_cache", None)
    cache_task = getattr(cache, "_cron", None)
    if isinstance(cache_task, asyncio.Task) and not cache_task.done():
        cache_loop = cache_task.get_loop()
        try:
            running_loop = asyncio.get_running_loop()
            if cache_loop is running_loop:
                await cancel_task(cache_task)
            elif cache_loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    cancel_task(cache_task), cache_loop,
                )
                await asyncio.wait_for(asyncio.wrap_future(future), timeout=timeout)
            elif not cache_loop.is_closed():
                def drain_orphan_loop() -> None:
                    asyncio.set_event_loop(cache_loop)
                    cache_task.cancel()
                    cache_loop.run_until_complete(
                        asyncio.gather(cache_task, return_exceptions=True),
                    )

                await asyncio.wait_for(
                    asyncio.to_thread(drain_orphan_loop), timeout=timeout,
                )
        except (TimeoutError, asyncio.CancelledError, RuntimeError, OSError):
            logger.warning("飞书 SDK 缓存协程未能在退出前完整回收", exc_info=True)

    async def drain() -> list[asyncio.Task]:
        current = asyncio.current_task()
        roots: list[asyncio.Task] = []
        background: list[asyncio.Task] = []
        for task in asyncio.all_tasks(ws_loop):
            if task is current or task.done():
                continue
            # WSClient.start() blocks on this sentinel via run_until_complete.
            # Cancel it only after every real background task has been awaited.
            if _task_name(task).endswith("._select") or _task_name(task) == "_select":
                roots.append(task)
            else:
                background.append(task)
        for task in background:
            task.cancel()
        if background:
            await asyncio.gather(*background, return_exceptions=True)
        disconnect = getattr(ws, "_disconnect", None)
        if callable(disconnect):
            await disconnect()
        return roots

    try:
        if ws_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(drain(), ws_loop)
            roots = await asyncio.wait_for(asyncio.wrap_future(future), timeout=timeout)
            for task in roots:
                ws_loop.call_soon_threadsafe(task.cancel)
            # Let run_until_complete consume the sentinel cancellation before
            # FeishuChannel.stop() considers stopping/closing the loop.
            deadline = asyncio.get_running_loop().time() + timeout
            while ws_loop.is_running() and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.01)
            if ws_loop.is_running():
                logger.warning("飞书 SDK WebSocket 哨兵循环未能按时退出")
                return False
        else:
            roots = ws_loop.run_until_complete(drain())
            for task in roots:
                task.cancel()
            if roots:
                ws_loop.run_until_complete(asyncio.gather(*roots, return_exceptions=True))
        # We already disconnected and fully drained this transport.  Prevent
        # FeishuChannel.stop() from running its private fallback a second time;
        # that fallback calls run_until_complete from our active asyncio loop
        # and creates an un-awaited _disconnect coroutine before raising.
        if getattr(channel, "_ws_client", None) is ws:
            channel._ws_client = None
        return True
    except (TimeoutError, asyncio.CancelledError, RuntimeError, OSError):
        logger.warning("飞书 SDK 后台协程未能在退出前完整回收", exc_info=True)
        return False


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

    def credential_state(self) -> dict:
        """Return a UI-safe snapshot; this is intentionally not a credential reader."""
        account = self.store.bot_account("feishu")
        if not account:
            return {
                "state": "not_configured", "app_id_present": False,
                "app_id_suffix": "", "app_secret_present": False,
                "last_validated_at": "",
            }
        app_id = str(account.get("account_id") or "")
        secret = ""
        try:
            if account.get("secret_target"):
                secret = self.credentials.get(str(account["secret_target"])) or ""
            elif app_id:
                secret = os.environ.get("QM_FEISHU_APP_SECRET", "")
        except Exception:
            # A broken credential store must not result in a connection attempt.
            pass
        configured = bool(app_id and secret)
        return {
            "state": str(account.get("status") or "not_configured") if configured else "not_configured",
            "app_id_present": bool(app_id), "app_id_suffix": app_id[-4:] if app_id else "",
            "app_secret_present": bool(secret),
            "last_validated_at": str(account.get("last_validated_at") or ""),
        }

    def public_account(self, account: dict) -> dict:
        """Project persisted account metadata into a status-safe representation."""
        state = self.credential_state()
        return {
            "channel": "feishu",
            "status": state["state"],
            "app_id_present": state["app_id_present"],
            "app_id_suffix": state["app_id_suffix"],
            "app_secret_present": state["app_secret_present"],
            "last_validated_at": state["last_validated_at"],
            "updated_at": str(account.get("updated_at") or ""),
            "last_error": _safe_feishu_error(account.get("last_error")),
        }

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
            if response.status_code == 429:
                state, message = "rate_limited", "飞书验证请求受限；请稍后重试"
            elif valid:
                state, message = "connected", "App ID / App Secret 有效"
            else:
                state, message = "invalid_credentials", "App ID 或 App Secret 无效"
            return {
                "status": "success" if valid else "warning" if state == "rate_limited" else "error",
                "state": state, "message": message,
                "latency_ms": round((time.perf_counter() - started) * 1000),
            }
        except httpx.ConnectError:
            return {
                "status": "warning", "state": "network_error",
                "message": "飞书网络不可达；凭据可保存后重试",
                "latency_ms": round((time.perf_counter() - started) * 1000),
            }
        except (httpx.TransportError, ValueError):
            return {
                "status": "warning", "state": "tls_error",
                "message": "飞书 TLS/传输验证失败；请检查系统时间、证书和网络",
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

    def is_configured(self) -> bool:
        try:
            app_id, secret = self.credentials_value()
            return bool(app_id and secret)
        except (CredentialError, RuntimeError):
            return False

    @staticmethod
    def safe_error(value: object) -> str:
        return _safe_feishu_error(value)

    @staticmethod
    def failure_state(value: object) -> str:
        message = str(value or "").casefold()
        if "429" in message or "rate limit" in message or "too many" in message:
            return "rate_limited"
        if any(token in message for token in ("ssl", "tls", "certificate")):
            return "tls_error"
        if any(token in message for token in (
            "401", "403", "invalid credential", "app secret", "app id", "unauthorized",
        )):
            return "invalid_credentials"
        return "network_error"

    def _client(self):
        try:
            import lark_oapi as lark
        except ImportError as exc:
            raise RuntimeError("未安装 lark-oapi，无法使用飞书 Bot") from exc
        app_id, secret = self.credentials_value()
        return lark.Client.builder().app_id(app_id).app_secret(secret).build()

    def send(self, *, chat_id: str, text: str) -> str:
        return self._send_message(chat_id=chat_id, msg_type="text", content={"text": text})

    def send_card(self, *, chat_id: str, card: dict) -> str:
        """发送飞书消息卡片；用于主通道的结构化告警。"""
        return self._send_message(chat_id=chat_id, msg_type="interactive", content=card)

    def update_card(self, *, message_id: str, card: dict) -> None:
        """原位更新已发送的交互卡片，用于长任务进度与最终报告。"""
        from lark_oapi.api.im.v1 import PatchMessageRequest, PatchMessageRequestBody

        request = PatchMessageRequest.builder().message_id(message_id).request_body(
            PatchMessageRequestBody.builder().content(
                json.dumps(card, ensure_ascii=False)).build()
        ).build()
        response = self._client().im.v1.message.patch(request)
        if not response.success():
            raise RuntimeError(f"飞书卡片更新失败 code={response.code}: {response.msg}")

    def _send_message(self, *, chat_id: str, msg_type: str, content: dict) -> str:
        from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody

        request = CreateMessageRequest.builder().receive_id_type("chat_id").request_body(
            CreateMessageRequestBody.builder().receive_id(chat_id).msg_type(msg_type).content(
                json.dumps(content, ensure_ascii=False)).build()
        ).build()
        response = self._client().im.v1.message.create(request)
        if not response.success():
            raise RuntimeError(f"飞书发送失败 code={response.code}: {response.msg}")
        message_id = str(getattr(response.data, "message_id", "") or "")
        if not message_id:
            raise RuntimeError("飞书发送成功但响应缺少 message_id")
        return message_id

    def listen_forever(self, on_message: Callable[[ActorContext, str], None],
                       stop_event: threading.Event) -> None:
        if not self.is_configured():
            # Direct calls are also harmless: no SDK import, WebSocket or retry loop.
            return
        try:
            from lark_oapi.channel import FeishuChannel
            try:
                from lark_oapi.core import LogLevel
            except ImportError:  # 兼容旧 SDK 及精简测试替身。
                LogLevel = None
            try:
                from lark_oapi.channel import PolicyConfig
            except ImportError:  # 兼容旧 SDK 及精简测试替身。
                PolicyConfig = None
        except ImportError as exc:
            raise RuntimeError("未安装 lark-oapi，无法启动飞书长连接") from exc
        normalize_third_party_logger("Lark")
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
            chat_type: Literal["direct", "group"] = (
                "direct" if message.chat_type == "p2p" else "group"
            )
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

        async def lifecycle_event(event) -> None:
            """Acknowledge non-message lifecycle events without treating them as failures."""
            event_type = str(
                getattr(event, "event_type", "")
                or getattr(event, "type", "")
                or "botAdded"
            )
            logger.info("飞书 Bot 生命周期事件已接收: %s", event_type)
            self.store.set_bot_status("feishu", app_id, "listening")

        async def raw_event(event) -> None:
            event_type = str(
                getattr(event, "event_type", "")
                or getattr(getattr(event, "header", None), "event_type", "")
            )
            if event_type in {
                "im.chat.access_event.bot_p2p_chat_entered_v1",
                "im.chat.member.user.added_v1",
            }:
                await lifecycle_event(event)

        async def serve() -> None:
            nonlocal channel
            # 允许普通群消息进入本地上下文缓存；是否回复由上面的真实 @ 检查决定。
            options = {"app_id": app_id, "app_secret": secret}
            if LogLevel is not None:
                options["log_level"] = LogLevel.WARNING
            if PolicyConfig is not None:
                options["policy"] = PolicyConfig(
                    require_mention=False, respond_to_mention_all=False)
            channel = FeishuChannel(**options)
            channel.on("message", receive)
            # 新版 SDK 用 botAdded 归一化会话进入/入群事件，同时保留 raw 兜底；
            # 旧 SDK 和测试替身没有这些事件名时不做猜测性注册。
            try:
                from lark_oapi.channel.events import ChannelEventName

                supported = set(get_args(ChannelEventName))
                if "botAdded" in supported:
                    channel.on("botAdded", lifecycle_event)
                if "raw" in supported:
                    channel.on("raw", raw_event)
            except (ImportError, TypeError):
                logger.debug("当前飞书 SDK 不暴露生命周期事件注册表")
            self.store.set_bot_status("feishu", app_id, "connecting")
            try:
                await channel.start_background(timeout=30)
                self.store.set_bot_status("feishu", app_id, "listening")
                logger.info("飞书 Bot 长连接已就绪")
                await asyncio.to_thread(stop_event.wait)
            finally:
                try:
                    await _drain_lark_ws_tasks(channel)
                    await channel.stop_background()
                except (OSError, RuntimeError, TypeError, ValueError):
                    if not stop_event.is_set():
                        raise
                    logger.info("飞书 Bot 长连接关闭时 SDK 已先行释放资源")
                if stop_event.is_set():
                    self.store.set_bot_status("feishu", app_id, "configured")
                    logger.info("飞书 Bot 长连接已停止")

        asyncio.run(serve())
