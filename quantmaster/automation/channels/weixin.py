from __future__ import annotations

import base64
import os
import random
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from io import BytesIO
from typing import Any

import httpx

from quantmaster import __version__
from quantmaster.automation.channels.actor_context import ActorContext
from quantmaster.config import get_config
from quantmaster.credentials import CredentialStore

ILINK_APP_ID = "bot"
ILINK_APP_CLIENT_VERSION = (2 << 16) | (4 << 8) | 6
BOT_TYPE = "3"


def _base_info() -> dict:
    return {"channel_version": "2.4.6", "bot_agent": f"QuantMaster/{__version__}"}


@dataclass
class WeixinLoginSession:
    id: str
    qrcode: str
    qrcode_url: str
    started_at: float
    api_base_url: str


class WeixinClawBotClient:
    """腾讯微信 ClawBot 的 iLink HTTP/JSON 直连客户端。"""

    def __init__(self, store: Any, credentials: CredentialStore | None = None,
                 base_url: str = ""):
        self.store = store
        self.credentials = credentials or CredentialStore()
        self.base_url = (base_url or get_config().automation.weixin_api_base).rstrip("/")
        self._sessions: dict[str, WeixinLoginSession] = {}
        self._session_lock = threading.RLock()

    @staticmethod
    def _common_headers() -> dict[str, str]:
        return {
            "iLink-App-Id": ILINK_APP_ID,
            "iLink-App-ClientVersion": str(ILINK_APP_CLIENT_VERSION),
        }

    def _headers(self, token: str) -> dict[str, str]:
        random_uin = base64.b64encode(str(random.getrandbits(32)).encode()).decode()
        return {
            **self._common_headers(), "Content-Type": "application/json",
            "AuthorizationType": "ilink_bot_token", "Authorization": f"Bearer {token}",
            "X-WECHAT-UIN": random_uin,
        }

    @staticmethod
    def _qr_svg_data(value: str) -> str:
        try:
            import qrcode
            from qrcode.image.svg import SvgPathImage

            image = qrcode.make(value, image_factory=SvgPathImage, box_size=8, border=2)
            buffer = BytesIO()
            image.save(buffer)
            return "data:image/svg+xml;base64," + base64.b64encode(buffer.getvalue()).decode()
        except ImportError:
            return ""

    def start_login(self) -> dict:
        existing_tokens = []
        for account in self.store.bot_accounts("weixin")[:10]:
            try:
                token = self.credentials.get(account["secret_target"])
            except Exception:
                token = None
            if token:
                existing_tokens.append(token)
        response = httpx.post(
            f"{self.base_url}/ilink/bot/get_bot_qrcode",
            params={"bot_type": BOT_TYPE}, json={"local_token_list": existing_tokens},
            headers=self._common_headers(), timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("qrcode") or not data.get("qrcode_img_content"):
            raise RuntimeError(f"微信 ClawBot 未返回有效二维码: {data}")
        session = WeixinLoginSession(
            id=uuid.uuid4().hex, qrcode=data["qrcode"],
            qrcode_url=data["qrcode_img_content"], started_at=__import__("time").time(),
            api_base_url=self.base_url,
        )
        with self._session_lock:
            self._sessions[session.id] = session
        return {
            "session_id": session.id, "status": "wait", "expires_in": 300,
            "qrcode_url": session.qrcode_url,
            "qrcode_svg": self._qr_svg_data(session.qrcode_url),
        }

    def poll_login(self, session_id: str, verify_code: str = "") -> dict:
        with self._session_lock:
            session = self._sessions.get(session_id)
        if not session:
            raise ValueError("微信登录会话不存在或已失效")
        params = {"qrcode": session.qrcode}
        if verify_code:
            params["verify_code"] = verify_code
        try:
            response = httpx.get(
                f"{session.api_base_url}/ilink/bot/get_qrcode_status", params=params,
                headers=self._common_headers(), timeout=40,
            )
            response.raise_for_status()
            data = response.json()
        except httpx.TimeoutException:
            return {"session_id": session_id, "status": "wait"}
        status = data.get("status", "wait")
        result = {"session_id": session_id, "status": status}
        if status == "scaned_but_redirect" and data.get("redirect_host"):
            redirect = str(data["redirect_host"]).strip().rstrip("/")
            if not redirect.startswith("https://"):
                redirect = f"https://{redirect}"
            session.api_base_url = redirect
            result["status"] = "scaned"
            result["redirected"] = True
            return result
        if status == "need_verifycode":
            result["needs_verify_code"] = True
            return result
        if status == "binded_redirect":
            with self._session_lock:
                self._sessions.pop(session_id, None)
            return {"session_id": session_id, "status": "confirmed", "already_connected": True}
        if status == "confirmed":
            token, account_id = data.get("bot_token"), data.get("ilink_bot_id")
            user_id = data.get("ilink_user_id") or ""
            if not token or not account_id:
                raise RuntimeError("微信已确认扫码，但没有返回 bot_token/ilink_bot_id")
            secret_target = CredentialStore.weixin_target(account_id)
            self.credentials.set(secret_target, token)
            api_base = (data.get("baseurl") or self.base_url).rstrip("/")
            self.store.save_bot_account(
                channel="weixin", account_id=account_id, user_id=user_id,
                base_url=api_base, secret_target=secret_target, status="waiting_message",
            )
            actor = f"weixin:{account_id}:{user_id}"
            self.store.bind_target(
                "weixin_owner", target=user_id, account_id=account_id,
                owner_actor=actor, actor=actor,
            )
            self.store.set_target_status(
                "weixin_owner", "waiting_message", "请先给 ClawBot 发送一条消息以建立 context_token")
            with self._session_lock:
                self._sessions.pop(session_id, None)
            result.update({"account_id": account_id, "user_id": user_id,
                           "status": "confirmed", "needs_first_message": True})
        return result

    def _token(self, account: dict) -> str:
        env = os.environ.get("QM_WEIXIN_BOT_TOKEN", "")
        if env:
            return env
        token = self.credentials.get(account["secret_target"])
        if not token:
            raise RuntimeError("微信 ClawBot token 不存在，请重新扫码")
        return token

    def send(self, *, account_id: str, to_user_id: str, context_token: str, text: str) -> None:
        account = self.store.bot_account("weixin", account_id)
        if not account:
            raise RuntimeError("微信 ClawBot 账号未配置")
        if not context_token:
            raise RuntimeError("微信会话尚无 context_token，请先给机器人发送一条消息")
        response = httpx.post(
            f"{account['base_url'].rstrip('/')}/ilink/bot/sendmessage",
            headers=self._headers(self._token(account)), timeout=15,
            json={
                "msg": {"to_user_id": to_user_id, "context_token": context_token,
                        "message_type": 2, "message_state": 2,
                        "item_list": [{"type": 1, "text_item": {"text": text}}]},
                "base_info": _base_info(),
            },
        )
        response.raise_for_status()
        data = response.json()
        if data.get("ret") not in (None, 0):
            raise RuntimeError(f"微信发送失败 ret={data.get('ret')}: {data.get('errmsg', '')}")

    def _handle_inbound_message(
        self, message: dict[str, Any], account: dict[str, Any], account_id: str,
        on_message: Callable[[ActorContext, str], None],
    ) -> None:
        if message.get("message_type") != 1:
            return
        message_id = str(message.get("message_id") or message.get("seq") or uuid.uuid4().hex)
        if not self.store.claim_inbound("weixin", message_id):
            return
        user_id = str(message.get("from_user_id") or account.get("user_id") or "")
        context_token = str(message.get("context_token") or "")
        target = self.store.target("weixin_owner")
        if target and user_id == target["target"] and context_token:
            self.store.update_context_token("weixin_owner", context_token)
        text = "\n".join(
            str(item.get("text_item", {}).get("text", ""))
            for item in message.get("item_list") or [] if item.get("type") == 1
        ).strip()
        if text:
            on_message(ActorContext(
                channel="weixin", target=user_id, account_id=account_id,
                chat_type="direct", sender_id=user_id,
            ), text)

    def _poll_once(
        self, account: dict[str, Any], account_id: str,
        on_message: Callable[[ActorContext, str], None],
    ) -> None:
        account = self.store.bot_account("weixin", account_id) or account
        response = httpx.post(
            f"{account['base_url'].rstrip('/')}/ilink/bot/getupdates",
            headers=self._headers(self._token(account)), timeout=40,
            json={"get_updates_buf": account.get("cursor", ""), "base_info": _base_info()},
        )
        response.raise_for_status()
        data = response.json()
        if data.get("ret") not in (None, 0):
            raise RuntimeError(f"getupdates ret={data.get('ret')}: {data.get('errmsg', '')}")
        if data.get("get_updates_buf") is not None:
            self.store.update_bot_cursor("weixin", account_id, data["get_updates_buf"])
        for message in data.get("msgs") or []:
            self._handle_inbound_message(message, account, account_id, on_message)
        self.store.set_bot_status("weixin", account_id, "healthy")

    def poll_forever(self, on_message: Callable[[ActorContext, str], None],
                     stop_event: threading.Event) -> None:
        account = self.store.bot_account("weixin")
        if not account:
            return
        account_id = account["account_id"]
        while not stop_event.is_set():
            try:
                self._poll_once(account, account_id, on_message)
            except httpx.TimeoutException:
                continue
            except Exception as exc:
                self.store.set_bot_status("weixin", account_id, "degraded", str(exc))
                stop_event.wait(5)
