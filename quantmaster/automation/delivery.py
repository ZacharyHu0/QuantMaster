from __future__ import annotations

from typing import Any, Protocol

import httpx

from quantmaster.automation.channels import FeishuBotClient, WeixinClawBotClient
from quantmaster.automation.store import AutomationStore
from quantmaster.credentials import CredentialError


class DeliveryError(RuntimeError):
    def __init__(self, message: str, *, permanent: bool = False, needs_rebind: bool = False):
        super().__init__(message)
        self.permanent = permanent
        self.needs_rebind = needs_rebind


def _trim(value: Any, limit: int = 280) -> str:
    text = str(value).strip()
    return text if len(text) <= limit else text[:limit - 1] + "…"


def _lark_md(value: Any) -> str:
    text = _trim(value)
    for character in ("\\", "*", "_", "[", "]", "<", ">"):
        text = text.replace(character, f"\\{character}")
    return text


def format_alert(item: dict[str, Any], channel: str) -> str:
    labels = {
        "market_turn": "盘中变盘", "market_close": "收盘状态变化",
        "important_news": "重要消息", "task_failure": "自动化任务失败",
        "task_report": "自动化报告",
    }
    direction = {"up": "偏强", "down": "偏弱", "neutral": "中性"}.get(
        item.get("direction"), "中性")
    title = item.get("payload", {}).get("title") or labels.get(item.get("kind"), item.get("kind"))
    lines = [f"【QuantMaster · {labels.get(item.get('kind'), '提醒')}】", str(title)]
    lines.append(
        f"强度 {float(item.get('score', 0)):.0f}/100 · {direction} · "
        f"数据截至 {item.get('data_as_of') or '未知'}")
    evidence = item.get("evidence") or []
    if evidence:
        lines.append("依据：" + "；".join(_trim(value) for value in evidence[:4]))
    symbols = item.get("symbols") or []
    if symbols:
        lines.append("相关标的：" + "、".join(symbols[:12]))
    urls = item.get("source_urls") or []
    if urls:
        lines.append("原文：" + " ".join(urls[:3]))
    lines.append("仅作量化研究与记录，不构成投资建议。")
    return "\n".join(lines)


def format_feishu_card(item: dict[str, Any]) -> dict[str, Any]:
    """飞书主通道使用结构化卡片，便于在群聊中快速核查证据。"""
    labels = {
        "market_turn": "盘中变盘", "market_close": "收盘状态变化",
        "important_news": "重要消息", "task_failure": "自动化任务失败",
        "task_report": "自动化报告",
    }
    direction = {"up": "偏强", "down": "偏弱", "neutral": "中性"}.get(
        item.get("direction"), "中性")
    template = (
        "red" if item.get("direction") == "up" or item.get("severity") == "critical"
        else "green" if item.get("direction") == "down" else "blue"
    )
    title = _trim(
        item.get("payload", {}).get("title") or labels.get(item.get("kind"), "QuantMaster 提醒"),
        80,
    )
    lines = [
        f"**强度**  {float(item.get('score', 0)):.0f}/100    **方向**  {direction}",
        f"**数据截至**  {item.get('data_as_of') or '未知'}",
    ]
    evidence = item.get("evidence") or []
    if evidence:
        lines.extend(["", "**核查依据**", *[f"• {_lark_md(value)}" for value in evidence[:5]]])
    symbols = item.get("symbols") or []
    if symbols:
        lines.extend(["", "**相关标的**  " + "、".join(symbols[:12])])
    urls = [str(url) for url in (item.get("source_urls") or [])
            if str(url).startswith(("https://", "http://"))]
    if urls:
        links = "  ".join(f"[原文 {index + 1}]({url})" for index, url in enumerate(urls[:3]))
        lines.extend(["", links])
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": f"QuantMaster · {title}"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}},
            {"tag": "hr"},
            {"tag": "note", "elements": [{
                "tag": "plain_text", "content": "仅作量化研究与记录，不构成投资建议。",
            }]},
        ],
    }


class DeliveryGateway(Protocol):
    def send(self, delivery: dict[str, Any]) -> None: ...


class BotDeliveryGateway:
    """把出站消息直接送到腾讯微信 ClawBot 或飞书应用 Bot。"""

    def __init__(self, store: AutomationStore,
                 weixin: WeixinClawBotClient | None = None,
                 feishu: FeishuBotClient | None = None):
        self.store = store
        self.weixin = weixin or WeixinClawBotClient(store)
        self.feishu = feishu or FeishuBotClient(store)

    def send(self, delivery: dict[str, Any]) -> None:
        if not delivery.get("target") or not delivery.get("account_id"):
            raise DeliveryError("推送目标尚未绑定", permanent=True, needs_rebind=True)
        message = format_alert(delivery, delivery["channel"])
        try:
            if delivery["channel"] == "weixin":
                if not delivery.get("context_token"):
                    raise DeliveryError(
                        "微信会话尚无 context_token，请先给 ClawBot 发送一条消息",
                        permanent=True, needs_rebind=True)
                self.weixin.send(
                    account_id=delivery["account_id"],
                    to_user_id=delivery["target"],
                    context_token=delivery["context_token"],
                    text=message,
                )
            elif delivery["channel"] == "feishu":
                self.feishu.send_card(
                    chat_id=delivery["target"], card=format_feishu_card(delivery))
            else:
                raise DeliveryError(f"不支持的推送频道：{delivery['channel']}", permanent=True)
        except DeliveryError:
            raise
        except CredentialError as exc:
            raise DeliveryError(str(exc), permanent=True, needs_rebind=True) from exc
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            body = exc.response.text[:500]
            needs_rebind = code in {400, 404} and any(
                word in body.lower() for word in ("context", "recipient", "chat_id", "user"))
            raise DeliveryError(
                f"{delivery['channel']} 接口返回 {code}: {body}",
                permanent=code in {400, 401, 403, 404}, needs_rebind=needs_rebind,
            ) from exc
        except httpx.HTTPError as exc:
            raise DeliveryError(f"{delivery['channel']} 接口暂时不可达: {exc}") from exc
        except RuntimeError as exc:
            message = str(exc)
            permanent = any(word in message for word in (
                "尚未配置", "不存在", "未安装", "token", "App ID", "App Secret"))
            needs_rebind = "context_token" in message or "重新扫码" in message
            raise DeliveryError(message, permanent=permanent, needs_rebind=needs_rebind) from exc


class OutboxDispatcher:
    def __init__(self, store: AutomationStore, gateway: DeliveryGateway | None = None):
        self.store = store
        self.gateway = gateway or BotDeliveryGateway(store)

    def dispatch(self, limit: int = 20) -> dict[str, int]:
        result = {"delivered": 0, "failed": 0, "retried": 0}
        for item in self.store.due_deliveries(limit):
            try:
                self.gateway.send(item)
            except DeliveryError as exc:
                self.store.delivery_failure(item["id"], str(exc), permanent=exc.permanent)
                if exc.needs_rebind:
                    self.store.set_target_status(item["target_id"], "needs_rebind", str(exc))
                elif exc.permanent:
                    self.store.set_target_status(item["target_id"], "degraded", str(exc))
                result["failed" if exc.permanent else "retried"] += 1
            else:
                self.store.delivery_success(item["id"])
                self.store.set_target_status(item["target_id"], "healthy")
                result["delivered"] += 1
        return result
