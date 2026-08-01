from __future__ import annotations

from collections.abc import Callable
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


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _news_direction(value: Any) -> str:
    return {"up": "利好", "down": "利空", "neutral": "中性"}.get(str(value), "中性")


def _digest_counts(payload: dict[str, Any]) -> dict[str, int]:
    configured = payload.get("counts") or {}
    if configured:
        return {
            "up": int(configured.get("up") or 0),
            "down": int(configured.get("down") or 0),
            "neutral": int(configured.get("neutral") or 0),
        }
    result = {"up": 0, "down": 0, "neutral": 0}
    for child in payload.get("items") or []:
        direction = child.get("direction")
        result[direction if direction in result else "neutral"] += 1
    return result


def _feishu_category(kind: Any, payload: dict[str, Any]) -> str:
    if kind == "important_news":
        return "资讯摘要" if payload.get("digest") else "重要资讯"
    if kind == "task_failure":
        phases = set(payload.get("phases") or [])
        if phases == {"fetch"}:
            return "资讯拉取异常"
        if phases == {"analysis"}:
            return "资讯分析异常"
        if {"fetch", "analysis"}.issubset(phases):
            return "资讯处理异常"
        if str(payload.get("task") or "") in {
            "fast_news_scan", "official_news_scan", "periodic_news_scan", "news_digest",
        }:
            return "资讯任务异常"
        return "任务异常"
    return {
        "market_turn": "盘中变盘",
        "market_close": "收盘状态",
        "task_report": "任务结果",
    }.get(str(kind), "系统提醒")


def _feishu_result_lines(result: Any) -> list[str]:
    if not isinstance(result, dict):
        return []
    status_labels = {
        "ok": "已完成", "completed": "已完成", "succeeded": "已完成",
        "skipped": "已跳过", "pending_confirmation": "等待确认",
        "failed": "失败",
    }
    fields = (
        ("status", "状态"), ("reason", "说明"), ("signal_date", "信号日期"),
        ("latest", "最新数据"), ("items", "资讯数量"), ("picks", "候选数量"),
        ("planned", "计划调整"), ("breadth", "上涨比例"),
        ("sample_size", "样本数量"),
    )
    lines: list[str] = []
    for key, label in fields:
        value = result.get(key)
        if value is None or value == "":
            continue
        if key == "status":
            value = status_labels.get(str(value), value)
        elif key == "breadth":
            value = f"{_number(value) * 100:.1f}%"
        lines.append(f"**{label}**  {_lark_md(value)}")
    return lines[:6]


def _feishu_footer(kind: Any) -> str:
    if kind == "task_failure":
        return "系统会按计划重试；若连续出现，请检查自动化任务与数据源状态。"
    if kind == "task_report":
        return "可在 QuantMaster 自动化页面查看完整运行记录。"
    return "仅作量化研究与记录，不构成投资建议。"


def format_alert(item: dict[str, Any], channel: str) -> str:
    labels: dict[Any, str] = {
        "market_turn": "盘中变盘", "market_close": "收盘状态变化",
        "important_news": "重要资讯", "task_failure": "自动化任务失败",
        "task_report": "自动化报告",
    }
    kind = str(item.get("kind") or "")
    payload = item.get("payload", {})
    direction = {"up": "偏强", "down": "偏弱", "neutral": "中性"}.get(
        str(item.get("direction") or "neutral"), "中性")
    title = payload.get("title") or labels.get(kind, kind)
    lines = [f"【QuantMaster · {labels.get(item.get('kind'), '提醒')}】", str(title)]
    if kind == "important_news" and payload.get("digest"):
        counts = _digest_counts(payload)
        lines.append(
            f"共 {len(payload.get('items') or [])} 条 · 利好 {counts['up']} · "
            f"利空 {counts['down']} · 中性 {counts['neutral']}")
        for index, child in enumerate((payload.get("items") or [])[:5], 1):
            lines.append(
                f"{index}. [{_news_direction(child.get('direction'))}] "
                f"{_trim(child.get('title') or '消息', 100)}")
            if child.get("summary"):
                lines.append(f"   摘要：{_trim(child['summary'], 180)}")
            if child.get("symbols"):
                lines.append("   标的：" + "、".join(child["symbols"][:6]))
            if child.get("sectors"):
                lines.append("   板块：" + "、".join(child["sectors"][:5]))
            if str(child.get("url") or "").startswith(("https://", "http://")):
                lines.append(f"   原文：{child['url']}")
    elif kind == "important_news":
        sentiment = _number(payload.get("sentiment"))
        lines.append(
            f"研判 {_news_direction(item.get('direction'))} ({sentiment:+.2f}) · "
            f"重要度 {_number(item.get('score')):.0f}/100")
        if payload.get("summary"):
            lines.append("摘要：" + _trim(payload["summary"], 200))
        if payload.get("sectors"):
            lines.append("相关板块：" + "、".join(payload["sectors"][:5]))
        lines.append(f"数据截至：{item.get('data_as_of') or '未知'}")
    else:
        lines.append(
            f"强度 {_number(item.get('score')):.0f}/100 · {direction} · "
            f"数据截至 {item.get('data_as_of') or '未知'}")
    evidence = item.get("evidence") or []
    if evidence and not (kind == "important_news" and payload.get("digest")):
        lines.append("核查依据：" + "；".join(_trim(value) for value in evidence[:4]))
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
    kind = str(item.get("kind") or "")
    payload = item.get("payload", {})
    direction = {"up": "偏强", "down": "偏弱", "neutral": "中性"}.get(
        str(item.get("direction") or "neutral"), "中性")
    template = (
        "red" if item.get("direction") == "up" or item.get("severity") == "critical"
        else "green" if item.get("direction") == "down" else "blue"
    )
    category = _feishu_category(kind, payload)
    if kind == "important_news" and payload.get("digest"):
        counts = _digest_counts(payload)
        lines = [
            (f"**本期**  {len(payload.get('items') or [])} 条    **利好**  {counts['up']}    "
             f"**利空**  {counts['down']}    **中性**  {counts['neutral']}"),
            f"**数据截至**  {item.get('data_as_of') or '未知'}",
        ]
        for index, child in enumerate((payload.get("items") or [])[:5], 1):
            child_title = _lark_md(child.get("title") or "消息")
            url = str(child.get("url") or "")
            if url.startswith(("https://", "http://")):
                child_title = f"[{child_title}]({url})"
            lines.extend([
                "",
                f"**{index} · {_news_direction(child.get('direction'))}**  {child_title}",
            ])
            if child.get("summary"):
                lines.append(f"摘要：{_lark_md(child['summary'])}")
            if child.get("symbols"):
                lines.append("标的：" + "、".join(child["symbols"][:6]))
            if child.get("sectors"):
                lines.append("板块：" + "、".join(child["sectors"][:5]))
    elif kind == "important_news":
        sentiment = _number(payload.get("sentiment"))
        lines = [
            f"**{_lark_md(payload.get('title') or '未命名资讯')}**",
            "",
            (f"**研判**  {_news_direction(item.get('direction'))} ({sentiment:+.2f})    "
             f"**重要度**  {_number(item.get('score')):.0f}/100"),
        ]
        if payload.get("summary"):
            lines.extend(["", "**摘要**", _lark_md(payload["summary"])])
        context = []
        if payload.get("source"):
            context.append(f"**来源**  {_lark_md(payload['source'])}")
        if payload.get("event_type"):
            context.append(f"**类型**  {_lark_md(payload['event_type'])}")
        relevance = {"holding": "持仓", "watchlist": "关注", "market": "全市场"}.get(
            str(item.get("relevance") or "market"), "全市场")
        context.append(f"**范围**  {relevance}")
        lines.extend(["", "    ".join(context)])
        if payload.get("sectors"):
            lines.append("**相关板块**  " + "、".join(payload["sectors"][:5]))
        lines.append(f"**数据截至**  {item.get('data_as_of') or '未知'}")
    elif kind == "market_turn":
        movement = "快速走强" if item.get("direction") == "up" else "快速转弱"
        lines = [
            f"**{_lark_md(payload.get('title') or f'市场{movement}')}**",
            (f"**强度**  {_number(item.get('score')):.0f}/100    **方向**  {direction}    "
             f"**连续确认**  {int(_number(payload.get('confirmation_count'), 1))} 次"),
        ]
        if payload.get("median_return") is not None:
            lines.append(f"**15 分钟指数收益中位数**  {_number(payload['median_return']) * 100:+.2f}%")
        if payload.get("breadth_delta_pp") is not None:
            lines.append(f"**上涨家数比例变化**  {_number(payload['breadth_delta_pp']):+.1f} 个百分点")
        lines.append(f"**数据截至**  {item.get('data_as_of') or '未知'}")
    elif kind == "market_close":
        current = payload.get("current") if isinstance(payload.get("current"), dict) else {}
        previous = payload.get("previous") if isinstance(payload.get("previous"), dict) else {}
        current_state = current.get("state_label") or current.get("state") or "未知"
        previous_state = previous.get("state_label") or previous.get("state") or "未知"
        lines = [
            f"**状态变化**  {_lark_md(previous_state)} → {_lark_md(current_state)}",
            (f"**牛市分数**  {_number(previous.get('bull_score')):.1f} → "
             f"{_number(current.get('bull_score')):.1f}    **方向**  {direction}"),
        ]
        if current.get("return_1d") is not None:
            lines.append(f"**当日涨跌**  {_number(current['return_1d']) * 100:+.2f}%")
        if current.get("advance_ratio") is not None:
            lines.append(f"**上涨比例**  {_number(current['advance_ratio']) * 100:.1f}%")
        lines.append(f"**数据截至**  {item.get('data_as_of') or current.get('as_of') or '未知'}")
    elif kind == "task_failure":
        task_label = payload.get("task_label")
        lines = [
            (f"**任务**  {_lark_md(task_label)}" if task_label
             else f"**事项**  {_lark_md(payload.get('title') or '自动化任务未正常完成')}")
        ]
        phases = set(payload.get("phases") or [])
        phase_labels = [label for key, label in (("fetch", "资讯拉取"), ("analysis", "新闻分析"))
                        if key in phases]
        if phase_labels:
            lines.append("**异常阶段**  " + "、".join(phase_labels))
        impact = "本轮部分结果可用，其余项目将在后续调度中重试。" if payload.get("partial") else (
            "本轮任务未正常完成，系统将在后续调度中重试。")
        lines.extend([
            f"**影响**  {impact}",
            f"**发生时间**  {item.get('occurred_at') or item.get('data_as_of') or '未知'}",
        ])
    elif kind == "task_report":
        lines = [f"**{_lark_md(payload.get('title') or '自动化任务结果')}**"]
        result_lines = _feishu_result_lines(payload.get("result"))
        if result_lines:
            lines.extend(["", *result_lines])
        lines.append(f"**数据截至**  {item.get('data_as_of') or '未知'}")
    else:
        lines = [
            f"**强度**  {_number(item.get('score')):.0f}/100    **方向**  {direction}",
            f"**数据截至**  {item.get('data_as_of') or '未知'}",
        ]
    evidence = item.get("evidence") or []
    if evidence and not (kind == "important_news" and payload.get("digest")) \
            and kind != "market_close":
        evidence_title = {
            "task_failure": "错误详情", "market_turn": "触发依据",
            "task_report": "运行说明",
        }.get(str(kind), "核查依据")
        lines.extend(["", f"**{evidence_title}**", *[f"• {_lark_md(value)}" for value in evidence[:5]]])
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
            "title": {"tag": "plain_text", "content": category},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}},
            {"tag": "hr"},
            {"tag": "note", "elements": [{
                "tag": "plain_text", "content": _feishu_footer(kind),
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
        self.analysis_delivery_handler: Callable[[int], dict[str, int]] | None = None

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
        if self.analysis_delivery_handler:
            analysis = self.analysis_delivery_handler(limit)
            for key in result:
                result[key] += int(analysis.get(key) or 0)
        return result
