from __future__ import annotations

import email.utils
import logging
import os
import socket
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC
from typing import Any, Protocol

import httpx

from quantmaster.automation.channels import FeishuBotClient, WeixinClawBotClient
from quantmaster.automation.store import AutomationStore
from quantmaster.credentials import CredentialError

logger = logging.getLogger(__name__)


class DeliveryError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        permanent: bool = False,
        needs_rebind: bool = False,
        retry_after_at: float = 0.0,
        diagnostic_code: str = "delivery_failed",
        ambiguous: bool = False,
    ):
        super().__init__(message)
        self.permanent = permanent
        self.needs_rebind = needs_rebind
        self.retry_after_at = max(0.0, float(retry_after_at or 0.0))
        self.diagnostic_code = str(diagnostic_code or "delivery_failed")[:80]
        self.ambiguous = bool(ambiguous)


def _retry_after_at(headers: Any) -> float:
    raw = str((headers or {}).get("Retry-After") or "").strip()
    if not raw:
        return 0.0
    try:
        return time.time() + max(0.0, min(float(raw), 7 * 86400.0))
    except ValueError:
        try:
            value = email.utils.parsedate_to_datetime(raw)
            if value.tzinfo is None:
                value = value.replace(tzinfo=UTC)
            return max(time.time(), value.timestamp())
        except (TypeError, ValueError, OverflowError):
            return 0.0


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


def _feishu_footer(kind: Any, payload: dict[str, Any] | None = None) -> str:
    if kind == "task_failure":
        if (payload or {}).get("terminal"):
            return "已停止自动重试的资讯可在 QuantMaster 资讯分析队列中核查并恢复。"
        return "系统会按计划重试；若连续出现，请检查自动化任务与数据源状态。"
    if kind == "task_report":
        return "可在 QuantMaster 自动化页面查看完整运行记录。"
    return "仅作量化研究与记录，不构成投资建议。"


def _alert_digest_lines(payload: dict[str, Any]) -> list[str]:
    counts = _digest_counts(payload)
    lines = [
        f"共 {len(payload.get('items') or [])} 条 · 利好 {counts['up']} · "
        f"利空 {counts['down']} · 中性 {counts['neutral']}",
    ]
    for index, child in enumerate((payload.get("items") or [])[:5], 1):
        lines.append(
            f"{index}. [{_news_direction(child.get('direction'))}] "
            f"{_trim(child.get('title') or '消息', 100)}",
        )
        if child.get("summary"):
            lines.append(f"   摘要：{_trim(child['summary'], 180)}")
        if child.get("symbols"):
            lines.append("   标的：" + "、".join(child["symbols"][:6]))
        if child.get("sectors"):
            lines.append("   板块：" + "、".join(child["sectors"][:5]))
        if str(child.get("url") or "").startswith(("https://", "http://")):
            lines.append(f"   原文：{child['url']}")
    return lines


def _alert_news_lines(item: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    sentiment = _number(payload.get("sentiment"))
    lines = [
        f"研判 {_news_direction(item.get('direction'))} ({sentiment:+.2f}) · "
        f"重要度 {_number(item.get('score')):.0f}/100",
    ]
    if payload.get("summary"):
        lines.append("摘要：" + _trim(payload["summary"], 200))
    if payload.get("sectors"):
        lines.append("相关板块：" + "、".join(payload["sectors"][:5]))
    lines.append(f"数据截至：{item.get('data_as_of') or '未知'}")
    return lines


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
        lines.extend(_alert_digest_lines(payload))
    elif kind == "important_news":
        lines.extend(_alert_news_lines(item, payload))
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


def _feishu_digest_lines(item: dict[str, Any], payload: dict[str, Any]) -> list[str]:
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
        lines.extend(["", f"**{index} · {_news_direction(child.get('direction'))}**  {child_title}"])
        if child.get("summary"):
            lines.append(f"摘要：{_lark_md(child['summary'])}")
        if child.get("symbols"):
            lines.append("标的：" + "、".join(child["symbols"][:6]))
        if child.get("sectors"):
            lines.append("板块：" + "、".join(child["sectors"][:5]))
    return lines


def _feishu_news_lines(item: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    sentiment = _number(payload.get("sentiment"))
    lines = [
        f"**{_lark_md(payload.get('title') or '未命名资讯')}**", "",
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
    return lines


def _feishu_market_turn_lines(item: dict[str, Any], payload: dict[str, Any], direction: str) -> list[str]:
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
    return lines


def _feishu_market_close_lines(item: dict[str, Any], payload: dict[str, Any], direction: str) -> list[str]:
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
    return lines


def _feishu_task_failure_lines(item: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    task_label = payload.get("task_label")
    lines = [f"**任务**  {_lark_md(task_label)}" if task_label else
             f"**事项**  {_lark_md(payload.get('title') or '自动化任务未正常完成')}"]
    phases = set(payload.get("phases") or [])
    phase_labels = [
        label for key, label in (("fetch", "资讯拉取"), ("analysis", "新闻分析"))
        if key in phases
    ]
    if phase_labels:
        lines.append("**异常阶段**  " + "、".join(phase_labels))
    if not payload.get("partial"):
        impact = "本轮任务未正常完成，系统将在后续调度中重试。"
    elif payload.get("terminal"):
        impact = f"{int(payload.get('dead_letter') or 0)} 条资讯已停止自动重试，请核查后恢复。"
    else:
        impact = "本轮部分结果可用，其余项目将在后续调度中重试。"
    lines.extend([
        f"**影响**  {impact}",
        f"**发生时间**  {item.get('occurred_at') or item.get('data_as_of') or '未知'}",
    ])
    return lines


def _feishu_task_report_lines(item: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    lines = [f"**{_lark_md(payload.get('title') or '自动化任务结果')}**"]
    result_lines = _feishu_result_lines(payload.get("result"))
    if result_lines:
        lines.extend(["", *result_lines])
    lines.append(f"**数据截至**  {item.get('data_as_of') or '未知'}")
    return lines


def _feishu_lines(item: dict[str, Any], kind: str, payload: dict[str, Any], direction: str) -> list[str]:
    if kind == "important_news":
        return (
            _feishu_digest_lines(item, payload)
            if payload.get("digest") else _feishu_news_lines(item, payload)
        )
    builders = {
        "market_turn": lambda: _feishu_market_turn_lines(item, payload, direction),
        "market_close": lambda: _feishu_market_close_lines(item, payload, direction),
        "task_failure": lambda: _feishu_task_failure_lines(item, payload),
        "task_report": lambda: _feishu_task_report_lines(item, payload),
    }
    return builders.get(kind, lambda: [
        f"**强度**  {_number(item.get('score')):.0f}/100    **方向**  {direction}",
        f"**数据截至**  {item.get('data_as_of') or '未知'}",
    ])()


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
    lines = _feishu_lines(item, kind, payload, direction)
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
                "tag": "plain_text", "content": _feishu_footer(kind, payload),
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

    def _deliver(self, delivery: dict[str, Any], message: str) -> None:
        if delivery["channel"] == "weixin":
            if not delivery.get("context_token"):
                raise DeliveryError(
                    "微信会话尚无 context_token，请先给 ClawBot 发送一条消息",
                    permanent=True, needs_rebind=True,
                )
            self.weixin.send(
                account_id=delivery["account_id"], to_user_id=delivery["target"],
                context_token=delivery["context_token"], text=message,
            )
            return
        if delivery["channel"] == "feishu":
            self.feishu.send_card(
                chat_id=delivery["target"], card=format_feishu_card(delivery),
            )
            return
        raise DeliveryError(f"不支持的推送频道：{delivery['channel']}", permanent=True)

    def _deliver_with_errors(self, delivery: dict[str, Any], message: str) -> None:
        try:
            self._deliver(delivery, message)
        except DeliveryError:
            raise
        except CredentialError as exc:
            raise DeliveryError(
                str(exc), permanent=True, needs_rebind=True,
                diagnostic_code="credentials_invalid",
            ) from exc
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            body = exc.response.text[:500]
            needs_rebind = code in {400, 404} and any(
                word in body.lower() for word in ("context", "recipient", "chat_id", "user"))
            raise DeliveryError(
                f"{delivery['channel']} 接口返回 {code}: {body}",
                permanent=code in {400, 401, 403, 404}, needs_rebind=needs_rebind,
                retry_after_at=_retry_after_at(exc.response.headers),
                diagnostic_code=f"http_{code}",
            ) from exc
        except httpx.HTTPError as exc:
            raise DeliveryError(
                f"{delivery['channel']} 接口暂时不可达: {exc}",
                diagnostic_code="transport_error",
            ) from exc
        except RuntimeError as exc:
            error = str(exc)
            permanent = any(word in error for word in (
                "尚未配置", "不存在", "未安装", "token", "App ID", "App Secret",
            ))
            needs_rebind = "context_token" in error or "重新扫码" in error
            code = "not_configured" if permanent else (
                "rate_limited" if any(value in error.casefold() for value in (
                    "429", "rate limit", "too many",
                )) else "transport_error"
            )
            raise DeliveryError(
                error, permanent=permanent, needs_rebind=needs_rebind,
                diagnostic_code=code,
            ) from exc
        except (OSError, ValueError, TypeError, AttributeError, ImportError) as exc:
            error = str(exc)
            diagnostic = "tls_error" if any(value in error.casefold() for value in (
                "ssl", "tls", "certificate",
            )) else "transport_error"
            raise DeliveryError(error, diagnostic_code=diagnostic) from exc

    def send(self, delivery: dict[str, Any]) -> None:
        if not delivery.get("target") or not delivery.get("account_id"):
            raise DeliveryError("推送目标尚未绑定", permanent=True, needs_rebind=True)
        message = format_alert(delivery, delivery["channel"])
        self._deliver_with_errors(delivery, message)


class OutboxDispatcher:
    def __init__(self, store: AutomationStore, gateway: DeliveryGateway | None = None):
        self.store = store
        self.gateway = gateway or BotDeliveryGateway(store)
        self._injected_gateway = gateway is not None
        self.analysis_delivery_handler: Callable[[int, str], dict[str, int]] | None = None
        self.owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self._worker_lock = threading.Lock()
        self._wake_event = threading.Event()
        self._stop_event = threading.Event()
        self._worker_thread: threading.Thread | None = None
        self._dispatching = False
        self.coalesced_triggers = 0

    def available_channels(self) -> set[str]:
        advertised = getattr(self.gateway, "available_channels", None)
        if callable(advertised):
            return {str(value) for value in advertised()}
        if self._injected_gateway:
            return {
                str(target["channel"])
                for target in self.store.targets()
                if target.get("enabled") and target.get("target") and target.get("account_id")
            }
        result: set[str] = set()
        if self.store.bot_account("weixin"):
            result.add("weixin")
        feishu = self.store.bot_account("feishu")
        if feishu and str(feishu.get("status") or "") not in {"disabled", "not_configured"}:
            configured = getattr(getattr(self.gateway, "feishu", None), "is_configured", None)
            if not callable(configured) or configured():
                result.add("feishu")
        return result

    def enabled(self) -> bool:
        return bool(self.available_channels())

    def start(self) -> bool:
        """Start the single durable delivery worker without performing I/O inline."""
        if not self.enabled():
            return False
        with self._worker_lock:
            if self._worker_thread and self._worker_thread.is_alive():
                return False
            self._stop_event.clear()
            self._wake_event.clear()
            worker = threading.Thread(
                target=self._worker,
                name="qm-outbox-delivery",
                daemon=True,
            )
            self._worker_thread = worker
            worker.start()
        return True

    def wake(self) -> bool:
        """Quick APScheduler callback; overlapping triggers collapse into one wake."""
        with self._worker_lock:
            coalesced = self._dispatching or self._wake_event.is_set()
            if coalesced:
                self.coalesced_triggers += 1
            self._wake_event.set()
        return not coalesced

    def stop(self, timeout: float = 5.0) -> None:
        with self._worker_lock:
            worker = self._worker_thread
            self._stop_event.set()
            self._wake_event.set()
        if worker and worker is not threading.current_thread():
            worker.join(timeout=max(0.0, timeout))
        with self._worker_lock:
            if self._worker_thread is worker and (not worker or not worker.is_alive()):
                self._worker_thread = None
                self._dispatching = False

    def worker_status(self) -> dict[str, Any]:
        with self._worker_lock:
            worker = self._worker_thread
            return {
                "running": bool(worker and worker.is_alive()),
                "dispatching": self._dispatching,
                "wake_pending": self._wake_event.is_set(),
                "coalesced_triggers": self.coalesced_triggers,
            }

    def _worker(self) -> None:
        while True:
            self._wake_event.wait()
            self._wake_event.clear()
            if self._stop_event.is_set():
                return
            with self._worker_lock:
                self._dispatching = True
            try:
                self.dispatch()
            except (sqlite3.Error, OSError):
                logger.exception("outbox durable worker cycle failed")
            finally:
                with self._worker_lock:
                    self._dispatching = False

    def _send(self, item: dict[str, Any], token: str) -> None:
        stop = threading.Event()

        def renew() -> None:
            while not stop.wait(30.0):
                try:
                    if not self.store.heartbeat_delivery(
                        str(item["id"]), self.owner, token,
                    ):
                        return
                except (sqlite3.Error, OSError):
                    logger.warning(
                        "outbox delivery heartbeat failed id=%s", item["id"], exc_info=True,
                    )

        heartbeat = threading.Thread(
            target=renew, name=f"outbox-heartbeat-{str(item['id'])[-8:]}", daemon=True,
        )
        heartbeat.start()
        try:
            self.gateway.send(item)
        finally:
            stop.set()
            heartbeat.join(timeout=1.0)

    def dispatch(self, limit: int = 20) -> dict[str, int]:
        result = {"delivered": 0, "failed": 0, "retried": 0}
        channels = self.available_channels()
        if not channels:
            return result
        for item in self.store.claim_deliveries(
            self.owner, limit=limit, channels=channels,
        ):
            outcome = self._dispatch_item(item)
            if outcome:
                result[outcome] += 1
        if self.analysis_delivery_handler and "feishu" in channels:
            analysis = self.analysis_delivery_handler(limit, self.owner)
            for key in ("delivered", "failed", "retried"):
                result[key] += int(analysis.get(key) or 0)
        return result

    def _dispatch_item(self, item: dict[str, Any]) -> str:
        token = str(item["lease_token"])
        if not self.store.begin_delivery(str(item["id"]), self.owner, token):
            return ""
        try:
            self._send(item, token)
        except DeliveryError as exc:
            return self._record_delivery_error(item, token, exc)
        except (
            OSError, ValueError, TypeError, AttributeError, ImportError, RuntimeError,
        ) as exc:
            logger.exception("outbox delivery outcome is unknown id=%s", item["id"])
            self.store.delivery_failure(
                str(item["id"]), self.owner, token, str(exc), ambiguous=True,
                diagnostic_code="delivery_outcome_unknown",
            )
            self.store.set_target_status(item["target_id"], "degraded", str(exc))
            return "failed"
        return self._ack_delivery(item, token)

    def _record_delivery_error(
        self, item: dict[str, Any], token: str, exc: DeliveryError,
    ) -> str:
        status = self.store.delivery_failure(
            str(item["id"]), self.owner, token, str(exc),
            permanent=exc.permanent, ambiguous=exc.ambiguous,
            retry_after_at=exc.retry_after_at, diagnostic_code=exc.diagnostic_code,
        )
        if exc.needs_rebind:
            self.store.set_target_status(item["target_id"], "needs_rebind", str(exc))
        elif exc.permanent:
            self.store.set_target_status(item["target_id"], "degraded", str(exc))
        return "retried" if status == "retry_wait" else "failed"

    def _ack_delivery(self, item: dict[str, Any], token: str) -> str:
        try:
            acknowledged = self.store.delivery_success(
                str(item["id"]), self.owner, token,
            )
        except (sqlite3.Error, OSError):
            logger.exception(
                "outbox delivery ack failed; item will be quarantined after lease expiry id=%s",
                item["id"],
            )
            return ""
        if not acknowledged:
            logger.error(
                "outbox delivery ack lost; item quarantined on lease recovery id=%s", item["id"],
            )
            return ""
        self.store.set_target_status(item["target_id"], "healthy")
        return "delivered"
