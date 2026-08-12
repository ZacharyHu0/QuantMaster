from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import UTC, datetime, timedelta

import httpx
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from quantmaster.ai.crawler import NewsItem
from quantmaster.automation.channels.feishu import FeishuBotClient, feishu_connection_error
from quantmaster.automation.channels.weixin import WeixinClawBotClient
from quantmaster.automation.commands import BotCommandRouter
from quantmaster.automation.delivery import OutboxDispatcher, format_alert, format_feishu_card
from quantmaster.automation.models import ActorContext, AlertEvent
from quantmaster.automation.news import importance_score, news_event
from quantmaster.automation.policy import EVENT_KINDS, policy_allows, resolved_policy
from quantmaster.automation.service import NEWS_TASKS, AutomationService
from quantmaster.automation.store import AutomationStore
from quantmaster.data.base import BarDataEnvelope, BarDataQuality
from quantmaster.server.app import app


class MemoryCredentials:
    def __init__(self):
        self.values = {}

    def get(self, target):
        return self.values.get(target)

    def set(self, target, value):
        self.values[target] = value

    def delete(self, target):
        self.values.pop(target, None)


def test_legacy_news_schedules_are_replaced_by_settings_defaults(tmp_path):
    path = tmp_path / "automation.sqlite"
    AutomationStore(path)
    expected = {
        "fast_news_scan": {"type": "interval", "minutes": 1},
        "official_news_scan": {"type": "interval", "minutes": 10},
        "periodic_news_scan": {"type": "interval", "minutes": 60},
    }
    with sqlite3.connect(path) as connection:
        for name, schedule in expected.items():
            connection.execute(
                "UPDATE job_templates SET schedule=? WHERE name=?",
                (json.dumps(schedule), name),
            )
        connection.execute("PRAGMA user_version=6")

    migrated = AutomationStore(path)
    schedules = {item["name"]: item["schedule"] for item in migrated.jobs()}
    assert {name: schedules[name] for name in expected} == {
        "fast_news_scan": {"type": "interval", "minutes": 20},
        "official_news_scan": {"type": "interval", "minutes": 120},
        "periodic_news_scan": {"type": "interval", "minutes": 360},
    }


def test_news_schedule_defaults_follow_settings(tmp_path, isolated_config):
    isolated_config.automation.fast_news_interval_minutes = 25
    isolated_config.automation.official_news_interval_minutes = 150
    isolated_config.automation.periodic_news_interval_minutes = 420

    store = AutomationStore(tmp_path / "automation.sqlite")

    assert [store.job(name)["schedule"]["minutes"] for name in (
        "fast_news_scan", "official_news_scan", "periodic_news_scan",
    )] == [25, 150, 420]


def test_news_interval_setting_hot_syncs_scheduler_projection(tmp_path, isolated_config):
    store = AutomationStore(tmp_path / "automation.sqlite")
    isolated_config.automation.fast_news_interval_minutes = 45
    isolated_config.automation.official_news_interval_minutes = 180
    isolated_config.automation.periodic_news_interval_minutes = 480

    assert store.sync_news_intervals() == {
        "fast_news_scan": 45,
        "official_news_scan": 180,
        "periodic_news_scan": 480,
    }
    assert [store.job(name)["schedule"]["minutes"] for name in (
        "fast_news_scan", "official_news_scan", "periodic_news_scan",
    )] == [45, 180, 480]


class RecordingGateway:
    def __init__(self):
        self.items = []

    def send(self, delivery):
        self.items.append(delivery)


class FailingGateway(RecordingGateway):
    def __init__(self, error):
        super().__init__()
        self.error = error

    def send(self, delivery):
        super().send(delivery)
        raise self.error


class WeixinOnlyGateway(RecordingGateway):
    def available_channels(self):
        return {"weixin"}


class BlockingGateway(RecordingGateway):
    def __init__(self):
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.thread_ids = []

    def send(self, delivery):
        super().send(delivery)
        self.thread_ids.append(threading.get_ident())
        self.entered.set()
        assert self.release.wait(5)


def _response(method: str, url: str, payload: dict) -> httpx.Response:
    return httpx.Response(200, json=payload, request=httpx.Request(method, url))


def test_policy_presets_and_overrides():
    conservative = resolved_policy("conservative")
    sensitive = resolved_policy("sensitive")
    assert conservative["regime_threshold"] > sensitive["regime_threshold"]
    assert policy_allows({"kind": "market_turn", "score": 60}, sensitive)
    assert not policy_allows({"kind": "market_turn", "score": 60}, conservative)
    custom = resolved_policy("balanced", {
        "regime_threshold": 72, "news_thresholds": {"holding": 55},
    })
    assert custom["regime_threshold"] == 72
    assert custom["news_thresholds"] == {"holding": 55, "watchlist": 75, "market": 80}
    assert custom["event_types"] == list(EVENT_KINDS)
    silent = resolved_policy("balanced", {"event_types": []})
    assert silent["event_types"] == []
    assert not policy_allows({"kind": "task_failure", "score": 100}, silent)
    with pytest.raises(ValueError, match="事件类型列表"):
        resolved_policy("balanced", {"event_types": "important_news"})


def test_content_subscription_is_absolute_but_explicit_test_bypasses_it(tmp_path):
    store = AutomationStore(tmp_path / "automation.sqlite")
    store.bind_target(
        "feishu_owner", target="oc_chat", account_id="cli_app",
        owner_actor="feishu:cli_app:ou_owner", actor="test",
    )
    store.update_target_policy(
        "feishu_owner", preset="balanced", overrides={"event_types": []}, actor="test",
    )
    gateway = RecordingGateway()
    service = AutomationService(store, OutboxDispatcher(store, gateway))

    critical = AlertEvent(
        kind="important_news", score=100, severity="critical",
        dedupe_key="critical-unsubscribed", payload={"title": "重大资讯"},
    )
    failure = AlertEvent(
        kind="task_failure", score=100, severity="critical",
        dedupe_key="failure-unsubscribed", payload={"title": "任务失败"},
    )
    assert service.process_event(critical, {"feishu_owner"})["enqueued"] == 0
    assert service.process_event(failure, {"feishu_owner"})["enqueued"] == 0

    result = service.test_target("feishu_owner")
    assert result["enqueued"] == 1
    assert result["dispatch"]["delivered"] == 1
    assert gateway.items[0]["kind"] == "task_report"


def test_store_binding_outbox_and_delivery(tmp_path):
    store = AutomationStore(tmp_path / "automation.sqlite")
    assert store.inbound_status("feishu") == {"total": 0, "last_received_at": ""}
    assert store.claim_inbound(
        "feishu", "om_probe", chat_type="direct", account_id="cli_app") is True
    assert store.inbound_status("feishu")["total"] == 1
    assert store.inbound_status("feishu", "direct")["total"] == 1
    assert store.inbound_status("feishu", "group")["total"] == 0
    store.bind_target(
        "feishu_owner", target="oc_chat", account_id="cli_app",
        owner_actor="feishu:cli_app:ou_owner", actor="test",
    )
    gateway = RecordingGateway()
    service = AutomationService(store, OutboxDispatcher(store, gateway))
    event = AlertEvent(
        kind="important_news", score=90, severity="high", relevance="holding",
        data_as_of="2026-07-27T10:00:00+08:00", evidence=["官方公告"],
        payload={"title": "持仓公司重大事项"},
    )
    result = service.process_event(event, {"feishu_owner"})
    assert result["enqueued"] == 1
    assert service.dispatcher.dispatch() == {"delivered": 1, "failed": 0, "retried": 0}
    assert gateway.items[0]["target"] == "oc_chat"
    assert service.dispatcher.dispatch()["delivered"] == 0


def _queued_delivery(tmp_path, *, target_id="feishu_owner", channel="feishu"):
    store = AutomationStore(tmp_path / f"{target_id}.sqlite")
    store.bind_target(
        target_id, target="oc_chat", account_id="cli_app",
        owner_actor=f"{channel}:cli_app:owner", actor="test",
    )
    event = AlertEvent(
        kind="task_report",
        score=0,
        severity="info",
        dedupe_key=f"event-{target_id}",
    )
    saved, _ = store.save_event(event)
    assert store.enqueue(saved["id"], target_id)
    return store


def test_outbox_claim_is_atomic_fenced_and_sent_is_not_reclaimed(tmp_path):
    store = _queued_delivery(tmp_path)
    first = store.claim_deliveries("worker-a", channels={"feishu"})
    second = store.claim_deliveries("worker-b", channels={"feishu"})
    assert len(first) == 1 and second == []
    item = first[0]
    assert not store.begin_delivery(item["id"], "worker-b", item["lease_token"])
    assert store.begin_delivery(item["id"], "worker-a", item["lease_token"])
    assert store.delivery_success(item["id"], "worker-a", item["lease_token"])
    assert store.claim_deliveries("worker-b", channels={"feishu"}) == []
    with store._conn() as connection:
        row = connection.execute(
            "SELECT status,attempts FROM delivery_attempts WHERE id=?", (item["id"],),
        ).fetchone()
    assert dict(row) == {"status": "sent", "attempts": 1}


def test_outbox_expired_claim_recovers_but_expired_sending_is_ambiguous(tmp_path):
    store = _queued_delivery(tmp_path)
    claimed = store.claim_deliveries("worker-a", channels={"feishu"})[0]
    with store._conn() as connection:
        connection.execute(
            "UPDATE delivery_attempts SET lease_expires_at=0 WHERE id=?", (claimed["id"],),
        )
    recovered = store.claim_deliveries("worker-b", channels={"feishu"})[0]
    assert recovered["id"] == claimed["id"]
    assert store.begin_delivery(recovered["id"], "worker-b", recovered["lease_token"])
    with store._conn() as connection:
        connection.execute(
            "UPDATE delivery_attempts SET lease_expires_at=0 WHERE id=?", (claimed["id"],),
        )
    assert store.claim_deliveries("worker-c", channels={"feishu"}) == []
    with store._conn() as connection:
        row = connection.execute(
            "SELECT status,diagnostic_code,ambiguous_at FROM delivery_attempts WHERE id=?",
            (claimed["id"],),
        ).fetchone()
    assert row["status"] == "ambiguous"
    assert row["diagnostic_code"] == "delivery_ack_unknown"
    assert row["ambiguous_at"]


def test_outbox_send_success_with_ack_failure_is_never_blindly_retried(
    tmp_path, monkeypatch,
):
    store = _queued_delivery(tmp_path)
    gateway = RecordingGateway()
    dispatcher = OutboxDispatcher(store, gateway)
    monkeypatch.setattr(
        store, "delivery_success",
        lambda *_args: (_ for _ in ()).throw(sqlite3.OperationalError("ack unavailable")),
    )

    assert dispatcher.dispatch() == {"delivered": 0, "failed": 0, "retried": 0}
    assert len(gateway.items) == 1
    with store._conn() as connection:
        row = connection.execute(
            "SELECT id,status FROM delivery_attempts",
        ).fetchone()
        assert row["status"] == "sending"
        connection.execute(
            "UPDATE delivery_attempts SET lease_expires_at=0 WHERE id=?", (row["id"],),
        )
    assert dispatcher.dispatch() == {"delivered": 0, "failed": 0, "retried": 0}
    assert len(gateway.items) == 1
    with store._conn() as connection:
        assert connection.execute(
            "SELECT status FROM delivery_attempts",
        ).fetchone()[0] == "ambiguous"


def test_outbox_retry_after_and_dead_letter_are_item_granular(tmp_path):
    from quantmaster.automation.delivery import DeliveryError

    store = _queued_delivery(tmp_path)
    retry_at = time.time() + 600
    gateway = FailingGateway(DeliveryError(
        "limited", retry_after_at=retry_at, diagnostic_code="http_429",
    ))
    dispatcher = OutboxDispatcher(store, gateway)
    assert dispatcher.dispatch() == {"delivered": 0, "failed": 0, "retried": 1}
    with store._conn() as connection:
        row = connection.execute(
            "SELECT status,next_attempt_at,retry_after_at,diagnostic_code "
            "FROM delivery_attempts",
        ).fetchone()
    assert row["status"] == "retry_wait"
    assert row["next_attempt_at"] >= retry_at
    assert row["retry_after_at"] == pytest.approx(retry_at)
    assert row["diagnostic_code"] == "http_429"
    assert dispatcher.dispatch() == {"delivered": 0, "failed": 0, "retried": 0}

    with store._conn() as connection:
        connection.execute(
            "UPDATE delivery_attempts SET next_attempt_at=0,attempts=4 WHERE status='retry_wait'"
        )
    assert dispatcher.dispatch() == {"delivered": 0, "failed": 1, "retried": 0}
    with store._conn() as connection:
        assert connection.execute(
            "SELECT status FROM delivery_attempts",
        ).fetchone()[0] == "dead_letter"


def test_outbox_skips_unavailable_feishu_but_delivers_weixin(tmp_path):
    store = AutomationStore(tmp_path / "automation.sqlite")
    for channel in ("feishu", "weixin"):
        target_id = f"{channel}_owner"
        store.bind_target(
            target_id,
            target=f"{channel}_target",
            account_id=f"{channel}_account",
            owner_actor=f"{channel}:{channel}_account:owner",
            actor="test",
        )
        saved, _ = store.save_event(AlertEvent(
            kind="task_report",
            score=0,
            severity="info",
            dedupe_key=f"event-{channel}",
        ))
        assert store.enqueue(saved["id"], target_id)

    gateway = WeixinOnlyGateway()
    dispatcher = OutboxDispatcher(store, gateway)

    assert dispatcher.dispatch() == {"delivered": 1, "failed": 0, "retried": 0}
    assert [item["channel"] for item in gateway.items] == ["weixin"]
    with store._conn() as connection:
        rows = connection.execute(
            "SELECT nt.channel,da.status FROM delivery_attempts da "
            "JOIN notification_targets nt ON nt.id=da.target_id ORDER BY nt.channel"
        ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("feishu", "pending"), ("weixin", "sent"),
    ]


def test_outbox_worker_is_single_instance_and_coalesces_overlapping_wakes(tmp_path):
    store = _queued_delivery(tmp_path)
    gateway = BlockingGateway()
    dispatcher = OutboxDispatcher(store, gateway)

    assert dispatcher.start()
    caller_thread = threading.get_ident()
    worker = dispatcher._worker_thread
    assert worker is not None
    assert not dispatcher.start()
    assert dispatcher._worker_thread is worker
    assert dispatcher.wake()
    assert gateway.entered.wait(2)
    assert gateway.thread_ids == [worker.ident]
    assert gateway.thread_ids != [caller_thread]

    assert not dispatcher.wake()
    assert not dispatcher.wake()
    assert dispatcher.worker_status()["coalesced_triggers"] == 2
    gateway.release.set()
    deadline = time.time() + 2
    while dispatcher.worker_status()["dispatching"] and time.time() < deadline:
        time.sleep(0.01)
    dispatcher.stop()

    assert len(gateway.items) == 1
    assert not dispatcher.worker_status()["running"]


def test_binding_code_is_single_use(tmp_path):
    store = AutomationStore(tmp_path / "automation.sqlite")
    code = store.create_binding_code("feishu_owner")["code"]
    assert store.consume_binding_code(code)["payload"]["target_id"] == "feishu_owner"
    assert store.consume_binding_code(code) is None


def test_binding_flow_preserves_code_on_wrong_chat_and_requires_owner_first(tmp_path):
    store = AutomationStore(tmp_path / "automation.sqlite")
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    with pytest.raises(ValueError, match="管理员私聊"):
        service.create_binding("feishu_group")

    binding = service.create_binding("feishu_owner")
    wrong_chat = ActorContext(
        channel="feishu", account_id="cli_app", target="oc_group",
        chat_type="group", sender_id="ou_owner",
    )
    with pytest.raises(ValueError, match="会话类型"):
        service.bind(wrong_chat, binding["code"])
    assert service.binding_status(binding["id"])["status"] == "pending"

    owner = ActorContext(
        channel="feishu", account_id="cli_app", target="oc_owner",
        chat_type="direct", sender_id="ou_owner",
    )
    service.bind(owner, binding["code"])
    assert service.binding_status(binding["id"])["status"] == "bound"

    group_binding = service.create_binding("feishu_group")
    service.bind(wrong_chat, group_binding["code"])
    assert service.binding_status(group_binding["id"])["status"] == "bound"


def test_weixin_qr_auth_and_context_send(tmp_path, monkeypatch):
    store = AutomationStore(tmp_path / "automation.sqlite")
    credentials = MemoryCredentials()
    calls = []

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        if url.endswith("get_bot_qrcode"):
            return _response("POST", url, {
                "ret": 0, "qrcode": "qr-id", "qrcode_img_content": "https://weixin.example/qr",
            })
        if url.endswith("sendmessage"):
            return _response("POST", url, {"ret": 0})
        raise AssertionError(url)

    def fake_get(url, **kwargs):
        return _response("GET", url, {
            "status": "confirmed", "bot_token": "bot-secret", "ilink_bot_id": "bot-1",
            "ilink_user_id": "wx-user", "baseurl": "https://ilink.example",
        })

    monkeypatch.setattr("quantmaster.automation.channels.weixin.httpx.post", fake_post)
    monkeypatch.setattr("quantmaster.automation.channels.weixin.httpx.get", fake_get)
    client = WeixinClawBotClient(store, credentials, "https://ilinkai.weixin.qq.com")
    login = client.start_login()
    result = client.poll_login(login["session_id"])
    assert result["status"] == "confirmed"
    assert credentials.values["bot:weixin:bot-1"] == "bot-secret"
    assert store.target("weixin_owner")["target"] == "wx-user"

    client.send(
        account_id="bot-1", to_user_id="wx-user", context_token="context-1", text="测试",
    )
    send = next(value for url, value in calls if url.endswith("sendmessage"))
    assert send["json"]["msg"]["context_token"] == "context-1"
    assert send["json"]["msg"]["message_type"] == 2
    assert send["headers"]["Authorization"] == "Bearer bot-secret"
    assert send["headers"]["AuthorizationType"] == "ilink_bot_token"


def test_chat_can_change_current_target_policy(tmp_path):
    store = AutomationStore(tmp_path / "automation.sqlite")
    actor = ActorContext(
        channel="feishu", account_id="cli_app", target="oc_owner",
        chat_type="direct", sender_id="ou_owner",
    )
    store.bind_target(
        "feishu_owner", target=actor.target, account_id=actor.account_id,
        owner_actor=actor.actor_key, actor="test",
    )
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    router = BotCommandRouter(service, lambda *_: None)
    answer = router.execute(actor, "把当前推送强度调成敏感")
    assert "敏感" in answer
    assert store.target("feishu_owner")["preset"] == "sensitive"

    answer = router.execute(actor, "提醒少一点")
    assert "保守" in answer
    assert store.target("feishu_owner")["preset"] == "conservative"


def test_bot_has_contextual_help_and_actionable_fallback(tmp_path):
    store = AutomationStore(tmp_path / "automation.sqlite")
    actor = ActorContext(
        channel="feishu", account_id="cli_app", target="oc_owner",
        chat_type="direct", sender_id="ou_owner",
    )
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    router = BotCommandRouter(service, lambda *_: None)

    unbound_help = router.execute(actor, "怎么用？")
    assert "当前会话还没有绑定" in unbound_help
    assert "自动化 → Bot 推送" in unbound_help

    store.bind_target(
        "feishu_owner", target=actor.target, account_id=actor.account_id,
        owner_actor=actor.actor_key, actor="test",
    )
    help_text = router.execute(actor, "帮助")
    assert "固定操作由受控指令助手执行" in help_text
    assert "现在大盘怎么样" in help_text
    assert "仅管理员私聊" in help_text

    service.query = lambda view: {"resolved_view": view}
    natural_query = router.execute(actor, "现在大盘怎么样？")
    assert '"resolved_view": "market"' in natural_query

    service.contextual_chat = lambda current_actor, text: "AI 已结合上下文回答"
    fallback = router.execute(actor, "帮我预测十年后的股价")
    assert fallback == "AI 已结合上下文回答"


def test_group_help_explains_mention_and_permissions(tmp_path):
    store = AutomationStore(tmp_path / "automation.sqlite")
    owner_actor = "feishu:cli_app:ou_owner"
    store.bind_target(
        "feishu_owner", target="oc_owner", account_id="cli_app",
        owner_actor=owner_actor, actor="test",
    )
    store.bind_target(
        "feishu_group", target="oc_group", account_id="cli_app",
        owner_actor=owner_actor, actor="test",
    )
    actor = ActorContext(
        channel="feishu", account_id="cli_app", target="oc_group",
        chat_type="group", sender_id="ou_member",
    )
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    help_text = BotCommandRouter(service, lambda *_: None).execute(actor, "使用说明")
    assert "真正 @QuantMaster" in help_text
    assert "普通成员可以查询" in help_text
    assert "账本（仅管理员私聊" not in help_text


def test_contextual_chat_compacts_long_history_into_topic_memory(tmp_path, monkeypatch):
    store = AutomationStore(tmp_path / "automation.sqlite")
    owner_actor = "feishu:cli_app:ou_owner"
    store.bind_target(
        "feishu_group", target="oc_group", account_id="cli_app",
        owner_actor=owner_actor, actor="test",
    )
    started = datetime(2026, 7, 1, tzinfo=UTC)
    for index in range(65):
        topic = "贵州茅台 600519 估值存在分歧" if index % 9 == 0 else "行业日常讨论"
        store.remember_conversation_message(
            channel="feishu", account_id="cli_app", chat_id="oc_group",
            message_id=f"om_{index:03d}", sender_id="ou_member",
            sender_name="群成员", text=f"{topic}，记录 {index}",
            created_at=(started + timedelta(minutes=index)).isoformat(timespec="seconds"),
        )
    store.remember_conversation_message(
        channel="feishu", account_id="cli_app", chat_id="oc_group",
        message_id="om_current", sender_id="ou_owner", sender_name="管理员",
        text="这个估值分歧具体是什么？", mentioned_bot=True,
        created_at=(started + timedelta(minutes=66)).isoformat(timespec="seconds"),
    )

    captured = {}

    class FakeLLMClient:
        def chat_json(self, prompt, system=None):
            captured["compact_prompt"] = prompt
            return {
                "topics": [{
                    "topic": "贵州茅台估值", "symbols": ["600519"],
                    "summary": "群内对估值存在分歧", "viewpoints": ["偏高", "可接受"],
                    "open_questions": ["需要最新盈利预测"],
                }],
                "timeline": [], "carryovers": [],
            }

        def chat(self, prompt, system=None, history=None):
            captured["answer_prompt"] = prompt
            captured["system"] = system
            return "群里近期围绕 600519 的估值有两类观点。"

    monkeypatch.setattr("quantmaster.ai.llm.LLMClient", FakeLLMClient)
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    replies: list[str] = []
    monkeypatch.setattr(service, "reply", lambda _actor, value: replies.append(value))
    actor = ActorContext(
        channel="feishu", account_id="cli_app", target="oc_group",
        chat_type="group", sender_id="ou_owner", sender_name="管理员",
        message_id="om_current", reply_text="贵州茅台是不是太贵了？",
    )

    accepted = service.contextual_chat(actor, "这个估值分歧具体是什么？")
    assert "后台回答队列" in accepted
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and not replies:
        time.sleep(0.01)
    assert replies and "600519" in replies[0]
    memory = store.conversation_memory(
        channel="feishu", account_id="cli_app", chat_id="oc_group",
    )
    assert memory["source_count"] > 0
    assert memory["memory"]["topics"][0]["topic"] == "贵州茅台估值"
    assert store.conversation_stats(
        channel="feishu", account_id="cli_app", chat_id="oc_group",
    )["count"] < 66
    assert "已有记忆" in captured["compact_prompt"]
    assert "已压缩的话题记忆" in captured["answer_prompt"]
    assert "600519" in captured["answer_prompt"]
    assert "不可信资料" in captured["system"]
    service.jobs.stop()
    service.executor.shutdown(wait=False, cancel_futures=True)


def test_official_holding_news_has_high_priority():
    item = NewsItem(
        source="sse", title="公司收到立案通知", content="重大事项",
        symbols=["600000.SH"], published_at="2026-07-27T09:00:00+08:00",
    )
    score, relevance, reasons = importance_score(item, {"600000.SH"}, set())
    assert score >= 95
    assert relevance == "holding"
    assert "官方来源 +10" in reasons


def test_feishu_primary_alert_uses_structured_card():
    card = format_feishu_card({
        "kind": "market_turn", "score": 88, "severity": "high", "direction": "down",
        "data_as_of": "2026-07-27T14:35:00+08:00", "symbols": ["000300.SH"],
        "evidence": ["4 个指数同向", "市场宽度快速下降"],
        "source_urls": ["https://example.com/source"],
        "payload": {"title": "指数与宽度同步转弱"},
    })
    assert card["header"]["template"] == "green"
    assert card["header"]["title"]["content"] == "盘中变盘"
    content = card["elements"][0]["text"]["content"]
    assert "指数与宽度同步转弱" in content
    assert "触发依据" in content
    assert "000300.SH" in content
    assert "https://example.com/source" in content


def test_feishu_market_close_and_task_result_use_category_headers():
    close_card = format_feishu_card({
        "kind": "market_close", "score": 82, "severity": "high", "direction": "up",
        "data_as_of": "2026-08-01", "evidence": ["市场状态 下行 → 震荡"],
        "payload": {
            "previous": {"state": "down", "state_label": "下行", "bull_score": 35.1},
            "current": {
                "state": "range", "state_label": "震荡", "bull_score": 52.3,
                "return_1d": 0.0235, "advance_ratio": 0.75,
            },
        },
    })
    assert close_card["header"]["title"]["content"] == "收盘状态"
    close_content = close_card["elements"][0]["text"]["content"]
    assert "**状态变化**  下行 → 震荡" in close_content
    assert "**牛市分数**  35.1 → 52.3" in close_content
    assert "**上涨比例**  75.0%" in close_content
    assert close_content.count("市场状态 下行 → 震荡") == 0

    task_card = format_feishu_card({
        "kind": "task_report", "score": 0, "severity": "info", "direction": "neutral",
        "data_as_of": "2026-08-01T15:05:00+08:00",
        "evidence": ["运行编号 abc123"],
        "payload": {
            "title": "收盘流水线已完成",
            "result": {"status": "completed", "signal_date": "2026-08-01", "picks": 6},
        },
    })
    assert task_card["header"]["title"]["content"] == "任务结果"
    task_content = task_card["elements"][0]["text"]["content"]
    assert "收盘流水线已完成" in task_content
    assert "**状态**  已完成" in task_content
    assert "**候选数量**  6" in task_content
    assert "**运行说明**" in task_content


def test_news_alert_surfaces_summary_and_bullish_bearish_judgement():
    event = news_event(NewsItem(
        source="sse", title="公司上调业绩预告", content="预计净利润同比增长",
        url="https://example.com/news", published_at="2026-07-27T10:00:00+08:00",
        symbols=["600000.SH"], sectors=["银行"], event_type="业绩", sentiment=0.72,
        summary="盈利预测上修，业绩增速超预期", is_official=True,
    ), {"600000.SH"}, set()).to_dict()

    text = format_alert(event, "weixin")
    assert "研判 利好 (+0.72)" in text
    assert "摘要：盈利预测上修，业绩增速超预期" in text
    assert "相关板块：银行" in text
    assert "核查依据" in text

    card = format_feishu_card(event)
    assert card["header"]["title"]["content"] == "重要资讯"
    content = card["elements"][0]["text"]["content"]
    assert "公司上调业绩预告" in content
    assert "**研判**  利好 (+0.72)" in content
    assert "**摘要**" in content
    assert "盈利预测上修" in content
    assert "**来源**  sse" in content
    assert "**类型**  业绩" in content
    assert "**范围**  持仓" in content
    assert "**相关板块**  银行" in content


def test_news_digest_contains_compact_directional_summaries(tmp_path):
    store = AutomationStore(tmp_path / "automation.sqlite")
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    for index, (direction, sentiment, summary) in enumerate((
        ("up", 0.6, "需求回暖带动盈利预期上修"),
        ("down", -0.5, "监管调查增加短期不确定性"),
    )):
        store.save_event(AlertEvent(
            kind="important_news", score=88 - index, severity="high",
            direction=direction, relevance="market", dedupe_key=f"digest-source-{index}",
            payload={
                "title": f"资讯 {index + 1}", "summary": summary, "sentiment": sentiment,
            },
        ))

    assert service._task_news_digest() == {"items": 2}
    digest = next(
        item for item in store.recent_events(10) if item.get("payload", {}).get("digest")
    )
    assert digest["kind"] == "important_news"
    assert digest["payload"]["counts"] == {"up": 1, "down": 1, "neutral": 0}
    text = format_alert(digest, "weixin")
    assert "利好 1 · 利空 1 · 中性 0" in text
    assert "[利好] 资讯 1" in text
    assert "需求回暖带动盈利预期上修" in text
    card = format_feishu_card(digest)
    assert card["header"]["title"]["content"] == "资讯摘要"
    assert "**1 · 利好**" in card["elements"][0]["text"]["content"]


@pytest.mark.parametrize("task_name", sorted(NEWS_TASKS))
def test_news_task_does_not_emit_generic_completion_report(tmp_path, monkeypatch, task_name):
    store = AutomationStore(tmp_path / "automation.sqlite")
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    emitted = []
    monkeypatch.setattr(service, f"_task_{task_name}", lambda: {"saved": 0, "events": 0})
    monkeypatch.setattr(
        service, "process_event", lambda event, *args, **kwargs: emitted.append(event) or {},
    )
    run_id = store.start_run(task_name, "test")
    service._run_task(run_id, task_name, "test")
    assert emitted == []
    assert store.recent_runs(1)[0]["status"] == "succeeded"


def test_news_task_reports_fetch_errors_once_per_day_and_suppresses_transient_analysis(
    tmp_path, monkeypatch,
):
    store = AutomationStore(tmp_path / "automation.sqlite")
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    monkeypatch.setattr(service, "_task_official_news_scan", lambda: {
        "sources": [{"source": "csrc", "fetched": 3, "saved": 1}],
        "errors": {"sse": "request failed token=top-secret"},
        "annotation": {
            "processed": 5, "completed": 3, "failed": 2,
            "retry_scheduled": 2, "dead_letter": 0,
        },
    })

    for _ in range(2):
        run_id = store.start_run("official_news_scan", "test")
        service._run_task(run_id, "official_news_scan", "test")

    failures = [event for event in store.recent_events(10) if event["kind"] == "task_failure"]
    assert len(failures) == 1
    failure = failures[0]
    assert failure["payload"]["title"] == "新闻拉取异常：官方资讯扫描"
    assert failure["payload"]["phases"] == ["fetch"]
    assert any("部分来源拉取失败" in value for value in failure["evidence"])
    assert not any("分析" in value for value in failure["evidence"])
    assert "top-secret" not in " ".join(failure["evidence"])
    assert [run["status"] for run in store.recent_runs(2)] == ["succeeded", "succeeded"]

    card = format_feishu_card(failure)
    assert card["header"]["title"]["content"] == "资讯拉取异常"
    content = card["elements"][0]["text"]["content"]
    assert "**任务**  官方资讯扫描" in content
    assert "**异常阶段**  资讯拉取" in content
    assert "**影响**  本轮部分结果可用" in content
    assert "**错误详情**" in content
    assert "系统会按计划重试" in card["elements"][-1]["elements"][0]["content"]


def test_news_transient_analysis_failure_stays_silent_until_dead_letter(tmp_path, monkeypatch):
    store = AutomationStore(tmp_path / "automation.sqlite")
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    monkeypatch.setattr(service, "_task_fast_news_scan", lambda: {
        "sources": [{"source": "eastmoney", "fetched": 2, "saved": 2}],
        "errors": {},
        "annotation": {
            "processed": 2, "completed": 0, "failed": 2,
            "retry_scheduled": 2, "dead_letter": 0,
            "failure_details": [{
                "code": "read_timeout", "message": "模型在 180 秒内未返回结果",
                "retryable": True, "failed": 2, "retry_scheduled": 2,
                "dead_letter": 0,
            }],
        },
    })

    run_id = store.start_run("fast_news_scan", "test")
    service._run_task(run_id, "fast_news_scan", "test")

    assert not [event for event in store.recent_events(10) if event["kind"] == "task_failure"]
    assert store.recent_runs(1)[0]["status"] == "succeeded"


def test_news_dead_letter_alert_includes_structured_root_cause(tmp_path, monkeypatch):
    store = AutomationStore(tmp_path / "automation.sqlite")
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    monkeypatch.setattr(service, "_task_fast_news_scan", lambda: {
        "sources": [{"source": "eastmoney", "fetched": 2, "saved": 2}],
        "errors": {},
        "annotation": {
            "processed": 2, "completed": 0, "failed": 2,
            "retry_scheduled": 0, "dead_letter": 2,
            "failure_details": [{
                "code": "read_timeout", "message": "模型在 180 秒内未返回结果",
                "retryable": True, "failed": 2, "retry_scheduled": 0,
                "dead_letter": 2,
            }],
        },
    })

    run_id = store.start_run("fast_news_scan", "test")
    service._run_task(run_id, "fast_news_scan", "test")

    failure = next(event for event in store.recent_events(10) if event["kind"] == "task_failure")
    assert failure["payload"]["title"] == "新闻分析需要处理：快讯扫描"
    assert failure["payload"]["terminal"] is True
    assert failure["payload"]["dead_letter"] == 2
    assert failure["payload"]["error_codes"] == ["read_timeout"]
    assert any("2 条已暂停自动重试" in value for value in failure["evidence"])
    assert any("read_timeout：模型在 180 秒内未返回结果" in value
               for value in failure["evidence"])
    card = format_feishu_card(failure)
    assert "2 条资讯已停止自动重试" in card["elements"][0]["text"]["content"]
    assert "资讯分析队列中核查并恢复" in card["elements"][-1]["elements"][0]["content"]


def test_feishu_channel_lifecycle_and_normalized_message(tmp_path, monkeypatch):
    import sys
    import threading
    from types import SimpleNamespace

    store = AutomationStore(tmp_path / "automation.sqlite")
    client = FeishuBotClient(store, MemoryCredentials())
    client.configure("cli_app", "secret")
    store.bind_target(
        "feishu_group", target="oc_group", account_id="cli_app",
        owner_actor="feishu:cli_app:ou_owner", actor="test",
    )
    lifecycle = []
    received = []

    class FakePolicyConfig:
        def __init__(self, **kwargs):
            self.require_mention = kwargs["require_mention"]
            self.respond_to_mention_all = kwargs["respond_to_mention_all"]

    class FakeFeishuChannel:
        def __init__(self, **kwargs):
            assert kwargs["app_id"] == "cli_app"
            assert kwargs["app_secret"] == "secret"
            assert kwargs["policy"].require_mention is False
            assert kwargs["policy"].respond_to_mention_all is False
            self.handler = None
            self.bot_identity = SimpleNamespace(open_id="ou_bot", name="QuantMaster")

        def on(self, event, handler):
            assert event == "message"
            self.handler = handler

        async def start_background(self, *, timeout):
            lifecycle.append(("start", timeout))
            direct = SimpleNamespace(
                sender=SimpleNamespace(is_bot=False), message_id="om_1",
                chat_type="p2p", mentioned_bot=False, content_text="查询大盘",
                mentions=[], chat_id="oc_chat", sender_id="ou_owner",
                sender_name="测试用户",
            )
            await self.handler(direct)
            context = SimpleNamespace(
                sender=SimpleNamespace(is_bot=False), message_id="om_context",
                chat_type="group", mentioned_bot=False,
                content_text="贵州茅台 600519 的估值最近有分歧",
                mentions=[], chat_id="oc_group", sender_id="ou_member",
                sender_name="群成员", reply=None,
            )
            await self.handler(context)
            literal_at = SimpleNamespace(
                sender=SimpleNamespace(is_bot=False), message_id="om_literal",
                chat_type="group", mentioned_bot=False,
                content_text="@QuantMaster 这里只是手工输入的文字",
                mentions=[], chat_id="oc_group", sender_id="ou_member",
                sender_name="群成员", reply=None,
            )
            await self.handler(literal_at)
            group = SimpleNamespace(
                sender=SimpleNamespace(is_bot=False), message_id="om_2",
                chat_type="group", mentioned_bot=False,
                content_text="@QuantMaster 绑定 QuantMaster ABCD1234",
                mentions=[SimpleNamespace(
                    key="@_user_1", name="QuantMaster", open_id="ou_bot",
                )],
                chat_id="oc_group", sender_id="ou_owner", sender_name="测试用户",
                reply=None,
            )
            await self.handler(group)

        async def stop_background(self):
            lifecycle.append(("stop", None))

    monkeypatch.setitem(
        sys.modules, "lark_oapi.channel",
        SimpleNamespace(FeishuChannel=FakeFeishuChannel, PolicyConfig=FakePolicyConfig),
    )
    stop_event = threading.Event()
    stop_event.set()
    client.listen_forever(lambda actor, text: received.append((actor, text)), stop_event)

    assert lifecycle == [("start", 30), ("stop", None)]
    assert len(received) == 2
    assert received[0][0].target == "oc_chat"
    assert received[0][0].sender_name == "测试用户"
    assert received[0][1] == "查询大盘"
    assert received[1][0].chat_type == "group"
    assert received[1][1] == "绑定 QuantMaster ABCD1234"
    context = store.conversation_context(
        channel="feishu", account_id="cli_app", chat_id="oc_group",
    )
    assert [item["text"] for item in context] == [
        "贵州茅台 600519 的估值最近有分歧",
        "@QuantMaster 这里只是手工输入的文字",
        "绑定 QuantMaster ABCD1234",
    ]
    assert store.bot_account("feishu")["status"] == "configured"


def test_feishu_sdk_tasks_are_drained_before_private_loop_closes(monkeypatch):
    import asyncio
    import threading
    from types import SimpleNamespace

    from lark_oapi.ws import client as ws_client_module

    from quantmaster.automation.channels.feishu import (
        _drain_lark_ws_tasks,
        _track_lark_ws_tasks,
    )

    ws_loop = asyncio.new_event_loop()
    cache_loop = asyncio.new_event_loop()
    ready = threading.Event()
    thread_finished = threading.Event()
    events = []
    remaining = []

    async def _select():
        try:
            await asyncio.sleep(3600)
        finally:
            events.append("select-stopped")

    async def background(name):
        try:
            await asyncio.sleep(3600)
        finally:
            events.append(f"{name}-stopped")

    def run_ws_loop():
        asyncio.set_event_loop(ws_loop)
        root = ws_loop.create_task(_select())
        background_tasks = [
            ws_loop.create_task(background(name), name=f"sdk-{name}")
            for name in ("ping", "receive")
        ]
        ready.set()
        try:
            ws_loop.run_until_complete(root)
        except asyncio.CancelledError:
            pass
        finally:
            remaining.extend(asyncio.all_tasks(ws_loop))
            ws_loop.close()
            thread_finished.set()
        assert all(task.done() for task in background_tasks)

    async def disconnect():
        events.append("disconnected")

    monkeypatch.setattr(ws_client_module, "loop", ws_loop)
    cache_task = cache_loop.create_task(background("cache"), name="sdk-cache")
    ws = SimpleNamespace(
        _auto_reconnect=True,
        _disconnect=disconnect,
        _cache=SimpleNamespace(_cron=cache_task),
    )

    channel = SimpleNamespace(_ws_client=ws)
    _track_lark_ws_tasks(channel)
    worker = threading.Thread(target=run_ws_loop, daemon=True)
    worker.start()
    assert ready.wait(1)
    assert asyncio.run(_drain_lark_ws_tasks(channel, timeout=1)) is True
    assert thread_finished.wait(1)
    worker.join(timeout=1)
    cache_remaining = asyncio.all_tasks(cache_loop)
    cache_loop.close()

    assert ws._auto_reconnect is False
    assert channel._ws_client is None
    assert remaining == []
    assert cache_remaining == set()
    assert cache_task.cancelled()
    assert set(events) == {
        "ping-stopped", "receive-stopped", "disconnected", "select-stopped",
    }


def test_feishu_async_listener_can_close_from_its_owner_loop(tmp_path, monkeypatch):
    import asyncio
    import sys
    import threading
    from types import SimpleNamespace

    store = AutomationStore(tmp_path / "automation.sqlite")
    client = FeishuBotClient(store, MemoryCredentials())
    client.configure("cli_app", "secret")
    events = []

    class FakeFeishuChannel:
        def __init__(self, **_kwargs):
            self._ws_client = None

        def on(self, _event, _handler):
            pass

        async def start_background(self, *, timeout):
            events.append(("start", timeout, asyncio.get_running_loop()))

        async def stop_background(self):
            events.append(("stop", None, asyncio.get_running_loop()))

    monkeypatch.setitem(
        sys.modules, "lark_oapi.channel",
        SimpleNamespace(FeishuChannel=FakeFeishuChannel),
    )

    async def exercise():
        stop_event = threading.Event()
        listener = asyncio.create_task(client.listen(lambda *_: None, stop_event))
        while client._active_channel is None:
            await asyncio.sleep(0)
        await client.aclose()
        await listener

    asyncio.run(exercise())

    assert [event[0] for event in events] == ["start", "stop"]
    assert events[0][2] is events[1][2]
    assert client._active_channel is None
    assert store.bot_account("feishu")["status"] == "configured"


def test_feishu_sync_bridges_reject_a_running_loop_without_creating_coroutines(
        tmp_path, monkeypatch):
    import asyncio
    import threading

    store = AutomationStore(tmp_path / "automation.sqlite")
    client = FeishuBotClient(store, MemoryCredentials())
    client.configure("cli_app", "secret")
    called = []
    monkeypatch.setattr(asyncio, "run", lambda _coro: called.append(True))

    async def exercise():
        with pytest.raises(RuntimeError, match=r"await FeishuBotClient\.listen"):
            client.listen_forever(lambda *_: None, threading.Event())
        with pytest.raises(RuntimeError, match=r"await FeishuBotClient\.aclose"):
            client.close()

    # Use Runner directly because this test deliberately guards asyncio.run.
    with asyncio.Runner() as runner:
        runner.run(exercise())
    assert called == []


def test_feishu_cross_loop_close_is_owned_once(tmp_path, monkeypatch):
    import asyncio
    import sys
    import threading
    from types import SimpleNamespace

    store = AutomationStore(tmp_path / "automation.sqlite")
    client = FeishuBotClient(store, MemoryCredentials())
    client.configure("cli_app", "secret")
    stop_calls = []
    started = threading.Event()

    class FakeFeishuChannel:
        def __init__(self, **_kwargs):
            self._ws_client = None

        def on(self, _event, _handler):
            pass

        async def start_background(self, *, timeout):
            assert timeout == 30
            started.set()

        async def stop_background(self):
            stop_calls.append(threading.current_thread().name)

    monkeypatch.setitem(
        sys.modules, "lark_oapi.channel",
        SimpleNamespace(FeishuChannel=FakeFeishuChannel),
    )
    stop_event = threading.Event()
    owner = threading.Thread(
        target=client.listen_forever,
        args=(lambda *_: None, stop_event),
        name="feishu-owner",
    )
    owner.start()
    assert started.wait(1)

    async def close_twice():
        await asyncio.gather(client.aclose(), client.aclose())

    asyncio.run(close_twice())
    owner.join(timeout=1)

    assert not owner.is_alive()
    assert stop_calls == ["feishu-owner"]
    assert client._active_channel is None


def test_feishu_config_verifies_replaces_and_removes_credentials(tmp_path, monkeypatch):
    store = AutomationStore(tmp_path / "automation.sqlite")
    credentials = MemoryCredentials()
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    service.feishu = FeishuBotClient(store, credentials)
    monkeypatch.setattr(
        service.feishu, "verify",
        lambda app_id, secret: {"status": "success", "message": "有效", "latency_ms": 3},
    )

    result = service.configure_feishu("cli_first", "secret-one")
    assert result["verification"]["status"] == "success"
    assert credentials.values["bot:feishu:cli_first"] == "secret-one"
    service.configure_feishu("cli_second", "secret-two")
    assert "bot:feishu:cli_first" not in credentials.values
    assert store.bot_account("feishu")["account_id"] == "cli_second"

    removed = service.remove_feishu()
    assert removed == {"status": "ok", "warnings": []}
    assert credentials.values == {}
    assert store.bot_account("feishu") is None


def test_feishu_missing_credentials_never_starts_listener_or_dispatcher(tmp_path, monkeypatch):
    import apscheduler.schedulers.background as scheduler_module

    import quantmaster.automation.runtime as runtime_module

    store = AutomationStore(tmp_path / "automation.sqlite")
    store.save_bot_account(channel="feishu", account_id="cli_missing", secret_target="missing")
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    service.feishu = FeishuBotClient(store, MemoryCredentials())
    runtime = runtime_module.AutomationRuntime(service)
    runtime.started = runtime.leader = True
    started = []

    class UnexpectedThread:
        def __init__(self, *args, **kwargs):
            started.append((args, kwargs))

    with monkeypatch.context() as channel_patch:
        channel_patch.setattr(runtime_module.threading, "Thread", UnexpectedThread)
        assert runtime.start_channels()["feishu"] is False
        assert started == []
        assert store.bot_account("feishu")["status"] == "not_configured"

    class FakeScheduler:
        def __init__(self, **_kwargs):
            self.job_ids = []
            self.callbacks = {}

        def start(self):
            return None

        def add_job(self, *_args, **kwargs):
            self.job_ids.append(kwargs["id"])
            self.callbacks[kwargs["id"]] = _args[0]

        def shutdown(self, **_kwargs):
            return None

    monkeypatch.setattr(scheduler_module, "BackgroundScheduler", FakeScheduler)
    monkeypatch.setattr(runtime, "reload_jobs", lambda: None)
    assert runtime._start_scheduler_locked()
    assert runtime.scheduler.job_ids == ["_lease", "_cleanup"]

    mixed = runtime_module.AutomationRuntime(
        AutomationService(store, OutboxDispatcher(store, WeixinOnlyGateway())),
    )
    mixed.leader = True
    monkeypatch.setattr(mixed, "reload_jobs", lambda: None)
    assert mixed._start_scheduler_locked()
    assert mixed.scheduler.job_ids == ["_lease", "_outbox", "_cleanup"]
    assert mixed.scheduler.callbacks["_outbox"].__self__ is mixed.service.dispatcher
    assert mixed.scheduler.callbacks["_outbox"].__func__ is OutboxDispatcher.wake


def test_feishu_public_state_is_redacted_and_tracks_validation(tmp_path):
    store = AutomationStore(tmp_path / "automation.sqlite")
    credentials = MemoryCredentials()
    client = FeishuBotClient(store, credentials)
    client.configure("cli_12345678", "super-secret-token")
    store.set_bot_validation(
        "feishu", "cli_12345678", "network_error",
        "Authorization=Bearer super-secret-token ticket=abc123",
    )

    public = client.public_account(store.bot_account("feishu") or {})

    assert public["app_id_suffix"] == "5678"
    assert public["app_secret_present"] is True
    assert public["last_validated_at"]
    assert "super-secret-token" not in str(public)
    assert "abc123" not in str(public)
    assert "***" in public["last_error"]


@pytest.mark.parametrize(("exc", "kind", "retryable"), [
    (RuntimeError("EOF while reading from server"), "connection_eof", True),
    (RuntimeError("[SSL: CERTIFICATE_VERIFY_FAILED] self signed certificate"),
     "tls_certificate", False),
    (RuntimeError("connection reset by peer"), "network_connection", True),
])
def test_feishu_connection_errors_are_classified_without_secrets(exc, kind, retryable):
    exc.__cause__ = RuntimeError("app_secret=not-for-logs")

    result = feishu_connection_error(exc)

    assert result["kind"] == kind
    assert result["retryable"] is retryable
    assert "not-for-logs" not in result["summary"]
    assert "app_secret=***" in result["summary"]


def test_feishu_runtime_retries_eof_and_persists_degraded_diagnostic(
        tmp_path, monkeypatch,
):
    import threading
    from types import SimpleNamespace

    import quantmaster.automation.runtime as runtime_module

    store = AutomationStore(tmp_path / "automation.sqlite")
    store.save_bot_account(channel="feishu", account_id="cli_app", status="configured")
    attempts = []
    recovered = threading.Event()
    stop_event = threading.Event()

    def listen(_on_message, received_stop):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("EOF while reading from server app_secret=do-not-log")
        recovered.set()
        received_stop.set()

    runtime = runtime_module.AutomationRuntime(SimpleNamespace(
        store=store,
        feishu=SimpleNamespace(listen_forever=listen),
        weixin=SimpleNamespace(),
    ))
    runtime._channel_stops["feishu"] = stop_event
    monkeypatch.setattr(runtime_module, "FEISHU_RECONNECT_INITIAL_SECONDS", 0.001)

    runtime._channel_worker("feishu")

    assert recovered.is_set()
    assert len(attempts) == 2
    account = store.bot_account("feishu")
    assert account["status"] == "degraded"
    assert "connection_eof" in account["last_error"]
    assert "do-not-log" not in account["last_error"]
    assert "app_secret=***" in account["last_error"]


def test_feishu_runtime_uses_client_failure_state_when_available(tmp_path, monkeypatch):
    import threading
    from types import SimpleNamespace

    import quantmaster.automation.runtime as runtime_module

    store = AutomationStore(tmp_path / "automation.sqlite")
    store.save_bot_account(channel="feishu", account_id="cli_app", status="configured")
    stop_event = threading.Event()

    def listen(_on_message, _received_stop):
        raise RuntimeError("bad credentials")

    runtime = runtime_module.AutomationRuntime(SimpleNamespace(
        store=store,
        feishu=SimpleNamespace(
            listen_forever=listen,
            failure_state=lambda _exc: "invalid_credentials",
        ),
        weixin=SimpleNamespace(),
    ))
    runtime._channel_stops["feishu"] = stop_event

    runtime._channel_worker("feishu")

    assert store.bot_account("feishu")["status"] == "invalid_credentials"


def test_runtime_standby_automatically_takes_over_expired_lease(monkeypatch):
    import threading
    from types import SimpleNamespace

    import quantmaster.automation.runtime as runtime_module

    class FakeStore:
        def __init__(self):
            self.attempts = 0
            self.released = []

        def acquire_lease(self, name, owner):
            self.attempts += 1
            return self.attempts >= 2

        def release_lease(self, name, owner):
            self.released.append((name, owner))

    store = FakeStore()
    runtime = runtime_module.AutomationRuntime(SimpleNamespace(store=store))
    activated = threading.Event()

    def activate():
        runtime.leader = True
        activated.set()
        return True

    monkeypatch.setattr(runtime_module, "STANDBY_RETRY_SECONDS", 0.01)
    monkeypatch.setattr(
        runtime_module, "get_config",
        lambda: SimpleNamespace(automation=SimpleNamespace(enabled=True)),
    )
    monkeypatch.setattr(runtime, "_activate_leader_locked", activate)

    assert runtime.start() is False
    assert activated.wait(1)
    assert runtime.leader is True
    assert store.attempts == 2
    runtime.stop()


def test_intraday_monitor_uses_fresh_breadth_cache_instead_of_fake_neutral(
    tmp_path, monkeypatch,
):
    from types import SimpleNamespace

    store = AutomationStore(tmp_path / "automation.sqlite")
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    symbols = ["000300.SH", "000905.SH", "000852.SH", "399006.SZ"]
    now = pd.Timestamp.now(tz="Asia/Shanghai").tz_localize(None)
    cutoff = now.floor("5min") - pd.Timedelta(minutes=5)
    index = pd.date_range(cutoff - pd.Timedelta(minutes=20), cutoff, freq="5min")
    frame = pd.DataFrame({"close": [100.0] * len(index)}, index=index)
    store.save_breadth(cutoff.isoformat(), 0.63, 4)

    monkeypatch.setattr(
        "quantmaster.automation.service.get_config",
        lambda: SimpleNamespace(automation=SimpleNamespace(
            timezone="Asia/Shanghai", sentinel_indices=symbols, primary_universe="demo",
        )),
    )
    monkeypatch.setattr(
        "quantmaster.data.refresh_intraday",
        lambda *args, **kwargs: BarDataEnvelope(
            data=frame,
            quality=BarDataQuality(
                status="verified",
                requested_start=str(index[0]),
                requested_end=str(index[-1]),
                observed_start=str(index[0]),
                observed_end=str(index[-1]),
                coverage_ratio=1.0,
                sources=("fixture",),
                timezone="Asia/Shanghai",
            ),
            provenance=({"source": "fixture"},),
        ),
    )
    monkeypatch.setattr(
        "quantmaster.data.universe.load_universe_analysis", lambda _name: symbols,
    )

    def unavailable(*args, **kwargs):
        raise RuntimeError("eastmoney circuit open")

    monkeypatch.setattr("quantmaster.data.akshare_source.AkshareSource.spot", unavailable)
    monkeypatch.setattr("quantmaster.data.refresh_spot", unavailable)
    result = service._task_intraday_monitor()

    assert result["status"] == "degraded"
    assert result["breadth_source"] == "cache"
    assert result["breadth"] == pytest.approx(0.63)
    service.jobs.stop()
    service.executor.shutdown(wait=False, cancel_futures=True)


def test_daily_close_keeps_degraded_analysis_preview_without_formal_save(
    tmp_path, monkeypatch,
):
    from types import SimpleNamespace

    store = AutomationStore(tmp_path / "automation.sqlite")
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    dates = pd.bdate_range("2026-07-01", "2026-08-07")
    close = pd.DataFrame({"600519.SH": range(len(dates))}, index=dates, dtype=float)
    panel = {
        "open": close + 1,
        "high": close + 2,
        "low": close,
        "close": close + 1,
        "volume": close + 1_000,
    }
    envelope = BarDataEnvelope(
        data=panel,
        quality=BarDataQuality(
            status="degraded",
            requested_start="2025-03-25",
            requested_end="2026-08-07",
            observed_start="2026-07-01",
            observed_end="2026-08-07",
            coverage_ratio=1.0,
            sources=("free-stockdb",),
            issues=("复权因子链未验证",),
            timezone="exchange-date",
            adjustment="qfq_requested_unverified",
            requested_symbols=("600519.SH",),
            observed_symbols=("600519.SH",),
        ),
        provenance=({"source": "free-stockdb"},),
    )
    saved = []
    monkeypatch.setattr(
        "quantmaster.automation.service.get_config",
        lambda: SimpleNamespace(
            automation=SimpleNamespace(primary_universe="demo"),
        ),
    )
    monkeypatch.setattr(
        "quantmaster.automation.service.market_date",
        lambda: datetime(2026, 8, 7).date(),
    )
    monkeypatch.setattr(
        "quantmaster.data.universe.load_universe_analysis_snapshot",
        lambda _name: SimpleNamespace(
            symbols=("600519.SH",),
            to_dict=lambda: {"name": "demo", "symbols": ["600519.SH"]},
        ),
    )
    monkeypatch.setattr("quantmaster.data.refresh_panel", lambda *_args, **_kwargs: envelope)
    monkeypatch.setattr(
        "quantmaster.data.read_stock_names", lambda _symbols: {"600519.SH": "贵州茅台"},
    )
    monkeypatch.setattr(
        "quantmaster.data.industry.load_industry_analysis_context",
        lambda: ({"600519.SH": "食品饮料"}, {
            "status": "degraded", "formal_eligible": False, "issues": ["目录过期"],
        }),
    )
    monkeypatch.setattr(
        "quantmaster.decision.hybrid_daily_selection",
        lambda *_args, **_kwargs: {
            "signal_date": "2026-08-07",
            "picks": [{"symbol": "600519.SH", "score": 0.8}],
            "data_quality": {"status": "complete"},
        },
    )
    monkeypatch.setattr(
        "quantmaster.decision.DecisionStore.save",
        lambda *_args, **_kwargs: saved.append(True),
    )
    monkeypatch.setattr(
        "quantmaster.market.regime.analyze_market",
        lambda _panel: {"current": {"regime": "neutral"}},
    )
    monkeypatch.setattr(
        "quantmaster.automation.service.close_regime_event",
        lambda *_args, **_kwargs: None,
    )

    result = service._task_daily_close_pipeline()

    assert result["status"] == "degraded"
    assert result["preview_picks"][0]["symbol"] == "600519.SH"
    assert result["persistence"]["status"] == "blocked"
    assert result["persistence"]["saved"] is False
    assert saved == []
    service.jobs.stop()
    service.executor.shutdown(wait=False, cancel_futures=True)


def test_automation_run_uses_unified_durable_job_and_idempotency(tmp_path, monkeypatch):
    import time

    store = AutomationStore(tmp_path / "automation.sqlite")
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    monkeypatch.setattr(service, "_task_news_digest", lambda: {"items": 0})

    first = service.run_task(
        "news_digest", actor="test", idempotency_key="manual-request-1",
    )
    duplicate = service.run_task(
        "news_digest", actor="test", idempotency_key="manual-request-1",
    )
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        job = service.jobs.store.get(first["job_id"])
        if job["status"] == "completed":
            break
        time.sleep(0.01)
    else:
        raise AssertionError(service.jobs.store.get(first["job_id"]))

    assert duplicate["job_id"] == first["job_id"]
    assert duplicate["created"] is False
    assert job["result_artifact_id"]
    assert store.recent_runs(10) == []
    service.jobs.stop()
    service.executor.shutdown(wait=False, cancel_futures=True)


def test_daily_triggers_manual_and_stockdb_share_trade_date_job(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from zoneinfo import ZoneInfo

    import quantmaster.automation.runtime as runtime_module

    store = AutomationStore(tmp_path / "automation.sqlite")
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    runtime = runtime_module.AutomationRuntime(service)
    moment = datetime(2026, 8, 12, 15, 20, tzinfo=ZoneInfo("Asia/Shanghai"))
    monkeypatch.setattr(
        "quantmaster.automation.service.resolve_session_target",
        lambda as_of="", now=None: SimpleNamespace(
            session=as_of or "2026-08-12", ready=True,
        ),
    )

    first = runtime.discover_job("daily_close_pipeline", now=moment)
    second = runtime.discover_job("daily_close_pipeline", now=moment.replace(minute=35))
    manual = service.run_task("daily_close_pipeline", actor="web", as_of="2026-08-12")
    stockdb = service.run_task(
        "daily_close_pipeline", actor="free-stockdb", as_of="2026-08-12",
        business_key="daily_close_pipeline:date:2026-08-12",
    )

    assert {first["job_id"], second["job_id"], manual["job_id"], stockdb["job_id"]} == {
        first["job_id"],
    }
    job = service.jobs.store.get(first["job_id"])
    assert job["trigger_count"] == 4
    assert job["coalesced_count"] == 3
    service.jobs.stop()
    service.executor.shutdown(wait=False, cancel_futures=True)


def test_daily_scheduler_uses_the_same_resolved_trade_date_as_manual(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from zoneinfo import ZoneInfo

    import quantmaster.automation.runtime as runtime_module

    store = AutomationStore(tmp_path / "automation.sqlite")
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    runtime = runtime_module.AutomationRuntime(service)
    monkeypatch.setattr(
        "quantmaster.automation.service.resolve_session_target",
        lambda as_of="", now=None: SimpleNamespace(
            session=as_of or "2026-08-14", ready=True,
        ),
    )
    holiday = datetime(2026, 8, 17, 15, 20, tzinfo=ZoneInfo("Asia/Shanghai"))

    scheduled = runtime.discover_job("daily_close_pipeline", now=holiday)
    manual = service.run_task(
        "daily_close_pipeline", actor="web", as_of="2026-08-14",
    )

    assert scheduled["business_key"] == "daily_close_pipeline:date:2026-08-14"
    assert manual["job_id"] == scheduled["job_id"]
    service.jobs.stop()
    service.executor.shutdown(wait=False, cancel_futures=True)


def test_interval_discovery_merges_missed_windows_without_losing_boundary(tmp_path):
    from types import SimpleNamespace
    from zoneinfo import ZoneInfo

    import quantmaster.automation.runtime as runtime_module

    store = AutomationStore(tmp_path / "automation.sqlite")
    submitted = []
    service = SimpleNamespace(
        store=store,
        run_task=lambda name, **kwargs: submitted.append((name, kwargs)) or kwargs,
    )
    runtime = runtime_module.AutomationRuntime(service)
    zone = ZoneInfo("Asia/Shanghai")

    runtime.discover_job("fast_news_scan", now=datetime(2026, 8, 12, 10, 20, tzinfo=zone))
    runtime.discover_job("fast_news_scan", now=datetime(2026, 8, 12, 11, 20, tzinfo=zone))

    assert len(submitted) == 2
    first_key = submitted[0][1]["business_key"]
    second_key = submitted[1][1]["business_key"]
    assert "10:00:00+08:00:2026-08-12T10:20:00+08:00" in first_key
    assert "10:20:00+08:00:2026-08-12T11:20:00+08:00" in second_key
    assert store.scheduler_cursor("fast_news_scan") == pytest.approx(
        datetime(2026, 8, 12, 11, 20, tzinfo=zone).timestamp()
    )


def test_startup_daily_catchup_discovers_due_date_once(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from zoneinfo import ZoneInfo

    import quantmaster.automation.runtime as runtime_module

    store = AutomationStore(tmp_path / "automation.sqlite")
    calls = []
    runtime = runtime_module.AutomationRuntime(SimpleNamespace(store=store))
    monkeypatch.setattr(
        runtime, "discover_job",
        lambda name, **kwargs: calls.append((name, kwargs["actor"])) or {"task": name},
    )
    now = datetime(2026, 8, 12, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

    recovered = runtime.catch_up_daily_jobs(now=now)

    assert ("daily_close_pipeline", "startup_recovery") in calls
    assert ("news_digest", "startup_recovery") in calls
    assert len(recovered) == len(calls)
def test_scheduler_status_projection_is_additive_and_redacts_runtime_internals(
    tmp_path, isolated_config,
):
    from quantmaster.config import get_config
    from quantmaster.runtime.jobs import UnifiedJobStore
    from quantmaster.server.automation import _automation_runs, _job_projection

    store = UnifiedJobStore(get_config().data_root / "jobs.sqlite")
    raw, _ = store.submit("automation.fast_news_scan", {"name": "fast_news_scan"})
    with store._conn() as connection:
        connection.execute(
            "UPDATE runtime_jobs SET status='running',progress=1,phase='fetch',detail='window 3',"
            "owner='private-worker',lease_token='private-token',heartbeat_at=?,started_at=? WHERE id=?",
            (time.time(), datetime.now(UTC).isoformat(), raw["id"]),
        )

    runs = _automation_runs()
    assert len(runs) == 1
    public = runs[0]
    assert public["progress"] == 1
    assert public["started_at"] and public["heartbeat_at"]
    assert public["links"]["self"] == f"/api/v1/jobs/{raw['id']}"
    assert not ({"owner", "lease_token", "lease_expires", "spec"} & set(public))

    jobs = _job_projection([
        {"name": "fast_news_scan", "enabled": True, "schedule": {}, "next_run": ""},
    ], runs)
    assert jobs[0]["job_kind"] == "time_window"
    assert jobs[0]["execution"]["active_job_id"] == raw["id"]
    assert jobs[0]["execution"]["running_instances"] == 1


def test_automation_api_and_ui_contract():
    client = TestClient(app)
    overview = client.get("/api/v1/automation/overview")
    assert overview.status_code == 200
    data = overview.json()
    assert set(data["channels"]) == {"weixin", "feishu"}
    assert set(data["inbound"]) == {"weixin", "feishu"}
    assert "gateway" not in data
    assert all("context_token" not in target for target in data["targets"])
    assert client.post("/api/v1/automation/jobs/news_digest/run").status_code == 403

    page = client.get("/").text
    assert 'data-tab="automation"' in page
    assert "自动化运营" in page
    assert 'role="tablist" aria-label="自动化运营视图"' in page
    assert 'id="automation-view-overview"' in page
    assert 'id="automation-view-jobs"' in page
    assert 'id="automation-view-messaging"' in page
    assert 'id="automation-view-records"' in page
    assert 'role="tabpanel" aria-labelledby="automation-view-overview"' in page
    assert "/static/automation.css?rev=" in page
    assert "/static/automation.js?rev=" in page
    assert "%%QM_AUTOMATION" not in page
    assert "腾讯微信 ClawBot" in page
    assert "飞书企业自建应用 Bot" in page
    automation_script = client.get("/static/automation.js").text
    settings_script = client.get("/static/settings.js").text
    assert "feishu-config-form" not in automation_script
    assert "data-feishu-diagnose" in automation_script
    assert "feishuStageLabels" in settings_script
    assert "function parseSymbols(value)" in settings_script
    script = client.get("/static/automation.js").text
    assert "conservative:'保守'" in script
    assert "balanced:'均衡'" in script
    assert "sensitive:'敏感'" in script
    assert "important_news:'重要资讯'" in script
    assert "未订阅任何内容；自动化与 Bot 监听仍会继续运行" in script
    assert "targetFeedback" in script
    assert "automation-audit-panel" in page
    assert 'data-record-panel="audit"' in page
    assert "/api/v1/automation/jobs" in script
    assert "/api/v1/automation/events?limit=50" in script
    assert "/api/v1/automation/audit?limit=50" in script
    assert "alert(" not in script
    assert "news-source-feedback" in page
    assert "长连接正常，但尚未收到消息事件" in script
    assert "发送测试消息" in script
    assert "测试消息未发送" in script
    assert "function progressValue(value)" in script
    assert "Number(value.progress) <= 1" not in script
    assert "Durable Job" in script
    assert "任务停滞" in script
    assert "合法退避" in script
    assert "已连接同一任务" in script
