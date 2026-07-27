from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from quantmaster.ai.crawler import NewsItem
from quantmaster.automation.channels.feishu import FeishuBotClient
from quantmaster.automation.channels.weixin import WeixinClawBotClient
from quantmaster.automation.commands import BotCommandRouter
from quantmaster.automation.delivery import OutboxDispatcher, format_feishu_card
from quantmaster.automation.models import ActorContext, AlertEvent
from quantmaster.automation.news import importance_score
from quantmaster.automation.policy import policy_allows, resolved_policy
from quantmaster.automation.service import AutomationService
from quantmaster.automation.store import AutomationStore
from quantmaster.server.app import app


class MemoryCredentials:
    def __init__(self):
        self.values = {}

    def get(self, target):
        return self.values.get(target)

    def set(self, target, value):
        self.values[target] = value


class RecordingGateway:
    def __init__(self):
        self.items = []

    def send(self, delivery):
        self.items.append(delivery)


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


def test_store_binding_outbox_and_delivery(tmp_path):
    store = AutomationStore(tmp_path / "automation.sqlite")
    assert store.inbound_status("feishu") == {"total": 0, "last_received_at": ""}
    assert store.claim_inbound("feishu", "om_probe") is True
    assert store.inbound_status("feishu")["total"] == 1
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


def test_binding_code_is_single_use(tmp_path):
    store = AutomationStore(tmp_path / "automation.sqlite")
    code = store.create_binding_code("feishu_owner")["code"]
    assert store.consume_binding_code(code)["payload"]["target_id"] == "feishu_owner"
    assert store.consume_binding_code(code) is None


def test_binding_flow_preserves_code_on_wrong_chat_and_requires_owner_first(tmp_path):
    store = AutomationStore(tmp_path / "automation.sqlite")
    service = AutomationService(store, OutboxDispatcher(store, RecordingGateway()))
    with pytest.raises(ValueError, match="主人私聊"):
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
    assert "sensitive" in answer
    assert store.target("feishu_owner")["preset"] == "sensitive"


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
    assert card["header"]["title"]["content"].startswith("QuantMaster")
    content = card["elements"][0]["text"]["content"]
    assert "核查依据" in content
    assert "000300.SH" in content
    assert "https://example.com/source" in content


def test_feishu_channel_lifecycle_and_normalized_message(tmp_path, monkeypatch):
    import sys
    import threading
    from types import SimpleNamespace

    store = AutomationStore(tmp_path / "automation.sqlite")
    client = FeishuBotClient(store, MemoryCredentials())
    client.configure("cli_app", "secret")
    lifecycle = []
    received = []

    class FakeFeishuChannel:
        def __init__(self, **kwargs):
            assert kwargs == {"app_id": "cli_app", "app_secret": "secret"}
            self.handler = None

        def on(self, event, handler):
            assert event == "message"
            self.handler = handler

        async def start_background(self, *, timeout):
            lifecycle.append(("start", timeout))
            message = SimpleNamespace(
                sender=SimpleNamespace(is_bot=False), message_id="om_1",
                chat_type="p2p", mentioned_bot=False, content_text="查询大盘",
                mentions=[], chat_id="oc_chat", sender_id="ou_owner",
                sender_name="测试用户",
            )
            await self.handler(message)

        async def stop_background(self):
            lifecycle.append(("stop", None))

    monkeypatch.setitem(
        sys.modules, "lark_oapi.channel",
        SimpleNamespace(FeishuChannel=FakeFeishuChannel),
    )
    stop_event = threading.Event()
    stop_event.set()
    client.listen_forever(lambda actor, text: received.append((actor, text)), stop_event)

    assert lifecycle == [("start", 30), ("stop", None)]
    assert received[0][0].target == "oc_chat"
    assert received[0][0].sender_name == "测试用户"
    assert received[0][1] == "查询大盘"
    assert store.bot_account("feishu")["status"] == "configured"


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


def test_automation_api_and_ui_contract():
    client = TestClient(app)
    overview = client.get("/api/automation/overview")
    assert overview.status_code == 200
    data = overview.json()
    assert set(data["channels"]) == {"weixin", "feishu"}
    assert set(data["inbound"]) == {"weixin", "feishu"}
    assert "gateway" not in data
    assert all("context_token" not in target for target in data["targets"])
    assert client.post("/api/automation/jobs/news_digest/run").status_code == 403

    page = client.get("/").text
    assert 'data-tab="automation"' in page
    assert "腾讯微信 ClawBot" in page
    assert "飞书企业自建应用 Bot" in page
    script = client.get("/static/automation.js").text
    assert "conservative:'保守'" in script
    assert "balanced:'均衡'" in script
    assert "sensitive:'敏感'" in script
    assert "长连接正常，但尚未收到消息事件" in script
    assert "测试（先绑定）" in script
