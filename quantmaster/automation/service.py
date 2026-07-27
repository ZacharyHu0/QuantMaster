from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from quantmaster.automation.channels import FeishuBotClient, WeixinClawBotClient
from quantmaster.automation.delivery import BotDeliveryGateway, OutboxDispatcher
from quantmaster.automation.detector import MarketTurnDetector, close_regime_event
from quantmaster.automation.models import ActorContext, AlertEvent, stable_hash
from quantmaster.automation.news import CRITICAL_PATTERNS, importance_score, news_event
from quantmaster.automation.policy import policy_allows, resolved_policy
from quantmaster.automation.store import AutomationStore
from quantmaster.config import get_config

logger = logging.getLogger(__name__)

ALLOWED_TASKS = {
    "intraday_monitor", "fast_news_scan", "official_news_scan", "periodic_news_scan",
    "daily_close_pipeline", "news_digest", "paper_rebalance_proposal",
}
UNFILTERED_KINDS = {"task_failure", "task_report"}


def validate_schedule(name: str, schedule: dict[str, Any]) -> dict[str, Any]:
    if name not in ALLOWED_TASKS:
        raise ValueError("任务不在允许列表中")
    kind = schedule.get("type")
    if kind == "interval":
        minutes = int(schedule.get("minutes", 0))
        if not 5 <= minutes <= 60:
            raise ValueError("扫描间隔必须为 5–60 分钟")
        result = {"type": "interval", "minutes": minutes}
        if "window" in schedule:
            if not re.fullmatch(r"\d{2}:\d{2}-\d{2}:\d{2}", str(schedule["window"])):
                raise ValueError("时间窗口格式应为 HH:MM-HH:MM")
            result["window"] = schedule["window"]
        if "windows" in schedule:
            windows = list(schedule["windows"])
            if not windows or any(not re.fullmatch(r"\d{2}:\d{2}-\d{2}:\d{2}", str(v)) for v in windows):
                raise ValueError("时间窗口格式应为 HH:MM-HH:MM")
            result["windows"] = windows
        result["weekdays"] = bool(schedule.get("weekdays", False))
        return result
    if kind == "daily":
        times = list(schedule.get("times") or [])
        invalid_time = any(
            not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", str(value)) for value in times)
        if not 1 <= len(times) <= 3 or invalid_time:
            raise ValueError("每日任务需要 1–3 个 HH:MM 时间")
        return {"type": "daily", "times": times, "weekdays": bool(schedule.get("weekdays", False))}
    raise ValueError("任务计划仅支持 interval 或 daily")


class AutomationService:
    def __init__(self, store: AutomationStore | None = None,
                 dispatcher: OutboxDispatcher | None = None):
        self.store = store or AutomationStore()
        self.weixin = WeixinClawBotClient(self.store)
        self.feishu = FeishuBotClient(self.store)
        self.dispatcher = dispatcher or OutboxDispatcher(
            self.store, BotDeliveryGateway(self.store, self.weixin, self.feishu))
        self.detector = MarketTurnDetector()
        self.executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="qm-automation")

    # ---------- 状态与策略 ----------

    @staticmethod
    def _public_target(target: dict[str, Any]) -> dict[str, Any]:
        value = {key: item for key, item in target.items() if key != "context_token"}
        value["has_context"] = bool(target.get("context_token"))
        return value

    def public_targets(self) -> list[dict[str, Any]]:
        targets = sorted(
            self.store.targets(), key=lambda target: (target["channel"] != "feishu", target["id"]))
        return [self._public_target(target) for target in targets]

    def overview(self) -> dict[str, Any]:
        cfg = get_config().automation
        targets = self.public_targets()
        accounts = [
            {key: value for key, value in account.items() if key != "secret_target"}
            for account in self.store.bot_accounts()
        ]
        return {
            "enabled": cfg.enabled, "timezone": cfg.timezone,
            "runtime": "running" if cfg.enabled else "disabled",
            "channels": {
                "feishu": {"configured": any(a["channel"] == "feishu" for a in accounts),
                           "label": "飞书应用 Bot", "role": "primary"},
                "weixin": {"configured": any(a["channel"] == "weixin" for a in accounts),
                           "label": "腾讯微信 ClawBot", "role": "limited"},
            },
            "bot_accounts": accounts,
            "inbound": {
                "feishu": {
                    **self.store.inbound_status("feishu"),
                    "direct": self.store.inbound_status("feishu", "direct"),
                    "group": self.store.inbound_status("feishu", "group"),
                },
                "weixin": self.store.inbound_status("weixin"),
            },
            "targets": targets, "jobs": self.store.jobs(),
            "recent_runs": self.store.recent_runs(12),
            "recent_events": self.store.recent_events(12),
        }

    def start_weixin_login(self) -> dict:
        return self.weixin.start_login()

    def poll_weixin_login(self, session_id: str, verify_code: str = "") -> dict:
        return self.weixin.poll_login(session_id, verify_code)

    def configure_feishu(self, app_id: str, app_secret: str) -> dict:
        account = self.feishu.configure(app_id, app_secret)
        return {key: value for key, value in account.items() if key != "secret_target"}

    def create_binding(self, target_id: str, actor: str = "web") -> dict:
        target = self.store.target(target_id)
        if not target:
            raise KeyError("推送目标不存在")
        if target["channel"] != "feishu":
            raise ValueError("绑定码只用于飞书会话")
        owner = self.store.target("feishu_owner")
        if target["chat_type"] == "group" and not (
                owner and owner["target"] and owner["owner_actor"]):
            raise ValueError("请先完成飞书主人私聊绑定，再由主人到目标群绑定")
        result = self.store.create_binding_code(target_id, actor)
        self.store.audit(actor, "create_binding", "target", target_id, {}, {
            "binding_id": result["id"], "expires_at": result["expires_at"],
        }, "pending")
        return result

    def binding_status(self, action_id: str) -> dict:
        action = self.store.binding_action(action_id)
        if not action:
            raise KeyError("绑定会话不存在")
        target_id = str(action["payload"].get("target_id") or "")
        target = self.store.target(target_id)
        if not target:
            raise KeyError("推送目标不存在")
        bound = bool(target["target"] and target["account_id"])
        return {
            "id": action_id, "target_id": target_id,
            "status": "bound" if action["status"] == "consumed" and bound else action["status"],
            "expires_at": action["expires_at"], "bound": bound,
            "inbound": self.store.inbound_status("feishu", target["chat_type"]),
        }

    def reply(self, actor: ActorContext, text: str) -> None:
        target = self.store.target_by_route(actor.channel, actor.account_id, actor.target)
        if actor.channel == "weixin":
            context_token = target.get("context_token", "") if target else ""
            self.weixin.send(
                account_id=actor.account_id, to_user_id=actor.target,
                context_token=context_token, text=text,
            )
        else:
            self.feishu.send(chat_id=actor.target, text=text)

    def update_policy(self, target_id: str, preset: str, overrides: dict,
                      enabled: bool | None, actor: str) -> dict:
        return self.store.update_target_policy(
            target_id, preset=preset, overrides=overrides, enabled=enabled, actor=actor)

    def update_schedule(self, name: str, *, action: str, schedule: dict | None,
                        actor: str) -> dict:
        current = self.store.job(name)
        if current is None:
            raise ValueError("任务模板不存在")
        if action == "pause":
            enabled, value = False, current["schedule"]
        elif action == "resume":
            enabled, value = True, current["schedule"]
        elif action == "reschedule":
            enabled, value = current["enabled"], validate_schedule(name, schedule or {})
        else:
            raise ValueError("action 仅支持 pause/resume/reschedule")
        return self.store.update_job(name, enabled, value, actor)

    def process_event(self, event: AlertEvent, target_ids: set[str] | None = None) -> dict:
        stored, created = self.store.save_event(event)
        if not created:
            return {"event": stored, "created": False, "enqueued": 0}
        count = 0
        now = datetime.now(timezone.utc)
        for target in self.store.targets():
            if target_ids is not None and target["id"] not in target_ids:
                continue
            if not target["enabled"] or not target["target"] or target["status"] == "paused":
                continue
            policy = resolved_policy(target["preset"], target["overrides"])
            bypass = event.kind in UNFILTERED_KINDS or event.score >= 95
            if not bypass and not policy_allows(stored, policy):
                continue
            if event.kind == "market_turn" and not bypass:
                if int(event.payload.get("confirmation_count", 1)) < int(policy["confirmation_bars"]):
                    continue
                last = self.store.last_delivered_event(target["id"], "market_turn")
                if last and last["direction"] == event.direction:
                    delivered = datetime.fromisoformat(last["delivered_at"])
                    if now - delivered < timedelta(minutes=int(policy["cooldown_minutes"])):
                        continue
            if not bypass and self.store.hourly_delivery_count(target["id"]) >= int(policy["hourly_cap"]):
                continue
            count += int(self.store.enqueue(event.id, target["id"]))
        return {"event": stored, "created": True, "enqueued": count}

    def test_target(self, target_id: str) -> dict:
        target = self.store.target(target_id)
        if target is None:
            raise ValueError("推送目标不存在")
        event = AlertEvent(
            kind="task_report", score=0, severity="info", data_as_of=datetime.now().isoformat(),
            evidence=["如果你收到这条消息，说明 QuantMaster 与当前 Bot 会话的直连链路正常"],
            dedupe_key=stable_hash({"test": target_id, "at": datetime.now().isoformat()}),
            payload={"title": f"{target['label']} 测试推送"},
        )
        result = self.process_event(event, {target_id})
        result["dispatch"] = self.dispatcher.dispatch()
        return result

    # ---------- 身份、绑定与权限 ----------

    def is_owner(self, actor: ActorContext) -> bool:
        return actor.actor_key in self.store.owner_actors()

    def require_owner(self, actor: ActorContext, *, private: bool = False) -> None:
        if not self.is_owner(actor):
            raise PermissionError("只有已绑定主人可以执行该操作")
        if private and actor.chat_type != "direct":
            raise PermissionError("账本和模拟盘写入只能在主人私聊中执行")

    def bind(self, actor: ActorContext, code: str) -> dict:
        action = self.store.binding_for_code(code)
        if not action:
            raise ValueError("绑定码无效、已使用或已过期")
        target_id = action["payload"]["target_id"]
        target = self.store.target(target_id)
        if not target or target["channel"] != actor.channel or target["chat_type"] != actor.chat_type:
            raise ValueError("绑定码对应的频道或会话类型与当前会话不一致")
        if actor.chat_type == "group" and not self.is_owner(actor):
            raise PermissionError("飞书群必须由已经绑定的主人完成绑定")
        if not self.store.consume_binding_code(code, expected_id=action["id"]):
            raise ValueError("绑定码已被使用，请重新生成")
        return self.store.bind_target(
            target_id, target=actor.target, account_id=actor.account_id,
            owner_actor=actor.actor_key, actor=actor.actor_key,
        )

    # ---------- 查询与受控任务 ----------

    def query(self, view: str) -> dict[str, Any]:
        if view == "jobs":
            return {"jobs": self.store.jobs(), "runs": self.store.recent_runs(20)}
        if view == "alerts":
            return {"events": self.store.recent_events(30)}
        if view == "news":
            from quantmaster.ai.crawler import NewsStore
            return {"items": NewsStore().recent(30)}
        if view == "selection":
            from quantmaster.decision import DecisionStore
            return {"selection": DecisionStore().latest(get_config().automation.primary_universe)}
        if view == "ledger":
            from quantmaster.portfolio import Ledger, ledger_report
            return ledger_report(Ledger())
        if view == "market":
            from quantmaster.data.storage import IntradayBarStore
            store = IntradayBarStore("5m")
            items = []
            for symbol in get_config().automation.sentinel_indices:
                frame = store.get(symbol)
                if frame is not None and not frame.empty:
                    items.append({"symbol": symbol, "as_of": str(frame.index[-1]),
                                  "close": float(frame["close"].iloc[-1])})
            return {"indices": items, "latest_event": next(iter(self.store.recent_events(1)), None)}
        raise ValueError("view 仅支持 market/selection/news/ledger/jobs/alerts")

    def run_task(self, name: str, *, actor: str = "scheduler") -> dict:
        if name not in ALLOWED_TASKS:
            raise ValueError("任务不在允许列表中")
        run_id = self.store.start_run(name, actor)
        self.executor.submit(self._run_task, run_id, name, actor)
        return {"status": "accepted", "run_id": run_id, "task": name}

    def _run_task(self, run_id: str, name: str, actor: str) -> None:
        try:
            result = getattr(self, f"_task_{name}")()
            self.store.finish_run(run_id, result=result)
            report = AlertEvent(
                kind="task_report", score=0, severity="info", data_as_of=datetime.now().isoformat(),
                evidence=[f"任务 {name} 已完成", f"运行编号 {run_id[:10]}"],
                dedupe_key=stable_hash({"task": name, "run": run_id}),
                payload={"title": f"任务完成：{name}", "result": result},
            )
            self.process_event(report)
        except Exception as exc:  # pragma: no cover - 网络任务错误路径
            logger.exception("自动化任务 %s 失败", name)
            self.store.finish_run(run_id, error=str(exc))
            event = AlertEvent(
                kind="task_failure", score=100, severity="critical", data_as_of=datetime.now().isoformat(),
                evidence=[str(exc)[:500], f"运行编号 {run_id[:10]}"],
                dedupe_key=stable_hash({"task_failure": name, "hour": datetime.now().strftime("%Y%m%d%H")}),
                payload={"title": f"任务失败：{name}"},
            )
            self.process_event(event)

    # ---------- 任务实现 ----------

    def _task_intraday_monitor(self) -> dict:
        from quantmaster.data import load_intraday
        from quantmaster.data.akshare_source import AkshareSource
        from quantmaster.data.universe import load_universe

        now = pd.Timestamp.now(tz=get_config().automation.timezone).tz_localize(None)
        cutoff = now.floor("5min") - pd.Timedelta(minutes=5)
        start = cutoff - pd.Timedelta(days=35)
        bars = {
            symbol: load_intraday(symbol, str(start), str(now), "5m")
            for symbol in get_config().automation.sentinel_indices
        }
        latest = [pd.Timestamp(frame.index[-1]) for frame in bars.values() if not frame.empty]
        if not latest or cutoff - min(latest) > pd.Timedelta(minutes=10):
            return {"status": "skipped", "reason": "分钟行情超过 10 分钟未更新"}
        symbols = load_universe(get_config().automation.primary_universe)
        spot = AkshareSource().spot(symbols)
        ratio = float((pd.to_numeric(spot["change_pct"], errors="coerce") > 0).mean()) if len(spot) else 0.5
        self.store.save_breadth(cutoff.isoformat(), ratio, len(spot))
        breadth_rows = self.store.breadth()
        breadth = pd.Series(
            [row["advance_ratio"] for row in breadth_rows],
            index=pd.to_datetime([row["observed_at"] for row in breadth_rows]),
        )
        event = self.detector.evaluate(bars, breadth, cutoff=cutoff)
        return {"status": "ok", "event": self.process_event(event) if event else None,
                "breadth": ratio, "sample_size": len(spot)}

    def _news_context(self) -> tuple[set[str], set[str]]:
        from quantmaster.data.universe import load_universe
        from quantmaster.portfolio import AssetListStore, Ledger

        holdings = {p.symbol for p in Ledger().positions() if p.shares > 0}
        lists = AssetListStore().all()
        watchlist = set(get_config().automation.watchlist)
        watchlist.update(item["symbol"] for values in lists.values() for item in values)
        watchlist.update(load_universe(get_config().automation.primary_universe))
        return holdings, watchlist

    def _scan_news(self, group: str) -> dict:
        from quantmaster.ai.crawler import AICrawler, NewsItem, NewsStore

        crawler = AICrawler(store=NewsStore())
        result = crawler.run(group=group)
        holdings, watchlist = self._news_context()
        events = []
        candidate_ids = {
            *result.get("new_ids", []),
            *result.get("annotation", {}).get("completed_ids", []),
        }
        for item_id in sorted(candidate_ids):
            row = crawler.store.detail(int(item_id))
            if row is None:
                continue
            item = NewsItem(
                source=row["source_id"], title=row["title"], content=row["content"],
                url=row["url"], published_at=row["published_at"],
                symbols=row["symbols"], event_type=row["event_type"],
                sentiment=float(row["sentiment"] or 0), summary=row["summary"],
                confidence=float(row["confidence"] or 0),
                is_official=bool(row["is_official"]), db_id=int(row["id"]),
            )
            score, scope, _ = importance_score(item, holdings, watchlist)
            item.importance_score, item.scope = score, scope
            item.urgency = "critical" if score >= 95 else "high" if score >= 80 else "normal"
            crawler.store.update_context(
                int(item.db_id), importance_score=score, scope=scope, urgency=item.urgency,
            )
            # 无 LLM 时只有官方明确重大关键词能立即推送；其余仍入库供摘要查看。
            can_push = row["analysis_status"] == "complete" or (
                item.is_official and CRITICAL_PATTERNS.search(item.title + item.content)
            )
            if can_push:
                events.append(self.process_event(news_event(item, holdings, watchlist)))
        result["events"] = len(events)
        return result

    def _task_fast_news_scan(self) -> dict:
        return self._scan_news("fast")

    def _task_official_news_scan(self) -> dict:
        return self._scan_news("official")

    def _task_periodic_news_scan(self) -> dict:
        return self._scan_news("periodic")

    def _task_daily_close_pipeline(self) -> dict:
        from quantmaster.data import load_panel
        from quantmaster.data.universe import load_universe
        from quantmaster.decision import DecisionStore, daily_selection
        from quantmaster.market.regime import analyze_market

        cfg = get_config().automation
        end = pd.Timestamp.now().normalize()
        start = end - pd.Timedelta(days=500)
        symbols = load_universe(cfg.primary_universe)
        panel = load_panel(symbols, str(start.date()), str(end.date()))
        latest = pd.Timestamp(panel["close"].dropna(how="all").index[-1]).normalize()
        if latest < end:
            return {"status": "skipped", "reason": "无新 K 线", "latest": str(latest.date())}
        selection = daily_selection(panel, top_n=10, horizon=3)
        DecisionStore().save(selection, cfg.primary_universe)
        market = analyze_market(panel)
        current = market["current"]
        previous = None
        for item in self.store.recent_events(100):
            if item["kind"] == "market_close":
                previous = item["payload"].get("current")
                break
        event = close_regime_event(current, previous, str(latest.date()))
        if event:
            self.process_event(event)
        return {"status": "ok", "signal_date": selection["signal_date"],
                "picks": len(selection["picks"]), "market": current}

    def _task_news_digest(self) -> dict:
        items = [item for item in self.store.recent_events(100) if item["kind"] == "important_news"][:10]
        event = AlertEvent(
            kind="task_report", score=0, severity="info", data_as_of=datetime.now().isoformat(),
            evidence=[f"本期汇总 {len(items)} 条重要消息"] +
                     [str(item.get("payload", {}).get("title", "消息")) for item in items[:5]],
            dedupe_key=stable_hash({"digest": datetime.now().strftime("%Y%m%d%H")}),
            payload={"title": "重要消息摘要", "items": items},
        )
        self.process_event(event)
        return {"items": len(items)}

    def _task_paper_rebalance_proposal(self) -> dict:
        from quantmaster.backtest.paper import PaperTrader
        from quantmaster.backtest.strategy import SwingStrategy
        from quantmaster.data.universe import load_universe

        owner_targets = [target for target in self.store.targets()
                         if target["chat_type"] == "direct" and target["owner_actor"] and target["target"]]
        if not owner_targets:
            return {"status": "skipped", "reason": "尚未绑定主人私聊"}
        target = next((value for value in owner_targets if value["id"] == "feishu_owner"), owner_targets[0])
        symbols = load_universe(get_config().automation.primary_universe)
        proposal = PaperTrader(initialize=False).propose_once(
            SwingStrategy(top_n=5, holding_days=3), symbols)
        route_key = f"{target['channel']}:{target['account_id']}:{target['target']}"
        pending = self.store.create_pending_action(
            kind="paper_rebalance", actor=target["owner_actor"], route_key=route_key,
            payload=proposal, ttl_seconds=300,
        )
        event = AlertEvent(
            kind="task_report", score=0, severity="info", data_as_of=proposal["signal_date"],
            evidence=[f"计划成交 {len(proposal['planned'])} 笔",
                      f"确认码 {pending['code']}，5 分钟内在当前私聊确认"],
            dedupe_key=stable_hash({"paper_proposal": proposal["signal_date"]}),
            payload={"title": "模拟调仓待确认", "intent_id": pending["intent_id"]},
        )
        self.process_event(event, {target["id"]})
        return {"status": "pending_confirmation", "intent_id": pending["intent_id"],
                "planned": len(proposal["planned"])}

    # ---------- 私聊二阶段写入 ----------

    def prepare_ledger(self, actor: ActorContext, entry_type: str, payload: dict) -> dict:
        self.require_owner(actor, private=True)
        if entry_type == "trade":
            from quantmaster.data.universe import normalize_symbol
            value = {
                "date": str(pd.to_datetime(payload["date"], errors="raise").date()),
                "symbol": normalize_symbol(payload["symbol"]),
                "side": str(payload["side"]).lower(), "price": float(payload["price"]),
                "shares": float(payload["shares"]), "fee": float(payload.get("fee", 0)),
                "note": str(payload.get("note", ""))[:200],
            }
            if value["side"] not in {"buy", "sell"} or value["price"] <= 0 \
                    or value["shares"] <= 0 or value["fee"] < 0:
                raise ValueError("成交方向或数值非法")
        elif entry_type == "cashflow":
            value = {
                "date": str(pd.to_datetime(payload["date"], errors="raise").date()),
                "amount": abs(float(payload["amount"])), "kind": str(payload.get("kind", "deposit")),
                "note": str(payload.get("note", ""))[:200],
            }
            if value["amount"] <= 0 or value["kind"] not in {"deposit", "withdraw", "dividend"}:
                raise ValueError("现金流类型或金额非法")
        else:
            raise ValueError("首期仅支持 trade/cashflow")
        return self.store.create_pending_action(
            kind=entry_type, actor=actor.actor_key, route_key=actor.route_key,
            payload=value, ttl_seconds=300,
        )

    def confirm_ledger(self, actor: ActorContext, intent_id: str, code: str) -> dict:
        self.require_owner(actor, private=True)
        action = self.store.consume_pending_action(
            action_id=intent_id, code=code, actor=actor.actor_key, route_key=actor.route_key)
        payload, kind = action["payload"], action["kind"]
        if kind == "trade":
            from quantmaster.portfolio import Ledger, TradeRecord
            created = Ledger().add_trade(TradeRecord(**payload), idempotency_key=intent_id)
        elif kind == "cashflow":
            from quantmaster.portfolio import Ledger
            created = Ledger().add_cashflow(**payload, idempotency_key=intent_id)
        elif kind == "paper_rebalance":
            from quantmaster.backtest.paper import PaperTrader
            from quantmaster.portfolio import TradeRecord
            trades = [TradeRecord(**item) for item in payload.get("planned", [])]
            created = bool(PaperTrader(initialize=False).apply_rebalance(
                trades, idempotency_prefix=intent_id))
        else:
            raise ValueError("未知确认类型")
        self.store.audit(actor.actor_key, "confirm_write", kind, intent_id, {}, payload,
                         "created" if created else "duplicate")
        return {"status": "ok", "created": created, "type": kind}
