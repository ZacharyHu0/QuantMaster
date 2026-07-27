from __future__ import annotations

import json
import re
from collections.abc import Callable
from datetime import date

from quantmaster.automation.models import ActorContext
from quantmaster.automation.service import AutomationService

PRESET_NAMES = {"保守": "conservative", "均衡": "balanced", "敏感": "sensitive"}
TASK_NAMES = {
    "盘中监控": "intraday_monitor", "变盘监控": "intraday_monitor",
    "快讯": "fast_news_scan", "官方公告": "official_news_scan", "定期资讯": "periodic_news_scan",
    "收盘": "daily_close_pipeline", "新闻摘要": "news_digest",
    "模拟调仓": "paper_rebalance_proposal",
}
VIEW_NAMES = {
    "市场": "market", "选股": "selection", "资讯": "news", "新闻": "news",
    "持仓": "ledger", "账本": "ledger", "任务": "jobs", "告警": "alerts",
}


class BotCommandRouter:
    def __init__(self, service: AutomationService,
                 reply: Callable[[ActorContext, str], None]):
        self.service = service
        self.reply = reply

    def _bound(self, actor: ActorContext) -> dict:
        target = self.service.store.target_by_route(actor.channel, actor.account_id, actor.target)
        if not target:
            raise PermissionError("当前会话尚未绑定，请先在自动化页面生成绑定码")
        return target

    @staticmethod
    def _brief(value: object) -> str:
        text = json.dumps(value, ensure_ascii=False, indent=2, default=str)
        return text[:3500] + ("\n…内容已截断" if len(text) > 3500 else "")

    def handle(self, actor: ActorContext, text: str) -> None:
        try:
            answer = self.execute(actor, text.strip())
        except Exception as exc:
            answer = f"未执行：{exc}"
        self.reply(actor, answer)

    def execute(self, actor: ActorContext, text: str) -> str:
        binding = re.fullmatch(r"绑定(?:\s+QuantMaster)?\s+([A-Fa-f0-9]{8})", text)
        if binding:
            target = self.service.bind(actor, binding.group(1))
            return f"已绑定为「{target['label']}」，默认推送强度：均衡。"

        self._bound(actor)
        confirm = re.fullmatch(r"确认(?:\s+([a-f0-9]{16,32}))?\s+(\d{6})", text)
        if confirm:
            self.service.require_owner(actor, private=True)
            intent_id = confirm.group(1)
            if not intent_id:
                pending = self.service.store.latest_pending_action(actor.actor_key, actor.route_key)
                if not pending:
                    raise ValueError("当前私聊没有待确认操作")
                intent_id = pending["id"]
            result = self.service.confirm_ledger(actor, intent_id, confirm.group(2))
            return f"确认完成：{result['type']}，{'已写入' if result['created'] else '此前已写入'}。"

        policy = re.search(r"(?:调成|设置为|推送强度为?)\s*(保守|均衡|敏感)", text)
        if policy:
            self.service.require_owner(actor)
            target = self._bound(actor)
            before = target["preset"]
            after = PRESET_NAMES[policy.group(1)]
            self.service.update_policy(target["id"], after, target["overrides"], None, actor.actor_key)
            return f"当前会话推送强度已从 {before} 调整为 {after}。"

        for label, task in TASK_NAMES.items():
            if label not in text:
                continue
            if text.startswith(("暂停", "停掉", "关闭")):
                self.service.require_owner(actor)
                self.service.update_schedule(task, action="pause", schedule=None, actor=actor.actor_key)
                from quantmaster.automation.runtime import get_runtime
                get_runtime().reload_jobs()
                return f"已暂停 {label}。"
            if text.startswith(("恢复", "开启")):
                self.service.require_owner(actor)
                self.service.update_schedule(task, action="resume", schedule=None, actor=actor.actor_key)
                from quantmaster.automation.runtime import get_runtime
                get_runtime().reload_jobs()
                return f"已恢复 {label}。"
            if text.startswith(("运行", "执行", "立即")):
                self.service.require_owner(actor, private=task == "paper_rebalance_proposal")
                result = self.service.run_task(task, actor=actor.actor_key)
                return f"任务已受理，run_id={result['run_id']}；完成后会另行推送。"

        trade = re.fullmatch(
            r"(?:记一笔|记录)?\s*(买入|卖出)\s+(\d{6}(?:\.(?:SH|SZ|BJ))?)\s+"
            r"([\d.]+)\s*股?\s+(?:价格|@)\s*([\d.]+)(?:\s+费用\s*([\d.]+))?", text, re.I)
        if trade:
            intent = self.service.prepare_ledger(actor, "trade", {
                "date": str(date.today()), "symbol": trade.group(2),
                "side": "buy" if trade.group(1) == "买入" else "sell",
                "shares": float(trade.group(3)), "price": float(trade.group(4)),
                "fee": float(trade.group(5) or 0),
            })
            return (f"成交预览：{trade.group(1)} {trade.group(2)} {trade.group(3)}股 "
                    f"@{trade.group(4)}，费用 {trade.group(5) or 0}。\n"
                    f"5 分钟内回复「确认 {intent['code']}」提交。")

        cash = re.fullmatch(r"(?:记录)?\s*(入金|出金|分红)\s*([\d.]+)", text)
        if cash:
            kind = {"入金": "deposit", "出金": "withdraw", "分红": "dividend"}[cash.group(1)]
            intent = self.service.prepare_ledger(actor, "cashflow", {
                "date": str(date.today()), "amount": float(cash.group(2)), "kind": kind,
            })
            return (
                f"现金流预览：{cash.group(1)} {cash.group(2)} 元。\n"
                f"5 分钟内回复「确认 {intent['code']}」提交。")

        for label, view in VIEW_NAMES.items():
            if label in text:
                return self._brief(self.service.query(view))

        return (
            "可用命令示例：\n"
            "• 把当前推送强度调成敏感\n• 查看任务 / 持仓 / 新闻 / 告警\n"
            "• 运行收盘 / 暂停盘中监控 / 恢复快讯\n"
            "• 买入 600519 100股 价格1500 费用5\n• 入金 100000\n"
            "所有写入都只允许主人私聊并需要二次确认。"
        )
