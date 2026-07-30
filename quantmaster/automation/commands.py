from __future__ import annotations

import json
import re
from collections.abc import Callable
from datetime import date

from quantmaster.automation.models import ActorContext
from quantmaster.automation.service import AutomationService

PRESET_NAMES = {"保守": "conservative", "均衡": "balanced", "敏感": "sensitive"}
PRESET_LABELS = {value: label for label, value in PRESET_NAMES.items()}
TASK_NAMES = {
    "盘中监控": "intraday_monitor", "变盘监控": "intraday_monitor",
    "快讯": "fast_news_scan", "官方公告": "official_news_scan", "定期资讯": "periodic_news_scan",
    "收盘": "daily_close_pipeline", "新闻摘要": "news_digest",
    "模拟调仓": "paper_rebalance_proposal",
}
VIEW_NAMES = {
    "市场": "market", "大盘": "market", "行情": "market",
    "选股": "selection", "候选股": "selection", "股票候选": "selection",
    "资讯": "news", "新闻": "news", "重要消息": "news",
    "持仓": "ledger", "账本": "ledger", "任务": "jobs", "告警": "alerts",
}
HELP_PATTERN = re.compile(
    r"^(?:/help|help|帮助|使用说明|操作说明|指令|命令|菜单|怎么用|如何使用|"
    r"你(?:会|能)(?:做)?什么|有什么功能)[？?。！!\s]*$",
    re.IGNORECASE,
)

STOCK_ANALYSIS_EXCLUSIONS = {"大盘", "市场", "行情", "任务", "持仓", "选股", "新闻", "资讯"}


def stock_analysis_query(text: str) -> str:
    """提取明确的个股分析意图；普通聊天和大盘查询继续走原有路由。"""
    value = re.sub(r"[？?。！!]+$", "", str(text).strip())
    patterns = (
        r"^(?:请|帮我)?\s*(?:做(?:一份)?|生成(?:一份)?)?\s*"
        r"(?:完整|深度|六维)?\s*(?:个股|股票|标的)?\s*分析(?:一下)?\s*[:：]?\s*(.+)$",
        r"^(?:请|帮我)?\s*(?:看看|研究|评估)(?:一下)?\s*[:：]?\s*"
        r"([A-Za-z0-9.\u4e00-\u9fff·]{2,24})$",
        r"^([A-Za-z0-9.\u4e00-\u9fff·]{2,24}?)\s*"
        r"(?:这只股票|这个标的)?\s*(?:怎么样|如何|值得关注吗|值得买吗)$",
    )
    for pattern in patterns:
        match = re.fullmatch(pattern, value, re.IGNORECASE)
        if not match:
            continue
        query = re.sub(
            r"^(?:一下|下)\s*", "", match.group(1).strip(), flags=re.IGNORECASE,
        )
        query = re.sub(r"(?:这只|这个)?(?:股票|标的)$", "", query).strip()
        if not query or any(term in query for term in STOCK_ANALYSIS_EXCLUSIONS):
            return ""
        return query[:80]
    return ""


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

    def _help(self, actor: ActorContext) -> str:
        target = self.service.store.target_by_route(
            actor.channel, actor.account_id, actor.target,
        )
        if not target:
            return (
                "QuantMaster 使用说明\n\n"
                "当前会话还没有绑定，因此暂时不会执行查询或设置。请打开 QuantMaster 的"
                "「自动化 → Bot 推送」，生成对应绑定码，再把页面给出的完整绑定指令发到这里。\n\n"
                "绑定成功后，发送「帮助」即可查看全部自然语言示例。"
            )

        lines = [
            "QuantMaster 使用说明",
            "",
            "你可以直接用自然语言表达。固定操作由受控指令助手执行；其他问题会由 AI 回答。",
            "",
            "查询",
            "• 现在大盘怎么样",
            "• 分析贵州茅台 / 六维分析 600519（生成完整报告）",
            "• 查看今天的选股 / 持仓 / 重要消息 / 告警 / 任务",
            "",
            "推送与任务（仅管理员）",
            "• 提醒少一点 / 把推送强度调成均衡 / 提醒敏感一点",
            "• 立即运行收盘 / 暂停盘中监控 / 恢复快讯",
        ]
        if actor.chat_type == "direct":
            lines.extend([
                "",
                "账本（仅管理员私聊，写入前会再次确认）",
                "• 买入 600519 100股 价格1500 费用5",
                "• 入金 100000",
            ])
        if actor.channel == "feishu" and actor.chat_type == "group":
            lines.extend([
                "",
                "群聊提示：我会静默参考近期话题；内容过长时压缩为话题记忆。"
                "只有真正 @QuantMaster 才会回复。普通成员可以查询和提问，"
                "推送设置与任务控制只有管理员可以执行。",
            ])
        lines.extend(["", "随时发送「帮助」可再次查看本说明。"])
        return "\n".join(lines)

    @staticmethod
    def _brief(value: object) -> str:
        text = json.dumps(value, ensure_ascii=False, indent=2, default=str)
        return text[:3500] + ("\n…内容已截断" if len(text) > 3500 else "")

    def handle(self, actor: ActorContext, text: str) -> None:
        analysis_query = stock_analysis_query(text)
        if analysis_query and actor.channel == "feishu":
            try:
                self.service.handle_stock_analysis(actor, analysis_query)
            except Exception as exc:
                self.reply(actor, f"个股分析未能启动：{exc}\n发送“帮助”查看可用说法。")
            if actor.chat_type == "group":
                self.service.executor.submit(self.service.maintain_conversation, actor)
            return
        try:
            answer = self.execute(actor, text.strip())
        except Exception as exc:
            answer = f"未执行：{exc}\n发送「帮助」查看可用说法。"
        self.reply(actor, answer)
        if actor.channel == "feishu" and actor.chat_type == "group":
            self.service.executor.submit(self.service.maintain_conversation, actor)

    def execute(self, actor: ActorContext, text: str) -> str:
        if HELP_PATTERN.fullmatch(text):
            return self._help(actor)

        binding = re.fullmatch(r"绑定(?:\s+QuantMaster)?\s+([A-Fa-f0-9]{8})", text)
        if binding:
            target = self.service.bind(actor, binding.group(1))
            return (
                f"已绑定为「{target['label']}」，默认推送强度：均衡。\n"
                "现在可以直接问「大盘怎么样」；发送「帮助」查看完整使用说明。"
            )

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
        natural_policy = None
        if not policy:
            if re.search(r"(?:提醒|推送).*(?:少一点|少些|低频|安静一点)", text):
                natural_policy = "保守"
            elif re.search(r"(?:提醒|推送).*(?:正常|适中|均衡)", text):
                natural_policy = "均衡"
            elif re.search(r"(?:提醒|推送).*(?:多一点|积极一点|敏感一点)", text):
                natural_policy = "敏感"
        if policy:
            natural_policy = policy.group(1)
        if natural_policy:
            self.service.require_owner(actor)
            target = self._bound(actor)
            before = target["preset"]
            after = PRESET_NAMES[natural_policy]
            self.service.update_policy(target["id"], after, target["overrides"], None, actor.actor_key)
            return (
                f"当前会话推送强度已从 {PRESET_LABELS.get(before, before)}"
                f"调整为 {PRESET_LABELS[after]}。"
            )

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
                return (
                    f"任务已受理，run_id={result['run_id']}；"
                    "结果会按当前会话的推送内容设置发送。"
                )

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

        return self.service.contextual_chat(actor, text)
