"""个股六维分析的飞书进度卡片与报告卡片。"""

from __future__ import annotations

from typing import Any

from quantmaster.analysis.stock import STOCK_ANALYSIS_PHASES


def _text(value: Any, limit: int = 260) -> str:
    result = str(value or "").strip()
    if len(result) > limit:
        result = result[:limit - 1] + "…"
    for character in ("\\", "*", "_", "[", "]", "<", ">"):
        result = result.replace(character, f"\\{character}")
    return result


def _progress_bar(progress: int) -> str:
    completed = max(0, min(10, round(progress / 10)))
    return "█" * completed + "░" * (10 - completed)


def stock_analysis_progress_card(
    query: str, progress: int = 3, phase: str = "准备分析", detail: str = "正在创建分析任务",
) -> dict[str, Any]:
    """一张可原位更新的进度卡，避免飞书会话被阶段消息刷屏。"""
    value = max(0, min(100, int(progress)))
    stage_lines = []
    for threshold, label in STOCK_ANALYSIS_PHASES:
        if value >= threshold:
            marker = "✓"
        elif not stage_lines or all(line.startswith("✓") for line in stage_lines):
            marker = "→"
        else:
            marker = "·"
        stage_lines.append(f"{marker} {label}")
    content = (
        f"**分析标的**  {_text(query, 80)}\n"
        f"**当前阶段**  {_text(phase, 80)}\n\n"
        f"`{_progress_bar(value)}`  **{value}%**\n"
        f"{_text(detail, 240)}\n\n"
        + "　".join(stage_lines)
    )
    return {
        "config": {"wide_screen_mode": True, "enable_forward": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "QuantMaster · 个股六维分析"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": content}},
            {"tag": "note", "elements": [{
                "tag": "plain_text",
                "content": "可继续聊天；分析完成后本卡片会原位更新。",
            }]},
        ],
    }


def stock_analysis_failure_card(query: str, message: str) -> dict[str, Any]:
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "red",
            "title": {"tag": "plain_text", "content": "QuantMaster · 分析未完成"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                f"**分析标的**  {_text(query, 80)}\n\n"
                f"**原因**  {_text(message, 500)}\n\n"
                "请补充准确代码（如 `600519.SH`）或稍后重试。"
            )}},
            {"tag": "note", "elements": [{
                "tag": "plain_text", "content": "没有执行交易或写入账本。",
            }]},
        ],
    }


def _dimension_content(item: dict[str, Any]) -> str:
    status = {"complete": "数据较完整", "partial": "部分数据", "unavailable": "数据缺失"}.get(
        str(item.get("status")), "待核查")
    metrics = []
    for metric in (item.get("metrics") or [])[:6]:
        line = f"**{_text(metric.get('label'), 50)}**  {_text(metric.get('display'), 100)}"
        if metric.get("note"):
            line += f"  ·  {_text(metric['note'], 100)}"
        metrics.append(line)
    signals = [f"• {_text(value)}" for value in (item.get("signals") or [])[:3]]
    risks = [f"• 风险：{_text(value)}" for value in (item.get("risks") or [])[:2]]
    rows = [
        f"**{_text(item.get('number'))} {_text(item.get('title'))}**  "
        f"`{float(item.get('score') or 0):.0f}/100`  {_text(item.get('stance'))}  ·  {status}",
        _text(item.get("summary"), 420),
    ]
    if metrics:
        rows.extend(["", "　｜　".join(metrics)])
    if signals or risks:
        rows.extend(["", *signals, *risks])
    return "\n".join(rows)


def stock_analysis_report_card(report: dict[str, Any]) -> dict[str, Any]:
    instrument = report.get("instrument") or {}
    quote = report.get("quote") or {}
    overall = report.get("overall") or {}
    change = quote.get("change_pct")
    template = "red" if isinstance(change, (int, float)) and change > 0 else (
        "green" if isinstance(change, (int, float)) and change < 0 else "blue")
    name = str(instrument.get("name") or instrument.get("en_name") or instrument.get("symbol") or "标的")
    symbol = str(instrument.get("symbol") or "")
    price = quote.get("current")
    change_text = "—" if change is None else f"{float(change):+.2f}%"
    price_text = "—" if price is None else f"{float(price):.2f}"
    summary = (
        f"**综合判断**  {float(overall.get('score') or 0):.1f}/100 · "
        f"{_text(overall.get('stance'))}\n"
        f"**数据覆盖**  {float(overall.get('coverage') or 0):.0f}%    "
        f"**结论置信**  {float(overall.get('confidence') or 0):.0f}%\n"
        f"**最近收盘**  {price_text} ({change_text})    **数据截至**  {_text(report.get('data_as_of'))}\n\n"
        f"**一句话结论**\n{_text(overall.get('thesis'), 480)}\n\n"
        f"{_text(overall.get('summary'), 700)}"
    )
    elements: list[dict[str, Any]] = [
        {"tag": "div", "text": {"tag": "lark_md", "content": summary}},
        {"tag": "hr"},
    ]
    for index, item in enumerate(report.get("dimensions") or []):
        elements.append({
            "tag": "div", "text": {"tag": "lark_md", "content": _dimension_content(item)},
        })
        if index < len(report.get("dimensions") or []) - 1:
            elements.append({"tag": "hr"})
    scenarios = report.get("scenarios") or []
    if scenarios:
        scenario_lines = ["**情景验证**"]
        for scenario in scenarios[:3]:
            scenario_lines.extend([
                f"**{_text(scenario.get('title'))} · {_text(scenario.get('priority'))}**",
                f"触发：{_text(scenario.get('condition'), 300)}",
                f"应对：{_text(scenario.get('response'), 240)}",
            ])
        elements.extend([
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": "\n\n".join(scenario_lines)}},
        ])
    risks = [str(value) for value in (overall.get("risks") or [])[:6]]
    warnings = [str(value) for value in (report.get("warnings") or [])[:4]]
    if risks or warnings:
        elements.extend([
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                "**总风险清单**\n" + "\n".join(f"• {_text(value)}" for value in [*risks, *warnings])
            )}},
        ])
    elements.append({
        "tag": "note", "elements": [{
            "tag": "plain_text", "content": str(report.get("disclaimer") or "仅作研究，不构成投资建议。"),
        }],
    })
    return {
        "config": {"wide_screen_mode": True, "enable_forward": True},
        "header": {
            "template": template,
            "title": {
                "tag": "plain_text",
                "content": f"QuantMaster · {name}（{symbol}）六维分析",
            },
        },
        "elements": elements,
    }
