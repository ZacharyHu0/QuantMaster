"""个股六维分析的飞书进度卡片与报告卡片。"""

from __future__ import annotations

import json
from typing import Any

from quantmaster.analysis.stock import STOCK_ANALYSIS_PHASES

FEISHU_CARD_LIMIT_BYTES = 28 * 1024


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
    *, mode: str = "deep", dimensions: list[dict[str, Any]] | None = None,
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
        f"**分析模式**  {'快速' if mode == 'quick' else '深度联网'}\n"
        f"**当前阶段**  {_text(phase, 80)}\n\n"
        f"`{_progress_bar(value)}`  **{value}%**\n"
        f"{_text(detail, 240)}\n\n"
        + "　".join(stage_lines)
    )
    elements: list[dict[str, Any]] = [
        {"tag": "div", "text": {"tag": "lark_md", "content": content}},
    ]
    for item in dimensions or []:
        elements.extend([
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": _dimension_content(item)}},
        ])
    elements.append({"tag": "note", "elements": [{
        "tag": "plain_text", "content": "可继续聊天；已完成维度会立即保留在本卡片中。",
    }]})
    card = {
        "config": {"wide_screen_mode": True, "enable_forward": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "QuantMaster · 个股六维分析"},
        },
        "elements": elements,
    }
    if card_size_bytes(card) > FEISHU_CARD_LIMIT_BYTES:
        compact_elements: list[dict[str, Any]] = [elements[0]]
        for item in dimensions or []:
            compact_elements.extend([
                {"tag": "hr"},
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": _dimension_content(item, compact=True)},
                },
            ])
        compact_elements.append(elements[-1])
        card["elements"] = compact_elements
    if card_size_bytes(card) > FEISHU_CARD_LIMIT_BYTES:
        raise ValueError("飞书渐进卡超过 28 KB 限制")
    return card


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


def _dimension_content(item: dict[str, Any], *, compact: bool = False) -> str:
    status = {"complete": "数据较完整", "partial": "部分数据", "unavailable": "数据缺失"}.get(
        str(item.get("status")), "待核查")
    metrics = []
    metric_limit = 3 if compact else 6
    for metric in (item.get("metrics") or [])[:metric_limit]:
        line = f"**{_text(metric.get('label'), 40)}**  {_text(metric.get('display'), 60)}"
        if metric.get("note"):
            line += f"  ·  {_text(metric['note'], 60 if compact else 100)}"
        metrics.append(line)
    signals = [
        f"• {_text(value, 160 if compact else 260)}"
        for value in (item.get("signals") or [])[:1 if compact else 3]
    ]
    risks = [f"• 风险：{_text(value)}" for value in (item.get("risks") or [])[:2]]
    rows = [
        f"**{_text(item.get('number'))} {_text(item.get('title'))}**  "
        f"`{float(item.get('score') or 0):.0f}/100`  {_text(item.get('stance'))}  ·  {status}",
        _text(item.get("summary"), 240 if compact else 420),
    ]
    if metrics:
        rows.extend(["", "　｜　".join(metrics)])
    if signals or risks:
        rows.extend(["", *signals, *risks])
    if item.get("degraded_reason"):
        rows.extend(["", f"降级：{_text(item['degraded_reason'], 400)}"])
    references, seen = [], set()
    for evidence in item.get("evidence") or []:
        source = evidence.get("source") or {}
        url = str(source.get("url") or "")
        if not url.startswith(("http://", "https://")) or url in seen:
            continue
        seen.add(url)
        references.append(f"[{_text(source.get('name') or evidence.get('title'), 60)}]({url})")
    if references:
        rows.extend(["", "来源：" + "　".join(references[:2 if compact else 4])])
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
    card = {
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
    if card_size_bytes(card) <= FEISHU_CARD_LIMIT_BYTES:
        return card
    compact_elements: list[dict[str, Any]] = [
        {"tag": "div", "text": {"tag": "lark_md", "content": summary}}, {"tag": "hr"},
    ]
    for index, item in enumerate(report.get("dimensions") or []):
        compact_elements.append({
            "tag": "div", "text": {"tag": "lark_md", "content": (
                f"**{_text(item.get('number'))} {_text(item.get('title'))}**  "
                f"`{float(item.get('score') or 0):.0f}/100`  {_text(item.get('stance'))}\n"
                f"{_text(item.get('summary'), 900)}\n" + "\n".join(
                    f"• 风险：{_text(value, 280)}" for value in (item.get("risks") or [])[:3]
                )
            )},
        })
        if index < len(report.get("dimensions") or []) - 1:
            compact_elements.append({"tag": "hr"})
    compact_elements.append({"tag": "note", "elements": [{
        "tag": "plain_text",
        "content": str(report.get("disclaimer") or "仅作研究，不构成投资建议。"),
    }]})
    card["elements"] = compact_elements
    if card_size_bytes(card) > FEISHU_CARD_LIMIT_BYTES:
        raise ValueError("六维主卡超过 28 KB，无法在不丢失结论的情况下发送")
    return card


def card_size_bytes(card: dict[str, Any]) -> int:
    return len(json.dumps(card, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _appendix_card(
    title: str, elements: list[dict[str, Any]], index: int, total: int,
) -> dict[str, Any]:
    return {
        "config": {"wide_screen_mode": True, "enable_forward": True},
        "header": {"template": "grey", "title": {"tag": "plain_text", "content": (
            f"QuantMaster · {title} · 证据附录 {index}/{total}"
        )}},
        "elements": elements,
    }


def _evidence_blocks(report: dict[str, Any]) -> list[dict[str, Any]]:
    blocks, number = [], 0
    for dimension in report.get("dimensions") or []:
        for evidence in dimension.get("evidence") or []:
            number += 1
            source = evidence.get("source") or {}
            url = str(source.get("url") or "")
            source_text = _text(source.get("name") or "未知来源", 180)
            if url.startswith(("http://", "https://")):
                source_text = f"[{source_text}]({url})"
            value = json.dumps(
                evidence.get("value"), ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False,
            )
            escaped_value = _text(value, max(2, len(value) + 1))
            content = (
                f"**E{number:03d} · {_text(dimension.get('title'), 40)} · "
                f"{_text(evidence.get('title'), 240)}**\n"
                f"来源 L{source.get('level', '—')}：{source_text}\n"
                f"发布日期：{_text(evidence.get('published_at') or '未提供', 80)}　"
                f"数据时点：{_text(evidence.get('data_as_of') or '未提供', 80)}\n"
                f"证据 ID：`{_text(evidence.get('id'), 80)}`\n"
                f"摘要：{_text(evidence.get('excerpt') or '—', 1600)}\n"
                f"结构化值：`{escaped_value}`"
            )
            chunks = [content[offset:offset + 8000] for offset in range(0, len(content), 8000)]
            for chunk_index, chunk in enumerate(chunks or [content], 1):
                suffix = f"（续 {chunk_index}/{len(chunks)}）\n" if len(chunks) > 1 else ""
                blocks.append({
                    "tag": "div", "text": {"tag": "lark_md", "content": suffix + chunk},
                })
    return blocks


def stock_analysis_report_cards(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Return a self-contained main card plus lossless <=28KB numbered appendices."""
    main, blocks = stock_analysis_report_card(report), _evidence_blocks(report)
    if not blocks:
        return [main]
    instrument = report.get("instrument") or {}
    title = str(instrument.get("name") or instrument.get("symbol") or "个股分析")
    groups: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for block in blocks:
        candidate = [*current, block]
        if current and card_size_bytes(_appendix_card(title, candidate, 999, 999)) > FEISHU_CARD_LIMIT_BYTES:
            groups.append(current)
            current = [block]
        else:
            current = candidate
        if card_size_bytes(_appendix_card(title, current, 999, 999)) > FEISHU_CARD_LIMIT_BYTES:
            raise ValueError("单个证据分片超过飞书 28 KB 限制")
    if current:
        groups.append(current)
    cards = [main, *[
        _appendix_card(title, values, index, len(groups))
        for index, values in enumerate(groups, 1)
    ]]
    if any(card_size_bytes(value) > FEISHU_CARD_LIMIT_BYTES for value in cards):
        raise ValueError("飞书卡片分片超过 28 KB 限制")
    return cards
