from __future__ import annotations

from copy import deepcopy
from typing import Any

PRESETS: dict[str, dict[str, Any]] = {
    "conservative": {
        "regime_threshold": 80, "confirmation_bars": 3, "cooldown_minutes": 60,
        "news_thresholds": {"holding": 75, "watchlist": 85, "market": 90},
        "hourly_cap": 3,
    },
    "balanced": {
        "regime_threshold": 65, "confirmation_bars": 2, "cooldown_minutes": 30,
        "news_thresholds": {"holding": 65, "watchlist": 75, "market": 80},
        "hourly_cap": 6,
    },
    "sensitive": {
        "regime_threshold": 50, "confirmation_bars": 1, "cooldown_minutes": 15,
        "news_thresholds": {"holding": 50, "watchlist": 60, "market": 70},
        "hourly_cap": 12,
    },
}

EVENT_KINDS = {"market_turn", "market_close", "important_news", "task_failure", "task_report"}


def resolved_policy(preset: str, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    if preset not in PRESETS:
        raise ValueError("preset 仅支持 conservative/balanced/sensitive")
    value = deepcopy(PRESETS[preset])
    overrides = overrides or {}
    allowed = {"regime_threshold", "confirmation_bars", "cooldown_minutes",
               "news_thresholds", "hourly_cap", "event_types"}
    unknown = set(overrides) - allowed
    if unknown:
        raise ValueError(f"不支持的高级设置: {', '.join(sorted(unknown))}")
    value.update({key: child for key, child in overrides.items() if key != "news_thresholds"})
    if "news_thresholds" in overrides:
        value["news_thresholds"].update(overrides["news_thresholds"])
    if not 1 <= int(value["confirmation_bars"]) <= 3:
        raise ValueError("确认根数必须为 1–3")
    if not 15 <= int(value["cooldown_minutes"]) <= 120:
        raise ValueError("冷却时间必须为 15–120 分钟")
    if not 1 <= int(value["hourly_cap"]) <= 30:
        raise ValueError("每小时上限必须为 1–30")
    for number in [value["regime_threshold"], *value["news_thresholds"].values()]:
        if not 0 <= float(number) <= 100:
            raise ValueError("推送阈值必须为 0–100")
    if "event_types" in value:
        invalid = set(value["event_types"]) - EVENT_KINDS
        if invalid:
            raise ValueError(f"未知事件类型: {', '.join(sorted(invalid))}")
    return value


def event_threshold(event: dict[str, Any], policy: dict[str, Any]) -> float:
    if event["kind"] in {"market_turn", "market_close"}:
        return float(policy["regime_threshold"])
    if event["kind"] == "important_news":
        relevance = event.get("relevance") or "market"
        return float(policy["news_thresholds"].get(relevance, policy["news_thresholds"]["market"]))
    return 0.0


def policy_allows(event: dict[str, Any], policy: dict[str, Any]) -> bool:
    event_types = policy.get("event_types")
    if event_types is not None and event["kind"] not in event_types:
        return False
    return float(event.get("score", 0)) >= event_threshold(event, policy)
