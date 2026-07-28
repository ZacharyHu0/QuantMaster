from __future__ import annotations

import re
from collections.abc import Iterable

from quantmaster.ai.crawler import NewsItem
from quantmaster.automation.models import AlertEvent, stable_hash

CRITICAL_PATTERNS = re.compile(
    r"退市|终止上市|暂停上市|停牌|债务违约|立案|行政处罚|重大事故|控制权变更|"
    r"要约收购|重大资产重组|业绩预亏|业绩骤降|欺诈发行|财务造假"
)
HIGH_PATTERNS = re.compile(r"并购|重组|增持|减持|回购|业绩预告|监管函|问询函|诉讼|仲裁")
SYSTEMIC_PATTERNS = re.compile(r"证监会|国务院|央行|货币政策|资本市场|交易制度|印花税|房地产")
OFFICIAL_SOURCES = {"csrc", "sse", "szse"}


def importance_score(item: NewsItem, holdings: set[str], watchlist: set[str],
                     corroborated: bool = False) -> tuple[float, str, list[str]]:
    text = f"{item.title} {item.content}"
    if CRITICAL_PATTERNS.search(text):
        base, category = 70, "重大"
    elif HIGH_PATTERNS.search(text) or item.event_type in {"并购", "业绩", "政策"}:
        base, category = 55, "高影响"
    else:
        base, category = 30, "普通"
    symbols = set(item.symbols)
    relevance = "holding" if symbols & holdings else "watchlist" if symbols & watchlist else "market"
    relevance_bonus = 20 if relevance == "holding" else 10 if relevance == "watchlist" else (
        15 if SYSTEMIC_PATTERNS.search(text) else 0)
    official = item.is_official or item.source in OFFICIAL_SOURCES
    score = base + (10 if official else 0) + relevance_bonus + (10 if corroborated else 0)
    score += min(5, abs(float(item.sentiment or 0)) * 5)
    reasons = [f"{category}事件基础分 {base}", f"相关范围：{relevance}"]
    if official:
        reasons.append("官方来源 +10")
    if corroborated:
        reasons.append("多来源确认 +10")
    return min(100.0, round(score, 2)), relevance, reasons


def news_event(item: NewsItem, holdings: set[str], watchlist: set[str],
               corroborated: bool = False) -> AlertEvent:
    score, relevance, reasons = importance_score(item, holdings, watchlist, corroborated)
    normalized = re.sub(r"\W+", "", item.title.casefold())
    fingerprint = stable_hash({
        "title": normalized, "symbols": sorted(item.symbols),
        "sectors": sorted(item.sectors),
        "event_type": item.event_type or "其他", "day": (item.published_at or "")[:10],
    })
    return AlertEvent(
        kind="important_news", score=score,
        severity="critical" if score >= 95 else ("high" if score >= 80 else "medium"),
        direction="up" if item.sentiment > 0.15 else "down" if item.sentiment < -0.15 else "neutral",
        data_as_of=item.published_at, symbols=list(item.symbols), relevance=relevance,
        evidence=reasons,
        source_urls=[item.url] if item.url else [], dedupe_key=fingerprint,
        payload={"source": item.source, "title": item.title,
                 "summary": item.summary or item.content[:160],
                 "sentiment": float(item.sentiment or 0),
                 "event_type": item.event_type,
                 "sectors": list(item.sectors)},
    )


def build_news_events(items: Iterable[NewsItem], holdings: set[str],
                      watchlist: set[str]) -> list[AlertEvent]:
    return [news_event(item, holdings, watchlist) for item in items]
