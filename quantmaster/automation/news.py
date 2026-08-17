from __future__ import annotations

import re
from collections.abc import Iterable

from quantmaster.ai.crawler import NewsItem
from quantmaster.ai.news_scoring import (
    CRITICAL_PATTERNS as _CRITICAL_PATTERNS,
)
from quantmaster.ai.news_scoring import (
    importance_score,
)
from quantmaster.automation.models import AlertEvent, stable_hash

CRITICAL_PATTERNS = _CRITICAL_PATTERNS


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
