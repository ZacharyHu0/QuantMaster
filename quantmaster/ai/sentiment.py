"""舆情因子：把爬虫入库的新闻情绪聚合成 (date × symbol) 因子面板。"""

from __future__ import annotations

import math

import pandas as pd

from quantmaster.ai.crawler import NewsStore
from quantmaster.config import get_config
from quantmaster.factors.base import Factor, PanelDict


_MINIMUM_RESEARCH_SESSIONS = 756 + 30 + 252
_MINIMUM_HISTORY_COVERAGE = 0.70


def news_sentiment_readiness(
    start: str,
    end: str,
    *,
    store: NewsStore | None = None,
    minimum_sessions: int = _MINIMUM_RESEARCH_SESSIONS,
    minimum_coverage: float = _MINIMUM_HISTORY_COVERAGE,
) -> dict:
    """Assess local PIT news history for research without contacting the network."""
    requested = pd.bdate_range(start, end)
    database = get_config().data_root / "news.sqlite"
    if store is None and not database.is_file():
        return {
            "ready": False,
            "available_start": "",
            "available_end": "",
            "event_count": 0,
            "history_sessions": 0,
            "overlap_sessions": 0,
            "requested_sessions": len(requested),
            "coverage": 0.0,
            "minimum_sessions": int(minimum_sessions),
            "minimum_coverage": float(minimum_coverage),
        }
    coverage = (store or NewsStore(database)).factor_coverage(
        get_config().news.factor_min_confidence,
    )
    first_epoch = float(coverage.get("first_seen_at") or 0)
    last_epoch = float(coverage.get("last_seen_at") or 0)
    if not first_epoch or not last_epoch:
        available = pd.DatetimeIndex([])
        overlap = pd.DatetimeIndex([])
        available_start = available_end = ""
    else:
        first = pd.Timestamp(first_epoch, unit="s", tz="UTC").tz_convert(
            "Asia/Shanghai",
        ).tz_localize(None).normalize()
        last = pd.Timestamp(last_epoch, unit="s", tz="UTC").tz_convert(
            "Asia/Shanghai",
        ).tz_localize(None).normalize()
        available = pd.bdate_range(first, last)
        overlap_start = max(first, requested[0]) if len(requested) else first
        overlap_end = min(last, requested[-1]) if len(requested) else last
        overlap = (
            pd.bdate_range(overlap_start, overlap_end)
            if overlap_start <= overlap_end else pd.DatetimeIndex([])
        )
        available_start = first.strftime("%Y-%m-%d")
        available_end = last.strftime("%Y-%m-%d")
    ratio = len(overlap) / max(1, len(requested))
    history_sessions = len(available)
    return {
        "ready": (
            int(coverage.get("event_count") or 0) > 0
            and history_sessions >= int(minimum_sessions)
            and ratio >= float(minimum_coverage)
        ),
        "available_start": available_start,
        "available_end": available_end,
        "event_count": int(coverage.get("event_count") or 0),
        "history_sessions": history_sessions,
        "overlap_sessions": len(overlap),
        "requested_sessions": len(requested),
        "coverage": round(ratio, 6),
        "minimum_sessions": int(minimum_sessions),
        "minimum_coverage": float(minimum_coverage),
    }


def sentiment_panel(
    store: NewsStore | None = None,
    halflife_days: float = 3.0,
    limit: int = 5000,
) -> pd.DataFrame:
    """按股票聚合新闻情绪，指数衰减加权（半衰期默认 3 天）。

    返回 date × symbol 的情绪面板，可与量价因子一同标准化、合成。
    """
    store = store or NewsStore()
    rows = store.recent(limit=limit)
    records = []
    for row in rows:
        date = pd.to_datetime(row.get("published_at") or None, errors="coerce")
        if pd.isna(date):
            continue
        for symbol in row.get("symbols", []):
            records.append({"date": date.normalize(), "symbol": symbol,
                            "sentiment": row.get("sentiment") or 0.0})
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    daily = df.groupby(["date", "symbol"])["sentiment"].mean().unstack()
    daily = daily.sort_index().asfreq("D")
    # 指数衰减：没有新消息时情绪逐日向 0 衰减
    decay = 0.5 ** (1.0 / halflife_days)
    values = daily.to_numpy(copy=True)
    for i in range(1, len(values)):
        prev = values[i - 1]
        cur = values[i]
        carried = prev * decay
        mask = pd.isna(cur)
        cur[mask] = carried[mask]
        values[i] = cur
    return pd.DataFrame(values, index=daily.index, columns=daily.columns)


def quality_sentiment_panel(
    reference_index: pd.DatetimeIndex,
    symbols: list[str],
    store: NewsStore | None = None,
    halflife_days: float | None = None,
    min_confidence: float | None = None,
) -> pd.DataFrame:
    """构造可回测的质量加权个股消息面因子。

    资讯按首次入库时间映射到其后第一个收盘时点；15:00 后入库的消息只进入
    下一交易日，防止盘后消息泄漏进当日信号。精确重复内容只保留最高权重贡献。
    """
    index = pd.DatetimeIndex(reference_index).tz_localize(None).normalize().unique().sort_values()
    columns = list(dict.fromkeys(symbols))
    result = pd.DataFrame(float("nan"), index=index, columns=columns, dtype=float)
    if index.empty or not columns:
        return result
    cfg = get_config().news
    halflife = float(halflife_days or cfg.factor_halflife_days)
    minimum = cfg.factor_min_confidence if min_confidence is None else float(min_confidence)
    if halflife <= 0:
        raise ValueError("消息面因子半衰期必须大于 0")
    store = store or NewsStore()
    start_epoch = (
        index[0] - pd.Timedelta(days=max(30, math.ceil(halflife * 8)))
    ).tz_localize("Asia/Shanghai").timestamp()
    end_epoch = (
        index[-1] + pd.Timedelta(days=2)
    ).tz_localize("Asia/Shanghai").timestamp()
    rows = store.factor_rows(start_epoch, end_epoch)
    if not rows:
        return result

    impulses: dict[tuple[pd.Timestamp, str], list[tuple[float, float]]] = {}
    deduped: dict[tuple[pd.Timestamp, str, str], tuple[float, float]] = {}
    column_set = set(columns)
    for row in rows:
        confidence = float(row.get("confidence") or 0)
        importance = float(row.get("importance_score") or 0)
        source_weight = float(row.get("source_weight") or 0)
        weight = source_weight * confidence * importance / 100.0
        if confidence < minimum or weight <= 0:
            continue
        first_seen = pd.Timestamp(float(row["first_seen_at"]), unit="s", tz="UTC").tz_convert(
            "Asia/Shanghai")
        local_day = first_seen.tz_localize(None).normalize()
        side = "right" if (first_seen.hour, first_seen.minute) > (15, 0) else "left"
        position = int(index.searchsorted(local_day, side=side))
        if position >= len(index):
            continue
        signal_day = index[position]
        sentiment = max(-1.0, min(1.0, float(row.get("sentiment") or 0)))
        content_hash = str(row.get("content_hash") or row.get("id") or "")
        for symbol in row.get("symbols") or []:
            if symbol not in column_set:
                continue
            key = (signal_day, symbol, content_hash)
            previous = deduped.get(key)
            if previous is None or weight > previous[1]:
                deduped[key] = (sentiment, weight)
    for (day, symbol, _content_hash), value in deduped.items():
        impulses.setdefault((day, symbol), []).append(value)

    for symbol in columns:
        previous: float | None = None
        previous_day: pd.Timestamp | None = None
        for day in index:
            values = impulses.get((day, symbol), [])
            impulse = None
            if values:
                denominator = sum(weight for _score, weight in values)
                impulse = sum(score * weight for score, weight in values) / denominator
            if previous is None:
                if impulse is None:
                    continue
                current = impulse
            else:
                elapsed = max(0, (day - previous_day).days) if previous_day is not None else 1
                decay = 0.5 ** (elapsed / halflife)
                current = previous * decay if impulse is None else previous * decay + impulse * (1 - decay)
            result.at[day, symbol] = max(-1.0, min(1.0, current))
            previous, previous_day = current, day
    return result


class NewsSentimentFactor(Factor):
    """本地资讯库驱动的一等因子，不会在计算或回测时触网。"""

    name = "news_sentiment"
    description = (
        "[消息面] 情绪×置信度×重要度×来源权重聚合，按首次获取时点对齐，默认 3 日衰减。"
    )

    def __init__(self, store: NewsStore | None = None):
        self.store = store

    def compute(self, panel: PanelDict) -> pd.DataFrame:
        reference = panel["close"]
        return quality_sentiment_panel(
            reference.index, list(reference.columns), store=self.store,
        ).reindex(index=reference.index, columns=reference.columns)


def list_news_factors() -> list[dict]:
    return [{"name": NewsSentimentFactor.name,
             "description": NewsSentimentFactor.description, "expression": ""}]
