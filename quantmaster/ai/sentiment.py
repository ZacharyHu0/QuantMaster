"""舆情因子：把爬虫入库的新闻情绪聚合成 (date × symbol) 因子面板。"""

from __future__ import annotations

import math
from typing import Any, Literal

import pandas as pd

from quantmaster.ai.crawler import NewsStore
from quantmaster.config import get_config
from quantmaster.factors.base import Factor, PanelDict
from quantmaster.trading_sessions import daily_signal_cutoff

_MINIMUM_RESEARCH_SESSIONS = 756 + 30 + 252
_MINIMUM_HISTORY_COVERAGE = 0.70
_SANDBOX_FAST_SOURCE_WEIGHT = 0.25

NewsFactorTier = Literal["production", "sandbox"]


def _factor_tier(value: str) -> NewsFactorTier:
    if value not in {"production", "sandbox"}:
        raise ValueError("消息面因子 tier 只支持 production 或 sandbox")
    return "production" if value == "production" else "sandbox"


def _factor_metadata(
    index: pd.DatetimeIndex,
    rows: list[dict[str, Any]],
    *,
    tier: NewsFactorTier,
) -> dict[str, Any]:
    signal_days = pd.DatetimeIndex([
        pd.Timestamp(row["_signal_day"]).normalize()
        for row in rows
        if row.get("_signal_day") is not None
    ]).unique().sort_values()
    if signal_days.empty:
        sample_start = sample_end = ""
        history_sessions = overlap_sessions = 0
    else:
        sample_start = signal_days[0].strftime("%Y-%m-%d")
        sample_end = signal_days[-1].strftime("%Y-%m-%d")
        history_sessions = len(pd.bdate_range(signal_days[0], signal_days[-1]))
        overlap_sessions = int(((index >= signal_days[0]) & (index <= signal_days[-1])).sum())
    requested_sessions = len(index)
    coverage = overlap_sessions / max(1, requested_sessions)

    sources: dict[str, dict[str, Any]] = {}
    row_reasons: set[str] = set()
    for row in rows:
        source_id = str(row.get("source_id") or "unknown")
        source = sources.setdefault(source_id, {
            "source_id": source_id,
            "source_name": str(row.get("source_name") or source_id),
            "source_group": str(row.get("source_group") or ""),
            "event_count": 0,
            "weight_multiplier": float(row.get("_source_multiplier") or 1.0),
            "formal_row_count": 0,
            "formal_eligible": False,
            "reasons": set(),
        })
        source["event_count"] += 1
        source["formal_row_count"] += int(bool(row.get("formal_eligible")))
        reasons = {
            str(reason) for reason in row.get("formal_ineligible_reasons") or [] if reason
        }
        source["reasons"].update(reasons)
        row_reasons.update(reasons)

    reasons = set(row_reasons)
    if tier == "sandbox":
        reasons.add("sandbox_tier")
    if not rows:
        reasons.add("no_usable_publication_rows")
    if history_sessions < _MINIMUM_RESEARCH_SESSIONS:
        reasons.add("history_sessions_below_1038")
    if coverage < _MINIMUM_HISTORY_COVERAGE:
        reasons.add("coverage_below_70_percent")
    production_eligible = bool(
        tier == "production"
        and rows
        and history_sessions >= _MINIMUM_RESEARCH_SESSIONS
        and coverage >= _MINIMUM_HISTORY_COVERAGE
        and all(bool(row.get("formal_eligible")) for row in rows)
    )
    if not production_eligible and not reasons:
        reasons.add("production_promotion_gate_failed")

    source_values = []
    for source_id in sorted(sources):
        source = sources[source_id]
        source_reasons = set(source.pop("reasons"))
        if tier == "sandbox":
            source_reasons.add("sandbox_tier")
        if history_sessions < _MINIMUM_RESEARCH_SESSIONS:
            source_reasons.add("history_sessions_below_1038")
        if coverage < _MINIMUM_HISTORY_COVERAGE:
            source_reasons.add("coverage_below_70_percent")
        source["formal_eligible"] = bool(
            production_eligible and source["formal_row_count"] == source["event_count"]
        )
        source["reasons"] = sorted(source_reasons)
        source_values.append(source)

    return {
        "alignment_contract": "publication_v2",
        "tier": tier,
        "sample_start": sample_start,
        "sample_end": sample_end,
        "sessions": history_sessions,
        "requested_sessions": requested_sessions,
        "overlap_sessions": overlap_sessions,
        "coverage": round(coverage, 6),
        "event_count": len(rows),
        "sources": source_values,
        "formal_eligible": production_eligible,
        "reasons": sorted(reasons),
        "promotion_requirements": {
            "minimum_sessions": _MINIMUM_RESEARCH_SESSIONS,
            "minimum_coverage": _MINIMUM_HISTORY_COVERAGE,
            "complete_ingest_window": True,
            "official_bound_raw_evidence": True,
        },
    }


def news_sentiment_readiness(
    start: str,
    end: str,
    *,
    store: NewsStore | None = None,
    minimum_sessions: int = _MINIMUM_RESEARCH_SESSIONS,
    minimum_coverage: float = _MINIMUM_HISTORY_COVERAGE,
) -> dict:
    """Assess local publication-aligned news history without contacting the network."""
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
    first_epoch = float(coverage.get("first_published_at") or 0)
    last_epoch = float(coverage.get("last_published_at") or 0)
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
    *,
    tier: NewsFactorTier = "production",
) -> pd.DataFrame:
    """构造可回测的质量加权个股消息面因子。

    资讯完成处理后按来源给出的发布时间映射到对应收盘信号；上海 15:00 后发布
    的消息进入下一交易日。处理、抓取与正文版本时间只保留作证据审计，不改变
    市场影响的归属日期。精确重复内容只保留最高权重贡献。``production`` 只读取
    完整官方证据窗口；显式 ``sandbox`` 可预览内置快讯，但结果携带不可晋级原因，
    且 fast 来源仅按 25% 来源权重参与。
    """
    selected_tier = _factor_tier(tier)
    index = pd.DatetimeIndex(reference_index).tz_localize(None).normalize().unique().sort_values()
    columns = list(dict.fromkeys(symbols))
    result = pd.DataFrame(float("nan"), index=index, columns=columns, dtype=float)
    if index.empty or not columns:
        result.attrs["news_factor"] = _factor_metadata(index, [], tier=selected_tier)
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
    rows = (
        store.factor_rows(start_epoch, end_epoch)
        if selected_tier == "production"
        else store.sandbox_factor_rows(start_epoch, end_epoch)
    )
    if not rows:
        result.attrs["news_factor"] = _factor_metadata(index, [], tier=selected_tier)
        return result

    impulses: dict[tuple[pd.Timestamp, str], list[tuple[float, float]]] = {}
    deduped: dict[tuple[pd.Timestamp, str, str], tuple[float, float]] = {}
    used_rows: dict[int | str, dict[str, Any]] = {}
    column_set = set(columns)
    for row in rows:
        confidence = float(row.get("confidence") or 0)
        importance = float(row.get("importance_score") or 0)
        source_weight = float(row.get("source_weight") or 0)
        source_multiplier = (
            _SANDBOX_FAST_SOURCE_WEIGHT
            if selected_tier == "sandbox"
            and (
                str(row.get("source_id") or "") == "sina_live"
                or (
                    str(row.get("source_group") or "") == "fast"
                    and not bool(row.get("formal_eligible"))
                )
            )
            else 1.0
        )
        source_weight *= source_multiplier
        base_weight = source_weight * confidence * importance / 100.0
        if confidence < minimum or base_weight <= 0:
            continue
        published_epoch = float(row.get("published_at_epoch") or 0)
        if published_epoch <= 0:
            continue
        publication = pd.Timestamp(
            published_epoch, unit="s", tz="UTC",
        ).tz_convert("Asia/Shanghai")
        local_day = publication.tz_localize(None).normalize()
        cutoff = pd.Timestamp(daily_signal_cutoff(publication.date()))
        side = "right" if publication > cutoff else "left"
        position = int(index.searchsorted(local_day, side=side))
        if position >= len(index):
            continue
        signal_day = index[position]
        weight = base_weight
        if weight <= 0:
            continue
        sentiment = max(-1.0, min(1.0, float(row.get("sentiment") or 0)))
        content_hash = str(row.get("content_hash") or row.get("id") or "")
        matching_symbols = [
            symbol for symbol in row.get("symbols") or [] if symbol in column_set
        ]
        if not matching_symbols:
            continue
        metadata_row = dict(row)
        metadata_row["_signal_day"] = signal_day
        metadata_row["_source_multiplier"] = source_multiplier
        used_rows[row.get("id") or content_hash] = metadata_row
        for symbol in matching_symbols:
            key = (signal_day, symbol, content_hash)
            existing_entry = deduped.get(key)
            if existing_entry is None or weight > existing_entry[1]:
                deduped[key] = (sentiment, weight)
    for (day, symbol, _content_hash), value in deduped.items():
        impulses.setdefault((day, symbol), []).append(value)

    for symbol in columns:
        previous_value: float | None = None
        previous_day: pd.Timestamp | None = None
        for day in index:
            values = impulses.get((day, symbol), [])
            impulse = None
            if values:
                denominator = sum(weight for _score, weight in values)
                impulse = sum(score * weight for score, weight in values) / denominator
            if previous_value is None:
                if impulse is None:
                    continue
                current = impulse
            else:
                elapsed = max(0, (day - previous_day).days) if previous_day is not None else 1
                decay = 0.5 ** (elapsed / halflife)
                current = (
                    previous_value * decay
                    if impulse is None
                    else previous_value * decay + impulse * (1 - decay)
                )
            result.at[day, symbol] = max(-1.0, min(1.0, current))
            previous_value, previous_day = current, day
    result.attrs["news_factor"] = _factor_metadata(
        index, list(used_rows.values()), tier=selected_tier,
    )
    return result


class NewsSentimentFactor(Factor):
    """本地资讯库驱动的一等因子，不会在计算或回测时触网。"""

    name = "news_sentiment"
    description = (
        "[消息面] 情绪×置信度×重要度×来源权重聚合，按资讯发布时间对齐，默认 3 日衰减。"
    )

    def __init__(
        self,
        store: NewsStore | None = None,
        *,
        tier: NewsFactorTier = "production",
    ):
        self.store = store
        self.tier = _factor_tier(tier)

    def compute(self, panel: PanelDict) -> pd.DataFrame:
        reference = panel["close"]
        return quality_sentiment_panel(
            reference.index, list(reference.columns), store=self.store, tier=self.tier,
        ).reindex(index=reference.index, columns=reference.columns)


def list_news_factors() -> list[dict]:
    return [{"name": NewsSentimentFactor.name,
             "description": NewsSentimentFactor.description, "expression": ""}]
