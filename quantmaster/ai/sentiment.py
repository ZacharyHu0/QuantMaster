"""舆情因子：把爬虫入库的新闻情绪聚合成 (date × symbol) 因子面板。"""

from __future__ import annotations

import pandas as pd

from quantmaster.ai.crawler import NewsStore


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
