"""AI 爬虫：抓取财经资讯 → LLM 结构化 → 本地存储。

内置免费源（JSON/网页接口，无需 key）：
- 新浪财经 7x24 快讯
- 东方财富全球财经快讯

流水线：fetch（抓取）→ extract（LLM 结构化：关联股票/事件类型/情绪分）
→ store（SQLite），情绪分可聚合为舆情因子。

礼貌抓取：控制频率、仅访问公开接口。可自行在 SOURCES 中登记新源。
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from quantmaster.ai.llm import LLMClient
from quantmaster.config import get_config

USER_AGENT = "Mozilla/5.0 (compatible; QuantMaster/0.1; +https://github.com/ZacharyHu0/QuantMaster)"


@dataclass
class NewsItem:
    source: str
    title: str
    content: str
    url: str = ""
    published_at: str = ""
    # LLM 结构化结果
    symbols: list[str] = field(default_factory=list)
    event_type: str = ""      # 政策/业绩/并购/行业/宏观/其他
    sentiment: float = 0.0    # -1(极度利空) ~ +1(极度利好)
    summary: str = ""


# ---- 抓取器（每个源一个函数，返回 list[NewsItem]） ----

def fetch_sina_live(limit: int = 30) -> list[NewsItem]:  # pragma: no cover - 网络
    """新浪财经 7x24 快讯。"""
    url = "https://zhibo.sina.com.cn/api/zhibo/feed"
    params = {"page": 1, "page_size": limit, "zhibo_id": 152, "tag_id": 0}
    resp = httpx.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    items = []
    for entry in data.get("result", {}).get("data", {}).get("feed", {}).get("list", []):
        text = re.sub(r"<[^>]+>", "", entry.get("rich_text", ""))
        items.append(NewsItem(
            source="sina_live",
            title=text[:60],
            content=text,
            published_at=entry.get("create_time", ""),
        ))
    return items


def fetch_eastmoney_fast(limit: int = 30) -> list[NewsItem]:  # pragma: no cover - 网络
    """东方财富全球财经快讯。"""
    url = "https://np-listapi.eastmoney.com/comm/web/getFastNewsList"
    params = {"client": "web", "biz": "web_724", "fastColumn": "102", "sortEnd": "", "pageSize": limit}
    resp = httpx.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    items = []
    for entry in (data.get("data") or {}).get("fastNewsList", []) or []:
        items.append(NewsItem(
            source="eastmoney_fast",
            title=entry.get("title", "")[:60],
            content=entry.get("summary") or entry.get("title", ""),
            published_at=entry.get("showTime", ""),
        ))
    return items


SOURCES = {
    "sina_live": fetch_sina_live,
    "eastmoney_fast": fetch_eastmoney_fast,
}

EXTRACT_SYSTEM = """你是A股财经新闻分析师。对每条新闻输出：
- symbols: 直接相关的A股代码数组（格式 600519.SH / 000001.SZ，无法确定则空数组）
- event_type: 政策|业绩|并购|行业|宏观|其他
- sentiment: -1到1的数值，对相关股票（无个股则对A股整体）的利空/利好程度
- summary: 不超过40字的摘要"""


class NewsStore:
    def __init__(self, path: Path | None = None):
        self.path = path or get_config().data_root / "news.sqlite"
        with self._conn() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS news ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "source TEXT, title TEXT, content TEXT, url TEXT, published_at TEXT,"
                "symbols TEXT, event_type TEXT, sentiment REAL, summary TEXT,"
                "created_at REAL, UNIQUE(source, title, published_at))"
            )

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def save(self, items: list[NewsItem]) -> int:
        saved = 0
        with self._conn() as conn:
            for item in items:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO news "
                        "(source,title,content,url,published_at,symbols,event_type,"
                        "sentiment,summary,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (item.source, item.title, item.content, item.url, item.published_at,
                         json.dumps(item.symbols, ensure_ascii=False), item.event_type,
                         item.sentiment, item.summary, time.time()),
                    )
                    saved += conn.total_changes > 0
                except sqlite3.Error:
                    continue
        return saved

    def recent(self, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM news ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["symbols"] = json.loads(d.get("symbols") or "[]")
            result.append(d)
        return result


class AICrawler:
    """抓取 + LLM 结构化 + 入库。未配置 LLM 时可只抓取入库（skip_llm=True）。"""

    def __init__(self, client: LLMClient | None = None, store: NewsStore | None = None):
        self._client = client
        self.store = store or NewsStore()

    @property
    def client(self) -> LLMClient:
        if self._client is None:
            self._client = LLMClient()
        return self._client

    def extract(self, items: list[NewsItem], batch_size: int = 10) -> list[NewsItem]:
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            numbered = "\n".join(f"{j + 1}. {it.content[:300]}" for j, it in enumerate(batch))
            prompt = (
                f"分析以下 {len(batch)} 条新闻，输出 JSON 数组（与输入同序等长）：\n"
                '[{"symbols": [], "event_type": "", "sentiment": 0.0, "summary": ""}]\n\n'
                + numbered
            )
            try:
                parsed = self.client.chat_json(prompt, system=EXTRACT_SYSTEM)
            except Exception:
                continue
            if not isinstance(parsed, list):
                continue
            for item, result in zip(batch, parsed, strict=False):
                if not isinstance(result, dict):
                    continue
                item.symbols = [str(s) for s in result.get("symbols", [])]
                item.event_type = str(result.get("event_type", ""))
                try:
                    item.sentiment = max(-1.0, min(1.0, float(result.get("sentiment", 0))))
                except (TypeError, ValueError):
                    item.sentiment = 0.0
                item.summary = str(result.get("summary", ""))
        return items

    def run(self, sources: list[str] | None = None, limit: int = 30,
            skip_llm: bool = False) -> dict:
        names = sources or list(SOURCES)
        fetched: list[NewsItem] = []
        errors: dict[str, str] = {}
        for name in names:
            fetcher = SOURCES.get(name)
            if fetcher is None:
                errors[name] = "未知源"
                continue
            try:
                fetched.extend(fetcher(limit=limit))
            except Exception as e:
                errors[name] = str(e)
        if fetched and not skip_llm:
            self.extract(fetched)
        saved = self.store.save(fetched)
        return {"fetched": len(fetched), "saved": saved, "errors": errors}
