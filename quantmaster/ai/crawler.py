"""AI 爬虫：抓取财经资讯 → LLM 结构化 → 本地存储。

内置免费源（JSON/网页接口，无需 key）：
- 新浪财经 7x24 快讯
- 东方财富全球财经快讯

流水线：fetch（抓取）→ extract（LLM 结构化：关联股票/事件类型/情绪分）
→ store（SQLite），情绪分可聚合为舆情因子。

礼貌抓取：控制频率、仅访问公开接口。可自行在 SOURCES 中登记新源。
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx

from quantmaster.ai.llm import LLMClient
from quantmaster.ai.news_sources import (
    FetchedArticle,
    NewsSourceStore,
    fetch_declarative_source,
)
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
    importance_score: float = 0.0
    scope: str = ""
    urgency: str = ""
    confidence: float = 0.0
    fingerprint: str = ""
    is_official: bool = False
    raw_cache_key: str = ""
    analysis_status: str = "pending"
    db_id: int | None = None


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


class _ListingParser(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self._href = ""
        self._text: list[str] = []
        self.links: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            self._href = dict(attrs).get("href") or ""
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._href:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._href:
            return
        text = re.sub(r"\s+", " ", "".join(self._text)).strip()
        if len(text) >= 8 and not self._href.lower().startswith(("javascript:", "#")):
            self.links.append((text, urljoin(self.base_url, self._href)))
        self._href, self._text = "", []


def _fetch_official_listing(source: str, url: str, limit: int = 30) -> list[NewsItem]:
    response = httpx.get(url, headers={"User-Agent": USER_AGENT}, timeout=20, follow_redirects=True)
    response.raise_for_status()
    parser = _ListingParser(str(response.url))
    parser.feed(response.text)
    seen: set[tuple[str, str]] = set()
    items: list[NewsItem] = []
    for title, link in parser.links:
        key = (title, link)
        if key in seen:
            continue
        seen.add(key)
        if not re.search(r"公告|通知|决定|意见|规则|监管|处罚|问询|回复|报告|发布|答记者问|解读", title) \
                and not re.search(r"/20\d{2}[-_/]", link):
            continue
        items.append(NewsItem(
            source=source, title=title[:120], content=title, url=link, is_official=True,
        ))
        if len(items) >= limit:
            break
    return items


def fetch_csrc(limit: int = 30) -> list[NewsItem]:  # pragma: no cover - 网络
    return _fetch_official_listing(
        "csrc", "https://www.csrc.gov.cn/csrc/c100028/common_list.shtml", limit)


def fetch_sse(limit: int = 30) -> list[NewsItem]:  # pragma: no cover - 网络
    return _fetch_official_listing(
        "sse", "https://www.sse.com.cn/disclosure/listedinfo/announcement/", limit)


def fetch_szse(limit: int = 30) -> list[NewsItem]:  # pragma: no cover - 网络
    return _fetch_official_listing(
        "szse", "https://www.szse.cn/disclosure/listed/notice/index.html", limit)


SOURCES = {
    "sina_live": fetch_sina_live,
    "eastmoney_fast": fetch_eastmoney_fast,
    "csrc": fetch_csrc,
    "sse": fetch_sse,
    "szse": fetch_szse,
}

EXTRACT_SYSTEM = """你是A股财经新闻分析师。对每条新闻输出：
- symbols: 直接相关的A股代码数组（格式 600519.SH / 000001.SZ，无法确定则空数组）
- event_type: 政策|业绩|并购|行业|宏观|其他
- sentiment: -1到1的数值，对相关股票（无个股则对A股整体）的利空/利好程度
- summary: 不超过40字的摘要
- scope: holding|watchlist|market
- urgency: critical|high|normal
- confidence: 0到1"""


class NewsStore:
    """长期结构化资讯库；原始响应由 :class:`NewsSourceStore` 分层缓存。"""

    ANALYSIS_VERSION = 1

    def __init__(self, path: Path | None = None):
        self.path = path or get_config().data_root / "news.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.sources = NewsSourceStore(self.path)
        self._migrate()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    @staticmethod
    def fingerprint(item: NewsItem) -> str:
        title = re.sub(r"\W+", "", item.title.casefold())
        identity = f"{item.url.strip().lower()}|{title}|{item.published_at.strip()}"
        return hashlib.sha256(f"{item.source}|{identity}".encode()).hexdigest()

    @staticmethod
    def content_hash(item: NewsItem) -> str:
        text = re.sub(r"\s+", "", f"{item.title}\n{item.content}".casefold())
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def _migrate(self) -> None:
        with self._conn() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS news ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,source TEXT,title TEXT,content TEXT,"
                "url TEXT,published_at TEXT,symbols TEXT,event_type TEXT,sentiment REAL,summary TEXT,"
                "created_at REAL,importance_score REAL DEFAULT 0,scope TEXT DEFAULT '',"
                "urgency TEXT DEFAULT '',confidence REAL DEFAULT 0,fingerprint TEXT DEFAULT '',"
                "is_official INTEGER DEFAULT 0,source_id TEXT DEFAULT '',"
                "content_hash TEXT DEFAULT '',first_seen_at REAL DEFAULT 0,last_seen_at REAL DEFAULT 0,"
                "raw_cache_key TEXT DEFAULT '',analysis_status TEXT DEFAULT 'pending',"
                "analysis_attempts INTEGER DEFAULT 0,analysis_error TEXT DEFAULT '',"
                "analysis_version INTEGER DEFAULT 1,next_retry_at REAL DEFAULT 0,"
                "parser_version TEXT DEFAULT '1',UNIQUE(source,title,published_at))"
            )
            columns = {row[1] for row in conn.execute("PRAGMA table_info(news)")}
            additions = {
                "importance_score": "REAL DEFAULT 0", "scope": "TEXT DEFAULT ''",
                "urgency": "TEXT DEFAULT ''", "confidence": "REAL DEFAULT 0",
                "fingerprint": "TEXT DEFAULT ''", "is_official": "INTEGER DEFAULT 0",
                "source_id": "TEXT DEFAULT ''", "content_hash": "TEXT DEFAULT ''",
                "first_seen_at": "REAL DEFAULT 0", "last_seen_at": "REAL DEFAULT 0",
                "raw_cache_key": "TEXT DEFAULT ''", "analysis_status": "TEXT DEFAULT 'pending'",
                "analysis_attempts": "INTEGER DEFAULT 0", "analysis_error": "TEXT DEFAULT ''",
                "analysis_version": "INTEGER DEFAULT 1", "next_retry_at": "REAL DEFAULT 0",
                "parser_version": "TEXT DEFAULT '1'",
            }
            for name, sql_type in additions.items():
                if name not in columns:
                    conn.execute(f"ALTER TABLE news ADD COLUMN {name} {sql_type}")
            conn.execute("UPDATE news SET source_id=source WHERE source_id='' OR source_id IS NULL")
            conn.execute("UPDATE news SET first_seen_at=created_at WHERE first_seen_at=0")
            conn.execute("UPDATE news SET last_seen_at=created_at WHERE last_seen_at=0")
            rows = conn.execute(
                "SELECT id,source,title,content,url,published_at,fingerprint,content_hash,"
                "summary,confidence,symbols,analysis_status FROM news"
            ).fetchall()
            for row in rows:
                item = NewsItem(
                    source=row["source"], title=row["title"] or "", content=row["content"] or "",
                    url=row["url"] or "", published_at=row["published_at"] or "",
                )
                fingerprint = row["fingerprint"] or self.fingerprint(item)
                content_hash = row["content_hash"] or self.content_hash(item)
                status = row["analysis_status"] or "pending"
                has_analysis = (
                    row["summary"] or row["confidence"]
                    or row["symbols"] not in {"", "[]", None}
                )
                if status == "pending" and has_analysis:
                    status = "complete"
                conn.execute(
                    "UPDATE news SET fingerprint=?,content_hash=?,analysis_status=? WHERE id=?",
                    (fingerprint, content_hash, status, row["id"]),
                )
            conn.executescript("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_news_fingerprint_unique
                    ON news(fingerprint) WHERE fingerprint<>'';
                CREATE INDEX IF NOT EXISTS idx_news_recent ON news(id DESC);
                CREATE INDEX IF NOT EXISTS idx_news_source ON news(source_id,id DESC);
                CREATE INDEX IF NOT EXISTS idx_news_analysis
                    ON news(analysis_status,next_retry_at,id);
                CREATE INDEX IF NOT EXISTS idx_news_seen ON news(first_seen_at DESC);
            """)

    @staticmethod
    def _decode(row: sqlite3.Row) -> dict:
        value = dict(row)
        try:
            value["symbols"] = json.loads(value.get("symbols") or "[]")
        except (TypeError, json.JSONDecodeError):
            value["symbols"] = []
        value["is_official"] = bool(value.get("is_official"))
        epoch = float(value.get("first_seen_at") or value.get("created_at") or 0)
        value["first_seen_epoch"] = epoch
        value["first_seen_at"] = (
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch)) if epoch else ""
        )
        return value

    def save(self, items: list[NewsItem]) -> int:
        saved = 0
        now = time.time()
        with self._conn() as conn:
            for item in items:
                item.fingerprint = item.fingerprint or self.fingerprint(item)
                content_hash = self.content_hash(item)
                status = (
                    item.analysis_status
                    if item.analysis_status in {"pending", "complete"}
                    else "pending"
                )
                row = conn.execute(
                    "SELECT id,analysis_status,length(content) AS content_length FROM news "
                    "WHERE fingerprint=? OR (source=? AND title=? AND published_at=?) LIMIT 1",
                    (item.fingerprint, item.source, item.title, item.published_at),
                ).fetchone()
                if row:
                    analysis_sql = ""
                    analysis_params: list[Any] = []
                    if status == "complete" and row["analysis_status"] != "complete":
                        analysis_sql = (
                            ",symbols=?,event_type=?,sentiment=?,summary=?,importance_score=?,"
                            "scope=?,urgency=?,confidence=?,analysis_status='complete',analysis_error=''"
                        )
                        analysis_params = [
                            json.dumps(item.symbols, ensure_ascii=False), item.event_type,
                            item.sentiment, item.summary, item.importance_score, item.scope,
                            item.urgency, item.confidence,
                        ]
                    content = item.content if len(item.content) >= int(row["content_length"] or 0) else None
                    conn.execute(
                        "UPDATE news SET content=COALESCE(?,content),url=CASE WHEN ?<>'' THEN ? ELSE url END,"
                        "last_seen_at=?,raw_cache_key=CASE WHEN ?<>'' THEN ? ELSE raw_cache_key END "
                        f"{analysis_sql} WHERE id=?",
                        [content, item.url, item.url, now, item.raw_cache_key, item.raw_cache_key,
                         *analysis_params, row["id"]],
                    )
                    continue
                try:
                    conn.execute(
                        "INSERT INTO news "
                        "(source,title,content,url,published_at,symbols,event_type,sentiment,summary,"
                        "created_at,importance_score,scope,urgency,confidence,fingerprint,is_official,"
                        "source_id,content_hash,first_seen_at,last_seen_at,raw_cache_key,analysis_status,"
                        "analysis_version,parser_version) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (item.source, item.title, item.content, item.url, item.published_at,
                         json.dumps(item.symbols, ensure_ascii=False), item.event_type, item.sentiment,
                         item.summary, now, item.importance_score, item.scope, item.urgency,
                         item.confidence, item.fingerprint, int(item.is_official), item.source,
                         content_hash, now, now, item.raw_cache_key, status,
                         self.ANALYSIS_VERSION, "1"),
                    )
                    saved += 1
                except sqlite3.IntegrityError:
                    continue
        return saved

    def pending(self, limit: int = 100, ids: list[int] | None = None) -> list[dict]:
        where = "analysis_status IN ('pending','failed') AND analysis_attempts<3 AND next_retry_at<=?"
        params: list[Any] = [time.time()]
        if ids:
            placeholders = ",".join("?" for _ in ids)
            where += f" AND id IN ({placeholders})"
            params.extend(ids)
        params.append(max(1, min(limit, 1000)))
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT * FROM news WHERE {where} ORDER BY id LIMIT ?", params
            ).fetchall()
        return [self._decode(row) for row in rows]

    def update_analysis(self, item_id: int, item: NewsItem) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE news SET symbols=?,event_type=?,sentiment=?,summary=?,importance_score=?,"
                "scope=?,urgency=?,confidence=?,analysis_status='complete',analysis_error='',"
                "analysis_version=?,next_retry_at=0 WHERE id=?",
                (json.dumps(item.symbols, ensure_ascii=False), item.event_type, item.sentiment,
                 item.summary, item.importance_score, item.scope, item.urgency, item.confidence,
                 self.ANALYSIS_VERSION, item_id),
            )

    def analysis_failure(self, item_ids: list[int], error: str) -> None:
        if not item_ids:
            return
        with self._conn() as conn:
            for item_id in item_ids:
                row = conn.execute(
                    "SELECT analysis_attempts FROM news WHERE id=?", (item_id,)
                ).fetchone()
                attempts = int(row[0] if row else 0) + 1
                delay = (60, 300, 1800)[min(attempts - 1, 2)]
                conn.execute(
                    "UPDATE news SET analysis_status='failed',analysis_attempts=?,analysis_error=?,"
                    "next_retry_at=? WHERE id=? AND analysis_status<>'complete'",
                    (attempts, error[:1000], time.time() + delay, item_id),
                )

    def reset_analysis(self, ids: list[int] | None = None) -> int:
        where, params = "analysis_status<>'pending'", []
        if ids:
            placeholders = ",".join("?" for _ in ids)
            where = f"id IN ({placeholders})"
            params = list(ids)
        with self._conn() as conn:
            cursor = conn.execute(
                f"UPDATE news SET analysis_status='pending',analysis_attempts=0,analysis_error='',"
                f"next_retry_at=0 WHERE {where}", params,
            )
        return cursor.rowcount

    def query(self, *, limit: int = 50, cursor: int | None = None, q: str = "",
              source: str = "", group_name: str = "", event_type: str = "",
              sentiment: str = "", scope: str = "", symbol: str = "",
              status: str = "", date_from: float | None = None,
              date_to: float | None = None, sort: str = "recent") -> dict:
        clauses, params = [], []
        if cursor and sort != "importance":
            clauses.append("n.id<?")
            params.append(cursor)
        if q:
            escaped = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append(
                "(n.title LIKE ? ESCAPE '\\' OR n.summary LIKE ? ESCAPE '\\' "
                "OR n.content LIKE ? ESCAPE '\\')"
            )
            params.extend([f"%{escaped}%"] * 3)
        if source:
            clauses.append("n.source_id=?")
            params.append(source)
        if group_name:
            clauses.append("s.group_name=?")
            params.append(group_name)
        if event_type:
            clauses.append("n.event_type=?")
            params.append(event_type)
        if sentiment == "positive":
            clauses.append("n.sentiment>0.15")
        elif sentiment == "negative":
            clauses.append("n.sentiment<-0.15")
        elif sentiment == "neutral":
            clauses.append("n.sentiment BETWEEN -0.15 AND 0.15")
        if scope:
            clauses.append("n.scope=?")
            params.append(scope)
        if symbol:
            clauses.append("n.symbols LIKE ?")
            params.append(f'%"{symbol}"%')
        if status:
            clauses.append("n.analysis_status=?")
            params.append(status)
        if date_from is not None:
            clauses.append("n.first_seen_at>=?")
            params.append(date_from)
        if date_to is not None:
            clauses.append("n.first_seen_at<=?")
            params.append(date_to)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        order = "n.importance_score DESC,n.id DESC" if sort == "importance" else "n.id DESC"
        page_size = max(1, min(limit, 100))
        offset = max(0, int(cursor or 0)) if sort == "importance" else 0
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT n.*,COALESCE(s.name,n.source_id) AS source_name,"
                "COALESCE(s.group_name,'') AS source_group,COALESCE(s.factor_weight,1) AS source_weight "
                f"FROM news n LEFT JOIN news_sources s ON s.id=n.source_id {where} "
                f"ORDER BY {order} LIMIT ? OFFSET ?", [*params, page_size + 1, offset],
            ).fetchall()
        has_more = len(rows) > page_size
        items = [self._decode(row) for row in rows[:page_size]]
        return {
            "items": items, "has_more": has_more,
            "next_cursor": (
                offset + page_size if has_more and sort == "importance"
                else items[-1]["id"] if has_more and items else None
            ),
        }

    def recent(self, limit: int = 50) -> list[dict]:
        return self.query(limit=limit)["items"]

    def max_id(self) -> int:
        with self._conn() as conn:
            row = conn.execute("SELECT COALESCE(MAX(id),0) FROM news").fetchone()
        return int(row[0] if row else 0)

    def after_id(self, item_id: int, limit: int = 500) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT n.*,COALESCE(s.name,n.source_id) AS source_name,"
                "COALESCE(s.group_name,'') AS source_group,"
                "COALESCE(s.factor_weight,1) AS source_weight FROM news n "
                "LEFT JOIN news_sources s ON s.id=n.source_id WHERE n.id>? "
                "ORDER BY n.id LIMIT ?",
                (item_id, max(1, min(limit, 2000))),
            ).fetchall()
        return [self._decode(row) for row in rows]

    def update_context(self, item_id: int, *, importance_score: float,
                       scope: str, urgency: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE news SET importance_score=?,scope=?,urgency=? WHERE id=?",
                (importance_score, scope, urgency, item_id),
            )

    def detail(self, item_id: int) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT n.*,COALESCE(s.name,n.source_id) AS source_name,"
                "COALESCE(s.group_name,'') AS source_group,COALESCE(s.factor_weight,1) AS source_weight "
                "FROM news n LEFT JOIN news_sources s ON s.id=n.source_id WHERE n.id=?",
                (item_id,),
            ).fetchone()
        return self._decode(row) if row else None

    def factor_rows(self, start_epoch: float | None = None,
                    end_epoch: float | None = None) -> list[dict]:
        clauses = ["n.analysis_status='complete'"]
        params: list[Any] = []
        if start_epoch is not None:
            clauses.append("n.first_seen_at>=?")
            params.append(start_epoch)
        if end_epoch is not None:
            clauses.append("n.first_seen_at<=?")
            params.append(end_epoch)
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT n.id,n.first_seen_at,n.symbols,n.sentiment,n.confidence,"
                "n.importance_score,n.content_hash,COALESCE(s.factor_weight,1) AS source_weight "
                "FROM news n LEFT JOIN news_sources s ON s.id=n.source_id WHERE "
                + " AND ".join(clauses) + " ORDER BY n.first_seen_at,n.id",
                params,
            ).fetchall()
        result = []
        for row in rows:
            value = dict(row)
            value["symbols"] = json.loads(value.get("symbols") or "[]")
            result.append(value)
        return result

    def stats(self, days: int = 30) -> dict:
        cutoff = time.time() - max(1, min(days, 3650)) * 86400
        with self._conn() as conn:
            counts = conn.execute(
                "SELECT COUNT(*) AS total,"
                "SUM(analysis_status='complete') AS annotated,"
                "SUM(analysis_status='pending') AS pending,"
                "SUM(analysis_status='failed') AS failed,"
                "SUM(importance_score>=80) AS important,"
                "SUM(sentiment>0.15 AND analysis_status='complete') AS positive,"
                "SUM(sentiment<-0.15 AND analysis_status='complete') AS negative "
                "FROM news WHERE first_seen_at>=?", (cutoff,),
            ).fetchone()
            rows = conn.execute(
                "SELECT n.first_seen_at,n.sentiment,n.confidence,n.importance_score,n.symbols,"
                "COALESCE(s.factor_weight,1) AS source_weight FROM news n "
                "LEFT JOIN news_sources s ON s.id=n.source_id "
                "WHERE n.first_seen_at>=? AND n.analysis_status='complete'",
                (cutoff,),
            ).fetchall()
        daily: dict[str, list[tuple[float, float]]] = {}
        symbol_counts: dict[str, int] = {}
        minimum = get_config().news.factor_min_confidence
        for row in rows:
            confidence = float(row["confidence"] or 0)
            if confidence < minimum:
                continue
            weight = float(row["source_weight"] or 1) * confidence * float(row["importance_score"] or 0) / 100
            if weight <= 0:
                continue
            day = datetime.fromtimestamp(
                float(row["first_seen_at"]), tz=timezone.utc,
            ).astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
            daily.setdefault(day, []).append((float(row["sentiment"] or 0), weight))
            for symbol in json.loads(row["symbols"] or "[]"):
                symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1
        series = [
            [day, round(sum(score * weight for score, weight in values) /
                        sum(weight for _score, weight in values), 4)]
            for day, values in sorted(daily.items())
        ]
        data = dict(counts) if counts else {}
        data["coverage"] = round(int(data.get("annotated") or 0) / max(1, int(data.get("total") or 0)), 4)
        data["sentiment_series"] = series
        data["top_symbols"] = [
            {"symbol": symbol, "count": count}
            for symbol, count in sorted(symbol_counts.items(), key=lambda item: item[1], reverse=True)[:8]
        ]
        return data


class AICrawler:
    """先归档再标注；模型不可用时资讯仍会可靠进入待处理队列。"""

    def __init__(self, client: LLMClient | None = None, store: NewsStore | None = None,
                 source_store: NewsSourceStore | None = None):
        self._client = client
        self.store = store or NewsStore()
        self.source_store = source_store or self.store.sources

    @property
    def client(self) -> LLMClient:
        if self._client is None:
            self._client = LLMClient()
        return self._client

    @staticmethod
    def _apply_result(item: NewsItem, result: dict) -> None:
        item.symbols = [str(s).strip().upper() for s in result.get("symbols", []) if str(s).strip()]
        item.event_type = str(result.get("event_type", "其他"))
        try:
            item.sentiment = max(-1.0, min(1.0, float(result.get("sentiment", 0))))
        except (TypeError, ValueError):
            item.sentiment = 0.0
        item.summary = str(result.get("summary", ""))[:240]
        item.scope = str(result.get("scope", "market"))
        item.urgency = str(result.get("urgency", "normal"))
        try:
            item.confidence = max(0.0, min(1.0, float(result.get("confidence", 0))))
        except (TypeError, ValueError):
            item.confidence = 0.0
        item.analysis_status = "complete"

    def extract(self, items: list[NewsItem], batch_size: int = 10) -> list[NewsItem]:
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            numbered = "\n".join(f"{j + 1}. {it.content[:300]}" for j, it in enumerate(batch))
            prompt = (
                f"分析以下 {len(batch)} 条新闻，输出 JSON 数组（与输入同序等长）：\n"
                '[{"symbols": [], "event_type": "", "sentiment": 0.0, "summary": "", '
                '"scope": "market", "urgency": "normal", "confidence": 0.5}]\n\n'
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
                self._apply_result(item, result)
        return items

    @staticmethod
    def _from_fetched(value: FetchedArticle) -> NewsItem:
        return NewsItem(
            source=value.source, title=value.title, content=value.content, url=value.url,
            published_at=value.published_at, is_official=value.is_official,
            raw_cache_key=value.raw_cache_key, analysis_status="pending",
        )

    def _fetch_source(self, source: dict, limit: int | None = None,
                      *, preview: bool = False) -> list[NewsItem]:
        if source["kind"] == "builtin":
            fetcher = SOURCES.get(source["id"])
            if fetcher is None:
                raise ValueError("内置来源采集器不存在")
            items = fetcher(limit=min(limit or source["item_limit"], source["item_limit"]))
            for item in items:
                item.is_official = bool(source["is_official"])
                item.analysis_status = "pending"
            return items
        value = dict(source)
        if limit is not None:
            value["item_limit"] = min(limit, int(value["item_limit"]))
        return [self._from_fetched(item) for item in fetch_declarative_source(
            value, self.source_store, preview=preview)]

    def preview(self, value: dict, token: str = "") -> list[dict]:
        """测试尚未保存的声明式来源；Token 只存在于本次调用内存。"""
        temporary = dict(value)
        temporary.setdefault("id", f"preview_{uuid.uuid4().hex[:8]}")
        temporary.setdefault("built_in", False)
        temporary.setdefault("secret_state", "none")
        temporary["item_limit"] = min(int(temporary.get("item_limit", 3)), 3)

        class _PreviewStore:
            def __init__(self, base: NewsSourceStore, secret: str):
                self.base, self.secret = base, secret

            def token(self, source):
                if source.get("auth_type") == "none":
                    return ""
                if not self.secret:
                    raise ValueError("测试鉴权来源时需要填写 API Token")
                return self.secret

            def cache_headers(self, source_id, url):
                return {}

            def save_response(self, *args, **kwargs):
                return ""

            def touch_not_modified(self, *args, **kwargs):
                return None

        articles = fetch_declarative_source(temporary, _PreviewStore(self.source_store, token), preview=True)
        return [
            {"title": item.title, "content": item.content[:500], "url": item.url,
             "published_at": item.published_at}
            for item in articles
        ]

    def enrich_pending(self, limit: int | None = None, ids: list[int] | None = None) -> dict:
        cfg = get_config().news
        rows = self.store.pending(limit=limit or cfg.annotation_items_per_run, ids=ids)
        if not rows:
            return {"processed": 0, "completed": 0, "failed": 0, "completed_ids": []}
        batch_size = cfg.annotation_batch_size
        completed = 0
        completed_ids: list[int] = []
        for index in range(0, len(rows), batch_size):
            chunk = rows[index:index + batch_size]
            items = [NewsItem(
                source=row["source_id"], title=row["title"], content=row["content"],
                url=row["url"], published_at=row["published_at"],
                is_official=row["is_official"], db_id=row["id"],
            ) for row in chunk]
            numbered = "\n".join(f"{offset + 1}. {item.content[:500]}" for offset, item in enumerate(items))
            prompt = (
                f"分析以下 {len(items)} 条新闻，输出 JSON 数组（与输入同序等长）：\n"
                '[{"symbols": [], "event_type": "其他", "sentiment": 0.0, "summary": "", '
                '"scope": "market", "urgency": "normal", "confidence": 0.5}]\n\n'
                + numbered
            )
            try:
                parsed = self.client.chat_json(prompt, system=EXTRACT_SYSTEM)
                if not isinstance(parsed, list) or len(parsed) != len(items):
                    raise ValueError("LLM 标注结果数量与输入不一致")
                if any(not isinstance(result, dict) for result in parsed):
                    raise ValueError("LLM 标注结果包含非对象元素")
                from quantmaster.automation.news import importance_score

                for item, result in zip(items, parsed, strict=True):
                    self._apply_result(item, result)
                    item.importance_score, item.scope, _ = importance_score(item, set(), set())
                    self.store.update_analysis(int(item.db_id), item)
                    completed += 1
                    completed_ids.append(int(item.db_id))
            except Exception as exc:
                self.store.analysis_failure([int(item.db_id) for item in items], str(exc))
        return {
            "processed": len(rows), "completed": completed,
            "failed": len(rows) - completed, "completed_ids": completed_ids,
        }

    def reanalyze(self, ids: list[int] | None = None, limit: int | None = None) -> dict:
        reset = self.store.reset_analysis(ids)
        return {"reset": reset, **self.enrich_pending(limit=limit, ids=ids)}

    def run(self, sources: list[str] | None = None, limit: int = 30,
            skip_llm: bool = False, group: str | None = None) -> dict:
        before_id = self.store.max_id()
        configs: list[dict] = []
        if sources:
            for source_id in sources:
                config = self.source_store.get(source_id)
                if config is None and source_id in SOURCES:
                    config = {
                        "id": source_id, "kind": "builtin", "item_limit": limit,
                        "is_official": source_id in {"csrc", "sse", "szse"},
                    }
                if config is not None:
                    configs.append(config)
            missing = sorted(set(sources) - {item["id"] for item in configs})
            if missing:
                raise ValueError(f"资讯来源不存在：{', '.join(missing)}")
        else:
            configs = self.source_store.list(enabled=True, group_name=group)
        fetched_count = saved_count = 0
        errors: dict[str, str] = {}
        source_results: list[dict] = []
        for source in configs:
            source_id = source["id"]
            run_id = self.source_store.start_run(source_id) if self.source_store.get(source_id) else ""
            try:
                items = self._fetch_source(source, limit)
                saved = self.store.save(items)
                fetched_count += len(items)
                saved_count += saved
                if run_id:
                    self.source_store.finish_run(run_id, fetched=len(items), saved=saved, pending=saved)
                source_results.append({"source": source_id, "fetched": len(items), "saved": saved})
            except Exception as exc:
                errors[source_id] = str(exc)
                if run_id:
                    self.source_store.finish_run(run_id, error=str(exc))
        annotation = {"processed": 0, "completed": 0, "failed": 0, "completed_ids": []}
        llm_cfg = get_config().llm
        can_annotate = self._client is not None or bool(llm_cfg.api_key or llm_cfg.base_url)
        if (not skip_llm and get_config().news.annotation_enabled and can_annotate):
            annotation = self.enrich_pending()
        new_ids = [int(item["id"]) for item in self.store.after_id(before_id)]
        return {
            "fetched": fetched_count,
            "saved": saved_count,
            "pending": max(0, saved_count - annotation["completed"]),
            "annotation": annotation, "new_ids": new_ids,
            "sources": source_results, "errors": errors,
        }
