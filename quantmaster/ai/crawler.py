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
import math
import re
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx

from quantmaster.ai.llm import LLMClient, LLMError
from quantmaster.ai.news_claims import ClaimMode, NewsClaimStore, normalize_news_ids
from quantmaster.ai.news_sources import (
    FetchedArticle,
    NewsSourceStore,
    fetch_declarative_source,
)
from quantmaster.config import get_config
from quantmaster.runtime.jobs import WorkerIdentity
from quantmaster.runtime.sqlite import connect_sqlite

USER_AGENT = "Mozilla/5.0 (compatible; QuantMaster/0.1; +https://github.com/ZacharyHu0/QuantMaster)"

# 与行情、因子模块使用的申万 2021 一级行业口径保持一致。模型输出只接受该白名单，
# 避免“新能源 / AI / 大消费”等主题概念与一级行业混在同一评分维度。
SECTOR_NAMES = (
    "农林牧渔", "基础化工", "钢铁", "有色金属", "电子", "家用电器", "食品饮料",
    "纺织服饰", "轻工制造", "医药生物", "公用事业", "交通运输", "房地产",
    "商贸零售", "社会服务", "综合", "建筑材料", "建筑装饰", "电力设备",
    "国防军工", "计算机", "传媒", "通信", "银行", "非银金融", "汽车",
    "机械设备", "煤炭", "石油石化", "环保", "美容护理",
)
SECTOR_NAME_SET = frozenset(SECTOR_NAMES)
SECTOR_ALIASES = {
    "化工": "基础化工", "商业贸易": "商贸零售", "休闲服务": "社会服务",
    "电气设备": "电力设备", "纺织服装": "纺织服饰",
}


def _id_chunks(values: list[int], size: int = 400) -> Iterator[list[int]]:
    """Keep bulk queue operations below conservative SQLite parameter limits."""
    for index in range(0, len(values), size):
        yield values[index:index + size]


@dataclass
class NewsItem:
    source: str
    title: str
    content: str
    url: str = ""
    published_at: str = ""
    # LLM 结构化结果
    symbols: list[str] = field(default_factory=list)
    sectors: list[str] = field(default_factory=list)  # 申万 2021 一级行业，最多 5 个
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
- sectors: 直接受影响的申万2021一级行业数组，最多5个，无法确定则空数组；只可从以下名称选择：
  农林牧渔|基础化工|钢铁|有色金属|电子|家用电器|食品饮料|纺织服饰|轻工制造|医药生物|公用事业|交通运输|房地产|商贸零售|社会服务|综合|建筑材料|建筑装饰|电力设备|国防军工|计算机|传媒|通信|银行|非银金融|汽车|机械设备|煤炭|石油石化|环保|美容护理
- event_type: 政策|业绩|并购|行业|宏观|其他
- sentiment: -1到1的数值，对相关股票（无个股则对A股整体）的利空/利好程度
- summary: 不超过40字的摘要
- scope: holding|watchlist|market
- urgency: critical|high|normal
- confidence: 0到1"""


def _normalize_sectors(values: list[Any]) -> list[str]:
    normalized: list[str] = []
    for raw_value in values:
        value = SECTOR_ALIASES.get(str(raw_value).strip(), str(raw_value).strip())
        if value in SECTOR_NAME_SET and value not in normalized:
            normalized.append(value)
    return normalized[:5]


def _sentiment_snapshot(values: list[tuple[float, float]]) -> dict[str, Any]:
    total_weight = sum(weight for _score, weight in values)
    if total_weight <= 0:
        return {"value": 0.0, "score": 0.0, "label": "暂无数据", "event_count": 0}
    value = sum(score * weight for score, weight in values) / total_weight
    label = (
        "明显偏多" if value >= 0.35 else "偏多" if value >= 0.1
        else "明显偏空" if value <= -0.35 else "偏空" if value <= -0.1
        else "中性"
    )
    return {
        "value": round(value, 4), "score": round(value * 100, 2),
        "label": label, "event_count": len(values),
    }


def _adaptive_sentiment_scale(values: list[float], default: int = 20) -> int:
    finite = [abs(float(value)) for value in values if math.isfinite(float(value))]
    if not finite:
        return default
    padded = max(finite) * 1.15
    return next((bucket for bucket in (10, 20, 40, 60, 80, 100) if bucket >= padded), 100)


def _safe_analysis_error(exc: Exception) -> str:
    message = str(exc).strip() or "标注服务未返回结果"
    message = re.sub(
        r"(?i)((?:api[_-]?key|token|authorization)\s*[=:]\s*)[^\s,;]+",
        r"\1***", message,
    )
    message = re.sub(r"(?i)(bearer\s+)[^\s,;]+", r"\1***", message)
    message = re.sub(r"\bsk-[A-Za-z0-9_-]{8,}\b", "sk-***", message)
    return message[:497] + "…" if len(message) > 500 else message


def _analysis_failure_code(exc: Exception) -> str:
    if isinstance(exc, LLMError):
        return str(exc.code or "llm_error")[:80]
    value = str(exc).casefold()
    if "rate limit" in value or "too many requests" in value or "限流" in value:
        return "rate_limit"
    if "timeout" in value or "timed out" in value or "超时" in value:
        return "timeout"
    if "auth" in value or "token" in value or "unauthorized" in value or "鉴权" in value:
        return "authentication"
    if "数量与输入不一致" in str(exc):
        return "invalid_batch_shape"
    return type(exc).__name__.removesuffix("Error").casefold()[:80] or "unknown"


class NewsStore:
    """长期结构化资讯库；原始响应由 :class:`NewsSourceStore` 分层缓存。"""

    ANALYSIS_VERSION = 2

    def __init__(self, path: Path | None = None):
        self.path = path or get_config().data_root / "news.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.sources = NewsSourceStore(self.path)
        from quantmaster.data.industry import load_cached_industry_map

        self._industry_map = load_cached_industry_map()
        self._migrate()
        self.claims = NewsClaimStore(self.path)

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(self.path, timeout=5.0, row_factory=True)

    @staticmethod
    def fingerprint(item: NewsItem) -> str:
        title = re.sub(r"\W+", "", item.title.casefold())
        identity = f"{item.url.strip().lower()}|{title}|{item.published_at.strip()}"
        return hashlib.sha256(f"{item.source}|{identity}".encode()).hexdigest()

    @staticmethod
    def content_hash(item: NewsItem) -> str:
        text = re.sub(r"\s+", "", (item.content or item.title).casefold())
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def _migrate(self) -> None:
        with self._conn() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS news ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,source TEXT,title TEXT,content TEXT,"
                "url TEXT,published_at TEXT,symbols TEXT,sectors TEXT,event_type TEXT,"
                "sentiment REAL,summary TEXT,"
                "created_at REAL,importance_score REAL DEFAULT 0,scope TEXT DEFAULT '',"
                "urgency TEXT DEFAULT '',confidence REAL DEFAULT 0,fingerprint TEXT DEFAULT '',"
                "is_official INTEGER DEFAULT 0,source_id TEXT DEFAULT '',"
                "content_hash TEXT DEFAULT '',first_seen_at REAL DEFAULT 0,last_seen_at REAL DEFAULT 0,"
                "raw_cache_key TEXT DEFAULT '',analysis_status TEXT DEFAULT 'pending',"
                "analysis_attempts INTEGER DEFAULT 0,analysis_error TEXT DEFAULT '',"
                "analysis_version INTEGER DEFAULT 1,next_retry_at REAL DEFAULT 0,"
                "parser_version TEXT DEFAULT '1',analysis_recovery_count INTEGER DEFAULT 0,"
                "last_failure_code TEXT DEFAULT '',analysis_updated_at REAL DEFAULT 0,"
                "UNIQUE(source,title,published_at))"
            )
            columns = {row[1] for row in conn.execute("PRAGMA table_info(news)")}
            additions = {
                "importance_score": "REAL DEFAULT 0", "scope": "TEXT DEFAULT ''",
                "urgency": "TEXT DEFAULT ''", "confidence": "REAL DEFAULT 0",
                "sectors": "TEXT DEFAULT '[]'",
                "fingerprint": "TEXT DEFAULT ''", "is_official": "INTEGER DEFAULT 0",
                "source_id": "TEXT DEFAULT ''", "content_hash": "TEXT DEFAULT ''",
                "first_seen_at": "REAL DEFAULT 0", "last_seen_at": "REAL DEFAULT 0",
                "raw_cache_key": "TEXT DEFAULT ''", "analysis_status": "TEXT DEFAULT 'pending'",
                "analysis_attempts": "INTEGER DEFAULT 0", "analysis_error": "TEXT DEFAULT ''",
                "analysis_version": "INTEGER DEFAULT 1", "next_retry_at": "REAL DEFAULT 0",
                "parser_version": "TEXT DEFAULT '1'",
                "analysis_recovery_count": "INTEGER DEFAULT 0",
                "last_failure_code": "TEXT DEFAULT ''",
                "analysis_updated_at": "REAL DEFAULT 0",
            }
            for name, sql_type in additions.items():
                if name not in columns:
                    conn.execute(f"ALTER TABLE news ADD COLUMN {name} {sql_type}")
            conn.execute("UPDATE news SET source_id=source WHERE source_id='' OR source_id IS NULL")
            conn.execute("UPDATE news SET first_seen_at=created_at WHERE first_seen_at=0")
            conn.execute("UPDATE news SET last_seen_at=created_at WHERE last_seen_at=0")
            conn.execute(
                "UPDATE news SET analysis_status='dead_letter' "
                "WHERE analysis_status='failed' AND analysis_attempts>=3"
            )
            conn.execute(
                "UPDATE news SET analysis_updated_at=last_seen_at "
                "WHERE analysis_status='complete' AND analysis_updated_at=0"
            )
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
                content_hash = self.content_hash(item)
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
                CREATE INDEX IF NOT EXISTS idx_news_stats
                    ON news(analysis_status,first_seen_at,confidence);
            """)
            NewsClaimStore.migrate(conn)

    def _decode(self, row: sqlite3.Row) -> dict:
        value = dict(row)
        for field_name in ("symbols", "sectors"):
            try:
                decoded = json.loads(value.get(field_name) or "[]")
                value[field_name] = decoded if isinstance(decoded, list) else []
            except (TypeError, json.JSONDecodeError):
                value[field_name] = []
        inferred = [self._industry_map.get(str(symbol), "") for symbol in value["symbols"]]
        value["sectors"] = _normalize_sectors([*value["sectors"], *inferred])
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
                item.sectors = _normalize_sectors(item.sectors)
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
                            ",symbols=?,sectors=?,event_type=?,sentiment=?,summary=?,importance_score=?,"
                            "scope=?,urgency=?,confidence=?,analysis_status='complete',analysis_error=''"
                        )
                        analysis_params = [
                            json.dumps(item.symbols, ensure_ascii=False),
                            json.dumps(item.sectors, ensure_ascii=False), item.event_type,
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
                        "(source,title,content,url,published_at,symbols,sectors,event_type,sentiment,summary,"
                        "created_at,importance_score,scope,urgency,confidence,fingerprint,is_official,"
                        "source_id,content_hash,first_seen_at,last_seen_at,raw_cache_key,analysis_status,"
                        "analysis_version,parser_version) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (item.source, item.title, item.content, item.url, item.published_at,
                         json.dumps(item.symbols, ensure_ascii=False),
                         json.dumps(item.sectors, ensure_ascii=False), item.event_type, item.sentiment,
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
        where = (
            "analysis_status IN ('pending','failed','recovery') "
            "AND analysis_attempts<3 AND next_retry_at<=?"
        )
        params: list[Any] = [time.time()]
        selected_limit = max(1, int(limit))
        if ids is not None:
            selected = list(dict.fromkeys(int(value) for value in ids))
            if not selected:
                return []
            rows: list[sqlite3.Row] = []
            with self._conn() as conn:
                for chunk in _id_chunks(selected):
                    placeholders = ",".join("?" for _ in chunk)
                    rows.extend(conn.execute(
                        f"SELECT * FROM news WHERE {where} "
                        f"AND id IN ({placeholders}) ORDER BY id",
                        [*params, *chunk],
                    ).fetchall())
            rows.sort(key=lambda row: int(row["id"]))
            return [self._decode(row) for row in rows[:selected_limit]]
        params.append(selected_limit)
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT * FROM news WHERE {where} ORDER BY id LIMIT ?", params
            ).fetchall()
        return [self._decode(row) for row in rows]

    def update_analysis(
        self,
        item_id: int,
        item: NewsItem,
        *,
        claim_token: str = "",
        claim_owner: str = "",
    ) -> bool:
        item.sectors = _normalize_sectors(item.sectors)
        with self._conn() as conn:
            return self._update_analysis(
                conn, item_id, item, claim_token=claim_token, claim_owner=claim_owner,
            )

    def _update_analysis(
        self,
        conn: sqlite3.Connection,
        item_id: int,
        item: NewsItem,
        *,
        claim_token: str = "",
        claim_owner: str = "",
    ) -> bool:
        if claim_token and not self.claims.owns(
            item_id, claim_token, claim_owner, connection=conn,
        ):
            return False
        changed = conn.execute(
            "UPDATE news SET symbols=?,sectors=?,event_type=?,sentiment=?,summary=?,importance_score=?,"
            "scope=?,urgency=?,confidence=?,analysis_status='complete',analysis_error='',"
            "analysis_version=?,next_retry_at=0,last_failure_code='',analysis_updated_at=? "
            "WHERE id=?",
            (json.dumps(item.symbols, ensure_ascii=False),
             json.dumps(item.sectors, ensure_ascii=False), item.event_type,
             item.sentiment, item.summary, item.importance_score, item.scope, item.urgency,
             item.confidence, self.ANALYSIS_VERSION, time.time(), item_id),
        ).rowcount
        return bool(changed)

    def update_analyses(
        self,
        items: list[tuple[int, NewsItem]],
        *,
        claim_token: str,
        claim_owner: str,
    ) -> list[int]:
        """Fence and persist one provider batch in a single SQLite transaction."""
        written: list[int] = []
        with self._conn() as connection:
            for item_id, item in items:
                item.sectors = _normalize_sectors(item.sectors)
                if self._update_analysis(
                    connection,
                    item_id,
                    item,
                    claim_token=claim_token,
                    claim_owner=claim_owner,
                ):
                    written.append(item_id)
        return written

    def analysis_failure(
        self, item_ids: list[int], error: str, failure_code: str = "unknown", *,
        retryable: bool = True, retry_after: float | None = None,
        claim_token: str = "", claim_owner: str = "",
    ) -> dict[str, Any]:
        outcome: dict[str, Any] = {
            "failed": 0, "retry_scheduled": 0, "dead_letter": 0,
            "next_retry_at": 0.0,
        }
        if not item_ids:
            return outcome
        provider_delay = 0.0
        try:
            candidate = float(retry_after or 0.0)
            if math.isfinite(candidate):
                provider_delay = min(max(0.0, candidate), 7 * 86400.0)
        except (TypeError, ValueError, OverflowError):
            provider_delay = 0.0
        with self._conn() as conn:
            for item_id in item_ids:
                if claim_token and not self.claims.owns(
                    item_id, claim_token, claim_owner, connection=conn,
                ):
                    continue
                row = conn.execute(
                    "SELECT analysis_attempts,analysis_status,analysis_recovery_count "
                    "FROM news WHERE id=?", (item_id,)
                ).fetchone()
                if row is None or row["analysis_status"] == "complete":
                    continue
                attempts = int(row[0] if row else 0) + 1
                recovering = bool(row and row["analysis_status"] == "recovery")
                recovery_count = int(row["analysis_recovery_count"] if row else 0)
                dead_letter = not retryable or recovering or attempts >= 3
                base_delay = (
                    (86400, 3 * 86400, 7 * 86400)[min(max(recovery_count, 1) - 1, 2)]
                    if dead_letter else (60, 300, 1800)[min(attempts - 1, 2)]
                )
                delay = max(float(base_delay), provider_delay if not dead_letter else 0.0)
                next_retry_at = time.time() + delay
                conn.execute(
                    "UPDATE news SET analysis_status=?,analysis_attempts=?,analysis_error=?,"
                    "last_failure_code=?,next_retry_at=?,analysis_updated_at=? "
                    "WHERE id=? AND analysis_status<>'complete'",
                    (
                        "dead_letter" if dead_letter else "failed",
                        attempts, error[:1000], failure_code[:80], next_retry_at,
                        time.time(), item_id,
                    ),
                )
                outcome["failed"] += 1
                outcome["dead_letter" if dead_letter else "retry_scheduled"] += 1
                outcome["next_retry_at"] = max(outcome["next_retry_at"], next_retry_at)
        return outcome

    def prepare_dead_letter_recovery(
        self, limit: int | None = 20, ids: list[int] | None = None,
    ) -> list[int]:
        clauses = [
            "analysis_status='dead_letter'",
            "analysis_recovery_count<3",
            "next_retry_at<=?",
        ]
        params: list[Any] = [time.time()]
        if ids is not None:
            if not ids:
                return []
            placeholders = ",".join("?" for _ in ids)
            clauses.append(f"id IN ({placeholders})")
            params.extend(int(value) for value in ids)
        limit_sql = ""
        if limit is not None:
            limit_sql = " LIMIT ?"
            params.append(max(1, int(limit)))
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                f"SELECT id FROM news WHERE {' AND '.join(clauses)} "
                f"ORDER BY next_retry_at,id{limit_sql}",
                params,
            ).fetchall()
            selected = [int(row["id"]) for row in rows]
            for chunk in _id_chunks(selected):
                placeholders = ",".join("?" for _ in chunk)
                conn.execute(
                    "UPDATE news SET analysis_status='recovery',analysis_attempts=0,"
                    "analysis_recovery_count=analysis_recovery_count+1,next_retry_at=0 "
                    f"WHERE id IN ({placeholders})",
                    chunk,
                )
        return selected

    def prepare_failed_retry(
        self, limit: int | None = 100, ids: list[int] | None = None,
    ) -> list[int]:
        """将用户显式选择的失败标注立即放回待处理队列。"""
        clauses = ["analysis_status='failed'"]
        params: list[Any] = []
        if ids is not None:
            if not ids:
                return []
            placeholders = ",".join("?" for _ in ids)
            clauses.append(f"id IN ({placeholders})")
            params.extend(int(value) for value in ids)
        limit_sql = ""
        if limit is not None:
            limit_sql = " LIMIT ?"
            params.append(max(1, int(limit)))
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                f"SELECT id FROM news WHERE {' AND '.join(clauses)} "
                f"ORDER BY analysis_updated_at,id{limit_sql}",
                params,
            ).fetchall()
            selected = [int(row["id"]) for row in rows]
            for chunk in _id_chunks(selected):
                placeholders = ",".join("?" for _ in chunk)
                conn.execute(
                    "UPDATE news SET analysis_status='pending',analysis_attempts=0,"
                    "analysis_error='',next_retry_at=0,last_failure_code='' "
                    f"WHERE id IN ({placeholders}) AND analysis_status='failed'",
                    chunk,
                )
        return selected

    def llm_recently_healthy(self, hours: int = 24) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT MAX(analysis_updated_at) AS latest FROM news "
                "WHERE analysis_status='complete'"
            ).fetchone()
        latest = float(row["latest"] or 0) if row else 0.0
        return latest == 0 or latest >= time.time() - max(1, int(hours)) * 3600

    def reset_analysis(self, ids: list[int] | None = None) -> int:
        where, params = "analysis_status<>'pending'", []
        normalized = normalize_news_ids(ids)
        if normalized is not None:
            if not normalized:
                return 0
            changed = 0
            with self._conn() as conn:
                for chunk in _id_chunks(normalized):
                    placeholders = ",".join("?" for _ in chunk)
                    changed += conn.execute(
                        "UPDATE news SET analysis_status='pending',analysis_attempts=0,"
                        "analysis_error='',next_retry_at=0,analysis_recovery_count=0,"
                        f"last_failure_code='' WHERE id IN ({placeholders})",
                        chunk,
                    ).rowcount
            return changed
        with self._conn() as conn:
            cursor = conn.execute(
                f"UPDATE news SET analysis_status='pending',analysis_attempts=0,analysis_error='',"
                f"next_retry_at=0,analysis_recovery_count=0,last_failure_code='' WHERE {where}", params,
            )
        return cursor.rowcount

    def claimable_count(
        self,
        *,
        mode: ClaimMode = "pending",
        ids: list[int] | None = None,
        max_id: int | None = None,
        manual: bool = False,
        now: float | None = None,
    ) -> dict[str, int]:
        """Count a fixed queue window without acquiring or mutating any work."""
        current = time.time() if now is None else float(now)
        normalized = normalize_news_ids(ids)
        eligible, params = NewsClaimStore._eligible(mode, manual=manual)
        params = [current if isinstance(value, float) else value for value in params]
        id_sql, id_params = NewsClaimStore._id_predicate(normalized)
        max_sql = " AND n.id<=?" if max_id is not None else ""
        if max_id is not None:
            id_params.append(int(max_id))
        with self._conn() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS total,"
                "SUM(c.news_id IS NOT NULL AND c.lease_expires_at>?) AS in_progress "
                "FROM news n LEFT JOIN news_analysis_claims c ON c.news_id=n.id "
                f"WHERE {eligible}{id_sql}{max_sql}",
                [current, *params, *id_params],
            ).fetchone()
        total = int(row["total"] or 0)
        in_progress = int(row["in_progress"] or 0)
        return {
            "total": total,
            "claimable": max(0, total - in_progress),
            "in_progress": in_progress,
        }

    def rows_by_ids(self, ids: list[int]) -> list[dict]:
        normalized = normalize_news_ids(ids) or []
        rows: list[sqlite3.Row] = []
        with self._conn() as connection:
            for chunk in _id_chunks(normalized):
                placeholders = ",".join("?" for _ in chunk)
                rows.extend(connection.execute(
                    f"SELECT * FROM news WHERE id IN ({placeholders}) ORDER BY id", chunk,
                ).fetchall())
        rows.sort(key=lambda row: int(row["id"]))
        return [self._decode(row) for row in rows]

    def query(self, *, limit: int = 50, cursor: int | None = None, q: str = "",
              source: str = "", group_name: str = "", event_type: str = "",
              sentiment: str = "", scope: str = "", symbol: str = "",
              status: str = "", date_from: float | None = None,
              date_to: float | None = None, sort: str = "recent") -> dict:
        clauses: list[str] = []
        params: list[object] = []
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
        items = []
        for row in rows[:page_size]:
            item = self._decode(row)
            content = str(item.get("content") or "")
            item["content_truncated"] = len(content) > 2000
            if item["content_truncated"]:
                item["content"] = content[:2000]
            items.append(item)
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
        now = time.time()
        cutoff = now - max(1, min(days, 3650)) * 86400
        news_config = get_config().news
        minimum = news_config.factor_min_confidence
        halflife_days = max(0.01, float(news_config.factor_halflife_days))
        with self._conn() as conn:
            counts = conn.execute(
                "SELECT COUNT(*) AS total,"
                "SUM(analysis_status='complete') AS annotated,"
                "SUM(analysis_status='pending') AS pending,"
                "SUM(analysis_status='failed') AS failed,"
                "SUM(analysis_status='dead_letter') AS dead_letter,"
                "SUM(importance_score>=80) AS important,"
                "SUM(sentiment>0.15 AND analysis_status='complete') AS positive,"
                "SUM(sentiment<-0.15 AND analysis_status='complete') AS negative "
                "FROM news WHERE first_seen_at>=?", (cutoff,),
            ).fetchone()
            queue_counts = conn.execute(
                "SELECT SUM(analysis_status='pending') AS pending,"
                "SUM(analysis_status='failed') AS failed,"
                "SUM(analysis_status='recovery') AS recovery,"
                "SUM(analysis_status='dead_letter') AS dead_letter,"
                "SUM(analysis_status='dead_letter' AND analysis_recovery_count<3 "
                "AND next_retry_at<=?) AS recoverable_dead_letter FROM news",
                (now,),
            ).fetchone()
            rows = conn.execute(
                "WITH base AS ("
                "SELECT n.id,n.content_hash,n.first_seen_at,n.sentiment,n.confidence,"
                "n.importance_score,n.symbols,n.sectors,"
                "COALESCE(s.factor_weight,1) * n.confidence * n.importance_score / 100.0 "
                "AS quality_weight FROM news n LEFT JOIN news_sources s ON s.id=n.source_id "
                "WHERE n.first_seen_at>=? AND n.analysis_status='complete' AND n.confidence>=?"
                "), ranked AS ("
                "SELECT *,ROW_NUMBER() OVER (PARTITION BY "
                "COALESCE(NULLIF(content_hash,''),'id:' || id) "
                "ORDER BY quality_weight DESC,id) AS rank FROM base WHERE quality_weight>0"
                ") SELECT * FROM ranked WHERE rank=1 ORDER BY first_seen_at,id",
                (cutoff, minimum),
            ).fetchall()
        daily: dict[str, list[tuple[float, float]]] = {}
        market_values: list[tuple[float, float]] = []
        sector_values: dict[str, list[tuple[float, float]]] = {}
        sector_counts: dict[str, dict[str, int]] = {}
        symbol_counts: dict[str, int] = {}
        for row in rows:
            quality_weight = float(row["quality_weight"] or 0)
            sentiment = max(-1.0, min(1.0, float(row["sentiment"] or 0)))
            day = datetime.fromtimestamp(
                float(row["first_seen_at"]), tz=timezone.utc,
            ).astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
            daily.setdefault(day, []).append((sentiment, quality_weight))
            age_days = max(0.0, (now - float(row["first_seen_at"] or now)) / 86400)
            current_weight = quality_weight * 0.5 ** (age_days / halflife_days)
            market_values.append((sentiment, current_weight))
            try:
                symbols = json.loads(row["symbols"] or "[]")
            except (TypeError, json.JSONDecodeError):
                symbols = []
            try:
                stored_sectors = json.loads(row["sectors"] or "[]")
            except (TypeError, json.JSONDecodeError):
                stored_sectors = []
            sectors = _normalize_sectors([
                *(stored_sectors if isinstance(stored_sectors, list) else []),
                *(self._industry_map.get(str(symbol), "") for symbol in symbols),
            ])
            for sector in sectors:
                sector_values.setdefault(sector, []).append((sentiment, current_weight))
                counts_for_sector = sector_counts.setdefault(
                    sector, {"event_count": 0, "positive": 0, "negative": 0},
                )
                counts_for_sector["event_count"] += 1
                if sentiment > 0.15:
                    counts_for_sector["positive"] += 1
                elif sentiment < -0.15:
                    counts_for_sector["negative"] += 1
            for symbol in symbols:
                symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1
        series: list[tuple[str, float]] = [
            (day, round(sum(score * weight for score, weight in values) /
                        sum(weight for _score, weight in values), 4))
            for day, values in sorted(daily.items())
        ]
        data: dict[str, Any] = dict(counts) if counts else {}
        data["queue"] = {
            key: int((queue_counts[key] if queue_counts else 0) or 0)
            for key in (
                "pending", "failed", "recovery", "dead_letter",
                "recoverable_dead_letter",
            )
        }
        claims = self.claims.stats(now=now)
        data["queue"]["claims"] = claims
        data["queue"]["processing"] = int(data["queue"]["recovery"]) + claims["active"]
        data["coverage"] = round(int(data.get("annotated") or 0) / max(1, int(data.get("total") or 0)), 4)
        data["sentiment_series"] = series
        data["halflife_days"] = halflife_days
        data["market_sentiment"] = _sentiment_snapshot(market_values)
        data["sector_scores"] = sorted((
            {
                "sector": sector,
                **_sentiment_snapshot(values),
                **sector_counts[sector],
            }
            for sector, values in sector_values.items()
        ), key=lambda item: (-abs(float(item["score"])), str(item["sector"])))
        market_scale_values = [
            float(data["market_sentiment"].get("score") or 0),
            *(float(item[1]) * 100 for item in series),
        ] if data["market_sentiment"].get("event_count") else []
        sector_scale_values = [float(item.get("score") or 0) for item in data["sector_scores"]]
        data["display_scale"] = {
            "mode": "adaptive_bucket_v1",
            "theoretical_abs_max": 100,
            "market_abs_max": _adaptive_sentiment_scale(market_scale_values),
            "sector_abs_max": _adaptive_sentiment_scale(sector_scale_values),
        }
        top_symbols = sorted(
            symbol_counts.items(), key=lambda item: (-item[1], item[0]),
        )[:24]
        from quantmaster.data import load_stock_names

        symbol_names = load_stock_names([symbol for symbol, _count in top_symbols])
        data["top_symbols"] = [
            {
                "symbol": symbol,
                "name": symbol_names.get(symbol, ""),
                "count": count,
            }
            for symbol, count in top_symbols
        ]
        return data


@contextmanager
def _claim_heartbeat(
    claims: NewsClaimStore,
    token: str,
    owner: str,
    *,
    lease_seconds: float = 90.0,
) -> Iterator[threading.Event]:
    """Renew a claim while a provider call blocks the worker thread."""
    stop = threading.Event()
    alive = threading.Event()
    alive.set()

    def renew() -> None:
        interval = max(1.0, min(30.0, lease_seconds / 3))
        while not stop.wait(interval):
            if not claims.heartbeat(token, owner, lease_seconds=lease_seconds):
                alive.clear()
                return

    thread = threading.Thread(target=renew, name="qm-news-claim-heartbeat", daemon=True)
    thread.start()
    try:
        yield alive
    finally:
        stop.set()
        thread.join(timeout=1.0)


class AICrawler:
    """先归档再标注；模型不可用时资讯仍会可靠进入待处理队列。"""

    def __init__(self, client: LLMClient | None = None, store: NewsStore | None = None,
                 source_store: NewsSourceStore | None = None):
        self._client = client
        self.store = store or NewsStore()
        self.source_store = source_store or self.store.sources
        self.identity = WorkerIdentity.create("news-analysis")

    @property
    def client(self) -> LLMClient:
        if self._client is None:
            self._client = LLMClient()
        return self._client

    @staticmethod
    def _apply_result(
        item: NewsItem, result: dict, industry_map: dict[str, str] | None = None,
    ) -> None:
        symbols = result.get("symbols", [])
        if not isinstance(symbols, list):
            symbols = []
        item.symbols = list(dict.fromkeys(
            value for symbol in symbols
            if re.fullmatch(r"\d{6}\.(?:SH|SZ|BJ)", value := str(symbol).strip().upper())
        ))[:30]
        sectors = result.get("sectors", [])
        if not isinstance(sectors, list):
            sectors = []
        mapping = industry_map or {}
        item.sectors = _normalize_sectors([
            *sectors, *(mapping.get(symbol, "") for symbol in item.symbols),
        ])
        event_type = str(result.get("event_type", "其他"))
        allowed_event_types = {"政策", "业绩", "并购", "行业", "宏观", "其他"}
        item.event_type = event_type if event_type in allowed_event_types else "其他"
        try:
            sentiment = float(result.get("sentiment", 0))
            item.sentiment = max(-1.0, min(1.0, sentiment)) if math.isfinite(sentiment) else 0.0
        except (TypeError, ValueError):
            item.sentiment = 0.0
        item.summary = str(result.get("summary", ""))[:240]
        scope = str(result.get("scope", "market"))
        urgency = str(result.get("urgency", "normal"))
        item.scope = scope if scope in {"holding", "watchlist", "market"} else "market"
        item.urgency = urgency if urgency in {"critical", "high", "normal"} else "normal"
        try:
            confidence = float(result.get("confidence", 0))
            item.confidence = max(0.0, min(1.0, confidence)) if math.isfinite(confidence) else 0.0
        except (TypeError, ValueError):
            item.confidence = 0.0
        item.analysis_status = "complete"

    def extract(self, items: list[NewsItem], batch_size: int = 10) -> list[NewsItem]:
        cfg = get_config().news
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            numbered = "\n".join(f"{j + 1}. {it.content[:300]}" for j, it in enumerate(batch))
            prompt = (
                f"分析以下 {len(batch)} 条新闻，输出 JSON 数组（与输入同序等长）：\n"
                '[{"symbols": [], "sectors": [], "event_type": "", "sentiment": 0.0, "summary": "", '
                '"scope": "market", "urgency": "normal", "confidence": 0.5}]\n\n'
                + numbered
            )
            try:
                parsed = self.client.chat_json(
                    prompt, system=EXTRACT_SYSTEM,
                    timeout=cfg.annotation_timeout,
                    reasoning_effort=cfg.annotation_reasoning_effort,
                )
            except Exception:
                continue
            if not isinstance(parsed, list):
                continue
            for item, result in zip(batch, parsed, strict=False):
                if not isinstance(result, dict):
                    continue
                self._apply_result(item, result, self.store._industry_map)
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

    @staticmethod
    def _annotation_prompt(items: list[NewsItem]) -> str:
        numbered = "\n".join(
            f"{offset + 1}. {item.content[:500]}" for offset, item in enumerate(items)
        )
        return (
            f"分析以下 {len(items)} 条新闻，输出 JSON 数组（与输入同序等长）：\n"
            '[{"symbols": [], "sectors": [], "event_type": "其他", "sentiment": 0.0, "summary": "", '
            '"scope": "market", "urgency": "normal", "confidence": 0.5}]\n\n'
            + numbered
        )

    @staticmethod
    def _stream_item(item: dict) -> dict:
        value = dict(item)
        content = str(value.get("content") or "")
        value["content_truncated"] = len(content) > 2000
        if value["content_truncated"]:
            value["content"] = content[:2000]
        return value

    def enrich_pending_events(
        self, limit: int | None = None, ids: list[int] | None = None,
        batch_size: int | None = None,
        *,
        mode: ClaimMode = "pending",
        manual: bool = False,
    ) -> Iterator[dict]:
        """Claim and process one fixed queue window, yielding durable progress."""
        cfg = get_config().news
        normalized_ids = normalize_news_ids(ids)
        size = max(1, min(int(batch_size or cfg.annotation_batch_size), 50))
        max_id = self.store.max_id()
        queue = self.store.claimable_count(
            mode=mode, ids=normalized_ids, max_id=max_id, manual=manual,
        )
        if limit is None:
            selected_limit = (
                queue["claimable"] if mode in {"failed", "dead_letter"}
                else cfg.annotation_items_per_run
            )
        else:
            selected_limit = max(1, min(int(limit), 1000))
        total = min(queue["claimable"], selected_limit)
        batch_count = math.ceil(total / size) if total else 0
        yield {
            "type": "start", "total": total, "processed": 0,
            "completed": 0, "failed": 0, "retry_scheduled": 0,
            "dead_letter": 0, "batch_count": batch_count, "claimed": 0,
            "in_progress": queue["in_progress"], "recovered_leases": 0,
        }
        completed = failed = processed = retry_scheduled = dead_letter = 0
        claimed = recovered_leases = 0
        completed_ids: list[int] = []
        failure_details: list[dict[str, Any]] = []
        remaining_ids = list(normalized_ids) if normalized_ids is not None else None
        batch_number = 0
        while processed < selected_limit:
            batch = self.store.claims.claim(
                owner=self.identity.value,
                task_type=f"news:{mode}",
                mode=mode,
                limit=min(size, selected_limit - processed),
                ids=remaining_ids,
                max_id=max_id,
                manual=manual,
            )
            recovered_leases += batch.recovered_leases
            if not batch.ids:
                break
            batch_number += 1
            claimed += len(batch.ids)
            if remaining_ids is not None:
                claimed_set = set(batch.ids)
                remaining_ids = [value for value in remaining_ids if value not in claimed_set]
            chunk = self.store.rows_by_ids(list(batch.ids))
            items = [NewsItem(
                source=row["source_id"], title=row["title"], content=row["content"],
                url=row["url"], published_at=row["published_at"],
                is_official=row["is_official"], db_id=row["id"],
            ) for row in chunk]
            chunk_completed = 0
            chunk_retry_scheduled = chunk_dead_letter = 0
            error = ""
            try:
                with _claim_heartbeat(
                    self.store.claims, batch.token, self.identity.value,
                ) as lease_alive:
                    parsed = self.client.chat_json(
                        self._annotation_prompt(items), system=EXTRACT_SYSTEM,
                        timeout=cfg.annotation_timeout,
                        reasoning_effort=cfg.annotation_reasoning_effort,
                    )
                    if not isinstance(parsed, list) or len(parsed) != len(items):
                        raise ValueError("LLM 标注结果数量与输入不一致")
                    if any(not isinstance(result, dict) for result in parsed):
                        raise ValueError("LLM 标注结果包含非对象元素")
                    if not lease_alive.is_set():
                        raise LLMError(
                            "资讯分析租约已转交其他 worker",
                            code="claim_lost", retryable=True,
                        )
                    from quantmaster.automation.news import importance_score

                    prepared: list[tuple[int, NewsItem]] = []
                    for item, result in zip(items, parsed, strict=True):
                        if item.db_id is None:
                            raise ValueError("待标注资讯缺少持久化 ID")
                        self._apply_result(item, result, self.store._industry_map)
                        item.importance_score, item.scope, _ = importance_score(
                            item, set(), set(),
                        )
                        prepared.append((item.db_id, item))
                    written_ids = self.store.update_analyses(
                        prepared,
                        claim_token=batch.token,
                        claim_owner=self.identity.value,
                    )
                    completed += len(written_ids)
                    chunk_completed += len(written_ids)
                    completed_ids.extend(written_ids)
            except Exception as exc:
                error = _safe_analysis_error(exc)
                failure_code = _analysis_failure_code(exc)
                retryable = exc.retryable if isinstance(exc, LLMError) else True
                outcome = self.store.analysis_failure(
                    [item.db_id for item in items if item.db_id is not None],
                    error,
                    failure_code,
                    retryable=retryable,
                    retry_after=exc.retry_after if isinstance(exc, LLMError) else None,
                    claim_token=batch.token,
                    claim_owner=self.identity.value,
                )
                chunk_retry_scheduled = int(outcome["retry_scheduled"])
                chunk_dead_letter = int(outcome["dead_letter"])
                retry_scheduled += chunk_retry_scheduled
                dead_letter += chunk_dead_letter
                failure_details.append({
                    "batch": batch_number,
                    "code": failure_code,
                    "message": error,
                    "retryable": bool(retryable),
                    "failed": int(outcome["failed"]),
                    "retry_scheduled": chunk_retry_scheduled,
                    "dead_letter": chunk_dead_letter,
                    "next_retry_at": float(outcome["next_retry_at"]),
                })
            finally:
                self.store.claims.release(batch.token, self.identity.value)
            chunk_failed = len(items) - chunk_completed
            failed += chunk_failed
            processed += len(items)
            updated_ids = [item.db_id for item in items if item.db_id is not None]
            updated_items = [
                self._stream_item(value)
                for value in self.store.rows_by_ids(updated_ids)
            ]
            yield {
                "type": "batch", "batch": batch_number,
                "batch_count": batch_count, "processed": processed, "total": total,
                "completed": completed, "failed": failed,
                "batch_completed": chunk_completed, "batch_failed": chunk_failed,
                "retry_scheduled": retry_scheduled, "dead_letter": dead_letter,
                "batch_retry_scheduled": chunk_retry_scheduled,
                "batch_dead_letter": chunk_dead_letter,
                "completed_ids": completed_ids[-chunk_completed:] if chunk_completed else [],
                "updated_items": updated_items, "error": error, "claimed": claimed,
                "in_progress": queue["in_progress"],
                "recovered_leases": recovered_leases,
            }
        result = {
            "processed": processed, "completed": completed,
            "failed": failed, "retry_scheduled": retry_scheduled,
            "dead_letter": dead_letter, "failure_details": failure_details,
            "completed_ids": completed_ids, "claimed": claimed,
            "in_progress": queue["in_progress"],
            "recovered_leases": recovered_leases,
        }
        yield {"type": "complete", **result}

    def enrich_pending(
        self, limit: int | None = None, ids: list[int] | None = None,
        batch_size: int | None = None,
        *,
        mode: ClaimMode = "pending",
        manual: bool = False,
    ) -> dict:
        result = {
            "processed": 0, "completed": 0, "failed": 0,
            "retry_scheduled": 0, "dead_letter": 0,
            "failure_details": [], "completed_ids": [],
        }
        for event in self.enrich_pending_events(
            limit=limit, ids=ids, batch_size=batch_size, mode=mode, manual=manual,
        ):
            if event["type"] == "complete":
                result = {key: value for key, value in event.items() if key != "type"}
        return result

    def reanalyze(self, ids: list[int] | None = None, limit: int | None = None) -> dict:
        reset = self.store.reset_analysis(ids)
        return {"reset": reset, **self.enrich_pending(limit=limit, ids=ids)}

    def retry_failed(
        self, ids: list[int] | None = None, limit: int | None = 100, batch_size: int = 5,
    ) -> dict:
        result = self.enrich_pending(
            ids=ids, limit=limit, batch_size=batch_size,
            mode="failed", manual=True,
        )
        return {"status": "ok", "selected": result["claimed"], **result}

    def recover_dead_letters(
        self, ids: list[int] | None = None, limit: int | None = 20, batch_size: int = 5,
        *,
        manual: bool = False,
    ) -> dict:
        if not manual and not self.store.llm_recently_healthy():
            return {
                "status": "skipped",
                "reason": "LLM 最近 24 小时没有成功标注",
                "selected": 0,
            }
        result = self.enrich_pending(
            ids=ids, limit=limit, batch_size=batch_size,
            mode="dead_letter", manual=manual,
        )
        return {"status": "ok", "selected": result["claimed"], **result}

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
        annotation: dict[str, Any] = {
            "processed": 0, "completed": 0, "failed": 0,
            "retry_scheduled": 0, "dead_letter": 0,
            "failure_details": [], "completed_ids": [],
        }
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
