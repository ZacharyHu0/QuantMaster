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
import logging
import math
import re
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quantmaster.ai.llm import LLMClient, LLMError
from quantmaster.ai.news_claims import (
    ClaimBatch,
    ClaimMode,
    NewsClaimStore,
    normalize_news_ids,
)
from quantmaster.ai.news_contracts import (
    FetchBatch,
    FetchedArticle,
    NewsProviderError,
)
from quantmaster.ai.news_pipeline_lock import NewsPipelineLock
from quantmaster.ai.news_providers import fetch_builtin_source
from quantmaster.ai.news_sources import (
    NewsSourceStore,
    fetch_declarative_source,
)
from quantmaster.ai.news_storage import (
    aggregate_news_event_focus,
    aggregate_news_stats,
    migrate_news_schema,
    news_content_hash,
    news_fingerprint,
    register_news_raw_verifier,
    replace_news_dimensions,
)
from quantmaster.config import get_config
from quantmaster.runtime.jobs import WorkerIdentity
from quantmaster.runtime.sqlite import connect_sqlite

logger = logging.getLogger(__name__)

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

_DASHBOARD_WINDOWS = (1, 3, 7, 30)
_DASHBOARD_ALGORITHM_VERSION = "QM_NEWS_DASHBOARD_V1"


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
    published_at_epoch: float = 0.0
    fetched_at: float = 0.0
    provider_item_id: str = ""
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
    content_scope: str = "unknown"
    parser_version: str = "1"
    evidence_binding_hash: str = ""
    ingest_window_id: str = ""
    ingest_batch_id: str = ""
    analysis_status: str = "pending"
    db_id: int | None = None


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
    return _sentiment_snapshot_from_totals(
        sum(score * weight for score, weight in values),
        total_weight,
        len(values),
    )


def _sentiment_snapshot_from_totals(
    weighted_score: float,
    total_weight: float,
    event_count: int,
) -> dict[str, Any]:
    if total_weight <= 0:
        return {"value": 0.0, "score": 0.0, "label": "暂无数据", "event_count": 0}
    value = weighted_score / total_weight
    label = (
        "明显偏多" if value >= 0.35 else "偏多" if value >= 0.1
        else "明显偏空" if value <= -0.35 else "偏空" if value <= -0.1
        else "中性"
    )
    return {
        "value": round(value, 4), "score": round(value * 100, 2),
        "label": label, "event_count": event_count,
    }


def _adaptive_sentiment_scale(values: list[float], default: int = 20) -> int:
    finite = [abs(float(value)) for value in values if math.isfinite(float(value))]
    if not finite:
        return default
    padded = max(finite) * 1.15
    return next((bucket for bucket in (10, 20, 40, 60, 80, 100) if bucket >= padded), 100)


_GENERIC_ANALYSIS_ERROR = "资讯分析失败，请查看本机日志"


def _analysis_error_message(code: str) -> str:
    normalized = str(code or "unknown").strip().casefold()
    if normalized in {"rate_limit", "http_429"}:
        return "模型服务拒绝了请求：调用过于频繁或额度不足。请稍后重试并检查服务额度。"
    if normalized == "queue_timeout":
        return "分析请求等待并发槽位超时。请稍后重试，或降低资讯处理并发。"
    if normalized in {"timeout", "read_timeout", "request_timeout"}:
        return "模型在设定时间内没有返回结果。请检查服务状态，或提高资讯分析超时时间。"
    if normalized in {"network", "network_error", "connect_timeout"}:
        return "无法连接模型服务。请检查网络、API 地址以及模型网关是否在线。"
    if normalized in {"authentication", "http_401", "http_403"}:
        return "模型服务拒绝鉴权。请检查 API 密钥以及该密钥的模型访问权限。"
    if normalized == "http_400":
        return "模型服务拒绝了请求（HTTP 400）。请检查模型名称和接口协议是否兼容。"
    if normalized == "http_404":
        return "没有找到模型接口或模型（HTTP 404）。请检查 API 地址和模型名称。"
    if normalized in {"http_408", "http_425"}:
        return "模型服务暂时无法处理请求。请稍后重试。"
    if normalized in {"http_500", "http_502", "http_503", "http_504"}:
        return "模型服务或上游网关暂时异常。请检查网关状态后重试。"
    if normalized.startswith("http_"):
        status = normalized.removeprefix("http_") or "未知"
        return f"模型服务返回 HTTP {status}。请检查接口配置和服务状态。"
    if normalized == "invalid_response":
        return (
            "模型接口返回的内容无法解析。常见原因是 API 地址指向网页、网关返回错误页，"
            "或接口协议不兼容；请检查 API 地址和模型服务。"
        )
    if normalized == "invalid_json":
        return "模型返回的分析内容不是合法 JSON。请检查模型兼容性或更换资讯分析模型。"
    if normalized == "empty_response":
        return "模型返回了空内容。请检查模型服务状态后重试。"
    if normalized == "claim_lost":
        return "该资讯已由另一项分析任务接管，无需重复处理。"
    if normalized == "invalid_batch_shape":
        return "模型返回的结果数量与本批资讯不一致。请重试或减小分析批次。"
    return f"资讯分析遇到未识别错误（{normalized.upper()}）。请查看本机日志获取技术细节。"


def _safe_analysis_error(exc: Exception) -> str:
    code = _analysis_failure_code(exc)
    if code == "invalid_response" and isinstance(exc, LLMError):
        return str(exc).strip()[:1000]
    return _analysis_error_message(code)


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

    def __init__(self, path: Path | None = None, *, read_only: bool = False):
        self.path = path or get_config().data_root / "news.sqlite"
        self.read_only = bool(read_only)
        if not self.read_only:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.sources = NewsSourceStore(self.path, read_only=self.read_only)
        from quantmaster.data.industry import (
            IndustrySnapshotIntegrityError,
            LegacyIndustrySnapshotError,
            load_cached_industry_map,
        )

        try:
            self._industry_map = load_cached_industry_map()
        except LegacyIndustrySnapshotError:
            # Expected after upgrading a personal installation from the mutable
            # pre-v2 cache.  It remains unavailable to formal news dimensions
            # until a verified current snapshot replaces it.
            self._industry_map = {}
        except (IndustrySnapshotIntegrityError, OSError, TypeError, ValueError) as exc:
            # Industry labels are optional analysis enrichment.  A legacy or
            # damaged projection must not make the news corpus unreadable.
            logger.warning("行业映射不可用于资讯标签增强，继续使用原始 symbols/sectors: %s", exc)
            self._industry_map = {}
        if not self.read_only:
            self._migrate()
        self.claims = NewsClaimStore(self.path, read_only=self.read_only)

    def _conn(self, *, write_intent: bool = False) -> sqlite3.Connection:
        return connect_sqlite(
            self.path,
            timeout=0.25 if self.read_only else (30.0 if write_intent else 5.0),
            row_factory=True,
            read_only=self.read_only,
        )

    def _replace_dimensions(
        self,
        connection: sqlite3.Connection,
        item_id: int,
        item: NewsItem,
    ) -> None:
        replace_news_dimensions(
            connection,
            item_id,
            item.symbols,
            item.sectors,
            industry_map=self._industry_map,
            normalize_sectors=_normalize_sectors,
        )

    @staticmethod
    def _factor_analysis_values(
        connection: sqlite3.Connection,
        source_id: str,
        importance_score: float,
    ) -> tuple[float | None, float | None]:
        """Freeze formal importance and source weight in the analysis transaction."""
        try:
            importance = float(importance_score)
        except (TypeError, ValueError, OverflowError):
            return None, None
        if not math.isfinite(importance) or not 0 <= importance <= 100:
            return None, None
        row = connection.execute(
            "SELECT factor_weight FROM news_sources WHERE id=?",
            (str(source_id or ""),),
        ).fetchone()
        if row is None:
            return None, None
        try:
            source_weight = float(row[0])
        except (TypeError, ValueError, OverflowError):
            return None, None
        if not math.isfinite(source_weight) or not 0 <= source_weight <= 3:
            return None, None
        return importance, source_weight

    @staticmethod
    def fingerprint(item: NewsItem) -> str:
        return news_fingerprint(
            item.source, item.title, item.url, item.published_at, item.provider_item_id,
        )

    @staticmethod
    def content_hash(item: NewsItem) -> str:
        return news_content_hash(item.content, item.title)

    def _migrate(self) -> None:
        with self._conn() as conn:
            NewsClaimStore.migrate(conn)
            migrate_news_schema(
                conn,
                industry_map=self._industry_map,
                normalize_sectors=_normalize_sectors,
            )

    def _dashboard_input_fingerprint(self) -> str:
        """Version the materialised read models from small SQLite metadata only."""

        with self._conn() as conn:
            news = conn.execute(
                "SELECT COUNT(*) AS row_count,COALESCE(MAX(id),0) AS max_id,"
                "COALESCE(MAX(last_seen_at),0) AS last_seen,"
                "COALESCE(MAX(content_version_at),0) AS content_version,"
                "COALESCE(MAX(analysis_updated_at),0) AS analysis_version "
                "FROM news"
            ).fetchone()
            sources = conn.execute(
                "SELECT COUNT(*) AS row_count,COALESCE(MAX(updated_at),'') AS updated_at "
                "FROM news_sources"
            ).fetchone()
        payload = {
            "schema_version": 1,
            "algorithm_version": _DASHBOARD_ALGORITHM_VERSION,
            "news": dict(news) if news else {},
            "sources": dict(sources) if sources else {},
        }
        encoded = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _dashboard_snapshot_id(
        kind: str, window_days: int, fingerprint: str, payload_json: str,
    ) -> str:
        value = "|".join((
            _DASHBOARD_ALGORITHM_VERSION, str(kind), str(window_days),
            str(fingerprint), payload_json,
        ))
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _materialized_dashboard(self, kind: str, window_days: int) -> dict[str, Any]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT input_fingerprint,snapshot_id,payload_json,generated_at "
                "FROM news_dashboard_materializations WHERE kind=? AND window_days=?",
                (str(kind), int(window_days)),
            ).fetchone()
        if row is None:
            raise FileNotFoundError(f"资讯 {kind}/{window_days} 尚未物化")
        try:
            payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise FileNotFoundError(f"资讯 {kind}/{window_days} 物化快照已损坏") from exc
        if not isinstance(payload, dict):
            raise FileNotFoundError(f"资讯 {kind}/{window_days} 物化快照格式无效")
        value = dict(payload)
        value["meta"] = {
            "snapshot_id": str(row["snapshot_id"]),
            "schema_version": 1,
            "algorithm_version": _DASHBOARD_ALGORITHM_VERSION,
            "input_fingerprint": str(row["input_fingerprint"]),
            "as_of": datetime.fromtimestamp(
                float(row["generated_at"]), UTC,
            ).date().isoformat(),
            "generated_at": datetime.fromtimestamp(
                float(row["generated_at"]), UTC,
            ).isoformat(),
            "stale": False,
            "stale_reasons": [],
            "quality": {},
        }
        return value

    def publish_dashboard_materializations(self) -> dict[str, Any]:
        """Publish all read-only news dashboard payloads in one transaction.

        Aggregation and evidence checks belong to the ingest/annotation worker.
        A reader sees either the previous complete set or the new complete set;
        it never rebuilds a window because a user opened the page.
        """

        if self.read_only:
            raise RuntimeError("只读资讯存储不能发布物化快照")
        fingerprint = self._dashboard_input_fingerprint()
        generated_at = time.time()
        payloads: list[tuple[str, int, dict[str, Any]]] = []
        for window_days in _DASHBOARD_WINDOWS:
            payloads.append(("stats", window_days, self._stats_dynamic(window_days)))
            payloads.append(("event_focus", window_days, self._event_focus_dynamic(window_days)))
        published: dict[str, str] = {}
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for kind, window_days, payload in payloads:
                payload_json = json.dumps(
                    payload, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"), default=str,
                )
                snapshot_id = self._dashboard_snapshot_id(
                    kind, window_days, fingerprint, payload_json,
                )
                conn.execute(
                    "INSERT INTO news_dashboard_materializations("
                    "kind,window_days,input_fingerprint,snapshot_id,payload_json,generated_at) "
                    "VALUES(?,?,?,?,?,?) ON CONFLICT(kind,window_days) DO UPDATE SET "
                    "input_fingerprint=excluded.input_fingerprint,"
                    "snapshot_id=excluded.snapshot_id,payload_json=excluded.payload_json,"
                    "generated_at=excluded.generated_at",
                    (kind, window_days, fingerprint, snapshot_id, payload_json, generated_at),
                )
                published[f"{kind}:{window_days}"] = snapshot_id
        # Expose a genuine generation to other DAG nodes without hashing news
        # bodies or walking evidence files.  The catalog itself preserves the
        # generation on a no-op materialisation with the same fingerprint.
        try:
            from quantmaster.runtime.derived import DerivedArtifactCatalog

            DerivedArtifactCatalog(self.path.parent / "derived").advance_source_generation(
                "news.dashboard", "all", fingerprint,
                coverage_end=datetime.fromtimestamp(generated_at, UTC).date().isoformat(),
            )
        except (OSError, RuntimeError, sqlite3.Error):
            logger.warning("资讯 dashboard generation 推进失败", exc_info=True)
        return {
            "input_fingerprint": fingerprint,
            "generated_at": generated_at,
            "snapshots": published,
        }

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
        # Workbench and alert APIs retain the established public name, while
        # formal factor consumers select factor_importance_score explicitly.
        value["importance_score"] = float(value.get("alert_importance_score") or 0)
        epoch = float(value.get("first_seen_at") or value.get("created_at") or 0)
        value["first_seen_epoch"] = epoch
        value["first_seen_at"] = (
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch)) if epoch else ""
        )
        if (
            value.get("analysis_status") in {"failed", "dead_letter"}
            and str(value.get("analysis_error") or "").strip()
            in {"", _GENERIC_ANALYSIS_ERROR}
        ):
            value["analysis_error"] = _analysis_error_message(
                str(value.get("last_failure_code") or "unknown"),
            )
        return value

    def save(self, items: list[NewsItem]) -> int:
        saved = 0
        now = time.time()
        ingest_identities = {
            (item.ingest_window_id, item.ingest_batch_id)
            for item in items
            if item.ingest_window_id and item.ingest_batch_id
        }
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
                if item.provider_item_id:
                    row = conn.execute(
                        "SELECT id,source_id,analysis_status,title,content,content_hash,raw_cache_key,"
                        "content_scope,fetched_at,provider_item_id,evidence_binding_hash,"
                        "ingest_window_id,ingest_batch_id FROM news "
                        "WHERE source_id=? AND provider_item_id=? LIMIT 1",
                        (item.source, item.provider_item_id),
                    ).fetchone()
                else:
                    row = conn.execute(
                        "SELECT id,source_id,analysis_status,title,content,content_hash,raw_cache_key,"
                        "content_scope,fetched_at,provider_item_id,evidence_binding_hash,"
                        "ingest_window_id,ingest_batch_id FROM news "
                        "WHERE fingerprint=? OR "
                        "(provider_item_id='' AND source=? AND title=? AND published_at=?) "
                        "LIMIT 1",
                        (item.fingerprint, item.source, item.title, item.published_at),
                    ).fetchone()
                if row:
                    scope_downgrade = (
                        str(row["content_scope"] or "") in {"full_article", "full_text"}
                        and item.content_scope not in {"full_article", "full_text"}
                    )
                    if scope_downgrade:
                        conn.execute(
                            "UPDATE news SET last_seen_at=?,fetched_at=MAX(fetched_at,?) "
                            "WHERE id=?",
                            (now, item.fetched_at or now, int(row["id"])),
                        )
                        continue
                    previous_hash = str(row["content_hash"] or news_content_hash(
                        str(row["content"] or ""), str(row["title"] or ""),
                    ))
                    is_provider_revision = bool(
                        item.provider_item_id
                        and item.provider_item_id == str(row["provider_item_id"] or "")
                        and (
                            content_hash != previous_hash
                            or item.title != str(row["title"] or "")
                        )
                    )
                    if is_provider_revision:
                        factor_importance, factor_weight = (
                            self._factor_analysis_values(
                                conn,
                                str(row["source_id"] or item.source),
                                item.importance_score,
                            )
                            if status == "complete" else (None, None)
                        )
                        conn.execute(
                            "INSERT INTO news_revisions("
                            "news_id,revision_number,title,content,content_hash,raw_cache_key,"
                            "fetched_at,evidence_binding_hash,recorded_at) VALUES (?,"
                            "COALESCE((SELECT MAX(revision_number)+1 FROM news_revisions "
                            "WHERE news_id=?),1),?,?,?,?,?,?,?)",
                            (
                                int(row["id"]), int(row["id"]), str(row["title"] or ""),
                                str(row["content"] or ""), previous_hash,
                                str(row["raw_cache_key"] or ""),
                                float(row["fetched_at"] or 0),
                                str(row["evidence_binding_hash"] or ""), now,
                            ),
                        )
                        conn.execute(
                            "UPDATE news SET title=?,content=?,content_hash=?,url=?,last_seen_at=?,"
                            "fetched_at=MAX(fetched_at,?),published_at=CASE WHEN ?<>'' THEN ? "
                            "ELSE published_at END,published_at_epoch=CASE WHEN ?>0 THEN ? "
                            "ELSE published_at_epoch END,raw_cache_key=?,evidence_binding_hash=?,"
                            "parser_version=?,ingest_window_id=?,ingest_batch_id=?,"
                            "is_official=?,content_scope=?,"
                            "symbols=?,sectors=?,event_type=?,sentiment=?,summary=?,"
                            "importance_score=?,factor_importance_score=?,"
                            "factor_weight_at_analysis=?,alert_importance_score=?,"
                            "scope=?,urgency=?,confidence=?,analysis_status=?,"
                            "analysis_attempts=0,analysis_error='',next_retry_at=0,"
                            "last_failure_code='',analysis_updated_at=?,content_version_at=?,"
                            "analysis_version=? "
                            "WHERE id=?",
                            (
                                item.title, item.content, content_hash, item.url, now,
                                item.fetched_at or now, item.published_at, item.published_at,
                                item.published_at_epoch, item.published_at_epoch,
                                item.raw_cache_key, item.evidence_binding_hash,
                                item.parser_version, item.ingest_window_id,
                                item.ingest_batch_id, int(item.is_official), item.content_scope,
                                json.dumps(item.symbols, ensure_ascii=False),
                                json.dumps(item.sectors, ensure_ascii=False), item.event_type,
                                item.sentiment, item.summary, item.importance_score,
                                factor_importance, factor_weight, item.importance_score,
                                item.scope, item.urgency, item.confidence, status,
                                now if status == "complete" else 0,
                                now, self.ANALYSIS_VERSION, int(row["id"]),
                            ),
                        )
                        self._replace_dimensions(conn, int(row["id"]), item)
                        saved += 1
                        continue
                    analysis_sql = ""
                    analysis_params: list[Any] = []
                    if status == "complete" and row["analysis_status"] != "complete":
                        factor_importance, factor_weight = self._factor_analysis_values(
                            conn,
                            str(row["source_id"] or item.source),
                            item.importance_score,
                        )
                        analysis_sql = (
                            ",symbols=?,sectors=?,event_type=?,sentiment=?,summary=?,importance_score=?,"
                            "factor_importance_score=?,factor_weight_at_analysis=?,"
                            "alert_importance_score=?,scope=?,urgency=?,confidence=?,"
                            "analysis_status='complete',analysis_error='',"
                            "analysis_updated_at=?"
                        )
                        analysis_params = [
                            json.dumps(item.symbols, ensure_ascii=False),
                            json.dumps(item.sectors, ensure_ascii=False), item.event_type,
                            item.sentiment, item.summary, item.importance_score,
                            factor_importance, factor_weight, item.importance_score,
                            item.scope, item.urgency, item.confidence, now,
                        ]
                    content = (
                        item.content
                        if not item.evidence_binding_hash
                        and len(item.content) >= len(str(row["content"] or ""))
                        else None
                    )
                    conn.execute(
                        "UPDATE news SET content=COALESCE(?,content),url=CASE WHEN ?<>'' THEN ? ELSE url END,"
                        "last_seen_at=?,fetched_at=MAX(fetched_at,?),"
                        "published_at=CASE WHEN ?<>'' THEN ? ELSE published_at END,"
                        "published_at_epoch=CASE WHEN ?>0 THEN ? ELSE published_at_epoch END,"
                        "provider_item_id=CASE WHEN ?<>'' THEN ? ELSE provider_item_id END,"
                        "raw_cache_key=CASE WHEN ?<>'' AND ?<>'' THEN ? ELSE raw_cache_key END,"
                        "evidence_binding_hash=CASE WHEN ?<>'' AND ?<>'' THEN ? "
                        "ELSE evidence_binding_hash END,"
                        "parser_version=CASE WHEN ?<>'' AND ?<>'' THEN ? ELSE parser_version END,"
                        "ingest_window_id=CASE WHEN ?<>'' THEN ? ELSE ingest_window_id END,"
                        "ingest_batch_id=CASE WHEN ?<>'' THEN ? ELSE ingest_batch_id END,"
                        "content_scope=CASE WHEN ?<>'unknown' THEN ? ELSE content_scope END "
                        f"{analysis_sql} WHERE id=?",
                        [content, item.url, item.url, now, item.fetched_at or now,
                         item.published_at, item.published_at,
                         item.published_at_epoch, item.published_at_epoch,
                          item.provider_item_id, item.provider_item_id,
                          item.raw_cache_key, item.evidence_binding_hash, item.raw_cache_key,
                          item.raw_cache_key, item.evidence_binding_hash,
                          item.evidence_binding_hash,
                          item.raw_cache_key, item.evidence_binding_hash, item.parser_version,
                          item.ingest_window_id, item.ingest_window_id,
                          item.ingest_batch_id, item.ingest_batch_id,
                          item.content_scope, item.content_scope,
                          *analysis_params, row["id"]],
                    )
                    if analysis_sql:
                        self._replace_dimensions(conn, int(row["id"]), item)
                    continue
                try:
                    factor_importance, factor_weight = (
                        self._factor_analysis_values(
                            conn, item.source, item.importance_score,
                        )
                        if status == "complete" else (None, None)
                    )
                    cursor = conn.execute(
                        "INSERT INTO news "
                        "(source,title,content,url,published_at,published_at_epoch,fetched_at,provider_item_id,"
                        "symbols,sectors,event_type,sentiment,summary,"
                        "created_at,importance_score,factor_importance_score,"
                        "factor_weight_at_analysis,alert_importance_score,"
                        "scope,urgency,confidence,fingerprint,is_official,"
                        "content_scope,source_id,content_hash,first_seen_at,last_seen_at,"
                        "raw_cache_key,evidence_binding_hash,ingest_window_id,ingest_batch_id,"
                        "analysis_status,"
                        "analysis_updated_at,content_version_at,analysis_version,parser_version) "
                        "VALUES (" + ",".join("?" for _ in range(37)) + ")",
                        (item.source, item.title, item.content, item.url, item.published_at,
                         item.published_at_epoch, item.fetched_at or now, item.provider_item_id,
                         json.dumps(item.symbols, ensure_ascii=False),
                         json.dumps(item.sectors, ensure_ascii=False), item.event_type, item.sentiment,
                         item.summary, now, item.importance_score,
                           factor_importance, factor_weight, item.importance_score,
                           item.scope, item.urgency, item.confidence,
                           item.fingerprint, int(item.is_official),
                          item.content_scope, item.source,
                          content_hash, now, now, item.raw_cache_key,
                          item.evidence_binding_hash, item.ingest_window_id,
                          item.ingest_batch_id, status,
                          now if status == "complete" else 0, now,
                          self.ANALYSIS_VERSION, item.parser_version),
                    )
                    if cursor.lastrowid is None:
                        raise sqlite3.IntegrityError("资讯写入未返回记录 ID")
                    self._replace_dimensions(conn, int(cursor.lastrowid), item)
                    saved += 1
                except sqlite3.IntegrityError:
                    continue
        self.sources.complete_persisted_ingest_batches(ingest_identities)
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
        with self._conn(write_intent=True) as conn:
            conn.execute("BEGIN IMMEDIATE")
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
        source_row = conn.execute(
            "SELECT source_id FROM news WHERE id=?", (item_id,),
        ).fetchone()
        if source_row is None:
            return False
        factor_importance, factor_weight = self._factor_analysis_values(
            conn, str(source_row[0] or item.source), item.importance_score,
        )
        changed = conn.execute(
            "UPDATE news SET symbols=?,sectors=?,event_type=?,sentiment=?,summary=?,importance_score=?,"
            "factor_importance_score=?,factor_weight_at_analysis=?,alert_importance_score=?,"
            "scope=?,urgency=?,confidence=?,analysis_status='complete',analysis_error='',"
            "analysis_version=?,next_retry_at=0,last_failure_code='',analysis_updated_at=? "
            "WHERE id=?",
            (json.dumps(item.symbols, ensure_ascii=False),
             json.dumps(item.sectors, ensure_ascii=False), item.event_type,
             item.sentiment, item.summary, item.importance_score,
             factor_importance, factor_weight, item.importance_score,
             item.scope, item.urgency, item.confidence,
             self.ANALYSIS_VERSION, time.time(), item_id),
        ).rowcount
        if changed:
            self._replace_dimensions(conn, item_id, item)
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
        with self._conn(write_intent=True) as connection:
            # Claims must be checked only after this writer owns the reserved
            # lock.  A deferred transaction lets concurrent annotation batches
            # all read their claims first and then deadlock while upgrading to
            # writers; SQLite reports that upgrade cycle immediately as
            # ``database is locked`` even with a busy timeout configured.
            connection.execute("BEGIN IMMEDIATE")
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
        with self._conn(write_intent=True) as conn:
            # Serialize the read/modify/write attempt counter for the same
            # reason as successful analysis persistence.  A database lock is
            # infrastructure contention, not another failed model attempt.
            conn.execute("BEGIN IMMEDIATE")
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
        where = "analysis_status<>'pending'"
        params: list[Any] = []
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
        order = (
            "n.alert_importance_score DESC,n.id DESC"
            if sort == "importance" else "n.id DESC"
        )
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
                "UPDATE news SET alert_importance_score=?,scope=?,urgency=? WHERE id=?",
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
        """Return completed formal analyses in the requested publication window.

        Processing timestamps remain part of the evidence contract, but never move
        an event out of the window in which it was published.
        """
        clauses = [
            "n.analysis_status='complete'",
            "n.content_scope IN ('full_text','full_article','feed_summary')",
            "n.is_official=1",
            "COALESCE(s.is_official,0)=1",
            "COALESCE(s.built_in,0)=1",
            "n.published_at_epoch>0",
            "n.factor_importance_score>0",
            "n.factor_importance_score<=100",
            "n.factor_weight_at_analysis>0",
            "n.factor_weight_at_analysis<=3",
            "n.raw_cache_key<>''",
            "qm_news_raw_valid(n.raw_cache_key)=1",
            "n.evidence_binding_hash<>''",
            "n.ingest_window_id<>''",
            "n.ingest_batch_id<>''",
            "qm_news_article_evidence_valid("
            "n.source_id,n.raw_cache_key,n.url,n.provider_item_id,n.title,n.content,"
            "n.published_at,n.published_at_epoch,n.content_scope,n.parser_version,"
            "n.content_hash,n.evidence_binding_hash)=1",
            "EXISTS (SELECT 1 FROM news_raw_manifest h "
            "WHERE h.source_id=n.source_id AND h.raw_cache_key=n.raw_cache_key)",
            "EXISTS (SELECT 1 FROM news_article_evidence_manifest e "
            "WHERE e.binding_hash=n.evidence_binding_hash "
            "AND e.source_id=n.source_id AND e.raw_cache_key=n.raw_cache_key "
            "AND e.article_url=n.url AND e.provider_item_id=n.provider_item_id "
            "AND e.content_hash=n.content_hash AND e.title=n.title AND e.content=n.content "
            "AND e.published_at=n.published_at "
            "AND e.published_at_epoch=n.published_at_epoch "
            "AND e.content_scope=n.content_scope AND e.parser_version=n.parser_version)",
            "EXISTS (SELECT 1 FROM news_ingest_windows w "
            "JOIN news_ingest_batches b ON b.window_id=w.window_id "
            "JOIN news_ingest_batch_articles ba ON ba.batch_id=b.batch_id "
            "WHERE w.window_id=n.ingest_window_id AND w.source_id=n.source_id "
            "AND w.status='complete' AND w.completed_batch_id<>'' "
            "AND b.batch_id=n.ingest_batch_id AND b.source_id=n.source_id "
            "AND ba.evidence_binding_hash=n.evidence_binding_hash "
            "AND ba.source_id=n.source_id AND ba.provider_item_id=n.provider_item_id "
            "AND ba.raw_cache_key=n.raw_cache_key)",
        ]
        params: list[Any] = []
        if start_epoch is not None:
            clauses.append("n.published_at_epoch>=?")
            params.append(start_epoch)
        if end_epoch is not None:
            clauses.append("n.published_at_epoch<=?")
            params.append(end_epoch)
        with self._conn() as conn:
            register_news_raw_verifier(conn)
            rows = conn.execute(
                "SELECT n.id,n.first_seen_at,n.content_version_at,n.analysis_updated_at,"
                "n.published_at_epoch,n.symbols,n.sentiment,n.confidence,"
                "n.factor_importance_score AS importance_score,n.content_hash,"
                "n.factor_weight_at_analysis AS source_weight,n.source_id,"
                "COALESCE(s.name,n.source_id) AS source_name,"
                "COALESCE(s.group_name,'') AS source_group,n.content_scope "
                "FROM news n LEFT JOIN news_sources s ON s.id=n.source_id WHERE "
                + " AND ".join(clauses)
                + " ORDER BY n.published_at_epoch,n.id",
                params,
            ).fetchall()
        result = []
        for row in rows:
            value = dict(row)
            value["symbols"] = json.loads(value.get("symbols") or "[]")
            value["formal_eligible"] = True
            value["formal_ineligible_reasons"] = []
            result.append(value)
        return result

    def sandbox_factor_rows(self, start_epoch: float | None = None,
                            end_epoch: float | None = None) -> list[dict]:
        """Return completed built-in rows for explicitly non-production research.

        This path deliberately permits provider excerpts and pending ingest windows so
        recent news can be explored before the formal evidence window is complete.  It
        does *not* relax :meth:`factor_rows`: every returned row carries the exact
        reasons that keep it out of the production contract. ``start_epoch`` and
        ``end_epoch`` always describe the event publication window; later processing
        does not rewrite that window.
        """
        published_epoch_sql = (
            "CASE WHEN n.published_at_epoch>0 THEN n.published_at_epoch "
            "WHEN substr(n.published_at,-1,1)='Z' "
            "OR substr(n.published_at,-6,1) IN ('+','-') "
            "THEN CAST(strftime('%s',n.published_at) AS REAL) "
            "ELSE CAST(strftime('%s',n.published_at,'-8 hours') AS REAL) END"
        )
        importance_sql = (
            "COALESCE(NULLIF(n.factor_importance_score,0),n.importance_score)"
        )
        source_weight_sql = (
            "COALESCE(NULLIF(n.factor_weight_at_analysis,0),s.factor_weight)"
        )
        clauses = [
            "n.analysis_status='complete'",
            "n.content_scope IN ("
            "'full_text','full_article','feed_summary','provider_excerpt','unknown')",
            "COALESCE(s.built_in,0)=1",
            f"{published_epoch_sql}>0",
            "n.first_seen_at>0",
            "n.content_version_at>0",
            "n.analysis_updated_at>0",
            f"{importance_sql}>0",
            f"{importance_sql}<=100",
            f"{source_weight_sql}>0",
            f"{source_weight_sql}<=3",
        ]
        params: list[Any] = []
        if start_epoch is not None:
            clauses.append(f"{published_epoch_sql}>=?")
            params.append(start_epoch)
        if end_epoch is not None:
            clauses.append(f"{published_epoch_sql}<=?")
            params.append(end_epoch)

        formal_ids = {
            int(row["id"])
            for row in self.factor_rows(start_epoch, end_epoch)
        }
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT n.id,n.first_seen_at,n.content_version_at,n.analysis_updated_at,"
                f"{published_epoch_sql} AS published_at_epoch,"
                "n.symbols,n.sentiment,n.confidence,"
                f"{importance_sql} AS importance_score,n.content_hash,"
                f"{source_weight_sql} AS source_weight,n.source_id,"
                "COALESCE(s.name,n.source_id) AS source_name,"
                "COALESCE(s.group_name,'') AS source_group,"
                "n.is_official AS article_is_official,"
                "COALESCE(s.is_official,0) AS source_is_official,n.content_scope,"
                "(n.published_at_epoch<=0) AS legacy_published_epoch,"
                "(n.factor_importance_score IS NULL) AS legacy_factor_importance,"
                "(n.factor_weight_at_analysis IS NULL) AS legacy_source_weight,"
                "n.raw_cache_key,n.evidence_binding_hash,n.ingest_window_id,"
                "n.ingest_batch_id,EXISTS (SELECT 1 FROM news_ingest_windows w "
                "WHERE w.window_id=n.ingest_window_id AND w.source_id=n.source_id "
                "AND w.status='complete' AND w.completed_batch_id<>'') "
                "AS ingest_window_complete FROM news n "
                "LEFT JOIN news_sources s ON s.id=n.source_id WHERE "
                + " AND ".join(clauses)
                + f" ORDER BY {published_epoch_sql},n.id",
                params,
            ).fetchall()
        result = []
        for row in rows:
            value = dict(row)
            value["symbols"] = json.loads(value.get("symbols") or "[]")
            reasons: list[str] = []
            if not bool(value.get("article_is_official")) or not bool(
                value.get("source_is_official")
            ):
                reasons.append("non_official_source")
            if value.get("content_scope") == "provider_excerpt":
                reasons.append("provider_excerpt_only")
            if value.get("content_scope") == "unknown":
                reasons.append("legacy_unknown_content_scope")
            if (
                bool(value.get("legacy_published_epoch"))
                or bool(value.get("legacy_factor_importance"))
                or bool(value.get("legacy_source_weight"))
            ):
                reasons.append("legacy_unfrozen_analysis_contract")
            if not value.get("raw_cache_key") or not value.get("evidence_binding_hash"):
                reasons.append("formal_raw_evidence_missing")
            if not bool(value.get("ingest_window_complete")):
                reasons.append("ingest_window_incomplete")
            formal_eligible = int(value["id"]) in formal_ids
            if not formal_eligible and not reasons:
                reasons.append("formal_evidence_contract_failed")
            value["formal_eligible"] = formal_eligible
            value["formal_ineligible_reasons"] = reasons
            result.append(value)
        return result

    def factor_coverage(self, minimum_confidence: float = 0.0) -> dict[str, Any]:
        """Return the usable local annotation span without materialising news rows."""
        with self._conn() as conn:
            register_news_raw_verifier(conn)
            row = conn.execute(
                "SELECT MIN(n.published_at_epoch) AS first_published_at,"
                "MAX(n.published_at_epoch) AS last_published_at,COUNT(*) AS event_count "
                "FROM news n LEFT JOIN news_sources s ON s.id=n.source_id "
                "WHERE n.analysis_status='complete' AND n.first_seen_at>0 "
                "AND n.content_version_at>0 AND n.analysis_updated_at>0 "
                "AND COALESCE(n.confidence,0)>=? "
                "AND n.factor_importance_score>0 AND n.factor_importance_score<=100 "
                "AND n.published_at_epoch>0 "
                "AND n.content_scope IN ('full_text','full_article','feed_summary') "
                "AND n.is_official=1 AND COALESCE(s.is_official,0)=1 "
                "AND COALESCE(s.built_in,0)=1 "
                "AND n.raw_cache_key<>'' AND qm_news_raw_valid(n.raw_cache_key)=1 "
                "AND n.evidence_binding_hash<>'' "
                "AND n.ingest_window_id<>'' AND n.ingest_batch_id<>'' "
                "AND qm_news_article_evidence_valid("
                "n.source_id,n.raw_cache_key,n.url,n.provider_item_id,n.title,n.content,"
                "n.published_at,n.published_at_epoch,n.content_scope,n.parser_version,"
                "n.content_hash,n.evidence_binding_hash)=1 "
                "AND EXISTS (SELECT 1 FROM news_raw_manifest h "
                "WHERE h.source_id=n.source_id AND h.raw_cache_key=n.raw_cache_key) "
                "AND EXISTS (SELECT 1 FROM news_article_evidence_manifest e "
                "WHERE e.binding_hash=n.evidence_binding_hash "
                "AND e.source_id=n.source_id AND e.raw_cache_key=n.raw_cache_key "
                "AND e.article_url=n.url AND e.provider_item_id=n.provider_item_id "
                "AND e.content_hash=n.content_hash AND e.title=n.title "
                "AND e.content=n.content AND e.published_at=n.published_at "
                "AND e.published_at_epoch=n.published_at_epoch "
                "AND e.content_scope=n.content_scope "
                "AND e.parser_version=n.parser_version) "
                "AND EXISTS (SELECT 1 FROM news_ingest_windows w "
                "JOIN news_ingest_batches b ON b.window_id=w.window_id "
                "JOIN news_ingest_batch_articles ba ON ba.batch_id=b.batch_id "
                "WHERE w.window_id=n.ingest_window_id AND w.source_id=n.source_id "
                "AND w.status='complete' AND w.completed_batch_id<>'' "
                "AND b.batch_id=n.ingest_batch_id AND b.source_id=n.source_id "
                "AND ba.evidence_binding_hash=n.evidence_binding_hash "
                "AND ba.source_id=n.source_id AND ba.provider_item_id=n.provider_item_id "
                "AND ba.raw_cache_key=n.raw_cache_key) "
                "AND n.factor_weight_at_analysis>0 AND n.factor_weight_at_analysis<=3",
                (max(0.0, float(minimum_confidence)),),
            ).fetchone()
        value = dict(row) if row else {}
        return {
            "first_published_at": float(value.get("first_published_at") or 0),
            "last_published_at": float(value.get("last_published_at") or 0),
            "event_count": int(value.get("event_count") or 0),
        }

    def market_sentiment(
        self, *, as_of: float, days: int = 30, knowledge_as_of: float | None = None,
    ) -> dict[str, Any]:
        """Return a quality-weighted market proxy with explicit event/PIT cutoffs.

        By default evidence is required to have been visible by ``as_of``.  An
        operational snapshot may pass its generation time as ``knowledge_as_of``
        while keeping the publication window bounded by ``as_of``.
        """
        reference = float(as_of)
        if not math.isfinite(reference) or reference <= 0:
            raise ValueError("情绪代理时点必须是有效时间戳")
        visible_until = reference if knowledge_as_of is None else float(knowledge_as_of)
        if not math.isfinite(visible_until) or visible_until <= 0:
            raise ValueError("资讯证据可见时点必须是有效时间戳")
        window_days = max(1, min(int(days), 3650))
        cutoff = reference - window_days * 86400
        news_config = get_config().news
        minimum = float(news_config.factor_min_confidence)
        halflife_days = max(0.01, float(news_config.factor_halflife_days))
        with self._conn() as conn:
            aggregate_rows = aggregate_news_stats(
                conn,
                cutoff=cutoff,
                until=reference,
                minimum_confidence=minimum,
                now=reference,
                halflife_days=halflife_days,
                knowledge_until=visible_until,
            )
        market_row: dict[str, Any] = next(
            (row for row in aggregate_rows if str(row.get("item_type") or "") == "market"),
            {},
        )
        snapshot = _sentiment_snapshot_from_totals(
            float(market_row.get("weighted_score") or 0),
            float(market_row.get("total_weight") or 0),
            int(market_row.get("event_count") or 0),
        )
        snapshot.update({
            "as_of_epoch": reference,
            "knowledge_as_of_epoch": visible_until,
            "lookback_days": window_days,
            "halflife_days": halflife_days,
            "minimum_confidence": minimum,
            "total_weight": round(float(market_row.get("total_weight") or 0), 4),
        })
        return snapshot

    def _stats_dynamic(self, days: int = 30) -> dict:
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
                "SUM(alert_importance_score>=80) AS important,"
                "SUM(sentiment>0.15 AND analysis_status='complete') AS positive,"
                "SUM(sentiment<-0.15 AND analysis_status='complete') AS negative "
                "FROM news WHERE first_seen_at>=? AND first_seen_at<=?", (cutoff, now),
            ).fetchone()
            queue_counts = conn.execute(
                "SELECT SUM(analysis_status='pending') AS pending,"
                "SUM(analysis_status='failed') AS failed,"
                "SUM(analysis_status='recovery') AS recovery,"
                "SUM(analysis_status='dead_letter') AS dead_letter,"
                "SUM(analysis_status='dead_letter') AS manual_recoverable_dead_letter,"
                "SUM(analysis_status='dead_letter' AND analysis_recovery_count<3 "
                "AND next_retry_at<=?) AS recoverable_dead_letter FROM news",
                (now,),
            ).fetchone()
            aggregate_rows = aggregate_news_stats(
                conn,
                cutoff=cutoff,
                until=now,
                minimum_confidence=minimum,
                now=now,
                halflife_days=halflife_days,
            )
        series: list[tuple[str, float]] = []
        market_aggregate = {
            "weighted_score": 0.0,
            "total_weight": 0.0,
            "event_count": 0,
        }
        sector_scores: list[dict[str, Any]] = []
        symbol_counts: dict[str, int] = {}
        for row in aggregate_rows:
            item_type = str(row["item_type"])
            item_key = str(row["item_key"] or "")
            weighted_score = float(row["weighted_score"] or 0)
            total_weight = float(row["total_weight"] or 0)
            event_count = int(row["event_count"] or 0)
            if item_type == "daily" and total_weight > 0:
                series.append((item_key, round(weighted_score / total_weight, 4)))
            elif item_type == "market":
                market_aggregate = {
                    "weighted_score": weighted_score,
                    "total_weight": total_weight,
                    "event_count": event_count,
                }
            elif item_type == "sector":
                sector_scores.append({
                    "sector": item_key,
                    **_sentiment_snapshot_from_totals(
                        weighted_score, total_weight, event_count,
                    ),
                    "positive": int(row["positive"] or 0),
                    "negative": int(row["negative"] or 0),
                })
            elif item_type == "symbol":
                symbol_counts[item_key] = event_count
        series.sort(key=lambda item: item[0])
        data: dict[str, Any] = dict(counts) if counts else {}
        data["queue"] = {
            key: int((queue_counts[key] if queue_counts else 0) or 0)
            for key in (
                "pending", "failed", "recovery", "dead_letter",
                "manual_recoverable_dead_letter", "recoverable_dead_letter",
            )
        }
        claims = self.claims.stats(now=now)
        data["queue"]["claims"] = claims
        data["queue"]["processing"] = int(data["queue"]["recovery"]) + claims["active"]
        data["coverage"] = round(int(data.get("annotated") or 0) / max(1, int(data.get("total") or 0)), 4)
        data["sentiment_series"] = series
        data["halflife_days"] = halflife_days
        data["market_sentiment"] = _sentiment_snapshot_from_totals(
            float(market_aggregate["weighted_score"]),
            float(market_aggregate["total_weight"]),
            int(market_aggregate["event_count"]),
        )
        data["sector_scores"] = sorted(
            sector_scores,
            key=lambda item: (-abs(float(item["score"])), str(item["sector"])),
        )
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
        from quantmaster.data import read_stock_names

        symbol_names = read_stock_names([symbol for symbol, _count in top_symbols])
        data["top_symbols"] = [
            {
                "symbol": symbol,
                "name": symbol_names.get(symbol, ""),
                "count": count,
            }
            for symbol, count in top_symbols
        ]
        return data

    def _event_focus_dynamic(self, days: int = 7) -> dict:
        """Return short-cycle symbol mentions using the same quality gates as stats."""
        window_days = int(days)
        if window_days not in {1, 3, 7, 30}:
            raise ValueError("事件聚焦窗口仅支持 1、3、7、30 日")
        now = time.time()
        cutoff = now - window_days * 86400
        minimum = get_config().news.factor_min_confidence
        with self._conn() as conn:
            rows = aggregate_news_event_focus(
                conn,
                cutoff=cutoff,
                until=now,
                minimum_confidence=minimum,
            )
        from quantmaster.data import read_stock_names

        symbols = [str(row["symbol"]) for row in rows]
        symbol_names = read_stock_names(symbols)
        return {
            "days": window_days,
            "top_symbols": [
                {
                    "symbol": symbol,
                    "name": symbol_names.get(symbol, ""),
                    "count": int(row["event_count"] or 0),
                }
                for row, symbol in zip(rows, symbols, strict=True)
            ],
        }

    def stats(self, days: int = 30) -> dict:
        window_days = max(1, min(int(days), 3650))
        if self.read_only:
            return self._materialized_dashboard("stats", window_days)
        return self._stats_dynamic(window_days)

    def event_focus(self, days: int = 7) -> dict:
        window_days = int(days)
        if window_days not in _DASHBOARD_WINDOWS:
            raise ValueError("事件聚焦窗口仅支持 1、3、7、30 日")
        if self.read_only:
            return self._materialized_dashboard("event_focus", window_days)
        return self._event_focus_dynamic(window_days)


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
        delay = interval
        while not stop.wait(delay):
            try:
                if not claims.heartbeat(token, owner, lease_seconds=lease_seconds):
                    alive.clear()
                    return
                delay = interval
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).casefold():
                    alive.clear()
                    logger.warning("资讯分析租约续期失败", exc_info=True)
                    return
                # A transient writer must not kill the heartbeat thread.  Retry
                # promptly; a later zero-row heartbeat still detects expiry.
                delay = 1.0
            except sqlite3.Error:
                alive.clear()
                logger.warning("资讯分析租约续期失败", exc_info=True)
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
        self._client_lock = threading.Lock()
        self.store = store or NewsStore()
        self.source_store = source_store or self.store.sources
        self.identity = WorkerIdentity.create("news-analysis")

    @property
    def client(self) -> LLMClient:
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    self._client = LLMClient(concurrency_scope="news")
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
                    model=cfg.annotation_model or None,
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
            published_at=value.published_at, published_at_epoch=value.published_at_epoch,
            fetched_at=value.fetched_at, provider_item_id=value.provider_item_id,
            is_official=value.is_official,
            raw_cache_key=value.raw_cache_key, content_scope=value.content_scope,
            parser_version=value.parser_version,
            evidence_binding_hash=value.evidence_binding_hash,
            ingest_window_id=value.ingest_window_id,
            ingest_batch_id=value.ingest_batch_id,
            analysis_status="pending",
        )

    def _fetch_source_batch(self, source: dict, limit: int | None = None,
                            *, preview: bool = False) -> FetchBatch:
        selected_limit = min(limit or source["item_limit"], source["item_limit"])
        if source["kind"] == "builtin":
            batch = fetch_builtin_source(source, self.source_store, limit=selected_limit)
        else:
            value = dict(source)
            value["item_limit"] = selected_limit
            batch = fetch_declarative_source(
                value,
                self.source_store,
                preview=preview,
                state={} if preview else self.source_store.state(source["id"]),
            )
        if not preview:
            self.source_store.bind_articles(batch.articles)
        return batch

    def _fetch_source(self, source: dict, limit: int | None = None,
                      *, preview: bool = False) -> list[NewsItem]:
        batch = self._fetch_source_batch(source, limit, preview=preview)
        return [self._from_fetched(item) for item in batch.articles]

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

            def cached_response(self, source_id, url):
                return None

        batch = fetch_declarative_source(
            temporary, _PreviewStore(self.source_store, token), preview=True,
        )
        return [
            {"title": item.title, "content": item.content[:500], "url": item.url,
             "published_at": item.published_at}
            for item in batch.articles
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

    def _process_annotation_batch(
        self,
        batch: ClaimBatch,
        items: list[NewsItem],
        batch_number: int,
        cfg: Any,
        cancelled: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        """Process one claimed provider batch without mutating shared progress counters."""
        written_ids: list[int] = []
        retry_scheduled = dead_letter = 0
        failure_detail: dict[str, Any] | None = None
        error = ""
        try:
            with _claim_heartbeat(
                self.store.claims, batch.token, self.identity.value,
            ) as lease_alive:
                if cancelled is not None and cancelled():
                    raise InterruptedError("news annotation cancelled")
                parsed = self.client.chat_json(
                    self._annotation_prompt(items), system=EXTRACT_SYSTEM,
                    timeout=cfg.annotation_timeout,
                    reasoning_effort=cfg.annotation_reasoning_effort,
                    model=cfg.annotation_model or None,
                )
                if cancelled is not None and cancelled():
                    raise InterruptedError("news annotation cancelled")
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
        except InterruptedError:
            # A configuration rotation is neither a provider failure nor a
            # dead letter.  The finally block releases its claim immediately.
            raise
        except sqlite3.Error:
            # Persistence contention is infrastructure failure, not another
            # failed model attempt.  Leave the rows pending and release their
            # claims so the already independent successful batches stay saved.
            logger.warning("资讯分析结果落库失败", exc_info=True)
            error = "资讯分析结果暂未写入，请稍后重试"
            failure_detail = {
                "batch": batch_number,
                "code": "storage_busy",
                "message": error,
                "retryable": True,
                "failed": len(items),
                "retry_scheduled": 0,
                "dead_letter": 0,
                "next_retry_at": 0.0,
            }
        except Exception as exc:
            logger.warning("资讯分析批次失败", exc_info=True)
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
            retry_scheduled = int(outcome["retry_scheduled"])
            dead_letter = int(outcome["dead_letter"])
            failure_detail = {
                "batch": batch_number,
                "code": failure_code,
                "message": error,
                "retryable": bool(retryable),
                "failed": int(outcome["failed"]),
                "retry_scheduled": retry_scheduled,
                "dead_letter": dead_letter,
                "next_retry_at": float(outcome["next_retry_at"]),
            }
        finally:
            try:
                self.store.claims.release(batch.token, self.identity.value)
            except sqlite3.Error:
                logger.warning("资讯分析租约释放失败", exc_info=True)
        return {
            "batch": batch_number,
            "item_count": len(items),
            "updated_ids": [item.db_id for item in items if item.db_id is not None],
            "completed_ids": written_ids,
            "retry_scheduled": retry_scheduled,
            "dead_letter": dead_letter,
            "failure_detail": failure_detail,
            "error": error,
        }

    def enrich_pending_events(
        self, limit: int | None = None, ids: list[int] | None = None,
        batch_size: int | None = None,
        *,
        mode: ClaimMode = "pending",
        manual: bool = False,
        cancelled: Callable[[], bool] | None = None,
    ) -> Iterator[dict]:
        """Claim and process one fixed queue window, yielding durable progress."""
        with NewsPipelineLock(self.store.path):
            yield from self._enrich_pending_events_unlocked(
                limit=limit, ids=ids, batch_size=batch_size,
                mode=mode, manual=manual, cancelled=cancelled,
            )

    def _enrich_pending_events_unlocked(
        self, limit: int | None = None, ids: list[int] | None = None,
        batch_size: int | None = None,
        *,
        mode: ClaimMode = "pending",
        manual: bool = False,
        cancelled: Callable[[], bool] | None = None,
    ) -> Iterator[dict]:
        """Run one queue window while the database-wide pipeline lock is held."""
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
        concurrency = max(
            1,
            min(int(cfg.annotation_max_concurrency), 16, batch_count or 1),
        )
        futures: dict[Future[dict[str, Any]], tuple[int, ClaimBatch]] = {}
        executor = ThreadPoolExecutor(
            max_workers=concurrency,
            thread_name_prefix="qm-news-annotation",
        )

        def submit_next() -> bool:
            nonlocal batch_number, claimed, recovered_leases, remaining_ids
            if cancelled is not None and cancelled():
                raise InterruptedError("news annotation cancelled")
            if claimed >= total:
                return False
            batch = self.store.claims.claim(
                owner=self.identity.value,
                task_type=f"news:{mode}",
                mode=mode,
                limit=min(size, total - claimed),
                ids=remaining_ids,
                max_id=max_id,
                manual=manual,
            )
            recovered_leases += batch.recovered_leases
            if not batch.ids:
                return False
            batch_number += 1
            claimed += len(batch.ids)
            if remaining_ids is not None:
                claimed_set = set(batch.ids)
                remaining_ids = [
                    value for value in remaining_ids if value not in claimed_set
                ]
            chunk = self.store.rows_by_ids(list(batch.ids))
            items = [NewsItem(
                source=row["source_id"], title=row["title"], content=row["content"],
                url=row["url"], published_at=row["published_at"],
                is_official=row["is_official"], db_id=row["id"],
            ) for row in chunk]
            future = executor.submit(
                self._process_annotation_batch, batch, items, batch_number, cfg, cancelled,
            )
            futures[future] = (batch_number, batch)
            return True

        try:
            while len(futures) < concurrency and submit_next():
                pass
            while futures:
                if cancelled is not None and cancelled():
                    raise InterruptedError("news annotation cancelled")
                done, _ = wait(
                    tuple(futures), timeout=0.1, return_when=FIRST_COMPLETED,
                )
                if not done:
                    continue
                for future in sorted(done, key=lambda value: futures[value][0]):
                    futures.pop(future)
                    outcome = future.result()
                    if cancelled is not None and cancelled():
                        raise InterruptedError("news annotation cancelled")
                    chunk_completed = len(outcome["completed_ids"])
                    chunk_failed = int(outcome["item_count"]) - chunk_completed
                    chunk_retry_scheduled = int(outcome["retry_scheduled"])
                    chunk_dead_letter = int(outcome["dead_letter"])
                    processed += int(outcome["item_count"])
                    completed += chunk_completed
                    failed += chunk_failed
                    retry_scheduled += chunk_retry_scheduled
                    dead_letter += chunk_dead_letter
                    completed_ids.extend(outcome["completed_ids"])
                    if outcome["failure_detail"] is not None:
                        failure_details.append(outcome["failure_detail"])
                    updated_items = [
                        self._stream_item(value)
                        for value in self.store.rows_by_ids(outcome["updated_ids"])
                    ]
                    yield {
                        "type": "batch", "batch": outcome["batch"],
                        "batch_count": batch_count, "processed": processed,
                        "total": total, "completed": completed, "failed": failed,
                        "batch_completed": chunk_completed,
                        "batch_failed": chunk_failed,
                        "retry_scheduled": retry_scheduled,
                        "dead_letter": dead_letter,
                        "batch_retry_scheduled": chunk_retry_scheduled,
                        "batch_dead_letter": chunk_dead_letter,
                        "completed_ids": outcome["completed_ids"],
                        "updated_items": updated_items,
                        "error": outcome["error"], "claimed": claimed,
                        "in_progress": queue["in_progress"],
                        "recovered_leases": recovered_leases,
                    }
                    while len(futures) < concurrency and submit_next():
                        pass
        finally:
            for future, (_, batch) in futures.items():
                future.cancel()
                try:
                    self.store.claims.release(batch.token, self.identity.value)
                except sqlite3.Error:
                    logger.warning("资讯取消时释放分析租约失败", exc_info=True)
            executor.shutdown(wait=True, cancel_futures=True)
        result = {
            "processed": processed, "completed": completed,
            "failed": failed, "retry_scheduled": retry_scheduled,
            "dead_letter": dead_letter, "failure_details": failure_details,
            "completed_ids": completed_ids, "claimed": claimed,
            "in_progress": queue["in_progress"],
            "recovered_leases": recovered_leases,
        }
        try:
            self.store.publish_dashboard_materializations()
        except (OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
            # The prior complete read model deliberately remains current when
            # a writer cannot publish a replacement.  Do not turn a completed
            # annotation batch into a false database failure.
            logger.warning("资讯 dashboard 物化发布失败", exc_info=True)
        yield {"type": "complete", **result}

    def enrich_pending(
        self, limit: int | None = None, ids: list[int] | None = None,
        batch_size: int | None = None,
        *,
        mode: ClaimMode = "pending",
        manual: bool = False,
        cancelled: Callable[[], bool] | None = None,
    ) -> dict:
        result = {
            "processed": 0, "completed": 0, "failed": 0,
            "retry_scheduled": 0, "dead_letter": 0,
            "failure_details": [], "completed_ids": [],
        }
        for event in self.enrich_pending_events(
            limit=limit, ids=ids, batch_size=batch_size, mode=mode, manual=manual,
            cancelled=cancelled,
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
        cancelled: Callable[[], bool] | None = None,
    ) -> dict:
        if not manual and not self.store.llm_recently_healthy():
            return {
                "status": "skipped",
                "reason": "LLM 最近 24 小时没有成功标注",
                "selected": 0,
            }
        result = self.enrich_pending(
            ids=ids, limit=limit, batch_size=batch_size,
            mode="dead_letter", manual=manual, cancelled=cancelled,
        )
        return {"status": "ok", "selected": result["claimed"], **result}

    def run(self, sources: list[str] | None = None, limit: int = 30,
            skip_llm: bool = False, group: str | None = None,
            cancelled: Callable[[], bool] | None = None) -> dict:
        with NewsPipelineLock(self.store.path):
            return self._run_unlocked(
                sources=sources, limit=limit, skip_llm=skip_llm, group=group,
                cancelled=cancelled,
            )

    def _run_unlocked(self, sources: list[str] | None = None, limit: int = 30,
                      skip_llm: bool = False, group: str | None = None,
                      cancelled: Callable[[], bool] | None = None) -> dict:
        before_id = self.store.max_id()
        configs: list[dict] = []
        if sources:
            for source_id in sources:
                config = self.source_store.get(source_id)
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
            if cancelled is not None and cancelled():
                raise InterruptedError("news crawl cancelled")
            source_id = source["id"]
            run_id = self.source_store.start_run(source_id) if self.source_store.get(source_id) else ""
            try:
                batch = self._fetch_source_batch(source, limit)
                window_id = self.source_store.register_ingest_batch(
                    batch, run_id or uuid.uuid4().hex,
                )
                items = [self._from_fetched(item) for item in batch.articles]
                saved = self.store.save(items)
                if (
                    batch.complete
                    and batch.articles
                    and all(
                        not item.is_official or bool(item.evidence_binding_hash)
                        for item in batch.articles
                    )
                ):
                    self.source_store.complete_evidence_bootstrap(source_id)
                fetched_count += len(items)
                saved_count += saved
                if run_id:
                    run_status = (
                        "success"
                        if batch.complete and batch.health in {"healthy", "not_modified"}
                        else "degraded"
                    )
                    self.source_store.finish_run(
                        run_id, fetched=len(items), saved=saved, pending=saved,
                        status=run_status,
                    )
                self.source_store.record_batch(batch)
                source_results.append({
                    "source": source_id, "fetched": len(items), "saved": saved,
                    "health": batch.health, "watermark": batch.watermark,
                    "complete": batch.complete, "ingest_window_id": window_id,
                })
            except InterruptedError:
                raise
            except Exception as exc:
                logger.warning("资讯来源抓取失败 source=%s", source_id, exc_info=True)
                code = exc.code if isinstance(exc, NewsProviderError) else type(exc).__name__.casefold()
                public_error = f"资讯来源抓取失败（{code}），请查看本机日志"
                errors[source_id] = public_error
                self.source_store.record_failure(source_id, code=code, message=str(exc))
                if run_id:
                    self.source_store.finish_run(run_id, error=public_error)
        annotation: dict[str, Any] = {
            "processed": 0, "completed": 0, "failed": 0,
            "retry_scheduled": 0, "dead_letter": 0,
            "failure_details": [], "completed_ids": [],
        }
        llm_cfg = get_config().llm
        can_annotate = self._client is not None or bool(llm_cfg.api_key or llm_cfg.base_url)
        if cancelled is not None and cancelled():
            raise InterruptedError("news crawl cancelled")
        if (not skip_llm and get_config().news.annotation_enabled and can_annotate):
            annotation = self.enrich_pending(cancelled=cancelled)
        else:
            try:
                self.store.publish_dashboard_materializations()
            except (OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
                logger.warning("资讯 dashboard 物化发布失败", exc_info=True)
        new_ids = [int(item["id"]) for item in self.store.after_id(before_id)]
        return {
            "fetched": fetched_count,
            "saved": saved_count,
            "pending": max(0, saved_count - annotation["completed"]),
            "annotation": annotation, "new_ids": new_ids,
            "sources": source_results, "errors": errors,
        }
