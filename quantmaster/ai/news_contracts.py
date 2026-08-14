"""Strict contracts shared by news providers, storage, and orchestration."""

from __future__ import annotations

import gzip
import hashlib
import hmac
import json
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

from quantmaster.data.cache_contracts import CacheResultKind

HealthStatus = Literal["healthy", "degraded", "failed", "not_modified"]


class NewsProviderError(RuntimeError):
    """A provider failed without producing a trustworthy batch."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "provider_error",
        retryable: bool = True,
        result_kind: CacheResultKind = CacheResultKind.TEMPORARY_FAILURE,
    ):
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.result_kind = result_kind


class NewsContractError(NewsProviderError):
    """The upstream response no longer satisfies its declared schema."""

    def __init__(self, message: str, *, code: str = "contract_error"):
        super().__init__(
            message,
            code=code,
            retryable=False,
            result_kind=CacheResultKind.INVALID_RESPONSE,
        )


@dataclass(slots=True)
class FetchedArticle:
    source: str
    title: str
    content: str
    url: str = ""
    published_at: str = ""
    published_at_epoch: float = 0.0
    fetched_at: float = 0.0
    provider_item_id: str = ""
    is_official: bool = False
    raw_cache_key: str = ""
    content_scope: str = "unknown"
    parser_version: str = "1"
    evidence_binding_hash: str = ""
    ingest_window_id: str = ""
    ingest_batch_id: str = ""


@dataclass(slots=True)
class FetchBatch:
    source_id: str
    articles: list[FetchedArticle] = field(default_factory=list)
    watermark: str = ""
    previous_watermark: str = ""
    health: HealthStatus = "healthy"
    complete: bool = True
    raw_cache_keys: list[str] = field(default_factory=list)
    error_code: str = ""
    message: str = ""
    latest_published_at: float = 0.0
    pending_watermark: str = ""
    next_cursor: str = ""


BUILTIN_SOURCES: tuple[dict[str, Any], ...] = (
    {
        "id": "sina_live", "name": "新浪财经 7×24", "kind": "builtin",
        "group_name": "fast", "url": "https://zhibo.sina.com.cn/api/zhibo/feed",
        "is_official": False, "item_limit": 50, "max_age_hours": 6,
    },
    {
        "id": "eastmoney_fast", "name": "东方财富快讯", "kind": "builtin",
        "group_name": "fast",
        "url": "https://np-weblist.eastmoney.com/comm/web/getFastNewsList",
        "is_official": False, "item_limit": 50, "max_age_hours": 6,
    },
    {
        "id": "jin10_authorized", "name": "金十开放平台（需授权）", "kind": "builtin",
        "group_name": "fast", "url": "https://open.jin10.com/",
        "is_official": False, "item_limit": 50, "max_age_hours": 6,
        "enabled": False, "needs_credentials": True,
    },
    {
        "id": "csrc", "name": "中国证监会要闻", "kind": "builtin",
        "group_name": "official",
        "url": "https://www.csrc.gov.cn/searchList/a1a078ee0bc54721ab6b148884c784a8",
        "is_official": True, "max_age_hours": 336,
    },
    {
        "id": "sse", "name": "上海证券交易所公告", "kind": "builtin",
        "group_name": "official", "url": "https://www.sse.com.cn/disclosure/announcement/general/index.shtml",
        "is_official": True, "max_age_hours": 168,
    },
    {
        "id": "szse", "name": "深圳证券交易所通知公告", "kind": "builtin",
        "group_name": "official",
        "url": "https://www.szse.cn/disclosure/notice/general/index.html",
        "is_official": True, "max_age_hours": 336,
    },
    {
        "id": "pboc", "name": "中国人民银行新闻", "kind": "builtin",
        "group_name": "official", "url": "https://www.pbc.gov.cn/goutongjiaoliu/113456/113469/index.html",
        "is_official": True, "max_age_hours": 168,
    },
    {
        "id": "nbs_release", "name": "国家统计局最新发布", "kind": "builtin",
        "group_name": "periodic", "url": "https://www.stats.gov.cn/sj/zxfb/rss.xml",
        "is_official": True, "max_age_hours": 1080,
    },
    {
        "id": "nbs_interpretation", "name": "国家统计局数据解读", "kind": "builtin",
        "group_name": "periodic", "url": "https://www.stats.gov.cn/sj/sjjd/rss.xml",
        "is_official": True, "max_age_hours": 1080,
    },
    {
        "id": "ndrc", "name": "国家发展改革委新闻发布", "kind": "builtin",
        "group_name": "periodic", "url": "https://www.ndrc.gov.cn/xwdt/xwfb/wap_index.html",
        "is_official": True, "max_age_hours": 336,
    },
)

BUILTIN_SOURCE_IDS = frozenset(item["id"] for item in BUILTIN_SOURCES)

# These hosts are part of the evidence contract, not a best-effort SSRF list.
# An official response may only be archived when every requested/redirected URL
# remains on the source's frozen host set.  Additions require an explicit source
# contract change after the real provider topology has been verified.
BUILTIN_OFFICIAL_ALLOWED_HOSTS: dict[str, frozenset[str]] = {
    "csrc": frozenset({"www.csrc.gov.cn"}),
    "sse": frozenset({"www.sse.com.cn"}),
    "szse": frozenset({"www.szse.cn"}),
    "pboc": frozenset({"www.pbc.gov.cn"}),
    "nbs_release": frozenset({"www.stats.gov.cn"}),
    "nbs_interpretation": frozenset({"www.stats.gov.cn"}),
    "ndrc": frozenset({"www.ndrc.gov.cn"}),
}

_RAW_EVIDENCE_LIMIT = 5 * 1024 * 1024
_NBS_RAW_EVIDENCE_LIMIT = 8 * 1024 * 1024
_RAW_DIGEST = re.compile(r"[0-9a-f]{64}")


def news_content_hash(content: str, title: str) -> str:
    """Return the canonical hash used by both parsed evidence and news rows."""
    text = re.sub(r"\s+", "", (content or title).casefold())
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def article_evidence_binding_hash(
    *,
    source_id: str,
    raw_cache_key: str,
    url: str,
    provider_item_id: str,
    title: str,
    content: str,
    published_at: str,
    published_at_epoch: float,
    content_scope: str,
    parser_version: str,
) -> str:
    """Bind one parsed article projection to one immutable raw response.

    The digest covers every field that can alter the formal event identity or
    its parsed meaning.  Formal SQL recomputes this digest from the persisted
    row, so changing a row or borrowing another item from the same raw feed
    fails closed.
    """
    payload = {
        "contract": "quantmaster.news.article-evidence.v1",
        "source_id": str(source_id),
        "raw_cache_key": str(raw_cache_key).replace("\\", "/"),
        "url": str(url),
        "provider_item_id": str(provider_item_id),
        "title": str(title),
        "content": str(content),
        "content_hash": news_content_hash(str(content), str(title)),
        "published_at": str(published_at),
        "published_at_epoch": float(published_at_epoch),
        "content_scope": str(content_scope),
        "parser_version": str(parser_version),
    }
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def read_raw_evidence(database_path: str | Path, raw_cache_key: str) -> bytes | None:
    """Return a verified official raw blob, or ``None`` for any broken evidence.

    The path is constrained to the database-adjacent ``news_raw`` tree.  The
    decompressed bytes must match the SHA-256 digest embedded in the filename.
    """
    key = str(raw_cache_key or "").replace("\\", "/")
    if not key.startswith("news_raw/") or not key.endswith(".gz"):
        return None
    database = Path(database_path).resolve()
    raw_root = (database.parent / "news_raw").resolve()
    candidate = (database.parent / key).resolve()
    if raw_root not in candidate.parents:
        return None
    digest = candidate.stem.casefold()
    if _RAW_DIGEST.fullmatch(digest) is None:
        return None
    limit = (
        _NBS_RAW_EVIDENCE_LIMIT
        if key.startswith(("news_raw/nbs_release/", "news_raw/nbs_interpretation/"))
        else _RAW_EVIDENCE_LIMIT
    )
    try:
        with gzip.open(candidate, "rb") as handle:
            content = handle.read(limit + 1)
    except (OSError, EOFError, gzip.BadGzipFile):
        return None
    if len(content) > limit:
        return None
    actual = hashlib.sha256(content).hexdigest()
    return content if hmac.compare_digest(actual, digest) else None


def evaluate_freshness(
    source: dict[str, Any],
    latest_published_at: float,
    previous_watermark: str,
    *,
    now: float | None = None,
) -> tuple[HealthStatus, str, str]:
    """Evaluate content age independently from HTTP request success."""
    try:
        max_age_seconds = float(source["max_age_hours"]) * 3600.0
    except (KeyError, TypeError, ValueError) as exc:
        raise NewsContractError(
            f"{source['name']} 未声明新鲜度契约", code="missing_freshness_contract",
        ) from exc
    if max_age_seconds <= 0 or latest_published_at <= 0:
        raise NewsContractError(
            f"{source['name']} 缺少可验证的最新发布时间",
            code="missing_latest_published_at",
        )
    reference = time.time() if now is None else now
    if latest_published_at > reference + 300:
        raise NewsContractError(
            f"{source['name']} 的最新发布时间晚于抓取时点",
            code="future_latest_published_at",
        )
    age_seconds = max(0.0, reference - latest_published_at)
    if age_seconds <= max_age_seconds:
        return "healthy", "", ""
    message = (
        f"最新可信条目已滞后 {age_seconds / 3600.0:.1f} 小时，"
        f"超过 {max_age_seconds / 3600.0:.1f} 小时契约"
    )
    if not previous_watermark:
        raise NewsContractError(message, code="stale_initial_batch")
    return "degraded", "stale_provider", message


def normalize_published_at(value: Any, *, default_timezone: str = "Asia/Shanghai") -> tuple[str, float]:
    """Return an offset-aware ISO timestamp and epoch, or an explicit empty value."""
    if value is None or value == "":
        return "", 0.0
    parsed: datetime | None = None
    if isinstance(value, (int, float)):
        number = float(value)
        # Provider adapters must declare Unix units before reaching this shared
        # boundary.  Guessing seconds versus milliseconds corrupts YYYYMMDD and
        # silently turns provider schema drift into plausible old timestamps.
        if not (100_000_000 <= number < 10_000_000_000):
            raise NewsContractError(
                "数字发布时间缺少明确的 Unix 秒契约",
                code="ambiguous_published_at_unit",
            )
        try:
            parsed = datetime.fromtimestamp(number, UTC)
        except (OSError, OverflowError, ValueError):
            parsed = None
    else:
        text = str(value).strip()
        if text.isdigit():
            raise NewsContractError(
                "纯数字发布时间必须由 provider adapter 按声明格式解析",
                code="ambiguous_published_at_format",
            )
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = parsedate_to_datetime(text)
            except (TypeError, ValueError, OverflowError):
                parsed = None
        if parsed is None:
            for template in ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    parsed = datetime.strptime(text, template)
                    break
                except ValueError:
                    continue
    if parsed is None:
        raise NewsContractError(f"无法解析发布时间：{value!r}", code="invalid_published_at")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(default_timezone))
    parsed = parsed.astimezone(UTC)
    epoch = parsed.timestamp()
    now = datetime.now(UTC).timestamp()
    if epoch > now + 300:
        raise NewsContractError("来源发布时间晚于当前时间", code="future_published_at")
    return parsed.isoformat(timespec="seconds"), epoch
