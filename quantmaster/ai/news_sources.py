"""可配置财经资讯来源、声明式解析器与短期原始响应缓存。"""

from __future__ import annotations

import builtins
import gzip
import hashlib
import ipaddress
import json
import os
import re
import socket
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qsl, urljoin, urlparse

import feedparser
import httpx
from bs4 import BeautifulSoup

from quantmaster.ai.news_contracts import (
    BUILTIN_OFFICIAL_ALLOWED_HOSTS,
    BUILTIN_SOURCE_IDS,
    BUILTIN_SOURCES,
    FetchBatch,
    FetchedArticle,
    NewsContractError,
    article_evidence_binding_hash,
    evaluate_freshness,
    news_content_hash,
    normalize_news_text,
    normalize_published_at,
    read_raw_evidence,
)
from quantmaster.config import get_config
from quantmaster.credentials import CredentialError, CredentialStore
from quantmaster.data.cache_contracts import CacheResultKind
from quantmaster.runtime.sqlite import connect_sqlite

SOURCE_KINDS = {"builtin", "rss", "json", "html"}
SOURCE_GROUPS = {"fast", "official", "periodic"}
MAX_RESPONSE_BYTES = 5 * 1024 * 1024
MAX_BUILTIN_NBS_RESPONSE_BYTES = 8 * 1024 * 1024
FETCH_TIMEOUT = 20.0
MAX_REDIRECTS = 4
PARSER_VERSION = "1"
USER_AGENT = "Mozilla/5.0 (compatible; QuantMaster/0.4; local research workstation)"
BUILTIN_SOURCE_HOSTS = {
    str(item["id"]): str(urlparse(str(item["url"])).hostname or "").lower()
    for item in BUILTIN_SOURCES
}


class NewsFetchStore(Protocol):
    def token(self, source: dict[str, Any]) -> str: ...
    def cache_headers(self, source_id: str, url: str) -> dict[str, str]: ...
    def save_response(
        self, source_id: str, url: str, content: bytes, headers: httpx.Headers,
        status_code: int, *, official: bool = False,
    ) -> str: ...
    def touch_not_modified(self, source_id: str, url: str) -> None: ...
    def cached_response(self, source_id: str, url: str) -> tuple[bytes, str] | None: ...


def _official_host_allowed(source_id: str, url: str) -> bool:
    try:
        parsed = urlparse(str(url))
        hostname = str(parsed.hostname or "").casefold()
        port = parsed.port
    except ValueError:
        return False
    return bool(
        parsed.scheme.casefold() == "https"
        and parsed.username is None
        and parsed.password is None
        and port in {None, 443}
        and hostname in BUILTIN_OFFICIAL_ALLOWED_HOSTS.get(str(source_id), frozenset())
    )


def _require_official_host(source: dict[str, Any], url: str) -> None:
    """Reject official built-in URLs outside their frozen evidence hosts."""
    if not (
        source.get("kind") == "builtin"
        and bool(source.get("is_official"))
    ):
        return
    source_id = str(source.get("id") or "")
    if not _official_host_allowed(source_id, url):
        hostname = str(urlparse(str(url)).hostname or "").casefold() or "<missing>"
        raise NewsContractError(
            f"{source_id} 官方证据 URL 主机不在冻结白名单：{hostname}",
            code="official_host_mismatch",
        )


def _utc_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _clean_text(value: Any) -> str:
    soup = BeautifulSoup(normalize_news_text(value), "html.parser")
    return re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()


def _ingest_article_manifest(
    articles: list[FetchedArticle], source_id: str,
) -> tuple[tuple[tuple[str, str, str, str, str], ...], str]:
    """Freeze one batch's deduplicated article evidence for replay comparison."""
    audit_articles: dict[str, FetchedArticle] = {}
    for item in articles:
        identity = str(
            item.evidence_binding_hash
            or hashlib.sha256(
                f"{source_id}|{item.provider_item_id}|{item.url}".encode(),
            ).hexdigest()
        )
        evidence = (
            str(item.evidence_binding_hash or ""),
            source_id,
            str(item.provider_item_id or ""),
            str(item.raw_cache_key or "").replace("\\", "/"),
        )
        previous = audit_articles.get(identity)
        if previous is not None and evidence != (
            str(previous.evidence_binding_hash or ""),
            source_id,
            str(previous.provider_item_id or ""),
            str(previous.raw_cache_key or "").replace("\\", "/"),
        ):
            raise NewsContractError(
                "资讯抓取批次包含身份相同但证据不同的文章关联",
                code="ingest_batch_article_conflict",
            )
        audit_articles.setdefault(identity, item)
    identities = sorted(audit_articles)
    identity_hash = hashlib.sha256(
        json.dumps(identities, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    manifest = tuple(
        (
            identity,
            str(item.evidence_binding_hash or ""),
            source_id,
            str(item.provider_item_id or ""),
            str(item.raw_cache_key or "").replace("\\", "/"),
        )
        for identity, item in sorted(audit_articles.items())
    )
    return manifest, identity_hash


def _validate_source_dict(value: dict[str, Any], *, creating: bool = False) -> dict[str, Any]:
    result = dict(value)
    kind = str(result.get("kind") or "").strip().lower()
    if kind not in SOURCE_KINDS:
        raise ValueError("来源类型仅支持 builtin/rss/json/html")
    if creating and kind == "builtin":
        raise ValueError("不能创建新的内置来源")
    group = str(result.get("group_name") or "periodic").strip().lower()
    if group not in SOURCE_GROUPS:
        raise ValueError("来源分组仅支持 fast/official/periodic")
    if kind != "builtin" and group == "official":
        raise ValueError("自定义来源不能使用官方分组")
    name = str(result.get("name") or "").strip()
    if not 1 <= len(name) <= 80:
        raise ValueError("来源名称需要 1–80 个字符")
    url = str(result.get("url") or "").strip()
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("来源地址必须是完整的 http(s) URL")
    if parsed.username or parsed.password:
        raise ValueError("来源地址不能包含用户名或密码")
    if any(re.search(r"auth|token|secret|api[-_]?key", key, re.I)
           for key, _value in parse_qsl(parsed.query, keep_blank_values=True)):
        raise ValueError("API Token 不能写入来源地址，请使用专用鉴权字段")
    item_limit = int(result.get("item_limit", 30))
    if not 1 <= item_limit <= 100:
        raise ValueError("单次抓取数量需要在 1–100 之间")
    factor_weight = float(result.get("factor_weight", 1.0))
    if not 0 <= factor_weight <= 3:
        raise ValueError("因子权重需要在 0–3 之间")
    default_max_age = 6.0 if group == "fast" else 1080.0
    max_age_hours = float(result.get("max_age_hours") or default_max_age)
    if not 1 <= max_age_hours <= 8760:
        raise ValueError("来源新鲜度门槛需要在 1–8760 小时之间")
    auth_type = str(result.get("auth_type") or "none").strip().lower()
    if auth_type not in {"none", "bearer", "header"}:
        raise ValueError("鉴权类型仅支持 none/bearer/header")
    auth_header = str(result.get("auth_header") or "").strip()
    if auth_type == "header":
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9-]{0,63}", auth_header):
            raise ValueError("自定义鉴权 Header 名称不合法")
        if auth_header.lower() in {"cookie", "host", "content-length"}:
            raise ValueError("该 Header 不允许用于来源鉴权")
    parser = result.get("parser") or {}
    if not isinstance(parser, dict):
        raise ValueError("解析规则必须是对象")
    if kind == "json":
        for field in ("items_path", "title_path"):
            if not str(parser.get(field) or "").strip():
                raise ValueError(f"JSON 来源需要填写 {field}")
    if kind == "html":
        for field in ("item_selector", "title_selector"):
            if not str(parser.get(field) or "").strip():
                raise ValueError(f"HTML 来源需要填写 {field}")
    if kind != "builtin" and bool(result.get("is_official", False)):
        raise ValueError("自定义来源不能声明为官方来源")
    headers = parser.get("headers") or {}
    if not isinstance(headers, dict):
        raise ValueError("普通请求头必须是对象")
    if len(headers) > 20 or any(
        not re.fullmatch(r"[A-Za-z][A-Za-z0-9-]{0,63}", str(key))
        or len(str(value)) > 1000 for key, value in headers.items()
    ):
        raise ValueError("普通请求头最多 20 个，且名称或内容不能过长")
    forbidden = {"authorization", "proxy-authorization", "cookie", "host", "content-length"}
    if any(str(key).lower() in forbidden for key in headers):
        raise ValueError("敏感请求头必须使用凭据字段，不能写入解析规则")
    if any(re.search(r"auth|token|secret|api[-_]?key", str(key), re.I) for key in headers):
        raise ValueError("可能包含凭据的请求头必须使用专用鉴权字段")
    result.update({
        "kind": kind, "group_name": group, "name": name, "url": url,
        "item_limit": item_limit, "factor_weight": factor_weight,
        "max_age_hours": max_age_hours,
        "auth_type": auth_type, "auth_header": auth_header, "parser": parser,
        "enabled": bool(result.get("enabled", True)),
        "is_official": bool(result.get("is_official", False)) if kind == "builtin" else False,
    })
    return result


class NewsSourceStore:
    """来源配置、抓取运行记录和 HTTP 条件缓存。"""

    def __init__(
        self,
        path: Path | None = None,
        credentials: CredentialStore | None = None,
        *,
        read_only: bool = False,
        initialize: bool = False,
    ):
        self.path = path or get_config().data_root / "news.sqlite"
        self.read_only = bool(read_only)
        database_exists = self.path.is_file()
        if not self.read_only:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        # A page reader must not wake the OS keyring merely to render whether a
        # source was previously configured.  The persisted secret_state is the
        # complete read contract for that view.
        self.credentials = credentials if self.read_only else (credentials or CredentialStore())
        self.raw_root = self.path.parent / "news_raw"
        if not database_exists or initialize:
            if self.read_only:
                raise FileNotFoundError(self.path)
            self._migrate()
            self._seed_builtins()
        else:
            self.require_current(self.path)

    @classmethod
    def initialize_current(cls, path: Path) -> None:
        """Initialize source tables once when the owning news database is new."""
        cls(path, initialize=True)

    @staticmethod
    def require_current(path: Path) -> None:
        required = {
            "news_sources", "news_source_runs", "news_http_cache", "news_raw_manifest",
            "news_article_evidence_manifest", "news_ingest_windows", "news_ingest_batches",
            "news_ingest_batch_articles", "news_ingest_item_queue",
            "news_ingest_failure_diagnostics", "news_source_state",
        }
        connection = connect_sqlite(path, row_factory=True, read_only=True)
        try:
            tables = {
                str(row[0]) for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            missing = sorted(required - tables)
            if missing:
                raise RuntimeError(
                    "资讯来源数据库缺少当前表，需先执行一次性迁移：" + ",".join(missing)
                )
            source_columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(news_sources)")
            }
            state_columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(news_source_state)")
            }
            missing_columns = sorted(
                {"max_age_hours", "needs_credentials"} - source_columns
                | {
                    "latest_published_at", "pending_watermark", "backfill_cursor",
                    "evidence_bootstrap_pending",
                } - state_columns
            )
            if missing_columns:
                raise RuntimeError(
                    "资讯来源数据库缺少当前字段，需先执行一次性迁移："
                    + ",".join(missing_columns)
                )
        finally:
            connection.close()

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(
            self.path,
            timeout=0.25 if self.read_only else 5.0,
            row_factory=True,
            read_only=self.read_only,
        )

    def _migrate(self) -> None:
        with self._conn() as conn:
            had_article_manifest = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='news_article_evidence_manifest'",
            ).fetchone() is not None
            had_ingest_windows = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='news_ingest_windows'",
            ).fetchone() is not None
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS news_sources (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1, group_name TEXT NOT NULL,
                    url TEXT NOT NULL, item_limit INTEGER NOT NULL DEFAULT 30,
                    max_age_hours REAL NOT NULL DEFAULT 0,
                    factor_weight REAL NOT NULL DEFAULT 1, is_official INTEGER NOT NULL DEFAULT 0,
                    parser TEXT NOT NULL DEFAULT '{}', auth_type TEXT NOT NULL DEFAULT 'none',
                    auth_header TEXT NOT NULL DEFAULT '', secret_state TEXT NOT NULL DEFAULT 'none',
                    needs_credentials INTEGER NOT NULL DEFAULT 0,
                    built_in INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS news_source_runs (
                    id TEXT PRIMARY KEY, source_id TEXT NOT NULL, status TEXT NOT NULL,
                    started_at TEXT NOT NULL, finished_at TEXT NOT NULL DEFAULT '',
                    fetched INTEGER NOT NULL DEFAULT 0, saved INTEGER NOT NULL DEFAULT 0,
                    pending INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '');
                CREATE TABLE IF NOT EXISTS news_http_cache (
                    source_id TEXT NOT NULL, url TEXT NOT NULL, etag TEXT NOT NULL DEFAULT '',
                    last_modified TEXT NOT NULL DEFAULT '', raw_cache_key TEXT NOT NULL DEFAULT '',
                    fetched_at REAL NOT NULL DEFAULT 0, status_code INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(source_id,url));
                CREATE TABLE IF NOT EXISTS news_raw_manifest (
                    source_id TEXT NOT NULL,url TEXT NOT NULL,
                    raw_cache_key TEXT NOT NULL,fetched_at REAL NOT NULL,
                    status_code INTEGER NOT NULL,
                    PRIMARY KEY(source_id,url,raw_cache_key));
                CREATE INDEX IF NOT EXISTS idx_news_raw_manifest_key
                    ON news_raw_manifest(source_id,raw_cache_key);
                CREATE TABLE IF NOT EXISTS news_article_evidence_manifest (
                    binding_hash TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,raw_cache_key TEXT NOT NULL,
                    article_url TEXT NOT NULL,provider_item_id TEXT NOT NULL,
                    content_hash TEXT NOT NULL,title TEXT NOT NULL,content TEXT NOT NULL,
                    published_at TEXT NOT NULL,published_at_epoch REAL NOT NULL,
                    content_scope TEXT NOT NULL,parser_version TEXT NOT NULL,
                    bound_at REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS idx_news_article_evidence_lookup
                    ON news_article_evidence_manifest(
                        source_id,raw_cache_key,article_url,provider_item_id,content_hash);
                CREATE TABLE IF NOT EXISTS news_ingest_windows (
                    window_id TEXT PRIMARY KEY,source_id TEXT NOT NULL,
                    previous_watermark TEXT NOT NULL,candidate_watermark TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('pending','complete')),
                    opened_at REAL NOT NULL,completed_at REAL NOT NULL DEFAULT 0,
                    completed_batch_id TEXT NOT NULL DEFAULT '');
                CREATE TABLE IF NOT EXISTS news_ingest_batches (
                    batch_id TEXT PRIMARY KEY,window_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,complete INTEGER NOT NULL,
                    health TEXT NOT NULL,error_code TEXT NOT NULL,
                    article_count INTEGER NOT NULL,article_identity_hash TEXT NOT NULL,
                    recorded_at REAL NOT NULL,
                    FOREIGN KEY(window_id) REFERENCES news_ingest_windows(window_id));
                CREATE INDEX IF NOT EXISTS idx_news_ingest_batches_window
                    ON news_ingest_batches(window_id,recorded_at);
                CREATE TABLE IF NOT EXISTS news_ingest_batch_articles (
                    batch_id TEXT NOT NULL,article_identity TEXT NOT NULL,
                    evidence_binding_hash TEXT NOT NULL,
                    source_id TEXT NOT NULL,provider_item_id TEXT NOT NULL,
                    raw_cache_key TEXT NOT NULL,
                    PRIMARY KEY(batch_id,article_identity),
                    FOREIGN KEY(batch_id) REFERENCES news_ingest_batches(batch_id));
                CREATE INDEX IF NOT EXISTS idx_news_runs_source
                    ON news_source_runs(source_id,started_at DESC);
                CREATE TABLE IF NOT EXISTS news_ingest_item_queue (
                    window_id TEXT NOT NULL,batch_id TEXT NOT NULL,source_id TEXT NOT NULL,
                    item_key TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN
                        ('discovered','detail_pending','detail_fetched','normalized','stored',
                         'analysis_pending','analyzed','failed')),
                    provider_item_id TEXT NOT NULL DEFAULT '',article_url TEXT NOT NULL DEFAULT '',
                    published_at TEXT NOT NULL DEFAULT '',parser_version TEXT NOT NULL DEFAULT '1',
                    attempts INTEGER NOT NULL DEFAULT 0,last_error_code TEXT NOT NULL DEFAULT '',
                    updated_at REAL NOT NULL,
                    PRIMARY KEY(window_id,item_key),
                    FOREIGN KEY(window_id) REFERENCES news_ingest_windows(window_id));
                CREATE INDEX IF NOT EXISTS idx_news_ingest_item_queue_pending
                    ON news_ingest_item_queue(source_id,status,updated_at);
                CREATE TABLE IF NOT EXISTS news_ingest_failure_diagnostics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,source_id TEXT NOT NULL,
                    window_id TEXT NOT NULL DEFAULT '',item_key TEXT NOT NULL DEFAULT '',
                    stage TEXT NOT NULL,diagnostic_code TEXT NOT NULL,
                    raw_response_ref TEXT NOT NULL DEFAULT '',content_type TEXT NOT NULL DEFAULT '',
                    encoding TEXT NOT NULL DEFAULT '',parser_version TEXT NOT NULL DEFAULT '',
                    detail TEXT NOT NULL DEFAULT '',recorded_at REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS idx_news_ingest_failure_diagnostics_source
                    ON news_ingest_failure_diagnostics(source_id,recorded_at DESC);
                CREATE TABLE IF NOT EXISTS news_source_state (
                    source_id TEXT PRIMARY KEY,watermark TEXT NOT NULL DEFAULT '',
                    pending_watermark TEXT NOT NULL DEFAULT '',
                    backfill_cursor TEXT NOT NULL DEFAULT '',
                    latest_published_at REAL NOT NULL DEFAULT 0,
                    health TEXT NOT NULL DEFAULT 'unknown',last_success_at TEXT NOT NULL DEFAULT '',
                    last_attempt_at TEXT NOT NULL DEFAULT '',consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    last_error_code TEXT NOT NULL DEFAULT '',last_error TEXT NOT NULL DEFAULT '',
                    last_raw_cache_key TEXT NOT NULL DEFAULT '',
                    evidence_bootstrap_pending INTEGER NOT NULL DEFAULT 0);
            """)
            columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(news_sources)")}
            if "max_age_hours" not in columns:
                conn.execute(
                    "ALTER TABLE news_sources ADD COLUMN max_age_hours REAL NOT NULL DEFAULT 0",
                )
            if "needs_credentials" not in columns:
                conn.execute(
                    "ALTER TABLE news_sources "
                    "ADD COLUMN needs_credentials INTEGER NOT NULL DEFAULT 0",
                )
            state_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(news_source_state)")
            }
            if "latest_published_at" not in state_columns:
                conn.execute(
                    "ALTER TABLE news_source_state "
                    "ADD COLUMN latest_published_at REAL NOT NULL DEFAULT 0",
                )
            if "pending_watermark" not in state_columns:
                conn.execute(
                    "ALTER TABLE news_source_state "
                    "ADD COLUMN pending_watermark TEXT NOT NULL DEFAULT ''",
                )
            if "backfill_cursor" not in state_columns:
                conn.execute(
                    "ALTER TABLE news_source_state "
                    "ADD COLUMN backfill_cursor TEXT NOT NULL DEFAULT ''",
                )
            if "evidence_bootstrap_pending" not in state_columns:
                conn.execute(
                    "ALTER TABLE news_source_state ADD COLUMN "
                    "evidence_bootstrap_pending INTEGER NOT NULL DEFAULT 0",
                )
            if not had_article_manifest or not had_ingest_windows:
                # A v5 cache may keep returning 304 forever even though its
                # legacy rows have no per-item parser binding.  Persist a
                # bootstrap fence and suppress validators until one complete,
                # newly parsed official window has been bound.  Existing rows
                # and the committed watermark are intentionally untouched.
                conn.execute(
                    "UPDATE news_http_cache SET etag='',last_modified='' "
                    "WHERE source_id IN (SELECT id FROM news_sources "
                    "WHERE built_in=1 AND is_official=1)",
                )
                conn.execute(
                    "INSERT INTO news_source_state(source_id,evidence_bootstrap_pending) "
                    "SELECT id,1 FROM news_sources WHERE built_in=1 AND is_official=1 "
                    "ON CONFLICT(source_id) DO UPDATE SET evidence_bootstrap_pending=1",
                )

    def _seed_builtins(self) -> None:
        now = _utc_iso()
        with self._conn() as conn:
            for item in BUILTIN_SOURCES:
                conn.execute(
                    "INSERT INTO news_sources "
                    "(id,name,kind,enabled,group_name,url,item_limit,max_age_hours,factor_weight,is_official,"
                    "parser,auth_type,auth_header,secret_state,needs_credentials,built_in,"
                    "created_at,updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,'{}','none','','none',?,1,?,?) "
                    "ON CONFLICT(id) DO UPDATE SET name=excluded.name,kind='builtin',"
                    "url=excluded.url,is_official=excluded.is_official,"
                    "needs_credentials=excluded.needs_credentials,built_in=1,"
                    "enabled=CASE WHEN excluded.needs_credentials=1 THEN 0 ELSE enabled END",
                    (item["id"], item["name"], item["kind"],
                     int(item.get("enabled", True)), item["group_name"],
                     item["url"], int(item.get("item_limit", 30)),
                     float(item["max_age_hours"]), 1.0,
                     int(item["is_official"]), int(item.get("needs_credentials", False)),
                     now, now),
                )
                conn.execute(
                    "UPDATE news_sources SET max_age_hours=?,updated_at=? "
                    "WHERE id=? AND built_in=1 AND max_age_hours<=0",
                    (float(item["max_age_hours"]), now, item["id"]),
                )
            conn.execute(
                "UPDATE news_sources SET max_age_hours=336,updated_at=? "
                "WHERE id='ndrc' AND built_in=1 AND max_age_hours=1080 "
                "AND updated_at=created_at",
                (now,),
            )
            placeholders = ",".join("?" for _ in BUILTIN_SOURCE_IDS)
            if placeholders:
                conn.execute(
                    f"UPDATE news_sources SET enabled=0 WHERE built_in=1 AND id NOT IN ({placeholders})",
                    sorted(BUILTIN_SOURCE_IDS),
                )

    @staticmethod
    def _decode(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        result = dict(row)
        result["enabled"] = bool(result["enabled"])
        result["is_official"] = bool(result["is_official"])
        result["built_in"] = bool(result["built_in"])
        result["needs_credentials"] = bool(result.get("needs_credentials"))
        result["factor_eligible"] = bool(
            result["built_in"]
            and result["is_official"]
            and result["id"] in {
                "csrc", "sse", "szse", "pboc", "nbs_release",
                "nbs_interpretation", "ndrc",
            }
        )
        result["parser"] = json.loads(result.get("parser") or "{}")
        result["auth_configured"] = result.get("secret_state") == "keyring"
        return result

    def list(self, *, enabled: bool | None = None, group_name: str | None = None) -> list[dict]:
        where: list[str] = []
        params: list[Any] = []
        if enabled is not None:
            where.append("s.enabled=?")
            params.append(int(enabled))
        if group_name:
            where.append("s.group_name=?")
            params.append(group_name)
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT s.*,(SELECT status FROM news_source_runs r WHERE r.source_id=s.id "
                "ORDER BY r.started_at DESC LIMIT 1) AS last_status,"
                "(SELECT finished_at FROM news_source_runs r WHERE r.source_id=s.id "
                "ORDER BY r.started_at DESC LIMIT 1) AS last_run,"
                "(SELECT error FROM news_source_runs r WHERE r.source_id=s.id "
                "ORDER BY r.started_at DESC LIMIT 1) AS last_run_error,"
                "COALESCE((SELECT fetched FROM news_source_runs r WHERE r.source_id=s.id "
                "ORDER BY r.started_at DESC LIMIT 1),0) AS last_fetched,"
                "COALESCE((SELECT saved FROM news_source_runs r WHERE r.source_id=s.id "
                "ORDER BY r.started_at DESC LIMIT 1),0) AS last_saved,"
                "COALESCE((SELECT pending FROM news_source_runs r WHERE r.source_id=s.id "
                "ORDER BY r.started_at DESC LIMIT 1),0) AS last_pending,"
                "COALESCE(st.health,'unknown') AS health,COALESCE(st.watermark,'') AS watermark,"
                "COALESCE(st.pending_watermark,'') AS pending_watermark,"
                "COALESCE(st.backfill_cursor,'') AS backfill_cursor,"
                "COALESCE(st.latest_published_at,0) AS latest_published_at,"
                "COALESCE(st.consecutive_failures,0) AS consecutive_failures,"
                "COALESCE(st.last_error_code,'') AS last_error_code,"
                "COALESCE(st.last_error,'') AS last_error,"
                "COALESCE(st.last_success_at,'') AS last_success_at,"
                "(SELECT COUNT(*) FROM news_ingest_item_queue q WHERE q.source_id=s.id "
                "AND q.status NOT IN ('stored','analyzed')) AS durable_queue_depth "
                f"FROM news_sources s LEFT JOIN news_source_state st ON st.source_id=s.id "
                f"{clause} ORDER BY built_in DESC,s.rowid",
                params,
            ).fetchall()
        return [self._decode(row) or {} for row in rows]

    def get(self, source_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM news_sources WHERE id=?", (source_id,)).fetchone()
        return self._decode(row)

    def create(self, value: dict[str, Any], *, token: str = "") -> dict[str, Any]:
        item = _validate_source_dict(value, creating=True)
        source_id = f"src_{uuid.uuid4().hex[:16]}"
        now = _utc_iso()
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO news_sources "
                "(id,name,kind,enabled,group_name,url,item_limit,max_age_hours,"
                "factor_weight,is_official,parser,"
                "auth_type,auth_header,secret_state,needs_credentials,built_in,"
                "created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,?,?)",
                (source_id, item["name"], item["kind"], int(item["enabled"]),
                 item["group_name"], item["url"], item["item_limit"], item["max_age_hours"],
                 item["factor_weight"],
                 int(item["is_official"]), json.dumps(item["parser"], ensure_ascii=False),
                 item["auth_type"], item["auth_header"], "none", now, now),
            )
        try:
            if item["auth_type"] != "none":
                if not token.strip():
                    raise ValueError("启用来源鉴权时必须填写 API Token")
                self.set_token(source_id, token)
        except Exception:
            with self._conn() as conn:
                conn.execute("DELETE FROM news_sources WHERE id=?", (source_id,))
            raise
        return self.get(source_id) or {}

    def update(self, source_id: str, value: dict[str, Any], *, token_action: str = "keep",
               token: str = "") -> dict[str, Any]:
        current = self.get(source_id)
        if current is None:
            raise KeyError("资讯来源不存在")
        merged = {**current, **value}
        if current["built_in"]:
            for key in ("name", "kind", "url", "parser", "auth_type", "auth_header",
                        "is_official", "needs_credentials"):
                merged[key] = current[key]
        item = _validate_source_dict(merged)
        if current.get("needs_credentials") and item["enabled"]:
            raise ValueError("该内置来源需要已实现且已配置的授权适配器，当前只能保持停用")
        if (item["auth_type"] != "none" and token_action == "keep"
                and not current.get("auth_configured")):
            raise ValueError("启用来源鉴权时必须填写 API Token")
        now = _utc_iso()
        with self._conn() as conn:
            conn.execute(
                "UPDATE news_sources SET name=?,enabled=?,group_name=?,url=?,item_limit=?,"
                "max_age_hours=?,"
                "factor_weight=?,is_official=?,parser=?,auth_type=?,auth_header=?,updated_at=? "
                "WHERE id=?",
                (item["name"], int(item["enabled"]), item["group_name"], item["url"],
                 item["item_limit"], item["max_age_hours"], item["factor_weight"],
                 int(item["is_official"]),
                 json.dumps(item["parser"], ensure_ascii=False), item["auth_type"],
                 item["auth_header"], now, source_id),
            )
        if item["auth_type"] == "none" or token_action == "clear":
            self.clear_token(source_id)
        elif token_action == "replace":
            if not token.strip():
                raise ValueError("替换来源凭据时必须填写 API Token")
            self.set_token(source_id, token)
        return self.get(source_id) or {}

    def delete(self, source_id: str) -> None:
        current = self.get(source_id)
        if current is None:
            raise KeyError("资讯来源不存在")
        if current["built_in"]:
            raise ValueError("内置来源不能删除，可以将其停用")
        self.clear_token(source_id)
        with self._conn() as conn:
            conn.execute("DELETE FROM news_sources WHERE id=?", (source_id,))

    def set_token(self, source_id: str, token: str) -> None:
        target = CredentialStore.news_source_target(source_id)
        self.credentials.set(target, token.strip())
        with self._conn() as conn:
            conn.execute(
                "UPDATE news_sources SET secret_state='keyring',updated_at=? WHERE id=?",
                (_utc_iso(), source_id),
            )

    def clear_token(self, source_id: str) -> None:
        try:
            self.credentials.delete(CredentialStore.news_source_target(source_id))
        except CredentialError:
            current = self.get(source_id)
            if current and current.get("secret_state") == "keyring":
                raise
        with self._conn() as conn:
            conn.execute(
                "UPDATE news_sources SET secret_state='none',updated_at=? WHERE id=?",
                (_utc_iso(), source_id),
            )

    def token(self, source: dict[str, Any]) -> str:
        if source.get("auth_type") == "none":
            return ""
        if source.get("secret_state") != "keyring":
            raise CredentialError("该来源尚未配置 API Token")
        value = self.credentials.get(CredentialStore.news_source_target(source["id"]))
        if not value:
            raise CredentialError("系统凭据库中没有该来源的 API Token")
        return value

    def start_run(self, source_id: str) -> str:
        run_id = uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO news_source_runs(id,source_id,status,started_at) VALUES (?,?,'running',?)",
                (run_id, source_id, _utc_iso()),
            )
        return run_id

    def state(self, source_id: str) -> dict[str, Any]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM news_source_state WHERE source_id=?", (source_id,),
            ).fetchone()
        return dict(row) if row else {
            "source_id": source_id, "watermark": "", "health": "unknown",
            "pending_watermark": "", "backfill_cursor": "",
            "latest_published_at": 0.0,
            "last_success_at": "", "last_attempt_at": "", "consecutive_failures": 0,
            "last_error_code": "", "last_error": "", "last_raw_cache_key": "",
            "evidence_bootstrap_pending": 0,
        }

    def complete_evidence_bootstrap(self, source_id: str) -> None:
        """Release the conditional-request fence after a fully bound window."""
        with self._conn() as conn:
            conn.execute(
                "UPDATE news_source_state SET evidence_bootstrap_pending=0 WHERE source_id=?",
                (source_id,),
            )

    def _ignore_empty_ingest_batch(
        self, batch: FetchBatch, durable_batch_id: str, closes_empty_gap: bool,
    ) -> bool:
        if batch.articles or closes_empty_gap:
            return False
        # An empty non-boundary batch is normally intentionally ignored. It
        # cannot be a valid replay of a batch that recorded article evidence.
        if durable_batch_id:
            with self._conn() as conn:
                exists = conn.execute(
                    "SELECT 1 FROM news_ingest_batches WHERE batch_id=?",
                    (durable_batch_id,),
                ).fetchone()
            if exists is not None:
                raise NewsContractError(
                    "资讯抓取批次重放与已持久化证据不一致",
                    code="ingest_batch_replay_conflict",
                )
        return True

    @staticmethod
    def _queue_item_key(article: Any) -> str:
        """Return a source business key, never a content-derived identity.

        Provider IDs are authoritative when supplied.  Some HTML/RSS sources do
        not expose one, so the source URL plus publication value is the stable
        recovery key; title is only a last-resort display key.
        """
        provider_id = str(getattr(article, "provider_item_id", "") or "").strip()
        if provider_id:
            return f"id:{provider_id}"
        url = str(getattr(article, "url", "") or "").strip()
        published = str(getattr(article, "published_at", "") or "").strip()
        if url:
            return f"url:{url}|published:{published}"
        return f"title:{str(getattr(article, 'title', '') or '').strip()}|published:{published}"

    @staticmethod
    def _sanitized_response_ref(value: str) -> str:
        """Keep a local evidence reference while removing credential-bearing URLs."""
        raw = str(value or "").strip().replace("\\", "/")
        if not raw:
            return ""
        parsed = urlparse(raw)
        if parsed.scheme and parsed.hostname:
            safe_query = "&".join(
                f"{key}=<redacted>" if re.search(r"token|secret|key|auth|signature", key, re.I)
                else key
                for key, _ in parse_qsl(parsed.query, keep_blank_values=True)
            )
            # Rebuild netloc so userinfo is never retained.
            netloc = parsed.hostname or ""
            if parsed.port:
                netloc = f"{netloc}:{parsed.port}"
            return parsed._replace(netloc=netloc, query=safe_query).geturl()[:500]
        return raw[:500]

    def record_item_diagnostic(
        self, source_id: str, *, stage: str, diagnostic_code: str,
        raw_response_ref: str = "", content_type: str = "", encoding: str = "",
        parser_version: str = "", detail: str = "", window_id: str = "",
        item_key: str = "",
    ) -> None:
        """Persist compact, copy-safe failure evidence; never retain response bodies."""
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO news_ingest_failure_diagnostics("
                "source_id,window_id,item_key,stage,diagnostic_code,raw_response_ref,"
                "content_type,encoding,parser_version,detail,recorded_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (source_id, window_id, item_key, stage[:40], diagnostic_code[:80],
                 self._sanitized_response_ref(raw_response_ref), content_type[:120],
                 encoding[:80], parser_version[:80], detail[:500], time.time()),
            )

    def pending_ingest_items(self, source_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        """Durable recovery view; completed items are intentionally never returned."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM news_ingest_item_queue WHERE source_id=? "
                "AND status NOT IN ('stored','analyzed') ORDER BY updated_at,item_key LIMIT ?",
                (source_id, max(1, int(limit))),
            ).fetchall()
        return [dict(row) for row in rows]

    def ingest_queue_depth(self, source_id: str) -> int:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM news_ingest_item_queue WHERE source_id=? "
                "AND status NOT IN ('stored','analyzed')", (source_id,),
            ).fetchone()
        return int(row[0] if row else 0)

    def register_ingest_batch(self, batch: FetchBatch, batch_id: str) -> str:
        """Persist an immutable provider batch and attach its durable window identity."""
        durable_batch_id = str(batch_id or uuid.uuid4().hex)
        closes_empty_gap = bool(
            batch.complete
            and batch.previous_watermark
            and batch.watermark
            and batch.watermark != batch.previous_watermark
        )
        if self._ignore_empty_ingest_batch(
            batch, durable_batch_id if batch_id else "", closes_empty_gap,
        ):
            return ""
        source_id = str(batch.source_id or "")
        if not source_id:
            raise NewsContractError("资讯批次缺少来源身份", code="missing_batch_source")
        candidate = str(
            batch.pending_watermark
            or batch.watermark
            or (batch.articles[0].provider_item_id if batch.articles else "")
            or (batch.articles[0].evidence_binding_hash if batch.articles else "")
        )
        previous = str(batch.previous_watermark or "")
        if not candidate:
            raise NewsContractError("资讯批次缺少窗口头身份", code="missing_window_head")
        window_payload = json.dumps(
            {
                "contract": "quantmaster.news.ingest-window.v1",
                "source_id": source_id,
                "previous_watermark": previous,
                "candidate_watermark": candidate,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        window_id = hashlib.sha256(window_payload.encode("utf-8")).hexdigest()
        article_manifest, identity_hash = _ingest_article_manifest(batch.articles, source_id)
        batch_metadata = (
            window_id,
            source_id,
            int(batch.complete),
            str(batch.health),
            str(batch.error_code or ""),
            len(article_manifest),
            identity_hash,
        )
        recorded_at = time.time()
        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO news_ingest_windows("
                "window_id,source_id,previous_watermark,candidate_watermark,status,opened_at) "
                "VALUES (?,?,?,?,'pending',?)",
                (window_id, source_id, previous, candidate, recorded_at),
            )
            window = conn.execute(
                "SELECT source_id,previous_watermark,candidate_watermark "
                "FROM news_ingest_windows WHERE window_id=?",
                (window_id,),
            ).fetchone()
            if window is None or tuple(window) != (source_id, previous, candidate):
                raise NewsContractError(
                    "资讯抓取窗口身份发生不可解释冲突", code="ingest_window_conflict",
                )
            existing_batch = conn.execute(
                "SELECT window_id,source_id,complete,health,error_code,article_count,"
                "article_identity_hash FROM news_ingest_batches WHERE batch_id=?",
                (durable_batch_id,),
            ).fetchone()
            if existing_batch is None:
                conn.execute(
                    "INSERT INTO news_ingest_batches("
                    "batch_id,window_id,source_id,complete,health,error_code,article_count,"
                    "article_identity_hash,recorded_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (durable_batch_id, *batch_metadata, recorded_at),
                )
                conn.executemany(
                    "INSERT INTO news_ingest_batch_articles("
                    "batch_id,article_identity,evidence_binding_hash,source_id,provider_item_id,"
                    "raw_cache_key) VALUES (?,?,?,?,?,?)",
                    ((durable_batch_id, *article) for article in article_manifest),
                )
            else:
                existing_manifest = tuple(
                    tuple(row) for row in conn.execute(
                        "SELECT article_identity,evidence_binding_hash,source_id,provider_item_id,"
                        "raw_cache_key FROM news_ingest_batch_articles WHERE batch_id=? "
                        "ORDER BY article_identity",
                        (durable_batch_id,),
                    )
                )
                if tuple(existing_batch) != batch_metadata or existing_manifest != article_manifest:
                    raise NewsContractError(
                        "资讯抓取批次重放与已持久化证据不一致",
                        code="ingest_batch_replay_conflict",
                    )
            if closes_empty_gap:
                conn.execute(
                    "UPDATE news_ingest_windows SET status='complete',completed_at=?,"
                    "completed_batch_id=? WHERE window_id=? AND status='pending'",
                    (recorded_at, durable_batch_id, window_id),
                )
            for article in batch.articles:
                item_key = self._queue_item_key(article)
                existing_item = conn.execute(
                    "SELECT provider_item_id,article_url,published_at FROM news_ingest_item_queue "
                    "WHERE window_id=? AND item_key=?",
                    (window_id, item_key),
                ).fetchone()
                values = (
                    window_id, durable_batch_id, source_id, item_key, "normalized",
                    str(article.provider_item_id or ""), str(article.url or ""),
                    str(article.published_at or ""), str(article.parser_version or "1"),
                    recorded_at,
                )
                if existing_item is not None and tuple(existing_item) != values[5:8]:
                    raise NewsContractError(
                        "资讯恢复队列业务键对应的来源字段发生冲突",
                        code="ingest_item_identity_conflict",
                    )
                conn.execute(
                    "INSERT INTO news_ingest_item_queue("
                    "window_id,batch_id,source_id,item_key,status,provider_item_id,article_url,"
                    "published_at,parser_version,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(window_id,item_key) DO UPDATE SET "
                    "batch_id=excluded.batch_id,parser_version=excluded.parser_version,"
                    "updated_at=CASE WHEN news_ingest_item_queue.status IN ('stored','analyzed') "
                    "THEN news_ingest_item_queue.updated_at ELSE excluded.updated_at END",
                    values,
                )
        for article in batch.articles:
            article.ingest_window_id = window_id
            article.ingest_batch_id = durable_batch_id
        return window_id

    def complete_ingest_window(self, window_id: str, batch_id: str) -> None:
        """Monotonically release a window only after its complete batch was persisted."""
        with self._conn() as conn:
            batch = conn.execute(
                "SELECT article_count FROM news_ingest_batches WHERE batch_id=? "
                "AND window_id=? AND complete=1",
                (batch_id, window_id),
            ).fetchone()
            if batch is None:
                raise NewsContractError(
                    "不能用不完整批次提交资讯窗口", code="incomplete_window_commit",
                )
            news_table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='news'",
            ).fetchone()
            persisted = 0
            if news_table is not None:
                persisted = int(conn.execute(
                    "SELECT COUNT(*) FROM news WHERE ingest_window_id=? AND ingest_batch_id=?",
                    (window_id, batch_id),
                ).fetchone()[0])
            if persisted < int(batch["article_count"]):
                raise NewsContractError(
                    "完整资讯批次尚有 durable queue 条目未落盘，拒绝释放正式窗口",
                    code="window_articles_not_persisted",
                )
            remaining = conn.execute(
                "SELECT COUNT(*) FROM news_ingest_item_queue WHERE window_id=? "
                "AND status NOT IN ('stored','analyzed')", (window_id,),
            ).fetchone()
            if int(remaining[0] if remaining else 0):
                raise NewsContractError(
                    "资讯窗口仍有未完成的 durable queue 条目，拒绝推进水位",
                    code="window_pending_items",
                )
            conn.execute(
                "UPDATE news_ingest_windows SET status='complete',completed_at=?,"
                "completed_batch_id=? WHERE window_id=? AND status='pending'",
                (time.time(), batch_id, window_id),
            )

    def complete_persisted_ingest_batches(
        self, identities: set[tuple[str, str]],
    ) -> None:
        """Release only identities whose immutable provider batch says complete."""
        for window_id, batch_id in sorted(identities):
            with self._conn() as conn:
                complete = conn.execute(
                    "SELECT complete FROM news_ingest_batches WHERE window_id=? AND batch_id=?",
                    (window_id, batch_id),
                ).fetchone()
                pending = conn.execute(
                    "SELECT 1 FROM news_ingest_item_queue WHERE window_id=? "
                    "AND status NOT IN ('stored','analyzed') LIMIT 1", (window_id,),
                ).fetchone()
            # Partial commits are an expected crash-recovery state.  Do not
            # turn them into a failed write; leave the window fenced until the
            # remaining business-key entries are durably stored.
            if complete is not None and bool(complete["complete"]) and pending is None:
                self.complete_ingest_window(window_id, batch_id)

    def mark_ingest_items_stored(self, items: builtins.list[Any]) -> None:
        """Advance each normalized item independently after its news row commits.

        This is deliberately separate from window completion: an interruption in
        the tail of a batch leaves the already committed rows durable and only
        the remaining queue entries recoverable.
        """
        if not items:
            return
        with self._conn() as conn:
            for item in items:
                window_id = str(getattr(item, "ingest_window_id", "") or "")
                if not window_id:
                    continue
                key = self._queue_item_key(item)
                changed = conn.execute(
                    "UPDATE news_ingest_item_queue SET status='stored',updated_at=?,"
                    "last_error_code='' WHERE window_id=? AND item_key=? "
                    "AND status NOT IN ('stored','analyzed')",
                    (time.time(), window_id, key),
                ).rowcount
                if not changed:
                    exists = conn.execute(
                        "SELECT status FROM news_ingest_item_queue WHERE window_id=? AND item_key=?",
                        (window_id, key),
                    ).fetchone()
                    if exists is None:
                        raise NewsContractError(
                            "持久化资讯未在 durable queue 中登记",
                            code="ingest_item_queue_missing",
                        )

    def mark_ingest_item_failed(
        self, item: Any, *, stage: str, diagnostic_code: str, detail: str = "",
    ) -> None:
        window_id = str(getattr(item, "ingest_window_id", "") or "")
        if not window_id:
            return
        key = self._queue_item_key(item)
        with self._conn() as conn:
            conn.execute(
                "UPDATE news_ingest_item_queue SET status='failed',attempts=attempts+1,"
                "last_error_code=?,updated_at=? WHERE window_id=? AND item_key=?",
                (diagnostic_code[:80], time.time(), window_id, key),
            )
        self.record_item_diagnostic(
            str(getattr(item, "source", "") or ""), stage=stage,
            diagnostic_code=diagnostic_code, raw_response_ref=str(
                getattr(item, "raw_cache_key", "") or getattr(item, "url", "") or ""
            ), parser_version=str(getattr(item, "parser_version", "") or ""),
            detail=detail, window_id=window_id, item_key=key,
        )

    def record_batch(self, batch: FetchBatch) -> None:
        now = _utc_iso()
        raw_key = batch.raw_cache_keys[-1] if batch.raw_cache_keys else ""
        committed_watermark = batch.watermark if batch.complete else batch.previous_watermark
        health = batch.health
        error_code = batch.error_code
        message = batch.message
        if not batch.complete and health in {"healthy", "not_modified"}:
            health = "degraded"
            error_code = error_code or "watermark_not_reached"
            message = message or "批次不完整，拒绝推进 committed 水位"
        with self._conn() as conn:
            pending_queue = conn.execute(
                "SELECT COUNT(*) FROM news_ingest_item_queue WHERE source_id=? "
                "AND status NOT IN ('stored','analyzed')", (batch.source_id,),
            ).fetchone()
            if batch.complete and int(pending_queue[0] if pending_queue else 0):
                # The queue is the recovery fence.  A complete provider page is
                # not enough to release a watermark if its durable items were
                # not all committed.
                committed_watermark = batch.previous_watermark
                health = "degraded"
                error_code = "durable_queue_pending"
                message = "存在尚未落盘的资讯条目，watermark 保持不变"
            conn.execute(
                "INSERT INTO news_source_state("
                "source_id,watermark,pending_watermark,backfill_cursor,"
                "latest_published_at,health,last_success_at,"
                "last_attempt_at,consecutive_failures,last_error_code,last_error,last_raw_cache_key) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET "
                "watermark=CASE WHEN excluded.watermark<>'' THEN excluded.watermark ELSE watermark END,"
                "pending_watermark=excluded.pending_watermark,"
                "backfill_cursor=excluded.backfill_cursor,"
                "latest_published_at=CASE WHEN excluded.latest_published_at>0 "
                "THEN excluded.latest_published_at ELSE latest_published_at END,"
                "health=excluded.health,last_success_at=CASE WHEN excluded.health IN "
                "('healthy','not_modified') THEN excluded.last_attempt_at ELSE last_success_at END,"
                "last_attempt_at=excluded.last_attempt_at,consecutive_failures=CASE WHEN "
                "excluded.health IN ('healthy','not_modified') THEN 0 ELSE consecutive_failures+1 END,"
                "last_error_code=excluded.last_error_code,last_error=excluded.last_error,"
                "last_raw_cache_key=CASE WHEN excluded.last_raw_cache_key<>'' "
                "THEN excluded.last_raw_cache_key ELSE last_raw_cache_key END",
                (
                    batch.source_id, committed_watermark, batch.pending_watermark,
                    batch.next_cursor, batch.latest_published_at,
                    health, now if health in {"healthy", "not_modified"} else "", now,
                    0 if health in {"healthy", "not_modified"} else 1,
                    error_code[:80], message[:1000], raw_key,
                ),
            )

    def record_failure(self, source_id: str, *, code: str, message: str) -> None:
        now = _utc_iso()
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO news_source_state(source_id,health,last_attempt_at,consecutive_failures,"
                "last_error_code,last_error) VALUES (?,'failed',?,1,?,?) "
                "ON CONFLICT(source_id) DO UPDATE SET health='failed',"
                "last_attempt_at=excluded.last_attempt_at,"
                "consecutive_failures=consecutive_failures+1,last_error_code=excluded.last_error_code,"
                "last_error=excluded.last_error",
                (source_id, now, code[:80], message[:1000]),
            )

    def finish_run(self, run_id: str, *, fetched: int = 0, saved: int = 0,
                   pending: int = 0, error: str = "", status: str = "") -> None:
        run_status = status if status in {"success", "degraded"} else (
            "failed" if error else "success"
        )
        with self._conn() as conn:
            conn.execute(
                "UPDATE news_source_runs SET status=?,finished_at=?,fetched=?,saved=?,pending=?,"
                "error=? WHERE id=?",
                (run_status, _utc_iso(), fetched, saved, pending,
                 error[:1000], run_id),
            )

    def cache_headers(self, source_id: str, url: str) -> dict[str, str]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT h.etag,h.last_modified,"
                "COALESCE(st.evidence_bootstrap_pending,0) AS evidence_bootstrap_pending "
                "FROM news_http_cache h LEFT JOIN news_source_state st "
                "ON st.source_id=h.source_id WHERE h.source_id=? AND h.url=?",
                (source_id, url),
            ).fetchone()
        if not row or bool(row["evidence_bootstrap_pending"]):
            return {}
        result = {}
        if row["etag"]:
            result["If-None-Match"] = row["etag"]
        if row["last_modified"]:
            result["If-Modified-Since"] = row["last_modified"]
        return result

    def save_response(self, source_id: str, url: str, content: bytes,
                      headers: httpx.Headers, status_code: int, *, official: bool = False) -> str:
        if official:
            source = self.get(source_id)
            if not (
                source
                and source.get("built_in")
                and source.get("is_official")
                and _official_host_allowed(source_id, url)
            ):
                raise NewsContractError(
                    f"{source_id} 响应不能作为官方证据归档：URL 主机不在冻结白名单",
                    code="official_host_mismatch",
                )
        digest = hashlib.sha256(content).hexdigest()
        key = f"sha256:{digest}"
        if official:
            stamp = datetime.now(UTC).strftime("%Y-%m-%d")
            directory = self.raw_root / source_id / stamp
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"{digest}.gz"
            key = str(path.relative_to(self.path.parent)).replace("\\", "/")
            if read_raw_evidence(self.path, key) is None:
                temporary = directory / f".{digest}.{uuid.uuid4().hex}.tmp"
                try:
                    with temporary.open("xb") as raw_handle:
                        with gzip.GzipFile(
                            filename="", mode="wb", fileobj=raw_handle, mtime=0,
                        ) as gzip_handle:
                            gzip_handle.write(content)
                        raw_handle.flush()
                        os.fsync(raw_handle.fileno())
                    os.replace(temporary, path)
                finally:
                    temporary.unlink(missing_ok=True)
                if read_raw_evidence(self.path, key) is None:
                    raise OSError("官方资讯原始证据写入后校验失败")
        with self._conn() as conn:
            if official:
                conn.execute(
                    "INSERT OR IGNORE INTO news_raw_manifest("
                    "source_id,url,raw_cache_key,fetched_at,status_code) "
                    "VALUES (?,?,?,?,?)",
                    (source_id, url, key, time.time(), status_code),
                )
            conn.execute(
                "INSERT INTO news_http_cache(source_id,url,etag,last_modified,raw_cache_key,"
                "fetched_at,status_code) VALUES (?,?,?,?,?,?,?) ON CONFLICT(source_id,url) "
                "DO UPDATE SET etag=excluded.etag,last_modified=excluded.last_modified,"
                "raw_cache_key=excluded.raw_cache_key,fetched_at=excluded.fetched_at,"
                "status_code=excluded.status_code",
                (source_id, url, headers.get("etag", ""), headers.get("last-modified", ""),
                 key, time.time(), status_code),
            )
        return key

    def bind_articles(self, articles: builtins.list[Any]) -> None:
        """Append exact raw-to-parser bindings for official article projections.

        This method must be called by the fetch pipeline after parsing and
        before the rows are persisted.  It deliberately does not infer or
        backfill bindings from legacy rows: evidence that was not bound at
        parse time is ineligible for formal factors.
        """
        official_articles = [item for item in articles if bool(item.is_official)]
        if not official_articles:
            return
        bound_at = time.time()
        with self._conn() as conn:
            for article in official_articles:
                source_id = str(article.source)
                raw_key = str(article.raw_cache_key or "").replace("\\", "/")
                article_url = str(article.url or "")
                provider_item_id = str(article.provider_item_id or "")
                parser_version = str(getattr(article, "parser_version", "1") or "1")
                source = conn.execute(
                    "SELECT built_in,is_official FROM news_sources WHERE id=?",
                    (source_id,),
                ).fetchone()
                if not (
                    source
                    and bool(source["built_in"])
                    and bool(source["is_official"])
                    and _official_host_allowed(source_id, article_url)
                    and raw_key.startswith(f"news_raw/{source_id}/")
                    and read_raw_evidence(self.path, raw_key) is not None
                ):
                    raise NewsContractError(
                        f"{source_id} 条目缺少同源、同主机的可恢复官方原始证据",
                        code="article_evidence_invalid",
                    )
                raw_rows = conn.execute(
                    "SELECT url,status_code FROM news_raw_manifest WHERE source_id=? "
                    "AND raw_cache_key=? ORDER BY fetched_at",
                    (source_id, raw_key),
                ).fetchall()
                current_fetch = conn.execute(
                    "SELECT url,status_code FROM news_http_cache WHERE source_id=? "
                    "AND raw_cache_key=? AND status_code BETWEEN 200 AND 299 "
                    "ORDER BY fetched_at DESC LIMIT 1",
                    (source_id, raw_key),
                ).fetchone()
                has_original_manifest = any(
                    _official_host_allowed(source_id, str(row["url"]))
                    and 200 <= int(row["status_code"]) < 400
                    and int(row["status_code"]) != 304
                    for row in raw_rows
                )
                has_fresh_200 = bool(
                    current_fetch
                    and _official_host_allowed(source_id, str(current_fetch["url"]))
                )
                if not raw_rows or not (has_original_manifest or has_fresh_200):
                    raise NewsContractError(
                        f"{source_id} 条目原始证据没有追加式 HTTP 清单记录",
                        code="article_evidence_unmanifested",
                    )
                binding_hash = article_evidence_binding_hash(
                    source_id=source_id,
                    raw_cache_key=raw_key,
                    url=article_url,
                    provider_item_id=provider_item_id,
                    title=str(article.title),
                    content=str(article.content),
                    published_at=str(article.published_at or ""),
                    published_at_epoch=float(article.published_at_epoch or 0.0),
                    content_scope=str(article.content_scope or "unknown"),
                    parser_version=parser_version,
                )
                content_hash = news_content_hash(str(article.content), str(article.title))
                values = (
                    binding_hash, source_id, raw_key, article_url, provider_item_id,
                    content_hash, str(article.title), str(article.content),
                    str(article.published_at or ""), float(article.published_at_epoch or 0.0),
                    str(article.content_scope or "unknown"), parser_version, bound_at,
                )
                conn.execute(
                    "INSERT OR IGNORE INTO news_article_evidence_manifest("
                    "binding_hash,source_id,raw_cache_key,article_url,provider_item_id,"
                    "content_hash,title,content,published_at,published_at_epoch,"
                    "content_scope,parser_version,bound_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    values,
                )
                recorded = conn.execute(
                    "SELECT source_id,raw_cache_key,article_url,provider_item_id,content_hash,"
                    "title,content,published_at,published_at_epoch,content_scope,parser_version "
                    "FROM news_article_evidence_manifest WHERE binding_hash=?",
                    (binding_hash,),
                ).fetchone()
                if recorded is None or tuple(recorded) != values[1:-1]:
                    raise NewsContractError(
                        f"{source_id} 条目证据绑定清单发生不可解释冲突",
                        code="article_evidence_conflict",
                    )
                article.parser_version = parser_version
                article.evidence_binding_hash = binding_hash

    def touch_not_modified(self, source_id: str, url: str) -> None:
        fetched_at = time.time()
        with self._conn() as conn:
            conn.execute(
                "UPDATE news_http_cache SET fetched_at=?,status_code=304 WHERE source_id=? AND url=?",
                (fetched_at, source_id, url),
            )
            row = conn.execute(
                "SELECT h.raw_cache_key FROM news_http_cache h "
                "JOIN news_sources s ON s.id=h.source_id "
                "WHERE h.source_id=? AND h.url=? AND s.built_in=1 AND s.is_official=1",
                (source_id, url),
            ).fetchone()
            key = str(row["raw_cache_key"] or "") if row else ""
            if (
                key.startswith(f"news_raw/{source_id}/")
                and _official_host_allowed(source_id, url)
                and read_raw_evidence(self.path, key) is not None
            ):
                conn.execute(
                    "INSERT OR IGNORE INTO news_raw_manifest("
                    "source_id,url,raw_cache_key,fetched_at,status_code) "
                    "VALUES (?,?,?,?,304)",
                    (source_id, url, key, fetched_at),
                )

    def cached_response(self, source_id: str, url: str) -> tuple[bytes, str] | None:
        """Recover a previously archived official response after an HTTP 304."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT raw_cache_key FROM news_http_cache WHERE source_id=? AND url=?",
                (source_id, url),
            ).fetchone()
        key = str(row["raw_cache_key"] or "") if row else ""
        content = read_raw_evidence(self.path, key)
        return (content, key) if content is not None else None

    def cleanup_raw(self, retention_days: int) -> int:
        """Fail closed: raw evidence GC stays disabled until reference-safe GC is approved."""
        self.raw_gc_candidates(retention_days)
        return 0

    def raw_gc_candidates(self, retention_days: int) -> builtins.list[str]:
        """Report old, unreferenced evidence without mutating the evidence store."""
        cutoff = time.time() - max(0, retention_days) * 86400
        referenced: set[str] = set()
        with self._conn() as conn:
            tables = {
                str(row[0])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            for table in ("news", "news_revisions"):
                if table not in tables:
                    continue
                referenced.update(
                    str(row[0]).replace("\\", "/")
                    for row in conn.execute(
                        f"SELECT DISTINCT raw_cache_key FROM {table} "
                        "WHERE raw_cache_key LIKE 'news_raw/%'",
                    )
                    if row[0]
                )
        root = self.raw_root.resolve()
        if not root.is_dir():
            return []
        candidates = []
        for path in root.rglob("*.gz"):
            resolved = path.resolve()
            if root not in resolved.parents or path.stat().st_mtime >= cutoff:
                continue
            key = str(path.relative_to(self.path.parent)).replace("\\", "/")
            if key not in referenced:
                candidates.append(key)
        return sorted(candidates)


def _ensure_public_url(url: str, *, allow_fake_ip: bool = False) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("来源地址必须是完整的 http(s) URL")
    hostname = parsed.hostname.strip("[]").lower()
    if hostname == "localhost" or hostname.endswith(".localhost"):
        raise ValueError("资讯来源不能访问本机或私有网络地址")
    try:
        literal_address = ipaddress.ip_address(hostname)
    except ValueError:
        literal_address = None
    fake_ip_network = ipaddress.ip_network("198.18.0.0/15")
    try:
        default_port = 443 if parsed.scheme == "https" else 80
        addresses = {
            item[4][0] for item in socket.getaddrinfo(hostname, parsed.port or default_port)
        }
    except socket.gaierror as exc:
        raise ValueError(f"无法解析来源域名：{hostname}") from exc
    for value in addresses:
        address = ipaddress.ip_address(value)
        # Clash-style TUN DNS intentionally maps public hostnames into the
        # RFC 2544 benchmark range.  Only frozen built-in hosts may request
        # this exception; custom sources and literal 198.18/15 URLs remain
        # blocked so a proxy cannot bypass the SSRF boundary.
        if allow_fake_ip and literal_address is None and address in fake_ip_network:
            continue
        if (address.is_private or address.is_loopback or address.is_link_local or
                address.is_multicast or address.is_reserved or address.is_unspecified):
            raise ValueError("资讯来源不能访问本机或私有网络地址")


def _request_headers(source: dict[str, Any], token: str) -> dict[str, str]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, application/xml, text/html;q=0.9, */*;q=0.5",
    }
    headers.update({str(k): str(v) for k, v in (source.get("parser", {}).get("headers") or {}).items()})
    if source.get("auth_type") == "bearer":
        headers["Authorization"] = f"Bearer {token}"
    elif source.get("auth_type") == "header":
        headers[source.get("auth_header") or "X-API-Key"] = token
    return headers


def _origin(url: str) -> tuple[str, str, int]:
    parsed = urlparse(url)
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return parsed.scheme.lower(), (parsed.hostname or "").lower(), port


def _without_auth(headers: dict[str, str], source: dict[str, Any]) -> dict[str, str]:
    result = dict(headers)
    result.pop("Authorization", None)
    if source.get("auth_type") == "header":
        result.pop(source.get("auth_header") or "X-API-Key", None)
    return result


def _allow_builtin_fake_ip(source: dict[str, Any], url: str) -> bool:
    source_id = str(source.get("id") or "")
    hostname = str(urlparse(url).hostname or "").lower()
    return (
        source.get("kind") == "builtin"
        and source_id in BUILTIN_SOURCE_IDS
        and hostname == BUILTIN_SOURCE_HOSTS.get(source_id)
    )


def _validate_http_representation(
    source: dict[str, Any], content: bytes, headers: httpx.Headers
) -> None:
    if not content.strip():
        raise NewsContractError("资讯来源返回空响应", code="empty_response")

    media_type = headers.get("content-type", "").partition(";")[0].strip().casefold()
    source_id = str(source.get("id") or "")
    kind = str(source.get("kind") or "")
    expects_json = kind == "json" or source_id in {
        "sina_live", "eastmoney_fast", "jin10_authorized", "csrc",
    }
    expects_xml = kind == "rss" or source_id in {"nbs_release", "nbs_interpretation"}
    expected = "json" if expects_json else "xml" if expects_xml else "html"
    if media_type and expected not in media_type and not (
        expected == "xml" and media_type in {"application/rss+xml", "text/xml"}
    ):
        raise NewsContractError(
            f"资讯来源 Content-Type 与声明不一致: {media_type}", code="unexpected_media_type"
        )

    prefix = content[:4096].lstrip().lower()
    looks_html = prefix.startswith((b"<!doctype html", b"<html"))
    if looks_html and expected != "html":
        raise NewsContractError("资讯接口返回了 HTML 页面", code="html_interstitial")
    if not looks_html:
        return
    soup = BeautifulSoup(content, "html.parser")
    visible = " ".join(
        node.get_text(" ", strip=True) for node in soup.select("title, h1, h2, form")
    ).casefold()
    markers = {
        CacheResultKind.PERMISSION_DENIED: ("access denied", "permission denied", "无权限", "禁止访问"),
        CacheResultKind.INVALID_RESPONSE: (
            "captcha", "验证码", "login", "登录", "upstream error", "service unavailable",
        ),
    }
    for result_kind, values in markers.items():
        if any(value in visible for value in values):
            error = NewsContractError("资讯来源返回拦截或错误页面", code="provider_interstitial")
            error.result_kind = result_kind
            raise error


def _fetch_bytes(source: dict[str, Any], url: str, store: NewsFetchStore,
                 *, preview: bool = False) -> tuple[bytes | None, str, str]:
    current = url
    _require_official_host(source, current)
    token = store.token(source)
    base_headers = _request_headers(source, token)
    headers = dict(base_headers)
    if _origin(current) != _origin(source["url"]):
        headers = _without_auth(headers, source)
    if not preview and bool(source.get("_conditional_cache", True)):
        headers.update(store.cache_headers(source["id"], current))
    response_limit = (
        MAX_BUILTIN_NBS_RESPONSE_BYTES
        if source.get("kind") == "builtin"
        and source.get("id") in {"nbs_release", "nbs_interpretation"}
        else MAX_RESPONSE_BYTES
    )
    with httpx.Client(timeout=FETCH_TIMEOUT, follow_redirects=False) as client:
        for _ in range(MAX_REDIRECTS + 1):
            _require_official_host(source, current)
            _ensure_public_url(
                current,
                allow_fake_ip=_allow_builtin_fake_ip(source, current),
            )
            for attempt in range(2):
                try:
                    with client.stream("GET", current, headers=headers) as response:
                        if response.status_code in {301, 302, 303, 307, 308}:
                            location = response.headers.get("location")
                            if not location:
                                response.raise_for_status()
                            current = urljoin(current, location)
                            _require_official_host(source, current)
                            headers = dict(base_headers)
                            if _origin(current) != _origin(source["url"]):
                                headers = _without_auth(headers, source)
                            break
                        if response.status_code == 304 and not preview:
                            store.touch_not_modified(source["id"], current)
                            return None, current, ""
                        response.raise_for_status()
                        chunks, size = [], 0
                        for chunk in response.iter_bytes():
                            size += len(chunk)
                            if size > response_limit:
                                raise ValueError(
                                    f"来源响应超过 {response_limit // (1024 * 1024)}MB 安全上限"
                                )
                            chunks.append(chunk)
                        content = b"".join(chunks)
                        _validate_http_representation(source, content, response.headers)
                        key = "" if preview else store.save_response(
                            source["id"], current, content, response.headers,
                            response.status_code, official=bool(source.get("is_official")),
                        )
                        return content, current, key
                except httpx.TransportError:
                    if attempt:
                        raise
                    time.sleep(1)
    raise ValueError("来源重定向次数超过安全上限")


def _path_value(value: Any, path: str) -> Any:
    current = value
    for part in [item for item in str(path).split(".") if item]:
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            index = int(part)
            current = current[index] if index < len(current) else None
        else:
            return None
    return current


def _parse_rss(source: dict[str, Any], content: bytes, base_url: str,
               raw_key: str) -> list[FetchedArticle]:
    feed = feedparser.parse(content)
    result = []
    for entry in feed.entries[:source["item_limit"]]:
        payload = ""
        if getattr(entry, "content", None):
            payload = entry.content[0].get("value", "")
        payload = payload or getattr(entry, "summary", "") or getattr(entry, "description", "")
        title = _clean_text(getattr(entry, "title", ""))
        if not title:
            continue
        published, published_epoch = normalize_published_at(
            getattr(entry, "published", "") or "",
        )
        link = urljoin(base_url, str(getattr(entry, "link", "") or ""))
        provider_id = str(getattr(entry, "id", "") or getattr(entry, "guid", "") or link)
        cleaned_payload = _clean_text(payload)
        result.append(FetchedArticle(
            source=source["id"], title=title[:240], content=cleaned_payload or title,
            url=link, published_at=published, published_at_epoch=published_epoch,
            fetched_at=time.time(), provider_item_id=provider_id,
            is_official=source["is_official"], raw_cache_key=raw_key,
            content_scope="feed_summary" if cleaned_payload else "listing_title_only",
        ))
    return result


def _parse_json(source: dict[str, Any], content: bytes, base_url: str,
                raw_key: str) -> list[FetchedArticle]:
    parser = source["parser"]
    payload = json.loads(content.decode(parser.get("encoding") or "utf-8-sig"))
    items = _path_value(payload, parser["items_path"])
    if not isinstance(items, list):
        raise ValueError("JSON items_path 没有指向数组")
    result = []
    for item in items[:source["item_limit"]]:
        title = _clean_text(_path_value(item, parser["title_path"]))
        if not title:
            continue
        body = _clean_text(_path_value(item, parser.get("content_path", ""))) or title
        link = str(_path_value(item, parser.get("url_path", "")) or "")
        published_path = str(parser.get("published_at_path") or "").strip()
        published, published_epoch = normalize_published_at(
            _path_value(item, published_path) if published_path else "",
        )
        id_path = str(parser.get("id_path") or "").strip()
        result.append(FetchedArticle(
            source=source["id"], title=title[:240], content=body,
            url=urljoin(base_url, link), published_at=published,
            published_at_epoch=published_epoch, fetched_at=time.time(),
            provider_item_id=str(_path_value(item, id_path) if id_path else link),
            is_official=source["is_official"], raw_cache_key=raw_key,
        ))
    return result


def _node_text(node: Any, selector: str) -> str:
    selected = node.select_one(selector) if selector else node
    return _clean_text(selected.get_text(" ", strip=True)) if selected else ""


def _node_link(node: Any, selector: str, attribute: str = "href") -> str:
    selected = node.select_one(selector) if selector else node
    return str(selected.get(attribute, "") if selected else "")


def _detail_content(source: dict[str, Any], article: FetchedArticle, store: NewsFetchStore,
                    selector: str, preview: bool) -> tuple[str, str]:
    if not article.url:
        return article.content, article.raw_cache_key
    content, final_url, raw_key = _fetch_bytes(source, article.url, store, preview=preview)
    if content is None:
        return article.content, article.raw_cache_key
    soup = BeautifulSoup(content, "html.parser")
    selected = soup.select_one(selector)
    text = _clean_text(selected.get_text(" ", strip=True)) if selected else ""
    article.url = final_url
    return text or article.content, raw_key or article.raw_cache_key


def _parse_html(source: dict[str, Any], content: bytes, base_url: str,
                raw_key: str, store: NewsFetchStore, preview: bool) -> list[FetchedArticle]:
    parser = source["parser"]
    soup = BeautifulSoup(content, "html.parser")
    nodes = soup.select(parser["item_selector"])
    result = []
    for node in nodes[:source["item_limit"]]:
        title = _node_text(node, parser["title_selector"])
        if not title:
            continue
        body = _node_text(node, parser.get("content_selector", "")) or title
        link = _node_link(node, parser.get("url_selector", ""), parser.get("url_attribute", "href"))
        published_selector = str(parser.get("published_at_selector") or "").strip()
        published, published_epoch = normalize_published_at(
            _node_text(node, published_selector) if published_selector else "",
        )
        result.append(FetchedArticle(
            source=source["id"], title=title[:240], content=body,
            url=urljoin(base_url, link), published_at=published,
            published_at_epoch=published_epoch, fetched_at=time.time(), provider_item_id=link,
            is_official=source["is_official"], raw_cache_key=raw_key,
        ))
    selector = str(parser.get("detail_content_selector") or "").strip()
    if selector and result:
        with ThreadPoolExecutor(max_workers=min(4, len(result))) as executor:
            futures = {
                executor.submit(_detail_content, source, article, store, selector, preview): article
                for article in result if article.url
            }
            for future in as_completed(futures):
                article = futures[future]
                try:
                    article.content, article.raw_cache_key = future.result()
                except Exception:
                    continue
    return result


def fetch_declarative_source(
    source: dict[str, Any],
    store: NewsFetchStore,
    *,
    preview: bool = False,
    state: dict[str, Any] | None = None,
) -> FetchBatch:
    """Fetch a custom source under the same batch, freshness, and 304 contract."""
    source = _validate_source_dict(source)
    if source["kind"] == "builtin":
        raise ValueError("内置来源由专用采集器处理")
    current_state = state or {}
    previous_watermark = str(current_state.get("watermark") or "")
    if current_state.get("pending_watermark"):
        source = {**source, "_conditional_cache": False}
    content, final_url, raw_key = _fetch_bytes(source, source["url"], store, preview=preview)
    if content is None:
        latest_published_at = float(current_state.get("latest_published_at") or 0.0)
        health, error_code, message = evaluate_freshness(
            source, latest_published_at, previous_watermark, now=time.time(),
        )
        pending_watermark = str(current_state.get("pending_watermark") or "")
        if pending_watermark:
            return FetchBatch(
                source_id=source["id"], watermark=previous_watermark,
                previous_watermark=previous_watermark,
                health="degraded", complete=False,
                error_code=error_code or "backfill_not_completed_on_304",
                message=message or (
                    "来源返回 304，但先前缺口尚未补齐；保留 committed 水位"
                ),
                latest_published_at=latest_published_at,
                pending_watermark=pending_watermark,
            )
        return FetchBatch(
            source_id=source["id"], watermark=previous_watermark,
            previous_watermark=previous_watermark,
            health="not_modified" if health == "healthy" else health,
            error_code=error_code, message=message,
            latest_published_at=latest_published_at,
        )
    if source["kind"] == "rss":
        result = _parse_rss(source, content, final_url, raw_key)
        if not result:
            raise NewsContractError("RSS 来源没有解析出任何条目", code="empty_rss")
    elif source["kind"] == "json":
        result = _parse_json(source, content, final_url, raw_key)
        if not result:
            raise NewsContractError("JSON 来源没有解析出任何条目", code="empty_json")
    else:
        result = _parse_html(source, content, final_url, raw_key, store, preview)
        if not result:
            raise NewsContractError("HTML 来源没有解析出任何条目", code="empty_html")
    for article in result:
        if not article.provider_item_id:
            identity = "|".join((article.url, article.title, article.published_at))
            article.provider_item_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    result.sort(
        key=lambda item: (item.published_at_epoch, item.provider_item_id), reverse=True,
    )
    latest_published_at = max(item.published_at_epoch for item in result)
    health, freshness_code, freshness_message = evaluate_freshness(
        source, latest_published_at, previous_watermark, now=time.time(),
    )
    candidate_watermark = result[0].provider_item_id
    selected = result
    reached = not previous_watermark
    if previous_watermark:
        selected = []
        for article in result:
            if article.provider_item_id == previous_watermark:
                reached = True
                break
            selected.append(article)
    if not reached and not freshness_code:
        health = "degraded"
    raw_keys = list(dict.fromkeys(
        item.raw_cache_key for item in result if item.raw_cache_key
    ))
    return FetchBatch(
        source_id=source["id"], articles=selected,
        watermark=candidate_watermark if reached else previous_watermark,
        previous_watermark=previous_watermark,
        health=health, complete=reached,
        raw_cache_keys=raw_keys,
        error_code=freshness_code or ("" if reached else "watermark_not_reached"),
        message=freshness_message or (
            "" if reached else "自定义来源当前响应未包含上次水位；已保留 committed 水位"
        ),
        latest_published_at=latest_published_at,
        pending_watermark="" if reached else candidate_watermark,
    )
