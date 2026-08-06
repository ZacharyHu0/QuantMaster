"""可配置财经资讯来源、声明式解析器与短期原始响应缓存。"""

from __future__ import annotations

import gzip
import hashlib
import ipaddress
import json
import re
import socket
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urljoin, urlparse

import feedparser
import httpx
from bs4 import BeautifulSoup

from quantmaster.config import get_config
from quantmaster.credentials import CredentialError, CredentialStore
from quantmaster.runtime.sqlite import connect_sqlite

SOURCE_KINDS = {"builtin", "rss", "json", "html"}
SOURCE_GROUPS = {"fast", "official", "periodic"}
MAX_RESPONSE_BYTES = 5 * 1024 * 1024
FETCH_TIMEOUT = 20.0
MAX_REDIRECTS = 4
PARSER_VERSION = "1"
USER_AGENT = "Mozilla/5.0 (compatible; QuantMaster/0.4; local research workstation)"


@dataclass
class FetchedArticle:
    source: str
    title: str
    content: str
    url: str = ""
    published_at: str = ""
    is_official: bool = False
    raw_cache_key: str = ""


BUILTIN_SOURCES: tuple[dict[str, Any], ...] = (
    {
        "id": "sina_live", "name": "新浪财经 7×24", "kind": "builtin",
        "group_name": "fast", "url": "https://zhibo.sina.com.cn/api/zhibo/feed",
        "is_official": False,
    },
    {
        "id": "eastmoney_fast", "name": "东方财富快讯", "kind": "builtin",
        "group_name": "fast",
        "url": "https://np-listapi.eastmoney.com/comm/web/getFastNewsList",
        "is_official": False,
    },
    {
        "id": "csrc", "name": "中国证监会", "kind": "builtin",
        "group_name": "official",
        "url": "https://www.csrc.gov.cn/csrc/c100028/common_list.shtml",
        "is_official": True,
    },
    {
        "id": "sse", "name": "上海证券交易所", "kind": "builtin",
        "group_name": "official",
        "url": "https://www.sse.com.cn/disclosure/listedinfo/announcement/",
        "is_official": True,
    },
    {
        "id": "szse", "name": "深圳证券交易所", "kind": "builtin",
        "group_name": "official",
        "url": "https://www.szse.cn/disclosure/listed/notice/index.html",
        "is_official": True,
    },
)


def _utc_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    soup = BeautifulSoup(str(value), "html.parser")
    return re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()


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
        "auth_type": auth_type, "auth_header": auth_header, "parser": parser,
        "enabled": bool(result.get("enabled", True)),
        "is_official": bool(result.get("is_official", False)),
    })
    return result


class NewsSourceStore:
    """来源配置、抓取运行记录和 HTTP 条件缓存。"""

    def __init__(self, path: Path | None = None, credentials: CredentialStore | None = None):
        self.path = path or get_config().data_root / "news.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.credentials = credentials or CredentialStore()
        self.raw_root = self.path.parent / "news_raw"
        self._migrate()
        self._seed_builtins()

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(self.path, timeout=5.0, row_factory=True)

    def _migrate(self) -> None:
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS news_sources (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1, group_name TEXT NOT NULL,
                    url TEXT NOT NULL, item_limit INTEGER NOT NULL DEFAULT 30,
                    factor_weight REAL NOT NULL DEFAULT 1, is_official INTEGER NOT NULL DEFAULT 0,
                    parser TEXT NOT NULL DEFAULT '{}', auth_type TEXT NOT NULL DEFAULT 'none',
                    auth_header TEXT NOT NULL DEFAULT '', secret_state TEXT NOT NULL DEFAULT 'none',
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
                CREATE INDEX IF NOT EXISTS idx_news_runs_source
                    ON news_source_runs(source_id,started_at DESC);
            """)

    def _seed_builtins(self) -> None:
        now = _utc_iso()
        with self._conn() as conn:
            for item in BUILTIN_SOURCES:
                conn.execute(
                    "INSERT OR IGNORE INTO news_sources "
                    "(id,name,kind,enabled,group_name,url,item_limit,factor_weight,is_official,"
                    "parser,auth_type,auth_header,secret_state,built_in,created_at,updated_at) "
                    "VALUES (?,?,?,1,?,?,30,1,?,'{}','none','','none',1,?,?)",
                    (item["id"], item["name"], item["kind"], item["group_name"],
                     item["url"], int(item["is_official"]), now, now),
                )

    @staticmethod
    def _decode(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        result = dict(row)
        result["enabled"] = bool(result["enabled"])
        result["is_official"] = bool(result["is_official"])
        result["built_in"] = bool(result["built_in"])
        result["parser"] = json.loads(result.get("parser") or "{}")
        result["auth_configured"] = result.get("secret_state") == "keyring"
        return result

    def list(self, *, enabled: bool | None = None, group_name: str | None = None) -> list[dict]:
        where, params = [], []
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
                "ORDER BY r.started_at DESC LIMIT 1) AS last_error "
                f"FROM news_sources s {clause} ORDER BY built_in DESC,rowid",
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
                "(id,name,kind,enabled,group_name,url,item_limit,factor_weight,is_official,parser,"
                "auth_type,auth_header,secret_state,built_in,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)",
                (source_id, item["name"], item["kind"], int(item["enabled"]),
                 item["group_name"], item["url"], item["item_limit"], item["factor_weight"],
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
                        "is_official"):
                merged[key] = current[key]
        item = _validate_source_dict(merged)
        if (item["auth_type"] != "none" and token_action == "keep"
                and not current.get("auth_configured")):
            raise ValueError("启用来源鉴权时必须填写 API Token")
        now = _utc_iso()
        with self._conn() as conn:
            conn.execute(
                "UPDATE news_sources SET name=?,enabled=?,group_name=?,url=?,item_limit=?,"
                "factor_weight=?,is_official=?,parser=?,auth_type=?,auth_header=?,updated_at=? "
                "WHERE id=?",
                (item["name"], int(item["enabled"]), item["group_name"], item["url"],
                 item["item_limit"], item["factor_weight"], int(item["is_official"]),
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

    def finish_run(self, run_id: str, *, fetched: int = 0, saved: int = 0,
                   pending: int = 0, error: str = "") -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE news_source_runs SET status=?,finished_at=?,fetched=?,saved=?,pending=?,"
                "error=? WHERE id=?",
                ("failed" if error else "success", _utc_iso(), fetched, saved, pending,
                 error[:1000], run_id),
            )

    def cache_headers(self, source_id: str, url: str) -> dict[str, str]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT etag,last_modified FROM news_http_cache WHERE source_id=? AND url=?",
                (source_id, url),
            ).fetchone()
        if not row:
            return {}
        result = {}
        if row["etag"]:
            result["If-None-Match"] = row["etag"]
        if row["last_modified"]:
            result["If-Modified-Since"] = row["last_modified"]
        return result

    def save_response(self, source_id: str, url: str, content: bytes,
                      headers: httpx.Headers, status_code: int) -> str:
        digest = hashlib.sha256(content).hexdigest()
        stamp = datetime.now(UTC).strftime("%Y-%m-%d")
        directory = self.raw_root / source_id / stamp
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{digest}.gz"
        if not path.exists():
            with gzip.open(path, "wb") as handle:
                handle.write(content)
        key = str(path.relative_to(self.path.parent)).replace("\\", "/")
        with self._conn() as conn:
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

    def touch_not_modified(self, source_id: str, url: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE news_http_cache SET fetched_at=?,status_code=304 WHERE source_id=? AND url=?",
                (time.time(), source_id, url),
            )

    def cleanup_raw(self, retention_days: int) -> int:
        cutoff = time.time() - max(0, retention_days) * 86400
        removed = 0
        root = self.raw_root.resolve()
        if root.is_dir():
            for path in root.rglob("*.gz"):
                resolved = path.resolve()
                if root not in resolved.parents or path.stat().st_mtime >= cutoff:
                    continue
                path.unlink(missing_ok=True)
                removed += 1
            for directory in sorted(root.rglob("*"), reverse=True):
                if directory.is_dir() and not any(directory.iterdir()):
                    directory.rmdir()
        with self._conn() as conn:
            conn.execute("DELETE FROM news_http_cache WHERE fetched_at<?", (cutoff,))
        return removed


def _ensure_public_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("来源地址必须是完整的 http(s) URL")
    hostname = parsed.hostname.strip("[]").lower()
    if hostname == "localhost" or hostname.endswith(".localhost"):
        raise ValueError("资讯来源不能访问本机或私有网络地址")
    try:
        default_port = 443 if parsed.scheme == "https" else 80
        addresses = {
            item[4][0] for item in socket.getaddrinfo(hostname, parsed.port or default_port)
        }
    except socket.gaierror as exc:
        raise ValueError(f"无法解析来源域名：{hostname}") from exc
    for value in addresses:
        address = ipaddress.ip_address(value)
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


def _fetch_bytes(source: dict[str, Any], url: str, store: NewsSourceStore,
                 *, preview: bool = False) -> tuple[bytes | None, str, str]:
    current = url
    token = store.token(source)
    base_headers = _request_headers(source, token)
    headers = dict(base_headers)
    if _origin(current) != _origin(source["url"]):
        headers = _without_auth(headers, source)
    if not preview:
        headers.update(store.cache_headers(source["id"], current))
    with httpx.Client(timeout=FETCH_TIMEOUT, follow_redirects=False) as client:
        for _ in range(MAX_REDIRECTS + 1):
            _ensure_public_url(current)
            with client.stream("GET", current, headers=headers) as response:
                if response.status_code in {301, 302, 303, 307, 308}:
                    location = response.headers.get("location")
                    if not location:
                        response.raise_for_status()
                    current = urljoin(current, location)
                    headers = dict(base_headers)
                    if _origin(current) != _origin(source["url"]):
                        headers = _without_auth(headers, source)
                    continue
                if response.status_code == 304 and not preview:
                    store.touch_not_modified(source["id"], current)
                    return None, current, ""
                response.raise_for_status()
                chunks, size = [], 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > MAX_RESPONSE_BYTES:
                        raise ValueError("来源响应超过 5MB 安全上限")
                    chunks.append(chunk)
                content = b"".join(chunks)
                key = "" if preview else store.save_response(
                    source["id"], current, content, response.headers, response.status_code)
                return content, current, key
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
        result.append(FetchedArticle(
            source=source["id"], title=title[:240], content=_clean_text(payload) or title,
            url=urljoin(base_url, str(getattr(entry, "link", "") or "")),
            published_at=str(getattr(entry, "published", "") or getattr(entry, "updated", "") or ""),
            is_official=source["is_official"], raw_cache_key=raw_key,
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
        published = str(_path_value(item, parser.get("published_at_path", "")) or "")
        result.append(FetchedArticle(
            source=source["id"], title=title[:240], content=body,
            url=urljoin(base_url, link), published_at=published,
            is_official=source["is_official"], raw_cache_key=raw_key,
        ))
    return result


def _node_text(node: Any, selector: str) -> str:
    selected = node.select_one(selector) if selector else node
    return _clean_text(selected.get_text(" ", strip=True)) if selected else ""


def _node_link(node: Any, selector: str, attribute: str = "href") -> str:
    selected = node.select_one(selector) if selector else node
    return str(selected.get(attribute, "") if selected else "")


def _detail_content(source: dict[str, Any], article: FetchedArticle, store: NewsSourceStore,
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
                raw_key: str, store: NewsSourceStore, preview: bool) -> list[FetchedArticle]:
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
        published = _node_text(node, parser.get("published_at_selector", ""))
        result.append(FetchedArticle(
            source=source["id"], title=title[:240], content=body,
            url=urljoin(base_url, link), published_at=published,
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


def fetch_declarative_source(source: dict[str, Any], store: NewsSourceStore,
                             *, preview: bool = False) -> list[FetchedArticle]:
    """抓取一个 RSS/JSON/HTML 来源；预览模式不写原始响应缓存。"""
    source = _validate_source_dict(source)
    if source["kind"] == "builtin":
        raise ValueError("内置来源由专用采集器处理")
    content, final_url, raw_key = _fetch_bytes(source, source["url"], store, preview=preview)
    if content is None:
        return []
    if source["kind"] == "rss":
        return _parse_rss(source, content, final_url, raw_key)
    if source["kind"] == "json":
        return _parse_json(source, content, final_url, raw_key)
    return _parse_html(source, content, final_url, raw_key, store, preview)
