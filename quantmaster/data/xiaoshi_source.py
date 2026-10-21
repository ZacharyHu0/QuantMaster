"""Read-only Xiaoshi market, history, news, and publication client."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import tempfile
import threading
import time
from collections.abc import Callable, Iterable
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.base import (
    DataCapability,
    DataSource,
    normalize_bars,
    validate_frequency,
)
from quantmaster.market_capabilities import Market

API_BASE = "https://api.shizixi.com"
MANIFEST_PATH = "/api/v3/manifest"
CAPABILITIES_PATH = "/api/v3/capabilities"
BOOTSTRAP_PATH = "/api/v3/bootstrap"
PACKAGE_MANIFEST_PATH = "/api/v3/tools/xiaoshi-agent-tools/manifest"
_RESOURCE_FIELDS = {
    "prompt": ("prompt_url", "prompt_version", "prompt_sha256", "prompt.txt"),
    "skill": ("skill_url", "skill_version", "skill_sha256", "SKILL.md"),
    "api_schema": ("api_schema_url", "manifest_version", "api_schema_sha256", "llms.txt"),
}
_MARKETS = {"CN", "HK", "US"}
_INSTRUMENTS = {"stock", "index", "etf"}
_R2_HOST_SUFFIX = ".r2.cloudflarestorage.com"


class XiaoshiError(RuntimeError):
    """Safe client error that never embeds credentials or response bodies."""


class XiaoshiRateLimited(XiaoshiError):
    def __init__(self, retry_after: float):
        super().__init__(f"小石请求受控限流，请在 {retry_after:g} 秒后重试")
        self.retry_after = retry_after


class XiaoshiBulkDownloadRequired(XiaoshiError):
    """The API deliberately requires the local ``xiaoshi-data`` CLI."""

    def __init__(self, message: str = "小石要求使用 xiaoshi-data CLI 批量下载"):
        super().__init__(message)
        self.cli = "xiaoshi-data"


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, candidate = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(candidate, path)
    finally:
        Path(candidate).unlink(missing_ok=True)


class XiaoshiPublicationStore:
    """Keep one verified prompt/Skill/schema publication with last-good fallback."""

    def __init__(self, root: Path | None = None, client: httpx.Client | None = None):
        self.root = root or get_config().data_root / "xiaoshi" / "publications"
        self.client = client or httpx.Client(timeout=20.0, follow_redirects=False)
        self._checked = False
        self._lock = threading.Lock()

    @property
    def metadata_path(self) -> Path:
        return self.root / "manifest.json"

    def _stored_manifest(self) -> dict[str, Any]:
        try:
            value = json.loads(self.metadata_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, TypeError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def _resource_path(self, filename: str, checksum: str) -> Path:
        return self.root / f"{checksum}.{filename}"

    @property
    def capabilities_path(self) -> Path:
        return self.root / "capabilities.json"

    @property
    def bootstrap_path(self) -> Path:
        return self.root / "bootstrap.json"

    @staticmethod
    def _resource_url(value: Any) -> str:
        url = str(value or "")
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname != "api.shizixi.com":
            raise XiaoshiError("小石发布资源地址不可信")
        return url

    def _get(self, url: str) -> httpx.Response:
        response = self.client.get(
            url,
            headers={"Cache-Control": "no-store, no-cache", "Pragma": "no-cache"},
        )
        response.raise_for_status()
        return response

    def _v3_manifest_changes(
        self, manifest: dict[str, Any], stored: dict[str, Any], *, force: bool,
    ) -> list[tuple[str, str, str]]:
        if manifest.get("contract") != "xiaoshi-agent-contract/v1":
            raise XiaoshiError("小石 Manifest 合同版本不受支持")
        capabilities_url = self._resource_url(manifest.get("capabilities_url"))
        if capabilities_url != f"{API_BASE}{CAPABILITIES_PATH}":
            raise XiaoshiError("小石能力目录地址与 Manifest 不一致")
        bootstrap_url = self._resource_url(manifest.get("bootstrap_url"))
        if bootstrap_url != f"{API_BASE}{BOOTSTRAP_PATH}":
            raise XiaoshiError("小石接入说明地址与 Manifest 不一致")
        checksum = str(manifest.get("capabilities_sha256") or "")
        if not re.fullmatch(r"[0-9a-f]{64}", checksum):
            raise XiaoshiError("小石能力目录校验和无效")
        changed = []
        old_checksum = str(stored.get("capabilities_sha256") or "")
        if force or old_checksum != checksum or not self.capabilities_path.is_file():
            changed.append(("capabilities.json", capabilities_url, checksum))
        if force or stored.get("prompt_version") != manifest.get("prompt_version"):
            changed.append(("bootstrap.json", bootstrap_url, ""))
        return changed

    def _legacy_manifest_changes(
        self, manifest: dict[str, Any], stored: dict[str, Any], *, force: bool,
    ) -> list[tuple[str, str, str]]:
        checksums = manifest.get("checksums") or {}
        changed = []
        stored_checksums = stored.get("checksums") or {}
        for name, (url_field, version_field, checksum_field, filename) in _RESOURCE_FIELDS.items():
            checksum = str(checksums.get(checksum_field) or "")
            if not re.fullmatch(r"[0-9a-f]{64}", checksum):
                raise XiaoshiError(f"小石 {name} 校验和无效")
            current_checksum = str(stored_checksums.get(checksum_field) or "")
            version_changed = str(stored.get(version_field) or "") != str(
                manifest.get(version_field) or "",
            )
            if force or current_checksum != checksum or version_changed:
                changed.append((filename, self._resource_url(manifest.get(url_field)), checksum))
        return changed

    def _manifest_changes(
        self, stored: dict[str, Any], *, force: bool,
    ) -> tuple[dict[str, Any], list[tuple[str, str, str]]]:
        manifest = self._get(f"{API_BASE}{MANIFEST_PATH}").json()
        if not isinstance(manifest, dict):
            raise XiaoshiError("小石 Manifest 不是对象")
        if isinstance(manifest.get("checksums"), dict):
            changed = self._legacy_manifest_changes(manifest, stored, force=force)
        else:
            changed = self._v3_manifest_changes(manifest, stored, force=force)
        return manifest, changed

    def _store_changed(self, changed: list[tuple[str, str, str]]) -> None:
        candidates = []
        for filename, url, checksum in changed:
            body = self._get(url).content
            if checksum and hashlib.sha256(body).hexdigest() != checksum:
                raise XiaoshiError(f"小石资源 {filename} 校验失败")
            candidates.append((filename, checksum, body))
        for filename, checksum, body in candidates:
            if filename in {"capabilities.json", "bootstrap.json"}:
                _atomic_write(self.root / filename, body)
            else:
                _atomic_write(self._resource_path(filename, checksum), body)

    def _has_complete_fallback(self, stored: dict[str, Any]) -> bool:
        if stored.get("contract") == "xiaoshi-agent-contract/v1":
            return bool(
                self.capabilities_path.is_file() and self.bootstrap_path.is_file()
            )
        checksums = stored.get("checksums") or {}
        return bool(stored) and all(
            self._resource_path(item[3], str(checksums.get(item[2]) or "")).is_file()
            for item in _RESOURCE_FIELDS.values()
        )

    def ensure_current(self, *, force: bool = False) -> dict[str, Any]:
        with self._lock:
            if self._checked and not force:
                return self._stored_manifest()
            stored = self._stored_manifest()
            try:
                manifest, changed = self._manifest_changes(stored, force=force)
                self._store_changed(changed)
                _atomic_write(
                    self.metadata_path,
                    json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode("utf-8"),
                )
                self._checked = True
                return manifest
            except (httpx.HTTPError, ValueError, TypeError, XiaoshiError) as exc:
                self._checked = True
                if self._has_complete_fallback(stored):
                    return stored
                if isinstance(exc, XiaoshiError):
                    raise
                raise XiaoshiError("小石发布资源不可用，且没有已验证的本地版本") from exc

    def skill_text(self) -> str:
        self.ensure_current()
        if self._stored_manifest().get("contract") == "xiaoshi-agent-contract/v1":
            workflows = self.capabilities().get("agent_workflows", {}).get("items", [])
            prompts = [
                str(item.get("prompt") or "").strip()
                for item in workflows
                if isinstance(item, dict) and str(item.get("prompt") or "").strip()
            ]
            if prompts:
                return "\n\n".join(prompts)
            raise XiaoshiError("小石能力目录未提供可用研究工作流")
        try:
            manifest = self._stored_manifest()
            checksum = str((manifest.get("checksums") or {}).get("skill_sha256") or "")
            return self._resource_path("SKILL.md", checksum).read_text(encoding="utf-8")
        except (FileNotFoundError, OSError, UnicodeError) as exc:
            raise XiaoshiError("小石量化 Skill 本地版本不可读") from exc

    def capabilities(self) -> dict[str, Any]:
        """Return the single v3 capability directory after hash validation."""
        manifest = self.ensure_current()
        if manifest.get("contract") != "xiaoshi-agent-contract/v1":
            raise XiaoshiError("小石能力目录需要 v3 Manifest")
        try:
            value = json.loads(self.capabilities_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise XiaoshiError("小石能力目录不可读") from exc
        if not isinstance(value, dict):
            raise XiaoshiError("小石能力目录不是对象")
        return value

    def bootstrap(self) -> dict[str, Any]:
        self.ensure_current()
        try:
            value = json.loads(self.bootstrap_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise XiaoshiError("小石接入说明不可读") from exc
        if not isinstance(value, dict):
            raise XiaoshiError("小石接入说明不是对象")
        return value

    def package_manifest(self) -> dict[str, Any]:
        """Read the official package manifest without downloading or installing it."""
        manifest = self._get(f"{API_BASE}{PACKAGE_MANIFEST_PATH}").json()
        if not isinstance(manifest, dict) or manifest.get("contract") != "xiaoshi-agent-tools-package/v1":
            raise XiaoshiError("小石工具包清单合同版本不受支持")
        if not re.fullmatch(r"[0-9a-f]{64}", str(manifest.get("sha256") or "")):
            raise XiaoshiError("小石工具包清单缺少有效校验和")
        return manifest


class XiaoshiClient:
    """Small authenticated client with one auth probe and strict identity checks."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        client: httpx.Client | None = None,
        publications: XiaoshiPublicationStore | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        retry_rate_limit: bool | None = None,
    ):
        self.api_key = (api_key if api_key is not None else get_config().data.xiaoshi_api_key).strip()
        self.client = client or httpx.Client(
            base_url=API_BASE,
            timeout=get_config().data.provider_timeout,
            follow_redirects=False,
        )
        self.publications = publications or XiaoshiPublicationStore(client=self.client)
        self.sleeper = sleeper
        # Supplying a sleeper is retained as an explicit compatibility opt-in
        # for callers that own a bounded retry loop.  The normal v3 client stops
        # on 429 and lets the caller follow Retry-After.
        self.retry_rate_limit = (
            retry_rate_limit if retry_rate_limit is not None else sleeper is not time.sleep
        )
        self._authenticated = False
        self._reported_errors: set[str] = set()

    @property
    def headers(self) -> dict[str, str]:
        if not self.api_key:
            raise XiaoshiError("尚未配置小石 API Key")
        return {"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"}

    @staticmethod
    def _retry_after(response: httpx.Response) -> float:
        header = response.headers.get("Retry-After", "").strip()
        try:
            return max(0.0, float(header))
        except ValueError:
            try:
                detail = response.json().get("detail") or {}
                return max(0.0, float(detail.get("retry_after_seconds") or 1.0))
            except (ValueError, TypeError, AttributeError):
                return 1.0

    def _send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any = None,
        authenticate: bool = True,
    ) -> httpx.Response:
        if authenticate and not self._authenticated:
            self.check_api_key()
        attempts = 2 if self.retry_rate_limit else 1
        for attempt in range(attempts):
            try:
                response = self.client.request(
                    method,
                    path,
                    params=params,
                    json=json_body,
                    headers=self.headers,
                )
            except httpx.HTTPError as exc:
                raise XiaoshiError(f"小石接口暂时不可用（{type(exc).__name__}）") from exc
            if response.status_code == 429:
                delay = self._retry_after(response)
                if attempt + 1 < attempts:
                    self.sleeper(delay)
                    continue
                raise XiaoshiRateLimited(delay)
            break
        if response.is_error:
            request_id = response.headers.get("x-request-id", "")
            suffix = f"，request_id={request_id}" if request_id else ""
            try:
                error_body = response.json()
            except (ValueError, TypeError):
                error_body = {}
            detail = error_body.get("detail") if isinstance(error_body, dict) else {}
            code = error_body.get("code") if isinstance(error_body, dict) else ""
            if isinstance(detail, dict):
                code = detail.get("error") or code
            if code in {"bulk_download_required", "bulk_download_required_use_cli"}:
                raise XiaoshiBulkDownloadRequired()
            raise XiaoshiError(f"小石接口返回 HTTP {response.status_code}{suffix}")
        return response

    def check_api_key(self) -> dict[str, Any]:
        self.publications.ensure_current()
        response = self._send("GET", "/api/v3/auth/api-key/check", authenticate=False)
        value = response.json()
        if not isinstance(value, dict):
            raise XiaoshiError("小石鉴权响应无效")
        self._authenticated = True
        return value

    def contract(self) -> dict[str, Any]:
        """Return the locally verified platform contract and capability hash."""
        return self.publications.ensure_current()

    def package_manifest(self) -> dict[str, Any]:
        """Inspect the current tool package; installation remains caller-authorized."""
        return self.publications.package_manifest()

    def discover(self) -> dict[str, dict[str, Any]]:
        """Read the required catalog/schema/coverage/freshness set once.

        The returned values are metadata only.  The method does not create a
        download session; bulk delivery remains the local CLI's responsibility.
        """
        self.publications.ensure_current()
        return {
            "catalog": self.json("GET", "/api/v3/quant-data/catalog"),
            "schema": self.json("GET", "/api/v3/quant/events/schema"),
            "coverage": self.json("GET", "/api/v3/market/capabilities"),
            "freshness": self.json("GET", "/api/v3/status"),
        }

    def query_dataset(
        self,
        dataset: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", dataset.strip()):
            raise ValueError("小石数据集名称无效")
        return self.json("GET", f"/api/v3/quant-data/{dataset}", params=params)

    def json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: Any = None,
    ) -> dict[str, Any]:
        value = self._send(method, path, params=params, json_body=body).json()
        if not isinstance(value, dict):
            raise XiaoshiError("小石接口响应不是对象")
        return value

    @staticmethod
    def _identity(market: str, instrument: str) -> tuple[str, str]:
        normalized_market = str(market).upper()
        normalized_instrument = str(instrument).lower()
        if normalized_market not in _MARKETS:
            raise ValueError("market 必须显式为 CN/HK/US")
        if normalized_instrument not in _INSTRUMENTS:
            raise ValueError("instrument 必须显式为 stock/index/etf")
        return normalized_market, normalized_instrument

    @classmethod
    def _validate_quote(
        cls,
        value: dict[str, Any],
        market: str,
        instrument: str,
        expected_name: str = "",
    ) -> dict[str, Any]:
        normalized_market, normalized_instrument = cls._identity(market, instrument)
        name = str(value.get("name") or "").strip()
        if (
            str(value.get("market") or "").upper() != normalized_market
            or str(value.get("instrument") or "").lower() != normalized_instrument
            or not name
        ):
            raise XiaoshiError("小石行情标的身份校验失败")
        if expected_name and name.casefold() != expected_name.strip().casefold():
            raise XiaoshiError("小石行情名称与本地证券主数据不一致")
        return value

    def quote(
        self,
        symbol: str,
        *,
        market: str,
        instrument: str,
        expected_name: str = "",
    ) -> dict[str, Any]:
        normalized_market, normalized_instrument = self._identity(market, instrument)
        value = self.json(
            "GET",
            f"/api/v3/market/quote/{symbol}",
            params={"market": normalized_market, "instrument": normalized_instrument},
        )
        return self._validate_quote(value, normalized_market, normalized_instrument, expected_name)

    def quotes(self, requests: Iterable[dict[str, str]]) -> list[dict[str, Any]]:
        normalized = []
        for request in requests:
            market, instrument = self._identity(request.get("market", ""), request.get("instrument", ""))
            normalized.append({
                "symbol": str(request.get("symbol") or ""),
                "market": market,
                "instrument": instrument,
            })
        if not 1 <= len(normalized) <= 100:
            raise ValueError("小石批量行情每次必须包含 1–100 个标的")
        payload = self.json("POST", "/api/v3/market/quotes", body={"requests": normalized})
        rows = (
            payload.get("items")
            or payload.get("data")
            or payload.get("quotes")
            or payload.get("results")
        )
        if not isinstance(rows, list) or len(rows) != len(normalized):
            raise XiaoshiError("小石批量行情响应数量不一致")
        return [
            self._validate_quote(dict(row), request["market"], request["instrument"])
            for row, request in zip(rows, normalized, strict=True)
            if isinstance(row, dict)
        ]

    def news(
        self,
        *,
        after_id: int,
        page_size: int = 100,
        since: str = "",
        until: str = "",
        **filters: Any,
    ) -> dict[str, Any]:
        if after_id < 0 or not 1 <= page_size <= 100:
            raise ValueError("新闻 after_id/page_size 无效")
        params = {"after_id": after_id, "page_size": page_size, **filters}
        if since:
            params["since"] = since
        if until:
            if not since:
                raise ValueError("新闻 until 必须与 since 一起使用")
            params["until"] = until
        return self.json("GET", "/api/v3/news", params=params)

    def history_session(self, dataset: str, **dimensions: Any) -> dict[str, Any]:
        allowed = {
            "daily-stock", "daily-date", "daily", "global-daily", "min1",
            "min1-market", "event-archive", "sector-constituents",
        }
        if dataset not in allowed:
            raise ValueError("不支持的小石历史数据集")
        return self.json(
            "GET",
            "/api/v3/history/download-session",
            params={"dataset": dataset, **dimensions},
        )

    @staticmethod
    def _file_records(session: dict[str, Any]) -> list[dict[str, Any]]:
        files = session.get("files")
        if isinstance(files, dict):
            files = [files]
        if not isinstance(files, list) or not files or not all(isinstance(item, dict) for item in files):
            raise XiaoshiError("小石历史下载会话没有文件")
        return files

    def download_files(self, session: dict[str, Any]) -> list[tuple[dict[str, Any], bytes]]:
        result = []
        for item in self._file_records(session):
            url = str(item.get("url") or "")
            parsed = urlparse(url)
            if parsed.scheme != "https" or not (
                str(parsed.hostname or "").endswith(_R2_HOST_SUFFIX)
                or parsed.hostname == "kline.shizixi.com"
            ):
                raise XiaoshiError("小石 R2 下载地址不可信")
            response = httpx.get(url, timeout=get_config().data.provider_timeout, follow_redirects=False)
            response.raise_for_status()
            body = response.content
            expected_size = int(item.get("size") or item.get("bytes") or 0)
            expected_hash = str(item.get("sha256") or "")
            if expected_size and len(body) != expected_size:
                raise XiaoshiError("小石 R2 文件大小校验失败")
            if (
                not re.fullmatch(r"[0-9a-f]{64}", expected_hash)
                or hashlib.sha256(body).hexdigest() != expected_hash
            ):
                raise XiaoshiError("小石 R2 文件校验和失败")
            result.append((item, body))
        return result

    def history_frame(self, dataset: str, **dimensions: Any) -> pd.DataFrame:
        session = self.history_session(dataset, **dimensions)
        frames = [pd.read_parquet(io.BytesIO(body)) for _item, body in self.download_files(session)]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def archived_events(self, date: str, event_type: str = "") -> pd.DataFrame:
        dimensions = {"date": date}
        if event_type:
            dimensions["event_type"] = event_type
        return self.history_frame("event-archive", **dimensions)

    def sector_constituents(self, date: str) -> pd.DataFrame:
        return self.history_frame("sector-constituents", date=date)

    def financial_timeline(
        self,
        *,
        since: str,
        to: str,
        event_types: str = "",
        **filters: Any,
    ) -> dict[str, Any]:
        start, end = pd.Timestamp(since), pd.Timestamp(to)
        if end < start or end - start > pd.Timedelta(days=31):
            raise ValueError("小石在线金融时间轴必须为不超过 31 天的正向窗口")
        params = {"since": since, "to": to, **filters}
        if event_types:
            params["event_types"] = event_types
        return self.json("GET", "/api/v3/quant/events", params=params)

    def future_dynamic_latest(self, limit: int = 20) -> dict[str, Any]:
        if not 1 <= limit <= 100:
            raise ValueError("未来金融事件数量必须为 1–100")
        return self.json("GET", "/api/v3/future-dynamic/latest", params={"limit": limit})


@lru_cache(maxsize=2)
def _client_for_key(api_key: str) -> XiaoshiClient:
    return XiaoshiClient(api_key)


def get_xiaoshi_client() -> XiaoshiClient:
    """Reuse one authenticated task client so ordinary calls do not recheck Manifest."""
    return _client_for_key(get_config().data.xiaoshi_api_key)


def _symbol_identity(symbol: str) -> tuple[str, str, str]:
    value = str(symbol).strip().upper()
    code, separator, suffix = value.rpartition(".")
    if not separator:
        raise ValueError("小石行情需要带市场后缀的规范标的")
    if suffix in {"SH", "SZ", "BJ"}:
        return code, "CN", value
    if suffix == "HK":
        return code, "HK", value
    if suffix == "US":
        return code, "US", value
    raise ValueError("小石当前仅支持 CN/HK/US 股票")


def _matching_minutes(frame: pd.DataFrame, code: str, canonical: str) -> pd.DataFrame:
    if "ts_code" not in frame:
        return frame
    return frame[frame["ts_code"].astype(str).str.upper().isin({code, canonical})]


def _resample_cn_minutes(minute: pd.DataFrame, rule: str) -> pd.DataFrame:
    sessions = []
    aggregation = {
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
    }
    if "amount" in minute:
        aggregation["amount"] = "sum"
    for _date, day in minute.groupby(minute.index.date):
        for left, right in (("09:30", "11:30"), ("13:00", "15:00")):
            part = day.between_time(left, right, inclusive="both")
            if not part.empty:
                sessions.append(
                    part.resample(rule, origin=part.index[0])
                    .agg(aggregation)
                    .dropna(subset=["open", "high", "low", "close"]),
                )
    return pd.concat(sessions).sort_index() if sessions else pd.DataFrame()


class XiaoshiSource(DataSource):
    name = "xiaoshi"
    markets = (Market.CN, Market.HK, Market.US)
    capabilities = frozenset({
        DataCapability.DAILY,
        DataCapability.DAILY_BARS,
        DataCapability.INTRADAY,
        DataCapability.INTRADAY_BARS,
        DataCapability.SPOT,
    })

    def __init__(self, client: XiaoshiClient | None = None):
        self.client = client or get_xiaoshi_client()

    def supports_capability(self, capability: DataCapability | str) -> bool:
        value = capability if isinstance(capability, DataCapability) else DataCapability(str(capability))
        cfg = get_config().data
        if value in {DataCapability.DAILY, DataCapability.DAILY_BARS}:
            return cfg.xiaoshi_history_enabled
        if value in {DataCapability.INTRADAY, DataCapability.INTRADAY_BARS}:
            return cfg.xiaoshi_minute_enabled
        if value == DataCapability.SPOT:
            return cfg.xiaoshi_realtime_enabled
        return False

    def daily(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        if not get_config().data.xiaoshi_history_enabled:
            raise XiaoshiError("小石历史行情已在设置中关闭")
        code, market, canonical = _symbol_identity(symbol)
        adjust = "qfq" if market == "CN" else "raw"
        frames = []
        for year in range(pd.Timestamp(start).year, pd.Timestamp(end).year + 1):
            frame = self.client.history_frame(
                "daily-stock", market=market, code=code, adjust=adjust, year=year,
            )
            if frame.empty:
                continue
            if "market" in frame:
                frame = frame[frame["market"].astype(str).str.upper() == market]
            if "code" in frame:
                frame = frame[frame["code"].astype(str).str.upper().isin({code, canonical})]
            frames.append(frame)
        if not frames:
            return pd.DataFrame()
        result = normalize_bars(pd.concat(frames, ignore_index=True))
        return result.loc[pd.Timestamp(start):pd.Timestamp(end)]

    def intraday(self, symbol: str, start: str, end: str, frequency: str = "5m") -> pd.DataFrame:
        if not get_config().data.xiaoshi_minute_enabled:
            raise XiaoshiError("小石历史分钟线已在设置中关闭")
        normalized = validate_frequency(frequency)
        if normalized == "1d":
            return self.daily(symbol, start, end)
        code, market, canonical = _symbol_identity(symbol)
        if market != "CN":
            raise XiaoshiError("港美历史分钟线尚未发布为 R2 历史数据集")
        frames = []
        for year in range(pd.Timestamp(start).year, pd.Timestamp(end).year + 1):
            frame = self.client.history_frame("min1", code=code, year=year)
            frames.append(_matching_minutes(frame, code, canonical))
        if not frames:
            return pd.DataFrame()
        raw = pd.concat(frames, ignore_index=True)
        if "trade_time" in raw:
            raw = raw.rename(columns={"trade_time": "date", "vol": "volume"})
        minute = normalize_bars(raw)
        minute = minute[~minute.index.duplicated(keep="last")]
        if normalized != "1m":
            minute = _resample_cn_minutes(minute, normalized)
        return minute.loc[pd.Timestamp(start):pd.Timestamp(end)]

    def spot(self, symbols: list[str]) -> pd.DataFrame:
        if not get_config().data.xiaoshi_realtime_enabled:
            raise XiaoshiError("小石实时行情已在设置中关闭")
        identities = [_symbol_identity(symbol) for symbol in symbols]
        rows = self.client.quotes([
            {"symbol": code, "market": market, "instrument": "stock"}
            for code, market, _canonical in identities
        ])
        result = []
        for row, (code, _market, canonical) in zip(rows, identities, strict=True):
            result.append({
                "symbol": canonical,
                "code": code,
                "name": row["name"],
                "price": row.get("price"),
                "change_pct": row.get("change_pct"),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "volume": row.get("volume"),
                "amount": row.get("amount"),
                "source": self.name,
                "observed_at": row.get("observed_at"),
            })
        return pd.DataFrame(result)
