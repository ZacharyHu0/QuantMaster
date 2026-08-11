"""Immutable, content-addressed evidence for security-master denominators."""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from io import BufferedRandom
from pathlib import Path
from typing import Any

from quantmaster.config import get_config
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.trading_sessions import daily_signal_cutoff, market_date

SCHEMA_VERSION = 2
SUSPENSION_SCHEMA_VERSION = 2
SUSPENSION_CONTRACT = "tushare-suspend_d-trade-date-v1"
SUSPENSION_SOURCE = "tushare:suspend_d"
MEMBERSHIP_CONTRACT = "tushare-lifecycle-membership-v1"
TUSHARE_CATALOG_QUERY: dict[str, Any] = {
    "stock_basic": {"list_status": ["L", "D", "P"]},
    "fund_basic": {"market": "E", "status": ["L", "D"]},
    "index_basic": {"market": ["CSI", "SSE", "SZSE"]},
    "hk_basic": {"list_status": ["L", "D", "P"]},
}
TUSHARE_CATALOG_REQUESTS = tuple(
    [("stock_basic", "list_status", status) for status in ("L", "D", "P")]
    + [("fund_basic", "status", status) for status in ("L", "D")]
    + [("index_basic", "market", market) for market in ("CSI", "SSE", "SZSE")]
    + [("hk_basic", "list_status", status) for status in ("L", "D", "P")]
)
TUSHARE_MINIMUM_ASSET_COUNTS = {"CN:stock": 3000, "CN:etf": 100}
# Suspended-listing partitions do not contribute to the current active
# denominator.  Preserve and hash a successful empty response instead of
# inventing a member solely to make the partition non-empty.
TUSHARE_EMPTY_PARTITIONS = {
    ("stock_basic", "list_status", "P"),
    ("hk_basic", "list_status", "P"),
}


def tushare_catalog_request_params(
    endpoint: str, partition_key: str, partition_value: str,
) -> dict[str, Any]:
    identity = (endpoint, partition_key, partition_value)
    if identity not in set(TUSHARE_CATALOG_REQUESTS):
        raise InstrumentCatalogEvidenceError(f"Tushare 目录分区身份非法: {identity}")
    if endpoint == "stock_basic":
        return {
            "exchange": "",
            "list_status": partition_value,
            "fields": (
                "ts_code,symbol,name,fullname,enname,exchange,curr_type,"
                "list_status,list_date,delist_date"
            ),
        }
    if endpoint == "fund_basic":
        return {
            "market": "E",
            "status": partition_value,
            "fields": "ts_code,name,fund_type,status,list_date,delist_date",
        }
    if endpoint == "index_basic":
        return {
            "market": partition_value,
            "fields": "ts_code,name,fullname,market",
        }
    return {
        "list_status": partition_value,
        "fields": "ts_code,symbol,name,fullname,enname,list_status,list_date,delist_date",
    }


class InstrumentCatalogEvidenceError(RuntimeError):
    """The requested denominator has no intact authoritative catalog snapshot."""


def canonical_json(value: Any) -> str:
    return strict_json_dumps(value, sort_keys=True, default=str)


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class InstrumentCatalogSnapshot:
    snapshot_id: str
    acquired_at: str
    source: str
    records: tuple[dict[str, Any], ...]
    manifest: dict[str, Any]
    path: Path
    file_sha256: str
    file_size: int
    file_mtime_ns: int

    def evidence(self, *, market: str, asset_type: str, as_of: str) -> dict[str, Any]:
        symbols = snapshot_symbols(self, market=market, asset_type=asset_type, as_of=as_of)
        observation_as_of = str(self.manifest["active_as_of"])
        return {
            "schema_version": SCHEMA_VERSION,
            "snapshot_id": self.snapshot_id,
            "snapshot_sha256": self.snapshot_id,
            "records_sha256": self.manifest["records_sha256"],
            "acquired_at": self.acquired_at,
            "as_of": as_of,
            "membership_as_of": as_of,
            "observation_active_as_of": observation_as_of,
            "membership_reconstructed": as_of != observation_as_of,
            "membership_contract": MEMBERSHIP_CONTRACT,
            "source": self.source,
            "query": self.manifest["query"],
            "expected_count": len(symbols),
            "asset_snapshot_count": int(
                self.manifest["active_asset_counts"].get(
                    f"{market.upper()}:{asset_type.lower()}", 0,
                )
            ),
            "total_asset_snapshot_count": int(
                self.manifest["asset_counts"].get(f"{market.upper()}:{asset_type.lower()}", 0)
            ),
            "file_sha256": self.file_sha256,
            "file_size": self.file_size,
            "file_mtime_ns": self.file_mtime_ns,
            "relative_path": str(self.path.relative_to(get_config().data_root)),
        }


def _root() -> Path:
    return get_config().data_root / "instrument_catalog_snapshots" / "objects"


def _suspension_root() -> Path:
    return get_config().data_root / "suspension_snapshots" / "objects"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def _exclusive_file_lock(path: Path, *, timeout: float = 30.0) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    stream: BufferedRandom = path.open("a+b")
    if path.stat().st_size == 0:
        stream.write(b"\0")
        stream.flush()
    deadline = time.monotonic() + timeout
    while True:
        try:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(  # type: ignore[attr-defined]
                    stream.fileno(), msvcrt.LK_NBLCK, 1,  # type: ignore[attr-defined]
                )
            else:
                import fcntl

                fcntl.flock(  # type: ignore[attr-defined]
                    stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB,  # type: ignore[attr-defined]
                )
            break
        except (OSError, BlockingIOError):
            if time.monotonic() >= deadline:
                stream.close()
                raise InstrumentCatalogEvidenceError(
                    f"等待证据文件锁超时: {path.name}"
                ) from None
            time.sleep(0.02)
    try:
        yield
    finally:
        try:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(  # type: ignore[attr-defined]
                    stream.fileno(), msvcrt.LK_UNLCK, 1,  # type: ignore[attr-defined]
                )
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)  # type: ignore[attr-defined]
        finally:
            stream.close()


def _parse_acquired(value: str | datetime | None) -> datetime:
    if value is None:
        parsed = datetime.now(UTC)
    elif isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("catalog acquired_at 必须是带时区 ISO 时间") from exc
    if parsed.tzinfo is None:
        raise ValueError("catalog acquired_at 必须包含时区")
    return parsed.astimezone(UTC)


def _normalized_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    value = json.loads(canonical_json([dict(item) for item in records]))
    if not isinstance(value, list) or not value:
        raise InstrumentCatalogEvidenceError("证券目录快照为空")
    symbols: set[str] = set()
    for row in value:
        if not isinstance(row, dict):
            raise InstrumentCatalogEvidenceError("证券目录包含非对象记录")
        symbol = str(row.get("symbol") or "").strip().upper()
        market = str(row.get("market") or "").strip().upper()
        asset_type = str(row.get("asset_type") or "").strip().lower()
        if not symbol or not market or not asset_type:
            raise InstrumentCatalogEvidenceError("证券目录缺少 symbol/market/asset_type")
        if symbol in symbols:
            raise InstrumentCatalogEvidenceError(f"证券目录代码重复: {symbol}")
        symbols.add(symbol)
        row["symbol"] = symbol
        row["market"] = market
        row["asset_type"] = asset_type
    return sorted(value, key=lambda row: str(row["symbol"]))


def _canonical_record_list(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    value = json.loads(canonical_json([dict(item) for item in records]))
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise InstrumentCatalogEvidenceError("上游响应包含非对象记录")
    return value


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _catalog_date_text(value: Any) -> str:
    text = _text(value).replace("/", "-")
    if not text:
        return ""
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError as exc:
        raise InstrumentCatalogEvidenceError("Tushare 目录包含非法生命周期日期") from exc


def _tushare_partition_required_columns(endpoint: str) -> set[str]:
    required = {
        "stock_basic": {"ts_code", "list_status", "list_date", "delist_date"},
        "fund_basic": {"ts_code", "name", "fund_type", "status", "list_date", "delist_date"},
        "index_basic": {"ts_code", "name", "market"},
        # ``hk_basic`` does not consistently return its optional ``symbol``
        # field even when it is requested.  ``ts_code`` is the provider's
        # stable identity and the normalizer already derives the five-digit
        # display code from it when ``symbol`` is absent.
        "hk_basic": {"ts_code", "name", "list_status", "list_date", "delist_date"},
    }
    try:
        return required[endpoint]
    except KeyError as exc:
        raise InstrumentCatalogEvidenceError(
            f"不支持的 Tushare 目录分区 endpoint: {endpoint}"
        ) from exc


def _normalize_tushare_catalog_partition(
    endpoint: str,
    partition_key: str,
    partition_value: str,
    raw_records: Iterable[dict[str, Any]],
    *,
    raw_columns: Iterable[str],
) -> list[dict[str, Any]]:
    """Derive public catalog rows from one exact provider partition."""
    identity = (endpoint, partition_key, partition_value)
    if identity not in set(TUSHARE_CATALOG_REQUESTS):
        raise InstrumentCatalogEvidenceError(f"Tushare 目录分区身份非法: {identity}")
    raw = _canonical_record_list(raw_records)
    columns = {str(value) for value in raw_columns}
    missing = sorted(_tushare_partition_required_columns(endpoint) - columns)
    if missing:
        raise InstrumentCatalogEvidenceError(
            f"Tushare 目录分区 {identity} 缺少字段: {missing}"
        )
    if raw:
        observed_columns = {str(key) for row in raw for key in row}
        if observed_columns != columns:
            raise InstrumentCatalogEvidenceError(
                f"Tushare 目录分区 {identity} schema 与原始行不一致"
            )

    normalized: list[dict[str, Any]] = []
    for row in raw:
        if endpoint in {"stock_basic", "hk_basic"}:
            actual_partition = _text(row.get("list_status")).upper()
        elif endpoint == "fund_basic":
            actual_partition = _text(row.get("status")).upper()
        else:
            actual_partition = _text(row.get("market")).upper()
        if actual_partition != partition_value:
            raise InstrumentCatalogEvidenceError(
                f"Tushare 目录原始行越过分区边界: {identity} != {actual_partition}"
            )

        if endpoint == "stock_basic":
            symbol = _text(row.get("ts_code")).upper()
            if not symbol:
                raise InstrumentCatalogEvidenceError(f"{identity} 包含空 ts_code")
            normalized.append({
                "symbol": symbol,
                "provider_symbol": symbol,
                "name": _text(row.get("name")),
                "full_name": _text(row.get("fullname")),
                "en_name": _text(row.get("enname")),
                "market": "CN",
                "exchange": symbol.rsplit(".", 1)[-1],
                "asset_type": "stock",
                "currency": _text(row.get("curr_type")) or "CNY",
                "status": actual_partition,
                "list_date": _catalog_date_text(row.get("list_date")),
                "delist_date": _catalog_date_text(row.get("delist_date")),
            })
        elif endpoint == "fund_basic":
            symbol = _text(row.get("ts_code")).upper()
            name = _text(row.get("name"))
            if not symbol or not name:
                raise InstrumentCatalogEvidenceError(f"{identity} 包含空 ts_code/name")
            fund_type = _text(row.get("fund_type")).upper()
            normalized.append({
                "symbol": symbol,
                "name": name,
                "market": "CN",
                "exchange": symbol.rsplit(".", 1)[-1],
                "asset_type": (
                    "etf" if "ETF" in fund_type or "ETF" in name.upper()
                    or "交易型" in fund_type else "fund"
                ),
                "currency": "CNY",
                "status": actual_partition,
                "list_date": _catalog_date_text(row.get("list_date")),
                "delist_date": _catalog_date_text(row.get("delist_date")),
            })
        elif endpoint == "index_basic":
            symbol = _text(row.get("ts_code")).upper()
            name = _text(row.get("name"))
            if not symbol or not name:
                raise InstrumentCatalogEvidenceError(f"{identity} 包含空 ts_code/name")
            normalized.append({
                "symbol": symbol,
                "name": name,
                "full_name": _text(row.get("fullname")),
                "market": "CN",
                "exchange": symbol.rsplit(".", 1)[-1],
                "asset_type": "index",
                "currency": "CNY",
                "status": "listed",
            })
        else:
            provider = _text(row.get("ts_code")).upper()
            code = (_text(row.get("symbol")) or provider.partition(".")[0]).zfill(5)
            name = _text(row.get("name"))
            if not provider or not code or not name:
                raise InstrumentCatalogEvidenceError(
                    f"{identity} 包含空 ts_code/symbol/name"
                )
            normalized.append({
                "symbol": f"{code}.HK",
                "provider_symbol": provider,
                "name": name,
                "full_name": _text(row.get("fullname")),
                "en_name": _text(row.get("enname")),
                "market": "HK",
                "exchange": "HKEX",
                "asset_type": "stock",
                "currency": "HKD",
                "status": actual_partition,
                "list_date": _catalog_date_text(row.get("list_date")),
                "delist_date": _catalog_date_text(row.get("delist_date")),
            })
    return sorted(normalized, key=lambda row: str(row["symbol"]))


def tushare_catalog_partition_evidence(
    endpoint: str,
    partition_key: str,
    partition_value: str,
    *,
    params: dict[str, Any],
    raw_records: Iterable[dict[str, Any]],
    raw_columns: Iterable[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build replayable evidence for one catalog request, without trusting its summary."""
    params_value = json.loads(canonical_json(params))
    expected_params = tushare_catalog_request_params(
        endpoint, partition_key, partition_value,
    )
    if params_value != expected_params:
        raise InstrumentCatalogEvidenceError("Tushare 目录请求参数与固定查询契约不一致")
    raw = _canonical_record_list(raw_records)
    columns = sorted({str(value) for value in raw_columns})
    normalized = _normalize_tushare_catalog_partition(
        endpoint,
        partition_key,
        partition_value,
        raw,
        raw_columns=columns,
    )
    request_identity = {
        "endpoint": endpoint,
        "partition_key": partition_key,
        "partition_value": partition_value,
        "params": params_value,
    }
    return normalized, {
        **request_identity,
        "request_identity_sha256": content_hash(request_identity),
        "status": "success",
        "raw_record_count": len(raw),
        "raw_columns": columns,
        "raw_records_sha256": content_hash(raw),
        "raw_records": raw,
        "normalized_record_count": len(normalized),
        "normalized_records_sha256": content_hash(normalized),
    }


def _counts(records: Iterable[dict[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in records:
        key = f"{str(row['market']).upper()}:{str(row['asset_type']).lower()}"
        result[key] = result.get(key, 0) + 1
    return dict(sorted(result.items()))


def _validate_query(source: str, query: dict[str, Any]) -> None:
    if source == "tushare:catalog" and query != TUSHARE_CATALOG_QUERY:
        raise InstrumentCatalogEvidenceError(
            "Tushare 目录查询未完整覆盖 L/D/P 股票及 L/D 场内基金状态"
        )


def _validate_request_outcomes(
    source: str,
    outcomes: list[dict[str, Any]],
    records: list[dict[str, Any]],
) -> None:
    if source != "tushare:catalog":
        return
    keyed = {
        (
            str(item.get("endpoint") or ""),
            str(item.get("partition_key") or ""),
            str(item.get("partition_value") or ""),
        ): item
        for item in outcomes
    }
    required = set(TUSHARE_CATALOG_REQUESTS)
    if set(keyed) != required or len(outcomes) != len(required):
        missing = sorted(required - set(keyed))
        extra = sorted(set(keyed) - required)
        raise InstrumentCatalogEvidenceError(
            f"Tushare 目录子请求证据不完整（missing={missing}，extra={extra}）"
        )
    recovered: list[dict[str, Any]] = []
    for key, item in keyed.items():
        if item.get("status") != "success":
            raise InstrumentCatalogEvidenceError(f"Tushare 目录子请求未成功: {key}")
        if (
            int(item.get("raw_record_count") or 0) <= 0
            and key not in TUSHARE_EMPTY_PARTITIONS
        ):
            raise InstrumentCatalogEvidenceError(
                f"Tushare 目录子请求为空且没有独立空集基线: {key}"
            )
        raw_records = item.get("raw_records")
        raw_columns = item.get("raw_columns")
        if not isinstance(raw_records, list) or not isinstance(raw_columns, list):
            raise InstrumentCatalogEvidenceError(
                f"Tushare 目录子请求缺少可恢复原始响应: {key}"
            )
        normalized, rebuilt = tushare_catalog_partition_evidence(
            key[0],
            key[1],
            key[2],
            params=item.get("params") or {},
            raw_records=raw_records,
            raw_columns=raw_columns,
        )
        if canonical_json(rebuilt) != canonical_json(item):
            raise InstrumentCatalogEvidenceError(
                f"Tushare 目录子请求摘要无法从原始响应重算: {key}"
            )
        recovered.extend(normalized)
    if _normalized_records(recovered) != records:
        raise InstrumentCatalogEvidenceError(
            "Tushare 目录冻结 records 与逐分区原始响应的规范化结果不一致"
        )


def _parse_lifecycle_date(raw: Any, *, field: str, symbol: str) -> date | None:
    text = str(raw or "").strip().replace("/", "-")
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise InstrumentCatalogEvidenceError(
            f"证券目录 {symbol} {field} 非法"
        ) from exc


def _active_counts(records: Iterable[dict[str, Any]], *, as_of: date) -> dict[str, int]:
    active: list[dict[str, Any]] = []
    for row in records:
        symbol = str(row.get("symbol") or "")
        key = f"{str(row.get('market') or '').upper()}:{str(row.get('asset_type') or '').lower()}"
        if key not in TUSHARE_MINIMUM_ASSET_COUNTS:
            continue
        if str(row.get("status") or "").upper() != "L":
            continue
        listed = _parse_lifecycle_date(row.get("list_date"), field="list_date", symbol=symbol)
        delisted = _parse_lifecycle_date(
            row.get("delist_date"), field="delist_date", symbol=symbol,
        )
        if listed is None:
            # A same-day authoritative L partition proves current membership
            # even when the provider omits a newly listed fund's list_date.
            # The raw omission remains frozen and historical readers still
            # reject this row outside the observation date.
            if delisted is None or as_of <= delisted:
                active.append(row)
            continue
        if listed <= as_of and (delisted is None or as_of <= delisted):
            active.append(row)
    return _counts(active)


def _validate_lifecycle_contracts(
    records: Iterable[dict[str, Any]], *, observed_as_of: date,
) -> None:
    for row in records:
        key = (
            f"{str(row.get('market') or '').upper()}:"
            f"{str(row.get('asset_type') or '').lower()}"
        )
        if key not in TUSHARE_MINIMUM_ASSET_COUNTS:
            continue
        symbol = str(row.get("symbol") or "")
        status = str(row.get("status") or "").upper()
        if status not in {"L", "D", "P"}:
            raise InstrumentCatalogEvidenceError(
                f"证券目录 {symbol} 生命周期状态非法: {status}"
            )
        listed = _parse_lifecycle_date(
            row.get("list_date"), field="list_date", symbol=symbol,
        )
        delisted = _parse_lifecycle_date(
            row.get("delist_date"), field="delist_date", symbol=symbol,
        )
        if listed is not None and delisted is not None and listed > delisted:
            raise InstrumentCatalogEvidenceError(
                f"证券目录 {symbol} 的生命周期日期先后矛盾"
            )
        if status == "D":
            if delisted is not None and delisted > observed_as_of:
                raise InstrumentCatalogEvidenceError(
                    f"证券目录 {symbol} 的 D 生命周期与观测日不一致"
                )
def _validate_completeness(
    records: list[dict[str, Any]], source: str, root: Path, *, active_as_of: date,
) -> None:
    if source != "tushare:catalog":
        return
    counts = _active_counts(records, as_of=active_as_of)
    _validate_lifecycle_contracts(records, observed_as_of=active_as_of)
    for key, minimum in TUSHARE_MINIMUM_ASSET_COUNTS.items():
        if int(counts.get(key, 0)) < minimum:
            raise InstrumentCatalogEvidenceError(
                f"Tushare 目录 {key} 在 {active_as_of} 仅 {counts.get(key, 0)} 个 active，"
                f"低于独立完整性下界 {minimum}"
            )
    previous = [
        _load_snapshot_file(path)
        for path in root.glob("*.json")
        if path.is_file()
    ] if root.is_dir() else []
    previous = [item for item in previous if item.source == source]
    if not previous:
        return
    latest = max(previous, key=lambda item: _parse_acquired(item.acquired_at))
    for key, old_count in latest.manifest["active_asset_counts"].items():
        if old_count <= 0 or key not in TUSHARE_MINIMUM_ASSET_COUNTS:
            continue
        new_count = int(counts.get(key, 0))
        if new_count < int(old_count) * 0.9:
            raise InstrumentCatalogEvidenceError(
                f"Tushare active 目录 {key} 从 {old_count} 骤降至 {new_count}，"
                "拒绝冻结 partial 响应"
            )


def freeze_instrument_catalog(
    records: Iterable[dict[str, Any]],
    *,
    source: str,
    query: dict[str, Any],
    request_outcomes: Iterable[dict[str, Any]] = (),
    acquired_at: str | datetime | None = None,
) -> InstrumentCatalogSnapshot:
    """Atomically freeze one provider response; an observation identity is immutable."""
    acquired = _parse_acquired(acquired_at)
    normalized = _normalized_records(records)
    query_value = json.loads(canonical_json(query))
    outcomes = json.loads(canonical_json([dict(item) for item in request_outcomes]))
    _validate_query(source, query_value)
    _validate_request_outcomes(source, outcomes, normalized)
    root = _root()
    active_as_of = market_date(acquired)
    _validate_completeness(
        normalized, source, root, active_as_of=active_as_of,
    )
    schema = sorted({str(key) for row in normalized for key in row})
    core = {
        "schema_version": SCHEMA_VERSION,
        "source": str(source),
        "query": query_value,
        "request_outcomes": outcomes,
        "acquired_at": acquired.isoformat(),
        "observation_id": content_hash({
            "source": str(source), "acquired_at": acquired.isoformat(),
        }),
        "record_schema": schema,
        "record_count": len(normalized),
        "asset_counts": _counts(normalized),
        "active_as_of": active_as_of.isoformat(),
        "active_asset_counts": _active_counts(normalized, as_of=active_as_of),
        "records_sha256": content_hash(normalized),
        "records": normalized,
    }
    snapshot_id = content_hash(core)
    payload = {**core, "snapshot_id": snapshot_id}
    serialized = canonical_json(payload).encode("utf-8")
    root.mkdir(parents=True, exist_ok=True)

    for existing in root.glob("*.json"):
        loaded = _load_snapshot_file(existing)
        if loaded.manifest["observation_id"] == core["observation_id"]:
            if loaded.snapshot_id != snapshot_id:
                raise InstrumentCatalogEvidenceError(
                    "同一 catalog observation_id 对应不同内容，拒绝覆盖"
                )
            return loaded

    target = root / f"{snapshot_id}.json"
    if target.exists():
        if target.read_bytes() != serialized:
            raise InstrumentCatalogEvidenceError("内容寻址目录对象发生冲突")
        return _load_snapshot_file(target)
    staged = root / f".{snapshot_id}.{uuid.uuid4().hex}.tmp"
    try:
        with staged.open("xb") as stream:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(staged, target)
    finally:
        staged.unlink(missing_ok=True)
    return _load_snapshot_file(target)


def _load_snapshot_file(path: Path) -> InstrumentCatalogSnapshot:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InstrumentCatalogEvidenceError(f"证券目录快照不可读: {path.name}") from exc
    if not isinstance(payload, dict) or int(payload.get("schema_version") or 0) != SCHEMA_VERSION:
        raise InstrumentCatalogEvidenceError(f"证券目录快照 schema 非法: {path.name}")
    if raw != canonical_json(payload).encode("utf-8"):
        raise InstrumentCatalogEvidenceError(f"证券目录快照文件身份失败: {path.name}")
    snapshot_id = str(payload.get("snapshot_id") or "")
    core = {key: value for key, value in payload.items() if key != "snapshot_id"}
    if content_hash(core) != snapshot_id or path.stem != snapshot_id:
        raise InstrumentCatalogEvidenceError(f"证券目录快照 self-hash 失败: {path.name}")
    records = _normalized_records(payload.get("records") or ())
    if content_hash(records) != payload.get("records_sha256"):
        raise InstrumentCatalogEvidenceError(f"证券目录 records hash 失败: {path.name}")
    if _counts(records) != payload.get("asset_counts"):
        raise InstrumentCatalogEvidenceError(f"证券目录 asset count 失败: {path.name}")
    if len(records) != int(payload.get("record_count") or 0):
        raise InstrumentCatalogEvidenceError(f"证券目录 record count 失败: {path.name}")
    try:
        active_as_of = date.fromisoformat(str(payload.get("active_as_of") or ""))
    except ValueError as exc:
        raise InstrumentCatalogEvidenceError(
            f"证券目录 active_as_of 非法: {path.name}"
        ) from exc
    if active_as_of != market_date(_parse_acquired(str(payload.get("acquired_at") or ""))):
        raise InstrumentCatalogEvidenceError(
            f"证券目录 active_as_of 与 acquired_at 不一致: {path.name}"
        )
    _validate_lifecycle_contracts(records, observed_as_of=active_as_of)
    if _active_counts(records, as_of=active_as_of) != payload.get("active_asset_counts"):
        raise InstrumentCatalogEvidenceError(
            f"证券目录 active asset count 失败: {path.name}"
        )
    _validate_query(str(payload.get("source") or ""), payload.get("query") or {})
    _validate_request_outcomes(
        str(payload.get("source") or ""),
        payload.get("request_outcomes") or [],
        records,
    )
    if str(payload.get("source") or "") == "tushare:catalog":
        for key, minimum in TUSHARE_MINIMUM_ASSET_COUNTS.items():
            if int(payload["active_asset_counts"].get(key, 0)) < minimum:
                raise InstrumentCatalogEvidenceError(
                    f"证券目录 active {key} 低于完整性下界: {path.name}"
                )
    stat = path.stat()
    return InstrumentCatalogSnapshot(
        snapshot_id=snapshot_id,
        acquired_at=str(payload["acquired_at"]),
        source=str(payload["source"]),
        records=tuple(records),
        manifest=payload,
        path=path,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        file_size=stat.st_size,
        file_mtime_ns=stat.st_mtime_ns,
    )


def snapshot_symbols(
    snapshot: InstrumentCatalogSnapshot,
    *,
    market: str,
    asset_type: str,
    as_of: str,
) -> set[str]:
    def parse_date(raw: object, *, field: str, symbol: str) -> date | None:
        text = str(raw or "").strip().replace("/", "-")
        if not text:
            return None
        if len(text) == 8 and text.isdigit():
            text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
        try:
            return date.fromisoformat(text[:10])
        except ValueError as exc:
            raise InstrumentCatalogEvidenceError(
                f"证券目录 {symbol} {field} 非法"
            ) from exc

    target = date.fromisoformat(as_of)
    observation = date.fromisoformat(str(snapshot.manifest["active_as_of"]))
    if target > observation:
        raise InstrumentCatalogEvidenceError(
            f"证券目录观测日 {observation} 早于目标日 {target}，不能推导未来成员"
        )
    result: set[str] = set()
    for row in snapshot.records:
        if str(row["market"]).upper() != market.upper():
            continue
        if str(row["asset_type"]).lower() != asset_type.lower():
            continue
        status = str(row.get("status") or "").upper()
        if status not in {"L", "LISTED", "ACTIVE", "D", "DELISTED", "P", "SUSPENDED"}:
            continue
        listed = parse_date(row.get("list_date"), field="list_date", symbol=str(row["symbol"]))
        delisted = parse_date(
            row.get("delist_date"), field="delist_date", symbol=str(row["symbol"]),
        )
        if status in {"L", "LISTED", "ACTIVE"}:
            if listed is None:
                if target == observation:
                    if delisted is None or target <= delisted:
                        result.add(str(row["symbol"]))
                    continue
                raise InstrumentCatalogEvidenceError(
                    f"证券目录 {row['symbol']} 缺少历史可用的 list_date"
                )
            if listed <= target and (delisted is None or target <= delisted):
                result.add(str(row["symbol"]))
            continue

        # The provider's D/P partitions describe state at acquisition time.
        # They are not current members, but a complete lifecycle can prove that
        # the instrument existed on an earlier target date.  Never re-date the
        # observation itself: evidence retains the real acquired_at and records
        # the requested membership_as_of separately.
        if target == observation:
            continue
        if status in {"D", "DELISTED"}:
            if delisted is None:
                raise InstrumentCatalogEvidenceError(
                    f"证券目录 {row['symbol']} 缺少历史可用的 delist_date"
                )
            if target > delisted:
                continue
        elif delisted is not None and target > delisted:
            continue
        if listed is None:
            raise InstrumentCatalogEvidenceError(
                f"证券目录 {row['symbol']} 缺少历史可用的 list_date"
            )
        if listed <= target:
            result.add(str(row["symbol"]))
    if not result:
        raise InstrumentCatalogEvidenceError(
            f"证券目录在 {as_of} 没有 {market}:{asset_type} expected 成员"
        )
    return result


def _select_catalog_membership(
    candidates: list[tuple[datetime, InstrumentCatalogSnapshot]],
    *,
    target: date,
    market: str,
    asset_type: str,
    newest_first: bool,
) -> tuple[InstrumentCatalogSnapshot, set[str], dict[str, Any]]:
    failures: list[str] = []
    for _acquired, snapshot in sorted(
        candidates, key=lambda item: item[0], reverse=newest_first,
    ):
        try:
            symbols = snapshot_symbols(
                snapshot, market=market, asset_type=asset_type, as_of=target.isoformat(),
            )
        except InstrumentCatalogEvidenceError as exc:
            failures.append(str(exc))
            continue
        evidence = snapshot.evidence(
            market=market, asset_type=asset_type, as_of=target.isoformat(),
        )
        return snapshot, symbols, evidence
    detail = failures[0] if failures else "生命周期证据不足"
    raise InstrumentCatalogEvidenceError(
        f"没有可推导 {target.isoformat()} 成员的证券目录快照：{detail}"
    )


def load_instrument_catalog_snapshot(
    *,
    as_of: str | None = None,
    market: str = "CN",
    asset_type: str = "stock",
    max_age_days: int = 7,
) -> tuple[InstrumentCatalogSnapshot, set[str], dict[str, Any]]:
    """Load a verified catalog object; never infer completeness from InstrumentStore."""
    root = _root()
    paths = sorted(root.glob("*.json")) if root.is_dir() else []
    if not paths:
        raise InstrumentCatalogEvidenceError("没有不可变证券目录快照")
    snapshots = [
        snapshot for path in paths
        if (snapshot := _load_snapshot_file(path)).source == "tushare:catalog"
    ]
    if not snapshots:
        raise InstrumentCatalogEvidenceError("没有权威 Tushare 不可变证券目录快照")
    observations: dict[str, str] = {}
    for snapshot in snapshots:
        identity = str(snapshot.manifest.get("observation_id") or "")
        previous = observations.setdefault(identity, snapshot.snapshot_id)
        if previous != snapshot.snapshot_id:
            raise InstrumentCatalogEvidenceError(
                "同一 catalog observation_id 存在冲突对象"
            )
    target = date.fromisoformat(as_of) if as_of else market_date()
    cutoff = daily_signal_cutoff(target)
    candidates = []
    now = datetime.now(UTC)
    for snapshot in snapshots:
        acquired = _parse_acquired(snapshot.acquired_at)
        local = acquired.astimezone(cutoff.tzinfo)
        if as_of:
            # A later full L/D/P observation may prove earlier membership from
            # lifecycle dates.  It must be acquired after the target close and
            # keeps its real acquisition timestamp; it is never relabelled as a
            # target-day observation.
            eligible = local >= cutoff
        else:
            eligible = local <= now.astimezone(cutoff.tzinfo)
            eligible = eligible and local >= daily_signal_cutoff(local.date())
            eligible = eligible and acquired >= now - timedelta(days=max_age_days)
        if eligible:
            candidates.append((acquired, snapshot))
    if not candidates:
        scope = f"as_of={target.isoformat()}" if as_of else "current"
        raise InstrumentCatalogEvidenceError(f"没有满足截止时间与新鲜度的证券目录快照: {scope}")
    if as_of:
        # Prefer observations acquired on the requested session itself.  A
        # later catalog can reconstruct lifecycle membership, but it must not
        # overwrite a target-day metadata snapshot with a late backfill of an
        # older effective date when a same-day closing observation exists.
        same_day = [
            item
            for item in candidates
            if item[0].astimezone(cutoff.tzinfo).date() == target
        ]
        if same_day:
            candidates = same_day
    return _select_catalog_membership(
        candidates,
        target=target,
        market=market,
        asset_type=asset_type,
        # For an explicit historical target, use the newest verified
        # post-close observation that can reconstruct that target.  Choosing
        # the oldest candidate would silently retain a pre-close directory and
        # miss same-day metadata updates.
        newest_first=True,
    )


def verify_instrument_catalog_evidence(
    evidence: dict[str, Any],
    *,
    market: str = "CN",
    asset_type: str = "stock",
) -> tuple[InstrumentCatalogSnapshot, set[str]]:
    """Reopen an exact catalog artifact and reject path, hash, or file-identity drift."""
    relative = str(evidence.get("relative_path") or "")
    root = get_config().data_root.resolve()
    path = (root / relative).resolve()
    if root not in path.parents or not path.is_file():
        raise InstrumentCatalogEvidenceError("证券目录 evidence 路径不存在或越界")
    snapshot = _load_snapshot_file(path)
    if snapshot.source != "tushare:catalog":
        raise InstrumentCatalogEvidenceError("证券目录 evidence 来源不受信任")
    if snapshot.snapshot_id != str(evidence.get("snapshot_id") or ""):
        raise InstrumentCatalogEvidenceError("证券目录 evidence snapshot_id 不匹配")
    if snapshot.file_sha256 != str(evidence.get("file_sha256") or ""):
        raise InstrumentCatalogEvidenceError("证券目录 evidence 文件哈希不匹配")
    if snapshot.file_size != int(evidence.get("file_size") or 0):
        raise InstrumentCatalogEvidenceError("证券目录 evidence 文件大小不匹配")
    if snapshot.file_mtime_ns != int(evidence.get("file_mtime_ns") or 0):
        raise InstrumentCatalogEvidenceError("证券目录 evidence 文件身份已变化")
    as_of = str(evidence.get("as_of") or "")
    expected_membership = {
        "membership_as_of": as_of,
        "observation_active_as_of": str(snapshot.manifest["active_as_of"]),
        "membership_reconstructed": as_of != str(snapshot.manifest["active_as_of"]),
        "membership_contract": MEMBERSHIP_CONTRACT,
    }
    if any(evidence.get(key) != value for key, value in expected_membership.items()):
        raise InstrumentCatalogEvidenceError("证券目录 evidence 成员日期契约不匹配")
    symbols = snapshot_symbols(
        snapshot, market=market, asset_type=asset_type, as_of=as_of,
    )
    if len(symbols) != int(evidence.get("expected_count") or 0):
        raise InstrumentCatalogEvidenceError("证券目录 evidence expected_count 不匹配")
    return snapshot, symbols


def _compact_trade_date(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        text = text[:10].replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise InstrumentCatalogEvidenceError("suspend_d 原始 trade_date 非法")
    return text


def tushare_suspension_request_evidence(
    trade_date: str,
    *,
    raw_records: Iterable[dict[str, Any]],
    raw_columns: Iterable[str],
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    """Derive the no-trade set from the exact full-day ``suspend_d`` response."""
    target = date.fromisoformat(trade_date)
    compact = target.strftime("%Y%m%d")
    params = {
        "trade_date": compact,
        "fields": "ts_code,trade_date,suspend_timing,suspend_type",
    }
    raw = _canonical_record_list(raw_records)
    columns = sorted({str(value) for value in raw_columns})
    required = {"ts_code", "trade_date", "suspend_timing", "suspend_type"}
    if not required.issubset(columns):
        raise InstrumentCatalogEvidenceError(
            f"suspend_d 原始响应缺少字段: {sorted(required - set(columns))}"
        )
    if raw:
        observed_columns = {str(key) for row in raw for key in row}
        if observed_columns != set(columns):
            raise InstrumentCatalogEvidenceError("suspend_d schema 与原始行不一致")
    rows: list[dict[str, str]] = []
    for raw_row in raw:
        if _compact_trade_date(raw_row.get("trade_date")) != compact:
            raise InstrumentCatalogEvidenceError("suspend_d 原始响应混入非目标交易日")
        symbol = _text(raw_row.get("ts_code")).upper()
        if not symbol:
            raise InstrumentCatalogEvidenceError("suspend_d 原始响应包含空 ts_code")
        suspend_type = _text(raw_row.get("suspend_type") or "S")
        suspend_timing = _text(raw_row.get("suspend_timing"))
        if suspend_type.upper().startswith("R"):
            continue
        if suspend_timing and "09:30" not in suspend_timing:
            continue
        rows.append({
            "symbol": symbol,
            "trade_date": compact,
            "suspend_type": suspend_type,
            "suspend_timing": suspend_timing,
        })
    rows.sort(
        key=lambda item: (
            item["symbol"], item["suspend_type"], item["suspend_timing"],
        )
    )
    request_identity = {"endpoint": "suspend_d", "params": params}
    evidence = {
        **request_identity,
        "request_identity_sha256": content_hash(request_identity),
        "status": "success",
        "raw_record_count": len(raw),
        "raw_columns": columns,
        "raw_records_sha256": content_hash(raw),
        "raw_records": raw,
        "normalized_record_count": len(rows),
        "normalized_records_sha256": content_hash(rows),
    }
    return rows, evidence


def _validate_suspension_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], datetime]:
    if payload.get("contract") != SUSPENSION_CONTRACT:
        raise InstrumentCatalogEvidenceError("suspend_d contract 不受信任")
    if payload.get("source") != SUSPENSION_SOURCE:
        raise InstrumentCatalogEvidenceError("suspend_d source 不受信任")
    if int(payload.get("schema_version") or 0) != SUSPENSION_SCHEMA_VERSION:
        raise InstrumentCatalogEvidenceError("suspend_d schema 不受信任")
    trade_date = str(payload.get("trade_date") or "")
    target_date = date.fromisoformat(trade_date)
    request = payload.get("request_evidence")
    if not isinstance(request, dict):
        raise InstrumentCatalogEvidenceError("suspend_d 缺少可恢复原始响应")
    rows, rebuilt = tushare_suspension_request_evidence(
        trade_date,
        raw_records=request.get("raw_records") or (),
        raw_columns=request.get("raw_columns") or (),
    )
    if canonical_json(rebuilt) != canonical_json(request):
        raise InstrumentCatalogEvidenceError("suspend_d 请求摘要无法从原始响应重算")
    if canonical_json(rows) != canonical_json(payload.get("rows") or []):
        raise InstrumentCatalogEvidenceError("suspend_d rows 与原始响应不一致")
    symbols = sorted({item["symbol"] for item in rows})
    if symbols != payload.get("symbols"):
        raise InstrumentCatalogEvidenceError("suspend_d symbols 与原始响应不一致")
    core = {
        key: value for key, value in payload.items()
        if key not in {"content_hash", "symbols"}
    }
    if content_hash(core) != str(payload.get("content_hash") or ""):
        raise InstrumentCatalogEvidenceError("suspend_d 响应内容哈希不匹配")
    acquired = _parse_acquired(str(payload.get("acquired_at") or ""))
    cutoff = daily_signal_cutoff(target_date)
    acquired_local = acquired.astimezone(cutoff.tzinfo)
    if acquired_local < cutoff:
        raise InstrumentCatalogEvidenceError(
            "suspend_d 证据早于目标日收盘，不能证明完整停牌集合"
        )
    return core, acquired


def freeze_suspension_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    """Freeze one official suspend_d response as an immutable no-trade artifact."""
    core, acquired = _validate_suspension_payload(payload)
    digest = str(payload["content_hash"])
    trade_date = str(core.get("trade_date") or "")
    symbols = list(payload["symbols"])
    frozen = {**core, "content_hash": digest, "symbols": symbols}
    serialized = canonical_json(frozen).encode("utf-8")
    root = _suspension_root()
    root.mkdir(parents=True, exist_ok=True)
    target = root / f"{trade_date}--{digest}.json"
    with _exclusive_file_lock(root.parent / ".locks" / f"{trade_date}.lock"):
        existing = sorted(root.glob(f"{trade_date}--*.json"))
        if len(existing) > 1:
            raise InstrumentCatalogEvidenceError(
                f"{trade_date} 已存在冲突的 suspend_d 证据"
            )
        if existing:
            prior = load_suspension_snapshot(trade_date)
            prior_payload = json.loads(existing[0].read_text(encoding="utf-8"))
            if canonical_json(prior_payload.get("rows") or []) != canonical_json(
                frozen["rows"]
            ):
                raise InstrumentCatalogEvidenceError(
                    f"{trade_date} suspend_d 同日观测内容冲突"
                )
            return prior
        staged = root / f".{digest}.{uuid.uuid4().hex}.tmp"
        try:
            with staged.open("xb") as stream:
                stream.write(serialized)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(staged, target)
        finally:
            staged.unlink(missing_ok=True)
    return {
        "trade_date": trade_date,
        "acquired_at": acquired.isoformat(),
        "content_hash": digest,
        "symbols": symbols,
        "source": SUSPENSION_SOURCE,
        "contract": SUSPENSION_CONTRACT,
        "schema_version": SUSPENSION_SCHEMA_VERSION,
        "request_identity_sha256": core["request_evidence"]["request_identity_sha256"],
        "relative_path": str(target.relative_to(get_config().data_root)),
        "file_sha256": _file_sha256(target),
    }


def load_suspension_snapshot(trade_date: str) -> dict[str, Any]:
    paths = sorted(_suspension_root().glob(f"{trade_date}--*.json"))
    if len(paths) != 1:
        raise InstrumentCatalogEvidenceError(
            f"{trade_date} 缺少唯一、不可变的 suspend_d no-trade 证据"
        )
    path = paths[0]
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InstrumentCatalogEvidenceError("suspend_d 快照不可读") from exc
    if raw != canonical_json(payload).encode("utf-8"):
        raise InstrumentCatalogEvidenceError("suspend_d 快照文件身份失败")
    if str(payload.get("trade_date") or "") != trade_date:
        raise InstrumentCatalogEvidenceError("suspend_d 快照文件名与 trade_date 不一致")
    core, _acquired = _validate_suspension_payload(payload)
    if (
        path.stem.rsplit("--", 1)[-1] != payload.get("content_hash")
    ):
        raise InstrumentCatalogEvidenceError("suspend_d 快照哈希失败")
    return {
        "trade_date": trade_date,
        "acquired_at": str(payload.get("acquired_at") or ""),
        "content_hash": str(payload["content_hash"]),
        "symbols": list(payload.get("symbols") or []),
        "source": SUSPENSION_SOURCE,
        "contract": SUSPENSION_CONTRACT,
        "schema_version": SUSPENSION_SCHEMA_VERSION,
        "request_identity_sha256": core["request_evidence"]["request_identity_sha256"],
        "relative_path": str(path.relative_to(get_config().data_root)),
        "file_sha256": _file_sha256(path),
    }


def load_or_fetch_suspension_snapshot(source: Any, trade_date: str) -> dict[str, Any]:
    try:
        return load_suspension_snapshot(trade_date)
    except InstrumentCatalogEvidenceError:
        if source is None or not hasattr(source, "suspension_snapshot"):
            raise
    payload = source.suspension_snapshot(trade_date)
    return freeze_suspension_snapshot(payload)
