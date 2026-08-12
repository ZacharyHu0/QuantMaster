"""Versioned rotation cache, authoritative preferences and durable jobs."""

from __future__ import annotations

import hashlib
import json
import logging
import numbers
import os
import sqlite3
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from functools import singledispatch
from io import BufferedRandom
from pathlib import Path
from typing import Any

import pandas as pd

from quantmaster.config import get_config
from quantmaster.runtime.derived import DerivedArtifactCatalog
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.sqlite import connect_sqlite, migrate_schema

ACTIVE_JOB_STATUSES = frozenset({"queued", "running", "cancelling"})
TERMINAL_JOB_STATUSES = frozenset({"completed", "failed", "cancelled"})
logger = logging.getLogger(__name__)
ETF_METADATA_HISTORY_SCHEMA_VERSION = "2.0"
_ETF_METADATA_CONTENT_FIELDS = (
    "symbol", "name", "benchmark", "benchmark_code", "index_name",
    "benchmark_level", "benchmark_type", "index_provider", "index_type",
    "fund_type", "invest_type", "mgr_name", "custod_name", "mgt_fee",
    "etf_type", "exchange", "asset_type", "status", "list_date",
    "delist_date", "category", "asset_class", "sector_id", "sector_name",
    "normalized_index", "classification_source", "classification_confidence",
    "metadata_source", "updated_at", "directory_member_source",
    "directory_member_observed_at", "effective_date", "observed_at",
    "directory_source", "directory_acquired_at", "directory_cutoff_at",
    "directory_freshness", "directory_master_record_count",
    "directory_master_batch_record_count", "directory_expected_symbols",
    "directory_observed_symbols", "directory_quality_reason",
    "directory_catalog_snapshot_id", "directory_catalog_records_sha256",
    "directory_catalog_file_sha256", "directory_catalog_file_size",
    "directory_catalog_file_mtime_ns", "directory_catalog_relative_path",
    "directory_catalog_as_of", "directory_catalog_expected_count",
    "directory_master_snapshot_sha256", "directory_snapshot_id",
    "directory_attestation_sha256", "directory_complete", "market",
)
_ETF_METADATA_DERIVED_COLUMNS = frozenset(
    {"observation_id", "observation_content_sha256", "observation_integrity"}
)
_ETF_METADATA_LOCK = threading.RLock()
_SNAPSHOT_WINDOWS = (1, 3, 5, 20)
_SNAPSHOT_ITEM_COLUMNS = (
    "kind,snapshot_id,item_key,position,name,level,stage,grade,category,benchmark,"
    "primary_industry_name,score_1,change_1,excess_1,amount_1,advance_1,score_3,"
    "change_3,excess_3,amount_3,advance_3,score_5,change_5,excess_5,amount_5,"
    "advance_5,score_20,change_20,excess_20,amount_20,advance_20,grade_1,grade_3,"
    "grade_5,grade_20,focus_1,focus_3,focus_5,focus_20,coverage,flow_1,flow_3,"
    "flow_5,flow_20,daily_flow,flow,streak,payload_json"
)


@dataclass
class _SnapshotWriteBatch:
    headers: list[tuple[Any, ...]]
    items: list[tuple[Any, ...]]
    details: list[tuple[Any, ...]]
    artifacts: list[tuple[str, str, str, str]]


class RotationIntegrityError(RuntimeError):
    """A rebuildable rotation artifact exists but failed integrity validation."""


@contextmanager
def _etf_metadata_file_lock(path: Path, timeout: float = 0.2) -> Iterator[None]:
    """Serialize the parquet/manifest pair across worker processes."""

    path.parent.mkdir(parents=True, exist_ok=True)
    stream: BufferedRandom = path.open("a+b")
    if path.stat().st_size == 0:
        stream.write(b"\0")
        stream.flush()
    deadline = time.monotonic() + max(0.1, timeout)
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
                raise RotationIntegrityError("等待 ETF 元数据历史文件锁超时") from None
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


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@singledispatch
def _canonical_metadata_value(value: Any) -> Any:
    """Return one stable JSON value for an otherwise unsupported scalar."""
    converted = _optional_metadata_conversion(value, "item")
    if converted is not _UNCONVERTED and converted is not value:
        return _canonical_metadata_value(converted)
    if not isinstance(value, (str, bytes)):
        converted = _optional_metadata_conversion(value, "tolist")
        if converted is not _UNCONVERTED and converted is not value:
            return _canonical_metadata_value(converted)
    if _metadata_value_is_missing(value):
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return _canonical_metadata_fallback(value)


@_canonical_metadata_value.register(type(None))
def _canonical_none(_value: None) -> None:
    return None


@_canonical_metadata_value.register(dict)
def _canonical_mapping(value: dict[Any, Any]) -> dict[str, Any]:
    return {
        str(key): _canonical_metadata_value(item)
        for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
    }


@_canonical_metadata_value.register(set)
@_canonical_metadata_value.register(frozenset)
def _canonical_set(value: set[Any] | frozenset[Any]) -> list[Any]:
    return sorted(
        (_canonical_metadata_value(item) for item in value),
        key=lambda item: strict_json_dumps(item, sort_keys=True),
    )


@_canonical_metadata_value.register(list)
@_canonical_metadata_value.register(tuple)
def _canonical_sequence(value: list[Any] | tuple[Any, ...]) -> list[Any]:
    return [_canonical_metadata_value(item) for item in value]


@_canonical_metadata_value.register(pd.Timestamp)
def _canonical_timestamp(value: pd.Timestamp) -> str:
    return value.isoformat()


@_canonical_metadata_value.register(bool)
def _canonical_bool(value: bool) -> bool:
    return value


@_canonical_metadata_value.register(numbers.Integral)
def _canonical_integral(value: numbers.Integral) -> str:
    return str(int(value))


@_canonical_metadata_value.register(numbers.Real)
def _canonical_real(value: numbers.Real) -> str | None:
    try:
        if pd.isna(value):
            return None
        return format(float(value), ".17g")
    except (TypeError, ValueError, OverflowError):
        return str(value)


_UNCONVERTED = object()


def _optional_metadata_conversion(value: Any, method: str) -> Any:
    conversion = getattr(value, method, None)
    if conversion is None:
        return _UNCONVERTED
    try:
        return conversion()
    except (TypeError, ValueError):
        return _UNCONVERTED


def _metadata_value_is_missing(value: Any) -> bool:
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _canonical_metadata_fallback(value: Any) -> Any:
    converted = _optional_metadata_conversion(value, "isoformat")
    return str(value) if converted is _UNCONVERTED else converted


def _metadata_content_hash(row: dict[str, Any]) -> str:
    payload = {}
    for key in _ETF_METADATA_CONTENT_FIELDS:
        normalized = _canonical_metadata_value(row.get(key))
        if normalized is not None:
            payload[str(key)] = normalized
    return _hash_text(strict_json_dumps(payload, sort_keys=True))


def _metadata_observation_id(symbol: str, observed_at: str) -> str:
    return "etf_meta_observation_" + _hash_text(
        strict_json_dumps(
            {
                "schema_version": ETF_METADATA_HISTORY_SCHEMA_VERSION,
                "symbol": str(symbol).upper(),
                "observed_at": observed_at,
            },
            sort_keys=True,
        )
    )


def _optional_number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _snapshot_focus_score(
    change: float | None,
    excess: float | None,
    amount: float | None,
    advance: float | None,
    grade: str,
) -> int:
    return sum((
        change is not None and change > 0,
        excess is not None and excess > 0,
        advance is not None and advance >= 0.5,
        amount is not None and amount > 0,
        grade in {"A", "B"},
    ))


def _snapshot_window_fields(item: dict[str, Any]) -> tuple[list[Any], list[str], list[int]]:
    signals = item.get("signals") if isinstance(item.get("signals"), dict) else {}
    scores = item.get("scores") if isinstance(item.get("scores"), dict) else {}
    values: list[Any] = []
    grades: list[str] = []
    focus: list[int] = []
    for window in _SNAPSHOT_WINDOWS:
        signal = signals.get(str(window))
        score = scores.get(str(window))
        signal = signal if isinstance(signal, dict) else {}
        score = score if isinstance(score, dict) else {}
        change = _optional_number(signal.get("rotation_change_pp"))
        excess = _optional_number(signal.get("excess_return"))
        amount = _optional_number(signal.get("amount_activity"))
        advance = _optional_number(signal.get("advance_ratio"))
        grade = str(score.get("grade") or "")
        values.extend((
            _optional_number(score.get("score")),
            change, excess, amount, advance,
        ))
        grades.append(grade)
        focus.append(_snapshot_focus_score(change, excess, amount, advance, grade))
    return values, grades, focus


def _snapshot_item_row(
    kind: str,
    snapshot_id: str,
    position: int,
    raw_item: dict[str, Any],
) -> tuple[Any, ...]:
    item = dict(raw_item)
    item_key = str(item.get("code") or item.get("symbol") or "").upper()
    if not item_key:
        raise RotationIntegrityError(f"{kind} 快照列表存在无标识项目")
    primary = item.get("primary_industry")
    primary_name = str(primary.get("name") or "") if isinstance(primary, dict) else ""
    window_values, grade_values, focus_values = _snapshot_window_fields(item)
    flows = item.get("flows") if isinstance(item.get("flows"), dict) else {}
    flow_values = [_optional_number(flows.get(str(window))) for window in _SNAPSHOT_WINDOWS]
    selected_flow = flow_values[2]
    return (
        str(kind), snapshot_id, item_key, position,
        str(item.get("name") or ""), str(item.get("level") or ""),
        str(item.get("stage") or ""), "",
        str(item.get("category") or ""), str(item.get("benchmark") or ""),
        primary_name, *window_values, *grade_values, *focus_values,
        _optional_number(item.get("coverage")), *flow_values,
        _optional_number(item.get("flow")),
        _optional_number(selected_flow if selected_flow is not None else item.get("flow")),
        _optional_number(item.get("flow_streak_sessions")), strict_json_dumps(item),
    )


def _snapshot_detail_rows(
    kind: str,
    snapshot_id: str,
    details: Any,
) -> list[tuple[str, str, str, str]]:
    if not isinstance(details, dict):
        return []
    return [
        (str(kind), snapshot_id, str(key).upper(), strict_json_dumps(value))
        for key, value in details.items()
        if isinstance(value, dict)
    ]


def _selected_snapshot_keys(
    allowed_keys: set[str] | None,
    include_l1: bool,
) -> tuple[str, list[str]] | None:
    if allowed_keys is None:
        return "", []
    keys = sorted({str(value).upper() for value in allowed_keys})
    if not keys:
        return ("level='L1'", []) if include_l1 else None
    placeholders = ",".join("?" for _ in keys)
    if include_l1:
        return f"(level='L1' OR item_key IN ({placeholders}))", keys
    return f"item_key IN ({placeholders})", keys


def _snapshot_item_filters(
    kind: str,
    snapshot_id: str,
    *,
    query: str,
    level: str,
    allowed_keys: set[str] | None,
    include_l1: bool,
    stage: str,
    category: str,
    grade: str,
    window: int,
) -> tuple[str, list[Any]] | None:
    clauses = ["kind=?", "snapshot_id=?"]
    params: list[Any] = [str(kind), snapshot_id]
    needle = str(query).strip().casefold()
    if needle:
        clauses.append(
            "(lower(name) LIKE ? OR lower(item_key) LIKE ? "
            "OR lower(primary_industry_name) LIKE ? OR lower(benchmark) LIKE ?)"
        )
        like = f"%{needle}%"
        params.extend((like, like, like, like))
    if level:
        clauses.append("level=?")
        params.append(str(level))
    key_filter = _selected_snapshot_keys(allowed_keys, include_l1)
    if key_filter is None:
        return None
    key_clause, keys = key_filter
    if key_clause:
        clauses.append(key_clause)
        params.extend(keys)
    for column, value in (("stage", stage), ("category", category)):
        if value:
            clauses.append(f"{column}=?")
            params.append(str(value))
    if grade:
        clauses.append(f"grade_{window}=?")
        params.append(str(grade))
    return " WHERE " + " AND ".join(clauses), params


def _snapshot_item_order(sort: str, order: str, window: int) -> str:
    columns = {
        "position": "position",
        "name": "name COLLATE NOCASE",
        "score": f"score_{window}",
        "change": f"change_{window}",
        "excess": f"excess_{window}",
        "amount": f"amount_{window}",
        "coverage": "coverage",
        "flow": f"flow_{window}",
        "daily": "daily_flow",
        "streak": "streak",
        "focus": f"focus_{window}",
    }
    if str(sort) == "focus":
        return (
            f"focus_{window} DESC, score_{window} DESC, change_{window} DESC, "
            f"excess_{window} DESC, coverage DESC, name COLLATE NOCASE ASC, item_key ASC"
        )
    selected = columns.get(str(sort), "position")
    direction = "DESC" if str(order).lower() == "desc" else "ASC"
    return (
        f"CASE WHEN {selected} IS NULL THEN 1 ELSE 0 END ASC, "
        f"{selected} {direction}, item_key ASC"
    )


def _empty_snapshot_page(page_size: int) -> dict[str, Any]:
    return {"page": 1, "page_size": page_size, "total": 0, "pages": 1}


def _snapshot_page_meta(page: int, page_size: int, total: int, pages: int) -> dict[str, Any]:
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "pages": pages,
        "has_previous": page > 1,
        "has_next": page < pages,
    }


class RotationStore:
    """Keep rebuildable analytics separate from user-selected L2 preferences."""

    def __init__(self, root: str | Path | None = None, *, read_only: bool = False):
        base = Path(root) if root is not None else get_config().data_root / "rotation"
        self.root = base.resolve()
        self.read_only = bool(read_only)
        self.cache_path = self.root / "cache.sqlite"
        self.preferences_path = self.root / "preferences.sqlite"
        self.etf_path = self.root / "etf_observations.parquet"
        self.etf_metadata_path = self.root / "etf_metadata.parquet"
        self.etf_metadata_history_path = self.root / "etf_metadata_history.parquet"
        self.etf_metadata_history_manifest_path = (
            self.root / "etf_metadata_history.manifest.json"
        )
        self.derived = DerivedArtifactCatalog(
            self.root.parent / "derived", read_only=self.read_only,
        )
        # The runtime worker owns schema migration and preference seeding.  A
        # page reader observes only a published cache and treats an absent
        # ledger as cold rather than creating it under the HTTP request.
        if not self.read_only:
            self.root.mkdir(parents=True, exist_ok=True)
            self._initialize()

    def _cache(self) -> sqlite3.Connection:
        return connect_sqlite(
            self.cache_path,
            policy="cache",
            row_factory=True,
            timeout=0.25 if self.read_only else 30.0,
            read_only=self.read_only,
        )

    def _preferences(self) -> sqlite3.Connection:
        return connect_sqlite(
            self.preferences_path,
            policy="authoritative",
            row_factory=True,
            timeout=0.25 if self.read_only else 30.0,
            read_only=self.read_only,
        )

    @staticmethod
    def _cache_v1(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE snapshots (
                kind TEXT PRIMARY KEY,
                snapshot_id TEXT NOT NULL,
                as_of TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                content_sha256 TEXT NOT NULL
            );
            CREATE TABLE taxonomy_nodes (
                code TEXT PRIMARY KEY,
                level TEXT NOT NULL,
                parent_code TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL,
                observed_at REAL NOT NULL
            );
            CREATE INDEX idx_rotation_taxonomy_level
                ON taxonomy_nodes(level, parent_code, code);
            CREATE TABLE theme_catalog (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                observed_at REAL NOT NULL
            );
            CREATE INDEX idx_rotation_theme_name ON theme_catalog(name, code);
            CREATE TABLE runtime_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            );
            """
        )

    @staticmethod
    def _preferences_v1(connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TABLE preferences ("
            "id INTEGER PRIMARY KEY CHECK(id=1),payload_json TEXT NOT NULL,updated_at REAL NOT NULL)"
        )
        connection.execute(
            "INSERT INTO preferences(id,payload_json,updated_at) VALUES(1,?,?)",
            (strict_json_dumps({"l2_codes": []}), time.time()),
        )

    @staticmethod
    def _cache_v2(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE theme_sync_runs (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                directory_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                total_count INTEGER NOT NULL,
                completed_count INTEGER NOT NULL DEFAULT 0,
                issues_json TEXT NOT NULL DEFAULT '[]',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(source,directory_hash)
            );
            CREATE TABLE theme_sync_items (
                run_id TEXT NOT NULL REFERENCES theme_sync_runs(id) ON DELETE CASCADE,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                pages INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL,
                PRIMARY KEY(run_id,code)
            );
            CREATE INDEX idx_theme_sync_items_status
                ON theme_sync_items(run_id,status,code);
            """
        )

    @staticmethod
    def _cache_v3(connection: sqlite3.Connection) -> None:
        """Split large list snapshots into compact headers and indexed rows."""

        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(snapshots)").fetchall()
        }
        if "items_indexed" not in columns:
            connection.execute(
                "ALTER TABLE snapshots ADD COLUMN items_indexed INTEGER NOT NULL DEFAULT 0"
            )
        if "details_indexed" not in columns:
            connection.execute(
                "ALTER TABLE snapshots ADD COLUMN details_indexed INTEGER NOT NULL DEFAULT 0"
            )
        connection.executescript(
            """
            CREATE TABLE snapshot_items (
                kind TEXT NOT NULL,
                snapshot_id TEXT NOT NULL,
                item_key TEXT NOT NULL,
                position INTEGER NOT NULL,
                name TEXT NOT NULL DEFAULT '',
                level TEXT NOT NULL DEFAULT '',
                stage TEXT NOT NULL DEFAULT '',
                grade TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '',
                benchmark TEXT NOT NULL DEFAULT '',
                primary_industry_name TEXT NOT NULL DEFAULT '',
                score_1 REAL, score_3 REAL, score_5 REAL, score_20 REAL,
                change_1 REAL, change_3 REAL, change_5 REAL, change_20 REAL,
                excess_1 REAL, excess_3 REAL, excess_5 REAL, excess_20 REAL,
                amount_1 REAL, amount_3 REAL, amount_5 REAL, amount_20 REAL,
                advance_1 REAL, advance_3 REAL, advance_5 REAL, advance_20 REAL,
                focus_1 INTEGER NOT NULL DEFAULT 0,
                focus_3 INTEGER NOT NULL DEFAULT 0,
                focus_5 INTEGER NOT NULL DEFAULT 0,
                focus_20 INTEGER NOT NULL DEFAULT 0,
                coverage REAL,
                flow REAL,
                streak REAL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY(kind, snapshot_id, item_key)
            );
            CREATE INDEX idx_rotation_snapshot_items_lookup
                ON snapshot_items(kind, snapshot_id, level, stage, grade, category, item_key);
            CREATE INDEX idx_rotation_snapshot_items_theme_sort
                ON snapshot_items(kind, snapshot_id, change_5 DESC, score_5 DESC, item_key);
            CREATE TABLE snapshot_details (
                kind TEXT NOT NULL,
                snapshot_id TEXT NOT NULL,
                item_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY(kind, snapshot_id, item_key)
            );
            """
        )

    @staticmethod
    def _cache_v4(connection: sqlite3.Connection) -> None:
        """Store every ETF flow window instead of reusing the five-day value."""

        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(snapshot_items)").fetchall()
        }
        for name in ("flow_1", "flow_3", "flow_5", "flow_20", "daily_flow"):
            if name not in columns:
                connection.execute(f"ALTER TABLE snapshot_items ADD COLUMN {name} REAL")

    @staticmethod
    def _cache_v5(connection: sqlite3.Connection) -> None:
        """Persist window-specific grades for SQL-side grade filtering."""

        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(snapshot_items)").fetchall()
        }
        for name in ("grade_1", "grade_3", "grade_5", "grade_20"):
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE snapshot_items ADD COLUMN {name} TEXT NOT NULL DEFAULT ''"
                )

    @staticmethod
    def _cache_v6(connection: sqlite3.Connection) -> None:
        """Attach explicit identity to legacy current snapshots without inventing dates."""

        taxonomy_rows = connection.execute(
            "SELECT code,payload_json FROM taxonomy_nodes"
        ).fetchall()
        for row in taxonomy_rows:
            try:
                payload = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict) or payload.get("taxonomy_id"):
                continue
            # Legacy rows came from the dedicated index_classify/member path,
            # but lack retained membership intervals.  Preserve that limitation.
            if str(payload.get("source") or "").upper() != "SW2021":
                continue
            payload.update({
                "taxonomy_id": "sws:industry:2021",
                "authority": "申万",
                "version": "2021",
                "code_type": "sw_index_code",
                "membership_semantics": "current_snapshot",
            })
            connection.execute(
                "UPDATE taxonomy_nodes SET payload_json=? WHERE code=?",
                (strict_json_dumps(payload), str(row["code"])),
            )

        identities = {
            "eastmoney-concept": "eastmoney:concept:live",
            "tushare:dc-concept": "eastmoney:concept:live",
            "ths:concept": "ths:concept:live",
            "free-stockdb:concept": "stockdb:concept:declared",
        }
        theme_rows = connection.execute(
            "SELECT code,payload_json FROM theme_catalog"
        ).fetchall()
        for row in theme_rows:
            try:
                payload = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict) or payload.get("taxonomy_id"):
                continue
            taxonomy_id = identities.get(str(payload.get("source") or ""))
            if not taxonomy_id:
                continue
            payload.update({
                "taxonomy_id": taxonomy_id,
                "membership_semantics": "current_snapshot",
            })
            connection.execute(
                "UPDATE theme_catalog SET payload_json=? WHERE code=?",
                (strict_json_dumps(payload), str(row["code"])),
            )

    def _initialize(self) -> None:
        with self._cache() as connection:
            migrate_schema(connection, (
                (1, self._cache_v1), (2, self._cache_v2), (3, self._cache_v3),
                (4, self._cache_v4), (5, self._cache_v5), (6, self._cache_v6),
            ))
        with self._preferences() as connection:
            migrate_schema(connection, ((1, self._preferences_v1),))

    @staticmethod
    def _indexed_snapshot_items(
        kind: str,
        snapshot_id: str,
        items: Any,
    ) -> list[tuple[Any, ...]]:
        if not isinstance(items, list):
            return []
        return [
            _snapshot_item_row(str(kind), snapshot_id, position, raw_item)
            for position, raw_item in enumerate(items)
            if isinstance(raw_item, dict)
        ]

    def _prepare_snapshot(
        self,
        kind: str,
        raw_payload: dict[str, Any],
    ) -> tuple[tuple[Any, ...], list[tuple[Any, ...]], list[tuple[Any, ...]], tuple[str, ...]]:
        payload = dict(raw_payload)
        meta = dict(payload.get("meta") or {})
        data = dict(payload.get("data") or {})
        snapshot_id = str(meta.get("snapshot_id") or "")
        items = data.pop("items", None)
        details = data.pop("details", None)
        compact = {"meta": meta, "data": data}
        text = strict_json_dumps(compact)
        header = (
            str(kind), snapshot_id, str(meta.get("as_of") or ""),
            str(meta.get("generated_at") or ""), text, _hash_text(text),
            int(isinstance(items, list)), int(isinstance(details, dict)),
        )
        indexed_items = self._indexed_snapshot_items(str(kind), snapshot_id, items)
        detail_rows = _snapshot_detail_rows(str(kind), snapshot_id, details)
        artifact = self.derived.put_json(compact, schema_version="2")
        artifact_row = (
            str(kind),
            str(artifact["artifact_id"]),
            str(meta.get("input_fingerprint") or ""),
            str(meta.get("algorithm_version") or ""),
        )
        return (
            header,
            indexed_items,
            detail_rows,
            artifact_row,
        )

    def _write_snapshot_batch(self, batch: _SnapshotWriteBatch) -> None:
        with self._cache() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for kind, *_rest in batch.headers:
                connection.execute("DELETE FROM snapshot_items WHERE kind=?", (kind,))
                connection.execute("DELETE FROM snapshot_details WHERE kind=?", (kind,))
            connection.executemany(
                "INSERT INTO snapshots(kind,snapshot_id,as_of,generated_at,payload_json,"
                "content_sha256,items_indexed,details_indexed) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(kind) DO UPDATE SET "
                "snapshot_id=excluded.snapshot_id,as_of=excluded.as_of,"
                "generated_at=excluded.generated_at,payload_json=excluded.payload_json,"
                "content_sha256=excluded.content_sha256,items_indexed=excluded.items_indexed,"
                "details_indexed=excluded.details_indexed",
                batch.headers,
            )
            if batch.items:
                connection.executemany(
                    "INSERT INTO snapshot_items(" + _SNAPSHOT_ITEM_COLUMNS + ") VALUES("
                    + ",".join("?" for _ in range(48)) + ")",
                    batch.items,
                )
            if batch.details:
                connection.executemany(
                    "INSERT INTO snapshot_details(kind,snapshot_id,item_key,payload_json) "
                    "VALUES(?,?,?,?)",
                    batch.details,
                )

    def _publish_snapshot_batch(self, artifacts: list[tuple[str, str, str, str]]) -> None:
        self.derived.publish_snapshots(
            "rotation", {kind: artifact_id for kind, artifact_id, _fp, _algo in artifacts},
        )
        for kind, artifact_id, input_fingerprint, algorithm_version in artifacts:
            if input_fingerprint and algorithm_version:
                self.derived.record_node(
                    f"rotation.{kind}",
                    "current",
                    input_fingerprint,
                    algorithm_version,
                    output_artifact_id=artifact_id,
                )

    def save_snapshots(self, payloads: dict[str, dict[str, Any]]) -> None:
        """Commit a coherent set of views with list rows outside the JSON header.

        Internal aggregate readers can still call :meth:`snapshot`, while list
        APIs call :meth:`snapshot_items_page` and deserialize only the requested
        rows.  The header and all list/detail rows are replaced atomically.
        """

        batch = _SnapshotWriteBatch([], [], [], [])
        for kind, raw_payload in payloads.items():
            header, items, details, artifact = self._prepare_snapshot(kind, raw_payload)
            batch.headers.append(header)
            batch.items.extend(items)
            batch.details.extend(details)
            batch.artifacts.append(artifact)
        self._write_snapshot_batch(batch)
        # The immutable objects are fsync'ed before they are registered.  Only
        # after the indexed cache commits do we advance all affected current
        # pointers in one catalog transaction.
        self._publish_snapshot_batch(batch.artifacts)

    def snapshot(self, kind: str) -> dict[str, Any] | None:
        try:
            with self._cache() as connection:
                row = connection.execute(
                    "SELECT snapshot_id,payload_json,content_sha256,items_indexed,details_indexed "
                    "FROM snapshots WHERE kind=?", (kind,),
                ).fetchone()
        except (FileNotFoundError, sqlite3.OperationalError):
            return None
        if row is None:
            return None
        text = str(row["payload_json"])
        if _hash_text(text) != str(row["content_sha256"]):
            raise RotationIntegrityError(f"{kind} 快照内容哈希不匹配")
        try:
            value = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RotationIntegrityError(f"{kind} 快照不是有效 JSON") from exc
        if not isinstance(value, dict):
            raise RotationIntegrityError(f"{kind} 快照根节点不是对象")
        if bool(row["items_indexed"]):
            try:
                with self._cache() as connection:
                    items = connection.execute(
                        "SELECT payload_json FROM snapshot_items WHERE kind=? AND snapshot_id=? "
                        "ORDER BY position,item_key",
                        (kind, str(row["snapshot_id"])),
                    ).fetchall()
            except (FileNotFoundError, sqlite3.OperationalError):
                items = []
            value.setdefault("data", {})["items"] = [
                json.loads(str(item["payload_json"])) for item in items
            ]
        if bool(row["details_indexed"]):
            try:
                with self._cache() as connection:
                    details = connection.execute(
                        "SELECT item_key,payload_json FROM snapshot_details WHERE kind=? AND snapshot_id=?",
                        (kind, str(row["snapshot_id"])),
                    ).fetchall()
            except (FileNotFoundError, sqlite3.OperationalError):
                details = []
            value.setdefault("data", {})["details"] = {
                str(item["item_key"]): json.loads(str(item["payload_json"]))
                for item in details
            }
        return value

    def snapshot_header(self, kind: str) -> dict[str, Any] | None:
        """Read only a current compact header, never its list/detail rows."""

        try:
            with self._cache() as connection:
                row = connection.execute(
                    "SELECT payload_json,content_sha256 FROM snapshots WHERE kind=?", (kind,),
                ).fetchone()
        except (FileNotFoundError, sqlite3.OperationalError):
            return None
        if row is None:
            return None
        text = str(row["payload_json"])
        if _hash_text(text) != str(row["content_sha256"]):
            raise RotationIntegrityError(f"{kind} 快照内容哈希不匹配")
        try:
            value = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RotationIntegrityError(f"{kind} 快照不是有效 JSON") from exc
        if not isinstance(value, dict):
            raise RotationIntegrityError(f"{kind} 快照根节点不是对象")
        return value

    def snapshot_detail(self, kind: str, item_key: str) -> dict[str, Any] | None:
        header = self.snapshot_header(kind)
        if header is None:
            return None
        snapshot_id = str((header.get("meta") or {}).get("snapshot_id") or "")
        try:
            with self._cache() as connection:
                row = connection.execute(
                    "SELECT payload_json FROM snapshot_details WHERE kind=? AND snapshot_id=? AND item_key=?",
                    (str(kind), snapshot_id, str(item_key).upper()),
                ).fetchone()
        except (FileNotFoundError, sqlite3.OperationalError):
            return None
        if row is None:
            return None
        try:
            return json.loads(str(row["payload_json"]))
        except json.JSONDecodeError as exc:
            raise RotationIntegrityError(f"{kind} 明细快照不是有效 JSON") from exc

    def _read_snapshot_items_page(
        self,
        where: str,
        params: list[Any],
        order_by: str,
        page: int,
        page_size: int,
    ) -> tuple[list[sqlite3.Row], dict[str, Any]]:
        with self._cache() as connection:
            total = int(connection.execute(
                "SELECT COUNT(*) FROM snapshot_items" + where, tuple(params),
            ).fetchone()[0])
            pages = max(1, (total + page_size - 1) // page_size)
            current = min(page, pages)
            offset = (current - 1) * page_size
            rows = connection.execute(
                "SELECT payload_json FROM snapshot_items"
                + where
                + " ORDER BY "
                + order_by
                + " LIMIT ? OFFSET ?",
                (*params, page_size, offset),
            ).fetchall()
        return rows, _snapshot_page_meta(current, page_size, total, pages)

    @staticmethod
    def _decode_snapshot_items(kind: str, rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
        try:
            return [json.loads(str(row["payload_json"])) for row in rows]
        except json.JSONDecodeError as exc:
            raise RotationIntegrityError(f"{kind} 列表索引不是有效 JSON") from exc

    def snapshot_items_page(
        self,
        kind: str,
        *,
        query: str = "",
        level: str = "",
        allowed_keys: set[str] | None = None,
        include_l1: bool = False,
        stage: str = "",
        grade: str = "",
        category: str = "",
        sort: str = "position",
        order: str = "asc",
        window: int = 5,
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]], dict[str, Any]]:
        """Filter/sort/page in SQLite, parsing only the selected response rows."""

        selected_window = int(window)
        if selected_window not in {1, 3, 5, 20}:
            raise ValueError("轮动观察窗口仅支持 1、3、5、20 日")
        selected_size = max(1, min(500, int(page_size)))
        header = self.snapshot_header(kind)
        if header is None:
            return None, [], _empty_snapshot_page(page_size)
        snapshot_id = str((header.get("meta") or {}).get("snapshot_id") or "")
        filters = _snapshot_item_filters(
            kind,
            snapshot_id,
            query=query,
            level=level,
            allowed_keys=allowed_keys,
            include_l1=include_l1,
            stage=stage,
            category=category,
            grade=grade,
            window=selected_window,
        )
        if filters is None:
            return header, [], _empty_snapshot_page(page_size)
        where, params = filters
        selected_page = max(1, int(page))
        try:
            rows, pagination = self._read_snapshot_items_page(
                where,
                params,
                _snapshot_item_order(sort, order, selected_window),
                selected_page,
                selected_size,
            )
        except (FileNotFoundError, sqlite3.OperationalError):
            pagination = _snapshot_page_meta(1, selected_size, 0, 1)
            return header, [], pagination
        return header, self._decode_snapshot_items(kind, rows), pagination

    def snapshot_item_categories(self, kind: str) -> list[str]:
        header = self.snapshot_header(kind)
        if header is None:
            return []
        snapshot_id = str((header.get("meta") or {}).get("snapshot_id") or "")
        try:
            with self._cache() as connection:
                rows = connection.execute(
                    "SELECT DISTINCT category FROM snapshot_items WHERE kind=? AND snapshot_id=? "
                    "AND category<>'' ORDER BY category COLLATE NOCASE",
                    (str(kind), snapshot_id),
                ).fetchall()
        except (FileNotFoundError, sqlite3.OperationalError):
            return []
        return [str(row["category"]) for row in rows]

    def snapshots(self) -> list[dict[str, Any]]:
        try:
            with self._cache() as connection:
                rows = connection.execute(
                    "SELECT kind,snapshot_id,as_of,generated_at FROM snapshots ORDER BY kind"
                ).fetchall()
        except (FileNotFoundError, sqlite3.OperationalError):
            return []
        return [dict(row) for row in rows]

    def preferences(self) -> dict[str, Any]:
        try:
            with self._preferences() as connection:
                row = connection.execute(
                    "SELECT payload_json,updated_at FROM preferences WHERE id=1"
                ).fetchone()
        except (FileNotFoundError, sqlite3.OperationalError):
            row = None
        value = json.loads(str(row["payload_json"])) if row else {}
        return {
            "l2_codes": [str(code) for code in value.get("l2_codes") or []],
            "updated_at": float(row["updated_at"]) if row else 0.0,
        }

    def save_preferences(self, value: dict[str, Any]) -> dict[str, Any]:
        l2_codes = list(dict.fromkeys(
            str(code).strip().upper() for code in value.get("l2_codes") or [] if str(code).strip()
        ))
        if len(l2_codes) > 30:
            raise ValueError("最多关注 30 个申万二级行业")
        payload = {"l2_codes": l2_codes}
        now = time.time()
        with self._preferences() as connection:
            connection.execute(
                "UPDATE preferences SET payload_json=?,updated_at=? WHERE id=1",
                (strict_json_dumps(payload), now),
            )
        return {**payload, "updated_at": now}

    def replace_taxonomy_nodes(self, nodes: list[dict[str, Any]]) -> None:
        rows = []
        for node in nodes:
            code = str(node.get("code") or "").strip().upper()
            level = str(node.get("level") or "").strip().upper()
            if not code or level not in {"L1", "L2"}:
                continue
            rows.append((
                code, level, str(node.get("parent_code") or "").strip().upper(),
                strict_json_dumps(node), time.time(),
            ))
        with self._cache() as connection:
            connection.execute("DELETE FROM taxonomy_nodes")
            connection.executemany(
                "INSERT INTO taxonomy_nodes(code,level,parent_code,payload_json,observed_at) "
                "VALUES(?,?,?,?,?)",
                rows,
            )
        identity = _hash_text(strict_json_dumps([
            json.loads(value[3]) for value in sorted(rows, key=lambda value: value[0])
        ], sort_keys=True))
        self.derived.advance_source_generation("rotation.taxonomy", "all", identity)

    def taxonomy_nodes(self, level: str | None = None) -> list[dict[str, Any]]:
        try:
            with self._cache() as connection:
                if level:
                    rows = connection.execute(
                        "SELECT payload_json FROM taxonomy_nodes WHERE level=? ORDER BY code",
                        (str(level).upper(),),
                    ).fetchall()
                else:
                    rows = connection.execute(
                        "SELECT payload_json FROM taxonomy_nodes ORDER BY level,code"
                    ).fetchall()
        except (FileNotFoundError, sqlite3.OperationalError):
            return []
        result = []
        for row in rows:
            try:
                item = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                result.append(item)
        return result

    def taxonomy_evidence(self, level: str | None = None) -> list[dict[str, Any]]:
        """Return taxonomy payloads with their persisted knowledge timestamp."""
        with self._cache() as connection:
            if level:
                rows = connection.execute(
                    "SELECT payload_json,observed_at FROM taxonomy_nodes "
                    "WHERE level=? ORDER BY code",
                    (str(level).upper(),),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT payload_json,observed_at FROM taxonomy_nodes ORDER BY level,code"
                ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            try:
                item = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                result.append({**item, "observed_at_epoch": float(row["observed_at"])})
        return result

    def replace_themes(self, themes: list[dict[str, Any]]) -> None:
        rows = []
        observed_at = time.time()
        for theme in themes:
            code = str(theme.get("code") or "").strip().upper()
            name = str(theme.get("name") or "").strip()
            if code and name:
                rows.append((code, name, strict_json_dumps(theme), observed_at))
        with self._cache() as connection:
            connection.execute("DELETE FROM theme_catalog")
            connection.executemany(
                "INSERT INTO theme_catalog(code,name,payload_json,observed_at) VALUES(?,?,?,?)",
                rows,
            )
        identity = _hash_text(strict_json_dumps([
            json.loads(value[2]) for value in sorted(rows, key=lambda value: value[0])
        ], sort_keys=True))
        coverage = sorted({
            str(theme.get(key) or "")[:10]
            for theme in themes for key in ("as_of", "observed_at", "effective_date")
            if str(theme.get(key) or "")[:10]
        })
        self.derived.advance_source_generation(
            "rotation.themes", "all", identity,
            coverage_start=coverage[0] if coverage else "",
            coverage_end=coverage[-1] if coverage else "",
        )

    def themes(self) -> list[dict[str, Any]]:
        with self._cache() as connection:
            rows = connection.execute(
                "SELECT payload_json FROM theme_catalog ORDER BY name,code"
            ).fetchall()
        result = []
        for row in rows:
            try:
                item = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                result.append(item)
        return result

    def theme_evidence(self) -> list[dict[str, Any]]:
        """Return concept payloads with their persisted knowledge timestamp."""
        with self._cache() as connection:
            rows = connection.execute(
                "SELECT payload_json,observed_at FROM theme_catalog ORDER BY name,code"
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            try:
                item = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                result.append({**item, "observed_at_epoch": float(row["observed_at"])})
        return result

    def begin_theme_sync(
        self, source: str, directory_hash: str, total_count: int,
    ) -> dict[str, Any]:
        """Create or resume one source-coherent theme catalog staging run."""
        now = time.time()
        with self._cache() as connection:
            row = connection.execute(
                "SELECT id,status FROM theme_sync_runs WHERE source=? AND directory_hash=?",
                (source, directory_hash),
            ).fetchone()
            if row is None:
                run_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO theme_sync_runs(id,source,directory_hash,status,total_count,"
                    "created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                    (run_id, source, directory_hash, "running", int(total_count), now, now),
                )
            else:
                run_id = str(row["id"])
                connection.execute(
                    "UPDATE theme_sync_runs SET status='running',total_count=?,updated_at=? "
                    "WHERE id=?",
                    (int(total_count), now, run_id),
                )
            rows = connection.execute(
                "SELECT code,payload_json,pages FROM theme_sync_items "
                "WHERE run_id=? AND status='complete'",
                (run_id,),
            ).fetchall()
            attempted_count = int(connection.execute(
                "SELECT COUNT(*) FROM theme_sync_items WHERE run_id=?",
                (run_id,),
            ).fetchone()[0])
        items: dict[str, dict[str, Any]] = {}
        for item in rows:
            try:
                payload = json.loads(str(item["payload_json"]))
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                items[str(item["code"])] = payload
        return {
            "run_id": run_id,
            "items": items,
            "attempted_count": attempted_count,
        }

    def has_pending_theme_sync(self, sources: tuple[str, ...]) -> bool:
        """Report unfinished partitions without touching provider health state."""
        if not sources:
            return False
        placeholders = ",".join("?" for _ in sources)
        with self._cache() as connection:
            row = connection.execute(
                f"SELECT 1 FROM theme_sync_runs r WHERE r.source IN ({placeholders}) "
                "AND (r.status IN ('running','incomplete') OR EXISTS ("
                "SELECT 1 FROM theme_sync_items i WHERE i.run_id=r.id "
                "AND i.status!='complete')) LIMIT 1",
                tuple(sources),
            ).fetchone()
        return row is not None

    def save_theme_sync_item(
        self,
        run_id: str,
        code: str,
        name: str,
        *,
        payload: dict[str, Any] | None = None,
        error: str = "",
        pages: int = 0,
    ) -> None:
        now = time.time()
        status = "complete" if payload else "failed"
        with self._cache() as connection:
            connection.execute(
                "INSERT INTO theme_sync_items(run_id,code,name,status,payload_json,error,pages,"
                "updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(run_id,code) DO UPDATE SET "
                "name=excluded.name,status=excluded.status,payload_json=excluded.payload_json,"
                "error=excluded.error,pages=excluded.pages,updated_at=excluded.updated_at",
                (
                    run_id,
                    str(code),
                    str(name),
                    status,
                    strict_json_dumps(payload) if payload else "",
                    str(error)[:500],
                    max(0, int(pages)),
                    now,
                ),
            )
            completed = connection.execute(
                "SELECT COUNT(*) FROM theme_sync_items WHERE run_id=? AND status='complete'",
                (run_id,),
            ).fetchone()[0]
            connection.execute(
                "UPDATE theme_sync_runs SET completed_count=?,updated_at=? WHERE id=?",
                (int(completed), now, run_id),
            )

    def commit_theme_sync(
        self,
        run_id: str,
        themes: list[dict[str, Any]],
        issues: list[str],
    ) -> None:
        """Atomically publish a validated staging run and its audit outcome."""
        observed_at = time.time()
        rows = [
            (
                str(theme.get("code") or "").strip().upper(),
                str(theme.get("name") or "").strip(),
                strict_json_dumps(theme),
                observed_at,
            )
            for theme in themes
            if str(theme.get("code") or "").strip()
            and str(theme.get("name") or "").strip()
            and theme.get("members")
        ]
        if not rows:
            raise ValueError("题材暂存目录没有可提交的有效成分")
        with self._cache() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DELETE FROM theme_catalog")
            connection.executemany(
                "INSERT INTO theme_catalog(code,name,payload_json,observed_at) VALUES(?,?,?,?)",
                rows,
            )
            connection.execute(
                "UPDATE theme_sync_runs SET status='completed',completed_count=?,issues_json=?,"
                "updated_at=? WHERE id=?",
                (len(rows), strict_json_dumps(issues), observed_at, run_id),
            )
            connection.commit()
        identity = _hash_text(strict_json_dumps([
            json.loads(value[2]) for value in sorted(rows, key=lambda value: value[0])
        ], sort_keys=True))
        coverage = sorted({
            str(theme.get(key) or "")[:10]
            for theme in themes for key in ("as_of", "observed_at", "effective_date")
            if str(theme.get(key) or "")[:10]
        })
        self.derived.advance_source_generation(
            "rotation.themes", "all", identity,
            coverage_start=coverage[0] if coverage else "",
            coverage_end=coverage[-1] if coverage else "",
        )

    def reuse_published_theme_sync(self, run_id: str, issues: list[str]) -> None:
        """Close a resumed staging run without rewriting its published catalog.

        A fully traversed partial catalog can be reused when the upstream directory
        is unchanged.  Rewriting ``theme_catalog`` would incorrectly make the old
        observations look freshly downloaded, so only the run audit is updated.
        """
        with self._cache() as connection:
            completed = int(connection.execute(
                "SELECT COUNT(*) FROM theme_sync_items WHERE run_id=? AND status='complete'",
                (run_id,),
            ).fetchone()[0])
            connection.execute(
                "UPDATE theme_sync_runs SET status='completed',completed_count=?,"
                "issues_json=?,updated_at=? WHERE id=?",
                (completed, strict_json_dumps(issues), time.time(), run_id),
            )

    def fail_theme_sync(self, run_id: str, issues: list[str]) -> None:
        with self._cache() as connection:
            connection.execute(
                "UPDATE theme_sync_runs SET status='incomplete',issues_json=?,updated_at=? "
                "WHERE id=?",
                (strict_json_dumps(issues), time.time(), run_id),
            )

    def runtime_state(self, key: str) -> str:
        with self._cache() as connection:
            row = connection.execute(
                "SELECT value FROM runtime_state WHERE key=?", (str(key),),
            ).fetchone()
        return str(row["value"]) if row else ""

    def set_runtime_state(self, key: str, value: str) -> None:
        with self._cache() as connection:
            connection.execute(
                "INSERT INTO runtime_state(key,value,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value,"
                "updated_at=excluded.updated_at",
                (str(key), str(value), time.time()),
            )

    def save_etf_observations(self, frame: pd.DataFrame) -> None:
        if frame is None or frame.empty:
            return
        fd, temp_name = tempfile.mkstemp(
            prefix=".etf_observations.", suffix=".parquet.tmp", dir=self.root,
        )
        os.close(fd)
        temp = Path(temp_name)
        try:
            frame.to_parquet(temp, index=False)
            with temp.open("rb+") as stream:
                os.fsync(stream.fileno())
            os.replace(temp, self.etf_path)
            coverage_column = next((
                value for value in ("trade_date", "date", "as_of") if value in frame.columns
            ), "")
            coverage = frame[coverage_column].astype(str) if coverage_column else pd.Series(dtype=str)
            self.derived.advance_source_generation(
                "rotation.etf_observations",
                "all",
                _hash_file(self.etf_path),
                coverage_start=str(coverage.min() if not coverage.empty else ""),
                coverage_end=str(coverage.max() if not coverage.empty else ""),
            )
        finally:
            temp.unlink(missing_ok=True)

    def source_generations(self, source: str = "") -> list[dict[str, Any]]:
        """Expose compact generation rows for refresh fingerprint construction."""

        return self.derived.source_generations(source)

    def mark_source_coverage(self, source: str, coverage_end: str) -> None:
        """Record a successful freshness probe without manufacturing a generation.

        Providers occasionally confirm that an unchanged taxonomy or ETF
        directory is still current.  That must suppress another remote call on
        the next refresh, but it must *not* invalidate every dependent
        snapshot: the content identity and generation remain unchanged.
        """

        target = str(coverage_end or "")[:10]
        if not target:
            return
        for row in self.derived.source_generations(str(source)):
            previous_end = str(row.get("coverage_end") or "")[:10]
            self.derived.advance_source_generation(
                str(row["source"]),
                str(row["partition_key"]),
                str(row["content_id"]),
                coverage_start=str(row.get("coverage_start") or "")[:10],
                coverage_end=max(previous_end, target),
            )

    def etf_observations(self) -> pd.DataFrame:
        if not self.etf_path.is_file():
            return pd.DataFrame()
        try:
            return pd.read_parquet(self.etf_path)
        except (OSError, ValueError) as exc:
            logger.error("ETF 观察文件完整性校验失败: %s", self.etf_path, exc_info=True)
            raise RotationIntegrityError("ETF 观察文件损坏，拒绝按空数据继续计算") from exc

    def save_etf_metadata(self, frame: pd.DataFrame) -> None:
        """Persist immutable observations and a tamper-evident history manifest."""

        if frame is None or frame.empty:
            return
        with _ETF_METADATA_LOCK, _etf_metadata_file_lock(
            self.root / ".etf_metadata_history.lock"
        ):
            current = self._prepare_etf_metadata_observations(frame)
            history = self._read_etf_metadata_history_locked()
            combined = pd.concat((history, current), ignore_index=True, sort=False)
            conflicts = (
                combined.groupby("observation_id")["observation_content_sha256"].nunique()
                if not combined.empty
                else pd.Series(dtype=int)
            )
            conflicting_ids = sorted(conflicts[conflicts.gt(1)].index.astype(str))
            if conflicting_ids:
                raise RotationIntegrityError(
                    "ETF 元数据观察身份出现冲突内容，拒绝改写历史: "
                    + ", ".join(conflicting_ids[:5])
                )
            combined = (
                combined.sort_values(["observed_at", "symbol", "observation_id"])
                .drop_duplicates("observation_id", keep="first")
                .reset_index(drop=True)
            )
            previous_ids = (
                set(history.get("observation_id", pd.Series(dtype=str)).astype(str))
                if not history.empty
                else set()
            )
            if set(combined["observation_id"].astype(str)) != previous_ids:
                self._write_etf_metadata_history(combined)
            current_changed = True
            if self.etf_metadata_path.is_file():
                try:
                    existing = self._prepare_etf_metadata_observations(
                        pd.read_parquet(self.etf_metadata_path)
                    )
                    existing_ids = existing[[
                        "observation_id", "observation_content_sha256",
                    ]].sort_values("observation_id").reset_index(drop=True)
                    current_ids = current[[
                        "observation_id", "observation_content_sha256",
                    ]].sort_values("observation_id").reset_index(drop=True)
                    current_changed = not existing_ids.equals(current_ids)
                except (OSError, ValueError, KeyError):
                    # A corrupt current file must be replaced by the verified
                    # observation we just received; it must not become an
                    # implicit no-op.
                    current_changed = True
            if current_changed:
                self._write_etf_metadata_frame(
                    self.etf_metadata_path,
                    current,
                    ".etf_metadata.",
                )
            if self.etf_metadata_path.is_file():
                self.derived.advance_source_generation(
                    "rotation.etf_metadata",
                    "current",
                    _hash_file(self.etf_metadata_path),
                    coverage_start=str(current["observed_at"].min() or ""),
                    coverage_end=str(current["observed_at"].max() or ""),
                )

    @staticmethod
    def _prepare_etf_metadata_observations(frame: pd.DataFrame) -> pd.DataFrame:
        current = frame.copy()
        if "symbol" not in current:
            raise RotationIntegrityError("ETF 元数据观察缺少 symbol")
        current["symbol"] = current["symbol"].fillna("").astype(str).str.upper()
        if current["symbol"].eq("").any():
            raise RotationIntegrityError("ETF 元数据观察包含空 symbol")
        if "observed_at" not in current:
            current["observed_at"] = pd.Timestamp.now(tz="UTC").isoformat()
        parsed = pd.to_datetime(current["observed_at"], errors="coerce", utc=True)
        missing = parsed.isna()
        if missing.any():
            parsed.loc[missing] = pd.Timestamp.now(tz="UTC")
        current["observed_at"] = parsed.map(lambda value: value.isoformat())
        for column in _ETF_METADATA_DERIVED_COLUMNS:
            if column in current:
                current = current.drop(columns=column)
        records = current.to_dict("records")
        current["observation_id"] = [
            _metadata_observation_id(str(row["symbol"]), str(row["observed_at"]))
            for row in records
        ]
        current["observation_content_sha256"] = [
            _metadata_content_hash(row) for row in records
        ]
        current["observation_integrity"] = "verified"
        return current

    @staticmethod
    def _write_etf_metadata_frame(
        target: Path,
        value: pd.DataFrame,
        prefix: str,
    ) -> None:
        fd, temp_name = tempfile.mkstemp(
            prefix=prefix, suffix=".parquet.tmp", dir=target.parent,
        )
        os.close(fd)
        temp = Path(temp_name)
        try:
            value.to_parquet(temp, index=False)
            with temp.open("rb+") as stream:
                os.fsync(stream.fileno())
            os.replace(temp, target)
        finally:
            temp.unlink(missing_ok=True)

    @staticmethod
    def _history_logical_hash(frame: pd.DataFrame) -> str:
        rows = sorted(
            (
                {
                    "observation_id": str(row.observation_id),
                    "content_sha256": str(row.observation_content_sha256),
                }
                for row in frame[[
                    "observation_id", "observation_content_sha256"
                ]].itertuples(index=False)
            ),
            key=lambda row: row["observation_id"],
        )
        return _hash_text(strict_json_dumps(rows, sort_keys=True))

    def _write_etf_metadata_history(self, frame: pd.DataFrame) -> None:
        self._write_etf_metadata_frame(
            self.etf_metadata_history_path,
            frame,
            ".etf_metadata_history.",
        )
        manifest = {
            "schema_version": ETF_METADATA_HISTORY_SCHEMA_VERSION,
            "artifact": "etf_metadata_history",
            "file_sha256": _hash_file(self.etf_metadata_history_path),
            "logical_sha256": self._history_logical_hash(frame),
            "row_count": len(frame),
            "observation_count": frame["observation_id"].nunique(),
            "written_at": pd.Timestamp.now(tz="UTC").isoformat(),
        }
        manifest["manifest_sha256"] = _hash_text(
            strict_json_dumps(manifest, sort_keys=True)
        )
        encoded = json.dumps(
            manifest,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
        fd, temp_name = tempfile.mkstemp(
            prefix=".etf_metadata_history.manifest.",
            suffix=".json.tmp",
            dir=self.root,
        )
        os.close(fd)
        temp = Path(temp_name)
        try:
            temp.write_text(encoded, encoding="utf-8")
            with temp.open("rb+") as stream:
                os.fsync(stream.fileno())
            os.replace(temp, self.etf_metadata_history_manifest_path)
        finally:
            temp.unlink(missing_ok=True)

    def _read_etf_metadata_history_manifest(self) -> dict[str, Any]:
        if not self.etf_metadata_history_manifest_path.is_file():
            raise RotationIntegrityError(
                "ETF 元数据历史缺少完整性 manifest；旧历史不得静默升级为可信证据"
            )
        try:
            manifest = json.loads(
                self.etf_metadata_history_manifest_path.read_text(encoding="utf-8")
            )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise RotationIntegrityError("ETF 元数据历史 manifest 损坏") from exc
        if not isinstance(manifest, dict):
            raise RotationIntegrityError("ETF 元数据历史 manifest 损坏")
        claimed_manifest_hash = str(manifest.pop("manifest_sha256", ""))
        actual_manifest_hash = _hash_text(strict_json_dumps(manifest, sort_keys=True))
        if claimed_manifest_hash != actual_manifest_hash:
            raise RotationIntegrityError("ETF 元数据历史 manifest 哈希不匹配")
        if (
            manifest.get("schema_version") != ETF_METADATA_HISTORY_SCHEMA_VERSION
            or manifest.get("artifact") != "etf_metadata_history"
        ):
            raise RotationIntegrityError("ETF 元数据历史 manifest 契约已淘汰或类型错误")
        return manifest

    def _read_etf_metadata_history_file(self, manifest: dict[str, Any]) -> pd.DataFrame:
        if _hash_file(self.etf_metadata_history_path) != manifest.get("file_sha256"):
            raise RotationIntegrityError("ETF 元数据历史文件哈希与 manifest 不匹配")
        try:
            history = pd.read_parquet(self.etf_metadata_history_path)
        except (OSError, ValueError) as exc:
            raise RotationIntegrityError("ETF 元数据历史损坏，拒绝丢失 PIT 证据") from exc
        required = {
            "symbol",
            "observed_at",
            "observation_id",
            "observation_content_sha256",
            "observation_integrity",
        }
        if not required.issubset(history.columns):
            raise RotationIntegrityError("ETF 元数据历史缺少不可变观察字段")
        return history

    def _validate_etf_metadata_observations(self, history: pd.DataFrame) -> None:
        prepared = self._prepare_etf_metadata_observations(history)
        identities_match = (
            prepared["observation_id"].tolist()
            == history["observation_id"].astype(str).tolist()
        )
        content_hashes_match = (
            prepared["observation_content_sha256"].tolist()
            == history["observation_content_sha256"].astype(str).tolist()
        )
        observations_verified = history["observation_integrity"].astype(str).eq("verified").all()
        if not identities_match or not content_hashes_match or not observations_verified:
            raise RotationIntegrityError("ETF 元数据历史观察身份或内容哈希不匹配")

    def _validate_etf_metadata_manifest_rows(
        self,
        history: pd.DataFrame,
        manifest: dict[str, Any],
    ) -> None:
        if len(history) != int(manifest.get("row_count") or -1):
            raise RotationIntegrityError("ETF 元数据历史行数与 manifest 不匹配")
        if history["observation_id"].nunique() != int(
            manifest.get("observation_count") or -1
        ):
            raise RotationIntegrityError("ETF 元数据历史观察数与 manifest 不匹配")
        if self._history_logical_hash(history) != manifest.get("logical_sha256"):
            raise RotationIntegrityError("ETF 元数据历史逻辑哈希与 manifest 不匹配")

    def _read_verified_etf_metadata_history(self) -> pd.DataFrame:
        manifest = self._read_etf_metadata_history_manifest()
        history = self._read_etf_metadata_history_file(manifest)
        self._validate_etf_metadata_observations(history)
        self._validate_etf_metadata_manifest_rows(history, manifest)
        return history

    def _quarantine_legacy_etf_metadata_history(self) -> None:
        """Move an obsolete, rebuildable history aside without decoding it."""
        manifest_path = self.etf_metadata_history_manifest_path
        if not manifest_path.is_file():
            return
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return
        if manifest.get("schema_version") == ETF_METADATA_HISTORY_SCHEMA_VERSION:
            return
        stamp = str(time.time_ns())
        quarantine = self.root / "quarantine" / f"etf_metadata_history-{stamp}"
        quarantine.mkdir(parents=True, exist_ok=True)
        for path in (
            self.etf_metadata_history_path,
            self.etf_metadata_history_manifest_path,
        ):
            if path.is_file():
                path.replace(quarantine / path.name)
        logger.warning(
            "ETF 元数据历史契约已淘汰，已隔离并等待 v2 重建：%s", quarantine,
        )

    def etf_metadata(self) -> pd.DataFrame:
        if not self.etf_metadata_path.is_file():
            return pd.DataFrame()
        try:
            return pd.read_parquet(self.etf_metadata_path)
        except (OSError, ValueError) as exc:
            logger.error("ETF 元数据文件完整性校验失败: %s", self.etf_metadata_path, exc_info=True)
            raise RotationIntegrityError("ETF 元数据文件损坏，拒绝按空目录继续分类") from exc

    def etf_metadata_history(self) -> pd.DataFrame:
        # Published parquet/manifest replacements are atomic.  A reader never
        # queues behind a writer; it only retries the tiny manifest/file swap
        # window for at most 200ms, then reports an explicit integrity error.
        deadline = time.monotonic() + 0.2
        while True:
            try:
                return self._read_etf_metadata_history_locked()
            except RotationIntegrityError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)

    def _read_etf_metadata_history_locked(self) -> pd.DataFrame:
        if not self.etf_metadata_history_path.is_file():
            return pd.DataFrame()
        try:
            manifest = json.loads(
                self.etf_metadata_history_manifest_path.read_text(encoding="utf-8")
            )
        except FileNotFoundError:
            manifest = None
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            manifest = None
        if (
            isinstance(manifest, dict)
            and manifest.get("schema_version") != ETF_METADATA_HISTORY_SCHEMA_VERSION
        ):
            self._quarantine_legacy_etf_metadata_history()
            return pd.DataFrame()
        return self._read_verified_etf_metadata_history()
