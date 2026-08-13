"""Shared cache result, identity, negative-cache and observability contracts.

This module deliberately keeps cache identity readable.  A cache key is canonical JSON
containing the business dimensions which can change a result; it is never replaced by a
validation hash or an opaque tag.  Payload stores remain owned by their data domains.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar, Literal

from quantmaster.runtime.sqlite import connect_sqlite


class CacheResultKind(StrEnum):
    SUCCESS = "success"
    PARTIAL = "partial"
    EMPTY_VALID = "empty_valid"
    NOT_FOUND = "not_found"
    RATE_LIMITED = "rate_limited"
    TEMPORARY_FAILURE = "temporary_failure"
    INVALID_RESPONSE = "invalid_response"
    PERMISSION_DENIED = "permission_denied"


POSITIVE_CACHE_RESULTS = frozenset({CacheResultKind.SUCCESS, CacheResultKind.EMPTY_VALID})
FAILURE_RESULTS = frozenset({
    CacheResultKind.RATE_LIMITED,
    CacheResultKind.TEMPORARY_FAILURE,
    CacheResultKind.INVALID_RESPONSE,
    CacheResultKind.PERMISSION_DENIED,
})


class CacheContractError(ValueError):
    """A caller attempted to blur an explicit cache contract."""


@dataclass(frozen=True)
class CacheResult:
    kind: CacheResultKind
    diagnostic_code: str = ""
    detail: str = ""
    completed_items: int = 0
    expected_items: int | None = None

    @property
    def cacheable_as_batch(self) -> bool:
        return self.kind in POSITIVE_CACHE_RESULTS

    @property
    def requires_pending(self) -> bool:
        return self.kind == CacheResultKind.PARTIAL

    def require_cacheable_as_batch(self) -> None:
        if not self.cacheable_as_batch:
            raise CacheContractError(
                f"{self.kind.value} 不能作为正常批次结果写入缓存"
            )


@dataclass(frozen=True)
class CacheKey:
    """Readable identity for every dimension which can affect a cached result."""

    namespace: str
    provider: str
    resource: str
    symbol: str = ""
    market: str = ""
    timeframe: str = ""
    range_start: str = ""
    range_end: str = ""
    as_of: str = ""
    taxonomy: str = ""
    adjustment: str = ""
    currency: str = ""
    unit: str = ""
    filters: tuple[tuple[str, str], ...] = ()
    page: str = ""
    page_size: int | None = None
    complete_range: bool | None = None
    config_revision: str = ""
    parser_revision: str = ""

    def __post_init__(self) -> None:
        if not self.namespace.strip() or not self.provider.strip() or not self.resource.strip():
            raise CacheContractError("cache key 必须包含 namespace、provider 和 resource")
        normalized = tuple(sorted((str(key), str(value)) for key, value in self.filters))
        if normalized != self.filters:
            object.__setattr__(self, "filters", normalized)

    def as_dict(self) -> dict[str, Any]:
        return {
            "namespace": self.namespace,
            "provider": self.provider,
            "resource": self.resource,
            "symbol": self.symbol,
            "market": self.market,
            "timeframe": self.timeframe,
            "range": {"start": self.range_start, "end": self.range_end},
            "as_of": self.as_of,
            "taxonomy": self.taxonomy,
            "adjustment": self.adjustment,
            "currency": self.currency,
            "unit": self.unit,
            "filters": dict(self.filters),
            "pagination": {
                "page": self.page,
                "page_size": self.page_size,
                "complete_range": self.complete_range,
            },
            "revision": {
                "config": self.config_revision,
                "parser": self.parser_revision,
            },
        }

    def canonical(self) -> str:
        return json.dumps(
            self.as_dict(), sort_keys=True, ensure_ascii=False, separators=(",", ":"),
        )


@dataclass(frozen=True)
class CacheNamespaceContract:
    namespace: str
    value_kind: str
    freshness_rule: str
    required_key_fields: tuple[str, ...]
    dependencies: tuple[str, ...]
    allows_valid_empty: bool = False
    allows_negative: bool = False
    negative_ttl_seconds: int = 0
    provisional_negative_ttl_seconds: int = 0
    preserve_unique_raw: bool = False

    def validate_key(self, key: CacheKey) -> None:
        if key.namespace != self.namespace:
            raise CacheContractError(
                f"key namespace {key.namespace!r} 不属于 {self.namespace!r}"
            )
        values = key.as_dict()
        flattened = {
            **values,
            "range_start": key.range_start,
            "range_end": key.range_end,
            "page_size": key.page_size,
            "complete_range": key.complete_range,
            "config_revision": key.config_revision,
            "parser_revision": key.parser_revision,
        }
        missing = [name for name in self.required_key_fields if flattened.get(name) in (None, "")]
        if missing:
            raise CacheContractError(
                f"{self.namespace} cache key 缺少业务维度: {', '.join(missing)}"
            )


@dataclass(frozen=True)
class NegativeCacheRecord:
    key: CacheKey
    negative_reason: str
    source: str
    observed_at: str
    expires_at: str
    dependency_revisions: tuple[tuple[str, str], ...] = ()

    @classmethod
    def confirmed_not_found(
        cls,
        key: CacheKey,
        *,
        negative_reason: str,
        source: str,
        ttl_seconds: int,
        observed_at: datetime | None = None,
        dependency_revisions: Mapping[str, str] | None = None,
    ) -> NegativeCacheRecord:
        if not negative_reason.strip() or not source.strip():
            raise CacheContractError("负缓存必须记录 negative_reason 和 source")
        if ttl_seconds <= 0:
            raise CacheContractError("负缓存 TTL 必须为正数")
        observed = (observed_at or datetime.now(UTC)).astimezone(UTC)
        expires = datetime.fromtimestamp(observed.timestamp() + ttl_seconds, UTC)
        revisions = tuple(sorted((dependency_revisions or {}).items()))
        return cls(
            key=key,
            negative_reason=negative_reason,
            source=source,
            observed_at=observed.isoformat(),
            expires_at=expires.isoformat(),
            dependency_revisions=revisions,
        )


class NegativeCacheStore:
    """Durable, revision-aware negative cache for confirmed absence only."""

    SCHEMA_VERSION: ClassVar[int] = 1

    def __init__(self, path: str | Path):
        self.path = Path(path)
        with self._connect() as connection:
            self._initialize(connection)

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.path, policy="cache", row_factory=True)

    @classmethod
    def _initialize(cls, connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS negative_cache ("
            "namespace TEXT NOT NULL,key_json TEXT NOT NULL,negative_reason TEXT NOT NULL,"
            "source TEXT NOT NULL,observed_at TEXT NOT NULL,expires_at TEXT NOT NULL,"
            "dependency_revisions TEXT NOT NULL DEFAULT '{}',last_accessed_at TEXT NOT NULL,"
            "PRIMARY KEY(namespace,key_json))"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_negative_cache_expiry "
            "ON negative_cache(expires_at)"
        )
        connection.execute(f"PRAGMA user_version={cls.SCHEMA_VERSION}")

    def put(
        self,
        record: NegativeCacheRecord,
        contract: CacheNamespaceContract,
        *,
        result: CacheResult,
    ) -> None:
        require_negative_result(result)
        contract.validate_key(record.key)
        if not contract.allows_negative:
            raise CacheContractError(f"{contract.namespace} 不允许负缓存")
        now = datetime.now(UTC).isoformat()
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO negative_cache(namespace,key_json,negative_reason,source,"
                "observed_at,expires_at,dependency_revisions,last_accessed_at) "
                "VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(namespace,key_json) DO UPDATE SET "
                "negative_reason=excluded.negative_reason,source=excluded.source,"
                "observed_at=excluded.observed_at,expires_at=excluded.expires_at,"
                "dependency_revisions=excluded.dependency_revisions,"
                "last_accessed_at=excluded.last_accessed_at",
                (
                    record.key.namespace, record.key.canonical(), record.negative_reason,
                    record.source, record.observed_at, record.expires_at,
                    json.dumps(dict(record.dependency_revisions), sort_keys=True), now,
                ),
            )

    def get(
        self,
        key: CacheKey,
        *,
        dependency_revisions: Mapping[str, str] | None = None,
        now: datetime | None = None,
    ) -> NegativeCacheRecord | None:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        key_json = key.canonical()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM negative_cache WHERE namespace=? AND key_json=?",
                (key.namespace, key_json),
            ).fetchone()
            if row is None:
                return None
            stored_revisions = json.loads(str(row["dependency_revisions"]) or "{}")
            expired = datetime.fromisoformat(str(row["expires_at"])) <= current
            incompatible = any(
                stored_revisions.get(name) != revision
                for name, revision in (dependency_revisions or {}).items()
            )
            if expired or incompatible:
                connection.execute(
                    "DELETE FROM negative_cache WHERE namespace=? AND key_json=?",
                    (key.namespace, key_json),
                )
                return None
            connection.execute(
                "UPDATE negative_cache SET last_accessed_at=? "
                "WHERE namespace=? AND key_json=?",
                (current.isoformat(), key.namespace, key_json),
            )
            return NegativeCacheRecord(
                key=key,
                negative_reason=str(row["negative_reason"]),
                source=str(row["source"]),
                observed_at=str(row["observed_at"]),
                expires_at=str(row["expires_at"]),
                dependency_revisions=tuple(sorted(stored_revisions.items())),
            )

    def invalidate_dependency(
        self, namespace: str, dependency: str, current_revision: str,
    ) -> int:
        """Invalidate only incompatible entries in one dependent namespace."""

        removed = 0
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT key_json,dependency_revisions FROM negative_cache WHERE namespace=?",
                (namespace,),
            ).fetchall()
            for row in rows:
                revisions = json.loads(str(row["dependency_revisions"]) or "{}")
                if revisions.get(dependency) == current_revision:
                    continue
                connection.execute(
                    "DELETE FROM negative_cache WHERE namespace=? AND key_json=?",
                    (namespace, str(row["key_json"])),
                )
                removed += 1
        return removed

    def prune_expired(self, *, now: datetime | None = None) -> int:
        current = (now or datetime.now(UTC)).astimezone(UTC).isoformat()
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM negative_cache WHERE expires_at<=?", (current,),
            )
            return max(0, int(cursor.rowcount))

    def rows(self, namespace: str = "") -> list[dict[str, Any]]:
        query = "SELECT * FROM negative_cache"
        params: tuple[str, ...] = ()
        if namespace:
            query += " WHERE namespace=?"
            params = (namespace,)
        query += " ORDER BY expires_at"
        with self._connect() as connection:
            return [dict(row) for row in connection.execute(query, params)]


@dataclass
class _NamespaceObservation:
    hits: int = 0
    misses: int = 0
    fresh: int = 0
    stale: int = 0
    partial: int = 0
    negative: int = 0
    oldest: str = ""
    newest: str = ""
    pending_completed: int = 0
    pending_total: int = 0
    negative_reasons: dict[str, int] = field(default_factory=dict)
    stale_consumers: set[str] = field(default_factory=set)
    issues: set[str] = field(default_factory=set)


class CacheNamespaceRegistry:
    """Process-local cache metrics with stable namespace contracts."""

    def __init__(self) -> None:
        self._contracts: dict[str, CacheNamespaceContract] = {}
        self._observations: dict[str, _NamespaceObservation] = {}
        self._lock = threading.RLock()

    def register(self, contract: CacheNamespaceContract) -> None:
        with self._lock:
            existing = self._contracts.get(contract.namespace)
            if existing is not None and existing != contract:
                raise CacheContractError(f"cache namespace 合同冲突: {contract.namespace}")
            self._contracts[contract.namespace] = contract
            self._observations.setdefault(contract.namespace, _NamespaceObservation())

    @staticmethod
    def _observe_counts(
        value: _NamespaceObservation,
        *,
        hit: bool | None,
        state: str,
        observed_at: str,
    ) -> None:
        if hit is True:
            value.hits += 1
        elif hit is False:
            value.misses += 1
        if state in {"fresh", "stale", "partial", "negative"}:
            setattr(value, state, getattr(value, state) + 1)
        if observed_at:
            value.oldest = min(filter(None, (value.oldest, observed_at)), default=observed_at)
            value.newest = max(value.newest, observed_at)

    @staticmethod
    def _observe_details(
        value: _NamespaceObservation,
        *,
        negative_reason: str,
        stale_consumer: str,
        pending_completed: int | None,
        pending_total: int | None,
        diagnostic_code: str,
    ) -> None:
        if negative_reason:
            value.negative_reasons[negative_reason] = (
                value.negative_reasons.get(negative_reason, 0) + 1
            )
        if stale_consumer:
            value.stale_consumers.add(stale_consumer)
        if pending_completed is not None:
            value.pending_completed = max(0, pending_completed)
        if pending_total is not None:
            value.pending_total = max(0, pending_total)
        if diagnostic_code:
            value.issues.add(diagnostic_code)

    def observe(
        self,
        namespace: str,
        *,
        hit: bool | None = None,
        state: str = "",
        observed_at: str = "",
        negative_reason: str = "",
        stale_consumer: str = "",
        pending_completed: int | None = None,
        pending_total: int | None = None,
        diagnostic_code: str = "",
    ) -> None:
        with self._lock:
            if namespace not in self._contracts:
                raise CacheContractError(f"未注册 cache namespace: {namespace}")
            value = self._observations[namespace]
            self._observe_counts(value, hit=hit, state=state, observed_at=observed_at)
            self._observe_details(
                value,
                negative_reason=negative_reason,
                stale_consumer=stale_consumer,
                pending_completed=pending_completed,
                pending_total=pending_total,
                diagnostic_code=diagnostic_code,
            )

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            result = []
            for namespace, contract in sorted(self._contracts.items()):
                value = self._observations[namespace]
                requests = value.hits + value.misses
                result.append({
                    "namespace": namespace,
                    "value_kind": contract.value_kind,
                    "freshness_rule": contract.freshness_rule,
                    "dependencies": list(contract.dependencies),
                    "hit_rate": value.hits / requests if requests else None,
                    "hits": value.hits,
                    "misses": value.misses,
                    "fresh": value.fresh,
                    "stale": value.stale,
                    "partial": value.partial,
                    "negative": value.negative,
                    "oldest": value.oldest,
                    "newest": value.newest,
                    "pending": {
                        "completed": value.pending_completed,
                        "total": value.pending_total,
                    },
                    "negative_reasons": dict(sorted(value.negative_reasons.items())),
                    "stale_consumers": sorted(value.stale_consumers),
                    "issues": sorted(value.issues),
                    "diagnostic_code": "" if requests else "CACHE_NAMESPACE_UNOBSERVED",
                })
            return result


BUILTIN_CACHE_CONTRACTS = (
    CacheNamespaceContract(
        "stockdb.raw", "unique raw market evidence",
        "provider generation plus requested session range; never discard the only raw evidence",
        ("provider", "resource", "market", "range_start", "range_end", "complete_range"),
        ("stockdb_generation",), preserve_unique_raw=True,
    ),
    CacheNamespaceContract(
        "provider.raw", "raw provider response",
        "endpoint-specific trading-session or publication-event rule",
        ("provider", "resource", "range_start", "range_end", "config_revision"),
        ("provider_config",), allows_valid_empty=True, preserve_unique_raw=True,
    ),
    CacheNamespaceContract(
        "provider.normalized", "validated normalized frame",
        "inherits raw freshness and parser compatibility",
        (
            "provider", "resource", "range_start", "range_end", "config_revision",
            "parser_revision",
        ),
        ("provider_config", "parser"), allows_valid_empty=True,
    ),
    CacheNamespaceContract(
        "news.raw", "content-addressed HTTP evidence",
        "conditional HTTP validators and source publication cadence",
        ("provider", "resource", "config_revision"),
        ("source_config",), allows_valid_empty=True, preserve_unique_raw=True,
    ),
    CacheNamespaceContract(
        "news.normalized", "parsed news item",
        "raw response revision plus parser revision",
        ("provider", "resource", "config_revision", "parser_revision"),
        ("source_config", "parser"), allows_valid_empty=True,
    ),
    CacheNamespaceContract(
        "market.bars", "normalized OHLCV bars",
        "market calendar and close boundary; formal reads are bounded by as_of",
        (
            "provider", "resource", "symbol", "market", "timeframe", "range_start",
            "range_end", "adjustment", "currency", "unit", "config_revision",
            "parser_revision",
        ),
        ("provider_config", "parser", "calendar"),
    ),
    CacheNamespaceContract(
        "industry.catalog", "point-in-time taxonomy membership",
        "taxonomy release event and explicit as_of",
        ("provider", "resource", "market", "as_of", "taxonomy", "config_revision"),
        ("provider_config", "taxonomy"), allows_negative=True,
        negative_ttl_seconds=30 * 86400, provisional_negative_ttl_seconds=6 * 3600,
    ),
    CacheNamespaceContract(
        "instrument.catalog", "point-in-time instrument catalog",
        "listing/delisting publication event and market session",
        ("provider", "resource", "market", "as_of", "config_revision"),
        ("provider_config", "catalog_schema"), allows_negative=True,
        negative_ttl_seconds=30 * 86400, provisional_negative_ttl_seconds=3600,
    ),
    CacheNamespaceContract(
        "model.catalog", "model metadata and artifact availability",
        "artifact publication or removal event",
        ("provider", "resource", "config_revision", "parser_revision"),
        ("model_config", "model_schema"), allows_negative=True,
        negative_ttl_seconds=86400, provisional_negative_ttl_seconds=300,
    ),
    CacheNamespaceContract(
        "capability.probe", "provider capability observation",
        "short probe TTL; credentials/config changes invalidate only the provider lane",
        ("provider", "resource", "config_revision"),
        ("provider_config",),
    ),
    CacheNamespaceContract(
        "lab.panel", "research panel",
        "immutable historical inputs bounded by as_of",
        (
            "provider", "resource", "market", "timeframe", "range_start", "range_end",
            "as_of", "adjustment", "currency", "unit", "config_revision",
            "parser_revision",
        ),
        ("provider_config", "parser", "calendar", "universe"),
    ),
)


cache_registry = CacheNamespaceRegistry()
for _contract in BUILTIN_CACHE_CONTRACTS:
    cache_registry.register(_contract)


def cache_contract(namespace: str) -> CacheNamespaceContract:
    for contract in BUILTIN_CACHE_CONTRACTS:
        if contract.namespace == namespace:
            return contract
    raise CacheContractError(f"未知 cache namespace: {namespace}")


def require_negative_result(result: CacheResult) -> None:
    """Reject temporary/provider failures before a negative cache write."""

    if result.kind != CacheResultKind.NOT_FOUND:
        raise CacheContractError(
            f"只有 not_found 可写负缓存，收到 {result.kind.value}"
        )


RevisionAction = Literal["use_normalized", "reparse_raw", "refetch"]


def revision_action(
    *,
    cached_config_revision: str,
    cached_parser_revision: str,
    current_config_revision: str,
    current_parser_revision: str,
    raw_available: bool,
) -> RevisionAction:
    """Choose the narrowest safe action after config or parser changes.

    Provider/config changes make the raw response identity incompatible and require a
    refetch.  A parser-only change reuses local raw evidence whenever it is still present.
    """

    if cached_config_revision != current_config_revision:
        return "refetch"
    if cached_parser_revision != current_parser_revision:
        return "reparse_raw" if raw_available else "refetch"
    return "use_normalized"


@dataclass(frozen=True)
class CacheCleanupCandidate:
    path: Path
    namespace: str
    size_bytes: int
    last_accessed_at: str
    regenerable: bool
    unique_raw: bool = False
    in_use: bool = False


def plan_cache_cleanup(
    root: str | Path,
    candidates: list[CacheCleanupCandidate],
    *,
    bytes_to_free: int,
) -> list[CacheCleanupCandidate]:
    """Plan namespace-aware cleanup without deleting files or dangling evidence.

    The caller still owns deletion and must re-check active use immediately before each
    removal.  This planner only returns regenerable, non-raw, non-active files contained
    by the named cache root, ordered by oldest access then largest size.
    """

    if bytes_to_free <= 0:
        return []
    boundary = Path(root).resolve()
    eligible = []
    for candidate in candidates:
        path = candidate.path.resolve()
        if path != boundary and not path.is_relative_to(boundary):
            raise CacheContractError(f"清理候选越出 cache root: {path}")
        if (
            candidate.in_use
            or candidate.unique_raw
            or not candidate.regenerable
            or candidate.size_bytes <= 0
        ):
            continue
        eligible.append(candidate)
    eligible.sort(key=lambda item: (item.last_accessed_at, -item.size_bytes, item.namespace))
    selected = []
    freed = 0
    for candidate in eligible:
        selected.append(candidate)
        freed += candidate.size_bytes
        if freed >= bytes_to_free:
            break
    return selected
