"""Published, local-only market overview snapshots.

The market page used to rebuild every card directly from BarStore on every
request.  That is safer than doing a provider refresh in GET, but it still
turns a slow disk or a locked database into a page-wide stall.  The runtime
worker builds this compact projection and atomically advances one catalog
pointer; Web generations only read that pointer and immutable JSON artifact.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from quantmaster.runtime.derived import DerivedArtifactCatalog, DerivedArtifactIntegrityError
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.problems import OperationProblem, make_problem


DOMAIN = "market"
SNAPSHOT_TYPE = "overview"
SCHEMA_VERSION = "1"
ALGORITHM_VERSION = "QM_MARKET_OVERVIEW_SNAPSHOT_V1"
# One inexpensive pointer check per short window is enough to notice a worker
# publication promptly. Between checks, pages reuse a verified immutable
# object and its pre-encoded wire representation.
SNAPSHOT_POINTER_CACHE_SECONDS = 0.25


@dataclass(frozen=True)
class _SnapshotRead:
    root: str
    artifact_id: str
    updated_at: float
    checked_at: float
    payload: dict[str, Any]
    encoded: bytes


_snapshot_cache: _SnapshotRead | None = None
_snapshot_cache_lock = threading.Lock()


def _snapshot_unavailable(exc: Exception) -> OperationProblem:
    return OperationProblem(
        503,
        make_problem(
            "snapshot_unavailable",
            severity="warning",
            source="市场概览快照",
            title="尚无可展示的市场快照",
            message="后台 worker 尚未发布市场总览，或当前快照未通过完整性校验。",
            action="可继续浏览其他页面；等待 runtime-worker 完成一次本地快照构建后重试。",
            blocking=False,
            can_continue=True,
        ),
    )


def invalidate_market_overview_snapshot_cache(root: object | None = None) -> None:
    """Drop this process's immutable projection after a same-process publish."""

    global _snapshot_cache
    with _snapshot_cache_lock:
        if root is None or (_snapshot_cache is not None and _snapshot_cache.root == str(root)):
            _snapshot_cache = None


def publish_market_overview_snapshot() -> dict[str, Any]:
    """Build and atomically publish the default market-card projection.

    This function is runtime-worker only.  The existing builder is deliberately
    called with ``refresh='local'``: its only inputs are local BarStore,
    portfolio and name-cache records.  A failed build never replaces the
    previous pointer.
    """

    # Kept as a lazy import so ordinary Web snapshot reads never import the
    # FastAPI application or its optional provider integrations.
    from quantmaster.server.app import _market_overview_data

    data = _market_overview_data(refresh="local")
    if not isinstance(data, dict):
        raise RuntimeError("市场快照构建器返回了无效结果")
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    catalog = DerivedArtifactCatalog()
    artifact = catalog.put_json(
        data,
        schema_version=SCHEMA_VERSION,
        coverage_end=str(meta.get("as_of") or ""),
    )
    published = catalog.publish_snapshot(DOMAIN, SNAPSHOT_TYPE, str(artifact["artifact_id"]))
    invalidate_market_overview_snapshot_cache(catalog.root)
    return {
        "id": str(artifact["artifact_id"]),
        "published_at": float(published.get("updated_at") or 0),
        "as_of": str(meta.get("as_of") or ""),
    }


def _snapshot_state(data: dict[str, Any]) -> tuple[str, list[str]]:
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    quality = data.get("data_quality") if isinstance(data.get("data_quality"), dict) else {}
    issues = [str(value) for value in quality.get("issues", []) if str(value)]
    issues.extend(str(value) for value in meta.get("stale_reasons", []) if str(value))
    if bool(meta.get("stale")) or bool(quality.get("stale")):
        return "stale", list(dict.fromkeys(issues))
    if str(quality.get("status") or "") == "verified":
        return "fresh", list(dict.fromkeys(issues))
    return "degraded", list(dict.fromkeys(issues))


def _read_market_overview_snapshot() -> _SnapshotRead:
    """Load and verify a new immutable artifact at most once per cache window."""

    global _snapshot_cache
    try:
        catalog = DerivedArtifactCatalog(read_only=True)
        root = str(catalog.root)
    except (FileNotFoundError, OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
        raise _snapshot_unavailable(exc) from exc

    now = time.monotonic()
    # The lock makes concurrent first readers a single flight. Holding it for
    # a local SQLite pointer check and one JSON parse is bounded, and prevents
    # every Web thread from hashing/decoding the same 350KB snapshot at once.
    with _snapshot_cache_lock:
        cached = _snapshot_cache
        if (
            cached is not None
            and cached.root == root
            and now - cached.checked_at < SNAPSHOT_POINTER_CACHE_SECONDS
        ):
            return cached
        try:
            pointer = catalog.current_snapshot_pointer(DOMAIN, SNAPSHOT_TYPE)
            if pointer is None:
                raise KeyError("current market overview snapshot")
            artifact_id = str(pointer["artifact_id"])
            updated_at = float(pointer.get("updated_at") or 0)
            if (
                cached is not None
                and cached.root == root
                and cached.artifact_id == artifact_id
                and cached.updated_at == updated_at
            ):
                _snapshot_cache = _SnapshotRead(
                    root=root,
                    artifact_id=artifact_id,
                    updated_at=updated_at,
                    checked_at=now,
                    payload=cached.payload,
                    encoded=cached.encoded,
                )
                return _snapshot_cache
            data = catalog.read_json(artifact_id)
            if not isinstance(data, dict) or not isinstance(data.get("groups"), dict):
                raise DerivedArtifactIntegrityError("市场快照 payload 不完整")
        except (
            FileNotFoundError,
            KeyError,
            OSError,
            RuntimeError,
            ValueError,
            sqlite3.Error,
            DerivedArtifactIntegrityError,
        ) as exc:
            raise _snapshot_unavailable(exc) from exc

        meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
        state, issues = _snapshot_state(data)
        published_at = datetime.fromtimestamp(updated_at, UTC).isoformat() if updated_at else ""
        payload = {
            "data": data,
            "snapshot": {
                "id": artifact_id,
                "as_of": str(meta.get("as_of") or ""),
                "published_at": published_at,
                "state": state,
                "source_generations": {},
                "issues": issues,
                "algorithm_version": ALGORITHM_VERSION,
            },
            # A stale page must remain readable even when the worker is down.
            # The explicit refresh control uses the unified job API instead of
            # turning this read path into a hidden submission side effect.
            "refresh": {"job_id": "", "status": "idle"},
        }
        _snapshot_cache = _SnapshotRead(
            root=root,
            artifact_id=artifact_id,
            updated_at=updated_at,
            checked_at=now,
            payload=payload,
            encoded=strict_json_dumps(payload).encode("utf-8"),
        )
        return _snapshot_cache


def read_market_overview_snapshot() -> dict[str, Any]:
    """Return the last fully published response; callers must treat it immutable."""

    return _read_market_overview_snapshot().payload


def read_market_overview_snapshot_wire() -> tuple[dict[str, Any], bytes]:
    """Return a verified response plus its one-time encoded JSON representation."""

    snapshot = _read_market_overview_snapshot()
    return snapshot.payload, snapshot.encoded
