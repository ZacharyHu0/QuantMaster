"""Immutable, reusable ingestion snapshots over a user-managed free-stockdb."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.free_stockdb_contracts import (
    StockDBArtifactIdentity,
    StockDBCatalogSnapshot,
    StockDBFieldCoverage,
    StockDBIngestSnapshot,
)
from quantmaster.data.free_stockdb_source import FreeStockDBSource
from quantmaster.data.resilience import remote_io_allowed
from quantmaster.runtime.derived import DerivedArtifactCatalog
from quantmaster.runtime.json import content_hash

Progress = Callable[[int, str, str], None]
Cancelled = Callable[[], bool]
Validator = Callable[[pd.DataFrame, list[dict[str, Any]], int], tuple[str, dict[str, Any]]]
STOCKDB_INGEST_SCHEMA_VERSION = "3.0"
STOCKDB_CROSS_VALIDATION_SCHEMA_VERSION = "1.0"
_CROSS_VALIDATION_SYMBOLS = 32
_CROSS_VALIDATION_DATES = 5
_CROSS_PRICE_TOLERANCE = 0.005
_CROSS_UNIT_TOLERANCE = 0.01


class StockDBIngestRejected(RuntimeError):
    def __init__(self, reasons: list[str], coverage: dict[str, Any], as_of_date: str = ""):
        super().__init__("；".join(reasons))
        self.reasons = reasons
        self.coverage = coverage
        self.as_of_date = as_of_date


_FIELD_UNITS = {
    "open": "CNY/share",
    "high": "CNY/share",
    "low": "CNY/share",
    "close": "CNY/share",
    "pre_close": "CNY/share",
    "volume": "share",
    "amount": "CNY",
    "pct_chg": "percent",
    "amplitude": "percent",
    "turnover": "percent",
    "vol_ratio": "ratio",
    "total_share": "share",
    "float_share": "share",
    "total_mv": "CNY",
    "float_mv": "CNY",
    "pe_ttm": "ratio",
    "pb": "ratio",
    "is_st": "boolean",
    "name": "text",
}

# These meanings are evidenced by the locally installed vendor SDK, its table
# examples, and arithmetic checks in the shipped examples (rather than by
# projecting a similarly named Tushare field onto StockDB).  They apply to the
# A-share/ETF K-line tables this adapter reads; futures are deliberately out of
# scope for this source.
_FIELD_SEMANTICS = {
    "open": "CNY per share; raw unless the query explicitly requests fq",
    "high": "CNY per share; raw unless the query explicitly requests fq",
    "low": "CNY per share; raw unless the query explicitly requests fq",
    "close": "CNY per share; raw unless the query explicitly requests fq",
    "pre_close": "CNY per share; previous session close and adjusted with OHLC",
    "volume": "shares traded; never adjusted by the StockDB fq transform",
    "amount": "CNY traded; never adjusted by the StockDB fq transform",
    "pct_chg": "percent",
    "amplitude": "percent",
    "turnover": "percent of float shares",
    "vol_ratio": "dimensionless ratio",
    "total_share": "shares outstanding",
    "float_share": "tradable float shares",
    "total_mv": "CNY market value",
    "float_mv": "CNY float market value",
    "pe_ttm": "dimensionless trailing P/E ratio",
    "pb": "dimensionless price-to-book ratio",
    "is_st": "boolean session observation; vendor does not document PIT revision timing",
    "name": "vendor display name, not a stable security identifier",
}


def _frame_hash(frame: pd.DataFrame) -> str:
    if frame.empty:
        return hashlib.sha256(b"empty").hexdigest()
    value = frame.copy()
    for column in value:
        if pd.api.types.is_datetime64_any_dtype(value[column]):
            value[column] = pd.to_datetime(value[column], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
    sort = [column for column in ("symbol", "date", "event_time_utc") if column in value]
    if sort:
        value = value.sort_values(sort, kind="mergesort").reset_index(drop=True)
    digest = hashlib.sha256()
    digest.update("\x1f".join(str(column) for column in value.columns).encode())
    digest.update(
        pd.util.hash_pandas_object(value, index=False, categorize=True)
        .to_numpy(
            dtype="uint64",
            copy=False,
        )
        .tobytes()
    )
    return digest.hexdigest()


def _json_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
    ).hexdigest()


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(fd)
    temp = Path(temp_name)
    try:
        temp.write_text(value, encoding="utf-8")
        with temp.open("rb+") as stream:
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def _atomic_frame(path: Path, frame: pd.DataFrame) -> None:
    """Commit a verified remote fragment without exposing a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(fd)
    temp = Path(temp_name)
    try:
        frame.to_parquet(temp, index=True)
        with temp.open("rb+") as stream:
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


class StockDBIngestStore:
    """Content-addressed payloads with immutable, small manifest files."""

    def __init__(self, root: str | Path | None = None, *, retain: int | None = None):
        cfg = get_config().data
        self.root = Path(root or (get_config().data_root / "stockdb-ingest")).resolve()
        self.retain = int(retain if retain is not None else cfg.free_stockdb_ingest_retain)
        self._lock = threading.RLock()

    @property
    def manifests(self) -> Path:
        return self.root / "manifests"

    @property
    def content(self) -> Path:
        return self.root / "content"

    @property
    def pins(self) -> Path:
        return self.root / "pins"

    @property
    def cross_validation(self) -> Path:
        """Worktree-local durable state for the bounded remote audit lane."""
        return self.root / "cross-validation"

    @staticmethod
    def _checkpoint_symbol(symbol: str) -> str:
        value = re.sub(r"[^A-Za-z0-9._-]+", "_", str(symbol).upper())
        if not value:
            raise ValueError("cross-validation checkpoint requires a symbol")
        return value

    def _cross_validation_item_path(self, request_id: str, symbol: str) -> Path:
        return self.cross_validation / str(request_id) / "items" / (
            f"{self._checkpoint_symbol(symbol)}.json"
        )

    def _cross_validation_frame_path(self, request_id: str, symbol: str) -> Path:
        return self.cross_validation / str(request_id) / "frames" / (
            f"{self._checkpoint_symbol(symbol)}.parquet"
        )

    def cross_validation_item(self, request_id: str, symbol: str) -> dict[str, Any] | None:
        """Read one completed audit item; malformed state is retried safely."""
        try:
            value = json.loads(
                self._cross_validation_item_path(request_id, symbol).read_text(encoding="utf-8")
            )
        except (FileNotFoundError, OSError, TypeError, ValueError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None

    def persist_cross_validation_item(
        self,
        request_id: str,
        symbol: str,
        value: dict[str, Any],
        *,
        frame: pd.DataFrame | None = None,
    ) -> dict[str, Any]:
        """Atomically upsert one audit item after its identity/time/field checks.

        A verified item owns a small normalized remote fragment.  The manifest
        is written only after that fragment has been fsync'ed, so a later
        interrupted batch can reuse completed symbols without another request.
        """
        payload = {**value, "request_id": str(request_id), "symbol": str(symbol).upper()}
        with self._lock:
            if frame is not None:
                _atomic_frame(self._cross_validation_frame_path(request_id, symbol), frame)
                payload["frame"] = self._cross_validation_frame_path(request_id, symbol).name
            _atomic_text(
                self._cross_validation_item_path(request_id, symbol),
                json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, default=str),
            )
        return payload

    def load_cross_validation_frame(self, request_id: str, symbol: str) -> pd.DataFrame:
        path = self._cross_validation_frame_path(request_id, symbol)
        try:
            return pd.read_parquet(path)
        except (FileNotFoundError, OSError, ValueError):
            return pd.DataFrame()

    @staticmethod
    def _pin_name(namespace: str, reference_id: str) -> str:
        digest = hashlib.sha256(f"{namespace}:{reference_id}".encode()).hexdigest()[:24]
        return f"{digest}.json"

    def pin(
        self,
        ingest_id: str,
        namespace: str,
        reference_id: str,
        metadata: dict[str, Any] | None = None,
        *,
        require_exists: bool = True,
    ) -> dict[str, Any]:
        """Protect an ingest referenced by a published immutable artifact."""
        if require_exists and self.get(ingest_id) is None:
            raise FileNotFoundError(f"被引用的 free-stockdb 摄取不存在: {ingest_id}")
        payload = {
            "ingest_id": str(ingest_id),
            "namespace": str(namespace),
            "reference_id": str(reference_id),
            "metadata": metadata or {},
        }
        _atomic_text(
            self.pins / self._pin_name(namespace, reference_id),
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, default=str),
        )
        return payload

    def references(self, ingest_id: str = "") -> list[dict[str, Any]]:
        values: list[dict[str, Any]] = []
        for path in self.pins.glob("*.json"):
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            if not ingest_id or str(value.get("ingest_id") or "") == ingest_id:
                values.append(value)
        return sorted(
            values,
            key=lambda item: (
                str(item.get("namespace") or ""),
                str(item.get("reference_id") or ""),
            ),
        )

    @staticmethod
    def _ingest_ids(value: Any) -> set[str]:
        found: set[str] = set()
        if isinstance(value, dict):
            for key, item in value.items():
                if key == "ingest_id" and str(item).startswith("sdi_"):
                    found.add(str(item))
                found.update(StockDBIngestStore._ingest_ids(item))
        elif isinstance(value, (list, tuple)):
            for item in value:
                found.update(StockDBIngestStore._ingest_ids(item))
        elif isinstance(value, str) and value.startswith("sdi_"):
            found.add(value)
        return found

    def _after_close_references(self, root: Path) -> dict[tuple[str, str], str]:
        found: dict[tuple[str, str], str] = {}
        after_close = root / "after_close.sqlite"
        if not after_close.is_file():
            return found
        try:
            from quantmaster.runtime.sqlite import connect_sqlite

            with connect_sqlite(after_close, row_factory=True) as connection:
                rows = connection.execute("SELECT snapshot_id,payload_json FROM snapshots").fetchall()
            for row in rows:
                for ingest_id in self._ingest_ids(json.loads(str(row["payload_json"]))):
                    found[("after_close", str(row["snapshot_id"]))] = ingest_id
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
            pass
        return found

    def _etf_references(self, root: Path) -> dict[tuple[str, str], str]:
        found: dict[tuple[str, str], str] = {}
        for path in (root / "etf-research" / "snapshots").glob("*.json"):
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
                for ingest_id in self._ingest_ids(value):
                    found[("etf_research", str(value.get("snapshot_id") or path.stem))] = ingest_id
            except (OSError, ValueError, json.JSONDecodeError):
                continue
        return found

    def _research_references(self, root: Path) -> dict[tuple[str, str], str]:
        found: dict[tuple[str, str], str] = {}
        research_catalog = root / "research_lake" / "_meta" / "catalog.sqlite"
        if not research_catalog.is_file():
            return found
        try:
            from quantmaster.runtime.sqlite import connect_sqlite

            with connect_sqlite(research_catalog, row_factory=True) as connection:
                rows = connection.execute(
                    "SELECT partition_key,input_hashes_json FROM research_partitions"
                ).fetchall()
            for row in rows:
                for ingest_id in self._ingest_ids(json.loads(str(row["input_hashes_json"]))):
                    found[("research_lake", str(row["partition_key"]))] = ingest_id
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
            pass
        return found

    def backfill_references(self) -> dict[str, Any]:
        """Rebuild pins from existing formal stores before garbage collection."""
        root = get_config().data_root
        found = self._after_close_references(root)
        found.update(self._etf_references(root))
        found.update(self._research_references(root))
        for (namespace, reference_id), ingest_id in found.items():
            self.pin(ingest_id, namespace, reference_id, require_exists=False)
        missing = sorted({value for value in found.values() if self.get(value) is None})
        return {"references": len(found), "missing_ingests": missing}

    def history(self, limit: int = 100) -> list[StockDBIngestSnapshot]:
        records: list[StockDBIngestSnapshot] = []
        for path in sorted(
            self.manifests.glob("*.json"),
            key=lambda item: item.stat().st_mtime_ns,
            reverse=True,
        ):
            try:
                records.append(StockDBIngestSnapshot.from_dict(json.loads(path.read_text(encoding="utf-8"))))
            except (OSError, TypeError, ValueError, json.JSONDecodeError):
                continue
            if len(records) >= max(1, limit):
                break
        return records

    def get(self, ingest_id: str) -> StockDBIngestSnapshot | None:
        path = self.manifests / f"{ingest_id}.json"
        try:
            return StockDBIngestSnapshot.from_dict(json.loads(path.read_text(encoding="utf-8")))
        except FileNotFoundError:
            return None

    def find(self, cache_key: str) -> StockDBIngestSnapshot | None:
        return next(
            (
                item
                for item in self.history(self.retain + 20)
                if item.provenance.get("cache_key") == cache_key
                and item.status in {"complete", "degraded"}
            ),
            None,
        )

    def load_frame(self, snapshot: StockDBIngestSnapshot, name: str = "stock_daily") -> pd.DataFrame:
        digest = snapshot.content_hashes.get(name, "")
        path = self.content / f"{digest}.parquet"
        return pd.read_parquet(path) if digest and path.is_file() else pd.DataFrame()

    def load_json(self, snapshot: StockDBIngestSnapshot, name: str) -> Any:
        digest = snapshot.content_hashes.get(name, "")
        path = self.content / f"{digest}.json"
        return json.loads(path.read_text(encoding="utf-8")) if digest and path.is_file() else []

    def _write_frame(self, frame: pd.DataFrame) -> str:
        digest = _frame_hash(frame)
        target = self.content / f"{digest}.parquet"
        if target.is_file():
            return digest
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=f".{digest}.", suffix=".parquet.tmp", dir=target.parent)
        os.close(fd)
        temp = Path(temp_name)
        try:
            frame.to_parquet(temp, index=False)
            if not target.exists():
                os.replace(temp, target)
        finally:
            temp.unlink(missing_ok=True)
        return digest

    def _write_json(self, value: Any) -> str:
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        target = self.content / f"{digest}.json"
        if not target.is_file():
            _atomic_text(target, encoded)
        return digest

    def publish(
        self,
        *,
        frame: pd.DataFrame,
        adjustment: pd.DataFrame | None = None,
        boards: list[dict[str, Any]],
        catalog: list[dict[str, Any]],
        delisted: list[dict[str, Any]],
        as_of_date: str,
        artifact_id: str,
        master_snapshot_id: str,
        start_date: str,
        end_date: str,
        coverage: dict[str, Any],
        provenance: dict[str, Any],
        assets: dict[str, Any] | None = None,
        catalog_snapshot: StockDBCatalogSnapshot | None = None,
        session_dates: list[str] | tuple[str, ...] = (),
        session_source: str = "",
        status: str = "complete",
        issues: list[str] | tuple[str, ...] = (),
    ) -> StockDBIngestSnapshot:
        with self._lock:
            hashes = {
                "stock_daily": self._write_frame(frame),
                "stock_adjustment_factors": self._write_frame(
                    adjustment if adjustment is not None else pd.DataFrame()
                ),
                "boards": self._write_json(boards),
                "catalog": self._write_json(catalog),
                "delisted": self._write_json(delisted),
            }
            if catalog_snapshot is not None:
                hashes["catalog_snapshot"] = self._write_json(catalog_snapshot.to_dict())
            logical = {
                "schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                "as_of_date": as_of_date,
                "artifact_id": artifact_id,
                "master_snapshot_id": master_snapshot_id,
                "start_date": start_date,
                "end_date": end_date,
                "content_hashes": hashes,
                "coverage": coverage,
                "provenance": provenance,
                "catalog_id": catalog_snapshot.snapshot_id if catalog_snapshot else "",
                "session_dates": list(session_dates),
                "session_source": session_source,
                "status": status,
                "issues": list(issues),
            }
            ingest_id = "sdi_" + content_hash(logical)[:24]
            snapshot = StockDBIngestSnapshot(
                ingest_id=ingest_id,
                as_of_date=as_of_date,
                artifact_id=artifact_id,
                master_snapshot_id=master_snapshot_id,
                start_date=start_date,
                end_date=end_date,
                assets=assets or {"stock": {"rows": len(frame)}},
                coverage=coverage,
                content_hashes=hashes,
                provenance=provenance,
                catalog_id=catalog_snapshot.snapshot_id if catalog_snapshot else "",
                session_dates=tuple(session_dates),
                session_source=session_source,
                status=status,
                issues=tuple(issues),
            )
            path = self.manifests / f"{ingest_id}.json"
            encoded = json.dumps(
                snapshot.to_dict(), ensure_ascii=False, sort_keys=True, indent=2, default=str
            )
            if path.exists():
                existing = self.get(ingest_id)
                if existing is None or existing.content_hashes != hashes:
                    raise RuntimeError(f"free-stockdb 摄取清单不可变: {ingest_id}")
                self._advance_generation(existing)
                return existing
            _atomic_text(path, encoded)
            self._advance_generation(snapshot)
            self.prune()
            return snapshot

    def publish_etf(
        self,
        *,
        daily: pd.DataFrame,
        minutes: pd.DataFrame,
        profiles: list[dict[str, Any]],
        as_of_date: str,
        artifact_id: str,
        master_snapshot_id: str,
        start_date: str,
        end_date: str,
        coverage: dict[str, Any],
        provenance: dict[str, Any],
        session_dates: list[str] | tuple[str, ...] = (),
        session_source: str = "stockdb_broad",
    ) -> StockDBIngestSnapshot:
        with self._lock:
            hashes = {
                "etf_daily": self._write_frame(daily),
                "etf_minutes": self._write_frame(minutes),
                "etf_profiles": self._write_json(profiles),
            }
            logical = {
                "schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                "asset": "etf",
                "as_of_date": as_of_date,
                "artifact_id": artifact_id,
                "master_snapshot_id": master_snapshot_id,
                "start_date": start_date,
                "end_date": end_date,
                "content_hashes": hashes,
                "coverage": coverage,
                "session_dates": list(session_dates),
                "session_source": session_source,
            }
            ingest_id = "sdi_" + content_hash(logical)[:24]
            snapshot = StockDBIngestSnapshot(
                ingest_id=ingest_id,
                as_of_date=as_of_date,
                artifact_id=artifact_id,
                master_snapshot_id=master_snapshot_id,
                start_date=start_date,
                end_date=end_date,
                assets={
                    "etf": {"daily_rows": len(daily), "minute_rows": len(minutes), "symbols": len(profiles)},
                },
                coverage=coverage,
                content_hashes=hashes,
                provenance=provenance,
                session_dates=tuple(session_dates),
                session_source=session_source,
            )
            path = self.manifests / f"{ingest_id}.json"
            encoded = json.dumps(
                snapshot.to_dict(), ensure_ascii=False, sort_keys=True, indent=2, default=str
            )
            if path.exists():
                existing = self.get(ingest_id)
                if existing is None or existing.content_hashes != hashes:
                    raise RuntimeError(f"free-stockdb ETF 摄取清单不可变: {ingest_id}")
                self._advance_generation(existing)
                return existing
            _atomic_text(path, encoded)
            self._advance_generation(snapshot)
            self.prune()
            return snapshot

    @staticmethod
    def _advance_generation(snapshot: StockDBIngestSnapshot) -> None:
        """Publish the immutable ingest identity into the shared DAG catalog.

        The manifest has already been atomically written at this point.  A
        compact catalog generation is therefore a trustworthy input version for
        market, ETF and after-close nodes; readers never need to hash or scan
        the large Parquet payload again just to decide whether work is stale.
        """

        asset = "etf" if "etf" in snapshot.assets else "stock"
        coverage_start = str(snapshot.start_date or snapshot.as_of_date or "")
        coverage_end = str(snapshot.as_of_date or snapshot.end_date or "")
        DerivedArtifactCatalog().advance_source_generation(
            f"stockdb.ingest.{asset}",
            str(snapshot.as_of_date or snapshot.ingest_id),
            str(snapshot.ingest_id),
            coverage_start=coverage_start,
            coverage_end=coverage_end,
        )

    def accepted_cross_validation(self, stock_daily_hash: str) -> dict[str, Any] | None:
        """Reuse immutable successful sample evidence for identical bytes."""

        if not stock_daily_hash:
            return None
        for snapshot in self.history(self.retain + 20):
            if snapshot.content_hashes.get("stock_daily") != stock_daily_hash:
                continue
            evidence = snapshot.coverage.get("cross_source_validation")
            if (
                isinstance(evidence, dict)
                and evidence.get("schema_version") == STOCKDB_CROSS_VALIDATION_SCHEMA_VERSION
                and evidence.get("status") == "verified"
                and evidence.get("content_hash") == stock_daily_hash
            ):
                return dict(evidence)
        return None

    def prune(self) -> None:
        self.backfill_references()
        manifests = sorted(
            self.manifests.glob("*.json"),
            key=lambda item: item.stat().st_mtime_ns,
            reverse=True,
        )
        pinned = {str(item.get("ingest_id") or "") for item in self.references()}
        unpinned_kept = 0
        kept_paths: list[Path] = []
        for path in manifests:
            if path.stem in pinned or unpinned_kept < self.retain:
                kept_paths.append(path)
                if path.stem not in pinned:
                    unpinned_kept += 1
            else:
                path.unlink(missing_ok=True)
        referenced: set[str] = set()
        for path in kept_paths:
            try:
                item = StockDBIngestSnapshot.from_dict(json.loads(path.read_text(encoding="utf-8")))
                referenced.update(item.content_hashes.values())
            except (OSError, TypeError, ValueError, json.JSONDecodeError):
                continue
        for path in self.content.glob("*"):
            if path.stem not in referenced:
                path.unlink(missing_ok=True)


class StockDBIngestService:
    def __init__(
        self,
        source: FreeStockDBSource | None = None,
        store: StockDBIngestStore | None = None,
    ):
        self.source = source or FreeStockDBSource()
        self.store = store or StockDBIngestStore()
        self.read_metrics: list[dict[str, Any]] = []

    @staticmethod
    def master_snapshot_id(instruments: Iterable[Any]) -> str:
        records = [
            {
                "symbol": str(item.symbol),
                "status": str(item.status),
                "list_date": str(item.list_date),
                "delist_date": str(item.delist_date),
                "asset_type": str(item.asset_type),
                "source": str(item.source),
            }
            for item in instruments
        ]
        return "master_" + content_hash(sorted(records, key=lambda item: item["symbol"]))[:24]

    @staticmethod
    def _master_records(instruments: Iterable[Any]) -> list[dict[str, Any]]:
        return sorted(
            [
                {
                    "symbol": str(item.symbol).upper(),
                    "code": str(item.code),
                    "name": str(item.name),
                    "status": str(item.status),
                    "list_date": str(item.list_date),
                    "delist_date": str(item.delist_date),
                    "asset_type": str(item.asset_type),
                    "source": str(item.source),
                }
                for item in instruments
            ],
            key=lambda item: item["symbol"],
        )

    @staticmethod
    def _catalog_symbol(item: dict[str, Any]) -> tuple[str, str]:
        raw = str(item.get("symbol") or item.get("ts_code") or item.get("code") or "").upper()
        if "." in raw:
            code, suffix = raw.rsplit(".", 1)
            if suffix in {"SH", "SZ", "BJ"}:
                return f"{code.zfill(6)}.{suffix}", code.zfill(6)
        code = "".join(character for character in raw if character.isdigit())[-6:].zfill(6)
        exchange = str(item.get("exchange") or item.get("market") or "").upper()
        suffix = {"SSE": "SH", "SH": "SH", "SZSE": "SZ", "SZ": "SZ", "BSE": "BJ", "BJ": "BJ"}.get(
            exchange, ""
        )
        return (f"{code}.{suffix}" if code and suffix else ""), code

    @classmethod
    def catalog_snapshot(
        cls,
        *,
        catalog: list[dict[str, Any]],
        delisted: list[dict[str, Any]],
        boards: list[dict[str, Any]],
        instruments: list[Any],
        as_of_date: str,
        artifact_id: str,
    ) -> StockDBCatalogSnapshot:
        master = cls._master_records(instruments)
        master_by_symbol = {item["symbol"]: item for item in master}
        master_by_code = {item["code"].zfill(6): item for item in master}
        vendor_symbols: set[str] = set()
        aliases: list[dict[str, str]] = []
        listing_differences: list[dict[str, str]] = []
        status_differences: list[dict[str, str]] = []
        for item in catalog:
            symbol, code = cls._catalog_symbol(item)
            matched = master_by_symbol.get(symbol) if symbol else None
            if matched is None and code:
                matched = master_by_code.get(code)
                if matched is not None:
                    aliases.append({"stockdb": symbol or code, "master": matched["symbol"]})
            if matched is None:
                continue
            vendor_symbols.add(matched["symbol"])
            vendor_list = str(item.get("list_date") or item.get("ipo_date") or "")[:10]
            if vendor_list and matched["list_date"] and vendor_list != matched["list_date"][:10]:
                listing_differences.append(
                    {
                        "symbol": matched["symbol"],
                        "stockdb": vendor_list,
                        "master": matched["list_date"][:10],
                    }
                )
            vendor_status = str(item.get("status") or item.get("list_status") or "").casefold()
            master_status = str(matched["status"]).casefold()
            if vendor_status and master_status and vendor_status != master_status:
                status_differences.append(
                    {
                        "symbol": matched["symbol"],
                        "stockdb": vendor_status,
                        "master": master_status,
                    }
                )
        delisted_codes = {cls._catalog_symbol(item)[1] for item in delisted if cls._catalog_symbol(item)[1]}
        delisted_suspects = sorted(
            item["symbol"]
            for code, item in master_by_code.items()
            if code in delisted_codes and not item["delist_date"]
        )
        missing = sorted(set(master_by_symbol) - vendor_symbols) if catalog else []
        coverage = {
            "upstream": "vendor-declared-unverified",
            "upstream_evidence": "not_provided",
            "distribution": "free-stockdb",
            "independent_cross_validation": False,
            "master_symbols": len(master),
            "stockdb_records": len(catalog),
            "matched_symbols": len(vendor_symbols),
            "differences": {
                "code_aliases": aliases[:200],
                "listing_dates": listing_differences[:200],
                "statuses": status_differences[:200],
                "delisted_suspects": delisted_suspects[:200],
                "catalog_missing_symbols": missing[:500],
                "counts": {
                    "code_aliases": len(aliases),
                    "listing_dates": len(listing_differences),
                    "statuses": len(status_differences),
                    "delisted_suspects": len(delisted_suspects),
                    "catalog_missing_symbols": len(missing),
                },
            },
        }
        logical = {
            "as_of_date": as_of_date,
            "artifact_id": artifact_id,
            "securities_hash": _json_hash(catalog),
            "delisted_hash": _json_hash(delisted),
            "boards_hash": _json_hash(boards),
            "master_hash": _json_hash(master),
            "coverage": coverage,
        }
        return StockDBCatalogSnapshot(
            snapshot_id="sdc_" + content_hash(logical)[:24],
            as_of_date=as_of_date,
            artifact_id=artifact_id,
            securities=tuple(catalog),
            delisted=tuple(delisted),
            boards=tuple(boards),
            coverage=coverage,
        )

    @staticmethod
    def field_contracts(
        frame: pd.DataFrame,
        as_of_date: str,
        *,
        asset_class: str,
        source: str,
        validation: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        latest = (
            frame.loc[
                pd.to_datetime(frame.get("date"), errors="coerce").dt.date == pd.Timestamp(as_of_date).date()
            ]
            if "date" in frame
            else pd.DataFrame()
        )
        validations = validation or {}
        fields = sorted(set(_FIELD_UNITS) | set(frame.columns))
        result = []
        for column in fields:
            available = column in frame
            applicable = not (asset_class == "etf" and column in {"pe_ttm", "pb", "is_st"})
            rows = int(frame[column].notna().sum()) if available else 0
            latest_rows = int(latest[column].notna().sum()) if column in latest else 0
            missing_reason = ""
            if not applicable:
                missing_reason = "not_applicable"
            elif not available:
                missing_reason = "column_missing"
            elif rows == 0:
                missing_reason = "all_null"
            elif rows < len(frame):
                missing_reason = "partial_null"
            result.append(
                StockDBFieldCoverage(
                    field=column,
                    unit=_FIELD_UNITS.get(column, "vendor_defined"),
                    asset_classes=(asset_class,),
                    source=source,
                    available=available,
                    applicable=applicable,
                    rows=rows,
                    total_rows=len(frame),
                    ratio=round(rows / len(frame), 6) if len(frame) else None,
                    latest_rows=latest_rows,
                    latest_total_rows=len(latest),
                    latest_ratio=round(latest_rows / len(latest), 6) if len(latest) else None,
                    missing_reason=missing_reason,
                    validation={
                        "semantic": _FIELD_SEMANTICS.get(
                            column,
                            "vendor-defined field; no project semantic contract is required",
                        ),
                        **dict(validations.get(column) or {}),
                    },
                ).to_dict()
            )
        return result

    def _read_batches(
        self,
        symbols: list[str],
        start: str,
        end: str,
        *,
        progress: Progress,
        cancelled: Cancelled,
        progress_start: int = 5,
        progress_span: int = 48,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        # The native SDK is optimized for broad cross-section reads.  Small
        # 300-symbol slices multiply Python/SQLite setup costs and make a full
        # A-share refresh needlessly chatty.  Keep one SDK call in flight (the
        # vendor runtime is not documented as thread-safe), but start with a
        # materially wider batch and adapt around a human-scale two-second
        # slice budget.
        offset, batch_size = 0, 1000
        target_seconds = 2.5
        while offset < len(symbols):
            if cancelled():
                raise InterruptedError("free-stockdb 摄取已取消")
            batch = symbols[offset : offset + batch_size]
            started = time.perf_counter()
            frames.append(self.source.daily_cross_section(batch, start, end))
            elapsed = time.perf_counter() - started
            self.read_metrics.append(
                {
                    "symbols": len(batch),
                    "start": start,
                    "end": end,
                    "elapsed_seconds": round(elapsed, 6),
                    "target_seconds": target_seconds,
                    "within_target": elapsed <= target_seconds,
                }
            )
            offset += len(batch)
            if elapsed > target_seconds:
                batch_size = max(500, int(batch_size * 0.80))
            elif elapsed < target_seconds * 0.55:
                batch_size = min(2000, int(batch_size * 1.25))
            progress(
                progress_start + int(progress_span * offset / max(1, len(symbols))),
                "读取本地数据库",
                f"已读取 {offset}/{len(symbols)} · 批次 {batch_size} · {elapsed:.2f}s",
            )
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def read_cross_section_history(
        self,
        symbols: list[str],
        start: str,
        end: str,
        *,
        progress: Progress,
        cancelled: Cancelled,
    ) -> pd.DataFrame:
        """Read a raw point-in-time local StockDB panel without publishing.

        This is the bounded local-read seam for display/current-analysis
        consumers.  It never performs cross-source validation and never writes
        an immutable ingest snapshot; formal research still goes through
        :meth:`load_or_create` and its stricter adjustment-factor gate.
        """

        return self._read_batches(
            list(dict.fromkeys(str(symbol).upper() for symbol in symbols)),
            start,
            end,
            progress=progress,
            cancelled=cancelled,
        )

    @staticmethod
    def cross_validation_sample(frame: pd.DataFrame) -> dict[str, Any]:
        """Pick a content-addressed, exchange/liquidity-stratified sample.

        The sample is deliberately computed from the immutable StockDB payload,
        rather than the current clock or a random seed.  Re-ingesting identical
        bytes therefore audits the same securities and sessions and can reuse
        its accepted evidence.
        """

        digest = _frame_hash(frame)
        required = {"symbol", "date"}
        if frame.empty or not required.issubset(frame):
            return {
                "content_hash": digest,
                "symbols": [],
                "trade_dates": [],
                "strata": [],
                "issues": ["StockDB 数据缺少抽检所需的 symbol/date 字段"],
            }
        value = frame.copy()
        value["symbol"] = value["symbol"].astype(str).str.upper()
        value["date"] = pd.to_datetime(value["date"], errors="coerce").dt.normalize()
        value = value.dropna(subset=["symbol", "date"])
        if value.empty:
            return {
                "content_hash": digest,
                "symbols": [],
                "trade_dates": [],
                "strata": [],
                "issues": ["StockDB 数据没有可解析的交易日"],
            }
        liquidity_column = "amount" if "amount" in value else "volume" if "volume" in value else ""
        if liquidity_column:
            value["_liquidity"] = pd.to_numeric(value[liquidity_column], errors="coerce").fillna(0.0)
        else:
            value["_liquidity"] = 0.0
        stats = value.groupby("symbol", sort=True)["_liquidity"].median().to_frame("liquidity")
        stats["exchange"] = [
            symbol.rsplit(".", 1)[-1] if "." in symbol else "UNKNOWN"
            for symbol in stats.index
        ]
        ranked = stats.sort_values(["liquidity"], kind="mergesort").index.tolist()
        buckets = {
            symbol: min(2, int(position * 3 / max(1, len(ranked))))
            for position, symbol in enumerate(ranked)
        }
        groups: dict[str, list[str]] = {}
        for symbol in stats.index:
            key = f"{stats.at[symbol, 'exchange']}:L{buckets[str(symbol)]}"
            groups.setdefault(key, []).append(str(symbol))

        def lottery(symbol: str) -> str:
            return hashlib.sha256(f"{digest}:{symbol}".encode()).hexdigest()

        for symbols in groups.values():
            symbols.sort(key=lottery)
        selected: list[str] = []
        while len(selected) < _CROSS_VALIDATION_SYMBOLS:
            added = False
            for key in sorted(groups):
                if not groups[key] or len(selected) >= _CROSS_VALIDATION_SYMBOLS:
                    continue
                selected.append(groups[key].pop(0))
                added = True
            if not added:
                break
        dates_by_symbol = {
            symbol: set(value.loc[value["symbol"] == symbol, "date"].tolist())
            for symbol in selected
        }
        common_dates = set.intersection(*dates_by_symbol.values()) if dates_by_symbol else set()
        dates = sorted(common_dates)[-_CROSS_VALIDATION_DATES:]
        strata = [
            {
                "symbol": symbol,
                "exchange": str(stats.at[symbol, "exchange"]),
                "liquidity_bucket": f"L{buckets[symbol]}",
            }
            for symbol in selected
        ]
        issues: list[str] = []
        if len(selected) < _CROSS_VALIDATION_SYMBOLS:
            issues.append(f"可抽检证券仅 {len(selected)}/{_CROSS_VALIDATION_SYMBOLS} 只")
        if len(dates) < _CROSS_VALIDATION_DATES:
            issues.append(f"样本共同交易日仅 {len(dates)}/{_CROSS_VALIDATION_DATES} 天")
        return {
            "content_hash": digest,
            "symbols": selected,
            "trade_dates": [pd.Timestamp(item).date().isoformat() for item in dates],
            "strata": strata,
            "issues": issues,
        }

    def _cross_source_validation(self, frame: pd.DataFrame) -> dict[str, Any]:
        """Validate a StockDB sample, checkpointing each usable remote response.

        Local Tushare cache is always inspected before a remote request.  Each
        remote response is normalized and range/identity checked before an
        atomic per-symbol checkpoint is published.  A batch interruption thus
        keeps earlier verified fragments and retries only missing symbols.
        """

        sample = self.cross_validation_sample(frame)
        result: dict[str, Any] = {
            "schema_version": STOCKDB_CROSS_VALIDATION_SCHEMA_VERSION,
            "content_hash": sample["content_hash"],
            "sample": {
                "symbols": sample["symbols"],
                "trade_dates": sample["trade_dates"],
                "strata": sample["strata"],
            },
            "issues": list(sample["issues"]),
            "field_checks": {},
            "cache_hits": 0,
            "remote_fetches": 0,
            "remote_requests_avoided": 0,
            "completed_items": 0,
            "reused_items": 0,
            "item_status": {},
            "reused": False,
        }
        reused = self.store.accepted_cross_validation(sample["content_hash"])
        if reused is not None:
            reused["reused"] = True
            return reused
        symbols = list(sample["symbols"])
        dates = list(sample["trade_dates"])
        if not symbols or not dates:
            result["status"] = "locally_validated"
            return result
        if not get_config().data.tushare_token:
            result["status"] = "locally_validated"
            result["issues"].append("未配置 Tushare，未执行独立抽检")
            return result
        if not remote_io_allowed():
            result["status"] = "locally_validated"
            result["issues"].append("当前上下文禁止联网，未执行独立抽检")
            return result

        from quantmaster.data.tushare_source import TushareSource

        start, end = dates[0], dates[-1]
        # The remote evidence is keyed by the exact requested exchange-date
        # window, not by a new content hash.  It remains reusable when a
        # later StockDB ingest needs the same independently observed sessions.
        request_id = f"dates-{start}-to-{end}"

        def acceptable(symbol: str, observed: pd.DataFrame) -> tuple[pd.DataFrame, str]:
            if observed is None or observed.empty:
                return pd.DataFrame(), "empty_response"
            value = observed.copy()
            value.index = pd.to_datetime(value.index, errors="coerce").normalize()
            value = value.loc[~value.index.isna()].sort_index()
            if value.index.duplicated().any():
                return pd.DataFrame(), "duplicate_trade_dates"
            required = {"open", "high", "low", "close", "volume"}
            missing = sorted(required - set(value.columns))
            if missing:
                return pd.DataFrame(), "missing_fields:" + ",".join(missing)
            expected = pd.DatetimeIndex(pd.to_datetime(dates)).normalize()
            value = value.loc[value.index.isin(expected)]
            if len(value) != len(expected):
                return pd.DataFrame(), "incomplete_trade_dates"
            numeric = value[list(required)].apply(pd.to_numeric, errors="coerce")
            if not bool(np.isfinite(numeric).all().all()):
                return pd.DataFrame(), "nonfinite_required_fields"
            prices = numeric[["open", "high", "low", "close"]]
            coherent = (
                prices.gt(0).all(axis=1)
                & numeric["volume"].ge(0)
                & numeric["high"].ge(prices[["open", "close"]].max(axis=1))
                & numeric["low"].le(prices[["open", "close"]].min(axis=1))
            )
            if not bool(coherent.all()):
                return pd.DataFrame(), "invalid_ohlcv_semantics"
            value.index.name = "date"
            return value, ""

        def fetch(symbol: str) -> tuple[str, str, pd.DataFrame]:
            source = TushareSource()
            cached = source.cached_daily(symbol, start, end)
            if cached is not None and not cached.empty:
                return symbol, "cache", cached
            return symbol, "remote", source.daily(symbol, start, end)

        fetched: dict[str, pd.DataFrame] = {}
        failures: dict[str, str] = {}
        pending: list[str] = []
        for symbol in symbols:
            item = self.store.cross_validation_item(request_id, symbol)
            if item and item.get("status") == "complete":
                checkpointed = self.store.load_cross_validation_frame(request_id, symbol)
                checked, reason = acceptable(symbol, checkpointed)
                if not checked.empty:
                    fetched[symbol] = checked
                    result["reused_items"] += 1
                    result["remote_requests_avoided"] += 1
                    result["item_status"][symbol] = "checkpoint"
                    continue
                # A corrupt/incomplete checkpoint is never trusted as complete.
                self.store.persist_cross_validation_item(
                    request_id, symbol,
                    {"status": "retry", "reason": f"checkpoint_{reason}"},
                )
            pending.append(symbol)

        # At most two independent provider calls run at once.  Completion is
        # persisted in this loop, rather than after all futures succeed.
        with ThreadPoolExecutor(
            max_workers=min(2, max(1, len(pending))),
            thread_name_prefix="stockdb-audit",
        ) as pool:
            futures = {pool.submit(fetch, symbol): symbol for symbol in pending}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    observed_symbol, origin, remote = future.result()
                    checked, reason = acceptable(observed_symbol, remote)
                    if checked.empty:
                        raise RuntimeError(reason or "Tushare 抽检响应为空")
                    fetched[observed_symbol] = checked
                    if origin == "cache":
                        result["cache_hits"] += 1
                        result["remote_requests_avoided"] += 1
                    else:
                        result["remote_fetches"] += 1
                    self.store.persist_cross_validation_item(
                        request_id, observed_symbol,
                        {"status": "complete", "origin": origin, "start": start, "end": end},
                        frame=checked,
                    )
                    result["completed_items"] += 1
                    result["item_status"][observed_symbol] = origin
                except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
                    reason = str(exc)[:240] or type(exc).__name__
                    failures[symbol] = reason
                    self.store.persist_cross_validation_item(
                        request_id, symbol,
                        {"status": "retry", "reason": reason, "start": start, "end": end},
                    )
                    result["item_status"][symbol] = "retry"
        if failures:
            result["status"] = "locally_validated"
            result["failures"] = failures
            result["issues"].append(
                f"{len(failures)} 个抽检证券未完成独立证据；已保存其余 {len(fetched)} 项检查点"
            )
            return result

        local = frame.copy()
        local["symbol"] = local["symbol"].astype(str).str.upper()
        local["date"] = pd.to_datetime(local["date"], errors="coerce").dt.normalize()
        local = local.loc[
            local["symbol"].isin(symbols) & local["date"].isin(pd.to_datetime(dates))
        ]
        totals: dict[str, dict[str, int]] = {}
        mismatches: list[str] = []
        for symbol in symbols:
            left = local.loc[local["symbol"] == symbol].set_index("date").sort_index()
            right = fetched[symbol].copy()
            joined = left.join(right, how="inner", lsuffix="_stockdb", rsuffix="_tushare")
            if len(joined) != len(dates):
                mismatches.append(f"{symbol}: 共同抽检交易日不完整")
                continue
            for field in ("open", "high", "low", "close", "volume", "amount"):
                left_name, right_name = f"{field}_stockdb", f"{field}_tushare"
                if left_name not in joined or right_name not in joined:
                    if field in {"open", "high", "low", "close", "volume"}:
                        mismatches.append(f"{symbol}: 缺少抽检字段 {field}")
                    continue
                left_values = pd.to_numeric(joined[left_name], errors="coerce")
                right_values = pd.to_numeric(joined[right_name], errors="coerce")
                finite = np.isfinite(left_values) & np.isfinite(right_values)
                if not bool(finite.all()):
                    mismatches.append(f"{symbol}: {field} 存在非有限抽检值")
                    continue
                if field in {"open", "high", "low", "close"}:
                    positive = left_values.gt(0) & right_values.gt(0)
                    if not bool(positive.all()):
                        mismatches.append(f"{symbol}: {field} 存在非正抽检价格")
                        continue
                    scale = float((right_values / left_values).median())
                    matched = (left_values - right_values / scale).abs().le(
                        left_values.abs().clip(lower=1e-12) * _CROSS_PRICE_TOLERANCE,
                    )
                    threshold = 0.80
                else:
                    matched = (left_values - right_values).abs().le(
                        right_values.abs().clip(lower=1e-12) * _CROSS_UNIT_TOLERANCE,
                    )
                    threshold = 0.99
                stat = totals.setdefault(field, {"rows": 0, "matched": 0, "required": 0})
                stat["rows"] += len(matched)
                stat["matched"] += int(matched.sum())
                stat["required"] = 1
                if float(matched.mean()) < threshold:
                    mismatches.append(f"{symbol}: {field} 与 Tushare 抽检不一致")
        result["field_checks"] = {
            field: {
                "rows": stat["rows"],
                "matching_ratio": round(stat["matched"] / max(1, stat["rows"]), 6),
                "tolerance": (
                    _CROSS_PRICE_TOLERANCE
                    if field in {"open", "high", "low", "close"}
                    else _CROSS_UNIT_TOLERANCE
                ),
            }
            for field, stat in sorted(totals.items())
        }
        if mismatches:
            result["status"] = "rejected"
            result["issues"].extend(mismatches[:100])
        else:
            result["status"] = "verified"
        return result

    @staticmethod
    def _broad_sessions(frame: pd.DataFrame, expected_symbols: int) -> list[str]:
        if frame.empty or "date" not in frame or "symbol" not in frame:
            return []
        value = frame[["date", "symbol"]].copy()
        value["date"] = pd.to_datetime(value["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        counts = value.dropna().drop_duplicates().groupby("date")["symbol"].nunique()
        minimum = max(1, min(100, math.ceil(expected_symbols * 0.50)))
        return sorted(str(item) for item in counts[counts >= minimum].index)

    @staticmethod
    def _official_sessions(start: str, end: str) -> list[str]:
        if not get_config().data.tushare_token:
            return []
        try:
            from quantmaster.data.tushare_source import TushareSource

            return [str(value.date()) for value in TushareSource().trade_calendar(start, end)]
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            return []

    @staticmethod
    def _data_session(requested_end: str) -> str:
        try:
            from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

            status = free_stockdb_runtime.status()
            session = str(status.get("validated_session") or status.get("actual_session") or "")
            generation = content_hash(
                {
                    "session": session,
                    "validation": status.get("validation") or {},
                    "update_result": status.get("update_result"),
                    "exit_code": status.get("exit_code"),
                }
            )[:12]
            return f"{session or requested_end}:{generation}"
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            return requested_end

    def load_or_create(
        self,
        *,
        instruments: list[Any],
        start: str,
        end: str,
        validator: Validator,
        progress: Progress,
        cancelled: Cancelled,
    ) -> tuple[StockDBIngestSnapshot, pd.DataFrame, list[dict[str, Any]], bool]:
        ingest_started = time.perf_counter()
        self.read_metrics = []
        symbols = [str(item.symbol).upper() for item in instruments]
        master_id = self.master_snapshot_id(instruments)
        data_session = self._data_session(end)
        identity = getattr(self.source, "artifact_identity", None)
        boards = self.source.board_hierarchy()
        catalog: list[dict[str, Any]] = []
        delisted: list[dict[str, Any]] = []
        catalog_issue = ""
        try:
            catalog_reader = getattr(self.source, "security_catalog", None)
            delisted_reader = getattr(self.source, "delisted_catalog", None)
            catalog = catalog_reader() if callable(catalog_reader) else []
            delisted = delisted_reader() if callable(delisted_reader) else []
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            catalog_issue = f"证券/退市目录未能归档：{str(exc)[:160]}"
        board_hash = _json_hash(boards)
        catalog_hash = _json_hash({"securities": catalog, "delisted": delisted})
        artifact = (
            identity(
                data_session=data_session,
                board_hash=board_hash,
                catalog_hash=catalog_hash,
            )
            if callable(identity)
            else StockDBArtifactIdentity.discover(
                None,
                None,
                data_session=data_session,
                board_hash=board_hash,
                catalog_hash=catalog_hash,
            )
        )
        cfg = get_config().data
        history_sessions = max(21, int(cfg.free_stockdb_stock_history_sessions))
        end_stamp = pd.Timestamp(end).normalize()
        initial_days = max(180, int(cfg.free_stockdb_stock_initial_lookback_days))
        max_days = max(initial_days, int(cfg.free_stockdb_stock_max_lookback_days))
        requested_start = min(pd.Timestamp(start).normalize(), end_stamp - pd.Timedelta(days=initial_days))
        hard_start = end_stamp - pd.Timedelta(days=max_days)
        cache_key = content_hash(
            {
                "schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                "artifact_id": artifact.artifact_id,
                "master_snapshot_id": master_id,
                "start": requested_start.date().isoformat(),
                "end": end,
                "symbols": symbols,
                "history_sessions": history_sessions,
            }
        )
        # Test/dynamic adapters may mutate their in-memory payload without a
        # vendor artifact generation.  Only the concrete runtime adapter can
        # safely promise cache identity.
        reusable = self.store.find(cache_key) if type(self.source) is FreeStockDBSource else None
        if reusable is not None:
            frame = self.store.load_frame(reusable)
            adjustment = self.store.load_frame(reusable, "stock_adjustment_factors")
            boards = self.store.load_json(reusable, "boards")
            evidence = reusable.coverage.get("cross_source_validation")
            if (
                not frame.empty
                and boards
                and len(reusable.session_dates) >= history_sessions
                and isinstance(evidence, dict)
                and evidence.get("status") in {"verified", "locally_validated"}
            ):
                progress(55, "复用本地摄取", reusable.ingest_id)
                return reusable, self._research_prices(frame, adjustment), boards, True

        frame = self._read_batches(
            symbols,
            requested_start.date().isoformat(),
            end,
            progress=progress,
            cancelled=cancelled,
        )
        broad_sessions = self._broad_sessions(frame, len(symbols))
        while len(broad_sessions) < history_sessions and requested_start > hard_start:
            previous_start = requested_start
            requested_start = max(hard_start, requested_start - pd.Timedelta(days=90))
            older_end = previous_start - pd.Timedelta(days=1)
            progress(
                54,
                "扩展历史窗口",
                f"仅有 {len(broad_sessions)}/{history_sessions} 个交易日，"
                f"向前补读至 {requested_start.date()}",
            )
            older = self._read_batches(
                symbols,
                requested_start.date().isoformat(),
                older_end.date().isoformat(),
                progress=progress,
                cancelled=cancelled,
                progress_start=5,
                progress_span=48,
            )
            if not older.empty:
                frame = pd.concat((older, frame), ignore_index=True)
                if {"symbol", "date"}.issubset(frame.columns):
                    frame = frame.drop_duplicates(["symbol", "date"], keep="last")
            broad_sessions = self._broad_sessions(frame, len(symbols))
        official_sessions = self._official_sessions(
            requested_start.date().isoformat(),
            end_stamp.date().isoformat(),
        )
        session_source = "tushare:SSE" if official_sessions else "stockdb_broad_coverage"
        calendar_issues = [] if official_sessions else [
            "缺少独立权威交易日历；StockDB 广泛覆盖日期不能证明全市场未漏整日"
        ]
        available_dates = (
            set(pd.to_datetime(frame.get("date"), errors="coerce").dropna().dt.strftime("%Y-%m-%d"))
            if "date" in frame
            else set()
        )
        expected_session_dates = list(official_sessions or broad_sessions)[-history_sessions:]
        observed_session_dates = [
            value for value in expected_session_dates if value in available_dates
        ]
        if (
            len(expected_session_dates) < history_sessions
            or len(observed_session_dates) < len(expected_session_dates)
        ):
            missing_sessions = [
                value for value in expected_session_dates if value not in available_dates
            ]
            coverage = {
                "status": "rejected",
                "required_history_sessions": history_sessions,
                "expected_session_dates": expected_session_dates,
                "observed_session_dates": observed_session_dates,
                "missing_session_dates": missing_sessions,
                "observed_history_sessions": len(observed_session_dates),
                "history_window_start": requested_start.date().isoformat(),
                "history_window_end": end_stamp.date().isoformat(),
                "session_source": session_source,
            }
            raise StockDBIngestRejected(
                [
                    "A 股广泛覆盖摄取只有 "
                    f"{len(observed_session_dates)}/{history_sessions} 个权威预期交易日"
                ],
                coverage,
                observed_session_dates[-1] if observed_session_dates else "",
            )
        session_dates = expected_session_dates
        frame_dates = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        frame = frame.loc[frame_dates.isin(session_dates)].copy()
        as_of_date, coverage = validator(frame, boards, len(symbols))
        coverage.update(
            {
                "required_history_sessions": history_sessions,
                "observed_history_sessions": len(session_dates),
                "session_dates": session_dates,
                "observed_session_dates": observed_session_dates,
                "session_source": session_source,
                "calendar_status": "verified" if official_sessions else "degraded",
                "history_window_start": session_dates[0],
                "history_window_end": session_dates[-1],
            }
        )
        coverage["performance"] = {
            "cache_hit": False,
            "read_fragments": self.read_metrics,
            "read_elapsed_seconds": round(
                sum(float(item["elapsed_seconds"]) for item in self.read_metrics), 6
            ),
            "ingest_elapsed_seconds": round(time.perf_counter() - ingest_started, 6),
            "fragment_target_seconds": 1.0,
        }
        coverage["fields"] = self.field_contracts(
            frame,
            as_of_date,
            asset_class="stock",
            source=self.source.name,
            validation=coverage.get("consistency") or {},
        )
        cross_validation = self._cross_source_validation(frame)
        coverage["cross_source_validation"] = cross_validation
        coverage["acceptance"] = {
            "formal_allowed": False,
            "preview_allowed": True,
            "reason": "复权因子完整性尚未逐标的确认，结果仅可预览",
        }
        if catalog_issue:
            coverage.setdefault("issues_non_blocking", []).append(catalog_issue)
        adjustment = pd.DataFrame(columns=["symbol", "date", "adj_factor"])
        factor_reader = getattr(self.source, "adjustment_factors", None)
        if callable(factor_reader):
            try:
                adjustment = factor_reader(symbols, session_dates[0], end)
                covered_factor_symbols = (
                    set(adjustment["symbol"].astype(str)) if not adjustment.empty else set()
                )
                missing_factor_symbols = sorted(set(symbols) - covered_factor_symbols)
                coverage["adjustment_factor_symbols"] = len(covered_factor_symbols)
                coverage["adjustment_factor_missing_symbols"] = missing_factor_symbols
                if missing_factor_symbols:
                    coverage["price_adjustment_status"] = "degraded"
                    coverage.setdefault("issues_non_blocking", []).append(
                        f"{len(missing_factor_symbols)} 只证券缺少可验证复权因子；正式研究已停止"
                    )
                else:
                    coverage["price_adjustment_status"] = "verified"
                    coverage["acceptance"] = {
                        "formal_allowed": bool(
                            cross_validation.get("status") == "verified" and not calendar_issues
                        ),
                        "preview_allowed": True,
                        "reason": "" if (
                            cross_validation.get("status") == "verified" and not calendar_issues
                        ) else "整批独立抽检或交易日历证据尚未完成，结果仅可预览",
                    }
            except (OSError, RuntimeError, TypeError, ValueError) as exc:
                coverage["price_adjustment_status"] = "degraded"
                coverage["adjustment_factor_missing_symbols"] = list(symbols)
                coverage.setdefault("issues_non_blocking", []).append(f"复权因子未能归档：{str(exc)[:160]}")
        catalog_snapshot = self.catalog_snapshot(
            catalog=catalog,
            delisted=delisted,
            boards=boards,
            instruments=instruments,
            as_of_date=as_of_date,
            artifact_id=artifact.artifact_id,
        )
        coverage["catalog"] = catalog_snapshot.coverage
        cross_status = str(cross_validation.get("status") or "locally_validated")
        acceptance_issues = [*calendar_issues]
        if cross_status != "verified":
            acceptance_issues.append(
                "StockDB 整批独立抽检未通过正式资格：" + cross_status
            )
        snapshot = self.store.publish(
            frame=frame,
            adjustment=adjustment,
            boards=boards,
            catalog=catalog,
            delisted=delisted,
            as_of_date=as_of_date,
            artifact_id=artifact.artifact_id,
            master_snapshot_id=master_id,
            start_date=session_dates[0],
            end_date=end,
            coverage=coverage,
            provenance={
                "source": self.source.name,
                "cache_key": cache_key,
                "ingest_schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                "upstream": "vendor-declared-unverified",
                "upstream_evidence": "not_provided",
                "distribution": "free-stockdb",
                "artifact": artifact.to_dict(),
                "sdk_version": (
                    self.source.sdk_version() if callable(getattr(self.source, "sdk_version", None)) else ""
                ),
                "field_sources": {column: self.source.name for column in frame.columns},
                "price_storage": "raw",
                "research_price_formula": "raw_price*adj_factor/latest_factor@as_of:v1",
            },
            catalog_snapshot=catalog_snapshot,
            session_dates=session_dates,
            session_source=session_source,
            status=(
                "quarantined" if cross_status == "rejected"
                else "degraded" if acceptance_issues else "complete"
            ),
            issues=acceptance_issues,
        )
        if cross_status == "rejected":
            raise StockDBIngestRejected(
                ["StockDB 整批抽检发现严重跨源不一致，已隔离该代次"],
                coverage,
                as_of_date,
            )
        return snapshot, self._research_prices(frame, adjustment), boards, False

    @staticmethod
    def _research_prices(frame: pd.DataFrame, adjustment: pd.DataFrame) -> pd.DataFrame:
        """Derive stable qfq research prices while leaving the archived frame raw."""
        if frame.empty:
            return frame
        if adjustment.empty:
            raise ValueError("正式研究缺少完整复权因子链")
        factors = adjustment[["symbol", "date", "adj_factor"]].copy()
        factors["date"] = pd.to_datetime(factors["date"], errors="coerce")
        factors["adj_factor"] = pd.to_numeric(factors["adj_factor"], errors="coerce")
        factors = factors.dropna().sort_values(["date", "symbol"], kind="mergesort")
        duplicate = factors.duplicated(["symbol", "date"], keep=False)
        if duplicate.any():
            conflicts = (
                factors.loc[duplicate]
                .groupby(["symbol", "date"])["adj_factor"]
                .nunique()
            )
            if bool(conflicts.gt(1).any()):
                raise ValueError("复权因子存在同证券同日冲突值")
            factors = factors.drop_duplicates(["symbol", "date"], keep="last")
        if factors.empty:
            raise ValueError("正式研究复权因子链没有有效记录")
        value = frame.copy()
        value["date"] = pd.to_datetime(value["date"], errors="coerce")
        value = pd.merge_asof(
            value.sort_values(["date", "symbol"]),
            factors,
            on="date",
            by="symbol",
            direction="backward",
        )
        latest = factors.groupby("symbol", sort=False)["adj_factor"].last()
        denominator = value["symbol"].map(latest)
        verified = (
            value["adj_factor"].notna()
            & value["adj_factor"].gt(0)
            & denominator.notna()
            & denominator.gt(0)
        )
        missing_symbols = sorted(set(value.loc[~verified, "symbol"].astype(str)))
        if missing_symbols:
            raise ValueError(
                f"正式研究复权因子链不完整：{len(missing_symbols)} 只证券缺口；"
                + "、".join(missing_symbols[:10])
            )
        scale = value["adj_factor"].div(denominator).replace([np.inf, -np.inf], np.nan)
        for column in ("open", "high", "low", "close", "pre_close"):
            if column in value:
                raw = pd.to_numeric(value[column], errors="coerce")
                value[column] = raw.where(~verified, raw * scale)
        value["price_adjustment"] = "forward_adjusted_from_frozen_factor_v1"
        value["adjustment_status"] = "verified"
        return value.drop(columns=["adj_factor"]).sort_values(["symbol", "date"]).reset_index(drop=True)
