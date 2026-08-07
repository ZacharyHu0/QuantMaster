"""Immutable, reusable ingestion snapshots over a user-managed free-stockdb."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.free_stockdb_contracts import (
    StockDBArtifactIdentity,
    StockDBIngestSnapshot,
)
from quantmaster.data.free_stockdb_source import FreeStockDBSource
from quantmaster.research.contracts import content_hash

Progress = Callable[[int, str, str], None]
Cancelled = Callable[[], bool]
Validator = Callable[[pd.DataFrame, list[dict[str, Any]], int], tuple[str, dict[str, Any]]]
STOCKDB_INGEST_SCHEMA_VERSION = "2.0"


def _frame_hash(frame: pd.DataFrame) -> str:
    if frame.empty:
        return hashlib.sha256(b"empty").hexdigest()
    value = frame.copy()
    for column in value:
        if pd.api.types.is_datetime64_any_dtype(value[column]):
            value[column] = pd.to_datetime(value[column], errors="coerce").dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
    sort = [column for column in ("symbol", "date", "event_time_utc") if column in value]
    if sort:
        value = value.sort_values(sort, kind="mergesort").reset_index(drop=True)
    digest = hashlib.sha256()
    digest.update("\x1f".join(str(column) for column in value.columns).encode())
    digest.update(pd.util.hash_pandas_object(value, index=False, categorize=True).to_numpy(
        dtype="uint64", copy=False,
    ).tobytes())
    return digest.hexdigest()


def _json_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str,
    ).encode()).hexdigest()


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

    def history(self, limit: int = 100) -> list[StockDBIngestSnapshot]:
        records: list[StockDBIngestSnapshot] = []
        for path in sorted(
            self.manifests.glob("*.json"),
            key=lambda item: item.stat().st_mtime_ns,
            reverse=True,
        ):
            try:
                records.append(StockDBIngestSnapshot.from_dict(json.loads(
                    path.read_text(encoding="utf-8")
                )))
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
        return next((
            item for item in self.history(self.retain + 20)
            if item.provenance.get("cache_key") == cache_key and item.status == "complete"
        ), None)

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
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"), default=str)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        target = self.content / f"{digest}.json"
        if not target.is_file():
            _atomic_text(target, encoded)
        return digest

    def publish(
        self, *, frame: pd.DataFrame, adjustment: pd.DataFrame | None = None,
        boards: list[dict[str, Any]],
        catalog: list[dict[str, Any]], delisted: list[dict[str, Any]],
        as_of_date: str, artifact_id: str, master_snapshot_id: str,
        start_date: str, end_date: str, coverage: dict[str, Any],
        provenance: dict[str, Any], assets: dict[str, Any] | None = None,
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
            logical = {
                "schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                "as_of_date": as_of_date, "artifact_id": artifact_id,
                "master_snapshot_id": master_snapshot_id, "start_date": start_date,
                "end_date": end_date, "content_hashes": hashes,
                "coverage": coverage, "provenance": provenance,
            }
            ingest_id = "sdi_" + content_hash(logical)[:24]
            snapshot = StockDBIngestSnapshot(
                ingest_id=ingest_id, as_of_date=as_of_date, artifact_id=artifact_id,
                master_snapshot_id=master_snapshot_id, start_date=start_date,
                end_date=end_date, assets=assets or {"stock": {"rows": len(frame)}},
                coverage=coverage, content_hashes=hashes, provenance=provenance,
            )
            path = self.manifests / f"{ingest_id}.json"
            encoded = json.dumps(snapshot.to_dict(), ensure_ascii=False, sort_keys=True,
                                 indent=2, default=str)
            if path.exists():
                existing = self.get(ingest_id)
                if existing is None or existing.content_hashes != hashes:
                    raise RuntimeError(f"free-stockdb 摄取清单不可变: {ingest_id}")
                return existing
            _atomic_text(path, encoded)
            self.prune()
            return snapshot

    def publish_etf(
        self, *, daily: pd.DataFrame, minutes: pd.DataFrame,
        profiles: list[dict[str, Any]], as_of_date: str, artifact_id: str,
        master_snapshot_id: str, start_date: str, end_date: str,
        coverage: dict[str, Any], provenance: dict[str, Any],
    ) -> StockDBIngestSnapshot:
        with self._lock:
            hashes = {
                "etf_daily": self._write_frame(daily),
                "etf_minutes": self._write_frame(minutes),
                "etf_profiles": self._write_json(profiles),
            }
            logical = {
                "schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                "asset": "etf", "as_of_date": as_of_date,
                "artifact_id": artifact_id, "master_snapshot_id": master_snapshot_id,
                "start_date": start_date, "end_date": end_date,
                "content_hashes": hashes, "coverage": coverage,
            }
            ingest_id = "sdi_" + content_hash(logical)[:24]
            snapshot = StockDBIngestSnapshot(
                ingest_id=ingest_id, as_of_date=as_of_date, artifact_id=artifact_id,
                master_snapshot_id=master_snapshot_id, start_date=start_date,
                end_date=end_date, assets={
                    "etf": {"daily_rows": len(daily), "minute_rows": len(minutes),
                            "symbols": len(profiles)},
                }, coverage=coverage, content_hashes=hashes, provenance=provenance,
            )
            path = self.manifests / f"{ingest_id}.json"
            encoded = json.dumps(snapshot.to_dict(), ensure_ascii=False, sort_keys=True,
                                 indent=2, default=str)
            if path.exists():
                existing = self.get(ingest_id)
                if existing is None or existing.content_hashes != hashes:
                    raise RuntimeError(f"free-stockdb ETF 摄取清单不可变: {ingest_id}")
                return existing
            _atomic_text(path, encoded)
            self.prune()
            return snapshot

    def prune(self) -> None:
        manifests = sorted(
            self.manifests.glob("*.json"),
            key=lambda item: item.stat().st_mtime_ns,
            reverse=True,
        )
        for path in manifests[self.retain:]:
            path.unlink(missing_ok=True)
        referenced: set[str] = set()
        for item in self.history(self.retain):
            referenced.update(item.content_hashes.values())
        for path in self.content.glob("*"):
            if path.stem not in referenced:
                path.unlink(missing_ok=True)


class StockDBIngestService:
    def __init__(
        self, source: FreeStockDBSource | None = None,
        store: StockDBIngestStore | None = None,
    ):
        self.source = source or FreeStockDBSource()
        self.store = store or StockDBIngestStore()

    @staticmethod
    def master_snapshot_id(instruments: Iterable[Any]) -> str:
        records = [{
            "symbol": str(item.symbol), "status": str(item.status),
            "list_date": str(item.list_date), "delist_date": str(item.delist_date),
            "asset_type": str(item.asset_type), "source": str(item.source),
        } for item in instruments]
        return "master_" + content_hash(sorted(records, key=lambda item: item["symbol"]))[:24]

    @staticmethod
    def _data_session(requested_end: str) -> str:
        try:
            from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

            status = free_stockdb_runtime.status()
            session = str(status.get("validated_session") or status.get("actual_session") or "")
            generation = content_hash({
                "session": session, "validation": status.get("validation") or {},
                "update_result": status.get("update_result"),
                "exit_code": status.get("exit_code"),
            })[:12]
            return f"{session or requested_end}:{generation}"
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            return requested_end

    def load_or_create(
        self, *, instruments: list[Any], start: str, end: str,
        validator: Validator, progress: Progress, cancelled: Cancelled,
    ) -> tuple[StockDBIngestSnapshot, pd.DataFrame, list[dict[str, Any]], bool]:
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
                data_session=data_session, board_hash=board_hash,
                catalog_hash=catalog_hash,
            ) if callable(identity)
            else StockDBArtifactIdentity.discover(
                None, None, data_session=data_session, board_hash=board_hash,
                catalog_hash=catalog_hash,
            )
        )
        cache_key = content_hash({
            "schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
            "artifact_id": artifact.artifact_id, "master_snapshot_id": master_id,
            "start": start, "end": end, "symbols": symbols,
        })
        # Test/dynamic adapters may mutate their in-memory payload without a
        # vendor artifact generation.  Only the concrete runtime adapter can
        # safely promise cache identity.
        reusable = self.store.find(cache_key) if type(self.source) is FreeStockDBSource else None
        if reusable is not None:
            frame = self.store.load_frame(reusable)
            adjustment = self.store.load_frame(reusable, "stock_adjustment_factors")
            boards = self.store.load_json(reusable, "boards")
            if not frame.empty and boards:
                progress(55, "复用本地摄取", reusable.ingest_id)
                return reusable, self._research_prices(frame, adjustment), boards, True

        frames: list[pd.DataFrame] = []
        offset, batch_size = 0, 300
        while offset < len(symbols):
            if cancelled():
                raise InterruptedError("free-stockdb 摄取已取消")
            batch = symbols[offset:offset + batch_size]
            started = time.perf_counter()
            frames.append(self.source.daily_cross_section(batch, start, end))
            elapsed = time.perf_counter() - started
            offset += len(batch)
            if elapsed > 1.0:
                batch_size = max(100, int(batch_size * 0.75))
            elif elapsed < 0.4:
                batch_size = min(500, int(batch_size * 1.2))
            progress(
                5 + int(48 * offset / max(1, len(symbols))), "读取本地数据库",
                f"已读取 {offset}/{len(symbols)} · 下一批 {batch_size}",
            )
        frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        as_of_date, coverage = validator(frame, boards, len(symbols))
        if catalog_issue:
            coverage.setdefault("issues_non_blocking", []).append(catalog_issue)
        adjustment = pd.DataFrame(columns=["symbol", "date", "adj_factor"])
        factor_reader = getattr(self.source, "adjustment_factors", None)
        if callable(factor_reader):
            try:
                adjustment = factor_reader(symbols, start, end)
                coverage["adjustment_factor_symbols"] = int(
                    adjustment["symbol"].nunique() if not adjustment.empty else 0
                )
            except (OSError, RuntimeError, TypeError, ValueError) as exc:
                coverage.setdefault("issues_non_blocking", []).append(
                    f"复权因子未能归档：{str(exc)[:160]}"
                )
        snapshot = self.store.publish(
            frame=frame, adjustment=adjustment, boards=boards,
            catalog=catalog, delisted=delisted,
            as_of_date=as_of_date, artifact_id=artifact.artifact_id,
            master_snapshot_id=master_id, start_date=start, end_date=end,
            coverage=coverage, provenance={
                "source": self.source.name, "cache_key": cache_key,
                "ingest_schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                "upstream": "tushare", "distribution": "free-stockdb",
                "artifact": artifact.to_dict(),
                "sdk_version": (
                    self.source.sdk_version() if callable(getattr(self.source, "sdk_version", None))
                    else ""
                ),
                "field_sources": {column: self.source.name for column in frame.columns},
                "price_storage": "raw",
                "research_price_formula": "raw_price*adj_factor/latest_factor@as_of:v1",
            },
        )
        return snapshot, self._research_prices(frame, adjustment), boards, False

    @staticmethod
    def _research_prices(frame: pd.DataFrame, adjustment: pd.DataFrame) -> pd.DataFrame:
        """Derive stable qfq research prices while leaving the archived frame raw."""
        if frame.empty or adjustment.empty:
            return frame
        factors = adjustment[["symbol", "date", "adj_factor"]].copy()
        factors["date"] = pd.to_datetime(factors["date"], errors="coerce")
        factors["adj_factor"] = pd.to_numeric(factors["adj_factor"], errors="coerce")
        factors = factors.dropna().sort_values(["date", "symbol"])
        if factors.empty:
            return frame
        value = frame.copy()
        value["date"] = pd.to_datetime(value["date"], errors="coerce")
        value = pd.merge_asof(
            value.sort_values(["date", "symbol"]), factors,
            on="date", by="symbol", direction="backward",
        )
        latest = factors.groupby("symbol", sort=False)["adj_factor"].last()
        denominator = value["symbol"].map(latest)
        scale = value["adj_factor"].fillna(1.0).div(denominator).replace([np.inf, -np.inf], np.nan)
        scale = scale.fillna(1.0)
        for column in ("open", "high", "low", "close", "pre_close"):
            if column in value:
                value[column] = pd.to_numeric(value[column], errors="coerce") * scale
        value["price_adjustment"] = "qfq_from_frozen_factor_v1"
        return value.drop(columns=["adj_factor"]).sort_values(["symbol", "date"]).reset_index(drop=True)
