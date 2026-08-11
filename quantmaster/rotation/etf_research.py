"""All-exchange ETF research backed by local Tushare-distributed stockdb data."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import threading
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.free_stockdb_contracts import StockDBIngestSnapshot
from quantmaster.data.free_stockdb_ingest import (
    STOCKDB_INGEST_SCHEMA_VERSION,
    StockDBIngestService,
    StockDBIngestStore,
)
from quantmaster.data.free_stockdb_ingest import _frame_hash as _stockdb_frame_hash
from quantmaster.data.free_stockdb_source import FreeStockDBSource
from quantmaster.data.instruments import Instrument, InstrumentStore
from quantmaster.data.resilience import PROVIDER_HEALTH
from quantmaster.research.contracts import content_hash
from quantmaster.rotation.etf_models import (
    ETF_RESEARCH_MODEL_VERSION,
    ETF_SCHEMA_VERSION,
    EtfProfile,
    EtfResearchItem,
    EtfResearchSnapshot,
)
from quantmaster.rotation.etf_v2 import (
    ETF_CATEGORIES,
    adjusted_daily_metrics,
    build_sector_research,
    classify_etf_profile,
    fund_evidence,
)
from quantmaster.runtime.paths import confined_path
from quantmaster.trading_sessions import (
    daily_signal_cutoff,
    market_date,
    market_now,
    resolve_session_target,
)

Progress = Callable[[int, str, str], None]
Cancelled = Callable[[], bool]
EtfResearchTier = Literal["production", "sandbox"]
_ADJUSTMENT_COLUMNS = ("symbol", "date", "adj_factor", "source", "acquired_at")
ETF_DIRECTORY_ATTESTATION_VERSION = "1.0"
ETF_DIRECTORY_TRUSTED_SOURCE = "tushare:catalog"
_PRODUCTION_SNAPSHOT_ID = re.compile(r"etf_[0-9a-f]{24}")
_EXCHANGE_ETF_SYMBOL = re.compile(r"[0-9]{6}\.(?:SH|SZ)")


def _clean_scalar_text(*candidates: Any) -> str:
    """Return the first real scalar string without serializing pandas null sentinels."""

    for candidate in candidates:
        if candidate is None:
            continue
        try:
            if bool(pd.isna(candidate)):
                continue
        except (TypeError, ValueError):
            pass
        value = str(candidate).strip()
        if value and value.casefold() not in {"nan", "nat", "none", "<na>"}:
            return value
    return ""


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(fd)
    temp = Path(temp_name)
    try:
        temp.write_text(value, encoding="utf-8")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def classify_etf(
    name: str,
    *,
    benchmark: str = "",
    fund_type: str = "",
    invest_type: str = "",
) -> tuple[str, tuple[str, ...]]:
    taxonomy = classify_etf_profile(
        name,
        benchmark=benchmark,
        fund_type=fund_type,
        invest_type=invest_type,
    )
    return taxonomy["category"], taxonomy["classification_evidence"]


def _frame_hash(frame: pd.DataFrame, columns: tuple[str, ...]) -> str:
    if frame is None or frame.empty:
        return content_hash([])
    selected = frame[[column for column in columns if column in frame]].copy()
    for column in selected:
        if pd.api.types.is_datetime64_any_dtype(selected[column]):
            selected[column] = selected[column].map(
                lambda value: value.isoformat() if pd.notna(value) else ""
            )
    selected = selected.sort_values(list(selected.columns)).reset_index(drop=True)
    hashed = pd.util.hash_pandas_object(selected, index=False).values.tobytes()
    return hashlib.sha256(hashed).hexdigest()


def etf_directory_master_hash(frame: pd.DataFrame) -> str:
    """Hash the trusted master identity independently from enriched ETF metadata."""

    required = {
        "symbol",
        "exchange",
        "asset_type",
        "status",
        "list_date",
        "delist_date",
        "directory_member_source",
        "directory_member_observed_at",
        "effective_date",
        "directory_source",
        "directory_acquired_at",
        "directory_cutoff_at",
        "directory_master_record_count",
        "directory_master_batch_record_count",
        "directory_master_snapshot_sha256",
        "directory_catalog_snapshot_id",
        "directory_catalog_records_sha256",
        "directory_catalog_file_sha256",
        "directory_catalog_file_size",
        "directory_catalog_file_mtime_ns",
        "directory_catalog_relative_path",
        "directory_catalog_as_of",
        "directory_catalog_expected_count",
        "directory_expected_symbols",
        "directory_observed_symbols",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("ETF 目录主表哈希缺少字段: " + ", ".join(missing))

    def one_text(column: str) -> str:
        values = {
            str(value).strip()
            for value in frame[column].dropna()
            if str(value).strip() and str(value).strip().casefold() != "nan"
        }
        if len(values) != 1:
            raise ValueError(f"ETF 目录主表字段 {column} 不是单一批次")
        return next(iter(values))

    def utc_text(value: Any) -> str:
        parsed = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(parsed):
            raise ValueError("ETF 目录主表时间缺失或不合法")
        return pd.Timestamp(parsed).isoformat()

    effective_date = pd.Timestamp(one_text("effective_date")).date().isoformat()
    source = one_text("directory_source")
    acquired_at = utc_text(one_text("directory_acquired_at"))
    cutoff_at = utc_text(one_text("directory_cutoff_at"))
    master_record_count = int(float(one_text("directory_master_record_count")))
    master_batch_record_count = int(
        float(one_text("directory_master_batch_record_count"))
    )
    master_snapshot_sha256 = one_text("directory_master_snapshot_sha256")
    catalog_snapshot_id = one_text("directory_catalog_snapshot_id")
    catalog_records_sha256 = one_text("directory_catalog_records_sha256")
    catalog_file_sha256 = one_text("directory_catalog_file_sha256")
    catalog_file_size = int(one_text("directory_catalog_file_size"))
    catalog_file_mtime_ns = int(one_text("directory_catalog_file_mtime_ns"))
    catalog_relative_path = one_text("directory_catalog_relative_path")
    catalog_as_of = pd.Timestamp(one_text("directory_catalog_as_of")).date().isoformat()
    catalog_expected_count = int(float(one_text("directory_catalog_expected_count")))
    expected_symbols = int(float(one_text("directory_expected_symbols")))
    observed_symbols = int(float(one_text("directory_observed_symbols")))
    rows = []
    for row in frame.to_dict("records"):
        rows.append(
            {
                "symbol": str(row.get("symbol") or "").upper(),
                "exchange": str(row.get("exchange") or "").upper(),
                "asset_type": str(row.get("asset_type") or "").casefold(),
                "status": str(row.get("status") or "").casefold(),
                "list_date": str(row.get("list_date") or ""),
                "delist_date": str(row.get("delist_date") or ""),
                "source": str(row.get("directory_member_source") or ""),
                "observed_at": utc_text(row.get("directory_member_observed_at")),
            }
        )
    return content_hash(
        {
            "attestation_version": ETF_DIRECTORY_ATTESTATION_VERSION,
            "effective_date": effective_date,
            "source": source,
            "acquired_at": acquired_at,
            "cutoff_at": cutoff_at,
            "master_record_count": master_record_count,
            "master_batch_record_count": master_batch_record_count,
            "master_snapshot_sha256": master_snapshot_sha256,
            "catalog_snapshot_id": catalog_snapshot_id,
            "catalog_records_sha256": catalog_records_sha256,
            "catalog_file_sha256": catalog_file_sha256,
            "catalog_file_size": catalog_file_size,
            "catalog_file_mtime_ns": catalog_file_mtime_ns,
            "catalog_relative_path": catalog_relative_path,
            "catalog_as_of": catalog_as_of,
            "catalog_expected_count": catalog_expected_count,
            "expected_symbols": expected_symbols,
            "observed_symbols": observed_symbols,
            "rows": sorted(rows, key=lambda row: row["symbol"]),
        }
    )


def is_exchange_etf(instrument: Instrument) -> bool:
    if instrument.exchange not in {"SH", "SZ"}:
        return False
    if instrument.status.casefold() not in {"listed", "active", "l"}:
        return False
    text = instrument.name.upper()
    if "LOF" in text or "联接" in text:
        return False
    return instrument.asset_type == "etf" or (
        instrument.asset_type == "fund" and ("ETF" in text or "交易型" in text)
    )


class EtfResearchStore:
    def __init__(self, root: str | Path | None = None):
        self.root = Path(root or (get_config().data_root / "etf-research")).resolve()
        self._lock = threading.RLock()
        self._cache: dict[str, tuple[int, int, EtfResearchSnapshot]] = {}

    @property
    def frozen_adjustments(self) -> Path:
        return self.root / "evidence" / "frozen-adjustments"

    def _snapshot_path(self, snapshot_id: str) -> Path:
        value = str(snapshot_id or "")
        if _PRODUCTION_SNAPSHOT_ID.fullmatch(value) is None:
            raise ValueError("ETF 研究快照标识无效")
        return confined_path(
            self.root / "snapshots",
            f"{value}.json",
            label="ETF 研究快照",
        )

    @staticmethod
    def _require_evidence_hash(value: str) -> str:
        digest = str(value or "").casefold()
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise RuntimeError("ETF 快照缺少有效的复权证据哈希")
        return digest

    def freeze_adjustments(self, frame: pd.DataFrame, expected_hash: str) -> Path:
        """Persist one immutable, content-addressed factor artifact for snapshot replay."""

        digest = self._require_evidence_hash(expected_hash)
        value = frame.copy() if frame is not None else pd.DataFrame()
        actual = _frame_hash(value, _ADJUSTMENT_COLUMNS)
        if actual != digest:
            raise RuntimeError(
                f"ETF 复权证据哈希不匹配: snapshot={digest}, actual={actual}"
            )
        target = self.frozen_adjustments / f"{digest}.parquet"
        with self._lock:
            if target.is_file():
                self.load_frozen_adjustments(digest)
                return target
            target.parent.mkdir(parents=True, exist_ok=True)
            fd, temp_name = tempfile.mkstemp(
                prefix=f".{digest}.", suffix=".parquet.tmp", dir=target.parent
            )
            os.close(fd)
            temp = Path(temp_name)
            try:
                value.to_parquet(temp, index=False)
                persisted = pd.read_parquet(temp)
                persisted_hash = _frame_hash(persisted, _ADJUSTMENT_COLUMNS)
                if persisted_hash != digest:
                    raise RuntimeError(
                        "ETF 复权冻结制品写入后哈希不一致: "
                        f"snapshot={digest}, persisted={persisted_hash}"
                    )
                if not target.exists():
                    os.replace(temp, target)
            finally:
                temp.unlink(missing_ok=True)
        return target

    def load_frozen_adjustments(self, expected_hash: str) -> pd.DataFrame:
        digest = self._require_evidence_hash(expected_hash)
        target = self.frozen_adjustments / f"{digest}.parquet"
        if not target.is_file():
            raise RuntimeError(
                f"ETF 快照的冻结复权证据缺失: {digest}；旧快照不可用当前因子回填"
            )
        try:
            value = pd.read_parquet(target)
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"ETF 冻结复权证据无法读取: {digest}") from exc
        actual = _frame_hash(value, _ADJUSTMENT_COLUMNS)
        if actual != digest:
            raise RuntimeError(
                f"ETF 冻结复权证据哈希不匹配: snapshot={digest}, actual={actual}"
            )
        return value

    def publish(self, snapshot: EtfResearchSnapshot) -> EtfResearchSnapshot:
        if (
            snapshot.tier != "production"
            or not snapshot.formal_eligible
        ):
            raise RuntimeError("EtfResearchStore 仅接受正式 production 快照")
        try:
            target = self._snapshot_path(snapshot.snapshot_id)
        except ValueError as exc:
            raise RuntimeError("EtfResearchStore 仅接受内容寻址的 production 快照") from exc
        encoded = json.dumps(snapshot.to_dict(), ensure_ascii=False, sort_keys=True, indent=2, default=str)
        with self._lock:
            if target.exists():
                existing = EtfResearchSnapshot.from_dict(json.loads(target.read_text(encoding="utf-8")))
                identity = (
                    "ingest_id",
                    "artifact_id",
                    "as_of_date",
                    "input_hash",
                    "evidence_hashes",
                    "research_model_version",
                    "schema_version",
                )
                if any(getattr(existing, key) != getattr(snapshot, key) for key in identity):
                    raise RuntimeError(f"ETF 研究快照不可变: {snapshot.snapshot_id}")
                snapshot = existing
            else:
                _atomic_text(target, encoded)
            _atomic_text(
                self.root / "latest.json",
                json.dumps(
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "last_failure": {},
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
            )
            self._cache.clear()
            stat = target.stat()
            self._cache[snapshot.snapshot_id] = (stat.st_mtime_ns, stat.st_size, snapshot)
            snapshots_root = self.root / "snapshots"
            for path in snapshots_root.glob("*.json"):
                if path == target:
                    continue
                try:
                    value = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, ValueError, json.JSONDecodeError):
                    continue
                if (
                    value.get("schema_version") != ETF_SCHEMA_VERSION
                    or value.get("research_model_version") != ETF_RESEARCH_MODEL_VERSION
                ):
                    path.unlink(missing_ok=True)
            return snapshot

    def get(self, snapshot_id: str) -> EtfResearchSnapshot | None:
        try:
            path = self._snapshot_path(snapshot_id)
            stat = path.stat()
            signature = (stat.st_mtime_ns, stat.st_size)
            with self._lock:
                cached = self._cache.get(snapshot_id)
                if cached is not None and cached[:2] == signature:
                    return cached[2]
            value = json.loads(path.read_text(encoding="utf-8"))
            snapshot = EtfResearchSnapshot.from_dict(value)
            with self._lock:
                self._cache[snapshot_id] = (*signature, snapshot)
            return snapshot
        except (FileNotFoundError, OSError, TypeError, ValueError, json.JSONDecodeError):
            # Obsolete contracts are deliberately unavailable instead of being reinterpreted.
            return None

    def latest(self) -> EtfResearchSnapshot | None:
        try:
            state = json.loads((self.root / "latest.json").read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            return None
        snapshot = self.get(str(state.get("snapshot_id") or ""))
        failure = state.get("last_failure") or {}
        if snapshot is not None and failure:
            data = snapshot.to_dict()
            data["staleness"] = {
                "stale": True,
                "reason": str(failure.get("reason") or "ETF 研究刷新失败"),
                "last_attempt_at": str(failure.get("attempted_at") or ""),
            }
            return EtfResearchSnapshot.from_dict(data)
        return snapshot

    def history(self, limit: int = 50) -> list[dict[str, Any]]:
        paths = sorted(
            (self.root / "snapshots").glob("*.json"), key=lambda item: item.stat().st_mtime_ns, reverse=True
        )
        result: list[dict[str, Any]] = []
        for path in paths:
            if len(result) >= max(1, limit):
                break
            try:
                snapshot = self.get(path.stem)
                if snapshot is None:
                    continue
                result.append(
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "ingest_id": snapshot.ingest_id,
                        "as_of_date": snapshot.as_of_date,
                        "generated_at": snapshot.generated_at,
                        "coverage": snapshot.coverage,
                        "item_count": len(snapshot.items),
                        "categories": snapshot.categories,
                    }
                )
            except (OSError, KeyError, ValueError, json.JSONDecodeError):
                continue
        return result

    def record_failure(self, reason: str) -> None:
        from datetime import UTC, datetime

        latest = self.latest()
        _atomic_text(
            self.root / "latest.json",
            json.dumps(
                {
                    "snapshot_id": latest.snapshot_id if latest else "",
                    "last_failure": {
                        "reason": reason[:500],
                        "attempted_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
        )


class EtfResearchService:
    def __init__(
        self,
        *,
        source: FreeStockDBSource | None = None,
        instruments: InstrumentStore | None = None,
        ingest_store: StockDBIngestStore | None = None,
        store: EtfResearchStore | None = None,
    ):
        self.source = source or FreeStockDBSource()
        self.instruments = instruments or InstrumentStore()
        self.ingest_store = ingest_store or StockDBIngestStore()
        self.store = store or EtfResearchStore()
        self._profile_capabilities: dict[str, Any] = {}
        self._profile_metadata_frame = pd.DataFrame()
        self._detail_history_cache: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        self._scan_lock = threading.RLock()
        self._preview_lock = threading.RLock()
        self._previews: dict[str, EtfResearchSnapshot] = {}
        self._preview_daily: dict[str, pd.DataFrame] = {}
        self._preview_factors: dict[str, pd.DataFrame] = {}

    @staticmethod
    def _research_tier(value: str) -> EtfResearchTier:
        tier = str(value or "production").strip().casefold()
        if tier not in {"production", "sandbox"}:
            raise ValueError("ETF 研究 tier 仅支持 production 或 sandbox")
        return tier  # type: ignore[return-value]

    @staticmethod
    def _research_target(as_of: str = "") -> tuple[pd.Timestamp, str]:
        """Resolve the research ceiling from completed local stockdb evidence first."""

        current_market_date = pd.Timestamp(market_date()).normalize()
        if as_of:
            target = pd.Timestamp(as_of).normalize()
            if target > current_market_date:
                raise RuntimeError(
                    f"ETF 研究 as_of {target.date()} 晚于当前市场日 "
                    f"{current_market_date.date()}"
                )
            return target, "explicit-as-of"

        validated_target: pd.Timestamp | None = None
        try:
            from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

            status = free_stockdb_runtime.status()
            validated = pd.to_datetime(
                status.get("validated_session"), errors="coerce",
            )
            if pd.notna(validated):
                target = pd.Timestamp(validated).normalize()
                before_target_close = (
                    target == current_market_date
                    and pd.Timestamp(market_now())
                    < pd.Timestamp(daily_signal_cutoff(target.date()))
                )
                if target <= current_market_date and not before_target_close:
                    validated_target = target
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            pass
        try:
            expectation = resolve_session_target()
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            expectation = None
        expected_target = (
            pd.Timestamp(expectation.session).normalize()
            if expectation is not None and expectation.ready and expectation.session
            else None
        )
        if validated_target is not None and (
            expected_target is None or validated_target <= expected_target
        ):
            return validated_target, "free-stockdb:validated-session"
        if expected_target is not None and expected_target <= current_market_date:
            return expected_target, expectation.source
        # An unavailable calendar is a reason to use a bounded prior-day request
        # ceiling, never the current wall-clock date.  The published snapshot is
        # still taken from the latest local daily bar returned below.
        bounded = current_market_date - pd.Timedelta(days=1)
        return bounded, "bounded-prior-day"

    def preview(self, snapshot_id: str = "") -> EtfResearchSnapshot | None:
        """Return an in-process sandbox preview without consulting production history."""

        with self._preview_lock:
            if snapshot_id:
                return self._previews.get(str(snapshot_id))
            return next(reversed(self._previews.values()), None) if self._previews else None

    def resolve_snapshot(
        self,
        snapshot_id: str = "",
        *,
        tier: str = "production",
    ) -> EtfResearchSnapshot | None:
        selected_tier = self._research_tier(tier)
        if selected_tier == "sandbox":
            return self.preview(snapshot_id)
        snapshot = self.store.get(snapshot_id) if snapshot_id else self.store.latest()
        if snapshot is not None and (
            snapshot.tier != "production" or not snapshot.formal_eligible
        ):
            raise RuntimeError("ETF production 路径拒绝非正式 sandbox 快照")
        return snapshot

    def _remember_preview(
        self,
        snapshot: EtfResearchSnapshot,
        daily: pd.DataFrame,
        factors: pd.DataFrame,
    ) -> EtfResearchSnapshot:
        if snapshot.tier != "sandbox" or snapshot.formal_eligible:
            raise RuntimeError("ETF preview 不得标记为 production 可发布")
        with self._preview_lock:
            self._previews[snapshot.snapshot_id] = snapshot
            self._preview_daily[snapshot.snapshot_id] = daily.copy(deep=True)
            self._preview_factors[snapshot.snapshot_id] = factors.copy(deep=True)
            while len(self._previews) > 5:
                oldest = next(iter(self._previews))
                self._previews.pop(oldest, None)
                self._preview_daily.pop(oldest, None)
                self._preview_factors.pop(oldest, None)
        return snapshot

    @staticmethod
    def _official_metadata() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        cfg = get_config().data
        if not cfg.tushare_token:
            return {}, {
                "status": "fallback",
                "source": "quantmaster:local-rules",
                "reason": "当前进程未读取到 Tushare 凭据，使用本地证券主表与显式主题词典",
            }
        if PROVIDER_HEALTH.disabled_status("tushare:etf_basic"):
            return {}, {
                "status": "fallback",
                "source": "quantmaster:local-rules",
                "reason": "etf_basic 已按当前凭据跳过，使用本地证券主表与显式主题词典",
            }
        try:
            from quantmaster.data.tushare_source import TushareSource

            source = TushareSource()
            basic = source._call(
                "etf_basic",
                7,
                list_status="L",
                fields=(
                    "ts_code,extname,cname,index_code,index_name,list_date,list_status,"
                    "exchange,mgr_name,custod_name,mgt_fee,etf_type"
                ),
            )
        except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
            return {}, {
                "status": "fallback",
                "source": "quantmaster:local-rules",
                "reason": f"etf_basic 不可用：{str(exc)[:160]}",
            }
        if basic.empty or "ts_code" not in basic:
            return {}, {
                "status": "fallback",
                "source": "quantmaster:local-rules",
                "reason": "etf_basic 返回空目录",
            }
        benchmark_rows: dict[str, dict[str, Any]] = {}
        benchmark_capability: dict[str, Any]
        try:
            benchmarks = source._call(
                "mkt_idx_bmk",
                5,
                fields="ts_code,name,fullname,bmk_level,bmk_type,bmk_src,idx_type",
            )
            if not benchmarks.empty and "ts_code" in benchmarks:
                benchmark_rows = {
                    _clean_scalar_text(row.get("ts_code")).upper(): row
                    for row in benchmarks.to_dict("records")
                    if _clean_scalar_text(row.get("ts_code"))
                }
            benchmark_capability = {
                "status": "ready" if benchmark_rows else "fallback",
                "source": "tushare:mkt_idx_bmk",
                "covered_indices": len(benchmark_rows),
                "reason": ("官方业绩基准分类可用" if benchmark_rows else "mkt_idx_bmk 返回空目录"),
            }
        except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
            benchmark_capability = {
                "status": "fallback",
                "source": "quantmaster:explicit-rules",
                "covered_indices": 0,
                "reason": f"mkt_idx_bmk 不可用：{str(exc)[:160]}",
            }
        metadata: dict[str, dict[str, Any]] = {}
        for row in basic.to_dict("records"):
            symbol = _clean_scalar_text(row.get("ts_code")).upper()
            if not symbol:
                continue
            benchmark = benchmark_rows.get(_clean_scalar_text(row.get("index_code")).upper(), {})
            metadata[symbol] = {
                "name": _clean_scalar_text(row.get("extname"), row.get("cname")),
                "benchmark_code": _clean_scalar_text(row.get("index_code")),
                "index_name": _clean_scalar_text(row.get("index_name")),
                "benchmark_type": _clean_scalar_text(benchmark.get("bmk_type")),
                "benchmark_level": _clean_scalar_text(benchmark.get("bmk_level")),
                "index_type": _clean_scalar_text(benchmark.get("idx_type")),
                "index_provider": _clean_scalar_text(benchmark.get("bmk_src")),
                "manager": _clean_scalar_text(row.get("mgr_name")),
                "custodian": _clean_scalar_text(row.get("custod_name")),
                "management_fee": pd.to_numeric(pd.Series([row.get("mgt_fee")]), errors="coerce").iloc[0],
                "etf_type": _clean_scalar_text(row.get("etf_type")),
                "list_date": _clean_scalar_text(row.get("list_date")),
            }
        return metadata, {
            "status": "ready",
            "source": "tushare:etf_basic",
            "covered_symbols": len(metadata),
            "reason": "官方 ETF 基础信息可用",
            "benchmark_classification": benchmark_capability,
        }

    @staticmethod
    def _metadata_effective_date(row: dict[str, Any]) -> pd.Timestamp | None:
        for key in ("effective_date", "as_of_date", "trade_date", "updated_at"):
            raw = row.get(key)
            if raw in (None, ""):
                continue
            parsed = pd.to_datetime(raw, errors="coerce")
            if pd.notna(parsed):
                stamp = pd.Timestamp(parsed)
                if stamp.tzinfo is not None:
                    stamp = stamp.tz_convert("Asia/Shanghai")
                return pd.Timestamp(stamp.date())
        return None

    @staticmethod
    def _metadata_observed_at(row: dict[str, Any]) -> pd.Timestamp | None:
        """Return a precise, timezone-aware metadata observation instant."""
        for key in ("observed_at", "updated_at"):
            raw = row.get(key)
            if raw in (None, ""):
                continue
            parsed = pd.to_datetime(raw, errors="coerce")
            if pd.isna(parsed):
                continue
            stamp = pd.Timestamp(parsed)
            if stamp.tzinfo is None:
                return None
            return stamp.tz_convert("UTC")
        return None

    def _sandbox_profiles(
        self,
        target: pd.Timestamp,
        *,
        historical: bool,
    ) -> list[EtfProfile]:
        """Build an explicitly incomplete local denominator for exploratory analysis."""

        from quantmaster.rotation.store import RotationStore

        def clean(*values: Any) -> str:
            for raw in values:
                if raw is None:
                    continue
                if isinstance(raw, (dict, list, set, tuple)):
                    if raw:
                        return str(raw)
                    continue
                if pd.isna(raw):
                    continue
                value = str(raw).strip()
                if value and value.casefold() not in {"nan", "none", "nat"}:
                    return value
            return ""

        def utc_iso(value: pd.Timestamp | None) -> str:
            if value is None or pd.isna(value):
                return ""
            stamp = pd.Timestamp(value)
            if stamp.tzinfo is None:
                return ""
            return stamp.tz_convert("UTC").isoformat()

        cutoff = pd.Timestamp(daily_signal_cutoff(target.date())).tz_convert("UTC")
        current = pd.Timestamp(market_now())
        if current.tzinfo is None:
            current = current.tz_localize("Asia/Shanghai")
        knowledge_cutoff = cutoff if historical else current.tz_convert("UTC")
        candidates: dict[str, dict[str, Any]] = {}
        evidence: dict[str, list[dict[str, str]]] = {}
        rich_rows: dict[str, tuple[tuple[pd.Timestamp, pd.Timestamp], dict[str, Any]]] = {}
        share_rows: dict[str, tuple[tuple[pd.Timestamp, pd.Timestamp], dict[str, Any]]] = {}

        def lifecycle_valid(row: dict[str, Any]) -> bool:
            listed = pd.to_datetime(row.get("list_date"), errors="coerce")
            delisted = pd.to_datetime(row.get("delist_date"), errors="coerce")
            if pd.notna(listed) and pd.Timestamp(listed).normalize() > target:
                return False
            if pd.notna(delisted) and target > pd.Timestamp(delisted).normalize():
                return False
            status = clean(row.get("status")).casefold()
            return not (
                status in {"d", "delisted", "terminated"}
                and pd.isna(delisted)
            )

        def accept_symbol(symbol: str) -> bool:
            return symbol.endswith((".SH", ".SZ")) and len(symbol.split(".", 1)[0]) == 6

        def add_candidate(
            symbol: str,
            row: dict[str, Any],
            *,
            kind: str,
            source: str,
            observed_at: pd.Timestamp | None,
        ) -> None:
            canonical = symbol.upper()
            if not accept_symbol(canonical) or not lifecycle_valid(row):
                return
            name = clean(row.get("name"), canonical)
            if "LOF" in name.upper() or "联接" in name:
                return
            existing = candidates.get(canonical, {})
            candidates[canonical] = {
                **existing,
                **{key: value for key, value in row.items() if clean(value)},
                "symbol": canonical,
                "name": clean(row.get("name"), existing.get("name"), canonical),
                "exchange": clean(row.get("exchange"), existing.get("exchange"), canonical[-2:]).upper(),
                "asset_type": "etf",
            }
            item = {
                "kind": kind,
                "source": source or "unknown-local-source",
                "observed_at": utc_iso(observed_at),
            }
            if item not in evidence.setdefault(canonical, []):
                evidence[canonical].append(item)

        try:
            local_instruments = self.instruments.list(market="CN")
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            local_instruments = []
        for instrument in local_instruments:
            name = clean(instrument.name)
            if instrument.exchange not in {"SH", "SZ"}:
                continue
            if instrument.asset_type != "etf" and not (
                instrument.asset_type == "fund"
                and ("ETF" in name.upper() or "交易型" in name)
            ):
                continue
            observed_at = (
                pd.Timestamp(float(instrument.observed_at), unit="s", tz="UTC")
                if float(instrument.observed_at or 0) > 0
                else None
            )
            if historical and observed_at is None:
                continue
            if observed_at is not None and observed_at > knowledge_cutoff:
                continue
            add_candidate(
                instrument.symbol,
                instrument.to_dict(),
                kind="instrument_store",
                source=clean(instrument.source, "InstrumentStore"),
                observed_at=observed_at,
            )

        metadata = pd.DataFrame()
        observations = pd.DataFrame()
        try:
            metadata_store = RotationStore()
            metadata = (
                metadata_store.etf_metadata_history()
                if hasattr(metadata_store, "etf_metadata_history")
                else metadata_store.etf_metadata()
            )
            observations = metadata_store.etf_observations()
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            pass

        if not metadata.empty and "symbol" in metadata:
            for row in metadata.to_dict("records"):
                symbol = clean(row.get("symbol")).upper()
                if not accept_symbol(symbol):
                    continue
                asset_type = clean(row.get("asset_type")).casefold()
                name = clean(row.get("name"))
                if asset_type and asset_type not in {"etf", "fund"}:
                    continue
                if asset_type != "etf" and name and "ETF" not in name.upper() and "交易型" not in name:
                    continue
                effective = self._metadata_effective_date(row)
                observed_at = self._metadata_observed_at(row)
                if effective is not None and effective > target:
                    continue
                if observed_at is not None and observed_at > knowledge_cutoff:
                    continue
                if historical and observed_at is None:
                    continue
                effective_key = effective if effective is not None else pd.Timestamp.min
                observed_key = (
                    observed_at.tz_localize(None)
                    if observed_at is not None
                    else pd.Timestamp.min
                )
                key = (effective_key, observed_key)
                if symbol not in rich_rows or key >= rich_rows[symbol][0]:
                    rich_rows[symbol] = (key, dict(row))
                add_candidate(
                    symbol,
                    row,
                    kind="etf_metadata",
                    source=clean(row.get("metadata_source"), "RotationStore:etf_metadata"),
                    observed_at=observed_at,
                )

        if not observations.empty and "symbol" in observations:
            for row in observations.to_dict("records"):
                symbol = clean(row.get("symbol")).upper()
                if not accept_symbol(symbol):
                    continue
                trade_date = pd.to_datetime(row.get("trade_date"), errors="coerce")
                if pd.isna(trade_date) or pd.Timestamp(trade_date).normalize() > target:
                    continue
                acquired = pd.to_datetime(row.get("acquired_at"), errors="coerce", utc=True)
                acquired_at = pd.Timestamp(acquired) if pd.notna(acquired) else None
                if acquired_at is not None and acquired_at > knowledge_cutoff:
                    continue
                if historical and acquired_at is None:
                    continue
                observed_key = (
                    acquired_at.tz_localize(None)
                    if acquired_at is not None
                    else pd.Timestamp.min
                )
                key = (pd.Timestamp(trade_date).normalize(), observed_key)
                if symbol not in share_rows or key >= share_rows[symbol][0]:
                    share_rows[symbol] = (key, dict(row))
                add_candidate(
                    symbol,
                    {
                        **row,
                        "name": clean(row.get("name")),
                        "list_date": clean(row.get("list_date")),
                        "delist_date": clean(row.get("delist_date")),
                    },
                    kind="etf_observation",
                    source=clean(
                        row.get("share_source"),
                        row.get("source"),
                        "RotationStore:etf_observations",
                    ),
                    observed_at=acquired_at,
                )

        if not candidates:
            self._profile_metadata_frame = pd.DataFrame()
            self._profile_capabilities = {
                "status": "unavailable",
                "tier": "sandbox",
                "formal_eligible": False,
                "source": "local-etf-evidence",
                "covered_symbols": 0,
                "reason": "本地 InstrumentStore/StockDB 没有截至目标时点可用的 ETF 记录",
            }
            raise RuntimeError("本地证据中没有可用的沪深场内 ETF sandbox 分析母集")

        denominator_rows: list[dict[str, Any]] = []
        result: list[EtfProfile] = []
        profile_metadata: list[dict[str, Any]] = []
        for symbol in sorted(candidates):
            base = candidates[symbol]
            rich = rich_rows.get(symbol, ((pd.Timestamp.min, pd.Timestamp.min), {}))[1]
            share = share_rows.get(symbol, ((pd.Timestamp.min, pd.Timestamp.min), {}))[1]
            lifecycle = dict(base)
            for lifecycle_row in (share, rich):
                for lifecycle_field in ("list_date", "delist_date", "status"):
                    value = clean(lifecycle_row.get(lifecycle_field))
                    if value:
                        lifecycle[lifecycle_field] = value
            if not lifecycle_valid(lifecycle):
                continue
            sources = sorted({item["source"] for item in evidence.get(symbol, [])})
            observed_values = sorted(
                item["observed_at"] for item in evidence.get(symbol, []) if item["observed_at"]
            )
            name = clean(rich.get("name"), base.get("name"), symbol)
            benchmark = clean(rich.get("benchmark"), share.get("benchmark"))
            benchmark_code = clean(rich.get("benchmark_code"))
            fund_type = clean(rich.get("fund_type"), share.get("fund_type"), "ETF")
            invest_type = clean(rich.get("invest_type"), share.get("invest_type"))
            taxonomy = classify_etf_profile(
                name,
                benchmark=benchmark,
                benchmark_code=benchmark_code,
                index_name=clean(rich.get("index_name"), benchmark),
                fund_type=fund_type,
                invest_type=invest_type,
                etf_type=clean(rich.get("etf_type")),
                benchmark_type=clean(rich.get("benchmark_type")),
                index_type=clean(rich.get("index_type")),
                metadata_source="local_stockdb",
            )
            effective = self._metadata_effective_date(rich) if rich else None
            if effective is None and share:
                parsed_effective = pd.to_datetime(share.get("trade_date"), errors="coerce")
                effective = (
                    pd.Timestamp(parsed_effective).normalize()
                    if pd.notna(parsed_effective)
                    else None
                )
            effective_text = effective.date().isoformat() if effective is not None else ""
            raw_list_date = clean(rich.get("list_date"), base.get("list_date"))
            raw_delist_date = clean(rich.get("delist_date"), base.get("delist_date"))
            member = {
                "symbol": symbol,
                "sources": sources,
                "observed_at": observed_values[-1] if observed_values else "",
                "evidence": sorted(
                    evidence.get(symbol, []),
                    key=lambda item: (item["kind"], item["source"], item["observed_at"]),
                ),
                "list_date": raw_list_date,
                "delist_date": raw_delist_date,
                "metadata_effective_as_of": effective_text,
            }
            denominator_rows.append(member)
            fee = rich.get("management_fee", rich.get("mgt_fee"))
            numeric_fee = pd.to_numeric(pd.Series([fee]), errors="coerce").iloc[0]
            result.append(
                EtfProfile(
                    symbol=symbol,
                    name=name,
                    category=taxonomy["category"],
                    asset_class=taxonomy["asset_class"],
                    sector_id=taxonomy["sector_id"],
                    sector_name=taxonomy["sector_name"],
                    benchmark=benchmark,
                    benchmark_code=benchmark_code,
                    benchmark_type=clean(rich.get("benchmark_type")),
                    benchmark_level=clean(rich.get("benchmark_level")),
                    index_type=clean(rich.get("index_type")),
                    index_provider=clean(rich.get("index_provider")),
                    normalized_index=taxonomy["normalized_index"],
                    fund_type=fund_type,
                    invest_type=invest_type,
                    manager=clean(rich.get("manager"), rich.get("mgr_name")),
                    custodian=clean(rich.get("custodian"), rich.get("custod_name")),
                    management_fee=float(numeric_fee) if pd.notna(numeric_fee) else None,
                    metadata_source=" + ".join(sources) or "local_stockdb",
                    classification_source=taxonomy["classification_source"],
                    classification_confidence=taxonomy["classification_confidence"],
                    list_date=raw_list_date,
                    metadata_effective_as_of=effective_text,
                    status=clean(rich.get("status"), base.get("status"), "locally_observed"),
                    classification_evidence=taxonomy["classification_evidence"],
                )
            )
            profile_metadata.append(
                {
                    **base,
                    **rich,
                    "symbol": symbol,
                    "name": name,
                    "metadata_source": " + ".join(sources) or "local_stockdb",
                    "observed_at": member["observed_at"],
                    "updated_at": member["metadata_effective_as_of"],
                }
            )

        denominator_id = "etf_sandbox_denominator_" + content_hash(
            {
                "as_of": target.date().isoformat(),
                "knowledge_cutoff": knowledge_cutoff.isoformat(),
                "rows": denominator_rows,
            }
        )[:24]
        timed = sum(bool(row["observed_at"]) for row in denominator_rows)
        named = sum(item.name != item.symbol for item in result)
        source_values = sorted(
            {source for row in denominator_rows for source in row["sources"]}
        )
        denominator = {
            "snapshot_id": denominator_id,
            "as_of": target.date().isoformat(),
            "knowledge_cutoff": knowledge_cutoff.isoformat(),
            "scope": "locally-observed-sh-sz-etfs",
            "complete_market_denominator": False,
            "formal_eligible": False,
            "expected_symbols": len(denominator_rows),
            "observed_symbols": len(denominator_rows),
            "coverage": 1.0,
            "named_coverage": named / len(denominator_rows),
            "timed_coverage": timed / len(denominator_rows),
            "sources": source_values,
            "members": denominator_rows,
        }
        self._profile_metadata_frame = pd.DataFrame(profile_metadata)
        self._profile_capabilities = {
            "status": "degraded",
            "tier": "sandbox",
            "formal_eligible": False,
            "source": " + ".join(source_values) or "local-etf-evidence",
            "covered_symbols": len(result),
            "official_covered_symbols": 0,
            "enhanced_covered_symbols": 0,
            "benchmark_covered_symbols": sum(bool(item.benchmark) for item in result),
            "denominator": denominator,
            "reason": (
                "不可变 Tushare 目录不可用；本结果仅使用截止时点前的本地已观测 ETF，"
                "可用于探索分析但不得发布为正式快照"
            ),
        }
        return result

    def profiles(
        self,
        as_of: str = "",
        *,
        tier: str = "production",
    ) -> list[EtfProfile]:
        """Build ETF profiles using only metadata evidenced by ``as_of``."""
        from quantmaster.data.instrument_snapshots import (
            InstrumentCatalogEvidenceError,
            load_instrument_catalog_snapshot,
            verify_instrument_catalog_evidence,
        )

        selected_tier = self._research_tier(tier)
        target, target_source = self._research_target(as_of)
        current_market_date = pd.Timestamp(market_date()).normalize()
        if target > current_market_date:
            raise RuntimeError(
                f"ETF 研究 as_of {target.date()} 晚于当前市场日 {current_market_date.date()}"
            )
        historical = bool(as_of)
        if selected_tier == "sandbox":
            return self._sandbox_profiles(target, historical=historical)
        try:
            catalog_snapshot, expected_symbols, catalog_evidence = (
                load_instrument_catalog_snapshot(
                    as_of=target.date().isoformat(),
                    market="CN",
                    asset_type="etf",
                )
            )
        except (InstrumentCatalogEvidenceError, OSError, TypeError, ValueError) as exc:
            if not historical:
                try:
                    local_profiles = self._sandbox_profiles(target, historical=False)
                except (OSError, RuntimeError, TypeError, ValueError) as local_exc:
                    self._profile_capabilities = {
                        "status": "unavailable",
                        "source": "immutable-tushare-catalog + local-etf-evidence",
                        "covered_symbols": 0,
                        "reason": (
                            f"正式 ETF 目录不可用：{str(exc)[:180]}；"
                            f"本地 ETF 母集也不可用：{str(local_exc)[:180]}"
                        ),
                    }
                    raise RuntimeError(
                        f"{target.date()} 没有可用的 ETF 目录或本地母集"
                    ) from local_exc
                local_capabilities = dict(self._profile_capabilities)
                denominator = dict(local_capabilities.get("denominator") or {})
                denominator.update({
                    "formal_eligible": True,
                    "publication_basis": "explicit-local-denominator-degradation",
                })
                reason = (
                    f"{target.date()} 的不可变 Tushare ETF 目录无法精确复验："
                    f"{str(exc)[:180]}；已改用 stockdb 与本地缓存中已观测的场内 ETF "
                    "母集继续生成，未覆盖产品不参与结论"
                )
                self._profile_capabilities = {
                    **local_capabilities,
                    "status": "degraded",
                    "tier": "production",
                    "formal_eligible": True,
                    "publication_allowed": True,
                    "source": local_capabilities.get("source") or "local-etf-evidence",
                    "target_source": target_source,
                    "denominator": denominator,
                    "reason": reason,
                    "catalog_error": str(exc)[:300],
                }
                return local_profiles
            self._profile_capabilities = {
                "status": "unavailable",
                "source": "immutable-tushare-catalog",
                "covered_symbols": 0,
                "target_source": target_source,
                "reason": f"不可变 ETF 证券目录证据不可用：{exc}",
            }
            raise RuntimeError(
                f"{target.date()} 没有完整、可复验的 Tushare ETF 目录 artifact"
            ) from exc
        catalog_rows = {
            str(row.get("symbol") or "").upper(): dict(row)
            for row in catalog_snapshot.records
            if str(row.get("symbol") or "").upper() in expected_symbols
        }
        if set(catalog_rows) != expected_symbols:
            raise RuntimeError("ETF catalog artifact 的 records 与 PIT expected 集合不一致")
        for symbol, row in catalog_rows.items():
            status = str(row.get("status") or "").strip().casefold()
            listed = pd.to_datetime(row.get("list_date"), errors="coerce")
            delisted = pd.to_datetime(row.get("delist_date"), errors="coerce")
            if (
                str(row.get("exchange") or "").upper() not in {"SH", "SZ"}
                or str(row.get("asset_type") or "").casefold() != "etf"
                or not status
                or pd.isna(listed)
                or pd.Timestamp(listed).normalize() > target
                or (status in {"d", "delisted", "terminated"} and pd.isna(delisted))
            ):
                raise RuntimeError(f"ETF catalog artifact 的 {symbol} 生命周期证据不完整")
        directory: dict[str, dict[str, Any]] = {
            symbol: {
                **row,
                "symbol": symbol,
                "metadata_source": ETF_DIRECTORY_TRUSTED_SOURCE,
                "_metadata_effective": target,
            }
            for symbol, row in catalog_rows.items()
        }
        observations = self._direct_share_observations()
        share_metadata: dict[str, dict[str, str]] = {}
        if not observations.empty:
            observations = observations.copy()
            observations["trade_date"] = pd.to_datetime(
                observations.get("trade_date"), errors="coerce"
            )
            observations["acquired_at"] = pd.to_datetime(
                observations.get("acquired_at"), errors="coerce", utc=True,
            )
            eligible_observations = observations["trade_date"].le(target)
            if historical:
                cutoff = pd.Timestamp(daily_signal_cutoff(target.date())).tz_convert("UTC")
                eligible_observations &= (
                    observations["acquired_at"].notna()
                    & observations["acquired_at"].le(cutoff)
                )
            observations = observations.loc[eligible_observations]
            for symbol, group in observations.groupby("symbol"):
                last = group.sort_values(["trade_date", "acquired_at"]).iloc[-1]
                share_metadata[str(symbol).upper()] = {
                    **{
                        key: _clean_scalar_text(last.get(key))
                        for key in ("benchmark", "fund_type", "invest_type")
                    },
                    "effective_as_of": pd.Timestamp(last["trade_date"]).strftime("%Y-%m-%d"),
                }
        cached = pd.DataFrame()
        try:
            from quantmaster.rotation.store import RotationStore
        except ImportError:
            cached = pd.DataFrame()
        else:
            metadata_store = RotationStore()
            cached = (
                metadata_store.etf_metadata_history()
                if hasattr(metadata_store, "etf_metadata_history")
                else metadata_store.etf_metadata()
            )
        if not cached.empty and "symbol" in cached:
            cached = cached.copy()
            cached["symbol"] = cached["symbol"].astype(str).str.upper()
            records = cached.to_dict("records")
            cached["_metadata_effective"] = [
                self._metadata_effective_date(row) for row in records
            ]
            cached["_metadata_observed"] = pd.to_datetime(
                [self._metadata_observed_at(row) for row in records], utc=True,
            )
            catalog_effective = pd.Timestamp(catalog_evidence["as_of"]).normalize()
            eligible = cached["_metadata_effective"].eq(catalog_effective)
            cached = cached.loc[eligible].sort_values(
                ["symbol", "_metadata_effective", "_metadata_observed"],
                na_position="first",
            ).copy()
            required = {
                "exchange",
                "asset_type",
                "status",
                "list_date",
                "delist_date",
                "directory_snapshot_id",
                "directory_complete",
                "directory_expected_symbols",
                "directory_observed_symbols",
                "directory_member_source",
                "directory_member_observed_at",
                "directory_source",
                "directory_acquired_at",
                "directory_cutoff_at",
                "directory_freshness",
                "directory_master_record_count",
                "directory_master_batch_record_count",
                "directory_master_snapshot_sha256",
                "directory_catalog_snapshot_id",
                "directory_catalog_records_sha256",
                "directory_catalog_file_sha256",
                "directory_catalog_file_size",
                "directory_catalog_file_mtime_ns",
                "directory_catalog_relative_path",
                "directory_catalog_as_of",
                "directory_catalog_expected_count",
                "directory_attestation_sha256",
            }
            complete_snapshots: list[pd.DataFrame] = []
            claimed_complete = False
            if not cached.empty:
                missing_columns = sorted(required - set(cached.columns))
                if missing_columns:
                    if "directory_complete" in cached and cached[
                        "directory_complete"
                    ].map(
                        lambda value: value is True
                        or str(value).strip().casefold() in {"1", "true", "yes"}
                    ).any():
                        raise RuntimeError(
                            "ETF 目录声称 complete 但缺少 artifact 证据字段: "
                            + ", ".join(missing_columns)
                        )
                    cached = pd.DataFrame()
            if not cached.empty:
                for directory_snapshot_id, group in cached.groupby(
                    "directory_snapshot_id", dropna=False
                ):
                    truth = group["directory_complete"].map(
                        lambda value: value is True
                        or str(value).strip().casefold() in {"1", "true", "yes"}
                    )
                    if not truth.any():
                        continue
                    claimed_complete = True
                    snapshot_key = str(directory_snapshot_id or "").strip()
                    master_group = (
                        group.sort_values("_metadata_observed")
                        .drop_duplicates("symbol", keep="last")
                        .copy()
                    )
                    expected_count = len(expected_symbols)
                    symbol_set = set(master_group["symbol"].astype(str).str.upper())
                    acquired = pd.to_datetime(
                        master_group["directory_acquired_at"], errors="coerce", utc=True
                    )
                    member_observed = pd.to_datetime(
                        master_group["directory_member_observed_at"],
                        errors="coerce",
                        utc=True,
                    )
                    declared_cutoff = pd.to_datetime(
                        master_group["directory_cutoff_at"], errors="coerce", utc=True
                    )
                    metadata_observed = master_group["_metadata_observed"]
                    catalog_acquired = pd.Timestamp(catalog_snapshot.acquired_at)
                    if catalog_acquired.tzinfo is None:
                        catalog_acquired = catalog_acquired.tz_localize("UTC")
                    catalog_acquired = catalog_acquired.tz_convert("UTC")
                    cutoff = pd.Timestamp(
                        daily_signal_cutoff(catalog_effective.date())
                    ).tz_convert("UTC")
                    representative = master_group.iloc[0]
                    artifact_evidence = {
                        "snapshot_id": str(
                            representative["directory_catalog_snapshot_id"]
                        ),
                        "file_sha256": str(
                            representative["directory_catalog_file_sha256"]
                        ),
                        "file_size": int(
                            representative["directory_catalog_file_size"]
                        ),
                        "file_mtime_ns": int(
                            representative["directory_catalog_file_mtime_ns"]
                        ),
                        "relative_path": str(
                            representative["directory_catalog_relative_path"]
                        ),
                        "as_of": str(representative["directory_catalog_as_of"]),
                        "expected_count": int(
                            representative["directory_catalog_expected_count"]
                        ),
                        # Reconstruct the complete membership contract as well
                        # as the file identity.  Omitting these fields makes a
                        # valid historical catalog look unverifiable even when
                        # its bytes and record hash are unchanged.
                        "membership_as_of": str(
                            catalog_evidence.get("membership_as_of")
                            or representative["directory_catalog_as_of"]
                        ),
                        "observation_active_as_of": str(
                            catalog_evidence.get("observation_active_as_of")
                            or representative["directory_catalog_as_of"]
                        ),
                        "membership_reconstructed": bool(
                            catalog_evidence.get("membership_reconstructed", False)
                        ),
                        "membership_contract": str(
                            catalog_evidence.get("membership_contract") or ""
                        ),
                    }
                    try:
                        verified_catalog, verified_symbols = (
                            verify_instrument_catalog_evidence(
                                artifact_evidence,
                                market="CN",
                                asset_type="etf",
                            )
                        )
                        actual_attestation = etf_directory_master_hash(master_group)
                    except (
                        InstrumentCatalogEvidenceError,
                        OSError,
                        TypeError,
                        ValueError,
                    ):
                        continue
                    counts_valid = all(
                        master_group[column].astype(str).nunique() == 1
                        and int(master_group[column].iloc[0]) == expected_count
                        for column in (
                            "directory_expected_symbols",
                            "directory_observed_symbols",
                            "directory_catalog_expected_count",
                        )
                    )
                    master_count = int(catalog_snapshot.manifest["record_count"])
                    master_counts_valid = all(
                        master_group[column].astype(str).nunique() == 1
                        and int(master_group[column].iloc[0]) == master_count
                        for column in (
                            "directory_master_record_count",
                            "directory_master_batch_record_count",
                        )
                    )
                    identity_valid = all(
                        all(
                            str(row.get(column) or "")
                            == str(catalog_rows[symbol].get(column) or "")
                            for column in (
                                "exchange",
                                "asset_type",
                                "status",
                                "list_date",
                                "delist_date",
                            )
                        )
                        for symbol, row in {
                            str(item.get("symbol") or "").upper(): item
                            for item in master_group.to_dict("records")
                        }.items()
                        if symbol in catalog_rows
                    )
                    batch_valid = bool(
                        acquired.notna().all()
                        and acquired.nunique() == 1
                        and acquired.iloc[0] == catalog_acquired
                        and acquired.iloc[0] >= cutoff
                        and acquired.iloc[0].tz_convert("Asia/Shanghai").date()
                        == catalog_effective.date()
                        and member_observed.notna().all()
                        and member_observed.eq(catalog_acquired).all()
                        and declared_cutoff.notna().all()
                        and declared_cutoff.eq(cutoff).all()
                        and metadata_observed.notna().all()
                        and metadata_observed.ge(catalog_acquired).all()
                        and metadata_observed.map(
                            lambda value: value.tz_convert("Asia/Shanghai").date()
                        ).eq(catalog_effective.date()).all()
                    )
                    fields_valid = (
                        truth.all()
                        and symbol_set == expected_symbols == verified_symbols
                        and verified_catalog.snapshot_id == catalog_snapshot.snapshot_id
                        and master_group["directory_source"].astype(str).eq(
                            ETF_DIRECTORY_TRUSTED_SOURCE
                        ).all()
                        and master_group["directory_member_source"].astype(str).eq(
                            ETF_DIRECTORY_TRUSTED_SOURCE
                        ).all()
                        and master_group["directory_freshness"].astype(str).eq("fresh").all()
                        and master_group["directory_master_snapshot_sha256"].astype(str).eq(
                            catalog_snapshot.snapshot_id
                        ).all()
                        and master_group["directory_catalog_snapshot_id"].astype(str).eq(
                            catalog_snapshot.snapshot_id
                        ).all()
                        and master_group["directory_catalog_records_sha256"].astype(str).eq(
                            catalog_evidence["records_sha256"]
                        ).all()
                        and master_group["directory_catalog_file_sha256"].astype(str).eq(
                            catalog_evidence["file_sha256"]
                        ).all()
                        and master_group["directory_catalog_as_of"].astype(str).eq(
                            catalog_evidence["as_of"]
                        ).all()
                        and master_group["directory_attestation_sha256"].astype(str).eq(
                            actual_attestation
                        ).all()
                        and snapshot_key == "etf_directory_" + actual_attestation[:24]
                    )
                    if (
                        fields_valid
                        and counts_valid
                        and master_counts_valid
                        and identity_valid
                        and batch_valid
                    ):
                        complete_snapshots.append(master_group)
                if claimed_complete and not complete_snapshots:
                    raise RuntimeError(
                        f"{target.date()} 的 ETF complete 目录无法通过 immutable artifact 复验"
                    )
            if complete_snapshots:
                cached = max(
                    complete_snapshots,
                    key=lambda frame: frame["_metadata_observed"].max(),
                ).sort_values(["symbol", "_metadata_observed"])
                rich_directory = {
                    str(row.get("symbol") or "").upper(): row
                    for row in cached.to_dict("records")
                }
                identity_columns = (
                    "symbol",
                    "exchange",
                    "asset_type",
                    "status",
                    "list_date",
                    "delist_date",
                )
                for symbol, base in catalog_rows.items():
                    rich = rich_directory.get(symbol, {})
                    merged = {**base, **rich}
                    for column in identity_columns:
                        merged[column] = symbol if column == "symbol" else base.get(column, "")
                    merged["_metadata_effective"] = catalog_effective
                    directory[symbol] = merged
            else:
                cached = pd.DataFrame(directory.values())
            sources = sorted(
                {
                    str(value)
                    for value in cached.get("metadata_source", pd.Series(dtype=str)).dropna()
                    if str(value)
                }
            )
            source_values = cached.get("metadata_source", pd.Series("", index=cached.index)).astype(str)
            enhanced_covered = int(
                source_values.str.contains("tushare:etf_basic", na=False).sum()
            )
            benchmark_covered = int(
                (
                    cached.get("benchmark_code", pd.Series("", index=cached.index))
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .ne("")
                    | cached.get("benchmark", pd.Series("", index=cached.index))
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .ne("")
                ).sum()
            )
            self._profile_capabilities = {
                "status": "ready",
                "source": ", ".join(sources) or ETF_DIRECTORY_TRUSTED_SOURCE,
                "covered_symbols": len(directory),
                "official_covered_symbols": len(directory),
                "enhanced_covered_symbols": enhanced_covered,
                "benchmark_covered_symbols": benchmark_covered,
                "reason": (
                    "不可变 Tushare ETF 目录已复验；etf_basic 增强权限不足"
                    if enhanced_covered < len(directory)
                    else "不可变 Tushare ETF 目录及增强信息均已复验"
                ),
            }
        else:
            self._profile_capabilities = {
                "status": "ready",
                "source": ETF_DIRECTORY_TRUSTED_SOURCE,
                "covered_symbols": len(directory),
                "official_covered_symbols": len(directory),
                "enhanced_covered_symbols": 0,
                "benchmark_covered_symbols": 0,
                "reason": "不可变 Tushare ETF 目录可用；没有可验证的增强元数据",
            }
            cached = pd.DataFrame(directory.values())
        self._profile_metadata_frame = cached.copy()
        result = []
        instruments: list[Instrument] = []
        for symbol, rich in directory.items():
            name = str(rich.get("name") or symbol)
            listed = pd.to_datetime(rich.get("list_date"), errors="coerce")
            delisted = pd.to_datetime(rich.get("delist_date"), errors="coerce")
            if pd.isna(listed) or pd.Timestamp(listed).normalize() > target:
                raise RuntimeError(f"ETF catalog artifact 的 {symbol} list_date 无效")
            if pd.notna(delisted) and target > pd.Timestamp(delisted).normalize():
                raise RuntimeError(f"ETF catalog artifact 的 {symbol} 不属于目标日 universe")
            instruments.append(
                Instrument(
                    symbol=symbol,
                    code=symbol.split(".", 1)[0],
                    name=name,
                    market="CN",
                    exchange=str(rich.get("exchange") or "").upper(),
                    asset_type="etf",
                    status=str(rich.get("status") or ""),
                    source=ETF_DIRECTORY_TRUSTED_SOURCE,
                    list_date=str(rich.get("list_date") or ""),
                    delist_date=str(rich.get("delist_date") or ""),
                )
            )
        for instrument in instruments:
            extra = share_metadata.get(instrument.symbol, {})
            rich = directory.get(instrument.symbol, {})
            raw_list_date = _clean_scalar_text(
                rich.get("list_date"), instrument.list_date,
            )
            listed = pd.to_datetime(raw_list_date, errors="coerce")
            if pd.notna(listed) and pd.Timestamp(listed).normalize() > target:
                continue
            raw_source = _clean_scalar_text(
                rich.get("metadata_source"), self._profile_capabilities.get("source")
            )
            source = (
                "etf_basic"
                if "etf_basic" in raw_source
                else "fund_basic"
                if "tushare:fund_basic" in raw_source
                else "fund_basic"
                if raw_source == ETF_DIRECTORY_TRUSTED_SOURCE
                else "local_stockdb"
            )
            benchmark = _clean_scalar_text(rich.get("benchmark"), extra.get("benchmark"))
            index_name = _clean_scalar_text(
                rich.get("index_name"), rich.get("normalized_index"), benchmark
            )
            benchmark_code = _clean_scalar_text(rich.get("benchmark_code"))
            fund_type = _clean_scalar_text(rich.get("fund_type"), extra.get("fund_type"))
            invest_type = _clean_scalar_text(rich.get("invest_type"), extra.get("invest_type"))
            profile_name = _clean_scalar_text(rich.get("name"), instrument.name)
            taxonomy = classify_etf_profile(
                profile_name,
                benchmark=benchmark,
                benchmark_code=benchmark_code,
                index_name=index_name,
                fund_type=fund_type,
                invest_type=invest_type,
                etf_type=_clean_scalar_text(rich.get("etf_type")),
                benchmark_type=_clean_scalar_text(rich.get("benchmark_type")),
                index_type=_clean_scalar_text(rich.get("index_type")),
                metadata_source=source,
            )
            fee = rich.get("management_fee", rich.get("mgt_fee"))
            numeric_fee = pd.to_numeric(pd.Series([fee]), errors="coerce").iloc[0]
            result.append(
                EtfProfile(
                    symbol=instrument.symbol,
                    name=profile_name,
                    category=taxonomy["category"],
                    asset_class=taxonomy["asset_class"],
                    sector_id=taxonomy["sector_id"],
                    sector_name=taxonomy["sector_name"],
                    benchmark=benchmark,
                    benchmark_code=benchmark_code,
                    benchmark_type=_clean_scalar_text(rich.get("benchmark_type")),
                    benchmark_level=_clean_scalar_text(rich.get("benchmark_level")),
                    index_type=_clean_scalar_text(rich.get("index_type")),
                    index_provider=_clean_scalar_text(rich.get("index_provider")),
                    normalized_index=taxonomy["normalized_index"],
                    fund_type=fund_type,
                    invest_type=invest_type,
                    manager=_clean_scalar_text(rich.get("manager"), rich.get("mgr_name")),
                    custodian=_clean_scalar_text(rich.get("custodian"), rich.get("custod_name")),
                    management_fee=float(numeric_fee) if pd.notna(numeric_fee) else None,
                    metadata_source=source,
                    classification_source=taxonomy["classification_source"],
                    classification_confidence=taxonomy["classification_confidence"],
                    list_date=raw_list_date,
                    metadata_effective_as_of=(
                        pd.Timestamp(rich["_metadata_effective"]).strftime("%Y-%m-%d")
                        if rich.get("_metadata_effective") is not None
                        and pd.notna(rich.get("_metadata_effective"))
                        else str(extra.get("effective_as_of") or "")
                    ),
                    status=instrument.status,
                    classification_evidence=taxonomy["classification_evidence"],
                )
            )
        return sorted(result, key=lambda item: item.symbol)

    @staticmethod
    def _direct_share_observations() -> pd.DataFrame:
        try:
            from quantmaster.rotation.store import RotationStore

            return RotationStore().etf_observations()
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            return pd.DataFrame()

    @staticmethod
    def _direct_metadata() -> pd.DataFrame:
        try:
            from quantmaster.rotation.store import RotationStore

            return RotationStore().etf_metadata()
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            return pd.DataFrame()

    def _adjustment_factors(
        self,
        daily: pd.DataFrame,
        *,
        progress: Progress,
        cancelled: Cancelled,
        as_of: str = "",
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        target = self.store.root / "evidence" / "adjustment_factors.parquet"
        acquired_now = market_now().isoformat()
        historical_cutoff = (
            pd.Timestamp(daily_signal_cutoff(date.fromisoformat(as_of))).tz_convert("UTC")
            if as_of
            else None
        )
        try:
            cached = pd.read_parquet(target) if target.is_file() else pd.DataFrame()
        except (OSError, ValueError):
            cached = pd.DataFrame()
        if not cached.empty:
            cached["date"] = pd.to_datetime(cached.get("date"), errors="coerce")
            cached["symbol"] = cached.get("symbol", pd.Series(dtype=str)).astype(str).str.upper()
            cached["adj_factor"] = pd.to_numeric(cached.get("adj_factor"), errors="coerce")
            if "source" not in cached:
                cached["source"] = "unverified:legacy-factor-cache"
            else:
                cached["source"] = cached["source"].fillna(
                    "unverified:legacy-factor-cache"
                ).astype(str)
            if "acquired_at" not in cached:
                cached["acquired_at"] = pd.NaT
            cached["acquired_at"] = pd.to_datetime(
                cached["acquired_at"], errors="coerce", utc=True,
            )
            cached = cached[
                cached["date"].notna()
                & cached["symbol"].ne("")
                & np.isfinite(cached["adj_factor"])
                & cached["adj_factor"].gt(0)
            ]

        if "adj_factor" in daily:
            embedded = daily[["symbol", "date", "adj_factor"]].copy()
            embedded["date"] = pd.to_datetime(embedded["date"], errors="coerce")
            embedded["adj_factor"] = pd.to_numeric(embedded["adj_factor"], errors="coerce")
            embedded["source"] = (
                "free-stockdb:embedded-factor"
                if daily.attrs.get("adjustment_status") == "verified"
                else "unverified:free-stockdb-embedded-factor"
            )
            embedded["acquired_at"] = acquired_now
            cached = pd.concat([cached, embedded], ignore_index=True)

        grid = daily[["symbol", "date"]].copy()
        grid["symbol"] = grid["symbol"].astype(str).str.upper()
        grid["date"] = pd.to_datetime(grid["date"], errors="coerce").dt.normalize()
        grid = grid.dropna().drop_duplicates().sort_values(["symbol", "date"])
        dates = sorted(grid["date"].unique())
        symbols = sorted(daily["symbol"].dropna().astype(str).str.upper().unique())
        start = pd.Timestamp(dates[0]).date().isoformat() if dates else ""
        end = pd.Timestamp(dates[-1]).date().isoformat() if dates else ""
        expected = grid.groupby("symbol")["date"].nunique()

        def eligible_factors(frame: pd.DataFrame) -> pd.DataFrame:
            if frame is None or frame.empty:
                return pd.DataFrame(
                    columns=["symbol", "date", "adj_factor", "source", "acquired_at"]
                )
            usable = frame.copy()
            usable["adj_factor"] = pd.to_numeric(usable.get("adj_factor"), errors="coerce")
            usable["acquired_at"] = pd.to_datetime(
                usable.get("acquired_at"), errors="coerce", utc=True,
            )
            usable = usable[
                np.isfinite(usable["adj_factor"])
                & usable["adj_factor"].gt(0)
                & usable["acquired_at"].notna()
                & ~usable.get("source", pd.Series("", index=usable.index)).astype(str).str.startswith(
                    "unverified:"
                )
            ]
            if historical_cutoff is not None:
                usable = usable[usable["acquired_at"].le(historical_cutoff)]
            return usable

        def missing_symbols(frame: pd.DataFrame) -> list[str]:
            frame = eligible_factors(frame)
            counts = (
                frame.groupby("symbol")["date"].nunique()
                if frame is not None and not frame.empty
                else pd.Series(dtype=int)
            )
            return [
                symbol
                for symbol in symbols
                if int(counts.get(symbol, 0)) < max(1, round(int(expected.get(symbol, 0)) * 0.95))
            ]

        missing = missing_symbols(cached)
        capability: dict[str, Any] = {
            "status": "ready" if not missing else "partial",
            "source": "adjustment-factor-cache",
            "covered_symbols": len(symbols) - len(missing),
            "expected_symbols": len(symbols),
            "reason": "可核查复权因子已覆盖研究窗口" if not missing else "复权因子缓存覆盖不足",
        }

        if missing and start and end:
            try:
                local_events = self.source.adjustment_factors(missing, start, end)
                if local_events.empty:
                    raise RuntimeError(
                        "stockdb 累计复权事件为空，不能据此证明各产品均无复权事件"
                    )
                local_events = local_events.copy()
                local_events["symbol"] = local_events.get(
                    "symbol", pd.Series(dtype=str)
                ).astype(str).str.upper()
                local_events["date"] = pd.to_datetime(
                    local_events.get("date"), errors="coerce"
                ).dt.normalize()
                local_events["adj_factor"] = pd.to_numeric(
                    local_events.get("adj_factor"), errors="coerce"
                )
                local_events = local_events.dropna(subset=["symbol", "date", "adj_factor"])
                local_events = local_events[
                    np.isfinite(local_events["adj_factor"])
                    & local_events["adj_factor"].gt(0)
                ]
                if local_events.empty:
                    raise RuntimeError("stockdb 累计复权事件没有有限正数证据")
                dense_frames: list[pd.DataFrame] = []
                grid_groups = {
                    str(symbol): group[["symbol", "date"]]
                    for symbol, group in grid.groupby("symbol", sort=False)
                }
                event_groups = {
                    str(symbol): group[["date", "adj_factor"]].sort_values("date")
                    for symbol, group in local_events.groupby("symbol", sort=False)
                }
                for symbol in set(missing).intersection(event_groups):
                    symbol_dates = grid_groups[symbol]
                    events = event_groups[symbol]
                    dense = pd.merge_asof(
                        symbol_dates.sort_values("date"),
                        events,
                        on="date",
                        direction="backward",
                    )
                    dense["symbol"] = symbol
                    dense["adj_factor"] = dense["adj_factor"].fillna(1.0)
                    dense["source"] = "free-stockdb:cum-factor-events"
                    dense["acquired_at"] = acquired_now
                    dense_frames.append(
                        dense[["symbol", "date", "adj_factor", "source", "acquired_at"]]
                    )
                if dense_frames:
                    cached = pd.concat([cached, *dense_frames], ignore_index=True)
                else:
                    raise RuntimeError(
                        "stockdb 未对任何请求产品返回可验证累计复权事件"
                    )
                capability.update(
                    {
                        "source": "free-stockdb:cum-factor-events",
                        "reason": (
                            "仅将 stockdb 明确返回事件的产品展开；"
                            "未返回产品继续保持缺失并尝试官方补证"
                        ),
                    }
                )
                progress(63, "读取本地 ETF 复权证据", f"{len(missing)} 只产品")
            except InterruptedError:
                raise
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
                capability.update(
                    {
                        "status": "partial" if not cached.empty else "unavailable",
                        "reason": f"本地 stockdb 复权证据不可用：{str(exc)[:180]}",
                    }
                )

        missing = missing_symbols(cached)
        if missing:
            capability.update(
                {
                    "status": "partial" if not cached.empty else "unavailable",
                    "reason": (
                        f"{capability.get('reason', '本地复权证据不完整')}；"
                        f"{len(missing)} 只产品使用收益率链或短周期原价降级，"
                        "研究刷新不串行等待远程 fund_adj"
                    ),
                }
            )

        if not cached.empty:
            cached["date"] = pd.to_datetime(cached["date"], errors="coerce")
            cached["symbol"] = cached["symbol"].astype(str).str.upper()
            cached["adj_factor"] = pd.to_numeric(cached["adj_factor"], errors="coerce")
            if "source" not in cached:
                cached["source"] = "unverified:legacy-factor-cache"
            if "acquired_at" not in cached:
                cached["acquired_at"] = pd.NaT
            cached["acquired_at"] = pd.to_datetime(
                cached["acquired_at"], errors="coerce", utc=True,
            )
            cached = (
                cached[
                    cached["date"].notna()
                    & cached["symbol"].ne("")
                    & np.isfinite(cached["adj_factor"])
                    & cached["adj_factor"].gt(0)
                ]
                .drop_duplicates(["symbol", "date", "acquired_at"], keep="last")
                .sort_values(["symbol", "date", "acquired_at"], na_position="first")
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            fd, temp_name = tempfile.mkstemp(
                prefix=".adjustment_factors.", suffix=".parquet.tmp", dir=target.parent
            )
            os.close(fd)
            temp = Path(temp_name)
            try:
                cached.to_parquet(temp, index=False)
                os.replace(temp, target)
            finally:
                temp.unlink(missing_ok=True)
        usable = eligible_factors(cached)
        usable = (
            usable.sort_values(["symbol", "date", "acquired_at"])
            .drop_duplicates(["symbol", "date"], keep="last")
        )
        missing = missing_symbols(usable)
        covered = len(symbols) - len(missing)
        capability["covered_symbols"] = covered
        capability["coverage"] = covered / len(symbols) if symbols else 0.0
        if covered == len(symbols) and symbols:
            capability.update(
                {"status": "ready", "reason": "可核查复权因子已覆盖全部产品研究窗口"}
            )
        return usable, capability

    @staticmethod
    def _minute_metrics(frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for symbol, group in frame.groupby("symbol"):
            values = group.sort_values("date").copy()
            close = pd.to_numeric(values["close"], errors="coerce")
            volume = pd.to_numeric(values["volume"], errors="coerce")
            amount = pd.to_numeric(values.get("amount"), errors="coerce")
            vwap = amount.sum() / volume.sum() if volume.sum() > 0 and amount.notna().any() else np.nan
            returns = close.pct_change().replace([np.inf, -np.inf], np.nan)
            times = pd.to_datetime(values["date"], errors="coerce")
            total_amount = amount.sum()
            first = amount[times.dt.time <= pd.Timestamp("10:30").time()].sum()
            last = amount[times.dt.time >= pd.Timestamp("14:00").time()].sum()
            result[str(symbol)] = {
                "rows": len(values),
                "complete_session": len(values) >= 240,
                "vwap_deviation": float(close.iloc[-1] / vwap - 1) if vwap and np.isfinite(vwap) else None,
                "realized_volatility": float(returns.std()) if returns.notna().any() else None,
                "intraday_drawdown": float((close / close.cummax() - 1).min())
                if close.notna().any()
                else None,
                "first_hour_amount_share": float(first / total_amount) if total_amount > 0 else None,
                "last_hour_amount_share": float(last / total_amount) if total_amount > 0 else None,
                "scoring_input": False,
            }
        return result

    def intraday(self, symbol: str, *, as_of_date: str) -> dict[str, Any]:
        """Read and cache one ETF minute series only when its trend view requests it."""

        canonical = str(symbol or "").strip().upper()
        if _EXCHANGE_ETF_SYMBOL.fullmatch(canonical) is None:
            raise ValueError("ETF 代码格式无效")
        session = pd.Timestamp(as_of_date).date().isoformat()
        session_start = pd.Timestamp(f"{session} 09:30:00")
        session_end = pd.Timestamp(f"{session} 15:00:00")

        def session_only(value: pd.DataFrame) -> pd.DataFrame:
            if value is None or value.empty:
                return pd.DataFrame()
            result = value.copy()

            def local_stamp(raw: Any) -> pd.Timestamp:
                parsed = pd.to_datetime(raw, errors="coerce")
                if pd.isna(parsed):
                    return pd.NaT
                stamp = pd.Timestamp(parsed)
                if stamp.tzinfo is not None:
                    stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
                return stamp

            result["date"] = result.get(
                "date", pd.Series(pd.NaT, index=result.index)
            ).map(local_stamp)
            return result[
                result["date"].notna()
                & result["date"].between(session_start, session_end)
            ].copy()

        evidence_root = (self.store.root / "evidence" / "intraday").resolve()
        safe_symbol = canonical.replace(".", "_")
        target = confined_path(
            evidence_root,
            f"{safe_symbol}_{session}.parquet",
            label="ETF 分钟证据",
        )
        frame = pd.DataFrame()
        cache_hit = False
        if target.is_file():
            try:
                frame = session_only(pd.read_parquet(target))
                cache_hit = not frame.empty
            except (OSError, ValueError):
                frame = pd.DataFrame()
        if frame.empty:
            start = f"{session} 09:30:00"
            end = f"{session} 15:00:00"
            frame = session_only(
                self.source.intraday_many([canonical], start, end, "1m")
            )
            if not frame.empty:
                evidence_root.mkdir(parents=True, exist_ok=True)
                fd, temp_name = tempfile.mkstemp(
                    prefix=".intraday.",
                    suffix=".parquet.tmp",
                    dir=evidence_root,
                )
                os.close(fd)
                temp = Path(temp_name)
                try:
                    frame.to_parquet(temp, index=False)
                    os.replace(temp, target)
                finally:
                    temp.unlink(missing_ok=True)
        if frame.empty:
            return {
                "symbol": canonical,
                "date": session,
                "status": "missing",
                "source": "free-stockdb",
                "cache_hit": cache_hit,
                "metrics": {"rows": 0, "complete_session": False, "scoring_input": False},
                "series": [],
            }
        values = session_only(frame)
        if "symbol" not in values:
            values["symbol"] = canonical
        for column in ("close", "volume", "amount"):
            if column not in values:
                values[column] = np.nan
        values["symbol"] = values["symbol"].astype(str).str.upper()
        values = values[values["symbol"].eq(canonical)]
        values["date"] = pd.to_datetime(values.get("date"), errors="coerce")
        values = values.dropna(subset=["date"]).sort_values("date")
        metrics = self._minute_metrics(values).get(
            canonical,
            {"rows": len(values), "complete_session": False, "scoring_input": False},
        )
        return {
            "symbol": canonical,
            "date": session,
            "status": "ready" if metrics.get("complete_session") else "partial",
            "source": "free-stockdb",
            "cache_hit": cache_hit,
            "metrics": metrics,
            "series": [
                {
                    "time": row.date.isoformat(timespec="minutes"),
                    "close": float(row.close) if pd.notna(row.close) else None,
                    "volume": float(row.volume) if pd.notna(row.volume) else None,
                    "amount": float(row.amount) if pd.notna(row.amount) else None,
                }
                for row in values[["date", "close", "volume", "amount"]].itertuples(index=False)
            ],
        }

    def product_history(
        self,
        symbol: str,
        *,
        snapshot_id: str,
        tier: str = "production",
    ) -> list[dict[str, Any]]:
        """Replay history exclusively from evidence frozen by the selected tier."""

        canonical = str(symbol or "").upper()
        selected_tier = self._research_tier(tier)
        preview = selected_tier == "sandbox"
        snapshot = self.resolve_snapshot(str(snapshot_id or ""), tier=selected_tier)
        if snapshot is None:
            raise RuntimeError(f"ETF 研究快照不存在或契约已淘汰: {snapshot_id}")
        if snapshot.snapshot_id != str(snapshot_id or ""):
            raise RuntimeError(
                "ETF 快照路径与内部标识不匹配: "
                f"requested={snapshot_id}, embedded={snapshot.snapshot_id}"
            )
        if not any(item.symbol == canonical for item in snapshot.items):
            raise RuntimeError(f"ETF 不在指定研究快照中: {canonical}")
        input_evidence: dict[str, Any] = {
            "ingest_id": snapshot.ingest_id,
            "research_model_version": snapshot.research_model_version,
            "evidence_hashes": snapshot.evidence_hashes,
        }
        if preview:
            if snapshot.tier != "sandbox" or snapshot.formal_eligible:
                raise RuntimeError("ETF sandbox preview 发布资格契约无效")
            input_evidence.update({"tier": "sandbox", "formal_eligible": False})
        elif snapshot.tier != "production" or not snapshot.formal_eligible:
            raise RuntimeError("ETF production 快照发布资格契约无效")
        expected_input_hash = content_hash(input_evidence)
        if expected_input_hash != snapshot.input_hash:
            raise RuntimeError(
                f"ETF 快照输入哈希不匹配: snapshot={snapshot.input_hash}, actual={expected_input_hash}"
            )
        expected_snapshot_id = (
            ("etf_preview_" if preview else "etf_")
            + hashlib.sha256(
                f"{snapshot.as_of_date}:{snapshot.research_model_version}:{snapshot.input_hash}".encode()
            ).hexdigest()[:24]
        )
        if expected_snapshot_id != snapshot.snapshot_id:
            raise RuntimeError(
                f"ETF 快照标识不匹配: snapshot={snapshot.snapshot_id}, actual={expected_snapshot_id}"
            )
        key = (canonical, snapshot.snapshot_id, snapshot.input_hash)
        cached = self._detail_history_cache.get(key)
        if cached is not None:
            return cached

        if preview:
            with self._preview_lock:
                daily = self._preview_daily.get(snapshot.snapshot_id, pd.DataFrame()).copy()
                factors = self._preview_factors.get(snapshot.snapshot_id, pd.DataFrame()).copy()
            if daily.empty:
                raise RuntimeError(f"ETF sandbox preview 行情已过期: {snapshot.snapshot_id}")
            actual_daily_hash = _stockdb_frame_hash(daily)
            if actual_daily_hash != snapshot.evidence_hashes.get("行情明细"):
                raise RuntimeError("ETF sandbox preview 行情内存证据哈希不匹配")
            actual_factor_hash = _frame_hash(factors, _ADJUSTMENT_COLUMNS)
            if actual_factor_hash != snapshot.evidence_hashes.get("复权"):
                raise RuntimeError("ETF sandbox preview 复权内存证据哈希不匹配")
            end = pd.Timestamp(snapshot.as_of_date).normalize()
            daily["symbol"] = daily.get("symbol", pd.Series(dtype=str)).astype(str).str.upper()
            daily["date"] = pd.to_datetime(daily.get("date"), errors="coerce")
            daily = daily[
                daily["symbol"].eq(canonical)
                & daily["date"].notna()
                & daily["date"].le(end)
            ]
            if daily.empty:
                raise RuntimeError(
                    f"ETF sandbox preview 行情中没有 {canonical} 截至 {snapshot.as_of_date} 的记录"
                )
            if not factors.empty and "symbol" in factors:
                factors = factors[factors["symbol"].astype(str).str.upper().eq(canonical)]
            history = adjusted_daily_metrics(daily, factors).get("history") or []
            self._detail_history_cache.clear()
            self._detail_history_cache[key] = history
            return history

        ingest = self.ingest_store.get(snapshot.ingest_id)
        if ingest is None:
            raise RuntimeError(
                f"ETF 快照引用的 StockDB 摄取已缺失: {snapshot.ingest_id}"
            )
        if ingest.ingest_id != snapshot.ingest_id:
            raise RuntimeError(
                "StockDB 摄取清单路径与内部标识不匹配: "
                f"requested={snapshot.ingest_id}, embedded={ingest.ingest_id}"
            )
        expected_ingest_id = "sdi_" + content_hash(
            {
                "schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                "asset": "etf",
                "as_of_date": ingest.as_of_date,
                "artifact_id": ingest.artifact_id,
                "master_snapshot_id": ingest.master_snapshot_id,
                "start_date": ingest.start_date,
                "end_date": ingest.end_date,
                "content_hashes": ingest.content_hashes,
                "coverage": ingest.coverage,
                "session_dates": list(ingest.session_dates),
                "session_source": ingest.session_source,
            }
        )[:24]
        if expected_ingest_id != ingest.ingest_id:
            raise RuntimeError(
                "StockDB 摄取清单内容哈希不匹配: "
                f"manifest={ingest.ingest_id}, actual={expected_ingest_id}"
            )
        if ingest.as_of_date != snapshot.as_of_date:
            raise RuntimeError(
                "ETF 快照与 StockDB 摄取日期不一致: "
                f"snapshot={snapshot.as_of_date}, ingest={ingest.as_of_date}"
            )
        expected_market_hash = content_hash(ingest.content_hashes)
        if snapshot.evidence_hashes.get("行情") != expected_market_hash:
            raise RuntimeError(
                "ETF 快照行情证据哈希不匹配: "
                f"snapshot={snapshot.evidence_hashes.get('行情') or 'missing'}, "
                f"ingest={expected_market_hash}"
            )
        daily = self.ingest_store.load_frame(ingest, "etf_daily")
        expected_daily_hash = str(ingest.content_hashes.get("etf_daily") or "")
        if not expected_daily_hash or daily.empty:
            raise RuntimeError(f"ETF 快照的冻结行情证据缺失: {snapshot.ingest_id}")
        actual_daily_hash = _stockdb_frame_hash(daily)
        if actual_daily_hash != expected_daily_hash:
            raise RuntimeError(
                "ETF 冻结行情证据哈希不匹配: "
                f"snapshot={expected_daily_hash}, actual={actual_daily_hash}"
            )

        factors = self.store.load_frozen_adjustments(
            snapshot.evidence_hashes.get("复权", "")
        )
        end = pd.Timestamp(snapshot.as_of_date).normalize()
        daily["symbol"] = daily.get("symbol", pd.Series(dtype=str)).astype(str).str.upper()
        daily["date"] = pd.to_datetime(daily.get("date"), errors="coerce")
        daily = daily[
            daily["symbol"].eq(canonical)
            & daily["date"].notna()
            & daily["date"].le(end)
        ]
        if daily.empty:
            raise RuntimeError(f"ETF 冻结行情中没有 {canonical} 截至 {snapshot.as_of_date} 的记录")
        if not factors.empty and "symbol" in factors:
            factors = factors[factors["symbol"].astype(str).str.upper().eq(canonical)]
        history = adjusted_daily_metrics(daily, factors).get("history") or []
        self._detail_history_cache.clear()
        self._detail_history_cache[key] = history
        return history

    def scan(
        self,
        *,
        as_of: str = "",
        tier: str = "production",
        progress: Progress | None = None,
        cancelled: Cancelled | None = None,
        refresh_warnings: list[str] | tuple[str, ...] = (),
    ) -> EtfResearchSnapshot:
        with self._scan_lock:
            return self._scan(
                as_of=as_of,
                tier=tier,
                progress=progress,
                cancelled=cancelled,
                refresh_warnings=refresh_warnings,
            )

    def _scan(
        self,
        *,
        as_of: str = "",
        tier: str = "production",
        progress: Progress | None = None,
        cancelled: Cancelled | None = None,
        refresh_warnings: list[str] | tuple[str, ...] = (),
    ) -> EtfResearchSnapshot:
        cfg = get_config().data
        if not cfg.free_stockdb_etf_research_enabled:
            raise RuntimeError("ETF 研究已在设置中停用")
        progress = progress or (lambda *_: None)
        cancelled = cancelled or (lambda: False)
        selected_tier = self._research_tier(tier)
        end, target_source = self._research_target(as_of)
        progress(3, "确定 ETF 研究日", f"{end.date()} · {target_source}")
        profiles = self.profiles(as_of=as_of, tier=selected_tier)
        self._profile_capabilities.setdefault("target_source", target_source)
        self._profile_capabilities.setdefault("target_date", end.date().isoformat())
        if not profiles:
            raise RuntimeError("证券主数据中没有沪深场内 ETF")
        start = end - pd.DateOffset(years=3, days=20)
        symbols = [item.symbol for item in profiles]
        master_id = "etf_master_" + content_hash([item.to_dict() for item in profiles])[:24]
        data_session = StockDBIngestService._data_session(str(end.date()))
        identity = self.source.artifact_identity(data_session=data_session)
        cache_key = content_hash(
            {
                "schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                "asset": "etf",
                "artifact": identity.artifact_id,
                "master": master_id,
                "start": str(start.date()),
                "end": str(end.date()),
                "symbols": symbols,
                "etf_research_schema": ETF_SCHEMA_VERSION,
                "tier": selected_tier,
            }
        )
        ingest_history = (
            self.ingest_store.history(100)
            if selected_tier == "production"
            else []
        )
        ingest = (
            next(
                (
                    item
                    for item in ingest_history
                    if item.provenance.get("cache_key") == cache_key and "etf" in item.assets
                ),
                None,
            )
            if selected_tier == "production"
            else None
        )
        daily = pd.DataFrame()
        if ingest is not None:
            daily = self.ingest_store.load_frame(ingest, "etf_daily")
        if daily.empty and selected_tier == "production":
            target_symbols = set(symbols)
            compatible = None
            historical_cutoff = (
                pd.Timestamp(daily_signal_cutoff(end.date())).tz_convert("UTC")
                if as_of
                else None
            )
            for candidate in ingest_history:
                if (
                    candidate.status != "complete"
                    or "etf" not in candidate.assets
                    or candidate.artifact_id != identity.artifact_id
                    or candidate.start_date > str(start.date())
                    or candidate.end_date < str(end.date())
                ):
                    continue
                candidate_created = pd.to_datetime(
                    candidate.created_at,
                    errors="coerce",
                    utc=True,
                )
                if historical_cutoff is not None and (
                    pd.isna(candidate_created)
                    or pd.Timestamp(candidate_created) > historical_cutoff
                ):
                    continue
                try:
                    cached_profiles = self.ingest_store.load_json(candidate, "etf_profiles")
                except (OSError, TypeError, ValueError, json.JSONDecodeError):
                    continue
                cached_symbols = {
                    str(item.get("symbol") or "").upper()
                    for item in cached_profiles
                    if isinstance(item, dict) and item.get("symbol")
                }
                if cached_symbols == target_symbols:
                    compatible = candidate
                    break
            if compatible is not None:
                daily = self.ingest_store.load_frame(compatible, "etf_daily")
                if not daily.empty:
                    daily = daily.copy()
                    daily["symbol"] = daily.get(
                        "symbol", pd.Series(dtype=str)
                    ).astype(str).str.upper()
                    daily["date"] = pd.to_datetime(
                        daily.get("date"), errors="coerce"
                    )
                    daily = daily[
                        daily["symbol"].isin(target_symbols)
                        & daily["date"].notna()
                        & daily["date"].ge(start)
                        & daily["date"].le(end)
                    ].copy()
                    observed_symbols = set(daily["symbol"].unique())
                    if daily.empty or observed_symbols != target_symbols:
                        daily = pd.DataFrame()
                        compatible = None
                if compatible is not None and not daily.empty:
                    actual = daily["date"].max().date().isoformat()
                    session_dates = sorted(
                        daily["date"]
                        .dropna()
                        .dt.strftime("%Y-%m-%d")
                        .unique()
                        .tolist()
                    )
                    coverage = {
                        **compatible.coverage,
                        "status": "complete",
                        "symbol_ratio": 1.0,
                        "requested_symbols": len(target_symbols),
                        "observed_symbols": len(observed_symbols),
                        "start": str(start.date()),
                        "end": actual,
                    }
                    ingest = self.ingest_store.publish_etf(
                        daily=daily,
                        minutes=pd.DataFrame(),
                        profiles=[item.to_dict() for item in profiles],
                        as_of_date=actual,
                        artifact_id=identity.artifact_id,
                        master_snapshot_id=master_id,
                        start_date=str(start.date()),
                        end_date=actual,
                        coverage=coverage,
                        provenance={
                            **compatible.provenance,
                            "cache_key": cache_key,
                            "profile_refresh_from": compatible.ingest_id,
                        },
                        session_dates=session_dates,
                        session_source=compatible.session_source,
                    )
                    progress(5, "复用 ETF 日线", f"元数据重算复用 {compatible.ingest_id}")
        if daily.empty:
            frames = []
            for offset in range(0, len(symbols), 300):
                if cancelled():
                    raise InterruptedError("ETF 研究扫描已取消")
                batch = symbols[offset : offset + 300]
                frames.append(
                    self.source.daily_cross_section(
                        batch,
                        str(start.date()),
                        str(end.date()),
                    )
                )
                progress(
                    5 + int(50 * (offset + len(batch)) / len(symbols)),
                    "读取 ETF 日线",
                    f"{offset + len(batch)}/{len(symbols)}",
                )
            daily = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            if daily.empty:
                raise RuntimeError("free-stockdb 没有返回 ETF 日频截面")
            daily = daily.copy()
            daily["symbol"] = daily.get("symbol", pd.Series(dtype=str)).astype(str).str.upper()
            daily["date"] = pd.to_datetime(daily.get("date"), errors="coerce")
            daily = daily[
                daily["symbol"].isin(symbols)
                & daily["date"].notna()
                & daily["date"].ge(start)
                & daily["date"].le(end)
            ].copy()
            if daily.empty:
                raise RuntimeError("free-stockdb 没有返回目标日之前的 ETF 日频截面")
            if selected_tier == "sandbox" and as_of:
                for _alignment_attempt in range(4):
                    actual_end = pd.Timestamp(daily["date"].max()).normalize()
                    if actual_end == end:
                        break
                    end = actual_end
                    profiles = self.profiles(
                        as_of=end.date().isoformat(),
                        tier="sandbox",
                    )
                    if not profiles:
                        raise RuntimeError("实际行情日没有可复验的本地 ETF 分析母集")
                    symbols = [item.symbol for item in profiles]
                    start = end - pd.DateOffset(years=3, days=20)
                    aligned_frames = []
                    for offset in range(0, len(symbols), 300):
                        if cancelled():
                            raise InterruptedError("ETF 研究扫描已取消")
                        batch = symbols[offset : offset + 300]
                        aligned_frames.append(
                            self.source.daily_cross_section(
                                batch,
                                str(start.date()),
                                str(end.date()),
                            )
                        )
                    daily = (
                        pd.concat(aligned_frames, ignore_index=True)
                        if aligned_frames
                        else pd.DataFrame()
                    )
                    if daily.empty:
                        raise RuntimeError("实际行情日没有返回 ETF 日频截面")
                    daily["symbol"] = daily.get(
                        "symbol", pd.Series(dtype=str)
                    ).astype(str).str.upper()
                    daily["date"] = pd.to_datetime(daily.get("date"), errors="coerce")
                    daily = daily[
                        daily["symbol"].isin(symbols)
                        & daily["date"].notna()
                        & daily["date"].ge(start)
                        & daily["date"].le(end)
                    ].copy()
                    if daily.empty:
                        raise RuntimeError("实际行情日没有可用的 ETF 日频截面")
                else:
                    raise RuntimeError("ETF sandbox 无法对齐行情日与证据截止时点")
                master_id = "etf_master_" + content_hash(
                    [item.to_dict() for item in profiles]
                )[:24]
                data_session = StockDBIngestService._data_session(str(end.date()))
                identity = self.source.artifact_identity(data_session=data_session)
                cache_key = content_hash(
                    {
                        "schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                        "asset": "etf",
                        "artifact": identity.artifact_id,
                        "master": master_id,
                        "start": str(start.date()),
                        "end": str(end.date()),
                        "symbols": symbols,
                        "etf_research_schema": ETF_SCHEMA_VERSION,
                        "tier": selected_tier,
                    }
                )
            actual = pd.Timestamp(daily["date"].max()).date().isoformat()
            latest = daily[
                daily["date"].dt.date == date.fromisoformat(actual)
            ]
            observed = int(latest["symbol"].nunique())
            ratio = observed / len(symbols)
            required_ratio = float(
                latest[["open", "high", "low", "close", "volume"]].notna().all(axis=1).mean()
            )
            if selected_tier == "production" and (ratio < 0.80 or required_ratio < 0.95):
                raise RuntimeError(
                    f"ETF 完整性门未通过：覆盖 {observed}/{len(symbols)}，OHLCV {required_ratio:.1%}"
                )
            coverage = {
                "status": (
                    "complete"
                    if ratio >= 0.80 and required_ratio >= 0.95
                    and selected_tier == "production"
                    else "degraded"
                ),
                "expected_symbols": len(symbols),
                "observed_symbols": observed,
                "symbol_ratio": round(ratio, 6),
                "required_ohlcv_ratio": round(required_ratio, 6),
                "tier": selected_tier,
                "formal_eligible": selected_tier == "production",
            }
            etf_sessions = sorted(
                pd.to_datetime(
                    daily["date"],
                    errors="coerce",
                )
                .dropna()
                .dt.strftime("%Y-%m-%d")
                .unique()
                .tolist()
            )
            coverage["fields"] = StockDBIngestService.field_contracts(
                daily,
                actual,
                asset_class="etf",
                source=self.source.name,
            )
            ingest_provenance = {
                "cache_key": cache_key,
                "upstream": "vendor-declared-unverified",
                "upstream_evidence": "not_provided",
                "distribution": "free-stockdb",
                "artifact": identity.to_dict(),
                "ingest_schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                "price_storage": "raw",
                "tier": selected_tier,
            }
            if selected_tier == "production":
                ingest = self.ingest_store.publish_etf(
                    daily=daily,
                    minutes=pd.DataFrame(),
                    profiles=[item.to_dict() for item in profiles],
                    as_of_date=actual,
                    artifact_id=identity.artifact_id,
                    master_snapshot_id=master_id,
                    start_date=str(start.date()),
                    end_date=str(end.date()),
                    coverage=coverage,
                    provenance=ingest_provenance,
                    session_dates=etf_sessions,
                    session_source="stockdb_broad_coverage",
                )
            else:
                content_hashes = {
                    "etf_daily": _stockdb_frame_hash(daily),
                    "etf_minutes": _stockdb_frame_hash(pd.DataFrame()),
                    "etf_profiles": content_hash([item.to_dict() for item in profiles]),
                }
                logical = {
                    "schema_version": STOCKDB_INGEST_SCHEMA_VERSION,
                    "asset": "etf-preview",
                    "as_of_date": actual,
                    "artifact_id": identity.artifact_id,
                    "master_snapshot_id": master_id,
                    "start_date": str(start.date()),
                    "end_date": str(end.date()),
                    "content_hashes": content_hashes,
                    "coverage": coverage,
                    "session_dates": etf_sessions,
                    "session_source": "stockdb_local_preview",
                }
                ingest = StockDBIngestSnapshot(
                    ingest_id="preview_sdi_" + content_hash(logical)[:24],
                    as_of_date=actual,
                    artifact_id=identity.artifact_id,
                    master_snapshot_id=master_id,
                    start_date=str(start.date()),
                    end_date=str(end.date()),
                    assets={
                        "etf": {
                            "daily_rows": len(daily),
                            "minute_rows": 0,
                            "symbols": len(profiles),
                        }
                    },
                    coverage=coverage,
                    content_hashes=content_hashes,
                    provenance=ingest_provenance,
                    session_dates=tuple(etf_sessions),
                    session_source="stockdb_local_preview",
                    status="degraded",
                    issues=("本地母集不代表完整市场目录",),
                )
        if ingest is None:
            raise RuntimeError("ETF 行情摄取未生成")
        actual = ingest.as_of_date
        daily["symbol"] = daily["symbol"].astype(str).str.upper()
        daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
        session_dates = sorted(daily["date"].dropna().dt.date.astype(str).unique().tolist())
        factors, adjustment_capability = self._adjustment_factors(
            daily, progress=progress, cancelled=cancelled, as_of=as_of
        )
        direct = self._direct_share_observations()
        if not direct.empty:
            direct["trade_date"] = pd.to_datetime(direct["trade_date"], errors="coerce")
            direct["symbol"] = direct["symbol"].astype(str).str.upper()
            eligible_direct = direct["trade_date"].dt.date <= date.fromisoformat(actual)
            if as_of:
                acquired = pd.to_datetime(
                    direct.get("acquired_at"), errors="coerce", utc=True,
                )
                cutoff = pd.Timestamp(
                    daily_signal_cutoff(date.fromisoformat(actual))
                ).tz_convert("UTC")
                eligible_direct &= acquired.notna() & acquired.le(cutoff)
                direct = direct.assign(acquired_at=acquired)
            direct = direct.loc[eligible_direct]
        metadata_cache = self._profile_metadata_frame.copy()
        # Keep a separate refresh-only fingerprint for the authoritative metadata
        # input.  It is not used to build a historical profile (so it cannot
        # introduce look-ahead), but it lets the UI notice a changed local source
        # and request an explicit rescan for either current or historical views.
        metadata_source_cache = self._direct_metadata()
        evidence_hashes = {
            "行情": content_hash(ingest.content_hashes),
            "份额": _frame_hash(
                direct,
                (
                    "symbol",
                    "trade_date",
                    "shares",
                    "total_size",
                    "nav",
                    "close",
                    "share_source",
                    "source",
                ),
            ),
            "复权": _frame_hash(
                factors, ("symbol", "date", "adj_factor", "source", "acquired_at")
            ),
            "元数据": (
                _frame_hash(
                    metadata_cache,
                    (
                        "symbol",
                        "name",
                        "benchmark",
                        "benchmark_code",
                        "benchmark_type",
                        "benchmark_level",
                        "index_type",
                        "index_provider",
                        "fund_type",
                        "invest_type",
                        "mgt_fee",
                        "metadata_source",
                    ),
                )
                if not metadata_cache.empty
                else content_hash([profile.to_dict() for profile in profiles])
            ),
        }
        evidence_hashes["元数据源"] = _frame_hash(
            metadata_source_cache,
            (
                "symbol",
                "name",
                "benchmark",
                "benchmark_code",
                "benchmark_type",
                "benchmark_level",
                "index_type",
                "index_provider",
                "fund_type",
                "invest_type",
                "mgt_fee",
                "metadata_source",
            ),
        )
        denominator = self._profile_capabilities.get("denominator") or {}
        if denominator:
            evidence_hashes["母集"] = content_hash(
                denominator
            )
        if selected_tier == "sandbox":
            evidence_hashes["行情明细"] = _stockdb_frame_hash(daily)
        else:
            self.store.freeze_adjustments(factors, evidence_hashes["复权"])
        input_evidence: dict[str, Any] = {
            "ingest_id": ingest.ingest_id,
            "research_model_version": ETF_RESEARCH_MODEL_VERSION,
            "evidence_hashes": evidence_hashes,
        }
        if selected_tier == "sandbox":
            input_evidence.update({"tier": "sandbox", "formal_eligible": False})
        input_hash = content_hash(input_evidence)
        snapshot_id = (
            ("etf_" if selected_tier == "production" else "etf_preview_")
            + hashlib.sha256(f"{actual}:{ETF_RESEARCH_MODEL_VERSION}:{input_hash}".encode()).hexdigest()[:24]
        )
        existing = self.store.get(snapshot_id) if selected_tier == "production" else None
        if existing is not None:
            existing = self.store.publish(existing)
            self.ingest_store.pin(
                existing.ingest_id,
                "etf_research",
                existing.snapshot_id,
                {"as_of_date": existing.as_of_date},
            )
            progress(100, "复用 ETF 板块研究", existing.snapshot_id)
            return existing

        progress(70, "计算 ETF 板块证据", "趋势、位置、活跃度分别公开")
        metric_columns = [
            column
            for column in ("symbol", "date", "close", "pct_chg", "amount", "adj_factor")
            if column in daily
        ]
        metric_daily = daily[metric_columns].copy()
        metric_daily = (
            metric_daily.dropna(subset=["symbol", "date"])
            .sort_values(["symbol", "date"])
            .drop_duplicates(["symbol", "date"], keep="last")
        )
        for column in ("close", "pct_chg", "amount", "adj_factor"):
            if column in metric_daily:
                metric_daily[column] = pd.to_numeric(metric_daily[column], errors="coerce")
        daily_groups = {
            str(symbol): group for symbol, group in metric_daily.groupby("symbol", sort=False)
        }
        metric_factors = factors
        if not factors.empty:
            factor_columns = [
                column for column in ("symbol", "date", "adj_factor", "source") if column in factors
            ]
            metric_factors = (
                factors[factor_columns]
                .sort_values(["symbol", "date"])
                .drop_duplicates(["symbol", "date"], keep="last")
            )
        factor_groups = (
            {
                str(symbol): group
                for symbol, group in metric_factors.groupby("symbol", sort=False)
            }
            if not metric_factors.empty
            else {}
        )
        session_index = {value: index for index, value in enumerate(session_dates)}
        direct_groups: dict[str, pd.DataFrame] = {}
        if not direct.empty:
            prepared_direct = direct.copy()
            prepared_direct["shares"] = pd.to_numeric(
                prepared_direct.get("shares"), errors="coerce"
            )
            prepared_direct = (
                prepared_direct.dropna(subset=["symbol", "trade_date", "shares"])
                .sort_values(["symbol", "trade_date"])
                .drop_duplicates(["symbol", "trade_date"], keep="last")
            )
            direct_groups = {
                str(symbol): group
                for symbol, group in prepared_direct.groupby("symbol", sort=False)
            }
        rows: list[dict[str, Any]] = []
        for profile in profiles:
            metric = adjusted_daily_metrics(
                daily_groups.get(profile.symbol, pd.DataFrame()),
                factor_groups.get(profile.symbol),
                prepared=True,
            )
            observations = direct_groups.get(profile.symbol, pd.DataFrame())
            funds = fund_evidence(
                observations,
                as_of_date=actual,
                session_dates=session_dates,
                fallback_price=metric.get("close"),
                session_index=session_index,
                prepared=True,
            )
            total_size = None
            if not observations.empty and "total_size" in observations:
                sizes = pd.to_numeric(observations["total_size"], errors="coerce").dropna()
                total_size = float(sizes.iloc[-1]) if not sizes.empty else None
            if total_size is None and funds.get("share") is not None and metric.get("close") is not None:
                total_size = float(funds["share"] * metric["close"])
            rows.append(
                {
                    "profile": profile,
                    "metrics": metric,
                    "funds": funds,
                    "total_size": total_size,
                }
            )
        sectors, representative_by_symbol, queues, candidate_queues, summaries = build_sector_research(rows)
        items: list[EtfResearchItem] = []
        for row in rows:
            profile = row["profile"]
            metrics = {key: value for key, value in row["metrics"].items() if key != "history"}
            representative_symbol = representative_by_symbol[profile.symbol]
            items.append(
                EtfResearchItem(
                    symbol=profile.symbol,
                    name=profile.name,
                    category=profile.category,
                    asset_class=profile.asset_class,
                    sector_id=profile.sector_id,
                    sector_name=profile.sector_name,
                    normalized_index=profile.normalized_index,
                    benchmark_code=profile.benchmark_code,
                    is_representative=profile.symbol == representative_symbol,
                    representative_symbol=representative_symbol,
                    metrics=metrics,
                    funds=row["funds"],
                    metadata={
                        "manager": profile.manager,
                        "custodian": profile.custodian,
                        "management_fee": profile.management_fee,
                        "total_size": row["total_size"],
                        "benchmark_type": profile.benchmark_type,
                        "benchmark_level": profile.benchmark_level,
                        "index_type": profile.index_type,
                        "index_provider": profile.index_provider,
                        "list_date": profile.list_date,
                        "metadata_effective_as_of": profile.metadata_effective_as_of,
                        "classification_confidence": profile.classification_confidence,
                        "classification_evidence": profile.classification_evidence,
                    },
                    coverage={
                        "daily": profile.symbol in daily_groups,
                        "adjustment": metrics.get("adjustment_status")
                        in {"official", "verified_local"},
                        "shares": row["funds"].get("status") in {"confirmed_zero", "confirmed_change"},
                    },
                    provenance={
                        "price": "free-stockdb:vendor-upstream-unverified",
                        "adjustment": metrics.get("adjustment_source") or "unavailable",
                        "shares": row["funds"].get("source") or "unavailable",
                        "metadata": profile.metadata_source,
                        "classification": profile.classification_source,
                    },
                    as_of_date=actual,
                    snapshot_id=snapshot_id,
                    ingest_id=ingest.ingest_id,
                    artifact_id=ingest.artifact_id,
                )
            )
        items.sort(key=lambda item: (ETF_CATEGORIES.index(item.category), item.sector_name, item.symbol))

        share_date = (
            direct["trade_date"].max().date().isoformat()
            if not direct.empty and direct["trade_date"].notna().any()
            else ""
        )
        factor_date = (
            factors["date"].max().date().isoformat()
            if not factors.empty and factors["date"].notna().any()
            else ""
        )
        metadata_date = ""
        if not metadata_cache.empty and "updated_at" in metadata_cache:
            parsed_metadata_dates = pd.to_datetime(metadata_cache["updated_at"], errors="coerce").dropna()
            if not parsed_metadata_dates.empty:
                metadata_date = parsed_metadata_dates.max().date().isoformat()
        confirmed_shares = sum(
            item.funds.get("status") in {"confirmed_zero", "confirmed_change"} for item in items
        )
        verified_adjustments = sum(
            item.metrics.get("adjustment_status") in {"official", "verified_local"}
            for item in items
        )
        usable_metadata = sum(bool(item.name and item.sector_name) for item in items)
        official_metadata = sum(
            item.provenance.get("metadata") in {"etf_basic", "fund_basic"} for item in items
        )
        enhanced_metadata = sum(
            item.provenance.get("metadata") == "etf_basic" for item in items
        )
        effective_refresh_warnings = list(refresh_warnings)
        if self._profile_capabilities.get("status") == "degraded":
            metadata_warning = str(self._profile_capabilities.get("reason") or "").strip()
            if metadata_warning and metadata_warning not in effective_refresh_warnings:
                effective_refresh_warnings.append(metadata_warning)
        freshness = {
            "research": {
                "date": actual,
                "status": "ready" if selected_tier == "production" else "degraded",
                "coverage": 1.0,
                "requested_ceiling": end.date().isoformat(),
                "source": target_source,
            },
            "market": {
                "date": actual,
                "status": (
                    "ready"
                    if ingest.coverage.get("status") == "complete"
                    else "degraded"
                ),
                "coverage": float(ingest.coverage.get("symbol_ratio") or 0),
                "source": "free-stockdb",
            },
            "shares": {
                "date": share_date,
                "status": "ready" if share_date == actual else ("stale" if share_date else "missing"),
                "coverage": confirmed_shares / len(items) if items else 0.0,
                "source": "etf_share_size/fund_share",
            },
            "adjustment": {
                "date": factor_date,
                "status": adjustment_capability["status"],
                "coverage": verified_adjustments / len(items) if items else 0.0,
                "source": adjustment_capability.get("source", "adjustment-factor-cache"),
            },
            "metadata": {
                "date": metadata_date,
                "status": (
                    self._profile_capabilities.get("status", "fallback")
                    if metadata_date else "missing"
                ),
                "coverage": usable_metadata / len(items) if items else 0.0,
                "official_coverage": official_metadata / len(items) if items else 0.0,
                "enhanced_coverage": enhanced_metadata / len(items) if items else 0.0,
                "source": self._profile_capabilities.get("source", "security-master"),
            },
        }
        capabilities = {
            "metadata": self._profile_capabilities,
            "adjustment": {
                **adjustment_capability,
                "research_covered_symbols": verified_adjustments,
                "research_expected_symbols": len(items),
                "research_coverage": verified_adjustments / len(items) if items else 0.0,
            },
            "shares": {
                "status": freshness["shares"]["status"],
                "source": "etf_share_size 优先，fund_share 降级",
                "confirmed_symbols": confirmed_shares,
                "expected_symbols": len(items),
            },
            "intraday": {
                "status": "on_demand",
                "source": "free-stockdb",
                "scoring_input": False,
                "reason": "仅在打开单只 ETF 趋势标签时读取并缓存",
            },
            "refresh_warnings": effective_refresh_warnings,
            "publication": {
                "tier": selected_tier,
                "formal_eligible": selected_tier == "production",
                "status": "ready" if selected_tier == "production" else "blocked",
                "reason": (
                    "价格研究已发布；ETF 目录按明确降级母集计算"
                    if selected_tier == "production"
                    and self._profile_capabilities.get("status") == "degraded"
                    else "已通过不可变 ETF 目录与正式完整性门"
                    if selected_tier == "production"
                    else "sandbox 使用本地非完整母集，禁止发布为 production 快照"
                ),
            },
            "session": {
                "requested": str(as_of or ""),
                "resolved": end.date().isoformat(),
                "published": actual,
                "source": target_source,
            },
        }
        share_status_counts = {
            status: sum(item.funds.get("status") == status for item in items)
            for status in ("confirmed_change", "confirmed_zero", "stale", "missing")
        }
        snapshot = EtfResearchSnapshot(
            snapshot_id=snapshot_id,
            ingest_id=ingest.ingest_id,
            artifact_id=ingest.artifact_id,
            as_of_date=actual,
            coverage={
                **ingest.coverage,
                "product_count": len(items),
                "sector_count": len(sectors),
                "verified_adjustment_products": verified_adjustments,
                "official_metadata_products": official_metadata,
                "enhanced_metadata_products": enhanced_metadata,
                "share_status_counts": share_status_counts,
                "tier": selected_tier,
                "formal_eligible": selected_tier == "production",
                "denominator": self._profile_capabilities.get("denominator", {}),
            },
            provenance={
                "upstream": "vendor-declared-unverified",
                "upstream_evidence": "not_provided",
                "distribution": "free-stockdb + optional Tushare evidence cache",
                "calculation": "QuantMaster ETF Sector Radar V3",
                "tier": selected_tier,
                "formal_eligible": selected_tier == "production",
                "denominator_source": self._profile_capabilities.get("source", ""),
            },
            items=tuple(items),
            sectors=tuple(sectors),
            queues=queues,
            candidate_queues=candidate_queues,
            summaries=summaries,
            freshness=freshness,
            capabilities=capabilities,
            evidence_hashes=evidence_hashes,
            categories=tuple(
                category for category in ETF_CATEGORIES if any(item.category == category for item in items)
            ),
            input_hash=input_hash,
            tier=selected_tier,
            formal_eligible=selected_tier == "production",
        )
        if selected_tier == "production":
            snapshot = self.store.publish(snapshot)
            self.ingest_store.pin(
                snapshot.ingest_id,
                "etf_research",
                snapshot.snapshot_id,
                {"as_of_date": snapshot.as_of_date},
            )
            detail = f"{len(sectors)} 个板块 · {len(items)} 只产品"
        else:
            snapshot = self._remember_preview(snapshot, daily, factors)
            detail = f"本地预览 {len(sectors)} 个板块 · {len(items)} 只产品 · 不可发布"
        progress(100, "ETF 研究完成", detail)
        return snapshot


_lock = threading.Lock()
_instance: EtfResearchService | None = None


def get_etf_research_service() -> EtfResearchService:
    global _instance
    with _lock:
        if _instance is None:
            _instance = EtfResearchService()
        return _instance


def reset_etf_research_service() -> None:
    global _instance
    with _lock:
        _instance = None
