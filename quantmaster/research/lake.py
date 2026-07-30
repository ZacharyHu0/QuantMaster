"""Date-partitioned Parquet lake layered beside the legacy symbol BarStore."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import uuid
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from quantmaster.config import get_config
from quantmaster.research.catalog import ResearchCatalog
from quantmaster.research.contracts import (
    ArtifactKind,
    ArtifactRef,
    AssetClass,
    Frequency,
    ResearchSpec,
    content_hash,
    utc_now,
)

_SAFE_PART = re.compile(r"^[A-Za-z0-9_.-]+$")
_KEY_COLUMNS = ("trade_date", "symbol")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _safe(value: str) -> str:
    text = str(value).strip()
    if not _SAFE_PART.fullmatch(text) or text in {".", ".."}:
        raise ValueError(f"非法研究湖路径片段: {value!r}")
    return text


def _as_date(value: str | pd.Timestamp) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


class ResearchLake:
    """Atomic Parquet partitions plus a transactional lineage catalog."""

    def __init__(self, root: str | Path | None = None):
        base = Path(root) if root is not None else get_config().data_root / "research_lake"
        self.root = base.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.meta_root = self.root / "_meta"
        self.meta_root.mkdir(parents=True, exist_ok=True)
        self.catalog = ResearchCatalog(self.meta_root / "catalog.sqlite")

    @staticmethod
    def dataset_for_kind(kind: ArtifactKind, dataset_id: str = "") -> str:
        if kind in {ArtifactKind.FACTOR, ArtifactKind.LABEL}:
            return "wide"
        return _safe(dataset_id or "default")

    def partition_path(
        self,
        kind: ArtifactKind,
        asset_class: AssetClass,
        frequency: Frequency,
        dataset_id: str,
        trade_date: str,
    ) -> Path:
        date_text = _as_date(trade_date)
        compact = date_text.replace("-", "")
        year = compact[:4]
        dataset = self.dataset_for_kind(kind, dataset_id)
        if kind in {ArtifactKind.RAW, ArtifactKind.RISK, ArtifactKind.MODEL}:
            path = self.root / kind.value / asset_class.value / frequency.value / dataset / year
        else:
            path = self.root / kind.value / asset_class.value / frequency.value / year
        return path / f"{compact}.parquet"

    @staticmethod
    def _normalize_frame(
        frame: pd.DataFrame,
        *,
        kind: ArtifactKind,
        frequency: Frequency,
        trade_date: str,
    ) -> pd.DataFrame:
        if frame is None or frame.empty:
            raise ValueError("不能写入空研究分区")
        value = frame.copy()
        if isinstance(value.index, pd.DatetimeIndex) and "trade_date" not in value:
            value = value.reset_index().rename(columns={value.index.name or "index": "trade_date"})
        if "date" in value and "trade_date" not in value:
            value = value.rename(columns={"date": "trade_date"})
        if "ts_code" in value and "symbol" not in value:
            value = value.rename(columns={"ts_code": "symbol"})
        for key in _KEY_COLUMNS:
            if key not in value:
                raise ValueError(f"研究分区缺少主键列 {key}")
        dates = pd.to_datetime(value["trade_date"], errors="coerce").dt.normalize()
        if dates.isna().any():
            raise ValueError("trade_date 包含非法值")
        target = pd.Timestamp(trade_date).normalize()
        if not dates.eq(target).all():
            raise ValueError("单个分区只能包含一个 trade_date")
        value["trade_date"] = dates.dt.date
        value["symbol"] = value["symbol"].astype(str).str.strip().str.upper()
        if value["symbol"].eq("").any():
            raise ValueError("symbol 不能为空")
        if frequency != Frequency.DAILY:
            if "event_time_utc" not in value:
                raise ValueError("分钟分区必须包含 event_time_utc")
            timestamp = pd.to_datetime(value["event_time_utc"], utc=True, errors="coerce")
            if timestamp.isna().any():
                raise ValueError("event_time_utc 包含非法值")
            value["event_time_utc"] = timestamp
            keys = ["trade_date", "event_time_utc", "symbol"]
        else:
            keys = list(_KEY_COLUMNS)
        if value.duplicated(keys).any():
            duplicate = value.loc[value.duplicated(keys, keep=False), keys].head(3).to_dict("records")
            raise ValueError(f"研究分区主键重复: {duplicate}")
        value = value.replace([np.inf, -np.inf], np.nan)
        numeric = [
            column for column in value
            if column not in keys and pd.api.types.is_numeric_dtype(value[column])
        ]
        target_type = "float64" if kind == ArtifactKind.RAW else "float32"
        for column in numeric:
            if pd.api.types.is_bool_dtype(value[column]) or pd.api.types.is_integer_dtype(value[column]):
                continue
            value[column] = pd.to_numeric(value[column], errors="coerce").astype(target_type)
        ordered = keys + [column for column in value if column not in keys]
        return value[ordered].sort_values(keys).reset_index(drop=True)

    @staticmethod
    def _schema_hash(table: pa.Table) -> str:
        return content_hash([(field.name, str(field.type), field.nullable) for field in table.schema])

    def write_partition(
        self,
        kind: ArtifactKind,
        asset_class: AssetClass,
        frequency: Frequency,
        dataset_id: str,
        trade_date: str,
        frame: pd.DataFrame,
        *,
        merge_columns: bool = False,
        spec_versions: dict[str, str] | None = None,
        input_hashes: dict[str, str] | None = None,
        run_id: str = "",
        owner: str = "",
    ) -> dict[str, Any]:
        date_text = _as_date(trade_date)
        dataset = self.dataset_for_kind(kind, dataset_id)
        key = self.catalog.partition_key(kind, asset_class, frequency, dataset, date_text)
        owner = owner or run_id or uuid.uuid4().hex
        if not self.catalog.claim(key, owner):
            raise RuntimeError(f"分区正在由其他任务写入: {key}")
        target = self.partition_path(kind, asset_class, frequency, dataset_id, date_text)
        try:
            previous_metadata = self.catalog.partition(
                kind, asset_class, frequency, dataset, date_text,
            )
            normalized = self._normalize_frame(
                frame, kind=kind, frequency=frequency, trade_date=date_text,
            )
            if merge_columns and target.exists():
                existing = pd.read_parquet(target)
                existing = self._normalize_frame(
                    existing, kind=kind, frequency=frequency, trade_date=date_text,
                )
                keys = ["trade_date", "symbol"]
                if frequency != Frequency.DAILY:
                    keys.insert(1, "event_time_utc")
                replacing = [column for column in normalized if column not in keys]
                existing = existing.drop(columns=[c for c in replacing if c in existing], errors="ignore")
                normalized = existing.merge(normalized, on=keys, how="outer", validate="one_to_one")
                normalized = self._normalize_frame(
                    normalized, kind=kind, frequency=frequency, trade_date=date_text,
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pandas(normalized, preserve_index=False)
            fd, temp_name = tempfile.mkstemp(
                prefix=f".{target.stem}.", suffix=".parquet.tmp", dir=target.parent,
            )
            os.close(fd)
            temp = Path(temp_name)
            try:
                pq.write_table(table, temp, compression="zstd", use_dictionary=True)
                with temp.open("rb+") as stream:
                    os.fsync(stream.fileno())
                os.replace(temp, target)
            finally:
                temp.unlink(missing_ok=True)
            relative = target.relative_to(self.root).as_posix()
            merged_versions = dict((previous_metadata or {}).get("spec_versions") or {})
            merged_versions.update(spec_versions or {})
            merged_inputs = dict((previous_metadata or {}).get("input_hashes") or {})
            merged_inputs.update(input_hashes or {})
            metadata = self.catalog.record_partition({
                "kind": kind.value,
                "asset_class": asset_class.value,
                "frequency": frequency.value,
                "dataset_id": dataset,
                "trade_date": date_text,
                "path": relative,
                "row_count": len(normalized),
                "columns": list(normalized.columns),
                "schema_hash": self._schema_hash(table),
                "content_sha256": file_sha256(target),
                "spec_versions": merged_versions,
                "input_hashes": merged_inputs,
                "run_id": run_id,
                "updated_at": utc_now(),
            })
            return metadata
        finally:
            self.catalog.release(key, owner)

    def read_partition(
        self,
        kind: ArtifactKind,
        asset_class: AssetClass,
        frequency: Frequency,
        dataset_id: str,
        trade_date: str,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        path = self.partition_path(kind, asset_class, frequency, dataset_id, trade_date)
        if not path.is_file():
            return pd.DataFrame()
        return pd.read_parquet(path, columns=columns)

    def read_range(
        self,
        kind: ArtifactKind,
        asset_class: AssetClass,
        frequency: Frequency,
        dataset_id: str,
        start: str,
        end: str,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        partitions = self.catalog.partitions(
            kind=kind, asset_class=asset_class, frequency=frequency,
            dataset_id=self.dataset_for_kind(kind, dataset_id), start=start, end=end,
        )
        frames = []
        for item in partitions:
            path = (self.root / item["path"]).resolve()
            if self.root not in path.parents or not path.is_file():
                continue
            frames.append(pd.read_parquet(path, columns=columns))
        if not frames:
            return pd.DataFrame(columns=columns)
        result = pd.concat(frames, ignore_index=True)
        sort = [column for column in ("trade_date", "event_time_utc", "symbol") if column in result]
        return result.sort_values(sort).reset_index(drop=True)

    def write_artifact_values(
        self,
        spec: ResearchSpec,
        values: pd.DataFrame,
        *,
        asset_class: AssetClass | None = None,
        run_id: str,
        input_hashes: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Write long values with columns trade_date/symbol/value into versioned wide partitions."""
        self.catalog.register_spec(spec)
        if spec.output not in values and "value" in values:
            values = values.rename(columns={"value": spec.output})
        if spec.output not in values:
            raise ValueError(f"{spec.id} 结果缺少输出列 {spec.output}")
        output = values[["trade_date", "symbol", spec.output]].rename(
            columns={spec.output: spec.storage_column}
        )
        selected_asset = asset_class or spec.asset_classes[0]
        if selected_asset not in spec.asset_classes:
            raise ValueError(f"{spec.id} 不支持资产 {selected_asset.value}")
        records = []
        for date_value, group in output.groupby(pd.to_datetime(output["trade_date"]).dt.date):
            records.append(self.write_partition(
                spec.kind, selected_asset, spec.frequency, spec.id, str(date_value), group,
                merge_columns=spec.kind in {ArtifactKind.FACTOR, ArtifactKind.LABEL},
                spec_versions={spec.id: spec.version}, input_hashes=input_hashes,
                run_id=run_id,
            ))
        return records

    def artifact_panel(self, ref: ArtifactRef, start: str, end: str) -> pd.DataFrame:
        dataset_id = "QM_STYLE_V1" if ref.kind == ArtifactKind.RISK else ref.id
        data = self.read_range(
            ref.kind, ref.asset_class, ref.frequency, dataset_id, start, end,
            columns=["trade_date", "symbol", ref.storage_column],
        )
        if data.empty or ref.storage_column not in data:
            return pd.DataFrame()
        return data.pivot(index="trade_date", columns="symbol", values=ref.storage_column).sort_index()

    def materialize_bar_store(
        self,
        symbols: Iterable[str] | None,
        start: str,
        end: str,
        *,
        asset_class: AssetClass = AssetClass.STOCK,
        dataset_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lazily derive date partitions from legacy per-symbol Parquet caches."""
        from quantmaster.data.storage import BarStore

        store = BarStore()
        selected = list(symbols) if symbols is not None else store.symbols()
        if symbols is None:
            try:
                from quantmaster.data.instruments import InstrumentStore

                instruments = InstrumentStore().get_many(selected)
                selected = [
                    symbol for symbol in selected
                    if instruments.get(symbol)
                    and instruments[symbol].asset_type == asset_class.value
                ]
            except Exception:
                # An absent master must not invent asset classes; explicit symbols remain available.
                selected = []
        by_date: dict[str, list[pd.DataFrame]] = {}
        for symbol in selected:
            frame = store.get(symbol)
            if frame is None or frame.empty:
                continue
            sliced = frame.loc[start:end].copy()
            if sliced.empty:
                continue
            sliced = sliced.reset_index().rename(columns={sliced.index.name or "index": "trade_date"})
            sliced["symbol"] = symbol
            sliced["adjustment"] = "qfq_cache"
            for date_value, group in sliced.groupby(pd.to_datetime(sliced["trade_date"]).dt.date):
                by_date.setdefault(str(date_value), []).append(group)
        dataset = dataset_id or f"{asset_class.value}_bars"
        records = []
        for trade_date, frames in sorted(by_date.items()):
            records.append(self.write_partition(
                ArtifactKind.RAW, asset_class, Frequency.DAILY, dataset, trade_date,
                pd.concat(frames, ignore_index=True),
                input_hashes={"barstore": content_hash(sorted(selected))},
                run_id="barstore-materialize",
            ))
        return records

    def project_to_bar_store(self, frame: pd.DataFrame) -> int:
        """Idempotently expose normalized lake bars through existing history APIs."""
        from quantmaster.data.storage import BarStore

        required = {"trade_date", "symbol", "open", "high", "low", "close", "volume"}
        if not required.issubset(frame):
            raise ValueError(f"BarStore 投影缺少字段: {sorted(required - set(frame))}")
        store = BarStore()
        count = 0
        for symbol, group in frame.groupby("symbol"):
            value = group.copy()
            value.index = pd.DatetimeIndex(pd.to_datetime(value.pop("trade_date")), name="date")
            value = value.drop(columns=["symbol", "asset_class", "exchange"], errors="ignore")
            keep = [c for c in ("open", "high", "low", "close", "volume", "amount", "turnover") if c in value]
            store.put(str(symbol), value[keep].sort_index())
            count += len(value)
        return count

    def write_run_files(self, run_id: str, manifest: dict[str, Any], **tables: pd.DataFrame) -> Path:
        run = self.root / "runs" / _safe(run_id)
        run.mkdir(parents=True, exist_ok=True)
        temp = run / ".manifest.json.tmp"
        temp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp, run / "manifest.json")
        for name, table in tables.items():
            if table is not None and not table.empty:
                table.to_parquet(run / f"{_safe(name)}.parquet", index=False)
        return run


class FeatureBatchProvider:
    """Version-pinned tabular and [N,T,F] views over lake artifacts."""

    def __init__(self, lake: ResearchLake | None = None):
        self.lake = lake or ResearchLake()
        self._tabular_cache: dict[str, pd.DataFrame] = {}

    def _cache_key(self, refs: tuple[ArtifactRef, ...], start: str, end: str) -> str:
        inputs = []
        for ref in refs:
            dataset_id = "QM_STYLE_V1" if ref.kind == ArtifactKind.RISK else ref.id
            partitions = self.lake.catalog.partitions(
                kind=ref.kind, asset_class=ref.asset_class, frequency=ref.frequency,
                dataset_id=dataset_id, start=start, end=end,
            )
            inputs.append({
                "ref": ref.to_dict(),
                "partitions": [item["content_sha256"] for item in partitions],
            })
        return content_hash({"start": start, "end": end, "inputs": inputs})

    def tabular(self, refs: Iterable[ArtifactRef], start: str, end: str) -> pd.DataFrame:
        selected = tuple(refs)
        cache_key = self._cache_key(selected, start, end)
        cached = self._tabular_cache.get(cache_key)
        if cached is not None:
            return cached.copy()
        tables: list[pd.DataFrame] = []
        for ref in selected:
            panel = self.lake.artifact_panel(ref, start, end)
            if panel.empty:
                continue
            values = panel.rename_axis(index="trade_date", columns="symbol").reset_index().melt(
                id_vars="trade_date", var_name="symbol", value_name=ref.id,
            )
            tables.append(values)
        if not tables:
            return pd.DataFrame(columns=["trade_date", "symbol"])
        result = tables[0]
        for table in tables[1:]:
            result = result.merge(table, on=["trade_date", "symbol"], how="outer", validate="one_to_one")
        result = result.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
        if len(self._tabular_cache) >= 16:
            self._tabular_cache.pop(next(iter(self._tabular_cache)))
        self._tabular_cache[cache_key] = result.copy()
        return result

    def tensor(
        self,
        refs: Iterable[ArtifactRef],
        start: str,
        end: str,
        lookback: int,
    ) -> dict[str, Any]:
        if lookback <= 0:
            raise ValueError("lookback 必须大于 0")
        table = self.tabular(refs, start, end)
        features = [column for column in table if column not in {"trade_date", "symbol"}]
        if table.empty or not features:
            return {
                "values": np.empty((0, lookback, 0), dtype="float32"),
                "mask": np.empty((0, lookback, 0), dtype=bool),
                "keys": [],
                "features": features,
            }
        table["trade_date"] = pd.to_datetime(table["trade_date"])
        samples: list[np.ndarray] = []
        masks: list[np.ndarray] = []
        keys: list[dict[str, str]] = []
        for symbol, group in table.groupby("symbol"):
            group = group.sort_values("trade_date")
            values = group[features].to_numpy(dtype="float64")
            for index in range(lookback - 1, len(group)):
                window = values[index - lookback + 1:index + 1]
                samples.append(np.nan_to_num(window, nan=0.0).astype("float32"))
                masks.append(np.isfinite(window))
                keys.append({
                    "trade_date": str(group.iloc[index]["trade_date"].date()),
                    "symbol": str(symbol),
                })
        return {
            "values": (
                np.stack(samples)
                if samples else np.empty((0, lookback, len(features)), dtype="float32")
            ),
            "mask": np.stack(masks) if masks else np.empty((0, lookback, len(features)), dtype=bool),
            "keys": keys,
            "features": features,
        }
