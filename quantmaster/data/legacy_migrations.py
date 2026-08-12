"""Explicit one-shot migrations for retired market-data read paths.

These helpers intentionally operate on an explicit data root.  Normal readers and
store constructors never call them.  Every decision is based on a named legacy
artifact and exact source columns; uncertain values are reported or isolated
instead of being promoted into the current runtime contract.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import tempfile
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import pandas as pd

from quantmaster.data.legacy_migration import MigrationRecord
from quantmaster.runtime.sqlite import connect_sqlite

MigrationRow = dict[str, Any]


def _row(
    record_key: str,
    outcome: str,
    diagnostic_code: str,
    *,
    unknown_fields: Iterable[str] = (),
    detail: str = "",
) -> MigrationRow:
    return {
        "record_key": record_key,
        "outcome": outcome,
        "diagnostic_code": diagnostic_code,
        "unknown_fields": sorted(set(str(value) for value in unknown_fields)),
        "detail": detail,
    }


def _legacy_bar_name(symbol: str) -> str:
    return re.sub(r"[^0-9A-Za-z._^-]", "_", symbol)


def _migrate_bar_file(
    bars: Path, quarantine: Path, old_name: str, candidates: list[str], dry_run: bool,
) -> MigrationRow | None:
    source = bars / f"{old_name}.parquet"
    quarantined = quarantine / source.name
    if not source.is_file():
        return _row(
            f"bars/{source.name}", "conflict", "bar_filename_isolated",
            detail=",".join(candidates),
        ) if quarantined.is_file() else None
    if len(candidates) != 1:
        code, detail = "bar_symbol_collision", ",".join(candidates)
        target = quarantined
    else:
        symbol = candidates[0]
        current = bars / f"{symbol}.parquet"
        if not current.exists():
            if not dry_run:
                os.replace(source, current)
            return _row(f"bars/{source.name}", "converted", "bar_filename_migrated", detail=symbol)
        code, detail, target = "bar_target_exists", symbol, quarantined
    if not dry_run:
        quarantine.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            os.replace(source, target)
    return _row(f"bars/{source.name}", "conflict", code, detail=detail)


def migrate_bar_filenames(
    root: str | Path, *, dry_run: bool = False, only_keys: set[str] | None = None,
) -> list[MigrationRow]:
    """Move only filenames having one unambiguous ``bar_meta.symbol`` owner."""
    data_root = Path(root)
    bars = data_root / "bars"
    database = bars / "meta.sqlite"
    if not database.is_file():
        return []
    try:
        with connect_sqlite(database, policy="cache", read_only=True) as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bar_meta'"
            ).fetchone()
            symbols = [str(item[0]) for item in connection.execute(
                "SELECT symbol FROM bar_meta ORDER BY symbol"
            )] if table else []
    except sqlite3.Error as exc:
        return [_row("bars/meta.sqlite", "review", "bar_meta_unreadable", detail=str(exc))]

    owners: dict[str, list[str]] = {}
    for symbol in symbols:
        old = _legacy_bar_name(symbol)
        if old != symbol:
            owners.setdefault(old, []).append(symbol)

    results: list[MigrationRow] = []
    quarantine = data_root / "migration_quarantine" / "market_data" / "bars"
    for old_name, candidates in sorted(owners.items()):
        record_key = f"bars/{old_name}.parquet"
        if only_keys is not None and record_key not in only_keys:
            continue
        result = _migrate_bar_file(bars, quarantine, old_name, candidates, dry_run)
        if result is not None:
            results.append(result)
    return results


def _instrument_name_result(
    connection: sqlite3.Connection, symbol: str, values: set[str], current: dict[str, str],
    dry_run: bool,
) -> MigrationRow:
    if len(values) != 1:
        return _row(
            f"instrument:{symbol}", "conflict", "instrument_name_conflict",
            detail=" | ".join(sorted(values)),
        )
    name = next(iter(values))
    if symbol not in current:
        return _row(
            f"instrument:{symbol}", "blank", "instrument_symbol_missing",
            detail="不凭旧名称创建证券记录",
        )
    if current[symbol].strip():
        return _row(f"instrument:{symbol}", "unchanged", "instrument_name_present")
    if not dry_run:
        connection.execute("UPDATE instruments SET name=? WHERE symbol=? AND name=''", (name, symbol))
    return _row(f"instrument:{symbol}", "converted", "instrument_name_filled")


def _etf_reviews(
    connection: sqlite3.Connection, only_keys: set[str] | None,
) -> Iterable[MigrationRow]:
    columns = {str(item[1]) for item in connection.execute("PRAGMA table_info(instruments)")}
    if not {"symbol", "name", "market", "exchange", "asset_type"}.issubset(columns):
        return
    rows = connection.execute(
        """SELECT symbol FROM instruments
           WHERE market='CN' AND exchange IN ('SH','SZ') AND asset_type='fund'
             AND UPPER(name) LIKE '%ETF%' AND UPPER(name) NOT LIKE '%LOF%'
             AND name NOT LIKE '%联接%' ORDER BY symbol"""
    )
    for (symbol,) in rows:
        if only_keys is None or f"instrument:{symbol}:asset_type" in only_keys:
            yield _row(
                f"instrument:{symbol}:asset_type", "review",
                "instrument_etf_semantics_unproven", detail="名称不是 asset_type 的可靠证据",
            )


def _load_legacy_names(source: Path) -> tuple[dict[str, set[str]], list[MigrationRow]]:
    results: list[MigrationRow] = []
    payload: object = {}
    if source.is_file():
        try:
            payload = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            results.append(_row(
                "stock_names.json", "review", "instrument_names_unreadable", detail=str(exc),
            ))
        if not isinstance(payload, dict) or not isinstance(payload.get("names"), dict):
            unknown = payload.keys() if isinstance(payload, dict) else ()
            results.append(_row(
                "stock_names.json", "review", "instrument_names_unknown_format",
                unknown_fields=unknown,
            ))
            payload = {}
    names: dict[str, set[str]] = {}
    raw_names = payload.get("names", {}) if isinstance(payload, dict) else {}
    for raw_symbol, raw_name in raw_names.items():
        symbol, name = str(raw_symbol).strip().upper(), str(raw_name).strip()
        if symbol and name:
            names.setdefault(symbol, set()).add(name)
    return names, results


def migrate_instrument_names(
    root: str | Path, *, dry_run: bool = False, only_keys: set[str] | None = None,
) -> list[MigrationRow]:
    """Fill an empty current name for an existing symbol; infer no other field."""
    data_root = Path(root)
    source = data_root / "stock_names.json"
    database = data_root / "security_master.sqlite"
    names, results = _load_legacy_names(source)
    if not database.is_file():
        if source.is_file():
            results.append(_row(
                "stock_names.json", "blank", "instrument_catalog_missing",
                detail="旧名称未创建证券记录",
            ))
        return results

    connection = connect_sqlite(database, policy="cache")
    try:
        rows = {
            str(item[0]): str(item[1] or "")
            for item in connection.execute("SELECT symbol,name FROM instruments")
        }
        for symbol, values in sorted(names.items()):
            record_key = f"instrument:{symbol}"
            if only_keys is not None and record_key not in only_keys:
                continue
            results.append(_instrument_name_result(connection, symbol, values, rows, dry_run))

        # The old constructor used a name heuristic to relabel funds as ETFs.
        # Preserve candidates in the audit stream, but do not change asset type.
        results.extend(_etf_reviews(connection, only_keys))
        if not dry_run:
            connection.commit()
        else:
            connection.rollback()
    except sqlite3.Error as exc:
        connection.rollback()
        return [_row(
            "security_master.sqlite", "review", "instrument_catalog_unreadable",
            detail=str(exc),
        )]
    finally:
        connection.close()
    return results


_INDEX_REQUIRED = {"index_code", "con_code", "trade_date", "weight"}

_ETF_OBSERVATION_V0 = {
    "trade_date", "symbol", "name", "category", "benchmark", "shares", "nav", "close",
}
_ETF_OBSERVATION_V1 = _ETF_OBSERVATION_V0 | {"total_size", "share_source"}
_ETF_OBSERVATION_CURRENT = _ETF_OBSERVATION_V1 | {"acquired_at"}
_FACTOR_V0 = {"symbol", "date", "adj_factor"}
_FACTOR_V1 = _FACTOR_V0 | {"source"}
_FACTOR_CURRENT = _FACTOR_V1 | {"acquired_at"}


def _archive_artifact(data_root: Path, relative: Path) -> Path:
    source = data_root / relative
    target = data_root / "migration_quarantine" / "market_data" / "rotation_artifacts" / relative
    if target.exists():
        raise FileExistsError(f"migration quarantine target already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=target.parent, prefix=f".{target.name}.", suffix=".tmp", delete=False,
    ) as stream:
        staged = Path(stream.name)
    try:
        shutil.copy2(source, staged)
        with staged.open("rb+") as stream:
            os.fsync(stream.fileno())
        os.replace(staged, target)
        source.unlink()
    finally:
        staged.unlink(missing_ok=True)
    return target


def _copy_artifact(data_root: Path, relative: Path) -> Path:
    source = data_root / relative
    target = data_root / "migration_quarantine" / "market_data" / "rotation_artifacts" / relative
    if target.exists():
        raise FileExistsError(f"migration quarantine target already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    with target.open("rb+") as stream:
        os.fsync(stream.fileno())
    return target


def _read_parquet_columns(path: Path) -> tuple[pd.DataFrame | None, MigrationRow | None]:
    try:
        return pd.read_parquet(path), None
    except (OSError, ValueError, ImportError) as exc:
        return None, _row(path.as_posix(), "review", "rotation_parquet_unreadable", detail=str(exc))


def _current_observation_valid(frame: pd.DataFrame) -> bool:
    if frame.empty:
        return True
    dates = pd.to_datetime(frame["trade_date"], errors="coerce")
    symbols = frame["symbol"].fillna("").astype(str).str.strip()
    acquired_present = frame["acquired_at"].notna()
    acquired = pd.to_datetime(frame["acquired_at"], errors="coerce", utc=True)
    return bool(
        dates.notna().all() and symbols.ne("").all()
        and (~(acquired_present & acquired.isna())).all()
    )


def _current_factor_valid(frame: pd.DataFrame) -> bool:
    if frame.empty:
        return True
    dates = pd.to_datetime(frame["date"], errors="coerce")
    acquired_present = frame["acquired_at"].notna()
    acquired = pd.to_datetime(frame["acquired_at"], errors="coerce", utc=True)
    factors = pd.to_numeric(frame["adj_factor"], errors="coerce")
    symbols = frame["symbol"].fillna("").astype(str).str.strip()
    return bool(
        dates.notna().all() and (~(acquired_present & acquired.isna())).all()
        and factors.notna().all() and factors.gt(0).all() and symbols.ne("").all()
    )


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    with tempfile.NamedTemporaryFile(
        dir=path.parent, prefix=f".{path.stem}.", suffix=".parquet.tmp", delete=False,
    ) as stream:
        staged = Path(stream.name)
    try:
        frame.to_parquet(staged, index=False)
        with staged.open("rb+") as stream:
            os.fsync(stream.fileno())
        os.replace(staged, path)
    finally:
        staged.unlink(missing_ok=True)


def _load_metadata_manifest(path: Path, key: str) -> tuple[dict[str, Any] | None, MigrationRow | None]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, _row(key, "review", "rotation_metadata_manifest_unreadable", detail=str(exc))
    if not isinstance(value, dict):
        return None, _row(key, "review", "rotation_metadata_manifest_unknown_format")
    return value, None


def _validate_v1_metadata_history(
    parquet: Path, manifest: dict[str, Any], key: str,
) -> tuple[pd.DataFrame | None, MigrationRow | None]:
    v1_manifest_fields = {
        "schema_version", "artifact", "file_sha256", "logical_sha256", "row_count",
        "observation_count", "written_at", "manifest_sha256",
    }
    if (
        manifest.get("schema_version") != "1.0"
        or manifest.get("artifact") != "etf_metadata_history"
        or set(manifest) != v1_manifest_fields
    ):
        return None, _row(
            key, "review", "rotation_metadata_manifest_unknown_contract",
            unknown_fields=set(manifest) - v1_manifest_fields,
            detail="missing=" + ",".join(sorted(v1_manifest_fields - set(manifest))),
        )
    frame, error = _read_parquet_columns(parquet)
    if error is not None or frame is None:
        return None, _row(key, "review", "rotation_metadata_history_v1_unreadable")
    required = {
        "symbol", "observed_at", "observation_id", "observation_content_sha256",
        "observation_integrity",
    }
    valid_shape = (
        required.issubset(frame.columns)
        and len(frame) == int(manifest.get("row_count") or -1)
        and frame["observation_id"].nunique() == int(manifest.get("observation_count") or -1)
    )
    if not valid_shape:
        return None, _row(key, "review", "rotation_metadata_history_v1_shape_failed")
    return frame, None


def _migrate_etf_observations(
    root: Path, *, dry_run: bool, only_keys: set[str] | None,
) -> list[MigrationRow]:
    relative = Path("rotation/etf_observations.parquet")
    key = "rotation/etf_observations"
    if only_keys is not None and key not in only_keys:
        return []
    path = root / relative
    if not path.is_file():
        return []
    frame, error = _read_parquet_columns(path)
    if error is not None or frame is None:
        return [_row(key, "review", "rotation_etf_observations_unreadable")]
    columns = set(frame.columns)
    if columns == _ETF_OBSERVATION_CURRENT:
        valid = _current_observation_valid(frame)
        return [_row(
            key, "unchanged" if valid else "review",
            "rotation_etf_observations_current" if valid
            else "rotation_etf_observations_current_invalid",
        )]
    if columns != _ETF_OBSERVATION_V0 and columns != _ETF_OBSERVATION_V1:
        return [_row(
            key, "review", "rotation_etf_observations_unknown_contract",
            unknown_fields=columns - _ETF_OBSERVATION_CURRENT,
            detail="missing=" + ",".join(sorted(_ETF_OBSERVATION_CURRENT - columns)),
        )]
    migrated = frame.copy()
    if columns == _ETF_OBSERVATION_V0:
        migrated["total_size"] = pd.NA
        # Git history proves the v0 writer exclusively used fund_share.
        migrated["share_source"] = "tushare:fund_share"
    migrated["acquired_at"] = pd.NaT
    migrated = migrated.loc[:, sorted(_ETF_OBSERVATION_CURRENT)]
    if not dry_run:
        try:
            _copy_artifact(root, relative)
            _atomic_parquet(path, migrated)
        except FileExistsError as exc:
            return [_row(key, "conflict", "rotation_etf_observations_quarantine_conflict", detail=str(exc))]
    return [_row(
        key, "converted", "rotation_etf_observations_migrated",
        unknown_fields=("acquired_at",),
        detail=f"rows={len(frame)}; acquired_at remains blank",
    )]


def _inspect_metadata_history(root: Path, *, dry_run: bool, only_keys: set[str] | None) -> list[MigrationRow]:
    key = "rotation/etf_metadata_history"
    if only_keys is not None and key not in only_keys:
        return []
    parquet_rel = Path("rotation/etf_metadata_history.parquet")
    manifest_rel = Path("rotation/etf_metadata_history.manifest.json")
    parquet, manifest_path = root / parquet_rel, root / manifest_rel
    quarantine = root / "migration_quarantine" / "market_data" / "rotation_artifacts"
    if not parquet.exists() and not manifest_path.exists() and (
        (quarantine / parquet_rel).is_file() and (quarantine / manifest_rel).is_file()
    ):
        return [_row(key, "blank", "rotation_metadata_history_v1_isolated")]
    if not parquet.exists() and not manifest_path.exists():
        return []
    if not parquet.is_file() or not manifest_path.is_file():
        return [_row(key, "review", "rotation_metadata_history_pair_incomplete")]
    manifest, error = _load_metadata_manifest(manifest_path, key)
    if error is not None or manifest is None:
        return [error] if error is not None else []
    if manifest.get("schema_version") == "2.0":
        return [_row(key, "unchanged", "rotation_metadata_history_current")]
    frame, error = _validate_v1_metadata_history(parquet, manifest, key)
    if error is not None or frame is None:
        return [error] if error is not None else []
    if not dry_run:
        try:
            _archive_artifact(root, parquet_rel)
            _archive_artifact(root, manifest_rel)
        except FileExistsError as exc:
            return [_row(key, "conflict", "rotation_metadata_history_quarantine_conflict", detail=str(exc))]
    return [_row(
        key, "blank", "rotation_metadata_history_v1_isolated",
        detail=f"rows={len(frame)}; v2-only directory evidence remains blank until rebuilt",
    )]


def _inspect_simple_rotation_parquet(
    root: Path, *, relative: Path, key: str, legacy_shapes: tuple[set[str], ...],
    current_shape: set[str], validator: Callable[[pd.DataFrame], bool], dry_run: bool,
    only_keys: set[str] | None, diagnostic: str, unknown_fields: tuple[str, ...],
) -> list[MigrationRow]:
    if only_keys is not None and key not in only_keys:
        return []
    path = root / relative
    if not path.is_file():
        isolated = (
            root / "migration_quarantine" / "market_data" / "rotation_artifacts" / relative
        )
        if isolated.is_file():
            return [_row(key, "blank", f"{diagnostic}_isolated")]
        return []
    frame, error = _read_parquet_columns(path)
    if error is not None or frame is None:
        return [_row(key, "review", f"{diagnostic}_unreadable", detail=error["detail"] if error else "")]
    columns = set(frame.columns)
    if columns == current_shape:
        return [_row(
            key, "unchanged" if validator(frame) else "review",
            f"{diagnostic}_current" if validator(frame) else f"{diagnostic}_current_invalid",
        )]
    if columns not in legacy_shapes:
        return [_row(
            key, "review", f"{diagnostic}_unknown_contract",
            unknown_fields=columns - current_shape,
            detail="missing=" + ",".join(sorted(current_shape - columns)),
        )]
    if not dry_run:
        try:
            _archive_artifact(root, relative)
        except FileExistsError as exc:
            return [_row(key, "conflict", f"{diagnostic}_quarantine_conflict", detail=str(exc))]
    return [_row(
        key, "blank", f"{diagnostic}_isolated", unknown_fields=unknown_fields,
        detail=f"rows={len(frame)}; acquisition time is not recoverable from the old writer",
    )]


def migrate_rotation_etf_artifacts(
    root: str | Path, *, dry_run: bool = False, only_keys: set[str] | None = None,
) -> list[MigrationRow]:
    """Retire only exact Git-confirmed ETF cache contracts outside normal readers."""
    data_root = Path(root)
    results = _inspect_metadata_history(data_root, dry_run=dry_run, only_keys=only_keys)
    results += _migrate_etf_observations(
        data_root, dry_run=dry_run, only_keys=only_keys,
    )
    results += _inspect_simple_rotation_parquet(
        data_root, relative=Path("etf-research/evidence/adjustment_factors.parquet"),
        key="etf-research/evidence/adjustment_factors", legacy_shapes=(_FACTOR_V0, _FACTOR_V1),
        current_shape=_FACTOR_CURRENT, validator=_current_factor_valid,
        dry_run=dry_run, only_keys=only_keys, diagnostic="rotation_adjustment_factors",
        unknown_fields=("acquired_at", "source"),
    )
    return results


def migrate_index_membership(
    root: str | Path, *, dry_run: bool = False, only_keys: set[str] | None = None,
) -> list[MigrationRow]:
    """Isolate exact old Tushare rows with temporal evidence left null.

    File mtimes describe filesystem activity, not provider publication or
    acquisition.  Therefore these rows cannot enter the PIT research lake.
    """
    data_root = Path(root)
    source_root = data_root / "api_cache" / "tushare"
    if not source_root.is_dir():
        return []
    target_root = data_root / "migration_quarantine" / "market_data" / "index_membership"
    results: list[MigrationRow] = []
    for source in sorted(source_root.glob("index_weight-*.parquet")):
        key = f"api_cache/tushare/{source.name}"
        if only_keys is not None and key not in only_keys:
            continue
        try:
            frame = pd.read_parquet(source)
        except (OSError, ValueError, ImportError) as exc:
            results.append(_row(key, "review", "index_membership_unreadable", detail=str(exc)))
            continue
        columns = set(frame.columns)
        if not _INDEX_REQUIRED.issubset(columns):
            results.append(_row(
                key, "review", "index_membership_unknown_format",
                unknown_fields=columns - _INDEX_REQUIRED,
                detail="missing=" + ",".join(sorted(_INDEX_REQUIRED - columns)),
            ))
            continue
        common = frame[["trade_date", "con_code", "index_code", "weight"]].rename(
            columns={"con_code": "symbol"}
        )
        common["published_at"] = pd.NaT
        common["acquired_at"] = pd.NaT
        common["temporal_quality"] = pd.NA
        common = common.dropna(subset=["trade_date", "symbol", "index_code"])
        unknown = columns - _INDEX_REQUIRED
        if common.empty:
            results.append(_row(
                key, "blank", "index_membership_empty", unknown_fields=unknown,
            ))
            continue
        target = target_root / source.name
        if not dry_run and not target.exists():
            target_root.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                dir=target_root, prefix=f".{source.stem}.", suffix=".tmp", delete=False,
            ) as stream:
                staged = Path(stream.name)
            try:
                common.to_parquet(staged, index=False)
                os.replace(staged, target)
            finally:
                staged.unlink(missing_ok=True)
        results.append(_row(
            key, "blank", "index_membership_temporal_evidence_missing",
            unknown_fields=unknown,
            detail=f"isolated_rows={len(common)}; published_at/acquired_at 留空",
        ))
    return results


def migrate_industry_current_projection(
    root: str | Path, *, dry_run: bool = False, only_keys: set[str] | None = None,
) -> list[MigrationRow]:
    """Convert exactly ``{updated_at, mapping}`` into a current-only projection."""
    path = Path(root) / "industry_map.json"
    if only_keys is not None and "industry_map.json" not in only_keys:
        return []
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [_row("industry_map.json", "review", "industry_map_unreadable", detail=str(exc))]
    if (
        isinstance(payload, dict)
        and payload.get("schema_version") == 3
        and payload.get("projection") == "current_only"
    ):
        return [_row("industry_map.json", "unchanged", "industry_current_only_present")]
    if not isinstance(payload, dict) or not isinstance(payload.get("mapping"), dict):
        unknown = payload.keys() if isinstance(payload, dict) else ()
        return [_row(
            "industry_map.json", "review", "industry_map_unknown_format",
            unknown_fields=unknown,
        )]
    try:
        updated_at = float(payload["updated_at"])
    except (KeyError, TypeError, ValueError, OverflowError):
        return [_row(
            "industry_map.json", "review", "industry_updated_at_missing",
            unknown_fields=set(payload) - {"updated_at", "mapping"},
        )]
    mapping = {
        str(symbol).strip().upper(): str(industry).strip()
        for symbol, industry in payload["mapping"].items()
        if str(symbol).strip() and str(industry).strip()
    }
    unknown = set(payload) - {"updated_at", "mapping"}
    if not mapping:
        return [_row(
            "industry_map.json", "blank", "industry_mapping_empty",
            unknown_fields=unknown,
        )]
    current = {
        "schema_version": 3,
        "projection": "current_only",
        "updated_at": updated_at,
        "mapping": mapping,
    }
    if not dry_run:
        serialized = json.dumps(
            current, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent,
            prefix=f".{path.name}.", suffix=".tmp", delete=False,
        ) as stream:
            staged = Path(stream.name)
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.replace(staged, path)
        finally:
            staged.unlink(missing_ok=True)
    return [_row(
        "industry_map.json", "converted", "industry_current_only_migrated",
        unknown_fields=unknown,
        detail="历史时点、完整性与分母留空",
    )]


class MarketDataLegacyMigrator:
    """Adapter for the repository-wide resumable legacy migration runner."""

    name = "market_data"
    backup_paths = (
        "bars", "security_master.sqlite", "stock_names.json", "industry_map.json",
        "migration_quarantine/market_data",
        "rotation/etf_metadata_history.parquet",
        "rotation/etf_metadata_history.manifest.json",
        "rotation/etf_observations.parquet",
        "etf-research/evidence/adjustment_factors.parquet",
    )
    _domains: tuple[Callable[..., list[MigrationRow]], ...] = (
        migrate_bar_filenames,
        migrate_instrument_names,
        migrate_index_membership,
        migrate_industry_current_projection,
        migrate_rotation_etf_artifacts,
    )

    def inspect(self, root: str | Path) -> Iterable[MigrationRow]:
        for migrate in self._domains:
            yield from (_as_record(item) for item in migrate(root, dry_run=True))

    def migrate_batch(
        self, root: str | Path, after_key: str, limit: int,
    ) -> Iterable[MigrationRow]:
        if limit <= 0:
            return
        candidates = sorted(
            (
                record for record in self.inspect(root)
                if record.record_key > str(after_key or "")
            ),
            key=lambda record: record.record_key,
        )[:limit]
        selected = {record.record_key for record in candidates}
        migrated: dict[str, MigrationRecord] = {}
        for migrate in self._domains:
            for item in migrate(root, dry_run=False, only_keys=selected):
                record = _as_record(item)
                migrated[record.record_key] = record
        for candidate in candidates:
            key = candidate.record_key
            if key in migrated:
                yield migrated[key]

    def rollback(self, root: str | Path, backup_root: str | Path) -> None:
        """Restore only market-data paths from a runner-created data-root backup."""
        from quantmaster.data.migration import restore_backup_path

        for relative in self.backup_paths:
            restore_backup_path(Path(root), Path(backup_root), relative)


def _as_record(value: MigrationRow) -> MigrationRecord:
    return MigrationRecord(
        record_key=str(value["record_key"]), outcome=str(value["outcome"]),
        diagnostic_code=str(value.get("diagnostic_code") or ""),
        unknown_fields=tuple(value.get("unknown_fields") or ()),
        detail=str(value.get("detail") or ""),
    )


market_data_legacy_migrator = MarketDataLegacyMigrator()
