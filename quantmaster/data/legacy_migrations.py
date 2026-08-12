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
import shutil
import sqlite3
import tempfile
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import pandas as pd

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
        source = bars / f"{old_name}.parquet"
        record_key = f"bars/{source.name}"
        if only_keys is not None and record_key not in only_keys:
            continue
        quarantined = quarantine / source.name
        if not source.is_file():
            if quarantined.is_file():
                results.append(_row(
                    f"bars/{source.name}", "conflict", "bar_filename_isolated",
                    detail=",".join(candidates),
                ))
            continue
        if len(candidates) != 1:
            if not dry_run:
                quarantine.mkdir(parents=True, exist_ok=True)
                if not quarantined.exists():
                    os.replace(source, quarantined)
            results.append(_row(
                f"bars/{source.name}", "conflict", "bar_symbol_collision",
                detail=",".join(candidates),
            ))
            continue
        symbol = candidates[0]
        target = bars / f"{symbol}.parquet"
        if target.exists():
            if not dry_run:
                quarantine.mkdir(parents=True, exist_ok=True)
                if not quarantined.exists():
                    os.replace(source, quarantined)
            results.append(_row(
                f"bars/{source.name}", "conflict", "bar_target_exists",
                detail=symbol,
            ))
            continue
        if not dry_run:
            os.replace(source, target)
        results.append(_row(
            f"bars/{source.name}", "converted", "bar_filename_migrated",
            detail=symbol,
        ))
    return results


def migrate_instrument_names(
    root: str | Path, *, dry_run: bool = False, only_keys: set[str] | None = None,
) -> list[MigrationRow]:
    """Fill an empty current name for an existing symbol; infer no other field."""
    data_root = Path(root)
    source = data_root / "stock_names.json"
    database = data_root / "security_master.sqlite"
    results: list[MigrationRow] = []
    payload: object = {}
    if source.is_file():
        try:
            payload = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            results.append(_row(
                "stock_names.json", "review", "instrument_names_unreadable",
                detail=str(exc),
            ))
        if not isinstance(payload, dict) or not isinstance(payload.get("names"), dict):
            unknown = payload.keys() if isinstance(payload, dict) else ()
            results.append(_row(
                "stock_names.json", "review", "instrument_names_unknown_format",
                unknown_fields=unknown,
            ))
            payload = {}
    if not database.is_file():
        if source.is_file():
            results.append(_row(
                "stock_names.json", "blank", "instrument_catalog_missing",
                detail="旧名称未创建证券记录",
            ))
        return results

    names: dict[str, set[str]] = {}
    raw_names = payload.get("names", {}) if isinstance(payload, dict) else {}
    for raw_symbol, raw_name in raw_names.items():
        symbol, name = str(raw_symbol).strip().upper(), str(raw_name).strip()
        if symbol and name:
            names.setdefault(symbol, set()).add(name)
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
            if len(values) != 1:
                results.append(_row(
                    f"instrument:{symbol}", "conflict", "instrument_name_conflict",
                    detail=" | ".join(sorted(values)),
                ))
                continue
            name = next(iter(values))
            if symbol not in rows:
                results.append(_row(
                    f"instrument:{symbol}", "blank", "instrument_symbol_missing",
                    detail="不凭旧名称创建证券记录",
                ))
            elif rows[symbol].strip():
                results.append(_row(
                    f"instrument:{symbol}", "unchanged", "instrument_name_present",
                ))
            else:
                if not dry_run:
                    connection.execute(
                        "UPDATE instruments SET name=? WHERE symbol=? AND name=''", (name, symbol),
                    )
                results.append(_row(
                    f"instrument:{symbol}", "converted", "instrument_name_filled",
                ))

        # The old constructor used a name heuristic to relabel funds as ETFs.
        # Preserve candidates in the audit stream, but do not change asset type.
        columns = {str(item[1]) for item in connection.execute("PRAGMA table_info(instruments)")}
        if {"symbol", "name", "market", "exchange", "asset_type"}.issubset(columns):
            for symbol in (item[0] for item in connection.execute(
                """SELECT symbol FROM instruments
                   WHERE market='CN' AND exchange IN ('SH','SZ') AND asset_type='fund'
                     AND UPPER(name) LIKE '%ETF%' AND UPPER(name) NOT LIKE '%LOF%'
                     AND name NOT LIKE '%联接%' ORDER BY symbol"""
            )):
                if only_keys is not None and f"instrument:{symbol}:asset_type" not in only_keys:
                    continue
                results.append(_row(
                    f"instrument:{symbol}:asset_type", "review",
                    "instrument_etf_semantics_unproven",
                    detail="名称不是 asset_type 的可靠证据",
                ))
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
    _domains: tuple[Callable[..., list[MigrationRow]], ...] = (
        migrate_bar_filenames,
        migrate_instrument_names,
        migrate_index_membership,
        migrate_industry_current_projection,
    )

    def inspect(self, root: str | Path) -> Iterable[MigrationRow]:
        for migrate in self._domains:
            yield from migrate(root, dry_run=True)

    def migrate_batch(
        self, root: str | Path, after_key: str, limit: int,
    ) -> Iterable[MigrationRow]:
        if limit <= 0:
            return
        candidates = sorted(
            (
                record for record in self.inspect(root)
                if str(record["record_key"]) > str(after_key or "")
            ),
            key=lambda record: str(record["record_key"]),
        )[:limit]
        selected = {str(record["record_key"]) for record in candidates}
        for migrate in self._domains:
            yield from migrate(root, dry_run=False, only_keys=selected)

    def rollback(self, root: str | Path, backup_root: str | Path) -> None:
        """Restore only market-data paths from a runner-created data-root backup."""
        destination, backup = Path(root), Path(backup_root)
        for relative in (
            Path("bars"), Path("security_master.sqlite"), Path("stock_names.json"),
            Path("industry_map.json"), Path("migration_quarantine") / "market_data",
        ):
            source = backup / relative
            target = destination / relative
            if not source.exists():
                continue
            if source.is_dir():
                if target.exists():
                    shutil.rmtree(target)
                shutil.copytree(source, target)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
