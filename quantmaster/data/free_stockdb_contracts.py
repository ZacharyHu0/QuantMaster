"""Versioned, JSON-safe contracts for the user-managed free-stockdb runtime."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact(path: Path | None) -> dict[str, Any]:
    if path is None or not path.is_file():
        return {"path": str(path or ""), "available": False}
    stat = path.stat()
    return {
        "path": str(path),
        "available": True,
        "sha256": _sha256(path),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


@dataclass(frozen=True)
class StockDBArtifactIdentity:
    artifact_id: str
    sdk: dict[str, Any]
    native: tuple[dict[str, Any], ...] = ()
    programs: tuple[dict[str, Any], ...] = ()
    data_session: str = ""
    catalog_hash: str = ""
    board_hash: str = ""

    @classmethod
    def discover(
        cls,
        sdk_path: str | Path | None,
        runtime_root: str | Path | None,
        *,
        data_session: str = "",
        catalog_hash: str = "",
        board_hash: str = "",
    ) -> StockDBArtifactIdentity:
        sdk = Path(sdk_path).resolve() if sdk_path else None
        root = Path(runtime_root).resolve() if runtime_root else None
        native_paths: list[Path] = []
        if sdk is not None and sdk.parent.is_dir():
            for pattern in ("stockdb*.pyd", "stockdb*.so", "*.dll"):
                native_paths.extend(sorted(sdk.parent.glob(pattern)))
        programs = []
        if root is not None and root.is_dir():
            for name in ("stockdb.exe", "数据更新.exe"):
                candidate = root / name
                if candidate.is_file():
                    programs.append(candidate)
        sdk_record = _artifact(sdk)
        native = tuple(_artifact(path) for path in dict.fromkeys(native_paths))
        program_records = tuple(_artifact(path) for path in programs)
        subject = {
            "sdk": sdk_record,
            "native": native,
            "programs": program_records,
            "data_session": data_session,
            "catalog_hash": catalog_hash,
            "board_hash": board_hash,
        }
        artifact_id = hashlib.sha256(
            json.dumps(
                subject,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        return cls(artifact_id, sdk_record, native, program_records, data_session, catalog_hash, board_hash)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["native"] = list(self.native)
        value["programs"] = list(self.programs)
        return value


@dataclass(frozen=True)
class StockDBCatalogSnapshot:
    snapshot_id: str
    as_of_date: str
    artifact_id: str
    securities: tuple[dict[str, Any], ...] = ()
    delisted: tuple[dict[str, Any], ...] = ()
    boards: tuple[dict[str, Any], ...] = ()
    coverage: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        for key in ("securities", "delisted", "boards"):
            value[key] = list(value[key])
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> StockDBCatalogSnapshot:
        data = dict(value)
        for key in ("securities", "delisted", "boards"):
            data[key] = tuple(data.get(key) or ())
        return cls(**data)


@dataclass(frozen=True)
class StockDBFieldCoverage:
    """Machine-readable field semantics for one immutable ingest."""

    field: str
    unit: str
    asset_classes: tuple[str, ...]
    source: str
    available: bool
    applicable: bool
    rows: int
    total_rows: int
    ratio: float | None
    latest_rows: int
    latest_total_rows: int
    latest_ratio: float | None
    missing_reason: str = ""
    validation: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["asset_classes"] = list(self.asset_classes)
        return value


@dataclass(frozen=True)
class StockDBIngestSnapshot:
    ingest_id: str
    as_of_date: str
    artifact_id: str
    master_snapshot_id: str
    start_date: str
    end_date: str
    assets: dict[str, Any]
    coverage: dict[str, Any]
    content_hashes: dict[str, str]
    provenance: dict[str, Any]
    catalog_id: str = ""
    session_dates: tuple[str, ...] = ()
    session_source: str = ""
    status: str = "complete"
    issues: tuple[str, ...] = ()
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["issues"] = list(self.issues)
        value["session_dates"] = list(self.session_dates)
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> StockDBIngestSnapshot:
        data = dict(value)
        data["issues"] = tuple(data.get("issues") or ())
        data["session_dates"] = tuple(data.get("session_dates") or ())
        data.setdefault("catalog_id", "")
        data.setdefault("session_source", "")
        return cls(**data)


@dataclass(frozen=True)
class StockDBCompatibilityProfile:
    """Artifact-bound admission record for optional vendor-native acceleration."""

    artifact_id: str
    status: str
    methods: dict[str, dict[str, Any]]
    samples: tuple[dict[str, Any], ...] = ()
    schema_version: str = "1.0"
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["samples"] = list(self.samples)
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> StockDBCompatibilityProfile:
        data = dict(value)
        data["samples"] = tuple(data.get("samples") or ())
        return cls(**data)
