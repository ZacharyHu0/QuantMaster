"""Versioned, JSON-safe contracts for the user-managed free-stockdb runtime."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
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
        "path": str(path), "available": True, "sha256": _sha256(path),
        "size": stat.st_size, "mtime_ns": stat.st_mtime_ns,
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
        cls, sdk_path: str | Path | None, runtime_root: str | Path | None,
        *, data_session: str = "", catalog_hash: str = "", board_hash: str = "",
    ) -> "StockDBArtifactIdentity":
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
            "sdk": sdk_record, "native": native, "programs": program_records,
            "data_session": data_session, "catalog_hash": catalog_hash,
            "board_hash": board_hash,
        }
        artifact_id = hashlib.sha256(json.dumps(
            subject, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest()
        return cls(artifact_id, sdk_record, native, program_records,
                   data_session, catalog_hash, board_hash)

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
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        for key in ("securities", "delisted", "boards"):
            value[key] = list(value[key])
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
    status: str = "complete"
    issues: tuple[str, ...] = ()
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["issues"] = list(self.issues)
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "StockDBIngestSnapshot":
        data = dict(value)
        data["issues"] = tuple(data.get("issues") or ())
        return cls(**data)
