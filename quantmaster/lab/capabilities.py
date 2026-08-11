"""Published, local-only Quant Lab capability snapshot.

Hardware and data-pool inspection can import PyTorch and invoke ``nvidia-smi``.
Those are useful worker diagnostics, but they must never hold up the first
render of the Lab page.  The runtime worker publishes one immutable derived
artifact and Web generations only read its current pointer.
"""

from __future__ import annotations

import importlib.util
import sqlite3
from datetime import UTC, datetime
from typing import Any

from quantmaster.config import get_config
from quantmaster.runtime.derived import DerivedArtifactCatalog, DerivedArtifactIntegrityError

DOMAIN = "lab"
SNAPSHOT_TYPE = "capabilities"
SCHEMA_VERSION = "1"


def _fallback_capabilities() -> dict[str, Any]:
    """Return an honest cold state without probing hardware or local stores."""

    cfg = get_config()
    return {
        "tushare": {
            "configured": bool(cfg.data.tushare_token),
            "cached_membership": False,
            "production_membership": False,
        },
        "ml": {"torch": False, "sklearn": False, "onnxruntime": False},
        "llm": {"configured": bool(cfg.llm.api_key), "provider": cfg.llm.provider},
        "local_data": {
            "catalogued_symbols": 0,
            "bytes": 0,
            "network_required_for_research": False,
        },
        "models": {
            "available_models": [],
            "torch": False,
            "torch_version": "",
            "cuda_runtime": "",
            "torch_build": "unknown",
            "sklearn": False,
            "requested_device": str(cfg.lab.device or "auto"),
            "device": "cpu",
            "gpu": {"available": False, "hardware_available": False},
            "optuna": False,
            "multi_horizon_models": [],
        },
        "catalog_size": 48,
        "safe_dsl": True,
        "arbitrary_python": False,
        "restricted_python": True,
        "python_mining_enabled": bool(cfg.lab.ai_python_mining_enabled),
        "python_mining_limits": {"llm_calls": 3, "candidates": 24, "finalists": 3},
        "optuna": False,
        "research_protocol": "756/20/252",
        "snapshot": {
            "state": "unavailable",
            "issues": ["lab_capabilities_snapshot_unavailable"],
        },
    }


def build_capabilities() -> dict[str, Any]:
    """Perform the expensive inspection in the runtime-worker only."""

    from quantmaster.lab.dataset import readiness
    from quantmaster.lab.ml import capabilities as ml_capabilities

    cfg = get_config()
    return {
        **readiness(),
        "models": ml_capabilities(),
        "catalog_size": 48,
        "safe_dsl": True,
        "arbitrary_python": False,
        "restricted_python": True,
        "python_mining_enabled": bool(cfg.lab.ai_python_mining_enabled),
        "python_mining_limits": {"llm_calls": 3, "candidates": 24, "finalists": 3},
        "optuna": bool(importlib.util.find_spec("optuna")),
        "research_protocol": "756/20/252",
    }


def publish_capabilities() -> dict[str, Any]:
    """Atomically publish the latest worker-owned capability projection."""

    published_at = datetime.now(UTC).isoformat()
    payload = build_capabilities()
    document = {
        "schema_version": SCHEMA_VERSION,
        "published_at": published_at,
        "capabilities": payload,
    }
    catalog = DerivedArtifactCatalog()
    artifact = catalog.put_json(document, schema_version=SCHEMA_VERSION)
    catalog.publish_snapshot(DOMAIN, SNAPSHOT_TYPE, str(artifact["artifact_id"]))
    return {
        "id": str(artifact["artifact_id"]),
        "published_at": published_at,
        "state": "fresh",
    }


def read_published_capabilities() -> dict[str, Any]:
    """Read a verified snapshot or a bounded cold-state projection.

    This deliberately performs no capability calculation: no hardware probe,
    data-pool walk, module import or network call is permitted in a Web read.
    """

    fallback = _fallback_capabilities()
    try:
        catalog = DerivedArtifactCatalog(read_only=True)
        snapshot = catalog.current_snapshot(DOMAIN, SNAPSHOT_TYPE)
        if snapshot is None:
            return fallback
        value = catalog.read_json(str(snapshot["artifact_id"]))
        if not isinstance(value, dict) or not isinstance(value.get("capabilities"), dict):
            return fallback
        capabilities = dict(value["capabilities"])
        capabilities["snapshot"] = {
            "id": str(snapshot["artifact_id"]),
            "published_at": str(value.get("published_at") or ""),
            "state": "fresh",
            "issues": [],
        }
        return capabilities
    except (
        FileNotFoundError,
        KeyError,
        OSError,
        RuntimeError,
        ValueError,
        sqlite3.Error,
        DerivedArtifactIntegrityError,
    ):
        return fallback
