"""Small, side-effect-free Web readiness and runtime status projection.

The projection deliberately does not construct stores, schedulers, or optional
providers.  It is safe to serve from the Web process while the durable worker
is starting, restarting, or unavailable.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from quantmaster import __version__
from quantmaster.config import get_config, get_config_readiness
from quantmaster.release import RELEASE_DATE


def readiness_status(*, include_optional_services: bool = True) -> dict[str, Any]:
    """Return the Web readiness contract without opening a database.

    ``core_ready`` intentionally has no dependency on LLMs, remote providers,
    automation channels, or the runtime worker.  A locally usable Web process
    with an accessible local data root may open its UI while those optional
    services recover in the background.
    """

    storage = get_config_readiness()
    storage_ready = storage.get("status") == "ready"
    optional_ready = _optional_services_ready() if include_optional_services else False
    core_ready = storage_ready
    return {
        "status": "ready" if core_ready else "not_ready",
        "version": __version__,
        "release_date": RELEASE_DATE,
        "data_root": str(storage.get("data_root") or ""),
        "process_started": True,
        # A response from this ASGI app proves that this generation owns an
        # active HTTP handler.  The listener itself is validated by startup.
        "web_bound": True,
        "core_ready": core_ready,
        "storage_ready": storage_ready,
        "optional_services_ready": optional_ready,
        "fully_ready": bool(core_ready and optional_ready),
    }


def _optional_services_ready() -> bool:
    """Read worker availability only; never initialise optional services."""

    try:
        from quantmaster.runtime.worker import runtime_worker_status

        return bool(runtime_worker_status().get("available"))
    except (OSError, RuntimeError, TypeError, ValueError):
        return False


def runtime_status() -> dict[str, Any]:
    """Build the public, non-secret status shown in the existing UI drawer."""

    readiness = readiness_status()
    cfg = get_config()
    try:
        from quantmaster.runtime.worker import runtime_worker_status

        worker = runtime_worker_status()
    except (OSError, RuntimeError, TypeError, ValueError):
        worker = {"status": "unavailable", "available": False, "reason": "状态读取失败"}
    from quantmaster.server.storage_status import storage_status

    return {
        "web": {
            "pid": os.getpid(),
            "host": cfg.server.host,
            "port": cfg.server.port,
            "generation": os.environ.get("QM_WEB_GENERATION", "0"),
            "version": __version__,
            "process_started": readiness["process_started"],
            "web_bound": readiness["web_bound"],
        },
        "readiness": readiness,
        "supervisor": {
            "status": str(worker.get("supervisor", {}).get("status") or worker.get("status") or "unknown"),
            "pid": worker.get("supervisor", {}).get("pid"),
            "worker_pid": worker.get("pid"),
            "available": bool(worker.get("available")),
            "reason": str(worker.get("reason") or ""),
        },
        "storage": storage_status(),
        # The scheduler is owned by the durable worker.  Do not instantiate
        # AutomationRuntime from a status GET merely to ask it for this value.
        "scheduler": {
            "status": "running" if worker.get("available") else "unavailable",
            "managed_by": "runtime-worker",
            "worker_pid": worker.get("pid"),
        },
        "lifecycle": worker.get("lifecycle") or {
            "state": "running" if worker.get("available") else "stopping",
            "generation": str(worker.get("generation") or os.environ.get("QM_WEB_GENERATION", "0")),
            "phase": "accepting" if worker.get("available") else "worker_unavailable",
            "task_counts": {"active": 0, "converging": 0, "handoff": 0},
            "durable_queue": {"pending": 0},
            "deadline": {"phase": "", "remaining_seconds": 0.0},
            "timeout_issues": [],
            "tasks": [],
        },
        "checked_at": datetime.now(UTC).isoformat(),
    }
