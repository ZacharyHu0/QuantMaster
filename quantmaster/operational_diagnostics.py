"""Cross-domain stability metrics shared by HTTP diagnostics and ``qm doctor``."""

from __future__ import annotations

import sqlite3
from typing import Any


def collect_operational_metrics() -> dict[str, Any]:
    result: dict[str, Any] = {}

    from quantmaster.ai.crawler import NewsStore
    from quantmaster.ai.llm import llm_gate_status

    news = NewsStore()
    queue = news.stats(days=1).get("queue") or {}
    result["news_analysis"] = {
        "processing": int(queue.get("processing") or 0),
        "claims": dict(queue.get("claims") or {}),
        "pending": int(queue.get("pending") or 0),
        "failed": int(queue.get("failed") or 0),
        "dead_letter": int(queue.get("dead_letter") or 0),
    }
    result["llm"] = llm_gate_status()

    from quantmaster.backtest.paper_accounts import PaperStore

    paper = PaperStore()
    runs = [
        value for account in paper.accounts(include_archived=True)
        if (value := paper.latest_auto_run(str(account["id"]))) is not None
    ]
    completed = [value for value in runs if value.get("status") == "completed"]
    result["paper_automation"] = {
        "accounts": len(runs),
        "last_success": max(
            (str(value.get("updated_at") or "") for value in completed), default="",
        ),
        "retrying": sum(value.get("status") == "failed" for value in runs),
        "needs_manual_recovery": sum(
            value.get("status") == "manual_recovery" for value in runs
        ),
        "running": sum(value.get("status") == "running" for value in runs),
    }

    from quantmaster.trading_sessions import expected_session

    result["trading_calendar"] = expected_session().as_dict()

    from quantmaster.rotation.store import RotationIntegrityError, RotationStore

    rotation = RotationStore()
    qualities: dict[str, Any] = {}
    for kind in ("temperature", "structure", "industries", "themes", "etf_flows"):
        try:
            snapshot = rotation.snapshot(kind)
        except RotationIntegrityError as exc:
            qualities[kind] = {"status": "corrupt", "issues": [str(exc)[:300]]}
            continue
        if snapshot is None:
            qualities[kind] = {"status": "cold", "issues": ["尚无快照"]}
            continue
        meta = snapshot.get("meta") or {}
        quality = meta.get("quality") or {}
        qualities[kind] = {
            "status": str(quality.get("status") or "unknown"),
            "as_of": str(meta.get("as_of") or ""),
            "algorithm_version": str(meta.get("algorithm_version") or ""),
            "coverage": quality.get("coverage"),
            "issues": list(quality.get("issues") or []),
        }
    result["rotation_snapshots"] = qualities
    return result


def safe_operational_metrics() -> dict[str, Any]:
    """Diagnostic boundary: one failed database must not hide base health."""
    try:
        return collect_operational_metrics()
    except (ImportError, KeyError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error) as exc:
        return {
            "status": "degraded",
            "error": f"{type(exc).__name__}: {str(exc)[:500]}",
        }
