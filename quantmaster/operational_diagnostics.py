"""Cross-domain stability metrics shared by HTTP diagnostics and ``qm doctor``."""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

logger = logging.getLogger(__name__)


def _database_schema_metrics() -> dict[str, dict[str, Any]]:
    from quantmaster.ai.news_storage import NEWS_SCHEMA_VERSION
    from quantmaster.automation.store import AUTOMATION_SCHEMA_VERSION
    from quantmaster.backtest.paper_accounts import PAPER_SCHEMA_VERSION
    from quantmaster.config import get_config
    from quantmaster.lab.store import LAB_SCHEMA_VERSION
    from quantmaster.portfolio.ledger import LEDGER_SCHEMA_VERSION
    from quantmaster.research.catalog import RESEARCH_SCHEMA_VERSION
    from quantmaster.runtime.sqlite import connect_sqlite

    root = get_config().data_root
    databases = {
        "news": (root / "news.sqlite", NEWS_SCHEMA_VERSION, "meta"),
        "paper": (root / "paper.sqlite", PAPER_SCHEMA_VERSION, "pragma"),
        "automation": (
            root / "automation.sqlite", AUTOMATION_SCHEMA_VERSION, "pragma",
        ),
        "lab": (root / "lab.sqlite", LAB_SCHEMA_VERSION, "pragma"),
        "research": (
            root / "research_lake" / "_meta" / "catalog.sqlite",
            RESEARCH_SCHEMA_VERSION,
            "pragma",
        ),
        "ledger": (root / "ledger_default.sqlite", LEDGER_SCHEMA_VERSION, "pragma"),
    }
    result: dict[str, dict[str, Any]] = {}
    for name, (path, expected, source) in databases.items():
        if not path.exists():
            result[name] = {"status": "cold", "current": 0, "expected": expected}
            continue
        try:
            with connect_sqlite(path, row_factory=True) as connection:
                if source == "meta":
                    row = connection.execute(
                        "SELECT value FROM news_store_meta WHERE key='schema_version'"
                    ).fetchone()
                    current = int(row[0]) if row else 0
                else:
                    current = int(connection.execute("PRAGMA user_version").fetchone()[0])
        except (OSError, sqlite3.Error, TypeError, ValueError):
            logger.warning("诊断无法读取数据库 schema name=%s", name, exc_info=True)
            result[name] = {
                "status": "unreadable",
                "current": None,
                "expected": expected,
                "error": "数据库不可读，请查看本机日志",
            }
            continue
        status = "ok" if current == expected else (
            "upgrade_required" if current < expected else "newer"
        )
        result[name] = {"status": status, "current": current, "expected": expected}
    return result


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

    from quantmaster.data.resilience import PROVIDER_SCHEDULER

    result["data_providers"] = PROVIDER_SCHEDULER.status()

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
        except RotationIntegrityError:
            logger.warning("诊断发现板块快照损坏 kind=%s", kind, exc_info=True)
            qualities[kind] = {
                "status": "corrupt", "issues": ["板块快照完整性校验失败"],
            }
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
    result["database_schemas"] = _database_schema_metrics()
    return result


def safe_operational_metrics() -> dict[str, Any]:
    """Diagnostic boundary: one failed database must not hide base health."""
    try:
        return collect_operational_metrics()
    except (ImportError, KeyError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        logger.warning("运行指标收集失败", exc_info=True)
        return {
            "status": "degraded",
            "error": "运行指标收集失败，请查看本机日志",
        }
