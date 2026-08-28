"""Cross-domain stability metrics for HTTP diagnostics and ``qm doctor``."""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)

_SESSION_CODE_BY_COMPLETION = {
    "current_session_partial": "SESSION_PARTIAL",
    "current_session_closed_waiting_provider": "SESSION_CLOSED_WAIT_PROVIDER",
    "current_session_provider_published_waiting_ingest": "SESSION_WAITING_INGEST",
    "calendar_unavailable": "CALENDAR_UNVERIFIED",
}


def _parse_exact_timestamp(
    value: object, field: str,
) -> tuple[datetime | None, dict[str, str] | None]:
    text = str(value or "").strip()
    if not text:
        return None, None
    if not re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}", text):
        return None, {"field": field, "diagnostic_code": "TIME_UNINTERPRETABLE"}
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None, {"field": field, "diagnostic_code": "TIME_UNINTERPRETABLE"}
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None, {"field": field, "diagnostic_code": "TIME_UNZONED"}
    return parsed, None


def _valid_session_date(value: str) -> date | None:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == value else None


def _completion_state(
    validation: dict[str, Any], calendar: dict[str, Any], *, complete: bool, accepted: bool,
) -> str:
    if not validation:
        return str(calendar.get("completion") or "calendar_unavailable")
    if complete:
        return "current_session_complete"
    return "current_session_partial" if accepted else "current_session_closed_waiting_provider"


def _session_diagnostic_codes(
    completion_state: str, target_session: str, actual_session: str,
) -> list[str]:
    codes = [code] if (code := _SESSION_CODE_BY_COMPLETION.get(completion_state)) else []
    target_date = _valid_session_date(target_session)
    actual_date = _valid_session_date(actual_session)
    if target_date is not None and actual_date is not None and actual_date < target_date:
        codes.append("DATA_LATE")
    return codes


def _append_timestamp_codes(
    codes: list[str], timestamp_diagnostics: list[dict[str, str]],
) -> None:
    for diagnostic in timestamp_diagnostics:
        code = diagnostic["diagnostic_code"]
        if code not in codes:
            codes.append(code)


def _timestamp_metrics(
    validation: dict[str, Any], stockdb_status: dict[str, Any], diagnostic_codes: list[str],
) -> tuple[str, str, list[dict[str, str]], int | None, int | None]:
    provider_published_at = str(validation.get("provider_published_at") or "")
    ingested_at = str(stockdb_status.get("updated_at") or "")
    provider_time, provider_time_issue = _parse_exact_timestamp(
        provider_published_at, "provider_published_at",
    )
    ingest_time, ingest_time_issue = _parse_exact_timestamp(ingested_at, "ingested_at")
    timestamp_diagnostics = [
        item for item in (provider_time_issue, ingest_time_issue) if item is not None
    ]
    _append_timestamp_codes(diagnostic_codes, timestamp_diagnostics)
    if provider_time is None or ingest_time is None:
        return provider_published_at, ingested_at, timestamp_diagnostics, None, None
    latency = int((ingest_time - provider_time).total_seconds())
    if latency < 0:
        diagnostic_codes.append("PROVIDER_CLOCK_SKEW")
        return provider_published_at, ingested_at, timestamp_diagnostics, None, abs(latency)
    return provider_published_at, ingested_at, timestamp_diagnostics, latency, None


def _market_states(
    completion_state: str,
    provider_published_at: str,
    ingested_at: str,
    *,
    complete: bool,
    accepted: bool,
) -> tuple[str, str]:
    if completion_state == "current_session_closed_waiting_provider":
        provider_state = "waiting"
    elif provider_published_at:
        provider_state = "published"
    elif accepted or complete:
        provider_state = "published_time_unavailable"
    else:
        provider_state = "unavailable"
    if completion_state == "current_session_provider_published_waiting_ingest":
        ingest_state = "waiting"
    elif completion_state == "current_session_partial":
        ingest_state = "partial"
    elif complete and ingested_at:
        ingest_state = "complete"
    else:
        ingest_state = "unavailable"
    return provider_state, ingest_state


def _unavailable_market(timezone: str) -> dict[str, Any]:
    return {
        "market_timezone": timezone,
        "session_date": "",
        "session_phase": "unavailable",
        "latest_complete_session": "",
        "next_session": "",
        "next_session_reason": "未提供经验证的未来交易日历",
        "completion_state": "calendar_unavailable",
        "provider_state": "unavailable",
        "ingest_state": "unavailable",
        "provider_published_at": "",
        "ingested_at": "",
        "ingest_latency_seconds": None,
        "provider_clock_skew_seconds": None,
        "late_record_count": None,
        "diagnostic_code": "CALENDAR_UNVERIFIED",
        "diagnostic_codes": ["CALENDAR_UNVERIFIED"],
        "timestamp_diagnostics": [],
    }


def _market_session_metrics(
    stockdb_status: dict[str, Any],
    calendar_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    validation = dict(stockdb_status.get("validation") or {})
    calendar = dict(calendar_status or {})
    complete = bool(validation.get("complete"))
    accepted = bool(validation.get("accepted"))
    target_session = str(
        validation.get("target_session")
        or stockdb_status.get("target_session")
        or calendar.get("session")
        or ""
    )
    actual_session = str(
        validation.get("actual_session") or stockdb_status.get("actual_session") or ""
    )
    latest_complete = (
        actual_session if complete else str(stockdb_status.get("validated_session") or "")
    )
    if not latest_complete and bool(calendar.get("ready")):
        latest_complete = str(calendar.get("session") or "")
    completion_state = _completion_state(
        validation, calendar, complete=complete, accepted=accepted,
    )
    diagnostic_codes = _session_diagnostic_codes(
        completion_state, target_session, actual_session,
    )
    (
        provider_published_at,
        ingested_at,
        timestamp_diagnostics,
        ingest_latency_seconds,
        provider_clock_skew_seconds,
    ) = _timestamp_metrics(validation, stockdb_status, diagnostic_codes)
    provider_state, ingest_state = _market_states(
        completion_state,
        provider_published_at,
        ingested_at,
        complete=complete,
        accepted=accepted,
    )
    return {
        "CN": {
            "market_timezone": "Asia/Shanghai",
            "session_date": target_session,
            "session_phase": "unavailable",
            "latest_complete_session": latest_complete,
            "next_session": "",
            "next_session_reason": "未提供经验证的未来交易日历",
            "completion_state": completion_state,
            "coverage_ratio": validation.get("symbol_ratio"),
            "missing_symbol_count": int(validation.get("missing_symbol_count") or 0),
            "provider_state": provider_state,
            "ingest_state": ingest_state,
            "provider_published_at": provider_published_at,
            "ingested_at": ingested_at,
            "ingest_latency_seconds": ingest_latency_seconds,
            "provider_clock_skew_seconds": provider_clock_skew_seconds,
            "late_record_count": None,
            "diagnostic_code": diagnostic_codes[0] if diagnostic_codes else "",
            "diagnostic_codes": diagnostic_codes,
            "timestamp_diagnostics": timestamp_diagnostics,
        },
        "HK": _unavailable_market("Asia/Hong_Kong"),
        "US": _unavailable_market("America/New_York"),
    }


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

    from quantmaster.trading_sessions import expected_session

    calendar_status = expected_session().as_dict()
    result["trading_calendar"] = calendar_status

    from quantmaster.ai.crawler import NewsStore
    from quantmaster.ai.llm import llm_gate_status, news_llm_gate_status

    news = NewsStore()
    queue = news.stats(days=1).get("queue") or {}
    result["news_analysis"] = {
        "processing": int(queue.get("processing") or 0),
        "claims": dict(queue.get("claims") or {}),
        "pending": int(queue.get("pending") or 0),
        "failed": int(queue.get("failed") or 0),
        "dead_letter": int(queue.get("dead_letter") or 0),
    }
    result["llm"] = {**llm_gate_status(), "news": news_llm_gate_status()}

    from quantmaster.data.registry import data_source_capabilities
    from quantmaster.data.resilience import PROVIDER_SCHEDULER

    result["data_providers"] = PROVIDER_SCHEDULER.status()
    result["data_source_capabilities"] = data_source_capabilities()
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    result["free_stockdb_runtime"] = free_stockdb_runtime.status()
    stockdb_status = result["free_stockdb_runtime"]
    if market_sessions := _market_session_metrics(stockdb_status, calendar_status):
        result["market_sessions"] = market_sessions

    from quantmaster.backtest.paper_accounts import PaperStore

    paper = PaperStore()
    runs = [
        value for account in paper.accounts(include_archived=True)
        if (value := paper.reportable_auto_run(str(account["id"]))) is not None
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
