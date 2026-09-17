"""Read planned StockDB downtime from the existing owner's local mailbox."""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path

from quantmaster.config import get_config
from quantmaster.logging_config import redact_sensitive_text
from quantmaster.runtime.sqlite import connect_sqlite


def stockdb_wait_reason(*, planned_only: bool = False) -> str:
    """Never construct an owner, create a mailbox, or probe a supplier.

    Health accounting needs fresh affirmative planned-outage evidence;
    maintenance also waits on failed validation and unreadable owner state.
    """
    cfg = get_config()
    if not cfg.data.free_stockdb_managed:
        return ""
    configured = os.environ.get("QM_FREE_STOCKDB_CONTROL_PATH", "").strip()
    path = Path(configured) if configured else cfg.free_stockdb_root / ".quantmaster-control.sqlite"
    try:
        with connect_sqlite(path, read_only=True, timeout=0.2) as connection:
            row = connection.execute(
                "SELECT payload_json,updated_at FROM runtime_state WHERE singleton=1",
            ).fetchone()
        if row is None:
            raise ValueError("owner state missing")
        status = json.loads(row[0])
        if not isinstance(status, dict):
            raise ValueError("owner state invalid")
        phase = str(status.get("phase") or "")
        planned = status.get("state") == "updating" or phase in {
            "stopping", "syncing", "validating", "closing", "restarting",
        }
        failed = phase == "retry_wait" or status.get("update_result") in {
            "failed", "manual_required", "retry_wait",
        }
        if not planned and (planned_only or not failed):
            return ""
        if time.time() - float(row[1]) > 30:
            return "" if planned_only else "StockDB 更新状态已失联，等待监督器恢复确认"
        message = redact_sensitive_text(str(status.get("message") or phase))[:500]
        return f"等待 StockDB 更新与验收：{message}"
    except FileNotFoundError:
        return "StockDB 监督器尚未发布更新状态" if configured and not planned_only else ""
    except (OSError, sqlite3.Error, TypeError, ValueError):
        return "" if planned_only else "StockDB 本地更新状态暂不可读，等待监督器确认"
