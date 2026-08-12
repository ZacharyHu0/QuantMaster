from __future__ import annotations

import json
import secrets
import sqlite3
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from quantmaster.automation.models import AlertEvent, utc_now
from quantmaster.automation.policy import resolved_policy
from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite, execute_sql_script

DEFAULT_TARGETS = (
    ("weixin_owner", "weixin", "微信管理员私聊", "direct"),
    ("feishu_group", "feishu", "飞书提醒群", "group"),
    ("feishu_owner", "feishu", "飞书管理员私聊", "direct"),
)
AUTOMATION_SCHEMA_VERSION = 12

NEWS_INTERVAL_FIELDS = {
    "fast_news_scan": "fast_news_interval_minutes",
    "official_news_scan": "official_news_interval_minutes",
    "periodic_news_scan": "periodic_news_interval_minutes",
}

DEFAULT_JOBS = {
    "intraday_monitor": (True, {
        "type": "interval", "minutes": 5,
        "windows": ["09:35-11:30", "13:05-15:00"], "weekdays": True,
    }),
    "fast_news_scan": (True, {"type": "interval", "minutes": 20}),
    "official_news_scan": (True, {"type": "interval", "minutes": 120}),
    "periodic_news_scan": (True, {"type": "interval", "minutes": 360}),
    "daily_close_pipeline": (True, {"type": "daily", "times": ["15:20", "15:35", "15:50"], "weekdays": True}),
    "news_digest": (True, {"type": "daily", "times": ["11:35", "15:25", "21:00"]}),
    "news_dead_letter_recovery": (
        True, {"type": "daily", "times": ["03:45"]},
    ),
    "paper_rebalance_proposal": (False, {"type": "daily", "times": ["15:30"], "weekdays": True}),
}

class AutomationStore:
    def __init__(self, path: Path | None = None, *, read_only: bool = False):
        self.path = path or get_config().data_root / "automation.sqlite"
        self.read_only = bool(read_only)
        # Construction is deliberately side-effect free.  A writer must call
        # initialize() during an explicit startup phase; historical databases
        # are accepted only by the one-shot automation contract migrator.

    def _conn(self) -> sqlite3.Connection:
        if not self.read_only and not self.path.is_file():
            self.initialize()
        return connect_sqlite(
            self.path,
            timeout=0.25 if self.read_only else 5.0,
            row_factory=True,
            read_only=self.read_only,
        )

    def initialize(self) -> None:
        if self.read_only:
            raise RuntimeError("只读 AutomationStore 不能初始化")
        existed = self.path.is_file()
        if existed:
            with connect_sqlite(self.path) as conn:
                version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            if version != AUTOMATION_SCHEMA_VERSION:
                raise RuntimeError(
                    "automation_schema_requires_explicit_contract_migration: "
                    f"found={version}, expected={AUTOMATION_SCHEMA_VERSION}"
                )
        self.path.parent.mkdir(parents=True, exist_ok=True)

        def current_schema(conn: sqlite3.Connection) -> None:
            execute_sql_script(conn, """
                CREATE TABLE IF NOT EXISTS notification_targets (
                    id TEXT PRIMARY KEY, channel TEXT NOT NULL, label TEXT NOT NULL,
                    target TEXT NOT NULL DEFAULT '', account_id TEXT NOT NULL DEFAULT '',
                    chat_type TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
                    preset TEXT NOT NULL DEFAULT 'balanced', overrides TEXT NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL DEFAULT 'unbound', last_error TEXT NOT NULL DEFAULT '',
                    owner_actor TEXT NOT NULL DEFAULT '', context_token TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS bot_accounts (
                    id TEXT PRIMARY KEY, channel TEXT NOT NULL, account_id TEXT NOT NULL,
                    user_id TEXT NOT NULL DEFAULT '', base_url TEXT NOT NULL DEFAULT '',
                    secret_target TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'configured',
                    cursor TEXT NOT NULL DEFAULT '', last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL, last_validated_at TEXT NOT NULL DEFAULT '',
                    UNIQUE(channel,account_id));
                CREATE TABLE IF NOT EXISTS inbound_messages (
                    channel TEXT NOT NULL, message_id TEXT NOT NULL, received_at TEXT NOT NULL,
                    chat_type TEXT NOT NULL DEFAULT '', account_id TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(channel,message_id));
                CREATE TABLE IF NOT EXISTS conversation_messages (
                    channel TEXT NOT NULL, account_id TEXT NOT NULL, chat_id TEXT NOT NULL,
                    message_id TEXT NOT NULL, sender_id TEXT NOT NULL DEFAULT '',
                    sender_name TEXT NOT NULL DEFAULT '', text TEXT NOT NULL,
                    is_bot INTEGER NOT NULL DEFAULT 0, mentioned_bot INTEGER NOT NULL DEFAULT 0,
                    reply_to TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
                    PRIMARY KEY(channel,message_id));
                CREATE TABLE IF NOT EXISTS conversation_memories (
                    channel TEXT NOT NULL, account_id TEXT NOT NULL, chat_id TEXT NOT NULL,
                    memory TEXT NOT NULL DEFAULT '{}', source_count INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(channel,account_id,chat_id));
                CREATE TABLE IF NOT EXISTS job_templates (
                    name TEXT PRIMARY KEY, enabled INTEGER NOT NULL, schedule TEXT NOT NULL,
                    args TEXT NOT NULL DEFAULT '{}', next_run TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS task_runs (
                    id TEXT PRIMARY KEY, job_name TEXT NOT NULL, status TEXT NOT NULL,
                    actor TEXT NOT NULL DEFAULT 'scheduler', started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL DEFAULT '', result TEXT NOT NULL DEFAULT '{}',
                    error TEXT NOT NULL DEFAULT '');
                CREATE TABLE IF NOT EXISTS alert_events (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, score REAL NOT NULL,
                    severity TEXT NOT NULL, direction TEXT NOT NULL, occurred_at TEXT NOT NULL,
                    data_as_of TEXT NOT NULL, symbols TEXT NOT NULL, relevance TEXT NOT NULL,
                    evidence TEXT NOT NULL, source_urls TEXT NOT NULL, payload TEXT NOT NULL,
                    dedupe_key TEXT NOT NULL UNIQUE, expires_at TEXT NOT NULL DEFAULT '');
                CREATE TABLE IF NOT EXISTS delivery_attempts (
                    id TEXT PRIMARY KEY, event_id TEXT NOT NULL, target_id TEXT NOT NULL,
                    status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
                    delivered_at TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
                    lease_owner TEXT NOT NULL DEFAULT '', lease_token TEXT NOT NULL DEFAULT '',
                    lease_expires_at REAL NOT NULL DEFAULT 0, heartbeat_at REAL NOT NULL DEFAULT 0,
                    retry_after_at REAL NOT NULL DEFAULT 0,
                    diagnostic_code TEXT NOT NULL DEFAULT '', ambiguous_at TEXT NOT NULL DEFAULT '',
                    UNIQUE(event_id, target_id),
                    FOREIGN KEY(event_id) REFERENCES alert_events(id),
                    FOREIGN KEY(target_id) REFERENCES notification_targets(id));
                CREATE TABLE IF NOT EXISTS analysis_deliveries (
                    id TEXT PRIMARY KEY, job_id TEXT NOT NULL, analysis_id TEXT NOT NULL,
                    target_id TEXT NOT NULL, message_id TEXT NOT NULL DEFAULT '',
                    query TEXT NOT NULL DEFAULT '', mode TEXT NOT NULL DEFAULT 'deep',
                    status TEXT NOT NULL DEFAULT 'pending', event_seq INTEGER NOT NULL DEFAULT 0,
                    update_count INTEGER NOT NULL DEFAULT 0, appendix_cursor INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
                    delivered_at TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    lease_owner TEXT NOT NULL DEFAULT '', lease_token TEXT NOT NULL DEFAULT '',
                    lease_expires_at REAL NOT NULL DEFAULT 0, heartbeat_at REAL NOT NULL DEFAULT 0,
                    operation TEXT NOT NULL DEFAULT '', diagnostic_code TEXT NOT NULL DEFAULT '',
                    ambiguous_at TEXT NOT NULL DEFAULT '', UNIQUE(job_id,target_id),
                    FOREIGN KEY(target_id) REFERENCES notification_targets(id));
                CREATE TABLE IF NOT EXISTS pending_actions (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, actor TEXT NOT NULL,
                    route_key TEXT NOT NULL, payload TEXT NOT NULL, payload_hash TEXT NOT NULL,
                    code_hash TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
                    expires_at REAL NOT NULL, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, actor TEXT NOT NULL,
                    action TEXT NOT NULL, object_type TEXT NOT NULL, object_id TEXT NOT NULL,
                    before_value TEXT NOT NULL DEFAULT '{}', after_value TEXT NOT NULL DEFAULT '{}',
                    result TEXT NOT NULL, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS runtime_leases (
                    name TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS scheduler_cursors (
                    job_name TEXT PRIMARY KEY, window_end REAL NOT NULL, updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS market_breadth (
                    observed_at TEXT PRIMARY KEY, advance_ratio REAL NOT NULL,
                    sample_size INTEGER NOT NULL);
                CREATE INDEX IF NOT EXISTS idx_delivery_due
                    ON delivery_attempts(status, next_attempt_at);
                CREATE INDEX IF NOT EXISTS idx_analysis_delivery_due
                    ON analysis_deliveries(status, next_attempt_at);
                CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_conversation_chat_created
                    ON conversation_messages(channel,account_id,chat_id,created_at DESC);
            """)
        with connect_sqlite(self.path, row_factory=True) as conn:
            current_schema(conn)
            conn.execute(f"PRAGMA user_version={AUTOMATION_SCHEMA_VERSION}")
        self.ensure_defaults()

    @staticmethod
    def _decode_row(row: sqlite3.Row | None, json_fields: tuple[str, ...] = ()) -> dict | None:
        if row is None:
            return None
        value = dict(row)
        for key in json_fields:
            value[key] = json.loads(value.get(key) or "{}")
        for key in ("enabled",):
            if key in value:
                value[key] = bool(value[key])
        return value

    def ensure_defaults(self) -> None:
        now = utc_now()
        with self._conn() as conn:
            for target_id, channel, label, chat_type in DEFAULT_TARGETS:
                conn.execute(
                    "INSERT OR IGNORE INTO notification_targets "
                    "(id,channel,label,chat_type,updated_at) VALUES (?,?,?,?,?)",
                    (target_id, channel, label, chat_type, now),
                )
                conn.execute(
                    "UPDATE notification_targets SET label=? WHERE id=?",
                    (label, target_id),
                )
            for name, (enabled, schedule) in DEFAULT_JOBS.items():
                conn.execute(
                    "INSERT OR IGNORE INTO job_templates "
                    "(name,enabled,schedule,updated_at) VALUES (?,?,?,?)",
                    (name, int(enabled), json.dumps(schedule), now),
                )

    def sync_news_intervals(self) -> dict[str, int]:
        """Make the persisted scheduler projection match the settings document."""
        cfg = get_config().automation
        values = {
            name: int(getattr(cfg, field))
            for name, field in NEWS_INTERVAL_FIELDS.items()
        }
        now = utc_now()
        with self._conn() as conn:
            for name, minutes in values.items():
                conn.execute(
                    "UPDATE job_templates SET schedule=?,updated_at=? WHERE name=?",
                    (json.dumps({"type": "interval", "minutes": minutes}), now, name),
                )
        return values

    def targets(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM notification_targets ORDER BY rowid").fetchall()
        return [self._decode_row(row, ("overrides",)) or {} for row in rows]

    def target(self, target_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM notification_targets WHERE id=?", (target_id,)).fetchone()
        return self._decode_row(row, ("overrides",))

    def target_by_route(self, channel: str, account_id: str, target: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM notification_targets WHERE channel=? AND account_id=? AND target=?",
                (channel, account_id, target),
            ).fetchone()
        return self._decode_row(row, ("overrides",))

    def update_target_policy(self, target_id: str, *, preset: str, overrides: dict,
                             enabled: bool | None = None, actor: str = "web") -> dict:
        resolved_policy(preset, overrides)
        before = self.target(target_id)
        if before is None:
            raise KeyError("推送目标不存在")
        is_enabled = before["enabled"] if enabled is None else bool(enabled)
        now = utc_now()
        with self._conn() as conn:
            conn.execute(
                "UPDATE notification_targets SET preset=?,overrides=?,enabled=?,updated_at=? WHERE id=?",
                (preset, json.dumps(overrides, ensure_ascii=False), int(is_enabled), now, target_id),
            )
        after = self.target(target_id)
        self.audit(actor, "update_policy", "target", target_id, before, after, "ok")
        return after or {}

    def bind_target(self, target_id: str, *, target: str, account_id: str, owner_actor: str,
                    actor: str) -> dict:
        before = self.target(target_id)
        if before is None:
            raise KeyError("推送目标不存在")
        with self._conn() as conn:
            conn.execute(
                "UPDATE notification_targets SET target=?,account_id=?,owner_actor=?,"
                "context_token='',status='healthy',last_error='',updated_at=? WHERE id=?",
                (target, account_id, owner_actor, utc_now(), target_id),
            )
        after = self.target(target_id)
        self.audit(actor, "bind", "target", target_id, before, after, "ok")
        return after or {}

    def set_target_status(self, target_id: str, status: str, error: str = "") -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE notification_targets SET status=?,last_error=?,updated_at=? WHERE id=?",
                (status, error[:500], utc_now(), target_id),
            )

    def update_context_token(self, target_id: str, context_token: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE notification_targets SET context_token=?,status='healthy',last_error='',"
                "updated_at=? WHERE id=?", (context_token, utc_now(), target_id))

    def save_bot_account(self, *, channel: str, account_id: str, user_id: str = "",
                         base_url: str = "", secret_target: str = "", status: str = "configured",
                         error: str = "") -> dict:
        row_id = f"{channel}:{account_id}"
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO bot_accounts "
                "(id,channel,account_id,user_id,base_url,secret_target,status,last_error,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
                "user_id=excluded.user_id,base_url=excluded.base_url,secret_target=excluded.secret_target,"
                "status=excluded.status,last_error=excluded.last_error,updated_at=excluded.updated_at",
                (row_id, channel, account_id, user_id, base_url, secret_target, status,
                 error[:500], utc_now()),
            )
            row = conn.execute("SELECT * FROM bot_accounts WHERE id=?", (row_id,)).fetchone()
        return dict(row)

    def bot_accounts(self, channel: str | None = None) -> list[dict]:
        query = "SELECT * FROM bot_accounts"
        params: tuple = ()
        if channel:
            query += " WHERE channel=?"
            params = (channel,)
        query += " ORDER BY updated_at DESC"
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def bot_account(self, channel: str, account_id: str | None = None) -> dict | None:
        accounts = self.bot_accounts(channel)
        if account_id:
            return next((item for item in accounts if item["account_id"] == account_id), None)
        return accounts[0] if accounts else None

    def delete_bot_accounts(self, channel: str, *, mark_targets: bool = True) -> list[dict]:
        accounts = self.bot_accounts(channel)
        with self._conn() as conn:
            conn.execute("DELETE FROM bot_accounts WHERE channel=?", (channel,))
            if mark_targets:
                conn.execute(
                    "UPDATE notification_targets SET status='needs_rebind',"
                    "last_error='channel_credentials_removed',updated_at=? WHERE channel=?",
                    (utc_now(), channel),
                )
        return accounts

    def channel_credentials_removed(self, channel: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM notification_targets WHERE channel=? "
                "AND last_error='channel_credentials_removed' LIMIT 1",
                (channel,),
            ).fetchone()
        return row is not None

    def clear_channel_removal_marker(self, channel: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE notification_targets SET status=CASE WHEN target='' THEN 'unbound' "
                "ELSE 'needs_rebind' END,last_error='',updated_at=? WHERE channel=? "
                "AND last_error='channel_credentials_removed'",
                (utc_now(), channel),
            )

    def delete_other_bot_accounts(self, channel: str, account_id: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "DELETE FROM bot_accounts WHERE channel=? AND account_id<>?",
                (channel, account_id),
            )

    def update_bot_cursor(self, channel: str, account_id: str, cursor: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE bot_accounts SET cursor=?,updated_at=? WHERE channel=? AND account_id=?",
                (cursor, utc_now(), channel, account_id),
            )

    def set_bot_status(self, channel: str, account_id: str, status: str, error: str = "") -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE bot_accounts SET status=?,last_error=?,updated_at=? "
                "WHERE channel=? AND account_id=?",
                (status, error[:500], utc_now(), channel, account_id),
            )

    def set_bot_validation(self, channel: str, account_id: str, status: str,
                           error: str = "") -> None:
        """Persist only a sanitized validation outcome, never credentials or SDK payloads."""
        with self._conn() as conn:
            conn.execute(
                "UPDATE bot_accounts SET status=?,last_error=?,last_validated_at=?,updated_at=? "
                "WHERE channel=? AND account_id=?",
                (status, error[:500], utc_now(), utc_now(), channel, account_id),
            )

    def claim_inbound(self, channel: str, message_id: str, *, chat_type: str = "",
                      account_id: str = "") -> bool:
        with self._conn() as conn:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO inbound_messages"
                "(channel,message_id,received_at,chat_type,account_id) VALUES (?,?,?,?,?)",
                (channel, message_id, utc_now(), chat_type, account_id),
            )
        return cursor.rowcount == 1

    def inbound_status(self, channel: str, chat_type: str = "") -> dict:
        where = "channel=?"
        params: tuple[str, ...] = (channel,)
        if chat_type:
            where += " AND chat_type=?"
            params = (channel, chat_type)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS total,MAX(received_at) AS last_received_at "
                f"FROM inbound_messages WHERE {where}", params,
            ).fetchone()
        return {
            "total": int(row["total"] or 0),
            "last_received_at": str(row["last_received_at"] or ""),
        }

    def remember_conversation_message(
            self, *, channel: str, account_id: str, chat_id: str, message_id: str,
            sender_id: str, sender_name: str, text: str, is_bot: bool = False,
            mentioned_bot: bool = False, reply_to: str = "", created_at: str = "",
    ) -> bool:
        """保存已绑定会话文本；过长内容会在被 @ 时压缩为话题记忆。"""
        value = text.strip()[:4000]
        if not chat_id or not message_id or not value:
            return False
        with self._conn() as conn:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO conversation_messages "
                "(channel,account_id,chat_id,message_id,sender_id,sender_name,text,is_bot,"
                "mentioned_bot,reply_to,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (channel, account_id, chat_id, message_id, sender_id, sender_name[:100],
                 value, int(is_bot), int(mentioned_bot), reply_to, created_at or utc_now()),
            )
        return cursor.rowcount == 1

    def conversation_context(
            self, *, channel: str, account_id: str, chat_id: str,
            exclude_message_id: str = "", limit: int = 80, oldest: bool = False,
    ) -> list[dict]:
        query = (
            "SELECT * FROM conversation_messages WHERE channel=? AND account_id=? "
            "AND chat_id=?"
        )
        params: list[Any] = [channel, account_id, chat_id]
        if exclude_message_id:
            query += " AND message_id<>?"
            params.append(exclude_message_id)
        order = "created_at ASC,rowid ASC" if oldest else "created_at DESC,rowid DESC"
        query += f" ORDER BY {order} LIMIT ?"
        params.append(max(1, min(int(limit), 500)))
        with self._conn() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        values = [dict(row) for row in rows]
        return values if oldest else list(reversed(values))

    def conversation_stats(self, *, channel: str, account_id: str, chat_id: str) -> dict:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count,COALESCE(SUM(LENGTH(text)),0) AS characters "
                "FROM conversation_messages WHERE channel=? AND account_id=? AND chat_id=?",
                (channel, account_id, chat_id),
            ).fetchone()
        return {"count": int(row["count"]), "characters": int(row["characters"])}

    def conversation_memory(self, *, channel: str, account_id: str, chat_id: str) -> dict:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT memory,source_count,updated_at FROM conversation_memories "
                "WHERE channel=? AND account_id=? AND chat_id=?",
                (channel, account_id, chat_id),
            ).fetchone()
        if not row:
            return {"memory": {}, "source_count": 0, "updated_at": ""}
        try:
            memory = json.loads(row["memory"] or "{}")
        except json.JSONDecodeError:
            memory = {}
        return {
            "memory": memory, "source_count": int(row["source_count"]),
            "updated_at": str(row["updated_at"]),
        }

    def compact_conversation(
            self, *, channel: str, account_id: str, chat_id: str,
            message_ids: list[str], memory: dict, expected_source_count: int | None = None,
    ) -> int:
        """原子保存话题记忆并删除已被该记忆覆盖的原文。"""
        unique_ids = list(dict.fromkeys(value for value in message_ids if value))
        if not unique_ids:
            return 0
        placeholders = ",".join("?" for _ in unique_ids)
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT source_count FROM conversation_memories "
                "WHERE channel=? AND account_id=? AND chat_id=?",
                (channel, account_id, chat_id),
            ).fetchone()
            if (
                expected_source_count is not None
                and int((existing or {"source_count": 0})["source_count"])
                != int(expected_source_count)
            ):
                return 0
            present = conn.execute(
                "SELECT COUNT(*) AS count FROM conversation_messages WHERE channel=? "
                f"AND account_id=? AND chat_id=? AND message_id IN ({placeholders})",
                (channel, account_id, chat_id, *unique_ids),
            ).fetchone()
            covered = int(present["count"] or 0)
            if not covered:
                return 0
            conn.execute(
                "INSERT INTO conversation_memories "
                "(channel,account_id,chat_id,memory,source_count,updated_at) VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(channel,account_id,chat_id) DO UPDATE SET "
                "memory=excluded.memory,source_count=excluded.source_count,"
                "updated_at=excluded.updated_at",
                (channel, account_id, chat_id, json.dumps(memory, ensure_ascii=False),
                 int((existing or {"source_count": 0})["source_count"]) + covered, utc_now()),
            )
            conn.execute(
                "DELETE FROM conversation_messages WHERE channel=? AND account_id=? "
                f"AND chat_id=? AND message_id IN ({placeholders})",
                (channel, account_id, chat_id, *unique_ids),
            )
        return covered

    def jobs(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM job_templates ORDER BY rowid").fetchall()
        return [self._decode_row(row, ("schedule", "args")) or {} for row in rows]

    def job(self, name: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM job_templates WHERE name=?", (name,)).fetchone()
        return self._decode_row(row, ("schedule", "args"))

    def update_job(self, name: str, enabled: bool, schedule: dict, actor: str) -> dict:
        before = self.job(name)
        if before is None:
            raise KeyError("任务模板不存在")
        with self._conn() as conn:
            conn.execute(
                "UPDATE job_templates SET enabled=?,schedule=?,updated_at=? WHERE name=?",
                (int(enabled), json.dumps(schedule), utc_now(), name),
            )
        after = self.job(name)
        self.audit(actor, "update_schedule", "job", name, before, after, "ok")
        return after or {}

    def set_next_run(self, name: str, next_run: str) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE job_templates SET next_run=? WHERE name=?", (next_run, name))

    def scheduler_cursor(self, name: str) -> float:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT window_end FROM scheduler_cursors WHERE job_name=?", (name,),
            ).fetchone()
        return float(row[0]) if row else 0.0

    def advance_scheduler_cursor(self, name: str, window_end: float) -> bool:
        """Monotonically persist the last discovered interval boundary."""

        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT window_end FROM scheduler_cursors WHERE job_name=?", (name,),
            ).fetchone()
            previous = float(row[0]) if row else 0.0
            if previous >= float(window_end):
                return False
            conn.execute(
                "INSERT INTO scheduler_cursors(job_name,window_end,updated_at) VALUES (?,?,?) "
                "ON CONFLICT(job_name) DO UPDATE SET window_end=excluded.window_end,"
                "updated_at=excluded.updated_at",
                (name, float(window_end), utc_now()),
            )
        return True

    def start_run(self, job_name: str, actor: str) -> str:
        run_id = uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO task_runs (id,job_name,status,actor,started_at) VALUES (?,?,?,?,?)",
                (run_id, job_name, "running", actor, utc_now()),
            )
        return run_id

    def finish_run(self, run_id: str, *, result: dict | None = None, error: str = "") -> None:
        status = "failed" if error else "succeeded"
        with self._conn() as conn:
            conn.execute(
                "UPDATE task_runs SET status=?,finished_at=?,result=?,error=? WHERE id=?",
                (status, utc_now(), json.dumps(result or {}, ensure_ascii=False), error[:2000], run_id),
            )

    def recent_runs(self, limit: int = 30) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM task_runs ORDER BY started_at DESC LIMIT ?", (limit,)).fetchall()
        return [self._decode_row(row, ("result",)) or {} for row in rows]

    def save_event(self, event: AlertEvent) -> tuple[dict, bool]:
        value = event.to_dict()
        with self._conn() as conn:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO alert_events "
                "(id,kind,score,severity,direction,occurred_at,data_as_of,symbols,relevance,"
                "evidence,source_urls,payload,dedupe_key,expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (event.id, event.kind, event.score, event.severity, event.direction,
                 event.occurred_at, event.data_as_of, json.dumps(event.symbols), event.relevance,
                 json.dumps(event.evidence, ensure_ascii=False), json.dumps(event.source_urls),
                 json.dumps(event.payload, ensure_ascii=False), event.dedupe_key, event.expires_at),
            )
            created = cursor.rowcount == 1
            row = conn.execute(
                "SELECT * FROM alert_events WHERE dedupe_key=?", (event.dedupe_key,)).fetchone()
        return self._decode_row(row, ("symbols", "evidence", "source_urls", "payload")) or value, created

    def recent_events(self, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM alert_events ORDER BY occurred_at DESC LIMIT ?", (limit,)).fetchall()
        return [
            self._decode_row(row, ("symbols", "evidence", "source_urls", "payload")) or {}
            for row in rows
        ]

    def enqueue(self, event_id: str, target_id: str) -> bool:
        with self._conn() as conn:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO delivery_attempts "
                "(id,event_id,target_id,status,next_attempt_at,created_at) VALUES (?,?,?,'pending',?,?)",
                (uuid.uuid4().hex, event_id, target_id, time.time(), utc_now()),
            )
        return cursor.rowcount == 1

    def claim_deliveries(
        self,
        owner: str,
        *,
        limit: int = 20,
        lease_seconds: float = 120.0,
        channels: set[str] | None = None,
    ) -> list[dict]:
        """Atomically claim a small due batch and fence expired send attempts.

        An expired ``claimed`` row is safe to recover because no external call
        started.  An expired ``sending`` row is deliberately quarantined as
        ``ambiguous``: the remote side may have accepted it before the process
        disappeared, so an automatic resend would be unsafe.
        """
        current = time.time()
        token = uuid.uuid4().hex
        channel_values = sorted(str(value) for value in channels or () if value)
        channel_sql = ""
        params: list[Any] = [current]
        if channels is not None:
            if not channel_values:
                return []
            placeholders = ",".join("?" for _ in channel_values)
            channel_sql = f" AND t.channel IN ({placeholders})"
            params.extend(channel_values)
        params.append(max(1, min(100, int(limit))))
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE delivery_attempts SET status='retry_wait',lease_owner='',"
                "lease_token='',lease_expires_at=0,heartbeat_at=0,next_attempt_at=? "
                "WHERE status='claimed' AND lease_expires_at<=?",
                (current, current),
            )
            conn.execute(
                "UPDATE delivery_attempts SET status='ambiguous',lease_owner='',"
                "lease_token='',lease_expires_at=0,heartbeat_at=0,ambiguous_at=?,"
                "diagnostic_code='delivery_ack_unknown',last_error="
                "CASE WHEN last_error='' THEN 'delivery outcome unknown after worker lease expired' "
                "ELSE last_error END WHERE status='sending' AND lease_expires_at<=?",
                (utc_now(), current),
            )
            rows = conn.execute(
                "SELECT d.id FROM delivery_attempts d "
                "JOIN notification_targets t ON t.id=d.target_id "
                "WHERE d.status IN ('pending','retry_wait') AND d.next_attempt_at<=? "
                "AND t.enabled=1"
                f"{channel_sql} ORDER BY d.next_attempt_at,d.created_at LIMIT ?",
                params,
            ).fetchall()
            selected = [str(row["id"]) for row in rows]
            if not selected:
                return []
            placeholders = ",".join("?" for _ in selected)
            expiry = current + max(15.0, float(lease_seconds))
            conn.execute(
                "UPDATE delivery_attempts SET status='claimed',lease_owner=?,lease_token=?,"
                "lease_expires_at=?,heartbeat_at=? "
                f"WHERE id IN ({placeholders}) AND status IN ('pending','retry_wait')",
                (owner, token, expiry, current, *selected),
            )
            claimed = conn.execute(
                "SELECT d.*,e.kind,e.score,e.severity,e.direction,e.occurred_at,e.data_as_of,"
                "e.symbols,e.relevance,e.evidence,e.source_urls,e.payload,t.channel,t.target,"
                "t.account_id,t.context_token,t.label,t.status AS target_status "
                "FROM delivery_attempts d JOIN alert_events e ON e.id=d.event_id "
                "JOIN notification_targets t ON t.id=d.target_id "
                f"WHERE d.id IN ({placeholders}) AND d.lease_owner=? AND d.lease_token=? "
                "ORDER BY d.next_attempt_at,d.created_at",
                (*selected, owner, token),
            ).fetchall()
        return [
            self._decode_row(row, ("symbols", "evidence", "source_urls", "payload")) or {}
            for row in claimed
        ]

    def begin_delivery(self, delivery_id: str, owner: str, token: str) -> bool:
        current = time.time()
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE delivery_attempts SET status='sending',attempts=attempts+1,"
                "heartbeat_at=? WHERE id=? AND status='claimed' AND lease_owner=? "
                "AND lease_token=? AND lease_expires_at>?",
                (current, delivery_id, owner, token, current),
            ).rowcount
        return changed == 1

    def heartbeat_delivery(
        self, delivery_id: str, owner: str, token: str, *, lease_seconds: float = 120.0,
    ) -> bool:
        current = time.time()
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE delivery_attempts SET heartbeat_at=?,lease_expires_at=? "
                "WHERE id=? AND status IN ('claimed','sending') AND lease_owner=? "
                "AND lease_token=? AND lease_expires_at>?",
                (current, current + max(15.0, float(lease_seconds)), delivery_id,
                 owner, token, current),
            ).rowcount
        return changed == 1

    def delivery_success(self, delivery_id: str, owner: str, token: str) -> bool:
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE delivery_attempts SET status='sent',delivered_at=?,last_error='',"
                "diagnostic_code='',retry_after_at=0,lease_owner='',lease_token='',"
                "lease_expires_at=0,heartbeat_at=0 WHERE id=? AND status='sending' "
                "AND lease_owner=? AND lease_token=?",
                (utc_now(), delivery_id, owner, token),
            ).rowcount
        return changed == 1

    def delivery_failure(
        self,
        delivery_id: str,
        owner: str,
        token: str,
        error: str,
        *,
        permanent: bool = False,
        ambiguous: bool = False,
        retry_after_at: float = 0.0,
        diagnostic_code: str = "delivery_failed",
    ) -> str:
        delays = (30, 120, 600, 1800)
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT attempts FROM delivery_attempts WHERE id=? AND status='sending' "
                "AND lease_owner=? AND lease_token=?",
                (delivery_id, owner, token),
            ).fetchone()
            if row is None:
                return "lease_lost"
            attempts = max(1, int(row["attempts"] or 0))
            terminal = permanent or attempts >= 5
            status = "ambiguous" if ambiguous else "dead_letter" if terminal else "retry_wait"
            delay = delays[min(attempts - 1, len(delays) - 1)]
            retry_at = 0.0 if status != "retry_wait" else max(
                time.time() + delay, float(retry_after_at or 0.0),
            )
            conn.execute(
                "UPDATE delivery_attempts SET status=?,next_attempt_at=?,retry_after_at=?,"
                "last_error=?,diagnostic_code=?,ambiguous_at=?,lease_owner='',lease_token='',"
                "lease_expires_at=0,heartbeat_at=0 WHERE id=? AND status='sending' "
                "AND lease_owner=? AND lease_token=?",
                (status, retry_at, float(retry_after_at or 0.0), error[:1000],
                 diagnostic_code[:80], utc_now() if ambiguous else "", delivery_id,
                 owner, token),
            )
        return status

    def hourly_delivery_count(self, target_id: str) -> int:
        cutoff = (datetime.now(UTC) - timedelta(hours=1)).isoformat(timespec="seconds")
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM delivery_attempts WHERE target_id=? "
                "AND status='sent' AND delivered_at>=?", (target_id, cutoff)).fetchone()
        return int(row[0])

    def last_delivered_event(self, target_id: str, kind: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT e.direction,e.occurred_at,d.delivered_at,e.score FROM delivery_attempts d "
                "JOIN alert_events e ON e.id=d.event_id WHERE d.target_id=? AND e.kind=? "
                "AND d.status='sent' ORDER BY d.delivered_at DESC LIMIT 1",
                (target_id, kind),
            ).fetchone()
        return dict(row) if row else None

    def owner_actors(self) -> set[str]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT owner_actor FROM notification_targets WHERE owner_actor<>''"
            ).fetchall()
        return {str(row[0]) for row in rows}

    def create_binding_code(self, target_id: str, actor: str = "web") -> dict:
        if self.target(target_id) is None:
            raise KeyError("推送目标不存在")
        code = secrets.token_hex(4).upper()
        action_id = uuid.uuid4().hex
        expires_at = time.time() + 600
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO pending_actions "
                "(id,kind,actor,route_key,payload,payload_hash,code_hash,expires_at,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (action_id, "binding", actor, "", json.dumps({"target_id": target_id}), "",
                 self._code_hash(code), expires_at, utc_now()),
            )
        return {
            "id": action_id, "code": code, "expires_in": 600,
            "expires_at": expires_at, "target_id": target_id,
        }

    def binding_action(self, action_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM pending_actions WHERE id=? AND kind='binding'", (action_id,)
            ).fetchone()
            if row and row["status"] == "pending" and row["expires_at"] < time.time():
                conn.execute(
                    "UPDATE pending_actions SET status='expired' WHERE id=?", (action_id,)
                )
                row = conn.execute(
                    "SELECT * FROM pending_actions WHERE id=?", (action_id,)
                ).fetchone()
        return self._decode_row(row, ("payload",))

    def binding_for_code(self, code: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM pending_actions WHERE kind='binding' AND code_hash=? "
                "AND status='pending' AND expires_at>=?",
                (self._code_hash(code), time.time()),
            ).fetchone()
        return self._decode_row(row, ("payload",))

    def consume_binding_code(self, code: str, *, expected_id: str = "") -> dict | None:
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM pending_actions WHERE kind='binding' AND code_hash=? "
                "AND status='pending' AND expires_at>=?", (self._code_hash(code), time.time())).fetchone()
            if row is None or (expected_id and row["id"] != expected_id):
                return None
            conn.execute("UPDATE pending_actions SET status='consumed' WHERE id=?", (row["id"],))
        return self._decode_row(row, ("payload",))

    def create_pending_action(self, *, kind: str, actor: str, route_key: str,
                              payload: dict, ttl_seconds: int = 300) -> dict:
        code = f"{secrets.randbelow(1_000_000):06d}"
        action_id = uuid.uuid4().hex
        payload_hash = __import__("hashlib").sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO pending_actions "
                "(id,kind,actor,route_key,payload,payload_hash,code_hash,expires_at,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (action_id, kind, actor, route_key, json.dumps(payload, ensure_ascii=False),
                 payload_hash, self._code_hash(code), time.time() + ttl_seconds, utc_now()),
            )
        return {"intent_id": action_id, "code": code, "expires_in": ttl_seconds,
                "payload": payload, "payload_hash": payload_hash}

    def consume_pending_action(self, *, action_id: str, code: str, actor: str,
                               route_key: str) -> dict:
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM pending_actions WHERE id=?", (action_id,)).fetchone()
            if row is None:
                raise ValueError("确认意图不存在")
            if row["status"] != "pending":
                raise ValueError("确认意图已使用或已失效")
            if row["expires_at"] < time.time():
                conn.execute("UPDATE pending_actions SET status='expired' WHERE id=?", (action_id,))
                raise ValueError("确认码已过期")
            if row["actor"] != actor or row["route_key"] != route_key:
                raise PermissionError("确认意图与当前操作者或私聊不匹配")
            if not secrets.compare_digest(row["code_hash"], self._code_hash(code)):
                raise ValueError("确认码错误")
            conn.execute("UPDATE pending_actions SET status='consumed' WHERE id=?", (action_id,))
        return self._decode_row(row, ("payload",)) or {}

    def latest_pending_action(self, actor: str, route_key: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM pending_actions WHERE actor=? AND route_key=? AND status='pending' "
                "AND expires_at>=? ORDER BY created_at DESC LIMIT 1",
                (actor, route_key, time.time()),
            ).fetchone()
        return self._decode_row(row, ("payload",))

    @staticmethod
    def _code_hash(code: str) -> str:
        return __import__("hashlib").sha256(code.strip().upper().encode()).hexdigest()

    def audit(self, actor: str, action: str, object_type: str, object_id: str,
              before: Any, after: Any, result: str) -> None:
        def safe(value: Any) -> str:
            return json.dumps(value if value is not None else {}, ensure_ascii=False, default=str)
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO audit_log "
                "(actor,action,object_type,object_id,before_value,after_value,result,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (actor, action, object_type, object_id, safe(before), safe(after), result, utc_now()),
            )

    def audit_entries(self, limit: int = 100) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [self._decode_row(row, ("before_value", "after_value")) or {} for row in rows]

    def acquire_lease(self, name: str, owner: str, ttl: float = 30.0) -> bool:
        now = time.time()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT owner,expires_at FROM runtime_leases WHERE name=?", (name,)).fetchone()
            if row and row["owner"] != owner and row["expires_at"] > now:
                return False
            conn.execute(
                "INSERT INTO runtime_leases(name,owner,expires_at) VALUES (?,?,?) "
                "ON CONFLICT(name) DO UPDATE SET owner=excluded.owner,expires_at=excluded.expires_at",
                (name, owner, now + ttl),
            )
        return True

    def save_analysis_delivery(
        self, *, job_id: str, analysis_id: str, target_id: str, message_id: str,
        query: str = "", mode: str = "deep",
    ) -> dict:
        """Persist Feishu routing so restarts resume the original progress card."""
        now, delivery_id = utc_now(), uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO analysis_deliveries "
                "(id,job_id,analysis_id,target_id,message_id,query,mode,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id,target_id) DO UPDATE SET "
                "analysis_id=excluded.analysis_id,message_id=CASE WHEN excluded.message_id<>'' "
                "THEN excluded.message_id ELSE analysis_deliveries.message_id END,"
                "query=excluded.query,mode=excluded.mode,"
                "status=CASE WHEN analysis_deliveries.status IN "
                "('sent','dead_letter','ambiguous') THEN analysis_deliveries.status "
                "ELSE 'pending' END,updated_at=excluded.updated_at",
                (delivery_id, job_id, analysis_id, target_id, message_id,
                 query[:80], mode, now, now),
            )
            row = conn.execute(
                "SELECT * FROM analysis_deliveries WHERE job_id=? AND target_id=?",
                (job_id, target_id),
            ).fetchone()
        return dict(row)

    def analysis_delivery(self, job_id: str, target_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM analysis_deliveries WHERE job_id=? AND target_id=?",
                (job_id, target_id),
            ).fetchone()
        return dict(row) if row else None

    def claim_analysis_deliveries(
        self, owner: str, *, limit: int = 20, lease_seconds: float = 120.0,
    ) -> list[dict]:
        current = time.time()
        token = uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE analysis_deliveries SET status='retry_wait',lease_owner='',"
                "lease_token='',lease_expires_at=0,heartbeat_at=0,operation='',"
                "next_attempt_at=? WHERE status='claimed' AND lease_expires_at<=?",
                (current, current),
            )
            conn.execute(
                "UPDATE analysis_deliveries SET status='retry_wait',lease_owner='',"
                "lease_token='',lease_expires_at=0,heartbeat_at=0,operation='',"
                "next_attempt_at=?,diagnostic_code='worker_lease_expired' "
                "WHERE status='sending' AND operation='update' AND lease_expires_at<=?",
                (current, current),
            )
            conn.execute(
                "UPDATE analysis_deliveries SET status='ambiguous',lease_owner='',"
                "lease_token='',lease_expires_at=0,heartbeat_at=0,ambiguous_at=?,"
                "diagnostic_code='appendix_ack_unknown',last_error="
                "CASE WHEN last_error='' THEN "
                "'appendix delivery outcome unknown after worker lease expired' "
                "ELSE last_error END WHERE status='sending' AND operation='appendix' "
                "AND lease_expires_at<=?",
                (utc_now(), current),
            )
            rows = conn.execute(
                "SELECT d.id FROM analysis_deliveries d "
                "JOIN notification_targets t ON t.id=d.target_id "
                "WHERE d.status IN ('pending','retry_wait') AND d.next_attempt_at<=? "
                "AND t.enabled=1 AND t.channel='feishu' ORDER BY d.updated_at LIMIT ?",
                (current, max(1, min(100, int(limit)))),
            ).fetchall()
            selected = [str(row["id"]) for row in rows]
            if not selected:
                return []
            placeholders = ",".join("?" for _ in selected)
            conn.execute(
                "UPDATE analysis_deliveries SET status='claimed',lease_owner=?,lease_token=?,"
                "lease_expires_at=?,heartbeat_at=? "
                f"WHERE id IN ({placeholders}) AND status IN ('pending','retry_wait')",
                (owner, token, current + max(15.0, float(lease_seconds)), current, *selected),
            )
            claimed = conn.execute(
                "SELECT d.*,t.channel,t.account_id,t.target,t.chat_type,t.status AS target_status "
                "FROM analysis_deliveries d JOIN notification_targets t ON t.id=d.target_id "
                f"WHERE d.id IN ({placeholders}) AND d.lease_owner=? AND d.lease_token=? "
                "ORDER BY d.updated_at",
                (*selected, owner, token),
            ).fetchall()
        return [dict(row) for row in claimed]

    def due_analysis_deliveries(self, limit: int = 20) -> list[dict]:
        """Compatibility entry point that still performs an atomic fenced claim."""
        return self.claim_analysis_deliveries("legacy-analysis-dispatch", limit=limit)

    def begin_analysis_delivery(
        self, delivery_id: str, owner: str, token: str, *, operation: str,
    ) -> bool:
        if operation not in {"inspect", "update", "appendix"}:
            raise ValueError("分析投递操作非法")
        current = time.time()
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE analysis_deliveries SET status='sending',operation=?,"
                "attempts=attempts+1,heartbeat_at=? WHERE id=? AND status='claimed' "
                "AND lease_owner=? AND lease_token=? AND lease_expires_at>?",
                (operation, current, delivery_id, owner, token, current),
            ).rowcount
        return changed == 1

    def heartbeat_analysis_delivery(
        self, delivery_id: str, owner: str, token: str, *, lease_seconds: float = 120.0,
    ) -> bool:
        current = time.time()
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE analysis_deliveries SET heartbeat_at=?,lease_expires_at=? "
                "WHERE id=? AND status IN ('claimed','sending') AND lease_owner=? "
                "AND lease_token=? AND lease_expires_at>?",
                (current, current + max(15.0, float(lease_seconds)), delivery_id,
                 owner, token, current),
            ).rowcount
        return changed == 1

    def set_analysis_delivery_operation(
        self, delivery_id: str, owner: str, token: str, operation: str,
    ) -> bool:
        if operation not in {"inspect", "update", "appendix"}:
            raise ValueError("分析投递操作非法")
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE analysis_deliveries SET operation=?,heartbeat_at=? "
                "WHERE id=? AND status='sending' AND lease_owner=? AND lease_token=?",
                (operation, time.time(), delivery_id, owner, token),
            ).rowcount
        return changed == 1

    def update_analysis_delivery(
        self, delivery_id: str, *, event_seq: int | None = None,
        status: str | None = None, message_id: str | None = None,
        update_increment: int = 0, appendix_cursor: int | None = None,
        last_error: str | None = None, next_attempt_at: float | None = None,
        owner: str = "", token: str = "", release: bool = False,
        diagnostic_code: str | None = None,
    ) -> dict:
        aliases = {
            "active": "pending", "retry": "retry_wait",
            "delivered": "sent", "failed": "dead_letter",
        }
        if status is not None:
            status = aliases.get(status, status)
        if status is not None and status not in {
            "pending", "claimed", "sending", "retry_wait", "sent",
            "dead_letter", "ambiguous",
        }:
            raise ValueError("分析投递状态非法")
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            where = "id=?"
            params: tuple[Any, ...] = (delivery_id,)
            if owner or token:
                where += " AND lease_owner=? AND lease_token=? AND status IN ('claimed','sending')"
                params = (delivery_id, owner, token)
            current = conn.execute(
                f"SELECT * FROM analysis_deliveries WHERE {where}", params,
            ).fetchone()
            if current is None:
                raise KeyError(delivery_id)
            seq = int(current["event_seq"])
            if event_seq is not None:
                if int(event_seq) < seq:
                    raise ValueError("分析投递 event_seq 不能倒退")
                seq = int(event_seq)
            updates = int(current["update_count"]) + max(0, int(update_increment))
            if updates > 10:
                raise ValueError("单任务飞书卡片更新不能超过 10 次")
            resolved_status = status or str(current["status"])
            delivered_at = (
                utc_now() if resolved_status == "sent" else str(current["delivered_at"]))
            lease_values = ("", "", 0.0, 0.0, "") if release else (
                str(current["lease_owner"]), str(current["lease_token"]),
                float(current["lease_expires_at"]), float(current["heartbeat_at"]),
                str(current["operation"]),
            )
            conn.execute(
                "UPDATE analysis_deliveries SET event_seq=?,status=?,message_id=?,update_count=?,"
                "appendix_cursor=?,last_error=?,next_attempt_at=?,delivered_at=?,updated_at=?,"
                "lease_owner=?,lease_token=?,lease_expires_at=?,heartbeat_at=?,operation=?,"
                "diagnostic_code=? WHERE id=?",
                (seq, resolved_status,
                 str(current["message_id"] if message_id is None else message_id), updates,
                 int(current["appendix_cursor"] if appendix_cursor is None else appendix_cursor),
                 str(current["last_error"] if last_error is None else last_error)[:1000],
                 float(current["next_attempt_at"] if next_attempt_at is None else next_attempt_at),
                 delivered_at, utc_now(), *lease_values,
                 str(current["diagnostic_code"] if diagnostic_code is None
                     else diagnostic_code)[:80], delivery_id),
            )
            row = conn.execute(
                "SELECT * FROM analysis_deliveries WHERE id=?", (delivery_id,),
            ).fetchone()
        return dict(row)

    def fail_analysis_delivery(
        self, delivery_id: str, owner: str, token: str, error: str,
        *, ambiguous: bool = False, diagnostic_code: str = "analysis_delivery_failed",
    ) -> str:
        delays = (30, 120, 600, 1800)
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT attempts FROM analysis_deliveries WHERE id=? AND status='sending' "
                "AND lease_owner=? AND lease_token=?",
                (delivery_id, owner, token),
            ).fetchone()
            if row is None:
                return "lease_lost"
            attempts = max(1, int(row["attempts"] or 0))
            status = "ambiguous" if ambiguous else (
                "dead_letter" if attempts >= 5 else "retry_wait"
            )
            next_attempt = 0.0 if status != "retry_wait" else (
                time.time() + delays[min(attempts - 1, len(delays) - 1)]
            )
            conn.execute(
                "UPDATE analysis_deliveries SET status=?,next_attempt_at=?,last_error=?,"
                "diagnostic_code=?,ambiguous_at=?,lease_owner='',lease_token='',"
                "lease_expires_at=0,heartbeat_at=0,operation='' WHERE id=? AND status='sending' "
                "AND lease_owner=? AND lease_token=?",
                (status, next_attempt, error[:1000], diagnostic_code[:80],
                 utc_now() if ambiguous else "", delivery_id, owner, token),
            )
        return status

    def release_lease(self, name: str, owner: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "DELETE FROM runtime_leases WHERE name=? AND owner=?", (name, owner))

    def save_breadth(self, observed_at: str, advance_ratio: float, sample_size: int) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO market_breadth(observed_at,advance_ratio,sample_size) "
                "VALUES (?,?,?)", (observed_at, float(advance_ratio), int(sample_size)))

    def breadth(self, limit: int = 3000) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM market_breadth ORDER BY observed_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in reversed(rows)]

    def latest_breadth(self) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM market_breadth ORDER BY observed_at DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row is not None else None

    def cleanup(self, retention_days: int) -> None:
        cutoff = (datetime.now(UTC) - timedelta(days=retention_days)).isoformat(timespec="seconds")
        with self._conn() as conn:
            conn.execute("DELETE FROM task_runs WHERE started_at<?", (cutoff,))
            conn.execute("DELETE FROM audit_log WHERE created_at<?", (cutoff,))
            conn.execute(
                "DELETE FROM delivery_attempts WHERE created_at<? "
                "AND status IN ('sent','dead_letter','ambiguous')",
                (cutoff,),
            )
            conn.execute(
                "DELETE FROM analysis_deliveries WHERE created_at<? "
                "AND status IN ('delivered','failed')", (cutoff,),
            )
            conn.execute(
                "DELETE FROM conversation_messages WHERE created_at<?", (cutoff,)
            )
