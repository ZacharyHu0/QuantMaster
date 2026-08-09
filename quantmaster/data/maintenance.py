"""行情数据库增量同步任务：持久化进度、可取消、失败时保留旧缓存。"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Literal

from quantmaster.config import get_config
from quantmaster.data.registry import RefreshMode, load_history
from quantmaster.data.storage import BarStore
from quantmaster.runtime.jobs import WorkerIdentity, lease_deadline
from quantmaster.runtime.sqlite import connect_sqlite
from quantmaster.trading_sessions import market_date

RefreshScope = Literal["market", "universe", "all_cached"]
logger = logging.getLogger(__name__)


def market_symbols() -> list[str]:
    from quantmaster.data.akshare_source import A_SHARE_INDEXES, FUTURES_MAIN
    from quantmaster.data.yfinance_source import GLOBAL_REFS

    return list(dict.fromkeys([
        *A_SHARE_INDEXES,
        *(symbol for symbol in GLOBAL_REFS if "=" not in symbol and "-" not in symbol),
        *(symbol for symbol in FUTURES_MAIN if not symbol.startswith("IF")),
        *(symbol for symbol in GLOBAL_REFS if "=" in symbol or "-" in symbol),
    ]))


class DataRefreshManager:
    _SCHEMA = (
        "CREATE TABLE IF NOT EXISTS refresh_jobs ("
        "id TEXT PRIMARY KEY,status TEXT NOT NULL,scope TEXT NOT NULL,"
        "universe_name TEXT NOT NULL,start_date TEXT NOT NULL,end_date TEXT NOT NULL,"
        "symbols_json TEXT NOT NULL,next_index INTEGER NOT NULL DEFAULT 0,"
        "total INTEGER NOT NULL,succeeded INTEGER NOT NULL DEFAULT 0,"
        "failed INTEGER NOT NULL DEFAULT 0,failures_json TEXT NOT NULL DEFAULT '[]',"
        "current_symbol TEXT NOT NULL DEFAULT '',cancel_requested INTEGER NOT NULL DEFAULT 0,"
        "created_at REAL NOT NULL,updated_at REAL NOT NULL,"
        "owner TEXT NOT NULL DEFAULT '',lease_expires REAL NOT NULL DEFAULT 0,"
        "heartbeat_at REAL NOT NULL DEFAULT 0,attempt INTEGER NOT NULL DEFAULT 1,"
        "original_symbols_json TEXT NOT NULL DEFAULT '[]')"
    )
    _FAILURE_SCHEMA = (
        "CREATE TABLE IF NOT EXISTS refresh_failures ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL,"
        "attempt INTEGER NOT NULL DEFAULT 1,symbol TEXT NOT NULL,error TEXT NOT NULL)"
    )
    _EVENT_SCHEMA = (
        "CREATE TABLE IF NOT EXISTS refresh_events ("
        "seq INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL,"
        "attempt INTEGER NOT NULL,event_json TEXT NOT NULL,created_at REAL NOT NULL)"
    )

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._threads: dict[str, threading.Thread] = {}
        self._initialized_roots: set[str] = set()
        self.identity = WorkerIdentity.create("data-refresh")
        self._stop = threading.Event()
        self._accepting = True
        self._migrate()

    @staticmethod
    def _path() -> Path:
        return get_config().data_root / "data_refresh.sqlite"

    def _conn(self) -> sqlite3.Connection:
        path = self._path()
        conn = connect_sqlite(path)
        # data.root 可以热切换；每次连接都保证新目录具备任务表。
        conn.execute(self._SCHEMA)
        conn.execute(self._FAILURE_SCHEMA)
        conn.execute(self._EVENT_SCHEMA)
        root_key = str(path.resolve())
        with self._lock:
            first_for_root = root_key not in self._initialized_roots
            if first_for_root:
                columns = {
                    row[1] for row in conn.execute("PRAGMA table_info(refresh_jobs)").fetchall()
                }
                additions = {
                    "owner": "TEXT NOT NULL DEFAULT ''",
                    "lease_expires": "REAL NOT NULL DEFAULT 0",
                    "heartbeat_at": "REAL NOT NULL DEFAULT 0",
                    "attempt": "INTEGER NOT NULL DEFAULT 1",
                    "original_symbols_json": "TEXT NOT NULL DEFAULT '[]'",
                }
                for name, definition in additions.items():
                    if name not in columns:
                        conn.execute(
                            f"ALTER TABLE refresh_jobs ADD COLUMN {name} {definition}"
                        )
                failure_columns = {
                    row[1] for row in conn.execute(
                        "PRAGMA table_info(refresh_failures)"
                    ).fetchall()
                }
                if "attempt" not in failure_columns:
                    conn.execute(
                        "ALTER TABLE refresh_failures "
                        "ADD COLUMN attempt INTEGER NOT NULL DEFAULT 1"
                    )
                conn.execute(
                    "UPDATE refresh_jobs SET original_symbols_json=symbols_json "
                    "WHERE original_symbols_json='[]'"
                )
                conn.execute(
                    "UPDATE refresh_jobs SET status='interrupted',owner='',lease_expires=0,"
                    "current_symbol='',updated_at=? WHERE status IN ('running','cancelling') "
                    "AND lease_expires<=?",
                    (time.time(), time.time()),
                )
                conn.commit()
                self._initialized_roots.add(root_key)
        return conn

    def _migrate(self) -> None:
        with self._conn():
            pass

    @staticmethod
    def _resolve_symbols(scope: RefreshScope, universe: str, start: str, end: str) -> list[str]:
        if scope == "market":
            return market_symbols()
        if scope == "all_cached":
            return BarStore().symbols()
        if not universe:
            raise ValueError("指定候选刷新需要选择候选")
        if universe.lower() == "csi800":
            from quantmaster.lab.dataset import load_csi800_membership

            membership = load_csi800_membership(start, end)
            return sorted(symbol for symbol in membership if membership[symbol].any())
        from quantmaster.data.universe import load_universe

        return load_universe(universe)

    def _plan(
        self, scope: RefreshScope, universe: str = "", start: str = "",
    ) -> tuple[dict, list[str]]:
        end = market_date().isoformat()
        if scope == "market":
            start = str(market_date() - timedelta(days=365))
        elif not start:
            start = get_config().lab.start
        try:
            start_date = date.fromisoformat(start)
        except ValueError:
            raise ValueError("刷新起始日期必须是 YYYY-MM-DD") from None
        if start_date > market_date():
            raise ValueError("刷新起始日期不能晚于今天")
        symbols = self._resolve_symbols(scope, universe, start, end)
        from quantmaster.data.resilience import PROVIDER_HEALTH

        health = PROVIDER_HEALTH.status()
        unhealthy = [
            lane for lane, item in health.items()
            if item.get("state") != "closed" and float(item.get("open_until") or 0) > time.time()
        ]
        preview = {
            "scope": scope,
            "universe": universe,
            "start": start,
            "end": end,
            "total": len(symbols),
            "unhealthy_sources": unhealthy,
            "message": (
                f"将增量同步 {len(symbols)} 个日线标的；"
                "已缓存标的只请求尾部重叠区间，未缓存标的才按起始日期初始化"
            ),
        }
        return preview, symbols

    def preview(
        self, scope: RefreshScope, universe: str = "", start: str = "",
    ) -> dict:
        preview, _symbols = self._plan(scope, universe, start)
        return preview

    def create(
        self, scope: RefreshScope, universe: str = "", start: str = "",
    ) -> dict:
        preview, symbols = self._plan(scope, universe, start)
        with self._lock, self._conn() as conn:
            if not self._accepting:
                raise RuntimeError("行情刷新执行器正在停止，暂不接受新任务")
            conn.execute("BEGIN IMMEDIATE")
            active = conn.execute(
                "SELECT id FROM refresh_jobs "
                "WHERE status IN ('queued','running','cancelling') LIMIT 1"
            ).fetchone()
            if active:
                raise ValueError(f"已有行情刷新任务正在运行：{active[0]}")
            job_id = uuid.uuid4().hex
            now = time.time()
            conn.execute(
                "INSERT INTO refresh_jobs "
                "(id,status,scope,universe_name,start_date,end_date,symbols_json,"
                "original_symbols_json,total,created_at,updated_at) "
                "VALUES (?,'queued',?,?,?,?,?,?,?,?,?)",
                (job_id, scope, universe, preview["start"], preview["end"],
                 json.dumps(symbols, ensure_ascii=False), json.dumps(symbols, ensure_ascii=False),
                 len(symbols), now, now),
            )
            conn.execute(
                "INSERT INTO refresh_events(job_id,attempt,event_json,created_at) "
                "VALUES (?,?,?,?)",
                (job_id, 1, json.dumps({"type": "queued"}), now),
            )
        self._start(job_id)
        return self.get(job_id)

    def _start(self, job_id: str) -> None:
        with self._lock:
            current = self._threads.get(job_id)
            if current and current.is_alive():
                return
            thread = threading.Thread(
                target=self._run, args=(job_id,), name=f"data-refresh-{job_id[:8]}", daemon=True)
            self._threads[job_id] = thread
            thread.start()

    def _run(self, job_id: str) -> None:
        now = time.time()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            changed = conn.execute(
                "UPDATE refresh_jobs SET status='running',owner=?,lease_expires=?,"
                "heartbeat_at=?,updated_at=? WHERE id=? "
                "AND status IN ('queued','interrupted') AND cancel_requested=0",
                (self.identity.value, lease_deadline(), now, now, job_id),
            ).rowcount
            if not changed:
                return
            attempt = int(conn.execute(
                "SELECT attempt FROM refresh_jobs WHERE id=?", (job_id,)
            ).fetchone()[0])
            conn.execute(
                "INSERT INTO refresh_events(job_id,attempt,event_json,created_at) "
                "VALUES (?,?,?,?)",
                (job_id, attempt, json.dumps({
                    "type": "claimed", "owner": self.identity.value,
                }), now),
            )

        heartbeat_stop = threading.Event()
        lease_alive = threading.Event()
        lease_alive.set()

        def heartbeat() -> None:
            while not heartbeat_stop.wait(5.0):
                tick = time.time()
                with self._conn() as connection:
                    alive = connection.execute(
                        "UPDATE refresh_jobs SET lease_expires=?,heartbeat_at=?,updated_at=? "
                        "WHERE id=? AND owner=? AND status IN ('running','cancelling')",
                        (
                            lease_deadline(), tick, tick, job_id, self.identity.value,
                        ),
                    ).rowcount
                if not alive:
                    lease_alive.clear()
                    return

        heartbeat_thread = threading.Thread(
            target=heartbeat, name=f"data-refresh-heartbeat-{job_id[:8]}", daemon=True,
        )
        heartbeat_thread.start()
        try:
            self._execute(job_id, attempt, lease_alive)
        except Exception:
            logger.exception("行情刷新任务意外失败 job=%s", job_id)
            with self._conn() as conn:
                conn.execute(
                    "UPDATE refresh_jobs SET status='interrupted',owner='',lease_expires=0,"
                    "current_symbol='',updated_at=? WHERE id=? AND owner=?",
                    (time.time(), job_id, self.identity.value),
                )
        finally:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=1.0)

    def _execute(
        self,
        job_id: str,
        attempt: int,
        lease_alive: threading.Event,
    ) -> None:
        store = BarStore()
        while True:
            if self._stop.is_set():
                with self._conn() as conn:
                    conn.execute(
                        "UPDATE refresh_jobs SET status='interrupted',owner='',lease_expires=0,"
                        "current_symbol='',updated_at=? WHERE id=? AND owner=?",
                        (time.time(), job_id, self.identity.value),
                    )
                    conn.execute(
                        "INSERT INTO refresh_events(job_id,attempt,event_json,created_at) "
                        "VALUES (?,?,?,?)",
                        (job_id, attempt, json.dumps({
                            "type": "interrupted", "reason": "process_shutdown",
                        }), time.time()),
                    )
                return
            if not lease_alive.is_set():
                return
            with self._conn() as conn:
                row = conn.execute(
                    "SELECT scope,start_date,end_date,symbols_json,next_index,cancel_requested "
                    "FROM refresh_jobs WHERE id=? AND owner=?",
                    (job_id, self.identity.value),
                ).fetchone()
            if row is None:
                return
            scope, default_start, end, raw_symbols, index, cancelled = row
            symbols = json.loads(raw_symbols)
            if cancelled:
                with self._conn() as conn:
                    conn.execute(
                        "UPDATE refresh_jobs SET status='cancelled',current_symbol='',owner='',"
                        "lease_expires=0,updated_at=? WHERE id=? AND owner=?",
                        (time.time(), job_id, self.identity.value),
                    )
                    conn.execute(
                        "INSERT INTO refresh_events(job_id,attempt,event_json,created_at) "
                        "VALUES (?,?,?,?)",
                        (job_id, attempt, json.dumps({"type": "cancelled"}), time.time()),
                    )
                return
            if index >= len(symbols):
                with self._conn() as conn:
                    failed = conn.execute(
                        "SELECT failed FROM refresh_jobs WHERE id=?", (job_id,)).fetchone()[0]
                    conn.execute(
                        "UPDATE refresh_jobs SET status=?,current_symbol='',owner='',lease_expires=0,"
                        "updated_at=? WHERE id=? AND owner=?",
                        (
                            "completed_with_errors" if failed else "completed", time.time(),
                            job_id, self.identity.value,
                        ),
                    )
                    conn.execute(
                        "INSERT INTO refresh_events(job_id,attempt,event_json,created_at) "
                        "VALUES (?,?,?,?)",
                        (job_id, attempt, json.dumps({
                            "type": "completed_with_errors" if failed else "completed",
                            "failed": int(failed),
                        }), time.time()),
                    )
                return

            symbol = str(symbols[index])
            coverage = store.coverage(symbol)
            start = coverage[0] if scope == "all_cached" and coverage else default_start
            with self._conn() as conn:
                changed = conn.execute(
                    "UPDATE refresh_jobs SET current_symbol=?,updated_at=? "
                    "WHERE id=? AND owner=?",
                    (symbol, time.time(), job_id, self.identity.value),
                ).rowcount
            if not changed:
                return
            error = ""
            try:
                market_envelope = load_history(
                    symbol, start, end, store=store, refresh=RefreshMode.INCREMENTAL,
                    priority="maintenance",
                )
                market_envelope.require_data()
                if market_envelope.quality.status != "verified":
                    error = "；".join(market_envelope.quality.issues) or "行情证据仍为降级状态"
            except Exception as exc:
                from quantmaster.logging_config import redact_sensitive_text

                error = redact_sensitive_text(exc)[:300]
            if not lease_alive.is_set():
                return
            with self._conn() as conn:
                if error:
                    conn.execute(
                        "INSERT INTO refresh_failures (job_id,attempt,symbol,error) "
                        "VALUES (?,?,?,?)",
                        (job_id, attempt, symbol, error),
                    )
                    changed = conn.execute(
                        "UPDATE refresh_jobs SET next_index=?,failed=failed+1,"
                        "current_symbol='',updated_at=? WHERE id=? AND owner=?",
                        (index + 1, time.time(), job_id, self.identity.value),
                    ).rowcount
                else:
                    changed = conn.execute(
                        "UPDATE refresh_jobs SET next_index=?,succeeded=succeeded+1,"
                        "current_symbol='',updated_at=? WHERE id=? AND owner=?",
                        (index + 1, time.time(), job_id, self.identity.value),
                    ).rowcount
            if not changed:
                return

    def get(self, job_id: str) -> dict:
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM refresh_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        item = dict(row)
        item.pop("symbols_json", None)
        item.pop("original_symbols_json", None)
        item.pop("owner", None)
        item.pop("lease_expires", None)
        legacy_failures = json.loads(item.pop("failures_json", "[]"))
        with self._conn() as conn:
            failures = conn.execute(
                "SELECT symbol,error FROM refresh_failures WHERE job_id=? AND attempt=? "
                "ORDER BY id DESC LIMIT 200", (job_id, int(item["attempt"])),
            ).fetchall()
        item["failures"] = [
            {"symbol": row[0], "error": row[1]} for row in reversed(failures)
        ] or legacy_failures[-200:]
        item["progress"] = round(100 * int(item["next_index"]) / max(1, int(item["total"])))
        item["cancel_requested"] = bool(item["cancel_requested"])
        return item

    def latest(self) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT id FROM refresh_jobs ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        return self.get(str(row[0])) if row else None

    def list(self, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id FROM refresh_jobs ORDER BY created_at DESC LIMIT ?",
                (max(1, min(int(limit), 200)),),
            ).fetchall()
        return [self.get(str(row[0])) for row in rows]

    @property
    def active(self) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM refresh_jobs "
                "WHERE status IN ('queued','running','cancelling') LIMIT 1"
            ).fetchone()
        return row is not None

    def cancel(self, job_id: str) -> dict:
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status,attempt FROM refresh_jobs WHERE id=?", (job_id,)
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            if row[0] == "queued":
                status = "cancelled"
            elif row[0] in {"running", "cancelling"}:
                status = "cancelling"
            else:
                raise ValueError("当前任务不能取消")
            changed = conn.execute(
                "UPDATE refresh_jobs SET status=?,cancel_requested=1,updated_at=? WHERE id=?",
                (status, time.time(), job_id),
            ).rowcount
            conn.execute(
                "INSERT INTO refresh_events(job_id,attempt,event_json,created_at) "
                "VALUES (?,?,?,?)",
                (job_id, int(row[1]), json.dumps({"type": "cancel_requested"}), time.time()),
            )
        if not changed:
            raise KeyError(job_id)
        return self.get(job_id)

    def resume(self, job_id: str) -> dict:
        with self._lock, self._conn() as conn:
            if not self._accepting:
                raise RuntimeError("行情刷新执行器正在停止，暂不能续跑")
            conn.execute("BEGIN IMMEDIATE")
            active = conn.execute(
                "SELECT id FROM refresh_jobs WHERE id<>? "
                "AND status IN ('queued','running','cancelling') LIMIT 1",
                (job_id,),
            ).fetchone()
            if active:
                raise ValueError(f"已有行情刷新任务正在运行：{active[0]}")
            row = conn.execute(
                "SELECT status,failures_json,attempt FROM refresh_jobs WHERE id=?", (job_id,)
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            if row[0] not in {"completed_with_errors", "interrupted", "cancelled"}:
                raise ValueError("当前任务不能续跑")
            if row[0] == "completed_with_errors":
                retry_symbols = [item[0] for item in conn.execute(
                    "SELECT symbol FROM refresh_failures WHERE job_id=? AND attempt=? ORDER BY id",
                    (job_id, int(row[2])),
                ).fetchall()]
                if not retry_symbols:
                    retry_symbols = [item["symbol"] for item in json.loads(row[1])]
                if not retry_symbols:
                    raise ValueError("没有可重试的失败标的")
                conn.execute(
                    "UPDATE refresh_jobs SET status='queued',symbols_json=?,next_index=0,"
                    "total=?,succeeded=0,failed=0,failures_json='[]',cancel_requested=0,"
                    "current_symbol='',owner='',lease_expires=0,heartbeat_at=0,attempt=attempt+1,"
                    "updated_at=? WHERE id=?",
                    (json.dumps(retry_symbols, ensure_ascii=False), len(retry_symbols),
                     time.time(), job_id),
                )
                changed = 1
            else:
                changed = conn.execute(
                    "UPDATE refresh_jobs SET status='queued',cancel_requested=0,owner='',"
                    "lease_expires=0,heartbeat_at=0,attempt=attempt+1,updated_at=? "
                    "WHERE id=? AND status IN ('interrupted','cancelled')",
                    (time.time(), job_id),
                ).rowcount
            attempt = int(row[2]) + 1
            conn.execute(
                "INSERT INTO refresh_events(job_id,attempt,event_json,created_at) "
                "VALUES (?,?,?,?)",
                (job_id, attempt, json.dumps({
                    "type": "resumed", "previous_status": row[0],
                }), time.time()),
            )
        if not changed:
            raise ValueError("当前任务不能续跑")
        self._start(job_id)
        return self.get(job_id)

    def events(self, job_id: str, after: int = 0, limit: int = 500) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT seq,attempt,event_json,created_at FROM refresh_events "
                "WHERE job_id=? AND seq>? ORDER BY seq LIMIT ?",
                (job_id, max(0, after), max(1, min(limit, 2000))),
            ).fetchall()
        return [{
            "seq": row[0], "attempt": row[1], "created_at": row[3],
            **json.loads(row[2]),
        } for row in rows]

    def start(self) -> None:
        with self._lock:
            if any(thread.is_alive() for thread in self._threads.values()):
                self._accepting = True
                return
            self._stop.clear()
            self._accepting = True

    def shutdown(self, timeout: float = 10.0) -> None:
        with self._lock:
            self._accepting = False
            self._stop.set()
            threads = list(self._threads.values())
        per_thread = max(0.05, timeout / max(1, len(threads)))
        for thread in threads:
            thread.join(timeout=per_thread)
        with self._conn() as conn:
            conn.execute(
                "UPDATE refresh_jobs SET status='interrupted',owner='',lease_expires=0,"
                "current_symbol='',updated_at=? WHERE owner=? "
                "AND status IN ('running','cancelling')",
                (time.time(), self.identity.value),
            )


data_refresh_manager = DataRefreshManager()
