"""行情数据库全量刷新任务：持久化进度、可取消、失败时保留旧缓存。"""

from __future__ import annotations

import json
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

RefreshScope = Literal["market", "universe", "all_cached"]


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
        "created_at REAL NOT NULL,updated_at REAL NOT NULL)"
    )
    _FAILURE_SCHEMA = (
        "CREATE TABLE IF NOT EXISTS refresh_failures ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL,"
        "symbol TEXT NOT NULL,error TEXT NOT NULL)"
    )

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._threads: dict[str, threading.Thread] = {}
        self._initialized_roots: set[str] = set()
        self._migrate()

    @staticmethod
    def _path() -> Path:
        return get_config().data_root / "data_refresh.sqlite"

    def _conn(self) -> sqlite3.Connection:
        path = self._path()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        # data.root 可以热切换；每次连接都保证新目录具备任务表。
        conn.execute(self._SCHEMA)
        conn.execute(self._FAILURE_SCHEMA)
        root_key = str(path.resolve())
        with self._lock:
            first_for_root = root_key not in self._initialized_roots
            self._initialized_roots.add(root_key)
        if first_for_root:
            # 只有首次接管某个数据根目录时恢复崩溃任务，不能在任务运行期间
            # 因普通数据库连接把它误标为中断。
            conn.execute(
                "UPDATE refresh_jobs SET status='interrupted',current_symbol='' "
                "WHERE status IN ('running','cancelling')"
            )
            conn.commit()
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
        end = str(date.today())
        if scope == "market":
            start = str(date.today() - timedelta(days=365))
        elif not start:
            start = get_config().lab.start
        try:
            start_date = date.fromisoformat(start)
        except ValueError:
            raise ValueError("刷新起始日期必须是 YYYY-MM-DD") from None
        if start_date > date.today():
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
            "message": f"将全量刷新 {len(symbols)} 个日线标的；原缓存仅在单标的验证成功后替换",
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
            conn.execute("BEGIN IMMEDIATE")
            active = conn.execute(
                "SELECT id FROM refresh_jobs WHERE status IN ('running','cancelling') LIMIT 1"
            ).fetchone()
            if active:
                raise ValueError(f"已有行情刷新任务正在运行：{active[0]}")
            job_id = uuid.uuid4().hex
            now = time.time()
            conn.execute(
                "INSERT INTO refresh_jobs "
                "(id,status,scope,universe_name,start_date,end_date,symbols_json,total,"
                "created_at,updated_at) VALUES (?,'running',?,?,?,?,?,?,?,?)",
                (job_id, scope, universe, preview["start"], preview["end"],
                 json.dumps(symbols, ensure_ascii=False), len(symbols), now, now),
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
        store = BarStore()
        while True:
            with self._conn() as conn:
                row = conn.execute(
                    "SELECT scope,start_date,end_date,symbols_json,next_index,cancel_requested "
                    "FROM refresh_jobs WHERE id=?", (job_id,)
                ).fetchone()
            if row is None:
                return
            scope, default_start, end, raw_symbols, index, cancelled = row
            symbols = json.loads(raw_symbols)
            if cancelled:
                with self._conn() as conn:
                    conn.execute(
                        "UPDATE refresh_jobs SET status='cancelled',current_symbol='',updated_at=? "
                        "WHERE id=?", (time.time(), job_id))
                return
            if index >= len(symbols):
                with self._conn() as conn:
                    failed = conn.execute(
                        "SELECT failed FROM refresh_jobs WHERE id=?", (job_id,)).fetchone()[0]
                    conn.execute(
                        "UPDATE refresh_jobs SET status=?,current_symbol='',updated_at=? WHERE id=?",
                        ("completed_with_errors" if failed else "completed", time.time(), job_id),
                    )
                return

            symbol = str(symbols[index])
            coverage = store.coverage(symbol)
            start = coverage[0] if scope == "all_cached" and coverage else default_start
            with self._conn() as conn:
                conn.execute(
                    "UPDATE refresh_jobs SET current_symbol=?,updated_at=? WHERE id=?",
                    (symbol, time.time(), job_id),
                )
            error = ""
            try:
                load_history(
                    symbol, start, end, store=store, refresh=RefreshMode.FULL,
                    priority="maintenance",
                )
                meta = store.metadata(symbol) or {}
                if meta.get("last_status") == "refresh_failed":
                    error = "所有数据源失败，已保留原缓存"
            except Exception as exc:
                from quantmaster.logging_config import redact_sensitive_text

                error = redact_sensitive_text(exc)[:300]
            with self._conn() as conn:
                if error:
                    conn.execute(
                        "INSERT INTO refresh_failures (job_id,symbol,error) VALUES (?,?,?)",
                        (job_id, symbol, error),
                    )
                    conn.execute(
                        "UPDATE refresh_jobs SET next_index=?,failed=failed+1,"
                        "current_symbol='',updated_at=? WHERE id=?",
                        (index + 1, time.time(), job_id),
                    )
                else:
                    conn.execute(
                        "UPDATE refresh_jobs SET next_index=?,succeeded=succeeded+1,"
                        "current_symbol='',updated_at=? WHERE id=?",
                        (index + 1, time.time(), job_id),
                    )

    def get(self, job_id: str) -> dict:
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM refresh_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        item = dict(row)
        item.pop("symbols_json", None)
        legacy_failures = json.loads(item.pop("failures_json", "[]"))
        with self._conn() as conn:
            failures = conn.execute(
                "SELECT symbol,error FROM refresh_failures WHERE job_id=? "
                "ORDER BY id DESC LIMIT 200", (job_id,)
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

    @property
    def active(self) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM refresh_jobs WHERE status IN ('running','cancelling') LIMIT 1"
            ).fetchone()
        return row is not None

    def cancel(self, job_id: str) -> dict:
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE refresh_jobs SET status='cancelling',cancel_requested=1,updated_at=? "
                "WHERE id=? AND status='running'", (time.time(), job_id)).rowcount
        if not changed:
            item = self.get(job_id)
            if item["status"] not in {"cancelling", "cancelled"}:
                raise ValueError("当前任务不能取消")
        return self.get(job_id)

    def resume(self, job_id: str) -> dict:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT status,failures_json FROM refresh_jobs WHERE id=?", (job_id,)
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            if row[0] == "completed_with_errors":
                retry_symbols = [item[0] for item in conn.execute(
                    "SELECT symbol FROM refresh_failures WHERE job_id=? ORDER BY id", (job_id,)
                ).fetchall()]
                if not retry_symbols:
                    retry_symbols = [item["symbol"] for item in json.loads(row[1])]
                if not retry_symbols:
                    raise ValueError("没有可重试的失败标的")
                conn.execute("DELETE FROM refresh_failures WHERE job_id=?", (job_id,))
                conn.execute(
                    "UPDATE refresh_jobs SET status='running',symbols_json=?,next_index=0,"
                    "total=?,succeeded=0,failed=0,failures_json='[]',cancel_requested=0,"
                    "current_symbol='',updated_at=? WHERE id=?",
                    (json.dumps(retry_symbols, ensure_ascii=False), len(retry_symbols),
                     time.time(), job_id),
                )
                changed = 1
            else:
                changed = conn.execute(
                    "UPDATE refresh_jobs SET status='running',cancel_requested=0,updated_at=? "
                    "WHERE id=? AND status IN ('interrupted','cancelled')",
                    (time.time(), job_id),
                ).rowcount
        if not changed:
            raise ValueError("当前任务不能续跑")
        self._start(job_id)
        return self.get(job_id)


data_refresh_manager = DataRefreshManager()
