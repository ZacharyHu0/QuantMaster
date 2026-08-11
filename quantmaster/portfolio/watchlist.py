"""自选与关注列表：本地 SQLite 持久化，不触发任何行情网络请求。"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite

LIST_NAMES = {"favorites", "following"}


def normalize_symbol(symbol: str) -> str:
    """统一常见 A 股代码；其他市场代码仅做去空格和大写。"""
    value = str(symbol).strip().upper()
    if not value:
        raise ValueError("代码不能为空")
    if re.fullmatch(r"\d{6}", value):
        suffix = "SH" if value.startswith(("6", "9")) else (
            "BJ" if value.startswith(("4", "8")) else "SZ")
        value = f"{value}.{suffix}"
    if len(value) > 40:
        raise ValueError("代码过长")
    return value


class AssetListStore:
    """管理互相独立的自选（favorites）与关注（following）列表。"""

    def __init__(self, path: Path | None = None, *, read_only: bool = False):
        self.path = path or get_config().data_root / "asset_lists.sqlite"
        self.read_only = bool(read_only)
        if not self.read_only:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._conn() as conn:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS asset_lists ("
                    "list_name TEXT NOT NULL, symbol TEXT NOT NULL, name TEXT NOT NULL DEFAULT '',"
                    "added_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                    "PRIMARY KEY (list_name, symbol))"
                )

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(
            self.path,
            timeout=0.25 if self.read_only else 30.0,
            row_factory=True,
            read_only=self.read_only,
        )

    def _require_writable(self) -> None:
        if self.read_only:
            raise RuntimeError("只读自选列表不能修改")

    @staticmethod
    def _validate_list(list_name: str) -> str:
        if list_name not in LIST_NAMES:
            raise ValueError("列表必须是 favorites 或 following")
        return list_name

    def add(self, list_name: str, symbol: str, name: str = "") -> dict:
        self._require_writable()
        list_name = self._validate_list(list_name)
        symbol = normalize_symbol(symbol)
        clean_name = str(name).strip()[:80]
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO asset_lists (list_name,symbol,name) VALUES (?,?,?) "
                "ON CONFLICT(list_name,symbol) DO UPDATE SET "
                "name=CASE WHEN excluded.name='' THEN asset_lists.name ELSE excluded.name END",
                (list_name, symbol, clean_name),
            )
        return next(item for item in self.list(list_name) if item["symbol"] == symbol)

    def remove(self, list_name: str, symbol: str) -> bool:
        self._require_writable()
        list_name = self._validate_list(list_name)
        symbol = normalize_symbol(symbol)
        with self._conn() as conn:
            cursor = conn.execute(
                "DELETE FROM asset_lists WHERE list_name=? AND symbol=?",
                (list_name, symbol),
            )
        return cursor.rowcount > 0

    def list(self, list_name: str) -> list[dict]:
        list_name = self._validate_list(list_name)
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    "SELECT symbol,name,added_at FROM asset_lists "
                    "WHERE list_name=? ORDER BY added_at DESC, symbol",
                    (list_name,),
                ).fetchall()
        except FileNotFoundError:
            if self.read_only:
                return []
            raise
        except sqlite3.OperationalError as exc:
            if self.read_only and "no such table" in str(exc).lower():
                return []
            raise
        return [dict(row) for row in rows]

    def all(self) -> dict[str, list[dict]]:
        return {name: self.list(name) for name in ("favorites", "following")}
