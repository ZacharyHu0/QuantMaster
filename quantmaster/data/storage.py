"""本地数据缓存：日线存 Parquet（每个 symbol 一个文件），元信息存 SQLite。

免费数据源普遍有频率限制，本地缓存能显著加速研究迭代，也让回测可复现。
"""

from __future__ import annotations

import re
import sqlite3
import time
from pathlib import Path

import pandas as pd

from quantmaster.config import get_config


def _safe_name(symbol: str) -> str:
    return re.sub(r"[^0-9A-Za-z._^-]", "_", symbol)


class BarStore:
    def __init__(self, root: Path | None = None):
        self.root = Path(root) if root else get_config().data_root / "bars"
        self.root.mkdir(parents=True, exist_ok=True)
        self.meta_db = self.root / "meta.sqlite"
        with self._conn() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS bar_meta ("
                "symbol TEXT PRIMARY KEY, start TEXT, end TEXT, updated_at REAL)"
            )

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.meta_db)

    def _path(self, symbol: str) -> Path:
        return self.root / f"{_safe_name(symbol)}.parquet"

    def get(self, symbol: str) -> pd.DataFrame | None:
        path = self._path(symbol)
        if not path.exists():
            return None
        try:
            return pd.read_parquet(path)
        except Exception:
            return None

    def put(self, symbol: str, df: pd.DataFrame, replace: bool = False) -> None:
        """写入缓存。replace=True 整体替换（前复权数据必须整段替换，
        增量合并会混合不同复权基准，见 registry.load_history 的说明）。"""
        if df is None or df.empty:
            return
        if not replace:
            old = self.get(symbol)
            if old is not None and not old.empty:
                df = pd.concat([old, df])
                df = df[~df.index.duplicated(keep="last")].sort_index()
        df = df.sort_index()
        df.to_parquet(self._path(symbol))
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO bar_meta VALUES (?,?,?,?)",
                (symbol, str(df.index.min().date()), str(df.index.max().date()), time.time()),
            )

    def freshness(self, symbol: str) -> float | None:
        """距上次更新的秒数；无记录返回 None。"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT updated_at FROM bar_meta WHERE symbol=?", (symbol,)
            ).fetchone()
        return (time.time() - row[0]) if row else None

    def coverage(self, symbol: str) -> tuple[str, str] | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT start, end FROM bar_meta WHERE symbol=?", (symbol,)
            ).fetchone()
        return (row[0], row[1]) if row else None

    def symbols(self) -> list[str]:
        with self._conn() as conn:
            rows = conn.execute("SELECT symbol FROM bar_meta ORDER BY symbol").fetchall()
        return [r[0] for r in rows]
