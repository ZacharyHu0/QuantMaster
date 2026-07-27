"""外部数据接口的重试、限速与原始响应缓存。

行情研究会反复请求相同区间。这里把网络层的防护放在数据源内部：

- AKShare 失败时指数退避重试，再由 registry 降级到其他数据源；
- Tushare 使用 SQLite 跨进程匀速限流，避免瞬时突发触发 2000 积分档流控；
- Tushare 原始 DataFrame 按 ``endpoint + params`` 缓存为 Parquet，相同请求
  即使跨进程、重启服务也不再次消耗接口次数。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import tempfile
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

import pandas as pd

from quantmaster.config import get_config

logger = logging.getLogger(__name__)
T = TypeVar("T")


def akshare_call(label: str, func: Callable[..., T], *args, **kwargs) -> T:
    """执行 AKShare 请求，失败时按配置做指数退避重试。"""
    cfg = get_config().data
    attempts = max(1, int(cfg.akshare_retries))
    backoff = max(0.0, float(cfg.akshare_retry_backoff))
    for attempt in range(1, attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception:
            if attempt >= attempts:
                raise
            delay = backoff * (2 ** (attempt - 1))
            logger.warning(
                "AKShare %s 失败（%s/%s），%.2f 秒后重试",
                label, attempt, attempts, delay, exc_info=True,
            )
            if delay:
                time.sleep(delay)
    raise AssertionError("unreachable")


class TushareRateLimiter:
    """按稳定间隔放行请求；SQLite 让同一数据目录下的进程共享限流。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_call = 0.0

    def wait(self) -> None:
        calls = max(1, int(get_config().data.tushare_calls_per_minute))
        interval = 60.0 / calls
        with self._lock:
            path = get_config().data_root / "tushare_rate.sqlite"
            with sqlite3.connect(path, timeout=30.0) as conn:
                conn.execute("PRAGMA busy_timeout=30000")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS rate_state ("
                    "name TEXT PRIMARY KEY, next_call REAL NOT NULL)"
                )
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT next_call FROM rate_state WHERE name='global'").fetchone()
                now = time.time()
                next_call = float(row[0]) if row else self._next_call
                reserved = max(now, next_call)
                conn.execute(
                    "INSERT OR REPLACE INTO rate_state VALUES ('global', ?)",
                    (reserved + interval,),
                )
                conn.commit()
            self._next_call = reserved + interval
            delay = reserved - time.time()
        if delay > 0:
            time.sleep(delay)


TUSHARE_LIMITER = TushareRateLimiter()
TUSHARE_REQUEST_LOCK = threading.Lock()


class EndpointFrameCache:
    """磁盘持久化的 DataFrame 接口缓存，以文件 mtime 判断新鲜度。"""

    def __init__(self, provider: str = "tushare", root: Path | None = None):
        base = Path(root) if root else get_config().data_root / "api_cache"
        self.root = base / provider
        self.root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _digest(endpoint: str, params: dict) -> str:
        payload = json.dumps(
            {"endpoint": endpoint, "params": params},
            sort_keys=True, ensure_ascii=False, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()[:24]

    def path_for(self, endpoint: str, params: dict) -> Path:
        safe_endpoint = "".join(c if c.isalnum() or c in "-_" else "_" for c in endpoint)
        return self.root / f"{safe_endpoint}-{self._digest(endpoint, params)}.parquet"

    def get(self, endpoint: str, params: dict, ttl_days: float) -> pd.DataFrame | None:
        path = self.path_for(endpoint, params)
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > max(0.0, ttl_days) * 86400:
            return None
        try:
            return pd.read_parquet(path)
        except Exception:
            logger.warning("接口缓存损坏，将重新拉取: %s", path)
            return None

    def put(self, endpoint: str, params: dict, frame: pd.DataFrame) -> None:
        if frame is None:
            return
        target = self.path_for(endpoint, params)
        fd, temp_name = tempfile.mkstemp(
            prefix=f".{target.stem}.", suffix=".parquet.tmp", dir=self.root)
        os.close(fd)
        temp_path = Path(temp_name)
        try:
            frame.to_parquet(temp_path, index=False)
            os.replace(temp_path, target)
        finally:
            temp_path.unlink(missing_ok=True)
