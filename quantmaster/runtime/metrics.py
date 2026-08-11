"""Bounded local performance metrics for request and refresh-node attribution."""

from __future__ import annotations

import os
import queue
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite


class RuntimeMetrics:
    """A lightweight 7-day / 10,000-row observability store.

    Metrics must never become an availability dependency: write failures are
    intentionally ignored by callers, and payloads contain only route/node
    dimensions rather than user data or request bodies.
    """

    MAX_ROWS = 10_000
    RETENTION_SECONDS = 7 * 24 * 3600

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path else get_config().data_root / "runtime_metrics.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._writes = 0
        with self._conn() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS request_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recorded_at REAL NOT NULL,
                    route TEXT NOT NULL,
                    method TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    duration_ms REAL NOT NULL,
                    response_bytes INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_request_metrics_route_time
                    ON request_metrics(route, recorded_at);
                CREATE TABLE IF NOT EXISTS refresh_node_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recorded_at REAL NOT NULL,
                    job_id TEXT NOT NULL DEFAULT '',
                    node TEXT NOT NULL,
                    input_fingerprint TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    wall_ms REAL NOT NULL,
                    cpu_ms REAL NOT NULL,
                    cache_hit INTEGER NOT NULL DEFAULT 0,
                    input_rows INTEGER NOT NULL DEFAULT 0,
                    output_rows INTEGER NOT NULL DEFAULT 0,
                    files_read INTEGER NOT NULL DEFAULT 0,
                    remote_calls INTEGER NOT NULL DEFAULT 0,
                    lock_wait_ms REAL NOT NULL DEFAULT 0,
                    artifact_bytes INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_refresh_node_metrics_time
                    ON refresh_node_metrics(node, recorded_at);
                """
            )

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(self.path, policy="cache", timeout=0.2, row_factory=True)

    def _prune(self, connection: sqlite3.Connection) -> None:
        cutoff = time.time() - self.RETENTION_SECONDS
        connection.execute("DELETE FROM request_metrics WHERE recorded_at<?", (cutoff,))
        connection.execute("DELETE FROM refresh_node_metrics WHERE recorded_at<?", (cutoff,))
        for table in ("request_metrics", "refresh_node_metrics"):
            connection.execute(
                f"DELETE FROM {table} WHERE id NOT IN ("
                f"SELECT id FROM {table} ORDER BY id DESC LIMIT ?)",
                (self.MAX_ROWS,),
            )

    def _occasionally_prune(self, connection: sqlite3.Connection) -> None:
        with self._lock:
            self._writes += 1
            should_prune = self._writes % 128 == 0
        if should_prune:
            self._prune(connection)

    def record_request(
        self,
        *,
        route: str,
        method: str,
        status_code: int,
        duration_ms: float,
        response_bytes: int = 0,
    ) -> None:
        try:
            with self._conn() as connection:
                connection.execute(
                    "INSERT INTO request_metrics(recorded_at,route,method,status_code,duration_ms,"
                    "response_bytes) VALUES(?,?,?,?,?,?)",
                    (
                        time.time(), str(route)[:300], str(method)[:12], int(status_code),
                        max(0.0, float(duration_ms)), max(0, int(response_bytes)),
                    ),
                )
                self._occasionally_prune(connection)
        except (OSError, sqlite3.Error):
            return

    def record_node(
        self,
        node: str,
        *,
        job_id: str = "",
        input_fingerprint: str = "",
        status: str = "completed",
        wall_ms: float = 0.0,
        cpu_ms: float = 0.0,
        cache_hit: bool = False,
        input_rows: int = 0,
        output_rows: int = 0,
        files_read: int = 0,
        remote_calls: int = 0,
        lock_wait_ms: float = 0.0,
        artifact_bytes: int = 0,
    ) -> None:
        try:
            with self._conn() as connection:
                connection.execute(
                    "INSERT INTO refresh_node_metrics(recorded_at,job_id,node,input_fingerprint,status,"
                    "wall_ms,cpu_ms,cache_hit,input_rows,output_rows,files_read,remote_calls,"
                    "lock_wait_ms,artifact_bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        time.time(), str(job_id)[:100], str(node)[:200],
                        str(input_fingerprint)[:128], str(status)[:40],
                        max(0.0, float(wall_ms)), max(0.0, float(cpu_ms)), int(bool(cache_hit)),
                        max(0, int(input_rows)), max(0, int(output_rows)), max(0, int(files_read)),
                        max(0, int(remote_calls)), max(0.0, float(lock_wait_ms)),
                        max(0, int(artifact_bytes)),
                    ),
                )
                self._occasionally_prune(connection)
        except (OSError, sqlite3.Error):
            return

    @contextmanager
    def node_timer(
        self,
        node: str,
        *,
        job_id: str = "",
        input_fingerprint: str = "",
        **dimensions: Any,
    ) -> Iterator[dict[str, Any]]:
        """Measure one DAG node and persist a completed/failed attribution row."""

        started_wall = time.perf_counter()
        started_cpu = time.process_time()
        values = dict(dimensions)
        try:
            yield values
        except BaseException:
            self.record_node(
                node, job_id=job_id, input_fingerprint=input_fingerprint,
                status="failed", wall_ms=(time.perf_counter() - started_wall) * 1000,
                cpu_ms=(time.process_time() - started_cpu) * 1000, **values,
            )
            raise
        else:
            self.record_node(
                node, job_id=job_id, input_fingerprint=input_fingerprint,
                status="completed", wall_ms=(time.perf_counter() - started_wall) * 1000,
                cpu_ms=(time.process_time() - started_cpu) * 1000, **values,
            )


_METRICS: RuntimeMetrics | None = None
_METRICS_LOCK = threading.Lock()


def get_runtime_metrics() -> RuntimeMetrics:
    global _METRICS
    path = get_config().data_root / "runtime_metrics.sqlite"
    with _METRICS_LOCK:
        if _METRICS is None or _METRICS.path != path:
            _METRICS = RuntimeMetrics(path)
        return _METRICS


class RuntimeMetricsRecorder:
    """Bounded asynchronous ingress for Web-request metrics.

    A page request never opens the metrics SQLite database.  If the queue is
    full during a fault, losing an observability sample is preferable to
    delaying the page that reports the fault.
    """

    MAX_PENDING = 2_048

    def __init__(self) -> None:
        self._queue: queue.Queue[dict[str, Any] | None] = queue.Queue(self.MAX_PENDING)
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def _ensure_worker(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run,
                name="runtime-metrics-writer",
                daemon=True,
            )
            self._thread.start()

    def record_request(self, **values: Any) -> None:
        # A reloadable Web generation must not create a metrics writer thread
        # or open SQLite after serving a request.  The supervisor-owned
        # runtime-worker records its own node metrics; when that worker is not
        # available it is explicitly safe to lose a request sample rather
        # than let observability recreate the page-freeze failure mode.
        if os.environ.get("QM_WEB_PROCESS") == "1":
            return
        try:
            self._queue.put_nowait({"kind": "request", **values})
        except queue.Full:
            return
        self._ensure_worker()

    def record_node(self, node: str, **values: Any) -> None:
        if os.environ.get("QM_WEB_PROCESS") == "1":
            return
        try:
            self._queue.put_nowait({"kind": "node", "node": node, **values})
        except queue.Full:
            return
        self._ensure_worker()

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                return
            try:
                metrics = get_runtime_metrics()
                kind = item.pop("kind", "")
                if kind == "request":
                    metrics.record_request(**item)
                elif kind == "node":
                    node = str(item.pop("node"))
                    metrics.record_node(node, **item)
            except (OSError, RuntimeError, ValueError, sqlite3.Error):
                # Same availability rule as RuntimeMetrics: observation cannot
                # be allowed to recursively create an availability incident.
                continue

    def shutdown(self) -> None:
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            return


_RECORDER: RuntimeMetricsRecorder | None = None
_RECORDER_LOCK = threading.Lock()


def get_runtime_metrics_recorder() -> RuntimeMetricsRecorder:
    global _RECORDER
    with _RECORDER_LOCK:
        if _RECORDER is None:
            _RECORDER = RuntimeMetricsRecorder()
        return _RECORDER
