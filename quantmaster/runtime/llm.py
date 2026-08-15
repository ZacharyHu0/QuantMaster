"""Durable LLM execution revisions and HTTP request-plane safety guards.

Only opaque revision identifiers live in the durable metadata.  Credentials,
prompts and provider responses remain in their existing stores and artifacts.
"""

from __future__ import annotations

import contextvars
import logging
import sqlite3
import threading
import time
import uuid
import weakref
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite

logger = logging.getLogger(__name__)

_http_request_active: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "quantmaster_http_request_active", default=False,
)
_execution_lease: contextvars.ContextVar[tuple[str, str, str, Any] | None] = contextvars.ContextVar(
    "quantmaster_llm_execution_lease", default=None,
)


class DirectLLMRequestError(RuntimeError):
    """A provider transport was reached from a FastAPI request handler."""


def enter_http_request() -> contextvars.Token[bool]:
    return _http_request_active.set(True)


def leave_http_request(token: contextvars.Token[bool]) -> None:
    _http_request_active.reset(token)


def reject_http_llm_transport() -> None:
    """Make a future synchronous route regression an explicit error."""
    if _http_request_active.get():
        raise DirectLLMRequestError("LLM transport is forbidden during an HTTP request")


def require_execution_lease() -> None:
    """Strict helper for worker-only code paths and focused regression tests."""
    reject_http_llm_transport()
    if _execution_lease.get() is None:
        raise DirectLLMRequestError("LLM transport requires a durable execution lease")
    if not execution_lease_current():
        raise InterruptedError("LLM execution lease expired after configuration rotation")
    if execution_lease_cancelled():
        raise InterruptedError("LLM execution lease was cancelled")


def execution_lease_current() -> bool:
    value = _execution_lease.get()
    if value is None:
        return False
    _job_id, scope, revision, _context = value
    return get_llm_execution_coordinator().current(scope, revision)


def execution_lease_stale() -> bool:
    """An absent lease is fine for CLI callers; an expired one is never fine."""
    return _execution_lease.get() is not None and not execution_lease_current()


def execution_lease_cancelled() -> bool:
    """Return whether the active worker lease must stop before dispatch.

    A revision change and an explicit job cancellation share the same gate
    predicate.  The latter matters when a task is already waiting behind the
    FIFO provider limit: it must be removed without issuing its HTTP request.
    No lease means a CLI/local caller remains permitted to use ``LLMClient``.
    """
    value = _execution_lease.get()
    if value is None:
        return False
    if not execution_lease_current():
        return True
    _job_id, _scope, _revision, context = value
    try:
        cancelled = getattr(context, "cancelled", None)
        return bool(cancelled()) if callable(cancelled) else False
    except (KeyError, OSError, RuntimeError, ValueError, sqlite3.Error):
        # A lost/invalid ledger lease is never permission to make a provider
        # request.  Treat it as cancelled and let the task converge safely.
        return True


class LLMExecutionCoordinator:
    """Persistent global/news revision registry with stale-task cancellation."""

    def __init__(self, path: Path | None = None) -> None:
        root = get_config().data_root
        self.path = Path(path) if path else root / "_runtime" / "llm_revisions.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._stores: weakref.WeakSet[Any] = weakref.WeakSet()
        self._lock = threading.RLock()
        with self._conn() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS llm_revisions ("
                "scope TEXT PRIMARY KEY, revision TEXT NOT NULL, updated_at REAL NOT NULL)"
            )
            for scope in ("global", "news"):
                connection.execute(
                    "INSERT OR IGNORE INTO llm_revisions(scope,revision,updated_at) VALUES (?,?,?)",
                    (scope, uuid.uuid4().hex, time.time()),
                )

    def _conn(self):
        return connect_sqlite(self.path, timeout=5.0, row_factory=True)

    def register_store(self, store: Any) -> None:
        self._stores.add(store)

    def revision(self, scope: str = "global") -> str:
        normalized = "news" if scope == "news" else "global"
        with self._conn() as connection:
            row = connection.execute(
                "SELECT revision FROM llm_revisions WHERE scope=?", (normalized,),
            ).fetchone()
        if row is not None:
            return str(row["revision"])
        # A manually repaired database is treated as an explicit new revision,
        # never as an invitation to replay unknown provider work.
        value = uuid.uuid4().hex
        with self._conn() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO llm_revisions(scope,revision,updated_at) VALUES (?,?,?)",
                (normalized, value, time.time()),
            )
        return value

    def _job_stores(self) -> list[Any]:
        """Return physical ledgers once, including a web process with no runtime."""
        stores = {str(store.path.resolve()): store for store in list(self._stores)}
        # The web process intentionally does not start a runtime, but must
        # still durably fence jobs as soon as settings are saved.
        default_path = get_config().data_root / "jobs.sqlite"
        if str(default_path.resolve()) not in stores and _job_store_factory is not None:
            store = _job_store_factory(default_path)
            stores[str(default_path.resolve())] = store
        return list(stores.values())

    def rotate(
        self,
        *,
        global_scope: bool = True,
        news_scope: bool = False,
        reason: str = "configuration_changed",
    ) -> dict[str, Any]:
        scopes = ([] if not global_scope else ["global"]) + (["news"] if news_scope else [])
        revisions: dict[str, str] = {}
        with self._lock, self._conn() as connection:
            for scope in scopes:
                value = uuid.uuid4().hex
                connection.execute(
                    "UPDATE llm_revisions SET revision=?,updated_at=? WHERE scope=?",
                    (value, time.time(), scope),
                )
                revisions[scope] = value
        cancellation = {"queued_cancelled": 0, "running_cancelling": 0}
        for store in self._job_stores():
            for scope in scopes:
                result = store.cancel_stale_llm(scope, revisions[scope], reason)
                for key in cancellation:
                    cancellation[key] += int(result[key])
        return {"revisions": revisions, **cancellation, "reason": reason}

    def current(self, scope: str, revision: str) -> bool:
        return bool(revision) and self.revision(scope) == revision

    @contextmanager
    def lease(self, context: Any, scope: str, revision: str) -> Iterator[None]:
        if not self.current(scope, revision):
            raise InterruptedError("LLM configuration revision is no longer current")
        token = _execution_lease.set((str(context.job_id), scope, revision, context))
        try:
            yield
        finally:
            _execution_lease.reset(token)

    def diagnostics(self) -> dict[str, Any]:
        counts = {
            "global": {"queued": 0, "running": 0, "cancelling": 0, "cancelled": 0},
            "news": {"queued": 0, "running": 0, "cancelling": 0, "cancelled": 0},
        }
        stores = {str(store.path.resolve()): store for store in self._job_stores()}
        for store in stores.values():
            for job in store.list(10_000):
                scope = str(job.get("llm_scope") or "")
                if scope not in counts:
                    continue
                status = str(job.get("status") or "")
                if status in counts[scope]:
                    counts[scope][status] += 1
        config = get_config()
        limits = {
            "global": {"max_concurrency": max(1, int(config.llm.max_concurrency)),
                       "queue_timeout_seconds": float(config.llm.queue_timeout)},
            "news": {"max_concurrency": max(1, int(config.news.annotation_max_concurrency)),
                     "queue_timeout_seconds": float(config.llm.queue_timeout)},
        }
        return {
            "revisions": {scope: self.revision(scope) for scope in counts},
            "registered_job_stores": len(stores),
            "jobs": {key: sum(values[key] for values in counts.values()) for key in counts["global"]},
            "limits": limits,
            "scopes": {
                scope: {"revision": self.revision(scope), "jobs": counts[scope],
                        "limits": limits[scope]}
                for scope in counts
            },
        }


_instance: LLMExecutionCoordinator | None = None
_instance_root = ""
_instance_lock = threading.Lock()
_job_store_factory: Callable[[Path], Any] | None = None


def register_job_store_factory(factory: Callable[[Path], Any]) -> None:
    global _job_store_factory
    _job_store_factory = factory


def get_llm_execution_coordinator() -> LLMExecutionCoordinator:
    global _instance, _instance_root
    root = str(get_config().data_root.resolve())
    with _instance_lock:
        if _instance is None or _instance_root != root:
            _instance = LLMExecutionCoordinator()
            _instance_root = root
        return _instance
