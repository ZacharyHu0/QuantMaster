"""模拟盘自动运行 worker 与租约心跳。"""

from __future__ import annotations

import logging
import sqlite3
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime

from quantmaster.backtest.paper_accounts import (
    PaperService,
    PaperStore,
    get_paper_service,
)
from quantmaster.config import get_config
from quantmaster.runtime.jobs import WorkerIdentity
from quantmaster.trading_sessions import SessionExpectation, expected_session

logger = logging.getLogger(__name__)


@contextmanager
def _paper_auto_heartbeat(
    store: PaperStore,
    run_date: str,
    account_id: str,
    owner: str,
    token: str,
) -> Iterator[threading.Event]:
    stop = threading.Event()
    alive = threading.Event()
    alive.set()

    def renew() -> None:
        while not stop.wait(30.0):
            if not store.heartbeat_auto_run(run_date, account_id, owner, token):
                alive.clear()
                return

    thread = threading.Thread(target=renew, name="qm-paper-lease-heartbeat", daemon=True)
    thread.start()
    try:
        yield alive
    finally:
        stop.set()
        thread.join(timeout=1.0)


class PaperAutomationWorker:
    """Persistent daily runner for accounts that explicitly enable auto trading."""

    def __init__(
        self,
        service: PaperService | None = None,
        poll_seconds: float = 45.0,
        session_resolver: Callable[[datetime | None], SessionExpectation] = expected_session,
    ):
        self.service = service or PaperService()
        self.identity = WorkerIdentity.create("paper-auto")
        self.poll_seconds = max(1.0, float(poll_seconds))
        self.session_resolver = session_resolver
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._running = threading.Event()

    @property
    def idle(self) -> bool:
        return not self._running.is_set()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._wake.set()
        self._thread = threading.Thread(
            target=self._run,
            name="quantmaster-paper-auto",
            daemon=True,
        )
        self._thread.start()

    def wake(self) -> None:
        self._wake.set()

    def requeue_market_data(self, run_date: str) -> int:
        """Re-arm only accounts whose last attempt lacked market evidence."""
        changed = self.service.store.requeue_market_data_failures(str(run_date))
        if changed:
            self.wake()
        return changed

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=max(0.1, timeout))

    def run_due_once(self, now: datetime | None = None) -> dict:
        expectation = self.session_resolver(now)
        if not expectation.ready:
            return {
                "status": "calendar_unavailable",
                "accounts": [],
                "calendar": expectation.as_dict(),
                "reason": expectation.reason,
            }
        run_date = expectation.session
        accounts = [
            account
            for account in self.service.store.accounts()
            if account["status"] == "active" and account["mode"] == "auto"
        ]
        if not accounts:
            return {"status": "idle", "accounts": []}
        results = []
        for account in accounts:
            account_id = str(account["id"])
            token = self.service.store.claim_auto_run(
                run_date,
                account_id,
                self.identity.value,
            )
            if not token:
                continue
            self._running.set()
            try:
                with _paper_auto_heartbeat(
                    self.service.store,
                    run_date,
                    account_id,
                    self.identity.value,
                    token,
                ) as lease_alive:
                    result = self.service.run_auto_account(
                        account_id,
                        expected_signal_date=run_date,
                    )
                    completed = lease_alive.is_set() and self.service.store.complete_auto_run(
                        run_date,
                        account_id,
                        self.identity.value,
                        token,
                        result,
                    )
                if completed:
                    self.service.store.clear_runtime_warning(account_id)
                    results.append(result)
                else:
                    results.append(
                        {
                            "status": "lease_lost",
                            "account_id": account_id,
                            "error": "自动运行租约已由其他 worker 接管",
                        }
                    )
            except (KeyError, OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
                message = str(exc)[:500]
                failure_code = (
                    "market_data_unavailable"
                    if "行情证据不可用" in message
                    or "收盘行情" in message
                    or "待撮合行情证据" in message
                    else "execution_error"
                )
                self.service.store.set_warning(account_id, message)
                self.service.store.fail_auto_run(
                    run_date,
                    account_id,
                    self.identity.value,
                    token,
                    message,
                    failure_code=failure_code,
                )
                logger.warning("每日模拟交易未完成 account=%s: %s", account_id, exc)
                results.append(
                    {
                        "status": "failed",
                        "account_id": account_id,
                        "error": message,
                    }
                )
            finally:
                self._running.clear()
        return {
            "status": (
                "completed"
                if results and not any(item.get("status") == "failed" for item in results)
                else "partial"
                if results
                else "already_processed"
            ),
            "accounts": results,
            "calendar": expectation.as_dict(),
        }

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_due_once()
            except (OSError, sqlite3.Error):
                logger.exception("模拟盘每日自动执行检查失败")
            self._wake.wait(self.poll_seconds)
            self._wake.clear()


_auto_worker: PaperAutomationWorker | None = None
_auto_worker_root = ""
_paper_automation_lock = threading.RLock()


def get_paper_automation_worker() -> PaperAutomationWorker:
    global _auto_worker, _auto_worker_root
    root = str(get_config().data_root.resolve())
    with _paper_automation_lock:
        if _auto_worker is None or root != _auto_worker_root:
            if _auto_worker is not None:
                _auto_worker.stop()
            _auto_worker = PaperAutomationWorker(get_paper_service())
            _auto_worker_root = root
        return _auto_worker
