"""Generation-owned executor for synchronous HTTP stream producers."""

from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

logger = logging.getLogger(__name__)


class StreamGenerationClosed(RuntimeError):
    """The client or Web generation stopped accepting producer work."""


class WebStreamRuntime:
    """Own stream futures for exactly one immutable Web generation."""

    def __init__(self, generation: str | None = None, *, max_workers: int = 16) -> None:
        self.generation = str(generation or os.environ.get("QM_WEB_GENERATION", "0"))
        self._max_workers = max(1, int(max_workers))
        self._lock = threading.RLock()
        self._executor: ThreadPoolExecutor | None = None
        self._futures: dict[Future[Any], dict[str, Any]] = {}
        self._accepting = True
        self._phase = "accepting"
        self._issues: list[dict[str, str]] = []

    @property
    def accepting(self) -> bool:
        with self._lock:
            return self._accepting

    def submit(
        self,
        task: Callable[[], None],
        *,
        request_id: str,
        cancel: threading.Event,
    ) -> Future[Any]:
        with self._lock:
            if not self._accepting:
                raise StreamGenerationClosed(
                    f"Web generation {self.generation} is draining"
                )
            if self._executor is None:
                self._executor = ThreadPoolExecutor(
                    max_workers=self._max_workers,
                    thread_name_prefix=f"qm-web-stream-g{self.generation}",
                )
            future = self._executor.submit(task)
            self._futures[future] = {
                "request_id": str(request_id),
                "cancel": cancel,
                "diagnostic_id": f"QM-STREAM-{uuid.uuid4().hex[:10].upper()}",
                "created_at": time.time(),
            }
            future.add_done_callback(self._forget)
            return future

    def _forget(self, future: Future[Any]) -> None:
        with self._lock:
            self._futures.pop(future, None)

    def shutdown(self, timeout: float = 5.0) -> None:
        """Fence admission, signal producers, then wait only for owned futures."""

        with self._lock:
            self._accepting = False
            self._phase = "draining"
            executor, self._executor = self._executor, None
            futures = list(self._futures.items())
        for _future, metadata in futures:
            metadata["cancel"].set()
        deadline = time.monotonic() + max(0.0, float(timeout))
        for future, metadata in futures:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                future.result(timeout=remaining)
            except TimeoutError:
                issue = {
                    "diagnostic_id": str(metadata["diagnostic_id"]),
                    "phase": "draining",
                    "detail": "stream producer exceeded shutdown deadline",
                }
                with self._lock:
                    self._issues.append(issue)
                logger.warning(
                    "Web stream 排空超时 generation=%s request_id=%s diagnostic_id=%s",
                    self.generation, metadata["request_id"], metadata["diagnostic_id"],
                )
            except (StreamGenerationClosed, Exception):
                # Producer exceptions are already converted to stream events
                # by the request wrapper; shutdown owns convergence only.
                pass
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        with self._lock:
            self._phase = "stopped"

    def status(self) -> dict[str, Any]:
        with self._lock:
            active = sum(1 for future in self._futures if not future.done())
            return {
                "state": "running" if self._accepting else self._phase,
                "generation": self.generation,
                "phase": self._phase,
                "task_counts": {
                    "active": active,
                    "converging": active if not self._accepting else 0,
                    "handoff": 0,
                },
                "timeout_issues": list(self._issues),
            }
