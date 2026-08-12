"""Owner-scoped runtime lifecycle diagnostics and managed long-running tasks.

This module deliberately does not inspect global asyncio/thread state.  A
component may only register and converge work that it created for its own
generation; framework and third-party tasks remain with their real owners.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class ManagedTask:
    name: str
    component_owner: str
    generation: str
    created_at: str
    phase: str
    diagnostic_id: str
    shutdown_policy: str
    deadline_seconds: float
    thread: threading.Thread
    stop: Callable[[], None] | None = None

    def public(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "component_owner": self.component_owner,
            "generation": self.generation,
            "created_at": self.created_at,
            "phase": self.phase,
            "diagnostic_id": self.diagnostic_id,
            "shutdown_policy": self.shutdown_policy,
            "deadline_seconds": self.deadline_seconds,
            "alive": self.thread.is_alive(),
        }


class RuntimeLifecycle:
    """Track one owner's immutable generation and its shutdown state machine."""

    def __init__(self, component_owner: str, generation: str) -> None:
        self.component_owner = str(component_owner)
        self.generation = str(generation)
        self._lock = threading.RLock()
        self._tasks: dict[str, ManagedTask] = {}
        self._state = "running"
        self._phase = "accepting"
        self._phase_started = time.monotonic()
        self._deadline_seconds = 0.0
        self._timeout_issues: list[dict[str, Any]] = []
        self._durable_pending = 0
        self._handoff = 0

    @property
    def accepting(self) -> bool:
        with self._lock:
            return self._state == "running"

    def start_thread(
        self,
        *,
        name: str,
        target: Callable[[], None],
        phase: str,
        diagnostic_id: str,
        shutdown_policy: str,
        deadline_seconds: float,
        stop: Callable[[], None] | None = None,
        daemon: bool = True,
    ) -> threading.Thread:
        """Create and register one task owned by this exact generation."""

        with self._lock:
            if self._state != "running":
                raise RuntimeError(
                    f"{self.component_owner} generation {self.generation} is draining"
                )
            task_id = uuid.uuid4().hex

            def run() -> None:
                try:
                    target()
                finally:
                    with self._lock:
                        self._tasks.pop(task_id, None)

            thread = threading.Thread(target=run, name=name, daemon=daemon)
            self._tasks[task_id] = ManagedTask(
                name=name,
                component_owner=self.component_owner,
                generation=self.generation,
                created_at=_utc_now(),
                phase=str(phase),
                diagnostic_id=str(diagnostic_id),
                shutdown_policy=str(shutdown_policy),
                deadline_seconds=max(0.0, float(deadline_seconds)),
                thread=thread,
                stop=stop,
            )
            thread.start()
            return thread

    def begin_shutdown(self, *, reloading: bool = False) -> None:
        with self._lock:
            if self._state in {"stopping", "stopped"}:
                return
            self._state = "reloading" if reloading else "draining"
            self._phase = "stop_accepting"
            self._phase_started = time.monotonic()
            self._deadline_seconds = 2.0

    def enter_phase(self, phase: str, deadline_seconds: float) -> None:
        with self._lock:
            self._state = "stopping" if phase not in {"drain_atomic", "handoff"} else self._state
            self._phase = str(phase)
            self._phase_started = time.monotonic()
            self._deadline_seconds = max(0.0, float(deadline_seconds))

    def set_durable_counts(self, *, pending: int, handoff: int = 0) -> None:
        with self._lock:
            self._durable_pending = max(0, int(pending))
            self._handoff = max(0, int(handoff))

    def record_timeout(
        self,
        *,
        component: str,
        phase: str,
        diagnostic_id: str,
        detail: str,
    ) -> None:
        issue = {
            "component": str(component)[:80],
            "phase": str(phase)[:80],
            "diagnostic_id": str(diagnostic_id)[:80],
            "detail": str(detail)[:240],
        }
        with self._lock:
            if issue not in self._timeout_issues:
                self._timeout_issues.append(issue)
                self._timeout_issues = self._timeout_issues[-20:]
        logger.warning(
            "生命周期阶段超时 component=%s phase=%s diagnostic_id=%s detail=%s",
            issue["component"], issue["phase"], issue["diagnostic_id"], issue["detail"],
        )

    def converge_owned(self) -> None:
        """Signal and join only tasks registered by this owner/generation."""

        with self._lock:
            tasks = list(self._tasks.values())
        for task in tasks:
            if task.component_owner != self.component_owner or task.generation != self.generation:
                continue
            if task.stop is not None:
                try:
                    task.stop()
                except (OSError, RuntimeError, ValueError):
                    logger.warning(
                        "托管任务停止信号失败 diagnostic_id=%s", task.diagnostic_id,
                        exc_info=True,
                    )
        for task in tasks:
            if task.component_owner != self.component_owner or task.generation != self.generation:
                continue
            if task.thread is threading.current_thread():
                continue
            task.thread.join(timeout=task.deadline_seconds)
            if task.thread.is_alive():
                self.record_timeout(
                    component=task.component_owner,
                    phase=task.phase,
                    diagnostic_id=task.diagnostic_id,
                    detail=f"task {task.name} exceeded {task.deadline_seconds:.1f}s deadline",
                )

    def finish(self) -> None:
        with self._lock:
            self._state = "stopped"
            self._phase = "stopped"
            self._phase_started = time.monotonic()
            self._deadline_seconds = 0.0

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            tasks = [task.public() for task in self._tasks.values() if task.thread.is_alive()]
            elapsed = max(0.0, time.monotonic() - self._phase_started)
            remaining = max(0.0, self._deadline_seconds - elapsed)
            state = self._state
            phase = self._phase
            issues = list(self._timeout_issues)
            pending = self._durable_pending
            handoff = self._handoff
        converging = sum(1 for task in tasks if state != "running")
        return {
            "state": state,
            "generation": self.generation,
            "phase": phase,
            "task_counts": {
                "active": len(tasks),
                "converging": converging,
                "handoff": handoff,
            },
            "durable_queue": {"pending": pending},
            "deadline": {
                "phase": phase,
                "remaining_seconds": round(remaining, 3),
            },
            "timeout_issues": issues,
            "tasks": tasks,
        }
