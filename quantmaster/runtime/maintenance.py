"""Global stop-the-world barrier for data-root migration and destructive maintenance."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class MaintenanceActiveError(RuntimeError):
    """A write was attempted while the data root was frozen."""


@dataclass(frozen=True)
class MaintenanceParticipant:
    name: str
    drain: Callable[[], None]
    resume: Callable[[], None]
    idle: Callable[[], bool]


@dataclass(frozen=True)
class MaintenanceLease:
    token: str
    reason: str


class MaintenanceBarrier:
    """Coordinate bounded component drain before exposing a read-only frozen state."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._participants: dict[str, MaintenanceParticipant] = {}
        self._state = "open"
        self._token = ""
        self._reason = ""
        self._local = threading.local()

    @property
    def active(self) -> bool:
        with self._lock:
            return self._state != "open"

    @property
    def frozen(self) -> bool:
        with self._lock:
            return self._state == "frozen"

    def status(self) -> dict[str, object]:
        with self._lock:
            return {
                "state": self._state,
                "active": self._state != "open",
                "reason": self._reason,
                "participants": sorted(self._participants),
            }

    def register(self, participant: MaintenanceParticipant) -> Callable[[], None]:
        with self._lock:
            if participant.name in self._participants:
                raise ValueError(f"维护参与者重复注册: {participant.name}")
            self._participants[participant.name] = participant

        def unregister() -> None:
            with self._lock:
                current = self._participants.get(participant.name)
                if current is participant:
                    self._participants.pop(participant.name, None)

        return unregister

    def enter(self, reason: str, *, timeout: float = 30.0) -> MaintenanceLease:
        with self._lock:
            if self._state != "open":
                raise MaintenanceActiveError(
                    f"维护屏障已激活: {self._reason or self._state}"
                )
            token = uuid.uuid4().hex
            self._state = "draining"
            self._token = token
            self._reason = str(reason)[:300]
            participants = list(self._participants.values())
        drained: list[MaintenanceParticipant] = []
        try:
            for participant in participants:
                participant.drain()
                drained.append(participant)
            deadline = time.monotonic() + max(0.1, float(timeout))
            while True:
                busy = [participant.name for participant in drained if not participant.idle()]
                if not busy:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("后台组件未在期限内排空: " + ", ".join(busy))
                time.sleep(0.05)
            with self._lock:
                if self._token != token or self._state != "draining":
                    raise MaintenanceActiveError("维护屏障所有权在排空期间失效")
                self._state = "frozen"
            return MaintenanceLease(token, self._reason)
        except Exception:
            with self._lock:
                if self._token == token:
                    self._state = "open"
                    self._token = ""
                    self._reason = ""
            for participant in reversed(drained):
                participant.resume()
            raise

    def exit(self, lease: MaintenanceLease) -> None:
        with self._lock:
            if self._token != lease.token or self._state != "frozen":
                raise MaintenanceActiveError("维护屏障租约无效或已经释放")
            participants = list(self._participants.values())
            self._state = "open"
            self._token = ""
            self._reason = ""
        failures = []
        for participant in reversed(participants):
            try:
                participant.resume()
            except Exception as exc:  # lifecycle boundary: attempt all resumes
                logger.exception(
                    "Maintenance participant failed to resume name=%s", participant.name,
                )
                failures.append(f"{participant.name}: {type(exc).__name__}: {exc}")
        if failures:
            raise RuntimeError("维护结束后组件恢复失败: " + "; ".join(failures))

    def require_writable(self) -> None:
        with self._lock:
            if self._state != "open" and not self.write_authorized:
                raise MaintenanceActiveError(
                    f"数据根处于维护状态: {self._reason or self._state}"
                )

    @property
    def write_authorized(self) -> bool:
        token = str(getattr(self._local, "authorized_token", ""))
        with self._lock:
            return bool(token and self._state == "frozen" and token == self._token)

    @contextmanager
    def authorize(self, lease: MaintenanceLease):
        """Allow only the lease-owning thread to perform the frozen migration."""
        with self._lock:
            if self._state != "frozen" or lease.token != self._token:
                raise MaintenanceActiveError("维护写授权租约无效")
        previous = getattr(self._local, "authorized_token", "")
        self._local.authorized_token = lease.token
        try:
            yield
        finally:
            self._local.authorized_token = previous


maintenance_barrier = MaintenanceBarrier()
