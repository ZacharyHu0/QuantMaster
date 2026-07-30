"""Shared durable-job vocabulary and worker identity helpers."""

from __future__ import annotations

import os
import socket
import time
import uuid
from dataclasses import dataclass

ACTIVE_STATUSES = frozenset({"queued", "running", "cancelling", "interrupted"})
TERMINAL_STATUSES = frozenset({
    "completed", "completed_with_errors", "failed", "cancelled",
})
DEFAULT_LEASE_SECONDS = 30.0


@dataclass(frozen=True)
class WorkerIdentity:
    value: str
    host: str
    pid: int

    @classmethod
    def create(cls, kind: str) -> WorkerIdentity:
        host = socket.gethostname() or "localhost"
        pid = os.getpid()
        suffix = uuid.uuid4().hex[:12]
        return cls(f"{kind}:{host}:{pid}:{suffix}", host, pid)


def lease_deadline(seconds: float = DEFAULT_LEASE_SECONDS) -> float:
    return time.time() + max(5.0, float(seconds))
