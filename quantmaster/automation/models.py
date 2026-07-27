from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def stable_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class AlertEvent:
    kind: str
    score: float
    severity: str
    direction: str = "neutral"
    occurred_at: str = field(default_factory=utc_now)
    data_as_of: str = ""
    symbols: list[str] = field(default_factory=list)
    relevance: str = "market"
    evidence: list[str] = field(default_factory=list)
    source_urls: list[str] = field(default_factory=list)
    dedupe_key: str = ""
    expires_at: str = ""
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.score = max(0.0, min(100.0, float(self.score)))
        if not self.dedupe_key:
            self.dedupe_key = stable_hash({
                "kind": self.kind, "direction": self.direction,
                "symbols": sorted(self.symbols), "data_as_of": self.data_as_of,
            })

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ActorContext:
    channel: Literal["weixin", "feishu"]
    target: str
    account_id: str
    chat_type: Literal["direct", "group"]
    sender_id: str
    sender_name: str = ""

    @property
    def actor_key(self) -> str:
        return f"{self.channel}:{self.account_id}:{self.sender_id}"

    @property
    def route_key(self) -> str:
        return f"{self.channel}:{self.account_id}:{self.target}"


@dataclass(slots=True)
class LedgerIntent:
    actor: str
    entry_type: Literal["trade", "cashflow", "paper_rebalance"]
    normalized_payload: dict[str, Any]
    route_key: str
    expires_at: str
    payload_hash: str = ""
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: str = "pending"

    def __post_init__(self) -> None:
        if not self.payload_hash:
            self.payload_hash = stable_hash(self.normalized_payload)
