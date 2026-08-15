"""Transport-neutral actor identity passed between bot channels and automation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(slots=True)
class ActorContext:
    channel: Literal["weixin", "feishu"]
    target: str
    account_id: str
    chat_type: Literal["direct", "group"]
    sender_id: str
    sender_name: str = ""
    message_id: str = ""
    reply_to: str = ""
    reply_text: str = ""

    @property
    def actor_key(self) -> str:
        return f"{self.channel}:{self.account_id}:{self.sender_id}"

    @property
    def route_key(self) -> str:
        return f"{self.channel}:{self.account_id}:{self.target}"
