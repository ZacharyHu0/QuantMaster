"""Explicit seam for Lab to invoke optional factor-mining implementations."""

from __future__ import annotations

from typing import Any

_miners: dict[str, Any] = {}


def register_factor_miners(**miners: Any) -> None:
    _miners.update(miners)


def factor_miner(name: str) -> Any:
    try:
        return _miners[name]
    except KeyError as exc:
        raise RuntimeError(f"因子挖掘器尚未注册: {name}") from exc
