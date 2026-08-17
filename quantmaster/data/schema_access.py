"""Explicit callbacks for schema migrations crossing domain boundaries."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_factories: dict[str, Callable[..., Any]] = {}


def register_schema_target(name: str, factory: Callable[..., Any]) -> None:
    _factories[name] = factory


def schema_target(name: str, *args: Any, **kwargs: Any) -> Any:
    return schema_factory(name)(*args, **kwargs)


def schema_factory(name: str) -> Callable[..., Any]:
    factory = _factories.get(name)
    if factory is None:
        raise RuntimeError(f"schema 目标尚未注册: {name}")
    return factory


def register_ledger(factory: Callable[..., Any]) -> None:
    register_schema_target("ledger", factory)


def register_research_catalog(factory: Callable[..., Any]) -> None:
    register_schema_target("research_catalog", factory)


def register_paper_store(factory: Callable[..., Any]) -> None:
    register_schema_target("paper_store", factory)


def register_backtest_store(factory: Callable[..., Any]) -> None:
    register_schema_target("backtest_store", factory)


def register_lab_store(factory: Callable[..., Any]) -> None:
    register_schema_target("lab_store", factory)


def register_rotation_store(factory: Callable[..., Any]) -> None:
    register_schema_target("rotation_store", factory)


def register_membership_loader(factory: Callable[..., Any]) -> None:
    register_schema_target("membership_loader", factory)


def register_market_overview_publisher(factory: Callable[..., Any]) -> None:
    register_schema_target("market_overview_publisher", factory)
