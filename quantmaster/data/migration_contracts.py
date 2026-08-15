"""Shared migration records and registration seam."""

from __future__ import annotations

import importlib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class MigrationRecord:
    record_key: str
    outcome: str
    diagnostic_code: str = ""
    unknown_fields: tuple[str, ...] = ()
    detail: str = ""


class DomainMigrator(Protocol):
    name: str

    def inspect(self, root: Path) -> Iterable[MigrationRecord]: ...

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]: ...

    def rollback(self, root: Path, backup_root: Path) -> None: ...


_MIGRATORS: dict[str, DomainMigrator] = {}


def register_migrator(migrator: DomainMigrator) -> None:
    if not migrator.name or migrator.name in _MIGRATORS:
        raise ValueError(f"重复或无效的迁移类型：{migrator.name!r}")
    _MIGRATORS[migrator.name] = migrator


def registered_migrators() -> tuple[str, ...]:
    return tuple(sorted(_MIGRATORS))


def migrator_named(name: str) -> DomainMigrator | None:
    return _MIGRATORS.get(name)


class _BuiltinMigrator:
    def __init__(self, name: str, module: str, attribute: str, *, construct: bool = False):
        self.name = name
        self._module = module
        self._attribute = attribute
        self._construct = construct
        self._value: DomainMigrator | None = None

    def _resolve(self) -> DomainMigrator:
        if self._value is None:
            value = getattr(importlib.import_module(self._module), self._attribute)
            self._value = value() if self._construct else value
        return self._value

    def __getattr__(self, name: str):
        return getattr(self._resolve(), name)


_BUILTINS = (
    ("market_data", "quantmaster.data.legacy_migrations", "market_data_legacy_migrator"),
    ("decision", "quantmaster.decision.migration", "decision_legacy_migrator"),
    ("after_close", "quantmaster.after_close.migration", "after_close_legacy_migrator"),
    ("news", "quantmaster.ai.news_migration", "news_contract_migrator"),
    ("automation-contract-v9", "quantmaster.automation.migration", "automation_contract_migrator"),
    ("paper-ledger", "quantmaster.backtest.paper_legacy_migration", "PaperLegacyMigrator", True),
    ("startup-schema", "quantmaster.data.startup_schema_migration", "startup_schema_migrator"),
    ("store-schema", "quantmaster.data.store_schema_migration", "store_schema_migrator"),
    ("data-jobs", "quantmaster.data.job_migration", "data_job_legacy_migrator"),
    ("research-jobs", "quantmaster.research.job_migration", "research_job_legacy_migrator"),
    ("remaining-schema", "quantmaster.data.remaining_schema_migration", "remaining_schema_migrator"),
    ("lab-model-artifact", "quantmaster.lab.model_migration", "lab_model_artifact_migrator"),
)


def register_builtin_migrations() -> None:
    for spec in _BUILTINS:
        name, module, attribute, *options = spec
        if migrator_named(name) is None:
            register_migrator(_BuiltinMigrator(name, module, attribute, construct=bool(options)))
