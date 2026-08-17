"""Shared research value objects with no data-source dependencies."""

from __future__ import annotations

import enum


class _ValueEnum(enum.StrEnum):
    def __str__(self) -> str:
        return self.value


class AssetClass(_ValueEnum):
    STOCK = "stock"
    ETF = "etf"
    FUTURE = "future"


class Frequency(_ValueEnum):
    DAILY = "1d"
    MINUTE_1 = "1m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"
    MINUTE_60 = "60m"


class ArtifactKind(_ValueEnum):
    RAW = "raw"
    FACTOR = "factors"
    LABEL = "labels"
    RISK = "risk"
    MODEL = "models"
