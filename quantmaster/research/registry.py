"""Declarative provider registry and built-in research metadata."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from typing import Any

from quantmaster.research.contracts import (
    ArtifactKind,
    AssetClass,
    DataRequest,
    FactorProviderSpec,
    ResearchSpec,
)

ProviderFunction = Callable[..., Any]


class ProviderRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, FactorProviderSpec] = {}
        self._functions: dict[str, ProviderFunction] = {}
        self._outputs: dict[tuple[str, str], ResearchSpec] = {}
        self._latest_outputs: dict[str, ResearchSpec] = {}
        self._aliases: dict[str, str] = {}

    def register(self, provider: FactorProviderSpec, function: ProviderFunction | None = None) -> None:
        if provider.id in self._providers and self._providers[provider.id] != provider:
            raise ValueError(f"provider {provider.id} 已注册且定义不同")
        for output in provider.outputs:
            identity = (output.id, output.version)
            existing = self._outputs.get(identity)
            if existing and existing != output:
                raise ValueError(f"研究输出 {output.id}@{output.version} 已注册且定义不同")
            for alias in output.aliases:
                canonical = self._aliases.get(alias)
                if canonical and canonical != output.id:
                    raise ValueError(f"研究别名 {alias} 同时指向 {canonical} 和 {output.id}")
                self._aliases[alias] = output.id
            self._outputs[identity] = output
            # Bare IDs intentionally select the current registry definition.
            # Version-pinned plans must use ``resolve(..., version=...)`` so
            # a historical artifact never silently changes meaning.
            self._latest_outputs[output.id] = output
        self._providers[provider.id] = provider
        if function is not None:
            self._functions[provider.id] = function

    def provider(self, provider_id: str) -> FactorProviderSpec:
        try:
            return self._providers[provider_id]
        except KeyError:
            raise KeyError(f"未知研究 provider: {provider_id}") from None

    def function(self, provider_id: str) -> ProviderFunction:
        try:
            return self._functions[provider_id]
        except KeyError:
            raise KeyError(f"provider {provider_id} 没有可执行实现") from None

    def resolve(self, output_id: str, *, version: str | None = None) -> ResearchSpec:
        canonical = self._aliases.get(output_id, output_id)
        try:
            if version is not None:
                return self._outputs[(canonical, str(version))]
            return self._latest_outputs[canonical]
        except KeyError:
            suffix = f"@{version}" if version is not None else ""
            raise KeyError(f"未知或不可用的研究输出: {output_id}{suffix}") from None

    def select(
        self,
        ids: Iterable[str] | None = None,
        *,
        tags: Iterable[str] | None = None,
        kind: ArtifactKind | None = None,
        asset_class: AssetClass | None = None,
    ) -> list[ResearchSpec]:
        if ids:
            selected = []
            for item in ids:
                if item in self._providers:
                    selected.extend(self._providers[item].outputs)
                else:
                    selected.append(self.resolve(item))
            selected = list({(item.id, item.version): item for item in selected}.values())
        else:
            selected = list(self._latest_outputs.values())
        required_tags = set(tags or ())
        return sorted((
            item for item in selected
            if (kind is None or item.kind == kind)
            and (asset_class is None or asset_class in item.asset_classes)
            and (not required_tags or required_tags.issubset(item.tags))
        ), key=lambda item: (item.kind.value, item.id, item.version))

    def providers_for(self, specs: Iterable[ResearchSpec]) -> list[FactorProviderSpec]:
        provider_ids = dict.fromkeys(item.provider_id for item in specs if item.provider_id)
        return [self.provider(provider_id) for provider_id in provider_ids]

    def catalog(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.select()]


def _spec(
    id_: str,
    kind: ArtifactKind,
    *,
    assets: tuple[AssetClass, ...],
    provider: str,
    description: str,
    tags: tuple[str, ...],
    lookback: int = 0,
    lookahead: int = 0,
    output: str | None = None,
    dependencies: tuple[DataRequest, ...] = (),
) -> ResearchSpec:
    return ResearchSpec(
        id=id_, version="1.0.0", kind=kind, asset_classes=assets,
        name=id_, description=description, tags=tags, provider_id=provider,
        output=output or id_, dependencies=dependencies,
        lookback_sessions=lookback, lookahead_sessions=lookahead,
    )


def built_in_registry() -> ProviderRegistry:
    from quantmaster.research.providers import (
        compute_core_factors,
        compute_forward_labels,
        compute_qm_style_v1,
    )

    registry = ProviderRegistry()
    all_assets = (AssetClass.STOCK, AssetClass.ETF, AssetClass.FUTURE)
    bars = (DataRequest(
        "bars", ("open", "high", "low", "close", "volume", "amount", "research_price"),
        lookback_sessions=20,
    ),)
    factor_outputs = tuple(_spec(
        id_, ArtifactKind.FACTOR, assets=all_assets, provider="cross_asset_core",
        description=description, tags=("builtin", "cross-asset", "interpretable"),
        lookback=lookback, dependencies=bars,
    ) for id_, description, lookback in (
        ("cross_momentum_20d", "20 日研究价格动量。", 20),
        ("cross_reversal_5d", "5 日研究价格反转。", 5),
        ("cross_realized_vol_20d", "20 日实现波动率。", 20),
        ("cross_volume_ratio_20d", "当前成交量相对 20 日均量。", 20),
        ("cross_price_volume_corr_20d", "收益与成交量变化的 20 日相关。", 20),
        ("cross_amihud_20d", "20 日 Amihud 非流动性。", 20),
    ))
    registry.register(FactorProviderSpec(
        id="cross_asset_core", outputs=factor_outputs, dependencies=bars,
        asset_classes=all_assets,
    ), compute_core_factors)

    label_outputs = tuple(_spec(
        f"fwd_return_{horizon}d", ArtifactKind.LABEL, assets=all_assets,
        provider="forward_returns", description=f"未来 {horizon} 个交易日研究价格收益。",
        tags=("builtin", "label", "point-in-time"), lookahead=horizon,
        dependencies=(DataRequest("bars", ("research_price",), lookahead_sessions=horizon),),
    ) for horizon in (1, 3, 5, 7, 10, 20, 30))
    registry.register(FactorProviderSpec(
        id="forward_returns", outputs=label_outputs,
        dependencies=(DataRequest("bars", ("research_price",), lookahead_sessions=7),),
        asset_classes=all_assets,
    ), compute_forward_labels)

    risk_dependencies = (
        DataRequest("stock_bars", ("close",), lookback_sessions=252),
        DataRequest(
            "stock_daily_basic",
            ("total_mv", "pb", "turnover_rate_f", "turnover_rate", "industry"),
            lookback_sessions=20,
        ),
    )
    risk_outputs = tuple(_spec(
        f"qm_style_{name.lower()}", ArtifactKind.RISK, assets=(AssetClass.STOCK,),
        provider="qm_style_v1", description=description,
        tags=("builtin", "risk-model", "qm-style-v1"), lookback=lookback,
        output=name, dependencies=risk_dependencies,
    ) for name, description, lookback in (
        ("SIZE", "对数总市值风格暴露。", 1),
        ("VALUE", "负对数 PB 价值暴露。", 1),
        ("MOMENTUM", "252 至 21 日动量暴露。", 252),
        ("VOLATILITY", "60 日收益波动率暴露。", 60),
        ("LIQUIDITY", "20 日自由流通换手率暴露。", 20),
    ))
    registry.register(FactorProviderSpec(
        id="qm_style_v1", outputs=risk_outputs, dependencies=risk_dependencies,
        asset_classes=(AssetClass.STOCK,),
    ), compute_qm_style_v1)

    _register_legacy_catalog(registry)
    return registry


def _register_legacy_catalog(registry: ProviderRegistry) -> None:
    """Expose the existing curated 48 without changing their execution path."""
    from quantmaster.lab.catalog import curated_catalog

    for legacy in curated_catalog():
        numbers = [int(item) for item in re.findall(r"(?<![A-Za-z_])[0-9]+", legacy.expression)]
        lookback = max(numbers, default=1)
        dependencies = (DataRequest(
            "stock_bars", tuple(legacy.required_features), lookback_sessions=lookback,
        ),)
        output = ResearchSpec(
            id=legacy.slug,
            # The publication-aligned news factor changed its evidence and
            # timing contract.  Preserve pre-v2 artifacts as 1.0.0 evidence;
            # new plans must use a distinct identity instead of overwriting
            # the historic specification in the research catalog.
            version="2.0.0" if legacy.slug == "news_sentiment" else "1.0.0",
            kind=ArtifactKind.FACTOR,
            asset_classes=(AssetClass.STOCK,), name=legacy.name,
            description=legacy.description, tags=tuple(dict.fromkeys((*legacy.tags, "curated-48"))),
            provider_id=f"legacy_{legacy.slug}", output=legacy.slug,
            dependencies=dependencies, lookback_sessions=lookback,
        )
        registry.register(FactorProviderSpec(
            id=f"legacy_{legacy.slug}", outputs=(output,), dependencies=dependencies,
            asset_classes=(AssetClass.STOCK,),
        ))
