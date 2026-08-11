"""Dependency planner and reproducible execution engine for the research lake."""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Iterable
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.research.adapters import (
    COMPLETE_CROSS_SECTION_DATASETS,
    DATASET_BY_ID,
    DEFAULT_DATASETS,
    CompositeResearchAdapter,
    TushareResearchAdapter,
)
from quantmaster.research.contracts import (
    ArtifactKind,
    ArtifactRef,
    AssetClass,
    ExecutionPlan,
    Frequency,
    KernelBackend,
    PlanTask,
    ResearchSpec,
    RunManifest,
    content_hash,
    utc_now,
)
from quantmaster.research.diagnostics import factor_diagnostics
from quantmaster.research.kernel import Kernel
from quantmaster.research.lake import ResearchLake
from quantmaster.research.providers import build_future_continuous
from quantmaster.research.registry import ProviderRegistry, built_in_registry
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.trading_sessions import market_date


def _asset_dataset(asset_class: AssetClass) -> str:
    return {
        AssetClass.STOCK: "stock_bars",
        AssetClass.ETF: "etf_bars",
        AssetClass.FUTURE: "future_continuous",
    }[asset_class]


def _dependencies_for_asset(dataset_id: str, asset_class: AssetClass) -> tuple[str, ...]:
    if dataset_id == "bars":
        if asset_class == AssetClass.FUTURE:
            return ("future_bars", "future_main_mapping")
        return (_asset_dataset(asset_class),)
    if dataset_id == "future_continuous":
        return ("future_bars", "future_main_mapping")
    return (dataset_id,)


class ResearchEngine:
    def __init__(
        self,
        lake: ResearchLake | None = None,
        registry: ProviderRegistry | None = None,
        adapter: TushareResearchAdapter | CompositeResearchAdapter | None = None,
        *,
        read_only: bool = False,
    ):
        self.read_only = bool(read_only)
        self.lake = lake or ResearchLake(read_only=self.read_only)
        self.registry = registry or built_in_registry()
        self.adapter = adapter or CompositeResearchAdapter(self.lake.catalog)
        if not self.read_only:
            for item in self.registry.select():
                self.lake.catalog.register_spec(item)

    def catalog(self) -> dict[str, Any]:
        return {
            "datasets": [item.to_dict() for item in DATASET_BY_ID.values()],
            "specs": self.registry.catalog(),
            "partitions": self.coverage(),
        }

    def capabilities(self) -> dict[str, Any]:
        from quantmaster.research.kernel import kernel_capabilities

        return {
            "data": self.adapter.capabilities(),
            "kernel": kernel_capabilities(),
        }

    def preview_date(self, dataset_id: str, trade_date: str) -> pd.DataFrame:
        """Build a non-persistent degraded cross-section preview."""
        if not isinstance(self.adapter, CompositeResearchAdapter):
            raise RuntimeError("当前研究适配器不支持 degraded preview")
        return self.adapter.preview_date(dataset_id, trade_date)

    def coverage(self) -> list[dict[str, Any]]:
        rows = self.lake.catalog.partitions()
        grouped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for item in rows:
            key = (item["kind"], item["asset_class"], item["frequency"], item["dataset_id"])
            value = grouped.setdefault(key, {
                "kind": key[0], "asset_class": key[1], "frequency": key[2],
                "dataset_id": key[3], "partitions": 0, "rows": 0,
                "start": item["trade_date"], "end": item["trade_date"],
            })
            value["partitions"] += 1
            value["rows"] += int(item["row_count"])
            value["start"] = min(value["start"], item["trade_date"])
            value["end"] = max(value["end"], item["trade_date"])
        return sorted(grouped.values(), key=lambda item: (
            item["kind"], item["asset_class"], item["dataset_id"],
        ))

    def plan(
        self,
        start: str,
        end: str,
        *,
        asset_classes: Iterable[AssetClass | str] = (AssetClass.STOCK,),
        datasets: Iterable[str] | None = None,
        spec_ids: Iterable[str] | None = None,
        mode: str = "historical",
        backend: KernelBackend | str = KernelBackend.AUTO,
    ) -> ExecutionPlan:
        start_date, end_date = date.fromisoformat(start), date.fromisoformat(end)
        if start_date > end_date:
            raise ValueError("开始日期不能晚于结束日期")
        if end_date > market_date():
            raise ValueError("结束日期不能晚于今天")
        if mode not in {"historical", "incremental"}:
            raise ValueError("mode 只支持 historical/incremental")
        assets = tuple(dict.fromkeys(AssetClass(item) for item in asset_classes))
        if not assets:
            raise ValueError("至少选择一种资产")
        requested_specs = self.registry.select(spec_ids) if spec_ids else []
        for spec in requested_specs:
            if not any(asset in spec.asset_classes for asset in assets):
                raise ValueError(f"{spec.id} 不支持已选资产")
        selected_datasets = set(datasets or ())
        if not selected_datasets:
            for asset in assets:
                selected_datasets.update(DEFAULT_DATASETS[asset])
        for dataset_id in selected_datasets:
            if dataset_id not in DATASET_BY_ID:
                raise ValueError(f"未知研究数据集: {dataset_id}")
        for spec in requested_specs:
            for asset in assets:
                if asset not in spec.asset_classes:
                    continue
                for request in spec.dependencies:
                    selected_datasets.update(_dependencies_for_asset(request.dataset_id, asset))

        tasks: list[PlanTask] = []
        warnings: list[str] = []
        target_dates: set[str] = set()
        for asset in assets:
            asset_specs = [item for item in requested_specs if asset in item.asset_classes]
            lookback = max((item.lookback_sessions for item in asset_specs), default=0)
            lookahead = max((item.lookahead_sessions for item in asset_specs), default=0)
            history_margin = lookback * 2 + 10 if lookback else 0
            future_margin = lookahead * 2 + 10 if lookahead else 0
            calendar_start = str(
                (pd.Timestamp(start) - pd.tseries.offsets.BDay(history_margin)).date()
            )
            latest_needed = pd.Timestamp(end) + pd.tseries.offsets.BDay(future_margin)
            calendar_end = str(min(latest_needed.date(), market_date()))
            calendar, source = self.adapter.official_calendar(asset, calendar_start, calendar_end)
            if source.startswith("fallback"):
                calendar_dataset = (
                    "future_bars" if asset == AssetClass.FUTURE else _asset_dataset(asset)
                )
                local_partitions = self.lake.catalog.partitions(
                    kind=ArtifactKind.RAW, asset_class=asset, frequency=Frequency.DAILY,
                    dataset_id=calendar_dataset, start=calendar_start, end=calendar_end,
                )
                if local_partitions:
                    calendar = pd.DatetimeIndex(
                        pd.to_datetime([item["trade_date"] for item in local_partitions]).unique()
                    ).sort_values()
                    source = f"local:{calendar_dataset}"
                    warnings.append(
                        f"{asset.value} 官方日历不可用，复用本地已落盘交易日"
                    )
                else:
                    raise RuntimeError(
                        f"{asset.value} 官方交易日历不可用，且本地没有已验证交易日；"
                        "为避免把节假日当作交易日，已拒绝生成研究计划"
                    )
            start_stamp, end_stamp = pd.Timestamp(start), pd.Timestamp(end)
            target_calendar = calendar[(calendar >= start_stamp) & (calendar <= end_stamp)]
            before = calendar[calendar < start_stamp][-lookback:] if lookback else calendar[:0]
            after = calendar[calendar > end_stamp][:lookahead] if lookahead else calendar[:0]
            needed_calendar = before.append(target_calendar).append(after).unique().sort_values()
            asset_targets = [str(item.date()) for item in target_calendar]
            target_dates.update(asset_targets)
            all_dates = [str(item.date()) for item in needed_calendar]
            asset_datasets = [
                item for item in selected_datasets
                if DATASET_BY_ID[item].asset_class == asset
            ]
            for dataset_id in sorted(asset_datasets):
                definition = DATASET_BY_ID[dataset_id]
                snapshot_date = asset_targets[-1] if asset_targets else end
                dates = [snapshot_date] if definition.partitioning == "snapshot" else all_dates
                revision_start = str((pd.Timestamp(end) - pd.tseries.offsets.BDay(
                    definition.revision_sessions
                )).date())
                for trade_date in dates:
                    existing = self.lake.catalog.partition(
                        ArtifactKind.RAW, asset, definition.frequency, dataset_id, trade_date,
                    )
                    revise = mode == "incremental" and trade_date >= revision_start
                    if existing and not revise:
                        continue
                    tasks.append(PlanTask(
                        kind="sync", dataset_id=dataset_id, asset_class=asset,
                        frequency=definition.frequency, trade_date=trade_date,
                        columns=definition.columns, reason="revision" if revise else "missing",
                    ))
            provider_ids = dict.fromkeys(
                item.provider_id for item in asset_specs if item.provider_id
            )
            for provider_id in provider_ids:
                tasks.append(PlanTask(
                    kind="compute", dataset_id=provider_id, asset_class=asset,
                    frequency=Frequency.DAILY, trade_date=end,
                    provider_ids=(provider_id,), reason="selected",
                ))

        capabilities = {item["dataset_id"]: item for item in self.adapter.capabilities()}
        pending_datasets = {
            task.dataset_id for task in tasks if task.kind == "sync"
        }
        blocks = []
        for dataset_id in sorted(pending_datasets):
            state = capabilities[dataset_id]["state"]
            if state == "temporary_failure":
                warnings.append(
                    f"{dataset_id} 上次同步暂时失败，本次计划将重新探测数据源"
                )
            elif state != "available":
                blocks.append({
                    "dataset_id": dataset_id,
                    "state": state,
                    "detail": capabilities[dataset_id]["detail"],
                })

        refs = tuple(
            ArtifactRef(
                kind=spec.kind, id=spec.id, version=spec.version,
                asset_class=asset, frequency=spec.frequency, output=spec.output,
            )
            for spec in requested_specs for asset in assets if asset in spec.asset_classes
        )
        row_estimates = {AssetClass.STOCK: 6000, AssetClass.ETF: 1200, AssetClass.FUTURE: 2500}
        estimated_rows = sum(
            row_estimates[task.asset_class] for task in tasks if task.kind == "sync"
        )
        return ExecutionPlan(
            id=uuid.uuid4().hex, start=start, end=end,
            target_dates=tuple(sorted(target_dates)), asset_classes=assets,
            frequency=Frequency.DAILY, datasets=tuple(sorted(selected_datasets)),
            selected_specs=refs, tasks=tuple(tasks), backend=KernelBackend(backend),
            warnings=tuple(dict.fromkeys(warnings)), capability_blocks=tuple(blocks),
            estimated_rows=estimated_rows, estimated_bytes=estimated_rows * 128,
        )

    def execute(
        self,
        plan: ExecutionPlan,
        *,
        cancelled: Callable[[], bool] | None = None,
        progress: Callable[[int, int, PlanTask], None] | None = None,
        continue_on_sync_error: bool = False,
    ) -> dict[str, Any]:
        if plan.capability_blocks:
            detail = "；".join(
                f"{item['dataset_id']}: {item['detail']}" for item in plan.capability_blocks
            )
            raise ValueError(f"研究计划存在能力阻塞：{detail}")
        started = utc_now()
        kernel = Kernel(plan.backend)
        input_records: list[dict[str, Any]] = []
        output_records: list[dict[str, Any]] = []
        warnings = list(plan.warnings)
        failed_tasks: list[dict[str, str]] = []
        run_id = plan.id
        manifest = RunManifest(
            run_id=run_id, plan_hash=plan.plan_hash, status="running",
            backend_requested=plan.backend, backend_used=kernel.backend_used,
            started_at=started, warnings=tuple(warnings),
        )
        self.lake.catalog.save_run(manifest)
        try:
            for index, task in enumerate(plan.tasks, start=1):
                if cancelled and cancelled():
                    raise InterruptedError("研究任务已取消")
                try:
                    records = self.execute_task(plan, task, kernel=kernel, run_id=run_id)
                except InterruptedError:
                    raise
                except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
                    if not continue_on_sync_error or task.kind != "sync":
                        raise
                    failure = {
                        "dataset_id": task.dataset_id,
                        "trade_date": task.trade_date,
                        "error": str(exc)[:300],
                    }
                    failed_tasks.append(failure)
                    warnings.append(
                        f"{task.dataset_id} {task.trade_date} 同步失败：{str(exc)[:180]}"
                    )
                    records = []
                (input_records if task.kind == "sync" else output_records).extend(records)
                if progress:
                    progress(index, len(plan.tasks), task)
            if kernel.fallback_reason:
                warnings.append(kernel.fallback_reason)
            diagnostics = self._emit_diagnostics(plan, run_id)
            finished = RunManifest(
                run_id=run_id,
                plan_hash=plan.plan_hash,
                status="completed_with_errors" if failed_tasks else "completed",
                backend_requested=plan.backend, backend_used=kernel.backend_used,
                started_at=started, finished_at=utc_now(),
                input_partitions=tuple(input_records), output_partitions=tuple(output_records),
                warnings=tuple(dict.fromkeys(warnings)),
            )
            payload = finished.to_dict()
            payload["diagnostics"] = diagnostics
            payload["failed_tasks"] = failed_tasks
            self.lake.catalog.save_run(payload)
            self.lake.write_run_files(run_id, payload)
            return payload
        except Exception as exc:
            failed = RunManifest(
                run_id=run_id, plan_hash=plan.plan_hash,
                status="cancelled" if isinstance(exc, InterruptedError) else "failed",
                backend_requested=plan.backend, backend_used=kernel.backend_used,
                started_at=started, finished_at=utc_now(),
                input_partitions=tuple(input_records), output_partitions=tuple(output_records),
                warnings=tuple(dict.fromkeys(warnings)), error=str(exc)[:1000],
            )
            self.lake.catalog.save_run(failed)
            raise

    def execute_task(
        self,
        plan: ExecutionPlan,
        task: PlanTask,
        *,
        kernel: Kernel | None = None,
        run_id: str = "",
    ) -> list[dict[str, Any]]:
        kernel = kernel or Kernel(plan.backend)
        if task.kind == "sync":
            frame = self.adapter.fetch_date(task.dataset_id, task.trade_date)
            if frame.empty:
                raise RuntimeError(f"{task.dataset_id} {task.trade_date} 返回空数据")
            if task.dataset_id in COMPLETE_CROSS_SECTION_DATASETS:
                quality = frame.attrs.get("research_partition_quality")
                if not isinstance(quality, dict) or quality.get("status") != "verified_complete":
                    raise RuntimeError(
                        f"{task.dataset_id} {task.trade_date} 缺少完整横截面质量证明，"
                        "拒绝发布 RAW 分区"
                    )
            return [self.lake.write_partition(
                ArtifactKind.RAW, task.asset_class, task.frequency, task.dataset_id,
                task.trade_date, frame, run_id=run_id, owner=run_id or plan.id,
            )]
        if task.kind != "compute":
            raise ValueError(f"未知计划任务: {task.kind}")
        provider = self.registry.provider(task.dataset_id)
        selected_specs = []
        for ref in plan.selected_specs:
            if ref.asset_class != task.asset_class:
                continue
            spec = self.registry.resolve(ref.id, version=ref.version)
            if spec.provider_id == provider.id:
                selected_specs.append(spec)
        if not selected_specs:
            return []
        inputs, input_hashes = self._provider_inputs(
            task.asset_class, provider.id, plan.start, plan.end,
        )
        if provider.id == "qm_style_v1":
            values = self.registry.function(provider.id)(inputs["bars"], inputs["daily_basic"])
            return self._write_risk(values, selected_specs, task.asset_class, run_id, input_hashes)
        if provider.id.startswith("legacy_"):
            values = self._compute_legacy(selected_specs[0], inputs["bars"])
        else:
            function = self.registry.function(provider.id)
            values = (
                function(inputs["bars"], kernel)
                if provider.id == "cross_asset_core" else function(inputs["bars"])
            )
        values["trade_date"] = pd.to_datetime(values["trade_date"])
        values = values.loc[
            (values["trade_date"] >= pd.Timestamp(plan.start))
            & (values["trade_date"] <= pd.Timestamp(plan.end))
        ]
        records = []
        for spec in selected_specs:
            if spec.output not in values:
                raise RuntimeError(f"provider {provider.id} 未产出 {spec.output}")
            records.extend(self.lake.write_artifact_values(
                spec, values, asset_class=task.asset_class, run_id=run_id,
                input_hashes=input_hashes,
            ))
        return records

    def _provider_inputs(
        self, asset_class: AssetClass, provider_id: str, start: str, end: str,
    ) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
        provider = self.registry.provider(provider_id)
        lookback = provider.lookback_sessions
        lookahead = provider.lookahead_sessions
        base_dataset = {
            AssetClass.STOCK: "stock_bars",
            AssetClass.ETF: "etf_bars",
            AssetClass.FUTURE: "future_bars",
        }[asset_class]
        available = self.lake.catalog.partitions(
            kind=ArtifactKind.RAW, asset_class=asset_class, frequency=Frequency.DAILY,
            dataset_id=base_dataset, end=market_date().isoformat(),
        )
        available_dates = pd.DatetimeIndex(
            pd.to_datetime([item["trade_date"] for item in available]).unique()
        ).sort_values()
        start_stamp, end_stamp = pd.Timestamp(start), pd.Timestamp(end)
        before = (
            available_dates[available_dates < start_stamp][-lookback:]
            if lookback else available_dates[:0]
        )
        after = (
            available_dates[available_dates > end_stamp][:lookahead]
            if lookahead else available_dates[:0]
        )
        load_start = str((before[0] if len(before) else start_stamp).date())
        load_end = str((after[-1] if len(after) else end_stamp).date())
        if asset_class == AssetClass.STOCK:
            bars = self.lake.read_range(
                ArtifactKind.RAW, asset_class, Frequency.DAILY, "stock_bars", load_start, load_end,
            )
            if bars.empty:
                raise RuntimeError(f"stock 缺少 {load_start} 至 {load_end} 的研究行情")
            adjustment = self.lake.read_range(
                ArtifactKind.RAW, asset_class, Frequency.DAILY, "stock_adj_factor", load_start, load_end,
            )
            if adjustment.empty:
                raise RuntimeError("股票研究价格缺少不可变 stock_adj_factor 分区")
            keys = ["trade_date", "symbol"]
            if bars.duplicated(keys).any() or adjustment.duplicated(keys).any():
                raise RuntimeError("股票 bars/adj_factor 主键重复，拒绝构造研究价格")
            bar_keys = set(map(tuple, bars[keys].astype(str).to_numpy()))
            factor_keys = set(map(tuple, adjustment[keys].astype(str).to_numpy()))
            if bar_keys != factor_keys:
                missing = sorted(bar_keys - factor_keys)[:10]
                extra = sorted(factor_keys - bar_keys)[:10]
                raise RuntimeError(
                    "stock_adj_factor 与 stock_bars 不是一对一完整集合；"
                    f"missing={missing}，extra={extra}"
                )
            factors = pd.to_numeric(adjustment["adj_factor"], errors="coerce")
            if not (factors.notna() & np.isfinite(factors) & factors.gt(0)).all():
                raise RuntimeError("stock_adj_factor 包含非 finite 或非正值")
            keep = adjustment[[*keys, "adj_factor"]]
            bars = bars.merge(keep, on=keys, how="inner", validate="one_to_one")
            bars["research_price"] = pd.to_numeric(
                bars["close"], errors="coerce",
            ) * pd.to_numeric(bars["adj_factor"], errors="coerce")
            daily_basic = self.lake.read_range(
                ArtifactKind.RAW, asset_class, Frequency.DAILY, "stock_daily_basic",
                load_start, load_end,
            )
        elif asset_class == AssetClass.ETF:
            bars = self.lake.read_range(
                ArtifactKind.RAW, asset_class, Frequency.DAILY, "etf_bars", load_start, load_end,
            )
            if "research_price" not in bars:
                bars["research_price"] = bars.get("close")
            daily_basic = pd.DataFrame()
        else:
            contract_bars = self.lake.read_range(
                ArtifactKind.RAW, asset_class, Frequency.DAILY, "future_bars", load_start, load_end,
            )
            mapping = self.lake.read_range(
                ArtifactKind.RAW, asset_class, Frequency.DAILY, "future_main_mapping",
                load_start, load_end,
            )
            bars = build_future_continuous(contract_bars, mapping)
            if bars.empty:
                raise RuntimeError("期货主力连续序列为空")
            for trade_date, group in bars.groupby(pd.to_datetime(bars["trade_date"]).dt.date):
                self.lake.write_partition(
                    ArtifactKind.RAW, asset_class, Frequency.DAILY, "future_continuous",
                    str(trade_date), group, run_id="future-continuous",
                )
            daily_basic = pd.DataFrame()
        if bars.empty:
            raise RuntimeError(f"{asset_class.value} 缺少 {load_start} 至 {load_end} 的研究行情")
        partitions = []
        for dataset_id in (
            ("stock_bars", "stock_adj_factor", "stock_daily_basic")
            if asset_class == AssetClass.STOCK else
            (("etf_bars",) if asset_class == AssetClass.ETF else
             ("future_bars", "future_main_mapping"))
        ):
            partitions.extend(self.lake.catalog.partitions(
                kind=ArtifactKind.RAW, asset_class=asset_class, frequency=Frequency.DAILY,
                dataset_id=dataset_id, start=load_start, end=load_end,
            ))
        lineage = sorted((item["partition_key"], item["content_sha256"]) for item in partitions)
        return {"bars": bars, "daily_basic": daily_basic}, {
            "partition_set": content_hash(lineage), "partition_count": str(len(lineage)),
        }

    def _write_risk(
        self,
        values: pd.DataFrame,
        specs: list[ResearchSpec],
        asset_class: AssetClass,
        run_id: str,
        input_hashes: dict[str, str],
    ) -> list[dict[str, Any]]:
        for spec in specs:
            self.lake.catalog.register_spec(spec)
        versions = {spec.id: spec.version for spec in specs}
        records = []
        value = values.copy()
        renamed: dict[str, str] = {}
        for spec in specs:
            renamed[spec.output] = spec.storage_column
            raw_column = f"{spec.output}_raw"
            if raw_column in value:
                renamed[raw_column] = f"{spec.storage_column}__raw"
        value = value.rename(columns=renamed)
        for trade_date, group in value.groupby(pd.to_datetime(value["trade_date"]).dt.date):
            records.append(self.lake.write_partition(
                ArtifactKind.RISK, asset_class, Frequency.DAILY, "QM_STYLE_V1",
                str(trade_date), group, spec_versions=versions,
                input_hashes=input_hashes, run_id=run_id,
            ))
        return records

    @staticmethod
    def _compute_legacy(spec: ResearchSpec, bars: pd.DataFrame) -> pd.DataFrame:
        from quantmaster.factors.engine import compute_factor
        from quantmaster.factors.library import BUILTIN_FACTORS

        factor = BUILTIN_FACTORS.get(spec.id)
        if factor is None:
            raise ValueError(f"精选因子 {spec.id} 继续由 Quant Lab/PIT 数据入口执行")
        panels = {}
        for column in ("open", "high", "low", "close", "volume", "amount", "turnover"):
            if column in bars:
                panels[column] = bars.pivot(
                    index="trade_date", columns="symbol", values=column,
                ).sort_index()
        values = compute_factor(factor, panels)
        return values.rename_axis(index="trade_date", columns="symbol").reset_index().melt(
            id_vars="trade_date", var_name="symbol", value_name=spec.output,
        )

    def _emit_diagnostics(self, plan: ExecutionPlan, run_id: str) -> list[dict[str, Any]]:
        label_refs = [ref for ref in plan.selected_specs if ref.kind == ArtifactKind.LABEL]
        factor_refs = [ref for ref in plan.selected_specs if ref.kind == ArtifactKind.FACTOR]
        summaries = []
        tables: dict[str, pd.DataFrame] = {}
        for factor_ref in factor_refs:
            labels_for_asset = [ref for ref in label_refs if ref.asset_class == factor_ref.asset_class]
            if not labels_for_asset:
                continue
            factor_panel = self.lake.artifact_panel(factor_ref, plan.start, plan.end)
            if factor_panel.empty:
                continue
            factors = factor_panel.rename_axis(
                index="trade_date", columns="symbol",
            ).reset_index().melt(
                id_vars="trade_date", var_name="symbol", value_name=factor_ref.id,
            )
            labels: pd.DataFrame | None = None
            for label_ref in labels_for_asset:
                panel = self.lake.artifact_panel(label_ref, plan.start, plan.end)
                current = panel.rename_axis(
                    index="trade_date", columns="symbol",
                ).reset_index().melt(
                    id_vars="trade_date", var_name="symbol", value_name=label_ref.id,
                )
                labels = current if labels is None else labels.merge(
                    current, on=["trade_date", "symbol"], how="outer", validate="one_to_one",
                )
            if labels is None:
                continue
            report = factor_diagnostics(factors, labels, factor_ref.id)
            summary = {"asset_class": factor_ref.asset_class.value, **report.pop("summary")}
            summaries.append(summary)
            prefix = f"{factor_ref.asset_class.value}_{factor_ref.id}"
            tables.update({f"{prefix}_{name}": table for name, table in report.items()})
        if summaries:
            manifest_path = self.lake.root / "runs" / run_id / "diagnostics.json"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            temp = manifest_path.with_suffix(".json.tmp")
            temp.write_text(strict_json_dumps(summaries, indent=2), encoding="utf-8")
            temp.replace(manifest_path)
            self.lake.write_run_files(
                run_id, {"run_id": run_id, "diagnostics": summaries},
                commit=False, **tables,
            )
        return summaries

    def publish_model_predictions(
        self,
        model_id: str,
        version: str,
        asset_class: AssetClass,
        values: pd.DataFrame,
        *,
        run_id: str,
        input_refs: Iterable[ArtifactRef] = (),
    ) -> list[dict[str, Any]]:
        spec = ResearchSpec(
            id=model_id, version=version, kind=ArtifactKind.MODEL,
            asset_classes=(asset_class,), name=model_id, provider_id="model_predictions",
            output="prediction", tags=("model", "prediction", "point-in-time"),
        )
        input_hashes = {"artifact_refs": content_hash([item.to_dict() for item in input_refs])}
        self.lake.catalog.register_spec(spec)
        records = []
        output = values.rename(columns={"value": "prediction"})
        for trade_date, group in output.groupby(pd.to_datetime(output["trade_date"]).dt.date):
            payload = group[["trade_date", "symbol", "prediction"]].rename(
                columns={"prediction": spec.storage_column}
            )
            records.append(self.lake.write_partition(
                ArtifactKind.MODEL, asset_class, Frequency.DAILY, model_id, str(trade_date),
                payload, spec_versions={model_id: version}, input_hashes=input_hashes,
                run_id=run_id,
            ))
        return records


def save_plan(plan: ExecutionPlan, path: str | Path) -> None:
    Path(path).write_text(strict_json_dumps(plan.to_dict(), indent=2), encoding="utf-8")


def load_plan(path: str | Path) -> ExecutionPlan:
    return ExecutionPlan.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))
