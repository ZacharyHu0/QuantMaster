"""Repair handler for research partitions, registered through the data seam."""

from __future__ import annotations

from typing import Any

from quantmaster.repair_access import quarantine_file
from quantmaster.research.contracts import (
    ArtifactKind,
    AssetClass,
    Frequency,
)
from quantmaster.research_access import (
    register_research_repair_handler,
    research_engine,
    research_lake,
)


def repair_research_partition(item: dict[str, Any]) -> dict[str, Any]:
    spec = item["spec"]
    lake = research_lake(spec["root"])
    metadata = dict(spec["metadata"])
    key = str(metadata["partition_key"])
    owner = f"repair:{item['id']}"
    if not lake.catalog.claim(key, owner):
        raise RuntimeError(f"研究分区正在由其他任务写入: {key}")
    try:
        target = lake.path_for_repair(metadata)
        quarantine = quarantine_file(
            target,
            category="research",
            target=key,
            reason=str(item["reason"]),
        )
        lake.catalog.delete_partition(key)
    finally:
        lake.catalog.release(key, owner)
    trade_date = str(metadata["trade_date"])
    kind = ArtifactKind(str(metadata["kind"]))
    asset = AssetClass(str(metadata["asset_class"]))
    frequency = Frequency(str(metadata["frequency"]))
    if frequency != Frequency.DAILY:
        raise RuntimeError("目前只允许自动重建日频研究分区")
    if (
        kind == ArtifactKind.RAW
        and asset == AssetClass.STOCK
        and str(metadata["dataset_id"]) == "stock_bars"
    ):
        lake.materialize_bar_store(None, trade_date, trade_date, asset_class=asset)
        rebuilt = lake.catalog.partition(
            kind, asset, frequency, str(metadata["dataset_id"]), trade_date,
        )
        if rebuilt is not None:
            lake.validate_partition(rebuilt, enqueue_repair=False)
            return {
                "partition_key": metadata["partition_key"],
                "quarantine": quarantine,
                "source": "barstore",
            }
    engine = research_engine(lake)
    spec_ids = list((metadata.get("spec_versions") or {}).keys())
    if kind in {ArtifactKind.FACTOR, ArtifactKind.LABEL, ArtifactKind.RISK, ArtifactKind.MODEL}:
        if not spec_ids:
            raise RuntimeError("研究分区缺少可执行规格血缘")
        plan = engine.plan(
            trade_date, trade_date, asset_classes=[asset], spec_ids=spec_ids,
            mode="historical",
        )
    else:
        plan = engine.plan(
            trade_date, trade_date, asset_classes=[asset],
            datasets=[str(metadata["dataset_id"])], mode="historical",
        )
    engine.execute(plan)
    rebuilt = lake.catalog.partition(
        kind, asset, frequency, str(metadata["dataset_id"]), trade_date,
    )
    if rebuilt is None:
        raise RuntimeError("研究任务结束后目标分区仍不存在")
    lake.validate_partition(rebuilt, enqueue_repair=False)
    return {"partition_key": metadata["partition_key"], "quarantine": quarantine}


register_research_repair_handler(repair_research_partition)
