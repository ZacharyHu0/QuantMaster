"""Quant Lab 应用服务：统一数据、发现、验证、训练和 AI 修正流程。"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import time
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.factors.base import ExpressionFactor
from quantmaster.horizons import SUPPORTED_HORIZONS
from quantmaster.lab.catalog import curated_catalog
from quantmaster.lab.dataset import (
    create_snapshot,
    dataset_repair_plan,
    load_csi800_membership,
    load_local_dataset,
)
from quantmaster.lab.errors import LabError
from quantmaster.lab.models import DataPolicy, FactorSpec
from quantmaster.lab.preflight import compact_preflight, require_runnable, run_preflight
from quantmaster.lab.store import LabStore
from quantmaster.lab.strategy import (
    atomic_horizon_gate,
    combine_scores,
    ensemble_weights,
    execute_daily_targets,
    holding_actions,
    moving_block_return_interval,
    strategy_sealed_gate,
    target_weights,
)
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.trading_sessions import market_date

logger = logging.getLogger(__name__)


def _slug(prefix: str, expression: str) -> str:
    from quantmaster.lab.models import content_hash

    return f"{prefix}_{content_hash(expression)[:10]}"


def _extend_panel_with_local_symbols(
    panel: dict[str, pd.DataFrame], symbols: list[str],
) -> tuple[dict[str, pd.DataFrame], list[str]]:
    """Append locally available holdings without contacting a data provider."""
    from quantmaster.data.storage import BarStore

    reference = set(str(symbol) for symbol in panel["close"].columns)
    requested = list(dict.fromkeys(
        str(symbol) for symbol in symbols if symbol and str(symbol) not in reference
    ))
    if not requested:
        return panel, []
    start = pd.Timestamp(panel["close"].index.min()).strftime("%Y-%m-%d")
    end = pd.Timestamp(panel["close"].index.max()).strftime("%Y-%m-%d")
    fields: list[str] = [
        field for field in ("open", "high", "low", "close", "volume", "amount")
        if field in panel
    ]
    batch = BarStore().read_many(
        requested, columns=fields, start=start, end=end,
        max_workers=min(8, max(1, len(requested))), enqueue_repair=False,
    )
    if not batch.frames:
        return panel, requested
    enriched = dict(panel)
    for field in fields:
        additions = {
            symbol: frame[field]
            for symbol, frame in batch.frames.items()
            if field in frame and not frame[field].dropna().empty
        }
        if additions:
            added = pd.DataFrame(additions).sort_index()
            enriched[field] = panel[field].join(added, how="outer").sort_index()
    return enriched, [symbol for symbol in requested if symbol not in batch.frames]


def _ledger_weight_context(
    ledger: Any, panel: dict[str, pd.DataFrame], as_of: pd.Timestamp,
) -> tuple[dict[str, float], dict[str, Any], set[str]]:
    """Value the real local ledger at the latest signal date using only local closes."""
    from quantmaster.portfolio import ledger_report

    positions = [position for position in ledger.positions() if position.shares > 1e-9]
    close = panel["close"].loc[:as_of]
    prices: dict[str, float] = {}
    for position in positions:
        if position.symbol not in close.columns:
            continue
        values = close[position.symbol].replace([np.inf, -np.inf], np.nan).dropna()
        if len(values) and float(values.iloc[-1]) > 0:
            prices[position.symbol] = float(values.iloc[-1])
    report = ledger_report(
        ledger,
        prices=prices,
        as_of=pd.Timestamp(as_of).strftime("%Y-%m-%d"),
    )
    total_assets = float(report.get("total_assets") or 0.0)
    market_value = float(report.get("market_value") or 0.0)
    denominator = total_assets if total_assets > 1e-9 else market_value
    current = {
        str(item["symbol"]): float(item["market_value"]) / denominator
        for item in report.get("positions") or []
        if float(item.get("shares") or 0.0) > 1e-9 and denominator > 1e-9
    }
    warnings = [str(item) for item in report.get("warnings") or []]
    trades = ledger.trades()
    empty_portfolio = not positions and not len(trades)
    reliable = bool(not warnings and (total_assets > 1e-9 or empty_portfolio))
    summary = {
        "source": "local_real_ledger",
        "ledger": Path(ledger.path).name,
        "valuation_date": pd.Timestamp(as_of).strftime("%Y-%m-%d"),
        "last_trade_date": str(trades["date"].max()) if len(trades) else "",
        "holding_count": len(current),
        "total_assets": total_assets,
        "cash": float(report.get("cash") or 0.0),
        "market_value": market_value,
        "cash_weight": (
            float(report.get("cash") or 0.0) / total_assets if total_assets > 1e-9 else 0.0
        ),
        "capital_recorded": total_assets > 1e-9,
        "missing_price": list(report.get("missing_price") or []),
        "warnings": warnings,
        "reliable": reliable,
    }
    return current, summary, set(prices)


class LabService:
    def __init__(self, store: LabStore | None = None, *, read_only: bool = False):
        self.store = store or LabStore(read_only=read_only)
        # Catalog seeding is a runtime-worker startup responsibility.  A
        # read-only HTTP service deliberately observes the last published
        # ledger as-is and never opens a write transaction just to draw a tab.
        if not self.store.read_only:
            self.store.sync_catalog(curated_catalog())

    def capabilities(self) -> dict[str, Any]:
        from quantmaster.lab.capabilities import read_published_capabilities

        return read_published_capabilities()

    def overview(self) -> dict[str, Any]:
        cfg = get_config().lab
        return {
            **self.store.overview(),
            "capabilities": self.capabilities(),
            "research": {
                "universe": cfg.universe,
                "start": cfg.start,
                "horizons": cfg.horizons,
                "daily_budget_hours": cfg.daily_budget_hours,
                "max_workers": cfg.max_workers,
                "window": [cfg.window_start, cfg.window_end],
                "weekly_days": cfg.weekly_days,
                "ai_python_mining_enabled": cfg.ai_python_mining_enabled,
                "allow_cloud_sample": cfg.allow_cloud_sample,
            },
            "recent_jobs": self.store.jobs(8, summary=True),
            "recent_experiments": self.store.list_experiments(6, summary=True),
            "recent_studies": self.store.studies(6, summary=True),
        }

    def dashboard(self) -> dict[str, Any]:
        """One compact first-paint payload; large results stay on detail routes."""
        cfg = get_config().lab
        snapshot_record = self.store.latest_snapshot(cfg.universe) or {}
        snapshot_payload = dict(snapshot_record.get("payload") or {})
        if self.store.read_only:
            # ``run_preflight`` inspects every local bar and persists a
            # convenience cache.  That belongs to an explicit command or the
            # runtime worker, not to first paint.  Reuse published evidence
            # when present and otherwise state the honest degraded condition.
            published_preflight = snapshot_payload.get("preflight")
            admission = (
                dict(published_preflight)
                if isinstance(published_preflight, dict)
                else {
                    "operation": "validate",
                    "runnable": bool(snapshot_payload.get("production_eligible")),
                    "state": (
                        "ready" if snapshot_payload.get("production_eligible")
                        else "degraded" if snapshot_record else "unavailable"
                    ),
                    "resource_class": "cpu",
                    "data_policy": cfg.data_policy,
                    "blockers": ([] if snapshot_record else [{
                        "code": "SNAPSHOT_UNAVAILABLE",
                        "message": "没有已发布的本地研究快照",
                        "action": "通过后台任务准备数据后重试",
                        "context": {},
                    }]),
                    "warnings": ([{
                        "code": "EVIDENCE_STALE",
                        "message": "当前展示的是已发布快照；完整预检将在后台更新",
                        "action": "如需正式操作，请执行显式预检或刷新任务",
                        "context": {},
                    }] if snapshot_record else []),
                    "dataset": {
                        key: snapshot_payload.get(key)
                        for key in (
                            "universe", "start", "end", "as_of", "state",
                            "symbol_count", "research_quality", "production_eligible",
                        )
                        if key in snapshot_payload
                    },
                    "compute": {},
                }
            )
        else:
            admission = self.preflight("validate", {
                "universe": cfg.universe, "start": cfg.start,
                "end": market_date().isoformat(),
            })
        snapshot = {
            "id": snapshot_record.get("id", ""),
            "created_at": snapshot_record.get("created_at", ""),
            **{
                key: snapshot_payload.get(key)
                for key in (
                    "snapshot_hash", "manifest_hash", "universe", "start", "end",
                    "as_of", "state", "symbol_count", "research_quality",
                    "production_eligible", "data_policy",
                )
            },
        }
        return {
            "summary": self.store.overview(),
            "readiness": self.capabilities(),
            "preflight": compact_preflight(admission, sample_limit=3),
            "snapshot": snapshot,
            "jobs": self.store.jobs(12, summary=True),
            "experiments": self.store.list_experiments(8, summary=True),
            "studies": self.store.studies(6, summary=True),
            "research": {
                "universe": cfg.universe, "start": cfg.start,
                "horizons": cfg.horizons, "data_policy": cfg.data_policy,
                "device": cfg.device, "daily_budget_hours": cfg.daily_budget_hours,
                "max_workers": cfg.max_workers,
                "window": [cfg.window_start, cfg.window_end],
                "weekly_days": cfg.weekly_days,
                "ai_python_mining_enabled": cfg.ai_python_mining_enabled,
            },
        }

    def doctor(self) -> dict[str, Any]:
        """Compact, actionable, network-free runtime diagnosis."""
        cfg = get_config()
        admission = self.preflight("validate", {
            "universe": cfg.lab.universe, "start": cfg.lab.start,
            "end": market_date().isoformat(),
        })
        capabilities = self.capabilities()
        models = capabilities["models"]
        dataset = admission.get("dataset") or {}
        overview = self.store.overview()
        job_statuses = overview.get("job_statuses") or {}
        checks = [
            {
                "name": "本地快照", "state": dataset.get("state", "missing"),
                "detail": (
                    f"{dataset.get('symbol_count', 0)} 标的 · as_of "
                    f"{dataset.get('as_of') or '未知'}"
                ),
                "action": (
                    (admission.get("blockers") or admission.get("warnings") or [{}])[0]
                    .get("action", "无需处理")
                ),
            },
            {
                "name": "PyTorch", "state": "ready" if models.get("torch") else "missing",
                "detail": str(models.get("torch_version") or "未安装"),
                "action": (
                    "安装 quantmaster[ml] 的 CUDA 版 PyTorch"
                    if not models.get("torch") else "无需处理"
                ),
            },
            {
                "name": "CUDA", "state": "ready" if (models.get("gpu") or {}).get("available") else "missing",
                "detail": str((models.get("gpu") or {}).get("name") or "不可用"),
                "action": (
                    "验证 torch.cuda.is_available() 与 NVIDIA 驱动"
                    if not (models.get("gpu") or {}).get("available") else "无需处理"
                ),
            },
            {
                "name": "Optuna", "state": "ready" if capabilities.get("optuna") else "missing",
                "detail": "多目标优化可用" if capabilities.get("optuna") else "未安装",
                "action": "安装 quantmaster[ml]" if not capabilities.get("optuna") else "无需处理",
            },
            {
                "name": "LLM", "state": "ready" if capabilities["llm"]["configured"] else "optional",
                "detail": capabilities["llm"].get("provider") or "未配置",
                "action": "仅 AI 发现需要配置" if not capabilities["llm"]["configured"] else "无需处理",
            },
            {
                "name": "Tushare", "state": "ready" if capabilities["tushare"]["configured"] else "optional",
                "detail": "远端更新可用" if capabilities["tushare"]["configured"] else "仅本地研究可用",
                "action": (
                    "仅显式更新数据时需要配置"
                    if not capabilities["tushare"]["configured"] else "无需处理"
                ),
            },
            {
                "name": "Worker", "state": "busy" if job_statuses.get("running") else "idle",
                "detail": (
                    f"运行 {job_statuses.get('running', 0)} · "
                    f"排队 {job_statuses.get('queued', 0)}"
                ),
                "action": "通过 qm serve 或 qm lab worker 执行队列",
            },
        ]
        admission = compact_preflight(admission, sample_limit=3)
        dataset = admission.get("dataset") or {}
        return {
            "state": admission["state"], "runnable": admission["runnable"],
            "checks": checks, "blockers": admission["blockers"],
            "warnings": admission["warnings"], "resource": admission["resource_class"],
            "compute": admission["compute"], "dataset": dataset,
        }

    def benchmark_local(
        self, *, universe: str, start: str, end: str, runs: int = 2,
    ) -> dict[str, Any]:
        """Measure the offline snapshot path; this function cannot invoke providers."""
        from quantmaster.lab.dataset import clear_local_dataset_caches

        clear_local_dataset_caches()
        timings = []
        snapshots = []
        for _run in range(max(2, min(int(runs), 5))):
            started = time.perf_counter()
            panel, _membership, snapshot = load_local_dataset(
                universe, start, end, policy=DataPolicy.LOCAL_ONLY.value,
            )
            timings.append(time.perf_counter() - started)
            snapshots.append(snapshot)
        return {
            "universe": universe, "start": start, "end": end,
            "symbols": len(panel["close"].columns),
            "dates": len(panel["close"].index),
            "cold_seconds": round(timings[0], 6),
            "cache_seconds": round(min(timings[1:]), 6),
            "runs": [round(value, 6) for value in timings],
            "network_calls": 0,
            "snapshot_hash": snapshots[-1]["snapshot_hash"],
            "as_of": snapshots[-1].get("as_of", ""),
            "state": snapshots[-1].get("state", ""),
            "targets": {"cold_seconds": 8.0, "cache_seconds": 1.0},
            "passed": timings[0] <= 8.0 and min(timings[1:]) <= 1.0,
        }

    def preflight(self, operation: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        values = dict(params or {})
        if operation == "optimize" and values.get("study_id") and not values.get("models"):
            study = self.store.study(str(values["study_id"]))
            if study:
                values = {**study.get("config", {}), **values}
        report = run_preflight(operation, values)
        universe = str(values.get("universe") or get_config().lab.universe)
        start = str(values.get("start") or get_config().lab.start)
        end = str(values.get("end") or market_date().isoformat())
        repair = dataset_repair_plan(universe, start, end)
        if repair["repair_symbol_count"] or repair["membership_missing"]:
            report["coverage"] = repair
        if operation == "prepare_data":
            estimate = dict(report.get("estimate") or {})
            bars_root = get_config().data_root / "bars"
            existing = sorted(
                (int(item.get("existing_bytes") or 0) for item in repair.get("gaps") or []),
                reverse=True,
            )
            workers = min(4, max(1, int(get_config().lab.max_workers)))
            rewritten_bytes = sum(existing[:workers])
            new_bytes = int(repair.get("missing_session_count") or 0) * 64
            sqlite_headroom = 8 * 1024 * 1024
            peak_bytes = rewritten_bytes + new_bytes + sqlite_headroom
            probe = bars_root if bars_root.exists() else get_config().data_root
            free_bytes = shutil.disk_usage(probe).free
            estimate.update({
                "disk_bytes": peak_bytes,
                "disk_free_bytes": free_bytes,
                "repair_output_bytes": new_bytes,
                "repair_temporary_bytes": rewritten_bytes,
                "repair_sqlite_headroom_bytes": sqlite_headroom,
                "space_purpose": "bars_atomic_rewrite",
            })
            report["estimate"] = estimate
            if peak_bytes > free_bytes and not any(
                item.get("code") == "DISK_SPACE_INSUFFICIENT"
                for item in report.get("blockers") or []
            ):
                report.setdefault("blockers", []).append({
                    "code": "DISK_SPACE_INSUFFICIENT",
                    "message": "数据补齐所需磁盘空间不足",
                    "action": "释放数据目录所在卷空间，或缩小补齐范围后重试",
                    "context": {"required_bytes": peak_bytes, "free_bytes": free_bytes},
                })
                report["runnable"] = False
                report["state"] = "blocked"
        if operation != "validate" or not values.get("version_id"):
            return report
        version = self.store.version(str(values["version_id"]))
        if version is None:
            return report
        spec = FactorSpec.from_dict(version["spec"])
        expression = spec.expression or spec.slug
        if "news_sentiment" not in spec.required_features and expression != "news_sentiment":
            return report
        from quantmaster.ai.sentiment import news_sentiment_readiness

        readiness_report = news_sentiment_readiness(start, end)
        report["features"] = {"news_sentiment": readiness_report}
        if readiness_report["ready"]:
            return report
        available = (
            f"{readiness_report['available_start']} 至 {readiness_report['available_end']}"
            if readiness_report["available_start"] else "无可用标注"
        )
        report.setdefault("blockers", []).append({
            "code": "FEATURE_HISTORY_INSUFFICIENT",
            "message": (
                f"news_sentiment 本地标注历史不足：{available}，"
                f"不能可信验证 {start} 至 {end} 的研究区间"
            ),
            "action": (
                "继续积累本地新闻标注；达到 756 日开发期、30 日隔离期和 "
                "252 日密封期后再验证。量价因子研究不受影响"
            ),
            "context": readiness_report,
        })
        report["runnable"] = False
        report["state"] = "blocked"
        return report

    def _stage_model_publication(
        self,
        *,
        version_id: str,
        experiment_id: str,
        artifact_dir: Path,
        slug: str,
        prediction_rows: pd.DataFrame,
    ) -> dict[str, Any]:
        """Durably stage prediction rows, then record their immutable outbox request."""
        from quantmaster.lab.ml import artifact_sha256

        root = Path(get_config().data_root).resolve()
        artifact_dir.mkdir(parents=True, exist_ok=True)
        target = artifact_dir / "research_predictions.parquet"
        staged = artifact_dir / ".research_predictions.parquet.tmp"
        prediction_rows.to_parquet(staged, index=False)
        with staged.open("rb+") as stream:
            os.fsync(stream.fileno())
        os.replace(staged, target)
        relative = target.resolve().relative_to(root).as_posix()
        payload = {
            "schema_version": 1,
            "kind": "model_predictions",
            "slug": slug,
            "version": "1.0.0",
            "asset_class": "stock",
            "run_id": experiment_id,
            "path": relative,
            "content_sha256": artifact_sha256(target),
            "rows": len(prediction_rows),
        }
        return self.store.enqueue_publication(
            "model_predictions", version_id, experiment_id, payload,
        )

    def publish_model_outbox(self, publication_id: str) -> dict[str, Any]:
        """Idempotently publish one staged model output; failure remains retryable."""
        current = self.store.publication(publication_id)
        if current is None:
            raise KeyError("模型发布任务不存在")
        if current["status"] == "published":
            return current
        owner = f"lab-publish:{os.getpid()}:{uuid.uuid4().hex[:12]}"
        claimed = self.store.claim_publication(publication_id, owner)
        if claimed is None:
            return self.store.publication(publication_id) or current
        try:
            from quantmaster.lab.ml import artifact_sha256
            from quantmaster.research.contracts import AssetClass
            from quantmaster.research.engine import ResearchEngine

            payload = claimed["payload"]
            root = Path(get_config().data_root).resolve()
            path = (root / str(payload["path"])).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                raise ValueError("模型预测暂存文件缺失或路径越界")
            if artifact_sha256(path) != str(payload["content_sha256"]):
                raise ValueError("模型预测暂存文件哈希不匹配")
            rows = pd.read_parquet(path)
            required = {"trade_date", "symbol", "value"}
            if not required.issubset(rows):
                raise ValueError(f"模型预测缺少字段: {sorted(required - set(rows))}")
            records = ResearchEngine().publish_model_predictions(
                str(payload["slug"]), str(payload["version"]),
                AssetClass(str(payload["asset_class"])), rows,
                run_id=str(payload["run_id"]),
            )
            result = {
                "ref": (
                    f"artifact:model:{payload['asset_class']}:"
                    f"{payload['slug']}@{payload['version']}"
                ),
                "partitions": len(records),
                "content_sha256": payload["content_sha256"],
            }
            if not self.store.complete_publication(publication_id, owner, result):
                raise RuntimeError("模型发布租约在提交前失效")
        except Exception as exc:
            logger.exception("Lab model publication failed publication=%s", publication_id)
            self.store.fail_publication(
                publication_id, owner, f"{type(exc).__name__}: {exc}",
            )
        return self.store.publication(publication_id) or claimed

    def recover_publications(self, limit: int = 20) -> dict[str, int]:
        """Retry due/lease-expired outbox work without affecting training status."""
        attempted = published = 0
        for item in self.store.pending_publications(limit):
            attempted += 1
            value = self.publish_model_outbox(str(item["id"]))
            published += int(value.get("status") == "published")
        return {"attempted": attempted, "published": published}

    def create_expression(
        self, *, name: str, expression: str, description: str = "",
        category: str = "人工研究", rationale: str = "", actor: str = "web",
        parent_id: str = "",
    ) -> dict:
        ExpressionFactor(expression)
        slug = _slug("manual", expression)
        spec = FactorSpec(
            slug=slug,
            name=name.strip() or slug,
            expression=expression.strip(),
            description=description.strip(),
            category=category.strip() or "人工研究",
            rationale=rationale.strip(),
            required_features=tuple(sorted(_expression_fields(expression))),
            horizons=tuple(get_config().lab.horizons),
            tags=("manual",),
        )
        _factor, version, _created = self.store.create_factor(
            spec, actor=actor, parent_id=parent_id)
        return self.store.version(version["id"]) or version

    def enqueue(self, kind: str, params: dict[str, Any]) -> dict:
        allowed = {
            "prepare_data", "validate", "discover_genetic", "discover_llm",
            "optimize", "bias_audit", "discover_python", "research_cycle", "shadow_score",
        }
        if kind not in allowed:
            raise ValueError(f"未知研究任务: {kind}")
        clean = dict(params)
        if kind == "prepare_data":
            clean.setdefault("data_policy", DataPolicy.REFRESH_MISSING.value)
        admission = self.preflight(kind, clean)
        require_runnable(admission)
        if kind == "discover_python":
            if not get_config().lab.ai_python_mining_enabled:
                raise ValueError("受限 Python AutoMiner 尚未在设置中心启用")
            clean["rounds"] = min(3, max(1, int(clean.get("rounds", 3))))
            clean["candidate_limit"] = min(24, max(1, int(clean.get("candidate_limit", 24))))
            clean["finalists"] = min(3, max(1, int(clean.get("finalists", 3))))
            run = self.store.create_mining_run(clean)
            clean["run_id"] = run["id"]
            job = self.store.enqueue(kind, clean, preflight=admission)
            self.store.update_mining_run(run["id"], job_id=job["id"])
            return job
        return self.store.enqueue(kind, clean, preflight=admission)

    def retry_job(self, job_id: str) -> dict:
        source = self.store.job(job_id)
        if source is None:
            raise KeyError("任务不存在")
        if source["status"] not in {
            "paused", "completed", "completed_with_warnings", "failed", "cancelled",
        }:
            raise ValueError("只能按相同参数重新运行已结束的任务")
        params = dict(source.get("params") or {})
        params.pop("_scheduled", None)
        created = self.enqueue(str(source["kind"]), params)
        self.store.append_event(created["id"], {
            "type": "retry_of", "source_job_id": job_id,
            "phase": "预检通过，按历史参数重新运行",
        })
        self.store.append_event(job_id, {
            "type": "retried_as", "job_id": created["id"],
            "phase": "已创建重新运行任务",
        })
        return self.store.job(created["id"]) or created

    def preview_python_mining(self, *, start: str, end: str, horizon: int = 3) -> dict:
        from quantmaster.lab.research import sealed_three_way_split

        dates = pd.bdate_range(start, end)
        return {
            "split": sealed_three_way_split(dates, purge_gap=max(7, int(horizon))),
            "limits": {"llm_calls": 3, "candidates": 24, "finalists": 3},
            "test_policy": "sealed_until_finalist_order_frozen",
            "data_policy": "feature_registry_metadata_only; no raw sample leaves this process",
        }

    def create_study(self, payload: dict[str, Any]) -> dict:
        """校验配置、登记 Study，再把长任务放入统一可恢复队列。"""

        from quantmaster.lab.research import OptimizationSpec

        config = dict(payload)
        scheduled = bool(config.pop("_scheduled", False))
        config["end"] = config.get("end") or market_date().isoformat()
        spec = OptimizationSpec.from_dict(config)
        require_runnable(self.preflight("optimize", spec.to_dict()))
        study = self.store.create_study(spec.to_dict())
        job = self.enqueue("optimize", {
            "study_id": study["id"], **({"_scheduled": True} if scheduled else {}),
        })
        return self.store.update_study(study["id"], job_id=job["id"], status="queued")

    def resume_study(self, study_id: str) -> dict:
        study = self.store.study(study_id)
        if study is None:
            raise KeyError("优化 Study 不存在")
        if study["status"] not in {"paused", "failed", "interrupted"}:
            raise ValueError("只有暂停、失败或中断的 Study 可以恢复")
        job = self.enqueue("optimize", {"study_id": study_id, "resume": True})
        return self.store.update_study(study_id, job_id=job["id"], status="queued")

    def _context(
        self, universe: str, start: str, end: str,
        progress=None, data_policy: str | None = None,
    ) -> tuple[dict[str, pd.DataFrame], pd.DataFrame | None, dict]:
        from quantmaster.data import refresh_panel
        from quantmaster.data.universe import load_universe

        policy = DataPolicy(data_policy or get_config().lab.data_policy)
        if policy != DataPolicy.REFRESH_MISSING or not get_config().data.tushare_token:
            panel, membership, snapshot = load_local_dataset(
                universe, start, end, policy=policy.value, progress=progress,
            )
            stored = self.store.save_snapshot(snapshot)
            return panel, membership, stored

        membership = None
        if universe.lower() == "csi800":
            if progress:
                progress(5, "加载 point-in-time 中证800成分")
            membership = load_csi800_membership(start, end)
            symbols = [symbol for symbol in membership if membership[symbol].any()]
        else:
            symbols = load_universe(universe)
        if progress:
            progress(15, f"加载 {len(symbols)} 只标的日线")

        def on_symbol(done: int, total: int, symbol: str, success: bool) -> None:
            if progress:
                progress(
                    15 + int(35 * done / max(1, total)),
                    f"行情 {done}/{total} · {symbol}{'' if success else ' 跳过'}",
                )

        market_envelope = refresh_panel(symbols, start, end, progress=on_symbol)
        panel = market_envelope.require_data()
        snapshot = create_snapshot(
            universe,
            start,
            end,
            panel=panel,
            membership=membership,
            market_data_quality=market_envelope.quality.to_dict(),
        ).to_dict()
        stored = self.store.save_snapshot(snapshot)
        if progress:
            progress(52, "数据快照已冻结")
        return panel, membership, stored

    def prepare_data(
        self,
        *,
        universe: str,
        start: str,
        end: str,
        provider: str = "",
        include_warmup: bool = True,
        data_policy: str = DataPolicy.REFRESH_MISSING.value,
        progress=None,
        cancelled=None,
    ) -> dict[str, Any]:
        """Explicitly repair planned local gaps through one user-selected provider."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from quantmaster.data import RefreshMode, refresh_history
        from quantmaster.lab.dataset import clear_local_dataset_caches

        del data_policy
        before = dataset_repair_plan(universe, start, end)
        stages: dict[str, dict[str, Any]] = {
            "universe": {"status": "completed", "partitions": 1},
            "membership": {
                "status": "pending" if before["membership_missing"] else "completed",
                "partitions": 0 if before["membership_missing"] else 1,
            },
            "bars": {"status": "running", "completed": 0, "failed": 0},
            "dataset": {"status": "pending"},
            "snapshot": {"status": "pending"},
        }

        def checkpoint(
            value: int, stage: str, status: str, detail: str = "", **metadata: Any,
        ) -> None:
            if progress:
                try:
                    progress(
                        value, f"数据准备 · {stage}", detail,
                        event_type="partition_checkpoint",
                        metadata={"stage": stage, "status": status, **metadata},
                    )
                except TypeError:
                    # Direct service callers historically supplied a simple
                    # three-argument callback.  The worker callback accepts
                    # the durable event metadata above.
                    progress(value, f"数据准备 · {stage}", detail)

        checkpoint(
            1, "universe", "completed",
            f"已规划 {before['repair_symbol_count']} 个行情分区",
            partition="universe", persisted=1,
        )
        selected = str(provider or "").strip().lower()
        if not selected:
            stockdb = next(item for item in before["providers"] if item["id"] == "free-stockdb")
            selected = (
                "free-stockdb"
                if stockdb["available"] and not before["membership_missing"] else "tushare"
            )
        if selected not in {"free-stockdb", "tushare"}:
            raise LabError(
                "INVALID_REQUEST", f"不支持的数据补齐来源: {selected}",
                action="选择本机 StockDB 或 Tushare",
            )
        if before["membership_missing"]:
            if selected != "tushare":
                raise LabError(
                    "DATASET_MISSING", "本机 StockDB 不能补齐 CSI800 点时成分",
                    action="改用 Tushare，或先导入 PIT 成分缓存",
                )
            if progress:
                progress(3, "通过 Tushare 补齐 CSI800 点时成分")
            load_csi800_membership(start, end)
            clear_local_dataset_caches()
            before = dataset_repair_plan(universe, start, end)
            stages["membership"] = {"status": "completed", "partitions": 1}
            checkpoint(
                4, "membership", "completed", "CSI800 点时成分已持久化",
                partition="csi800_membership", persisted=1,
            )

        targets: list[dict[str, Any]] = []
        for item in before["gaps"]:
            segments = [
                segment for segment in item.get("segments") or []
                if include_warmup or segment.get("kind") == "critical"
            ]
            if not segments:
                continue
            targets.append({
                **item,
                "repair_start": min(str(segment["start"]) for segment in segments),
                "repair_end": max(str(segment["end"]) for segment in segments),
            })
        failures: dict[str, str] = {}
        degraded: dict[str, dict[str, Any]] = {}
        persisted: list[str] = []
        completed = 0

        def repair(item: dict[str, Any]) -> tuple[str, dict[str, Any]]:
            if cancelled and cancelled():
                raise InterruptedError("数据补齐已取消")
            market_envelope = refresh_history(
                str(item["symbol"]), str(item["repair_start"]), str(item["repair_end"]),
                mode=RefreshMode.INCREMENTAL, work_class="interactive",
                source_name=selected,
            )
            market_envelope.require_data()
            return str(item["symbol"]), market_envelope.quality.to_dict()

        workers = min(4, max(1, int(get_config().lab.max_workers)), max(1, len(targets)))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="lab-data-repair") as pool:
            futures = {pool.submit(repair, item): item for item in targets}
            for future in as_completed(futures):
                item = futures[future]
                try:
                    repaired_symbol, quality = future.result()
                    persisted.append(repaired_symbol)
                    if quality.get("status") != "verified":
                        degraded[repaired_symbol] = quality
                    checkpoint(
                        5 + int(82 * (completed + 1) / max(1, len(targets))),
                        "bars", "completed", f"{repaired_symbol} 已持久化",
                        partition=repaired_symbol, persisted=len(persisted),
                        total=len(targets), quality_status=quality.get("status", ""),
                    )
                except InterruptedError:
                    raise
                except (OSError, RuntimeError, TypeError, ValueError) as exc:
                    failures[str(item["symbol"])] = str(exc)[:300]
                    checkpoint(
                        5 + int(82 * (completed + 1) / max(1, len(targets))),
                        "bars", "failed", str(exc)[:300],
                        partition=str(item["symbol"]), persisted=len(persisted),
                        total=len(targets), error_type=type(exc).__name__,
                    )
                completed += 1
                if progress:
                    progress(
                        5 + int(82 * completed / max(1, len(targets))),
                        f"{selected} 补齐 {completed}/{len(targets)} · {item['symbol']}",
                    )
        clear_local_dataset_caches()
        stages["bars"] = {
            "status": "completed_with_warnings" if failures else "completed",
            "completed": len(persisted), "failed": len(failures), "total": len(targets),
        }
        after = dataset_repair_plan(universe, start, end)
        count_key = "repair_symbol_count" if include_warmup else "critical_repair_symbol_count"
        resolved = max(0, int(before[count_key]) - int(after[count_key]))
        snapshot: dict[str, Any] = {}
        if after["research_eligible"]:
            stages["dataset"] = {"status": "running"}
            _panel, _membership, value = load_local_dataset(
                universe, start, end, policy=DataPolicy.PREFER_LOCAL.value,
            )
            stages["dataset"] = {"status": "completed"}
            checkpoint(91, "dataset", "completed", "本地研究数据集已物化", persisted=1)
            snapshot = self.store.save_snapshot(value)
            stages["snapshot"] = {"status": "completed", "partitions": 1}
            checkpoint(95, "snapshot", "completed", "冻结快照已登记", persisted=1)
        else:
            stages["dataset"] = {"status": "blocked", "remaining": after[count_key]}
            stages["snapshot"] = {"status": "not_published"}
        if progress:
            remaining = int(after[count_key])
            progress(96, f"补齐完成 · 修复 {resolved} 只，当前范围剩余 {remaining} 只")
        if targets and not resolved and failures:
            raise LabError(
                "EXTERNAL_SERVICE_UNAVAILABLE",
                f"{selected} 未能补齐任何缺口",
                action="检查数据源配置或服务状态后重试，也可切换另一数据源",
                retryable=True,
                context={"provider": selected, "failed_symbols": len(failures)},
                status_code=503,
            )
        warnings = [
            {
                "code": "DATA_PARTITION_INCOMPLETE",
                "message": f"{len(failures)} 个行情分区未完成；已持久化结果仍然保留",
                "action": "修复数据源或存储问题后按原参数重试",
                "context": {"failed_partitions": sorted(failures)[:20]},
            }
        ] if failures else []
        return {
            "provider": selected,
            "requested_symbols": len(targets),
            "resolved_symbols": resolved,
            "remaining_symbols": after["repair_symbol_count"],
            "remaining_critical_symbols": after["critical_repair_symbol_count"],
            "include_warmup": bool(include_warmup),
            "failures": failures,
            "degraded": degraded,
            "warnings": warnings,
            "stages": stages,
            "partitions": {
                "total": len(targets), "persisted": len(persisted),
                "failed": len(failures), "remaining": int(after[count_key]),
                "persisted_items": sorted(persisted),
                "failed_items": sorted(failures),
            },
            "safe_retry_point": (
                "snapshot" if snapshot else "dataset" if after["research_eligible"] else "bars"
            ),
            "analysis_ready_symbols": max(0, len(targets) - len(failures)),
            "formal_eligible": bool(after.get("research_eligible")),
            "before": before,
            "after": after,
            "snapshot": snapshot,
        }

    def validate_version(
        self, version_id: str, *, universe: str, start: str, end: str,
        progress=None, cancelled=None,
    ) -> dict:
        from quantmaster.lab.validation import validate_factor_values

        version = self.store.version(version_id)
        if version is None:
            raise KeyError("因子版本不存在")
        spec = FactorSpec.from_dict(version["spec"])
        expression = spec.expression or spec.slug
        if spec.kind == "expression" and (
            "news_sentiment" in spec.required_features or expression == "news_sentiment"
        ):
            from quantmaster.ai.sentiment import news_sentiment_readiness

            feature_readiness = news_sentiment_readiness(start, end)
            if not feature_readiness["ready"]:
                available = (
                    f"{feature_readiness['available_start']} 至 "
                    f"{feature_readiness['available_end']}"
                    if feature_readiness["available_start"] else "无可用标注"
                )
                raise LabError(
                    "FEATURE_HISTORY_INSUFFICIENT",
                    f"news_sentiment 本地标注历史不足：{available}",
                    action=(
                        "继续积累本地新闻标注；达到完整开发期、隔离期和密封期后重试"
                    ),
                    retryable=True,
                    context=feature_readiness,
                    status_code=409,
                )
        python_features: dict[str, pd.DataFrame] | None = None
        parameter_variants: dict[str, pd.DataFrame] = {}
        python_manifest: dict | None = None
        if spec.kind == "python":
            python_features, _catalog, snapshot, _bundle_hash = self._python_mining_context(
                universe, start, end, progress,
            )
            panel = {"close": python_features["close"]}
            membership = (
                python_features.get("membership", pd.DataFrame()).astype(bool)
                if "membership" in python_features else None
            )
        else:
            panel, membership, snapshot = self._context(universe, start, end, progress)
        if cancelled and cancelled():
            raise InterruptedError("因子验证已取消")
        if spec.kind == "learned":
            from quantmaster.lab.ml import predict_panel

            if progress:
                progress(58, "加载学习模型并校验工件完整性")
            values = predict_panel(panel, spec.model)
        elif spec.kind == "python":
            from quantmaster.factors.python_artifact import (
                RestrictedPythonRunner,
                execute_python_factor_artifact,
            )

            if progress:
                progress(58, "校验受限 Python 工件与内容哈希")
            values = execute_python_factor_artifact(
                get_config().data_root, spec.artifact, python_features or {},
            )
            artifact_root = Path(get_config().data_root).resolve()
            manifest_path = (artifact_root / str(spec.artifact.get("manifest") or "")).resolve()
            source_path = (artifact_root / str(spec.artifact.get("source") or "")).resolve()
            if artifact_root not in manifest_path.parents or artifact_root not in source_path.parents:
                raise ValueError("Python 因子工件路径越界")
            python_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            source = source_path.read_text(encoding="utf-8")
            selected_params = spec.artifact.get("parameters") or {}
            plateau = python_manifest.get("audit", {}).get("parameter_plateau", {})
            runner = RestrictedPythonRunner()
            for item in plateau.get("variants", [])[:8]:
                params = item.get("params") or {}
                if params == selected_params:
                    continue
                try:
                    label = json.dumps(params, ensure_ascii=False, sort_keys=True)
                    parameter_variants[label] = runner.execute(
                        source, python_features or {}, params,
                    )
                except Exception:
                    continue
        elif spec.kind == "expression":
            from quantmaster.factors import compute_factor
            from quantmaster.factors.fundamental import resolve_factor
            from quantmaster.lab.robustness import expression_parameter_variants

            symbols = list(panel["close"].columns)
            expression = spec.expression or spec.slug
            if progress:
                progress(58, "计算因子并执行统一标准化")

            def fundamental_progress(
                done: int, total: int, symbol: str, success: bool,
            ) -> None:
                if progress:
                    progress(
                        58 + int(6 * done / max(1, total)),
                        f"基本面 {done}/{total} · {symbol}{'' if success else ' 跳过'}",
                    )

            factor = resolve_factor(
                expression,
                symbols,
                start,
                end,
                progress=fundamental_progress,
                cancelled=cancelled,
            )
            if cancelled and cancelled():
                raise InterruptedError("因子验证已取消")
            values = compute_factor(factor, panel)
            try:
                expressions = expression_parameter_variants(expression)
            except Exception:
                expressions = {}
            for label, candidate_expression in expressions.items():
                if cancelled and cancelled():
                    raise InterruptedError("因子验证已取消")
                try:
                    candidate = resolve_factor(candidate_expression, symbols, start, end)
                    parameter_variants[label] = compute_factor(candidate, panel)
                except Exception:
                    continue
        else:
            raise ValueError(f"{spec.kind} 类型尚未提供可复验的运行时")
        if progress:
            progress(65, "运行 purged walk-forward 与多重检验")
        if cancelled and cancelled():
            raise InterruptedError("因子验证已取消")
        configured_horizons = tuple(get_config().lab.horizons)
        validation_horizons = tuple(
            item for item in spec.horizons if item in configured_horizons
        ) or configured_horizons
        report = validate_factor_values(
            values,
            panel["close"],
            name=spec.name,
            horizons=validation_horizons,
            membership=membership,
            research_quality=snapshot["payload"]["research_quality"],
            panel=python_features if spec.kind == "python" else panel,
            parameter_variants=parameter_variants,
            open_prices=panel.get("open"),
        )
        if spec.kind == "python":
            artifact_root = Path(get_config().data_root).resolve()
            manifest_path = (artifact_root / str(spec.artifact.get("manifest") or "")).resolve()
            if artifact_root not in manifest_path.parents:
                raise ValueError("Python 因子清单路径越界")
            manifest = python_manifest or json.loads(manifest_path.read_text(encoding="utf-8"))
            blockers = []
            if manifest.get("non_pit_features"):
                blockers.append(
                    "使用非 PIT 特征: " + ", ".join(manifest["non_pit_features"])
                )
            if manifest.get("runtime_incompatible_features"):
                blockers.append(
                    "Champion 运行时不可复现特征: "
                    + ", ".join(manifest["runtime_incompatible_features"])
                )
            report["gates"]["hard_failures"].extend(blockers)
            report["gates"]["passed"] = not (
                report["gates"]["hard_failures"] or report["gates"]["soft_failures"]
            )
            report["gates"]["override_allowed"] = not report["gates"]["hard_failures"]
            report["gates"]["bias_audit_required"] = True
        report["dataset_snapshot"] = snapshot["snapshot_hash"]
        updated = self.store.save_validation(version_id, snapshot["snapshot_hash"], report)
        if progress:
            progress(96, "验证证据已写入研究账本")
        return {"version": updated, "report": report, "snapshot": snapshot["payload"]}

    def discover_genetic(
        self, *, universe: str, start: str, end: str, population: int = 60,
        generations: int = 8, top_n: int = 10, horizon: int = 3, progress=None,
    ) -> dict:
        from quantmaster.factors.mining import GeneticMiner

        panel, _membership, snapshot = self._context(universe, start, end, progress)
        if progress:
            progress(58, f"遗传搜索 · {population} × {generations}")
        miner = GeneticMiner(population=population, generations=generations)
        mined = miner.mine(panel, top_n=top_n, periods=horizon, progress=False)
        versions = []
        for rank, item in enumerate(mined, start=1):
            spec = FactorSpec(
                slug=_slug("gp", item.expression),
                name=f"GP 候选 {rank}",
                expression=item.expression,
                description="遗传规划自动生成，待统一验证与人工审批。",
                category="AI 发现",
                rationale=f"初筛 fitness={item.fitness:.4f}, IC={item.ic_mean:.4f}",
                horizons=tuple(get_config().lab.horizons),
                tags=("genetic", "discovered"),
            )
            _factor, version, _created = self.store.create_factor(
                spec, source="genetic", actor="worker")
            versions.append(self.store.version(version["id"]) or version)
        if progress:
            progress(96, f"保存 {len(versions)} 个候选")
        return {
            "method": "genetic", "candidates": versions,
            "raw": [asdict(item) for item in mined], "snapshot": snapshot["snapshot_hash"],
        }

    def discover_llm(
        self, *, universe: str, start: str, end: str, count: int = 8,
        rounds: int = 2, horizon: int = 3, progress=None, cancelled=None,
    ) -> dict:
        from quantmaster.factors.mining import LLMFactorMiner

        panel, _membership, snapshot = self._context(universe, start, end, progress)
        rounds = max(1, int(rounds))

        def relay(event: dict[str, Any]) -> None:
            if not progress:
                return
            event_type = str(event.get("type") or "progress")
            round_number = max(1, int(event.get("round") or 1))
            total_rounds = max(1, int(event.get("rounds") or rounds))
            span = 36.0 / total_rounds
            start_value = 58.0 + span * (round_number - 1)
            value = start_value
            phase, detail = "AI 因子发现", ""
            if event_type == "llm_attempt_started":
                phase = (
                    f"AI 第 {round_number}/{total_rounds} 轮 · "
                    f"尝试 {event['attempt']}/{event['max_attempts']}"
                )
                provider = str(event.get("provider") or "模型服务")
                model = str(event.get("model") or "当前模型")
                detail = (
                    f"等待 {provider} · {model}；本次最长 "
                    f"{event['timeout_seconds']} 秒"
                )
            elif event_type == "llm_response_received":
                value += span * 0.32
                phase = f"AI 第 {round_number}/{total_rounds} 轮已响应"
                detail = f"收到 {event.get('candidate_count', 0)} 个候选，开始本地校验"
            elif event_type == "llm_candidate_checked":
                total = max(1, int(event.get("candidate_count") or 1))
                done = max(0, int(event.get("candidate") or 0))
                value += span * (0.32 + 0.58 * done / total)
                phase = f"本地校验第 {round_number}/{total_rounds} 轮候选"
                detail = f"已校验 {done}/{total} 个安全 DSL 表达式"
            elif event_type == "llm_round_completed":
                value += span
                phase = f"AI 第 {round_number}/{total_rounds} 轮完成"
                detail = (
                    f"安全表达式 {event.get('dsl_valid', 0)} 个 · "
                    f"初筛达标 {event.get('threshold_passed', 0)} 个"
                )
            elif event_type == "llm_attempt_failed":
                phase = f"AI 第 {round_number}/{total_rounds} 轮请求未完成"
                detail = str(event.get("message") or "模型请求失败")
            elif event_type == "llm_retry_scheduled":
                phase = f"AI 第 {round_number}/{total_rounds} 轮准备重试"
                detail = (
                    f"{event.get('retry_in_seconds', 0):g} 秒后进行第 "
                    f"{event.get('next_attempt')}/4 次尝试：{event.get('message', '')}"
                )
            metadata = {key: item for key, item in event.items() if key != "type"}
            progress(
                min(94, int(value)), phase, detail,
                event_type=event_type, metadata=metadata,
            )

        report = LLMFactorMiner().mine_report(
            panel,
            n=count,
            rounds=rounds,
            periods=horizon,
            max_retries=3,
            on_event=relay,
            cancelled=cancelled,
        )
        if cancelled and cancelled():
            raise InterruptedError("AI 因子发现已取消或 LLM 配置已更新")
        mined = [item for item in report.factors if not item.error]
        versions = []
        for rank, item in enumerate(mined, start=1):
            if cancelled and cancelled():
                raise InterruptedError("AI 因子发现已取消或 LLM 配置已更新")
            spec = FactorSpec(
                slug=_slug("llm", item.expression),
                name=f"AI 候选 {rank}",
                expression=item.expression,
                description="AI 提出、由本地历史数据初筛，待统一验证与人工审批。",
                category="AI 发现",
                rationale=item.rationale,
                horizons=tuple(get_config().lab.horizons),
                tags=("llm", "discovered"),
            )
            _factor, version, _created = self.store.create_factor(
                spec, source="llm", actor="worker")
            versions.append(self.store.version(version["id"]) or version)
        if progress:
            detail = report.warnings[0]["message"] if report.warnings else "候选已写入版本账本"
            progress(96, f"保存 {len(versions)} 个候选", detail)
        return {
            "method": "llm", "candidates": versions,
            "raw": [asdict(item) for item in report.factors],
            "snapshot": snapshot["snapshot_hash"],
            "rounds_requested": report.rounds_requested,
            "rounds_completed": report.rounds_completed,
            "attempts": report.attempts,
            "warnings": report.warnings,
        }

    def _python_mining_context(
        self, universe: str, start: str, end: str, progress=None,
    ) -> tuple[dict[str, pd.DataFrame], list[dict[str, Any]], dict, str]:
        from quantmaster.data.research import ResearchDataBundle, load_research_bundle
        from quantmaster.data.research_features import registered_features

        panel, membership, snapshot = self._context(universe, start, end, progress)
        quality = str(snapshot["payload"].get("research_quality") or "sandbox")
        symbols = list(panel["close"].columns)
        if quality == "production":
            def relay(
                done: int, total: int, symbol: str, success: bool, detail: str = "",
            ) -> None:
                if progress:
                    detail = f"{done}/{total} · {symbol}" + (f" · {detail}" if detail else "")
                    if not success:
                        detail += " · 严格数据门禁失败"
                    progress(
                        20 + int(30 * done / max(1, total)), "PIT 研究包", detail,
                    )

            bundle = load_research_bundle(
                symbols, start, end, membership=membership, progress=relay,
            )
            bundle.fundamentals = self._pit_fundamentals(
                symbols, start, end, production=True,
            )
        else:
            bundle = ResearchDataBundle.from_legacy_panel(panel, membership=membership)
            bundle.fundamentals = self._pit_fundamentals(
                symbols, start, end, production=False,
            )
        close = bundle.signal["close"]
        bundle.signal.setdefault("returns", close.pct_change(fill_method=None))
        if "amount" in bundle.signal and "volume" in bundle.signal:
            bundle.signal.setdefault("vwap", bundle.signal["amount"].div(
                bundle.signal["volume"].replace(0, pd.NA)))
        try:
            from quantmaster.ai.sentiment import quality_sentiment_panel

            news_tier: Literal["production", "sandbox"] = (
                "production" if quality == "production" else "sandbox"
            )
            news_panel = quality_sentiment_panel(
                close.index, symbols, tier=news_tier,
            )
            news_metadata = dict(news_panel.attrs.get("news_factor") or {})
            aligned_news = news_panel.reindex(index=close.index, columns=close.columns)
            aligned_news.attrs["news_factor"] = news_metadata
            bundle.signal["news_sentiment"] = aligned_news
            bundle.manifest["news_sentiment"] = news_metadata
            if news_tier == "sandbox" and news_metadata.get("event_count"):
                bundle.warnings.append({
                    "code": "news_feature_sandbox",
                    "level": "warning",
                    "message": (
                        "消息面使用 sandbox 预览；短历史、快讯或未完成抓取窗口"
                        "不得晋级 production。"
                    ),
                })
        except Exception as exc:
            bundle.warnings.append({
                "code": "news_feature_unavailable", "level": "warning",
                "message": str(exc)[:300],
            })
        from quantmaster.data.industry import load_cached_industry_map

        industry = load_cached_industry_map()
        names = sorted({industry.get(symbol, "") for symbol in symbols} - {""})
        for number, name in enumerate(names, start=1):
            key = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or str(number)
            industry_values = [
                1.0 if industry.get(symbol) == name else 0.0 for symbol in symbols
            ]
            bundle.context[f"industry_{key[:48]}"] = pd.DataFrame(
                [industry_values] * len(close.index), index=close.index, columns=symbols,
            )
        feature_values, descriptors = registered_features(bundle)
        catalog = [item.to_dict() for item in descriptors]
        news_metadata = dict(
            bundle.signal.get("news_sentiment", pd.DataFrame()).attrs.get("news_factor") or {},
        )
        for descriptor in catalog:
            if descriptor["name"] != "news_sentiment":
                continue
            descriptor["tier"] = str(news_metadata.get("tier") or "production")
            descriptor["formal_eligible"] = bool(news_metadata.get("formal_eligible"))
            descriptor["evidence"] = news_metadata
            if not descriptor["formal_eligible"]:
                descriptor["pit_grade"] = "research_only"
        return feature_values, catalog, snapshot, bundle.manifest_hash

    def discover_python(
        self, *, run_id: str, universe: str, start: str, end: str, horizon: int = 3,
        rounds: int = 3, candidate_limit: int = 24, finalists: int = 3,
        progress=None, cancelled=None,
    ) -> dict:
        from quantmaster.factors.mining import PythonFactorMiner
        from quantmaster.factors.python_artifact import write_python_factor_artifact
        from quantmaster.lab.validation import validate_factor_values

        if not get_config().lab.ai_python_mining_enabled:
            raise ValueError("受限 Python AutoMiner 尚未启用")
        self.store.update_mining_run(run_id, status="running")
        try:
            features, feature_catalog, snapshot, bundle_hash = self._python_mining_context(
                universe, start, end, progress,
            )

            def checkpoint(candidate) -> None:
                if cancelled and cancelled():
                    raise InterruptedError("AutoMiner 已取消或 LLM 配置已更新")
                self.store.save_mining_candidate(run_id, candidate.to_dict())

            miner = PythonFactorMiner()
            report = miner.mine_report(
                features, feature_catalog, horizon=horizon, rounds=rounds,
                candidate_limit=candidate_limit, finalists=finalists, progress=progress,
                cancelled=cancelled, on_candidate=checkpoint,
            )
            if cancelled and cancelled():
                result = {
                    "method": "python", "run_id": run_id, "cancelled": True,
                    "split": report.split, "candidate_count": len(report.candidates),
                    "finalist_count": 0, "warnings": [],
                }
                self.store.update_mining_run(
                    run_id, status="cancelled", split=report.split, result=result,
                    snapshot_hash=snapshot["snapshot_hash"],
                )
                return result
            quality = str(snapshot["payload"].get("research_quality") or "sandbox")
            grades = {item["name"]: item["pit_grade"] for item in feature_catalog}
            runtime_compatible = {
                item["name"]: bool(item.get("runtime_compatible", True))
                for item in feature_catalog
            }
            versions = []
            for order, candidate in enumerate(report.finalists, start=1):
                if cancelled and cancelled():
                    raise InterruptedError("AutoMiner 已取消或 LLM 配置已更新")
                non_pit_features = sorted(
                    name for name in candidate.required_features
                    if grades.get(name) == "research_only"
                )
                runtime_only_features = sorted(
                    name for name in candidate.required_features
                    if not runtime_compatible.get(name, True)
                )
                artifact = write_python_factor_artifact(
                    get_config().data_root, source=candidate.code,
                    params=candidate.selected_params, manifest={
                        "kind": "restricted-python-factor", "name": candidate.name,
                        "hypothesis": candidate.hypothesis, "objective": candidate.objective,
                        "required_features": candidate.required_features,
                        "warmup": candidate.warmup, "horizon": horizon,
                        "split": report.split, "finalist_order": order,
                        "dataset_snapshot": snapshot["snapshot_hash"],
                        "research_bundle_hash": bundle_hash, "research_quality": quality,
                        "non_pit_features": non_pit_features,
                        "runtime_incompatible_features": runtime_only_features,
                        "audit": candidate.audit,
                    },
                )
                candidate.artifact = artifact
                spec = FactorSpec(
                    slug=_slug("python", f"{candidate.code}\n{candidate.selected_params}"),
                    name=candidate.name, kind="python",
                    description="AI 提出受限 Python，已完成本地三段验证，待人工审批。",
                    category="AI 自动挖掘", rationale=candidate.hypothesis,
                    required_features=tuple(candidate.required_features), horizons=(horizon,),
                    artifact=artifact, tags=("python", "autominer", quality),
                )
                _factor, version, _created = self.store.create_factor(
                    spec, source="python-autominer", actor="worker",
                )
                test = candidate.test_metrics
                full_values = miner.runner.execute(
                    candidate.code, features, candidate.selected_params,
                )
                sensitivity_variants = {}
                plateau = candidate.audit.get("parameter_plateau", {})
                for item in plateau.get("variants", [])[:8]:
                    params = item.get("params") or {}
                    if params == candidate.selected_params:
                        continue
                    try:
                        label = json.dumps(params, ensure_ascii=False, sort_keys=True)
                        sensitivity_variants[label] = miner.runner.execute(
                            candidate.code, features, params,
                        )
                    except Exception:
                        continue
                membership = features.get("membership")
                validation = validate_factor_values(
                    full_values,
                    features["close"],
                    name=candidate.name,
                    horizons=(horizon,),
                    membership=membership.astype(bool) if membership is not None else None,
                    research_quality=quality,
                    panel=features,
                    parameter_variants=sensitivity_variants,
                )
                hard_failures = validation["gates"]["hard_failures"]
                if quality != "production":
                    failure = "候选不是 point-in-time 生产级快照"
                    if failure not in hard_failures:
                        hard_failures.append(failure)
                if non_pit_features:
                    hard_failures.append(
                        f"使用非 PIT 特征，仅限研究: {', '.join(non_pit_features)}"
                    )
                if runtime_only_features:
                    hard_failures.append(
                        "特征仅用于研究回放，当前 Champion 运行时不可复现: "
                        + ", ".join(runtime_only_features)
                    )
                if not candidate.audit.get("lookahead", {}).get("passed"):
                    hard_failures.append("前视审计未通过")
                if not candidate.audit.get("recursive", {}).get("passed"):
                    hard_failures.append("递归稳定性审计未通过")
                soft_failures = validation["gates"]["soft_failures"]
                if abs(float(test.get("rank_ic", 0))) < 0.02:
                    soft_failures.append("密封 TEST |RankIC| 低于 0.02")
                if float(candidate.valid_metrics.get("q_value", 1)) > 0.10:
                    soft_failures.append("候选族 BH-FDR q-value 高于 0.10")
                horizon_report = validation["horizons"][str(horizon)]
                horizon_report.update({
                    "train": candidate.train_metrics,
                    "valid": candidate.valid_metrics,
                    "sealed_test": test,
                    "q_value": candidate.valid_metrics.get("q_value", 1),
                })
                validation.update({
                    "sealed_holdout": report.split["test"],
                    "dataset_snapshot": snapshot["snapshot_hash"],
                    "research_quality": quality,
                    "family_fdr": True,
                })
                validation["gates"].update({
                    "passed": not hard_failures and not soft_failures,
                    "override_allowed": not hard_failures,
                    "bias_audit_required": True,
                })
                version = self.store.save_validation(
                    version["id"], snapshot["snapshot_hash"], validation,
                )
                self.store.save_bias_audit(version["id"], snapshot["snapshot_hash"], {
                    "passed": not hard_failures, "checks": candidate.audit,
                    "version_id": version["id"],
                })
                candidate.factor_version_id = version["id"]
                checkpoint(candidate)
                versions.append(version)
            result = {
                "method": "python", "run_id": run_id,
                "snapshot": snapshot["snapshot_hash"], "research_quality": quality,
                "split": report.split, "rounds_requested": report.rounds_requested,
                "rounds_completed": report.rounds_completed, "llm_calls": report.llm_calls,
                "candidate_count": len(report.candidates),
                "finalist_count": len(report.finalists), "versions": versions,
                "warnings": report.warnings,
            }
            status = "completed_with_warnings" if report.warnings else "completed"
            self.store.update_mining_run(
                run_id, status=status, split=report.split, result=result,
                snapshot_hash=snapshot["snapshot_hash"],
            )
            if progress:
                progress(97, f"AutoMiner 完成 · {len(versions)} 个候选待人工审批")
            return result
        except InterruptedError:
            self.store.update_mining_run(
                run_id, status="cancelled", result={"cancelled": True},
            )
            raise
        except Exception as exc:
            self.store.update_mining_run(
                run_id, status="failed", result={"error": str(exc)[:1000]},
            )
            raise

    def train_model(
        self, *, model: str, universe: str, start: str, end: str, horizon: int = 3,
        sequence_length: int = 20, config: dict | None = None, progress=None,
        cancelled=None,
    ) -> dict:
        raise LabError(
            "LAB_MODEL_SCHEMA_RETIRED",
            "单周期训练工件合同已退役，不再写入 schema v1",
            action="使用共享多周期优化生成唯一 schema v2 模型",
            status_code=409,
        )
        from quantmaster.lab.ml import artifact_sha256, make_indexed_samples, train_indexed
        from quantmaster.lab.models import utc_now
        from quantmaster.lab.validation import validate_factor_values

        experiment = self.store.create_experiment(
            f"{model.upper()} · {universe} · {horizon}d", model,
            {"universe": universe, "start": start, "end": end, "horizon": horizon,
             "sequence_length": sequence_length, **(config or {})},
        )
        try:
            panel, membership, snapshot = self._context(universe, start, end, progress)
            if progress:
                progress(57, "构造 48 维时序特征")
            cache_root = (
                Path(get_config().data_root) / "lab_cache" / "features"
                / f"{snapshot['snapshot_hash']}-lab-v3"
            )
            samples = make_indexed_samples(
                panel, horizon=horizon, sequence_length=sequence_length,
                membership=membership, storage_dir=cache_root,
            )
            feature_names = samples.feature_names
            artifact_dir = (
                Path(get_config().data_root) / "lab_artifacts" / experiment["id"]
            )
            result = train_indexed(
                model, samples, artifact_dir=artifact_dir,
                config={
                    "device": get_config().lab.device,
                    "gpu_memory_fraction": get_config().lab.gpu_memory_fraction,
                    **(config or {}),
                },
                progress=progress, cancelled=cancelled,
            )
            predicted = result.pop("_predicted")
            result.pop("_actual")
            validation_positions = result.pop("_validation_positions")
            prediction_rows = samples.metadata_frame(validation_positions)
            prediction_rows["value"] = predicted
            predicted_values = prediction_rows.pivot(
                index="date", columns="symbol", values="value",
            ).reindex(columns=panel["close"].columns)
            if progress:
                progress(92, "运行学习模型样本外统一验证")
            report = validate_factor_values(
                predicted_values,
                panel["close"],
                name=f"{model.upper()} {horizon}日超额收益",
                horizons=(horizon,),
                membership=membership,
                research_quality=snapshot["payload"]["research_quality"],
                panel=panel,
            )
            report["model_metrics"] = result.get("metrics", {})
            report["dataset_snapshot"] = snapshot["snapshot_hash"]

            root = Path(get_config().data_root).resolve()
            artifact = Path(result["artifact"]).resolve()
            artifact_relative = artifact.relative_to(root).as_posix()
            manifest_path = artifact_dir / "manifest.json"
            manifest = {
                "schema_version": 1,
                "kind": model,
                "features": feature_names,
                "feature_version": "lab-v2-cross-sectional",
                "minimum_feature_coverage": 0.80,
                "sequence_length": sequence_length,
                "horizon": horizon,
                "training_universe": universe,
                "training_start": start,
                "trained_through": end,
                "fit_through": result.get("fit_through"),
                "validation_start": result.get("validation_start"),
                "snapshot_hash": snapshot["snapshot_hash"],
                "research_quality": snapshot["payload"]["research_quality"],
                "artifact": artifact_relative,
                "artifact_sha256": artifact_sha256(artifact),
                "metrics": result.get("metrics", {}),
                "created_at": utc_now(),
            }
            manifest_path.write_text(
                strict_json_dumps(manifest, indent=2), encoding="utf-8",
            )
            manifest_relative = manifest_path.resolve().relative_to(root).as_posix()
            learned = FactorSpec(
                slug=f"ml_{model}_{experiment['id'][:12]}",
                name=f"{model.upper()} · {universe} · {horizon}日",
                kind="learned",
                description="Quant Lab 训练的超额收益模型；默认影子运行，验证和人工批准后方可部署。",
                category="学习模型",
                required_features=tuple(feature_names),
                horizons=(horizon,),
                rationale="48 维日线特征、截面标准化与时间顺序样本外验证。",
                model={
                    "manifest": manifest_relative,
                    "artifact_sha256": manifest["artifact_sha256"],
                    "experiment_id": experiment["id"],
                    "trained_through": end,
                    "fit_through": result.get("fit_through"),
                    "validation_start": result.get("validation_start"),
                    "training_universe": universe,
                    "research_quality": manifest["research_quality"],
                },
                tags=("ml", model, "shadow"),
            )
            _factor, version, _created = self.store.create_factor(
                learned, source="ml", actor="worker",
            )
            version = self.store.save_validation(
                version["id"], snapshot["snapshot_hash"], report,
            )
            research_rows = prediction_rows.rename(columns={"date": "trade_date"})[
                ["trade_date", "symbol", "value"]
            ]
            publication: dict[str, Any] | None = None
            publication_warning: dict[str, Any] | None = None
            try:
                publication = self._stage_model_publication(
                    version_id=version["id"], experiment_id=experiment["id"],
                    artifact_dir=artifact_dir, slug=learned.slug,
                    prediction_rows=research_rows,
                )
                publication = self.publish_model_outbox(str(publication["id"]))
                if publication.get("status") != "published":
                    publication_warning = {
                        "code": "model_publication_pending",
                        "message": "模型训练已完成，研究分区发布将在后台安全重试",
                        "publication_id": publication["id"],
                        "error": publication.get("last_error", ""),
                    }
            except Exception as exc:
                logger.exception(
                    "Unable to stage Lab model publication experiment=%s", experiment["id"],
                )
                publication_warning = {
                    "code": "model_publication_staging_failed",
                    "message": "模型训练已完成，但预测分区暂存失败",
                    "error": f"{type(exc).__name__}: {exc}"[:1000],
                }
            result.update({
                "features": feature_names,
                "snapshot_hash": snapshot["snapshot_hash"],
                "experiment_id": experiment["id"],
                "version_id": version["id"],
                "version_status": version["status"],
                "validation": {
                    "candidate_score": report.get("candidate_score"),
                    "best_horizon": report.get("best_horizon"),
                    "coverage": report.get("coverage"),
                    "gates": report.get("gates", {}),
                },
                "manifest": manifest_relative,
                "sample_representation": "indexed-feature-cube",
                "feature_cache_hit": samples.cache_hit,
                "research_artifact": (
                    publication.get("result", {}) if publication else {
                        "ref": f"artifact:model:stock:{learned.slug}@1.0.0",
                        "partitions": 0,
                    }
                ),
                "publication": publication or {},
            })
            if publication_warning:
                result.setdefault("warnings", []).append(publication_warning)
            self.store.update_experiment(
                experiment["id"],
                status="completed_with_warnings" if publication_warning else "completed",
                result=result,
                dataset_id=snapshot["id"],
            )
            return result
        except Exception as exc:
            self.store.update_experiment(
                experiment["id"], status="failed", result={"error": str(exc)[:1000]})
            raise

    def optimize_study(
        self, study_id: str, *, progress=None, cancelled=None, resume: bool = False,
    ) -> dict:
        """执行持久化多目标研究；密封集通过后仅生成 Shadow Candidate。"""
        from quantmaster.lab.ml import artifact_sha256
        from quantmaster.lab.models import utc_now
        from quantmaster.lab.optimization import OptimizationRunner
        from quantmaster.lab.research import OptimizationSpec

        study = self.store.study(study_id)
        if study is None:
            raise KeyError("优化 Study 不存在")
        spec = OptimizationSpec.from_dict(study["config"])
        self.store.update_study(study_id, status="running")
        experiment = self.store.create_experiment(
            f"Multi-horizon · {spec.universe} · {study_id[:8]}",
            "multi-objective", spec.to_dict(),
        )
        self.store.update_study(study_id, experiment_id=experiment["id"])
        try:
            if spec.research_tier == "production":
                from quantmaster.data.research import load_research_bundle

                if progress:
                    progress(5, "加载 point-in-time 中证800成分")
                membership = load_csi800_membership(spec.start, spec.end)
                symbols = sorted(
                    symbol for symbol in membership if membership[symbol].any()
                )

                def research_progress(
                    done: int, total: int, symbol: str, success: bool, detail: str = "",
                ) -> None:
                    if progress:
                        detail = f"{done}/{total} · {symbol}" + (f" · {detail}" if detail else "")
                        if not success:
                            detail += " · 数据门禁失败"
                        progress(
                            20 + int(30 * done / max(1, total)),
                            "原始成交/PIT约束", detail,
                        )

                research_bundle = load_research_bundle(
                    symbols, spec.start, spec.end, membership=membership,
                    progress=research_progress,
                )
                panel = research_bundle.signal
                payload = create_snapshot(
                    spec.universe, spec.start, spec.end,
                    panel=panel, membership=membership,
                ).to_dict()
                payload.pop("snapshot_hash", None)
                payload["research_bundle"] = {
                    **research_bundle.manifest,
                    "manifest_hash": research_bundle.manifest_hash,
                }
                snapshot = self.store.save_snapshot(payload)
            else:
                panel, membership, snapshot = self._context(
                    spec.universe, spec.start, spec.end, progress,
                )
            fundamentals: dict[str, pd.DataFrame] = {}
            if "pit_fundamental_v1" in spec.features.groups:
                if progress:
                    progress(54, "加载 PIT 基本面")
                fundamentals = self._pit_fundamentals(
                    list(panel["close"].columns), spec.start, spec.end,
                    production=spec.research_tier == "production",
                    progress=progress,
                )
                from quantmaster.data.research import frame_fingerprint

                payload = dict(snapshot["payload"])
                payload.pop("snapshot_hash", None)
                payload["feature_input_hashes"] = {
                    name: frame_fingerprint(frame) for name, frame in fundamentals.items()
                }
                snapshot = self.store.save_snapshot(payload)

            def checkpoint(result: dict[str, Any]) -> None:
                self.store.update_study(
                    study_id, status=str(result.get("status") or "running"), result=result,
                    storage_url=str(result.get("storage") or ""),
                )

            runner = OptimizationRunner(Path(get_config().data_root) / "lab_artifacts")
            result = runner.run(
                study_id, spec, panel, membership=membership, fundamentals=fundamentals,
                progress=progress, cancelled=cancelled, checkpoint=checkpoint,
            )
            result["dataset_snapshot"] = snapshot["snapshot_hash"]
            result["research_tier"] = spec.research_tier
            if result.get("candidate"):
                root = Path(get_config().data_root).resolve()

                def relative(value: str) -> str:
                    return Path(value).resolve().relative_to(root).as_posix()

                manifest_path = root / "lab_artifacts" / study_id / "manifest-v2.json"
                manifest = {
                    "schema_version": 2,
                    "kind": str(result["recommended"]["params"]["model"]),
                    "horizons": list(spec.protocol.horizons),
                    "features": spec.features.to_dict(),
                    "feature_names": result.get("feature_names", []),
                    "sequence_length": spec.sequence_length,
                    "training_universe": spec.universe,
                    "protocol": spec.protocol.to_dict(),
                    "snapshot_hash": snapshot["snapshot_hash"],
                    "research_quality": spec.research_tier,
                    "prediction_artifact": relative(result["prediction_artifact"]),
                    "prediction_sha256": result["prediction_sha256"],
                    "fold_artifacts": [
                        {**item, "artifact": relative(item["artifact"])}
                        for item in result["fold_artifacts"]
                    ],
                    "live_artifact": {
                        **result["live_artifact"],
                        "artifact": relative(result["live_artifact"]["artifact"]),
                    },
                    "model_config": {
                        key: value for key, value in result["recommended"]["params"].items()
                        if key != "model"
                    },
                    "calibration": result["calibration"],
                    "calibration_models": result["calibration_models"],
                    "trained_through": result["live_artifact"]["fold"]["train_end"],
                    "maximum_age_trading_days": 25,
                    "created_at": utc_now(),
                }
                manifest_path.write_text(
                    strict_json_dumps(manifest, indent=2), encoding="utf-8",
                )
                learned = FactorSpec(
                    slug=f"ml_multi_{study_id[:12]}",
                    name=f"共享多周期 · {spec.universe} · {study_id[:8]}",
                    kind="learned", category="学习模型",
                    description="756/20 滚动训练并经 252 日密封留出评估的共享多周期模型。",
                    required_features=tuple(manifest.get("feature_names") or ()),
                    horizons=tuple(spec.protocol.horizons),
                    rationale="开发期 Pareto 选参、锁参后一次性密封评估；仅生成 Shadow 候选。",
                    model={
                        "manifest": relative(str(manifest_path)),
                        "manifest_sha256": artifact_sha256(manifest_path),
                        "study_id": study_id, "experiment_id": experiment["id"],
                        "training_universe": spec.universe,
                        "research_quality": spec.research_tier,
                    },
                    tags=("ml", "multi-horizon", "rolling-oof", "shadow"),
                )
                _factor, version, _created = self.store.create_factor(
                    learned, source="optimization", actor="worker",
                )
                horizons = {}
                for key, item in result["sealed_metrics"]["horizons"].items():
                    horizons[key] = {
                        "horizon": item["horizon"], "oos_rank_ic": item["rank_ic"],
                        "oos_icir": item["icir"], "q_value": item["q_value"],
                        "net_information_ratio": item["net_information_ratio"],
                        "net_annual_return": item["net_annual_return"],
                        "max_drawdown": item["max_drawdown"],
                        "turnover_daily": item["turnover"], "folds": [],
                    }
                report = {
                    "coverage": result["sealed_metrics"]["coverage"],
                    "best_horizon": max(
                        horizons.values(), key=lambda item: item["net_information_ratio"]
                    )["horizon"],
                    "candidate_score": round(
                        50 + 10 * result["sealed_metrics"]["net_information_ratio"], 2,
                    ),
                    "horizons": horizons,
                    "gates": {
                        "passed": spec.research_tier == "production",
                        "hard_failures": (
                            [] if spec.research_tier == "production"
                            else ["sandbox_research_tier"]
                        ),
                        "soft_failures": [],
                        "override_allowed": False, "bias_audit_required": True,
                    },
                    "sealed_holdout": result["sealed_holdout"],
                    "research_protocol": spec.protocol.to_dict(),
                    "family_fdr": True, "model_metrics": result["sealed_metrics"],
                    "calibration": result["calibration"],
                    "dataset_snapshot": snapshot["snapshot_hash"],
                }
                version = self.store.save_validation(
                    version["id"], snapshot["snapshot_hash"], report,
                )
                result["version_id"] = version["id"]
                result["version_status"] = version["status"]
                result["manifest"] = relative(str(manifest_path))
            final_status = "paused" if result.get("paused") else "completed"
            self.store.update_study(study_id, status=final_status, result=result)
            self.store.update_experiment(
                experiment["id"], status=final_status, result=result, dataset_id=snapshot["id"],
            )
            return result
        except InterruptedError:
            self.store.update_study(
                study_id, status="interrupted", result={"message": "研究已安全中断，可恢复"},
            )
            self.store.update_experiment(
                experiment["id"], status="interrupted",
                result={"message": "研究已安全中断，可恢复"},
            )
            raise
        except Exception as exc:
            self.store.update_study(study_id, status="failed", result={"error": str(exc)[:1000]})
            self.store.update_experiment(
                experiment["id"], status="failed", result={"error": str(exc)[:1000]},
            )
            raise

    @staticmethod
    def _pit_fundamentals(
        symbols: list[str], start: str, end: str, *, production: bool, progress=None,
    ) -> dict[str, pd.DataFrame]:
        if not production:
            from quantmaster.data.fundamentals import fundamental_panel

            return fundamental_panel(symbols, start, end)
        from quantmaster.data.fundamentals import quarterly_to_daily
        from quantmaster.data.tushare_source import TushareSource

        source = TushareSource()
        dates = pd.bdate_range(start, end)
        daily: dict[str, pd.DataFrame] = {}
        roe: dict[str, pd.DataFrame] = {}
        missing_daily, missing_roe = [], []
        for number, symbol in enumerate(symbols, start=1):
            indicators = source.daily_indicators(symbol, start, end)
            if not indicators.empty:
                daily[symbol] = indicators
            else:
                missing_daily.append(symbol)
            values = source.quarterly_roe(symbol, str(max(1990, int(start[:4]) - 1)))
            if not values.empty:
                roe[symbol] = quarterly_to_daily(values, dates)
            else:
                missing_roe.append(symbol)
            if progress:
                progress(
                    54 + int(10 * number / max(1, len(symbols))),
                    f"PIT 基本面 {number}/{len(symbols)} · {symbol}",
                )
        if missing_daily or missing_roe:
            detail = []
            if missing_daily:
                detail.append(
                    f"每日指标缺失 {len(missing_daily)} 只: {', '.join(missing_daily[:5])}"
                )
            if missing_roe:
                detail.append(
                    f"公告日 ROE 缺失 {len(missing_roe)} 只: {', '.join(missing_roe[:5])}"
                )
            raise RuntimeError("production PIT 基本面门禁未通过；" + "；".join(detail))
        result: dict[str, pd.DataFrame] = {}
        for field in ("pe_ttm", "pb", "dv_ratio", "total_mv"):
            result[field] = pd.DataFrame({
                symbol: frame[field].reindex(dates)
                for symbol, frame in daily.items() if field in frame
            })
        result["roe"] = pd.DataFrame({
            symbol: frame["roe"].reindex(dates) for symbol, frame in roe.items()
        })
        if not result.get("roe", pd.DataFrame()).notna().any().any():
            raise RuntimeError("production 研究缺少按真实公告日对齐的 ROE")
        return result

    def bias_audit(
        self, version_id: str, *, universe: str, start: str, end: str, progress=None,
    ) -> dict:
        """对新研究协议运行前缀、warm-up、标签成熟度和 PIT 清单审计。"""
        from quantmaster.lab.ml import artifact_sha256
        from quantmaster.lab.research import compare_prefixes, recursive_stability

        version = self.store.version(version_id)
        if version is None:
            raise KeyError("因子版本不存在")
        panel, membership, snapshot = self._context(universe, start, end, progress)
        spec = FactorSpec.from_dict(version["spec"])
        checks: dict[str, Any] = {}
        learned_manifest: dict[str, Any] = {}
        if spec.kind == "expression":
            from quantmaster.factors import compute_factor
            from quantmaster.factors.fundamental import resolve_factor

            factor = resolve_factor(spec.expression or spec.slug, list(panel["close"]), start, end)
            full = compute_factor(factor, panel)
            prefix_checks = []
            for ratio in (0.55, 0.70, 0.85):
                length = max(120, int(len(panel["close"]) * ratio))
                truncated = {key: value.iloc[:length] for key, value in panel.items()}
                prefix_checks.append(compare_prefixes(full.iloc[:length], compute_factor(factor, truncated)))
            checks["lookahead"] = {
                "passed": all(item["passed"] for item in prefix_checks), "prefixes": prefix_checks,
            }
            warmups = {}
            for length in (60, 120, 240, 480):
                if len(panel["close"]) >= length:
                    computed = compute_factor(
                        factor, {key: value.iloc[-length:] for key, value in panel.items()},
                    )
                    warmups[length] = computed.iloc[-1]
            checks["recursive"] = (
                recursive_stability(warmups, tolerance=1e-5)
                if len(warmups) >= 2 else {
                    "passed": False,
                    "reason": "历史长度不足，无法比较至少两个 warm-up 窗口",
                }
            )
        elif spec.kind == "python":
            root = Path(get_config().data_root).resolve()
            manifest_path = (root / str(spec.artifact.get("manifest") or "")).resolve()
            source_path = (root / str(spec.artifact.get("source") or "")).resolve()
            if root not in manifest_path.parents or root not in source_path.parents:
                raise ValueError("Python 因子工件路径越界")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            import hashlib
            integrity = hashlib.sha256(source_path.read_bytes()).hexdigest() == str(
                spec.artifact.get("source_sha256") or ""
            )
            audit = manifest.get("audit") or {}
            checks["lookahead"] = audit.get("lookahead") or {"passed": False}
            checks["recursive"] = audit.get("recursive") or {"passed": False}
            checks["artifact_integrity"] = {"passed": integrity}
        else:
            manifest_relative = str((spec.model or {}).get("manifest") or "")
            root = Path(get_config().data_root).resolve()
            manifest_path = (root / manifest_relative).resolve()
            if not manifest_path.is_relative_to(root) or not manifest_path.is_file():
                raise ValueError("学习模型 manifest 不存在或越出数据目录")
            expected_manifest = str((spec.model or {}).get("manifest_sha256") or "")
            if expected_manifest and artifact_sha256(manifest_path) != expected_manifest:
                raise ValueError("学习模型 manifest 完整性校验失败")
            learned_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            folds = learned_manifest.get("fold_artifacts") or []
            temporal = all(
                item.get("fold", {}).get("train_end", "")
                < item.get("fold", {}).get("test_start", "") for item in folds
            )
            integrity = True
            for item in folds:
                artifact = (root / str(item.get("artifact") or "")).resolve()
                if (
                    not artifact.is_relative_to(root) or not artifact.is_file()
                    or artifact_sha256(artifact) != item.get("artifact_sha256")
                ):
                    integrity = False
                    break
            checks["lookahead"] = {"passed": temporal, "fold_count": len(folds)}
            checks["recursive"] = {"passed": True, "reason": "模型由固定长度序列清单约束"}
            checks["artifact_integrity"] = {"passed": integrity}
        protocol = learned_manifest.get("protocol") or {}
        maximum_horizon = max(spec.horizons)
        checks["target_maturity"] = {
            "passed": not learned_manifest or int(protocol.get("purge_gap", 0)) >= maximum_horizon,
            "maximum_horizon": maximum_horizon,
            "purge_gap": protocol.get("purge_gap") if learned_manifest else None,
        }
        checks["pit_membership"] = {
            "passed": membership is not None and snapshot["payload"]["research_quality"] == "production",
        }
        checks["dataset_manifest"] = {
            "passed": bool(snapshot.get("snapshot_hash")), "snapshot_hash": snapshot.get("snapshot_hash"),
        }
        report = {
            "passed": all(item.get("passed", False) for item in checks.values()),
            "checks": checks, "version_id": version_id,
        }
        return self.store.save_bias_audit(version_id, snapshot["snapshot_hash"], report)

    def suggest_revision(
        self, version_id: str, *, use_cloud: bool = False,
        sample_consent: bool = False, sample: dict | None = None,
    ) -> dict:
        version = self.store.version(version_id)
        if version is None:
            raise KeyError("因子版本不存在")
        spec = FactorSpec.from_dict(version["spec"])
        if not spec.expression:
            raise ValueError("Copilot 修正只适用于安全 DSL 表达式因子")
        report = version.get("validation") or {}
        outbound = {
            "factor": {"expression": spec.expression, "rationale": spec.rationale},
            "validation_metrics": _metric_summary(report),
        }
        if sample_consent:
            if not get_config().lab.allow_cloud_sample:
                raise ValueError("设置中心尚未允许发送匿名样本")
            outbound["anonymous_sample"] = sample or {}
        if use_cloud:
            from quantmaster.runtime.llm import require_execution_lease

            require_execution_lease()
            payload = self._cloud_suggestion(outbound)
            require_execution_lease()
        else:
            payload = self._local_suggestion(spec, report)
        ExpressionFactor(str(payload["expression"]))
        payload["provider"] = "cloud" if use_cloud else "local"
        payload["sample_shared"] = bool(sample_consent)
        # The durable generic task owns the final fence.  Keep this check
        # immediately adjacent to the Lab-ledger write as well, so a rotated
        # provider response cannot create a user-visible pending suggestion.
        if use_cloud:
            require_execution_lease()
        return self.store.save_suggestion(
            version_id, version["content_hash"], payload, outbound)

    @staticmethod
    def _local_suggestion(spec: FactorSpec, report: dict) -> dict[str, Any]:
        best = (report.get("horizons") or {}).get(str(report.get("best_horizon")), {})
        expression = spec.expression
        risks = []
        if best.get("turnover_daily", 0) > 0.4:
            expression = f"ts_mean(({expression}), 3)"
            rationale = "用 3 日平滑降低换手，并保留原始信号方向。"
            risks.append("平滑可能削弱短周期拐点")
        elif abs(report.get("max_existing_correlation", 0)) >= 0.7:
            expression = f"rank(({expression})) - rank(ts_mean(returns, 20))"
            rationale = "剥离常见中期收益暴露，提高相对新颖性。"
            risks.append("正交化可能改变经济含义")
        else:
            expression = f"rank(({expression}))"
            rationale = "显式使用截面秩，降低异常值对候选稳定性的影响。"
            risks.append("秩变换会丢失信号幅度")
        return {
            "expression": expression,
            "rationale": rationale,
            "expected_effect": "改善跨阶段稳定性；必须重新跑完整验证后才能审批。",
            "risks": risks,
        }

    @staticmethod
    def _cloud_suggestion(outbound: dict) -> dict[str, Any]:
        from quantmaster.ai.llm import LLMClient

        response = LLMClient().chat_json(
            "请根据以下因子结构与本地验证指标，提出一次最小、可解释的表达式修正。"
            "不要假设看到了原始股票数据。输出 expression、rationale、expected_effect、risks。\n"
            f"{outbound}",
            system="你是 A 股量化因子研究助手。只使用安全 DSL，不输出 Python。",
        )
        if not isinstance(response, dict):
            raise ValueError("AI 建议不是 JSON 对象")
        return {
            "expression": str(response.get("expression", "")),
            "rationale": str(response.get("rationale", "")),
            "expected_effect": str(response.get("expected_effect", "")),
            "risks": [str(item) for item in response.get("risks", [])][:8],
        }

    def apply_suggestion(self, suggestion_id: str, *, actor: str = "web") -> dict:
        suggestion = self.store.suggestion(suggestion_id)
        if suggestion is None or suggestion["status"] != "pending":
            raise ValueError("建议不存在或已处理")
        base = self.store.version(suggestion["version_id"])
        if base is None:
            raise KeyError("基础因子版本不存在")
        if base["content_hash"] != suggestion["base_hash"]:
            raise ValueError("基础版本已变化，请重新生成建议")
        payload = suggestion["payload"]
        base_spec = dict(base["spec"])
        base_spec.update({
            "expression": payload["expression"],
            "rationale": payload.get("rationale", base_spec.get("rationale", "")),
        })
        _factor, version, _created = self.store.create_factor(
            FactorSpec.from_dict(base_spec), source="copilot", actor=actor,
            parent_id=base["id"],
        )
        self.store.resolve_suggestion(suggestion_id, "accepted")
        return self.store.version(version["id"]) or version

    @staticmethod
    def _expression_values(
        version: dict[str, Any], panel: dict[str, pd.DataFrame], start: str, end: str,
    ) -> pd.DataFrame:
        from quantmaster.factors import compute_factor
        from quantmaster.factors.fundamental import resolve_factor

        spec = FactorSpec.from_dict(version["spec"])
        if spec.kind != "expression":
            raise ValueError(f"{spec.kind} 因子尚未接入组合执行内核")
        factor = resolve_factor(
            spec.expression or spec.slug, list(panel["close"].columns), start, end,
        )
        return compute_factor(factor, panel)

    def research_cycle(
        self, *, universe: str = "csi800", start: str = "2015-01-01",
        end: str = "", progress=None, cancelled=None,
    ) -> dict[str, Any]:
        """Build horizon-specific ensembles without exposing sealed data to selection."""
        from quantmaster.lab.research import (
            WalkForwardSpec,
            benjamini_hochberg_family,
            walk_forward_folds,
        )
        from quantmaster.lab.validation import validate_factor_values

        end = end or market_date().isoformat()
        panel, membership, snapshot = self._context(
            universe, start, end, progress=progress, data_policy=DataPolicy.PREFER_LOCAL.value,
        )
        dates = pd.DatetimeIndex(panel["close"].index).normalize().unique().sort_values()
        protocol = WalkForwardSpec(horizons=SUPPORTED_HORIZONS)
        folds, sealed = walk_forward_folds(dates, protocol)
        cycle = self.store.create_research_cycle(
            snapshot_id=str(snapshot.get("id") or ""),
            protocol={
                **protocol.to_dict(), "folds": [item.to_dict() for item in folds],
                "sealed": sealed.to_dict(), "fdr_family": "candidate_x_horizon",
            },
        )
        development_dates = dates[dates <= pd.Timestamp(sealed.train_end)]
        development_panel = {
            key: frame.reindex(index=development_dates) for key, frame in panel.items()
        }
        development_membership = (
            membership.reindex(index=development_dates) if membership is not None else None
        )
        records: dict[str, dict[str, Any]] = {}
        for status in ("approved", "production", "degraded"):
            for item in self.store.list_factors(status=status, limit=500)["items"]:
                version_id = str(item["version_id"])
                version = self.store.version(version_id)
                if version is not None:
                    records[version_id] = version
        reports: dict[str, dict[str, Any]] = {}
        raw_values: dict[str, pd.DataFrame] = {}
        skipped: list[dict[str, str]] = []
        for number, (version_id, version) in enumerate(records.items(), start=1):
            if cancelled and cancelled():
                raise InterruptedError("研究周期已取消")
            if progress:
                progress(8 + int(42 * number / max(1, len(records))),
                         f"开发区复验 {number}/{len(records)} · {version.get('name', '')}")
            try:
                values = self._expression_values(version, panel, start, end)
                raw_values[version_id] = values
                report = validate_factor_values(
                    values.reindex(index=development_dates), development_panel["close"],
                    name=str(version.get("name") or version_id),
                    horizons=SUPPORTED_HORIZONS, membership=development_membership,
                    research_quality=str((snapshot.get("payload") or {}).get(
                        "research_quality", "production"
                    )), panel=development_panel, open_prices=development_panel.get("open"),
                    essential_only=True,
                )
                reports[version_id] = report
            except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
                skipped.append({"version_id": version_id, "reason": str(exc)})

        family: list[tuple[dict[str, Any], dict[str, Any]]] = []
        p_values: list[float] = []
        for report in reports.values():
            for evidence in report["horizons"].values():
                family.append((report, evidence))
                p_values.append(float(evidence.get("p_value", 1.0)))
        for (report, evidence), q_value in zip(
            family, benjamini_hochberg_family(p_values), strict=True,
        ):
            evidence["q_value"] = float(q_value)
            evidence["gates"] = atomic_horizon_gate(
                evidence, coverage=float(report.get("coverage") or 0),
                research_quality=str((snapshot.get("payload") or {}).get(
                    "research_quality", "production"
                )),
            )
        for version_id, report in reports.items():
            eligible = [
                int(value["horizon"]) for value in report["horizons"].values()
                if (value.get("gates") or {}).get("passed")
            ]
            report["eligible_horizons"] = eligible
            report["gates"] = {
                "passed": bool(eligible), "hard_failures": [],
                "soft_failures": [] if eligible else ["没有周期通过本轮统一 FDR 门槛"],
                "override_allowed": True,
            }
            self.store.save_validation(
                version_id, str((snapshot.get("payload") or {}).get("snapshot_hash") or ""),
                report,
            )

        sealed_dates = dates[dates >= pd.Timestamp(sealed.test_start)]
        sealed_panel = {key: frame.reindex(index=sealed_dates) for key, frame in panel.items()}
        sealed_membership = (
            membership.reindex(
                index=sealed_dates, columns=sealed_panel["close"].columns,
            ).fillna(False)
            if membership is not None else None
        )
        sealed_open = sealed_panel.get("open", sealed_panel["close"])
        sealed_benchmark = (
            (sealed_open.shift(-1) / sealed_open - 1)
            .where(sealed_membership)
            .mean(axis=1)
            if sealed_membership is not None else None
        )
        from quantmaster.data.industry import load_industry_map

        industry_map = load_industry_map(as_of=str(pd.Timestamp(dates[-1]).date()))
        strategies: list[dict[str, Any]] = []
        horizon_outcomes: dict[str, dict[str, Any]] = {}
        for position, horizon in enumerate(SUPPORTED_HORIZONS, start=1):
            candidates = []
            directions: dict[str, int] = {}
            for version_id, report in reports.items():
                evidence = report["horizons"].get(str(horizon)) or {}
                if not (evidence.get("gates") or {}).get("passed"):
                    continue
                directions[version_id] = int(evidence.get("direction") or 1)
                execution = evidence.get("execution") or {}
                candidates.append({
                    "version_id": version_id,
                    "development_score": max(
                        1e-6,
                        float(execution.get("net_information_ratio") or 0)
                        + abs(float(evidence.get("oos_icir") or 0)),
                    ),
                })
            development_values = {
                version_id: (
                    raw_values[version_id].reindex(index=development_dates)
                    * directions[version_id]
                ).where(development_membership)
                for version_id in directions
            }
            components = ensemble_weights(
                candidates, development_values, horizon=horizon,
            )
            if not components:
                horizon_outcomes[str(horizon)] = {
                    "qualified_factors": len(candidates),
                    "status": "no_strategy",
                    "reason": (
                        "合格因子不足 3 个" if len(candidates) < 3
                        else "合格因子高度相关，去重后不足 3 个"
                    ),
                }
                continue
            for component in components:
                component["direction"] = directions[component["version_id"]]
            full_oriented = {
                component["version_id"]: (
                    raw_values[component["version_id"]]
                    * int(component["direction"])
                ).where(membership)
                for component in components
            }
            combined = combine_scores(components, full_oriented)
            execution = execute_daily_targets(
                combined.reindex(index=sealed_dates), sealed_panel,
                horizon=horizon, top_n=12, cap_weight=0.10,
                industry_map=industry_map,
                benchmark_returns=sealed_benchmark,
            )
            bootstrap = moving_block_return_interval(
                execution["daily_excess"], block_days=max(20, 2 * horizon),
                seed=protocol.seed + horizon,
            )
            baseline_calmar = 0.0
            try:
                from quantmaster.decision.hybrid import rule_signal_bundle

                rule_score, _weights, _features = rule_signal_bundle(panel, horizon)
                baseline_result = execute_daily_targets(
                    rule_score.where(membership).reindex(index=sealed_dates), sealed_panel,
                    horizon=horizon, top_n=12, cap_weight=0.10,
                    industry_map=industry_map,
                    benchmark_returns=sealed_benchmark,
                )
                baseline_calmar = float(baseline_result["metrics"].get("calmar") or 0)
                baseline_annual = float(
                    baseline_result["metrics"].get("net_annual_excess_return") or 0
                )
            except (KeyError, TypeError, ValueError):
                baseline_annual = 0.0
            gate = strategy_sealed_gate(
                execution["metrics"], bootstrap, baseline_calmar=baseline_calmar,
            )
            evidence = {
                "horizon": horizon, "opened_once": True,
                "period": {"start": sealed.test_start, "end": sealed.test_end},
                "dataset_id": snapshot.get("id", ""), "metrics": execution["metrics"],
                "bootstrap": bootstrap, "gates": gate,
            }
            candidate = self.store.save_strategy_candidate(
                cycle_id=cycle["id"], horizon=horizon,
                name=f"CSI800 {horizon}日多因子组合",
                components=components,
                development={
                    "period_end": sealed.train_end,
                    "candidate_count": len(candidates),
                    "selection": "median_fold_net_ir_drawdown_turnover",
                    "max_component_correlation": 0.70,
                },
                sealed_evidence=evidence,
                return_curve={"baseline_annual_net_excess_return": baseline_annual},
            )
            strategies.append(candidate)
            horizon_outcomes[str(horizon)] = {
                "qualified_factors": len(candidates),
                "selected_factors": len(components),
                "status": candidate["status"],
                "strategy_id": candidate["id"],
            }
            if progress:
                progress(55 + int(40 * position / len(SUPPORTED_HORIZONS)),
                         f"{horizon} 日组合密封评估完成")
        result = {
            "cycle_id": cycle["id"], "snapshot_id": snapshot.get("id", ""),
            "protocol": protocol.to_dict(), "sealed": sealed.to_dict(),
            "factor_reports": len(reports), "family_tests": len(family),
            "strategies": [item["id"] for item in strategies], "skipped": skipped,
            "horizon_outcomes": horizon_outcomes,
            "network_calls": 0,
        }
        self.store.complete_research_cycle(cycle["id"], result)
        return result

    def shadow_score(
        self, *, strategy_id: str = "", universe: str = "csi800",
        start: str = "2015-01-01", end: str = "", progress=None,
    ) -> dict[str, Any]:
        end = end or market_date().isoformat()
        candidates = [self.store.strategy(strategy_id)] if strategy_id else [
            *self.store.strategies(status="shadow_challenger", limit=30),
            *self.store.strategies(status="paper", limit=30),
            *self.store.strategies(status="champion", limit=30),
        ]
        candidates = [item for item in candidates if item]
        if not candidates:
            return {"scored": 0, "signals": [], "network_calls": 0}
        panel, _membership, _snapshot = self._context(
            universe, start, end, progress=progress, data_policy=DataPolicy.PREFER_LOCAL.value,
        )
        from quantmaster.portfolio import Ledger

        ledger = Ledger()
        ledger_symbols = [
            position.symbol for position in ledger.positions() if position.shares > 1e-9
        ]
        reference_symbols = [str(symbol) for symbol in panel["close"].columns]
        panel, missing_holding_bars = _extend_panel_with_local_symbols(
            panel, ledger_symbols,
        )
        versions: dict[str, dict[str, Any]] = {}
        raw: dict[str, pd.DataFrame] = {}
        for candidate in candidates:
            for component in candidate.get("components") or []:
                version_id = str(component["version_id"])
                if version_id in raw:
                    continue
                version = self.store.version(version_id)
                if version is None:
                    continue
                versions[version_id] = version
                raw[version_id] = self._expression_values(version, panel, start, end)
        signals = []
        open_prices = panel["open"].sort_index()
        dates = open_prices.index
        from quantmaster.data.industry import load_industry_map

        industry_map = load_industry_map(as_of=end)
        for candidate in candidates:
            components = candidate.get("components") or []
            values = {
                str(item["version_id"]): raw[str(item["version_id"])]
                * int(item.get("direction") or 1)
                for item in components if str(item["version_id"]) in raw
            }
            score = combine_scores(
                components, values, reference_columns=reference_symbols,
            )
            latest = score.dropna(how="all").index[-1]
            weights = target_weights(
                score, top_n=12, cap_weight=0.10, industry_map=industry_map,
            ).loc[latest]
            target = {str(symbol): float(value) for symbol, value in weights.items() if value > 0}
            current, portfolio, _priced_holdings = _ledger_weight_context(
                ledger, panel, pd.Timestamp(latest),
            )
            portfolio["unscored_holdings"] = list(missing_holding_bars)
            score_row = score.loc[latest]
            latest_prices = panel["close"].loc[:latest].ffill().iloc[-1]
            confidence: dict[str, float] = {}
            for symbol in set(target) | set(current):
                score_ready = symbol in score_row.index and pd.notna(score_row.get(symbol))
                price = latest_prices.get(symbol)
                price_ready = pd.notna(price) and np.isfinite(float(price)) and float(price) > 0
                confidence[symbol] = (
                    (1.0 if symbol in reference_symbols else 0.75)
                    if score_ready and price_ready else 0.0
                )
            horizon = int(candidate["horizon"])
            mature_date = (pd.Timestamp(latest) + pd.offsets.BDay(horizon + 1)).strftime("%Y-%m-%d")
            actions = holding_actions(
                target, current, evidence_valid=bool(portfolio["reliable"]),
                confidence=confidence,
            )
            signal = self.store.save_shadow_signal(
                candidate["id"], signal_date=pd.Timestamp(latest).strftime("%Y-%m-%d"),
                mature_date=mature_date,
                payload={
                    "target_weights": target,
                    "current_weights": current,
                    "actions": actions,
                    "portfolio": portfolio,
                    "confidence": confidence,
                    "reference_distribution": "csi800",
                    "strategy_evidence": {
                        "strategy_id": candidate["id"],
                        "status": candidate["status"],
                        "sealed_gates": (
                            (candidate.get("sealed_evidence") or {}).get("gates") or {}
                        ),
                    },
                    "horizon": horizon,
                },
            )
            for pending in self.store.shadow_signals(candidate["id"], limit=500):
                if pending.get("status") != "pending":
                    continue
                signal_date = pd.Timestamp(pending["signal_date"])
                positions = np.flatnonzero(dates > signal_date)
                if len(positions) <= horizon:
                    continue
                execution_pos, mature_pos = int(positions[0]), int(positions[horizon])
                held = pending["payload_json"].get("target_weights") or {}
                realized = []
                for symbol, weight in held.items():
                    if symbol not in open_prices.columns:
                        continue
                    first = open_prices.iloc[execution_pos][symbol]
                    last = open_prices.iloc[mature_pos][symbol]
                    if pd.notna(first) and pd.notna(last) and first > 0:
                        realized.append(float(weight) * (float(last) / float(first) - 1.0))
                gross = float(sum(realized))
                trade = get_config().trade
                cost = float(
                    2 * trade.commission_rate + trade.stamp_tax_rate
                    + 2 * trade.transfer_fee_rate + 2 * trade.slippage
                )
                benchmark = float(
                    (open_prices.iloc[mature_pos] / open_prices.iloc[execution_pos] - 1.0).mean()
                )
                self.store.save_shadow_signal(
                    candidate["id"], signal_date=pending["signal_date"],
                    mature_date=pd.Timestamp(dates[mature_pos]).strftime("%Y-%m-%d"),
                    payload=pending["payload_json"],
                    realized={"gross_return": gross, "net_return": gross - cost,
                              "net_excess_return": gross - cost - benchmark},
                )
            matured = [
                item for item in self.store.shadow_signals(candidate["id"], limit=500)
                if item.get("status") == "matured"
            ]
            returns = pd.Series([
                float(item["realized_json"].get("net_excess_return") or 0) for item in matured
            ], dtype=float)
            nav = (1 + returns.clip(lower=-0.999)).cumprod()
            max_dd = float((1 - nav / nav.cummax()).max()) if len(nav) else 0.0
            sealed_dd = float(
                ((candidate.get("sealed_evidence") or {}).get("metrics") or {}).get(
                    "max_drawdown", 0.25
                )
            )
            summary = {
                "matured_signal_days": len(matured),
                "net_excess_return": float(nav.iloc[-1] - 1) if len(nav) else 0.0,
                "max_drawdown": max_dd,
                "drawdown_within_stress": max_dd <= max(0.01, sealed_dd),
                "coverage_degraded": False,
            }
            self.store.update_strategy_tracking(candidate["id"], shadow=summary)
            signals.append({**signal, "actions": actions, "shadow_summary": summary})
        return {"scored": len(signals), "signals": signals, "network_calls": 0}

    def workbench(self, horizon: int | None = None) -> dict[str, Any]:
        if horizon is not None and horizon not in SUPPORTED_HORIZONS:
            raise ValueError("预测周期不受支持")
        strategies = self.store.strategies(limit=100)
        latest_cycle = self.store.latest_research_cycle() or {}
        cycle_outcomes = (latest_cycle.get("result_json") or {}).get("horizon_outcomes") or {}
        matrix = []
        for value in SUPPORTED_HORIZONS:
            items = [item for item in strategies if int(item["horizon"]) == value]
            latest = items[0] if items else None
            evidence = (latest or {}).get("sealed_evidence") or {}
            matrix.append({
                "horizon": value, "strategy_id": (latest or {}).get("id", ""),
                "status": (latest or {}).get("status", "missing"),
                "metrics": evidence.get("metrics") or {},
                "gates": evidence.get("gates") or {},
                "bootstrap": evidence.get("bootstrap") or {},
                "shadow": (latest or {}).get("shadow_summary") or {},
                "outcome": cycle_outcomes.get(str(value)) or {},
            })
        latest_actions: list[dict[str, Any]] = []
        portfolio: dict[str, Any] = {}
        for strategy in strategies:
            signals = self.store.shadow_signals(strategy["id"], limit=1)
            if signals:
                payload = signals[0]["payload_json"]
                if not portfolio and payload.get("portfolio"):
                    portfolio = dict(payload["portfolio"])
                latest_actions.extend([
                    {**item, "horizon": strategy["horizon"], "strategy_id": strategy["id"]}
                    for item in (payload.get("actions") or [])
                ])
        curve = {}
        curve_source = next((item for item in strategies if item.get("id")), None)
        if curve_source:
            curve = self.store.strategy_return_curve(curve_source["id"])
        return {
            "horizons": list(SUPPORTED_HORIZONS), "matrix": matrix,
            "funnel": self.store.overview().get("strategy_statuses", {}),
            "strategies": strategies, "latest_actions": latest_actions,
            "portfolio": portfolio,
            "return_curve": curve,
            "latest_research_cycle": latest_cycle,
        }

    def run_job(self, job: dict, progress=None, cancelled=None) -> dict:
        params = dict(job["params"])
        params.pop("_scheduled", None)
        kind = job["kind"]
        if kind == "prepare_data":
            return self.prepare_data(progress=progress, cancelled=cancelled, **params)
        if kind == "validate":
            return self.validate_version(progress=progress, cancelled=cancelled, **params)
        if kind == "discover_genetic":
            return self.discover_genetic(progress=progress, **params)
        if kind == "discover_llm":
            return self.discover_llm(progress=progress, cancelled=cancelled, **params)
        if kind == "discover_python":
            return self.discover_python(progress=progress, cancelled=cancelled, **params)
        if kind == "optimize":
            return self.optimize_study(progress=progress, cancelled=cancelled, **params)
        if kind == "bias_audit":
            return self.bias_audit(progress=progress, **params)
        if kind == "research_cycle":
            return self.research_cycle(progress=progress, cancelled=cancelled, **params)
        if kind == "shadow_score":
            return self.shadow_score(progress=progress, **params)
        raise ValueError(f"无法执行任务: {kind}")


def _expression_fields(expression: str) -> set[str]:
    fields = {"open", "high", "low", "close", "volume", "amount", "turnover", "vwap", "returns"}
    return {name for name in fields if re.search(rf"\b{re.escape(name)}\b", expression)}


def _metric_summary(report: dict) -> dict:
    return {
        "coverage": report.get("coverage"),
        "best_horizon": report.get("best_horizon"),
        "candidate_score": report.get("candidate_score"),
        "max_existing_correlation": report.get("max_existing_correlation"),
        "horizons": report.get("horizons", {}),
        "gates": report.get("gates", {}),
    }
