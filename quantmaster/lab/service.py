"""Quant Lab 应用服务：统一数据、发现、验证、训练和 AI 修正流程。"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import re
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from quantmaster.config import get_config
from quantmaster.factors.base import ExpressionFactor
from quantmaster.lab.catalog import curated_catalog
from quantmaster.lab.dataset import create_snapshot, load_csi800_membership, readiness
from quantmaster.lab.models import FactorSpec
from quantmaster.lab.store import LabStore
from quantmaster.runtime.json import strict_json_dumps

logger = logging.getLogger(__name__)


def _slug(prefix: str, expression: str) -> str:
    from quantmaster.lab.models import content_hash

    return f"{prefix}_{content_hash(expression)[:10]}"


class LabService:
    def __init__(self, store: LabStore | None = None):
        self.store = store or LabStore()
        self.store.sync_catalog(curated_catalog())

    def capabilities(self) -> dict[str, Any]:
        from quantmaster.lab.ml import capabilities as ml_capabilities

        return {
            **readiness(),
            "models": ml_capabilities(),
            "catalog_size": 48,
            "safe_dsl": True,
            "arbitrary_python": False,
            "restricted_python": True,
            "python_mining_enabled": bool(get_config().lab.ai_python_mining_enabled),
            "python_mining_limits": {"llm_calls": 3, "candidates": 24, "finalists": 3},
            "optuna": bool(importlib.util.find_spec("optuna")),
            "research_protocol": "756/20/252",
        }

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
            },
            "recent_jobs": self.store.jobs(8),
            "recent_experiments": self.store.list_experiments(6),
            "recent_studies": self.store.studies(6),
        }

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
            "prepare_data", "validate", "discover_genetic", "discover_llm", "train",
            "optimize", "bias_audit", "discover_python",
        }
        if kind not in allowed:
            raise ValueError(f"未知研究任务: {kind}")
        if kind == "discover_python":
            if not get_config().lab.ai_python_mining_enabled:
                raise ValueError("受限 Python AutoMiner 尚未在设置中心启用")
            clean = dict(params)
            clean["rounds"] = min(3, max(1, int(clean.get("rounds", 3))))
            clean["candidate_limit"] = min(24, max(1, int(clean.get("candidate_limit", 24))))
            clean["finalists"] = min(3, max(1, int(clean.get("finalists", 3))))
            run = self.store.create_mining_run(clean)
            clean["run_id"] = run["id"]
            job = self.store.enqueue(kind, clean)
            self.store.update_mining_run(run["id"], job_id=job["id"])
            return job
        return self.store.enqueue(kind, params)

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
        from datetime import date

        from quantmaster.lab.research import OptimizationSpec

        config = dict(payload)
        config["end"] = config.get("end") or date.today().isoformat()
        spec = OptimizationSpec.from_dict(config)
        study = self.store.create_study(spec.to_dict())
        job = self.enqueue("optimize", {"study_id": study["id"]})
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
        progress=None,
    ) -> tuple[dict[str, pd.DataFrame], pd.DataFrame | None, dict]:
        from quantmaster.data import load_panel
        from quantmaster.data.universe import load_universe

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

        panel = load_panel(symbols, start, end, progress=on_symbol)
        snapshot = create_snapshot(
            universe, start, end, panel=panel, membership=membership).to_dict()
        stored = self.store.save_snapshot(snapshot)
        if progress:
            progress(52, "数据快照已冻结")
        return panel, membership, stored

    def validate_version(
        self, version_id: str, *, universe: str, start: str, end: str,
        progress=None, cancelled=None,
    ) -> dict:
        from quantmaster.lab.validation import validate_factor_values

        version = self.store.version(version_id)
        if version is None:
            raise KeyError("因子版本不存在")
        spec = FactorSpec.from_dict(version["spec"])
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
            if expression == "news_sentiment":
                raise ValueError("消息面因子需先完成新闻标注；当前快照没有 news_sentiment 字段")
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
        mined = [item for item in report.factors if not item.error]
        versions = []
        for rank, item in enumerate(mined, start=1):
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
            def relay(done: int, total: int, symbol: str, success: bool) -> None:
                if progress:
                    detail = f"{done}/{total} · {symbol}"
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
        bundle.signal.setdefault("returns", close.pct_change())
        if "amount" in bundle.signal and "volume" in bundle.signal:
            bundle.signal.setdefault("vwap", bundle.signal["amount"].div(
                bundle.signal["volume"].replace(0, pd.NA)))
        try:
            from quantmaster.ai.sentiment import quality_sentiment_panel

            bundle.signal["news_sentiment"] = quality_sentiment_panel(
                close.index, symbols,
            ).reindex(index=close.index, columns=close.columns)
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
            values = [1.0 if industry.get(symbol) == name else 0.0 for symbol in symbols]
            bundle.context[f"industry_{key[:48]}"] = pd.DataFrame(
                [values] * len(close.index), index=close.index, columns=symbols,
            )
        values, descriptors = registered_features(bundle)
        return values, [item.to_dict() for item in descriptors], snapshot, bundle.manifest_hash

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
        from quantmaster.lab.ml import artifact_sha256, make_samples, train
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
            samples, targets, metadata, feature_names = make_samples(
                panel, horizon=horizon, sequence_length=sequence_length, membership=membership)
            artifact_dir = (
                Path(get_config().data_root) / "lab_artifacts" / experiment["id"]
            )
            result = train(
                model, samples, targets, metadata, artifact_dir=artifact_dir,
                config={"device": get_config().lab.device, **(config or {})},
                progress=progress, cancelled=cancelled,
            )
            predicted = result.pop("_predicted")
            result.pop("_actual")
            validation_metadata = result.pop("_validation_metadata")
            prediction_rows = pd.DataFrame(validation_metadata)
            prediction_rows["value"] = predicted
            prediction_rows["date"] = pd.to_datetime(prediction_rows["date"])
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

                def research_progress(done: int, total: int, symbol: str, success: bool) -> None:
                    if progress:
                        detail = f"{done}/{total} · {symbol}"
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
            payload = self._cloud_suggestion(outbound)
        else:
            payload = self._local_suggestion(spec, report)
        ExpressionFactor(str(payload["expression"]))
        payload["provider"] = "cloud" if use_cloud else "local"
        payload["sample_shared"] = bool(sample_consent)
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

    def run_job(self, job: dict, progress=None, cancelled=None) -> dict:
        params = dict(job["params"])
        params.pop("_scheduled", None)
        kind = job["kind"]
        if kind == "prepare_data":
            _panel, _membership, snapshot = self._context(progress=progress, **params)
            return {"snapshot": snapshot}
        if kind == "validate":
            return self.validate_version(progress=progress, cancelled=cancelled, **params)
        if kind == "discover_genetic":
            return self.discover_genetic(progress=progress, **params)
        if kind == "discover_llm":
            return self.discover_llm(progress=progress, cancelled=cancelled, **params)
        if kind == "discover_python":
            return self.discover_python(progress=progress, cancelled=cancelled, **params)
        if kind == "train":
            return self.train_model(progress=progress, cancelled=cancelled, **params)
        if kind == "optimize":
            return self.optimize_study(progress=progress, cancelled=cancelled, **params)
        if kind == "bias_audit":
            return self.bias_audit(progress=progress, **params)
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
