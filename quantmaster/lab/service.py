"""Quant Lab 应用服务：统一数据、发现、验证、训练和 AI 修正流程。"""

from __future__ import annotations

import json
import re
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
                "window": [cfg.window_start, cfg.window_end],
                "weekly_days": cfg.weekly_days,
            },
            "recent_jobs": self.store.jobs(8),
            "recent_experiments": self.store.list_experiments(6),
        }

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
        allowed = {"prepare_data", "validate", "discover_genetic", "discover_llm", "train"}
        if kind not in allowed:
            raise ValueError(f"未知研究任务: {kind}")
        return self.store.enqueue(kind, params)

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
        progress=None,
    ) -> dict:
        from quantmaster.lab.validation import validate_factor_values

        version = self.store.version(version_id)
        if version is None:
            raise KeyError("因子版本不存在")
        spec = FactorSpec.from_dict(version["spec"])
        panel, membership, snapshot = self._context(universe, start, end, progress)
        if spec.kind == "learned":
            from quantmaster.lab.ml import predict_panel

            if progress:
                progress(58, "加载学习模型并校验工件完整性")
            values = predict_panel(panel, spec.model)
        elif spec.kind == "expression":
            from quantmaster.factors import compute_factor
            from quantmaster.factors.fundamental import resolve_factor

            symbols = list(panel["close"].columns)
            expression = spec.expression or spec.slug
            if expression == "news_sentiment":
                raise ValueError("消息面因子需先完成新闻标注；当前快照没有 news_sentiment 字段")
            if progress:
                progress(58, "计算因子并执行统一标准化")
            factor = resolve_factor(expression, symbols, start, end)
            values = compute_factor(factor, panel)
        else:
            raise ValueError(f"{spec.kind} 类型尚未提供可复验的运行时")
        if progress:
            progress(65, "运行 purged walk-forward 与多重检验")
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
        )
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
                json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8",
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
            })
            self.store.update_experiment(
                experiment["id"], status="completed", result=result,
                dataset_id=snapshot["id"],
            )
            return result
        except Exception as exc:
            self.store.update_experiment(
                experiment["id"], status="failed", result={"error": str(exc)[:1000]})
            raise

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
            return self.validate_version(progress=progress, **params)
        if kind == "discover_genetic":
            return self.discover_genetic(progress=progress, **params)
        if kind == "discover_llm":
            return self.discover_llm(progress=progress, cancelled=cancelled, **params)
        if kind == "train":
            return self.train_model(progress=progress, cancelled=cancelled, **params)
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
