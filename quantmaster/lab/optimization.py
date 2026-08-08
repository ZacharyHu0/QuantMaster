"""Optuna 多目标研究、滚动 OOF 评估和密封留出执行器。"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.lab.models import content_hash, utc_now
from quantmaster.lab.multihorizon import (
    apply_probability_calibrators,
    fit_multi_fold,
    fit_probability_calibrators,
    fold_positions,
    make_multi_horizon_samples,
    predictions_to_frame,
    probability_diagnostics,
)
from quantmaster.lab.research import (
    OptimizationSpec,
    TimeFold,
    benjamini_hochberg_family,
    walk_forward_folds,
)

Progress = Callable[[int, str], None]
Cancelled = Callable[[], bool]
Checkpoint = Callable[[dict[str, Any]], None]


def estimated_roundtrip_cost() -> float:
    trade = get_config().trade
    return float(
        2 * trade.commission_rate + trade.stamp_tax_rate
        + 2 * trade.transfer_fee_rate + 2 * trade.slippage
    )


def _rank_correlation(actual: pd.Series, predicted: pd.Series) -> float:
    joined = pd.concat([actual, predicted], axis=1).dropna()
    if len(joined) < 5:
        return float("nan")
    return float(joined.iloc[:, 0].rank().corr(joined.iloc[:, 1].rank()))


def _two_sided_p(value: float, observations: int) -> float:
    if not np.isfinite(value) or observations < 4 or abs(value) >= 1:
        return 1.0 if not np.isfinite(value) else 0.0
    statistic = abs(value) * math.sqrt((observations - 2) / max(1e-12, 1 - value * value))
    return float(math.erfc(statistic / math.sqrt(2)))


def evaluate_predictions(
    frame: pd.DataFrame, *, top_n: int, roundtrip_cost: float,
) -> dict[str, Any]:
    """把横截面 OOF 预测转换为扣费收益、RankIC、换手和稳定性证据。"""
    horizons: dict[str, Any] = {}
    p_values: list[float] = []
    ordered_horizons: list[int] = []
    for horizon, horizon_frame in frame.groupby("horizon", sort=True):
        daily_ic, gross_returns, net_returns, turnovers = [], [], [], []
        previous: set[str] = set()
        for _date, group in horizon_frame.groupby("date", sort=True):
            usable = group.dropna(subset=["expected_excess", "actual_return"])
            daily_ic.append(_rank_correlation(usable["actual_excess"], usable["expected_excess"]))
            selected = usable.nlargest(min(top_n, len(usable)), "expected_excess")
            current = set(selected["symbol"].astype(str))
            turnover = 1.0 if not previous and current else (
                1 - len(previous & current) / max(1, len(previous | current))
            )
            gross = float(selected["actual_return"].mean()) if current else 0.0
            cost = turnover * roundtrip_cost
            gross_returns.append(gross)
            net_returns.append(gross - cost)
            turnovers.append(turnover)
            previous = current
        ic = pd.Series(daily_ic, dtype=float).dropna()
        net = pd.Series(net_returns, dtype=float)
        gross = pd.Series(gross_returns, dtype=float)
        turnover_series = pd.Series(turnovers, dtype=float)
        scale = 244 / max(1, int(horizon))
        net_ir = float(net.mean() / net.std(ddof=1) * math.sqrt(scale)) if net.std(ddof=1) > 0 else 0.0
        net_nav = (1 + net.clip(lower=-0.999)).cumprod()
        drawdown = float((1 - net_nav / net_nav.cummax()).max()) if not net_nav.empty else 1.0
        rank_ic = float(ic.mean()) if not ic.empty else 0.0
        icir = float(ic.mean() / ic.std(ddof=1)) if len(ic) > 1 and ic.std(ddof=1) > 0 else 0.0
        p_value = _two_sided_p(rank_ic, len(ic))
        cost_mean = float((turnover_series * roundtrip_cost).mean())
        edge_cost = abs(float(gross.mean())) / max(cost_mean, 1e-12)
        yearly = float((1 + net.clip(lower=-0.999)).prod() ** (scale / max(1, len(net))) - 1)
        horizons[str(int(horizon))] = {
            "horizon": int(horizon), "days": len(net),
            "rank_ic": rank_ic, "icir": icir, "p_value": p_value,
            "net_information_ratio": net_ir, "net_annual_return": yearly,
            "max_drawdown": drawdown, "turnover": float(turnover_series.mean()),
            "edge_cost_ratio": edge_cost,
            "coverage": float(horizon_frame["expected_excess"].notna().mean()),
        }
        p_values.append(p_value)
        ordered_horizons.append(int(horizon))
    for horizon, q_value in zip(ordered_horizons, benjamini_hochberg_family(p_values), strict=True):
        horizons[str(horizon)]["q_value"] = float(q_value)
    values = list(horizons.values())
    return {
        "horizons": horizons,
        "net_information_ratio": float(np.median([item["net_information_ratio"] for item in values])),
        "rank_ic": float(np.median([item["rank_ic"] for item in values])),
        "max_drawdown": float(max(item["max_drawdown"] for item in values)),
        "turnover": float(np.median([item["turnover"] for item in values])),
        "coverage": float(min(item["coverage"] for item in values)),
        "edge_cost_ratio": float(min(item["edge_cost_ratio"] for item in values)),
    }


def feasibility(metrics: dict[str, Any], spec: OptimizationSpec) -> dict[str, Any]:
    horizon_values = list((metrics.get("horizons") or {}).values())
    fold_values = list(metrics.get("folds") or [])
    signs = [
        np.sign(item.get("rank_ic", 0))
        for item in (fold_values if fold_values else horizon_values)
    ]
    dominant = max((signs.count(-1), signs.count(1)), default=0)
    sign_ratio = dominant / max(1, len(signs))
    failures = []
    if metrics.get("coverage", 0) < spec.minimum_coverage:
        failures.append("coverage")
    if sign_ratio < spec.minimum_fold_sign_ratio:
        failures.append("fold_sign_stability")
    if any(item.get("q_value", 1) > spec.maximum_fdr_q for item in horizon_values):
        failures.append("family_fdr")
    if any(abs(item.get("rank_ic", 0)) < 0.02 for item in horizon_values):
        failures.append("rank_ic")
    if any(abs(item.get("icir", 0)) < 0.20 for item in horizon_values):
        failures.append("icir")
    if metrics.get("edge_cost_ratio", 0) < spec.minimum_edge_cost_ratio:
        failures.append("edge_cost_ratio")
    return {"feasible": not failures, "failures": failures, "sign_ratio": sign_ratio}


def select_recommended(trials: list[dict[str, Any]]) -> dict[str, Any] | None:
    feasible = [
        item for item in trials if item.get("feasible") and item.get("pareto", True)
    ]
    if not feasible:
        return None
    return max(
        feasible,
        key=lambda item: (
            float(item["metrics"].get("net_information_ratio", -1e9)),
            -float(item["metrics"].get("max_drawdown", 1e9)),
            -float(item["metrics"].get("turnover", 1e9)),
        ),
    )


def _trial_config(trial, models: tuple[str, ...]) -> tuple[str, dict[str, Any]]:
    kind = trial.suggest_categorical("model", list(models))
    if kind == "ridge":
        return kind, {"alpha": trial.suggest_float("alpha", 0.05, 20.0, log=True)}
    return kind, {
        "learning_rate": trial.suggest_float("learning_rate", 1e-4, 2e-3, log=True),
        "batch_size": trial.suggest_categorical("batch_size", [128, 256, 512]),
        "gradient_accumulation": trial.suggest_categorical("gradient_accumulation", [1, 2, 4]),
        "epochs": trial.suggest_int("epochs", 12, 36), "patience": 5,
        "device": get_config().lab.device,
    }


def _sealed_folds(
    dates: pd.DatetimeIndex, sealed: TimeFold, spec: OptimizationSpec,
) -> list[TimeFold]:
    index = pd.DatetimeIndex(dates).normalize().unique().sort_values()
    positions = np.flatnonzero((index >= sealed.test_start) & (index <= sealed.test_end))
    folds = []
    for block, start_position in enumerate(positions[::spec.protocol.retrain_every], start=1):
        end_position = min(start_position + spec.protocol.retrain_every - 1, positions[-1])
        train_end = start_position - spec.protocol.purge_gap - 1
        train_start = max(0, train_end - spec.protocol.train_window + 1)
        folds.append(TimeFold(
            name=f"sealed-{block}", train_start=index[train_start].strftime("%Y-%m-%d"),
            train_end=index[train_end].strftime("%Y-%m-%d"),
            test_start=index[start_position].strftime("%Y-%m-%d"),
            test_end=index[end_position].strftime("%Y-%m-%d"), sealed=True,
        ))
    return folds


class OptimizationRunner:
    def __init__(self, artifact_root: str | Path):
        self.artifact_root = Path(artifact_root)
        self.artifact_root.mkdir(parents=True, exist_ok=True)

    def run(
        self, study_id: str, spec: OptimizationSpec, panel: dict[str, pd.DataFrame],
        *, membership: pd.DataFrame | None = None,
        fundamentals: dict[str, pd.DataFrame] | None = None,
        progress: Progress | None = None, cancelled: Cancelled | None = None,
        checkpoint: Checkpoint | None = None,
    ) -> dict[str, Any]:
        try:
            import optuna
        except ImportError as exc:
            raise RuntimeError("多目标优化需要 Optuna：pip install 'quantmaster[ml]'") from exc
        started = time.monotonic()
        deadline = started + spec.budget_hours * 3600
        root = self.artifact_root / study_id
        root.mkdir(parents=True, exist_ok=True)
        samples = make_multi_horizon_samples(
            panel, horizons=spec.protocol.horizons,
            sequence_length=spec.sequence_length, membership=membership,
            fundamentals=fundamentals, feature_spec=spec.features,
            storage_dir=root / "sample-store-v4",
        )
        dates = pd.DatetimeIndex(panel["close"].index)
        mature_dates = dates[:-max(spec.protocol.horizons)]
        development_folds, sealed = walk_forward_folds(mature_dates, spec.protocol)
        storage_path = (root / "optuna.sqlite").resolve()
        from quantmaster.lab.ml import capabilities

        supported = set(capabilities()["multi_horizon_models"])
        available_models = tuple(kind for kind in spec.models if kind in supported)
        if not available_models:
            raise RuntimeError("当前环境没有可用的共享模型后端；请至少安装 scikit-learn")
        study = optuna.create_study(
            study_name=study_id, storage=f"sqlite:///{storage_path.as_posix()}", load_if_exists=True,
            directions=["maximize", "maximize", "minimize"],
            sampler=optuna.samplers.NSGAIISampler(seed=spec.seed),
        )
        if not study.trials and "ridge" in available_models:
            study.enqueue_trial({"model": "ridge", "alpha": 1.0})
        cost = estimated_roundtrip_cost()

        def objective(trial):
            if cancelled and cancelled():
                raise KeyboardInterrupt
            kind, config = _trial_config(trial, available_models)
            fold_frames = []
            fold_telemetry = []
            for number, fold in enumerate(development_folds, start=1):
                if time.monotonic() >= deadline:
                    raise optuna.TrialPruned("time_budget")
                train, valid = fold_positions(samples, fold)
                suffix = ".npz" if kind == "ridge" else ".pt"
                result = fit_multi_fold(
                    kind, samples, train, valid,
                    artifact_path=root / "trials" / str(trial.number) / f"fold-{number}{suffix}",
                    config={"seed": spec.seed, **config}, roundtrip_cost=cost,
                    progress=progress, cancelled=cancelled,
                )
                fold_frame = predictions_to_frame(samples, valid, result["_predictions"])
                fold_frame["fold"] = fold.name
                fold_frames.append(fold_frame)
                fold_telemetry.append(result.get("telemetry", {}))
            combined = pd.concat(fold_frames, ignore_index=True)
            trial_root = root / "trials" / str(trial.number)
            trial_root.mkdir(parents=True, exist_ok=True)
            combined.to_parquet(trial_root / "oof_predictions.parquet", index=False)
            metrics = evaluate_predictions(
                combined, top_n=spec.top_n,
                roundtrip_cost=cost,
            )
            metrics["telemetry"] = fold_telemetry
            metrics["folds"] = [
                {
                    "name": fold_name,
                    **{
                        key: value for key, value in evaluate_predictions(
                            group, top_n=spec.top_n, roundtrip_cost=cost,
                        ).items() if key != "horizons"
                    },
                }
                for fold_name, group in combined.groupby("fold", sort=True)
            ]
            gate = feasibility(metrics, spec)
            trial.set_user_attr("metrics", metrics)
            trial.set_user_attr("feasible", gate["feasible"])
            trial.set_user_attr("failures", gate["failures"])
            return metrics["net_information_ratio"], metrics["rank_ic"], metrics["max_drawdown"]

        remaining = max(0.0, deadline - time.monotonic())
        finished_trials = sum(
            trial.state.name not in {"WAITING", "RUNNING"} for trial in study.trials
        )
        trials_remaining = max(0, spec.max_trials - finished_trials)
        try:
            if trials_remaining:
                study.optimize(
                    objective, n_trials=trials_remaining, timeout=remaining,
                    gc_after_trial=True, show_progress_bar=False, catch=(RuntimeError,),
                )
        except KeyboardInterrupt as exc:
            raise InterruptedError("优化已取消") from exc
        pareto_numbers = {trial.number for trial in study.best_trials}
        trials = []
        for trial in study.trials:
            if trial.state.name != "COMPLETE" or not trial.user_attrs.get("metrics"):
                continue
            trials.append({
                "number": trial.number, "params": trial.params,
                "metrics": trial.user_attrs["metrics"],
                "feasible": bool(trial.user_attrs.get("feasible")),
                "pareto": trial.number in pareto_numbers,
                "failures": list(trial.user_attrs.get("failures") or []),
                "values": list(trial.values or []),
            })
        # 一次 Study 的所有 trial×horizon 属于同一假设族，统一校正，避免只在
        # 单个候选内部做 FDR 后仍产生“多试几次总会显著”的选择偏差。
        family_locations: list[tuple[dict[str, Any], str]] = []
        family_p_values: list[float] = []
        for item in trials:
            for horizon_key, horizon_metrics in item["metrics"]["horizons"].items():
                family_locations.append((item, horizon_key))
                family_p_values.append(float(horizon_metrics.get("p_value", 1.0)))
        for (item, horizon_key), q_value in zip(
            family_locations, benjamini_hochberg_family(family_p_values), strict=True,
        ):
            item["metrics"]["horizons"][horizon_key]["q_value"] = q_value
        for item in trials:
            gate = feasibility(item["metrics"], spec)
            item["feasible"] = gate["feasible"]
            item["failures"] = gate["failures"]
        recommended = select_recommended(trials)
        interim = {
            "study_id": study_id, "status": "optimizing", "trials": trials,
            "recommended": recommended, "storage": str(storage_path),
            "protocol": spec.protocol.to_dict(), "sealed_holdout": sealed.to_dict(),
            "feature_names": samples.feature_names,
        }
        if checkpoint:
            checkpoint(interim)
        if recommended is None:
            completed_count = sum(
                trial.state.name not in {"WAITING", "RUNNING"} for trial in study.trials
            )
            if time.monotonic() >= deadline and completed_count < spec.max_trials:
                return {
                    **interim, "status": "paused", "paused": True,
                    "warnings": [{
                        "code": "time_budget",
                        "message": "搜索预算已用完；Study 已保存，可恢复剩余 Trials。",
                    }],
                }
            return {
                **interim, "status": "completed", "candidate": None,
                "warnings": [{
                    "code": "no_feasible_pareto_trial",
                    "message": "开发期没有满足稳定性、FDR 与成本门槛的 Pareto 解。",
                }],
            }
        if time.monotonic() >= deadline:
            return {
                **interim, "status": "paused", "paused": True,
                "warnings": [{"code": "time_budget", "message": "已保存 Pareto 前沿，等待恢复密封评估。"}],
            }
        selected_kind = str(recommended["params"]["model"])
        selected_config = {key: value for key, value in recommended["params"].items() if key != "model"}
        development_predictions = pd.read_parquet(
            root / "trials" / str(recommended["number"]) / "oof_predictions.parquet",
        )
        calibration_models = fit_probability_calibrators(
            development_predictions, roundtrip_cost=cost,
        )
        sealed_frames: list[pd.DataFrame] = []
        artifacts: list[dict[str, Any]] = []
        sealed_folds = _sealed_folds(mature_dates, sealed, spec)
        for number, fold in enumerate(sealed_folds, start=1):
            if time.monotonic() >= deadline:
                return {
                    **interim, "status": "paused", "paused": True,
                    "sealed_completed_blocks": len(sealed_frames),
                    "sealed_total_blocks": len(sealed_folds),
                    "warnings": [{"code": "time_budget", "message": "密封滚动评估已检查点保存，等待恢复。"}],
                }
            train, valid = fold_positions(samples, fold)
            suffix = ".npz" if selected_kind == "ridge" else ".pt"
            artifact = root / "sealed" / f"fold-{number}{suffix}"
            prediction_block = root / "sealed" / f"predictions-{number}.parquet"
            metadata_block = root / "sealed" / f"fold-{number}.json"
            block_identity = content_hash({
                "kind": selected_kind, "config": selected_config, "fold": fold.to_dict(),
            })
            if artifact.is_file() and prediction_block.is_file() and metadata_block.is_file():
                metadata = json.loads(metadata_block.read_text(encoding="utf-8"))
                if (
                    metadata.get("identity") == block_identity
                    and metadata.get("artifact_sha256") == _sha256(artifact)
                    and metadata.get("prediction_sha256") == _sha256(prediction_block)
                ):
                    sealed_frames.append(pd.read_parquet(prediction_block))
                    artifacts.append({
                        "fold": fold.to_dict(), "artifact": str(artifact.resolve()),
                        "artifact_sha256": metadata["artifact_sha256"],
                        "telemetry": metadata.get("telemetry", {}),
                    })
                    if progress:
                        progress(
                            82 + int(12 * number / len(sealed_folds)),
                            f"复用密封滚动块 {number}/{len(sealed_folds)}",
                        )
                    continue
            result = fit_multi_fold(
                selected_kind, samples, train, valid, artifact_path=artifact,
                config={"seed": spec.seed, **selected_config}, roundtrip_cost=cost,
                progress=progress, cancelled=cancelled,
            )
            block_frame = predictions_to_frame(samples, valid, result["_predictions"])
            block_frame.to_parquet(prediction_block, index=False)
            metadata_block.write_text(json.dumps({
                "identity": block_identity,
                "artifact_sha256": result["artifact_sha256"],
                "prediction_sha256": _sha256(prediction_block),
                "telemetry": result.get("telemetry", {}),
            }, ensure_ascii=False), encoding="utf-8")
            sealed_frames.append(block_frame)
            artifacts.append({
                "fold": fold.to_dict(), "artifact": str(artifact.resolve()),
                "artifact_sha256": result["artifact_sha256"],
                "telemetry": result.get("telemetry", {}),
            })
            if progress:
                progress(82 + int(12 * number / len(sealed_folds)),
                         f"密封滚动评估 {number}/{len(sealed_folds)}")
        predictions = apply_probability_calibrators(
            pd.concat(sealed_frames, ignore_index=True), calibration_models,
        )
        prediction_path = root / "sealed_predictions.parquet"
        predictions.to_parquet(prediction_path, index=False)
        sealed_metrics = evaluate_predictions(
            predictions, top_n=spec.top_n, roundtrip_cost=cost,
        )
        sealed_gate = feasibility(sealed_metrics, spec)
        result = {
            **interim, "status": "completed", "sealed_metrics": sealed_metrics,
            "sealed_gate": sealed_gate,
            "calibration": probability_diagnostics(predictions),
            "calibration_models": calibration_models,
            "prediction_artifact": str(prediction_path.resolve()),
            "prediction_sha256": _sha256(prediction_path),
            "fold_artifacts": artifacts, "live_artifact": artifacts[-1],
            "telemetry": artifacts[-1].get("telemetry", {}),
            "candidate": bool(sealed_gate["feasible"]), "completed_at": utc_now(),
        }
        if checkpoint:
            checkpoint(result)
        return result


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
