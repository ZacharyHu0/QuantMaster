"""Standard factor diagnostics emitted alongside versioned research artifacts."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def _safe_mean(value: pd.Series) -> float | None:
    clean = pd.to_numeric(value, errors="coerce").dropna()
    return float(clean.mean()) if not clean.empty else None


def _correlation(sample: pd.DataFrame, left: str, right: str, method: str) -> float:
    if len(sample) < 3 or sample[left].nunique() < 2 or sample[right].nunique() < 2:
        return np.nan
    return float(sample[left].corr(sample[right], method=method))


def factor_diagnostics(
    factors: pd.DataFrame,
    labels: pd.DataFrame,
    factor_column: str,
    *,
    quantiles: int = 5,
) -> dict[str, Any]:
    """Coverage, IC decay, quantile returns and turnover efficiency."""
    keys = ["trade_date", "symbol"]
    if factor_column not in factors:
        raise ValueError(f"因子表缺少 {factor_column}")
    label_columns = [column for column in labels if column.startswith("fwd_return_")]
    if not label_columns:
        raise ValueError("标签表缺少 fwd_return_* 列")
    value = factors[[*keys, factor_column]].merge(
        labels[[*keys, *label_columns]], on=keys, how="inner", validate="one_to_one",
    )
    value["trade_date"] = pd.to_datetime(value["trade_date"])
    coverage_rows, ic_rows, quantile_rows = [], [], []
    previous_top: set[str] | None = None
    turnovers: list[float] = []
    for trade_date, group in value.groupby("trade_date"):
        available = group[factor_column].notna()
        coverage_rows.append({
            "trade_date": trade_date,
            "coverage": float(available.mean()),
            "available": int(available.sum()),
            "total": len(group),
        })
        ranked = group.loc[available].copy()
        if not ranked.empty:
            ranked["quantile"] = pd.qcut(
                ranked[factor_column].rank(method="first"),
                min(quantiles, len(ranked)), labels=False, duplicates="drop",
            )
            top_quantile = ranked["quantile"].max()
            top = set(ranked.loc[ranked["quantile"] == top_quantile, "symbol"].astype(str))
            if previous_top is not None and (top or previous_top):
                turnovers.append(1 - len(top & previous_top) / max(1, len(top | previous_top)))
            previous_top = top
        for label in label_columns:
            sample = group[[factor_column, label]].dropna()
            ic_rows.append({
                "trade_date": trade_date,
                "label": label,
                "ic": _correlation(sample, factor_column, label, "pearson"),
                "rank_ic": _correlation(sample, factor_column, label, "spearman"),
                "count": len(sample),
            })
            if not ranked.empty:
                for quantile, bucket in ranked.groupby("quantile"):
                    quantile_rows.append({
                        "trade_date": trade_date,
                        "label": label,
                        "quantile": int(quantile),
                        "return": _safe_mean(bucket[label]),
                        "count": int(bucket[label].notna().sum()),
                    })
    coverage = pd.DataFrame(coverage_rows)
    ic = pd.DataFrame(ic_rows)
    quantile = pd.DataFrame(quantile_rows)
    if not ic.empty:
        ic["year"] = pd.to_datetime(ic["trade_date"]).dt.year
        by_year = ic.groupby(["year", "label"], as_index=False).agg(
            ic=("ic", "mean"), rank_ic=("rank_ic", "mean"), observations=("count", "sum"),
        )
        decay = ic.groupby("label", as_index=False).agg(
            ic=("ic", "mean"), rank_ic=("rank_ic", "mean"), dates=("trade_date", "nunique"),
        )
    else:
        by_year = pd.DataFrame(columns=["year", "label", "ic", "rank_ic", "observations"])
        decay = pd.DataFrame(columns=["label", "ic", "rank_ic", "dates"])
    top_bottom = None
    if not quantile.empty:
        average = quantile.groupby(["label", "quantile"], as_index=False)["return"].mean()
        spreads = []
        for label, group in average.groupby("label"):
            ordered = group.sort_values("quantile")
            spreads.append({
                "label": label,
                "top_bottom_return": float(ordered.iloc[-1]["return"] - ordered.iloc[0]["return"]),
            })
        top_bottom = pd.DataFrame(spreads)
    else:
        average = pd.DataFrame(columns=["label", "quantile", "return"])
        top_bottom = pd.DataFrame(columns=["label", "top_bottom_return"])
    mean_turnover = float(np.mean(turnovers)) if turnovers else None
    efficiency = []
    for item in top_bottom.to_dict("records"):
        spread = item["top_bottom_return"]
        efficiency.append({
            **item,
            "mean_return_bp_per_1pct_turnover": (
                float(spread * 10_000 / (mean_turnover * 100))
                if mean_turnover and np.isfinite(spread) else None
            ),
        })
    summary = {
        "factor": factor_column,
        "mean_coverage": _safe_mean(coverage.get("coverage", pd.Series(dtype=float))),
        "mean_turnover": mean_turnover,
        "ic_decay": decay.to_dict("records"),
        "turnover_efficiency": efficiency,
    }
    return {
        "summary": summary,
        "coverage": coverage,
        "ic": ic.drop(columns=["year"], errors="ignore"),
        "ic_by_year": by_year,
        "ic_decay": decay,
        "quantile_returns": quantile,
        "average_quantile_returns": average,
    }
