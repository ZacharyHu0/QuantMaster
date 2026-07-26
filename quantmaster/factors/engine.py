"""因子计算引擎：批量计算 + 标准化流水线。"""

from __future__ import annotations

import pandas as pd

from quantmaster.factors.base import Factor, PanelDict
from quantmaster.factors import ops


def compute_factor(
    factor: Factor,
    panel: PanelDict,
    standardize: bool = True,
) -> pd.DataFrame:
    """计算单个因子。

    standardize=True 时执行标准流水线：截面缩尾 -> 截面标准分，
    使不同因子可比、可加权合成。
    """
    values = factor.compute(panel)
    if standardize:
        values = ops.zscore(ops.winsorize(values))
    return values


def compute_factors(
    factors: list[Factor],
    panel: PanelDict,
    standardize: bool = True,
) -> dict[str, pd.DataFrame]:
    return {f.name: compute_factor(f, panel, standardize=standardize) for f in factors}


def combine_factors(
    factor_values: dict[str, pd.DataFrame],
    weights: dict[str, float] | None = None,
) -> pd.DataFrame:
    """多因子加权合成（输入应为标准化后的因子值）。"""
    weights = weights or {name: 1.0 for name in factor_values}
    total: pd.DataFrame | None = None
    for name, values in factor_values.items():
        w = weights.get(name, 0.0)
        term = values * w
        total = term if total is None else total.add(term, fill_value=0.0)
    assert total is not None, "factor_values 不能为空"
    return total
