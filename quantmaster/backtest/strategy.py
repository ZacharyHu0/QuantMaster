"""策略层：把「因子/规则」转成回测引擎需要的目标权重矩阵。"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from quantmaster.factors.base import Factor, PanelDict
from quantmaster.factors.engine import compute_factor


class Strategy(ABC):
    """策略基类：输出 date × symbol 的目标权重（T 日收盘决定，T+1 开盘执行）。"""

    name: str = "strategy"

    @abstractmethod
    def target_weights(self, panel: PanelDict) -> pd.DataFrame:
        ...


def rebalance_mask(dates: pd.DatetimeIndex, freq: str = "W") -> pd.Series:
    """调仓日掩码。freq: D=每日, W=每周最后交易日, M=每月最后交易日。"""
    s = pd.Series(True, index=dates)
    if freq.upper() == "D":
        return s
    period = dates.to_period("W" if freq.upper() == "W" else "M")
    is_last = pd.Series(period, index=dates).ne(pd.Series(period, index=dates).shift(-1))
    is_last.iloc[-1] = True
    return is_last


class FactorStrategy(Strategy):
    """因子选股：调仓日按因子值从高到低取前 top_n 只，等权买入。

    cap_weight 限制单票最大权重；因子值缺失的股票不参与排名。
    """

    def __init__(
        self,
        factor: Factor,
        top_n: int = 5,
        rebalance: str = "W",
        cap_weight: float = 0.35,
        standardize: bool = True,
    ):
        self.factor = factor
        self.top_n = top_n
        self.rebalance = rebalance
        self.cap_weight = cap_weight
        self.standardize = standardize
        self.name = f"factor_{factor.name}_top{top_n}_{rebalance}"

    def target_weights(self, panel: PanelDict) -> pd.DataFrame:
        values = compute_factor(self.factor, panel, standardize=self.standardize)
        close = panel["close"]
        values = values.reindex(index=close.index, columns=close.columns)

        ranks = values.rank(axis=1, ascending=False)
        selected = (ranks <= self.top_n).astype(float).where(values.notna(), 0.0)
        counts = selected.sum(axis=1).replace(0, pd.NA)
        weights = selected.div(counts, axis=0).clip(upper=self.cap_weight).fillna(0.0)

        mask = rebalance_mask(close.index, self.rebalance)
        weights = weights.where(mask, other=float("nan"))   # 非调仓日不发信号
        return weights


class BuyAndHold(Strategy):
    """基准：首日等权买入并持有。"""

    name = "buy_and_hold"

    def target_weights(self, panel: PanelDict) -> pd.DataFrame:
        close = panel["close"]
        weights = pd.DataFrame(float("nan"), index=close.index, columns=close.columns)
        first = close.notna().any(axis=1).idxmax()
        n = close.loc[first].notna().sum()
        weights.loc[first] = close.loc[first].notna().astype(float) / max(n, 1)
        return weights
