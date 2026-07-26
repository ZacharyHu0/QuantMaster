"""因子有效性分析：IC / 分层回测 / 多空组合。

几个核心概念（面向本科水平读者）：

- IC（信息系数）：某天的因子值与「下一期收益」的截面相关系数。
  IC > 0 表示因子值大的股票下期涨得多。常用 Spearman 秩相关（RankIC），
  对极端值更稳健。经验上 |IC均值| > 0.03 就值得关注，> 0.05 算好因子。

- ICIR：IC均值 / IC标准差，衡量因子稳定性（类似夏普比率的思想）。

- 分层回测：每天按因子值把股票分成 N 组（Q1 最低 … QN 最高），
  分别计算每组的等权收益。有效因子应呈现「单调性」：组序越高收益越高。

- 多空组合：做多最高组、做空最低组的收益，剔除市场整体涨跌的影响
  （A股难以做空个股，多空收益主要用于评价因子本身，实盘常用纯多头）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd


@dataclass
class FactorReport:
    name: str
    ic_mean: float
    ic_std: float
    icir: float
    ic_positive_ratio: float          # IC > 0 的天数占比
    ic_series: pd.Series = field(repr=False)
    quantile_returns: pd.DataFrame = field(repr=False)   # 各分组的累计净值
    quantile_annual: dict = field(default_factory=dict)  # 各分组年化收益
    long_short_annual: float = 0.0
    monotonicity: float = 0.0         # 分组年化收益与组序的相关系数（越接近1单调性越好）
    turnover: float = 0.0             # 最高分组的日均换手率

    def summary(self) -> dict:
        return {
            "name": self.name,
            "ic_mean": round(self.ic_mean, 4),
            "ic_std": round(self.ic_std, 4),
            "icir": round(self.icir, 3),
            "ic_positive_ratio": round(self.ic_positive_ratio, 3),
            "long_short_annual": round(self.long_short_annual, 4),
            "monotonicity": round(self.monotonicity, 3),
            "top_quantile_turnover": round(self.turnover, 3),
            "quantile_annual": {k: round(v, 4) for k, v in self.quantile_annual.items()},
        }


def forward_returns(close: pd.DataFrame, periods: int = 1) -> pd.DataFrame:
    """未来 periods 日收益（分析用，行索引对齐到「当天」）。"""
    return close.shift(-periods) / close - 1.0


def information_coefficient(
    factor_values: pd.DataFrame,
    fwd_returns: pd.DataFrame,
    method: str = "spearman",
) -> pd.Series:
    """逐日截面 IC 序列。

    spearman（RankIC）通过「先秩变换、再皮尔逊相关」实现，与
    scipy.stats.spearmanr 等价，但不引入 scipy 依赖。
    """
    aligned_factor, aligned_ret = factor_values.align(fwd_returns, join="inner")
    if method == "spearman":
        aligned_factor = aligned_factor.rank(axis=1)
        aligned_ret = aligned_ret.rank(axis=1)
    ic = aligned_factor.corrwith(aligned_ret, axis=1)
    return ic.dropna()


def quantile_backtest(
    factor_values: pd.DataFrame,
    close: pd.DataFrame,
    quantiles: int = 5,
    periods: int = 1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """分层回测。

    返回 (每组逐日收益, 每组累计净值)。分组用 T 日因子值，收益取 T+1 日起
    的未来收益（因子已隐含 shift 对齐，无未来函数）。
    """
    fwd = forward_returns(close, periods=periods)
    factor_aligned, fwd_aligned = factor_values.align(fwd, join="inner")

    labels = factor_aligned.rank(axis=1, pct=True)
    group_returns = {}
    for q in range(quantiles):
        lo, hi = q / quantiles, (q + 1) / quantiles
        mask = (labels > lo) & (labels <= hi) if q > 0 else (labels <= hi)
        group_returns[f"Q{q + 1}"] = fwd_aligned.where(mask).mean(axis=1) / periods

    daily = pd.DataFrame(group_returns).dropna(how="all")
    nav = (1 + daily.fillna(0)).cumprod()
    return daily, nav


def top_quantile_turnover(factor_values: pd.DataFrame, quantiles: int = 5) -> float:
    """最高分组的成分变动率（日均）。换手过高的因子交易成本会吃掉收益。"""
    labels = factor_values.rank(axis=1, pct=True)
    top = labels > (1 - 1 / quantiles)
    prev = top.shift(1).fillna(False)
    changed = (top & ~prev).sum(axis=1)
    size = top.sum(axis=1).replace(0, np.nan)
    return float((changed / size).mean())


def annualize(daily_returns: pd.Series, trading_days: int = 244) -> float:
    """由日收益序列计算年化收益（A股每年约244个交易日）。"""
    clean = daily_returns.dropna()
    if clean.empty:
        return 0.0
    total = float((1 + clean).prod())
    if total <= 0:
        return -1.0
    return total ** (trading_days / len(clean)) - 1.0


def analyze_factor(
    factor_values: pd.DataFrame,
    close: pd.DataFrame,
    name: str = "factor",
    quantiles: int = 5,
    periods: int = 1,
) -> FactorReport:
    """一站式因子体检：IC、ICIR、分层、多空、单调性、换手。"""
    fwd = forward_returns(close, periods=periods)
    ic = information_coefficient(factor_values, fwd)
    daily, nav = quantile_backtest(factor_values, close, quantiles=quantiles, periods=periods)

    quantile_annual = {col: annualize(daily[col]) for col in daily.columns}
    long_short = daily[f"Q{quantiles}"] - daily["Q1"]

    order = np.arange(1, quantiles + 1, dtype=float)
    annual_values = np.array([quantile_annual[f"Q{q}"] for q in range(1, quantiles + 1)])
    if np.std(annual_values) > 0:
        monotonicity = float(np.corrcoef(order, annual_values)[0, 1])
    else:
        monotonicity = 0.0

    return FactorReport(
        name=name,
        ic_mean=float(ic.mean()),
        ic_std=float(ic.std()),
        icir=float(ic.mean() / ic.std()) if ic.std() > 0 else 0.0,
        ic_positive_ratio=float((ic > 0).mean()),
        ic_series=ic,
        quantile_returns=nav,
        quantile_annual=quantile_annual,
        long_short_annual=annualize(long_short),
        monotonicity=monotonicity,
        turnover=top_quantile_turnover(factor_values, quantiles=quantiles),
    )
