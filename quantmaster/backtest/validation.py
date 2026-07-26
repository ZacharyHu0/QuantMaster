"""样本外验证：对抗量化研究中最大的敌人——过拟合。

为什么需要样本外验证（面向本科水平读者）：

回测收益好 ≠ 因子真的有效。在同一段历史上反复调参、筛因子，本质上是
在「记忆」这段历史的噪声——统计上叫**过拟合**（overfitting），业内叫
「回测过度」（backtest overfitting）。参数试得越多，碰巧好看的组合就
越多，但那些「碰巧」在未来大概率失效。

对策是把时间轴切开，只用一部分做研究：

- **样本内 / 样本外**（IS / OOS）：在 split 日期之前研究因子（样本内），
  再看它在 split 之后「没见过的数据」上是否依然有效（样本外）。样本外
  IC 相对样本内保留得越多，因子越可信；符号都反了则基本可判失效。
- **滚动前推**（walk-forward）：把时间均分成若干段逐段检验，看因子
  表现是否稳定——一个只在某一段有效的因子，很可能只是踩中了当时的
  风格行情。
- **网格搜索要克制**：``grid_search`` 提供参数网格便于系统性比较，但
  切记「网格越大、最优组合越不可信」（多重检验问题）。正确用法是在
  样本内网格搜索、再拿最优组合去样本外验证，而不是直接汇报网格里
  最好看的那条回测曲线。

时间对齐约定（无未来函数）：因子值只用当日及更早数据；IC 用「T 日因子
对 T→T+periods 的未来收益」。切分样本时，样本内末尾 periods 天的 IC
依赖 split 之后的价格，为严格隔离已将其从样本内剔除（见代码注释）。
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from quantmaster.backtest.engine import BacktestConfig, run_backtest
from quantmaster.backtest.strategy import FactorStrategy
from quantmaster.factors.analysis import forward_returns, information_coefficient
from quantmaster.factors.base import Factor, PanelDict
from quantmaster.factors.library import get_factor


def _ic_series(factor: Factor, panel: PanelDict, periods: int = 1) -> pd.Series:
    """整段时间轴上的逐日 RankIC 序列。

    因子在全量面板上计算是安全的：时序算子只回看历史、截面算子只用当日
    数据，因此「样本内」日期的因子值不会泄漏任何 split 之后的信息。
    IC 本身不受单调变换影响，这里直接用原始因子值（不做标准化）。
    """
    values = factor.compute(panel)
    fwd = forward_returns(panel["close"], periods=periods)
    return information_coefficient(values, fwd)


def _segment_stats(ic: pd.Series) -> tuple[float, float]:
    """一段 IC 序列的 (均值, ICIR)。"""
    mean = float(ic.mean())
    std = float(ic.std())
    icir = mean / std if std > 0 else 0.0
    return mean, icir


def train_test_ic(
    factor: Factor,
    panel: PanelDict,
    split: str,
    periods: int = 1,
) -> dict:
    """样本内/样本外 IC 对比：split 日期前为样本内（IS），之后为样本外（OOS）。

    输出关键字段：
    - degradation = 1 - |oos_ic| / |is_ic|，衡量样本外衰减了多少
      （0 表示完全保持，1 表示彻底消失；is_ic 为 0 时置 None）。
    - verdict：样本外 IC 保持 >60% => "稳健"，30%~60% => "衰减"，
      更低 => "疑似过拟合"；样本外与样本内符号相反 => "失效"。
    """
    ic = _ic_series(factor, panel, periods=periods)
    if ic.empty:
        raise ValueError("IC 序列为空：面板数据不足以计算该因子")

    split_ts = pd.Timestamp(split)
    is_ic = ic[ic.index < split_ts]
    oos_ic = ic[ic.index >= split_ts]
    # 样本内末尾 periods 天的 IC 用到 split 之后的价格（未来收益跨越了
    # 分界线），为严格隔离样本外信息，将这段「边界重叠」从样本内剔除。
    if periods > 0 and len(is_ic) > periods:
        is_ic = is_ic.iloc[:-periods]
    if is_ic.empty or oos_ic.empty:
        raise ValueError(
            f"split={split} 使样本内/样本外为空（IC 范围 "
            f"{ic.index[0].date()} ~ {ic.index[-1].date()}）"
        )

    is_mean, is_icir = _segment_stats(is_ic)
    oos_mean, oos_icir = _segment_stats(oos_ic)

    if is_mean == 0.0:
        degradation = None
        verdict = "疑似过拟合"   # 样本内本就无效，谈不上样本外保持
    else:
        retention = abs(oos_mean) / abs(is_mean)
        degradation = 1.0 - retention
        if is_mean * oos_mean < 0:
            verdict = "失效"
        elif retention > 0.6:
            verdict = "稳健"
        elif retention >= 0.3:
            verdict = "衰减"
        else:
            verdict = "疑似过拟合"

    return {
        "factor": factor.name,
        "split": str(split),
        "is_ic": round(is_mean, 4),
        "is_icir": round(is_icir, 3),
        "is_days": len(is_ic),
        "oos_ic": round(oos_mean, 4),
        "oos_icir": round(oos_icir, 3),
        "oos_days": len(oos_ic),
        "degradation": round(degradation, 4) if degradation is not None else None,
        "verdict": verdict,
    }


def walk_forward_ic(
    factor: Factor,
    panel: PanelDict,
    n_splits: int = 4,
    periods: int = 1,
) -> pd.DataFrame:
    """滚动前推检验：把 IC 序列的时间轴均分为 n_splits 段，逐段统计。

    返回 DataFrame（每段一行，共 n_splits 行）：start/end（段起止日期）、
    days、ic_mean、icir。各段 IC 均值同号且量级接近 => 因子稳定；
    只有个别段显著 => 很可能只是踩中了那段行情。
    """
    if n_splits < 1:
        raise ValueError("n_splits 必须 >= 1")
    ic = _ic_series(factor, panel, periods=periods)

    rows = []
    for i, chunk_idx in enumerate(np.array_split(np.arange(len(ic)), n_splits), start=1):
        chunk = ic.iloc[chunk_idx]
        # 剔除段尾 periods 天：其未来收益跨越段边界，会让相邻段共享信息
        if i < n_splits and periods > 0 and len(chunk) > periods:
            chunk = chunk.iloc[:-periods]
        if chunk.empty:
            rows.append({"segment": i, "start": None, "end": None,
                         "days": 0, "ic_mean": np.nan, "icir": np.nan})
            continue
        mean, icir = _segment_stats(chunk)
        rows.append({
            "segment": i,
            "start": str(chunk.index[0].date()),
            "end": str(chunk.index[-1].date()),
            "days": len(chunk),
            "ic_mean": round(mean, 4),
            "icir": round(icir, 3),
        })
    return pd.DataFrame(rows).set_index("segment")


def grid_search(
    panel: PanelDict,
    factor_names: list[str],
    top_ns: list[int],
    rebalances: list[str],
    metric: str = "sharpe",
    benchmark_close: pd.Series | None = None,
    config: BacktestConfig | None = None,
) -> pd.DataFrame:
    """参数网格搜索：对（因子 × 持仓数 × 调仓频率）全组合各跑一次回测。

    因子名经 :func:`quantmaster.factors.library.get_factor` 解析（内置因子名
    或表达式均可）。返回按 metric 降序排序的 DataFrame，列含
    factor / top_n / rebalance / annual_return / sharpe / max_drawdown / calmar。
    某个组合失败（如表达式非法、数据不足）时记 NaN 行，不中断整个网格。

    注意：网格搜出的「最优」参数只是样本内最优，务必再做样本外验证
    （见模块 docstring 关于多重检验的讨论）。
    """
    metric_columns = ("annual_return", "sharpe", "max_drawdown", "calmar")
    if metric not in metric_columns:
        raise ValueError(f"metric 必须是 {metric_columns} 之一，收到 {metric!r}")

    rows = []
    for name in factor_names:
        for top_n in top_ns:
            for rebalance in rebalances:
                row: dict = {"factor": name, "top_n": top_n, "rebalance": rebalance,
                             **{col: np.nan for col in metric_columns}}
                try:
                    strategy = FactorStrategy(get_factor(name), top_n=top_n, rebalance=rebalance)
                    result = run_backtest(
                        panel, strategy.target_weights(panel),
                        config=config, benchmark_close=benchmark_close,
                    )
                    for col in metric_columns:
                        row[col] = float(result.metrics.get(col, np.nan))
                except Exception as e:   # 单组合失败记 NaN，不中断网格
                    logging.getLogger(__name__).warning(
                        "网格组合失败 factor=%s top_n=%s rebalance=%s: %s",
                        name, top_n, rebalance, e)
                rows.append(row)

    df = pd.DataFrame(rows)
    # 排序方向按指标极性：max_drawdown 越小越好，其余越大越好
    ascending = metric == "max_drawdown"
    return df.sort_values(metric, ascending=ascending, na_position="last").reset_index(drop=True)
