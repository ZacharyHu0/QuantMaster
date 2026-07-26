"""因子算子库。

所有算子的输入/输出都是「面板 DataFrame」：行 = 交易日，列 = 股票。
命名沿用 WorldQuant Alpha101 的习惯：
    ts_xxx  沿时间轴的滚动计算（每只股票独立）
    rank    截面排名（同一天所有股票之间比较，输出 0~1 分位）
以 delta(close, 5) 为例：今天收盘价减去 5 天前收盘价，即 5 日涨幅（差值）。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

Panel = pd.DataFrame

EPS = 1e-12


# ---------- 时间序列算子（沿时间轴，每列独立） ----------

def delay(x: Panel, n: int) -> Panel:
    """n 天前的值。"""
    return x.shift(int(n))


def delta(x: Panel, n: int) -> Panel:
    """当前值 - n 天前的值。"""
    return x - x.shift(int(n))


def pct_change(x: Panel, n: int) -> Panel:
    """n 日收益率（涨跌幅）。"""
    return x / x.shift(int(n)) - 1.0


def ts_mean(x: Panel, n: int) -> Panel:
    return x.rolling(int(n), min_periods=max(2, int(n) // 2)).mean()


def ts_std(x: Panel, n: int) -> Panel:
    return x.rolling(int(n), min_periods=max(2, int(n) // 2)).std()


def ts_min(x: Panel, n: int) -> Panel:
    return x.rolling(int(n), min_periods=max(2, int(n) // 2)).min()


def ts_max(x: Panel, n: int) -> Panel:
    return x.rolling(int(n), min_periods=max(2, int(n) // 2)).max()


def ts_sum(x: Panel, n: int) -> Panel:
    return x.rolling(int(n), min_periods=max(2, int(n) // 2)).sum()


def ts_rank(x: Panel, n: int) -> Panel:
    """当前值在过去 n 天中的分位（0~1）。"""
    n = int(n)
    return x.rolling(n, min_periods=max(2, n // 2)).rank(pct=True)


def ts_zscore(x: Panel, n: int) -> Panel:
    """滚动标准分：(x - 均值) / 标准差。"""
    return (x - ts_mean(x, n)) / (ts_std(x, n) + EPS)


def ts_corr(x: Panel, y: Panel, n: int) -> Panel:
    """两个面板的滚动相关系数（逐列）。"""
    return x.rolling(int(n), min_periods=max(3, int(n) // 2)).corr(y)


def ema(x: Panel, n: int) -> Panel:
    return x.ewm(span=int(n), adjust=False).mean()


# ---------- 截面算子（同一天所有股票之间） ----------

def rank(x: Panel) -> Panel:
    """截面分位排名（0~1），值越大排名越高。"""
    return x.rank(axis=1, pct=True)


def zscore(x: Panel) -> Panel:
    """截面标准分。"""
    mean = x.mean(axis=1)
    std = x.std(axis=1)
    return x.sub(mean, axis=0).div(std + EPS, axis=0)


def demean(x: Panel) -> Panel:
    """截面去均值。"""
    return x.sub(x.mean(axis=1), axis=0)


def winsorize(x: Panel, k: float = 3.0) -> Panel:
    """截面缩尾：把偏离中位数超过 k 倍 MAD 的极端值压回边界，抑制离群点。"""
    med = x.median(axis=1)
    mad = (x.sub(med, axis=0)).abs().median(axis=1)
    lower = med - k * 1.4826 * mad
    upper = med + k * 1.4826 * mad
    return x.clip(lower=lower, upper=upper, axis=0)


# ---------- 逐元素算子 ----------

def log(x: Panel) -> Panel:
    return np.log(x.abs() + EPS)


def sign(x: Panel) -> Panel:
    return np.sign(x)


def abs_(x: Panel) -> Panel:
    return x.abs()


def sqrt(x: Panel) -> Panel:
    return np.sqrt(x.abs())


def power(x: Panel, a: float) -> Panel:
    return np.sign(x) * (x.abs() ** float(a))


def max_(x: Panel, y: Panel) -> Panel:
    return np.maximum(x, y)


def min_(x: Panel, y: Panel) -> Panel:
    return np.minimum(x, y)


# 表达式引擎可用的算子注册表（名字 -> (函数, 参数个数)）
OPERATORS: dict[str, tuple] = {
    "delay": (delay, 2),
    "delta": (delta, 2),
    "pct_change": (pct_change, 2),
    "ts_mean": (ts_mean, 2),
    "ts_std": (ts_std, 2),
    "ts_min": (ts_min, 2),
    "ts_max": (ts_max, 2),
    "ts_sum": (ts_sum, 2),
    "ts_rank": (ts_rank, 2),
    "ts_zscore": (ts_zscore, 2),
    "ts_corr": (ts_corr, 3),
    "ema": (ema, 2),
    "rank": (rank, 1),
    "zscore": (zscore, 1),
    "demean": (demean, 1),
    "winsorize": (winsorize, 1),
    "log": (log, 1),
    "sign": (sign, 1),
    "abs": (abs_, 1),
    "sqrt": (sqrt, 1),
    "power": (power, 2),
    "max": (max_, 2),
    "min": (min_, 2),
}
