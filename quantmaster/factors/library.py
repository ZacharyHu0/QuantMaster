"""内置因子库：覆盖 A 股研究中最常用的量价类因子。

每个因子附一句话说明。约定：因子值越大代表越「看好」（分析时若发现
IC 为负，直接取相反数即可得到同等强度的反向因子）。

价值/质量类因子（PE/PB/ROE 等）需要财务数据，接入 Tushare 或
akshare 财务接口后可仿照此文件用 ExpressionFactor/FuncFactor 扩展。
"""

from __future__ import annotations

from quantmaster.factors.base import ExpressionFactor, Factor

BUILTIN_FACTORS: dict[str, Factor] = {}


def _register(expr: str, name: str, description: str) -> None:
    BUILTIN_FACTORS[name] = ExpressionFactor(expr, name=name, description=description)


# ---- 动量 / 反转 ----
_register(
    "pct_change(close, 20)",
    "mom_20d",
    "20日动量：过去一个月涨幅，追涨。A股短周期动量弱、需与反转搭配。",
)
_register(
    "pct_change(close, 120) - pct_change(close, 20)",
    "mom_gap",
    "中期动量剔除近月：过去半年涨幅减去近一个月涨幅，经典动量改良。",
)
_register(
    "-pct_change(close, 5)",
    "rev_5d",
    "5日反转：近一周跌得多的股票下期倾向反弹。A股短期反转效应较显著。",
)
_register(
    "-rank(pct_change(close, 20)) * rank(ts_std(returns, 20))",
    "rev_vol",
    "高波动反转：近月跌幅大且波动高的股票反弹更强。",
)

# ---- 波动 / 风险 ----
_register(
    "-ts_std(returns, 20)",
    "low_vol_20d",
    "低波动：近月日收益波动率取负。低波动异象在A股长期有效。",
)
_register(
    "-(ts_max(high, 20) / ts_min(low, 20) - 1)",
    "low_range_20d",
    "低振幅：近月最高价/最低价区间越窄越好，与低波动互补。",
)

# ---- 量价 / 流动性 ----
_register(
    "-ts_zscore(volume, 20)",
    "vol_shrink",
    "缩量：当前成交量相对近月均值越低越好（缩量回调优于放量下跌）。",
)
_register(
    "-ts_corr(rank(volume), rank(close), 10)",
    "pv_corr",
    "量价背离：近10日量与价的秩相关取负，量价齐升的股票短期倾向回调。",
)
_register(
    "-ts_mean(turnover, 20)",
    "low_turnover",
    "低换手：近月平均换手率低代表筹码稳定，是A股经典的流动性溢价因子。",
)
_register(
    "(close - ts_min(low, 20)) / (ts_max(high, 20) - ts_min(low, 20) + 0.0001)",
    "price_pos",
    "价格位置：当前价在近月高低区间中的位置（0~1），衡量强弱。",
)

# ---- 趋势 ----
_register(
    "close / ts_mean(close, 20) - 1",
    "bias_20d",
    "20日乖离率：价格偏离月均线的幅度，正值代表强势。",
)
_register(
    "(ema(close, 12) - ema(close, 26)) / close",
    "macd_dif",
    "MACD DIF（归一化）：快慢均线差，衡量中短期趋势方向。",
)


def get_factor(name_or_expr: str) -> Factor:
    """按名称取内置因子；不是内置名称则当作表达式解析。"""
    if name_or_expr in BUILTIN_FACTORS:
        return BUILTIN_FACTORS[name_or_expr]
    return ExpressionFactor(name_or_expr)


def list_factors() -> list[dict]:
    return [
        {"name": f.name, "description": f.description,
         "expression": getattr(f, "expression", "")}
        for f in BUILTIN_FACTORS.values()
    ]
