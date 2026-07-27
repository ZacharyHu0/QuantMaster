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

# ---- Quant Lab 扩展因子：保持表达式短、逻辑可解释，作为 AI/ML 的稳定基线 ----
_register("pct_change(close, 5)", "mom_5d", "5日动量：近一周涨幅，适合短周期趋势研究。")
_register("pct_change(close, 10)", "mom_10d", "10日动量：两周价格趋势。")
_register("pct_change(close, 60)", "mom_60d", "60日动量：季度价格趋势。")
_register("pct_change(close, 120)", "mom_120d", "120日动量：半年价格趋势。")
_register("-pct_change(close, 1)", "rev_1d", "1日反转：隔夜短期价格反转。")
_register("-pct_change(close, 3)", "rev_3d", "3日反转：极短周期超跌修复。")
_register("-pct_change(close, 10)", "rev_10d", "10日反转：两周价格反转。")
_register("close / ts_mean(close, 5) - 1", "bias_5d", "5日乖离率：价格相对周均线强弱。")
_register("close / ts_mean(close, 10) - 1", "bias_10d", "10日乖离率：价格相对双周均线强弱。")
_register("close / ts_mean(close, 60) - 1", "bias_60d", "60日乖离率：价格相对季度均线强弱。")
_register("(ema(close, 5) - ema(close, 20)) / close", "ema_cross_5_20", "5/20日均线差：短趋势斜率。")
_register("(ema(close, 10) - ema(close, 60)) / close", "ema_cross_10_60", "10/60日均线差：中期趋势斜率。")
_register("ts_mean(sign(returns), 20)", "trend_consistency_20", "20日趋势一致性：上涨日占优程度。")
_register(
    "(close - ts_max(high, 20)) / (ts_max(high, 20) + 0.0001)",
    "breakout_20",
    "20日突破距离：越接近或突破前高越强。",
)
_register("-ts_std(returns, 5)", "low_vol_5d", "5日低波动：周内收益波动率取负。")
_register("-ts_std(returns, 10)", "low_vol_10d", "10日低波动：双周收益波动率取负。")
_register("-ts_std(returns, 60)", "low_vol_60d", "60日低波动：季度收益波动率取负。")
_register("-(ts_max(high, 10) / ts_min(low, 10) - 1)", "low_range_10d", "10日低振幅：双周价格区间越窄越好。")
_register("-(ts_max(high, 60) / ts_min(low, 60) - 1)", "low_range_60d", "60日低振幅：季度价格区间越窄越好。")
_register("-ts_mean(abs(returns), 20)", "mean_abs_return_20", "20日平均绝对收益取负：稳健的低波动代理。")
_register("-ts_zscore(volume, 5)", "vol_shrink_5d", "5日缩量：成交量相对短窗均值越低越好。")
_register("pct_change(ts_mean(volume, 5), 20)", "volume_momentum", "成交量动量：近周均量相对一个月前的变化。")
_register("ts_zscore(amount, 20)", "amount_shock", "成交额冲击：当前成交额相对月内历史的位置。")
_register("-ts_zscore(turnover, 10)", "turnover_shock", "换手冲击取负：异常高换手通常伴随短期拥挤。")
_register("-ts_mean(turnover, 60)", "low_turnover_60", "60日低换手：季度筹码稳定度。")
_register("-ts_corr(rank(volume), rank(close), 20)", "pv_divergence_20", "20日量价背离：量价相关取负。")
_register("-(close / vwap - 1)", "vwap_reversion", "VWAP 偏离反转：收盘相对成交均价偏离取负。")
_register("(close - open) / (high - low + 0.0001)", "intraday_strength", "日内强度：实体涨幅相对全天振幅。")
_register("-(open / delay(close, 1) - 1)", "overnight_reversal", "隔夜缺口反转：开盘缺口取负。")
_register(
    "(close - ts_min(low, 60)) / (ts_max(high, 60) - ts_min(low, 60) + 0.0001)",
    "price_pos_60",
    "60日价格位置：当前价在季度区间的位置。",
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
