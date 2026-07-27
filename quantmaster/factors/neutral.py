"""因子行业中性化。

为什么要中性化（面向本科水平读者）：
很多"有效因子"其实只是在押注某个行业。例如低波动因子长期偏好银行/公用
事业，小市值因子天然回避大金融——这时 IC 里混着行业贝塔，分层收益里
混着行业行情。行业中性化把因子值在每个行业内部去均值：之后"看好某股票"
的含义变成"在它所属行业里相对看好"，行业押注被剔除，剩下的才更接近
真正的选股 alpha。

用法：
    from quantmaster.data.industry import load_industry_map
    from quantmaster.factors.neutral import industry_neutralize

    neutral = industry_neutralize(values, load_industry_map())
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def industry_neutralize(
    values: pd.DataFrame,
    industry_map: dict[str, str],
    min_members: int = 2,
) -> pd.DataFrame:
    """逐日在行业内部去均值（行业均值按当日可得成员计算）。

    - 行业成员数 < min_members 的行业不做调整（自身减自身会把信息抹成 0）；
    - 映射里没有的股票保持原值，并汇总告警一次；
    - 输入应为原始或标准化后的因子面板（date × symbol）。
    """
    if not industry_map:
        logger.warning("行业映射为空，industry_neutralize 原样返回")
        return values

    result = values.copy()
    unmapped = [s for s in values.columns if s not in industry_map]
    if unmapped:
        logger.warning("以下 %d 只股票缺少行业映射，保持原值: %s%s",
                       len(unmapped), ", ".join(unmapped[:5]),
                       " ..." if len(unmapped) > 5 else "")

    groups: dict[str, list[str]] = {}
    for symbol in values.columns:
        industry = industry_map.get(symbol)
        if industry is not None:
            groups.setdefault(industry, []).append(symbol)

    for members in groups.values():
        if len(members) < min_members:
            continue
        block = values[members]
        result[members] = block.sub(block.mean(axis=1), axis=0)
    return result
