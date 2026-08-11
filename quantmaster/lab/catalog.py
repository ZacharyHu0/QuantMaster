"""统一因子目录：42 个量价表达式 + 5 个基本面 + 1 个新闻情绪。"""

from __future__ import annotations

from quantmaster.factors.fundamental import list_fundamental_factors
from quantmaster.factors.library import list_factors
from quantmaster.lab.models import FactorSpec


def _category(name: str) -> str:
    if name.startswith(("mom_", "bias_", "ema_", "macd", "trend_", "breakout")):
        return "动量与趋势"
    if name.startswith(("rev_", "price_pos", "overnight", "intraday", "vwap")):
        return "反转与价格结构"
    if name.startswith(("low_vol", "low_range", "mean_abs")):
        return "波动与风险"
    if name.startswith(("vol_", "volume_", "amount_", "turnover", "low_turnover", "pv_")):
        return "流动性与量价"
    return "量价研究"


def curated_catalog() -> list[FactorSpec]:
    result = [
        FactorSpec(
            slug=item["name"],
            name=item["name"],
            expression=item["expression"],
            description=item["description"],
            category=_category(item["name"]),
            required_features=tuple(
                field for field in (
                    "open", "high", "low", "close", "volume", "amount", "turnover", "vwap",
                    "returns",
                ) if field in item["expression"]
            ),
            rationale=item["description"],
            tags=("builtin", "interpretable"),
        )
        for item in list_factors()
    ]
    fundamental_fields = {
        "ep": "pe_ttm", "bp": "pb", "dividend_yield": "dv_ratio",
        "small_cap": "total_mv", "roe": "roe",
    }
    for item in list_fundamental_factors():
        result.append(FactorSpec(
            slug=item["name"],
            name=item["name"],
            expression="",
            description=item["description"],
            category="基本面",
            required_features=(fundamental_fields[item["name"]],),
            rationale=item["description"],
            tags=("builtin", "point-in-time", "fundamental"),
        ))
    result.append(FactorSpec(
        slug="news_sentiment",
        name="news_sentiment",
        expression="",
        description="新闻情绪：按发布时间与股票相关性聚合，并以半衰期衰减。",
        category="情绪与事件",
        required_features=("news_sentiment",),
        rationale="结构化财经快讯可能在短周期内改变风险偏好。",
        tags=("builtin", "alternative", "publication-aligned"),
    ))
    if len(result) != 48:
        raise RuntimeError(f"精选因子目录应为 48 项，实际 {len(result)} 项")
    return result


def catalog_groups() -> list[dict]:
    counts: dict[str, int] = {}
    for factor in curated_catalog():
        counts[factor.category] = counts.get(factor.category, 0) + 1
    return [{"name": name, "count": count} for name, count in counts.items()]
