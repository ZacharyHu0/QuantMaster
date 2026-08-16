"""价值 / 质量因子：由基本面面板构造的 FuncFactor 集合。

为什么用 EP（盈利收益率 = 1/PE）而不是直接用 PE 排序？
    「PE 越低越便宜」只在盈利为正时成立。盈利为负时 PE 变成负数，
    排序彻底失真：一只亏损股 PE=-5 会排在 PE=10 的公司前面，但它并不
    便宜——它在亏钱。取倒数 EP = E/P 后，「越大越便宜」在盈利为正的
    区间内单调成立；对 PE<=0（亏损）的股票直接置 NaN，让它们退出当天
    的截面排序，而不是带着错误的符号参与比较。BP = 1/PB 同理（净资产
    为负、资不抵债的公司剔除）。

为什么强调财报发布滞后？
    ROE 等季度指标以「报告期」为索引，但披露要晚 1~4 个月。构造
    fund_panel 时必须先经过 quarterly_to_daily() 的 lag_days 滞后处理
    （见 quantmaster/data/fundamentals.py），否则因子在报告期当天就用上
    了未来才公布的财报——回测收益虚高且实盘不可复制。本模块假设传入
    的 fund_panel 已完成这一步。

约定与内置因子库（library.py）一致：因子值越大代表越「看好」。因此
市值因子取 -log(总市值)——A 股长期存在小市值溢价，市值越小因子值越大。
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import numpy as np
import pandas as pd

from quantmaster.factors.base import Factor, FuncFactor, PanelDict


def _safe_inverse(df: pd.DataFrame) -> pd.DataFrame:
    """1/x；x<=0 或缺失处置 NaN（负 PE/PB 参与排序会失真，直接剔除）。"""
    return (1.0 / df).where(df > 0)


def _neg_log_mv(df: pd.DataFrame) -> pd.DataFrame:
    """-log(市值)：对数压缩市值的长尾分布，负号让「市值小 -> 因子值大」。"""
    return -np.log(df.where(df > 0))


def _identity(df: pd.DataFrame) -> pd.DataFrame:
    return df


def _make_func(
    raw: pd.DataFrame,
    transform: Callable[[pd.DataFrame], pd.DataFrame],
) -> Callable[[PanelDict], pd.DataFrame]:
    """闭包工厂：把一张基本面数据表绑定进 FuncFactor 的 func。

    func 的入参是行情面板（与其他因子接口一致），但数据来自闭包里的
    基本面表；计算时 reindex 到行情面板 close 的 index/columns——保证
    任何基本面因子的输出都与行情面板同形状，可直接进入标准化 / IC /
    分层回测流水线。行情里有而基本面里没有的日期或股票自然为 NaN。
    """

    def func(panel: PanelDict) -> pd.DataFrame:
        ref = panel["close"]
        aligned = raw.reindex(index=ref.index, columns=ref.columns).astype(float)
        return transform(aligned)

    return func


# (因子名, 依赖的基本面字段, 变换函数, 说明)
_FACTOR_SPECS: tuple[tuple[str, str, Callable[[pd.DataFrame], pd.DataFrame], str], ...] = (
    ("ep", "pe_ttm", _safe_inverse,
     "盈利收益率 1/PE(TTM)：越大越便宜；PE<=0（亏损）置 NaN 退出排序。"),
    ("bp", "pb", _safe_inverse,
     "账面市值比 1/PB：经典价值因子；PB<=0（资不抵债）置 NaN。"),
    ("dividend_yield", "dv_ratio", _identity,
     "股息率：高分红代表现金流扎实、估值偏低，红利风格核心因子。"),
    ("small_cap", "total_mv", _neg_log_mv,
     "-log(总市值)：A 股经典小市值因子，市值越小因子值越大。"),
    ("roe", "roe", _identity,
     "净资产收益率（需已含财报发布滞后）：盈利质量，越高越好。"),
)


# 全部基本面因子名（无需数据即可判断某个名字是不是基本面因子）
FUNDAMENTAL_FACTOR_NAMES: tuple[str, ...] = tuple(spec[0] for spec in _FACTOR_SPECS)
FUNDAMENTAL_FIELD_BY_FACTOR: dict[str, str] = {
    name: field for name, field, _transform, _description in _FACTOR_SPECS
}


def list_fundamental_factors() -> list[dict]:
    """基本面因子清单（含说明），供 CLI/Web 展示；不触网。"""
    return [
        {"name": name, "description": f"[基本面] {desc}", "expression": ""}
        for name, _field, _transform, desc in _FACTOR_SPECS
    ]


def resolve_factor(
    name_or_expr: str,
    symbols: list[str],
    start: str,
    end: str,
    *,
    progress=None,
    cancelled=None,
) -> Factor:
    """统一因子入口：内置量价因子 / 表达式 / 基本面因子（自动拉数）。

    基本面因子首次使用会触网拉取估值/财务数据（此后走本地缓存）；
    其余情况与 quantmaster.factors.library.get_factor 行为一致。
    """
    from quantmaster.factors.library import get_factor

    name_or_expr = name_or_expr.strip()
    if name_or_expr.startswith("artifact:"):
        from quantmaster.factors.artifact import ArtifactFactor, parse_artifact_reference

        reference = parse_artifact_reference(name_or_expr)
        if reference is None:
            raise ValueError(
                "研究产物格式应为 artifact:factor|risk|model:stock|etf|future:id@1.0.0"
            )
        return ArtifactFactor(reference)
    if name_or_expr == "news_sentiment":
        from quantmaster.ai.sentiment import NewsSentimentFactor

        return cast(Any, NewsSentimentFactor())
    if name_or_expr in FUNDAMENTAL_FACTOR_NAMES:
        from quantmaster.data.fundamentals import fundamental_panel

        # 每日估值接口会一次返回全部估值列，但季度 ROE 是另一条昂贵的 API
        # 链路。按因子声明精确加载字段，既让同一份每日指标缓存供 EP/BP/股息率/
        # 小市值复用，也避免验证这些因子时无谓地为全市场请求 ROE。
        required_field = FUNDAMENTAL_FIELD_BY_FACTOR[name_or_expr]
        fund = fundamental_panel(
            symbols,
            start,
            end,
            fields=(required_field,),
            progress=progress,
            cancelled=cancelled,
        )
        factors = make_fundamental_factors(fund)
        if name_or_expr not in factors:
            raise ValueError(
                f"基本面因子 {name_or_expr} 数据获取失败（依赖字段缺失），"
                f"可用: {sorted(factors) or '无'}"
            )
        return factors[name_or_expr]
    from quantmaster.factors.library import BUILTIN_FACTORS

    if name_or_expr in BUILTIN_FACTORS:
        return BUILTIN_FACTORS[name_or_expr]

    # Quant Lab display names are unique registry aliases. Resolving them here keeps
    # copied names and autocomplete insertions executable without exposing the raw
    # expression (whose function commas conflict with multi-factor separators).
    from quantmaster.schema_access import schema_target

    stored = schema_target("lab_store").factor_reference(name_or_expr)
    if stored is not None:
        if stored["kind"] != "expression":
            raise ValueError(
                f"Quant Lab 因子“{stored['name']}”是 {stored['kind']} 类型；"
                "请在回测工作台选择 Quant Lab OOF 版本策略并填写版本 ID"
            )
        spec = stored.get("spec") or {}
        expression = str(spec.get("expression") or "").strip()
        if not expression:
            raise ValueError(f"Quant Lab 因子“{stored['name']}”没有可执行表达式")
        from quantmaster.factors.base import ExpressionFactor

        return ExpressionFactor(
            expression, name=stored["name"], description=str(spec.get("description") or ""),
        )
    return get_factor(name_or_expr)


def make_fundamental_factors(fund_panel: dict[str, pd.DataFrame]) -> dict[str, Factor]:
    """由基本面面板（fundamental_panel 的输出）构造价值/质量因子字典。

    只为 fund_panel 中实际存在的字段建因子——例如没有 "roe" 数据就不会
    产出 roe 因子，因此可以放心传入任意字段子集。返回 {因子名: Factor}，
    每个因子的 compute(panel) 输出与行情面板 close 同形状。
    """
    factors: dict[str, Factor] = {}
    for name, field, transform, desc in _FACTOR_SPECS:
        if field not in fund_panel:
            continue
        factors[name] = FuncFactor(name, _make_func(fund_panel[field], transform), description=desc)
    return factors
