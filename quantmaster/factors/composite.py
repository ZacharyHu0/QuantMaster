"""多因子合成与正交化。

为什么不用等权合成？
    engine.combine_factors 的默认做法是等权相加，隐含两个假设：
    (1) 每个因子的预测能力一样强；(2) 每个因子的方向都是对的。
    实际上因子有强有弱、甚至会随市场风格切换而失效或反向——等权会让
    弱因子、反向因子稀释强因子的信号。

IC 加权的直觉：
    IC（信息系数）衡量「因子值与下期收益的截面相关性」，可以理解为
    因子近期的"命中率"。用滚动窗口内的 IC 均值做权重，等于把资金按
    "谁最近预测得准"来分配：强因子权重大，弱因子权重小；IC 为负的
    因子自然拿到负权重——相当于自动把它反过来用，无需人工翻转方向。
    method="icir" 进一步用 IC均值/IC标准差 做权重（类似夏普比率的思想），
    惩罚"时灵时不灵"的不稳定因子。
    关键纪律：今天使用的权重只能由昨天为止的 IC 算出（shift(1)），
    否则就是用未来数据给自己打分——典型的未来函数。

共线因子为何要看相关性 / 正交化？
    两个高度相关的因子（比如 5 日反转和 10 日反转）携带的是同一份信息。
    同时纳入等于把这份信息重复计权，组合会在该风格上不知不觉地超配，
    风格一旦反转回撤会被放大；对回归类的加权方法还会造成多重共线性、
    权重估计不稳定。两种对策：
    - factor_correlation + greedy_select：算因子两两的截面秩相关，
      贪心地只保留一组互相"不太像"且各自有效的因子；
    - orthogonalize：把新因子对已有因子回归取残差，只留下
      "老因子解释不了的增量信息"。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from quantmaster.factors.analysis import forward_returns, information_coefficient

EPS = 1e-12


def factor_correlation(values: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """因子两两「截面秩相关的时序均值」矩阵。

    对每个交易日，计算两个因子在当日全体股票上的 Spearman 秩相关
    （先 rank 再皮尔逊相关，不引入 scipy），再对时间取均值。
    比起把面板拉平后算一个大相关系数，逐日截面相关剔除了
    「两个因子整体水平同涨同跌」带来的伪相关，更贴近选股场景。

    参数:
        values: 因子名 -> 因子值面板（date × symbol）。

    返回:
        对称的 DataFrame（factor × factor），对角线为 1。
    """
    names = list(values)
    ranked = {name: values[name].rank(axis=1) for name in names}
    corr = pd.DataFrame(np.eye(len(names)), index=names, columns=names)
    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            fa, fb = ranked[a].align(ranked[b], join="inner")
            daily = fa.corrwith(fb, axis=1)
            value = float(daily.mean()) if daily.notna().any() else float("nan")
            corr.loc[a, b] = corr.loc[b, a] = value
    return corr


def ic_weighted_combine(
    values: dict[str, pd.DataFrame],
    close: pd.DataFrame,
    lookback: int = 60,
    method: str = "ic",
    min_periods: int = 20,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """动态 IC 加权合成多因子。

    流程：
    1. 对每个因子算逐日 RankIC 序列（因子值 vs 下一日收益的截面秩相关）；
    2. 取滚动 lookback 日的 IC 均值（method="icir" 时用 均值/标准差）
       作为该因子的原始权重；
    3. 权重整体 shift(1)；
    4. 按当日权重绝对值之和归一，加权求和得到合成因子。

    为什么必须 shift(1)：T 日的 IC 用到了 T→T+1 的收益，也就是要等
    T+1 日收盘才算得出来。若不 shift，T 日的合成权重就"偷看"了
    T+1 日的行情——未来函数。shift(1) 后，T 日权重只依赖 T-1 日及
    更早的 IC（最晚用到 T 日收盘价），T 日收盘后即可计算，无未来信息。

    IC 为负的因子滚动均值为负，自然获得负权重——等价于自动反向使用，
    无需人工判断因子方向。

    参数:
        values: 因子名 -> 因子值面板（应为同一标准化流水线的产物）。
        close: 收盘价面板，用于计算下一日收益。
        lookback: 滚动窗口长度（交易日）。
        method: "ic" 用滚动 IC 均值加权；"icir" 用滚动 IC均值/标准差 加权。
        min_periods: 滚动窗口最少样本数，不足则权重为 NaN（冷启动期）。

    返回:
        (合成因子面板 date × symbol, 权重历史 DataFrame date × factor)。
        冷启动期（IC 样本不足）权重与合成值均为 NaN。
    """
    if not values:
        raise ValueError("values 不能为空")
    if method not in ("ic", "icir"):
        raise ValueError(f"method 只支持 'ic' 或 'icir'，实际: {method!r}")

    fwd = forward_returns(close, periods=1)
    ic_df = pd.DataFrame(
        {name: information_coefficient(vals, fwd) for name, vals in values.items()}
    )

    rolling_mean = ic_df.rolling(lookback, min_periods=min_periods).mean()
    if method == "icir":
        rolling_std = ic_df.rolling(lookback, min_periods=min_periods).std()
        raw = rolling_mean / (rolling_std + EPS)
    else:
        raw = rolling_mean

    # 防未来函数：T 日权重只能用 T-1 日及更早的 IC（见函数 docstring）。
    # 先对齐到行情全索引再 shift：IC 序列因末日 forward return 缺失比行情短一天，
    # 若直接 shift，最后一个交易日（实盘出信号的那天）会拿不到权重。
    raw = raw.reindex(close.index).shift(1)

    # 截面归一：按绝对值之和缩放，保留正负号（负 IC -> 负权重 -> 自动反向）
    denom = raw.abs().sum(axis=1)
    weights = raw.div(denom.where(denom > 0), axis=0)

    # 合成时对「部分因子缺失」的股票按可得因子的 |权重| 重归一，
    # 避免缺失值被当 0 参与加权（否则部分覆盖的股票合成值被系统性压向 0）。
    combined: pd.DataFrame | None = None
    coverage: pd.DataFrame | None = None
    for name, vals in values.items():
        w = weights[name].reindex(vals.index)
        term = vals.fillna(0.0).mul(w, axis=0)
        combined = term if combined is None else combined.add(term, fill_value=0.0)
        cov = vals.notna().astype(float).mul(w.abs(), axis=0)
        coverage = cov if coverage is None else coverage.add(cov, fill_value=0.0)
    assert combined is not None and coverage is not None
    combined = combined / coverage.where(coverage > 0)
    return combined, weights


def orthogonalize(target: pd.DataFrame, base: pd.DataFrame) -> pd.DataFrame:
    """逐日截面把 target 对 base 回归取残差（含截距的一元 OLS）。

    每个交易日，在当日全体股票的截面上做回归
        target_i = alpha + beta * base_i + eps_i
    并返回残差 eps。残差就是「target 中不能被 base 线性解释的部分」，
    与 base 的截面相关恰好为 0。

    经典用例：小市值因子与换手率高度相关（小盘股往往换手更活跃），
    把市值因子对换手率正交化后，残差剔除了换手率能解释的部分，
    剩下"纯"市值效应——若残差仍有显著 IC，说明市值本身有增量信息，
    而不只是换手率效应换了个马甲。

    实现是向量化的：对每一行（每个交易日）用 beta = cov(t, b) / var(b)
    的解析解，pandas 按行广播计算，不逐日循环调用 np.linalg。

    参数:
        target: 待正交化的因子面板（date × symbol）。
        base: 作为回归自变量的因子面板。

    返回:
        残差面板，形状与两者对齐后的交集一致；某日某股票任一面板缺失
        则残差为 NaN。base 当日截面为常数（方差为 0）时退化为仅去均值。
    """
    t, b = target.align(base, join="inner")
    valid = t.notna() & b.notna()
    t_v = t.where(valid)
    b_v = b.where(valid)

    # 逐行（逐日）去均值；NaN 位置在两个面板中一致，均值分母对得上
    t_dm = t_v.sub(t_v.mean(axis=1), axis=0)
    b_dm = b_v.sub(b_v.mean(axis=1), axis=0)

    cov = (t_dm * b_dm).mean(axis=1)
    var = (b_dm**2).mean(axis=1)
    beta = cov / (var + EPS)

    # 残差 = 去均值后的 target - beta * 去均值后的 base（截距已由去均值吸收）
    return t_dm.sub(b_dm.mul(beta, axis=0))


def greedy_select(
    values: dict[str, pd.DataFrame],
    close: pd.DataFrame,
    max_corr: float = 0.6,
    top_k: int = 5,
) -> list[str]:
    """按 |RankIC 均值| 降序贪心挑选一组低相关因子。

    步骤：
    1. 对每个因子算逐日 RankIC 序列的均值，按绝对值降序排序
       （负 IC 因子反着用同样有价值，故看绝对值）；
    2. 依次考察：若候选与任一已选因子的截面秩相关（factor_correlation）
       绝对值超过 max_corr，视为信息重复，跳过；
    3. 最多选出 top_k 个。

    贪心不保证全局最优，但直觉清晰、结果稳定，是多因子入库前
    去冗余的常用做法。

    参数:
        values: 因子名 -> 因子值面板。
        close: 收盘价面板，用于计算下一日收益与 RankIC。
        max_corr: 允许的两两相关性绝对值上限。
        top_k: 最多选出的因子个数。

    返回:
        入选因子名列表（按挑选顺序，即 |IC| 从大到小）。
    """
    if not values:
        return []
    fwd = forward_returns(close, periods=1)
    ic_means: dict[str, float] = {}
    for name, vals in values.items():
        mean = float(information_coefficient(vals, fwd).mean())
        if not np.isnan(mean):
            ic_means[name] = mean

    corr = factor_correlation(values)
    selected: list[str] = []
    for name in sorted(ic_means, key=lambda n: abs(ic_means[n]), reverse=True):
        if len(selected) >= top_k:
            break
        redundant = any(abs(float(corr.loc[name, chosen])) > max_corr for chosen in selected)
        if not redundant:
            selected.append(name)
    return selected
