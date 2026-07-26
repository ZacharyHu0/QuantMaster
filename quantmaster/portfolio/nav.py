"""实盘账本每日净值重建：逐日现金/市值/盈亏，以及时间加权净值（TWR）。

为什么账户总资产的涨跌不能直接当作「收益率曲线」（本科水平即可理解）：
你今天往账户里转入 5 万元，总资产立刻多了 5 万，但这不是「赚来的」；
明天取走 2 万，总资产少 2 万，也不是「亏掉的」。想衡量策略本身的
好坏，必须把这些「外部出入金」从收益里剔除——这就是时间加权收益
（Time-Weighted Return，TWR）的出发点。

TWR 的逐日剔除法（本模块的做法）：
1. 把整个持有期切成一天一天的小段；
2. 每天算一个「纯投资收益率」::

       r_t = (assets_t - flow_t) / assets_{t-1} - 1

   其中 assets_t 是当日收盘后的总资产（现金 + 持仓市值），flow_t 是
   当日的外部净流入（入金为正、出金为负）。分子先把当天转进来的钱
   减掉（约定出入金发生在当日收盘、不参与当日盈亏），剩下的变化才
   是投资本身赚/亏的；分母是昨天收盘的总资产，即这笔收益的本金。
3. 把每天的 (1 + r_t) 连乘起来，得到起点为 1.0 的净值曲线::

       twr_nav_t = twr_nav_{t-1} * (1 + r_t)

   这样无论你何时入金/出金、金额多大，净值曲线只反映投资能力，
   可以直接与沪深300 等基准指数的归一净值放在同一张图上对比。

两个容易混淆的口径：
- 分红不是外部流入：分红是持仓「生出来的钱」，属于投资收益的一部分，
  所以计入 cash 和 pnl，但不计入 flow_t——它会体现在 r_t 里。
- pnl（累计盈亏）= 总资产 - 净投入（累计入金 - 累计出金），衡量
  「你的钱」赚了多少绝对金额；TWR 衡量「你的策略」的相对表现。
  两者可能方向相反：先亏后赚但资金大头在亏损期，pnl 可能为负而
  TWR 为正。

估值口径与 performance.py 保持一致：
    cash = 入金 - 出金 + 分红 + 卖出净额 - 买入总额 - 交易费用
持仓按当日收盘价估值；停牌/缺价日用「最近一个可得价」前向填充
（ffill 只用过去的价格，绝不用未来价格回填，避免未来函数）。
"""

from __future__ import annotations

import pandas as pd

from quantmaster.portfolio.ledger import Ledger

# 年化用的交易日数，与 factors.analysis.annualize 保持一致（A股每年约244个交易日）
TRADING_DAYS = 244

_NAV_COLUMNS = ["cash", "position_value", "total_assets", "net_invested", "pnl", "twr_nav"]


def _empty_nav() -> pd.DataFrame:
    """空账本对应的空结果（保留列结构，方便调用方统一处理）。"""
    return pd.DataFrame(columns=_NAV_COLUMNS, index=pd.DatetimeIndex([], name="date"))


def _daily_sum(values: pd.Series, dates: pd.Series, index: pd.DatetimeIndex) -> pd.Series:
    """把逐笔金额按日期汇总，并对齐到完整日历（无记录的日子为 0）。"""
    if values.empty:
        return pd.Series(0.0, index=index)
    summed = values.groupby(dates).sum()
    return summed.reindex(index, fill_value=0.0).astype(float)


def daily_nav(ledger: Ledger, prices: pd.DataFrame, end: str | None = None) -> pd.DataFrame:
    """从账本逐日重建净值曲线。

    参数：
        ledger: 实盘账本（trades + cashflows）。
        prices: date × symbol 的收盘价面板，由调用方提供（离线测试直接
                传合成数据；生产环境由上层从 BarStore 组装）。
        end:    截止日期 YYYY-MM-DD；默认取价格面板与账本事件的最后日期。

    返回 DataFrame（index 为日期），列：
        cash            现金余额（入金-出金+分红+卖出净额-买入总额-费用）
        position_value  持仓市值（当日收盘估值，缺价用最近可得价 ffill）
        total_assets    总资产 = cash + position_value
        net_invested    净投入 = 累计入金 - 累计出金
        pnl             累计盈亏 = total_assets - net_invested
        twr_nav         时间加权净值（起点 1.0，剔除出入金影响，见模块 docstring）
    """
    trades = ledger.trades()
    cashflows = ledger.cashflows()
    if trades.empty and cashflows.empty:
        return _empty_nav()

    trades = trades.copy()
    cashflows = cashflows.copy()
    if len(trades):
        trades["date"] = pd.to_datetime(trades["date"])
    if len(cashflows):
        cashflows["date"] = pd.to_datetime(cashflows["date"])

    # ---- 日历：账本事件日 ∪ 价格日，从最早事件起、到 end 截止 ----
    event_dates = pd.DatetimeIndex(
        sorted(set(trades["date"].tolist()) | set(cashflows["date"].tolist()))
    )
    price_index = pd.DatetimeIndex(pd.to_datetime(prices.index)) if len(prices) else pd.DatetimeIndex([])
    start_ts = event_dates.min()
    if end is not None:
        end_ts = pd.Timestamp(end)
    else:
        end_ts = max([event_dates.max(), *([price_index.max()] if len(price_index) else [])])
    index = price_index.union(event_dates)
    index = index[(index >= start_ts) & (index <= end_ts)]
    if len(index) == 0:
        return _empty_nav()
    index.name = "date"

    # 截止日之后的记录不参与重建
    trades = trades[trades["date"] <= end_ts]
    cashflows = cashflows[cashflows["date"] <= end_ts]

    # ---- 现金流按日汇总（账本里 amount 均存为正数，方向由 kind 决定）----
    def _flow(kind: str) -> pd.Series:
        sub = cashflows[cashflows["kind"] == kind] if len(cashflows) else cashflows
        return _daily_sum(sub["amount"] if len(sub) else pd.Series(dtype=float),
                          sub["date"] if len(sub) else pd.Series(dtype=object), index)

    deposit = _flow("deposit")
    withdraw = _flow("withdraw")
    dividend = _flow("dividend")

    buys = trades[trades["side"] == "buy"] if len(trades) else trades
    sells = trades[trades["side"] == "sell"] if len(trades) else trades
    buy_cash = _daily_sum(buys["price"] * buys["shares"] if len(buys) else pd.Series(dtype=float),
                          buys["date"] if len(buys) else pd.Series(dtype=object), index)
    sell_cash = _daily_sum(sells["price"] * sells["shares"] if len(sells) else pd.Series(dtype=float),
                           sells["date"] if len(sells) else pd.Series(dtype=object), index)
    fees = _daily_sum(trades["fee"] if len(trades) else pd.Series(dtype=float),
                      trades["date"] if len(trades) else pd.Series(dtype=object), index)

    # cash 口径与 performance.ledger_report 一致
    cash = (deposit - withdraw + dividend + sell_cash - buy_cash - fees).cumsum()

    # ---- 持仓股数：买入为正、卖出为负，逐日累加 ----
    if len(trades):
        signed = trades["shares"].where(trades["side"] == "buy", -trades["shares"])
        holdings = (
            pd.DataFrame({"date": trades["date"], "symbol": trades["symbol"], "shares": signed})
            .pivot_table(index="date", columns="symbol", values="shares", aggfunc="sum")
            .reindex(index, fill_value=0.0)
            .fillna(0.0)
            .cumsum()
        )
    else:
        holdings = pd.DataFrame(index=index)

    # ---- 估值价格：收盘价 ffill；仍缺价的（如买入早于首个价格日）退回最近成交价 ----
    # 两处填充都只向「过去」看（ffill），绝不用未来价格回填，避免未来函数。
    symbols = list(holdings.columns)
    if symbols:
        close = prices.copy()
        close.index = pd.to_datetime(close.index)
        close = close.reindex(index=index, columns=symbols).ffill()
        trade_px = (
            pd.DataFrame({"date": trades["date"], "symbol": trades["symbol"], "price": trades["price"]})
            .pivot_table(index="date", columns="symbol", values="price", aggfunc="last")
            .reindex(index=index, columns=symbols)
            .ffill()
        )
        value_px = close.fillna(trade_px)
        # 有持仓却完全无价的情况理论上不存在（有持仓必有成交价），保底按 0 估值
        position_value = (holdings * value_px).where(holdings != 0.0, 0.0).fillna(0.0).sum(axis=1)
    else:
        position_value = pd.Series(0.0, index=index)

    total_assets = cash + position_value
    net_invested = (deposit - withdraw).cumsum()
    pnl = total_assets - net_invested

    # ---- TWR：逐日剔除外部出入金（分红不算外部流入，留在收益里）----
    external_flow = deposit - withdraw
    prev_assets = total_assets.shift(1).fillna(0.0)
    daily_return = pd.Series(0.0, index=index)
    # 昨日资产为 0（含当日才首次入金）时无「本金」可言，该日收益记 0
    has_base = prev_assets > 1e-9
    daily_return[has_base] = (
        (total_assets[has_base] - external_flow[has_base]) / prev_assets[has_base] - 1.0
    )
    twr_nav = (1.0 + daily_return).cumprod()

    return pd.DataFrame(
        {
            "cash": cash,
            "position_value": position_value,
            "total_assets": total_assets,
            "net_invested": net_invested,
            "pnl": pnl,
            "twr_nav": twr_nav,
        },
        index=index,
    )


def _annualize_ratio(total_ratio: float, n_days: int) -> float:
    """由期末/期初净值比与交易日数计算年化收益率。"""
    if n_days <= 0:
        return 0.0
    if total_ratio <= 0:
        return -1.0
    return float(total_ratio ** (TRADING_DAYS / n_days) - 1.0)


def nav_with_benchmark(nav_df: pd.DataFrame, benchmark_close: pd.Series) -> dict:
    """TWR 净值与基准指数归一净值对齐，输出可 JSON 序列化的对比数据。

    两条曲线都在「共同日期区间」的首日归一到 1.0，之后的相对高低即为
    超额表现。excess_annual = 组合年化收益 - 基准年化收益（同区间）。

    返回：{"dates": [...], "twr": [...], "benchmark": [...], "excess_annual": float}
    """
    empty: dict = {"dates": [], "twr": [], "benchmark": [], "excess_annual": 0.0}
    if nav_df.empty or benchmark_close.empty or "twr_nav" not in nav_df.columns:
        return empty

    bench = benchmark_close.copy()
    bench.index = pd.to_datetime(bench.index)
    twr = nav_df["twr_nav"].copy()
    twr.index = pd.to_datetime(twr.index)

    aligned = pd.concat([twr.rename("twr"), bench.rename("bench")], axis=1, join="inner").dropna()
    if aligned.empty or aligned["twr"].iloc[0] <= 0 or aligned["bench"].iloc[0] <= 0:
        return empty

    twr_norm = aligned["twr"] / aligned["twr"].iloc[0]
    bench_norm = aligned["bench"] / aligned["bench"].iloc[0]

    n_days = len(aligned) - 1  # 日收益个数 = 观测点数 - 1
    excess_annual = _annualize_ratio(float(twr_norm.iloc[-1]), n_days) - _annualize_ratio(
        float(bench_norm.iloc[-1]), n_days
    )

    return {
        "dates": [d.strftime("%Y-%m-%d") for d in aligned.index],
        "twr": [round(float(v), 6) for v in twr_norm],
        "benchmark": [round(float(v), 6) for v in bench_norm],
        "excess_annual": round(float(excess_annual), 6),
    }
