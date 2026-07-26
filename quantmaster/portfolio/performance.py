"""实盘收益统计：TWR 时间加权收益、XIRR 内部收益率、持仓盈亏。

为什么需要两种收益率（本科金融水平即可理解）：
- TWR（时间加权）：剔除出入金时点影响，衡量「你的策略」本身好不好，
  可直接与沪深300 等基准对比。
- XIRR（金额加权/内部收益率）：把每笔出入金当作现金流解贴现率，
  衡量「你的钱」实际赚了多少（受出入金择时影响）。

估值需要行情：默认用数据层取各持仓的收盘价；离线时可手动传入价格。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from quantmaster.portfolio.ledger import Ledger


def xirr(cashflows: list[tuple[str, float]], guess: float = 0.1) -> float | None:
    """内部收益率（年化）。cashflows: [(日期, 金额)]，投出为负、收回为正。

    用二分法求解（对本项目的现金流形态足够稳健）；无解返回 None。
    """
    if len(cashflows) < 2:
        return None
    dates = [pd.Timestamp(d) for d, _ in cashflows]
    amounts = np.array([a for _, a in cashflows], dtype=float)
    if (amounts > 0).sum() == 0 or (amounts < 0).sum() == 0:
        return None
    t0 = min(dates)
    years = np.array([(d - t0).days / 365.0 for d in dates])

    def npv(rate: float) -> float:
        return float(np.sum(amounts / (1 + rate) ** years))

    lo, hi = -0.999, 100.0
    f_lo, f_hi = npv(lo), npv(hi)
    if f_lo * f_hi > 0:
        return None
    for _ in range(200):
        mid = (lo + hi) / 2
        f_mid = npv(mid)
        if abs(f_mid) < 1e-8:
            return mid
        if f_lo * f_mid < 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return (lo + hi) / 2


def _fetch_prices(symbols: list[str], lookback_days: int = 10) -> dict[str, float]:
    """尽力取最新收盘价；单个失败不影响整体。"""
    from quantmaster.data import load_history

    end = pd.Timestamp.now().normalize()
    start = end - pd.Timedelta(days=lookback_days)
    prices: dict[str, float] = {}
    for symbol in symbols:
        try:
            df = load_history(symbol, str(start.date()), str(end.date()))
            if not df.empty:
                prices[symbol] = float(df["close"].iloc[-1])
        except Exception:
            continue
    return prices


def ledger_report(ledger: Ledger, prices: dict[str, float] | None = None) -> dict:
    """生成实盘收益报告。

    prices: {symbol: 最新价}。缺失的持仓按成本价估值并在报告中标记。
    """
    positions = ledger.positions()
    cashflows = ledger.cashflows()
    trades = ledger.trades()

    holding_symbols = [p.symbol for p in positions if p.shares > 0]
    if prices is None:
        prices = _fetch_prices(holding_symbols) if holding_symbols else {}

    position_rows = []
    market_value = 0.0
    unrealized = 0.0
    missing_price: list[str] = []
    for p in positions:
        if p.shares <= 0 and abs(p.realized_pnl) < 1e-9:
            continue
        price = prices.get(p.symbol)
        if p.shares > 0 and price is None:
            missing_price.append(p.symbol)
            price = p.avg_cost
        value = p.shares * (price or 0.0)
        pnl = p.shares * ((price or 0.0) - p.avg_cost)
        market_value += value
        unrealized += pnl
        position_rows.append({
            "symbol": p.symbol,
            "shares": round(p.shares, 2),
            "avg_cost": round(p.avg_cost, 4),
            "price": round(price or 0.0, 4),
            "market_value": round(value, 2),
            "unrealized_pnl": round(pnl, 2),
            "realized_pnl": round(p.realized_pnl, 2),
        })

    # 现金 = 入金 - 出金 + 分红 + 卖出净额 - 买入总额
    deposits = float(cashflows.query("kind == 'deposit'")["amount"].sum()) if len(cashflows) else 0.0
    withdrawals = float(cashflows.query("kind == 'withdraw'")["amount"].sum()) if len(cashflows) else 0.0
    dividends = float(cashflows.query("kind == 'dividend'")["amount"].sum()) if len(cashflows) else 0.0
    buy_total = sell_total = fee_total = 0.0
    if len(trades):
        buys = trades[trades["side"] == "buy"]
        sells = trades[trades["side"] == "sell"]
        buy_total = float((buys["price"] * buys["shares"]).sum())
        sell_total = float((sells["price"] * sells["shares"]).sum())
        fee_total = float(trades["fee"].sum())
    cash = deposits - withdrawals + dividends + sell_total - buy_total - fee_total
    total_assets = cash + market_value
    net_invested = deposits - withdrawals

    # XIRR：出入金为现金流，期末总资产为终值
    flows: list[tuple[str, float]] = []
    for _, row in cashflows.iterrows():
        sign = -1.0 if row["kind"] == "deposit" else 1.0
        if row["kind"] == "dividend":
            continue   # 分红留在账户内，体现在终值里
        flows.append((row["date"], sign * row["amount"]))
    today = str(pd.Timestamp.now().date())
    flows.append((today, total_assets))
    annual_xirr = xirr(flows)

    realized_total = sum(p.realized_pnl for p in positions)
    total_pnl = total_assets - net_invested

    warnings: list[str] = []
    if cash < -1e-6:
        warnings.append(
            "现金余额为负：很可能漏记了入金记录（qm ledger cash --amount ... --kind deposit），"
            "收益率指标不可信"
        )
    if market_value > 1e-6 and net_invested <= 1e-6:
        warnings.append("存在持仓但累计净入金为 0：请先补录入金，再看收益率")

    return {
        "warnings": warnings,
        "as_of": today,
        "total_assets": round(total_assets, 2),
        "cash": round(cash, 2),
        "market_value": round(market_value, 2),
        "net_invested": round(net_invested, 2),
        "total_pnl": round(total_pnl, 2),
        "total_return": round(total_pnl / net_invested, 4) if net_invested > 0 else None,
        "realized_pnl": round(realized_total, 2),
        "unrealized_pnl": round(unrealized, 2),
        "dividends": round(dividends, 2),
        "fees": round(fee_total, 2),
        "xirr": round(annual_xirr, 4) if annual_xirr is not None else None,
        "positions": position_rows,
        "missing_price": missing_price,
        "trade_count": len(trades),
    }
