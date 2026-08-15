"""实盘收益统计：TWR 时间加权收益、XIRR 内部收益率、持仓盈亏。

为什么需要两种收益率（本科金融水平即可理解）：
- TWR（时间加权）：剔除出入金时点影响，衡量「你的策略」本身好不好，
  可直接与沪深300 等基准对比。
- XIRR（金额加权/内部收益率）：把每笔出入金当作现金流解贴现率，
  衡量「你的钱」实际赚了多少（受出入金择时影响）。

估值需要行情：默认用数据层取各持仓的收盘价；离线时可手动传入价格。
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from quantmaster.portfolio.ledger import Ledger
from quantmaster.trading_sessions import market_date


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


def _fetch_prices(
    symbols: list[str], lookback_days: int = 10,
) -> tuple[dict[str, float], dict[str, dict[str, Any]]]:
    """取最新收盘价，同时保留每个价格的质量、来源与实际观测日。"""
    from quantmaster.data.registry import refresh_history

    end = pd.Timestamp(market_date())
    start = end - pd.Timedelta(days=lookback_days)
    prices: dict[str, float] = {}
    contracts: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        try:
            envelope = refresh_history(symbol, str(start.date()), str(end.date()))
            df = envelope.require_data()
            if not df.empty:
                prices[symbol] = float(df["close"].iloc[-1])
                observed_end = pd.Timestamp(df.index[-1]).strftime("%Y-%m-%d")
                quality = envelope.quality.to_dict()
                quality["observed_end"] = quality.get("observed_end") or observed_end
                contracts[symbol] = {
                    "quality": quality,
                    "provenance": list(envelope.provenance),
                    "price_as_of": observed_end,
                }
                continue
            contracts[symbol] = {
                "quality": {
                    "status": "unavailable",
                    "issues": ["行情结果为空"],
                    "stale": False,
                    "partial": True,
                },
                "provenance": list(envelope.provenance),
                "price_as_of": "",
            }
        except Exception as exc:
            contracts[symbol] = {
                "quality": {
                    "status": "unavailable",
                    "issues": [str(exc)],
                    "stale": False,
                    "partial": True,
                },
                "provenance": [],
                "price_as_of": "",
            }
    return prices, contracts


def ledger_report(
    ledger: Ledger,
    prices: dict[str, float] | None = None,
    *,
    as_of: str | None = None,
    price_contracts: dict[str, dict[str, Any]] | None = None,
) -> dict:
    """生成实盘收益报告。

    prices: {symbol: 最新价}。调用方提供价格时应同时传 as_of/price_contracts；
    缺少契约的手工价格会被明确标记为 degraded。缺失持仓按成本价估值并标记 unavailable。
    """
    positions = ledger.positions()
    cashflows = ledger.cashflows()
    trades = ledger.trades()

    holding_symbols = [p.symbol for p in positions if p.shares > 0]
    supplied_prices = prices is not None
    if prices is None:
        prices, fetched_contracts = (
            _fetch_prices(holding_symbols) if holding_symbols else ({}, {})
        )
        price_contracts = fetched_contracts
    else:
        prices = dict(prices)
        price_contracts = dict(price_contracts or {})

    for symbol in holding_symbols:
        if symbol in prices and symbol not in price_contracts:
            price_contracts[symbol] = {
                "quality": {
                    "status": "degraded",
                    "issues": ["调用方提供价格但未附行情质量与来源契约"],
                    "stale": False,
                    "partial": False,
                },
                "provenance": [],
                "price_as_of": as_of or "",
            }

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
            price_contracts[p.symbol] = {
                "quality": {
                    "status": "unavailable",
                    "issues": ["缺少行情，按持仓成本估值"],
                    "stale": False,
                    "partial": True,
                },
                "provenance": [],
                "price_as_of": "",
            }
        contract = price_contracts.get(p.symbol, {})
        contract_quality = contract.get("quality") or {}
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
            "price_as_of": str(contract.get("price_as_of") or ""),
            "price_quality": str(contract_quality.get("status") or "unavailable"),
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
    observed_dates = sorted({
        str(contract.get("price_as_of") or "")[:10]
        for symbol, contract in price_contracts.items()
        if symbol in holding_symbols and str(contract.get("price_as_of") or "")
    })
    complete_observation = len(observed_dates) > 0 and all(
        str(price_contracts.get(symbol, {}).get("price_as_of") or "")
        for symbol in holding_symbols
    )
    valuation_as_of = (
        min(observed_dates)
        if holding_symbols and complete_observation
        else as_of if not holding_symbols else ""
    )
    if not holding_symbols:
        valuation_as_of = as_of or market_date().isoformat()
    if valuation_as_of:
        flows.append((valuation_as_of, total_assets))
        annual_xirr = xirr(flows)
    else:
        annual_xirr = None

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

    by_symbol = {
        symbol: price_contracts.get(symbol, {})
        for symbol in holding_symbols
    }
    quality_statuses = [
        str((contract.get("quality") or {}).get("status") or "unavailable")
        for contract in by_symbol.values()
    ]
    quality_issues = [
        f"{symbol}: {issue}"
        for symbol, contract in by_symbol.items()
        for issue in ((contract.get("quality") or {}).get("issues") or [])
    ]
    stale = any(
        bool((contract.get("quality") or {}).get("stale"))
        for contract in by_symbol.values()
    )
    partial = bool(missing_price) or any(
        bool((contract.get("quality") or {}).get("partial"))
        for contract in by_symbol.values()
    )
    if missing_price or "unavailable" in quality_statuses:
        data_status = "unavailable"
    elif "degraded" in quality_statuses or stale or partial or len(observed_dates) > 1:
        data_status = "degraded"
    else:
        data_status = "verified"
    if len(observed_dates) > 1:
        quality_issues.append("持仓行情观测日不一致；报告 as_of 取最早观测日")
    if holding_symbols and not valuation_as_of:
        quality_issues.append("无法建立全部持仓的共同估值时点")
    if supplied_prices and holding_symbols and as_of is None:
        quality_issues.append("调用方提供价格但未声明估值时点")

    return {
        "warnings": warnings,
        "as_of": valuation_as_of,
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
        "data_quality": {
            "status": data_status,
            "stale": stale,
            "partial": partial,
            "coverage_ratio": (
                len(set(holding_symbols) - set(missing_price)) / len(holding_symbols)
                if holding_symbols else 1.0
            ),
            "requested_symbols": holding_symbols,
            "observed_symbols": [
                symbol for symbol in holding_symbols if symbol not in missing_price
            ],
            "missing_symbols": missing_price,
            "observed_start": min(observed_dates) if observed_dates else "",
            "observed_end": max(observed_dates) if observed_dates else "",
            "issues": list(dict.fromkeys(quality_issues)),
            "by_symbol": by_symbol,
        },
        "market_provenance": {
            symbol: contract.get("provenance") or []
            for symbol, contract in by_symbol.items()
        },
    }
