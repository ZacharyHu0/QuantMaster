"""命令行入口 `qm`。

    qm serve                                    启动 Web 界面
    qm fetch --universe demo --start 2022-01-01 预取行情到本地缓存
    qm factors                                  列出内置因子
    qm factor-test "rank(-delta(close, 5))"     因子体检
    qm backtest --factor mom_20d --top 5        因子选股回测（--full 输出年/月收益，--stop-loss 止损）
    qm validate "expr" --split 2024-01-01       样本外验证（防过拟合）
    qm grid --factors mom_20d,rev_5d            参数网格扫描
    qm fund-test ep                             基本面因子体检（PE/PB/股息/市值/ROE）
    qm mine --generations 8                     遗传规划挖因子
    qm mine-llm --rounds 2                      LLM 挖因子
    qm crawl [--skip-llm]                       抓取财经快讯
    qm paper run --factor mom_20d               模拟盘执行一次调仓
    qm ledger import trades.csv                 导入券商成交
    qm ledger report                            实盘收益报告
    qm ledger nav                               实盘每日净值（TWR）与基准对比
"""

from __future__ import annotations

import argparse
import json
import sys

import pandas as pd


def _today() -> str:
    return str(pd.Timestamp.now().date())


def _print_json(data) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2, default=str))


def _load_panel(universe: str, start: str, end: str):
    from quantmaster.data import load_panel
    from quantmaster.data.universe import load_universe

    symbols = load_universe(universe)
    print(f"加载 {len(symbols)} 只标的 {start} ~ {end} …", file=sys.stderr)
    return load_panel(symbols, start, end)


def cmd_serve(args) -> None:
    from quantmaster.server.app import serve

    serve()


def cmd_fetch(args) -> None:
    from quantmaster.data import load_history
    from quantmaster.data.universe import load_universe

    symbols = load_universe(args.universe)
    end = args.end or _today()
    ok = failed = 0
    for symbol in symbols:
        try:
            df = load_history(symbol, args.start, end, use_cache=not args.force)
            print(f"  {symbol}: {len(df)} 条 ({df.index.min().date()} ~ {df.index.max().date()})")
            ok += 1
        except Exception as e:
            print(f"  {symbol}: 失败 {e}", file=sys.stderr)
            failed += 1
    print(f"完成: {ok} 成功, {failed} 失败")


def cmd_factors(args) -> None:
    from quantmaster.factors.library import list_factors

    for f in list_factors():
        print(f"  {f['name']:<16} {f['expression']:<48} {f['description']}")


def cmd_factor_test(args) -> None:
    from quantmaster.factors import analyze_factor, compute_factor
    from quantmaster.factors.library import get_factor

    factor = get_factor(args.expression)
    panel = _load_panel(args.universe, args.start, args.end or _today())
    values = compute_factor(factor, panel)
    report = analyze_factor(values, panel["close"], name=factor.name, quantiles=args.quantiles)
    _print_json(report.summary())


def cmd_backtest(args) -> None:
    from quantmaster.backtest import (
        BacktestConfig,
        FactorStrategy,
        monthly_return_table,
        run_backtest,
        yearly_returns,
    )
    from quantmaster.data import load_history
    from quantmaster.factors.library import get_factor

    end = args.end or _today()
    panel = _load_panel(args.universe, args.start, end)
    strategy = FactorStrategy(get_factor(args.factor), top_n=args.top, rebalance=args.rebalance)
    benchmark = None
    try:
        benchmark = load_history(args.benchmark, args.start, end)["close"]
    except Exception as e:
        print(f"基准 {args.benchmark} 加载失败: {e}", file=sys.stderr)
    result = run_backtest(panel, strategy.target_weights(panel),
                          BacktestConfig(initial_capital=args.capital,
                                         stop_loss=args.stop_loss,
                                         take_profit=args.take_profit),
                          benchmark_close=benchmark)
    _print_json(result.metrics)
    if args.full:
        print("\n== 年度收益 ==")
        print(yearly_returns(result.returns).to_string())
        print("\n== 月度收益（%） ==")
        print((monthly_return_table(result.returns) * 100).round(2).to_string())


def cmd_validate(args) -> None:
    """样本外验证：防过拟合的第一道关卡。"""
    from quantmaster.backtest import train_test_ic, walk_forward_ic
    from quantmaster.factors.library import get_factor

    factor = get_factor(args.expression)
    panel = _load_panel(args.universe, args.start, args.end or _today())
    result = train_test_ic(factor, panel, split=args.split)
    _print_json(result)
    print("\n== 滚动分段 IC（稳定性） ==")
    print(walk_forward_ic(factor, panel, n_splits=args.splits).to_string())


def cmd_grid(args) -> None:
    from quantmaster.backtest import grid_search
    from quantmaster.data import load_history

    end = args.end or _today()
    panel = _load_panel(args.universe, args.start, end)
    benchmark = None
    try:
        benchmark = load_history(args.benchmark, args.start, end)["close"]
    except Exception:
        pass
    table = grid_search(
        panel,
        factor_names=args.factors.split(","),
        top_ns=[int(x) for x in args.tops.split(",")],
        rebalances=args.rebalances.split(","),
        metric=args.metric,
        benchmark_close=benchmark,
    )
    print(table.to_string(index=False))


def cmd_fund_test(args) -> None:
    """基本面因子体检（需要网络拉取估值/财务数据，结果会缓存）。"""
    from quantmaster.data import load_panel
    from quantmaster.data.fundamentals import fundamental_panel
    from quantmaster.data.universe import load_universe
    from quantmaster.factors import analyze_factor, compute_factor, make_fundamental_factors

    end = args.end or _today()
    symbols = load_universe(args.universe)
    panel = load_panel(symbols, args.start, end)
    fund = fundamental_panel(symbols, args.start, end)
    factors = make_fundamental_factors(fund)
    if args.factor not in factors:
        print(f"可用基本面因子: {sorted(factors)}", file=sys.stderr)
        raise SystemExit(1)
    values = compute_factor(factors[args.factor], panel)
    report = analyze_factor(values, panel["close"], name=args.factor, quantiles=args.quantiles)
    _print_json(report.summary())


def cmd_mine(args) -> None:
    from quantmaster.factors.mining import GeneticMiner

    panel = _load_panel(args.universe, args.start, args.end or _today())
    miner = GeneticMiner(population=args.population, generations=args.generations, seed=args.seed)
    mined = miner.mine(panel, top_n=args.top)
    print(f"\n{'fitness':>8} {'RankIC':>8} {'ICIR':>7}  expression")
    for m in mined:
        print(f"{m.fitness:8.4f} {m.ic_mean:8.4f} {m.icir:7.3f}  {m.expression}")


def cmd_mine_llm(args) -> None:
    from quantmaster.factors.mining import LLMFactorMiner

    panel = _load_panel(args.universe, args.start, args.end or _today())
    mined = LLMFactorMiner().mine(panel, n=args.n, rounds=args.rounds)
    print(f"\n{'RankIC':>8} {'ICIR':>7} 达标  expression / 逻辑")
    for m in mined:
        print(f"{m.ic_mean:8.4f} {m.icir:7.3f} {'✅' if m.valid else '❌'}  {m.expression}")
        if m.rationale:
            print(f"{'':>26}{m.rationale}")


def cmd_crawl(args) -> None:
    from quantmaster.ai.crawler import AICrawler

    result = AICrawler().run(skip_llm=args.skip_llm)
    _print_json(result)


def cmd_paper(args) -> None:
    from quantmaster.backtest.paper import PaperTrader
    from quantmaster.backtest.strategy import FactorStrategy
    from quantmaster.data.universe import load_universe
    from quantmaster.factors.library import get_factor

    trader = PaperTrader(initial_capital=args.capital)
    if args.paper_cmd == "run":
        result = trader.run_once(
            FactorStrategy(get_factor(args.factor), top_n=args.top, rebalance=args.rebalance),
            load_universe(args.universe),
        )
        _print_json(result)
    else:
        _print_json(trader.report())


def cmd_daily(args) -> None:
    """每日例程：更新行情 -> 抓取快讯 -> 模拟盘调仓 -> 输出报告。

    适合交易日收盘后跑一次（挂 crontab / Windows 计划任务）：
        30 15 * * 1-5  cd /path/to/QuantMaster && qm daily >> daily.log 2>&1
    """
    from quantmaster.ai.crawler import AICrawler
    from quantmaster.backtest.paper import PaperTrader
    from quantmaster.backtest.strategy import FactorStrategy
    from quantmaster.data import load_history
    from quantmaster.data.universe import load_universe
    from quantmaster.factors.library import get_factor

    end = _today()
    symbols = load_universe(args.universe)

    print(f"== 1/3 更新行情（{len(symbols)} 只 + 基准）==", file=sys.stderr)
    ok = 0
    for symbol in [*symbols, args.benchmark]:
        try:
            load_history(symbol, args.start, end)
            ok += 1
        except Exception as e:
            print(f"  {symbol}: {e}", file=sys.stderr)
    print(f"  行情就绪 {ok}/{len(symbols) + 1}", file=sys.stderr)

    print("== 2/3 抓取财经快讯 ==", file=sys.stderr)
    try:
        crawl = AICrawler().run(skip_llm=args.skip_llm)
        print(f"  抓取 {crawl['fetched']} 条，入库 {crawl['saved']} 条", file=sys.stderr)
    except Exception as e:
        print(f"  快讯抓取失败（不影响后续）: {e}", file=sys.stderr)

    print("== 3/3 模拟盘调仓 ==", file=sys.stderr)
    trader = PaperTrader(initial_capital=args.capital)
    result = trader.run_once(
        FactorStrategy(get_factor(args.factor), top_n=args.top, rebalance=args.rebalance),
        symbols,
    )
    _print_json(result)


def cmd_ledger(args) -> None:
    from quantmaster.portfolio import Ledger, TradeRecord, ledger_report

    ledger = Ledger()
    if args.ledger_cmd == "nav":
        import pandas as pd

        from quantmaster.data import load_history
        from quantmaster.data.storage import BarStore
        from quantmaster.portfolio import daily_nav, nav_warnings, nav_with_benchmark

        trades = ledger.trades()
        if trades.empty:
            print("账本为空，先导入成交记录", file=sys.stderr)
            raise SystemExit(1)
        symbols = sorted(trades["symbol"].unique())
        start = str(pd.to_datetime(trades["date"]).min().date())
        end = _today()
        store = BarStore()
        prices = {}
        for symbol in symbols:
            try:
                prices[symbol] = load_history(symbol, start, end, store=store)["close"]
            except Exception as e:
                print(f"  {symbol} 行情缺失（按最近成交价估值）: {e}", file=sys.stderr)
        nav = daily_nav(ledger, pd.DataFrame(prices))
        for warning in nav_warnings(nav):
            print(f"⚠️  {warning}", file=sys.stderr)
        summary = {
            "as_of": str(nav.index[-1].date()),
            "total_assets": round(float(nav["total_assets"].iloc[-1]), 2),
            "pnl": round(float(nav["pnl"].iloc[-1]), 2),
            "twr_nav": round(float(nav["twr_nav"].iloc[-1]), 4),
        }
        try:
            bench = load_history(args.benchmark, start, end, store=store)["close"]
            summary["benchmark"] = args.benchmark
            summary["excess_annual"] = nav_with_benchmark(nav, bench)["excess_annual"]
        except Exception:
            pass
        _print_json(summary)
        return
    if args.ledger_cmd == "import":
        count = ledger.import_csv(args.file)
        print(f"导入 {count} 条成交记录")
    elif args.ledger_cmd == "add":
        ledger.add_trade(TradeRecord(date=args.date or _today(), symbol=args.symbol,
                                     side=args.side, price=args.price,
                                     shares=args.shares, fee=args.fee))
        print("已记录")
    elif args.ledger_cmd == "cash":
        ledger.add_cashflow(args.date or _today(), args.amount, args.kind)
        print("已记录")
    else:
        _print_json(ledger_report(ledger))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qm", description="QuantMaster — A股量化研究平台")
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p):
        p.add_argument("--universe", default="demo", help="股票池名（默认 demo）")
        p.add_argument("--start", default="2022-01-01")
        p.add_argument("--end", default=None)

    sub.add_parser("serve", help="启动 Web 界面").set_defaults(func=cmd_serve)

    p = sub.add_parser("fetch", help="预取行情到本地缓存")
    common(p)
    p.add_argument("--force", action="store_true", help="忽略缓存强制刷新")
    p.set_defaults(func=cmd_fetch)

    sub.add_parser("factors", help="列出内置因子").set_defaults(func=cmd_factors)

    p = sub.add_parser("factor-test", help="因子体检（IC/分层/换手）")
    p.add_argument("expression", help="内置因子名或表达式")
    common(p)
    p.add_argument("--quantiles", type=int, default=5)
    p.set_defaults(func=cmd_factor_test)

    p = sub.add_parser("backtest", help="因子选股回测")
    common(p)
    p.add_argument("--factor", default="mom_20d")
    p.add_argument("--top", type=int, default=5)
    p.add_argument("--rebalance", default="W", choices=["D", "W", "M"])
    p.add_argument("--benchmark", default="000300.SH")
    p.add_argument("--capital", type=float, default=1_000_000)
    p.add_argument("--stop-loss", type=float, default=None, help="止损线，如 0.08")
    p.add_argument("--take-profit", type=float, default=None, help="止盈线，如 0.25")
    p.add_argument("--full", action="store_true", help="输出年度/月度收益表")
    p.set_defaults(func=cmd_backtest)

    p = sub.add_parser("validate", help="因子样本外验证（防过拟合）")
    p.add_argument("expression", help="内置因子名或表达式")
    common(p)
    p.add_argument("--split", required=True, help="训练/验证切分日期，如 2024-01-01")
    p.add_argument("--splits", type=int, default=4, help="滚动分段数")
    p.set_defaults(func=cmd_validate)

    p = sub.add_parser("grid", help="参数网格扫描（因子×持仓数×调仓频率）")
    common(p)
    p.add_argument("--factors", default="mom_20d,rev_5d", help="逗号分隔的因子名/表达式")
    p.add_argument("--tops", default="3,5,10", help="逗号分隔的持仓数")
    p.add_argument("--rebalances", default="W,M", help="逗号分隔的调仓频率")
    p.add_argument("--metric", default="sharpe",
                   choices=["sharpe", "annual_return", "max_drawdown", "calmar"])
    p.add_argument("--benchmark", default="000300.SH")
    p.set_defaults(func=cmd_grid)

    p = sub.add_parser("fund-test", help="基本面因子体检（ep/bp/dividend_yield/small_cap/roe）")
    p.add_argument("factor", help="基本面因子名")
    common(p)
    p.add_argument("--quantiles", type=int, default=5)
    p.set_defaults(func=cmd_fund_test)

    p = sub.add_parser("mine", help="遗传规划因子挖掘")
    common(p)
    p.add_argument("--generations", type=int, default=8)
    p.add_argument("--population", type=int, default=60)
    p.add_argument("--top", type=int, default=10)
    p.add_argument("--seed", type=int, default=42)
    p.set_defaults(func=cmd_mine)

    p = sub.add_parser("mine-llm", help="LLM 因子挖掘")
    common(p)
    p.add_argument("--n", type=int, default=8)
    p.add_argument("--rounds", type=int, default=2)
    p.set_defaults(func=cmd_mine_llm)

    p = sub.add_parser("crawl", help="抓取财经快讯（+LLM 标注）")
    p.add_argument("--skip-llm", action="store_true", help="只抓取不做 LLM 标注")
    p.set_defaults(func=cmd_crawl)

    p = sub.add_parser("paper", help="模拟盘")
    psub = p.add_subparsers(dest="paper_cmd", required=True)
    pr = psub.add_parser("run", help="按策略执行一次模拟调仓")
    pr.add_argument("--universe", default="demo")
    pr.add_argument("--factor", default="mom_20d")
    pr.add_argument("--top", type=int, default=5)
    pr.add_argument("--rebalance", default="W", choices=["D", "W", "M"])
    pr.add_argument("--capital", type=float, default=1_000_000)
    psub.add_parser("report", help="模拟盘收益报告")
    p.set_defaults(func=cmd_paper, capital=1_000_000)

    p = sub.add_parser("daily", help="每日例程：更新行情+抓快讯+模拟盘调仓（适合挂定时任务）")
    p.add_argument("--universe", default="demo")
    p.add_argument("--start", default="2022-01-01")
    p.add_argument("--factor", default="mom_20d")
    p.add_argument("--top", type=int, default=5)
    p.add_argument("--rebalance", default="W", choices=["D", "W", "M"])
    p.add_argument("--benchmark", default="000300.SH")
    p.add_argument("--capital", type=float, default=1_000_000)
    p.add_argument("--skip-llm", action="store_true", help="快讯不做 LLM 标注")
    p.set_defaults(func=cmd_daily)

    p = sub.add_parser("ledger", help="实盘账本")
    lsub = p.add_subparsers(dest="ledger_cmd", required=True)
    li = lsub.add_parser("import", help="导入券商成交 CSV")
    li.add_argument("file")
    la = lsub.add_parser("add", help="记一笔成交")
    la.add_argument("--date", default=None)
    la.add_argument("--symbol", required=True)
    la.add_argument("--side", required=True, choices=["buy", "sell"])
    la.add_argument("--price", type=float, required=True)
    la.add_argument("--shares", type=float, required=True)
    la.add_argument("--fee", type=float, default=0.0)
    lc = lsub.add_parser("cash", help="记录出入金/分红")
    lc.add_argument("--date", default=None)
    lc.add_argument("--amount", type=float, required=True)
    lc.add_argument("--kind", default="deposit", choices=["deposit", "withdraw", "dividend"])
    lsub.add_parser("report", help="收益报告")
    ln = lsub.add_parser("nav", help="每日净值曲线与基准对比（TWR）")
    ln.add_argument("--benchmark", default="000300.SH")
    p.set_defaults(func=cmd_ledger)

    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
