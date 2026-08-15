"""命令行入口 `qm`。

qm serve                                    启动 Web 界面
qm fetch --universe demo --start 2022-01-01 预取行情到本地缓存
qm regime --universe demo                     牛熊/趋势/板块状态
qm select --universe demo --horizon 3          每日短周期选股
qm factors                                  列出内置因子
qm factor-test "rank(-delta(close, 5))"     因子体检
qm backtest --factor mom_20d --top 5        因子选股回测（--full 输出年/月收益，--stop-loss 止损）
qm lab discover --method genetic           提交可恢复的因子发现任务
qm lab validate <version_id>               对候选版本执行统一样本外验证
qm crawl [--skip-llm]                       抓取财经快讯
qm paper create --name 动量观察 --factor mom_20d  创建独立模拟账户
qm ledger import trades.csv                 导入券商成交
qm ledger report                            实盘收益报告
qm ledger nav                               实盘每日净值（TWR）与基准对比
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

import pandas as pd

from quantmaster.logging_config import redact_sensitive_text
from quantmaster.trading_sessions import market_date, resolve_session_target


def _today() -> str:
    return market_date().isoformat()


def _close_day() -> str:
    """Default date for daily research commands, excluding today's open session."""
    expectation = resolve_session_target()
    if expectation.ready and expectation.session:
        return expectation.session
    return (market_date() - pd.Timedelta(days=1)).date().isoformat()


def _print_json(data) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2, default=str))


def _usable_market_data(envelope, *, label: str):
    if envelope.quality.status == "degraded":
        issues = "；".join(envelope.quality.issues) or "证据降级"
        print(f"{label} 使用降级行情：{issues}", file=sys.stderr)
    return envelope.require_data()


def _load_panel(universe: str, start: str, end: str):
    from quantmaster.data import refresh_panel
    from quantmaster.data.universe import load_universe_analysis_snapshot

    universe_snapshot = load_universe_analysis_snapshot(universe)
    symbols = list(universe_snapshot.symbols)
    if not universe_snapshot.formal_eligible:
        print(
            f"{universe} 使用旧候选 sandbox 预览：{'；'.join(universe_snapshot.issues)}",
            file=sys.stderr,
        )
    print(f"加载 {len(symbols)} 只标的 {start} ~ {end} …", file=sys.stderr)
    return _usable_market_data(
        refresh_panel(symbols, start, end), label=f"{universe} 面板",
    )


def cmd_serve(args) -> None:
    from quantmaster.runtime.windows_app import initialize_windows_app_process
    from quantmaster.server.app import serve

    initialize_windows_app_process(root=True)
    browser_timer = None
    if getattr(args, "open_browser", False):
        import threading
        import webbrowser

        from quantmaster.config import get_config

        cfg = get_config().server
        browser_timer = threading.Timer(1.5, webbrowser.open, args=(f"http://{cfg.host}:{cfg.port}",))
        browser_timer.daemon = True
        browser_timer.start()
    try:
        serve(reload=bool(getattr(args, "reload", False)))
    finally:
        if browser_timer is not None:
            browser_timer.cancel()


def cmd_activate(args) -> int:
    """Activate a pre-staged immutable slot and print the transition result."""

    from quantmaster.runtime.activation import activate_installed_slot

    result = activate_installed_slot(
        args.build_sha,
        root_pid=args.root_pid,
        ready_timeout=args.ready_timeout,
    )
    _print_json(result)
    return 0 if result.get("status") in {"activated", "already_active", "rolled_back"} else 2


def cmd_doctor(args) -> int:
    from quantmaster.doctor import run_doctor

    report = run_doctor(deep=bool(args.deep))
    _print_json(report)
    if report["counts"]["high"]:
        return 2
    if args.strict and report["counts"]["warning"]:
        return 1
    return 0


def cmd_automation(args) -> None:
    from quantmaster.automation.service import AutomationService
    from quantmaster.config import get_config

    service = AutomationService()
    service.store.initialize()
    service.store.sync_news_intervals()
    if args.automation_cmd == "run":
        _print_json(service.run_task(args.task, actor="cli"))
        return
    if args.automation_cmd == "dispatch":
        _print_json(service.dispatcher.dispatch(args.limit))
        return

    checks: dict[str, dict[str, object]] = {}
    for module in ("apscheduler", "lark_oapi", "qrcode", "keyring"):
        try:
            __import__(module)
            checks[module] = {"ok": True}
        except Exception as exc:
            checks[module] = {"ok": False, "message": str(exc)}
    cfg = get_config().automation
    accounts = service.store.bot_accounts()
    _print_json(
        {
            "enabled": cfg.enabled,
            "timezone": cfg.timezone,
            "dependencies": checks,
            "channels": [
                {key: value for key, value in account.items() if key != "secret_target"}
                for account in accounts
            ],
            "targets": service.public_targets(),
            "jobs": service.store.jobs(),
            "hint": "定时任务和 Bot 长连接由 qm serve 承载",
        }
    )


def cmd_lab(args) -> None:
    """Quant Lab 的独立 Worker、研究任务和人工审批入口。"""
    from quantmaster.lab.service import LabService

    if args.lab_cmd == "worker":
        from quantmaster.lab.worker import run_standalone

        run_standalone()
        return
    service = LabService()
    if args.lab_cmd == "doctor":
        report = service.doctor()
        if args.json:
            _print_json(report)
        else:
            print("检查\t状态\t详情\t修复动作")
            for item in report["checks"]:
                print(
                    f"{item['name']}\t{item['state']}\t{item['detail']}\t{item['action']}"
                )
            print(f"\n总体: {report['state']} · {'可运行' if report['runnable'] else '已阻止'}")
        return
    if args.lab_cmd == "benchmark":
        _print_json(service.benchmark_local(
            universe=args.universe, start=args.start,
            end=args.end or _close_day(), runs=args.runs,
        ))
        return
    if args.lab_cmd == "list":
        _print_json(service.store.list_factors(status=args.status, search=args.search, limit=args.limit))
        return
    if args.lab_cmd == "jobs":
        from quantmaster.lab.jobs import list_lab_jobs

        _print_json({"items": list_lab_jobs(args.limit)})
        return
    if args.lab_cmd == "studies":
        _print_json({"items": service.store.studies(args.limit)})
        return
    if args.lab_cmd == "resume":
        _print_json(service.resume_study(args.study_id))
        return
    if args.lab_cmd == "approve":
        _print_json(service.store.approve(args.version_id, actor="cli", reason=args.reason))
        return
    if args.lab_cmd == "deploy":
        _print_json(
            service.store.deploy(args.version_id, universe=args.universe, horizon=args.horizon, actor="cli")
        )
        return
    if args.lab_cmd == "optimize":
        study = service.create_study(
            {
                "universe": args.universe,
                "start": args.start,
                "end": args.end or _close_day(),
                "models": [item.strip() for item in args.models.split(",") if item.strip()],
                "budget_hours": args.budget_hours,
                "max_trials": args.max_trials,
                "top_n": args.top,
                "sequence_length": args.sequence_length,
                "research_tier": args.research_tier,
            }
        )
        _print_json(
            {
                "study": study,
                "hint": "Study 已进入可恢复队列；只会产出 Shadow 候选，不会自动晋升 Champion",
            }
        )
        return
    if args.lab_cmd == "audit":
        job = service.enqueue(
            "bias_audit",
            {
                "version_id": args.version_id,
                "universe": args.universe,
                "start": args.start,
                "end": args.end or _close_day(),
            },
        )
        _print_json({"job": job, "hint": "偏差审计已进入研究队列"})
        return

    end = args.end or _close_day()
    base = {"universe": args.universe, "start": args.start, "end": end}
    if args.lab_cmd == "prepare-data":
        job = service.enqueue("prepare_data", base)
    elif args.lab_cmd in {"validate", "score"}:
        job = service.enqueue("validate", {"version_id": args.version_id, **base})
    elif args.lab_cmd == "discover":
        kind = {
            "llm": "discover_llm",
            "python": "discover_python",
        }.get(args.method, "discover_genetic")
        params = {**base, "horizon": args.horizon, "top_n": args.top}
        if args.method == "llm":
            params = {**base, "horizon": args.horizon, "count": args.top, "rounds": args.rounds}
        elif args.method == "python":
            params = {
                **base,
                "horizon": args.horizon,
                "rounds": args.rounds,
                "candidate_limit": args.candidates,
                "finalists": args.finalists,
            }
        else:
            params.update({"population": args.population, "generations": args.generations})
        job = service.enqueue(kind, params)
    elif args.lab_cmd == "train":
        job = service.enqueue(
            "train",
            {
                **base,
                "model": args.model,
                "horizon": args.horizon,
                "sequence_length": args.sequence_length,
                "config": {"epochs": args.epochs},
            },
        )
    else:  # pragma: no cover - argparse 保证子命令完整
        raise ValueError(f"未知 lab 子命令: {args.lab_cmd}")
    _print_json(
        {
            "job": job,
            "hint": "任务已进入可恢复队列；由 qm serve 或 qm lab worker 执行",
        }
    )


def cmd_fetch(args) -> None:
    from quantmaster.data import refresh_bars
    from quantmaster.data.universe import load_universe_analysis

    symbols = load_universe_analysis(args.universe)
    end = args.end or _close_day()
    ok = failed = 0
    for symbol in symbols:
        try:
            df = _usable_market_data(
                refresh_bars(
                    symbol, args.start, end, frequency=args.frequency,
                    use_cache=not args.force,
                ),
                label=symbol,
            )
            print(f"  {symbol} {args.frequency}: {len(df)} 条 ({df.index.min()} ~ {df.index.max()})")
            ok += 1
        except Exception as e:
            print(f"  {symbol}: 失败 {redact_sensitive_text(e)}", file=sys.stderr)
            failed += 1
    print(f"完成: {ok} 成功, {failed} 失败")


def cmd_regime(args) -> None:
    from quantmaster.data.industry import load_industry_analysis_context
    from quantmaster.market import analyze_market, analyze_sectors

    end = args.end or _close_day()
    panel = _load_panel(args.universe, args.start, end)
    report = analyze_market(panel)
    past = report.pop("past").tail(args.history)
    payload = {
        **report,
        "past": [
            {
                "date": str(idx.date()),
                **{
                    key: (None if pd.isna(value) else (value.item() if hasattr(value, "item") else value))
                    for key, value in row.items()
                },
            }
            for idx, row in past.iterrows()
        ],
        "sectors": [],
    }
    if not args.no_sectors:
        mapping, industry_evidence = load_industry_analysis_context(
            as_of=end if args.end else None,
        )
        sectors = analyze_sectors(panel, mapping).head(args.sector_top)
        payload["sectors"] = sectors.to_dict(orient="records")
        payload["industry_evidence"] = industry_evidence
    _print_json(payload)


def cmd_select(args) -> None:
    from quantmaster.data import refresh_panel
    from quantmaster.data.industry import load_industry_analysis_context
    from quantmaster.data.names import read_stock_names
    from quantmaster.data.universe import load_universe_analysis_snapshot
    from quantmaster.decision import DecisionStore, hybrid_daily_selection

    end = args.end or _close_day()
    if args.end and args.policy_mode == "live":
        raise ValueError(
            "显式历史截止日不能使用 live 模式；请选择 historical_replay 或 retrospective"
        )
    if not args.no_save and args.policy_mode == "retrospective":
        raise ValueError("retrospective 结果只能配合 --no-save 使用")
    universe_snapshot = load_universe_analysis_snapshot(
        args.universe, as_of=end if args.end else None,
    )
    market_envelope = refresh_panel(list(universe_snapshot.symbols), args.start, end)
    panel = _usable_market_data(market_envelope, label=f"{args.universe} 面板")
    mapping, industry_evidence = (
        ({}, None) if args.no_industry
        else load_industry_analysis_context(as_of=end if args.end else None)
    )
    names = read_stock_names(list(panel["close"].columns))
    decision_feature_inputs: dict[str, pd.DataFrame] = {}
    report = hybrid_daily_selection(
        panel,
        top_n=args.top,
        horizon=args.horizon,
        profile=args.profile,
        universe=args.universe,
        industry_map=mapping,
        name_map=names,
        policy_mode=args.policy_mode,
        evidence_sink=decision_feature_inputs,
    )
    report["calculation_quality"] = report.get("data_quality")
    report["data_quality"] = market_envelope.quality.to_dict()
    report["market_provenance"] = list(market_envelope.provenance)
    report["universe_evidence"] = universe_snapshot.to_dict()
    report["industry_evidence"] = industry_evidence
    save_allowed = (
        market_envelope.quality.formal_eligible
        and universe_snapshot.formal_eligible
        and (
            args.no_industry
            or bool((industry_evidence or {}).get("formal_eligible"))
        )
    )
    report["persistence"] = {
        "requested": not args.no_save,
        "saved": False,
        "status": (
            "not_requested" if args.no_save else "pending" if save_allowed else "blocked"
        ),
        "reason": (
            "" if args.no_save or save_allowed
            else "行情、候选池或行业证据未通过正式门；结果按降级预览输出"
        ),
    }
    if not args.no_save:
        if save_allowed:
            report["persistence"].update(saved=True, status="saved")
            DecisionStore().save(
                report, args.universe, panel={**panel, **decision_feature_inputs},
            )
    _print_json(report)


def cmd_decisions(args) -> None:
    from quantmaster.decision import DecisionStore

    _print_json({"snapshots": DecisionStore().history(args.universe, args.limit)})


def _after_close_score_version(args, service) -> int:
    from quantmaster.after_close.models import SCORE_VERSION, SHADOW_SCORE_VERSION

    health = service.store.health(500)
    if args.score_version_cmd == "status":
        _print_json(health)
        return 0
    if args.score_version_cmd == "promote":
        if not health["manual_review_eligible"]:
            _print_json(
                {
                    "status": "rejected",
                    "reason": "V2 尚未通过人工评审资格门",
                    "checks": health["promotion_checks"],
                }
            )
            return 1
        _print_json(
            {
                "status": "promoted",
                **service.store.set_active_score_version(SHADOW_SCORE_VERSION),
            }
        )
        return 0
    _print_json(
        {
            "status": "rolled_back",
            **service.store.set_active_score_version(SCORE_VERSION),
        }
    )
    return 0


def _run_after_close_job(args, service) -> int:
    import time

    from quantmaster.after_close.jobs import get_after_close_jobs

    jobs = get_after_close_jobs()
    job, created = jobs.submit(
        as_of=args.as_of or "",
        force=args.after_close_cmd == "rerun",
    )
    jobs.start()
    print(
        f"盘后扫描任务 {job['id']} {'已创建' if created else '已复用'}",
        file=sys.stderr,
    )
    while True:
        job = jobs.get(str(job["id"]))
        if job["status"] not in {"queued", "running", "cancelling", "interrupted"}:
            break
        print(
            f"{int(job.get('progress') or 0):3d}% {job.get('phase') or '等待'} {job.get('detail') or ''}",
            file=sys.stderr,
        )
        time.sleep(0.5)
    if job["status"] not in {"completed", "completed_with_warnings"}:
        _print_json(jobs.public(job))
        return 1
    snapshot = service.store.public_latest()
    _print_json({"job": jobs.public(job), "snapshot": snapshot})
    return 0


def _export_after_close_snapshot(args, snapshot, payload) -> None:
    from pathlib import Path

    from quantmaster.runtime.json import strict_json_dumps

    target = Path(args.output).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if args.format == "csv":
        import csv

        fields = [
            "rank",
            "symbol",
            "name",
            "score",
            "as_of_date",
            "sectors",
            "reasons",
            "return_5d",
            "return_20d",
            "trend_20d",
            "avg_amount_20d",
            "amount_change",
            "volatility_20d",
            "drawdown_20d",
            "float_mv",
            "total_mv",
            "pe_ttm",
            "pb",
        ]
        with target.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for candidate in snapshot.candidates:
                writer.writerow(
                    {
                        "rank": candidate.rank,
                        "symbol": candidate.symbol,
                        "name": candidate.name,
                        "score": candidate.score,
                        "as_of_date": candidate.as_of_date,
                        "sectors": " / ".join(item["name"] for item in candidate.sectors),
                        "reasons": "；".join(candidate.reasons),
                        **{key: candidate.metrics.get(key) for key in fields if key in candidate.metrics},
                    }
                )
    else:
        target.write_text(strict_json_dumps(payload, indent=2), encoding="utf-8")
    _print_json({"status": "ok", "path": str(target), "snapshot_id": snapshot.snapshot_id})


def cmd_after_close(args) -> int:
    from quantmaster.after_close.service import get_after_close_service

    service = get_after_close_service()
    if args.after_close_cmd == "score-version":
        return _after_close_score_version(args, service)
    if args.after_close_cmd in {"scan", "rerun"}:
        return _run_after_close_job(args, service)
    if args.after_close_cmd == "history":
        _print_json({"items": service.store.history(args.limit)})
        return 0
    snapshot = (
        service.store.get(args.snapshot_id) if getattr(args, "snapshot_id", "") else service.store.latest()
    )
    if snapshot is None:
        raise FileNotFoundError("尚无盘后研究快照")
    payload = snapshot.to_dict()
    payload["validation"] = {
        **payload.get("validation", {}),
        "labels": service.store.labels(snapshot.snapshot_id),
    }
    if args.after_close_cmd == "export":
        _export_after_close_snapshot(args, snapshot, payload)
    else:
        _print_json(payload)
    return 0


def cmd_stockdb(args) -> int:
    from quantmaster.config import get_config
    from quantmaster.data.free_stockdb_ingest import StockDBIngestStore
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
    from quantmaster.data.free_stockdb_source import FreeStockDBSource

    store = StockDBIngestStore()
    if args.stockdb_cmd == "validate-native":
        from datetime import date, timedelta

        from quantmaster.data.free_stockdb_compatibility import validate_runtime_profile

        end = args.end or _close_day()
        start = args.start or (date.fromisoformat(end) - timedelta(days=300)).isoformat()
        samples = [
            {"symbol": symbol, "start": start, "end": end, "kind": kind}
            for symbol, kind in (
                ("600000.SH", "ordinary"),
                ("000001.SZ", "suspension_or_boundary"),
                ("510300.SH", "etf"),
            )
        ]
        profile = validate_runtime_profile(FreeStockDBSource(), samples)
        _print_json(profile.to_dict())
        return 0 if profile.status in {"compatible", "partial"} else 1
    if args.stockdb_cmd == "status":
        from quantmaster.data.free_stockdb_compatibility import StockDBCompatibilityStore

        source = FreeStockDBSource()
        compatibility = StockDBCompatibilityStore().get(source.artifact_identity().artifact_id)
        _print_json(
            {
                "runtime": free_stockdb_runtime.status(),
                "latest_ingest": store.history(1)[0].to_dict() if store.history(1) else None,
                "compatibility": compatibility.to_dict() if compatibility else None,
                "native_acceleration_enabled": get_config().data.free_stockdb_native_acceleration_enabled,
            }
        )
        return 0
    if args.stockdb_cmd == "audit":
        from quantmaster.data.free_stockdb_compatibility import StockDBCompatibilityStore

        source = FreeStockDBSource()
        probe = source.probe()
        boards = source.board_hierarchy()
        catalog = source.security_catalog()
        compatibility = StockDBCompatibilityStore().get(source.artifact_identity().artifact_id)
        _print_json(
            {
                "status": "ok",
                "upstream": "vendor-declared-unverified",
                "upstream_evidence": "not_provided",
                "distribution": "free-stockdb",
                "independent_cross_validation": False,
                "probe": probe,
                "artifact": source.artifact_identity(
                    data_session=str(free_stockdb_runtime.status().get("validated_session") or ""),
                ).to_dict(),
                "catalog_count": len(catalog),
                "board_count": len(boards),
                "board_levels": sorted({str(item.get("level") or "") for item in boards}),
                "compatibility": compatibility.to_dict() if compatibility else None,
            }
        )
        return 0
    # A formal stock ingest shares the after-close integrity gate and therefore
    # also publishes/reuses the corresponding research snapshot.
    from quantmaster.after_close.service import get_after_close_service

    snapshot = get_after_close_service().scan(as_of=args.as_of or "")
    _print_json(
        {
            "ingest_id": snapshot.ingest_id,
            "artifact_id": snapshot.artifact_id,
            "as_of_date": snapshot.as_of_date,
            "coverage": snapshot.coverage,
        }
    )
    return 0


def _run_etf_research_job(args, service) -> int:
    import time

    from quantmaster.rotation.etf_jobs import get_etf_research_jobs

    jobs = get_etf_research_jobs()
    job, _ = jobs.submit(as_of=args.as_of or "")
    jobs.start()
    while True:
        job = jobs.get(str(job["id"]))
        if job["status"] not in {"queued", "running", "cancelling", "interrupted"}:
            break
        print(f"{int(job.get('progress') or 0):3d}% {job.get('phase') or '等待'}", file=sys.stderr)
        time.sleep(0.5)
    if job["status"] not in {"completed", "completed_with_warnings"}:
        _print_json(jobs.public(job))
        return 1
    _print_json(service.store.latest().to_dict())
    return 0


def cmd_etf_research(args) -> int:
    from pathlib import Path

    from quantmaster.rotation.etf_jobs import get_etf_research_jobs
    from quantmaster.rotation.etf_research import get_etf_research_service
    from quantmaster.runtime.json import strict_json_dumps

    service = get_etf_research_service()
    if args.etf_research_cmd == "cancel":
        jobs = get_etf_research_jobs()
        _print_json(jobs.public(jobs.cancel(args.job_id)))
        return 0
    if args.etf_research_cmd == "resume":
        jobs = get_etf_research_jobs()
        resumed = jobs.retry(args.job_id)
        jobs.start()
        _print_json(jobs.public(resumed))
        return 0
    if args.etf_research_cmd == "scan":
        return _run_etf_research_job(args, service)
    if args.etf_research_cmd == "history":
        _print_json({"items": service.store.history(args.limit)})
        return 0
    snapshot = service.store.get(args.snapshot_id) if args.snapshot_id else service.store.latest()
    if snapshot is None:
        raise FileNotFoundError("尚无 ETF 研究快照")
    if args.etf_research_cmd == "export":
        target = Path(args.output).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        if args.format == "json":
            target.write_text(strict_json_dumps(snapshot.to_dict(), indent=2), encoding="utf-8")
        else:
            pd.DataFrame([item.to_dict() for item in snapshot.items]).to_csv(
                target,
                index=False,
                encoding="utf-8-sig",
            )
        _print_json({"status": "ok", "path": str(target), "snapshot_id": snapshot.snapshot_id})
        return 0
    _print_json(snapshot.to_dict())
    return 0


def cmd_factors(args) -> None:
    from quantmaster.ai.sentiment import list_news_factors
    from quantmaster.factors.fundamental import list_fundamental_factors
    from quantmaster.factors.library import list_factors

    for f in list_factors() + list_fundamental_factors() + list_news_factors():
        print(f"  {f['name']:<16} {f['expression']:<48} {f['description']}")


def cmd_factor_test(args) -> None:
    from quantmaster.factors import run_factor_test

    result = run_factor_test(
        expression=args.expression,
        universe=args.universe,
        start=args.start,
        end=args.end,
        quantiles=args.quantiles,
        neutralize=args.neutralize,
        refresh=True,
    )
    if (result.get("universe_evidence") or {}).get("formal_eligible") is False:
        print("⚠️ Sandbox：结果不可进入正式研究", file=sys.stderr)
    if (result.get("data_quality") or {}).get("status") == "degraded":
        print("⚠️ 行情数据已降级", file=sys.stderr)
    if args.neutralize:
        if not result.get("neutralized"):
            print("⚠️ 行业中性化未执行", file=sys.stderr)
        if (result.get("industry_evidence") or {}).get("status") == "degraded":
            print("⚠️ 行业证据已降级", file=sys.stderr)
    _print_json(result["summary"])


def cmd_backtest(args) -> None:
    from quantmaster.backtest.application import execute_backtest
    from quantmaster.backtest.spec import BacktestSpec, split_factor_references

    if args.strategy == "decision":
        strategy = {
            "kind": "decision", "profile": args.profile, "top_n": args.top,
            "holding_days": args.holding_days, "cap_weight": 0.25,
            "policy_snapshot": {},
        }
    else:
        factor_count = len(split_factor_references(args.factor))
        strategy = {
            "kind": "factor", "factor": args.factor, "top_n": args.top,
            "rebalance": args.rebalance,
            "weighting": args.weighting if factor_count > 1 else "equal",
            "cap_weight": 0.35,
        }
    spec = BacktestSpec.model_validate({
        "strategy": strategy,
        "universe": args.universe,
        "start": args.start,
        "end": args.end,
        "benchmark": args.benchmark,
        "initial_capital": args.capital,
        "stop_loss": args.stop_loss,
        "take_profit": args.take_profit,
    })
    execution = execute_backtest(spec)
    manifest, artifact = execution["manifest"], execution["artifact"]
    _print_json({
        **artifact["metrics"],
        "research_tier": manifest["research_tier"],
        "formal_eligible": manifest["formal_eligible"],
        "warnings": manifest["warnings"],
    })
    if args.full:
        print("\n== 年度收益 ==")
        yearly = pd.DataFrame(artifact["yearly"])
        print(yearly.set_index("year").to_string() if not yearly.empty else "无")
        print("\n== 月度收益（%） ==")
        monthly = pd.DataFrame(artifact["monthly"])
        if not monthly.empty:
            monthly = monthly.set_index("year").apply(pd.to_numeric) * 100
        print(monthly.round(2).to_string() if not monthly.empty else "无")


def cmd_crawl(args) -> None:
    from quantmaster.ai.crawler import AICrawler

    result = AICrawler().run(skip_llm=args.skip_llm)
    _print_json(result)


def _paper_strategy_payload(args) -> dict:
    if args.strategy == "decision":
        return {
            "kind": "decision",
            "profile": args.profile,
            "top_n": args.top,
            "holding_days": args.holding_days,
            "cap_weight": 0.25,
            "policy_snapshot": {},
        }
    return {
        "kind": "factor",
        "factor": args.factor,
        "top_n": args.top,
        "rebalance": args.rebalance,
        "weighting": "equal",
        "cap_weight": 0.35,
    }


def cmd_paper(args) -> None:
    from quantmaster.backtest.paper_accounts import get_paper_service
    from quantmaster.backtest.spec import PaperAccountSpec

    service = get_paper_service()
    if args.paper_cmd == "accounts":
        _print_json({"items": service.store.accounts(include_archived=True)})
        return
    if args.paper_cmd == "confirm":
        _print_json(service.store.confirm(args.cycle))
        return
    if args.paper_cmd == "process":
        _print_json(service.process(args.account))
        return
    if args.paper_cmd == "report":
        account_id = args.account
        if not account_id:
            accounts = service.store.accounts()
            if not accounts:
                raise ValueError("还没有模拟账户；先运行 qm paper create")
            account_id = accounts[0]["id"]
        _print_json(service.report(account_id))
        return
    if args.paper_cmd == "propose":
        _print_json(service.propose(args.account))
        return

    strategy = _paper_strategy_payload(args)
    if args.paper_cmd == "create":
        account = service.create_account(
            PaperAccountSpec.model_validate(
                {
                    "name": args.name,
                    "strategy": strategy,
                    "universe": args.universe,
                    "initial_capital": args.capital,
                    "mode": args.mode,
                }
            )
        )
        _print_json(account)
        return


def cmd_daily(args) -> None:
    """每日例程：更新行情 -> 抓取快讯 -> 生成选股 -> 处理已确认订单并提案。

    适合交易日收盘后跑一次（挂 crontab / Windows 计划任务）：
        30 15 * * 1-5  cd /path/to/QuantMaster && qm daily >> daily.log 2>&1
    """
    from quantmaster.ai.crawler import AICrawler
    from quantmaster.backtest.paper_accounts import get_paper_service
    from quantmaster.backtest.spec import PaperAccountSpec
    from quantmaster.data import read_stock_names, refresh_history, refresh_panel
    from quantmaster.data.industry import load_industry_analysis_context
    from quantmaster.data.universe import load_universe_analysis_snapshot
    from quantmaster.decision import DecisionStore, hybrid_daily_selection

    end = _close_day()
    universe_snapshot = load_universe_analysis_snapshot(args.universe)
    symbols = list(universe_snapshot.symbols)

    print(f"== 1/4 更新行情（{len(symbols)} 只 + 基准）==", file=sys.stderr)
    ok = 0
    for symbol in [*symbols, args.benchmark]:
        try:
            _usable_market_data(
                refresh_history(symbol, args.start, end), label=symbol,
            )
            ok += 1
        except Exception as e:
            print(f"  {symbol}: {redact_sensitive_text(e)}", file=sys.stderr)
    print(f"  行情就绪 {ok}/{len(symbols) + 1}", file=sys.stderr)

    print("== 2/4 抓取财经快讯 ==", file=sys.stderr)
    try:
        crawl = AICrawler().run(skip_llm=args.skip_llm)
        print(f"  抓取 {crawl['fetched']} 条，入库 {crawl['saved']} 条", file=sys.stderr)
    except Exception as e:
        print(
            f"  快讯抓取失败（不影响后续）: {redact_sensitive_text(e)}",
            file=sys.stderr,
        )

    print("== 3/4 生成并保存每日选股 ==", file=sys.stderr)
    market_envelope = refresh_panel(symbols, args.start, end)
    panel = _usable_market_data(market_envelope, label=f"{args.universe} 面板")
    industry_map, industry_evidence = load_industry_analysis_context()
    decision_feature_inputs: dict[str, pd.DataFrame] = {}
    selection = hybrid_daily_selection(
        panel,
        top_n=args.top,
        horizon=args.holding_days,
        profile=args.profile,
        universe=args.universe,
        industry_map=industry_map,
        name_map=read_stock_names(symbols),
        evidence_sink=decision_feature_inputs,
    )
    selection["calculation_quality"] = selection.get("data_quality")
    selection["data_quality"] = market_envelope.quality.to_dict()
    selection["market_provenance"] = list(market_envelope.provenance)
    selection["universe_evidence"] = universe_snapshot.to_dict()
    selection["industry_evidence"] = industry_evidence
    save_allowed = (
        market_envelope.quality.formal_eligible
        and universe_snapshot.formal_eligible
        and bool(industry_evidence.get("formal_eligible"))
    )
    selection["persistence"] = {
        "requested": True,
        "saved": save_allowed,
        "status": "saved" if save_allowed else "blocked",
        "reason": (
            "" if save_allowed
            else "行情、候选池或行业证据未通过正式门；已生成预览但未写入正式历史"
        ),
    }
    if save_allowed:
        DecisionStore().save(
            selection, args.universe, panel={**panel, **decision_feature_inputs},
        )
    print(
        f"  {selection['signal_date']}：{len(selection['picks'])} 只候选，"
        f"建议仓位 {selection['recommended_exposure']:.0%}",
        file=sys.stderr,
    )
    if not save_allowed:
        print("== 4/4 正式执行已跳过；输出降级分析预览 ==", file=sys.stderr)
        _print_json(selection)
        return

    print("== 4/4 处理模拟订单并生成收盘提案 ==", file=sys.stderr)
    strategy = _paper_strategy_payload(args)
    service = get_paper_service()
    desired_spec = PaperAccountSpec.model_validate(
        {
            "name": "每日例程模拟盘",
            "strategy": strategy,
            "universe": args.universe,
            "initial_capital": args.capital,
            "mode": "manual",
        }
    )
    account = next(
        (item for item in service.store.accounts() if item["name"] == "每日例程模拟盘"),
        None,
    )
    if account is None:
        account = service.create_account(desired_spec)
    else:
        from quantmaster.backtest.spec import pin_decision_strategy

        desired_strategy = pin_decision_strategy(
            desired_spec.strategy,
            desired_spec.universe,
        ).model_dump(mode="json")
        if account["strategy"] != desired_strategy or account["universe"] != args.universe:
            raise ValueError("每日例程模拟盘的策略快照不同；请新建账户或恢复原参数")
    processed = service.process(account["id"], panel=panel)
    proposal = service.propose(account["id"], panel=panel)
    _print_json({"selection": selection, "processed": processed, "proposal": proposal})


def cmd_universe(args) -> None:
    from quantmaster.data.universe import (
        index_universe,
        load_universe_analysis_snapshot,
        save_universe,
    )

    if args.universe_cmd == "create":
        if args.index:
            symbols = index_universe(args.index)
        elif args.symbols:
            symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
        else:
            print("需要 --index（指数成分）或 --symbols（逗号分隔代码）", file=sys.stderr)
            raise SystemExit(1)
        save_universe(args.name, symbols)
        print(f"候选 {args.name} 已保存：{len(symbols)} 只")
    elif args.universe_cmd == "show":
        snapshot = load_universe_analysis_snapshot(args.name)
        symbols = list(snapshot.symbols)
        print(f"{args.name}: {len(symbols)} 只")
        if not snapshot.formal_eligible:
            print(f"  [sandbox] {'；'.join(snapshot.issues)}")
        for s in symbols:
            print(f"  {s}")
    else:
        from quantmaster.config import get_config

        pool_dir = get_config().data_root / "universe"
        names = ["demo（内置）"]
        if pool_dir.exists():
            names += sorted(p.stem for p in pool_dir.glob("*.json"))
        print("\n".join(names))


def cmd_ledger(args) -> None:
    from quantmaster.portfolio import Ledger, TradeRecord, ledger_report

    ledger = Ledger()
    if args.ledger_cmd == "nav":
        import pandas as pd

        from quantmaster.data import refresh_history
        from quantmaster.data.storage import BarStore
        from quantmaster.portfolio import daily_nav, nav_warnings, nav_with_benchmark

        trades = ledger.trades()
        if trades.empty:
            print("账本为空，先导入成交记录", file=sys.stderr)
            raise SystemExit(1)
        symbols = sorted(trades["symbol"].unique())
        start = str(pd.to_datetime(trades["date"]).min().date())
        end = _close_day()
        store = BarStore()
        prices = {}
        for symbol in symbols:
            try:
                prices[symbol] = _usable_market_data(
                    refresh_history(symbol, start, end, store=store), label=symbol,
                )["close"]
            except Exception as e:
                print(
                    f"  {symbol} 行情缺失（按最近成交价估值）: {redact_sensitive_text(e)}",
                    file=sys.stderr,
                )
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
            bench = _usable_market_data(
                refresh_history(args.benchmark, start, end, store=store),
                label=args.benchmark,
            )["close"]
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
        ledger.add_trade(
            TradeRecord(
                date=args.date or _today(),
                symbol=args.symbol,
                side=args.side,
                price=args.price,
                shares=args.shares,
                fee=args.fee,
            )
        )
        print("已记录")
    elif args.ledger_cmd == "cash":
        ledger.add_cashflow(args.date or _today(), args.amount, args.kind)
        print("已记录")
    else:
        _print_json(ledger_report(ledger))


def _research_assets(value: str):
    from quantmaster.research import AssetClass

    try:
        return tuple(AssetClass(item.strip().lower()) for item in value.split(",") if item.strip())
    except ValueError as exc:
        raise ValueError("资产只支持 stock,etf,future") from exc


def cmd_data(args) -> None:
    """Versioned research lake, capability and production job commands."""
    from quantmaster.research import KernelBackend
    from quantmaster.research.engine import ResearchEngine, save_plan

    engine = ResearchEngine()
    if args.data_cmd == "catalog":
        _print_json(engine.catalog())
        return
    if args.data_cmd == "capabilities":
        _print_json(engine.capabilities())
        return
    if args.data_cmd == "preview":
        trade_date = args.date or _close_day()
        frame = engine.preview_date(args.dataset, trade_date)
        rows = json.loads(
            frame.head(args.limit).to_json(
                orient="records", date_format="iso", force_ascii=False,
            )
        )
        _print_json({
            "tier": "sandbox",
            "dataset_id": args.dataset,
            "trade_date": trade_date,
            "row_count": len(frame),
            "returned_rows": len(rows),
            "quality": dict(frame.attrs.get("research_partition_quality") or {}),
            "rows": rows,
        })
        return
    if args.data_cmd in {"jobs", "status", "cancel", "resume"}:
        from quantmaster.research.jobs import get_research_job_manager

        manager = get_research_job_manager()
        if args.data_cmd in {"jobs", "status"}:
            _print_json({"items": manager.list(args.limit)})
        elif args.data_cmd == "cancel":
            _print_json(manager.cancel(args.job_id))
        else:
            resumed = manager.resume(args.job_id)
            _print_json(manager.wait(resumed["id"]))
        return
    if args.data_cmd == "materialize":
        from quantmaster.data.instruments import InstrumentStore
        from quantmaster.data.storage import BarStore

        symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
        if not symbols:
            candidates = BarStore().symbols()
            instruments = InstrumentStore().get_many(candidates)
            symbols = [
                symbol
                for symbol in candidates
                if (instruments.get(symbol) and instruments[symbol].asset_type == args.asset)
            ]
        records = engine.lake.materialize_bar_store(
            symbols,
            args.start,
            args.end or _close_day(),
            asset_class=_research_assets(args.asset)[0],
        )
        _print_json(
            {
                "asset_class": args.asset,
                "symbols": len(symbols),
                "partitions": len(records),
                "rows": sum(item["row_count"] for item in records),
            }
        )
        return

    end = args.end or _close_day()
    plan = engine.plan(
        args.start,
        end,
        asset_classes=_research_assets(args.assets),
        datasets=tuple(item.strip() for item in args.datasets.split(",") if item.strip()) or None,
        spec_ids=tuple(item.strip() for item in args.specs.split(",") if item.strip()) or None,
        mode=args.mode,
        backend=KernelBackend(args.backend),
    )
    if args.data_cmd == "plan":
        if args.output:
            save_plan(plan, args.output)
        _print_json(plan.to_dict())
        return

    def progress(index, total, task):
        print(f"[{index}/{total}] {task.key}", file=sys.stderr)

    result = engine.execute(plan, progress=progress)
    _print_json(result)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qm", description="QuantMaster — A股量化研究平台")
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="展开每次完整 traceback；可放在任意子命令前后",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p):
        p.add_argument("--universe", default="demo", help="候选名称（默认 demo）")
        p.add_argument("--start", default="2022-01-01")
        p.add_argument("--end", default=None)

    p = sub.add_parser("serve", help="启动 Web 界面")
    p.add_argument("--open", dest="open_browser", action="store_true", help="启动后自动打开浏览器")
    p.add_argument(
        "--reload",
        action="store_true",
        help="启用前端按钮触发的安全热更新（FreeStockDB 保持运行）",
    )
    p.add_argument(
        "--no-reload",
        dest="reload",
        action="store_false",
        help="关闭主站代码热更新",
    )
    p.set_defaults(reload=False)
    p.set_defaults(func=cmd_serve)

    p = sub.add_parser("activate", help="激活已验证的不可变使用槽并在失败时回滚")
    p.add_argument("build_sha", help="候选槽对应的完整 lowercase main SHA")
    p.add_argument("--root-pid", type=int, default=None, help="当前 QuantMaster root Job Object 的 PID")
    p.add_argument("--ready-timeout", type=float, default=15.0, help="候选健康检查期限（最多 15 秒）")
    p.set_defaults(func=cmd_activate)

    p = sub.add_parser("app", help="桌面模式：启动服务并自动打开浏览器（等价 serve --open）")
    p.set_defaults(func=cmd_serve, open_browser=True, reload=False)

    p = sub.add_parser("doctor", help="检查运行边界、存储完整性和工程约束")
    p.add_argument("--deep", action="store_true", help="逐库、逐文件并执行架构/API 深度检查")
    p.add_argument("--strict", action="store_true", help="存在 warning 时也返回非零状态")
    p.set_defaults(func=cmd_doctor)

    p = sub.add_parser("automation", help="Bot 推送与定时任务诊断/手动执行")
    asub = p.add_subparsers(dest="automation_cmd", required=True)
    asub.add_parser("doctor", help="检查依赖、Bot 账号、推送目标和任务状态")
    arun = asub.add_parser("run", help="立即提交一个自动化任务")
    arun.add_argument(
        "task",
        choices=[
            "intraday_monitor",
            "fast_news_scan",
            "official_news_scan",
            "periodic_news_scan",
            "daily_close_pipeline",
            "news_digest",
            "paper_rebalance_proposal",
        ],
    )
    adispatch = asub.add_parser("dispatch", help="立即投递待发消息")
    adispatch.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_automation)

    p = sub.add_parser("lab", help="AI Quant Lab：发现、训练、验证、审批和 Worker")
    lq = p.add_subparsers(dest="lab_cmd", required=True)
    ldoctor = lq.add_parser("doctor", help="紧凑检查数据、依赖、CUDA 与 Worker")
    ldoctor.add_argument("--json", action="store_true")
    lbenchmark = lq.add_parser("benchmark", help="运行零网络本地快照冷/热基准")
    lbenchmark.add_argument("--universe", default="csi800")
    lbenchmark.add_argument("--start", default="2015-01-01")
    lbenchmark.add_argument("--end", default=None)
    lbenchmark.add_argument("--runs", type=int, default=2)
    lq.add_parser("worker", help="启动独立研究 Worker")
    llist = lq.add_parser("list", help="列出版本化因子目录")
    llist.add_argument("--status", default=None)
    llist.add_argument("--search", default="")
    llist.add_argument("--limit", type=int, default=100)
    ljobs = lq.add_parser("jobs", help="查看研究任务")
    ljobs.add_argument("--limit", type=int, default=50)
    lstudies = lq.add_parser("studies", help="查看多目标优化 Study")
    lstudies.add_argument("--limit", type=int, default=50)
    lresume = lq.add_parser("resume", help="恢复暂停或中断的优化 Study")
    lresume.add_argument("study_id")

    def lab_common(parser):
        parser.add_argument("--universe", default="demo")
        parser.add_argument("--start", default="2022-01-01")
        parser.add_argument("--end", default=None)

    lprepare = lq.add_parser("prepare-data", help="冻结数据与候选快照")
    lab_common(lprepare)
    for lab_command in ("validate", "score"):
        item = lq.add_parser(lab_command, help="提交统一样本外验证任务")
        item.add_argument("version_id")
        lab_common(item)
    ldiscover = lq.add_parser("discover", help="提交遗传、DSL LLM 或 Python AutoMiner 任务")
    lab_common(ldiscover)
    ldiscover.add_argument("--method", choices=["genetic", "llm", "python"], default="genetic")
    ldiscover.add_argument("--horizon", type=int, choices=[1, 3, 5, 7, 10, 20, 30], default=3)
    ldiscover.add_argument("--top", type=int, default=10)
    ldiscover.add_argument("--population", type=int, default=60)
    ldiscover.add_argument("--generations", type=int, default=8)
    ldiscover.add_argument("--rounds", type=int, default=3)
    ldiscover.add_argument("--candidates", type=int, default=24)
    ldiscover.add_argument("--finalists", type=int, default=3)
    ltrain = lq.add_parser("train", help="提交 Ridge/深度学习实验")
    lab_common(ltrain)
    ltrain.add_argument(
        "--model", choices=["ridge", "mlp", "tcn", "gru", "transformer", "dae"], default="ridge"
    )
    ltrain.add_argument("--horizon", type=int, choices=[1, 3, 5, 7, 10, 20, 30], default=3)
    ltrain.add_argument("--sequence-length", type=int, default=20)
    ltrain.add_argument("--epochs", type=int, default=30)
    loptimize = lq.add_parser("optimize", help="提交共享多周期 Pareto 优化")
    loptimize.add_argument("--universe", default="csi800")
    loptimize.add_argument("--start", default="2015-01-01")
    loptimize.add_argument("--end", default=None)
    loptimize.add_argument(
        "--models",
        default="multi-transformer,multi-tcn,multi-gru,ridge",
        help="逗号分隔的共享模型；Ridge 始终可作 CPU 基线",
    )
    loptimize.add_argument("--budget-hours", type=float, default=10.0)
    loptimize.add_argument("--max-trials", type=int, default=40)
    loptimize.add_argument("--top", type=int, default=20)
    loptimize.add_argument("--sequence-length", type=int, default=20)
    loptimize.add_argument(
        "--research-tier",
        choices=["production", "sandbox"],
        default="production",
    )
    laudit = lq.add_parser("audit", help="提交防前视、递归稳定性与 PIT 审计")
    laudit.add_argument("version_id")
    laudit.add_argument("--universe", default="csi800")
    laudit.add_argument("--start", default="2015-01-01")
    laudit.add_argument("--end", default=None)
    lapprove = lq.add_parser("approve", help="人工批准候选版本")
    lapprove.add_argument("version_id")
    lapprove.add_argument("--reason", default="")
    ldeploy = lq.add_parser("deploy", help="设为研究生产 champion（不连接券商）")
    ldeploy.add_argument("version_id")
    ldeploy.add_argument("--universe", default="csi800")
    ldeploy.add_argument("--horizon", type=int, choices=[1, 3, 5, 7, 10, 20, 30], default=3)
    p.set_defaults(func=cmd_lab)

    p = sub.add_parser("fetch", help="预取行情到本地缓存")
    common(p)
    p.add_argument(
        "--frequency",
        default="1d",
        choices=["1d", "1m", "5m", "15m", "30m", "60m"],
        help="K线频率；分钟线会按频率独立长期归档",
    )
    p.add_argument("--force", action="store_true", help="忽略缓存强制刷新")
    p.set_defaults(func=cmd_fetch)

    p = sub.add_parser("data", help="跨资产研究目录、日期分区湖与生产任务")
    dsub = p.add_subparsers(dest="data_cmd", required=True)
    dsub.add_parser("catalog", help="列出数据集、版本化研究规格和本地覆盖")
    dsub.add_parser("capabilities", help="查看 Tushare 权限和 Python/Rust 内核状态")
    preview = dsub.add_parser("preview", help="预览不完整横截面；不写入正式研究湖")
    preview.add_argument("--dataset", default="stock_bars")
    preview.add_argument("--date", default=None)
    preview.add_argument("--limit", type=int, default=500)

    def data_plan_args(command):
        command.add_argument("--assets", default="stock", help="逗号分隔：stock,etf,future")
        command.add_argument("--datasets", default="", help="数据集 ID；留空使用资产日线基线")
        command.add_argument("--specs", default="", help="同时计算的因子/标签/风险规格 ID")
        command.add_argument("--start", default="2022-01-01")
        command.add_argument("--end", default=None)
        command.add_argument("--mode", choices=["historical", "incremental"], default="historical")
        command.add_argument("--backend", choices=["auto", "python", "rust"], default="auto")

    dp = dsub.add_parser("plan", help="只生成依赖、权限、分区和成本计划")
    data_plan_args(dp)
    dp.add_argument("--output", default="", help="可选的计划 JSON 输出路径")
    ds = dsub.add_parser("sync", help="前台执行历史或增量研究计划")
    data_plan_args(ds)
    dm = dsub.add_parser("materialize", help="从现有 BarStore 按需派生日期分区")
    dm.add_argument("--asset", choices=["stock", "etf", "future"], default="stock")
    dm.add_argument("--symbols", default="", help="逗号分隔；留空按证券主数据筛选缓存")
    dm.add_argument("--start", default="2022-01-01")
    dm.add_argument("--end", default=None)
    for command_name in ("jobs", "status"):
        job_parser = dsub.add_parser(command_name, help="查看持久化研究任务")
        job_parser.add_argument("--limit", type=int, default=50)
    dc = dsub.add_parser("cancel", help="请求取消正在由 Web/Worker 执行的任务")
    dc.add_argument("job_id")
    dr = dsub.add_parser("resume", help="恢复中断、取消或部分失败的任务")
    dr.add_argument("job_id")
    p.set_defaults(func=cmd_data, output="", mode="historical", backend="auto")

    p = sub.add_parser("regime", help="牛熊、上/下行/震荡、板块强弱与1-7日展望")
    common(p)
    p.add_argument("--history", type=int, default=60, help="输出最近多少个历史状态")
    p.add_argument("--sector-top", type=int, default=10)
    p.add_argument("--no-sectors", action="store_true", help="不加载行业映射")
    p.set_defaults(func=cmd_regime)

    p = sub.add_parser("select", help="Hybrid v2 每日决策：规则、Quant Lab 因子与批准模型")
    common(p)
    p.add_argument("--top", type=int, default=10)
    p.add_argument("--horizon", type=int, default=3, choices=[1, 3, 5, 7, 10, 20, 30])
    p.add_argument("--profile", default="risk_adjusted", choices=["risk_adjusted", "short_term", "stable"])
    p.add_argument("--no-industry", action="store_true", help="不加载行业名称")
    p.add_argument("--no-save", action="store_true", help="不保存本次决策快照")
    p.add_argument(
        "--policy-mode",
        choices=["live", "historical_replay", "retrospective"],
        default="live",
    )
    p.set_defaults(func=cmd_select)

    p = sub.add_parser("decisions", help="查看本地保存的历史选股快照")
    p.add_argument("--universe", default=None, help="按候选过滤")
    p.add_argument("--limit", type=int, default=30)
    p.set_defaults(func=cmd_decisions)

    p = sub.add_parser("after-close", help="free-stockdb 全 A 股盘后板块与研究候选")
    acsub = p.add_subparsers(dest="after_close_cmd", required=True)
    for command, help_text in (
        ("scan", "创建或复用当日盘后扫描"),
        ("rerun", "强制创建一次可复跑扫描"),
    ):
        item = acsub.add_parser(command, help=help_text)
        item.add_argument("--as-of", default="", help="历史重放截止日 YYYY-MM-DD")
    show = acsub.add_parser("show", help="查看最新或指定不可变快照")
    show.add_argument("--snapshot-id", default="")
    history = acsub.add_parser("history", help="查看盘后快照历史")
    history.add_argument("--limit", type=int, default=30)
    export = acsub.add_parser("export", help="导出快照 JSON/CSV")
    export.add_argument("output")
    export.add_argument("--snapshot-id", default="")
    export.add_argument("--format", choices=["json", "csv"], default="json")
    score_version = acsub.add_parser("score-version", help="查看、提升或回滚盘后评分版本")
    score_sub = score_version.add_subparsers(dest="score_version_cmd", required=True)
    score_sub.add_parser("status", help="查看 V2 人工评审资格与当前正式版本")
    score_sub.add_parser("promote", help="资格门通过后人工提升 V2，仅影响后续快照")
    score_sub.add_parser("rollback", help="将后续快照正式评分回滚到 V1")
    p.set_defaults(
        func=cmd_after_close,
        as_of="",
        snapshot_id="",
        limit=30,
        output="",
        format="json",
        score_version_cmd="",
    )

    p = sub.add_parser("stockdb", help="free-stockdb 制品、摄取与能力诊断")
    sdsub = p.add_subparsers(dest="stockdb_cmd", required=True)
    sdsub.add_parser("audit", help="运行本地 SDK、目录、板块与制品审计")
    sdsub.add_parser("status", help="查看托管运行时与最近摄取")
    native = sdsub.add_parser("validate-native", help="校验当前制品原生指标并写入兼容档案")
    native.add_argument("--start", default="")
    native.add_argument("--end", default="")
    sdi = sdsub.add_parser("ingest", help="通过正式完整性门创建或复用 A 股摄取")
    sdi.add_argument("--as-of", default="")
    p.set_defaults(func=cmd_stockdb, as_of="")

    p = sub.add_parser("etf-research", help="全场沪深 ETF 分类研究")
    ersub = p.add_subparsers(dest="etf_research_cmd", required=True)
    erscan = ersub.add_parser("scan", help="创建或复用 ETF 研究快照")
    erscan.add_argument("--as-of", default="")
    ershow = ersub.add_parser("show", help="查看最新或指定 ETF 快照")
    ershow.add_argument("--snapshot-id", default="")
    erhistory = ersub.add_parser("history", help="查看 ETF 研究快照历史")
    erhistory.add_argument("--limit", type=int, default=30)
    ercancel = ersub.add_parser("cancel", help="安全取消 ETF 研究任务")
    ercancel.add_argument("job_id")
    erresume = ersub.add_parser("resume", help="恢复失败、取消或中断的 ETF 研究任务")
    erresume.add_argument("job_id")
    erexport = ersub.add_parser("export", help="导出 ETF 研究快照")
    erexport.add_argument("output")
    erexport.add_argument("--snapshot-id", default="")
    erexport.add_argument("--format", choices=["json", "csv"], default="json")
    p.set_defaults(
        func=cmd_etf_research,
        as_of="",
        snapshot_id="",
        limit=30,
        output="",
        format="json",
        job_id="",
    )

    sub.add_parser("factors", help="列出内置因子").set_defaults(func=cmd_factors)

    p = sub.add_parser("factor-test", help="因子体检（IC/分层/换手）")
    p.add_argument("expression", help="内置因子名或表达式")
    common(p)
    p.add_argument("--quantiles", type=int, default=5)
    p.add_argument(
        "--neutralize", action="store_true", help="行业中性化：行业内去均值后再评估（剔除行业押注）"
    )
    p.set_defaults(func=cmd_factor_test)

    p = sub.add_parser("backtest", help="因子选股回测（--factor 逗号分隔多个名字 = 多因子组合）")
    common(p)
    p.add_argument(
        "--strategy",
        default="factor",
        choices=["factor", "decision"],
        help="factor=传统因子；decision=Hybrid v2",
    )
    p.add_argument("--profile", default="risk_adjusted", choices=["risk_adjusted", "short_term", "stable"])
    p.add_argument(
        "--factor", default="mom_20d", help="因子名/表达式；逗号分隔多个则做多因子合成，如 mom_20d,rev_5d,ep"
    )
    p.add_argument(
        "--weighting", default="equal", choices=["equal", "ic"], help="多因子合成方式：等权 或 滚动IC动态加权"
    )
    p.add_argument("--top", type=int, default=5)
    p.add_argument(
        "--holding-days",
        type=int,
        default=3,
        choices=[1, 3, 5, 7, 10, 20, 30],
        help="decision 策略持有与调仓周期",
    )
    p.add_argument("--rebalance", default="W", choices=["D", "W", "M"])
    p.add_argument("--benchmark", default="000300.SH")
    p.add_argument("--capital", type=float, default=1_000_000)
    p.add_argument("--stop-loss", type=float, default=None, help="止损线，如 0.08")
    p.add_argument("--take-profit", type=float, default=None, help="止盈线，如 0.25")
    p.add_argument("--full", action="store_true", help="输出年度/月度收益表")
    p.set_defaults(func=cmd_backtest)

    p = sub.add_parser("crawl", help="抓取财经快讯（+LLM 标注）")
    p.add_argument("--skip-llm", action="store_true", help="只抓取不做 LLM 标注")
    p.set_defaults(func=cmd_crawl)

    p = sub.add_parser("paper", help="多账户模拟盘")
    psub = p.add_subparsers(dest="paper_cmd", required=True)

    def paper_strategy_args(command):
        command.add_argument("--universe", default="demo")
        command.add_argument("--strategy", default="factor", choices=["factor", "decision"])
        command.add_argument(
            "--profile", default="risk_adjusted", choices=["risk_adjusted", "short_term", "stable"]
        )
        command.add_argument("--factor", default="mom_20d")
        command.add_argument("--top", type=int, default=5)
        command.add_argument("--holding-days", type=int, default=3, choices=[1, 3, 5, 7, 10, 20, 30])
        command.add_argument("--rebalance", default="W", choices=["D", "W", "M"])
        command.add_argument("--capital", type=float, default=1_000_000)

    pc = psub.add_parser("create", help="创建不可变策略快照账户")
    pc.add_argument("--name", required=True)
    pc.add_argument("--mode", default="manual", choices=["manual", "auto"])
    paper_strategy_args(pc)
    pp = psub.add_parser("propose", help="按最新收盘信号生成提案（不写成交）")
    pp.add_argument("--account", required=True)
    pcf = psub.add_parser("confirm", help="确认提案并进入待开盘")
    pcf.add_argument("--cycle", required=True)
    ppx = psub.add_parser("process", help="按下一可用交易日开盘处理订单")
    ppx.add_argument("--account", required=True)
    preport = psub.add_parser("report", help="模拟账户收益与订单报告")
    preport.add_argument("--account", default="")
    psub.add_parser("accounts", help="列出模拟账户")
    p.set_defaults(func=cmd_paper, capital=1_000_000)

    p = sub.add_parser("daily", help="每日例程：更新行情、抓快讯、处理订单并生成模拟提案")
    p.add_argument("--universe", default="demo")
    p.add_argument("--start", default="2022-01-01")
    p.add_argument("--strategy", default="decision", choices=["factor", "decision"])
    p.add_argument("--profile", default="risk_adjusted", choices=["risk_adjusted", "short_term", "stable"])
    p.add_argument("--factor", default="mom_20d")
    p.add_argument("--top", type=int, default=5)
    p.add_argument("--holding-days", type=int, default=3, choices=[1, 3, 5, 7, 10, 20, 30])
    p.add_argument("--rebalance", default="W", choices=["D", "W", "M"])
    p.add_argument("--benchmark", default="000300.SH")
    p.add_argument("--capital", type=float, default=1_000_000)
    p.add_argument("--skip-llm", action="store_true", help="快讯不做 LLM 标注")
    p.set_defaults(func=cmd_daily)

    p = sub.add_parser("universe", help="候选管理")
    usub = p.add_subparsers(dest="universe_cmd", required=True)
    uc = usub.add_parser("create", help="创建候选（指数成分或手动列表）")
    uc.add_argument("name")
    uc.add_argument("--index", default=None, help="指数代码，如 000300.SH（沪深300成分）")
    uc.add_argument("--symbols", default=None, help="逗号分隔的代码列表")
    us = usub.add_parser("show", help="查看候选")
    us.add_argument("name")
    usub.add_parser("list", help="列出全部候选")
    p.set_defaults(func=cmd_universe)

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


def _extract_verbose(argv: list[str]) -> tuple[list[str], bool]:
    """允许 ``--verbose`` 出现在任意子命令层级，但尊重 ``--`` 分隔符。"""
    cleaned: list[str] = []
    verbose = False
    positional_only = False
    for value in argv:
        if value == "--":
            positional_only = True
            cleaned.append(value)
        elif value == "--verbose" and not positional_only:
            verbose = True
        else:
            cleaned.append(value)
    return cleaned, verbose


def main(argv: list[str] | None = None) -> int:
    from quantmaster.logging_config import configure_logging

    raw_argv = list(sys.argv[1:] if argv is None else argv)
    if len(raw_argv) == 3 and raw_argv[0] == "__factor-runner":
        from quantmaster.factors.python_artifact import _worker

        return _worker(raw_argv[1], raw_argv[2])
    parsed_argv, verbose = _extract_verbose(raw_argv)
    args = build_parser().parse_args(parsed_argv)
    configure_logging(verbose=verbose)
    args.verbose = verbose
    try:
        result = args.func(args)
        return int(result or 0)
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("命令已停止")
        return 130
    except Exception as exc:
        from quantmaster.runtime.activation import ActivationBlocked

        if isinstance(exc, ActivationBlocked):
            _print_json({
                "status": "blocked",
                "blocker": {"reason": exc.code, "detail": exc.detail, **exc.context},
            })
            return 2
        logging.getLogger(__name__).exception(
            "命令执行失败",
            extra={"traceback_policy": "always"},
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
