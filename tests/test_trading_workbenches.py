"""回测工作台与多账户模拟盘的关键安全回归。"""

from __future__ import annotations

import hashlib
import json

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from quantmaster.backtest import BacktestConfig, run_backtest
from quantmaster.backtest.paper_accounts import PaperService, PaperStore
from quantmaster.backtest.spec import (
    BacktestSpec,
    DecisionStrategySpec,
    PaperAccountSpec,
    build_strategy,
    pin_decision_strategy,
)
from quantmaster.backtest.workbench import BacktestService, BacktestStore, get_backtest_worker
from quantmaster.portfolio import Ledger, TradeRecord
from quantmaster.server.app import app
from quantmaster.server.management import _issue_csrf


def price_panel(dates, first=(10.0, 11.0), second=None):
    index = pd.DatetimeIndex(dates)
    columns = ["600000.SH", "000001.SZ"]
    close_rows = [first] * len(index) if second is None else [first] * (len(index) - 1) + [second]
    close = pd.DataFrame(close_rows, index=index, columns=columns, dtype=float)
    return {
        "open": close.copy(), "high": close.copy(), "low": close.copy(),
        "close": close.copy(), "volume": close * 100_000,
    }


def account_spec(name="日频验证", *, rebalance="D"):
    return PaperAccountSpec.model_validate({
        "name": name,
        "strategy": {
            "kind": "factor", "factor": "rank(close)", "top_n": 1,
            "rebalance": rebalance, "weighting": "equal", "cap_weight": 0.35,
        },
        "universe": "demo", "initial_capital": 100_000, "mode": "manual",
    })


def make_paper_service(tmp_path, name="日频验证", *, rebalance="D"):
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts")
    account = store.create_account(
        account_spec(name, rebalance=rebalance),
        symbols=["600000.SH", "000001.SZ"],
    )
    return PaperService(store), account


def test_paper_confirmation_does_not_write_before_next_open(tmp_path):
    service, account = make_paper_service(tmp_path)
    signal_panel = price_panel(pd.bdate_range("2024-01-01", periods=5))
    proposal = service.propose(account["id"], panel=signal_panel)
    ledger = service.store.ledger(account["id"])
    assert proposal["status"] == "proposed"
    assert proposal["ledger_written"] is False
    assert ledger.trades().empty

    confirmed = service.store.confirm(proposal["id"])
    assert confirmed["status"] == "confirmed"
    assert ledger.trades().empty

    waiting = service.process(account["id"], panel=signal_panel)
    assert waiting["status"] == "waiting_open"
    assert ledger.trades().empty


def test_paper_executes_t_plus_one_open_and_never_overdraws(tmp_path):
    service, account = make_paper_service(tmp_path)
    dates = pd.bdate_range("2024-01-01", periods=6)
    proposal = service.propose(account["id"], panel=price_panel(dates[:-1]))
    service.store.confirm(proposal["id"])
    result = service.process(account["id"], panel=price_panel(dates))
    trades = service.store.ledger(account["id"]).trades()

    assert result["status"] == "completed"
    assert not trades.empty
    assert set(trades["date"]) == {str(dates[-1].date())}
    assert (trades["price"] == 11.0 * (1 + 0.001)).all()
    assert result["report"]["cash"] >= 0
    assert (trades["shares"] % 100 == 0).all()


def test_backtest_and_paper_share_first_open_fill_semantics(tmp_path):
    service, account = make_paper_service(tmp_path)
    dates = pd.bdate_range("2024-01-01", periods=6)
    panel = price_panel(dates)
    proposal = service.propose(account["id"], panel={key: value.iloc[:-1] for key, value in panel.items()})
    service.store.confirm(proposal["id"])
    paper_result = service.process(account["id"], panel=panel)
    paper_trade = paper_result["filled"][0]

    weights = pd.DataFrame(float("nan"), index=dates, columns=panel["close"].columns)
    weights.loc[dates[-2]] = pd.Series(proposal["target_weights"])
    backtest = run_backtest(
        panel, weights, BacktestConfig(initial_capital=100_000),
    )
    matching = next(trade for trade in backtest.trades if trade.symbol == paper_trade["symbol"])

    assert matching.date == paper_trade["date"]
    assert matching.shares == paper_trade["shares"]
    assert matching.price == paper_trade["price"]
    assert matching.cost == paper_trade["fee"]


def test_limit_up_order_stays_blocked_and_retries_next_session(tmp_path):
    service, account = make_paper_service(tmp_path)
    signal_dates = pd.bdate_range("2024-01-01", periods=5)
    signal = price_panel(signal_dates, first=(9.0, 10.0))
    proposal = service.propose(account["id"], panel=signal)
    service.store.confirm(proposal["id"])

    monday = signal_dates[-1] + pd.offsets.BDay(1)
    blocked_panel = price_panel([*signal_dates, monday], first=(9.0, 10.0))
    blocked_panel["open"].loc[monday, "000001.SZ"] = 11.0
    blocked_panel["close"].loc[monday, "000001.SZ"] = 11.0
    blocked = service.process(account["id"], panel=blocked_panel)
    assert blocked["status"] == "blocked"
    assert blocked["blocked"][0]["reason"] == "limit_up"
    assert service.store.ledger(account["id"]).trades().empty

    tuesday = monday + pd.offsets.BDay(1)
    retry_panel = price_panel([*signal_dates, monday, tuesday], first=(9.0, 10.0))
    retry_panel["close"].loc[monday, "000001.SZ"] = 11.0
    retry_panel["open"].loc[tuesday, "000001.SZ"] = 10.8
    retried = service.process(account["id"], panel=retry_panel)
    assert retried["status"] == "completed"
    assert set(service.store.ledger(account["id"]).trades()["date"]) == {str(tuesday.date())}


def test_weekly_paper_signal_is_not_fabricated_midweek(tmp_path):
    service, account = make_paper_service(tmp_path, rebalance="W")
    dates = pd.bdate_range("2024-01-01", periods=3)  # 周一至周三
    result = service.propose(account["id"], panel=price_panel(dates))
    assert result["status"] == "not_due"
    assert service.store.cycles(account["id"]) == []


def test_paper_accounts_are_isolated_and_strategy_snapshot_is_immutable(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts")
    first = store.create_account(account_spec("账户 A"), symbols=["600000.SH"])
    second = store.create_account(account_spec("账户 B"), symbols=["000001.SZ"])
    store.ledger(first["id"]).add_trade(TradeRecord(
        date="2024-01-02", symbol="600000.SH", side="buy", price=10, shares=100,
    ))
    assert len(store.ledger(first["id"]).trades()) == 1
    assert store.ledger(second["id"]).trades().empty
    assert store.account(first["id"])["strategy_hash"] == first["strategy_hash"]
    with pytest.raises(ValueError, match="已存在"):
        store.create_account(account_spec("账户 A"), symbols=["600000.SH"])


def test_removed_holding_is_quoted_but_not_ranked_for_new_target(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts")
    account = store.create_account(
        account_spec("候选边界"), symbols=["600000.SH", "000002.SZ"],
    )
    ledger = store.ledger(account["id"])
    ledger.add_trade(TradeRecord(
        date="2023-12-29", symbol="000001.SZ", side="buy", price=10, shares=100,
    ))
    service = PaperService(store)
    dates = pd.bdate_range("2024-01-01", periods=5)
    panel = price_panel(dates, first=(10.0, 100.0))
    for frame in panel.values():
        frame["000002.SZ"] = 20.0

    proposal = service.propose(account["id"], panel=panel)

    assert proposal["target_weights"]["000002.SZ"] > 0
    assert proposal["target_weights"]["000001.SZ"] == 0


def test_unapproved_strategy_is_allowed_with_persistent_warning():
    service = PaperService(PaperStore())
    account = service.create_account(account_spec("未批准策略"))
    assert account["status"] == "active"
    assert "未关联" in account["warning"]
    assert service.report(account["id"])["warnings"][0] == account["warning"]


def test_daily_orchestration_processes_all_active_and_proposes_only_auto(tmp_path, monkeypatch):
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts")
    manual = store.create_account(account_spec("人工账户"), symbols=["600000.SH"])
    automatic_spec = account_spec("自动账户").model_copy(update={"mode": "auto"})
    automatic = store.create_account(automatic_spec, symbols=["000001.SZ"])
    service = PaperService(store)
    processed, proposed = [], []
    monkeypatch.setattr(
        service, "process",
        lambda account_id: processed.append(account_id) or {"status": "idle"},
    )
    monkeypatch.setattr(
        service, "propose",
        lambda account_id: proposed.append(account_id) or {"status": "not_due"},
    )

    result = service.run_active_accounts()

    assert set(processed) == {manual["id"], automatic["id"]}
    assert proposed == [automatic["id"]]
    assert result["processed"] == 2
    assert result["failed"] == 0


def test_legacy_paper_ledger_migration_is_idempotent_and_preserves_source(isolated_config):
    root = isolated_config.data_root
    source = root / "ledger_paper.sqlite"
    legacy = Ledger(path=source)
    legacy.add_cashflow("2024-01-01", 50_000, "deposit")
    legacy.add_trade(TradeRecord(
        date="2024-01-02", symbol="600000.SH", side="buy", price=10, shares=100,
    ))
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    store = PaperStore()
    first = store.migrate_legacy()
    second = store.migrate_legacy()

    assert first["id"] == second["id"]
    assert first["status"] == "paused"
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    assert len(store.ledger(first["id"]).trades()) == 1


def test_backtest_store_persists_artifact_events_compare_and_cancel(tmp_path, panel):
    store = BacktestStore(tmp_path / "runs.sqlite", tmp_path / "artifacts")
    service = BacktestService(store)
    spec = BacktestSpec.model_validate({
        "name": "合成行情",
        "strategy": {
            "kind": "factor", "factor": "rank(close)", "top_n": 2,
            "rebalance": "D", "weighting": "equal", "cap_weight": 0.35,
        },
        "universe": "demo", "start": "2023-01-02", "end": "2023-08-01",
        "benchmark": None, "initial_capital": 100_000,
    })
    run = store.create(spec)
    manifest, payload = service.run(
        run, panel=panel, progress=lambda value, phase, detail: store.update(
            run["id"], value, phase, detail,
        ), cancelled=lambda: False,
    )
    path = store.write_artifact(run["id"], payload["artifact"])
    store.finish(
        run["id"], manifest=manifest, result=payload["summary"], artifact_path=str(path),
    )
    completed = store.get(run["id"], include_artifact=True)
    assert completed["status"] == "completed"
    assert completed["artifact"]["manifest"]["config_hash"] == spec.snapshot_hash
    assert store.events(run["id"])[-1]["type"] == "completed"

    second = store.create(spec.model_copy(update={"name": "对照"}))
    path2 = store.write_artifact(second["id"], payload["artifact"])
    store.finish(second["id"], manifest=manifest, result=payload["summary"], artifact_path=str(path2))
    assert len(service.compare([run["id"], second["id"]])["runs"]) == 2

    queued = store.create(spec.model_copy(update={"name": "待取消"}))
    cancelled = store.cancel(queued["id"])
    assert cancelled["status"] == "cancelled"
    assert cancelled["cancel_requested"] is True


def test_hybrid_decision_snapshot_is_shared_by_backtest_and_paper(
    tmp_path, panel, monkeypatch,
):
    from quantmaster.decision import resolve_policy
    from quantmaster.lab.store import LabStore

    symbols = list(panel["close"].columns)
    policy = resolve_policy(
        "demo", 1, "stable", symbols=symbols,
        store=LabStore(tmp_path / "lab.sqlite"),
    )
    decision = DecisionStrategySpec.model_validate({
        "kind": "decision", "profile": "stable", "top_n": 3,
        "holding_days": 1, "cap_weight": 0.25, "policy_snapshot": policy,
    })
    strategy = build_strategy(
        decision, symbols, "2023-01-02", "2023-08-01", universe="demo",
    )
    weights = strategy.target_weights(panel)
    assert not weights.dropna(how="all").empty
    assert weights.fillna(0).sum(axis=1).max() <= 0.65 + 1e-9

    backtest_store = BacktestStore(tmp_path / "runs.sqlite", tmp_path / "artifacts")
    run = BacktestService(backtest_store).enqueue(BacktestSpec.model_validate({
        "name": "Hybrid 快照", "strategy": decision.model_dump(mode="json"),
        "universe": "demo", "start": "2023-01-02", "end": "2023-08-01",
        "benchmark": None, "initial_capital": 100_000,
    }))
    assert run["config"]["strategy"]["policy_snapshot"]["policy_hash"] == policy["policy_hash"]

    paper = PaperService(PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts"))
    monkeypatch.setattr(
        paper, "_resolve_universe", lambda name, as_of: (symbols, {"quality": "sandbox"}),
    )
    account = paper.create_account(PaperAccountSpec.model_validate({
        "name": "Hybrid 模拟", "strategy": decision.model_dump(mode="json"),
        "universe": "demo", "initial_capital": 100_000, "mode": "manual",
    }))
    proposal = paper.propose(account["id"], panel=panel)
    assert proposal["status"] == "proposed"
    stored_policy = paper.store.account(account["id"])["strategy"]["policy_snapshot"]
    assert stored_policy["policy_hash"] == policy["policy_hash"]

    tampered = json.loads(json.dumps(policy))
    tampered["risk"]["max_exposure"] = 1.0
    with pytest.raises(ValueError, match="完整性"):
        pin_decision_strategy(
            decision.model_copy(update={"policy_snapshot": tampered}), "demo",
        )


def test_trading_api_requires_csrf_and_ui_exposes_workflow_contract(monkeypatch):
    client = TestClient(app)
    worker = get_backtest_worker()
    monkeypatch.setattr(worker, "start", lambda: None)
    payload = {
        "name": "接口回测",
        "strategy": {"kind": "swing", "top_n": 3, "holding_days": 3, "cap_weight": 0.25},
        "universe": "demo", "start": "2023-01-01", "end": "2023-12-31",
        "benchmark": None, "initial_capital": 100_000,
    }
    assert client.post("/api/backtests", json=payload).status_code == 403
    token = _issue_csrf()
    client.cookies.set("qm_csrf", token)
    created = client.post(
        "/api/backtests", json=payload, headers={"X-CSRF-Token": token},
    )
    assert created.status_code == 202
    assert created.json()["status"] == "queued"
    worker.service.store.cancel(created.json()["id"])

    page = client.get("/").text
    assert 'href="/static/trading.css"' in page
    assert 'src="/static/trading.js"' in page
    assert "回测工作台" in page
    assert "Hybrid v2 决策" in page
    assert 'data-bt-field="decision"' in page
    assert 'data-paper-field="decision"' in page
    assert "生成调仓提案" in page
    assert "确认并等待开盘" not in page  # 仅在真实提案渲染后出现
