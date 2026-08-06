"""回测工作台与多账户模拟盘的关键安全回归。"""

from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from quantmaster.backtest import BacktestConfig, run_backtest
from quantmaster.backtest.paper_accounts import (
    PaperService,
    PaperStore,
)
from quantmaster.backtest.paper_automation import PaperAutomationWorker
from quantmaster.backtest.spec import (
    BacktestSpec,
    DecisionStrategySpec,
    PaperAccountSpec,
    build_strategy,
    pin_decision_strategy,
    split_factor_references,
)
from quantmaster.backtest.workbench import BacktestService, BacktestStore, get_backtest_worker
from quantmaster.portfolio import TradeRecord
from quantmaster.server.app import app
from quantmaster.server.management import _issue_csrf
from quantmaster.trading_sessions import SessionExpectation


def price_panel(dates, first=(10.0, 11.0), second=None):
    index = pd.DatetimeIndex(dates)
    columns = ["600000.SH", "000001.SZ"]
    close_rows = [first] * len(index) if second is None else [first] * (len(index) - 1) + [second]
    close = pd.DataFrame(close_rows, index=index, columns=columns, dtype=float)
    return {
        "open": close.copy(),
        "high": close.copy(),
        "low": close.copy(),
        "close": close.copy(),
        "volume": close * 100_000,
    }


_ROUTE_STRATEGY = {"kind": "swing", "top_n": 3, "holding_days": 3, "cap_weight": 0.25}
_ROUTE_ARTIFACT = {
    "summary": {"return": 0.1},
    "trades": [
        {
            "date": "2026-08-04",
            "symbol": "600000.SH",
            "side": "buy",
            "price": 10.0,
            "shares": 100,
            "amount": 1000.0,
            "cost": 1.0,
            "note": "open",
        }
    ],
}


class _RouteAutoWorker:
    def __init__(self, wake_count):
        self.wake_count = wake_count

    def wake(self):
        self.wake_count[0] += 1


class _RouteBacktestStore:
    def list(self, limit):
        return [{"id": "completed", "limit": limit}]

    def get(self, run_id, include_artifact=False):
        if run_id == "missing":
            return None
        return {
            "id": run_id,
            "status": "queued" if run_id == "queued" else "completed",
            "config": {
                "strategy": _ROUTE_STRATEGY,
                "universe": "demo",
                "initial_capital": 100_000,
            },
            "artifact": _ROUTE_ARTIFACT if include_artifact and run_id != "queued" else None,
        }

    def events(self, run_id, after=0):
        if run_id == "broken":
            raise KeyError("回测不存在")
        return [{"run_id": run_id, "after": after}]

    def cancel(self, run_id):
        if run_id == "broken":
            raise ValueError("不能取消")
        return {"id": run_id, "status": "cancelled"}


class _RouteBacktestService:
    def __init__(self):
        self.store = _RouteBacktestStore()

    def compare(self, run_ids):
        if "broken" in run_ids:
            raise ValueError("比较失败")
        return {"run_ids": run_ids}


class _RouteQueueService:
    def __init__(self):
        self.fail = False

    def enqueue(self, spec):
        if self.fail:
            raise ValueError("未知字段")
        return {"id": "new-run", "status": "queued", "name": spec.name}


class _RouteBacktestWorker:
    def __init__(self):
        self.service = _RouteQueueService()
        self.started = 0

    def start(self):
        self.started += 1


class _RoutePaperStore:
    def accounts(self, include_archived=False):
        return [{"id": "account", "include_archived": include_archived}]

    def account(self, account_id):
        return (
            None
            if account_id == "missing"
            else {
                "id": account_id,
                "status": "active",
                "mode": "manual",
            }
        )

    def update_account(self, account_id, status=None, mode=None):
        if account_id == "missing":
            raise KeyError("模拟账户不存在")
        return {"id": account_id, "status": status or "active", "mode": mode or "manual"}

    def confirm(self, cycle_id):
        if cycle_id == "missing":
            raise KeyError("周期不存在")
        return {"id": cycle_id, "status": "confirmed"}

    def cycles(self, account_id, limit=30):
        return [{"account_id": account_id, "limit": limit}]


class _RoutePaperService:
    def __init__(self):
        self.store = _RoutePaperStore()

    def create_account(self, spec):
        return {"id": "account", "status": "active", "mode": spec.mode}

    def account_details(self, account_id):
        account = self.store.account(account_id)
        if account is None:
            raise KeyError("模拟账户不存在")
        return {**account, "activity": {"strategy_editable": True}}

    def update_account(
        self,
        account_id,
        *,
        name=None,
        status=None,
        mode=None,
        strategy=None,
        universe=None,
    ):
        if account_id == "missing":
            raise KeyError("模拟账户不存在")
        return {
            "id": account_id,
            "name": name or "route account",
            "status": status or "active",
            "mode": mode or "manual",
            "strategy": strategy.model_dump(mode="json") if strategy is not None else _ROUTE_STRATEGY,
            "universe": universe or "demo",
        }

    def archive_account(self, account_id):
        if account_id == "missing":
            raise KeyError("模拟账户不存在")
        return {"id": account_id, "status": "archived"}

    def clone_account(self, account_id, name, mode):
        if account_id == "missing":
            raise KeyError("模拟账户不存在")
        return {"id": "clone", "name": name, "status": "active", "mode": mode}

    def propose(self, account_id):
        if account_id == "missing":
            raise KeyError("模拟账户不存在")
        return {"account_id": account_id, "status": "proposed"}

    def process(self, account_id):
        if account_id == "missing":
            raise ValueError("处理失败")
        return {"account_id": account_id, "status": "processed"}

    def report(self, account_id):
        if account_id == "missing":
            raise KeyError("模拟账户不存在")
        return {"account_id": account_id, "warning": ""}


def test_backtest_json_export_is_strict_for_nonfinite_artifact_values(monkeypatch):
    from quantmaster.server import trading

    class Store:
        @staticmethod
        def get(run_id, include_artifact=False):
            return {
                "id": run_id,
                "status": "completed",
                "artifact": {"metric": float("nan"), "values": [float("inf")]},
            }

    service = type("Service", (), {"store": Store()})()
    monkeypatch.setattr(trading, "_service", lambda: service)

    response = TestClient(app).get("/api/v1/backtests/export-strict/export")

    assert response.status_code == 200
    assert response.json() == {"metric": None, "values": [None]}
    assert b"NaN" not in response.content and b"Infinity" not in response.content


def account_spec(name="日频验证", *, rebalance="D"):
    return PaperAccountSpec.model_validate(
        {
            "name": name,
            "strategy": {
                "kind": "factor",
                "factor": "rank(close)",
                "top_n": 1,
                "rebalance": rebalance,
                "weighting": "equal",
                "cap_weight": 0.35,
            },
            "universe": "demo",
            "initial_capital": 100_000,
            "mode": "manual",
        }
    )


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
        panel,
        weights,
        BacktestConfig(initial_capital=100_000),
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
    store.ledger(first["id"]).add_trade(
        TradeRecord(
            date="2024-01-02",
            symbol="600000.SH",
            side="buy",
            price=10,
            shares=100,
        )
    )
    assert len(store.ledger(first["id"]).trades()) == 1
    assert store.ledger(second["id"]).trades().empty
    assert store.account(first["id"])["strategy_hash"] == first["strategy_hash"]
    with pytest.raises(ValueError, match="已存在"):
        store.create_account(account_spec("账户 A"), symbols=["600000.SH"])


def test_paper_strategy_change_preserves_history_and_schedules_transition(
    tmp_path,
    monkeypatch,
    panel,
):
    service, account = make_paper_service(tmp_path, "可编辑账户")
    original_hash = account["strategy_hash"]
    same_strategy = account_spec("任意名称").strategy
    monkeypatch.setattr(
        service,
        "_resolve_universe",
        lambda *_args: pytest.fail("只改名称不应重新解析候选快照"),
    )

    renamed = service.update_account(
        account["id"],
        name="只改名称",
        strategy=same_strategy,
        universe="demo",
    )
    assert renamed["name"] == "只改名称"
    assert renamed["strategy_hash"] == original_hash

    monkeypatch.setattr(
        service,
        "_resolve_universe",
        lambda _name, _as_of: (["600000.SH", "600001.SH"], {"quality": "fixture"}),
    )
    monkeypatch.setattr(service, "_strategy_change_signal_date", lambda: "2026-08-06")
    recent_index = pd.bdate_range(end="2026-08-06", periods=len(panel["close"]))
    recent_panel = {name: values.set_axis(recent_index) for name, values in panel.items()}
    monkeypatch.setattr("quantmaster.data.load_panel", lambda *_args, **_kwargs: recent_panel)
    changed_strategy = same_strategy.model_copy(update={"top_n": 2})
    changed = service.update_account(account["id"], strategy=changed_strategy)
    assert changed["strategy"]["top_n"] == 2
    assert changed["strategy_hash"] != original_hash
    assert changed["transition"]["status"] == "confirmed", changed["transition"]
    assert changed["transition"]["signal_date"] == "2026-08-06"
    details = service.account_details(account["id"])
    assert details["management"]["strategy_editable"] is True
    assert details["management"]["pending_strategy_change"] is False
    assert details["management"] == {
        "strategy_editable": True,
        "pending_strategy_change": False,
        "strategy_effective_after": "",
        "can_archive": True,
        "can_restore": False,
        "delete_mode": "archive",
    }

    prior_cycles = service.store.cycles(account["id"])
    changed_again = service.update_account(
        account["id"],
        strategy=changed_strategy.model_copy(update={"top_n": 1}),
    )
    assert changed_again["strategy"]["top_n"] == 1
    assert any(
        cycle["status"] == "superseded"
        for cycle in service.store.cycles(account["id"])
        if cycle["id"] in {item["id"] for item in prior_cycles}
    )

    before_close = datetime(2026, 8, 6, 14, 59, tzinfo=ZoneInfo("Asia/Shanghai"))
    after_close = datetime(2026, 8, 6, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert PaperService._strategy_change_signal_date(before_close) == "2026-08-06"
    assert PaperService._strategy_change_signal_date(after_close) == "2026-08-07"


def test_paper_delete_is_recoverable_and_preserves_history(tmp_path):
    service, account = make_paper_service(tmp_path, "可恢复账户")
    cycle, _created = service.store.create_cycle(
        account,
        "2026-08-05",
        {"600000.SH": 0.35},
        {"600000.SH": 10.0},
        [],
    )

    archived = service.archive_account(account["id"])

    assert archived["status"] == "archived"
    assert service.store.cycles(account["id"])[0]["status"] == "superseded"
    assert service.store.orders(cycle_id=cycle["id"])[0]["status"] == "superseded"
    assert service.store.accounts() == []
    assert service.store.accounts(include_archived=True)[0]["id"] == account["id"]
    assert not service.store.ledger(account["id"]).cashflows().empty
    assert service.account_details(account["id"])["management"] == {
        "strategy_editable": False,
        "pending_strategy_change": False,
        "strategy_effective_after": "",
        "can_archive": False,
        "can_restore": True,
        "delete_mode": "archive",
    }

    restored = service.update_account(account["id"], status="paused")
    assert restored["status"] == "paused"
    assert service.store.accounts()[0]["id"] == account["id"]


def test_removed_holding_is_quoted_but_not_ranked_for_new_target(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts")
    account = store.create_account(
        account_spec("候选边界"),
        symbols=["600000.SH", "000002.SZ"],
    )
    ledger = store.ledger(account["id"])
    ledger.add_trade(
        TradeRecord(
            date="2023-12-29",
            symbol="000001.SZ",
            side="buy",
            price=10,
            shares=100,
        )
    )
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


def test_paper_creation_reuses_resolved_symbols_when_pinning_decision(
    tmp_path,
    monkeypatch,
):
    symbols = ["600000.SH", "000001.SZ"]
    service = PaperService(PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts"))
    monkeypatch.setattr(
        service,
        "_resolve_universe",
        lambda name, as_of: (symbols, {"as_of": as_of, "quality": "sandbox"}),
    )
    monkeypatch.setattr(
        "quantmaster.data.universe.load_universe",
        lambda name: pytest.fail("候选已经解析，不应在固化策略时重复读取"),
    )
    spec = PaperAccountSpec.model_validate(
        {
            "name": "一次解析",
            "strategy": {
                "kind": "decision",
                "profile": "risk_adjusted",
                "top_n": 5,
                "holding_days": 3,
                "cap_weight": 0.25,
                "policy_snapshot": {},
            },
            "universe": "large-custom",
            "initial_capital": 100_000,
            "mode": "manual",
        }
    )

    account = service.create_account(spec)

    assert account["universe_snapshot"]["symbols"] == sorted(symbols)
    assert account["strategy"]["policy_snapshot"]["universe"] == "large-custom"


def test_daily_orchestration_processes_all_active_and_proposes_only_auto(tmp_path, monkeypatch):
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts")
    manual = store.create_account(account_spec("人工账户"), symbols=["600000.SH"])
    automatic_spec = account_spec("自动账户").model_copy(update={"mode": "auto"})
    automatic = store.create_account(automatic_spec, symbols=["000001.SZ"])
    service = PaperService(store)
    processed, proposed = [], []
    monkeypatch.setattr(
        service,
        "process",
        lambda account_id: processed.append(account_id) or {"status": "idle"},
    )
    monkeypatch.setattr(
        service,
        "propose",
        lambda account_id: proposed.append(account_id) or {"status": "not_due"},
    )

    result = service.run_active_accounts()

    assert set(processed) == {manual["id"], automatic["id"]}
    assert proposed == [automatic["id"]]
    assert result["processed"] == 2
    assert result["failed"] == 0


def test_auto_account_runs_daily_without_manual_buttons_and_is_idempotent(tmp_path, monkeypatch):
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts")
    manual = store.create_account(account_spec("人工账户"), symbols=["600000.SH"])
    automatic = store.create_account(
        account_spec("自动账户").model_copy(update={"mode": "auto"}),
        symbols=["000001.SZ"],
    )
    service = PaperService(store)
    processed, proposed = [], []
    monkeypatch.setattr(
        service,
        "process",
        lambda account_id: processed.append(account_id) or {"status": "completed"},
    )
    monkeypatch.setattr(
        service,
        "propose",
        lambda account_id: (
            proposed.append(account_id)
            or {
                "status": "confirmed",
                "signal_date": "2026-08-04",
            }
        ),
    )
    worker = PaperAutomationWorker(
        service,
        poll_seconds=1,
        session_resolver=lambda _now: SessionExpectation(
            "2026-08-04",
            "unit",
            True,
            "fixture",
        ),
    )
    due = datetime(2026, 8, 4, 18, 31, tzinfo=ZoneInfo("Asia/Shanghai"))

    first = worker.run_due_once(due)
    second = worker.run_due_once(due)

    assert first["status"] == "completed"
    assert second["status"] == "already_processed"
    assert processed == [automatic["id"]]
    assert proposed == [automatic["id"]]
    assert manual["id"] not in processed
    assert store.latest_auto_run(automatic["id"])["status"] == "completed"


def test_auto_run_lease_token_fences_old_worker_and_exhausts_at_six(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts")
    account = store.create_account(
        account_spec("租约账户").model_copy(update={"mode": "auto"}),
        symbols=["600000.SH"],
    )
    account_id = account["id"]
    token_one = store.claim_auto_run("2026-02-13", account_id, "worker-one", now=100)
    assert token_one
    with store._conn() as connection:
        connection.execute(
            "UPDATE paper_auto_runs SET lease_expires=0 WHERE run_date=? AND account_id=?",
            ("2026-02-13", account_id),
        )
    token_two = store.claim_auto_run("2026-02-13", account_id, "worker-two", now=101)
    assert token_two and token_two != token_one
    assert (
        store.complete_auto_run(
            "2026-02-13",
            account_id,
            "worker-one",
            token_one,
            {"old": True},
            now=101,
        )
        is False
    )
    assert (
        store.complete_auto_run(
            "2026-02-13",
            account_id,
            "worker-two",
            token_two,
            {"new": True},
            now=101,
        )
        is True
    )

    current = 1000.0
    for attempt in range(6):
        token = store.claim_auto_run(
            "2026-10-09",
            account_id,
            "worker",
            now=current,
        )
        assert token
        assert store.fail_auto_run(
            "2026-10-09",
            account_id,
            "worker",
            token,
            "暂时失败",
            now=current,
        )
        current += (3 * 60 * 60) + attempt
    latest = store.latest_auto_run(account_id)
    assert latest["status"] == "manual_recovery"
    assert latest["attempts"] == 6
    assert store.recover_auto_run("2026-10-09", account_id) is True


def test_success_clears_runtime_warning_but_keeps_strategy_warning(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts")
    account = store.create_account(
        account_spec("告警分层"),
        symbols=["600000.SH"],
        warning="策略来源待批准",
    )
    store.set_runtime_warning(account["id"], "行情暂不可用")
    assert store.account(account["id"])["warning"] == "行情暂不可用"
    store.clear_runtime_warning(account["id"])
    restored = store.account(account["id"])
    assert restored["runtime_warning"] == ""
    assert restored["strategy_warning"] == "策略来源待批准"
    assert restored["warning"] == "策略来源待批准"


def test_backtest_store_persists_artifact_events_compare_and_cancel(tmp_path, panel):
    store = BacktestStore(tmp_path / "runs.sqlite", tmp_path / "artifacts")
    service = BacktestService(store)
    spec = BacktestSpec.model_validate(
        {
            "name": "合成行情",
            "strategy": {
                "kind": "factor",
                "factor": "rank(close)",
                "top_n": 2,
                "rebalance": "D",
                "weighting": "equal",
                "cap_weight": 0.35,
            },
            "universe": "demo",
            "start": "2023-01-02",
            "end": "2023-08-01",
            "benchmark": None,
            "initial_capital": 100_000,
        }
    )
    run = store.create(spec)
    manifest, payload = service.run(
        run,
        panel=panel,
        progress=lambda value, phase, detail: store.update(
            run["id"],
            value,
            phase,
            detail,
        ),
        cancelled=lambda: False,
    )
    path = store.write_artifact(run["id"], payload["artifact"])
    store.finish(
        run["id"],
        manifest=manifest,
        result=payload["summary"],
        artifact_path=str(path),
    )
    completed = store.get(run["id"], include_artifact=True)
    assert completed["status"] == "completed"
    assert completed["artifact"]["manifest"]["config_hash"] == spec.snapshot_hash
    assert store.events(run["id"])[-1]["type"] == "completed"

    strict_path = store.write_artifact(
        "strict-json",
        {"nan": float("nan"), "infinity": [float("inf")]},
    )
    assert json.loads(strict_path.read_text(encoding="utf-8")) == {
        "nan": None,
        "infinity": [None],
    }
    assert "NaN" not in strict_path.read_text(encoding="utf-8")
    assert "Infinity" not in strict_path.read_text(encoding="utf-8")

    second = store.create(spec.model_copy(update={"name": "对照"}))
    path2 = store.write_artifact(second["id"], payload["artifact"])
    store.finish(second["id"], manifest=manifest, result=payload["summary"], artifact_path=str(path2))
    assert len(service.compare([run["id"], second["id"]])["runs"]) == 2

    queued = store.create(spec.model_copy(update={"name": "待取消"}))
    cancelled = store.cancel(queued["id"])
    assert cancelled["status"] == "cancelled"
    assert cancelled["cancel_requested"] is True


def test_backtest_worker_reclaims_only_stale_lease_and_rejects_old_owner(tmp_path):
    store = BacktestStore(tmp_path / "runs.sqlite", tmp_path / "artifacts")
    spec = BacktestSpec.model_validate(
        {
            "name": "租约测试",
            "strategy": {"kind": "factor", "factor": "rank(close)", "top_n": 1},
            "universe": "demo",
            "start": "2023-01-02",
            "end": "2023-02-01",
            "benchmark": None,
        }
    )
    created = store.create(spec)
    assert store.claim_next("worker-a")["id"] == created["id"]
    assert store.heartbeat(created["id"], "worker-a")
    assert store.interrupt_stale(stale_after_seconds=30) == 0

    with store._conn() as connection:
        connection.execute(
            "UPDATE backtest_runs SET heartbeat_at='2000-01-01T00:00:00+00:00' WHERE id=?",
            (created["id"],),
        )
    assert store.interrupt_stale(stale_after_seconds=30) == 1
    assert store.claim_next("worker-b")["id"] == created["id"]
    assert not store.update(
        created["id"],
        50,
        "旧 worker",
        expected_worker="worker-a",
    )
    assert not store.finish(
        created["id"],
        error="stale result",
        expected_worker="worker-a",
    )
    assert store.get(created["id"])["worker"] == "worker-b"
    assert store.get(created["id"])["status"] == "running"


def test_hybrid_decision_snapshot_is_shared_by_backtest_and_paper(
    tmp_path,
    panel,
    monkeypatch,
):
    from quantmaster.decision import resolve_policy
    from quantmaster.lab.store import LabStore

    symbols = list(panel["close"].columns)
    policy = resolve_policy(
        "demo",
        1,
        "stable",
        symbols=symbols,
        store=LabStore(tmp_path / "lab.sqlite"),
    )
    decision = DecisionStrategySpec.model_validate(
        {
            "kind": "decision",
            "profile": "stable",
            "top_n": 3,
            "holding_days": 1,
            "cap_weight": 0.25,
            "policy_snapshot": policy,
        }
    )
    strategy = build_strategy(
        decision,
        symbols,
        "2023-01-02",
        "2023-08-01",
        universe="demo",
    )
    weights = strategy.target_weights(panel)
    assert not weights.dropna(how="all").empty
    assert weights.fillna(0).sum(axis=1).max() <= 0.65 + 1e-9

    backtest_store = BacktestStore(tmp_path / "runs.sqlite", tmp_path / "artifacts")
    run = BacktestService(backtest_store).enqueue(
        BacktestSpec.model_validate(
            {
                "name": "Hybrid 快照",
                "strategy": decision.model_dump(mode="json"),
                "universe": "demo",
                "start": "2023-01-02",
                "end": "2023-08-01",
                "benchmark": None,
                "initial_capital": 100_000,
            }
        )
    )
    assert run["config"]["strategy"]["policy_snapshot"]["policy_hash"] == policy["policy_hash"]

    paper = PaperService(PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts"))
    monkeypatch.setattr(
        paper,
        "_resolve_universe",
        lambda name, as_of: (symbols, {"quality": "sandbox"}),
    )
    account = paper.create_account(
        PaperAccountSpec.model_validate(
            {
                "name": "Hybrid 模拟",
                "strategy": decision.model_dump(mode="json"),
                "universe": "demo",
                "initial_capital": 100_000,
                "mode": "manual",
            }
        )
    )
    proposal = paper.propose(account["id"], panel=panel)
    assert proposal["status"] == "proposed"
    stored_policy = paper.store.account(account["id"])["strategy"]["policy_snapshot"]
    assert stored_policy["policy_hash"] == policy["policy_hash"]

    tampered = json.loads(json.dumps(policy))
    tampered["risk"]["max_exposure"] = 1.0
    with pytest.raises(ValueError, match="完整性"):
        pin_decision_strategy(
            decision.model_copy(update={"policy_snapshot": tampered}),
            "demo",
        )


def test_trading_api_requires_csrf_and_ui_exposes_workflow_contract(monkeypatch):
    client = TestClient(app)
    worker = get_backtest_worker()
    monkeypatch.setattr(worker, "start", lambda: None)
    payload = {
        "name": "接口回测",
        "strategy": {"kind": "swing", "top_n": 3, "holding_days": 3, "cap_weight": 0.25},
        "universe": "demo",
        "start": "2023-01-01",
        "end": "2023-12-31",
        "benchmark": None,
        "initial_capital": 100_000,
    }
    assert client.post("/api/v1/backtests", json=payload).status_code == 403
    token = _issue_csrf()
    client.cookies.set("qm_csrf", token)
    created = client.post(
        "/api/v1/backtests",
        json=payload,
        headers={"X-CSRF-Token": token},
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
    assert 'id="bt-factor-input"' in page
    assert 'role="combobox"' in page
    assert 'id="bt-factor-options" role="listbox"' in page
    assert 'popover="manual"' in page
    assert 'id="bt-factor-completion-hint"' not in page
    backtest_form = page.split('id="bt-form"', 1)[1].split("</form>", 1)[0]
    assert 'list="factor-list" value="mom_20d"' not in backtest_form
    assert "生成调仓提案" in page
    assert "确认并等待开盘" not in page  # 仅在真实提案渲染后出现
    assert "每日自动交易" in page
    assert "进入页面只读取历史快照，不会自动计算" in page
    app_script = client.get("/static/app.js").text
    assert "void loadDecisionHistory()" in app_script
    assert "document.getElementById('decision-form').requestSubmit()" not in app_script

    css = client.get("/static/trading.css").text
    checkbox_rule = css.split(
        '.trading-history-row input[type="checkbox"]',
        1,
    )[1].split("}", 1)[0]
    assert "min-width: 18px" in checkbox_rule
    assert "padding: 0" in checkbox_rule
    assert "justify-self: center" in checkbox_rule
    assert ".factor-completion-option.active" in css
    assert "position: fixed" in css.split(".factor-completion-menu", 1)[1].split("}", 1)[0]


def test_factor_reference_splitter_preserves_expression_commas():
    assert split_factor_references("ts_corr(rank(volume), rank(close), 20), mom_20d") == [
        "ts_corr(rank(volume), rank(close), 20)",
        "mom_20d",
    ]
    with pytest.raises(ValueError, match="括号"):
        split_factor_references("rank(close), ts_mean(volume, 20")


def test_trading_api_errors_never_expose_exception_details() -> None:
    from quantmaster.server import trading

    internal = r"C:\private\ledger.sqlite Bearer secret-value"
    cases = (
        (KeyError(internal), 404, "交易资源不存在"),
        (ValueError(internal), 400, "交易请求参数或状态无效"),
        (RuntimeError(internal), 500, "交易请求执行失败，请查看本机日志"),
    )

    for exception, status, detail in cases:
        public = trading._error(exception)
        assert public.status_code == status
        assert public.detail == detail
        assert "private" not in str(public.detail)
        assert "secret-value" not in str(public.detail)


def test_backtest_api_rejects_invalid_factor_before_queue(monkeypatch):
    client = TestClient(app)
    worker = get_backtest_worker()
    monkeypatch.setattr(worker, "start", lambda: None)
    token = _issue_csrf()
    client.cookies.set("qm_csrf", token)
    response = client.post(
        "/api/v1/backtests",
        json={
            "strategy": {"kind": "factor", "factor": "unknown_factor"},
            "universe": "demo",
            "start": "2023-01-01",
            "end": "2023-12-31",
            "benchmark": None,
        },
        headers={"X-CSRF-Token": token},
    )
    assert response.status_code == 422
    assert response.json()["detail"] == "回测参数无效，请检查策略、标的池和日期范围"


def test_trading_route_contracts_cover_exports_and_paper_lifecycle(monkeypatch):
    from quantmaster.server import trading

    monkeypatch.setattr(trading, "_require_csrf", lambda request: None)
    wake_count = [0]
    monkeypatch.setattr(
        trading,
        "get_paper_automation_worker",
        lambda: _RouteAutoWorker(wake_count),
    )
    strategy = _ROUTE_STRATEGY
    backtest_service = _RouteBacktestService()
    monkeypatch.setattr(trading, "_service", lambda: backtest_service)
    worker = _RouteBacktestWorker()
    monkeypatch.setattr(trading, "get_backtest_worker", lambda: worker)
    request = object()
    spec = BacktestSpec.model_validate(
        {
            "name": "route coverage",
            "strategy": strategy,
            "universe": "demo",
            "start": "2023-01-01",
            "end": "2023-12-31",
            "benchmark": None,
            "initial_capital": 100_000,
        }
    )

    assert trading.create_backtest(spec, request)["id"] == "new-run"
    assert worker.started == 1
    worker.service.fail = True
    with pytest.raises(trading.HTTPException) as invalid_run:
        trading.create_backtest(spec, request)
    assert invalid_run.value.status_code == 422
    assert trading.list_backtests(12)["items"][0]["limit"] == 12
    assert trading.get_backtest("completed")["status"] == "completed"
    with pytest.raises(trading.HTTPException) as missing_run:
        trading.get_backtest("missing")
    assert missing_run.value.status_code == 404
    assert trading.backtest_events("completed", after=3)["items"][0]["after"] == 3
    with pytest.raises(trading.HTTPException) as missing_events:
        trading.backtest_events("broken")
    assert missing_events.value.status_code == 404
    assert trading.cancel_backtest("completed", request)["status"] == "cancelled"
    with pytest.raises(trading.HTTPException) as bad_cancel:
        trading.cancel_backtest("broken", request)
    assert bad_cancel.value.status_code == 400
    assert trading.compare_backtests(
        trading.CompareRequest(run_ids=["one", "two"]),
        request,
    )["run_ids"] == ["one", "two"]
    with pytest.raises(trading.HTTPException):
        trading.compare_backtests(
            trading.CompareRequest(run_ids=["one", "broken"]),
            request,
        )

    json_export = trading.export_backtest("completed")
    assert json_export.media_type == "application/json; charset=utf-8"
    assert b'"summary"' in json_export.body
    csv_export = trading.export_backtest("completed", format="trades_csv")
    assert b"600000.SH" in csv_export.body
    with pytest.raises(trading.HTTPException) as missing_export:
        trading.export_backtest("missing")
    assert missing_export.value.status_code == 404
    with pytest.raises(trading.HTTPException) as queued_export:
        trading.export_backtest("queued")
    assert queued_export.value.status_code == 409

    paper_service = _RoutePaperService()
    monkeypatch.setattr(trading, "get_paper_service", lambda: paper_service)
    paper_spec = PaperAccountSpec.model_validate(
        {
            "name": "auto route",
            "strategy": strategy,
            "universe": "demo",
            "initial_capital": 100_000,
            "mode": "auto",
        }
    )
    assert trading.create_paper_account(paper_spec, request)["mode"] == "auto"
    assert wake_count[0] == 1
    promoted = trading.promote_backtest(
        "completed",
        trading.PromoteRequest(name="promoted", mode="auto"),
        request,
    )
    assert promoted["mode"] == "auto"
    with pytest.raises(trading.HTTPException) as promote_missing:
        trading.promote_backtest(
            "missing",
            trading.PromoteRequest(name="missing"),
            request,
        )
    assert promote_missing.value.status_code == 404
    with pytest.raises(trading.HTTPException) as promote_queued:
        trading.promote_backtest(
            "queued",
            trading.PromoteRequest(name="queued"),
            request,
        )
    assert promote_queued.value.status_code == 409

    assert trading.list_paper_accounts(True)["items"][0]["include_archived"] is True
    assert trading.get_paper_account("account")["id"] == "account"
    with pytest.raises(trading.HTTPException) as missing_account:
        trading.get_paper_account("missing")
    assert missing_account.value.status_code == 404
    with pytest.raises(trading.HTTPException) as empty_update:
        trading.update_paper_account("account", trading.PaperAccountUpdate(), request)
    assert empty_update.value.status_code == 422
    updated = trading.update_paper_account(
        "account",
        trading.PaperAccountUpdate(name="renamed", status="paused", mode="auto"),
        request,
    )
    assert updated["status"] == "paused"
    assert updated["name"] == "renamed"
    with pytest.raises(trading.HTTPException):
        trading.update_paper_account(
            "missing",
            trading.PaperAccountUpdate(status="paused"),
            request,
        )
    deleted = trading.delete_paper_account("account", request)
    assert deleted["deleted"] is True and deleted["recoverable"] is True
    assert deleted["account"]["status"] == "archived"
    with pytest.raises(trading.HTTPException) as missing_delete:
        trading.delete_paper_account("missing", request)
    assert missing_delete.value.status_code == 404
    assert (
        trading.clone_paper_account(
            "account",
            trading.CloneAccountRequest(name="clone", mode="auto"),
            request,
        )["id"]
        == "clone"
    )
    with pytest.raises(trading.HTTPException):
        trading.clone_paper_account(
            "missing",
            trading.CloneAccountRequest(name="clone"),
            request,
        )
    assert trading.propose_paper_cycle("account", request)["status"] == "proposed"
    with pytest.raises(trading.HTTPException):
        trading.propose_paper_cycle("missing", request)
    assert trading.confirm_paper_cycle("cycle", request)["status"] == "confirmed"
    with pytest.raises(trading.HTTPException):
        trading.confirm_paper_cycle("missing", request)
    assert trading.process_paper_account("account", request)["status"] == "processed"
    with pytest.raises(trading.HTTPException):
        trading.process_paper_account("missing", request)
    assert trading.paper_account_report("account")["warning"] == ""
    with pytest.raises(trading.HTTPException):
        trading.paper_account_report("missing")
    assert trading.paper_account_cycles("account", limit=9)["items"][0]["limit"] == 9
    with pytest.raises(trading.HTTPException) as missing_cycles:
        trading.paper_account_cycles("missing")
    assert missing_cycles.value.status_code == 404
