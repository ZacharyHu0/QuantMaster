"""回测工作台与多账户模拟盘的关键安全回归。"""

from __future__ import annotations

import json
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from quantmaster.backtest import BacktestConfig, run_backtest
from quantmaster.backtest.jobs import BacktestJobManager, get_backtest_job_manager
from quantmaster.backtest.paper_accounts import (
    PaperService,
    PaperStore,
)
from quantmaster.backtest.paper_automation import PaperAutomationWorker
from quantmaster.backtest.paper_market import CalendarEvidence, PaperMarket
from quantmaster.backtest.spec import (
    BacktestSpec,
    DecisionStrategySpec,
    PaperAccountSpec,
    build_strategy,
    content_hash,
    pin_decision_strategy,
    split_factor_references,
)
from quantmaster.backtest.workbench import (
    BacktestStore,
)
from quantmaster.data.base import BarDataEnvelope, BarDataQuality, DataEvidenceNotReady
from quantmaster.portfolio import TradeRecord
from quantmaster.runtime.jobs import UnifiedJobRuntime, UnifiedJobStore
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


_ROUTE_STRATEGY = {
    "kind": "factor", "factor": "mom_20d", "top_n": 3,
    "rebalance": "W", "weighting": "equal", "cap_weight": 0.35,
}
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
            "manifest": (
                {"formal_eligible": run_id == "completed"}
                if run_id != "unclassified" else {}
            ),
        }

    def events(self, run_id, after=0):
        if run_id == "broken":
            raise KeyError("回测不存在")
        return [{"run_id": run_id, "after": after}]

    def cancel(self, run_id):
        if run_id == "broken":
            raise ValueError("不能取消")
        return {"id": run_id, "status": "cancelled"}


class _RouteBacktestManager:
    def __init__(self):
        self.store = _RouteBacktestStore()
        self.queue = _RouteQueueService()
        self.started = 0

    def enqueue(self, spec):
        return self.queue.enqueue(spec)

    def start(self):
        self.started += 1

    def compare(self, run_ids):
        if "broken" in run_ids:
            raise ValueError("比较失败")
        return {"run_ids": run_ids}

    def get(self, run_id, include_artifact=False):
        value = self.store.get(run_id, include_artifact=include_artifact)
        if value is None:
            raise KeyError(run_id)
        return value

    def cancel(self, run_id):
        return self.store.cancel(run_id)


class _RouteQueueService:
    def __init__(self):
        self.fail = False

    def enqueue(self, spec):
        if self.fail:
            raise ValueError("未知字段")
        return {"id": "new-run", "status": "queued", "name": spec.name}


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

    monkeypatch.setattr(
        trading,
        "read_backtest_job",
        lambda run_id, include_artifact=False: {
            "id": run_id,
            "status": "completed",
            "artifact": {"metric": float("nan"), "values": [float("inf")]},
        },
    )

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


def validated_panel(panel):
    sessions = sorted({
        pd.Timestamp(value).date()
        for frame in panel.values()
        for value in frame.index
    })
    return {
        "panel": panel,
        "calendar_evidence": CalendarEvidence.build(
            PaperMarket.CN, sessions, source="test:verified-calendar",
        ),
        "observed_at": datetime.combine(
            sessions[-1], datetime.max.time(), ZoneInfo("Asia/Shanghai"),
        ),
    }


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

    waiting = service.process(account["id"], **validated_panel(signal_panel))
    assert waiting["status"] == "waiting_market_data"
    assert ledger.trades().empty


def test_paper_proposal_reads_local_formal_evidence_without_remote_refresh(tmp_path, monkeypatch):
    service, account = make_paper_service(tmp_path)
    panel = price_panel(pd.bdate_range("2024-01-01", periods=5))
    quality = BarDataQuality(
        "degraded", "2023-01-01", "2024-01-05",
        issues=("本地证据尚未通过正式验收",), partial=True,
    )
    calls = []

    def read_panel(symbols, start, end, **kwargs):
        calls.append((symbols, start, end, kwargs))
        return BarDataEnvelope(panel, quality)

    monkeypatch.setattr("quantmaster.data.read_panel", read_panel)
    monkeypatch.setattr(
        "quantmaster.backtest.paper_accounts.resolve_session_target",
        lambda: SessionExpectation("2024-01-05", "fixture-clock", True, "fixture"),
    )
    monkeypatch.setattr(
        "quantmaster.data.refresh_panel",
        lambda *_args, **_kwargs: pytest.fail("提案门禁不得触发远端刷新"),
    )

    with pytest.raises(DataEvidenceNotReady, match="本地证据"):
        service.propose(account["id"])

    assert calls[0][3]["purpose"] == "formal_research"
    assert service.store.cycles(account["id"]) == []
    assert service.store.ledger(account["id"]).trades().empty


def test_paper_executes_t_plus_one_open_and_never_overdraws(tmp_path):
    service, account = make_paper_service(tmp_path)
    dates = pd.bdate_range("2024-01-01", periods=6)
    proposal = service.propose(account["id"], panel=price_panel(dates[:-1]))
    service.store.confirm(proposal["id"])
    result = service.process(account["id"], **validated_panel(price_panel(dates)))
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
    paper_result = service.process(account["id"], **validated_panel(panel))
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
    blocked = service.process(account["id"], **validated_panel(blocked_panel))
    assert blocked["status"] == "blocked"
    assert blocked["blocked"][0]["reason"] == "limit_up"
    blocked_order = service.store.orders(cycle_id=proposal["id"])[0]
    assert blocked_order["status"] == "waiting_price"
    assert service.store.ledger(account["id"]).trades().empty

    tuesday = monday + pd.offsets.BDay(1)
    retry_panel = price_panel([*signal_dates, monday, tuesday], first=(9.0, 10.0))
    retry_panel["close"].loc[monday, "000001.SZ"] = 11.0
    retry_panel["open"].loc[tuesday, "000001.SZ"] = 10.8
    retried = service.process(account["id"], **validated_panel(retry_panel))
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


@pytest.mark.parametrize(
    "host_date",
    (date(2026, 8, 6), date(2026, 8, 15)),
    ids=("evidence-day", "host-date-rollover"),
)
def test_paper_strategy_change_preserves_history_and_schedules_transition(
    tmp_path,
    monkeypatch,
    panel,
    host_date,
):
    monkeypatch.setattr(
        "quantmaster.backtest.paper_accounts.market_date", lambda: host_date,
    )
    monkeypatch.setattr(
        "quantmaster.backtest.paper_accounts.resolve_session_target",
        lambda: SessionExpectation("2026-08-06", "fixture-clock", True, "fixture"),
    )
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
    recent_envelope = BarDataEnvelope(
        data=recent_panel,
        quality=BarDataQuality(
            status="verified",
            requested_start=str(recent_index[0].date()),
            requested_end=str(recent_index[-1].date()),
            observed_start=str(recent_index[0].date()),
            observed_end=str(recent_index[-1].date()),
            coverage_ratio=1.0,
            sources=("fixture",),
            timezone="Asia/Shanghai",
            adjustment="qfq",
            requested_symbols=("600000.SH", "600001.SH"),
            observed_symbols=("600000.SH", "600001.SH"),
        ),
        provenance=({"source": "fixture"},),
    )
    monkeypatch.setattr(
        "quantmaster.data.read_panel", lambda *_args, **_kwargs: recent_envelope,
    )
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


def test_paper_strategy_transition_error_is_redacted(tmp_path, monkeypatch):
    service, account = make_paper_service(tmp_path, "异常脱敏账户")
    strategy = account_spec("任意名称").strategy.model_copy(update={"top_n": 2})
    monkeypatch.setattr(
        service,
        "_resolve_universe",
        lambda _name, _as_of: (["600000.SH", "000001.SZ"], {"quality": "fixture"}),
    )
    monkeypatch.setattr(service, "_strategy_change_signal_date", lambda: "2026-08-07")

    def fail_proposal(_account_id):
        raise RuntimeError(r"C:\private\quotes.sqlite Bearer secret-value")

    monkeypatch.setattr(service, "propose", fail_proposal)
    updated = service.update_account(account["id"], strategy=strategy)

    message = updated["transition"]["message"]
    assert updated["transition"]["status"] == "waiting_data"
    assert message == "策略已保存，等待行情就绪后按 2026-08-07 信号日生成强制调仓"
    assert "private" not in message
    assert "secret-value" not in message


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
        lambda account_id, **_kwargs: processed.append(account_id) or {"status": "completed"},
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


def test_paper_order_state_machine_requires_reason_and_rejects_invalid_transition(tmp_path):
    service, account = make_paper_service(tmp_path)
    proposal = service.propose(
        account["id"], panel=price_panel(pd.bdate_range("2024-01-01", periods=5)),
    )
    order = service.store.confirm(proposal["id"])["orders"][0]

    with pytest.raises(ValueError, match="waiting_reason"):
        service.store.transition_order(order["id"], "waiting_market_data")
    waiting = service.store.transition_order(
        order["id"],
        "waiting_market_data",
        waiting_reason="missing_open:000001.SZ:2024-01-08",
        next_check_at="2024-01-08T01:30:00+00:00",
    )
    assert waiting["status"] == "waiting_market_data"
    assert waiting["waiting_reason"].startswith("missing_open")
    assert waiting["version"] == 1
    with pytest.raises(ValueError, match="不能从"):
        service.store.transition_order(order["id"], "proposed")


def test_paper_order_partial_fills_are_idempotent_and_arithmetically_consistent(tmp_path):
    service, account = make_paper_service(tmp_path)
    proposal = service.propose(
        account["id"], panel=price_panel(pd.bdate_range("2024-01-01", periods=5)),
    )
    confirmed = service.store.confirm(proposal["id"])
    order = confirmed["orders"][0]

    partial, written = service.store.record_fill(
        order["id"], fill_key="fill-1", quantity=400, price=10, fee=5,
        requested_qty=1_000, market_ref="bar:000001.SZ:2024-01-08:open",
        rule_version="paper-open-v2",
    )
    duplicate, duplicate_written = service.store.record_fill(
        order["id"], fill_key="fill-1", quantity=400, price=10, fee=5,
        requested_qty=1_000,
    )
    filled, second_written = service.store.record_fill(
        order["id"], fill_key="fill-2", quantity=600, price=12, fee=7,
        requested_qty=1_000,
    )

    assert written is True and duplicate_written is False and second_written is True
    assert partial["status"] == duplicate["status"] == "partially_filled"
    assert filled["status"] == "filled"
    assert filled["filled_qty"] == 1_000
    assert filled["remaining_qty"] == 0
    assert filled["avg_fill_price"] == pytest.approx(11.2)
    assert filled["fee"] == 12
    assert len(service.store.order_fills(order["id"])) == 2
    with pytest.raises(ValueError, match="成交证据冲突"):
        service.store.record_fill(
            order["id"], fill_key="fill-1", quantity=401, price=10, fee=5,
            requested_qty=1_000,
        )
    with pytest.raises(ValueError, match="终态订单"):
        service.store.record_fill(
            order["id"], fill_key="fill-3", quantity=1, price=12,
            requested_qty=1_001,
        )


def test_paper_auto_run_health_distinguishes_expired_lease_and_reclaim(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts")
    account = store.create_account(
        account_spec("租约诊断").model_copy(update={"mode": "auto"}),
        symbols=["600000.SH"],
    )
    account_id = account["id"]
    first = store.claim_auto_run("2026-02-13", account_id, "worker-one", now=100)
    assert first
    issues = store.scan_auto_run_health(now=200)
    assert issues[0]["diagnostic_code"] == "lease_expired"

    second = store.claim_auto_run("2026-02-13", account_id, "worker-two", now=200)
    assert second and second != first
    latest = store.latest_auto_run(account_id)
    assert latest["diagnostic_code"] == "lease_reclaimed"
    assert latest["reclaim_count"] == 1
    assert store.scan_auto_run_health(now=201) == []


def test_paper_auto_run_reclaims_expired_lease_at_retry_ceiling(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts")
    account = store.create_account(
        account_spec("租约重领边界").model_copy(update={"mode": "auto"}),
        symbols=["600000.SH"],
    )
    account_id = account["id"]
    first = store.claim_auto_run("2026-02-13", account_id, "dead-worker", now=100)
    assert first
    with store._conn() as conn:
        conn.execute(
            "UPDATE paper_auto_runs SET attempts=6,lease_expires=101,heartbeat_at=100 "
            "WHERE run_date='2026-02-13' AND account_id=?",
            (account_id,),
        )

    reclaimed = store.claim_auto_run(
        "2026-02-13", account_id, "replacement-worker", now=200,
    )

    assert reclaimed and reclaimed != first
    latest = store.latest_auto_run(account_id)
    assert latest["status"] == "running"
    assert latest["attempts"] == 6
    assert latest["diagnostic_code"] == "lease_reclaimed"
    assert latest["reclaim_count"] == 1


def test_stockdb_success_requeues_older_market_failures_and_only_resumes_data_pauses(
    tmp_path,
):
    store = PaperStore(tmp_path / "paper.sqlite", tmp_path / "accounts")
    data_paused = store.create_account(
        account_spec("行情暂停").model_copy(update={"mode": "auto"}),
        symbols=["600000.SH"],
    )
    manual_paused = store.create_account(
        account_spec("人工暂停").model_copy(update={"mode": "auto"}),
        symbols=["600000.SH"],
    )
    strategy_paused = store.create_account(
        account_spec("策略暂停").model_copy(update={"mode": "auto"}),
        symbols=["600000.SH"],
    )
    for account in (data_paused, manual_paused, strategy_paused):
        store.update_account(account["id"], status="paused")
    store.set_runtime_warning(data_paused["id"], "行情证据不可用：缺少目标日")
    store.set_runtime_warning(strategy_paused["id"], "策略快照需要人工迁移")

    token = store.claim_auto_run("2026-08-12", data_paused["id"], "worker", now=100)
    assert token
    assert store.fail_auto_run(
        "2026-08-12", data_paused["id"], "worker", token,
        "行情证据不可用", failure_code="market_data_unavailable", now=100,
    )
    future_token = store.claim_auto_run(
        "2026-08-14", manual_paused["id"], "worker", now=100,
    )
    assert future_token
    assert store.fail_auto_run(
        "2026-08-14", manual_paused["id"], "worker", future_token,
        "行情证据不可用", failure_code="market_data_unavailable", now=100,
    )

    assert store.requeue_market_data_failures("2026-08-13") == 1
    recovered = store.account(data_paused["id"])
    assert recovered["status"] == "active"
    assert recovered["runtime_warning"] == ""
    assert store.latest_auto_run(data_paused["id"])["attempts"] == 0
    assert store.account(manual_paused["id"])["status"] == "paused"
    assert store.latest_auto_run(manual_paused["id"])["attempts"] == 1
    assert store.account(strategy_paused["id"])["status"] == "paused"


def test_paper_process_recovers_ledger_fill_after_lease_loss(tmp_path, monkeypatch):
    service, account = make_paper_service(tmp_path)
    dates = pd.bdate_range("2024-01-01", periods=6)
    proposal = service.propose(
        account["id"], panel=price_panel(dates[:-1]),
    )
    service.store.confirm(proposal["id"])
    ledger = service.store.ledger(account["id"])
    original_add = ledger.add_trade
    ledger_written = False

    def add_then_lose_lease(trade, idempotency_key=None):
        nonlocal ledger_written
        written = original_add(trade, idempotency_key=idempotency_key)
        ledger_written = True
        return written

    monkeypatch.setattr(service.store, "ledger", lambda _account_id: ledger)
    monkeypatch.setattr(ledger, "add_trade", add_then_lose_lease)
    with pytest.raises(RuntimeError, match="lease_lost"):
        service.process(
            account["id"], **validated_panel(price_panel(dates)),
            lease_guard=lambda: not ledger_written,
        )
    assert len(ledger.trades()) == 1
    assert service.store.order_fills(proposal["orders"][0]["id"]) == []

    monkeypatch.setattr(ledger, "add_trade", original_add)
    recovered = service.process(
        account["id"], **validated_panel(price_panel(dates)), lease_guard=lambda: True,
    )

    assert recovered["status"] == "completed"
    assert len(ledger.trades()) == 1
    fills = service.store.order_fills(proposal["orders"][0]["id"])
    assert len(fills) == 1
    assert fills[0]["fill_key"].endswith(f":{dates[-1].date()}:open:v2")


def test_process_rejects_uncontracted_fixture_and_lease_loss_before_write(tmp_path):
    service, account = make_paper_service(tmp_path)
    dates = pd.bdate_range("2024-01-01", periods=6)
    proposal = service.propose(account["id"], panel=price_panel(dates[:-1]))
    service.store.confirm(proposal["id"])
    with pytest.raises(ValueError, match="calendar_evidence"):
        service.process(account["id"], panel=price_panel(dates))
    with pytest.raises(RuntimeError, match="lease_lost"):
        service.process(
            account["id"], **validated_panel(price_panel(dates)), lease_guard=lambda: False,
        )
    assert service.store.ledger(account["id"]).trades().empty


def test_order_health_scan_and_ledger_reconciliation_do_not_invent_fills(tmp_path):
    service, account = make_paper_service(tmp_path)
    proposal = service.propose(
        account["id"], panel=price_panel(pd.bdate_range("2024-01-01", periods=5)),
    )
    order = service.store.confirm(proposal["id"])["orders"][0]
    service.store.transition_order(
        order["id"], "waiting_market_data",
        waiting_reason="missing_open", next_check_at="2024-01-08T01:00:00+00:00",
    )
    assert service.store.scan_order_health(now="2024-01-09T00:00:00+00:00")[0][
        "diagnostic_code"
    ] == "next_check_due"

    service.store.ledger(account["id"]).add_trade(
        TradeRecord("2024-01-08", order["symbol"], "buy", 10, 100),
        idempotency_key=order["idempotency_key"],
    )
    result = service.store.reconcile_order_ledgers(account["id"])
    assert result == {"repaired": 0, "conflicts": 1}
    assert service.store.order_fills(order["id"]) == []
    refreshed = service.store.orders(account_id=account["id"])[0]
    assert refreshed["integrity_code"] == "ledger_trade_unproven"


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


def test_backtest_formal_eligibility_is_explicit_and_fail_closed() -> None:
    from quantmaster.backtest.application import _formal_eligibility

    production = BacktestSpec.model_validate({
        "strategy": {"kind": "factor", "factor": "mom_20d", "top_n": 1},
        "universe": "csi800",
        "start": "2023-01-02",
        "end": "2023-02-01",
        "benchmark": None,
        "research_tier": "production",
    })
    assert _formal_eligibility(
        production,
        resolved_tier="production",
        universe_quality="production",
        data_quality={"status": "complete"},
        research_manifest={"manifest_hash": "evidence"},
        benchmark_required=False,
        warnings=[],
    ) == (True, [])
    assert _formal_eligibility(
        production,
        resolved_tier="production",
        universe_quality="production",
        data_quality={
            "status": "complete",
            "benchmark_status": "verified",
            "benchmark_contract": {"formal_eligible": True},
        },
        research_manifest={"manifest_hash": "evidence"},
        benchmark_required=True,
        warnings=[],
    ) == (True, [])
    assert _formal_eligibility(
        production,
        resolved_tier="production",
        universe_quality="production",
        data_quality={"status": "complete"},
        research_manifest={},
        benchmark_required=False,
        warnings=[],
    ) == (False, ["missing_research_manifest"])
    assert _formal_eligibility(
        production.model_copy(update={"research_tier": "sandbox"}),
        resolved_tier="sandbox",
        universe_quality="sandbox",
        data_quality={"status": "partial"},
        research_manifest={},
        benchmark_required=False,
        warnings=[{"code": "partial_market_data"}],
    ) == (
        False,
        [
            "sandbox_research_tier", "universe_not_pit", "missing_research_manifest",
            "incomplete_market_evidence", "data_quality_not_complete",
        ],
    )

    lab = BacktestSpec.model_validate({
        **production.model_dump(mode="json"),
        "strategy": {
            "kind": "lab_version", "version_id": "approved-oof",
            "horizon": 3, "top_n": 20, "rebalance_days": 3, "cap_weight": 0.1,
        },
    })
    assert _formal_eligibility(
        lab,
        resolved_tier="production",
        universe_quality="production",
        data_quality={"status": "complete"},
        research_manifest={"manifest_hash": "evidence"},
        benchmark_required=False,
        warnings=[],
    ) == (False, ["lab_oof_result"])


def test_degraded_benchmark_envelope_blocks_formal_eligibility(
    panel, monkeypatch,
) -> None:
    from quantmaster.backtest.application import execute_backtest

    spec = BacktestSpec.model_validate({
        "strategy": {"kind": "factor", "factor": "mom_20d", "top_n": 1},
        "universe": "demo",
        "start": "2023-01-02",
        "end": "2023-08-01",
        "benchmark": "000300.SH",
    })
    quality = BarDataQuality(
        status="degraded",
        requested_start=spec.start,
        requested_end=spec.end or "",
        observed_start=spec.start,
        observed_end=spec.end or "",
        issues=("benchmark evidence degraded",),
    )
    benchmark = pd.DataFrame({"close": panel["close"].mean(axis=1)})
    monkeypatch.setattr(
        "quantmaster.data.refresh_history",
        lambda *_args, **_kwargs: BarDataEnvelope(benchmark, quality),
    )

    result = execute_backtest(spec, panel=panel)

    assert result["manifest"]["data_quality"]["benchmark_status"] == "degraded"
    assert result["manifest"]["data_quality"]["benchmark_contract"][
        "formal_eligible"
    ] is False
    assert "benchmark_evidence_not_verified" in result["manifest"][
        "eligibility_reasons"
    ]


def test_injected_benchmark_without_provenance_blocks_formal_eligibility(panel) -> None:
    from quantmaster.backtest.application import execute_backtest

    spec = BacktestSpec.model_validate({
        "strategy": {"kind": "factor", "factor": "mom_20d", "top_n": 1},
        "universe": "demo",
        "start": "2023-01-02",
        "end": "2023-08-01",
        "benchmark": None,
    })

    result = execute_backtest(
        spec,
        panel=panel,
        benchmark_close=panel["close"].mean(axis=1),
    )

    assert result["manifest"]["data_quality"]["benchmark_status"] == "not_requested"
    assert "benchmark_contract" not in result["manifest"]["data_quality"]
    assert "benchmark_evidence_not_verified" in result["manifest"][
        "eligibility_reasons"
    ]


def test_hybrid_decision_snapshot_is_shared_by_backtest_and_paper(
    tmp_path,
    panel,
    monkeypatch,
):
    from quantmaster.decision import hybrid_daily_selection, resolve_policy
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

    runtime = UnifiedJobRuntime(
        UnifiedJobStore(tmp_path / "jobs.sqlite"), dispatch=False,
    )
    backtest_jobs = BacktestJobManager(
        BacktestStore(tmp_path / "backtests.sqlite", tmp_path / "artifacts"),
        runtime=runtime,
    )
    run = backtest_jobs.enqueue(
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
    assert proposal["status"] in {"proposed", "completed"}
    signal_weights = strategy.signal_bundle(panel, force_latest=True).weights.iloc[-1]
    expected_target = {
        str(symbol): float(weight)
        for symbol, weight in signal_weights.fillna(0).items()
        if weight > 0
    }
    assert proposal["target_weights"] == pytest.approx(expected_target)
    daily = hybrid_daily_selection(
        panel,
        top_n=decision.top_n,
        horizon=decision.holding_days,
        profile=decision.profile,
        universe="demo",
        policy_snapshot=policy,
        cap_weight=decision.cap_weight,
    )
    daily_target = {
        pick["symbol"]: pick["target_weight"]
        for pick in daily["picks"] if pick["target_weight"] > 0
    }
    assert daily_target == pytest.approx(expected_target)
    stored_policy = paper.store.account(account["id"])["strategy"]["policy_snapshot"]
    assert stored_policy["policy_hash"] == policy["policy_hash"]

    tampered = json.loads(json.dumps(policy))
    tampered["risk"]["max_exposure"] = 1.0
    with pytest.raises(ValueError, match="完整性"):
        pin_decision_strategy(
            decision.model_copy(update={"policy_snapshot": tampered}),
            "demo",
        )


def test_old_hybrid_paper_account_requires_explicit_migration_without_mutating_history(
    tmp_path,
    panel,
):
    from quantmaster.decision import resolve_policy
    from quantmaster.lab.store import LabStore

    symbols = list(panel["close"].columns)
    current = resolve_policy(
        "demo", 1, "risk_adjusted", symbols=symbols,
        store=LabStore(tmp_path / "legacy-lab.sqlite"),
    )
    legacy = json.loads(json.dumps(current))
    legacy.pop("position_control", None)
    legacy["schema_version"] = 2
    legacy["engine_version"] = "hybrid-v2"
    legacy.pop("policy_hash", None)
    legacy.pop("model_version", None)
    legacy["policy_hash"] = content_hash(legacy)
    legacy["model_version"] = f"hybrid-v2:risk_adjusted:{legacy['policy_hash'][:12]}"
    decision = DecisionStrategySpec(
        profile="risk_adjusted",
        top_n=3,
        holding_days=1,
        cap_weight=0.25,
        policy_snapshot=legacy,
    )
    store = PaperStore(tmp_path / "legacy-paper.sqlite", tmp_path / "legacy-accounts")
    account = store.create_account(
        PaperAccountSpec(
            name="旧 Hybrid 模拟",
            strategy=decision,
            universe="demo",
            initial_capital=100_000,
            mode="manual",
        ),
        symbols=symbols,
        universe_meta={"quality": "sandbox"},
    )
    old_cycle, _ = store.create_cycle(
        account,
        "2024-01-02",
        {symbols[0]: 0.25},
        {symbols[0]: 10.0},
        [],
    )
    before_cashflows = len(store.ledger(account["id"]).cashflows())
    service = PaperService(store)

    with pytest.raises(ValueError, match="显式历史数据迁移"):
        service.propose(account["id"], panel=panel)

    stored = store.account(account["id"])
    assert stored["strategy"]["policy_snapshot"] == legacy
    assert store.cycle(old_cycle["id"])["status"] == "proposed"
    assert {order["status"] for order in store.orders(cycle_id=old_cycle["id"])} == {
        "proposed"
    }
    assert len(store.ledger(account["id"]).cashflows()) == before_cashflows


def test_trading_api_requires_csrf_and_ui_exposes_workflow_contract(monkeypatch):
    client = TestClient(app)
    manager = get_backtest_job_manager()
    monkeypatch.setattr(manager, "_owns_runtime", lambda: False)
    monkeypatch.setattr(manager, "start", lambda: None)
    payload = {
        "name": "接口回测",
        "strategy": {
            "kind": "factor", "factor": "mom_20d", "top_n": 3,
            "rebalance": "W", "weighting": "equal", "cap_weight": 0.35,
        },
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
    manager.cancel(created.json()["id"])
    legacy = {**payload, "strategy": {"kind": "swing", "top_n": 3, "holding_days": 3}}
    assert client.post(
        "/api/v1/backtests", json=legacy, headers={"X-CSRF-Token": token},
    ).status_code == 422

    page = client.get("/").text
    assert 'href="/static/trading.css"' not in page
    assert 'src="/static/trading.js"' not in page
    assert "回测工作台" in page
    assert "Hybrid v2 决策" in page
    assert 'option value="swing"' not in page
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
    backtest_layout = page.split('<div class="trading-layout">', 1)[1].split(
        "</section>\n\n<!-- ================= 挖掘", 1,
    )[0]
    assert backtest_layout.index('class="trading-config"') < backtest_layout.index(
        'class="trading-workspace"'
    ) < backtest_layout.index('class="trading-history-section"')
    assert 'class="trading-history-table" role="table"' in backtest_layout
    assert backtest_layout.count('role="columnheader"') == 6
    assert all(
        label in backtest_layout
        for label in ("选择", "实验名称", "候选 · 策略", "年化收益", "状态", "操作")
    )
    app_script = client.get("/static/app.js").text
    today_adapter = client.get("/static/workspaces/today.js").text
    assert "await context.shell.loadDecisionHistory()" in today_adapter
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

    trading_script = client.get("/static/trading.js").text
    assert "artifact.manifest?.formal_eligible === true" in trading_script
    assert "缺少正式资格证据" in trading_script
    assert "正式结果 · 可晋升" in trading_script
    assert "后台撮合任务" in trading_script
    assert "订单业务状态" in trading_script
    assert "核心数量冲突" in trading_script
    assert "const PAPER_PROPOSAL_TIMEOUT_MS = 60_000;" in trading_script
    assert "timeoutMs: PAPER_PROPOSAL_TIMEOUT_MS" in trading_script
    assert 'class="trading-history-row" role="row"' in trading_script
    assert trading_script.count('class="trading-history-cell') == 6
    assert trading_script.count('role="cell"') >= 6
    assert "waiting_market_data: '等待行情'" in trading_script
    assert "stalled: '已卡死'" in trading_script
    assert '.paper-task-panel[data-health="problem"]' in css
    assert ".trading-status.waiting_market_open" in css
    blocked_rule = css.split(".trading-status.blocked", 1)[1].split("}", 1)[0]
    assert "var(--bad)" not in blocked_rule


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


def test_trading_route_suppresses_internal_exception_chain(monkeypatch) -> None:
    from quantmaster.server import trading

    class FailingPaperService:
        @staticmethod
        def update_account(*_args, **_kwargs):
            raise RuntimeError(r"C:\private\ledger.sqlite Bearer secret-value")

    monkeypatch.setattr(trading, "get_paper_service", FailingPaperService)
    token = _issue_csrf()
    client = TestClient(app)
    client.cookies.set("qm_csrf", token)
    response = client.patch(
        "/api/v1/paper/accounts/account-1",
        json={"name": "安全名称"},
        headers={"X-CSRF-Token": token},
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "交易请求执行失败，请查看本机日志"
    assert "private" not in response.text
    assert "secret-value" not in response.text


def test_paper_proposal_returns_structured_evidence_problem(monkeypatch) -> None:
    from quantmaster.server import trading

    quality = BarDataQuality(
        "degraded", "2026-08-01", "2026-08-18",
        issues=("本地行情证据尚未通过正式验收",), partial=True,
    )

    class PendingEvidencePaperService:
        @staticmethod
        def propose(*_args, **_kwargs):
            raise DataEvidenceNotReady(quality)

    monkeypatch.setattr(trading, "get_paper_service", PendingEvidencePaperService)
    token = _issue_csrf()
    client = TestClient(app)
    client.cookies.set("qm_csrf", token)

    response = client.post(
        "/api/v1/paper/accounts/account-1/proposals",
        headers={"X-CSRF-Token": token},
    )

    assert response.status_code == 409
    payload = response.json()
    assert payload["problem"]["code"] == "evidence_not_ready"
    assert payload["data_quality"]["formal_eligible"] is False
    assert "本地行情证据" in payload["problem"]["message"]


def test_management_snapshot_and_migration_errors_are_redacted(monkeypatch) -> None:
    from quantmaster.server import management

    internal = r"C:\private\config.yaml Bearer secret-value"

    def fail_rollback(_snapshot_id):
        raise FileNotFoundError(internal)

    def fail_get(_task_id):
        raise KeyError(internal)

    monkeypatch.setattr(management.settings_manager, "rollback", fail_rollback)
    monkeypatch.setattr(management.migration_manager, "get", fail_get)
    token = _issue_csrf()
    client = TestClient(app)
    client.cookies.set("qm_csrf", token)

    snapshot = client.post(
        "/api/v1/settings/snapshots/missing/rollback",
        headers={"X-CSRF-Token": token},
    )
    migration = client.get("/api/v1/data/migrations/missing")

    assert snapshot.status_code == 404
    assert snapshot.json()["detail"] == "设置快照不存在"
    assert migration.status_code == 404
    assert migration.json()["detail"] == "数据迁移任务不存在"
    assert "private" not in snapshot.text + migration.text
    assert "secret-value" not in snapshot.text + migration.text


def test_backtest_api_rejects_invalid_factor_before_queue(monkeypatch):
    client = TestClient(app)
    manager = get_backtest_job_manager()
    monkeypatch.setattr(manager, "_owns_runtime", lambda: False)
    monkeypatch.setattr(manager, "start", lambda: None)
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
    backtest_manager = _RouteBacktestManager()
    monkeypatch.setattr(trading, "_manager", lambda: backtest_manager)
    monkeypatch.setattr(
        trading,
        "list_backtest_jobs",
        lambda limit: backtest_manager.store.list(limit),
    )
    monkeypatch.setattr(
        trading,
        "read_backtest_job",
        lambda run_id, include_artifact=False: backtest_manager.get(
            run_id, include_artifact=include_artifact,
        ),
    )
    monkeypatch.setattr(
        trading,
        "backtest_job_events",
        lambda run_id, after=0: backtest_manager.store.events(run_id, after=after),
    )
    monkeypatch.setattr(trading, "_require_backtest_snapshot", lambda: None)
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
    assert backtest_manager.started == 1
    backtest_manager.queue.fail = True
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
    monkeypatch.setattr(trading, "get_paper_service", lambda **_kwargs: paper_service)
    monkeypatch.setattr(trading, "_read_paper_service", lambda: paper_service)
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
    for blocked_id in ("sandbox", "unclassified"):
        with pytest.raises(trading.HTTPException) as blocked:
            trading.promote_backtest(
                blocked_id,
                trading.PromoteRequest(name="blocked"),
                request,
            )
        assert blocked.value.status_code == 409
        assert "正式" in blocked.value.detail
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
