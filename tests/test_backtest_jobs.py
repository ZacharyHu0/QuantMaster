from __future__ import annotations

import threading

import pytest

from quantmaster.backtest.jobs import BacktestJobManager
from quantmaster.backtest.spec import BacktestSpec
from quantmaster.backtest.workbench import BacktestService, BacktestStore
from quantmaster.runtime.jobs import (
    JobContext,
    JobLeaseLost,
    UnifiedJobRuntime,
    UnifiedJobStore,
)
from quantmaster.runtime.problems import OperationProblem, make_problem


def _spec(name: str = "统一回测") -> BacktestSpec:
    return BacktestSpec.model_validate({
        "name": name,
        "strategy": {"kind": "factor", "factor": "mom_20d", "top_n": 3},
        "universe": "demo",
        "start": "2023-01-01",
        "end": "2023-12-31",
        "benchmark": None,
        "initial_capital": 100_000,
    })


def _manager(tmp_path, service: BacktestService | None = None):
    runtime = UnifiedJobRuntime(
        UnifiedJobStore(tmp_path / "jobs.sqlite"),
        max_workers=1,
        dispatch=False,
    )
    manager = BacktestJobManager(
        BacktestStore(tmp_path / "backtests.sqlite", tmp_path / "backtests"),
        service or BacktestService(),
        runtime,
    )
    assert runtime._handlers["backtest.run"].process_entrypoint == (
        "quantmaster.backtest.jobs:run_backtest_job"
    )
    return manager, runtime


def _claim_context(runtime: UnifiedJobRuntime, job_id: str) -> JobContext:
    assert runtime.store.claim(job_id, runtime.identity.value, lease_seconds=30)
    job = runtime.store.get(job_id)
    lease_alive = threading.Event()
    lease_alive.set()
    return JobContext(runtime, job, lease_alive, runtime.generation)


def _finish(runtime: UnifiedJobRuntime, context: JobContext, outcome) -> None:
    active = runtime.store.get(context.job_id)
    assert runtime.store.finish(
        context.job_id,
        runtime.identity.value,
        outcome,
        lease_token=str(active["lease_token"]),
    )


def test_backtest_manager_commits_domain_result_and_reuses_it_on_same_job_retry(
    tmp_path,
    monkeypatch,
) -> None:
    service = BacktestService()
    calls: list[str] = []

    def execute(job_id, name, spec, **_kwargs):
        calls.append(job_id)
        manifest = {
            "config_hash": spec.snapshot_hash,
            "strategy_snapshot": spec.strategy.model_dump(mode="json"),
            "data_quality": {"status": "complete"},
            "warnings": [],
            "formal_eligible": False,
        }
        artifact = {
            "manifest": manifest,
            "metrics": {"annual_return": 0.1},
            "nav": [["2023-01-03", 1.0]],
            "trades": [],
        }
        return manifest, {"summary": {"metrics": artifact["metrics"]}, "artifact": artifact}

    monkeypatch.setattr(service, "run", execute)
    manager, runtime = _manager(tmp_path, service)
    queued = manager.enqueue(_spec())
    first = _claim_context(runtime, queued["id"])
    first_outcome = manager._handle(first, runtime.store.get(queued["id"])["spec"])

    # Simulate a supervisor loss after the immutable domain commit but before
    # its terminal lifecycle update. The next attempt must not recompute.
    assert runtime.store.interrupt_owned(runtime.identity.value) == [queued["id"]]
    retried = runtime.retry(queued["id"])
    assert retried["id"] == queued["id"] and retried["attempt"] == 2
    second = _claim_context(runtime, queued["id"])
    second_outcome = manager._handle(second, runtime.store.get(queued["id"])["spec"])
    _finish(runtime, second, second_outcome)

    completed = manager.get(queued["id"], include_artifact=True)
    assert completed["status"] == "completed"
    assert completed["artifact"]["metrics"]["annual_return"] == 0.1
    assert calls == [queued["id"]]
    assert manager._domain_store().results(queued["id"])[0]["attempt"] == 1
    assert any(
        event["type"] == "backtest_result_reused"
        for event in runtime.store.events(queued["id"])
    )
    assert first_outcome.status == second_outcome.status == "completed"


def test_backtest_manager_preserves_confirmation_problem_as_domain_outcome(
    tmp_path,
    monkeypatch,
) -> None:
    service = BacktestService()
    problem = make_problem(
        "partial_market_data",
        severity="warning",
        source="策略回测",
        title="回测数据不完整",
        message="一只候选缺少行情。",
        action="补齐数据，或确认后仅用可用数据继续。",
        blocking=True,
        can_continue=True,
    )

    def blocked(*_args, **_kwargs):
        raise OperationProblem(409, problem, data_quality={"status": "needs_confirmation"})

    monkeypatch.setattr(service, "run", blocked)
    manager, runtime = _manager(tmp_path, service)
    queued = manager.enqueue(_spec())
    context = _claim_context(runtime, queued["id"])
    outcome = manager._handle(context, runtime.store.get(queued["id"])["spec"])
    _finish(runtime, context, outcome)

    failed = manager.get(queued["id"])
    assert failed["status"] == "failed"
    assert failed["outcome"] == "needs_confirmation"
    assert failed["result"]["problem"]["can_continue"] is True
    assert failed["result"]["data_quality"]["status"] == "needs_confirmation"
    assert failed["diagnostic"]["code"] == "partial_market_data"


def test_backtest_task_uses_unified_lease_fencing_and_same_job_retry(tmp_path) -> None:
    manager, runtime = _manager(tmp_path)
    queued = manager.enqueue(_spec())
    assert runtime.store.claim(queued["id"], "worker-old", lease_seconds=5)
    old = runtime.store.get(queued["id"])
    with runtime.store._conn() as connection:
        connection.execute(
            "UPDATE runtime_jobs SET lease_expires=0 WHERE id=?",
            (queued["id"],),
        )
    assert runtime.store.recover_expired() == [queued["id"]]
    assert runtime.store.claim(queued["id"], "worker-new", lease_seconds=30)
    fresh = runtime.store.get(queued["id"])

    with pytest.raises(JobLeaseLost):
        runtime.store.progress(
            queued["id"], "worker-old", str(old["lease_token"]), 50, "旧 worker",
        )
    assert fresh["lease_token"] != old["lease_token"]
    assert runtime.store.interrupt_owned("worker-new") == [queued["id"]]
    assert manager.retry(queued["id"])["id"] == queued["id"]


def test_backtest_store_keeps_attempt_results_immutable_and_strict(tmp_path) -> None:
    store = BacktestStore(tmp_path / "backtests.sqlite", tmp_path / "backtests")
    spec = {"name": "strict", "config": _spec().model_dump(mode="json"), "config_hash": "h"}
    first = store.save_result(
        "job-one",
        1,
        name="strict",
        spec=spec,
        outcome="completed",
        artifact={"nan": float("nan"), "infinity": [float("inf")]},
    )
    assert first["artifact"] == {"nan": None, "infinity": [None]}
    assert store.save_result(
        "job-one",
        1,
        name="strict",
        spec=spec,
        outcome="completed",
        artifact={"nan": float("nan"), "infinity": [float("inf")]},
    )["content_hash"] == first["content_hash"]
    with pytest.raises(ValueError, match="不同领域结果"):
        store.save_result(
            "job-one",
            1,
            name="strict",
            spec=spec,
            outcome="failed",
            diagnostic={"code": "changed"},
        )
