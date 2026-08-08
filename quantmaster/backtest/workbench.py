"""可恢复的异步回测工作台。"""

from __future__ import annotations

import builtins
import json
import logging
import os
import socket
import sqlite3
import tempfile
import threading
import uuid
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from quantmaster import __version__
from quantmaster.backtest.spec import BacktestSpec, canonical_json
from quantmaster.config import get_config
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.sqlite import connect_sqlite

logger = logging.getLogger(__name__)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class BacktestStore:
    """SQLite 保存任务元数据，JSON 文件保存可导出的完整结果。"""

    def __init__(self, path: str | Path | None = None, artifact_root: str | Path | None = None):
        self.path = Path(path) if path else get_config().data_root / "backtests.sqlite"
        self.artifact_root = (
            Path(artifact_root) if artifact_root else get_config().data_root / "backtests"
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        self._migrate()

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(self.path, row_factory=True)

    def _migrate(self) -> None:
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS backtest_runs (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL,
                    config_json TEXT NOT NULL, config_hash TEXT NOT NULL,
                    manifest_json TEXT NOT NULL DEFAULT '{}', result_json TEXT NOT NULL DEFAULT '{}',
                    artifact_path TEXT NOT NULL DEFAULT '', progress INTEGER NOT NULL DEFAULT 0,
                    phase TEXT NOT NULL DEFAULT '', detail TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '', cancel_requested INTEGER NOT NULL DEFAULT 0,
                    worker TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
                    started_at TEXT NOT NULL DEFAULT '', heartbeat_at TEXT NOT NULL DEFAULT '',
                    finished_at TEXT NOT NULL DEFAULT '');
                CREATE TABLE IF NOT EXISTS backtest_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL,
                    event_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES backtest_runs(id));
                CREATE INDEX IF NOT EXISTS idx_backtest_status
                    ON backtest_runs(status,created_at);
                CREATE INDEX IF NOT EXISTS idx_backtest_events
                    ON backtest_events(run_id,seq);
            """)
            now = utc_now()
            active = conn.execute(
                "SELECT id,config_json,progress FROM backtest_runs "
                "WHERE status IN ('queued','running','interrupted')"
            ).fetchall()
            for row in active:
                try:
                    config = json.loads(row["config_json"] or "{}")
                except (json.JSONDecodeError, TypeError):
                    continue
                if config.get("strategy", {}).get("kind") != "swing":
                    continue
                detail = "旧 Swing 执行器已移除；任务保留为只读历史记录。"
                conn.execute(
                    "UPDATE backtest_runs SET status='cancelled',cancel_requested=1,worker='',"
                    "phase='旧策略已归档',detail=?,heartbeat_at=?,finished_at=? WHERE id=?",
                    (detail, now, now, row["id"]),
                )
                conn.execute(
                    "INSERT INTO backtest_events(run_id,event_json,created_at) VALUES (?,?,?)",
                    (
                        row["id"],
                        canonical_json({
                            "type": "cancelled",
                            "progress": int(row["progress"] or 0),
                            "phase": "旧策略已归档",
                            "detail": detail,
                        }),
                        now,
                    ),
                )

    @staticmethod
    def _decode(row: sqlite3.Row | None) -> dict | None:
        if row is None:
            return None
        value = dict(row)
        for field in ("config_json", "manifest_json", "result_json"):
            value[field.removesuffix("_json")] = json.loads(value.pop(field) or "{}")
        value["cancel_requested"] = bool(value["cancel_requested"])
        value["legacy_read_only"] = (
            value.get("config", {}).get("strategy", {}).get("kind") == "swing"
        )
        return value

    def create(self, spec: BacktestSpec) -> dict:
        run_id, now = uuid.uuid4().hex, utc_now()
        config = spec.model_dump(mode="json")
        name = spec.name.strip() or f"{spec.strategy.kind} · {spec.universe} · {spec.start}"
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO backtest_runs "
                "(id,name,status,config_json,config_hash,created_at) VALUES (?,?,?,?,?,?)",
                (run_id, name, "queued", canonical_json(config), spec.snapshot_hash, now),
            )
        self.append_event(run_id, {"type": "queued", "progress": 0, "phase": "等待执行"})
        return self.get(run_id) or {}

    def append_event(self, run_id: str, event: dict) -> int:
        with self._conn() as conn:
            cursor = conn.execute(
                "INSERT INTO backtest_events(run_id,event_json,created_at) VALUES (?,?,?)",
                (run_id, canonical_json(event), utc_now()),
            )
        return int(cursor.lastrowid or 0)

    def claim_next(self, worker: str) -> dict | None:
        now = utc_now()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT id FROM backtest_runs WHERE status IN ('queued','interrupted') "
                "AND cancel_requested=0 ORDER BY created_at LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            changed = conn.execute(
                "UPDATE backtest_runs SET status='running',worker=?,started_at=CASE "
                "WHEN started_at='' THEN ? ELSE started_at END,heartbeat_at=? "
                "WHERE id=? AND status IN ('queued','interrupted')",
                (worker, now, now, row["id"]),
            ).rowcount
            if not changed:
                return None
        return self.get(row["id"])

    def update(
        self,
        run_id: str,
        progress: int,
        phase: str,
        detail: str = "",
        *,
        expected_worker: str = "",
    ) -> bool:
        value = max(0, min(100, int(progress)))
        with self._conn() as conn:
            where = "id=? AND status='running'"
            params: list[Any] = [value, phase, detail[:500], utc_now(), run_id]
            if expected_worker:
                where += " AND worker=?"
                params.append(expected_worker)
            changed = conn.execute(
                "UPDATE backtest_runs SET progress=?,phase=?,detail=?,heartbeat_at=? "
                f"WHERE {where}", params,
            ).rowcount
        if not changed:
            return False
        self.append_event(run_id, {
            "type": "progress", "progress": value, "phase": phase, "detail": detail[:300],
        })
        return True

    def heartbeat(self, run_id: str, worker: str) -> bool:
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE backtest_runs SET heartbeat_at=? "
                "WHERE id=? AND worker=? AND status='running'",
                (utc_now(), run_id, worker),
            ).rowcount
        return bool(changed)

    def finish(
        self,
        run_id: str,
        *,
        manifest: dict | None = None,
        result: dict | None = None,
        artifact_path: str = "",
        error: str = "",
        expected_worker: str = "",
    ) -> bool:
        current = self.get(run_id)
        if current is None:
            raise KeyError("回测不存在")
        cancelled = current["cancel_requested"]
        status = "cancelled" if cancelled else "failed" if error else "completed"
        progress = current["progress"] if cancelled or error else 100
        now = utc_now()
        with self._conn() as conn:
            where = "id=?"
            params: list[Any] = [
                status, progress, canonical_json(manifest or {}), canonical_json(result or {}),
                artifact_path, error[:1500], now, now, run_id,
            ]
            if expected_worker:
                where += " AND worker=? AND status='running'"
                params.append(expected_worker)
            changed = conn.execute(
                "UPDATE backtest_runs SET status=?,progress=?,manifest_json=?,result_json=?,"
                f"artifact_path=?,error=?,finished_at=?,heartbeat_at=? WHERE {where}", params,
            ).rowcount
        if not changed:
            return False
        self.append_event(run_id, {
            "type": status,
            "progress": progress,
            "phase": "已取消" if cancelled else "执行失败" if error else "执行完成",
            "detail": error[:300],
        })
        return True

    def needs_confirmation(
        self,
        run_id: str,
        *,
        problem: dict,
        data_quality: dict | None = None,
        expected_worker: str = "",
    ) -> bool:
        now = utc_now()
        with self._conn() as conn:
            where = "id=? AND status='running'"
            params: list[Any] = [
                canonical_json({
                    "problem": problem,
                    "data_quality": data_quality or {},
                }),
                str(problem.get("message") or "需要用户确认")[:500],
                now,
                now,
                run_id,
            ]
            if expected_worker:
                where += " AND worker=?"
                params.append(expected_worker)
            changed = conn.execute(
                "UPDATE backtest_runs SET status='needs_confirmation',result_json=?,"
                "phase='等待用户确认',detail=?,heartbeat_at=?,finished_at=?,worker='' "
                f"WHERE {where}",
                params,
            ).rowcount
        if changed:
            self.append_event(run_id, {
                "type": "needs_confirmation",
                "progress": 0,
                "phase": "等待用户确认",
                "problem": problem,
            })
        return bool(changed)

    def cancel(self, run_id: str) -> dict:
        with self._conn() as conn:
            row = conn.execute("SELECT status FROM backtest_runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise KeyError("回测不存在")
            if row["status"] in {"completed", "failed", "cancelled", "needs_confirmation"}:
                return self.get(run_id) or {}
            status = "cancelled" if row["status"] in {"queued", "interrupted"} else row["status"]
            conn.execute(
                "UPDATE backtest_runs SET cancel_requested=1,status=?,finished_at=CASE "
                "WHEN ?='cancelled' THEN ? ELSE finished_at END WHERE id=?",
                (status, status, utc_now(), run_id),
            )
        self.append_event(run_id, {"type": "cancel_requested", "phase": "正在安全停止"})
        return self.get(run_id) or {}

    def is_cancelled(self, run_id: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT cancel_requested FROM backtest_runs WHERE id=?", (run_id,),
            ).fetchone()
        return bool(row and row[0])

    def interrupt_stale(self, worker: str = "", stale_after_seconds: int = 30) -> int:
        with self._conn() as conn:
            if worker:
                cursor = conn.execute(
                    "UPDATE backtest_runs SET status='interrupted',worker='' "
                    "WHERE status='running' AND worker=?", (worker,),
                )
            else:
                cursor = conn.execute(
                    "UPDATE backtest_runs SET status='interrupted',worker='' "
                    "WHERE status='running' AND (heartbeat_at='' OR "
                    "julianday(heartbeat_at)<julianday('now',?))",
                    (f"-{max(1, int(stale_after_seconds))} seconds",),
                )
        return cursor.rowcount

    def interrupt_running(self, worker: str = "") -> int:
        """Backward-compatible name; never interrupts another live worker."""
        return self.interrupt_stale(worker)

    def get(self, run_id: str, *, include_artifact: bool = False) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM backtest_runs WHERE id=?", (run_id,)).fetchone()
        value = self._decode(row)
        if value and include_artifact and value["artifact_path"]:
            path = Path(value["artifact_path"])
            if path.is_file():
                value["artifact"] = json.loads(path.read_text(encoding="utf-8"))
        return value

    def list(self, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM backtest_runs ORDER BY created_at DESC LIMIT ?",
                (max(1, min(limit, 200)),),
            ).fetchall()
        return [self._decode(row) or {} for row in rows]

    def events(
        self, run_id: str, after: int = 0, limit: int = 500,
    ) -> builtins.list[dict]:
        if self.get(run_id) is None:
            raise KeyError("回测不存在")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT seq,event_json,created_at FROM backtest_events "
                "WHERE run_id=? AND seq>? ORDER BY seq LIMIT ?",
                (run_id, max(0, after), max(1, min(limit, 2000))),
            ).fetchall()
        return [
            {"seq": row["seq"], "created_at": row["created_at"], **json.loads(row["event_json"])}
            for row in rows
        ]

    def write_artifact(self, run_id: str, payload: dict) -> Path:
        directory = self.artifact_root / run_id
        directory.mkdir(parents=True, exist_ok=True)
        destination = directory / "result.json"
        descriptor, temp_name = tempfile.mkstemp(prefix=".result.", suffix=".tmp", dir=directory)
        temp = Path(temp_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(strict_json_dumps(payload))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, destination)
        finally:
            temp.unlink(missing_ok=True)
        return destination


class BacktestService:
    def __init__(self, store: BacktestStore | None = None):
        self.store = store or BacktestStore()

    def enqueue(self, spec: BacktestSpec) -> dict:
        from quantmaster.backtest.spec import pin_decision_strategy, preflight_strategy

        preflight_strategy(spec)
        strategy = pin_decision_strategy(spec.strategy, spec.universe)
        if strategy is not spec.strategy:
            spec = spec.model_copy(update={"strategy": strategy})
        return self.store.create(spec)

    @staticmethod
    def _points(series: pd.Series | None) -> list[list[Any]]:
        if series is None:
            return []
        return [
            [pd.Timestamp(index).strftime("%Y-%m-%d"), round(float(value), 6)]
            for index, value in series.dropna().items()
        ]

    def run(
        self,
        run: dict,
        *,
        progress: Callable[[int, str, str], None],
        cancelled: Callable[[], bool],
        panel: dict[str, pd.DataFrame] | None = None,
        membership: pd.DataFrame | None = None,
        benchmark_close: pd.Series | None = None,
    ) -> tuple[dict, dict]:
        from quantmaster.backtest.engine import BacktestConfig, run_backtest
        from quantmaster.backtest.quality import (
            assess_panel_quality,
            assess_signal_quality,
        )
        from quantmaster.backtest.report import full_report
        from quantmaster.backtest.spec import build_strategy
        from quantmaster.data import load_history, load_panel
        from quantmaster.data.universe import load_universe
        from quantmaster.lab.dataset import create_snapshot, load_csi800_membership
        from quantmaster.runtime.problems import OperationProblem, make_problem

        spec = BacktestSpec.model_validate(run["config"])
        end = spec.end or str(pd.Timestamp.now().date())
        warnings: list[dict[str, str]] = []
        resolved_tier = (
            "production" if spec.research_tier == "auto" and spec.universe.lower() == "csi800"
            else "sandbox" if spec.research_tier == "auto" else spec.research_tier
        )
        research_manifest: dict[str, Any] = {}

        def checkpoint(value: int, phase: str, detail: str = "") -> None:
            if cancelled():
                raise InterruptedError("用户取消回测")
            progress(value, phase, detail)

        checkpoint(5, "准备候选", "解析固定候选或历史成分快照")
        if spec.universe.lower() == "csi800":
            if membership is None:
                membership = load_csi800_membership(spec.start, end)
            symbols = sorted(symbol for symbol in membership if membership[symbol].any())
            universe_quality = "production"
        else:
            symbols = load_universe(spec.universe)
            universe_quality = "sandbox"
            warnings.append({
                "code": "fixed_universe",
                "level": "warning",
                "message": "固定候选可能包含幸存者偏差；生产研究建议使用 csi800 历史成分。",
            })
        if not symbols:
            raise ValueError("候选中没有可回测标的")

        checkpoint(18, "加载行情", f"读取 {len(symbols)} 只标的的历史行情")
        provided_panel = panel is not None
        if panel is None:
            if resolved_tier == "production":
                from quantmaster.data.research import load_research_bundle

                def research_progress(
                    done: int, total: int, symbol: str, success: bool, detail: str = "",
                ) -> None:
                    request_estimate = max(0, total - done) * 4
                    suffix = f" · {detail}" if detail else ""
                    if detail == "下载 PIT 缺口":
                        minutes = (request_estimate + 119) // 120
                        suffix += f" · 余约 {request_estimate} 请求 / {minutes} 分钟（120/分）"
                    checkpoint(
                        18 + int(18 * done / max(1, total)), "加载原始成交/PIT约束",
                        f"{done}/{total} · {symbol}{'' if success else ' 失败'}{suffix}",
                    )

                research_bundle = load_research_bundle(
                    symbols, spec.start, end, membership=membership, progress=research_progress,
                )
                panel = research_bundle.backtest_panel()
                research_manifest = {
                    **research_bundle.manifest,
                    "manifest_hash": research_bundle.manifest_hash,
                }
            else:
                panel = load_panel(symbols, spec.start, end)
                warnings.append({
                    "code": "sandbox_execution_approximation", "level": "warning",
                    "message": "Sandbox 使用旧前复权缓存与代码板涨跌停近似，不能作为生产晋升证据。",
                })
        elif resolved_tier == "production":
            required = {
                "execution_open", "execution_close", "adj_factor",
                "up_limit", "down_limit", "suspended",
            }
            missing = sorted(required - set(panel))
            if missing:
                raise ValueError("production 回测缺少真实成交字段：" + "、".join(missing))
        quality_symbols = list(panel.get("close", pd.DataFrame()).columns) if provided_panel else symbols
        data_quality, panel_warnings = assess_panel_quality(
            panel,
            quality_symbols,
            minimum_symbols=spec.strategy.top_n,
            allow_partial=spec.allow_partial,
        )
        warnings.extend(panel_warnings)
        close = panel["close"]
        symbols = list(close.columns)

        checkpoint(38, "计算信号", "按策略快照生成目标权重")
        strategy = build_strategy(
            spec.strategy, symbols, spec.start, end, universe=spec.universe,
        )
        signal_bundle = strategy.signal_bundle(panel, eligibility_mask=membership)
        if (
            resolved_tier == "production"
            and signal_bundle.metadata.get("prediction_source") == "rolling_oof"
            and signal_bundle.metadata.get("research_quality") != "production"
        ):
            raise ValueError("production 回测拒绝使用 Sandbox 训练的学习模型")
        weights = signal_bundle.weights
        if (
            membership is not None
            and signal_bundle.metadata.get("position_control")
            != "hybrid-position-control-v1"
        ):
            active = weights.notna().any(axis=1)
            totals = weights.loc[active].sum(axis=1).replace(0, float("nan"))
            weights.loc[active] = weights.loc[active].div(totals, axis=0).fillna(0.0)
        warnings.extend(assess_signal_quality(
            panel, weights, data_quality, allow_partial=spec.allow_partial,
            intentional_flat=signal_bundle.intentional_flat,
        ))

        if benchmark_close is None and spec.benchmark:
            try:
                benchmark_close = load_history(spec.benchmark, spec.start, end)["close"]
                if benchmark_close.empty:
                    raise ValueError("基准没有可用收盘价")
                data_quality["benchmark_status"] = "complete"
            except Exception as exc:
                data_quality["benchmark_status"] = "unavailable"
                data_quality["status"] = "partial"
                warnings.append({
                    "code": "benchmark_unavailable", "level": "warning",
                    "message": f"基准 {spec.benchmark} 不可用，超额指标未计算：{exc}",
                })
        elif not spec.benchmark:
            data_quality["benchmark_status"] = "not_requested"

        checkpoint(60, "模拟成交", "按 T 日收盘信号、T+1 日开盘与 A 股费用规则撮合")
        result = run_backtest(
            panel,
            weights,
            BacktestConfig(
                initial_capital=spec.initial_capital,
                stop_loss=spec.stop_loss,
                take_profit=spec.take_profit,
                research_tier=resolved_tier,
            ),
            benchmark_close=benchmark_close,
        )
        if not result.trades and not (
            signal_bundle.intentional_flat is not None
            and signal_bundle.intentional_flat.fillna(False).any()
            and not weights.gt(0).to_numpy().any()
        ):
            data_quality["status"] = "blocked"
            raise OperationProblem(
                422,
                make_problem(
                    "no_valid_trades",
                    source="策略回测",
                    title="回测没有产生有效成交",
                    message="所有信号均未形成可验证成交，不能把空净值曲线当作有效回测结果。",
                    action="检查成交日价格、涨跌停限制、资金规模和策略信号后重试。",
                    blocking=True,
                    problem_id="backtest:no-valid-trades",
                ),
                data_quality=data_quality,
            )
        checkpoint(82, "生成报告", "汇总净值、回撤、成交与分期绩效")
        report = full_report(result)
        drawdown = result.nav / result.nav.cummax() - 1.0
        exposure = result.positions.sum(axis=1) / (result.nav * spec.initial_capital)
        snapshot = create_snapshot(
            spec.universe, spec.start, end, panel=panel, membership=membership,
        ).to_dict()
        trade_config = asdict(get_config().trade)
        manifest = {
            "app_version": __version__,
            "config_hash": run["config_hash"],
            "strategy_name": strategy.name,
            "strategy_snapshot": spec.strategy.model_dump(mode="json"),
            "universe": spec.universe,
            "universe_quality": universe_quality,
            "research_tier": resolved_tier,
            "date_range": {"requested": [spec.start, end], "actual": [
                pd.Timestamp(close.index.min()).strftime("%Y-%m-%d"),
                pd.Timestamp(close.index.max()).strftime("%Y-%m-%d"),
            ]},
            "symbol_count": len(symbols),
            "benchmark": spec.benchmark or "",
            "execution": "T close signal -> T+1 open execution",
            "trade_config": trade_config,
            "dataset": snapshot,
            "research_data": research_manifest,
            "data_quality": data_quality,
            "warnings": warnings,
        }
        position_history = [
            {
                "date": pd.Timestamp(date).strftime("%Y-%m-%d"),
                "values": {
                    str(symbol): round(float(value), 2)
                    for symbol, value in row.items() if float(value) > 0
                },
            }
            for date, row in result.positions.iterrows()
        ]
        artifact = {
            "id": run["id"],
            "name": run["name"],
            "config": spec.model_dump(mode="json"),
            "manifest": manifest,
            "metrics": report["metrics"],
            "nav": self._points(result.nav),
            "benchmark_nav": self._points(result.benchmark_nav),
            "drawdown": self._points(drawdown),
            "exposure": self._points(exposure),
            "positions": position_history,
            "trades": [asdict(trade) for trade in result.trades],
            "blocked_orders": [asdict(order) for order in result.blocked_orders],
            "yearly": report["yearly"],
            "monthly": report["monthly"],
            "trade_stats": report["trade_stats"],
            "risk_diagnostics": report.get("risk_diagnostics", {}),
            "stress_tests": report.get("stress_tests", []),
            "trade_lifecycle": report.get("trade_lifecycle", {}),
            "attribution": {
                name: self._points(
                    values.reindex_like(weights).mul(weights.fillna(0.0)).sum(axis=1)
                )
                for name, values in signal_bundle.contributions.items()
            },
            "signal_metadata": signal_bundle.metadata,
        }
        summary = {
            "strategy": strategy.name,
            "metrics": report["metrics"],
            "trade_stats": report["trade_stats"],
            "warnings": warnings,
            "data_quality": data_quality,
            "nav_points": len(artifact["nav"]),
            "trade_count": len(result.trades),
            "blocked_order_count": len(result.blocked_orders),
        }
        checkpoint(96, "保存结果", "原子写入可复现实验产物")
        return manifest, {"summary": summary, "artifact": artifact}

    def compare(self, run_ids: list[str]) -> dict:
        unique = list(dict.fromkeys(run_ids))
        if not 2 <= len(unique) <= 4:
            raise ValueError("请选择 2–4 个回测进行比较")
        runs = []
        for run_id in unique:
            run = self.store.get(run_id, include_artifact=True)
            if run is None:
                raise KeyError(f"回测不存在: {run_id}")
            if run["status"] != "completed" or "artifact" not in run:
                raise ValueError(f"回测 {run['name']} 尚未完成")
            artifact = run["artifact"]
            runs.append({
                "id": run_id, "name": run["name"], "config": run["config"],
                "metrics": artifact["metrics"], "nav": artifact["nav"],
                "warnings": artifact["manifest"].get("warnings", []),
            })
        return {"runs": runs}


class BacktestWorker:
    def __init__(self, service: BacktestService | None = None, poll_seconds: float = 0.4):
        self.service = service or BacktestService()
        self.poll_seconds = poll_seconds
        self.worker_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self.service.store.interrupt_stale()
            self._thread = threading.Thread(
                target=self.run_forever, name="backtest-worker", daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self.service.store.interrupt_stale(self.worker_id)
        self._thread = None

    def run_forever(self) -> None:
        while not self._stop.is_set():
            self.service.store.interrupt_stale()
            run = self.service.store.claim_next(self.worker_id)
            if run is None:
                self._stop.wait(self.poll_seconds)
                continue
            self.run_one(run)

    def run_one(self, run: dict) -> None:
        from quantmaster.runtime.problems import OperationProblem

        run_id = run["id"]
        heartbeat_stop = threading.Event()
        lease_alive = threading.Event()
        lease_alive.set()

        def heartbeat() -> None:
            while not heartbeat_stop.wait(5.0):
                if self.service.store.heartbeat(run_id, self.worker_id):
                    continue
                lease_alive.clear()
                return

        heartbeat_thread = threading.Thread(
            target=heartbeat, name=f"backtest-heartbeat-{run_id[:8]}", daemon=True,
        )
        heartbeat_thread.start()

        def report_progress(value: int, phase: str, detail: str = "") -> None:
            self.service.store.update(
                run_id, value, phase, detail, expected_worker=self.worker_id,
            )

        try:
            manifest, payload = self.service.run(
                run,
                progress=report_progress,
                cancelled=lambda: (
                    self._stop.is_set() or not lease_alive.is_set()
                    or self.service.store.is_cancelled(run_id)
                ),
            )
            if not lease_alive.is_set():
                return
            path = self.service.store.write_artifact(run_id, payload["artifact"])
            self.service.store.finish(
                run_id, manifest=manifest, result=payload["summary"], artifact_path=str(path),
                expected_worker=self.worker_id,
            )
        except InterruptedError:
            if not lease_alive.is_set():
                return
            if self._stop.is_set() and not self.service.store.is_cancelled(run_id):
                self.service.store.interrupt_stale(self.worker_id)
                return
            self.service.store.cancel(run_id)
            self.service.store.finish(run_id, expected_worker=self.worker_id)
        except OperationProblem as exc:
            logger.warning(
                "回测任务被数据门禁阻止 run=%s code=%s",
                run_id, exc.problem.get("code"),
            )
            if exc.problem.get("can_continue"):
                self.service.store.needs_confirmation(
                    run_id,
                    problem=exc.problem,
                    data_quality=exc.data_quality,
                    expected_worker=self.worker_id,
                )
            else:
                self.service.store.finish(
                    run_id,
                    result={
                        "problem": exc.problem,
                        "data_quality": exc.data_quality or {},
                    },
                    error=exc.problem["message"],
                    expected_worker=self.worker_id,
                )
        except Exception as exc:
            logger.exception("回测任务失败 run=%s", run_id)
            self.service.store.finish(
                run_id, error=str(exc), expected_worker=self.worker_id,
            )
        finally:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=1.0)


_worker: BacktestWorker | None = None
_worker_root = ""


def get_backtest_worker() -> BacktestWorker:
    global _worker, _worker_root
    root = str(get_config().data_root.resolve())
    if _worker is None or root != _worker_root:
        if _worker is not None:
            _worker.stop()
        _worker = BacktestWorker()
        _worker_root = root
    return _worker
