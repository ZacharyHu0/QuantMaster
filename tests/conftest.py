"""测试夹具：合成行情面板 + 隔离的临时数据目录（不触网）。"""

from __future__ import annotations

import os
import re
import shutil
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from quantmaster.config import Config, set_config

_FULL_ONLY_FILES = frozenset({
    "test_automation.py",
    "test_backtest.py",
    "test_composite.py",
    "test_data_resilience.py",
    "test_factors.py",
    "test_fundamentals.py",
    "test_lab.py",
    "test_management_data.py",
    "test_news_workbench.py",
    "test_regime_decision.py",
    "test_report.py",
    "test_research_optimization.py",
    "test_research_pipeline.py",
    "test_rotation_analytics.py",
    "test_rotation_api.py",
    "test_rotation_provider.py",
    "test_rotation_store_service.py",
    "test_settings_checks.py",
    "test_llm.py",
    "test_stock_analysis.py",
    "test_trading_workbenches.py",
    "test_ui_management.py",
})


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--full",
        action="store_true",
        default=False,
        help="run slower integration suites in addition to the default fast contract suite",
    )


def pytest_collection_modifyitems(config, items) -> None:
    if config.getoption("--full"):
        return
    run_browser = os.environ.get("QM_RUN_UI") == "1"
    selected = []
    deselected = []
    for item in items:
        filename = item.path.name
        if filename not in _FULL_ONLY_FILES or (
            filename == "test_ui_management.py" and run_browser
        ):
            selected.append(item)
        else:
            deselected.append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected


def _referenced_test_symbols() -> set[str]:
    pattern = re.compile(
        r"(?<![A-Z0-9])\^?[A-Z0-9][A-Z0-9.^-]*\."
        r"(?:SH|SZ|BJ|CSI|HK|US|JP|KR|SHF|DCE|CZCE|CFFEX|INE|GFEX)\b"
    )
    root = Path(__file__).parent
    return {
        symbol
        for test_file in root.glob("test_*.py")
        for symbol in pattern.findall(test_file.read_text(encoding="utf-8"))
    }


def _finish_security_master_seed(path: Path) -> Path:
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.execute("PRAGMA journal_mode=DELETE")
    from quantmaster.runtime.sqlite import reset_sqlite_runtime_for_tests

    reset_sqlite_runtime_for_tests()
    return path


@pytest.fixture(scope="session")
def _minimal_security_master(tmp_path_factory) -> Path:
    """Build the small current-contract catalog once instead of 600 times."""
    from quantmaster.data import instruments

    path = tmp_path_factory.mktemp("catalog-seed") / "security_master.sqlite"
    original = instruments.InstrumentStore._import_bundled_snapshot

    def import_minimal(store):
        records = instruments._seed_records()
        records.extend([
            {
                "symbol": symbol,
                "code": symbol.split(".", 1)[0],
                "name": name,
                "market": "CN",
                "exchange": symbol.rsplit(".", 1)[1],
                "asset_type": "index",
                "currency": "CNY",
            }
            for symbol, name in (
                ("000300.SH", "沪深300"),
                ("000905.SH", "中证500"),
                ("000852.SH", "中证1000"),
                ("399006.SZ", "创业板指"),
            )
        ])
        known = {str(record["symbol"]) for record in records}
        current_names = {"000001.SZ": "平安银行", "600000.SH": "浦发银行"}
        future_suffixes = {"SHF", "DCE", "CZCE", "CFFEX", "INE", "GFEX"}
        for symbol in sorted(_referenced_test_symbols() - known):
            code, suffix = symbol.rsplit(".", 1)
            records.append({
                "symbol": symbol,
                "provider_symbol": symbol,
                "code": code,
                "name": current_names.get(symbol, ""),
                "market": (
                    "CN" if suffix in instruments.DOMESTIC_SUFFIXES
                    else "FUT" if suffix in future_suffixes
                    else suffix
                ),
                "exchange": suffix,
                "asset_type": (
                    "index" if suffix == "CSI" or symbol.startswith("^")
                    else "future" if suffix in future_suffixes
                    else "stock"
                ),
            })
        store.upsert(records, source="test-seed", source_priority=10)

    instruments.InstrumentStore._import_bundled_snapshot = import_minimal
    try:
        instruments.InstrumentStore(path)
    finally:
        instruments.InstrumentStore._import_bundled_snapshot = original
    return _finish_security_master_seed(path)


@pytest.fixture(scope="session")
def _full_security_master(tmp_path_factory) -> Path:
    """Import the packaged catalog once for the tests that exercise the full snapshot."""
    from quantmaster.data.instruments import InstrumentStore

    path = tmp_path_factory.mktemp("full-catalog-seed") / "security_master.sqlite"
    InstrumentStore(path)
    return _finish_security_master_seed(path)


@pytest.fixture(autouse=True)
def isolated_config(tmp_path, request, _minimal_security_master):
    """每个测试用独立数据目录，避免污染真实数据。"""
    from quantmaster.data.repair import reset_data_repair_manager_for_tests
    from quantmaster.rotation.etf_jobs import shutdown_etf_research_jobs
    from quantmaster.rotation.etf_research import reset_etf_research_service
    from quantmaster.rotation.service import reset_rotation_runtime_for_tests
    from quantmaster.runtime.sqlite import reset_sqlite_runtime_for_tests

    reset_data_repair_manager_for_tests()
    reset_rotation_runtime_for_tests()
    shutdown_etf_research_jobs()
    reset_etf_research_service()
    reset_sqlite_runtime_for_tests()
    cfg = Config()
    cfg.data.root = str(tmp_path / "data")
    cfg.data_root.mkdir(parents=True, exist_ok=True)
    seed = (
        request.getfixturevalue("_full_security_master")
        if request.node.path.name == "test_instruments.py"
        else _minimal_security_master
    )
    shutil.copy2(seed, cfg.data_root / "security_master.sqlite")
    set_config(cfg)
    yield cfg
    reset_data_repair_manager_for_tests()
    reset_rotation_runtime_for_tests()
    set_config(None)
    reset_sqlite_runtime_for_tests()


def make_panel(days: int = 150, n: int = 8, seed: int = 7) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2023-01-02", periods=days)
    symbols = [f"60000{i}.SH" for i in range(n)]
    log_returns = rng.normal(0.0003, 0.02, (days, n))
    close = pd.DataFrame(
        100 * np.exp(np.cumsum(log_returns, axis=0)), index=dates, columns=symbols
    )
    open_px = close * (1 + rng.normal(0, 0.003, (days, n)))
    high = np.maximum(close, open_px) * (1 + abs(rng.normal(0, 0.005, (days, n))))
    low = np.minimum(close, open_px) * (1 - abs(rng.normal(0, 0.005, (days, n))))
    volume = pd.DataFrame(rng.uniform(1e6, 1e7, (days, n)), index=dates, columns=symbols)
    return {
        "open": open_px, "high": pd.DataFrame(high, index=dates, columns=symbols),
        "low": pd.DataFrame(low, index=dates, columns=symbols),
        "close": close, "volume": volume,
        "amount": volume * close, "turnover": volume / 1e8,
    }


@pytest.fixture
def panel() -> dict[str, pd.DataFrame]:
    return make_panel()
