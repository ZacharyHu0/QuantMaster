from __future__ import annotations

import pandas as pd

from quantmaster.rotation.board_indexes import (
    BOARD_INDEX_ALGORITHM_VERSION,
    BOARD_INDEX_MEMBERSHIP,
    BOARD_INDEX_METHODS,
    build_board_index_data,
)


class _StockDB:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], int]] = []

    def boards(self):
        return [
            {
                "code": "801010.SI", "name": "农林牧渔", "category": "申万一级",
                "symbols": ["000001.SZ", "600000.SH"],
            },
            {
                "code": "801011.SI", "name": "林业", "category": "申万二级",
                "symbols": ["000001.SZ"],
            },
            {
                "code": "300001.TI", "name": "测试题材", "category": "概念",
                "symbols": ["600000.SH"],
            },
            {
                "code": "300002.TI", "name": "未入选题材", "category": "概念",
                "symbols": ["600000.SH"],
            },
        ]

    def native_board_index(self, symbols, _start, _end, *, method, base):
        assert base == 1000
        self.calls.append((tuple(symbols), method))
        return [
            {
                "date": 20260810 + offset,
                "open": 1000 + offset,
                "high": 1002 + offset,
                "low": 999 + offset,
                "close": 1000 + offset + method,
                "pct_chg": 0.1,
                "volume": 100,
                "amount": 200,
                "stock_count": len(symbols),
            }
            for offset in range(6)
        ]


def test_board_index_snapshot_uses_current_membership_and_all_five_methods():
    source = _StockDB()
    dates = pd.date_range("2026-08-10", periods=3, freq="B")
    close = pd.DataFrame(
        {"000001.SZ": [10.0, 10.5, 11.0], "600000.SH": [8.0, 8.1, 8.2]},
        index=dates,
    )
    amount = pd.DataFrame(
        {"000001.SZ": [1, 2, 3], "600000.SH": [4, 5, 6]}, index=dates,
    )
    progress = []

    data, quality = build_board_index_data(
        source,
        close=close,
        amount=amount,
        names={"000001.SZ": "平安银行", "600000.SH": "浦发银行"},
        as_of="2026-08-17",
        selected_l2={"801011.SI"},
        theme_codes={"300001.TI"},
        progress=lambda *value: progress.append(value),
        cancelled=lambda: False,
    )

    assert [item["category"] for item in data["items"]] == ["sw1", "sw2", "theme"]
    assert len(source.calls) == 3 * len(BOARD_INDEX_METHODS)
    assert quality["status"] == "complete"
    detail = data["details"]["SW1:801010.SI"]
    assert detail["membership_semantics"] == BOARD_INDEX_MEMBERSHIP
    assert set(detail["series"]) == set(BOARD_INDEX_METHODS)
    assert detail["constituents"][0]["name"] == "平安银行"
    assert data["definition"]["frequency"] == "1d"
    assert data["definition"]["algorithm_version"] == BOARD_INDEX_ALGORITHM_VERSION
    assert progress[-1][0] == 95


def test_board_index_snapshot_marks_one_method_unavailable_without_fabricating_data():
    source = _StockDB()
    original = source.native_board_index

    def calculate(symbols, start, end, *, method, base):
        if method == 2:
            raise RuntimeError("缺少流通市值")
        return original(symbols, start, end, method=method, base=base)

    source.native_board_index = calculate
    dates = pd.date_range("2026-08-10", periods=2, freq="B")
    close = pd.DataFrame({"000001.SZ": [10.0, 10.5]}, index=dates)
    amount = pd.DataFrame({"000001.SZ": [1, 2]}, index=dates)

    data, quality = build_board_index_data(
        source,
        close=close,
        amount=amount,
        names={},
        as_of="2026-08-17",
        selected_l2=set(),
        theme_codes=set(),
        progress=lambda *_args: None,
        cancelled=lambda: False,
    )

    method = data["details"]["SW1:801010.SI"]["methods"]["float_mv"]
    assert method == {
        "status": "unavailable",
        "last": None,
        "changes": {"1": None, "3": None, "5": None, "20": None},
        "sessions": 0,
        "reason": "缺少流通市值",
    }
    assert quality["status"] == "partial"


def test_board_index_batches_publish_partial_and_resume_only_pending_boards():
    source = _StockDB()
    source.boards = lambda: [
        {
            "code": f"801{index:03d}.SI",
            "name": f"行业{index}",
            "category": "申万一级",
            "symbols": ["000001.SZ"],
        }
        for index in range(14)
    ]
    dates = pd.date_range("2026-08-10", periods=2, freq="B")
    close = pd.DataFrame({"000001.SZ": [10.0, 10.5]}, index=dates)
    amount = pd.DataFrame({"000001.SZ": [1, 2]}, index=dates)
    checkpoints = []

    data, quality = build_board_index_data(
        source,
        close=close,
        amount=amount,
        names={},
        as_of="2026-08-17",
        selected_l2=set(),
        theme_codes=set(),
        progress=lambda *_args: None,
        cancelled=lambda: False,
        checkpoint=lambda value, state: checkpoints.append((value, state)),
    )

    partial_data, partial_quality = checkpoints[0]
    assert partial_quality["status"] == "partial"
    assert partial_data["summary"] == {
        "board_count": 12,
        "expected_board_count": 14,
        "pending_board_count": 2,
        "method_count": 5,
        "unavailable_method_count": 0,
    }
    assert quality["status"] == "complete"
    assert data["summary"]["pending_board_count"] == 0

    resumed_source = _StockDB()
    resumed_source.boards = source.boards
    resumed, resumed_quality = build_board_index_data(
        resumed_source,
        close=close,
        amount=amount,
        names={},
        as_of="2026-08-17",
        selected_l2=set(),
        theme_codes=set(),
        progress=lambda *_args: None,
        cancelled=lambda: False,
        resume_details=partial_data["details"],
    )

    assert len(resumed_source.calls) == 2 * len(BOARD_INDEX_METHODS)
    assert resumed_quality["status"] == "complete"
    assert len(resumed["details"]) == 14
