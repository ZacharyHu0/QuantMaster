"""StockDB-backed daily synthetic board-index snapshots."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

import pandas as pd

BOARD_INDEX_METHODS = {
    "equal": 1,
    "float_mv": 2,
    "amount": 3,
    "volume": 4,
    "total_mv": 5,
}
BOARD_INDEX_MEMBERSHIP = "current_constituents_backcast"
BOARD_INDEX_BASE = 1000.0
BOARD_INDEX_SESSIONS = 120
BOARD_INDEX_ALGORITHM_VERSION = "QM_BOARD_INDEX_V1"
BOARD_INDEX_BATCH_SIZE = 12
_CATEGORY_MAP = {"申万一级": "sw1", "申万二级": "sw2", "概念": "theme"}
_LEVEL_MAP = {"sw1": "L1", "sw2": "L2", "theme": "CONCEPT"}
_WINDOWS = (1, 3, 5, 20)


def _number(value: Any, digits: int = 6) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return round(result, digits) if pd.notna(result) else None


def _date(value: Any) -> str:
    text = str(value or "").partition(".")[0]
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    try:
        return pd.Timestamp(value).date().isoformat()
    except (TypeError, ValueError):
        return ""


def _series(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = ("open", "high", "low", "close", "pct_chg", "volume", "amount")
    result = [
        {
            "date": _date(row.get("date")),
            **{field: _number(row.get(field)) for field in fields},
            "stock_count": int(row.get("stock_count") or 0),
        }
        for row in rows
        if _date(row.get("date"))
    ]
    result.sort(key=lambda row: row["date"])
    return result[-BOARD_INDEX_SESSIONS:]


def _method_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    closes = [float(row["close"]) for row in rows if row.get("close") is not None]
    changes: dict[str, float | None] = {}
    for window in _WINDOWS:
        changes[str(window)] = (
            round((closes[-1] / closes[-1 - window] - 1) * 100, 4)
            if len(closes) > window and closes[-1 - window] else None
        )
    return {
        "status": "ready" if closes else "unavailable",
        "last": round(closes[-1], 4) if closes else None,
        "changes": changes,
        "sessions": len(rows),
    }


def _constituents(
    symbols: list[str],
    close: pd.DataFrame,
    amount: pd.DataFrame,
    names: dict[str, str],
) -> list[dict[str, Any]]:
    result = []
    for symbol in symbols:
        values = close[symbol].dropna() if symbol in close else pd.Series(dtype=float)
        amounts = amount[symbol].dropna() if symbol in amount else pd.Series(dtype=float)
        result.append({
            "symbol": symbol,
            "name": names.get(symbol) or symbol,
            "last": _number(values.iloc[-1], 4) if not values.empty else None,
            "change_pct": (
                round((float(values.iloc[-1]) / float(values.iloc[-2]) - 1) * 100, 4)
                if len(values) > 1 and float(values.iloc[-2]) else None
            ),
            "amount": _number(amounts.iloc[-1], 2) if not amounts.empty else None,
            "as_of": str(values.index[-1].date()) if not values.empty else "",
        })
    return result


def _selected_boards(
    boards: Iterable[dict[str, Any]],
    *,
    selected_l2: set[str],
    theme_codes: set[str],
) -> list[dict[str, Any]]:
    selected = []
    for board in boards:
        category = _CATEGORY_MAP.get(str(board.get("category") or ""))
        code = str(board.get("code") or "").upper()
        if category not in {"sw1", "sw2", "theme"} or not code:
            continue
        if category == "theme" and code not in theme_codes:
            continue
        selected.append({
            **board,
            "category_key": category,
            "code": code,
            "priority": category == "sw1" or (
                category == "sw2" and code in selected_l2
            ),
        })
    selected.sort(key=lambda board: (
        0 if board["category_key"] == "sw1" else
        1 if board["category_key"] == "sw2" and board["code"] in selected_l2 else
        2 if board["category_key"] == "sw2" else 3,
        str(board.get("name") or ""),
        board["code"],
    ))
    return selected


def _snapshot_payload(
    *,
    as_of: str,
    items: list[dict[str, Any]],
    details: dict[str, dict[str, Any]],
    expected_boards: int,
    unavailable: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    expected_methods = expected_boards * len(BOARD_INDEX_METHODS)
    completed_methods = len(items) * len(BOARD_INDEX_METHODS)
    eligible = completed_methods - unavailable
    pending_boards = expected_boards - len(items)
    issues = []
    if pending_boards:
        issues.append(f"{pending_boards} 个板块等待后台补齐")
    if unavailable:
        issues.append(f"{unavailable} 个板块算法暂不可用")
    quality = {
        "status": "complete" if not issues else "partial",
        "eligible_count": eligible,
        "expected_count": expected_methods,
        "coverage": round(eligible / expected_methods, 4) if expected_methods else None,
        "issues": issues,
    }
    return {
        "as_of": as_of,
        "items": list(items),
        "details": dict(details),
        "summary": {
            "board_count": len(items),
            "expected_board_count": expected_boards,
            "pending_board_count": pending_boards,
            "method_count": len(BOARD_INDEX_METHODS),
            "unavailable_method_count": unavailable,
        },
        "definition": {
            "methods": list(BOARD_INDEX_METHODS),
            "membership_semantics": BOARD_INDEX_MEMBERSHIP,
            "frequency": "1d",
            "base": BOARD_INDEX_BASE,
            "sessions": BOARD_INDEX_SESSIONS,
            "algorithm_version": BOARD_INDEX_ALGORITHM_VERSION,
        },
    }, quality


def _list_item(detail: dict[str, Any]) -> dict[str, Any]:
    return {
        key: detail.get(key)
        for key in (
            "code", "board_code", "name", "category", "level", "member_count",
            "eligible_count", "coverage", "methods",
        )
    }


def _unavailable_method_count(detail: dict[str, Any]) -> int:
    methods = detail.get("methods") if isinstance(detail.get("methods"), dict) else {}
    return sum(
        str(methods.get(name, {}).get("status") or "unavailable") != "ready"
        for name in BOARD_INDEX_METHODS
    )


def build_board_index_data(
    source: Any,
    *,
    close: pd.DataFrame,
    amount: pd.DataFrame,
    names: dict[str, str],
    as_of: str,
    selected_l2: set[str],
    theme_codes: set[str],
    progress: Callable[[int, str, str], None],
    cancelled: Callable[[], bool],
    checkpoint: Callable[[dict[str, Any], dict[str, Any]], None] | None = None,
    resume_details: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build compact list rows and separately indexed details for current boards."""

    boards = _selected_boards(
        source.boards(), selected_l2=selected_l2, theme_codes=theme_codes,
    )
    if not boards:
        raise RuntimeError("本地 StockDB 没有可用于板块指数的当前成分目录")
    start = (pd.Timestamp(as_of) - pd.Timedelta(days=260)).date().isoformat()
    items: list[dict[str, Any]] = []
    details: dict[str, dict[str, Any]] = {}
    unavailable = 0
    for position, board in enumerate(boards):
        if cancelled():
            raise InterruptedError("板块指数刷新已取消")
        category = board["category_key"]
        board_code = board["code"]
        item_key = f"{category}:{board_code}".upper()
        resumed = (resume_details or {}).get(item_key)
        if isinstance(resumed, dict):
            detail = dict(resumed)
            items.append(_list_item(detail))
            details[item_key] = detail
            unavailable += _unavailable_method_count(detail)
            progress(
                88 + round((position + 1) / len(boards) * 7),
                "恢复板块指数",
                f"{position + 1}/{len(boards)} · {detail.get('name') or board_code}",
            )
            continue
        symbols = sorted(set(str(value).upper() for value in board.get("symbols") or []))
        method_series: dict[str, list[dict[str, Any]]] = {}
        methods: dict[str, dict[str, Any]] = {}
        for name, method in BOARD_INDEX_METHODS.items():
            try:
                rows = _series(source.native_board_index(
                    symbols, start, as_of, method=method, base=BOARD_INDEX_BASE,
                ))
                method_series[name] = rows
                methods[name] = _method_summary(rows)
                if not rows:
                    methods[name]["reason"] = "StockDB 未返回可用交易日"
                    unavailable += 1
            except (OSError, RuntimeError, TypeError, ValueError) as exc:
                methods[name] = {
                    "status": "unavailable", "last": None,
                    "changes": {str(window): None for window in _WINDOWS},
                    "sessions": 0, "reason": str(exc)[:160],
                }
                method_series[name] = []
                unavailable += 1
        quotes = _constituents(symbols, close, amount, names)
        observed = sum(item["last"] is not None for item in quotes)
        item = {
            "code": item_key,
            "board_code": board_code,
            "name": str(board.get("name") or board_code),
            "category": category,
            "level": _LEVEL_MAP[category],
            "member_count": len(symbols),
            "eligible_count": observed,
            "coverage": round(observed / len(symbols), 4) if symbols else 0.0,
            "methods": methods,
        }
        items.append(item)
        details[item_key] = {
            **item,
            "membership_semantics": BOARD_INDEX_MEMBERSHIP,
            "frequency": "1d",
            "base": BOARD_INDEX_BASE,
            "series": method_series,
            "constituents": quotes,
        }
        progress(
            88 + round((position + 1) / len(boards) * 7),
            "计算板块指数",
            f"{position + 1}/{len(boards)} · {item['name']}",
        )
        next_board = boards[position + 1] if position + 1 < len(boards) else None
        priority_boundary = bool(board["priority"]) and not bool(
            next_board and next_board["priority"]
        )
        batch_boundary = (position + 1) % BOARD_INDEX_BATCH_SIZE == 0
        if checkpoint is not None and (
            priority_boundary or batch_boundary or next_board is None
        ):
            checkpoint(*_snapshot_payload(
                as_of=as_of,
                items=items,
                details=details,
                expected_boards=len(boards),
                unavailable=unavailable,
            ))
    return _snapshot_payload(
        as_of=as_of,
        items=items,
        details=details,
        expected_boards=len(boards),
        unavailable=unavailable,
    )
