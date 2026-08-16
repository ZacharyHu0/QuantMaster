"""Market-owned application service for the market overview projection."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Literal, cast

import pandas as pd

from quantmaster.runtime.json import strict_json_dumps
from quantmaster.trading_sessions import default_close_data_end

logger = logging.getLogger(__name__)

PERSONAL_MARKET_GROUP = "我的股票"
ProgressEmitter = Callable[..., None]


def _series_to_points(series: pd.Series) -> list[list]:
    return [[str(key.date()), round(float(value), 6)] for key, value in series.dropna().items()]


def _personal_market_symbols() -> tuple[dict[str, str], dict[str, list[str]]]:
    """Merge favorites, follows and holdings while preserving their memberships."""
    from quantmaster.data.names import read_stock_names
    from quantmaster.portfolio import AssetListStore, Ledger

    symbols: dict[str, str] = {}
    memberships: dict[str, list[str]] = {}

    def usable_name(value: object, symbol: str) -> str:
        name = str(value or "").strip()
        return "" if name.upper() == symbol else name

    lists = AssetListStore(read_only=True).all()
    for list_name in ("favorites", "following"):
        for item in lists.get(list_name, []):
            symbol = str(item["symbol"]).upper()
            name = usable_name(item.get("name"), symbol)
            symbols.setdefault(symbol, name)
            if name and not symbols[symbol]:
                symbols[symbol] = name
            memberships.setdefault(symbol, []).append(list_name)

    for position in Ledger(read_only=True).positions():
        if position.shares <= 0:
            continue
        symbol = str(position.symbol).upper()
        symbols.setdefault(symbol, "")
        memberships.setdefault(symbol, []).append("holdings")

    missing = [symbol for symbol, name in symbols.items() if not usable_name(name, symbol)]
    if missing:
        cached_names = read_stock_names(missing)
        for symbol in missing:
            symbols[symbol] = usable_name(cached_names.get(symbol), symbol) or symbol
    return symbols, memberships


def _market_groups() -> dict[str, dict[str, str]]:
    from quantmaster.data.akshare_source import A_SHARE_INDEXES, FUTURES_MAIN
    from quantmaster.data.reference_catalog import GLOBAL_REFS

    return {
        "A股指数": dict(A_SHARE_INDEXES),
        "全球市场": {
            key: value[1] for key, value in GLOBAL_REFS.items() if "=" not in key and "-" not in key
        },
        "商品与汇率": {
            **{key: value for key, value in FUTURES_MAIN.items() if not key.startswith("IF")},
            **{key: value[1] for key, value in GLOBAL_REFS.items() if "=" in key or "-" in key},
        },
    }


def _market_item(symbol: str, name: str, frame: pd.DataFrame, meta: dict | None) -> dict | None:
    from quantmaster.market import classify_opportunity, indicator_frame

    if frame is None or frame.empty or "close" not in frame:
        return None
    close = frame["close"].dropna()
    if close.empty:
        return None
    checked_at = (meta or {}).get("checked_at")
    rsi = None
    rsi_history: list[list] = []
    try:
        rsi_series = indicator_frame(pd.DataFrame({"close": close}))["rsi_14"].dropna()
        rsi_history = _series_to_points(rsi_series.tail(180))
        if not rsi_series.empty and pd.notna(rsi_series.iloc[-1]):
            rsi = round(float(rsi_series.iloc[-1]), 2)
    except ValueError:
        rsi = None
    try:
        quality = json.loads(str((meta or {}).get("quality_json") or "{}"))
        if not isinstance(quality, dict):
            quality = {}
    except (TypeError, ValueError, json.JSONDecodeError):
        quality = {}
    if not quality:
        quality = {
            "status": "degraded",
            "sources": [str((meta or {}).get("last_source") or "local-cache")],
            "issues": ["缓存缺少版本化质量与来源证据"],
            "stale": str((meta or {}).get("last_status") or "") in {
                "stale", "refresh_failed",
            },
            "partial": True,
        }
    freshness = (
        "stale"
        if str((meta or {}).get("last_status") or "ready") in {"stale", "refresh_failed"}
        else "ready"
    )
    return {
        "symbol": symbol,
        "name": name,
        "last": round(float(close.iloc[-1]), 3),
        "change_pct": round(float(close.iloc[-1] / close.iloc[-2] - 1) * 100, 2) if len(close) > 1 else 0.0,
        "nav": _series_to_points(close / close.iloc[0]),
        "as_of": str(close.index[-1].date()),
        "checked_at": (pd.Timestamp.fromtimestamp(float(checked_at)).isoformat() if checked_at else ""),
        "cache_status": str((meta or {}).get("last_status") or "ready"),
        "source": str((meta or {}).get("last_source") or "local-cache"),
        "rsi_14": rsi,
        "rsi_history": rsi_history,
        "opportunity": classify_opportunity(rsi),
        "data_quality": quality,
        "freshness": freshness,
        "state": freshness,
    }


def _market_overview_response(
    groups: dict[str, dict[str, str]],
    items: dict[tuple[str, str], dict],
    failures: dict[tuple[str, str], dict],
    store,
    total: int,
) -> dict:
    """Assemble a market response strictly from already available cards."""

    result = {
        group: [items[(group, symbol)] for symbol in symbols if (group, symbol) in items]
        for group, symbols in groups.items()
    }
    unavailable = []
    group_statuses = {}
    for group, symbols in groups.items():
        stale = sum(
            str(items[(group, symbol)].get("freshness")) == "stale"
            for symbol in symbols
            if (group, symbol) in items
        )
        missing = [symbol for symbol in symbols if (group, symbol) not in items]
        for symbol in missing:
            missing_issue = failures.get((group, symbol), {})
            meta = store.metadata(symbol) or {}
            checked_at = float(meta.get("checked_at") or 0)
            error_code = missing_issue.get("error_code", "no_usable_data")
            message = missing_issue.get("message", "没有本地缓存，且数据源未返回可用行情")
            last_success_at = (
                pd.Timestamp.fromtimestamp(checked_at).isoformat() if checked_at else ""
            )
            unavailable_item = {
                "group": group,
                "symbol": symbol,
                "name": symbols[symbol],
                "state": "unavailable",
                "status": "unavailable",
                "error_code": error_code,
                "message": message,
                "source_attempts": missing_issue.get("source_attempts", []),
                "last_success_at": last_success_at,
            }
            unavailable.append(unavailable_item)
            result[group].append({
                **unavailable_item,
                "last": None,
                "change_pct": None,
                "nav": [],
                "as_of": "",
                "checked_at": last_success_at,
                "cache_status": "unavailable",
                "source": "",
                "rsi_14": None,
                "rsi_history": [],
                "opportunity": {"code": "unavailable", "label": "行情暂缺"},
                "data_quality": {
                    "status": "unavailable",
                    "sources": [],
                    "issues": [message],
                    "stale": False,
                    "partial": True,
                },
                "freshness": "unavailable",
            })
        group_statuses[group] = {
            "configured": len(symbols),
            "ready": len(result[group]) - stale - len(missing),
            "stale": stale,
            "unavailable": len(missing),
            "issues": [
                {
                    "symbol": symbol,
                    "error_code": status_issue.get("error_code", "unavailable"),
                    "message": status_issue.get("message", "数据源未返回可用行情"),
                }
                for symbol in symbols
                if (status_issue := failures.get((group, symbol)))
            ],
        }
    item_qualities = [
        item.get("data_quality") or {}
        for values in result.values()
        for item in values
    ]
    stale_total = sum(int(str(value["stale"])) for value in group_statuses.values())
    degraded_total = sum(
        str(value.get("status") or "degraded") != "verified"
        or bool(value.get("stale"))
        or bool(value.get("partial"))
        for value in item_qualities
    )
    missing_total = len(unavailable)
    ready_total = sum(int(value["ready"]) for value in group_statuses.values())
    observed_total = ready_total + stale_total
    quality_status = (
        "unavailable" if total and not observed_total
        else "degraded" if degraded_total or stale_total or missing_total
        else "verified"
    )
    lineage = [
        {
            "group": group,
            "symbol": item.get("symbol"),
            "as_of": item.get("as_of"),
            "checked_at": item.get("checked_at"),
        }
        for group, values in sorted(result.items())
        for item in values
    ]
    snapshot_id = hashlib.sha256(
        strict_json_dumps(lineage, sort_keys=True).encode("utf-8")
    ).hexdigest()[:24] if lineage else ""
    stale_reasons = list((
        f"{missing_total} 个标的不可用" if missing_total else "",
        f"{stale_total} 个标的使用陈旧缓存" if stale_total else "",
    ))
    return {
        "meta": {
            "snapshot_id": snapshot_id,
            "schema_version": 2,
            "algorithm_version": "QM_MARKET_OVERVIEW_V2",
            "input_fingerprint": snapshot_id,
            "as_of": max(
                (str(item.get("as_of") or "") for values in result.values() for item in values),
                default="",
            ),
            "generated_at": max(
                (
                    str(item.get("checked_at") or "")
                    for values in result.values()
                    for item in values
                ),
                default="",
            ),
            "stale": bool(stale_total or missing_total),
            "stale_reasons": [value for value in stale_reasons if value],
            "quality": {"status": quality_status},
        },
        "groups": result,
        "group_counts": {group: len(symbols) for group, symbols in groups.items()},
        "group_statuses": group_statuses,
        "unavailable_items": unavailable,
        "data_quality": {
            "status": quality_status,
            "stale": bool(stale_total),
            "partial": bool(stale_total or missing_total),
            "issues": [item for item in [
                f"{missing_total} 个标的不可用" if missing_total else "",
                f"{stale_total} 个标的使用陈旧缓存" if stale_total else "",
                f"{degraded_total} 个标的证据未完全验证" if degraded_total else "",
            ] if item],
            "requested_count": total,
            "observed_count": observed_total,
        },
    }


def build_market_overview_data(
    start: str | None = None,
    progress: ProgressEmitter | None = None,
    refresh: Literal["auto", "incremental", "local"] = "auto",
) -> dict:
    """Build the personal-stock and reference-market projection from owned evidence."""
    from quantmaster import data as data_api
    from quantmaster.data.reference_catalog import GLOBAL_REFS
    from quantmaster.data.reference_market import refresh_reference_panel
    from quantmaster.data.storage import BarStore

    end = pd.Timestamp(default_close_data_end())
    start_ts = pd.Timestamp(start) if start else end - pd.Timedelta(days=365)
    start_value, end_value = str(start_ts.date()), str(end.date())
    personal_symbols, personal_memberships = _personal_market_symbols()
    groups = {PERSONAL_MARKET_GROUP: personal_symbols, **_market_groups()}
    store = BarStore()
    items: dict[tuple[str, str], dict] = {}
    failures: dict[tuple[str, str], dict] = {}
    total = sum(len(symbols) for symbols in groups.values())
    completed = 0

    for group, symbols in groups.items():
        for symbol, name in symbols.items():
            cached = store.get(symbol, columns=["close"])
            if cached is None:
                continue
            item = _market_item(symbol, name, cached.loc[start_value:end_value], store.metadata(symbol))
            if item is None:
                continue
            if group == PERSONAL_MARKET_GROUP:
                item["memberships"] = personal_memberships.get(symbol, [])
            items[(group, symbol)] = item
            if progress:
                progress(
                    2,
                    "读取本地市场缓存",
                    f"{name} · 已显示本地数据",
                    {"kind": "market_item", "stage": "cache", "group": group, "item": item},
                )

    if refresh == "local":
        return _market_overview_response(groups, items, failures, store, total)

    yahoo_symbols = set(GLOBAL_REFS)

    def one(group: str, symbol: str, name: str):
        envelope = data_api.refresh_history(
            symbol,
            start_value,
            end_value,
            store=store,
            mode=refresh,
            work_class="interactive",
        )
        return group, symbol, name, envelope.require_data(), envelope.quality.to_dict()

    futures = {}
    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="market-sync") as executor:
        batch = [symbol for symbol in yahoo_symbols if any(symbol in values for values in groups.values())]
        futures[executor.submit(refresh_reference_panel, batch, start_value, end_value, refresh, store)] = (
            "__yahoo__",
            "",
            "",
        )
        for group, symbols in groups.items():
            for symbol, name in symbols.items():
                if symbol in yahoo_symbols:
                    continue
                futures[executor.submit(one, group, symbol, name)] = (group, symbol, name)

        for future in as_completed(futures):
            group, symbol, name = futures[future]
            if group == "__yahoo__":
                try:
                    frames, reference_failures = future.result()
                except Exception as exc:
                    logger.debug("全球参考市场同步失败: %s", exc)
                    frames, reference_failures = {}, {}
                batch_lookup: dict[str, list[tuple[str, str]]] = {}
                for candidate_group, values in groups.items():
                    for candidate_symbol, candidate_name in values.items():
                        if candidate_symbol in yahoo_symbols:
                            batch_lookup.setdefault(candidate_symbol, []).append(
                                (candidate_group, candidate_name)
                            )
                for batch_symbol in batch:
                    frame = frames.get(batch_symbol)
                    for batch_group, batch_name in batch_lookup[batch_symbol]:
                        completed += 1
                        if batch_symbol in reference_failures:
                            failures[(batch_group, batch_symbol)] = reference_failures[batch_symbol]
                        item = _market_item(batch_symbol, batch_name, frame, store.metadata(batch_symbol))
                        if item is None and batch_symbol in reference_failures:
                            item = items.get((batch_group, batch_symbol))
                            if item is not None:
                                item = {
                                    **item,
                                    "cache_status": "stale",
                                    "freshness": "stale",
                                    "state": "stale",
                                }
                        if item is not None and batch_group == PERSONAL_MARKET_GROUP:
                            item["memberships"] = personal_memberships.get(batch_symbol, [])
                        if item is not None:
                            items[(batch_group, batch_symbol)] = item
                        if progress:
                            progress(
                                3 + round(94 * completed / max(1, total)),
                                "同步市场行情",
                                f"{completed}/{total} · {batch_name} · "
                                f"{'已更新' if item else '沿用缓存或跳过'}",
                                {
                                    "kind": "market_item",
                                    "stage": "updated",
                                    "group": batch_group,
                                    "item": item,
                                }
                                if item
                                else None,
                                "info" if item else "warning",
                            )
                continue
            completed += 1
            try:
                market_result = cast(
                    tuple[str, str, str, pd.DataFrame, dict],
                    future.result(),
                )
                frame = market_result[3]
                item = _market_item(symbol, name, frame, store.metadata(symbol))
                if item is not None:
                    item["data_quality"] = market_result[4]
            except Exception as exc:
                logger.debug("市场概览跳过 %s: %s", symbol, exc)
                item = items.get((group, symbol))
                failures[(group, symbol)] = {
                    "error_code": type(exc).__name__,
                    "message": (str(exc).strip() or "同步失败")[:500],
                    "source_attempts": [],
                }
            if item is not None and group == PERSONAL_MARKET_GROUP:
                item["memberships"] = personal_memberships.get(symbol, [])
            if item is not None:
                items[(group, symbol)] = item
            if progress:
                progress(
                    3 + round(94 * completed / max(1, total)),
                    "同步市场行情",
                    f"{completed}/{total} · {name} · {'已更新' if item else '已跳过'}",
                    {"kind": "market_item", "stage": "updated", "group": group, "item": item}
                    if item is not None
                    else None,
                    "info" if item else "warning",
                )

    return _market_overview_response(groups, items, failures, store, total)
