"""Layered online acquisition for rotation inputs.

Automatic refresh prefers Tushare's date-partitioned research data and strict SW2021
classification.  Concepts prefer Eastmoney through AKShare and fall back to Tushare's
DC catalog when that interface is available.  A failed source never deletes the
previous catalog or ETF observations.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable
from datetime import date
from typing import Any

import pandas as pd

from quantmaster.data.resilience import akshare_call
from quantmaster.data.tushare_source import TushareSource
from quantmaster.rotation.store import RotationStore
from quantmaster.rotation.taxonomy import SW2021_L1

logger = logging.getLogger(__name__)

_BROAD_CORE_TERMS = (
    "沪深300", "中证A50", "中证A500", "中证1000", "中证2000", "中证500",
    "中证800", "中证100", "上证50", "上证180", "上证380", "上证综指",
    "上证指数", "深证100", "深证成指", "深证主板50", "创业板指", "创业板50",
    "科创50", "科创100", "北证50", "MSCI中国A", "富时中国A50", "标普中国A股",
)
_BROAD_STRATEGY_TERMS = (
    "红利", "股息", "低波", "自由现金流", "央企", "国企", "民企",
)
_SECTOR_OR_NON_CN_TERMS = (
    "医药", "医疗", "消费", "食品", "酒", "金融", "银行", "证券", "保险",
    "地产", "能源", "煤炭", "有色", "化工", "电力", "新能源", "光伏", "电池",
    "半导体", "芯片", "计算机", "传媒", "通信", "军工", "汽车", "农业", "家电",
    "机械", "材料", "资源", "港股", "恒生", "纳指", "标普500", "日经", "德国",
    "法国", "美国", "黄金", "白银", "商品", "债", "货币",
)
_EASTMONEY_THEME_SOURCE = "eastmoney-concept"
_TUSHARE_THEME_SOURCE = "tushare:dc-concept"


class RotationProviderCallError(RuntimeError):
    """An upstream provider call failed at the classified network boundary."""


class ThemeSourceUnavailable(RuntimeError):
    """A theme provider cannot currently supply a usable coherent catalog."""


def _symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.endswith((".SH", ".SZ", ".BJ")):
        return text
    digits = text.zfill(6) if text.isdigit() else text
    if len(digits) != 6 or not digits.isdigit():
        return ""
    suffix = "SH" if digits.startswith(("6", "9")) else (
        "BJ" if digits.startswith(("4", "8")) else "SZ"
    )
    return f"{digits}.{suffix}"


def _broad_etf_category(name: Any, benchmark: Any = "") -> str:
    """Classify only auditable mainland broad-market and broad-strategy ETFs."""
    name_text = "" if name is None or pd.isna(name) else str(name)
    benchmark_text = "" if benchmark is None or pd.isna(benchmark) else str(benchmark)
    text = f"{name_text} {benchmark_text}".upper().replace(" ", "")
    if any(term.upper() in text for term in _SECTOR_OR_NON_CN_TERMS):
        return ""
    if any(term.upper() in text for term in _BROAD_CORE_TERMS):
        return "核心宽基"
    if any(term.upper() in text for term in _BROAD_STRATEGY_TERMS):
        return "策略宽基"
    return ""


class RotationProvider:
    def __init__(self, store: RotationStore, source: TushareSource | None = None):
        self.store = store
        self.source = source or TushareSource()

    def _tushare_call(self, endpoint: str, ttl_days: int, **params) -> pd.DataFrame:
        try:
            return self.source._call(endpoint, ttl_days, **params)
        except Exception as exc:  # Tushare SDK raises a plain Exception for permissions
            raise RotationProviderCallError(
                f"Tushare {endpoint} 调用失败：{str(exc)[:180]}"
            ) from exc

    @staticmethod
    def _eastmoney_theme_call(
        label: str,
        function: Callable[..., pd.DataFrame],
        *args,
        **params,
    ) -> pd.DataFrame:
        try:
            return akshare_call(label, function, *args, **params)
        except Exception as exc:  # AKShare/requests provider boundary
            raise ThemeSourceUnavailable(
                f"东方财富 {label} 调用失败：{str(exc)[:180]}"
            ) from exc

    def sync_market_history(self, progress, cancelled, *, rebuild: bool = False) -> dict[str, Any]:
        """Materialize three years of date-partitioned full-market stock bars."""
        from quantmaster.research.contracts import ArtifactKind, AssetClass, Frequency
        from quantmaster.research.engine import ResearchEngine
        from quantmaster.research.lake import ResearchLake

        end = date.today().isoformat()
        start = (pd.Timestamp(end) - pd.DateOffset(years=3, days=20)).date().isoformat()
        lake = ResearchLake()
        existing = lake.catalog.partitions(
            kind=ArtifactKind.RAW,
            asset_class=AssetClass.STOCK,
            frequency=Frequency.DAILY,
            dataset_id="stock_bars",
            start=start,
            end=end,
        )
        engine = ResearchEngine(lake=lake)
        plan = engine.plan(
            start,
            end,
            asset_classes=(AssetClass.STOCK,),
            datasets=("stock_bars",),
            mode="historical" if rebuild or not existing else "incremental",
        )
        if not plan.tasks:
            progress(28, "全市场日线已就绪", f"{len(existing)} 个交易日分区")
            return {"partitions": len(existing), "tasks": 0}

        def on_progress(index, total, task):
            if cancelled():
                raise InterruptedError("板块联动数据同步已取消")
            value = 3 + round(25 * index / max(1, total))
            progress(value, "同步全市场日线", f"{index}/{total} · {task.trade_date}")

        result = engine.execute(plan, cancelled=cancelled, progress=on_progress)
        return {
            "partitions": len(existing) + len(plan.tasks),
            "tasks": len(plan.tasks),
            "run_id": result["run_id"],
        }

    def sync_industry_taxonomy(self, progress, cancelled) -> dict[str, Any]:
        """Fetch L1/L2 memberships per L1 so provider row limits cannot truncate the market."""
        classes = self._tushare_call(
            "index_classify", 30, level="L1", src="SW2021",
            fields="index_code,industry_name,level",
        )
        previous = {str(item.get("code")): item for item in self.store.taxonomy_nodes()}
        strict_l1 = dict(SW2021_L1)
        nodes: dict[str, dict[str, Any]] = {}
        rows = [
            row for _, row in classes.iterrows()
            if strict_l1.get(str(row.get("index_code") or "").strip().upper())
            == str(row.get("industry_name") or "").strip()
        ]
        for index, row in enumerate(rows, start=1):
            if cancelled():
                raise InterruptedError("申万行业同步已取消")
            l1_code = str(row.get("index_code") or "").strip().upper()
            l1_name = str(row.get("industry_name") or "").strip()
            if strict_l1.get(l1_code) != l1_name:
                continue
            try:
                members = self._tushare_call(
                    "index_member_all", 30, l1_code=l1_code, is_new="Y",
                    fields="l1_code,l1_name,l2_code,l2_name,ts_code,is_new",
                )
            except RotationProviderCallError:
                logger.warning("申万行业 %s 成分同步失败，保留旧目录", l1_name, exc_info=True)
                for code, item in previous.items():
                    if code == l1_code or str(item.get("parent_code") or "") == l1_code:
                        nodes[code] = item
                continue
            l1_members: list[str] = []
            l2: dict[str, dict[str, Any]] = {}
            for _, member in members.iterrows():
                symbol = _symbol(member.get("ts_code"))
                if not symbol:
                    continue
                l1_members.append(symbol)
                l2_code = str(member.get("l2_code") or "").strip().upper()
                l2_name = str(member.get("l2_name") or "").strip()
                if l2_code and l2_name:
                    node = l2.setdefault(l2_code, {
                        "code": l2_code, "name": l2_name, "level": "L2",
                        "parent_code": l1_code, "members": [], "source": "SW2021",
                    })
                    node["members"].append(symbol)
            nodes[l1_code] = {
                "code": l1_code, "name": l1_name, "level": "L1",
                "parent_code": "", "members": sorted(set(l1_members)), "source": "SW2021",
            }
            for code, node in l2.items():
                node["members"] = sorted(set(node["members"]))
                nodes[code] = node
            progress(
                29 + round(10 * index / max(1, len(rows))),
                "同步申万层级",
                f"{index}/{len(rows)} · {l1_name}",
            )
        if nodes:
            self.store.replace_taxonomy_nodes(list(nodes.values()))
        return {
            "l1": sum(item.get("level") == "L1" for item in nodes.values()),
            "l2": sum(item.get("level") == "L2" for item in nodes.values()),
        }

    @staticmethod
    def _themes_from_source(
        previous_items: list[dict[str, Any]], source: str,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        matching = [
            item for item in previous_items
            if str(item.get("source") or "") == source
        ]
        return (
            {str(item.get("code") or ""): item for item in matching},
            {str(item.get("name") or ""): item for item in matching},
        )

    def _sync_eastmoney_themes(
        self,
        progress,
        cancelled,
        previous_items: list[dict[str, Any]],
    ) -> dict[str, Any]:  # pragma: no cover - 网络
        """Scan Eastmoney concepts without mixing a prior provider's taxonomy."""
        try:
            import akshare as ak
        except ModuleNotFoundError as exc:
            raise ThemeSourceUnavailable("AKShare 可选数据扩展未安装") from exc

        boards = self._eastmoney_theme_call(
            "stock_board_concept_name_em", ak.stock_board_concept_name_em,
        )
        if boards is None or boards.empty:
            raise ThemeSourceUnavailable("东方财富概念目录为空")
        previous_code, previous_name = self._themes_from_source(
            previous_items, _EASTMONEY_THEME_SOURCE,
        )
        previous_matching = list(previous_code.values())
        themes: dict[str, dict[str, Any]] = {}
        rows = [row for _, row in boards.iterrows()]
        fresh_count = 0
        member_failures = 0
        for index, row in enumerate(rows, start=1):
            if cancelled():
                raise InterruptedError("东方财富概念扫描已取消")
            name = str(row.get("板块名称") or row.get("名称") or "").strip()
            raw_code = str(row.get("板块代码") or row.get("代码") or "").strip().upper()
            code = raw_code or f"EMC_{hashlib.sha1(name.encode('utf-8')).hexdigest()[:10].upper()}"
            if not name:
                continue
            member_symbol = (
                raw_code
                if raw_code.startswith("BK") and raw_code[2:].isdigit()
                else name
            )
            try:
                members = self._eastmoney_theme_call(
                    f"stock_board_concept_cons_em({member_symbol})",
                    ak.stock_board_concept_cons_em,
                    symbol=member_symbol,
                )
                values = [
                    symbol for raw in members.get("代码", pd.Series(dtype=str))
                    if (symbol := _symbol(raw))
                ]
                if not values:
                    raise ThemeSourceUnavailable("概念成分为空")
                themes[code] = {
                    "code": code, "name": name, "members": sorted(set(values)),
                    "aliases": [], "source": _EASTMONEY_THEME_SOURCE,
                }
                fresh_count += 1
                member_failures = 0
            except ThemeSourceUnavailable:
                member_failures += 1
                old = previous_code.get(code) or previous_name.get(name)
                if old:
                    themes[code] = old
                logger.warning("概念 %s 成分同步失败，保留旧快照", name, exc_info=True)
                if fresh_count == 0 and member_failures >= 3:
                    break
            if index % 20 == 0 or index == len(rows):
                unprocessed = {
                    str(item.get("code") or ""): item for item in previous_matching
                    if str(item.get("code") or "") not in themes
                }
                if fresh_count:
                    self.store.replace_themes([*themes.values(), *unprocessed.values()])
                progress(
                    39 + round(16 * index / max(1, len(rows))),
                    "扫描细分题材",
                    f"东方财富 {index}/{len(rows)} · 成功 {fresh_count}",
                )
        if not fresh_count:
            raise ThemeSourceUnavailable("东方财富概念成分连续不可用")
        return {
            "catalog": len(rows),
            "available": len(themes),
            "fresh": fresh_count,
            "source": _EASTMONEY_THEME_SOURCE,
            "issues": [],
        }

    def _sync_tushare_themes(
        self,
        progress,
        cancelled,
        previous_items: list[dict[str, Any]],
    ) -> dict[str, Any]:  # pragma: no cover - 网络
        """Use the permission-gated Tushare DC catalog as the second provider."""
        end = pd.Timestamp(date.today())
        start = end - pd.Timedelta(days=14)
        # Use candidate weekdays instead of the core Tushare trade-calendar lane.  The
        # DC permission probe must not depend on or poison the 2000-point data channel.
        sessions = list(pd.bdate_range(start, end))
        if not sessions:
            sessions = [end]
        boards = pd.DataFrame()
        trade_date = ""
        for session in reversed(sessions[-7:]):
            if cancelled():
                raise InterruptedError("Tushare DC 概念扫描已取消")
            candidate = pd.Timestamp(session).strftime("%Y%m%d")
            boards = self._tushare_call(
                "dc_index",
                1,
                provider_lane="tushare:dc-concept",
                trade_date=candidate,
                idx_type="概念板块",
                fields="ts_code,trade_date,name,idx_type",
            )
            if boards is not None and not boards.empty:
                trade_date = candidate
                break
        if boards is None or boards.empty or not trade_date:
            raise ThemeSourceUnavailable("Tushare DC 概念目录为空")
        if not {"ts_code", "name"}.issubset(boards.columns):
            raise ThemeSourceUnavailable("Tushare DC 概念目录缺少代码或名称列")

        previous_code, previous_name = self._themes_from_source(
            previous_items, _TUSHARE_THEME_SOURCE,
        )
        previous_matching = list(previous_code.values())
        themes: dict[str, dict[str, Any]] = {}
        rows = [row for _, row in boards.drop_duplicates("ts_code").iterrows()]
        fresh_count = 0
        member_failures = 0
        for index, row in enumerate(rows, start=1):
            if cancelled():
                raise InterruptedError("Tushare DC 概念扫描已取消")
            code = str(row.get("ts_code") or "").strip().upper()
            name = str(row.get("name") or "").strip()
            if not code or not name:
                continue
            try:
                members = self._tushare_call(
                    "dc_member",
                    7,
                    provider_lane="tushare:dc-concept",
                    trade_date=trade_date,
                    ts_code=code,
                    fields="trade_date,ts_code,con_code,name",
                )
                values = [
                    symbol for raw in members.get("con_code", pd.Series(dtype=str))
                    if (symbol := _symbol(raw))
                ]
                if not values:
                    raise ThemeSourceUnavailable("概念成分为空")
                themes[code] = {
                    "code": code,
                    "name": name,
                    "members": sorted(set(values)),
                    "aliases": [],
                    "source": _TUSHARE_THEME_SOURCE,
                }
                fresh_count += 1
                member_failures = 0
            except (RotationProviderCallError, ThemeSourceUnavailable):
                member_failures += 1
                old = previous_code.get(code) or previous_name.get(name)
                if old:
                    themes[code] = old
                logger.warning(
                    "Tushare DC 概念 %s 成分同步失败，保留同源旧快照",
                    name,
                    exc_info=True,
                )
                if fresh_count == 0 and member_failures >= 3:
                    break
            if index % 20 == 0 or index == len(rows):
                unprocessed = {
                    str(item.get("code") or ""): item for item in previous_matching
                    if str(item.get("code") or "") not in themes
                }
                if fresh_count:
                    self.store.replace_themes([*themes.values(), *unprocessed.values()])
                progress(
                    39 + round(16 * index / max(1, len(rows))),
                    "扫描细分题材",
                    f"Tushare DC {index}/{len(rows)} · 成功 {fresh_count}",
                )
        if not fresh_count:
            raise ThemeSourceUnavailable("Tushare DC 概念成分连续不可用")
        return {
            "catalog": len(rows),
            "available": len(themes),
            "fresh": fresh_count,
            "source": _TUSHARE_THEME_SOURCE,
            "trade_date": trade_date,
            "issues": ["东方财富概念接口不可用，已自动切换为 Tushare DC 概念目录。"],
        }

    def sync_themes(self, progress, cancelled) -> dict[str, Any]:  # pragma: no cover - 网络
        """Refresh one coherent concept taxonomy, preferring Eastmoney then Tushare."""
        previous_items = self.store.themes()
        try:
            return self._sync_eastmoney_themes(
                progress, cancelled, previous_items,
            )
        except InterruptedError:
            raise
        except ThemeSourceUnavailable as eastmoney_error:
            logger.warning(
                "东方财富概念目录不可用，尝试 Tushare DC 后备源",
                exc_info=True,
            )
            try:
                return self._sync_tushare_themes(
                    progress, cancelled, previous_items,
                )
            except InterruptedError:
                raise
            except (RotationProviderCallError, ThemeSourceUnavailable) as tushare_error:
                raise RuntimeError(
                    "题材目录双源不可用："
                    f"东方财富 {str(eastmoney_error)[:100]}；"
                    f"Tushare {str(tushare_error)[:100]}"
                ) from tushare_error

    def sync_etf_observations(self, progress, cancelled) -> dict[str, Any]:
        """Fetch recent bulk fund shares and closes, preserving earlier observations."""
        previous = self.store.etf_observations()
        end = pd.Timestamp(date.today())
        start = end - pd.Timedelta(days=45 if previous.empty else 20)
        calendar = self.source.trade_calendar(str(start.date()), str(end.date()))
        bootstrap_sessions = 25 if previous.empty else 6
        dates = [
            pd.Timestamp(value) for value in calendar if pd.Timestamp(value) <= end
        ][-bootstrap_sessions:]
        basic = self.source._call(
            "fund_basic", 7, market="E", status="L",
            fields="ts_code,name,fund_type,invest_type,benchmark,list_date,market",
        )
        basic = basic.rename(columns={"ts_code": "symbol"})
        if "name" not in basic:
            basic["name"] = ""
        if "fund_type" not in basic:
            basic["fund_type"] = ""
        basic["name"] = basic["name"].astype(str)
        basic["fund_type"] = basic["fund_type"].astype(str)
        basic = basic[
            basic["name"].str.contains("ETF", case=False, na=False)
            | basic["fund_type"].str.contains("ETF|交易型", case=False, na=False)
        ].copy()
        if "benchmark" not in basic:
            basic["benchmark"] = ""
        basic["category"] = [
            _broad_etf_category(name, benchmark)
            for name, benchmark in zip(basic["name"], basic["benchmark"], strict=True)
        ]
        basic = basic[basic["category"] != ""]
        if basic.empty:
            raise RuntimeError("基金目录中没有可核查的宽基 ETF")
        names = basic.set_index("symbol")["name"].to_dict()
        categories = basic.set_index("symbol")["category"].to_dict()
        rows = []
        nav_available = True
        for index, trade_date in enumerate(dates, start=1):
            if cancelled():
                raise InterruptedError("ETF 份额同步已取消")
            compact = trade_date.strftime("%Y%m%d")
            shares = self.source._call(
                "fund_share", 1, trade_date=compact,
                fields="ts_code,trade_date,fd_share",
            ).rename(columns={"ts_code": "symbol", "fd_share": "shares"})
            daily = self.source._call(
                "fund_daily", 1, trade_date=compact,
                fields="ts_code,trade_date,close",
            ).rename(columns={"ts_code": "symbol"})
            nav = pd.DataFrame(columns=["symbol", "nav"])
            if nav_available:
                try:
                    nav = self._tushare_call(
                        "fund_nav", 1, nav_date=compact, market="E",
                        fields="ts_code,nav_date,unit_nav",
                    ).rename(columns={"ts_code": "symbol", "unit_nav": "nav"})
                    if not {"symbol", "nav"}.issubset(nav.columns):
                        nav = pd.DataFrame(columns=["symbol", "nav"])
                except RotationProviderCallError:
                    nav_available = False
                    logger.warning(
                        "场内基金单位净值接口不可用，本轮 ETF 资金改用收盘价并逐只标记",
                        exc_info=True,
                    )
            if shares.empty:
                continue
            shares = shares[shares["symbol"].isin(categories)]
            if shares.empty:
                continue
            shares["shares"] = pd.to_numeric(shares["shares"], errors="coerce") * 10_000
            merged = shares.merge(daily[["symbol", "close"]], on="symbol", how="left")
            if not nav.empty:
                nav = nav.drop_duplicates("symbol", keep="last")
                merged = merged.merge(nav[["symbol", "nav"]], on="symbol", how="left")
            else:
                merged["nav"] = pd.NA
            merged["trade_date"] = trade_date.normalize()
            merged["name"] = merged["symbol"].map(names).fillna(merged["symbol"])
            merged["category"] = merged["symbol"].map(categories)
            rows.append(merged[[
                "trade_date", "symbol", "name", "category", "shares", "nav", "close",
            ]])
            progress(
                55 + round(7 * index / max(1, len(dates))),
                "同步 ETF 份额",
                f"{index}/{len(dates)} · {trade_date.date()}",
            )
        if not rows:
            raise RuntimeError("ETF 份额接口未返回可用数据")
        result = pd.concat([previous, *rows], ignore_index=True)
        result["category"] = [
            _broad_etf_category(name)
            for name in result.get("name", pd.Series("", index=result.index))
        ]
        result = result[result["category"] != ""]
        result["trade_date"] = pd.to_datetime(result["trade_date"], errors="coerce")
        result = result.dropna(subset=["trade_date", "symbol"]).drop_duplicates(
            ["trade_date", "symbol"], keep="last",
        ).sort_values(["symbol", "trade_date"])
        self.store.save_etf_observations(result)
        return {"rows": len(result), "symbols": int(result["symbol"].nunique())}
