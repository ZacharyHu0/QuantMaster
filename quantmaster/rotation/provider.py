"""Layered online acquisition for rotation inputs.

Automatic refresh prefers Tushare's date-partitioned research data and strict SW2021
classification.  Eastmoney concepts use AKShare as a replaceable snapshot source.  A
failed source never deletes the previous catalog or ETF observations.
"""

from __future__ import annotations

import hashlib
import logging
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
        classes = self.source._call(
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
                members = self.source._call(
                    "index_member_all", 30, l1_code=l1_code, is_new="Y",
                    fields="l1_code,l1_name,l2_code,l2_name,ts_code,is_new",
                )
            except Exception:
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

    def sync_themes(self, progress, cancelled) -> dict[str, Any]:  # pragma: no cover - 网络
        """Scan the complete Eastmoney concept catalog, checkpointing partial success."""
        import akshare as ak

        boards = akshare_call("stock_board_concept_name_em", ak.stock_board_concept_name_em)
        previous_items = self.store.themes()
        previous_code = {str(item.get("code") or ""): item for item in previous_items}
        previous_name = {str(item.get("name") or ""): item for item in previous_items}
        themes: dict[str, dict[str, Any]] = {}
        rows = [row for _, row in boards.iterrows()]
        for index, row in enumerate(rows, start=1):
            if cancelled():
                raise InterruptedError("东方财富概念扫描已取消")
            name = str(row.get("板块名称") or row.get("名称") or "").strip()
            raw_code = str(row.get("板块代码") or row.get("代码") or "").strip().upper()
            code = raw_code or f"EMC_{hashlib.sha1(name.encode('utf-8')).hexdigest()[:10].upper()}"
            if not name:
                continue
            try:
                members = akshare_call(
                    f"stock_board_concept_cons_em({name})",
                    ak.stock_board_concept_cons_em,
                    symbol=name,
                )
                values = [
                    symbol for raw in members.get("代码", pd.Series(dtype=str))
                    if (symbol := _symbol(raw))
                ]
                if not values:
                    raise RuntimeError("概念成分为空")
                themes[code] = {
                    "code": code, "name": name, "members": sorted(set(values)),
                    "aliases": [], "source": "eastmoney-concept",
                }
            except Exception:
                old = previous_code.get(code) or previous_name.get(name)
                if old:
                    themes[code] = old
                logger.warning("概念 %s 成分同步失败，保留旧快照", name, exc_info=True)
            if index % 20 == 0 or index == len(rows):
                unprocessed = {
                    str(item.get("code") or ""): item for item in previous_items
                    if str(item.get("code") or "") not in themes
                }
                self.store.replace_themes([*themes.values(), *unprocessed.values()])
                progress(
                    39 + round(16 * index / max(1, len(rows))),
                    "扫描细分题材",
                    f"{index}/{len(rows)} · 成功 {len(themes)}",
                )
        return {"catalog": len(rows), "available": len(themes)}

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
                    nav = self.source._call(
                        "fund_nav", 1, nav_date=compact, market="E",
                        fields="ts_code,nav_date,unit_nav",
                    ).rename(columns={"ts_code": "symbol", "unit_nav": "nav"})
                    if not {"symbol", "nav"}.issubset(nav.columns):
                        nav = pd.DataFrame(columns=["symbol", "nav"])
                except Exception:
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
