"""ETF 研究页 V2 的纯计算内核。

模型只回答可核查的日终研究问题：板块趋势、所处位置、活跃度、一级市场
申赎证据以及状态失效条件。它刻意不生成统一总分，也不把缺失值补成零。
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.rotation.etf_models import EtfProfile

ETF_CATEGORIES = (
    "境内宽基",
    "策略",
    "行业主题",
    "海外权益",
    "债券",
    "商品",
    "货币",
)

STATE_LABELS = {
    "low_turn": "低位转强",
    "leading": "领涨共振",
    "improving": "趋势改善",
    "weakening": "走弱",
    "watch": "震荡观察",
    "not_applicable": "位置不适用",
}

_INDUSTRY_ALIASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("创新药", ("创新药", "生物药")),
    ("中药", ("中药",)),
    ("生物科技", ("疫苗", "生物科技", "生物技术")),
    ("医疗", ("医疗器械", "医疗服务", "医疗")),
    ("医药", ("医药卫生", "全指医药", "医药", "生物医药", "制药")),
    ("半导体", ("半导体", "芯片", "集成电路")),
    ("人工智能", ("人工智能", "AI产业", "AI ETF", "CHATGPT", "算力")),
    ("机器人", ("机器人", "人形机器")),
    ("信创", ("信息技术应用创新", "信息技术创新", "信创")),
    ("工业互联网", ("工业互联网",)),
    ("数字经济", ("数字经济",)),
    ("信息安全", ("信息安全", "网络安全")),
    ("物联网", ("物联网",)),
    ("计算机", ("计算机", "软件", "云计算", "大数据")),
    ("通信", ("通信设备", "通信", "电信", "5G")),
    ("新能源车", ("新能源车", "新能源汽车", "智能汽车", "智能电动汽车")),
    ("汽车零部件", ("汽车零部件", "车联网")),
    ("电池", ("电池", "锂电")),
    ("光伏", ("光伏", "太阳能")),
    ("新能源", ("新能源", "清洁能源", "绿色电力", "绿色能源", "现代能源")),
    ("电网设备", ("电网设备",)),
    ("碳中和", ("碳中和", "低碳经济")),
    ("新材料", ("新材料",)),
    ("有色金属", ("有色金属", "工业金属", "稀土", "稀有金属")),
    ("煤炭", ("煤炭",)),
    ("石油石化", ("石油天然气", "天然气", "石油石化", "油气", "石化")),
    ("商业航天", ("卫星产业", "航天航空", "通用航空", "商业航天")),
    ("军工", ("国防军工", "国防", "军工", "航空航天", "船舶")),
    ("高端制造", ("机床", "装备产业", "智能制造")),
    ("消费", ("主要消费", "可选消费", "消费")),
    ("酒", ("白酒", "酒ETF", "酒类")),
    ("食品饮料", ("食品饮料", "食品")),
    ("农业", ("农业", "畜牧", "养殖", "粮食产业", "农牧")),
    ("证券", ("证券公司", "证券", "券商")),
    ("银行", ("银行",)),
    ("金融", ("非银金融", "金融地产", "金融")),
    ("房地产", ("房地产", "地产")),
    ("基建", ("基建", "建筑材料", "建材")),
    ("机械", ("机械设备", "工程机械", "机械")),
    ("化工", ("细分化工", "化工", "化学")),
    ("钢铁", ("钢铁",)),
    ("家电", ("家用电器", "家电")),
    ("传媒", ("传媒", "游戏", "动漫")),
    ("电子", ("消费电子", "电子")),
    ("环保", ("环保", "环境治理")),
    ("物流", ("物流", "交通运输", "交运", "运输主题")),
    ("旅游", ("旅游",)),
    ("养老", ("养老",)),
    ("虚拟现实", ("虚拟现实",)),
)

_BROAD_ALIASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("沪深300", ("沪深300", "300ETF")),
    ("中证A100", ("中证A100", "A100ETF")),
    ("中证A股", ("中证A股",)),
    ("中证A500", ("中证A500", "A500")),
    ("中证1000", ("中证1000", "1000ETF")),
    ("中证2000", ("中证2000", "2000ETF")),
    ("中证500", ("中证500", "500ETF")),
    ("科创创业50", ("科创创业50",)),
    ("创业板50", ("创业板50",)),
    ("深证50", ("深证50",)),
    ("上证50", ("上证50",)),
    ("上证180", ("上证180",)),
    ("上证综合", ("上证综合", "上证综指")),
    ("科创50", ("科创50",)),
    ("科创板", ("科创板", "科创100", "科创200")),
    ("创业板", ("创业板", "创业100")),
    ("深证100", ("深证100",)),
    ("深证成份", ("深证成份",)),
    ("中小100", ("中小企业100", "中小100")),
    ("中创400", ("中创400",)),
    ("中证A50", ("中证A50", "A50")),
    ("全市场", ("中证全指", "全指", "综指")),
)

_STRATEGY_ALIASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("红利低波", ("红利低波", "红利低波动")),
    ("红利", ("红利", "股息")),
    ("低波动", ("低波", "低波动")),
    ("价值", ("价值",)),
    ("成长", ("成长",)),
    ("质量", ("质量",)),
    ("ESG", ("ESG", "可持续发展", "社会责任")),
    ("基本面", ("基本面",)),
    ("国企改革", ("国有企业改革", "国企改革")),
    ("增强策略", ("增强", "策略", "SMART")),
)

_OVERSEAS_ALIASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("港股生物科技", ("恒生生物科技",)),
    ("沪港深科技", ("沪港深互联网", "沪港深科技")),
    ("恒生科技", ("恒生科技",)),
    ("恒生指数", ("恒生指数", "恒生ETF")),
    ("港股通", ("港股通", "港股")),
    ("纳斯达克", ("纳斯达克", "纳指")),
    ("标普500", ("标普500", "标普")),
    ("日经225", ("日经225", "日经")),
    ("德国", ("德国",)),
    ("法国", ("法国",)),
    ("印度", ("印度",)),
    ("东南亚", ("东南亚",)),
    ("海外权益", ("QDII", "海外", "中概", "H股")),
)

_OVERSEAS_REGIONS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("港股", ("港股", "恒生", "H股", "沪港深")),
    ("美国", ("纳斯达克", "纳指", "标普", "美国")),
    ("日本", ("日经", "日本")),
    ("欧洲", ("德国", "法国", "欧洲")),
    ("印度", ("印度",)),
    ("东南亚", ("东南亚", "新加坡", "越南")),
    ("海外", ("QDII", "海外", "跨境", "中概")),
)

_OVERSEAS_THEMES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("科技", ("恒生科技", "纳斯达克", "纳指", "互联网", "科技", "人工智能", "芯片")),
    ("汽车", ("汽车", "新能源车")),
    ("金融", ("金融", "银行", "证券")),
    ("红利", ("红利", "股息", "低波")),
    ("医药", ("医药", "医疗", "生物科技", "创新药")),
    ("消费", ("消费", "食品", "酒")),
)

_BOND_ALIASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("国债", ("国债",)),
    ("政金债", ("政金债", "政策性金融债", "国开")),
    ("信用债", ("信用债", "公司债", "企业债")),
    ("可转债", ("可转债", "转债")),
    ("短融", ("短融", "短债")),
    ("债券", ("债券", "债ETF", "债")),
)

_COMMODITY_ALIASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("黄金", ("黄金", "金ETF")),
    ("原油", ("原油", "石油基金")),
    ("豆粕", ("豆粕",)),
    ("能源化工", ("能源化工",)),
    ("商品", ("商品期货", "商品ETF", "期货ETF")),
)


def _text(*values: Any) -> str:
    return " ".join(str(value or "") for value in values).upper()


def _match_alias(
    text: str,
    aliases: Sequence[tuple[str, Sequence[str]]],
) -> tuple[str, tuple[str, ...]] | None:
    for canonical, tokens in aliases:
        matched = tuple(token for token in tokens if token.upper() in text)
        if matched:
            return canonical, matched[:3]
    return None


def normalize_index_name(value: str) -> str:
    """Normalize an index label without erasing its economically meaningful theme."""

    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"[（(](?:全收益|净收益|价格|收益率|收益率公式|计算公式)[）)]", "", text)
    text = re.sub(r"(?:收益率)?(?:计算)?公式.*$", "", text)
    text = re.sub(r"(?:×|X|\*)?\s*100\s*%.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"收益率$", "", text)
    text = re.sub(r"=.*$", "", text)
    bilingual = re.fullmatch(r"\s*(.*?)\s*[（(]\s*([^（）()]+?)\s*[）)]\s*", text)
    if bilingual:
        outer, inner = bilingual.groups()
        outer_has_chinese = bool(re.search(r"[\u4e00-\u9fff]", outer))
        inner_has_chinese = bool(re.search(r"[\u4e00-\u9fff]", inner))
        if outer_has_chinese != inner_has_chinese:
            # 数据源会把同一指数写成“英文(中文)”或“中文(英文)”。研究页统一
            # 保留中文可读名称，避免一个经济指数被拆成多个板块。
            text = outer if outer_has_chinese else inner
    text = re.sub(r"(?:全收益|净收益|价格)?指数(?:收益率)?$", "", text)
    text = re.sub(r"收益率.*$", "", text)
    text = re.sub(r"\s+", "", text)
    return text.strip("-·：:，,")


def infer_index_name_from_product(value: str) -> str:
    """Recover the disclosed index phrase from a fund name when metadata is unavailable."""

    text = re.sub(r"\s+", "", str(value or ""))
    match = re.search(
        r"((?:中证|国证|上证|深证|创业板|科创创业|恒生|沪港深|纳斯达克|标普|日经|"
        r"中创|粤港澳|战略新兴|长三角|杭州湾|成渝|G60)"
        r"[A-Z0-9\u4e00-\u9fff]+?)(?=ETF)",
        text,
        flags=re.IGNORECASE,
    )
    return normalize_index_name(match.group(1)) if match else ""


def classify_etf_profile(
    name: str,
    *,
    benchmark: str = "",
    benchmark_code: str = "",
    index_name: str = "",
    fund_type: str = "",
    invest_type: str = "",
    etf_type: str = "",
    benchmark_type: str = "",
    index_type: str = "",
    metadata_source: str = "fund_basic",
) -> dict[str, Any]:
    """Return two-level ETF taxonomy with industry rules ahead of broad-index tokens."""

    text = _text(name, index_name, benchmark, fund_type, invest_type, etf_type)
    industry = _match_alias(text, _INDUSTRY_ALIASES)
    broad = _match_alias(text, _BROAD_ALIASES)
    strategy = _match_alias(text, _STRATEGY_ALIASES)
    overseas = _match_alias(text, _OVERSEAS_ALIASES)
    overseas_region = _match_alias(text, _OVERSEAS_REGIONS)
    overseas_theme = _match_alias(text, _OVERSEAS_THEMES)
    bond = _match_alias(text, _BOND_ALIASES)
    commodity = _match_alias(text, _COMMODITY_ALIASES)
    official_kind = _text(benchmark_type, index_type)
    official_evidence = (f"官方基准分类：{benchmark_type or index_type}",)
    inferred_index = infer_index_name_from_product(name)
    fallback_sector = (
        normalize_index_name(index_name)
        or normalize_index_name(benchmark)
        or inferred_index
    )

    if any(token in text for token in ("货币", "现金", "添益", "保证金")):
        category, asset_class, sector, evidence = "货币", "money", "货币", ("货币型关键词",)
    elif bond:
        category, asset_class, sector, evidence = "债券", "bond", bond[0], bond[1]
    elif overseas_region or overseas or "跨境" in official_kind:
        region = overseas_region[0] if overseas_region else "海外"
        if overseas_theme:
            sector = f"{region}{overseas_theme[0]}"
            evidence = (
                tuple((*overseas_region[1], *overseas_theme[1]))[:3]
                if overseas_region
                else overseas_theme[1]
            )
        else:
            sector = overseas[0] if overseas else region
            evidence = overseas[1] if overseas else (f"境外地域：{region}",)
        category, asset_class = "海外权益", "overseas_equity"
    elif commodity and not industry:
        category, asset_class, sector, evidence = "商品", "commodity", commodity[0], commodity[1]
    elif strategy or "策略" in official_kind:
        sector = strategy[0] if strategy else (fallback_sector or "策略指数")
        evidence = strategy[1] if strategy else official_evidence
        category, asset_class = "策略", "equity"
    elif industry or "行业主题" in official_kind:
        # “中证全指医药”等名称必须先命中行业，不能被“全指”吞掉。
        sector = industry[0] if industry else (fallback_sector or "其他主题")
        evidence = industry[1] if industry else official_evidence
        category, asset_class = "行业主题", "equity"
    elif broad or "宽基" in official_kind:
        sector = broad[0] if broad else (fallback_sector or "境内宽基")
        evidence = broad[1] if broad else official_evidence
        category, asset_class = "境内宽基", "equity"
    else:
        category, asset_class = "行业主题", "equity"
        sector = fallback_sector or "其他主题"
        evidence = (f"规范化指数：{fallback_sector}",) if fallback_sector else ("ETF 名称规则",)

    official = metadata_source == "etf_basic" and bool(index_name or benchmark_code)
    official_benchmark = official and bool(benchmark_type or index_type)
    normalized = (
        normalize_index_name(index_name)
        or normalize_index_name(benchmark)
        or inferred_index
        or sector
    )
    index_key = str(benchmark_code or normalized or sector).upper()
    sector_id = "etf-sector-" + hashlib.sha1(f"{asset_class}|{sector}".encode()).hexdigest()[:12]
    return {
        "category": category,
        "asset_class": asset_class,
        "sector_id": sector_id,
        "sector_name": sector,
        "normalized_index": normalized,
        "index_key": index_key,
        "classification_source": (
            "tushare:etf_basic+mkt_idx_bmk+quantmaster-rules"
            if official_benchmark
            else ("tushare:etf_basic+quantmaster-rules" if official else "quantmaster:explicit-rules")
        ),
        "classification_confidence": 1.0 if official else (0.75 if benchmark else 0.6),
        "classification_evidence": tuple(evidence),
    }


def adjusted_daily_metrics(
    frame: pd.DataFrame,
    factors: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Calculate returns from verified adjustment evidence and never guess long position."""

    if frame is None or frame.empty:
        return {"sessions": 0, "adjustment_status": "missing", "history": []}
    values = frame.copy()
    values["date"] = pd.to_datetime(values.get("date"), errors="coerce")
    values = values.dropna(subset=["date"]).sort_values("date").drop_duplicates("date", keep="last")
    raw_close = pd.to_numeric(values.get("close"), errors="coerce")
    amount = pd.to_numeric(values.get("amount"), errors="coerce")
    research = pd.Series(np.nan, index=values.index, dtype=float)
    adjustment_status = "unavailable"
    adjustment_source = ""
    factor_coverage = 0.0

    factor_frame = factors.copy() if factors is not None and not factors.empty else pd.DataFrame()
    if factor_frame.empty and "adj_factor" in values:
        factor_frame = values[["date", "adj_factor"]].copy()
    if not factor_frame.empty:
        factor_frame["date"] = pd.to_datetime(factor_frame.get("date"), errors="coerce")
        factor_frame["adj_factor"] = pd.to_numeric(factor_frame.get("adj_factor"), errors="coerce")
        factor_frame = factor_frame.dropna(subset=["date", "adj_factor"]).drop_duplicates("date", keep="last")
        aligned = values[["date"]].merge(factor_frame, on="date", how="left")["adj_factor"]
        factor_coverage = float(aligned.notna().mean()) if len(aligned) else 0.0
        if factor_coverage >= 0.95 and aligned.notna().any():
            aligned.index = values.index
            latest_factor = float(aligned.dropna().iloc[-1])
            if latest_factor > 0:
                research = raw_close * aligned / latest_factor
                sources = {
                    str(value)
                    for value in factor_frame.get("source", pd.Series(dtype=str)).dropna()
                    if str(value)
                }
                if sources and all(value.startswith("tushare:fund_adj") for value in sources):
                    adjustment_status = "official"
                    adjustment_source = "tushare:fund_adj"
                else:
                    adjustment_status = "verified_local"
                    adjustment_source = ", ".join(sorted(sources)) or "verified:adjustment-factor"

    verified_adjustment = adjustment_status in {"official", "verified_local"}
    if not verified_adjustment and "pct_chg" in values:
        pct = pd.to_numeric(values.get("pct_chg"), errors="coerce") / 100.0
        valid_pct = pct.notna() & np.isfinite(pct) & pct.gt(-1.0)
        invalid_positions = np.flatnonzero(~valid_pct.to_numpy())
        suffix_start = int(invalid_positions[-1] + 1) if len(invalid_positions) else 0
        pct_suffix = pct.iloc[suffix_start:]
        raw_tail = raw_close.tail(65)
        raw_tail_returns = raw_tail.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
        raw_short_safe = bool(
            len(raw_tail) >= 61
            and raw_tail.notna().all()
            and raw_tail.gt(0).all()
            and len(raw_tail_returns) >= 60
            and raw_tail_returns.abs().max() <= 0.35
        )
        if len(pct_suffix) < 61 and raw_short_safe:
            research.loc[raw_tail.index] = raw_tail
            adjustment_status = "raw_short_fallback"
            adjustment_source = "stockdb:raw_close_continuity_guard"
        elif len(pct_suffix) >= 2 and raw_close.notna().any():
            chained = (1.0 + pct_suffix).cumprod()
            chained = chained / chained.iloc[-1] * float(raw_close.dropna().iloc[-1])
            research.loc[pct_suffix.index] = chained
            adjustment_status = "return_chain"
            adjustment_source = "tushare:pct_chg"

    research = pd.to_numeric(research, errors="coerce")
    valid = pd.DataFrame(
        {"date": values["date"], "price": research, "amount": amount, "raw_close": raw_close}
    ).dropna(subset=["price"])
    prices = valid["price"].reset_index(drop=True)
    returns = prices.pct_change().replace([np.inf, -np.inf], np.nan)

    def period_return(sessions: int) -> float | None:
        if len(prices) < sessions + 1 or float(prices.iloc[-sessions - 1]) == 0:
            return None
        return float(prices.iloc[-1] / prices.iloc[-sessions - 1] - 1)

    def moving_average(sessions: int) -> float | None:
        if len(prices) < sessions:
            return None
        return float(prices.tail(sessions).mean())

    def position(sessions: int, *, long_term: bool = False) -> float | None:
        if len(prices) < sessions or (
            long_term and adjustment_status not in {"official", "verified_local"}
        ):
            return None
        window = prices.tail(sessions)
        low, high = float(window.min()), float(window.max())
        if high <= low:
            return None
        return float((prices.iloc[-1] - low) / (high - low) * 100)

    ma20, ma60 = moving_average(20), moving_average(60)
    ma20_slope = None
    if len(prices) >= 25:
        prior_ma20 = float(prices.iloc[-25:-5].mean())
        if prior_ma20:
            ma20_slope = float(ma20 / prior_ma20 - 1) if ma20 is not None else None
    latest5 = amount.tail(5).mean() if amount.notna().sum() >= 5 else np.nan
    previous20 = amount.iloc[-25:-5].mean() if amount.notna().sum() >= 25 else np.nan
    amount_ratio = (
        float(latest5 / previous20)
        if pd.notna(latest5) and pd.notna(previous20) and float(previous20) > 0
        else None
    )
    avg_amount20 = float(amount.tail(20).mean()) if amount.notna().any() else None
    position250 = position(250, long_term=True)
    drawdown250 = None
    if position250 is not None:
        high250 = float(prices.tail(250).max())
        drawdown250 = float(prices.iloc[-1] / high250 - 1) if high250 else None

    history = [
        {
            "date": row.date.date().isoformat(),
            "price": float(row.price),
            "amount": float(row.amount) if pd.notna(row.amount) else None,
        }
        for row in valid.tail(260).itertuples(index=False)
    ]
    latest_raw = raw_close.dropna()
    return {
        "sessions": len(prices),
        "close": float(latest_raw.iloc[-1]) if not latest_raw.empty else None,
        "research_price": float(prices.iloc[-1]) if len(prices) else None,
        "return_5d": period_return(5),
        "return_20d": period_return(20),
        "return_60d": period_return(60),
        "ma20": ma20,
        "ma60": ma60,
        "ma20_slope": ma20_slope,
        "above_ma20": bool(ma20 is not None and len(prices) and prices.iloc[-1] > ma20),
        "ma20_above_ma60": bool(ma20 is not None and ma60 is not None and ma20 > ma60),
        "position_20d": position(20),
        "position_60d": position(60),
        "position_250d": position250,
        "drawdown_250d": drawdown250,
        "avg_amount_20d": avg_amount20,
        "amount_ratio_5v20": amount_ratio,
        "volatility_20d": float(returns.tail(20).std()) if returns.notna().any() else None,
        "adjustment_status": adjustment_status,
        "adjustment_source": adjustment_source,
        "adjustment_coverage": round(factor_coverage, 6),
        "history": history,
    }


def fund_evidence(
    observations: pd.DataFrame,
    *,
    as_of_date: str,
    session_dates: Sequence[str],
    fallback_price: float | None,
) -> dict[str, Any]:
    """Describe primary-market share changes without mistaking gaps for daily evidence."""

    empty = {
        "status": "missing",
        "effective_date": "",
        "lag_sessions": None,
        "source": "",
        "share": None,
        "share_delta": None,
        "share_change_pct": None,
        "estimated_flow": None,
        "unchanged_sessions": None,
        "period_kind": "unavailable",
        "period_sessions": None,
        "period_label": "",
        "consecutive": False,
        "message": "— · 未覆盖连续份额快照",
    }
    if observations is None or observations.empty:
        return empty
    frame = observations.copy()
    frame["trade_date"] = pd.to_datetime(frame.get("trade_date"), errors="coerce")
    frame["shares"] = pd.to_numeric(frame.get("shares"), errors="coerce")
    frame = frame.dropna(subset=["trade_date", "shares"]).sort_values("trade_date")
    frame = frame[frame["trade_date"].dt.date <= pd.Timestamp(as_of_date).date()]
    frame = frame.drop_duplicates("trade_date", keep="last")
    if frame.empty:
        return empty
    latest_date = frame.iloc[-1]["trade_date"].date().isoformat()
    sessions = sorted(
        {
            pd.Timestamp(value).date().isoformat()
            for value in session_dates
            if pd.notna(pd.to_datetime(value, errors="coerce"))
        }
    )
    session_index = {value: index for index, value in enumerate(sessions)}
    lag = (
        session_index[as_of_date] - session_index[latest_date]
        if (as_of_date in session_index and latest_date in session_index)
        else max(0, (pd.Timestamp(as_of_date) - pd.Timestamp(latest_date)).days)
    )
    source = str(frame.iloc[-1].get("share_source") or frame.iloc[-1].get("source") or "tushare:fund_share")
    latest_share = float(frame.iloc[-1]["shares"])
    if latest_date != as_of_date:
        return {
            **empty,
            "status": "stale",
            "effective_date": latest_date,
            "lag_sessions": int(lag),
            "source": source,
            "share": latest_share,
            "message": f"— · 份额仅截至 {latest_date}",
        }
    if len(frame) < 2:
        return {
            **empty,
            "status": "missing",
            "effective_date": latest_date,
            "lag_sessions": 0,
            "source": source,
            "share": latest_share,
            "message": "— · 缺少上一交易日份额",
        }
    prior_share = float(frame.iloc[-2]["shares"])
    prior_date = frame.iloc[-2]["trade_date"].date().isoformat()
    if prior_date in session_index and latest_date in session_index:
        period_sessions = session_index[latest_date] - session_index[prior_date]
    else:
        period_sessions = max(1, int(np.busday_count(prior_date, latest_date)))
    if period_sessions <= 0:
        return {
            **empty,
            "status": "missing",
            "effective_date": latest_date,
            "lag_sessions": 0,
            "source": source,
            "share": latest_share,
            "message": "— · 上一份额快照日期无效",
        }
    consecutive = period_sessions == 1
    period_kind = "daily" if consecutive else "interval"
    period_label = "当日变化" if consecutive else f"近 {period_sessions} 个交易日累计变化"
    delta = latest_share - prior_share
    change_pct = delta / prior_share if prior_share else None
    price = pd.to_numeric(
        pd.Series([frame.iloc[-1].get("nav"), frame.iloc[-1].get("close"), fallback_price]),
        errors="coerce",
    ).dropna()
    estimated_flow = float(delta * price.iloc[0]) if not price.empty else None
    unchanged = 0
    records = frame[["trade_date", "shares"]].to_dict("records")
    for index in range(len(records) - 1, 0, -1):
        current_date = records[index]["trade_date"].date().isoformat()
        previous_date = records[index - 1]["trade_date"].date().isoformat()
        if (
            current_date not in session_index
            or previous_date not in session_index
            or session_index[current_date] - session_index[previous_date] != 1
            or not np.isclose(
                records[index]["shares"],
                records[index - 1]["shares"],
                rtol=1e-10,
                atol=1e-6,
            )
        ):
            break
        unchanged += 1
    zero = bool(np.isclose(delta, 0.0, rtol=1e-10, atol=1e-6))
    return {
        "status": "confirmed_zero" if zero else "confirmed_change",
        "effective_date": latest_date,
        "lag_sessions": 0,
        "source": source,
        "share": latest_share,
        "prior_share": prior_share,
        "share_delta": 0.0 if zero else float(delta),
        "share_change_pct": 0.0 if zero else (float(change_pct) if change_pct is not None else None),
        "estimated_flow": 0.0 if zero and estimated_flow is not None else estimated_flow,
        "unchanged_sessions": unchanged,
        "period_kind": period_kind,
        "period_sessions": period_sessions,
        "period_label": period_label,
        "consecutive": consecutive,
        "message": (
            "0份（0.00%）· 已确认当日无净申赎"
            if zero and consecutive
            else (
                f"0份（0.00%）· {period_label}；期间快照不完整"
                if zero
                else (
                    "已确认当日发生净申购/赎回"
                    if consecutive
                    else f"{period_label}；不可解释为当日申赎"
                )
            )
        ),
    }


def _median(rows: Iterable[Mapping[str, Any]], key: str) -> float | None:
    values = pd.to_numeric(pd.Series([row.get(key) for row in rows]), errors="coerce").dropna()
    return float(values.median()) if not values.empty else None


def _percentile(value: float | None, peers: Sequence[float | None]) -> float | None:
    if value is None or not np.isfinite(value):
        return None
    valid = np.asarray([item for item in peers if item is not None and np.isfinite(item)], dtype=float)
    if len(valid) == 0:
        return None
    below = float((valid < value).sum())
    equal = float(np.isclose(valid, value).sum())
    return (below + 0.5 * equal) / len(valid) * 100


def _absolute_score(value: float | None, low: float, high: float) -> float | None:
    if value is None or not np.isfinite(value):
        return None
    return float(np.clip((value - low) / (high - low) * 100, 0, 100))


def _weighted(parts: Sequence[tuple[float, float | None]]) -> float | None:
    if any(value is None for _, value in parts):
        return None
    return float(sum(weight * float(value) for weight, value in parts))


def _representative_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    metrics = row["metrics"]
    funds = row["funds"]
    completeness = (
        sum(
            value is not None
            for value in (
                metrics.get("return_5d"),
                metrics.get("return_20d"),
                metrics.get("return_60d"),
                metrics.get("avg_amount_20d"),
            )
        )
        + int(metrics.get("adjustment_status") == "official")
        + int(funds.get("status") in {"confirmed_zero", "confirmed_change"})
    )
    size = row.get("total_size") or funds.get("share") or 0
    return (
        completeness,
        float(metrics.get("avg_amount_20d") or 0),
        float(size or 0),
        "".join(chr(0x10FFFF - ord(char)) for char in row["profile"].symbol),
    )


def _sector_history(representatives: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    series: list[pd.DataFrame] = []
    for index, row in enumerate(representatives):
        history = pd.DataFrame(row["metrics"].get("history") or [])
        if history.empty:
            continue
        history["date"] = pd.to_datetime(history["date"], errors="coerce")
        history["price"] = pd.to_numeric(history["price"], errors="coerce")
        history["amount"] = pd.to_numeric(history.get("amount"), errors="coerce")
        history = history.dropna(subset=["date", "price"])
        if history.empty or float(history["price"].iloc[0]) == 0:
            continue
        history[f"price_{index}"] = history["price"] / float(history["price"].iloc[0]) * 100
        history[f"amount_{index}"] = history["amount"]
        series.append(history[["date", f"price_{index}", f"amount_{index}"]])
    if not series:
        return []
    merged = series[0]
    for frame in series[1:]:
        merged = merged.merge(frame, on="date", how="outer")
    price_columns = [column for column in merged if column.startswith("price_")]
    amount_columns = [column for column in merged if column.startswith("amount_")]
    merged["price"] = merged[price_columns].median(axis=1, skipna=True)
    merged["amount"] = merged[amount_columns].sum(axis=1, min_count=1)
    return [
        {
            "date": row.date.date().isoformat(),
            "price": round(float(row.price), 6),
            "amount": float(row.amount) if pd.notna(row.amount) else None,
        }
        for row in merged.dropna(subset=["date", "price"]).sort_values("date").tail(260).itertuples()
    ]


def _candidate_assessment(
    label: str,
    conditions: Sequence[tuple[bool, str]],
    confirmation_conditions: Sequence[tuple[bool, str]] = (),
) -> dict[str, Any]:
    eligible = all(passed for passed, _ in conditions)
    evaluated = (*conditions, *confirmation_conditions)
    met = [description for passed, description in evaluated if passed]
    unmet = [description for passed, description in evaluated if not passed]
    return {
        "label": label,
        "eligible": eligible,
        "met_conditions": met,
        "unmet_conditions": unmet,
    }


def build_sector_research(
    rows: Sequence[dict[str, Any]],
) -> tuple[
    list[dict[str, Any]],
    dict[str, str],
    dict[str, tuple[str, ...]],
    dict[str, tuple[str, ...]],
    tuple[dict[str, Any], ...],
]:
    """Aggregate normalized-index representatives into sectors and apply public state gates."""

    by_index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        profile: EtfProfile = row["profile"]
        index_key = profile.benchmark_code or profile.normalized_index or profile.sector_name
        by_index[f"{profile.asset_class}|{profile.sector_id}|{index_key}"].append(row)
    index_representatives = {key: max(items, key=_representative_key) for key, items in by_index.items()}
    representative_by_symbol: dict[str, str] = {}
    for key, members in by_index.items():
        representative = index_representatives[key]["profile"].symbol
        representative_by_symbol.update({row["profile"].symbol: representative for row in members})

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["profile"].sector_id].append(row)
    sectors: list[dict[str, Any]] = []
    metric_keys = (
        "return_5d",
        "return_20d",
        "return_60d",
        "ma20_slope",
        "position_20d",
        "position_60d",
        "position_250d",
        "drawdown_250d",
        "avg_amount_20d",
        "amount_ratio_5v20",
        "volatility_20d",
    )
    for sector_id, members in grouped.items():
        profile = members[0]["profile"]
        reps = [row for row in index_representatives.values() if row["profile"].sector_id == sector_id]
        if not reps:
            reps = [max(members, key=_representative_key)]
        representative = max(reps, key=_representative_key)
        metrics = {key: _median((row["metrics"] for row in reps), key) for key in metric_keys}
        metrics["above_ma20"] = bool(
            reps and sum(bool(row["metrics"].get("above_ma20")) for row in reps) >= len(reps) / 2
        )
        metrics["ma20_above_ma60"] = bool(
            reps and sum(bool(row["metrics"].get("ma20_above_ma60")) for row in reps) >= len(reps) / 2
        )
        confirmed = [
            row["funds"]
            for row in members
            if row["funds"].get("status") in {"confirmed_zero", "confirmed_change"}
        ]
        stale = [row["funds"] for row in members if row["funds"].get("status") == "stale"]
        dated_evidence = confirmed or stale
        prior_shares = [
            item.get("prior_share") for item in confirmed if item.get("prior_share") is not None
        ]
        share_delta = sum(float(item.get("share_delta") or 0) for item in confirmed) if confirmed else None
        prior_total = sum(float(value) for value in prior_shares) if prior_shares else None
        estimated = [item.get("estimated_flow") for item in confirmed]
        estimated_flow = (
            sum(float(value) for value in estimated if value is not None)
            if any(value is not None for value in estimated)
            else None
        )
        fund_coverage = len(confirmed) / len(members) if members else 0.0
        coverage_level = "high" if fund_coverage >= 0.8 else ("medium" if fund_coverage >= 0.5 else "low")
        periods = [
            int(item.get("period_sessions") or 0)
            for item in confirmed
            if item.get("period_sessions") is not None
        ]
        all_daily = (
            bool(confirmed)
            and len(periods) == len(confirmed)
            and all(value == 1 for value in periods)
        )
        period_sessions = max(periods, default=None)
        period_label = (
            "当日变化"
            if all_daily
            else (f"近 {period_sessions} 个交易日累计变化" if period_sessions else "变化区间待核")
        )
        unchanged_values = [
            int(item.get("unchanged_sessions") or 0)
            for item in confirmed
            if item.get("unchanged_sessions") is not None
        ]
        funds = {
            "status": "confirmed" if confirmed else ("stale" if stale else "missing"),
            "effective_date": max((item.get("effective_date") or "" for item in dated_evidence), default=""),
            "coverage": fund_coverage,
            "coverage_level": coverage_level,
            "confirmed_members": len(confirmed),
            "member_count": len(members),
            "share_delta": share_delta,
            "share_change_pct": share_delta / prior_total
            if share_delta is not None and prior_total
            else None,
            "estimated_flow": estimated_flow,
            "unchanged_sessions": min(unchanged_values, default=None),
            "period_kind": "daily" if all_daily else ("interval" if confirmed else "unavailable"),
            "period_sessions": period_sessions,
            "period_label": period_label,
            "consecutive": all_daily,
            "directional_interpretation": bool(fund_coverage >= 0.5 and all_daily),
            "source": ", ".join(
                sorted({str(item.get("source") or "") for item in dated_evidence if item.get("source")})
            ),
        }
        sectors.append(
            {
                "sector_id": sector_id,
                "sector_name": profile.sector_name,
                "category": profile.category,
                "asset_class": profile.asset_class,
                "representative": {
                    "symbol": representative["profile"].symbol,
                    "name": representative["profile"].name,
                    "normalized_index": representative["profile"].normalized_index,
                },
                "member_count": len(members),
                "index_count": len(reps),
                "metrics": metrics,
                "funds": funds,
                "history": _sector_history(reps),
                "classification_confidence": round(
                    float(np.mean([row["profile"].classification_confidence for row in members])), 4
                ),
                "adjustment_coverage": round(
                    sum(row["metrics"].get("adjustment_status") == "official" for row in reps)
                    / max(1, len(reps)),
                    4,
                ),
            }
        )

    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for sector in sectors:
        by_asset[sector["asset_class"]].append(sector)
    for asset_sectors in by_asset.values():
        peer_count = len(asset_sectors)
        asset_class = str(asset_sectors[0].get("asset_class") or "") if asset_sectors else ""
        long_covered = sum(item["metrics"].get("position_250d") is not None for item in asset_sectors)
        long_coverage = long_covered / peer_count if peer_count else 0.0
        use_long_position = asset_class != "money" and long_coverage >= 0.8
        position_metric = (
            "" if asset_class == "money" else ("position_250d" if use_long_position else "position_60d")
        )
        position_horizon = 0 if asset_class == "money" else (250 if use_long_position else 60)
        position_label = (
            "位置不适用"
            if asset_class == "money"
            else ("250 日复权位置" if use_long_position else "60 日阶段位置")
        )
        for sector in asset_sectors:
            metric = sector["metrics"]
            if peer_count >= 8:
                p5 = _percentile(
                    metric["return_5d"], [item["metrics"]["return_5d"] for item in asset_sectors]
                )
                p20 = _percentile(
                    metric["return_20d"], [item["metrics"]["return_20d"] for item in asset_sectors]
                )
                p60 = _percentile(
                    metric["return_60d"], [item["metrics"]["return_60d"] for item in asset_sectors]
                )
                p_ratio = _percentile(
                    metric["amount_ratio_5v20"],
                    [item["metrics"]["amount_ratio_5v20"] for item in asset_sectors],
                )
                log_amount = (
                    np.log1p(metric["avg_amount_20d"]) if metric["avg_amount_20d"] is not None else None
                )
                peer_logs = [
                    np.log1p(item["metrics"]["avg_amount_20d"])
                    if item["metrics"]["avg_amount_20d"] is not None
                    else None
                    for item in asset_sectors
                ]
                p_amount = _percentile(log_amount, peer_logs)
            else:
                p5 = _absolute_score(metric["return_5d"], -0.05, 0.05)
                p20 = _absolute_score(metric["return_20d"], -0.12, 0.12)
                p60 = _absolute_score(metric["return_60d"], -0.25, 0.25)
                p_ratio = _absolute_score(metric["amount_ratio_5v20"], 0.5, 1.5)
                log_amount = (
                    np.log1p(metric["avg_amount_20d"]) if metric["avg_amount_20d"] is not None else None
                )
                p_amount = _absolute_score(log_amount, np.log1p(1_000_000), np.log1p(1_000_000_000))
            trend = _weighted(((0.25, p5), (0.45, p20), (0.30, p60)))
            activity = _weighted(((0.70, p_ratio), (0.30, p_amount)))
            position_peer = _percentile(
                metric["position_250d"], [item["metrics"]["position_250d"] for item in asset_sectors]
            )
            display_position_peer = _percentile(
                metric.get(position_metric),
                [item["metrics"].get(position_metric) for item in asset_sectors],
            )
            volatility_peer = _percentile(
                metric["volatility_20d"], [item["metrics"]["volatility_20d"] for item in asset_sectors]
            )
            sector["trend_strength"] = round(trend, 2) if trend is not None else None
            sector["activity_score"] = round(activity, 2) if activity is not None else None
            sector["position_peer_percentile"] = (
                round(position_peer, 2) if position_peer is not None else None
            )
            sector["display_position"] = metric.get(position_metric)
            sector["position_source"] = (
                "official_adjusted" if use_long_position else "stage_research_series"
            )
            sector["position_metric"] = position_metric
            sector["position_horizon"] = position_horizon
            sector["position_label"] = position_label
            sector["position_coverage"] = round(
                (
                    sum(item["metrics"].get(position_metric) is not None for item in asset_sectors)
                    / peer_count
                )
                if peer_count
                else 0.0,
                4,
            )
            sector["long_position_coverage"] = round(long_coverage, 4)
            sector["display_position_peer_percentile"] = (
                round(display_position_peer, 2) if display_position_peer is not None else None
            )
            sector["volatility_peer_percentile"] = (
                round(volatility_peer, 2) if volatility_peer is not None else None
            )
            sector["confidence"] = {
                "level": "high"
                if peer_count >= 8 and sector["adjustment_coverage"] >= 0.8
                else ("medium" if peer_count >= 8 else "low"),
                "peer_count": peer_count,
                "relative_percentiles": peer_count >= 8,
            }

    for sector in sectors:
        metric = sector["metrics"]
        trend, activity = sector["trend_strength"], sector["activity_score"]
        position, position_peer = metric["position_250d"], sector["position_peer_percentile"]
        ratio = metric["amount_ratio_5v20"]
        relative = bool(sector["confidence"]["relative_percentiles"])
        if sector["asset_class"] == "money":
            state = "not_applicable"
        elif (
            position is not None
            and position <= 40
            and (not relative or (position_peer is not None and position_peer <= 40))
            and metric["return_5d"] is not None
            and metric["return_5d"] > 0
            and metric["above_ma20"]
            and metric["ma20_slope"] is not None
            and metric["ma20_slope"] > 0
            and trend is not None
            and trend >= 60
            and ratio is not None
            and ratio >= 1.10
        ):
            state = "low_turn"
        elif (
            metric["return_20d"] is not None
            and metric["return_20d"] > 0
            and metric["return_60d"] is not None
            and metric["return_60d"] > 0
            and metric["above_ma20"]
            and metric["ma20_above_ma60"]
            and trend is not None
            and trend >= 70
            and activity is not None
            and activity >= 60
            and ratio is not None
            and ratio >= 1.10
        ):
            state = "leading"
        elif (
            metric["return_5d"] is not None
            and metric["return_5d"] > 0
            and metric["above_ma20"]
            and metric["ma20_slope"] is not None
            and metric["ma20_slope"] > 0
            and trend is not None
            and trend >= 55
        ):
            state = "improving"
        elif (
            metric["return_20d"] is not None
            and metric["return_20d"] < 0
            and not metric["above_ma20"]
            and metric["ma20_slope"] is not None
            and metric["ma20_slope"] < 0
            and trend is not None
            and trend <= 40
        ):
            state = "weakening"
        else:
            state = "watch"
        position60 = metric.get("position_60d")
        r5 = metric.get("return_5d")
        r20 = metric.get("return_20d")
        candidates = {
            "momentum_hot": _candidate_assessment(
                "动量热门候选",
                (
                    (state != "leading", "尚未满足严格领涨确认"),
                    (trend is not None and trend >= 70, "趋势强度不低于 70"),
                    (activity is not None and activity >= 60, "活跃度不低于 60"),
                    (r5 is not None and r5 > 0, "5 日收益为正"),
                    (r20 is not None and r20 > 0, "20 日收益为正"),
                ),
                (
                    (metric.get("return_60d") is not None and metric["return_60d"] > 0, "60 日收益为正"),
                    (bool(metric.get("above_ma20")), "价格站上 MA20"),
                    (bool(metric.get("ma20_above_ma60")), "MA20 高于 MA60"),
                    (ratio is not None and ratio >= 1.10, "量能比不低于 1.10"),
                ),
            ),
            "stage_low_rebound": _candidate_assessment(
                "阶段低位转强候选",
                (
                    (sector["asset_class"] != "money", "资产类别可评估位置"),
                    (position60 is not None and position60 <= 40, "60 日阶段位置不高于 40"),
                    (r5 is not None and r5 > 0, "5 日收益为正"),
                    (bool(metric.get("above_ma20")), "价格站上 MA20"),
                    (ratio is not None and ratio >= 1.10, "量能比不低于 1.10"),
                ),
                (
                    (position is not None and position <= 40, "250 日复权位置不高于 40"),
                    (
                        not relative or (position_peer is not None and position_peer <= 40),
                        "长期位置处于同类后 40%",
                    ),
                    (metric.get("ma20_slope") is not None and metric["ma20_slope"] > 0, "MA20 斜率转正"),
                    (trend is not None and trend >= 60, "趋势强度不低于 60"),
                ),
            ),
            "stage_high_activity": _candidate_assessment(
                "阶段高位活跃候选",
                (
                    (sector["asset_class"] != "money", "资产类别可评估位置"),
                    (position60 is not None and position60 >= 85, "60 日阶段位置不低于 85"),
                    (activity is not None and activity >= 60, "活跃度不低于 60"),
                    (
                        sector["volatility_peer_percentile"] is not None
                        and sector["volatility_peer_percentile"] >= 60,
                        "波动率处于同类前 40%",
                    ),
                ),
                (
                    (position is not None and position >= 85, "250 日复权位置不低于 85"),
                    (
                        not relative or (position_peer is not None and position_peer >= 75),
                        "长期位置处于同类前 25%",
                    ),
                    (activity is not None and activity >= 75, "活跃度不低于 75"),
                    (
                        sector["volatility_peer_percentile"] is not None
                        and sector["volatility_peer_percentile"] >= 75,
                        "波动率处于同类前 25%",
                    ),
                ),
            ),
        }
        candidate_codes = [code for code, value in candidates.items() if value["eligible"]]
        high_risk = bool(
            position is not None
            and position >= 85
            and (not relative or (position_peer is not None and position_peer >= 75))
            and activity is not None
            and activity >= 75
            and (
                (
                    relative
                    and sector["volatility_peer_percentile"] is not None
                    and sector["volatility_peer_percentile"] >= 75
                )
                or (
                    not relative and metric["volatility_20d"] is not None and metric["volatility_20d"] >= 0.02
                )
            )
        )
        badges: list[dict[str, str]] = []
        if high_risk:
            badges.append({"code": "crowded_high", "label": "高位拥挤风险", "tone": "risk"})
        flow = sector["funds"].get("estimated_flow")
        if (
            sector["funds"].get("status") == "confirmed"
            and sector["funds"].get("directional_interpretation")
            and sector["asset_class"] != "money"
            and state in {"low_turn", "leading", "improving", "weakening"}
            and flow is not None
        ):
            bullish = state in {"low_turn", "leading", "improving"}
            confirming = (bullish and flow > 0) or (state == "weakening" and flow < 0)
            if flow != 0:
                badges.append(
                    {
                        "code": "fund_confirm" if confirming else "fund_divergence",
                        "label": "资金确认" if confirming else "资金背离",
                        "tone": "info" if confirming else "warning",
                    }
                )
        funds_value = sector["funds"]
        funds_value["flow_direction"] = (
            "inflow"
            if flow is not None and flow > 0
            else ("outflow" if flow is not None and flow < 0 else "flat")
        )
        if funds_value.get("status") != "confirmed":
            funds_value["interpretation_note"] = "份额证据不足，仅展示有效日期与来源"
        elif funds_value.get("coverage_level") == "low":
            funds_value["interpretation_note"] = "覆盖低于 50%，数值仅供核查，不生成资金确认或背离"
        elif not funds_value.get("consecutive"):
            funds_value["interpretation_note"] = "相邻交易日快照缺失，按累计变化展示，不解释为当日资金"
        elif sector["asset_class"] == "money" or state == "watch":
            funds_value["interpretation_note"] = "仅中性展示净申购或净赎回，不改变主状态"
        else:
            funds_value["interpretation_note"] = "连续交易日证据可用于辅助核查主状态"
        invalidations = {
            "low_turn": "跌回 MA20 下方，或量能比低于 1.10",
            "leading": "MA20 下穿 MA60，或 20 日收益转负",
            "improving": "跌破 MA20 且 MA20 斜率转负",
            "weakening": "重新站上 MA20 且 5 日收益转正",
            "watch": "等待 5 日收益、MA20 斜率与量能共同突破",
            "not_applicable": "货币 ETF 不判定高低位，仅观察流动性与申赎",
        }
        sector.update(
            {
                "state": state,
                "state_label": STATE_LABELS[state],
                "risk_badges": badges,
                "candidates": candidates,
                "candidate_codes": candidate_codes,
                "invalidation": invalidations[state],
                "evidence": {
                    "return_5d": metric["return_5d"],
                    "return_20d": metric["return_20d"],
                    "return_60d": metric["return_60d"],
                    "above_ma20": metric["above_ma20"],
                    "ma20_slope": metric["ma20_slope"],
                    "amount_ratio": ratio,
                    "position_250d": position,
                    "position_60d": position60,
                    "display_position": sector.get("display_position"),
                    "position_metric": sector.get("position_metric"),
                },
            }
        )

    order = {"low_turn": 0, "leading": 1, "improving": 2, "watch": 3, "weakening": 4, "not_applicable": 5}
    sectors.sort(
        key=lambda item: (order[item["state"]], -(item["trend_strength"] or -1), item["sector_name"])
    )
    queues: dict[str, tuple[str, ...]] = {}
    for state in ("leading", "low_turn", "improving", "weakening", "watch"):
        selected = [item for item in sectors if item["state"] == state]
        selected.sort(key=lambda item: item["trend_strength"] or -1, reverse=state != "weakening")
        queues[state] = tuple(item["sector_id"] for item in selected)
    risks = [
        item for item in sectors if any(badge["code"] == "crowded_high" for badge in item["risk_badges"])
    ]
    risks.sort(key=lambda item: item["activity_score"] or -1, reverse=True)
    queues["risk"] = tuple(item["sector_id"] for item in risks)

    candidate_queues: dict[str, tuple[str, ...]] = {}
    for code in ("momentum_hot", "stage_low_rebound", "stage_high_activity"):
        selected = [item for item in sectors if code in item.get("candidate_codes", ())]
        selected.sort(
            key=lambda item: (
                item.get("activity_score") if code == "stage_high_activity" else item.get("trend_strength")
            )
            or -1,
            reverse=True,
        )
        candidate_queues[code] = tuple(item["sector_id"] for item in selected)

    lookup = {item["sector_id"]: item for item in sectors}
    rankable = [item for item in sectors if item.get("trend_strength") is not None]
    strongest = max(rankable, key=lambda item: item["trend_strength"], default=None)
    strict_low = lookup.get(queues["low_turn"][0]) if queues["low_turn"] else None
    staged_low = (
        lookup.get(candidate_queues["stage_low_rebound"][0])
        if candidate_queues["stage_low_rebound"]
        else None
    )
    strict_risk = lookup.get(queues["risk"][0]) if queues["risk"] else None
    staged_risk = (
        lookup.get(candidate_queues["stage_high_activity"][0])
        if candidate_queues["stage_high_activity"]
        else None
    )
    weak_risk = lookup.get(queues["weakening"][0]) if queues["weakening"] else None
    position_available = any(item.get("display_position") is not None for item in sectors)

    summaries: list[dict[str, Any]] = []
    if strongest:
        summaries.append(
            {
                "kind": "strongest",
                "title": "趋势最强",
                "sector_id": strongest["sector_id"],
                "sector_name": strongest["sector_name"],
                "state": strongest["state"],
                "evaluation_status": "confirmed",
                "text": (
                    f"{strongest['sector_name']} · 代表 {strongest['representative']['name']} · "
                    f"趋势 {strongest['trend_strength']:.0f}"
                ),
            }
        )
    else:
        summaries.append(
            {
                "kind": "strongest",
                "title": "趋势最强",
                "sector_id": "",
                "sector_name": "",
                "state": "none",
                "evaluation_status": "unavailable",
                "text": "收益或成交额证据不足，暂无法比较板块趋势",
            }
        )

    low = strict_low or staged_low
    low_status = "confirmed" if strict_low else ("candidate" if staged_low else "confirmed")
    if low:
        low_text = (
            f"{low['sector_name']} · {low['state_label']} · 趋势 {low['trend_strength']:.0f}"
            if strict_low
            else f"{low['sector_name']} · 60 日阶段低位候选，尚未满足长期确认"
        )
    else:
        low_text = (
            "本期未发现满足严格或阶段候选条件的低位机会"
            if position_available
            else "阶段位置证据不足，暂无法评估低位机会"
        )
        if not position_available:
            low_status = "unavailable"
    summaries.append(
        {
            "kind": "low_turn",
            "title": "低位机会",
            "sector_id": low["sector_id"] if low else "",
            "sector_name": low["sector_name"] if low else "",
            "state": low["state"] if low else "none",
            "evaluation_status": low_status,
            "text": low_text,
        }
    )

    risk = strict_risk or staged_risk or weak_risk
    if strict_risk:
        risk_status, risk_text = "confirmed", f"{risk['sector_name']} · 高位拥挤风险已确认"
    elif staged_risk:
        risk_status, risk_text = "candidate", f"{risk['sector_name']} · 60 日阶段高位活跃候选"
    elif weak_risk:
        risk_status, risk_text = "confirmed", f"{risk['sector_name']} · 严格走弱，留意趋势延续"
    elif position_available:
        risk_status, risk_text = "confirmed", "本期未发现长期拥挤、阶段高位活跃或严格走弱板块"
    else:
        risk_status, risk_text = "unavailable", "位置证据不足，暂无法评估高位风险"
    summaries.append(
        {
            "kind": "risk",
            "title": "主要风险",
            "sector_id": risk["sector_id"] if risk else "",
            "sector_name": risk["sector_name"] if risk else "",
            "state": risk["state"] if risk else "none",
            "evaluation_status": risk_status,
            "text": risk_text,
        }
    )
    return sectors, representative_by_symbol, queues, candidate_queues, tuple(summaries)
