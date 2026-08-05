"""Shared, evidence-based A-share session expectations.

Calendar consumers must never invent Chinese holidays from generic weekdays.
This resolver accepts either the official SSE calendar or dates evidenced by
validated local market data.  With neither source it returns a safe skip.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from datetime import time as wall_time
from zoneinfo import ZoneInfo

from quantmaster.config import get_config

SHANGHAI = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class SessionExpectation:
    session: str = ""
    source: str = "unavailable"
    ready: bool = False
    reason: str = ""

    def as_dict(self) -> dict[str, str | bool]:
        return {
            "session": self.session,
            "source": self.source,
            "ready": self.ready,
            "reason": self.reason,
        }


def _normalize_now(value: datetime | None) -> datetime:
    current = value or datetime.now(SHANGHAI)
    if current.tzinfo is None:
        return current.replace(tzinfo=SHANGHAI)
    return current.astimezone(SHANGHAI)


def _latest_not_after(values: Iterable[object], cutoff: date) -> str:
    dates: set[date] = set()
    for raw in values:
        try:
            parsed = date.fromisoformat(str(raw)[:10])
        except ValueError:
            continue
        if parsed <= cutoff:
            dates.add(parsed)
    return max(dates).isoformat() if dates else ""


class SessionExpectationResolver:
    """Resolve the latest session whose close data should be available."""

    def __init__(self, *, ready_at: wall_time = wall_time(18, 30)):
        self.ready_at = ready_at

    def _official_sessions(self, start: date, end: date) -> list[str]:
        if not get_config().data.tushare_token:
            return []
        from quantmaster.data.tushare_source import TushareSource

        calendar = TushareSource().trade_calendar(start.isoformat(), end.isoformat())
        return [str(value.date()) for value in calendar]

    @staticmethod
    def _research_sessions(start: date, end: date) -> list[str]:
        root = get_config().data_root / "research_lake"
        catalog_path = root / "_meta" / "catalog.sqlite"
        if not catalog_path.is_file():
            return []
        from quantmaster.research.catalog import ResearchCatalog
        from quantmaster.research.contracts import AssetClass, Frequency

        return ResearchCatalog(catalog_path).trading_dates(
            AssetClass.STOCK, Frequency.DAILY, start.isoformat(), end.isoformat(),
        )

    @staticmethod
    def _bar_sessions(end: date) -> list[str]:
        from quantmaster.data.storage import BarStore

        metadata = BarStore().metadata_many()
        candidates = [
            str(item.get("end") or "")[:10]
            for symbol, item in metadata.items()
            if symbol.endswith((".SH", ".SZ", ".BJ"))
            and item.get("content_sha256")
            and int(item.get("row_count") or 0) > 0
            and str(item.get("end") or "")[:10] <= end.isoformat()
        ]
        if not candidates:
            return []
        value, count = Counter(candidates).most_common(1)[0]
        # A lone security can be suspended or stale; require broad corroboration.
        minimum = min(100, max(5, len(metadata) // 10))
        return [value] if count >= minimum else []

    def resolve(self, now: datetime | None = None) -> SessionExpectation:
        current = _normalize_now(now)
        cutoff = current.date()
        if current.timetz().replace(tzinfo=None) < self.ready_at:
            cutoff -= timedelta(days=1)
        start = cutoff - timedelta(days=45)
        failures: list[str] = []
        try:
            session = _latest_not_after(self._official_sessions(start, cutoff), cutoff)
            if session:
                return SessionExpectation(session, "tushare:SSE", True, "官方交易日历")
        except (ImportError, OSError, RuntimeError, ValueError) as exc:
            failures.append(f"官方日历不可用：{str(exc)[:160]}")
        try:
            session = _latest_not_after(self._research_sessions(start, cutoff), cutoff)
            if session:
                return SessionExpectation(session, "research_lake", True, "已验证本地交易分区")
        except (ImportError, OSError, RuntimeError, ValueError) as exc:
            failures.append(f"研究湖日历不可用：{str(exc)[:160]}")
        try:
            session = _latest_not_after(self._bar_sessions(cutoff), cutoff)
            if session:
                return SessionExpectation(session, "bar_catalog", True, "多标的校验行情")
        except (ImportError, OSError, RuntimeError, ValueError) as exc:
            failures.append(f"行情目录不可用：{str(exc)[:160]}")
        action = "请配置 Tushare 交易日历或先完成一次全市场日线同步"
        detail = "；".join(failures)
        return SessionExpectation(
            source="unavailable",
            ready=False,
            reason=f"{action}{f'（{detail}）' if detail else ''}",
        )


_resolver = SessionExpectationResolver()


def expected_session(now: datetime | None = None) -> SessionExpectation:
    return _resolver.resolve(now)
