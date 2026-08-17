"""Explicit business-date and instant contracts owned by the data domain.

Dates name market/business periods.  Instants name knowledge boundaries.  The
two are deliberately separate so a date-only ``as_of`` can never silently turn
into host midnight or an arbitrary end-of-day timestamp.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class TemporalContractError(ValueError):
    """A provider or caller supplied an ambiguous temporal value."""

    def __init__(self, message: str, *, code: str = "temporal_contract_error") -> None:
        super().__init__(message)
        self.code = code


class ProviderDateFormat(StrEnum):
    ISO = "iso"
    YYYYMMDD = "YYYYMMDD"


class KnowledgeMode(StrEnum):
    STRICT_OBSERVED = "strict_observed"
    TRUSTED_PUBLISHED = "trusted_published"


@dataclass(frozen=True)
class InformationBoundary:
    as_of_date: date
    cutoff_at: datetime
    market_timezone: str
    knowledge_mode: KnowledgeMode = KnowledgeMode.STRICT_OBSERVED

    def __post_init__(self) -> None:
        if self.cutoff_at.tzinfo is None or self.cutoff_at.utcoffset() is None:
            raise TemporalContractError("cutoff_at 必须包含时区")
        try:
            ZoneInfo(self.market_timezone)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise TemporalContractError("market_timezone 必须是有效 IANA 时区") from exc

    @property
    def cutoff_utc(self) -> datetime:
        return self.cutoff_at.astimezone(UTC)


def parse_business_date(value: date | str, *, field: str = "as_of_date") -> date:
    if isinstance(value, datetime):
        raise TemporalContractError(
            f"{field} 是业务日期，不能传入精确时刻",
            code="instant_used_as_business_date",
        )
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise TemporalContractError(
            f"{field} 必须使用 YYYY-MM-DD", code="invalid_business_date"
        ) from exc
    if parsed.isoformat() != text:
        raise TemporalContractError(
            f"{field} 必须使用 YYYY-MM-DD", code="invalid_business_date"
        )
    return parsed


def parse_provider_date(
    value: date | str,
    *,
    field: str,
    provider_format: ProviderDateFormat,
) -> date:
    """Parse a provider date only after its wire format has been declared."""
    if provider_format == ProviderDateFormat.ISO:
        return parse_business_date(value, field=field)
    if provider_format != ProviderDateFormat.YYYYMMDD:
        raise TemporalContractError(
            f"{field} 的 provider 日期格式未受支持: {provider_format!s}",
            code="unsupported_provider_date_format",
        )
    if isinstance(value, (date, datetime)):
        raise TemporalContractError(
            f"{field} 声明为 YYYYMMDD，必须保留 provider 原始字符串",
            code="provider_date_wire_type_mismatch",
        )
    text = str(value or "").strip()
    if len(text) != 8 or not text.isascii() or not text.isdigit():
        raise TemporalContractError(
            f"{field} 必须严格使用 YYYYMMDD", code="invalid_provider_date"
        )
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError as exc:
        raise TemporalContractError(
            f"{field} 不是有效的 YYYYMMDD 日期", code="invalid_provider_date"
        ) from exc


def _parse_unix_instant(value: int | float, *, field: str, unix_unit: str | None) -> datetime:
    if unix_unit not in {"s", "ms"}:
        raise TemporalContractError(
            f"{field} 的 Unix 时间必须明确声明 s 或 ms",
            code="unix_unit_required",
        )
    number = float(value) / (1000.0 if unix_unit == "ms" else 1.0)
    try:
        return datetime.fromtimestamp(number, UTC)
    except (OSError, OverflowError, ValueError) as exc:
        raise TemporalContractError(
            f"{field} 超出可解析范围", code="instant_out_of_range"
        ) from exc


def _parse_iso_instant(value: Any, *, field: str, unix_unit: str | None) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise TemporalContractError(f"{field} 不能为空", code="missing_instant")
    if text.isdigit():
        return _parse_unix_instant(int(text), field=field, unix_unit=unix_unit)
    if "T" not in text and " " not in text:
        raise TemporalContractError(
            f"{field} 是精确时刻，不能只提供日期", code="date_used_as_instant"
        )
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TemporalContractError(
            f"{field} 不是可识别的 ISO 时间", code="invalid_iso_instant"
        ) from exc


def _attach_declared_timezone(
    value: datetime,
    *,
    field: str,
    default_timezone: str | None,
) -> datetime:
    if value.tzinfo is not None and value.utcoffset() is not None:
        return value
    if not default_timezone:
        raise TemporalContractError(
            f"{field} 缺少时区且 provider 未声明默认时区",
            code="timezone_required",
        )
    try:
        zone = ZoneInfo(default_timezone)
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise TemporalContractError(
            f"{field} 的默认时区不是有效 IANA 时区", code="invalid_timezone"
        ) from exc
    candidates = [value.replace(tzinfo=zone, fold=fold) for fold in (0, 1)]
    valid = [
        candidate
        for candidate in candidates
        if candidate.astimezone(UTC).astimezone(zone).replace(tzinfo=None) == value
    ]
    if not valid:
        raise TemporalContractError(
            f"{field} 位于 {default_timezone} DST 跳时中，不存在该本地时刻",
            code="nonexistent_local_time",
        )
    if len(valid) == 2 and valid[0].utcoffset() != valid[1].utcoffset():
        raise TemporalContractError(
            f"{field} 位于 {default_timezone} DST 回拨中，provider 未声明 fold/offset",
            code="ambiguous_local_time",
        )
    return valid[0]


def parse_instant(
    value: Any,
    *,
    field: str,
    default_timezone: str | None = None,
    unix_unit: str | None = None,
) -> datetime:
    """Parse a precise instant without guessing timezone or Unix units."""
    parsed: datetime
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        parsed = _parse_unix_instant(value, field=field, unix_unit=unix_unit)
    else:
        parsed = _parse_iso_instant(value, field=field, unix_unit=unix_unit)
    return _attach_declared_timezone(
        parsed,
        field=field,
        default_timezone=default_timezone,
    ).astimezone(UTC)


def information_time(
    *,
    published_at: datetime | None,
    first_observed_at: datetime | None,
    mode: KnowledgeMode,
) -> datetime | None:
    """Return the instant that admits an item in an explicit replay mode."""
    if mode == KnowledgeMode.STRICT_OBSERVED:
        return first_observed_at.astimezone(UTC) if first_observed_at else None
    return published_at.astimezone(UTC) if published_at else None
