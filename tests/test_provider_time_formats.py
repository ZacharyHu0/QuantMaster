from datetime import UTC, date, datetime

import pandas as pd
import pytest

from quantmaster.data.resilience import ProviderContractChanged
from quantmaster.data.tushare_source import TushareSource
from quantmaster.temporal import (
    InformationBoundary,
    KnowledgeMode,
    ProviderDateFormat,
    TemporalContractError,
    information_time,
    parse_business_date,
    parse_instant,
    parse_provider_date,
)


def test_provider_date_requires_declared_wire_format() -> None:
    assert parse_provider_date(
        "20260813",
        field="trade_date",
        provider_format=ProviderDateFormat.YYYYMMDD,
    ) == date(2026, 8, 13)
    assert parse_provider_date(
        "2026-08-13",
        field="trade_date",
        provider_format=ProviderDateFormat.ISO,
    ) == date(2026, 8, 13)

    with pytest.raises(TemporalContractError) as caught:
        parse_provider_date(
            "2026-08-13",
            field="trade_date",
            provider_format=ProviderDateFormat.YYYYMMDD,
        )
    assert caught.value.code == "invalid_provider_date"


@pytest.mark.parametrize(
    ("value", "unit"),
    [(1786581000, "s"), (1786581000000, "ms"), ("1786581000000", "ms")],
)
def test_unix_instant_requires_explicit_unit(value: object, unit: str) -> None:
    assert parse_instant(value, field="fetched_at", unix_unit=unit) == datetime(
        2026, 8, 13, 0, 30, tzinfo=UTC
    )

    with pytest.raises(TemporalContractError) as caught:
        parse_instant(value, field="fetched_at")
    assert caught.value.code == "unix_unit_required"


def test_iso_instant_never_uses_host_timezone_or_date_midnight() -> None:
    assert parse_instant(
        "2026-08-13T09:30:00-04:00", field="event_at"
    ) == datetime(2026, 8, 13, 13, 30, tzinfo=UTC)
    assert parse_instant(
        "2026-08-13T09:00:00",
        field="event_at",
        default_timezone="Asia/Shanghai",
    ) == datetime(2026, 8, 13, 1, 0, tzinfo=UTC)

    with pytest.raises(TemporalContractError) as missing_zone:
        parse_instant("2026-08-13 09:30:00", field="event_at")
    assert missing_zone.value.code == "timezone_required"

    with pytest.raises(TemporalContractError) as date_only:
        parse_instant(
            "2026-08-13", field="event_at", default_timezone="Asia/Shanghai"
        )
    assert date_only.value.code == "date_used_as_instant"


def test_business_date_and_cutoff_are_distinct_contracts() -> None:
    assert parse_business_date("2026-08-13") == date(2026, 8, 13)
    with pytest.raises(TemporalContractError, match="业务日期"):
        parse_business_date(datetime(2026, 8, 13, tzinfo=UTC))
    with pytest.raises(TemporalContractError, match="时区"):
        InformationBoundary(
            date(2026, 8, 13), datetime(2026, 8, 13, 15), "Asia/Shanghai",
        )


def test_replay_modes_choose_observed_or_trusted_published_explicitly() -> None:
    published = datetime(2026, 8, 1, tzinfo=UTC)
    observed = datetime(2026, 8, 10, tzinfo=UTC)
    assert information_time(
        published_at=published, first_observed_at=observed,
        mode=KnowledgeMode.STRICT_OBSERVED,
    ) == observed
    assert information_time(
        published_at=published, first_observed_at=observed,
        mode=KnowledgeMode.TRUSTED_PUBLISHED,
    ) == published


@pytest.mark.parametrize(
    ("value", "code"),
    [
        ("2026-03-08 02:30:00", "nonexistent_local_time"),
        ("2026-11-01 01:30:00", "ambiguous_local_time"),
    ],
)
def test_declared_iana_timezone_rejects_dst_gaps_and_folds(
    value: str, code: str
) -> None:
    with pytest.raises(TemporalContractError) as caught:
        parse_instant(
            value,
            field="event_at",
            default_timezone="America/New_York",
        )
    assert caught.value.code == code


def test_tushare_daily_boundary_rejects_non_yyyymmdd_trade_date() -> None:
    raw = pd.DataFrame(
        {
            "trade_date": ["2026-08-13"],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.0],
            "vol": [10.0],
            "amount": [20.0],
        }
    )

    with pytest.raises(ProviderContractChanged, match=r"invalid_provider_date"):
        TushareSource._normalize_market_frame(raw)


def test_tushare_financial_boundary_keeps_missing_ann_date_but_rejects_bad_value() -> None:
    valid = pd.DataFrame(
        {
            "ann_date": ["20260430", None],
            "end_date": ["20260331", "20260331"],
            "roe": [4.2, 5.0],
            "update_flag": ["0", "1"],
        }
    )
    normalized = TushareSource._normalize_quarterly_roe(valid)
    assert list(normalized.index) == [pd.Timestamp("2026-04-30")]
    assert normalized.iloc[0]["report_date"] == pd.Timestamp("2026-03-31")

    invalid = valid.iloc[[0]].copy()
    invalid.loc[:, "ann_date"] = "2026/04/30"
    with pytest.raises(ProviderContractChanged, match=r"invalid_provider_date"):
        TushareSource._normalize_quarterly_roe(invalid)
