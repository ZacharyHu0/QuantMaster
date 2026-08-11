from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from quantmaster.data.instrument_snapshots import (
    SUSPENSION_CONTRACT,
    SUSPENSION_SCHEMA_VERSION,
    SUSPENSION_SOURCE,
    TUSHARE_CATALOG_QUERY,
    TUSHARE_CATALOG_REQUESTS,
    InstrumentCatalogEvidenceError,
    content_hash,
    freeze_instrument_catalog,
    freeze_suspension_snapshot,
    load_instrument_catalog_snapshot,
    load_suspension_snapshot,
    snapshot_symbols,
    tushare_catalog_partition_evidence,
    tushare_catalog_request_params,
    tushare_suspension_request_evidence,
    verify_instrument_catalog_evidence,
)


def _complete_catalog(
    stock_count: int = 3000, etf_count: int = 100,
) -> tuple[list[dict], list[dict]]:
    raw: dict[tuple[str, str, str], list[dict]] = {
        request: [] for request in TUSHARE_CATALOG_REQUESTS
    }
    raw[("stock_basic", "list_status", "L")] = [{
        "ts_code": f"{600000 + index:06d}.SH", "symbol": f"{600000 + index:06d}",
        "name": f"股票{index}", "fullname": "", "enname": "", "exchange": "SSE",
        "curr_type": "CNY", "list_status": "L", "list_date": "20200101",
        "delist_date": "",
    } for index in range(stock_count)]
    raw[("stock_basic", "list_status", "D")] = [{
        "ts_code": "430001.BJ", "symbol": "430001", "name": "历史退市股票",
        "fullname": "", "enname": "", "exchange": "BSE", "curr_type": "CNY",
        "list_status": "D", "list_date": "20100101", "delist_date": "20200101",
    }]
    raw[("stock_basic", "list_status", "P")] = [{
        "ts_code": "430002.BJ", "symbol": "430002", "name": "暂停上市股票",
        "fullname": "", "enname": "", "exchange": "BSE", "curr_type": "CNY",
        "list_status": "P", "list_date": "20100101", "delist_date": "",
    }]
    raw[("fund_basic", "status", "L")] = [{
        "ts_code": f"{510000 + index:06d}.SH", "name": f"ETF{index}",
        "fund_type": "ETF", "status": "L", "list_date": "20200101",
        "delist_date": "",
    } for index in range(etf_count)]
    raw[("fund_basic", "status", "D")] = [{
        "ts_code": "159999.SZ", "name": "历史退市ETF", "fund_type": "ETF",
        "status": "D", "list_date": "20100101", "delist_date": "20200101",
    }]
    for index, market in enumerate(("CSI", "SSE", "SZSE"), start=1):
        raw[("index_basic", "market", market)] = [{
            "ts_code": f"{index:06d}.{market}", "name": f"{market}指数",
            "fullname": f"{market}指数", "market": market,
        }]
    for index, status in enumerate(("L", "D", "P"), start=1):
        raw[("hk_basic", "list_status", status)] = [{
            "ts_code": f"0000{index}.HK", "symbol": f"0000{index}",
            "name": f"港股{status}", "fullname": "", "enname": "",
            "list_status": status, "list_date": "20100101",
            "delist_date": "20200101" if status == "D" else "",
        }]
    records: list[dict] = []
    outcomes: list[dict] = []
    for endpoint, key, value in TUSHARE_CATALOG_REQUESTS:
        partition = raw[(endpoint, key, value)]
        normalized, evidence = tushare_catalog_partition_evidence(
            endpoint,
            key,
            value,
            params=tushare_catalog_request_params(endpoint, key, value),
            raw_records=partition,
            raw_columns=partition[0],
        )
        records.extend(normalized)
        outcomes.append(evidence)
    return records, outcomes


def _complete_records(stock_count: int = 3000, etf_count: int = 100) -> list[dict]:
    return _complete_catalog(stock_count, etf_count)[0]


def _after_close(days_ago: int = 0) -> datetime:
    now = datetime.now(UTC)
    shanghai_date = (now + timedelta(hours=8) - timedelta(days=days_ago)).date()
    return datetime.combine(shanghai_date, datetime.min.time(), UTC) + timedelta(hours=7, minutes=1)


def _request_outcomes(stock_count: int = 3000, etf_count: int = 100) -> list[dict]:
    return _complete_catalog(stock_count, etf_count)[1]


def _suspension_payload(
    trade_date: str,
    acquired_at: str,
    raw_records: list[dict],
) -> dict:
    rows, request_evidence = tushare_suspension_request_evidence(
        trade_date,
        raw_records=raw_records,
        raw_columns=("ts_code", "trade_date", "suspend_timing", "suspend_type"),
    )
    core = {
        "schema_version": SUSPENSION_SCHEMA_VERSION,
        "contract": SUSPENSION_CONTRACT,
        "source": SUSPENSION_SOURCE,
        "trade_date": trade_date,
        "acquired_at": acquired_at,
        "rows": rows,
        "request_evidence": request_evidence,
    }
    return {
        **core,
        "content_hash": content_hash(core),
        "symbols": sorted({item["symbol"] for item in rows}),
    }


def _rebuild_catalog_outcomes(outcomes: list[dict]) -> tuple[list[dict], list[dict]]:
    records: list[dict] = []
    rebuilt: list[dict] = []
    for item in outcomes:
        normalized, evidence = tushare_catalog_partition_evidence(
            item["endpoint"], item["partition_key"], item["partition_value"],
            params=item["params"], raw_records=item["raw_records"],
            raw_columns=item["raw_columns"],
        )
        records.extend(normalized)
        rebuilt.append(evidence)
    return records, rebuilt


def test_catalog_snapshot_is_content_addressed_and_recoverable(isolated_config):
    acquired = _after_close()
    snapshot = freeze_instrument_catalog(
        _complete_records(),
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=_request_outcomes(),
        acquired_at=acquired,
    )
    loaded, symbols, evidence = load_instrument_catalog_snapshot(
        as_of=str((acquired + timedelta(hours=8)).date()),
        market="CN",
        asset_type="stock",
    )
    assert loaded.snapshot_id == snapshot.snapshot_id
    assert len(symbols) == 3000
    assert evidence["asset_snapshot_count"] == 3000
    restored, restored_symbols = verify_instrument_catalog_evidence(evidence)
    assert restored.snapshot_id == snapshot.snapshot_id
    assert restored_symbols == symbols


def test_next_day_observation_reconstructs_previous_session_membership(isolated_config):
    _records, outcomes = _complete_catalog()
    stock_p = next(
        item for item in outcomes
        if item["endpoint"] == "stock_basic" and item["partition_value"] == "P"
    )
    stock_p["raw_records"] = []
    acquired = _after_close()
    observation_date = (acquired + timedelta(hours=8)).date()
    target = observation_date - timedelta(days=1)
    stock_d = next(
        item for item in outcomes
        if item["endpoint"] == "stock_basic" and item["partition_value"] == "D"
    )
    stock_d["raw_records"][0]["delist_date"] = observation_date.strftime("%Y%m%d")
    records, outcomes = _rebuild_catalog_outcomes(outcomes)
    snapshot = freeze_instrument_catalog(
        records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=outcomes,
        acquired_at=acquired,
    )

    loaded, symbols, evidence = load_instrument_catalog_snapshot(
        as_of=target.isoformat(), market="CN", asset_type="stock",
    )

    assert loaded.snapshot_id == snapshot.snapshot_id
    assert "430001.BJ" in symbols
    assert len(symbols) == 3001
    assert evidence["acquired_at"] == snapshot.acquired_at
    assert evidence["membership_as_of"] == target.isoformat()
    assert evidence["observation_active_as_of"] == observation_date.isoformat()
    assert evidence["membership_reconstructed"] is True
    restored, restored_symbols = verify_instrument_catalog_evidence(evidence)
    assert restored.snapshot_id == snapshot.snapshot_id
    assert restored_symbols == symbols
    with pytest.raises(InstrumentCatalogEvidenceError, match="成员日期契约"):
        verify_instrument_catalog_evidence({
            **evidence,
            "membership_as_of": observation_date.isoformat(),
        })


def test_historical_reconstruction_rejects_ambiguous_delisted_lifecycle(isolated_config):
    _records, outcomes = _complete_catalog()
    stock_p = next(
        item for item in outcomes
        if item["endpoint"] == "stock_basic" and item["partition_value"] == "P"
    )
    stock_p["raw_records"] = []
    stock_d = next(
        item for item in outcomes
        if item["endpoint"] == "stock_basic" and item["partition_value"] == "D"
    )
    stock_d["raw_records"][0]["delist_date"] = ""
    records, outcomes = _rebuild_catalog_outcomes(outcomes)
    acquired = _after_close()
    observation_date = (acquired + timedelta(hours=8)).date()
    freeze_instrument_catalog(
        records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=outcomes,
        acquired_at=acquired,
    )

    with pytest.raises(InstrumentCatalogEvidenceError, match="缺少历史可用的 delist_date"):
        load_instrument_catalog_snapshot(
            as_of=(observation_date - timedelta(days=1)).isoformat(),
            market="CN",
            asset_type="stock",
        )


def test_clean_one_stock_response_cannot_self_certify_complete():
    records, outcomes = _complete_catalog(stock_count=1)
    with pytest.raises(InstrumentCatalogEvidenceError, match="完整性下界"):
        freeze_instrument_catalog(
            records,
            source="tushare:catalog",
            query=TUSHARE_CATALOG_QUERY,
            request_outcomes=outcomes,
            acquired_at=_after_close(),
        )


def test_missing_delisted_partition_outcome_cannot_claim_full_query():
    outcomes = [
        item for item in _request_outcomes()
        if not (
            item["endpoint"] == "stock_basic"
            and item["partition_value"] == "D"
        )
    ]
    with pytest.raises(InstrumentCatalogEvidenceError, match="子请求证据不完整"):
        freeze_instrument_catalog(
            _complete_records(),
            source="tushare:catalog",
            query=TUSHARE_CATALOG_QUERY,
            request_outcomes=outcomes,
            acquired_at=_after_close(),
        )


def test_required_catalog_partition_cannot_use_empty_success_as_completeness():
    outcomes = _request_outcomes()
    next(
        item for item in outcomes
        if item["endpoint"] == "stock_basic" and item["partition_value"] == "D"
    )["raw_record_count"] = 0
    with pytest.raises(InstrumentCatalogEvidenceError, match="没有独立空集基线"):
        freeze_instrument_catalog(
            _complete_records(),
            source="tushare:catalog",
            query=TUSHARE_CATALOG_QUERY,
            request_outcomes=outcomes,
            acquired_at=_after_close(),
        )


def test_empty_suspended_partitions_do_not_block_current_active_universe():
    _records, outcomes = _complete_catalog()
    for item in outcomes:
        if (
            item["endpoint"] in {"stock_basic", "hk_basic"}
            and item["partition_value"] == "P"
        ):
            item["raw_records"] = []
    records, outcomes = _rebuild_catalog_outcomes(outcomes)

    snapshot = freeze_instrument_catalog(
        records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=outcomes,
        acquired_at=_after_close(),
    )

    assert snapshot.manifest["active_asset_counts"]["CN:stock"] == 3000


def test_current_listed_row_without_list_date_is_observation_day_only():
    records, outcomes = _complete_catalog()
    fund_l = next(
        item for item in outcomes
        if item["endpoint"] == "fund_basic" and item["partition_value"] == "L"
    )
    fund_l["raw_records"][0]["list_date"] = ""
    records, outcomes = _rebuild_catalog_outcomes(outcomes)
    acquired = _after_close()
    observation_date = (acquired + timedelta(hours=8)).date()
    snapshot = freeze_instrument_catalog(
        records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=outcomes,
        acquired_at=acquired,
    )

    current = snapshot_symbols(
        snapshot,
        market="CN",
        asset_type="etf",
        as_of=observation_date.isoformat(),
    )
    assert "510000.SH" in current
    with pytest.raises(InstrumentCatalogEvidenceError, match="历史可用"):
        snapshot_symbols(
            snapshot,
            market="CN",
            asset_type="etf",
            as_of=(observation_date - timedelta(days=1)).isoformat(),
        )


def test_current_missing_list_date_still_respects_expired_delist_date():
    _records, outcomes = _complete_catalog(etf_count=101)
    fund_l = next(
        item for item in outcomes
        if item["endpoint"] == "fund_basic" and item["partition_value"] == "L"
    )
    fund_l["raw_records"][0]["list_date"] = ""
    fund_l["raw_records"][0]["delist_date"] = "20200101"
    records, outcomes = _rebuild_catalog_outcomes(outcomes)
    acquired = _after_close()
    observation_date = (acquired + timedelta(hours=8)).date()
    snapshot = freeze_instrument_catalog(
        records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=outcomes,
        acquired_at=acquired,
    )

    current = snapshot_symbols(
        snapshot,
        market="CN",
        asset_type="etf",
        as_of=observation_date.isoformat(),
    )
    assert "510000.SH" not in current
    assert len(current) == 100


def test_future_listed_fund_is_frozen_but_excluded_before_list_date():
    _records, outcomes = _complete_catalog(etf_count=101)
    fund_l = next(
        item for item in outcomes
        if item["endpoint"] == "fund_basic" and item["partition_value"] == "L"
    )
    fund_l["raw_records"][0]["list_date"] = "20990101"
    records, outcomes = _rebuild_catalog_outcomes(outcomes)
    acquired = _after_close()
    observation_date = (acquired + timedelta(hours=8)).date()
    snapshot = freeze_instrument_catalog(
        records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=outcomes,
        acquired_at=acquired,
    )

    current = snapshot_symbols(
        snapshot,
        market="CN",
        asset_type="etf",
        as_of=observation_date.isoformat(),
    )
    assert "510000.SH" not in current
    assert len(current) == 100


def test_synthetic_outcomes_cannot_certify_unbound_catalog_records():
    records, outcomes = _complete_catalog()
    records[0] = {**records[0], "name": "未出现在原始分区响应中的合成名称"}
    with pytest.raises(InstrumentCatalogEvidenceError, match="逐分区原始响应"):
        freeze_instrument_catalog(
            records,
            source="tushare:catalog",
            query=TUSHARE_CATALOG_QUERY,
            request_outcomes=outcomes,
            acquired_at=_after_close(),
        )


def test_historical_delisted_rows_cannot_fill_active_completeness_floor():
    _records, outcomes = _complete_catalog()
    stock_l = next(
        item for item in outcomes
        if item["endpoint"] == "stock_basic" and item["partition_value"] == "L"
    )
    stock_d = next(
        item for item in outcomes
        if item["endpoint"] == "stock_basic" and item["partition_value"] == "D"
    )
    moved_stocks = stock_l["raw_records"][1:]
    stock_l["raw_records"] = stock_l["raw_records"][:1]
    stock_d["raw_records"].extend([
        {**row, "list_status": "D", "delist_date": "20200101"}
        for row in moved_stocks
    ])
    fund_l = next(
        item for item in outcomes
        if item["endpoint"] == "fund_basic" and item["partition_value"] == "L"
    )
    fund_d = next(
        item for item in outcomes
        if item["endpoint"] == "fund_basic" and item["partition_value"] == "D"
    )
    moved_funds = fund_l["raw_records"][1:]
    fund_l["raw_records"] = fund_l["raw_records"][:1]
    fund_d["raw_records"].extend([
        {**row, "status": "D", "delist_date": "20200101"}
        for row in moved_funds
    ])
    records, outcomes = _rebuild_catalog_outcomes(outcomes)
    with pytest.raises(InstrumentCatalogEvidenceError, match="active"):
        freeze_instrument_catalog(
            records,
            source="tushare:catalog",
            query=TUSHARE_CATALOG_QUERY,
            request_outcomes=outcomes,
            acquired_at=_after_close(),
        )


def test_delisted_partition_without_dates_can_prove_current_inactivity():
    _records, outcomes = _complete_catalog()
    stock_d = next(
        item for item in outcomes
        if item["endpoint"] == "stock_basic" and item["partition_value"] == "D"
    )
    stock_d["raw_records"][0]["delist_date"] = ""
    records, outcomes = _rebuild_catalog_outcomes(outcomes)
    snapshot = freeze_instrument_catalog(
        records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=outcomes,
        acquired_at=_after_close(),
    )

    assert snapshot.manifest["active_asset_counts"]["CN:stock"] == 3000


def test_delisted_row_without_list_date_can_prove_current_inactivity():
    _records, outcomes = _complete_catalog()
    hk_d = next(
        item for item in outcomes
        if item["endpoint"] == "hk_basic" and item["partition_value"] == "D"
    )
    hk_d["raw_records"][0]["list_date"] = ""
    records, outcomes = _rebuild_catalog_outcomes(outcomes)

    snapshot = freeze_instrument_catalog(
        records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=outcomes,
        acquired_at=_after_close(),
    )

    assert snapshot.manifest["active_asset_counts"]["CN:stock"] == 3000


def test_auxiliary_hk_lifecycle_gaps_do_not_block_cn_denominator():
    _records, outcomes = _complete_catalog()
    hk_l = next(
        item for item in outcomes
        if item["endpoint"] == "hk_basic" and item["partition_value"] == "L"
    )
    hk_l["raw_records"][0]["delist_date"] = "20200101"
    records, outcomes = _rebuild_catalog_outcomes(outcomes)

    snapshot = freeze_instrument_catalog(
        records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=outcomes,
        acquired_at=_after_close(),
    )

    assert snapshot.manifest["active_asset_counts"]["CN:stock"] == 3000


def test_cn_listed_partition_excludes_an_expired_delist_date():
    _records, outcomes = _complete_catalog(stock_count=3001)
    stock_l = next(
        item for item in outcomes
        if item["endpoint"] == "stock_basic" and item["partition_value"] == "L"
    )
    stock_l["raw_records"][0]["delist_date"] = "20200101"
    records, outcomes = _rebuild_catalog_outcomes(outcomes)

    acquired = _after_close()
    snapshot = freeze_instrument_catalog(
        records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=outcomes,
        acquired_at=acquired,
    )
    symbols = snapshot_symbols(
        snapshot,
        market="CN",
        asset_type="stock",
        as_of=(acquired + timedelta(hours=8)).date().isoformat(),
    )

    assert "600000.SH" not in symbols
    assert len(symbols) == 3000


def test_tushare_catalog_preserves_partition_evidence_and_delisted_fund_fields():
    from quantmaster.data.tushare_source import TushareSource

    class Source:
        instrument_catalog = TushareSource.instrument_catalog

        @staticmethod
        def _call(endpoint, _ttl, **params):
            if endpoint == "stock_basic":
                return pd.DataFrame(columns=[
                    "ts_code", "symbol", "name", "fullname", "enname", "exchange",
                    "curr_type", "list_status", "list_date", "delist_date",
                ])
            if endpoint == "fund_basic":
                if params["status"] == "D":
                    return pd.DataFrame([{
                        "ts_code": "510300.SH",
                        "name": "退市ETF",
                        "fund_type": "ETF",
                        "status": "D",
                        "list_date": "20200101",
                        "delist_date": "20260801",
                    }])
                return pd.DataFrame(columns=[
                    "ts_code", "name", "fund_type", "status", "list_date", "delist_date",
                ])
            if endpoint == "index_basic":
                return pd.DataFrame(columns=["ts_code", "name", "fullname", "market"])
            if endpoint == "hk_basic" and params["list_status"] == "L":
                # The live endpoint can omit its optional ``symbol`` column
                # even when the request fields include it.  ``ts_code`` is
                # sufficient to recover QuantMaster's canonical identity.
                return pd.DataFrame([{
                    "ts_code": "00001.HK", "name": "长和", "fullname": "长江和记实业",
                    "enname": "CK Hutchison", "list_status": "L",
                    "list_date": "20150318", "delist_date": "",
                }])
            return pd.DataFrame(columns=[
                "ts_code", "name", "fullname", "enname", "list_status",
                "list_date", "delist_date",
            ])

    records, outcomes = Source().instrument_catalog()
    fund = next(item for item in records if item["symbol"] == "510300.SH")
    hk_stock = next(item for item in records if item["symbol"] == "00001.HK")
    assert fund["status"] == "D"
    assert fund["delist_date"] == "2026-08-01"
    assert hk_stock["provider_symbol"] == "00001.HK"
    assert {
        (item["endpoint"], item["partition_key"], item["partition_value"])
        for item in outcomes
    } == set(TUSHARE_CATALOG_REQUESTS)
    assert all(item["status"] == "success" for item in outcomes)


def test_stale_catalog_snapshot_is_not_current():
    freeze_instrument_catalog(
        _complete_records(),
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=_request_outcomes(),
        acquired_at=_after_close(days_ago=8),
    )
    with pytest.raises(InstrumentCatalogEvidenceError, match="新鲜度"):
        load_instrument_catalog_snapshot(market="CN", asset_type="stock")


def test_catalog_tamper_and_observation_conflict_are_rejected(isolated_config):
    acquired = _after_close()
    snapshot = freeze_instrument_catalog(
        _complete_records(),
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=_request_outcomes(),
        acquired_at=acquired,
    )
    payload = json.loads(snapshot.path.read_text(encoding="utf-8"))
    payload["records"][0]["symbol"] = "999999.SH"
    snapshot.path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(InstrumentCatalogEvidenceError, match=r"self-hash|身份"):
        load_instrument_catalog_snapshot(
            as_of=str((acquired + timedelta(hours=8)).date()),
        )


def test_same_observation_identity_cannot_change_content():
    acquired = _after_close()
    records = _complete_records()
    freeze_instrument_catalog(
        records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=_request_outcomes(),
        acquired_at=acquired,
    )
    changed = _complete_records(stock_count=3001)
    with pytest.raises(InstrumentCatalogEvidenceError, match="observation_id"):
        freeze_instrument_catalog(
            changed,
            source="tushare:catalog",
            query=TUSHARE_CATALOG_QUERY,
            request_outcomes=_request_outcomes(stock_count=3001),
            acquired_at=acquired,
        )


def test_catalog_asset_count_cliff_is_rejected_after_verified_snapshot():
    acquired = _after_close()
    freeze_instrument_catalog(
        _complete_records(stock_count=3500),
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=_request_outcomes(stock_count=3500),
        acquired_at=acquired,
    )
    with pytest.raises(InstrumentCatalogEvidenceError, match="骤降"):
        freeze_instrument_catalog(
            _complete_records(stock_count=3000),
            source="tushare:catalog",
            query=TUSHARE_CATALOG_QUERY,
            request_outcomes=_request_outcomes(),
            acquired_at=acquired + timedelta(seconds=1),
        )


def test_suspension_snapshot_is_immutable_and_tamper_evident():
    acquired = _after_close()
    trade_date = str((acquired + timedelta(hours=8)).date())
    payload = _suspension_payload(trade_date, acquired.isoformat(), [{
        "ts_code": "600001.SH",
        "trade_date": trade_date.replace("-", ""),
        "suspend_type": "S",
        "suspend_timing": "09:30-15:00",
    }])
    evidence = freeze_suspension_snapshot(payload)
    assert load_suspension_snapshot(trade_date)["symbols"] == ["600001.SH"]
    path = evidence["relative_path"]
    from quantmaster.config import get_config

    target = get_config().data_root / path
    target.write_text(target.read_text(encoding="utf-8") + " ", encoding="utf-8")
    with pytest.raises(InstrumentCatalogEvidenceError, match=r"不可读|哈希|身份"):
        load_suspension_snapshot(trade_date)


def test_suspension_snapshot_accepts_next_day_exact_trade_date_response(isolated_config):
    payload = _suspension_payload(
        "2026-08-07", "2026-08-07T17:00:00+00:00", [],
    )

    frozen = freeze_suspension_snapshot(payload)

    assert frozen["trade_date"] == "2026-08-07"
    assert frozen["acquired_at"] == "2026-08-07T17:00:00+00:00"


def test_suspension_snapshot_rejects_target_day_preclose_observation(isolated_config):
    payload = _suspension_payload(
        "2026-08-07", "2026-08-07T06:59:59+00:00", [],
    )

    with pytest.raises(InstrumentCatalogEvidenceError, match="早于目标日收盘"):
        freeze_suspension_snapshot(payload)


def test_suspension_fetch_bypasses_preclose_endpoint_cache(tmp_path):
    from quantmaster.data.resilience import EndpointFrameCache
    from quantmaster.data.tushare_source import TushareSource

    trade_date = "20260807"
    fields = "ts_code,trade_date,suspend_timing,suspend_type"
    cache = EndpointFrameCache("suspend-authoritative", root=tmp_path / "cache")
    cache.put(
        "suspend_d", {"trade_date": trade_date, "fields": fields},
        pd.DataFrame(columns=[
            "ts_code", "trade_date", "suspend_timing", "suspend_type",
        ]),
    )

    class Api:
        calls = 0

        def suspend_d(self, **_params):
            self.calls += 1
            return pd.DataFrame([{
                "ts_code": "600001.SH", "trade_date": trade_date,
                "suspend_timing": "09:30-15:00", "suspend_type": "S",
            }])

    source = TushareSource(cache)
    api = Api()
    source._api = api

    payload = source.suspension_snapshot("2026-08-07")

    assert api.calls == 1
    assert payload["symbols"] == ["600001.SH"]


@pytest.mark.parametrize("field,value", [
    ("contract", "self-signed-suspension-v1"),
    ("source", "user:claimed-suspensions"),
])
def test_suspension_snapshot_rejects_untrusted_contract_or_source(field, value):
    acquired = _after_close()
    trade_date = str((acquired + timedelta(hours=8)).date())
    payload = _suspension_payload(trade_date, acquired.isoformat(), [])
    payload[field] = value
    core = {key: item for key, item in payload.items() if key not in {"content_hash", "symbols"}}
    payload["content_hash"] = content_hash(core)
    with pytest.raises(InstrumentCatalogEvidenceError, match=field):
        freeze_suspension_snapshot(payload)


def test_same_suspension_rows_are_idempotent_across_concurrent_acquisitions(
    isolated_config,
):
    acquired = _after_close()
    trade_date = str((acquired + timedelta(hours=8)).date())
    raw = [{
        "ts_code": "600001.SH", "trade_date": trade_date.replace("-", ""),
        "suspend_type": "S", "suspend_timing": "09:30-15:00",
    }]
    payloads = [
        _suspension_payload(
            trade_date, (acquired + timedelta(seconds=offset)).isoformat(), raw,
        )
        for offset in (0, 1)
    ]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(freeze_suspension_snapshot, payloads))
    assert results[0]["content_hash"] == results[1]["content_hash"]
    objects = list(
        (isolated_config.data_root / "suspension_snapshots" / "objects").glob("*.json")
    )
    assert len(objects) == 1


def test_conflicting_same_day_suspension_rows_do_not_poison_unique_artifact(
    isolated_config,
):
    acquired = _after_close()
    trade_date = str((acquired + timedelta(hours=8)).date())
    first = _suspension_payload(trade_date, acquired.isoformat(), [])
    freeze_suspension_snapshot(first)
    conflicting = _suspension_payload(
        trade_date,
        (acquired + timedelta(seconds=1)).isoformat(),
        [{
            "ts_code": "600001.SH", "trade_date": trade_date.replace("-", ""),
            "suspend_type": "S", "suspend_timing": "09:30-15:00",
        }],
    )
    with pytest.raises(InstrumentCatalogEvidenceError, match="同日观测内容冲突"):
        freeze_suspension_snapshot(conflicting)
    objects = list(
        (isolated_config.data_root / "suspension_snapshots" / "objects").glob("*.json")
    )
    assert len(objects) == 1
