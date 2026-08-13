from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from quantmaster.data.cache_contracts import (
    CacheCleanupCandidate,
    CacheContractError,
    CacheKey,
    CacheResult,
    CacheResultKind,
    NegativeCacheRecord,
    NegativeCacheStore,
    cache_contract,
    plan_cache_cleanup,
    require_negative_result,
    revision_action,
)


def _catalog_key(**changes) -> CacheKey:
    values = {
        "namespace": "instrument.catalog",
        "provider": "tushare",
        "resource": "stock_basic",
        "market": "CN",
        "as_of": "2026-08-13",
        "config_revision": "cfg-4",
        "filters": (("list_status", "L"),),
        "page": "1",
        "page_size": 2000,
        "complete_range": True,
    }
    values.update(changes)
    return CacheKey(**values)


@pytest.mark.parametrize("kind", [
    CacheResultKind.RATE_LIMITED,
    CacheResultKind.TEMPORARY_FAILURE,
    CacheResultKind.INVALID_RESPONSE,
    CacheResultKind.PERMISSION_DENIED,
    CacheResultKind.PARTIAL,
    CacheResultKind.EMPTY_VALID,
])
def test_only_confirmed_not_found_can_enter_negative_cache(kind) -> None:
    with pytest.raises(CacheContractError, match="只有 not_found"):
        require_negative_result(CacheResult(kind))

    require_negative_result(CacheResult(CacheResultKind.NOT_FOUND))


def test_partial_and_not_found_are_not_normal_batch_cache_values() -> None:
    assert CacheResult(CacheResultKind.SUCCESS).cacheable_as_batch
    assert CacheResult(CacheResultKind.EMPTY_VALID).cacheable_as_batch
    partial = CacheResult(CacheResultKind.PARTIAL, completed_items=2, expected_items=3)
    assert partial.requires_pending
    with pytest.raises(CacheContractError, match="不能作为正常批次"):
        partial.require_cacheable_as_batch()
    with pytest.raises(CacheContractError, match="不能作为正常批次"):
        CacheResult(CacheResultKind.NOT_FOUND).require_cacheable_as_batch()


def test_key_is_readable_complete_and_revision_isolated() -> None:
    first = _catalog_key()
    second = _catalog_key(config_revision="cfg-5")
    assert first.canonical() != second.canonical()
    assert '"provider":"tushare"' in first.canonical()
    assert '"list_status":"L"' in first.canonical()
    assert '"complete_range":true' in first.canonical()
    assert "validation_hash" not in first.canonical()
    assert "hash_tag" not in first.canonical()


def test_namespace_rejects_incomplete_business_key() -> None:
    contract = cache_contract("market.bars")
    with pytest.raises(CacheContractError, match="adjustment"):
        contract.validate_key(CacheKey(
            namespace="market.bars", provider="stockdb", resource="daily",
            symbol="600000.SH", market="CN", timeframe="1d",
            range_start="2026-01-01", range_end="2026-08-13",
            currency="CNY", unit="share,CNY", config_revision="cfg-1",
            parser_revision="bars-v2",
        ))


def test_negative_cache_records_reason_source_ttl_and_revision(tmp_path) -> None:
    store = NegativeCacheStore(tmp_path / "negative.sqlite")
    key = _catalog_key()
    observed = datetime(2026, 8, 13, 2, tzinfo=UTC)
    record = NegativeCacheRecord.confirmed_not_found(
        key,
        negative_reason="provider_catalog_confirmed_absent",
        source="tushare:stock_basic",
        ttl_seconds=3600,
        observed_at=observed,
        dependency_revisions={"provider_config": "cfg-4"},
    )
    store.put(
        record, cache_contract("instrument.catalog"),
        result=CacheResult(CacheResultKind.NOT_FOUND),
    )

    hit = store.get(
        key, dependency_revisions={"provider_config": "cfg-4"},
        now=observed + timedelta(minutes=10),
    )
    assert hit is not None
    assert hit.negative_reason == "provider_catalog_confirmed_absent"
    assert hit.source == "tushare:stock_basic"
    assert hit.observed_at == observed.isoformat()
    assert hit.expires_at == (observed + timedelta(hours=1)).isoformat()

    assert store.get(
        key, dependency_revisions={"provider_config": "cfg-5"},
        now=observed + timedelta(minutes=20),
    ) is None
    assert store.rows() == []


def test_negative_cache_expiry_and_precise_namespace_invalidation(tmp_path) -> None:
    store = NegativeCacheStore(tmp_path / "negative.sqlite")
    observed = datetime(2026, 8, 13, 2, tzinfo=UTC)
    contract = cache_contract("instrument.catalog")
    first = _catalog_key(resource="stock_basic")
    second = _catalog_key(resource="fund_basic")
    for key, revision in ((first, "cfg-4"), (second, "cfg-5")):
        store.put(
            NegativeCacheRecord.confirmed_not_found(
                key, negative_reason="confirmed_absent", source=f"tushare:{key.resource}",
                ttl_seconds=60, observed_at=observed,
                dependency_revisions={"provider_config": revision},
            ),
            contract,
            result=CacheResult(CacheResultKind.NOT_FOUND),
        )

    assert store.invalidate_dependency("instrument.catalog", "provider_config", "cfg-5") == 1
    assert [row["key_json"] for row in store.rows()] == [second.canonical()]
    assert store.prune_expired(now=observed + timedelta(minutes=2)) == 1
    assert store.rows() == []


def test_raw_evidence_contract_is_protected_from_generic_cleanup() -> None:
    assert cache_contract("stockdb.raw").preserve_unique_raw
    assert cache_contract("provider.raw").preserve_unique_raw
    assert cache_contract("news.raw").preserve_unique_raw


def test_negative_store_rejects_failure_even_with_forged_record(tmp_path) -> None:
    store = NegativeCacheStore(tmp_path / "negative.sqlite")
    record = NegativeCacheRecord.confirmed_not_found(
        _catalog_key(), negative_reason="absent", source="catalog", ttl_seconds=30,
    )
    with pytest.raises(CacheContractError, match="只有 not_found"):
        store.put(
            record, cache_contract("instrument.catalog"),
            result=CacheResult(CacheResultKind.PERMISSION_DENIED),
        )
    assert store.rows() == []


def test_parser_change_reuses_raw_but_config_change_refetches() -> None:
    assert revision_action(
        cached_config_revision="cfg-1", cached_parser_revision="parser-1",
        current_config_revision="cfg-1", current_parser_revision="parser-2",
        raw_available=True,
    ) == "reparse_raw"
    assert revision_action(
        cached_config_revision="cfg-1", cached_parser_revision="parser-1",
        current_config_revision="cfg-2", current_parser_revision="parser-2",
        raw_available=True,
    ) == "refetch"
    assert revision_action(
        cached_config_revision="cfg-1", cached_parser_revision="parser-1",
        current_config_revision="cfg-1", current_parser_revision="parser-1",
        raw_available=True,
    ) == "use_normalized"


def test_cleanup_prefers_old_regenerable_and_protects_raw_or_active(tmp_path) -> None:
    candidates = [
        CacheCleanupCandidate(
            tmp_path / "normalized-old", "provider.normalized", 60, "2026-01-01",
            regenerable=True,
        ),
        CacheCleanupCandidate(
            tmp_path / "normalized-new", "provider.normalized", 80, "2026-08-01",
            regenerable=True,
        ),
        CacheCleanupCandidate(
            tmp_path / "unique-raw", "provider.raw", 1000, "2025-01-01",
            regenerable=False, unique_raw=True,
        ),
        CacheCleanupCandidate(
            tmp_path / "active", "lab.panel", 1000, "2025-01-01",
            regenerable=True, in_use=True,
        ),
    ]
    planned = plan_cache_cleanup(tmp_path, candidates, bytes_to_free=100)
    assert [item.path.name for item in planned] == ["normalized-old", "normalized-new"]


def test_cleanup_rejects_candidate_outside_namespace_root(tmp_path) -> None:
    outside = tmp_path.parent / "outside"
    with pytest.raises(CacheContractError, match="越出"):
        plan_cache_cleanup(
            tmp_path,
            [CacheCleanupCandidate(
                outside, "provider.normalized", 10, "2026-01-01", regenerable=True,
            )],
            bytes_to_free=1,
        )
