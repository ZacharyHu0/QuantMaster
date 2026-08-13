from quantmaster.server.cache_observability import collect_cache_observability


def test_cache_observability_normalizes_namespace_evidence_without_inventing_hit_rate():
    payload = collect_cache_observability({
        "config_revision": "cfg-12",
        "namespaces": [{
            "namespace": "market.bars",
            "label": "行情",
            "hits": 9,
            "misses": 1,
            "counts": {"fresh": 7, "stale": 1, "partial": 2, "negative": 1},
            "oldest_at": "2026-08-01T00:00:00Z",
            "newest_at": "2026-08-13T07:00:00Z",
            "refresh": {"completed": 8, "total": 10, "pending": 2},
            "negatives": [{
                "negative_reason": "instrument_not_found",
                "source": "exchange-master",
                "observed_at": "2026-08-13T06:00:00Z",
                "expires_at": "2026-08-14T06:00:00Z",
            }],
            "stale_consumers": ["市场页"],
            "provider_revalidation_pending": 3,
            "parser_revision": "bars-v4",
            "issues": [{"diagnostic_code": "CACHE_PARTIAL", "message": "2 项待补齐"}],
        }, {
            "namespace": "custom_cold",
            "observed": True,
        }],
    })

    market = next(value for value in payload["namespaces"] if value["namespace"] == "market.bars")
    assert market["hit_rate"] == 0.9
    assert market["counts"] == {"fresh": 7, "stale": 1, "partial": 2, "negative": 1}
    assert market["refresh"] == {"completed": 8, "total": 10, "pending": 2}
    assert market["negatives"][0]["reason"] == "instrument_not_found"
    assert market["stale_consumers"] == ["市场页"]
    assert market["config_revision"] == "cfg-12"
    assert market["parser_revision"] == "bars-v4"
    assert market["issues"][0]["code"] == "CACHE_PARTIAL"
    cold = next(value for value in payload["namespaces"] if value["namespace"] == "custom_cold")
    assert cold["hit_rate"] is None
    assert payload["summary"]["hit_rate"] == 0.9
    assert payload["summary"]["pending"] == 2
    assert payload["summary"]["provider_revalidation_pending"] == 3


def test_cache_observability_marks_missing_known_namespaces_unobserved():
    payload = collect_cache_observability({"namespaces": []})

    assert payload["summary"]["observed_count"] == 0
    assert payload["summary"]["hit_rate"] is None
    assert payload["namespaces"]
    assert all(value["observed"] is False for value in payload["namespaces"])
    assert {
        value["diagnostic_code"] for value in payload["namespaces"]
    } == {"CACHE_NAMESPACE_UNOBSERVED"}


def test_cache_observability_consumes_finalized_registry_snapshot_shape():
    payload = collect_cache_observability([{
        "namespace": "news.raw",
        "value_kind": "content-addressed HTTP evidence",
        "freshness_rule": "conditional HTTP validators",
        "dependencies": ["source_config"],
        "hit_rate": 0.75,
        "hits": 3,
        "misses": 1,
        "fresh": 2,
        "stale": 1,
        "partial": 0,
        "negative": 2,
        "oldest": "2026-08-10T00:00:00Z",
        "newest": "2026-08-13T00:00:00Z",
        "pending": {"completed": 4, "total": 6},
        "negative_reasons": {"article_not_found": 2},
        "stale_consumers": ["资讯页"],
        "issues": ["CACHE_STALE_USED"],
        "diagnostic_code": "",
    }])
    namespace = next(value for value in payload["namespaces"] if value["namespace"] == "news.raw")

    assert namespace["observed"] is True
    assert namespace["refresh"] == {"completed": 4, "total": 6, "pending": 2}
    assert namespace["negatives"][0]["reason"] == "article_not_found"
    assert namespace["negatives"][0]["count"] == 2
    assert namespace["issues"] == [{"code": "CACHE_STALE_USED", "message": ""}]
    assert namespace["value_kind"] == "content-addressed HTTP evidence"
    assert namespace["dependencies"] == ["source_config"]


def test_cache_observability_bounds_diagnostic_and_negative_lists():
    payload = collect_cache_observability({"namespaces": [{
        "namespace": "provider.raw",
        "negatives": [{"reason": f"reason-{index}"} for index in range(30)],
        "issues": [{"code": f"CODE-{index}"} for index in range(30)],
        "stale_consumers": [f"page-{index}" for index in range(30)],
    }]})
    namespace = next(
        value for value in payload["namespaces"]
        if value["namespace"] == "provider.raw"
    )

    assert len(namespace["negatives"]) == 20
    assert len(namespace["issues"]) == 20
    assert len(namespace["stale_consumers"]) == 20
