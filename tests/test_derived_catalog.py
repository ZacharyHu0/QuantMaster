from __future__ import annotations

import pandas as pd
import pytest

from quantmaster.runtime.derived import (
    DerivedArtifactCatalog,
    DerivedArtifactIntegrityError,
)


def test_generations_fingerprint_artifacts_and_atomic_pointer(tmp_path):
    catalog = DerivedArtifactCatalog(tmp_path / "derived")
    first = catalog.advance_source_generation(
        "stockdb", "bars:2026-08", "ingest-a", coverage_start="2026-08-01", coverage_end="2026-08-10",
    )
    same = catalog.advance_source_generation("stockdb", "bars:2026-08", "ingest-a")
    changed = catalog.advance_source_generation("stockdb", "bars:2026-08", "ingest-b")
    assert first["generation"] == same["generation"] == 1
    assert changed["generation"] == 2

    fingerprint = catalog.input_fingerprint(
        schema_version=2,
        algorithm_version="rotation-v3",
        parameters={"window": 5, "scope": "themes"},
        source_generations=catalog.source_generations("stockdb"),
    )
    artifact = catalog.put_json({"schema_version": 2, "items": [{"code": "A"}]})
    catalog.record_node(
        "rotation.themes",
        "all",
        fingerprint,
        "rotation-v3",
        output_artifact_id=artifact["artifact_id"],
    )
    cached = catalog.node_cache_hit("rotation.themes", "all", fingerprint, "rotation-v3")
    assert cached["artifact"]["artifact_id"] == artifact["artifact_id"]
    catalog.publish_snapshot("rotation", "themes", artifact["artifact_id"])
    current = catalog.current_snapshot("rotation", "themes")
    assert current["artifact"]["artifact_id"] == artifact["artifact_id"]
    assert catalog.read_json(artifact["artifact_id"])["items"][0]["code"] == "A"

    industries = catalog.put_json({"schema_version": 2, "items": [{"code": "801080.SI"}]})
    published = catalog.publish_snapshots("rotation", {
        "themes": artifact["artifact_id"],
        "industries": industries["artifact_id"],
    })
    assert set(published) == {"themes", "industries"}
    current = catalog.current_snapshot("rotation", "industries")
    assert current["artifact"]["artifact_id"] == industries["artifact_id"]


def test_parquet_artifacts_are_verified_before_reading(tmp_path):
    catalog = DerivedArtifactCatalog(tmp_path / "derived")
    artifact = catalog.put_frame(pd.DataFrame({"code": ["A", "B"], "score": [1.0, 2.0]}))
    assert catalog.read_frame(artifact["artifact_id"])["code"].tolist() == ["A", "B"]
    path = catalog.artifact(artifact["artifact_id"])["path"]
    path.write_bytes(b"corrupt")
    with pytest.raises(DerivedArtifactIntegrityError, match="哈希"):
        catalog.artifact(artifact["artifact_id"])
