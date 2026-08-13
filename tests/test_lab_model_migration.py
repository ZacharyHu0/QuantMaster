from __future__ import annotations

import json

from quantmaster.lab.model_migration import LabModelArtifactMigrator
from quantmaster.lab.models import FactorSpec
from quantmaster.lab.store import LabStore


def _write_v1(root, name="old"):
    directory = root / "lab_artifacts" / name
    directory.mkdir(parents=True)
    (directory / "model.npz").write_bytes(b"old")
    manifest = directory / "manifest.json"
    manifest.write_text(json.dumps({
        "schema_version": 1, "kind": "ridge", "features": ["close"],
        "sequence_length": 20, "horizon": 3,
        "artifact": f"lab_artifacts/{name}/model.npz", "artifact_sha256": "evidence",
    }), encoding="utf-8")
    return manifest


def test_lab_model_migration_isolates_exact_v1_and_blanks_reference(tmp_path):
    manifest = _write_v1(tmp_path)
    store = LabStore(tmp_path / "lab.sqlite")
    _factor, version, _created = store.create_factor(FactorSpec(
        slug="old", name="old", kind="learned", horizons=(3,),
        model={"manifest": "lab_artifacts/old/manifest.json"},
    ))
    migrator = LabModelArtifactMigrator()
    dry = list(migrator.inspect(tmp_path))
    assert [(item.outcome, item.diagnostic_code) for item in dry] == [
        ("review", "lab_model_schema_v1_requires_isolation")
    ]
    applied = list(migrator.migrate_batch(tmp_path, after_key="", limit=10))
    assert applied[0].diagnostic_code == "lab_model_schema_v1_isolated"
    assert not manifest.exists()
    assert (tmp_path / "migration_quarantine/lab_models/old/manifest.json").is_file()
    retired = store.version(version["id"])
    assert retired["status"] == "archived"
    assert retired["spec"]["model"]["manifest"] == ""
    assert retired["spec"]["model"]["diagnostic_code"] == "lab_model_schema_v1_isolated"
    assert list(migrator.migrate_batch(tmp_path, after_key="", limit=10)) == []


def test_lab_model_migration_never_guesses_unlabelled_or_corrupt_current(tmp_path):
    old = _write_v1(tmp_path, "unknown")
    value = json.loads(old.read_text(encoding="utf-8"))
    value.pop("schema_version")
    old.write_text(json.dumps(value), encoding="utf-8")
    current = tmp_path / "lab_artifacts/current/manifest-v2.json"
    current.parent.mkdir(parents=True)
    current.write_text(json.dumps({"schema_version": 2, "kind": "ridge"}), encoding="utf-8")
    records = {item.record_key: item for item in LabModelArtifactMigrator().inspect(tmp_path)}
    assert records["lab_artifacts/unknown/manifest.json"].diagnostic_code == "lab_model_schema_unclassified"
    assert records["lab_artifacts/current/manifest-v2.json"].diagnostic_code == "lab_model_schema_v2_corrupt"
