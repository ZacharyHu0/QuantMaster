"""Trusted GitHub release discovery and local staging contracts."""

from __future__ import annotations

import base64
import hashlib
import io
import json
from pathlib import Path

from quantmaster.runtime import release_staging as staging
from quantmaster.runtime.update import update_status

SHA_A = "a" * 40
SHA_B = "b" * 40


class Response(io.BytesIO):
    def __init__(self, value: bytes) -> None:
        super().__init__(value)
        self.headers = {"Content-Length": str(len(value))}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def _encoded(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


def _asset(name: str, content: bytes, tag: str = "v1.18.1") -> dict[str, object]:
    asset_id = {
        staging.WINDOWS_ASSET: 101,
        staging.SBOM_ASSET: 102,
        staging.CHECKSUMS_ASSET: 103,
    }[name]
    return {
        "id": asset_id,
        "name": name,
        "url": f"{staging.API_ROOT}/releases/assets/{asset_id}",
        "browser_download_url": f"{staging.REPOSITORY_URL}/releases/download/{tag}/{name}",
        "digest": f"sha256:{hashlib.sha256(content).hexdigest()}",
    }


def _root(root: Path) -> None:
    slot = root / "slots" / SHA_A
    slot.mkdir(parents=True)
    (slot / "slot_meta.json").write_text(
        json.dumps({"schema": 1, "build_sha": SHA_A, "version": "1.18.0"}),
        encoding="utf-8",
    )
    root.joinpath("active.json").write_text(json.dumps({
        "schema": 1,
        "active": SHA_A,
        "previous": "",
        "pending": "",
        "status": "stable",
        "last_error": "",
    }), encoding="utf-8")
    root.joinpath("launcher.target").write_text(f"{SHA_A}\n", encoding="ascii")


def _github(*, windows: bytes = b"trusted-windows-onefile"):
    tag = "v1.18.1"
    sbom = _encoded({
        "bomFormat": "CycloneDX",
        "metadata": {"component": {"name": "QuantMaster", "version": "1.18.1"}},
        "components": [{
            "name": staging.WINDOWS_ASSET,
            "hashes": [{"alg": "SHA-256", "content": hashlib.sha256(windows).hexdigest()}],
        }],
    })
    checksums = (
        f"{hashlib.sha256(windows).hexdigest()}  {staging.WINDOWS_ASSET}\n"
        f"{hashlib.sha256(sbom).hexdigest()}  {staging.SBOM_ASSET}\n"
    ).encode()
    assets = {
        staging.WINDOWS_ASSET: _asset(staging.WINDOWS_ASSET, windows),
        staging.SBOM_ASSET: _asset(staging.SBOM_ASSET, sbom),
        staging.CHECKSUMS_ASSET: _asset(staging.CHECKSUMS_ASSET, checksums),
    }
    statement = {
        "subject": [{
            "name": staging.WINDOWS_ASSET,
            "digest": {"sha256": hashlib.sha256(windows).hexdigest()},
        }],
        "predicateType": "https://slsa.dev/provenance/v1",
        "predicate": {"buildDefinition": {
            "externalParameters": {"workflow": {
                "repository": staging.REPOSITORY_URL,
                "path": f"/{staging.RELEASE_WORKFLOW}",
                "ref": f"refs/tags/{tag}",
            }},
            "internalParameters": {"github": {
                "repository_id": str(staging.REPOSITORY_ID),
                "event_name": "push",
            }},
            "resolvedDependencies": [{"digest": {"gitCommit": SHA_B}}],
        }},
    }
    attestation = _encoded({"attestations": [{"bundle": {"dsseEnvelope": {
        "payload": base64.b64encode(_encoded(statement)).decode(),
    }}}]})
    release = _encoded({
        "draft": False,
        "prerelease": False,
        "tag_name": tag,
        "name": "v1.18.1",
        "published_at": "2026-08-20T12:00:00Z",
        "assets": list(assets.values()),
    })
    tag_object = "c" * 40
    responses = {
        f"{staging.API_ROOT}/releases/latest": release,
        f"{staging.API_ROOT}/git/ref/tags/{tag}": _encoded({
            "object": {"type": "tag", "sha": tag_object},
        }),
        f"{staging.API_ROOT}/git/tags/{tag_object}": _encoded({
            "object": {"type": "commit", "sha": SHA_B},
        }),
        f"{staging.API_ROOT}/attestations/sha256:{hashlib.sha256(windows).hexdigest()}": attestation,
        **{
            str(asset["url"]): content
            for name, content in {
                staging.WINDOWS_ASSET: windows,
                staging.SBOM_ASSET: sbom,
                staging.CHECKSUMS_ASSET: checksums,
            }.items()
            for asset in (assets[name],)
        },
    }

    def open_url(request, **_kwargs):
        return Response(responses[request.full_url])

    return open_url, responses, assets


def test_latest_release_is_verified_staged_and_visible(tmp_path, monkeypatch):
    _root(tmp_path)
    opener, _responses, _assets = _github()
    monkeypatch.setattr(staging, "_smoke", lambda _exe: {
        "layout": "onefile", "help_ok": True, "help_seconds": 0.01,
    })

    result = staging.stage_latest_release(tmp_path, opener=opener, os_name="nt")

    assert result["status"] == "staged"
    slot = tmp_path / "slots" / SHA_B
    assert (slot / "QuantMaster.exe").read_bytes() == b"trusted-windows-onefile"
    marker = json.loads((slot / ".quantmaster-stage.json").read_text(encoding="utf-8"))
    assert marker["source"] == "github-release"
    assert marker["attestation"]["verified"] is True
    status = update_status(tmp_path)
    candidate = next(item for item in status["staged"] if item["build_sha"] == SHA_B)
    assert candidate["source"] == "github-release"
    assert candidate["eligible"] is True
    assert status["release_staging"]["status"] == "staged"
    assert str(tmp_path) not in json.dumps(status)


def test_release_staging_is_idempotent(tmp_path, monkeypatch):
    _root(tmp_path)
    opener, _responses, _assets = _github()
    monkeypatch.setattr(staging, "_smoke", lambda _exe: {
        "layout": "onefile", "help_ok": True, "help_seconds": 0.01,
    })
    assert staging.stage_latest_release(tmp_path, opener=opener, os_name="nt")["status"] == "staged"

    result = staging.stage_latest_release(tmp_path, opener=opener, os_name="nt")

    assert result["status"] == "already_staged"


def test_release_staging_fails_closed_before_writing_slot_on_digest_mismatch(
    tmp_path, monkeypatch,
):
    _root(tmp_path)
    opener, responses, assets = _github()
    url = str(assets[staging.WINDOWS_ASSET]["url"])
    responses[url] = b"tampered"
    monkeypatch.setattr(
        staging, "_smoke", lambda _exe: (_ for _ in ()).throw(AssertionError("must not execute")),
    )

    result = staging.stage_latest_release(tmp_path, opener=opener, os_name="nt")

    assert result["status"] == "blocked"
    assert result["code"] == "release_digest_mismatch"
    assert not (tmp_path / "slots" / SHA_B).exists()


def test_release_staging_rejects_wrong_attestation_workflow(tmp_path, monkeypatch):
    _root(tmp_path)
    opener, responses, assets = _github()
    digest = str(assets[staging.WINDOWS_ASSET]["digest"])[7:]
    statement = {
        "subject": [{"name": staging.WINDOWS_ASSET, "digest": {"sha256": digest}}],
        "predicateType": "https://slsa.dev/provenance/v1",
        "predicate": {"buildDefinition": {
            "externalParameters": {"workflow": {
                "repository": staging.REPOSITORY_URL,
                "path": "/.github/workflows/untrusted.yml",
                "ref": "refs/tags/v1.18.1",
            }},
            "internalParameters": {"github": {
                "repository_id": staging.REPOSITORY_ID, "event_name": "push",
            }},
            "resolvedDependencies": [{"digest": {"gitCommit": SHA_B}}],
        }},
    }
    responses[f"{staging.API_ROOT}/attestations/sha256:{digest}"] = _encoded({
        "attestations": [{"bundle": {"dsseEnvelope": {
            "payload": base64.b64encode(_encoded(statement)).decode(),
        }}}],
    })
    monkeypatch.setattr(
        staging, "_smoke", lambda _exe: (_ for _ in ()).throw(AssertionError("must not execute")),
    )

    result = staging.stage_latest_release(tmp_path, opener=opener, os_name="nt")

    assert result["status"] == "blocked"
    assert result["code"] == "release_attestation_invalid"
    assert not (tmp_path / "slots" / SHA_B).exists()
