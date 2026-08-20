"""Discover and stage the latest trusted QuantMaster GitHub release."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quantmaster.release import VERSION
from quantmaster.runtime.activation import (
    FULL_SHA,
    ActivationBlocked,
    SlotRegistry,
    installed_app_root,
    lifecycle_lock,
)

REPOSITORY = "ZacharyHu0/QuantMaster"
REPOSITORY_ID = 1_313_070_611
REPOSITORY_URL = f"https://github.com/{REPOSITORY}"
API_ROOT = f"https://api.github.com/repos/{REPOSITORY}"
RELEASE_WORKFLOW = ".github/workflows/release.yml"
WINDOWS_ASSET = "QuantMaster-windows.exe"
CHECKSUMS_ASSET = "SHA256SUMS"
SBOM_ASSET = "QuantMaster.cdx.json"
MAX_ONEFILE_BYTES = 350 * 1024 * 1024
MAX_METADATA_BYTES = 8 * 1024 * 1024
OPERATION_FILE = ".release-stage-operation.json"
VERSION_PATTERN = re.compile(r"(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")

_worker_lock = threading.Lock()
_worker: threading.Thread | None = None


class ReleaseStageBlocked(RuntimeError):
    """A path-free, fail-closed release staging decision."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


OpenURL = Callable[..., Any]


def operation_path(app_root: str | Path | None = None) -> Path:
    root = Path(app_root).resolve() if app_root is not None else installed_app_root()
    return root / OPERATION_FILE


def read_release_staging_status(app_root: str | Path | None = None) -> dict[str, object] | None:
    try:
        value = json.loads(operation_path(app_root).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError, ActivationBlocked):
        return None
    return dict(value) if isinstance(value, dict) else None


def _write_status(root: Path, status: str, **fields: object) -> dict[str, object]:
    payload = {
        "schema": 1,
        "status": status,
        "updated_at": datetime.now(UTC).isoformat(),
        **fields,
    }
    path = operation_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return payload


def _request(
    url: str, *, opener: OpenURL, limit: int, accept: str = "application/vnd.github+json",
) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": accept,
            "User-Agent": f"QuantMaster/{VERSION}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        response = opener(request, timeout=30)
        with response:
            expected = response.headers.get("Content-Length")
            if expected and int(expected) > limit:
                raise ReleaseStageBlocked("release_asset_too_large", "GitHub release 制品超过本地大小上限")
            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = response.read(min(1024 * 1024, limit + 1 - total))
                if not chunk:
                    break
                total += len(chunk)
                if total > limit:
                    raise ReleaseStageBlocked(
                        "release_asset_too_large", "GitHub release 制品超过本地大小上限",
                    )
                chunks.append(chunk)
            return b"".join(chunks)
    except ReleaseStageBlocked:
        raise
    except (OSError, ValueError, urllib.error.URLError) as exc:
        raise ReleaseStageBlocked("github_unavailable", "GitHub release 暂时不可用") from exc


def _json(url: str, *, opener: OpenURL) -> Mapping[str, object]:
    try:
        value = json.loads(_request(url, opener=opener, limit=MAX_METADATA_BYTES))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ReleaseStageBlocked("github_response_invalid", "GitHub 返回的 release 元数据无效") from exc
    if not isinstance(value, Mapping):
        raise ReleaseStageBlocked("github_response_invalid", "GitHub 返回的 release 元数据无效")
    return value


def _version(value: object) -> tuple[int, int, int]:
    match = VERSION_PATTERN.fullmatch(str(value or ""))
    if match is None:
        raise ReleaseStageBlocked("release_version_invalid", "GitHub release 版本号无效")
    return (
        int(match.group("major")),
        int(match.group("minor")),
        int(match.group("patch")),
    )


def _digest(asset: Mapping[str, object]) -> str:
    value = str(asset.get("digest") or "")
    if not value.startswith("sha256:") or SHA256_PATTERN.fullmatch(value[7:]) is None:
        raise ReleaseStageBlocked("release_digest_missing", "GitHub release 制品缺少 SHA-256 digest")
    return value[7:]


def _assets(release: Mapping[str, object], tag: str) -> dict[str, Mapping[str, object]]:
    raw_assets = release.get("assets")
    if not isinstance(raw_assets, list):
        raise ReleaseStageBlocked("release_assets_invalid", "GitHub release assets 无效")
    result: dict[str, Mapping[str, object]] = {}
    required = {WINDOWS_ASSET, CHECKSUMS_ASSET, SBOM_ASSET}
    prefix = f"{REPOSITORY_URL}/releases/download/{tag}/"
    for raw in raw_assets:
        if not isinstance(raw, Mapping):
            continue
        name = str(raw.get("name") or "")
        if name not in required or name in result:
            continue
        url = str(raw.get("browser_download_url") or "")
        if url != prefix + name:
            raise ReleaseStageBlocked("release_asset_url_invalid", "GitHub release 制品 URL 不受信任")
        asset_id = raw.get("id")
        api_url = str(raw.get("url") or "")
        if (
            not isinstance(asset_id, int)
            or asset_id <= 0
            or api_url != f"{API_ROOT}/releases/assets/{asset_id}"
        ):
            raise ReleaseStageBlocked("release_asset_url_invalid", "GitHub release asset API URL 不受信任")
        _digest(raw)
        result[name] = raw
    if set(result) != required:
        raise ReleaseStageBlocked(
            "release_assets_missing", "GitHub release 缺少 Windows、checksum 或 SBOM 制品",
        )
    return result


def _resolve_tag(tag: str, *, opener: OpenURL) -> str:
    ref = _json(f"{API_ROOT}/git/ref/tags/{tag}", opener=opener)
    ref_object = ref.get("object")
    if not isinstance(ref_object, Mapping) or ref_object.get("type") != "tag":
        raise ReleaseStageBlocked("release_tag_untrusted", "GitHub release 必须使用 annotated tag")
    tag_object_sha = str(ref_object.get("sha") or "")
    if FULL_SHA.fullmatch(tag_object_sha) is None:
        raise ReleaseStageBlocked("release_tag_untrusted", "GitHub release tag object 无效")
    annotated = _json(f"{API_ROOT}/git/tags/{tag_object_sha}", opener=opener)
    target = annotated.get("object")
    if not isinstance(target, Mapping) or target.get("type") != "commit":
        raise ReleaseStageBlocked("release_tag_untrusted", "GitHub release tag 未指向 commit")
    commit = str(target.get("sha") or "")
    if FULL_SHA.fullmatch(commit) is None:
        raise ReleaseStageBlocked("release_tag_untrusted", "GitHub release commit SHA 无效")
    return commit


def _download(asset: Mapping[str, object], *, opener: OpenURL, limit: int) -> bytes:
    content = _request(
        str(asset["url"]),
        opener=opener,
        limit=limit,
        accept="application/octet-stream",
    )
    if hashlib.sha256(content).hexdigest() != _digest(asset):
        raise ReleaseStageBlocked("release_digest_mismatch", "GitHub release 制品 digest 不匹配")
    return content


def _download_to(
    asset: Mapping[str, object], destination: Path, *, opener: OpenURL, limit: int,
) -> str:
    request = urllib.request.Request(
        str(asset["url"]),
        headers={
            "Accept": "application/octet-stream",
            "User-Agent": f"QuantMaster/{VERSION}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    expected_digest = _digest(asset)
    digest = hashlib.sha256()
    total = 0
    try:
        response = opener(request, timeout=30)
        with response, destination.open("xb") as stream:
            expected_size = response.headers.get("Content-Length")
            if expected_size and int(expected_size) > limit:
                raise ReleaseStageBlocked(
                    "release_asset_too_large", "GitHub release 制品超过本地大小上限",
                )
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > limit:
                    raise ReleaseStageBlocked(
                        "release_asset_too_large", "GitHub release 制品超过本地大小上限",
                    )
                digest.update(chunk)
                stream.write(chunk)
    except ReleaseStageBlocked:
        destination.unlink(missing_ok=True)
        raise
    except (OSError, ValueError, urllib.error.URLError) as exc:
        destination.unlink(missing_ok=True)
        raise ReleaseStageBlocked("github_unavailable", "GitHub release 暂时不可用") from exc
    actual = digest.hexdigest()
    if actual != expected_digest:
        destination.unlink(missing_ok=True)
        raise ReleaseStageBlocked("release_digest_mismatch", "GitHub release 制品 digest 不匹配")
    return actual


def _checksums(content: bytes) -> dict[str, str]:
    try:
        lines = content.decode("utf-8").splitlines()
    except UnicodeError as exc:
        raise ReleaseStageBlocked("release_checksums_invalid", "SHA256SUMS 不是 UTF-8 文本") from exc
    result: dict[str, str] = {}
    for line in lines:
        parts = line.split()
        if len(parts) != 2 or SHA256_PATTERN.fullmatch(parts[0]) is None:
            raise ReleaseStageBlocked("release_checksums_invalid", "SHA256SUMS 格式无效")
        name = parts[1].removeprefix("*")
        if name in result:
            raise ReleaseStageBlocked("release_checksums_invalid", "SHA256SUMS 含重复制品")
        result[name] = parts[0]
    if WINDOWS_ASSET not in result or SBOM_ASSET not in result:
        raise ReleaseStageBlocked("release_checksums_invalid", "SHA256SUMS 缺少必需制品")
    return result


def _verify_sbom(content: bytes, *, digest: str) -> None:
    try:
        sbom = json.loads(content)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ReleaseStageBlocked("release_sbom_invalid", "CycloneDX SBOM 无效") from exc
    if not isinstance(sbom, Mapping) or str(sbom.get("bomFormat") or "") != "CycloneDX":
        raise ReleaseStageBlocked("release_sbom_invalid", "release 制品缺少 CycloneDX SBOM")
    components = sbom.get("components")
    for item in components if isinstance(components, list) else []:
        if not isinstance(item, Mapping):
            continue
        hashes = item.get("hashes")
        for item_hash in hashes if isinstance(hashes, list) else []:
            if (
                isinstance(item_hash, Mapping)
                and str(item_hash.get("alg") or "").upper().replace("-", "") == "SHA256"
                and str(item_hash.get("content") or "").lower() == digest
            ):
                return
    raise ReleaseStageBlocked("release_sbom_mismatch", "SBOM 未绑定 Windows 制品 digest")


def _attestation_statement(payload: Mapping[str, object], *, digest: str) -> Mapping[str, object] | None:
    bundle = payload.get("bundle")
    envelope = bundle.get("dsseEnvelope") if isinstance(bundle, Mapping) else None
    encoded = envelope.get("payload") if isinstance(envelope, Mapping) else None
    if not isinstance(encoded, str):
        return None
    try:
        statement = json.loads(base64.b64decode(encoded, validate=True))
    except (ValueError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(statement, Mapping):
        return None
    subjects = statement.get("subject")
    for subject in subjects if isinstance(subjects, list) else []:
        subject_digest = subject.get("digest") if isinstance(subject, Mapping) else None
        if (
            isinstance(subject_digest, Mapping)
            and str(subject.get("name") or "") == WINDOWS_ASSET
            and str(subject_digest.get("sha256") or "") == digest
        ):
            return statement
    return None


def _verify_attestation(*, digest: str, tag: str, commit: str, opener: OpenURL) -> None:
    response = _json(f"{API_ROOT}/attestations/sha256:{digest}", opener=opener)
    attestations = response.get("attestations")
    for attestation in attestations if isinstance(attestations, list) else []:
        if not isinstance(attestation, Mapping):
            continue
        statement = _attestation_statement(attestation, digest=digest)
        if statement is None or statement.get("predicateType") != "https://slsa.dev/provenance/v1":
            continue
        predicate = statement.get("predicate")
        definition = predicate.get("buildDefinition") if isinstance(predicate, Mapping) else None
        external = definition.get("externalParameters") if isinstance(definition, Mapping) else None
        workflow = external.get("workflow") if isinstance(external, Mapping) else None
        internal = definition.get("internalParameters") if isinstance(definition, Mapping) else None
        github = internal.get("github") if isinstance(internal, Mapping) else None
        resolved = definition.get("resolvedDependencies") if isinstance(definition, Mapping) else None
        commit_bound = any(
            isinstance(item, Mapping)
            and isinstance(item.get("digest"), Mapping)
            and str(item["digest"].get("gitCommit") or "") == commit
            for item in resolved if isinstance(resolved, list)
        )
        try:
            repository_id = (
                int(github.get("repository_id") or 0) if isinstance(github, Mapping) else 0
            )
        except (TypeError, ValueError):
            repository_id = 0
        if (
            isinstance(workflow, Mapping)
            and str(workflow.get("repository") or "") == REPOSITORY_URL
            and str(workflow.get("path") or "").lstrip("/") == RELEASE_WORKFLOW
            and str(workflow.get("ref") or "") == f"refs/tags/{tag}"
            and isinstance(github, Mapping)
            and repository_id == REPOSITORY_ID
            and str(github.get("event_name") or "") == "push"
            and commit_bound
        ):
            return
    raise ReleaseStageBlocked("release_attestation_invalid", "GitHub provenance attestation 不匹配")


def _installed_version(registry: SlotRegistry) -> tuple[int, int, int]:
    try:
        active = str(registry.read().get("active") or "")
        meta = json.loads((registry.slot(active) / "slot_meta.json").read_text(encoding="utf-8"))
        if isinstance(meta, Mapping):
            return _version(meta.get("version"))
    except (ActivationBlocked, OSError, UnicodeError, json.JSONDecodeError, ReleaseStageBlocked):
        pass
    return _version(VERSION)


def _smoke(executable: Path) -> dict[str, object]:
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="quantmaster-release-smoke-") as raw_temp:
        root = Path(raw_temp)
        environment = os.environ.copy()
        environment.update({
            "APPDATA": str(root / "appdata"),
            "LOCALAPPDATA": str(root / "localappdata"),
            "PYINSTALLER_SUPPRESS_SPLASH_SCREEN": "1",
            "QM_FREE_STOCKDB_MANAGED": "false",
            "QM_FREE_STOCKDB_AUTO_UPDATE": "false",
            "QM_FREE_STOCKDB_ONLINE_ENABLED": "false",
            "QM_AKSHARE_ENABLED": "false",
            "QM_TUSHARE_ENABLED": "false",
            "QM_YFINANCE_ENABLED": "false",
            "QM_AUTOMATION_ENABLED": "false",
            "QM_LAB_ENABLED": "false",
        })
        for name in ("QM_BUILD_SHA", "QM_SLOT_ID", "QM_RUNTIME_GENERATION", "PYTHONPATH", "PYTHONHOME"):
            environment.pop(name, None)
        try:
            completed = subprocess.run(
                [str(executable), "--help"],
                cwd=executable.parent,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise ReleaseStageBlocked("release_smoke_failed", "Windows release 本地 smoke 启动失败") from exc
    if completed.returncode:
        raise ReleaseStageBlocked("release_smoke_failed", "Windows release 本地 smoke 未通过")
    return {"layout": "onefile", "help_ok": True, "help_seconds": round(time.monotonic() - started, 3)}


def _stage_slot(
    root: Path,
    *,
    executable: Path,
    build_sha: str,
    version: str,
    release_date: str,
    title: str,
    digest: str,
    tag: str,
) -> str:
    registry = SlotRegistry(root)
    with lifecycle_lock(root):
        registry.read()
        final = registry.slot(build_sha)
        if final.exists():
            registry.validate_candidate(build_sha)
            return "already_staged"
        registry.slots.mkdir(parents=True, exist_ok=True)
        temporary = registry.slots / f".{build_sha}.{uuid.uuid4().hex}.staging"
        try:
            temporary.mkdir()
            candidate = temporary / "QuantMaster.exe"
            shutil.copyfile(executable, candidate)
            smoke = _smoke(candidate)
            smoke.update({"build_sha": build_sha, "slot_id": build_sha})
            marker = {
                "schema": 1,
                "status": "staged",
                "source": "github-release",
                "build_sha": build_sha,
                "slot_id": build_sha,
                "release_tag": tag,
                "asset": WINDOWS_ASSET,
                "asset_sha256": digest,
                "attestation": {
                    "provider": "github",
                    "repository": REPOSITORY,
                    "repository_id": REPOSITORY_ID,
                    "workflow": RELEASE_WORKFLOW,
                    "ref": f"refs/tags/{tag}",
                    "subject_sha256": digest,
                    "verified": True,
                },
                "size": {
                    "mode": "github-release-onefile",
                    "build_sha": build_sha,
                    "bytes": candidate.stat().st_size,
                    "within_hard_limits": True,
                },
                "smoke": smoke,
                "staged_at": datetime.now(UTC).isoformat(),
            }
            (temporary / ".quantmaster-stage.json").write_text(
                json.dumps(marker, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            (temporary / "slot_meta.json").write_text(
                json.dumps({
                    "schema": 1,
                    "build_sha": build_sha,
                    "version": version,
                    "release_date": release_date,
                    "title": title,
                }, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            os.replace(temporary, final)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)
    return "staged"


def stage_latest_release(
    app_root: str | Path | None = None,
    *,
    opener: OpenURL = urllib.request.urlopen,
    os_name: str | None = None,
) -> dict[str, object]:
    """Stage the latest stable official Windows release without activating it."""

    root = Path(app_root).resolve() if app_root is not None else installed_app_root()
    if (os_name or os.name) != "nt":
        return _write_status(root, "unsupported", message="自动 release staging 目前仅支持 Windows")
    _write_status(root, "checking")
    try:
        release = _json(f"{API_ROOT}/releases/latest", opener=opener)
        if release.get("draft") is not False or release.get("prerelease") is not False:
            raise ReleaseStageBlocked("release_not_stable", "GitHub latest release 不是稳定发布")
        tag = str(release.get("tag_name") or "")
        version = tag.removeprefix("v") if tag.startswith("v") else ""
        candidate_version = _version(version)
        build_sha = _resolve_tag(tag, opener=opener)
        registry = SlotRegistry(root)
        if candidate_version <= _installed_version(registry):
            return _write_status(root, "up_to_date", version=version, build_sha=build_sha)
        final = registry.slot(build_sha)
        if final.exists():
            registry.validate_candidate(build_sha)
            return _write_status(root, "already_staged", version=version, build_sha=build_sha)
        assets = _assets(release, tag)
        checksum_content = _download(assets[CHECKSUMS_ASSET], opener=opener, limit=MAX_METADATA_BYTES)
        sbom_content = _download(assets[SBOM_ASSET], opener=opener, limit=MAX_METADATA_BYTES)
        checksums = _checksums(checksum_content)
        digest = _digest(assets[WINDOWS_ASSET])
        if checksums[WINDOWS_ASSET] != digest or checksums[SBOM_ASSET] != _digest(assets[SBOM_ASSET]):
            raise ReleaseStageBlocked(
                "release_checksums_mismatch", "SHA256SUMS 与 GitHub asset digest 不一致",
            )
        _verify_sbom(sbom_content, digest=digest)
        _verify_attestation(digest=digest, tag=tag, commit=build_sha, opener=opener)
        _write_status(root, "downloading", version=version, build_sha=build_sha)
        with tempfile.TemporaryDirectory(prefix="quantmaster-release-download-") as raw_temp:
            executable = Path(raw_temp) / WINDOWS_ASSET
            actual_digest = _download_to(
                assets[WINDOWS_ASSET], executable, opener=opener, limit=MAX_ONEFILE_BYTES,
            )
            if checksums[WINDOWS_ASSET] != actual_digest:
                raise ReleaseStageBlocked("release_checksums_mismatch", "Windows 制品与 SHA256SUMS 不一致")
            status = _stage_slot(
                root,
                executable=executable,
                build_sha=build_sha,
                version=version,
                release_date=str(release.get("published_at") or "")[:10],
                title=str(release.get("name") or tag)[:200],
                digest=digest,
                tag=tag,
            )
        return _write_status(root, status, version=version, build_sha=build_sha)
    except ReleaseStageBlocked as exc:
        return _write_status(root, "blocked", code=exc.code, message=exc.detail)
    except (ActivationBlocked, OSError, ValueError, TypeError):
        return _write_status(root, "blocked", code="release_staging_failed", message="release staging 未完成")


def start_release_staging() -> bool:
    """Start one best-effort background check in a packaged Windows process."""

    global _worker
    if os.name != "nt" or not getattr(sys, "frozen", False):
        return False
    with _worker_lock:
        if _worker is not None and _worker.is_alive():
            return False
        _worker = threading.Thread(
            target=stage_latest_release,
            name="quant-release-staging",
            daemon=True,
        )
        _worker.start()
    return True
