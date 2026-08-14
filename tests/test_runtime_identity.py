from __future__ import annotations

import os

import pytest

FULL_SHA = "0123456789abcdef0123456789abcdef01234567"


def test_source_identity_has_one_root_generation(monkeypatch):
    from quantmaster.runtime.identity import (
        BUILD_SHA_ENV,
        RUNTIME_GENERATION_ENV,
        SLOT_ID_ENV,
        get_application_identity,
    )

    for name in (BUILD_SHA_ENV, SLOT_ID_ENV, RUNTIME_GENERATION_ENV):
        monkeypatch.delenv(name, raising=False)

    first = get_application_identity()
    second = get_application_identity()

    assert first.build_sha == "source"
    assert first.slot_id == "source"
    assert len(first.runtime_generation) == 32
    assert first == second


def test_packaged_build_binds_full_sha_before_runtime_start(monkeypatch):
    from quantmaster.runtime.identity import (
        BUILD_SHA_ENV,
        RUNTIME_GENERATION_ENV,
        SLOT_ID_ENV,
        bind_packaged_build,
        get_application_identity,
    )

    for name in (BUILD_SHA_ENV, SLOT_ID_ENV, RUNTIME_GENERATION_ENV):
        monkeypatch.delenv(name, raising=False)

    bind_packaged_build(FULL_SHA)

    identity = get_application_identity()
    assert identity.build_sha == FULL_SHA
    assert identity.slot_id == FULL_SHA
    assert len(identity.runtime_generation) == 32


def test_required_identity_rejects_a_different_runtime_generation(monkeypatch):
    from quantmaster.runtime.identity import (
        BUILD_SHA_ENV,
        RUNTIME_GENERATION_ENV,
        SLOT_ID_ENV,
        ApplicationIdentity,
        require_application_identity,
    )

    monkeypatch.setenv(BUILD_SHA_ENV, FULL_SHA)
    monkeypatch.setenv(SLOT_ID_ENV, FULL_SHA)
    monkeypatch.setenv(RUNTIME_GENERATION_ENV, "a" * 32)

    expected = ApplicationIdentity(FULL_SHA, FULL_SHA, "b" * 32)
    with pytest.raises(RuntimeError, match="runtime_identity_mismatch"):
        require_application_identity(expected)


@pytest.mark.parametrize(
    ("build_sha", "slot_id", "generation", "code"),
    [
        ("short", FULL_SHA, "a" * 32, "runtime_identity_invalid_build_sha"),
        (FULL_SHA, "", "a" * 32, "runtime_identity_invalid_slot_id"),
        (FULL_SHA, FULL_SHA, "not-a-uuid", "runtime_identity_invalid_generation"),
    ],
)
def test_malformed_explicit_identity_fails_closed(
    monkeypatch, build_sha, slot_id, generation, code,
):
    from quantmaster.runtime.identity import (
        BUILD_SHA_ENV,
        RUNTIME_GENERATION_ENV,
        SLOT_ID_ENV,
        get_application_identity,
    )

    monkeypatch.setenv(BUILD_SHA_ENV, build_sha)
    monkeypatch.setenv(SLOT_ID_ENV, slot_id)
    monkeypatch.setenv(RUNTIME_GENERATION_ENV, generation)

    with pytest.raises(RuntimeError, match=code):
        get_application_identity()


def test_worker_composition_root_establishes_identity_before_spawn(
    monkeypatch, isolated_config,
):
    from quantmaster.bootstrap import get_worker_supervisor
    from quantmaster.runtime.identity import (
        BUILD_SHA_ENV,
        RUNTIME_GENERATION_ENV,
        SLOT_ID_ENV,
    )

    for name in (BUILD_SHA_ENV, SLOT_ID_ENV, RUNTIME_GENERATION_ENV):
        monkeypatch.delenv(name, raising=False)

    get_worker_supervisor()

    assert os.environ[BUILD_SHA_ENV] == "source"
    assert os.environ[SLOT_ID_ENV] == "source"
    assert len(os.environ[RUNTIME_GENERATION_ENV]) == 32
