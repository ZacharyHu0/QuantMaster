"""Exact identity shared by one QuantMaster application process tree."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass

BUILD_SHA_ENV = "QM_BUILD_SHA"
SLOT_ID_ENV = "QM_SLOT_ID"
RUNTIME_GENERATION_ENV = "QM_RUNTIME_GENERATION"
_SOURCE_GENERATION = uuid.uuid4().hex
_HEX = "0123456789abcdef"


@dataclass(frozen=True)
class ApplicationIdentity:
    build_sha: str
    slot_id: str
    runtime_generation: str


class RuntimeIdentityMismatch(RuntimeError):
    """A child or worker belongs to a different application activation."""


def _full_sha(value: str) -> bool:
    return len(value) == 40 and all(character in _HEX for character in value)


def _validate(identity: ApplicationIdentity) -> None:
    if identity.build_sha != "source" and not _full_sha(identity.build_sha):
        raise RuntimeError("runtime_identity_invalid_build_sha")
    if not identity.slot_id or identity.slot_id.strip() != identity.slot_id:
        raise RuntimeError("runtime_identity_invalid_slot_id")
    if len(identity.runtime_generation) != 32 or any(
        character not in _HEX for character in identity.runtime_generation
    ):
        raise RuntimeError("runtime_identity_invalid_generation")


def bind_packaged_build(build_sha: str) -> None:
    """Bind an immutable packaged commit before application imports start."""

    value = str(build_sha).strip()
    if not _full_sha(value):
        raise RuntimeError("runtime_identity_invalid_build_sha")
    os.environ[BUILD_SHA_ENV] = value
    os.environ.setdefault(SLOT_ID_ENV, value)


def get_application_identity() -> ApplicationIdentity:
    """Return and establish the identity inherited by future child processes."""

    identity = ApplicationIdentity(
        os.environ.setdefault(BUILD_SHA_ENV, "source"),
        os.environ.setdefault(SLOT_ID_ENV, "source"),
        os.environ.setdefault(RUNTIME_GENERATION_ENV, _SOURCE_GENERATION),
    )
    _validate(identity)
    return identity


def require_application_identity(expected: ApplicationIdentity) -> ApplicationIdentity:
    """Return the current identity or reject a mixed application process tree."""

    current = get_application_identity()
    if current != expected:
        raise RuntimeIdentityMismatch("runtime_identity_mismatch")
    return current
