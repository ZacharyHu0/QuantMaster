"""QuantMaster 发布元数据的运行期预检。

这里提供不依赖 Git 上下文的纯数据校验：版本号格式、发布日期、以及
``RELEASES`` 条目结构。带 Git / CHANGELOG 上下文的发布门禁继续由
``scripts/release/sync.py`` 拥有，本模块不重复实现。
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from datetime import date

from quantmaster.release import RELEASE_DATE, RELEASES, VERSION

_SEMVER = re.compile(r"^\d+\.\d+\.\d+$")
_ENTRY_FIELDS = ("version", "date", "sections")


def parse_semver(value: object) -> tuple[int, int, int]:
    """Parse ``major.minor.patch`` into an int tuple, raising on invalid input."""

    text = str(value or "").strip()
    if _SEMVER.fullmatch(text) is None:
        raise ValueError(f"不是有效的语义版本号：{text!r}")
    major, minor, patch = text.split(".")
    return int(major), int(minor), int(patch)


def release_date(release_date_value: object) -> date:
    """Parse an ISO release date, raising on invalid input."""

    text = str(release_date_value or "").strip()
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"不是有效的发布日期：{text!r}") from exc


def _validate_identity_fields(entry: Mapping[object, object]) -> list[str]:
    errors = [f"发布条目缺少字段：{field}" for field in _ENTRY_FIELDS if field not in entry]
    if "version" in entry:
        try:
            parse_semver(entry.get("version"))
        except ValueError as exc:
            errors.append(str(exc))
    if "date" in entry:
        try:
            release_date(entry.get("date"))
        except ValueError as exc:
            errors.append(str(exc))
    return errors


def _validate_sections(sections: object) -> list[str]:
    if not isinstance(sections, Iterable):
        return ["发布条目的 sections 必须是可迭代对象"]
    errors: list[str] = []
    for section in sections:
        if not isinstance(section, Mapping):
            errors.append("sections 的每一项必须是 mapping")
            continue
        if not isinstance(section.get("title"), str) or not str(section["title"]).strip():
            errors.append("section 缺少非空 title")
        items = section.get("items")
        if not isinstance(items, Iterable):
            errors.append("section 的 items 必须是可迭代对象")
        elif not any(isinstance(item, str) and item.strip() for item in items):
            errors.append("section 的 items 至少需要一条非空文本")
    return errors


def validate_release_entry(entry: object) -> list[str]:
    """Return user-facing errors for a single ``RELEASES`` entry."""

    if not isinstance(entry, Mapping):
        return ["发布条目必须是 mapping"]
    errors = _validate_identity_fields(entry)
    errors.extend(_validate_sections(entry.get("sections")))
    return errors


def validate_release_metadata() -> list[str]:
    """Validate the imported runtime release metadata; empty list means valid."""

    errors: list[str] = []
    try:
        parse_semver(VERSION)
    except ValueError as exc:
        errors.append(f"VERSION：{exc}")
    try:
        release_date(RELEASE_DATE)
    except ValueError as exc:
        errors.append(f"RELEASE_DATE：{exc}")
    if not isinstance(RELEASES, Iterable) or isinstance(RELEASES, (str, bytes)):
        errors.append("RELEASES 必须是条目可迭代对象")
        return errors
    entries = list(RELEASES)
    if not entries:
        errors.append("RELEASES 至少需要一条发布记录")
        return errors
    for index, entry in enumerate(entries):
        for error in validate_release_entry(entry):
            errors.append(f"RELEASES[{index}]：{error}")
    first = entries[0]
    if isinstance(first, Mapping):
        if first.get("version") != VERSION:
            errors.append("RELEASES 第一项必须使用 VERSION")
        if first.get("date") != RELEASE_DATE:
            errors.append("RELEASES 第一项必须使用 RELEASE_DATE")
    return errors


def release_metadata_is_valid() -> tuple[bool, list[str]]:
    """Return ``(valid, errors)`` for the imported runtime release metadata."""

    errors = validate_release_metadata()
    return not errors, errors
