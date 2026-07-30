"""Strict industry taxonomy helpers used by rotation analytics.

The legacy ``industry_map.json`` may contain several unrelated taxonomies.  Rotation
only accepts exact SW2021 level-one names here; level-two nodes are loaded from the
dedicated rotation taxonomy store and never alter level-one totals.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SW2021_L1: tuple[tuple[str, str], ...] = (
    ("801010.SI", "农林牧渔"),
    ("801030.SI", "基础化工"),
    ("801040.SI", "钢铁"),
    ("801050.SI", "有色金属"),
    ("801080.SI", "电子"),
    ("801110.SI", "家用电器"),
    ("801120.SI", "食品饮料"),
    ("801130.SI", "纺织服饰"),
    ("801140.SI", "轻工制造"),
    ("801150.SI", "医药生物"),
    ("801160.SI", "公用事业"),
    ("801170.SI", "交通运输"),
    ("801180.SI", "房地产"),
    ("801200.SI", "商贸零售"),
    ("801210.SI", "社会服务"),
    ("801230.SI", "综合"),
    ("801710.SI", "建筑材料"),
    ("801720.SI", "建筑装饰"),
    ("801730.SI", "电力设备"),
    ("801740.SI", "国防军工"),
    ("801750.SI", "计算机"),
    ("801760.SI", "传媒"),
    ("801770.SI", "通信"),
    ("801780.SI", "银行"),
    ("801790.SI", "非银金融"),
    ("801880.SI", "汽车"),
    ("801890.SI", "机械设备"),
    ("801950.SI", "煤炭"),
    ("801960.SI", "石油石化"),
    ("801970.SI", "环保"),
    ("801980.SI", "美容护理"),
)

_L1_CODE_BY_NAME = {name: code for code, name in SW2021_L1}


def strict_l1_groups(mapping: Mapping[str, str]) -> dict[str, dict[str, Any]]:
    """Return only exact SW2021 L1 memberships from a possibly mixed cache."""
    grouped: dict[str, list[str]] = {code: [] for code, _ in SW2021_L1}
    for symbol, raw_name in mapping.items():
        name = str(raw_name).strip()
        code = _L1_CODE_BY_NAME.get(name)
        if code and str(symbol).upper().endswith((".SH", ".SZ", ".BJ")):
            grouped[code].append(str(symbol).upper())
    return {
        code: {
            "code": code,
            "name": name,
            "level": "L1",
            "parent_code": "",
            "members": sorted(set(grouped[code])),
            "source": "SW2021",
        }
        for code, name in SW2021_L1
    }


def merge_l2_groups(
    l1_groups: Mapping[str, Mapping[str, Any]],
    stored_nodes: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Validate dedicated L2 nodes without changing any L1 membership."""
    result: dict[str, dict[str, Any]] = {}
    for node in stored_nodes:
        code = str(node.get("code") or "").strip().upper()
        parent = str(node.get("parent_code") or "").strip().upper()
        name = str(node.get("name") or "").strip()
        members = node.get("members") or []
        if not code or not name or parent not in l1_groups or code in result:
            continue
        allowed = {
            str(symbol).upper() for symbol in members
            if str(symbol).upper().endswith((".SH", ".SZ", ".BJ"))
        }
        result[code] = {
            "code": code,
            "name": name,
            "level": "L2",
            "parent_code": parent,
            "members": sorted(allowed),
            "source": "SW2021",
        }
    return result


def taxonomy_payload(
    l1_groups: Mapping[str, Mapping[str, Any]],
    l2_groups: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    def public(node: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "code": str(node["code"]),
            "name": str(node["name"]),
            "level": str(node["level"]),
            "parent_code": str(node.get("parent_code") or ""),
            "member_count": len(node.get("members") or []),
            "source": str(node.get("source") or "SW2021"),
        }

    return {
        "version": "SW2021",
        "l1": [public(node) for node in l1_groups.values()],
        "l2": [public(node) for node in l2_groups.values()],
    }
