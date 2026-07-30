"""Transport-neutral local-host validation shared by settings and HTTP runtime."""

from __future__ import annotations

import ipaddress
from urllib.parse import urlsplit

LOCAL_HOSTNAMES = frozenset({"localhost", "testserver", "testclient"})


def hostname(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    parsed = urlsplit(raw if "://" in raw else f"http://{raw}")
    return (parsed.hostname or "").lower().rstrip(".")


def is_loopback_host(value: str) -> bool:
    host = hostname(value)
    if host in LOCAL_HOSTNAMES:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def validate_listen_host(value: str) -> str:
    """Reject network-facing binds because the process holds private portfolio data."""
    host = value.strip().lower()
    if not is_loopback_host(host):
        raise ValueError("QuantMaster 仅允许监听本机回环地址（127.0.0.1、::1 或 localhost）")
    return host
