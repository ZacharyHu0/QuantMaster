"""Local-only HTTP boundary and stateless CSRF protection."""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time

from fastapi import HTTPException, Request, Response

from quantmaster.runtime.network import hostname as _hostname
from quantmaster.runtime.network import is_loopback_host
from quantmaster.runtime.network import validate_listen_host as validate_listen_host

UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
CSRF_TTL_SECONDS = 8 * 60 * 60
_CSRF_SECRET = secrets.token_bytes(32)
def is_local_request(request: Request) -> bool:
    client = request.client.host if request.client else ""
    return is_loopback_host(client)


def require_local(request: Request) -> None:
    if not is_local_request(request):
        raise HTTPException(403, "QuantMaster 页面和业务接口仅允许从本机访问")
    if not is_loopback_host(request.headers.get("host", "")):
        raise HTTPException(403, "拒绝不可信的 Host 请求头")


def issue_csrf() -> str:
    expires = int(time.time()) + CSRF_TTL_SECONDS
    payload = f"{expires}.{secrets.token_urlsafe(24)}"
    signature = hmac.new(_CSRF_SECRET, payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{signature}"


def _valid_csrf(token: str) -> bool:
    try:
        expires_raw, nonce, signature = token.split(".", 2)
        expires = int(expires_raw)
    except (TypeError, ValueError):
        return False
    if not nonce or expires < int(time.time()):
        return False
    payload = f"{expires}.{nonce}"
    expected = hmac.new(_CSRF_SECRET, payload.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature, expected)


def require_csrf(request: Request) -> None:
    require_local(request)
    header = request.headers.get("x-csrf-token", "")
    cookie = request.cookies.get("qm_csrf", "")
    if not header or not cookie or not hmac.compare_digest(header, cookie):
        raise HTTPException(403, "CSRF 令牌缺失或无效；请刷新页面后重试")
    if not _valid_csrf(header):
        raise HTTPException(403, "CSRF 令牌已过期；请刷新页面后重试")

    origin = request.headers.get("origin", "")
    if origin and _hostname(origin) != _hostname(request.headers.get("host", "")):
        raise HTTPException(403, "拒绝跨来源写入请求")


def enforce_request_security(request: Request) -> None:
    """Apply one complete policy before any route handler can run."""
    require_local(request)
    if request.url.path.startswith("/api/") and request.method.upper() in UNSAFE_METHODS:
        require_csrf(request)


def attach_csrf_cookie(response: Response, request: Request, token: str) -> None:
    response.set_cookie(
        "qm_csrf",
        token,
        httponly=False,
        samesite="strict",
        secure=request.url.scheme == "https",
        max_age=CSRF_TTL_SECONDS,
        path="/",
    )


def apply_security_headers(response: Response) -> None:
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
    response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
    response.headers.setdefault(
        "Permissions-Policy",
        "camera=(), microphone=(), geolocation=(), payment=(), usb=()",
    )
