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


class SecurityViolation(HTTPException):
    """A local HTTP boundary rejection with a stable machine-readable code."""

    def __init__(self, code: str, detail: str, *, action: str = "请刷新本机页面后重试。"):
        super().__init__(status_code=403, detail=detail)
        self.code = code
        self.action = action


def is_local_request(request: Request) -> bool:
    client = request.client.host if request.client else ""
    return is_loopback_host(client)


def require_local(request: Request) -> None:
    if not is_local_request(request):
        raise SecurityViolation(
            "client_not_loopback",
            "QuantMaster 页面和业务接口仅允许从本机访问",
            action="请仅从运行 QuantMaster 的本机访问。",
        )
    if not is_loopback_host(request.headers.get("host", "")):
        raise SecurityViolation(
            "host_rejected",
            "拒绝不可信的 Host 请求头",
            action="请使用 127.0.0.1 或 localhost 打开 QuantMaster。",
        )


def require_same_origin(request: Request) -> None:
    """Reject an explicit Origin that does not match the request Host."""
    origin = request.headers.get("origin", "")
    if origin and _hostname(origin) != _hostname(request.headers.get("host", "")):
        raise SecurityViolation(
            "origin_rejected",
            "拒绝不可信的 Origin 请求头",
            action="请仅从当前 QuantMaster 页面发起请求。",
        )


def issue_csrf() -> str:
    expires = int(time.time()) + CSRF_TTL_SECONDS
    payload = f"{expires}.{secrets.token_urlsafe(24)}"
    signature = hmac.new(_CSRF_SECRET, payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{signature}"


def _csrf_state(token: str) -> str:
    if not token:
        return "missing"
    try:
        expires_raw, nonce, signature = token.split(".", 2)
        expires = int(expires_raw)
    except (TypeError, ValueError):
        return "invalid"
    if not nonce:
        return "invalid"
    if expires < int(time.time()):
        return "expired"
    payload = f"{expires}.{nonce}"
    expected = hmac.new(_CSRF_SECRET, payload.encode(), hashlib.sha256).hexdigest()
    return "valid" if hmac.compare_digest(signature, expected) else "invalid"


def _valid_csrf(token: str) -> bool:
    return _csrf_state(token) == "valid"


def require_csrf(request: Request) -> None:
    require_local(request)
    require_same_origin(request)

    header = request.headers.get("x-csrf-token", "")
    cookie = request.cookies.get("qm_csrf", "")
    if not header or not cookie:
        raise SecurityViolation("csrf_missing", "CSRF 令牌缺失；页面将自动续签后重试")
    if not hmac.compare_digest(header, cookie):
        raise SecurityViolation("csrf_mismatch", "CSRF 令牌不一致；页面将自动续签后重试")
    state = _csrf_state(header)
    if state == "expired":
        raise SecurityViolation("csrf_expired", "CSRF 令牌已过期；页面将自动续签后重试")
    if state != "valid":
        raise SecurityViolation("csrf_invalid", "CSRF 令牌已失效；页面将自动续签后重试")


def enforce_request_security(request: Request) -> None:
    """Apply one complete policy before any route handler can run."""
    require_local(request)
    require_same_origin(request)
    if request.url.path.startswith("/api/v1/") and request.method.upper() in UNSAFE_METHODS:
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


def ensure_csrf_cookie(response: Response, request: Request) -> str:
    """刷新页面时替换已过期或因服务重启而失效的浏览器令牌。"""
    token = request.cookies.get("qm_csrf", "")
    if not _valid_csrf(token):
        token = issue_csrf()
        attach_csrf_cookie(response, request, token)
    return token


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
