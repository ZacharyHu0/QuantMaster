# API error contract

Every API failure is a redacted envelope with `code`, `message`, `problem`,
`field` when applicable, `request_id`/`diagnostic_id`, `retryable`, optional
`retry_after`, and `suggestion`. `X-Request-ID` is the same identifier. Request
validation never returns rejected values or Pydantic context.

`GET /api/v1/health` is the only health probe. HTTP 200 remains process
liveness; the response also carries the lightweight, store-free `core_ready` and
`readiness_status` projection. It does not run full diagnostics or contact
news, rotation, LLM, Feishu, or a provider. Optional component state is reported
by `GET /api/v1/diagnostics`; `/health/live` and `/health/ready` were removed.

Evidence found in local logs (parameters are intentionally not logged):

| Endpoint | Historical result | Cause / current contract |
| --- | --- | --- |
| `GET /api/v1/health` | 404 ×24 (Aug 9–10) | obsolete probe; now the sole 200 probe |
| news event focus | 422 ×19 | `days` is strictly `1,3,7,30` |
| rotation themes / ETF flow items | 422 ×9 / ×12 | strict window, paging and enum contract |
| news stats / event focus / sources | 500 ×5 / ×2 / ×1 | SQLite lock; now 503 with retry guidance |
| selection history | 500 ×2 | removed payload-hash gate; malformed evidence stays explicit degraded data |

Security rejections retain their boundary codes (`csrf_*`, `origin_rejected`,
`client_not_loopback`, `host_rejected`, `permission_denied`) and are expected
403s: they are warning-logged without a traceback and must not be retried except
for the browser's single CSRF-refresh retry.
