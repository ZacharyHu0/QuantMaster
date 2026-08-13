# Cache governance contract

This document is the inventory and ownership map for QuantMaster caches.  The executable
result, key, negative-cache, revision and cleanup contracts live in
`quantmaster.data.cache_contracts`.

## Result contract

| Result | Meaning | Normal batch cache | Negative cache | Required follow-up |
|---|---|---:|---:|---|
| `success` | Semantically validated complete response | yes | no | normal freshness rule |
| `partial` | Some requested items/pages succeeded | no | no | persist successful items and durable pending failures |
| `empty_valid` | Provider contract proves the complete requested set is empty | yes | no | normal freshness rule |
| `not_found` | Authoritative source proves the named resource does not exist | no | yes | reason/source/observed/expiry required |
| `rate_limited` | HTTP 429 or documented rate limit | no | no | retry after provider recovery |
| `temporary_failure` | timeout, DNS, TLS, connection or HTTP 5xx | no | no | retry; stale display data may be explicit |
| `invalid_response` | wrong content type, login/captcha/error HTML, schema or parse failure | no | no | keep raw evidence, fix/replace parser |
| `permission_denied` | authentication, authorization or entitlement failure | no | no | configuration/operator action |

An empty container does not identify a result.  Callers must prove `empty_valid`; otherwise
it remains a failure or partial response.  `not_found` is the only negative-cache write
result.  New listings and not-yet-published resources use the namespace's provisional short
negative TTL, not the long confirmed-absence TTL.

## Namespace inventory

| Namespace / existing storage | Key and value | Freshness / write condition | Main callers | Revision / cleanup |
|---|---|---|---|---|
| `stockdb.raw`; `data/stockdb-ingest`, managed free-stockdb | provider generation, symbol/market, requested sessions, completeness; raw bars/catalog evidence | generation and requested trading sessions; write only validated item/chunk | data registry, after-close, research lake, Lab | stockdb generation; preserve the only raw evidence |
| `provider.raw`; `data/api_cache/<provider>` | provider + endpoint + all parameters/range/page/config revision; raw response/frame | endpoint trading-session or publication rule; validated HTTP/status/schema | Tushare/AKShare adapters, rotation, research | provider config; retain raw for parser replay |
| `provider.normalized` | raw identity plus parser revision, adjustment, currency/unit | compatible raw freshness and parser revision | market/fundamental consumers | parser-only change reparses raw; config change refetches |
| `news.raw`; `data/news_raw`, `news_http_cache`, raw manifest | source + URL/provider item/request page/config revision; gzip HTTP evidence | HTTP validators and publication cadence; only valid 2xx/304 response contracts | news providers/crawler/claims | source config; referenced unique raw is not deleted |
| `news.normalized`; news SQLite rows/revisions | raw binding + parser version + article identity | raw compatibility and parser revision; complete list/page evidence | news crawler, analysis and formal claims | parser change replays raw; never borrow another article's raw |
| `market.bars`; `data/bars`, rotation cache | provider, symbol/market, timeframe/range/as-of, adjustment, currency/unit, revisions | market calendar/close boundary; formal use bounded by `as_of` | registry, analysis, after-close, Lab, research | provider/parser/calendar; stale display is explicit |
| `industry.catalog`; `industry_map.json`, board/industry caches | provider, market, taxonomy, as-of, filters/config revision | taxonomy release event and observation time | analysis, rotation, security master | provider/taxonomy; confirmed absence only may be negative |
| `instrument.catalog`; security master and catalog snapshots | provider, market, as-of, listing status/page/completeness/config | listing/delisting publication event | resolver, universe, ingest completeness | provider/catalog schema; new listing uses short negative TTL |
| `model.catalog`; Lab/research model metadata | backend, model/artifact identity, config/schema revisions | artifact publication/removal event | Lab capability/preflight/service | model config/schema; unavailable backend is not model absence |
| `capability.probe`; source health/runtime status | provider lane, capability, config revision | short probe TTL and provider recovery | registry, settings/readiness/doctor | invalidate only changed provider lane |
| `lab.panel` and feature caches | provider/universe, market, timeframe/range/as-of, adjustment, unit/currency and revisions | immutable historical inputs; no rows after `as_of` | Lab dataset/ML/multihorizon | provider/parser/calendar/universe; regenerable LRU/budget cleanup |
| market display snapshots (`fear_greed`, overview, rotation overview) | explicit view/source generation | short display freshness; SWR may expose age and refresh issue | dashboard/automation | source generation; regenerable and low retention priority |

The current endpoint Parquet cache uses a deterministic digest only as a filesystem filename.
New business identity is the readable `CacheKey.canonical()` contract; this project does not
add validation hashes, hash tags or an automatic hash invalidation mechanism.

## Key and invalidation rules

A key includes every business parameter which can change a result: provider, resource,
symbol/market, timeframe/range/`as_of`, taxonomy, adjustment, currency/unit, filters,
pagination/completeness and monotonic config/parser revisions where those dependencies exist.
Defaults are materialized before key construction.  Two semantically incompatible keys cannot
share a normalized entry.

Invalidation is dependency-scoped:

- provider credentials/config revision invalidates only entries depending on that provider;
- parser-only changes invalidate normalized output and prefer local raw replay;
- taxonomy/catalog/calendar revisions invalidate only dependent namespaces;
- a negative record with an incompatible dependency revision is deleted on read;
- no workflow clears every cache because one dependency changed.

## Freshness policy

| Data purpose | Freshness basis | Stale use |
|---|---|---|
| intraday/display | market session and small endpoint-specific interval | SWR allowed with age, refresh problem and stale consumer disclosed |
| daily bars | exchange calendar and post-close availability | display may use disclosed stale; formal read must cover requested sessions |
| catalogs/taxonomy | listing or taxonomy publication event and observation time | current display may be stale; historical requires point-in-time evidence |
| fundamentals | report announcement/update event, not a uniform day TTL | formal result includes only announcements at or before `as_of` |
| news | source publication cadence plus HTTP validators | stale list may display; formal claim requires recoverable bound raw |
| historical/formal research | immutable inputs compatible with exact `as_of` | never admit future observations or silently stale/incomplete inputs |

## Partial persistence and cleanup

Multi-symbol and paginated fetches persist each validated item or safe chunk atomically.  The
batch is `partial` until every expected item/page is accounted for.  Missing items enter a
durable pending queue with a concrete result/failure reason; retries request only pending
identities.  An omitted item is never synthesized as `empty_valid` or `not_found`.

Cleanup first partitions by namespace and evidence value.  It excludes active files,
authoritative databases, pending writes and the only raw evidence.  Among regenerable entries it
uses last access and disk budget, deletes files before now-empty cache directories only when the
directory is owned and no references remain, and re-checks active use immediately before each
deletion.  `plan_cache_cleanup` is deliberately read-only; callers own safe deletion.

## Local evidence audit, 2026-08-13

The primary instance was inspected read-only.  `data/api_cache/tushare` contained 59,951 Parquet
files (about 1.27 GB); the existing format has no sidecar result kind, semantic validation status,
dependency revisions or negative reason, so absence/permission/parse pollution cannot be proven
or repaired safely from filenames alone.  A metadata-only scan over all files exceeded two
minutes; no files were modified.  Logs contain repeated free-stockdb connect timeouts and news
source `WinError 10013` connection failures, plus stale market-cache fallbacks and incomplete
requested-symbol coverage.  These are temporary failures and partial outcomes under this
contract, never legal empty or negative results.

Pollution recovery therefore remains `0` records: the task did not infer invalidity from an empty
frame or filename and did not mutate the live instance without a maintenance barrier, exact DB
confirmation and backup.  New writes are protected by executable validation/result contracts;
legacy cache repair needs a separately confirmed instance-data operation after a scanner can
produce deterministic semantic evidence.
