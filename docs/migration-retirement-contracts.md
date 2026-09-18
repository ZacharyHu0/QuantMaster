# Migration retirement contracts

This inventory separates durable migration behavior from the source files that happen to
contain it. The `# for_version` comments in `quantmaster/data/migration.py` describe code
provenance; their count is not a supported product contract. The persisted domain IDs returned
by `registered_migrations()` and the behavior of each planner are supported contracts.

## Infrastructure that remains active

`DataMigrationManager` copies and switches the configured data root. `LegacyMigrationManager`,
the backup manifest helpers, and the `DomainMigrator` registry provide read-only planning,
offline apply, rollback, and resumable audit state. These are current operational
infrastructure, not historical converters. They remain while the product supports data-root
moves or any registered migration domain.

Every registered planner must inspect its source through read-only connections, preserve
unknown fields as evidence, and return a conflict or review result when it cannot identify a
supported source generation. Planning must not create the migration state database, backup
directories, source schemas, or target schemas. Apply remains the only path allowed to mutate a
store, after the runner has captured its declared `backup_paths`.

## Registered domain inventory

“Current schema” domains upgrade stores that are still written by the product. “One-time”
domains convert a retired representation into a current store or quarantine it. “Hybrid”
domains do both.

| Persisted domain ID | Class and kind | Confirmed source | Target or supported result | Retirement condition |
| --- | --- | --- | --- | --- |
| `remaining-schema` | `RemainingSchemaMigrator`; current schema | `source_health.sqlite`, `tushare_rate.sqlite`, Research Lake catalog, bar metadata stores, and standalone or account ledgers | Current schema versions for the same stores | Keep while these stores can be opened from an older supported release. Retire only after the oldest supported installation already writes every current schema and retained recovery copies have no older supported generation. |
| `startup-schema` | `StartupSchemaMigrator`; current schema | `jobs.sqlite` and `paper.sqlite` | Current unified-job and paper-store schemas | Keep while startup can encounter a supported older jobs or paper schema. Retirement needs a declared minimum installation version plus zero applicable local and recovery inventory. |
| `store-schema` | `StoreSchemaMigrator`; current schema | `lab.sqlite`, `rotation/cache.sqlite`, and `rotation/preferences.sqlite` | Current Lab and Rotation schemas | Keep while those live stores have supported in-place upgrades. Retirement needs a declared minimum installation version plus zero applicable local and recovery inventory. |
| `automation-contract-v9` | `AutomationContractMigrator`; hybrid | `automation.sqlite` schema v6-v11, exact retired default schedules, and Feishu rows missing an explicit secret target | Automation schema v12; current schedules; credentials left explicitly unconfigured when provenance is unavailable | Keep while v6-v11 stores are supported or exact retired defaults/credential gaps can remain. A current v12 database alone does not prove the converter can be removed. |
| `news` | `NewsContractMigrator`; hybrid | `news.sqlite`, older supported news schemas, four exact `*_legacy_v3` archive tables, and current rows missing optional evidence | Current news schema; archive tables removed only with the runner backup; unavailable optional facts remain blank | Keep while older news schemas or v3 archive tables may exist in local or recovery inventory. Removal also requires retained backups to preserve the last recovery route for archive retirement. |
| `market_data` | `MarketDataLegacyMigrator`; one-time | Legacy bar filenames, stock-name history, index-weight cache files, current-only industry JSON, and retired Rotation ETF artifacts | Canonical current files where identity is provable; ambiguous material is quarantined or reported | Retire after read-only plans report no applicable record in every supported data root and retained recovery copy, and recovery no longer depends on these converters. |
| `decision` | `DecisionLegacyMigrator`; one-time | Rows in `decisions.sqlite.selection_snapshots` without the current decision payload schema | Current payloads in the same rows; unclassifiable or identity-conflicting rows remain explicit | Retire after all supported roots and retained recovery copies contain only validated current payloads and the migration audit/backup remains sufficient for recovery. |
| `after_close` | `AfterCloseLegacyMigrator`; one-time | Snapshot payload schema 1.0 or 1.1 in `after_close.sqlite` | Current after-close snapshot payload; unavailable optional facts remain blank | Retire after read-only inspection of all supported roots and retained recovery copies finds no 1.0/1.1 payload and recovery no longer needs the converter. |
| `data-jobs` | `DataJobLegacyMigrator`; one-time | Lifecycle tables in `data_refresh.sqlite` and `data_repairs.sqlite` | Durable jobs, events, and artifacts in `jobs.sqlite`; retired source tables no longer plan work | Retire after no supported root or recovery copy contains either legacy lifecycle table and imported jobs have verified target identities. |
| `backtest-jobs` | `BacktestJobLegacyMigrator`; one-time | Legacy lifecycle rows in `backtests.sqlite` plus `backtests/` artifacts | Unified lifecycle in `jobs.sqlite` and current backtest result/artifact representation | Retire after all supported roots and recovery copies lack legacy backtest lifecycle rows and target collision checks remain clean. |
| `paper-ledger` | `PaperLegacyMigrator`; one-time | Standalone `ledger_paper.sqlite` | A paused imported account in `paper.sqlite` with its ledger under `paper_accounts/`; unrecoverable strategy metadata stays blank | Retire after the standalone ledger is absent or has a verified `paper_legacy_imports` receipt in every supported root and recovery copy. |
| `lab-jobs` | `LabJobLegacyMigrator`; one-time | Lab lifecycle tables in `lab.sqlite` (supported v11/current layouts) and linked Lab artifacts | Unified lifecycle in `jobs.sqlite`; domain results retained in current `lab.sqlite` | Retire after all supported roots and recovery copies lack the legacy Lab lifecycle tables and imported job/artifact/domain links verify without collisions. |
| `research-jobs` | `ResearchJobLegacyMigrator`; one-time | `research_jobs` and `research_job_events` in the Research Lake catalog | Unified lifecycle in `jobs.sqlite`; current catalog keeps research domain tables | Retire after all supported roots and recovery copies lack both legacy lifecycle tables and imported provenance links verify without collisions. |
| `lab-model-artifact` | `LabModelArtifactMigrator`; one-time isolation | Lab model manifests with the retired v1 artifact contract | References archived as unavailable and the artifact directory moved under `migration_quarantine/lab_models` | Retire after no supported root or recovery copy contains a v1 manifest, and the quarantine plus database backup remains sufficient for recovery. |

The local read-only inventory captured for this audit reported all fourteen domain IDs. It
confirmed current versions for `lab.sqlite` (12), `automation.sqlite` (12), `paper.sqlite` (6),
`source_health.sqlite` (4), and the inspected ledgers (1). It also recorded completed historical
runs for several one-time domains. That evidence does not establish zero applicability: stores
whose version is encoded in tables or metadata require their domain planner, and retained backup
or recovery roots were not exhausted. Therefore this audit authorizes no converter deletion and
introduces no guessed minimum version.

## Executable evidence

The registry test in `tests/test_legacy_contract_migration.py` calls `plan()` for every persisted
domain against an empty isolated root and proves that planning creates no files. The same module
tests backup-boundary inventory, explicit unknown-domain rejection, apply conflict blocking, and
rollback.

Supported upgrades and idempotence are exercised by
`tests/test_startup_schema_migration.py` and `tests/test_store_schema_migration.py`. Those modules
also prove that unknown or damaged generations produce conflicts and retain their original data.
The domain migration test modules cover their own source-to-target conversion and recovery rules.
These behavior checks replace the deleted AST manifest test that required exactly fifteen source
sections.
