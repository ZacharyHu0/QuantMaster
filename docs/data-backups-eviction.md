# data/backups Eviction

`data/backups/` previously held ~54,000 untracked files totaling ~7.2 GB:
a dump of prior site-packages (websockets, win32ctypes, xlrd, yfinance,
_pytest, packaging, etc.) plus a full quarantined copy of an earlier
codebase checkout.

**Evicted on 2026-08-17** — 52,718 files deleted. The directory is now
empty (3 residual Windows-locked `.pytest-tmp` stubs remain, harmless).

`data/` is excluded from git by `.gitignore`; no tracked content was lost.
No source module, test, or script referenced a path under `data/backups/`
(verified by full-tree scan).
