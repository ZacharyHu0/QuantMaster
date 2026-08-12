# QuantMaster repository instructions

## Product evolution

- QuantMaster is personal software. Unless the owner explicitly requests compatibility, prefer a
  clean replacement over legacy adapters, deprecated parameter mappings, duplicate routes, or old
  snapshot decoders. Delete obsolete paths instead of carrying historical baggage forward.
- Data integrity and recoverability still matter: a clean replacement must fail explicitly when
  evidence is unavailable and must not silently reinterpret old data as a new contract.

## Data source priority

- Whenever research needs data, inspect stockdb and existing local caches first. Only call Tushare
  or another remote provider for fields that are missing or stale locally; never report remote
  unavailability before checking the local evidence.

## Verification environment

- Run Python validation through the project virtual environment: `./.venv/Scripts/python.exe`
  on Windows (or the platform-equivalent `.venv` interpreter). Do not use a system Python for
  project tests, linting, or scripts.
- Full-only suites require `--full`. In restricted Windows environments, use a writable
  workspace-local pytest base directory, for example
  `./.venv/Scripts/python.exe -m pytest --full --basetemp .artifacts/pytest/run <target>`.

## Isolated task workflow

- Each feature, bug, or independent refactor must use one `codex/<task-slug>` branch in its own
  `.worktrees/<task-slug>` worktree. Do not mix unrelated goals in one worktree or release.
- Create and remove task worktrees with `./.venv/Scripts/python.exe scripts/dev/tasks.py start
  <slug>` and `... tasks.py remove <slug>`. Removal is allowed only after the task tree has been
  squash-integrated into `main` and the worktree is clean.
- Task branches may use small checkpoint commits. They must not edit `quantmaster/release.py` or
  `CHANGELOG.md`; those files belong only to an explicitly requested or materially valuable release
  commit on `main`.
- When a task discovers independent work, create a separate task instead of expanding the current
  diff. Resolve conflicts with current `origin/main` inside the task worktree before integration.
- `.artifacts`, pytest temporary directories, writable databases, and runtime state are local to
  one worktree. Never point concurrent worktrees at the same writable path or `--basetemp`.

## Layered verification and integration

- During development run `./.venv/Scripts/python.exe scripts/dev/tasks.py check`. The checked-in
  impact map selects adjacent contracts and invokes explicit tests with `--full`; unknown or
  infrastructure paths fail safe to the complete Python suite. Use `--staged` to inspect only the
  index or `--base <ref>` when the comparison base is not `origin/main`.
- Re-run a failure by exact pytest node id or `--last-failed`, then rerun the task impact set after
  the fix. Do not repeatedly run the complete suite during the edit loop without a concrete need.
- Before integration, commit all task changes, update to current `origin/main`, and run
  `./.venv/Scripts/python.exe scripts/dev/tasks.py ready`. Add `--ui`, `--rust`, or `--package`
  when the task affects those lanes.
- `ready` runs the complete static and Python gates. After it passes, squash exactly that one task
  into one independently revertible commit on `main`. Ordinary integration commits do not update
  version metadata or create a release.
- Refresh local duration-balanced shards with `scripts/ci/run.py --refresh-durations` when the
  slowest shard exceeds the fastest by 25%. Keep the three shard wall times within 20% when
  practical. The timing file is local cache, not release evidence.

## Release bookkeeping

- Create a release only when the owner explicitly requests one or when a materially valuable update
  warrants publication. Ordinary fixes, documentation changes, tests, and refactors may be pushed
  to `main` without changing version metadata, tagging, or publishing a GitHub Release.
- Every actual release commit must increment `VERSION` in `quantmaster/release.py` using semantic
  versioning and set `RELEASE_DATE` to the actual release date. Task branches must not touch either
  release metadata file.
- `MAJOR` is owner-controlled and must never change without a separate, explicit authorization from
  the owner. New functionality increments `MINOR`; fixes and patches increment only `PATCH`.
- Add the matching user-facing notes to `RELEASES` in `quantmaster/release.py` and to the top
  of `CHANGELOG.md` in the same change.
- `quantmaster/release.py` is the runtime version source. Do not hard-code another application
  version in Python, HTML, or JavaScript; `pyproject.toml` reads it dynamically.
- Git tags and GitHub Releases must use `v{VERSION}`. The release workflow verifies the tag and
  publishes `CHANGELOG.md` as the GitHub Release body, so those records must stay synchronized.

## Release synchronization

- Run `python scripts/release/sync.py install` once after cloning. The tracked hooks validate every
  commit and automatically push version-incrementing commits made on `main` to `origin/main`.
- A failed push leaves a pending marker inside `.git` and blocks the next release commit. Recover
  with `python scripts/release/sync.py push`; inspect the state with `python scripts/release/sync.py status`.
- Auto-push is intentionally limited to `main`. Never enable it for the archived Claude branch or
  use a release commit to move that branch.
- Do not bypass the hooks for normal project work. Ordinary commits remain independently validated
  changes; versioned release synchronization applies only to commits that update release metadata.
