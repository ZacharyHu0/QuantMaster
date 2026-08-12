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
- Linked task worktrees do not contain a separate `.venv`. Before entering one, resolve the
  interpreter from the primary checkout and keep using that absolute path for every Python command;
  on Windows this is `<primary>\\.venv\\Scripts\\python.exe`. A missing
  `.worktrees/<slug>/.venv` is not evidence that the project interpreter is unavailable, and must
  never trigger a fallback to system Python or creation/copying of another virtual environment.
- Commands which invoke `uv` must use a per-worktree writable cache inside project artifacts, for
  example set `UV_CACHE_DIR=<primary>/.artifacts/worktrees/<slug>/uv-cache`. Do not depend on or
  modify the user's global uv cache, and never share one writable cache between concurrent
  worktrees.
- Full-only suites require `--full`. In restricted Windows environments, use a writable
  workspace-local pytest base directory, for example
  `./.venv/Scripts/python.exe -m pytest --full --basetemp .artifacts/pytest/run <target>`.

## Isolated task workflow

- Each feature, bug, or independent refactor must use one `codex/<task-slug>` branch in its own
  `.worktrees/<task-slug>` worktree. Do not mix unrelated goals in one worktree or release.
- Create and remove task worktrees with `./.venv/Scripts/python.exe scripts/dev/tasks.py start
  <slug>` and `... tasks.py remove <slug>`. Removal is allowed only after the task tree has been
  squash-integrated into `main` and the worktree is clean.
- Run `start` and `remove` from the primary checkout with its project interpreter. If a restricted
  environment prevents Git from writing branch locks or worktree metadata, request the narrow
  repository-scoped Git write authorization and retry the same task-script command. Do not work in
  the primary checkout as a workaround, manually recreate the task branch/worktree, change global
  Git configuration, or bypass the task script.
- Task branches may use small checkpoint commits and may update `CHANGELOG.md` when the change is
  user-facing. They must not edit `quantmaster/release.py`; version metadata is updated on `main`
  only when semantic-versioning rules warrant it.
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

- Commits may update `CHANGELOG.md` as needed. When an integrated change warrants a version bump,
  update `VERSION` in `quantmaster/release.py` using semantic versioning and set `RELEASE_DATE` to
  the actual version date. A version commit is not a release and may be pushed normally.
- Create a Git tag and GitHub Release only when the owner explicitly requests publication. Never
  infer release authorization from a changelog edit, version bump, merge, commit, or push.
- `MAJOR` is owner-controlled and must never change without a separate, explicit authorization from
  the owner. New functionality increments `MINOR`; fixes and patches increment only `PATCH`.
- Add the matching user-facing notes to `RELEASES` in `quantmaster/release.py` and to the top
  of `CHANGELOG.md` in the same change.
- `quantmaster/release.py` is the runtime version source. Do not hard-code another application
  version in Python, HTML, or JavaScript; `pyproject.toml` reads it dynamically.
- Git tags and GitHub Releases must use `v{VERSION}`. The release workflow verifies the tag and
  publishes `CHANGELOG.md` as the GitHub Release body, so those records must stay synchronized.
- A published version may be replaced in place only when the owner explicitly requests it and the
  exact tagged commit has a failed GitHub CI run. Authorize with `scripts/release/sync.py recover-ci
  --run-id <id> --replace`, commit and push a descendant fix without changing `VERSION`, then run
  `scripts/release/sync.py replace-failed`. The command revalidates the failed run, clean synchronized
  `main`, unchanged version, tag target, and ancestry before replacing the Release and tag.

## Release synchronization

- Run `python scripts/release/sync.py install` once after cloning. The tracked hooks validate
  version metadata on `main` but never push, tag, or publish automatically.
- Push ordinary and version commits through the normal explicit Git workflow. Inspect metadata and
  branch synchronization with `python scripts/release/sync.py status`.
- Do not bypass the hooks for normal project work. Release publication remains a separate,
  owner-authorized tag workflow.
