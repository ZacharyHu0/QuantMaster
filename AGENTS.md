# QuantMaster repository instructions

## Product evolution

- QuantMaster is personal software. Unless the owner explicitly requests compatibility, prefer a
  clean replacement over legacy adapters, deprecated parameter mappings, duplicate routes, or old
  snapshot decoders. Delete obsolete paths instead of carrying historical baggage forward.
- Data integrity and recoverability still matter: a clean replacement must fail explicitly when
  evidence is unavailable and must not silently reinterpret old data as a new contract.
- Do not add validation hashes, hash tags, or similar hashing mechanisms by default. Introduce one
  only when there is a concrete, documented reason that it saves more time or compute than it costs.

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
  task-local pytest base directory prepared by the task tooling, for example
  `<primary>/.artifacts/worktrees/<slug>/pytest/runs/<run-id>`. Do not let pytest create its own
  cache or base directory inside `.worktrees/<slug>`.

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
  diff. Resolve integration conflicts inside the completed task worktree during the integration
  phase, not during feature development.
- `.artifacts`, pytest temporary directories, writable databases, and runtime state are local to
  one worktree. Never point concurrent worktrees at the same writable path or `--basetemp`.
- Immediately after `tasks.py start`, choose and record one development baseline. Keep that
  baseline fixed while implementing the task and running exact or impact checks. During this
  development phase, do not poll, compare, fetch, merge, rebase, or otherwise follow local `main`
  or `origin/main`; progress on either branch is not a reason to stop work or repeat validation.
- Finish the requested behavior, regression tests, exact tests, and `tasks.py check` on the fixed
  development baseline. Commit the completed task and make its worktree clean before considering
  integration or inspecting how the integration baseline has moved.
- Only after development is complete, enter one integration phase: read the latest local `main`
  and `origin/main`, select the integration baseline, align the completed task once, resolve any
  conflict in the task worktree, run the final `tasks.py ready`, and immediately integrate the
  validated commit. Do not repeatedly chase unrelated `main` progress between those steps.
- Independent tasks may develop in parallel on their own fixed baselines. Tasks with a real
  runtime, schema, or contract dependency must run serially: integrate the prerequisite before
  starting the dependent task from a baseline that contains it. Concurrent movement of `main`
  alone does not make independent tasks dependent.
- Before every Git write that targets another worktree, verify its absolute path, branch, and clean
  state with `git -C <absolute-worktree> branch --show-current` and `git -C <absolute-worktree>
  status --short`. The write command itself must also use `git -C <absolute-worktree>`;
  `safe.directory` establishes trust only and never selects the command's working tree.
- Stop immediately for unmerged files, an in-progress merge/rebase/cherry-pick, detached or
  unexpected HEAD, an unexpectedly dirty checkout that the next write would affect, unrelated task
  changes, or partial command success. Re-establish and report the exact repository state before
  another write. Movement of `main` during development is expected and is not an error to inspect.

## Worktree lifecycle and Windows ACLs

- The task tooling owns the complete worktree lifecycle. `tasks.py start` must create every
  task-writable cache, temporary, test, database, and runtime directory under
  `<primary>/.artifacts/worktrees/<slug>` before any third-party tool writes there. New runners
  must use the repository's ACL-safe directory preparation helper; they must not introduce an
  unprepared writable path or place disposable runtime state inside the checkout.
- Preserve inherited Windows ACLs on prepared directories. Never rely on pytest, package managers,
  formatters, or application code to create the root of a task-writable directory because their
  initialization or `chmod` behavior can make cleanup dependent on the sandbox identity that ran
  them. Validation for task tooling must cover creation and removal under different Windows
  identities or equivalent ACL fixtures.
- `tasks.py remove <slug>` is the only normal cleanup interface and must be safe to retry after
  interruption or partial Git removal. It must re-establish the actual registered/residual state,
  prove integration and checkout cleanliness, remove only the verified task checkout and its
  task-local artifacts, and delete the task branch only after checkout cleanup succeeds. A missing
  registration with a verified clean residual checkout is a recoverable intermediate state, not a
  reason to require manual deletion.
- A normal cleanup must not require `takeown`, `icacls`, administrator elevation, Explorer deletion,
  or a hand-written `Remove-Item`. If inherited ACLs still block deletion, treat that as a defect in
  task directory preparation or `tasks.py remove`: retain the branch and evidence, fix the lifecycle
  tooling in a separate task, add a regression test, and retry the same remove command. Manual ACL
  repair is an exceptional owner-authorized recovery action, never the documented routine.
- Bulk cleanup must calculate candidates from current evidence immediately before each removal.
  Remove only clean tasks that `tasks.py` proves are fully integrated into `main`; skip dirty,
  unintegrated, active, or explicitly protected worktrees without changing them. Do not infer that
  detached Codex-managed worktrees or arbitrary `.artifacts` checkouts are disposable task trees.
- Cleanup reporting must distinguish removed worktrees, protected/skipped worktrees, dirty or
  unintegrated worktrees, and recoverable residual states. Do not report success while a registered
  checkout, verified residual directory, task artifact root, or task branch that should have been
  removed remains.

## Layered verification and integration

- During development run `./.venv/Scripts/python.exe scripts/dev/tasks.py check`. The checked-in
  impact map selects adjacent contracts and invokes explicit tests with `--full`; unknown or
  infrastructure paths fail safe to the complete Python suite. Use `--staged` to inspect only the
  index or `--base <ref>` when the comparison base is not the task's recorded development baseline.
- Re-run a failure by exact pytest node id or `--last-failed`, then rerun the task impact set after
  the fix. Do not repeatedly run the complete suite during the edit loop without a concrete need.
- After the development work is complete and committed, begin the integration phase. Select the
  latest approved local/remote integration state once, align the task to it, and run
  `./.venv/Scripts/python.exe scripts/dev/tasks.py ready`. Add `--ui`, `--rust`, or `--package`
  when the task affects those lanes.
- `ready` runs the complete static and Python gates. After it passes, squash exactly that one task
  into one independently revertible commit on `main`. Ordinary integration commits do not update
  version metadata or create a release.
- Refresh local duration-balanced shards with `scripts/ci/run.py --refresh-durations` when the
  slowest shard exceeds the fastest by 25%. Keep the three shard wall times within 20% when
  practical. The timing file is local cache, not release evidence.
- Development checks validate the task on its fixed development baseline. Reserve one complete
  `tasks.py ready` run for the committed task after its one-time integration-base alignment. If
  `check` already ran identical complete gates for the same commit SHA, baseline, options, and clean
  worktree, record and reuse that evidence instead of immediately rerunning it. Any code,
  dependency, configuration, baseline, option, or base change invalidates reuse; another branch
  advancing by itself does not invalidate development evidence.
- Size outer command timeouts from repository timing evidence. Use at least five minutes for focused
  Python validation, ten minutes for a large impact set, and fifteen minutes for `ready` unless
  current evidence warrants more. Pytest's per-test timeout remains the individual hang guard. An
  outer timeout is infrastructure interruption, not a test failure; never rerun with the same known
  insufficient limit.
- Before modifying `main` in the integration phase, recheck both worktrees and run a read-only
  conflict preview such as `git merge-tree` against the selected integration baseline. Resolve
  conflicts in the task worktree, commit, and revalidate there. Do not start a squash merge on
  `main` to discover a predictable conflict.
- Keep integration mechanical after a successful full gate. The selected integration baseline is
  fixed for that integration attempt. If conflict resolution or another integration step changes
  the effective tree, prior validation is stale and the changed task tree must be validated before
  committing it to `main`. A genuine dependency or conflicting intervening change requires an
  explicit new integration attempt, not a continuous loop following unrelated `main` progress.

## Stale task classification

- Do not equate `tasks.py` refusing removal with an active or valuable task. Classify every stale
  task as exactly one of: patch-equivalent to `main`; superseded by identified later `main` commits;
  dirty or demonstrably active; or still containing independent value absent from `main`.
- Prove patch equivalence with stable patch IDs when possible. For a superseded task, record the old
  task commit and replacing `main` commit(s), inspect `range-diff` or relevant file differences, and
  verify that the key runtime contract and adjacent tests remain on `main`. Similar subjects alone
  are not evidence.
- Preserve dirty or active worktrees. Move genuinely missing value through a new task and validate
  it independently through the fixed development and one-time integration phases; do not merge a
  stale branch wholesale. Remove an old task only after its value is proven present or the owner
  explicitly abandons it.
- Report precise states: "not automatically proven integrated", "superseded", "dirty/active", or
  "independent value remains". Never describe all non-removable worktrees as active, unmerged, or
  safe to delete.

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
- After the version commit is pushed into `origin/main` history, run `python scripts/release/sync.py
  cut [--commit <sha>]` to freeze exactly one release candidate. The candidate records `VERSION`,
  `RELEASE_DATE`, and the full Git commit SHA in repository-local state. Human confirmation is for
  that immutable SHA, not for the moving `main` or `HEAD`.
- Only one unfinished candidate may exist. Concurrent workers may continue integrating and pushing
  `main`; advancement of `main` is reported by `status` but is not a candidate failure and must not
  trigger rebasing, rollback, or replacement of the frozen candidate.
- Publication remains owner-authorized and explicit: `python scripts/release/sync.py publish`
  revalidates candidate metadata and ancestry, creates an annotated `v{VERSION}` tag at the frozen
  SHA, and pushes only that tag. `cut` never creates or pushes a tag or GitHub Release.
- Published tags are immutable by default. The only same-version replacement remains the existing
  `recover-ci --run-id <id> --replace` plus `replace-failed` path, and it must retain exact failed
  GitHub CI evidence, authorized tag target, unchanged version, and descendant-fix checks.
- Do not bypass the hooks for normal project work. Release publication remains a separate,
  owner-authorized tag workflow.

## 易犯错误

- 不要让 pytest 首次创建任务 worktree 的 `cache_dir`。pytest 的原子缓存初始化会调用
  `chmod`；在 Windows 沙箱中，这可能移除目录的继承 ACL，导致后续由不同沙箱身份运行的
  任务无法自动清理缓存。任务脚本必须先创建 worktree 专属缓存目录，再通过
  `-o cache_dir=<path>` 交给 pytest；`--basetemp` 也必须保持 worktree 独占。
- 不要把管理员 PowerShell、`takeown`/`icacls` 或手动删除残余目录当作任务收尾步骤。
  出现这种需求说明 worktree 生命周期工具或可写目录约定存在缺陷；应保留可恢复证据，
  修复并测试 `tasks.py`，然后重新执行同一个 `remove` 命令。
