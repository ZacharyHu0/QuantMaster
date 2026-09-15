# Deterministic task workflow

This document defines the isolated development, validation, integration, and cleanup workflow.
Development and integration are separate phases: a task finishes on a fixed development baseline
before spending time on `main` movement.

## 1. Start once and fix the development baseline

Create every independent task with `scripts/dev/tasks.py start <slug>` from the primary checkout.
`start` records the development commit SHA in the task manifest. `check` defaults to that SHA,
not the moving `origin/main` ref. Legacy manifests require an explicit `check --base <recorded-SHA>`.

The primary checkout is the task control plane and permanently holds a clean `main`. Do not edit
or switch branches there. For concurrent work, the coordinating agent creates every task first,
then starts each coding agent with its absolute `.worktrees/<slug>` directory as the working
directory. Lifecycle writes (`start`, `remove`, and `gc`) share one Git-common-dir lock; ordinary
development and validation remain parallel across task worktrees.

During development, do not poll, compare, fetch, merge, or rebase against local `main` or
`origin/main`. Progress on either branch is expected and is not a reason to interrupt the task or
repeat validation. Tasks with a real runtime, schema, or contract dependency must be ordered
explicitly: integrate the prerequisite before starting the dependent task.

## 2. Development loop

The development phase is complete only when all of the following are true inside the task
worktree:

- the requested behavior and regression tests are implemented;
- exact failing tests pass;
- `tasks.py check` (impact-mapped validation) passes;
- the task changes are committed and the worktree is clean.

Run `tasks.py check` after each coherent change using the primary checkout's absolute interpreter
path. It selects adjacent contracts from the checked-in impact map and runs full-only tests with
`--full`; unknown or infrastructure paths fail safe to the complete Python suite. After a failure,
rerun the exact pytest node id or `--last-failed`, then rerun the impact set. Do not run the full
suite during the edit loop.

On Windows, direct pytest invocations without `--basetemp` are automatically routed to a unique
`.artifacts/worktrees/<slug>/pytest/runs/direct-*` directory with a task-local cache. Repository
tests must never use `%TEMP%/pytest-of-*`; explicit basetemps remain supported when a checked-in
runner supplies a prepared artifact path.

Checkpoint commits are allowed. Task branches never edit `quantmaster/release/history.py` or
`CHANGELOG.md`. Do not push a checkpoint while a local gate is red; fix it locally first.

## 3. Verify every Git write target

Before a Git write against any linked checkout:

```powershell
git -C <absolute-worktree> branch --show-current
git -C <absolute-worktree> status --short
```

The write command must also use `git -C <absolute-worktree>`. `safe.directory` only establishes
trust and never selects the working tree.

Stop immediately for unmerged files, an in-progress merge/rebase/cherry-pick, detached or
unexpected HEAD, an unexpectedly dirty checkout the next write would affect, partial command
success, or unrelated task changes. Report the exact repository state before another write.
Movement of `main` during development is expected and is not an error to inspect.

## 4. Integration happens once

Only after development is complete:

1. Read the latest local `main` and `origin/main` state once and select the integration baseline.
2. Verify the task branch and all Git write targets using absolute paths.
3. Align the completed task to that baseline once; resolve conflicts inside the task worktree.
4. Run focused conflict-sensitive checks. Push the aligned commit while the PR is still Draft.
5. Wait for Draft fast/core to pass, then mark the PR Ready; `ready_for_review` triggers the
   full CI matrix for that exact commit.
6. After the matrix is green, run `tasks.py ready --accept-ci` to record its authoritative evidence.
   Without GitHub/CI access, run `tasks.py ready` locally (add `--ui` / `--rust` / `--package` for
   those lanes).
7. Update the PR with the exact evidence. After review passes, squash-merge as one independently
   revertible `main` commit, then run `tasks.py finish <slug> --pr <number>` from the primary checkout.

The integration baseline is fixed for that attempt. A genuine dependency or conflicting change
requires a deliberate new integration attempt, not a background loop that chases `main`.
Use a read-only merge preview such as `git merge-tree` before touching `main`; resolve conflicts in
the task worktree, never through an exploratory merge in the primary checkout.

## 5. Validation layers

| Phase | Gate | Scope |
| --- | --- | --- |
| Development | `tasks.py check` | Impact-mapped pytest nodes + changed-file Ruff |
| Draft PR push | CI `fast-gate` + `core` | Ruff, exception/complexity policy, mypy, core contract tests |
| Ready PR | full CI matrix | Coverage shards, native parity, browser, Windows package, package audit |
| Integration | `tasks.py ready --accept-ci` | Reuses the green Ready-PR CI run for the exact SHA |
| Main push | full CI matrix | Release safety net |

Validation evidence may be reused only when commit SHA, baseline, Python environment, options, and
policy baselines are identical; `tasks.py` records and compares them automatically. Any code,
dependency, configuration, baseline, or option change invalidates the recorded evidence. Mere
advancement of another branch does not.

Use outer timeouts from repository evidence: at least 5 minutes for focused Python validation,
10 minutes for a large impact set, and 15 minutes for a local `tasks.py ready`. Pytest's per-test
timeout remains the individual hang guard; an outer timeout is infrastructure interruption and
must not be retried with the same insufficient limit.

Refresh local duration-balanced shards with `scripts/ci/run.py --refresh-durations` when the
slowest shard exceeds the fastest by 25%. Keep the three shard wall times within 20% when
practical.

## 6. Classify stale tasks from evidence

### Session preflight and concurrent worktrees

Every new session must use a unique task slug. From the primary checkout, verify an existing
task before editing it:

```powershell
.\.venv\Scripts\python.exe scripts\dev\tasks.py preflight <task-slug>
```

`tasks.py` records a task manifest under `.artifacts/task-manifests/<task-slug>.json` and checks
the exact branch/worktree/artifact mapping before `serve`, `check`, `ready`, or `remove` proceeds.
Different sessions must use different slugs; the admin lease serializes lifecycle changes and
the per-task lease isolates writable state.

Cleanup has explicit durable phases:

| State | Meaning | Action |
| --- | --- | --- |
| `active` | Development task | Preserve |
| `checkout_pending_cleanup` | Checkout or branch removal unfinished | Release own processes/handles, retry |
| `pending_cleanup` | Git lifecycle finished; artifact/archive work remains | Retry artifact cleanup |
| `removed` | Checkout, branch and disposable artifacts removed | Deliverables remain archived |

Use `tasks.py status` for a read-only JSON inventory and `tasks.py retry-cleanup` to preview
queued work. `retry-cleanup --apply` processes due entries; `retry-cleanup <slug> --apply`
retries that task immediately. `start` (before creating the new task) and `gc --apply` also
process due entries. Backoff starts at 60 seconds and is capped at one hour, with five automatic
attempts before requiring an explicit retry. No always-running janitor is installed.

`finish <slug> --pr <number>` records an already merged GitHub PR before touching the checkout.
The receipt binds repository, PR, branch head, PR base and merge SHA. It survives squash merges
and later overlapping main changes. A changed branch head or merge absent from local main
blocks removal. `finish` may fetch and fast-forward main once; it never resolves a main conflict.
An interrupted call can be repeated; a saved receipt avoids repeating the merge.

With merge authorization, `finish <slug> --pr <number> --merge` checks the Ready PR and full
exact-SHA gate, then uses GitHub CLI's `--match-head-commit` protection. Queue/asynchronous merges
return `TASK_MERGE_PENDING`; retry after GitHub completes them. This command does not post
bookkeeping comments: run `github_sync.py reconcile` separately to preview those changes.

Before deleting task artifacts, the tool atomically moves non-disposable entries into
`.artifacts/task-deliverables/<slug>`. Existing destination names block rather than overwrite.
Only `cache`, `pytest`, `uv-cache`, and `runtime` are disposable; store final reports at the task
artifact root or in `deliverables/`. Archive retention is explicit and independent of GC.

Failure of automatic integration detection does not mean a task is active. Assign every stale task
to exactly one category:

1. **Patch-equivalent** — stable patch ID or equivalent proof matches `main`.
2. **Superseded** — identified later `main` commits retain the key contract and adjacent tests;
   verify with `range-diff` or file comparison, not commit subjects alone.
3. **Dirty or active** — preserve without mutation.
4. **Independent value remains** — port the value into a new task and integrate it through the
   same two phases.

If a task artifact root cannot be inspected or have its ACL restored by the current identity,
`tasks.py remove` reports `TASK_ARTIFACT_ACL_UNRECOVERABLE`, records `pending_cleanup`, and
keeps the artifact manifest as the retry record. The Git lifecycle may already be complete;
after the required path permission is available, retry `tasks.py remove <task-slug>` (or the
`retry-cleanup <task-slug> --apply`) until the artifact root is gone. Never delete paths by hand.

Delete an old task only after its value is proven present on `main` or the owner explicitly
abandons it. Report the exact category; never describe every non-removable task as active,
unmerged, or safe to delete.

## 7. Separate development from the stable application

The stable application and task worktrees are different execution environments:

- A stable instance runs an immutable, package-validated `main` slot.
- A task development server runs only from its own worktree, using the primary checkout's
  absolute `.venv` interpreter and task-local ports, configuration, data, logs, and control
  database under `.artifacts/worktrees/<slug>/runtime/dev`.
- A development server may read an explicitly configured stable StockDB read-only; it cannot
  update, stop, or replace the stable instance.
- Integrating a task does not activate it. Activation is owner-managed and replaces the complete
  application generation.

Start a task server from the primary checkout:

```powershell
.\.venv\Scripts\python.exe scripts\dev\tasks.py serve <slug> --open
```

Pass `--stockdb-root <absolute-path>` only for read-only access to an installed StockDB SDK.

## 8. GitHub management

The normative GitHub procedure is [docs/github-workflow.md](github-workflow.md). Summary of the
development-phase view:

1. Create or select the Issue; start the isolated `codex/<slug>` worktree.
2. Push the first coherent commit and open a Draft PR with `Closes #<issue>`.
3. Run `scripts/dev/github_sync.py reconcile` (dry-run) and apply safe fixes with `--apply`.
4. Complete the one-time integration alignment and push the aligned Draft commit.
5. Wait for Draft fast/core, mark the PR Ready, and let the full CI matrix run.
6. Record the green exact-SHA evidence with `tasks.py ready --accept-ci`, resolve review,
   squash-merge, then `tasks.py finish <slug> --pr <number>`.

Discussions host architecture proposals and evidence-backed decisions. Irreversible migrations,
required features that exceed a hard package budget, or inconclusive SciPy/Rust benchmarks pause
only the affected task until a decision post with alternatives, measured evidence, rollback
limits, and a recommendation is published.

Ordinary merges do not create tags or releases. The agent owns merge and tag mechanics, but the
tag workflow publishes a GitHub Release, so a release tag is pushed only after the owner explicitly
confirms that Release.

### Legacy recovery

With explicit owner authorization to retain unfinished work as an archive and remove its
original checkout/branch, use `tasks.py archive <slug> --apply` from the primary checkout.
The tool writes `.artifacts/task-archives/<slug>/history.bundle`, `checkout.zip`, and
`receipt.json`. The bundle contains the exact branch history; the ZIP preserves tracked,
modified, untracked and ignored checkout files (except the linked `.git` pointer).
Links and in-progress Git operations fail closed. SHA-256 and Git bundle verification must
pass before removal. A changed file/head invalidates the receipt; interrupted deletion may
only remove surviving files that still match the archive. `remove` and `retry-cleanup` resume
that operation without claiming the archived branch was merged.

To restore elsewhere, clone `history.bundle` with `--branch codex/<slug>`, then overlay
`checkout.zip` to recover uncommitted state. Validate the SHA-256 values in `receipt.json`
first. These archives are permanent until explicitly disposed of; GC never removes them.

Start with `tasks.py status`; it includes branch-only tasks, unregistered checkouts, missing
manifests and old removal intents. Inventory does not grant deletion permission. For a clean
legacy task with a known merged PR, use `finish <slug> --pr <number>`; repository and exact head
must match. Otherwise establish patch equivalence or reviewed superseding evidence. Dirty,
active, ambiguous and independently valuable work remains protected. Slugs with receipts or
archives cannot be reused.

CLI merge behavior: [GitHub CLI merge reference](https://cli.github.com/manual/gh_pr_merge).
Receipt fields: [GitHub pull request API](https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request).

### Physical cleanup acceptance

Git completion and physical cleanup are separate: inventory exposes `git_complete` and
`cleanup_complete`. `remove`, `finish`, `archive`, `retry-cleanup --apply` and `gc --apply`
return exit 2 (`TASK_CLEANUP_PENDING`) if their cleanup scope still contains pending state.
`status` stays read-only with exit 0; `status --require-clean` returns 2 for pending or invalid
entries. An active development task does not count as cleanup debt.

Permission errors (`inspection_denied`, `deletion_denied`) report
`permission_handoff_required` and stop automatic retries, including GC. Existing manifests
are classified by their saved error too. Transient sharing violations retain bounded retry.
The coordinating session owns a Blocked Issue with the slug, stable error and access-change
condition. After an authorized maintenance identity can access the residual artifacts,
retry the explicit slug, verify `cleanup_complete=true`, and only then close the cleanup Issue.
This is an explicit handoff; no administrator escalation or background daemon is installed.

Local staging creates unique build/extraction directories under its already leased roots
with inherited permissions. It does not promote private `tempfile` directories into durable
slots. The Windows staging regression checks a separate cleanup SID's inherited Modify grant
through extraction and slot rename, then removes the result. It checks the ACL contract;
it does not pretend that a same-user process is a different authenticated OS identity.
