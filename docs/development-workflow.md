# Deterministic task workflow

This document defines the isolated development, validation, integration, and cleanup workflow.
Development and integration are separate phases: a task finishes on a fixed development baseline
before spending time on `main` movement.

## 1. Start once and fix the development baseline

Create every independent task with `scripts/dev/tasks.py start <slug>` from the primary checkout.
Choose and record one development baseline at task creation.

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
   revertible `main` commit, then run `tasks.py remove <slug>` from the primary checkout.

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

Failure of automatic integration detection does not mean a task is active. Assign every stale task
to exactly one category:

1. **Patch-equivalent** — stable patch ID or equivalent proof matches `main`.
2. **Superseded** — identified later `main` commits retain the key contract and adjacent tests;
   verify with `range-diff` or file comparison, not commit subjects alone.
3. **Dirty or active** — preserve without mutation.
4. **Independent value remains** — port the value into a new task and integrate it through the
   same two phases.

If a task artifact root cannot be inspected or have its ACL restored by the current identity,
`tasks.py remove` reports `TASK_ARTIFACT_ACL_UNRECOVERABLE`, preserves the artifact and branch,
and remains the only retry interface after the required path permission is available.

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
   squash-merge, then `tasks.py remove <slug>`.

Discussions host architecture proposals and evidence-backed decisions. Irreversible migrations,
required features that exceed a hard package budget, or inconclusive SciPy/Rust benchmarks pause
only the affected task until a decision post with alternatives, measured evidence, rollback
limits, and a recommendation is published.

Ordinary merges do not create tags or releases. The agent owns merge and tag mechanics, but the
tag workflow publishes a GitHub Release, so a release tag is pushed only after the owner explicitly
confirms that Release.
