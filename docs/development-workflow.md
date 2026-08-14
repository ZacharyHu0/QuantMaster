# Deterministic task workflow

This document defines the fixed workflow for isolated development, validation, integration, and
stale-worktree review. Its central rule is that development and integration are separate phases.
A task finishes its own work on a fixed development baseline before it spends time following
changes to `main`.

## 1. Start once and fix the development baseline

Create every independent task through `scripts/dev/tasks.py start`. At task creation, choose and
record one development baseline in the task metadata. The resulting task worktree and branch are
the task's isolated development environment.

During the development phase, do not poll, compare, fetch, merge, rebase, or otherwise follow local
`main` or `origin/main`. Progress on either branch is expected and is not a reason to interrupt the
task, move its baseline, or repeat validation. Implement the complete task and its tests against the
recorded development baseline.

Independent tasks may develop in parallel on their own fixed baselines. Tasks with a real runtime,
schema, or contract dependency must be ordered explicitly: integrate the prerequisite first, then
start the dependent task from a baseline that contains it. Ordinary concurrent movement of `main`
does not create a dependency.

## 2. Complete development before considering integration

The development phase is complete only when all of the following are true inside the task
worktree:

- the requested behavior and regression tests are implemented;
- exact failing tests pass;
- the appropriate `tasks.py check` or documented impact checks pass;
- the task changes are committed and the worktree is clean;
- remaining issues, if any, are explicitly outside the task's scope.

Do not use a moving integration baseline as a reason to postpone this completion point. Development
checks prove the task works on its recorded baseline. They are useful evidence, but they are not the
final integration gate.

## 3. Verify every Git write target

Before a Git write against any linked checkout, run the equivalent of:

```powershell
git -C <absolute-worktree> branch --show-current
git -C <absolute-worktree> status --short
```

The write command must also contain `git -C <absolute-worktree>`. A `safe.directory` option only
marks a repository as trusted; it does not select that repository as the working directory.

Stop instead of continuing when any of these conditions is present:

- unmerged files or an in-progress merge, rebase, or cherry-pick;
- detached or unexpected HEAD;
- an unexpectedly dirty checkout that the next write would affect;
- a command that succeeded only partially;
- unrelated changes inside the task worktree.

Recover and re-inspect the exact repository state before starting another write. Movement of
`main` during the development phase is not an error and must not be inspected as task state.

## 4. Enter the integration phase once

Only after development is complete may the task enter the integration phase. The integrator then:

1. Reads the latest local `main` and `origin/main` state once and selects the integration baseline.
2. Verifies the task branch and all Git write targets using absolute paths.
3. Aligns the completed task to that integration baseline once.
4. Resolves conflicts inside the task worktree and commits the resolved task state.
5. Runs focused checks for conflict-sensitive areas and one final `tasks.py ready` on that state.
6. Pushes the validated task commit, updates its Draft PR with the exact evidence, and marks it
   ready for review.
7. After required GitHub Actions checks pass, the agent squash-merges the PR as one independently
   revertible `main` commit without returning to feature development or repeatedly chasing
   unrelated new `main` progress.

The selected integration baseline remains fixed for this integration attempt. If a genuine
dependency or conflicting change lands before integration can complete, report that concrete event
and restart the integration phase deliberately; do not create a background loop that continually
updates the task merely because `main` advanced.

Use a read-only merge preview such as `git merge-tree` before touching `main`. Resolve conflicts in
the task worktree, not through an exploratory merge in the primary checkout. If conflict resolution
changes the effective tree, the final gate must cover that changed committed state.

## 5. Validate in layers without duplicate full runs

During development, rerun an exact failing pytest node or use `tasks.py check`. Reserve
`tasks.py ready` for the committed, clean state in the integration phase after the one-time baseline
alignment.

Validation evidence may be reused only when all of the following are identical and recorded:

- task commit SHA and clean worktree;
- selected baseline for that phase;
- Python environment and dependencies;
- validation options and requested lanes;
- checked-in policy and baseline files.

Any change to code, dependencies, configuration, policy baselines, options, or the selected
integration base invalidates final-gate evidence. Mere awareness that another branch advanced does
not invalidate development evidence and must not trigger a rebase or rerun.

Choose outer command timeouts from observed repository duration, with these minimum defaults:

| Validation | Minimum outer timeout |
| --- | ---: |
| Focused Python tests | 5 minutes |
| Large impact set | 10 minutes |
| `tasks.py ready` | 15 minutes |

Pytest's per-test timeout remains the guard for individual hangs. An outer timeout means the runner
interrupted validation; it is not a failed test and must not be retried with the same insufficient
limit.

## 6. Classify stale worktrees from evidence

Failure of automatic integration detection does not mean a task is active. Assign every stale task
to exactly one category:

1. **Patch-equivalent** — stable patch ID or equivalent proof matches `main`.
2. **Superseded** — identified later `main` commits retain the key runtime contract and adjacent
   tests; verify with `range-diff` or relevant file comparison, not commit subjects alone.
3. **Dirty or active** — preserve without mutation.
4. **Independent value remains** — port the valuable portion into a new task, then develop,
   validate, and integrate that task through the same two phases.

Delete an old task only after its value is proven present on `main` or the owner explicitly chooses
to abandon it. Cleanup reports must use the category names above rather than conflating
"not automatically proven integrated" with active, valuable, or safe to delete.

## 7. Keep scope changes explicit

When cleanup or feature work exposes an independent lifecycle-tool defect, stop and report the
blocker and expected extra validation cost. Fix it in a separate task with a regression test. Do not
silently turn a cleanup into a tooling refactor or combine unrelated fixes in one integration.

## 8. Separate development from the stable application

The stable application and task worktrees are different execution environments:

- A stable instance runs an immutable, package-validated `main` slot. It never imports Python or
  static assets from the primary checkout or a task worktree.
- A task development server runs only from its own worktree, using the primary checkout's absolute
  `.venv` interpreter and task-local ports, configuration, data, logs, and control database under
  `.artifacts/worktrees/<slug>/runtime/dev`.
- A development server may read an explicitly configured stable StockDB, but it must use an
  unmanaged, read-only connection and cannot update, stop, or replace the stable instance.
- Integrating a task into `main` does not activate it. A candidate becomes eligible only after the
  package lane passes for the exact `main` commit, and the owner activates that candidate manually.
- Activation replaces the complete application generation (Web, runtime, compute workers, and
  static assets). A source reload is not an update mechanism.

These rules keep the instance used for daily work alive while source files, dependencies, and task
servers change independently.

## 9. Use GitHub as the management record

The detailed, normative GitHub procedure is [docs/github-workflow.md](github-workflow.md). The
requirements below are the development-phase view of that procedure; they are repository rules,
not suggestions kept only in an agent prompt.

Every task starts from a GitHub Issue that states its scope, non-goals, public seam, data and
rollback risk, performance budgets, and acceptance checks. The task branch and PR link that Issue;
the PR description remains the authoritative validation report.

Before implementation, set the Issue owner, type/risk labels, milestone, active v1.16.0 Project,
parent Epic, and any `blocked by`/`blocking` relationships. Keep the Issue, PR, Project status and
parent Epic synchronized as the task moves from `In progress` to `In review`, `Blocked`, and `Done`.

The normal flow is:

1. Create or select the Issue, then create the isolated `codex/<slug>` task worktree.
2. Push the first coherent commit and open a Draft PR with `Closes #<issue>`.
3. Record exact tests and `tasks.py check` evidence as the implementation changes.
4. Complete the one-time integration alignment and `tasks.py ready` gate, then mark the PR ready.
5. Let GitHub Actions run the required lanes. The agent resolves review findings in the same task
   branch, revalidates changed evidence, and squash-merges the PR when all gates pass.
6. Remove the integrated task worktree through `tasks.py remove` and update the parent Epic.

Use GitHub Discussions for architecture proposals and evidence-backed choices that do not belong to
one implementation diff. In particular, irreversible migrations, required features that exceed a
hard package budget, or inconclusive SciPy/Rust benchmarks pause only the affected task and receive
a decision post with alternatives, measured evidence, rollback limits, and a recommended option.

Ordinary PR merges do not create tags. The agent owns merge and tag mechanics, but the current tag
workflow publishes a GitHub Release; therefore it pushes a release tag only after the owner
explicitly confirms that Release. Version metadata, candidate freezing, and tag publication continue
to follow the repository release synchronization contract.
