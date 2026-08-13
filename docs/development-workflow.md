# Deterministic task workflow

This document defines the fixed workflow for isolated development, validation, integration, and
stale-worktree review. The goal is to validate the tree that will actually be integrated exactly
once, keep Git writes directed at an explicitly verified checkout, and preserve unfinished work.

## 1. Establish the final baseline first

Create task worktrees only through `scripts/dev/tasks.py start`. Immediately compare the task HEAD,
local `main`, and `origin/main`. Local `main` is the integration baseline when it contains approved
commits that have not been pushed yet. Update the new task to that baseline before porting code,
editing, or running validation.

Dependent tasks are serial. Finish task A through integration and cleanup before aligning task B to
the resulting `main`. A downstream task must not be validated against a base that is already known
to change.

## 2. Verify every Git write target

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
- an unexpectedly dirty primary checkout;
- a command that succeeded only partially;
- a task branch that no longer has the intended integration base.

Recover and re-inspect the exact repository state before starting another operation.

## 3. Validate in layers without duplicate full runs

During the edit loop, rerun an exact failing pytest node or use `tasks.py check`. Commit the final
task, update it to the final integration base, and obtain one complete gate through `tasks.py ready`.

If `check` fails safe to the same complete gates, its result may replace the immediate `ready` run
only when all of the following are identical and recorded:

- task commit SHA and clean worktree;
- comparison base;
- Python environment and dependencies;
- validation options and requested lanes;
- checked-in policy and baseline files.

Any change to code, dependencies, configuration, policy baselines, options, or base commit
invalidates the evidence.

Choose outer command timeouts from observed repository duration, with these minimum defaults:

| Validation | Minimum outer timeout |
| --- | ---: |
| Focused Python tests | 5 minutes |
| Large impact set | 10 minutes |
| `tasks.py ready` | 15 minutes |

Pytest's per-test timeout remains the guard for individual hangs. An outer timeout means the runner
interrupted validation; it is not a failed test and must not be retried with the same insufficient
limit.

## 4. Preview integration before touching `main`

Before a squash integration:

1. Verify the task branch and both worktree statuses again.
2. Confirm the task is based on current local `main`.
3. Use a read-only merge preview such as `git merge-tree`.
4. Resolve any conflict inside the task worktree, commit it, and invalidate/re-run validation.
5. After a clean preview and valid full-gate evidence, perform a mechanical squash integration.

Do not start a merge on `main` merely to discover conflicts. If integration changes the effective
tree relative to the validated task commit, validate the changed task state before committing it.

## 5. Classify stale worktrees from evidence

Failure of automatic integration detection does not mean a task is active. Assign every stale task
to exactly one category:

1. **Patch-equivalent** — stable patch ID or equivalent proof matches `main`.
2. **Superseded** — identified later `main` commits retain the key runtime contract and adjacent
   tests; verify with `range-diff` or relevant file comparison, not commit subjects alone.
3. **Dirty or active** — preserve without mutation.
4. **Independent value remains** — port the valuable portion into a new task based on current
   `main`, then validate and integrate that task independently.

Delete an old task only after its value is proven present on `main` or the owner explicitly chooses
to abandon it. Cleanup reports must use the category names above rather than conflating
"not automatically proven integrated" with active, valuable, or safe to delete.

## 6. Keep scope changes explicit

When cleanup or feature work exposes an independent lifecycle-tool defect, stop and report the
blocker and expected extra validation cost. Fix it in a separate task with a regression test. Do not
silently turn a cleanup into a tooling refactor or combine unrelated fixes in one integration.
