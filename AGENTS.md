# QuantMaster Repository Instructions

Owner-authorized goal: keep correctness gates, but make the Codex + GPT development loop fast.
When a checked-in script can answer a question, run the script and trust its output; do not
re-derive policy from prose or chat context.

## Product invariants

- QuantMaster is personal software. Prefer a clean replacement over legacy adapters, duplicate
  routes, or old snapshot decoders. A clean replacement must fail explicitly when evidence is
  unavailable and must not silently reinterpret old data as a new contract.
- Data source priority: inspect stockdb and existing local caches first; only call Tushare or
  another remote provider for fields that are missing or stale locally.
- Use the primary checkout's project interpreter for every Python command:
  `<primary>\.venv\Scripts\python.exe` on Windows (`.venv/bin/python` elsewhere). Task worktrees
  share that interpreter. Never fall back to system Python.
- `.artifacts`, pytest caches, writable databases, and runtime state are task-local. `tasks.py`
  owns every writable path; do not create, chmod, or delete task directories manually.

## Task lifecycle (the only workflow)

1. **Issue first.** Create or select a GitHub Issue. It must record scope, non-goals, and
   acceptance checks; the Issue template supplies the rest. Do not start code without it.
2. **Start the task.** From the primary checkout:
   `./.venv/Scripts/python.exe scripts/dev/tasks.py start <slug>`.
   Record the development baseline and keep it fixed. Do not fetch, merge, or rebase during
   development; movement of `main` is expected and irrelevant until integration.
3. **Develop on the baseline.**
   - After each coherent change run `tasks.py check` from the task worktree using the primary
     interpreter's absolute path. It runs the impact map and static checks; rerun failures by
     exact pytest node id. Do not run the full suite in the edit loop.
   - Small checkpoint commits are fine. Task branches never edit `quantmaster/release.py` or
     `CHANGELOG.md`.
   - Fix known failures locally before pushing. Repeated checkpoint pushes with a red gate are
     not permitted.
4. **Push and open a Draft PR.** After the first coherent commit, push `codex/<slug>`, open a
   Draft PR using `Closes #<issue>`, and fill the PR template. Then run
   `./.venv/Scripts/python.exe scripts/dev/github_sync.py reconcile` (dry-run by default) and
   apply its safe metadata fixes with `--apply`; handle only what the script still reports.
   Draft PRs run the fast CI lane; heavy lanes wait for Ready.
5. **Integrate once.** After development is complete: read local `main` and `origin/main` once,
   align the task to the selected integration baseline once, resolve conflicts inside the task
   worktree, and commit. Do not chase new `main` commits in a loop.
6. **Run the integration gate.** `tasks.py ready --accept-ci` reuses the green GitHub Actions
   run for the exact task commit as the authoritative full gate. Without GitHub/CI access, run
   `tasks.py ready` locally (with `--ui` / `--rust` / `--package` when those lanes changed).
7. **Mark Ready, merge, clean up.** Mark the PR ready only after the integration gate passes.
   Resolve review, wait for the full CI matrix, squash-merge, then immediately
   `tasks.py remove <slug>` from the primary checkout and finish with
   `github_sync.py reconcile --apply`.

## Validation layers (do not duplicate)

| Phase | Command / trigger | Coverage |
| --- | --- | --- |
| Development | `tasks.py check` | Impact-mapped pytest nodes + changed-file Ruff; full only when impact map requires it |
| Draft PR push | CI `fast-gate` + `core` | Ruff, exception/complexity policy, mypy, core contract tests (Linux + Windows + macOS) |
| Ready PR | full CI matrix | 3 coverage shards, native parity, browser, Windows package, quality/package audit |
| Integration | `tasks.py ready --accept-ci` | Records the green Ready-PR CI evidence for the exact SHA; no local full rerun |
| Main push | full CI matrix | Release safety net |

- `scripts/ci/run.py --fast` reproduces the Draft gate locally; `--full`, `--all`,
  `--ui`, `--rust`, and `--package` remain available when a lane genuinely needs local proof.
- Evidence may be reused only for the same commit SHA, baseline, environment, and options.
  `tasks.py` records and compares these automatically; do not rerun "to be sure".
- Use at least 5 minutes for focused pytest, 10 minutes for a large impact set, and 15 minutes
  for a local `tasks.py ready`. Pytest's per-test timeout remains the hang guard.

## GitHub bookkeeping is automated

- `scripts/dev/github_sync.py reconcile` is the single bookkeeping entry point. Default is
  dry-run; `--apply` executes only the safe fixes it lists: closing merged-but-open issues
  (unless they carry `blocked`), closing duplicate open issues, and commenting on stale Drafts.
- Update Project status at phase boundaries only: `In progress` on task start, `In review` when
  the PR is marked Ready, `Blocked` with a comment when a blocker appears, `Done` after merge.
  Labels and milestone are set when the Issue is created.
- A Draft PR with no update for 48 hours must be explicitly marked `Blocked` with an unblock
  condition, or advanced to Ready.
- Discussions remain the place for architecture decisions, irreversible migrations, hard budget
  conflicts, and inconclusive Rust/SciPy benchmarks.

## Releases and versioning

- Task branches never edit `quantmaster/release.py` or `CHANGELOG.md`. Ordinary squash merges
  never bump versions or create releases.
- Version bumps are centralized in one explicit version PR requested by the owner. That PR alone
  updates `VERSION`, `RELEASE_DATE`, `RELEASES`, and the top of `CHANGELOG.md` together.
  PR bodies never speculate about a target version.
- `quantmaster/release.py` is the only runtime version source.
- Tags and GitHub Releases are owner-authorized only. Never infer authorization from a merge,
  changelog edit, or version bump. Tag mechanics follow `scripts/release/sync.py`; run
  `python scripts/release/sync.py install` once after cloning.

## Stop conditions

Stop and report the exact repository state before any further write when you see: unmerged files
or an in-progress merge/rebase/cherry-pick; detached or unexpected HEAD; an unexpectedly dirty
checkout the next write would affect; partial command success; or unrelated changes in a task
worktree. For any Git write against another worktree, verify
`git -C <absolute-worktree> branch --show-current` and `git -C <absolute-worktree> status --short`
first, and use `git -C <absolute-worktree>` in the write command itself.

## 易犯错误

- 不要让 pytest 或应用代码创建任务 worktree 的可写根目录；`tasks.py start` 已经准备好
  cache、basetemp、uv cache 和 runtime 目录。手动创建或 `chmod` 会破坏 Windows ACL 继承。
- 不要用管理员 PowerShell、`takeown`/`icacls` 或手写 `Remove-Item` 收尾。出现这种需求说明
  `tasks.py` 有缺陷：保留证据，另开任务修复生命周期工具，再重跑同一个 `remove`。
- 不要手工创建 worktree/分支或绕过 PR 直接改 `main`，除非 owner 明确授权紧急例外。
- 不要为“机械增长”的质量 ratchet 反复推分支：先在本地解决或按 policy 记录审计，再推一次。
