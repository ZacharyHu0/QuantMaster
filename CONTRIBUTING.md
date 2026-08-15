# Contributing to QuantMaster

开始任何代码、文档、测试或 CI 工作前，请先阅读：

1. [AGENTS.md](AGENTS.md) — 所有 agent 和贡献者必须遵守的仓库规则（任务生命周期、分层
   验证、状态对账与发布授权）；
2. [docs/development-workflow.md](docs/development-workflow.md) — worktree、固定基线、
   开发与集成两阶段、验证分层和清理细则；
3. [docs/github-workflow.md](docs/github-workflow.md) — GitHub Issue、Draft PR、Actions、
   状态对账、合并、tag 和 Release 流程。

QuantMaster 使用 GitHub Issue 作为任务范围记录、Draft PR 作为变更与验证报告、Discussion
作为跨任务决策记录。任务必须先在 Issue 建立，再通过 `scripts/dev/tasks.py start <slug>`
创建 `codex/<slug>` 独立 worktree；不得直接在 `main` 开发或绕过 PR 合并。

开发期只跑 `tasks.py check` 影响集；Draft PR 只触发 CI 快检，标记 Ready 后运行完整矩阵。
集成时用 `tasks.py ready --accept-ci` 复用同一 commit 的绿色 CI，无 CI/网络时才本地运行
`tasks.py ready`。Issue/PR/Project 状态用 `scripts/dev/github_sync.py reconcile` 对账，默认
dry-run，`--apply` 只执行脚本列出的安全修复。

任务分支不修改 `quantmaster/release.py` 或 `CHANGELOG.md`；版本变更由 owner 要求时单独
版本 PR 完成。由于当前 release tag 会触发 GitHub Release，任何 tag/发布动作都必须等 owner
对具体候选 SHA 和 Release 明确确认。

遇到不可逆迁移、硬预算冲突或基准无法决策时，先在 RFC Discussion 中记录证据化决策，再
继续受影响任务。
