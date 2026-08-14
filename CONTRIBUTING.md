# Contributing to QuantMaster

开始任何代码、文档、测试或 CI 工作前，请先阅读：

1. [AGENTS.md](AGENTS.md) — 所有 agent 和贡献者必须遵守的仓库规范；
2. [docs/github-workflow.md](docs/github-workflow.md) — GitHub Issue、Project、Discussion、
   Draft PR、Actions、审查、合并、tag 和 Release 流程；
3. [docs/development-workflow.md](docs/development-workflow.md) — worktree、固定基线、验证、
   集成和清理细则。

QuantMaster 使用 GitHub Issue 作为任务范围记录、Project 作为状态看板、Discussion 作为
跨任务决策记录、Draft PR 作为变更与验证报告。任务必须在 Issue 建立后，通过
`scripts/dev/tasks.py start <slug>` 创建 `codex/<slug>` 独立 worktree；不得直接在 `main`
开发或绕过 PR 合并。

agent 负责正常 PR 维护、审查修复、Actions 跟进、squash merge、tag 机制和 worktree 清理。
由于当前 release tag 会触发 GitHub Release，任何 Release tag/发布动作都必须等 owner 对
具体候选 SHA 和 Release 明确确认。普通合并不会自动发布。

提交 PR 前请使用仓库模板记录精确测试、`tasks.py check`、最终 `ready` lane、包体/UI 证据、
风险和回滚方式。遇到不可逆迁移、硬预算冲突或基准无法决策时，先在 RFC Discussion 中
记录证据化决策，再继续受影响任务。
