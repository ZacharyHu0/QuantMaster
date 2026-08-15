# GitHub 工作流规范

本文件是 QuantMaster 的 GitHub 项目管理操作规范，与根目录 `AGENTS.md` 一起构成仓库规则。
核心原则：**Issue/PR 是权威记录，Project 是阶段状态，状态对账由脚本自动完成**；
上下文、聊天消息或个人习惯与本文件冲突时，以 `AGENTS.md`、本文件和仓库配置为准。

## 1. 一个任务一个管理记录

每个功能、缺陷、基础设施修复或独立重构必须先创建 GitHub Issue。Issue 模板负责收集范围、
非目标、公开 seam、数据/迁移风险、性能预算和验收检查；agent 创建 Issue 时设置 owner、
类型/风险 label 和 milestone，并把任务 slug、分支和 worktree 路径回链到 Issue。

任务开始后使用 `scripts/dev/tasks.py start <slug>` 创建 `codex/<slug>` 分支和独立 worktree，
固定一个开发基线；开发阶段不跟踪移动中的 `main`。

## 2. 开发与 PR

1. 首个完整任务提交推送后，立即创建 Draft PR，使用 `Closes #<issue>`，按 PR 模板填写。
2. Draft 阶段 CI 只运行 `fast-gate` 与跨平台 `core`：Ruff、异常/复杂度策略、mypy 和核心
   契约测试。完整覆盖分片、native parity、browser 和 Windows 打包等重型 lane 在 PR 标记
   Ready 后才运行。
3. 运行 `python scripts/dev/github_sync.py reconcile`（默认 dry-run）检查 Issue/PR 状态；
   脚本列出的安全修复用 `--apply` 执行。需要人工决策的项必须处理到有明确结论。
4. 开发完成后只对齐一次 integration baseline，运行 `tasks.py ready --accept-ci` 复用同一
   commit 的绿色 GitHub Actions 证据；无 CI/网络时才本地运行 `tasks.py ready`。随后标记
   Ready，等待完整 CI 与审查。
5. 任何修复都在同一任务分支完成；涉及 UI、Rust 或打包时在 PR 中勾选相应 Ready lane。
   修复后重新验证受影响证据，不要盲目重跑全套。

PR 是代码变更的权威验证报告，Issue 是范围与依赖的权威记录，Project 是阶段状态视图。

## 3. 状态对账：`github_sync.py reconcile`

所有 agent 与维护者使用同一个对账入口。默认 dry-run 只报告；`--apply` 仅执行以下安全修复：

| 发现 | 安全修复 | 例外 |
| --- | --- | --- |
| PR 已合并但 `Closes` 的 Issue 仍 open | 评论并关闭 Issue | Issue 带 `blocked` 标签时只报告 |
| 多个 open Issue 标题完全相同 | 给较新的 Issue 加 `duplicate` 标签并关闭 | 保留最早记录 |
| Draft PR 超过 48 小时未更新 | 在 PR 评论提醒 | 不自动转状态 |
| 其他 Project/label/milestone 不一致 | 输出精确的修复建议 | 需要 owner 或看板权限时报告 |

Project 状态只在阶段边界手工更新：任务开始 `In progress`、PR Ready 后 `In review`、遇到阻塞
`Blocked`（必须附评论和解除条件）、合并后 `Done`。不要为每次 checkpoint push 更新 Project。

## 4. Discussion 与证据化决策

跨多个任务的架构提案、产品边界和证据化取舍放在 GitHub Discussions。以下情况必须先发
RFC/决策帖并暂停受影响任务：不可逆 schema/数据迁移、硬包体预算与必需功能冲突、Rust 或
SciPy 基准无法决定去留。决策帖包含候选方案、实测证据、回滚限制和推荐选项，并在 Issue/PR
中回链。当前主 RFC 为 Discussion #3。

## 5. 合并、tag 与 Release

- agent 负责维护 PR、处理审查、跟进 Actions、squash merge 和任务 worktree 清理；不得绕过
  PR 直接改 `main`，除非 owner 明确授权紧急例外。
- 普通任务合并不自动产生版本、tag 或 Release，也**不修改** `quantmaster/release.py` 或
  `CHANGELOG.md`。版本变更只在 owner 明确要求时，由单独的版本 PR 一次完成。
- 当前 `v*` tag workflow 会发布 GitHub Release，因此没有 owner 对具体版本、候选 SHA 和
  Release 的明确确认，不得推送 release tag。发布前后使用 `scripts/release/sync.py` 的
  status/cut/publish 检查。

## 6. 完成检查表

宣布任务完成前，逐项确认：

- [ ] Issue 已存在，范围/非目标/验收齐全，并链接任务 slug；
- [ ] 分支为 `codex/<slug>`，独立 worktree，开发基线固定；
- [ ] Draft PR 使用 `Closes #<issue>`，按模板填写验证与回滚证据；
- [ ] Draft 快检与 Ready 后完整 CI 均绿；`tasks.py ready --accept-ci`（或本地 ready）已记录；
- [ ] PR Ready 后 squash merge，Issue/Project 状态已同步；
- [ ] `tasks.py remove <slug>` 已清理，无手工删除的 worktree 残余；
- [ ] 没有未经 owner 确认的 Release tag 或 GitHub Release。

## 7. 仓库入口

- agent 强制规则：[AGENTS.md](../AGENTS.md)
- 开发、集成和清理：[development-workflow.md](development-workflow.md)
- 状态对账脚本：`scripts/dev/github_sync.py`
- Issue 模板：`.github/ISSUE_TEMPLATE/`
- PR 模板：`.github/pull_request_template.md`
- CI 与发布 workflow：`.github/workflows/`
- 人类贡献者入口：[CONTRIBUTING.md](../CONTRIBUTING.md)
