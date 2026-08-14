# GitHub 工作流规范

本文件是 QuantMaster 的 GitHub 项目管理操作规范。它与根目录 `AGENTS.md` 一起构成
所有 agent 必须遵守的仓库规则；PR、Issue 模板和 GitHub Actions 是同一规则的自动化入口。
如果上下文、聊天消息或个人习惯与本文件冲突，以 `AGENTS.md`、本文件和仓库配置为准。

## 1. 一个任务必须有一个管理记录

每个功能、缺陷、基础设施修复或独立重构都必须先创建 GitHub Issue，再创建任务 worktree。
Issue 至少记录：

- 目标、非目标、公开 seam、数据/schema 与回滚风险；
- 性能、包体或首屏预算（不适用时明确写 N/A）；
- 精确验收检查、父 Epic、相关 RFC Discussion 和依赖关系。

Issue 创建后必须设置负责人、合适的 label、`QuantMaster Structural Refactor` 等对应
milestone，并加入 v1.16.0 Project。跨任务依赖使用 GitHub 的 parent、blocked by/blocking
关系，不只在正文里写一段无法查询的文字。任务 slug、分支和 worktree 路径应在 Issue 或
关联 PR 中留下可追溯记录。

## 2. 开发与 PR

1. 使用 `scripts/dev/tasks.py start <slug>` 创建 `codex/<slug>` 分支和独立 worktree，固定
   一个开发基线；开发阶段不跟踪移动中的 `main`。
2. 首个完整任务提交推送后，立即创建 Draft PR，使用 `Closes #<issue>`，并关联父 Epic、
   RFC 和阻塞任务。
3. PR 必须设置 label、milestone、负责人和 Project；描述中保留精确测试命令、
   `tasks.py check`、最终 `tasks.py ready` 的 lane、包体/UI 证据、迁移风险和回滚步骤。
4. 请求 Copilot/配置的代码审查，等待 GitHub Actions 必需检查和审查结果；任何修复都在
   同一任务分支完成，并重新验证受影响证据。
5. 只有在一次性集成基线对齐、最终门禁通过、工作树干净后，才将 Draft PR 标记为 ready。

PR 是代码变更的权威验证报告；Issue 是范围与依赖的权威记录；Project 是阶段状态和优先级
的权威视图。三者不能互相替代。

## 3. Project、milestone 与状态同步

所有非临时任务都必须出现在 v1.16.0 Project，并保持以下状态同步：

| 事件 | Issue / PR | Project |
| --- | --- | --- |
| 开始实现 | Issue open，任务已分配 | `In progress` |
| Draft PR 已推送 | PR 链接 Issue | `In review` 或按看板规则标记 |
| CI/审查阻断 | Issue/PR 评论记录证据 | `Blocked`，并链接阻塞 Issue |
| PR squash 合并 | Issue 自动关闭或明确关闭 | `Done`，更新父 Epic 进度 |
| worktree 清理 | 记录清理结果 | 保留完成历史，不删除管理记录 |

状态改变时，agent 应同步 Issue、PR、Project 和父 Epic；不能只改其中一个。里程碑用于
发布批次，label 用于类型/风险筛选，Project 用于当前工作状态，三者含义不同。

## 4. Discussion 与证据化决策

跨多个任务的架构提案、产品边界和证据化取舍放在 GitHub Discussions。以下情况必须先发
RFC/决策帖并暂停受影响任务：不可逆 schema/数据迁移、硬包体预算与必需功能冲突、Rust
或 SciPy 基准无法决定去留。决策帖要包含候选方案、实测证据、回滚限制和推荐选项，并在
Issue/PR 中回链。当前主 RFC 为 Discussion #3。

## 5. 合并、tag 与 Release

- agent 负责维护 PR、处理审查、跟进 Actions、squash merge 和正常任务 worktree 清理；
  不得绕过 PR 直接改 `main`，除非 owner 明确授权紧急例外。
- 合并普通任务不自动产生版本、tag 或 GitHub Release；普通版本提交也必须经过 PR。
- agent 可以管理 tag 机制和候选冻结，但当前 `v*` tag workflow 会发布 GitHub Release，
  所以没有 owner 对具体版本、候选 SHA 和 Release 的明确确认，不得推送 release tag。
- owner 确认的是不可变候选 SHA，不是会继续移动的 `main`。发布前必须通过
  `scripts/release/sync.py` 的状态、候选和同步检查。
- 发布后 tag 默认不可变；同版本替换只能走仓库规定的失败 CI 恢复流程。

## 6. 完成检查表

在 agent 宣布任务完成前，逐项确认：

- [ ] Issue 已存在，范围/非目标/风险/预算/验收齐全；
- [ ] Issue 已设置 owner、label、milestone、Project、父/阻塞关系；
- [ ] 分支为 `codex/<slug>`，worktree 独立且开发基线固定；
- [ ] Draft PR 使用 `Closes #<issue>`，包含完整验证和回滚证据；
- [ ] 精确测试、`tasks.py check`、所需 `ready` lane 和 GitHub Actions 均有结果；
- [ ] Copilot/代码审查结果已处理；
- [ ] PR 已 ready 后才 squash merge，Issue/Project/Epic 已同步；
- [ ] 使用 `tasks.py remove <slug>` 清理，未手动删除 worktree；
- [ ] 没有未经 owner 确认的 Release tag 或 GitHub Release。

## 7. 仓库入口

- agent 强制规则：[AGENTS.md](../AGENTS.md)
- 开发、集成和清理：[development-workflow.md](development-workflow.md)
- Issue 模板：`.github/ISSUE_TEMPLATE/`
- PR 模板：`.github/pull_request_template.md`
- CI 与发布 workflow：`.github/workflows/`
- 人类贡献者入口：[CONTRIBUTING.md](../CONTRIBUTING.md)
