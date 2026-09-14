# 工作流修复说明

本次按 owner 明确授权的例外修复任务生命周期工具，使用独立任务目录开发和验证，
免除这次修复的 Issue、Draft PR 和 Ready 循环。产品代码、版本号与发布逻辑不在范围内。

## 已修复的问题

| 原因 | 修复后的行为 |
| --- | --- |
| squash 合并后，main 后续修改使反向补丁检查失效 | `finish` 保存 repository、PR、最终 head、观察到的 PR base、merge SHA；按最终 head 和 main 可达性验证 |
| checkout 删除失败直接退出，GC 又永久保护它 | 保存 `checkout_pending_cleanup`，同一 `remove` 可以恢复 |
| 工件删除与 Git 收尾混在一起 | 先保存完成记录并删除任务分支，再处理工件；工件失败单独记为 `pending_cleanup` |
| 文档声称存在 janitor，实际没有调用入口 | 新增 `retry-cleanup`，`start` 与 `gc --apply` 处理到期队列；最多五次自动尝试 |
| Windows 文件占用被当作权限损坏 | 保留 sharing violation 错误，不先 chmod；ACL 恢复子进程增加 30 秒上限 |
| 报告与缓存一起删除 | 非临时文件移入 `.artifacts/task-deliverables/<slug>`，同名冲突保留两端并暂停 |
| 开发规范要求固定基线，但 `check` 默认比较移动中的 origin/main | `start` 保存基线 SHA，`check` 默认使用该 SHA |
| 已完成任务 slug 可被复用，旧凭据可能污染新任务 | 有历史 manifest、凭据或交付物的 slug 禁止复用 |
| 清理前缺少部分任务上下文检查 | 拒绝分支不符、未提交内容、进行中的 Git 操作和跨任务目录链接 |
| 遗留分支、目录和删除记录无法统一观察 | `status` 提供只读 JSON 清单，不因缺少 manifest 就声称任务可以删除 |

## 使用方式

以下命令从主 checkout 执行，Python 均使用项目 `.venv`：

```powershell
# 查看实际状态，不修改任务
.\.venv\Scripts\python.exe scripts/dev/tasks.py status

# 已在 GitHub 合并的 PR：保存凭据并收尾，可重复调用
.\.venv\Scripts\python.exe scripts/dev/tasks.py finish <slug> --pr <number>

# 已授权合并的 Ready PR：先确认完整门禁和精确 head，再合并并收尾
.\.venv\Scripts\python.exe scripts/dev/tasks.py finish <slug> --pr <number> --merge

# 查看队列，或立即重试一个已登记的待清理任务
.\.venv\Scripts\python.exe scripts/dev/tasks.py retry-cleanup
.\.venv\Scripts\python.exe scripts/dev/tasks.py retry-cleanup <slug> --apply
```

`finish --merge` 使用 GitHub CLI 的 `--match-head-commit`，不使用管理员绕过选项。
如果 GitHub 将合并排队，工具返回 `TASK_MERGE_PENDING`，稍后重试同一命令。
合并后分支新增提交会触发 `TASK_HEAD_MOVED`；未提交文件会触发 `TASK_DIRTY`。
这些情况需要先保留或处理代码，不能靠反复删除解决。

自动重试从 60 秒退避到最多一小时；五次仍不成功后需要显式重试。
没有安装常驻进程或系统计划任务。正在持有任务租约的应用和测试不会被强杀。
GitHub 评论与状态对账继续使用 `github_sync.py reconcile`，清理重试不发送消息。

## 验证证据

- `tests/test_task_workflow.py` 与 `tests/test_ci_worktree.py`：135 项通过。
- `scripts/ci/run.py --fast`：全仓 Ruff、异常策略、复杂度策略、mypy 通过，126 项核心测试通过。
- 使用真实 Git 临时仓库复现多提交 squash 后 main 重叠修改、删除失败、重复收尾及中断恢复。
- Windows 实际打开不允许删除共享的文件句柄：首次清理进入 pending，释放句柄后重试成功。
- 验证报告内容保留、dirty/new-head 拒绝删除、固定基线不随 origin/main 移动、重试预算和只读盘点。
- GitHub 合并调用通过隔离测试验证参数和门禁顺序；本次未为测试而合并远程 PR。

测试中有一条现有 Starlette/httpx 弃用提醒，不影响结果。本次没有运行完整产品回测或打包矩阵。

## 历史残余的边界

本次修复期间的只读盘点发现 38 个没有 manifest 的已登记历史 worktree，以及 19 份旧删除记录。
19 份记录中，多数对应的 checkout 和分支已经不存在；因此不能把删除记录数直接当作失败任务数。
这些记录有的仍关联分支或未登记目录，需要逐项确认 PR、最终提交及剩余代码价值。

本次不批量认定历史任务已被替代，不删除其他任务的未提交工作。已知合并 PR 可用新的 `finish`
补充凭据；无法匹配最终 head 的任务继续保留，先提取独立价值或取得明确弃置决定。
之前交付的项目审查 Markdown 仍保留在审查任务的工件目录中。

详细规则见 [开发工作流](development-workflow.md) 和 [GitHub 工作流](github-workflow.md)。
外部接口依据：[GitHub CLI merge](https://cli.github.com/manual/gh_pr_merge)、
[GitHub Pull Requests API](https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request)。
