Closes #

## 变更摘要

<!-- 先写用户或系统得到的结果，再写实现；非目标单列。 -->

- 新增 / 修改：
- 删除 / 替代：
- 非目标：

## 风险与回滚

- 数据 / schema / 迁移影响（无则写“无”）：
- 主要风险与失败信号：
- 回滚步骤：

## 验证

### 精确测试

```text
<完整命令>
<结果，例如：29 passed>
```

### Task workflow

- [ ] 已在任务 worktree 使用主 checkout 的 `.venv` 运行 `scripts/dev/tasks.py check`。
- [ ] 已只对齐一次 integration baseline，任务改动已提交且 worktree 干净。
- [ ] 已在 Draft 状态推送 aligned commit，且 Draft fast/core 已通过。
- [ ] 已标记 Ready，并等待同一 commit 的完整 CI 通过。
- [ ] 已运行 `scripts/dev/tasks.py ready --accept-ci`（复用同一 commit 的绿色 CI），
      或无 CI 时本地运行 `tasks.py ready`；审查通过后执行 squash merge。

Ready lanes（实际运行的勾选；不适用留空并说明）：

- [ ] `--ui`
- [ ] `--rust`
- [ ] `--package`
- 不适用说明：

## 证据（仅适用项填写，否则写 N/A）

- UI 截图（桌面 + 受影响窄屏）：
- 包体 / 首屏 / 启动指标与归因：
- 测量环境与命令：

## 发布授权

- [ ] 本 PR **不**创建或替换 Git tag / GitHub Release。
- [ ] Owner 已另行明确授权发布，并已记录候选 SHA、版本和发布日期。
