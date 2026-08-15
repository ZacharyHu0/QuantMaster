---
status: accepted
---

# Windows frozen readiness 与布局启动预算

Discussion #95 的 Q4=A 确认：`GET /api/v1/health` 保持 HTTP 200 的进程存活语义，并直接附带不访问完整诊断、资讯、轮动或远端服务的 `core_ready` 投影；`/api/v1/diagnostics` 继续承载可延迟完成的完整运行诊断。Windows 当前默认发布路径保留 onefile，`--help` 与 Web `core_ready` 都必须在 20 秒内完成；最终安装 onedir 的 `--help` 必须在 1.5 秒内完成，所有门禁都报告实测延迟且超限即失败。

Windows onefile 使用 PyInstaller 官方早期 Splash，使单 EXE 在自解压期间立即提供反馈。Splash 直接显示 bootloader 正在解压的真实文件名，Python 入口只更新真实阶段；Web 在 Uvicorn 监听成功且轻量 `core_ready` 后关闭，CLI 在进入具体 handler 前关闭，错误退出由入口兜底关闭。不显示百分比或确定性进度条。外部自动化可设置 `PYINSTALLER_SUPPRESS_SPLASH_SCREEN=1`。onedir 与 POSIX 产物不装配 Splash。

## Considered Options

- 通过 `/diagnostics` 推断 `core_ready`：拒绝，因为可选数据诊断会污染核心就绪门禁。
- 对所有 frozen 布局统一使用 1.5 秒：拒绝，因为 onefile 自解压成本与 onedir 启动路径不同。
- onefile 保持过短门禁且无早期反馈：拒绝；真实本地与 CI 单 EXE 已证明自解压会稳定越界，且用户无法判断进程是否仍在启动。
- onefile 只报告不阻断：拒绝；过渡布局也必须有明确的 20 秒硬上限。
- 自建 launcher、伪造百分比或固定时长动画：拒绝；官方 bootloader 已提供更早、更真实且更少代码的反馈。

## Consequences

默认 Windows onefile 必须同时通过 20 秒 help、20 秒 Web `core_ready`、Splash 先可见并在监听且 `core_ready` 后关闭、三进程精确身份和协调退出验证。onedir 仍是无 Splash 的显式测量路径；成为默认安装布局前必须满足 1.5 秒 help、包体和对应 frozen smoke 合同。
