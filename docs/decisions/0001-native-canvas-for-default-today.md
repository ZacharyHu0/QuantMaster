# 0001：默认 Today 使用专用 Canvas 图形

- 状态：Accepted
- 日期：2026-08-14
- 关联：[Issue #65](https://github.com/ZacharyHu0/QuantMaster/issues/65)、[Discussion #78](https://github.com/ZacharyHu0/QuantMaster/discussions/78)

## Context

默认 Today 行情页的恐贪仪表、恐贪历史和行情火花线使用 ECharts。它们让首屏必须下载
约 1.03 MB 的 ECharts；但这些图形只需要固定的仪表、折线、面积、参考线、标签和 tooltip，
不需要通用图表系统的缩放、图例或多系列编排。Issue #65 要求首屏 raw HTML、CSS、JS
不超过 1 MiB，并让 ECharts 只在高级图表所属视图首次激活时加载。

## Decision

默认 Today 的三类轻量图使用一个专用 Canvas 2D 模块，保留现有用户可见信息、ARIA、
tooltip、响应式缩放、reduced-motion 和 unmount 清理。允许字体度量和抗锯齿造成 1–3 px
视觉差异。K 线、Lab、Rotation 等高级图表在所属视图首次激活时再动态加载 ECharts。

测试通过用户可见 Canvas、文本、ARIA、tooltip、resize 和销毁行为验证，不保留
`getOption()` 或 ECharts 源码字符串合同，也不提供假 ECharts facade 或双渲染路径。

## Consequences

- 首屏不再下载 ECharts、通用图表脚本或图表样式。
- 默认行情图只有产品实际需要的固定能力；新增通用交互时应先证明它属于首屏。
- 高级图表首次打开会产生一次延迟加载，之后复用浏览器模块和资源缓存。
- Canvas 实例、监听器、observer 和动画帧必须由 Today adapter 在 unmount 时释放。

## Rejected alternatives

- **B：默认 Today 继续加载 ECharts。** 无法满足首屏资源和网络合同。
- **C：移除或点击后才显示现有轻量图。** 会改变默认 Today 的信息密度和主旅程。
