# Issues #28, #30, #31, #32 综合研究分析

## 概述

这四个 issue 均归属于 [Discussion #22](https://github.com/ZacharyHu0/QuantMaster/discussions/22)（QuantMaster v1.16.1 experience polish），属于前端体验增强，风险均为 `risk:low`，主要由 `enhancement` / `refactor` / `frontend` 标签组成。

它们共享同一个目标：让用户在现有静态前端（FastAPI + ECharts + 原生 JS）中更流畅地获取市场/板块/个股/收盘后数据，并保证数据时可追溯、质量可感知、正式资格可判断。

---

## #28 — feat(ui): unify market data-state and snapshot view models

### 收益

- **一致性**：当前 market、rotation、stock-analysis、after-close 四个页面各自独立拼装 API 响应、加载态、错误提示和质量指示器，用户无法快速判断当前数据是 `fresh`、`stale`、`partial` 还是 `degraded`，也无法确认 formal eligibility。统一后四个页面行为一致。
- **可维护性**：消除重复代码，减少未来添加新页面时的样板代码。
- **可测试性**：集中 adapter 便于单元测试和 contract test。
- **可扩展性**：后续 #30、#31、#32 都可以复用这个统一 view-model 层。

### 可行性

**中等**。当前前端是纯 JS，没有框架。关键挑战：

1. 需要识别四个页面当前如何处理 API 响应（`loadMarket`、`loadRotation`、`loadStockAnalysis`、`loadAfterClose`）。
2. 创建 `data-state.js`（约 15–20 KB），包含：
   - `DataState` 类：封装 `state`、`data`、`asOf`、`coverage`、`quality`、`formalEligible`、`loading`、`error`。
   - 请求去重：对相同 `endpoint + asOf` 的并发请求合并为一次。
   - 超时与 stale 检测。
3. 修改四个页面，将原有 API 调用替换为 `DataState` 适配器。
4. 补充 focused UI/API contract tests。

静态 JS 预算 25 KB 绰绰有余（预计 15–18 KB）。

### 方案

```
quantmaster/server/static/data-state.js  ← 新增
├── class DataState
│   ├── constructor(path, { dedupAsOf, timeout })
│   ├── fetch()                         ← 返回 Promise，自动去重
│   ├── state: 'loading' | 'ready' | 'stale' | 'partial' | 'degraded' | 'unavailable'
│   ├── data                            ← 解析后的载荷
│   ├── asOf, coverage, quality, formalEligible
│   └── onStateChange(callback)         ← 监听状态变化
├── function createDataState(path, opts)  ← 工厂，自动管理缓存
└── function formatState(state)         ← 统一状态文案
```

各页面改动：
- `market.js`：`loadMarket()` → 内部使用 `createDataState('/api/v1/market/overview')`，展示统一状态栏。
- `rotation.js`：同理。
- `stock-analysis.js`：同理。
- `after-close.js`：同理。

### 评估

**推荐程度：★★★★★**（最高）
- 收益高，风险低，预算充足。
- 是其他三个 issue 的底层依赖（统一 view-model 后，#30 的上下文传递、#32 的快照绑定都能复用）。
- 可在一个 worktree 内独立完成，不涉及后端 schema 或迁移。

---

## #30 — feat(ui): connect market, board, and stock evidence drilldowns

### 收益

- **减少上下文丢失**：用户从市场总览 → 板块 → 个股 → 收盘后选股时，当前需要手动切换页面，容易丢失 snapshot/as-of/板块语境。
- **提高效率**：深链接让用户一键直达关联数据，减少重复操作。

### 可行性

中等。主要挑战：

1. **URL 路由**：当前是 hash-based SPA（`#observe/quotes`），需要扩展为支持参数（如 `#observe/quotes?board=cn&as-of=2026-08-15`）。
2. **板块详情页**：需要新增 `board-detail` 视图，展示 PIT 成分覆盖、历史表现/资金流摘要、成员入口。这需要前端页面 + 后端 API（可能已有 `/api/v1/board/*` 端点）。
3. **上下文传递**：market/rotation 页面需在导航时携带 `board`、`as-of`、`snapshot` 等参数。
4. **测试**：新增 Playwright 浏览器测试覆盖深链接路径。

### 方案

```
扩展 workspace-loader.js 路由解析：
- 支持 URL 参数：`#observe/quotes?board=cn&as-of=2026-08-15`
- 导航时统一携带 snapshot pointer

新增 board-detail 视图：
- 复用现有 rotation API 的板块数据
- 展示历史曲线（ECharts 或 Canvas）、成员列表、证据状态

修改 market.js / rotation.js：
- 板块/个股链接改为 `href="#observe/quotes?board=..."`
- activateTab 时读取参数并传递到目标页面
```

### 评估

**推荐程度：★★★★☆**
- 收益较高，但工作量较大，且部分依赖 #28 的 view-model 统一。
- 板块详情页需要后端 API 保证（现有 `/api/v1/rotation/board` 可能已可用）。
- 建议在 #28 完成后启动。

---

## #31 — feat(portfolio): add grouped watchlist workflows and batch actions

### 收益

- **提升组合管理效率**：用户可创建分组、拖拽排序、批量导入/导出标的、设置列视图、带冷却的提醒。
- **减少重复操作**：批量添加/移除、导入导出减少逐只操作。

### 可行性

**中高**。主要挑战：

1. **分组管理**：需要后端 AssetListStore 扩展支持分组（group 字段），前端需分组拖拽 UI。
2. **批量操作**：现有 API 可能每只标的逐个请求，需要新增批量端点。
3. **导入导出**：CSV 导入解析，符号规范化，错误报告。
4. **提醒冷却**：若现有提醒契约允许，增加冷却/合并逻辑。
5. **体积预算**：静态 JS ≤ 30 KB，需要精打细算。

### 方案

```
后端：
- AssetListStore 增加 group 字段
- 新增批量导入/导出 API
- 提醒冷却逻辑（若需要）

前端：
- portfolio.js 扩展分组 UI
- 使用原生 HTML5 Drag & Drop
- 批量导入对话框（CSV 粘贴/上传）
- 列配置面板
```

### 评估

**推荐程度：★★★☆☆**
- 收益中等，但复杂度最高，涉及前后端 schema 变化。
- 体积预算 30 KB 较紧，需要精心设计。
- 建议在核心体验 issue (#28) 完成后考虑。

---

## #32 — feat(research): bind saved screening schemes to immutable snapshots

### 收益

- **可复现性**：当前筛选方案只保存 filter JSON，重跑可能落到不同日期/覆盖/规则版本。绑定快照后，用户可精确复现同一时间点的结果。
- **正式资格**：绑定 snapshot/as-of 后，结果可被认定为 formal eligible，进入正式研究链。
- **旧方案兼容**：无绑定字段的旧方案自动标记为 `unbound/preview-only`，不会误用。

### 可行性

**中高**。主要挑战：

1. **后端 schema**：`saved_schemes` 表或证据字段需要扩展以存储 `snapshot_id`、`as_of`、`rule_version`、`coverage`、`provenance`。
2. **API 端点**：新增 `GET/POST /api/v1/research/selection/schemes` 支持绑定快照。
3. **前端 UI**：`candidates.js` 和 `after-close.js` 需展示绑定状态、支持保存/重跑、进度显示。
4. **旧方案迁移**：读取旧方案时缺字段标记为 `unbound`，不自动升级。
5. **快照目录**：需要确保 snapshot catalog 存在且可寻址。

### 方案

```
后端：
- 定义 ScreeningSchemeEvidence 模型（filter, snapshot_id, as_of, rule_version, coverage, quality, provenance）
- 持久化到 after-close 证据链或单独 scheme store
- 新增 API 端点
- 旧方案兼容：读取时缺字段标记为 unbound

前端：
- after-close.js 保存方案时携带 snapshot 元数据
- candidates.js 展示方案绑定状态
- 支持按原快照重跑
- 进度/取消/恢复
```

### 评估

**推荐程度：★★★★☆**
- 收益高（正式研究链的关键环节），但实现复杂度较高，涉及前后端 schema 和持久化。
- 部分依赖 #28 的 view-model 统一（方案列表展示统一状态）。
- 建议在 #28 完成后启动，但可以独立进行。

---

## 总结与建议顺序

| 优先级 | Issue | 收益 | 复杂度 | 依赖 | 推荐 |
|--------|-------|------|--------|------|------|
| 1 | #28 | 高 | 中 | 无 | **★★★★★** |
| 2 | #32 | 高 | 中高 | #28（可选） | **★★★★☆** |
| 3 | #30 | 中高 | 中 | #28 | **★★★★☆** |
| 4 | #31 | 中 | 高 | 无 | **★★★☆☆** |

**建议第一步**：从 `#28` 开始，创建共享 data-state view-model，这是后续所有体验增强的基础，且独立、风险低、预算充足。

---

## 下一步

我已决定为 `#28` 启动 Draft PR，因为它：
1. 收益最高（四个页面统一 data-state 展示）
2. 风险最低（不涉及后端 schema、迁移）
3. 是其他三个 issue 的底层依赖
4. 预算宽松（25 KB JS 绰绰有余）

待 `#28` 完成后再评估 `#32` 和 `#30` 的顺序。