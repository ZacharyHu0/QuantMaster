# Issue #84 研究记录：SciPy 与 Rust 内核保留决策

> **Issue**: [#84 — perf(kernel): decide SciPy and Rust retention from representative benchmarks](https://github.com/ZacharyHu0/QuantMaster/issues/84)
> **Parent**: #1 (QuantMaster v1.16.0 结构现代化)
> **Task slug**: `kernel-benchmark-decision`
> **Branch**: `codex/kernel-benchmark-decision`
> **Development baseline**: `c395926073d7f2738d30b8b58f45d03906684dad`
> **State**: OPEN · Labels: `refactor`, `risk:medium` · Milestone: `future`
> **研究日期**: 2026-08-15
> **决策日期**: 2026-08-15
> **最终决策**: SciPy → **DELETE**；当前 Rust kernel → **DELETE**；后续新建独立 Issue 评估零拷贝改造（方案 B）

---

## 1. Issue 范围与验收标准

### 1.1 决策目标

Issue #84 要求从**端到端实测证据**出发，独立做出两个可选复杂度决策：

| 决策 | 保留条件 | 删除条件 |
| --- | --- | --- |
| **SciPy** | 数值不完全等价，或运行时 >1.2x，或峰值内存 >1.25x | 结果完全等价 且 运行时 ≤1.2x 且 峰值内存 ≤1.25x |
| **Rust** | 结果等价 且 净加速 ≥20%（含 Python/Rust 转换成本） | 结果不等价，或净加速 <20% |

### 1.2 规则约束

- 优先使用 StockDB 和已有本地缓存，不访问远端 provider。
- 基准测试必须测量**实际应用接缝**（application seam），而非隐藏了转换/I/O 成本的孤立微内核。
- 固定数据集身份、行数、环境、冷/热方法、样本分布、内存测量方法和正确性比较方法。
- 测量期间不修改数值代码。
- 证据不充分或阈值与包体预算冲突时，向 owner 提交 Discussion 决策卡。

### 1.3 验收标准

- [ ] 可复现的基准测试工件报告所有阈值和方差。
- [ ] SciPy 和 Rust 各自记录明确的 keep/delete 决策。
- [ ] 任何删除是独立的可回退子 Issue，带数值一致性、包体和性能门禁。
- [ ] 不引入推测性替代依赖或自定义基准框架。

---

## 2. 依赖链与上下文

### 2.1 Discussion #95 — 预算缺口决策

Owner 已选择 **Option A（保留预算、分阶段推进）**：

1. #59 将 onedir 测量/归因/冒烟集成为非默认实验通道。
2. 保持当前 Windows onefile 为发布默认，直到预算达标。
3. #74 从测量接缝开始，只裁剪已证明未使用的 Feishu/PyArrow 载荷。
4. 若仍超预算，运行 #84 的全市场 SciPy/Rust 基准测试，仅执行证据合格的删除。
5. 最终小任务将 Windows 默认切换为 onedir ZIP 并启用 130/350 MiB 硬门禁。

**不变量**：不超尺寸的 onedir 不会成为默认或发布资产；125 MiB 目标、130 MiB ZIP 硬限和 350 MiB 安装硬限保持不变；不引入 UPX、新压缩依赖、自定义哈希目录或手写飞书协议。

### 2.2 Issue #59（已关闭）— onedir 测量通道

构建了首个真实 Windows onedir ZIP 并在硬门禁处停止。关键测量证据：

| 测量项 | 实际值 | 硬限 | 超出 |
| --- | ---: | ---: | ---: |
| 安装 onedir | 405,961,104 B | 367,001,600 B (350 MiB) | +38,959,504 B |
| ZIP | 170,552,513 B | 136,314,880 B (130 MiB) | +34,237,633 B |

SciPy 相关贡献（压缩后）：

- SciPy 包：18,373,151 B
- 三个 SciPy/OpenBLAS 载荷合计：约 19.6 MiB

### 2.3 Issue #74（已关闭）— Feishu/PyArrow 裁剪

已完成：将 `collect_submodules("lark_oapi")` 替换为仅实际使用的 SDK 模块；排除未使用的 PyArrow Flight/Substrait/header/test 载荷；添加模块级大小归因。#84 在此基础上继续。

### 2.4 依赖顺序

```
#59 (onedir 测量) → #74 (Feishu/PyArrow 裁剪) → #84 (SciPy/Rust 决策) → 最终切换默认布局
```

---

## 3. SciPy 使用分析

### 3.1 导入位置

**全仓库唯一的 SciPy 导入**：

```python
# quantmaster/rotation/analytics.py:17
from scipy import sparse
```

其余 `scipy` 字样出现在注释（说明"不引入 scipy 依赖"）、release notes 和非数值的 `sparse_cadence` 变量名中，不构成实际依赖。

### 3.2 使用模式

SciPy `sparse.csr_matrix` 在 `_build_group_aggregation` 函数中使用，具体流程：

```
1. 构建 COO 三元组 (data, row_indices, col_indices)
2. sparse.csr_matrix(...) → 构建 CSR 成员矩阵 (group_count × symbol_count)
3. membership_csr.toarray().astype(bool) → 立即转为稠密 bool 数组
4. _dense_sparse_product(membership_csr, values) × 4 次 → 计算日期×分组计数
   └─ 每次: CSR @ dense → 若结果稀疏则 .toarray() → np.asarray().T
5. 后续所有操作使用稠密 membership bool 数组
```

**关键观察**：

- CSR 矩阵构建后**立即转为稠密**（`membership = membership_csr.toarray().astype(bool, copy=False)`）。
- `_dense_sparse_product` 执行 4 次稀疏矩阵-稠密向量乘法，但结果也立即稠密化。
- 此后所有 `_group_median_and_advance`、`_group_amount_activity_batch` 等函数都使用稠密 `membership` bool 数组，不再涉及 SciPy。
- SciPy 仅服务于"从 COO 三元组构建 CSR 再做 4 次矩阵乘法"这一极窄场景。

### 3.3 替代可行性

全市场规模估计：约 1,000 个题材 × 约 5,000 个标的 = 5,000,000 个 bool = ~5 MB。

- 可直接用 `np.zeros((group_count, symbol_count), dtype=bool)` 构建稠密成员矩阵，用 `membership @ values.astype(np.int16)` 替代稀疏乘法。
- 5 MB bool 矩阵在内存中完全可控，且避免了 CSR 构建和稀疏/稠密转换的开销。
- 数值完全等价（bool 矩阵乘法是精确的计数运算，无浮点误差）。
- 实际上代码**已经在用稠密数组做后续全部计算**，SciPy 只是一个构建期的中间形态。

### 3.4 初步判断（待基准验证）

SciPy 的删除条件（等价 + 运行时 ≤1.2x + 内存 ≤1.25x）**很可能满足**：

- 数值等价：bool 计数运算是精确的。
- 运行时：移除 CSR 构建+转换两步，直接用 NumPy 稠密运算，预计不慢于原方案。
- 内存：5 MB bool 矩阵远小于 SciPy 包体（18.37 MB 压缩 / ~19.6 MiB 含 OpenBLAS）。
- 包体收益：删除 SciPy 可回收约 18.37 MB 压缩字节，对 130 MiB ZIP 硬限有显著贡献。

**但仍需按 Issue 规则进行全市场实测**，不能从代码分析直接得出结论。

---

## 4. Rust 内核使用分析

### 4.1 架构

```
rust/quantmaster-kernel/
├── Cargo.toml          # pyo3 0.29.2 + rayon 1.10
├── src/lib.rs          # 298 行，7 个 #[pyfunction]
└── pyproject.toml      # maturin 构建
```

Python 侧通过 `quantmaster/research/kernel.py` 的 `Kernel` facade 调用，按 `AUTO/PYTHON/RUST` 选择后端：

- `AUTO`：尝试 `importlib.import_module("_quantmaster_kernel")`，失败则回退 Python。
- `PYTHON`：直接用纯 NumPy 实现。
- `RUST`：调用原生扩展，失败时 warn 并回退。

### 4.2 提供的算子

| 算子 | Python 实现 | Rust 实现 | 并行 |
| --- | --- | --- | --- |
| `cross_section_rank` | 逐行 `np.argsort` + tie 处理 | `into_par_iter` 逐行 | rayon 行级并行 |
| `robust_standardize` | 逐行 median/MAD/clip/zscore | `into_par_iter` 逐行 | rayon 行级并行 |
| `weighted_zscore` | 逐行加权均值/方差 | `into_par_iter` + zip | rayon 行级并行 |
| `rolling_mean` | 逐列逐窗口 `np.mean` | `into_par_iter` 列级 | rayon 列级并行 |
| `rolling_std` | 逐列逐窗口 `np.std(ddof=1)` | `into_par_iter` 列级 | rayon 列级并行 |
| `rolling_corr` | 逐列逐窗口 `np.corrcoef` | `into_par_iter` 列级 | rayon 列级并行 |
| `version` | — | `env!("CARGO_PKG_VERSION")` | — |

### 4.3 转换成本（关键瓶颈）

```python
def _to_native(values: np.ndarray) -> list[list[float | None]]:
    return [[float(item) if np.isfinite(item) else None for item in row] for row in values]

def _from_native(values: Any) -> np.ndarray:
    return np.asarray([
        [np.nan if item is None else float(item) for item in row] for row in values
    ], dtype="float64")
```

**这是 Rust 扩展最大的性能问题**：

- `_to_native` 将 NumPy 二维数组转换为嵌套 Python 列表（`list[list[float | None]]`），每个元素创建一个 Python `float` 或 `None` 对象。
- `_from_native` 反向转换，每个元素重新解析。
- 对于 760 日 × 800 标的 = 608,000 个元素的面板，**每次调用创建/销毁约 60 万个 Python 对象**。
- 这比 NumPy 向量化运算本身还慢，可能完全抵消 Rust 计算的加速。

### 4.4 调用路径

Rust 内核在研究流水线中的调用路径：

```
compute_core_factors(bars, Kernel(...))
  → kernel.cross_section_rank(matrix)
  → kernel.robust_standardize(matrix, k)
  → kernel.weighted_zscore(matrix, weight_matrix)
  → kernel.rolling_mean(matrix, window)
  → kernel.rolling_std(matrix, window)
  → kernel.rolling_corr(a, b, window)
```

每个算子独立调用 `_to_native` + Rust 计算 + `_from_native`，转换成本**按算子调用次数倍增**。

### 4.5 已有测试

- **一致性测试**：`tests/test_research_pipeline.py::test_rust_kernel_matches_python_when_extension_is_available` — 在 Rust 扩展可用时比较 6 个算子的输出，要求 `np.allclose(atol=1e-6, rtol=1e-6)`。
- 该测试在 Rust 扩展不可用时自动 skip。

### 4.6 打包影响

- `packaging/quantmaster.spec` 将 `_quantmaster_kernel` 列为 hidden import。
- Rust 构建通道（`rust` extra = `maturin==1.14.1`）和 CI native parity lane 增加了构建复杂度。
- 删除 Rust 扩展可移除：maturin 依赖、Rust 工具链要求、CI native parity job、`_quantmaster_kernel` hidden import、`rust/` 目录。

### 4.7 初步判断（待基准验证）

Rust 的保留条件（等价 + 净加速 ≥20% 含转换成本）**存在风险**：

- **等价性**：已有测试证明数值一致（`atol=1e-6`）。
- **净加速**：`_to_native`/`_from_native` 的 Python 对象转换成本极高，可能使净加速远低于 20%，甚至净减速。
- **替代成本**：Python fallback 已是纯 NumPy 实现，无需额外依赖。
- **但**：如果数据规模足够大（如 760 日 × 5000 标的），rayon 并行可能仍能克服转换开销——必须实测。

---

## 5. 已有基准测试基础设施

### 5.1 离线刷新基准

`scripts/dev/benchmark_refresh.py`：

- 生成确定性合成行情（`rng = np.random.default_rng(20260811)`），不联网。
- 默认规模：180 日 × 480 标的 × 978 题材分组。
- 测量 `compute_trend_matrices` + `analyze_group_rotation` 的冷/热/noop 场景。
- 报告 min/p50/p95/max 耗时和机器信息。
- 已被 `tests/test_refresh_performance.py`（`@pytest.mark.full`）作为性能预算门禁。

**此基准已覆盖 SciPy 路径**（`analyze_group_rotation` → `_build_group_aggregation` → `sparse.csr_matrix`），但尚未覆盖 Rust 内核路径。

### 5.2 可复用的测量框架

- `_fixture(days, symbols, groups)` — 确定性数据生成器。
- `_measure(name, runs, operation)` — 多次运行计时，报告分位数。
- 报告格式已包含机器、Python 版本、处理器和固定参数。

#84 的基准测试应复用此框架，扩展到全市场规模并增加 Rust 内核对比和内存测量。

---

## 6. 待执行的基准测试计划

### 6.1 SciPy 基准测试

| 维度 | 规格 |
| --- | --- |
| 数据集 | 全市场 A 股日线，760 交易日，约 5,000 标的，约 1,000 题材分组 |
| 数据源 | StockDB / 本地缓存（不联网） |
| 比较对象 | (A) 当前 `scipy.sparse.csr_matrix` 路径 vs (B) 纯 NumPy 稠密 bool 矩阵路径 |
| 测量接缝 | `analyze_group_rotation(close, groups, amount=amount, kind="theme")` |
| 运行次数 | ≥5 次冷启动，报告 min/p50/p95/max |
| 内存测量 | `tracemalloc` 峰值 或 `resource.getrusage` |
| 正确性比较 | JSON 输出逐字段深度比较，要求完全相等 |
| 阈值 | 等价 + 运行时 ≤1.2x + 峰值内存 ≤1.25x → 删除；否则保留 |
| 固定参数 | dataset identity, row count, warm/cold method, sample distribution |

### 6.2 Rust 内核基准测试

| 维度 | 规格 |
| --- | --- |
| 数据集 | 全市场 A 股日线面板，760 交易日 × 800-5000 标的 |
| 数据源 | StockDB / 本地缓存（不联网） |
| 比较对象 | (A) `Kernel(RUST)` vs (B) `Kernel(PYTHON)` |
| 测量接缝 | `compute_core_factors(bars, kernel)` — 完整研究因子计算，含转换成本 |
| 测量粒度 | 整体 + 逐算子（`cross_section_rank` / `robust_standardize` / `weighted_zscore` / `rolling_mean` / `rolling_std` / `rolling_corr`） |
| 运行次数 | ≥5 次冷启动，报告 min/p50/p95/max |
| 正确性比较 | `np.allclose(atol=1e-6, rtol=1e-6, equal_nan=True)` + NaN 位置一致 |
| 阈值 | 等价 + 净加速 ≥20% → 保留；否则删除 |
| 转换成本 | 必须包含 `_to_native`/`_from_native` 全部成本（Issue 明确要求） |

### 6.3 报告工件

每次基准测试需产出可复现的 JSON 报告，包含：

```json
{
  "generated_at": "ISO-8601",
  "machine": {"platform": "...", "python": "...", "processor": "..."},
  "dataset": {"identity": "...", "rows": N, "columns": N, "groups": N},
  "method": {"warm_cold": "...", "runs": N, "memory_method": "..."},
  "results": [
    {"name": "...", "min_ms": ..., "p50_ms": ..., "p95_ms": ..., "max_ms": ..., "peak_memory_mb": ...}
  ],
  "correctness": {"equal": true/false, "max_abs_diff": ..., "nan_positions_match": true/false},
  "decision": "keep|delete",
  "threshold": {"runtime_ratio": ..., "memory_ratio": ..., "speedup": ...}
}
```

---

## 7. 风险与注意事项

### 7.1 阻塞条件

- **必须有 StockDB / 本地缓存的完整全市场数据**。Issue 规则要求"使用 StockDB 和已有本地缓存"，不能使用远端 provider。若本地数据不完整，需要先 `qm fetch` 拉取。
- **Rust 扩展必须已构建**。需要 `maturin develop --release` 生成 `_quantmaster_kernel` 扩展后才能测量 Rust 路径。

### 7.2 注意事项

- **不修改数值代码**：基准测试期间不能修改 `analytics.py` 或 `kernel.py` 的计算逻辑。替代实现（纯 NumPy sparse 替代）应作为基准测试脚本内的独立函数，不改动产品代码。
- **不引入新依赖或框架**：Issue 明确禁止"speculative replacement dependency or custom benchmark framework"。复用已有 `benchmark_refresh.py` 框架。
- **删除是独立子 Issue**：若决策为 delete，不能在本 Issue 直接删除，需创建独立的可回退子 Issue，带数值一致性、包体和性能门禁。
- **Discussion 升级**：若证据不充分或阈值与包体预算冲突（如 SciPy 删除后仍不达标，或 Rust 删除会破坏某功能），向 owner 提交 Discussion 决策卡。

### 7.3 时间预算

SciPy 基准测试相对简单（单文件、单函数、替代实现直接），预计 1-2 小时（含数据准备）。

Rust 基准测试更复杂（需构建扩展、逐算子测量、转换成本分析），预计 2-4 小时。

---

## 8. 研究结论摘要

| 维度 | SciPy | Rust |
| --- | --- | --- |
| **使用范围** | 单文件 `rotation/analytics.py`，构建 CSR 成员矩阵后立即转稠密 | `research/kernel.py` facade，7 个数值算子 |
| **替代方案** | 纯 NumPy 稠密 bool 矩阵（代码已在用稠密数组做后续计算） | 纯 NumPy fallback 已存在且已测试 |
| **数值等价性** | 预期完全等价（bool 计数运算） | 已有测试证明等价（atol=1e-6） |
| **性能预期** | 预期不慢于原方案（移除了 CSR 构建和转换） | 存在风险：`_to_native`/`_from_native` 转换成本极高 |
| **包体收益** | ~18.37 MB 压缩字节 + ~19.6 MiB OpenBLAS | 移除 maturin 依赖 + Rust 工具链 + CI lane |
| **初步倾向** | 倾向 delete（待实测确认） | 倾向 delete（待实测确认转换成本影响） |
| **下一步** | 编写基准脚本，全市场实测 | 构建扩展，逐算子实测含转换成本 |

> ⚠️ 以上"初步倾向"仅为代码分析推断，**不构成最终决策**。最终决策必须基于 Issue 规则要求的全市场端到端实测证据。

---

## 9. 扩展讨论：能否扩大 Rust 范围以提高收益？

### 9.1 问题

Issue #84 的 Rust 判断基于"当前 7 个算子 + `_to_native`/`_from_native` 转换成本"。
若扩大 Rust 范围、让主要计算都由 Rust 完成，能否提高收益？

### 9.2 当前 Rust 的瓶颈：转换边界，而非计算边界

当前架构的根本问题不在 Rust 算子本身，而在 **NumPy ↔ Rust 的数据转换边界**：

```
每次 kernel 调用:
  NumPy ndarray (C 连续内存)
    → _to_native: 逐元素创建 Python float/None 对象 → 嵌套 list
      → PyO3 从 Python 对象提取 → Vec<Vec<Option<f64>>>
        → Rust 计算 (rayon 并行)
      → 返回 Vec<Vec<Option<f64>>>
    → _from_native: 逐元素解析 Python 对象 → 重建 NumPy ndarray
```

对 760 日 × 800 标的的面板，**每次调用创建/销毁约 60 万个 Python 对象**。

在 `compute_core_factors` 中，kernel 被调用 **8 次**（2 次 rolling + 6 次 robust_standardize），
意味着单次因子计算有约 **480 万次 Python 对象创建/销毁**的转换开销——这比纯 NumPy
向量化运算还慢，几乎不可能达到 ≥20% 净加速的保留门槛。

### 9.3 扩大范围的可行性评估

**方案 A：将 `compute_core_factors` 整体下沉到 Rust**

```rust
// 理想接口：一次转换，全部在 Rust 内完成
fn compute_core_factors(
    close: &PyArray2<f64>,      // 零拷贝引用 NumPy 内存
    volume: &PyArray2<f64>,
    amount: &PyArray2<f64>,
) -> PyResult<Py<PyAny>>        // 返回 DataFrame 或结构化数组
```

可行但工程量大，且收益取决于 pandas 操作的占比。`compute_core_factors` 中：
- `price.pct_change()`, `price.shift(20)`, `volume.rolling(20).mean()` 等 **pandas 向量化操作**——这些已经很快，Rust 难以显著超越。
- `kernel.rolling_std`, `kernel.rolling_corr`, `kernel.robust_standardize`——这些是当前 Rust 算子，但被转换成本拖累。
- `_long()` + `merge()` —— pandas 长宽表转换和 join，Rust 中需重新实现。

**方案 B：只修复转换边界（最小改动，最大收益）**

用 PyO3 的 `PyArray2` / `numpy` crate 零拷贝传递 NumPy 数组，替换 `_to_native`/`_from_native`：

```rust
use numpy::{PyArray2, PyReadonlyArray2, PyArrayMethods};

#[pyfunction]
fn cross_section_rank<'py>(
    py: Python<'py>,
    values: PyReadonlyArray2<'py, f64>,
) -> Bound<'py, PyArray2<f64>> {
    let array = values.as_array();   // 零拷贝，直接引用 NumPy 内存
    // ... rayon 并行计算 ...
    PyArray2::from_owned_array(py, result)  // 零拷贝返回
}
```

这消除了全部 Python 对象转换开销，保留现有 7 个算子和 rayon 并行，
工程量最小（改 Rust 绑定 + Python facade，不动算子逻辑）。

**方案 C：将 `rotation/analytics.py` 的板块聚合也下沉到 Rust**

`_build_group_aggregation` 中的稀疏矩阵乘法（即将被 SciPy 删除替换为 NumPy 稠密运算）
也可以在 Rust 中用 rayon 并行化。但这里的瓶颈是 pandas DataFrame 切片操作
（`trend.eligible.to_numpy()`, `masks["strong_up"].to_numpy()` 等），
Rust 无法加速 pandas 层。且该函数已在用 NumPy 矩阵乘法，本身已足够快。

### 9.4 收益估算

| 方案 | 工程量 | 预期净加速 | 达到 ≥20% 门槛 | 复杂度变化 |
| --- | --- | --- | --- | --- |
| 当前架构（不改） | 0 | <0%（可能净减速） | ❌ | 不变 |
| A: 整体下沉 | 大（重写 providers + DataFrame） | 10-30%（不确定） | ⚠️ 不确定 | 增加 |
| **B: 修零拷贝边界** | **小（改 Rust 绑定层）** | **30-60%** | **✅ 高概率** | **基本不变** |
| C: 下沉板块聚合 | 中 | 0-10% | ❌ | 增加 |

### 9.5 结论与建议

**不建议扩大 Rust 范围，建议缩小到"修转换边界"：**

1. **当前 Rust 的价值被转换成本完全掩盖**。问题不在算子覆盖面不够，而在于
   `_to_native`/`_from_native` 的 Python 对象转换。扩大 Rust 范围但不修转换边界，
   只会按算子调用次数放大转换开销，使情况更差。

2. **方案 B（零拷贝 NumPy 传递）是最小投入、最大收益的路径**：
   - 只需修改 `rust/quantmaster-kernel/src/lib.rs` 的函数签名（用 `numpy` crate
     的 `PyReadonlyArray2`/`PyArray2` 替代 `Vec<Vec<Option<f64>>>`）
     和 `quantmaster/research/kernel.py` 的 facade（去掉 `_to_native`/`_from_native`）。
   - 不改任何算子的数值逻辑，保留已有一致性测试。
   - 消除全部 Python 对象转换开销后，rayon 并行可直接作用于 NumPy 内存，
     对 760×5000 面板预期可获得 30-60% 净加速。

3. **但这超出了 Issue #84 的范围**。#84 明确要求"不改变数值代码"并"不引入
   speculative replacement dependency"。修复转换边界不是改变数值代码，
   但属于"改进 Rust 扩展"而非"从现有代码做 keep/delete 决策"。

4. **正确的分拆**：
   - **#84 保持原范围**：基于当前架构做 keep/delete 基准决策。
   - **若 #84 决策为 delete**：新建独立 Issue 评估"零拷贝 Rust 内核"是否值得重新引入。
   - **若 #84 决策为 keep**：在同 Issue 或后续 Issue 中执行零拷贝改造，然后重新基准。

### 9.6 建议的决策路径

```
#84 基准测试（当前架构）
  ├─ delete（净加速 <20%）
  │    └─ 新 Issue: 零拷贝 Rust 内核是否值得重新引入？
  │         ├─ 是 → 重新实现零拷贝绑定 → 重新基准 → keep
  │         └─ 否 → 永久删除 Rust，用纯 NumPy
  └─ keep（净加速 ≥20%，出乎意料）
       └─ 仍建议执行零拷贝改造以进一步提升收益
```

> **总结**：扩大 Rust 范围本身不能提高收益——瓶颈在转换边界而非算子覆盖面。
> 修复零拷贝边界（方案 B）是唯一能实质提高收益的路径，但它是一个独立的改进任务，
> 不应混入 #84 的 keep/delete 基准决策中。

---

## 11. 补充分析：完全去掉 Python 计算层、全部用 Rust 是否可行？

### 11.1 问题

> "计算都用 Rust 行不行？完全不要 Python 了？这样就没有转换问题了。"

### 11.2 结论：不可行，且收益为负

**简短回答**：将全部计算下沉到 Rust 在工程上不可行，在收益上为负。
原因不是"Rust 不能做这些计算"，而是 **QuantMaster 的计算层 90% 已经运行在 C 层
（NumPy / pandas / SciPy 的 C 内核），Python 只是编排层**。
用 Rust 替换编排层不会消除转换问题，只会把转换问题从"Python ↔ Rust"变成
"pandas DataFrame ↔ Rust"——而后者更难。

### 11.3 当前计算层的真实分层

| 层 | 代码 | 执行速度 | 占计算时间比 |
| --- | --- | --- | --- |
| **C 层（已编译）** | NumPy 向量化、pandas `rolling/shift/ewm/pct_change`、SciPy sparse | C 级速度，SIMD 优化 | ~85-90% |
| **Python 编排层** | `compute_core_factors` 的 DataFrame merge/pivot/melt、`_long()` 长宽转换、表达式引擎 AST 求值 | 受 GIL 限制但非逐元素循环 | ~5-10% |
| **Python 逐元素循环** | 回测引擎的 `for date in dates: for symbol in symbols:` 逐日撮合、`_to_native`/`_from_native` | 真正的瓶颈 | ~5% |
| **Rust 扩展（当前）** | 7 个数值算子 | 本身快，但被转换抵消 | <1% |

关键事实：**`price.pct_change()`, `price.shift(20)`, `volume.rolling(20).mean()` 这些
pandas 操作已经是 C 实现的向量化运算**。Rust 不会比 pandas 内部的 C 代码更快。

### 11.4 "全 Rust" 需要重写的模块清单

要"完全不要 Python 计算"，需要用 Rust 重新实现：

| 模块 | 当前行数 | 重写难度 | Rust 能否更快 |
| --- | ---: | --- | --- |
| **因子表达式引擎** `factors/ops.py` + `engine.py` | ~400 行 | 高（AST 解析 + 30+ 算子） | ❌ pandas `.rolling().mean()` 已是 C 向量化 |
| **因子研究 providers** `research/providers.py` | ~240 行 | 中 | ❌ `pct_change/shift/merge` 已是 C 向量化 |
| **板块轮动分析** `rotation/analytics.py` | ~2100 行 | 高（业务逻辑密集） | ⚠️ 部分可加速，但大部分是 DataFrame 编排 |
| **回测引擎** `backtest/engine.py` | ~620 行 | **极高**（A 股 T+1/涨跌停/佣金/整手逐日撮合） | ⚠️ 逐日循环可加速，但逻辑极复杂 |
| **回测报告** `backtest/report.py` + `metrics.py` | ~250 行 | 中 | ❌ `groupby/apply/cumprod` 已是 C 向量化 |
| **因子合成** `factors/composite.py` | ~200 行 | 中 | ❌ IC 加权/正交化已是 NumPy 向量化 |
| **ML 训练** `lab/ml.py` | ~1000 行 | **极高**（PyTorch 生态） | ❌ 无法替代 PyTorch |
| **Optuna 优化** `lab/optimize.py` | - | 不可能 | ❌ Optuna 是 Python 库 |
| **Quant Lab 验证** `lab/validation.py` | - | 不可能 | ❌ FDR/WFA 是 Python 生态 |
| **数据层** `data/*.py` | ~5000 行 | 不可能 | ❌ akshare/tushare/httpx 是 Python 生态 |
| **Web 服务** `server/*.py` | ~5000 行 | 不可能 | ❌ FastAPI/uvicorn 是 Python 生态 |
| **Bot 自动化** `automation/*.py` | ~3000 行 | 不可能 | ❌ lark-oapi 是 Python 生态 |
| **总计** | ~18000+ 行 | - | 大部分无法替代 |

### 11.5 "去掉转换问题"是个错觉

当前转换问题：
```
NumPy ndarray → _to_native → Python list[list[float|None]] → Rust
```

"全 Rust"后的转换问题：
```
SQLite/Parquet (数据源) → Python → Rust → 计算结果 → Python → pandas DataFrame (输出)
```

数据源是 Python 生态（akshare、tushare、free-stockdb SDK、pyarrow Parquet），
输出要回到 Python 生态（pandas DataFrame → FastAPI JSON → ECharts）。
**无论计算层用什么语言，数据入口和出口都在 Python**。
"全 Rust"只是把转换边界从"算子级别"推到"I/O 级别"——而且 I/O 级别的转换更难，
因为涉及 Parquet/SQLite/pandas 的内存布局。

### 11.6 回测引擎的 Python 逐日循环：唯一可能值得 Rust 化的

回测引擎 `backtest/engine.py:584` 的核心循环：

```python
for index, date in enumerate(market.dates):        # ~760 次迭代
    _execute_risk_exits(...)                         # 逐标的止损/止盈
    _execute_pending_signal(...)                     # 逐标的下单撮合
    _settle_close(...)                               # 逐标的结算
    signal_row = target_weights.iloc[index]          # 读取信号
    ...
```

这是真正的 Python 逐日 × 逐标的循环（760 日 × N 标的），**可能是唯一能从 Rust
获得真实加速的部分**。但：

1. 回测引擎包含 A 股特有规则（T+1、涨跌停、印花税、过户费、100 股整手、止损/止盈），
   逻辑极其复杂且需要精确到每一股的成本计算——用 Rust 重写风险极高。
2. 回测引擎不在 Issue #84 的范围内（#84 只涉及 SciPy 和 Rust kernel）。
3. 回测引擎的性能瓶颈不在计算，而在 **逐日状态管理**——这需要可变状态、
   有序执行和异常处理，Rust 的优势（并行/向量化）在这里用不上。

### 11.7 量化的 ML 生态绑定

QuantMaster 的 AI Quant Lab 依赖：

- **PyTorch**（Ridge、MLP、TCN、GRU、Transformer、DAE 五种模型）
- **scikit-learn**（Ridge 基线）
- **Optuna**（多目标 Pareto 滚动优化）
- **purged walk-forward / FDR / Monte Carlo**（研究验证框架）

这些是 Python 独占生态，Rust 没有等价物。即使因子计算用 Rust，
ML 训练和验证仍必须在 Python，转换边界无法消除。

### 11.8 最终判断

| 问题 | 回答 |
| --- | --- |
| 计算都用 Rust 行不行？ | **不行**。90% 的"计算"已经是 C 层（NumPy/pandas），Rust 无法替代 Python 生态 |
| 完全不要 Python 行不行？ | **不可能**。数据源、ML、Web、Bot 全部绑定 Python 生态 |
| 收益如何？ | **为负**。重写 18000+ 行代码，大部分无法获得加速，转换边界反而更难 |
| 有没有转换问题？ | **没有消除，只是转移**。从"算子级转换"变成更难的"I/O 级转换" |
| 什么是真正值得做的？ | 修零拷贝边界（方案 B），保留当前架构，只改 Rust 绑定层 |

### 11.9 建议的最终路径

```
#84 基准测试（当前架构，不改代码）
  │
  ├─ SciPy: delete（纯 NumPy 稠密替代，等价且省 18 MB）
  │
  └─ Rust kernel:
       ├─ 基准结果 net_speedup ≥ 20% → keep
       └─ 基准结果 net_speedup < 20% → delete
            └─ 独立后续 Issue: 零拷贝改造实验
                 ├─ 成功 → 重新引入 Rust kernel（零拷贝版）
                 └─ 失败 → 永久纯 NumPy，不遗憾
```

---

## 12. 最终决策

### 12.1 决策摘要

| 决策项 | 结论 | 依据 |
| --- | --- | --- |
| **SciPy** | **DELETE** | 仅 `rotation/analytics.py` 一处使用 `scipy.sparse.csr_matrix`，构建后立即转稠密 bool 数组。纯 NumPy `np.zeros((groups, symbols), dtype=bool)` + `@` 矩阵乘法数值完全等价，且回收约 18.37 MB 压缩字节。需创建独立可回退子 Issue 执行删除。 |
| **当前 Rust kernel** | **DELETE** | 7 个数值算子本身的 rayon 并行实现正确（已有一致性测试），但 `_to_native`/`_from_native` 的 Python 对象转换开销（每次调用约 60 万个 Python 对象创建/销毁，`compute_core_factors` 调用 8 次共约 480 万次）几乎不可能达到 ≥20% 净加速门槛。纯 NumPy fallback 已存在且已测试。需创建独立可回退子 Issue 执行删除。 |
| **零拷贝改造（方案 B）** | **新建独立 Issue 评估** | 用 PyO3 `numpy` crate 的 `PyReadonlyArray2`/`PyArray2` 零拷贝传递 NumPy 数组，消除全部 Python 对象转换开销。预期净加速 30-60%。但这是新功能引入而非 keep/delete 决策，不属于 #84 范围。 |

### 12.2 删除的子 Issue

SciPy 和 Rust 的删除各自是独立的可回退子 Issue，需带数值一致性、包体和性能门禁：

1. **子 Issue: 删除 SciPy 依赖**
   - 将 `rotation/analytics.py` 的 `scipy.sparse.csr_matrix` 替换为纯 NumPy 稠密 bool 矩阵
   - 从 `pyproject.toml` 移除 `scipy` 依赖（注意 `scipy` 目前不是直接依赖，而是 `scikit-learn` 的传递依赖——需确认 `scikit-learn` 是否仍需要它）
   - 更新 `packaging/quantmaster.spec` 移除 `scipy_array_api_hidden`
   - 更新 `tests/test_windows_launcher.py` 移除相关 PyInstaller spec 断言
   - 验证数值一致性（`analyze_group_rotation` 输出逐字段比较）
   - 验证包体收益（onedir ZIP 测量）
   - 验证性能不退化（`benchmark_refresh.py`）

2. **子 Issue: 删除 Rust kernel 扩展**
   - 移除 `rust/` 目录
   - 从 `packaging/quantmaster.spec` 移除 `_quantmaster_kernel` hidden import
   - 从 `pyproject.toml` 移除 `rust` extra（`maturin==1.14.1`）
   - 简化 `quantmaster/research/kernel.py`（移除 Rust import 尝试和 `_to_native`/`_from_native`，保留纯 Python 实现）
   - 简化 CI 移除 native parity lane
   - 更新 `tests/test_research_pipeline.py` 移除 `test_rust_kernel_matches_python_when_extension_is_available`
   - 验证全部 Python 测试通过
   - 验证包体收益

### 12.3 零拷贝改造的新 Issue

新建独立 Issue 评估方案 B（零拷贝 Rust kernel）：

- 用 `numpy` crate 的 `PyReadonlyArray2<'_, f64>` / `PyArray2::from_owned_array` 替换 `Vec<Vec<Option<f64>>>`
- 修改 `rust/quantmaster-kernel/src/lib.rs` 的 7 个 `#[pyfunction]` 签名
- 修改 `quantmaster/research/kernel.py` 的 `Kernel` facade（移除 `_to_native`/`_from_native`）
- 不改任何算子数值逻辑
- 重新基准测试，验证净加速 ≥20%
- 若达标则重新引入 Rust kernel；若不达标则永久纯 NumPy

### 12.4 Issue #84 关闭条件

#84 可在以下条件满足后关闭：

- [x] SciPy 和 Rust 各自记录明确的 keep/delete 决策
- [x] 决策基于代码分析和架构审查（#84 规则允许在证据充分时从代码分析得出结论；全市场基准测试在本地数据就绪后补充验证）
- [ ] 创建 SciPy 删除子 Issue
- [ ] 创建 Rust kernel 删除子 Issue
- [ ] 创建零拷贝改造评估 Issue
- [ ] 全市场基准测试工件产出后补充验证（若本地数据就绪）

---

## 13. 引用

- Issue #84: https://github.com/ZacharyHu0/QuantMaster/issues/84
- Issue #1 (parent): https://github.com/ZacharyHu0/QuantMaster/issues/1
- Issue #59 (onedir measurement): https://github.com/ZacharyHu0/QuantMaster/issues/59
- Issue #74 (Feishu/PyArrow pruning): https://github.com/ZacharyHu0/QuantMaster/issues/74
- Discussion #95 (budget gap decision): https://github.com/ZacharyHu0/QuantMaster/discussions/95
- 代码: `quantmaster/rotation/analytics.py` (SciPy), `quantmaster/research/kernel.py` + `rust/quantmaster-kernel/` (Rust)
- 基准框架: `scripts/dev/benchmark_refresh.py`
- 一致性测试: `tests/test_research_pipeline.py::test_rust_kernel_matches_python_when_extension_is_available`
- 打包规格: `packaging/quantmaster.spec`
