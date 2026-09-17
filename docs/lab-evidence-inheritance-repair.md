# Lab 证据目录继承修复（Refs #517，子任务 #551）

## 根因与证据边界

已确认本次 `prepare_data` 的 dataset 收尾失败来自既有 `lab_evidence`
关闭 Windows ACL 继承，严格目录检查因此拒绝创建暂存快照。行情准备没有待补
分区，四份行情文件字节及远端指标未变化；这不是行情下载失败，也不是 #545 的缓存竞态。

operator 的只读证据：父数据目录继承正常；证据根由当前身份拥有，具有规范的
三条显式 Allow FullControl（OWNER RIGHTS、SYSTEM、Administrators），保护开启。
根下有两个旧快照、16 个文件、335880224 字节；18 个后代均未保护，无重解析点。
这些是不可丢弃的实验依据。

源码唯一首建路径为 `dataset._freeze_dataset_evidence` 的普通 `mkdir`；暂存目录
已经调用 `create_inheriting_temporary_directory`，使用 `mkdir(0o777)` 并验证继承。
私有三 ACE 形态可由 Windows Python `mkdir(0o700)` 产生，私有 tempfile
提取的子树经同卷 rename 后也可能丢失父级 cleanup 授权。但生产目录缺少首次创建
审计，**首次创建者未知**；不能反推历史工具，也不能把没有 cause 日志的历史失败算作同一原因。

## 修改与维护边界

`quantmaster.runtime.lab_evidence_repair` 提供仅此目录的 `preview / apply / restore`。
它不复制、删除、覆盖或移动任何证据，不改 owner、父数据目录、task 根或任何后代的
显式 ACL。唯一的直接 ACL 写入是证据根：解除保护并保留显式 ACE；后代接受 Windows
正常继承传播。具有受保护后代、不明私有根规则、非规范 ACL、拒绝 ACE、重解析点、
路径或身份变化的情况全部拒绝，不尝试广域恢复或权限提升。

维护必须持有原有 runtime-worker `maintenance.enter` 产生的冻结租约。
该机制调用现有 worker plan 排空流程，包括 Lab worker，并核对各任务 idle。
入口通过绑定数据根与完整应用身份的 IPC `maintenance.status` 验证 token、冻结状态、
worker 身份和参与者。仅声明“已经停写”无效；此外 Windows 共享模式句柄在整个操作内
排除现存文件写入者，阻止新的写入及被锁对象的 rename/delete。工具不停止进程，
不自行进入或退出生产维护，也不加载默认生产配置。

## operator 执行流程

1. 唯一生产 operator 使用既有维护入口取得冻结租约，记录当前应用 SHA、slot、generation、
   worker 身份、token。不要把 token、真实路径、用户名、SDDL 或原始异常公开。
2. 在自己的受管运行环境使用主 checkout 的项目解释器，从已验证修复 worktree 运行模块。
   显式传入当前数据根、逐字确认的证据根、自己的持久非 disposable receipt 路径及应用身份。
   receipt 的父目录必须已存在；工具不创建父目录。不要将 receipt 放入 pytest/cache/runtime
   等将被生命周期清理的可丢弃目录。
3. 先执行 `preview`，将具体对象清单、文件哈希、总字节数及 inventory digest 交主协调复核。
   它只读取，且不会创建 receipt。预期为 19 个对象（包含证据根）、16 个文件、上述字节总数，
   直接 ACL 写入范围只有根。范围不同则停下说明，不能自动扩大。
4. 复核后用相同参数执行 `apply`，额外传入 `--expected-inventory <approved-preview-digest>`。
   摘要不匹配即拒绝，不能拿新清单替代已经复核的清单。开始前再次核对身份/内容及实时冻结租约；首次以独占创建
   并 fsync 的方式保存原 DACL、owner、对象身份、逐文件 SHA-256 和停写证据，再进行 ACL 写入。
   成功必须返回 `apply_complete` 并保存后验 ACL。该 receipt 为私有恢复材料，只保存在受管
   非 disposable 交付目录，任务收尾由生命周期工具保留。
5. 复核内容/身份未变、根及后代继承正常、父目录完全未改，原显式规则不变，父级可传播的
   授权在每个后代都存在。随后由 operator 通过已有机制释放租约，再执行正常产品验收。

以下参数值均是占位说明，不应原样运行；实际绝对路径与租约只在本机填入：

```text
<primary-interpreter> -m quantmaster.runtime.lab_evidence_repair preview
  --data-root <confirmed-data-root> --confirm <confirmed-lab-evidence>
  --receipt <private-persistent-receipt.jsonl>
  --build-sha <installed-full-sha> --slot-id <installed-slot>
  --generation <installed-generation> --maintenance-token <frozen-lease-token>
```

执行或恢复时只将 `preview` 分别替换为 `apply`、`restore`。任何非零返回都不能算成功；
保留 receipt 与维护窗口，检查是否已写入 ACL。禁止手工删除旧目录或执行管理员 ACL 覆盖。

## 中断、恢复与隔离证明

receipt 的首行在 ACL 修改之前持久化。即使写 ACL 后进程中断，仍可在重新验证冻结租约、
父 ACL、完整对象身份和文件哈希后执行 `restore`。它将原根 DACL 应用回原对象，由 Windows
正常传播，验证所有对象的 owner、保护状态和完整授权集合恢复；不会修改证据内容。
恢复后原来的继承阻断也会回来，不能把恢复当作正向修复成功。

Windows 会加入自动继承标志并调整等价 Allow ACE 顺序，因此恢复证明比较精确授权语义，
不宣称 SDDL 字节完全一致；原始与恢复后的 SDDL 均留在 receipt。任何文件变化、对象替换或
父 ACL 变化均阻断恢复，避免覆盖恢复操作之后的新证据或策略。恢复无需 receipt 的完成行。

隔离测试使用受管任务目录、真实 Windows DACL 和既有未来 cleanup SID 探针，不新建系统账号。
覆盖旧 `mkdir(0o700)` 根、真实文件保全、冻结租约与写句柄拒绝、恢复、中断后的恢复、
受保护后代、重解析逃逸，以及正常新建/解压/同卷 rename 的未来 cleanup 授权。
正常首建继续沿用现有继承 helper；本次不增加自动 ACL 自愈或重复维护框架。

本机 Windows / 项目 Python 3.12.13 已实际跑通私有根修复、授权恢复、写者拒绝、
未来 cleanup SID 传播和中断恢复。`tasks.py check` 使用 impact map 运行运行时与
架构关联检查；复杂度检查不增加既有 C901 计数，新增模块 mypy 检查通过。
这些是隔离环境证明，不是生产处理完成或生产速度基准。

## 安装验收要求

隔离测试通过及合并不等于生产验收通过。生产维护只能由指定 operator 执行，先 preview
交协调复核。维护后正常 `prepare_data` 必须完成 dataset 与 snapshot；旧两个快照和全部
行情文件仍可核验，零缺口请求不能增加行情远端调用；新的快照目录与文件保持继承。
记录真实耗时、最终任务状态与新旧快照可读性，再恢复原维护/自动开关状态。
本子任务不关闭 #517，不修改版本或 release。
