# J29.1–J29.3 公开成本报告

## 1. 范围与结论边界

本报告使用本地 HBase MiniCluster 和固定合成请求，覆盖 `HISTORY + CURRENT` 与 `NATIVE` 两种候选布局。它用于观察 M2 协议成本与单活队列行为，不代表目标集群的容量、吞吐或 p99 验收。B04/B05 尚未提供，目标验收门槛仍未冻结。

J29.1 通过现有 invocation、manifest、control 行 CAS、发布解析、decision archive、materialization 与最终 control CAS 执行真实提交。J29.2 将选中事务部分落地后交给真实 `RecoveryManager` 接管并收尾；通过向其 owner claim 传入原租约截止时间加 1 ms 模拟租约已过期，不等待真实的一小时。读取在 pin 固定该未完成视图后启动。测试专用 interlock 按读页完成顺序逐项放行恢复物化，确定性地产生读/恢复交错，不等待恢复结果再读。J29.3 使用 KV 与 metadata-only 模拟续租组成有界公平队列，metadata-only 提交只模拟续租类写入，不代表 etcd Lease TTL、到期删除或 Watch 语义。

## 2. 固定负载

- Seed：`20261104`。
- 候选基线：单 key 64 B、16 key × 1024 B、大事务；metadata-only 模拟续租不改变 KV revision。
- 读负载：8 个候选，均匀分布到 4 个 salt bucket；先部分物化，再由新 owner 恢复剩余版本，同时按每页 1 个 key 读取。HBase 页耗尽需要终止探测，因此共 12 次页请求、8 次 scanner 重开；测试 interlock 使其中 8 页与恢复物化交错，只调度恢复物化，不暂停读请求；此确定性时序不代替自然竞争或目标集群延迟。
- 混合队列：4 个单 key KV、2 个 8 key 大事务、2 个 metadata-only 模拟续租；单活执行，最多连续处理 3 个业务事务后运行一个待处理续租。
- HISTORY 和 NATIVE 复用相同 workload digest、JDK、MiniCluster 与 bucket 配置；协议内部操作和下述选定负载字节纳入观测，完整内部行成本未被计量。

## 3. 指标口径

`j29-commit-baseline` 区分 invocation bind、准备、publish-and-finalize、finalization 与总耗时。finalization 是 publish-and-finalize 的子区间，二者不能相加。control CAS 与 system-record 操作记录应用层调用、冲突/未知结果、选定负载字节和调用耗时。`inputBytes` 在调用开始时计入，`outputBytes` 只计成功结果中的选定字段：

- `system.*.read` 输入计 0，输出计返回 value 长度；`create` 输入计 value 长度、输出计 0；`cas` 输入计存在的 expected value 与 replacement 长度之和、输出计 0。它们不单独计入 system-record identity 或 type。
- `control.read` 输入计 0、输出计返回的 comparison token 长度；`initialize` 输入计编码后的初始状态长度；`cas` 输入计旧 comparison token 与编码后的替换状态长度。后两者输出均计 0。
- `materialize.preflight` 和 `materialize.apply` 分别计候选记录的逻辑 key 与可选 value 长度，输出计 0；相同候选可在两个操作中分别计数，不代表去重后的数据量。

这些值不包含未单独计数的 identity、物理行键、family/qualifier、Cell 元数据及其他请求字段，也不能当作完整请求或内部行大小。`j29-read` 的物理行、返回 Cell 字节和 scan RPC 数来自存储页计数，是不同口径；同时报告读页重开次数、恢复物化调用数、恢复进行中完成的页数及恢复总时长。队列等待从单线程执行器提交到任务开始计时，服务时间单独记录。

以下数据未由当前 MiniCluster instrumentation 可靠提供，保持为缺口：点请求的 HBase RPC/retry 数、服务端 scan counters、WAL/HFile IO 和 compaction 成本、JVM/direct-memory 高水位、恢复 reserve 的真实账本峰值、多进程故障接管成本。选定应用负载字节不能当作网络字节或 HBase Cell 字节。

## 4. 原始分支观测（历史）

本节数据来自原始提交 `643cc287b3f5a0f7c76ee24893e556a73981bb9e`，其 J28 父提交为 `4e92e763d9e1e2e2ffc33ac0905e8c841a02b7a0`。它不代表合并后最新基线的测量结果。运行环境为 HBase `2.6.6-hadoop3`、Temurin JDK `21.0.12.1+1-LTS`、单 RegionServer 和 4 个 salt bucket。固定 seed 为 `20261104`，workload SHA-256 为 `99fd421f6ecc306ec263fc2a53ee0b08523d8793b239c0d945a26a5ec50402fd`。`scripts/verify/j29.sh` 的 MiniCluster 用例通过 1 次，失败、错误和跳过均为 0；同次机器报告保存在本地忽略目录 `.artifacts/m2/j29/`。

下表为每类单次观测耗时，单位为微秒；没有预热和重复运行，因此这些数值只描述本次本地执行。finalization 是 publish-and-finalize 总区间的子区间，不能相加。8 个请求的队列 p99 是小样本最大值，不作为稳定尾延迟估计。

**表 1 原始分支两种布局的单次本地观测**

| 布局 | 1-key / 16-key / 模拟续租提交总耗时 | 读屏障+pin / 候选点读 / 12 页读取（8 次重开） | 队列等待 p50 / p95 / p99 | control CAS 未知/失败 |
| --- | --- | --- | --- | --- |
| HISTORY + CURRENT | 94,726 / 218,119 / 21,098 | 17,121 / 10,019 / 75,873 | 103,337 / 368,466 / 368,466 | 0 / 0 |
| NATIVE | 30,659 / 107,649 / 15,524 | 4,690 / 3,181 / 40,590 | 65,303 / 205,703 / 205,703 | 0 / 0 |

两种布局均执行 12 次页请求、8 次重开和 8 次恢复物化；恢复进行中分别完成 10 页（HISTORY）和 9 页（NATIVE），恢复总时长分别为 135,776 µs 和 65,642 µs，包含测试 interlock 的逐页让步时间。

本次请求边界采样的 heap 峰值为 HISTORY 89,176,024 B、NATIVE 58,268,784 B；direct buffer 分别为 12,592,074 B 和 12,592,070 B；线程数为 24 和 25。两个布局的候选逻辑负载峰值均为 16,710 B；扫描器峰值均为 1，pin 释放后活跃数均为 0（容量 32）。这些是稀疏请求边界采样值，不是连续 JVM 高水位。

所有记录的 control CAS 均无冲突、未知结果或失败。每种布局均完成 26 次 control CAS、46 次 materialize apply；系统记录中的候选、manifest、decision、snapshot pin 操作和上述选定负载字节数见同目录 `summary.json`。本次仅运行一次且 HISTORY 先于 NATIVE 执行，结果不用于宣称某种物理布局更快。

## 5. 最新基线集成验证

基于 PR #32 的合并提交 `db8e12fba2deb1fca351193f293678c6e616632a`，在对应源码提交 `680f0cfcf2c6d545f90356ee7f49e380f4be6ec2` 的工作树执行 `scripts/verify/j29.sh`，提交后核对测试源码哈希一致。锁定 Temurin JDK 21 和单 RegionServer MiniCluster 下，SPI 13 项、core 198 项、HBase 单测 129 项及包含两种布局的 J29 集成用例 1 项通过，失败、错误和跳过均为 0。两种布局均记录 12 次页请求、8 次重开、8 次恢复物化，pin 释放后活跃数为 0；原始机器报告留在本地忽略目录。

新基线的夹具在独占的新系统表上、服务启动前显式初始化 pin 注册表，点读和扫描共用请求作用域内的有界候选预算。初始化发生在计数包装内，因此本次系统记录总量包含注册表初始化操作；读屏障和 pin 打开耗时不包含这一步。此次仍是单次合成负载，不用于比较布局性能或推断目标环境容量、p99 和恢复目标。

以上记录对应所列源码版本；后续结构修订的版本和验证以 PR 当前回复为准。

## 6. 后续验收

J29.4 的公开 Lease/业务提交/到期混合负载补充见 [M4 阶段验收记录](m4-stage-acceptance-report.md)。该报告基于本地 MiniCluster，记录 HISTORY/NATIVE 和并发 1/2/4 的观测；并发 2/4 出现资源拒绝，容量未接受。目标容量、p99 与恢复结论仍需冻结 B04/B05 并在获批目标环境独立测量；本报告和 M4 补充都不替代这些证据。
