# M4 Lease、注册监听与 Watch 阶段验收记录

**记录日期：** 2026-11-20

**验收范围：** 公开实现、协议互通与本地 HBase MiniCluster 验证

**阶段评审：** 待维护者评审；本报告 PR 请求 `Reidddddd` 评审

## 1. 结论与边界

J40–J47 的实现与公开验证已完成，八个实现 PR 均已合并到上游 `main`、正式 review 为 `APPROVED`，GitHub 状态检查共 294/294 成功。J29.4 的公开混合负载报告也已按当前源码清单和报告约束核验。

**M4 的八项实现任务及公开验证材料已齐备；正式阶段验收仍待评审。** 当前主计划的 M2/M3 阶段记录仍未关闭，而 M4 进入条件要求 M2 内核已验收。本报告不推定前置阶段通过；维护者需在评审本记录 PR 时一并核对这些前置门。M4 阶段 checklist 暂不勾选。目标集群容量、生产启用或正式发布验收也不在本报告建议范围内。

J29.4 的 `passed` 表示该公开 MiniCluster 场景和报告结构完成；它不表示并发负载全部请求成功或容量达标。并发 2/4 已观测到大量 `RESOURCE_EXHAUSTED`，报告将 `target_capacity_accepted` 标为 `false`。因此保留容量结论为未通过/未接受，不能从成功 RPC 的 p99 推导目标 SLO。

## 2. M4 实现与正式评审

| 任务 | PR | 合并状态 | 正式 review | GitHub 状态检查 |
|---|---|---|---|---:|
| J40 Lease 持久状态与管理查询 | [#47](https://github.com/Reidddddd/etcd-hbase/pull/47) | 已合并 | `Reidddddd` / APPROVED | 21/21 成功 |
| J41 Lease key 绑定与原子注册 | [#48](https://github.com/Reidddddd/etcd-hbase/pull/48) | 已合并 | `Reidddddd` / APPROVED | 36/36 成功 |
| J42 持久化 KeepAlive 续约 | [#49](https://github.com/Reidddddd/etcd-hbase/pull/49) | 已合并 | `Reidddddd` / APPROVED | 37/37 成功 |
| J43 Lease 到期、撤销与接管 | [#50](https://github.com/Reidddddd/etcd-hbase/pull/50) | 已合并 | `Reidddddd` / APPROVED | 38/38 成功 |
| J44 已提交事件索引与共享 tailer | [#51](https://github.com/Reidddddd/etcd-hbase/pull/51) | 已合并 | `Reidddddd` / APPROVED | 39/39 成功 |
| J45 Watch 双向流与订阅生命周期 | [#52](https://github.com/Reidddddd/etcd-hbase/pull/52) | 已合并 | `Reidddddd` / APPROVED | 40/40 成功 |
| J46 Watch 历史回放、progress、fragment 与 Go client 互通 | [#53](https://github.com/Reidddddd/etcd-hbase/pull/53) | 已合并 | `Reidddddd` / APPROVED | 41/41 成功 |
| J47 Watch 流控、取消与资源排空 | [#54](https://github.com/Reidddddd/etcd-hbase/pull/54) | 已合并 | `Reidddddd` / APPROVED | 42/42 成功 |

以上状态于 2026-11-20 从上游 GitHub 核对；八个 PR 的 base 均为 `main`。状态检查数量按各 PR 当前可见的检查记录统计。J45–J47 覆盖历史回放与实时交接、progress/fragment、慢消费、取消、Compact 边界及 Go v3.5.23 client 互通；具体用例和范围见各 PR 与 [M4 设计](../specs/2026-11-11-m4-lease-watch-design.md)。

## 3. J29.4 公开混合负载证据

报告标识为 `j29.4-closeout-r4-20261120`，scope 为 `public-local-minicluster`，源代码提交为 `1de13e973c84ad917d1e344f0eeefd02a92ad374`，报告源码清单聚合 SHA-256 为 `129221da9237993e20e1a92da98f9a2ba66d4a7856f191bd808a90678ad564c3`。仓库的 J29.4 evidence validator 已核对报告状态、HISTORY/NATIVE 两种布局、并发度 1/2/4、双运行时 mTLS owner 接管以及源码文件清单哈希；校验通过。原始机器报告留在本地忽略目录，未上传；下表是公开验收摘要。

每格 p99 只统计成功的客户端 RPC，括号为样本数；被拒绝的请求不进入延迟样本。负载迭代数较小且没有目标 SLO，以下 p99 只描述这次公开运行。

| 布局 | 并发 | KeepAlive p99（样本） | Put p99（样本） | KeepAlive `RESOURCE_EXHAUSTED` | Put `RESOURCE_EXHAUSTED` |
|---|---:|---:|---:|---:|---:|
| HISTORY | 1 | 120.701 ms（24） | 83.406 ms（24） | 0/24 | 0/24 |
| HISTORY | 2 | 63.598 ms（29） | 49.647 ms（31） | 19/48 | 17/48 |
| HISTORY | 4 | 63.283 ms（346） | 49.251 ms（370） | 432/778 | 408/778 |
| NATIVE | 1 | 37.987 ms（24） | 26.175 ms（24） | 0/24 | 0/24 |
| NATIVE | 2 | 46.103 ms（38） | 36.115 ms（39） | 10/48 | 9/48 |
| NATIVE | 4 | 47.566 ms（534） | 37.023 ms（545） | 529/1063 | 518/1063 |

| 布局 | 并发 | 报告口径的逻辑 HBase 操作 / KeepAlive 请求 | 逻辑请求负载字节 / KeepAlive 请求 | 到期至撤销观测延迟 | 陈旧候选 |
|---|---:|---:|---:|---:|---:|
| HISTORY | 1 | 79.375 | 36,421.25 B | — | — |
| HISTORY | 2 | 47.917 | 21,948.06 B | — | — |
| HISTORY | 4 | 35.509 | 16,342.08 B | 9.769 s | 1/2 |
| NATIVE | 1 | 79.000 | 35,854.17 B | — | — |
| NATIVE | 2 | 62.542 | 28,390.63 B | — | — |
| NATIVE | 4 | 40.024 | 18,348.67 B | 13.272 s | 1/2 |

上述 HBase 操作数和请求负载字节是应用侧逻辑计数，不是网络字节、物理 Cell 字节或 RegionServer RPC 计数。报告中的服务端 `unknown_outcomes` 计数按 HISTORY 并发 1/2/4 为 0/19/428，按 NATIVE 为 0/10/524；当前报告未按拒绝原因进一步分类，因此不将该计数等同于 `RESOURCE_EXHAUSTED`，也不据此推断数据丢失。后续容量验收应补充结果未知原因分类和完成状态核对。

双运行时 mTLS owner 接管用例观测到 owner epoch 从 7 增至 8，耗时 5.176 s。HISTORY/NATIVE 并发 4 的到期撤销都发生在混合负载中，扫描分别看到 2 条候选，其中 1 条为陈旧候选。

## 4. 未关闭风险与后续门槛

- 并发 2/4 的高比例请求被 `RESOURCE_EXHAUSTED` 拒绝；该运行没有接受目标容量。需要结合冻结后的 B04/B05 工作负载、TTL 与容量门槛重新测量，必要时调整准入或调度，再做目标验收。
- 到期至撤销的 9.769 s 和 13.272 s 来自本地 MiniCluster。主计划中的 15 s 是当前参考值，不是已批准的目标 SLO；这次测量不能宣称满足目标环境时限。
- HBase 物理 RPC 网络字节未采集；恢复 reserve 记录的是每表组配置值，运行时实际峰值未测。容量结论保持未接受。
- MiniCluster 与公开 CI 不覆盖实际目标集群的认证授权、凭据轮换、网络、时钟、HDFS/WAL 故障和长期运行。生产 Lease 仍按设计 fail-closed，直到 J51 身份生命周期等生产门槛完成；M6 消费者验收继续归 M6。
- 本报告不关闭 M0、M2、M3 的阶段门，不改变 J36 缓存状态，也不替代 M5 的身份、Compact/GC、运维与生命周期任务。

## 5. 阶段记录

| 字段 | 记录 |
|---|---|
| 阶段 | M4：Lease、注册监听与 Watch |
| 主任务 | J40–J47，8/8 已完成；实现 PR 均合并并获正式批准 |
| 跨阶段交付 | J29.4 公开 MiniCluster 证据已生成并完成报告校验；对应 M2 主任务是否关闭仍按 J29.1–J29.3 的评审状态处理 |
| 阶段退出结果 | J40–J47 开发与公开功能验证材料齐备；正式阶段结论待评审并核对 M2/M3 前置阶段门；容量和目标环境不在通过范围 |
| 设计、实现、验证依据 | [M4 设计](../specs/2026-11-11-m4-lease-watch-design.md)、[J29 公开成本报告](j29-public-cost-report.md)、PR #47–#54 |
| 压力/故障/并发证据 | J29.4 两种布局与并发 1/2/4；J47 MiniCluster/Go client CI；高并发拒绝和未测量项见第 3、4 节 |
| 未关闭风险 | 目标容量、物理 HBase RPC 字节、reserve 实际峰值、目标身份与生产故障环境；开发阶段可进入后续开发，生产启用仍受限 |
| 评审人及结论 | `Reidddddd`；本验收记录 PR 待评审 |
| 实际验收日期 | 待维护者评审后填写 |
| 下一阶段准入 | M2/M3 前置阶段状态需按主计划核对；不据此跳过 M5/M6 各自的准入、身份、GC 与消费者验收条件 |
