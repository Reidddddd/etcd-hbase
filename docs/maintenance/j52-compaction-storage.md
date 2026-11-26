# J52 逻辑压缩与历史回收的当前边界

## 1. 交付范围

本次提供内部逻辑 Compact 选择器、KV 历史回收许可和 HBase 精确版本删除。`CompactionService` 复用现有控制行与 system 记录，`GcCoordinator` 复用持久 snapshot pin 注册表。`KV.Compact` 已通过 `CompactGateway` 装配到既有 owner lane，与发布和恢复串行执行；J52 保持进行中。

`physical=false` 返回本次逻辑选择结果。当前运行时尚未装配历史枚举与持久检查点，`physical=true` 在逻辑选择成功后返回 `UNAVAILABLE`；该错误不撤销已持久化的逻辑压缩边界，也不表示物理回收完成。请求取消或超时后不返回成功响应。

新的操作先以 `COMPACTION` 类别持久保存请求身份、授权范围、目标 revision 和完整控制状态摘要，再用控制行 CAS 选择逻辑边界。控制行的 `compact_operation_id` 使丢失 ACK 的操作可以按原身份恢复；替换该身份前，必须先持久确认前一操作的逻辑结论。逻辑压缩不增加 KV revision 或 sequence。控制状态未启用授权发布屏障时拒绝选择；控制行含 Compact 身份但 descriptor 未声明能力时拒绝读取。

初始 `Compact(0)` 与已压缩到零不同。固定 etcd v3.5.23 的原生对照覆盖初始零、负数、重复、未来 revision，以及 Compact 后 Range/Watch 的 C−1、C、C+1 边界。重复与未来 revision 在网关中投影为 `OUT_OF_RANGE`。Go 测试目前只提供原生行为依据，不代表 Java Compact RPC 已互通。

## 2. KV 历史回收约束

回收许可绑定 profile、pin 注册表、逻辑操作及本轮 scope。所有分片的 revision/sequence 注册 floor 确认后，活动 pin 继续分别限制两个边界。回收器重新核对 owner 和控制身份；HBase 适配器再次核对本表组实际 pin 注册表。许可只允许处理冗余 KV 物化版本。

HISTORY 每个 key 保留 safe revision 处的最后基线、墓碑和更高版本。旧行用完整 guard 条件，在同一 `RowMutations` 中写入 RETIRED guard 并精确删除 `m/v`，随后强一致确认。退休证明继续阻止迟到 materializer 重插。

NATIVE 同样保留基线和更高版本。删除明确 timestamp 的 `m/v/f`，并用最新 fence 的完整内容 CAS 安装维护代次。并发新写先获胜时重新读取并计算计划；ACK 丢失先强读，不给旧计划换条件重发。取消结束调用结果，在途 HBase producer 仍须排空；本页未确认时调用方不能推进游标。

每次 HISTORY 页最多处理 64 个版本，NATIVE 页最多删除 64 个版本。当前入口处理显式枚举的单个 key；持久 bucket/key 检查点、后台调度和预算准入仍待装配。保留的最小 guard 会持续占用空间，不能把它描述为常数磁盘容量。

## 3. 尚未交付的完整 Compact 能力

以下内容属于已批准的 J52，当前仍需实现：

- 跨副本 owner RPC 转发、实际用户连接的管理权限复核、前后台身份与调度生命周期。
- 结合 `EventHistoryRetention`、tailer/backfill 和确切根引用的事件、索引、manifest、候选、决定及结果回收闭包。
- invocation 结果窗口的运行时关闭、在途结果查询的持久保护，以及不可重放的最小证明。
- 持久 GC 检查点、重启接管，以及 `physical=false` 的工作登记和 `physical=true` 的完成等待、超时后后台继续。

因此，本次不设置物理完成水位，不将逻辑 CAS 或 KV 页删除解释为完整 physical 完成，也不宣称 J52 验收通过。批准设计中的 physical 投影仍要求全部应回收记录完成精确删除和强读确认；HFile 空间释放另由 HBase compaction 完成。

## 4. 格式与验证入口

system descriptor 需显式包含 `COMPACTION` 类别和 `etcdhbase.m5.compaction-format=1`。没有该能力的 store 拒绝读写 Compact 记录或安装控制身份。已有控制格式增加 optional `compact_operation_id`；旧版严格解码器会拒绝该字段，不能把启用后的 writer 回退称为兼容。J55 仍需接入受控升级与启动能力校验。

公开存储门禁：

```sh
scripts/verify/m5.sh --scope public --task J52 --artifact-root .artifacts/m5/j52-storage-run
```

入口运行确定性逻辑/pin 单测、HISTORY/NATIVE MiniCluster 和固定 v3.5.23 原生 oracle，校验当前测试报告无失败、错误或跳过。它登记在 M5 CI 中，运行报告明确保留第 3 节缺口。真实目标环境、HFile 空间收敛、完整 Java Go 对照和正式 review 不由这个存储门禁替代。
