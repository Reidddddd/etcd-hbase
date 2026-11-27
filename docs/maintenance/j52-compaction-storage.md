# J52 逻辑压缩与历史回收的当前边界

## 1. 交付范围

本次提供内部逻辑 Compact 选择器、KV 历史回收许可、系统记录引用闭包和 HBase 精确退休原语。`CompactionService` 复用现有控制行与 system 记录，`GcCoordinator` 复用持久 snapshot pin 注册表。`KV.Compact` 的授权及逻辑选择通过 `CompactGateway` 装配到既有 owner lane，与发布和恢复串行执行；完成等待在离开该 lane 后执行。独立后台 worker 持续回收 KV 历史，J52 保持进行中。

本 PR 按[范围评审结论](https://github.com/Reidddddd/etcd-hbase/pull/68#issuecomment-7210841766)交付 KV 历史实现切片。当前 `physical=true` RPC 的受限投影只确认本次目标所需的 KV 历史回收及 KV/辅助 pin 保护排空，不等待事件、决定、结果等内部记录域完成。因此，该成功响应不满足[完整设计 §6.1](../specs/2026-11-20-m5-auth-gc-lifecycle-design.md#61-逻辑确认与物理完成)，也不代表整个后端物理完成或 J52 完成。HFile 空间归还仍由 HBase compaction 异步执行。

`physical=false` 在逻辑边界与选定操作身份持久化后返回；`COMPACTION` 记录和控制行的选定身份共同登记可恢复工作，各 bucket 检查点由后台延迟创建。`physical=true` 在请求自己的目标 revision 和 baseSequence 均被所有 KV bucket 的持久完成水位覆盖后返回。取消或超时只结束本次等待，不撤销逻辑边界或后台任务；原操作继续执行，同一 revision 重试仍返回 `ErrCompacted`。更强的后续 Compact 任务可以覆盖旧目标，但不能仅凭更高逻辑边界报告旧目标物理完成。

启用会话认证时，网关从当前 RPC 的 token 解析用户身份。创建 COMPACTION 记录前和逻辑边界 CAS 前，均复核同一会话及授权代次，并使用该用户的真实 HBase 连接检查表组内 system、history/native 和 current 表的 `ADMIN` 权限。缺失、无效或已撤销的会话，以及管理权限不足的请求，在准入时不创建操作记录，也不改变控制行；权限检查不受支持或不可用时同样拒绝执行。会话认证关闭时，配置的后端身份仍须通过相同的 `ADMIN` 检查，不因使用服务身份而豁免。

已获授权并创建 PREPARED 记录后，会话或管理权限若在选择前失效，CAS 前复核阻止推进逻辑边界，已创建的准备记录保留原身份。取消只结束请求结果；用户身份借用仍持有到实际连接或权限 RPC 排空，不取消共享连接 future。HBase 外部 ACL 与控制行 CAS 不属于同一事务；这里的实际权限复核不能宣称提供外部权限系统的原子撤销保证。

新的操作先以 `COMPACTION` 类别持久保存请求身份、授权范围、目标 revision 和完整控制状态摘要，再用控制行 CAS 选择逻辑边界。控制行的 `compact_operation_id` 使丢失 ACK 的操作可以按原身份恢复；替换该身份前，必须先持久确认前一操作的逻辑结论。逻辑压缩不增加 KV revision 或 sequence。控制状态未启用授权发布屏障时拒绝选择；控制行含 Compact 身份但 descriptor 未声明能力时拒绝读取。

初始 `Compact(0)` 与已压缩到零不同。固定 etcd v3.5.23 的原生对照覆盖初始零、负数、重复、未来 revision，以及 Compact 后 Range/Watch 的 C−1、C、C+1 边界。重复与未来 revision 在网关中投影为 `OUT_OF_RANGE`。Go 测试提供原生行为依据；Java RPC 另有真实 Kerberos MiniCluster 验证，尚不代表完整 Java/Go 客户端互操作验收。

`Compact(0, physical=false)` 的 protobuf 编码可以为空，网关仍以一个最小计量单位提交 owner lane。队列因关闭或容量限制拒绝请求时，网关会投影该错误。已派发工作使用队列拥有的服务 scope 完成；成功响应等待该 scope 清理结束，清理失败不会被投影为成功。是否可以返回成功仍由原 RPC scope 决定，客户端取消或清理失败不等于回滚已选择的逻辑边界。

历史 Watch 回放的寻址、控制行复核和事件读取使用普通请求预算，各自保留有界读取 scope；不占用 Lease 到期处理和共享 tailer 的后台保留容量。普通预算不足仍按资源错误终止对应订阅，不扩大配置容量或丢弃事件。取消后，读取预算及历史保护继续持有至实际 producer 排空。

## 2. KV 历史回收约束

回收许可绑定 profile、pin 注册表、逻辑操作及本轮 scope。所有分片的 revision/sequence 注册 floor 确认后，活动 pin 继续分别限制两个边界。回收器重新核对 owner 和控制身份；HBase 适配器再次核对本表组实际 pin 注册表。`HistoryGcPermit` 只允许处理冗余 KV 物化版本；系统记录使用独立许可。

HISTORY 每个 key 保留 safe revision 处的最后基线、墓碑和更高版本。旧行用完整 guard 条件，在同一 `RowMutations` 中写入 RETIRED guard 并精确删除 `m/v`，随后强一致确认。退休证明继续阻止迟到 materializer 重插。

NATIVE 同样保留基线和更高版本。删除明确 timestamp 的 `m/v/f`，并用最新 fence 的完整内容 CAS 安装维护代次。并发新写先获胜时重新读取并计算计划；ACK 丢失先强读，不给旧计划换条件重发。取消结束调用结果，在途 HBase producer 仍须排空；本页未确认时调用方不能推进游标。

每次 HISTORY 页最多处理 64 个版本，NATIVE 页最多删除 64 个版本。Native 选页前读取 descriptor 允许的全部 metadata/fence 版本，内存预留按实际版本上限计算。枚举每次只寻找一个 bucket 内的下一个 key，不读取业务 value；游标排除该 key 的完整物理前缀，不跨 salt bucket 比较逻辑 key。

后台每轮只推进一个持久 key 意图、一个版本页或一次 bucket EOF，随后轮转 bucket，并使用独立的维护 scope、deadline 和常规预算。没有 Lease 运行时也会在启用 Compact 能力时装配 KV worker。删除前持久保存 active key，删除及强读确认后才用精确旧值 CAS 推进版本游标；key 完整处理前不能越过它。owner 接管保留已确认游标，并以 owner epoch 与操作代次约束后续进度；未知结果下一轮重读，不推断成功。

检查点分别保存本轮冻结的 safe revision/sequence 和已完成 revision/sequence。旧 pin 导致的保守低水位只表示部分回收，不能完成更高请求目标；安全边界扩大后，下一轮从 bucket 开始重新检查。只有后端 EOF 且所有 key 页确认后才发布该轮完成水位。正的 completed generation 证明至少完成过一轮，默认 `0/0` 计数不能证明 `Compact(0)` 已完成。较强任务开始时保留旧完成回执，取消、进程终止和 owner 接管均不清除它。

请求等待方以有界 point read 检查各 bucket；等待不占用 owner lane，pin 尚未解除时 Put 仍能执行。请求与后台独立，取消后等待方仍持有准入和实际读取资源直至 producer 排空，成功响应还须等待清理结果。运行时关闭或失去 owner 时停止 worker 和等待方，排空后再释放底层连接及自有 timer。保留的最小 guard 会持续占用空间，不能把它描述为常数磁盘容量。

## 3. 系统记录退休与结果窗口

`GcCoordinator.authorizeSystemRecords` 先确认事件及 backfill 保护，再推进 KV 注册 floor。签发的不可变许可绑定 profile、两个 sealed 注册表及配置、event namespace、owner 与授权屏障、选定 Compact 操作、独立 revision/sequence 边界、确切根保护和本轮带 deadline 的 scope。普通调用方不能自行构造许可。消费端从目标 HBase 表重新读取控制行、LOGICAL 操作、实际注册表与已收尾 backfill checkpoint；缺失、损坏或身份变化均停止本页，不把原调用方提供的端口当作持久权威。

`HBaseSystemRecordClosure` 绑定实际 store 及其路由，保留 invocation 的不可重放证明。其余域按真实身份解码，检查存活 invocation、decision、事件 revision 索引、manifest root/directory/page 和 result directory 的引用；PREPARING/SEALED 事务在后续引用写出前也受保护。确切 pin 根和事务独立于两个 floor 保留。证明扫描完整分页，损坏或超出扫描、行数及字节预算时返回失败，不发布可恢复的成功游标。分页沿实际 HBase 行序推进，墓碑与 lookahead 仍计入物理工作量。

`InvocationWindowCloser` 使用同一 store 的实际 invocation repository。它先核验许可、边界及确切保护，再用 CAS 关闭结果窗口；保留摘要及不可重放绑定，随后通过引用闭包处理 RESULT。仍被 manifest 或其他目录引用的结果继续保留，不能因窗口已关闭就直接删除。

退休用同一 `CheckAndMutate(RowMutations)` 比较完整旧值和墓碑缺失，移除旧 value，并保留 `r={0}` guard 与严格编码的 `t` 墓碑。两者使用保留的内部 timestamp `Long.MAX_VALUE - 1`，使迟到的旧 absent-CAS 不能让记录复活；它不是 KV revision 或应用墙钟。目标 HBase 的 future timestamp 策略必须允许该保留值；拒绝时退休失败，不声明已删除。MiniCluster 覆盖此协议，真实目标环境的 timestamp 配置尚未验收。

## 4. 系统记录后台检查点

启用 Lease/事件运行时后，系统记录 GC 在独立的常规准入中运行；它不占用恢复、续约和到期处理的 reserve。没有选定 Compact 或正在发布时等待；授权、证明和存储失败会记录 GC failure 并有界重试，准入不足记录 GC backlog。页计数、已检查记录数和退休记录数与 Lease expiry 指标分开统计。

每次调度最多处理一个 `recordDomain + bucket` 页，随后轮转到下一分片；游标沿各 bucket 的物理行序独立推进。检查点绑定实际 Compact operation/generation、owner epoch、盐桶拓扑和本轮 revision/sequence 边界；同一许可下接管保留进度，拓扑变化或行值身份不一致则拒绝恢复。整页成功后才用精确值 CAS 推进。旧 worker 持有的字节不能覆盖接管后的检查点，未知结果可以有界重做。

首次领取、owner 接管或 Compact 操作切换先持久进入 `FENCING`，此时只持有元数据租约，不能开始退休。fence 游标与原扫描游标分别保存；同 owner 中断可继续 fence 游标，新 owner 必须重新覆盖整个目标分片。全部有界物理页确认并 CAS 转为 `SCANNING` 后，才激活新的回收者。

每个 live 系统记录在独立维护 family `gc` 中保留严格编码的 `g` Cell，内容是 owner epoch 和 Compact generation，使用保留 timestamp `Long.MAX_VALUE - 2`。安装只允许单调提升；退休在同一目标行的 `CheckAndMutate` 中同时比较完整旧值、墓碑缺失及确切 `g`，不能在失败后换上更高 fence 重发。接管前已校验的目标行必然已存在；永久 guard/墓碑保留物理行，使完整 fence 遍历能够覆盖这些迟到请求的目标。fencing 期间仍可能结束的旧请求，与新 holder 激活后被同行条件拒绝的旧请求分别验证。已退休行由永久 `r`/`t` guard 阻断旧写，不重复安装 `g`；新行在实际退休前也安装当前 fence。

未知 fence ACK 不推进游标；重读确认同一 fence 后可有界重做。损坏、缺页、scope 关闭或较高 fence 均不能被解释为已完成。业务 point/scan 只请求原 payload family，live 仍是唯一 `r`，retired 仍是 `r`/`t`；业务条件写也不覆盖维护 family。GC 独立读取并严格校验 `gc:g`，退休请求读回已退休行时核对 Compact generation，两个 family 的退休条件仍在同一 HBase 行上原子执行。维护字段不进入旧 reader 的 Get/Scan 投影，也不是 KV revision。

GC 要求持久 descriptor 声明 `etcdhbase.m5.gc-fence-format=1`，并存在非过期、单版本的 `gc` family。family 数量、名称、格式或保留策略不符会拒绝绑定；仅有原 Compact 能力时，普通记录仍可读取，但 GC closure、退休入口及配置了 Compact 的 M4 运行时拒绝激活 GC，不自动增加 family 或改写 descriptor。未声明 Compact 的 M4 表不启动这个 worker。启动时的能力快照只作初始门禁；每次 GC 权威核验及 fence 写入入口还从实际 HBase Admin 重读当前 descriptor，观察到 marker 缺失、未知/损坏版本、缺少 family、错误保留策略或 Compact 能力撤销时，拒绝扫描与维护 mutation。该读取绑定 RequestScope 的取消和真实 producer 排空，不以旧缓存继续写入。

当前 descriptor 读取不是 DDL 与数据行的跨操作事务。schema 变更前仍须停止并排空维护 writer；不支持一边修改 descriptor、一边让已经获准的维护请求继续执行。

该 descriptor 由独立的 provisioning helper 显式构造，本 PR 没有在线 schema 升级或回退命令。已启动旧 reader 继续使用原 family，真实基线 reader 的读取和扫描已单独验证；旧二进制的新启动会因 descriptor 不再是单 family 而拒绝绑定。不能靠删除 `gc` family、去掉 marker 或运行旧维护 writer 实现在线回退；启用及后续迁移须停止旧维护 writer，不支持混跑不检查同行 fence 的维护实现。J55 的受控迁移与真实目标环境的滚动升级/回退验收仍未交付。

完整 sweep 结束，或真实许可的操作、代次变化后，持久增加 sweep 编号并重新检查该 bucket。同一 Compact 的安全边界扩大时，当前 sweep 保留原保守边界，从新取得的真实许可派生更窄许可继续分页；下一 sweep 再采用新边界，避免前台 sequence 增长反复重置游标。派生许可保留全部权威、根保护及 scope，拒绝放宽边界。这样，前一轮因 pin 或引用而保留的前缀不会永久跳过；`completedPages` 保持累计，避免重置游标造成检查点 ABA。记录退休仍由后端独立复核实际授权。

每页有记录数、扫描字节、引用证明及总时长上限，并保留整轮粗粒度 I/O、scanner、清理和内存额度。取消后额度继续占用到实际 producer 排空，调度器还等待清理结果；排队任务在停止后不再启动回收。当前不是逐条 HBase RPC 的完整费用核算，也未交付 GC 导致的前台尾延迟和长期空间收敛验收。

## 5. 尚未交付的完整 Compact 能力

以下内容属于已批准的 J52，当前仍需实现：

- 跨副本 Compact owner RPC 转发、完整记录域的调度生命周期，以及真实目标环境的管理权限验收。
- invocation 结果窗口的运行时关闭、在途结果查询的持久保护，以及不可重放的最小证明。
- 把事件、决定、结果等内部记录的精确删除、引用解除及保护排空纳入同一目标的持久完成证明。现有系统 GC sweep EOF 不能独立证明该闭包已排空。

本次只设置 KV 历史完成水位，不将它解释为完整 physical 完成，也不宣称 J52 验收通过。完整设计仍要求全部应回收记录完成精确删除和强读确认；本 PR 的受限 RPC 行为及其偏差见第 1 节。

## 6. 格式与验证入口

系统记录 GC 检查点值使用格式 2，分别编码 fence/扫描游标及明确的 phase；格式 1 会被严格拒绝，不自动回退或丢弃旧进度。新旧维护格式的受控升级仍待 J55。KV 历史检查点使用独立的 `RECOVERY_CHECKPOINT` 身份 `HGC/1` 和值 `HGV/1`，不复用系统记录扫描游标；编码绑定 profile、16 字节 pin 注册表身份、owner epoch、操作代次、bucket 拓扑和完成回执。

system descriptor 需显式包含 `COMPACTION` 类别和 `etcdhbase.m5.compaction-format=1`。没有该能力的 store 拒绝读写 Compact 记录或安装控制身份。系统记录 GC 还要求第 4 节的独立维护 family 和持久 fence 能力；不将原 Compact 标记解释为可直接写入新的维护布局。已有控制格式增加 optional `compact_operation_id`；旧版严格解码器会拒绝该字段，不能把启用后的 writer 回退称为兼容。J55 仍需接入受控升级与启动能力校验。

公开存储门禁：

```sh
scripts/verify/m5.sh --scope public --task J52 --artifact-root .artifacts/m5/j52-storage-run
```

入口运行确定性逻辑/pin/许可及扫描单测、HISTORY/NATIVE MiniCluster、系统记录与结果窗口 MiniCluster，以及固定 v3.5.23 原生 oracle，校验当前测试报告无失败、错误或跳过。门禁构建至 `server`，保留原有 HBase 集成套件，并覆盖 GC 生命周期、KV 与系统记录检查点及真实双 bucket 重启回收。`J52PhysicalCompactIT` 使用实际管理身份、生产 runtime 和 gRPC，分别验证两种布局下旧 pin 阻塞、owner lane 可继续写入、全部目标版本精确删除及基线保留，以及取消后强杀 owner JVM、接管继续原任务和逻辑请求登记后台工作。测试门禁要求这些方法实际执行，且 Kerberos 测试按 JVM 隔离。它登记在 M5 CI 中，运行报告明确保留第 5 节缺口。真实目标环境、HFile 空间收敛、完整 Java Go 对照和正式 review 不由这个存储门禁替代。

J52 专属门禁在实际执行的 `server` reactor 中登记 `CompactGatewayAuthorizationTest`，覆盖请求身份、每张目标表的 `ADMIN` 权限、真实 gRPC token 传递、CAS 前失效及取消排空；任一授权套件或关键方法缺失均不能通过报告校验。J09 仍执行完整 server 构建，J52 同时保留 GC worker 的真实执行套件。真实 Kerberos/HBase 的 `ADMIN` 授予与撤销反例属于 J04 的安全后端集成测试；替身网关测试和实际后端 ACL 测试各自保留验证范围。

Watch 回放与后台容量的隔离由 server 的确定性回归覆盖：后台保留槽全部占用时仍可在普通预算内回放，在途回放也不抢占最后一个后台槽；普通预算耗尽时仍验证资源拒绝及实际容量诊断。真实双布局 Go 回放、fragment 和断流恢复由既有 J46/J47 门禁验证。
