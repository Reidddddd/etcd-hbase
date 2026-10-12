# 分类验收目标与负载初稿（J00.3）

状态：待评审。当前只完成分类、静态参数与可执行验证场景的定义，生产目标、目标部署覆盖项和测量结果待输入。源码基线为 [Milvus v3.0.1](milvus-etcd-usage.md)，HBase 容量和运维能力成熟是已确认前提。

## 环境与输入记录

| 输入 | 当前事实 / 约束 | 正式验收所需输入 |
|---|---|---|
| Milvus | 最低版本 v3.0.1；固定审计 commit，后续版本重新审计 | M6 使用的镜像摘要、实际覆盖配置及消费者启用项 |
| 部署与 MQ | 新集群、外部 etcd endpoints、Kafka；服务不依赖 Kafka | Pod/集群数量、连接方式、TLS/profile、Kafka 版本与资源配置 |
| HBase / Hadoop / client | 标准 Java client，JDK 21，实际组合未提供 | B02 版本、访问方式、依赖及运行参数 |
| 盐与表组 | 全部物理 rowkey 加盐，独立表组绑定端点 | B01 算法、编码、桶数、样例及预分区；B05 表组/Region 规模 |
| 身份 | Authenticate 提供后端凭据，token 跨副本关联实际身份 | B03 身份建立、权限、后台/重启激活及轮换；B06 TLS/交接规则 |
| 工作负载 | 有源码参数和调用形态，没有生产测量数据 | B05 KV/value/版本/删除比例、QPS、Lease/Watch/事件率与峰值 |
| 运维 | 无本地业务持久化，多副本可替换 | B06 RPO/RTO、备份窗口、告警与负责人 |

目标未填写不阻止 J01/J02 或公开候选原型；正式容量、性能和生产验收前冻结相应目标/profile。没有实际目标身份或部署证据时，不记录环境验收已通过。

## 已核对的静态参数

这些是固定源码默认值，不是生产部署值或验收承诺；配置覆盖、运行组件数和实际 TTL/续租结果需随报告记录。

| 参数 | 源码值 | 依据 |
|---|---|---|
| Session TTL / 注册重试 | 15 秒 / 30 次 | [component_param.go][component] |
| 续租发送调度 | 基于响应 TTL/3；还有 client 发送轮询和重连 | [etcd lease.go][lease] |
| metastore.paginationSize | 100000 条 | [service_param.go][service] |
| metastore.readConcurrency | 32 | [service_param.go][service]；DataCoord 恢复还发出五类元数据扫描 |
| metastore.maxEtcdTxnNum | 64 操作，实际请求还受比较/字节/客户端限制 | [service_param.go][service]；不能当作服务已确定的事务上限 |
| EtcdKV 请求 timeout | 默认 10 秒，创建方可覆盖 | [etcd_kv.go][kv] |
| 配置读取 / 默认轮询周期 | 3 秒 / 5 秒 | [etcd_source.go][config]、[base_table.go][base] |
| legacy GC 批次/页 | 2000 | [legacy_snapshot_gc.go][gc]，tombstone GC 复用 |
| 健康检查上下文 | 列表 3 秒、每 member 探测 3 秒 | [etcd_util.go][client] |
| Go client 发/收限制 | 2 MiB / math.MaxInt32（默认未覆盖时） | [client options.go][options]；不是服务预算 |

示例：10000 Lease、TTL=15 秒、约每 5 秒续租，约 2000 次逻辑续租/秒。例子用于构造负载，与客户端调度、重连、持久化和索引写放大分开计量，不作为生产目标。

## 分类目标表

| 类别 | 必须记录的目标/口径 | 目标值 | 输入/验收归属 |
|---|---|---|---|
| 容量 | KV、value/长 key 分布、版本/删除比例、Lease/Watch、表组与副本数 | 待输入 | B05；J17/J70/J71/J76 |
| 请求延迟 | 点读/范围读/写/Txn/续租 QPS，p50/p95/p99，排队等待与拒绝率 | 待输入 | B04/B05；J29/J70–J77 |
| 启动恢复 | 单/多集群恢复时间、collection 并发、扫描页大小和实际返回字节 | 待输入 | B05；J17/J70 |
| 接管与流恢复 | owner 接管、Watch 追赶/回放、KeepAlive 重连、到期处理延迟分别计量 | 待输入 | B04/B06；J29/J62/J74/J75 |
| 访问成本 | 每业务操作 Get/Put/CAS、实际读写字节、scanner/预取、过滤、恢复/GC IO | 待输入 | B04/B05；J17/J29/J72/J76 |
| 资源上限 | JVM heap/direct memory、scanner/RPC/流/队列/缓存/pin/响应与事务字节 | 待输入 | B05/B06；J13/J47/J56/J72/J76 |
| 存储维护 | 历史窗口、事件保留、逻辑 Compact 和回收后的磁盘收敛、备份窗口 | 待输入 | B05/B06；J52/J53/J57/J76 |
| 可用性与管理收益 | RPO/RTO、故障恢复、扩缩容/升级步骤、平台运维成本 | 待输入 | B06；J57/J75/J80–J83 |
| 原生对照 | 相同请求与负载分别报告正确性、容量、延迟、恢复和成本 | 待输入 | J07/J77，不预设每项更快 |

每个最终目标另记录责任人、确认日期和 profile 版本。HBase 容量充足不代表兼容层队列、串行提交或单请求内存无上限。

## 负载 profile 初稿

| ID | 可复现场景 | 必须变化的参数 | 核验任务 |
|---|---|---|---|
| P01 点与事务 | 原始 bytes 点读写、VERSION/VALUE/MOD CAS、混合事务、大 value/长 key/删除 | 数据规模、value 分位、并发、事务操作数/字节、副本数 | J29/J72/J77 |
| P02 启动恢复 | DataCoord collection 并发 × segment/insert/delta/stats/BM25 五类扫描 | 默认并发 32 作为一个 profile，桶数、页大小、空/墓碑/版本比例、同时恢复集群数 | J17/J70 |
| P03 完整与存在性扫描 | 无 Limit Prefix、100000 条页、HasPrefix、GcConfirm 全范围读取 | 匹配量、空桶、深度、响应字节与取消时机 | J14/J15/J17/J70 |
| P04 租约 | 15 秒默认 session、多角色绑定、续租/到期/业务提交竞争 | Lease 数、绑定数/字节、重连峰值、TTL、时钟/暂停、Pod 数 | J29/J42/J43/J75 |
| P05 监听 | 列表 R→Watch、事件回放、DELETE PrevKV、fragment、慢消费 | 订阅数、事件率/字节、历史窗口、大 revision、同时重连及副本数 | J44–J47/J62/J75 |
| P06 配置 | 5 秒轮询与线性化写后刷新，无缓存与优化路径对照 | 进程数、key/value 大小、轮询抖动、身份数、写频率 | J36/J63/J71 |
| P07 K8s 与身份 | A 登录 B 请求、删 Pod、全 Pod 重建、滚动更新和缩容 | 连接/流分布、凭据轮换、未就绪认证路由、owner/后台任务接管 | J50/J56/J71/J75 |
| P08 长稳 | 持续写入/删除、历史累积、停止写入后压缩/安全回收，备份并行 | 总 KV/版本、历史窗口、保留事件、GC 速率、表组数 | J52/J53/J57/J76 |

数据集种子、生成规则、实际部署配置摘要、预热、稳态/故障时段、采样窗口和重复次数在运行前固定。记录成功、错误、超预算和取消全部结果，不仅统计成功请求。

## 当前可下结论的范围

已确认版本、静态参数和调用形态；上述 profile 与目标分类可指导 J01–J07 及后续原型。尚不能下性能、故障持久性、生产容量、JDK/HBase 认证兼容或 Milvus 端到端成功的结论。

[component]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/pkg/util/paramtable/component_param.go#L1014
[service]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/pkg/util/paramtable/service_param.go#L553
[kv]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/kv/etcd/etcd_kv.go#L38
[config]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/pkg/config/etcd_source.go#L36
[base]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/pkg/util/paramtable/base_table.go#L123
[gc]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/metastore/kv/rootcoord/legacy_snapshot_gc.go#L18
[client]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/pkg/util/etcd/etcd_util.go#L311
[lease]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/client/v3/lease.go#L515
[options]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/client/v3/options.go#L30
