# Milvus etcd 调用基线（J00.1）

状态：待评审。审计日期：2026-10-12。此文记录源码行为，不代表兼容服务或集成测试已通过。

## 固定版本与适用范围

| 项目 | 固定基线 |
|---|---|
| 最低支持 Milvus 版本 | 用户确认至少 v3.0.1；本轮实际审计 v3.0.1，后续版本接入前重新核对 |
| Milvus 源码 | [v3.0.1 / 658cbd16899bb715a17d4d6f727531a376678ca6][milvus] |
| Go 模块 | 根模块与 `pkg` 模块均声明 Go 1.26.6，etcd API/client/server 均为 v3.5.23；见 [根 go.mod][gomod]、[pkg/go.mod][pkgmod] |
| etcd 协议、client 与原生对照源码 | [v3.5.23 / 1cf4bc40111fd95d1e14a90a09a146357b15eb13][etcd] |
| Java 服务要求 | JDK 21；与 Milvus 的 Go 工具链分别记录 |
| 集成部署约束 | 新建集群、外部 etcd endpoints 指向兼容服务、Kafka；兼容服务不依赖 Kafka，不验证 Woodpecker 内部调用 |
| 认证约束 | Milvus 使用 HBase 凭据配置 etcd client；经 Authenticate 获取 token，TLS 和实际后端身份机制按 B03/B06 确认 |
| 环境状态 | 未提供生产 HBase/profile、部署覆盖配置或实际负载；未运行 Milvus、Go client 或原生 etcd 对照 |

v3.0.1 的实际 etcd 依赖与计划 v3.5.23 一致，无需因本次 Milvus 版本选择改变协议基线。不能把“至少 v3.0.1”解释为所有更高版本已经验收。探索过程中查看的 v2.6.7 和 master 提交不属于本次交付基线。

源码摘要、直接导入 client 的文件清单、协议方法清单见 [机器可读审计记录](baselines/j00-source-baseline.json)。根/pkg 清单一致，且未发现对 etcd 模块的替换；本轮没有执行 `go list -m all`，不将静态依赖核对称为实际构建验证。

## 审计方法

1. 在固定源码中枚举直接导入 `go.etcd.io/etcd/client/v3` 的非测试 Go 文件，检查调用选项、响应字段和错误消费。
2. 对 MetaKv/TxnKV 的上层消费者继续追踪到 etcd 实现；仅搜索直接导入 client 的文件不足以覆盖 catalog 和 TSO。
3. 追踪 client/v3 内部的 Authenticate、token 刷新、Watch/KeepAlive 重连及维护 RPC 映射，区分 Go 方法和 wire RPC。
4. 记录迁移工具、embedded 路径、测试辅助与 Woodpecker 路径，防止把它们混入 Kafka + external-etcd 集成结论。
5. 没有观察到直接调用的已承诺功能仍保留在 [API 矩阵](api-matrix.md)，以需求和协议作为依据，不缩减首版范围。

本轮是源码审计，未执行全程序静态调用图、条件编译检查或运行时追踪。源代码中通过接口、动态选项传递的组合仍需 J07/M6 覆盖；文件清单是检索入口，不是每个文件都会发出 RPC 的声明。

## 调用与消费者

下文 `M` 表示 Milvus 配置得到的 meta root，`K` 表示 KV/TSO root。它们是客户端 key 前缀，服务接收原始字节，不根据前缀白名单筛选请求。

| ID | 入口 / 源码 | key 或区间、请求组合 | 响应消费与失败行为 | 矩阵 / 验证 |
|---|---|---|---|---|
| U01 | [EtcdKV 点读/前缀读][kv]：Load、LoadBytes、LoadWithPrefix、LoadBytesWithPrefix2、LoadBytesWithRevision | root 下精确 key 或 `[prefix, GetPrefixRangeEnd(prefix))`；前缀 + key 升序；无 limit | 使用 Kvs/Value/Version、Header.Revision；Count 用于分配容量或判空；后端错误向上传播 | R01/R02/R06；C01/C02/C06 |
| U02 | [WalkWithPrefix][walk] | 显式 range_end、全局 limit、key 升序；下一页从 `lastKey + 0x00` 开始，固定区间上界 | 按 More 决定是否继续，不用 Count 决定页数；More=true 时必须有可推进的 Kvs。没有传固定 WithRev，不能声明整个多页 Walk 是单一快照 | R03；C03 |
| U03 | [Has/HasPrefix][exists]、[Session.GoingStop][session]、[DataCoord 判空][catalog] | 精确 CountOnly；前缀 CountOnly + Limit=1；GcConfirm 另见 U10 | Count 的真实 0/1 决定存在性，失败不能转成不存在 | R04/R05；C04/C05 |
| U04 | [EtcdKV 修改/事务][kv]、[parsePredicates][predicates] | Put/Delete 点和前缀；MultiLoad 在 Txn 中 OpGet；MultiSave/Remove 使用 Then；VERSION/VALUE 等值 compare | Txn.Succeeded 决定 CAS 或 IO 失败；读取 Responses 中 Range/Kvs；修改不能部分可见 | P01/D01/D02/T01/T02；C07–C12 |
| U05 | [Session ID 与注册][session]：checkIDExist、getServerIDWithKey、registerService、getOpsForCoordinator | `M/session/id` 的 VERSION=0 初始化、VALUE CAS 自增；角色 key 的 VERSION=0 + Put(lease) | Grant.ID、Txn.Succeeded、Header.Revision。协调器同一 Txn 注册主体与三个 legacy role，另写不带 Lease 的 version key；不能假定全部操作都带同一 Lease | T01/L01/P01；C11/C16 |
| U06 | [KeepAlive 与 TTL 检查][session] | LeaseKeepAlive 双向流；连接前检查 TimeToLive；停机 Revoke | TTL<=0 或 ErrLeaseNotFound 会导致 session 失效/进程退出；流关闭后检查 TTL 再重连，不能伪造租约已存活 | L02/L03/L04；C17/C18 |
| U07 | [Session 列表/Watch/主备][session]、[QueryCoord][query]、[ProxyWatcher][proxy] | `M/session/<role>` Prefix + key 排序；Watch Prefix/PrevKV/WithRev。QueryCoord、Proxy 从列表 R+1 开始；Session rewatch 和主备使用调用方传入的 R | Header.Revision 建立监听起点；DELETE 使用 PrevKv.Value 解析节点；错误经重新列表或终止处理 | W01–W03；C19–C22 |
| U08 | [Streaming sessionDiscoverer][discoverer] | 初始 Get 使用 Serializable；非 exclusive 使用 Prefix；Watch 从已记录 R+1 开始 | PUT 更新节点，DELETE 根据 Kv.Key 移除；以 Watch Header.Revision 更新水位；出错退出本轮 Discover，由上层重建 | R02/W01；C20/C21 |
| U09 | [EtcdSource][config]、[BaseTable][base] | `root/config` 字节前缀；周期读 Serializable；写后刷新不带 Serializable；3 秒读取上下文，默认 5 秒轮询 | 构建配置 map；失败返回错误，不更新为空配置。线性化写后读须覆盖已成功写入 | R02/R07；C23 |
| U10 | [DataCoord catalog][catalog]、[恢复入口][dcmeta] | collection 并发恢复；segment、insert/delta/stats/BM25 五类扫描；分页默认 100000。GcConfirm 使用 LoadWithPrefix 完整读取 collection/partition 前缀 | 反序列化 Kvs；GcConfirm 只判断 keys/values 都为空，失败返回 false；不预支改成 HasPrefix 的收益 | R02/R03；C02/C03；负载 P02/P03 |
| U11 | [RootCoord legacy GC][tombstone]、[legacy snapshot GC][snapshotgc] | 类别前缀 Walk，页/批量 2000；检查 value 后分批删除，key 从完整路径转为相对路径 | 新集群没有 legacy 数据也可能发起空范围扫描；不要因“无迁移”删掉兼容调用。业务 tombstone 字节只是 value，不是服务逻辑删除 | R03/D01/T01；C03/C09 |
| U12 | [Streaming catalog][scatalog]、[Node catalog][ncatalog]、[恢复持久化][recovery] | `M/streamingcoord-meta/`、`M/streamingnode-meta/wal/<pchannel>/...`；子元数据批量读写，随后保存 salvage checkpoint 和 consume checkpoint | 元数据全部成功后才推进 checkpoint；这是多个请求之间的顺序，不是要求服务理解 Kafka 消息 | P01/T01；C24 |
| U13 | [CDC meta][cdc]、[controller][cdcwatch] | 复制任务 Prefix + key 排序；MOD=读取到的 ModRevision 条件删除；Watch Prefix/PrevKV/WithRev | ModRevision 和 Txn.Succeeded 防止迟到删除新任务；Header.Revision 接列表与监听 | R02/T02/W01；C12/C25 |
| U14 | [TSO][tso]、[TSO KV root][tsokv] | K 下的单 key Load/Save，高水位原始字节 | v3.0.1 的 loadTimestamp 对任何 Load 错误返回 ZeroTime,nil；此风险仍存在。J66 必须修复调用方或验证隔离方案；服务须保留失败 | R01/P01/E01；C26 |
| U15 | [Telemetry][telemetry]、[构造路径][telemetrymanager] | `/client-telemetry/configs/` 全局前缀，点 Put/Delete，无 Lease；不局限 M/K | 启动 loadCache 读取错误仅记录日志；旧配置删除 best effort；一次性命令 TTL 在客户端内存，不是 etcd Lease | R02/P01/D01；C27 |
| U16 | [HealthCheck][client] | MemberList → 逐 member ClientURLs 建 client → Get("health") → AlarmList | ClientURLs 必须非空、可连接；不存在的 health key可正常成功，PermissionDenied 也被视为探测成功；真实 alarm 或失败会使健康检查失败 | O01/O02；C28 |

## client 自动行为与错误链

有用户名/密码时，client 初始化会先调用 Authenticate，再以 gRPC metadata `token` 携带返回值；见 [client 初始化/getToken][clientinit]、[credentials][credentials]。Milvus 没有显式 `Authenticate()` 调用不代表此 RPC 不需要实现。没有配置用户名/密码时不发这个请求；配置了凭据但服务返回原生 ErrAuthNotEnabled 时，client 可清除 token 继续连接。开发无认证必须明确配置，生产认证失败不能冒充 auth 未开启。

配置中的 dial keepalive 是 HTTP/2 传输 ping，与 LeaseKeepAlive 不同。client AutoSyncInterval 默认 0，Milvus 工厂未主动设置；因此不要声称所有 client 启动都发 MemberList，但 HealthCheck 确实会发。Status 是已承诺运维能力，另有 client RejectOldCluster/etcdctl 可触发，不能因业务源码未直接调用而删除。

| 层次 | 实际行为 / 必须核对的边界 |
|---|---|
| Java/HBase → gRPC | 存储不可达、权限、deadline、CAS 结果未知分开；空 Range 只表示成功读取后 key 不存在 |
| gRPC → rpctypes | [error.go][errors] 按错误描述识别 etcd sentinel；只匹配 code 不足以触发 token 刷新或 compact 恢复 |
| client unary 重试 | [retry.go][retry]：Range 等只读请求对 Unavailable 可重试；Put/Delete/Txn 的重试限制更严格，不能把任何后端错误伪装成客户端连接尚未建立 |
| token 刷新 | [retry_interceptor][interceptor] 识别 ErrInvalidAuthToken、ErrAuthOldRevision 和有凭据时的 ErrUserEmpty；不可把正常凭据失效变成 PermissionDenied |
| Watch | [watch.go][watchclient] 管理重连、创建响应、revision 和 fragment；[Milvus IsRetriableWatchErr][client] 还显式识别 compaction 与 token 错误，重读列表再订阅 |
| Lease | [lease.go][leaseclient] 按响应 TTL/3 安排下次发送；TTL<=0 关闭相应 channel；Milvus 的 TimeToLive 分支还检查 lease sentinel |
| EtcdKV → Milvus | getEtcdMeta 使用 merr.WrapErrIoFailed 包装失败；Load 在成功但 Count=0 时生成 key-not-found；TSO 仍吞掉上述 Load 错误，GC 则失败视为未完成 |

Go client 默认发送限制 2 MiB、接收限制 math.MaxInt32，默认 unary 重试上限 100；见 [options.go][options]。这些是客户端边界，不是本服务的响应预算或吞吐目标。J06/J07 验证客户端最终观测结果，J66/M6 验证真实消费者，当前只有静态证据。

## Count 差异的消费者审计

| 消费者 | 用法 | 首版规则的影响 |
|---|---|---|
| Load/LoadBytes、session ID/version 查询 | 点读 Count 判空 | 精确 0/1 保持语义 |
| Has、GoingStop | 精确 CountOnly | Kvs 为空、Count 真实 0/1 |
| HasPrefix | Prefix + CountOnly + Limit=1 | 仅要求真实存在性；不存在需检查相关桶 |
| LoadWithPrefix、LoadBytesWithRevision、CDC 列表 | Count 用于切片容量，返回全部 Kvs | 无 Limit 的 Count=len(Kvs)，不改变结果 |
| WalkWithPrefix | 根据 More 和最后一个 key 翻页 | 本页 Count 不影响循环；必须保证 More 真实、More=true 时有可推进结果 |
| [旧版本迁移工具][migration] | 无 Limit 的范围 CountOnly 得总数，再按总数循环 | 明确不适用于本项目首版的新集群范围；该 CountOnly 返回明确不支持，不为迁移工具扩展准确计数 |

本轮未发现 Kafka/external-etcd 主路径以分页 Count 当全范围总数的直接消费者；这不是动态路径的完整性证明。后续版本、部署覆盖项及接口传入选项需在 M6 重核。

## 已观察调用与首版需求的关系

直接生产路径观察到 VERSION/VALUE/MOD compare、key 升序、Prefix/Range/Limit、存在性计数、Lease 绑定与流、Created/PrevKV/revision Watch、Serializable 读以及 Authenticate/MemberList/Alarm 的调用链。

FromKey、KeysOnly、Range revision 过滤、其他 compare 目标与结果、nested/混合事务、Watch progress/filter/fragment、Lease 列表/TTL keys、Compact、Status 等仍按开发计划完整交付。部分来自迁移/工具/client 库能力或显式兼容要求；“未观察到直接业务调用”不产生范围删除授权。字段边界及验证归属见 API 矩阵。

## 后续版本复核

接入 v3.0.2 或更新版本时重新核对 root/pkg go.mod 与 replace、client 构造参数、全部直接导入文件、接口消费者、错误消费和部署 profile。发生依赖、key 形态或消费语义变化时更新固定 commit、摘要和矩阵证据，并执行对应 J07/M6 用例；不能只修改版本号继续沿用当前结论。

[milvus]: https://github.com/milvus-io/milvus/tree/658cbd16899bb715a17d4d6f727531a376678ca6
[gomod]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/go.mod#L36
[pkgmod]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/pkg/go.mod#L39
[etcd]: https://github.com/etcd-io/etcd/tree/1cf4bc40111fd95d1e14a90a09a146357b15eb13
[kv]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/kv/etcd/etcd_kv.go
[walk]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/kv/etcd/etcd_kv.go#L79
[exists]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/kv/etcd/etcd_kv.go#L140
[predicates]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/kv/etcd/util.go
[session]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/util/sessionutil/session_util.go
[catalog]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/metastore/kv/datacoord/kv_catalog.go
[query]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/querycoordv2/server.go
[proxy]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/util/proxyutil/proxy_watcher.go
[discoverer]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/util/streamingutil/service/discoverer/session_discoverer.go
[config]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/pkg/config/etcd_source.go
[base]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/pkg/util/paramtable/base_table.go
[dcmeta]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/datacoord/meta.go
[tombstone]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/metastore/kv/rootcoord/legacy_tombstone_gc.go
[snapshotgc]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/metastore/kv/rootcoord/legacy_snapshot_gc.go
[scatalog]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/metastore/kv/streamingcoord/kv_catalog.go
[ncatalog]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/metastore/kv/streamingnode/kv_catalog.go
[recovery]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/streamingnode/server/wal/recovery/recovery_background_task.go
[cdc]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/cdc/meta/replicate_meta.go
[cdcwatch]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/cdc/controller/controller.go
[tso]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/tso/tso.go#L78
[tsokv]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/util/tsoutil/tso.go
[telemetry]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/rootcoord/telemetry/command_store.go
[telemetrymanager]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/internal/rootcoord/telemetry/manager.go#L120
[client]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/pkg/util/etcd/etcd_util.go
[clientinit]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/client/v3/client.go
[credentials]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/client/v3/credentials/credentials.go
[errors]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/api/v3rpc/rpctypes/error.go
[retry]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/client/v3/retry.go
[interceptor]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/client/v3/retry_interceptor.go
[watchclient]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/client/v3/watch.go
[leaseclient]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/client/v3/lease.go
[options]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/client/v3/options.go
[migration]: https://github.com/milvus-io/milvus/blob/658cbd16899bb715a17d4d6f727531a376678ca6/cmd/tools/migration/backend/etcd210.go#L453
