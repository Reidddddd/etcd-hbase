# 首版 API 与验证矩阵（J00.2）

状态：待评审；M0 服务端业务 handler 尚未实现。WP20 已按[原生场景清单](../testkit/scenarios/m0-native.yaml)在两个独立的 etcd v3.5.23 嵌入实例上运行 13 个步骤，并完成 trace 对照和损坏样本拒绝检查。这只是 testkit 的原生基线验证，不代表 Java 服务行为或 C01–C33 完整验收通过。版本、部署范围及调用证据见 [Milvus 调用基线](milvus-etcd-usage.md)。

本矩阵落实开发计划第 4 节的完整首版范围。直接调用、client 自动调用及需求承诺分别标注；未观察到业务调用不会移除已承诺功能。原已明确不支持的能力验收真实拒绝行为。M0 骨架尚未实现的 handler 返回 UNIMPLEMENTED，不能把本文的“首版支持”视为当前已支持。

## 完整 gRPC 方法清单

下表覆盖固定 etcd v3.5.23 `rpc.proto` descriptor 中的全部服务方法，并显式列出该版本服务端 Lock/Election proto 的额外方法。请求/响应完整字段名、字段号及流方向由 `testkit/cases/m0-wire.json` 对照 `protocol/baseline/wire-contract.json` 校验。`first-release` 表示目标首版契约；`M0` 当前骨架对所有未实现方法都返回 `UNIMPLEMENTED`。

| RPC method | Request → response | Stream shape | First-release | usage_id | Tasks | Cases | M0 behavior / rejection |
|---|---|---|---|---|---|---|---|
| `etcdserverpb.Auth/AuthDisable` | `etcdserverpb.AuthDisableRequest` → `etcdserverpb.AuthDisableResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/AuthEnable` | `etcdserverpb.AuthEnableRequest` → `etcdserverpb.AuthEnableResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/AuthStatus` | `etcdserverpb.AuthStatusRequest` → `etcdserverpb.AuthStatusResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/Authenticate` | `etcdserverpb.AuthenticateRequest` → `etcdserverpb.AuthenticateResponse` | unary | supported | U09, U16 | J04, J50, J51 | C29, C30 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `etcdserverpb.Auth/RoleAdd` | `etcdserverpb.AuthRoleAddRequest` → `etcdserverpb.AuthRoleAddResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/RoleDelete` | `etcdserverpb.AuthRoleDeleteRequest` → `etcdserverpb.AuthRoleDeleteResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/RoleGet` | `etcdserverpb.AuthRoleGetRequest` → `etcdserverpb.AuthRoleGetResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/RoleGrantPermission` | `etcdserverpb.AuthRoleGrantPermissionRequest` → `etcdserverpb.AuthRoleGrantPermissionResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/RoleList` | `etcdserverpb.AuthRoleListRequest` → `etcdserverpb.AuthRoleListResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/RoleRevokePermission` | `etcdserverpb.AuthRoleRevokePermissionRequest` → `etcdserverpb.AuthRoleRevokePermissionResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/UserAdd` | `etcdserverpb.AuthUserAddRequest` → `etcdserverpb.AuthUserAddResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/UserChangePassword` | `etcdserverpb.AuthUserChangePasswordRequest` → `etcdserverpb.AuthUserChangePasswordResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/UserDelete` | `etcdserverpb.AuthUserDeleteRequest` → `etcdserverpb.AuthUserDeleteResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/UserGet` | `etcdserverpb.AuthUserGetRequest` → `etcdserverpb.AuthUserGetResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/UserGrantRole` | `etcdserverpb.AuthUserGrantRoleRequest` → `etcdserverpb.AuthUserGrantRoleResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/UserList` | `etcdserverpb.AuthUserListRequest` → `etcdserverpb.AuthUserListResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Auth/UserRevokeRole` | `etcdserverpb.AuthUserRevokeRoleRequest` → `etcdserverpb.AuthUserRevokeRoleResponse` | unary | conditional | — | J51 | C31 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED until B03-backed auth mapping and its permission behavior are verified; no anonymous fallback |
| `etcdserverpb.Cluster/MemberAdd` | `etcdserverpb.MemberAddRequest` → `etcdserverpb.MemberAddResponse` | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED in the first release |
| `etcdserverpb.Cluster/MemberList` | `etcdserverpb.MemberListRequest` → `etcdserverpb.MemberListResponse` | unary | supported | U16 | J54, J67 | C28 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `etcdserverpb.Cluster/MemberPromote` | `etcdserverpb.MemberPromoteRequest` → `etcdserverpb.MemberPromoteResponse` | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED in the first release |
| `etcdserverpb.Cluster/MemberRemove` | `etcdserverpb.MemberRemoveRequest` → `etcdserverpb.MemberRemoveResponse` | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED in the first release |
| `etcdserverpb.Cluster/MemberUpdate` | `etcdserverpb.MemberUpdateRequest` → `etcdserverpb.MemberUpdateResponse` | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED in the first release |
| `etcdserverpb.KV/Compact` | `etcdserverpb.CompactionRequest` → `etcdserverpb.CompactionResponse` | unary | supported | — | J52 | C14 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `etcdserverpb.KV/DeleteRange` | `etcdserverpb.DeleteRangeRequest` → `etcdserverpb.DeleteRangeResponse` | unary | supported | U04, U11, U15 | J33, J43, J44 | C09, C10, C18, C27 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `etcdserverpb.KV/Put` | `etcdserverpb.PutRequest` → `etcdserverpb.PutResponse` | unary | supported | U04, U05, U12, U14, U15 | J30, J41, J66 | C01, C07, C08, C16, C24, C26, C27 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `etcdserverpb.KV/Range` | `etcdserverpb.RangeRequest` → `etcdserverpb.RangeResponse` | unary | supported | U01, U02, U03, U05, U07, U08, U09, U10, U13, U14, U15, U16 | J14, J15, J25, J34, J35, J36, J63 | C01, C02, C03, C04, C05, C06, C23, C26, C27, C28 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `etcdserverpb.KV/Txn` | `etcdserverpb.TxnRequest` → `etcdserverpb.TxnResponse` | unary | supported | U04, U05, U12, U13 | J31, J32, J41, J43, J44 | C11, C12, C13, C16, C24, C25 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `etcdserverpb.Lease/LeaseGrant` | `etcdserverpb.LeaseGrantRequest` → `etcdserverpb.LeaseGrantResponse` | unary | supported | U05 | J40 | C08, C15, C16 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `etcdserverpb.Lease/LeaseKeepAlive` | `etcdserverpb.LeaseKeepAliveRequest` → `etcdserverpb.LeaseKeepAliveResponse` | bidirectional-streaming | supported | U06 | J42 | C17 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `etcdserverpb.Lease/LeaseLeases` | `etcdserverpb.LeaseLeasesRequest` → `etcdserverpb.LeaseLeasesResponse` | unary | supported | — | J40 | C15 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `etcdserverpb.Lease/LeaseRevoke` | `etcdserverpb.LeaseRevokeRequest` → `etcdserverpb.LeaseRevokeResponse` | unary | supported | U06 | J41, J43 | C18 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `etcdserverpb.Lease/LeaseTimeToLive` | `etcdserverpb.LeaseTimeToLiveRequest` → `etcdserverpb.LeaseTimeToLiveResponse` | unary | supported | U06 | J40 | C15, C18 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `etcdserverpb.Maintenance/Alarm` | `etcdserverpb.AlarmRequest` → `etcdserverpb.AlarmResponse` | unary | mixed | U16 | J54 | C28, C32 | M0 skeleton: UNIMPLEMENTED. Alarm GET is a future supported query; ACTIVATE and DEACTIVATE return UNIMPLEMENTED unless explicitly mapped |
| `etcdserverpb.Maintenance/Defragment` | `etcdserverpb.DefragmentRequest` → `etcdserverpb.DefragmentResponse` | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED in the first release |
| `etcdserverpb.Maintenance/Downgrade` | `etcdserverpb.DowngradeRequest` → `etcdserverpb.DowngradeResponse` | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED in the first release |
| `etcdserverpb.Maintenance/Hash` | `etcdserverpb.HashRequest` → `etcdserverpb.HashResponse` | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED in the first release |
| `etcdserverpb.Maintenance/HashKV` | `etcdserverpb.HashKVRequest` → `etcdserverpb.HashKVResponse` | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED in the first release |
| `etcdserverpb.Maintenance/MoveLeader` | `etcdserverpb.MoveLeaderRequest` → `etcdserverpb.MoveLeaderResponse` | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED in the first release |
| `etcdserverpb.Maintenance/Snapshot` | `etcdserverpb.SnapshotRequest` → `etcdserverpb.SnapshotResponse` | server-streaming | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED in the first release |
| `etcdserverpb.Maintenance/Status` | `etcdserverpb.StatusRequest` → `etcdserverpb.StatusResponse` | unary | supported | — | J54, J67 | C32 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `etcdserverpb.Watch/Watch` | `etcdserverpb.WatchRequest` → `etcdserverpb.WatchResponse` | bidirectional-streaming | supported | U07, U08, U13 | J44, J45, J46, J47 | C19, C20, C21, C22 | M0 skeleton: UNIMPLEMENTED. Return the etcd v3.5.23-defined error/status for invalid input, missing state, deadline, cancellation, and backend failure; M0 skeleton returns UNIMPLEMENTED |
| `v3lockpb.Lock/Lock` | external proto | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED; the public v3 API descriptor does not include this server-side service |
| `v3lockpb.Lock/Unlock` | external proto | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED; the public v3 API descriptor does not include this server-side service |
| `v3electionpb.Election/Campaign` | external proto | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED; the public v3 API descriptor does not include this server-side service |
| `v3electionpb.Election/Proclaim` | external proto | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED; the public v3 API descriptor does not include this server-side service |
| `v3electionpb.Election/Leader` | external proto | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED; the public v3 API descriptor does not include this server-side service |
| `v3electionpb.Election/Observe` | external proto | bidirectional-streaming | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED; the public v3 API descriptor does not include this server-side service |
| `v3electionpb.Election/Resign` | external proto | unary | unsupported | — | J54 | C33 | M0 skeleton: UNIMPLEMENTED. UNIMPLEMENTED; the public v3 API descriptor does not include this server-side service |

The seven external proto methods are fixed to the etcd v3.5.23 source revision but are intentionally not part of the vendored public v3 API descriptor: `Lock/Lock`, `Lock/Unlock`, `Election/Campaign`, `Election/Proclaim`, `Election/Leader`, `Election/Observe`, and `Election/Resign`. Their exact package paths and source paths are recorded in `testkit/cases/m0-wire.json`; they are unsupported and return `UNIMPLEMENTED`.
## 共用协议契约

协议为 etcd v3.5.23 的 [rpc.proto][proto]，KV 字段为 [kv.proto][kvproto]，完整 method 路径和流形态记录在 [源码基线](baselines/j00-source-baseline.json)。

- key/value 是原始 bytes，按无符号字节序比较，不以 UTF-8、目录树或 Milvus 业务模型解释；覆盖零字节、0xff、长 key 和尾斜杠。
- `range_end` 空表示精确 key；普通区间为 `[key, range_end)`；`range_end=0x00` 表示 FromKey；key/range_end 同为 0x00 表示全部 key。Prefix 转成该字节前缀的上界，`/root` 可以匹配 `/root2`。
- ResponseHeader 的 cluster/member 身份、revision 和适用字段来自本服务真实状态。Raft 专属字段的投影规则由 J03/J54 记录，不能用固定值伪装原生成员或共识状态。
- CreateRevision/ModRevision/Version/Lease 保持 etcd 元数据语义；一个修改事务使用一个 KV revision；失败 compare、纯读取和纯续租不制造 KV 修改 revision。
- 所有桶在一个有效 ReadView 中完成一次 Range。多次独立 Range 没有自动共享快照，显式 revision 才选择同一逻辑历史。
- 明确参数错误、认证/授权、资源预算、取消、deadline、后端失败和提交结果未知；失败不投影为空列表或成功。响应超预算不静默截断。

## KV / Range / Txn

以下证据 Uxx 对应调用基线中的入口。Cxx 对应下文验收用例定义，均待 J07 及对应实现任务执行。

| ID / wire RPC | 首版请求组合与支持边界 | 必须真实的响应与错误 | 调用依据 | 实现任务 / 用例 |
|---|---|---|---|---|
| R01 `/etcdserverpb.KV/Range` | 精确 key，revision<=0 当前默认一致读；空 key 的非法形态按原生拒绝 | Kvs 0/1、原始字节和全部 KV 元数据、Header；成功不存在与失败分开 | U01/U05/U14/U16 | J25/J34；C01/C26 |
| R02 Range | Prefix / 普通区间；无 Limit 返回完整匹配结果；key 升序 | Count=len(Kvs)，不得隐藏下层后代；超预算明确失败 | U01/U07/U08/U09/U10/U13/U15 | J14/J34；C02/C27 |
| R03 Range | Limit>0、全局 key 排序，NONE/ASCEND 为 key 升序，DESCEND 为 key 降序；跨桶归并 | 本页 Count=len(Kvs)；More 真值来自下一条可见匹配记录；不能每桶默认预读 L 条 | U02；KEY+ASCEND 的 Go WithSort 实际转换成 NONE | J14/J35；C03 |
| R04 Range | 精确 key + count_only=true | Kvs 为空，Count 为真实 0/1 | U03/U05 | J15；C04 |
| R05 Range | 范围/Prefix + count_only=true + limit=1 | Kvs 为空，Count 为真实存在性 0/1，More=false；计数差异仅按下表豁免 | U03 | J15；C05 |
| R06 Range | revision>0 历史读取；keys_only；min/max_mod_revision 和 min/max_create_revision，边界 inclusive | 按请求视图选择版本，再过滤、排序和限制；KeysOnly 清空 value，保留元数据；压缩/未来 revision 正确失败 | 历史/过滤/KeysOnly 是需求承诺；U01 使用版本字段 | J25/J34；C06 |
| R07 Range | serializable=true 接受；默认请求仍按一致读处理 | Serializable 可以提供更强一致性，不得放松默认一致读；写后读取覆盖已成功写入 | U08/U09 | J25/J36/J63；C23 |
| R08 Range | FromKey 和全 key 空间哨兵；不在服务端添加 root 白名单 | 任意原始 key，包括全局 Telemetry/health；预算和历史规则相同 | FromKey 属需求承诺；U15/U16 要求全局 key | J34；C02/C27 |
| P01 `/etcdserverpb.KV/Put` | 原始 key/value、lease=0 或有效 Lease、prev_kv；创建/更新/删除后重建 | Header、适用 PrevKV 和元数据，绑定/换绑与写入原子；未知 Lease 返回原生可识别错误 | U04/U05/U12/U14/U15 | J30/J41；C07/C08/C16 |
| D01 `/etcdserverpb.KV/DeleteRange` | 精确 key、prev_kv | deleted 真实、PrevKvs 为实际旧值、DELETE 事件与状态同提交；不存在 key不制造假事件 | U04/U11/U15 | J33/J44；C09 |
| D02 DeleteRange | Prefix / 普通区间 / FromKey、prev_kv；有界准备但统一对外发布 | deleted 真实，不能返回部分删除成功或把分页准备当多个独立事务 | U04/U11；FromKey 属需求承诺 | J33/J43；C10 |
| T01 `/etcdserverpb.KV/Txn` | Compare conjunction、Then/Else、读写/删除 RequestOp、读分支和混合操作；请求内顺序按原生对照 | Succeeded、Responses 顺序和类型、Header；修改分支一次 revision，纯读无修改；CAS false 不是后端故障 | U04/U05/U12/U13 | J31/J32；C11 |
| T02 Txn | VERSION/VALUE/MOD/CREATE/LEASE compare、EQUAL/NOT_EQUAL/GREATER/LESS、精确/范围 compare | 元数据和字节比较真实；缺失 key、空范围、边界和类型校验按原生；选中 Else 写分支仍正常提交 | U04/U05/U13 使用 VERSION/VALUE/MOD 的等值；其余是契约覆盖 | J31/J32；C12 |
| T03 Txn | nested Txn、重复或重叠操作接受/拒绝组合 | 不因某组合实现困难静默改操作顺序或丢弃操作；原生拒绝的组合返回对应错误，接受的组合保持原子可见性 | 需求承诺；本轮未观察生产路径直接 OpTxn/Else | J32；C13 |
| K01 `/etcdserverpb.KV/Compact` | revision、logical/physical 请求边界 | 真实逻辑压缩水位；活跃读、当前值和 Watch 保留规则成立；physical 不冒充完成未实现的回收 | 需求承诺，非业务直接调用 | J52；C14 |

Range 的非 KEY 排序属于 J35 要逐项说明支持或明确拒绝的边界；首版不能静默用 key 排序代替 VERSION/CREATE/MOD/VALUE。Put 的 ignore_value/ignore_lease 非默认组合不属于已确定的调用组合，J30 必须明确支持或返回 UNIMPLEMENTED，不得忽略标志并普通写入。这些边界的最终选择在对应实现评审记录，不据此削减上表已承诺能力。

## Count 已接受差异

| 请求 | 首版 Count | More / 结果要求 |
|---|---|---|
| 精确 Get | len(Kvs)，0 或 1 | 真实匹配 |
| 无 Limit 普通 Range | len(Kvs) | 完整结果，不能返回半份 |
| 有 Limit 普通 Range | 本页 len(Kvs)，不是全范围总数 | More 真实；L+1 可见匹配用于判定 |
| 精确 CountOnly | 真实 0/1，Kvs 空 | 成功不存在与读取失败分开 |
| 范围 CountOnly + Limit=1 | 真实存在性 0/1，Kvs 空 | More=false；不以随机数或缓存猜测存在性 |
| 其他范围 CountOnly | 明确 UNIMPLEMENTED | 不发起准确全范围计数 |

范围 CountOnly+Limit=1 可能与原生总数不同；只豁免这里的 Count，不豁免 More、revision、错误或任何其他字段。原生 [MVCC Count 路径][mvcc-count] 返回空 KVs，[Range 组装][range-response] 仅在返回 KVs 超过 Limit 时设置 More，因此本组合 More=false，即使匹配数大于一。C05 仍需执行运行对照；不能用普通 Range 的额外记录规则要求 CountOnly 再扫描第二条。消费者与迁移工具的区别见调用基线的 Count 审计。

## Lease / Watch

| ID / wire RPC | 请求与首版契约 | 响应、错误与联动 | 依据 | 实现任务 / 用例 |
|---|---|---|---|---|
| L01 `/etcdserverpb.Lease/LeaseGrant` | TTL、自动或显式 ID；碰撞、过大/非法 TTL 按原生对照 | ID/TTL/Header/Error 真实；状态可恢复；已经接受的绑定规模必须可撤销 | U05；显式 ID 属协议覆盖 | J40；C15/C16 |
| L02 `/etcdserverpb.Lease/LeaseKeepAlive` | 双向流、多 Lease ID；权威续租状态持久化后回复 | ID/TTL 真实；纯续租无 KV revision/PUT 事件；失败重连与客户端 TTL 解释兼容 | U06，client 按 TTL/3 安排续租 | J42；C17 |
| L03 `/etcdserverpb.Lease/LeaseRevoke` | 按 ID 撤销；到期走相同删除语义 | 失效状态、绑定删除与 Watch 原子关联；A换绑B/重建不被旧索引误删 | U06 | J41/J43；C18 |
| L04 `/etcdserverpb.Lease/LeaseTimeToLive` | ID，keys=false/true | TTL、grantedTTL、keys/Header 真实；不存在、到期与权限失败按原生区分 | U06 使用 keys=false；keys=true 是已承诺管理范围 | J40；C15/C18 |
| L05 `/etcdserverpb.Lease/LeaseLeases` | 列举有效 Lease，预算明确 | 真实 leases/Header，超预算失败而不截断伪成功 | 需求承诺 | J40；C15 |
| W01 `/etcdserverpb.Watch/Watch` | 双向流 create/cancel/progress；精确/范围/Prefix/FromKey；多 watchId | Created、Canceled、watchId、Header、events/type/KV 真实；重复 ID 与取消行为对照原生 | U07/U08/U13；进度与多 ID 属协议契约 | J44/J45；C19/C21 |
| W02 Watch | inclusive start_revision、0/未指定为新监听、prev_kv | DELETE PrevKV 真实；历史到实时交接不漏/重复；压缩取消包括 compact_revision/cancel_reason | U07/U08/U13；不能统一替调用方 R 加一 | J45/J46/J47；C20/C22 |
| W03 Watch | progress_notify、显式 progress request、NOPUT/NODELETE、fragment | progress 不超前于已交付事件；不启用 fragment 时整 revision，启用时按 client 重组/重连边界验证 | 需求承诺，client 对 fragment 有重组逻辑 | J46/J47；C21/C22 |

CreatedNotify 是 Go client 对创建响应的交付选项，不是 WatchCreateRequest 的同名 wire 字段。服务必须发真实 created 响应，即使用户没有请求在 WatchChan 中观察它，client 仍用它建立 watchId。

## Auth / 运维 / 明确不支持项

| ID / wire RPC | 首版结果要求 | 依据及请求字段 | 实现任务 / 用例 |
|---|---|---|---|
| A01 `/etcdserverpb.Auth/Authenticate` | 验证实际后端身份/表组权限后返回 token；失败不能发 token；core 管理 session，provider 提供后端身份 | U09/U16 的 client 工厂经 client.getToken 自动调用；name/password，后续 metadata token | J04/J50/J51；C29/C30 |
| A02 `/etcdserverpb.Auth/*` 管理方法（清单如下） | 每个 RPC 交付真实后端映射或明确 UNIMPLEMENTED；B03 尚待输入，不能声称已确定可映射能力或完整原生 RBAC | 首版管理矩阵要求；本轮未发现 Kafka 主路径直接调用这些管理 RPC | J51；C31 |
| O01 `/etcdserverpb.Cluster/MemberList` | Header 和成员 ID/ClientURLs 对应真实可达服务端点；ClientURLs 不为空，表组隔离有效 | U16；client AutoSync 或用户显式 Sync 也可发出；linearizable 字段行为对照 | J54/J67；C28 |
| O02 `/etcdserverpb.Maintenance/Alarm` | GET/memberID/alarm 查询返回真实可解释状态；ACTIVATE/DEACTIVATE 的支持或拒绝明确，不冒充 Raft alarm 管理 | U16 的 AlarmList 映射到 Alarm GET | J54；C28/C32 |
| O03 `/etcdserverpb.Maintenance/Status` | Header/version/dbSize/leader/Raft专属字段/errors/dbSizeInUse/isLearner 等字段逐项有真实来源或明确兼容说明；不能简单返回零值健康状态 | 需求承诺、client 库/etcdctl 入口；本轮未发现直接业务调用 | J54/J67；C32 |
| X01 Cluster 成员修改 | MemberAdd/MemberRemove/MemberUpdate/MemberPromote 明确 UNIMPLEMENTED | 原已不支持，不用 HBase 表操作伪造成员修改 | J54；C33 |
| X02 Maintenance 原生管理 | Defragment/Hash/HashKV/Snapshot/MoveLeader/Downgrade 明确 UNIMPLEMENTED | 原生 Snapshot 不等于本服务的 admin 备份 | J54/J57；C33 |
| X03 Lock / Election 服务 | Lock/Unlock、Campaign/Proclaim/Leader/Observe/Resign 明确 UNIMPLEMENTED | 原生专用服务不等于 Milvus 用 Txn/Lease/Watch 自行实现的主备协调 | J54；C33 |

A02 管理方法完整清单：AuthEnable、AuthDisable、AuthStatus、UserAdd、UserGet、UserList、UserDelete、UserChangePassword、UserGrantRole、UserRevokeRole、RoleAdd、RoleGet、RoleList、RoleDelete、RoleGrantPermission、RoleRevokePermission。其 protobuf 请求/响应字段随固定 descriptor 校验；映射范围和实际后端权限在 J51 附逐方法报告。本轮保留这一完整验收责任，不虚构目标环境能力。

## 错误与重试验收

下表的 wire 描述以固定 [rpctypes/error.go][errors] 为准，实施时复制对应协议定义或建立可验证的投影，不能仅返回相同 gRPC code 的任意描述。

| ID | 情况 | gRPC / Go client 结果 | 消费者后果与任务 |
|---|---|---|---|
| E01 | HBase 不可用/超时/重试耗尽 | Unavailable 或实际 deadline 分类，保留来源；不是空 Kvs | U14 TSO 吞错仍需 J66 处理；J03/J07/J66/J73 |
| E02 | 成功点读不存在 | Range 正常成功、Count=0；Milvus Load 转 key-not-found | 不用失败模拟首次初始化；J25/J34 |
| E03 | 已压缩/未来 revision | OutOfRange + ErrCompacted/ErrFutureRev 对应描述；Watch 使用取消响应字段 | Session/Proxy 重读列表与监听；J25/J47/J52 |
| E04 | Lease 不存在/失效 | 根据 RPC 返回 TTL、错误或流响应，对照 ErrLeaseNotFound；不能所有方法统一 NotFound | Milvus checkKeepaliveTTL 可能退出进程；J40/J42/J43 |
| E05 | 无效/过期 token | Unauthenticated + ErrInvalidAuthToken；AuthOldRevision/UserEmpty 按实际原因 | client 刷新及 Milvus rewatch；J50/J51/J62 |
| E06 | 凭据认证失败 / 权限不足 | Authenticate 原生 ErrAuthFailed 的 InvalidArgument；权限 ErrPermissionDenied 的 PermissionDenied | 认证失败不发 token，权限错误不匿名回退；J04/J50/J51 |
| E07 | 参数/重复 key/预算/未知方法 | InvalidArgument 对应原生校验；资源超限明确 ResourceExhausted；未支持 UNIMPLEMENTED | 原生 request-too-large 与服务资源预算分别验证；J03/J13/J32/J54 |
| E08 | 取消/deadline/CAS 结果未知 | Canceled/DeadlineExceeded 或实际投影；发布后服务继续恢复，内部查询决定，不盲目重新发布 | Go client 对 mutable RPC 重试受限；J06/J23/J27 |

## 验收用例定义（33 项完整验收均未完成）

J07 建立请求/结果驱动；对应功能阶段执行协议对照；M6 再执行真实消费者。WP20 原生清单只覆盖列出的操作子集，各 C01–C33 仍需完成其表内完整场景，且 Java 服务等价对照尚未开始。每个结果记录固定版本、请求 bytes/参数、Kvs/元数据/Header、错误描述和客户端最终行为。只允许已接受 Count 差异、标识映射及已明确的后端管理映射，不用归一化掩盖顺序、遗漏或版本错误。

| 用例 | 请求场景与判据 | 首次完整行为验收 |
|---|---|---|
| C01 | 空/存在/更新 key、bytes、0xff/零字节、不存在与后端失败；准确元数据 | M3 |
| C02 | 深层 Prefix、相邻前缀、FromKey/全 key 范围、空桶和无 Limit 完整响应；跨桶相同 ReadView | M3 |
| C03 | 升/降序 L=1、边界页、L+1、墓碑/过滤、下一页起点；本页 Count、More 正确，不默认 N×L 预取 | M1/M3 |
| C04 | 精确 CountOnly 的存在/不存在、Kvs 空、失败不为零 | M1/M3 |
| C05 | Prefix CountOnly+Limit=1 的 0/1/多个匹配；Count 豁免但单独对照 More；其他范围 CountOnly 明确拒绝 | M1/M3 |
| C06 | 历史读取、删除/重建、KeysOnly、四项 revision 过滤边界、压缩/未来 revision、预算错误 | M3/M5 |
| C07 | Put 创建/更新/PrevKV；Version/Create/ModRevision；ignore 标志的明示支持或拒绝 | M3 |
| C08 | 有效/未知 Lease、lease=0、A→B 换绑；原子可见性 | M4 |
| C09 | 点删/不存在点删、真实 deleted/PrevKvs、重复删除与真实事件 | M3/M4 |
| C10 | 跨桶区间删除、大计划、准备失败与取消，不能部分成功 | M3/M4 |
| C11 | 多 key Compare、Then/Else、读取/修改混合、纯读、失败分支、一次 revision、响应顺序 | M2/M3 |
| C12 | 五种 compare 目标/四种结果、缺失 key、空范围和边界；CDC 迟到删除不删新版本 | M3/M6 |
| C13 | nested/重复/重叠读写操作组合，原生接受或拒绝及原子可见性 | M3 |
| C14 | Compact 边界、physical 请求、活跃读/Watcher/GC 竞争、真实压缩错误 | M5 |
| C15 | Grant 自动/显式 ID、TTL、碰撞、TTL keys、LeaseLeases、到期/不存在 | M4 |
| C16 | 主体/三个角色同 Lease 与不带 Lease 的 version key 单事务，CAS false/部分准备失败 | M4/M6 |
| C17 | KeepAlive 多 ID、TTL/3、已承诺续租后的切换和重连；不增加 KV revision/事件 | M4 |
| C18 | 到期/Revoke 与续租/换绑/重建竞争；TTL<=0/lease sentinel 的真实消费者 | M4/M6 |
| C19 | Watch 创建/cancel、多个 ID、重复 ID、精确/Prefix/FromKey；client 识别 created | M4 |
| C20 | Get R→Watch R+1 与直接 Watch R 均按 inclusive 请求执行，交接期间写入无遗漏 | M4/M6 |
| C21 | progress、filter、完整 revision/fragment、慢消费、断流/取消/重连与客户端重组 | M4 |
| C22 | DELETE PrevKV、compact_revision、token 错误→列表重建/rewatch；无幽灵 session | M4/M6 |
| C23 | 5 秒 Serializable 配置轮询与线性化写后刷新，错误不清空配置，身份/视图缓存隔离 | M6 |
| C24 | 子元数据写成功→salvage checkpoint→consume checkpoint，逐步失败/重启仍保序 | M6 |
| C25 | CDC 列表/事件/revision、MOD 条件删除和重复/迟到任务 | M6 |
| C26 | TSO/ID 高水位读取后端失败的真实错误链；修复调用方或验证隔离方案后证明不回退初始化 | M6 |
| C27 | 全局 Telemetry 前缀和 point key、root 前缀外 key；失败消费与 best-effort 删除记录 | M6 |
| C28 | MemberList 非空可达 ClientURLs→health Get→Alarm GET，全副本及失败路径 | M5/M6 |
| C29 | Authenticate 实际身份/权限/TLS→token→后续 RPC，密码不入输出 | M5 |
| C30 | A 登录 B 请求、全 Pod 重建、token 刷新/后端身份失效/轮换及后台任务接管 | M5/M7 |
| C31 | 每项 Auth 管理 RPC 的真实映射或明确拒绝，输入字段与错误客户端可识别 | M5 |
| C32 | Status 与 Alarm 每字段真实来源/投影、适用预算及负向请求 | M5/M6 |
| C33 | 全部已明确不支持的 Cluster/Maintenance/Lock/Election 方法，无伪成功；M0 未实现方法亦明确失败 | M0/M5 |

[proto]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/api/etcdserverpb/rpc.proto
[kvproto]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/api/mvccpb/kv.proto
[errors]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/api/v3rpc/rpctypes/error.go
[mvcc-count]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/server/mvcc/kvstore_txn.go#L139
[range-response]: https://github.com/etcd-io/etcd/blob/1cf4bc40111fd95d1e14a90a09a146357b15eb13/server/etcdserver/apply.go#L412
