# M0 工程、环境与协议设计

**状态：设计已批准（[PR #2](https://github.com/Reidddddd/etcd-hbase/pull/2)）** · **日期：2026-10-10** · **需求基线：** [DEVELOPMENT_PLAN.md v0.6](https://github.com/Reidddddd/etcd-hbase/blob/56bef2b1248d970cd2fe41f36249d96610913ae4/DEVELOPMENT_PLAN.md)

> 本文是 M0 的设计规格，描述目标、架构、模块契约、数据与身份边界、兼容要求和未决事项。具体任务分解与执行流程由独立实施计划维护。设计获批前不开始 M0 代码实现。

标记约定：

- **事实**：来自需求基线或当前仓库状态。
- **已确认要求**：开发计划已确定、实现必须遵循的边界。
- **提议**：供本轮设计评审确认，评审通过后作为实现依据。
- **待输入**：依赖目标环境或维护者决策，当前不能假设具体值。

## 1. 背景与目标

etcd-hbase 是一个独立部署的 Java 服务，面向 Milvus 使用 etcd v3 gRPC 的调用路径，以 HBase 提供持久化后端。服务需要兼容经过调用分析确认的 etcd 协议和行为；它不解释 Milvus 的业务数据，也不要求公开构建依赖 Milvus 专属仓库或发行物。

M0 建立后续存储语义实现所依赖的工程、运行环境和协议边界。它需要明确：

1. Java 21、多模块 Maven 工程、公开构建和协议生成的基线。
2. gRPC 协议、Go 客户端互通和错误映射的边界。
3. 公开扩展 SPI、内部存储接口和 HBase 专用适配的依赖方向。
4. 逻辑表组、盐配置和历史数据布局候选的选择边界。
5. etcd Authenticate 凭据与 HBase 后端身份的桥接契约。
6. 请求取消、资源释放、配置校验和安全日志的基本运行契约。

M0 的输出是可复现的工程与协议基线、可启动的协议服务骨架、基础 HBase 连接操作验证、公开扩展边界以及足以指导后续实现的设计证据。它不代表完整 etcd KV、Txn、Lease、Watch 或 Auth 语义已交付。

## 2. 现状〔事实〕

截至需求基线 commit 56bef2b1248d970cd2fe41f36249d96610913ae4：

- 仓库已经定义 Java 21、Maven 多模块、固定 etcd v3.5.23 协议基线，以及公开发行版与扩展发行版的分界。
- M0 对应 J00–J09，范围包括调用/API 基线、工程构建、proto 生成、服务骨架、HBase 身份与基础操作、表配置候选、运行配置与请求上下文、Go 驱动、扩展 SPI 和开源规则。
- 仓库尚无 M0 的 Java 服务实现；后续文档中的架构和行为是需求或候选方案，不能写成已验证实现。
- 目标 HBase 版本、目标认证机制、生产盐编码和工作负载数值尚未全部提供。
- HBase 多行事务、服务端 gRPC 骨架和真实 HBase 基础探针属于不同验证范围。任一范围通过都不能推出完整 etcd 语义或生产验收通过。

本文以已确认要求作为实现边界，将候选方案明确标为提议或待输入；不把尚未完成的原型验证描述为事实。

## 3. 设计范围与约束

### 3.1 M0 范围

| 设计面 | M0 需要建立的边界 |
|---|---|
| 工程与发行 | JDK 21、Maven Wrapper/toolchain、模块依赖、公开制品和依赖来源规则 |
| 协议 | etcd v3.5.23 proto 来源与生成契约、descriptor/wire 守卫、Java gRPC 服务骨架 |
| 客户端互通 | 固定 Go etcd client 能调用服务骨架，并识别方法、字节、流终止和错误 |
| HBase 接入 | 固定 Java/HBase client 组合；以真实身份验证连接、Get、Put、CAS、Scan 及失败和关闭行为 |
| 配置与运行 | 端口到表组绑定、配置 schema 校验、请求 deadline/cancel、日志及有限类别指标 |
| 数据配置 | 盐 profile 和表配置的候选模型、预分区/校验规则、空表组初始化 CLI 原型 |
| 扩展 | 明确通用 SPI、core 内部 port 和 HBase 专用 API 的边界 |
| 开源 | 默认构建不依赖私有扩展、目标环境凭据或专用制品源 |

### 3.2 不在 M0 内完成

- 完整 KV/Range/Txn、revision/MVCC、Watch 历史回放、Lease 到期、Compact/GC 和崩溃恢复语义；这些由后续阶段实现并验收。
- 依据单次 HBase 探针宣称多副本、提交接管、身份轮换、长时间运行或生产负载已通过。
- 在没有 B01 输入前冻结目标盐算法、桶数和预分区边界。
- 在没有原型对照结果前选定独立 history 表或 HBase 原生 Cell 多版本作为最终物理布局。
- 将 etcd Authenticate 桥接称为完整复现 etcd 用户、角色和权限管理。
- 通过开发无认证 profile 替代真实目标身份或目标环境验收。

### 3.3 设计约束

1. HBase 是业务持久状态的权威后端；Pod 本地磁盘不承载业务数据。
2. 对外维持 etcd v3 gRPC 协议边界；调用范围和行为以固定 Milvus/Go client 的调用矩阵为准。
3. 一个监听端点绑定一个明确表组；rowkey 不添加额外的 etcd 集群路由 ID。
4. 公共 SPI 不依赖 HBase client、gRPC 实现或 core 内部状态类型。
5. 不支持或尚未实现的业务 RPC 返回明确的协议错误，不构造虚假 KV、revision、token 或成功结果。
6. 目标环境输入缺失时，可验证公开工程能力，但不得将公开候选结果升级为目标环境结论。

## 4. 架构提议〔评审中〕

### 4.1 组件与请求路径

~~~mermaid
flowchart LR
    C[Milvus / Go etcd client] --> G[Java 21 gRPC server]
    G --> P[etcd protocol mapping]
    P --> A[authentication and request validation]
    A --> S[service boundary and RequestScope]
    S --> I[core internal ports]
    I --> H[HBase adapter]
    H --> T[HBase table group]
    A --> E[Header and error mapping]
    E --> G
~~~

请求入口只处理协议转换、身份解析、输入校验、deadline/cancel 传播和资源准入。业务状态语义由 core 定义，HBase 适配器实现内部存储接口。gRPC 层不直接拼接 rowkey 或自行推断持久化提交状态。

M0 服务骨架注册协议服务并建立 Go 客户端可观察的传输行为。尚无业务实现的 handler 返回 UNIMPLEMENTED；将来只有在对应语义真实实现并满足 API 矩阵后，才可返回业务成功响应。

### 4.2 模块职责与依赖

| 模块 | 职责 | 不负责 |
|---|---|---|
| protocol | 固定上游 proto 来源和生成配置，维护生成物/descriptor 校验 | 手工修改生成代码或定义 HBase 数据模型 |
| spi | 对外扩展契约、稳定 DTO、能力标识 | core 提交模型、HBase 类型、gRPC 请求对象 |
| core | 领域模型、内部存储 port、RequestScope、BackendFailure 等内核边界 | 暴露未稳定的提交实现为公共 API |
| hbase | 官方 HBase client 适配、连接与 scanner 生命周期、物理编码候选 | 承担 gRPC 或 etcd 协议映射 |
| grpc | 协议服务、请求验证、鉴权调用、Header/Error 投影及流边界 | 持有 HBase 连接或选择物理 rowkey |
| server | 配置装载、依赖组装、端口绑定和进程生命周期 | 复制协议生成逻辑或业务存储实现 |
| admin | 表组初始化与诊断原型 | 代替 server 生命周期或生产恢复协议 |
| testkit | Go 驱动、原生 etcd 对照和公开契约验证 | 将 fixture 结果当成真实 HBase 证据 |

建议依赖方向：

~~~mermaid
flowchart BT
    core --> spi
    hbase --> core
    hbase --> spi
    grpc --> protocol
    grpc --> core
    grpc --> spi
    admin --> hbase
    server --> grpc
    server --> hbase
    server --> admin
    testkit --> protocol
~~~

生成协议、公共 SPI、core 内部 port 和 HBase 适配分别维护。core 可以依赖 SPI 中的环境扩展契约；HBase 模块实现 core 定义的内部存储契约。HBase 专用连接工厂若需要公开，应留在 HBase 专属 API，不将 HBase 类型引入通用 SPI。

### 4.3 部署与表组隔离

**已确认要求：** 一个 endpoint 静态绑定一个逻辑 HBase 表组。不同 endpoint/table-group 的数据隔离由配置和后端授权共同保证；rowkey 不附加 etcd 集群 ID。

表组是配置和授权边界，不意味着 M0 已实现完整多租户隔离。后续实现还需验证缓存键、连接复用、Watch、后台任务、提交协调和恢复任务都保留相同的表组与身份边界。

服务不把业务持久化放在本地。Pod 替换后能否恢复提交、流和后台任务，取决于后续持久化协议与目标身份的跨副本机制，M0 不以“服务可启动”证明这些属性。

## 5. 协议与兼容设计

### 5.1 协议来源和生成

**基线要求：** 以 Milvus 调用分析所指向的 etcd v3.5.23 proto 作为协议基线。实现前将具体源仓库 commit、文件清单、摘要、protoc、protobuf 和 grpc-java 版本一并锁定。

proto 与其 import 依赖由生成流程维护。生成产物不手工编辑；descriptor/wire 检查覆盖服务路径、方法形态、字段编号、字段类型和流方向。协议版本升级是显式兼容变更，需要重新审计调用矩阵与 Go 客户端行为。

### 5.2 API 能力和响应真实性

API 矩阵负责说明 method、请求选项、响应字段消费者、错误/重试行为、支持阶段和验证方式。M0 固化该矩阵的来源和验证形式，但不把全量 method 的业务实现提前纳入服务骨架。

| 情况 | 对客户端的行为 |
|---|---|
| 方法已实现且验证通过 | 返回从真实后端状态和已实现语义计算出的响应 |
| 方法属于后续阶段、当前未实现 | 返回 UNIMPLEMENTED |
| 请求字段或组合违反已定义契约 | 返回明确的参数/协议错误 |
| 认证失败或表组授权不足 | 失败，不退化为空数据或匿名访问 |
| 后端超时、断连或状态不确定 | 保留可区分的失败分类，不伪装成不存在或成功 |

具体方法支持集以冻结后的 API 矩阵为准。Count 等已接受差异必须明确披露；More、删除状态、revision、事务结果和 Watch 事件不得通过固定值或测试 fixture 掩盖。

### 5.3 Header、错误和流

gRPC 边界负责将内部失败分类映射为客户端可识别的状态和 etcd 响应字段。M0 建立集中式 Header/Error 工厂，错误映射表须通过目标 Go client 和原生 etcd 对照验证；不同失败不得都折叠为 UNKNOWN 或空响应。

至少区分参数错误、未认证、无权限、后端不可用、deadline 到期、客户端取消和未实现方法。具体 gRPC code、details 与 etcd header 字段以 v3.5.23 协议及调用方实际消费为准。

双向流必须传播取消，并在终止时释放 scanner、队列和连接引用。流正常关闭、客户端取消、服务端错误和 deadline 结束对客户端应有可判别结果；M0 验证传输层终止，不代表已实现 Watch 历史或事件语义。

## 6. 存储边界与候选布局

### 6.1 HBase 连接与基础操作

优先使用官方 HBase AsyncConnection/AsyncTable。连接由服务生命周期管理并按后端身份和授权边界复用，不按 RPC 创建；scanner、buffer、重试和队列均须受资源预算约束。

M0 的真实后端探针验证连接、Get、Put、CAS、Scan、认证/权限失败、超时、取消和关闭行为。探针数据使用明确的专用表组或前缀；独立连接读回用于验证后端读写和跨连接可见性，并记录实际表级及 mutation 级 WAL/durability 配置。跨连接读回本身不证明 RegionServer 故障后的持久性；故障耐久性验证留在后续阶段，不纳入 M0 验收。任何本地 mock 或 fixture 都只能验证调用封装和错误路径，不能替代真实 HBase 证据。

目标 HBase、Hadoop 与 client 版本组合仍待 B02 输入和兼容性验证。公开开发环境采用的版本只能作为公开候选组合记录。

### 6.2 盐 profile

所有逻辑记录类别都需要遵循同一套可版本化的盐编码约定；点读、范围扫描、后台枚举和初始化工具必须使用同一 profile。

**提议：** 默认公开候选可使用 CRC32 对完整逻辑标识取模得到桶号，并以固定宽度大端字节编码桶号。profile 记录算法、桶数、盐宽度、编码版本和预分区边界，并提供可复算测试向量。

该候选不等于目标规则。目标算法、桶数、编码、分区键及样例需由 B01 确认。算法或桶数不能在已有表组上静默变更；变更需要独立的数据重编码/迁移设计。

确定性盐 profile 下，精确 key 可计算出唯一桶号并执行单桶点读；点查的额外成本是盐计算。范围查询需要定位并扫描相关桶中的逻辑 key 区间，再合并结果；桶数增加范围查询的跨桶定位和归并成本。点查不应退化为多桶搜索。盐本身不解决应用提交串行化或事务可见性问题。

### 6.3 逻辑记录与物理历史方案

逻辑上需要区分当前值、可按 revision 读取的历史、提交/事件控制记录，以及 Lease/授权等辅助状态。M0 保留以下物理候选，不在设计阶段作未经原型验证的选择：

| 逻辑类别 | 逻辑职责 | 物理映射 |
|---|---|---|
| latest | 当前已提交 KV、逻辑删除及必要的私有候选入口 | 候选为独立当前值表或与其他数据共置 |
| history | 按 key/revision 读取的不可变已提交版本 | 独立 history 表或 HBase Cell 多版本 |
| system | 提交决定、manifest、事件页、恢复/压缩水位等控制记录 | 一个或多个系统表，按访问方式和容量原型决定 |
| lease/auth | Lease 及绑定/到期索引、必要的非敏感身份与授权控制元数据 | 系统记录的逻辑类别；不得保存明文后端密码 |

| 候选 | 需要验证的关键问题 |
|---|---|
| 独立 history 表 | history 与 latest 的写入顺序、点读/区间读取、删除枚举、恢复成本和表组运维 |
| HBase 原生 Cell 多版本 | revision 到 timestamp 的映射、旧 owner 迟到写隔离、删除/重建、版本保留、TTL/compaction 与逻辑 Compact 的相互作用 |

两种方案都必须保留统一的逻辑提交、可见性、压缩边界和读保护语义。HBase 单行 CAS 不提供表组范围的多行事务；具体提交协议属于后续阶段，并需通过故障窗口和并发验证。

**决策状态：** M0 设计确认候选及比较维度；最终物理布局由后续原型与容量/恢复证据决定。未决前，不应把候选 rowkey 或列族清单写成稳定存储格式。

## 7. 身份、安全与隐私

### 7.1 Authenticate 到后端身份

**已确认要求：** Milvus 通过 etcd Authenticate 提交 HBase 账号凭据。服务验证凭据后发放 etcd token，后续 RPC 使用该 token 关联到授权的后端身份和表组。服务 core 中的 `AuthSessionStore` 管理有界 etcd session，负责 token 校验及其到账号、表组授权和后端上下文引用的映射；`BackendIdentityProvider` 负责验证提交的凭据，并取得或恢复相应的 HBase 后端身份上下文，不负责解析或管理 etcd token/session。

~~~mermaid
sequenceDiagram
    participant C as Go etcd client
    participant G as gRPC service
    participant S as AuthSessionStore
    participant I as BackendIdentityProvider
    participant H as HBase
    C->>G: Authenticate(name, password)
    G->>I: authenticate credentials for configured table group
    I->>H: establish and validate backend identity
    H-->>I: authenticated identity and access result
    I-->>G: scoped backend context
    G->>S: create bounded session and token mapping
    S-->>G: etcd token
    G-->>C: etcd token
    C->>G: subsequent RPC with token
    G->>S: validate token and resolve scoped identity
    S-->>G: identity, table-group authorization, context reference
    G->>I: obtain or restore backend identity context
    I-->>G: scoped backend context
~~~

具体接口签名、session 存储方式和上下文交接机制留到实现阶段确定。**待输入：** B03 决定实际 HBase 认证机制、身份激活方式、目标权限和凭据交接约束。不能假设标准 HBase 使用任意 username/password 协议，也不能在 HBase 不可达时发放看似成功的 token。

M0 建立 BackendIdentityProvider 契约并验证目标环境可用时的真实身份访问。token 的跨副本共享、全 Pod 重启后的身份激活、凭据轮换和后台任务身份生命周期需在后续身份/部署设计中完成；单 Pod 内存 session 不能被作为多副本方案。

### 7.2 安全约束

- 认证凭据在传输中受 TLS 保护；密码不能进入 token、业务 KV、提交记录、配置示例或诊断输出。
- 日志、异常、指标标签不包含密码、token、完整 key 或环境专属账号标识。
- 连接复用、缓存和请求合并必须以身份和表组为隔离维度，不能跨未经证明等价的权限边界。
- 表组权限不足、身份失效、认证失败和 HBase 暂时不可用分别保留可诊断错误类别。
- 公开构建和测试只使用占位凭据及公开可访问依赖；目标凭据通过受控机制提供，不写进仓库。
- 本地无认证只允许显式开发 profile，不能默认为发布或目标环境行为。

## 8. 配置、请求生命周期与可观测性

### 8.1 配置与启动

启动配置显式声明 listener、endpoint 到表组的绑定、HBase/client profile、盐 profile、连接与扫描预算及 provider ID。启动前对缺失值、重复绑定、非法端口、未知 profile 和不兼容 schema 进行校验；不合法配置应失败并指出字段，不静默回退到开发配置。

配置文件只携带非敏感标识和受控凭据引用，不嵌入密码。表组初始化 CLI 原型负责校验并安全创建候选表配置；重复运行不得破坏已有表组。正式生产初始化、升级和恢复流程不属于该原型的证明范围。

### 8.2 请求上下文和取消

core 的 RequestScope 承载请求 deadline、取消信号、认证身份、表组和必要的追踪上下文。取消沿 gRPC、异步 client 和 scanner 向下传播；RPC 结束后释放 scanner、临时 buffer、并发配额和连接引用。

对于已经进入持久化发布边界的写操作，客户端取消不能让服务静默遗弃不完整状态。该行为依赖后续提交/恢复协议；M0 只确立接口边界和可验证的资源释放规则，不声称完成事务恢复。

### 8.3 日志与指标

日志关联请求、方法、错误类别、阶段耗时和资源结果，但不记录完整 key、凭据或 token。指标使用有限类别和稳定单位，避免将 key、账号、租户或任意异常文本作为标签。提交等待、队列、活跃流、scanner、连接池等指标需与资源预算对应。

## 9. 开源发行与模块扩展

公开默认发行物可独立构建和启动，不查找扩展仓库、私有包源或目标环境资源。协议与依赖来源、版本、许可证和所需通知可追溯；CI 只使用公开可获取依赖。

**提议：** 扩展由独立工程依赖已发布的公共 SPI 或明确公开的 HBase 专用 API。默认发行物不打包扩展实现。provider 通过显式 ID 选择，启动时装配；缺失、重复或能力不兼容时清晰失败，不依赖发现顺序。

公共 SPI 候选包括 SaltStrategy 与 BackendIdentityProvider，以及实现扩展实际需要的不可变 DTO/能力类型。配置、指标和 endpoint 绑定是否成为公共 SPI，需以真实扩展需求验证后再决定。RequestScope、BackendFailure 和存储/提交 port 留在 core；HBaseConnectionFactory 属于 HBase 专用边界。

许可证当前为 Apache-2.0 候选，需维护者决定后才能视为仓库结论。Artifact 命名和发布位置同样由维护者确认。

## 10. 关键决策与替代方案

| 决策 | 当前设计 | 评审状态 / 理由 |
|---|---|---|
| Java 运行基线 | JDK 21，构建、CI 和运行镜像一致 | 已确认；满足工程统一性要求 |
| etcd 协议 | 固定 v3.5.23 来源与生成器，校验 wire/descriptor | 已确认版本目标；源 commit 和工具摘要随实现基线记录 |
| 集群隔离 | endpoint 绑定独立表组，不加 rowkey 路由 ID | 已确认；避免在业务 key 上引入额外路由语义 |
| HBase 连接 | 官方异步 API 优先，由服务生命周期管理并按身份隔离 | 提议；目标 client 组合待 B02 验证 |
| 历史布局 | 独立 history 与原生 Cell 多版本均保留为候选 | 待原型；不提前绑定物理 schema |
| 盐策略 | 公开可复现候选与目标盐规则分开 | 算法候选可用于开发；目标值待 B01 |
| 认证 | etcd Authenticate 桥接 HBase 身份，不声称等价于 etcd RBAC | 桥接需求已确认；机制待 B03 |
| 未实现 RPC | 返回 UNIMPLEMENTED，不伪造成功 | 已确认；避免 Go 客户端将假状态当作真实服务结果 |
| 通用 SPI 与 HBase API | 通用 SPI 不暴露 HBase/gRPC 类型 | 提议；控制扩展耦合，HBase 专属 API 单独演进 |

### 10.1 被保留的替代方案

**历史数据物理方案：** 独立 history 表较易显式组织提交历史；Cell 多版本可能减少表/收尾操作，但引入 timestamp、删除、TTL、compaction 与逻辑 revision 的耦合。需要用相同提交语义和目标访问成本进行原型比较。

**认证实现：** 不把 Authenticate 当成用户名密码转发协议。BackendIdentityProvider 隔离目标 HBase 认证机制；具体是委派认证、服务代理身份还是受控凭据会话，由 B03 的目标平台约束决定。

**扩展接口范围：** 不把所有内部接口一律公开 SPI。仅稳定、跨发行版确需扩展的能力进入 SPI；内部存储与提交接口保持在 core，HBase 类型留在 HBase API。

## 11. 设计验证与阶段退出条件

验证用于证明设计契约，不以文档存在或编译成功代替行为证据。

| 验证面 | 需要的证据 |
|---|---|
| 工程 | Java 21 下可重复构建；依赖版本和冲突可追溯；公开构建不依赖私有输入 |
| 协议 | 固定 proto/生成器摘要；descriptor 字段、方法和流方向检查；Go client 可识别服务方法与终止状态 |
| 错误 | Go client 对真实支持错误、未实现错误、取消、deadline 和后端失败有可区分结果 |
| 后端 | 使用已确认目标版本和真实身份完成 Get/Put/CAS/Scan；独立读回；认证、权限、超时及关闭证据 |
| 配置 | 非法端口、缺少表组、冲突盐/Schema/provider 等输入明确失败 |
| 生命周期 | 请求取消后 scanner、配额和连接引用释放；凭据不进入日志、指标、仓库或公开报告 |
| 扩展与开源 | 通用 SPI 不引用 HBase/gRPC；无扩展时公开构建和启动成立；许可证/制品决策可追溯 |

M0 阶段只有在 J00–J09 对应交付、适用验证、真实目标身份与 HBase 基础操作证据、公开扩展/构建边界评审均齐备后，才可标记阶段验收通过。缺少目标环境输入时，记录已完成的公开部分和等待项；不得将其表述为目标环境验收完成。即使 M0 通过，也不意味着后续 M1–M8 的 etcd 语义、故障恢复、容量或生产验收通过。

## 12. 已知风险与未决项

### 12.1 外部输入

| 编号 | 未决输入 | 受影响的设计/结论 |
|---|---|---|
| B01 | 目标盐算法、编码、桶数、预分区边界及校验样例 | 目标表 schema、rowkey、range 扫描与初始化 |
| B02 | 目标 HBase/Hadoop/client 版本及允许的 JDK 21 组合 | 依赖锁定、连接与运行兼容 |
| B03 | Authenticate 凭据如何建立目标 HBase 身份、权限、跨副本/后台身份及轮换机制 | BackendIdentityProvider、连接隔离、目标探针 |
| B04 | 提交/读屏障/续租相关验收 profile | 后续一致性与容量目标，不阻塞基础工程 |
| B05 | KV、value、版本、请求并发和目标集群规模 | 容量预算及后续负载设计 |
| B06 | TLS、凭据交接、部署监控、RPO/RTO 和测试环境规则 | 身份生命周期、部署和运维验收 |
| I07 | 维护者确认开源许可证和来源/NOTICE 要求 | LICENSE、NOTICE 与发布要求 |
| I08 | Artifact 命名和公开发布位置 | Maven 坐标和发布流程 |

### 12.2 风险

1. **目标身份无法跨副本使用：** 单 Pod session 不能支撑多副本或重启恢复。需由后续身份生命周期方案证明，而非依赖粘性会话。
2. **表布局过早冻结：** 盐、历史保留和提交记录相互影响；缺少目标输入和原型会导致不兼容的数据格式。
3. **把 HBase 操作成功等同于 etcd 兼容：** Get/Put/CAS/Scan 只证明基础客户端访问，不证明 revision、事务、Lease 或 Watch 语义。
4. **未实现 RPC 的静默成功：** 空响应或虚构 Header 会误导 Go client；必须保留明确失败行为。
5. **请求取消与持久化边界脱节：** 后续事务发布协议需将客户端等待与服务恢复生命周期分离。
6. **默认发行依赖私有环境：** 任何私有制品、凭据或扩展工程隐式进入公共构建都会破坏开源边界。

## 13. 与实施计划的边界

本文定义 M0 的技术约束和需要验证的设计契约。具体任务拆分、逐步执行过程和各交付切片的验证记录由独立实施计划维护；实施时应按架构依赖安排顺序，并将每项设计决策对应到验证证据。

实现阶段不得将本文标注为待输入的事项静默改写为已确认值。

## 14. 变更记录

| 日期 | 变更 |
|---|---|
| 2026-10-10 | 初稿：从 M0 执行清单中提取设计目标、架构契约、候选决策、边界和未决输入，提交评审。 |
| 2026-10-12 | 按评审澄清 HBase 探针的可见性与 durability 边界、确定性盐的点查/范围查成本，以及 etcd session 与后端身份 provider 的职责。 |
| 2026-10-12 | PR #2 已批准并合并；更新设计状态，M0 的实现与阶段验收仍按独立任务证据判断。 |

## 15. 参考

- [仓库开发计划与 M0 基线](https://github.com/Reidddddd/etcd-hbase/blob/56bef2b1248d970cd2fe41f36249d96610913ae4/DEVELOPMENT_PLAN.md)
- [etcd v3.5 API](https://etcd.io/docs/v3.5/learning/api/)
- [Apache HBase 基础前置条件与 Java 支持](https://hbase.apache.org/docs/configuration/basic-prerequisites/)
- [Apache HBase Durability API](https://hbase.apache.org/2.6/devapidocs/org/apache/hadoop/hbase/client/Durability.html)
