# M0 公共 SPI 契约

本文定义 M0 中 `spi` 模块对扩展实现公开的最小契约。SPI 只包含与具体存储和传输无关的类型，不承担 etcd API、HBase 表布局或 gRPC 服务语义。

## 模块边界

- `spi` 的生产源码只依赖 Java 标准库；不得引用 HBase、Hadoop、gRPC、Protobuf、`core` 或 `server`。测试依赖保持 test scope。
- HBase 连接工厂和 HBase 专属 DTO 属于 `hbase` 模块。通用 SPI 不暴露 HBase 类型。
- `core` 负责请求范围、后端错误及提交/存储端口；`grpc` 负责协议绑定和错误映射。
- provider 通过配置中的稳定 ID 显式选择。启动装配应检测缺失、重复和未知 ID，不依赖服务发现顺序。

## 盐策略

`SaltStrategy` 暴露稳定 `id()`、不可变 `SaltProfile`、`salt(byte[])` 和 `splitKeys()`。实现对完整逻辑标识符计算固定前缀，不得修改输入数组，并返回调用方拥有的新数组。点查、范围扫描和分区边界必须使用同一 profile。

`HashSaltStrategy` 的公开候选 `crc32-bucket-v1` 计算完整标识符的 CRC32 值，对 16 个 bucket 取模，并以 2 字节大端无符号整数编码桶号。它输出桶 `1..15` 的有序 split keys；固定测试向量见 [`m0-salt-profile.md`](../storage/m0-salt-profile.md)。版本 1 的布局属于持久化格式；任何改变输入范围、取模或编码规则的实现都必须使用新版本/策略 ID，不得静默改变现有数据含义。当前候选不添加集群路由标识。目标规则仍等待 B01。

## 后端身份

`BackendIdentityProvider` 按显式 `providerId()` 选择，并由 server 统一关闭。`authenticate` 接收 `BackendCredentials` 和 `AuthenticationScope`，以 `CompletionStage` 返回 `AuthorizedBackendIdentity`。提供者必须验证请求的表组权限；失败使用稳定的 `BackendIdentityException.Reason`，不能把权限不足降级成匿名访问。

`AuthorizedBackendIdentity` 包含显式 provider ID、subject、opaque context reference、不可变的允许表组集合、请求 scope 和 `credentialGeneration`。允许表组必须包含请求表组。provider 轮换身份凭据时必须递增 generation，使连接管理器不复用旧凭据代次的连接。身份 DTO 的 `toString()` 不暴露 subject 或 context reference。该身份只表示后端授权上下文，不创建、验证或替代 etcd token、lease 或 session。

`authenticate` 必须尽快返回 `CompletionStage`，不能在 RPC/event-loop 线程上等待后端 I/O。需要阻塞式认证 I/O 的 provider 应使用有界、由 provider 管理的 executor。provider 实例可能被并发复用，必须保证并发调用安全。提供者验证权限失败时须用稳定的 `BackendIdentityException.Reason` 分类，不得降级为匿名访问，也不得在日志或异常信息中暴露凭据。

若异步认证需要在 `authenticate` 返回后继续使用密码，provider 必须在返回前复制 `char[]`，并在用完后清零；调用方在认证 stage 完成后关闭原始 `BackendCredentials`。provider 拥有 `contextRef` 指向的上下文和自身创建的资源。身份及上下文只能由其授权会话持有者在会话有效期内使用，不得超过该会话的生命周期。当前 SPI 尚无逐身份释放回调；provider 必须对自身上下文状态设置有界容量或过期回收策略，并在 `close()` 时停止接受新认证、清理未完成工作及释放全部 provider 资源。未来会话层须明确身份释放与会话失效的衔接。身份 SPI 不创建、验证或替代 etcd token、lease 或 session。

`BackendCredentials` 接收并防御性复制 `char[]` 密钥；异步 provider 必须在 `authenticate` 返回前复制需要的密钥，并在用完后清零；调用方在身份验证完成后关闭原对象。`toString()` 保持脱敏。配置只接受受控 `env://` 或 `secret://` 引用，不嵌入凭据值。

认证失败使用 `BackendIdentityException.Reason` 区分凭据拒绝、权限不足和后端不可用。实现不得将秘密写入日志或异常消息，也不得把权限不足降级成匿名访问。

## 配置与装配

`RuntimeConfigurationLoader` 接受一个有界 UTF-8 YAML 文档，使用安全构造器并拒绝重复键、集合 alias、多文档、未知字段、标量类型强制转换和不支持的 schema。它在启动前校验端口/监听冲突、profile 引用、provider/策略 ID、凭据引用、盐宽度/分区边界及资源预算。失败信息只包含字段路径和稳定原因，不携带原始配置值或解析异常。

`runtime-candidate.yaml` 只用于一次性 loopback 开发验证；`DEV_NO_AUTH` 只能绑定 loopback。它不代表目标环境的 HBase 地址、身份或最终历史布局。`history_layout: UNDECIDED` 保留两种历史存储方案尚未评审定案的状态。

## 验证

- `scripts/verify/m0-spi.py` 检查 SPI 的 POM 和 Java 源码边界。
- `scripts/dev/mvn21.sh -ntp -pl server -am test` 验证配置加载和 SPI 基础用例。
- 真实 provider、目标身份、目标 HBase 连通性及 table-group 授权仍需要相应环境输入和探针证据；此接口与本地候选配置不构成该类验收证据。
