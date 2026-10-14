# M0 后端身份与 HBase 连接边界

本文记录 M0 的公开身份契约、连接管理边界和未决目标输入。它不声明目标 HBase 已通过认证或操作验收。

## 身份契约

`BackendIdentityProvider` 使用配置中的显式 provider ID。它接收 `BackendCredentials` 与 `AuthenticationScope`，异步返回 `AuthorizedBackendIdentity`。身份携带 subject、opaque context reference、请求的 connection profile、不可变允许表组集合和凭据代次。允许集合必须包含当前请求表组。provider 只负责取得/验证后端授权上下文，不负责 etcd token、租约或会话。

异步 provider 若需要在 `authenticate` 返回后继续使用密码，必须先复制 `char[]`，并在不再需要时清零。调用方负责关闭原始 `BackendCredentials`。异常原因使用稳定分类，日志与异常消息不得带原始凭据或未经筛选的后端 message。

`authenticate` 必须尽快返回，不能在 RPC/event-loop 线程上等待后端 I/O；需要阻塞 I/O 的 provider 应使用有界、由 provider 管理的 executor。provider 可能被并发调用，必须保证线程安全。provider 拥有 `contextRef` 指向的上下文。身份可由其授权会话持有者在会话有效期内使用；当前 SPI 没有逐身份释放回调，因此 provider 必须限制或过期回收上下文，并在幂等的 `close()` 中停止新认证、结束自有工作并释放资源。未来会话层须明确会话失效与身份释放的衔接。

配置校验将 `DEV_NO_AUTH` 与 `dev-no-auth` provider 绑定，并要求 listener 为 loopback。`DevelopmentIdentityProvider` 返回合成身份、凭据代次为 0；它只用于显式本地开发路径。它不连接目标身份、不证明账号权限，也不能作为目标验收证据。

## 连接创建与复用

HBase 特有接口 `HBaseConnectionFactory` 位于 `hbase` 模块并返回官方 HBase `AsyncConnection`。连接由工厂拥有，调用者借用但不得关闭；应用关闭时由工厂关闭所有连接。HBase 官方客户端将连接描述为可由多线程应用长期复用的重量级对象；异步 client 指南也要求应用退出时关闭 `AsyncConnection`。[HBase client 指南：连接与异步 client](https://hbase.apache.org/docs/architecture/client/)

`OfficialHBaseConnectionFactory` 的复用键包含 provider、subject、opaque context reference、表组、connection profile 和 `credentialGeneration`。相同身份的并发创建会合并为一个 future；一个调用方取消其等待不会取消其他调用共享的创建。连接数达到硬上限时返回 `RESOURCE_LIMIT`，创建失败会移除对应条目以允许后续重试。身份错误保留 authentication、permission 或 unavailable 分类，但不暴露底层 message。该原型不逐出可能仍在服务请求的连接；工厂关闭后新请求失败，关闭期间迟到创建的连接会立即回收。

使用 `Configuration` 的默认 creator 只接收 `dev-no-auth` 身份，并按显式 connection profile 创建连接。其他 provider 必须注入经过目标机制验证的 creator。锁定的 `org.apache.hbase:hbase-shaded-client-byo-hadoop:2.6.6-hadoop3` artifact 提供 `ConnectionFactory.createAsyncConnection(Configuration)` 和带 `User` 的重载；API 存在本身并不能证明目标 username/password 可以映射到 `User` 或目标授权机制。[HBase `ConnectionFactory` API](https://hbase.apache.org/2.5/apidocs/org/apache/hadoop/hbase/client/ConnectionFactory.html)、[HBase `AsyncConnection` API](https://hbase.apache.org/2.6/devapidocs/org/apache/hadoop/hbase/client/AsyncConnection.html)

## 未决目标输入与验收边界

- **B02**：目标 HBase/client/Hadoop 版本、JDK 21 兼容组合和连接地址尚未提供。
- **B03**：Authenticate 凭据如何建立 HBase 授权身份、专用测试账号权限、跨副本上下文和凭据轮换机制尚未提供。
- **B06**：TLS、凭据引用/交接、运行监控及恢复目标尚未提供。

因此当前实现只交付身份接口、配置约束、开发身份和有界连接生命周期。`createAsyncConnection` 成功不等于指定身份具备 Get/Put/CAS/Scan 权限。WP16 需要分别用 public 临时 HBase 和目标环境对专用表执行真实操作并记录读回、失败分类、超时/取消和关闭证据；public 无认证结果不能替代 target 身份证据。

跨副本身份引用、全副本重建后的身份激活、已发布提交的后台恢复身份和轮换失效仍由 J50/J51/J55/J56 验证。客户端取消只能释放请求拥有的资源，不能关闭共享 HBase connection，也不能丢弃已发布提交的恢复上下文。
