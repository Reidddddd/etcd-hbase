# M0 Java 模块边界

本文记录 M0 八个 Maven 模块之间的依赖方向和职责。它配合[公共 SPI 契约](../spi/m0-spi-contract.md)与[后端身份边界](m0-auth-hbase.md)阅读。

## 模块职责

| 模块 | 依赖方向 | 职责与边界 |
|---|---|---|
| `protocol` | 无项目模块依赖 | 保存 vendored etcd proto、生成配置与 descriptor；生成 API 不手工修改。 |
| `spi` | 仅 Java 标准库 | 提供盐策略、后端身份和必要不可变 DTO；不引用 HBase、Hadoop、gRPC、Protobuf、`core` 或 `server`。 |
| `core` | `spi` | 保存 `RequestScope`、`BackendFailure` 及内部存储/提交 port；不依赖 HBase 客户端或 gRPC。 |
| `hbase` | `core`、`spi` | 实现后端存储与身份连接适配；`HBaseConnectionFactory` 和 HBase 类型只在本模块边界内暴露。 |
| `grpc` | `protocol`、`core`、`spi` | 注册协议服务，处理 Header/Error 投影、请求取消和流生命周期；不依赖 HBase 类型。 |
| `server` | `grpc`、`hbase`、`core`、`spi` | 读取并校验配置，组装 provider、连接工厂、gRPC listener 和有限类别请求观测。 |
| `admin` | `hbase`、`spi` | 提供表组初始化计划、显式确认的 apply 和运维 CLI；不实现服务端业务语义。 |
| `testkit` | `protocol`、`spi` | 保存 Go client 驱动、原生 etcd 对照输入及互通 fixture；不进入服务运行时依赖。 |

## 请求观测边界

`grpc` 定义 `RequestCompletionRecorder`，只传固定的 method/outcome 枚举、生成的 trace ID、`RequestScope` 和单调时钟耗时。`server` 的 `RequestMetrics` 实现该接口，并由 `Bootstrap` 注入 gRPC 拦截器。trace ID 只用于日志关联，不作为指标维度；原始 key、账号、密码、token、表组动态名称和后端异常 message 不通过该接口传递。

该回调是 `grpc` 与 `server` 之间的内部装配边界，不属于面向扩展发行版的公共 `spi`。扩展 provider 不需要实现请求日志或指标接口。

## 依赖约束

- 生产模块按表中方向依赖；不得增加 `spi` 到实现模块、`core` 到 HBase/gRPC、或 `grpc` 到 HBase 的反向依赖。
- `server` 负责生命周期组装。共享 `AsyncConnection` 由 HBase 工厂管理，单个请求取消只释放请求拥有的资源。
- 未实现的 etcd RPC 在 `grpc` 骨架中以 `UNIMPLEMENTED` 终止。模块能够编译、服务能够启动或协议能互通，不代表 KV、Lease、Watch、Auth 或目标 HBase 语义已经通过验收。
- `scripts/verify/m0-spi.py` 检查 SPI 的 Maven 依赖与 Java 源码引用边界；新增生产依赖时同步复核本表和检查结果。
