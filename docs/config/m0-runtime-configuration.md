# M0 运行配置

M0 服务配置使用单一 YAML 文档。加载顺序为：读取 YAML 文件、应用下表中的环境变量覆盖、校验必填字段并绑定到 Java 配置对象，再执行统一校验。未知 YAML 字段和未识别的运行配置环境变量都会导致启动配置失败。工具链变量 `ETCD_HBASE_JDK21_HOME` 与 `ETCD_HBASE_MAVEN_REPOSITORY` 不属于运行配置，loader 会忽略它们。

## 环境变量覆盖

环境变量只覆盖第一项 listener 和运行资源预算。表组、HBase 身份模式、credential reference、provider、盐 profile 及表结构仍由 YAML 明确声明。

| 环境变量 | YAML 字段 | 类型 |
| --- | --- | --- |
| `ETCD_HBASE_LISTENER_BIND_HOST` | `listeners[0].bind_host` | 字符串 |
| `ETCD_HBASE_LISTENER_PORT` | `listeners[0].port` | 十进制整数 |
| `ETCD_HBASE_LISTENER_TABLE_GROUP_ID` | `listeners[0].table_group_id` | 字符串 |
| `ETCD_HBASE_MAX_CONFIG_BYTES` | `resources.max_config_bytes` | 字节 |
| `ETCD_HBASE_MAX_CONCURRENT_REQUESTS` | `resources.max_concurrent_requests` | 请求数 |
| `ETCD_HBASE_MAX_SCANNERS_PER_TABLE` | `resources.max_scanners_per_table` | scanner 数 |
| `ETCD_HBASE_REQUEST_DEADLINE_MS` | `resources.request_deadline_ms` | 毫秒 |
| `ETCD_HBASE_MAX_RPC_BYTES` | `resources.max_rpc_bytes` | 字节 |
| `ETCD_HBASE_MAX_REQUEST_BYTES` | `resources.max_request_bytes` | 字节 |
| `ETCD_HBASE_MAX_QUEUED_REQUESTS` | `resources.max_queued_requests` | 请求数 |

只有映射表中的变量会被应用；不带前缀的进程环境变量不会被 loader 当作配置项。变量值不写入异常消息。YAML 文件与允许的环境覆盖合计不得超过 65536 字节，`max_config_bytes` 还必须覆盖该有效配置大小。

## 监听与身份

`bind_host` 和 HBase quorum 项必须是主机名或 IP 字面地址；不接受 `*` 通配符。`port: 0` 表示由操作系统分配随机端口，仅允许 loopback 地址；多个随机端口 listener 不视为端口冲突。IPv6 listener 地址按字面地址规范化后检查冲突，支持括号、压缩和展开写法。重复端口检查会把 `localhost` 保守地视为与显式 loopback IP 可能重叠，不执行 DNS 查询。`DEV_NO_AUTH` 必须显式出现在 HBase profile 中；listener 和逗号分隔的 HBase quorum 中每个地址都必须是 loopback 地址或 `localhost`。校验只解析 IP 字面地址，不为其他主机名执行 DNS 查询。其他身份模式必须提供 `env://NAME` 或 `secret://reference` credential reference；YAML 中不接受明文密码或 token。

同一 HBase quorum、client port 和 namespace 是一个物理表命名范围。即使两个 HBase profile 使用不同 ID，只要连接目标与 namespace 相同，表组就不能重复声明同名物理表。

## 资源预算

所有资源预算都必须为正数。当前 loader 的上限为：配置 65536 字节、并发请求 100000、每表 scanner 1024、请求 deadline 600000 毫秒、RPC 和请求各 67108864 字节、排队请求 100000。`max_rpc_bytes` 不得大于 `max_request_bytes`。

`config/schema/server.schema.json` 描述同一组配置字段。测试会从 Java record 反射字段集合并与 schema 的 properties、required 字段和基础类型逐项比较；表组/profile/provider 引用、listener 冲突、loopback、盐边界及资源间约束由 loader 的跨字段校验继续执行。

## 示例

- [开发候选配置](../../config/examples/m0/server-skeleton.yaml)：明确使用 `DEV_NO_AUTH`，监听 `127.0.0.1:0`。
- [分离 history 表候选](../../config/examples/m0/history-table.yaml)
- [原生 Cell 多版本候选](../../config/examples/m0/native-cell-versions.yaml)
