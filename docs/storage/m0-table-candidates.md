# M0 历史表配置候选

本文定义供公开原型读取的 table-group 配置形状。独立 history 表和 HBase 原生 Cell 多版本都保留为候选；M0 不据此选择最终物理布局。目标盐和 HBase 版本输入分别等待 B01、B02。

## 两种布局

| 候选 | 数据表 | 记录类别 | 样本保留参数 |
|---|---|---|---|
| 独立 history 表 | `latest`、`history`、`system` | `latest` 保存 `CURRENT`；`history` 保存 `HISTORY`；`system` 保存 `DECISION`、`EVENT`、`LEASE`、`AUTH` 和 `CONTROL` | history 列族 `max_versions=1`；所有样本列族不启用墙钟 TTL |
| 原生 Cell 多版本 | `kv`、`system` | `kv` 保存 `CURRENT` 和 `HISTORY`；`system` 保存相同控制类记录 | `kv` 列族 `max_versions=64`，仅供至多 64 个样本版本；不启用墙钟 TTL |

相应的完整配置可由同一 loader 校验：[`history-table.yaml`](../../config/examples/m0/history-table.yaml) 与 [`native-cell-versions.yaml`](../../config/examples/m0/native-cell-versions.yaml)。样本表名使用 HBase profile 中配置的 namespace；它们不是目标环境的表名。

## 每张候选表的配置

每张表都记录独立的 `schema_version`、与 table group 盐 profile 对齐的 `profile_version`、列族、预分区 `split_keys_hex`、逻辑 `record_categories` 和 `identity_permissions`。loader 要求每张候选表的 split keys 与 table group 的 salt profile 相同，并校验列族版本/TTL/删除策略、类别归属、表名冲突和权限条目。

`identity_permissions` 中的 `identity_ref` 是 `runtime` 或 `initializer` 这样的逻辑角色标签，不是账户名，也不声称目标 HBase ACL 已创建。真实身份机制、跨副本凭据和目标权限仍须由 B03/B06 与目标探针确定。运行角色的样本权限是 `READ`/`WRITE`；初始化角色另需 `CREATE`/`ADMIN`。

`ttl_seconds: -1` 是本项目配置 schema 的“不过期”哨兵值。适配层应将其转换为锁定 HBase client 的无限 TTL 值；不可把 `-1` 直接当作 HBase API 的数值。适配时使用该版本的 `HConstants.FOREVER`；family TTL 以秒计。[HBase API](https://hbase.apache.org/2.6/devapidocs/org/apache/hadoop/hbase/HConstants.html) [HBase Reference Guide](https://hbase.apache.org/book.html#ttl)。Native Cell 候选禁用墙钟 TTL，因为 Cell timestamp 表示逻辑 revision，而 HBase family TTL 按时间秒数计算。未来的安全回收仍需结合逻辑 Compact、删除标记、compaction 和读保护原型决定。

## 保留的协议边界

- 两种布局都必须保留统一的逻辑提交、可见性、压缩边界和读保护语义。
- 本配置不定义 rowkey 或 revision 到 Cell timestamp 的映射；候选 schema 不等于稳定存储格式。
- 每个 listener 绑定一个 table group；table group ID 不加入 rowkey。
- `max_versions=64` 仅支撑有上限的公开样本，不承诺无界历史。正式保留策略归后续 J11/J24/J25/J52 原型和报告。
- 建表原型只能面向确认的空表组；正式启动、恢复和非空表处理归 J55。
