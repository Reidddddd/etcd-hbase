# M1 内部 KV 存储格式

本格式用于专用原型表，不启用生产 etcd KV/Range handler，不提供全局提交或并发快照。最终历史布局在 M2 结合隔离、恢复和回收证据后选择。

**表 1　物理定位**

| 表形态 | RowKey | Cell timestamp | 默认版本保留 |
|---|---|---|---|
| CURRENT | `bucket || E(key)` | `revision - 1` | 每列 1 个版本，不提供历史读取保证 |
| HISTORY | `bucket || E(key) || BE64(revision)` | `revision - 1` | 每 key/revision 独立不可变行 |
| NATIVE | `bucket || E(key)` | `revision - 1` | 原型默认每列最多 64 个版本 |

E 将原始 `00` 转为 `00 ff`，以 `00 00` 终止，按无符号字节保序。盐输入为完整逻辑 key；同 key 全部 revision 在同一桶，生产桶数/算法不得沿用测试值而未经确认。单桶使用相同的常量桶前缀，作为不分散路由的对照，不冒充省略盐字节的另一套编码。

RowKey 格式 ID `ordered-zero-escape-v1`、表形态、列族和盐 strategy/version/buckets/width 保存在表 descriptor 的 `etcdhbase.storage.*` 属性中。绑定时先验证，不自动采用、升级或覆盖未知表。RowKey 没有逐行格式号或固定 KV recordKind；system 混合记录不使用这里的专表绑定。

**表 2　列投影与版本**

| qualifier | 内容 |
|---|---|
| `m` | 独立内部 protobuf RecordMetadata：格式号、create/mod revision、version、lease、活值引用或逻辑墓碑 |
| `v` | ValuePayload：格式号、mod revision、原始 value |

元数据投影只选择 m。完整投影的活值必须找到同版本 v，并校验消息 revision、长度和 Cell timestamp；墓碑不回退读取旧 v。空 value 是有 payload 的活值，省略投影和墓碑均不等于空 value。元数据和值同次单行 Put，显式 SYNC_WAL；HISTORY 的插入使用 absent-row CAS，拒绝覆盖既存版本。CURRENT/NATIVE 的原始 Put 不解决同 timestamp 迟到写隔离，不能作为事务提交证明。

所有 revision 是正 int64；timestamp 范围为 0 到 `Long.MAX_VALUE - 1`，历史上界 r 使用 `[0,r)`。revision 型 timestamp 不使用墙钟 TTL；版本上限也不能替代逻辑 Compact 和跨副本读保护。原生 HBase Delete 不等于逻辑墓碑。

编码后的完整 RowKey 不超过 HBase 行长上限；编码检查包含盐、转义膨胀、终止和历史后缀。内部值最多 4 MiB；批量最多 128 项且应用保留量最多 16 MiB。物理页最多 1024 条逻辑记录、64 MiB 应用保留量和 100 万条物理记录，并有独立有限物理字节预算；这些是原型硬上限，不是生产容量承诺。

请求 scope 取消停止新工作。写操作的物理完成以有界后端回调结算；取消或超时不撤销已接收 mutation。非空点读结果在调用方提供的有界 worker 中解码，排队期间取消或超时不再复制 value，worker 拒绝任务返回资源上限错误。扫描在同一 worker 中阻塞拉取，有限页关闭 scanner 后再交付；scope 等待已开始任务的资源清理及已持有或晚到 scanner 的实际关闭，清理未完成时不提前释放物理许可。页在实际清理完成后重新检查 deadline。页的断点是最后完整消费的逻辑 key，M1 分页验证只使用冻结数据集。

单行条件不匹配与后端故障分开；跨行批量不承诺事务原子性，按输入索引保留成功、失败或未知结果。SUCCEEDED 只代表已确认物理写入，不代表全局事务提交；FAILED 包括客户端调用前的编码拒绝、取消或超时，以及已明确拒绝的写入；UNKNOWN 只用于客户端调用已经开始、但结果无法确认的失败，不用于已知尚未调用后端的拒绝。扫描成本中的返回 Cell 字节、client scan RPC 与逻辑操作分别记账，不能写成 HFile/磁盘 IO 或全部 META/重试成本。
