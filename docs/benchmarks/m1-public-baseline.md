# M1 公开冻结数据基线

固定 seed 为 `20261030`，78 个逻辑 key、118 次变更，最终 66 个活 key；摘要为 `80e44c6c5c8acadad794501fa41b4bb3de0679454bfbd91b001fb3975e1dab96`。`FrozenScanDataset` 保留完整变更序列，线性 oracle 独立于 row codec、HBase 和归并实现。内容包括深层前缀、小前缀、零/ff 字节、2048 字节 key、131072 字节 value、删除重建和固定热点 key。

## 运行条件

- 标准 HBase 2.6.6-hadoop3 client/临时服务，Java 21；3 个 RegionServer 在同一测试 JVM/主机，使用本地 `file:///`，没有 HDFS DataNode。实际版本见运行输出。
- 两种历史布局、1/4/16 桶，每张数据表均按同一数据集的物理 key 分位预分为 16 个 region。单桶也有 16 个 region。HISTORY 另实际写入 latest 表，因此为 2 张表/32 个数据 region、236 次应用 Put；NATIVE 为 1 张表/16 个 region、118 次应用 Put，辅助成本单列。
- 单请求并发，物理扫描许可 1；head 预留为桶数乘 16384 字节，page 16384，I/O 64 MiB，output 2 MiB；SYNC_WAL、TTL 无限，NATIVE 最大版本 64。应用账本峰值不等于 JVM 总内存。
- 每组分 MemStore 与 flush 后两阶段；flush 后清一次 HBase block cache，随后逐渐预热，OS/文件系统缓存保持。每场景 3 个样本，p95/p99 因样本量均为最大值，仅作复现/回归基线。

## 初始本地观察

下表是 MemStore 阶段每次查询的平均已观测扫描 RPC，包含有限页重开及观测到的预取；不含完整 META、点读、关闭和晚到计数。空前缀无结果，小前缀返回 2 条，恢复前缀 Limit=8，固定热点前缀返回 1 条。

| 布局 | 桶 | 空范围 | 小前缀 | 恢复前缀 | 固定热点 key |
|---|---:|---:|---:|---:|---:|
| HISTORY | 1 | 1.00 | 3.00 | 28.00 | 22.00 |
| HISTORY | 4 | 4.00 | 4.00 | 34.33 | 25.00 |
| HISTORY | 16 | 16.00 | 17.00 | 57.33 | 36.00 |
| NATIVE | 1 | 1.00 | 3.00 | 28.67 | 2.00 |
| NATIVE | 4 | 4.00 | 4.00 | 34.67 | 5.00 |
| NATIVE | 16 | 16.00 | 17.00 | 56.67 | 16.00 |

全部 6 个 profile、108 个场景与 oracle 一致，More/Count 与版本逐条核对；所有 profile 的固定 key 写入均只映射到 1 个 region。多桶对空/小前缀产生扇出成本；盐规则的选择仍须结合真实负载，单个固定 key 的历史版本不会被盐分散。最终历史布局留待 M2 的提交/可见性及回收验证。

## 口径与复现

`returnedPhysicalRows` 是 client 实际收到的 HBase Result 行，`returnedCellBytes` 是返回 Cell 序列化大小。`observedServerCounters` 单列页完成时可见的服务端 ROWS_SCANNED、BLOCK_BYTES_SCANNED、FS/block-cache/MemStore 字节及 scanner retry 计数；这些不等于物理磁盘 IO，也不覆盖晚到统计。跨 client/server 版本为 0 的计数可能没有上报，不能推成没有 IO。m/v 同列族的列投影不保证文件块层的 value IO 隔离。

`applicationPointReads` 单列选定 value 的 Get 次数，不冒充实际 RPC。请求总读预算、队列公平性和取消竞争由相应契约测试覆盖；公开 SIMPLE 临时环境另通过真实 scanner map 观察打开和取消后关闭，不能推广成目标认证/TLS或其他 client 的关闭证明。J11 单独提供 minor/major compaction 与保留配置证据。

执行 `bash scripts/verify/m1.sh --through J17`；门禁要求真实 suite、冻结摘要、完整矩阵、所有 oracle 与资源边界，缺失/重复/失败/跳过均拒绝。JSON 摘要和原始日志在本地验证目录，CI 打印脱敏的合成计数。B05 负载/容量输入、目标身份/权限/TLS、JVM/direct-memory 峰值及完整物理 IO 尚待验证；当前不声明容量验收通过。
