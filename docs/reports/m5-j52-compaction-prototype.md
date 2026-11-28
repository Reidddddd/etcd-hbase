# J52.1 保留参数与真实 HBase 实验

## 1. 范围与状态

本文供 J24.1/J25 历史布局选型与 J52 评审使用，记录两种候选布局的保留配置、实际读写及 major compaction 结果。2026-11-28 已完成公开临时环境中的版本容量、TTL、删除标记和 active HFile 观测；当前交付仍待对应 head 的 CI 与正式 review。最终布局尚未选择，真实目标环境尚未验收。

实验使用项目锁定的 HBase `2.6.6-hadoop3`、JDK 21、一个 RegionServer 的独立进程 MiniCluster 和四个 salt bucket。临时环境采用 simple 身份；本实验不证明目标集群的 Kerberos、ADMIN 权限、DDL 策略或存储运维行为。HBase compaction 是存储文件合并，与服务的逻辑 Compact 边界不同。

## 2. 配置与生产约束

[`StorageTableBinding`](../../hbase/src/main/java/io/etcdhbase/hbase/storage/StorageTableBinding.java) 的 HISTORY 将 revision 编入行键，NATIVE 将同一 key 的 revision 编为 Cell timestamp。NATIVE 的默认 `maxVersions=64`，其余布局默认 `1`；所有生产 binding 都要求 TTL 为 `FOREVER`。NATIVE binding 拒绝单版本，完整 materializer 另外要求版本配置为 `2..1024`。

[`HBaseMaterializationStore`](../../hbase/src/main/java/io/etcdhbase/hbase/storage/HBaseMaterializationStore.java) 在 NATIVE 物化前读取实际 metadata 版本数，达到配置容量时返回 `RESOURCE_LIMIT`，不会继续写入并依赖 HBase 自动淘汰旧历史。`64` 是每列物理版本容量，不能据此承诺固定的 revision 跨度或持续写入吞吐。

[`J52RetentionDescriptorTest`](../../hbase/src/test/java/io/etcdhbase/hbase/storage/J52RetentionDescriptorTest.java) 保留四项配置契约测试：拒绝有限 TTL、拒绝 NATIVE 单版本、核对 HISTORY 默认值，以及核对 NATIVE 默认值与 `128` 配置。拒绝用例只改变待测参数；此前移除对应生产校验的反向探针能使它们失败。descriptor 构造与单测均不替代以下真实 HBase 实验。

## 3. 版本容量与持久读保护

[`J52HistoryCollectionIT`](../../hbase/src/test/java/io/etcdhbase/hbase/storage/J52HistoryCollectionIT.java) 先写 revision 2，再通过生产 `ReadViewService` 和 `SnapshotPinManager` 注册持久读 pin。每次最终观测都核对相同 pin generation 的持久保护，并确认实验仍在原始一分钟 lease 内完成。

**表 1 同一 key 的版本容量结果**

| 路径 | 实际写入与准入 | Flush 后 revision 2 | Major 后 revision 2 | 结论 |
|---|---|---|---|---|
| HISTORY 生产物化 | 70 个 revision，`maxVersions=1` | 保留 | 保留 | 每个 revision 单独成行，单 Cell 版本限制未淘汰这些行 |
| NATIVE 生产物化 | 64 个版本；第 65 次写入被 `RESOURCE_LIMIT` 拒绝 | 保留 | 保留 | 生产容量保护阻止静默丢失 pinned 历史 |
| NATIVE 隔离 raw Put | 在上述临时行上绕过 materializer，补到 70 个 metadata 版本 | 本次仍保留 | 已淘汰 | HBase 的版本保留不读取应用 pin，容量保护不可绕过 |

raw Put 使用合成 metadata，只经原始 HBase `Get` 观测，不作为合法生产记录解码，也不声称生产入口成功写入了 70 个 NATIVE 版本。此次追加版本先进入另一个 HFile，因此 flush 后仍能精确读到旧文件中的版本；major compaction 合并后旧版本消失。不能把 flush 单独视为跨文件淘汰已完成。

## 4. 有限 TTL 的实际行为

隔离实验仅将有效 descriptor 的 TTL 改为 2 秒，并断言生产 `validate()` 拒绝该配置，再用测试设施显式创建实验表。两种布局均观察到：

- `RevisionTimestamp.timestamp(2)` 对应的 Cell 立即不可见，因为逻辑 revision timestamp 不能用作墙钟 TTL。
- 使用实际墙钟 timestamp 的探针先可见，随后到期不可见。
- 已确认的 major compaction 后，两类过期探针仍不可见。

一个人工 future timestamp 为 `Long.MAX_VALUE - 3` 的 anchor 使实验有非空输入和输出文件。它仅用于 fixture 观测，不属于业务时间、生产写入策略或 TTL 绕过方案。生产 TTL 校验保持不变。

## 5. 删除标记与迟到写

生产回收实验先确认旧 pin 阻止删除，释放并排空后精确删除 revision 2、3；逻辑墓碑 revision 4 与更新 revision 5、6 在实际 major compaction 前后都保留。使用原始 `Scan.setRaw(true)` 统计 HBase Delete marker：HISTORY 从 4 个降为 0，NATIVE 从 6 个降为 0。随后重放原生产 materializer 的迟到写，仍被 RETIRED guard 或当前 fence 拒绝，旧版本没有复活。

独立 raw 表另做反例：精确删除 timestamp 1 的 Cell，确认 major compaction 已清除 Delete marker，再以相同 timestamp 迟到 Put。两种布局的 raw Cell 都重新可见。该反例故意不经过生产 fencing，说明 Delete marker 清除后仍须保留生产 guard；它不是生产写入入口的漏洞复现。

## 6. Major Compaction 与空间观测

[`HBaseCompactionProbe`](../../hbase/src/test/java/io/etcdhbase/hbase/storage/HBaseCompactionProbe.java) 先实际 flush 并取得非空输入，再调用 `AsyncAdmin.majorCompact()`。完成要求同时满足：原来有输入文件的 region 的 last-major timestamp 增长、当前 active StoreFile 集合与所有输入文件完全不相交且非空、compaction state 为 `NONE`。随后逐项核对读写语义；不以 `NONE` 或空查询单独证明完成。

文件集合从 MiniCluster RegionServer 的实际 `HStore.getStorefiles()` 读取，排除已经 compact、尚待 archive 的旧输入文件；大小取 HStoreFile 的实际文件信息。该本地控制通道与既有 scanner 计数通道共用同步请求及 nonce，应答通过原子文件发布。相邻 scanner 取消回归确认远端 scanner 曾打开且已关闭。

**表 2 本次 active HFile 字节观测**

| 实验 | HISTORY：前 → 后（字节） | NATIVE：前 → 后（字节） |
|---|---|---|
| 生产版本容量 | 19,541 → 19,575 | 16,603 → 16,637 |
| Raw NATIVE 超容量 | 不适用 | 21,784 → 16,628 |
| 有限 TTL 探针 | 14,592 → 9,677 | 14,496 → 9,629 |
| 生产精确删除 | 5,941 → 5,819 | 5,647 → 5,495 |

注：这些数值是一次临时环境运行的 active StoreFile 大小，不是固定断言、HDFS 总占用或磁盘已归还字节。全部保留的版本实验中，文件元数据可使合并后大小略增。测试要求真实完成与正确读取，不要求每次字节都下降。

## 7. 安全约束与剩余验收

本次证据支持以下约束：生产 TTL 保持 `FOREVER`；NATIVE 必须保留容量准入并评估高更新密度 key 的写入压力；回收先受逻辑边界和持久 pin 限制，再做精确删除及强读确认；永久 guard/fence 不能仅因 HBase Delete marker 已清除而删除。HISTORY 的 revision 行成本与 NATIVE 的容量背压仍须结合访问及恢复成本选型。

J52.2a–J52.2d 分别交付结果窗口、系统记录回执、完整等待和跨副本转发，状态与范围见[维护说明](../maintenance/j52-compaction-storage.md)。这些完成回执证明本次应回收内容的删除确认，不承诺 HFile 字节立即下降；本实验也没有把 HFile 等待接入 `physical=true`。

最终布局选择、真实目标环境的版本/配置/权限复测、长期混合负载、归档与 snapshot 保留成本、GC 对前台尾延迟的影响，均未由本次短期 MiniCluster 实验完成。目标环境记录与最终选型仍分别保留，不将 J52 整体标为验收完成。

## 8. 验证入口

定向实验及相邻 scanner 回归：

```sh
scripts/dev/mvn21.sh -q -pl hbase -am -Pm5-j52-public \
  -Dtest=J52RetentionDescriptorTest -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dit.test=J52HistoryCollectionIT#nativeAdmissionPreservesPinnedHistoryWhileRawVersionEvictionIgnoresPins+finiteTtlExpiresWallClockCellsAndImmediatelyHidesRevisionTimestampsInBothLayouts+preservesPinnedBaselineTombstoneNewerWritesAndRetiredGuardsInBothLayouts+unfencedLatePutCanReappearAfterItsDeleteMarkerIsCompacted,M1StorageIT#cancelledPhysicalPageClosesTheObservedServerScannerBeforeLeaseExpiry' \
  -Dfailsafe.failIfNoSpecifiedTests=false verify
```

四个实验方法各自覆盖两种布局，相邻 scanner 回归独立执行。完整公共 J52 门禁交由当前 head/base 的 CI，不能将本地定向结果概括为全量通过：

```sh
python3 scripts/verify/m5-report.py --validate-task J52
PYTHONDONTWRITEBYTECODE=1 python3 scripts/verify/test_m5_report.py
scripts/verify/m5.sh --scope public --task J52 \
  --artifact-root .artifacts/m5/j52-retention
```
