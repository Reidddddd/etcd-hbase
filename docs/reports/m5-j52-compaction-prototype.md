# J52.1 保留参数契约与待验证项

## 1. 范围与状态

本文供 J24.1/J25 历史布局选型与 J52 评审使用，记录保留参数的源码约束、descriptor 单元测试及尚缺的 HBase 实验。descriptor 是表和列族的配置描述；构造并校验它不涉及 HBase 读写。

本次只交付参数契约回归，**J52.1 仍未完成**，也不选择最终历史布局。版本淘汰、TTL 到期、删除标记经过 compaction 后的行为与真实目标环境均未在本次实测。HBase compaction 指存储文件合并，与服务的逻辑 Compact 边界不同。

## 2. 源码约束

以下为既有实现事实，不是本次新增的平台行为：

- [`StorageTableBinding`](../../hbase/src/main/java/io/etcdhbase/hbase/storage/StorageTableBinding.java) 的 `HISTORY` 布局将 revision 编入行键；`NATIVE` 布局使用同一行的 Cell 多版本。`descriptor()` 对 `NATIVE` 默认设置 `maxVersions=64`，其余布局为 `1`；TTL 使用 HBase 的 `FOREVER` 默认值。
- `StorageTableBinding.validate()` 先核对表身份、列族、durability 与格式/profile 属性，再要求 TTL 为 `FOREVER`，并拒绝 `NATIVE` 的 `maxVersions < 2`。
- [`HBaseMaterializationStore.open()`](../../hbase/src/main/java/io/etcdhbase/hbase/storage/HBaseMaterializationStore.java) 还单独限制 `NATIVE` 的版本配置为 `2..1024`。binding 层允许某个 descriptor 不等于完整启动或读写验证通过。

`64` 是每列物理 Cell 版本数的默认配置，不是全局 revision 跨度，也不证明读 pin 可保留固定长度的历史。某个 key 的更新密度、版本配置、读取方式及 HBase 的实际保留行为都需要纳入选型实验。

## 3. Descriptor 单元测试

[`J52RetentionDescriptorTest`](../../hbase/src/test/java/io/etcdhbase/hbase/storage/J52RetentionDescriptorTest.java) 只执行 Java descriptor 构造与校验，不启动 MiniCluster。

**表 1 参数契约断言及证据边界**

| 用例 | 断言 | 不证明的行为 |
|---|---|---|
| 有限 TTL | 从每种布局的有效 descriptor 仅将 TTL 改为 `60`，校验拒绝 | HBase 何时淘汰到期 Cell |
| NATIVE 单版本 | 从有效 descriptor 仅将 `maxVersions` 改为 `1`，校验拒绝 | 真实历史读取或删除结果 |
| HISTORY 默认值 | `maxVersions=1`、TTL 为 `FOREVER`，校验通过 | revision 行集合的完整性与长期保留 |
| NATIVE 默认值与可配置性 | 默认 `64`、TTL 为 `FOREVER`；改为 `128` 后 binding 校验也通过 | 固定逻辑读窗口或完整运行时配置验收 |

拒绝用例先校验原始 descriptor，再只修改待测参数，保留全部格式属性，避免因无关配置错误而误通过。临时移除生产代码的保留参数校验时，原始四项测试仍全部通过；修正后的 TTL 与单版本两项测试失败。该反向探针仅用于验证断言有效性，生产代码已恢复，未提交校验变更。

测试以 Surefire 单测登记到 [`J52.json`](../../testkit/cases/m5/J52.json)，报告校验器要求该类及全部四个方法存在。清单遗漏类或任一方法的反向探针必须失败。

## 4. 安全约束与未验证风险

逻辑边界、pin 和底层保留配置承担不同职责。TTL 为 `FOREVER` 与合适的版本配置是当前实现的配置要求；参数校验通过不足以证明读 pin 能阻止 HBase 自身的版本淘汰。NATIVE 配置容量是否满足承诺的历史读取，需要覆盖高更新密度 key、活跃 pin、flush 与 compaction 的实验，不能从默认 `64` 推出固定可读窗口。

HISTORY 按 revision 分行，单行版本默认值也不能单独证明历史完整性；仍需验证行集合、精确删除、基线保留及迟到写。现有双布局 KV 回收用例的范围见[维护说明](../maintenance/j52-compaction-storage.md)，不将其中的短期逻辑墓碑/精确删除检查解释为删除标记经过长期 compaction 后的存活证明。

当前 physical RPC 仅投影 KV 历史完成。全部内部记录域的保护排空、结果窗口运行时保护和完整物理完成证明仍未交付，详见[设计 §6.1](../specs/2026-11-20-m5-auth-gc-lifecycle-design.md)及维护说明。HFile 空间收敛也不能由 descriptor 或删除确认断言推出。

**表 2 J52.1 仍需补充的实验**

| 待验证项 | 所需证据 |
|---|---|
| NATIVE 版本容量 | 超出配置版本数的同一 key 更新，核对不同 revision 的可读性、活跃 pin 与 flush/compaction 前后结果 |
| TTL 到期 | 隔离实验中观测有限 TTL 的实际过期行为，区分配置拒绝与底层淘汰 |
| 删除标记 | 两布局下逻辑墓碑、HBase 删除标记、迟到写及 compaction 前后的精确读取结果 |
| Compaction 与空间 | 实际触发与完成证据、HFile 空间变化；区分逻辑不可读、精确删除确认与磁盘归还 |
| 目标环境 | 固定目标 HBase 版本、配置、权限及环境，复核上述结果后再形成选型结论 |

## 5. 验证入口

参数单测：

```sh
scripts/dev/mvn21.sh -B -ntp -pl hbase -am \
  -Dtest=J52RetentionDescriptorTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

J52 清单与报告防遗漏检查：

```sh
python3 scripts/verify/m5-report.py --validate-task J52
python3 scripts/verify/test_m5_report.py
```

完整公共存储门禁入口如下；运行结果应以对应 head/base 的 CI 报告为准，不能仅因本文列出命令就视为已经执行：

```sh
scripts/verify/m5.sh --scope public --task J52 \
  --artifact-root .artifacts/m5/j52-retention
```

本次新增四项参数单测与清单防遗漏检查的执行证据记录在 PR 中。完整 J52 门禁即使通过，也不替代表 2 的专门实验、最终布局选型或真实目标环境验收。
