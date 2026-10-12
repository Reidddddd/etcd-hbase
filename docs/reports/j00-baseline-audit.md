# J00 调用、API 与目标基线审计报告

日期：2026-10-12。负责人：Codex。分支：`codex/j00-baseline`。状态：交付与静态验证完成，待评审；未记录任务完成或阶段验收通过。

## 交付与版本

| 子任务 | 交付 | 结论 |
|---|---|---|
| J00.1 | [Milvus 调用基线](../milvus-etcd-usage.md)、[源码摘要与完整 RPC 清单](../baselines/j00-source-baseline.json) | Milvus 最低 v3.0.1，本轮固定审计 v3.0.1；16 组入口及消费者，client 自动认证/重试/流恢复；范围限 Kafka + external-etcd 新集群 |
| J00.2 | [首版 API 与验证矩阵](../api-matrix.md) | 全部首版能力、响应字段、Count 差异、错误链、明确不支持项及 C01–C33 用例定义；未观察直接调用的承诺功能仍保留 |
| J00.3 | [分类验收目标初稿](../acceptance-targets.md) | 静态默认值、目标口径、P01–P08 负载 profile 与待输入项；不虚构生产目标或测量结果 |

Milvus commit 为 `658cbd16899bb715a17d4d6f727531a376678ca6`；根与 pkg 模块的 etcd API/client/server 要求均为 v3.5.23。协议与原生对照固定 etcd commit `1cf4bc40111fd95d1e14a90a09a146357b15eb13`。本次最低版本要求没有改变 etcd 协议方向，Java 服务仍为 JDK 21。

源码位置以公开仓库固定 commit 链接引用；本仓库保存文件摘要及提取结果，不复制依赖源码。探索过的旧 tag/master 不作为交付依据；更新 Milvus 版本需重做审计并执行受影响用例。

## 已执行验证

[`scripts/verify_j00_baseline.py`](../../scripts/verify_j00_baseline.py) 使用 Python 3 标准库，对固定版本的本地源码执行只读校验：

- Milvus/etcd HEAD 与已跟踪文件无修改，审计文件 SHA-256 一致。
- Milvus 非测试直接导入 client 的 46 个检索入口与记录一致；包含排除的迁移、embedded、Woodpecker、测试辅助路径，不能把 46 当作实际生产 RPC 调用数。
- 三份 proto 提取的 48 个完整 RPC 路径、请求/响应类型及流形态一致。
- 根与 pkg 模块的 etcd API/client/server 版本均为 v3.5.23。
- 三份交付文档的 49 个固定源码链接指向对应 commit，文件存在、引用行号有效；本地链接、引用定义及 C01–C33 编号齐全。
- 仓库文档无尾随空白，Python 脚本语法及 JSON 格式检查通过。

结果：上述静态校验通过。源码链接检查验证路径和行号，内容含义仍由人工源码审计与后续评审判断；检索入口不是全程序调用图。

## 复现

需要 git、Python 3 及两份公开源码。可在独立目录获取：

```sh
git clone --depth 1 --branch v3.0.1 https://github.com/milvus-io/milvus.git milvus-v3.0.1
git -C milvus-v3.0.1 checkout --detach 658cbd16899bb715a17d4d6f727531a376678ca6
git clone --depth 1 --branch v3.5.23 https://github.com/etcd-io/etcd.git etcd-v3.5.23
git -C etcd-v3.5.23 checkout --detach 1cf4bc40111fd95d1e14a90a09a146357b15eb13
```

在本项目根目录执行（替换两份源码的路径）：

```sh
python3 scripts/verify_j00_baseline.py \
  --milvus-source /path/to/milvus-v3.0.1 \
  --etcd-source /path/to/etcd-v3.5.23
```

## 后续责任与未执行项

- J01–J03：工程、proto 生成及服务骨架；固定 descriptor 后核对全部方法。当前没有 Java 服务实现。
- J07：建立 Go client / 原生 etcd 请求对照驱动；C01–C33 目前只有定义，执行数为零。
- J66/M6：v3.0.1 的 TSO 仍吞掉 Load 错误，GcConfirm 仍完整读取；真实消费者验证不可沿用其他版本的结论。默认 Session TTL 为 15 秒，多角色与 checkpoint 顺序纳入负载与验证。
- B01–B06：盐、HBase/client 组合、实际身份/权限、容量/profile、运维等输入仍待补充；J00.3 初稿和后续基础工程可继续推进。
- 本轮未执行 `go list -m all`、Go client、原生 etcd、HBase 或 Milvus 运行测试，不对构建、性能、故障持久性或端到端兼容性下结论。

J00 当前等待交付评审，完成计数仍为 0/78，M0 未验收。J00.3 的初稿责任与生产目标确认、后续实际测量责任分开记录，目标待输入不等于本次静态验证失败。
