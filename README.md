# etcd-hbase

一个以 Apache HBase 为存储后端、使用 Java 21 构建的 etcd v3 API 兼容服务。首版范围、架构边界和验收标准见[开发计划](DEVELOPMENT_PLAN.md)及已评审的 [M0 工程、环境与协议设计](docs/specs/2026-10-10-m0-engineering-environment-protocol-design.md)。

## 当前实现

M0 已包含 Java 21 / Maven 八模块工程、固定的 etcd v3.5.23 proto 和 Java 生成校验、公开 SPI 与运行配置、盐/表配置候选、HBase 探针和初始化 CLI 原型、Go 原生 etcd 对照与 Go/Java 协议互通，以及 gRPC 服务骨架。

服务骨架尚未实现 KV、Lease、Watch、Auth 等业务语义；未实现的方法明确返回 gRPC `UNIMPLEMENTED`。公开 HBase 检查使用可丢弃的开发无认证配置，不证明目标集群的认证、权限、Java/HBase 兼容性或操作验收。当前 M0 **尚未通过阶段验收**。

## 开发与验证

安装锁定的 Temurin JDK 21，并设置 `ETCD_HBASE_JDK21_HOME`：

```bash
export ETCD_HBASE_JDK21_HOME=/path/to/temurin-21
scripts/dev/configure-toolchain.py --java-home "$ETCD_HBASE_JDK21_HOME"
scripts/dev/mvn21.sh clean verify
scripts/verify/m0.sh --scope public
```

`scripts/verify/m0.sh --scope public` 会运行公开协议、Java、Go、临时 HBase 和打包运行镜像检查；镜像检查需要 Docker。各命令生成的机器证据保存在被忽略的 `.artifacts/m0/` 下。target HBase 需等目标输入齐备后，通过 `scripts/verify/hbase-probe.sh --profile target --config-ref "$ETCD_HBASE_M0_TARGET_CONFIG"` 单独运行。

## 待确认输入

目标盐格式 B01、目标 HBase 版本组合 B02、实际后端身份机制 B03，以及目标访问安全条件 B06 尚未提供。它们分别限制目标盐配置和目标 HBase 身份/操作验收。项目许可证选择 I07、公开 artifact 命名与发布位置 I08 也等待维护者决定；生产负载目标 B04/B05 在 M0 只保留分类初稿，按后续计划完善。

逐项事实、影响范围和解除方式见[外部输入台账](docs/inputs/m0-inputs.yaml)。公开盐候选、开发身份和本地配置不替代这些目标输入。首版仅承诺开发计划和 API 矩阵明确列出的能力，不声明完整等价于原生 etcd。

## 设计、基线与验证范围

- [M0 设计方案](docs/specs/2026-10-10-m0-engineering-environment-protocol-design.md)：模块边界、协议、后端身份、安全和验收设计。
- [开发计划与任务状态](DEVELOPMENT_PLAN.md)：阶段、主任务、完成标准、风险和评审记录。
- [Milvus 调用基线](docs/milvus-etcd-usage.md)与[API 矩阵](docs/api-matrix.md)：固定调用范围、Count 差异和请求/错误消费者。
- [分类验收目标](docs/acceptance-targets.md)、[工具链基线](docs/build/m0-toolchain.md)、[后端身份边界](docs/architecture/m0-auth-hbase.md)：公开候选、目标输入和证据边界。
