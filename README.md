# etcd-hbase

An etcd v3 API-compatible server backed by Apache HBase, built with Java 21.

面向开源的独立服务。默认发行版提供公开可用的实现，扩展发行版通过公开 SPI 复用相同内核。

## 开发与评审

- [开发计划与进度记录](DEVELOPMENT_PLAN.md)：架构与扩展边界、9 个阶段、78 个主任务及子任务、4 个开发检查点、完成标准、风险与评审记录。
- [Milvus 调用基线](docs/milvus-etcd-usage.md)：以最低支持版本 v3.0.1 固定调用、client/proto 版本与错误消费者。
- [首版 API 与验证矩阵](docs/api-matrix.md)、[分类验收目标](docs/acceptance-targets.md)、[J00 审计报告](docs/reports/j00-baseline-audit.md)：当前待评审，运行验证尚未执行。

当前进入 M0 基线整理，尚未交付可运行实现。首版采用轻量 Count，兼容范围与差异以开发计划的能力矩阵为准，不声明完整等价于原生 etcd。

部署目标为无本地业务持久化的 K8s 多副本；跨副本认证、提交接管和 Watch/KeepAlive 恢复，以及 HBase 原生多版本存储评估已纳入计划，均待实现与验证。
