# M0 Java 工具链与公开依赖基线

本文件固定可公开获取的工程工具和依赖版本，供 Java 构建及 M0 public 验证复用。机器可读版本与归档摘要见 [`build/dependency-lock.json`](../../build/dependency-lock.json)。目标 HBase 版本尚未由 B02 确认；公开候选参与依赖/构建检查并由 public-local 探针实际运行，不作为目标集群或 Apache 官方 Java 21 运行支持结论。

## Java 与 Maven

| 项目 | 固定值 | 用途 |
|---|---|---|
| JDK | Eclipse Temurin 21.0.12.1+1 | Java 21 LTS；不开启 preview |
| Maven | 3.9.16 | Wrapper 下载并校验发行归档 |
| Maven Wrapper | 3.3.4 | 项目级构建入口 |
| Java release | 21 | 编译目标；toolchain 选择同一主版本 JDK |

JDK 和 Maven 的平台归档 SHA-256、Maven Wrapper JAR SHA-256 均记录在锁文件中。构建通过 `ETCD_HBASE_JDK21_HOME` 指向已安装的 JDK 21；`scripts/dev/configure-toolchain.py` 只生成仓库本地的 ignored toolchain 文件。脚本不得改写用户级 `~/.m2/toolchains.xml`。公开依赖解析只使用 Maven Central 的 HTTPS 端点。

```sh
scripts/dev/configure-toolchain.py --java-home "$ETCD_HBASE_JDK21_HOME"
scripts/dev/mvn21.sh -version
scripts/dev/mvn21.sh clean verify
```

Maven 运行进程和 Maven compiler/test toolchain 都检查 Java 21。版本范围、`LATEST`、SNAPSHOT 插件或运行依赖、preview flags 均不允许。新增或升级依赖时，同步更新锁文件、来源、SHA-256 和选择理由，并在 Java 21 下重跑 reactor 验证。

## 公开验证与运行包

`.github/workflows/m0.yml` 将协议、Java 21 构建、Go 原生 etcd 对照、Go/Java 互通、public HBase 探针和运行镜像拆成独立检查。GitHub Actions 均固定到完整 commit SHA；Java、Go、Maven Wrapper 和 Maven 插件由本仓库的工具链锁及 Maven lock 校验。`scripts/verify/m0.sh --scope public` 按同一组脚本顺序执行本地公开验证，并在 `.artifacts/m0/public/<run-id>/INDEX.json` 写入机器可读证据索引和实际测试/跳过数量。

`scripts/dev/m0-server.sh` 从安装到仓库内隔离 Maven 仓库的模块 JAR 启动服务，运行 classpath 不包含 `target/classes`。`server/src/main/docker/Dockerfile.m0` 使用按摘要固定的 Temurin 21 JDK/JRE 镜像，运行用户为非 root；`scripts/verify/runtime-image.sh` 检查镜像进程启动、打包服务返回明确的 `UNIMPLEMENTED`，并能干净退出。该镜像只用于 M0 服务骨架运行检查。

public HBase 集成检查必须实际执行 `LocalHBaseProbeIT` 且不能跳过。它只证明公开、无认证测试配置下的探针行为，不代表 B02/B03/B06 指定的目标 HBase、身份或 TLS 验收。M0 公开证据索引会把 target HBase 状态记录为等待这些输入。

Go/Java wire 检查通过固定 Go clientv3 与 raw apiv3 调用 Java 骨架，并用 test-only fixture 验证客户端错误映射、deadline 和取消。Watch 取消用例发送真实 `WatchCreateRequest`，由 Java 端确认请求字段及 deadline 触发后的 scope 清理；driver 进程有独立总时限并检查正常退出。`transport`/`transport-fixture` 记录仅证明协议路径及失败终止；它们不证明完整 etcd 语义或目标 HBase 身份。native etcd 对照单独记录为 `native-etcd`。

## 插件和运行库

Maven 插件、gRPC、Protobuf/protoc、Guava、SLF4J、Hadoop、HBase、JSON/YAML 与测试库采用锁文件中的精确版本。gRPC 与 Protobuf 使用各自同一发布线的版本；protoc 及 protoc-gen-grpc-java 由 Maven 插件按平台 classifier 获取。服务传输使用 `grpc-netty-shaded`，其 Netty 实现随 gRPC 版本固定并保持 relocation，不在模块间暴露 Netty 包。

Go testkit 使用 Go 1.27.2；macOS arm64、Linux x64 和 Windows x64 归档摘要及 etcd client v3.5.23 的 module sums 记录在锁文件中。M0 public workflow 已运行原生 etcd 对照及 Go/Java wire 检查；这些结果证明已声明的 testkit 和骨架互通，不代表完整 etcd 业务语义。

Maven reactor 的 dependency convergence 由 Enforcer 检查；server 依赖树通过 root reactor 生成，确保本项目模块及其运行依赖都展开。当前锁定的公开候选包括 gRPC Java 1.84.1、protobuf-java 4.36.2、Guava 33.7.2-jre、HBase 2.6.6-Hadoop 3 与 Hadoop 3.4.3。传递依赖请求的 Guava/Protobuf 版本由 dependencyManagement 收敛到这些锁定版本；POM 未配置手工 exclusions。HBase 候选的 Java 21 运行兼容性仍需按下文 B02 目标输入验证。

## HBase 候选与目标兼容性

公开依赖候选为 `org.apache.hbase:hbase-shaded-client-byo-hadoop:2.6.6-hadoop3`，配套 Hadoop `3.4.3`。该候选来自 Apache HBase 2.6.6 的 Hadoop 3 构建。`LocalHBaseProbeIT` 在 Temurin 21 下启动隔离的无认证临时 HBase，实际执行原始字节 Put/Get、CAS、Scan、独立连接读回和连接重建读回；public CI 要求该测试真实执行且不能 skip。这是当前项目对该 public-local 组合的运行证据。Apache HBase 支持矩阵将 Java 21 支持标记为 HBase 2.6.8+ 和 3.0.1+；截至 2026-10-13，公开下载页列出的 HBase 2.6/3.0 GA 版本尚未达到这些门槛。因此，本候选**未获官方 Java 21 运行支持**，不得据此宣称官方支持或目标集群兼容。

目标 HBase/Hadoop 发行版、认证模式和运行约束由 B02 提供后再确定目标依赖组合。目标输入未到位前，不把公开候选作为目标环境的替代，不运行或宣称目标 HBase 验收。

## 官方来源

- [Eclipse Temurin 21 发布页](https://adoptium.net/temurin/releases?version=21)
- [Apache Maven 3.9.16 发布说明](https://maven.apache.org/docs/3.9.16/release-notes.html)
- [Apache Maven Wrapper 3.3.4 与 distributionSha256Sum](https://maven.apache.org/tools/wrapper/maven-wrapper-plugin/wrapper-mojo.html)
- [Maven 官方插件列表](https://maven.apache.org/plugins/index.html)
- [gRPC Java 官方依赖与生成配置](https://github.com/grpc/grpc-java)
- [Protobuf 版本支持与 protoc/Java 版本对应关系](https://protobuf.dev/support/version-support/)
- [Apache HBase Java 支持矩阵](https://hbase.apache.org/docs/configuration/basic-prerequisites/)
- [Apache HBase 发布下载页](https://hbase.apache.org/downloads/)
- [Go 官方发布历史](https://go.dev/doc/devel/release)
- [Go 官方下载与校验和](https://go.dev/dl/)
