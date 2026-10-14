# 第三方软件跟踪清单

本清单列出 M0 锁定的主要公开工具、直接依赖和服务端依赖候选，帮助维护者审查来源与许可。确切 Maven 坐标和归档摘要见 [`build/dependency-lock.json`](build/dependency-lock.json)。Hadoop/HBase 传递依赖尚未形成最终发行包，本文件不是完整的传递依赖通知或 SBOM；发行前需按 [`docs/build/open-source-policy.md`](docs/build/open-source-policy.md) 生成并审阅完整清单。

| 组件 | 锁定版本 | 上游许可 / 来源 |
|---|---:|---|
| Eclipse Temurin JDK | 21.0.12.1+1 | [GPL-2.0 with Classpath Exception](https://github.com/adoptium/temurin21-binaries/blob/master/LICENSE) |
| Go toolchain | 1.27.2 | [BSD 3-Clause](https://go.dev/LICENSE) |
| Apache Maven / Maven Wrapper 与构建插件 | 3.9.16 / 3.3.4 / 见锁文件 | [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) |
| Apache HBase client / Hadoop | 2.6.6-hadoop3 / 3.4.3 | [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) |
| gRPC Java / shaded Netty transport | 1.84.1 / 4.2.16.Final | [Apache License 2.0](https://github.com/grpc/grpc-java/blob/master/LICENSE) / [Apache License 2.0](https://github.com/netty/netty/blob/4.2/LICENSE.txt) |
| Protocol Buffers Java / protoc | 4.36.2 | [BSD 3-Clause](https://github.com/protocolbuffers/protobuf/blob/main/LICENSE) |
| etcd API proto sources | v3.5.23 | [Apache License 2.0](https://github.com/etcd-io/etcd/blob/v3.5.23/LICENSE) |
| gogo proto options | v1.3.2 | [BSD 3-Clause](https://github.com/gogo/protobuf/blob/v1.3.2/LICENSE) |
| Go etcd API / client / server modules | v3.5.23 | [Apache License 2.0](https://github.com/etcd-io/etcd/blob/v3.5.23/LICENSE) |
| Go etcd compatibility client | v2.305.23 | [Apache License 2.0](https://github.com/etcd-io/etcd/blob/v3.5.23/LICENSE) |
| gRPC-Go | 1.79.3 | [Apache License 2.0](https://github.com/grpc/grpc-go/blob/v1.79.3/LICENSE); [NOTICE](https://github.com/grpc/grpc-go/blob/v1.79.3/NOTICE.txt) |
| Go Protocol Buffers / compatibility runtime | 1.36.10 / 1.5.4 | [BSD 3-Clause](https://github.com/protocolbuffers/protobuf-go/blob/v1.36.10/LICENSE) / [BSD 3-Clause](https://github.com/golang/protobuf/blob/v1.5.4/LICENSE) |
| bbolt | 1.3.12 | [MIT License](https://github.com/etcd-io/bbolt/blob/v1.3.12/LICENSE) |
| Kubernetes sigs YAML | 1.2.0 | [MIT License](https://github.com/kubernetes-sigs/yaml/blob/v1.2.0/LICENSE) |
| Google API proto imports | pinned commit in protocol source lock | [Apache License 2.0](https://github.com/googleapis/googleapis/blob/34bb7a118468a6ec56deb31842525971e41be2ac/LICENSE) |
| Guava | 33.7.2-jre | [Apache License 2.0](https://github.com/google/guava/blob/master/COPYING) |
| SLF4J | 2.0.20 | [MIT License](https://www.slf4j.org/license.html) |
| Jackson / SnakeYAML | 2.22.3 / 2.7 | [Apache License 2.0](https://github.com/FasterXML/jackson/blob/2.22/LICENSE) / [Apache License 2.0](https://bitbucket.org/snakeyaml/snakeyaml/src/master/LICENSE.txt) |
| JUnit / Mockito / AssertJ | 6.1.3 / 5.24.0 / 3.27.7 | [EPL 2.0](https://github.com/junit-team/junit5/blob/main/LICENSE.md) / [MIT](https://github.com/mockito/mockito/blob/main/LICENSE) / [Apache 2.0](https://github.com/assertj/assertj/blob/main/LICENSE) |

协议 proto 文件的来源 commit 与逐文件 SHA-256 见 [`protocol/upstream-lock.json`](protocol/upstream-lock.json) 和 [`protocol/UPSTREAM.md`](protocol/UPSTREAM.md)；生成代码由这些输入产生。I07 的项目许可决定和 I08 的公开制品命名/发布目的地尚未提供；本跟踪表不替代这些维护者决定。新增第三方组件时，先核对所用版本的上游许可与 NOTICE，再扩展本表及机器锁文件。
