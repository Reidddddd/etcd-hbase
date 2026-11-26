# 认证配置与部署清单运维

## 1. 身份凭据版本绑定

安全 endpoint 的客户端 profile 可配置 `credential_versions`。每项包含 `principal`、正整数 `generation` 和 `credential_reference`；引用指向受部署方控制的凭据材料，格式与现有凭据引用相同：第一行是完整主体名，换行后的内容是凭据。配置只保存引用和版本，不保存密码。

认证时，提交的主体和凭据必须与引用中的材料一致，还必须通过实际后端认证和表权限检查，才能签发带该版本的身份。没有版本绑定的主体不能建立持久认证会话；引用不可读时返回依赖不可用。服务后台身份仍使用其独立配置，不使用这些客户端绑定。

同一凭据族的版本必须由可信的部署凭据供应方维护，并在所有副本之间可比较：轮换时先发布新的独立凭据引用并递增版本，不能原地修改旧版本的材料或重复使用版本。旧副本不得覆盖已推进的持久授权状态。当前授权代次在新版本通过认证后推进；仅修改配置本身不构成已经完成的撤销。

凭据族由 `trust_domain`、provider、`backend_identity_domain` 和经过后端验证的主体共同确定；`credentialGeneration` 仅在同族内可比较。authority 的 scoped 摘要同时绑定该族与表组、连接 profile。通过完整 registry 配置更新及旧表组屏障安装后切换 `backend_identity_domain`，新族可从较低的凭据版本开始；同族的旧版本仍被拒绝。旧族 authority 保留，切换回旧域也须重新认证并推进授权代次，原 token 不会恢复有效。

当前未发布的 J51 将 scoped 摘要前缀从 `etcd-hbase-auth-subject-v1` 显式改为 `etcd-hbase-auth-subject-v2`，并纳入 `backend_identity_domain`。原实验摘要下的 token 失败关闭，客户端须重新 Authenticate；这不是对旧 token 的兼容升级。不提供双摘要读取或自动迁移。auth-control 记录编码、类别编号及授权屏障语义保持不变；已有实验记录仍按原格式解码，身份激活后必须匹配当前摘要才能使用。实验环境须同时更新所有副本，避免混用摘要算法。

## 2. 持久表组清单

部署 registry 是表组清单、配置代次和撤销进度的唯一持久入口；这些状态在同一行上进行精确 CAS。`AuthStatus` 只读取该清单的表组投影，不维护独立的配置行或写入接口。

管理员负责维护同一 `trust_domain` 下的完整表组清单，并确保每个副本使用一致的身份域和凭据代次。服务不会根据单个副本自动推断全量清单。清单遗漏表组会破坏跨表组撤销所依赖的范围，新增或删除表组必须通过下述更新流程。

所有会话服务都依赖预置的 registry、目标表组屏障、身份域和配置摘要。首次登录与凭据轮换使用同一部署操作：对同一后端凭据族的全部启用表组持久登记目标、安装屏障并确认释放后，才签发会话。各运行时只推进本组目标；其他组未完成或屏障安装失败时保留原操作，后续运行时从同一 registry 恢复，不退回旧授权代次。

先创建认证控制表及各表组，保存表组初始化时输出的 `cluster_id`（十进制 uint64；不是 HBase 的 cluster ID）。准备 JSON 文件，列出该信任域的全部表组及其目标运行配置，例如：

```json
{
  "groups": [
    {"configuration_file": "/etc/etcd-hbase/group-a.json", "table_group_id": "group-a", "cluster_id": "42", "enabled": true},
    {"configuration_file": "/etc/etcd-hbase/group-b.json", "table_group_id": "group-b", "cluster_id": "43", "enabled": true}
  ]
}
```

每个 `configuration_file` 必须是该组实际生效的完整服务配置的绝对路径；运维执行环境中的配置覆盖须与运行副本一致。端点 client profile 需要 `backend_identity_domain` 和 `credential_versions`。命令按照服务使用的算法计算数据集和配置摘要，并检查各文件指向相同的认证控制表；它不能验证清单是否遗漏了其他部署，也不能替管理员验证填写的 `cluster_id`。

以下命令使用 `--config` 中认证控制表身份连接 HBase；该身份必须由部署方授权并拥有对应的表权限。`--m3-config` 提供连接超时等运行参数，不启动监听器：

```bash
scripts/dev/launch-server.sh auth-registry plan --config /etc/etcd-hbase/operator.json --m3-config /etc/etcd-hbase/activation.json --inventory /etc/etcd-hbase/inventory.json
scripts/dev/launch-server.sh auth-registry initialize --config /etc/etcd-hbase/operator.json --m3-config /etc/etcd-hbase/activation.json --inventory /etc/etcd-hbase/inventory.json
scripts/dev/launch-server.sh auth-registry status --config /etc/etcd-hbase/operator.json --m3-config /etc/etcd-hbase/activation.json
scripts/dev/launch-server.sh auth-registry update --config /etc/etcd-hbase/operator.json --m3-config /etc/etcd-hbase/activation.json --inventory /etc/etcd-hbase/next-inventory.json --expected-generation 1
```

`plan` 只计算清单，`initialize` 只在登记不存在时创建；`update` 只预约变更，不表示完成。先用 `status` 确认预期代次，更新时保持所有旧表组运行，直到它们各自安装屏障；后台工作者会恢复中断的进度。`status` 中 `configuration_update` 消失且 `configuration_generation` 递增后，再切换到新配置或移除旧表组。配置更新期间授权和 `AuthStatus` 暂不可用；旧配置的副本在切换后不再就绪。

写入超时或连接失败可能是未知结果：先检查 `status` 中的预约、目标和替换清单，确保原操作已停止或完成后再决定是否重试。一次读取仍可能早于迟到写入，不能把暂未看到操作视作写入肯定失败。状态输出只含绑定摘要及进度，不含凭据。`AuthStatus.authRevision` 是此清单的持久配置代次，不是原生 etcd RBAC revision。

## 3. Lease 启用条件

生产配置启用 Lease 时，每个监听表组都必须配置安全认证、身份域、版本化客户端凭据及仅限该组的后台身份。启动还会检查实际后台表权限、授权屏障、恢复容量和时钟健康；条件不满足时拒绝就绪。时钟的不确定度与漂移上限是部署方提供的假设，进程内检查不能证明初始绝对时钟误差满足这些假设。读取缓存仍遵循其独立启用门槛。
