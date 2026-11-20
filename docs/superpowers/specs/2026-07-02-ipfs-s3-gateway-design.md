# IPFS S3 Gateway 设计

网关通过 axum 接收 HTTP 请求，由 s3s 校验 SigV4 并分发 S3 操作。Kubo 存储不可变的 UnixFS 内容，SQLite 或 PostgreSQL 保存桶、键、对象版本和任务状态。S3 以数据库已发布的版本为准；同一个 CID 可对应多个键或版本。对象的 ETag 是 CID；明文内容在目标 IPFS 节点能取到该 CID 时可直接读取。加密对象的 CID 指向密文。

## S3 文件与任务

普通 PUT 流式写入 Kubo，取得 CID 并完成本地 pin 后才发布 S3 对象。分段上传先独立写入 part，Complete 按已选 part 的顺序拼接并重新写入一个 UnixFS 文件。part 的 CID 不等同于完成对象；普通 S3 删除、版本删除、分段中止与生命周期清理都不解除主存储的 Kubo pin。

版本、标签、适用的远端 pin 引用，以及需同时提交的分段或任务结果在发布事务中写入。独立键和任务可以并行处理；发生写入竞争时，在受影响的键、版本、上传或任务上校验所有权。下载、上传、解压和供应商调用保持在写事务外。受影响的写入需要相同的提交边界时，例如一个 ZIP 批次的对象及清单，仍原子发布。

[持久导入](2026-07-29-ipfs3-import-design.md)按任务受理。`202 Accepted` 表示任务已持久化，状态请求返回后续进度或终态。任务只发布当前仍拥有的目标；已提交对象始终可以按普通 S3 语义读取。有限重试和租约回收处理暂态故障，无法完成的任务公开其终态。ZIP 输出面向独立文件；[批次发布和 UnixFS 根](../plans/2026-09-20-pinning-zip-unixfs.md)定义批次保留的结果。

[远端 pin](../../pinning-design-guidelines.md)以 S3 文件版本及其到期时间记录需求；ZIP 按成功输出文件创建引用。`(provider, canonical CID)` 共享供应商分配和配额计量。worker 在短事务中领取操作，在事务外提交或清理，再按原领取身份写回结果。供应商 ACK 表示相应操作得到接受；这与 S3 发布、主存储回收以及远端物理副本状态各自独立。未知提交效果保留为未知。

[生命周期](2026-08-26-lifecycle-program-design.md)按对象版本或未完成的分段上传执行。向 cold Kubo 转移时，必须在发布 `STANDARD_IA` 前核实独立 cold 节点上的同 CID 完整内容；这个可读性检查属于存储类别的发布边界，与远端备份 ACK 无关。

## 加密和 Range

普通对象默认明文；`x-amz-server-side-encryption: AES256` 选择 SSE-S3。网关生成对象密钥，用部署的 master key 包装并保存；SSE-C 接收客户提供的密钥，保存由 master key 计算的指纹，不存客户原始密钥。部署需保持 master key 不变。

[AES-GCM 流](../../../src/crypto/chunker.rs)每次初始化一个 cipher，复用至流结束；每个加密块使用新的随机 12 字节 nonce。完整块由 256 KiB 明文、nonce 和 16 字节认证标签组成，末块可以较短。读取时按帧认证后交付明文，与 HTTP 分帧无关。

明文 Range 委托给 Kubo。[加密 Range](../../../src/s3/ops/object.rs) 向存储该版本的 Kubo 请求覆盖指定范围的完整密文帧，认证所读帧后返回所选明文字节。旧 SSE-C 对象首次认领密钥指纹仍需完整认证。对象格式与 CID 不变。

## 身份与计划能力

网关从配置加载用户和桶策略；本地签发的临时凭据继承对应用户的权限。受保护操作按已认证的主体、操作、资源以及适用的版本和对象标签条件求值。CORS 和生命周期配置支持的 `ExpectedBucketOwner` 独立比较桶所有者。S3 请求限流属于请求边界，独立于后台供应商调度。

计划中的对象 webhook 以已提交的 PUT 或 DELETE 为事件来源。按对象事件记录交付任务，HTTP 交付在提交后执行，目的端 ACK 只结算该次交付。事件规则、订阅接口和其他未交付能力见 [路线图](../../../ROADMAP.md) 的计划范围；当前网关未提供这些接口。

运行配置和公开 API 用法见 [README](../../../README.md)。数据库初始化和离线升级见[数据库指南](../../database-storage.md)。

参考调研：[Kubo 能力边界](../../kubo-s3-feasibility-report.md)、[S3 API 与 Rust 生态](../../research/s3-ipfs-gateway-feasibility.md)、[分布式架构](../../ipfs-s3-feasibility-research.md)。
