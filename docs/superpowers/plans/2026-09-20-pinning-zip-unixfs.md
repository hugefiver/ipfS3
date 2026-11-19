# ZIP 批次、发布与 UnixFS 目录根

[README](../../../README.md#durable-ipfs3-import) 提供签名请求及响应的使用说明。[ZIP 安全边界](../../../src/zip/README.md) 说明路径、完整性和输入预算；[测试说明](../../testing.md#zip-publication-and-recovery) 列出发布与重放用例。

## 发布与重放

直接 PUT、分段上传 Complete 和持久导入都持久保存 ZIP 批次与最终清单。清单绑定批次实际发布的精确对象版本和 CID。直接 PUT 和分段上传保留重复路径的最后一个成功结果；导入拒绝重复目标或与源键冲突的目标。旧版 ZIP 合同保留源归档对象、CID ETag、VersionId 和原有结果。

v2 将源包发布、提取文件发布和远端 pin 选择分开。关闭源包发布时，必须提供显式 ZIP token，且不能关闭批次结果。此时源键上的既有对象不变，响应不生成虚拟 ETag 或 VersionId。至少启用一种发布。源包未发布且没有任何成功输出的分段上传 Complete 会持久保存批次失败；相同 parts 的重放返回原 4xx，批次仍可通过 UploadId 查询。导入的 `202 Accepted` 只表示任务已受理。

重放核对原始 PUT 内容或分段身份，以及冻结的请求描述与控制项。绑定导入源之后，恢复使用已绑定的内容；绑定前中断可能重新获取。相同请求返回持久结果，不重新发布对象或重复添加 pin 引用。源包、提取文件与 pin 引用的发布受同一所有权事务保护。

## UnixFS 目录根

根默认开启，ZIP 请求可用 `ipfs-s3:zip-root=true|false` 覆盖。分段上传初始化和导入受理时冻结此选项。根只包含最终发布成功的 ZIP 相对文件路径及必要的父目录，保留大小写和 Unicode，不包含归档、S3 目标前缀、失败项或空目录。构建复用已经写入 Kubo 的叶 CID。

关闭时状态为 `disabled`；没有成功文件时状态为 `empty`，不返回根 CID。成功构建可能是 `complete` 或 `partial`；构建失败为 `failed` 或 `retryable`，不返回未经验证的 CID，也不回滚已经发布的 S3 对象。根的持久身份独立于归档对象。只重试根构建时，使用保存的清单，不重新解压或发布文件。

## 远端 pin

远端 pin 不决定 S3 发布结果，也不由目录根状态推断。自动输出 pin 只针对已发布的成功提取文件，受 `[decompress_zip].auto_pin_outputs` 和服务器策略约束。v2 的 `x-ipfs3-zip-targets` 可在授权范围内选择已发布的类别。根是本地快照，不会自动发往远端，也不会因对象删除而触发物理回收。操作及状态查询见 [Remote Pinning](../../../README.md#remote-pinning)。
