# Decompress-Zip 上传扩展设计

作者：hugefiver

`decompress-zip=<prefix>` 是签名 S3 请求的扩展。直接 PUT 携带此参数和 ZIP 请求体；分段上传在 CreateMultipartUpload 时保存解压选项，Complete 时按保存的选项执行。提取文件使用同一桶，键由目标前缀和安全相对路径组成。相同扩展也可用于[持久导入](2026-07-29-ipfs3-import-design.md)。不带解压选项的请求保持标准 S3 上传语义。

## 输入与输出

归档与所提取文件流式写入 Kubo，内容不用整体缓冲。支持可流式处理的 Stored 和 Deflate 文件；入口检查路径、最终键、格式、输入预算及 entry 的 CRC32 和长度。格式边界、计数和资源限额见 [ZIP 安全边界](../../../src/zip/README.md)。ZIP 不支持 SSE-S3 或 SSE-C，也不支持密码保护的归档、跨桶提取或其他归档格式。

分段 Complete 的 XML 控制体上限为 4 MiB，在处理对象内容前读取并解析。每个 `Part` 可带各一个 `ChecksumCRC32`、`ChecksumCRC32C`、`ChecksumCRC64NVME`、`ChecksumSHA1` 和 `ChecksumSHA256` 字段。解析保留其文本值，完成逻辑不校验这些值；part 身份仍按对应上传及用于 Complete 的 part 列表处理。签名覆盖影响请求语义的 query 参数，包括 `decompress-zip`；使用预签名 URL 时需在生成签名之前加入这些参数。

旧版 ZIP 发布源归档及各个成功文件，默认返回 `DecompressZipResult`；`decompress-zip-result=false` 保留标准 S3 响应。v2 可以仅发布源包、仅发布解压文件或同时发布，返回 `ZipV2Result`。源不发布时保留该键上已有的对象，不生成虚拟 ETag 或 VersionId。每个实际发布对象以其 CID 为 ETag。请求选项、token 和重放规则见 [README](../../../README.md#zip-v2-request-and-replay-contract)。

## 文件和批次的提交边界

全局输入拒绝不发布任何新对象；可归于单个文件的处理失败进入批次结果，其他成功文件可继续。写入 Kubo、提取内容和收集结果在写事务外执行。发布事务检查当前目标所有权，提交适用的源包、成功文件的精确版本、批次清单、pin 引用，并结算同一任务或分段上传。重复完成或提交结果不明时，用当前任务或上传尝试的持久结果判断，不把同一键上的其他写入当作本次成功。仍可能被其他键或任务引用的归档、entry 和 multipart part CID 在失败或中止后保持本地 pin。

远端 pin 以成功发布的文件版本为单位，由文件引用记录需求，供应商操作在发布事务之后异步执行。供应商 ACK 只结算该备份操作；目录根状态和对象发布结果均不依赖它。自动 pin、重放及 UnixFS 目录根的有效合同见 [ZIP 批次、发布与 UnixFS 目录根](../plans/2026-09-20-pinning-zip-unixfs.md)。
