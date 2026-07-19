# Decompress-Zip 上传扩展 — Ready for implementation

- 日期: 2026-07-07
- 状态: Ready for implementation
- 作者: hugefiver
- 关联: `docs/superpowers/specs/2026-07-02-ipfs-s3-gateway-design.md`

## 1. 目标与范围

### 1.1 项目目标

在现有 S3 兼容网关上增加一个非标准 query 扩展 `decompress-zip`。客户端用 PutObject 或 Multipart Upload 上传 zip archive；网关保存 archive 本身，并在服务端流式解压 archive 内的文件，将每个安全 entry 写入同一 bucket 的指定前缀下。所有对象仍以 IPFS CID 作为 ETag，元数据仍写入 SeaORM store。

### 1.2 MVP 范围

- 支持 PutObject: `PUT /bucket/key.zip?decompress-zip=prefix/`。
- 支持 Multipart Upload: `POST /bucket/key.zip?uploads&decompress-zip=prefix/` 记录目标，Complete 后解压。
- 支持 zip entry 的 `Stored` 与 `Deflate`；拒绝 async stream reader 无法安全处理的格式组合，例如 `Stored + data_descriptor`。
- archive 字节流、entry 字节流与 Kubo 写入均保持流式；不得把整个 archive 或整个 entry collect 到内存。唯一允许缓冲的是 CompleteMultipartUpload XML control body，并且必须通过逐 frame 读取实施 4 MiB 硬上限。
- 发现 Zip Slip、绝对路径、Windows 盘符、反斜杠路径、空文件名、`.` / `..` 逃逸，或任一成功 staged entry 的最终 key 与 archive key 相同等全局拒绝错误时，返回 400，并且不发布本次请求创建的 archive 或 entry DB 记录；已经成功 pin 的 CID 按本文的保守 pin 语义保留。
- 默认返回自定义 `DecompressZipResult` XML；`decompress-zip-result=false` 返回标准 PutObject 或 CompleteMultipartUpload 响应形态。
- 复用现有 SigV4 鉴权、bucket/object store、Kubo add/cat/pin 与 Multipart 校验逻辑。

### 1.3 不在 MVP 范围

- tar、7z、rar 等非 zip 格式。
- 跨 bucket 解压；目标 bucket 必须是 archive 所在 bucket。
- 后台异步任务、任务查询 API、进度通知。
- zip 密码解密。
- 解压炸弹配额、entry 数量上限、总解压字节上限；这些作为后续配置项。
- 对 `decompress-zip` 请求支持 SSE-S3 或 SSE-C。MVP 遇到任何 server-side-encryption 相关 header 都返回 400；非 `decompress-zip` 请求的现有 SSE 行为不变。
- 在全局拒绝后恢复被本次请求覆盖的旧对象版本。MVP 的保证是“不发布本次请求新创建的 latest 对象记录”；目标前缀已有对象时，调用方应避免在恶意 archive 上复用生产前缀。
- CID 引用计数、独占 pin 租约和安全 GC。本次保守 pin 修订覆盖 archive、entry、Multipart root 以及所有 Multipart part CID，但不新增引用计数 migration 或引用跟踪；Task 1 已规划的 decompress metadata migration 保持不变。替换、失败、Complete 或 Abort 遗留的冗余 pin 交由后续具备全局可达性判断的 GC/引用计数工作处理。

### 1.4 验收标准

**PutObject + decompress-zip**:

1. `PUT /bucket/key.zip?decompress-zip=prefix/` 上传合法 zip，返回 `200 OK` 和 `DecompressZipResult` XML。
2. archive 以 `s3://bucket/key.zip` 发布，ETag 为 archive CID。
3. zip 内 `foo/bar.txt` 解压到 `s3://bucket/prefix/foo/bar.txt`，ETag 为 entry CID。
4. zip 内 `../escape.txt`、`/etc/passwd`、`C:\Windows\x`、`dir\file.txt`、空 entry 名都会使整个请求返回 `400 InvalidParameterValue`。
5. 全局拒绝时，本次请求创建的 archive DB 记录和 entry DB 记录不会保持可见；archive 与已成功 staged entry 的 pin 保留，并且失败路径不得调用 `pin_rm` 解除这些 CID。
6. 单个安全 entry 的 Kubo/DB 写入失败记录到 `Failures`，请求仍返回 `200 OK`，已经成功的其他 entry 和 archive 保留。
7. `decompress-zip-result=false` 执行同样的解压和发布动作，但返回标准 PutObject 空 body，ETag header 仍是 archive CID。
8. 不含 `decompress-zip` 的 `PUT /bucket/key` 继续走标准 s3s PutObject，行为不变。
9. 空 prefix 允许解压到 bucket 根，但若任一成功 staged entry 的最终 `entry.key == archive_key`，PutObject 在 archive 和任一 entry DB publish 前返回 `400 InvalidParameterValue`，message 固定为 `zip entry collides with archive key: {archive_key}`；archive/entry latest 均不存在，archive 与所有成功 staged entry CID 的 pin 保留且不调用 `pin_rm`。只有 `ExtractOutcome.entries` 中的成功 entry 参与检查；没有 staged CID 的失败 entry 不参与碰撞判断。

**Multipart + decompress-zip**:

10. `POST /bucket/key.zip?uploads&decompress-zip=prefix/` 创建 Multipart Upload，并在 upload 记录中持久化 `decompress_zip_target=prefix/` 与 `decompress_zip_result=true`。
11. UploadPart 不感知解压，行为与标准 Multipart 一致；同一 `(upload_id, part_number)` 的重传在新 CID `pin_add` 成功后以单条原子 upsert 替换 DB part 记录。旧 part CID 与 upsert 失败时的新 CID 均不得调用 `pin_rm`。
12. CompleteMultipartUpload 先执行现有 part 校验、拼接与 root CID add/pin；解压 upload 在确认没有全局拒绝后，才以单个 SeaORM transaction 发布 archive DB 记录并删除 upload（级联删除 parts）。无论 transaction 成功、失败或 outcome unknown，均不得对任一 part CID 调用 `pin_rm`；单 part 时 `part_cid == root_cid`，且任意 part CID 都可能被其他对象共享。
13. Multipart 解压在调用 `finalize_completed_multipart_archive` 前执行与 PutObject 相同的 archive/entry key 碰撞检查。若任一成功 staged `entry.key == completed.key`，返回相同的固定 `400 InvalidParameterValue`；不 finalize，不发布 archive 或 entry latest，upload/parts 原样保留以允许重试，root、part 与所有成功 staged entry CID 的 pin 均保留且不调用 `pin_rm`。
14. Complete 默认返回 `DecompressZipResult` XML；`decompress-zip-result=false` 返回标准 `CompleteMultipartUploadResult`。
15. AbortMultipartUpload 只在 bucket/key/upload 校验后删除 upload，依靠外键级联删除 part DB 记录，不触发解压且不对 part CID 调用 `pin_rm`。
16. `complete_multipart_upload_inner` 每次调用生成独立 `completion_attempt_id`。两个针对同一 `upload_id` 的并发 Complete 即使读取相同 upload/parts 并生成、pin 同一个 root CID，也各自使用不同 attempt ID；失败请求不得解除 root 或 part pin，最终恰有一个 latest completed object，其 row ID 等于 winner attempt ID，loser attempt row 不存在。
17. upload 持久化的 `object_id` 仅作为 `encryption_object_id` 继续用于 SSE part/root key 与 nonce 上下文，不得作为 Complete 提交或对账 identity。SeaORM transaction body error 被视为已回滚；`TransactionError::Connection` 被视为 commit outcome unknown，必须按当前 `completion_attempt_id`、`upload_id`、`root_cid` 精确读后对账。
18. 若 attempt A 返回 `OutcomeUnknown` 但未提交，而 attempt B 用不同 ID 提交成功，A 的对账必须因 A row 不存在分类为 `NotCommitted` 并返回错误；不得把 B 的 latest row 认作 A 的提交结果。所有分支均保留 root 与 part pin。
19. CompleteMultipartUpload XML 最大为 `4 * 1024 * 1024` bytes。声明超限 `Content-Length` 与 HTTP/1.1 chunked/no `Content-Length` 的超限请求都返回 `400 InvalidRequest`，message 固定为 `CompleteMultipartUpload XML exceeds 4 MiB`，并且在 Kubo add/cat/pin 或 archive/upload/part DB mutation 前终止；恰好 4 MiB 允许进入 XML parser。
20. Presigned URL 的 `decompress-zip` 与 `decompress-zip-result` 必须在 SigV4 canonical query 计算前已存在。签名后修改或追加会改变最终 decompression 语义的 query 必须返回 `403 SignatureDoesNotMatch`，且不触发 Kubo/DB mutation；header Authorization 与 presigned query 两种 SigV4 路径都保留真实 HTTP 覆盖。

## 2. 接口设计

### 2.1 行为分支

`decompress-zip` 是行为层面的纯增量扩展。无该参数的 PutObject 继续完全绕过自定义 route；CreateMultipartUpload 仅持久化默认的无解压选项。CompleteMultipartUpload 请求本身不携带 `decompress-zip`，而同步的 `S3Route::is_match` 无法查询 upload 记录，因此所有 `POST ?uploadId=...` 都经过 bounded raw Complete route。该 route 在鉴权和解析后读取 upload metadata：无解压目标时复用标准 Complete inner/finalize 流程并返回标准 `CompleteMultipartUploadResult`，有解压目标时才执行提取。所有 raw Complete 分支都必须接受并原样保留 s3s 0.14 `CompletedPart` 的五个合法可选 per-part checksum XML 字段：`ChecksumCRC32`、`ChecksumCRC32C`、`ChecksumCRC64NVME`、`ChecksumSHA1`、`ChecksumSHA256`。这里“原样保留”定义为保留 XML 字符内容在实体/字符引用解析后的 `String` 值，包括明确的前后空白和引用边界；结构缩进/换行仍是字段外 whitespace，不进入字段值。五个合法 checksum 的无属性自闭合元素映射为 `Some("")`，与 s3s 0.14 的 `String` XML 语义一致。route 不验证这些 checksum 值（既有 operation 也不验证），但每个字段在一个 `Part` 中至多出现一次；未知元素、重复字段（包括自闭合与 Start/End 混用）、错误 root/嵌套、field 内 Empty 与字段外内容仍返回 `MalformedXML`。标准 Complete 的可观察 S3 行为保持不变，但不声称绕过新的 bounded collector 和严格 XML parser。

| 请求 | 路由 | 行为 |
|---|---|---|
| `PUT /bucket/key` | 标准 s3s PutObject | 标准 S3 语义，ETag=CID |
| `PUT /bucket/key?decompress-zip=prefix/` | `DecompressZipRoute` | 存 archive、解压 entry、返回 `DecompressZipResult` |
| `PUT /bucket/key?decompress-zip=prefix/&decompress-zip-result=false` | `DecompressZipRoute` | 存 archive、解压 entry、返回标准 PutObject 空 body |
| `POST /bucket/key?uploads&decompress-zip=prefix/` | 标准 CreateMultipartUpload 解析额外 query | 创建 upload 并记录解压目标 |
| `POST /bucket/key?uploadId=...` 且 upload 记录无解压目标 | `DecompressZipRoute` raw Complete 分支 | bounded 解析后执行标准 Complete inner/finalize，返回标准 `CompleteMultipartUploadResult` |
| `POST /bucket/key?uploadId=...` 且 upload 记录含解压目标 | `DecompressZipRoute` raw Complete 分支 | 完成 archive 后解压，按 result flag 返回响应 |

#### Raw Complete ETag parity

raw parser 对 `ETag` 使用 s3s 0.14 的 typed HTTP-ETag 规则，而不是裁剪字符串：`ETag::parse_http_header` 成功时保留 `Strong` 或 `Weak` 类型；`InvalidFormat` 回退为实体/字符引用已解码但未经 trim/strip 的 `ETag::Strong` 原始 String；`InvalidChar` 返回 `MalformedXML`。无属性的 `<ETag/>` 与 s3s 的空 `String` 语义一致，解析为 `Strong("")`；重复 ETag 仍是 `MalformedXML`。因此非 decompress 的标准 Complete 在经 raw route 后仍可接受 XML-escaped `W/"<actual-etag>"` 并通过 operation 的 `.value()` 使用实际 part ETag，其他标准可观察行为不变。

### 2.2 Query 参数

- `decompress-zip=<target-prefix>`: 必填且值可以为空字符串。空字符串表示解压到 bucket 根路径。
- `decompress-zip-result=false`: 可选。仅字符串 `false` 关闭自定义结果 body；缺失或其他值均按 `true` 处理。
- query name/value 只解码一次，并与 s3s 的 SigV4 canonical query 保持相同 form 语义：raw `+` 表示空格，`%2B` 表示字面 `+`。因此已签名的 `%20` 改写为 raw `+` 不得在认证通过后改变解压目标语义。

`target-prefix` 规则:

- 使用 `/` 作为分隔符。
- 不允许以 `/` 开头。
- 不允许 `\`、Windows 盘符、`.` 或 `..` 段。
- 非空 prefix 规范化后以 `/` 结尾；例如 `prefix` 存为 `prefix/`。

### 2.3 路径映射规则

| zip entry 名 | target-prefix | 最终 S3 key | 结果 |
|---|---|---|---|
| `foo.txt` | `2026/` | `2026/foo.txt` | 写入对象 |
| `dir/foo.txt` | `2026/` | `2026/dir/foo.txt` | 写入对象 |
| `foo/bar/` | `2026/` | 无 | 目录条目跳过 |
| `../escape.txt` | 任意 | 无 | 全局拒绝 400 |
| `/etc/passwd` | 任意 | 无 | 全局拒绝 400 |
| `C:\Windows\x` | 任意 | 无 | 全局拒绝 400 |
| `dir\file.txt` | 任意 | 无 | 全局拒绝 400 |
| `archive.zip` | 空字符串，archive key=`archive.zip` | `archive.zip` | archive/entry key 碰撞，全局拒绝 400 |

重复 entry key 沿用 PutObject 覆盖语义：同一次成功请求内后出现的 entry 覆盖先出现的 entry，最终 latest 对象是最后一个成功 entry。

archive key 碰撞不沿用覆盖语义。route 层提供共享 helper `reject_archive_key_collision(archive_key: &str, entries: &[ExtractedEntry]) -> S3Result<()>`，只扫描 `ExtractOutcome.entries` 中已经成功 staged 且拥有 CID 的 entry。若任一 `entry.key == archive_key`，helper 返回 `InvalidParameterValue`，message 固定为 `zip entry collides with archive key: {archive_key}`。PutObject 必须在 archive DB publish 和任何 entry DB publish 之前调用；Multipart 必须在 `finalize_completed_multipart_archive` 和任何 entry DB publish 之前调用。`ExtractOutcome.failures` 中没有 staged CID 的失败 entry 不参与碰撞检查。

### 2.4 响应

默认响应是 XML，内容类型 `application/xml`，archive CID 同时放入 HTTP `ETag` header。

```xml
<DecompressZipResult>
  <ArchiveKey>2026/archive.zip</ArchiveKey>
  <ArchiveETag>QmArchiveCID...</ArchiveETag>
  <ArchiveSize>1048576</ArchiveSize>
  <ExtractedCount>2</ExtractedCount>
  <FailedCount>0</FailedCount>
  <Entries>
    <Entry>
      <Key>prefix/foo.txt</Key>
      <ETag>QmFooCID...</ETag>
      <Size>512</Size>
    </Entry>
    <Entry>
      <Key>prefix/dir/bar.txt</Key>
      <ETag>QmBarCID...</ETag>
      <Size>1024</Size>
    </Entry>
  </Entries>
  <Failures/>
</DecompressZipResult>
```

单个 entry 失败时仍返回 `200 OK`，失败详情放在 `Failures` 中。

```xml
<Failure>
  <EntryName>bad/corrupt.txt</EntryName>
  <Code>InternalError</Code>
  <Message>decompress stream error: invalid stored block lengths</Message>
</Failure>
```

全局拒绝使用标准 S3 error XML，HTTP 状态为 400。archive/entry key 碰撞固定返回 `<Code>InvalidParameterValue</Code>` 与 `zip entry collides with archive key: {archive_key}` message。

```xml
<Error>
  <Code>InvalidParameterValue</Code>
  <Message>zip entry '../escape.txt' escapes target prefix</Message>
</Error>
```

## 3. 架构设计

### 3.1 PutObject archive-first 流程

MVP 固定采用 archive-first 策略，不实现双消费者 tee。

```text
PUT /bucket/key.zip?decompress-zip=prefix/
  │
  ▼
s3s prepare + SigV4
  │
  ▼
DecompressZipRoute::call(S3Request<Body>)
  ├─ 校验 query、bucket/key、bucket exists、拒绝 SSE header
  ├─ req body → Kubo stream_add(pin=false) → pin_add(archive_cid)
  ├─ Kubo stream_cat(archive_cid) → async_zip stream reader
  ├─ 逐 entry sanitize；目录跳过；文件 entry 通过 duplex bridge 流式写 Kubo
  ├─ 解压完成后、任何 DB publish 前扫描成功 entries 与 archive key 碰撞
  ├─ 若出现路径/格式/碰撞全局拒绝: 保留 archive/entry pins，不 upsert DB，返回 400
  ├─ 若无全局拒绝: upsert archive 和成功 entries；失败 entries 写入 result
  └─ 根据 decompress-zip-result 返回 XML 或标准空 body
```

选择 archive-first 的原因:

- `s3s::Body` 是一次性消费流；tee 需要复杂背压协调。
- Kubo 已经是本地内容寻址存储，先 add 再 cat 同一 CID 简单可靠。
- archive CID 在解压失败时保守保留；当前 Kubo pin 架构没有能证明该 CID 独占的租约。
- 不 collect archive，不牺牲大文件能力。

### 3.2 Multipart 流程

```text
CreateMultipartUpload (?uploads&decompress-zip=prefix/)
  └─ 标准 create + 记录 decompress_zip_target/decompress_zip_result

UploadPart
  └─ 完全标准，part 不做解压；pin_add(new) → 原子 upsert part row；不 unpin old/new CID

CompleteMultipartUpload (?uploadId=...)
  ├─ upload 记录无 decompress target: 标准 complete → archive DB upsert → upload 删除 → 返回标准 CompleteMultipartUploadResult
  └─ upload 记录有 decompress target:
       校验 parts → 拼接 → root CID → pin → stream_cat(root_cid) → 解压 entries
       ├─ finalize 前扫描成功 entries 与 archive key 碰撞
       ├─ 全局拒绝: 保留 root/entry staged pins；保留 upload/parts；返回 400；不发布 archive DB 记录
       └─ 成功/partial failure: 单 transaction 完成 archive DB upsert + upload 删除/parts 级联删除 → entry DB upsert → 返回 XML 或标准 CompleteMultipartUploadResult；part pins 保留
```

UploadPart 的 DB 写入必须从当前“`pin_rm(old)` + `delete_part` + `insert_part`”改为：新 CID `pin_add` 成功后，对 `(upload_id, part_number)` 执行单条 SeaORM/SeaQuery `ON CONFLICT DO UPDATE`，原子更新 `cid/size/etag/uploaded_at`。upsert 失败时返回错误并保留新 pin；旧 DB row 保持不变。替换成功、失败、Complete 与 Abort 都不解除旧或新 part CID。该变更复用现有复合主键，不增加 migration。

Complete 解压读取 root CID。root CID 是 Complete 时重新 `stream_add` 得到的完整 UnixFS 文件，但内容寻址结果并不保证与 part CID 不同：单 part archive 可以出现 `part_cid == root_cid`，part CID 也可能同时被已有普通 object 或其他 upload 引用。Kubo recursive pin 以 CID 为单位且没有引用计数；同一个 root CID 还可能被两个同-upload 并发 Complete 同时生成并 pin。因此，在当前架构无法证明独占时，任何 UploadPart 替换/DB 失败、Complete 成功/失败/outcome unknown 或 Abort 都不得向 `/pin/rm` 提交涉及的 part/root CID。安全优先级是绝不解除可能支撑已提交对象的 pin；遗留冗余 pin 是可接受的，后续由具备引用计数或全局可达性证明的安全 GC 处理。

archive latest 写入与 upload 删除（级联 parts）必须在一个 SeaORM transaction 内完成，不再使用 transaction 后的 `object::delete_latest` 补偿。transaction body 返回错误并由 SeaORM 成功 rollback 时，返回明确的 `RolledBack`；commit/begin/rollback 等连接阶段错误返回 `OutcomeUnknown`。两个 `CommitCompletedUploadError` variant 都携带发起该 transaction 的 `completion_attempt_id`，finalizer 必须拒绝与当前 archive attempt 不一致的错误。对于 commit outcome unknown，立即按当前 `completion_attempt_id` 查询 candidate object row，并用该 row 的完整 `bucket`、`key`、`root_cid`、latest/multipart/metadata/encryption 字段及 `upload_id` 做精确对账：

- 每次 `complete_multipart_upload_inner` 一进入即生成 `completion_attempt_id = Uuid::new_v4()`；`CompletedMultipartArchive` 同时保存 `encryption_object_id = upload.object_id` 与 `completion_attempt_id`。前者只进入现有 SSE key/nonce 派生，后者写入 `LatestObjectRow.id` 并作为对账主键。
- 精确 attempt object row 存在（ID 等于当前 `completion_attempt_id`，bucket、key、CID/root、latest/multipart 及其余字段均匹配）且 upload 不存在：`Committed`。
- 当前 attempt object row 不存在：`NotCommitted`，无论 upload 是否已被另一个 attempt 删除；返回原 commit error。不同 attempt ID 的 winner row 不参与当前 attempt 的成功判断。
- 对账查询失败，或当前 attempt row 存在但字段不匹配、object 与 upload 同时存在等混合状态：`Unknown`，返回 outcome-unknown 错误。

普通 `TransactionError::Transaction`、对账后的 `NotCommitted`/`Unknown` 以及其他 finalize error 都保留 root 与 part pins；直接成功或对账为 `Committed` 也保留 part pins。Same-upload 并发 Complete 选择“恰一请求成功”的语义：两个 inner 必须先各自读取相同 upload/parts、得到并 pin `QmRoot`，确认 attempt ID 不同后通过同一个 barrier 真正同时进入 finalize transaction。结果恰一 `Ok` 一 `Err`；winner row ID 等于 winner attempt，loser attempt row 不存在，upload/parts 均已删除，且 `QmRoot`、`QmPart` 的 `/pin/rm` 调用均为零。

Outcome-unknown 隔离另有精确竞态：A 的 commit seam 返回 `OutcomeUnknown` 且不落库，并把 A reconcile 阻塞；B 用不同 `completion_attempt_id` 通过真实 transaction 提交后再释放 A。A 只能查询 A attempt row，因其不存在得到 `NotCommitted` 并返回错误；最终仅 B row 为 latest，A/B 一错一对，所有相关 CID 均无 `/pin/rm`。

### 3.3 s3s Route 约束

s3s 0.14 的自定义 route 使用裸 body，不提供 `PutObjectInput` DTO。

```rust
impl S3Route for DecompressZipRoute {
    fn is_match(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        extensions: &mut Extensions,
    ) -> bool;

    async fn call(
        &self,
        req: S3Request<Body>,
    ) -> S3Result<S3Response<Body>>;
}
```

因此 route 层必须自己解析 URI path 和 query，并显式构造 `S3Response<Body>` 的 body 与 headers。`check_access` 使用 s3s 默认实现，确保未认证请求不会进入解压 handler。

Raw Complete route 对 control body 使用固定边界：

```rust
const MAX_COMPLETE_MULTIPART_XML_BYTES: usize = 4 * 1024 * 1024;

async fn collect_complete_xml(body: &mut Body) -> S3Result<Vec<u8>>;
```

`s3s::Body` 实现 `http_body::Body<Data = Bytes> + Unpin`，因此 helper 使用 `http_body_util::BodyExt::frame(&mut self)` 读取下一 frame。每个 data frame 都必须先以 `bytes.len().checked_add(data.remaining())` 计算新长度，并在 overflow 或 `> MAX_COMPLETE_MULTIPART_XML_BYTES` 时、任何 `extend_from_slice` 之前返回固定的 `InvalidRequest`。body/frame error 映射为 `IncompleteBody`，message 前缀固定为 `failed to read CompleteMultipartUpload XML:`；trailers 忽略。可以根据 `Content-Length` 提前拒绝，但流式计数始终是权威限制，所以 chunked/no `Content-Length` 也受同一上限约束。该 helper 只服务 Complete XML；archive 与 entry payload 不得调用 `BodyExt::collect` 或任何等价全量收集 API。

### 3.4 Entry 流式写入 Kubo

`async_zip::base::read::stream::ZipFileReader` 的 entry reader 是 forward-only `futures_lite`/`futures-io` `AsyncRead`；开启 `tokio` feature 后，archive 输入可通过 `tokio_util::compat` 适配。现有 `kubo::add::stream_add` 最终使用 `reqwest::Body::wrap_stream`，stream 类型需要 `'static`。为避免 collect entry，需要使用 duplex 桥接，并在 entry reader 与 Tokio duplex writer 之间使用 `tokio_util::compat`：

```text
async_zip entry reader --FuturesAsyncReadCompatExt::compat() + tokio::io::copy--> duplex writer
duplex reader --ReaderStream--> kubo::add::stream_add
```

copy future 和 upload future 在同一个 async scope 内 `try_join!`，entry reader 不需要 `'static`，传给 reqwest 的 stream 只持有 duplex reader。

### 3.5 路径安全算法

`sanitize_entry(name, target_prefix)` 返回三态结果：安全文件 key、目录跳过、全局拒绝。

规则:

1. entry 名必须能转换为 UTF-8 字符串。
2. 拒绝空名。
3. 拒绝 `\`。
4. 拒绝以 `/` 开头。
5. 拒绝 Windows 盘符形态，例如 `C:`。
6. 拒绝任何 `.` 或 `..` 段。
7. 目录 entry 只允许以 `/` 结尾，且路径本身也必须通过上述检查；通过后跳过。
8. 拼接 `target_prefix + entry_name` 后再确认结果仍以规范化 prefix 开头。

### 3.6 失败策略

| 类型 | HTTP 结果 | DB 可见性 | Pin 行为 |
|---|---|---|---|
| query/prefix/header 无效 | 400 | 不写 DB | 无 CID |
| archive Kubo add/pin 失败 | 500 | 不写 DB | 不调用 `pin_rm`；pin 调用结果或 CID 共享性可能不明确 |
| Zip Slip / 无效 entry 名 / unsupported compression | 400 | 不发布本次请求对象 | 保留 archive 与已成功 staged entry pins |
| 成功 staged entry 的最终 key 与 archive key 碰撞 | 400 `InvalidParameterValue`，固定 message | Put 不发布 archive/entry；Multipart 不 finalize 且保留 upload/parts | 保留 archive/root、part 与成功 staged entry pins，不调用 `pin_rm` |
| 单个安全 entry Kubo add/pin 失败 | 200 + Failure | 该 entry 不写 DB | 不调用 `pin_rm`；若 pin 实际生效则保守保留 |
| 单个安全 entry 读取/解压失败 | 200 + Failure；停止继续解压后续 entry | 该 entry 不写 DB；archive 与已成功 entry 保留 | 保留任何已成功 `pin_add` 的 entry CID |
| 单个安全 entry DB upsert 失败 | 200 + Failure | 该 entry 不可见 | 保留 entry pin；CID 可能被其他对象共享 |
| UploadPart 替换已有 part | 标准 UploadPart 响应 | 原子 upsert 后 DB 只保留新 part row | 旧、新 part pin 均保留；不调用 `pin_rm` |
| UploadPart `pin_add` 成功后 DB upsert 失败 | 500 | 旧 part row 原样保留，或首次上传时无 part row | 新 part pin 保留；不调用 `pin_rm` |
| Multipart 标准 Complete 校验失败 | 标准 S3 error | 不触发解压 | 沿用现有逻辑 |
| Multipart transaction body error | 500 | transaction 已回滚；upload/parts 保留 | root 与 part pins 均保留 |
| Multipart direct commit 或 connection/commit error 对账为 `Committed` | 按成功继续 | 当前 attempt row 精确匹配；upload/parts 不存在 | root 与全部 part pins 保留 |
| Multipart connection/commit error，对账为 `NotCommitted` 或 `Unknown` | 500 | 保留现状或报告混合/未知状态 | root 与 part pins 均保留 |
| AbortMultipartUpload | 204 | upload 删除并级联删除 part rows | part pins 保留；不读取 part 内容、不调用 `pin_rm` |

全局拒绝错误优先于 best-effort partial success。只要出现全局拒绝，响应就是 400，result XML 不返回。

上述保守策略不声称解决 pin 泄漏。它只保证在没有 CID 引用计数或独占租约的现有架构下，请求清理不会解除现有对象或并发成功 Complete 所依赖的 recursive pin。对已成功 `pin_add` 的 archive、root、entry 或 Multipart part CID，本文涉及的上传、替换、Complete、失败和 Abort 路径均不得调用 `pin_rm`；本设计不引入引用计数，也不为 pin 安全另增 migration。

即使 extractor 只记录“本次请求新 staged 的 entry”，内容寻址仍可能让该 entry CID 与既有对象或并发请求相同；“由本次请求首次观察到”不等于“本次请求独占 pin”。因此当前最安全且唯一允许的 entry failure cleanup 是不调用 `/pin/rm`，保留可能冗余的 pin。

## 4. 模块设计

### 4.1 新增模块

```text
src/
├── s3/
│   └── route/
│       ├── mod.rs
│       └── decompress_zip.rs
├── zip/
│   ├── mod.rs
│   ├── sanitize.rs
│   ├── extract.rs
│   └── response.rs
└── store/migrations/
    └── m20260707_000001_decompress_zip.rs
```

### 4.2 关键类型

```rust
pub struct DecompressZipRoute {
    state: Arc<AppState>,
}

pub struct StoredObject {
    pub cid: String,
    pub size: i64,
}

pub enum SanitizedEntry {
    File { key: String },
    Directory,
}

pub struct ExtractOutcome {
    pub entries: Vec<ExtractedEntry>,
    pub failures: Vec<ExtractFailure>,
}

pub struct ExtractedEntry {
    pub key: String,
    pub cid: String,
    pub size: i64,
}

pub struct ExtractFailure {
    pub entry_name: String,
    pub code: String,
    pub message: String,
}

fn reject_archive_key_collision(
    archive_key: &str,
    entries: &[ExtractedEntry],
) -> S3Result<()>;

pub struct CompletedMultipartArchive {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub encryption_object_id: String,
    pub completion_attempt_id: String,
    pub root_cid: String,
    pub total_size: i64,
    pub content_type: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub encrypted: bool,
    pub key_wrap: Option<String>,
    pub decompress_zip_target: Option<String>,
    pub decompress_zip_result: bool,
    pub server_side_encryption: Option<ServerSideEncryption>,
}

pub enum CommitCompletedUploadError {
    RolledBack {
        completion_attempt_id: String,
        source: AppError,
    },
    OutcomeUnknown {
        completion_attempt_id: String,
        source: AppError,
    },
}

pub enum ReconciledCommitOutcome {
    Committed,
    NotCommitted,
    Unknown(AppError),
}
```

### 4.3 现有模块改动

| 模块 | 改动 |
|---|---|
| `Cargo.toml` | 增加 `async_zip = { version = "0.0.18", features = ["tokio", "deflate"] }` |
| `src/lib.rs` | 在非空 `src/zip/mod.rs` 创建的同一步导出 `pub mod zip;` |
| `src/main.rs` | 在非空 `src/zip/mod.rs` 创建的同一步为独立 binary module tree 声明 `mod zip;`，随后注册 `DecompressZipRoute::new(state.clone())` |
| `src/s3/mod.rs` | 导出共享 query decoder 与 `pub mod route;` |
| `src/s3/route/decompress_zip.rs` | Put/Complete 自定义路由；生产并共享 archive/entry key 碰撞 helper，在 Put publish 与 Multipart finalize 前调用 |
| `src/s3/ops/object.rs` | 抽取 plaintext stream 存储 helper，供 route 和 extractor 复用 |
| `src/s3/ops/multipart.rs` | 记录 decompress metadata；UploadPart 改为 pin 后原子 upsert 且移除所有 part `pin_rm`；抽取含双 identity 的 Complete inner result；Abort 仅删除 DB upload；触发 Complete 后解压 |
| `src/store/entities/multipart_upload.rs` | 增加 `decompress_zip_target: Option<String>` 与 `decompress_zip_result: bool` |
| `src/store/multipart.rs` | `create_upload` 写入并 `get_upload` 读取新增字段；以复合主键 `ON CONFLICT DO UPDATE` 提供 `upsert_part`；提供 attempt-ID 精确 commit/reconciliation |
| `src/store/migrations/mod.rs` | 注册新 migration |
| `src/error.rs` | 增加 zip-specific errors 并映射到 S3 400 |
| `tests/integration.rs` | 测试 harness 注册 custom route 并增加 decompress 场景 |

新 migration 的 up/down 都使用 SeaORM migration schema API。up 以两个 `manager.alter_table(Table::alter().add_column(...))` 调用增加 nullable text target 与 `BOOLEAN NOT NULL DEFAULT TRUE` result；down 按逆序以两个独立 `manager.alter_table(Table::alter().drop_column(...))` 调用删除列。SeaQuery 0.32.7 的 SQLite builder 不支持一个 alter statement 中含多个 option，因此不得把两个 drop 合并为一个 statement。down 不得创建替代表、复制数据、drop/rename `multipart_uploads`，也不得触碰 `multipart_parts`；原父表 identity、upload/part rows 与 `multipart_parts.upload_id -> multipart_uploads.upload_id ON DELETE CASCADE` 必须保持。

## 5. 依赖与兼容性

新增依赖:

```toml
async_zip = { version = "0.0.18", features = ["tokio", "deflate"] }
tokio-util = { version = "0.7", features = ["io", "compat"] }
futures-io = "0.3"
http-body-util = "0.1"
```

已存在并复用的依赖:

- `quick-xml = "0.41"` 用于 XML response 序列化或 XML escaping。
- `tokio-util` 的 `ReaderStream` 用于 `AsyncRead` → `Stream<Bytes>`，`compat` 用于 Tokio IO 与 `async_zip` 的 `futures_lite` IO 之间适配。
- `percent-encoding` 用于 URI path/query 解码。

`async_zip` forward-only reader 无法读取 central directory，因此不能承诺完整 zip 特性。MVP 明确拒绝无法在 local header 中安全确定边界或压缩方式的 entry。

## 6. 客户端示例

### 6.1 botocore presigned URL

```python
import boto3
import requests
from botocore.auth import S3SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from urllib.parse import quote, urlencode

session = boto3.Session(
    aws_access_key_id="pixivbot",
    aws_secret_access_key="...",
    region_name="us-east-1",
)
credentials = session.get_credentials().get_frozen_credentials()

bucket = "pixivbot-images"
key = "2026/archive.zip"
base_url = (
    "https://ipfs3.moyuteam.me/"
    f"{quote(bucket, safe='')}/{quote(key, safe='/')}"
)
custom_query = urlencode(
    [
        ("decompress-zip", "2026/"),
        ("decompress-zip-result", "true"),
    ],
    quote_via=quote,
    safe="",
)
request = AWSRequest(method="PUT", url=f"{base_url}?{custom_query}")
S3SigV4QueryAuth(
    credentials,
    "s3",
    "us-east-1",
    expires=900,
).add_auth(request)

with open("archive.zip", "rb") as f:
    response = requests.put(request.url, data=f)

print(response.status_code)
print(response.text)
```

`decompress-zip` 与 `decompress-zip-result` 在 `AWSRequest` 构造时已经位于 URL 中，随后才由 `S3SigV4QueryAuth.add_auth` 参与 canonical query 签名；禁止先生成 presigned URL 再追加 custom query。`get_frozen_credentials()` 保留临时凭证的 session token，signer 会把它加入 presigned query。签名后的 URL 不得再修改或追加会改变 query 名、值或最终 decompression 语义的参数。

### 6.2 curl

```powershell
curl.exe -X PUT `
  "https://ipfs3.moyuteam.me/pixivbot-images/2026/archive.zip?decompress-zip=2026/" `
  -H "Authorization: AWS4-HMAC-SHA256 Credential=pixivbot/..." `
  -H "x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD" `
  --data-binary "@archive.zip"
```

### 6.3 AWS CLI

AWS CLI 的 high-level `aws s3 cp` 不能附加自定义 query 参数。使用 SDK、presigned URL + HTTP client，或直接构造 SigV4 HTTP 请求。

## 7. 测试策略

### 7.1 单元测试

- `zip::sanitize`: 正常文件、目录跳过、空名、绝对路径、`..`、`.`、反斜杠、Windows 盘符、prefix 规范化。
- `zip::response`: `DecompressZipResult` XML escaping、空 entries、空 failures、非空 failures。
- `zip::extract`: 小 zip 流式解压，目录跳过，duplicate last wins，entry Kubo failure 记录 failure。
- `s3::route::decompress_zip`: `is_match` 匹配 PutObject decompress 与 `POST ?uploadId=...` Complete，但不截获 Create；query parser 解析 result flag 和 prefix。
- `s3::route::decompress_zip`: `reject_archive_key_collision` 对成功 entries 返回固定 `InvalidParameterValue`，并允许所有非碰撞 entries；Put 与 Complete 的路由测试锁定检查分别发生在 archive publish 与 Multipart finalize 前。
- `s3::route::decompress_zip`: `collect_complete_xml` 用多个 data frame 验证正常 XML、恰好 4 MiB 通过、4 MiB + 1 byte 在 extend 前以固定 `InvalidRequest` 拒绝、body/frame error 映射 `IncompleteBody`，并忽略 trailer；guard scan 禁止对 request input 使用无界 collect。
- `s3::route::decompress_zip`: raw Complete parser 接受并逐字段保留 s3s 0.14 的五种合法 per-part checksum；`  crc&amp;32  ` 解析为精确的 `"  crc&32  "`，五种无属性自闭合 checksum 均可解析为 `Some("")`；同一 checksum 元素重复（包括自闭合与 Start/End 混用）、未知/嵌套 Empty、错误 nesting 和字段外内容仍为 `MalformedXML`。
- `s3::route::decompress_zip`: raw Complete ETag 与 s3s 0.14 parity：escaped `W/&quot;QmPart&quot;` 保留为 `ETag::Weak("QmPart")`，格式错误（如 `&quot;QmPart`、`W/QmPart`）保留为精确的 raw `Strong`，quoted non-ASCII 触发 `MalformedXML`，无属性 `<ETag/>` 为 `Strong("")`，重复 ETag 仍拒绝。
- `store::multipart`: `upsert_part` 原子替换 `(upload_id, part_number)` 的 `cid/size/etag/uploaded_at`；transaction body rollback 映射为 `RolledBack`；connection/commit error 映射为 `OutcomeUnknown`；纯对账分类以 `completion_attempt_id` 覆盖 unknown-committed、attempt-row-absent `NotCommitted` 与混合状态。
- `s3::ops::multipart`: UploadPart 替换/DB failure、Complete 全分支和 Abort 都不调用 part `pin_rm`；窄 store seam 注入 commit outcome unknown、不同 attempt winner 与查询失败，所有结果均保留 root/parts pins。
- `store::migrations::m20260707_000001_decompress_zip`: 真实 SQLite 执行 init → seed bucket/upload/part → up → down；验证新增列与 existing upload 的 default `TRUE`、down 后 upload/part 原字段逐值不变、FK 仍指向同一 `multipart_uploads` 且 delete upload 继续 cascade part。另用 `PostgresQueryBuilder` 断言四个 builder 都生成 `ALTER TABLE ... ADD/DROP COLUMN`，不出现父表重建 SQL。

### 7.2 集成测试

- PutObject decompress 合法 zip: 返回 XML；archive 和 entries 都可 `GetObject`。
- PutObject `decompress-zip-result=false`: 空 body，ETag 是 archive CID，entries 可 list/get。
- PutObject traversal: 返回 400；archive key 和 target prefix 不可见；archive/entry CID 不得出现在 `/pin/rm`。
- PutObject archive-key collision: 真实签名 HTTP 使用空 prefix 与同名 entry，返回固定 400；archive/entry 同 key 无 latest、无其他 entry 发布，archive 与成功 entry CID 都已 add/pin 且零 `/pin/rm`。
- PutObject SSE header: 返回 400；标准 PutObject SSE 测试仍通过。
- Multipart decompress: Create 记录目标，UploadPart 标准，Complete 返回 XML 并发布 archive/entries；Complete 后 part pin 保留。
- Multipart result=false: Complete 返回标准 `CompleteMultipartUploadResult`。
- UploadPart 原子替换: 旧 part row 被新 row 整体替换，旧、新 CID 均不 `pin_rm`；首次 insert 与替换 update 分别注入 DB failure 时，无新 row/旧 row 不丢失且新 pin 都保留。
- 单 part `QmPart == QmRoot`: Complete 后 archive 通过标准 GetObject 可读，upload/parts DB rows 删除，该 CID 的 `/pin/rm` 调用为零。
- shared part CID: 先让普通 object 引用 `QmSharedPart`，再分别走 part 替换、Abort 和 Complete；普通 object 始终可读，`QmSharedPart` 从未被 `pin_rm`。
- Abort Multipart: upload/part DB rows 删除，不触发 Kubo cat，不产生 archive/entries，part pin 保留。
- Multipart traversal/global reject: 返回 400、DB 不可见、upload/parts 保留，并断言 root/archive 与 staged entry CID 均未调用 `/pin/rm`。
- Multipart archive-key collision: Create 使用空 prefix，UploadPart 后 Complete 同名 entry，返回固定 400；archive/entry latest 不存在，upload/parts 与 part ETag 保持以允许重试，root、part、成功 entry CID 都已 add/pin 且零 `/pin/rm`。
- 同-upload 并发 Complete: 两个 inner 读取相同 upload/parts、完成相同 `QmRoot` add+pin，得到不同 `completion_attempt_id` 后经同一 barrier 同时 finalize；恰一请求成功，winner row ID 等于 winner attempt，loser row 不存在，upload/parts 均不存在，且 `QmRoot`、`QmPart` 均从未出现在 `/pin/rm`。
- Commit outcome unknown: 覆盖当前 attempt 已提交、当前 attempt 未提交、A unknown 未提交而 B 不同 attempt 提交成功，以及查询失败；A 不得把 B row 认作自己，所有成功/错误/未知分支均保留 root/parts pins。
- 标准 Multipart 回归: UploadPart 替换、Complete 和 Abort 均保持标准 DB/HTTP 语义，但 part pins 保留且无 `/pin/rm`。
- 标准 Multipart weak-ETag + checksum Complete 回归: 真实 SigV4/TCP Create（无 decompress option）→ UploadPart → 自定义 Complete XML 发送 XML-escaped `W/"<actual ETag>"` 和五种合法 checksum；返回标准 `CompleteMultipartUploadResult`，root/latest 正确、upload/parts 删除、签名 Get 可读，且 part/root 无 `/pin/rm`。
- Complete XML hard limit: 通过真实 HTTP 分别发送有效签名、`Content-Length > 4 MiB` 的请求，以及有效签名、HTTP/1.1 chunked 且无 `Content-Length` 的请求；两者都返回 `400 InvalidRequest` 和固定 message，Complete 前后的 Kubo request log 与 upload/part/archive DB snapshot 不变。
- Presigned query: Rust presigner 在 canonical signing 前放入 custom tuples 与全部 `X-Amz-*` query，presigned PUT 成功后用真实 ListObjectsV2/GetObject 验证 archive 与 entry；在同一已签 URL 后修改/追加使 `decompress-zip` 最终值变化的参数，必须得到 `403 SignatureDoesNotMatch` 且无 Kubo/DB mutation。该覆盖不能替代现有 header Authorization signer 测试，两种路径都必须通过。

### 7.3 端到端

- 用 presigned URL 上传真实 zip，验证 `ListObjectsV2` 能列出 archive 和解压 entry。
- 用 Kubo `/api/v0/cat?arg=<archive_cid>` 验证 archive CID 是原始 zip。
- 用 Kubo `/api/v0/cat?arg=<entry_cid>` 验证 entry CID 是解压后的单文件内容。

## 8. 实施顺序

1. 增加依赖、错误类型、使用双后端 schema builder 且保持父表/FK identity 的 multipart migration，以及 entity/store 字段。
2. 抽取 plaintext object stream helper 和 multipart Complete inner result。
3. 实现 sanitizer、XML response、query/path parser。
4. 实现 archive-first extractor 与 duplex entry upload bridge。
5. 实现并注册 PutObject custom route；在任何 archive/entry DB publish 前调用共享 archive-key collision helper。
6. 实现 Multipart create、原子 `upsert_part`、Complete/Abort 保守 part-pin 路径、每次 Complete 独立 attempt identity、transaction outcome、read-after-error 对账和同-upload finalize 并发边界。
7. 实现 Multipart Complete route，在 finalize 前复用同一 collision helper；增加 Complete XML 4 MiB 边界、header/presigned 两种 SigV4、query tamper、collision、migration up/down、单元/集成和端到端验证。

该顺序确保每一步都有独立测试，并且非 `decompress-zip` S3 行为始终保持回归覆盖。
