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
- archive 字节流、entry 字节流与 Kubo 写入均保持流式；不得把整个 archive 或整个 entry collect 到内存。
- 发现 Zip Slip、绝对路径、Windows 盘符、反斜杠路径、空文件名、`.` / `..` 逃逸等全局拒绝错误时，返回 400，并且不发布本次请求创建的 archive 或 entry 对象。
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

### 1.4 验收标准

**PutObject + decompress-zip**:

1. `PUT /bucket/key.zip?decompress-zip=prefix/` 上传合法 zip，返回 `200 OK` 和 `DecompressZipResult` XML。
2. archive 以 `s3://bucket/key.zip` 发布，ETag 为 archive CID。
3. zip 内 `foo/bar.txt` 解压到 `s3://bucket/prefix/foo/bar.txt`，ETag 为 entry CID。
4. zip 内 `../escape.txt`、`/etc/passwd`、`C:\Windows\x`、`dir\file.txt`、空 entry 名都会使整个请求返回 `400 InvalidParameterValue`。
5. 全局拒绝时，本次请求创建的 archive DB 记录和 entry DB 记录不会保持可见；已 pin 的 CID 进行 best-effort `pin_rm`。
6. 单个安全 entry 的 Kubo/DB 写入失败记录到 `Failures`，请求仍返回 `200 OK`，已经成功的其他 entry 和 archive 保留。
7. `decompress-zip-result=false` 执行同样的解压和发布动作，但返回标准 PutObject 空 body，ETag header 仍是 archive CID。
8. 不含 `decompress-zip` 的 `PUT /bucket/key` 继续走标准 s3s PutObject，行为不变。

**Multipart + decompress-zip**:

9. `POST /bucket/key.zip?uploads&decompress-zip=prefix/` 创建 Multipart Upload，并在 upload 记录中持久化 `decompress_zip_target=prefix/` 与 `decompress_zip_result=true`。
10. UploadPart 不感知解压，行为与标准 Multipart 一致。
11. CompleteMultipartUpload 先执行现有 part 校验、拼接与 root CID add/pin；解压 upload 在确认没有全局拒绝后才发布 archive DB 记录、删除 upload 并 unpin part CID。
12. Complete 默认返回 `DecompressZipResult` XML；`decompress-zip-result=false` 返回标准 `CompleteMultipartUploadResult`。
13. AbortMultipartUpload 删除 upload 和 part 记录，不触发解压。

## 2. 接口设计

### 2.1 行为分支

`decompress-zip` 是纯增量扩展。没有该 query 参数的请求必须完全绕过新代码。

| 请求 | 路由 | 行为 |
|---|---|---|
| `PUT /bucket/key` | 标准 s3s PutObject | 标准 S3 语义，ETag=CID |
| `PUT /bucket/key?decompress-zip=prefix/` | `DecompressZipRoute` | 存 archive、解压 entry、返回 `DecompressZipResult` |
| `PUT /bucket/key?decompress-zip=prefix/&decompress-zip-result=false` | `DecompressZipRoute` | 存 archive、解压 entry、返回标准 PutObject 空 body |
| `POST /bucket/key?uploads&decompress-zip=prefix/` | 标准 CreateMultipartUpload 解析额外 query | 创建 upload 并记录解压目标 |
| `POST /bucket/key?uploadId=...` 且 upload 记录含解压目标 | `DecompressZipCompleteRoute` 或 Complete 内部分支 | 完成 archive 后解压，按 result flag 返回响应 |

### 2.2 Query 参数

- `decompress-zip=<target-prefix>`: 必填且值可以为空字符串。空字符串表示解压到 bucket 根路径。
- `decompress-zip-result=false`: 可选。仅字符串 `false` 关闭自定义结果 body；缺失或其他值均按 `true` 处理。

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

重复 entry key 沿用 PutObject 覆盖语义：同一次成功请求内后出现的 entry 覆盖先出现的 entry，最终 latest 对象是最后一个成功 entry。

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

全局拒绝使用标准 S3 error XML，HTTP 状态为 400。

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
  ├─ 若出现全局拒绝: pin_rm 已创建 CID，不 upsert DB，返回 400
  ├─ 若无全局拒绝: upsert archive 和成功 entries；失败 entries 写入 result
  └─ 根据 decompress-zip-result 返回 XML 或标准空 body
```

选择 archive-first 的原因:

- `s3s::Body` 是一次性消费流；tee 需要复杂背压协调。
- Kubo 已经是本地内容寻址存储，先 add 再 cat 同一 CID 简单可靠。
- archive CID 可以在解压失败时保留或回滚，语义清晰。
- 不 collect archive，不牺牲大文件能力。

### 3.2 Multipart 流程

```text
CreateMultipartUpload (?uploads&decompress-zip=prefix/)
  └─ 标准 create + 记录 decompress_zip_target/decompress_zip_result

UploadPart
  └─ 完全标准，part 不做解压

CompleteMultipartUpload (?uploadId=...)
  ├─ upload 记录无 decompress target: 标准 complete → archive DB upsert → upload 删除 → 返回标准 CompleteMultipartUploadResult
  └─ upload 记录有 decompress target:
       校验 parts → 拼接 → root CID → pin → stream_cat(root_cid) → 解压 entries
       ├─ 全局拒绝: pin_rm root/entry staged CIDs；保留 upload/parts；返回 400；不发布 archive DB 记录
       └─ 成功/partial failure: archive DB upsert → entry DB upsert → part unpin → upload 删除 → 返回 XML 或标准 CompleteMultipartUploadResult
```

Complete 解压读取 root CID。root CID 是 Complete 时重新 `stream_add` 得到的完整 UnixFS 文件，独立于 part CID；只有在解压没有全局拒绝并且 archive 发布完成后，才执行 `pin_rm` part CID 与 upload 删除。若 archive DB upsert 后的后续 finalize 步骤失败，必须通过 `object::delete_latest(bucket, key)` 取消本次新 archive 可见性并 best-effort `pin_rm(root_cid)`。这样可满足全局拒绝或 finalize 失败时“不发布本次请求 archive/entry DB 记录”的语义。

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

| 类型 | HTTP 结果 | DB 可见性 | CID 清理 |
|---|---|---|---|
| query/prefix/header 无效 | 400 | 不写 DB | 无 CID |
| archive Kubo add/pin 失败 | 500 | 不写 DB | best-effort pin_rm |
| Zip Slip / 无效 entry 名 / unsupported compression | 400 | 不发布本次请求对象 | best-effort pin_rm 本次 CID |
| 单个安全 entry Kubo add/pin 失败 | 200 + Failure | 该 entry 不写 DB | best-effort pin_rm |
| 单个安全 entry 读取/解压失败 | 200 + Failure；停止继续解压后续 entry | 该 entry 不写 DB；archive 与已成功 entry 保留 | best-effort pin_rm 该 entry CID |
| 单个安全 entry DB upsert 失败 | 200 + Failure | 该 entry 不可见 | best-effort pin_rm |
| Multipart 标准 Complete 校验失败 | 标准 S3 error | 不触发解压 | 沿用现有逻辑 |

全局拒绝错误优先于 best-effort partial success。只要出现全局拒绝，响应就是 400，result XML 不返回。

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

pub struct ExtractResult {
    pub archive_key: String,
    pub archive_cid: String,
    pub archive_size: i64,
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
```

### 4.3 现有模块改动

| 模块 | 改动 |
|---|---|
| `Cargo.toml` | 增加 `async_zip = { version = "0.0.18", features = ["tokio", "deflate"] }` |
| `src/lib.rs` | 导出 `pub mod zip;` |
| `src/main.rs` | 注册 `DecompressZipRoute::new(state.clone())` |
| `src/s3/mod.rs` | 导出 `pub mod route;` |
| `src/s3/ops/object.rs` | 抽取 plaintext stream 存储 helper，供 route 和 extractor 复用 |
| `src/s3/ops/multipart.rs` | 记录 decompress metadata；抽取 Complete inner result；触发 Complete 后解压 |
| `src/store/entities/multipart_upload.rs` | 增加 `decompress_zip_target: Option<String>` 与 `decompress_zip_result: bool` |
| `src/store/multipart.rs` | `create_upload` 写入并 `get_upload` 读取新增字段 |
| `src/store/migrations/mod.rs` | 注册新 migration |
| `src/error.rs` | 增加 zip-specific errors 并映射到 S3 400 |
| `tests/integration.rs` | 测试 harness 注册 custom route 并增加 decompress 场景 |

## 5. 依赖与兼容性

新增依赖:

```toml
async_zip = { version = "0.0.18", features = ["tokio", "deflate"] }
tokio-util = { version = "0.7", features = ["io", "compat"] }
futures-io = "0.3"
```

已存在并复用的依赖:

- `quick-xml = "0.41"` 用于 XML response 序列化或 XML escaping。
- `tokio-util` 的 `ReaderStream` 用于 `AsyncRead` → `Stream<Bytes>`，`compat` 用于 Tokio IO 与 `async_zip` 的 `futures_lite` IO 之间适配。
- `percent-encoding` 用于 URI path/query 解码。

`async_zip` forward-only reader 无法读取 central directory，因此不能承诺完整 zip 特性。MVP 明确拒绝无法在 local header 中安全确定边界或压缩方式的 entry。

## 6. 客户端示例

### 6.1 boto3 presigned URL

```python
import boto3
import requests
from urllib.parse import quote

s3 = boto3.client(
    "s3",
    endpoint_url="https://ipfs3.moyuteam.me",
    aws_access_key_id="pixivbot",
    aws_secret_access_key="...",
    region_name="us-east-1",
)

url = s3.generate_presigned_url(
    "put_object",
    Params={"Bucket": "pixivbot-images", "Key": "2026/archive.zip"},
    HttpMethod="PUT",
)
separator = "&" if "?" in url else "?"
url = f"{url}{separator}decompress-zip={quote('2026/')}&decompress-zip-result=true"

with open("archive.zip", "rb") as f:
    response = requests.put(url, data=f)

print(response.status_code)
print(response.text)
```

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
- `s3::route::decompress_zip`: `is_match` 只匹配 PutObject decompress 请求；query parser 解析 result flag 和 prefix。

### 7.2 集成测试

- PutObject decompress 合法 zip: 返回 XML；archive 和 entries 都可 `GetObject`。
- PutObject `decompress-zip-result=false`: 空 body，ETag 是 archive CID，entries 可 list/get。
- PutObject traversal: 返回 400；archive key 和 target prefix 不可见。
- PutObject SSE header: 返回 400；标准 PutObject SSE 测试仍通过。
- Multipart decompress: Create 记录目标，UploadPart 标准，Complete 返回 XML 并发布 archive/entries。
- Multipart result=false: Complete 返回标准 `CompleteMultipartUploadResult`。
- Abort Multipart: 不触发 Kubo cat，不产生 archive/entries。

### 7.3 端到端

- 用 presigned URL 上传真实 zip，验证 `ListObjectsV2` 能列出 archive 和解压 entry。
- 用 Kubo `/api/v0/cat?arg=<archive_cid>` 验证 archive CID 是原始 zip。
- 用 Kubo `/api/v0/cat?arg=<entry_cid>` 验证 entry CID 是解压后的单文件内容。

## 8. 实施顺序

1. 增加依赖、错误类型、multipart migration 与 entity/store 字段。
2. 抽取 plaintext object stream helper 和 multipart Complete inner result。
3. 实现 sanitizer、XML response、query/path parser。
4. 实现 archive-first extractor 与 duplex entry upload bridge。
5. 实现并注册 PutObject custom route。
6. 实现 Multipart create/complete 解压路径。
7. 增加单元、集成和端到端验证。

该顺序确保每一步都有独立测试，并且非 `decompress-zip` S3 行为始终保持回归覆盖。
