# Decompress-Zip 上传扩展 — WIP 设计文档

- 日期: 2026-07-07
- 状态: WIP (Work In Progress)
- 作者: hugefiver
- 关联: ipfs-s3-gateway 主设计 `2026-07-02-ipfs-s3-gateway-design.md`

## 1. 目标与范围

### 1.1 项目目标

在标准 S3 PutObject 语义上扩展一个 query 参数 `decompress-zip`,使客户端用**单个 PutObject 请求**上传一个 zip 压缩包,服务端**流式解压**并将包内文件按层级存入同一个 bucket 的指定路径下。上传完成后,压缩包本身与解压后的所有文件**均持久化存储**。

### 1.2 MVP 范围

- 支持 zip 格式(Stored 无压缩 + Deflate 压缩)
- PutObject + decompress-zip:单请求流式上传 + 流式解压
- Multipart Upload + decompress-zip:Create 记录目标,Complete 拼接后解压
- 流式上传 + 流式解压 + 流式写入 Kubo,全程不 collect
- 路径穿越防护(Zip Slip 攻击)
- 压缩包本身与解压文件均存储
- 复用现有 SigV4 鉴权与 PutObject / Multipart 内部逻辑

### 1.3 不在 MVP 范围

- tar.gz / tar.zst / 7z 等其他格式
- 跨 bucket 解压(目标必须同 bucket)
- 异步任务模式(初版同步)
- 部分失败回滚(初版 best-effort,保留已成功项,返回失败列表)
- 加密对象(zip 内文件不支持 SSE,沿用 bucket 默认)

### 1.4 验收标准

**PutObject + decompress-zip**:

1. `PUT /bucket/key.zip?decompress-zip=prefix/` 上传 zip,响应 200 + DecompressZipResult XML
2. 压缩包以 `s3://bucket/key.zip` 存储,ETag 为其 CID
3. zip 内 `foo/bar.txt` 解压到 `s3://bucket/prefix/foo/bar.txt`
4. zip 内 `../escape.txt` 被拒绝(400 InvalidParameterValue),不写入任何文件
5. 绝对路径条目 `/etc/passwd` 被拒绝
6. 全程内存占用恒定,与 zip 大小无关
7. 解压中途失败的文件:已成功的保留,失败项在响应 body 中列出
8. `decompress-zip-result=false` 返回标准 PutObject 空 body 响应
9. 不含 `decompress-zip` 的 `PUT /bucket/key` 走标准 PutObject,行为与 AWS S3 一致

**Multipart + decompress-zip**:

10. `POST /bucket/key?uploads&decompress-zip=prefix/` 创建 multipart upload,记录解压目标
11. UploadPart 行为与标准 Multipart 一致,不感知解压
12. CompleteMultipartUpload 拼接 parts 后解压,archive 与解压文件均存储
13. AbortMultipartUpload 不触发解压,行为与标准 Multipart 一致
14. Multipart 解压响应同 PutObject(DecompressZipResult XML 或标准 CompleteMultipartUploadResult)

## 2. 接口设计

### 2.1 行为分支:由 query 参数决定

`decompress-zip` 是**纯增量扩展**:不传入该参数的请求完全走标准 s3s PutObject 链路,行为与 AWS S3 一致。仅当 query 含 `decompress-zip` 时,S3Route 才拦截并进入解压流程。

| 请求 | 路由 | 行为 |
|---|---|---|
| `PUT /bucket/key` | 标准 s3s PutObject | 标准 S3 语义,空 body 响应,ETag=CID |
| `PUT /bucket/key?decompress-zip=prefix/` | DecompressZipRoute 拦截 | 解压 + 存储,返回 DecompressZipResult XML |
| `PUT /bucket/key?decompress-zip=prefix/&decompress-zip-result=false` | DecompressZipRoute 拦截 | 解压 + 存储,但返回标准 PutObject 空 body |

**匹配规则**(S3Route::is_match):

```rust
fn is_match(&self, method: &Method, uri: &Uri, _: &HeaderMap, _: &mut Extensions) -> bool {
    method == Method::PUT
        && uri.query().map_or(false, |q| q.contains("decompress-zip"))
}
```

不含 `decompress-zip` 的请求 `is_match` 返回 false,直接走标准 s3s PutObject,网关现有 PutObject 代码路径零改动。

### 2.2 解压请求格式

```
PUT /<bucket>/<key>?decompress-zip=<target-prefix> HTTP/1.1
Host: ipfs3.moyuteam.me
Authorization: AWS4-HMAC-SHA256 Credential=pixivbot/...
x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD
Content-Length: <zip 字节数>

<zip 字节流>
```

- `<bucket>`:压缩包存储的 bucket,也是解压目标 bucket(必须相同)
- `<key>`:压缩包本身的存储 key(如 `2026/archive.zip`)
- `decompress-zip=<target-prefix>`:解压目标前缀,zip 内文件按层级拼接在其后
- `decompress-zip-result=false`(可选):取消 result body,返回标准 PutObject 空 body 响应

### 2.3 路径映射规则

| zip 内 entry 名 | target-prefix | 最终 S3 key |
|---|---|---|
| `foo.txt` | `2026/` | `2026/foo.txt` |
| `dir/foo.txt` | `2026/` | `2026/dir/foo.txt` |
| `foo/bar/` (目录条目) | `2026/` | 跳过(目录条目不写入对象) |
| `../escape.txt` | 任意 | **拒绝整个请求 400** |
| `/etc/passwd` | 任意 | **拒绝整个请求 400** |

### 2.4 响应

`decompress-zip` 是网关自定义扩展,默认返回 `DecompressZipResult` XML body,提供解压详情(压缩包 CID、各解压文件 CID、失败列表)。需要标准 S3 PutObject 兼容(空 body)的客户端可加 `decompress-zip-result=false` 取消 result body。

#### 默认:返回 result body

```
PUT /bucket/key.zip?decompress-zip=prefix/
```

```
HTTP/1.1 200 OK
Content-Type: application/xml
ETag: "QmArchiveCID..."

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

部分失败也返回 200,详情在 body 的 `Failures` 中:

```xml
<DecompressZipResult>
  <ArchiveKey>2026/archive.zip</ArchiveKey>
  <ArchiveETag>Qm...</ArchiveETag>
  <ArchiveSize>1048576</ArchiveSize>
  <ExtractedCount>49</ExtractedCount>
  <FailedCount>1</FailedCount>
  <Entries>
    <Entry>
      <Key>prefix/good.txt</Key>
      <ETag>QmGoodCID...</ETag>
      <Size>512</Size>
    </Entry>
    <!-- ... 其余 48 个成功 entry ... -->
  </Entries>
  <Failures>
    <Failure>
      <EntryName>bad/corrupt.txt</EntryName>
      <Code>InternalError</Code>
      <Message>decompress stream error: invalid stored block lengths</Message>
    </Failure>
  </Failures>
</DecompressZipResult>
```

#### 兼容模式:标准 PutObject 空 body

```
PUT /bucket/key.zip?decompress-zip=prefix/&decompress-zip-result=false
```

```
HTTP/1.1 200 OK
ETag: "QmArchiveCID..."

(无 body)
```

压缩包 CID 仍在 `ETag` header 中。解压文件清单通过 `ListObjectsV2 ?prefix=prefix/` 二次查询,每个文件的 ETag 即其 CID。

#### 整体拒绝(路径穿越等)

```
HTTP/1.1 400 Bad Request
Content-Type: application/xml

<Error>
  <Code>InvalidParameterValue</Code>
  <Message>zip entry '../escape.txt' escapes target directory</Message>
</Error>
```

### 2.5 Multipart Upload 支持

大 zip 包(超过单次 PutObject 限制或客户端内存限制)可通过 Multipart Upload 上传。解压时机在 **CompleteMultipartUpload** — 此时所有 part 已拼接成完整 zip 并产生 root CID,网关读取 root CID 对应的完整 zip 字节流进行解压。

#### 2.5.1 接口流程

| 步骤 | 请求 | 行为 |
|---|---|---|
| 1. Create | `POST /bucket/key?uploads&decompress-zip=prefix/` | 创建 upload,持久化 `decompress_zip_target` 到 multipart upload 记录 |
| 2. Upload parts | `PUT /bucket/key?partNumber=N&uploadId=xxx` | 标准 UploadPart,**不**做解压,part 作为 zip 字节片段存储 |
| 3. Complete | `POST /bucket/key?uploadId=xxx` | 拼接 parts → root CID → **读取完整 zip 流解压** → 逐 entry PutObject |
| 4. Abort(可选) | `DELETE /bucket/key?uploadId=xxx` | 标准 AbortMultipartUpload,不触发解压 |

#### 2.5.2 CreateMultipartUpload 改动

`POST /bucket/key?uploads&decompress-zip=prefix/` 被 DecompressZipRoute 拦截(或标准 CreateMultipartUpload 增加参数解析)。`decompress_zip_target` 字段持久化到 multipart upload 记录:

```rust
// store/entities/multipart.rs 新增字段
pub decompress_zip_target: Option<String>, // None = 普通上传,Some(prefix) = Complete 时解压
```

Complete 时根据该字段判断是否触发解压。

#### 2.5.3 CompleteMultipartUpload 解压流程

```
POST /bucket/key?uploadId=xxx  (upload 记录含 decompress_zip_target)
  │
  ▼
1. 标准 Complete 流程(不变):
   ├─ 校验 parts 升序、ETag 匹配、part ≥ 5MiB(除最后)
   ├─ 拼接所有 part → stream_add → root_cid
   ├─ pin_add(root_cid)
   ├─ DB upsert(archive 对象:cid=root_cid, etag=root_cid)
   └─ pin_rm 所有 part CID
  │
  ▼
2. 解压流程(新增,仅 decompress_zip_target.is_some()):
   ├─ 从 Kubo stream_cat(root_cid) 读取完整 zip 字节流
   ├─ async-zip 流式解压,逐 entry:
   │   ├─ sanitize_entry(name, target_prefix)
   │   ├─ 路径穿越 → 收集到 failures,跳过该 entry
   │   └─ entry body 流 → kubo stream_add → pin_add → DB 写入
   └─ 汇总 ExtractResult
  │
  ▼
3. 响应(根据 decompress-zip-result):
   ├─ 默认:DecompressZipResult XML body
   └─ result=false:标准 CompleteMultipartUploadResult XML(仅 archive CID)
```

#### 2.5.4 解压时机选择:Complete 后而非 Complete 中

**选择**:先完成标准 Complete(拼接 + 存 archive + 回收 part CID),**再**从 Kubo 读 root CID 解压。

**理由**:

| 方案 | 做法 | 问题 |
|---|---|---|
| Complete 中解压 | 边拼接边解压(zip 是流式格式,理论可行) | zip Central Directory 在文件末尾,流式解压需要特殊处理;且 part 拼接是逐 part stream_cat,zip 解压器需要从头读,与 part 顺序解压耦合 |
| **Complete 后解压**(选择) | 先存 archive,再 stream_cat(root_cid) 读完整 zip 解压 | 多一次 Kubo 读,但解压独立、清晰、可失败重试(archive 已持久化) |

archive 先持久化保证:**即使解压全失败,zip 包本身仍可用**,符合"压缩包与解压文件均存储"的设计目标。

#### 2.5.5 Multipart 解压响应

默认返回 DecompressZipResult XML(同 2.4 节),archive 部分用 root_cid:

```xml
<DecompressZipResult>
  <ArchiveKey>2026/archive.zip</ArchiveKey>
  <ArchiveETag>QmRootCID...</ArchiveETag>
  <ArchiveSize>52428800</ArchiveSize>
  <ExtractedCount>49</ExtractedCount>
  <FailedCount>0</FailedCount>
  <Entries>
    <Entry>
      <Key>prefix/foo/bar.txt</Key>
      <ETag>QmFooCID...</ETag>
      <Size>1024</Size>
    </Entry>
    <!-- ... -->
  </Entries>
  <Failures/>
</DecompressZipResult>
```

`decompress-zip-result=false` 时返回标准 CompleteMultipartUploadResult:

```xml
<CompleteMultipartUploadResult>
  <Location>https://ipfs3.moyuteam.me/2026/archive.zip</Location>
  <Bucket>pixivbot-images</Bucket>
  <Key>2026/archive.zip</Key>
  <ETag>"QmRootCID..."</ETag>
</CompleteMultipartUploadResult>
```

#### 2.5.6 Create 时不校验 zip 有效性

CreateMultipartUpload 只记录 `decompress_zip_target`,不检查 zip 有效性(zip 内容还没上传)。zip 格式校验在 Complete 解压阶段。

#### 2.5.7 UploadPart 不感知解压

UploadPart 行为与标准 Multipart 完全一致:part 作为字节片段存储,part CID 即其 ETag。part 不含完整 zip 结构,不解压。

## 3. 架构设计

### 3.1 请求处理链路

```
PUT /bucket/key.zip?decompress-zip=prefix/
  │
  ▼
s3s prepare():
  1. S3Path 解析 → Object { bucket, key }
  2. SigV4 校验 → credentials
  3. S3Route::is_match → method=PUT + query 含 decompress-zip
     └─ 返回 Prepare::CustomRoute,跳过标准 PutObject
  │
  ▼
ZipDecompressRoute::call(S3Request<Body>):
  │ body: DynByteStream (zip 流)
  │
  ├─ 1. tee: body 分两路
  │    ├─ 路 A → 压缩包存储(流式 PutObject key.zip)
  │    └─ 路 B → async-zip 流式解压器
  │
  ├─ 2. 路 A: wrap_stream(tee_a) → kubo stream_add → pin_add → DB
  │    (压缩包本身先入库,确保即使解压全失败,压缩包仍可用)
  │
  ├─ 3. 路 B: async-zip 逐 entry
  │    for entry in zip_stream:
  │      ├─ 校验 entry.name 无路径穿越
  │      ├─ 跳过目录条目(name 以 / 结尾)
  │      ├─ 拼接 target_key = target_prefix + sanitized_name
  │      └─ entry.body 流式 → wrap_stream → kubo stream_add → pin_add → DB
  │
  └─ 4. 汇总结果,返回 XML
```

### 3.2 流式 tee 设计

zip 流必须同时喂给"压缩包存储"和"解压器"。初版方案:

```
                    ┌──► 路 A: 压缩包存储 (PutObject key.zip)
zip body stream ───►│
                    └──► 路 B: async-zip 解压 → 逐 entry PutObject
```

**实现选择**(WIP,待定):

| 方案 | 做法 | 优劣 |
|---|---|---|
| A. tokio::sync::mpsc 双消费者 | body stream 喂给两个 channel sender | 真流式,但需要协调两个消费者速率 |
| B. 先存压缩包,再从 blockstore 读取解压 | 路 A 完成后,从 Kubo 拉取刚存的 CID 流式喂给解压器 | 简单,但多一次 Kubo 读写往返 |
| C. 内存缓冲压缩包再分发 | collect 后分发 | 违反流式原则,大文件 OOM,**排除** |

**初版倾向方案 B**:实现简单,语义清晰(压缩包先持久化,再解压),Kubo blockstore 本身就是内容寻址存储,读自己的 CID 是本地操作。

### 3.3 路径穿越防护

zip 内 entry.name 可能包含恶意路径:

| 攻击模式 | 示例 | 检测方法 |
|---|---|---|
| 相对穿越 | `../escape.txt` | 规范化后路径不在 target_prefix 之下 |
| 绝对路径 | `/etc/passwd` | 以 `/` 开头 |
| Windows 绝对 | `C:\Windows\system32` | 含盘符 |
| 符号链接(zip 特有) | entry 为 symlink 指向外部 | 拒绝所有 symlink entry |

**规范化算法**(每个 entry):

```rust
fn sanitize_entry(name: &str, target_prefix: &str) -> Result<String, AppError> {
    // 1. 拒绝绝对路径
    if name.starts_with('/') || name.contains('\\') {
        return Err(AppError::InvalidZipEntry(name.into()));
    }
    // 2. 拒绝 Windows 盘符
    if name.len() >= 2 && name.as_bytes()[1] == b':' {
        return Err(AppError::InvalidZipEntry(name.into()));
    }
    // 3. 规范化拼接
    let full = format!("{target_prefix}{name}");
    let mut stack: Vec<&str> = Vec::new();
    for part in full.split('/') {
        match part {
            "" | "." => continue,
            ".." => { stack.pop().ok_or(AppError::ZipSlip(name.into()))?; }
            _ => stack.push(part),
        }
    }
    // 4. 确保结果仍在 target_prefix 之下(保险)
    let result = stack.join("/");
    if !result.starts_with(target_prefix.trim_end_matches('/')) {
        return Err(AppError::ZipSlip(name.into()));
    }
    Ok(result)
}
```

### 3.4 失败策略

**初版 best-effort**:解压中途某 entry 失败,不中断后续 entry,失败项收集到响应中。

| 失败类型 | 处理 |
|---|---|
| 路径穿越 | **拒绝整个请求 400**,不写入任何文件(包括压缩包) |
| 解压器流错误(单个 entry) | 记录失败,继续下一个 entry |
| Kubo add 失败(单个 entry) | 记录失败,继续下一个 entry |
| DB 写入失败(单个 entry) | 尝试 pin_rm 回滚该 entry,记录失败,继续 |
| 压缩包本身存储失败 | 整个请求 500,不继续解压 |

### 3.5 顺序保证

1. **先存压缩包**(路 A 完成) → 确保即使解压全失败,压缩包仍可用
2. **再逐 entry 解压**(路 B) → 每个成功 entry 立即入库
3. **返回汇总**

## 4. 模块设计

### 4.1 新增模块

```
src/
├── s3/
│   └── route/
│       └── decompress_zip.rs   # S3Route 实现 + handler
├── zip/
│   ├── mod.rs                  # 模块入口
│   ├── sanitize.rs             # 路径穿越防护
│   └── extract.rs              # 流式解压 + 逐 entry PutObject
└── (现有模块改动见 4.3)
```

### 4.2 关键类型

```rust
// src/s3/route/decompress_zip.rs
pub struct DecompressZipRoute;

#[async_trait::async_trait]
impl S3Route for DecompressZipRoute {
    fn is_match(&self, method: &Method, uri: &Uri, _: &HeaderMap, _: &mut Extensions) -> bool {
        method == Method::PUT
            && uri.query().map_or(false, |q| q.contains("decompress-zip"))
    }
    // check_access 用默认实现(要求 SigV4 credentials)
    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>>;
}

// src/zip/sanitize.rs
pub fn sanitize_entry(name: &str, target_prefix: &str) -> Result<String, AppError>;

// src/zip/extract.rs
pub struct ExtractResult {
    pub archive_cid: String,
    pub archive_size: u64,
    pub extracted: Vec<ExtractedEntry>,
    pub failures: Vec<ExtractFailure>,
}
pub async fn extract_zip_to_bucket(
    state: &AppState,
    bucket: &str,
    archive_key: &str,
    target_prefix: &str,
    zip_stream: DynByteStream,
) -> AppResult<ExtractResult>;
```

### 4.3 现有模块改动

| 模块 | 改动 |
|---|---|
| `Cargo.toml` | 加 `async-zip = "0.0.17"` |
| `src/main.rs` | `S3ServiceBuilder::set_route(DecompressZipRoute)` |
| `src/s3/ops/object.rs` | 抽取 `put_object_inner` 供 route 复用(不改对外接口) |
| `src/s3/ops/multipart.rs` | `create_multipart_upload` 解析 `decompress-zip` query 并持久化到 upload 记录;`complete_multipart_upload` 在标准 Complete 流程后检查 `decompress_zip_target`,若 Some 则调用解压逻辑 |
| `src/store/entities/multipart.rs` | 加 `decompress_zip_target: Option<String>` 字段 |
| `src/store/multipart.rs` | `create_upload` / `get_upload` 读写新字段 |
| `migrations/m20250701_000001_init.rs` 或新 migration | multipart 表加 `decompress_zip_target TEXT NULL` 列 |
| `src/error.rs` | 加 `InvalidZipEntry(String)` / `ZipSlip(String)` 变体 |

### 4.4 Multipart 解压的复用

`extract_zip_to_bucket` 接收 `zip_stream: DynByteStream`,对 PutObject 路径来自请求 body,对 Multipart 路径来自 `kubo::cat::stream_cat(root_cid)`。解压逻辑完全复用,差异仅在 zip 流来源:

```rust
// PutObject 路径
let zip_stream = req.body;  // S3Request<Body>
let result = extract_zip_to_bucket(&state, bucket, key, prefix, zip_stream).await?;

// Multipart 路径(Complete 后)
let zip_stream = kubo::cat::stream_cat(&state.kubo, &root_cid).await?;
let result = extract_zip_to_bucket(&state, bucket, key, prefix, zip_stream).await?;
```

## 5. 安全考量

1. **路径穿越**:3.3 节算法,逐 entry 校验,发现穿越立即拒绝整个请求
2. **解压炸弹**:zip 可声称 1GB 解压成 100GB。MVP 不做大小限制(信任客户端),v1 加 `max_total_decompressed_size`
3. **文件数量**:zip 可含 10 万 entry。MVP 不限制,v1 加 `max_entry_count`
4. **鉴权**:复用 SigV4,无凭证请求被拒(默认 check_access)
5. **bucket 隔离**:解压目标必须与压缩包同 bucket,不允许跨 bucket

## 6. 客户端使用示例

### 6.1 Python boto3

```python
import boto3
import requests

s3 = boto3.client("s3", endpoint_url="https://ipfs3.moyuteam.me",
                  aws_access_key_id="pixivbot", aws_secret_access_key="...",
                  region_name="us-east-1")

# 生成 presigned URL,追加 decompress-zip 参数
url = s3.generate_presigned_url(
    "put_object",
    Params={"Bucket": "pixivbot-images", "Key": "2026/archive.zip"},
    HttpMethod="PUT",
)
url += "&decompress-zip=2026/"

with open("archive.zip", "rb") as f:
    resp = requests.put(url, data=f)
print(resp.status_code, resp.text)
```

### 6.2 curl + SigV4

```bash
curl -X PUT \
  "https://ipfs3.moyuteam.me/pixivbot-images/2026/archive.zip?decompress-zip=2026/" \
  -H "Authorization: AWS4-HMAC-SHA256 Credential=pixivbot/..." \
  -H "x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD" \
  --data-binary @archive.zip
```

### 6.3 AWS CLI(受限)

AWS CLI 不直接支持自定义 query 参数。需用 `--cli-input-json` 或 SDK。**WIP:评估 `aws s3api put-object` 是否能通过 `--metadata` 传递,待验证**。

### 6.4 Multipart Upload + decompress-zip

```python
import boto3
import requests

s3 = boto3.client("s3", endpoint_url="https://ipfs3.moyuteam.me",
                  aws_access_key_id="pixivbot", aws_secret_access_key="...",
                  region_name="us-east-1")

bucket = "pixivbot-images"
key = "2026/big-archive.zip"
prefix = "2026/"

# 1. CreateMultipartUpload,追加 decompress-zip
create_url = s3.generate_presigned_url(
    "create_multipart_upload",
    Params={"Bucket": bucket, "Key": key},
    HttpMethod="POST",
)
create_url += "&decompress-zip=" + prefix
resp = requests.post(create_url)
upload_id = parse_xml(resp.text).find("UploadId").text

# 2. UploadPart(标准,可多个 part)
parts = []
for i, chunk in enumerate(read_chunks("big-archive.zip", 5 * 1024 * 1024), start=1):
    upload_url = s3.generate_presigned_url(
        "upload_part",
        Params={"Bucket": bucket, "Key": key, "UploadId": upload_id, "PartNumber": i},
        HttpMethod="PUT",
    )
    r = requests.put(upload_url, data=chunk)
    etag = r.headers["ETag"]
    parts.append({"PartNumber": i, "ETag": etag})

# 3. CompleteMultipartUpload,触发解压
complete_url = s3.generate_presigned_url(
    "complete_multipart_upload",
    Params={"Bucket": bucket, "Key": key, "UploadId": upload_id},
    HttpMethod="POST",
)
complete_body = "<CompleteMultipartUpload>" + "".join(
    f"<Part><PartNumber>{p['PartNumber']}</PartNumber><ETag>{p['ETag']}</ETag></Part>"
    for p in parts
) + "</CompleteMultipartUpload>"
resp = requests.post(complete_url, data=complete_body)
print(resp.status_code, resp.text)
# DecompressZipResult XML,含 root_cid 与各解压文件 CID
```

## 7. 开放问题(WIP)

1. **tee 方案**:方案 A(双消费者)vs 方案 B(先存后解压)。初版倾向 B,待性能测试。
2. **entry 顺序**:zip entry 通常按文件名排序,但规范不保证。是否需要排序后写入?MVP 不排序。
3. **目录条目**:zip 可能有显式目录条目(name 以 `/` 结尾)。MVP 跳过,v1 可选创建 0 字节占位。
4. **压缩包本身存储失败**:是否应该尝试清理已解压的文件?MVP 不清理(best-effort),v1 加事务性。
5. **加密 zip**:zip 支持密码保护。MVP 拒绝加密 zip(返回 InvalidParameterValue),v1 可加密码参数。
6. **响应格式**:XML 还是 JSON?S3 惯例 XML,但自定义操作可自由。MVP 用 XML。
7. **幂等性**:同 key 重传是否覆盖?MVP 沿用 PutObject 覆盖语义,已存在的 key 被覆盖。
8. **Multipart 解压失败后 archive 保留**:Complete 后 archive 已入库,若解压全失败,archive 仍保留(符合设计)。是否需要返回 warning?MVP 在 Failures 中体现。
9. **Multipart part CID 回收时机**:Complete 后 part CID 已 pin_rm,若随后解压读取 root_cid(含全部 part 数据)是否仍可用?是 — root_cid 是完整拼接后重新 stream_add 的新 CID,独立于 part CID,part 回收不影响 root。

## 8. 测试策略

### 8.1 单元测试

- `sanitize_entry`:各种路径穿越变种、正常路径、目录条目、空 entry
- `extract_zip_to_bucket`:mock Kubo + SQLite,小 zip(3 个文件)

### 8.2 集成测试

- 正常解压(5 个文件,2 层目录)
- 路径穿越拒绝(4 种攻击模式)
- 部分失败(构造损坏 zip,第 3 个 entry 解压失败)
- 大文件流式(100MB zip,验证内存恒定)
- 压缩包本身可独立 GetObject
- Multipart + decompress-zip:多 part 上传 → Complete 触发解压 → 验证 archive 与解压文件均可用
- Multipart + decompress-zip-result=false:返回标准 CompleteMultipartUploadResult

### 8.3 端到端

- rclone / boto3 上传真实 zip,验证可 `aws s3 ls` 列出所有解压文件
- `ipfs cat <archive_cid>` 拿到原始 zip 字节
- `ipfs cat <extracted_cid>` 拿到解压后的单文件

---

> **WIP 状态说明**:本文档为初版意图表达,关键设计点(tee 方案、失败策略、幂等性)待讨论确认后细化。实现阶段以最终 plan 为准。
