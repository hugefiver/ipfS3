# S3-Compatible IPFS Gateway — Feasibility Research Report

**Date:** 2026-07-02
**Scope:** Rust + axum 构建 S3 兼容网关（后端 IPFS/Kubo）可行性调研
**Status:** Draft — pending review

---

## 1. 最小实用 S3 API 面

### 1.1 操作必需性分级

| 优先级 | 操作 | 原因 | 依赖方 |
|--------|------|------|--------|
| **P0 核心必需** | ListBuckets | aws-cli `ls`、rclone `lsd`、boto3 `list_buckets()` 都依赖 | 全部 |
| **P0 核心必需** | CreateBucket / DeleteBucket | aws-cli `mb`/`rb`、rclone `mkdir`/`rmdir`、s3cmd `mb`/`rb` | 全部 |
| **P0 核心必需** | HeadBucket | rclone 用此检查 bucket 是否存在；aws-cli 静默使用 | rclone、aws-cli |
| **P0 核心必需** | ListObjects / ListObjectsV2 | aws-cli `ls`、rclone `ls`/`sync`、boto3 `list_objects()` 核心路径 | 全部 |
| **P0 核心必需** | HeadObject | aws-cli `head-object`、rclone 检查文件元数据、boto3 `head_object()` | 全部 |
| **P0 核心必需** | GetObject | 读取对象内容，所有工具的核心操作 | 全部 |
| **P0 核心必需** | PutObject | 上传对象，所有工具的核心操作 | 全部 |
| **P0 核心必需** | DeleteObject | 删除对象，aws-cli `rm`、rclone `delete` | 全部 |
| **P0 核心必需** | CopyObject | aws-cli `cp`（同 bucket 内）、rclone `copyto`/`moveto` | aws-cli、rclone |
| **P0 核心必需** | CreateMultipartUpload | aws-cli 对大文件自动使用分片上传（阈值 ~8MB） | aws-cli、boto3 |
| **P0 核心必需** | UploadPart | 分片上传的组成部分 | aws-cli、boto3 |
| **P0 核心必需** | CompleteMultipartUpload | 完成分片上传 | aws-cli、boto3 |
| **P0 核心必需** | AbortMultipartUpload | 取消分片上传 | aws-cli、boto3 |
| **P1 强烈建议** | DeleteObjects (batch) | rclone sync 批量删除、aws-cli `--recursive` 优化 | rclone、aws-cli |
| **P1 强烈建议** | PutObjectAcl / GetObjectAcl | rclone 默认在写操作后设置 ACL（可通过 `--s3-no-acl` 禁用） | rclone |
| **P1 强烈建议** | GetBucketLocation | aws-cli、部分 SDK 会查询 region | aws-cli、SDK |
| **P2 可选增强** | Versioning (PutBucketVersioning, ListObjectVersions) | 只有启用版本管理时才需要 | 部分工作流 |
| **P2 可选增强** | Lifecycle (PutBucketLifecycle) | 自动过期/清理，非必需 | 运维 |
| **P2 可选增强** | CORS (PutBucketCors) | 浏览器直接访问时需要 | Web 应用 |
| **P2 可选增强** | Presigned URLs | 临时授权下载/上传链接 | 部分 SDK |
| **P2 可选增强** | UploadPartCopy | 分片复制（大文件 CopyObject 会退避到此） | aws-cli 大文件复制 |

**来源:**
- [rclone S3 文档](https://github.com/rclone/rclone/blob/master/docs/content/s3.md) — 明确列出 rclone 所需的最小权限集
- [NVIDIA AIStore S3 兼容矩阵](https://github.com/NVIDIA/aistore/blob/main/docs/s3compat.md) — 列出 s3cmd/aws-cli/boto3 的功能覆盖
- [Raff S3 兼容性](https://docs.rafftechnologies.com/products/store/object-storage/concepts/s3-compatibility) — 生产级 S3 兼容实现的 API 覆盖
- [MagaluCloud s3-tester](https://github.com/MagaluCloud/s3-tester) — S3 兼容性测试清单

### 1.2 SigV4 鉴权校验要点

SigV4 签名流程（AWS 标准）：

1. **构造 CanonicalRequest:**
   ```
   HTTPMethod + "\n" +
   CanonicalURI + "\n" +
   CanonicalQueryString + "\n" +
   CanonicalHeaders + "\n" +
   SignedHeaders + "\n" +
   HashedPayload
   ```

2. **构造 StringToSign:**
   ```
   "AWS4-HMAC-SHA256" + "\n" +
   RequestDateTime (YYYYMMDDTHHMMSSZ) + "\n" +
   CredentialScope (YYYYMMDD/region/s3/aws4_request) + "\n" +
   Hex(SHA256(CanonicalRequest))
   ```

3. **计算签名:**
   ```
   kDate = HMAC-SHA256("AWS4" + SecretKey, DateStamp)
   kRegion = HMAC-SHA256(kDate, Region)
   kService = HMAC-SHA256(kRegion, "s3")
   kSigning = HMAC-SHA256(kService, "aws4_request")
   signature = Hex(HMAC-SHA256(kSigning, StringToSign))
   ```

4. **Authorization header:**
   ```
   AWS4-HMAC-SHA256 Credential=AKID/DateStamp/region/s3/aws4_request,
   SignedHeaders=host;x-amz-content-sha256;x-amz-date,
   Signature=<signature>
   ```

**关键实现注意事项:**
- 必须签名的头: `Host`、`x-amz-*` 系列
- `x-amz-content-sha256` 对 PUT 请求应为请求体的 SHA-256
- `x-amz-date` 必须与签名中的日期一致
- 签名有时间窗口（通常 ±15 分钟），需要 NTP 同步
- Chunked Transfer Encoding 下的 payload 签名（aws-chunked）是可选的增强

**来源:**
- [AWS SigV4 签名流程](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html)
- [aws-sigv4 crate (Rust)](https://docs.rs/aws-sigv4/latest/src/aws_sigv4/http_request/canonical_request.rs.html)
- [SigV4 无 SDK 示例 (Rust)](https://github.com/aws-samples/sigv4-signing-examples/blob/main/no-sdk/rust/src/main.rs)

---

## 2. Rust S3 服务端框架选型

### 2.1 s3s (Nugine/s3s) — 主推荐

| 属性 | 详情 |
|------|------|
| **crates.io** | [`s3s` v0.13.0](https://crates.io/crates/s3s) (latest: 2026-03-01) |
| **下载量** | 387,874 total; ~205k/90d |
| **MSRV** | 1.92.0 |
| **License** | Apache-2.0 |
| **仓库** | <https://github.com/Nugine/s3s> |

**核心能力:**
- ✅ **SigV4 + SigV2 鉴权**: 内置 `S3Auth` trait，提供 `SimpleAuth` 实现；只需实现 `get_secret_key(access_key) -> SecretKey` 即可接入自己的密钥存储
- ✅ **S3 路由**: 自动解析 bucket、key、查询参数，转换为类型化的 `S3Request<OperationInput>`
- ✅ **错误响应生成**: 所有 S3 错误自动转为正确的 HTTP 状态码 + XML 错误体
- ✅ **Hyper Service**: 实现 `hyper::service::Service` 和 `tower::Service`，可与任何兼容框架互操作
- ✅ **DTO 类型**: 从 AWS Smithy 模型生成，覆盖完整 S3 API 类型
- ✅ **自定义路由**: `S3Route` trait 允许拦截特定路径，内嵌 axum Router
- ✅ **流式支持**: `Body` 类型基于 `hyper::body::Body`，支持流式读写
- ✅ **校验**: 内置 bucket/object 名称验证
- ✅ **配置热重载**: `S3ConfigProvider` 支持动态配置

**s3s + axum 集成路径:**

s3s 官方文档明确声明支持 axum 集成：
> "See the `axum` example in the examples directory for integration with Axum."
> "The crate includes several examples: `axum` — Integration with the Axum web framework"

s3s 的 `S3Service` 同时实现 `hyper::service::Service<Request<Incoming>>` 和 `tower::Service<Request<B>>`。axum 基于 tower，因此有两条路径：

**路径 A: S3Service 作为 axum fallback handler（推荐）**
```rust
// s3s 的 S3Service 实现 tower::Service，可当作任何 tower Service 使用
let s3_service = S3ServiceBuilder::new(my_s3_impl)
    .set_auth(my_auth)
    .build()
    .into_shared();

let app = Router::new()
    // 自定义路由（健康检查、管理 API 等）
    .route("/health", get(health_check))
    .route("/admin/keys", get(list_keys))
    // 其他所有请求交给 s3s
    .fallback_service(s3_service);
```

**路径 B: 通过 S3Route 内嵌 axum Router**
s3s 提供 `S3Route` trait，可在 S3Service 内拦截特定路径，用 axum Router 处理：
```rust
impl S3Route for CustomRoute {
    fn is_match(&self, method: &Method, uri: &Uri, ...) -> bool { ... }
    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<...>> {
        let mut service = self.router.clone().into_service::<Body>();
        // 转换 request/response 在 s3s 和 axum 之间
        ...
    }
}
```
来源: [s3s GitLab commit `7e51d088`](https://telematik.prakinf.tu-ilmenau.de/gitlab/pub/s3s/-/commit/7e51d088) 包含完整 axum 自定义路由示例。

**路径 C: 共享 State**
s3s 的 `S3` trait 实现本身就是你的业务逻辑结构体，可以直接持有 `Arc<AppState>`（与 axum 共享的 state），因为 `S3` trait 要求 `Send + Sync + 'static`。

### 2.2 其他方案对比

| Crate | 用途 | 成熟度 | 与 s3s 对比 |
|-------|------|--------|------------|
| **s3s** (Nugine) | S3 服务端框架 | ★★★★☆ 活跃开发，387k 下载 | **选这个** |
| **rust-s3** (durch/rust-s3) | S3 客户端 | ★★★★★ 成熟稳定 | 客户端库，不能做服务端 |
| **aws-sdk-s3** (official) | S3 客户端 | ★★★★★ AWS 官方 | 客户端，太重，不适合网关 |
| **multistore** (developmentseed) | S3 网关代理 | ★★★☆☆ 新兴 | 设计目标是代理而非存储后端；可参考其 SigV4 解析 |
| **aricanduva** (bltavares) | S3→IPFS 桥接 | ★★☆☆☆ 实验性 | Rust + SQLite + IPFS，可参考架构但功能不完整 |
| **s3x** (RTradeLtd) | S3→IPFS (Go) | ★★★☆☆ | Go 实现，MinIO 网关模式，可参考其 name→hash 映射设计 |
| **rs3gw** (cool-japan) | S3 网关 | ★★☆☆☆ 新兴 | 面向 AI/HPC，依赖 scirs2-io，不是 IPFS 后端 |

### 2.3 推荐结论

**s3s + axum 是可行且推荐的路径。** s3s 是 crates.io 上唯一成熟的 Rust S3 服务端框架，有 387k+ 下载量、活跃维护、内置 SigV4、axum 集成示例。你只需实现 `S3` trait（约 15 个方法对应 P0 操作），其余 HTTP 协议细节由 s3s 处理。

**来源:**
- [s3s crates.io](https://crates.io/crates/s3s)
- [s3s docs.rs](https://docs.rs/s3s/latest/s3s/)
- [s3s GitHub](https://github.com/Nugine/s3s)
- [s3s axum example commit](https://telematik.prakinf.tu-ilmenau.de/gitlab/pub/s3s/-/commit/7e51d088)

---

## 3. 加密方案设计

### 3.1 算法选型

| 算法 | 优势 | 劣势 | 推荐度 |
|------|------|------|--------|
| **AES-256-GCM** | 硬件加速 (AES-NI)、标准化、广泛审计 | 需要 nonce 管理（96-bit）、GCM nonce 重用灾难性 | ★★★★★ 默认推荐 |
| **ChaCha20-Poly1305** | 无硬件加速需求、恒定时间、移动友好 | 略慢于 AES-NI 加速的 GCM（x86 上） | ★★★★☆ 备选 |
| **AES-256-GCM-SIV** | 抗 nonce 重用（misuse-resistant） | 比 GCM 稍慢、支持不如 GCM 广泛 | ★★★★☆ 安全优先场景 |
| **age** (rage) | 文件级加密标准、简洁、多接收者 | 非流式设计（需完整文件）、不适合逐块加解密 | ★★☆☆☆ 不适合对象存储 |

**推荐: AES-256-GCM 为主，ChaCha20-Poly1305 为可选替代。** 在 x86 服务器上 AES-NI 提供 ~1-2 GiB/s 的加解密吞吐，足够网关使用。

**来源:**
- [age crate (docs.rs)](https://docs.rs/age/latest/age/)
- [crypt-io crate](https://github.com/jamesgober/crypt-io) — AES-256-GCM/ChaCha20-Poly1305 性能基准
- [enc_file crate](https://crates.io/crates/enc_file) — XChaCha20-Poly1305 + AES-256-GCM-SIV 流式加密

### 3.2 密钥层级设计

```
┌─────────────────────────────────────────────┐
│               Master Key (MK)                │  ← 环境变量/文件/secrets manager
│         256-bit, 离线生成, 定期轮换            │
└──────────────────┬──────────────────────────┘
                   │ HKDF-SHA256
     ┌─────────────┼─────────────┐
     ▼             ▼             ▼
┌─────────┐  ┌─────────┐  ┌─────────┐
│ Bucket  │  │ Bucket  │  │ Bucket  │   ← 逐 bucket 数据密钥 (BK)
│ Key A   │  │ Key B   │  │ Key C   │    BK = HKDF-Expand(MK, bucket_name)
└────┬────┘  └────┬────┘  └────┬────┘
     │            │            │
     ▼            ▼            ▼
┌─────────┐  ┌─────────┐  ┌─────────┐
│ Object  │  │ Object  │  │ Object  │   ← 逐对象数据密钥 (OK)
│ Key 1   │  │ Key 2   │  │ Key 3   │    OK = HKDF-Expand(BK, object_key || nonce)
└─────────┘  └─────────┘  └─────────┘
```

- **MK**: 永不直接用于加密数据。仅用于派生 BK。
- **BK**: 通过 HKDF 从 MK + bucket_name 派生。如果用户配置了"逐目录加密"，每个目录前缀可视为独立的 bucket 密钥域。
- **OK**: 通过 HKDF 从 BK + object_key + 随机 nonce 派生。每次 PutObject 生成新的随机 nonce。
- **包封装 (Envelope):** OK 用 BK 加密后存入元数据。格式: `{ "alg": "AES-256-GCM", "nonce": "<base64>", "encrypted_key": "<base64>", "key_derivation": "HKDF-SHA256" }`

### 3.3 密钥存储位置

| 方案 | 适用场景 | 优劣 |
|------|---------|------|
| **元数据 DB 内** | 单节点/dev 环境 | 简单，无额外依赖；密钥与元数据同库 |
| **独立 KMS (Vault/等)** | 生产/多节点 | 安全隔离、审计、自动轮换；增加运维复杂度 |
| **环境变量/文件** | dev/单机 | 最简单；不安全、无轮换 |

**推荐:**
- **Dev**: MK 来自环境变量/文件，BK/OK 包封装存储在元数据 DB
- **Prod**: MK 来自 HashiCorp Vault 或云 KMS，BK/OK 包封装存储在元数据 DB

### 3.4 S3 加密语义映射

| S3 标准 | 映射方案 | 实现方式 |
|---------|---------|---------|
| **SSE-S3** (x-amz-server-side-encryption: AES256) | 网关默认加密 | 不暴露加密细节给客户端；桶配置决定是否加密 |
| **SSE-KMS** (x-amz-server-side-encryption: aws:kms) | 网关 KMS 加密 | 对接 Vault/云 KMS；header 中传递 key-id |
| **SSE-C** (x-amz-server-side-encryption-customer-algorithm: AES256) | 客户提供密钥 | 解析 `x-amz-server-side-encryption-customer-key` header；密钥仅用于此次操作，不存储 |
| **自定义** (x-ipfs-encryption: aes-256-gcm) | 扩展方案 | 自定义 header 触发逐文件加密配置；不干扰标准 S3 客户端 |

**建议**: 初期实现 SSE-S3 语义（桶级配置决定加密）+ SSE-C（客户提供密钥）。如果需要逐文件加密配置，使用自定义 header `x-ipfs-encryption`。

### 3.5 流式加解密设计

```
PutObject (写入):
  客户端 ──[chunked body]──▶ axum ──[Stream<Bytes>]──▶ 加密层 ──[encrypted chunks]──▶ IPFS Kubo /api/v0/add

GetObject (读取):
  IPFS Kubo /api/v0/cat ──[chunked body]──▶ 解密层 ──[Stream<Bytes>]──▶ axum ──[chunked response]──▶ 客户端
```

**实现要点:**
- 使用 `tokio_util::io::StreamReader` 将 `Stream<Bytes>` 转为 `AsyncRead`
- 使用 `futures::stream` 或自定义 `AsyncRead` 包装器逐块加解密
- AES-256-GCM 支持流式处理（每 chunk 独立认证），但需处理 chunk 边界（GCM 认证标签在末尾）
- 推荐 chunk 大小: 4-64 MiB（平衡内存与 IPFS block 大小）

**来源:**
- [SSE-C REST API 规范](https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-s3-c-encryption.html)
- [AWS S3 SSE-C 文档](https://github.com/awsdocs/amazon-s3-developer-guide/blob/master/doc_source/ServerSideEncryptionCustomerKeysSSEUsingRESTAPI.md)
- [axum stream-to-file example](https://github.com/tokio-rs/axum/blob/main/examples/stream-to-file/src/main.rs)

---

## 4. 元数据存储选型

### 4.1 存储需求

| 数据类型 | 内容 | 索引需求 |
|---------|------|---------|
| Bucket 元数据 | bucket_name, created_at, encryption_config | 按 name 查询 |
| Object 索引 | key → CID, size, etag, last_modified, content_type, encryption_envelope | 按 bucket+prefix 范围扫描 (ListObjects) |
| Multipart 状态 | upload_id, bucket, key, parts[], created_at, expires_at | 按 upload_id 查询 |

### 4.2 嵌入式数据库对比

| 方案 | 版本 | 活跃度 | 类型 | 优势 | 劣势 | 推荐度 |
|------|------|--------|------|------|------|--------|
| **rusqlite** | 0.40.1 (2026-06) | ★★★★★ 非常活跃 | SQL 嵌入式 | 成熟的 SQL、ACID、广泛使用、bundled 编译 | 单写者并发限制 | ★★★★★ **主推荐** |
| **sled** | 0.34.7 (2021) | ★☆☆☆☆ 半维护 | KV 嵌入式 | 高性能、无锁 B+ tree | 3年未更新稳定版、依赖安全警告 (RUSTSEC-2024-0384)、v1 长期 alpha | ★★☆☆☆ **不推荐** |
| **redb** | 4.x (2025) | ★★★★☆ 活跃 | KV 嵌入式 | 纯 Rust、MVCC、类型安全 | 相对较新、生态较小 | ★★★☆☆ 备选 |
| **libsql** (Turso) | — | ★★★★☆ 活跃 | SQL 嵌入式 | SQLite 兼容 + 分布式扩展 | 本地模式性能曾有争议 | ★★★☆☆ 备选 |

**sled 不推荐理由:**
- 最新稳定版 0.34.7 发布于 2021 年，3+ 年无更新
- 间接依赖 `instant` crate（已标记为 unmaintained: [RUSTSEC-2024-0384](https://rustsec.org/advisories/RUSTSEC-2024-0384)）
- 作者自己说: "if reliability is your primary constraint, use SQLite. sled is beta."
- 社区讨论: [reddit r/rust](https://www.reddit.com/r/rust/comments/1dsmj9d/embedded_keyvalue_database_2024/) "sled is kind of half maintained"

**来源:**
- [sled GitHub issues](https://github.com/spacejam/sled/issues/1514)
- [sled crates.io](https://crates.io/crates/sled)
- [rusqlite crates.io](https://crates.io/crates/rusqlite)
- [redb GitHub](https://github.com/cberner/redb)

### 4.3 分布式复制方案

| 方案 | 模式 | 成熟度 | 适用场景 |
|------|------|--------|---------|
| **openraft** (Databend) | Raft 共识 | ★★★★☆ 生产使用 | 多节点强一致共享元数据 |
| **rqlite** | Raft + SQLite | ★★★★★ 成熟 | 分布式 SQLite（外部进程） |
| **litestream** | SQLite WAL 复制 | ★★★★☆ 成熟 | 异步复制到 S3/对象存储 |
| **Turso/libsql** | 分布式 SQLite | ★★★☆☆ 发展 | 边缘分布式 SQL |

### 4.4 环境推荐

| 环境 | 元数据存储 | 理由 |
|------|-----------|------|
| **Dev (docker compose)** | rusqlite (bundled) | 零外部依赖，`docker compose up` 即可，数据持久化到 volume |
| **单节点生产** | rusqlite + WAL 模式 | SQLite WAL 模式支持并发读 + 单写者，对元数据查询足够；定期备份 |
| **多节点生产** | rusqlite + openraft 或 PostgreSQL | 如果需要多网关共享元数据：初期可用 rusqlite + openraft (Raft 复制 SQLite)，或直接上 PostgreSQL |

### 4.5 推荐架构

```
Dev:
  ┌──────────┐     ┌──────────┐
  │  axum    │────▶│ rusqlite │  (单文件 DB)
  │  gateway │     │  (WAL)   │
  └──────────┘     └──────────┘

Prod (多节点):
  ┌──────────┐     ┌──────────┐     ┌──────────┐
  │  axum    │     │  axum    │     │  axum    │
  │ gateway 1│     │ gateway 2│     │ gateway 3│
  └────┬─────┘     └────┬─────┘     └────┬─────┘
       │   Raft          │   Raft        │
       └─────────────────┼──────────────┘
                         │
              ┌──────────▼──────────┐
              │   openraft cluster  │
              │   + rusqlite state  │
              └─────────────────────┘
```

**来源:**
- [openraft GitHub](https://github.com/databendlabs/openraft)
- [openraft Getting Started](https://databendlabs.github.io/openraft/getting-started.html)

---

## 5. axum 流式上传/下载

### 5.1 流式写入 IPFS (PutObject)

axum 的 `Request<Body>` 提供 `into_body().into_data_stream()`，返回 `Stream<Item = Result<Bytes, _>>`。

```text
流程:
  axum handler 接收 Request<Body>
  → request.into_body().into_data_stream()
  → StreamReader::new(stream)  // tokio_util 将 Stream → AsyncRead
  → 构造 multipart/form-data 请求体
  → POST /api/v0/add 到 Kubo（流式传输请求体）
  → 解析 JSON 响应获取 CID
```

**关键 crate:**
- `tokio_util::io::StreamReader` — Stream → AsyncRead 转换
- `reqwest` (streaming body) 或 `hyper::Client` — 向 Kubo 发送流式请求
- `common_multipart_rfc7578` 或手动构造 multipart — 构造 Kubo `/api/v0/add` 所需的 multipart 请求体

**来源:**
- [axum stream-to-file example](https://github.com/tokio-rs/axum/blob/main/examples/stream-to-file/src/main.rs)
- [Kubo /api/v0/add API 文档](https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-add)

### 5.2 流式读取 IPFS (GetObject)

```text
流程:
  POST /api/v0/cat?arg=<CID> 到 Kubo
  → 响应体为 chunked transfer encoding (原始文件内容)
  → 将响应体 Stream 直接映射到 axum Response Body
  → 设置 Content-Type、Content-Length、ETag 等 header
```

**axum 实现要点:**
- axum 的 `Body` 类型支持 `From<StreamBody>` 或 `Body::from_stream()`
- 使用 `axum::body::Body::from_stream(stream)` 将 `Stream<Bytes>` 直接作为响应体
- 或使用 `http::Response<Body>` 手动构建响应

**关键 crate:**
- `ipfs-api-backend-hyper` — Rust IPFS HTTP 客户端，`cat()` 返回 `impl Stream<Item = Bytes>`
- 或直接用 `reqwest` 调用 Kubo HTTP API

**来源:**
- [ipfs-api crate (docs.rs)](https://docs.rs/ipfs-api/latest/ipfs_api/) — 提供 `add()`、`cat()`、`get()` 等 API
- [Kubo RPC API 文档](https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-cat)

### 5.3 流式架构总览

```
PutObject:
  Client ──PUT /bucket/key──▶ axum ──Stream<Bytes>──▶ [加密层] ──▶ reqwest/hyper POST multipart ──▶ Kubo /api/v0/add ──▶ CID

GetObject:
  Client ◀──200 OK chunked── axum ◀──Stream<Bytes>── [解密层] ◀── reqwest/hyper POST ── Kubo /api/v0/cat?arg=CID
```

**避免全量缓冲的关键:**
1. 永远不要 `collect().await` 整个 body
2. 使用 `StreamReader` 桥接 Stream 和 AsyncRead
3. reqwest 支持 `Body::wrap_stream()` 实现流式请求体
4. IPFS `/api/v0/add` 原生支持 chunked transfer encoding（multipart 流式上传）

---

## 6. 错误响应格式

### 6.1 S3 XML 错误格式

AWS S3 REST API 在出错时返回 XML 格式的错误体：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The resource you requested does not exist</Message>
  <Resource>/mybucket/myfoto.jpg</Resource>
  <RequestId>4442587FB7D0A2F9</RequestId>
</Error>
```

### 6.2 核心错误码 → HTTP 状态码映射

| S3 Error Code | HTTP Status | 含义 | 触发场景 |
|---------------|-------------|------|---------|
| `AccessDenied` | 403 Forbidden | 权限不足 | SigV4 签名无效、无权限 |
| `NoSuchBucket` | 404 Not Found | Bucket 不存在 | HeadBucket/GetObject 到不存在的 bucket |
| `NoSuchKey` | 404 Not Found | Object 不存在 | GetObject/HeadObject 到不存在的 key |
| `BucketAlreadyExists` | 409 Conflict | Bucket 已存在 | CreateBucket 重复名称 |
| `BucketNotEmpty` | 409 Conflict | Bucket 非空 | DeleteBucket 时有对象存在 |
| `InvalidBucketName` | 400 Bad Request | Bucket 名称不合法 | 不符合命名规范 |
| `EntityTooSmall` | 400 Bad Request | 分片太小 | UploadPart 小于 5MB（除最后一片） |
| `InvalidPart` | 400 Bad Request | 分片无效 | CompleteMultipartUpload 时 part 列表不完整 |
| `NoSuchUpload` | 404 Not Found | 分片上传不存在 | UploadPart/CompleteMultipartUpload 的 uploadId 无效 |
| `MalformedXML` | 400 Bad Request | XML 格式错误 | CompleteMultipartUpload 请求体 XML 格式错误 |
| `InternalError` | 500 Internal Server Error | 内部错误 | IPFS 连接失败、DB 错误等 |
| `ServiceUnavailable` | 503 Service Unavailable | 服务不可用 | IPFS 节点不可达 |
| `SignatureDoesNotMatch` | 403 Forbidden | 签名不匹配 | SigV4 签名验证失败 |
| `RequestTimeTooSkewed` | 403 Forbidden | 时间偏差过大 | 请求时间与服务器时间差 > 15 分钟 |

### 6.3 s3s 错误处理

s3s 框架**自动处理**错误码到 HTTP 状态码的映射和 XML 序列化。你只需返回 `S3Error`:

```rust
// s3s 提供标准错误构造
S3Error::new(StatusCode::NOT_FOUND, "NoSuchKey", "The specified key does not exist.")
```

s3s 的 `S3Result<T>` = `Result<T, S3Error>`，框架将其转为正确的 HTTP 响应。

**来源:**
- [AWS S3 Error Responses](https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html)
- [AWS S3 Error Best Practices](https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorBestPractices.html)
- [AWS S3 API Reference PDF](http://awsdocs.s3.amazonaws.com/S3/latest/s3-api.pdf) — 完整错误码列表

---

## 7. 总结与推荐

### 7.1 Rust Crate 选型推荐表

| 用途 | 推荐 Crate | 版本 | 成熟度 | 替代方案 |
|------|-----------|------|--------|---------|
| S3 服务端框架 | **s3s** | 0.13.0 | ★★★★☆ | multistore（新兴，功能不同） |
| HTTP 框架 | **axum** | 0.8.x | ★★★★★ | actix-web（s3s 也兼容） |
| IPFS 客户端 | **ipfs-api-backend-hyper** | 0.6.x | ★★★☆☆ | 直接用 reqwest + Kubo HTTP API |
| 元数据存储 (dev) | **rusqlite** | 0.40.x | ★★★★★ | redb (纯 Rust KV) |
| 元数据存储 (prod) | **rusqlite** + openraft | — | ★★★★☆ | PostgreSQL |
| 加密 (AEAD) | **aes-gcm** (RustCrypto) | 0.10.x | ★★★★★ | chacha20poly1305 |
| 密钥派生 (KDF) | **hkdf** (RustCrypto) | 0.12.x | ★★★★★ | — |
| HTTP 客户端 | **reqwest** | 0.12.x | ★★★★★ | hyper::Client |
| 流式 I/O | **tokio-util** | 0.7.x | ★★★★★ | — |
| 序列化 | **serde** + **serde_json** | 1.x | ★★★★★ | — |
| 共识协议 (多节点) | **openraft** | 0.9.x | ★★★★☆ | rqlite (外部进程) |

### 7.2 关键发现

1. **s3s 真实可用**: crates.io 上 387k+ 下载，活跃维护至 2026-03，内置 SigV4 鉴权，有 axum 集成示例。这是 Rust 生态中唯一的成熟 S3 服务端框架。

2. **SigV4 可复用**: s3s 内置 SigV4 + SigV2 验证，只需实现 `S3Auth` trait 提供密钥查找，无需自行实现签名算法。

3. **sled 不推荐**: 3 年未更新稳定版，有已知安全警告，作者自己推荐 SQLite。rusqlite 是成熟、活跃的替代方案。

4. **流式架构可行**: axum 原生支持 `Body::into_data_stream()` 流式读取，IPFS Kubo `/api/v0/add` 和 `/api/v0/cat` 原生支持 chunked transfer，桥接方案成熟。

5. **加密层级**: 推荐 HKDF 密钥派生 + AES-256-GCM 包封装方案，避免全量内存缓冲，支持逐 chunk 加解密。

6. **已有先例**: [aricanduva](https://github.com/bltavares/aricanduva) (Rust + SQLite + IPFS) 和 [s3x](https://github.com/RTradeLtd/s3x) (Go + MinIO + IPFS) 验证了 S3→IPFS 网关的可行性。aricanduva 的 "name→hash 映射 + SQLite" 设计可直接参考。

### 7.3 待验证项

| 项目 | 状态 | 备注 |
|------|------|------|
| s3s axum example 实际可运行性 | ⚠️ 需验证 | 官方文档说有 axum example，建议 clone s3s repo 实际运行 |
| IPFS `/api/v0/add` 大文件流式上传稳定性 | ⚠️ 需验证 | 历史上 Kubo 的 chunked encoding 有 bug (issue #3332)，需测试 |
| rusqlite WAL 模式在多网关并发下的表现 | ⚠️ 需验证 | WAL 支持多读单写，对元数据操作模式是否足够需要压力测试 |
| AES-256-GCM 逐 chunk 加解密与 S3 ETag 计算 | ⚠️ 需验证 | GCM 认证标签在 chunk 末尾，需设计 chunk 边界方案 |
| openraft + rusqlite 集成的复杂度 | ⚠️ 需验证 | 有官方示例但需要实际评估集成工作量 |
