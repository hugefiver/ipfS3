# IPFS S3 Gateway 设计规格

- **日期**: 2026-07-02
- **状态**: 已批准（待用户最终复核）
- **作者**: brainstorm 会话产出
- **关联调研**: `docs/kubo-s3-feasibility-report.md`, `docs/research/s3-ipfs-gateway-feasibility.md`, `docs/ipfs-s3-feasibility-research.md`

---

## 1. 目标与范围

### 1.1 项目目标

构建一个基于 IPFS（Kubo）的 S3 兼容网关，用 Rust + axum 实现。文件存储在 IPFS（内容寻址、不可变、去重），S3 语义通过元数据层补齐。支持明文存储（经任何 IPFS Gateway 直接访问）与逐对象加密。

### 1.2 MVP 范围

MVP 包含以下功能：

1. **核心 S3 CRUD**：ListBuckets, Create/DeleteBucket, HeadBucket, ListObjectsV2, Head/Get/Put/DeleteObject, CopyObject
2. **SigV4 鉴权**：aws cli / aws-sdk 标准签名校验
3. **加密**：AES-256-GCM 逐 chunk 加解密，支持 SSE-S3 / SSE-C / 明文三种模式
4. **Multipart Upload**：CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts
5. **docker compose 一键部署**：本地离线可运行

### 1.3 不在 MVP 范围

- 多节点分布式部署（IPFS Cluster、多 gateway 实例）
- Versioning / Lifecycle / Presigned URL / Object Tagging
- IAM / STS / 临时凭证
- Pinning Service 集成（架构预留接口，Noop 实现）
- DeleteObjects 批量删除

### 1.4 验收标准

1. `docker compose up` 一键启动，容器健康
2. `aws s3 mb s3://test-bucket` 成功
3. `aws s3 cp local.txt s3://test-bucket/path/to/file.txt` 成功，CID 可查
4. `aws s3 cp s3://test-bucket/path/to/file.txt download.txt` 内容一致
5. `aws s3 ls s3://test-bucket/` 列出对象
6. `aws s3 rm s3://test-bucket/path/to/file.txt` 后再 get 返回 404（MVP 不解除 pin，依赖 dev 禁用 GC）
7. `aws s3 cp s3://src s3://dst` CopyObject 成功，src 与 dst CID 相同
8. `curl -r 0-99` Range 下载前 100 字节正确（明文对象）
9. 错误凭证返回 403 SignatureDoesNotMatch
10. Multipart 上传 100MB 文件，分 10 片，Complete 后 GET 内容一致
11. 加密对象 PutObject（带 `--sse AES256`）后，`ipfs cat <cid>` 返回密文（非明文）；GET 经网关解密返回明文
12. 明文对象 PutObject（默认无加密头）后，`ipfs cat <cid>` 返回明文
13. SSE-C 客户端密钥 Put + Get 成功；错误密钥 Get 失败
14. ETag 返回对象 CID 字符串（非标准 S3 MD5，aws cli 兼容但 `--no-progress` 下可见差异）
15. 中止服务后 `docker compose down -v` 清理干净

---

## 2. 架构

### 2.1 总体架构

单一 Rust 二进制 `ipfs-s3-gateway`，三层结构：

```
aws cli ──SigV4──► axum(:9000)
                     │
                     ▼ fallback_service
                  s3s (SigV4校验 + S3路由 + XML错误)
                     │
                     ▼ S3 trait
                  业务层 (持有 Arc<AppState>)
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
    Kubo RPC     sea-orm       crypto/
    (:5001)      (PG/SQLite)   (AES-GCM)
    /add /cat    元数据存储    加解密流
    /pin /dag
```

**AppState**（`Arc` 共享）持有：
- Kubo HTTP 客户端（reqwest，带连接池与超时）
- sea-orm `DatabaseConnection`
- 静态凭证表（`HashMap<access_key, secret_key>`，启动时从配置加载）
- Master Key（加密用，从配置加载）

### 2.2 关键选型

| 决策点 | 选择 | 理由 |
|---|---|---|
| S3 协议层 | s3s v0.13.0 + axum | 内置 SigV4 + 路由 + XML 错误，省 ~60% 样板 |
| 元数据存储 | sea-orm（PG/SQLite 双后端） | dev 零依赖（SQLite），prod 零迁移（PG），单一 migration |
| 读写路径 | 全走 Kubo RPC(:5001) | 加密迭代改造可控，不依赖 Kubo Gateway |
| 加密 | AES-256-GCM 逐 chunk | 硬件加速，与 IPFS 分块对齐 |
| Multipart | 每 part 独立 add + Complete 时整体 add 生成根 CID | 内容寻址原生，无临时盘，规避 dag-pb 手动编码 |
| NAT 部署 | 预留 PinningService trait + Noop 实现 | MVP 本地自洽，后续接入 Pinata 等 |

### 2.3 模块结构

```
src/
├── main.rs              # 入口：加载配置→初始化 AppState→启动 axum
├── config.rs            # 配置（环境变量为主，可选 config 文件覆盖）
├── state.rs             # AppState 定义与初始化
├── error.rs             # AppError → S3Error 映射
├── auth.rs              # S3Auth trait 实现（access_key→secret_key 查询）
├── kubo/                # Kubo RPC 客户端
│   ├── mod.rs
│   ├── client.rs        # reqwest 封装，base_url + 超时 + 连接池
│   ├── add.rs           # 流式 POST /api/v0/add → CID
│   ├── cat.rs           # 流式 POST /api/v0/cat?arg=CID&bytes=from-to
│   ├── pin.rs           # POST /api/v0/pin/add, /pin/rm
│   ├── dag.rs           # POST /api/v0/dag/get（Multipart Complete 时读取 part 结构，可选）
├── store/               # 元数据存储（sea-orm）
│   ├── mod.rs           # 业务查询接口
│   ├── entities/        # sea-orm entity: bucket, object, multipart_upload, multipart_part
│   └── migration/       # 单一 migration（PG/SQLite 兼容）
├── crypto/              # 加密
│   ├── mod.rs           # CryptoStream: impl Stream<Item=Bytes>
│   ├── key.rs           # MK 加载, OK 生成/包封/解包
│   ├── aes_gcm.rs       # 逐 chunk 加解密 + nonce 派生
│   └── chunker.rs       # 固定大小 chunker（SizedChunkStream）
├── pinning/             # Pinning Service 抽象
│   ├── mod.rs           # PinningService trait
│   └── noop.rs          # Noop 实现（MVP）
└── s3/                  # S3 trait 实现
    ├── mod.rs           # S3Service 持有 Arc<AppState>
    ├── handler.rs       # S3 trait 方法分发
    └── ops/
        ├── bucket.rs    # ListBuckets, Create/DeleteBucket, HeadBucket
        ├── object.rs    # Head/Get/Put/Delete/Copy Object, ListObjectsV2
        └── multipart.rs # CreateMultipartUpload, UploadPart, Complete/Abort, ListParts
```

**模块边界原则**：
- `kubo/` 只懂 HTTP，不懂 S3 语义；可独立用 mock HTTP 测试
- `store/` 只懂 DB，不懂 Kubo；可独立用 in-memory SQLite 测试
- `crypto/` 只懂加解密流变换，不懂 S3 也不懂 Kubo
- `s3/` 编排上述模块，实现业务逻辑

---

## 3. 数据流

### 3.0 Bucket 操作

Bucket 操作为纯 DB 操作，不触及 Kubo：

- **CreateBucket**：`store::create_bucket(name, owner)` → 201（已存在则 409 `BucketAlreadyOwnedByYou`）
- **DeleteBucket**：`store::delete_bucket(name)` → 204（不存在则 404 `NoSuchBucket`；非空则 409 `BucketNotEmpty`）
- **HeadBucket**：`store::bucket_exists(name)` → 200（不存在则 404 `NoSuchBucket`）
- **ListBuckets**：`store::list_buckets(owner)` → 200 + XML

### 3.1 PutObject（流式，永不 collect）

**明文模式（默认，无加密头）**：
```
Request Body ──stream──► kubo::add::stream(body) ──► Kubo /api/v0/add
                                                          │
                                                          ▼ CID
                                              kubo::pin::add(cid)
                                              pinning::pin(cid)  [Noop]
                                                          │
                                                          ▼
                                              store::upsert_object(bucket, key, cid, size, etag=cid, content_type, encrypted=false)
                                                          │
                                                          ▼
                                              返回 ETag + 200 OK
```

**加密模式（SSE-S3，带 `x-amz-server-side-encryption: AES256`）**：
```
Request Body ──► crypto::chunker(256KiB) ──► crypto::aes_gcm::encrypt(OK, nonce)
                                                          │
                                                          ▼ 密文流
                                              kubo::add::stream(ciphertext) ──► CID
                                                          │
                                                          ▼
                                              kubo::pin::add(cid)
                                                          │
                                                          ▼
                                              store::upsert_object(..., encrypted=true, key_wrap=wrap(MK, OK))
```

> **x-amz-meta-\* 自定义元数据**：PutObject 时提取所有 `x-amz-meta-*` 请求头，序列化为 JSON 存入 `objects.metadata`。GetObject/HeadObject 时反序列化并还原为响应头。

### 3.2 GetObject（流式，支持 Range）

**明文模式**：
```
store::get_object(bucket, key) ──► (cid, size, content_type, encrypted=false, metadata)
                                          │
                                          ▼ Range header → bytes=from-to
                          kubo::cat::stream(cid, bytes=from-to) ──► Kubo /api/v0/cat
                                          │
                                          ▼
                          axum Body::from_stream(kubo_response.stream)
                                          │
                                          ▼
                          200 OK + Content-Range + Content-Length + body
```

**加密模式**：
```
store::get_object(bucket, key) ──► (cid, encrypted=true, key_wrap)
                                          │
                                          ▼ unwrap(MK) → OK
                          kubo::cat::stream(cid)  [完整拉取，Range 在解密后计算]
                                          │
                                          ▼ 密文流
                          crypto::chunker(256KiB + 28) ──► crypto::aes_gcm::decrypt(OK, nonce)
                                          │
                                          ▼ 明文流
                          [若 Range] 截取 Range 对应字节
                                          │
                                          ▼
                          axum Body::from_stream(plaintext_stream)
```

> **Range 与加密**：加密模式下 Range 复杂——密文按 chunk 加密，无法直接对密文 Range。MVP 策略：加密对象先完整解密为流，再按 Range 截取明文字节。代价是加密对象 Range 下载需服务端缓冲完整解密流。后续可优化为 chunk 级 Range（计算 Range 覆盖哪些 chunk，只拉取并解密这些 chunk）。
>
> **Content-Length**：始终返回明文 size（`objects.size` 存明文大小），与是否加密无关。

### 3.3 HeadObject / HeadBucket

**HeadObject**：纯 DB 查询，不调用 Kubo（元数据已在 DB）：
1. `store::get_object(bucket, key)` → 元数据
2. 返回 200 + 头：
   - `Content-Length: size`（明文大小）
   - `Content-Type: content_type`
   - `ETag: cid`
   - `Last-Modified: created_at`
   - 若 `encrypted=true` 且 SSE-S3：`x-amz-server-side-encryption: AES256`
   - 若 SSE-C：`x-amz-server-side-encryption-customer-algorithm: AES256` + `x-amz-server-side-encryption-customer-key-MD5: <回传客户端 MD5>`
   - `x-amz-meta-*`：从 `metadata` JSON 还原
3. 不存在则 404 `NoSuchKey`（bucket 不存在 404 `NoSuchBucket`）

**HeadBucket**：`store::bucket_exists(name)` → 200 或 404 `NoSuchBucket`

### 3.4 ListObjectsV2

纯 DB 查询，不触及 Kubo：
```sql
SELECT key, size, etag, created_at AS last_modified
FROM objects
WHERE bucket = ? AND is_latest = TRUE AND key LIKE ? || '%'
ORDER BY key
LIMIT ? OFFSET ?
```

### 3.5 CopyObject

源对象 CID 不变（内容寻址天然去重），DB 插入新记录指向同一 CID：
```
store::get_object(src) ──► (cid, size, ...)
store::upsert_object(dst_bucket, dst_key, cid, size, ...)  # 同一 CID
kubo::pin::add(cid)  # 重复 pin 同一 CID 是幂等操作，Kubo pin API 不计数，安全
```

> **Pin 引用计数限制**：Kubo pin API 不维护引用计数，`pin::rm` 会直接解除 pin 无论多少对象引用。因此 DeleteObject 时不能简单 `pin::rm`——会误删被其他 key 引用的 CID。MVP 简化策略：**DeleteObject 只删 DB 记录，不立即 pin::rm**，依赖 dev 环境禁用 GC 保证内容不丢。后续迭代在 DB 维护引用计数或改用 MFS 管理引用。

### 3.6 DeleteObject

```
store::get_object(bucket, key) ──► (cid)
store::delete_object(bucket, key)
# MVP: 不立即 kubo::pin::rm(cid)，见 3.5 CopyObject 的 pin 引用计数说明
# 依赖 dev 环境禁用 GC 保证内容不丢
```

dev 环境 GC 周期设长或禁用，避免误删。

---

## 4. 加密设计

### 4.1 密钥层级

**AES-256-GCM** 为主算法（硬件加速 AES-NI，~1-2 GiB/s），每对象独立密钥，密钥不落明文盘。

```
Master Key (MK)  ← 环境变量/配置（dev），KMS/Vault（prod 预留）
    │
    ▼ 随机生成 OK（256 bit）
Object Key (OK)  ← 每对象随机生成，用 MK 包封后存 DB
    │
    ▼ AES-256-GCM 加密对象内容
```

**OK 随机生成**（非 HKDF 派生），用 MK 包封后存 `objects.key_wrap` 列。MK 轮换时只需重新包封 OK，不重新加密内容。

### 4.2 逐 chunk 加密

采用**逐 chunk 加密**，每个 chunk 独立 AES-GCM，与 IPFS 分块对齐：

```
明文流 → 按 256KiB chunk（与 Kubo /add 默认 chunk size 对齐）
       → 每个 chunk: nonce(12B) + ciphertext + tag(16B)
       → 密文流 → Kubo /add
```

**密文格式**（每 chunk）：`[nonce(12B) || ciphertext(=plaintext_len) || tag(16B)]`
- 密文长度 = 明文长度 + 28 字节/chunk overhead
- 解密时需按 256KiB + 28 边界切分密文流

### 4.3 Nonce 策略

每个 chunk 用 `(object_id || chunk_index)` 作为 nonce 输入（确定性，无随机 nonce 存储开销，避免 nonce 重用）。

- `object_id`：DB 主键，UUID 字符串（如 `550e8400-e29b-41d4-a716-446655440000`），取其 UTF-8 字节表示（36 字节）
- `chunk_index`：chunk 在对象内的序号（从 0 开始，u64，8 字节小端序）
- 组合为 44 字节输入，经 HKDF-SHA256 派生为 12 字节 GCM nonce（避免直接截断导致的子集碰撞风险）
- **Multipart 场景**：`object_id` 对整个 Multipart 上传唯一（CreateMultipartUpload 时预生成并写入 `multipart_uploads.object_id` 列），`chunk_index` 在每个 part 内从 0 重新计数

### 4.4 SSE 映射

默认行为对齐 S3 标准：**无加密头 = 明文存储**（aws cli 默认上传为明文，可经 IPFS Gateway 直接访问）。

| S3 加密头 | 行为 | OK 存储 |
|---|---|---|
| 无头 | **明文**：不加密，直接存 IPFS。可经任何 IPFS Gateway `ipfs://<cid>` 访问 | N/A（`encrypted=false, key_wrap=NULL`） |
| `x-amz-server-side-encryption: AES256` | **SSE-S3**：用配置的 MK 加密。响应头含 `x-amz-server-side-encryption: AES256` | `key_wrap = wrap(MK, OK)` |
| `x-amz-server-side-encryption-customer-algorithm: AES256` + `x-amz-server-side-encryption-customer-key` + `x-amz-server-side-encryption-customer-key-MD5` | **SSE-C**：客户提供密钥。OK = 客户密钥，不存 DB | `key_wrap = NULL` |
| 自定义 `x-ipfs-encryption: none` | 强制明文（显式声明，与无头等价） | N/A |

> **与 S3 标准的差异**：AWS S3 支持 bucket-level default encryption（无头也加密）。本网关 MVP 不实现 bucket 级默认加密策略，无头即明文。后续迭代可通过 bucket 配置项支持。

### 4.5 明文访问路径

未加密对象可通过任何 IPFS Gateway 直接 `ipfs://<cid>` 访问。加密对象只能通过 S3 API（网关解密）。DB `objects.encrypted` 列标记。

---

## 5. Multipart Upload 设计

### 5.1 实现策略

每 part 独立 add 到 Kubo，Complete 时按序 cat 各 part 拼成连续流后整体 `/api/v0/add` 生成根 CID。此方案规避手动构造 UnixFS PBNode 的 dag-pb 编码复杂度，依赖 Kubo 内建 UnixFS builder（内容已在本地 pin，cat 为本地读，开销可控）。

> **为什么不用 `/dag/put`**：手动构造 UnixFS PBNode 需正确编码 dag-pb protobuf 节点（Data 字段 + Links 数组），调研报告标记为高复杂度。整体 add 让 Kubo 自行构造 UnixFS balanced/trickle DAG，实现简单且可靠。

### 5.2 操作流程

**CreateMultipartUpload**：
1. 生成 `upload_id`（UUID）和 `object_id`（UUID，用于加密 nonce 派生）
2. DB `multipart_uploads` 插入记录（含 `object_id`、`encryption_mode`、`key_wrap`、`content_type`、`metadata`）
3. 返回 `{Bucket, Key, UploadId}`

**UploadPart**：
1. body（可能加密，nonce 用 `multipart_uploads.object_id` 派生）→ `kubo::add::stream` → part-CID
2. `kubo::pin::add(part_cid)`（direct pin）——**先 pin 后写 DB**
3. DB `multipart_parts` 插入（upload_id, part_number, cid, size, etag）
   - 若 DB insert 失败：执行 `kubo::pin::rm(part_cid)` 回滚，返回 500
4. 返回 ETag = **part-CID 字符串**（与对象级 ETag=CID 策略一致）
5. **校验**：非最后一片若 size < 5MB，返回 `EntityTooSmall` 400

**CompleteMultipartUpload**：
1. 解析客户端请求体 XML，提取 Part List（每个 part 含 `PartNumber` + `ETag`）
2. 验证 Part List：
   - PartNumber 必须升序排列（否则 `InvalidPartOrder` 400）
   - 客户端 list 中的每个 part 必须在 DB `multipart_parts` 中存在（否则 `InvalidPart` 400）
   - 每个 part 的 ETag（= part-CID）与 DB 记录匹配（否则 `InvalidPart` 400）
   - S3 不要求 PartNumber 连续，也不要求 list 包含所有已上传的 parts（未在 list 中的 parts 视为丢弃）
3. 按客户端 list 顺序 `kubo::cat::stream` 各 part-CID 拼成连续流 → `kubo::add::stream` 整体 add → root_cid
4. `kubo::pin::add(root_cid)`（recursive pin）
   - 若 pin 失败：返回 500，parts 仍保留 pin 可重试 Complete
5. 删除 DB 中该 upload 的**所有** parts 的 direct pin（`kubo::pin::rm(part_cid)`，含 list 内和 list 外）——list 内 parts 被 root_cid 的 recursive pin 覆盖；list 外 parts 直接释放（内容丢弃）
6. DB `objects` 插入最终记录：
   - `cid=root_cid, size=客户端 list 中各 part 明文 size 之和, multipart=true, etag=root_cid`
   - 若 `encryption_mode='sse_s3'`：`encrypted=true, key_wrap=multipart_uploads.key_wrap`
   - 若 `encryption_mode='sse_c'`：`encrypted=true, key_wrap=NULL`
   - 若 `encryption_mode='none'`：`encrypted=false, key_wrap=NULL`
7. 删除 `multipart_uploads` + `multipart_parts` 记录（ON DELETE CASCADE 自动清理 parts）
8. 返回 `{Bucket, Key, ETag: <root_cid>}`

**AbortMultipartUpload**：
1. 查 `multipart_parts WHERE upload_id=?`
2. 逐个 `kubo::pin::rm(part_cid)`
3. 删 DB 记录（multipart_uploads + multipart_parts，ON DELETE CASCADE 自动）

**ListParts**：
1. 查 `multipart_parts WHERE upload_id=? ORDER BY part_number`
2. 返回 XML（含 PartNumber, ETag=part-CID, Size, LastModified）

### 5.3 与加密的交互

- 加密在 part 级别应用：每个 part 独立加密（part body → crypto stream → kubo add）。part-CID 是密文 part-CID。
- Complete 时整体 add 的是密文流拼接，root_cid 是密文根 CID。
- GET 解密：网关拉取 root_cid 对应内容流（Kubo 自动按 UnixFS 结构 cat 各 part），对密文流逐 chunk 解密后输出明文流。
- **nonce 复用风险**：每个 part 的 chunk nonce 用 `(object_id || chunk_index_within_part)`，其中 `object_id` 来自 `multipart_uploads.object_id`（CreateMultipartUpload 时预生成），`chunk_index` 在每个 part 内从 0 开始。object_id 对整个 Multipart 上传唯一，无重用。

### 5.4 GetObject 多段文件

由于 Complete 时已用整体 add 生成单一 root_cid（标准 UnixFS 文件），GET 行为与普通单文件对象一致：

1. 查 `store::get_object` 获取 `(cid=root_cid, multipart=true, ...)`
2. `kubo::cat::stream(root_cid, bytes=from-to)` —— Kubo 自动解析 UnixFS 结构并按序 cat 各 block
3. 网关无需手动解析 links 或拼接 part 流

**Range 支持**：
- **明文多段**：直接对 root_cid 用 `bytes=from-to`，Kubo 原生支持 Range
- **加密多段**：完整拉取密文流 → 逐 chunk 解密 → Range 截取明文（同单文件加密 Range 策略）

### 5.5 加密 + Multipart + Range 叠加

三者同时存在的场景处理优先级：

1. **Multipart + 加密（无 Range）**：`kubo::cat::stream(root_cid)` 拉密文流 → 逐 chunk 解密 → 明文流输出。
2. **Multipart + 加密 + Range**：MVP 策略——完整拉取并解密为明文流后按 Range 截取。代价同单文件加密 Range（需缓冲）。后续 v0.9 优化为 chunk 级 Range。
3. **Multipart + 明文 + Range**：直接对 root_cid Range 请求，Kubo 原生支持。

---

## 6. 元数据 Schema

单一 migration，PG/SQLite 兼容（SQLite 3.8.0+ 支持 partial index）：

```sql
CREATE TABLE buckets (
  name TEXT PRIMARY KEY,
  created_at TIMESTAMP NOT NULL,
  owner TEXT NOT NULL
);

CREATE TABLE objects (
  id TEXT PRIMARY KEY,             -- UUID 字符串（nonce 派生用其 UTF-8 字节）
  bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
  key TEXT NOT NULL,
  cid TEXT NOT NULL,
  size BIGINT NOT NULL,            -- 明文大小（用户可见大小，加密对象的密文开销不计）
  content_type TEXT,
  etag TEXT NOT NULL,              -- = cid 字段值
  metadata TEXT,                   -- JSON: x-amz-meta-* 自定义元数据
  encrypted BOOLEAN NOT NULL DEFAULT FALSE,
  key_wrap TEXT,                   -- SSE-S3 包封的 OK；SSE-C 时 NULL；明文时 NULL
  multipart BOOLEAN NOT NULL DEFAULT FALSE,
  is_latest BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP NOT NULL,
  UNIQUE (bucket, key, id)
);

-- 点查 + 范围扫描共用（is_latest 过滤避免扫描历史版本）
CREATE UNIQUE INDEX idx_objects_latest ON objects (bucket, key) WHERE is_latest = TRUE;

CREATE TABLE multipart_uploads (
  upload_id TEXT PRIMARY KEY,
  object_id TEXT NOT NULL,         -- 加密 nonce 派生用，CreateMultipartUpload 时预生成
  bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
  key TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  encryption_mode TEXT NOT NULL,   -- 'none' | 'sse_s3' | 'sse_c'
  key_wrap TEXT,                   -- SSE-S3 时存包封 OK
  content_type TEXT,
  metadata TEXT                    -- JSON
);

CREATE TABLE multipart_parts (
  upload_id TEXT NOT NULL REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE,
  part_number INT NOT NULL,        -- 1-10000
  cid TEXT NOT NULL,
  size BIGINT NOT NULL,            -- 明文 size（加密时为明文 part size）
  etag TEXT NOT NULL,              -- = cid 字段值（part-CID）
  uploaded_at TIMESTAMP NOT NULL,
  PRIMARY KEY (upload_id, part_number)
);

CREATE INDEX idx_multipart_parts_upload ON multipart_parts (upload_id, part_number);
```

---

## 7. 错误处理

统一错误类型 `AppError`，通过 `From` 转换 Kubo/sea-orm/IO 错误，最终映射到 `S3Error`：

| 错误场景 | S3Error 映射 | HTTP |
|---|---|---|
| Bucket 不存在 | `NoSuchBucket` | 404 |
| HeadBucket 不存在 | `NoSuchBucket` | 404 |
| Key 不存在 | `NoSuchKey` | 404 |
| Bucket 已存在 | `BucketAlreadyOwnedByYou` | 409 |
| DeleteBucket 时桶非空 | `BucketNotEmpty` | 409 |
| PutObject 时 Kubo /add 失败 | `InternalError` | 500 |
| GetObject 时 Kubo /cat 失败 | `InternalError` | 500 |
| sea-orm DB 错误 | `InternalError` | 500 |
| SigV4 校验失败 | s3s 自动 → `SignatureDoesNotMatch` | 403 |
| Range 越界 | `InvalidRange` | 416 |
| 元数据存在但 CID 丢失（GC 误删） | `InternalError` | 500 |
| Multipart upload_id 不存在 | `NoSuchUpload` | 404 |
| Part number 越界 | `InvalidPart` | 400 |
| Complete 时 parts 列表不完整 | `InvalidPart` / `InvalidPartOrder` | 400 |
| Complete 时客户端 ETag 与 DB 不匹配 | `InvalidPart` | 400 |
| UploadPart 非最后一片 size < 5MB | `EntityTooSmall` | 400 |
| SSE-C 密钥校验失败 | `AccessDenied` | 403 |

**原则**：对客户端永远返回标准 S3 错误码；内部错误日志带请求 ID + CID 便于追踪。PutObject 部分失败（/add 成功但 pin 或 DB 写入失败）的清理策略：尽力 `pin::rm` 已 add 的 CID，返回 500。

---

## 8. 测试策略

### 8.1 单元测试（`#[cfg(test)] mod tests`）

- `kubo/`：用 `wiremock` mock Kubo HTTP，验证请求 URL/参数/body 流
- `store/`：用 sea-orm in-memory SQLite，验证 CRUD + 并发 upsert
- `crypto/`：验证加解密往返、nonce 不重用、chunk 边界、跨 chunk 解密
- `s3/ops/`：mock Kubo + mock Store，验证业务逻辑（Range 解析、Copy 引用同 CID、Delete 清理顺序、Multipart 组装）

### 8.2 集成测试（`tests/` 目录）

启动真实 axum + s3s + in-memory SQLite + mock Kubo，用 `aws-sdk-s3` client 发真实 S3 请求，覆盖 P0 操作的 happy path + 错误路径。

### 8.3 docker compose 端到端（MVP 验收）

docker compose up → 等健康检查 → aws cli put/get/head/list/delete/copy/multipart/encryption。

---

## 9. docker compose 拓扑（dev）

### 9.1 MVP 拓扑（2 容器）

```yaml
services:
  kubo:
    image: ipfs/kubo:latest
    ports:
      - "5001:5001"          # RPC，仅 gateway 访问（dev 暴露便于调试）
    volumes:
      - ipfs_data:/data/ipfs
    environment:
      # dev 环境禁用 GC 避免误删
      - IPFS_GC_INTERVAL=never
    healthcheck:
      test: ["CMD", "ipfs", "id"]
      interval: 5s
      timeout: 3s
      retries: 10

  gateway:
    build: .
    ports:
      - "9000:9000"
    environment:
      KUBO_RPC_URL: http://kubo:5001
      DATABASE_URL: sqlite:///data/ipfs-s3.db
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
      MASTER_KEY: "must-replace-with-32-byte-hex-string"
    volumes:
      - gateway_data:/data
    depends_on:
      kubo:
        condition: service_healthy

volumes:
  ipfs_data:
  gateway_data:
```

MVP 用 SQLite（`sqlite:///data/ipfs-s3.db`），零外部容器。后续切换 PG 只改 `DATABASE_URL`，sea-orm 透明处理。

### 9.2 PostgreSQL profile（后续）

通过 `docker-compose.pg.yml` override 叠加 PG 容器，`DATABASE_URL` 改为 `postgres://...`，同一 migration 自动适配。

---

## 10. 配置

环境变量优先（docker 友好），可选 config 文件覆盖（`~/.ipfs-s3/config.toml`）。关键字段：

```toml
[server]
listen = "0.0.0.0:9000"

[kubo]
rpc_url = "http://127.0.0.1:5001"
timeout_secs = 300

[storage]
database_url = "sqlite:///data/ipfs-s3.db"   # sea-orm URL

[auth]
# 启动时从 [[auth.credentials]] 加载到 HashMap
[[auth.credentials]]
access_key = "minioadmin"
secret_key = "minioadmin"

[crypto]
master_key = "must-replace-with-32-byte-hex-string"    # dev: 环境变量；prod: KMS/Vault（预留）

[pinning]
provider = "noop"               # "noop" | "pinata" | "filebase"（后续）
```

凭证：MVP 用静态配置文件/环境变量加载多个 access_key/secret_key 对。每个请求通过 `S3Auth` trait 查 HashMap。不实现 IAM、STS、临时凭证。

---

## 11. 后续迭代路径（不在 MVP 范围）

| 迭代 | 内容 | 依赖 |
|---|---|---|
| v0.4 Pinning Service | 实现 PinningService trait，接入 Pinata/Filebase | API token 配置 |
| v0.5 多节点 | PG + 多 gateway 实例 + IPFS Cluster | docker compose profile |
| v0.6 Versioning / Lifecycle / Presigned URL | S3 高级特性 | — |
| v0.7 DeleteObjects 批量删除 / Object Tagging | 批量操作与标签 | — |
| v0.8 IAM / STS / 临时凭证 | 身份与访问管理 | — |
| v0.9 加密 Range 优化 | chunk 级 Range，避免完整解密 | 性能调优 |

---

## 12. NAT 部署说明

### 12.1 S3 网关通过 Cloudflare Tunnel

S3 是标准 HTTP，Cloudflare Tunnel 反向代理到本地 `:9000`，aws cli 配置 `endpoint_url` 即可。完全可行。

### 12.2 IPFS 内容公网访问

**约束**：Cloudflare Tunnel 是 HTTP 反向代理，不代理 libp2p swarm 流量。IPFS 节点发现走 libp2p（TCP/QUIC :4001），无法通过 Tunnel 暴露。

**解决路径**：通过 Pinning Service（Pinata/Filebase 等）将本地内容 pin 到公共 IPFS 网络，此后任何 IPFS Gateway（含 Cloudflare IPFS Gateway）可访问。

**MVP 策略**：预留 `PinningService` trait + Noop 实现，后续迭代接入真实服务。

---

## 13. 风险与缓解

| 风险 | 缓解 |
|---|---|
| s3s axum 集成可能遇坑 | 项目初期先跑通 s3s 官方 axum example，验证后再全面实现 |
| Kubo chunked 大文件上传稳定性（历史 issue #3332） | 流式 /add 测试覆盖大文件；备选方案：临时文件拼接 |
| AES-GCM chunk 边界与 ETag 计算 | ETag = 对象 CID 的字符串形式（如 `bafy...`），不含分隔符；加密对象的 ETag 是密文根 CID。这偏离 S3 标准（标准 ETag 是 MD5 或分片 ETag 组合 MD5），但满足 aws cli 基本功能 |
| aws cli / SDK 默认校验 ETag 是否为 MD5，CID 不匹配可能导致部分版本重试或失败 | 文档声明 ETag 非 MD5；建议客户端传输时加 `--no-progress` 或在 SDK 关闭内容校验（如 aws-sdk-s3 的 `disable_content_md5_validation`）；后续迭代可额外存 MD5 到 metadata 以兼容严格客户端 |
| 加密对象 Range 需完整解密（性能） | MVP 接受；v0.9 优化为 chunk 级 Range |
| Kubo GC 误删未 pin 内容 | dev 禁用 GC；所有 add 后立即 pin |
| sea-orm PG/SQLite 双后端 SQL 差异 | 用 sea-orm query builder 而非裸 SQL；migration 用 sea-orm-migration 跨后端宏 |
| Multipart Complete 时整体 add 失败 | 回滚：保留 parts pin，返回 500，客户端可重试 Complete |
| UploadPart DB insert 失败导致孤儿 part-CID | 立即 `kubo::pin::rm(part_cid)` 回滚；后台 GC 任务定期清理（后续迭代） |

---

## 14. 参考调研

- `docs/kubo-s3-feasibility-report.md` — Kubo 能力边界
- `docs/research/s3-ipfs-gateway-feasibility.md` — S3 API 面 + Rust 生态
- `docs/ipfs-s3-feasibility-research.md` — 分布式架构 + 既有方案
