# Client Compatibility v0.2 设计规格

- 日期：2026-07-19
- 状态：Ready for implementation
- 范围：`ROADMAP.md` 的 v0.2 全部 10 项
- 基线：`5e854e4`
- 关联：`docs/superpowers/plans/2026-07-07-rclone-compatibility-fixes.md`、`docs/superpowers/specs/2026-07-07-decompress-zip-upload-design.md`

## 1. 目标、范围和非目标

### 1.1 目标

本版本让网关能被常见 S3 客户端直接使用，而不只覆盖 AWS CLI 的基础上传路径。完成后，Docker Compose 可用持久化 SQLite 启动；`rclone`、MinIO `mc` 和 AWS CLI 的规定 smoke 流程有可重复的执行脚本和结果记录；标准 S3 SDK 的区域预检、v1 列表和批量删除得到正确响应。

v0.2 的完成条件是以下 10 项全部落地并在 `ROADMAP.md` 中从 `[ ]` 改为 `[x]`：

1. 修复 Docker Compose SQLite 文件数据库启动。
2. 实现 `GetBucketLocation`，固定服务区域为 `us-east-1`。
3. 实现 `ListObjects` v1，复用 v2 的列表、分页和 delimiter folding 语义。
4. 实现 `DeleteObjects` 批量删除。
5. 增加 rclone Docker smoke：`mkdir`、`copy`、`ls`、`cat`、`deletefile`、`rmdir`。
6. 增加 MinIO `mc` Docker smoke：`alias`、`mb`、`cp`、`ls`、`cat`、`stat`、`rm`、`rb`。
7. 保留 AWS CLI Docker smoke 作为基线回归。
8. 文档化 rclone 推荐选项：`list_version = 2`、`use_server_modtime = true`。
9. 通过 localhost 和 Compose 服务网络两条路径验证嵌套 key 的签名 `HeadObject`。
10. 在文档中维护客户端兼容矩阵。

### 1.2 非目标

- 不实现 v0.3 或后续功能，包括 presigned URL、bucket 名校验、HeadObject Range、SSE-C multipart 一致性校验、版本控制、对象 tagging、rclone backend plugin 或 pin 引用计数。
- 不改变 ETag 等于 IPFS CID、明文对象可经 Kubo `cat` 读取、加密对象经 S3 解密读取的既有约定。
- 不新增 `pin_rm`。单对象删除和批量删除只删除数据库中的 latest object 记录，不解除 Kubo pin。
- 不把客户端二进制、Docker 镜像或云端依赖纳入仓库，也不要求本机安装 AWS CLI 或 `mc`。
- 不把 Docker smoke 的成功伪装为 Rust 集成测试已经运行。缺少前置程序或镜像时，应准确记录未运行状态。

## 2. 方案比较与推荐

### 方案 A：在每个 S3 operation 内分别实现

`ListObjects` v1 和 v2 各自查询数据库、各自处理 delimiter、分页和截断。`DeleteObjects` 循环调用现有 `delete_object`。

优点是改动位置少，接口直观。缺点是 v1/v2 的 marker、token 和 `CommonPrefixes` 很容易出现分叉，批量删除也会把 v1 的“缺失 key 成功”语义错误地继承为 `NoSuchKey`。不采用。

### 方案 B：共享领域 helper，DTO 适配留在 operation 边界

抽取按 key 排序的分页和 folding helper，接受统一的排他 cursor、prefix、delimiter、max-keys。v1/v2 operation 只负责把各自输入映射成共享请求，并生成各自 DTO。批量删除通过 store 的幂等“标记非 latest”函数执行，不调用单对象 HTTP operation。

优点是唯一的分页语义来源，边界可单测，保留 `s3s` trait 的标准 DTO 路由。代价是需要为 store 新增一个不会因缺失 key 报错的 helper。**采用本方案。**

### 方案 C：以自定义 raw route 解析 XML 和 query

像 decompress-zip Complete route 一样，在 `S3Route` 中自行解析 List v1 与 DeleteObjects XML。

这能精细控制线协议，但标准 `s3s 0.14` DTO 已覆盖所需 operation，重复解析 XML 和 SigV4 边界没有收益，还会增加认证或序列化偏差风险。不采用。

## 3. 架构和模块边界

```text
S3 client
  │ SigV4
  ▼
axum → s3s service → S3Impl
                     ├─ get_bucket_location → ops/bucket.rs
                     ├─ list_objects v1    → ops/object.rs ─┐
                     ├─ list_objects v2    → ops/object.rs  ├─ shared listing page/folding
                     └─ delete_objects     → ops/object.rs ─┘
                                              │
                                              ▼
                                      store/object.rs (SeaORM)

Docker smoke scripts → docker compose stack → gateway + Kubo
compatibility matrix ← script output and test evidence
```

### 3.1 组件职责

| 组件 | 责任 | 不负责 |
|---|---|---|
| `src/s3/handler.rs` | 为 `S3Impl` 添加三个 `s3s::S3` trait method，并委派给 operation | 业务分页、XML 手写解析、pin 管理 |
| `src/s3/ops/bucket.rs` | 固定区域的 `GetBucketLocation` | 从 bucket metadata 推断区域，本版本没有 region metadata |
| `src/s3/ops/object.rs` | v1/v2 DTO 映射、共享 page builder、批量删除 response | 直接拼接 HTTP XML、Kubo unpin |
| `src/store/object.rs` | 按 prefix 和严格排他 key cursor 查询；单 key 幂等删除 | S3 DTO、delimiter folding |
| `src/config.rs` | 仅在非空环境变量存在时覆盖文件配置的 master key | 生成或持久化密钥 |
| `docker-compose.yml`、`config.docker.toml` 与 `config.example.toml` | 可启动的 SQLite file URL、Compose 网络 Kubo 地址与空 master-key 传递语义 | 自动下载 client image |
| `scripts/` 或 `tests/` 中的 smoke 工件 | 可执行的 Docker client smoke、前置检查和结果记录 | 在工具缺失时声称通过 |
| `docs/client-compatibility.md` | rclone 配置、兼容矩阵、每次验证证据 | 替代自动化测试 |

实现前应先确认 `s3s 0.14` 已验证的 DTO trait 签名，再使用下列边界：`GetBucketLocationInput/Output`、`ListObjectsInput/Output`、`DeleteObjectsInput/Output`。不得通过自定义 raw route 规避这些标准 operation。

### 3.2 共享列表页模型

新增非公开 `ListingRequest` 和 `ListingPage`，或等价的小型内部类型：

- 输入：`bucket`、可选 `prefix`、可选非空 `delimiter`、可选排他 `cursor`、`max_keys`。
- 输出：按字典序排列的 `entries`，每项是 concrete object 或 common prefix，`is_truncated`，以及最后实际消费的对象 key。
- `max_keys` 限制 `Contents + CommonPrefixes` 的合计数量，范围保持 1 到 1000，默认 1000。
- store 查询保持 `key > cursor`，所以 marker、continuation token 和 `start_after` 都是严格排他的，不重放 cursor 所指 key。
- 无 delimiter 时，所有 prefix 范围内的对象均为 `Contents`。有 delimiter 时，prefix 后第一个 delimiter 之前的直接子对象进入 `Contents`，其余对象合并为一次 `CommonPrefix`。
- 同一 common prefix 的连续对象只消耗一个可见名额，但必须继续扫描该 prefix 的后续行，直到找到新的可见条目或数据耗尽。否则无法正确判断 `IsTruncated`。

当前 `ListObjectsV2PageBuilder` 是实现起点。实现必须将它改名或泛化为 v1/v2 共用的 helper，而不是复制代码。

## 4. 三个 s3s operation 的精确语义

### 4.1 GetBucketLocation

`S3Impl::get_bucket_location` 委派给 `ops::bucket::get_bucket_location`。

1. 先用现有 `store::bucket::exists` 校验 bucket。
2. bucket 不存在时返回标准 `NoSuchBucket`，消息沿用 `bucket not found: {bucket}`。
3. bucket 存在时返回 HTTP `200 OK` 的标准 `GetBucketLocationOutput`。
4. 服务的唯一区域是 `us-east-1`。按 AWS S3 线协议，响应 XML 的 `<LocationConstraint>` 必须为空元素或空文本，不能输出字面字符串 `us-east-1`。这等价于 S3 对 `us-east-1` 的标准 LocationConstraint 表达。
5. 不增加 bucket region 列，不依据请求签名 region 变化，也不重定向到 region endpoint。

需要集成测试真实 SigV4 `GET /{bucket}?location`，断言状态 200、响应可被标准 S3 XML client 解码为 `None` 或空 LocationConstraint，并断言不存在 bucket 仍为 `NoSuchBucket`。

### 4.2 ListObjects v1

`S3Impl::list_objects` 委派给 `ops::object::list_objects`，并复用第 3.2 节的 shared listing page。

1. 先验证 bucket 存在，否则 `NoSuchBucket`。
2. `prefix` 为空或缺失时表示无前缀限制。`delimiter` 缺失或为空字符串时不 folding。
3. `marker` 缺失或为空时没有 cursor；非空 marker 是排他 cursor。数据库只返回 `key > marker` 的 latest rows。
4. `max_keys` 默认 1000，钳制为 1 到 1000。其上限同时计入 object 和 `CommonPrefixes`。
5. 响应回显请求的 `Name`、`Prefix`、`Delimiter`、`Marker`、`MaxKeys`、`EncodingType`。`Contents` 和 `CommonPrefixes` 从 shared page 映射，ETag 继续以 CID 返回。
6. `IsTruncated=false` 时 `NextMarker` 缺失。
7. `IsTruncated=true` 且没有 delimiter 时，`NextMarker` 是最后返回 object 的 key。
8. `IsTruncated=true` 且存在 delimiter 时，`NextMarker` 必须是最后消费的底层 object key，而不是显示的 common prefix。它可能位于一个已返回 common prefix 内部。下一页以严格 `key > NextMarker` 继续，既不重复也不跳过同一目录内尚未消费的对象。
9. v1 不接受或生成 v2 `ContinuationToken` / `NextContinuationToken`，v2 也不得被 v1 marker 语义改坏。

特别覆盖：对象 `a`, `photos/1`, `photos/2`, `videos/1`，`delimiter=/`、`max-keys=2` 的第一页返回 `a` 与 `photos/`、`IsTruncated=true`，NextMarker 是最后已消费的 `photos/*` 底层 key。用 NextMarker 请求第二页必须最终返回 `videos/` 且无重复或遗漏。

### 4.2.1 `encoding-type=url` response projection

列表页、数据库 cursor、v1 marker 查找和 v2 continuation-token/start-after 查找始终使用原始 key。仅在请求 `encoding-type=url` 时，于 DTO 映射边界按 RFC 3986 投影 response 字段：UTF-8 bytes 中只保留 `A-Z`、`a-z`、`0-9`、`-`、`.`、`_`、`~`，其余每个 byte 以大写十六进制 `%HH` 编码。因此 `/`、`%`、`(`、`)` 和 Unicode 均会被编码。

- v1：编码 `Contents.Key`、`CommonPrefixes.Prefix`、`Prefix`、`Delimiter`、`Marker`、`NextMarker`；`Name` 不编码。
- v2：编码 `Contents.Key`、`CommonPrefixes.Prefix`、`Prefix`、`Delimiter`、`StartAfter`；`Name` 不编码。
- v2 的 `ContinuationToken` / `NextContinuationToken` 保持既有 opaque/raw 行为，绝不经过 response projection。

返回编码后的 `NextMarker` 不改变底层分页身份：客户端必须以原始 marker 继续查询，store 仍严格执行 `key > cursor`。

### 4.3 DeleteObjects

`S3Impl::delete_objects` 委派给 `ops::object::delete_objects`。

1. 先验证 bucket 存在。不存在时整个请求返回 `NoSuchBucket`，不产生 per-key `Errors`。
2. 对输入 `Delete.Objects` 保持请求顺序逐项处理。每一项以 key 为身份；版本 ID 在未实现 versioning 的 v0.2 中不参与存储选择。
3. 删除要调用新的 store 级幂等 helper：将匹配的 latest row 设为非 latest。没有匹配 latest row 时返回“未删除任何 row”，但不是错误。
4. 每个对象，无论此前存在、已删除或输入中重复，均视为成功并加入 `Deleted`。这使请求可安全重试。
5. `quiet` 为 false 或缺失时，响应中的 `Deleted` 按输入顺序列出所有成功对象。`quiet=true` 时仍执行完全相同的删除，但省略 `Deleted` 条目。
6. 若单项发生可恢复的数据库错误，将该项写入 `Errors`，继续处理后续项。无法开始请求、bucket 检查失败等请求级故障仍返回标准 S3 error。
7. response 中不因缺失 key 产生 `NoSuchKey`。未实现 versioning，不产生 delete marker。
8. 绝不调用 `kubo::pin::pin_rm`。CID 可被 CopyObject、并发写入或其他 key 共享；删除只移除数据库可见性，保留 pin 是既有数据安全策略。

测试需覆盖：普通两个 key 删除、含缺失 key 的重试、重复 key、quiet response、一个注入的 store error 后其余 key 继续处理、零次 `/api/v0/pin/rm`。

## 5. Docker、配置和启动语义

### 5.1 SQLite 文件 URL

Compose gateway 使用 `/data` volume。文件数据库必须使用 SeaORM/SQLite 可创建的 URL：

```toml
[storage]
database_url = "sqlite:///data/ipfs-s3.db?mode=rwc"
```

新增受版本控制的 `config.docker.toml`，包含 Compose 专用的 `http://kubo:5001`、上述 SQLite URL以及开发测试凭证；Compose 将它只读挂载为 `/data/config.toml`。`config.example.toml` 保持通用示例用途并同步 SQLite `?mode=rwc` 要求。ignored `config.toml` 是操作者的本地配置，实施和提交都不得读取其凭证、修改它或依赖它。`AppState::new` 的连接和 migration 在 gateway 对外监听前完成，连接或 migration 失败时进程失败，不启动半可用服务。

### 5.2 master key 覆盖

配置加载的覆盖优先级保持 defaults、文件、环境变量。只有 `IPFS_S3_MASTER_KEY` 存在且值非空时，才覆盖 `[crypto].master_key`。

因此 Compose 的 `IPFS_S3_MASTER_KEY=${IPFS_S3_MASTER_KEY}` 在宿主变量未设置时传入空字符串，也**不得**覆盖 `/data/config.toml` 内的 master key。非空环境变量继续是有意的运行时覆盖。测试应分别验证：未设置、空字符串、非空有效 hex、非空无效值。前两者保留文件值，第三者替换文件值，第四者在状态初始化时失败。

### 5.3 启动与清理边界

- 可执行脚本先检查 Docker daemon 可访问、Compose 文件存在、预期 client image 已在本地。
- 仅在前置条件满足时运行 `docker compose up -d --build` 和 smoke 容器。`--build` 是明确构建本仓库 Dockerfile 的动作，不得在本任务中实际执行。
- 若 Kubo、gateway、AWS CLI 或 `mc` Docker image 不在本地，脚本输出所需 `docker pull` 或 Compose 命令并退出为 `SKIPPED`。没有明确的软件或镜像安装批准时，不拉取、不构建、不把该 smoke 写为已通过。
- 运行过的 smoke 使用独立、带时间戳的 bucket 名，结束时删除对象和 bucket。只有在操作者明确选择清理时执行 `docker compose down -v`，不得自动删除开发者 volume。

## 6. 客户端 smoke 和兼容矩阵

### 6.1 共通环境

脚本统一使用 path-style endpoint、`us-east-1` 和测试凭证 `test`/`test`。`mc`（以及 AWS CLI 在其 image 可用时）以两条地址验证：

| 路径 | endpoint | 目的 |
|---|---|---|
| 宿主 localhost | `http://127.0.0.1:9000` | 验证端口发布、localhost SigV4 canonical host 和嵌套 key HeadObject |
| Compose 网络 | `http://gateway:9000` | 验证容器内 DNS、服务名 host 和嵌套 key HeadObject |

`mc` 的两条路径都对同一 `nested/path/file.txt` 执行自身签名 `stat`，断言 CID ETag 与 Content-Length 相同。Rclone 只从 Compose 网络 `http://gateway:9000` 运行自身 S3 操作；它不运行 localhost probe、签名 nested HEAD，且不会调用 `mc` 代验。

### 6.2 rclone

推荐 remote 配置：

```ini
[ipfs-s3]
type = s3
provider = Other
endpoint = http://gateway:9000
access_key_id = test
secret_access_key = test
region = us-east-1
force_path_style = true
list_version = 2
use_server_modtime = true
```

`list_version = 2` 是推荐配置，不是回避 v1 实现的理由。v1 仍是 SDK 和旧客户端的标准兼容 surface。`use_server_modtime = true` 告诉 rclone 使用 S3 返回的 LastModified，而非把 ETag 当作 MD5 或尝试额外 metadata 行为。文档还必须说明 ETag 是 CID，不是 MD5；启用需要 MD5 ETag 校验的 rclone 选项不受支持。

rclone smoke 仅依次运行 `mkdir`、`copy`、`ls`、`cat`、`deletefile`、`rmdir`。`ls` 验证嵌套对象可见，`cat` 逐字节匹配上传 fixture。Rclone 成功时精确输出 `RESULT client=Rclone status=PASSED dual_head=NOT_RUN`，不输出 `EVIDENCE`；缺少 mc image 不得使 Rclone 变为 `SKIPPED`。默认配置可完成的步骤必须使用默认值；若具体版本需要上述两个推荐项，脚本在结果中打印实际生效配置。

### 6.3 MinIO mc 与 AWS CLI

`mc` smoke：创建 alias，`mb`，`cp`，`ls`，`cat`，`stat`，`rm`，`rb`。`stat` 覆盖 HeadObject，创建 alias 或 bucket 操作触发的区域预检覆盖 GetBucketLocation。

AWS CLI smoke 是基线回归：`s3 mb`、`s3 cp`、`s3 ls`、`s3api head-object`、`s3 rm`、`s3 rb`，并用 `s3api list-objects` 和 `s3api delete-objects` 覆盖 v1 和批量删除。AWS CLI 的 `s3api get-bucket-location` 必须验证空 LocationConstraint 的 `us-east-1` 语义。

本机已知 rclone 为 1.74.4，Docker 可用，AWS CLI 与 `mc` 缺失。文档和 CI 记录必须区分以下状态：

- `PASSED`：命令实际执行且所有断言成功。
- `FAILED`：前置条件满足、命令执行，但断言或退出码失败。
- `SKIPPED`：本机命令或本地 Docker image 缺失，且未获安装或拉取批准。记录准确缺失项和可直接执行的命令。

不得把 `SKIPPED` 表述为 `PASSED`，也不得因本机 `aws` 或 `mc` 缺失而伪报已运行。
任何实际执行得到的 `FAILED` 都阻止对应客户端 ROADMAP 项勾选和最终提交，直到缺陷修复并重跑为 `PASSED`。仅因未获执行授权或本地 image 缺失而得到的准确 `SKIPPED`，可在 smoke 工件、命令和结果跟踪均已交付时把对应客户端 smoke 项视为“测试能力已完成但本机未执行”；矩阵必须继续显示 `SKIPPED`，不能写成客户端已兼容。第 9 项“双路径 HeadObject”不是 artifact-only：Task 6 提交前必须实际运行 MinIO `mc` 的 real-stack smoke。AWS CLI 可因本地 image 缺失而 `SKIPPED`，而 Rclone 的 `PASSED dual_head=NOT_RUN` 合法但不能单独满足 item 9。只有实际 `mc` 对同一对象在 localhost 和 Compose 网络端点都完成自己的签名 `stat`，并在受版本控制日志中留下 `client=Mc verifier=Mc` 的 `RESULT status=PASSED dual_head=PASSED` 与匹配 `EVIDENCE dual_head=PASSED` 时，item 9 才完成。

### 6.4 兼容矩阵

新建或更新的 `docs/client-compatibility.md` 至少包含以下字段：

| Client | Version | Transport | Endpoint path | Auth/region | Operations | Recommended options | Result | Evidence date | Evidence command or log | Known limitation |
|---|---|---|---|---|---|---|---|---|---|---|

`Result` 只允许 `PASSED`、`FAILED`、`SKIPPED`。每行都要写操作集合和证据命令或日志位置，不能只写“compatible”。初始记录需明确 rclone 1.74.4、Docker 可用、AWS CLI 与 mc 缺失的事实，不杜撰测试结果。

### 6.5 受跟踪的真实 dual-head 证据

唯一批准的真实 smoke 收据是 `docs/client-smoke-evidence-2026-07-19.log`。它只能由 Task 6 的 mandatory `-Run` real-stack smoke 从捕获的 stdout/stderr 以 UTF-8 写入；预检、Rust 测试、静态 writer 数量和临时 transcript 都不能创建或替代它。执行只使用 `test`/`test` 测试场景，且日志不得包含 ignored `config.toml`、本地凭证、master key 或其他本地敏感配置。

写入后，Task 6 必须直接读取此日志，精确解析三个 `RESULT` 行：Rclone `PASSED/NOT_RUN`、Mc `PASSED/PASSED`、Aws `SKIPPED/NOT_RUN`。runtime parser 必须捕获 `verifier` 并强制 `verifier == client`，而非捕获后丢弃；本次日志只允许 `client=Mc verifier=Mc` 的 runtime `EVIDENCE`。Rclone matrix 行只能引用其 `RESULT dual_head=NOT_RUN`，而 item 9 只由 Mc same-client evidence 支撑。对应 Mc 矩阵行必须为 `PASSED`，其证据列引用该日志和 `dual_head=PASSED`；ROADMAP item 9 必须是唯一的 `[x]` 项并含 `Mc same-client dual endpoint smoke PASSED`。任何解析、交叉匹配、矩阵或 item 9 检查失败都阻止 staging 和提交。

## 7. 错误、安全和数据一致性

### 7.1 错误映射

| 情况 | HTTP/S3 结果 | 副作用 |
|---|---|---|
| GetBucketLocation 的 bucket 不存在 | `NoSuchBucket` | 无 |
| List v1/v2 的 bucket 不存在 | `NoSuchBucket` | 无 |
| 无效的 DTO/query/XML | 由 `s3s` 返回标准 `InvalidRequest` 或 `MalformedXML` | 无 |
| DeleteObjects 的 bucket 不存在 | 请求级 `NoSuchBucket` | 不处理子项 |
| DeleteObjects 中单 key store error | response `Errors` 中对应条目 | 继续后续 key |
| DeleteObjects 缺失 key | `Deleted`，不是 error | 无 DB row 变化 |
| SQLite 文件不能创建或 migration 失败 | gateway 进程启动失败 | 不监听 S3 端口 |
| 非空无效 master key | gateway 进程启动失败 | 不监听 S3 端口 |

错误信息不得输出凭证、master key、签名内容或数据库连接中的敏感信息。

### 7.2 安全

- 所有新增 operation 继续走 `s3s` 的 SigV4 认证和授权路径。smoke 不可用未签名请求替代。
- master key 不写入 smoke 输出、矩阵、脚本 echo 或测试 snapshot。
- 客户端配置示例只用 `test`/`test`。生产凭证通过外部秘密管理提供，不写进文档。
- 维持 path-style S3 endpoint，避免客户端将 bucket 拼入未配置的 DNS host。

### 7.3 数据一致性

列表只读取 `is_latest=true` 行，按照 key 字典序排序。使用严格排他 raw cursor 保证跨页不会重复 cursor key。delimiter folding 不改变数据库，只改变 S3 response 的投影。`encoding-type=url` 只在 DTO 映射时编码字段，绝不编码 `ListingPage`、DB cursor、marker/token 查询身份或 v2 continuation tokens。

单对象删除现有的“若 key 缺失则错误”语义可保留给 `DeleteObject`。`DeleteObjects` 必须使用独立的幂等 store helper，不能通过捕获 `NoSuchKey` 的方式模拟，避免未来 store 错误被误吞。每个成功批量项只把 latest row 标为非 latest，保留历史 row 与 CID pin。

批量请求不是跨所有 key 的全局事务。已经成功的项在后续项失败时保持成功，response 同时包含 `Deleted` 和 `Errors`，符合 S3 batch delete 的部分成功模型。

## 8. 测试策略和验收

### 8.1 单元与 operation 测试

- `get_bucket_location`：存在 bucket 返回空 LocationConstraint，不存在 bucket 返回 `NoSuchBucket`。
- shared listing helper：无 delimiter、prefix + delimiter、common prefix 去重、object 与 prefix 合并计数、cursor 排他、页边界、重复 prefix 扫描和 `IsTruncated`。
- List v1：marker 排他，v1 response field 回显，truncated 无 delimiter 的 NextMarker，truncated 有 delimiter 的 NextMarker，以及两页合并结果无重复无遗漏。
- List v2 回归：continuation token 优先于 start-after，NextContinuationToken 保持正确，现有 CommonPrefixes 行为不回退。
- URL projection：RFC3986 encoder 对 `/`、`%`、括号、Unicode 与 UTF-8 uppercase `%HH`；v1/v2 要求的 DTO 字段编码而 `Name`/v2 tokens 保持 raw；encoded response 不影响以 raw marker/token 翻页。
- `DeleteObjects`：存在/缺失/重复 key 均成功，quiet 隐藏 `Deleted`，请求顺序，单项错误继续处理，bucket 不存在不处理项，零 `pin_rm` 调用。
- config：环境变量缺失与空字符串不覆盖文件 master key，非空覆盖，非空无效值在初始化失败。

### 8.2 真实 TCP integration

复用 `tests/support/decompress.rs::start_harness`，它会启动真实 `TcpListener` 上的 axum + `s3s` 服务，并提供可观察 Kubo 请求日志。复用 `tests/support/sigv4.rs::send_sigv4` 构造经过真实 canonical path/query/header 签名的请求。

新增覆盖必须至少包括：

1. GetBucketLocation 成功和不存在 bucket。
2. List v1 的 delimiter、marker exclusive、NextMarker 连续两页。
3. `encoding-type=url` 的 v1/v2 raw SigV4 query，断言 XML 编码 `%2F`、`%252F`、`%28`、`%29`、Unicode UTF-8 bytes、contents/common-prefix 和回显字段，并以 raw marker 请求下一页而不重放。
4. DeleteObjects 的 normal、quiet、missing key、partial failure，以及断言 Kubo log 中没有 `/api/v0/pin/rm`。
5. localhost endpoint 上嵌套 key `HEAD /bucket/nested/path/file.txt` 的 SigV4 成功。
6. 标准 v2 regression：ListObjectsV2 delimiter/pagination、普通 Put/Get/Head/Delete、CopyObject、SSE、multipart 和 decompress-zip 既有回归全部通过。
7. ZIP regression：合法解压、路径拒绝、archive-key collision、partial entry failure、bounded Complete XML、header 与 presigned signing 的已有覆盖均不变。

### 8.3 Docker smoke

Docker smoke 是真实 Kubo 和 gateway 的端到端验证，与 wiremock TCP integration 分开报告。脚本按 6.1 到 6.3 节运行，写出 stdout/stderr、client version、image digest 或 image ID、Compose service 状态和矩阵行。提交前的 Task 6 real-stack run 必须把捕获的 stdout/stderr 无论退出码为何都以 UTF-8 写入 `docs/client-smoke-evidence-2026-07-19.log`，然后从该文件而非脚本静态形状读取 dual-head 事实。

若需要的 Docker image 不存在于本机且没有拉取批准，测试工件必须输出可执行命令，例如：

```powershell
docker pull <required-client-image>
docker compose up -d --build
```

然后以 `SKIPPED` 结束。不得执行该 `docker pull`，不得启动 stack，也不得把脚本可执行误写成 smoke 已运行。

### 8.4 三个规定场景

| 场景 | 过程 | 验收 |
|---|---|---|
| Happy path | Compose 使用 file SQLite 启动。rclone 创建 bucket、上传嵌套对象、列出、读取、删除并删 bucket；mc 执行自己的双 endpoint stat。 | Rclone 为 `PASSED/NOT_RUN`，Mc 为 `PASSED/PASSED` 且有 `client=Mc verifier=Mc` evidence；AWS image 缺失时为 `SKIPPED/NOT_RUN`；对象内容匹配且矩阵有精确证据。 |
| 边界与部分失败 | List v1 以 `delimiter=/`、小 `max-keys` 跨页，marker exclusive。DeleteObjects 含存在、缺失、重复 key 和一个注入 store error。空 master-key 环境覆盖 file config。 | NextMarker 不重复不遗漏，缺失 key 成功，quiet 正确，部分失败仅出现在 `Errors`，无 pin_rm，空环境值未覆盖文件 master key。 |
| 标准 v2/ZIP 回归 | 跑完整 Rust lib 和 integration suites，包含 v2 folding、标准 multipart、decompress-zip 的 raw Complete 和 ZIP 路径安全。 | v2 和 ZIP 可观察行为与 v0.1/已定 ZIP spec 一致，无因新 shared helper 或 handler trait method 引入的回归。 |

### 8.5 最终命令和可接受结果

在代码实施后，依次执行：

```powershell
cargo fmt --check
cargo test --lib
cargo test --test integration
```

Docker 与客户端命令只在前置条件和授权满足时执行。`aws`、`mc` 在当前本机缺失，不能作为本次设计写入时已经运行的验证。rclone 1.74.4 的本机存在也不意味着 Docker stack 或 rclone smoke 已运行。若要最终提交，获授权后必须执行 Task 6 的 real-stack run；AWS image 仍缺失时可准确 `SKIPPED`，Rclone 可真实 `PASSED dual_head=NOT_RUN`，但只有 Mc 的 `PASSED dual_head=PASSED` same-client evidence 能打开 item 9。旧 `tests/e2e.rs` real-stack suite 仍是可选补充，不能代替此门禁。

## 9. 实施顺序

1. 为 `get_bucket_location`、`list_objects`、`delete_objects` 增加 handler trait 桩和失败测试，确认 `s3s 0.14` DTO 细节。
2. 抽取 shared listing page/folding helper，并先锁住 v2 全部现有语义。
3. 在 store 增加幂等 latest-row delete helper，再实现 List v1 和 DeleteObjects DTO 映射。
4. 增加 GetBucketLocation、v1、DeleteObjects 与 URL projection 的 TCP SigV4 integration 覆盖，以及 localhost nested HeadObject 回归。
5. 修正 SQLite URL 和非空 master-key 覆盖逻辑，添加 config/startup 测试。
6. 创建 Docker smoke 脚本、rclone 文档和兼容矩阵。脚本实现前置检查和 `PASSED`/`FAILED`/`SKIPPED` 报告。
7. 运行 Rust 验证；提交前在获得 image/安装授权且本地前置条件具备时强制运行 Docker smoke，将捕获 stdout/stderr 写入受跟踪日志，并按第 6.5 节解析真实证据。
8. 仅当以上 10 项均完成、证据已写入矩阵、没有实际 `FAILED`，且第 9 项已有来自 `docs/client-smoke-evidence-2026-07-19.log` 的真实 `Mc/Mc` 双端点 stat `PASSED` 时，更新 `ROADMAP.md` 的 v0.2 十个 checkbox。未授权或缺 image 的客户端 smoke `SKIPPED` 必须按第 6.3 节保留原样；Rclone `PASSED/NOT_RUN` 不会满足 item 9。任何 `FAILED`、缺失日志或缺失 Mc same-client 双端点证据都阻止对应 checkbox 和最终提交。不得提前勾选，不得改动 v0.3+ 项。

## 10. 实施文件和环境限制

### 10.1 File Structure

`docs/client-smoke-evidence-2026-07-19.log` 是唯一允许受版本控制的实际 smoke 日志。它不在 Task 5 预检、静态检查或临时脚本 transcript 阶段生成；只在 Task 6 mandatory real smoke 运行时，以捕获的 stdout/stderr UTF-8 生成。内容只允许 `test`/`test` 测试场景，绝不包含本地敏感配置。

预期变更文件：

```text
src/s3/handler.rs
src/s3/ops/bucket.rs
src/s3/ops/object.rs
src/store/object.rs
src/config.rs
docker-compose.yml
config.example.toml
config.docker.toml
tests/integration.rs
tests/support/decompress.rs              (仅在 harness 需要能力时)
scripts/<client-smoke>.ps1               (或项目现有脚本目录的等价位置)
docs/client-compatibility.md
docs/client-smoke-evidence-2026-07-19.log  (仅 Task 6 mandatory real smoke 的受跟踪 stdout/stderr)
ROADMAP.md                               (仅在全部 10 项完成后)
```

当前环境限制：Docker 可用，rclone 为 1.74.4；AWS CLI 与 `mc` 本机命令缺失；`rclone/rclone:1.74.4` 与 `minio/mc:latest` 镜像已在本地，AWS CLI 镜像缺失；Context7 quota 已耗尽；`s3s 0.14` DTO 已通过 docs.rs 核实。实施不得安装软件、拉取 image、修改未列文件或添加 v0.3 功能，除非用户另行授权。Docker image 是否本地存在必须在实际 smoke 前检查，不能根据 Docker 可用性推断。

## 11. 规格自审

- **占位符检查**：没有未决占位符或待定语义。缺失客户端或镜像的行为定义为明确的 `SKIPPED`，不是未说明的例外；全 `SKIPPED` 的 item 9 明确为 `[ ]`、19 项且不可提交。
- **一致性检查**：List v1/v2 均使用同一个 shared page；`encoding-type=url` 只投影 wire DTO，v1 marker 与 v2 token 仍是 raw 边界，且 v2 tokens 保持 opaque。批量删除的幂等性不改变单删除语义。Docker 空 master key 规则与 config precedence 一致。Task 6 的实际输出、受跟踪日志、矩阵和 ROADMAP item 9 使用 Mc same-client `client=Mc verifier=Mc`/`dual_head=PASSED` 事实，Rclone 则准确为 `PASSED/NOT_RUN`。
- **范围检查**：覆盖 ROADMAP v0.2 的 10 项，没有引入 v0.3 功能、pin 引用计数或客户端插件；新增日志仅为唯一受跟踪的真实 smoke 证据。
- **歧义检查**：明确了 `us-east-1` 的空 LocationConstraint、v1 marker exclusive、delimiter 情况的 NextMarker、`encoding-type=url` 的 DTO-only RFC3986 projection 与 raw cursor/token 身份、DeleteObjects quiet/missing key/no-pin-rm、ROADMAP 更新时机、未获安装授权时的 Docker smoke 行为，以及提交前仅 Mc/Mc 的真实 dual-head 日志门禁。
