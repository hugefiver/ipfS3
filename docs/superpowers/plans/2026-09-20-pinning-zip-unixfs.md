# Pinning、ZIP 产物与 UnixFS 目录根：统一实施计划

日期：2026-09-20。状态：计划已通过审查；Stage 1 实现及验收完成，后续阶段尚未实施。

本文件是本轮唯一权威设计/实施/进度计划；不另建 spec，不单独提交计划。需求来源为用户提供的 `ipfs3_review_report.md`（2026-09-17）及最新 R24 确认。本轮基线为 `master@d3bf5cf`；规划时已确认工作区 clean、与 origin/master 一致，本轮不执行 push。

## 1. 目标、权限与完成定义

把报告 R01–R23 的缺陷、功能及明确属于后续研究的边界，与 R24 放在同一条可逐阶段交付的路径上：

- 修复 Pinata 解析、分类、恢复、日志与停滞问题；保留真实副作用证据。
- 交付可选控制 warn/skipped、可解释配置/策略、稳定身份与安全运维。
- 三个 ZIP 入口统一产物/批次/幂等契约，增加默认开启且可由签名 tag 覆盖的 UnixFS directory root CID。
- 增加真正的 Filebase RPC/upload、独立 Kubo provider、Cluster Proxy + REST 副本验证；严格复制原 CID，不混淆副本域和成功证据。
- 通过明确版本清单补 pin，完成迁移、退役、所有权与 retain 占用闭环。
- R14 保持专项研究：本轮计划覆盖其研究交付物和实施 gate，不把研究写成生产 GC/全局限速已实现。

**权限：**当前 planner 只创建本文件，不编辑产品、测试或配置，不派生 agent，不执行 Git 写。后续执行者按用户已授权的流程逐阶段实施并在每阶段完成时做 **一个 semantic commit**（标题 + 简短正文，无 footer/trailer/AI 署名）；计划随第一阶段提交，不做独立 plan commit。没有 push、tag、release、rebase 授权。真实 provider 写测试可能公开数据或产生费用，须另行明确授权；不得安装工具、自动拉取镜像或操作生产数据来凑验收。

**完成定义：**阶段结果有可复核的行为、状态及资源证据，而非只有日志、HTTP 200 或命令退出码。mock、真实本地后端、真实账号、真实 SDK 四类证据分开记录；不可执行的环境检查标记未验证，不记 passed。允许将环境受限的代码提交明确标为“实现完成，外部验收待办”，不得据此宣称该 provider/能力已实机可用。

## 2. 基线证据与不可回退的现有行为

报告基于 `c438e3c`，不能直接视为当前实现清单。规划核对了下列当前文件及调用方的定点发现：

| 领域 | 当前事实 / 实施入口 |
|---|---|
| Pinata | `src/pinning/pinata.rs`：`PinataQueuePage.jobs` 默认 Vec 不兼容显式 null；分页需 camelCase 别名；`require_success` 把 401/403 都视为 Authentication 且丢失正文；upload `find` 仍走 CID 队列；upload client 只有 connect timeout。 |
| Worker | `src/pinning/worker.rs`、`src/store/pinning/jobs.rs`：提交错误统一恢复、鉴权/Terminal 恢复等待和 provider Terminal 拦截 IO 存在矛盾；attempts 封顶并不等于停止调度。修复须验证实际 HTTP 次数。 |
| 日志 | `src/main.rs` 接受 EnvFilter/RUST_LOG；调用方已定位锁定依赖 s3s 0.14.1 `src/service.rs:615` 的 `debug!(?req)`。仅修改自有 tracing 不能消除签名请求泄漏。 |
| Provider 模型 | `src/pinning/provider.rs` 只有 request-id 风格的 get/find/unpin；find 返回 Vec。`src/pinning/config.rs` 仅有 Pinata/Filebase/Noop，Filebase 工厂仍为 PSA。 |
| DB | `pin_job` 没有历史 API/strategy 快照；`remote_pin` 以 provider/CID 作为键。现有 generation、claim、remote epoch 必须保留。 |
| ZIP | `src/zip/extract.rs`、`local_header.rs`、`integrity.rs` 已有 8 GiB 总解压预算、10,000 条目、64 MiB 元数据预算、CRC/截断/路径防护；不是从零补 ZIP bomb 防护。当前限额并未全部配置化。 |
| 三入口 | `src/s3/route/decompress_zip.rs`、`src/s3/ops/multipart.rs`、`src/import/decompress.rs` / `publication/zip.rs`。直接 PUT 先 add 源包再 cat 解压；MPU 捕获解压选项；import 有 durable claim、结果记录和 fingerprint。 |
| 发布 | `src/store/pinning/publication.rs::ZipPublicationRequest` 必须有 archive。目前 archive 和 entries 在现有所有权屏障下发布；不能仅把 archive 改为 Option 而忽略租约、版本和结果所有者。 |
| 重复规则 | 按用户确认保留 direct/MPU 同 key 的 last-wins；import 的 `ImportExtractionObserver::entry_started` 对重复目的地或 archive 冲突拒绝。root 应使用各入口真实最终发布集合，不统一改成新的去重规则。 |
| 响应 | `src/zip/response.rs` 的旧 XML 以 ArchiveKey/ArchiveETag/ArchiveSize 为主体；结果关闭仍保留空 PUT body / 标准 Complete XML。无源包模式不能沿用其虚构源对象。 |
| 可复用 CAR | `src/kubo/tier_copy.rs`、`verification.rs` 和 residency 已有 hot/cold CAR 流式复制、终态/trailer 检查和本地完整性 receipt。这不是远程 provider，不能直接套用其身份和删除语义。 |
| root 归属障碍 | `m20260912_000001_residency_references.rs` 的引用包含 `(object_id,cid) -> objects(id,cid)` 外键。不能把目录 CID 塞到 archive 的 residency reference，更不能为 source=false 伪造 archive 对象。 |

### 外部协议证据

- 报告的 Kubo v0.43.0、Cluster v1.1.6 是固定资料版本，不代表现场部署版本。
- 本次通过官方仓库 GitHub API 精确读取 Kubo v0.43.0 的 `core/commands/object/object.go`：`object/new` 映射到 RemovedObjectCmd。**禁止把 object/new + patch/add-link 作为当前实现路线。**
- 同版本 `core/commands/dag/put.go` 支持 input-codec、store-codec、hash、pin，注册 dag-json/dag-pb；它可能先 emit CID 后 batch.Commit，因此收到 CID/200 仍不等于成功。优先验证 bottom-up dag/put 路线，见 §5。
- 来源：`https://api.github.com/repos/ipfs/kubo/contents/core/commands/object/object.go?ref=v0.43.0`；`https://api.github.com/repos/ipfs/kubo/contents/core/commands/dag/put.go?ref=v0.43.0`。Context7 `/ipfs/kubo` 只作辅助，master 文档不能替代固定版本证据。
- 尚未执行 Kubo 目录构建、Filebase/Pinata 账号写入或 Cluster 实机测试；本计划不以官方文档替代这些证据。

## 3. 已确认范围与全局安全契约

### 3.1 用户已确认

1. R24 的根表示 ZIP 内**成功最终文件集合**，保留相对 ZIP 路径；不含 S3 目标 prefix，也不含 archive 本身。
2. config 默认 ON；签名客户端 tag 的精确 `true` / `false` 覆盖默认值。MPU 在初始化、import 在接收时捕获，不能完成时随新配置漂移。
3. partial 成功根明确标 partial；zero 成功不返回 root。保持各入口原有 duplicate、路径、CRC、整批错误及发布语义。
4. 不改变对象 ETag/VersionId，不自动给 directory root 创建远程 pin 工作；ZIP + SSE 仍拒绝。
5. TTL 保持既有意图接受/本地发布起算的语义；不是首次远程成功后重新起算，也不是服务商硬 TTL 保证。
6. retain 或清理未完成的远端占用继续计账；不靠 lease 过期清零来无限放行新上传。
7. 不扩 tar、ZIP+SSE、remote-only，也不顺带建设生产本地 GC/整套 HA。

### 3.2 实施约束

- 主体本地发布、可选请求 accepted/skipped/rejected、remote_submitted、remote_pinned、gateway_verified、content_verified、replication_complete 分开。accepted 不等于 pinned，源包成功不等于批次全部备份。
- 未知副作用保持 unknown；完整、权威、作用域匹配的查询才可能证明 absent。超时、解析失败、查无权限、分页不全不能当空结果或直接重 POST。
- 远程资源键至少包含稳定 backend/scope/CID/资源类型；配置名不是稳定身份，URL/secret/hash 不是副本身份。与主存储同域不计独立备份；同 Cluster pinset 多入口不重复计数。
- 默认 cleanup=retain。只有排他管理约定、确定归属、本应用引用为零、无在途操作且 fence 当前时才允许 managed 清理。外部/未知资源不得自动删除；mismatch、退役、迁移也走同一台账。
- 不调用全局 repo/gc、block/rm 或批量 pin/rm。现有本地 no-pin-rm 行为保持；取消本应用引用不承诺立即回收块。
- 默认显式 cid/upload/car 策略，不自动切换远程端点、公开性或传输方式。按原 CID 复制实际存储字节，SSE 对象不得被解密后上传 public。
- 原始合法 tag 可保留，但执行依据是持久 ExtensionDecision，不是再次扫描 tag。普通更新、复制、重启、新增 provider 都不能激活 skipped 旧意图。
- 仅可选 pin 控制能 warn；鉴权、标签基础结构、完整性、加密、DB/Kubo 主写失败、服务器强制保护不能吞掉。`all` 不偷换成可用 provider 子集。
- DB 网络操作不放在长事务内；复用 mutation/import ownership、数据库时钟和 heartbeat。迟到结果只能在原身份和 fence 下登记，不覆盖新 generation。

## 4. 共同模型与接口方向

以下内部名称为实施方向，允许等价拆分；对外命名在对应阶段的兼容测试中冻结，不能把草案当作当前已支持配置。

### 4.1 Provider 执行与生命周期

- **ProviderRevision**：稳定 provider id、display name、backend/scope、API/profile、策略、确认要求、endpoint 修订、secret 引用/管理员修订号；不持久化 secret 或 secret 的 hash。换账户/桶不能沿用旧 scope。
- **InvocationSnapshot**：任务真正使用的历史协议/策略、资源类型、correlation、影响能力、当时保留/清理规则。旧记录没有证据时 unknown/needs_attention，不能用新 TOML 猜历史。
- **ProviderError**：category、operation、HTTP status、安全 provider code/message、Retry-After、effect certainty、first/last error。建议类别含 authentication、permission_denied、unknown_forbidden、plan_restricted、quota、rate_limited、invalid_input、transport、protocol、cid_mismatch。
- **Observation / RemoteRef**：类型化 PSA request / hosted file / RPC pin / Cluster pin；CID、opaque id、历史路由、查询完整性/覆盖范围、ownership、确认级别/时间。query 不再以一个 Vec 混淆 absent 与不可查询。
- **SideEffectLedger**：提交在途/未知、已有/新建资源、共享引用、清理待办/retain、remote epoch；分离重试次数、实际 HTTP 次数、恢复查询次数和时间预算。
- **容量**：有效租约、在途预留、未知副作用、已确认资源、retain/清理待办分层；同域/CID 实际资源去重，独立意图可取消/续期。late success 登记实际资源，不复活过期 lease。
- **兼容迁移**：扩展 schema → 读旧新 → 升级所有 worker → 开启新状态/功能 → 显式迁移。不得让旧 worker 认领不理解的新状态。回滚先停新分配/worker，保留台账与旧协议路由，不能靠删表回滚。

### 4.2 ExtensionDecision、诊断与管理

- 决定绑定 principal/request、对象精确版本或 batch、tag/control revision、config revision、effective intent、warnings，并与对应发布/标签写入保持事务一致。
- warn 的手动控制组整体 skipped：不新增 executable lease/target/job/reservation；服务器独立自动意图仍按其规则执行。cancel/renew 被跳过不能宣称远端已取消/续期。
- 标准 S3 body/状态不改；增加有限 ASCII warning/status headers。v2 ZIP/import 结果含 Warnings，旧 body 不注入 JSON/元数据/207。CORS 仅在授权规则内 expose 这些 headers。
- doctor/config explain 必须使用与 worker 相同的有效配置，报告构建版本、配置来源、变量 present/missing、能力操作/作用域/观察时间；read-only 默认，不启动上传测试。真实运行配置与独立 CLI 所加载文件须明确区分，不能把后者冒充前者。
- 管理写动作优先复用受控进程/管理 CLI 的配置与数据库鉴权，不新增匿名 HTTP 管理面。status/diagnose/explain、create/reapply/backfill、retry/migrate/reconcile/retire 各自有明确身份与作用域。
- 管理计划保存精确版本清单、预估、未知副作用、确认记录及幂等 id；不需要人为规定 hash 收据。确认后新增/覆盖的 key 不自动进入清单。

### 4.3 ZIP 批次与策略

- 新增独立 **ZipBatch**、**ManifestEntry**、**RootBuild** / **RootReference**。batch 不依赖 archive 是否发布。包含 owner、来源入口、request fingerprint、捕获选项/决定、输入身份、精确成功版本清单、失败记录、终态响应和构建 revision/claim。
- 发布内容 `publish_source` / `publish_extracted` 与远端集合 `zip_targets` 独立。远端目标必须属于持久发布产物，双方 false 拒绝；source-only 可免解压，R24 此时不生成虚假空根。
- legacy 未启用新产物规则时保持原策略行为。新模式建议“入口批次允许上限 ∩ 每个输出自身命中规则”，禁止子条目绕过 private prefix，也禁止 always 补回排除的源包。first-match、literal prefix、priority 小值优先/同值名称稳定排序、one 粘性/故障转移、all 要求保持。安全冲突显式拒绝而不是放宽并集；见 §8 待裁定项。
- source=false 只在显式协商的 v2 扩展中使用；原 key 无新对象，不删除/覆盖旧对象，不返回虚构 ETag/VersionId。响应以 batch id、source_published、输入摘要（不是对象 ETag）、manifest 引用为主体。旧普通 PUT/Complete 不静默变语义。
- 相同已捕获幂等 token + 相同 fingerprint 重放持久结果；相同 token 不同 fingerprint 冲突。不同 token 不凭字节相同认定同一意图。fingerprint 覆盖输出选择、root override、结果版本、原始语义控制和目的地等；未传 override 的重试使用既有快照，不因 config 改变而冲突或新建。
- MPU upload_id 映射固定 batch；import 复用既有 token/job 身份；直接 PUT v2 增加签名幂等 token。结果写回断线不重发子版本/整批 pin。
- manifest 对应本批实际提交的版本/CID，不在构根或重试时读取“当前同 key”替代。条目后来覆盖/删除不篡改历史 root。
- 批次计数分别表示发布文件、逻辑目标、唯一 CID、目标引用、新传输、skipped/blocked、各 provider 确认、满足全部要求的条目。65 项 × 2 providers 是 130 逻辑目标，不等于 130 次上传。

## 5. R24：UnixFS directory root 设计与验收合同

### 5.1 开关、捕获和返回

建议配置 `[decompress_zip] unixfs_directory_root = true`；建议保留 tag `ipfs-s3:zip-root=true|false`。两者均是新增接口，不是现有配置。tag 优先于默认值；精确布尔、重复 tag 仍按基础标签校验处理，不能把 `False`、未知值默认为 true。root tag 与远程 pin 控制是两个独立组，无 remote provider 不得把本地 root 跳过。

- direct PUT 在 SigV4 后、主体副作用前解析；MPU 初始化持久化，Complete 不接受偷偷覆盖；import submit 持久化并加入 fingerprint。
- 保留合法原始 tag；CopyObject/PutObjectTagging 继承或回写 tag 不触发历史 ZIP 重新解压/构根。非 ZIP 操作不创造 root 工作。
- OFF 返回 disabled 且零 root RPC；零个成功最终文件返回 empty/无 CID。全部失败是否保留 archive 沿用入口原语义，不能用空目录 CID伪装成功。
- success 返回 complete root；部分条目失败但有成功产物返回 partial root。partial 只表示解压/发布成功集合不含失败项，不能掩饰树构建漏链接。
- 兼容返回优先使用 `x-ipfs3-zip-root-cid`、`x-ipfs3-zip-root-status` 和 batch 句柄；旧 XML、ETag/VersionId、结果关闭的 body 不变。v2 XML/import 状态增加结构化 root（CID/status/count/revision/safe error）。import 202 只是接受，CID 只能在验证及发布后出现。
- 根构建失败必须返回可查询的 failed/retryable 状态及有限 warning，**不返回不完整/未经验证的 CID**。不把 root failure 计入旧解压 FailedCount 或把已发布对象说成没发布。
- 建议 enable=true（无论默认或 signed tag）不是 required/原子成功保证：失败与对象发布隔离，不以新增默认功能破坏旧成功写入。**signed true 是否要强制根成功属于 §8 的待裁定公共合同**；在裁定前不能自行交付 strict-on 或静默 best-effort。若要求 strict-on，必须在发布前构建成功、失败不提交任何新对象，并设计可恢复响应；不得发布后再用整体失败诱导重复写。

### 5.2 最终集合与树的确定性

1. 复用现有解压校验和每入口重复策略，形成“规范相对路径 → 最终成功文件 CID/size/精确版本”的稳定清单；不直接使用包含重复项的原始结果 Vec。
2. 相对路径由解压时持有的可信 entry 路径保存，不能对完整 S3 key 做任意 substring replace。大小写/Unicode 不擅自归一化，不改变旧 S3 key。
3. 保持 nested path；只创建成功文件必需的父目录，不加入 archive、目标 prefix、失败项或只有 ZIP 空目录项的额外树。目录项不参与成功文件数。
4. direct/MPU duplicate last-wins 的 CID 与最终发布内容一致；import 保留整批拒绝重复目的地的既有行为。以成功发布决议而不是“最后看到的损坏 entry”覆盖前一成功文件。
5. `a` 和 `a/b` 可同时为合法 S3 keys，却不能同时放进 UnixFS 同一个名字。检测这种文件/目录冲突后整个 root 标 failed/path_conflict；不重命名、不丢文件、不挑一个子集仍返回 partial CID。默认兼容模式不影响旧对象发布；strict 选择依 §8。
6. 固定 directory CID codec/hash、链接排序和构建版本；同一最终路径/CID 集合重复构建得到同一个 root。只要求本实现的确定性，不声称等于任意 `ipfs add -r` 配置所得 CID。

### 5.3 Builder：不重读全部文件内容

首选验证 **bottom-up UnixFS directory dag-pb，经 dag/put 写入**，复用已成功条目 CID，目录只含标准 UnixFS Directory Data 和命名 Links。使用合法 CID 解析及规范化；不同 CID 文本表示不等于不同 DAG。不可用自定义 JSON manifest 的 CID 冒充 UnixFS root。

阶段开始做有限协议验证后冻结实现路线：

- 用 Kubo v0.43.0（以及声明支持的现场版本）验证 dag-json → dag-pb、Directory Data、UTF-8 名字、按标准排序、link Tsize、嵌套读取、`ls` 与 `/ipfs/<root>/<relative>` resolve。
- Tsize 是 DAG 累积尺寸，不是 S3 logical_size；选择官方支持的元数据查询或经测试的尺寸计算，不能猜。可以读取有限 DAG 元数据，不能 cat/re-add 所有文件。
- dag/put 终态和后续 recursive pin/完整性验证都成功后才拿到可发布 receipt。成功输出后的 trailer/commit 失败仍是失败/未知。
- 大扇出目录可能超过 DAG block 大小。实测既有最大条目/名称预算；若 flat dag-pb 超界，必须在同阶段支持标准 UnixFS HAMT 或采用已验证、批次隔离的 MFS 构建路线。不能悄悄缩小 ZIP 接受上限来回避。MFS 属备选而非默认：必须证明精确暂存命名空间、跨进程隔离、GC 保护及只删除本批自有路径，不做全局 files/rm；若需扩大节点权限则先授权。
- 构建 RPC、元数据字节数、树深度、并发和执行期限有界；取消及时释放槽位。调用数量按目录/节点预算，不无限递归或启动每文件 future。记录 10k 条目情况下的调用数、峰值内存及耗时。
- 不新增网络可控 endpoint，不通过客户端 tag 修改 Kubo 地址，不自动降级到不保 CID 的重新 add。

### 5.4 本地根归属、发布与恢复

新增独立 batch/root 引用表，或等价的有约束 typed owner 模型；**不得放宽现有对象 residency FK 来伪造 root 的对象身份**。Root owner 不依赖 archive 存在；保持物理节点/tier/CID 绑定，可复用 physical residency 的验证结果但使用独立引用关系。

推荐时序（允许等价实现，不变更事务边界）：

1. 接收时分配稳定 batch，持久化捕获配置、构建尝试/fence；沿用 direct/MPU/import 的 mutation/claim heartbeat，读取/解压/构根均可取消。
2. 提取后保存或准备最终 manifest，在 DB 长事务之外构建、pin、验证目录。预先记下 root build 意图；每个确定 CID/未知副作用可恢复，不在进程内留下唯一记录。
3. 在已有原子发布事务中提交成功对象及其版本、有效 pin 意图、manifest、root receipt/status、独立 root owner 和终态结果。claim 失效或发布失败不能把已构建目录宣称为成功批次。
4. DB 提交结果不确定时按 batch/fingerprint 查 durable outcome，不重发对象版本。构建或 pin 响应丢失只对账/重做确定性 root 工作；root-only retry 使用同一 manifest，不重新解压/发布/远传成功文件。
5. 进程终止、断线、ownership lost、lease 到期、并发 retry 下，旧尝试不能覆盖新 revision。发布前构成的 root 若变成 orphan 也登记为未采用/retain，不能对相同 CID 无所有权地 pin/rm。

root 是该批的持久快照，不随单个输出删除/覆盖自动改写或取消；batch 记录与 root 引用不能因 archive lifecycle 删除而级联丢失。当前本地策略是不自动 unpin/GC：逻辑 retirement 与物理保留分别可见，保护记录参与未来 GC 的安全边界，但本轮不实现自动 GC。构建暂存/未采用根也要有容量、数量与告警预算；若现场已经启用外部 GC，验证构建中间 DAG 的 pin 保护/恢复，不凭 DB 引用声称 Kubo 自己懂该保护。

目录 root 的本地 recursive pin 不建立远程 lease，不计为独立 remote replica。需要未来远传 root 时应使用完整 DAG CAR/目录能力与新显式授权，不能 `cat(root)` 或认为叶文件远端 pinned 等于目录根已远端 pinned。

## 6. 阶段计划与提交边界

推荐顺序 **1 → 2 → 3 → 4 → 5 → 6 → 7 → 8**。Stage 4 的 ZIP/UnixFS 不依赖 Filebase/Cluster，优先交付用户新增需求。Stage 5 只依赖 1–3，Stage 6 依赖 5；Stage 7 的单对象运维可以在 2–3 后推进，批次 backfill 依赖 4。允许按证据调整内部顺序，不牺牲每阶段可用成果或安全前置条件。只有 §8 的相应受影响分支等待裁定，其余已明确工作无需重复审批。

每阶段先用定点 failing regression 锁定旧缺陷/新契约，再最小实现；随功能同步配置示例、用户文档、迁移说明及证据。以下 commit 是后续执行建议，不是 planner 执行的命令。

### Stage 1 — 当前缺陷与安全基线（R01/R02/R03/R04/R07/R13）

**成果：**当前 Pinata 路径可诊断、拒绝不永远恢复、upload 不依赖无关 CID 队列、停滞能退出且所有日志级别不泄密。

- 修改 `pinata.rs`、`provider.rs`、`worker.rs`、`store/pinning/jobs.rs` 及必要增量迁移，保留最小实际协议快照/副作用字段供 Stage 2 扩展；不以临时内存状态取代恢复证据。
- null/missing/[] 只在列表字段兼容；错误类型/畸形 JSON仍报 protocol。分页 snake/camel 非空 token、字段冲突、token 循环/截断有界失败，不将不完整列表视为 absent。
- 安全限长读取错误体；确定套餐/权限拒绝与认证错误分开，未知 403 不猜。能力级拒绝不锁死 upload/list/cleanup；真正凭据错误按凭据作用域阻塞并能在显式修复后恢复。
- 持久化 effect certainty 和初始错误；明确拒绝 blocked/failed，明确未创建的 transient 退避，可能创建先 reconcile。耗尽恢复次数/时间进入 needs_attention，保留引用与容量，不继续高频调度、不盲重传。
- 新任务按历史实际 strategy/API 查询；旧任务证据未知先隔离，不能因为 TOML 改 upload 就假定过去也 upload。
- pinning 上传增加无进展期限、源流/响应等待与独立 recovery budget；参考现有 tier_copy 进度机制，不加误杀正常大文件的固定短总超时。超时可能有副作用，取消不等于未创建。
- R04 覆盖第三方 s3s：优先在 subscriber 对未审计 request-dump target 设**不可由 RUST_LOG 覆盖的独立硬过滤**，同时保留安全白名单业务日志；若选择依赖修补/升级须验证 API 兼容，不只覆盖一行 EnvFilter。检查 response/error/debug dump 及 URL、headers、query、正文、配置 Debug 的全部出口。

**证据：**新 fixture tests + 实际 HTTP 请求计数；403 CID blocked 后 upload/list 可用；超时→重启→find 不重复 POST；达到预算后 worker 不继续 IO；缓慢持续进展不超时、停滞释放槽位。真实 axum+s3s 表面用 Authorization/预签名/Cookie/SSE-C/token 哨兵值在 info/debug/trace、恶意 target RUST_LOG 下捕获输出，断言无泄漏且安全诊断仍存在。

**命令入口：**`cargo test --lib pinning::`，`cargo test --bin ipfs-s3-gateway`，新增进程级日志测试（新增前明确命名，不能把零匹配算通过）。结束执行通用质量门槛。

**提交：**`fix: harden pinning recovery and redact request diagnostics`。停止边界：不新增 RPC provider、不重排生产旧任务、不把 Stage 2 的全部模型重构塞进补丁。

### Stage 2 — 稳定身份、归属与可观察 ledger（R08/R10/R11/R21 基础）

**成果：**现有 PSA/Pinata 就能正确显示历史路由、共享占用、错误/恢复证据，配置改名/策略变化不误操作已有资源。

- 在 `config.rs`、`pinning/config.rs`、`provider.rs`、coordinator/worker 和 `store/pinning/{leases,quota,jobs,publication}.rs` 增量落地 §4.1 模型，使用 `store/entities/remote_pin.rs` 的现有远程记录；可按职责新增 ledger 模块，不重写全部 worker。
- 正式迁移 entities/schema；旧 request id 类型保留解析及原协议 cleanup 路由，unknown 旧记录进入有解释的隔离状态。schema 保留唯一键、CAS/fence、数据库时钟和共享引用并发保护。
- 容量纳入 retain/清理失败/未知副作用；TTL 保存原起点和 expires_at，重试/迁移不延长；到期 in-flight 的 late success 仍记资源。配额淘汰沿用已存在 soft lease 规则，不为套餐拒绝淘汰内容。
- status 提供 local/remote/verification 分层、first/last observed/error、原 CID、strategy、backend/scope；可用性至少一个 provider 与 all completion 分开。观察 stale/unknown 不擦除历史确认。
- provider retirement 状态至少可停止新分配同时维持旧资源路由；完整管理动作 Stage 7 交付。

**证据：**旧数据库 fixture升级与新库；SQLite + PostgreSQL 并发 attach/cancel/expire/cleanup；两个对象共 CID只占一次实际资源且不互删；外部 pre-existing/主 Kubo 同域不获删除权；改名、换 bucket、credential revision、丢旧配置均 fail-safe；retain 连续过期仍阻止越额；late success 不复活 lease。迁移前后旧 PSA/Pinata 行为及引用可读。

**命令入口：**`cargo test --lib store::pinning::`，`cargo test --test multi_gateway`；新增该阶段 schema/PG 测试需明确执行环境，不能仅用 SQLite 代替 PG。

**提交：**`feat: persist pinning identities and resource ownership ledger`。停止边界：不因新模型启用 managed 清理或扫描生产旧 tag。

### Stage 3 — 可选控制、doctor 与策略解释（R15/R18/R11，R19 基础）

**成果：**无 pin service 时主体操作成功并明确 skipped；raw tag 可读但永不隐式回放；能辨认实际加载配置和逐操作能力。

- 实现共同 ExtensionDecision；覆盖普通 PUT、CopyObject、PutObjectTagging、MPU 初始化/完成、直接 ZIP、durable import。包含 warning 的响应只在主体实际成功后发；import 202 不伪报完成。
- 本部署显式 warn，新配置样例 warn；旧部署省略兼容选项时保留旧拒绝语义并给迁移说明。无配置/无匹配/格式合法但可选不支持整体 skipped；基础损坏标签及主操作故障保持失败。
- 持久 skipped 决定与 control revision；普通 tag 更新/复制/inherit 不重放，显式 reapply 的入口/模型留给 Stage 7。MPU 初始 skipped 后配置 provider，Complete 仍 skipped；初始 accepted 遇新的安全禁止不得越过保护。
- 提供只读 config explain / pinning doctor/status/policy explain，启动安全摘要，能力 supported/denied/unknown 按 operation+scope+revision区分。管理入口明确本进程与文件配置的证据范围。
- 公共 warning headers、v2 扩展 Warnings、分页安全状态查询及 CORS expose 示例；不把 raw provider body/tag 值写入 headers。对用户对象名和外发 metadata 默认最小化，correlation 使用不透明标识。

**证据：**报告 T-HIST-01–06、T-HIST-09 和 §13.2 全部情景；HTTP 结果 + warnings + DB 无新增 pin工作/占额三重断言。新增 provider、重启、复制、普通标签改动都不激活 skipped；accepted 后实际失败不倒改历史决定。真实 SDK/CLI 至少验证标准 PUT/Complete 响应不损坏，并说明自定义头如何查看。

**命令入口：**`cargo test --lib pinning::`，`cargo test --lib s3::`，`cargo test --test integration`，`cargo test --test cors`；新增 doctor/config CLI 测试确认命令无写副作用。

**提交：**`feat: add persisted pin request decisions and diagnostics`。停止边界：不以警告绕过安全条件，不在 doctor 默认执行写探测。

### Stage 4 — ZIP 批次、产物和 UnixFS 根（R06/R12/R19/R22/R23/R24）

**成果：**三入口一致支持 ZIP 新模式与 root；默认兼容返回，source=false 有独立身份，partial/root失败可解释且不会重写已经成功的对象。

- 先裁定 §8 的 ZIP/root 公共合同并完成 §5.3 有界 builder 验证；只阻塞相关 ZIP 代码，不阻塞其他阶段。
- 落地 §4.3/§5；建议新模块 `src/zip/{batch,options,directory}.rs`、`src/kubo/directory.rs`、`src/store/zip/`，按责任拆分，不继续把所有逻辑堆入现有巨大 route/publication 文件。
- migration 增加 batch/manifest/root owner/build snapshot；更新 config、tag 解析、`store/multipart`、import request fingerprint/result，以及 publish_standard_zip / MPU commit / import completion 的原子发布入口。
- 形成统一最终发布集合再计算远程目标和目录；源可选但不移除既有 key/prefix ownership/fencing；v2 source=false 与旧客户端明确隔离。
- 参数化既有 ZIP 限额，增加单条目/树/规划/事务目标数/处理期限预算，保留 CRC、安全路径及总量上限；长事务前完成有界规划。后台 fanout 复用 durable worker，per-provider 并发/在途预算、分批 claim 与公平性保证前台普通写及必要恢复/清理不被整批饿死。跨进程全局公平不在此伪实现。
- root ready 仅在 final set、递归 pin/完整性、DB发布/receipt绑定都成功后返回；root failure 与对象发布边界按冻结合同执行。off、empty、complete、partial、failed、可重试中状态均有可查询语义。

**证据：**

1. direct PUT、MPU、import × config on/off × signed true/false/absent，捕获后配置改变、签名篡改/无效布尔/重复 tag、不支持 SSE；off零构根调用。
2. simple/nested/Unicode/escaped names、empty file、多块文件、duplicate winner、import重复拒绝、a 与 a/b 冲突；root不含 prefix/archive；失败项不在树内；zero无 CID；partial有且仅有成功最终文件。
3. 实际 Kubo `ls`/resolve/get及每条路径读取到预期原 CID和字节；确定性重复构建；不允许仅断言 mock Hash字段。构根调用记录证明没有 cat/re-add全部文件。
4. 10k 条目/深路径/64 MiB元数据边界、flat目录超大/合法 HAMT、预算/取消；没有降低原 ZIP 接受能力来让 root 测试通过，超预算root明确失败而非伪成功。
5. source=false时原 key 不存在/已有旧对象、versioning Enabled/Suspended/Disabled、结果关闭/旧客户端；源 ETag/VersionId不虚构，旧对象不动；source+extracted远程组合、逐输出 private策略、always不补源、auto/manual去重。
6. 响应丢失/重启/MPU完成重放/import重试/相同token冲突；manifest、版本、root与pin job不重复。构根每个边界 crash/DB提交未知/lease失效/目录pin响应丢失均可恢复；只重试root不重发对象。
7. archive不存在或被lifecycle删除时root owner仍正确；output覆盖后root仍表示原快照；不会出现伪FK、孤儿不可观察工作或自动pin/rm。
8. 65/30条目×多provider，有共享CID、少数失败与quota_blocked：实际工作有界，计数分母准确、定向重试不重传成功项、普通写和recovery仍进展。

**命令入口：**`cargo test --lib zip::`，`cargo test --lib import::`，`cargo test --lib s3::`，`cargo test --lib store::pinning::publication`，`cargo test --test integration`，`cargo test --test postgres_import`，`cargo test --test mutation_ownership_pg`，`cargo test --test residency_publication`。新增 `tests/zip_directory.rs`（拟）覆盖真实协议，现有真实客户端测试按可用已安装工具执行 PUT/MPU/import/GET；不能自动装 SDK或拉镜像。

**提交：**`feat: add durable zip batches and UnixFS directory roots`。停止边界：不远程pin root、不启本地GC、不支持remote-only/tar/SSE、不放宽import重复规则。

### Stage 5 — 公共 RPC、Filebase/Kubo 严格复制（R05/R09/R10/R13/R16/R21）

**成果：**Filebase RPC/upload 和独立 Kubo cid/upload/car 使用同一安全底层但不同配置档；严格 CID 与所有权/恢复/配额随功能同时可用。

- 新 `src/pinning/ipfs_rpc/` 抽 transport/add/observe/profile，复用 `src/kubo` 的 NDJSON、流式/idle/终态与 CAR 经验；主存储和远程实例的 endpoint、auth、scope 隔离。
- Filebase旧配置无 api仍PSA；新增 kind=filebase/api=rpc/strategy=upload；公共 ipfs_rpc/profile=kubo。能力矩阵前置校验，Filebase PSA/upload拒绝。不把换域名当RPC适配。
- Filebase仅发送官方及验证支持参数（add 的 cid-version/wrap-with-directory等）；不默认发送raw-leaves/chunker/pin=false/progress=true，不在upload后依赖已被拒绝的独立pin/add。
- Kubo可固定导入参数，upload返回CID严格规范化比较；CIDv0/v1合法等价与codec/DAG真差异区分。mismatch不修改ETag，记录资源后按所有权决定清理，不能按CID删除外部资源。
- Kubo CAR复用完整DAG传输，不用允许缺块的local-only导出来宣称完整；单根、Root/PinErrorMsg、末端/trailer、recursive根pin和实际内容验证齐全。目录不可走cat(root) upload。Filebase CAR保持experimental/unknown直到专项授权验证，不自动启用。
- RPC query必须区别recursive/direct/indirect及完整/未知，不能依赖不存在的gateway metadata；丢响应用类型化observation和历史snapshot恢复。
- URL无密码、可信管理员endpoint、内网显式授权、TLS/CA/mTLS与Bearer/Basic配置按支持范围验证；不关闭证书验证，不跨域重定向转凭据，status/gateway与RPC凭据隔离。普通S3 tag不能指定endpoint。

**证据：**报告 T-RPC-01–06、11–16、18及R09矩阵；零/小/raw/多块/MPU/SSE stored bytes/ZIP entry/目录CAR；fake 200+Hash后trailer错误不算成功；流停滞与未知副作用；共享/外部pin无误删。双Kubo独立环境验证目标递归pin与隔离源后读回；Filebase真实小文件add→CID→账户pin→网关字节单独授权，未授权只记mock通过。

**命令入口：**`cargo test --lib pinning::`，`cargo test --lib kubo::`，`cargo test --test integration`；新增RPC契约测试文件/受控真实测试，与现有`tests/lifecycle_transition_car_proxy.rs`回归，防止抽底层破坏hot/cold。

**提交：**`feat: add scoped IPFS RPC pinning providers`。停止边界：无自动策略回退、无Filebase付费能力承诺、无未经授权的写探测或managed cleanup。

### Stage 6 — Cluster 真正副本确认与 CAR 扩展（R17/R09/R11/R16）

**成果：**Cluster是独立remote pin service而非替换主Kubo；Proxy接收、节点available、min_replicas完成分开。原生REST路径按独立协议交付，不偷用Proxy透传假装Cluster CAR。

- `ClusterProxyProfile` 必须带 REST status verifier：读取 allocations 和全局 pins，绑定资源/分配revision、观察时间、去重peer；remote/missing/error/stale不算pinned副本。缺status配置不能注册为会伪报完成的完整provider。
- min_replicas是确认条件，不冒充已改Cluster replication_min/max。provider内部先达标，再按policy one/all聚合；同pinset多地址一个backend，物理故障域未知不猜。
- 原生 `ClusterRestProvider` 的add/CAR、状态、删除使用REST DTO/路径；不对同任务同时Proxy add和REST add。完整单根CAR、反向代理stream-channels配置及完整性另验；Proxy dag/import透传到配对Kubo不算Cluster登记/副本。
- allocation变化/人工删除/状态端点掉线使新观察degraded/unknown但不抹历史；是否自动repair由显式配置，不从旧tag产生工作。

**证据：**T-RPC-07–10、13、17–18：1/2与2/2、remote peer、陈旧观察、分配变化、多入口同域、不足副本不宣布all完成。完整/缺块/多根/错误根/重复块/源中断CAR，隔离源和第三方提供者后从确认的目标节点读取。实机使用已存在且授权的Kubo/Cluster镜像/环境；不得自动pull或在生产停止节点。

**命令入口：**`cargo test --lib pinning::`，`cargo test --test cluster`（核对实际测试过滤/环境），新增Proxy+REST contract tests；真实副本验收必须记录具体版本、backend与节点观察，mock不得记为N/N实机通过。

**提交：**`feat: verify Cluster pin replicas across proxy and REST APIs`。停止边界：不推断Filebase内部物理副本，不将两个同宿主peer宣传成灾备。

### Stage 7 — 历史补 pin、迁移、退役与定向恢复（R08/R20/R21/R23）

**成果：**修复旧队列、首次给历史子文件pin和退役源包意图是不同可审计动作，不靠SQL或重传ZIP。

- 管理入口实现create/renew/cancel/reapply/backfill与retry/migrate/reconcile/retire；共享前述snapshot/ledger/claim/容量模型。read-only dry-run默认展示目标范围、精确版本/CID、费用相关数量/逻辑占用、未知副作用及待授权动作。
- backfill规划→确认→执行，固定版本清单与幂等标识；覆盖/删除/不可读/新安全禁止逐条停止，不换成同key新对象。没有可信旧ZIP manifest时只列候选待明确确认，不能靠prefix或旧日志自动认领。
- retry/migrate只改变已有目标的执行，不把archive CID变成entries；unknown先查旧协议，明确未发送/拒绝才能安全重新排。旧成功资源保留原管理协议。
- retire停止新分配、盘点所有租约/retain/unknown/inflight/cleanup，再明确retain/迁移/受控清理；失去旧凭据或adapter时needs_attention，不删配置后无期限静默等待。retirement解除root逻辑引用也不触发本地pin/rm。
- 多batch共享目标更新进度，定向retry只处理缺少要求的目标；不重复上传已确认内容，不降低all，也不默认取消旧源包意图。

**证据：**报告T-HIST-07–14、16；旧SQLite/PG任务升级→diagnose→计划→确认→执行→重试/退役端到端。skipped需显式reapply；相同计划重放零额外发布/上传/重复计额；unknown残留不丢；到期/late成功/retain/cleanup失败占用准确。生产迁移仍须实际作用域授权，本阶段只在fixtures/隔离测试资源证明。

**命令入口：**`cargo test --lib store::pinning::`，`cargo test --test multi_gateway`，新增管理CLI集成测试（含dry-run零DB业务写/零provider写与确认后并发变化）；PG专项按已授权环境执行。

**提交：**`feat: add audited pinning backfill and provider retirement`。停止边界：不执行用户旧队列的真实迁移/删除，不把运维功能实现授权当生产变更授权。

### Stage 8 — R14 有界研究与整体收尾（R14，R12/R13/R23 的P2边界）

**成果：**在本文件内记录可执行的下一步建议、基准证据及明确的安全gate；不是生产GC/跨进程全局限速上线。

- 对已交付单进程provider限速/并发/在途字节/扇出调度测65/10k条目与多副本混合流量，记录前台延迟、队列积压、恢复与清理进展、容量/暂存上限。未测得瓶颈不乱加索引或重做HA。
- 研究跨进程backend级预算与公平性所需一致性/租约成本；现有每进程rate limit不能宣称全局保证。给出是否值得实施及具体触发阈值，不假装已部署。
- 本地GC研究盘点对象版本、delete marker、lifecycle hot/cold、MPU parts、import暂存、ZIP batch/root、中间目录DAG、unknown副作用与共享CID引用；说明root生成默认ON增加保留的实际成本。
- 提出未来安全GC的dry-run、全owner快照、在途屏障、ownership证明、双后端迁移与故障恢复条件；不得在研究中运行repo/gc、block/rm或解除本地pin。数据库逻辑引用释放不等于块释放。
- Filebase CAR/删除/较大文件仍按单独授权测试记录supported/denied/unknown；字节备份（允许不同CID）、remote-only、成功后起算TTL仍是另行范围，不借收尾顺带实现。
- 更新本文件的剩余风险、环境未验矩阵和用户文档边界；不另建独立spec/plan。

**证据：**有界基准、资源归属矩阵、现有no-pin-rm回归和明确go/no-go条件；未得授权的外部实验列未执行。

**提交：**`docs: record pinning capacity and local GC research boundaries`；若同时包含已授权离线性能测试，则可用`test:`标题并在正文明确研究而非生产能力。此提交属于用户要求的阶段提交，不是独立plan提交。

## 7. R01–R24 完整追踪

| ID | 范围与阶段 | 必须看到的结果 |
|---|---|---|
| R01 | 1：Pinata列表/分页 | null/missing/[]成功，非法/冲突/不全不当absent。 |
| R02 | 1：结构化错误 | plan_restricted与auth/权限/未知403分离，安全根因保留。 |
| R03 | 1–2：副作用恢复 | 确定拒绝停止恢复，unknown不盲POST，预算有效。 |
| R04 | 1：日志 | 第三方s3s与所有RUST_LOG级别不泄露凭据/签名。 |
| R05 | 5：Filebase RPC | 真upload完整闭环、不依赖被禁pin/add。 |
| R06 | 4：ZIP产物 | 发布/远传分离，extracted-only不隐含源任务。 |
| R07 | 1–2：恢复路由 | upload不无条件访问CID队列，历史协议不漂移。 |
| R08 | 2、7：身份/迁移/退役 | 改配置后旧资源仍正确路由，不靠SQL。 |
| R09 | 5–6；4目录验证 | 严格原CID、完整CAR，字节相同不冒充DAG相同。 |
| R10 | 2、5–7：所有权 | 外部/共享/主存储pin不误删，清理有ledger。 |
| R11 | 2–3、6：分层状态 | 接受、pin、可读、内容、复制各有范围/新鲜度。 |
| R12 | 4、8：ZIP资源/恢复 | 已有预算不回退，可配置、有界、失败可恢复。 |
| R13 | 1、5、8：停滞/预算 | 大文件持续进展可完成，stall释放槽位，unknown保留。 |
| R14 | 8：专项研究 | 单进程与全局边界明确；不伪交付生产GC。 |
| R15 | 3：warn/skipped | 主体成功+warning+无新增手动pin工作；旧tag不回放。 |
| R16 | 5–6：公共RPC | Kubo/Filebase/Profile隔离，不替换主Kubo。 |
| R17 | 6：Cluster | Proxy+REST达标证据，同pinset去重；原生REST/CAR独立协议。 |
| R18 | 3：doctor | 实际配置/版本/secret来源状态明确，默认零写探测。 |
| R19 | 3–4：逐目标策略 | 可解释first-match/priority/one/all与输出边界，不越权。 |
| R20 | 7：历史首次pin | 精确版本清单、显式create/reapply/backfill，不重传ZIP。 |
| R21 | 2、5、7：TTL/占用 | 原起点不重置；retain/unknown/清理未完持续计账。 |
| R22 | 4：source=false | 版本化批次响应，无伪源ETag/Version，重试不重复发布。 |
| R23 | 4、7、8：扇出/进度 | 目标和传输分开计数、有界调度、定向retry、公平性证据。 |
| R24 | 4：UnixFS root | 默认ON/签名覆盖，真实最终路径树、partial/zero、兼容返回、独立owner。 |

## 8. 仅需向调用方返回的实质裁定

已确认的范围不再次索取批准。以下剩余选择涉及公共协议/安全，不由执行者猜测；在相应Stage 4编码前裁定并直接更新本节，无需创建第二份设计：

1. **signed root=true 的失败强度。**推荐与config ON一致：尝试生成、失败返回可查warning/status，不影响原发布；若用户把true理解为“必须root成功”，则采用发布前严格失败合同，并接受与默认兼容模式区分。文件/目录冲突必须fail-closed root、不返回伪root；不能同时承诺“所有旧S3键组合可写”与“所有成功请求必有UnixFS根”。
2. **新ZIP逐输出policy组合。**推荐入口允许上限与逐输出规则取交集，服务器强制约束冲突拒绝；legacy保留。报告明确此处待确认，不能自行用并集扩大外发范围，也不能把新限制静默套到旧模式。
3. **source=false 的新协议协商细节。**推荐v2结果版本+签名幂等token，result=false在该模式下拒绝，除非协商了明确可查询batch句柄。确认对外参数/结果约定后才能实现；不需要重新确认已明确的“允许不发布源包”。

实现层的dag/put尺寸/HAMT选择通过有界协议证据解决，不默认上升为用户设计审批；若只能通过新增节点权限、降低原ZIP上限或改变返回承诺实现，再升级该具体问题。真实provider写测试授权、现场后端版本/身份、PG/容器环境是环境与权限gate，不阻塞离线修复或被视为功能已验收。

## 9. 统一质量门槛与执行记录

每阶段只对相关输入变更重跑对应验证，最终执行适用整体验证。使用PowerShell，不用Bash环境变量语法或`&&`；不安装软件。命令成功必须结合测试数量/断言、未ignored/未环境跳过和真实行为证据。

```powershell
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --lib
cargo test --test integration
```

上列是验收命令而非规划时已执行结果。完整测试若需未安装依赖/未授权下载，先报告阻塞，不自行安装。DB迁移阶段须有SQLite及真正PG并发证据；真实HTTP/SDK、双Kubo/Cluster、provider账号实验分别列出环境/版本/授权范围。长期worker、测试网关和容器仅清理由本轮创建且身份确认的资源，不能使用宽泛`docker compose down -v`删除未知卷。

每阶段提交前按仓库规则检查 `git status`、`git diff`、`git log --oneline -10`，仅stage本阶段预期文件、排除secret/临时日志；不改Git配置、不跳hooks。若工作区有其他变更则保留并分离，不能假定规划时clean仍成立。用户已授权每阶段commit，不重复询问；没有任何push授权。

阶段执行记录：

| 阶段 | 状态 | 行为/测试证据 | 实机未验或裁定 | Commit |
|---|---|---|---|---|
| 1 | 完成 | lib 1187 passed / 1 ignored；bin 13、integration 164、真实日志 2；PG 并发 3 passed；pinning 定向 352 passed | 未执行真实 Pinata/Filebase 账号写入；完整身份/账户 scope 模型留给 Stage 2 | 随本阶段代码统一提交，见 Git 历史 |
| 2 | 未开始 | — | PG环境待确认 | — |
| 3 | 未开始 | — | 真实客户端可用性待确认 | — |
| 4 | 未开始 | — | §8；directory协议/实机验证 | — |
| 5 | 未开始 | — | Filebase账号写授权/双Kubo环境 | — |
| 6 | 未开始 | — | Cluster版本/拓扑/隔离测试授权 | — |
| 7 | 未开始 | — | 不包含生产迁移授权 | — |
| 8 | 未开始 | — | 研究不包含生产GC授权 | — |

执行者可调整模块拆分、测试文件名和内部实施顺序，前提是保持目标、已确认合同、权限、安全及上述证据不变。重大偏离记录决定、依据与错误代价；安全、数据、公共协议或外部副作用变化交回调用方裁定。由调用方组织所需plan-critic；本planner不派生审查或执行agent，也不把计划完成当作审查/实现完成。

### Stage 1 验收补充

- 新增 `pin_submit_history` 保存实际调用协议/策略、副作用确定性、首末安全错误、恢复预算与隔离原因；旧无证据的未完成提交隔离，不凭新配置推断旧策略。升级须停止旧 worker，保留占额与恢复责任；本阶段不提供更换账户/endpoint 的透明迁移。
- Pinata/PSA 禁止自动跟随重定向，3xx 不作为未创建证明；合法重新提交重新激活恢复状态但不清空累计预算。明确拒绝与未知副作用分流；upload 查询不进入无关 CID 队列。
- Submit 停放及 history 更新在同一带 fence 事务中完成。无 request 的 Reconcile 在锁 remote/Reconcile 之前按 ID 获取 Submit fence、锁后复查。显式 repair 使用 Submit → history → Reconcile wake 顺序，覆盖两个竞争顺序，避免丢失唤醒。
- `tests/postgres_pinning_stage1.rs` 在专用 PostgreSQL 17 中实际执行 3 项：takeover 阻塞、最终 CAS 失败回滚、repair/park 双向交错；fixture schema 清理确认剩余为零。检查精确 stale 错误，避免较早 SQL 错误造成假通过。
- `tests/request_logging.rs` 驱动实际 binary 和已认证的 DeleteObjects，注入正文 key/自由 DB error 哨兵，在 info/debug/trace 和恶意 target filter 下验证硬过滤。安全 request ID、状态和有限 failure class 保留；第三方 dump 和敏感字段事件不可由 RUST_LOG 开启。
- 最后整合命令：`cargo test --locked --offline --lib --bin ipfs-s3-gateway --test integration --test request_logging --quiet` 全部通过；未将 ignored PG snapshot 或真实远端未执行项计作通过。Stage 1 限定复核无剩余 Critical/Important。
