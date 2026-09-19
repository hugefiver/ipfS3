# v0.6 Lifecycle Transition：Phase C–F 文件级实施计划

日期：2026-09-12。状态：计划完成，未实施、未运行验证；不是门禁通过记录。

唯一范围：完成 `ROADMAP.md:77` 剩余 transition；严格遵循 `docs/superpowers/specs/2026-08-26-lifecycle-program-design.md` Phase C–F。Phase A expiration、Phase B multipart abort、Versioning、CORS 已完成，不重做或扩展这些能力。本文不包含 Git 写入步骤。新增文件/符号是计划名称，非当前已存在的接口。

## 1. 完成条件与禁止事项

- 只有真实的 `STANDARD → STANDARD_IA`，同时支持 current `Transition` 和 `NoncurrentVersionTransition`；delete marker、未完成 multipart 不做 transition。
- 每个不可变内容版本有独立 primary residency；多个版本/对象共享 CID 时引用独立，物理内容按 tier/CID 去重。公开版本 ID、CID、ETag、加密 envelope、SSE-S3 key wrap、SSE-C fingerprint 均不改变。
- hot→cold 必须实际流式传送原 DAG/内容，验证原 CID 和 cold 本地完整性，再原子发布。没有验证，绝不报告 IA。
- 保留 `AppState.kubo` 及现有 `[kubo]` / `IPFS_S3_KUBO_RPC_URL` 的 hot 含义；新增可选 cold。默认无 cold、旧对象 STANDARD、原有 A/B 继续工作。
- Durable saga 独立于 `pin_jobs`；所有状态写入用 claim epoch/worker/有效 lease fence，执行前和发布前重查 revision、目标、过滤条件及冲突赢家。
- 所有支持的内容读取和 class-bearing 响应/列表一致使用选中不可变版本的 residency；cold 不可用不能悄悄回退 hot 来伪造成功。
- 只新增迁移，不修改既有 12 个迁移的生产 `up/down`、SQL、名称或历史顺序；允许适配旧迁移文件中仅依赖全局迁移注册数量的测试断言。所有逻辑释放经过 ownership/version/lease 边界；永不调用 `pin_rm`、GC 或 block 删除，即使计数为零。
- SQLite、真实 PostgreSQL、真实 SigV4 HTTP surface、真实双 Kubo/AWS 客户端、进程杀死恢复门禁均通过，才更新公开 README/配置样例/Compose/ROADMAP。
- 不新增 archive/restore、remote-provider cold tier、直接 PUT 到 IA、反向 lifecycle、认证变化、加密 Range 优化或版本标识变化。

## 2. 当前实现证据及关键裁定

### 2.1 已核实的接缝

| 现有路径/符号 | 事实与实施含义 |
| --- | --- |
| `src/store/mod.rs::migrator::Migrator::migrations`；`src/store/migrations/mod.rs` | 最新为 `m20260901_000001_lifecycle_abort_multipart`，共 12 个迁移；注册与迁移顺序测试都要追加 |
| `src/store/entities/object.rs::Model`；`entities/object_version.rs::Model` | `objects.id` 是内容 owner；`object_versions.id` 是版本行身份。`object_id` 连接两者；两者都不能替换为 key 或 public null version ID |
| `src/store/pinning/publication.rs::publish_in_transaction`、`write_object_version_and_update_lifecycle` | PUT、COPY、multipart complete、ZIP、import 共用原子内容发布边界 |
| `src/store/object_version.rs::install_content_version`、`prepare_install`、`remove_null_slot`、`remove_and_promote` | 覆盖产生新内容 ID；删除版本行不等于删除历史 `objects` 行。提升只改变 latest 投影，不能重置 class |
| `src/store/pinning/publication.rs::delete_*_in_transaction`；`src/store/pinning/leases.rs::lock_publication_lifecycle_frontier`、`end_active_leases_for_object` | residency 释放必须嵌入这些事务，不能绕过 tags、leases、ownership |
| `src/lifecycle/model.rs`、`config.rs`、`evaluator.rs` | 当前只有 expiration/abort；canonical JSON schema 1；transition 明确被拒绝；既有 UTC midnight 与 cursor 可复用 |
| `src/store/lifecycle_action.rs::claim_due_with_max_attempts`、`lock_claim_for_execution`、`mark_succeeded`、`schedule_retry` | 已有 claim epoch/DB clock/重试，但无长复制续租；action kind 有数据库 CHECK，不能仅扩 Rust enum |
| `src/lifecycle/actions.rs::execute_final_transaction`、`revalidate_candidate`、`action_matches_expected` | expiration 已在最终事务重算赢家；current/noncurrent 分类现在只识别 `ExpireNoncurrent`，需扩展 |
| `src/lifecycle/evaluator.rs::proposal_sort_key` | 现在仅按 action kind 区分优先级，没有 bucket versioning state；无法正确区分 current permanent delete 与 marker creation |
| `src/lifecycle/worker.rs::start_worker`、`run_worker` | 仅持 Store/config/token，无 Kubo；`main.rs` 已接入 root token 与 graceful shutdown |
| `src/s3/ops/object.rs::select_s3_object`、GET/HEAD/SSE-C helpers/COPY/listing | 当前选择器返回 `(object, version_id)`，读取写死 `state.kubo`；GET/HEAD/list 没有 residency class |
| `src/s3/ops/versioning.rs::content_dto` | `ObjectVersionStorageClass` 硬编码 STANDARD |
| `src/import/source.rs::execute_cid`；`src/kubo/add.rs::stream_add_with_progress` | CID import 保留外部 UnixFS DAG；普通 add 未记录原 DAG/chunker 参数，重新 cat+add 不能保证原 CID |
| `src/main.rs::health_check` | 当前 `/health` 是静态 `OK` liveness，不是 tier-ready 证据 |
| `tests/postgres_lifecycle.rs`；`tests/run-postgres-lifecycle-validation.ps1` | 可复用 PostgreSQL/双 gateway 的真实测试方式，但 Rust fixture 缺 env 可提前返回；最终门禁必须防止 skip 被当成 PASS |

### 2.2 矛盾、风险与需明确的解释

1. **只用 `stream_cat → stream_add` 会阻碍完整实现。** 外部 CID 导入、不同 chunker/raw-leaves/layout 的对象会生成新根。不能把这些合法旧对象永久留在 STANDARD 并宣称完整完成。选择 Kubo `dag/export → dag/import` 的 CAR 流式传输，保留原 DAG；这是程序设计明确允许的 Kubo-copy，不修改现有写入 CID 算法。可选 cat/add 快路不是必需，也不能作为唯一实现。
2. **cleanup 不等于物理 unpin。** 设计 117–120 行的“去除 hot residency reference”和 132–139 行的 no-pin-rm 可一致实现：只释放本 action/版本的逻辑 hot 引用，其他引用和 hot 物理存在记录保留；零引用也不物理删除。接受残留占盘，不承诺空间回收。
3. **SQL backfill 本身不能证明 Kubo 字节存在。** 迁移建立 STANDARD/legacy-unverified 记录；可恢复 verifier 完成 hot 本地 DAG 验证后标 verified。旧数据继续按历史 hot 路径读，不把 SQL 标签当验证证据；transition 必须先获得 verified hot。缺块要失败可观测，不改 CID、不删除、不伪造 IA。
4. **发布后 revision 改变不能撤销已经提交的 IA。** 发布前失效→取消；发布后 replay→识别同一已提交结果，只做幂等逻辑 cleanup/终结，不反向搬回 hot，也不因为现在不满足 STANDARD 前置条件而错误取消已提交动作。
5. **配置更新时序存在潜在字面冲突。** Phase D 必须修改运行时 `src/config.rs` 才能有独立 cold 配置；“config 只能全门禁后更新”若也禁止这个源文件，则与 Phase D 本身不兼容。本文将运行时配置代码列入 D，将 `config.example.toml`、`config.docker.toml` 和部署 Compose 列入 F。调用方须在 D 前确认该边界；若用户是字面禁止所有 config 修改，则 D 阻塞，应返回澄清，不偷偷绕开配置或宣称全计划可无条件执行。测试 Compose 也不提前改；门禁使用实现环境已准备的独立服务与显式 endpoints。
6. **s3s 与真实客户端仍有实施门禁。** 当前 `src/s3/ops/lifecycle.rs` 拒绝 `transition_default_minimum_object_size`。设计没有授权 AWS 默认 128 KiB/最小驻留日/费用规则；不自行加入隐藏限制，保持显式 filter 与 UTC eligibility。若锁定 s3s 0.14 DTO 或真实目标 SDK 自动请求该选项而无法表达设计行为，按设计 324–328 行停下提交设计修订，不降级测试。普通客户端选择不发送未支持扩展不是排除合法对象。

没有发现“严格不调用 pin_rm”与“真实 cold tier”本身不可实现的矛盾；真正的硬性技术障碍是把复制误实现成唯一 cat/add 路径。CAR 精确响应字段、流错误/trailer、pin 完整性 API 要由 D 的锁定 Kubo 版本证据确认，不能仅凭 HTTP 200。

## 3. 统一接口与数据模型（在 C 冻结，D/E 消费）

### 3.1 Residency 与引用

新增 `src/residency/{mod.rs,model.rs,router.rs,backfill.rs}` 与 `src/store/residency/{mod.rs,references.rs,publication.rs,backfill.rs}`。

- `StorageClass::{Standard,StandardIa}` 与 `KuboTier::{Hot,Cold}`；不要让 provider 名称成为 tier。
- `VersionResidency`：不可变 `version_row_id` 与 `object_id`，primary tier/class，monotonic residency revision；primary 更新不更新对象创建时间、noncurrent 时间、ETag 或 envelope。
- `PhysicalResidency`：唯一 `(tier, cid)`，tier node identity、verification state/time。共享物理记录不意味着共享公开 class。
- `ResidencyReference`：唯一 owner/reason/tier/CID，至少支持 retained version 与 transition staging/cleanup hold；owner 用不可变 identity，不用 key。
- 现有 `pin_lease`、`pin_lease_target`、remote/provider 状态仍是其引用事实来源。`reference_summary_in_transaction` 汇总 retained versions、residency refs、未完成 saga 和既有 active lease/desired target 保留理由；不将 provider pin 成功算 cold 本地验证，也不复制一套会漂移的 provider quota 计数。
- 对 multipart part、未发布 import 等 hot 内容，只保守保留其现有生命周期；不为它们伪造 public version/class。任何回收结论必须覆盖这些引用/未知保护；本计划不实施物理回收，因此不以汇总 count 授权删除。
- `attach_verified_hot_in_transaction`、`resolve_version_residency`、`release_version_reference_in_transaction`、`lock_residency_frontier`、`publish_cold_residency_in_transaction` 是窄接口。最后一个只有 E 的已验证且 fenced 路径可调用。
- 移除一个版本自己的逻辑引用不要求全局 count 为零；删除 tier/CID 的聚合存在/保留事实则必须证明全局引用已零，且本期仍保留物理 Kubo pin。计数结果不能由扫描时快照决定。
- 标记缺失仅在明确的 legacy 分支默认 STANDARD；未知 class、破损 owner 映射、已知 cold 记录缺失是内部存储错误，不能 blanket `unwrap_or(STANDARD)`。
- 生产不删除已经验证/迁移的 residency 表进行降级。版本删除后保留的 `objects` 行不能被 backfill“复活”为 live version。

### 3.2 事务与锁顺序

沿用既有 ownership/action/lease 顺序，不在网络复制期间持数据库事务。Lifecycle 的 action row fence 与 bucket ownership 事务边界分别沿用 `actions.rs` 当前路径；不要机械把 ownership helper 的“first operation”注释扩展成全局反转 action→bucket 的新锁序。

每条多 owner 路径按稳定 key/object ID、tier/CID 排序获取 residency frontier；新增 residency 锁在既有 version/owner/lease frontier 后统一获取。任何确需调整的既有锁序先补 PostgreSQL 并发证据，不在两个路径采用相反顺序。tag 变更、COPY、版本删除、import publication 与 transition publish 要共享 fence，不能只依赖进程 mutex。

### 3.3 Saga

新增 `src/store/entities/lifecycle_transition.rs` 和 `src/store/lifecycle_transition.rs`；一条 action 一条 saga，以 `action_id` 唯一关联，状态与生命周期 action 的 pending/claimed/retry/terminal 状态正交。

持久字段至少包括：不可变 target snapshot、source/destination tier/CID、source residency revision、expected node identity、prepare/copy/verify/publish/cleanup checkpoint、verification receipt、publication receipt、必要的 ownership generation。copy 完成不能等同 verify 完成。时间字段使用 DB clock；数据库约束限制固定方向及合法 shape。

`prepare → copy → verify → publish → cleanup` 是可恢复 checkpoint，外部 IO 与 checkpoint 写入不是一个原子事务，必须允许每个 IO 成功但 checkpoint 未写的重复执行。

## 4. 波次与文件级任务

依赖主线：`C1 schema/refs → C2 所有发布删除接入与 backfill → D dual-Kubo/读路由 → E1 协议和调度准备 → E2 saga → E3 同次开放API/报告 → F1 全矩阵 → F2 文档`。

E1 的纯模型/测试可以在 C/D 期间开发，但合入有效服务的配置 API 仍拒绝 transition；不能在 E2 完成前公开接受。每波先写会失败的局部回归测试，再实现，不要求每个小步骤重复全量测试。下列路径是各波 changed-path 边界；超出边界但仅为 AppState/内部签名编译适配时记录原因，涉及能力/数据/协议变化则返回设计决策。

### C1：新增 schema、residency 原语与升级安全

**新增**：

- `src/store/migrations/m20260912_000001_residency_references.rs::Migration`：新增 version residency、physical residency、reference、backfill cursor/verification 状态所需表/索引；不改 `objects.cid`/etag/crypto/version IDs。
- `src/store/entities/{version_residency,physical_residency,residency_reference,residency_backfill}.rs`。
- 上述 `src/store/residency/` 与 `src/residency/model.rs` 的数据层部分。

**修改**：`src/store/migrations/mod.rs`、`src/store/mod.rs::Migrator::migrations`、`src/store/entities/mod.rs`、`src/lib.rs`；追加注册与 shape 测试，不改旧 migration。

**实施要求**：

- SQLite/Postgres 都有 owner 唯一性、tier/class/verification CHECK、查询索引和一致 UTC 类型；拒绝 delete marker 获得内容 residency。
- 迁移只做数据库工作；legacy 数据写为 STANDARD + 待验证。迁移事务/重跑失败恢复使用现有 PostgreSQL advisory lock 与 SQLite 写串行化。
- 从第 12 版升级和全新安装都成立。保留 A/B action 原行、claim epoch、terminal state、config revision、cursor、tombstone；不能通过重建表丢失旧 action/index/constraint。
- destructive down 要 fail-closed：存在 IA/未完成 saga/无法无损恢复的 residency 时拒绝降级；不得改历史 migration 的 down。功能回退用禁用新 transition，而非擦表。

**验收**：新增 `tests/residency.rs` 和 `tests/postgres_residency.rs`，从真实旧 schema seed current/noncurrent/null/marker/shared-CID/orphan-object/active-lease 数据升级；逐字段对照不可变身份/encryption；约束非法写入失败；重复启动/两连接迁移安全。此波 S3 仍无 transition 支持。

### C2：让所有内容发布、删除、提升与旧对象建立正确引用

**修改路径/符号**：

- `src/store/pinning/publication.rs::{publish_in_transaction,write_object_version_and_update_lifecycle,delete_unversioned_current_in_transaction,delete_enabled_current_in_transaction,delete_suspended_current_in_transaction,delete_exact_in_transaction}`：发布 hot 引用与 version/lease 同事务；覆盖/永久删除释放正确旧 version 引用；创建 marker 不释放保留内容版本。
- `src/store/object_version.rs::{install_content_version,prepare_install,remove_null_slot,remove_and_promote,resolved_from_row,scan_versions}`：保留不可变 identity；在适当唯一删除边界做引用释放，避免 publication 和 remove 双释放；提升旧版本保留原 primary。
- `src/store/object.rs::{insert_immutable_in_transaction,set_only_latest}`：只需要 publication 接缝适配，不把 class 绑到 latest key。
- `src/store/pinning/leases.rs::{lock_publication_lifecycle_frontier,end_active_leases_for_object}` 与 `src/store/residency/references.rs`：同步读取/保护既有权威 lease/target 引用，继承已有 provider 行为，不重写 provider worker。
- `src/lifecycle/actions.rs::execute_lifecycle_delete_guarded`：复用已接入的精确版本删除路径，不再新增旁路 SQL 删除。
- `src/residency/backfill.rs`、`src/store/residency/backfill.rs`：有界 stable cursor、DB lease/epoch、hot 验证回写；在事务外读取 Kubo，回写前重验 owner/版本存在；重复/中断/覆盖不得复活旧版本。规则冲突计算仍把待验证 STANDARD 的 transition 视为赢家，但在源 hot residency 验证完成前不创建 durable action；后续有界扫描必须在同一 config revision 下再次发现并调度它。若旧版本的 action 已经存在，待验证依赖只能进入不消耗普通 action attempts 的等待状态，不能 terminal stale、failed-safe 或让较低优先级动作获胜。
- `src/kubo/verification.rs`（新增）与 `src/kubo/mod.rs`：提供 hot 可用的 `verify_local_residency`，通过现有 KuboClient 调本地完整性/pin 查询；必要时只在 `client.rs` 加窄访问器。C 不能依赖尚未实现的 D verifier 来声称 verified-hot 门禁完成。

**backfill 接口**：C 用现有 hot Kubo 和可注入 verifier 验证完整本地内容，D 复用统一的 local-verification primitive；可按 `(hot,CID)` 去重复 IO，但逐 version attach 引用。hot 下线只延后验证，不阻塞不相关正常启动/expiration，也不把它当 verified。运行时有缺失旧引用的安全惰性修补可与后台扫描共用同一事务原语。

**验收**：SQLite + Postgres 都覆盖相同 CID 两 key、两 version、不同 bucket、null 覆盖、marker 创建/删除、提升、lifecycle expiration、并发 backfill 与 PUT/Delete；检查另一个 owner 仍可解析/读取，lease 不被误终止，逻辑 count 不负数/不重复；zero-ref 仍没有 Kubo unpin 请求。PUT/COPY/multipart complete/ZIP/import 各有发布接入证据。另覆盖 backfill 延迟超过普通 action 重试窗口后恢复：不修改规则、不增加 config revision，current/noncurrent 旧对象最终都能 transition，且 expiration/abort 不被全局阻塞。

### D：独立可选 cold、保真复制与 tier-aware read selection

**修改/新增**：

- `src/config.rs::{Config,KuboConfig,Config::load,Config::default_for_test}`：新增可选 `cold_kubo`，建议 `[cold_kubo].rpc_url` / `IPFS_S3_COLD_KUBO_RPC_URL`；保留所有 hot 配置。validate 不输出 URL 凭据。此源文件变更须先解决 §2.2.5 时序边界。
- `src/state.rs::{AppState,new_with_env}`：保留 `pub kubo: KuboClient`，新增 `pub cold_kubo: Option<KuboClient>`；pinning coordinator 默认仍用 hot，不拿 provider 替代 cold。
- `src/residency/router.rs`：`TierClients`/`client_for_tier`/`resolve_read_source`；基于 DB residency 选择 client，明确 `cold_not_configured`、`tier_unavailable`、`cid_mismatch`、`local_copy_incomplete`，统一到 redacted AppError/S3 XML。
- `src/kubo/tier_copy.rs`（新增）、`src/kubo/health.rs`（新增）、`src/kubo/{mod,client,cat,pin,verification}.rs`：新增 `stream_copy_verified`、node identity/health probe，复用 C 的 `verify_local_residency`；使用现有受控 control client、无总上传超时 client、idle-bounded streaming 和 CancellationToken；不重用 `kubo/routing.rs` 的 provider discovery 语义命名。
- `src/kubo/cat.rs::stream_cat`：修正真实 Kubo Range 协议，把内部半开区间 `[start,end)` 编码为 `offset=start&length=end-start`，并同步修正仍模拟不存在的 `bytes=start-end` 参数的测试；plain Range 走 tier-aware client，SSE-S3/SSE-C 仍保持全量解密后切片，不扩展到 v0.8 的 chunk-level encrypted Range。
- `src/error.rs`：只暴露安全分类，不泄露 Kubo response/credentials；流建立失败在 HTTP response 提交前返回 S3 XML，发送 body 后的故障表现为截断/stream error而非伪成功 XML。
- `src/main.rs`：将必要 tier/verification 依赖注入 lifecycle/backfill worker，统一 child token/graceful drain；保留 `/health` 的现有 liveness 兼容，不将 `OK` 当 cold 就绪证据。tier 健康检查是内部 admission/诊断依赖，不擅自新建公开协议。

**复制算法**：

1. 从 hot 原 root 导出 CAR，带背压地包装为 cold import 请求体；不收集完整对象/CAR，不落全量临时文件，不解密。
2. 完整消费双方流与返回结果，检查错误记录/trailer、预期唯一 root、成功 pin 状态；CID 按解析后的完整 CID identity 比较（版本/codec/hash），不使用字节内容散列替代 CID。对象公开 CID/ETag 字符串绝不重编码。
3. 验证 cold 本地完整 DAG 与可读性及 pin；不是仅比 import response root，也不是允许通过 swarm 从 hot 临时取回块的普通 cat。使用已验证版本支持的本地完整性 API（如 files/stat with-local + recursive pin verification），D 门禁锁定准确参数/响应。
4. hot/cold URL 不同不足以证明独立：检查 node identity 不同且无共享 repo/卷；将已绑定 tier 的 identity 作为 verifier/publication 检查的一部分，配置指向其他节点不得复用旧 verified receipt。cold 不可用可暂缓新 transition，但已为 IA 的对象不允许降级 hot。
5. 失败可留下 cold 孤儿块/pin，不能发布引用或删除 hot。重试可重复导入同 DAG，restart 必须重新确认有效 receipt/本地存在。

**读取接入**：

- `src/store/object_version.rs::ResolvedVersion` 或新的 `ResolvedObjectRead` 包含 version identity + residency 一致快照。先选择不可变 version 后按同一快照路由，不二次按 key 查 class；list 使用批量 join/lookup 避免每项一次 Kubo RPC。
- `src/s3/ops/object.rs::{select_s3_object,get_object,head_object,collect_legacy_sse_c_plaintext,authenticate_sse_c_object,build_sse_c_get_response,copy_object}`：plain、SSE-S3、SSE-C、新 fingerprint/legacy、full/Range、显式 version/null 都经路由。
- COPY 的 source 验证读选中 source tier；新目标仍 STANDARD。若 source 只有 cold verified reference，先用保真复制保证目标 hot 完整存在，再走既有 publication；这不是对旧版本做 reverse lifecycle。不要假设 hot pin_add 会从 cold 联网找到对象。
- `src/s3/route/decompress_zip.rs`、`src/import/{source,decompress}.rs`、`src/s3/ops/multipart.rs` 审计具体读取来源：已发布版本经 residency；新上传 part、未发布 import、临时 ZIP root 保持 hot。不盲目替换所有 `state.kubo`，不新增目前没有的 UploadPartCopy API。
- `src/pinning/pinata.rs` 中 provider 验证仍是 provider 语义；如读同 CID 时要求 hot，保留其 lease hot 保护，不能将此成功当 cold 验证。

**验收**：双 wiremock 的正确 client 命中、不同节点身份、无 cold 默认、cold 下线；failed import/add、错 CID、缺 root、部分 DAG、pin failure、HTTP 200 错误记录、流超时/取消/背压、大于内存预算的对象、重启；所有失败都无 IA。真实双 Kubo（非默认 layout/import CID、raw leaf、空对象、多块、密文）证明原 CID，切断 hot 及 cold 的 swarm 获取后 cold 仍读完整内容；真实 Kubo 上验证非零起点、跨块 plain Range 的精确 bytes、`Content-Length` 与 `Content-Range`。此波配置 API 继续拒绝 transition，公开 IA 的开放留到 E。

### E1：增量 action schema 与完整 transition 规则/冲突模型

**新增**：`src/store/migrations/m20260912_000002_lifecycle_transition.rs::Migration`、`src/store/entities/lifecycle_transition.rs`、`src/store/lifecycle_transition.rs`；注册于 migrations/mod、store/mod、entities/mod。

**修改**：

- 新 migration 扩大 `lifecycle_actions` kind/target CHECK 接受 current/noncurrent transition，并建立 saga 表；SQLite 必要时新迁移内事务重建 action 表，完整保留 A/B 列、索引、状态和行；Postgres 原位约束更新，UTC timestamptz。不编辑 Phase A/B migration。
- `src/lifecycle/model.rs`：新增 `CurrentTransition`（Date 或 Days）、`NoncurrentTransition`（NoncurrentDays 与 DTO 支持的 NewerNoncurrentVersions）、`TransitionCurrent/TransitionNoncurrent`；canonical rules 新字段需 serde default，旧 schema 1 JSON 可读，旧 action idempotency 编码不变。
- `src/lifecycle/config.rs::{validate_and_canonicalize,from_canonical_json,to_s3_rules}`：完整 parse/validate/round-trip；只允许 STANDARD_IA 目的，完整规则原子替换；无日期/天数、二者同时、非法 class/组合、空数组、重复阶梯等拒绝且 revision 不变。遵循既有 rule/filter/1000 上限以及非当前保留数量的有效 filter 规则。
- `src/lifecycle/evaluator.rs::{LifecycleEvaluationContext,evaluate_candidate,proposal_sort_key,schedule_claimed_scan_page}`：提供 bucket versioning state、当前 primary class 与完整非当前计数事实，使用既有 DB clock/UTC 函数与过滤器；只对内容、STANDARD、正确 current/noncurrent 身份调度。
- 冲突按**实际操作效果**排序：permanent delete > transition > current marker creation；Unversioned/current null 在相应 versioning state 下的实际永久删除优先，Enabled marker creation 最后。保留同类最早 due 与稳定 rule tie-break；worker 使用同一个函数。已 transition 的当前版本后续 expiration 仍能调度执行，避免永久被已完成 transition 挡住。
- `src/store/lifecycle_scan.rs::scan_candidate_page`：增加 residency/versioning facts，不改既有 bounded cursor 语义；marker 不成为 transition 候选。
- `src/store/lifecycle_action.rs::{canonical_action_bytes,validate_action_identity,target_from_action,action_kind_from_db,persisted_action_kind}` 与 `src/lifecycle/actions.rs::{persisted_action_kind,revalidate_candidate,action_matches_expected}`：新增 kind，无侵入旧 expiration/abort identity；明确 current/noncurrent 判断不能沿用 `kind != ExpireNoncurrent`。

**验收**：canonical 老 JSON 回读、签名 XML 的预备测试、Days/Date/NoncurrentDays 恰好午夜与边界前后、tag/size/prefix、数量门槛、enabled/disabled、已有旧对象、永远不移 marker；permanent/transition/marker 的重叠规则矩阵与重复 action races；SQLite/Postgres 从第 13 版升级不改变 A/B 行。对外接受 transition 仍待 E2 完成，不提前关闭否定门禁。

### E2：独立 durable saga、claim 续租、最终发布 fence

**新增**：`src/lifecycle/transition.rs`（IO 协调）、`src/lifecycle/revalidation.rs`（从 actions 抽取最小共用 target/config/filter/winner 验证）、`src/store/lifecycle_transition.rs` 的事务实现；`src/lifecycle/mod.rs` 注册。

**修改**：`src/lifecycle/{actions,worker}.rs`、`src/store/lifecycle_action.rs`、`src/store/import/ownership.rs::{try_admit_lifecycle_mutation,clear_lifecycle_mutation_if_owned,verify_standard_mutation_guard}`、`src/store/residency/publication.rs`、`src/main.rs`。

- `execute_claimed_lifecycle_action` 按 kind 分派；expiration/abort 不得到 transition 的 IO 路径。worker 持窄 `TierClients` 依赖，或新增 tier-aware 启动入口并保留旧 start_worker hot-only 兼容包装。
- 新增 `renew_claim` CAS：action ID + worker ID + claim_epoch + claimed + 未过期，deadline 使用数据库 now；过期不能“复活”。heartbeat 只在 IO 进行中续租；丢失 fence 取消 IO，不写阶段/terminal，不清除新 owner guard。
- **prepare**：短事务锁 claim，读 DB now、revision/tombstone/rule、bucket/version/owner/tags/size/age/newer count/versioning、winner、STANDARD+verified hot；取得 ownership guard 和 source residency revision，写 staging refs/checkpoint。不存在/不适用则 terminal stale；临时 admission/DB/cold 故障有界 retry。源仍为合法 STANDARD 但 hot residency 尚待 backfill 验证属于依赖等待，不得消耗普通 attempts、terminalize 或改变冲突赢家；验证恢复后同一 revision 的动作必须继续。
- **copy**：事务外 CAR IO，持续监测 cancellation/claim；完成回写必须再次 fence。另一 worker 接管时允许保守重复复制，不把旧 worker 的临时结果当有效 receipt。
- **verify**：确认 root 与 cold 本地完整 pin，receipt 绑定 CID、node identity、source residency revision、saga；同 epoch 更新。重启后在发布前重新确认必要的实体事实。
- **publish**：新短事务重复 prepare 的全部资格检查（特别是 revision/tag/overwrite/current/noncurrent/winner），验证 claim 未过期、ownership guard、source residency revision、cold receipt；原子 attach cold reference + primary/class CAS + publication receipt/checkpoint。只改目标 immutable version，不碰 CID/ETag/crypto/tag/age/latest。
- **cleanup**：按不可变 owner 与 publication receipt 只释放该 transition 的多余 hot version/staging reference；保留共享 CID/leases/targets/其他 version 的保护；无 Kubo 删除。把 cleanup completion/action success/owned guard 清理尽可能同事务提交。
- 发布前配置失效、exact delete、版本角色改变或更高优先级 expiration→cancel，移除自己 staging 逻辑引用，不清理别人的持有。发布后按 receipt 恢复 cleanup；目标已被合法删除也可幂等结算，不复活它。不能以规则删除为由反向修改 class。
- bounded retries/max attempts/claim exhaustion 扩展安全分类；尤其已 publish 的 action 即使达到原通用 attempts 上限也要留下可恢复 cleanup 责任，不让 `fail_safe_exhausted` 静默抛弃它。逻辑 cleanup 可以独立持久重试；耗尽必须可见且保护引用仍在。
- 严格检查 shutdown：停止新 claims，取消/收束网络 IO 与 heartbeat，不遗留后台任务；forced kill 后新 epoch 接管，旧 worker 的 verify/publish/terminal 写入全部失效。
- 日志只输出允许公开的 bucket/key/version 与安全 failure category；既有 `actions.rs::log_action_diagnostic` 会输出内部 action_id，复用到新路径前按设计 redaction invariant 修正该共用 helper，不新增内部 IDs、原始 backend body 或密钥。

**验收**：每个 checkpoint 前后、IO 成功/DB 未提交、publish 提交/cleanup 未提交均 kill/restart；lease 到期、新 worker claim 后放开旧 worker；并发 config PUT/DELETE/disable、tag update、普通 PUT/COPY/import、exact delete、版本提升、expiration 竞争；两 gateway/Postgres 真竞争而非单连接串行模拟。任何 stale actor 都不能发布/终结别人的 claim。大文件复制超过默认 30 秒仍能完成，不靠把 lease 改为无限长。

### E3：同次开放合法规则与所有真实读取/报告面

**修改**：

- `src/s3/ops/lifecycle.rs::{put_bucket_lifecycle_configuration,get_bucket_lifecycle_configuration}`：只有 saga 可用时接受合法 current/noncurrent transition；无 cold 配置时含 transition 的 PUT 原子 InvalidRequest，expiration/abort-only 仍可用；暂时 cold 不健康不把合法规则当语法错误，已有配置 GET/DELETE 仍工作。
- `src/s3/ops/object.rs::{get_object,build_sse_c_get_response,head_object,build_listing_page,listing_dtos,list_objects,list_objects_v2}`：GET/HEAD（含 Range/SSE-C early return）和 ListObjects v1/v2 报告同一选中 residency；STANDARD header 可保留协议允许的省略习惯，IA 必须明确；列表显式正确 class，不做固定 STANDARD。
- `src/s3/ops/versioning.rs::{content_dto,build_version_listing_page,list_object_versions}`：每个内容 version 的 class 独立，marker 不伪造 StorageClass；分页/URL encoding/delimiter/cursor 不回退。
- `src/s3/ops/{object,multipart}.rs` 审计输入 storage_class：普通写入仅 hot/STANDARD；若现状忽略了非 STANDARD header，不允许让客户端以为它创建 IA，应显式拒绝本期不支持的直接类写入。此为防止虚假存储类声明，需真实 XML regression，不扩展为手动 transition API。
- `src/s3/handler.rs` 仅需要内部签名/DTO wiring 时改动；继续使用 s3s 0.14 路由/XML，不自建 lifecycle XML parser。

**验收**：通过真实 axum+s3s+SigV4 的 HTTP，而非直接调用 handler，验证合法 PUT/GET round-trip、非法原子拒绝、tombstone 后旧动作取消。GET/HEAD/versionId/null、ListObjects/ListObjectsV2/ListObjectVersions 同时混合 hot/cold/marker；复制 cold source 到 hot 新对象；密文、plain、Range、错误 SSE-C key；CID/ETag/bytes/metadata/tag/envelope 完全保真，hot 停止后 IA 仍读，cold 停止后 IA 不回退。

### F1：全门禁与可复查证据（不修改公开文档/部署配置）

**新增**：

- `tests/support/residency.rs`、`tests/support/lifecycle_transition.rs`：DB fixture、双 Kubo mock、checkpoint control、真实 signed request helpers。
- `tests/lifecycle_transition.rs`、`tests/postgres_lifecycle_transition.rs`、`tests/lifecycle-transition.Tests.ps1`、`tests/run-lifecycle-transition-validation.ps1`：真实 endpoint runner，缺前提显式 FAIL/NOT RUN，不把无 env 的 skip 记为通过。

**修改测试入口**：`tests/integration.rs` 挂 support；`tests/support/lifecycle.rs` 保留 A/B signed regressions；`tests/postgres_lifecycle.rs` 保留旧迁移/claim；`tests/multi_gateway.rs` 接入双 gateway 的 transition 并发；`tests/e2e.rs` 只添加所需真实读验证。不把整个历史 suite 重写为新框架。

**AppState 构造器编译适配边界**：现有 `src/s3/ops/{bucket,cors,lifecycle,multipart,object,tagging,versioning}.rs`、`src/s3/route/{gateway,decompress_zip}.rs`、`src/s3/route/import_object/tests.rs`、`src/import/{worker,pipeline}.rs`、`src/import/decompress/tests.rs`；`tests/support/{pinning,lifecycle,import,decompress,cors}.rs`、`tests/postgres_versioning.rs`。通常仅增 `cold_kubo: None`，新测试才配置 cold；不修改这些模块的无关行为。

**矩阵必须具备的结果证据**：

1. Fresh/upgrade SQLite 与 PostgreSQL schema；旧 A/B row 快照不变、schema CHECK 实际拒绝非法数据、迁移/重启幂等。
2. Shared CID：两 key、不同 bucket、不同 version、cold/hot 混合、active/manual lease；删除/覆盖/expiry/提升不会损伤剩余 owner，no pin_rm 请求监测为零。
3. Eligibility：DB now、UTC midnight、Date/Days/noncurrent time/数量、filters、disabled/deleted rules、missing bucket/config；每种实际永久删除/marker 状态冲突。
4. Saga：全部 state 边界与外部 IO 边界的进程杀死；epoch takeover、retry exhaustion、长复制 renewal、worker shutdown；最终 DB/class/read 三方一致。
5. Copy：真实 Kubo pin/本地完整性 + 两个独立数据卷/node IDs；非默认 DAG/CID import、大块、空对象、加密/多段完成根；cold 不能经 swarm 找 hot 补数据的离线读取证明。
6. Protocol：真实 SigV4 XML/API，标准与 IA 读/列表/版本列表/分页；GET 和 HEAD 不仅验证状态，还验证响应 header/body、CID/ETag 和 versionId；不得以 mock 返回任意同名 CID 代替真实完整性验证。
7. Regression：A expiration、B abort、Versioning/CORS、import/ZIP、pinning、multipart、SSE-C legacy、新 fingerprint 均无回退。流故障/存储失败不泄露凭据与内部 ID。

**执行环境与命令模板**（仅供实现执行，不在计划阶段启动 Docker/服务）：

```powershell
cargo fmt --check
cargo test --offline --lib
cargo test --offline --test integration
cargo test --offline --test residency
cargo test --offline --test lifecycle_transition
cargo test --offline --test cors
cargo test --offline --test postgres_residency -- --nocapture --test-threads=1
cargo test --offline --test postgres_lifecycle_transition -- --nocapture --test-threads=1
cargo test --offline --test postgres_lifecycle -- --nocapture --test-threads=1
cargo test --offline --test postgres_versioning -- --nocapture --test-threads=1
cargo test --offline --test postgres_cors -- --nocapture --test-threads=1
cargo test --offline --test multi_gateway -- --nocapture --test-threads=1
cargo clippy --offline --all-targets -- -D warnings
```

runner 要求已有 PostgreSQL 17、两个独立 Kubo、两 gateway、AWS CLI/SDK 可用，并接收显式 DB/endpoints；沿用 `IPFS_S3_TEST_POSTGRES_URL` 和现有 multi-gateway env，新增测试 hot/cold endpoint 参数。提前确认依赖/工具已安装；缺失不安装、不启动未授权基础设施、不降为 SQLite/mock 代替。`--offline` 缺缓存是未满足前提，不是假装测试已通过。每条原生命令检查退出码，PowerShell 不使用 Bash 语法。

新增 runner 的证据写 `tests/results/lifecycle-transition/<run-id>/`：gateway 实际 revision 与 dirty paths、Rust/s3s/AWS/SDK/Kubo/PG 版本、独立 tier topology、脱敏命令输出、测试实际执行/skip 数、XML/headers/bytes 断言、阶段 checkpoint/重启时间、最终引用快照与 cleanup 结果。不要写秘密环境变量、SSE-C key、连接串密码、原始内部 ID。

Live age fixtures 只允许独立测试 DB，在正常签名上传后通过限定行的测试 setup 调整 authoritative 时间，或者预置足龄数据；记录该 setup，禁止更改生产 DB/主机时钟或新增生产时间捷径。Kubo/进程停止与数据丢失模拟仅在该 runner 明确拥有的服务执行；不删除既有卷，不调用 pin_rm。测试结束恢复网络/进程、停自己启动的 worker、清自身 fixture，清理不成功也必须写证据。

### F2：门禁通过后更新文档、配置样例、Compose，最后 ROADMAP

**仅此波修改**：`README.md`、`config.example.toml`、`config.docker.toml`、`docker-compose.yml`；必要时新增 `docker-compose.lifecycle.yml` 作为 opt-in 双 tier overlay；`ROADMAP.md:77`。仓库当前只有 `README.md`，不创建翻译 README。

- 准确说明支持 current/noncurrent STANDARD→IA、真实冷节点、旧对象 backfill/default STANDARD、禁用配置/故障语义、no pin_rm 因而不承诺 hot 磁盘回收、不支持 archive/restore/provider pseudo-tier。
- 可选 cold 默认关闭；示例保留 hot env/schema，明确独立 cold volume/node、RPC 不公开暴露、多 gateway 使用相同绑定的 tier、升级与不可盲目降级的限制。
- Compose 不把 cold 变成默认启动必需依赖，不复用 hot 数据卷，不将 hot IPFS Cluster replica 或 Pinata/Filebase 指作冷层。锁定实际测试 image/version 证据，不将 `latest` 字样当版本凭据。
- F1 全部通过后才改这些文件；修改后的 TOML/Compose 还需解析与可选 cold 部署 smoke（属于部署文档回归验收）。其结果未通过前，ROADMAP 仍不勾选。
- 最后才把 lifecycle checkbox 改为已完成，保持 Versioning/CORS 既有状态，不新增与本任务无关的 roadmap/版本号声明。

## 5. 默认、安全回退与剩余限制

- **默认可逆**：未配置 cold 时所有历史/新内容保持 hot/STANDARD，A/B 服务不受阻；新增复制失败只留下额外物理块，primary 不动。禁用新规则会使未发布 action 取消，已发布 IA 保持原样。
- **已发布后不是零成本回滚**：停止 evaluator/取消新规则不等于移除 cold；已 IA 必须保留 cold 配置与数据、tier-aware reader 和 residency schema。不能回退到不识别 class 的旧二进制；需要单独设计的迁回/降级流程不在本期。
- **混合版本部署风险**：旧 reader 总读 hot，旧 writer 不写 residency，旧 worker 不认识新 action；在 E 开放前全部 serving/worker 实例升级并验证配置一致，旧版本不能继续写同库。不能以热节点尚残留 bytes 掩盖兼容性缺口。
- **计数不授权回收**：provider 计数不等于 Kubo residency 计数；未知/legacy/staging 都保守保护，零计数不调用 pin_rm。代价是额外存储，可接受且可见。
- **验证边界**：“GET 200”“import 200”“worker succeeded”“cargo test 全绿但 PG fixtures skipped”都不是完整证据；必须有真实 cold 本地 bytes、DB 发布与同一版本 S3 class 三者一致。
- **实施自由度**：新增模块可因责任边界微调命名/拆分，测试可并行；不可减少两种 transition、忽略旧 imported CID、绕开 immutable owner/fences、提前接受 IA、提前改公开文档。影响协议/安全/数据/范围的偏差返回调用方作设计决策。

## 6. 计划交接

当前工作树在计划写入前 `git status --short` 为空。本次仅新增本计划；未改代码/测试/配置/产品文档，未跑 Cargo/Docker/数据库/真实服务验证。

执行首要事项：确认 §2.2.5 的运行时 config 与公开配置样例边界，然后按 C1/C2 完成 immutable residency；D 必须以真实 Kubo 协议证明 CAR 保真和 local verification；E 不得在 D/E2 门禁之前开放 transition；F 以完整可复查证据决定是否允许勾选 ROADMAP。
