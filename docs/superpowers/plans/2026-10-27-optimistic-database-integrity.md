# Store 生命周期数据库完整性校验

## 交付目标与边界

SQLite 和 PostgreSQL 均在 Store/启动生命周期边界完成完整结构资格校验；同一 Store 及其 clone 的正常 S3、worker 和 runtime 业务路径不再重复查询 schema/catalog、迁移版本、结构 readiness 或 TEMP shadow。目标是消除热路径结构校验，而非把某次操作的 SQL 从 65 条降到 27 条。

- 资格绑定实际 Store 拥有的连接池及其固定 schema/search_path，不使用全局、按 URL 或跨 Store 的缓存。新 Store 独立取得资格；失败、取消、超时和未完成清理不得发布成功资格。
- 生命周期校验仍覆盖完整版本序列、管理存储与原生表结构、约束/索引/触发器、readiness，以及既有 shadow 拒绝规则。已有非空旧库/损坏库只读拒绝，不自动升级或修复；保留现有空库初始化行为。
- `pin_management_readiness` 是维护状态：生产写入者为 `management/schema/storage.rs::install`、`initialization.rs::upgrade_existing` 和 `initialization/upgrade.rs::backfill`，用于初始化及显式升级。正常业务不把它当作实时开关。其他 readiness 修改测试不得继续定义 warm runtime 契约。
- 结构维护、外部 DDL、TEMP 注入、迁移版本或 readiness 修改不属于在线逐操作检测保证。维护必须停止所有相关服务/worker/管理进程、排空操作、释放相关 Store/连接池，维护后重新启动并校验；不提供在线失效、后台巡检或大缓存子系统。
- 实时权限、配额、route/retirement、claim、epoch、generation、TTL、source/locator 完整性、跨进程原子性、远端 RPC 幂等和证据持久化全部保留。业务 SQL 报错正常传播，不借资格吞掉数据库错误。
- 裸 `ConnectionTrait`/`DatabaseConnection` 的管理、历史读取、升级、管理文件导入等独立边界保持完整校验，不能凭“同一 URL”或传入连接自动获得 runtime 信任。S3 内容导入发布属于应用 runtime，不与管理文件导入混同。
- 不改变 S3/管理文件协议、公开 API 契约或数据库格式，不新增迁移，不安装依赖，不涉及生产维护或部署。保留用户的 `ROADMAP.md` 修改。

## 波次一：统一 Store 资格与启动边界

涉及 `src/store/mod.rs`、`src/state.rs`、`src/store/initialization.rs`、`src/store/initialization/postgres*.rs` 及 `src/store/pinning/runtime_schema{.rs,/control.rs,/meter.rs}`。

1. 将现有 Store-owned `OnceCell` 资格用于两个后端，只保存已验证的静态资格和必要的版本信息，不保存业务行或准入结果。`RuntimeTransaction` 只能由其 Store 构造；保留现有 `Store::new` 兼容入口，在首次 runtime 使用时完成一次资格获取。
2. 应用启动在注册 provider identities、启动服务/worker 前取得资格。复用启动完整验证的成功结果，避免 `initialization::open` 后再跑一套等价完整验证：通过私有 Store-owned 初始化入口连接打开、验证与资格发布，保持公开初始化接口及其完整校验语义。不得增加任意连接可调用的“标记已验证”入口。
3. 保留 PostgreSQL startup 的 guarded connection、取消/超时、同连接清理、不确定 COMMIT 不重试等保证；仅在初始化事务/清理成功后发布资格。对懒初始化也保留失败可重试、并发初始化合并、单连接池不死锁、waiter 使用自身预算/取消状态等行为。
4. warm `schema_versions`/catalog classification 只读取静态资格，不发 SQL；可简化为实际需要的只读访问。删除 `check_schema` 及其 runtime 调用，不保留空函数或只改名的静态门禁。仍有意义的预算、deadline、取消检查放在现有 operation/session 边界，不能随结构检查一起消失。
5. `control.rs` 的逐次 SQLite compact/readiness/TEMP 查询不进入 runtime；不再需要的专用实现与测试删除，必要的冷启动拒绝能力由完整验证器承担。`meter.rs` 保留实际发生工作的计量，不为已删除查询收取虚构 SQL 成本；不得把 SQLite row 解码器直接用于 PG。无需为本优化重写通用计量框架。

完成证据：两个后端的冷 Store 均拒绝不完整结构、未知/缺失版本和非 ready 状态；成功资格仅对本 Store/clone 复用；失败或取消后不能以 warm 路径绕过校验；生产启动不做重复等价完整校验。

## 波次二：贯通实际业务路径，保留动态授权

依赖波次一的可信 RuntimeTransaction。优先复用已有 `ReceiptCatalog`、runtime 参数和 core 函数，不扩张成全局上下文或大量转发包装；普通 CRUD 无结构校验时无需改造。

| 路径/接口 | 最终改动要求 |
| --- | --- |
| `legacy_migration/receipt/catalog.rs`；`runtime_schema.rs::catalog` | SQLite/PG 的 Runtime 均走资格路径，PG 不再返回 Full。Full 仍使用完整验证器。历史版本分类能力保留在裸连接边界。 |
| `management/records/locator/discovery.rs`；`management/native/scope.rs::collect_runtime_scope`；`management/admission/{authority,locks}.rs` | Runtime 跳过结构扫描与重复版本比较，包括 PG 的 `collect_scope` 回退；继续验证 catalog revision 输入、database scope/identity、selector membership、source seals、locator 集合、依赖图和实际业务状态。不要把泛型 Full 入口整体改成免校验。 |
| `management/admission/worker_observation.rs`；`managed_jobs.rs`；`actions/job_authority{.rs,/route.rs,/completion.rs,/current_scope.rs}` | Store observation、调用前检查、source 检查、CompletionReader 对两后端保留 Runtime 身份；删除只因 PG 而转向 generic/Full 的分支。每个 observation 结束释放事务；其结果仍只是提示，不是跨 RPC 的授权。 |
| `jobs.rs` | `claim_due_worker_jobs_store`、`ensure_runtime_submit` 以及 managed-job claim 保持资格到内部 receipt/admission 读取；不得为 PG 退回完整检查。保留原有 PG claim/CAS/锁行为和 SQLite writer-first/重试语义，不把 SQLite 算法机械替换 PG 实现。 |
| `quota.rs`；`leases.rs`；`submission.rs` | `reserve_runtime_unique`、`settle_runtime_matching`、quota wake/release、目标投影/失败切换、retry/cleanup、receipt park/wake 均贯通 Runtime/catalog。删除静态检查，不删除配额计数、锁、退休准入、远端 epoch 与租约 generation/TTL 的新鲜读取。 |
| `publication{.rs,/application.rs,/reservations.rs,/zip.rs}` | 保持 `application::* → PublicationDatabase::Store → RuntimeTransaction` 在重试、配额预留、目标发布和提交全链路成立；PG 同样适用。`quota/production.rs` 的 Full/read-window 仅服务确实未获资格的裸连接入口，不能残留为线上 Store 快路径的回退。 |
| `s3/ops/{object,multipart,tagging}.rs`、`s3/route/decompress_zip*` 及对应 Store helper | 覆盖 PUT/Copy/CompleteMultipart/ZIP 发布、tagging 手工续租或取消、删除/覆盖后的租约收束。对仍借裸事务进入 receipt/admission 校验的路径传递真实 Store/runtime 上下文；维持整个对象/标签/租约事务和原锁顺序，不拆事务。 |
| `pinning/worker.rs` | 覆盖 claim → observation → dispatch/heartbeat → completion、到期处理、parked recovery、quota wake，以及 `management_tick` 的 claim/report。尤其贯通 `expire_worker_leases → expire_due_leases → end_leases`、管理任务与 cleanup 的内部调用，不能以“外层已调用 runtime”为完成依据。 |

### 必须保持的接口语义

- SQLite 的第一个业务写屏障仍先于业务读取；PG 的 row/table/advisory locks、NOWAIT、锁顺序、CAS 和事务隔离按原业务路径保留。`fence_runtime_authority` 去除 PG Full 回退时必须保留 authority/current-scope fencing；结构资格不是锁或准入许可。
- 新 claim/epoch/generation、TTL 变化、route 退休、额度耗尽、dirty/pending source 和 locator 不完整仍在实际事务中生效；不能缓存“没有管理记录”“不是迁移资源”等否定授权。
- 不可变 action/archive/receipt 内容校验本次保留：原文与 evidence 的身份、principal、database scope、plan/item、source closure、格式/长度和依赖绑定不是 schema 检查。只可消除明确由同一可信值已经完成的重复计算，不跨 operation 缓存 decoded authority，不取消内容校验以追求 SQL 数。
- `submission::complete_fact` 继续以原独立事实事务持久化已捕获的 provider 响应，不增加资格/readiness/业务准入依赖。投影、释放容量、唤醒工作仍在独立的新鲜授权边界执行；资格失败不得丢事实，也不得重复 provider POST。
- 裸连接管理 invocation、显式升级和导入的 Full 检查保留；Store-backed worker 后续执行使用 Runtime。管理请求解析/确认/TTL/授权与 receipt 绑定不因复用结构资格而简化。

完成证据：在真实业务调用链而非孤立 `schema_versions` helper 上观测到零重复结构查询；SQLite/PG 均不因 backend 分支或隐式泛型包装掉回 Full；动态拒绝、事务回滚、幂等和证据结果保持。

## 波次三：更新契约测试与最终验收

### 有效测试调整

- 更新 `runtime_schema/tests.rs`、`tests/{compact,shadows}.rs`：删除仅维护 warm 在线篡改检测或 compact 查询实现的测试；把版本/readiness/约束/启动 shadow 的有效拒绝用例放到冷 Store、重建 Store 或裸连接边界。不新增“损坏库必须继续工作”的反向测试。
- 更新 Store 路径旧契约：`publication/tests/runtime_publication_tests.rs` 的 live-controls/readiness-trigger 部分、`quota/runtime_wake_tests.rs`、`admission/worker_observation/tests.rs` 的 Store 部分、`rpc_worker_tests.rs` 的 transient-readiness recovery、`pinning/worker/tests/unpin_authority_tests/runtime_tests.rs`。只调整在线结构检测断言；保留其中配额、退休、dirty authority、connection cleanup、陈旧工作和事实恢复断言。仍走 Full 的测试（例如裸连接 observation/`quota_window_tests.rs`）继续保护完整校验。
- 双后端保留/补足资格并发、clone/new Store 隔离、失败不发布、取消与单连接池释放；改动启动结果传递时覆盖现有 PG startup timeout/cleanup 用例。
- 复用业务回归：退休后不能产生新任务、额度竞争不超配、旧 claim/epoch/generation 无权投影、TTL 到期不复活、取消/覆盖与后台工作竞争、重复/迟到响应不重复 POST 或释放容量、完成事实可持久化且恢复不丢证据。使用实际 Runtime 路径，而非只测试裸 Connection 版本。

### 少量 SQL 计数与真实表面证据

1. 在测试连接/SQL 执行观测层记录实际发送的 SQL，区分冷启动和资格完成后的业务区间；覆盖嵌套泛型/Full 查询，不能仅看 `runtime_schema::meter` 的计数。使用测试专用轻量计数/现有 tracing，不新增生产监控或依赖。
2. 在 SQLite、PG 各复用少量代表性路径：一个配置 pinning 的 S3 PUT/Complete 发布及 tagging 续租；一个非空 worker claim/submit/poll/完成恢复流程。断言 warm 区间对迁移版本、readiness、SQLite schema/TEMP catalog、PG 系统 catalog 的**结构校验读取为零**；业务权限和 source/locator 查询不归入该类别，必要 PG lock SQL 不误算成 catalog 检查。
3. 报告这些路径的实际总 SQL 数与结构校验数，使用相同 fixture 比较冷/热或修改前后；总数不做 65/27 之类脆弱精确锁定。观测必须确实捕获业务 SQL 和冷边界的校验 SQL，避免采集器未运行却得到假零。
4. 通过现有 axum/s3s + mock Kubo/provider 表面验证对象内容、CID/ETag、标签/租约、配额和持久化 RPC 事实，并覆盖 Copy/DELETE/ZIP 的受影响集成用例。保留至少一个现有双独立连接/Store 的配额或 claim 竞争用例，证明确实依赖数据库原子性而不是进程内资格缓存。

### 执行环境和完成判定

- 实现者按改动范围先运行模块定向测试；整合后集中运行一次必要的 lib 与受影响集成测试，验证证据必须覆盖最终改动及接口。
- 使用已有离线工具链，不安装依赖。PowerShell 示例：

  ```powershell
  $env:TMP = 'C:\Users\hugefiver\source\ipfS3\target'
  $env:TEMP = $env:TMP
  $env:CARGO_TARGET_DIR = $env:TMP
  cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --lib
  cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test integration
  ```

- PG 实际测试使用 `IPFS_S3_TEST_POSTGRES_URL` 的明确测试实例与隔离 schema fixture（如 `src/pinning/worker/tests/postgres_tests.rs`）；也可使用已安装的本地 PostgreSQL 容器镜像建立任务独占临时实例，不拉取镜像、不安装软件、不连接生产库或操作未知服务。仅清理本任务创建的容器、schema、连接和 worker。没有可用 PG 时，独立 SQLite 实现/验证可继续，但 PG SQL/并发验收必须报告未完成，不能用编译或被忽略测试代替。
- 相关模块注释及维护说明描述最终生命周期信任边界，不再宣称 runtime 每次检测 readiness/TEMP；不对说明文字编写锁定测试。
- 完成需同时具备：两个后端生命周期校验、真实 warm 路径零结构校验读取、动态安全回归通过、必要表面结果与清理证据。残余 Full 热调用、PG 未执行或未覆盖的安全行为均作为未完成项交接。
