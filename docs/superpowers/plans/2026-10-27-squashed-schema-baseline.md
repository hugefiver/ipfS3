# 合并 schema 基线并保留历史兼容

## 交付目标

SQLite 与 PostgreSQL 新库直接建立当前最终 schema，以一个新基线身份登记；不再执行从 `m20250701_000001_init` 到 `m20261023_000001_inline_documents` 的 36 段历史生产 DDL。基线可以按业务域分文件，共享适当的最终 DDL 构建逻辑，不要求单一大文件。

现有库通过既有显式 maintenance upgrade 接口保数据转换到新基线。支持的直接升级来源为：

- `m20261013_000001_zip_root_logical_release`，以下简称 m13。
- `m20261017_000001_management_storage`，以下简称 m17。
- 完整旧 36 项账本，以 `m20261023_000001_inline_documents` 结束，以下简称 m23。

继续保留既有历史记录、receipt、archive、历史目录和 ZIP 历史读取语义。历史只读支持范围不等于上述升级白名单，不新增更古老来源的自动升级。合并不是新增第 37 段迁移，也不是将旧生产迁移链整体改名搬迁。

最终交付包括最终布局基线、最小历史识别资料、必要的数据转换适配层、兼容读取接线及行为回归覆盖。保留所有当前产品功能和仍参与历史读取的表，不以表暂时为空为删除依据。

## 有效约束

- 不执行 Git 写操作、生产数据库操作、安装、下载或镜像拉取；不修改已有 `ROADMAP.md` 变更。
- 原始不可变 archive/receipt 的字节、metadata、catalog 序列、时间、身份与关联关系不改写。仅规范化活库 migration ledger 等转换所必需的状态；保留转换证明，不能靠删除业务或历史元数据、重建整库完成合并。
- 保留数据库 native identity 和 database scope；保留所有有效自定义索引，包括 `idx_object_versions_object_id` 与 `idx_physical_residencies_unverified`。这两个现存额外性能索引的保留不意味着将其强制加入所有新库。
- 非空旧库普通 open 只读返回 `UpgradeRequired`，不写锁表、身份、账本、readiness 或转换标记。未知、部分、混合、伪造账本以及身份/布局不匹配均 fail closed。
- 不削弱 Store 及 clone 的一次资格、启动资格复用、warm 路径零 schema/version/readiness/TEMP 结构查询。兼容逻辑只进入初始化、显式维护或既有历史读取边界，不加入逐业务操作扫描。
- 不削弱动态业务权限、quota、locks、CAS、claim epoch、generation、TTL、source locator evidence、独立 `complete_fact`，也不把历史证据升级成当前执行授权。
- PostgreSQL 全池连接固定 `search_path`；保留 guarded executor 的专属连接、锁等待预算、取消、cleanup、超时、连接丢弃和不确定 COMMIT 不盲重试语义。初始化和转换均不得绕开既有连接/事务安全边界。
- 执行者可以调整文件划分和等价实现顺序；涉及数据丢失、历史格式、授权、公开接口或验收缩减的问题必须返回协调者，不能自行降级兼容要求。

## 代码边界与最终职责

| 范围 | 最终职责及必须处理的接口 |
| --- | --- |
| `src/store/migrations/registry.rs`、`src/store/migrator.rs`、迁移模块入口 | 生产 registry 以新基线为起点；旧身份作为独立静态兼容资料，不再依赖实例化旧 Migration 获得历史名称 |
| `src/store/migrations/bridge.rs` | SQLite native migrator 与 PostgreSQL guarded 初始化使用同一最终布局定义；guarded 路径不另保留旧链 |
| `src/store/initialization.rs`、`initialization/upgrade.rs` | 区分新库、当前基线、明确支持的旧源、可验证恢复态及拒绝态；显式转换绑定 identity/scope/source/target |
| `src/store/initialization/postgres.rs`、`postgres/executor.rs` 及连接模块 | 保留锁内重检和 guarded 生命周期，接入最终基线但不重写安全模型 |
| `src/store/initialization/history.rs` 及其子模块 | 保留不可变历史转换档案和 `UpgradeStamp`；处理旧 target 的完成/中断记录与新基线转换的关联 |
| `src/store/pinning/management/schema.rs`、`schema/storage/` | 当前安装资格使用新基线版本；结构、触发器、readiness 验证仍完整，不放宽为仅检查版本末项 |
| `src/store/pinning/management/native/history.rs` | 历史完整序列映射到对应历史 catalog；新基线与完整旧 m23 可映射到同一最终布局，但证据中的原序列保持原样 |
| `src/store/pinning/legacy_migration/admission_schema.rs`、`admission_schema_sqlite.rs`、`snapshot.rs`、`snapshot_management.rs`、`compat.rs`、`receipt/`、`fence.rs` | 明确区分当前安装身份、历史布局能力和写授权；旧阶段名称的 membership 判断不能直接用于单项基线账本 |
| `src/store/pinning/management/native.rs`、`native/scope.rs`、`native/scan/history.rs`、records/actions | 审核版本绑定、当前授权与历史读取的连接点，保留精确证据校验及 stale 拒绝，不采用全局“版本等价”绕过授权 |
| `src/diagnostics/management/upgrade.rs`、`args/upgrade.rs` | 复用现有 maintenance CLI 参数和输出协议，只更新可支持的 source/target 内容；仍不创建缺失目标、不构造普通 AppState、不执行 provider/Kubo RPC |

新基线建议命名为 `m20261027_000001_schema_baseline`，由唯一常量供 registry、当前 schema 和 CLI 目标校验使用。该名称是新安装身份，不是对历史记录中 m23 的文本替换。未来新迁移从新基线向前追加。

历史识别资料保留完整旧 36 项有序身份及各既有 reader 的允许 endpoint/布局映射，允许复用名称资料，不合并不同 reader 的授权集合。至少覆盖：

- native archived catalog 的 m12 `m20261012_000001_pinning_management`、m13、m17、完整 m23，以及新基线。
- legacy snapshot/receipt 既有 Stage5 RPC ledger、Stage6 replica contract、legacy-pinning-migration、ZIP intent capture 与 m23 endpoint。
- 各历史 fixture 和 reader 仍支持的其他实际边界，逐调用点保持原有语义；上述清单不能作为删除未列出历史 reader 的依据。

## 最终布局

基线以旧 m23 最终安装结果为布局契约，覆盖全部业务和管理对象，而不只覆盖 `native::current_catalog()` 的投影：

- 表、列、类型、nullability、默认值、PK、unique/check/FK 约束及其行为。
- 必需索引、触发器、PostgreSQL 函数、序列/identity/default 关系及所有管理防护对象。
- 初始 identity/scope、readiness、revision/控制行，以及现有启动需要的初始化内容。
- PostgreSQL 时间和 JSON 类型、SQLite 类型/亲和性及约束执行方式。
- `import_jobs` 不含已经搬出的八个 request 列；`import_job_requests` 直接创建为最终布局，相关不可变文档存储/保护直接安装。
- 仍服务当前功能或历史语义的 legacy 表保留，包含 ZIP、pinning、历史 receipts/records 等。

按依赖顺序创建最终对象和必要初始数据。新库不执行历史中间形态的 ADD/DROP、表重建或数据 backfill。业务 seed 与不可变保护触发器的安装顺序须满足最终约束，不以关闭完整性保护掩盖错误。

布局等价允许安装身份、账本行及自然生成的 identity/time 不同；不允许遗漏约束、默认值、索引或触发器。自定义索引是旧库保留项，与新库必需 schema 对比分开。

## 显式转换与恢复契约

### 入口与识别

既有 `UpgradeRequest { native_identity, database_scope, source_version, target_version }` 保持接口形状。转换只针对调用者指定的已存在目标。

锁内验证 source 的完整精确账本、真实源布局、identity/scope 及已有转换证明；不凭末项、行数或“包含某个版本”认可来源。拿锁前读取不能替代拿锁后校验。严格区分：

| 目标状态 | 普通 open | 显式升级 |
| --- | --- | --- |
| 真正空库 | 原子安装新基线 | 不代替初始化入口 |
| 完整且 ready 的新基线 | 资格检查后正常使用 | 只有匹配既有转换证明的原请求可以幂等成功；不能凭当前账本宣称任意历史源已升级 |
| 完整旧 m23 且 ready | 只读 `UpgradeRequired` | 校验最终布局及控制状态后，保数据规范化账本并记录转换绑定，不无故重跑历史 backfill |
| 精确 m13 或 m17 | 只读 `UpgradeRequired` | 仅执行到最终布局所需的适配和现有逐 original 历史转换 |
| 可验证的中断转换 | 只读 `UpgradeRequired` | 仅匹配身份、source、target 和已有证明的请求可以恢复 |
| 未知/部分/混合/伪造账本，损坏布局或绑定不符 | 只读拒绝 | 拒绝，不修猜测状态，不写归一化结果 |

### m23 接入

ready m23 已具备最终物理布局；转换不得重建业务表、重写原始记录、重置 generation 或销毁索引。原子完成必要的活库 ledger 规范化与转换状态/证明发布。失败回滚后保持旧账本和数据可恢复；完成后重复同一请求安全，无新增业务副作用。

旧 m23 可能已包含历史 m13/m17 升级到 m23 的不可变 stamp。该旧 stamp 保持原字节及原 target；新基线转换用独立可验证的绑定表示，不覆盖或伪装原升级已经以新 target 完成。已有有效 sealed archives、locator、complete facts 不为换基线而重建。

### m13/m17 适配

将仍必需的管理存储建立、inline document 安装、import request 数据搬迁提取为明确的源布局适配，不保留完整旧生产 DDL 链。m13/m17 共用最终结构构建片段时，必须区分空库创建与已有数据转换。

保留现有逐 original 的有界历史转换、source/locator 重建和 revision fence。将八个 request 列搬到 `import_job_requests` 时验证数据对应关系、原 SQL 值语义及文档保护；不能用不受控序列化重写历史证据。转换影响的自定义索引必须保留等价有效语义；若依赖被搬走列且无法等价保留，应在破坏性操作前拒绝并报告具体冲突，不能静默 DROP。

原子性边界为：结构准备、必要 ledger/转换标记作为一个事务；每个 historical original 的转换作为一个事务；最终验证和 ready 发布作为一个事务。长历史回填沿用可恢复的多事务协议，不宣称全库 backfill 是单一事务。已提交进度保持可验证，失败事务全部回滚，任何未完成状态都不得发布 ready。

准备后如果活库账本已使用新基线，必须同时保留不可伪造来源的转换绑定及 backfill 状态；不能仅凭基线账本通过普通启动。最终发布前检查 coverage、source/locator seals、pending scope、sealed archives 和 revision vector，防止迁移期间业务变化被漏掉。

### 旧程序中断态与不确定提交

兼容旧程序已准备成 m23 结构、但 readiness 仍为 backfill 的有效 m13/m17 升级：校验原不可变 stamp 的 identity/scope/source/旧 target，按其真实语义恢复必要工作，再接入新基线；新请求的 target 不改写旧 stamp。没有合法原 stamp 的“看起来像中断”状态仍拒绝。

新转换在 DDL、搬迁、逐 original、最终 ready 和 COMMIT 边界中断后，应能通过重新连接、读取确定状态和匹配请求继续。PostgreSQL 不确定 COMMIT 返回不确定/失败结果，不在原连接上盲重试；后续显式恢复根据持久状态区分已完成、未完成和拒绝态。

## 历史证据与运行时资格

当前安装版本校验与历史 catalog 解码是两个接口：前者严格要求新账本及最终结构，后者精确认识已有历史格式。历史版本映射只能决定对应布局及只读能力，不能将任意旧 prefix 当作可执行当前 authority。

检查所有从 `Migrator::migrations()` 推导历史序列，以及将 `schema::VERSION` 用于阶段能力判断的调用点。单项新基线拥有当前完整能力，不能因缺少旧中间名称而遗漏 replica、ZIP capture、legacy receipt 或 management 数据；完整旧 m23 历史也不能因新版 VERSION 改名而被误判为无管理存储或 partial catalog。

历史 archives/records/receipts 经转换后仍可按原 key、引用、catalog 和 metadata 找回、导出、校验；保持 local/foreign、recorded/committed、historical/current、frozen/live 的区分。已有密码学完整性验证继续针对原始字节；不重签、不重新盖章以冒充旧证据。read-only 兼容不得放松当前 apply/checkpoint 的 schema、身份、TTL、revision 或确认绑定。

Store qualification 在建立或重建 Store 时读取新安装资格并按现有规则复用，clone 不重复资格扫描。普通 warm S3/worker 不增加任何用于识别旧迁移的 SQL。显式升级期间不继续使用已取得旧结构资格的 Store；通过既有维护入口操作，完成后新建/重启 Store。

## 实施波次与依赖

### 波次一：最终布局与兼容边界可独立验证

**产出：** 最小历史身份/布局资料、可复用真实旧库 fixture、双后端最终 schema 的行为参照。

- 定向盘点上述生产调用点与相关测试中的 registry、版本等值和 membership 依赖，划分安装校验、历史解码、授权绑定三类。
- 将仍有需求的 m13/m17/m23 及更早只读 endpoint fixture 与生产 registry 解耦。保留必要真实旧 DDL/数据在 test-only 支持层；integration tests 不能假设库的 `cfg(test)` 自动启用，按仓库实际 test support 接入。
- 为最终布局和显式转换建立确定性行为覆盖，优先复用现有 schema/SQL trace 工具。旧布局参照不得通过新基线本身生成，否则无法发现合并遗漏。
- 新基线定义与历史兼容资料在接口明确后可独立开发；正式切换入口依赖二者就绪。

**证据：** fixture 能表示真实源布局和账本、必要历史数据；既有有效历史读取用例保留，最终结构参照覆盖 native catalog 之外的对象。

### 波次二：新库直接安装最终基线

**产出：** 唯一当前基线 registry、按域最终 DDL、两个 backend 的初始化接线和严格当前资格校验。

- 按 FK、触发器和 seed 依赖实现最终布局；适当复用 `schema/storage` 的最终对象定义。
- SQLite 初始化和 PostgreSQL guarded bridge 使用同一基线语义；保持锁内重检、并发初始化及取消清理。
- 更新当前 `VERSION`、read_versions 和 qualification 消费者，保持精确账本校验、bounded reads、shadow/catalog 防护。

**证据：** 两后端 fresh schema 与旧 m23 最终结构等价；运行 fresh SQL 可观察到直接创建最终布局，而不是历史 ADD/DROP/backfill 链；新库首个真实读写成功。旧 m23 普通 open 只读拒绝。

### 波次三：显式保数据接入与历史读取兼容

**产出：** m23 账本接入、m13/m17 精确适配、可恢复状态协议、历史 reader 与新基线能力识别。

- 实现来源判定、锁内重检、身份绑定、必要 DDL/数据转换、账本规范化和完成发布。
- 接入旧 stamp 的完成/中断路径及新转换重复请求；不改变不可变证据格式或字节。
- 将历史 reader 从生产 registry 解耦，完整旧 m23 与新基线按各自身份正确解码；维护升级源与历史读取集合分离。
- 在同一波次处理历史兼容和转换，避免 ledger 切换后暂时交付无法读取档案的产品。

**证据：** 双后端三类来源转换后数据/身份/索引保持；失败和中断保持拒绝或可恢复；历史读、receipt 验证和导出仍真实；新库及升级库重启可用。

### 波次四：移除无需求旧生产链并完成集成证据

**产出：** 生产代码只保留新基线、最小历史识别资料和必要转换适配；测试覆盖最终有效需求。

- 删除无用旧生产 Migration 实现与仅验证中间迁移机制的专属测试；不以另一个生产目录继续装载全部旧链。
- 将仍承载当前功能、安全或历史兼容需求的测试迁移到 baseline、转换或 test-only 旧布局 fixture，不整体删掉历史测试。
- 更新直接依赖旧迁移模块/位置的 fixtures，尤其 `src/store/initialization/document_tests.rs`、`tests/stage75_history_conversion.rs`、`tests/support/stage7_retirement_barrier_schema.rs`、`stage75_recorded_status.rs`、`stage75_recorded_provider.rs`、`stage75_storage17.rs`、`stage75_source_catalog.rs` 及 ZIP 历史 fixtures。
- 集中验证 warm SQL、真实 CLI、重启 S3/worker 和 PostgreSQL 连接生命周期；只清理本任务创建的隔离资源。

**证据：** 生产安装调用图和实际 fresh SQL 证明真正合并；相关历史、安全和业务测试仍覆盖原需求；不添加“旧文件已删除”测试或文档字串锁。

## 验收证据

### Schema 与数据

- SQLite、PostgreSQL 各自 fresh 与旧 m23 参照比较全部相关 catalog 语义，并实际触发 FK/unique/check、immutable guards 和关键默认值。不要将完整 DDL 文本相等当作结构等价。
- m23 ready → baseline，以及 m13/m17 → baseline 均带非空业务/管理/历史数据执行；转换后比较业务值、SQL 类型敏感值、引用关系、identity/scope、自定义索引及可用查询。包含 request 八列搬迁的真实记录。
- 账本与真实结构不符、缺项/额外项/错误顺序、混合新旧身份、伪造来源、错 scope/identity/source/target、安全 shadow 等输入拒绝且无写入；合法新增基线身份不成为任意旧源的幂等成功凭证。

### 事务与恢复

- 在实际准备事务、搬迁/账本改写、单 original、最终 readiness 发布处注入失败，检查事务回滚及已提交进度边界，不仅断言错误返回。
- 重连恢复和重复请求最终只产生合法完成状态；原始证据不变，业务无重复副作用。覆盖旧 m23 backfill + 原 m13/m17→m23 stamp 的恢复。
- 两个维护请求竞争、初始化竞争、revision 变化、PG 锁超时/取消和连接 cleanup 继续安全；不确定 COMMIT 不产生自动重复执行。
- 在旧库普通启动拒绝前后检查 ledger、身份、readiness、锁表及数据未变；只有显式转换可进入写路径。

### 历史与授权

- `stage75_history_conversion`、recorded status/provider、history export/confirmation、retirement barrier 和 ZIP 读取保留有效回归。对原 archives/receipts 做原字节及结构化 metadata 校验，使用既有协议完整性校验，不额外发明验收摘要。
- 旧 endpoint 和完整旧 m23 历史可以在新基线运行时恢复/读取/导出；foreign/opaque 数据不成为 local committed authority，历史 provider capture 不成为当前 sealed inventory。
- 保留 TTL、CAS、epoch/generation、source locator、quota 和独立 complete_fact 的动态正反例。新 catalog 映射不能使陈旧确认或旧 checkpoint 绕过现有执行保护。

### 真实入口与 warm 路径

- 使用既有 maintenance CLI 对隔离库执行真实四参数升级，核对数据库结果而不仅看 `upgraded: true`；错误绑定不能修改库，且不产生 Kubo/provider 请求。
- fresh 和三类升级库关闭连接后重启 Store/AppState，执行代表性 S3 PUT/GET、覆盖/版本、multipart/ZIP、import、tagging、pin submit/poll/completion；同时核对对象内容及 worker 持久状态。
- 复用 `tests/support/optimistic_integrity.rs` 和 `sql_trace.rs`：冷启动后清零 trace，first operation、clone、worker、warm S3 不产生 schema/version/readiness/TEMP 查询。PG 以多连接池验证固定 `search_path`，动态业务校验仍发生。
- 沿用 `tests/optimistic_cluster_integrity.rs` 及相关 qualification/启动测试验证资格复用，不能用减少功能或跳过 worker 得到“零结构查询”。

## 验证执行边界

worker 对本地实质修改运行受影响的模块或定向测试，回报具体行为证据与未覆盖项；父级在接口整合后集中执行必要的全局/较重验证，不要求每小任务全套测试，也不重复已覆盖且输入无关的通过结果。现有有效失败不得跳过或削弱。

宿主命令使用 PowerShell。每次 Cargo 调用设置以下环境，固定工具链并离线锁定依赖；执行前确认目标目录属于当前工作区：

```powershell
$env:TMP = 'C:\Users\hugefiver\source\ipfS3\target'; $env:TEMP = 'C:\Users\hugefiver\source\ipfS3\target'; $env:CARGO_TARGET_DIR = 'C:\Users\hugefiver\source\ipfS3\target'; cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --lib
```

定向集成测试沿用同样环境和工具链，例如 `--test stage75_initialization`、`--test stage75_history_conversion`、`--test stage75_documents`、`--test stage75_management_upgrade_cli`，由执行者选择实际受影响目标。父级集中运行相关 SQLite 集成、PostgreSQL 专项及 warm S3/worker tests；被忽略的 PG 用例必须在授权隔离连接上实际执行，不能将默认跳过计作通过。

需要真实 Linux 验证时可复用已存在的 `rust:1-bookworm`（Rust 1.99）、`rust:1.95-bookworm` 和 `postgres:17`，不 pull、不安装、不下载，使用离线缓存和任务独占 Linux target/temp 路径；Windows 路径约定仅适用于宿主 Cargo。PG 测试由父级新建并明确指定任务隔离库/作用域，不使用生产连接，不假定先前临时库仍存在。清理仅针对本任务创建且身份确认的资源。

工具链、链接器、离线依赖或 harness 在产品执行前失败属于环境/覆盖缺口，不算产品通过或产品缺陷；保留未完成的 backend/真实入口验收，同时继续不依赖该阻塞的工作。

## 主要风险与需要上报的边界

1. **不可变版本绑定：** `UpgradeStamp`、archive catalog、receipt/checkpoint、native schema binding 可能共同依赖旧完整序列。以分离安装身份和历史识别处理，不能全局替换或放宽相等校验。若具体已提交格式无法在保留原 bytes/metadata 和授权语义下继续读取，报告具体类型、调用路径、失败 fixture 和所需决定，停止该依赖分支。
2. **兼容集合混淆：** 单项基线不包含历史能力名称。遗漏 capability 映射会丢字段/表；将历史 reader 集合当升级白名单会扩大权限。测试分别证明两条边界。
3. **DDL 等价不完整：** ORM entity 或 native catalog 不代表全部最终 schema。必须包括 triggers/functions/defaults/indexes 及 SQLite/PG 差异，不能只比字段/FK。
4. **转换与恢复身份：** 重命名 target 不能丢弃旧 backfill 的可恢复证明；已有 ready 基线也不能接受无证明的任意重放。完成发布和账本规范化必须与持久转换状态一致。
5. **索引及数据保护：** m23 不需业务表重建；m13/m17 转换涉及列搬迁。无法保留有效自定义索引语义时必须明确拒绝并上报，不自行牺牲数据/索引保证。
6. **资格与连接回归：** 禁止通过热路径兼容检查获得“看似可用”；PG 初始化不能退回普通池连接执行而损失取消/cleanup 保证。

基线命名、内部文件拆分和等价的 DDL 组织不需要新增用户决策。实际出现上述无法同时满足的不可变证据、公开接口或数据保证冲突时，由协调者获取具体决定；受阻结果保持未完成，不以删除历史兼容或缩减后端覆盖收尾。
