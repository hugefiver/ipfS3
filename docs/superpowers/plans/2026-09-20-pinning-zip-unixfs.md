# Pinning、ZIP 产物与 UnixFS 目录根

本计划定义当前产品要求、验收状态和剩余工作范围。Stage 1 至 7 及 Stage 9 已完成既定验收；management 的大规模数据库可用性由新增 Stage 7.5 解决，其完成后才进入 Stage 8。Stage 8 未完成，不包含生产 GC 或跨进程全局限速上线。

操作与接口见 [README](../../../README.md)、[测试说明](../../testing.md)、[配置示例](../../../config.example.toml)和[本地管理模块](../../../src/pinning/management/README.md)。文档不授予 Git 写入、生产迁移、真实账号写入或付费操作权限。

## 阶段状态

阶段编号是稳定需求标识，不代表执行顺序。

| 阶段 | 交付范围 | 当前状态与提交 |
|---|---|---|
| 1 | Pinata 解析、错误分类、有界恢复与日志脱敏 | 已验收，`2c948dd` |
| 2 | 稳定身份、所有权、容量与历史路由台账 | 已验收，`556c8e4` |
| 3 | 持久可选控制、warn/skipped、只读诊断与策略解释 | 已验收，`ba06d45` |
| 4 | 三入口 ZIP 批次、产物选择、UnixFS 目录根与恢复 | 已验收，`5d5e5c8` |
| 5 | Filebase/Kubo RPC、严格原 CID 复制与完整 CAR | 已验收，`8093cbd`；真实付费账号能力未验 |
| 6 | Cluster Proxy/REST 及真实副本确认 | 已验收，`1515f89`；同宿主测试不证明跨主机 HA |
| 7 | 九类本地管理动作、持久凭据、受限 worker 与退役屏障 | 已验收，`63ca11f` |
| 7.5 | 从需求重设计 management，解除全库规模对单次操作的固定门槛 | 实现与性能重构中，尚未通过完整验收；先于 Stage 8 |
| 8 | 容量、跨进程公平性及未来本地 GC 的有界研究 | 未完成；草稿与失败基准不计验收通过 |
| 9 R26 | 遗留未归域容量的显式迁移 | 已验收，`dc51927`；生产迁移未执行 |
| 9 R25 | ZIP 解压产物自动 pin、共同意图/发布核心与配置分层 | 已验收，`55e06fa` |

最终集中验证：默认 feature lib 1646 passed、0 failed、6 ignored；integration 165 passed、0 failed；fmt 与 strict all-targets Clippy 通过。Stage 7 完整九类动作、十次 apply、三次真实 receipt-scoped worker 执行、同文件数据库关闭重开、完整私密历史恢复、精确配置绑定、过期确认的已提交结果重放、逐点导入、退役和导出均通过。隔离 PostgreSQL 的正式 apply（含 Renew/跨连接 replay）、managed Submit、retirement concurrency、root-release migration 四专项通过。阶段审查阻断已闭合。

这些结果不证明任意规模数据库的 management 可用性、外部付费账号权限或物理空间回收。Schema 升级和普通 S3 冒烟通过，也不等于大库管理动作通过。Ignored、仅编译和重复 support 测试不计独立运行通过；各类真实节点、mock 和数据库证据的范围见[测试说明](../../testing.md)。

## 共同安全与兼容合同

- 本地发布、accepted/skipped/rejected、remote_submitted、remote_pinned、gateway_verified、content_verified、replication_complete 分层保存。源包成功或单个 provider 成功不代表整批、全部副本已完成。
- 未知副作用保持 unknown。超时、解析失败、权限不足、分页不全不证明 absent，也不允许盲目重 POST。明确拒绝停止无效恢复，重试与查询受次数、时间及无进展预算约束；迟到结果仍登记实际资源，不复活过期租约。
- 资源按稳定 backend/scope/CID/resource type 归域；配置别名、URL、secret 或其 hash 不是副本身份。同域共享 CID 只计一次实际资源，不同 owner 保留各自引用。同一 Cluster pinset 的多个入口不是独立副本。
- 默认 cleanup=retain。Managed 清理须有排他管理、确定应用归属、零引用、无在途操作和当前 fence；外部/未知资源不能自动删除。retain、unknown、清理失败与在途责任继续计账，TTL 到期不自动释放容量。
- TTL 从既有意图接受/本地发布起算；远端成功、重试、迁移不重置起点。它是保留请求，不是服务商硬 TTL 保证。
- 不自动执行本地 `pin/rm`、`repo/gc`、`block/rm` 或批量删除。取消引用、逻辑退役、远端 absent 与物理块/账单释放是不同事实。
- 显式选择 cid/upload/car，不自动切换端点、策略或公开性。复制原 CID 对应的实际存储字节；加密对象不能解密后上传 public。目录不能通过 `cat(root)` 冒充完整 DAG。
- 持久 ExtensionDecision 是执行依据。普通 tag 更新、复制、重启或新增 provider 不激活旧 skipped 意图；手动控制整体 skipped 不取消服务器独立自动意图。只有可选控制能 warn，鉴权、基础标签结构、完整性、加密、DB/Kubo 主写及强制保护仍失败。
- 配置与历史 invocation/route/ownership 分开。当前 TOML 不证明历史账号、协议或副作用；缺历史凭据/adapter 时 needs_attention，不借当前默认路由执行旧工作。
- 网络操作不进入长数据库事务。发布、任务、凭据和响应恢复沿用数据库时钟、claim、generation/epoch、原生精度 CAS、ownership 和固定锁序；旧尝试不能覆盖新状态。PG 围栏须覆盖空集/插入 phantom，不能只锁现有行。
- 升级前停止并排空全部旧 writers/workers，备份数据库；不支持混合版本。保留不可变历史、占额及凭据，禁止通过删表、清空旧预留或重写捕获决定绕过拒绝的迁移/回滚。
- 标准 S3 响应、ETag=CID、VersionId 和既有非 ZIP 行为不变。日志与公开诊断不泄露签名、凭据、URL query 或原始 provider 错误；第三方 request dump 不能由 RUST_LOG 重新开启。Doctor 默认零写探测，其独立 CLI 配置不冒充正在运行的网关配置。

## ZIP 批次、自动意图与目录根

### 发布与重放

直接 PUT、MPU Complete 和 durable import 共用捕获意图、最终清单、发布/租约及生命周期核心。v1/v2 只保留协议形态与显式细粒度能力差异。客户端不计算预期摘要，也不理解内部 provider/policy/target 身份；未知、重复、畸形或矛盾控制统一拒绝，完整 SigV4 与桶权限验证不削弱。

发布源包/提取文件与远端目标选择分开。`publish_source=false` 仅允许显式 v2、显式幂等 token 和批次结果，拒绝 `decompress-zip-result=false`。原 source key 的旧对象不被删除或覆盖，不构造虚假 ETag、VersionId 或 archive owner。全部输出 guard 在同一 bucket-first 准入边界核验，不能借 prefix overlap 失效 source 的其他 writer；准入后 guard 丢失令批次失败，不重新抢回 key。

成功清单固定本批实际发布的精确对象版本/CID，而不是当前同 key。direct/MPU 保持 last-success winner，import 保持重复目的地/源包冲突拒绝；路径、CRC、截断、ZIP+SSE 拒绝及资源限额不降低。默认每操作 8 GiB 展开、16 GiB 压缩、10000 entries、64 MiB metadata，另有有界构建/处理预算；这些不是跨请求磁盘承诺。全局拒绝、失败项、空目录项或零目标不创建空租约。

相同 token/UploadId 与相同合同只重放持久结果，不重新发布、建租约或计额；不同合同冲突。MPU 核对 parts，直接上传核对完整原始 bytes。URL/CID 输入首次取回并完整校验后绑定 artifact；绑定后恢复只读该 artifact，不重新取源换内容。绑定前崩溃可能再次获取；绑定后缺失/损坏必须显式失败。提交未知先按相同序列化边界查 durable outcome，不把 open 暂态当回滚证明。

无源且零成功的 v2 Complete 原子保存可查询失败终态，首次和相同 parts 的重放均返回相同 4xx。import 202 只表示受理。批次、manifest、根引用及恢复身份独立于 archive 生命周期。

### R25 自动产物与配置

`auto_pin_outputs` 在三入口和 v1/v2 使用同一语义。开启后仅在现有自动 policy 授权内，为成功发布的精确 output object 建立 Automatic lease；默认不为 archive/root/prefix 建远端意图。无 provider/自动授权时无隐式远端工作；关闭自动开关不关闭明确获准的手动控制。非 ZIP 自动行为不变。

原 `pin=true&content=decompressed` 保持 archive-owned Manual lease，targets 为本批成功输出。自动产物租约则各自 output-owned，删除 archive 不连带取消这些自动租约，删除子对象不改写 archive 的手动归属。v2 显式 source/both 仍须实际发布并获授权；无 source 与 archive-owner 手动意图矛盾时拒绝。版本化覆盖、delete marker、DeleteVersion 按精确 owner 生命周期处理，不用当前版本替代旧版本。

普通 policy 保持 ordered first-match、literal prefix、one 粘性/故障转移和完整 all；ZIP 输出规则与入口允许上限取交集，deny 优先否决，显式空规则/未匹配不授权。配置引用可读 policy，旧不透明引用仅按原值精确兼容解析。旧 Always/ZIP rules 配置须显式选择新自动语义或关闭，不能静默改变外发范围。历史 capture 按原合同解码，不用当前默认值补授权。

### UnixFS 根

默认开启，精确 `ipfs-s3:zip-root=true|false` 覆盖；MPU 初始化、import 接收时持久捕获，后续配置变化不漂移。开关与 remote pin 控制独立。根包含成功最终文件的 ZIP 相对路径，不含 S3 prefix、archive、失败项或额外空目录；保留大小写、Unicode 与嵌套路径。

OFF 零构根调用；零成功无 CID；部分成功返回 partial，完整成功返回 complete。文件/目录同名冲突或构树失败须 failed/warning，不返回漏链接或未经验证的根。启用表示尝试，不是 strict 原子成功承诺：根失败不撤销已成功对象，也不改旧 FailedCount 或诱导整体重复写入。

使用标准 UnixFS Directory/HAMT、规范 CID、固定 codec/hash/链接排序与构建 revision，bottom-up `dag/put` 复用叶 CID，不 cat/re-add 全文件。Tsize 使用 DAG 累积尺寸；支持既有最大条目/名称预算，不通过缩小 ZIP 上限回避大扇出。构建 RPC、元数据、深度、并发、时限和取消有界。终态/trailer、recursive pin、完整 DAG 与路径读取验证及 DB receipt 绑定完成后才发布 root CID。

独立 root owner 绑定 batch/revision/epoch/node/tier/CID，不伪造 object FK。历史根是持久快照，输出覆盖/删除不改树；root-only retry 不重新解压、发布或远传成功文件。未采用候选、中间节点与未知副作用保留可观察责任。根本地 pin 不建立远程租约，也不计独立副本；远传根需显式完整 DAG 能力与授权。

## Provider、遗留迁移与管理

Pinata 列表兼容 missing/null/[]，分页兼容已支持的 snake/camel 字段；非法类型、字段冲突、token 循环或截断必须有界失败，不当 absent。套餐拒绝、权限拒绝、鉴权和未知 403 分开，错误体安全限长；能力级拒绝不误封其他操作。Upload 查询不依赖无关 CID 队列，恢复遵从真实历史 API/strategy，无进展超时不误杀持续传输的大文件。默认只读的 config/policy/doctor/status 提供当前有效配置来源、安全能力与分层状态，不自动写探测；warning/status headers 有界 ASCII，CORS 只在桶规则授权范围 expose，旧 S3 body 不注入新格式。

Filebase 旧 PSA 配置保持兼容，RPC upload 只发送支持的参数，不依赖被拒绝的独立 `pin/add`。Kubo cid/upload/car 严格比较 codec/multihash 与完整 DAG；HTTP 200、Hash 或匹配 CID 不替代 EOF/trailer、recursive pin、内容验证或排他创建证明。CAR 检查单根、缺块、错误根与终态。Filebase CAR 不默认为已验能力。

Cluster 是独立 provider，不替换主 Kubo。Proxy 和原生 REST 使用各自协议，均需 REST verifier；Proxy `dag/import` 透传不算 Cluster CAR 登记。fresh、去重的 allocated peers 满足冻结 min_replicas/max_age 后才参加 one/all 聚合；queued/remote/missing/error/stale 不算副本。min_replicas 不修改 Cluster replication_min/max；分配变化使新观察 degraded/unknown，不抹历史。

### R26 遗留容量迁移

正式 CLI 默认只读 dry-run，不创建库、自动升级、注册 provider、启动 worker 或访问远端。操作员逐类声明真实精确 namespace、账号域和历史路由配对；当前配置不是历史归属证明。完整闭包覆盖 usage/remotes/ledger、所有 lease/target/job、全部历史 epoch/observations、捕获 JSON、冻结副本要求和管理职责；缺证、冲突、超预算或集合漂移整体拒绝。

确认后在完整原生围栏下原子 re-key 可变业务行、逐值转移占额并保存 durable receipt；不可变历史及原 payload 不改写。定点 receipt 只解析其确切历史到 operational 关系，不是全局 alias、账号成功证明或 Stage 7 授权。unknown/retain/在途/cleanup 责任及原 TTL 不变，不为解封清空台账、不新提交或 unpin。旧资源仍可查，新分配只有在所有实际 blocker 正确闭合后恢复。

Apply/replay 与精确 rollback 双向幂等。Rollback 只在原 after-state 未被后续业务改变时允许，追加反向凭据、不删除旧凭据；失败全事务回滚。生产库迁移仍须单独取得实际作用域授权。

### Stage 7 九类管理动作

正式本地 CLI 默认有界只读计划，无管理 HTTP 面。确认绑定完整 request/plan/逐项 ACK、精确对象版本/CID、完整历史 route、原生数据库身份、主体及 DB-clock TTL。导入文件、UUID、applied claims、自洽 snapshot 或 R26 receipt 都不授写权限。新 apply 重读原生围栏并原子提交业务与逐项 receipt；提交结果未知和已过期确认的精确已提交重放只恢复原结果，不重授权限或延长 TTL。

| 动作 | 必须保持的行为 |
|---|---|
| create | 为明确精确版本创建新意图/完整 BirthContract，不重发布内容，不碰撞覆盖旧 slot。 |
| renew | 明确续期现有合法 lease，保留原 created_at/source/owner；不复活取消或退役责任。 |
| cancel | 仅结束选定逻辑意图/引用，不清债、不授 unpin。 |
| reapply | 显式为 skipped 建新 operator intent，原决定、capture 与合同不变。 |
| backfill | 可信 committed manifest 固定成功版本；无 manifest 的候选须逐项确认，不靠 prefix/日志认领。 |
| retry | 只重排可证明未发送/拒绝且未满足要求的工作；已确认内容复用，unknown 先观察。 |
| migrate | 增加明确目的执行，保留源责任、原 TTL 和完整 one/all；逐点绑定真实用于历史读取、dispatch 与 completion，不重写历史 JSON。 |
| reconcile | 按原 route/epoch 只读观察，不隐式转 Submit/Unpin。 |
| retire | 在 canonical domain 建持久屏障并盘点所有责任，默认 retain；别名、新 ID 或重启不能复活分配。 |

`RetireWith` 的迁移、受控 cleanup、精确 root 逻辑引用解除分别确认。解除根引用与 receipt 原子提交，不删除 manifest/verification/history，不改变旧响应，不影响共享引用，不触本地 pin/GC。退役不能通过此 CLI 撤销，阻断 publication/tagging/MPU/import/startup/worker 的新建、续期、reactivate/failover 和 dispatch；已有合法历史观察及迟到事实仍可登记，不能借域关闭抹责任。

显式 evidence preparation 才允许 offline 本地源读取和证明写入。文件量实际 stored ciphertext/payload bytes，目录量完整 offline CAR 验证后根可达去重块的原始序列化 payload bytes（不计 CAR header/CID/framing）；旧无尺寸证明不升级、旧占额不重算、不因数字相同兼容不同 basis。目录尺寸不使 file-only upload 获得资格，也不建立全局块账本。

当前 Stage 7 实现每外层操作共享 ReadSession：解析、索引、scratch、复制、存储 source、predecessor、same-plan 和输出累计限 131072 nodes / 16777216 bytes，内部调用不复位。全 registry 全列 SQL 原始 UTF-8 限 4096 rows / 4194304 bytes；保留 foreign/private/NULL 和完整历史。公开 wire v2 单份脱敏 native review，单文档 16 MiB、131072 nodes、depth 64；authority predecessor depth 32 不限制普通数据叶。原始私密 cells 逐行恢复。全库门槛及其内外部消费方式是 Stage 7.5 的重设计对象，不作为长期业务容量承诺；该任务验收前不通过删历史、漏计或临时涨限规避当前拒绝。

计划/确认文件是私密材料，exclusive create、不覆盖、不建父目录；Unix owner-only，Windows protected owner/SYSTEM ACL 且同句柄核验。`--run` 仅一次 receipt-scoped due-job pass，不清空全站旧队列。运行失败不撤销已提交 apply，也不证明无远端副作用。

## Stage 7.5 — Management 大规模可用性重设计

**当前未验收。**本任务必须先于 Stage 8，交付可运行的实现和验收，不仅是研究建议。当前数据库约有 7.3 万个最新对象版本，已超过 management 全 registry 原像的 4096 行 / 4 MiB 门槛；普通 S3 可用不代表管理动作或其内部依赖可用。业务对象和历史持续增长时，单次管理操作只承担与其作用范围及必要依赖相称的有界工作，不因无关对象增加而失去可用性。

### 交付合同与授权边界

采用一个方案：**精确依赖闭包、事务内完整集合重验、关系化历史定位、不可变分段原像，以及先扫描后短事务发布的全域任务**。不再把全 registry 原像当作每次操作的输入；不把全量读取藏进 ORM、SQL 聚合、缓存预热或新的证据服务。SQLite 和 PostgreSQL 都必须可用。

保持精确版本/CID、主体与物理数据库身份、逐项 ACK、原 TTL/created_at/BirthContract、same-plan 顺序、source/route/epoch、one/all、配额及完整退役责任。未知副作用不重发，readonly 不升级写权限，迟到结果不复活 lease。每个正式 apply 的业务变更和全部 receipt 仍在同一事务提交；扫描页、导入页和准备记录不是已应用前缀。精确 committed replay 先于新业务 TTL/config 检查，不重复分配、计额或延长授权。

**Management 破坏性重构已获授权。**可直接重构 management schema、持久表示、wire/API 和内部接口，不要求旧 management wire v2、API、runtime reader 或兼容桥继续运行，不再设置兼容设计待批准门槛。新合同采用分段原像、选择器/依赖索引、扫描检查点、事务性围栏及显式版本的流式 wire；严格拒绝不支持的旧格式，不把局部原像伪装为旧全库 frame。

破坏兼容不等于丢弃数据或授权事实：全部历史、foreign/private/NULL、原始文本及原审阅必须可恢复；原 TTL/created_at、已提交事实、完整幂等 binding/result 和依赖身份必须保留。必要旧格式解析只属于显式隔离转换/受控升级，不要求进入正常 runtime。转换核验成功后通过新合同恢复原结果，不重新执行、重新计额或延长权限；不能只保留摘要或导出一份无法恢复的 opaque blob。

旧未执行 prepared/confirmed 不自动成为新合同的写权限：保留其原件和原期限供恢复审阅，执行须用新 plan ID 重新规划确认。旧已提交操作保留原幂等身份与结果映射，不能因新 API 或新 ID 再次执行。此规则不要求新 runtime 接受旧确认文件，也不抹除旧历史。

不扩大原动作的业务能力：采用 `MAX_ITEMS=128` 及显式请求的条目/文本安全预算，而非旧 management API 兼容要求；这些限制不用于全域 inventory、数据库总行数或无关历史。`RetireWith::Migrate` 必须覆盖全部 active targets，`ControlledCleanup` 必须覆盖全部符合安全条件的 current resources；不能为了分页把它们变成任选子集。若某域的**实际显式变更/选择集合**超出单次请求预算，必须明确拒绝，不能用多个成功前缀伪装一次原子动作。破坏 management 接口兼容不授权弱化原子性或改变 root 逻辑释放的数据保证。大库验收必须区分 inventory 规模与实际写集规模。

授权范围仅为 Stage 7.5 的 management 重构，不扩展到 S3/ZIP/R26 等非 management 合同；不新增管理 HTTP 面。保留 `ROADMAP.md` 和本计划已有未提交需求。允许先本地提交当前成果，再进行全面性能重构；提交不代表 Stage 7.5 已验收，不授权 push/tag/rebase/release。Stage 8、生产操作、真实付费 provider 写入、实际 unpin/GC、生产数据删除和软件安装均不在本任务授权内。必要转换仅限显式隔离/受控升级，普通启动不得自动升级既有非空库；实际库升级、停写/部署和回退执行分别需要具体作用域授权，不删除升级备份。

### 局部读取、完整依赖与事务围栏

局部操作的规划、确认、执行、历史恢复、status 和普通 consumer 均须贯通局部读取；确需全域盘点的操作遵守下述扫描合同。不能保留全库预检、重复 ORM 快照、JSON membership 全扫或执行末尾的全库预算。完整 catalog 元数据校验保留；语义投影复用 scoped 原始 cells，原审阅从冻结证据恢复。已有 before/after、reservation birth、original Submit、legacy movement 和 same-plan cursor 校验继续有效，单个 job-local 入口不证明整条调用链已经局部化。

Selectors 只能来自受验证 request/receipt 的业务关系，不能把任意 SQL 或 caller 行集当权限。每个 selector 保留完整参数、排序、有序 PK 集及行原文，空集合也须重验。查询使用原生类型索引谓词及复合 keyset，不靠 OFFSET 或索引列 CAST 查找；NULL 与空串分开，PG 时间保留原生精度。完整依赖闭包按原生 PK 去重并逐 row/edge 计预算，至少覆盖：

| 种子/动作 | 必须读取并保留的闭包 |
|---|---|
| 所有 object 动作 | 精确 object/version、bucket 与生命周期/可读性约束、public version 唯一绑定（含 null）、owner 与实际 source 各自身份、ExtensionDecision、精确 source proof/config/node；涉及 ZIP 时读 version→manifest→已提交 batch，不能把同 prefix 当来源。 |
| Create/Reapply/Backfill | 完整新 BirthContract/ordered candidates/one-all 要求、每个候选历史原 route 与 canonical domain、原 skipped 决定、显式 preparation；按全部相关 resource keys 查共享 targets/leases/jobs/current ledger/history/observations、usage 和 retirement。无新 job 时仍登记实际 lease/target/reservation birth，不以 job_ids 为空证明无需 authority。 |
| Renew/Cancel | 选定 target 的 lease/requirements、该 lease 的全部 targets（不止请求中的 target）、所有受影响资源和共享 owner、已有 jobs 与 generation；Renew 保留起点/source，覆盖 Extended/Kept 和旧 generation jobs，不把 Renew 变成新 Submit 授权。 |
| Retry/Reconcile | 选定 resource family 的全部仍有责任的 epochs、original invocation、submit history/observations、source proof、unknown/inflight/cleanup、真实 retryable park 和 receipt。Reconcile 只有观察权限；Retry 不能以局部空结果消除旧副作用。 |
| Migrate | 源/目的两边的完整资源闭包、整个要求集合、lease/CID/slot 对应逐点迁移链、predecessor 及真实 R26 item/reversal；两跳迁移不得退回 current alias。原 route、TTL、源债务与目的执行分别保留。 |
| 共享 resource | 按 canonical backend/scope/resource type/CID 查所有相关 operational provider keys；纳入同 CID 多 owner、无 target 的关联 lease、全部相关 epoch、在途 jobs 和 reservation-birth 记录。不能只按 provider_id 或当前 registry 行判断共享关系。 |
| 可信记录 | 精确 record key、原 plan 的完整成员/ACK/配置/主体/数据库绑定、完整 result facts 和其显式依赖；same-plan 只解析原排序中合法前驱，跨 plan 只沿确切 predecessor/selector 边。不得扫描全部 records 来发现 intents/jobs。 |
| Retire/RetireWith | canonical 域全部 routes/aliases、无 target 的 leases、expired/cancelled、current/history ledger、jobs/observations、unknown/retain/cleanup、ZIP root refs/builds/batches 和缺失归属。全域 inventory 与所选 mutation closure 分开；未知成员保留原行并 needs_attention，不跳过不可投影项。 |

资源闭包只沿语义需要的边扩展；共享 lease 的 requirements 必须完整，但不把与动作无关的全部 owner 对象或整个 provider 的所有 CID 递归吸入。确实很大的相关 fan-out 可以命中单次操作预算；错误必须区分相关闭包超预算与全库规模，不能静默截断后宣称 complete。证明缺失、同 slot 冲突、环路或超过 authority depth 32 均失败；普通叶数据不计为 predecessor 深度。

**新 apply 的串行化。**先独立查询精确 committed receipt；未提交时，在新写事务内按一致锁序取得相关行及依赖表的短时 phantom fence，完整保护空集合和新插入。冲突须释放后重试，不倒序等待或循环持锁。SQLite 在业务读取前取得并保持 caller transaction 的 writer boundary，不能依赖 autocommit 或失败的只读快照升级；PG 保留 schema/retirement/routes 的 SHARE NOWAIT、排序 route 行锁及相关 membership 表屏障。仅获取 writer fence 不得改变源 row、generation、dirty/pending 或 revision，真正行写入才触发对应维护；确认和 apply 不能因无业务变更而永久 Changed。

锁内重新执行 selectors，逐项比较完整 PK membership 和必要原始 cells，核对当前 schema/config、DB identity、fresh time、lease generation、remote epoch、quota、route 和 source。不得用 COUNT、SUM、timestamp、hash 或缓存“曾验证”替代原文/CAS。只锁相关行，不再先锁全 registry 全部行；表级屏障覆盖依赖表而非无条件遍历 registry。执行后只扩展 allocator 实际产生的相关 rows/jobs，再验证完整影响集合和 same-plan cursor；receipt/索引/业务原子写入。最后再检查工作预算、DB-clock TTL、业务 expiry 和 receipt 原文，任一失败整批回滚。网络 IO、全域扫描与文件输出不进入此事务。

### 普通 consumer 的索引与历史读取

新增持久**定位索引**，不新增“只看索引就授信”的捷径。关系至少包含：route/legacy alias→canonical domain 与原 route key；domain→retirement 原件；job/lease/target→精确 action key；lease/CID/slot→migration point；original table/key/provider→R26 operation/item/reversal；exact version→preparation；record→同 plan 成员和确切依赖。反向索引以 selector 为前导列，PK/FK 唯一性覆盖原件坐标，不能用全 records JSON 扫描实现。

**完整性维护合同。**PK/FK 只防错指向，不证明没有遗漏。源和 locator 的增删改（含 PK、归域、原文及 locator 移动）必须由受 schema 验证的事务性维护机制使受影响源 dirty、取消 seal，并留下可索引的 pending。source/locator 各有 generation；pending 不随源或最后一条 locator 级联消失，合法删除保留 tombstone，非法缺原件不能修成安全 absence。已知范围保留旧/新 selectors，未知归属标 global-unknown；不能信 caller 域或可改 locator 来排除 foreign 原件。不可变 applied 原件不得正常删除，绕过维护的 TRUNCATE/DDL/关闭触发器不属于可运行 schema。

源写入、完整 locator 维护与 seal 须在同一 caller transaction 内串行化；显式修复遵守同样的原子性。发布前完整解码原件和必要依赖，逐项核对推导出的全部 selectors 与实际 locator 集，并对最终 source/locator generations 做 CAS 后 seal；只有对应 generation 的 pending 才能清除。数量/hash 或一条许可 locator 不证明完整。源到全部 selectors、selector 到全部候选及 pending 都须有索引。旧源只能在显式升级中完整 backfill；全部原件、catalog、索引和维护机制验证通过且无待分类源才发布 readiness，失败保持 unavailable/pending。readiness 不豁免后续 dirty 检查。

每次 consumer 在一致事务/claim-fence 边界内检查 readiness、global-unknown 和本次范围 pending，然后读取全部候选、sealed generations、原件及依赖求 authority meet。job 覆盖直接 job、lease、Poll 的 original Submit 和逐点迁移，不取第一条或最宽权限；ReadOnly/LedgerOnly 必须参与合并。只有完整且确实为空的集合才能作为 ordinary 无管理授权记录处理，不完整必须拒绝，删去限制 locator 后仍有许可 locator 也不能放行。已知 dirty 只阻断其完整范围，无法归域的本库安全记录全局 fail-closed，不能全 records 扫描补洞。

原始大归档使用页游标，不重新展开全部嵌套 frame；缓存仅复用不可变 bytes/解析结果，有内存回收边界，不缓存 fresh authority。locator 只允许原写入事务或显式维护入口按源完整重建并重新 seal，ordinary consumer 不懒修复。正常原子写入的中间 pending 不对其他事务可见；失败全部回滚，异常提交的未封存源保持可索引阻断。

必须贯通有非空历史的 route/birth 校验、reservation capture/accounting、publication PUT/Copy/ZIP、MPU/import intake/replay、tag pin/renew、startup registration、普通 worker Submit/Poll/recovery、调用权限/域关闭检查、dispatch/completion/迟到观察及 legacy migration resolution。开放域只读自身 routes/retirement locator，已关闭域核验其确切 marker/receipt；新 alias/新 ID/配置 retired=false 不得绕过 canonical closure。managed worker 读取实际 source、完整 BirthContract 与 job/lease lineage；ordinary job 也不能因“非 management”绕过退休/legacy 约束。

### 分段原像、历史转换与审阅恢复

新持久结构按职责分为 archive header、不可变 row/cell chunks、ordered membership、item facts/dependency edges 和 scan checkpoint。header 绑定 scope/physical identity/principal/catalog/request/config/ACK/原期限；row/cell 包含完整 table/PK/column/NULL 标识和原始 UTF-8 bytes，长 cell 分段保存，不把任意 JSON 重新序列化后冒充原文。每个完整原审阅及 private/foreign cells 均可恢复。新 snapshot 只捕获 scope 的当前原像；旧全库审阅中已有的无关/私密数据继续完整保存，不为优化裁剪旧记录。

新 archive 使用独立版本，不重用 S7F1 等旧前缀。业务 action 保存有界根引用，全文保存在同库不可变分段结构；complete/sealed 状态、成员唯一性、严格顺序、长度和校验摘要共同检测缺页、重页、换序、截断及替换。摘要仅提供必要完整性，不替代 DB 原件、全部 ACK、成员原文或授权。新记录引用旧记录时保存确切 key/版本，不把旧 frame 内嵌进每个后继；same-plan 原审阅只存一次，结果按 item 存一次。

显式转换/升级须识别实际存在的 S7F1/S7L1/S7B1/S7I1/S7R1 原件及其 catalog（含 logical root release 前后历史 catalog），逐项验证旧叶 reference/source/predecessor 的坐标、原 bytes、主体与依赖，将完整原审阅和执行事实转换为新分段表示并保留原件。不能只信 locator、长度或摘要；缺依赖、环、跨 scope 授权混用或私密值不匹配均停止转换。正常 runtime 只消费已核验的新表示，不保留旧 reader/全库 capture/泛化兼容 wrapper 的运行要求；历史原文恢复和原结果恢复由转换后的归档与精确映射证明。

新 management wire v3 为有版本头、长度限定 frames 和终态 trailer 的顺序文件：小 request/config/ACK/item 部分有界，native rows/membership/inventory/results 流式分段读写，不先物化整份文件或 Snapshot，文件无任意外部路径引用。旧 v2 文件只可经显式转换入口处理，正常入口拒绝不支持格式，无需兼容 API。严格拒绝未知/重复字段及错误结构，文件中的 applied claims 始终不可信。

普通 review/export 仍只输出脱敏 projection；完整 private archive recovery 通过独立显式私密导出/恢复模式，不向普通计划新增 secret。私密模式写入同样 exclusive-create、owner-only/Windows protected owner+SYSTEM ACL、同句柄核验的文件；不创建父目录、不覆盖。导入先在 task-owned spool 中逐帧验证和去重，再绑定本地原件；文件内 applied/UUID/自洽文档永不授信。私密恢复保留历史，不修改当前业务表、数据库身份或退休状态；换物理数据库只能作归档审阅，不能自动获得原库写权限。没有完整 trailer/成功校验的文件不能进入 confirm/apply。

### 全域扫描、短锁发布与恢复

**一致性机制。**为需要多事务扫描的权威源表增加事务性 mutation revision（schema 固定列举，不包含扫描自身的工作表）。每次 INSERT/UPDATE/DELETE 与相应 revision 更新同事务，覆盖改 PK、同值写、rollback 和空表插入；PG TRUNCATE/DDL 不允许绕过受控迁移和 schema fence。revision 不复用、不回退，溢出 fail-closed。它只证明扫描期间没有源变更，**不是内容证明或写权限**；必须已经读全所需原文、验证 membership 和语义。局部动作不用全表 revision 判漂移，故无关对象更新不会使局部确认失效。

全域 scanner 记录 catalog/DB binding、完整 selector、source revision vector、每表最后复合 PK 和累计预算。每页在短一致读内读前后 revision、按 keyset 读取有界 metadata/原文、计算该页语义并落 spool/checkpoint；只有与起始 vector 相同才接受该页。最终 seal 在短事务拿 schema/相关源表 phantom fence，检查完整 vector、所有表 EOF、页/成员完整性和 reducer 完成，再发布 sealed inventory。无长寿命 RR snapshot、跨请求数据库 cursor 或全程 writer lock。

发现相关 revision 变化时当前 pass 不可 seal；保存失败原因并明确重新扫描，不能把新旧页拼接成一致快照。进程重启可在 vector 不变时从已验证 checkpoint 继续；vector 改变只重开受影响的一致扫描，不恢复旧授权期限。自动重试有次数/时间总预算；持续写入下可以返回 Changed/需重试，不声称无限活跃写入时必定完成。若实测业务负载下长期无法完成，必须向用户提出明确的受控静默窗口/更细作用域围栏需求，不能自动停网关或以持锁全库扫描取代。本机制的 revision 热点和漂移重试率是必须测量的交付风险。

默认 dry-run 仍零业务 DB 写：使用受保护的 task spool 保存扫描原文/游标，命令结束或取消按原语义退出；恢复显式读取已保留 spool，不创建/迁移目标业务库、注册或 probe。显式 confirm 可分段暂存不可执行 archive，最后在相同 vector/原文/配置成立且文件已安全预留时才原子发布确认及原 DB-clock TTL。未 sealed/confirmed 数据永远不能被 ordinary consumer 当作 authority。

确认/执行自身写入不能使全域 vector 永久失效，也不能宽泛排除 management 表。封存时只把已逐字段核验的本 plan self-record keys 作为显式转换：在源表屏障仍持有时，先核对扫描 vector，再提交这些确切自写及其原文，并保存更新后的 vector 与 self-transition；其余 source 行不豁免。任何非预期 self 行、同 plan 新成员或事务外变化均使转换失败。apply 的初始核验使用这一确认后的 vector，末尾只接受执行器记录并核验过的本事务变更；receipt publication 不递归要求重新扫描自身。检查点/chunk 工作表不包含业务 authority，只有原子发布后的根记录才参与正常索引与源围栏。

退役 inventory 扫描必须覆盖全域责任，使用磁盘/DB 上有唯一索引的 reducer 累积 membership、resource 去重、accounted bytes、unknown 和 root ownership，不建立大小随 N 增长的 Rust HashMap/Vec。summary 中同资源跨 epoch 去重、unknown 独立预留、physical_release_bytes=0 等语义不变。默认 Retire 只需原子写 canonical marker、相关 route retired 和 receipt；不是更新每个 lease。`RetireWith` 在完整 inventory 上核对原条件和**完整显式选择集合**，其实际 root CAS/cleanup job/迁移凭据核验按原请求上限进入同一短事务，不分批生效、不放松“全部 active”或“全部 current resource”的条件。最终 apply 若 sealed vector 已过时则不执行，必须重新审阅确认；若存在精确 committed receipt 则走原结果 replay。

Root 解除仅释放明确确认的精确逻辑引用，与 receipt 原子提交；不删除 manifest/verification/history，不改变旧响应、不影响共享引用，不触本地 pin/unpin/GC。迁移、cleanup 和 root 释放仍分别确认，退役不能隐式抹除责任。

archive/review/export/restore 和精确 committed receipt replay 读取不可变 sealed archive，不因当前新增无关历史改变原审阅或已提交结果。导出页可持久记录进度，但恢复不能覆盖已存在的目标文件；校验旧 spool 后向新的 exclusive-create 文件继续输出完整有效文档。导入/恢复的 staged chunks 可重放去重，complete marker 一次发布；取消、磁盘满、响应丢失和崩溃留下的未完成 staging 不可执行。仅可清理本任务创建且准确定位的临时 spool；不自动删除数据库历史或未知 partial 文件。

`PreparedPlanning` 的持久输出支持显式 `PrivateRecovery`，将 projection 绑定到输出进度、continuation 和 wire header，错配须在写入目标前拒绝。私密输出的每轮生产页额度不再作为最终完整文件交付的页额度；最终复制、同步、seek 和完整回读仍共享该轮原 30 秒、字节/行/resident 限额与持久任务总账。默认 Review/CLI 的既有交付页限制不变，typed 原件加载与已确认结果恢复不适用此例外。

**Current status 与冻结收据分离。**status 验证原 committed action 后，按 selectors 读取当前 lease/requirements/target/job、remote/ledger、observations 和 DB clock，重算 completion、replica freshness、TTL、unknown/needs_attention、queued/waiting/read-only；不做 provider IO，不延长授权，不改写冻结 receipt。retirement status 仍须覆盖当前 canonical routes、current/history 债务、在途/cleanup jobs、root 归属/逻辑释放及迁移目的 target/lease/完成证据。局部事实用局部读取，完整域盘点用有界一致扫描；未知归属/root 检查不能省略。跨页漂移/缺证/超预算须说明无法得出完整当前结果，不能用 apply 时 inventory 或旧 sealed status 代替当前状态。worker 完成和时间推进可以改变 status，但原审阅、binding、原期限及 committed replay 不变。

### 资源预算与可观察完成条件

分别约束驻留内存、累计工作量和 deadline：驻留量覆盖同时存活的 driver rows、原文、解析树、索引、缓存和输出 buffers，释放后可归还；工作量累计 SQL/rows/cells/raw bytes、解析/复制/输出 bytes、页、重试和耗时，跨子调用及 checkpoint 不复位，缓存不能绕过失败。不能把旧累计 ReadSession 的固定 16 MiB 直接套到整场 export，也不能只调大旧限额。

当前预算界限为 `MAX_ITEMS=128`、每页最多 256 rows / 1 MiB 原始 payload、超长 cell 每片最多 64 KiB、authority predecessor depth 32。读取前限定页及 cell 大小，不能先由驱动分配完整 blob。单个请求/合同仍受合法结构、文本及 depth 上限约束；巨大 opaque private cell 可原样流式归档，不能直接当合法授权 JSON。多页归档及历史转换按流式合同计量，不继承旧单份 v2 文档总限额，也不截断原文。

峰值预算覆盖整个外层操作及 bounded cache，不只单页；全域 reducer/visited set/文件目录索引落有界磁盘工作区。每次执行/恢复显式限定本轮处理页数、总输入/输出/暂存空间、elapsed deadline 和取消令牌；检查点保留整个任务累计值和总额度。到本轮上限返回可恢复进度而非成功，扩大任务总额度需显式操作，不能内部复位。初始限制在代表性负载中校准并写入使用说明，不以改变 profile/target/线程栈来通过测试。

数据库 page 和 final-write 事务分别有硬 deadline、锁等待/取消处理和服务端 statement timeout（适用后端）；NOWAIT 失败不循环持锁重试。final write 的硬限受原确认剩余 TTL 约束，末尾再次读取数据库时间。SQLite 的 interrupt/rollback 和 PG 的 cancel/rollback 必须实际证明锁已释放，不能只让 Rust future 超时。一个不能在所选事务预算内完成的真正大写集显式拒绝，不得部分 apply；全域只读扫描则可按上述协议继续。

### 受控升级与回退

受控升级须在旧 writers/workers 已排空、备份可恢复且不混跑版本的条件下进行。普通 management CLI 不自动建表或迁移。普通 gateway 启动在任何迁移、migration-lock 写入、provider identity 注册或 worker 启动前，只读分类目标 schema；空库初始化、既有库验证和显式升级须明确隔离，底层入口也不能绕过分类。

- 只有目标应用 schema 确实为空、没有既有应用表/迁移或未完成初始化证据的新库才可在部署原有建库权限内直接建立当前 schema 和 ready 索引；不能以缺少迁移元数据或 readiness 单独判空。并发初始化须在后端迁移锁内重新确认空状态；非空/不认识/partial 库不能冒充新库。
- 已有 Stage 7（及需要跨过 Stage 7.5 边界的其他非空旧库）普通启动返回 upgrade-required，在此之前无 DDL/DML、原件转换或注册身份写入；已经升级且 ready 的库只验证后启动。backfill 中、缺 schema/维护机制、失败升级或未知版本均拒绝启动，不自动续迁移。
- 提供独立显式本地 `--pinning-manage-upgrade` 入口，绑定操作员指定的数据库/原生身份、源版本和目标版本，经明确升级授权后才写入。入口不启动应用、注册 provider、启动 worker 或做 source/provider IO；作用域锁内验证源 schema 和身份。检查点限定同一次升级及原库，普通重启不能隐式续迁移。schema/持久格式批准不等于生产库执行许可。

升级在 unavailable/backfill 状态有界处理既有原件，完整保留原文、catalog、原 plan 成员、私密历史及所有 legacy reversal；维护机制须在 readiness 发布前生效并通过验证。缺证或冲突保留原数据并停止升级，不清空配额/retirement/预留来换取启动。

已提交旧 action/receipt 经显式转换后，必须通过新合同及保留的原幂等身份无需新确认恢复原结果；current status 按当前事实计算而不重授权限。原配置漂移和期限过期不妨碍精确结果恢复，也不授权新执行；旧 prepared 的新执行遵守重新规划确认规则。full history export 必须恢复全部原审阅、private/NULL/原始 cells 和依赖，不要求旧 API/文件继续可直接运行。转换不得修改 S3/ZIP/R26 的非 management 合同或原凭据，恢复不是把 foreign 原像写回当前业务表。

回退边界：尚未发布任何新格式业务/确认时，可在单独授权下从已验证备份恢复旧部署；已经发布新格式后，旧二进制必须因 schema/version 不支持而拒绝启动。不得删新表/删新 receipt 或恢复旧备份后继续接流量来丢弃已发生业务；此时只允许保留新数据的前向修复，或经用户批准、证明不会丢失后续业务的完整恢复。不存在未经验证的自动 down migration。

### 必要验收

交付包括实现及使用说明中的最终行为、预算和恢复限制。以下验收须有实际运行证据；专项按受影响行为运行，不是每次修改或发布前的无条件门禁。相关实现和接口未受后续变化影响的有效证据可复用，无需每小改全套重跑；旧小库、仅编译或 ignored 结果不能替代大库、非空 consumer 或 PG 并发证据。

1. **规模与局部成本。**隔离 SQLite 文件库和 PostgreSQL 都建立至少 100000 当前对象，加 noncurrent/null versions、leases/targets/jobs、epochs/observations、management 原像、retirement、R26 关系；至少比较 1k/10k/100k 无关数据量下相同局部 Create、Renew、Retry、普通 publication/worker。分别记录 SQL 次数和实际扫描/返回行、raw/parse/copy/output bytes、进程/分配高水位、wall time、锁持有/等待及取消时间。数据构造在测量前结束，cold/hot 证据分开。相同相关闭包的返回/解码/复制/驻留量不随无关 N 增长；EXPLAIN/实际 SQL 证据不得存在业务全表 COUNT/SUM、JSON membership scan 或隐藏 full capture。索引树读取开销可以随 N 的对数变化，不能用固定毫秒阈值掩盖全扫。
2. **相关规模与全域。**另增同 CID 多 owner、深历史/多 epoch、无 target lease，测相关闭包增长和安全拒绝；全域退役 inventory、导出/恢复处理超过旧各项门槛的真实原文。记录总工作随相关规模增长、峰值受 resident/page 配额限制、短事务锁时长、checkpoint 继续和 changed-pass 重启。RetireWith retain/migrate/cleanup/root-release 均在大库运行，migrate 完整覆盖 active 集、cleanup 不漏 current resources，越过原显式请求边界的情况准确拒绝而不是成功处理子集。
3. **九动作与真实执行面。**复用并扩展 Stage 7 完整九类动作/十次 apply 工作流，Renew 必须显式执行；至少保留原三次真实 receipt-scoped worker 调用，核对 HTTP 调用及持久完成维度而非成功日志。覆盖零新 job、同 CID 多 owner、同 plan 后继、两跳迁移、one/all、unknown 不重 POST、read-only 不升级、root 精确 CAS。受控 cleanup 只用隔离 HTTP fixture 验证授权/调度，不对真实节点执行删除。
4. **历史与输入。**同文件 DB close/reopen、跨连接 replay、原配置漂移/确认过期后的 committed replay、未提交确认过期拒绝、逐点 R26 导入/反向关系、原审阅及 foreign/private/NULL/原始文本完整恢复。用实际历史原件验证显式转换后原幂等身份、TTL、已提交结果和跨代依赖链完整，重复转换/恢复不重复执行业务；不要求旧 management 文件由新 runtime 直接接受。覆盖错误主体/DB identity、错/缺 predecessor、环、same-plan 错序、伪 applied claims、重复/未知字段、非法 UTF-8、超长 cell、深 JSON、坏长度/校验/终态、磁盘满和取消。恶意文件内容只作数据，不执行其中路径、URL、命令或指令；不能用 hash 相等作为这些授权测试的唯一断言。

   **动态/冻结对照。**同一 committed action 在 worker 完成、lease TTL 到期、replica 观察过期、出现 unknown/cleanup 责任后，current status 分别反映当前事实；退休迁移目的失效、root 未释放/归属未知仍给出相应 needs_attention。每次同时读取原审阅和精确 committed replay，证明 binding/原期限/原 result 未被 status 重写。大库普通 status 不读无关历史，必要 retirement-domain 扫描有完整性和峰值证据；取消/漂移不能退回冻结成功。

   **Locator 安全负查询。**SQLite/PG 均在 readiness 已发布后实际插入/更新/删除源或 locator，覆盖改 PK/域、把 locator 移到别的 selector/source、删最后一条、漏同 plan/Poll 原 Submit 依赖及事务中途失败。特别为同 job 保留许可 locator、删掉限制 locator，验证 pending 命中、调用拒绝、provider 调用数为零，而不是取剩余许可或降级 ordinary；已知脏范围不阻断无关域，无法归域时 global 阻断。显式按源重建并逐项完整核验才可恢复查询。记录 indexed pending/source-key 查询证据，禁止全 records JSON 扫描补洞；完整无匹配集合的真实 ordinary job 仍可运行。
5. **SQLite/PG 并发与故障。**分别实际交错执行 publication/tag renew/worker 与管理 apply/retire；测试首次插入空 route/retirement/target/job/历史集合、更新/删除后重插、在 scan 页间和 seal/apply 间变化、跨连接同 plan 重放、SQL 时间精度及错误锁序。证明 relevant phantom 整体拒绝、已提交的 unrelated 插入不导致局部证据漂移、全域漂移不混合页、自写确认/receipt 不误触永久漂移。短时 NOWAIT contention 可以使当前事务安全退出，但需测量重试/饥饿，不能把它冒充证据变化或全站锁等待成功。注入中段/末尾预算、最后一次 DB-clock TTL、receipt 写入/索引写入失败、提交响应丢失；检查业务/占额/receipt 全回滚或精确 committed recovery，没有成功前缀。证明 timeout/cancel 后数据库锁释放且未发生未授权 provider IO。

   **SQLite writer fence。**使用两个真实文件库连接：A 在 caller transaction 执行零匹配 UPDATE 后，B 的竞争写在 A commit/rollback 前不能成功；结束后 B 可继续。覆盖空 route 表、现有 identity/route、确认与 apply/replay；核对仅取 fence 不改变任何源 row/revision/pending，真正业务写才推动 revision，确认后 apply 不会永久 Changed。autocommit fence 不得通过保锁断言，取消/rollback 必须释放锁。

   **升级入口与启动。**SQLite/PG 分别用已有 Stage 7 含真实凭据的库通过正式 gateway/AppState 启动，证明 upgrade-required 且 schema/data/identity/registration 无写入；特别覆盖 SQLite migration-lock 表也未创建/改写。未显式选择受控升级、错误身份/源版本、途中失败/backfill 未 ready 均拒绝启动；正确显式升级后才可注册/启动。另跑真正空库初始化及并发首次初始化，证明可正常建当前 schema；部分应用表、缺 migration 元数据、未完成初始化不被当空库。检查转换后旧 receipt 的原结果可经新合同恢复且原幂等身份不丢，全部私密原文/原审阅可恢复，旧未执行确认仅拒绝新执行而未丢失；S3/ZIP/R26 非 management 行为与凭据不变。
6. **Pixivbot 实际请求形状。**只在本项目测试，不修改 Pixivbot：签名 v1 direct PUT 的 query 为 `decompress-zip=<prefix>/`，MPU 在 Create 时捕获同 query，UploadPart/Complete 依原协议继续；tag 为 `ipfs-s3%3Apin=true&ipfs-s3%3Aduration=1d&ipfs-s3%3Acontent=decompressed`。不加 v2 或 expected-sha256 专用头，正常 SigV4 headers 仍保留。验证 XML `Entry.ETag` 是各输出 CID、解压 bytes/精确版本、archive-owned Manual targets 与开启 `auto_pin_outputs` 时逐 output-owned Automatic lease、真实 worker HTTP calls、持久分层完成及 replay 无重复。覆盖 root ON/OFF、UnixFS root/DAG 和目录路径读取，普通读写/MPU/Copy/版本行为不退化。关闭目录构建的 HTTP fixture 不能单独证明 UnixFS 根，mock 不冒充付费账号能力。
7. **集中回归。**覆盖受影响的 wire、snapshot、九动作/worker/retirement/root、legacy、ZIP/RPC，文档中四个 PG 专项及上述规模/并发验收；同一证据覆盖多个要求时不重复运行。完整 lib、integration 必须使用默认并发，fmt 和 strict all-targets Clippy 仍是 Stage 7.5 的有效验收：

   ```powershell
   cargo test --locked --offline --lib
   cargo test --locked --offline --test integration
   cargo fmt --all -- --check
   cargo clippy --locked --offline --all-targets -- -D warnings
   ```

PostgreSQL 验收使用任务专属隔离实例和独占 `stage7_` schema，入口及夹具要求见 [docs/testing.md](../../testing.md#stage-75-management-scale-acceptance)。不触现有实例/生产库，不删除数据库或卷；只停止本任务实例并说明保留资源。

**完成判据：**局部操作及普通 consumer 不承担无关全库工作，全域操作有界扫描/恢复且保留全部责任、原文与幂等事实，九动作/十 apply（含 Renew 和三次真实 receipt-scoped worker）、Pixivbot v1 及上述必要验收均有有效通过证据，才可标记 Stage 7.5 已验收。持续写入下扫描可用性、revision 写热点、历史转换完整性和真正超大原子写集仍须如实说明。当前未验收，不进入 Stage 8，不开展生产操作。

## Stage 8 待完成研究与验收条件

本阶段只交付有界测量、建议和明确安全条件，不实施生产 GC、shared limiter、remote-only、允许不同 CID 的字节备份或成功后重起 TTL。

1. **容量测量。**固定源码及输入，对 65 项与 10k 项扇出、重复/唯一叶、多 provider、ordinary/recovery 混合负载建立 baseline。分别测操作前后、运行高水位、重启恢复后的 repo bytes、文件系统分配量、RSS/peak、暂存/缓冲、独立 pin 数及逻辑占额。S3 logical_size、账本 hold、单 DAG unique-block bytes、实际磁盘和服务商费用不能互换；每操作 ZIP 预算不是全局容量承诺。
2. **跨进程调度。**对单 gateway baseline 与两个独立 OS gateway 进程、共享 DB/同 backend-scope 做相同工作量对照。由受控 endpoint 同一时钟记录每种 Submit/poll/find/recovery/cleanup 的真实 HTTP starts、并发/active bytes、固定及滑动窗口 RPS、429/Retry-After、前台延迟、队列最老等待、逐进程进展、claim/heartbeat 和占额。覆盖共享 CID、丢响应、迟到成功、进程死亡/重启；实例内 permit 不证明全局预算。是否建设 shared limiter 及触发阈值须来自账号限制、部署 SLO 与测量，不能虚构生产 RPS。
3. **未完成基准。**跨进程 Submit 收敛问题尚未闭合，完整生产负载的单进程 baseline 和故障对照仍待验证。现有基准工具不代表 Stage 8 验收通过，不通过加时限或弱化完成条件取得通过。单进程负载与 10k HAMT 证据不证明跨进程公平、repo 增量或可回收空间。
4. **全 owner 盘点。**未来 collector 须覆盖全部 data versions/null/noncurrent、delete marker 后的历史内容、hot/cold/lifecycle 在途、MPU parts/替换/Complete、import artifacts、ZIP input/source=false、manifest/历史根引用、所有 build revision/epoch/candidate、中间 flat/HAMT 节点、未提交或迟到 writer、remote leases/jobs/全部历史债务，以及 MFS/其他应用/外部未知 owner。物理身份必须含 node/repo/tier，远端含 backend/scope/resource type；无 CID 的未知写入按受影响节点/操作窗口保守阻断，不伪造 absent。
5. **未来 GC 安全条件。**默认生产物理 GC NO-GO。只读 dry-run 须有完整 owner 快照、schema/节点身份、跨 DAG 可达性与保留理由，区分保护/候选/unknown，不把候选 DAG 大小相加为回收量。任何身份缺失、漏 owner、未知中间 pin 或快照漂移均停止。全部 gateway/import/MPU/root/lifecycle 及外部 writer 必须共享 node/repo 屏障并排空；执行前重验零 owner/在途、排他归属与当前 fence。DB reference 不替代 Kubo pin。双后端迁移须完整目标验证、DB residency 提交和旧 reader/writer 退出。破坏性边界的丢响应、进程死亡、迟到写、失 fence、重复执行及重启先恢复保护均须隔离实测；DB 回滚不能恢复已删块。
6. **外部能力。**Filebase CAR、较大文件、账号鉴权、删除释放与费用变化仍未验，需独立账号/费用/作用域授权。Mock DELETE、anonymous Kubo auth headers、逻辑释放或单 DAG bytes 不证明服务商权限、账单下降或物理回收。

上述研究和 GC 故障恢复实验尚未验收。生产迁移、外部付费账号操作、本地 unpin/GC 均未执行；测试通过不替代生产部署与外部服务能力验证。
