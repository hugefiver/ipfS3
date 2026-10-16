# Pinning、ZIP 产物与 UnixFS 目录根

本计划定义当前产品要求、验收状态和剩余研究范围。Stage 1 至 7 及 Stage 9 已验收；Stage 8 未完成，不包含生产 GC 或跨进程全局限速上线。

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
| 8 | 容量、跨进程公平性及未来本地 GC 的有界研究 | 未完成；草稿与失败基准不计验收通过 |
| 9 R26 | 遗留未归域容量的显式迁移 | 已验收，`dc51927`；生产迁移未执行 |
| 9 R25 | ZIP 解压产物自动 pin、共同意图/发布核心与配置分层 | 已验收，`55e06fa` |

最终集中验证：默认 feature lib 1646 passed、0 failed、6 ignored；integration 165 passed、0 failed；fmt 与 strict all-targets Clippy 通过。Stage 7 完整九类动作、十次 apply、三次真实 receipt-scoped worker 执行、同文件数据库关闭重开、完整私密历史恢复、精确配置绑定、过期确认的已提交结果重放、逐点导入、退役和导出均通过。隔离 PostgreSQL 的正式 apply（含 Renew/跨连接 replay）、managed Submit、retirement concurrency、root-release migration 四专项通过。阶段审查阻断已闭合。

这些结果不证明生产迁移、外部付费账号权限或物理空间回收。Ignored、仅编译和重复 support 测试不计独立运行通过；各类真实节点、mock 和数据库证据的范围见[测试说明](../../testing.md)。

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

每外层操作共享 ReadSession：解析、索引、scratch、复制、存储 source、predecessor、same-plan 和输出累计限 131072 nodes / 16777216 bytes，内部调用不复位。全 registry 全列 SQL 原始 UTF-8 限 4096 rows / 4194304 bytes；保留 foreign/private/NULL 和完整历史。公开 wire v2 单份脱敏 native review，单文档 16 MiB、131072 nodes、depth 64；authority predecessor depth 32 不限制普通数据叶。原始私密 cells 逐行恢复，不能靠删除历史或增加预算过关。

计划/确认文件是私密材料，exclusive create、不覆盖、不建父目录；Unix owner-only，Windows protected owner/SYSTEM ACL 且同句柄核验。`--run` 仅一次 receipt-scoped due-job pass，不清空全站旧队列。运行失败不撤销已提交 apply，也不证明无远端副作用。

## Stage 8 待完成研究与验收条件

本阶段只交付有界测量、建议和明确安全条件，不实施生产 GC、shared limiter、remote-only、允许不同 CID 的字节备份或成功后重起 TTL。

1. **容量测量。**固定源码及输入，对 65 项与 10k 项扇出、重复/唯一叶、多 provider、ordinary/recovery 混合负载建立 baseline。分别测操作前后、运行高水位、重启恢复后的 repo bytes、文件系统分配量、RSS/peak、暂存/缓冲、独立 pin 数及逻辑占额。S3 logical_size、账本 hold、单 DAG unique-block bytes、实际磁盘和服务商费用不能互换；每操作 ZIP 预算不是全局容量承诺。
2. **跨进程调度。**对单 gateway baseline 与两个独立 OS gateway 进程、共享 DB/同 backend-scope 做相同工作量对照。由受控 endpoint 同一时钟记录每种 Submit/poll/find/recovery/cleanup 的真实 HTTP starts、并发/active bytes、固定及滑动窗口 RPS、429/Retry-After、前台延迟、队列最老等待、逐进程进展、claim/heartbeat 和占额。覆盖共享 CID、丢响应、迟到成功、进程死亡/重启；实例内 permit 不证明全局预算。是否建设 shared limiter 及触发阈值须来自账号限制、部署 SLO 与测量，不能虚构生产 RPS。
3. **未完成基准。**跨进程 Submit 收敛问题尚未闭合，完整生产负载的单进程 baseline 和故障对照仍待验证。现有基准工具不代表 Stage 8 验收通过，不通过加时限或弱化完成条件取得通过。单进程负载与 10k HAMT 证据不证明跨进程公平、repo 增量或可回收空间。
4. **全 owner 盘点。**未来 collector 须覆盖全部 data versions/null/noncurrent、delete marker 后的历史内容、hot/cold/lifecycle 在途、MPU parts/替换/Complete、import artifacts、ZIP input/source=false、manifest/历史根引用、所有 build revision/epoch/candidate、中间 flat/HAMT 节点、未提交或迟到 writer、remote leases/jobs/全部历史债务，以及 MFS/其他应用/外部未知 owner。物理身份必须含 node/repo/tier，远端含 backend/scope/resource type；无 CID 的未知写入按受影响节点/操作窗口保守阻断，不伪造 absent。
5. **未来 GC 安全条件。**默认生产物理 GC NO-GO。只读 dry-run 须有完整 owner 快照、schema/节点身份、跨 DAG 可达性与保留理由，区分保护/候选/unknown，不把候选 DAG 大小相加为回收量。任何身份缺失、漏 owner、未知中间 pin 或快照漂移均停止。全部 gateway/import/MPU/root/lifecycle 及外部 writer 必须共享 node/repo 屏障并排空；执行前重验零 owner/在途、排他归属与当前 fence。DB reference 不替代 Kubo pin。双后端迁移须完整目标验证、DB residency 提交和旧 reader/writer 退出。破坏性边界的丢响应、进程死亡、迟到写、失 fence、重复执行及重启先恢复保护均须隔离实测；DB 回滚不能恢复已删块。
6. **外部能力。**Filebase CAR、较大文件、账号鉴权、删除释放与费用变化仍未验，需独立账号/费用/作用域授权。Mock DELETE、anonymous Kubo auth headers、逻辑释放或单 DAG bytes 不证明服务商权限、账单下降或物理回收。

上述研究和 GC 故障恢复实验尚未验收。生产迁移、外部付费账号操作、本地 unpin/GC 均未执行；测试通过不替代生产部署与外部服务能力验证。
