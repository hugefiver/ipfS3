# 本地 pinning 管理

本模块提供本地管理规划、确认、执行和归档接口，没有管理 HTTP 入口。
默认 dry-run 只读现有 SQLite/PostgreSQL，不创建业务库、迁移、注册身份或启动 worker，
也不做 source/provider IO。Stage 7.5 的完整验收尚未完成；旧 Stage 7 小库结果不能证明
当前大库、历史转换、wire v3 或并发合同已通过。

## 作用范围与事务

局部动作读取精确选择器的完整依赖闭包，不把全 registry 原像作为输入。闭包包含精确
object/version、bucket、实际 source 与 owner、共享 lease/target/resource、全部相关 epoch、
jobs/observations、原 route、preparation、可信历史记录及其依赖。ZIP 归属只来自精确
version→manifest→已提交 batch，不能从 prefix、raw tags 或旧日志猜测。

membership 包括完整有序主键集和原始 cells，空集合也须核验。store 在短写事务内重新执行
选择器，比较 membership 和原文，核对 schema/config、物理 DB identity、DB clock、
generation/epoch、配额及 source。锁/CAS/phantom fence 不能用 COUNT、摘要或缓存授权代替。
确认过的同一计划按原顺序处理合法前驱；不能漏掉同 plan 成员或任意跨 plan 接续。

每次正式 apply 的全部业务变化和完整 receipt 同事务提交，任何末尾预算、TTL 或原文核验
失败均整批回滚。页面、staging、准备记录不是成功前缀。网络 IO 不进入写事务。
精确 committed replay 先于新业务的 TTL/config 检查，恢复原结果，不重新分配、计额、
延长授权或发送请求。只有完整 binding 相同才能返回 `AlreadyApplied`；UUID、幂等 ID
存在或文件自报 applied 都不够。

普通 consumer 通过关系定位索引读取完整原件，核验 source/locator seal 和
本 scope/global-unknown pending。定位索引不是权限证据；缺证或 dirty 必须 fail-closed，
不能扫描全部 records 补洞、取剩余许可项，或当成普通无 management 权限的 job。
ReadOnly/LedgerOnly 等限制参与 authority meet，readonly 永不升级为远端写权限。

## 全域扫描与预算

退役 inventory 与局部 mutation closure 分开。全域工作用复合 keyset 分页、source revision
vector、检查点和有界磁盘 reducer，读完全部责任后才可 seal。每页使用短一致读，最终短事务
核验 vector、EOF、membership 和 reducer；不持有覆盖整个扫描的 writer lock 或长寿命快照。
revision 只用于检测漂移，不证明内容或授权。页间漂移使该 pass 失效，不能拼接新旧页面。
持续写入可能要求重扫；过时的 sealed inventory 不能用于新 apply，须重新审阅确认。

局部预算针对相关闭包，不受无关对象/历史总量支配。旧全 registry 的 4096 rows / 4 MiB
不是数据库容量合同，也不限制全域 inventory 的总行数。真正相关的 fan-out 仍可能超预算，
此时明确失败，不能截断后标 complete。显式请求最多 `MAX_ITEMS=128` 项；请求/语义模型
仍受 records、文本、depth 和 provider requirements 的安全预算约束。

分页边界为最多 256 rows / 1 MiB 原始 payload，长 cell 最多按 64 KiB 切片读取。
resident 预算追踪同时存活的内存，释放页后可归还；work 累计 SQL、rows/cells、原文、复制、
输出、页及重试成本，子调用和恢复不能重置累计额度。authority predecessor depth 最多 32，
普通数据叶不计为 predecessor 深度。

当前 CLI 流式文件预算为 8 GiB 工作字节、1000 万 rows、32 MiB accounted resident 和
每次操作 120 秒。dry-run/confirm 使用 120 秒工作窗口，其他普通 DB-backed 管理操作
保留 30 秒工作窗口；取消后最多等待 5 秒 owned-work drain。已有 checkpoint 保留原来的
累计上限和已花费工作量，新默认值不扩大旧任务额度。这些数值不等于已经证明的业务容量。
CLI 不提供累计任务预算调整。含 Provider 全域规划的 dry-run/confirm 可用互斥的
`--checkpoint NEWDIR` / `--resume EXISTINGDIR` 显式保留、跨进程继续受保护的工作区；
`--scan-pages 1..512` 只限制本轮页数，默认 64，不重置累计工作额度。恢复须匹配原请求、
主体、配置与物理数据库身份，并重验源。`outcome=incomplete` 不返回可执行计划，也不预留
最终输出。dry-run 只写本地工作区；显式 confirm 可以逐页提交不可执行的 archive，核验
live 原文与 canonical 结果。报告的 phase 与数据库 effects 包括这些已提交暂存页，不能将
暂存当作 authority 或已应用业务前缀。源漂移要求新 pass，不能混合旧页。Ready 后才按原
ACK 规则原子发布确认，并在首次发布时建立 DB-clock TTL，不从规划开始计时。
输出或确认失败保留 checkpoint；确认已提交但输出失败时，只读恢复原确认，不重发确认或
改变原 ACK/期限，并保留工作区。普通成功完成才清理本任务工作区。输出始终使用新
exclusive-create 文件，不覆盖或追加原 partial 文件，也不因恢复而延长已有授权。

恢复导出必须保留完整内容和确认语义，但不要求 JSON 编码或物理 chunk 划分逐字节相同。
每份文件独立验证完整性和 EOF；持久保存的原始归档字节及其完整性引用仍须精确保持。

`RetireWith::Migrate` 必须覆盖全部 active targets，`ControlledCleanup` 必须覆盖全部符合
安全条件的 current resources。实际显式选择/写集超单次请求预算时整次拒绝，不能为了分页
将原子动作改成任选子集或多个成功前缀。默认 Retire 不逐个改写所有 lease。

## 精确身份与九类动作

对象绑定 bucket/key、显式 public version（包括 null，绝不是查当前 key）、private version ID、
immutable object ID 和 canonical CID。源对象与 owner 不一定相同，须分别核验。
Provider 绑定稳定 backend/scope/storage_domain、历史 API/profile/strategy、资源类型及
credential/endpoint revision，不只绑定当前 alias。CID 的简单文本约束不证明 DAG 等价，
适配层须先用项目解析器规范化。key/path 不做 Unicode、大小写或 prefix 改写。

| 动作 | 安全语义 |
|---|---|
| Create | 显式建立新意图，绑定精确对象、route、完整 BirthContract/ordered candidates 和原起点/期限。 |
| Renew | 仅延长 active 且未到期的既有 lease，保留原起点/source；不是新 Submit 授权。 |
| Cancel | 取消本应用逻辑引用，不删 S3 内容、不释放 remote 占额、不 unpin。 |
| Reapply | 显式请求新意图，不自动重放历史 skipped 或重新解释 raw tags。 |
| Backfill | 选择精确历史内容；没有可信 manifest 的 legacy ZIP candidate 须独立 ACK。 |
| Retry | 只重试符合条件的定向工作；已确认且新鲜的维度复用，证据不足则观察，不盲目重复上传。 |
| Reconcile | 只按实际历史 route/epoch 观察，绝不授予 POST/Unpin 权限；旧 route 不可用则 blocked。 |
| Migrate | 绑定明确目的地，保留 CID、原 lease 起点/TTL、完整要求及源 cleanup/retain 责任；同资源域不冒充独立副本。 |
| Retire | 关闭 canonical 分配域，默认 retain，完整保留无 target/expired/cancelled lease、历史资源与未决责任。 |

`RetireWith` 是退役的显式 disposition，不是绕过九动作安全边界的通用删除入口。
cleanup、已证迁移和 root 引用解除各自绑定完整证据，不能凭 CID 或 provider 名推导所有权。
alias、新 ID 或配置 `retired=false` 不能绕过 canonical closure。

Unknown、inflight、cleanup 先对账/等待，源未决时迁移目的地 `NoWrite`。只有完整本地 ledger
和实际 fence 证明没有未解决副作用，且明确未发送/已拒绝或无已有工作时，才可调度 Submit。
查询失败、无权限、不完整分页或普通“查不到”不能抹除 unknown；unknown 不重发。

`one` 保留原候选顺序与决策，`all` 保留完整 provider 要求，不能缩成可用子集。
accepted、remote_submitted、remote_pinned、gateway_verified、content_verified、
replication_complete 分别核验，不能按枚举顺序互推；replica 数另核对。
同资源跨 epoch 的占额按真实 resource 去重，未知 epoch 保留独立未知预留。
`physical_release_bytes` 始终为 0，logical targets/bytes 不是收费增量或退款承诺。

幂等 ID 仍为 `management-v1:<plan label>:<item label>`，不是 wire 版本号。
labels 最多 64 字节，只允许小写 ASCII、数字、`-`、`_`，不得放 CID、key 或 secret。
完整 action、manifest、原审阅与依赖用于 binding 比较，字符串命名不能代替它们。

## Wire v3、原文归档与恢复

正常文件入口使用 `wire::v3::Reader/Writer` 的顺序流。格式头为 `IPFSMGMT\0\x03`，版本 3；
长度限定 frames 最多 1 MiB，cell bytes 最多每片 64 KiB，终态 trailer 核验完整性。
native、membership、inventory、results 分段，不含可执行的外部路径引用。
完整短 cell 可合并为一个物理记录，长 cell 保持分片流式读取；逻辑事件和原文语义相同。
Review 私密 cell 使用统一的流式表示，不按私密值长度选择紧凑编码。
未知/重复字段、错误结构、非法 UTF-8、坏长度/终态及不支持版本均拒绝。
没有完整验证的 staged 文件不得进入 confirm/apply。摘要只用于完整性，不是授权。
生产分页或重开可改变物理分帧，但不能改变原文、顺序或 NULL 语义；同一持久原件的重复恢复
保持字节一致。

普通 review/export 只输出脱敏 projection。SQL NULL、空文本和被隐藏私密值分别表示，
不能互换。新持久归档保留完整历史原审阅、private/foreign/NULL cells、原始 UTF-8 文本、
原 TTL/created_at、BirthContract、全部依赖和 committed binding/result。
旧全库审阅中原有无关或私密数据也不能裁剪。原文恢复不是重新序列化 JSON，不从当前
对象重建旧审阅，也不把 foreign 原像写回当前业务表。

私密恢复须显式选择 PrivateRecovery，完整保留已有私密原件；脱敏文件无法补出丢失的 secret。
`PreparedPlanning` 持久输出绑定所选 projection，恢复时不得切换。显式 PrivateRecovery
的页额度约束每轮生产推进；最终交付仍受同轮 30 秒、字节、行、resident 和持久任务总账
约束，不再扣生产页额度。写入前核对 projection，随后完整复制、同步、回读并验证 EOF。
默认 Review／CLI 的交付页限制不变；typed 原件加载和已确认结果恢复不适用此例外。
恢复保留历史而不修改当前业务表、DB identity 或退休状态。换物理 DB 只能作归档审阅，
不能获得原库写权限。导入 applied/result 声明始终不可信，自洽文件也不是原库授权。

### 实际 CLI

以下 PowerShell 命令假设已配置指定现有库、审阅 JSON 请求，且 `./private` 是受保护的现有
目录。Candidate 的每个实际 item ID 均须分别 `--ack-item`；没有 candidate 时省略该参数。
确认 TTL 默认 300 秒，可选 1 到 900 秒。`--source-node PEER` 仅断言预期节点，不是 probe。

```powershell
$env:IPFS_S3_CONFIG = "gateway.toml"
ipfs-s3-gateway --pinning-manage --help
ipfs-s3-gateway --pinning-manage --prepare ./prepare.json
ipfs-s3-gateway --pinning-manage ./request.json --output ./private/plan.v3
ipfs-s3-gateway --pinning-manage --review ./private/plan.v3
ipfs-s3-gateway --pinning-manage ./request.json --confirm ./private/confirmed.v3 --ack-item candidate-item-id --ttl-seconds 300
ipfs-s3-gateway --pinning-manage --apply ./private/confirmed.v3
ipfs-s3-gateway --pinning-manage --status ./private/confirmed.v3
ipfs-s3-gateway --pinning-manage --apply ./private/confirmed.v3 --run
ipfs-s3-gateway --pinning-manage --export ./private/confirmed.v3 --output ./private/review-copy.v3
ipfs-s3-gateway --pinning-manage --import ./private/review-copy.v3 --output ./private/validated-copy.v3
```

`--prepare` 是独立的显式 source 读取和 DB evidence 写入，无 provider IO。JSON 字段为
`record_id`、`identity`（ExactObject）、`expected_node`、`max_stored_bytes`、
`ttl_seconds`（1 到 3600）、`directory`。文件计 stored bytes，包括密文；目录证明完整可达
DAG/HAMT，按唯一 block 原始 payload 计字节，不含 CAR framing，不代表物理/provider 账单。
目录证明不授予 file-only upload 权限，也不从远端成功时刻重起 TTL。

`--confirm` 绑定逐项 ACK、principal `local_configuration`、DB scope、物理 DB identity、
配置和本地原件。文件先 exclusive-create，DB staging/确认随后发生。失败可能留下空或
不完整文件，甚至 DB 确认已提交；先检查保留状态，不覆盖文件重试。
所有新文件不自动建父目录、不覆盖。Unix owner-only；Windows `CREATE_NEW`，创建时设置
owner+SYSTEM protected ACL，并在同句柄核验。错误不回显路径、DB URL、SQL 或凭据。

`--review/--export/--import` 不加载配置、不打开业务 DB、不做 RPC。import 仅验证并冻结
未可信 artifact，不恢复 DB confirmation/receipt。下面只适用于已有的完整 private v3 文件：

```powershell
ipfs-s3-gateway --pinning-manage --review ./private/history.v3 --private-input
ipfs-s3-gateway --pinning-manage --export ./private/history.v3 --output ./private/redacted.v3 --private-input
ipfs-s3-gateway --pinning-manage --export ./private/history.v3 --output ./private/recovery.v3 --private-recovery
```

`--private-input` 接受私密输入但仍输出脱敏 review；`--private-recovery` 保留私密原文。
两者互斥，也适用于 `--import`。旧 Stage 7 v2 JSON 通过独立的 file-only 入口转换：

```powershell
ipfs-s3-gateway --pinning-manage --convert-v2 ./old-plan.json --output ./private/converted.v3 --private-recovery
```

转换保留原文和未可信 claims，不加载配置、不打开业务 DB、不做 RPC。输出是 archive-only
v3，不含可执行 request、ACK 或确认期限；正常文件入口仍拒绝 v2，转换文件不能直接 apply。
存量 DB 历史转换仍通过显式升级进行，文件转换不替代依赖图与本地原件校验。

当前没有直接从 DB 导出完整 private history 的 CLI 命令。公开
`wire::v3::history::Export` API 通过 `start/advance/suspend/resume/write_private` 分页导出
完整 records、retirement 原件与 archive segments（包括未引用 segments），未完成 pass
遇源变化拒绝封存，sealed 导出保持冻结。恢复不重置累计工作预算或原 deadline。
`history::restore_private` 返回只读归档，支持逐 cell、原审阅和 archive part 恢复，不接受
目标业务 DB，也不写回业务表、receipt、DB identity 或退休状态。

`--status` 核验 committed action 后，从当前 DB facts/clock 计算 completion、TTL、replica
freshness、unknown/needs_attention 和 queued/waiting/read-only，不做 provider IO，不延长
权限或改写冻结 receipt。退休状态还核对当前 routes/债务、cleanup、root 归属和迁移目的完成。
转换后的 committed 历史也按当前责任计算 status；原审阅与 committed replay 保持冻结结果。
缺证、取消、漂移或超预算不能退回冻结成功。`--run` 只做一次经核验 receipt-scoped due-job
pass，不全局清队列；claim cap、due time 和 readonly 限制仍有效。中断不撤销 committed apply，
也不证明远端从未发生副作用。

## 显式旧库升级与启动门槛

普通 gateway startup 先只读分类，再决定初始化真正空库或验证 current ready 库。
已有旧库、partial、backfill、未知版本或维护机制不完整时拒绝启动，要求显式升级；
在此之前不迁移、不写 migration-lock、不注册身份或启动 worker。缺 migration metadata
不是空库证明，普通 management CLI 也不自动建表或迁移。

SQLite 与 PostgreSQL 均在 Store 生命周期内完成一次完整 schema 资格校验，覆盖 native、
management、receipt、route 的表、约束、trigger、index、完整版本序列、readiness 与
TEMP shadow。启动复用已成功完成且清理完毕的校验；直接从可信池构造的 Store 在首次
runtime 操作时校验。clone 共享资格，新 Store 独立校验，失败或取消不发布成功资格。
warm S3/worker 不再查询结构、版本、readiness 或 TEMP。权限、原始绑定、retirement、
quota、claim、epoch、generation、TTL、ACK、source/locator seal 与完整 membership 仍
实时核验。裸 DB 管理、历史读取、维护、管理文件导入继续完整校验。RPC complete_fact
独立持久化已捕获事实，PG 永久 relation 的 OID/schema 绑定仍保留，不加 runtime 准入门禁。

连接池须保证所有连接（含重建的连接）使用同一固定 database/schema/search_path；PG
自定义池须逐连接初始化，不能把某次 checkout 的 SET 当作全池保证。应用池统一使用
URL startup options 与固定的数据库/角色默认设置。生命周期内不得注入 TEMP 或改 path。
结构、迁移版本、readiness、TEMP 的外部在线修改不在逐操作检测保证内。readiness 的
install、upgrade_existing、backfill 写入均属维护，不是业务实时开关。维护前必须停止
全部相关服务、worker、管理进程并排空操作，释放旧 Store、clone 和连接池；显式校验或
升级后重新启动。维护失败或完成后均不能继续复用旧 Store。

独立升级入口不加载 gateway config、不构造 AppState、不注册 provider、不启动 worker，
不做 source/provider RPC。五个参数均必填，identity/scope 必须是授权原库的真实绑定。
当前支持源版本 `m20261013_000001_zip_root_logical_release`，目标版本
`m20261017_000001_management_storage`。CLI 未提供 identity discovery 命令。
只有对确切数据库另获升级授权、停止并排空旧 writers/workers、备份且验证可恢复后才可运行：

```powershell
ipfs-s3-gateway --pinning-manage-upgrade `
  --database-url $upgradeDatabaseUrl `
  --native-identity $upgradeNativeIdentity `
  --database-scope $upgradeDatabaseScope `
  --source-version m20261013_000001_zip_root_logical_release `
  --target-version m20261017_000001_management_storage
```

先将三个变量设为核验过的本地值。参数形状不是生产维护授权。升级失败/backfill 未 ready
时仍拒绝普通启动，只可显式绑定同次升级与原库继续，不自动续迁移，不混跑二进制版本。
支持的旧存储原件经受控转换保留完整原审阅及私密历史、原 TTL、依赖、原幂等身份与结果。
旧未执行 prepared/confirmed 仅保留供审阅，执行需新 plan ID 重新规划确认；旧 committed
记录转换后恢复原结果，不重执行业务或计额。新格式已发布后无自动 down migration，
不得删新表/receipt 或用旧备份接流量丢弃后续业务。

## 精确 root 引用解除

`RetireWith` 可显式选择 `release_root_references { references: [...] }`。
每个 reference 是 `NativeKey`：`table="zip_root_references"`，`values` 全为字符串，按
`batch_id`, `revision`, `epoch`, `node_identity`, `tier`, `cid` 主键顺序排列。
这不是 prefix 选择器，不能手写 UUID 冒充授权。目录 root 不是 S3 对象版本，不能伪造
VersionId/ETag，也不表示九动作都直接支持 directory root。

确认绑定 DB 派生 batch owner、完整 reference/build/batch tuples 及私有 verification receipt。
新解除须 fresh 确认与精确 CAS，引用变化和 receipt 原子提交。
`logical_released_at=NULL` 表示逻辑活跃，非 NULL 为逻辑解除；原 retained/adopted state、
verification、manifest、root/batch 历史、旧 ZIP/MPU response 及共享 CID 的其他引用均保留。
默认 Retire/Retain 不解除 root，不承诺 unpin、GC、物理空间回收或远端占额释放。

## 非 management 协议与验收边界

Management wire v3 绝不是 ZIP v2。Pixivbot 仍用签名 ZIP v1 direct PUT：
`decompress-zip=<prefix>/`；MPU 在 Create 捕获该 query，UploadPart/Complete 按原协议继续。
tag 保持 `ipfs-s3%3Apin=true&ipfs-s3%3Aduration=1d&ipfs-s3%3Acontent=decompressed`，
不要求 v2 或 expected-sha256 专用头，正常 SigV4 headers 保留。S3/ZIP/R26 合同不因
management 文件或持久格式升级而改变。

完整验收范围见 [Stage 7.5 检查](../../../docs/testing.md#stage-75-management-scale-acceptance)。
命令示例、局部 PASS、旧 Stage 7 证据及仅编译均不证明 Stage 7.5 已完成。
本地/mock 结果不证明真实 provider 账号权限、物理 pin 或存储回收。本模块不授权生产操作，
也不进入 Stage 8。
