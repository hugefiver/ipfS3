# Stage 7 management：未注册的纯数据叶模块

唯一范围来源是 `docs/superpowers/plans/2026-09-20-pinning-zip-unixfs.md`
§3.1、§3.2、Stage 7。这里仅提供 owned DTO、有限验证、snapshot → dry-run、
完整确认绑定及确认后的精确重验。**不是已经落地的管理 CLI / 执行器，也不表示 Stage 7 已完成。**

没有数据库、provider、文件、时钟、随机数、环境变量或网络 IO；使用标准库及已有
`serde` / `serde_json` 依赖。
没有 URL、token、secret、secret_ref 或 secret digest 字段，没有新 hash 收据。
未注册到 `src/pinning/mod.rs`，未修改 Cargo、共享源码或外部文档；独立
`tests/management_plan_wire.rs` 用 `#[path]` 编译此叶和原有 39 个安全测试。

## 父集成接口

`mod.rs` 重导出数据类型和下列接口：

```rust
pub fn validate_request(request: &PlanRequest) -> Result<(), SafeError>;
pub fn validate_snapshot(snapshot: &Snapshot) -> Result<(), SafeError>;
pub fn dry_run(request: &PlanRequest, snapshot: &Snapshot)
    -> Result<DryRunPlan, SafeError>;
pub fn revalidate(plan: &DryRunPlan, confirmation: &Confirmation, current: &Snapshot)
    -> Result<Vec<CheckedItem>, SafeError>;

impl DryRunPlan {
    pub fn plan_id(&self) -> &str;
    pub fn items(&self) -> &[PlanEntry];
    pub fn summary(&self) -> &Summary;
    pub fn confirmation(&self, candidate_item_ids: Vec<String>) -> Confirmation;
}

impl IdempotencyId { pub fn as_str(&self) -> &str; }
impl RouteIdentity { pub fn remote_key(&self, cid: &str) -> RemoteKey; }

// management::wire；均无文件 IO
pub fn encode_plan(request: &PlanRequest, snapshot: &Snapshot,
    plan: &DryRunPlan, confirmation: Option<&Confirmation>)
    -> Result<Vec<u8>, SafeError>;
pub fn decode_plan(input: &[u8]) -> Result<ImportedPlan, SafeError>;
impl ImportedPlan {
    pub fn request(&self) -> &PlanRequest;
    pub fn snapshot_without_applied(&self) -> &Snapshot;
    pub fn untrusted_applied_claims(&self) -> &[AppliedItem];
    pub fn plan(&self) -> &DryRunPlan;
    pub fn confirmation(&self) -> Option<&Confirmation>;
}
```

集成流程：

1. 管理进程鉴权/授权后，用一致读构造有界 `PlanRequest`、`Snapshot`；默认只 dry-run。
   这里不扫描 prefix、不分析旧日志、不重放 raw tags、不发现 ZIP 归属。
2. 用 `wire::encode_plan` 保存原始输入及完整不可变计划；父层负责持久文件/DB IO。
   `decode_plan` 只从原始冻结 snapshot 重建并逐字段比较完整计划（包括 summary），
   **不能从新 current-key 重建获批清单**。导入结果的 applied 已隔离为未可信声明。
3. 展示固定范围、精确版本/CID、logical target/占额、unknown/inflight/cleanup、
   `Blocked` 原因及每条 `RemoteDirective`。任何 `Candidate` 均须独立逐项明确确认。
4. 操作者确认后调用 `plan.confirmation(已独立确认的 candidate item IDs)`，保存
   完整 `Confirmation.bindings`，以及父层负责的 principal、权限范围、时间和确认记录。
   仅 plan ID 或 item ID **不是**确认依据；替换同 ID 的计划会得到 `ConfirmationMismatch`。
5. 重新读取当前权威 snapshot，调用 `revalidate`。任一未应用行发生变化即整次安全停止，
   不返回一部分可执行结果；缺对象/缺版本/不可读/安全禁止/TTL 到期也不换同 key 新版本。
6. `Checked(step)` 仍只是**纯数据预检结果**，不是写权限或网络执行凭证。未来执行器须
   在真实事务/claim/CAS 中再次校验相同对象、lease/target fence、remote epoch、历史路由、
   inventory/domain frontier、配额和安全决策，再登记意图/任务；网络不放长事务内。
   preflight 与写之间的 TOCTOU 并未由这个叶模块解决。
7. 每条逻辑动作和完整原始 `PlanEntry` 的 applied receipt 原子提交。重放可信 receipt
   必须匹配完整 binding；返回 `AlreadyApplied` 时禁止再次登记工作、计额或发网络请求。
   不得把任意调用方提供的 `AppliedItem`、UUID 或 ID 存在性当作执行完成证明。

## 精确身份及适配要求

| 叶类型/字段 | 父层真实证据来源/责任 |
|---|---|
| `ExactObject` | 固定 bucket/key、显式 `PublicVersionId::Null` 或真实公开 ID、`object_versions.id`、immutable `objects.id`、原 CID；删除标记不是文件 |
| `ObjectSnapshot` | 对精确版本的可读性、逻辑字节数、持久 ExtensionDecision/control/config revision；不得再扫描历史 raw tag 来激活 skipped |
| `allowed_routes` | 对该对象实际存储字节、确切 route revision 的当前安全决策；包括迁移目的地；加密对象不得解密后外传 public |
| `SelectionEvidence` | `TrustedManifest` 必须来自已提交批次实际成功条目，并核对 batch/revision/path/version/CID；无可信 manifest 只能 `LegacyZipCandidate` |
| `LeaseSnapshot` | `pin_leases.id/owner_object_id/generation`、原意图起点/到期和状态；时间均为数据库时钟的秒，不从远端成功重起 TTL |
| `TargetSnapshot` | `pin_lease_targets.id`、lease、CID/logical_size、持久要求及真实执行 fence；`all` 的完整 provider 集不能缩为可用子集 |
| `RouteIdentity` | 历史 `ProviderRouteSnapshot` 的稳定 provider/backend/scope/storage_domain、API/profile/strategy、资源类型和管理员 credential/endpoint revision；不复制 URL/secret_ref |
| `RemoteSnapshot` | current remote epoch、真实副作用 certainty、opaque resource id、归属、引用、占额及独立确认维度；超时/不完整查询不是 NotSubmitted/Rejected |
| `historical_remotes` | `pin_resource_history` 的每个仍有责任的历史 epoch，包括 retain、unknown、在途和 cleanup；不能只传最新 `remote_pins` 行 |
| `LedgerDomainSnapshot` | backend/scope/resource type 范围内的**完整本地 ledger**及真实一致性 frontier；complete 不是远端不存在的查询证明 |
| `ProviderInventory` | 含无 target 的 lease、expired/cancelled lease、全部历史资源、root 逻辑引用的权威完整退休盘点；不能用部分分页宣称 complete |
| `Snapshot.applied` | 父执行器按完整绑定原子记录的可信 durable outcome，不是任意客户端自报结果 |

当前实体没有独立 target generation，也没有本叶的 domain/inventory 聚合 revision。
适配层不得编造自增值、随机版本或伪迁移：target 可绑定真实 lease generation，**前提是**
其所有目标变更确实受同一 fence/CAS 保护并重验目标全字段；聚合 frontier 必须由真实
事务/ownership 屏障提供。无法证明完整性和当前 fence 时不能把本叶接成执行器。

CID 在这里仅做非空/ASCII/长度约束及精确比较；**没有声称验证 CID 格式或 DAG 等价**。
父适配层须用项目已有 CID 解析器先规范化；同 DAG 的不同合法文本不能伪装成不同资源。
路径/key 不做 Unicode、大小写或 prefix 改写。公开 null version 永不表示“查当前 key”。
同 key 的新对象不会自动加入旧计划；原精确版本仍可读时只处理它，原版本失效则停止。

历史 current ledger 与 history 对同一 `(RemoteKey, epoch)` 不能重复/冲突。
不同 epoch 的同一已知 opaque resource 可以保留多条观察，但费用投影按真实 resource id 去重；
未识别资源的不同 epoch 仍保留独立未知预留。

## 动作语义和安全保障

- `Create(Intent)` / `Backfill(Intent)` / `Reapply(Intent)`：显式建立新意图，要求精确对象、
  明确 route、完整要求及固定起点/期限。历史 skipped 只能显式 `Reapply`；本叶不解释 raw tags。
- `Renew { expires_at }`：仅 active 且未到期的既有 lease，明确延长；保存原起点，零远端写承诺。
- `Cancel`：仅取消本应用逻辑引用的规划；不减 remote 占额，不宣称 provider 已取消，永不 unpin。
- `Retry`：定向既有 target，不把 archive CID 换成 entries。当前已确认且观察新鲜的所需维度
  只 `ReuseConfirmed`；已有但证据不足只 `ObserveExisting`；不会重复上传已确认内容。
- `Reconcile`：只规划按实际历史 route/epoch 查询，绝不 POST；历史路由不可用即 blocked。
- `Migrate { destination }`：明确目的地 identity/revision，CID/lease 起点及期限/要求不变，
  源资源与其 cleanup/retain 责任保留；不声称 migration complete，不隐式降低 `all`。
  同 storage_domain/资源域不能冒充迁移到独立副本。源 unknown/inflight/cleanup 先对账/等待，
  此时目的地 `NoWrite`。本叶不自动切换 strategy、公开性、端点或旧成功资源管理协议。
- `Retire`：默认 retain，仅规划停止新分配并盘点所有 current/history 责任。缺旧凭据/adapter
  用 `needs_attention` 表示；不删配置、lease、ledger、root 引用，不丢 retain/unknown/inflight/cleanup。
  后续迁移/managed cleanup 是另一项明确授权工作；本叶**没有 managed 删除指令**。

`RemoteDirective::SubmitWithFence` 只是 future scheduling 候选：只有没有未解决历史副作用，
且 current 记录明确未发送/被拒绝，或完整本地 ledger 中尚无工作，才可能出现它。
Unknown、inflight、cleanup、旧已知资源分别先对账/等待/观察。查询失败/无权限/分页不全
不能用于抹除 unknown；模型也没有把普通“查不到”编码为 authoritative absence 的快捷状态。

确认维度使用集合：accepted、remote_submitted、remote_pinned、gateway_verified、
content_verified、replication_complete **互不凭 enum 顺序推导**；replica count 另核对。
本叶不会把某个 target 可复用提升为所有 providers / 所有 ZIP entries 已完成。

幂等 ID 为 `management-v1:<非秘密 plan label>:<非秘密 item label>`，不含 CID/object key，
labels 最长 64 字节、仅小写 ASCII/数字/`-`/`_`。稳定字符串用于命名和查重，完整 action、
manifest、捕获状态才用于绑定比较。父层必须拒绝同 ID 不同 binding，不能靠 UUID 碰撞概率
替代该检查；重放成功也不是 provider ExactlyOnce 的实现声明。

所有错误为静态 `SafeError`，不回显输入、provider body、URL 或凭据。
含调用方字符串的数据类型没有 `Debug`/日志出口。wire 所需模型具有严格 serde 实现，
并非安全日志格式或自动授权的展示接口；父层须保护保存文件、仅展示获授权字段，
不能把 secret/token 填入 IDs 或 labels。直接对模型调用 serde 不具有 wire 入口的预算保证。

## Wire v1 边界

- 顶层固定字段：`schema_version: 1`、`applied_trust: "untrusted_claims_only"`、
  `request`、完整 `snapshot`、`plan { plan_id, items, summary }`、`confirmation`。
  confirmation 必须显式为完整绑定或 null；枚举使用 snake_case 外部标签，null version
  表示 `"null"` 枚举而非 JSON null 或查当前对象的选择器。没有 hash-only 收据。
- 输入先限制 `MAX_WIRE_BYTES = 16 MiB`，再用不构造 Value 树的 serde visitor 预遍历；
  `MAX_WIRE_NODES = 131072` 计每个 JSON 值/容器及字段键，`MAX_WIRE_DEPTH = 64`
  （根深度 0）。随后才创建 DTO，应用 request/snapshot/重建计划既有 records/text 预算。
  输出 writer 同样受字节预算约束，并检查结构预算；wire 的总体预算可比单份模型预算更严。
  `decode_plan` 接受已持有的字节切片；父层读取文件/DB blob 时也须在分配前限制长度，
  不能先无界 read-to-end 再把内存交给此入口。
- 所有 wire struct/带字段枚举拒绝未知字段，重复 JSON 字段、非法枚举、错误类型、非 UTF-8、
  尾随文档均安全拒绝。格式错误只返回 `InvalidInput`，预算错误为 `LimitExceeded`；
  不支持的版本为 `UnsupportedSchemaVersion`，重建计划不等为 `IdempotencyConflict`，
  同 ID 的完整 confirmation 替换为 `ConfirmationMismatch`。不向调用方返回 serde 错误文本。
- `snapshot.applied` 原样保存在文件中，**只用于重建当时的 dry-run 并核对文件内部一致性**。
  导入时移出到 `untrusted_applied_claims()`，`snapshot_without_applied()` 永远不含 receipt；
  导入不返回 `AlreadyApplied` 或写权限。父层不得把这些声明填回可信 current snapshot。
  保存/重建的 `summary.already_applied` 同样只是原始数据中的声明，不是经认证的执行统计。
- 无确认的计划、尚未逐项确认的 candidate 和 blocked 计划均可保存作审计；导入不会自动确认、
  解锁或执行它们，后续 `revalidate` 保持原先的全有或全无安全语义。
- 一致性比较不是认证：攻击者若同步替换 request、snapshot、plan 和 confirmation，叶模块
  没有密码学签名或外部可信原件来识别它。父层必须认证保存来源，绑定 principal/权限/确认记录，
  拒绝可信存储中同幂等 ID 的不同完整 binding；不能把自洽的客户端文档冒充已授权执行计划。

## 有界规划及费用展示

- 一次最多 `MAX_ITEMS = 128` 项；大范围运维由父层分页并保存明确页清单，不隐式续扫 current-key。
- request/snapshot/output plan 分别执行结构遍历预算 `MAX_RECORDS = 4096` 和累计文本预算
  `MAX_TEXT_BYTES = 4 MiB`（嵌套引用也计入预算）；最多 64 个 provider requirement。
- key/manifest 路径最长 1024 字节；CID 512；普通身份 256；数字相加溢出安全停止。
- `logical_targets` / `logical_intent_bytes` 是选定范围的逻辑规模（迁移含源+目的地），
  **不是执行的费用增量**；重放仍可展示原范围。
- `unique_cids`、`unique_resources`、`possible_new_transfers` 不混为一谈：共享 CID 不重复估算
  同域传输，已应用项不再估算新传输；候选传输仍待确认和真实 CAS。
- `accounted_remote_bytes` 保留 current/history 的逻辑占额；重放不隐藏 unknown/retain 占额，
  缺失 ledger 行也不会被当成已释放。已知同资源跨 epoch 取最大占额而非重复求和。
- `physical_release_bytes` 始终为 **0**。没有金额、套餐价格、实际磁盘回收或服务商退款承诺。
  logical 引用取消、TTL 到期、root 逻辑退役均不等于释放实际块空间。

## 单独验证及未验证边界

本模块未注册，因此仓库 `cargo test --lib` **不会发现这里的测试**；不能将其退出码当此叶验收。
独立 target 使用已缓存依赖，离线编译叶及原有 39 个用例；不运行全 lib 测试或容器：

```powershell
rustfmt --edition 2024 --check "src/pinning/management/mod.rs" "tests/management_plan_wire.rs"
cargo test --offline --locked --test management_plan_wire
cargo clippy --offline --locked --test management_plan_wire --no-deps -- -D warnings
```

内部测试覆盖九类动作、逐字段身份/fence/safety 变化、完整确认绑定、candidate 独立确认、
receipt 冲突与重放、历史 unknown/inflight/已知资源、容量去重、退役遗留责任、有限规模和静态错误。
独立 target 额外覆盖九动作 wire round-trip、完整原始 snapshot、plan/confirmation 替换、
对象/版本篡改、未可信 applied 隔离、字节/结构/深度/记录/文本预算及错误格式/静态错误。
这不是生产模块注册、数据库并发、CLI 权限、持久 IO、实际 provider 副作用、PG、
真实账号或生产运维验收。上述父层适配器、配置/鉴权、持久计划/receipt IO、claim/CAS、执行器、
管理 CLI 与所有外部验收仍未实现/未验证。
