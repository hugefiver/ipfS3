# Stage 7 本地 pinning 管理

管理模块已注册并接入本地 CLI，没有管理 HTTP 入口。默认只生成有界、只读计划，
不启动网关或后台 worker，不自动迁移 schema。现有数据库和本地配置访问权限必须受保护。

责任边界如下：

- plan/wire：借用 snapshot/原始行规划、逐项 ACK、精确重验和有界编解码，无 IO。
- native collect/evidence：从数据库采集完整原生 tuple；显式 preparation 读取本地源并持久化
  有期限的证据，不把 provider 查询或调用方声明当成本地内容证明。
- store transactions：重验原生状态、权限及 fence，原子提交动作和完整绑定的 durable receipt。
- CLI/scoped worker：保护计划文件，处理显式确认及 apply；`--run` 只对已核验 receipt
  进行一次 due-job pass，不全局清空队列，也不隐式获得额外 provider 权限。

本地运行验收已通过，覆盖九类动作、十次 apply、三次 receipt-scoped worker 执行、
root CAS、同文件数据库关闭重开、完整私密历史恢复、原确认过期后的精确重放和导出，
以及隔离 PostgreSQL 的正式 apply、managed Submit、退役并发和 root-release 专项。

## 规划与 wire 接口

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
    pub fn confirmation(&self) -> Option<&[String]>;
    pub fn session(&self) -> records::SharedDecodeBudget;
}

pub fn encode_review(session: &mut records::ReadSession, review: &records::ReviewHandle,
    confirmation: Option<&[String]>, output: &mut impl std::io::Write)
    -> Result<(), SafeError>;
pub fn decode_review(input: &[u8], session: records::SharedDecodeBudget)
    -> Result<ImportedPlan, SafeError>;
```

使用流程：

1. 管理进程鉴权/授权后，用一致读构造有界 `PlanRequest`、`Snapshot`；默认只 dry-run。
   这里不扫描 prefix、不分析旧日志、不重放 raw tags、不发现 ZIP 归属。
2. 用 `wire::encode_plan` 保存原始输入及完整不可变计划；CLI/store 负责持久文件/DB IO。
   `decode_plan` 只按原始冻结 snapshot 和 migration points 校验完整计划（包括 summary），
   **不能从新 current-key 重建获批清单**。导入结果的 applied 已隔离为未可信声明。
3. 展示固定范围、精确版本/CID、logical target/占额、unknown/inflight/cleanup、
   `Blocked` 原因及每条 `RemoteDirective`。任何 `Candidate` 均须独立逐项明确确认。
4. `plan.confirmation(已独立确认的 candidate item IDs)` 仅生成 ACK 数据，不产生权限。
   Store 持久化并验证 exact review、原 migration points、ACK、principal、物理数据库身份与期限，
   返回共享原始行的 `ConfirmedPlan`。导入时必须与这些本地原件逐字段匹配，不能从文件授信。
5. 重新读取当前权威 snapshot，调用 `revalidate`。任一未应用行发生变化即整次安全停止，
   不返回一部分可执行结果；缺对象/缺版本/不可读/安全禁止/TTL 到期也不换同 key 新版本。
6. `Checked(step)` 仍只是**纯数据预检结果**，不是写权限或网络执行凭证。store 执行事务须
   在真实事务/claim/CAS 中再次校验相同对象、lease/target fence、remote epoch、历史路由、
   inventory/domain frontier、配额和安全决策，再登记意图/任务；网络不放长事务内。
   单独通过纯数据 preflight 不能替代写事务中的重验。
7. 每条逻辑动作和完整原始 `PlanEntry` 的 applied receipt 原子提交。重放可信 receipt
   必须匹配完整 binding；返回 `AlreadyApplied` 时禁止再次登记工作、计额或发网络请求。
   不得把任意调用方提供的 `AppliedItem`、UUID 或 ID 存在性当作执行完成证明。

独立入口创建一个 `ReadSession`；复合 import → load → apply/status/scoped run 使用
`ImportedPlan::session()`，不在子调用复位预算。`ReviewHandle`/`ArchivedRows` 保留完整私密原像，
通过 `original_cell` 和逐行访问恢复，导出只借用它们作一次脱敏 projection，不重建完整 frame。
累计预算为 131072 nodes / 16777216 bytes；SQL 全 registry 限 4096 rows / 4194304 原始 UTF-8 bytes，
计入全部列、NULL 的保留语义及 foreign/private rows，而非仅三个 payload 列。解析、scratch、索引、
复制和序列化均先支付；数据复用不缓存授权结论，source/member/fence 与 authority depth 32 仍须重验。

## 精确身份及适配要求

| 类型/字段 | 权威证据来源/责任 |
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
| `Snapshot.applied` | store 按完整绑定原子记录的可信 durable outcome，不是任意客户端自报结果 |

适配层不得编造自增值、随机版本或伪迁移：target 可绑定真实 lease generation，**前提是**
其所有目标变更确实受同一 fence/CAS 保护并重验目标全字段；聚合 frontier 必须由真实
事务/ownership 屏障提供。无法证明完整性和当前 fence 时不能执行动作。

CID 在这里仅做非空/ASCII/长度约束及精确比较；**没有声称验证 CID 格式或 DAG 等价**。
集成适配层须用项目已有 CID 解析器先规范化；同 DAG 的不同合法文本不能伪装成不同资源。
路径/key 不做 Unicode、大小写或 prefix 改写。公开 null version 永不表示“查当前 key”。
同 key 的新对象不会自动加入旧计划；原精确版本仍可读时只处理它，原版本失效则停止。

历史 current ledger 与 history 对同一 `(RemoteKey, epoch)` 不能重复/冲突。
不同 epoch 的同一已知 opaque resource 可以保留多条观察，但费用投影按真实 resource id 去重；
未识别资源的不同 epoch 仍保留独立未知预留。

## 动作语义和安全保障

- `Create(Intent)` / `Backfill(Intent)` / `Reapply(Intent)`：显式建立新意图，要求精确对象、
   明确 route、完整要求及固定起点/期限。历史 skipped 只能显式 `Reapply`；规划层不解释 raw tags。
- `Renew { expires_at }`：仅 active 且未到期的既有 lease，明确延长；保存原起点，零远端写承诺。
- `Cancel`：仅取消本应用逻辑引用的规划；不减 remote 占额，不宣称 provider 已取消，永不 unpin。
- `Retry`：定向既有 target，不把 archive CID 换成 entries。当前已确认且观察新鲜的所需维度
  只 `ReuseConfirmed`；已有但证据不足只 `ObserveExisting`；不会重复上传已确认内容。
- `Reconcile`：只规划按实际历史 route/epoch 查询，绝不 POST；历史路由不可用即 blocked。
- `Migrate { destination }`：明确目的地 identity/revision，CID/lease 起点及期限/要求不变，
  源资源与其 cleanup/retain 责任保留；不声称 migration complete，不隐式降低 `all`。
  同 storage_domain/资源域不能冒充迁移到独立副本。源 unknown/inflight/cleanup 先对账/等待，
   此时目的地 `NoWrite`。管理动作不自动切换 strategy、公开性、端点或旧成功资源管理协议。
- `Retire`：默认 retain，停止新分配并盘点所有 current/history 责任。缺旧凭据/adapter
   用 `needs_attention` 表示；不删配置、lease、ledger，不解除 root 引用，不丢 retain/unknown/inflight/cleanup。
   `RetireWith` 的迁移、controlled cleanup 或 root 引用解除须各自显式选择并绑定完整证据；
   cleanup 不能凭 CID、provider 身份或退休盘点推导所有权。

`RemoteDirective::SubmitWithFence` 只是受 fence 约束的调度候选：只有没有未解决历史副作用，
且 current 记录明确未发送/被拒绝，或完整本地 ledger 中尚无工作，才可能出现它。
Unknown、inflight、cleanup、旧已知资源分别先对账/等待/观察。查询失败/无权限/分页不全
不能用于抹除 unknown；模型也没有把普通“查不到”编码为 authoritative absence 的快捷状态。

确认维度使用集合：accepted、remote_submitted、remote_pinned、gateway_verified、
content_verified、replication_complete **互不凭 enum 顺序推导**；replica count 另核对。
规划层不会把某个 target 可复用提升为所有 providers / 所有 ZIP entries 已完成。

幂等 ID 为 `management-v1:<非秘密 plan label>:<非秘密 item label>`，不含 CID/object key，
labels 最长 64 字节、仅小写 ASCII/数字/`-`/`_`。稳定字符串用于命名和查重，完整 action、
manifest、捕获状态才用于绑定比较。集成层必须拒绝同 ID 不同 binding，不能靠 UUID 碰撞概率
替代该检查；重放成功也不是 provider ExactlyOnce 的实现声明。

所有错误为静态 `SafeError`，不回显输入、provider body、URL 或凭据。
含调用方字符串的数据类型没有 `Debug`/日志出口。wire 所需模型具有严格 serde 实现，
并非安全日志格式或自动授权的展示接口；CLI 须保护保存文件、仅展示获授权字段，
不能把 secret/token 填入 IDs 或 labels。直接对模型调用 serde 不具有 wire 入口的预算保证。

## Wire v2 边界

- 顶层固定字段：`schema_version: 2`、`applied_trust: "untrusted_claims_only"`、
  `request`、不含 native 的 `snapshot`、单份脱敏 `native`、`plan { items, summary }`、
  `migration_points`、`confirmation`。confirmation 必须显式为 ACK 数组或 null；
  枚举使用 snake_case 外部标签，null version
  表示 `"null"` 枚举而非 JSON null 或查当前对象的选择器。没有 hash-only 收据。
- 输入先限制 `MAX_WIRE_BYTES = 16 MiB`，再用不构造 Value 树的 serde visitor 预遍历；
  `MAX_WIRE_NODES = 131072` 计每个 JSON 值/容器及字段键，`MAX_WIRE_DEPTH = 64`
  （根深度 0）。随后才创建一份未可信数据，校验 request/snapshot/原始计划的 records/text 预算。
  输出 writer 同样受字节预算约束，并检查结构预算；wire 的总体预算可比单份模型预算更严。
   `decode_plan` 接受已持有的字节切片；集成层读取文件/DB blob 时也须在分配前限制长度，
  不能先无界 read-to-end 再把内存交给此入口。
- 所有 wire struct/带字段枚举拒绝未知字段，重复 JSON 字段、非法枚举、错误类型、非 UTF-8、
  尾随文档均安全拒绝。格式错误只返回 `InvalidInput`，预算错误为 `LimitExceeded`；
  不支持的版本为 `UnsupportedSchemaVersion`，重建计划不等为 `IdempotencyConflict`，
  ACK 集合不匹配为 `ConfirmationMismatch`。不向调用方返回 serde 错误文本。
- `snapshot.applied` 原样保存在文件中，**只用于重建当时的 dry-run 并核对文件内部一致性**。
  导入时移出到 `untrusted_applied_claims()`，`snapshot_without_applied()` 永远不含 receipt；
   导入不返回 `AlreadyApplied` 或写权限。集成层不得把这些声明填回可信 current snapshot。
  保存/重建的 `summary.already_applied` 同样只是原始数据中的声明，不是经认证的执行统计。
- 无确认的计划、尚未逐项确认的 candidate 和 blocked 计划均可保存作审计；导入不会自动确认、
  解锁或执行它们，后续 `revalidate` 保持原先的全有或全无安全语义。
- 一致性比较不是认证：攻击者若同步替换 request、snapshot、plan 和 confirmation，纯数据层
   没有密码学签名或外部可信原件来识别它。集成层必须认证保存来源，绑定 principal/权限/确认记录，
  拒绝可信存储中同幂等 ID 的不同完整 binding；不能把自洽的客户端文档冒充已授权执行计划。

## 有界规划及费用展示

- 一次最多 `MAX_ITEMS = 128` 项；大范围运维须分页并保存明确页清单，不隐式续扫 current-key。
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

## 精确 root 引用解除

`Action::RetireWith` 可显式选择
`RetirementSelection::ReleaseRootReferences { references: Vec<NativeKey> }`。
以下只是 `selection` 字段的 JSON 形状，不是完整请求或授权文件：

```json
{
  "release_root_references": {
    "references": [
      {
        "table": "zip_root_references",
        "values": ["batch-id", "revision", "epoch", "node-identity", "tier", "cid"]
      }
    ]
  }
}
```

`values` 全部是字符串，按原生主键顺序排列：`batch_id`, `revision`, `epoch`,
`node_identity`, `tier`, `cid`。必须选择数据库中的精确引用，不能按 prefix 认领，
也不能手写 UUID 冒充授权。目录 root 不是 S3 对象版本，不能伪造 VersionId 或 ETag；
此 selection 不表示九类动作都直接支持 directory root。

完整 confirmation 绑定 DB 派生的 `RootReferenceSnapshot`：batch owner、完整 reference/build/batch
tuples，以及来自持久私有 verification receipt 的验证事实。新解除要求 fresh 确认与原生状态
重验，引用更新和动作 receipt 原子提交；自洽的导入文件不能替代私有 DB 证据。

`zip_root_references.logical_released_at` 为 NULL 时引用仍逻辑活跃，非 NULL 表示逻辑解除。
解除不改写原 `retained`/`adopted` state，不删除 verification、manifest、root 或批次历史，
不改旧 ZIP/MPU response，也不影响共享 CID 的其他引用。默认 `Retire` 和显式 `Retain`
绝不解除 root 引用。任何逻辑解除都不承诺 unpin、GC、物理空间回收或远端占额释放。

计划、确认及相关 metadata 文件是私有材料。CLI 不覆盖现有文件或创建父目录；Unix 新文件
使用 owner-only 模式，Windows 使用 `CREATE_NEW` 并在创建时设置仅 owner 与 SYSTEM 可访问的
protected ACL，在同一句柄上核验后才写入。仍须保护目录、本地配置与数据库访问。

## 验证范围

Stage 7 本地验收已完成，检查入口及 PostgreSQL 隔离前提见
[`docs/testing.md`](../../../docs/testing.md#stage-7-local-management-checks)。
模型、编译或 mock/local 结果不证明真实 provider 账号权限、物理远端 pin 或存储回收；
本地管理合同不授权生产操作、unpin 或 GC。
