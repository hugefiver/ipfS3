# 全库加固与测试去冗余执行计划

## 实施状态（2026-09-20）

- 本计划列出的应用加固和测试精简已完成。验证失败已修正并针对性复验：最终 lib 1164 passed、integration 164 passed、真实 PostgreSQL 44 passed；这是组合验证而非单次 all-targets 全绿。完整边界与依赖残余项见[验证记录](../../repository-hardening-validation-2026-09-20.md)，历史 F1 不作为当前源码的实机证据。
- 普通 content mutation 现使用数据库时钟的 120 秒 lease，并以 30 秒间隔续租。升级 `standard_mutation_leases` 迁移前必须停止并排空全部旧 writer；旧、新二进制不得与升级后的数据库混跑，因为旧 writer 会绕过新 fence。续租失败或进程崩溃后的恢复依赖 lease 到期和 fenced reclaim，不承诺即时接管。
- `POST multipart/form-data` 在 s3s 读取正文前拒绝；`PutObject` 和 `CompleteMultipartUpload` 的 `If-Match`/`If-None-Match` 明确拒绝，不是 CAS 支持。共享 ZIP 提取器已采用 10,000 条目、64 MiB 保守 metadata 账本、最终 1024 UTF-8 字节 key 限制，以及 CRC/尺寸和 Deflate descriptor 校验，细节以 [`src/zip/README.md`](../../../src/zip/README.md) 为准。
- Unix 的 `SIGTERM`/`SIGINT` 共用 30 秒 HTTP/worker drain 总预算，Compose 为 gateway 保留 40 秒；多 gateway Nginx 仅为 `GET`/`HEAD` failover，写请求的上游故障不重放。验证命令与外部环境边界见 [testing](../../testing.md)，依赖公告核对范围和遗留项见 [dependency audit](../../dependency-audit-2026-09-19.md)。

## 目标与边界

- 基线：v0.6 收尾已由 `2e8882e`、`ea4cf55` 推送至 `origin/master`，交接时工作树干净。此前提交/推送授权不外延到本轮；本计划不包含 Git 写操作。
- 已完成的审查结论作为定位依据，不重做全库审查。下列 confirmed 指代码路径已确认，**不代表已运行漏洞复现**；执行时先以行为测试取得 RED，再最小修复、定向 GREEN。
- 理想终态：未承诺请求在副作用前拒绝；持久化发布、读取与 ownership 在并发/取消/崩溃下保持一致；下载及解压资源有界且完整性可证；正常退出可收敛；测试保留真实风险覆盖而不重复运行无意义断言。
- 不实现浏览器表单上传、不扩展条件写 CAS、不更改 CID/ETag、加密及流式存储基本契约，不趁机重构无关模块。不得整包收集请求或解压内容。
- 仅使用可用的 normal/planner 配置，不派生 agent。以下 ownership 是代码责任分区，不是派发要求；单执行者可顺序完成。可作等价局部调整，记录重要偏离及理由，不加额外审批仪式。新增外部副作用、安装软件或扩大协议/安全边界不在本授权内。

## 波次与 ownership

### 波次 A：入口和外部输入隔离（各分区可独立定向验证）

**A1 — HTTP admission**：`src/s3/route/gateway.rs`、`src/s3/http.rs` 及实际服务装配点。

- 在进入 s3s 0.14 表单解析/聚合之前拒绝未承诺的浏览器 `multipart/form-data` PostObject。该路径可在认证前聚合最高 5 GiB，且项目仅读 header，忽略表单 SSE 字段而落明文；不以提高限制或补一处 SSE 解析代替关闭入口。
- 明确匹配方法、路由和媒体类型，兼容合法媒体类型参数；不误伤 MPU initiate/complete、普通对象写入及 custom routes。保留既有 S3 错误风格，不给所有 POST 设置统一拒绝。
- RED/GREEN：未经认证的表单、大/不结束的表单流、携带 SSE 表单字段均在读取/聚合正文前失败，无 Kubo/DB 发布；合法 MPU 与自定义 POST 仍按原路由执行。

**A2 — URL/CID 输入完整性**：`src/import/downloader.rs`、`source.rs`、submission 网络校验路径、`src/kubo/cat.rs`。

- 将现有取消与超时预算覆盖到 DNS、连接/TLS、TLS 后等待响应头及重定向每一跳；正文仍用 idle 超时，不引入截断合法长上传/下载的短总时限。审查 submission 的 DNS 等待是否同样受限；保持 SSRF 与重定向重验规则。
- `inspect_file` 检查 initial/trailer 错误并消费到合法流终点；`Content-Length` 只能作校验线索，不能据此提前判定成功。以流式计数证明大小和完整性，失败不得发布元数据。
- RED/GREEN：TLS 完成后无响应头、DNS 等待与各阶段取消可终止；initial/trailer 报错、已给 CL 但短流/中途失败均不发布；正常有/无 CL 输入均成功。使用可控服务/解析器和时间推进，避免真实外网计时测试。

**A3 — ZIP 安全边界**：`src/zip/{extract,local_header,sanitize}.rs`、`src/import/decompress*`、`src/s3/route/decompress_zip.rs`。

- 除 bytes 限额外，增加条目数与累计 metadata 的有限预算；在分配、缓存或发布前扣预算，计入空文件、目录及实际保留的名称/extra 等字段。复用两条解压入口的规则，保持流式处理。
- 对最终 `prefix + filename` 规范化结果执行 **1024 UTF-8 字节** key 上限检查，而不只验证 filename。
- 流式计算并核验 CRC/尺寸，覆盖 deflate data descriptor，保持当前承诺的合法 ZIP 兼容性。损坏条目不得发布；沿用现有逐条结果/失败语义，不擅自改成全 ZIP 原子事务。
- RED/GREEN：大量空条目、metadata 超预算、恰好预算及超一单位、Unicode 拼接 key 的 1024/1025 字节、错误 CRC/descriptor、合法 descriptor 流均有行为证据。上限采用有依据的有限默认值，记录兼容性代价；不靠将安全测试 ignore 来降低耗时。

### 波次 B：持久化协调与一致快照（先稳定 store 接口）

**B1 — mutation ownership（协调核心）**：`src/store/import/ownership.rs`、相关 entities/migrations、`src/store/pinning/publication.rs`、`src/store/lifecycle_*`、`src/lifecycle/actions.rs`。

- 为普通 mutation guard 建立跨实例、数据库时钟驱动的有限 lease 与 fence；提供按 mutation 身份/代次匹配的续租、释放、完成接口。失败精确释放；取消可受控清理，崩溃最终由 lease 回收，不能只依赖异步 Drop。
- 在同一发布事务内核验 guard 身份、代次及 lease 有效性；续租丢失即停止后续发布。过期任务不能写入，旧任务 release 不能清除新 guard，不能清其他实例活跃 guard；标准写入失效掉的 import 不得因 release/expiry 复活。
- 覆盖 key/prefix、多 key/COPY、MPU、ZIP/import 与 lifecycle 调用者，统一锁顺序。迁移应处理已有无 lease guard，不能无条件清空；明确升级时停止旧 writer/排空策略，避免混跑旧版绕过 fence。迁移与恢复规则保留真实测试。
- RED/GREEN：失败、取消、模拟进程消失后 lifecycle 可恢复；双实例长任务续租、过期重入、迟到 release/commit、prefix 重叠与 superseded import 均满足上述不变量。SQLite 不能替代 PostgreSQL 锁/隔离证据。

**B2 — object 读取快照**：`src/store/object*.rs`、`src/store/residency/*`、读取状态相关 store 接口；返回一次解析好的 version/object/residency/state 结果。

- GET/HEAD/COPY source 在短 DB 一致快照中完成所有相关读取；退出事务后才发网络请求。不能假定 PostgreSQL 默认 READ COMMITTED 的多条 SELECT 就是同一快照；选择后端有效的一条查询或明确快照隔离。
- 保持显式 version、delete marker、权限与损坏状态区分；真正不一致仍报错，不以重试或吞掉 500 掩盖数据损坏。COPY 只固定 source 视图，destination publication 仍用 B1 fence。
- RED/GREEN：用 barrier 控制 PUT/delete 与各读取步骤交错，响应对应某个完整合法版本，不出现伪损坏 500；真实损坏仍可检测。确认事务不跨网络等待。

**B3 — bucket 配置原子 owner 校验**：`src/store/{cors_config,lifecycle_config,bucket}.rs`、`src/s3/ops/{cors,lifecycle}.rs`。

- 将 `ExpectedBucketOwner` 从边界传到 store，写操作在 bucket ownership 锁及同一事务内复验 owner/当前 bucket；读操作在一致快照中联合校验 owner 与配置。复用现有锁协议，避免与 B1 锁顺序冲突。
- RED/GREEN：检查后删桶重建、另一 owner 复用桶名时，旧请求不能读出或改写新桶配置；正常匹配/不匹配、缺桶与无预期 owner 的既有语义不变。

### 波次 C：共享调用点集成与运行生命周期

**共享文件单一所有者**：`src/s3/ops/object.rs`、`multipart.rs`、公共 store 导出由集成者统一修改；A/B 分区先提供接口与定向测试，不同时改这些文件。推荐局部集成顺序如下（不要求每步全量测试）：

1. 条件写 admission：PUT 与 MPU complete 的 `If-Match`/`If-None-Match` 在 Kubo、guard 获取、part 消耗或 DB mutation 前显式拒绝，包括空值及两者同时出现；使用现有不支持能力的错误语义。不得静默忽略，也不做 CAS。GET/HEAD 及 COPY 已有条件语义不受影响。
2. 接入 B1 的 guard acquire/renew/release/fenced publication，随后接入 B2 的 GET/HEAD/COPY source 快照；COPY destination 与 MPU complete 的提交继续使用新的 fence。
3. 将 A3 解压、import 和 lifecycle 的 publication 接到同一 ownership 契约；检查 B3 使用一致锁顺序。分别证明失败无发布、part 未被错误消费、旧 import 未复活。

**进程退出 owner**：`src/main.rs`、worker 生命周期装配及 Compose 配置。

- Unix 同时处理 SIGTERM 与 ctrl_c，非 Unix 保留可编译的 ctrl_c 路径；统一取消入口，停止接新工作，按有限 deadline drain HTTP/worker/lease 续租与清理。超时后有明确退出行为，不无限等待；Compose grace 大于应用退出预算。
- 与 B1 协调：活跃发布所需 lease 不能先停续租再无限 drain；取消/超时遗留由 fence/expiry 保证安全。
- 证据：现有可用 Unix/容器环境中以真实 TERM 测试空闲和活跃任务，进程在预算内退出，重启后可继续处理且无过期发布。没有对应环境时明确未验证，不能拿源码包含 `SIGTERM` 作替代。

### 波次 D：仅验证未决项，去除测试噪声

- **历史版本 manual lease `is_latest` 门禁**：定位真实租约/引用路径，先做历史版本场景。只有证明阻止合法历史版本 pin/lease 后才最小修复；保留 version identity、授权与 superseded fence，不能全局移除门禁。
- **nginx `non_idempotent` retry**：核对实际启用配置，以 upstream 已接受写入但响应失败的场景判断是否重放。若可重放 mutation，禁止该类重试并保留安全的读取重试；不引入全局幂等协议。未证实的问题不得写成已修复。
- `tests/support/mod.rs` 的 22 个测试由 integration/multi_gateway/postgres_versioning/cluster 四个 target 重复 include：将行为测试集中到单一 target，helper 仍可复用，证明只执行一份且同样覆盖 helper 行为。
- 删除 `src/store/mod.rs::migrations_create_import_tables`（已被 `test_migration_runs` 覆盖）。完整迁移名称表、PowerShell 源码字符串契约中的无意义部分，以及无关接口回归、自证字符串、TDD 过程遗留测试可直接删；保留真实迁移/升级行为、cleanup 安全、并发/加密/安全回归。删除前记录重复覆盖者或不再有效的断言理由，不为删测另造形式测试。
- 已知 lib 1122 tests ≈66s、integration 164 tests ≈22s 为比较基线，不设硬性能阈值。streaming ≈2.5s、versioning ≈3s 不能仅因耗时 ignore；优先去重复和不必要等待。256 MiB/PG/实机已 ignored 可保持，相关改动仍须按环境定向执行或披露缺口。
- `cargo-audit` 过旧且不识别 lockfile v4：不安装/升级软件、不降级 lockfile。仅在已有可用且覆盖锁定依赖的可信替代工具/公告核对路径下验证；否则最终明确“依赖公告未完成检查”，不能报告 audit 通过。

## 验证与交付证据

1. 开始执行时只确认基线与工作树差异，不覆盖后来出现的用户改动。每个漏洞先运行最小 RED，再跑受影响的行为测试取得 GREEN；对不具确定性的竞争场景使用 barrier/受控时钟而非 sleep 碰运气。
2. 所有代码和测试精简合并后执行**一轮整体验证**：`cargo fmt --check`、`cargo clippy --all-targets -- -D warnings`、`cargo test --all-targets`。记录失败原因、目标/test 数及耗时；未改变输入不反复跑全量。若修正失败，只重跑受影响检查，说明最终证据对应的代码状态。
3. 使用已配置且获准的环境补充 PostgreSQL 双实例 lease/fence、snapshot/owner 竞争，真实 HTTP 表单/条件头拒绝与 MPU/custom route 正常路径，以及 Unix TERM/drain；不启动收费服务、不安装工具。安全验证应观察 DB/Kubo 无错误副作用、无悬挂任务，而非仅看成功日志。关闭本轮 mock/子进程并只清理本轮资源。
4. 最终列出：已修复及其 RED/GREEN 证据、未决项证实/排除结果、删测理由与剩余覆盖、整体验证结果与耗时、迁移/部署要求、PG/Unix/依赖公告等未验证风险。未执行的 ignored 测试不得算通过；不提交、不推送。
