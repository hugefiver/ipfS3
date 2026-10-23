# ZIP MPU 发布中断调查与恢复门槛

## 当前状态与证据边界

最后确认的生产缓解状态：**`ff6a55c` 原子 ZIP 发布事务内租约维护热修复已上线，pinning 并发为 1、扫描间隔为保守的 5 分钟；SQLite 默认连接数、应用超时、TTL、配额、策略和发布保护不变。原 6 小时恢复窗口已退出，但持续防阻塞与副本全部完成仍不能仅凭短期观测宣告交付。**
前置 `2db74d7` 查询修复包含 publication 内部免算未消费建议和 admission 完整证据上下文集合读。`ff6a55c` 在 Legacy MPU 的同一原子事务中，沿完整共享 frontier、逐对象及逐租约/target 投影边界维护仍有效的 source/root claim，不复活过期或失权租约，不改变最终 CAS、root adoption、版本绑定及 receipt 原子边界。
本地统一回归 1,869 项通过、6 项 PostgreSQL ignored；共享 CID 内部补充修正后的 8 项 publication lease、21 项 projection 与 165 项 HTTP 集成回归通过，Rust 1.92 GNU all-target strict Clippy 和实现复核通过。Docker 构建成功，官方 CI 的格式、Check、Clippy 通过，但 unit tests 重现同一个已核对的基线上传 watchdog 用例失败，后续 lifecycle/integration/security/native-isolation 被跳过，不能算通过；未改动的 Release validation 工作流在基线与当前均失败。真实 PostgreSQL 尚未验收。
本次目标 Linux 停写备份为 276,004,864 bytes，SQLite backup API 复制及基本可读和固定范围核对完成，不声称全库完整性扫描通过。控制器已通过的 22 项控制测试继续适用。部署观察 150 秒、20 次 health/ready 均为 200，但全库 residency 统计超过观察器自身 3 秒 SQL 预算，控制器以 exit 3 结束且未回滚，业务统计和后台进度仍未知。独立补核确认已有 20 批、完整收据、schema/migrations 未变，新镜像及 compose 身份正确，伴随容器未重启、活动 standard mutation 租约为 0；据此才准入剩余原始批次的正常 API 恢复。
生产恢复状态：**现场原 MPU 齐全的 A 组 23 批已全部通过正常 Complete 发布，完整 receipt 为 23，成功输出共 3,534 个、失败输出为 0；归档、输出的版本绑定、root verified/adopted 和完整 S3/Kubo 内容读取均已通过。已验收输出共 657,608,925 bytes，源归档共 656,579,727 bytes。固定 58 批仍有 35 批未恢复，均不在这 23 批已完成范围内，缺失输入与恢复权限限制仍有效。**
已完成批次的原 MPU/parts 由正常原子发布清理。pilot 的同契约幂等回放已验证无重复对象或版本，其余批次不重复做无必要回放。三个曾返回 409 的批次均已在对应热修复上线后完成受控恢复及完整验收，分别为 39、42、184 个输出；另有 64 输出批次在本轮独立请求之外完成，发起方未确认，但其完整内容、root、receipt 和版本绑定已独立核验，不重发。
最终三批的原件与冻结契约重新核对通过后，以新一次性 nonce、全局恢复锁和同一原 owner 顺序执行；恰好发送 3 次 Complete，无自动重试。184 输出/14 parts/111,192,884 source bytes 批次 HTTP 200、132.871 秒；268 输出/6 parts/45,377,813 source bytes 批次 HTTP 200、194.444 秒；1,646 输出/23 parts/186,557,027 source bytes 批次 HTTP 200、946.569 秒。三批的所有输出和源归档均完整读取并比对 S3/Kubo 内容，root complete/adopted、immutable bindings、完整 receipt 与原 MPU/parts 清理全部通过。此前 184 输出批次曾在 124.052 秒返回 `OperationAborted/FenceRejected`，该耗时仍不能证明唯一事故根因；本次未清除 fence、放宽 TTL 或修改 published/adopted 状态。
恢复期间的附加只读 API 观察在请求启动后才开始，覆盖 1,264.74 秒、196 次 health/ready 样本：health 无失败，readiness 143 次非 200；两段采样失败区间约 166.45 秒和 832.74 秒，之后均回到 200。这不是全程无阻塞的验收；大型单连接原子发布仍会长时间占用业务数据库，副本全部完成和普通扫描频率下持续可用尚未由本轮恢复证明。
本次范围为缓解数据库争用并通过正常 API 恢复具备原始材料的任务，不以根因闭环作为执行前提；完整重构列为 roadmap 下一步。
本报告固定范围为 `2026-10-22T13:16:29Z` 前现场清点时处于 `open/pending` 的 58 个批次。
此后仍有新批次产生，不并入本轮计数、恢复候选或验收分母。
现场数据结论依据本轮已确认的清点与处置事实；源码用于解释事务边界，不能替代逐批运行证据。
本文不公开服务器域名、绝对部署路径、凭据、签名、私密 metadata、对象 key 或 CID 清单。

**58 批全部为 `source=mpu`，不是 `direct`。此前将这些批次视为 direct 的假设不成立。**
`prepared`、目录 `verified/retained`、清单中的成功条目都不等于 S3 发布成功。
29 批原 MPU/parts 记录已与冻结 Complete 契约核对匹配；其中 A 组 23 批内容与实际发布已完成验收，B 组备份找回的 6 批仍未写回或执行恢复，副本全部完成尚未验收。
候选数量不是恢复成功数量。网络中断时须先视提交结果为未知，不能仅凭客户端异常判定失败；本轮最终未提交结论来自结束原执行后的数据库核对。

## 固定范围与准确分组

| 分组 | 批次数 | 已确认保留的证据 | 当前限制 |
| --- | ---: | --- | --- |
| A | 23 | 现场原 MPU、parts、prepared archive 与 manifest；记录与完成契约匹配 | 23 批正常发布及完整内容验收通过；原 MPU/parts 已由发布清理 |
| B | 33 | prepared archive 与 manifest，现场缺原 MPU | 旧备份找回 6 批匹配原件，尚未写回；其余 27 批缺原 metadata |
| C | 2 | 连 archive/manifest 证据也不齐全 | 输入不全，不具备执行恢复条件 |
| 合计 | 58 | A 与 B 共 56 批具有 prepared archive 与 manifest | 固定范围不随新批次扩大 |

恢复前清点：56 批 manifest 共记录 **15,694 个成功条目，版本绑定数为 0**。
当时相关 source 与 output 对象均不存在；这描述对象发布状态，不证明底层内容字节已丢失。
固定恢复批次之间未发现 output key 重复，仍须在执行时检查与当前对象及并发操作的冲突。

旧备份按 batch ID 求并集，找回 **6 个不同 B 组批次**的原 MPU 及 parts：10 月 16 日备份 3 批，10 月 6 日备份另 3 批。
核对包含原身份、bucket/key、加密模式、声明 parts 的编号、strong ETag/CID、size 及持久 Complete 契约。
历史上传缺少后续才增加的 `zip_intent_json` 列时，正常代码允许读取 batch 内的冻结 capture；不能借此改用当前策略或猜元数据。
记录匹配不代表 part 的全部内容可读，也不代表已把缺失输入写回生产。

本轮候选为 `23 + 6 = 29` 批；B 组其余 `33 - 6 = 27` 批缺原 metadata。
另有 C 组 2 批输入不全，因此 `29 + 27 + 2 = 58`。
27 批和 2 批均不能通过猜测、伪造 metadata 或直接改状态推进。

## 已确认的运行问题与处置事实

事故基线镜像 revision 为 `63ca11f`；当前热修复 revision 为 `ff6a55c`，继承 `2db74d7` 查询优化及 `e56b67e` 降载缓解，均不包含本地 Stage7.5 大量未提交实现。
当前工作树不能直接视为该生产二进制的完整实现，现有未提交改动必须保留。
原程序内嵌 SQLite `3.46.0`，诊断 Python 使用 SQLite `3.53.3`，两者执行计划与耗时不能直接等同。

网关默认数据库连接数为 1。
现场观察到 `scan_claim`、`action_claim`、V2 import claim scan 失败及 terminal cleanup 超时。cleanup 有 5 秒超时；backfill 失败样本间隔约 35 秒，不能将其解释为 cleanup 每 5 秒调度。
同时 CPU 约占用 1 核，readiness 间歇返回 503；实际服务没有稳定恢复。
默认单连接配置与实测慢 SQL 是已确认问题，但实际进程的完整根因尚未闭环。

现场已增加以下索引：

```sql
CREATE INDEX idx_object_versions_object_id
ON object_versions(object_id);
CREATE INDEX idx_physical_residencies_unverified
ON physical_residencies(tier,cid)
WHERE verification_state IN ('pending','failed');
```

仅后一个索引有已确认的 `sqlite_stat1='3 3 1'`，不能宣称两个索引均完成统计验收。
对应单独 SQL 的耗时从超过 2 秒降到约 0.48 ms、1.6 ms，但这不是实际服务恢复证据。
两张表的采样统计无效，已撤回；不得把该实验结果作为有效优化结论。
前轮备份已由 SQLite 备份 API 完成复制且可读，但 `quick_check` 因超时未完成，**未获得完整性检查通过结论，也未据此证明数据库损坏**。
本轮再次在停写窗口创建当前一致性备份：266,158,080 bytes，基线 `object_versions=73,291`、`published batches=223`，并保存固定 58 批范围及候选核对记录。
新备份及私有清单权限受限，关键表可读；没有反复运行全库完整性扫描，也不声称已有独立 Kubo 内容备份。
临时索引实验副本与系统调用追踪已清理，不能声称仍有完整追踪材料可供归因。

## 发布机制与结论强度

本地机制参考：
[MPU ZIP 路由](../../src/s3/route/decompress_zip.rs#L1285-L1452)、
[批次归档捕获](../../src/store/zip/batch.rs#L175-L218)、
[发布事务](../../src/store/pinning/publication/zip.rs#L69-L90)、
[root recovery](../../src/store/zip/recovery.rs#L25-L57)。

目录构建、verify 与 retained 可在独立事务中先完成。
之后应用发布事务统一写入 archive、outputs、versions、manifest 版本绑定、pinning、root adopted、batch published 与完整 MPU receipt。
该边界意味着异常或取消可能只留下 `open/pending` 批次及 `verified/retained` 目录证据。
这些状态相容，不足以说明发布事务曾成功，也不能仅凭目录可读手工补标 published。

现有源码 root recovery 只选择 `published + failed`，并要求已准备清单及完整版本绑定。
它不覆盖本轮 `open` 数据，不能将“有 root recovery”解释为本轮会自动完成发布。

标准 mutation 租约 TTL 为 120 秒，独立续租周期与续租超时均为 30 秒；commit phase gate 在数据库获取与发布事务执行之前就暂停独立续租，覆盖整个事务及其重试，不只是最终 COMMIT。
root claim 为 600 秒，与标准 mutation 租约不是同一条保护边界。Legacy MPU 当前在原发布事务内每 30 秒到期时协作维护这两条仍有效的 claim，包含同一共享 CID 内部的逐资源边界；首次 source 维护位于完整 source/prefix/output 权限校验之后，首次 root 维护位于原 batch 锁之后。失权、已过期或单条不可分割 SQL 跨越有效期仍严格拒绝，维护失败经原 transaction closure 返回并等待 rollback，不在外层取消整个 transaction/commit。
[租约实现](../../src/store/import/ownership/mutation_lease.rs#L245-L275)和
[提交阶段](../../src/store/import/ownership/mutation_lease.rs#L332-L346)表明，续租失败可取消工作，发布仍受数据库 fence 检查。
这是一种可能失败机制，**尚无逐批证据证明某批实际因 TTL 或续租失败而中断**。

慢 SQL、单连接竞争、租约失效与请求取消之间的完整因果链仍待运行证据确认。
不能把单独 SQL 加速等同于唯一根因定位，也不能把 Kubo 4002 代理 EOF 当作 S3 请求取消证据。
本轮未确认所有中断由同一种原因触发。

## 授权边界与执行前门槛

用户已明确暂停本轮业务写入，并允许 gateway 短维护。
允许现有 S3 凭据仅在服务器内存中使用，按原 owner 签名重放原 `CompleteMultipartUpload`。
允许现有 pinning 队列与冻结策略正常运行，包括可能发生的费用；这不等于授权修改策略。
本次已授权隔离 worktree 中的热修复提交、推送远程 master 与受控 gateway 镜像部署；不部署主工作树未提交的 Stage7.5 实现。不授权安装、伪造 metadata、直接改 published/adopted、GC 或手工 unpin。

固定范围已确认无 v2 intake、无 lifecycle 活规则或待执行动作、无 import jobs 或 v2 import 请求。
pinning 队列可以正常执行，不能把它与业务写入冻结混为一谈。
上述检查不是永久保证，执行前仍须 recheck 租约、并发冲突及范围内状态变化。

恢复必须同时满足以下门槛，任一项缺失即停止对应批次：

1. 保留当前一致性数据库备份、原 MPU 原件及内容引用，不删除归档、parts、目录或解除其 pin。
   复制完成、可读和完整性通过必须分开记录；不以全量复制 Kubo 或反复全库扫描新增执行门槛，也不声称这些检查已通过。
2. 以原 upload 身份、原 owner、原请求与冻结参数逐批核对原 MPU、parts 顺序及完成契约。
   旧备份只能提供可核对的原始证据，不能整体覆盖生产库或据此捏造缺失字段。
3. 通过正常 Complete 的内容读取、prepared archive/manifest 比对及后续真实读取验证 part 和输出内容。
   metadata 存在、CID 有记录、目录 verified 都不能替代运行时内容检查。
4. 检查当前 source/output 对象、版本前沿、mutation 租约与其他写入，源对象不可覆盖。
   即使批次间 output 无重复，也不能放宽与现存对象或外部并发操作的冲突保护。
5. 只走原正常 API 重放原 Complete，不直接写发布状态、绑定、adoption 或 receipt。
   服务若拒绝原始契约，保留错误并停止，不绕过 owner、幂等或租约校验。

29 批仅是可继续核验的候选；27 批缺原 metadata、2 批输入不全，保持未恢复状态。
任何副本恢复都必须遵守原冻结策略与现有队列边界，不能用手工 unpin 或 GC 抵消后果。

## Pilot 与实际运行验收

先对满足门槛的候选执行受控 pilot，记录正常 API 的真实结果，再决定后续批次是否可推进。
成功响应后还须核对 archive/output 可读性、内容一致性、版本绑定、batch published、root adoption 与完整 MPU receipt。
检查同一 Complete 的幂等重放，不得产生重复版本、重复发布或不一致 receipt。
pinning/副本状态须按冻结策略验收；入队成功不等于副本已完成。

性能验收必须使用实际运行二进制所含的 SQLite `3.46.0`，并覆盖实际连接配置与后台工作负载。
需观察查询计划、关键 SQL 耗时、连接等待、续租与提交耗时，以及 readiness 是否持续可用。
不能以 Python SQLite `3.53.3` 的单条查询结果替代实际进程验收。
索引是否被采用、统计是否有效、后台任务是否仍报错，都须与 pilot 结果一起核对。

### 首次基线镜像受控试验结果

- 以原 owner 的现有凭据对一个已发布对象执行签名 HEAD：HTTP 200，ETag 和 size 与数据库匹配，约 3.994 秒。凭据仅存在服务器进程内存，不输出或落盘。
- 在当前一致性备份和原件核对后，仅对一个具有 18 条清单、1 个 part 的现存 MPU 做单批试验，未批量推进。
- 首次尝试发生 SSH reset，随后 SSH 也连接超时，结果先按未知处理。SSH 恢复后，先停止原 gateway 并核对 batch、receipt 和绑定，确认没有提交，再启动原容器；没有盲重放。
- 在上述明确未提交且旧执行已结束后，仅对同一批做一次有私有持久日志的受控重试。客户端在等待 HTTP 响应头时超时，约 80.099 秒，没有取得 Complete XML 或成功回执。
- 重试窗口中观察到 root epoch 从 8 增至 9；最终该构建为 `verified`，但 batch 仍 `open/pending`、`source_published=0`、完整 archive receipt 不存在、18 条清单的版本绑定仍为 0。
- 超时后没有把客户端结束当成服务端未提交证明；再次结束原 gateway 执行后核对未提交。因此本轮可确认的发布恢复成功数为 **0**，不是仍未知，也不是提交后又手工回滚。
- 未向生产 INSERT 那 6 组备份 MPU/parts，未直接修改 publication、版本绑定、root adoption 或 receipt。固定范围的最终核查仍为 **58 个 `open/pending`**。

### 有期限的后台隔离验证

另获用户单独授权后，在 gateway 已停止的条件下保存 `residency_backfill.hot_verification` 原行，以新 epoch 设置最多 30 分钟的专用维护租约；cursor/completed 不变，ZIP source/root/mutation fences 不变，也不把任何对象标为 verified。

该窗口的真实检查得到 readiness HTTP 200、约 1.337 秒，但 CPU 仍约 100.59%，没有达到继续恢复的条件，因此未发起另一批试验。
维护结束仅以专用 owner/epoch CAS 清除维护 lease，受影响行数为 1；epoch 不回退，cursor/completed 不覆盖。最终维护 owner 已不存在，原生 worker 可以继续正常领取。

恢复窗口还观察到一条非 pilot 批次的未到期 mutation。租约存在不能单独证明外部客户端仍在执行，其来源与在途状态未确认；本轮没有强行清除此业务 fence。
该实验不支持“只暂停 backfill 就能解决故障”的结论，也未定位造成发布等待的唯一 SQL 或调用链。

### 保留证据与续作要求

私有恢复目录保留 DB 备份、固定 58 批范围、候选原件核对、受控请求结果和维护前后状态。初次恢复试验结束时已恢复原镜像，专用 backfill 维护占用已释放；当前 gateway 热修复与调度状态以本文顶部及生产缓解验收为准。

恢复以降载后的正常 Complete 路径为入口，执行前重新确认当前对象、完整关联、业务写入和在途租约。不要求本次定位争用根因，但必须由真实 pilot 证明发布可完成。对任何新超时仍先分类提交结果，未知不得重发；确认已提交后才做幂等回放与读取验收。

27 批缺原 metadata，2 批输入不全，仍需要原件或另行明确的恢复授权与方案。已有备份行不等于当前可直接重建并执行上传的许可；不得忽略取消/清理、所有权或更晚写入事实。
58 批恢复尚未闭环，内容、实际发布及 pinning 副本验收未完成；不能把客户端签名成功、root verified 或此次网络中断当作恢复成功或原事故的唯一根因。

## 生产缓解验收

热修复包含 SQLite text 主键可索引查询、每轮最多 4 个 quota waiter 的 FIFO 唤醒，以及内部 waiter 路径跳过未使用的 eviction advice 查询。默认 SQLite 连接数仍为 1。

- `e56b67e` 配合并发 1、间隔 15 秒时，已观察到连续约 40 秒 readiness 503、backfill 停止推进及扫描错误；该次已回滚，固定 58 批状态与 0 条完整收据未改变，未发起 Complete。
- `e56b67e` 的早期恢复窗口使用并发 1、扫描间隔 1 小时；`2db74d7` 与最终三批恢复时使用 6 小时窗口。当前 `ff6a55c` 已调整为并发 1、保守的 5 分钟扫描。worker 启动时仍会正常执行首轮扫描，不等于暂停 pinning，也不改变冻结策略或配额。
- 上述早期上线窗口约 122 秒、24 次 readiness 检查均为 HTTP 200；启动样本约 253 ms，其后约 4–10 ms；六类目标后台错误计数均为 0。backfill claim epoch 增加 25，但这不是 ZIP 发布完成的证据。
- 切换前停写备份为 266,891,264 bytes，已完成 SQLite backup API 复制及基本可读检查，不声称全库完整性扫描通过。Kubo 未重启，未清除业务 fence，未还原数据库或手工 unpin。
- 较长观察约 608 秒中，出现 4 次 readiness 503，单次约 2 秒；因此不能声称该配置消除了所有数据库等待。单批 Complete 完成后另一次实际检查为 health 200、约 3 ms，readiness 200、约 5 ms。
- 已通过正常 API 完成 1 批原 MPU 发布：客户端从启动到结果约 68.33 秒，HTTP 200；完整收据、archive/version、18 个输出的 version 绑定及原 upload/parts 正常删除均已核对。root 为 `complete`，当前 build 为 `verified`，唯一 root reference 为 `adopted`，与发布证据一致。
- 该批 18 个输出已逐项分块完整读取 S3 GET 与 Kubo cat：18/18 的字节数均等于 manifest size，内存 SHA-256 摘要互相一致，总计 5,480,012 bytes，最大单对象 363,060 bytes；未输出内容摘要、对象 key 或 CID。
- 在确认完整收据已提交后，同一原 Complete 契约幂等回放一次，HTTP 200，XML 与冻结收据及其响应 headers 一致。batch、receipt、manifest、对象和版本未变化；archive 加 18 个 outputs 的对象行与版本行均保持 19，无重复发布。未知结果期间未重发请求。
- 该配置属于临时恢复窗口：pinning 重复扫描与队列处理被延后，副本完成时间受影响。剩余候选及完整内容验收尚未全部完成，不能以短期 readiness 成功关闭事故。

## 恢复后的保守扫描运行状态

23 批恢复全部完成后，在无活动 standard mutation 租约及无在途业务连接条件下，另一次受控 gateway-only 切换将扫描间隔由 6 小时调整为 5 分钟，并发保持 1，镜像仍为 `ff6a55c`。切换前保存当前一致性备份 285,679,616 bytes，完整保护已完成 23 批及收据；不还原数据库、不清除业务 fence，Kubo 和伴随容器未重启。控制器 151.15 秒、20 次 health/ready 均为 200，但全库 residency 统计仍超出观察预算，exit 3 不算业务进度通过；随后独立核对当前镜像、compose、调度值、固定范围、完整收据及 schema/migrations 保护通过。

另行观测保守扫描运行 722.31 秒，共 140 次 health/ready，全部为 200；只读队列采样无超时。真实签名 S3 HEAD 观测 661.03 秒，共 66 次全部为 200，ETag/size 与已验收归档相符，最长约 64.23 ms。未恢复此前失败过的 15 秒配置，未增加连接数或 readiness 超时。这些是有限窗口的实际可用性证据，不是持续无阻塞保证。观察后的固定范围、完整收据和 schema/migrations 最终保护核对通过；补充后台错误计数的 docker logs 读取超过自身 5 秒预算，当前错误计数未知，不能宣称为 0。

该窗口内新增完成的 pin job 和新 pinned target 均为 0，不能把扫描间隔缩短或 API 可用等同于副本恢复。现场 pending job 为 0，40 条 running job 均没有 locked_until，仍处于不可擅自复用的歧义保护停泊；另有 3,687 个 quota_waiting targets、4 个 pinned targets。没有把 NULL claim 重置为可领取，没有绕过配额，也未验证全部剩余队列都能自动推进。大型原子发布的数据库占用、歧义任务的安全恢复及副本完成仍是后续独立工作，按现有安全边界和冻结策略推进。

## Stage8 前必须闭环的产品修复项

- **可恢复 open MPU：** 提供保留原 owner、完成契约与冻结策略的受控恢复入口，明确缺输入的阻断状态。
- **故障可观测性：** 关联批次、请求、租约、事务和后台任务，区分取消、续租失败、SQL 阻塞及提交结果未知。
- **原子发布、幂等与租约：** 验证发布与 receipt 一致提交，覆盖提交结果未知后的重放、长操作续租及 fence 失效。
- **索引迁移：** 将必要索引纳入正式迁移，在运行 SQLite 同版本验证计划、统计与实际服务表现。
- **恢复验收：** 保留 A 组 23 批正常发布与完整内容通过的证据；另 35 批中，6 批仅找回备份 MPU/parts，尚需当前权限、生命周期与内容核对及单独恢复授权，27 批缺原 metadata，2 批 archive/manifest 输入不全。副本全部完成仍须按冻结策略及现有安全边界另行验收。

上述项尚未完成，不能用现场加索引或候选计数替代 Stage8 前的闭环证据。
后续只依据实际 pilot 与恢复结果定向更新状态，不将新批次混入本轮固定范围。
