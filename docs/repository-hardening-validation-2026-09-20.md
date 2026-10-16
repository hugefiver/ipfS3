# 全库加固验证范围（2026-09-20）

本文记录 2026-09-20 加固交付的验证范围，不是当前源码的全量验证结论。当前能力和限制见 [README](../README.md)，运行入口见 [testing.md](testing.md)。

## 有效安全约束

浏览器 POST 表单在 s3s 聚合正文前拒绝。尚不支持的条件 PUT/MPU complete 明确拒绝，不提供 CAS。

普通内容 mutation 使用数据库 lease、续租和过期 fencing。部署 `standard_mutation_leases` migration 前必须排空并停止全部旧 writer，禁止新旧二进制混跑。恢复依赖 lease 到期和 fenced reclaim，不承诺即时接管。

GET/HEAD/COPY 读取一致的版本、对象、residency 和源 tags 快照。CORS/lifecycle 的 ExpectedBucketOwner 在锁内复验或同查询快照中读取。URL 下载与 CID inspect 保留有界等待及完整 EOF/header/trailer 校验。

ZIP 条目数、metadata 预算、最终 UTF-8 key 长度及 CRC/尺寸校验要求见 [ZIP 指南](../src/zip/README.md)。Unix TERM/INT 退出有界；Nginx 仅为 GET/HEAD failover，不重放失败写请求。

## 已记录的验证

以下为组合验证，不是一次 `--all-targets` 全绿运行：

| 检查 | 2026-09-20 结果 |
| --- | --- |
| lib | 1164 passed，0 failed，1 PG test ignored |
| integration | 164 passed |
| binary shutdown/HTTP tests | 10 passed |
| 真实 PostgreSQL | 44 passed，覆盖 mutation fence、residency、versioning、transition、CORS、import、lifecycle、snapshot 和 publication |
| PowerShell 行为回归 | 10 个脚本，45 项通过 |
| Linux 进程验证 | TERM/INT、活跃 PUT drain、30 秒强退和重启恢复通过 |
| Nginx HTTP 验证 | 写失败不重放、GET/HEAD failover 等 8 个场景通过 |
| 格式和静态检查 | fmt、all-target Clippy `-D warnings`、diff 检查通过 |

缺少环境时显式选择 MPU 跨副本测试会失败。该结果仅证明 fail-closed 门禁，不能计为实机场景通过。测试数量和行数变化也不证明性能收益。

## 未验证边界

这次验证未重跑完整 Cluster、多网关、256 MiB 双 Kubo F1 和全部 E2E 拓扑；相关 ignored 测试没有计作通过。Linux/Nginx 与 PostgreSQL 定向验证不替代完整跨平台或全拓扑认证，历史 F1 记录不作为后续源码的实机证据。

依赖核对范围与残余公告见 [dependency audit](dependency-audit-2026-09-19.md)。其中包括 dev/test 客户端的 `quick-xml`、未启用 MySQL 分支的 `rkyv`/`rsa`，以及编译期未维护的 `proc-macro-error2`；本记录不表示“无已知漏洞”。
