# 全库加固与测试精简结果 — 2026-09-20

## 交付范围

v0.6 收尾已通过 `2e8882e` 和 `ea4cf55` 推送至 `origin/master`。随后进行的全库加固、依赖更新和测试精简仍是未提交工作树；先前提交、推送授权未扩展到这些修改。

本轮修复了以下可达问题，并保留行为回归：

- 在 s3s 聚合正文前拒绝浏览器 POST 表单，避免匿名大正文聚合和表单 SSE 要求被忽略。
- 明确拒绝尚未支持的条件 PUT/MPU complete，避免条件被静默忽略；没有实现 CAS。
- 普通内容 mutation 的数据库 lease、续租、精确释放、过期 fencing；成功提交与续租互斥，生命周期对活跃 mutation 的等待不消耗普通失败预算。
- GET/HEAD/COPY 的版本、对象、residency 和源 tags 一致快照；CORS/lifecycle 的 ExpectedBucketOwner 在锁内复验或同查询快照中读取。
- URL 的 DNS/响应头等待期限、CID inspect 的完整 EOF/header/trailer 校验。
- ZIP 条目/metadata 预算、最终 UTF-8 key 长度、CRC 和尺寸（含已支持的 Deflate descriptor）校验。
- 历史版本手动租约续期、Unix TERM/INT 的有界退出、Nginx 写请求不透明重放。
- PostgreSQL bucket 锁升级环通过保持 `NO KEY UPDATE` 消除，而非依靠死锁重试。

升级要求及对客户端可见的限制见 [README](../README.md#hardening-boundaries)。部署新 migration 前必须排空并停止旧 writer；禁止新旧二进制混跑。

## 删除与保留的测试

- 删除文档 checkbox、源码字符串/散列、固定迁移数量/排列等无独立行为价值的断言，以及重复的 import 表存在性测试；保留真实迁移、旧数据、约束和并发启动验证。
- 22 个 support 行为测试只由 integration 注册，避免四个 target 重复执行，减少 66 次重复执行。
- 参数隔离、进程树清理、并发临时目录所有权和路径限制是真实安全行为，集中保留在 `tests/native-runner.Tests.ps1`，未作为“字符串测试”删除。
- e2e 12、multi-gateway 5、cluster 7 个外部测试显式 ignored。专用 runner/workflow 同步显式选择；缺少所需环境必须失败，不得以 skip 冒充通过。
- 活跃上传超过 idle window 的测试改为真实接收字节驱动虚拟时钟，保留停读、取消、背压和双 EOF 回归。该单例从约 2.5 秒降至约 0.3 秒，不据此宣称整个 suite 提速。

运行入口及 PostgreSQL 前提见 [testing.md](testing.md)。

## 验证结果与时间边界

这是修正后的组合验证记录，不是一次 `--all-targets` 全绿运行：完整命令先暴露陈旧迁移 fixture 和测试时钟问题，修正后仅重跑受影响检查；未改变的绿色结果保留。

| 检查 | 结果 |
| --- | --- |
| 最终 `cargo test --locked --offline --lib --quiet` | 1164 passed / 0 failed / 1 PG test ignored，harness 76.23 秒 |
| integration | 164 passed，最近 harness 28.80 秒 |
| binary shutdown/HTTP tests | 10 passed |
| HTTP admission / object snapshot / bucket owner | 6 / 2 / 5 passed |
| residency / schema / CORS / publication 默认 | 10 / 4 / 6 / 5 passed；publication 的 1 个 PG 场景另行执行 |
| 真实 PostgreSQL | 44 passed：mutation lease/fence 2、residency 2、versioning 9、residency concurrency 6、transition schema 2、saga 1、CORS 4、import 7、lifecycle 9、lib snapshot 1、publication 1 |
| PowerShell 行为回归 | 10 个脚本，45 项通过；变更的 runner 参数与解析另作定向验证 |
| Linux 当前源码进程验证 | TERM/INT、活跃 PUT drain、30 秒强退和重启恢复通过 |
| Nginx 实际 HTTP 验证 | 写失败不重放、GET/HEAD failover 等 8 个场景通过 |
| 显式选择缺环境的 MPU 跨副本测试 | 预期失败：0 passed / 1 failed、非零退出；这是 fail-closed 门禁验证，不是实机场景通过 |
| 最终格式、Clippy、diff 检查 | `cargo fmt --all --check`、`cargo clippy --locked --offline --all-targets -- -D warnings`、`git diff --check` 均通过 |

lib 基线为 1122 项、66.35 秒；新增必要回归后的最新时间没有证明整个 lib 提速。编译耗时、并发负载和 harness 耗时不混为一谈。不能将测试行数下降等同于性能收益。

## 依赖及未验证边界

用户明确批准获取本次安全升级所需的 Cargo 源包；未安装额外工具。运行依赖更新为 `event-listener 5.4.2`、`h2 0.4.19`、`rustls 0.23.45`、`s3s 0.14.1`（使用 `quick-xml 0.41.0`）。官方 OSV 扫描覆盖 507 个 registry 包版本，详情见 [dependency audit](dependency-audit-2026-09-19.md)。

残余公告包括 dev/test 客户端的 `quick-xml 0.38.4`、未启用 MySQL 分支中的 `rkyv`/`rsa`，以及编译期未维护的 `proc-macro-error2`。未声称“无已知漏洞”。

本轮没有重跑完整 Cluster、多网关、256 MiB 双 Kubo F1 和全部 E2E 拓扑；这些 ignored 测试没有计作通过。Linux/Nginx 实测与 PostgreSQL 定向验证不替代完整跨平台或全拓扑认证，历史 F1 记录也不改写为当前工作树证据。

本轮专用 PostgreSQL 容器已在读取日志、确认名称/所有权标签后停止并自动删除，结束查询无残留；此前已有的 F1 拓扑未动。各测试 schema、Nginx/退出测试容器与临时构建资源已由对应验证清理。结束时 `HEAD...origin/master` 为 `0/0`，上述加固修改仍未提交。
