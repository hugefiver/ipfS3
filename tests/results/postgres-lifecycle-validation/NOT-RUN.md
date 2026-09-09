# PostgreSQL lifecycle validation — NOT RUN

- 日期：2026-09-09。
- 状态：**运行器已就绪、尚未实跑**。本文件是 Task 8 的初始证据，不是 PG17 或双网关测试通过的证明。
- 初始状态：本任务无 PG 实例运行；未启动或探测 Docker、PostgreSQL、Kubo、网关，也未执行任何 PostgreSQL 测试。这里只验证静态契约，不声称已检查机器上所有外部服务。
- Task 9 前置：用户自行准备隔离、可丢弃的 PostgreSQL **17**、两台共享数据库的网关、负载均衡器及 Kubo；网关使用专用测试凭据并启用 lifecycle worker。确认数据库 URL 与两台网关实际使用的数据库一致，切勿指向生产环境。
- 本地必须已有 Rust 工具链、锁定依赖缓存和构建所需工具。运行器仅执行 `cargo test --locked --offline`；先用两个 target 的 `--no-run --message-format=json` 检查本地测试可执行产物，再运行完整 target，不下载镜像或安装工具。

## Task 9 待执行命令（本任务未执行）

在独立 pwsh 会话中设置实际的隔离测试 endpoint。以下占位符必须替换为隔离环境地址；本命令不会创建或启动服务：

```powershell
$env:IPFS_S3_MULTI_GATEWAY_A_ENDPOINT = '<gateway-a-endpoint>'
$env:IPFS_S3_MULTI_GATEWAY_B_ENDPOINT = '<gateway-b-endpoint>'
$env:IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT = '<load-balancer-endpoint>'
$env:IPFS_S3_MULTI_GATEWAY_KUBO_URL = '<kubo-rpc-endpoint>'
$postgresUrl = Read-Host 'Isolated PostgreSQL 17 URL'
pwsh -NoLogo -NoProfile -File tests/run-postgres-lifecycle-validation.ps1 -PostgresUrl $postgresUrl
if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL lifecycle validation failed; inspect timestamped evidence' }
```

如果 fixture 数据库与网关共享数据库不同，显式追加 `-MultiGatewayDatabaseUrl $gatewayDatabaseUrl`；省略时使用 `-PostgresUrl` 的值。运行器会设置 `IPFS_S3_TEST_POSTGRES_URL` 和 `IPFS_S3_MULTI_GATEWAY_DATABASE_URL`，并恢复原环境。

## 安全与证据边界

- 可达性检查使用有时限的 PostgreSQL SSLRequest 协议探测，不执行 SQL、不认证、不验证服务器主版本；PG17 版本及权限由 Task 9 确认，认证失败会由测试报告。不可达时立即失败，提示自行准备 PG17，无自动基础设施回退。
- 首个预检、构建或测试失败即停止；未执行步骤保持 `NOT RUN`，零测试、过滤、ignored 或显式 skipping 不算通过。
- 每次运行以 UTC 时间戳及随机 run ID 写入本目录：每步的 `.stdout.log`、`.stderr.log`，运行 `.diagnostics.log` 与 `.summary.json`。摘要含各步骤和整体退出码。不会覆盖本初始文件或既有运行证据。
- 数据库 URL 在诊断输出中脱敏；测试输出仍属于本地诊断材料，对外发布前必须人工检查 SQL、内部标识及其他敏感内容。
- 正常 owned schema 清理由 Rust 的 `PgFixture::cleanup` 负责；panic 路径由 `OwnedPgSchemaCleanup` 尽力清理。双网关测试通过自己的 S3 清理路径删除所创建的测试资源。脚本不重复扫描或删除 schema，也不停止用户准备的服务。
- `-SkipTeardown` 仅跳过 runner 层清理：runner 本身不拥有可删除资源，因此这一开关目前不会改变资源状态。**它不能禁用 Rust 内部无条件清理，不能保证保留 fixture schema**；调用时会明确警告。日志及外部服务始终保留；若需要保留 Rust fixture，必须另行授权修改 Rust 测试，当前任务禁止该修改。
- 本文件保留初始 NOT RUN 历史；Task 9 的实跑结论应以新的时间戳结果文件为准。
