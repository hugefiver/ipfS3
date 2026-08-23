# PostgreSQL Production Baseline Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in, single-node PostgreSQL 17 production Compose baseline with database-backed readiness, an in-image readiness probe, blocking live deployment validation, and evidence-gated operator documentation while leaving the default SQLite stack unchanged.

**Architecture:** Keep `docker-compose.yml` as the existing SQLite development topology and add a separate `docker-compose.postgres.yml` containing one PostgreSQL service, one Kubo service, and one stateless gateway. `src/main.rs` retains unconditional `/health`, adds `/ready` around `Store::db().ping()` with a two-second deadline, and dispatches `--ready-probe` before configuration, migrations, listeners, or workers; a PostgreSQL-only compatibility migration converts the three entity-backed JSON columns from `TEXT` to `JSONB` after a direct-store RED proves the runtime mismatch. Static PowerShell contracts lock the workflow shape, one from-scratch workflow-parity local Docker rerun gates the initial documentation, and a fourth independent GitHub Actions job reproduces that validation after push or pull-request submission.

**Tech Stack:** Rust 2024 (MSRV 1.92), axum 0.8, Tokio 1, reqwest 0.13, SeaORM 1.x (`DatabaseConnection::ping`; the lockfile currently resolves 1.1.20), Docker Compose v2, PostgreSQL 17, Kubo, PowerShell 7, and GitHub Actions.

**Global Constraints:**
- The existing SQLite development Compose stack remains unchanged.
- `docker-compose.postgres.yml` defines exactly these services: one PostgreSQL 17 instance, one Kubo instance, and one gateway.
- The production file declares no `ports` entry for PostgreSQL or Kubo.
- Every required secret interpolation uses Compose's required form, for example `${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}` and `${IPFS_S3_MASTER_KEY:?IPFS_S3_MASTER_KEY is required}`.
- Hosted and local production validation set exact `COMPOSE_DISABLE_ENV_FILE="1"` and never use `--env-file`, so required-secret probes cannot fall back to repository `.env`; validation must not read, rename, delete, modify, or output that file. This does not alter normal operator use of `.env` with the production Compose file outside validation.
- `POSTGRES_PASSWORD` must contain only URL-safe unreserved characters, `[A-Za-z0-9._~-]`, because it is interpolated into the database URL.
- `IPFS_S3_MASTER_KEY` must be exactly 64 hexadecimal characters representing the 32-byte master key.
- `IPFS_S3_GATEWAY_BIND` must be an explicit non-wildcard host bind, and `IPFS_S3_GATEWAY_PORT` must be a decimal port from 1 through 65535.
- Provider tokens are omitted unless an enabled provider configuration references them; no literal provider token appears in YAML.
- `/health` remains an unconditional liveness endpoint.
- The runtime image intentionally contains neither `curl` nor `wget`, so a Compose healthcheck cannot depend on either tool.
- Multiple gateway instances, migration leader election, PostgreSQL high availability, backup, TLS, or connection-pool tuning are non-goals.
- IPFS Cluster, private swarm configuration, Kubo readiness checks beyond its existing service healthcheck, or cloud resources are non-goals.
- Secret-file (`_FILE`) support, a secret manager, key rotation, or changing existing encryption semantics are non-goals.
- Production operator documentation must never recommend `docker compose down --volumes`, because it deletes that volume.
- `down --volumes` is permitted only in disposable CI cleanup, where the project, volume, and data were created solely for that job.
- The work does not modify `docker-compose.yml`, `config.docker.toml`, existing migration files, entity definitions, credential semantics, Kubo behavior, Cloudflared behavior, or the roadmap entries for multiple gateways, Cluster, and private swarm. The only migration changes are the new `m20260813_000001_postgres_json_columns` file, its module declaration, and its `Migrator` registration.
- Do not edit `.debug-journal.md`; the runtime evidence needed for this revision is already recorded in the authoritative spec and the runtime-discovered-blocker section below.
- PostgreSQL has no separate migration container or manual migration command; the `docker-entrypoint-initdb.d` bootstrap creates only the required `ipfs3` role/database, while the single gateway remains the only SeaORM application-schema migration writer.
- The approved spec at `docs/superpowers/specs/2026-08-13-postgresql-production-baseline-design.md` and this plan at `docs/superpowers/plans/2026-08-13-postgresql-production-baseline.md` are intended final submission artifacts. Implementation agents must keep the runtime-revised spec at Task-start SHA-256 `ce516e5056eb76f7e4859c40127aa252a0f3476d793238dc8fda89bb32c2c599`; they must not edit this plan except for checkbox progress if the orchestrator explicitly tracks execution in-place.
- This design authorizes no version-control write.
- Implementation should stage no unrelated files and must not create a commit, tag, or push without the user's explicit permission.
- Use PowerShell syntax for every local command and every multiline GitHub Actions script; do not use Bash environment assignment, chaining, redirection, or null-device syntax.
- Do not install an HTTP probe package, a PowerShell module, a YAML parser, an action linter, or any other software.

**Authoritative spec:** `docs/superpowers/specs/2026-08-13-postgresql-production-baseline-design.md`

## File Map

- Create `docker-compose.postgres.yml`: production-only PostgreSQL/Kubo/gateway topology, required interpolation, internal networking, durable database/IPFS volumes, and binary readiness healthcheck.
- Create `tests/compose.postgres-production-validation.yml`: disposable loopback publication of PostgreSQL, Kubo RPC, and gateway on fixed validation ports.
- Modify `src/main.rs`: run-mode parsing, bounded local binary probe, `/ready` handler, classified warning logs, router state, and focused binary unit tests.
- Create `tests/postgres-production-baseline.Tests.ps1`: dependency-free static contract for the production Compose files, default-Compose non-regression, runtime image, and new workflow job.
- Modify `.github/workflows/release-validation.yml`: add independent blocking job `postgres-production-deployment`; add one blocking PostgreSQL static-contract step to `client-smoke-infrastructure`; preserve `postgres-import` and the SQLite `e2e` job byte-for-byte except where YAML placement necessarily shifts.
- Modify `tests/release-validation.Tests.ps1`: update the exact root-job count from three to four and lock the new job's commands, order, ownership, diagnostics, and cleanup contract without weakening existing job assertions.
- Create `src/store/migrations/m20260813_000001_postgres_json_columns.rs`: PostgreSQL-only prevalidation, transactional `TEXT`↔`JSONB` conversion, exact down path, and SQL-shape/non-PostgreSQL unit tests.
- Modify `src/store/migrations/mod.rs`: declare `m20260813_000001_postgres_json_columns`.
- Modify `src/store/mod.rs`: append the compatibility migration to the private `Migrator` and update the exact registration-order unit test.
- Modify `tests/postgres_import.rs`: add the fifth serialized PostgreSQL test that proves the old-schema direct decode RED, fail-closed invalid legacy JSON, new-schema GREEN, three `jsonb` types, metadata/tags round-trip, and 6 MiB part upsert/get.
- Modify `README.md`: add production-only PowerShell invocation, secret handling, readiness, and non-destructive shutdown guidance after the local workflow-parity Docker evidence and static contracts are green.
- Modify `ROADMAP.md`: change only `- [ ] PostgreSQL production deployment` to `- [x] PostgreSQL production deployment` after the local workflow-parity Docker evidence and static contracts are green.
- Include unchanged `docs/superpowers/specs/2026-08-13-postgresql-production-baseline-design.md` and authoritative `docs/superpowers/plans/2026-08-13-postgresql-production-baseline.md` in the final reviewed/committed file manifest.
- Verify unchanged `docker-compose.yml`, `config.docker.toml`, `Dockerfile`, `tests/e2e.rs`, `src/store/entities/object.rs`, `src/store/entities/multipart_upload.rs`, every pre-existing migration file, and the three adjacent unchecked v0.5 roadmap items.

---

### Task 1: Add database readiness and the in-image binary probe

**Files:**
- Modify: `src/main.rs:1-88`
- Test: `src/main.rs` (`#[cfg(test)] mod tests` in the binary target)
- Verify unchanged: `src/state.rs:20-75`
- Verify unchanged: `src/store/mod.rs:29-49,77-81`
- Verify unchanged: `Cargo.toml`

**Interfaces:**
- Consumes: `AppState::new(cfg: &Config) -> anyhow::Result<Arc<AppState>>`, `Store::db(&self) -> &DatabaseConnection`, SeaORM `ConnectionTrait::ping(&self) -> Result<(), DbErr>`, and the existing `health_check() -> &'static str`.
- Produces: `const READY_DEADLINE: Duration`, `enum RunMode { Gateway, ReadyProbe }`, `fn parse_run_mode(args: &[String]) -> anyhow::Result<RunMode>`, `async fn readiness_response<F>(ping: F, deadline: Duration) -> axum::response::Response where F: Future<Output = Result<(), DbErr>>`, `async fn ready_handler(State<Arc<AppState>>) -> axum::response::Response`, `async fn ready_probe_url(url: &str, deadline: Duration) -> bool`, `async fn ready_probe() -> bool`, and `async fn run_gateway() -> anyhow::Result<()>`.

- [ ] **Step 1: Add focused failing tests for liveness, readiness, run-mode selection, and probe behavior**

In `src/main.rs`, first add the imports required by the test seam and append this test module before implementing the new functions. The test intentionally references missing symbols so the first binary-test run is RED.

```rust
#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::get,
    };
    use http_body_util::BodyExt as _;
    use sea_orm::{Database, DbErr};
    use tower::ServiceExt as _;
    use tracing::Instrument as _;
    use tracing_subscriber::{fmt::MakeWriter, prelude::*};

    use super::*;
    use ipfs_s3_gateway::{
        crypto::key::MasterKey,
        kubo::KuboClient,
        pinning::coordinator::PinningCoordinator,
        store::{self, Store},
    };

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    struct CapturedLogWriter(CapturedLogs);

    impl io::Write for CapturedLogWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> MakeWriter<'writer> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            CapturedLogWriter(self.clone())
        }
    }

    impl CapturedLogs {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    fn test_dispatch(writer: CapturedLogs) -> tracing::Dispatch {
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
            .without_time()
            .with_ansi(false)
            .with_target(false)
            .with_writer(writer)
            .with_filter(tracing_subscriber::filter::LevelFilter::WARN),
        );
        tracing::Dispatch::new(subscriber)
    }

    async fn test_state() -> Arc<AppState> {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        Arc::new(AppState {
            kubo: KuboClient::new("http://127.0.0.1:1".to_owned()),
            store: Store::new(db),
            credentials: HashMap::new(),
            master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
            pinning: PinningCoordinator::disabled_for_test(),
        })
    }

    async fn body_text(response: axum::response::Response) -> (StatusCode, String) {
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    async fn probe_server(
        status: StatusCode,
        body: &'static str,
        delay: Duration,
    ) -> String {
        let app = Router::new().route(
            "/ready",
            get(move || async move {
                tokio::time::sleep(delay).await;
                (status, body)
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let _server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}/ready")
    }

    #[tokio::test]
    async fn health_remains_unconditional() {
        assert_eq!(health_check().await, "OK");
    }

    #[tokio::test]
    async fn ready_route_pings_the_initialized_store() {
        let app = Router::new()
            .route("/health", get(health_check))
            .route("/ready", get(ready_handler))
            .with_state(test_state().await);
        let response = app
            .oneshot(Request::get("/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let (status, body) = body_text(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "READY");
    }

    #[tokio::test]
    async fn ready_error_is_redacted() {
        let logs = CapturedLogs::default();
        let response = readiness_response(
            async { Err(DbErr::Custom("postgres://user:password@db/internal".to_owned())) },
            READY_DEADLINE,
        )
        .with_subscriber(test_dispatch(logs.clone()))
        .await;
        let (status, body) = body_text(response).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "NOT READY");
        assert!(!body.contains("postgres"));
        assert!(!body.contains("password"));
        let events = logs.text();
        assert!(events.contains("failure=\"error\""));
        assert!(!events.contains("postgres://"));
        assert!(!events.contains("password"));
        assert!(!events.contains("internal"));
    }

    #[tokio::test]
    async fn ready_timeout_is_redacted_and_bounded() {
        let logs = CapturedLogs::default();
        let response = readiness_response(
            std::future::pending::<Result<(), DbErr>>(),
            Duration::from_millis(10),
        )
        .with_subscriber(test_dispatch(logs.clone()))
        .await;
        let (status, body) = body_text(response).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "NOT READY");
        let events = logs.text();
        assert!(events.contains("failure=\"timeout\""));
        assert!(!events.contains(READY_PROBE_URL));
    }

    #[test]
    fn run_mode_accepts_only_no_argument_or_the_single_probe_flag() {
        assert_eq!(parse_run_mode(&[]).unwrap(), RunMode::Gateway);
        assert_eq!(
            parse_run_mode(&["--ready-probe".to_owned()]).unwrap(),
            RunMode::ReadyProbe
        );
        assert!(parse_run_mode(&["--ready-probe".to_owned(), "extra".to_owned()]).is_err());
        assert!(parse_run_mode(&["--unknown".to_owned()]).is_err());
    }

    #[tokio::test]
    async fn probe_accepts_only_exact_ok_and_ready() {
        let ready = probe_server(StatusCode::OK, "READY", Duration::ZERO).await;
        let wrong_status =
            probe_server(StatusCode::SERVICE_UNAVAILABLE, "READY", Duration::ZERO).await;
        let wrong_body = probe_server(StatusCode::OK, "READY\n", Duration::ZERO).await;

        assert!(ready_probe_url(&ready, Duration::from_secs(1)).await);
        assert!(!ready_probe_url(&wrong_status, Duration::from_secs(1)).await);
        assert!(!ready_probe_url(&wrong_body, Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn probe_timeout_and_connection_failure_are_non_ready() {
        let slow = probe_server(StatusCode::OK, "READY", Duration::from_secs(1)).await;
        assert!(!ready_probe_url(&slow, Duration::from_millis(10)).await);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        assert!(
            !ready_probe_url(
                &format!("http://{address}/ready"),
                Duration::from_millis(100)
            )
            .await
        );
    }
}
```

- [ ] **Step 2: Run the binary tests and preserve RED evidence**

Run:

```powershell
cargo test --bin ipfs-s3-gateway
```

Expected: compilation fails because `RunMode`, `parse_run_mode`, `READY_DEADLINE`, `ready_handler`, `readiness_response`, and `ready_probe_url` do not exist. A failure caused by an unrelated pre-existing compile error must be recorded separately and fixed outside this task's file boundary only after plan refresh.

- [ ] **Step 3: Implement the exact readiness and probe interfaces**

Replace the top-level imports needed by these paths with this compatible set; keep every existing application import not shown here:

```rust
use std::{future::Future, sync::Arc, time::Duration};

use axum::{
    Router,
    extract::State,
    http::{Response as HttpResponse, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use sea_orm::{ConnectionTrait as _, DbErr};
```

Change `handle_s3_error`'s return type from `Response<S3Body>` to `HttpResponse<S3Body>`, then place these declarations after `health_check`:

```rust
const READY_DEADLINE: Duration = Duration::from_secs(2);
const READY_PROBE_URL: &str = "http://127.0.0.1:9000/ready";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunMode {
    Gateway,
    ReadyProbe,
}

fn parse_run_mode(args: &[String]) -> anyhow::Result<RunMode> {
    match args {
        [] => Ok(RunMode::Gateway),
        [argument] if argument == "--ready-probe" => Ok(RunMode::ReadyProbe),
        _ => anyhow::bail!("usage: ipfs-s3-gateway [--ready-probe]"),
    }
}

async fn readiness_response<F>(ping: F, deadline: Duration) -> Response
where
    F: Future<Output = Result<(), DbErr>>,
{
    match tokio::time::timeout(deadline, ping).await {
        Ok(Ok(())) => (StatusCode::OK, "READY").into_response(),
        Ok(Err(_)) => {
            tracing::warn!(failure = "error", "database readiness check failed");
            (StatusCode::SERVICE_UNAVAILABLE, "NOT READY").into_response()
        }
        Err(_) => {
            tracing::warn!(failure = "timeout", "database readiness check failed");
            (StatusCode::SERVICE_UNAVAILABLE, "NOT READY").into_response()
        }
    }
}

async fn ready_handler(State(state): State<Arc<AppState>>) -> Response {
    readiness_response(state.store.db().ping(), READY_DEADLINE).await
}

async fn ready_probe_url(url: &str, deadline: Duration) -> bool {
    let Ok(client) = reqwest::Client::builder().timeout(deadline).build() else {
        return false;
    };
    let request = async {
        let response = client.get(url).send().await.ok()?;
        if response.status() != StatusCode::OK {
            return None;
        }
        (response.text().await.ok()? == "READY").then_some(())
    };
    matches!(tokio::time::timeout(deadline, request).await, Ok(Some(())))
}

async fn ready_probe() -> bool {
    ready_probe_url(READY_PROBE_URL, READY_DEADLINE).await
}
```

Move the current `main` body, including tracing initialization, configuration, `AppState::new`, migrations through state initialization, router construction, listener binding, and worker lifecycle, into this exact signature:

```rust
async fn run_gateway() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let cfg = Config::load()?;
    tracing::info!(bind = %cfg.server.bind, kubo = %cfg.kubo.rpc_url, "starting ipfs-s3-gateway");

    let state = AppState::new(&cfg).await?;
    let import_config = cfg.imports.validate()?;
    let downloader = SourceDownloader::production(Arc::new(import_config.clone()));
    let imports = ImportCoordinator::new(import_config, downloader);

    let s3_impl = S3Impl::new(state.clone());
    let gateway_auth = GatewayAuth::new(state.clone());

    let s3_service = {
        let mut builder = S3ServiceBuilder::new(s3_impl);
        builder.set_validation(AwsNameValidation::new());
        builder.set_auth(gateway_auth);
        builder.set_route(s3::route::gateway::GatewayRoute::new(
            state.clone(),
            imports.clone(),
        ));
        builder.build()
    };

    let s3_service = HandleError::new(s3_service, handle_s3_error);
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/ready", get(ready_handler))
        .fallback_service(s3_service)
        .layer(axum::middleware::from_fn(
            s3::http::bridge_chunked_content_length,
        ))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(cfg.server.bind).await?;
    tracing::info!("listening on {}", cfg.server.bind);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let pinning_worker = state
        .pinning
        .start(state.store.clone(), shutdown.child_token());
    let import_worker = imports.start(state.clone(), shutdown.child_token());
    let signal_token = shutdown.clone();
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install shutdown signal");
        }
        signal_token.cancel();
    });
    let server_result = server.await;
    shutdown.cancel();
    let grace = Duration::from_secs(30);
    tokio::join!(
        pinning_worker.shutdown(grace),
        import_worker.shutdown(grace)
    );
    server_result?;

    Ok(())
}
```

Replace `main` with dispatch that occurs before `run_gateway()`:

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match parse_run_mode(&args)? {
        RunMode::Gateway => run_gateway().await,
        RunMode::ReadyProbe => {
            if ready_probe().await {
                Ok(())
            } else {
                std::process::exit(1)
            }
        }
    }
}
```

This ordering is mandatory: probe mode must not call tracing setup, `Config::load`, `AppState::new`, `ImportCoordinator::new`, listener bind, migration setup, or either worker start. Do not print a reqwest error, URL, body, or timeout reason from probe mode.

- [ ] **Step 4: Make the Rust tests GREEN and verify the executable surface**

Run sequentially:

```powershell
cargo test --bin ipfs-s3-gateway
if ($LASTEXITCODE -ne 0) { throw "binary readiness tests failed" }

cargo check --bin ipfs-s3-gateway
if ($LASTEXITCODE -ne 0) { throw "gateway binary check failed" }

cargo run --bin ipfs-s3-gateway -- --ready-probe
if ($LASTEXITCODE -eq 0) { throw "probe unexpectedly succeeded without a ready local gateway" }
```

Expected: all binary unit tests pass, including the warning-classification/redaction assertions; the binary checks; with no service listening on `127.0.0.1:9000`, probe mode exits nonzero within two seconds and does not create a listener, database, config file, or worker log. If a real gateway is already listening on that endpoint, skip only the final negative command and record the ownership conflict; do not stop another process.

- [ ] **Step 5: Record the task checkpoint without Git writes**

Run `git diff -- src/main.rs Cargo.toml src/store/mod.rs src/state.rs`. Expected: only `src/main.rs` has a diff; no dependency, store, state initialization, or migration code changed. Do not stage or commit.

---

### Task 2: Add the production Compose topology, CI override, and static contract

**Files:**
- Create: `tests/postgres-production-baseline.Tests.ps1`
- Create: `docker-compose.postgres.yml`
- Create: `tests/compose.postgres-production-validation.yml`
- Verify unchanged: `docker-compose.yml`
- Verify unchanged: `config.docker.toml`
- Verify unchanged: `Dockerfile`

**Interfaces:**
- Consumes: `/app/ipfs-s3-gateway --ready-probe` from Task 1; existing Kubo image/build and `ipfs id` healthcheck; gateway environment keys consumed by `Config::load`; fixed disposable ports `55433` (PostgreSQL), `55001` (Kubo RPC), and `59000` (gateway).
- Produces: production services `postgres`, `kubo`, and `gateway`; embedded immutable default/runtime baseline digests; required variables `POSTGRES_PASSWORD`, `IPFS_S3_ACCESS_KEY_ID`, `IPFS_S3_SECRET_ACCESS_KEY`, `IPFS_S3_MASTER_KEY`, `IPFS_S3_GATEWAY_BIND`, and `IPFS_S3_GATEWAY_PORT`; static command `pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1`.

- [ ] **Step 1: Embed immutable default/runtime digests and create the complete Compose-only static contract**

The static script below embeds SHA-256 values of the approved pre-implementation `docker-compose.yml` and `Dockerfile`. Do not regenerate them after a file change; a mismatch is a forbidden boundary violation requiring rollback or plan refresh.

Create `tests/postgres-production-baseline.Tests.ps1` with this content:

```powershell
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ProductionPath = Join-Path $RepoRoot "docker-compose.postgres.yml"
$ValidationPath = Join-Path $RepoRoot "tests/compose.postgres-production-validation.yml"
$DefaultPath = Join-Path $RepoRoot "docker-compose.yml"
$DockerfilePath = Join-Path $RepoRoot "Dockerfile"

function Read-NormalizedText {
    param([Parameter(Mandatory)][string]$Path)
    if (-not [IO.File]::Exists($Path)) { throw "Required file is missing: $Path" }
    return [IO.File]::ReadAllText($Path).Replace("`r`n", "`n").Replace("`r", "`n")
}

function Assert-True {
    param([Parameter(Mandatory)][bool]$Condition, [Parameter(Mandatory)][string]$Message)
    if (-not $Condition) { throw $Message }
}

function Assert-Contains {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Fragment,
        [Parameter(Mandatory)][string]$Message
    )
    Assert-True ($Text.Contains($Fragment, [StringComparison]::Ordinal)) $Message
}

function Assert-NotContains {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Fragment,
        [Parameter(Mandatory)][string]$Message
    )
    Assert-True (-not $Text.Contains($Fragment, [StringComparison]::Ordinal)) $Message
}

function Assert-Matches {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Pattern,
        [Parameter(Mandatory)][string]$Message
    )
    Assert-True ([regex]::IsMatch($Text, $Pattern)) $Message
}

function Assert-NotMatches {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Pattern,
        [Parameter(Mandatory)][string]$Message
    )
    Assert-True (-not [regex]::IsMatch($Text, $Pattern)) $Message
}

function Get-YamlBlock {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Key,
        [Parameter(Mandatory)][int]$Indent
    )
    $lines = @($Text -split "`n")
    $header = (" " * $Indent) + $Key + ":"
    $indexes = @(
        for ($index = 0; $index -lt $lines.Count; $index++) {
            if ($lines[$index].TrimEnd() -ceq $header) { $index }
        }
    )
    if ($indexes.Count -ne 1) { throw "Expected one YAML key '$header', found $($indexes.Count)" }
    $start = $indexes[0]
    $end = $lines.Count
    for ($index = $start + 1; $index -lt $lines.Count; $index++) {
        if ([string]::IsNullOrWhiteSpace($lines[$index])) { continue }
        $leading = [regex]::Match($lines[$index], '^( *)').Groups[1].Length
        if ($leading -le $Indent) { $end = $index; break }
    }
    if ($end -le $start + 1) { return "" }
    return $lines[($start + 1)..($end - 1)] -join "`n"
}

$Production = Read-NormalizedText $ProductionPath
$Validation = Read-NormalizedText $ValidationPath
$Default = Read-NormalizedText $DefaultPath
$Dockerfile = Read-NormalizedText $DockerfilePath
$expectedDefaultDigest = "4e0df23fcfbdd254933bcda0d18b17a215325052cb6d2a7cc70c213b8e44a48c"
$expectedDockerfileDigest = "34aa8b4b5b880ec474d14dabe625fce5fecb45c4e906eb3489ad335b7fb6837c"
$expectedDevConfigDigest = "506596ff7da7c684f5ab9b860a49784f676372f31fd9dbf08fef401438714183"
$actualDefaultDigest = (Get-FileHash -Algorithm SHA256 -LiteralPath $DefaultPath).Hash.ToLowerInvariant()
$actualDockerfileDigest = (Get-FileHash -Algorithm SHA256 -LiteralPath $DockerfilePath).Hash.ToLowerInvariant()
$actualDevConfigDigest = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $RepoRoot "config.docker.toml")).Hash.ToLowerInvariant()
Assert-True ($actualDefaultDigest -ceq $expectedDefaultDigest) "Default SQLite Compose changed"
Assert-True ($actualDockerfileDigest -ceq $expectedDockerfileDigest) "Runtime Dockerfile changed or gained a probe package"
Assert-True ($actualDevConfigDigest -ceq $expectedDevConfigDigest) "Default development config changed"

Assert-NotContains $Production "`t" "Production Compose must use spaces"
Assert-Contains $Production "name: ipfs3-postgres" "Production Compose must use its dedicated project name"
$services = Get-YamlBlock $Production "services" 0
$serviceNames = @([regex]::Matches($services, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($serviceNames.Count -eq 3) "Production Compose must define exactly three services"
foreach ($name in @("postgres", "kubo", "gateway")) {
    Assert-True ($serviceNames -ccontains $name) "Production Compose is missing service: $name"
}

$postgres = Get-YamlBlock $services "postgres" 2
$kubo = Get-YamlBlock $services "kubo" 2
$gateway = Get-YamlBlock $services "gateway" 2
Assert-Contains $postgres "    image: postgres:17" "PostgreSQL must use postgres:17"
Assert-Contains $postgres '      POSTGRES_DB: postgres' "PostgreSQL bootstrap database must remain postgres"
Assert-Contains $postgres '      POSTGRES_USER: postgres' "PostgreSQL bootstrap superuser must remain postgres"
Assert-Contains $postgres '      POSTGRES_PASSWORD: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"' "PostgreSQL password must use required interpolation"
Assert-Contains $postgres '      test: ["CMD-SHELL", "pg_isready -U ipfs3 -d ipfs3"]' "PostgreSQL application-account healthcheck is missing"
Assert-Contains $postgres '        target: /docker-entrypoint-initdb.d/10-ipfs3.sql' "PostgreSQL must mount the inline application-role initializer"
Assert-NotMatches $postgres '(?m)^    ports:\s*$' "Production PostgreSQL must not publish a host port"
Assert-Contains $kubo '      test: ["CMD", "ipfs", "id"]' "Kubo must retain its ipfs id healthcheck"
Assert-NotMatches $kubo '(?m)^    ports:\s*$' "Production Kubo must not publish a host port"
Assert-Contains $gateway '      - "${IPFS_S3_GATEWAY_BIND:?IPFS_S3_GATEWAY_BIND is required}:${IPFS_S3_GATEWAY_PORT:?IPFS_S3_GATEWAY_PORT is required}:9000"' "Gateway publication must require an explicit host bind and port"
Assert-Contains $gateway '      IPFS_S3_DATABASE_URL: "postgres://ipfs3:${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}@postgres:5432/ipfs3"' "Gateway PostgreSQL URL is incorrect"
Assert-Contains $gateway '      IPFS_S3_KUBO_RPC_URL: http://kubo:5001' "Gateway Kubo URL is incorrect"
Assert-Contains $gateway '      IPFS_S3_BIND: 0.0.0.0:9000' "Internal gateway bind is incorrect"
Assert-Contains $gateway '      IPFS_S3_ACCESS_KEY_ID: "${IPFS_S3_ACCESS_KEY_ID:?IPFS_S3_ACCESS_KEY_ID is required}"' "Access key must use required interpolation"
Assert-Contains $gateway '      IPFS_S3_SECRET_ACCESS_KEY: "${IPFS_S3_SECRET_ACCESS_KEY:?IPFS_S3_SECRET_ACCESS_KEY is required}"' "Secret key must use required interpolation"
Assert-Contains $gateway '      IPFS_S3_MASTER_KEY: "${IPFS_S3_MASTER_KEY:?IPFS_S3_MASTER_KEY is required}"' "Master key must use required interpolation"
Assert-Contains $gateway '      test: ["CMD", "/app/ipfs-s3-gateway", "--ready-probe"]' "Gateway must use the in-image binary probe"
Assert-Contains $gateway "        condition: service_healthy" "Gateway dependencies must be health-gated"

foreach ($required in @(
    "POSTGRES_PASSWORD",
    "IPFS_S3_ACCESS_KEY_ID",
    "IPFS_S3_SECRET_ACCESS_KEY",
    "IPFS_S3_MASTER_KEY",
    "IPFS_S3_GATEWAY_BIND",
    "IPFS_S3_GATEWAY_PORT"
)) {
    Assert-NotContains $Production ('${' + $required + ':-') "Required variable has a default: $required"
    Assert-NotContains $Production ('${' + $required + '-default') "Required variable has an alternate default: $required"
}
foreach ($forbidden in @(
    "container_name:",
    "cloudflared:",
    "gateway_data",
    "config.docker.toml",
    "IPFS_S3_CONFIG",
    "PINATA_JWT",
    "FILEBASE_PINNING_TOKEN",
    '0.0.0.0:${IPFS_S3_GATEWAY_PORT}'
)) {
    Assert-NotContains $Production $forbidden "Forbidden production Compose fragment: $forbidden"
}
Assert-NotMatches $Production '(?i)\b(?:curl|wget)\b' "Production healthcheck must not use curl or wget"

$configs = Get-YamlBlock $Production "configs" 0
Assert-Contains $configs "  postgres_init:" "PostgreSQL initializer config is missing"
Assert-Contains $configs "    content: |" "PostgreSQL initializer must be inline Compose content"
Assert-Contains $configs "      CREATE ROLE ipfs3 LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE" "Initializer must create a non-superuser application role"
Assert-Contains $configs '      CREATE DATABASE ipfs3 OWNER ipfs3;' "Initializer must create the application-owned database"
Assert-Contains $configs "      \set app_password '${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}'" "Initializer password must use required interpolation"
Assert-Contains $configs "      SELECT format('ALTER ROLE ipfs3 PASSWORD %L', :'app_password') \gexec" "Initializer must quote the interpolated password through psql"
$volumes = Get-YamlBlock $Production "volumes" 0
$volumeNames = @([regex]::Matches($volumes, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($volumeNames.Count -eq 2) "Production Compose must declare exactly two named volumes"
Assert-True ($volumeNames -ccontains "postgres_data") "postgres_data volume is missing"
Assert-True ($volumeNames -ccontains "ipfs_data") "ipfs_data volume is missing"

$validationServices = Get-YamlBlock $Validation "services" 0
$validationNames = @([regex]::Matches($validationServices, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($validationNames.Count -eq 3) "Validation override must mention exactly three services"
foreach ($name in @("postgres", "kubo", "gateway")) {
    Assert-True ($validationNames -ccontains $name) "Validation override is missing service: $name"
}
Assert-Contains (Get-YamlBlock $validationServices "postgres" 2) '      - "127.0.0.1:55433:5432"' "Validation PostgreSQL mapping is incorrect"
Assert-Contains (Get-YamlBlock $validationServices "kubo" 2) '      - "127.0.0.1:55001:5001"' "Validation Kubo mapping is incorrect"
Assert-Contains (Get-YamlBlock $validationServices "gateway" 2) '      - "127.0.0.1:59000:9000"' "Validation gateway mapping is incorrect"
Assert-NotMatches $Validation '(?m)^volumes:' "Validation override must not declare persistent volumes"
Assert-NotContains $Validation "0.0.0.0" "Validation override must publish only to loopback"

Assert-NotContains $Default "  postgres:" "Default Compose must not select PostgreSQL"
Assert-NotMatches $Dockerfile '(?im)\b(?:curl|wget)\b' "Runtime image must not install curl or wget"

Write-Host "postgres production baseline contract tests: PASSED"
```

- [ ] **Step 2: Run the static contract before creating either Compose file and preserve RED**

Run:

```powershell
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
```

Expected: exit nonzero with `Required file is missing:` naming `docker-compose.postgres.yml`; the embedded baseline digests are already present and therefore are not the RED cause. Do not weaken the missing-file check.

- [ ] **Step 3: Create the exact production Compose file**

Create `docker-compose.postgres.yml` with exactly this content:

```yaml
name: ipfs3-postgres

services:
  postgres:
    image: postgres:17
    environment:
      POSTGRES_DB: postgres
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    configs:
      - source: postgres_init
        target: /docker-entrypoint-initdb.d/10-ipfs3.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ipfs3 -d ipfs3"]
      interval: 5s
      timeout: 3s
      retries: 20
      start_period: 10s
    restart: unless-stopped

  kubo:
    build: ./ipfs
    image: ghcr.io/hugefiver/ipfs3-kubo:latest
    volumes:
      - ipfs_data:/data/ipfs
    environment:
      IPFS_PATH: /data/ipfs
    healthcheck:
      test: ["CMD", "ipfs", "id"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 15s
    restart: unless-stopped

  gateway:
    build: .
    image: ghcr.io/hugefiver/ipfs3:latest
    ports:
      - "${IPFS_S3_GATEWAY_BIND:?IPFS_S3_GATEWAY_BIND is required}:${IPFS_S3_GATEWAY_PORT:?IPFS_S3_GATEWAY_PORT is required}:9000"
    environment:
      IPFS_S3_BIND: 0.0.0.0:9000
      IPFS_S3_KUBO_RPC_URL: http://kubo:5001
      IPFS_S3_DATABASE_URL: "postgres://ipfs3:${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}@postgres:5432/ipfs3"
      IPFS_S3_ACCESS_KEY_ID: "${IPFS_S3_ACCESS_KEY_ID:?IPFS_S3_ACCESS_KEY_ID is required}"
      IPFS_S3_SECRET_ACCESS_KEY: "${IPFS_S3_SECRET_ACCESS_KEY:?IPFS_S3_SECRET_ACCESS_KEY is required}"
      IPFS_S3_MASTER_KEY: "${IPFS_S3_MASTER_KEY:?IPFS_S3_MASTER_KEY is required}"
      RUST_LOG: info
    depends_on:
      postgres:
        condition: service_healthy
      kubo:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "/app/ipfs-s3-gateway", "--ready-probe"]
      interval: 5s
      timeout: 3s
      retries: 12
      start_period: 10s
    restart: unless-stopped

volumes:
  postgres_data:
  ipfs_data:

configs:
  postgres_init:
    content: |
      \set app_password '${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}'
      CREATE ROLE ipfs3 LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
      SELECT format('ALTER ROLE ipfs3 PASSWORD %L', :'app_password') \gexec
      CREATE DATABASE ipfs3 OWNER ipfs3;
```

The top-level `name: ipfs3-postgres` gives direct production invocations a stable dedicated project and therefore dedicated volume/network names; validation still overrides it with `--project-name`. The inline `postgres_init` config is PostgreSQL role/database bootstrap, not an application schema migration: the official image runs it only for an empty `postgres_data` volume, it creates `ipfs3` as `NOSUPERUSER NOCREATEDB NOCREATEROLE`, and `AppState::new` remains the only owner of SeaORM application migrations. `POSTGRES_USER=postgres` is the bootstrap superuser; the gateway never receives that username and always connects as `ipfs3`. Do not add `version`, `container_name`, `ports` to PostgreSQL/Kubo, provider tokens, a host bind mount, a gateway volume, a migration service, application-schema SQL, Cloudflared, or any package-install command.

- [ ] **Step 4: Create the exact disposable validation override**

Create `tests/compose.postgres-production-validation.yml` with exactly this content:

```yaml
services:
  postgres:
    ports:
      - "127.0.0.1:55433:5432"

  kubo:
    ports:
      - "127.0.0.1:55001:5001"

  gateway:
    ports:
      - "127.0.0.1:59000:9000"
```

The full validation environment must set `IPFS_S3_GATEWAY_BIND=127.0.0.1` and `IPFS_S3_GATEWAY_PORT=59000`, so the override's gateway entry and the interpolated base mapping are the same unique Compose port mapping rather than two publications.

- [ ] **Step 5: Make the static contract GREEN and prove PowerShell syntax**

Run:

```powershell
$tokens = $null
$parseErrors = $null
$null = [System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path "tests/postgres-production-baseline.Tests.ps1"),
    [ref]$tokens,
    [ref]$parseErrors
)
if ($parseErrors.Count -ne 0) { throw ($parseErrors | Out-String) }

pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL production static contract failed" }
```

Expected: parser reports no errors; the script exits zero and prints `postgres production baseline contract tests: PASSED`. This task does not run Docker.

- [ ] **Step 6: Record the task checkpoint without Git writes**

Run `git diff -- docker-compose.postgres.yml tests/compose.postgres-production-validation.yml tests/postgres-production-baseline.Tests.ps1 docker-compose.yml config.docker.toml Dockerfile`. Expected: only the three new task files appear; default Compose, development config, and Dockerfile have no diff. Do not stage or commit.

---

### Task 3: Add the fourth blocking release-validation job and lock its workflow contract

> **Historical execution boundary:** Tasks 1-3 describe the baseline that was already implemented before the runtime PostgreSQL failure was isolated. The strict numeric-core Docker Compose version parser in this task is the separately fixed, plan-critic-approved form and must remain byte-for-byte intact. References below to `m20260730_000001_standard_mutation_fence` record the then-current workflow/static contract; they are not the final latest-marker expectation. Do not rerun or rewrite Task 3 wholesale—Task 4 adds the migration and performs a focused RED-to-GREEN update of those exact workflow/static fragments.

> **Focused validation corrections:** Later Task 5 Windows runtime exposed two validation-only defects. Reopen only Task 3's missing-secret environment lines and their static assertions: the first focused RED is null-based Env-provider restoration without post-restore checks; the second is implicit repository `.env` fallback when a required process variable is removed. GREEN requires `Remove-Item`, exact present-value restoration, job-wide `COMPOSE_DISABLE_ENV_FILE: "1"`, four-of-four rejection without `--env-file`, and static enforcement. Preserve every other Task 3 job, parser, ordering, cleanup, and historical migration fragment until Task 4's already-defined latest-marker update.

**Files:**
- Modify: `.github/workflows/release-validation.yml:16-93`
- Modify: `tests/release-validation.Tests.ps1:153-236`
- Modify: `tests/postgres-production-baseline.Tests.ps1` (append workflow-specific assertions)
- Test: `tests/release-validation.Tests.ps1`
- Test: `tests/postgres-production-baseline.Tests.ps1`
- Verify unchanged: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: Task 2 Compose files and fixed validation ports; Task 1 `/health`, `/ready`, and binary healthcheck; `tests/e2e.rs` environment keys `IPFS_S3_E2E_ENDPOINT`/`IPFS_S3_E2E_KUBO_URL` and fixed credentials `test`/`test`.
- Produces: independent blocking job `postgres-production-deployment`, unique `COMPOSE_PROJECT_NAME=ipfs3-pg-${{ github.run_id }}-${{ github.run_attempt }}`, job-wide `COMPOSE_DISABLE_ENV_FILE="1"` isolation for every Compose command, the historical pre-revision migration assertion for `m20260730_000001_standard_mutation_fence` that Task 4 replaces with the final marker/JSONB contract, four-job static release contract, and an exact three-command blocking `client-smoke-infrastructure` sequence that executes both release contracts before the existing client-smoke test.

- [ ] **Step 1: Extend both static tests first and preserve RED**

In `tests/release-validation.Tests.ps1`, replace the exact three-job block at lines 153-164 with:

```powershell
$jobsBlock = Get-YamlBlock -Text $Workflow -Key "jobs" -Indent 0
$jobNames = @([regex]::Matches($jobsBlock, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($jobNames.Count -eq 4) "Expected exactly four jobs, found $($jobNames.Count): $($jobNames -join ', ')"
foreach ($expectedJob in @(
    "postgres-import",
    "postgres-production-deployment",
    "e2e",
    "client-smoke-infrastructure"
)) {
    Assert-True ($jobNames -ccontains $expectedJob) "Required job is missing: $expectedJob"
}
Assert-NotMatches $jobsBlock '(?m)^    needs:' "Release-validation jobs must be independent"
Assert-NotMatches $jobsBlock '(?m)^    continue-on-error:' "Release-validation jobs must be blocking"

$postgresJob = Get-YamlBlock -Text $jobsBlock -Key "postgres-import" -Indent 2
$productionJob = Get-YamlBlock -Text $jobsBlock -Key "postgres-production-deployment" -Indent 2
$e2eJob = Get-YamlBlock -Text $jobsBlock -Key "e2e" -Indent 2
$clientJob = Get-YamlBlock -Text $jobsBlock -Key "client-smoke-infrastructure" -Indent 2
```

After the existing PostgreSQL-import assertions and before the existing E2E assertions, insert:

```powershell
Assert-Contains $productionJob "    runs-on: ubuntu-latest" "Production deployment job must use ubuntu-latest"
Assert-Contains $productionJob "    timeout-minutes: 60" "Production deployment timeout must be 60 minutes"
Assert-RustSetup -JobBlock $productionJob -JobName "Production deployment job"
Assert-Contains $productionJob '      COMPOSE_PROJECT_NAME: ipfs3-pg-${{ github.run_id }}-${{ github.run_attempt }}' "Production project name must include run ID and attempt"
$composeDisableEnvFileLines = [regex]::Matches($productionJob, '(?m)^      COMPOSE_DISABLE_ENV_FILE:\s*(.+?)\s*$')
Assert-True ($composeDisableEnvFileLines.Count -eq 1) "Production job must define COMPOSE_DISABLE_ENV_FILE exactly once"
Assert-True ($composeDisableEnvFileLines[0].Groups[1].Value.Trim() -ceq '"1"') 'Production job must set COMPOSE_DISABLE_ENV_FILE to exact string "1"'
Assert-NotContains $productionJob "--env-file" "Production validation must not use an alternative env file"
Assert-Contains $productionJob "      IPFS_S3_ACCESS_KEY_ID: test" "Production E2E access key must match tests/e2e.rs"
Assert-Contains $productionJob "      IPFS_S3_SECRET_ACCESS_KEY: test" "Production E2E secret key must match tests/e2e.rs"
Assert-Contains $productionJob "      IPFS_S3_E2E_ENDPOINT: http://127.0.0.1:59000" "Production E2E endpoint is incorrect"
Assert-Contains $productionJob "      IPFS_S3_E2E_KUBO_URL: http://127.0.0.1:55001" "Production Kubo endpoint is incorrect"
Assert-NotMatches $productionJob '(?m)^    continue-on-error:' "Production deployment job must be blocking"
Assert-InOrder -Text $productionJob -Message "Production deployment checks are missing or out of order." -Fragments @(
    "      - name: Verify Docker Compose",
    "      - name: Verify production environment contract",
    "      - name: Claim unique Compose project and fixed ports",
    "      - name: Build and start production topology",
    "      - name: Verify liveness and readiness",
    "      - name: Verify latest migration and application role",
    "      - name: Run serial PostgreSQL-backed end-to-end tests",
    "      - name: Stop PostgreSQL and verify readiness failure",
    "      - name: Production Compose diagnostics",
    "      - name: Production Compose cleanup and residual assertion"
)
foreach ($fragment in @(
    "docker-compose.postgres.yml",
    "tests/compose.postgres-production-validation.yml",
    "Docker Compose 2.23.1 or newer",
    "config --quiet",
    "up --detach --build --wait --wait-timeout 300 postgres kubo gateway",
    'm20260730_000001_standard_mutation_fence',
    "cargo test --test e2e -- --nocapture --test-threads=1",
    "stop postgres",
    'http://127.0.0.1:59000/health',
    'http://127.0.0.1:59000/ready',
    "logs --no-color postgres kubo gateway",
    "down --volumes --remove-orphans",
    "com.docker.compose.project"
)) {
    Assert-Contains $productionJob $fragment "Production deployment contract is missing: $fragment"
}
Assert-NotContains $productionJob "compose --wait" "No Compose wait is allowed after PostgreSQL is stopped"
Assert-True (([regex]::Matches($productionJob, '(?m)^        if: \$\{\{ always\(\) \}\}\s*$')).Count -eq 2) "Production diagnostics and cleanup must both use always()"
Assert-Contains $productionJob 'if ($env:COMPOSE_DISABLE_ENV_FILE -cne "1") { throw "COMPOSE_DISABLE_ENV_FILE must disable implicit .env loading" }' "Production job must fail closed unless implicit .env loading is disabled"
Assert-Matches $productionJob '(?s)foreach \(\$name in @\(\s*"POSTGRES_PASSWORD",\s*"IPFS_S3_ACCESS_KEY_ID",\s*"IPFS_S3_SECRET_ACCESS_KEY",\s*"IPFS_S3_MASTER_KEY"\s*\)\) \{.*?Remove-Item -LiteralPath "Env:\$name" -ErrorAction Stop.*?config --quiet.*?if \(\$missingExit -eq 0\)' "Production job must reject all four missing required variables with implicit .env disabled"
Assert-NotMatches $productionJob 'SetEnvironmentVariable\([^,\r\n]+,\s*\$null,\s*"Process"\)' "Production job must not use SetEnvironmentVariable(..., `$null, ...) to remove or restore an environment variable"
foreach ($restoreFragment in @(
    'Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop',
    'if (Test-Path -LiteralPath "Env:$name") { throw "Required variable removal did not produce absence: $name" }',
    'if ($null -ne [Environment]::GetEnvironmentVariable($name, "Process")) { throw "Required variable removal retained a process value: $name" }',
    'if (-not (Test-Path -LiteralPath "Env:$requiredName")) { throw "Required variable restoration lost presence: $requiredName" }',
    '$restoredRequiredValue = [Environment]::GetEnvironmentVariable($requiredName, "Process")',
    'if ($restoredRequiredValue -cne $savedRequiredValues[$requiredName]) { throw "Required variable restoration changed value: $requiredName" }'
)) {
    Assert-Contains $productionJob $restoreFragment "Production environment restoration contract is missing: $restoreFragment"
}
```

Retain the existing E2E assertions exactly, including its five commands and its two `continue-on-error` entries. Retain the workflow-wide assertion that exactly two step-level `continue-on-error: true` entries exist; the new production diagnostics handles log failure inside PowerShell and the new cleanup remains blocking.

In `tests/postgres-production-baseline.Tests.ps1`, replace its final `Write-Host` with this workflow-specific block followed by the same final message:

```powershell
$WorkflowPath = Join-Path $RepoRoot ".github/workflows/release-validation.yml"
$Workflow = Read-NormalizedText $WorkflowPath
$workflowJobs = Get-YamlBlock $Workflow "jobs" 0
$workflowJobNames = @([regex]::Matches($workflowJobs, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($workflowJobNames.Count -eq 4) "Release-validation must contain exactly four jobs"
foreach ($requiredJob in @("postgres-import", "postgres-production-deployment", "e2e", "client-smoke-infrastructure")) {
    Assert-True ($workflowJobNames -ccontains $requiredJob) "Release-validation job is missing: $requiredJob"
}
$productionJob = Get-YamlBlock $workflowJobs "postgres-production-deployment" 2
$clientJob = Get-YamlBlock $workflowJobs "client-smoke-infrastructure" 2
$composeDisableEnvFileLines = [regex]::Matches($productionJob, '(?m)^      COMPOSE_DISABLE_ENV_FILE:\s*(.+?)\s*$')
Assert-True ($composeDisableEnvFileLines.Count -eq 1) "Production job must define COMPOSE_DISABLE_ENV_FILE exactly once"
Assert-True ($composeDisableEnvFileLines[0].Groups[1].Value.Trim() -ceq '"1"') 'Production job must set COMPOSE_DISABLE_ENV_FILE to exact string "1"'
Assert-NotContains $productionJob "--env-file" "Production validation must not use an alternative env file"
$clientRunLines = @($clientJob -split "`n" | Where-Object { $_ -match '^\s+run:' })
$expectedClientRunLines = @(
    "        run: pwsh -NoProfile -File tests/release-validation.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/client-smoke.Tests.ps1"
)
Assert-True ($clientRunLines.Count -eq 3) "Client-smoke infrastructure job must contain exactly three blocking run commands"
for ($index = 0; $index -lt $expectedClientRunLines.Count; $index++) {
    Assert-True ($clientRunLines[$index].TrimEnd() -ceq $expectedClientRunLines[$index]) "Client-smoke infrastructure command $($index + 1) is missing, changed, or out of order"
}
Assert-NotMatches $clientJob '(?m)^        continue-on-error:' "Client-smoke infrastructure contract steps must be blocking"
foreach ($fragment in @(
    'COMPOSE_PROJECT_NAME: ipfs3-pg-${{ github.run_id }}-${{ github.run_attempt }}',
    "POSTGRES_PRODUCTION_OWNED=true",
    "POSTGRES_PRODUCTION_ATTEMPTED=true",
    "docker-compose.postgres.yml",
    "tests/compose.postgres-production-validation.yml",
    'IPFS_S3_E2E_ENDPOINT: http://127.0.0.1:59000',
    'IPFS_S3_E2E_KUBO_URL: http://127.0.0.1:55001',
    'm20260730_000001_standard_mutation_fence',
    "cargo test --test e2e -- --nocapture --test-threads=1",
    "down --volumes --remove-orphans",
    "com.docker.compose.project"
)) {
    Assert-Contains $productionJob $fragment "Workflow production job is missing: $fragment"
}
foreach ($contractFragment in @(
    "POSTGRES_PASSWORD -notmatch '^[A-Za-z0-9._~-]+$'",
    "IPFS_S3_MASTER_KEY -notmatch '^[0-9A-Fa-f]{64}$'",
    'IPFS_S3_GATEWAY_BIND -in @("", "0.0.0.0", "::", "[::]")',
    'IPFS_S3_GATEWAY_PORT, [ref]$gatewayPort'
)) {
    Assert-Contains $productionJob $contractFragment "Workflow environment validation is missing: $contractFragment"
}
Assert-Contains $productionJob 'if ($env:COMPOSE_DISABLE_ENV_FILE -cne "1") { throw "COMPOSE_DISABLE_ENV_FILE must disable implicit .env loading" }' "Workflow must fail closed unless implicit .env loading is disabled"
Assert-Matches $productionJob '(?s)foreach \(\$name in @\(\s*"POSTGRES_PASSWORD",\s*"IPFS_S3_ACCESS_KEY_ID",\s*"IPFS_S3_SECRET_ACCESS_KEY",\s*"IPFS_S3_MASTER_KEY"\s*\)\) \{.*?Remove-Item -LiteralPath "Env:\$name" -ErrorAction Stop.*?config --quiet.*?if \(\$missingExit -eq 0\)' "Workflow must reject all four missing required variables without implicit .env fallback"
Assert-NotMatches $productionJob 'SetEnvironmentVariable\([^,\r\n]+,\s*\$null,\s*"Process"\)' "Production job must not use SetEnvironmentVariable(..., `$null, ...) to remove or restore an environment variable"
foreach ($restoreFragment in @(
    'Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop',
    'if (Test-Path -LiteralPath "Env:$name") { throw "Required variable removal did not produce absence: $name" }',
    'if ($null -ne [Environment]::GetEnvironmentVariable($name, "Process")) { throw "Required variable removal retained a process value: $name" }',
    'if (-not (Test-Path -LiteralPath "Env:$requiredName")) { throw "Required variable restoration lost presence: $requiredName" }',
    '$restoredRequiredValue = [Environment]::GetEnvironmentVariable($requiredName, "Process")',
    'if ($restoredRequiredValue -cne $savedRequiredValues[$requiredName]) { throw "Required variable restoration changed value: $requiredName" }'
)) {
    Assert-Contains $productionJob $restoreFragment "Workflow environment restoration is missing: $restoreFragment"
}
Assert-Matches $productionJob '(?s)try \{.*?Remove-Item -LiteralPath "Env:\$name" -ErrorAction Stop.*?config --quiet.*?\} finally \{.*?SetEnvironmentVariable\(\$requiredName, \$savedRequiredValues\[\$requiredName\], "Process"\).*?Test-Path -LiteralPath "Env:\$requiredName".*?\$restoredRequiredValue -cne \$savedRequiredValues\[\$requiredName\]' "Each missing-secret probe must remove deterministically and restore exact presence/value in finally"
Assert-NotContains $productionJob "0.0.0.0:55433" "Validation PostgreSQL cannot bind wildcard"
Assert-NotContains $productionJob "0.0.0.0:55001" "Validation Kubo cannot bind wildcard"
Assert-NotContains $productionJob "0.0.0.0:59000" "Validation gateway cannot bind wildcard"
Assert-NotMatches $productionJob '(?m)^    needs:' "Production job must be independent"
Assert-NotMatches $productionJob '(?m)^    continue-on-error:' "Production job must be blocking"

Write-Host "postgres production baseline contract tests: PASSED"
```

Also replace the existing client-job assertions in `tests/release-validation.Tests.ps1` with this exact three-command contract before the RED run:

```powershell
$clientRunLines = @($clientJob -split "`n" | Where-Object { $_ -match '^\s+run:' })
Assert-True ($clientRunLines.Count -eq 3) "Client-smoke infrastructure job must contain exactly three run commands"
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/release-validation.Tests.ps1" "Release-validation contract command is missing or changed."
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1" "PostgreSQL production baseline contract command is missing or changed."
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/client-smoke.Tests.ps1" "Client-smoke infrastructure command is missing or changed."
Assert-InOrder -Text $clientJob -Message "Static release contracts must run before the existing client-smoke infrastructure test." -Fragments @(
    "        run: pwsh -NoProfile -File tests/release-validation.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/client-smoke.Tests.ps1"
)
```

Run:

```powershell
pwsh -NoProfile -File tests/release-validation.Tests.ps1
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
```

The original RED state had two independent expected causes: both static scripts failed because `postgres-production-deployment` was absent, and `tests/release-validation.Tests.ps1` also failed because the client job still had two commands instead of three. For the focused corrections against the already-implemented job, run the same scripts after adding the new assertions and expect RED because the workflow still uses null-based removal, lacks exact post-restore verification, and does not disable implicit `.env` loading with exact job-level value `"1"`; do not expect or recreate the historical missing-job failures. `tests/postgres-production-baseline.Tests.ps1` reads the workflow but does not invoke `tests/release-validation.Tests.ps1`; the workflow invokes each script as a sibling step, so there is no recursive script execution.

- [ ] **Step 2: Add the exact fourth job and the required blocking static-contract sibling step**

Insert this job between `postgres-import` and `e2e` in `.github/workflows/release-validation.yml`:

```yaml
  postgres-production-deployment:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    env:
      COMPOSE_PROJECT_NAME: ipfs3-pg-${{ github.run_id }}-${{ github.run_attempt }}
      COMPOSE_DISABLE_ENV_FILE: "1"
      POSTGRES_PASSWORD: pg-${{ github.run_id }}-${{ github.run_attempt }}
      IPFS_S3_ACCESS_KEY_ID: test
      IPFS_S3_SECRET_ACCESS_KEY: test
      IPFS_S3_MASTER_KEY: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
      IPFS_S3_GATEWAY_BIND: 127.0.0.1
      IPFS_S3_GATEWAY_PORT: 59000
      IPFS_S3_E2E_ENDPOINT: http://127.0.0.1:59000
      IPFS_S3_E2E_KUBO_URL: http://127.0.0.1:55001
    steps:
      - uses: actions/checkout@v7

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@v1
        with:
          toolchain: "1.92"

      - name: Cache cargo
        uses: Swatinem/rust-cache@v2

      - name: Verify Docker Compose
        shell: pwsh
        run: |
          docker compose version
          if ($LASTEXITCODE -ne 0) { throw "Docker Compose v2 is unavailable" }
          $composeVersionText = (docker compose version --short).Trim()
          $composeVersionMatch = [regex]::Match($composeVersionText, '^v?(?<core>\d+\.\d+\.\d+)(?:[-+][0-9A-Za-z.-]+)?$')
          $composeVersion = $null
          if (-not $composeVersionMatch.Success -or -not [Version]::TryParse($composeVersionMatch.Groups["core"].Value, [ref]$composeVersion) -or $composeVersion -lt [Version]"2.23.1") { throw "Docker Compose 2.23.1 or newer is required for inline configs.content" }

      - name: Verify production environment contract
        shell: pwsh
        run: |
          if ($env:COMPOSE_DISABLE_ENV_FILE -cne "1") { throw "COMPOSE_DISABLE_ENV_FILE must disable implicit .env loading" }
          $compose = @(
            "--project-name", $env:COMPOSE_PROJECT_NAME,
            "-f", "docker-compose.postgres.yml",
            "-f", "tests/compose.postgres-production-validation.yml"
          )
          $savedRequiredValues = @{}
          foreach ($requiredName in @("POSTGRES_PASSWORD", "IPFS_S3_ACCESS_KEY_ID", "IPFS_S3_SECRET_ACCESS_KEY", "IPFS_S3_MASTER_KEY")) {
            if (-not (Test-Path -LiteralPath "Env:$requiredName")) { throw "Required job variable is absent before validation: $requiredName" }
            $savedRequiredValues[$requiredName] = [Environment]::GetEnvironmentVariable($requiredName, "Process")
          }
          if ($env:POSTGRES_PASSWORD -notmatch '^[A-Za-z0-9._~-]+$') { throw "POSTGRES_PASSWORD is not URL-safe unreserved text" }
          if ($env:IPFS_S3_MASTER_KEY -notmatch '^[0-9A-Fa-f]{64}$') { throw "IPFS_S3_MASTER_KEY is not exactly 64 hexadecimal characters" }
          if ($env:IPFS_S3_GATEWAY_BIND -in @("", "0.0.0.0", "::", "[::]")) { throw "IPFS_S3_GATEWAY_BIND must be explicit and non-wildcard" }
          $gatewayPort = 0
          if (-not [int]::TryParse($env:IPFS_S3_GATEWAY_PORT, [ref]$gatewayPort) -or $gatewayPort -lt 1 -or $gatewayPort -gt 65535) { throw "IPFS_S3_GATEWAY_PORT must be decimal 1 through 65535" }
          foreach ($name in @(
            "POSTGRES_PASSWORD",
            "IPFS_S3_ACCESS_KEY_ID",
            "IPFS_S3_SECRET_ACCESS_KEY",
            "IPFS_S3_MASTER_KEY"
          )) {
            try {
              Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop
              if (Test-Path -LiteralPath "Env:$name") { throw "Required variable removal did not produce absence: $name" }
              if ($null -ne [Environment]::GetEnvironmentVariable($name, "Process")) { throw "Required variable removal retained a process value: $name" }
              docker compose @compose config --quiet
              $missingExit = $LASTEXITCODE
            } finally {
              foreach ($requiredName in $savedRequiredValues.Keys) {
                [Environment]::SetEnvironmentVariable($requiredName, $savedRequiredValues[$requiredName], "Process")
                if (-not (Test-Path -LiteralPath "Env:$requiredName")) { throw "Required variable restoration lost presence: $requiredName" }
                $restoredRequiredValue = [Environment]::GetEnvironmentVariable($requiredName, "Process")
                if ($restoredRequiredValue -cne $savedRequiredValues[$requiredName]) { throw "Required variable restoration changed value: $requiredName" }
              }
            }
            if ($missingExit -eq 0) { throw "Compose accepted missing required variable: $name" }
          }
          docker compose @compose config --quiet
          if ($LASTEXITCODE -ne 0) { throw "Complete production Compose configuration failed" }

      - name: Claim unique Compose project and fixed ports
        shell: pwsh
        run: |
          $project = $env:COMPOSE_PROJECT_NAME
          $containers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
          if ($LASTEXITCODE -ne 0) { throw "Container ownership preflight failed" }
          $networks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
          if ($LASTEXITCODE -ne 0) { throw "Network ownership preflight failed" }
          $volumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
          if ($LASTEXITCODE -ne 0) { throw "Volume ownership preflight failed" }
          if (($containers.Count + $networks.Count + $volumes.Count) -ne 0) {
            throw "Unique Compose project already owns resources: $project"
          }
          foreach ($port in @(55433, 55001, 59000)) {
            $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $port)
            try { $listener.Start() } catch { throw "Fixed validation port is occupied: $port" } finally { $listener.Stop() }
          }
          "POSTGRES_PRODUCTION_OWNED=true" | Add-Content -LiteralPath $env:GITHUB_ENV

      - name: Build and start production topology
        shell: pwsh
        run: |
          "POSTGRES_PRODUCTION_ATTEMPTED=true" | Add-Content -LiteralPath $env:GITHUB_ENV
          docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.postgres.yml -f tests/compose.postgres-production-validation.yml up --detach --build --wait --wait-timeout 300 postgres kubo gateway
          if ($LASTEXITCODE -ne 0) { throw "Production topology did not become healthy" }

      - name: Verify liveness and readiness
        shell: pwsh
        run: |
          $health = Invoke-WebRequest -Uri "http://127.0.0.1:59000/health" -TimeoutSec 5
          if ($health.StatusCode -ne 200 -or $health.Content -cne "OK") { throw "Unexpected liveness response" }
          $ready = Invoke-WebRequest -Uri "http://127.0.0.1:59000/ready" -TimeoutSec 5
          if ($ready.StatusCode -ne 200 -or $ready.Content -cne "READY") { throw "Unexpected readiness response" }

      - name: Verify latest migration and application role
        shell: pwsh
        run: |
          $migrationOutput = @(docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.postgres.yml -f tests/compose.postgres-production-validation.yml exec -T postgres psql -U postgres -d ipfs3 -tA -c "SELECT version FROM seaql_migrations WHERE version = 'm20260730_000001_standard_mutation_fence'; SELECT rolname || ':' || rolsuper::text FROM pg_roles WHERE rolname = 'ipfs3';")
          if ($LASTEXITCODE -ne 0) { throw "Migration and application-role query failed" }
          $databaseContract = @($migrationOutput | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
          if ($databaseContract.Count -ne 2) { throw "Unexpected database contract rows: $($databaseContract -join ', ')" }
          if ($databaseContract[0] -cne "m20260730_000001_standard_mutation_fence") { throw "Latest migration is absent: $($databaseContract[0])" }
          if ($databaseContract[1] -cne "ipfs3:false") { throw "Application role is absent or superuser: $($databaseContract[1])" }

      - name: Run serial PostgreSQL-backed end-to-end tests
        run: cargo test --test e2e -- --nocapture --test-threads=1

      - name: Stop PostgreSQL and verify readiness failure
        shell: pwsh
        run: |
          docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.postgres.yml -f tests/compose.postgres-production-validation.yml stop postgres
          if ($LASTEXITCODE -ne 0) { throw "PostgreSQL stop failed" }
          $health = Invoke-WebRequest -Uri "http://127.0.0.1:59000/health" -TimeoutSec 5
          if ($health.StatusCode -ne 200 -or $health.Content -cne "OK") { throw "Liveness failed after PostgreSQL stopped" }
          $deadline = [DateTime]::UtcNow.AddSeconds(10)
          $observed = $false
          do {
            try {
              $ready = Invoke-WebRequest -Uri "http://127.0.0.1:59000/ready" -TimeoutSec 3 -SkipHttpErrorCheck
              if ($ready.StatusCode -eq 503 -and $ready.Content -ceq "NOT READY") { $observed = $true; break }
            } catch {
              Write-Host "Readiness request has not reached the expected 503 response"
            }
            Start-Sleep -Milliseconds 500
          } while ([DateTime]::UtcNow -lt $deadline)
          if (-not $observed) { throw "Readiness did not become 503 NOT READY within ten seconds" }

      - name: Production Compose diagnostics
        if: ${{ always() }}
        shell: pwsh
        run: |
          if ($env:POSTGRES_PRODUCTION_ATTEMPTED -ne "true") { Write-Host "Production topology was not attempted"; exit 0 }
          docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.postgres.yml -f tests/compose.postgres-production-validation.yml logs --no-color postgres kubo gateway
          if ($LASTEXITCODE -ne 0) { Write-Warning "Production Compose diagnostics failed" }

      - name: Production Compose cleanup and residual assertion
        if: ${{ always() }}
        shell: pwsh
        run: |
          if ($env:POSTGRES_PRODUCTION_OWNED -ne "true" -or $env:POSTGRES_PRODUCTION_ATTEMPTED -ne "true") { Write-Host "No owned production topology requires cleanup"; exit 0 }
          $project = $env:COMPOSE_PROJECT_NAME
          docker compose --project-name $project -f docker-compose.postgres.yml -f tests/compose.postgres-production-validation.yml down --volumes --remove-orphans
          $downExit = $LASTEXITCODE
          $containers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
          $containerExit = $LASTEXITCODE
          $networks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
          $networkExit = $LASTEXITCODE
          $volumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
          $volumeExit = $LASTEXITCODE
          if ($downExit -ne 0 -or $containerExit -ne 0 -or $networkExit -ne 0 -or $volumeExit -ne 0) { throw "Production cleanup or residual query failed" }
          if (($containers.Count + $networks.Count + $volumes.Count) -ne 0) { throw "Residual Compose resources remain for project: $project" }
```

In the existing `client-smoke-infrastructure` job, insert this blocking sibling step between `Test release-validation workflow contract` and `Test client-smoke infrastructure`:

```yaml
      - name: Test PostgreSQL production baseline contract
        run: pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
```

The E2E command has no explicit `shell` because it is a single portable Cargo command. Every multiline command uses `shell: pwsh`. Job-level `COMPOSE_DISABLE_ENV_FILE: "1"` applies to every Compose invocation and prevents an ignored repository `.env` from supplying a removed required secret. Validation must not inspect, read, rename, delete, or modify `.env`, and must not add `--env-file` or create a temporary env file; this isolation is validation-only and does not change production Compose behavior for operators outside the job. Do not add `needs`, a job-level or step-level `continue-on-error` to the new static-contract step, service containers, secrets from GitHub settings, a second gateway, `docker compose --wait` after the PostgreSQL stop, or production instructions that reference the override. The three client-job commands must remain ordered: release-validation contract, PostgreSQL production baseline contract, client-smoke infrastructure. The workflow-wide real-client prohibitions at the end of `tests/release-validation.Tests.ps1` remain unchanged.

- [ ] **Step 3: Make both static workflow contracts GREEN**

Run sequentially:

```powershell
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Production baseline contract failed" }

pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Release-validation workflow contract failed" }

pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Existing client-smoke infrastructure contract failed" }
```

Expected: the three scripts print their `PASSED` messages. Both workflow contracts require exactly one job-level `COMPOSE_DISABLE_ENV_FILE: "1"`, reject absent/alternate values and any `--env-file` workaround, and prove that all four removed required variables are rejected without implicit `.env` fallback. They also reject `SetEnvironmentVariable(..., $null, ...)` in the production job, require `Remove-Item -ErrorAction Stop` for deterministic absence, and require post-restore presence plus case-sensitive value verification in `finally`; the Ubuntu `pwsh` job keeps all four required job-level values present after each probe. The workflow's `client-smoke-infrastructure` job has exactly three blocking commands in the specified order. The existing SQLite `e2e` block still contains exactly its original five commands, continues to start `kubo gateway` from only `docker-compose.yml`, and retains its two `continue-on-error` diagnostics/cleanup steps; the workflow-wide count remains exactly two. Existing assertions that forbid Cloudflared, real `aws`/`mc`/`rclone` commands, the real client-smoke runner, and `-Run` remain in force.

- [ ] **Step 4: Verify the workflow is syntactically PowerShell-safe and default CI is not weakened**

Run this read-only audit:

```powershell
$workflow = [IO.File]::ReadAllText(".github/workflows/release-validation.yml")
foreach ($forbidden in @("&&", "export ", "/dev/null", "source ./")) {
    if ($workflow.Contains($forbidden, [StringComparison]::Ordinal)) {
        throw "Non-PowerShell workflow syntax found: $forbidden"
    }
}
$productionStart = $workflow.IndexOf("  postgres-production-deployment:", [StringComparison]::Ordinal)
$e2eStart = $workflow.IndexOf("  e2e:", $productionStart, [StringComparison]::Ordinal)
$production = $workflow.Substring($productionStart, $e2eStart - $productionStart)
$multilineRuns = [regex]::Matches($production, '(?m)^        run: \|\s*$').Count
$pwshShells = [regex]::Matches($production, '(?m)^        shell: pwsh\s*$').Count
if ($multilineRuns -ne $pwshShells) { throw "Every multiline production step must declare shell: pwsh" }
$clientStart = $workflow.IndexOf("  client-smoke-infrastructure:", [StringComparison]::Ordinal)
$clientJob = $workflow.Substring($clientStart)
$clientRuns = @([regex]::Matches($clientJob, '(?m)^        run: (.+)$') | ForEach-Object { $_.Groups[1].Value.TrimEnd() })
$expectedClientRuns = @(
    "pwsh -NoProfile -File tests/release-validation.Tests.ps1",
    "pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1",
    "pwsh -NoProfile -File tests/client-smoke.Tests.ps1"
)
if ($clientRuns.Count -ne 3) { throw "Client-smoke infrastructure job must contain exactly three commands" }
for ($index = 0; $index -lt $expectedClientRuns.Count; $index++) {
    if ($clientRuns[$index] -cne $expectedClientRuns[$index]) { throw "Client-smoke infrastructure command order changed at position $($index + 1)" }
}
if ($clientJob -match '(?m)^        continue-on-error:') { throw "Client-smoke infrastructure commands must all be blocking" }

git diff --exit-code -- .github/workflows/ci.yml docker-compose.yml config.docker.toml
if ($LASTEXITCODE -ne 0) { throw "Default CI or SQLite Compose behavior changed" }
```

Expected: no exception and no diff for `.github/workflows/ci.yml`, `docker-compose.yml`, or `config.docker.toml`.

- [ ] **Step 5: Record the task checkpoint without Git writes**

Inspect `git diff -- .github/workflows/release-validation.yml tests/release-validation.Tests.ps1 tests/postgres-production-baseline.Tests.ps1`. Confirm all four jobs are independent and blocking; the only intended change to the existing `client-smoke-infrastructure` job is the inserted static-contract step; its exact command count is three and order is release-validation → PostgreSQL production baseline → client-smoke infrastructure. Confirm exact job-level `COMPOSE_DISABLE_ENV_FILE: "1"`, four-of-four missing-secret rejection, no `--env-file`, and both static enforcement blocks. Confirm the existing SQLite E2E commands and its two step-level `continue-on-error` entries remain unchanged, workflow-wide real-client prohibitions remain present, and only disposable project cleanup uses `down --volumes`. Do not stage or commit.

---

## Runtime-discovered blocker and plan revision

The first Task 4 runtime attempt is diagnostic history, not accepted evidence. The strict numeric-core Compose parser had already been corrected for vendor-suffixed version output and approved; preserve it. After that correction, the production PostgreSQL 17 run passed health, readiness, current migration, and role checks, but full E2E was 10/11 and a fresh isolated `test_10_multipart_upload` run was 0/1 with a stable HTTP 500 on the 6 MiB `UploadPart`.

The gateway completed `SELECT multipart_uploads ...` with one row before returning 500; it issued no `multipart_parts` INSERT. PostgreSQL verbose logs contained no `ERROR`, `STATEMENT`, or SQLSTATE. The schema, migrations through `m20260730_000001_standard_mutation_fence`, ownership, privileges, keys, foreign key, integer widths, and timestamp types were correct. The failed upload row had `metadata = NULL`, `tags_json = []`, and no part rows, while both columns were `TEXT`; the entities decode `multipart_upload.metadata` as `Option<Json>`, `multipart_upload.tags_json` as `Json`, and `object.metadata` as `Option<Json>` although that column is also `TEXT`.

This makes a PostgreSQL JSON decode mismatch the leading candidate, not a confirmed root cause. Task 4 must first reproduce the unredacted direct-store decode failure against the old schema and then make the same test pass only after the PostgreSQL-only migration. Do not retroactively describe Tasks 1-3 as having predicted this blocker, and do not use the prior 10/11 run to gate documentation.

---

### Task 4: Prove and repair PostgreSQL JSON-column compatibility, then refresh the workflow contract

**Files:**
- Create: `src/store/migrations/m20260813_000001_postgres_json_columns.rs`
- Modify: `src/store/migrations/mod.rs:1-7`
- Modify: `src/store/mod.rs:51-107`
- Modify/Test: `tests/postgres_import.rs:1-1577`
- Modify: `.github/workflows/release-validation.yml` (only the production database-contract step; preserve the strict numeric-core parser)
- Modify: `tests/release-validation.Tests.ps1` (latest marker, three deterministic JSONB rows, and step-name contract)
- Modify: `tests/postgres-production-baseline.Tests.ps1` (latest marker and three deterministic JSONB rows)
- Verify unchanged: `src/store/entities/object.rs`
- Verify unchanged: `src/store/entities/multipart_upload.rs`
- Verify unchanged: every existing `src/store/migrations/m*.rs`
- Verify unchanged: `tests/e2e.rs`

**Interfaces:**
- Consumes: the seven-migration pre-revision chain ending at `m20260730_000001_standard_mutation_fence`; `multipart::create_upload`, `multipart::get_upload`, `multipart::upsert_part`, and `multipart::get_part`; PostgreSQL 17 `pg_input_is_valid(string text, type text) -> boolean`; SeaORM PostgreSQL's default transactional migration execution; the existing serialized `tests/postgres_import.rs` fixture.
- Produces: latest marker `m20260813_000001_postgres_json_columns`; PostgreSQL `JSONB` columns `objects.metadata`, `multipart_uploads.metadata`, and `multipart_uploads.tags_json`; SQLite no-op behavior; a five-test PostgreSQL target; and workflow/static contracts requiring the new marker plus deterministic rows `multipart_uploads.metadata:jsonb`, `multipart_uploads.tags_json:jsonb`, and `objects.metadata:jsonb`.

- [ ] **Step 1: Add only the direct-store compatibility test and old-chain fixture**

In `tests/postgres_import.rs`, add these imports without changing entity definitions or existing test logic:

```rust
use ipfs_s3_gateway::store::{
    migrations::{
        m20250701_000001_init, m20260707_000001_decompress_zip,
        m20260720_000001_sse_c_key_fingerprint,
        m20260721_000001_multi_provider_pinning, m20260729_000001_ipfs3_import,
        m20260729_000002_postgres_utc_timestamps,
        m20260730_000001_standard_mutation_fence,
    },
    multipart,
};
use sea_orm_migration::{MigrationTrait, MigratorTrait};
```

Place this test-only migrator after the two existing PostgreSQL synchronization statics. It deliberately stops at the known old marker:

```rust
struct PreJsonCompatibilityMigrator;

impl MigratorTrait for PreJsonCompatibilityMigrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20250701_000001_init::Migration),
            Box::new(m20260707_000001_decompress_zip::Migration),
            Box::new(m20260720_000001_sse_c_key_fingerprint::Migration),
            Box::new(m20260721_000001_multi_provider_pinning::Migration),
            Box::new(m20260729_000001_ipfs3_import::Migration),
            Box::new(m20260729_000002_postgres_utc_timestamps::Migration),
            Box::new(m20260730_000001_standard_mutation_fence::Migration),
        ]
    }
}
```

Append this fifth test. The generated schema identifier contains only lowercase ASCII letters, digits, and underscores; `max_connections(1)` in `connect_single` keeps the session `search_path` on one PostgreSQL connection. The first version intentionally unwraps the old-schema read so the direct `AppError::Database` debug output is visible rather than S3-redacted:

```rust
#[tokio::test]
async fn postgres_json_columns_require_compatibility_migration() {
    let Ok(url) = std::env::var("IPFS_S3_TEST_POSTGRES_URL") else {
        eprintln!("skipping PostgreSQL JSON compatibility test: IPFS_S3_TEST_POSTGRES_URL is unset");
        return;
    };
    let _serial = POSTGRES_TEST_SERIAL.lock().await;
    let admin = connect_single(&url).await;
    let schema = format!("pg_json_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!(r#"CREATE SCHEMA "{schema}""#))
        .await
        .unwrap();
    let db = connect_single(&url).await;
    db.execute_unprepared(&format!(r#"SET search_path TO "{schema}""#))
        .await
        .unwrap();
    PreJsonCompatibilityMigrator::up(&db, None).await.unwrap();

    let suffix = uuid::Uuid::new_v4();
    let bucket_name = format!("pg-json-bucket-{suffix}");
    let upload_id = format!("pg-json-upload-{suffix}");
    let object_id = format!("pg-json-object-{suffix}");
    crate_bucket(&db, &bucket_name).await;
    let metadata = None;
    let tags: Vec<ObjectTag> = Vec::new();
    multipart::create_upload(
        &db,
        &upload_id,
        &object_id,
        &bucket_name,
        "six-mib.bin",
        "none",
        None,
        None,
        Some("application/octet-stream"),
        metadata,
        &tags,
        None,
        false,
    )
    .await
    .unwrap();

    let _upload = multipart::get_upload(&db, &upload_id).await.unwrap();
}
```

- [ ] **Step 2: Run a fresh five-test PostgreSQL target and preserve the expected unredacted RED**

Run this complete PowerShell block from the repository root. It uses a unique disposable project, refuses an occupied fixed port before claiming ownership, preserves/restores both environment variables, prints logs before project-scoped cleanup, and creates no evidence file:

```powershell
$hadUrl = Test-Path Env:IPFS_S3_TEST_POSTGRES_URL
$oldUrl = [Environment]::GetEnvironmentVariable("IPFS_S3_TEST_POSTGRES_URL", "Process")
$hadPort = Test-Path Env:IPFS3_IMPORT_POSTGRES_PORT
$oldPort = [Environment]::GetEnvironmentVariable("IPFS3_IMPORT_POSTGRES_PORT", "Process")
$project = "ipfs3-pg-json-red-$PID-$([Guid]::NewGuid().ToString('N').Substring(0, 8))".ToLowerInvariant()
$owned = $false
$attempted = $false
$redConfirmed = $false
$primary = $null
$cleanupErrors = [Collections.Generic.List[string]]::new()
try {
    $containers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($LASTEXITCODE -ne 0) { throw "RED fixture container preflight failed" }
    $networks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($LASTEXITCODE -ne 0) { throw "RED fixture network preflight failed" }
    $volumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($LASTEXITCODE -ne 0) { throw "RED fixture volume preflight failed" }
    if (($containers.Count + $networks.Count + $volumes.Count) -ne 0) { throw "BLOCKED: RED fixture project already owns resources; no cleanup attempted" }
    $occupied = @(Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $_.LocalPort -eq 55432 })
    if ($occupied.Count -ne 0) { throw "BLOCKED: RED fixture port 55432 is occupied; no cleanup attempted" }
    $owned = $true
    $env:IPFS3_IMPORT_POSTGRES_PORT = "55432"
    $env:IPFS_S3_TEST_POSTGRES_URL = "postgres://ipfs3:ipfs3@127.0.0.1:55432/ipfs3_import_test"
    $attempted = $true
    docker compose --project-name $project -f tests/compose.postgres-import.yml up --detach --wait --wait-timeout 120 postgres-import
    if ($LASTEXITCODE -ne 0) { throw "UNVERIFIED: RED PostgreSQL fixture did not become healthy" }
    $testOutput = @(& cargo test --test postgres_import -- --nocapture --test-threads=1 2>&1)
    $testExit = $LASTEXITCODE
    $testOutput | ForEach-Object { Write-Host $_ }
    $redText = $testOutput -join "`n"
    if ($testExit -eq 0) { throw "Direct-store test unexpectedly passed before the migration" }
    foreach ($required in @(
        "postgres_json_columns_require_compatibility_migration",
        "4 passed; 1 failed",
        "error occurred while decoding column",
        "mismatched types",
        "JSON",
        "TEXT"
    )) {
        if (-not $redText.Contains($required, [StringComparison]::OrdinalIgnoreCase)) { throw "Expected direct-store RED evidence is absent: $required" }
    }
    if ($redText.Contains("[]", [StringComparison]::Ordinal)) { throw "Stored tags JSON leaked into direct-store RED output" }
    $redConfirmed = $true
} catch {
    $primary = $_
} finally {
    if ($owned -and $attempted) {
        docker compose --project-name $project -f tests/compose.postgres-import.yml logs --no-color postgres-import
        if ($LASTEXITCODE -ne 0) { $cleanupErrors.Add("RED fixture logs failed") }
        docker compose --project-name $project -f tests/compose.postgres-import.yml down --volumes --remove-orphans
        if ($LASTEXITCODE -ne 0) { $cleanupErrors.Add("RED fixture cleanup failed") }
        $remainingContainers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $remainingContainersExit = $LASTEXITCODE
        if ($remainingContainersExit -ne 0) { $cleanupErrors.Add("RED fixture container residual query failed with exit $remainingContainersExit") }
        $remainingNetworks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $remainingNetworksExit = $LASTEXITCODE
        if ($remainingNetworksExit -ne 0) { $cleanupErrors.Add("RED fixture network residual query failed with exit $remainingNetworksExit") }
        $remainingVolumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $remainingVolumesExit = $LASTEXITCODE
        if ($remainingVolumesExit -ne 0) { $cleanupErrors.Add("RED fixture volume residual query failed with exit $remainingVolumesExit") }
        if (@($remainingContainers + $remainingNetworks + $remainingVolumes).Count -ne 0) { $cleanupErrors.Add("RED fixture resources remain") }
    }
    if ($hadUrl) { [Environment]::SetEnvironmentVariable("IPFS_S3_TEST_POSTGRES_URL", $oldUrl, "Process") } else { [Environment]::SetEnvironmentVariable("IPFS_S3_TEST_POSTGRES_URL", $null, "Process") }
    if ($hadPort) { [Environment]::SetEnvironmentVariable("IPFS3_IMPORT_POSTGRES_PORT", $oldPort, "Process") } else { [Environment]::SetEnvironmentVariable("IPFS3_IMPORT_POSTGRES_PORT", $null, "Process") }
}
if ($null -ne $primary) { throw "RED PostgreSQL target failed: $($primary.Exception.Message); cleanup: $($cleanupErrors -join '; ')" }
if (-not $redConfirmed) { throw "The old-schema direct-store RED was not confirmed" }
if ($cleanupErrors.Count -ne 0) { throw "RED fixture cleanup failed: $($cleanupErrors -join '; ')" }
Write-Host "PostgreSQL direct-store JSON compatibility RED: CONFIRMED"
```

Expected: the four pre-existing tests pass and only `postgres_json_columns_require_compatibility_migration` fails. Its unredacted direct-store diagnostic contains a PostgreSQL/SQLx column decode mismatch between JSON and TEXT without the stored metadata value. The RED confirmation is valid only when the container, network, and volume residual queries each exit zero and their combined non-empty resource count is zero; any query failure is retained in `$cleanupErrors` and prevents PASS rather than being treated as an empty result. The existing `catch`/post-`finally` report combines an unexpected primary error with all accumulated cleanup errors after restoring the environment. If insertion fails before `get_upload`, a different test fails, or the mismatch fragments are absent, stop and return the evidence for plan refresh—the candidate is not confirmed.

- [ ] **Step 3: Add the PostgreSQL-only transactional migration and exact SQL-shape tests**

PostgreSQL 17 official documentation defines `pg_input_is_valid(string text, type text) -> boolean`. Use it only as a no-content validity predicate; never call `pg_input_error_info`, which could expose stored content. SeaORM's PostgreSQL migration convention wraps the migration atomically, so leave the default transaction behavior enabled and do not implement `use_transaction`.

Create `src/store/migrations/m20260813_000001_postgres_json_columns.rs` with exactly this content:

```rust
use sea_orm::{ConnectionTrait, DatabaseBackend};
use sea_orm_migration::prelude::*;

#[derive(Clone, Copy)]
enum ConversionDirection {
    Up,
    Down,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        execute_statements(
            manager,
            statements_for_backend(
                manager.get_connection().get_database_backend(),
                ConversionDirection::Up,
            ),
        )
        .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        execute_statements(
            manager,
            statements_for_backend(
                manager.get_connection().get_database_backend(),
                ConversionDirection::Down,
            ),
        )
        .await
    }
}

async fn execute_statements(
    manager: &SchemaManager<'_>,
    statements: Vec<&'static str>,
) -> Result<(), DbErr> {
    for statement in statements {
        manager
            .get_connection()
            .execute_unprepared(statement)
            .await?;
    }
    Ok(())
}

fn statements_for_backend(
    backend: DatabaseBackend,
    direction: ConversionDirection,
) -> Vec<&'static str> {
    if backend != DatabaseBackend::Postgres {
        return Vec::new();
    }
    match direction {
        ConversionDirection::Up => postgres_up_statements(),
        ConversionDirection::Down => postgres_down_statements(),
    }
}

fn postgres_up_statements() -> Vec<&'static str> {
    vec![
        r#"DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM "objects"
        WHERE "metadata" IS NOT NULL
          AND NOT pg_input_is_valid("metadata", 'jsonb')
    ) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'invalid JSON in objects.metadata',
            ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1 FROM "multipart_uploads"
        WHERE "metadata" IS NOT NULL
          AND NOT pg_input_is_valid("metadata", 'jsonb')
    ) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'invalid JSON in multipart_uploads.metadata',
            ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1 FROM "multipart_uploads"
        WHERE "tags_json" IS NOT NULL
          AND NOT pg_input_is_valid("tags_json", 'jsonb')
    ) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'invalid JSON in multipart_uploads.tags_json',
            ERRCODE = '22023';
    END IF;
END
$$;"#,
        r#"ALTER TABLE "multipart_uploads" ALTER COLUMN "tags_json" DROP DEFAULT"#,
        r#"ALTER TABLE "objects"
ALTER COLUMN "metadata" TYPE JSONB
USING CASE WHEN "metadata" IS NULL THEN NULL ELSE "metadata"::jsonb END"#,
        r#"ALTER TABLE "multipart_uploads"
ALTER COLUMN "metadata" TYPE JSONB
USING CASE WHEN "metadata" IS NULL THEN NULL ELSE "metadata"::jsonb END"#,
        r#"ALTER TABLE "multipart_uploads"
ALTER COLUMN "tags_json" TYPE JSONB
USING "tags_json"::jsonb"#,
        r#"ALTER TABLE "multipart_uploads" ALTER COLUMN "tags_json" SET DEFAULT '[]'::jsonb"#,
    ]
}

fn postgres_down_statements() -> Vec<&'static str> {
    vec![
        r#"ALTER TABLE "multipart_uploads" ALTER COLUMN "tags_json" DROP DEFAULT"#,
        r#"ALTER TABLE "objects"
ALTER COLUMN "metadata" TYPE TEXT
USING CASE WHEN "metadata" IS NULL THEN NULL ELSE "metadata"::text END"#,
        r#"ALTER TABLE "multipart_uploads"
ALTER COLUMN "metadata" TYPE TEXT
USING CASE WHEN "metadata" IS NULL THEN NULL ELSE "metadata"::text END"#,
        r#"ALTER TABLE "multipart_uploads"
ALTER COLUMN "tags_json" TYPE TEXT
USING "tags_json"::text"#,
        r#"ALTER TABLE "multipart_uploads" ALTER COLUMN "tags_json" SET DEFAULT '[]'::text"#,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_up_prevalidates_without_content_then_preserves_json_semantics() {
        let statements = postgres_up_statements();
        assert_eq!(statements.len(), 6);
        let validation = statements[0];
        for predicate in [
            r#"NOT pg_input_is_valid("metadata", 'jsonb')"#,
            r#"NOT pg_input_is_valid("tags_json", 'jsonb')"#,
        ] {
            assert!(validation.contains(predicate));
        }
        for message in [
            "invalid JSON in objects.metadata",
            "invalid JSON in multipart_uploads.metadata",
            "invalid JSON in multipart_uploads.tags_json",
        ] {
            assert!(validation.contains(message));
        }
        assert!(!validation.contains("pg_input_error_info"));
        assert_eq!(
            &statements[1..],
            &[
                r#"ALTER TABLE "multipart_uploads" ALTER COLUMN "tags_json" DROP DEFAULT"#,
                r#"ALTER TABLE "objects"
ALTER COLUMN "metadata" TYPE JSONB
USING CASE WHEN "metadata" IS NULL THEN NULL ELSE "metadata"::jsonb END"#,
                r#"ALTER TABLE "multipart_uploads"
ALTER COLUMN "metadata" TYPE JSONB
USING CASE WHEN "metadata" IS NULL THEN NULL ELSE "metadata"::jsonb END"#,
                r#"ALTER TABLE "multipart_uploads"
ALTER COLUMN "tags_json" TYPE JSONB
USING "tags_json"::jsonb"#,
                r#"ALTER TABLE "multipart_uploads" ALTER COLUMN "tags_json" SET DEFAULT '[]'::jsonb"#,
            ]
        );
    }

    #[test]
    fn postgres_down_restores_text_and_logical_empty_array_default() {
        assert_eq!(
            postgres_down_statements(),
            [
                r#"ALTER TABLE "multipart_uploads" ALTER COLUMN "tags_json" DROP DEFAULT"#,
                r#"ALTER TABLE "objects"
ALTER COLUMN "metadata" TYPE TEXT
USING CASE WHEN "metadata" IS NULL THEN NULL ELSE "metadata"::text END"#,
                r#"ALTER TABLE "multipart_uploads"
ALTER COLUMN "metadata" TYPE TEXT
USING CASE WHEN "metadata" IS NULL THEN NULL ELSE "metadata"::text END"#,
                r#"ALTER TABLE "multipart_uploads"
ALTER COLUMN "tags_json" TYPE TEXT
USING "tags_json"::text"#,
                r#"ALTER TABLE "multipart_uploads" ALTER COLUMN "tags_json" SET DEFAULT '[]'::text"#,
            ]
        );
    }

    #[test]
    fn non_postgres_backends_are_noops() {
        for backend in [DatabaseBackend::Sqlite, DatabaseBackend::MySql] {
            assert!(statements_for_backend(backend, ConversionDirection::Up).is_empty());
            assert!(statements_for_backend(backend, ConversionDirection::Down).is_empty());
        }
    }
}
```

Append this module declaration to `src/store/migrations/mod.rs`:

```rust
pub mod m20260813_000001_postgres_json_columns;
```

In the private `migrator` module in `src/store/mod.rs`, add the import and append the migration after `StandardMutationFenceMigration`:

```rust
use crate::store::migrations::m20260813_000001_postgres_json_columns::Migration as PostgresJsonColumnsMigration;
```

```rust
Box::new(StandardMutationFenceMigration),
Box::new(PostgresJsonColumnsMigration),
```

Rename `standard_mutation_fence_migration_is_registered_after_import_migrations` to `postgres_json_columns_migration_is_registered_last` and replace that test's assertion with the complete ordered expectation:

```rust
assert_eq!(
    names,
    [
        "m20250701_000001_init",
        "m20260707_000001_decompress_zip",
        "m20260720_000001_sse_c_key_fingerprint",
        "m20260721_000001_multi_provider_pinning",
        "m20260729_000001_ipfs3_import",
        "m20260729_000002_postgres_utc_timestamps",
        "m20260730_000001_standard_mutation_fence",
        "m20260813_000001_postgres_json_columns",
    ]
);
```

- [ ] **Step 4: Turn the lasting fifth test GREEN and prove fail-closed legacy handling**

Extend the migration import in `tests/postgres_import.rs` with `m20260813_000001_postgres_json_columns`, then add this current-chain migrator beside `PreJsonCompatibilityMigrator`:

```rust
struct CurrentJsonCompatibilityMigrator;

impl MigratorTrait for CurrentJsonCompatibilityMigrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20250701_000001_init::Migration),
            Box::new(m20260707_000001_decompress_zip::Migration),
            Box::new(m20260720_000001_sse_c_key_fingerprint::Migration),
            Box::new(m20260721_000001_multi_provider_pinning::Migration),
            Box::new(m20260729_000001_ipfs3_import::Migration),
            Box::new(m20260729_000002_postgres_utc_timestamps::Migration),
            Box::new(m20260730_000001_standard_mutation_fence::Migration),
            Box::new(m20260813_000001_postgres_json_columns::Migration),
        ]
    }
}
```

Replace only the fifth test body from Step 1 with this lasting RED-to-GREEN toggle. It first asserts the old-schema unredacted decode mismatch, then proves invalid legacy text aborts the transactional migration without recording the new marker or changing any column, then removes only its invalid fixture row, applies the migration, and verifies store round-trips plus the 6 MiB part:

```rust
#[tokio::test]
async fn postgres_json_columns_require_compatibility_migration() {
    let Ok(url) = std::env::var("IPFS_S3_TEST_POSTGRES_URL") else {
        eprintln!("skipping PostgreSQL JSON compatibility test: IPFS_S3_TEST_POSTGRES_URL is unset");
        return;
    };
    let _serial = POSTGRES_TEST_SERIAL.lock().await;
    let admin = connect_single(&url).await;
    let schema = format!("pg_json_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!(r#"CREATE SCHEMA "{schema}""#))
        .await
        .unwrap();
    let db = connect_single(&url).await;
    db.execute_unprepared(&format!(r#"SET search_path TO "{schema}""#))
        .await
        .unwrap();
    PreJsonCompatibilityMigrator::up(&db, None).await.unwrap();

    let suffix = uuid::Uuid::new_v4();
    let bucket_name = format!("pg-json-bucket-{suffix}");
    let upload_id = format!("pg-json-upload-{suffix}");
    let object_id = format!("pg-json-object-{suffix}");
    crate_bucket(&db, &bucket_name).await;
    let metadata = None;
    let tags: Vec<ObjectTag> = Vec::new();
    multipart::create_upload(
        &db,
        &upload_id,
        &object_id,
        &bucket_name,
        "six-mib.bin",
        "none",
        None,
        None,
        Some("application/octet-stream"),
        metadata.clone(),
        &tags,
        None,
        false,
    )
    .await
    .unwrap();

    let old_error = multipart::get_upload(&db, &upload_id).await.unwrap_err();
    let old_diagnostic = format!("{old_error:?}");
    let old_diagnostic_lower = old_diagnostic.to_ascii_lowercase();
    for fragment in [
        "error occurred while decoding column",
        "mismatched types",
        "json",
        "text",
    ] {
        assert!(
            old_diagnostic_lower.contains(fragment),
            "missing RED diagnostic: {fragment}"
        );
    }
    assert!(!old_diagnostic.contains("[]"));

    let invalid_object_id = format!("pg-json-invalid-{suffix}");
    db.execute_unprepared(&format!(
        "INSERT INTO objects (id, bucket, key, cid, size, etag, metadata) \
         VALUES ('{invalid_object_id}', '{bucket_name}', 'invalid-json', 'bafy-invalid', 1, 'bafy-invalid', 'not-json')"
    ))
    .await
    .unwrap();
    let invalid_error = CurrentJsonCompatibilityMigrator::up(&db, None)
        .await
        .unwrap_err();
    let invalid_diagnostic = format!("{invalid_error}");
    assert!(invalid_diagnostic.contains("invalid JSON in objects.metadata"));
    assert!(!invalid_diagnostic.contains("not-json"));
    let marker_after_failure = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT EXISTS (SELECT 1 FROM seaql_migrations WHERE version = 'm20260813_000001_postgres_json_columns') AS applied",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<bool>("", "applied")
        .unwrap();
    assert!(!marker_after_failure);
    let types_after_failure = db
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT table_name || '.' || column_name || ':' || data_type AS contract \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() \
               AND (table_name, column_name) IN ( \
                   ('objects', 'metadata'), \
                   ('multipart_uploads', 'metadata'), \
                   ('multipart_uploads', 'tags_json') \
               ) ORDER BY table_name, column_name",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "contract").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        types_after_failure,
        [
            "multipart_uploads.metadata:text",
            "multipart_uploads.tags_json:text",
            "objects.metadata:text",
        ]
    );
    db.execute_unprepared(&format!("DELETE FROM objects WHERE id = '{invalid_object_id}'"))
        .await
        .unwrap();

    CurrentJsonCompatibilityMigrator::up(&db, None)
        .await
        .unwrap();
    let marker = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT version FROM seaql_migrations WHERE version = 'm20260813_000001_postgres_json_columns'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<String>("", "version")
        .unwrap();
    assert_eq!(marker, "m20260813_000001_postgres_json_columns");
    let jsonb_columns = db
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT table_name || '.' || column_name || ':' || data_type AS contract \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() \
               AND (table_name, column_name) IN ( \
                   ('objects', 'metadata'), \
                   ('multipart_uploads', 'metadata'), \
                   ('multipart_uploads', 'tags_json') \
               ) ORDER BY table_name, column_name",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "contract").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        jsonb_columns,
        [
            "multipart_uploads.metadata:jsonb",
            "multipart_uploads.tags_json:jsonb",
            "objects.metadata:jsonb",
        ]
    );
    let metadata_nullability = db
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT table_name || '.' || column_name || ':' || is_nullable AS contract \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() \
               AND (table_name, column_name) IN ( \
                   ('objects', 'metadata'), \
                   ('multipart_uploads', 'metadata') \
               ) ORDER BY table_name, column_name",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "contract").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        metadata_nullability,
        ["multipart_uploads.metadata:YES", "objects.metadata:YES"]
    );
    let tags_contract = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT is_nullable, column_default FROM information_schema.columns \
             WHERE table_schema = current_schema() \
               AND table_name = 'multipart_uploads' AND column_name = 'tags_json'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tags_contract.try_get::<String>("", "is_nullable").unwrap(), "NO");
    assert_eq!(
        tags_contract
            .try_get::<Option<String>>("", "column_default")
            .unwrap()
            .as_deref(),
        Some("'[]'::jsonb")
    );

    let upload = multipart::get_upload(&db, &upload_id).await.unwrap();
    assert_eq!(upload.metadata, metadata);
    assert!(
        ipfs_s3_gateway::store::pinning::tags::tags_from_json(&upload.tags_json)
            .unwrap()
            .is_empty()
    );
    multipart::upsert_part(
        &db,
        &upload_id,
        1,
        "bafy-six-mib",
        6_291_456,
        "bafy-six-mib",
    )
    .await
    .unwrap();
    let part = multipart::get_part(&db, &upload_id, 1).await.unwrap();
    assert_eq!((part.part_number, part.size), (1, 6_291_456));

    drop(db);
    admin
        .execute_unprepared(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
        .await
        .unwrap();
}
```

Run the dependency-free migration tests first:

```powershell
cargo test --lib postgres_json_columns -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL JSON migration SQL-shape tests failed" }
cargo test --lib postgres_json_columns_migration_is_registered_last -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Migration registration-order test failed" }
```

Then run the final five-test target against a second fresh disposable PostgreSQL 17 fixture using this complete block:

```powershell
$hadUrl = Test-Path Env:IPFS_S3_TEST_POSTGRES_URL
$oldUrl = [Environment]::GetEnvironmentVariable("IPFS_S3_TEST_POSTGRES_URL", "Process")
$hadPort = Test-Path Env:IPFS3_IMPORT_POSTGRES_PORT
$oldPort = [Environment]::GetEnvironmentVariable("IPFS3_IMPORT_POSTGRES_PORT", "Process")
$project = "ipfs3-pg-json-green-$PID-$([Guid]::NewGuid().ToString('N').Substring(0, 8))".ToLowerInvariant()
$owned = $false
$attempted = $false
$primary = $null
$cleanupErrors = [Collections.Generic.List[string]]::new()
try {
    try {
        $containers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($LASTEXITCODE -ne 0) { throw "GREEN fixture container preflight failed" }
        $networks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($LASTEXITCODE -ne 0) { throw "GREEN fixture network preflight failed" }
        $volumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($LASTEXITCODE -ne 0) { throw "GREEN fixture volume preflight failed" }
        if (($containers.Count + $networks.Count + $volumes.Count) -ne 0) { throw "BLOCKED: GREEN fixture project already owns resources; no cleanup attempted" }
        $occupied = @(Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $_.LocalPort -eq 55432 })
        if ($occupied.Count -ne 0) { throw "BLOCKED: GREEN fixture port 55432 is occupied; no cleanup attempted" }
        $owned = $true
        $env:IPFS3_IMPORT_POSTGRES_PORT = "55432"
        $env:IPFS_S3_TEST_POSTGRES_URL = "postgres://ipfs3:ipfs3@127.0.0.1:55432/ipfs3_import_test"
        $attempted = $true
        docker compose --project-name $project -f tests/compose.postgres-import.yml up --detach --wait --wait-timeout 120 postgres-import
        if ($LASTEXITCODE -ne 0) { throw "UNVERIFIED: GREEN PostgreSQL fixture did not become healthy" }
        cargo test --test postgres_import -- --nocapture --test-threads=1
        if ($LASTEXITCODE -ne 0) { throw "Five-test PostgreSQL target failed" }
    } catch {
        $primary = $_
    }
} finally {
    if ($owned -and $attempted) {
        docker compose --project-name $project -f tests/compose.postgres-import.yml logs --no-color postgres-import
        if ($LASTEXITCODE -ne 0) { $cleanupErrors.Add("GREEN fixture logs failed") }
        docker compose --project-name $project -f tests/compose.postgres-import.yml down --volumes --remove-orphans
        if ($LASTEXITCODE -ne 0) { $cleanupErrors.Add("GREEN fixture cleanup failed") }
        $remainingContainers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $remainingContainersExit = $LASTEXITCODE
        if ($remainingContainersExit -ne 0) { $cleanupErrors.Add("GREEN fixture container residual query failed with exit $remainingContainersExit") }
        $remainingNetworks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $remainingNetworksExit = $LASTEXITCODE
        if ($remainingNetworksExit -ne 0) { $cleanupErrors.Add("GREEN fixture network residual query failed with exit $remainingNetworksExit") }
        $remainingVolumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $remainingVolumesExit = $LASTEXITCODE
        if ($remainingVolumesExit -ne 0) { $cleanupErrors.Add("GREEN fixture volume residual query failed with exit $remainingVolumesExit") }
        if (@($remainingContainers + $remainingNetworks + $remainingVolumes).Count -ne 0) { $cleanupErrors.Add("GREEN fixture resources remain") }
    }
    if ($hadUrl) { [Environment]::SetEnvironmentVariable("IPFS_S3_TEST_POSTGRES_URL", $oldUrl, "Process") } else { [Environment]::SetEnvironmentVariable("IPFS_S3_TEST_POSTGRES_URL", $null, "Process") }
    if ($hadPort) { [Environment]::SetEnvironmentVariable("IPFS3_IMPORT_POSTGRES_PORT", $oldPort, "Process") } else { [Environment]::SetEnvironmentVariable("IPFS3_IMPORT_POSTGRES_PORT", $null, "Process") }
}
if ($null -ne $primary) { throw "GREEN PostgreSQL target failed: $($primary.Exception.Message); cleanup: $($cleanupErrors -join '; ')" }
if ($cleanupErrors.Count -ne 0) { throw "GREEN fixture cleanup failed: $($cleanupErrors -join '; ')" }
Write-Host "PostgreSQL JSON compatibility target: 5/5 PASSED"
```

Expected: the migration unit tests and registration test pass; the fresh PostgreSQL target reports five passed tests; the fifth test confirms old-schema decode RED, invalid-legacy rollback, new marker, all three `jsonb` types, both metadata columns still nullable, `tags_json` `NOT NULL` plus `'[]'::jsonb`, null-metadata/empty-tags round-trip, and part size `6291456`; logs precede cleanup. GREEN is reported only when all three residual queries exit zero and the independently computed non-empty resource count is zero. Query failures remain cleanup errors, are combined with `$primary` by the existing final report, and cannot masquerade as zero residual resources.

- [ ] **Step 5: Change workflow/static latest-schema expectations first and preserve contract RED**

In both `tests/release-validation.Tests.ps1` and `tests/postgres-production-baseline.Tests.ps1`, replace the current production marker expectation with these four fragments and explicitly forbid the superseded marker in the production job:

```powershell
foreach ($fragment in @(
    "m20260813_000001_postgres_json_columns",
    "multipart_uploads.metadata:jsonb",
    "multipart_uploads.tags_json:jsonb",
    "objects.metadata:jsonb"
)) {
    Assert-Contains $productionJob $fragment "Production JSON compatibility contract is missing: $fragment"
}
Assert-NotContains $productionJob "m20260730_000001_standard_mutation_fence" "Production workflow still accepts the superseded latest marker"
```

In `tests/release-validation.Tests.ps1`, change the ordered step-name fragment from `Verify latest migration and application role` to:

```powershell
"      - name: Verify latest migration, JSON columns, and application role"
```

Run:

```powershell
pwsh -NoProfile -File tests/release-validation.Tests.ps1
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
```

Expected RED: both scripts fail because the implemented workflow still contains only the superseded marker and two-row database contract. The strict numeric-core version parser assertions remain GREEN and must not be removed, relaxed, or regenerated.

- [ ] **Step 6: Update only the implemented workflow database-contract step and make both static contracts GREEN**

In `.github/workflows/release-validation.yml`, rename the existing step and replace only its `run` block with:

```yaml
      - name: Verify latest migration, JSON columns, and application role
        shell: pwsh
        run: |
          $databaseOutput = @(docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.postgres.yml -f tests/compose.postgres-production-validation.yml exec -T postgres psql -U postgres -d ipfs3 -tA -c "SELECT version FROM seaql_migrations WHERE version = 'm20260813_000001_postgres_json_columns'; SELECT rolname || ':' || rolsuper::text FROM pg_roles WHERE rolname = 'ipfs3'; SELECT table_name || '.' || column_name || ':' || data_type FROM information_schema.columns WHERE table_schema = current_schema() AND (table_name, column_name) IN (('objects', 'metadata'), ('multipart_uploads', 'metadata'), ('multipart_uploads', 'tags_json')) ORDER BY table_name, column_name;")
          if ($LASTEXITCODE -ne 0) { throw "Migration, application-role, and JSON-column query failed" }
          $databaseContract = @($databaseOutput | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
          $expectedDatabaseContract = @(
            "m20260813_000001_postgres_json_columns",
            "ipfs3:false",
            "multipart_uploads.metadata:jsonb",
            "multipart_uploads.tags_json:jsonb",
            "objects.metadata:jsonb"
          )
          if ($databaseContract.Count -ne $expectedDatabaseContract.Count) { throw "Unexpected database contract row count: $($databaseContract -join ', ')" }
          for ($index = 0; $index -lt $expectedDatabaseContract.Count; $index++) {
            if ($databaseContract[$index] -cne $expectedDatabaseContract[$index]) { throw "Database contract row $($index + 1) is incorrect: $($databaseContract[$index])" }
          }
```

Do not change the strict numeric-core Compose parser, job topology, environment values, fixed ports, E2E command, failure switch, diagnostics, cleanup, client static steps, or default SQLite E2E job. Then run:

```powershell
$scriptPaths = @(
    "tests/release-validation.Tests.ps1",
    "tests/postgres-production-baseline.Tests.ps1"
)
foreach ($scriptPath in $scriptPaths) {
    $tokens = $null
    $parseErrors = $null
    $null = [System.Management.Automation.Language.Parser]::ParseFile(
        (Resolve-Path $scriptPath),
        [ref]$tokens,
        [ref]$parseErrors
    )
    if ($parseErrors.Count -ne 0) { throw "$scriptPath parse failed: $($parseErrors | Out-String)" }
    pwsh -NoProfile -File $scriptPath
    if ($LASTEXITCODE -ne 0) { throw "$scriptPath contract failed" }
}
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Existing client-smoke infrastructure contract failed" }
```

Expected: both refreshed workflow contracts and the existing client-smoke contract pass. Static assertions require the new marker and the three deterministic `jsonb` rows, reject the old marker, retain exactly four independent blocking jobs, retain exactly three ordered client contract commands, and leave the default SQLite E2E job unchanged.

- [ ] **Step 7: Record the migration checkpoint without Git writes**

Run:

```powershell
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) { throw "Migration/test formatting check failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Library regression failed after migration registration" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Whitespace check failed" }
git diff -- src/store/migrations/m20260813_000001_postgres_json_columns.rs src/store/migrations/mod.rs src/store/mod.rs tests/postgres_import.rs .github/workflows/release-validation.yml tests/release-validation.Tests.ps1 tests/postgres-production-baseline.Tests.ps1
```

Expected: only the new migration, its module/registry wiring, the fifth PostgreSQL test, and focused workflow/static latest-schema changes appear. No existing migration, entity, `tests/e2e.rs`, parser, default Compose/config, or Dockerfile change is present. Do not stage or commit.

---

### Task 5: Rerun complete live PostgreSQL deployment, failure-switch, migration, E2E, and regression evidence

> **Runtime environment-restoration correction:** The first post-migration production run passed all behavior checks—four missing-secret failures, build/start, health/readiness, five database rows, E2E 11/11, PostgreSQL-stop 200/503 behavior, logs, cleanup, and zero residual resources—but the final verifier found that an originally absent `COMPOSE_PROJECT_NAME` had become present with an empty value. On Windows PowerShell, `[Environment]::SetEnvironmentVariable(name, $null, "Process")` leaves `Test-Path Env:name` true with an empty value, whereas `Remove-Item Env:name` restores true absence. That run is diagnostic only; the second PostgreSQL fixture and regressions did not run. After this correction, rerun Task 5 from Step 1 and do not reuse the prior live PASS lines for the documentation gate.

> **Runtime `.env` isolation correction:** A later fresh validation found that three removed required variables were rejected but removed `IPFS_S3_MASTER_KEY` was satisfied through the repository's ignored `.env`. The file's existence is sufficient evidence; validation must never read or output its contents. Default Compose `.env` loading is now disabled only in the hosted validation job and Task 5 production verifier. The prior product behavior PASS remains diagnostic, but missing-secret evidence is not 4/4 and cannot gate documentation until the fully isolated sequence is rerun.

**Files:**
- Verify: `docker-compose.postgres.yml`
- Verify: `tests/compose.postgres-production-validation.yml`
- Verify: `src/main.rs`
- Verify: `src/store/migrations/m20260813_000001_postgres_json_columns.rs`
- Verify: `src/store/migrations/mod.rs`
- Verify: `src/store/mod.rs`
- Verify: `.github/workflows/release-validation.yml`
- Verify: `tests/postgres_import.rs`
- Verify unchanged: `docker-compose.yml`
- Verify unchanged: `config.docker.toml`
- Evidence source only: concise command-emitted local evidence copied into the orchestrator's implementation handoff; do not create an in-repository evidence file. If a worker redirects a transcript to an OS-temp file for transport, it must read the concise evidence into the handoff and delete that exact temp file before Task 6.

**Interfaces:**
- Consumes: Tasks 1-4 GREEN Rust, Compose, migration, and static contracts; local Docker Compose v2; loopback ports `55433`, `55001`, and `59000`; `tests/e2e.rs` credentials `test`/`test`; existing live PG variable `IPFS_S3_TEST_POSTGRES_URL`.
- Produces: explicit LOCAL PASS/FAIL/UNVERIFIED records for the complete from-scratch workflow-parity production Compose interpolation with implicit `.env` disabled, healthy endpoints, latest migration plus three JSONB columns, serial E2E 11/11, PostgreSQL outage behavior, scoped cleanup, and exact ten-name environment restoration; a second fresh receipt for the five-test PostgreSQL target plus exact two-name environment restoration, binary/library/integration/fmt/Clippy regression, and unchanged SQLite workflow contract; and an explicit `HOSTED postgres-production-deployment: NOT RUN` statement for the initial implementation handoff.

- [ ] **Step 1: Prove exact Windows environment restoration without Docker, then run dependency-free and Rust readiness gates**

First run this deterministic no-Docker Windows PowerShell toggle. It demonstrates the legacy absent-state RED, then proves the corrected absent and case-sensitive present-value GREEN paths. A Windows environment entry with `Test-Path = $true` and value `""` is a real present-empty state; it is not treated as absent and no synthetic third state is invented.

```powershell
if (-not $IsWindows) { throw "The environment-restoration regression toggle must run in Windows PowerShell" }
$toggleName = "IPFS3_ENV_RESTORE_TOGGLE_${PID}_$([Guid]::NewGuid().ToString('N'))"
$togglePath = "Env:$toggleName"
if (Test-Path -LiteralPath $togglePath) { throw "Unique environment toggle name unexpectedly exists" }
try {
    [Environment]::SetEnvironmentVariable($toggleName, "temporary", "Process")
    [Environment]::SetEnvironmentVariable($toggleName, $null, "Process")
    $legacyPresent = Test-Path -LiteralPath $togglePath
    $legacyValue = [Environment]::GetEnvironmentVariable($toggleName, "Process")
    if (-not $legacyPresent -or $legacyValue -cne "") { throw "Expected Windows legacy RED semantics were not observed" }
    Write-Host "RED confirmed: SetEnvironmentVariable null produced present-empty instead of absence"

    Remove-Item -LiteralPath $togglePath -ErrorAction Stop
    if (Test-Path -LiteralPath $togglePath) { throw "Remove-Item did not restore absence" }
    if ($null -ne [Environment]::GetEnvironmentVariable($toggleName, "Process")) { throw "Removed environment variable retained a process value" }
    Write-Host "GREEN absent restoration: PASS"

    $savedPresentValue = "CaseSensitiveOriginal"
    [Environment]::SetEnvironmentVariable($toggleName, $savedPresentValue, "Process")
    [Environment]::SetEnvironmentVariable($toggleName, "temporary", "Process")
    [Environment]::SetEnvironmentVariable($toggleName, $savedPresentValue, "Process")
    if (-not (Test-Path -LiteralPath $togglePath)) { throw "Present-value restoration lost presence" }
    $restoredPresentValue = [Environment]::GetEnvironmentVariable($toggleName, "Process")
    if ($restoredPresentValue -cne $savedPresentValue) { throw "Present-value restoration changed case-sensitive value" }
    Write-Host "GREEN present-value restoration: PASS"
} finally {
    if (Test-Path -LiteralPath $togglePath) {
        Remove-Item -LiteralPath $togglePath -ErrorAction Stop
    }
    if ((Test-Path -LiteralPath $togglePath) -or $null -ne [Environment]::GetEnvironmentVariable($toggleName, "Process")) {
        throw "Environment toggle cleanup did not restore absence"
    }
}
```

Expected: one RED-confirmation line followed by both GREEN lines. The RED is an asserted observation of the Windows Env-provider bug, not a failing command; final toggle cleanup is absent. Then run sequentially and stop on the first failure:

```powershell
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Production baseline contract failed" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Release-validation contract failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Client-smoke contract failed" }
cargo test --bin ipfs-s3-gateway
if ($LASTEXITCODE -ne 0) { throw "Gateway binary tests failed" }
docker compose version
if ($LASTEXITCODE -ne 0) { throw "UNVERIFIED: Docker Compose v2 is unavailable" }
$composeVersionText = (docker compose version --short).Trim()
$composeVersionMatch = [regex]::Match($composeVersionText, '^v?(?<core>\d+\.\d+\.\d+)(?:[-+][0-9A-Za-z.-]+)?$')
$composeVersion = $null
if (-not $composeVersionMatch.Success -or -not [Version]::TryParse($composeVersionMatch.Groups["core"].Value, [ref]$composeVersion) -or $composeVersion -lt [Version]"2.23.1") { throw "UNVERIFIED: Docker Compose 2.23.1 or newer is required for inline configs.content" }
```

Expected: all three static contracts and binary tests pass; the workflow contracts require exact job-level `COMPOSE_DISABLE_ENV_FILE: "1"`, four-of-four missing-secret isolation, no `--env-file`, and exact restoration; Compose reports a v2 version. These local static passes prove the committed workflow shape only; they are not a GitHub-hosted job result. If Docker or its daemon is unavailable, preserve the exact output and mark every Docker-dependent line below `UNVERIFIED`; do not update README or ROADMAP.

- [ ] **Step 2: Run the production topology in a unique owned project**

Run this complete PowerShell block from the repository root. It is the initial documentation evidence gate: within one unique disposable project it executes, in the same order as `postgres-production-deployment`, the same four missing-secret `config --quiet` checks, full `config --quiet`, three-service `up --wait`, endpoint checks, exact `psql` query, serial E2E command, PostgreSQL-only stop/failure scenario, logs, `down --volumes --remove-orphans`, and residual-label checks. It sets the same environment-variable set and fixed loopback ports as the hosted job; only the unique project name and URL-safe disposable PostgreSQL password derive from the local process rather than GitHub run identity. It checks project labels and fixed ports before claiming ownership, saves and restores every related process environment variable, sets `attempted` immediately before `up`, captures logs before cleanup, never calls `--wait` after PostgreSQL stops, and queries only resources carrying the unique project label.

```powershell
$environmentNames = @(
    "COMPOSE_PROJECT_NAME",
    "COMPOSE_DISABLE_ENV_FILE",
    "POSTGRES_PASSWORD",
    "IPFS_S3_ACCESS_KEY_ID",
    "IPFS_S3_SECRET_ACCESS_KEY",
    "IPFS_S3_MASTER_KEY",
    "IPFS_S3_GATEWAY_BIND",
    "IPFS_S3_GATEWAY_PORT",
    "IPFS_S3_E2E_ENDPOINT",
    "IPFS_S3_E2E_KUBO_URL"
)
if ($environmentNames.Count -ne 10) { throw "Production environment snapshot must contain exactly ten names" }
$savedEnvironment = @{}
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [pscustomobject]@{
        Present = Test-Path -LiteralPath "Env:$name"
        Value = [Environment]::GetEnvironmentVariable($name, "Process")
    }
}

$project = "ipfs3-pg-local-$PID-$([Guid]::NewGuid().ToString('N').Substring(0, 8))".ToLowerInvariant()
$owned = $false
$attempted = $false
$primaryError = $null
$cleanupErrors = [Collections.Generic.List[string]]::new()
$localEvidence = [Collections.Generic.List[string]]::new()
$localEvidence.Add("LOCAL project=$project")
$localEvidence.Add("LOCAL command-order=config-missing-4,config-full,up-wait,health-ready,psql,e2e,stop-postgres,health-ready-failure,logs,down,residual")
$localEvidence.Add("LOCAL env/ports=COMPOSE_DISABLE_ENV_FILE:1;POSTGRES_PASSWORD:url-safe-disposable-redacted;IPFS_S3_ACCESS_KEY_ID:test;IPFS_S3_SECRET_ACCESS_KEY:test;IPFS_S3_MASTER_KEY:nonzero-64-hex-redacted;IPFS_S3_GATEWAY_BIND:127.0.0.1;IPFS_S3_GATEWAY_PORT:59000;IPFS_S3_E2E_ENDPOINT:http://127.0.0.1:59000;IPFS_S3_E2E_KUBO_URL:http://127.0.0.1:55001;POSTGRES_HOST_PORT:55433;KUBO_HOST_PORT:55001")

function Get-ProjectResourceIds {
    param([Parameter(Mandatory)][string]$Project)
    $containers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$Project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($LASTEXITCODE -ne 0) { throw "Container project query failed" }
    $networks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$Project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($LASTEXITCODE -ne 0) { throw "Network project query failed" }
    $volumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$Project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($LASTEXITCODE -ne 0) { throw "Volume project query failed" }
    return [pscustomobject]@{ Containers = $containers; Networks = $networks; Volumes = $volumes }
}

try {
    try {
        $existing = Get-ProjectResourceIds $project
        if (($existing.Containers.Count + $existing.Networks.Count + $existing.Volumes.Count) -ne 0) {
            throw "BLOCKED: unique project already owns resources; cleanup was not attempted: $project"
        }
        $listening = @(Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $_.LocalPort -in @(55433, 55001, 59000) })
        if ($listening.Count -ne 0) {
            throw "BLOCKED: fixed validation ports are occupied; cleanup was not attempted: $($listening.LocalPort -join ', ')"
        }
        $owned = $true

        $env:COMPOSE_PROJECT_NAME = $project
        $env:COMPOSE_DISABLE_ENV_FILE = "1"
        $validationPassword = "pg-local-$PID-$([Guid]::NewGuid().ToString('N').Substring(0, 8))"
        $env:POSTGRES_PASSWORD = $validationPassword
        $env:IPFS_S3_ACCESS_KEY_ID = "test"
        $env:IPFS_S3_SECRET_ACCESS_KEY = "test"
        $env:IPFS_S3_MASTER_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        $env:IPFS_S3_GATEWAY_BIND = "127.0.0.1"
        $env:IPFS_S3_GATEWAY_PORT = "59000"
        $env:IPFS_S3_E2E_ENDPOINT = "http://127.0.0.1:59000"
        $env:IPFS_S3_E2E_KUBO_URL = "http://127.0.0.1:55001"
        if ($env:COMPOSE_DISABLE_ENV_FILE -cne "1") { throw "COMPOSE_DISABLE_ENV_FILE must disable implicit .env loading" }
        $localEvidence.Add("LOCAL compose-env-isolation=PASS COMPOSE_DISABLE_ENV_FILE=1 implicit_dotenv=disabled env_file_override=none")

        $preConfigResources = Get-ProjectResourceIds $project
        if (($preConfigResources.Containers.Count + $preConfigResources.Networks.Count + $preConfigResources.Volumes.Count) -ne 0) {
            throw "Project resources appeared after ownership claim and before Compose config"
        }

        if ($env:POSTGRES_PASSWORD -notmatch '^[A-Za-z0-9._~-]+$') { throw "POSTGRES_PASSWORD is not URL-safe unreserved text" }
        if ($env:IPFS_S3_MASTER_KEY -notmatch '^[0-9A-Fa-f]{64}$') { throw "IPFS_S3_MASTER_KEY is not exactly 64 hexadecimal characters" }
        if ($env:IPFS_S3_GATEWAY_BIND -in @("", "0.0.0.0", "::", "[::]")) { throw "IPFS_S3_GATEWAY_BIND must be explicit and non-wildcard" }
        $gatewayPort = 0
        if (-not [int]::TryParse($env:IPFS_S3_GATEWAY_PORT, [ref]$gatewayPort) -or $gatewayPort -lt 1 -or $gatewayPort -gt 65535) { throw "IPFS_S3_GATEWAY_PORT must be decimal 1 through 65535" }

        foreach ($name in @(
            "POSTGRES_PASSWORD",
            "IPFS_S3_ACCESS_KEY_ID",
            "IPFS_S3_SECRET_ACCESS_KEY",
            "IPFS_S3_MASTER_KEY"
        )) {
            try {
                Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop
                if (Test-Path -LiteralPath "Env:$name") { throw "Missing-secret probe did not remove variable: $name" }
                if ($null -ne [Environment]::GetEnvironmentVariable($name, "Process")) { throw "Missing-secret probe retained process value: $name" }
                docker compose --project-name $project -f docker-compose.postgres.yml -f tests/compose.postgres-production-validation.yml config --quiet
                $missingExit = $LASTEXITCODE
            } finally {
                $env:POSTGRES_PASSWORD = $validationPassword
                $env:IPFS_S3_ACCESS_KEY_ID = "test"
                $env:IPFS_S3_SECRET_ACCESS_KEY = "test"
                $env:IPFS_S3_MASTER_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                $env:IPFS_S3_GATEWAY_BIND = "127.0.0.1"
                $env:IPFS_S3_GATEWAY_PORT = "59000"
                $env:IPFS_S3_E2E_ENDPOINT = "http://127.0.0.1:59000"
                $env:IPFS_S3_E2E_KUBO_URL = "http://127.0.0.1:55001"
            }
            if ($missingExit -eq 0) { throw "Compose accepted missing required variable: $name" }
            $localEvidence.Add("LOCAL config-missing-$name=PASS exit=$missingExit")
            $afterMissingConfig = Get-ProjectResourceIds $project
            if (($afterMissingConfig.Containers.Count + $afterMissingConfig.Networks.Count + $afterMissingConfig.Volumes.Count) -ne 0) {
                throw "Missing-secret Compose config created project resources"
            }
        }

        docker compose --project-name $project -f docker-compose.postgres.yml -f tests/compose.postgres-production-validation.yml config --quiet
        if ($LASTEXITCODE -ne 0) { throw "Complete production Compose configuration failed" }
        $localEvidence.Add("LOCAL config-full=PASS exit=0")
        $afterFullConfig = Get-ProjectResourceIds $project
        if (($afterFullConfig.Containers.Count + $afterFullConfig.Networks.Count + $afterFullConfig.Volumes.Count) -ne 0) {
            throw "Complete Compose config created project resources"
        }

        $attempted = $true
        docker compose --project-name $project -f docker-compose.postgres.yml -f tests/compose.postgres-production-validation.yml up --detach --build --wait --wait-timeout 300 postgres kubo gateway
        if ($LASTEXITCODE -ne 0) { throw "UNVERIFIED: production topology did not build or become healthy" }
        $localEvidence.Add("LOCAL compose-up-wait=PASS exit=0")

        $health = Invoke-WebRequest -Uri "http://127.0.0.1:59000/health" -TimeoutSec 5
        if ($health.StatusCode -ne 200 -or $health.Content -cne "OK") { throw "Unexpected liveness response" }
        $ready = Invoke-WebRequest -Uri "http://127.0.0.1:59000/ready" -TimeoutSec 5
        if ($ready.StatusCode -ne 200 -or $ready.Content -cne "READY") { throw "Unexpected readiness response" }
        $localEvidence.Add("LOCAL endpoints=PASS health=200/OK ready=200/READY")

        $databaseOutput = @(docker compose --project-name $project -f docker-compose.postgres.yml -f tests/compose.postgres-production-validation.yml exec -T postgres psql -U postgres -d ipfs3 -tA -c "SELECT version FROM seaql_migrations WHERE version = 'm20260813_000001_postgres_json_columns'; SELECT rolname || ':' || rolsuper::text FROM pg_roles WHERE rolname = 'ipfs3'; SELECT table_name || '.' || column_name || ':' || data_type FROM information_schema.columns WHERE table_schema = current_schema() AND (table_name, column_name) IN (('objects', 'metadata'), ('multipart_uploads', 'metadata'), ('multipart_uploads', 'tags_json')) ORDER BY table_name, column_name;")
        if ($LASTEXITCODE -ne 0) { throw "Migration, application-role, and JSON-column query failed" }
        $databaseContract = @($databaseOutput | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
        $expectedDatabaseContract = @(
            "m20260813_000001_postgres_json_columns",
            "ipfs3:false",
            "multipart_uploads.metadata:jsonb",
            "multipart_uploads.tags_json:jsonb",
            "objects.metadata:jsonb"
        )
        if ($databaseContract.Count -ne $expectedDatabaseContract.Count) { throw "Unexpected database contract rows: $($databaseContract -join ', ')" }
        for ($index = 0; $index -lt $expectedDatabaseContract.Count; $index++) {
            if ($databaseContract[$index] -cne $expectedDatabaseContract[$index]) { throw "Database contract row $($index + 1) is incorrect: $($databaseContract[$index])" }
        }
        $localEvidence.Add("LOCAL database=PASS migration=m20260813_000001_postgres_json_columns role=ipfs3:false jsonb=objects.metadata,multipart_uploads.metadata,multipart_uploads.tags_json")

        Write-Host "LOCAL command=cargo test --test e2e -- --nocapture --test-threads=1"
        $e2eOutput = @(& cargo test --test e2e -- --nocapture --test-threads=1 2>&1)
        $e2eExit = $LASTEXITCODE
        $e2eOutput | ForEach-Object { Write-Host $_ }
        if ($e2eExit -ne 0) { throw "PostgreSQL-backed E2E target failed" }
        $e2eSummary = $e2eOutput -join "`n"
        if (-not $e2eSummary.Contains("test result: ok. 11 passed; 0 failed", [StringComparison]::OrdinalIgnoreCase)) { throw "PostgreSQL-backed E2E did not report 11/11" }
        $localEvidence.Add("LOCAL e2e=PASS exit=0 tests=11/11 credentials=test/test")

        docker compose --project-name $project -f docker-compose.postgres.yml -f tests/compose.postgres-production-validation.yml stop postgres
        if ($LASTEXITCODE -ne 0) { throw "PostgreSQL stop failed" }
        $health = Invoke-WebRequest -Uri "http://127.0.0.1:59000/health" -TimeoutSec 5
        if ($health.StatusCode -ne 200 -or $health.Content -cne "OK") { throw "Liveness failed after PostgreSQL stopped" }
        $deadline = [DateTime]::UtcNow.AddSeconds(10)
        $observedNotReady = $false
        do {
            try {
                $notReady = Invoke-WebRequest -Uri "http://127.0.0.1:59000/ready" -TimeoutSec 3 -SkipHttpErrorCheck
                if ($notReady.StatusCode -eq 503 -and $notReady.Content -ceq "NOT READY") {
                    $observedNotReady = $true
                    break
                }
            } catch {
                Write-Host "Readiness has not yet returned the expected classified response"
            }
            Start-Sleep -Milliseconds 500
        } while ([DateTime]::UtcNow -lt $deadline)
        if (-not $observedNotReady) { throw "Readiness did not become 503 NOT READY within ten seconds" }
        $localEvidence.Add("LOCAL postgres-stop=PASS health=200/OK ready='503/NOT READY' deadline_seconds=10")
    } catch {
        $primaryError = $_
    }
} finally {
    if ($owned -and $attempted) {
        docker compose --project-name $project -f docker-compose.postgres.yml -f tests/compose.postgres-production-validation.yml logs --no-color postgres kubo gateway
        if ($LASTEXITCODE -ne 0) { $cleanupErrors.Add("diagnostic logs failed") }
        docker compose --project-name $project -f docker-compose.postgres.yml -f tests/compose.postgres-production-validation.yml down --volumes --remove-orphans
        if ($LASTEXITCODE -ne 0) { $cleanupErrors.Add("Compose down failed") }
        try {
            $residual = Get-ProjectResourceIds $project
            if (($residual.Containers.Count + $residual.Networks.Count + $residual.Volumes.Count) -ne 0) {
                $cleanupErrors.Add("project-labelled resources remain")
            } else {
                $localEvidence.Add("LOCAL cleanup=PASS containers=0 networks=0 volumes=0 logs_before_down=true")
            }
        } catch {
            $cleanupErrors.Add("residual-resource query failed: $($_.Exception.Message)")
        }
    }
    $environmentRestoreErrorCount = $cleanupErrors.Count
    foreach ($name in $environmentNames) {
        $saved = $savedEnvironment[$name]
        $environmentPath = "Env:$name"
        try {
            if ($saved.Present) {
                [Environment]::SetEnvironmentVariable($name, $saved.Value, "Process")
            } elseif (Test-Path -LiteralPath $environmentPath) {
                Remove-Item -LiteralPath $environmentPath -ErrorAction Stop
            }
            $restoredPresent = Test-Path -LiteralPath $environmentPath
            $restoredValue = [Environment]::GetEnvironmentVariable($name, "Process")
            if ($restoredPresent -ne $saved.Present) { throw "presence mismatch" }
            if ($saved.Present -and $restoredValue -cne $saved.Value) { throw "case-sensitive value mismatch" }
            if (-not $saved.Present -and $null -ne $restoredValue) { throw "absent variable retained a process value" }
        } catch {
            $cleanupErrors.Add("environment restoration failed for $name`: $($_.Exception.Message)")
        }
    }
    if ($cleanupErrors.Count -eq $environmentRestoreErrorCount) {
        $localEvidence.Add("LOCAL environment-restore=PASS names=10 presence_and_value=exact")
    }
}

$localEvidence | ForEach-Object { Write-Host $_ }
if ($null -ne $primaryError) {
    Write-Host "LOCAL result=FAIL primary=$($primaryError.Exception.Message) cleanup=$($cleanupErrors -join '; ')"
    throw "Primary production validation failed: $($primaryError.Exception.Message); cleanup: $($cleanupErrors -join '; ')"
}
if ($cleanupErrors.Count -ne 0) {
    Write-Host "LOCAL result=FAIL cleanup=$($cleanupErrors -join '; ')"
    throw "Production cleanup failed: $($cleanupErrors -join '; ')"
}
Write-Host "LOCAL result=PASS hosted=NOT_RUN"
Write-Host "production PostgreSQL topology live validation: PASSED"
```

Expected: `COMPOSE_DISABLE_ENV_FILE` is exact string `1` before every production `config`/`up` command, so no repository `.env` value can satisfy a removed required secret; the plan never reads, renames, deletes, or modifies `.env` and never supplies `--env-file` or a temporary env file. All four secret-removal config invocations fail; full config succeeds without printing secrets; three services become healthy; `/health` is `200 OK`, `/ready` is `200 READY`; the exact new migration row, `ipfs3:false`, and all three ordered `jsonb` rows are returned; E2E reports 11/11 with `test`/`test`; after only PostgreSQL stops, `/health` remains `200 OK` and `/ready` becomes `503 NOT READY` within ten seconds; logs precede `down`; no project-labelled container, network, or volume remains. All ten saved environment names, including `COMPOSE_DISABLE_ENV_FILE`, must independently recover their exact prior presence and case-sensitive value, producing `LOCAL environment-restore=PASS names=10 presence_and_value=exact`; any restore/remove/verification failure is aggregated in `$cleanupErrors`, combined with the primary failure, and blocks PASS. The final `LOCAL ...` lines are the concise workflow-parity evidence record: the orchestrator must copy them, the Cargo summary, the invoked command, and any exact failure into the durable implementation handoff. They are local Docker evidence only and must never be labelled as a GitHub-hosted job pass. A port, daemon, pull, build-network, environment-isolation, or environment-restoration failure is `UNVERIFIED`, not PASS. No cleanup runs if preflight finds an existing project or occupied fixed port.

- [ ] **Step 3: Rerun the five-test PostgreSQL target against its own fresh disposable fixture**

Use a second unique project and the repository's existing fixture, not the production database that the gateway used. Task 5 Step 2 restores `COMPOSE_DISABLE_ENV_FILE` before this step starts. This fixture does not own, set, or add it to its restoration count: its PostgreSQL URL and port are explicit process variables with higher precedence, so its environment ownership remains exactly those two names whether the caller's prior `COMPOSE_DISABLE_ENV_FILE` state was absent or present.

```powershell
$hadPostgresUrl = Test-Path -LiteralPath "Env:IPFS_S3_TEST_POSTGRES_URL"
$oldPostgresUrl = [Environment]::GetEnvironmentVariable("IPFS_S3_TEST_POSTGRES_URL", "Process")
$hadPostgresPort = Test-Path -LiteralPath "Env:IPFS3_IMPORT_POSTGRES_PORT"
$oldPostgresPort = [Environment]::GetEnvironmentVariable("IPFS3_IMPORT_POSTGRES_PORT", "Process")
$postgresProject = "ipfs3-pg-tests-$PID-$([Guid]::NewGuid().ToString('N').Substring(0, 8))".ToLowerInvariant()
$postgresOwned = $false
$postgresAttempted = $false
$postgresPrimary = $null
$postgresCleanup = [Collections.Generic.List[string]]::new()
try {
    try {
        $existingContainers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$postgresProject" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($LASTEXITCODE -ne 0) { throw "PostgreSQL test container preflight failed" }
        $existingNetworks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$postgresProject" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($LASTEXITCODE -ne 0) { throw "PostgreSQL test network preflight failed" }
        $existingVolumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$postgresProject" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($LASTEXITCODE -ne 0) { throw "PostgreSQL test volume preflight failed" }
        if (($existingContainers.Count + $existingNetworks.Count + $existingVolumes.Count) -ne 0) {
            throw "BLOCKED: PostgreSQL test project already owns resources; cleanup was not attempted"
        }
        $occupied = @(Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $_.LocalPort -eq 55432 })
        if ($occupied.Count -ne 0) { throw "BLOCKED: PostgreSQL test port 55432 is occupied; cleanup was not attempted" }
        $postgresOwned = $true
        $env:IPFS3_IMPORT_POSTGRES_PORT = "55432"
        $env:IPFS_S3_TEST_POSTGRES_URL = "postgres://ipfs3:ipfs3@127.0.0.1:55432/ipfs3_import_test"
        $postgresAttempted = $true
        docker compose --project-name $postgresProject -f tests/compose.postgres-import.yml up --detach --wait --wait-timeout 120 postgres-import
        if ($LASTEXITCODE -ne 0) { throw "UNVERIFIED: PostgreSQL import fixture did not become healthy" }
        $postgresOutput = @(& cargo test --test postgres_import -- --nocapture --test-threads=1 2>&1)
        $postgresExit = $LASTEXITCODE
        $postgresOutput | ForEach-Object { Write-Host $_ }
        if ($postgresExit -ne 0) { throw "Live PostgreSQL import target failed" }
        $postgresSummary = $postgresOutput -join "`n"
        if (-not $postgresSummary.Contains("test result: ok. 5 passed; 0 failed", [StringComparison]::OrdinalIgnoreCase)) { throw "PostgreSQL target did not report 5/5" }
    } catch {
        $postgresPrimary = $_
    }
} finally {
    if ($postgresOwned -and $postgresAttempted) {
        docker compose --project-name $postgresProject -f tests/compose.postgres-import.yml logs --no-color postgres-import
        if ($LASTEXITCODE -ne 0) { $postgresCleanup.Add("PostgreSQL test logs failed") }
        docker compose --project-name $postgresProject -f tests/compose.postgres-import.yml down --volumes --remove-orphans
        if ($LASTEXITCODE -ne 0) { $postgresCleanup.Add("PostgreSQL test cleanup failed") }
        $remainingContainers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$postgresProject" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($LASTEXITCODE -ne 0) { $postgresCleanup.Add("PostgreSQL test container residual query failed") }
        $remainingNetworks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$postgresProject" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($LASTEXITCODE -ne 0) { $postgresCleanup.Add("PostgreSQL test network residual query failed") }
        $remainingVolumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$postgresProject" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($LASTEXITCODE -ne 0) { $postgresCleanup.Add("PostgreSQL test volume residual query failed") }
        $remaining = @($remainingContainers + $remainingNetworks + $remainingVolumes) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        if ($remaining.Count -ne 0) { $postgresCleanup.Add("PostgreSQL test resources remain") }
    }
    foreach ($restore in @(
        [pscustomobject]@{ Name = "IPFS_S3_TEST_POSTGRES_URL"; Present = $hadPostgresUrl; Value = $oldPostgresUrl },
        [pscustomobject]@{ Name = "IPFS3_IMPORT_POSTGRES_PORT"; Present = $hadPostgresPort; Value = $oldPostgresPort }
    )) {
        $restorePath = "Env:$($restore.Name)"
        try {
            if ($restore.Present) {
                [Environment]::SetEnvironmentVariable($restore.Name, $restore.Value, "Process")
            } elseif (Test-Path -LiteralPath $restorePath) {
                Remove-Item -LiteralPath $restorePath -ErrorAction Stop
            }
            $restoredPresent = Test-Path -LiteralPath $restorePath
            $restoredValue = [Environment]::GetEnvironmentVariable($restore.Name, "Process")
            if ($restoredPresent -ne $restore.Present) { throw "presence mismatch" }
            if ($restore.Present -and $restoredValue -cne $restore.Value) { throw "case-sensitive value mismatch" }
            if (-not $restore.Present -and $null -ne $restoredValue) { throw "absent variable retained a process value" }
        } catch {
            $postgresCleanup.Add("PostgreSQL test environment restoration failed for $($restore.Name)`: $($_.Exception.Message)")
        }
    }
}
if ($null -ne $postgresPrimary) { throw "PostgreSQL target failed: $($postgresPrimary.Exception.Message); cleanup: $($postgresCleanup -join '; ')" }
if ($postgresCleanup.Count -ne 0) { throw "PostgreSQL target cleanup failed: $($postgresCleanup -join '; ')" }
Write-Host "LOCAL postgres_import=PASS tests=5/5 cleanup_containers=0 cleanup_networks=0 cleanup_volumes=0 logs_before_down=true environment_restore_names=2 presence_and_value=exact"
Write-Host "existing PostgreSQL target live validation: PASSED"
```

Expected: PostgreSQL 17 becomes healthy; all five tests execute rather than taking their missing-environment skip paths; all pass, including the isolated old-schema RED/new-schema GREEN JSON compatibility test and its 6 MiB part assertion; logs are collected before scoped disposable cleanup; no labelled resources remain. Both `IPFS_S3_TEST_POSTGRES_URL` and `IPFS3_IMPORT_POSTGRES_PORT` recover their exact prior presence and case-sensitive value. An originally absent name is removed only when currently present, then verified absent with a null process value; restoration failures join `$postgresCleanup`, combine with `$postgresPrimary`, and prevent the two-name PASS evidence.

- [ ] **Step 4: Run the complete non-Docker regression wave**

Run sequentially:

```powershell
cargo test --bin ipfs-s3-gateway
if ($LASTEXITCODE -ne 0) { throw "Binary tests failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Library tests failed" }
cargo test --test integration
if ($LASTEXITCODE -ne 0) { throw "Integration tests failed" }
cargo check --all-targets
if ($LASTEXITCODE -ne 0) { throw "All-target check failed" }
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) { throw "Formatting check failed" }
cargo clippy --all-targets -- -D warnings
if ($LASTEXITCODE -ne 0) { throw "Clippy failed" }
```

Expected: every command exits zero with no failed test, warning, formatting diff, or compile error. Do not add these commands to `release-validation.yml`; `.github/workflows/ci.yml` remains their owner.

- [ ] **Step 5: Record the evidence matrix and gate documentation**

Copy the concise `LOCAL ...` lines emitted by Step 2 and these status lines into the orchestrator's durable implementation handoff, not into a repository file. Include exact commands, exit codes/test summaries, and exact failure reasons; do not paste secrets or the full composed configuration. If command transport used an OS-temp transcript, delete that exact transcript after the handoff contains this concise record and verify it no longer exists.

```text
STATIC production baseline contract: PASS | FAIL
STATIC release-validation four-job contract: PASS | FAIL
STATIC existing client-smoke contract: PASS | FAIL
RUST binary readiness/probe tests: PASS | FAIL
POSTGRES JSON compatibility direct-store RED then fresh migration GREEN (5/5): PASS | FAIL | UNVERIFIED (<exact reason>)
LOCAL workflow-parity command/env/port sequence: PASS | FAIL | UNVERIFIED (<exact reason>)
LOCAL validation env isolation COMPOSE_DISABLE_ENV_FILE=1 implicit .env disabled/no --env-file: PASS | FAIL | UNVERIFIED (<exact reason>)
LOCAL COMPOSE missing required secrets (4/4 rejected): PASS | FAIL | UNVERIFIED (<exact reason>)
LOCAL LIVE production topology health/readiness/new-migration/three-JSONB/E2E-11-of-11: PASS | FAIL | UNVERIFIED (<exact reason>)
LOCAL LIVE PostgreSQL stopped health=OK ready=NOT READY: PASS | FAIL | UNVERIFIED (<exact reason>)
LOCAL LIVE production project cleanup/residual assertion: PASS | FAIL | UNVERIFIED (<exact reason>)
LOCAL LIVE existing postgres_import target (5 tests): PASS | FAIL | UNVERIFIED (<exact reason>)
LOCAL environment restoration production=10/10 postgres_fixture=2/2 exact presence/value: PASS | FAIL | UNVERIFIED (<exact reason>)
REGRESSION bin/lib/integration/check/fmt/clippy: PASS | FAIL
DEFAULT SQLite Compose/workflow unchanged: PASS | FAIL
HOSTED postgres-production-deployment: NOT RUN (initial implementation; required after push/PR and before merge/release)
```

Define local `PASS` as follows: retain the exact invoked commands and exit codes; confirm the production block was rerun from the beginning in one clean unique project with the same command order, environment-variable names, fixed ports, and PostgreSQL-stop scenario as the workflow job; retain exact `COMPOSE_DISABLE_ENV_FILE=1` evidence with no `--env-file` and four-of-four missing-secret rejection; retain the expected endpoint status/body pairs; retain the five exact database-contract rows (`m20260813_000001_postgres_json_columns`, `ipfs3:false`, and the three ordered `jsonb` rows); retain the E2E 11/11 and both PostgreSQL 5/5 summaries; retain exact prior presence/case-sensitive value restoration for all ten production names and both fixture names; and retain the zero counts for both projects' labelled containers, networks, and volumes. The documentation gate comprises all three `STATIC` lines, the Task 4 direct-store RED/migration GREEN line, the `LOCAL workflow-parity`/env-isolation/`LOCAL COMPOSE`/`LOCAL LIVE production` lines, the second fresh `LOCAL LIVE existing postgres_import` 5/5 line, and the 10/10 plus 2/2 environment-restoration line; every one must be `PASS`, and the hosted line must remain honestly `NOT RUN`, before Task 6 Steps 1-4 edit README or ROADMAP. `RUST binary`, `REGRESSION`, and `DEFAULT SQLite` remain mandatory final-acceptance evidence but do not substitute for or expand that documentation gate. Any documentation-gate `FAIL` or `UNVERIFIED` blocks README and ROADMAP edits; neither the restoration-mismatch run nor the three-of-four missing-secret run satisfies this gate. A hosted result is not part of this initial documentation gate because the job cannot run on GitHub until the change is pushed or submitted in a pull request. After push/PR, the actual hosted `postgres-production-deployment` job must pass before merge or release; local evidence must not be relabelled as that hosted result or used to weaken/bypass the blocking job.

---

### Task 6: Synchronize evidence-backed docs, run final boundaries, and hand off final review

**Files:**
- Modify conditionally after Task 5 from-scratch local workflow-parity Docker evidence, Task 4 compatibility RED/GREEN, and static contracts PASS: `README.md:20-38,162-187,294-317`
- Modify conditionally after Task 5 from-scratch local workflow-parity Docker evidence, Task 4 compatibility RED/GREEN, and static contracts PASS: `ROADMAP.md:65-70`
- Verify: every implementation file from Tasks 1-5
- Verify unchanged: `docker-compose.yml`
- Verify unchanged: `config.docker.toml`
- Verify unchanged: `Dockerfile`
- Verify: `src/store/mod.rs`
- Verify: `src/store/migrations/mod.rs`
- Verify: `src/store/migrations/m20260813_000001_postgres_json_columns.rs`
- Verify unchanged: every pre-existing `src/store/migrations/m*.rs`
- Verify unchanged: `src/store/entities/object.rs`
- Verify unchanged: `src/store/entities/multipart_upload.rs`
- Verify: `tests/postgres_import.rs`

**Interfaces:**
- Consumes: Task 4's direct-store RED/migration GREEN 5/5 receipt, Task 5's all-PASS documentation-gate lines (three static contracts, the from-scratch workflow-parity local production lines, and the second fresh PostgreSQL 5/5 target), explicit hosted `NOT RUN` status, and the exact production command `docker compose -f docker-compose.postgres.yml`; Task 5's remaining regression receipts stay mandatory for final review.
- Produces: locally evidence-bounded production operator instructions, exactly one checked roadmap item, final static/regression/diff receipts, an identity-bound final review handoff that distinguishes local from hosted evidence, and no Git write by any implementation worker.

- [ ] **Step 1: Prove the documentation starts RED only after local workflow-parity evidence is accepted**

After confirming every Task 4 compatibility line and every Task 5 documentation-gate line is `PASS`, and recording the hosted job as `NOT RUN`, run:

```powershell
$readme = [IO.File]::ReadAllText("README.md")
$roadmap = [IO.File]::ReadAllText("ROADMAP.md")
if ($readme.Contains("### PostgreSQL production baseline", [StringComparison]::Ordinal)) {
    throw "README already contains the production section; inspect interrupted work before editing"
}
if (-not $roadmap.Contains("- [ ] PostgreSQL production deployment", [StringComparison]::Ordinal)) {
    throw "The exact unchecked roadmap item is absent; stop for plan refresh"
}
```

Expected: no exception; the production section is absent and the exact roadmap item remains unchecked. If the local workflow-parity live Docker sequence or any static contract is not `PASS`, do not run this step and do not edit either file. Do not wait for or claim a GitHub-hosted pass before this initial documentation edit.

- [ ] **Step 2: Add production guidance without changing the default quick start**

Insert this section after the existing Docker Compose quick start and before `### Use with aws cli` in `README.md`:

````markdown
### PostgreSQL production baseline

`docker-compose.postgres.yml` is an explicit single-node production baseline:
one PostgreSQL 17 service, one Kubo service, and one gateway. The default
`docker-compose.yml` remains the SQLite development stack. PostgreSQL and Kubo
publish no host ports; only the gateway is published, and its host bind must be
an explicit non-wildcard address.

The PostgreSQL password is interpolated into a URL and must contain only
`A-Z`, `a-z`, `0-9`, `.`, `_`, `~`, or `-`. Keep the 64-hex-character master
key unchanged for the lifetime of encrypted data. Compose environment values
can be inspected by principals with Docker or host access, so restrict that
access and do not print `docker compose config` output into logs.

```powershell
$env:POSTGRES_PASSWORD = "replace-with-url-safe-password"
$env:IPFS_S3_ACCESS_KEY_ID = "replace-with-access-key"
$env:IPFS_S3_SECRET_ACCESS_KEY = "replace-with-secret-key"
$masterKey = [byte[]]::new(32)
[Security.Cryptography.RandomNumberGenerator]::Fill($masterKey)
$env:IPFS_S3_MASTER_KEY = [Convert]::ToHexString($masterKey).ToLowerInvariant()
$env:IPFS_S3_GATEWAY_BIND = "127.0.0.1"
$env:IPFS_S3_GATEWAY_PORT = "9000"

docker compose -f docker-compose.postgres.yml config --quiet
docker compose -f docker-compose.postgres.yml up --detach --build --wait --wait-timeout 300
```

`GET /health` is process liveness and returns `200 OK` with `OK` while the HTTP
server runs. `GET /ready` is database readiness and returns `200 OK` with
`READY` only when PostgreSQL responds; it returns `503 Service Unavailable`
with `NOT READY` after a database error or two-second timeout. Readiness does
not test Kubo or remote pinning providers.

Stop the production stack without deleting its named data volumes:

```powershell
docker compose -f docker-compose.postgres.yml down --remove-orphans
```

Never add `--volumes` to the production shutdown command: it deletes the
PostgreSQL and IPFS named volumes. This baseline does not provide multiple
gateways, migration leader election, PostgreSQL high availability, backups,
TLS, IPFS Cluster, a private swarm, a secret manager, or key rotation.
````

Also update the architecture diagram's health branch from:

```text
axum (HTTP :9000) ── /health ──► health_check
```

to:

```text
axum (HTTP :9000) ── /health ──► unconditional liveness
                  └─ /ready  ──► bounded database ping
```

Do not change the existing default `docker compose up -d --build` quick start, do not mention the validation override, do not publish PostgreSQL/Kubo ports, and do not present this baseline as multi-node or highly available.

- [ ] **Step 3: Check only the evidence-backed ROADMAP item**

In `ROADMAP.md`, make this one-line replacement:

```diff
-- [ ] PostgreSQL production deployment
+- [x] PostgreSQL production deployment
```

Keep these lines exactly unchecked:

```markdown
- [ ] Multiple gateway instances (horizontal scaling)
- [ ] IPFS Cluster for pinset replication
- [ ] Private swarm (swarm.key) for node-to-node communication
```

- [ ] **Step 4: Run exact documentation and destructive-command audits**

Run:

```powershell
$readme = [IO.File]::ReadAllText("README.md").Replace("`r`n", "`n").Replace("`r", "`n")
$roadmap = [IO.File]::ReadAllText("ROADMAP.md").Replace("`r`n", "`n").Replace("`r", "`n")
foreach ($required in @(
    "### PostgreSQL production baseline",
    "docker compose -f docker-compose.postgres.yml config --quiet",
    "docker compose -f docker-compose.postgres.yml up --detach --build --wait --wait-timeout 300",
    "docker compose -f docker-compose.postgres.yml down --remove-orphans",
    "GET /ready",
    "503 Service Unavailable",
    'Never add `--volumes`'
)) {
    if (-not $readme.Contains($required, [StringComparison]::Ordinal)) { throw "README production guidance is missing: $required" }
}
if ([regex]::IsMatch($readme, '(?m)^docker compose -f docker-compose\.postgres\.yml down .*--volumes')) {
    throw "README recommends destructive production volume deletion"
}
if ($readme.Contains("compose.postgres-production-validation.yml", [StringComparison]::Ordinal)) {
    throw "README exposes the CI-only override"
}
$checkedPostgres = [regex]::Matches($roadmap, '(?m)^- \[x\] PostgreSQL production deployment\s*$').Count
if ($checkedPostgres -ne 1) { throw "ROADMAP must contain one checked PostgreSQL production item" }
foreach ($unchecked in @(
    "- [ ] Multiple gateway instances (horizontal scaling)",
    "- [ ] IPFS Cluster for pinset replication",
    "- [ ] Private swarm (swarm.key) for node-to-node communication"
)) {
    if (-not $roadmap.Contains($unchecked, [StringComparison]::Ordinal)) { throw "Unrelated roadmap item changed: $unchecked" }
}
Write-Host "production documentation synchronization: PASSED"
```

Expected: exit zero and print `production documentation synchronization: PASSED`.

- [ ] **Step 5: Re-run all static and non-Docker regression gates after docs**

Run sequentially:

```powershell
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
pwsh -NoProfile -File tests/release-validation.Tests.ps1
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
cargo test --bin ipfs-s3-gateway
cargo test --lib
cargo test --test integration
cargo check --all-targets
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Expected: every command exits zero. Do not rerun Docker solely because README/ROADMAP changed; Task 5's from-scratch local workflow-parity live receipt remains valid because no runtime, migration, Compose, test, or workflow input changed after it.

- [ ] **Step 6: Enforce whitespace, exact file boundary, unchanged defaults, and no staging**

Run:

```powershell
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Git whitespace check failed" }

git diff --exit-code -- .debug-journal.md docker-compose.yml config.docker.toml Dockerfile tests/e2e.rs src/store/entities/object.rs src/store/entities/multipart_upload.rs
if ($LASTEXITCODE -ne 0) { throw "A forbidden default/runtime/entity/E2E file changed" }

$specPath = "docs/superpowers/specs/2026-08-13-postgresql-production-baseline-design.md"
$planPath = "docs/superpowers/plans/2026-08-13-postgresql-production-baseline.md"
$expectedSpecSha = "ce516e5056eb76f7e4859c40127aa252a0f3476d793238dc8fda89bb32c2c599"
$actualSpecSha = (Get-FileHash -Algorithm SHA256 -LiteralPath $specPath).Hash.ToLowerInvariant()
if ($actualSpecSha -cne $expectedSpecSha) { throw "Authoritative spec changed during implementation: $actualSpecSha" }
if (-not [IO.File]::Exists($planPath)) { throw "Authoritative implementation plan is missing" }

$allowedMigrationPaths = @(
    "src/store/migrations/m20260813_000001_postgres_json_columns.rs",
    "src/store/migrations/mod.rs"
)
$migrationDiff = @(git diff --name-only -- "src/store/migrations/*.rs")
$unexpectedMigrationDiff = @($migrationDiff | Where-Object { $allowedMigrationPaths -cnotcontains $_ })
if ($unexpectedMigrationDiff.Count -ne 0) { throw "Pre-existing migration source changed: $($unexpectedMigrationDiff -join ', ')" }

$allowedPaths = @(
    ".github/workflows/release-validation.yml",
    "README.md",
    "ROADMAP.md",
    "docker-compose.postgres.yml",
    "docs/superpowers/plans/2026-08-13-postgresql-production-baseline.md",
    "docs/superpowers/specs/2026-08-13-postgresql-production-baseline-design.md",
    "src/main.rs",
    "src/store/migrations/m20260813_000001_postgres_json_columns.rs",
    "src/store/migrations/mod.rs",
    "src/store/mod.rs",
    "tests/compose.postgres-production-validation.yml",
    "tests/postgres-production-baseline.Tests.ps1",
    "tests/postgres_import.rs",
    "tests/release-validation.Tests.ps1"
)
$changedPaths = @(
    git diff --name-only
    git diff --cached --name-only
    git ls-files --others --exclude-standard
) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique
$unexpected = @($changedPaths | Where-Object { $allowedPaths -cnotcontains $_ })
if ($unexpected.Count -ne 0) { throw "Out-of-scope changed paths: $($unexpected -join ', ')" }
foreach ($requiredPath in @(
    ".github/workflows/release-validation.yml",
    "README.md",
    "ROADMAP.md",
    "docker-compose.postgres.yml",
    "docs/superpowers/plans/2026-08-13-postgresql-production-baseline.md",
    "docs/superpowers/specs/2026-08-13-postgresql-production-baseline-design.md",
    "src/main.rs",
    "src/store/migrations/m20260813_000001_postgres_json_columns.rs",
    "src/store/migrations/mod.rs",
    "src/store/mod.rs",
    "tests/compose.postgres-production-validation.yml",
    "tests/postgres-production-baseline.Tests.ps1",
    "tests/postgres_import.rs",
    "tests/release-validation.Tests.ps1"
)) {
    if ($changedPaths -cnotcontains $requiredPath) { throw "Expected implementation path is absent: $requiredPath" }
}
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Implementation workers must not stage files" }
git status --short
```

Expected: no whitespace error; no debug-journal, default Compose/config, Dockerfile, entity, E2E, pre-existing migration, or spec-content diff; the spec hash remains exactly `ce516e5056eb76f7e4859c40127aa252a0f3476d793238dc8fda89bb32c2c599`; changed paths are exactly within the allowlist; every implementation path plus the untracked design/plan artifacts is present; the index is clean. The plan may differ from its planning-time hash only through orchestrator-owned checkbox progress; if execution does not track checkboxes in-place, implementation agents make no plan edit.

- [ ] **Step 7: Perform self-review and hand off an identity-bound final review**

Before handoff, inspect the complete current diff and verify all of these points explicitly:

1. Every acceptance criterion in the authoritative spec maps to a task and a passing evidence line.
2. The plan and implementation contain no placeholders, incomplete code blocks, contradictory port numbers, inconsistent function names, or vague error-handling instructions; the client static job has exactly three blocking commands in the required order with no recursive test invocation.
3. Rust signatures are consistent: `ready_handler` uses `State<Arc<AppState>>`; `readiness_response` accepts `Future<Output = Result<(), DbErr>>`; `ready_probe_url` uses one total deadline and exact status/body checks.
4. YAML names, ports, schema rows, and counts are consistent everywhere: job `postgres-production-deployment`; project prefix `ipfs3-pg`; exact validation isolation `COMPOSE_DISABLE_ENV_FILE="1"`; PostgreSQL `55433`; Kubo `55001`; gateway `59000`; credentials `test`/`test`; latest migration `m20260813_000001_postgres_json_columns`; exactly three JSONB rows; E2E 11/11; PostgreSQL target 5/5; environment restoration 10/10 plus 2/2.
5. Every multiline GitHub Actions command declares `shell: pwsh`; static scripts parse with PowerShell's AST parser; no Bash-only token appears.
6. File boundaries are focused; Rust changes are limited to `src/main.rs`, the new migration, its module/registry wiring, and the fifth PostgreSQL integration test; no dependency, entity, pre-existing migration, default Compose, development config, `tests/e2e.rs`, or Dockerfile change exists; the unchanged spec and authoritative plan are present in the final manifest.
7. Logs precede cleanup; cleanup is project-labelled and guarded by ownership/attempt state; fixed-resource refusal performs no cleanup; no broad prune/delete command exists.
8. Production documentation contains no `down --volumes`; the CI-only override is absent; exactly one ROADMAP box changed.
9. The Task 5 production rerun started from a clean unique disposable project and used the same commands, environment-variable names, fixed ports, database/JSONB assertions, and PostgreSQL-stop scenario as the hosted job; the concise handoff marks each result `LOCAL` and explicitly records `HOSTED postgres-production-deployment: NOT RUN`.
10. The no-Docker Windows toggle confirms the present-empty legacy RED and both corrected GREEN paths; the production verifier proves exact prior presence/case-sensitive value for all ten names, the separate fixture proves both names, and the workflow static contracts reject null-based removal while enforcing `Remove-Item` plus post-restore verification.
11. Hosted and local production validation set exact `COMPOSE_DISABLE_ENV_FILE="1"`, reject every alternate or missing value and `--env-file`, and prove all four missing required variables fail without reading, renaming, deleting, modifying, or printing repository `.env`; the separate PostgreSQL fixture remains a two-variable shell-precedence fixture after production restoration completes.

Create the review identity without changing files:

```powershell
$head = git rev-parse HEAD
$reviewFiles = @(
    ".github/workflows/release-validation.yml",
    "README.md",
    "ROADMAP.md",
    "docker-compose.postgres.yml",
    "docs/superpowers/plans/2026-08-13-postgresql-production-baseline.md",
    "docs/superpowers/specs/2026-08-13-postgresql-production-baseline-design.md",
    "src/main.rs",
    "src/store/migrations/m20260813_000001_postgres_json_columns.rs",
    "src/store/migrations/mod.rs",
    "src/store/mod.rs",
    "tests/compose.postgres-production-validation.yml",
    "tests/postgres-production-baseline.Tests.ps1",
    "tests/postgres_import.rs",
    "tests/release-validation.Tests.ps1"
)
$hashes = $reviewFiles | ForEach-Object {
    $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $_).Hash.ToLowerInvariant()
    "$hash  $_"
}
Write-Host "HEAD $head"
$hashes | Write-Host
```

Return the HEAD, hashes, exact final submission manifest, changed paths, Task 4 direct-store RED/migration GREEN receipt, Task 5 concise local evidence matrix, explicit `HOSTED postgres-production-deployment: NOT RUN` status, final regression results, and boundary receipts to the orchestrator. The final submission manifest includes every implementation path listed in `$reviewFiles` plus `docs/superpowers/specs/2026-08-13-postgresql-production-baseline-design.md` and `docs/superpowers/plans/2026-08-13-postgresql-production-baseline.md`; it therefore includes the new migration, migration module/registry files, and `tests/postgres_import.rs`. The orchestrator owns the one final acceptance review for this exact identity. Any edit after hashing invalidates that review identity and requires fresh hashes plus another final review. After an authorized push or pull request, the hosted job's actual PASS is a separate merge/release gate and must replace `NOT RUN` only when GitHub reports that result.

No implementation subagent may stage or commit. After final review approves the exact identity, only the orchestrator may perform a unified commit, and only if the user explicitly authorizes that Git write at that time; no push, tag, or release is authorized by this plan.

## Verification Waves and Acceptance Boundary

1. **Wave 1—Rust TDD:** binary tests fail on missing readiness/probe interfaces, then pass with exact redacted readiness and bounded probe behavior.
2. **Wave 2—Compose TDD:** the static production contract fails while production files are absent, then passes with exact three-service topology and disposable override.
3. **Wave 3—workflow TDD:** both static workflow contracts fail on the missing fourth job and missing third client static command, then pass after the production job and blocking sibling contract step are added without changing the existing SQLite E2E job; focused runtime corrections add RED on null-based environment removal and implicit `.env` fallback, then GREEN on `Remove-Item`, exact post-restore presence/value checks, exact job-level `COMPOSE_DISABLE_ENV_FILE="1"`, four-of-four rejection, and no `--env-file` without changing job topology.
4. **Wave 4—PostgreSQL compatibility TDD:** one fresh PostgreSQL 17 fixture proves the direct-store old-schema JSON/TEXT decode RED while the four prior tests stay green; SQL-shape and registration tests then drive the PostgreSQL-only fail-closed transactional migration, and a second fresh fixture proves five-of-five GREEN, three `jsonb` columns, metadata/tags round-trip, and the 6 MiB part.
5. **Wave 5—from-scratch local live Docker:** after the no-Docker Windows restoration toggle and workflow/static latest-schema contracts are green, one new unique disposable production project disables implicit `.env`, proves four-of-four required-secret rejection, and reruns the hosted job's full command order, ten-variable environment set, fixed ports, five database rows, E2E 11/11, and PostgreSQL-stop scenario from the beginning, with logs-first cleanup, zero residual resources, and exact restoration of all ten names; a separate new fixture reruns all five PostgreSQL tests and restores only its two explicit shell variables. Its concise durable evidence is labelled `LOCAL`, never hosted.
6. **Wave 6—local-evidence-gated docs and regression:** README/ROADMAP change only after the compatibility RED/GREEN receipt, every from-scratch local workflow-parity live line, and every static contract passes while hosted status remains `NOT RUN`; static, binary, library, integration, check, format, Clippy, whitespace, file-boundary, and no-staging gates then pass.
7. **Wave 7—final review handoff:** one exact HEAD/file-hash identity, its compatibility receipt, concise local evidence, and explicit hosted `NOT RUN` status go to the orchestrator; any later edit invalidates the identity. The actual hosted job PASS follows an authorized push/PR and is required before merge/release.

Initial implementation acceptance requires every local/static/Rust/regression/default evidence line—including env isolation, missing secrets 4/4, and restoration 10/10 plus 2/2—to be PASS, the hosted status to be reported as `NOT RUN`, the GitHub `postgres-production-deployment` job to remain independent and blocking, and the existing SQLite `e2e` job to remain unchanged and blocking. An unavailable local Docker surface or implicit `.env` fallback is explicitly `UNVERIFIED` and blocks documentation synchronization and initial acceptance. After push or pull-request creation, the actual hosted job PASS is required before merge or release, not before the initial implementation commit.

## Risks and Assumptions

- The approved spec cites SeaORM 1.1.14 for the `ping` API while the current lockfile resolves 1.1.20; both expose the same `DatabaseConnection`/`ConnectionTrait` ping contract. No dependency file should change.
- The pre-revision PostgreSQL schema stores three entity JSON fields as `TEXT`, which SeaORM/SQLx does not decode as `Json` on PostgreSQL. The new migration is intentionally PostgreSQL-only, prevalidates every non-null value without reporting content, preserves nullability/`NOT NULL`/the logical empty-array default, and relies on SeaORM's PostgreSQL transaction boundary so a failed conversion records no marker and changes no column.
- Compose merge treats the base and override gateway mapping as the same unique mapping only when `IPFS_S3_GATEWAY_BIND=127.0.0.1` and `IPFS_S3_GATEWAY_PORT=59000`; both CI and local live blocks force those values.
- Required interpolation rejects absent/empty values but cannot itself validate the allowed PostgreSQL password alphabet, master-key syntax, non-wildcard bind, or gateway port range. The CI/local PowerShell preflight validates all four before Docker resource creation, and README states the operator contract; adding a separate application-level Compose validator is outside this baseline.
- Docker Compose normally loads a project `.env` when no `--env-file` is supplied. Hosted and local production validation set and verify `COMPOSE_DISABLE_ENV_FILE="1"` so missing-secret probes cannot consume ignored local values; they never inspect the file. This is validation-only: `docker-compose.postgres.yml` remains unchanged and operators running it outside the validation job may continue to supply their own `.env` under normal Compose precedence.
- The runtime release build rejects an all-zero master key, so CI/live validation uses a nonzero 64-hex test value while existing E2E credentials remain exactly `test`/`test`.
- `/ready` intentionally checks only the database. Kubo failure after startup does not make database readiness fail.
- The inline `configs.content` role bootstrap requires Docker Compose 2.23.1 or newer; both CI and local live validation fail closed on an older version.
- Static text contracts intentionally reject semantically equivalent YAML that weakens reviewability. Any topology or workflow refactor must update the contract and receive a fresh review.
- Local fixed ports can be occupied by non-Docker processes. The Windows PowerShell preflight refuses before ownership; it never kills a process or deletes an existing resource.
- Windows PowerShell distinguishes a truly absent Env-provider entry from a present entry whose value is empty. The plan snapshots `Test-Path` and `GetEnvironmentVariable` separately, uses `Remove-Item -ErrorAction Stop` only when restoring prior absence from a currently present entry, and verifies both dimensions; a present-empty prior value stays on the present-value branch rather than becoming a fabricated third state.
- The local workflow-parity all-PASS live wave plus green static workflow contracts authorizes documentation work in this initial implementation flow. This evidence is not a GitHub-hosted PASS; the initial report says the hosted job has not run, and the new job remains a blocking post-push/PR merge/release requirement that cannot be bypassed.

## Plan Review Status

- Receipt: `waiting for receipt`
- Review owner: orchestrator
- Any edit to this plan invalidates a later receipt and requires review of the complete current revision.
