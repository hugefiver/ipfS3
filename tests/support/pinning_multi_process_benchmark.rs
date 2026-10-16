//! OS process ownership/configuration helpers for the standalone Stage 8 leaf.

#[path = "pinning_multi_process_benchmark_fixture.rs"]
mod fixture;
#[path = "pinning_multi_process_benchmark_http.rs"]
pub(super) mod http;
#[path = "pinning_multi_process_benchmark_metrics.rs"]
pub(super) mod metrics;

use ipfs_s3_gateway::{
    config::Config,
    pinning::{
        config::ValidatedPinningConfig,
        coordinator::{PinningCoordinator, normalize_validated_config},
    },
    state::AppState,
    store::{self, Store},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

pub type BenchResult<T> = Result<T, &'static str>;
pub const BUCKET: &str = "stage8-offline-benchmark";
pub const RPS: u32 = 4;
pub const W: usize = 3;
const OPT_IN: &str = "IPFS_S3_PINNING_BENCHMARK";
const SECONDS: &str = "IPFS_S3_PINNING_BENCHMARK_SECONDS";
const CHILD_TEST: &str = "pinning_multi_process_benchmark_child";
const DIAGNOSTIC: &str = "IPFS_S3_PINNING_BENCHMARK_DIAGNOSTIC";
const MODE: &str = "IPFS_S3_PINNING_BENCHMARK_MODE";

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Case {
    pub name: &'static str,
    pub processes: usize,
    pub providers: usize,
    pub outputs: usize,
    pub distinct: usize,
    pub seconds: u64,
    pub diagnostic: bool,
    pub mixed: bool,
}

impl Case {
    pub fn formal() -> Self {
        Self {
            name: "formal",
            processes: 2,
            providers: 1,
            outputs: 65,
            distinct: 33,
            seconds: 60,
            diagnostic: false,
            mixed: false,
        }
    }
    pub fn named(name: &str) -> BenchResult<Self> {
        let mut case = Self::formal();
        match name {
            "single-process" => {
                case.name = "single-process";
                case.processes = 1;
            }
            "dual-provider-65" | "dual-provider-10k" => {
                case.name = if name == "dual-provider-65" {
                    "dual-provider-65"
                } else {
                    "dual-provider-10k"
                };
                case.providers = 2;
                case.mixed = true;
                if name == "dual-provider-10k" {
                    case.outputs = 10_000;
                    case.distinct = 5_000;
                    case.seconds = 120;
                }
            }
            "duplicate-cids" | "unique-cids" | "equal-duplicate-cids" | "equal-unique-cids" => {
                case.name = match name {
                    "duplicate-cids" => "duplicate-cids",
                    "unique-cids" => "unique-cids",
                    "equal-duplicate-cids" => "equal-duplicate-cids",
                    _ => "equal-unique-cids",
                };
                case.diagnostic = true;
                case.seconds = 25;
                (case.outputs, case.distinct) = match name {
                    "duplicate-cids" => (17, 9),
                    "unique-cids" => (9, 9),
                    "equal-duplicate-cids" => (65, 33),
                    _ => (65, 65),
                };
            }
            _ => return Err("unknown bounded benchmark mode"),
        }
        Ok(case)
    }
    pub fn expected_allocations(self) -> usize {
        (self.distinct + 6 + usize::from(self.mixed)) * self.providers
    }
}

pub struct Outcome {
    pub report: Value,
    pub failure: Option<&'static str>,
}

async fn within_budget<T>(
    case: Case,
    deadline: Instant,
    future: impl std::future::Future<Output = BenchResult<T>>,
) -> BenchResult<T> {
    if case.mixed {
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
            .await
            .map_err(|_| "fixed mixed publication/observation budget exhausted")?
    } else {
        future.await
    }
}

fn options() -> BenchResult<u64> {
    if std::env::var(OPT_IN).ok().as_deref() != Some("offline-sqlite") {
        return Err("explicit offline-sqlite opt-in is required");
    }
    let seconds = std::env::var(SECONDS)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| (15..=120).contains(n))
        .ok_or("explicit observation seconds must be 15..120")?;
    Ok(seconds)
}

#[derive(Serialize, Deserialize)]
struct Ready {
    pid: u32,
    port: u16,
    allocation: String,
    concurrency: usize,
    min_interval_us: u64,
    provider_limits: Vec<Value>,
    start_identity: String,
}

/// Own only the exact spawned Child handles, including their launch nonce. Drop is synchronous kill AND wait,
/// including early errors/panics; no process-name discovery or batch termination.
#[derive(Default)]
struct Processes(Vec<Child>, Vec<String>);
impl Processes {
    fn spawn(
        &mut self,
        root: &Path,
        run: &str,
        role: usize,
        seconds: u64,
        diagnostic: bool,
        providers: usize,
    ) -> BenchResult<()> {
        let mut command =
            Command::new(std::env::current_exe().map_err(|_| "test executable unavailable")?);
        let start_identity = uuid::Uuid::new_v4().to_string();
        command
            .args(["--ignored", "--exact", CHILD_TEST, "--test-threads=1"])
            .env_clear()
            .env(OPT_IN, "offline-sqlite")
            .env(SECONDS, seconds.to_string())
            .env("STAGE8_BENCHMARK_ROOT", root)
            .env("STAGE8_BENCHMARK_RUN", run)
            .env("STAGE8_BENCHMARK_ROLE", role.to_string())
            .env("STAGE8_BENCHMARK_PROVIDERS", providers.to_string())
            .env("STAGE8_BENCHMARK_START_IDENTITY", &start_identity)
            .env(
                "STAGE8_FIXTURE_TOKEN",
                format!("offline-fixture-process-{role}"),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if diagnostic {
            command.env("STAGE8_CHILD_DIAGNOSTIC", "bounded");
        }
        // Only OS runtime essentials, never ambient account credentials/proxies.
        for name in ["SystemRoot", "WINDIR", "TEMP", "TMP", "TMPDIR"] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        self.0
            .push(command.spawn().map_err(|_| "OS child launch failed")?);
        self.1.push(start_identity);
        Ok(())
    }
    fn alive(&mut self) -> BenchResult<()> {
        for child in &mut self.0 {
            if child
                .try_wait()
                .map_err(|_| "OS child status unavailable")?
                .is_some()
            {
                return Err("OS worker child exited before completion");
            }
        }
        Ok(())
    }
    fn kill_wait(&mut self) -> (bool, usize) {
        let mut ok = true;
        let mut killed = 0;
        for child in &mut self.0 {
            match child.try_wait() {
                Ok(Some(_)) => {}
                _ => {
                    if child.kill().is_ok() {
                        killed += 1;
                    } else if child.try_wait().ok().flatten().is_none() {
                        ok = false;
                    }
                }
            }
            if child.wait().is_err() {
                ok = false;
            }
        }
        (ok, killed)
    }
    async fn stop(&mut self, root: &Path) -> (bool, bool, usize) {
        let marker = tokio::fs::write(root.join("stop"), b"stop").await.is_ok();
        let deadline = Instant::now() + Duration::from_secs(8);
        let mut graceful = false;
        while Instant::now() < deadline {
            let mut all_success = true;
            let mut all_exited = true;
            for child in &mut self.0 {
                match child.try_wait() {
                    Ok(Some(status)) => all_success &= status.success(),
                    _ => all_exited = false,
                }
            }
            if all_exited {
                graceful = all_success;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let (waited, killed) = self.kill_wait();
        (waited && marker, graceful, killed)
    }
}
impl Drop for Processes {
    fn drop(&mut self) {
        let _ = self.kill_wait();
    }
}

fn quote(value: &str) -> String {
    serde_json::to_string(value).unwrap()
}

fn configuration(
    root: &Path,
    run: &str,
    port: u16,
    case: Case,
) -> BenchResult<(String, Arc<PinningCoordinator>)> {
    // ONLY a newly owned file SQLite; deliberately no caller-supplied PG/DB URL.
    let path = root
        .join("ledger.sqlite")
        .to_string_lossy()
        .replace('\\', "/");
    let raw = format!(
        r#"
        [kubo]
        rpc_url = 'http://127.0.0.1:{port}'
        [storage]
        database_url = {}
        [crypto]
        master_key = '{}'
        [auth]
        [[auth.credentials]]
        access_key = 'test'
        secret_key = 'test'
        [decompress_zip]
        unixfs_directory_root = false
        [pinning]
        worker_interval = '1s'
        worker_concurrency = {W}
        [[pinning.providers]]
        name = 'offline-account'
        kind = 'filebase'
        endpoint = 'http://127.0.0.1:{port}/v1/ipfs'
        token_env = 'STAGE8_FIXTURE_TOKEN'
        priority = 1
        max_pins = 100
        max_bytes = 10000
        requests_per_second = {RPS}
        [[pinning.policies]]
        bucket = '{BUCKET}'
        prefix = ''
        trigger = 'always'
        provider_mode = 'all'
        providers = ['offline-account']
        default_duration = '1h'
        max_duration = '2h'
        allow_decompressed = true
        [pinning_identity]
        primary_storage_domain = 'offline-primary'
        [[pinning_identity.providers]]
        config_name = 'offline-account'
        provider_id = 'offline-account'
        display_name = 'Offline shared account'
        backend = 'filebase'
        scope = 'offline:{run}'
        storage_domain = 'offline-shared-remote'
        credential_revision = 1
        endpoint_revision = 1
        secret_ref = 'env:STAGE8_FIXTURE_TOKEN'
        api_profile = 'filebase-psa'
        strategy = 'cid'
        cleanup = 'managed'
    "#,
        quote(&format!("sqlite://{path}?mode=rwc")),
        "6d".repeat(32)
    );
    let raw = if case.providers == 2 {
        format!(
            "{}\n[[pinning.providers]]\nname = 'offline-replica'\nkind = 'filebase'\nendpoint = 'http://127.0.0.1:{port}/replica/v1/ipfs'\ntoken_env = 'STAGE8_FIXTURE_TOKEN'\npriority = 2\nmax_pins = 6000\nmax_bytes = 192000\nrequests_per_second = {RPS}\n[[pinning_identity.providers]]\nconfig_name = 'offline-replica'\nprovider_id = 'offline-replica'\ndisplay_name = 'Offline replica account'\nbackend = 'filebase'\nscope = 'offline-replica:{run}'\nstorage_domain = 'offline-replica-remote'\ncredential_revision = 1\nendpoint_revision = 1\nsecret_ref = 'env:STAGE8_FIXTURE_TOKEN'\napi_profile = 'filebase-psa'\nstrategy = 'cid'\ncleanup = 'managed'\n",
            raw.replace(
                "providers = ['offline-account']",
                "providers = ['offline-account', 'offline-replica']"
            )
            .replace("max_pins = 100", "max_pins = 6000")
            .replace("max_bytes = 10000", "max_bytes = 192000")
        )
    } else {
        raw
    };
    let config: Config = toml::from_str(&raw).map_err(|_| "fixture configuration invalid")?;
    let validated = normalize_validated_config(
        ValidatedPinningConfig::from_config(&config, |_| Some("offline-fixture-seed".into()))
            .map_err(|_| "fixture provider validation failed")?,
    )
    .map_err(|_| "fixture allocation normalization failed")?;
    let raw = format!(
        "{raw}\n[[decompress_zip.pin_output_rules]]\nname = 'stage8-bulk'\npriority = 1\nbucket = '{BUCKET}'\nprefix = 'bulk/'\neffect = 'allow'\npolicy_id = {}\n",
        quote(&validated.policies[0].identity)
    );
    let pinning = PinningCoordinator::build(validated)
        .map_err(|_| "fixture production coordinator failed")?;
    Ok((raw, pinning))
}

async fn ready(root: &Path, role: usize, processes: &mut Processes) -> BenchResult<Ready> {
    let path = root.join(format!("ready-{role}.json"));
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        processes.alive()?;
        // An incomplete write is just not ready yet; never print file contents.
        if let Ok(bytes) = tokio::fs::read(&path).await
            && let Ok(ready) = serde_json::from_slice(&bytes)
        {
            return Ok(ready);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err("production AppState/S3 child readiness deadline reached")
}

pub async fn child() -> BenchResult<()> {
    let seconds = options()?;
    let provider_count = std::env::var("STAGE8_BENCHMARK_PROVIDERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| (1..=2).contains(v))
        .ok_or("child provider count is not parent-authorized")?;
    let role = std::env::var("STAGE8_BENCHMARK_ROLE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v < 2)
        .ok_or("child role is not parent-authorized")?;
    let supplied_root = PathBuf::from(
        std::env::var_os("STAGE8_BENCHMARK_ROOT").ok_or("parent run directory missing")?,
    );
    let root = supplied_root
        .canonicalize()
        .map_err(|_| "parent run directory invalid")?;
    let temp = std::env::temp_dir()
        .canonicalize()
        .map_err(|_| "OS temporary parent unavailable")?;
    let run = std::env::var("STAGE8_BENCHMARK_RUN")
        .ok()
        .filter(|v| uuid::Uuid::parse_str(v).is_ok())
        .ok_or("parent run identity missing")?;
    if root.parent() != Some(temp.as_path())
        || !root
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with("stage8-pinning-"))
        || tokio::fs::read(root.join("owner"))
            .await
            .map_err(|_| "parent owner marker unavailable")?
            != run.as_bytes()
    {
        return Err("parent ownership fence failed");
    }
    let raw = tokio::fs::read_to_string(root.join("config.toml"))
        .await
        .map_err(|_| "private fixture configuration unavailable")?;
    let config: Config =
        toml::from_str(&raw).map_err(|_| "private fixture configuration invalid")?;
    // Preserve the parent's lexical SQLite spelling: Windows canonical paths
    // have a verbatim prefix, which is not part of the generated SQLite URL.
    let database = format!(
        "sqlite://{}?mode=rwc",
        supplied_root
            .join("ledger.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let loopback = |endpoint: &str| {
        url::Url::parse(endpoint).ok().is_some_and(|u| {
            u.scheme() == "http"
                && u.host_str() == Some("127.0.0.1")
                && u.port().is_some()
                && u.username().is_empty()
                && u.password().is_none()
        })
    };
    if config.storage.database_url != database
        || !loopback(&config.kubo.rpc_url)
        || config.cold_kubo.is_some()
        || config.pinning.providers.len() != provider_count
        || config
            .pinning
            .providers
            .iter()
            .any(|p| p.kind != "filebase" || p.endpoint.as_deref().is_none_or(|e| !loopback(e)))
        || std::env::var("STAGE8_FIXTURE_TOKEN").ok()
            != Some(format!("offline-fixture-process-{role}"))
    {
        return Err("child fixture-only database/network fence failed");
    }
    let state = AppState::new(&config)
        .await
        .map_err(|_| "production AppState startup failed")?;
    let diagnostic = std::env::var("STAGE8_CHILD_DIAGNOSTIC").ok().as_deref() == Some("bounded");
    let trace = metrics::WorkerTrace::default();
    if diagnostic {
        use tracing_subscriber::prelude::*;
        tracing_subscriber::registry()
            .with(trace.clone())
            .try_init()
            .map_err(|_| "diagnostic tracing subscriber unavailable")?;
    }
    let mut server = http::gateway(state.clone()).await?;
    let allocation = state.pinning.effective_config().providers[0].name.clone();
    let runtime = state
        .pinning
        .provider_runtime(&allocation)
        .ok_or("production limiter missing")?;
    let ready = Ready {
        pid: std::process::id(),
        port: server.port,
        allocation,
        concurrency: runtime.concurrency.available_permits(),
        min_interval_us: runtime.min_request_interval.as_micros() as u64,
        provider_limits: state.pinning.effective_config().providers.iter().map(|p| {
            let rt = state.pinning.provider_runtime(&p.name).expect("validated fixture runtime");
            json!({"allocation":p.name,"available_permits":rt.concurrency.available_permits(),"min_interval_us":rt.min_request_interval.as_micros() as u64})
        }).collect(),
        start_identity: std::env::var("STAGE8_BENCHMARK_START_IDENTITY").ok()
            .filter(|v| uuid::Uuid::parse_str(v).is_ok()).ok_or("parent launch identity missing")?,
    };
    tokio::fs::write(
        root.join(format!("ready-{role}.json")),
        serde_json::to_vec(&ready).map_err(|_| "child readiness encoding failed")?,
    )
    .await
    .map_err(|_| "child readiness publication failed")?;
    let deadline = Instant::now() + Duration::from_secs(seconds + 60);
    let mut worker = None;
    let mut observed_at = Instant::now();
    while Instant::now() < deadline && !root.join("stop").exists() {
        if worker.is_none() && root.join("go").exists() {
            worker = Some(
                state
                    .pinning
                    .start(state.store.clone(), CancellationToken::new()),
            );
        }
        if diagnostic && observed_at.elapsed() >= Duration::from_millis(250) {
            let health = runtime
                .health
                .try_read()
                .map(|v| format!("{v:?}"))
                .unwrap_or_else(|_| "write_locked".into());
            let provider_observations = state.pinning.effective_config().providers.iter().filter_map(|p| {
                let rt = state.pinning.provider_runtime(&p.name)?;
                Some(json!({"allocation":p.name,"available_permits":rt.concurrency.available_permits(),
                    "rate_gate_observation":"private production field; use HTTP start gaps, claim renewal timeline and available permits, not inferred direct gate occupancy"}))
            }).collect::<Vec<_>>();
            let observation = json!({"role":role,"observed_utc_us":chrono::Utc::now().timestamp_micros(),
                "worker_started":worker.is_some(),"available_provider_permits":runtime.concurrency.available_permits(),
                "health":health,"provider_runtime":provider_observations,"lock_for_seconds":state.pinning.settings().lock_for.num_seconds(),"trace":trace.events(),"trace_capacity":512,"trace_dropped":trace.dropped()});
            let pending = root.join(format!("observation-{role}.tmp"));
            tokio::fs::write(
                &pending,
                serde_json::to_vec(&observation).map_err(|_| "diagnostic encoding failed")?,
            )
            .await
            .map_err(|_| "diagnostic publication failed")?;
            tokio::fs::rename(pending, root.join(format!("observation-{role}.json")))
                .await
                .map_err(|_| "diagnostic atomic publication failed")?;
            observed_at = Instant::now();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if let Some(worker) = worker {
        worker.shutdown(Duration::from_secs(3)).await;
    }
    let server_ok = server.shutdown().await;
    let db_ok = state.store.db().clone().close().await.is_ok();
    if !server_ok || !db_ok {
        return Err("child local shutdown incomplete");
    }
    Ok(())
}

pub async fn parent() -> BenchResult<Outcome> {
    parent_run(Case::formal()).await
}

pub async fn comparison() -> BenchResult<Outcome> {
    let case = Case::named(&std::env::var(MODE).map_err(|_| "explicit comparison mode required")?)?;
    if case.diagnostic {
        return Err("diagnostic modes require the diagnostic entrypoint");
    }
    parent_run(case).await
}

pub async fn diagnostic() -> BenchResult<Outcome> {
    let seconds = options()?;
    let case =
        Case::named(&std::env::var(DIAGNOSTIC).map_err(|_| "explicit diagnostic mode required")?)?;
    if !case.diagnostic {
        return Err("comparison modes require the comparison entrypoint");
    }
    if seconds != 25 {
        return Err("narrow diagnostic requires exactly 25 observation seconds");
    }
    parent_run(case).await
}

fn source_fingerprints() -> Value {
    use sha2::{Digest, Sha256};
    let paths = [
        "src/pinning/worker.rs",
        "src/store/pinning/submission.rs",
        "src/store/pinning/jobs.rs",
        "src/store/pinning/quota.rs",
        "src/store/pinning/leases.rs",
    ];
    json!(
        paths
            .into_iter()
            .map(|path| {
                let digest = std::fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
                    .ok()
                    .map(|bytes| hex::encode(Sha256::digest(bytes)));
                json!({"source_seam":path,"sha256":digest})
            })
            .collect::<Vec<_>>()
    )
}

async fn parent_run(case: Case) -> BenchResult<Outcome> {
    let seconds = options()?; // Fail closed BEFORE sockets, DB, temp files or children.
    if seconds != case.seconds {
        return Err("mode requires its fixed observation budget");
    }
    if std::env::var_os("STAGE8_BENCHMARK_ROLE").is_some() {
        return Err("parent cannot run as a child");
    }
    let run = uuid::Uuid::new_v4().to_string();
    let dir = tempfile::Builder::new()
        .prefix("stage8-pinning-")
        .tempdir()
        .map_err(|_| "private temporary directory creation failed")?;
    let (fixture, mut fixture_server) = fixture::Fixture::start_for(case).await?;
    let (raw, pinning) = configuration(dir.path(), &run, fixture_server.port, case)?;
    tokio::fs::write(dir.path().join("owner"), run.as_bytes())
        .await
        .map_err(|_| "private owner marker creation failed")?;
    tokio::fs::write(dir.path().join("config.toml"), &raw)
        .await
        .map_err(|_| "private configuration creation failed")?;
    let config: Config =
        toml::from_str(&raw).map_err(|_| "parent fixture configuration invalid")?;
    let db = store::connect_database(&config.storage.database_url)
        .await
        .map_err(|_| "private SQLite connect failed")?;
    let store = Store::new(db);
    let mut processes = Processes::default();
    let mut measurements = metrics::Measurements::default();
    measurements.diagnostic = case.diagnostic || case.mixed;
    measurements.case = case;
    let source_before = measurements.diagnostic.then(source_fingerprints);
    let mut result = exercise(
        dir.path(),
        &run,
        seconds,
        &fixture,
        &pinning,
        &store,
        &mut processes,
        &mut measurements,
        case,
    )
    .await;
    let mut report = measurements.report(&fixture, seconds);
    if measurements.diagnostic {
        let mut observations = Vec::new();
        for role in 0..case.processes {
            match tokio::fs::read(dir.path().join(format!("observation-{role}.json"))).await {
                Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                    Ok(observation) => observations.push(observation),
                    Err(_) => {
                        result = result.and(Err("child diagnostic observation incomplete"));
                    }
                },
                Err(_) => {
                    result = result.and(Err("child diagnostic observation unavailable"));
                }
            }
        }
        report["kind"] = json!("bounded_diagnostic_not_original_benchmark_acceptance");
        report["diagnostic"] = json!({"observations":observations,"job_changes":measurements.diagnostic_changes(),"source_before":source_before,
            "source_after":source_fingerprints(),"source_observation_boundary":"selected seams at runtime, not a whole-tree build freeze",
            "original_65_output_39_pin_acceptance_unchanged":true});
    }
    report["mode"] = json!(case);
    report["child_launch_identities"] = json!(processes.1);
    report["child_cleanup_identity_boundary"] = json!(
        "parent-owned Child OS handles + matched PID and launch nonce; never PID-name discovery or reused-PID termination"
    );
    if case.mixed {
        report["kind"] = json!("bounded_mixed_traffic_not_formal_acceptance");
    }
    report["completion_contract"] = json!(if case.outputs == 10_000 {
        "fixed 120s publication+observation budget; backlog permitted, never counted completed; foreground, recovery and logical cleanup progress required"
    } else {
        "all expected submits must be done and allocations pinned within the fixed budget"
    });
    report["timing_boundaries"] = json!({"publication_included_in_observation_budget":case.mixed,"per_child_readiness_seconds":20,"graceful_child_shutdown_seconds":8,
        "loopback_shutdown_seconds":3,"child_failsafe_lifetime_seconds":seconds+60,"runtime_worker_threads_per_child":4,"max_os_worker_processes":case.processes});
    let (child_cleanup, child_graceful, killed_children) = processes.stop(dir.path()).await;
    let socket_cleanup = fixture_server.shutdown().await;
    drop(pinning);
    let db_cleanup = store.db().clone().close().await.is_ok();
    drop(store);
    let temp_cleanup = dir.close().is_ok();
    report["cleanup"] = json!({"children_stopped_and_waited":child_cleanup, "children_exited_gracefully":child_graceful,"exact_owned_children_killed":killed_children, "loopback_server_joined":socket_cleanup, "parent_database_closed":db_cleanup, "this_run_temp_directory_removed":temp_cleanup});
    let failure = result
        .err()
        .or_else(|| {
            (!(child_cleanup && socket_cleanup && db_cleanup && temp_cleanup))
                .then_some("owned-resource cleanup incomplete")
        })
        .or_else(|| {
            (!child_graceful).then_some("OS children required forced or unsuccessful termination")
        });
    report["bounded_case_criteria_met"] = json!(failure.is_none());
    report["completed"] = json!(failure.is_none() && report["workload_completed"] == true);
    report["failure_category"] = json!(failure);
    Ok(Outcome { report, failure })
}

#[allow(clippy::too_many_arguments)]
async fn exercise(
    root: &Path,
    run: &str,
    seconds: u64,
    fixture: &fixture::Fixture,
    pinning: &Arc<PinningCoordinator>,
    store: &Store,
    processes: &mut Processes,
    measurements: &mut metrics::Measurements,
    case: Case,
) -> BenchResult<()> {
    store::run_migrations(store.db())
        .await
        .map_err(|_| "private SQLite migration failed")?;
    pinning
        .register_identities(store)
        .await
        .map_err(|_| "shared fixture route registration failed")?;
    store::bucket::create(store.db(), BUCKET, None)
        .await
        .map_err(|_| "fixture bucket creation failed")?;
    let allocation = pinning.effective_config().providers[0].name.clone();
    let mut ports = Vec::new();
    for role in 0..case.processes {
        processes.spawn(
            root,
            run,
            role,
            seconds,
            case.diagnostic || case.mixed,
            case.providers,
        )?;
        let ready = ready(root, role, processes).await?;
        if ready.pid == std::process::id()
            || measurements.pids.contains(&ready.pid)
            || ready.pid != processes.0[role].id()
            || ready.allocation != allocation
            || ready.concurrency != W
            || ready.min_interval_us != 1_000_000 / u64::from(RPS)
            || ready.start_identity != processes.1[role]
            || ready.provider_limits.len() != case.providers
            || ready.provider_limits.iter().any(|p| {
                p["available_permits"] != W || p["min_interval_us"] != 1_000_000 / u64::from(RPS)
            })
        {
            return Err("child identity/production limiter evidence mismatch");
        }
        measurements.pids.push(ready.pid);
        ports.push(ready.port);
    }
    for index in 0..2 {
        let port = ports[index % ports.len()];
        http::put(
            port,
            &format!("recovery/{index}"),
            http::payload("recovery", index),
            false,
        )
        .await?;
    }
    let recovery = metrics::seed_interrupted_calls(store, pinning).await?;
    if case.mixed {
        http::put(ports[0], "cleanup/0", http::payload("cleanup", 0), false).await?;
    }
    let bulk_begin = fixture.now_us();
    let deadline = Instant::now() + Duration::from_secs(seconds);
    if case.name != "formal" {
        let batch_http_us = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            http::put_expected(
                ports[0],
                "batch.zip",
                http::archive_with_counts(case.outputs, case.distinct),
                true,
                case.outputs,
            ),
        )
        .await
        .map_err(|_| "fixed publication budget exhausted")??;
        measurements.add_diagnostic_bulk(case.distinct, bulk_begin, batch_http_us);
    } else {
        let batch_http_us = http::put(ports[0], "batch.zip", http::archive(), true).await?;
        measurements.add_bulk(bulk_begin, batch_http_us);
    }
    within_budget(
        case,
        deadline,
        measurements.sample(store, &allocation, fixture),
    )
    .await?;
    tokio::fs::write(root.join("go"), b"go")
        .await
        .map_err(|_| "shared worker start marker failed")?;
    let deadline = if case.mixed {
        deadline
    } else {
        Instant::now() + Duration::from_secs(seconds)
    };
    measurements.begin_observation(fixture.now_us());
    // Inject ordinary and recovery work while an actual provider HTTP call is
    // in flight, not merely after the batch publication has returned.
    while !fixture.events().iter().any(|e| e.process < 2) {
        processes.alive()?;
        within_budget(
            case,
            deadline,
            measurements.sample(store, &allocation, fixture),
        )
        .await?;
        if Instant::now() >= deadline {
            return Err("worker observation deadline reached before any HTTP service");
        }
        tokio::time::sleep(Duration::from_millis(if case.outputs == 10_000 {
            250
        } else {
            25
        }))
        .await;
    }
    let recovery_ready = fixture.now_us();
    within_budget(
        case,
        deadline,
        metrics::expire_interrupted_claims(store, &recovery),
    )
    .await?;
    measurements.add_recovery(recovery_ready);
    for index in 0..4 {
        let begin = fixture.now_us();
        let http_us = within_budget(
            case,
            deadline,
            http::put(
                ports[index % ports.len()],
                &format!("ordinary/{index}"),
                http::payload("ordinary", index),
                false,
            ),
        )
        .await?;
        measurements.add_ordinary(index, begin, http_us);
        within_budget(
            case,
            deadline,
            measurements.sample(store, &allocation, fixture),
        )
        .await?;
    }
    let mut cleanup_requested = false;
    loop {
        processes.alive()?;
        if case.outputs == 10_000 && Instant::now() >= deadline {
            break;
        }
        match within_budget(
            case,
            deadline,
            measurements.sample(store, &allocation, fixture),
        )
        .await
        {
            Err("fixed mixed publication/observation budget exhausted")
                if case.outputs == 10_000 =>
            {
                break;
            }
            result => result?,
        }
        if case.mixed && !cleanup_requested && measurements.cleanup_ready() {
            let started = fixture.now_us();
            let http_us =
                within_budget(case, deadline, http::delete(ports[0], "cleanup/0")).await?;
            measurements.cleanup_requested(started, http_us);
            cleanup_requested = true;
        }
        if if case.mixed {
            case.outputs != 10_000 && measurements.mixed_settled()
        } else if case.diagnostic {
            measurements.diagnostic_settled()
        } else {
            measurements.settled()
        } {
            break;
        }
        if Instant::now() >= deadline {
            if case.outputs == 10_000 {
                break;
            }
            return Err(
                "bounded observation expired with unfinished durable work; production blocker not relaxed",
            );
        }
        tokio::time::sleep(Duration::from_millis(if case.outputs == 10_000 {
            250
        } else {
            25
        }))
        .await;
    }
    if case.mixed {
        measurements.verify_mixed(fixture, &recovery, store).await
    } else if case.diagnostic {
        Ok(())
    } else {
        measurements
            .verify(store, &allocation, fixture, &recovery, case.processes)
            .await
    }
}
