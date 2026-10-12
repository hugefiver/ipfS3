//! Formal local management CLI. No HTTP route, AppState, migrations, startup
//! identity registration, background worker or implicit source/provider probe.
use std::time::Duration;

use anyhow::{Result, anyhow};
use ipfs_s3_gateway::{config::Config, crypto::key::MasterKey, kubo::KuboClient,
    pinning::management::local_config, store::pinning::management::records};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

#[path = "management/applied.rs"]
mod applied;
#[path = "management/args.rs"]
mod args;
#[path = "management/database.rs"]
mod database;
#[path = "management/files.rs"]
mod files;
#[path = "management/input.rs"]
mod input;
#[path = "management/json_bounds.rs"]
mod json_bounds;
#[path = "management/planning.rs"]
mod planning;
#[path = "management/preparation.rs"]
mod preparation;

pub use args::{Command, parse};

const WORK_BUDGET: Duration = Duration::from_secs(30);
const DRAIN_BUDGET: Duration = Duration::from_secs(5);

pub async fn run(command: Command) -> Result<()> {
    if command == Command::Help {
        println!("{}", args::HELP);
        return Ok(());
    }
    // All document bounds/DTO validation precede Config, DB and client factories.
    let input = tokio::time::timeout(WORK_BUDGET, input::load(&command)).await
        .map_err(|_| input::invalid())??;
    let raw = Config::load().map_err(|_| anyhow!("local configuration load failed (details withheld)"))?;
    MasterKey::from_hex(&raw.crypto.master_key)
        .map_err(|_| anyhow!("management local key configuration invalid (details withheld)"))?;
    let mut config = local_config::compile(&raw, |name| std::env::var(name).ok())?;
    if let Some(node) = input.source_node(&command) { config = config.with_source_node(node.to_owned())?; }
    let readonly = matches!(command, Command::DryRun { .. } | Command::Status { .. });
    let signal = crate::shutdown::ShutdownSignal::install()
        .map_err(|_| anyhow!("management cancellation handler unavailable"))?;
    let db = database::connect(&raw.storage.database_url, readonly).await?;
    let cancellation = CancellationToken::new();
    let (result, close_deadline) = {
        let work = async {
            let auth = records::authorize_local(&raw, &db).await?;
            match &input {
                input::Input::Request(request) => planning::execute(&db, &config, &auth, &command, request).await,
                input::Input::Imported(imported) => applied::execute(&db, &raw, &config, &auth, &command, imported, cancellation.clone()).await,
                input::Input::Prepare(request) => preparation::execute(&db, &config, &auth,
                    &KuboClient::new(raw.kubo.rpc_url.clone()), request, cancellation.clone()).await,
            }
        };
        tokio::pin!(work);
        tokio::select! {
            result = &mut work => (result, tokio::time::Instant::now() + DRAIN_BUDGET),
            _ = signal.wait() => drain(&mut work, &cancellation, "signal").await,
            _ = tokio::time::sleep(WORK_BUDGET) => drain(&mut work, &cancellation, "work_budget").await,
        }
    };
    cancellation.cancel();
    let closed = tokio::time::timeout_at(close_deadline, db.close()).await;
    if !matches!(closed, Ok(Ok(()))) { return Err(database::unavailable()); }
    match result {
        Ok(report) => files::print(&report).await,
        Err(error) => {
            if let Some(failure) = error.downcast_ref::<applied::FailureReport>() {
                files::print(&failure.report).await?;
            }
            Err(error)
        }
    }
}

async fn drain<F>(work: F, cancellation: &CancellationToken, reason: &'static str) -> (Result<Value>, tokio::time::Instant)
where F: std::future::Future<Output = Result<Value>> {
    cancellation.cancel();
    let deadline = tokio::time::Instant::now() + DRAIN_BUDGET;
    let result = match tokio::time::timeout_at(deadline, work).await {
        Ok(Ok(mut report)) => {
            report["interrupted"] = serde_json::json!(reason);
            Ok(report)
        }
        Ok(Err(error)) => Err(error),
        Err(_) => Err(anyhow!("management work stopped; inspect the original confirmed document with --status (details withheld)")),
    };
    (result, deadline)
}
