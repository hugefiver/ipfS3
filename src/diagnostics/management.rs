//! Formal local management CLI. Ordinary commands never migrate. Explicit
//! upgrade is isolated from AppState, registration, workers and provider/source IO.
use std::time::Duration;

use anyhow::{Result, anyhow};
use ipfs_s3_gateway::{
    config::Config, crypto::key::MasterKey, kubo::KuboClient, pinning::management::local_config,
    store::pinning::management::records,
};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

#[path = "management/applied.rs"]
mod applied;
#[path = "management/args.rs"]
mod args;
#[path = "management/artifacts.rs"]
mod artifacts;
#[path = "management/binding.rs"]
mod binding;
#[path = "management/database.rs"]
mod database;
#[path = "management/files.rs"]
mod files;
#[path = "management/input.rs"]
mod input;
#[path = "management/planning.rs"]
mod planning;
#[path = "management/preparation.rs"]
mod preparation;
#[path = "management/upgrade.rs"]
mod upgrade;

pub use args::{Command, UpgradeCommand, parse, parse_upgrade};
pub use upgrade::run as run_upgrade;

const WORK_BUDGET: Duration = Duration::from_secs(30);
const DRAIN_BUDGET: Duration = Duration::from_secs(5);

pub async fn run(command: Command) -> Result<()> {
    if command == Command::Help {
        println!("{}", args::HELP);
        return Ok(());
    }
    if matches!(
        command,
        Command::Review { .. }
            | Command::Export { .. }
            | Command::Import { .. }
            | Command::ConvertV2 { .. }
    ) {
        let report = tokio::task::spawn_blocking(move || artifacts::execute(&command))
            .await
            .map_err(|_| input::invalid())??;
        return files::print(&report).await;
    }
    // All document bounds/DTO validation precede Config, DB and client factories.
    let work_budget = if matches!(command, Command::DryRun { .. } | Command::Confirm { .. }) {
        ipfs_s3_gateway::pinning::management::wire::v3::files::stream::limits().elapsed
    } else {
        WORK_BUDGET
    };
    let input = tokio::time::timeout(work_budget, input::load(&command))
        .await
        .map_err(|_| input::invalid())??;
    let raw = Config::load()
        .map_err(|_| anyhow!("local configuration load failed (details withheld)"))?;
    MasterKey::from_hex(&raw.crypto.master_key)
        .map_err(|_| anyhow!("management local key configuration invalid (details withheld)"))?;
    let mut config = local_config::compile(&raw, |name| std::env::var(name).ok())?;
    if let Some(node) = input.source_node(&command) {
        config = config.with_source_node(node.to_owned())?;
    }
    let readonly = matches!(command, Command::DryRun { .. } | Command::Status { .. });
    let signal = crate::shutdown::ShutdownSignal::install()
        .map_err(|_| anyhow!("management cancellation handler unavailable"))?;
    let db = database::connect(&raw.storage.database_url, readonly).await?;
    let cancellation = CancellationToken::new();
    let work_deadline = std::time::Instant::now() + work_budget;
    let (result, close_deadline) = {
        let work = async {
            let auth = records::authorize_local(&raw, &db).await?;
            match input {
                input::Input::Request(request) => {
                    planning::execute(
                        &db,
                        &config,
                        &auth,
                        &command,
                        &request,
                        cancellation.clone(),
                        work_deadline,
                    )
                    .await
                }
                input::Input::Imported(imported) => {
                    applied::execute(
                        &db,
                        &raw,
                        &config,
                        &auth,
                        &command,
                        *imported,
                        cancellation.clone(),
                    )
                    .await
                }
                input::Input::Prepare(request) => {
                    preparation::execute(
                        &db,
                        &config,
                        &auth,
                        &KuboClient::new(raw.kubo.rpc_url.clone()),
                        &request,
                        cancellation.clone(),
                    )
                    .await
                }
            }
        };
        tokio::pin!(work);
        tokio::select! {
            result = &mut work => (result, tokio::time::Instant::now() + DRAIN_BUDGET),
            _ = signal.wait() => drain(&mut work, &cancellation, "signal", matches!(command, Command::Confirm { .. })).await,
            _ = tokio::time::sleep_until(work_deadline.into()) => drain(&mut work, &cancellation, "work_budget", matches!(command, Command::Confirm { .. })).await,
        }
    };
    cancellation.cancel();
    let closed = tokio::time::timeout_at(close_deadline, db.close()).await;
    let result = if matches!(closed, Ok(Ok(()))) {
        result
    } else {
        match result {
            Ok(report) if report.get("effects").is_some() => {
                Err(planning::close_failure(report, database::unavailable()))
            }
            Err(error) if error.downcast_ref::<planning::FailureReport>().is_some() => {
                Err(error.context("management database close failed"))
            }
            _ => Err(database::unavailable()),
        }
    };
    match result {
        Ok(report) => files::print(&report).await,
        Err(error) => {
            if let Some(failure) = error.downcast_ref::<applied::FailureReport>() {
                files::print(&failure.report).await?;
            }
            if let Some(failure) = error.downcast_ref::<planning::FailureReport>() {
                files::print(&failure.report).await?;
            }
            Err(error)
        }
    }
}

async fn drain<F>(
    work: F,
    cancellation: &CancellationToken,
    reason: &'static str,
    confirming: bool,
) -> (Result<Value>, tokio::time::Instant)
where
    F: std::future::Future<Output = Result<Value>>,
{
    cancellation.cancel();
    let deadline = tokio::time::Instant::now() + DRAIN_BUDGET;
    let result = match tokio::time::timeout_at(deadline, work).await {
        Ok(Ok(mut report)) => {
            report["interrupted"] = serde_json::json!(reason);
            Ok(report)
        }
        Ok(Err(error)) => Err(error),
        Err(_) => {
            let error = anyhow!(
                "management work stopped; inspect the original confirmed document with --status (details withheld)"
            );
            Err(if confirming {
                planning::close_failure(
                    serde_json::json!({
                        "mode": "confirm", "outcome": "needs_attention", "phase": "unknown",
                        "interrupted": reason, "effects_scope": "invocation",
                        "effects": { "database_commits": null, "commit_unknown": true },
                        "database_writes": true, "confirmation_committed": null,
                        "artifact_exported": null,
                        "source_rpc": "not_performed", "provider_rpc": "not_performed",
                    }),
                    error,
                )
            } else {
                error
            })
        }
    };
    (result, deadline)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn confirmation_drain_expiry_reports_unknown_effects_not_zero_writes() {
        let cancellation = CancellationToken::new();
        let (result, _) = drain(
            std::future::pending::<Result<Value>>(),
            &cancellation,
            "signal",
            true,
        )
        .await;
        assert!(cancellation.is_cancelled());
        let error = result.unwrap_err();
        let report = &error
            .downcast_ref::<planning::FailureReport>()
            .unwrap()
            .report;
        assert_eq!(report["effects"]["commit_unknown"], true);
        assert!(report["effects"]["database_commits"].is_null());
        assert_eq!(report["database_writes"], true);
        assert!(report["confirmation_committed"].is_null());
        assert!(report["artifact_exported"].is_null());
    }
}
