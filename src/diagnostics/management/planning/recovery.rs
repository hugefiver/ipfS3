use super::*;
use ipfs_s3_gateway::pinning::management::retirement::{CommittedPlanning, RunLimits};
use ipfs_s3_gateway::store::pinning::management::native::meter::Budget;

pub(super) async fn export_original_confirmation(
    db: &DatabaseConnection,
    command: &Command,
    task: CommittedPlanning,
    mut budget: Budget,
    limits: &RunLimits,
) -> Result<Value> {
    let mut report = json!({
        "mode": if matches!(command, Command::Confirm { .. }) { "confirm" } else { "dry_run" },
        "outcome": "complete", "phase": "confirmed", "database_writes": false,
        "effects": {"database_commits": 0, "commit_unknown": false}, "effects_scope": "invocation",
        "confirmation_committed": true, "artifact_exported": false, "checkpoint_retained": true,
        "recovered_original_confirmation": true,
        "artifact_trust": "public_original_confirmation_requires_local_database_authorization",
        "source_rpc": "not_performed", "provider_rpc": "not_performed",
    });
    let result = async {
        let binding = task.binding();
        let reserved: Result<_> = async {
            let output = match command {
                Command::Confirm {
                    output,
                    acknowledgements,
                    ttl_seconds,
                    ..
                } => {
                    if acknowledgements.as_slice()
                        != binding.confirmation.candidate_item_ids.as_slice()
                        || i64::from(*ttl_seconds)
                            != (binding.expires_at - binding.confirmed_at).num_seconds()
                    {
                        return Err(input::invalid());
                    }
                    Some(output.as_str())
                }
                Command::DryRun { output, .. } => output.as_deref(),
                _ => return Err(input::invalid()),
            };
            match output {
                Some(path) => Ok(Some(files::reserve(path).await?.into_std().await)),
                None => Ok(None),
            }
        }
        .await;
        match reserved {
            Ok(Some(file)) => {
                let (result, _) = task
                    .export_file(db, (file, budget), Projection::Review, limits)
                    .await;
                result.context("management original confirmation export failed")?;
                report["artifact_exported"] = json!(true);
            }
            Ok(None) => task.account_capture(&mut budget, limits).await?,
            Err(error) => {
                task.account_capture(&mut budget, limits).await?;
                return Err(error);
            }
        }
        let review = binding.review();
        report["summary"] = json!(review.summary());
        report["items"] = json!(review.entries().iter().enumerate().map(|(index, entry)|
                json!({"item_index": index, "status": entry.status, "step": entry.step})
            ).collect::<Vec<_>>());
        Ok(())
    }
    .await;
    let closed = task.close().await;
    match (result, closed) {
        (Err(error), _) => Err(confirmation::failure(report, error)),
        (_, Err(error)) => Err(confirmation::failure(report, error.into())),
        (Ok(()), Ok(_)) => Ok(report),
    }
}
