use anyhow::Result;
use ipfs_s3_gateway::{
    config::Config,
    kubo::KuboClient,
    pinning::{
        coordinator::PinningCoordinator,
        management::{local_config::ManagementConfig, wire::ImportedPlan},
    },
    store::{
        Store,
        pinning::management::{
            ManagementError, ManagementJobScope, actions,
            records::{self, CommittedAction, LocalAuthorization},
        },
    },
};
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseBackend, DatabaseConnection, IsolationLevel,
    TransactionTrait,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{Command, input};

#[derive(Debug, thiserror::Error)]
enum FailureReason {
    #[error("{0}")]
    Apply(ManagementError),
    #[error("management applied job scope invalid (details withheld)")]
    Scope,
    #[error("management scoped runtime configuration invalid (details withheld)")]
    Runtime,
    #[error("management scoped execution stopped or needs attention (details withheld)")]
    Run,
}

#[derive(thiserror::Error)]
#[error("{reason}")]
pub struct FailureReport {
    pub report: Value,
    reason: FailureReason,
}

impl std::fmt::Debug for FailureReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Error formatting never dumps the report or its caller-supplied labels.
        std::fmt::Debug::fmt(&self.reason, formatter)
    }
}

fn item(index: usize, action: &CommittedAction) -> Value {
    json!({
        "item_index": index, "item_id": action.receipt.binding.request.item_id,
        "receipt": "verified_original_database_receipt", "state": action.state,
        "intent_count": action.receipt.intent_ids.len(), "target_count": action.targets.len(),
        "job_directives": action.jobs.iter().map(|job| job.directive).collect::<Vec<_>>(),
        "physical_release_bytes": 0,
    })
}

fn items(actions: &[CommittedAction]) -> Vec<Value> {
    actions
        .iter()
        .enumerate()
        .map(|(index, action)| item(index, action))
        .collect()
}

fn apply_failure(failure: actions::ApplyFailure, run: bool) -> FailureReport {
    let needs_attention = failure
        .items
        .iter()
        .any(|item| item.state == records::ActionState::NeedsAttention);
    FailureReport {
        report: json!({"mode": "apply", "outcome": if needs_attention { "needs_attention" } else { "stopped" },
            "business_commit": if needs_attention { "unknown_inspect_original_confirmation_with_status" } else { "no_successful_prefix_reported" },
            "run_requested": run, "run_started": false, "source_rpc": "not_performed", "provider_rpc": "not_performed",
            "state_evidence": "typed_apply_failure", "diagnostic": failure.cause.to_string(),
            "items": failure.items.iter().enumerate().map(|(index, item)| json!({
                "item_index": index, "item_id": item.item_id, "state": item.state,
                "receipt": "not_reported", "physical_release_bytes": 0,
            })).collect::<Vec<_>>() }),
        reason: FailureReason::Apply(failure.cause),
    }
}

async fn run_failure(
    db: &DatabaseConnection,
    auth: &LocalAuthorization,
    binding: &records::ConfirmedBinding,
    mut report: Value,
    reason: FailureReason,
    started: bool,
    cancellation: &CancellationToken,
) -> anyhow::Error {
    let mut states = Vec::new();
    for (index, entry) in binding.confirmation.bindings.iter().enumerate() {
        let status = actions::status::lookup(db, auth, binding, &entry.request.item_id).await;
        states.push(match status {
            Ok(Some(action)) => item(index, &action),
            Ok(None) => json!({"item_index": index, "item_id": entry.request.item_id, "receipt": "not_applied", "state": "not_applied"}),
            Err(_) => json!({"item_index": index, "item_id": entry.request.item_id, "receipt": "status_unavailable", "state": null}),
        });
    }
    // Apply has already returned a committed/replayed result. A scoped failure
    // neither rolls that result back nor proves every item's current state.
    report["items"] = json!(states);
    report["state_evidence"] = json!("read_only_action_lookup_after_scoped_failure");
    report["run"] = json!({"outcome": "stopped_or_needs_attention", "started": started,
        "cancelled": cancellation.is_cancelled(), "diagnostic": reason.to_string()});
    if started {
        report["provider_rpc"] = json!("may_have_occurred_for_verified_scoped_jobs");
        report["source_rpc"] = json!("may_have_occurred_if_verified_scoped_job_required_source");
    }
    anyhow::Error::new(FailureReport { report, reason })
}

pub async fn execute(
    db: &DatabaseConnection,
    raw: &Config,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    command: &Command,
    imported: &ImportedPlan,
    cancellation: CancellationToken,
) -> Result<Value> {
    match command {
        Command::Status { .. } => {
            let tx = db
                .begin_with_config(
                    if db.get_database_backend() == DatabaseBackend::Postgres {
                        Some(IsolationLevel::RepeatableRead)
                    } else {
                        None
                    },
                    if db.get_database_backend() == DatabaseBackend::Postgres {
                        Some(AccessMode::ReadOnly)
                    } else {
                        None
                    },
                )
                .await
                .map_err(|_| super::database::unavailable())?;
            // B's read-only loader restores original private authority;
            // no fresh confirmation/TTL gate precedes receipt/status lookup.
            let binding = records::load_confirmed_from_imported(&tx, auth, imported).await?;
            let mut result = Vec::new();
            for (index, entry) in binding.confirmation.bindings.iter().enumerate() {
                let action = actions::status::lookup_with_work(
                    &tx,
                    auth,
                    &binding,
                    &entry.request.item_id,
                    imported.session(),
                )
                .await?;
                result.push(match action {
                    Some(action) => item(index, &action),
                    None => json!({"item_index": index, "item_id": entry.request.item_id, "receipt": "not_applied", "state": "not_applied"}),
                });
            }
            tx.rollback()
                .await
                .map_err(|_| super::database::unavailable())?;
            Ok(
                json!({"mode": "status", "database_writes": false, "source_rpc": "not_performed", "provider_rpc": "not_performed",
                "observation": "action_layer_stored_database_evidence_not_live", "state_evidence": "read_only_current_action_lookup",
                "items": result, "physical_release_bytes": 0}),
            )
        }
        Command::Apply { run, .. } => {
            let binding = records::load_confirmed_from_imported(db, auth, imported).await?;
            let applied = actions::apply_detailed_with_session(
                db,
                config,
                auth,
                &binding,
                imported.session(),
            )
            .await
            .map_err(|failure| apply_failure(failure, *run))?;
            let mut report = json!({"mode": "apply", "replayed": applied.replayed, "run_requested": run,
                "business_commit": if applied.replayed { "exact_replay_no_new_business" } else { "committed" },
                "state_evidence": "committed_action_result", "items": items(&applied.items),
                "source_rpc": "not_performed", "provider_rpc": "not_performed"});
            if *run {
                let scope = match ManagementJobScope::from_applied_with_work(
                    db,
                    auth,
                    &binding,
                    imported.session(),
                )
                .await
                {
                    Ok(scope) => scope,
                    Err(_) => {
                        return Err(run_failure(
                            db,
                            auth,
                            &binding,
                            report,
                            FailureReason::Scope,
                            false,
                            &cancellation,
                        )
                        .await);
                    }
                };
                let coordinator = match PinningCoordinator::build_with_decompress_config(
                    config.pinning.clone(),
                    Some(KuboClient::new(raw.kubo.rpc_url.clone())),
                    raw.pinning_control.unavailable,
                    &raw.decompress_zip,
                ) {
                    Ok(coordinator) => coordinator,
                    Err(_) => {
                        return Err(run_failure(
                            db,
                            auth,
                            &binding,
                            report,
                            FailureReason::Runtime,
                            false,
                            &cancellation,
                        )
                        .await);
                    }
                };
                let store = Store::new(db.clone()); // existing connection only; never bootstrap/migrate/register
                let mut run = tokio::task::JoinSet::new();
                let run_cancellation = cancellation.clone();
                run.spawn(async move {
                    coordinator
                        .management_tick(&store, &scope, &run_cancellation)
                        .await
                });
                let observations = match run.join_next().await {
                    Some(Ok(Ok(observations))) => observations,
                    _ => {
                        return Err(run_failure(
                            db,
                            auth,
                            &binding,
                            report,
                            FailureReason::Run,
                            true,
                            &cancellation,
                        )
                        .await);
                    }
                };
                report["items"] = json!(items(&observations.items));
                report["state_evidence"] = json!("scoped_job_report");
                report["provider_rpc"] = json!("explicit_receipt_scoped_pass_only");
                report["source_rpc"] = json!("only_if_verified_scoped_job_requires_source");
                report["run"] = json!({"passes": 1, "cancelled": cancellation.is_cancelled(),
                    "remaining_work": "due_time_and_claim_cap_are_not_overridden_inspect_item_states_or_repeat_explicit_run",
                    "submit_observations": observations.submit_observations.iter().map(|row| json!({
                        "recorded": true, "needs_attention": row.needs_attention, "started_at": row.started_at, "observed_at": row.observed_at,
                    })).collect::<Vec<_>>(),
                    "replica_observations": observations.replica_observations.iter().map(|row| json!({
                        "recorded": true, "projectable": row.projectable, "observed_at": row.observed_at,
                    })).collect::<Vec<_>>() });
            }
            Ok(report)
        }
        _ => Err(input::invalid()),
    }
}

#[cfg(test)]
#[path = "applied/runtime_tests.rs"]
mod runtime_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use records::ActionState;

    fn failure(states: &[ActionState]) -> actions::ApplyFailure {
        actions::ApplyFailure {
            cause: ManagementError::Database,
            items: states
                .iter()
                .enumerate()
                .map(|(index, state)| actions::StoppedItem {
                    item_id: format!("item-{index}"),
                    state: *state,
                })
                .collect(),
        }
    }

    #[test]
    fn uncertain_apply_preserves_attention_states_and_never_reports_a_started_run() {
        let failure = apply_failure(
            failure(&[ActionState::Stopped, ActionState::NeedsAttention]),
            true,
        );
        assert_eq!(failure.report["outcome"], "needs_attention");
        assert_eq!(
            failure.report["business_commit"],
            "unknown_inspect_original_confirmation_with_status"
        );
        assert_eq!(failure.report["items"][0]["state"], "stopped");
        assert_eq!(failure.report["items"][1]["state"], "needs_attention");
        assert_eq!(failure.report["run_requested"], true);
        assert_eq!(failure.report["run_started"], false);
        assert_eq!(failure.report["provider_rpc"], "not_performed");
        assert_eq!(failure.report["source_rpc"], "not_performed");
    }

    #[test]
    fn known_stopped_apply_preserves_typed_failure_evidence() {
        let failure = apply_failure(failure(&[ActionState::Stopped]), false);
        assert_eq!(failure.report["outcome"], "stopped");
        assert_eq!(
            failure.report["business_commit"],
            "no_successful_prefix_reported"
        );
        assert_eq!(failure.report["items"][0]["state"], "stopped");
        assert_eq!(failure.report["run_started"], false);
    }
}
