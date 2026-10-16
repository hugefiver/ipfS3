use anyhow::Result;
use ipfs_s3_gateway::{
    pinning::management::{
        self, PlanRequest, PlanStatus, db_snapshot, local_config::ManagementConfig, wire,
    },
    store::pinning::management::records::{self, LocalAuthorization},
};
use sea_orm::DatabaseConnection;
use serde_json::{Value, json};

use super::{Command, files, input};

pub async fn execute(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    command: &Command,
    request: &PlanRequest,
) -> Result<Value> {
    let work = records::SharedDecodeBudget::default();
    let collected = db_snapshot::collect_with_session(db, config, request, work.clone()).await?;
    let needs_attention = collected.needs_attention.clone();
    let snapshot = collected.planning_snapshot()?;
    let plan = management::dry_run_with_work(request, &snapshot, work.clone())?;
    let (mode, artifact_exported) = match command {
        Command::DryRun { output, .. } => {
            if let Some(path) = output {
                let bytes =
                    wire::encode_plan_with_session(request, &snapshot, &plan, None, work.clone())?;
                let mut file = files::reserve(path).await?;
                files::write(&mut file, &bytes).await?;
            }
            ("dry_run", output.is_some())
        }
        Command::Confirm {
            output,
            acknowledgements,
            ttl_seconds,
            ..
        } => {
            // Validate acknowledgement and exportability before explicit staging.
            // Reserve create_new before any confirmation write: an existing file
            // or missing parent cannot cause a hidden DB confirmation.
            let bytes = wire::encode_plan_with_session(
                request,
                &snapshot,
                &plan,
                Some(acknowledgements),
                work.clone(),
            )?;
            if plan.items().iter().any(|item| {
                matches!(item.status, PlanStatus::Blocked(_))
                    || item.status == PlanStatus::Candidate
                        && !acknowledgements.contains(&item.request.item_id)
            }) {
                return Err(input::invalid());
            }
            let mut file = files::reserve(output).await?;
            let binding = records::confirm_with_session(
                db,
                config,
                auth,
                (request, &snapshot, &plan),
                acknowledgements,
                chrono::Duration::seconds(i64::from(*ttl_seconds)),
                work,
            )
            .await?;
            // The public wire is exported, not the private binding or DB proposal.
            // Confirmation's reviewed tuple must remain exactly the one encoded.
            if &*binding.request != request
                || binding.confirmation.candidate_item_ids.as_slice() != acknowledgements
            {
                return Err(input::invalid());
            }
            files::write(&mut file, &bytes).await?;
            ("confirm", true)
        }
        _ => return Err(input::invalid()),
    };
    Ok(json!({
        "mode": mode, "database_writes": mode == "confirm", "artifact_exported": artifact_exported,
        "artifact_trust": if mode == "confirm" { "public_original_confirmation_requires_local_database_authorization" } else { "untrusted_proposal_not_authority" },
        "source_rpc": "not_performed", "provider_rpc": "not_performed",
        "summary": plan.summary(), "needs_attention": needs_attention,
        "items": plan.items().iter().enumerate().map(|(index, entry)| json!({"item_index": index, "status": entry.status, "step": entry.step})).collect::<Vec<_>>(),
    }))
}
