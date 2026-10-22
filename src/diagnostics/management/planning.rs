use anyhow::{Context, Result};
use ipfs_s3_gateway::{
    pinning::management::{
        self, PlanRequest, PlanStatus, Snapshot, db_snapshot,
        local_config::ManagementConfig,
        retirement::{PlanningCheckpoint, PlanningProgress},
        wire::v3::{self, Projection, planning::Header},
    },
    store::pinning::management::records::{self, LocalAuthorization},
};
use sea_orm::DatabaseConnection;
use serde_json::{Value, json};

use super::{Command, files, input};

#[path = "planning/confirmation.rs"]
mod confirmation;
#[path = "planning/recovery.rs"]
mod recovery;
pub use confirmation::FailureReport;

pub(super) fn close_failure(report: Value, cause: anyhow::Error) -> anyhow::Error {
    confirmation::failure(report, cause)
}

pub async fn execute(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    command: &Command,
    request: &PlanRequest,
    cancellation: tokio_util::sync::CancellationToken,
    deadline: std::time::Instant,
) -> Result<Value> {
    let mut budget = files::stream::budget();
    let mut limits = management::retirement::RunLimits::default();
    limits.cancellation = cancellation;
    limits.constrain_deadline(deadline, &mut budget)?;
    if let Some(options) = command.checkpoint() {
        if let Some(pages) = options.scan_pages {
            limits.pages = pages;
        }
        if options.resume {
            let local = Snapshot {
                configuration: Some(management::ConfigurationBinding {
                    routes: config
                        .pinning
                        .providers
                        .iter()
                        .map(|provider| provider.route_snapshot())
                        .collect(),
                    policies: config.pinning.policies.clone(),
                    limits: config
                        .pinning
                        .provider_limits
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                    zip_rules: Some(config.zip_rules.clone()),
                    source_node: config.source_node().map(str::to_owned),
                }),
                ..Snapshot::default()
            };
            if let Some(task) = PlanningCheckpoint::committed_task(
                std::path::Path::new(&options.path),
                db,
                auth,
                request,
                &local,
                &limits,
                &mut budget,
            )
            .await
            .context("management original confirmation recovery rejected (checkpoint retained)")?
            {
                let report =
                    recovery::export_original_confirmation(db, command, task, budget, &limits)
                        .await?;
                if let Err(error) = limits.check() {
                    return Err(close_failure(report, error.into()));
                }
                return Ok(report);
            }
        }
    }
    if let Command::DryRun {
        output,
        checkpoint: None,
        ..
    } = command
        && let Some(plan) = records::load_recorded_plan(db, auth, request, &mut budget).await?
    {
        if let Some(path) = output {
            let mut writer = files::stream::writer(path, Projection::Review, budget)?;
            v3::planning::recorded(db, &mut writer, &plan, Projection::Review).await?;
            files::stream::finish(writer)?;
        }
        let mut report = super::applied::recorded(command, &plan)?;
        report["artifact_exported"] = json!(output.is_some());
        report["artifact_trust"] = json!("historical_reply_not_execution_authority");
        limits.check()?;
        return Ok(report);
    }
    let work = records::SharedDecodeBudget::default();
    let collected = db_snapshot::collect_with_control(
        db,
        config,
        request,
        deadline,
        limits.cancellation.clone(),
        work.clone(),
    )
    .await
    .context(
        if command.checkpoint().is_some_and(|options| options.resume) {
            "management checkpoint capture failed; fresh scan required (checkpoint retained)"
        } else {
            "management scoped capture failed"
        },
    )?;
    let needs_attention = collected.needs_attention.clone();
    let (mut snapshot, scope) = collected
        .planning_with_scope()
        .context("management planning projection failed")?;
    let measured = scope.work();
    budget.charge(
        measured.sql,
        measured.rows,
        measured.cells,
        measured.raw_bytes,
    )?;
    budget.work.copied_bytes = measured.copied_bytes;
    budget.work.output_bytes = measured.output_bytes;
    budget.work.pages = measured.pages;
    let _source_resident = budget.reserve(scope.peak_resident())?;
    let prepared = if let Some(options) = command.checkpoint() {
        let path = std::path::Path::new(&options.path);
        // Only a new local capture and local authorization enter the checkpoint
        // API. Frozen input metadata never substitutes for either boundary.
        let mut checkpoint = if options.resume {
            PlanningCheckpoint::resume(path, db, auth.principal(), request, &snapshot, &limits)
                .await
                .context("management checkpoint resume rejected; fresh scan required (checkpoint retained)")?
        } else {
            PlanningCheckpoint::create(path, db, auth.principal(), request, &snapshot, &limits)
                .await
                .context("management checkpoint creation failed")?
        };
        snapshot = checkpoint.snapshot().clone();
        let advanced = async {
            let caps = checkpoint.reserve_capture(&budget, &limits)?;
            checkpoint.advance(db, &caps).await
        }
        .await;
        if !matches!(&advanced, Ok(PlanningProgress::Complete(_))) {
            let accounted = checkpoint.account_capture(&mut budget, &limits).await;
            if let Err(error) = accounted {
                checkpoint
                    .close()
                    .await
                    .context("management checkpoint close failed")?;
                return Err(error).context(
                    "management checkpoint capture accounting failed (checkpoint retained)",
                );
            }
        }
        match advanced {
            Ok(PlanningProgress::Incomplete) => {
                let path = checkpoint
                    .close()
                    .await
                    .context("management checkpoint close failed")?;
                limits.check()?;
                budget.check()?;
                return Ok(json!({
                    "mode": if matches!(command, Command::Confirm { .. }) { "confirm" } else { "dry_run" },
                    "outcome": "incomplete", "checkpoint": path,
                    "phase": "planning", "effects_scope": "invocation",
                    "effects": {"database_commits": 0, "commit_unknown": false},
                    "confirmation_committed": false,
                    "database_writes": false, "artifact_exported": false,
                    "source_rpc": "not_performed", "provider_rpc": "not_performed",
                }));
            }
            Ok(PlanningProgress::Complete(prepared)) => *prepared,
            Err(error) => {
                checkpoint
                    .close()
                    .await
                    .context("management checkpoint close failed")?;
                return Err(error).context(
                    "management checkpoint scan failed; fresh scan required (checkpoint retained)",
                );
            }
        }
    } else {
        limits.pages = 512;
        management::retirement::PreparedPlanning::collect_with_limits(
            db,
            request,
            &snapshot,
            work.clone(),
            &limits,
        )
        .await
        .context("management reviewed planning failed")?
    };
    let provider_confirmation = matches!(command, Command::Confirm { .. })
        && request
            .items
            .iter()
            .any(|item| matches!(item.subject, management::Subject::Provider { .. }));
    let result = if provider_confirmation {
        confirmation::ProviderConfirmation {
            db,
            config,
            auth,
            command,
            prepared: &prepared,
            request,
            snapshot: &snapshot,
            limits: &limits,
            needs_attention: &needs_attention,
        }
        .execute(budget, work)
        .await
    } else {
        async {
    let plan = &prepared.plan;
    let (mode, artifact_exported) = match command {
        Command::DryRun { output, .. } => {
            if let Some(path) = output {
                let global = request.items.iter().any(|item| matches!(item.subject, management::Subject::Provider { .. }));
                if global {
                    let initial_output = budget.work.output_bytes;
                    loop {
                    let (progress, returned) = prepared.prepare_output(
                        budget, header(auth, &snapshot, None)?, request, &snapshot, &limits,
                    ).await;
                    budget = returned;
                    let progress = progress.context("management review output preparation failed")?;
                    let pending = progress == management::retirement::Progress::Pending;
                    let produced = budget.work.output_bytes > initial_output;
                    if pending || command.checkpoint().is_some() && produced {
                        limits.check()?;
                        budget.check()?;
                        if pending && limits.pages_remaining() > 0 && limits.production_available() { continue; }
                        let checkpoint = prepared.checkpoint_path().ok_or_else(|| anyhow::anyhow!("management output preparation incomplete; use an explicit checkpoint"))?;
                        return Ok(json!({
                            "mode": "dry_run", "outcome": "incomplete", "phase": "output",
                            "checkpoint": checkpoint, "effects_scope": "invocation",
                            "effects": {"database_commits": 0, "commit_unknown": false},
                            "confirmation_committed": false, "database_writes": false,
                            "artifact_exported": false, "source_rpc": "not_performed", "provider_rpc": "not_performed",
                        }));
                    }
                    break;
                    }
                }
                let reserved: Result<_> = async {
                    let file = files::reserve(path).await?.into_std().await;
                    Ok((file, header(auth, &snapshot, None)?))
                }.await;
                let (file, header) = match reserved {
                    Ok(output) => output,
                    Err(error) => {
                        if !global { prepared.account_capture(&mut budget, &limits).await?; }
                        return Err(error);
                    }
                };
                let (result, _) = if global {
                    prepared.export_prepared_output((file, budget), Projection::Review, &limits).await
                } else { prepared
                    .export_file(
                        (file, budget),
                        header,
                        request,
                        (&snapshot, Some(&scope)),
                        None,
                        &limits,
                    )
                    .await };
                result.context("management review export failed")?;
            } else {
                prepared.account_capture(&mut budget, &limits).await?;
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
            if plan.items().iter().any(|item| {
                matches!(item.status, PlanStatus::Blocked(_))
                    || item.status == PlanStatus::Candidate
                        && !acknowledgements.contains(&item.request.item_id)
            }) || acknowledgements.iter().any(|id| {
                !plan
                    .items()
                    .iter()
                    .any(|item| item.status == PlanStatus::Candidate && &item.request.item_id == id)
            }) {
                return Err(input::invalid());
            }
            let (result, budget) = prepared
                .preflight_local(
                    budget,
                    header(auth, &snapshot, None)?,
                    request,
                    (&snapshot, Some(&scope)),
                    Some(acknowledgements),
                    &limits,
                )
                .await;
            result.context("management confirmation export preflight failed")?;
            let file = files::reserve(output).await?.into_std().await;
            budget.check()?;
            let binding = records::confirm_with_session(
                db,
                config,
                auth,
                (request, &snapshot, plan),
                acknowledgements,
                chrono::Duration::seconds(i64::from(*ttl_seconds)),
                work,
            )
            .await
            .context("management confirmation publication failed")?;
            // The public wire is exported, not the private binding or DB proposal.
            // Confirmation's reviewed tuple must remain exactly the one encoded.
            if &*binding.request != request
                || binding.confirmation.candidate_item_ids.as_slice() != acknowledgements
            {
                return Err(input::invalid());
            }
            let (result, _) = prepared
                .export_confirmed_file(db, (file, budget), &binding, Projection::Review, &limits)
                .await;
            result.context("management confirmed review export failed")?;
            ("confirm", true)
        }
        _ => return Err(input::invalid()),
    };
    let mut report = json!({
        "mode": mode, "database_writes": mode == "confirm", "artifact_exported": artifact_exported,
        "artifact_trust": if mode == "confirm" { "public_original_confirmation_requires_local_database_authorization" } else { "untrusted_proposal_not_authority" },
        "source_rpc": "not_performed", "provider_rpc": "not_performed",
        "summary": plan.summary(), "needs_attention": needs_attention,
        "items": plan.items().iter().enumerate().map(|(index, entry)| json!({"item_index": index, "status": entry.status, "step": entry.step})).collect::<Vec<_>>(),
    });
    if command.checkpoint().is_some() {
        report["outcome"] = json!("complete");
    }
    Ok(report)
    }.await
    };
    let complete = result
        .as_ref()
        .is_ok_and(|report| report["outcome"] != "incomplete");
    let closed = if prepared.checkpoint_path().is_some() && complete {
        prepared.cleanup().await
    } else {
        prepared.close().await
    };
    match (result, closed) {
        (Err(error), _) => Err(error),
        (Ok(mut report), Err(error)) if provider_confirmation => {
            if report.get("checkpoint").is_some() {
                report["checkpoint_retained"] = Value::Null;
                report["checkpoint_cleanup_failed"] = json!(true);
            }
            Err(confirmation::failure(report, error.into()))
        }
        (_, Err(error)) => Err(error).context("management task spool cleanup failed"),
        (Ok(report), Ok(())) => {
            if let Err(error) = limits.check() {
                return Err(if provider_confirmation {
                    confirmation::failure(report, error.into())
                } else {
                    error.into()
                });
            }
            Ok(report)
        }
    }
}

fn header<'a>(
    auth: &'a LocalAuthorization,
    snapshot: &'a Snapshot,
    expires_at: Option<String>,
) -> Result<Header<'a>> {
    let frame = snapshot.native_frame.as_ref().ok_or_else(input::invalid)?;
    Ok(Header {
        projection: Projection::Review,
        database_scope: auth.database_scope(),
        database_identity: auth.database_identity(),
        principal: auth.principal(),
        catalog: &frame.schema_versions,
        created_at: snapshot
            .database_now
            .ok_or_else(input::invalid)?
            .to_rfc3339(),
        expires_at,
    })
}
