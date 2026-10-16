use super::*;
use crate::pinning::management::{Action, Capture, PlanEntry, PlannedStep, Subject};
use sea_orm::DatabaseTransaction;

pub(super) async fn execute_with_jobs(
    tx: &DatabaseTransaction,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    entry: &PlanEntry,
    before: records::FrozenRows<'_>,
    existing_jobs: &std::collections::BTreeSet<&str>,
    view: records::BindingView<'_>,
) -> Result<(
    records::CommittedAction,
    Option<crate::store::pinning::leases::ManualLeaseRenewalOutcome>,
)> {
    view.copy(entry)?;
    let mut result = records::CommittedAction {
        receipt: records::ActionReceipt {
            binding: entry.clone(),
            intent_ids: vec![],
            job_ids: vec![],
        },
        state: records::ActionState::Accepted,
        targets: vec![],
        root_releases: vec![],
        jobs: vec![],
        migration_points: vec![],
        decision: None,
    };
    let mut renewal = None;
    match (&entry.capture, &entry.step, &entry.request.subject) {
        (
            Some(Capture::Object(capture)),
            Some(PlannedStep::Object(step)),
            Subject::Object { .. },
        ) => match &entry.request.action {
            Action::Create(intent) | Action::Reapply(intent) | Action::Backfill(intent) => {
                intents::create(tx, config, view, capture, intent, before, &mut result).await?
            }
            Action::Renew { expires_at } => {
                renewal = Some(
                    intents::renew(tx, config, capture, *expires_at, &mut result, view.into())
                        .await?,
                )
            }
            Action::Cancel => intents::cancel(tx, capture, &mut result, view.into()).await?,
            Action::Retry | Action::Reconcile => {
                recovery::execute(
                    tx,
                    capture,
                    step,
                    &entry.request.action,
                    before,
                    &mut result,
                    view.into(),
                )
                .await?
            }
            Action::Migrate { .. } => {
                migration::execute(tx, config, entry, before, &mut result, view.into()).await?
            }
            _ => return Err(ManagementError::Confirmation),
        },
        (
            Some(Capture::Provider(capture)),
            Some(PlannedStep::Retire(_)),
            Subject::Provider { route },
        ) => {
            retirement::execute(tx, auth, view, entry, capture, route, &mut result).await?;
        }
        _ => return Err(ManagementError::Confirmation),
    }
    let mut models = Vec::new();
    view.with_decode_work(|work| work.nodes(1 + result.targets.len()))?;
    for id in &result.targets {
        models.push(
            records::current_model::<crate::store::entities::pin_lease_target::Entity, _>(
                tx,
                &[id],
                view.into(),
            )
            .await?
            .ok_or(ManagementError::Changed)?,
        );
    }
    // A shared/reactivated job retains its actual source, not this referencing
    // lease's owner. Every additional receipt carries the same compatible proof.
    let mut values = Vec::<sea_orm::Value>::new();
    let mut parameter = |ids: &[String]| -> Result<String> {
        view.with_decode_work(|work| work.nodes(1 + ids.len()))?;
        let mut terms = Vec::new();
        for id in ids {
            for _ in 0..2 {
                view.copy(id)?;
            }
            values.push(id.clone().into());
            terms.push(format!("${}", values.len()));
        }
        Ok(if terms.is_empty() {
            "NULL".into()
        } else {
            terms.join(",")
        })
    };
    let leases = parameter(&result.receipt.intent_ids)?;
    let targets = parameter(&result.targets)?;
    let cancel = if matches!(entry.request.action, Action::Cancel) {
        format!(
            "OR EXISTS (SELECT 1 FROM pin_lease_targets t WHERE t.lease_id IN ({leases}) AND t.provider=pin_jobs.provider AND t.cid=pin_jobs.cid)"
        )
    } else {
        String::new()
    };
    let source = format!(
        "FROM pin_jobs WHERE state IN ('pending','retry','running','blocked') AND (lease_id IN ({leases}) OR target_id IN ({targets}) OR EXISTS (SELECT 1 FROM pin_lease_targets t WHERE t.id IN ({targets}) AND t.provider=pin_jobs.provider AND t.cid=pin_jobs.cid) {cancel})"
    );
    let after = records::current_models::<crate::store::entities::pin_job::Entity, _>(
        tx,
        &source,
        "pin_jobs.id",
        values,
        native::MAX_ROWS,
        view.into(),
    )
    .await?;
    for job in after {
        if !matches!(
            job.state.as_str(),
            "pending" | "retry" | "running" | "blocked"
        ) || result
            .jobs
            .iter()
            .any(|authority| authority.job_id == job.id)
        {
            continue;
        }
        // Cancellation publishes its new remote frontier, not an old generation's receipt.
        if matches!(entry.request.action, Action::Cancel) && existing_jobs.contains(job.id.as_str())
        {
            continue;
        }
        // Renewal leaves old claims/history intact. An old generation is not
        // re-stamped with the renewed lease's generation by job birth.
        if matches!(job.operation.as_str(), "submit" | "poll")
            && !crate::store::pinning::jobs::check_target_job_generation_with_work(
                tx,
                &job,
                view.into(),
            )
            .await?
        {
            continue;
        }
        let lease_matches = job
            .lease_id
            .as_ref()
            .is_some_and(|id| result.receipt.intent_ids.contains(id));
        let target_matches = job
            .target_id
            .as_ref()
            .is_some_and(|id| result.targets.contains(id));
        let shared_matches = models.iter().any(|target| {
            result.targets.contains(&target.id)
                && target.provider == job.provider
                && target.cid == job.cid
        });
        let remote_matches = matches!(entry.request.action, Action::Cancel)
            && before.table("pin_lease_targets").is_some_and(|table| {
                table.rows.iter().any(|target| {
                    native::value(target, "lease_id").is_some_and(|id| {
                        result.receipt.intent_ids.iter().any(|intent| intent == id)
                    }) && native::value(target, "provider") == Some(&job.provider)
                        && native::value(target, "cid") == Some(&job.cid)
                })
            });
        let born_observation = shared_matches
            && job.operation == "reconcile"
            && !existing_jobs.contains(job.id.as_str());
        if matches!(entry.request.action, Action::Renew { .. }) && !born_observation {
            continue;
        }
        if lease_matches
            || target_matches
            || remote_matches
            || shared_matches && matches!(job.operation.as_str(), "submit" | "poll")
            || born_observation
        {
            let directive = match job.operation.as_str() {
                "submit" | "poll" => records::JobDirective::Submit,
                "reconcile" => records::JobDirective::ReadOnlyObserve,
                "unpin" => records::JobDirective::ExistingCleanup,
                _ => return Err(ManagementError::InvalidEvidence),
            };
            let born_intent_work = lease_matches
                && before.row("pin_jobs", &[&job.id]).is_none()
                && matches!(
                    entry.request.action,
                    Action::Create(_) | Action::Backfill(_) | Action::Reapply(_)
                );
            if directive == records::JobDirective::Submit
                && (matches!(entry.request.action, Action::Cancel | Action::Reconcile)
                    || !born_intent_work
                        && result
                            .jobs
                            .iter()
                            .any(|job| job.directive == records::JobDirective::ReadOnlyObserve))
            {
                continue;
            }
            result.jobs.push(
                job_authority::birth(
                    tx,
                    &job,
                    entry,
                    directive,
                    before,
                    &result.receipt.intent_ids,
                    view.into(),
                )
                .await?,
            );
        }
    }
    result
        .jobs
        .sort_by(|left, right| left.job_id.cmp(&right.job_id));
    result.receipt.job_ids = result.jobs.iter().map(|job| job.job_id.clone()).collect();
    result.targets.sort();
    result.targets.dedup();
    Ok((result, renewal))
}
