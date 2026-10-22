//! Dynamic, reply-only status for locally verified converted originals.
use super::*;
use crate::pinning::management::{
    ConfigurationBinding, RetirementStep, Subject,
    retirement::{CurrentRetirementScan, RunLimits, Workspace},
};
use std::time::{Duration, Instant};

#[derive(serde::Serialize)]
pub struct RecordedRetirementStatus {
    pub observed_at: chrono::DateTime<chrono::Utc>,
    pub responsibility: RetirementStep,
}

#[derive(serde::Serialize)]
pub struct RecordedItemStatus {
    pub item_id: String,
    pub state: records::ActionState,
    pub retirement: Option<RecordedRetirementStatus>,
}

/// Read current facts without replacing the recorded result or manufacturing an
/// execution binding. Incomplete scans return Budget, never a successful prefix
/// or the historical state. Each lookup is one item/scan task; its limits and
/// decode session span all phases without an internal retry or budget reset.
pub async fn recorded_with_work(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    plan: &records::RecordedPlan,
    item_id: &str,
    limits: &RunLimits,
    work: records::SharedDecodeBudget,
) -> Result<RecordedItemStatus> {
    records::DecodeWork::shared(&work).with(|work| work.check())?;
    let started = Instant::now();
    let budget = tokio::sync::Mutex::new(native::meter::Budget::default());
    let result = async {
        let action = plan
            .actions()
            .iter()
            .find(|action| fields(action).0.item_id == item_id)
            .ok_or(ManagementError::Confirmation)?;
        let local = local_state(
            db,
            auth,
            (plan, item_id),
            limits,
            &work,
            &budget,
            remaining(limits, started)?,
        )
        .await?;
        let (request, _, _) = fields(action);
        let (state, retirement) = match local {
            Some(state) => (state, None),
            None => {
                let (state, current) = retirement_state(
                    db, config, auth, plan, action, limits, &work, &budget, started,
                )
                .await?;
                (state, Some(current))
            }
        };
        records::DecodeWork::shared(&work).copy(&request.item_id)?;
        records::DecodeWork::shared(&work).with(|work| work.nodes(1))?;
        remaining(limits, started)?;
        Ok(RecordedItemStatus {
            item_id: request.item_id.clone(),
            state,
            retirement,
        })
    }
    .await;
    super::super::super::admission::authority::charge(
        &budget.into_inner(),
        records::DecodeWork::shared(&work),
    )?;
    records::DecodeWork::shared(&work).with(|work| work.check())?;
    result
}

fn fields(
    action: &records::RecordedResult,
) -> (
    &crate::pinning::management::ItemRequest,
    &[records::JobAuthority],
    &[String],
) {
    match action {
        records::RecordedResult::Current(action) => (
            &action.receipt.binding.request,
            &action.jobs,
            &action.targets,
        ),
        records::RecordedResult::Historical(action) => (
            &action.receipt.binding.request,
            &action.jobs,
            &action.targets,
        ),
    }
}

fn remaining(limits: &RunLimits, started: Instant) -> Result<Duration> {
    if limits.cancellation.is_cancelled() {
        return Err(ManagementError::Budget);
    }
    limits
        .elapsed
        .checked_sub(started.elapsed())
        .filter(|left| !left.is_zero())
        .ok_or(ManagementError::Budget)
}

async fn local_state(
    db: &DatabaseConnection,
    auth: &records::LocalAuthorization,
    (plan, item_id): (&records::RecordedPlan, &str),
    limits: &RunLimits,
    work: &records::SharedDecodeBudget,
    budget: &tokio::sync::Mutex<native::meter::Budget>,
    remaining: Duration,
) -> Result<Option<records::ActionState>> {
    let mut tx = ManagementTransaction::begin_read_only(db, remaining, limits.cancellation.clone())
        .await
        .map_err(TransactionFailure::management_error)?;
    let result = tx
        .run_with(
            (auth, plan, item_id, work.clone(), budget),
            |tx, _, (auth, plan, item_id, work, budget)| {
                Box::pin(async move {
                    plan.check_current(tx, auth, &mut *budget.lock().await)
                        .await?;
                    let action = plan
                        .actions()
                        .iter()
                        .find(|action| fields(action).0.item_id == *item_id)
                        .ok_or(ManagementError::Confirmation)?;
                    if matches!(
                        fields(action).0.action,
                        Action::Retire | Action::RetireWith { .. }
                    ) {
                        Ok(None)
                    } else if let records::RecordedResult::Current(action) = action {
                        current_state_with_work(tx, action, work.clone())
                            .await
                            .map(Some)
                    } else {
                        Err(ManagementError::InvalidEvidence)
                    }
                })
            },
        )
        .await
        .map_err(TransactionFailure::management_error);
    tx.rollback()
        .await
        .map_err(TransactionFailure::management_error)?;
    result
}

#[allow(clippy::too_many_arguments)]
async fn retirement_state(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    plan: &records::RecordedPlan,
    action: &records::RecordedResult,
    limits: &RunLimits,
    work: &records::SharedDecodeBudget,
    budget: &tokio::sync::Mutex<native::meter::Budget>,
    started: Instant,
) -> Result<(records::ActionState, RecordedRetirementStatus)> {
    let (request, jobs, targets) = fields(action);
    let Subject::Provider { route } = &request.subject else {
        return Err(ManagementError::InvalidEvidence);
    };
    let configuration = ConfigurationBinding {
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
            .map(|(key, limit)| (key.clone(), limit.clone()))
            .collect(),
        zip_rules: Some(config.zip_rules.clone()),
        source_node: config.source_node().map(str::to_owned),
    };
    records::DecodeWork::shared(work).copy(&(route, &configuration))?;
    remaining(limits, started)?;
    let mut workspace = Workspace::create()?;
    let result = async {
        let (path, file) = workspace.reserve().await?;
        let mut scan =
            CurrentRetirementScan::start(db, route.clone(), configuration, &path, file, limits)
                .await?;
        let result = async {
            let (fence, state) = scan
                .prepare_action_status(db, limits)
                .await?
                .ok_or(ManagementError::Budget)?;
            let mut tx = ManagementTransaction::begin_read_only(
                db,
                remaining(limits, started)?,
                limits.cancellation.clone(),
            )
            .await
            .map_err(TransactionFailure::management_error)?;
            let result =
                tx
                    .run_with(
                        (
                            auth,
                            plan,
                            work.clone(),
                            budget,
                            limits,
                            request,
                            jobs,
                            targets,
                            fence,
                            state,
                        ),
                        |tx,
                         _,
                         (
                            auth,
                            plan,
                            work,
                            budget,
                            limits,
                            request,
                            jobs,
                            targets,
                            fence,
                            state,
                        )| {
                            Box::pin(async move {
                                let current = fence.current_status(tx, limits).await?;
                                plan.check_current(tx, auth, &mut *budget.lock().await)
                                    .await?;
                                let state = if *state == records::ActionState::NeedsAttention
                                    || observations::needs_attention(tx, jobs, work.clone()).await?
                                {
                                    records::ActionState::NeedsAttention
                                } else {
                                    retirement::destinations(
                                        tx,
                                        &request.action,
                                        targets,
                                        *state,
                                        work.clone(),
                                    )
                                    .await?
                                };
                                fence.current_status(tx, limits).await?;
                                Ok((
                                    state,
                                    RecordedRetirementStatus {
                                        observed_at: current.observed_at,
                                        responsibility: current.responsibility,
                                    },
                                ))
                            })
                        },
                    )
                    .await
                    .map_err(TransactionFailure::management_error);
            tx.rollback()
                .await
                .map_err(TransactionFailure::management_error)?;
            result
        }
        .await;
        scan.close().await?;
        result
    }
    .await;
    workspace.cleanup()?;
    result
}
