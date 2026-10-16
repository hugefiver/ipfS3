//! Exact applied-batch execution scope. Neither caller IDs nor confirm TTL grant IO.
use crate::{
    error::{AppError, AppResult},
    store::{
        entities::{pin_job, pin_replica_observation, pin_submit_observation},
        pinning::management::{
            actions::{
                job_authority::{self, JobPermit},
                status,
            },
            records,
        },
    },
};
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder};

#[derive(Clone, Copy)]
pub(crate) enum ManagedCall {
    Submit,
    Observe,
    Cleanup,
}

pub(crate) async fn managed_call_allowed<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    call: ManagedCall,
) -> AppResult<bool> {
    managed_call_allowed_with_work(db, job, call, records::SharedDecodeBudget::default()).await
}
pub(crate) async fn managed_call_allowed_with_work<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    call: ManagedCall,
    work: records::SharedDecodeBudget,
) -> AppResult<bool> {
    Ok(
        match (
            job_authority::check_before_call_with_work(db, job, work)
                .await
                .map_err(job_authority::safe)?,
            call,
        ) {
            (None, _) => true, // Ordinary jobs still pass their original operation/retirement fences.
            (Some(JobPermit::Submit(_)), ManagedCall::Submit) => job.operation == "submit",
            (
                Some(JobPermit::Submit(_) | JobPermit::ReadOnly(_) | JobPermit::LedgerOnly(_)),
                ManagedCall::Observe,
            ) => true,
            (Some(JobPermit::ExistingCleanup(_)), ManagedCall::Cleanup) => job.operation == "unpin",
            _ => false,
        },
    )
}

/// Constructible only through the record layer's complete original applied
/// receipt validation. Its authority is re-read at every claim/heartbeat/call.
pub struct ManagementJobScope {
    auth: records::LocalAuthorization,
    review: records::ConfirmedPlan,
    seeds: Vec<records::JobAuthority>,
}

/// Original typed receipts and facts for B's read-only completion aggregation.
/// No number of jobs/targets is converted into physical completion here.
pub struct ManagementJobReport {
    pub items: Vec<records::CommittedAction>,
    pub submit_observations: Vec<pin_submit_observation::Model>,
    pub replica_observations: Vec<pin_replica_observation::Model>,
}

impl ManagementJobScope {
    /// Restore the exact local confirmation, then require every original business
    /// action to be committed. An expired draft/import is never execution scope.
    pub async fn from_imported<C: ConnectionTrait>(
        db: &C,
        auth: &records::LocalAuthorization,
        imported: &crate::pinning::management::wire::ImportedPlan,
    ) -> AppResult<Self> {
        let binding = records::load_confirmed_from_imported(db, auth, imported)
            .await
            .map_err(|_| invalid())?;
        Self::from_applied_with_work(db, auth, &binding, imported.session()).await
    }

    pub async fn from_applied<C: ConnectionTrait>(
        db: &C,
        auth: &records::LocalAuthorization,
        binding: &records::ConfirmedBinding,
    ) -> AppResult<Self> {
        let work = records::SharedDecodeBudget::default();
        Self::from_applied_with_work(db, auth, binding, work).await
    }
    pub async fn from_applied_with_work<C: ConnectionTrait>(
        db: &C,
        auth: &records::LocalAuthorization,
        binding: &records::ConfirmedPlan,
        work: records::SharedDecodeBudget,
    ) -> AppResult<Self> {
        let review = records::read_confirmed_plan(db, binding, work.clone())
            .await
            .map_err(|_| invalid())?;
        Self::from_review_with_work(db, auth, review, work).await
    }
    async fn from_review_with_work<C: ConnectionTrait>(
        db: &C,
        auth: &records::LocalAuthorization,
        review: records::ConfirmedPlan,
        work: records::SharedDecodeBudget,
    ) -> AppResult<Self> {
        let binding = review.view().with_work(&work);
        let mut seeds = std::collections::BTreeMap::new();
        if binding.request.items.is_empty()
            || binding.request.items.len() != binding.confirmation.bindings.len()
        {
            return Err(invalid());
        }
        let mut items = std::collections::BTreeSet::new();
        let actions =
            records::actions_for_plan_with_work(db, auth, &binding.request.plan_id, work.clone())
                .await
                .map_err(|_| invalid())?;
        if actions.len() != binding.confirmation.bindings.len() {
            return Err(invalid());
        }
        for entry in binding.confirmation.bindings {
            if !items.insert(&entry.request.item_id)
                || !binding.request.items.contains(&entry.request)
            {
                return Err(invalid());
            }
            let trusted = actions
                .iter()
                .find(|trusted| {
                    trusted.action_view().receipt.binding.request.item_id == entry.request.item_id
                })
                .ok_or_else(invalid)?;
            if trusted.binding() != binding {
                return Err(invalid());
            }
            let action = trusted.action_view();
            if action.state == records::ActionState::Stopped
                || action.receipt.binding != *entry
                || action
                    .jobs
                    .iter()
                    .map(|job| job.job_id.clone())
                    .collect::<Vec<_>>()
                    != action.receipt.job_ids
            {
                return Err(invalid());
            }
            let mut jobs = std::collections::BTreeSet::new();
            for authority in &action.jobs {
                trusted.validate_job(authority).map_err(|_| invalid())?;
                if !jobs.insert(&authority.job_id) {
                    return Err(invalid());
                }
                // A shared/reactivated source must meet the complete immutable
                // authority. Its current epoch/permit cannot replace this seed.
                if seeds
                    .get(&authority.job_id)
                    .is_some_and(|old| old != authority)
                {
                    return Err(invalid());
                }
                binding
                    .copy(&(&authority.job_id, authority))
                    .map_err(|_| invalid())?;
                seeds.insert(authority.job_id.clone(), authority.clone());
            }
        }
        binding
            .copy(&(
                auth.principal(),
                auth.database_scope(),
                auth.database_identity(),
            ))
            .map_err(|_| invalid())?;
        Ok(Self {
            auth: auth.clone(),
            review,
            seeds: seeds.into_values().collect(),
        })
    }

    #[cfg(test)]
    pub(crate) async fn revalidate<C: ConnectionTrait>(&self, db: &C) -> AppResult<()> {
        self.revalidate_with_work(db, records::SharedDecodeBudget::default())
            .await
    }
    pub(crate) async fn revalidate_with_work<C: ConnectionTrait>(
        &self,
        db: &C,
        work: records::SharedDecodeBudget,
    ) -> AppResult<()> {
        self.auth.check(db).await.map_err(|_| invalid())?;
        // lookup_committed validates the original private binding without
        // treating an already committed confirmation TTL as a job deadline.
        let current = records::actions_for_plan_with_work(
            db,
            &self.auth,
            &self.review.view().request.plan_id,
            work,
        )
        .await
        .map_err(|_| invalid())?;
        if current.len() != self.review.view().confirmation.bindings.len()
            || current
                .iter()
                .any(|trusted| trusted.binding() != self.review.view())
        {
            return Err(invalid());
        }
        let mut seeds = std::collections::BTreeMap::new();
        for trusted in &current {
            for seed in &trusted.action_view().jobs {
                if seeds
                    .insert(&seed.job_id, seed)
                    .is_some_and(|old| old != seed)
                {
                    return Err(invalid());
                }
            }
        }
        if !seeds.into_values().eq(self.seeds.iter()) {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) fn candidates_with_work(
        &self,
        work: records::DecodeWork<'_>,
    ) -> AppResult<Condition> {
        work.with(|work| work.nodes(1 + self.seeds.len() * 5))
            .map_err(|_| invalid())?;
        for seed in &self.seeds {
            for _ in 0..3 {
                work.copy(&(
                    &seed.job_id,
                    &seed.lease_id,
                    &seed.target_id,
                    seed.lease_generation,
                    &seed.cid,
                ))
                .map_err(|_| invalid())?;
            }
        }
        let mut selected = Condition::any()
            .add(pin_job::Column::Id.is_in(self.seeds.iter().map(|seed| seed.job_id.clone())));
        for seed in self
            .seeds
            .iter()
            .filter(|seed| seed.directive == records::JobDirective::Submit)
        {
            if let (Some(lease), Some(target), Some(generation)) =
                (&seed.lease_id, &seed.target_id, seed.lease_generation)
            {
                selected = selected.add(
                    Condition::all()
                        .add(pin_job::Column::Operation.eq("poll"))
                        .add(pin_job::Column::LeaseId.eq(lease))
                        .add(pin_job::Column::TargetId.eq(target))
                        .add(pin_job::Column::ExpectedGeneration.eq(generation))
                        .add(pin_job::Column::Cid.eq(&seed.cid)),
                );
            }
        }
        Ok(selected)
    }

    pub(crate) async fn contains_with_work<C: ConnectionTrait>(
        &self,
        db: &C,
        job: &pin_job::Model,
        work: records::SharedDecodeBudget,
    ) -> AppResult<bool> {
        let Some(authority) = job_authority::lookup_with_work(db, job, work.clone())
            .await
            .map_err(|_| invalid())?
        else {
            return Ok(false);
        };
        for seed in &self.seeds {
            records::DecodeWork::shared(&work)
                .copy(&(seed, &job.id))
                .map_err(|_| invalid())?;
            let mut original = seed.clone();
            // B has already verified the native, request-bound successor and
            // complete compatible meet. Only its job ID differs from our seed.
            if job.operation == "poll" && seed.directive == records::JobDirective::Submit {
                original.job_id.clone_from(&job.id);
            }
            if original == authority {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) async fn report<C: ConnectionTrait>(
        &self,
        db: &C,
    ) -> AppResult<ManagementJobReport> {
        let work = records::SharedDecodeBudget::default();
        self.revalidate_with_work(db, work.clone()).await?;
        let mut ids = self
            .seeds
            .iter()
            .map(|seed| seed.job_id.clone())
            .collect::<Vec<_>>();
        for job in pin_job::Entity::find()
            .filter(self.candidates_with_work(records::DecodeWork::shared(&work))?)
            .all(db)
            .await?
        {
            if self.contains_with_work(db, &job, work.clone()).await? {
                ids.push(job.id);
            }
        }
        ids.sort();
        ids.dedup();
        let mut items = Vec::new();
        let view = self.review.view();
        for trusted in
            records::actions_for_plan_with_work(db, &self.auth, &view.request.plan_id, work.clone())
                .await
                .map_err(|_| invalid())?
        {
            if trusted.binding() != view {
                return Err(invalid());
            }
            records::DecodeWork::shared(&work)
                .copy(trusted.action_view())
                .map_err(|_| invalid())?;
            let mut action = trusted.action_view().clone();
            action.state = status::current_state_with_work(db, &action, work.clone())
                .await
                .map_err(|_| invalid())?;
            items.push(action);
        }
        let submit_observations = pin_submit_observation::Entity::find()
            .filter(pin_submit_observation::Column::JobId.is_in(ids.clone()))
            .order_by_asc(pin_submit_observation::Column::StartedAt)
            .all(db)
            .await?;
        let replica_observations = pin_replica_observation::Entity::find()
            .filter(pin_replica_observation::Column::JobId.is_in(ids))
            .order_by_asc(pin_replica_observation::Column::Sequence)
            .all(db)
            .await?;
        Ok(ManagementJobReport {
            items,
            submit_observations,
            replica_observations,
        })
    }
}

fn invalid() -> AppError {
    AppError::InvalidPinningRequest(
        "management execution scope is missing, stale, or invalid; needs attention".into(),
    )
}
