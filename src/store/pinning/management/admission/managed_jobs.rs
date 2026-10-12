//! Exact applied-batch execution scope. Neither caller IDs nor confirm TTL grant IO.
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder};
use crate::{error::{AppError, AppResult}, store::{entities::{pin_job, pin_replica_observation, pin_submit_observation}, pinning::management::{actions::{job_authority::{self, JobPermit}, status}, records}}};

#[derive(Clone, Copy)]
pub(crate) enum ManagedCall { Submit, Observe, Cleanup }

pub(crate) async fn managed_call_allowed<C: ConnectionTrait>(db: &C, job: &pin_job::Model, call: ManagedCall) -> AppResult<bool> {
    Ok(match (job_authority::check_before_call(db, job).await?, call) {
        (None, _) => true, // Ordinary jobs still pass their original operation/retirement fences.
        (Some(JobPermit::Submit(_)), ManagedCall::Submit) => job.operation == "submit",
        (Some(JobPermit::Submit(_) | JobPermit::ReadOnly(_) | JobPermit::LedgerOnly(_)), ManagedCall::Observe) => true,
        (Some(JobPermit::ExistingCleanup(_)), ManagedCall::Cleanup) => job.operation == "unpin",
        _ => false,
    })
}

/// Constructible only through the record layer's complete original applied
/// receipt validation. Its authority is re-read at every claim/heartbeat/call.
pub struct ManagementJobScope {
    auth: records::LocalAuthorization,
    binding: records::ConfirmedBinding,
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
        db: &C, auth: &records::LocalAuthorization,
        imported: &crate::pinning::management::wire::ImportedPlan,
    ) -> AppResult<Self> {
        let binding = records::load_confirmed_from_imported(db, auth, imported).await
            .map_err(|_| invalid())?;
        Self::from_applied(db, auth, &binding).await
    }

    pub async fn from_applied<C: ConnectionTrait>(db: &C, auth: &records::LocalAuthorization, binding: &records::ConfirmedBinding) -> AppResult<Self> {
        let mut seeds = std::collections::BTreeMap::new();
        if binding.request.items.is_empty() || binding.request.items.len() != binding.confirmation.bindings.len() { return Err(invalid()); }
        let mut items = std::collections::BTreeSet::new();
        for entry in &binding.confirmation.bindings {
            if !items.insert(&entry.request.item_id) || !binding.request.items.contains(&entry.request) { return Err(invalid()); }
            let action = records::lookup_committed(db, auth, binding, &entry.request.item_id).await
                .map_err(|_| invalid())?.ok_or_else(invalid)?;
            if action.state == records::ActionState::Stopped || action.receipt.binding != *entry
                || action.jobs.iter().map(|job| job.job_id.clone()).collect::<Vec<_>>() != action.receipt.job_ids
            { return Err(invalid()); }
            let mut jobs = std::collections::BTreeSet::new();
            for authority in &action.jobs {
                job_authority::validate_stored(binding, &action, authority).map_err(|_| invalid())?;
                if !jobs.insert(&authority.job_id) { return Err(invalid()); }
                // A shared/reactivated source must meet the complete immutable
                // authority. Its current epoch/permit cannot replace this seed.
                if seeds.get(&authority.job_id).is_some_and(|old| old != authority) { return Err(invalid()); }
                seeds.insert(authority.job_id.clone(), authority.clone());
            }
        }
        Ok(Self { auth: auth.clone(), binding: binding.clone(), seeds: seeds.into_values().collect() })
    }

    pub(crate) async fn revalidate<C: ConnectionTrait>(&self, db: &C) -> AppResult<()> {
        self.auth.check(db).await.map_err(|_| invalid())?;
        // lookup_committed validates the original private binding without
        // treating an already committed confirmation TTL as a job deadline.
        let current = Self::from_applied(db, &self.auth, &self.binding).await?;
        if current.seeds != self.seeds { return Err(invalid()); }
        Ok(())
    }

    pub(crate) fn candidates(&self) -> Condition {
        let mut selected = Condition::any().add(pin_job::Column::Id.is_in(self.seeds.iter().map(|seed| seed.job_id.clone())));
        for seed in self.seeds.iter().filter(|seed| seed.directive == records::JobDirective::Submit) {
            if let (Some(lease), Some(target), Some(generation)) = (&seed.lease_id, &seed.target_id, seed.lease_generation) {
                selected = selected.add(Condition::all()
                    .add(pin_job::Column::Operation.eq("poll"))
                    .add(pin_job::Column::LeaseId.eq(lease))
                    .add(pin_job::Column::TargetId.eq(target))
                    .add(pin_job::Column::ExpectedGeneration.eq(generation))
                    .add(pin_job::Column::Cid.eq(&seed.cid)));
            }
        }
        selected
    }

    pub(crate) async fn contains<C: ConnectionTrait>(&self, db: &C, job: &pin_job::Model) -> AppResult<bool> {
        let Some(authority) = job_authority::lookup(db, job).await.map_err(|_| invalid())? else { return Ok(false); };
        Ok(self.seeds.iter().any(|seed| {
            let mut original = seed.clone();
            // B has already verified the native, request-bound successor and
            // complete compatible meet. Only its job ID differs from our seed.
            if job.operation == "poll" && seed.directive == records::JobDirective::Submit { original.job_id.clone_from(&job.id); }
            original == authority
        }))
    }

    pub(crate) async fn report<C: ConnectionTrait>(&self, db: &C) -> AppResult<ManagementJobReport> {
        self.revalidate(db).await?;
        let mut ids = self.seeds.iter().map(|seed| seed.job_id.clone()).collect::<Vec<_>>();
        for job in pin_job::Entity::find().filter(self.candidates()).all(db).await? {
            if self.contains(db, &job).await? { ids.push(job.id); }
        }
        ids.sort(); ids.dedup();
        let mut items = Vec::new();
        for entry in &self.binding.confirmation.bindings {
            items.push(status::lookup(db, &self.auth, &self.binding, &entry.request.item_id)
                .await.map_err(|_| invalid())?.ok_or_else(invalid)?);
        }
        let submit_observations = pin_submit_observation::Entity::find()
            .filter(pin_submit_observation::Column::JobId.is_in(ids.clone()))
            .order_by_asc(pin_submit_observation::Column::StartedAt).all(db).await?;
        let replica_observations = pin_replica_observation::Entity::find()
            .filter(pin_replica_observation::Column::JobId.is_in(ids))
            .order_by_asc(pin_replica_observation::Column::Sequence).all(db).await?;
        Ok(ManagementJobReport { items, submit_observations, replica_observations })
    }
}

fn invalid() -> AppError { AppError::InvalidPinningRequest("management execution scope is missing, stale, or invalid; needs attention".into()) }
