//! A transaction-local pin state: all logical assignments share physical quota and jobs.
use super::{PublicationLease, invalid, keys::AllocationKeys, sql};
use crate::{
    error::{AppError, AppResult},
    pinning::zip_policy::ZipIntentSelection,
    store::{
        entities::{
            pin_invocation_route, pin_job, pin_lease, pin_lease_requirement, pin_lease_target,
            pin_provider_usage, pin_replica_observation, pin_resource_history,
            pin_submit_observation, remote_pin, remote_pin_ledger,
        },
        pinning::{
            jobs,
            ledger::submission::AdmissionBarrier,
            management::{
                admission::work_error, native::meter::ResidentGuard, records::DecodeWork,
            },
            publication,
        },
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{ConnectionTrait, TryIntoModel};
use std::collections::{BTreeMap, BTreeSet};

mod plan;
mod project;
mod read;
mod write;

type Pair = (String, String);

#[derive(PartialEq)]
struct PinEvidence {
    providers: BTreeSet<String>,
    submissions: BTreeMap<String, pin_submit_observation::Model>,
    histories: BTreeMap<String, jobs::history::Model>,
    releases: BTreeMap<(String, String, i64), pin_resource_history::Model>,
    replicas: BTreeMap<i64, pin_replica_observation::Model>,
}

pub(super) struct LockedPins {
    owners: Vec<String>,
    pairs: BTreeSet<Pair>,
    evidence: PinEvidence,
    requirements: BTreeMap<String, pin_lease_requirement::Model>,
    leases: BTreeMap<String, pin_lease::Model>,
    targets: BTreeMap<String, pin_lease_target::Model>,
    remotes: BTreeMap<Pair, remote_pin::Model>,
    ledgers: BTreeMap<Pair, remote_pin_ledger::Model>,
    usages: BTreeMap<String, pin_provider_usage::Model>,
    jobs: BTreeMap<String, pin_job::Model>,
    invocations: BTreeMap<String, pin_invocation_route::Model>,
    barriers: BTreeMap<String, AdmissionBarrier>,
    retired_sources: BTreeSet<Pair>,
    unconfirmed_sources: BTreeSet<Pair>,
    new_allocation_safe: bool,
    cluster_confirmed: BTreeSet<(String, Pair)>,
    resident: Vec<ResidentGuard>,
}

pub(super) struct PinWriteSet {
    before: LockedPins,
    publication_owners: Vec<String>,
    leases: BTreeMap<String, pin_lease::Model>,
    targets: BTreeMap<String, pin_lease_target::Model>,
    remotes: BTreeMap<Pair, remote_pin::Model>,
    ledgers: BTreeMap<Pair, remote_pin_ledger::Model>,
    usages: BTreeMap<String, pin_provider_usage::Model>,
    jobs: BTreeMap<String, pin_job::Model>,
    invocations: BTreeMap<String, pin_invocation_route::Model>,
    requirements: Vec<pin_lease_requirement::Model>,
    affected: BTreeSet<Pair>,
    ended: BTreeSet<Pair>,
    resident: Vec<ResidentGuard>,
}

fn desired(state: &str) -> bool {
    matches!(state, "waiting" | "submitted" | "pinned" | "degraded")
}
fn increment(value: i64) -> AppResult<i64> {
    value.checked_add(1).ok_or_else(invalid)
}

impl LockedPins {
    pub fn plan(
        self,
        selected: &ZipIntentSelection,
        keys: &AllocationKeys,
        owners: &[String],
        request: &super::CapturedZipPublication,
        now: DateTime<Utc>,
        work: DecodeWork<'_>,
    ) -> AppResult<PinWriteSet> {
        let cloned = self.resident.iter().try_fold(0usize, |total, guard| {
            total
                .checked_add(guard.bytes())
                .ok_or(AppError::ManagementBudget)
        })?;
        let owner_bytes =
            request
                .source
                .iter()
                .chain(&request.successes)
                .try_fold(0usize, |total, object| {
                    total
                        .checked_add(object.id.len())
                        .and_then(|bytes| bytes.checked_add(64))
                        .ok_or(AppError::ManagementBudget)
                })?;
        let resident = sql::retain_copy(
            work,
            cloned
                .checked_add(owner_bytes)
                .ok_or(AppError::ManagementBudget)?,
        )?;
        let mut planned = PinWriteSet {
            publication_owners: request
                .source
                .iter()
                .chain(&request.successes)
                .map(|object| object.id.clone())
                .collect(),
            leases: self.leases.clone(),
            targets: self.targets.clone(),
            remotes: self.remotes.clone(),
            ledgers: self.ledgers.clone(),
            usages: self.usages.clone(),
            jobs: self.jobs.clone(),
            invocations: self.invocations.clone(),
            before: self,
            requirements: Vec::new(),
            affected: BTreeSet::new(),
            ended: BTreeSet::new(),
            resident: vec![resident],
        };
        planned.end_owners(owners, now)?;
        for lease in &selected.leases {
            planned.assign(lease, keys, now, work)?;
        }
        planned.project(keys, now, work)?;
        Ok(planned)
    }
}

impl PinWriteSet {
    fn ensure_job(
        &mut self,
        job: jobs::NewPinJob,
        now: DateTime<Utc>,
        work: DecodeWork<'_>,
    ) -> AppResult<()> {
        let new = jobs::new_job_active_model(job, now).try_into_model()?;
        match self.jobs.get_mut(&new.id) {
            None => {
                let pair = (new.provider.clone(), new.cid.clone());
                let route = self.ledgers.get(&pair).and_then(|row| row.route.clone());
                let bytes = new
                    .id
                    .len()
                    .checked_add(new.provider.len())
                    .and_then(|n| n.checked_add(new.cid.len()))
                    .and_then(|n| n.checked_add(new.lease_id.as_ref().map_or(0, String::len)))
                    .and_then(|n| n.checked_add(new.target_id.as_ref().map_or(0, String::len)))
                    .and_then(|n| n.checked_add(route.as_ref().map_or(0, String::len)))
                    .and_then(|n| n.checked_add(512))
                    .ok_or(AppError::ManagementBudget)?;
                self.resident.push(sql::retain_copy(work, bytes)?);
                if let Some(route) = route {
                    let remote_epoch = self.remotes.get(&pair).ok_or_else(invalid)?.epoch;
                    self.invocations.insert(
                        new.id.clone(),
                        pin_invocation_route::Model {
                            job_id: new.id.clone(),
                            route,
                            remote_epoch,
                        },
                    );
                }
                self.jobs.insert(new.id.clone(), new);
            }
            Some(old) => match old.state.as_str() {
                "pending" => {
                    if new.operation == "reconcile" && new.next_attempt_at < old.next_attempt_at {
                        old.next_attempt_at = new.next_attempt_at;
                        old.updated_at = now;
                    }
                }
                "running" => {}
                "done" => {
                    let created = old.created_at;
                    *old = new;
                    old.created_at = created;
                }
                _ => return Err(invalid()),
            },
        }
        Ok(())
    }
}
