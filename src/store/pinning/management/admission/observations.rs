//! Append actual old invocation facts without changing any lease/resource lifetime.
use sea_orm::{ConnectionTrait, EntityTrait, Set};
use chrono::{DateTime, Utc};
use crate::{error::{AppError, AppResult}, pinning::{identity::ProviderRouteSnapshot, provider::{PinObservation, ProviderError, RemotePin, ObservedResource}}, store::{entities::{pin_invocation_route, pin_replica_observation}, pinning::jobs::ClaimedPinJob}};

pub(crate) async fn append_nonprojecting_observation<C: ConnectionTrait>(
    db: &C, claimed: &ClaimedPinJob, observation: &PinObservation, now: DateTime<Utc>,
) -> AppResult<()> {
    append_observation_result(db, claimed, observation.result.as_ref(), &observation.resources, now).await
}

async fn append_observation_result<C: ConnectionTrait>(
    db: &C, claimed: &ClaimedPinJob, result: Result<&RemotePin, &ProviderError>,
    observed_resources: &[ObservedResource], now: DateTime<Utc>,
) -> AppResult<()> {
    let capture = pin_invocation_route::Entity::find_by_id(claimed.model.id.clone()).one(db).await?
        .ok_or_else(|| AppError::Internal("historical observation invocation is missing".into()))?;
    let route: ProviderRouteSnapshot = serde_json::from_str(&capture.route)
        .map_err(|_| AppError::Internal("historical observation route is invalid".into()))?;
    let mut resources = observed_resources.to_vec();
    let outcome = match result {
        Ok(remote) => {
            if resources.is_empty() {
                // Legacy GET adapters return a parsed report without typed
                // replica resources. Preserve that actual report as Reported,
                // not recursive verification or exclusive creation evidence.
                resources.push(crate::pinning::provider::ObservedResource {
                    resource_type: route.resource_type(), cid: remote.cid.clone(), request_id: remote.request_id.clone(),
                    status: crate::pinning::provider::ObservedResourceStatus::Reported,
                    ownership: crate::pinning::identity::Ownership::Unknown, replica_evidence: None,
                });
            }
            match remote.status {
                crate::pinning::provider::RemotePinStatus::Queued => "ReportedQueued",
                crate::pinning::provider::RemotePinStatus::Pinning => "ReportedPinning",
                crate::pinning::provider::RemotePinStatus::Pinned => "ReportedPinned",
                crate::pinning::provider::RemotePinStatus::Failed => "ReportedFailed",
            }
        }
        Err(_) => "Unknown",
    };
    append_fact(db, claimed, &resources, outcome,
        result.err().map(|error| error.safe_evidence("management_read_only")), now).await
}

/// An actual successful/NotFound DELETE response, not exclusive ownership or
/// permission to release quota belonging to a newer epoch.
pub(crate) async fn append_nonprojecting_delete_acknowledgement<C: ConnectionTrait>(
    db: &C, claimed: &ClaimedPinJob, now: DateTime<Utc>,
) -> AppResult<()> {
    append_fact(db, claimed, &[], "DeleteAcknowledged", None, now).await
}

/// Borrow the actual DELETE error so the caller retains its full typed retry
/// input after committing this nonprojecting fact independently of settlement.
pub(crate) async fn append_nonprojecting_delete_error<C: ConnectionTrait>(
    db: &C, claimed: &ClaimedPinJob, error: &ProviderError, now: DateTime<Utc>,
) -> AppResult<()> {
    append_observation_result(db, claimed, Err(error), &[], now).await
}

async fn append_fact<C: ConnectionTrait>(
    db: &C, claimed: &ClaimedPinJob, resources: &[crate::pinning::provider::ObservedResource],
    outcome: &str, safe_error: Option<String>, now: DateTime<Utc>,
) -> AppResult<()> {
    let job = &claimed.model;
    let capture = pin_invocation_route::Entity::find_by_id(job.id.clone()).one(db).await?
        .ok_or_else(|| AppError::Internal("historical observation invocation is missing".into()))?;
    let _: ProviderRouteSnapshot = serde_json::from_str(&capture.route)
        .map_err(|_| AppError::Internal("historical observation route is invalid".into()))?;
    let until = job.locked_until.ok_or_else(|| AppError::Internal("historical observation claim is missing".into()))?;
    // This table is an append-only scoped observation ledger. Keep the real
    // operation and nonprojectable report classification; do not pretend a
    // readonly reconcile is a target Poll or forge a live replica proof.
    pin_replica_observation::Entity::insert(pin_replica_observation::ActiveModel {
        sequence: Default::default(), job_id: Set(job.id.clone()), provider: Set(job.provider.clone()),
        expected_cid: Set(job.cid.clone()), route: Set(capture.route), remote_epoch: Set(capture.remote_epoch),
        claim_until: Set(until), lease_id: Set(job.lease_id.clone()), target_id: Set(job.target_id.clone()),
        expected_generation: Set(job.expected_generation), phase: Set(job.operation.clone()),
        resources: Set(serde_json::to_string(resources).map_err(|_| AppError::Internal("historical observation serialization failed".into()))?),
        outcome: Set(outcome.into()), safe_error: Set(safe_error),
        projectable: Set(false), observed_at: Set(now),
    }).exec(db).await?;
    Ok(())
}
