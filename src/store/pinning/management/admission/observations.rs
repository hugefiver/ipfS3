//! Append actual old invocation facts without changing any lease/resource lifetime.
use crate::{
    error::{AppError, AppResult},
    pinning::provider::{
        ObservedResource, PinObservation, ProviderError, RemotePin, SubmitEffect, SubmitObservation,
    },
    store::{
        entities::{pin_replica_observation, pin_submit_observation},
        pinning::management::actions::job_authority::InvocationCapture,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{ConnectionTrait, EntityTrait, Set};

impl InvocationCapture {
    pub(crate) async fn append_submit<C: ConnectionTrait>(
        &self,
        db: &C,
        observation: &SubmitObservation,
        now: DateTime<Utc>,
    ) -> AppResult<()> {
        append_observation_result(
            db,
            self,
            observation.result.as_ref(),
            &observation.resources,
            Some(observation.effect),
            now,
        )
        .await
    }
}

pub(crate) async fn append_nonprojecting_observation<C: ConnectionTrait>(
    db: &C,
    capture: &InvocationCapture,
    observation: &PinObservation,
    now: DateTime<Utc>,
) -> AppResult<()> {
    append_observation_result(
        db,
        capture,
        observation.result.as_ref(),
        &observation.resources,
        None,
        now,
    )
    .await
}

async fn append_observation_result<C: ConnectionTrait>(
    db: &C,
    capture: &InvocationCapture,
    result: Result<&RemotePin, &ProviderError>,
    observed_resources: &[ObservedResource],
    effect: Option<SubmitEffect>,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let mut resources = observed_resources.to_vec();
    let outcome = match result {
        Ok(remote) => {
            if resources.is_empty() {
                // Legacy GET adapters return a parsed report without typed
                // replica resources. Preserve that actual report as Reported,
                // not recursive verification or exclusive creation evidence.
                resources.push(crate::pinning::provider::ObservedResource {
                    resource_type: capture.route.resource_type(),
                    cid: remote.cid.clone(),
                    request_id: remote.request_id.clone(),
                    status: crate::pinning::provider::ObservedResourceStatus::Reported,
                    ownership: crate::pinning::identity::Ownership::Unknown,
                    replica_evidence: None,
                });
            }
            match remote.status {
                crate::pinning::provider::RemotePinStatus::Queued => "ReportedQueued",
                crate::pinning::provider::RemotePinStatus::Pinning => "ReportedPinning",
                crate::pinning::provider::RemotePinStatus::Pinned => "ReportedPinned",
                crate::pinning::provider::RemotePinStatus::Failed => "ReportedFailed",
            }
        }
        Err(error) if effect == Some(SubmitEffect::NotSubmitted) && resources.is_empty() => {
            if error.class == crate::pinning::provider::ProviderErrorClass::NotSubmitted {
                "NotCreated"
            } else {
                "Rejected"
            }
        }
        Err(_) => "Unknown",
    };
    append_fact(
        db,
        capture,
        &resources,
        outcome,
        result.err().map(|error| match effect {
            Some(effect) => crate::store::pinning::ledger::submission::typed_error(error, effect),
            None => error.safe_evidence(&capture.job.operation),
        }),
        effect,
        now,
    )
    .await
}

/// An actual successful/NotFound DELETE response, not exclusive ownership or
/// permission to release quota belonging to a newer epoch.
pub(crate) async fn append_nonprojecting_delete_acknowledgement<C: ConnectionTrait>(
    db: &C,
    capture: &InvocationCapture,
    now: DateTime<Utc>,
) -> AppResult<()> {
    append_fact(db, capture, &[], "DeleteAcknowledged", None, None, now).await
}

/// Borrow the actual DELETE error so the caller retains its full typed retry
/// input after committing this nonprojecting fact independently of settlement.
pub(crate) async fn append_nonprojecting_delete_error<C: ConnectionTrait>(
    db: &C,
    capture: &InvocationCapture,
    error: &ProviderError,
    now: DateTime<Utc>,
) -> AppResult<()> {
    append_observation_result(db, capture, Err(error), &[], None, now).await
}

async fn append_fact<C: ConnectionTrait>(
    db: &C,
    capture: &InvocationCapture,
    resources: &[crate::pinning::provider::ObservedResource],
    outcome: &str,
    safe_error: Option<String>,
    effect: Option<SubmitEffect>,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let job = &capture.job;
    let until = job
        .locked_until
        .ok_or_else(|| AppError::Internal("historical observation claim is missing".into()))?;
    let replica_resources = serde_json::to_string(resources)
        .map_err(|_| AppError::Internal("historical observation serialization failed".into()))?;
    // RPC submissions retain their existing in-flight receipt/record protocol.
    // Other managed Submit facts use the original dispatch's captured call, not
    // a history or claim resolved after the provider response.
    if let (Some(submit_call), Some(effect)) = (capture.submit_call, effect) {
        pin_submit_observation::Entity::insert(pin_submit_observation::ActiveModel {
            id: Set(format!("{}:call:{}", job.id, submit_call)),
            job_id: Set(job.id.clone()),
            submit_call: Set(submit_call),
            provider: Set(job.provider.clone()),
            expected_cid: Set(job.cid.clone()),
            route: Set(capture.invocation.route.clone()),
            remote_epoch: Set(capture.invocation.remote_epoch),
            claim_until: Set(until),
            effect: Set(match effect {
                SubmitEffect::NotSubmitted if matches!(outcome, "NotCreated" | "Rejected") => {
                    "not_created"
                }
                SubmitEffect::Observed => "observed",
                _ => "unknown",
            }
            .into()),
            resources: Set(serde_json::to_string(
                &resources
                    .iter()
                    .cloned()
                    .map(|resource| {
                        crate::store::pinning::ledger::submission::resource_evidence_for_route(
                            &capture.route,
                            resource,
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .map_err(|_| {
                AppError::Internal("historical submission serialization failed".into())
            })?),
            outcome: Set(outcome.into()),
            safe_error: Set(safe_error.clone()),
            needs_attention: Set(matches!(outcome, "Unknown" | "ReportedFailed")),
            started_at: Set(capture.started_at),
            observed_at: Set(Some(now)),
        })
        .exec(db)
        .await?;
    }
    // This table is an append-only scoped observation ledger. Keep the real
    // operation and nonprojectable report classification; do not pretend a
    // readonly reconcile is a target Poll or forge a live replica proof.
    pin_replica_observation::Entity::insert(pin_replica_observation::ActiveModel {
        sequence: Default::default(),
        job_id: Set(job.id.clone()),
        provider: Set(job.provider.clone()),
        expected_cid: Set(job.cid.clone()),
        route: Set(capture.invocation.route.clone()),
        remote_epoch: Set(capture.invocation.remote_epoch),
        claim_until: Set(until),
        lease_id: Set(job.lease_id.clone()),
        target_id: Set(job.target_id.clone()),
        expected_generation: Set(job.expected_generation),
        phase: Set(job.operation.clone()),
        resources: Set(replica_resources),
        outcome: Set(outcome.into()),
        safe_error: Set(safe_error),
        projectable: Set(false),
        observed_at: Set(now),
    })
    .exec(db)
    .await?;
    Ok(())
}
