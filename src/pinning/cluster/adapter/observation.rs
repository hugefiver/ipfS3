use crate::pinning::cluster::{
    ReplicaEvidence, ReplicaOutcome,
    provider::{
        ClusterFailureCode, ClusterResourceStatus, ClusterWriteEffect, ClusterWriteFailure,
        ClusterWriteObservation,
    },
};
use crate::pinning::{
    identity::{Ownership, RemoteResourceType},
    provider::{
        ObservedResource, ObservedResourceStatus, ProviderError, ProviderErrorClass, RemotePin,
        RemotePinStatus, SubmitEffect, SubmitObservation,
    },
};

pub(super) fn write_error(error: ClusterWriteFailure) -> ProviderError {
    let class = match error.http_status {
        Some(401) => ProviderErrorClass::Authentication,
        Some(403) => ProviderErrorClass::UnknownForbidden,
        Some(429) => ProviderErrorClass::RateLimited,
        _ => match error.code {
            ClusterFailureCode::InvalidConfiguration | ClusterFailureCode::InvalidCid => {
                ProviderErrorClass::InvalidInput
            }
            ClusterFailureCode::Transport
            | ClusterFailureCode::IdleTimeout
            | ClusterFailureCode::ControlTimeout
            | ClusterFailureCode::Cancelled => ProviderErrorClass::Transient,
            _ => ProviderErrorClass::Protocol,
        },
    };
    ProviderError {
        class,
        message: format!("Cluster request failed: {:?}", error.code),
        retry_after: None,
    }
}

pub(super) fn pin(cid: &str, outcome: ReplicaOutcome) -> RemotePin {
    let complete = outcome == ReplicaOutcome::Complete;
    RemotePin {
        request_id: cid.into(),
        cid: cid.into(),
        status: if complete {
            RemotePinStatus::Pinned
        } else {
            RemotePinStatus::Pinning
        },
        raw_status: if complete {
            "replicas_verified"
        } else {
            "replicas_unconfirmed"
        }
        .into(),
        failure_reason: None,
    }
}

pub(super) fn resource(
    cid: &str,
    outcome: ReplicaOutcome,
    evidence: Option<ReplicaEvidence>,
    accepted: bool,
) -> ObservedResource {
    ObservedResource {
        resource_type: RemoteResourceType::ClusterPin,
        cid: cid.into(),
        request_id: cid.into(),
        status: if outcome == ReplicaOutcome::Complete {
            ObservedResourceStatus::ReplicasVerified
        } else if accepted || outcome == ReplicaOutcome::Insufficient {
            ObservedResourceStatus::PinAccepted
        } else {
            ObservedResourceStatus::Reported
        },
        ownership: Ownership::Unknown,
        replica_evidence: evidence,
    }
}

pub(super) fn submit(write: ClusterWriteObservation) -> SubmitObservation {
    let receipt = write.result.as_ref().ok().map(|r| &r.resource);
    let evidence = write
        .replicas
        .as_ref()
        .and_then(|p| p.evidence())
        .filter(|e| receipt == Some(&e.resource))
        .cloned();
    let mut resources: Vec<_> = write
        .resources
        .iter()
        .map(|r| {
            if receipt == Some(&r.resource) {
                resource(
                    &r.resource.cid,
                    ReplicaOutcome::Unknown,
                    evidence.clone(),
                    true,
                )
            } else {
                resource(
                    &r.resource.cid,
                    ReplicaOutcome::Unknown,
                    None,
                    r.status == ClusterResourceStatus::PinAccepted,
                )
            }
        })
        .collect();
    // Clone bounded historical facts first. Only now consume live outcome at
    // the integration gate; serialized counts are never a confirmation source.
    let outcome = write.replication_outcome();
    if outcome == ReplicaOutcome::Complete
        && let Some(index) = write
            .resources
            .iter()
            .position(|r| receipt == Some(&r.resource))
    {
        resources[index].status = ObservedResourceStatus::ReplicasVerified;
    }
    SubmitObservation {
        result: write
            .result
            .map(|r| pin(&r.resource.cid, outcome))
            .map_err(write_error),
        resources,
        effect: match write.effect {
            ClusterWriteEffect::NotSubmitted => SubmitEffect::NotSubmitted,
            ClusterWriteEffect::Observed => SubmitEffect::Observed,
            ClusterWriteEffect::Unknown => SubmitEffect::Unknown,
        },
    }
}
