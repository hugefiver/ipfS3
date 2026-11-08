//! Narrow historical DTO fields. Archive integrity is checked on original bytes
//! by the storage/wire reader before these runtime projections are exposed.
use crate::pinning::management::model::*;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectSnapshot {
    identity: ExactObject,
    logical_bytes: u64,
    readable: Option<bool>,
    read_evidence: Option<ReadEvidence>,
    permitted: Option<bool>,
    allowed_routes: Vec<RouteIdentity>,
    control_revision: Option<String>,
    #[serde(default, rename = "config_revision")]
    _config_revision: Option<String>,
    decision: Option<DecisionState>,
    evidence: SelectionEvidence,
}
impl From<ObjectSnapshot> for crate::pinning::management::model::ObjectSnapshot {
    fn from(value: ObjectSnapshot) -> Self {
        Self {
            identity: value.identity,
            logical_bytes: value.logical_bytes,
            readable: value.readable,
            read_evidence: value.read_evidence,
            permitted: value.permitted,
            allowed_routes: value.allowed_routes,
            control_revision: value.control_revision,
            decision: value.decision,
            evidence: value.evidence,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Requirements {
    mode: ProviderMode,
    provider_ids: Vec<String>,
    levels: Vec<ConfirmationLevel>,
    min_replicas: u32,
    max_age_secs: Option<u32>,
    policy_id: Option<String>,
    #[serde(default, rename = "policy_revision")]
    _policy_revision: Option<String>,
}
impl From<Requirements> for crate::pinning::management::model::Requirements {
    fn from(value: Requirements) -> Self {
        Self {
            mode: value.mode,
            provider_ids: value.provider_ids,
            levels: value.levels,
            min_replicas: value.min_replicas,
            max_age_secs: value.max_age_secs,
            policy_id: value.policy_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pinning::management::{Requirements, policy_reference};

    #[test]
    fn historical_policy_digest_is_ignored_without_relaxing_unknown_fields() {
        let mut original = serde_json::json!({
            "mode":"all", "provider_ids":["provider"], "levels":["remote_pinned"],
            "min_replicas":1, "max_age_secs":null, "policy_revision":"a".repeat(64),
        });
        let projected: Requirements = serde_json::from_value(original.clone()).unwrap();
        assert_eq!(projected.policy_id, None);
        assert!(
            serde_json::to_value(projected)
                .unwrap()
                .get("policy_revision")
                .is_none()
        );
        original["unrecognized_authority"] = true.into();
        assert!(serde_json::from_value::<Requirements>(original).is_err());
        assert_eq!(
            policy_reference(&format!("policy:0:{}", "a".repeat(64))),
            None
        );
        assert_eq!(policy_reference("policy:0"), Some("policy:0".into()));
    }
}
