//! Captured, safe-to-persist pin-control decisions. No raw tag or credential is stored here.
use serde::{Deserialize, Serialize};

use super::{
    policy::{LeaseIntent, LeaseSource, PublicationPolicy},
    tags::{ObjectTag, PinControl},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionEffect {
    Accepted,
    Skipped,
    NoIntent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarningCode {
    NoMatchingPolicy,
    NoAvailableProvider,
}

impl WarningCode {
    pub fn as_header_code(self) -> &'static str {
        match self {
            Self::NoMatchingPolicy => "pin-policy-unavailable",
            Self::NoAvailableProvider => "pin-provider-unavailable",
        }
    }
}

/// Both identifiers are opaque and assigned after authentication. For guarded
/// publications, `request_id` is the exact mutation ID, MPU upload ID, or import
/// job ID; the publication API rejects a capture from another admission.
/// Do not use object keys, tag values or provider response text here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionOrigin {
    pub principal_id: String,
    pub request_id: String,
}

impl DecisionOrigin {
    pub fn new(principal_id: impl Into<String>, request_id: impl Into<String>) -> Self {
        Self {
            principal_id: principal_id.into(),
            request_id: request_id.into(),
        }
    }

    fn valid(&self) -> bool {
        [&self.principal_id, &self.request_id]
            .into_iter()
            .all(|id| {
                !id.is_empty()
                    && id.len() <= 128
                    && id
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_:.".contains(&byte))
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "DecodedDecision")]
pub struct ExtensionDecision {
    pub effect: DecisionEffect,
    pub warning: Option<WarningCode>,
    /// A valid inherited legacy control without a source decision. Stored as
    /// no_intent for database compatibility, but never eligible for manual work.
    #[serde(default)]
    pub legacy_unknown: bool,
    pub origin: DecisionOrigin,
    /// An opaque UUID per captured control group (never a hash of raw tags).
    pub control_revision: String,
    /// Normalized reserved control (not the arbitrary user tag set).
    pub control: PinControl,
    /// Historical admission evidence; current policy resolves executable intents.
    pub effective_intents: Vec<LeaseIntent>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DecodedDecision {
    effect: DecisionEffect,
    warning: Option<WarningCode>,
    #[serde(default)]
    legacy_unknown: bool,
    origin: DecisionOrigin,
    control_revision: String,
    control: PinControl,
    effective_intents: Vec<LeaseIntent>,
    #[serde(default, rename = "config_revision")]
    _legacy_config_revision: Option<serde::de::IgnoredAny>,
}

impl From<DecodedDecision> for ExtensionDecision {
    fn from(value: DecodedDecision) -> Self {
        Self {
            effect: value.effect,
            warning: value.warning,
            legacy_unknown: value.legacy_unknown,
            origin: value.origin,
            control_revision: value.control_revision,
            control: value.control,
            effective_intents: value.effective_intents,
        }
    }
}

impl ExtensionDecision {
    pub(crate) fn capture(
        origin: DecisionOrigin,
        effect: DecisionEffect,
        warning: Option<WarningCode>,
        control: PinControl,
        policy: &PublicationPolicy,
    ) -> Result<Self, &'static str> {
        Self::capture_with_legacy_unknown(origin, effect, warning, false, control, policy)
    }

    pub(crate) fn capture_legacy_unknown(
        origin: DecisionOrigin,
        control: PinControl,
        policy: &PublicationPolicy,
    ) -> Result<Self, &'static str> {
        Self::capture_with_legacy_unknown(
            origin,
            DecisionEffect::NoIntent,
            None,
            true,
            control,
            policy,
        )
    }

    fn capture_with_legacy_unknown(
        origin: DecisionOrigin,
        effect: DecisionEffect,
        warning: Option<WarningCode>,
        legacy_unknown: bool,
        control: PinControl,
        policy: &PublicationPolicy,
    ) -> Result<Self, &'static str> {
        let decision = Self {
            effect,
            warning,
            legacy_unknown,
            origin,
            control_revision: uuid::Uuid::new_v4().to_string(),
            control,
            effective_intents: policy.leases.clone(),
        };
        decision.validate_policy(policy)?;
        Ok(decision)
    }

    pub fn validate_policy(&self, policy: &PublicationPolicy) -> Result<(), &'static str> {
        let control =
            PinControl::from_tags(&policy.tags).map_err(|_| "invalid captured control tags")?;
        self.validate_snapshot()?;
        if self.effective_intents != policy.leases || self.control != control {
            return Err("invalid or mismatched extension decision");
        }
        Ok(())
    }

    /// Validate a stored snapshot even if historical object tags were removed.
    pub fn validate_snapshot(&self) -> Result<(), &'static str> {
        let manual = self
            .effective_intents
            .iter()
            .filter(|intent| intent.source == LeaseSource::Manual)
            .count();
        if !self.origin.valid()
            || (self.legacy_unknown
                && (self.effect != DecisionEffect::NoIntent
                    || matches!(self.control, PinControl::Absent)))
            || uuid::Uuid::parse_str(&self.control_revision)
                .map_or(true, |uuid| uuid.to_string() != self.control_revision)
            || self.effective_intents.iter().any(|intent| {
                intent.duration.as_seconds() == 0 || intent.duration.as_seconds() > i64::MAX as u64
            })
            || match self.effect {
                DecisionEffect::Accepted => {
                    manual != 1
                        || self.warning.is_some()
                        || !matches!(self.control, PinControl::Request { .. })
                }
                DecisionEffect::Skipped => {
                    manual != 0
                        || self.warning.is_none()
                        || matches!(self.control, PinControl::Absent)
                }
                DecisionEffect::NoIntent => {
                    manual != 0
                        || self.warning.is_some()
                        || (matches!(self.control, PinControl::Request { .. })
                            && !self.legacy_unknown)
                }
            }
        {
            return Err("invalid or mismatched extension decision");
        }
        Ok(())
    }

    /// Read historical admission evidence and reject altered control tags.
    /// Execution must resolve the current policy separately.
    pub fn replay_policy(&self, tags: Vec<ObjectTag>) -> Result<PublicationPolicy, &'static str> {
        let policy = PublicationPolicy {
            tags,
            leases: self.effective_intents.clone(),
        };
        self.validate_policy(&policy)?;
        Ok(policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pinning::config::{LeaseDuration, ProviderMode};
    use crate::pinning::tags::ContentMode;

    #[test]
    fn legacy_unknown_snapshot_is_identifiable_and_cannot_become_manual() {
        let tags = vec![ObjectTag::new("ipfs-s3:pin", "true")];
        let policy = PublicationPolicy {
            tags: tags.clone(),
            leases: Vec::new(),
        };
        let control = PinControl::Request {
            duration: None,
            content: ContentMode::Object,
        };
        let decision = ExtensionDecision::capture_legacy_unknown(
            DecisionOrigin::new("owner", "request"),
            control.clone(),
            &policy,
        )
        .unwrap();
        let restored: ExtensionDecision =
            serde_json::from_str(&serde_json::to_string(&decision).unwrap()).unwrap();
        assert_eq!(restored.effect, DecisionEffect::NoIntent);
        assert!(restored.legacy_unknown);
        assert_eq!(restored.warning, None);
        restored.replay_policy(tags.clone()).unwrap();
        assert!(
            restored
                .replay_policy(vec![ObjectTag::new("ipfs-s3:pin", "false")])
                .is_err()
        );

        let mut forged = restored.clone();
        forged.effect = DecisionEffect::Accepted;
        assert!(forged.validate_snapshot().is_err());
        forged.effect = DecisionEffect::NoIntent;
        forged.warning = Some(WarningCode::NoAvailableProvider);
        assert!(forged.validate_snapshot().is_err());
        forged.warning = None;
        forged.legacy_unknown = false;
        assert!(forged.validate_snapshot().is_err());
        let mut forged = restored;
        forged.effective_intents.push(LeaseIntent {
            source: LeaseSource::Manual,
            policy_id: "policy".into(),
            provider_mode: ProviderMode::One,
            providers: vec!["provider".into()],
            content_mode: ContentMode::Object,
            duration: LeaseDuration::parse("1h").unwrap(),
        });
        assert!(forged.validate_snapshot().is_err());
        assert!(
            ExtensionDecision::capture_legacy_unknown(
                DecisionOrigin::new("owner", "request"),
                control,
                &PublicationPolicy {
                    tags: Vec::new(),
                    leases: Vec::new()
                },
            )
            .is_err()
        );
    }

    #[test]
    fn existing_snapshots_without_legacy_marker_still_deserialize() {
        let policy = PublicationPolicy {
            tags: Vec::new(),
            leases: Vec::new(),
        };
        let decision = ExtensionDecision::capture(
            DecisionOrigin::new("owner", "request"),
            DecisionEffect::NoIntent,
            None,
            PinControl::Absent,
            &policy,
        )
        .unwrap();
        let mut serialized = serde_json::to_value(&decision).unwrap();
        serialized.as_object_mut().unwrap().remove("legacy_unknown");
        serialized["config_revision"] = serde_json::json!("old-config-digest");
        let restored: ExtensionDecision = serde_json::from_value(serialized).unwrap();
        let current = serde_json::to_value(&restored).unwrap();
        assert!(current.get("config_revision").is_none());
        let mut unknown = current;
        unknown["unexpected_authority"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ExtensionDecision>(unknown).is_err());
        assert!(!restored.legacy_unknown);
        restored.validate_policy(&policy).unwrap();
    }
}
