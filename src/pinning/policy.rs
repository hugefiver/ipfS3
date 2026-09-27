use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    config::OptionalPinControlMode,
    error::AppError,
    pinning::{
        config::{
            LeaseDuration, PolicyTrigger, ProviderMode, ValidatedPinningConfig, ValidatedPolicy,
        },
        decision::{
            DecisionEffect, DecisionOrigin, ExtensionDecision, WarningCode, config_revision,
        },
        tags::{ContentMode, ObjectTag, PinControl},
    },
};

#[derive(Debug, Clone)]
pub struct PublicationContext<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub tags: &'a [ObjectTag],
    pub is_decompress_zip: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseSource {
    Automatic,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseIntent {
    pub source: LeaseSource,
    pub policy_id: String,
    pub provider_mode: ProviderMode,
    pub providers: Vec<String>,
    pub content_mode: ContentMode,
    pub duration: LeaseDuration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationPolicy {
    pub tags: Vec<ObjectTag>,
    pub leases: Vec<LeaseIntent>,
}

#[derive(Debug, Clone)]
pub struct PinPolicyEvaluator {
    policies: Vec<ValidatedPolicy>,
    provider_limits: crate::pinning::config::ProviderLimitMap,
    config_revision: String,
    optional_control: OptionalPinControlMode,
}

impl PinPolicyEvaluator {
    pub fn new(config: &ValidatedPinningConfig) -> Self {
        Self::with_mode(config, OptionalPinControlMode::Strict)
    }

    pub fn with_mode(config: &ValidatedPinningConfig, mode: OptionalPinControlMode) -> Self {
        Self {
            policies: config.policies.clone(),
            provider_limits: config.provider_limits.clone(),
            config_revision: config_revision(config, mode),
            optional_control: mode,
        }
    }

    /// Capture once after authorization/admission (PUT, MPU init, import receive).
    /// Serialize the decision with the durable MPU/import record; never re-interpret
    /// old raw tags at completion. Guarded publication checks the origin ID.
    pub fn evaluate_publication_decision(
        &self,
        context: PublicationContext<'_>,
        origin: DecisionOrigin,
    ) -> Result<(PublicationPolicy, ExtensionDecision), PolicyError> {
        let control = pin_control(context.tags)?;
        // A decompressed request outside ZIP is invalid even without a matching policy.
        if matches!(
            control,
            PinControl::Request {
                content: ContentMode::Decompressed,
                ..
            }
        ) && !context.is_decompress_zip
        {
            return Err(invalid_request(
                "decompressed pinning requires a decompress-zip upload",
            ));
        }
        let selected = self.matching_policy(context.bucket, context.key);
        let mut policy = match self.evaluate_publication(context.clone()) {
            Ok(policy) => policy,
            Err(_error)
                if self.optional_control == OptionalPinControlMode::Warn
                    && selected.is_none()
                    && matches!(control, PinControl::Request { .. } | PinControl::Cancel) =>
            {
                // Only the absence of a policy is optional; parsing and structure already passed.
                PublicationPolicy {
                    tags: context.tags.to_vec(),
                    leases: Vec::new(),
                }
            }
            Err(error) => return Err(error),
        };
        let warning = if selected.is_none() && !matches!(control, PinControl::Absent) {
            Some(WarningCode::NoMatchingPolicy)
        } else if let Some(selected) = selected {
            let enabled = selected
                .providers
                .iter()
                .filter(|name| {
                    self.provider_limits
                        .get(*name)
                        .is_some_and(|limits| limits.enabled)
                })
                .count();
            if !matches!(control, PinControl::Request { .. })
                || (enabled > 0
                    && (selected.provider_mode != ProviderMode::All
                        || enabled == selected.providers.len()))
            {
                None
            } else if self.optional_control == OptionalPinControlMode::Warn {
                policy
                    .leases
                    .retain(|intent| intent.source != LeaseSource::Manual);
                Some(WarningCode::NoAvailableProvider)
            } else {
                return Err(invalid_request("manual pinning has no available provider"));
            }
        } else {
            None
        };
        let effect = if warning.is_some() {
            DecisionEffect::Skipped
        } else if policy
            .leases
            .iter()
            .any(|intent| intent.source == LeaseSource::Manual)
        {
            DecisionEffect::Accepted
        } else {
            DecisionEffect::NoIntent
        };
        let decision = ExtensionDecision::capture(
            origin,
            self.config_revision.clone(),
            effect,
            warning,
            control,
            &policy,
        )
        .map_err(invalid_request)?;
        Ok((policy, decision))
    }

    pub fn evaluate_publication(
        &self,
        context: PublicationContext<'_>,
    ) -> Result<PublicationPolicy, PolicyError> {
        let control = pin_control(context.tags)?;
        let Some(policy) = self.matching_policy(context.bucket, context.key) else {
            if matches!(control, PinControl::Absent) {
                return Ok(PublicationPolicy {
                    tags: context.tags.to_vec(),
                    leases: Vec::new(),
                });
            }
            return Err(invalid_request("no pinning policy matches this object"));
        };

        let mut leases = Vec::new();
        if policy.trigger == PolicyTrigger::Always {
            leases.push(lease_intent(
                policy,
                LeaseSource::Automatic,
                ContentMode::Object,
                policy.default_duration,
            ));
        }

        match control {
            PinControl::Absent | PinControl::Cancel => {}
            PinControl::Request { duration, content } => {
                let duration = duration.unwrap_or(policy.default_duration);
                if duration > policy.max_duration {
                    return Err(invalid_request("requested duration exceeds policy maximum"));
                }
                validate_manual_content(content, context.is_decompress_zip, policy)?;
                leases.push(lease_intent(policy, LeaseSource::Manual, content, duration));
            }
            PinControl::Renew { .. } => {
                return Err(invalid_request(
                    "ipfs-s3:retain-until is invalid for an initial pin request",
                ));
            }
        }

        Ok(PublicationPolicy {
            tags: context.tags.to_vec(),
            leases,
        })
    }

    pub fn evaluate_tag_replacement(
        &self,
        existing_manual: Option<&ExistingManualLease>,
        replacement: &[ObjectTag],
        now: DateTime<Utc>,
    ) -> Result<ManualLeaseMutation, PolicyError> {
        let control = pin_control(replacement)?;
        let Some(existing) = existing_manual else {
            return match control {
                PinControl::Absent | PinControl::Cancel => Ok(ManualLeaseMutation::Keep),
                PinControl::Request { .. } | PinControl::Renew { .. } => {
                    Err(invalid_request("manual pin lease does not exist"))
                }
            };
        };

        match control {
            PinControl::Absent | PinControl::Cancel => {
                if existing.state == ExistingManualLeaseState::Active {
                    Ok(ManualLeaseMutation::Cancel)
                } else {
                    Ok(ManualLeaseMutation::Keep)
                }
            }
            PinControl::Request { .. } => Err(invalid_request(
                "ipfs-s3:retain-until is required to renew a manual pin lease",
            )),
            PinControl::Renew { retain_until } => {
                self.evaluate_renewal(existing, retain_until, now)
            }
        }
    }

    fn matching_policy(&self, bucket: &str, key: &str) -> Option<&ValidatedPolicy> {
        self.policies.iter().find(|policy| {
            (policy.bucket == "*" || policy.bucket == bucket) && key.starts_with(&policy.prefix)
        })
    }

    fn evaluate_renewal(
        &self,
        existing: &ExistingManualLease,
        retain_until: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<ManualLeaseMutation, PolicyError> {
        let policy = self
            .policies
            .iter()
            .find(|policy| policy.identity == existing.policy_id)
            .ok_or_else(|| invalid_request("captured pinning policy is unknown"))?;
        let maximum_retain_until = maximum_retain_until(now, policy.max_duration)?;
        if retain_until > maximum_retain_until {
            return Err(invalid_request(
                "retention timestamp exceeds policy maximum duration",
            ));
        }

        match existing.state {
            ExistingManualLeaseState::Active => {
                if retain_until == existing.expires_at {
                    Ok(ManualLeaseMutation::Keep)
                } else if retain_until < existing.expires_at {
                    Err(invalid_request(
                        "retention timestamp cannot shorten a manual pin lease",
                    ))
                } else {
                    Ok(ManualLeaseMutation::Renew { retain_until })
                }
            }
            ExistingManualLeaseState::Expired => {
                if retain_until <= existing.expires_at || retain_until <= now {
                    return Err(invalid_request(
                        "expired manual pin lease renewal must extend beyond its expiry and now",
                    ));
                }
                Ok(ManualLeaseMutation::Renew { retain_until })
            }
            ExistingManualLeaseState::Cancelled | ExistingManualLeaseState::Evicted => Err(
                invalid_request("cancelled or evicted manual pin lease cannot be renewed"),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualLeaseMutation {
    Keep,
    Renew { retain_until: DateTime<Utc> },
    Cancel,
}

#[derive(Debug, Clone)]
pub struct ExistingManualLease {
    pub id: String,
    pub policy_id: String,
    pub content_mode: ContentMode,
    pub expires_at: DateTime<Utc>,
    pub generation: i64,
    pub state: ExistingManualLeaseState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistingManualLeaseState {
    Active,
    Expired,
    Cancelled,
    Evicted,
}

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("invalid pinning request: {0}")]
    InvalidRequest(String),
}

impl From<PolicyError> for AppError {
    fn from(error: PolicyError) -> Self {
        match error {
            PolicyError::InvalidRequest(message) => AppError::InvalidPinningRequest(message),
        }
    }
}

fn pin_control(tags: &[ObjectTag]) -> Result<PinControl, PolicyError> {
    PinControl::from_tags(tags).map_err(|error| invalid_request(error.to_string()))
}

fn lease_intent(
    policy: &ValidatedPolicy,
    source: LeaseSource,
    content_mode: ContentMode,
    duration: LeaseDuration,
) -> LeaseIntent {
    LeaseIntent {
        source,
        policy_id: policy.identity.clone(),
        provider_mode: policy.provider_mode,
        providers: policy.providers.clone(),
        content_mode,
        duration,
    }
}

fn validate_manual_content(
    content: ContentMode,
    is_decompress_zip: bool,
    policy: &ValidatedPolicy,
) -> Result<(), PolicyError> {
    if content == ContentMode::Decompressed && !policy.allow_decompressed {
        return Err(invalid_request(
            "decompressed pinning is not allowed by the matching policy",
        ));
    }
    if content == ContentMode::Decompressed && !is_decompress_zip {
        return Err(invalid_request(
            "decompressed pinning requires a decompress-zip upload",
        ));
    }
    Ok(())
}

fn maximum_retain_until(
    now: DateTime<Utc>,
    max_duration: LeaseDuration,
) -> Result<DateTime<Utc>, PolicyError> {
    let seconds = i64::try_from(max_duration.as_seconds())
        .map_err(|_| invalid_request("policy maximum duration is out of range"))?;
    let duration = TimeDelta::try_seconds(seconds)
        .ok_or_else(|| invalid_request("policy maximum duration is out of range"))?;
    now.checked_add_signed(duration)
        .ok_or_else(|| invalid_request("policy maximum retention timestamp is out of range"))
}

fn invalid_request(message: impl Into<String>) -> PolicyError {
    PolicyError::InvalidRequest(message.into())
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};

    use super::{
        ExistingManualLease, ExistingManualLeaseState, LeaseSource, ManualLeaseMutation,
        PinPolicyEvaluator, PolicyError, PublicationContext,
    };
    use crate::{
        config::{PinningConfig, PolicyConfig, ProviderConfig},
        error::AppError,
        pinning::{
            config::{LeaseDuration, ProviderMode, ValidatedPinningConfig},
            tags::{ContentMode, ObjectTag},
        },
    };

    const NOW_YEAR: i32 = 2026;

    fn tags(pairs: &[(&str, &str)]) -> Vec<ObjectTag> {
        pairs
            .iter()
            .map(|(key, value)| ObjectTag::new(*key, *value))
            .collect()
    }

    fn provider(name: &str, priority: u32) -> ProviderConfig {
        ProviderConfig {
            name: name.to_owned(),
            kind: "noop".to_owned(),
            token_env: None,
            endpoint: None,
            api: None,
            strategy: None,
            upload_endpoint: None,
            enabled: true,
            priority,
            max_bytes: 100,
            max_pins: 10,
            requests_per_second: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn rule(
        bucket: &str,
        prefix: &str,
        trigger: &str,
        provider_mode: &str,
        providers: &[&str],
        default_duration: &str,
        max_duration: &str,
        allow_decompressed: bool,
    ) -> PolicyConfig {
        PolicyConfig {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            trigger: trigger.to_owned(),
            provider_mode: provider_mode.to_owned(),
            providers: providers
                .iter()
                .map(|provider| (*provider).to_owned())
                .collect(),
            default_duration: default_duration.to_owned(),
            max_duration: max_duration.to_owned(),
            allow_decompressed,
        }
    }

    fn evaluator_with_rules(policies: Vec<PolicyConfig>) -> PinPolicyEvaluator {
        let config = PinningConfig {
            worker_interval: "5s".to_owned(),
            worker_concurrency: 4,
            providers: vec![
                provider("alpha", 2),
                provider("bravo", 1),
                provider("charlie", 3),
            ],
            policies,
        };
        let validated = ValidatedPinningConfig::from_raw(&config, |_| None).unwrap();
        PinPolicyEvaluator::new(&validated)
    }

    fn request_policy() -> PinPolicyEvaluator {
        evaluator_with_rules(vec![rule(
            "bucket",
            "",
            "request",
            "one",
            &["alpha"],
            "1d",
            "30d",
            false,
        )])
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(NOW_YEAR, 7, 21, 12, 0, 0)
            .single()
            .unwrap()
    }

    fn at(day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(NOW_YEAR, 7, day, 12, 0, 0)
            .single()
            .unwrap()
    }

    fn august(day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(NOW_YEAR, 8, day, 12, 0, 0)
            .single()
            .unwrap()
    }

    fn publication<'a>(
        evaluator: &PinPolicyEvaluator,
        bucket: &'a str,
        key: &'a str,
        tags: &'a [ObjectTag],
        is_decompress_zip: bool,
    ) -> super::PublicationPolicy {
        evaluator
            .evaluate_publication(PublicationContext {
                bucket,
                key,
                tags,
                is_decompress_zip,
            })
            .unwrap()
    }

    fn policy_id(evaluator: &PinPolicyEvaluator) -> String {
        let request = tags(&[("ipfs-s3:pin", "true")]);
        publication(evaluator, "bucket", "object", &request, false).leases[0]
            .policy_id
            .clone()
    }

    fn existing_manual(
        policy_id: String,
        state: ExistingManualLeaseState,
        expires_at: DateTime<Utc>,
    ) -> ExistingManualLease {
        ExistingManualLease {
            id: "manual-lease".to_owned(),
            policy_id,
            content_mode: ContentMode::Object,
            expires_at,
            generation: 1,
            state,
        }
    }

    fn renew_tags(retain_until: DateTime<Utc>) -> Vec<ObjectTag> {
        tags(&[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", &retain_until.to_rfc3339()),
        ])
    }

    #[test]
    fn first_wildcard_literal_prefix_rule_wins_over_a_later_exact_rule() {
        let evaluator = evaluator_with_rules(vec![
            rule(
                "*",
                "images/",
                "request",
                "one",
                &["alpha"],
                "1d",
                "30d",
                false,
            ),
            rule(
                "photos",
                "images/raw/",
                "always",
                "all",
                &["alpha", "bravo"],
                "7d",
                "90d",
                true,
            ),
        ]);
        let no_tags = Vec::new();

        let plan = publication(&evaluator, "photos", "images/raw/a.nef", &no_tags, false);

        assert!(plan.leases.is_empty());
    }

    #[test]
    fn no_match_preserves_ordinary_tags_but_rejects_reserved_controls() {
        let evaluator = request_policy();
        let ordinary = tags(&[("team", "infra")]);
        let requested = tags(&[("ipfs-s3:pin", "true")]);

        let plan = publication(&evaluator, "other", "object", &ordinary, false);

        assert_eq!(plan.tags, ordinary);
        assert!(plan.leases.is_empty());
        assert!(
            evaluator
                .evaluate_publication(PublicationContext {
                    bucket: "other",
                    key: "object",
                    tags: &requested,
                    is_decompress_zip: false,
                })
                .is_err()
        );
    }

    #[test]
    fn warn_skips_unavailable_manual_group_but_never_invalid_controls() {
        use crate::config::OptionalPinControlMode;
        use crate::pinning::decision::{DecisionEffect, DecisionOrigin, WarningCode};
        let validated =
            ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
        let evaluator = PinPolicyEvaluator::with_mode(&validated, OptionalPinControlMode::Warn);
        let raw = tags(&[("ipfs-s3:pin", "true"), ("private", "sensitive")]);
        let (policy, decision) = evaluator
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "secret",
                    tags: &raw,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("principal-1", "request-1"),
            )
            .unwrap();
        assert_eq!(policy.tags, raw);
        assert!(policy.leases.is_empty());
        assert_eq!(decision.effect, DecisionEffect::Skipped);
        assert_eq!(decision.warning, Some(WarningCode::NoMatchingPolicy));
        let encoded = serde_json::to_string(&decision).unwrap();
        assert!(!encoded.contains("sensitive"));
        assert!(!encoded.contains("secret"));
        for invalid in [
            tags(&[("ipfs-s3:pin", "TRUE")]),
            tags(&[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "0d")]),
        ] {
            assert!(
                evaluator
                    .evaluate_publication_decision(
                        PublicationContext {
                            bucket: "bucket",
                            key: "key",
                            tags: &invalid,
                            is_decompress_zip: false
                        },
                        DecisionOrigin::new("principal-1", "request-2"),
                    )
                    .is_err()
            );
        }
    }

    #[test]
    fn unavailable_manual_does_not_consume_automatic_intent_or_downgrade_all() {
        use crate::{
            config::OptionalPinControlMode,
            pinning::decision::{DecisionEffect, DecisionOrigin, WarningCode},
        };
        let mut raw = PinningConfig {
            providers: vec![
                provider("alpha", 1),
                ProviderConfig {
                    enabled: false,
                    ..provider("bravo", 2)
                },
            ],
            policies: vec![rule(
                "bucket",
                "",
                "always",
                "one",
                &["alpha", "bravo"],
                "1d",
                "30d",
                false,
            )],
            ..PinningConfig::default()
        };
        let validated = ValidatedPinningConfig::from_raw(&raw, |_| None).unwrap();
        let evaluator = PinPolicyEvaluator::with_mode(&validated, OptionalPinControlMode::Warn);
        let requested = tags(&[("ipfs-s3:pin", "true")]);
        let (policy, decision) = evaluator
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "object",
                    tags: &requested,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("principal", "request"),
            )
            .unwrap();
        assert_eq!(decision.effect, DecisionEffect::Accepted);
        assert_eq!(policy.leases.len(), 2);

        // `all` may not silently shrink to the enabled provider. Automatic intent
        // remains independent even when the manual group is skipped.
        raw.policies[0].provider_mode = "all".into();
        let validated = ValidatedPinningConfig::from_raw(&raw, |_| None).unwrap();
        let evaluator = PinPolicyEvaluator::with_mode(&validated, OptionalPinControlMode::Warn);
        let (policy, decision) = evaluator
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "object",
                    tags: &requested,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("principal", "request-2"),
            )
            .unwrap();
        assert_eq!(decision.effect, DecisionEffect::Skipped);
        assert_eq!(decision.warning, Some(WarningCode::NoAvailableProvider));
        assert_eq!(policy.leases.len(), 1);
        assert_eq!(policy.leases[0].source, LeaseSource::Automatic);
        assert_eq!(policy.leases[0].providers, vec!["alpha", "bravo"]);
    }

    #[test]
    fn warn_with_only_disabled_provider_is_explicit_but_strict_stays_invalid() {
        use crate::{
            config::{Config, OptionalPinControlMode},
            pinning::decision::{DecisionEffect, DecisionOrigin, WarningCode},
        };
        let mut raw: Config = toml::from_str("[pinning_control]\nunavailable = 'warn'").unwrap();
        raw.pinning.providers = vec![provider("disabled", 1)];
        raw.pinning.providers[0].enabled = false;
        raw.pinning.policies = vec![rule(
            "bucket",
            "",
            "request",
            "all",
            &["disabled"],
            "1d",
            "30d",
            false,
        )];
        assert!(ValidatedPinningConfig::from_raw(&raw.pinning, |_| None).is_err());
        let validated = ValidatedPinningConfig::from_config(&raw, |_| None).unwrap();
        let eval = PinPolicyEvaluator::with_mode(&validated, raw.pinning_control.unavailable);
        let requested = tags(&[("ipfs-s3:pin", "true")]);
        let (policy, decision) = eval
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "object",
                    tags: &requested,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("principal", "request"),
            )
            .unwrap();
        assert_eq!(decision.effect, DecisionEffect::Skipped);
        assert_eq!(decision.warning, Some(WarningCode::NoAvailableProvider));
        assert!(policy.leases.is_empty());
        // Captured skipped control stays skipped even if the deployment switches
        // back to strict; no new pin work is created during completion.
        assert!(
            decision
                .verify_revision(&validated, OptionalPinControlMode::Strict)
                .is_ok()
        );
        raw.pinning.providers[0].enabled = true;
        let changed = ValidatedPinningConfig::from_config(&raw, |_| None).unwrap();
        assert!(
            decision
                .verify_revision(&changed, OptionalPinControlMode::Warn)
                .is_ok()
        );
        assert_eq!(decision.effect, DecisionEffect::Skipped); // Never re-evaluate old tags.

        let invalid = tags(&[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "31d")]);
        assert!(
            eval.evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "object",
                    tags: &invalid,
                    is_decompress_zip: false
                },
                DecisionOrigin::new("principal", "request-2"),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn omitted_control_legacy_route_decides_and_publishes_without_managed_cleanup() {
        use crate::{
            config::{Config, OptionalPinControlMode},
            pinning::decision::{DecisionEffect, DecisionOrigin},
            store::{
                self,
                pinning::{
                    ledger,
                    publication::{
                        DecidedPublish, PinTargetSpec, PublicationObject, PublicationRequest,
                        publish_decided_object,
                    },
                },
            },
        };
        use sea_orm::{Database, EntityTrait, PaginatorTrait};

        let config: Config = toml::from_str(
            r#"
            [pinning]
            [[pinning.providers]]
            name = "remote"
            kind = "pinata"
            token_env = "LEGACY_PIN_TOKEN"
            priority = 1
            max_bytes = 1000
            max_pins = 100
            [[pinning.policies]]
            bucket = "bucket"
            trigger = "always"
            provider_mode = "one"
            providers = ["remote"]
            default_duration = "1h"
            max_duration = "24h"
            "#,
        )
        .unwrap();
        assert_eq!(
            config.pinning_control.unavailable,
            OptionalPinControlMode::Strict
        );
        let validated =
            ValidatedPinningConfig::from_config(&config, |_| Some("token".into())).unwrap();
        let db = Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        store::bucket::create(&db, "bucket", None).await.unwrap();
        ledger::register_route(&db, "remote", &validated.providers[0].identity)
            .await
            .unwrap();

        let tags = tags(&[("ipfs-s3:pin", "true")]);
        let (policy, decision) = PinPolicyEvaluator::new(&validated)
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "object",
                    tags: &tags,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("principal", "request"),
            )
            .unwrap();
        assert_eq!(decision.effect, DecisionEffect::Accepted);
        assert_eq!(
            policy
                .leases
                .iter()
                .map(|lease| lease.source)
                .collect::<Vec<_>>(),
            vec![LeaseSource::Automatic, LeaseSource::Manual]
        );
        let object = PublicationObject::from_put(
            "object-id".into(),
            "bucket",
            "object",
            "bafy-legacy".into(),
            7,
            None,
            None,
            false,
            None,
            None,
            Utc::now(),
        );
        publish_decided_object(
            &db,
            PublicationRequest {
                object,
                tags,
                policy,
                object_target: PinTargetSpec {
                    cid: "bafy-legacy".into(),
                    logical_size: 7,
                },
            },
            DecidedPublish {
                decision: &decision,
                config: &validated,
                mode: OptionalPinControlMode::Strict,
                limits: &validated.provider_limits,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            store::entities::pin_lease::Entity::find()
                .count(&db)
                .await
                .unwrap(),
            2
        );
        let remote = ledger::get(&db, "remote", "bafy-legacy")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.ownership, "unknown");
        assert!(
            !ledger::cleanup_allowed(&db, "remote", "bafy-legacy")
                .await
                .unwrap()
        );
    }

    #[test]
    fn legacy_custom_route_capture_preserves_local_intent_but_never_cross_replays() {
        use crate::{
            config::OptionalPinControlMode,
            pinning::decision::{DecisionEffect, DecisionOrigin},
        };
        let mut remote = provider("remote", 1);
        remote.kind = "pinata".into();
        remote.token_env = Some("PINNING_TOKEN".into());
        remote.endpoint = Some("https://remote.example.test/secret-in-path".into());
        let raw = PinningConfig {
            providers: vec![remote],
            policies: vec![rule(
                "bucket",
                "",
                "request",
                "one",
                &["remote"],
                "1d",
                "30d",
                false,
            )],
            ..PinningConfig::default()
        };
        let secret = "test-secret-not-in-snapshot";
        let validated =
            ValidatedPinningConfig::from_raw(&raw, |_| Some(secret.to_owned())).unwrap();
        let evaluator = PinPolicyEvaluator::with_mode(&validated, OptionalPinControlMode::Warn);
        let requested = tags(&[("ipfs-s3:pin", "true")]);
        let (policy, accepted) = evaluator
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "object",
                    tags: &requested,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("principal", "request"),
            )
            .unwrap();
        assert_eq!(accepted.effect, DecisionEffect::Accepted);
        assert_eq!(policy.leases.len(), 1);
        assert!(
            accepted
                .verify_revision(&validated, OptionalPinControlMode::Warn)
                .is_ok()
        );
        assert!(
            accepted
                .verify_revision(&validated.clone(), OptionalPinControlMode::Warn)
                .is_ok()
        );
        // A process-local keyed route binding proves an unchanged reload while
        // rejecting a different endpoint without exposing the URL or token.
        let reloaded = ValidatedPinningConfig::from_raw(&raw, |_| Some(secret.to_owned())).unwrap();
        assert!(
            accepted
                .verify_revision(&reloaded, OptionalPinControlMode::Warn)
                .is_ok()
        );
        let mut changed = raw.clone();
        changed.providers[0].endpoint = Some("https://other.example.test/secret-in-path".into());
        let changed =
            ValidatedPinningConfig::from_raw(&changed, |_| Some(secret.to_owned())).unwrap();
        assert!(
            accepted
                .verify_revision(&changed, OptionalPinControlMode::Warn)
                .is_err()
        );
        let encoded = serde_json::to_string(&accepted).unwrap();
        assert!(!encoded.contains(secret));
        assert!(!encoded.contains("secret-in-path"));
        let (_, skipped) = evaluator
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "other",
                    key: "object",
                    tags: &requested,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("principal", "other-request"),
            )
            .unwrap();
        assert!(!serde_json::to_string(&skipped).unwrap().contains(secret));
    }

    #[test]
    fn strict_legacy_default_endpoint_is_stable_but_switching_endpoint_is_not() {
        use crate::{config::OptionalPinControlMode, pinning::decision::DecisionOrigin};
        let mut remote = provider("remote", 1);
        remote.kind = "pinata".into();
        remote.token_env = Some("PINNING_TOKEN".into());
        remote.endpoint = None;
        let mut raw = PinningConfig {
            providers: vec![remote],
            policies: vec![rule(
                "bucket",
                "",
                "request",
                "one",
                &["remote"],
                "1d",
                "30d",
                false,
            )],
            ..PinningConfig::default()
        };
        let original = ValidatedPinningConfig::from_raw(&raw, |_| Some("token".into())).unwrap();
        let tags = tags(&[("ipfs-s3:pin", "true")]);
        let (_, captured) = PinPolicyEvaluator::new(&original)
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "object",
                    tags: &tags,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("principal", "request"),
            )
            .unwrap();
        let same = ValidatedPinningConfig::from_raw(&raw, |_| Some("token".into())).unwrap();
        assert!(
            captured
                .verify_revision(&same, OptionalPinControlMode::Strict)
                .is_ok()
        );
        let rotated_token =
            ValidatedPinningConfig::from_raw(&raw, |_| Some("different-token".into())).unwrap();
        assert!(
            captured
                .verify_revision(&rotated_token, OptionalPinControlMode::Strict)
                .is_err()
        );
        raw.providers[0].endpoint = Some("https://other.example.test/v3".into());
        let changed = ValidatedPinningConfig::from_raw(&raw, |_| Some("token".into())).unwrap();
        assert!(
            captured
                .verify_revision(&changed, OptionalPinControlMode::Strict)
                .is_err()
        );
        let no_policy =
            ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
        assert!(
            PinPolicyEvaluator::new(&no_policy)
                .evaluate_publication_decision(
                    PublicationContext {
                        bucket: "bucket",
                        key: "object",
                        tags: &tags,
                        is_decompress_zip: false
                    },
                    DecisionOrigin::new("principal", "strict-no-policy"),
                )
                .is_err()
        );
    }

    #[test]
    fn request_trigger_requires_a_manual_request() {
        let evaluator = request_policy();
        let no_tags = Vec::new();
        let requested = tags(&[("ipfs-s3:pin", "true")]);

        assert!(
            publication(&evaluator, "bucket", "object", &no_tags, false)
                .leases
                .is_empty()
        );
        assert_eq!(
            publication(&evaluator, "bucket", "object", &requested, false).leases[0].source,
            LeaseSource::Manual
        );
    }

    #[test]
    fn always_policy_creates_ordered_independent_automatic_and_manual_leases() {
        let evaluator = evaluator_with_rules(vec![rule(
            "bucket",
            "",
            "always",
            "all",
            &["alpha", "bravo"],
            "30d",
            "90d",
            false,
        )]);
        let requested = tags(&[
            ("team", "infra"),
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:duration", "10d"),
        ]);

        let plan = publication(&evaluator, "bucket", "object", &requested, false);

        assert_eq!(plan.tags, requested);
        assert_eq!(
            plan.leases
                .iter()
                .map(|lease| lease.source)
                .collect::<Vec<_>>(),
            vec![LeaseSource::Automatic, LeaseSource::Manual]
        );
        assert_eq!(
            plan.leases[0].duration,
            LeaseDuration::parse("30d").unwrap()
        );
        assert_eq!(
            plan.leases[1].duration,
            LeaseDuration::parse("10d").unwrap()
        );
        assert_eq!(plan.leases[0].content_mode, ContentMode::Object);
        assert_eq!(plan.leases[1].content_mode, ContentMode::Object);
    }

    #[test]
    fn manual_duration_defaults_to_policy_default_accepts_max_and_rejects_over_max() {
        let evaluator = request_policy();
        let default_duration = tags(&[("ipfs-s3:pin", "true")]);
        let exact_max = tags(&[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "30d")]);
        let over_max = tags(&[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "31d")]);

        assert_eq!(
            publication(&evaluator, "bucket", "object", &default_duration, false).leases[0]
                .duration,
            LeaseDuration::parse("1d").unwrap()
        );
        assert_eq!(
            publication(&evaluator, "bucket", "object", &exact_max, false).leases[0].duration,
            LeaseDuration::parse("30d").unwrap()
        );
        assert!(
            evaluator
                .evaluate_publication(PublicationContext {
                    bucket: "bucket",
                    key: "object",
                    tags: &over_max,
                    is_decompress_zip: false,
                })
                .is_err()
        );
    }

    #[test]
    fn initial_retain_until_is_rejected() {
        let evaluator = request_policy();
        let renewal = renew_tags(at(22));

        assert!(
            evaluator
                .evaluate_publication(PublicationContext {
                    bucket: "bucket",
                    key: "object",
                    tags: &renewal,
                    is_decompress_zip: false,
                })
                .is_err()
        );
    }

    #[test]
    fn active_renewal_is_idempotent_for_equal_expiry_and_rejects_shorter_or_too_long_values() {
        let evaluator = request_policy();
        let existing = existing_manual(
            policy_id(&evaluator),
            ExistingManualLeaseState::Active,
            at(25),
        );

        assert_eq!(
            evaluator
                .evaluate_tag_replacement(Some(&existing), &renew_tags(at(25)), now())
                .unwrap(),
            ManualLeaseMutation::Keep
        );
        assert_eq!(
            evaluator
                .evaluate_tag_replacement(Some(&existing), &renew_tags(at(26)), now())
                .unwrap(),
            ManualLeaseMutation::Renew {
                retain_until: at(26)
            }
        );
        assert!(
            evaluator
                .evaluate_tag_replacement(Some(&existing), &renew_tags(at(24)), now())
                .is_err()
        );
        assert!(
            evaluator
                .evaluate_tag_replacement(Some(&existing), &renew_tags(august(21)), now())
                .is_err()
        );
    }

    #[test]
    fn expired_renewal_requires_a_timestamp_strictly_after_old_expiry_and_now() {
        let evaluator = request_policy();
        let existing = existing_manual(
            policy_id(&evaluator),
            ExistingManualLeaseState::Expired,
            at(20),
        );

        assert_eq!(
            evaluator
                .evaluate_tag_replacement(Some(&existing), &renew_tags(at(22)), now())
                .unwrap(),
            ManualLeaseMutation::Renew {
                retain_until: at(22)
            }
        );
        assert!(
            evaluator
                .evaluate_tag_replacement(Some(&existing), &renew_tags(now()), now())
                .is_err()
        );
        assert!(
            evaluator
                .evaluate_tag_replacement(Some(&existing), &renew_tags(at(20)), now())
                .is_err()
        );
    }

    #[test]
    fn cancelled_and_evicted_manual_leases_cannot_be_renewed() {
        let evaluator = request_policy();

        for state in [
            ExistingManualLeaseState::Cancelled,
            ExistingManualLeaseState::Evicted,
        ] {
            let existing = existing_manual(policy_id(&evaluator), state, at(20));
            assert!(
                evaluator
                    .evaluate_tag_replacement(Some(&existing), &renew_tags(at(22)), now())
                    .is_err()
            );
        }
    }

    #[test]
    fn unknown_captured_policy_identity_is_rejected() {
        let evaluator = request_policy();
        let existing = existing_manual(
            "policy:unknown".to_owned(),
            ExistingManualLeaseState::Active,
            at(25),
        );

        assert!(
            evaluator
                .evaluate_tag_replacement(Some(&existing), &renew_tags(at(26)), now())
                .is_err()
        );
    }

    #[test]
    fn omission_and_cancel_only_cancel_an_active_manual_lease() {
        let evaluator = request_policy();
        let active = existing_manual(
            policy_id(&evaluator),
            ExistingManualLeaseState::Active,
            at(25),
        );
        let expired = existing_manual(
            policy_id(&evaluator),
            ExistingManualLeaseState::Expired,
            at(20),
        );
        let empty = Vec::new();
        let cancel = tags(&[("ipfs-s3:pin", "false")]);

        assert_eq!(
            evaluator
                .evaluate_tag_replacement(Some(&active), &empty, now())
                .unwrap(),
            ManualLeaseMutation::Cancel
        );
        assert_eq!(
            evaluator
                .evaluate_tag_replacement(Some(&active), &cancel, now())
                .unwrap(),
            ManualLeaseMutation::Cancel
        );
        assert_eq!(
            evaluator
                .evaluate_tag_replacement(Some(&expired), &empty, now())
                .unwrap(),
            ManualLeaseMutation::Keep
        );
    }

    #[test]
    fn tag_replacement_cannot_create_or_reconfigure_a_manual_lease() {
        let evaluator = request_policy();
        let request = tags(&[("ipfs-s3:pin", "true")]);
        let existing = existing_manual(
            policy_id(&evaluator),
            ExistingManualLeaseState::Active,
            at(25),
        );
        let reconfigured = tags(&[("ipfs-s3:pin", "true"), ("ipfs-s3:content", "decompressed")]);

        assert!(
            evaluator
                .evaluate_tag_replacement(None, &request, now())
                .is_err()
        );
        assert!(
            evaluator
                .evaluate_tag_replacement(Some(&existing), &reconfigured, now())
                .is_err()
        );
    }

    #[test]
    fn provider_modes_preserve_validated_priority_name_order() {
        let evaluator = evaluator_with_rules(vec![
            rule(
                "bucket",
                "one/",
                "always",
                "one",
                &["charlie", "alpha", "bravo"],
                "1d",
                "30d",
                false,
            ),
            rule(
                "bucket",
                "all/",
                "always",
                "all",
                &["charlie", "alpha", "bravo"],
                "1d",
                "30d",
                false,
            ),
        ]);
        let no_tags = Vec::new();

        let one = publication(&evaluator, "bucket", "one/object", &no_tags, false);
        let all = publication(&evaluator, "bucket", "all/object", &no_tags, false);

        assert_eq!(one.leases[0].provider_mode, ProviderMode::One);
        assert_eq!(all.leases[0].provider_mode, ProviderMode::All);
        assert_eq!(one.leases[0].providers, vec!["bravo", "alpha", "charlie"]);
        assert_eq!(all.leases[0].providers, vec!["bravo", "alpha", "charlie"]);
    }

    #[test]
    fn manual_decompressed_content_requires_policy_permission_and_a_zip_context() {
        let request = tags(&[("ipfs-s3:pin", "true"), ("ipfs-s3:content", "decompressed")]);
        let disallowed = evaluator_with_rules(vec![rule(
            "bucket",
            "",
            "always",
            "one",
            &["alpha"],
            "1d",
            "30d",
            false,
        )]);
        let allowed = evaluator_with_rules(vec![rule(
            "bucket",
            "",
            "always",
            "one",
            &["alpha"],
            "1d",
            "30d",
            true,
        )]);

        assert!(
            disallowed
                .evaluate_publication(PublicationContext {
                    bucket: "bucket",
                    key: "archive.zip",
                    tags: &request,
                    is_decompress_zip: true,
                })
                .is_err()
        );
        assert!(
            allowed
                .evaluate_publication(PublicationContext {
                    bucket: "bucket",
                    key: "object",
                    tags: &request,
                    is_decompress_zip: false,
                })
                .is_err()
        );

        let plan = publication(&allowed, "bucket", "archive.zip", &request, true);
        assert_eq!(plan.leases[0].content_mode, ContentMode::Object);
        assert_eq!(plan.leases[1].content_mode, ContentMode::Decompressed);
    }

    #[test]
    fn policy_errors_become_invalid_argument_s3_errors() {
        let error: AppError = PolicyError::InvalidRequest("bad request".to_owned()).into();
        let s3_error: s3s::S3Error = error.into();

        assert_eq!(s3_error.code().as_str(), "InvalidArgument");
        assert_eq!(s3_error.status_code(), Some(http::StatusCode::BAD_REQUEST));
    }

    #[test]
    fn maximum_renewal_duration_overflow_is_an_error_not_a_panic() {
        let max_duration = LeaseDuration::parse("9223372036854775807s").unwrap();

        assert!(super::maximum_retain_until(now(), max_duration).is_err());
    }
}
