use chrono::{DateTime, TimeDelta, Utc};

use crate::{
    error::AppError,
    pinning::{
        config::{
            LeaseDuration, PolicyTrigger, ProviderMode, ValidatedPinningConfig, ValidatedPolicy,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseSource {
    Automatic,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
}

impl PinPolicyEvaluator {
    pub fn new(config: &ValidatedPinningConfig) -> Self {
        Self {
            policies: config.policies.clone(),
        }
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
