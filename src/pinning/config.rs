use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use anyhow::{anyhow, bail};
use sha2::{Digest, Sha256};

use crate::config::{PinningConfig, PolicyConfig, ProviderConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKind {
    Pinata,
    Filebase,
    Noop,
}

impl ProviderKind {
    fn parse(raw: &str) -> anyhow::Result<Self> {
        match raw {
            "pinata" => Ok(Self::Pinata),
            "filebase" => Ok(Self::Filebase),
            "noop" => Ok(Self::Noop),
            _ => bail!("unknown provider kind `{raw}`"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyTrigger {
    Always,
    Request,
}

impl PolicyTrigger {
    fn parse(raw: &str) -> anyhow::Result<Self> {
        match raw {
            "always" => Ok(Self::Always),
            "request" => Ok(Self::Request),
            _ => bail!("unknown policy trigger `{raw}`"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::Request => "request",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderMode {
    One,
    All,
}

impl ProviderMode {
    fn parse(raw: &str) -> anyhow::Result<Self> {
        match raw {
            "one" => Ok(Self::One),
            "all" => Ok(Self::All),
            _ => bail!("unknown provider mode `{raw}`"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::One => "one",
            Self::All => "all",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LeaseDuration(u64);

impl LeaseDuration {
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        let Some((unit_index, unit)) = raw.char_indices().last() else {
            bail!("invalid lease duration");
        };
        let number = &raw[..unit_index];
        if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!("invalid lease duration");
        }

        let value = number
            .parse::<u64>()
            .map_err(|_| anyhow!("invalid lease duration"))?;
        if value == 0 {
            bail!("invalid lease duration");
        }

        let multiplier = match unit {
            's' => 1,
            'm' => 60,
            'h' => 60 * 60,
            'd' => 60 * 60 * 24,
            _ => bail!("invalid lease duration"),
        };
        let seconds = value
            .checked_mul(multiplier)
            .filter(|seconds| *seconds <= i64::MAX as u64)
            .ok_or_else(|| anyhow!("invalid lease duration"))?;

        Ok(Self(seconds))
    }

    pub fn as_seconds(self) -> u64 {
        self.0
    }
}

#[derive(Clone)]
pub struct SecretToken(String);

impl SecretToken {
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretToken([REDACTED])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderLimits {
    pub priority: u32,
    pub max_bytes: i64,
    pub max_pins: i64,
    pub enabled: bool,
}

pub type ProviderLimitMap = BTreeMap<String, ProviderLimits>;

#[derive(Debug, Clone)]
pub struct ValidatedProvider {
    pub name: String,
    pub kind: ProviderKind,
    pub token: Option<SecretToken>,
    pub endpoint: Option<String>,
    pub limits: ProviderLimits,
    pub requests_per_second: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct ValidatedPolicy {
    pub identity: String,
    pub bucket: String,
    pub prefix: String,
    pub trigger: PolicyTrigger,
    pub provider_mode: ProviderMode,
    pub providers: Vec<String>,
    pub default_duration: LeaseDuration,
    pub max_duration: LeaseDuration,
    pub allow_decompressed: bool,
}

#[derive(Debug, Clone)]
pub struct ValidatedPinningConfig {
    pub worker_interval: LeaseDuration,
    pub worker_concurrency: usize,
    pub providers: Vec<ValidatedProvider>,
    pub policies: Vec<ValidatedPolicy>,
    pub provider_limits: ProviderLimitMap,
}

impl ValidatedPinningConfig {
    pub fn from_raw<F>(raw: &PinningConfig, get_env: F) -> anyhow::Result<Self>
    where
        F: Fn(&str) -> Option<String>,
    {
        let mut provider_names = BTreeSet::new();
        for provider in &raw.providers {
            if !provider_names.insert(provider.name.as_str()) {
                bail!("duplicate provider name `{}`", provider.name);
            }
        }

        let worker_interval = LeaseDuration::parse(&raw.worker_interval)?;
        if raw.worker_concurrency == 0 {
            bail!("worker concurrency must be greater than zero");
        }

        let mut providers = Vec::with_capacity(raw.providers.len());
        let mut provider_limits = ProviderLimitMap::new();
        for provider in &raw.providers {
            let validated = Self::validate_provider(provider, &get_env)?;
            provider_limits.insert(validated.name.clone(), validated.limits.clone());
            providers.push(validated);
        }

        let policies = raw
            .policies
            .iter()
            .enumerate()
            .map(|(index, policy)| Self::validate_policy(index, policy, &provider_limits))
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self {
            worker_interval,
            worker_concurrency: raw.worker_concurrency,
            providers,
            policies,
            provider_limits,
        })
    }

    fn validate_provider<F>(
        provider: &ProviderConfig,
        get_env: &F,
    ) -> anyhow::Result<ValidatedProvider>
    where
        F: Fn(&str) -> Option<String>,
    {
        if provider.name.is_empty() {
            bail!("provider name must not be empty");
        }

        let kind = ProviderKind::parse(&provider.kind)?;
        if provider.max_bytes == 0 {
            bail!(
                "provider `{}` max_bytes must be greater than zero",
                provider.name
            );
        }
        if provider.max_pins == 0 {
            bail!(
                "provider `{}` max_pins must be greater than zero",
                provider.name
            );
        }
        if provider.requests_per_second == Some(0) {
            bail!(
                "provider `{}` requests_per_second must be greater than zero",
                provider.name
            );
        }

        let limits = ProviderLimits {
            priority: provider.priority,
            max_bytes: quota_as_i64(provider.max_bytes, &provider.name, "max_bytes")?,
            max_pins: quota_as_i64(provider.max_pins, &provider.name, "max_pins")?,
            enabled: provider.enabled,
        };
        let token = match kind {
            ProviderKind::Pinata | ProviderKind::Filebase => {
                let token_env = provider
                    .token_env
                    .as_deref()
                    .filter(|name| !name.is_empty())
                    .ok_or_else(|| {
                        anyhow!(
                            "provider `{}` token environment variable must be configured",
                            provider.name
                        )
                    })?;
                let token = get_env(token_env)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        anyhow!(
                            "provider `{}` token environment variable `{token_env}` is not set",
                            provider.name
                        )
                    })?;
                Some(SecretToken(token))
            }
            ProviderKind::Noop => {
                if provider.token_env.is_some() {
                    bail!(
                        "noop provider `{}` must not configure a token environment variable",
                        provider.name
                    );
                }
                None
            }
        };

        Ok(ValidatedProvider {
            name: provider.name.clone(),
            kind,
            token,
            endpoint: provider.endpoint.clone(),
            limits,
            requests_per_second: provider.requests_per_second,
        })
    }

    fn validate_policy(
        index: usize,
        policy: &PolicyConfig,
        provider_limits: &ProviderLimitMap,
    ) -> anyhow::Result<ValidatedPolicy> {
        if policy.bucket.is_empty() || (policy.bucket.contains('*') && policy.bucket != "*") {
            bail!("policy bucket must be a non-empty exact name or `*`");
        }
        if policy.providers.is_empty() {
            bail!(
                "policy `{}` must reference at least one provider",
                policy.bucket
            );
        }

        let trigger = PolicyTrigger::parse(&policy.trigger)?;
        let provider_mode = ProviderMode::parse(&policy.provider_mode)?;
        let default_duration = LeaseDuration::parse(&policy.default_duration)?;
        let max_duration = LeaseDuration::parse(&policy.max_duration)?;
        if default_duration > max_duration {
            bail!(
                "policy `{}` default duration exceeds max duration",
                policy.bucket
            );
        }

        let mut referenced_provider_names = BTreeSet::new();
        let mut providers = Vec::with_capacity(policy.providers.len());
        for provider_name in &policy.providers {
            if provider_name.is_empty() {
                bail!(
                    "policy `{}` references an empty provider name",
                    policy.bucket
                );
            }
            if !provider_limits.contains_key(provider_name) {
                bail!(
                    "policy `{}` references unknown provider `{provider_name}`",
                    policy.bucket
                );
            }
            if !referenced_provider_names.insert(provider_name.as_str()) {
                bail!(
                    "policy '{}' contains duplicate provider '{}'",
                    policy.bucket,
                    provider_name
                );
            }
            providers.push(provider_name.clone());
        }
        if !providers
            .iter()
            .any(|provider_name| provider_limits[provider_name].enabled)
        {
            bail!(
                "policy `{}` must reference at least one enabled provider",
                policy.bucket
            );
        }
        providers.sort_by(|left, right| {
            provider_limits[left]
                .priority
                .cmp(&provider_limits[right].priority)
                .then_with(|| left.cmp(right))
        });

        let identity = policy_identity(
            index,
            policy,
            trigger,
            provider_mode,
            &providers,
            default_duration,
            max_duration,
        );
        Ok(ValidatedPolicy {
            identity,
            bucket: policy.bucket.clone(),
            prefix: policy.prefix.clone(),
            trigger,
            provider_mode,
            providers,
            default_duration,
            max_duration,
            allow_decompressed: policy.allow_decompressed,
        })
    }
}

fn quota_as_i64(value: u64, provider_name: &str, quota_name: &str) -> anyhow::Result<i64> {
    i64::try_from(value)
        .map_err(|_| anyhow!("provider `{provider_name}` {quota_name} exceeds i64::MAX"))
}

fn policy_identity(
    index: usize,
    policy: &PolicyConfig,
    trigger: PolicyTrigger,
    provider_mode: ProviderMode,
    providers: &[String],
    default_duration: LeaseDuration,
    max_duration: LeaseDuration,
) -> String {
    let mut canonical = String::new();
    append_canonical_field(&mut canonical, &policy.bucket);
    append_canonical_field(&mut canonical, &policy.prefix);
    append_canonical_field(&mut canonical, trigger.as_str());
    append_canonical_field(&mut canonical, provider_mode.as_str());
    append_canonical_field(&mut canonical, &providers.len().to_string());
    for provider in providers {
        append_canonical_field(&mut canonical, provider);
    }
    append_canonical_field(&mut canonical, &default_duration.as_seconds().to_string());
    append_canonical_field(&mut canonical, &max_duration.as_seconds().to_string());
    append_canonical_field(
        &mut canonical,
        if policy.allow_decompressed {
            "true"
        } else {
            "false"
        },
    );

    format!(
        "policy:{index}:{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    )
}

fn append_canonical_field(canonical: &mut String, value: &str) {
    canonical.push_str(&value.len().to_string());
    canonical.push(':');
    canonical.push_str(value);
    canonical.push('|');
}

#[cfg(test)]
mod tests {
    use super::{
        LeaseDuration, PolicyTrigger, ProviderKind, ProviderMode, SecretToken,
        ValidatedPinningConfig,
    };
    use crate::config::{PinningConfig, PolicyConfig, ProviderConfig};

    fn provider(name: &str, kind: &str) -> ProviderConfig {
        ProviderConfig {
            name: name.to_owned(),
            kind: kind.to_owned(),
            token_env: match kind {
                "pinata" => Some("PINATA_TOKEN".to_owned()),
                "filebase" => Some("FILEBASE_TOKEN".to_owned()),
                _ => None,
            },
            endpoint: Some(format!("https://{name}.example.test")),
            enabled: true,
            priority: 10,
            max_bytes: 100,
            max_pins: 10,
            requests_per_second: Some(5),
        }
    }

    fn policy(bucket: &str, providers: Vec<&str>) -> PolicyConfig {
        PolicyConfig {
            bucket: bucket.to_owned(),
            prefix: String::new(),
            trigger: "always".to_owned(),
            provider_mode: "one".to_owned(),
            providers: providers.into_iter().map(str::to_owned).collect(),
            default_duration: "1m".to_owned(),
            max_duration: "2h".to_owned(),
            allow_decompressed: false,
        }
    }

    fn raw(providers: Vec<ProviderConfig>, policies: Vec<PolicyConfig>) -> PinningConfig {
        PinningConfig {
            worker_interval: "5s".to_owned(),
            worker_concurrency: 4,
            providers,
            policies,
        }
    }

    fn environment(name: &str) -> Option<String> {
        Some(format!("token-for-{name}"))
    }

    fn assert_validation_error<F>(config: &PinningConfig, get_env: F, expected: &str)
    where
        F: Fn(&str) -> Option<String>,
    {
        let error = ValidatedPinningConfig::from_raw(config, get_env).unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "expected error containing {expected:?}, got {error:#}",
        );
    }

    #[test]
    fn parses_valid_lease_durations() {
        let cases = [("1s", 1), ("5m", 300), ("2h", 7_200), ("30d", 2_592_000)];

        for (raw, expected_seconds) in cases {
            assert_eq!(
                LeaseDuration::parse(raw).unwrap().as_seconds(),
                expected_seconds
            );
        }
    }

    #[test]
    fn rejects_invalid_lease_durations() {
        let cases = [
            "",
            "0s",
            "-1s",
            "+1s",
            "1.5h",
            "1w",
            " 1s",
            "1s ",
            "9223372036854775808s",
            "18446744073709551615d",
        ];

        for raw in cases {
            assert!(
                LeaseDuration::parse(raw).is_err(),
                "{raw:?} should be invalid"
            );
        }
    }

    #[test]
    fn validates_an_empty_default_as_a_noop_configuration() {
        let validated = ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| {
            panic!("no provider token should be resolved")
        })
        .unwrap();

        assert_eq!(validated.worker_interval.as_seconds(), 5);
        assert_eq!(validated.worker_concurrency, 4);
        assert!(validated.providers.is_empty());
        assert!(validated.policies.is_empty());
        assert!(validated.provider_limits.is_empty());
    }

    #[test]
    fn rejects_duplicate_provider_names_before_token_resolution() {
        let config = raw(
            vec![
                provider("duplicate", "pinata"),
                provider("duplicate", "filebase"),
            ],
            Vec::new(),
        );

        assert_validation_error(
            &config,
            |name| panic!("duplicate validation resolved token environment {name}"),
            "duplicate provider name",
        );
    }

    #[test]
    fn rejects_invalid_provider_configuration() {
        let mut unknown_kind = raw(vec![provider("unknown-kind", "other")], Vec::new());
        unknown_kind.providers[0].token_env = None;

        let mut missing_token_env = raw(vec![provider("missing-token", "pinata")], Vec::new());
        missing_token_env.providers[0].token_env = None;

        let mut empty_token_env = raw(vec![provider("empty-token", "filebase")], Vec::new());
        empty_token_env.providers[0].token_env = Some(String::new());

        let mut noop_token_env = raw(vec![provider("noop-token", "noop")], Vec::new());
        noop_token_env.providers[0].token_env = Some("NOOP_TOKEN".to_owned());

        let mut zero_max_bytes = raw(vec![provider("zero-bytes", "noop")], Vec::new());
        zero_max_bytes.providers[0].max_bytes = 0;

        let mut zero_max_pins = raw(vec![provider("zero-pins", "noop")], Vec::new());
        zero_max_pins.providers[0].max_pins = 0;

        let mut zero_requests = raw(vec![provider("zero-rps", "noop")], Vec::new());
        zero_requests.providers[0].requests_per_second = Some(0);

        let mut empty_name = raw(vec![provider("", "noop")], Vec::new());
        empty_name.providers[0].name = String::new();

        let mut excessive_bytes = raw(vec![provider("large-bytes", "noop")], Vec::new());
        excessive_bytes.providers[0].max_bytes = i64::MAX as u64 + 1;

        let mut excessive_pins = raw(vec![provider("large-pins", "noop")], Vec::new());
        excessive_pins.providers[0].max_pins = i64::MAX as u64 + 1;

        let cases = vec![
            (unknown_kind, "unknown provider kind"),
            (missing_token_env, "token environment variable"),
            (empty_token_env, "token environment variable"),
            (
                noop_token_env,
                "must not configure a token environment variable",
            ),
            (zero_max_bytes, "max_bytes must be greater than zero"),
            (zero_max_pins, "max_pins must be greater than zero"),
            (
                zero_requests,
                "requests_per_second must be greater than zero",
            ),
            (empty_name, "provider name must not be empty"),
            (excessive_bytes, "max_bytes exceeds i64::MAX"),
            (excessive_pins, "max_pins exceeds i64::MAX"),
        ];

        for (config, expected) in cases {
            assert_validation_error(&config, environment, expected);
        }

        let unresolved_token = raw(vec![provider("unresolved", "pinata")], Vec::new());
        assert_validation_error(
            &unresolved_token,
            |_| None,
            "token environment variable `PINATA_TOKEN` is not set",
        );
    }

    #[test]
    fn rejects_invalid_worker_configuration() {
        let mut invalid_interval = raw(Vec::new(), Vec::new());
        invalid_interval.worker_interval = "one second".to_owned();

        let mut zero_interval = raw(Vec::new(), Vec::new());
        zero_interval.worker_interval = "0s".to_owned();

        let mut excessive_interval = raw(Vec::new(), Vec::new());
        excessive_interval.worker_interval = "9223372036854775808s".to_owned();

        let mut zero_concurrency = raw(Vec::new(), Vec::new());
        zero_concurrency.worker_concurrency = 0;

        let cases = vec![
            (invalid_interval, "invalid lease duration"),
            (zero_interval, "invalid lease duration"),
            (excessive_interval, "invalid lease duration"),
            (
                zero_concurrency,
                "worker concurrency must be greater than zero",
            ),
        ];

        for (config, expected) in cases {
            assert_validation_error(&config, environment, expected);
        }
    }

    #[test]
    fn rejects_invalid_policy_configuration() {
        let providers = vec![provider("noop", "noop")];
        let mut disabled_provider = provider("disabled", "noop");
        disabled_provider.enabled = false;

        let empty_provider_set = raw(providers.clone(), vec![policy("bucket", Vec::new())]);
        let unknown_provider = raw(providers.clone(), vec![policy("bucket", vec!["missing"])]);
        let empty_provider_name = raw(providers.clone(), vec![policy("bucket", vec![""])]);
        let duplicate_provider = raw(
            providers.clone(),
            vec![policy("bucket", vec!["noop", "noop"])],
        );
        let empty_bucket = raw(providers.clone(), vec![policy("", vec!["noop"])]);
        let wildcard_bucket = raw(providers.clone(), vec![policy("bucket*", vec!["noop"])]);
        let only_disabled_provider = raw(
            vec![disabled_provider],
            vec![policy("bucket", vec!["disabled"])],
        );

        let mut unknown_trigger = raw(providers.clone(), vec![policy("bucket", vec!["noop"])]);
        unknown_trigger.policies[0].trigger = "Always".to_owned();

        let mut unknown_provider_mode =
            raw(providers.clone(), vec![policy("bucket", vec!["noop"])]);
        unknown_provider_mode.policies[0].provider_mode = "any".to_owned();

        let mut duration_exceeds_max = raw(providers.clone(), vec![policy("bucket", vec!["noop"])]);
        duration_exceeds_max.policies[0].default_duration = "3h".to_owned();

        let mut invalid_default_duration =
            raw(providers.clone(), vec![policy("bucket", vec!["noop"])]);
        invalid_default_duration.policies[0].default_duration = "0s".to_owned();

        let mut excessive_max_duration = raw(providers, vec![policy("bucket", vec!["noop"])]);
        excessive_max_duration.policies[0].max_duration = "9223372036854775808s".to_owned();

        let cases = vec![
            (empty_provider_set, "must reference at least one provider"),
            (unknown_provider, "references unknown provider"),
            (empty_provider_name, "references an empty provider name"),
            (
                duplicate_provider,
                "policy 'bucket' contains duplicate provider 'noop'",
            ),
            (empty_bucket, "bucket must be a non-empty exact name or `*`"),
            (
                wildcard_bucket,
                "bucket must be a non-empty exact name or `*`",
            ),
            (
                only_disabled_provider,
                "must reference at least one enabled provider",
            ),
            (unknown_trigger, "unknown policy trigger"),
            (unknown_provider_mode, "unknown provider mode"),
            (
                duration_exceeds_max,
                "default duration exceeds max duration",
            ),
            (invalid_default_duration, "invalid lease duration"),
            (excessive_max_duration, "invalid lease duration"),
        ];

        for (config, expected) in cases {
            assert_validation_error(&config, environment, expected);
        }
    }

    #[test]
    fn normalizes_providers_and_policies_without_reordering_rules() {
        let mut pinata = provider("pinata", "pinata");
        pinata.priority = 2;
        let mut filebase = provider("filebase", "filebase");
        filebase.priority = 1;
        let mut noop = provider("noop", "noop");
        noop.priority = 3;

        let mut first = policy("first-bucket", vec!["noop", "pinata", "filebase"]);
        first.prefix = "images/".to_owned();
        first.trigger = "request".to_owned();
        first.provider_mode = "all".to_owned();
        first.default_duration = "60s".to_owned();

        let second = policy("second-bucket", vec!["noop"]);
        let config = raw(vec![pinata, filebase, noop], vec![first, second]);
        let validated = ValidatedPinningConfig::from_raw(&config, environment).unwrap();

        assert_eq!(validated.providers.len(), 3);
        assert_eq!(validated.providers[0].kind, ProviderKind::Pinata);
        assert_eq!(validated.providers[1].kind, ProviderKind::Filebase);
        assert_eq!(validated.providers[2].kind, ProviderKind::Noop);
        assert_eq!(
            validated.providers[0].token.as_ref().unwrap().expose(),
            "token-for-PINATA_TOKEN"
        );
        assert_eq!(
            validated.providers[1].token.as_ref().unwrap().expose(),
            "token-for-FILEBASE_TOKEN"
        );
        assert!(validated.providers[2].token.is_none());

        assert_eq!(validated.policies.len(), 2);
        assert_eq!(validated.policies[0].bucket, "first-bucket");
        assert_eq!(validated.policies[0].trigger, PolicyTrigger::Request);
        assert_eq!(validated.policies[0].provider_mode, ProviderMode::All);
        assert_eq!(
            validated.policies[0].providers,
            vec!["filebase", "pinata", "noop"]
        );
        assert_eq!(validated.policies[1].bucket, "second-bucket");
        assert!(validated.policies[0].identity.starts_with("policy:0:"));
        assert!(validated.policies[1].identity.starts_with("policy:1:"));

        assert_eq!(validated.provider_limits["filebase"].priority, 1);
        assert_eq!(validated.provider_limits["filebase"].max_bytes, 100);
        assert_eq!(validated.provider_limits["filebase"].max_pins, 10);
        assert!(validated.provider_limits["filebase"].enabled);

        let mut equivalent = config.clone();
        equivalent.policies[0].default_duration = "1m".to_owned();
        let equivalent_validated =
            ValidatedPinningConfig::from_raw(&equivalent, environment).unwrap();
        assert_eq!(
            validated.policies[0].identity,
            equivalent_validated.policies[0].identity
        );

        let mut reordered = config.clone();
        reordered.policies.reverse();
        let reordered_validated =
            ValidatedPinningConfig::from_raw(&reordered, environment).unwrap();
        assert_ne!(
            validated.policies[0].identity,
            reordered_validated.policies[1].identity
        );
    }

    #[test]
    fn keeps_disabled_policy_providers_when_one_provider_is_enabled() {
        let mut enabled = provider("enabled", "noop");
        enabled.priority = 2;
        let mut disabled = provider("disabled", "noop");
        disabled.priority = 1;
        disabled.enabled = false;
        let config = raw(
            vec![enabled, disabled],
            vec![policy("bucket", vec!["enabled", "disabled"])],
        );

        let validated = ValidatedPinningConfig::from_raw(&config, environment).unwrap();

        assert_eq!(validated.policies[0].providers, vec!["disabled", "enabled"]);
        assert!(!validated.provider_limits["disabled"].enabled);
        assert!(validated.provider_limits["enabled"].enabled);
    }

    #[test]
    fn policy_identity_is_canonical_and_changes_for_each_field() {
        let fixture = canonical_identity_fixture();
        let identity = first_policy_identity(&fixture);
        assert_eq!(
            identity,
            "policy:0:61f2bdb126be32dd2420746af2c3e6634c28c4d90ab42a86ba4f4b03c7975690"
        );

        let mut equivalent_duration = fixture.clone();
        equivalent_duration.policies[0].default_duration = "1m".to_owned();
        assert_eq!(first_policy_identity(&equivalent_duration), identity);

        let mut changed_bucket = fixture.clone();
        changed_bucket.policies[0].bucket = "archives".to_owned();

        let mut changed_prefix = fixture.clone();
        changed_prefix.policies[0].prefix = "thumbnails/".to_owned();

        let mut changed_trigger = fixture.clone();
        changed_trigger.policies[0].trigger = "always".to_owned();

        let mut changed_provider_mode = fixture.clone();
        changed_provider_mode.policies[0].provider_mode = "one".to_owned();

        let mut changed_provider_list = fixture.clone();
        changed_provider_list.policies[0].providers = vec!["alpha".to_owned()];

        let mut changed_default_duration = fixture.clone();
        changed_default_duration.policies[0].default_duration = "2m".to_owned();

        let mut changed_max_duration = fixture.clone();
        changed_max_duration.policies[0].max_duration = "3h".to_owned();

        let mut changed_allow_decompressed = fixture.clone();
        changed_allow_decompressed.policies[0].allow_decompressed = false;

        let mutations = vec![
            ("bucket", changed_bucket),
            ("prefix", changed_prefix),
            ("trigger", changed_trigger),
            ("provider mode", changed_provider_mode),
            ("provider list", changed_provider_list),
            ("default duration", changed_default_duration),
            ("max duration", changed_max_duration),
            ("allow decompressed", changed_allow_decompressed),
        ];
        for (field, config) in mutations {
            assert_ne!(
                first_policy_identity(&config),
                identity,
                "{field} should change the policy identity"
            );
        }

        let mut reordered = fixture.clone();
        reordered.policies.push(policy("other", vec!["alpha"]));
        reordered.policies.reverse();
        assert_ne!(identity_for_policy(&reordered, "photos"), identity);
    }

    fn canonical_identity_fixture() -> PinningConfig {
        let mut alpha = provider("alpha", "noop");
        alpha.priority = 2;
        let mut bravo = provider("bravo", "noop");
        bravo.priority = 1;
        let mut policy = policy("photos", vec!["alpha", "bravo"]);
        policy.prefix = "images/".to_owned();
        policy.trigger = "request".to_owned();
        policy.provider_mode = "all".to_owned();
        policy.default_duration = "60s".to_owned();
        policy.max_duration = "2h".to_owned();
        policy.allow_decompressed = true;

        raw(vec![alpha, bravo], vec![policy])
    }

    fn first_policy_identity(config: &PinningConfig) -> String {
        ValidatedPinningConfig::from_raw(config, environment)
            .unwrap()
            .policies[0]
            .identity
            .clone()
    }

    fn identity_for_policy(config: &PinningConfig, bucket: &str) -> String {
        ValidatedPinningConfig::from_raw(config, environment)
            .unwrap()
            .policies
            .iter()
            .find(|policy| policy.bucket == bucket)
            .unwrap()
            .identity
            .clone()
    }

    #[test]
    fn redacts_secret_tokens_from_debug_output_and_errors() {
        let secret = "unforgettable-secret";
        let token = SecretToken(secret.to_owned());
        assert_eq!(format!("{token:?}"), "SecretToken([REDACTED])");
        assert_eq!(token.expose(), secret);

        let config = raw(
            vec![provider("pinata", "pinata")],
            vec![policy("bucket", vec!["missing"])],
        );
        let error =
            ValidatedPinningConfig::from_raw(&config, |_| Some(secret.to_owned())).unwrap_err();
        assert!(!format!("{error:#}").contains(secret));

        let valid = raw(vec![provider("pinata", "pinata")], Vec::new());
        let validated =
            ValidatedPinningConfig::from_raw(&valid, |_| Some(secret.to_owned())).unwrap();
        assert!(!format!("{validated:?}").contains(secret));
    }
}
