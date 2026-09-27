//! Captured, safe-to-persist pin-control decisions. No raw tag or credential is stored here.
use std::sync::LazyLock;

use hmac::{Hmac, KeyInit, Mac};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    config::{ProviderKind, ValidatedPinningConfig, ValidatedProvider},
    policy::{LeaseIntent, LeaseSource, PublicationPolicy},
    tags::{ObjectTag, PinControl},
};
use crate::config::OptionalPinControlMode;

// Legacy routes have no operator-maintained revision. A process-scoped key
// binds accepted decisions to their resolved route without persisting URLs or
// credentials (or a reusable unkeyed fingerprint of either). After a restart,
// pending legacy work must fail closed and be admitted afresh.
static LEGACY_ROUTE_KEY: LazyLock<[u8; 32]> = LazyLock::new(|| {
    let mut key = [0; 32];
    rand::rng().fill_bytes(&mut key);
    key
});

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

// Keep the existing public snapshot/JSON shape and the DB's 64-hex revision
// constraint. This 64-bit marker identifies a V2 revision; the remaining
// 192 bits are the SHA-256 digest. An old V1 digest matching the marker is
// negligibly unlikely (2^-64) and will fail closed rather than bypass a fence.
const DURABLE_REVISION_MARKER: &str = "d91d726f120fab02";

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
    /// Hash of validated, non-secret policy/provider identity and compatibility mode.
    pub config_revision: String,
    /// Normalized reserved control (not the arbitrary user tag set).
    pub control: PinControl,
    /// Executable intents frozen at capture. Skipped manual controls are never stored here.
    pub effective_intents: Vec<LeaseIntent>,
}

impl ExtensionDecision {
    pub(crate) fn capture(
        origin: DecisionOrigin,
        config_revision: String,
        effect: DecisionEffect,
        warning: Option<WarningCode>,
        control: PinControl,
        policy: &PublicationPolicy,
    ) -> Result<Self, &'static str> {
        Self::capture_with_legacy_unknown(
            origin,
            config_revision,
            effect,
            warning,
            false,
            control,
            policy,
        )
    }

    pub(crate) fn capture_legacy_unknown(
        origin: DecisionOrigin,
        config_revision: String,
        control: PinControl,
        policy: &PublicationPolicy,
    ) -> Result<Self, &'static str> {
        Self::capture_with_legacy_unknown(
            origin,
            config_revision,
            DecisionEffect::NoIntent,
            None,
            true,
            control,
            policy,
        )
    }

    fn capture_with_legacy_unknown(
        origin: DecisionOrigin,
        config_revision: String,
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
            config_revision,
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
            || self.config_revision.len() != 64
            || !self
                .config_revision
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
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

    /// Reconstruct executable leases solely from the captured decision, not the
    /// current policy or raw reserved control. Reject altered control tags.
    pub fn replay_policy(&self, tags: Vec<ObjectTag>) -> Result<PublicationPolicy, &'static str> {
        let policy = PublicationPolicy {
            tags,
            leases: self.effective_intents.clone(),
        };
        self.validate_policy(&policy)?;
        Ok(policy)
    }

    /// Opt in only at durable asynchronous admission (MPU/import). PUT/Copy
    /// and historical snapshots keep their original process-local route fence.
    /// Revision numbers are operator promises: rotate them when the actual
    /// account credential or endpoint changes; never persist a secret digest.
    pub fn capture_durable_revision(
        &mut self,
        config: &ValidatedPinningConfig,
        mode: OptionalPinControlMode,
    ) -> Result<(), &'static str> {
        self.validate_snapshot()?;
        if self.effective_intents.is_empty() {
            return Ok(());
        }
        let revision = durable_revision(config, mode, &self.effective_intents)?;
        self.config_revision = revision;
        Ok(())
    }

    /// Fail closed on a changed provider route, account, availability, or policy.
    /// A captured skipped decision must never be re-evaluated under the new config.
    pub fn verify_revision(
        &self,
        config: &ValidatedPinningConfig,
        mode: OptionalPinControlMode,
    ) -> Result<(), &'static str> {
        if self.effective_intents.is_empty() && self.effect != DecisionEffect::Accepted {
            return Ok(());
        }
        let current = if self.config_revision.starts_with(DURABLE_REVISION_MARKER) {
            durable_revision(config, mode, &self.effective_intents)
                .map_err(|_| "captured pinning configuration revision is unavailable")?
        } else {
            config_revision(config, mode)
        };
        if self.config_revision == current {
            Ok(())
        } else {
            Err("captured pinning configuration revision is unavailable")
        }
    }
}

/// V2 deliberately binds only providers referenced by executable intents.
/// Legacy providers elsewhere in the configuration cannot inject their random
/// process MAC into the durable revision of a selected explicit provider.
fn durable_revision(
    config: &ValidatedPinningConfig,
    mode: OptionalPinControlMode,
    intents: &[LeaseIntent],
) -> Result<String, &'static str> {
    use std::collections::BTreeSet;
    let mut hash = Sha256::new();
    field(&mut hash, "pin-control/durable-identity/v2");
    field(
        &mut hash,
        match mode {
            OptionalPinControlMode::Strict => "strict",
            OptionalPinControlMode::Warn => "warn",
        },
    );
    field(&mut hash, &config.policies.len().to_string());
    for policy in &config.policies {
        field(&mut hash, &policy.identity);
    }
    let names: BTreeSet<&str> = intents
        .iter()
        .flat_map(|intent| intent.providers.iter().map(String::as_str))
        .collect();
    field(&mut hash, &names.len().to_string());
    for name in names {
        let provider = config
            .providers
            .iter()
            .find(|provider| provider.name == name)
            .ok_or("captured pinning provider is unavailable")?;
        let identity = &provider.identity;
        // Disabled routes cannot execute; their enabled flag is included below,
        // so turning one on invalidates the capture before it can be used.
        if provider.limits.enabled
            && provider.kind != ProviderKind::Noop
            && identity.provider_id.starts_with("legacy:")
        {
            return Err(
                "durable remote pinning requires an explicit provider identity and revisions",
            );
        }
        for value in [
            &provider.name,
            &identity.provider_id,
            &identity.backend,
            &identity.scope,
            &identity.storage_domain,
            &identity.api_profile,
            &identity.strategy,
        ] {
            field(&mut hash, value);
        }
        for value in [identity.credential_revision, identity.endpoint_revision] {
            field(&mut hash, &value.to_string());
        }
        field(&mut hash, identity.secret_ref.as_deref().unwrap_or(""));
        field(
            &mut hash,
            match identity.cleanup {
                super::identity::CleanupMode::Retain => "retain",
                super::identity::CleanupMode::Managed => "managed",
            },
        );
        for value in [
            provider.limits.enabled.to_string(),
            provider.limits.priority.to_string(),
            provider.limits.max_bytes.to_string(),
            provider.limits.max_pins.to_string(),
            identity.retired.to_string(),
            provider
                .requests_per_second
                .map_or(String::new(), |rate| rate.to_string()),
        ] {
            field(&mut hash, &value);
        }
    }
    let digest = hex::encode(hash.finalize());
    Ok(format!("{DURABLE_REVISION_MARKER}{}", &digest[..48]))
}

pub fn config_revision(config: &ValidatedPinningConfig, mode: OptionalPinControlMode) -> String {
    let mut hash = Sha256::new();
    field(&mut hash, "pin-control-v1");
    field(
        &mut hash,
        match mode {
            OptionalPinControlMode::Strict => "strict",
            OptionalPinControlMode::Warn => "warn",
        },
    );
    field(&mut hash, "policies");
    field(&mut hash, &config.policies.len().to_string());
    for policy in &config.policies {
        field(&mut hash, &policy.identity);
    }
    field(&mut hash, "providers");
    field(&mut hash, &config.providers.len().to_string());
    for provider in &config.providers {
        let identity = &provider.identity;
        for item in [
            &provider.name,
            &identity.provider_id,
            &identity.backend,
            &identity.scope,
            &identity.storage_domain,
            &identity.api_profile,
            &identity.strategy,
        ] {
            field(&mut hash, item);
        }
        for value in [identity.credential_revision, identity.endpoint_revision] {
            field(&mut hash, &value.to_string());
        }
        // Only the reference name here; legacy credentials are bound below by
        // a process-local keyed MAC, not a persisted bare secret hash.
        field(&mut hash, identity.secret_ref.as_deref().unwrap_or(""));
        field(
            &mut hash,
            match identity.cleanup {
                super::identity::CleanupMode::Retain => "retain",
                super::identity::CleanupMode::Managed => "managed",
            },
        );
        for value in [
            provider.limits.enabled.to_string(),
            provider.limits.priority.to_string(),
            provider.limits.max_bytes.to_string(),
            provider.limits.max_pins.to_string(),
            identity.retired.to_string(),
            provider
                .requests_per_second
                .map_or(String::new(), |rate| rate.to_string()),
        ] {
            field(&mut hash, &value);
        }
        if provider.kind != ProviderKind::Noop && identity.provider_id.starts_with("legacy:") {
            hash.update(legacy_route_revision(provider));
        }
    }
    hex::encode(hash.finalize())
}

fn legacy_route_revision(provider: &ValidatedProvider) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(&*LEGACY_ROUTE_KEY).expect("fixed-size HMAC key");
    mac.update(b"ipfs-s3/legacy-route/v1");
    mac_field(&mut mac, &provider.name);
    mac_option(&mut mac, provider.endpoint.as_deref());
    mac_option(
        &mut mac,
        provider
            .pinata
            .as_ref()
            .and_then(|pinata| pinata.upload_endpoint.as_deref()),
    );
    mac_option(
        &mut mac,
        provider.token.as_ref().map(|token| token.expose()),
    );
    mac.finalize().into_bytes().into()
}

fn mac_option(mac: &mut Hmac<Sha256>, value: Option<&str>) {
    mac.update(&[u8::from(value.is_some())]);
    if let Some(value) = value {
        mac_field(mac, value);
    }
}

fn mac_field(mac: &mut Hmac<Sha256>, value: &str) {
    mac.update(&(value.len() as u64).to_be_bytes());
    mac.update(value.as_bytes());
}

fn field(hash: &mut Sha256, value: &str) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pinning::config::{LeaseDuration, ProviderMode, ValidatedPinningConfig};
    use crate::pinning::tags::ContentMode;

    // The test runner itself is the second process: its legacy MAC key is
    // independently initialized, rather than reused by a same-process reload.
    fn durable_fixture() -> (ValidatedPinningConfig, ExtensionDecision) {
        use crate::config::{PinningConfig, PolicyConfig, ProviderConfig};
        use crate::pinning::policy::{PinPolicyEvaluator, PublicationContext};
        let raw = PinningConfig {
            providers: vec![
                ProviderConfig {
                    name: "selected".into(),
                    kind: "pinata".into(),
                    token_env: Some("PINNING_TOKEN".into()),
                    endpoint: Some("https://example.test/private-route".into()),
                    enabled: true,
                    priority: 1,
                    max_bytes: 1024,
                    max_pins: 20,
                    api: None,
                    strategy: None,
                    upload_endpoint: None,
                    requests_per_second: None,
                },
                ProviderConfig {
                    name: "unrelated".into(),
                    kind: "pinata".into(),
                    token_env: Some("OTHER_TOKEN".into()),
                    endpoint: Some("https://other.test/private-route".into()),
                    enabled: true,
                    priority: 2,
                    max_bytes: 1024,
                    max_pins: 20,
                    api: None,
                    strategy: None,
                    upload_endpoint: None,
                    requests_per_second: None,
                },
            ],
            policies: vec![PolicyConfig {
                bucket: "bucket".into(),
                prefix: "".into(),
                trigger: "request".into(),
                provider_mode: "one".into(),
                providers: vec!["selected".into()],
                default_duration: "1h".into(),
                max_duration: "24h".into(),
                allow_decompressed: false,
            }],
            ..Default::default()
        };
        let mut config =
            ValidatedPinningConfig::from_raw(&raw, |name| Some(format!("secret-for-{name}")))
                .unwrap();
        config.providers[0].identity.provider_id = "pinata-selected".into();
        config.providers[0].identity.scope = "account:one".into();
        config.providers[0].identity.credential_revision = 2;
        config.providers[0].identity.endpoint_revision = 3;
        let tags = vec![ObjectTag::new("ipfs-s3:pin", "true")];
        let (_, decision) = PinPolicyEvaluator::new(&config)
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "item",
                    tags: &tags,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("principal", "request"),
            )
            .unwrap();
        (config, decision)
    }

    #[test]
    fn durable_revision_child() {
        let Ok(role) = std::env::var("IPFS3_REVISION_TEST_ROLE") else {
            return;
        };
        let (mut config, mut decision) = durable_fixture();
        match role.as_str() {
            "capture" => {
                decision
                    .capture_durable_revision(&config, OptionalPinControlMode::Strict)
                    .unwrap();
                println!("CAPTURED:{}", serde_json::to_string(&decision).unwrap());
            }
            "capture_legacy" => {
                println!("CAPTURED:{}", serde_json::to_string(&decision).unwrap());
            }
            "verify" => {
                decision =
                    serde_json::from_str(&std::env::var("IPFS3_REVISION_TEST_SNAPSHOT").unwrap())
                        .unwrap();
                config.providers[0].identity.credential_revision +=
                    u64::from(std::env::var("IPFS3_REVISION_TEST_CHANGE").unwrap() == "credential");
                config.providers[0].identity.endpoint_revision +=
                    u64::from(std::env::var("IPFS3_REVISION_TEST_CHANGE").unwrap() == "endpoint");
                if std::env::var("IPFS3_REVISION_TEST_CHANGE").unwrap() == "account" {
                    config.providers[0].identity.scope = "account:other".into();
                }
                assert_eq!(
                    decision
                        .verify_revision(&config, OptionalPinControlMode::Strict)
                        .is_ok(),
                    std::env::var("IPFS3_REVISION_TEST_CHANGE").unwrap() == "none"
                );
            }
            "verify_legacy" => {
                decision =
                    serde_json::from_str(&std::env::var("IPFS3_REVISION_TEST_SNAPSHOT").unwrap())
                        .unwrap();
                assert!(
                    decision
                        .verify_revision(&config, OptionalPinControlMode::Strict)
                        .is_err()
                );
            }
            _ => panic!("invalid child role"),
        }
    }

    #[test]
    fn durable_revision_survives_restart_and_fences_route_revisions() {
        use std::process::Command;
        let exe = std::env::current_exe().unwrap();
        let run = |role: &str, snapshot: Option<&str>, change: &str| {
            let mut child = Command::new(&exe);
            child
                .arg("--exact")
                .arg("pinning::decision::tests::durable_revision_child")
                .arg("--nocapture")
                .env("IPFS3_REVISION_TEST_ROLE", role)
                .env("IPFS3_REVISION_TEST_CHANGE", change);
            if let Some(snapshot) = snapshot {
                child.env("IPFS3_REVISION_TEST_SNAPSHOT", snapshot);
            }
            child.output().unwrap()
        };
        let captured = run("capture", None, "none");
        assert!(
            captured.status.success(),
            "capture process failed: {}",
            String::from_utf8_lossy(&captured.stderr)
        );
        let stdout = String::from_utf8(captured.stdout).unwrap();
        let snapshot = stdout
            .split("CAPTURED:")
            .nth(1)
            .unwrap()
            .lines()
            .next()
            .unwrap();
        assert!(!snapshot.contains("secret-for-"));
        assert!(!snapshot.contains("private-route"));
        for change in ["none", "credential", "endpoint", "account"] {
            let result = run("verify", Some(snapshot), change);
            assert!(
                result.status.success(),
                "{change}: {} {}",
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr)
            );
        }
        let legacy = run("capture_legacy", None, "none");
        assert!(legacy.status.success());
        let legacy_stdout = String::from_utf8(legacy.stdout).unwrap();
        let old_snapshot = legacy_stdout
            .split("CAPTURED:")
            .nth(1)
            .unwrap()
            .lines()
            .next()
            .unwrap();
        assert!(!old_snapshot.contains("revision_scheme"));
        assert!(
            run("verify_legacy", Some(old_snapshot), "none")
                .status
                .success()
        );
    }

    #[test]
    fn durable_capture_checks_all_executable_fallbacks_not_unrelated_providers() {
        use crate::pinning::policy::{PinPolicyEvaluator, PublicationContext};
        let (mut config, mut decision) = durable_fixture();
        // A legacy route elsewhere in the config must not poison selected work.
        decision
            .capture_durable_revision(&config, OptionalPinControlMode::Strict)
            .unwrap();
        assert!(
            decision
                .config_revision
                .starts_with(DURABLE_REVISION_MARKER)
        );
        assert!(
            decision
                .verify_revision(&config, OptionalPinControlMode::Strict)
                .is_ok()
        );
        config.providers[1].token = None; // unrelated legacy route's token is not persisted or hashed
        assert!(
            decision
                .verify_revision(&config, OptionalPinControlMode::Strict)
                .is_ok()
        );

        config.policies[0].providers.push("unrelated".into());
        config.policies[0].refresh_identity(0);
        let tags = vec![ObjectTag::new("ipfs-s3:pin", "true")];
        let (_, mut fallback) = PinPolicyEvaluator::new(&config)
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "item",
                    tags: &tags,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("principal", "fallback"),
            )
            .unwrap();
        assert!(
            fallback
                .capture_durable_revision(&config, OptionalPinControlMode::Strict)
                .is_err()
        );

        // A disabled fallback cannot run, but enabling it must fence the old snapshot.
        config.providers[1].limits.enabled = false;
        config.provider_limits.get_mut("unrelated").unwrap().enabled = false;
        let (_, mut disabled) = PinPolicyEvaluator::new(&config)
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "item",
                    tags: &tags,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("principal", "disabled"),
            )
            .unwrap();
        disabled
            .capture_durable_revision(&config, OptionalPinControlMode::Strict)
            .unwrap();
        config.providers[1].limits.enabled = true;
        config.provider_limits.get_mut("unrelated").unwrap().enabled = true;
        assert!(
            disabled
                .verify_revision(&config, OptionalPinControlMode::Strict)
                .is_err()
        );
    }

    #[test]
    fn historical_json_retains_process_local_fence() {
        let (config, decision) = durable_fixture();
        let json = serde_json::to_value(decision).unwrap();
        let historical: ExtensionDecision = serde_json::from_value(json).unwrap();
        assert!(
            !historical
                .config_revision
                .starts_with(DURABLE_REVISION_MARKER)
        );
        historical
            .verify_revision(&config, OptionalPinControlMode::Strict)
            .unwrap();
        let mut changed = config.clone();
        changed.providers[1].endpoint = Some("https://other.test/changed".into());
        assert!(
            historical
                .verify_revision(&changed, OptionalPinControlMode::Strict)
                .is_err()
        );
    }

    #[test]
    fn explicit_registry_identity_survives_normalized_config_reload() {
        use crate::pinning::coordinator::PinningCoordinator;
        use crate::pinning::policy::PublicationContext;
        let raw: crate::config::Config = toml::from_str(
            r#"
            [pinning_identity]
            primary_storage_domain = 'local'
            [[pinning_identity.providers]]
            config_name = 'remote'
            provider_id = 'stable-remote'
            display_name = 'Remote'
            backend = 'pinata'
            scope = 'account:one'
            storage_domain = 'remote'
            credential_revision = 2
            endpoint_revision = 3
            secret_ref = 'env:PINNING_TOKEN'
            api_profile = 'pinata-v3'
            strategy = 'cid'
            [[pinning.providers]]
            name = 'remote'
            kind = 'pinata'
            token_env = 'PINNING_TOKEN'
            priority = 1
            max_bytes = 1024
            max_pins = 20
            [[pinning.policies]]
            bucket = 'bucket'
            trigger = 'request'
            provider_mode = 'one'
            providers = ['remote']
            default_duration = '1h'
            max_duration = '24h'
        "#,
        )
        .unwrap();
        let load = || {
            PinningCoordinator::build(
                ValidatedPinningConfig::from_config(&raw, |_| Some("not-in-snapshot".into()))
                    .unwrap(),
            )
            .unwrap()
        };
        let initial = load();
        let tags = vec![ObjectTag::new("ipfs-s3:pin", "true")];
        let (policy, mut decision) = initial
            .policy()
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "key",
                    tags: &tags,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("owner", "upload"),
            )
            .unwrap();
        decision
            .capture_durable_revision(initial.effective_config(), initial.control_mode())
            .unwrap();
        let json = serde_json::to_string(&decision).unwrap();
        assert!(!json.contains("not-in-snapshot"));
        let restored: ExtensionDecision = serde_json::from_str(&json).unwrap();
        restored.replay_policy(policy.tags).unwrap();
        let restarted = load();
        restored
            .verify_revision(restarted.effective_config(), restarted.control_mode())
            .unwrap();
        let mut changed = restarted.effective_config().clone();
        changed.providers[0].identity.credential_revision += 1;
        assert!(
            restored
                .verify_revision(&changed, restarted.control_mode())
                .is_err()
        );
    }

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
            "0".repeat(64),
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
                "0".repeat(64),
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
            "0".repeat(64),
            DecisionEffect::NoIntent,
            None,
            PinControl::Absent,
            &policy,
        )
        .unwrap();
        let mut serialized = serde_json::to_value(&decision).unwrap();
        serialized.as_object_mut().unwrap().remove("legacy_unknown");
        let restored: ExtensionDecision = serde_json::from_value(serialized).unwrap();
        assert!(!restored.legacy_unknown);
        restored.validate_policy(&policy).unwrap();
    }
}
