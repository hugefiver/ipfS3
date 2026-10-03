use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, anyhow};
use chrono::Duration as ChronoDuration;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::OptionalPinControlMode;
use crate::kubo::KuboClient;
use crate::pinning::{
    config::{ProviderKind, ProviderLimitMap, ValidatedPinningConfig},
    filebase::build_filebase,
    identity::ProviderIdentity,
    noop::NoopProvider,
    pinata::build_pinata_with_options,
    policy::PinPolicyEvaluator,
    provider::PinningProvider,
    zip_policy::{ValidatedZipOutputRules, ZipOutputRuleConfig},
};
use crate::store::Store;
use crate::store::pinning::jobs::POLL_INTERVAL;
use crate::zip::extract::ZipExtractionLimits;

pub use crate::pinning::worker::PinningWorkerHandle;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerSettings {
    pub interval: Duration,
    pub poll_interval: Duration,
    pub worker_concurrency: usize,
    pub claim_limit: u64,
    pub lock_for: ChronoDuration,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
    pub max_attempts: u32,
    pub shutdown_grace: Duration,
}

pub struct PinningCoordinator {
    effective_config: ValidatedPinningConfig,
    control_mode: OptionalPinControlMode,
    zip_root_default: bool,
    zip_output_rules: ValidatedZipOutputRules,
    zip_extraction_limits: ZipExtractionLimits,
    policy: PinPolicyEvaluator,
    policy_providers: HashMap<String, Vec<String>>,
    providers: HashMap<String, Arc<dyn PinningProvider>>,
    limits: ProviderLimitMap,
    settings: WorkerSettings,
    provider_runtime: HashMap<String, ProviderRuntime>,
    identities: HashMap<String, ProviderIdentity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderHealth {
    Healthy,
    Degraded,
    Terminal,
}

#[derive(Debug, Clone)]
pub struct ProviderRuntime {
    pub priority: u32,
    pub concurrency: Arc<Semaphore>,
    pub min_request_interval: Duration,
    pub health: Arc<RwLock<ProviderHealth>>,
    pub(crate) next_request_at: Arc<Mutex<Option<Instant>>>,
}

/// Resolve the physical allocation namespace and reject incompatible aliases.
/// This is pure: diagnostics can inspect the same effective pinning config as
/// startup without constructing providers, connecting to Kubo, or touching a DB.
pub fn normalize_validated_config(
    mut config: ValidatedPinningConfig,
) -> anyhow::Result<ValidatedPinningConfig> {
    let aliases: HashMap<_, _> = config
        .providers
        .iter()
        .map(|p| (p.name.clone(), p.identity.allocation_key()))
        .collect();
    for (index, policy) in config.policies.iter_mut().enumerate() {
        policy.providers = policy
            .providers
            .iter()
            .map(|name| aliases[name].clone())
            .collect();
        let mut seen = std::collections::HashSet::new();
        policy.providers.retain(|name| seen.insert(name.clone()));
        policy.refresh_identity(index);
    }
    config
        .providers
        .sort_by(|a, b| a.identity.provider_id.cmp(&b.identity.provider_id));
    config.provider_limits.clear();
    let mut domain_routes: HashMap<String, ProviderIdentity> = HashMap::new();
    for provider in &mut config.providers {
        provider.name = provider.identity.allocation_key();
        if provider.name.len() > 255 {
            return Err(anyhow!(
                "pinning allocation identity exceeds the database key limit"
            ));
        }
        provider.limits.enabled &= !provider.identity.retired;
        if let Some(existing) = domain_routes.get(&provider.name) {
            let mut alias = provider.identity.clone();
            alias.provider_id.clone_from(&existing.provider_id);
            alias.display_name.clone_from(&existing.display_name);
            if &alias != existing {
                return Err(anyhow!(
                    "aliases in the same pinning domain must agree on protocol, strategy, revisions and cleanup"
                ));
            }
        } else {
            domain_routes.insert(provider.name.clone(), provider.identity.clone());
        }
        if let Some(existing) = config.provider_limits.get(&provider.name) {
            if existing != &provider.limits {
                return Err(anyhow!(
                    "aliases in the same pinning domain must have identical limits"
                ));
            }
        } else {
            config
                .provider_limits
                .insert(provider.name.clone(), provider.limits.clone());
        }
    }
    Ok(config)
}

impl PinningCoordinator {
    pub fn build(config: ValidatedPinningConfig) -> anyhow::Result<Arc<Self>> {
        Self::build_with_kubo(config, None)
    }

    pub fn build_with_kubo(
        config: ValidatedPinningConfig,
        kubo: Option<KuboClient>,
    ) -> anyhow::Result<Arc<Self>> {
        Self::build_with_kubo_and_mode(config, kubo, OptionalPinControlMode::Strict)
    }

    pub fn build_with_kubo_and_mode(
        config: ValidatedPinningConfig,
        kubo: Option<KuboClient>,
        mode: OptionalPinControlMode,
    ) -> anyhow::Result<Arc<Self>> {
        Self::build_with_kubo_mode_and_zip_root(config, kubo, mode, true)
    }

    pub fn build_with_kubo_mode_and_zip_root(
        config: ValidatedPinningConfig,
        kubo: Option<KuboClient>,
        mode: OptionalPinControlMode,
        zip_root_default: bool,
    ) -> anyhow::Result<Arc<Self>> {
        Self::build_with_kubo_mode_and_zip_root_and_rules(config, kubo, mode, zip_root_default, &[])
    }

    pub fn build_with_kubo_mode_and_zip_root_and_rules(
        config: ValidatedPinningConfig,
        kubo: Option<KuboClient>,
        mode: OptionalPinControlMode,
        zip_root_default: bool,
        raw_rules: &[ZipOutputRuleConfig],
    ) -> anyhow::Result<Arc<Self>> {
        Self::build_with_zip_limits(
            config,
            kubo,
            mode,
            zip_root_default,
            raw_rules,
            ZipExtractionLimits::default(),
        )
    }

    pub fn build_with_zip_limits(
        config: ValidatedPinningConfig,
        kubo: Option<KuboClient>,
        mode: OptionalPinControlMode,
        zip_root_default: bool,
        raw_rules: &[ZipOutputRuleConfig],
        zip_extraction_limits: ZipExtractionLimits,
    ) -> anyhow::Result<Arc<Self>> {
        // Resolve aliases before policy evaluation: all later target, quota and
        // job keys use the same physical resource namespace.
        let config = normalize_validated_config(config)?;
        let zip_output_rules = ValidatedZipOutputRules::compile(raw_rules, &config)?;
        let claim_limit = config
            .worker_concurrency
            .checked_mul(2)
            .and_then(|limit| u64::try_from(limit).ok())
            .ok_or_else(|| anyhow!("worker claim limit overflow"))?;
        let settings = WorkerSettings {
            interval: Duration::from_secs(config.worker_interval.as_seconds()),
            poll_interval: POLL_INTERVAL,
            worker_concurrency: config.worker_concurrency,
            claim_limit,
            lock_for: ChronoDuration::seconds(30),
            base_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(300),
            max_attempts: 8,
            shutdown_grace: Duration::from_secs(30),
        };
        let policy = PinPolicyEvaluator::with_mode(&config, mode);
        let policy_providers = config
            .policies
            .iter()
            .map(|policy| (policy.identity.clone(), policy.providers.clone()))
            .collect();
        let limits = config.provider_limits.clone();
        // Preserve the same alias-resolved snapshot used by the policy and worker,
        // before provider construction consumes credentials and route options.
        let effective_config = config.clone();
        let mut providers: HashMap<String, Arc<dyn PinningProvider>> = HashMap::new();
        let mut provider_runtime = HashMap::new();
        let mut identities = HashMap::new();

        for provider in config.providers {
            let name = provider.name.clone();
            if identities.contains_key(&name) {
                continue;
            }
            identities.insert(name.clone(), provider.identity.clone());
            let kind = provider.kind;
            let min_request_interval = match kind {
                ProviderKind::Noop => Duration::ZERO,
                ProviderKind::Pinata | ProviderKind::Filebase => Duration::from_secs_f64(
                    1.0 / f64::from(provider.requests_per_second.unwrap_or(1)),
                ),
            };
            provider_runtime.insert(
                name.clone(),
                ProviderRuntime {
                    priority: provider.limits.priority,
                    concurrency: Arc::new(Semaphore::new(settings.worker_concurrency)),
                    min_request_interval,
                    health: Arc::new(RwLock::new(ProviderHealth::Healthy)),
                    next_request_at: Arc::new(Mutex::new(None)),
                },
            );
            let implementation: Arc<dyn PinningProvider> = match provider.kind {
                ProviderKind::Pinata => Arc::new(build_pinata_with_options(
                    provider.name,
                    provider
                        .token
                        .context("validated Pinata provider is missing its token")?,
                    provider.endpoint,
                    provider
                        .pinata
                        .context("validated Pinata provider is missing its options")?,
                    kubo.clone(),
                )),
                ProviderKind::Filebase => Arc::new(build_filebase(
                    provider.name,
                    provider
                        .token
                        .context("validated Filebase provider is missing its token")?,
                    provider.endpoint,
                )),
                ProviderKind::Noop => Arc::new(NoopProvider::new(provider.name)),
            };
            if providers.insert(name.clone(), implementation).is_some() {
                return Err(anyhow!("duplicate validated provider `{name}`"));
            }
        }

        Ok(Arc::new(Self {
            effective_config,
            control_mode: mode,
            zip_root_default,
            zip_output_rules,
            zip_extraction_limits,
            policy,
            policy_providers,
            providers,
            limits,
            settings,
            provider_runtime,
            identities,
        }))
    }

    pub fn disabled_for_test() -> Arc<Self> {
        let raw = crate::config::PinningConfig::default();
        let validated = ValidatedPinningConfig::from_raw(&raw, |_| None)
            .expect("default pinning configuration must remain valid");
        Self::build(validated).expect("disabled pinning coordinator must build")
    }

    pub fn policy(&self) -> &PinPolicyEvaluator {
        &self.policy
    }

    /// Internal-only: contains resolved provider tokens; never log or expose over HTTP.
    pub fn effective_config(&self) -> &ValidatedPinningConfig {
        &self.effective_config
    }

    pub fn control_mode(&self) -> OptionalPinControlMode {
        self.control_mode
    }

    pub fn zip_root_default(&self) -> bool {
        self.zip_root_default
    }

    pub fn zip_output_rules(&self) -> &ValidatedZipOutputRules {
        &self.zip_output_rules
    }

    pub fn zip_extraction_limits(&self) -> ZipExtractionLimits {
        self.zip_extraction_limits
    }

    pub fn provider_limits(&self) -> &ProviderLimitMap {
        &self.limits
    }

    pub fn provider(&self, name: &str) -> Option<Arc<dyn PinningProvider>> {
        self.providers.get(self.route_key(name)?).cloned()
    }

    pub fn provider_identity(&self, key: &str) -> Option<&ProviderIdentity> {
        self.identities.get(self.route_key(key)?)
    }

    fn route_key<'a>(&'a self, key: &'a str) -> Option<&'a str> {
        if self.identities.contains_key(key) {
            return Some(key);
        }
        self.identities
            .iter()
            .filter(|(_, identity)| identity.owns_allocation_domain(key))
            .min_by_key(|(_, identity)| &identity.provider_id)
            .map(|(key, _)| key.as_str())
    }

    /// Register the current allocation routes before accepting publications.
    /// Existing resource snapshots are never overwritten by this operation.
    pub async fn register_identities(&self, store: &Store) -> crate::error::AppResult<()> {
        for (key, identity) in &self.identities {
            crate::store::pinning::ledger::register_route(store.db(), key, identity).await?;
        }
        Ok(())
    }

    pub fn settings(&self) -> &WorkerSettings {
        &self.settings
    }

    pub fn provider_runtime(&self, name: &str) -> Option<&ProviderRuntime> {
        self.provider_runtime.get(self.route_key(name)?)
    }

    /// Returns the captured policy order anchored at the failed provider, excluding unhealthy
    /// candidates. The anchor is retained even when it is terminal so the store can advance only
    /// to lower-preference providers and never automatically fail back.
    pub async fn ordered_failover_providers(
        &self,
        policy_id: &str,
        failed_provider: &str,
    ) -> Option<Vec<String>> {
        let configured = self.policy_providers.get(policy_id)?;
        let failed_index = configured
            .iter()
            .position(|provider| provider == failed_provider)?;
        let mut eligible = vec![failed_provider.to_owned()];
        for provider in &configured[failed_index + 1..] {
            if !self
                .limits
                .get(provider)
                .is_some_and(|limits| limits.enabled)
            {
                continue;
            }
            let Some(runtime) = self.provider_runtime.get(provider) else {
                continue;
            };
            if *runtime.health.read().await == ProviderHealth::Healthy {
                eligible.push(provider.clone());
            }
        }
        Some(eligible)
    }

    pub fn start(self: &Arc<Self>, store: Store, parent: CancellationToken) -> PinningWorkerHandle {
        crate::pinning::worker::start(self.clone(), store, parent)
    }

    #[cfg(test)]
    pub(crate) fn replace_provider_for_test(
        coordinator: &mut Arc<Self>,
        name: &str,
        provider: Arc<dyn PinningProvider>,
    ) {
        Arc::get_mut(coordinator)
            .expect("test coordinator must not yet be shared")
            .providers
            .insert(name.to_owned(), provider);
    }

    #[cfg(test)]
    pub(crate) fn configure_worker_for_test(
        coordinator: &mut Arc<Self>,
        configure: impl FnOnce(&mut WorkerSettings),
    ) {
        configure(
            &mut Arc::get_mut(coordinator)
                .expect("test coordinator must not yet be shared")
                .settings,
        );
    }

    #[cfg(test)]
    pub(crate) fn configure_provider_runtime_for_test(
        coordinator: &mut Arc<Self>,
        name: &str,
        concurrency: usize,
        min_request_interval: Duration,
    ) {
        let runtime = Arc::get_mut(coordinator)
            .expect("test coordinator must not yet be shared")
            .provider_runtime
            .get_mut(name)
            .expect("test provider runtime must exist");
        runtime.concurrency = Arc::new(Semaphore::new(concurrency));
        runtime.min_request_interval = min_request_interval;
        runtime.next_request_at = Arc::new(Mutex::new(None));
    }

    #[cfg(test)]
    pub(crate) fn configure_policy_providers_for_test(
        coordinator: &mut Arc<Self>,
        policy_id: &str,
        providers: &[&str],
    ) {
        Arc::get_mut(coordinator)
            .expect("test coordinator must not yet be shared")
            .policy_providers
            .insert(
                policy_id.to_owned(),
                providers
                    .iter()
                    .map(|provider| (*provider).to_owned())
                    .collect(),
            );
    }
}

#[cfg(test)]
mod tests {
    use super::PinningCoordinator;
    use crate::{
        config::{Config, OptionalPinControlMode, PinningConfig, PolicyConfig, ProviderConfig},
        pinning::{
            config::ValidatedPinningConfig,
            decision::{DecisionEffect, DecisionOrigin},
            policy::PublicationContext,
            tags::ObjectTag,
            zip_policy::{
                ZipOutputRuleConfig, ZipPlanWarning, ZipPublishedOutput, ZipRuleEffect, ZipTargets,
            },
        },
    };

    fn validated_noop_fixture() -> ValidatedPinningConfig {
        ValidatedPinningConfig::from_raw(
            &PinningConfig {
                worker_interval: "7s".to_owned(),
                worker_concurrency: 3,
                providers: vec![ProviderConfig {
                    name: "noop".to_owned(),
                    kind: "noop".to_owned(),
                    token_env: None,
                    endpoint: None,
                    api: None,
                    strategy: None,
                    upload_endpoint: None,
                    enabled: true,
                    priority: 1,
                    max_bytes: 1_000,
                    max_pins: 10,
                    requests_per_second: None,
                }],
                policies: vec![PolicyConfig {
                    bucket: "bucket".to_owned(),
                    prefix: String::new(),
                    trigger: "always".to_owned(),
                    provider_mode: "one".to_owned(),
                    providers: vec!["noop".to_owned()],
                    default_duration: "1h".to_owned(),
                    max_duration: "2h".to_owned(),
                    allow_decompressed: false,
                }],
            },
            |_| None,
        )
        .unwrap()
    }

    #[test]
    fn foundation_exposes_policy_limits_registry_settings_and_disabled_fixture() {
        let coordinator = PinningCoordinator::build(validated_noop_fixture()).unwrap();
        assert_eq!(coordinator.control_mode(), OptionalPinControlMode::Strict);
        assert_eq!(coordinator.effective_config().policies.len(), 1);
        let no_tags: Vec<ObjectTag> = Vec::new();
        assert_eq!(
            coordinator
                .policy()
                .evaluate_publication(PublicationContext {
                    bucket: "bucket",
                    key: "key",
                    tags: &no_tags,
                    is_decompress_zip: false,
                })
                .unwrap()
                .leases
                .len(),
            1
        );
        assert_eq!(coordinator.provider_limits()["noop"].max_pins, 10);
        assert_eq!(coordinator.provider("noop").unwrap().name(), "noop");
        let runtime = coordinator.provider_runtime("noop").unwrap();
        assert_eq!(runtime.priority, 1);
        assert_eq!(runtime.min_request_interval, std::time::Duration::ZERO);
        assert_eq!(runtime.concurrency.available_permits(), 3);
        assert_eq!(
            *runtime.health.blocking_read(),
            super::ProviderHealth::Healthy
        );
        assert_eq!(
            coordinator.settings().interval,
            std::time::Duration::from_secs(7)
        );
        assert_eq!(coordinator.settings().claim_limit, 6);
        assert_eq!(coordinator.settings().worker_concurrency, 3);
        assert_eq!(
            coordinator.settings().lock_for,
            chrono::Duration::seconds(30)
        );
        assert_eq!(
            coordinator.settings().base_backoff,
            std::time::Duration::from_secs(1)
        );
        assert_eq!(
            coordinator.settings().max_backoff,
            std::time::Duration::from_secs(300)
        );
        assert_eq!(coordinator.settings().max_attempts, 8);
        assert_eq!(
            coordinator.settings().shutdown_grace,
            std::time::Duration::from_secs(30)
        );
        assert_eq!(
            coordinator.settings().poll_interval,
            std::time::Duration::from_secs(5)
        );

        let disabled = PinningCoordinator::disabled_for_test();
        assert_eq!(disabled.control_mode(), OptionalPinControlMode::Strict);
        assert!(disabled.provider_limits().is_empty());
        assert!(disabled.provider("noop").is_none());
        assert_eq!(
            disabled.zip_output_rules().revision(),
            coordinator.zip_output_rules().revision()
        );

        let with_kubo =
            PinningCoordinator::build_with_kubo(validated_noop_fixture(), None).unwrap();
        assert_eq!(with_kubo.control_mode(), OptionalPinControlMode::Strict);
        assert_eq!(
            with_kubo.zip_output_rules().revision(),
            coordinator.zip_output_rules().revision()
        );
    }

    #[test]
    fn warn_mode_uses_resolved_aliases_for_policy_and_effective_config() {
        let raw: Config = toml::from_str(
            r#"
                [pinning_control]
                unavailable = "warn"
                [pinning]
                [[pinning.providers]]
                name = "first"
                kind = "noop"
                priority = 1
                max_bytes = 100
                max_pins = 10
                [[pinning.providers]]
                name = "second"
                kind = "noop"
                priority = 1
                max_bytes = 100
                max_pins = 10
                [[pinning.policies]]
                bucket = "bucket"
                trigger = "request"
                provider_mode = "one"
                providers = ["first", "second"]
                default_duration = "1h"
                max_duration = "2h"
                [pinning_identity]
                primary_storage_domain = "kubo:primary"
                [[pinning_identity.providers]]
                config_name = "first"
                provider_id = "noop-first"
                display_name = "First"
                backend = "noop"
                scope = "account:shared"
                storage_domain = "noop:shared"
                credential_revision = 1
                endpoint_revision = 1
                api_profile = "noop"
                strategy = "cid"
                [[pinning_identity.providers]]
                config_name = "second"
                provider_id = "noop-second"
                display_name = "Second"
                backend = "noop"
                scope = "account:shared"
                storage_domain = "noop:shared"
                credential_revision = 1
                endpoint_revision = 1
                api_profile = "noop"
                strategy = "cid"
            "#,
        )
        .unwrap();
        let validated = ValidatedPinningConfig::from_config(&raw, |_| None).unwrap();
        let coordinator = PinningCoordinator::build_with_kubo_and_mode(
            validated,
            None,
            raw.pinning_control.unavailable,
        )
        .unwrap();
        let effective = coordinator.effective_config();
        let key = effective.providers[0].identity.allocation_key();
        assert_ne!(key, "first");
        assert_eq!(effective.providers.len(), 2);
        assert!(
            effective
                .providers
                .iter()
                .all(|provider| provider.name == key)
        );
        assert_eq!(effective.policies[0].providers, vec![key.clone()]);
        assert_eq!(effective.provider_limits.len(), 1);
        assert_eq!(coordinator.provider_limits(), &effective.provider_limits);
        assert_eq!(coordinator.provider(&key).unwrap().name(), key);
        assert_eq!(coordinator.control_mode(), OptionalPinControlMode::Warn);
        assert_eq!(
            coordinator.zip_output_rules().revision(),
            PinningCoordinator::disabled_for_test()
                .zip_output_rules()
                .revision()
        );

        let tags = [ObjectTag::new("ipfs-s3:pin", "true")];
        let (policy, decision) = coordinator
            .policy()
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
        assert_eq!(policy.leases[0].providers, vec![key]);
        assert_eq!(decision.effect, DecisionEffect::Accepted);
        assert!(
            decision
                .verify_revision(effective, coordinator.control_mode())
                .is_ok()
        );
        assert!(
            decision
                .verify_revision(effective, OptionalPinControlMode::Strict)
                .is_err()
        );
    }

    #[test]
    fn zip_rules_compile_against_normalized_policy_and_reject_invalid_rules() {
        let validated = validated_noop_fixture();
        let normalized = super::normalize_validated_config(validated.clone()).unwrap();
        let rule = ZipOutputRuleConfig {
            name: "allow".to_owned(),
            priority: 10,
            bucket: "bucket".to_owned(),
            prefix: String::new(),
            effect: ZipRuleEffect::Allow,
            policy_id: Some(normalized.policies[0].identity.clone()),
        };
        let coordinator = PinningCoordinator::build_with_kubo_mode_and_zip_root_and_rules(
            validated.clone(),
            None,
            OptionalPinControlMode::Strict,
            false,
            std::slice::from_ref(&rule),
        )
        .unwrap();
        assert!(!coordinator.zip_root_default());
        assert_ne!(
            coordinator.zip_output_rules().revision(),
            PinningCoordinator::build(validated.clone())
                .unwrap()
                .zip_output_rules()
                .revision()
        );
        let plan = coordinator
            .zip_output_rules()
            .plan(
                ZipTargets {
                    source: false,
                    extracted: true,
                },
                None,
                &[ZipPublishedOutput {
                    bucket: "bucket".to_owned(),
                    key: "file".to_owned(),
                    version_id: "v1".to_owned(),
                    cid: "cid".to_owned(),
                }],
            )
            .unwrap();
        assert_eq!(plan.outputs[0].rule_name.as_deref(), Some("allow"));
        assert_eq!(plan.outputs[0].warning, None);

        let mut invalid = rule;
        invalid.policy_id = Some("unresolved-policy".to_owned());
        assert!(
            PinningCoordinator::build_with_kubo_mode_and_zip_root_and_rules(
                validated,
                None,
                OptionalPinControlMode::Strict,
                true,
                &[invalid],
            )
            .is_err()
        );
        assert_eq!(
            PinningCoordinator::disabled_for_test()
                .zip_output_rules()
                .plan(
                    ZipTargets {
                        source: false,
                        extracted: true
                    },
                    None,
                    &[ZipPublishedOutput {
                        bucket: "bucket".to_owned(),
                        key: "file".to_owned(),
                        version_id: "v1".to_owned(),
                        cid: "cid".to_owned()
                    }],
                )
                .unwrap()
                .outputs[0]
                .warning,
            Some(ZipPlanWarning::NoMatchingRule)
        );
    }

    #[test]
    fn external_runtime_defaults_to_one_rps_and_honors_positive_override() {
        let validated = ValidatedPinningConfig::from_raw(
            &PinningConfig {
                worker_interval: "1s".to_owned(),
                worker_concurrency: 4,
                providers: vec![
                    ProviderConfig {
                        name: "pinata".to_owned(),
                        kind: "pinata".to_owned(),
                        token_env: Some("PINATA_TOKEN".to_owned()),
                        endpoint: None,
                        api: None,
                        strategy: None,
                        upload_endpoint: None,
                        enabled: true,
                        priority: 2,
                        max_bytes: 1_000,
                        max_pins: 10,
                        requests_per_second: None,
                    },
                    ProviderConfig {
                        name: "filebase".to_owned(),
                        kind: "filebase".to_owned(),
                        token_env: Some("FILEBASE_TOKEN".to_owned()),
                        endpoint: None,
                        api: None,
                        strategy: None,
                        upload_endpoint: None,
                        enabled: true,
                        priority: 3,
                        max_bytes: 1_000,
                        max_pins: 10,
                        requests_per_second: Some(4),
                    },
                ],
                policies: Vec::new(),
            },
            |_| Some("test-token".to_owned()),
        )
        .unwrap();
        let coordinator = PinningCoordinator::build(validated).unwrap();
        assert_eq!(
            coordinator
                .provider_runtime("pinata")
                .unwrap()
                .min_request_interval,
            std::time::Duration::from_secs(1)
        );
        assert_eq!(
            coordinator
                .provider_runtime("filebase")
                .unwrap()
                .min_request_interval,
            std::time::Duration::from_millis(250)
        );
    }

    #[tokio::test]
    async fn failover_order_never_moves_backward_and_skips_disabled_or_degraded_candidates() {
        let provider = |name: &str, priority: u32, enabled: bool| ProviderConfig {
            name: name.to_owned(),
            kind: "noop".to_owned(),
            token_env: None,
            endpoint: None,
            api: None,
            strategy: None,
            upload_endpoint: None,
            enabled,
            priority,
            max_bytes: 1_000,
            max_pins: 10,
            requests_per_second: None,
        };
        let validated = ValidatedPinningConfig::from_raw(
            &PinningConfig {
                worker_interval: "1s".to_owned(),
                worker_concurrency: 2,
                providers: vec![
                    provider("primary", 1, true),
                    provider("degraded", 2, true),
                    provider("disabled", 3, false),
                    provider("fallback", 4, true),
                ],
                policies: vec![PolicyConfig {
                    bucket: "bucket".to_owned(),
                    prefix: String::new(),
                    trigger: "always".to_owned(),
                    provider_mode: "one".to_owned(),
                    providers: vec![
                        "fallback".to_owned(),
                        "disabled".to_owned(),
                        "degraded".to_owned(),
                        "primary".to_owned(),
                    ],
                    default_duration: "1h".to_owned(),
                    max_duration: "2h".to_owned(),
                    allow_decompressed: false,
                }],
            },
            |_| None,
        )
        .unwrap();
        let policy_id = validated.policies[0].identity.clone();
        let coordinator = PinningCoordinator::build(validated).unwrap();
        *coordinator
            .provider_runtime("degraded")
            .unwrap()
            .health
            .write()
            .await = super::ProviderHealth::Degraded;

        assert_eq!(
            coordinator
                .ordered_failover_providers(&policy_id, "primary")
                .await
                .unwrap(),
            vec!["primary".to_owned(), "fallback".to_owned()]
        );
        assert_eq!(
            coordinator
                .ordered_failover_providers(&policy_id, "degraded")
                .await
                .unwrap(),
            vec!["degraded".to_owned(), "fallback".to_owned()]
        );
    }
}
