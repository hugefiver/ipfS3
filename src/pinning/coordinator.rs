use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, anyhow};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::config::{DecompressZipConfig, OptionalPinControlMode};
use crate::kubo::KuboClient;
use crate::pinning::{
    config::{ProviderKind, ProviderLimitMap, ValidatedPinningConfig, ValidatedProvider},
    filebase::build_filebase,
    identity::ProviderIdentity,
    noop::NoopProvider,
    pinata::build_pinata_with_options,
    policy::{LeaseIntent, PinPolicyEvaluator},
    provider::{PinningProvider, canonical_resource_cid},
    zip_policy::{ValidatedZipOutputRules, ZipOutputRuleConfig},
};
use crate::store::Store;
use crate::zip::extract::ZipExtractionLimits;

pub use crate::pinning::cluster::build_cluster_provider;
pub use crate::pinning::worker::PinningWorkerHandle;

pub(crate) fn build_rpc_provider(
    provider: &ValidatedProvider,
    kubo: Option<KuboClient>,
) -> anyhow::Result<crate::pinning::ipfs_rpc::IpfsRpcProvider> {
    let options = provider
        .rpc
        .as_ref()
        .context("validated RPC provider is missing its options")?;
    crate::pinning::ipfs_rpc::IpfsRpcProvider::from_config(
        provider.name.clone(),
        provider
            .endpoint
            .clone()
            .unwrap_or_else(|| "https://rpc.filebase.io".into()),
        kubo,
        options,
    )
    .map_err(Into::into)
}

pub struct PinningCoordinator {
    decompress_zip_config: DecompressZipConfig,
    zip_output_rules: ValidatedZipOutputRules,
    policy: PinPolicyEvaluator,
    providers: HashMap<String, Arc<dyn PinningProvider>>,
    request_intervals: HashMap<String, Duration>,
    work_wake: watch::Sender<()>,
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
    let mut cluster_routes: HashMap<
        _,
        Option<crate::pinning::cluster::provider::ClusterProviderOptions>,
    > = HashMap::new();
    for provider in &mut config.providers {
        provider.name = provider.identity.allocation_key();
        if provider.name.len() > 255 {
            return Err(anyhow!(
                "pinning allocation identity exceeds the database key limit"
            ));
        }
        provider.limits.enabled &= !provider.identity.retired;
        if let Some(existing) = cluster_routes.get(&provider.name) {
            let mut alias = provider.cluster.clone();
            if let (Some(existing), Some(alias)) = (existing, &mut alias) {
                // Administrator-declared aliases may expose one pinset through
                // different addresses. They still have one selected route, not
                // fallback or extra replicas. All other options/revisions agree.
                alias.rest_endpoint.clone_from(&existing.rest_endpoint);
                alias.proxy_endpoint.clone_from(&existing.proxy_endpoint);
            }
            if existing != &alias {
                return Err(anyhow!(
                    "aliases in the same Cluster pinset must agree on transport and credentials"
                ));
            }
        } else {
            cluster_routes.insert(provider.name.clone(), provider.cluster.clone());
        }
        if let Some(existing) = domain_routes.get(&provider.name) {
            let mut alias = provider.identity.clone();
            alias.provider_id.clone_from(&existing.provider_id);
            alias.display_name.clone_from(&existing.display_name);
            if &alias != existing {
                return Err(anyhow!(
                    "aliases in the same pinning domain must agree on protocol, strategy and revisions"
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
        let zip = DecompressZipConfig {
            unixfs_directory_root: zip_root_default,
            limits: zip_extraction_limits,
            auto_pin_outputs: Some(!raw_rules.is_empty()),
            pin_output_rules: raw_rules.to_vec(),
            pin_output_rules_provided: true,
        };
        Self::build_with_decompress_config(config, kubo, mode, &zip)
    }

    pub fn build_with_decompress_config(
        config: ValidatedPinningConfig,
        kubo: Option<KuboClient>,
        mode: OptionalPinControlMode,
        zip: &DecompressZipConfig,
    ) -> anyhow::Result<Arc<Self>> {
        let config = normalize_validated_config(config)?;
        let zip_output_rules = ValidatedZipOutputRules::from_config(zip, &config)?;
        let mut providers: HashMap<String, Arc<dyn PinningProvider>> = HashMap::new();
        let mut request_intervals = HashMap::new();

        for provider in &config.providers {
            let name = provider.name.clone();
            if providers.contains_key(&name) {
                continue;
            }
            let kind = provider.kind;
            let min_request_interval = match kind {
                ProviderKind::Noop => Duration::ZERO,
                ProviderKind::Pinata
                | ProviderKind::Filebase
                | ProviderKind::IpfsRpc
                | ProviderKind::Cluster => Duration::from_secs_f64(
                    1.0 / f64::from(provider.requests_per_second.unwrap_or(1)),
                ),
            };
            request_intervals.insert(name.clone(), min_request_interval);
            let implementation: Arc<dyn PinningProvider> = match provider.kind {
                ProviderKind::Cluster => build_cluster_provider(provider, kubo.clone())?,
                ProviderKind::IpfsRpc => Arc::new(build_rpc_provider(provider, kubo.clone())?),
                ProviderKind::Filebase if provider.rpc.is_some() => {
                    Arc::new(build_rpc_provider(provider, kubo.clone())?)
                }
                ProviderKind::Pinata => Arc::new(build_pinata_with_options(
                    name.clone(),
                    provider
                        .token
                        .clone()
                        .context("validated Pinata provider is missing its token")?,
                    provider.endpoint.clone(),
                    provider
                        .pinata
                        .clone()
                        .context("validated Pinata provider is missing its options")?,
                    kubo.clone(),
                )),
                ProviderKind::Filebase => Arc::new(build_filebase(
                    name.clone(),
                    provider
                        .token
                        .clone()
                        .context("validated Filebase provider is missing its token")?,
                    provider.endpoint.clone(),
                )),
                ProviderKind::Noop => Arc::new(NoopProvider::new(name.clone())),
            };
            providers.insert(name, implementation);
        }

        Ok(Arc::new(Self {
            decompress_zip_config: zip.clone(),
            zip_output_rules,
            policy: PinPolicyEvaluator {
                config,
                optional_control: mode,
            },
            providers,
            request_intervals,
            work_wake: watch::channel(()).0,
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
        &self.policy.config
    }

    pub fn control_mode(&self) -> OptionalPinControlMode {
        self.policy.optional_control
    }

    pub fn zip_root_default(&self) -> bool {
        self.decompress_zip_config.unixfs_directory_root
    }

    pub fn decompress_zip_config(&self) -> &DecompressZipConfig {
        &self.decompress_zip_config
    }

    pub fn zip_output_rules(&self) -> &ValidatedZipOutputRules {
        &self.zip_output_rules
    }

    pub fn zip_extraction_limits(&self) -> ZipExtractionLimits {
        self.decompress_zip_config.limits
    }

    pub fn provider_limits(&self) -> &ProviderLimitMap {
        &self.effective_config().provider_limits
    }

    pub fn provider(&self, name: &str) -> Option<Arc<dyn PinningProvider>> {
        self.providers.get(name).cloned()
    }

    pub(crate) fn request_interval(&self, name: &str) -> Duration {
        self.request_intervals[name]
    }

    pub(super) fn subscribe_pin_work(&self) -> watch::Receiver<()> {
        self.work_wake.subscribe()
    }

    pub(crate) fn notify_pin_work(&self) {
        self.work_wake.send_replace(());
    }

    pub(crate) fn notify_put_committed(&self, source_cid: &str, intents: &[LeaseIntent]) {
        if intents.is_empty() {
            return;
        }
        self.notify_pin_work();
        if !tracing::enabled!(tracing::Level::DEBUG) {
            return;
        }
        let cid = canonical_resource_cid(source_cid).ok();
        for (index, intent) in intents.iter().enumerate() {
            for provider in &intent.providers {
                if !self.provider_limits()[provider].enabled
                    || intents[..index]
                        .iter()
                        .any(|earlier| earlier.providers.contains(provider))
                {
                    continue;
                }
                tracing::debug!(name: "pin_put_committed", cid = cid.as_deref(), source_cid, %provider, "PUT committed with pin intent for candidate provider");
            }
        }
    }

    pub fn start(self: &Arc<Self>, store: Store, parent: CancellationToken) -> PinningWorkerHandle {
        self.start_with_metrics(store, parent, None)
    }

    pub fn start_with_metrics(
        self: &Arc<Self>,
        store: Store,
        parent: CancellationToken,
        metrics: Option<Arc<crate::metrics::Metrics>>,
    ) -> PinningWorkerHandle {
        crate::pinning::worker::start(self.clone(), store, parent, metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::{Duration, PinningCoordinator};
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
                    cluster: None,
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
    fn coordinator_exposes_configured_policy_limits_and_providers() {
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
        let disabled = PinningCoordinator::disabled_for_test();
        assert_eq!(disabled.control_mode(), OptionalPinControlMode::Strict);
        assert!(disabled.provider_limits().is_empty());
        assert!(disabled.provider("noop").is_none());

        let with_kubo =
            PinningCoordinator::build_with_kubo(validated_noop_fixture(), None).unwrap();
        assert_eq!(with_kubo.control_mode(), OptionalPinControlMode::Strict);
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
        let resolved = coordinator
            .policy()
            .resolve_captured_publication(
                PublicationContext {
                    bucket: "bucket",
                    key: "object",
                    tags: &tags,
                    is_decompress_zip: false,
                },
                &decision,
            )
            .unwrap();
        assert_eq!(resolved.leases, policy.leases);
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
    fn coordinator_keeps_omitted_and_explicit_empty_zip_rules_distinct() {
        let config = validated_noop_fixture();
        let mut zip = crate::config::DecompressZipConfig {
            auto_pin_outputs: Some(true),
            ..Default::default()
        };
        for explicit_empty in [false, true] {
            zip.pin_output_rules_provided = explicit_empty;
            let coordinator = PinningCoordinator::build_with_decompress_config(
                config.clone(),
                None,
                OptionalPinControlMode::Strict,
                &zip,
            )
            .unwrap();
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
            assert_eq!(plan.outputs[0].intents.len(), usize::from(!explicit_empty));
        }
        let default = PinningCoordinator::build(config).unwrap();
        assert_eq!(
            default.decompress_zip_config().auto_pin_outputs,
            Some(false)
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
                        cluster: None,
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
                        cluster: None,
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
            coordinator.request_interval("pinata"),
            Duration::from_secs(1)
        );
        assert_eq!(
            coordinator.request_interval("filebase"),
            Duration::from_millis(250)
        );
    }
}
