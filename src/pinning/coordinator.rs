use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, anyhow};
use chrono::Duration as ChronoDuration;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::kubo::KuboClient;
use crate::pinning::{
    config::{ProviderKind, ProviderLimitMap, ValidatedPinningConfig},
    filebase::build_filebase,
    noop::NoopProvider,
    pinata::build_pinata_with_options,
    policy::PinPolicyEvaluator,
    provider::PinningProvider,
};
use crate::store::Store;
use crate::store::pinning::jobs::POLL_INTERVAL;

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
    policy: PinPolicyEvaluator,
    policy_providers: HashMap<String, Vec<String>>,
    providers: HashMap<String, Arc<dyn PinningProvider>>,
    limits: ProviderLimitMap,
    settings: WorkerSettings,
    provider_runtime: HashMap<String, ProviderRuntime>,
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

impl PinningCoordinator {
    pub fn build(config: ValidatedPinningConfig) -> anyhow::Result<Arc<Self>> {
        Self::build_with_kubo(config, None)
    }

    pub fn build_with_kubo(
        config: ValidatedPinningConfig,
        kubo: Option<KuboClient>,
    ) -> anyhow::Result<Arc<Self>> {
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
        let policy = PinPolicyEvaluator::new(&config);
        let policy_providers = config
            .policies
            .iter()
            .map(|policy| (policy.identity.clone(), policy.providers.clone()))
            .collect();
        let limits = config.provider_limits.clone();
        let mut providers: HashMap<String, Arc<dyn PinningProvider>> = HashMap::new();
        let mut provider_runtime = HashMap::new();

        for provider in config.providers {
            let name = provider.name.clone();
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
            policy,
            policy_providers,
            providers,
            limits,
            settings,
            provider_runtime,
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

    pub fn provider_limits(&self) -> &ProviderLimitMap {
        &self.limits
    }

    pub fn provider(&self, name: &str) -> Option<Arc<dyn PinningProvider>> {
        self.providers.get(name).cloned()
    }

    pub fn settings(&self) -> &WorkerSettings {
        &self.settings
    }

    pub fn provider_runtime(&self, name: &str) -> Option<&ProviderRuntime> {
        self.provider_runtime.get(name)
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
        config::{PinningConfig, PolicyConfig, ProviderConfig},
        pinning::{config::ValidatedPinningConfig, policy::PublicationContext, tags::ObjectTag},
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
        assert!(disabled.provider_limits().is_empty());
        assert!(disabled.provider("noop").is_none());
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
