use super::{ClusterAdapter, unavailable};
use crate::pinning::cluster::{
    ClusterNetworkPolicy,
    config::{normalized_endpoint, validate_independence},
    provider::{ClusterProvider, ClusterStrategy},
};
use crate::{
    kubo::KuboClient,
    pinning::{
        config::{ProviderKind, ValidatedProvider},
        provider::{PinningProvider, ProviderError},
    },
};
use std::sync::Arc;

/// Construction performs no network I/O. The byte source is the actual main
/// Kubo endpoint, served through the same hardened leaf transport as all peers.
pub fn build_cluster_provider(
    provider: &ValidatedProvider,
    kubo: Option<KuboClient>,
) -> Result<Arc<dyn PinningProvider>, ProviderError> {
    if provider.kind != ProviderKind::Cluster {
        return Err(unavailable());
    }
    let mut options = provider.cluster.clone().ok_or_else(unavailable)?;
    if options.pinset.backend != provider.identity.backend
        || options.pinset.scope != provider.identity.scope
        || options.protocol.api_profile() != provider.identity.api_profile
        || options.strategy.as_str() != provider.identity.strategy
    {
        return Err(unavailable());
    }
    if let Some(kubo) = &kubo {
        validate_independence(
            kubo.base_url(),
            &options.rest_endpoint,
            options.proxy_endpoint.as_deref(),
            ClusterNetworkPolicy::from_allow_private_network(options.allow_private_network),
        )
        .map_err(|_| unavailable())?;
        if let Some(source) = &mut options.source {
            if normalized_endpoint(&source.endpoint).map_err(|_| unavailable())?
                != normalized_endpoint(kubo.base_url()).map_err(|_| unavailable())?
            {
                return Err(unavailable());
            }
            source.endpoint = kubo.base_url().into();
        }
    } else if options.strategy != ClusterStrategy::Cid {
        return Err(unavailable());
    }
    let protocol = options.protocol;
    let strategy = options.strategy;
    let leaf = ClusterProvider::new(options).map_err(super::observation::write_error)?;
    Ok(Arc::new(ClusterAdapter {
        name: provider.name.clone(),
        protocol,
        strategy,
        leaf,
    }))
}
