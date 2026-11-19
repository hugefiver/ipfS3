#[path = "adapter/factory.rs"]
mod factory;
pub use factory::build_cluster_provider;

use super::provider::{ClusterFailureCode, ClusterProvider, ClusterWriteFailure};
use crate::pinning::provider::{
    PinningProvider, ProviderError, ProviderErrorClass, RemotePin, RemotePinStatus, SubmitPin,
};

struct ClusterAdapter {
    name: String,
    leaf: ClusterProvider,
}

#[async_trait::async_trait]
impl PinningProvider for ClusterAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        let receipt = self.leaf.submit(request).await.map_err(write_error)?;
        Ok(RemotePin {
            request_id: receipt.request_id,
            cid: receipt.resource.cid,
            status: RemotePinStatus::Pinned,
            raw_status: "accepted".into(),
            failure_reason: None,
        })
    }

    async fn unpin(&self, request_id: &str) -> Result<(), ProviderError> {
        self.leaf.unpin(request_id).await.map_err(write_error)
    }
}

fn write_error(error: ClusterWriteFailure) -> ProviderError {
    let class = match error.http_status {
        Some(401) => ProviderErrorClass::Authentication,
        Some(403) => ProviderErrorClass::UnknownForbidden,
        Some(429) => ProviderErrorClass::RateLimited,
        _ => match error.code {
            ClusterFailureCode::InvalidConfiguration
            | ClusterFailureCode::InvalidCid
            | ClusterFailureCode::DirectoryRequiresCar => ProviderErrorClass::InvalidInput,
            ClusterFailureCode::SourceFailure => ProviderErrorClass::NotSubmitted,
            ClusterFailureCode::Transport
            | ClusterFailureCode::IdleTimeout
            | ClusterFailureCode::ControlTimeout
            | ClusterFailureCode::Cancelled => ProviderErrorClass::Ambiguous,
            _ => ProviderErrorClass::Protocol,
        },
    };
    ProviderError {
        class,
        message: error.http_status.map_or_else(
            || error.to_string(),
            |status| format!("provider returned HTTP status {status}"),
        ),
        retry_after: None,
    }
}
