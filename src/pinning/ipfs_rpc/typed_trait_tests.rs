use super::tests::{CID, OTHER, provider, query, source_file, submit};
use super::{RpcProfile, RpcStrategy};
use crate::pinning::{
    identity::{Ownership, RemoteResourceType},
    provider::{
        HistoricalProvider, ObservedResourceStatus, PinningProvider, ProviderError,
        ProviderErrorClass, RemotePin, RemotePinStatus, SubmitEffect,
    },
};
use std::sync::Arc;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

struct LegacyProvider(Option<ProviderErrorClass>);

#[async_trait::async_trait]
impl PinningProvider for LegacyProvider {
    fn name(&self) -> &str {
        "legacy"
    }
    async fn submit(
        &self,
        _pin: crate::pinning::provider::SubmitPin,
    ) -> Result<RemotePin, ProviderError> {
        if let Some(class) = self.0 {
            return Err(ProviderError {
                class,
                message: "legacy rejected".into(),
                retry_after: None,
            });
        }
        Ok(RemotePin {
            request_id: "legacy-resource".into(),
            cid: CID.into(),
            status: RemotePinStatus::Pinned,
            raw_status: "pinned".into(),
            failure_reason: None,
        })
    }
    async fn find(
        &self,
        _query: crate::pinning::provider::FindPin,
    ) -> Result<Vec<RemotePin>, ProviderError> {
        unreachable!()
    }
    async fn get(&self, _id: &str) -> Result<RemotePin, ProviderError> {
        unreachable!()
    }
    async fn unpin(&self, _id: &str) -> Result<(), ProviderError> {
        unreachable!()
    }
}

#[tokio::test]
async fn legacy_default_observation_preserves_rejected_vs_unknown_class_semantics() {
    let ok = LegacyProvider(None).submit_observed(submit(CID)).await;
    assert!(ok.result.is_ok());
    assert_eq!(ok.effect, SubmitEffect::Observed);
    assert!(ok.resources.is_empty());
    for class in [
        ProviderErrorClass::NotSubmitted,
        ProviderErrorClass::Authentication,
        ProviderErrorClass::PermissionDenied,
        ProviderErrorClass::PlanRestricted,
        ProviderErrorClass::UnknownForbidden,
        ProviderErrorClass::InvalidInput,
        ProviderErrorClass::Quota,
        ProviderErrorClass::Terminal,
        ProviderErrorClass::NotFound,
        ProviderErrorClass::Ambiguous,
        ProviderErrorClass::Transient,
        ProviderErrorClass::Protocol,
        ProviderErrorClass::RateLimited,
    ] {
        let observation = LegacyProvider(Some(class))
            .submit_observed(submit(CID))
            .await;
        let error = observation.result.unwrap_err();
        assert_eq!(
            observation.effect,
            if error.definitely_not_submitted() {
                SubmitEffect::NotSubmitted
            } else {
                SubmitEffect::Unknown
            }
        );
        assert!(observation.resources.is_empty());
    }
}

#[tokio::test]
async fn historical_wrapper_preserves_full_rpc_observation_and_exact_routes() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    source_file(&source, CID, b"stored bytes").await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{OTHER}\"}}\n")),
        )
        .mount(&target)
        .await;
    let rpc: Arc<dyn PinningProvider> = Arc::new(provider(
        &target,
        &source,
        RpcProfile::Filebase,
        RpcStrategy::Upload,
    ));
    let historical = HistoricalProvider {
        inner: rpc.clone(),
        api: "filebase-rpc",
        strategy: "upload",
    };
    let observation = historical.submit_observed(submit(CID)).await;
    assert!(observation.result.is_err());
    assert_eq!(observation.effect, SubmitEffect::Observed);
    assert_eq!(observation.resources.len(), 1);
    let resource = &observation.resources[0];
    assert_eq!(resource.resource_type, RemoteResourceType::RpcPin);
    assert_eq!(resource.cid, OTHER);
    assert_eq!(resource.status, ObservedResourceStatus::PinAccepted);
    assert_eq!(resource.ownership, Ownership::Unknown);
    assert!(resource.request_id.starts_with("rpc-pin:filebase:"));
    let bad = HistoricalProvider {
        inner: rpc,
        api: "kubo",
        strategy: "cid",
    };
    let refused = bad.submit_observed(submit(CID)).await;
    assert_eq!(refused.effect, SubmitEffect::NotSubmitted);
    assert!(refused.resources.is_empty());
    assert!(bad.find(query(CID)).await.is_err());
    assert_eq!(target.received_requests().await.unwrap().len(), 1);
}
