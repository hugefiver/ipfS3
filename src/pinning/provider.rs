use std::{collections::BTreeMap, time::Duration};

use crate::pinning::identity::{Ownership, RemoteResourceType};

/// Submission evidence survives errors and historical adapters. An error class
/// alone is not proof that a dispatched RPC write had no side effects.
#[derive(Debug)]
pub struct SubmitObservation {
    pub result: Result<RemotePin, ProviderError>,
    pub resources: Vec<ObservedResource>,
    pub effect: SubmitEffect,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObservedResource {
    pub resource_type: RemoteResourceType,
    pub cid: String,
    pub request_id: String,
    pub status: ObservedResourceStatus,
    pub ownership: Ownership,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservedResourceStatus {
    Reported,
    Stored,
    PinAccepted,
    PinError,
    RecursiveVerified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmitEffect {
    NotSubmitted,
    Observed,
    Unknown,
}

/// Canonicalize only a ledger/comparison CID; never rewrite object CIDs/ETags.
pub fn canonical_cid(value: &str) -> Result<String, ProviderError> {
    let cid = cid::Cid::try_from(value).map_err(|_| ProviderError {
        class: ProviderErrorClass::InvalidInput,
        message: "invalid resource CID".into(),
        retry_after: None,
    })?;
    Ok(cid::Cid::new_v1(cid.codec(), *cid.hash()).to_string())
}

/// Explicit ledger name retained for the store/worker shared interface.
pub fn canonical_resource_cid(value: &str) -> Result<String, ProviderError> {
    canonical_cid(value)
}

pub fn cids_equivalent(left: &str, right: &str) -> Result<bool, ProviderError> {
    Ok(canonical_cid(left)? == canonical_cid(right)?)
}

#[async_trait::async_trait]
pub trait PinningProvider: Send + Sync + 'static {
    fn name(&self) -> &str;

    /// Actual protocol used by this implementation, not a configured display name.
    fn invocation_route(&self) -> (&'static str, &'static str) {
        ("unknown", "unknown")
    }

    async fn find_historical(
        &self,
        query: FindPin,
        api: &str,
        strategy: &str,
    ) -> Result<Vec<RemotePin>, ProviderError> {
        if self.invocation_route() != (api, strategy) {
            return Err(ProviderError {
                class: ProviderErrorClass::Protocol,
                message: "historical provider route unavailable".into(),
                retry_after: None,
            });
        }
        self.find(query).await
    }

    async fn observe_historical(
        &self,
        query: FindPin,
        api: &str,
        strategy: &str,
    ) -> Result<QueryObservation, ProviderError> {
        self.find_historical(query, api, strategy)
            .await
            .map(QueryObservation::Complete)
    }

    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError>;

    /// Legacy providers retain their existing rejection/unknown classification.
    /// RPC implementations override this to preserve resource-level evidence.
    async fn submit_observed(&self, request: SubmitPin) -> SubmitObservation {
        let result = self.submit(request).await;
        let effect = match &result {
            Ok(_) => SubmitEffect::Observed,
            Err(error) if error.definitely_not_submitted() => SubmitEffect::NotSubmitted,
            Err(_) => SubmitEffect::Unknown,
        };
        SubmitObservation {
            result,
            resources: Vec::new(),
            effect,
        }
    }

    async fn get(&self, request_id: &str) -> Result<RemotePin, ProviderError>;

    async fn get_historical(
        &self,
        request_id: &str,
        api: &str,
        strategy: &str,
    ) -> Result<RemotePin, ProviderError> {
        if self.invocation_route() != (api, strategy) {
            return Err(historical_route_error());
        }
        self.get(request_id).await
    }

    async fn find(&self, query: FindPin) -> Result<Vec<RemotePin>, ProviderError>;

    async fn unpin(&self, request_id: &str) -> Result<(), ProviderError>;

    async fn unpin_historical(
        &self,
        request_id: &str,
        api: &str,
        strategy: &str,
    ) -> Result<(), ProviderError> {
        if self.invocation_route() != (api, strategy) {
            return Err(historical_route_error());
        }
        self.unpin(request_id).await
    }
}

fn historical_route_error() -> ProviderError {
    ProviderError {
        class: ProviderErrorClass::Protocol,
        message: "historical provider route unavailable".into(),
        retry_after: None,
    }
}

/// A typed reference always carries the account and historical transport route.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RemoteRef {
    pub resource_type: crate::pinning::identity::RemoteResourceType,
    pub cid: String,
    pub opaque_id: String,
    pub route: crate::pinning::identity::ProviderRouteSnapshot,
    pub ownership: crate::pinning::identity::Ownership,
}

/// Only Complete is authoritative for absence. Unknown is never an empty page.
#[derive(Debug)]
pub enum QueryObservation {
    Complete(Vec<RemotePin>),
    Unknown(ProviderError),
}

pub(crate) struct HistoricalProvider {
    pub inner: std::sync::Arc<dyn PinningProvider>,
    pub api: &'static str,
    pub strategy: &'static str,
}

#[async_trait::async_trait]
impl PinningProvider for HistoricalProvider {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn invocation_route(&self) -> (&'static str, &'static str) {
        (self.api, self.strategy)
    }
    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        if self.inner.invocation_route() != self.invocation_route() {
            return Err(historical_route_error());
        }
        self.inner.submit(request).await
    }
    async fn submit_observed(&self, request: SubmitPin) -> SubmitObservation {
        if self.inner.invocation_route() != self.invocation_route() {
            return SubmitObservation {
                result: Err(historical_route_error()),
                resources: Vec::new(),
                effect: SubmitEffect::NotSubmitted,
            };
        }
        self.inner.submit_observed(request).await
    }
    async fn get(&self, id: &str) -> Result<RemotePin, ProviderError> {
        self.inner.get_historical(id, self.api, self.strategy).await
    }
    async fn unpin(&self, id: &str) -> Result<(), ProviderError> {
        self.inner
            .unpin_historical(id, self.api, self.strategy)
            .await
    }
    async fn find(&self, query: FindPin) -> Result<Vec<RemotePin>, ProviderError> {
        self.inner
            .find_historical(query, self.api, self.strategy)
            .await
    }
    async fn find_historical(
        &self,
        query: FindPin,
        api: &str,
        strategy: &str,
    ) -> Result<Vec<RemotePin>, ProviderError> {
        self.inner.find_historical(query, api, strategy).await
    }
    async fn observe_historical(
        &self,
        query: FindPin,
        api: &str,
        strategy: &str,
    ) -> Result<QueryObservation, ProviderError> {
        self.inner.observe_historical(query, api, strategy).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitPin {
    pub cid: String,
    pub name: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindPin {
    pub cid: String,
    pub metadata: BTreeMap<String, String>,
}

impl FindPin {
    pub fn for_job(cid: &str, job_id: &str) -> Self {
        Self {
            cid: cid.to_owned(),
            metadata: BTreeMap::from([("gateway_job_id".to_owned(), job_id.to_owned())]),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemotePinStatus {
    Queued,
    Pinning,
    Pinned,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePin {
    pub request_id: String,
    pub cid: String,
    pub status: RemotePinStatus,
    pub raw_status: String,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderErrorClass {
    Authentication,
    PermissionDenied,
    PlanRestricted,
    UnknownForbidden,
    InvalidInput,
    NotFound,
    Ambiguous,
    RateLimited,
    Quota,
    Transient,
    Terminal,
    Protocol,
    NotSubmitted,
}

impl ProviderError {
    pub fn safe_evidence(&self, operation: &str) -> String {
        let http_status = self
            .message
            .strip_prefix("provider returned HTTP status ")
            .and_then(|status| status.parse::<u16>().ok())
            .filter(|status| (100..=599).contains(status));
        serde_json::json!({ "category": format!("{:?}", self.class), "operation": operation,
            "http_status": http_status, "retry_after_seconds": self.retry_after.map(|delay| delay.as_secs()),
            "effect": if operation == "submit" && self.definitely_not_submitted() { "not_created" } else { "unknown" }
        }).to_string()
    }

    /// Only explicit rejection or pre-connection failure proves no creation.
    pub fn definitely_not_submitted(&self) -> bool {
        matches!(
            self.class,
            ProviderErrorClass::Authentication
                | ProviderErrorClass::PermissionDenied
                | ProviderErrorClass::PlanRestricted
                | ProviderErrorClass::UnknownForbidden
                | ProviderErrorClass::InvalidInput
                | ProviderErrorClass::RateLimited
                | ProviderErrorClass::Quota
                | ProviderErrorClass::NotSubmitted
        )
    }
}

#[derive(Debug, thiserror::Error)]
#[error("provider request failed: class={class:?}, message={message}")]
pub struct ProviderError {
    pub class: ProviderErrorClass,
    pub message: String,
    pub retry_after: Option<Duration>,
}

/// Read at most 8 KiB within a bounded deadline, then emit only a finite class.
/// Provider messages and unrecognized codes never leave this boundary.
pub(crate) async fn classify_forbidden_response(
    mut response: reqwest::Response,
    deadline: Duration,
) -> ProviderErrorClass {
    let code = tokio::time::timeout(deadline, async {
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            if body.len().checked_add(chunk.len())? > 8192 {
                return None;
            }
            body.extend_from_slice(&chunk);
        }
        let value: serde_json::Value = serde_json::from_slice(&body).ok()?;
        let nested = value
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(serde_json::Value::as_str);
        let top = value.get("code").and_then(serde_json::Value::as_str);
        if nested.zip(top).is_some_and(|(nested, top)| nested != top) {
            return None;
        }
        nested.or(top).map(str::to_owned)
    })
    .await
    .ok()
    .flatten();
    match code.as_deref() {
        Some("PLAN_RESTRICTED" | "PLAN_LIMIT_EXCEEDED") => ProviderErrorClass::PlanRestricted,
        Some("PERMISSION_DENIED" | "INSUFFICIENT_PERMISSIONS") => {
            ProviderErrorClass::PermissionDenied
        }
        _ => ProviderErrorClass::UnknownForbidden,
    }
}

#[cfg(test)]
mod tests {
    use super::FindPin;

    #[test]
    fn find_for_job_uses_stable_gateway_metadata() {
        let query = FindPin::for_job("bafy-target", "job-7");

        assert_eq!(query.cid, "bafy-target");
        assert_eq!(
            query.metadata.get("gateway_job_id"),
            Some(&"job-7".to_owned())
        );
    }
}
