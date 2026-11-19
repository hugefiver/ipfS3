use std::{collections::BTreeMap, time::Duration};

pub fn canonical_cid(value: &str) -> Result<String, ProviderError> {
    let cid = cid::Cid::try_from(value).map_err(|_| ProviderError {
        class: ProviderErrorClass::InvalidInput,
        message: "invalid resource CID".into(),
        retry_after: None,
    })?;
    Ok(cid::Cid::new_v1(cid.codec(), *cid.hash()).to_string())
}

pub use canonical_cid as canonical_resource_cid;

pub fn cids_equivalent(left: &str, right: &str) -> Result<bool, ProviderError> {
    Ok(canonical_cid(left)? == canonical_cid(right)?)
}

#[async_trait::async_trait]
pub trait PinningProvider: Send + Sync + 'static {
    fn name(&self) -> &str;
    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError>;
    async fn unpin(&self, request_id: &str) -> Result<(), ProviderError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitPin {
    pub cid: String,
    pub name: String,
    pub metadata: BTreeMap<String, String>,
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
