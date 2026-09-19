use std::{collections::BTreeMap, time::Duration};

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

    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError>;

    async fn get(&self, request_id: &str) -> Result<RemotePin, ProviderError>;

    async fn find(&self, query: FindPin) -> Result<Vec<RemotePin>, ProviderError>;

    async fn unpin(&self, request_id: &str) -> Result<(), ProviderError>;
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
