use std::{collections::BTreeMap, time::Duration};

#[async_trait::async_trait]
pub trait PinningProvider: Send + Sync + 'static {
    fn name(&self) -> &str;

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
    NotFound,
    Ambiguous,
    RateLimited,
    Quota,
    Transient,
    Terminal,
    Protocol,
}

#[derive(Debug, thiserror::Error)]
#[error("provider request failed: class={class:?}, message={message}")]
pub struct ProviderError {
    pub class: ProviderErrorClass,
    pub message: String,
    pub retry_after: Option<Duration>,
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
