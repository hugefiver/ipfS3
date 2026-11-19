use crate::pinning::provider::{
    PinningProvider, ProviderError, RemotePin, RemotePinStatus, SubmitPin,
};

pub struct NoopProvider {
    name: String,
}

impl NoopProvider {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl Default for NoopProvider {
    fn default() -> Self {
        Self::new("noop".to_owned())
    }
}

#[async_trait::async_trait]
impl PinningProvider for NoopProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        Ok(RemotePin {
            request_id: format!("noop:{}", request.cid),
            cid: request.cid,
            status: RemotePinStatus::Pinned,
            raw_status: "pinned".to_owned(),
            failure_reason: None,
        })
    }

    async fn unpin(&self, _request_id: &str) -> Result<(), ProviderError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::pinning::{
        noop::NoopProvider,
        provider::{PinningProvider, RemotePinStatus, SubmitPin},
    };

    #[tokio::test]
    async fn noop_operations_are_deterministic() {
        let provider = NoopProvider::new("local".to_owned());
        let submitted = provider
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key".to_owned(),
                metadata: BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            })
            .await
            .unwrap();

        assert_eq!(provider.name(), "local");
        assert_eq!(submitted.request_id, "noop:bafy-target");
        assert_eq!(submitted.cid, "bafy-target");
        assert_eq!(submitted.status, RemotePinStatus::Pinned);
        assert_eq!(submitted.raw_status, "pinned");
        assert_eq!(submitted.failure_reason, None);
        provider.unpin(&submitted.request_id).await.unwrap();
    }
}
