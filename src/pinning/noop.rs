use crate::pinning::provider::{
    FindPin, PinningProvider, ProviderError, RemotePin, RemotePinStatus, SubmitPin,
};

pub struct NoopProvider {
    name: String,
}

impl NoopProvider {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    fn pinned(request_id: String, cid: String) -> RemotePin {
        RemotePin {
            request_id,
            cid,
            status: RemotePinStatus::Pinned,
            raw_status: "pinned".to_owned(),
            failure_reason: None,
        }
    }
}

impl Default for NoopProvider {
    fn default() -> Self {
        Self::new("noop".to_owned())
    }
}

#[async_trait::async_trait]
impl PinningProvider for NoopProvider {
    fn invocation_route(&self) -> (&'static str, &'static str) {
        ("noop", "cid")
    }
    fn name(&self) -> &str {
        &self.name
    }

    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        Ok(Self::pinned(format!("noop:{}", request.cid), request.cid))
    }

    async fn get(&self, request_id: &str) -> Result<RemotePin, ProviderError> {
        let cid = request_id
            .strip_prefix("noop:")
            .unwrap_or(request_id)
            .to_owned();
        Ok(Self::pinned(request_id.to_owned(), cid))
    }

    async fn find(&self, query: FindPin) -> Result<Vec<RemotePin>, ProviderError> {
        Ok(vec![Self::pinned(format!("noop:{}", query.cid), query.cid)])
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
        provider::{FindPin, PinningProvider, RemotePinStatus, SubmitPin},
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
        assert_eq!(
            provider.get("noop:bafy-target").await.unwrap(),
            crate::pinning::provider::RemotePin {
                request_id: "noop:bafy-target".to_owned(),
                cid: "bafy-target".to_owned(),
                status: RemotePinStatus::Pinned,
                raw_status: "pinned".to_owned(),
                failure_reason: None,
            }
        );
        assert_eq!(
            provider
                .find(FindPin::for_job("bafy-target", "job-7"))
                .await
                .unwrap(),
            vec![crate::pinning::provider::RemotePin {
                request_id: "noop:bafy-target".to_owned(),
                cid: "bafy-target".to_owned(),
                status: RemotePinStatus::Pinned,
                raw_status: "pinned".to_owned(),
                failure_reason: None,
            }]
        );
        provider.unpin("request-ignored").await.unwrap();
    }
}
