use std::sync::Arc;

use s3s::S3Result;
use s3s::auth::{S3Auth, SecretKey};

use crate::state::AppState;

pub struct GatewayAuth {
    state: Arc<AppState>,
}

impl GatewayAuth {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl S3Auth for GatewayAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        self.state
            .credentials
            .get(access_key)
            .cloned()
            .ok_or_else(|| s3s::s3_error!(InvalidAccessKeyId, "unknown access key: {access_key}"))
    }
}
