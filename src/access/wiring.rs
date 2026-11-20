use std::sync::Arc;

use s3s::{
    S3Request, S3Result,
    access::{S3Access, S3AccessContext},
};

use super::Principal;
use crate::state::AppState;

pub struct GatewayAccess(pub Arc<AppState>);

#[async_trait::async_trait]
impl S3Access for GatewayAccess {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        let credentials = cx
            .credentials()
            .ok_or_else(|| s3s::s3_error!(AccessDenied, "Signature is required"))?;
        let subject =
            self.0
                .credentials
                .principal(&credentials.access_key, cx.headers(), cx.uri())?;
        cx.extensions_mut().insert(Principal(subject));
        Ok(())
    }
}

pub(crate) fn bind_custom<T>(state: &AppState, req: &mut S3Request<T>) -> S3Result<()> {
    let credentials = req
        .credentials
        .as_ref()
        .ok_or_else(|| s3s::s3_error!(AccessDenied, "Signature is required"))?;
    let subject = state
        .credentials
        .principal(&credentials.access_key, &req.headers, &req.uri)?;
    req.extensions.insert(Principal(subject));
    Ok(())
}
