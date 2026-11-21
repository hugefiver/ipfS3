use std::sync::Arc;

use http::{HeaderMap, Uri};
use s3s::{
    S3Request, S3Result,
    access::{S3Access, S3AccessContext},
    auth::Credentials,
};

use super::SessionPrincipal;
use crate::{auth::GatewayCredentials, state::AppState};

pub struct GatewayAccess(pub Arc<AppState>);

#[async_trait::async_trait]
impl S3Access for GatewayAccess {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        if let Some(principal) = session_principal(
            &self.0.credentials,
            cx.credentials(),
            cx.headers(),
            cx.uri(),
        )? {
            cx.extensions_mut().insert(principal);
        }
        Ok(())
    }
}

pub(crate) fn bind_custom<T>(state: &AppState, req: &mut S3Request<T>) -> S3Result<()> {
    if let Some(principal) = session_principal(
        &state.credentials,
        req.credentials.as_ref(),
        &req.headers,
        &req.uri,
    )? {
        req.extensions.insert(principal);
    }
    Ok(())
}

fn session_principal(
    gateway: &GatewayCredentials,
    credentials: Option<&Credentials>,
    headers: &HeaderMap,
    uri: &Uri,
) -> S3Result<Option<SessionPrincipal>> {
    let credentials =
        credentials.ok_or_else(|| s3s::s3_error!(AccessDenied, "Signature is required"))?;
    Ok(gateway
        .session_subject(&credentials.access_key, headers, uri)?
        .map(SessionPrincipal))
}
