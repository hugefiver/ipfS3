use std::sync::Arc;

use http::{HeaderMap, Uri};
use s3s::{
    S3Request, S3Result,
    access::{S3Access, S3AccessContext},
    auth::Credentials,
};

use super::{AnonymousHttp, SessionPrincipal};
use crate::{auth::GatewayCredentials, state::AppState};

pub struct GatewayAccess(pub Arc<AppState>);

#[async_trait::async_trait]
impl S3Access for GatewayAccess {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        if cx.credentials().is_none() {
            if has_auth_material(cx.headers(), cx.uri())
                || has_zip_controls(cx.headers(), cx.uri())
                || !matches!(
                    cx.s3_op().name(),
                    "HeadBucket"
                        | "GetBucketLocation"
                        | "ListObjects"
                        | "ListObjectsV2"
                        | "ListObjectVersions"
                        | "GetObject"
                        | "HeadObject"
                        | "PutObject"
                        | "CreateMultipartUpload"
                        | "UploadPart"
                        | "CompleteMultipartUpload"
                )
            {
                return Err(s3s::s3_error!(AccessDenied, "Signature is required"));
            }
            cx.extensions_mut().insert(AnonymousHttp);
            return Ok(());
        }
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

pub(crate) fn has_auth_material(headers: &HeaderMap, uri: &Uri) -> bool {
    headers.contains_key(http::header::AUTHORIZATION)
        || headers.contains_key("x-amz-security-token")
        || uri.query().is_some_and(|query| {
            url::form_urlencoded::parse(query.as_bytes()).any(|(key, _)| {
                matches!(
                    key.as_ref(),
                    "X-Amz-Algorithm"
                        | "X-Amz-Credential"
                        | "X-Amz-SignedHeaders"
                        | "X-Amz-Signature"
                        | "X-Amz-Security-Token"
                        | "X-Amz-Date"
                        | "X-Amz-Expires"
                        | "AWSAccessKeyId"
                        | "Signature"
                        | "Expires"
                )
            })
        })
}

pub(crate) fn has_zip_controls(headers: &HeaderMap, uri: &Uri) -> bool {
    headers
        .keys()
        .any(|key| key.as_str().starts_with("x-ipfs3-zip-"))
        || uri.query().is_some_and(|query| {
            url::form_urlencoded::parse(query.as_bytes()).any(|(key, _)| {
                key.starts_with("decompress-zip")
                    || key.starts_with("ipfs3-zip-")
                    || key.starts_with("x-ipfs3-zip-")
            })
        })
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
