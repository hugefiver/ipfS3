use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use axum::extract::ConnectInfo;
use http::{Extensions, HeaderMap, Uri};
use s3s::{
    S3Request, S3Result,
    access::{S3Access, S3AccessContext},
    auth::Credentials,
};

use super::{AnonymousHttp, SessionPrincipal};
use crate::{auth::GatewayCredentials, rate_limit::RateLimiter, state::AppState};

pub struct GatewayAccess(pub Arc<AppState>);

#[async_trait::async_trait]
impl S3Access for GatewayAccess {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        let principal = if cx.credentials().is_none() {
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
            None
        } else {
            session_principal(
                &self.0.credentials,
                cx.credentials(),
                cx.headers(),
                cx.uri(),
            )?
        };
        if let Some((limiter, peer)) = request_limiter(cx.extensions_mut()) {
            let user = verified_user(principal.as_ref(), cx.credentials());
            let get_object = cx.s3_op().name() == "GetObject";
            let peer = if user.is_none() {
                peer.map(|peer| limiter.anonymous_client_ip(peer, cx.headers(), get_object))
            } else {
                peer
            };
            if let Some(budget) = limiter.admit(user, peer, get_object)? {
                cx.extensions_mut().insert(budget);
            }
        }
        if let Some(principal) = principal {
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
    let credentials = req.credentials.as_ref();
    let principal = session_principal(&state.credentials, credentials, &req.headers, &req.uri)?;
    if let Some((limiter, peer)) = request_limiter(&req.extensions) {
        let user = verified_user(principal.as_ref(), credentials);
        limiter.admit(user, peer, false)?;
    }
    if let Some(principal) = principal {
        req.extensions.insert(principal);
    }
    Ok(())
}

fn verified_user<'a>(
    principal: Option<&'a SessionPrincipal>,
    credentials: Option<&'a Credentials>,
) -> Option<&'a str> {
    principal
        .map(|principal| principal.0.as_str())
        .or_else(|| credentials.map(|credentials| credentials.access_key.as_str()))
}

fn request_limiter(extensions: &Extensions) -> Option<(Arc<RateLimiter>, Option<IpAddr>)> {
    let limiter = extensions.get::<Arc<RateLimiter>>()?.clone();
    let peer = extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|peer| peer.0.ip());
    Some((limiter, peer))
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
