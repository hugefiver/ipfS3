//! Authorization for signed gateway requests.

mod wiring;
pub use wiring::GatewayAccess;
pub(crate) use wiring::bind_custom;

use s3s::{S3Request, S3Result};

use crate::{iam::Scope, state::AppState};

pub(crate) fn subject<'a, T>(state: &AppState, req: &'a S3Request<T>) -> S3Result<Option<&'a str>> {
    if let Some(principal) = req.extensions.get::<Principal>() {
        return Ok(Some(&principal.0));
    }
    let Some(credentials) = &req.credentials else {
        return Ok(None);
    };
    // A builder without the access hook has verified SigV4, but not a session token.
    if state
        .credentials
        .requires_session_token(&credentials.access_key)
    {
        return Err(s3s::s3_error!(InvalidToken, "unbound session credential"));
    }
    Ok(Some(&credentials.access_key))
}

pub(crate) fn scope<'a, T>(
    state: &'a AppState,
    req: &S3Request<T>,
    bucket: Option<&str>,
) -> S3Result<Scope<'a>> {
    let policies = &state.credentials.policies;
    let user = match subject(state, req)? {
        Some(user) => user,
        None if policies.is_empty() => "",
        None => {
            return Err(s3s::s3_error!(
                AccessDenied,
                "authenticated principal is required"
            ));
        }
    };
    Ok(policies.scope(user, bucket))
}

/// Stable local user behind a verified static or temporary credential.
#[derive(Clone, Debug)]
struct Principal(String);
