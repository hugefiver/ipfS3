//! Authorization and publication identities for gateway requests.

mod wiring;
pub use wiring::GatewayAccess;
pub(crate) use wiring::{bind_custom, has_auth_material, has_zip_controls};

use s3s::{S3Request, S3Result};
use sea_orm::ConnectionTrait;

use crate::{
    error::{AppError, AppResult},
    iam::{Action, Context, Resource, Scope},
    state::AppState,
    store::{bucket, entities},
};

/// Publication provenance only; never an authenticated policy subject or bucket owner.
/// The SigV4 Credential scope delimiter `/` separates this value from signable access key IDs.
pub(crate) const ANONYMOUS_ORIGIN: &str = "ipfs-s3/anonymous";

#[derive(Clone, Copy)]
struct AnonymousHttp;

pub(crate) fn anonymous_http<T>(req: &S3Request<T>) -> bool {
    req.extensions.get::<AnonymousHttp>().is_some()
}

pub(crate) fn subject<'a, T>(state: &AppState, req: &'a S3Request<T>) -> S3Result<Option<&'a str>> {
    if let Some(principal) = req.extensions.get::<SessionPrincipal>() {
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

pub(crate) fn service_scope<'a, T>(
    state: &'a AppState,
    req: &S3Request<T>,
    prospective_bucket: Option<&str>,
) -> S3Result<Scope<'a>> {
    let user = subject(state, req)?
        .ok_or_else(|| s3s::s3_error!(AccessDenied, "Signature is required"))?;
    Ok(state.credentials.policies.scope(user, prospective_bucket))
}

pub(crate) fn model_scope<'a>(
    state: &'a AppState,
    user: Option<&str>,
    model: &entities::bucket::Model,
) -> Scope<'a> {
    state.credentials.policies.bucket_scope(
        user,
        &model.name,
        model.owner.as_deref(),
        model.canned_acl.as_deref(),
    )
}

/// A missing bucket retains signed IAM scope solely for operations that must authorize
/// before returning their missing-resource error. Check `bucket_exists` before use.
pub(crate) async fn bucket_scope<'a>(
    state: &'a AppState,
    db: &impl ConnectionTrait,
    user: Option<&str>,
    name: &str,
) -> AppResult<Scope<'a>> {
    let (scope, _) = load_bucket_scope(state, db, user, name, false).await?;
    Ok(scope)
}

async fn load_bucket_scope<'a>(
    state: &'a AppState,
    db: &impl ConnectionTrait,
    user: Option<&str>,
    name: &str,
    embedded_legacy: bool,
) -> AppResult<(Scope<'a>, Option<entities::bucket::Model>)> {
    let policies = &state.credentials.policies;
    match bucket::get(db, name).await {
        Ok(model) => {
            let scope = if embedded_legacy && model.canned_acl.is_none() {
                policies.bucket_scope(Some(""), name, None, None)
            } else {
                model_scope(state, user, &model)
            };
            Ok((scope, Some(model)))
        }
        Err(AppError::NoSuchBucket(_)) => match user {
            Some(user) => Ok((policies.scope(user, Some(name)), None)),
            None if embedded_legacy => Ok((policies.scope("", Some(name)), None)),
            None => Err(AppError::AccessDenied("access denied".into())),
        },
        Err(error) => Err(error),
    }
}

async fn load_request_bucket_scope<'a, T>(
    state: &'a AppState,
    req: &S3Request<T>,
    name: &str,
) -> S3Result<(Scope<'a>, Option<entities::bucket::Model>)> {
    let user = subject(state, req)?;
    // Older embedded callers access unclassified buckets without installing an HTTP hook.
    let embedded_legacy =
        user.is_none() && !anonymous_http(req) && state.credentials.policies.is_empty();
    Ok(load_bucket_scope(state, state.store.db(), user, name, embedded_legacy).await?)
}

pub(crate) async fn request_bucket_scope<'a, T>(
    state: &'a AppState,
    req: &S3Request<T>,
    name: &str,
) -> S3Result<Scope<'a>> {
    let (scope, _) = load_request_bucket_scope(state, req, name).await?;
    Ok(scope)
}

async fn existing_bucket_scope<'a, T>(
    state: &'a AppState,
    req: &S3Request<T>,
    name: &str,
) -> S3Result<(Scope<'a>, entities::bucket::Model)> {
    let (scope, model) = load_request_bucket_scope(state, req, name).await?;
    let model = model.ok_or_else(|| AppError::NoSuchBucket(name.to_owned()))?;
    Ok((scope, model))
}

pub(crate) async fn scope<'a, T>(
    state: &'a AppState,
    req: &S3Request<T>,
    bucket: Option<&str>,
) -> S3Result<Scope<'a>> {
    let Some(name) = bucket else {
        return service_scope(state, req, None);
    };
    let (scope, _) = existing_bucket_scope(state, req, name).await?;
    Ok(scope)
}

pub(crate) async fn authorize<T>(
    state: &AppState,
    req: &S3Request<T>,
    action: Action,
    bucket: Option<&str>,
    resource: Resource<'_>,
) -> S3Result<()> {
    let scope = match action {
        Action::CreateBucket => service_scope(state, req, bucket)?,
        _ => scope(state, req, bucket).await?,
    };
    authorize_scope(&scope, action, resource)
}

pub(crate) async fn authorize_bucket<T>(
    state: &AppState,
    req: &S3Request<T>,
    action: Action,
    name: &str,
    resource: Resource<'_>,
) -> S3Result<entities::bucket::Model> {
    let (scope, model) = existing_bucket_scope(state, req, name).await?;
    authorize_scope(&scope, action, resource)?;
    Ok(model)
}

fn authorize_scope(scope: &Scope<'_>, action: Action, resource: Resource<'_>) -> S3Result<()> {
    if scope.allows(&Context {
        action,
        resource,
        version: None,
        request_tags: None,
        existing_tags: None,
    }) {
        Ok(())
    } else {
        Err(s3s::s3_error!(AccessDenied))
    }
}

/// Stable local user behind a verified temporary credential.
#[derive(Clone)]
struct SessionPrincipal(String);
