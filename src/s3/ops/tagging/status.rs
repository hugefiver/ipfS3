use crate::{
    error::{AppError, AppResult},
    s3::{ops::object::principal_id, query},
    state::AppState,
    store::object_version::{VersionSelector, resolve_version},
};
use chrono::{DateTime, Utc};
use http::{HeaderMap, HeaderValue, Method, Uri, header::CONTENT_TYPE};
use s3s::{Body, S3Request, S3Response, S3Result};
use sea_orm::{ConnectionTrait, Statement};
use serde::Serialize;

pub(crate) fn matches(uri: &Uri) -> bool {
    query::query_key_is_present(uri, "ipfs3-pin-status")
}

pub(crate) async fn get(state: &AppState, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    principal_id(&req)?;
    if req.method != Method::GET {
        return Err(s3s::s3_error!(MethodNotAllowed));
    }
    let invalid = || s3s::s3_error!(InvalidArgument, "invalid pin status request");
    let (bucket, key) = req
        .uri
        .path()
        .trim_start_matches('/')
        .split_once('/')
        .filter(|(b, k)| !b.is_empty() && !k.is_empty())
        .ok_or_else(invalid)?;
    let decode = |s: &str| {
        percent_encoding::percent_decode_str(s)
            .decode_utf8()
            .map(|s| s.into_owned())
            .map_err(|_| invalid())
    };
    let bucket = decode(bucket)?;
    let key = decode(key)?;
    let mut version = None;
    let mut status = false;
    for (name, value) in query::decoded_query_pairs(&req.uri)? {
        match name.as_str() {
            "ipfs3-pin-status" if !status && value.is_empty() => status = true,
            "versionId" if version.is_none() => version = Some(value),
            _ => return Err(invalid()),
        }
    }
    let selector = super::version_selector(version.as_deref())?;
    let db = state.store.db();
    let selected = resolve_version(db, &bucket, &key, &selector).await?;
    let owner =
        super::selected_content_owner(&selected, matches!(selector, VersionSelector::Current))?;
    let references = file_references(db, &owner.id, Utc::now()).await?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let version_id = super::response_version_id(db, &bucket, &selected).await?;
    if let Some(version) = version_id.as_ref() {
        headers.insert(
            "x-amz-version-id",
            HeaderValue::from_str(version)
                .map_err(|_| AppError::Internal("invalid stored version ID".into()))?,
        );
    }
    let body = serde_json::json!({ "version_id": version_id, "references": references });
    Ok(S3Response::with_headers(
        Body::from(body.to_string()),
        headers,
    ))
}

#[derive(Serialize)]
struct PinReference {
    provider: String,
    cid: String,
    source: String,
    expires_at: DateTime<Utc>,
    status: String,
}

async fn file_references(
    db: &impl ConnectionTrait,
    owner: &str,
    now: DateTime<Utc>,
) -> AppResult<Vec<PinReference>> {
    let rows = db.query_all(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT r.provider, r.cid, r.source, r.expires_at, p.status FROM pin_refs r \
         JOIN pin_resources p USING (provider, cid) WHERE r.owner_object_id = $1 AND r.expires_at > $2 \
         ORDER BY r.provider, r.cid, r.source, r.expires_at", [owner.into(), now.timestamp_micros().into()])).await?;
    rows.into_iter()
        .map(|r| {
            Ok(PinReference {
                provider: r.try_get("", "provider")?,
                cid: r.try_get("", "cid")?,
                source: r.try_get("", "source")?,
                expires_at: DateTime::from_timestamp_micros(r.try_get("", "expires_at")?)
                    .ok_or_else(|| AppError::Database("invalid pin expiry".into()))?,
                status: r.try_get("", "status")?,
            })
        })
        .collect()
}
