use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};

use crate::crypto::EncryptionMode;
use crate::error::AppError;
use crate::pinning::policy::{PublicationContext, PublicationPolicy};
use crate::pinning::tags::ObjectTag;
use crate::state::AppState;
use crate::store::object_version::{
    BucketVersioningState, PublicVersionId, VersionKind, VersionSelector,
};
use crate::store::pinning::publication::{
    PinTargetSpec, PublicationObject, PublicationRequest, PublicationResult,
};

/// Wraps a byte stream and counts the total bytes that flow through it.
/// The count handle is read after the stream has been fully consumed.
pub struct ByteCounter {
    count: Arc<AtomicU64>,
}

impl ByteCounter {
    pub fn new() -> (Self, Arc<AtomicU64>) {
        let count = Arc::new(AtomicU64::new(0));
        (
            Self {
                count: count.clone(),
            },
            count,
        )
    }

    pub fn wrap<S, E>(self, stream: S) -> impl Stream<Item = Result<Bytes, E>>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
    {
        let count = self.count;
        async_stream::stream! {
            let mut s = Box::pin(stream);
            while let Some(chunk) = s.next().await {
                if let Ok(ref b) = chunk {
                    count.fetch_add(b.len() as u64, Ordering::Relaxed);
                }
                yield chunk;
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub cid: String,
    pub size: i64,
}

pub async fn add_plain_object_stream<S, E>(
    state: &Arc<AppState>,
    stream: S,
) -> crate::error::AppResult<StoredObject>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    let (counter, count_handle) = ByteCounter::new();
    let counted = counter.wrap(stream);
    let cid = crate::kubo::add::stream_add(&state.kubo, counted, 1).await?;

    crate::kubo::pin::pin_add(&state.kubo, &cid).await?;

    Ok(StoredObject {
        cid,
        size: count_handle.load(Ordering::Relaxed) as i64,
    })
}

pub async fn publish_plain_object(
    state: &Arc<AppState>,
    bucket: &str,
    key: &str,
    content_type: Option<&str>,
    metadata: Option<serde_json::Value>,
    stored: &StoredObject,
    multipart: bool,
) -> S3Result<PublicationResult> {
    let object_id = uuid::Uuid::new_v4().to_string();
    let tags = Vec::new();
    let policy = evaluate_publication_policy(state, bucket, key, &tags)?;
    let mut object = PublicationObject::from_put(
        object_id,
        bucket,
        key,
        stored.cid.clone(),
        stored.size,
        content_type.map(str::to_owned),
        metadata,
        false,
        None,
        None,
        chrono::Utc::now(),
    );
    object.multipart = multipart;
    crate::store::pinning::publication::publish_object(
        state.store.db(),
        PublicationRequest {
            object,
            tags: policy.tags.clone(),
            policy,
            object_target: PinTargetSpec {
                cid: stored.cid.clone(),
                logical_size: stored.size,
            },
        },
        state.pinning.provider_limits(),
    )
    .await
    .map_err(Into::into)
}

#[allow(dead_code)]
pub async fn put_plain_object_stream<S, E>(
    state: &Arc<AppState>,
    bucket: &str,
    key: &str,
    content_type: Option<&str>,
    metadata: Option<serde_json::Value>,
    stream: S,
    multipart: bool,
) -> S3Result<StoredObject>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    let stored = add_plain_object_stream(state, stream).await?;
    publish_plain_object(
        state,
        bucket,
        key,
        content_type,
        metadata,
        &stored,
        multipart,
    )
    .await?;
    Ok(stored)
}

/// Determine the requested server-side encryption mode from request headers.
pub fn determine_encryption_mode(headers: &http::HeaderMap) -> S3Result<EncryptionMode> {
    let sse_c_headers = [
        "x-amz-server-side-encryption-customer-algorithm",
        "x-amz-server-side-encryption-customer-key",
        "x-amz-server-side-encryption-customer-key-md5",
    ];
    let sse_c_header_count = sse_c_headers
        .iter()
        .filter(|&&name| headers.contains_key(name))
        .count();

    if sse_c_header_count != 0 {
        if sse_c_header_count != sse_c_headers.len()
            || headers.contains_key("x-amz-server-side-encryption")
        {
            return Err(s3s::s3_error!(
                InvalidArgument,
                "SSE-C headers must be complete and cannot be combined with SSE-S3"
            ));
        }

        // SSE-C: customer provides key. Algorithm must be AES256.
        let val = headers
            .get("x-amz-server-side-encryption-customer-algorithm")
            .expect("complete SSE-C headers include algorithm");
        if val != "AES256" {
            return Err(s3s::s3_error!(
                InvalidArgument,
                "unsupported SSE-C algorithm; must be AES256"
            ));
        }
        return Ok(EncryptionMode::SseC);
    }
    if let Some(val) = headers.get("x-amz-server-side-encryption") {
        if val == "AES256" {
            return Ok(EncryptionMode::SseS3);
        }
        return Err(s3s::s3_error!(
            InvalidArgument,
            "unsupported server-side encryption value"
        ));
    }
    Ok(EncryptionMode::None)
}

/// Extract user-supplied custom metadata from `x-amz-meta-*` headers.
pub fn extract_custom_metadata(headers: &http::HeaderMap) -> Option<serde_json::Value> {
    let mut map = serde_json::Map::new();
    for (key, value) in headers.iter() {
        let key_str = key.as_str();
        if let Some(rest) = key_str.strip_prefix("x-amz-meta-")
            && let Ok(v) = value.to_str()
        {
            map.insert(rest.to_string(), serde_json::Value::String(v.to_string()));
        }
    }
    if map.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(map))
    }
}

/// Build the IPFS identity headers returned after a successful standard PutObject.
pub fn put_object_ipfs_headers(cid: &str) -> S3Result<http::HeaderMap> {
    let cid_header = http::HeaderValue::from_str(cid)
        .map_err(|e| s3s::s3_error!(InternalError, "invalid IPFS CID header: {e}"))?;
    let url_header = http::HeaderValue::from_str(&format!("ipfs://{cid}"))
        .map_err(|e| s3s::s3_error!(InternalError, "invalid IPFS URL header: {e}"))?;

    let mut headers = http::HeaderMap::new();
    headers.insert("x-amz-meta-ipfs-cid", cid_header);
    headers.insert("x-amz-meta-ipfs-url", url_header);
    Ok(headers)
}

const NORMAL_SSE_C_HEADERS: [&str; 3] = [
    "x-amz-server-side-encryption-customer-algorithm",
    "x-amz-server-side-encryption-customer-key",
    "x-amz-server-side-encryption-customer-key-md5",
];
const COPY_SOURCE_SSE_C_HEADERS: [&str; 3] = [
    "x-amz-copy-source-server-side-encryption-customer-algorithm",
    "x-amz-copy-source-server-side-encryption-customer-key",
    "x-amz-copy-source-server-side-encryption-customer-key-md5",
];

struct ValidatedSseCHeaders {
    key: crate::crypto::ObjectKey,
    key_md5: String,
}

fn parse_sse_c_header_set(
    headers: &http::HeaderMap,
    names: [&str; 3],
    forbidden_names: &[&str],
    required: bool,
) -> S3Result<Option<ValidatedSseCHeaders>> {
    use base64::Engine;

    let present = names
        .iter()
        .filter(|name| headers.contains_key(**name))
        .count();
    let forbidden_present = forbidden_names
        .iter()
        .any(|name| headers.contains_key(*name));
    if forbidden_present || (present != 0 && present != names.len()) {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "SSE-C headers must be complete and unmixed"
        ));
    }
    if present == 0 {
        return if required {
            Err(s3s::s3_error!(
                InvalidArgument,
                "complete SSE-C headers are required"
            ))
        } else {
            Ok(None)
        };
    }

    let algorithm = headers[names[0]]
        .to_str()
        .map_err(|_| s3s::s3_error!(InvalidArgument, "invalid SSE-C algorithm header"))?;
    if algorithm != "AES256" {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "unsupported SSE-C algorithm; must be AES256"
        ));
    }

    let key_b64 = headers
        .get(names[1])
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing SSE-C customer key"))?;
    let key_bytes = base64::engine::general_purpose::STANDARD
        .decode(key_b64)
        .map_err(|e| s3s::s3_error!(InvalidArgument, "invalid SSE-C key: {e}"))?;
    if key_bytes.len() != 32 {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "SSE-C key must be 32 bytes"
        ));
    }

    // Validate key-MD5 — AWS requires this header for all SSE-C operations.
    let md5_b64 = headers
        .get(names[2])
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing SSE-C customer key MD5"))?;
    {
        let client_md5 = base64::engine::general_purpose::STANDARD
            .decode(md5_b64)
            .map_err(|e| s3s::s3_error!(InvalidArgument, "invalid SSE-C key-MD5: {e}"))?;
        if client_md5.len() != 16 {
            return Err(s3s::s3_error!(
                InvalidArgument,
                "SSE-C key MD5 must be 16 bytes"
            ));
        }
        let computed = md5::compute(&key_bytes);
        if !bool::from(subtle::ConstantTimeEq::ct_eq(
            client_md5.as_slice(),
            computed.as_ref(),
        )) {
            return Err(s3s::s3_error!(
                InvalidArgument,
                "SSE-C key MD5 mismatch — key may be corrupted"
            ));
        }
    }

    let mut ok_arr = [0u8; 32];
    ok_arr.copy_from_slice(&key_bytes);
    Ok(Some(ValidatedSseCHeaders {
        key: crate::crypto::ObjectKey { bytes: ok_arr },
        key_md5: md5_b64.to_owned(),
    }))
}

fn extract_sse_c_headers(headers: &http::HeaderMap) -> S3Result<ValidatedSseCHeaders> {
    parse_sse_c_header_set(
        headers,
        NORMAL_SSE_C_HEADERS,
        &["x-amz-server-side-encryption"],
        true,
    )?
    .ok_or_else(|| s3s::s3_error!(InvalidArgument, "complete SSE-C headers are required"))
}

pub fn extract_sse_c_key(headers: &http::HeaderMap) -> S3Result<crate::crypto::ObjectKey> {
    Ok(extract_sse_c_headers(headers)?.key)
}

fn extract_copy_source_sse_c_headers(
    headers: &http::HeaderMap,
) -> S3Result<Option<ValidatedSseCHeaders>> {
    let mut forbidden = NORMAL_SSE_C_HEADERS.to_vec();
    forbidden.push("x-amz-server-side-encryption");
    parse_sse_c_header_set(headers, COPY_SOURCE_SSE_C_HEADERS, &forbidden, false)
}

fn invalid_pinning_argument(message: &str) -> s3s::S3Error {
    crate::error::AppError::InvalidPinningRequest(message.to_owned()).into()
}

fn single_control_header<'a>(
    headers: &'a http::HeaderMap,
    name: &'static str,
    duplicate_error: &'static str,
) -> S3Result<Option<&'a http::HeaderValue>> {
    let mut values = headers.get_all(name).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(invalid_pinning_argument(duplicate_error));
    }
    Ok(first)
}

fn parse_publication_tags(headers: &http::HeaderMap) -> S3Result<Vec<ObjectTag>> {
    let Some(header) =
        single_control_header(headers, "x-amz-tagging", "duplicate x-amz-tagging header")?
    else {
        return Ok(Vec::new());
    };
    let value = header
        .to_str()
        .map_err(|_| invalid_pinning_argument("invalid x-amz-tagging header"))?;
    crate::pinning::tags::parse_tagging_header(value)
        .map_err(|_| invalid_pinning_argument("invalid x-amz-tagging header"))
}

fn evaluate_publication_policy(
    state: &Arc<AppState>,
    bucket: &str,
    key: &str,
    tags: &[ObjectTag],
) -> S3Result<PublicationPolicy> {
    state
        .pinning
        .policy()
        .evaluate_publication(PublicationContext {
            bucket,
            key,
            tags,
            is_decompress_zip: false,
        })
        .map_err(crate::error::AppError::from)
        .map_err(s3s::S3Error::from)
}

fn copy_publication_tags(
    source_tags: Vec<ObjectTag>,
    headers: &http::HeaderMap,
) -> S3Result<Vec<ObjectTag>> {
    let directive_header = single_control_header(
        headers,
        "x-amz-tagging-directive",
        "duplicate x-amz-tagging-directive header",
    )?;
    let tagging =
        single_control_header(headers, "x-amz-tagging", "duplicate x-amz-tagging header")?;
    let directive = directive_header
        .map(|value| {
            value
                .to_str()
                .map_err(|_| invalid_pinning_argument("invalid tagging directive"))
        })
        .transpose()?
        .unwrap_or("COPY");

    match directive {
        "COPY" => {
            if tagging.is_some_and(|value| !value.as_bytes().is_empty()) {
                return Err(invalid_pinning_argument(
                    "x-amz-tagging must be empty when tagging directive is COPY",
                ));
            }
            Ok(source_tags)
        }
        "REPLACE" => {
            let header = tagging.ok_or_else(|| {
                invalid_pinning_argument(
                    "x-amz-tagging is required when tagging directive is REPLACE",
                )
            })?;
            let value = header
                .to_str()
                .map_err(|_| invalid_pinning_argument("invalid x-amz-tagging header"))?;
            crate::pinning::tags::parse_tagging_header(value)
                .map_err(|_| invalid_pinning_argument("invalid x-amz-tagging header"))
        }
        _ => Err(invalid_pinning_argument("invalid tagging directive")),
    }
}

/// Convert stored JSON metadata back to a `Metadata` map for S3 responses.
fn restore_metadata(json: &Option<serde_json::Value>) -> Option<Metadata> {
    let obj = json.as_ref()?.as_object()?;
    let mut map = Metadata::new();
    for (k, v) in obj {
        if let Some(s) = v.as_str() {
            map.insert(k.clone(), s.to_string());
        }
    }
    if map.is_empty() { None } else { Some(map) }
}

/// Resolve a half-open byte range `[start, end)` from an optional `Range`.
/// `total_size` is the full object size in bytes.
fn resolve_range(range: Option<&Range>, total_size: u64) -> S3Result<(u64, u64)> {
    match range {
        None => Ok((0, total_size)),
        Some(r) => {
            let checked = r
                .check(total_size)
                .map_err(|_| s3s::s3_error!(InvalidRange, "range not satisfiable"))?;
            Ok((checked.start, checked.end))
        }
    }
}

struct AuthenticatedSseCObject {
    key: Arc<crate::crypto::ObjectKey>,
    key_md5: String,
    fingerprint: String,
}

enum CopySourceSseCAuthentication {
    StoredFingerprint(String),
    Legacy(ValidatedSseCHeaders),
}

fn verify_object_sse_c_fingerprint(
    state: &Arc<AppState>,
    fingerprint: &str,
    key: &crate::crypto::ObjectKey,
) -> S3Result<()> {
    let matches = state
        .master_key
        .verify_sse_c_key_fingerprint(fingerprint, key)
        .map_err(|error| {
            s3s::s3_error!(
                InternalError,
                "invalid persisted SSE-C key fingerprint: {error}"
            )
        })?;
    if !matches {
        return Err(s3s::s3_error!(
            AccessDenied,
            "SSE-C customer key does not match object"
        ));
    }
    Ok(())
}

async fn authenticate_legacy_sse_c_plaintext(
    read_client: &crate::kubo::KuboClient,
    obj: &crate::store::entities::object::Model,
    key: Arc<crate::crypto::ObjectKey>,
) -> S3Result<()> {
    let cat = crate::kubo::cat::stream_cat(read_client, &obj.cid, None).await?;
    let decrypted = crate::crypto::chunker::decrypt_chunk_stream(cat, key);
    tokio::pin!(decrypted);
    let mut observed = 0_i64;
    while let Some(chunk) = decrypted.next().await {
        let chunk = chunk.map_err(|error| match error {
            crate::error::AppError::Crypto(_) => {
                s3s::s3_error!(AccessDenied, "SSE-C object authentication failed")
            }
            error @ crate::error::AppError::KuboRpc { .. } => error.into(),
            other => s3s::s3_error!(InternalError, "decrypt: {other}"),
        })?;
        let len = i64::try_from(chunk.len())
            .map_err(|_| s3s::s3_error!(AccessDenied, "SSE-C object size mismatch"))?;
        observed = observed
            .checked_add(len)
            .ok_or_else(|| s3s::s3_error!(AccessDenied, "SSE-C object size mismatch"))?;
    }
    if observed == 0 || observed != obj.size {
        return Err(s3s::s3_error!(AccessDenied, "SSE-C object size mismatch"));
    }
    Ok(())
}

async fn authenticate_sse_c_object(
    state: &Arc<AppState>,
    obj: &crate::store::entities::object::Model,
    read_client: &crate::kubo::KuboClient,
    headers: ValidatedSseCHeaders,
) -> S3Result<AuthenticatedSseCObject> {
    if let Some(fingerprint) = obj.sse_c_key_fingerprint.as_deref() {
        verify_object_sse_c_fingerprint(state, fingerprint, &headers.key)?;
        return Ok(AuthenticatedSseCObject {
            key: Arc::new(headers.key),
            key_md5: headers.key_md5,
            fingerprint: fingerprint.to_owned(),
        });
    }

    let key = Arc::new(headers.key);
    authenticate_legacy_sse_c_plaintext(read_client, obj, key.clone()).await?;
    let candidate = state.master_key.sse_c_key_fingerprint(&key);
    let claimed =
        crate::store::object::claim_sse_c_key_fingerprint(state.store.db(), &obj.id, &candidate)
            .await?;
    let fingerprint = claimed.sse_c_key_fingerprint.ok_or_else(|| {
        s3s::s3_error!(
            InternalError,
            "verified legacy SSE-C object fingerprint was not persisted"
        )
    })?;
    verify_object_sse_c_fingerprint(state, &fingerprint, &key)?;

    Ok(AuthenticatedSseCObject {
        key,
        key_md5: headers.key_md5,
        fingerprint,
    })
}

fn encrypted_object_body<S>(
    cat: S,
    key: Arc<crate::crypto::ObjectKey>,
    range: Option<(u64, u64)>,
    authentication_error: &'static str,
) -> StreamingBlob
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync + Unpin + 'static,
{
    let stream = async_stream::stream! {
        let decrypted = crate::crypto::chunker::decrypt_chunk_stream(cat, key);
        tokio::pin!(decrypted);
        let mut plaintext_offset = 0_u64;
        // For ranges, retaining only the final selected slice ensures fixed
        // Content-Length cannot complete until every later cipher chunk has
        // authenticated. `Bytes::slice` retains at most one plaintext chunk.
        let mut pending_selected = None;
        while let Some(chunk) = decrypted.next().await {
            let bytes = match chunk {
                Ok(bytes) => bytes,
                Err(crate::error::AppError::Crypto(_)) => {
                    yield Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        authentication_error,
                    ));
                    return;
                }
                Err(_) => {
                    yield Err(std::io::Error::other(
                        crate::error::INTERNAL_STORAGE_BACKEND_ERROR,
                    ));
                    return;
                }
            };
            let chunk_len = match u64::try_from(bytes.len()) {
                Ok(chunk_len) => chunk_len,
                Err(_) => {
                    yield Err(std::io::Error::other("decrypted object size exceeds limit"));
                    return;
                }
            };
            let chunk_end = match plaintext_offset.checked_add(chunk_len) {
                Some(chunk_end) => chunk_end,
                None => {
                    yield Err(std::io::Error::other("decrypted object size exceeds limit"));
                    return;
                }
            };

            if let Some((start, end)) = range {
                let selected_start = start.max(plaintext_offset);
                let selected_end = end.min(chunk_end);
                if selected_start < selected_end {
                    let local_start = (selected_start - plaintext_offset) as usize;
                    let local_end = (selected_end - plaintext_offset) as usize;
                    let selected = bytes.slice(local_start..local_end);
                    if let Some(previous) = pending_selected.replace(selected) {
                        yield Ok(previous);
                    }
                }
            } else {
                yield Ok(bytes);
            }
            plaintext_offset = chunk_end;
        }

        if let Some((_, end)) = range
            && plaintext_offset < end
        {
            yield Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "requested range exceeds available decrypted data",
            ));
            return;
        }
        if let Some(final_selected) = pending_selected {
            yield Ok(final_selected);
        }
    };
    StreamingBlob::wrap(stream)
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

struct SelectedS3Object {
    object: object::Model,
    public_version_id: Option<String>,
    residency: crate::residency::ResolvedVersionResidency,
    read_client: crate::kubo::KuboClient,
    tags: Vec<ObjectTag>,
}

async fn select_s3_object(
    state: &Arc<AppState>,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<SelectedS3Object> {
    let selector = match version_id {
        Some(version_id) => VersionSelector::Exact(PublicVersionId::parse_s3(version_id)?),
        None => VersionSelector::Current,
    };
    let db = state.store.db();
    let snapshot = crate::store::object_version::read_snapshot(db, bucket, key, &selector).await?;
    let resolved = snapshot.version;
    match resolved.kind {
        VersionKind::DeleteMarker => Err(AppError::DeleteMarker {
            version_id: resolved.public_version_id,
            created_at: resolved.created_at,
            current: version_id.is_none(),
        }
        .into()),
        VersionKind::Object => {
            let object = resolved.object.ok_or_else(|| {
                s3s::s3_error!(InternalError, "object version index is missing its object")
            })?;
            let residency = snapshot.residency.ok_or_else(|| {
                s3s::s3_error!(InternalError, "object snapshot is missing its residency")
            })?;
            if residency.identity.object_id != object.id || residency.identity.cid != object.cid {
                return Err(s3s::s3_error!(
                    InternalError,
                    "selected object does not match its immutable version residency"
                ));
            }
            let read_client = crate::residency::router::TierClients {
                hot: &state.kubo,
                cold: state.cold_kubo.as_ref(),
            }
            .resolve_read_source(&residency)
            .await?;
            let public_version_id = (snapshot.versioning_state
                != BucketVersioningState::Unversioned)
                .then_some(resolved.public_version_id);
            Ok(SelectedS3Object {
                object,
                public_version_id,
                residency,
                read_client,
                tags: snapshot.tags,
            })
        }
    }
}

pub async fn put_object(
    state: &Arc<AppState>,
    req: S3Request<PutObjectInput>,
) -> S3Result<S3Response<PutObjectOutput>> {
    crate::s3::http::reject_write_conditions(
        &req.headers,
        req.input.if_match.is_some() || req.input.if_none_match.is_some(),
    )?;
    super::storage_class::require_standard_write(req.input.storage_class.as_ref())?;
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let content_type = req.input.content_type.clone();
    let db = state.store.db();
    let tags = parse_publication_tags(&req.headers)?;
    let policy = evaluate_publication_policy(state, bucket, key, &tags)?;

    // Validate the bucket exists.
    let exists = crate::store::bucket::exists(db, bucket).await?;
    if !exists {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {}", bucket));
    }

    let enc_mode = determine_encryption_mode(&req.headers)?;
    let sse_c_headers = match enc_mode {
        EncryptionMode::SseC => Some(extract_sse_c_headers(&req.headers)?),
        EncryptionMode::None | EncryptionMode::SseS3 => None,
    };
    let metadata = extract_custom_metadata(&req.headers);
    let object_id = uuid::Uuid::new_v4().to_string();

    // Extract the body stream (Option<StreamingBlob>).
    let body = req
        .input
        .body
        .ok_or_else(|| s3s::s3_error!(IncompleteBody, "request body is missing"))?;

    let mutation_guard = crate::store::import::ownership::admit_content_mutation(
        db,
        bucket,
        key,
        None,
        crate::import::SupersedeReason::PutObject,
        chrono::Utc::now(),
    )
    .await?;

    crate::store::import::ownership::run_mutation(db, &mutation_guard.clone(), |lease| async move {
        // Wrap the body with a byte counter so we can record the plaintext size.
        let (counter, count_handle) = ByteCounter::new();
        let stream = counter.wrap(body);

        let (cid, encrypted, key_wrap, sse_c_key_fingerprint): (
            String,
            bool,
            Option<String>,
            Option<String>,
        ) = match enc_mode {
            EncryptionMode::None => {
                let cid = crate::kubo::add::stream_add(&state.kubo, stream, 1).await?;
                (cid, false, None, None)
            }
            EncryptionMode::SseS3 => {
                let ok = state.master_key.generate_object_key();
                let wrapped = state
                    .master_key
                    .wrap(&ok)
                    .map_err(|e| s3s::s3_error!(InternalError, "key wrap: {e}"))?;
                // encrypt_chunk_stream requires an Unpin stream; Box::pin satisfies
                // that because Pin<Box<T>> is always Unpin.
                let pinned = Box::pin(stream);
                let encrypted_stream =
                    crate::crypto::chunker::encrypt_chunk_stream(pinned, Arc::new(ok));
                let cid = crate::kubo::add::stream_add(&state.kubo, encrypted_stream, 1).await?;
                (cid, true, Some(wrapped), None)
            }
            EncryptionMode::SseC => {
                let validated = sse_c_headers.expect("SSE-C headers validated before admission");
                let fingerprint = state.master_key.sse_c_key_fingerprint(&validated.key);
                let pinned = Box::pin(stream);
                let encrypted_stream =
                    crate::crypto::chunker::encrypt_chunk_stream(pinned, Arc::new(validated.key));
                let cid = crate::kubo::add::stream_add(&state.kubo, encrypted_stream, 1).await?;
                (cid, true, None, Some(fingerprint))
            }
        };

        let size = count_handle.load(Ordering::Relaxed) as i64;

        // A CID can be shared with an earlier publication, and an RPC failure does
        // not prove Kubo left the pin unchanged. Conservative cleanup here could
        // therefore remove content that another object still needs.
        crate::kubo::pin::pin_add(&state.kubo, &cid).await?;

        let object_created_at = chrono::Utc::now();
        let publication = PublicationRequest {
            object: PublicationObject::from_put(
                object_id,
                bucket,
                key,
                cid.clone(),
                size,
                content_type,
                metadata,
                encrypted,
                key_wrap,
                sse_c_key_fingerprint,
                object_created_at,
            ),
            tags: policy.tags.clone(),
            policy,
            object_target: PinTargetSpec {
                cid: cid.clone(),
                logical_size: size,
            },
        };
        let publication_result = lease
            .commit(crate::store::pinning::publication::publish_standard_object(
                db,
                publication,
                mutation_guard,
                state.pinning.provider_limits(),
            ))
            .await?;

        let server_side_encryption = if enc_mode == EncryptionMode::SseS3 {
            Some(ServerSideEncryption::from_static("AES256"))
        } else {
            None
        };

        let headers = put_object_ipfs_headers(&cid)?;
        Ok(S3Response::with_headers(
            PutObjectOutput {
                e_tag: Some(ETag::Strong(cid.clone())),
                server_side_encryption,
                version_id: publication_result.version_id,
                ..Default::default()
            },
            headers,
        ))
    })
    .await
}

pub async fn get_object(
    state: &Arc<AppState>,
    req: S3Request<GetObjectInput>,
) -> S3Result<S3Response<GetObjectOutput>> {
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let selected = select_s3_object(state, bucket, key, req.input.version_id.as_deref()).await?;
    let obj = selected.object;
    let version_id = selected.public_version_id;
    let storage_class = StorageClass::from_static(selected.residency.storage_class.as_db_str());

    let has_range = req.input.range.is_some();
    let is_sse_c = obj.encrypted && obj.key_wrap.is_none();
    let mut sse_c_auth = if is_sse_c {
        Some(
            authenticate_sse_c_object(
                state,
                &obj,
                &selected.read_client,
                extract_sse_c_headers(&req.headers)?,
            )
            .await?,
        )
    } else {
        None
    };
    let sse_customer_key_md5 = sse_c_auth.as_ref().map(|auth| auth.key_md5.clone());

    let range_ref = req.input.range.as_ref();
    let total_size = u64::try_from(obj.size)
        .map_err(|_| s3s::s3_error!(InternalError, "negative stored object size"))?;
    let (start, end) = resolve_range(range_ref, total_size)?;

    // Build the response body stream.
    let body: StreamingBlob = if obj.encrypted {
        // Resolve the object key used for decryption.
        let ok = if let Some(ref wrapped) = obj.key_wrap {
            // SSE-S3: unwrap with the master key.
            state
                .master_key
                .unwrap(wrapped)
                .map_err(|e| s3s::s3_error!(InternalError, "key unwrap: {e}"))?
        } else {
            return build_sse_c_get_response(
                &obj,
                sse_c_auth.take().ok_or_else(|| {
                    s3s::s3_error!(InternalError, "missing authenticated SSE-C object key")
                })?,
                selected.read_client,
                start,
                end,
                has_range,
                sse_customer_key_md5,
                version_id,
                storage_class,
            )
            .await;
        };

        let ok_arc = Arc::new(ok);
        let cat = crate::kubo::cat::stream_cat(&selected.read_client, &obj.cid, None).await?;

        encrypted_object_body(
            cat,
            ok_arc,
            has_range.then_some((start, end)),
            "decryption failed",
        )
    } else {
        // Plaintext: stream directly from Kubo without collecting into memory.
        let kubo_range = if has_range { Some((start, end)) } else { None };
        let cat = crate::kubo::cat::stream_cat(&selected.read_client, &obj.cid, kubo_range).await?;
        let stream = async_stream::stream! {
            tokio::pin!(cat);
            while let Some(chunk) = cat.next().await {
                yield chunk;
            }
        };
        StreamingBlob::wrap(stream)
    };

    let body = super::object_body::finish_get_body(body, end.saturating_sub(start)).await?;
    let content_length = end.saturating_sub(start) as i64;
    let content_range = if has_range {
        Some(format!(
            "bytes {}-{}/{}",
            start,
            end.saturating_sub(1),
            obj.size
        ))
    } else {
        None
    };

    let server_side_encryption = if obj.encrypted && obj.key_wrap.is_some() {
        Some(ServerSideEncryption::from_static("AES256"))
    } else {
        None
    };

    Ok(S3Response::new(GetObjectOutput {
        body: Some(body),
        content_length: Some(content_length),
        content_type: obj.content_type.clone(),
        e_tag: Some(ETag::Strong(obj.etag.clone())),
        last_modified: Some(Timestamp::from(SystemTime::from(obj.created_at))),
        content_range,
        server_side_encryption,
        metadata: restore_metadata(&obj.metadata),
        version_id,
        storage_class: Some(storage_class),
        ..Default::default()
    }))
}

#[allow(clippy::too_many_arguments)]
async fn build_sse_c_get_response(
    obj: &crate::store::entities::object::Model,
    auth: AuthenticatedSseCObject,
    read_client: crate::kubo::KuboClient,
    start: u64,
    end: u64,
    has_range: bool,
    sse_customer_key_md5: Option<String>,
    version_id: Option<String>,
    storage_class: StorageClass,
) -> S3Result<S3Response<GetObjectOutput>> {
    // Legacy SSE-C objects were fully authenticated before reaching this
    // response. The response still uses a fresh streaming read so ranges never
    // require retaining the whole plaintext in memory.
    let cat = crate::kubo::cat::stream_cat(&read_client, &obj.cid, None).await?;
    let body = encrypted_object_body(
        cat,
        auth.key,
        has_range.then_some((start, end)),
        "SSE-C object authentication failed",
    );

    let body = super::object_body::finish_get_body(body, end.saturating_sub(start)).await?;

    let content_length = end.saturating_sub(start) as i64;
    let content_range =
        has_range.then(|| format!("bytes {}-{}/{}", start, end.saturating_sub(1), obj.size));
    Ok(S3Response::new(GetObjectOutput {
        body: Some(body),
        content_length: Some(content_length),
        content_type: obj.content_type.clone(),
        e_tag: Some(ETag::Strong(obj.etag.clone())),
        last_modified: Some(Timestamp::from(SystemTime::from(obj.created_at))),
        content_range,
        sse_customer_algorithm: Some("AES256".to_owned()),
        sse_customer_key_md5,
        metadata: restore_metadata(&obj.metadata),
        version_id,
        storage_class: Some(storage_class),
        ..Default::default()
    }))
}

pub async fn head_object(
    state: &Arc<AppState>,
    req: S3Request<HeadObjectInput>,
) -> S3Result<S3Response<HeadObjectOutput>> {
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let selected = select_s3_object(state, bucket, key, req.input.version_id.as_deref()).await?;
    let obj = selected.object;
    let version_id = selected.public_version_id;
    let storage_class = StorageClass::from_static(selected.residency.storage_class.as_db_str());
    let sse_c_auth = if obj.encrypted && obj.key_wrap.is_none() {
        Some(
            authenticate_sse_c_object(
                state,
                &obj,
                &selected.read_client,
                extract_sse_c_headers(&req.headers)?,
            )
            .await?,
        )
    } else {
        None
    };
    let total_size = u64::try_from(obj.size)
        .map_err(|_| s3s::s3_error!(InternalError, "negative stored object size"))?;
    let (start, end) = resolve_range(req.input.range.as_ref(), total_size)?;
    let selected_length = end.saturating_sub(start) as i64;

    let server_side_encryption = if obj.encrypted && obj.key_wrap.is_some() {
        Some(ServerSideEncryption::from_static("AES256"))
    } else {
        None
    };

    Ok(S3Response::new(HeadObjectOutput {
        content_length: Some(selected_length),
        content_type: obj.content_type.clone(),
        e_tag: Some(ETag::Strong(obj.etag.clone())),
        last_modified: Some(Timestamp::from(SystemTime::from(obj.created_at))),
        server_side_encryption,
        sse_customer_algorithm: sse_c_auth.as_ref().map(|_| "AES256".to_owned()),
        sse_customer_key_md5: sse_c_auth.map(|auth| auth.key_md5),
        metadata: restore_metadata(&obj.metadata),
        version_id,
        storage_class: Some(storage_class),
        ..Default::default()
    }))
}

pub async fn delete_object(
    state: &Arc<AppState>,
    req: S3Request<DeleteObjectInput>,
) -> S3Result<S3Response<DeleteObjectOutput>> {
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let db = state.store.db();
    let selector = match req.input.version_id.as_deref() {
        Some(version_id) => VersionSelector::Exact(PublicVersionId::parse_s3(version_id)?),
        None => VersionSelector::Current,
    };
    if matches!(&selector, VersionSelector::Exact(_))
        && crate::store::bucket::get_versioning_state(db, bucket).await?
            == BucketVersioningState::Unversioned
    {
        return Err(AppError::InvalidArgument(
            "version IDs are unavailable for an unversioned bucket".to_owned(),
        )
        .into());
    }

    let mutation_guard = crate::store::import::ownership::admit_content_mutation(
        db,
        bucket,
        key,
        None,
        crate::import::SupersedeReason::DeleteObject,
        chrono::Utc::now(),
    )
    .await?;

    crate::store::import::ownership::run_mutation(db, &mutation_guard.clone(), |lease| async move {
        let result = lease
            .commit(
                crate::store::pinning::publication::delete_version_with_leases_guarded(
                    db,
                    bucket,
                    key,
                    selector,
                    mutation_guard,
                    chrono::Utc::now(),
                ),
            )
            .await?;

        Ok(S3Response::new(DeleteObjectOutput {
            delete_marker: (result.created_delete_marker || result.deleted_delete_marker)
                .then_some(true),
            version_id: result.version_id,
            ..Default::default()
        }))
    })
    .await
}

fn delete_objects_item_error(error: &AppError) -> (String, String) {
    match error {
        AppError::NoSuchVersion { .. } => {
            ("NoSuchVersion".to_owned(), "version not found".to_owned())
        }
        AppError::InvalidArgument(_) => ("InvalidArgument".to_owned(), error.to_string()),
        AppError::StaleContentMutation => (
            "OperationAborted".to_owned(),
            "content mutation was superseded by a newer operation".to_owned(),
        ),
        _ => (
            "InternalError".to_owned(),
            "failed to delete object".to_owned(),
        ),
    }
}

pub async fn delete_objects(
    state: &Arc<AppState>,
    req: S3Request<DeleteObjectsInput>,
) -> S3Result<S3Response<DeleteObjectsOutput>> {
    let DeleteObjectsInput { bucket, delete, .. } = req.input;
    if delete.objects.len() > 1000 {
        return Err(s3s::s3_error!(
            MalformedXML,
            "DeleteObjects accepts at most 1000 object identifiers"
        ));
    }
    let db = state.store.db();

    if !crate::store::bucket::exists(db, &bucket).await? {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {}", bucket));
    }

    let quiet = delete.quiet.unwrap_or(false);
    let objects = delete.objects;
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for object in objects {
        let key = object.key;
        let incoming_version_id = object.version_id;
        let selector = match incoming_version_id.as_deref() {
            Some(version_id) => match PublicVersionId::parse_s3(version_id) {
                Ok(version_id) => VersionSelector::Exact(version_id),
                Err(error) => {
                    let (code, message) = delete_objects_item_error(&error);
                    errors.push(Error {
                        code: Some(code),
                        key: Some(key),
                        message: Some(message),
                        version_id: incoming_version_id,
                    });
                    continue;
                }
            },
            None => VersionSelector::Current,
        };
        if matches!(&selector, VersionSelector::Exact(_)) {
            match crate::store::bucket::get_versioning_state(db, &bucket).await {
                Ok(BucketVersioningState::Unversioned) => {
                    let error = AppError::InvalidArgument(
                        "version IDs are unavailable for an unversioned bucket".to_owned(),
                    );
                    let (code, message) = delete_objects_item_error(&error);
                    errors.push(Error {
                        code: Some(code),
                        key: Some(key),
                        message: Some(message),
                        version_id: incoming_version_id,
                    });
                    continue;
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::error!(%bucket, %key, %error, "failed to read versioning state for delete");
                    let (code, message) = delete_objects_item_error(&error);
                    errors.push(Error {
                        code: Some(code),
                        key: Some(key),
                        message: Some(message),
                        version_id: incoming_version_id,
                    });
                    continue;
                }
            }
        }
        let guard = match crate::store::import::ownership::admit_content_mutation(
            db,
            &bucket,
            &key,
            None,
            crate::import::SupersedeReason::DeleteObject,
            chrono::Utc::now(),
        )
        .await
        {
            Ok(guard) => guard,
            Err(error) => {
                tracing::error!(%bucket, %key, %error, "failed to admit object delete");
                let (code, message) = delete_objects_item_error(&error);
                errors.push(Error {
                    code: Some(code),
                    key: Some(key),
                    message: Some(message),
                    version_id: incoming_version_id,
                });
                continue;
            }
        };
        match crate::store::import::ownership::run_mutation(db, &guard.clone(), |lease| {
            let bucket = &bucket;
            let key = &key;
            async move {
                lease
                    .commit(
                        crate::store::pinning::publication::delete_version_with_leases_guarded(
                            db,
                            bucket,
                            key,
                            selector,
                            guard,
                            chrono::Utc::now(),
                        ),
                    )
                    .await
            }
        })
        .await
        {
            Ok(result) if !quiet => {
                let exact_version_id = incoming_version_id.clone();
                let delete_marker_version_id = if result.created_delete_marker {
                    result.version_id.clone()
                } else if result.deleted_delete_marker {
                    exact_version_id.clone()
                } else {
                    None
                };
                deleted.push(DeletedObject {
                    key: Some(key),
                    version_id: exact_version_id,
                    delete_marker: (result.created_delete_marker || result.deleted_delete_marker)
                        .then_some(true),
                    delete_marker_version_id,
                });
            }
            Ok(_) => {}
            Err(error) => {
                tracing::error!(%bucket, %key, %error, "failed to delete object");
                let (code, message) = delete_objects_item_error(&error);
                errors.push(Error {
                    code: Some(code),
                    key: Some(key),
                    message: Some(message),
                    version_id: incoming_version_id,
                });
            }
        }
    }

    Ok(S3Response::new(DeleteObjectsOutput {
        deleted: (!quiet && !deleted.is_empty()).then_some(deleted),
        errors: (!errors.is_empty()).then_some(errors),
        request_charged: None,
    }))
}

pub async fn copy_object(
    state: &Arc<AppState>,
    req: S3Request<CopyObjectInput>,
) -> S3Result<S3Response<CopyObjectOutput>> {
    super::storage_class::require_standard_write(req.input.storage_class.as_ref())?;
    let dst_bucket = &req.input.bucket;
    let dst_key = &req.input.key;
    let db = state.store.db();

    let (src_bucket, src_key, src_version_id) = match req.input.copy_source {
        CopySource::Bucket {
            ref bucket,
            ref key,
            ref version_id,
        } => (
            bucket.to_string(),
            key.to_string(),
            version_id.as_deref().map(str::to_owned),
        ),
        _ => {
            return Err(s3s::s3_error!(InvalidArgument, "unsupported copy source"));
        }
    };

    let selected_source =
        select_s3_object(state, &src_bucket, &src_key, src_version_id.as_deref()).await?;
    let src_obj = selected_source.object;
    let copy_source_version_id = selected_source.public_version_id;
    let tags = copy_publication_tags(selected_source.tags, &req.headers)?;

    // Validate destination bucket exists.
    let dst_exists = crate::store::bucket::exists(db, dst_bucket).await?;
    if !dst_exists {
        return Err(s3s::s3_error!(
            NoSuchBucket,
            "bucket not found: {}",
            dst_bucket
        ));
    }
    let policy = evaluate_publication_policy(state, dst_bucket, dst_key, &tags)?;

    let source_sse_c_headers = extract_copy_source_sse_c_headers(&req.headers)?;
    let source_sse_c_authentication = if src_obj.encrypted && src_obj.key_wrap.is_none() {
        let headers = source_sse_c_headers.ok_or_else(|| {
            s3s::s3_error!(
                InvalidArgument,
                "complete copy-source SSE-C headers are required"
            )
        })?;
        if let Some(fingerprint) = src_obj.sse_c_key_fingerprint.as_deref() {
            verify_object_sse_c_fingerprint(state, fingerprint, &headers.key)?;
            Some(CopySourceSseCAuthentication::StoredFingerprint(
                fingerprint.to_owned(),
            ))
        } else {
            Some(CopySourceSseCAuthentication::Legacy(headers))
        }
    } else {
        if source_sse_c_headers.is_some() {
            return Err(s3s::s3_error!(
                InvalidArgument,
                "copy-source SSE-C headers were provided for a non-SSE-C object"
            ));
        }
        None
    };

    let mutation_guard = crate::store::import::ownership::admit_content_mutation(
        db,
        dst_bucket,
        dst_key,
        None,
        crate::import::SupersedeReason::CopyObject,
        chrono::Utc::now(),
    )
    .await?;

    crate::store::import::ownership::run_mutation(db, &mutation_guard.clone(), |lease| async move {
        let verified_source_fingerprint = match source_sse_c_authentication {
            Some(CopySourceSseCAuthentication::StoredFingerprint(fingerprint)) => Some(fingerprint),
            Some(CopySourceSseCAuthentication::Legacy(headers)) => Some(
                authenticate_sse_c_object(state, &src_obj, &selected_source.read_client, headers)
                    .await?
                    .fingerprint,
            ),
            None => None,
        };

        let hot_receipt = match selected_source.residency.primary.tier {
            crate::residency::KuboTier::Hot => {
                // The source is already local to the publication tier. Re-pin its
                // content-addressed root before creating the independent owner.
                crate::kubo::pin::pin_add(&state.kubo, &src_obj.cid).await?;
                None
            }
            crate::residency::KuboTier::Cold => {
                // A hot pin request is not a transport primitive: the hot node may
                // be deliberately disconnected from cold. Copy and verify the
                // exact encrypted/plain DAG before publishing a STANDARD owner.
                Some(
                    crate::kubo::tier_copy::stream_copy_verified(
                        &selected_source.read_client,
                        &state.kubo,
                        &src_obj.cid,
                        selected_source.residency.physical.node_identity.as_deref(),
                        None,
                        &tokio_util::sync::CancellationToken::new(),
                    )
                    .await?,
                )
            }
        };

        let new_id = uuid::Uuid::new_v4().to_string();
        let object_created_at = chrono::Utc::now();
        let mut object = PublicationObject::from_put(
            new_id,
            dst_bucket,
            dst_key,
            src_obj.cid.clone(),
            src_obj.size,
            src_obj.content_type.clone(),
            src_obj.metadata.clone(),
            src_obj.encrypted,
            src_obj.key_wrap.clone(),
            verified_source_fingerprint,
            object_created_at,
        );
        object.multipart = src_obj.multipart;
        let publication = PublicationRequest {
            object,
            tags: policy.tags.clone(),
            policy,
            object_target: PinTargetSpec {
                cid: src_obj.cid.clone(),
                logical_size: src_obj.size,
            },
        };
        let publication_result = match hot_receipt {
            Some(receipt) => lease
                .commit(
                    crate::store::pinning::publication::publish_standard_object_with_hot_receipt(
                        db,
                        publication,
                        mutation_guard,
                        receipt,
                        state.pinning.provider_limits(),
                    ),
                )
                .await?,
            None => {
                lease
                    .commit(crate::store::pinning::publication::publish_standard_object(
                        db,
                        publication,
                        mutation_guard,
                        state.pinning.provider_limits(),
                    ))
                    .await?
            }
        };

        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(src_obj.etag.clone())),
                last_modified: Some(Timestamp::from(SystemTime::from(object_created_at))),
                ..Default::default()
            }),
            copy_source_version_id,
            version_id: publication_result.version_id,
            ..Default::default()
        }))
    })
    .await
}

use crate::store::entities::object;

struct ListingRequest<'a> {
    bucket: &'a str,
    prefix: &'a str,
    delimiter: Option<&'a str>,
    cursor: Option<&'a str>,
    max_keys: usize,
}

#[derive(Clone, Debug)]
enum ListingEntry {
    Object(object::Model),
    CommonPrefix {
        prefix: String,
        continuation_key: String,
    },
}

#[derive(Clone, Debug)]
struct ListingPage {
    entries: Vec<ListingEntry>,
    is_truncated: bool,
    next_cursor: Option<String>,
}

pub(crate) fn normalized_max_keys(value: Option<i32>) -> usize {
    value.unwrap_or(1000).clamp(1, 1000) as usize
}

async fn build_listing_page(
    state: &Arc<AppState>,
    request: ListingRequest<'_>,
) -> S3Result<ListingPage> {
    let db = state.store.db();
    if !crate::store::bucket::exists(db, request.bucket).await? {
        return Err(s3s::s3_error!(
            NoSuchBucket,
            "bucket not found: {}",
            request.bucket
        ));
    }

    let mut cursor = request
        .cursor
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let prefix_filter = (!request.prefix.is_empty()).then_some(request.prefix);
    let delimiter = request.delimiter.filter(|value| !value.is_empty());
    let batch_limit = (request.max_keys as u64 + 1).max(1000);
    let mut builder = ListingPageBuilder::new(request.prefix, delimiter, request.max_keys);

    'paging: loop {
        let rows = crate::store::object::list(
            db,
            request.bucket,
            prefix_filter,
            cursor.as_deref(),
            batch_limit,
        )
        .await?;
        let exhausted = rows.len() < batch_limit as usize;

        for row in rows {
            let row_key = row.key.clone();
            if builder.push_row(row) == PushListEntryResult::PageComplete {
                break 'paging;
            }
            cursor = Some(row_key);
        }

        if exhausted {
            break;
        }
    }

    Ok(builder.finish())
}

fn rfc3986_url_encode(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    encoded
}

pub(crate) fn url_encoding_requested(encoding_type: Option<&EncodingType>) -> bool {
    encoding_type.is_some_and(|encoding_type| encoding_type.as_str() == EncodingType::URL)
}

pub(crate) fn project_listing_field(value: &str, url_encode: bool) -> String {
    if url_encode {
        rfc3986_url_encode(value)
    } else {
        value.to_owned()
    }
}

pub(crate) fn project_optional_listing_field(
    value: Option<String>,
    url_encode: bool,
) -> Option<String> {
    value.map(|value| project_listing_field(&value, url_encode))
}

async fn listing_dtos(
    state: &Arc<AppState>,
    entries: &[ListingEntry],
    url_encode: bool,
) -> S3Result<(Vec<Object>, Vec<CommonPrefix>)> {
    let objects = entries
        .iter()
        .filter_map(|entry| match entry {
            ListingEntry::Object(model) => Some(model),
            ListingEntry::CommonPrefix { .. } => None,
        })
        .collect::<Vec<_>>();
    let classes = super::storage_class::classes_for_objects(state.store.db(), &objects).await?;
    let mut contents = Vec::new();
    let mut common_prefixes = Vec::new();

    for entry in entries {
        match entry {
            ListingEntry::Object(model) => contents.push(Object {
                key: Some(project_listing_field(&model.key, url_encode)),
                size: Some(model.size),
                e_tag: Some(ETag::Strong(model.etag.clone())),
                last_modified: Some(Timestamp::from(SystemTime::from(model.created_at))),
                storage_class: Some(ObjectStorageClass::from_static(
                    classes[&model.id].as_db_str(),
                )),
                ..Default::default()
            }),
            ListingEntry::CommonPrefix { prefix, .. } => common_prefixes.push(CommonPrefix {
                prefix: Some(project_listing_field(prefix, url_encode)),
            }),
        }
    }

    Ok((contents, common_prefixes))
}

pub async fn list_objects(
    state: &Arc<AppState>,
    req: S3Request<ListObjectsInput>,
) -> S3Result<S3Response<ListObjectsOutput>> {
    let bucket = req.input.bucket.clone();
    let prefix = req.input.prefix.clone();
    let delimiter = req.input.delimiter.clone();
    let marker = req.input.marker.clone();
    let encoding_type = req.input.encoding_type.clone();
    let url_encode = url_encoding_requested(encoding_type.as_ref());
    let max_keys = normalized_max_keys(req.input.max_keys);
    let page = build_listing_page(
        state,
        ListingRequest {
            bucket: &bucket,
            prefix: prefix.as_deref().unwrap_or(""),
            delimiter: delimiter.as_deref(),
            cursor: marker.as_deref(),
            max_keys,
        },
    )
    .await?;
    let next_marker = page
        .is_truncated
        .then(|| page.next_cursor.clone())
        .flatten();
    let (contents, common_prefixes) = listing_dtos(state, &page.entries, url_encode).await?;

    Ok(S3Response::new(ListObjectsOutput {
        name: Some(bucket),
        prefix: Some(project_listing_field(
            &prefix.unwrap_or_default(),
            url_encode,
        )),
        marker: project_optional_listing_field(marker, url_encode),
        max_keys: Some(max_keys as i32),
        is_truncated: Some(page.is_truncated),
        contents: Some(contents),
        common_prefixes: (!common_prefixes.is_empty()).then_some(common_prefixes),
        delimiter: project_optional_listing_field(delimiter, url_encode),
        next_marker: project_optional_listing_field(next_marker, url_encode),
        encoding_type,
        request_charged: None,
    }))
}

pub async fn list_objects_v2(
    state: &Arc<AppState>,
    req: S3Request<ListObjectsV2Input>,
) -> S3Result<S3Response<ListObjectsV2Output>> {
    let bucket = req.input.bucket.clone();
    let prefix = req.input.prefix.clone();
    let delimiter = req.input.delimiter.clone();
    let encoding_type = req.input.encoding_type.clone();
    let url_encode = url_encoding_requested(encoding_type.as_ref());
    let start_after = req.input.start_after.clone();
    let continuation_token = req.input.continuation_token.clone();
    let max_keys = normalized_max_keys(req.input.max_keys);
    let cursor = continuation_token
        .as_deref()
        .filter(|value| !value.is_empty())
        .or_else(|| start_after.as_deref().filter(|value| !value.is_empty()));
    let page = build_listing_page(
        state,
        ListingRequest {
            bucket: &bucket,
            prefix: prefix.as_deref().unwrap_or(""),
            delimiter: delimiter.as_deref(),
            cursor,
            max_keys,
        },
    )
    .await?;
    let (contents, common_prefixes) = listing_dtos(state, &page.entries, url_encode).await?;

    Ok(S3Response::new(ListObjectsV2Output {
        contents: Some(contents),
        common_prefixes: (!common_prefixes.is_empty()).then_some(common_prefixes),
        is_truncated: Some(page.is_truncated),
        continuation_token,
        next_continuation_token: page.next_cursor,
        key_count: Some(page.entries.len() as i32),
        max_keys: Some(max_keys as i32),
        name: Some(bucket),
        prefix: Some(project_listing_field(
            &prefix.unwrap_or_default(),
            url_encode,
        )),
        delimiter: project_optional_listing_field(delimiter, url_encode),
        encoding_type,
        start_after: project_optional_listing_field(start_after, url_encode),
        ..Default::default()
    }))
}

/// Determine the common prefix for `key` under `prefix` and `delimiter`.
///
/// Returns `None` when there is no delimiter, when the key does not start with
/// `prefix`, or when the remaining suffix contains no delimiter.
fn common_prefix_for_key(key: &str, prefix: &str, delimiter: Option<&str>) -> Option<String> {
    let delimiter = delimiter.filter(|value| !value.is_empty())?;
    let rest = key.strip_prefix(prefix)?;
    let index = rest.find(delimiter)?;
    Some(format!("{}{}", prefix, &rest[..index + delimiter.len()]))
}

/// Result of pushing a row into the page builder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PushListEntryResult {
    /// The row was consumed (added or merged) and the page can accept more.
    Continue,
    /// The row was not added because the page is full.
    PageComplete,
}

struct ListingPageBuilder {
    prefix: String,
    delimiter: Option<String>,
    max_keys: usize,
    entries: Vec<ListingEntry>,
    common_prefix_positions: HashMap<String, usize>,
    last_consumed_key: Option<String>,
    is_truncated: bool,
}

impl ListingPageBuilder {
    fn new(prefix: &str, delimiter: Option<&str>, max_keys: usize) -> Self {
        Self {
            prefix: prefix.to_owned(),
            delimiter: delimiter.map(str::to_owned),
            max_keys,
            entries: Vec::new(),
            common_prefix_positions: HashMap::new(),
            last_consumed_key: None,
            is_truncated: false,
        }
    }

    fn push_row(&mut self, row: object::Model) -> PushListEntryResult {
        let key = row.key.clone();

        if let Some(common_prefix) =
            common_prefix_for_key(&key, &self.prefix, self.delimiter.as_deref())
        {
            if let Some(&position) = self.common_prefix_positions.get(&common_prefix) {
                if let ListingEntry::CommonPrefix {
                    ref mut continuation_key,
                    ..
                } = self.entries[position]
                {
                    *continuation_key = key.clone();
                }
                self.last_consumed_key = Some(key);
                return PushListEntryResult::Continue;
            }

            if self.entries.len() >= self.max_keys {
                self.is_truncated = true;
                return PushListEntryResult::PageComplete;
            }

            let position = self.entries.len();
            self.entries.push(ListingEntry::CommonPrefix {
                prefix: common_prefix.clone(),
                continuation_key: key.clone(),
            });
            self.common_prefix_positions.insert(common_prefix, position);
            self.last_consumed_key = Some(key);
            return PushListEntryResult::Continue;
        }

        if self.entries.len() >= self.max_keys {
            self.is_truncated = true;
            return PushListEntryResult::PageComplete;
        }

        self.entries.push(ListingEntry::Object(row));
        self.last_consumed_key = Some(key);
        PushListEntryResult::Continue
    }

    fn finish(mut self) -> ListingPage {
        let next_cursor = if self.is_truncated {
            self.last_consumed_key.take()
        } else {
            None
        };
        ListingPage {
            entries: self.entries,
            is_truncated: self.is_truncated,
            next_cursor,
        }
    }
}

#[cfg(test)]
fn fold_listing_rows(
    rows: Vec<object::Model>,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: usize,
) -> ListingPage {
    let mut builder = ListingPageBuilder::new(prefix, delimiter, max_keys);
    for row in rows {
        if builder.push_row(row) == PushListEntryResult::PageComplete {
            break;
        }
    }
    builder.finish()
}

#[cfg(test)]
impl ListingPage {
    fn object_keys(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter_map(|entry| match entry {
                ListingEntry::Object(model) => Some(model.key.as_str()),
                ListingEntry::CommonPrefix { .. } => None,
            })
            .collect()
    }

    fn common_prefixes(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter_map(|entry| match entry {
                ListingEntry::Object(_) => None,
                ListingEntry::CommonPrefix { prefix, .. } => Some(prefix.as_str()),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::entities::{
        object, object_tag, object_version, physical_residency, pin_job, pin_lease,
        pin_lease_target, pin_provider_usage, remote_pin, version_residency,
    };
    use chrono::Utc;
    use sea_orm::{
        ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, IntoActiveModel,
        PaginatorTrait, QueryFilter, QueryOrder, Set, TransactionTrait,
    };

    mod copy_hot_receipt_tests;

    async fn test_state(kubo_uri: String) -> Arc<AppState> {
        use sea_orm::Database;

        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();

        Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new(kubo_uri),
            cold_kubo: None,
            store: crate::store::Store::new(db),
            credentials: HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
            pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
        })
    }

    fn configured_coordinator(
        endpoint: &str,
        trigger: &str,
        provider_mode: &str,
        prefix: &str,
    ) -> Arc<crate::pinning::coordinator::PinningCoordinator> {
        use crate::config::{PinningConfig, PolicyConfig, ProviderConfig};
        use crate::pinning::config::ValidatedPinningConfig;

        let provider = |name: &str, kind: &str, priority: u32| ProviderConfig {
            name: name.to_owned(),
            kind: kind.to_owned(),
            token_env: Some(format!("{name}_TOKEN")),
            endpoint: Some(endpoint.to_owned()),
            api: None,
            strategy: None,
            upload_endpoint: None,
            enabled: true,
            priority,
            max_bytes: 10_000,
            max_pins: 100,
            requests_per_second: None,
        };
        let validated = ValidatedPinningConfig::from_raw(
            &PinningConfig {
                worker_interval: "5s".to_owned(),
                worker_concurrency: 2,
                providers: vec![
                    provider("alpha", "pinata", 1),
                    provider("beta", "filebase", 2),
                ],
                policies: vec![PolicyConfig {
                    bucket: "bucket".to_owned(),
                    prefix: prefix.to_owned(),
                    trigger: trigger.to_owned(),
                    provider_mode: provider_mode.to_owned(),
                    providers: vec!["alpha".to_owned(), "beta".to_owned()],
                    default_duration: "1h".to_owned(),
                    max_duration: "24h".to_owned(),
                    allow_decompressed: false,
                }],
            },
            |_| Some("test-provider-token".to_owned()),
        )
        .unwrap();
        crate::pinning::coordinator::PinningCoordinator::build(validated).unwrap()
    }

    async fn pinning_state(
        kubo_uri: String,
        trigger: &str,
        provider_mode: &str,
        prefix: &str,
    ) -> Arc<AppState> {
        pinning_state_with_cold(kubo_uri, None, trigger, provider_mode, prefix).await
    }

    async fn pinning_state_with_cold(
        kubo_uri: String,
        cold_kubo_uri: Option<String>,
        trigger: &str,
        provider_mode: &str,
        prefix: &str,
    ) -> Arc<AppState> {
        use sea_orm::Database;

        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();

        Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new(kubo_uri.clone()),
            cold_kubo: cold_kubo_uri.map(crate::kubo::KuboClient::new),
            store: crate::store::Store::new(db),
            credentials: HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
            pinning: configured_coordinator(&kubo_uri, trigger, provider_mode, prefix),
        })
    }

    async fn kubo_server(cid: &str) -> wiremock::MockServer {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!("{{\"Hash\":\"{cid}\",\"Size\":\"4\"}}\n")),
            )
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{cid}\"]}}")),
            )
            .mount(&kubo)
            .await;
        kubo
    }

    fn s3_request<T>(
        input: T,
        method: http::Method,
        uri: &str,
        headers: http::HeaderMap,
    ) -> S3Request<T> {
        S3Request {
            input,
            method,
            uri: uri.parse().unwrap(),
            headers,
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn put_request(key: &str, tagging: Option<&str>) -> S3Request<PutObjectInput> {
        let mut headers = http::HeaderMap::new();
        if let Some(tagging) = tagging {
            headers.insert("x-amz-tagging", tagging.parse().unwrap());
        }
        s3_request(
            PutObjectInput {
                body: Some(StreamingBlob::from(s3s::Body::from(Bytes::from_static(
                    b"body",
                )))),
                bucket: "bucket".to_owned(),
                content_type: Some("text/plain".to_owned()),
                key: key.to_owned(),
                ..Default::default()
            },
            http::Method::PUT,
            &format!("/bucket/{key}"),
            headers,
        )
    }

    fn copy_request(
        source_key: &str,
        destination_key: &str,
        directive: Option<&str>,
        tagging: Option<&str>,
    ) -> S3Request<CopyObjectInput> {
        let mut headers = http::HeaderMap::new();
        if let Some(directive) = directive {
            headers.insert("x-amz-tagging-directive", directive.parse().unwrap());
        }
        if let Some(tagging) = tagging {
            headers.insert("x-amz-tagging", tagging.parse().unwrap());
        }
        s3_request(
            CopyObjectInput::builder()
                .bucket("bucket".to_owned())
                .copy_source(CopySource::Bucket {
                    bucket: "bucket".into(),
                    key: source_key.into(),
                    version_id: None,
                })
                .key(destination_key.to_owned())
                .build()
                .unwrap(),
            http::Method::PUT,
            &format!("/bucket/{destination_key}"),
            headers,
        )
    }

    fn copy_request_version(
        source_key: &str,
        source_version_id: Option<&str>,
        destination_key: &str,
        headers: http::HeaderMap,
    ) -> S3Request<CopyObjectInput> {
        s3_request(
            CopyObjectInput::builder()
                .bucket("bucket".to_owned())
                .copy_source(CopySource::Bucket {
                    bucket: "bucket".into(),
                    key: source_key.into(),
                    version_id: source_version_id.map(Into::into),
                })
                .key(destination_key.to_owned())
                .build()
                .unwrap(),
            http::Method::PUT,
            &format!("/bucket/{destination_key}"),
            headers,
        )
    }

    fn get_object_request(
        key: &str,
        version_id: Option<&str>,
        headers: http::HeaderMap,
    ) -> S3Request<GetObjectInput> {
        s3_request(
            GetObjectInput {
                bucket: "bucket".to_owned(),
                key: key.to_owned(),
                version_id: version_id.map(str::to_owned),
                ..Default::default()
            },
            http::Method::GET,
            &format!("/bucket/{key}"),
            headers,
        )
    }

    fn head_object_request(
        key: &str,
        version_id: Option<&str>,
        headers: http::HeaderMap,
    ) -> S3Request<HeadObjectInput> {
        s3_request(
            HeadObjectInput {
                bucket: "bucket".to_owned(),
                key: key.to_owned(),
                version_id: version_id.map(str::to_owned),
                ..Default::default()
            },
            http::Method::HEAD,
            &format!("/bucket/{key}"),
            headers,
        )
    }

    async fn versioned_read_state() -> (Arc<AppState>, wiremock::MockServer) {
        let kubo = kubo_server("unused").await;
        let state = pinning_state(kubo.uri(), "request", "one", "").await;
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            crate::store::object_version::BucketVersioningState::Enabled,
        )
        .await
        .unwrap();
        (state, kubo)
    }

    async fn mount_cat_body(kubo: &wiremock::MockServer, cid: &str, body: Vec<u8>) {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, ResponseTemplate};

        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .and(query_param("arg", cid))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(kubo)
            .await;
    }

    async fn mount_node_identity(kubo: &wiremock::MockServer, node_identity: &str) {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, ResponseTemplate};

        Mock::given(method("POST"))
            .and(path("/api/v0/id"))
            .and(query_param("peerid-base", "b58mh"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!(r#"{{"ID":"{node_identity}"}}"#)),
            )
            .mount(kubo)
            .await;
    }

    async fn read_get_body(response: S3Response<GetObjectOutput>) -> Vec<u8> {
        let mut body = response.output.body.expect("GetObject body");
        let mut bytes = Vec::new();
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk.expect("GetObject body chunk"));
        }
        bytes
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_versioned_read_object(
        state: &Arc<AppState>,
        object_id: &str,
        key: &str,
        cid: &str,
        encrypted: bool,
        key_wrap: Option<String>,
        sse_c_key_fingerprint: Option<String>,
        tags: Vec<crate::pinning::tags::ObjectTag>,
    ) -> PublicationResult {
        publish_versioned_read_object_with_size(
            state,
            object_id,
            key,
            cid,
            4,
            encrypted,
            key_wrap,
            sse_c_key_fingerprint,
            tags,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_versioned_read_object_with_size(
        state: &Arc<AppState>,
        object_id: &str,
        key: &str,
        cid: &str,
        size: i64,
        encrypted: bool,
        key_wrap: Option<String>,
        sse_c_key_fingerprint: Option<String>,
        tags: Vec<crate::pinning::tags::ObjectTag>,
    ) -> PublicationResult {
        let policy = state
            .pinning
            .policy()
            .evaluate_publication(PublicationContext {
                bucket: "bucket",
                key,
                tags: &tags,
                is_decompress_zip: false,
            })
            .unwrap();
        crate::store::pinning::publication::publish_object(
            state.store.db(),
            PublicationRequest {
                object: PublicationObject::from_put(
                    object_id.to_owned(),
                    "bucket",
                    key,
                    cid.to_owned(),
                    size,
                    Some("text/plain".to_owned()),
                    None,
                    encrypted,
                    key_wrap,
                    sse_c_key_fingerprint,
                    Utc::now(),
                ),
                tags: policy.tags.clone(),
                policy,
                object_target: PinTargetSpec {
                    cid: cid.to_owned(),
                    logical_size: size,
                },
            },
            state.pinning.provider_limits(),
        )
        .await
        .unwrap()
    }

    async fn move_version_to_verified_cold(
        state: &Arc<AppState>,
        key: &str,
        public_version_id: Option<&str>,
        node_identity: &str,
    ) {
        let mut query = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq("bucket"))
            .filter(object_version::Column::Key.eq(key));
        query = match public_version_id {
            Some("null") | None => query.filter(object_version::Column::VersionId.is_null()),
            Some(version_id) => query.filter(object_version::Column::VersionId.eq(version_id)),
        };
        let version = query
            .one(state.store.db())
            .await
            .unwrap()
            .expect("selected object version");
        let object_id = version.object_id.expect("content version object owner");
        let object = object::Entity::find_by_id(&object_id)
            .one(state.store.db())
            .await
            .unwrap()
            .expect("content object");
        let now = Utc::now();
        let receipt = serde_json::to_string(&crate::kubo::LocalResidencyVerificationReceipt {
            node_identity: node_identity.to_owned(),
            cid: object.cid.clone(),
        })
        .unwrap();

        physical_residency::Entity::insert(physical_residency::ActiveModel {
            tier: Set("cold".to_owned()),
            cid: Set(object.cid.clone()),
            node_identity: Set(Some(node_identity.to_owned())),
            verification_state: Set("verified".to_owned()),
            verification_receipt: Set(Some(receipt)),
            verified_at: Set(Some(now)),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .exec(state.store.db())
        .await
        .unwrap();

        let residency = version_residency::Entity::find_by_id(&version.id)
            .one(state.store.db())
            .await
            .unwrap()
            .expect("version residency");
        let mut residency = residency.into_active_model();
        residency.primary_tier = Set("cold".to_owned());
        residency.storage_class = Set("STANDARD_IA".to_owned());
        residency.revision = Set(2);
        residency.updated_at = Set(now);
        residency.update(state.store.db()).await.unwrap();
    }

    async fn install_current_marker(state: &Arc<AppState>, key: &str) -> String {
        state
            .store
            .db()
            .transaction(move |txn| {
                let key = key.to_owned();
                Box::pin(async move {
                    crate::store::object_version::install_delete_marker(
                        txn,
                        crate::store::object_version::BucketVersioningState::Enabled,
                        "bucket",
                        &key,
                        Utc::now(),
                    )
                    .await
                })
            })
            .await
            .unwrap()
    }

    fn delete_object_version_request(
        key: &str,
        version_id: Option<&str>,
    ) -> S3Request<DeleteObjectInput> {
        s3_request(
            DeleteObjectInput {
                bucket: "bucket".to_owned(),
                key: key.to_owned(),
                version_id: version_id.map(str::to_owned),
                ..Default::default()
            },
            http::Method::DELETE,
            &format!("/bucket/{key}"),
            http::HeaderMap::new(),
        )
    }

    fn delete_object_request(key: &str) -> S3Request<DeleteObjectInput> {
        delete_object_version_request(key, None)
    }

    async fn lease_sources_for_latest(state: &Arc<AppState>, key: &str) -> Vec<String> {
        let latest = crate::store::object::get_latest(state.store.db(), "bucket", key)
            .await
            .unwrap();
        pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq(latest.id))
            .filter(pin_lease::Column::State.eq("active"))
            .order_by_asc(pin_lease::Column::Source)
            .all(state.store.db())
            .await
            .unwrap()
            .into_iter()
            .map(|lease| lease.source)
            .collect()
    }

    async fn pending_operations(state: &Arc<AppState>) -> Vec<String> {
        pin_job::Entity::find()
            .filter(pin_job::Column::State.eq("pending"))
            .order_by_asc(pin_job::Column::Provider)
            .all(state.store.db())
            .await
            .unwrap()
            .into_iter()
            .map(|job| job.operation)
            .collect()
    }

    async fn publish_seed(
        state: &Arc<AppState>,
        id: &str,
        key: &str,
        cid: &str,
        tags: Vec<crate::pinning::tags::ObjectTag>,
    ) {
        use crate::pinning::policy::PublicationContext;
        use crate::store::pinning::publication::{
            PinTargetSpec, PublicationObject, PublicationRequest,
        };

        let policy = state
            .pinning
            .policy()
            .evaluate_publication(PublicationContext {
                bucket: "bucket",
                key,
                tags: &tags,
                is_decompress_zip: false,
            })
            .unwrap();
        let object = PublicationObject::from_put(
            id.to_owned(),
            "bucket",
            key,
            cid.to_owned(),
            4,
            Some("text/plain".to_owned()),
            None,
            false,
            None,
            None,
            Utc::now(),
        );
        crate::store::pinning::publication::publish_object(
            state.store.db(),
            PublicationRequest {
                object,
                tags: policy.tags.clone(),
                policy,
                object_target: PinTargetSpec {
                    cid: cid.to_owned(),
                    logical_size: 4,
                },
            },
            state.pinning.provider_limits(),
        )
        .await
        .unwrap();
    }

    async fn seed_copy_source(
        state: &Arc<AppState>,
        key: &str,
        cid: &str,
        tags: &[crate::pinning::tags::ObjectTag],
    ) {
        crate::store::object::upsert(
            state.store.db(),
            &format!("source-{key}"),
            "bucket",
            key,
            cid,
            4,
            Some("text/plain"),
            cid,
            None,
            false,
            None,
            None,
            false,
        )
        .await
        .unwrap();
        let object = crate::store::object::get_latest(state.store.db(), "bucket", key)
            .await
            .unwrap();
        state
            .store
            .db()
            .transaction(move |txn| {
                Box::pin(async move {
                    crate::store::object_version::install_content_version(
                        txn,
                        crate::store::object_version::BucketVersioningState::Unversioned,
                        &object,
                        Utc::now(),
                    )
                    .await
                })
            })
            .await
            .unwrap();
        crate::store::pinning::tags::replace_object_tags(
            state.store.db(),
            &format!("source-{key}"),
            tags,
        )
        .await
        .unwrap();
    }

    fn valid_sse_c_headers() -> http::HeaderMap {
        use base64::Engine;

        let key = [0x42; 32];
        let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
        let key_md5 = base64::engine::general_purpose::STANDARD.encode(md5::compute(key).as_ref());
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-amz-server-side-encryption-customer-algorithm",
            http::HeaderValue::from_static("AES256"),
        );
        headers.insert(
            "x-amz-server-side-encryption-customer-key",
            key_b64.parse().unwrap(),
        );
        headers.insert(
            "x-amz-server-side-encryption-customer-key-md5",
            key_md5.parse().unwrap(),
        );
        headers
    }

    fn assert_invalid_argument<T>(result: S3Result<T>) {
        assert_eq!(
            result
                .err()
                .expect("expected InvalidArgument")
                .code()
                .as_str(),
            "InvalidArgument"
        );
    }

    #[test]
    fn determine_encryption_mode_requires_complete_unmixed_sse_c_headers() {
        let complete_headers = valid_sse_c_headers();
        assert_eq!(
            determine_encryption_mode(&complete_headers).unwrap(),
            EncryptionMode::SseC
        );

        for missing_header in [
            "x-amz-server-side-encryption-customer-algorithm",
            "x-amz-server-side-encryption-customer-key",
            "x-amz-server-side-encryption-customer-key-md5",
        ] {
            let mut headers = complete_headers.clone();
            headers.remove(missing_header);
            assert_invalid_argument(determine_encryption_mode(&headers));
        }

        let mut mixed_headers = complete_headers.clone();
        mixed_headers.insert(
            "x-amz-server-side-encryption",
            http::HeaderValue::from_static("AES256"),
        );
        assert_invalid_argument(determine_encryption_mode(&mixed_headers));

        let mut unsupported_algorithm = complete_headers;
        unsupported_algorithm.insert(
            "x-amz-server-side-encryption-customer-algorithm",
            http::HeaderValue::from_static("AES128"),
        );
        assert_invalid_argument(determine_encryption_mode(&unsupported_algorithm));
    }

    #[test]
    fn extract_sse_c_key_rejects_malformed_values() {
        use base64::Engine;

        let mut missing_algorithm = valid_sse_c_headers();
        missing_algorithm.remove("x-amz-server-side-encryption-customer-algorithm");

        let mut unsupported_algorithm = valid_sse_c_headers();
        unsupported_algorithm.insert(
            "x-amz-server-side-encryption-customer-algorithm",
            http::HeaderValue::from_static("AES128"),
        );

        let mut mixed_with_sse_s3 = valid_sse_c_headers();
        mixed_with_sse_s3.insert(
            "x-amz-server-side-encryption",
            http::HeaderValue::from_static("AES256"),
        );

        let mut invalid_key_base64 = valid_sse_c_headers();
        invalid_key_base64.insert(
            "x-amz-server-side-encryption-customer-key",
            http::HeaderValue::from_static("not base64"),
        );

        let mut short_key = valid_sse_c_headers();
        short_key.insert(
            "x-amz-server-side-encryption-customer-key",
            base64::engine::general_purpose::STANDARD
                .encode([0x42; 31])
                .parse()
                .unwrap(),
        );

        let mut invalid_md5_base64 = valid_sse_c_headers();
        invalid_md5_base64.insert(
            "x-amz-server-side-encryption-customer-key-md5",
            http::HeaderValue::from_static("not base64"),
        );

        let mut short_md5 = valid_sse_c_headers();
        short_md5.insert(
            "x-amz-server-side-encryption-customer-key-md5",
            base64::engine::general_purpose::STANDARD
                .encode([0; 15])
                .parse()
                .unwrap(),
        );

        let mut incorrect_md5 = valid_sse_c_headers();
        incorrect_md5.insert(
            "x-amz-server-side-encryption-customer-key-md5",
            base64::engine::general_purpose::STANDARD
                .encode([0; 16])
                .parse()
                .unwrap(),
        );

        for headers in [
            missing_algorithm,
            unsupported_algorithm,
            mixed_with_sse_s3,
            invalid_key_base64,
            short_key,
            invalid_md5_base64,
            short_md5,
            incorrect_md5,
        ] {
            assert_invalid_argument(extract_sse_c_key(&headers));
        }
    }

    #[test]
    fn put_object_ipfs_headers_include_cid_and_reject_invalid_values() {
        let headers = put_object_ipfs_headers("QmValidCid").expect("valid CID headers");
        assert_eq!(
            headers["x-amz-meta-ipfs-cid"]
                .to_str()
                .expect("CID header text"),
            "QmValidCid"
        );
        assert_eq!(
            headers["x-amz-meta-ipfs-url"]
                .to_str()
                .expect("IPFS URL header text"),
            "ipfs://QmValidCid"
        );

        let error =
            put_object_ipfs_headers("QmInvalid\nCid").expect_err("invalid CID header must fail");
        assert_eq!(error.code().as_str(), "InternalError");
    }

    const COLD_NODE_ID: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
    const TEST_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    const TEST_CID_ALT: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";

    #[tokio::test]
    async fn current_and_exact_version_reads_use_their_immutable_residency() {
        let hot = wiremock::MockServer::start().await;
        let cold = wiremock::MockServer::start().await;
        mount_node_identity(&cold, COLD_NODE_ID).await;
        let state =
            pinning_state_with_cold(hot.uri(), Some(cold.uri()), "request", "one", "").await;
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            BucketVersioningState::Enabled,
        )
        .await
        .unwrap();

        let historical = publish_versioned_read_object(
            &state,
            "tiered-old",
            "tiered.txt",
            TEST_CID,
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        let historical_version = historical.version_id.expect("historical version ID");
        publish_versioned_read_object(
            &state,
            "tiered-current",
            "tiered.txt",
            TEST_CID_ALT,
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        move_version_to_verified_cold(
            &state,
            "tiered.txt",
            Some(&historical_version),
            COLD_NODE_ID,
        )
        .await;
        mount_cat_body(&hot, TEST_CID_ALT, b"new!".to_vec()).await;
        mount_cat_body(&cold, TEST_CID, b"old!".to_vec()).await;

        let current = get_object(
            &state,
            get_object_request("tiered.txt", None, http::HeaderMap::new()),
        )
        .await
        .expect("current hot read");
        assert_eq!(read_get_body(current).await, b"new!");

        let historical = get_object(
            &state,
            get_object_request(
                "tiered.txt",
                Some(&historical_version),
                http::HeaderMap::new(),
            ),
        )
        .await
        .expect("historical cold read");
        assert_eq!(read_get_body(historical).await, b"old!");

        let hot_requests = hot.received_requests().await.unwrap();
        assert_eq!(
            hot_requests
                .iter()
                .filter(|request| request.url.path() == "/api/v0/cat")
                .count(),
            1
        );
        let cold_requests = cold.received_requests().await.unwrap();
        assert_eq!(
            cold_requests
                .iter()
                .filter(|request| request.url.path() == "/api/v0/cat")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn explicit_null_version_can_read_from_cold_without_hot_fallback() {
        let hot = wiremock::MockServer::start().await;
        let cold = wiremock::MockServer::start().await;
        mount_node_identity(&cold, COLD_NODE_ID).await;
        let state =
            pinning_state_with_cold(hot.uri(), Some(cold.uri()), "request", "one", "").await;
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            BucketVersioningState::Suspended,
        )
        .await
        .unwrap();
        publish_versioned_read_object(
            &state,
            "tiered-null",
            "null.txt",
            TEST_CID,
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        move_version_to_verified_cold(&state, "null.txt", Some("null"), COLD_NODE_ID).await;
        mount_cat_body(&cold, TEST_CID, b"null".to_vec()).await;

        let response = get_object(
            &state,
            get_object_request("null.txt", Some("null"), http::HeaderMap::new()),
        )
        .await
        .expect("explicit null cold read");
        assert_eq!(response.output.version_id.as_deref(), Some("null"));
        assert_eq!(read_get_body(response).await, b"null");
        assert!(
            hot.received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() != "/api/v0/cat")
        );
    }

    #[tokio::test]
    async fn a_cold_read_failure_never_falls_back_to_hot() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, ResponseTemplate};

        let hot = wiremock::MockServer::start().await;
        let cold = wiremock::MockServer::start().await;
        mount_node_identity(&cold, COLD_NODE_ID).await;
        let state =
            pinning_state_with_cold(hot.uri(), Some(cold.uri()), "request", "one", "").await;
        publish_versioned_read_object(
            &state,
            "cold-failure",
            "cold-failure.txt",
            TEST_CID,
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        move_version_to_verified_cold(&state, "cold-failure.txt", None, COLD_NODE_ID).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .and(query_param("arg", TEST_CID))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&cold)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .and(query_param("arg", TEST_CID))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hot!"))
            .expect(0)
            .mount(&hot)
            .await;

        let error = get_object(
            &state,
            get_object_request("cold-failure.txt", None, http::HeaderMap::new()),
        )
        .await
        .expect_err("cat establishment must fail before response headers");
        assert_eq!(error.code().as_str(), "InternalError");
    }

    #[tokio::test]
    async fn cold_sse_s3_and_sse_c_gets_use_cold_for_full_and_range_reads() {
        let hot = wiremock::MockServer::start().await;
        let cold = wiremock::MockServer::start().await;
        mount_node_identity(&cold, COLD_NODE_ID).await;
        let state =
            pinning_state_with_cold(hot.uri(), Some(cold.uri()), "request", "one", "").await;

        let sse_s3_key = state.master_key.generate_object_key();
        publish_versioned_read_object(
            &state,
            "cold-sse-s3",
            "cold-sse-s3.bin",
            TEST_CID,
            true,
            Some(state.master_key.wrap(&sse_s3_key).unwrap()),
            None,
            Vec::new(),
        )
        .await;
        move_version_to_verified_cold(&state, "cold-sse-s3.bin", None, COLD_NODE_ID).await;
        mount_cat_body(
            &cold,
            TEST_CID,
            crate::crypto::aes_gcm::encrypt_chunk(&sse_s3_key, &[4; 12], b"cold")
                .unwrap()
                .to_vec(),
        )
        .await;

        let sse_c_key = crate::crypto::ObjectKey { bytes: [0x42; 32] };
        publish_versioned_read_object(
            &state,
            "cold-sse-c",
            "cold-sse-c.bin",
            TEST_CID_ALT,
            true,
            None,
            Some(state.master_key.sse_c_key_fingerprint(&sse_c_key)),
            Vec::new(),
        )
        .await;
        move_version_to_verified_cold(&state, "cold-sse-c.bin", None, COLD_NODE_ID).await;
        mount_cat_body(
            &cold,
            TEST_CID_ALT,
            crate::crypto::aes_gcm::encrypt_chunk(&sse_c_key, &[5; 12], b"cold")
                .unwrap()
                .to_vec(),
        )
        .await;

        for (key, headers) in [
            ("cold-sse-s3.bin", http::HeaderMap::new()),
            ("cold-sse-c.bin", valid_sse_c_headers()),
        ] {
            let full = get_object(&state, get_object_request(key, None, headers.clone()))
                .await
                .expect("cold encrypted full read");
            assert_eq!(read_get_body(full).await, b"cold");

            let mut range_request = get_object_request(key, None, headers);
            range_request.input.range = Some(Range::Int {
                first: 1,
                last: Some(2),
            });
            let range = get_object(&state, range_request)
                .await
                .expect("cold encrypted range read");
            assert_eq!(range.output.content_length, Some(2));
            assert_eq!(range.output.content_range.as_deref(), Some("bytes 1-2/4"));
            assert_eq!(read_get_body(range).await, b"ol");
        }

        assert!(
            hot.received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() != "/api/v0/cat")
        );
        let requests = cold.received_requests().await.unwrap();
        let cats: Vec<_> = requests
            .iter()
            .filter(|request| request.url.path() == "/api/v0/cat")
            .collect();
        assert_eq!(cats.len(), 4);
        for request in cats {
            assert!(
                request
                    .url
                    .query_pairs()
                    .any(|(key, value)| key == "offline" && value == "true")
            );
            assert!(
                request
                    .url
                    .query_pairs()
                    .all(|(key, _)| key != "offset" && key != "length"),
                "encrypted Range still decrypts full ciphertext before slicing"
            );
        }
    }

    #[tokio::test]
    async fn encrypted_range_streams_selected_bytes_and_drains_tail_authentication() {
        let hot = wiremock::MockServer::start().await;
        let state = pinning_state(hot.uri(), "request", "one", "").await;
        let key = state.master_key.generate_object_key();
        let first_plaintext = vec![0x5a; crate::crypto::chunker::CHUNK_SIZE];
        let mut ciphertext =
            crate::crypto::aes_gcm::encrypt_chunk(&key, &[1; 12], &first_plaintext)
                .unwrap()
                .to_vec();
        let mut corrupt_tail = crate::crypto::aes_gcm::encrypt_chunk(&key, &[2; 12], b"tail")
            .unwrap()
            .to_vec();
        *corrupt_tail.last_mut().unwrap() ^= 0xff;
        ciphertext.extend_from_slice(&corrupt_tail);
        let object_size = i64::try_from(first_plaintext.len() + 4).unwrap();
        publish_versioned_read_object_with_size(
            &state,
            "encrypted-range-drain",
            "encrypted-range.bin",
            TEST_CID,
            object_size,
            true,
            Some(state.master_key.wrap(&key).unwrap()),
            None,
            Vec::new(),
        )
        .await;
        mount_cat_body(&hot, TEST_CID, ciphertext).await;
        let mut request = get_object_request("encrypted-range.bin", None, http::HeaderMap::new());
        request.input.range = Some(Range::Int {
            first: 0,
            last: Some(3),
        });

        let response = get_object(&state, request)
            .await
            .expect("encrypted range response");
        let mut body = response.output.body.expect("encrypted range body");
        let tail_error = body
            .next()
            .await
            .expect("the unselected tail must be authenticated before the final selected bytes")
            .expect_err("corrupt tail must fail before completing Content-Length");
        assert_eq!(
            tail_error
                .downcast_ref::<std::io::Error>()
                .expect("stream error retains I/O kind")
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
        assert!(body.next().await.is_none());
    }

    #[tokio::test]
    async fn encrypted_cat_establishment_failure_is_an_s3_error_before_response_headers() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, ResponseTemplate};

        let hot = wiremock::MockServer::start().await;
        let state = pinning_state(hot.uri(), "request", "one", "").await;
        let key = state.master_key.generate_object_key();
        publish_versioned_read_object(
            &state,
            "encrypted-preheader-failure",
            "encrypted-preheader-failure.bin",
            TEST_CID,
            true,
            Some(state.master_key.wrap(&key).unwrap()),
            None,
            Vec::new(),
        )
        .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .and(query_param("arg", TEST_CID))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&hot)
            .await;

        let error = get_object(
            &state,
            get_object_request(
                "encrypted-preheader-failure.bin",
                None,
                http::HeaderMap::new(),
            ),
        )
        .await
        .expect_err("encrypted cat failure must precede S3 response construction");
        assert_eq!(error.code().as_str(), "InternalError");
    }

    #[tokio::test]
    async fn legacy_sse_c_head_authenticates_against_the_selected_cold_client() {
        let hot = wiremock::MockServer::start().await;
        let cold = wiremock::MockServer::start().await;
        mount_node_identity(&cold, COLD_NODE_ID).await;
        let state =
            pinning_state_with_cold(hot.uri(), Some(cold.uri()), "request", "one", "").await;
        let key = crate::crypto::ObjectKey { bytes: [0x42; 32] };
        publish_versioned_read_object(
            &state,
            "legacy-cold-head",
            "legacy-cold-head.bin",
            TEST_CID,
            true,
            None,
            None,
            Vec::new(),
        )
        .await;
        move_version_to_verified_cold(&state, "legacy-cold-head.bin", None, COLD_NODE_ID).await;
        mount_cat_body(
            &cold,
            TEST_CID,
            crate::crypto::aes_gcm::encrypt_chunk(&key, &[3; 12], b"body")
                .unwrap()
                .to_vec(),
        )
        .await;

        let response = head_object(
            &state,
            head_object_request("legacy-cold-head.bin", None, valid_sse_c_headers()),
        )
        .await
        .expect("legacy SSE-C HEAD authenticates against cold");
        let requests = cold.received_requests().await.unwrap();
        let cats: Vec<_> = requests
            .iter()
            .filter(|request| request.url.path() == "/api/v0/cat")
            .collect();
        assert_eq!(cats.len(), 1);
        assert!(
            cats[0]
                .url
                .query_pairs()
                .any(|(key, value)| key == "offline" && value == "true"),
            "legacy SSE-C authentication also requires local-only cold bytes"
        );
        assert_eq!(
            response.output.sse_customer_algorithm.as_deref(),
            Some("AES256")
        );
        assert!(
            hot.received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() != "/api/v0/cat")
        );
    }

    #[tokio::test]
    async fn cold_reads_reject_online_only_bytes_in_every_encryption_mode() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, ResponseTemplate};

        for mode in ["plain", "sse-s3", "sse-c", "legacy-sse-c"] {
            let hot = wiremock::MockServer::start().await;
            let cold = wiremock::MockServer::start().await;
            mount_node_identity(&cold, COLD_NODE_ID).await;
            let state =
                pinning_state_with_cold(hot.uri(), Some(cold.uri()), "request", "one", "").await;
            let key = crate::crypto::ObjectKey { bytes: [0x42; 32] };
            publish_versioned_read_object(
                &state,
                "online-only",
                "online-only.bin",
                TEST_CID,
                mode != "plain",
                (mode == "sse-s3").then(|| state.master_key.wrap(&key).unwrap()),
                (mode == "sse-c").then(|| state.master_key.sse_c_key_fingerprint(&key)),
                Vec::new(),
            )
            .await;
            move_version_to_verified_cold(&state, "online-only.bin", None, COLD_NODE_ID).await;
            let online_bytes = if mode == "plain" {
                b"body".to_vec()
            } else {
                crate::crypto::aes_gcm::encrypt_chunk(&key, &[3; 12], b"body")
                    .unwrap()
                    .to_vec()
            };
            // A normal online cat could retrieve these from another peer.
            mount_cat_body(&cold, TEST_CID, online_bytes).await;
            Mock::given(method("POST"))
                .and(path("/api/v0/cat"))
                .and(query_param("offline", "true"))
                .respond_with(ResponseTemplate::new(500).set_body_string("block missing locally"))
                .with_priority(1)
                .expect(1)
                .mount(&cold)
                .await;
            let headers = if matches!(mode, "sse-c" | "legacy-sse-c") {
                valid_sse_c_headers()
            } else {
                http::HeaderMap::new()
            };
            let error = get_object(&state, get_object_request("online-only.bin", None, headers))
                .await
                .expect_err("cold must not silently retrieve blocks via swarm");
            assert_eq!(error.code().as_str(), "InternalError", "mode={mode}");
            assert!(hot.received_requests().await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn copy_from_cold_streams_and_verifies_the_source_dag_into_hot_before_publication() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, ResponseTemplate};

        const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
        const HOT_NODE_ID: &str = "QmPChd2hVbrJ6i1a7aDPgS6G9X4YuJ5sS7cGqf6ZkK3vYq";

        let hot = wiremock::MockServer::start().await;
        let cold = wiremock::MockServer::start().await;
        mount_node_identity(&cold, COLD_NODE_ID).await;
        mount_node_identity(&hot, HOT_NODE_ID).await;
        let state =
            pinning_state_with_cold(hot.uri(), Some(cold.uri()), "request", "one", "").await;
        publish_versioned_read_object(
            &state,
            "cold-copy-source",
            "cold-copy-source.bin",
            CID,
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        move_version_to_verified_cold(&state, "cold-copy-source.bin", None, COLD_NODE_ID).await;

        Mock::given(method("POST"))
            .and(path("/api/v0/dag/export"))
            .and(query_param("arg", CID))
            .and(query_param("offline", "true"))
            .and(query_param("progress", "false"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"faithful-car-bytes"))
            .expect(1)
            .mount(&cold)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/import"))
            .and(query_param("pin-roots", "true"))
            .and(query_param("stats", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "{{\"Root\":{{\"Cid\":{{\"/\":\"{CID}\"}},\"PinErrorMsg\":\"\"}}}}\n{{\"Stats\":{{\"BlockCount\":1,\"BlockBytesCount\":18}}}}\n"
            )))
            .expect(1)
            .mount(&hot)
            .await;
        for kubo in [&cold, &hot] {
            Mock::given(method("POST"))
                .and(path("/api/v0/pin/ls"))
                .and(query_param("arg", CID))
                .and(query_param("type", "recursive"))
                .and(query_param("offline", "true"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_string(format!(
                        r#"{{"Keys":{{"{CID}":{{"Type":"recursive"}}}}}}"#
                    )),
                )
                .expect(2)
                .mount(kubo)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/v0/files/stat"))
                .and(query_param("arg", format!("/ipfs/{CID}")))
                .and(query_param("with-local", "true"))
                .and(query_param("offline", "true"))
                .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                    r#"{{"Hash":"{CID}","WithLocality":true,"Local":true}}"#
                )))
                .expect(1)
                .mount(kubo)
                .await;
        }

        let response = copy_object(
            &state,
            copy_request_version(
                "cold-copy-source.bin",
                None,
                "hot-copy-destination.bin",
                http::HeaderMap::new(),
            ),
        )
        .await
        .expect("verified cold-to-hot CopyObject");
        assert_eq!(
            response
                .output
                .copy_object_result
                .as_ref()
                .and_then(|result| result.e_tag.as_ref())
                .map(ETag::value),
            Some(CID)
        );
        let destination = crate::store::object::get_latest(
            state.store.db(),
            "bucket",
            "hot-copy-destination.bin",
        )
        .await
        .unwrap();
        assert_eq!(destination.cid, CID);

        let hot_requests = hot.received_requests().await.unwrap();
        let import = hot_requests
            .iter()
            .find(|request| request.url.path() == "/api/v0/dag/import")
            .expect("hot import request");
        assert!(
            String::from_utf8_lossy(&import.body).contains("faithful-car-bytes"),
            "the actual cold CAR stream must feed the hot import"
        );
        assert!(
            hot_requests
                .iter()
                .all(|request| request.url.path() != "/api/v0/pin/add"),
            "cold CopyObject must not substitute a hot pin request for transport"
        );
    }

    #[tokio::test]
    async fn get_and_head_current_content_return_public_version() {
        let (state, kubo) = versioned_read_state().await;
        publish_versioned_read_object(
            &state,
            "current-old",
            "current.txt",
            "QmCurrentOld",
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        let current = publish_versioned_read_object(
            &state,
            "current-new",
            "current.txt",
            "QmCurrentNew",
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        let current_version = current.version_id.expect("versioned current ID");
        mount_cat_body(&kubo, "QmCurrentNew", b"body".to_vec()).await;

        let get = get_object(
            &state,
            get_object_request("current.txt", None, http::HeaderMap::new()),
        )
        .await
        .expect("current GetObject");
        assert_eq!(
            get.output.e_tag.as_ref().map(ETag::value),
            Some("QmCurrentNew")
        );
        assert_eq!(
            get.output.version_id.as_deref(),
            Some(current_version.as_str())
        );

        let head = head_object(
            &state,
            head_object_request("current.txt", None, http::HeaderMap::new()),
        )
        .await
        .expect("current HeadObject");
        assert_eq!(
            head.output.e_tag.as_ref().map(ETag::value),
            Some("QmCurrentNew")
        );
        assert_eq!(
            head.output.version_id.as_deref(),
            Some(current_version.as_str())
        );
    }

    #[tokio::test]
    async fn get_and_head_exact_historical_plain_sse_s3_and_sse_c_versions() {
        let (state, kubo) = versioned_read_state().await;

        let plain_old = publish_versioned_read_object(
            &state,
            "plain-old",
            "plain.txt",
            "QmPlainOld",
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        publish_versioned_read_object(
            &state,
            "plain-new",
            "plain.txt",
            "QmPlainNew",
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        mount_cat_body(&kubo, "QmPlainOld", b"old!".to_vec()).await;

        let sse_s3_old_key = state.master_key.generate_object_key();
        let sse_s3_old = publish_versioned_read_object(
            &state,
            "sse-s3-old",
            "sse-s3.txt",
            "QmSseS3Old",
            true,
            Some(state.master_key.wrap(&sse_s3_old_key).unwrap()),
            None,
            Vec::new(),
        )
        .await;
        let sse_s3_new_key = state.master_key.generate_object_key();
        publish_versioned_read_object(
            &state,
            "sse-s3-new",
            "sse-s3.txt",
            "QmSseS3New",
            true,
            Some(state.master_key.wrap(&sse_s3_new_key).unwrap()),
            None,
            Vec::new(),
        )
        .await;

        mount_cat_body(
            &kubo,
            "QmSseS3Old",
            crate::crypto::aes_gcm::encrypt_chunk(&sse_s3_old_key, &[0x11; 12], b"old!")
                .unwrap()
                .to_vec(),
        )
        .await;

        let sse_c_key = crate::crypto::ObjectKey { bytes: [0x42; 32] };
        let sse_c_fingerprint = state.master_key.sse_c_key_fingerprint(&sse_c_key);
        let sse_c_old = publish_versioned_read_object(
            &state,
            "sse-c-old",
            "sse-c.txt",
            "QmSseCOld",
            true,
            None,
            Some(sse_c_fingerprint.clone()),
            Vec::new(),
        )
        .await;
        publish_versioned_read_object(
            &state,
            "sse-c-new",
            "sse-c.txt",
            "QmSseCNew",
            true,
            None,
            Some(sse_c_fingerprint),
            Vec::new(),
        )
        .await;
        mount_cat_body(
            &kubo,
            "QmSseCOld",
            crate::crypto::aes_gcm::encrypt_chunk(&sse_c_key, &[0x22; 12], b"cold")
                .unwrap()
                .to_vec(),
        )
        .await;

        let plain_version = plain_old.version_id.expect("plain historical version");
        let plain = get_object(
            &state,
            get_object_request("plain.txt", Some(&plain_version), http::HeaderMap::new()),
        )
        .await
        .expect("historical plain GetObject");
        assert_eq!(
            plain.output.e_tag.as_ref().map(ETag::value),
            Some("QmPlainOld")
        );
        assert_eq!(
            plain.output.version_id.as_deref(),
            Some(plain_version.as_str())
        );

        let sse_s3_version = sse_s3_old.version_id.expect("SSE-S3 historical version");
        let sse_s3 = get_object(
            &state,
            get_object_request("sse-s3.txt", Some(&sse_s3_version), http::HeaderMap::new()),
        )
        .await
        .expect("historical SSE-S3 GetObject");
        assert_eq!(
            sse_s3.output.e_tag.as_ref().map(ETag::value),
            Some("QmSseS3Old")
        );
        assert_eq!(
            sse_s3.output.version_id.as_deref(),
            Some(sse_s3_version.as_str())
        );
        assert_eq!(
            sse_s3.output.server_side_encryption,
            Some(ServerSideEncryption::from_static("AES256"))
        );
        assert_eq!(read_get_body(sse_s3).await, b"old!");

        let sse_c_version = sse_c_old.version_id.expect("SSE-C historical version");
        let sse_c = get_object(
            &state,
            get_object_request("sse-c.txt", Some(&sse_c_version), valid_sse_c_headers()),
        )
        .await
        .expect("historical SSE-C GetObject");
        assert_eq!(
            sse_c.output.e_tag.as_ref().map(ETag::value),
            Some("QmSseCOld")
        );
        assert_eq!(
            sse_c.output.version_id.as_deref(),
            Some(sse_c_version.as_str())
        );
        assert_eq!(
            sse_c.output.sse_customer_algorithm.as_deref(),
            Some("AES256")
        );
        assert_eq!(read_get_body(sse_c).await, b"cold");

        let head = head_object(
            &state,
            head_object_request("sse-c.txt", Some(&sse_c_version), valid_sse_c_headers()),
        )
        .await
        .expect("historical SSE-C HeadObject");
        assert_eq!(
            head.output.e_tag.as_ref().map(ETag::value),
            Some("QmSseCOld")
        );
        assert_eq!(
            head.output.version_id.as_deref(),
            Some(sse_c_version.as_str())
        );
        assert_eq!(
            head.output.sse_customer_algorithm.as_deref(),
            Some("AES256")
        );
    }

    #[tokio::test]
    async fn current_marker_returns_404_headers_without_kubo() {
        let (state, kubo) = versioned_read_state().await;
        publish_versioned_read_object(
            &state,
            "marker-source",
            "marker-current.txt",
            "QmMarkerSource",
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        let marker_version = install_current_marker(&state, "marker-current.txt").await;

        for error in [
            get_object(
                &state,
                get_object_request("marker-current.txt", None, http::HeaderMap::new()),
            )
            .await
            .expect_err("current-marker GetObject must fail"),
            head_object(
                &state,
                head_object_request("marker-current.txt", None, http::HeaderMap::new()),
            )
            .await
            .expect_err("current-marker HeadObject must fail"),
        ] {
            assert_eq!(error.code().as_str(), "NoSuchKey");
            assert_eq!(error.status_code(), Some(http::StatusCode::NOT_FOUND));
            let headers = error.headers().expect("current-marker headers");
            assert_eq!(headers["x-amz-delete-marker"], "true");
            assert_eq!(headers["x-amz-version-id"], marker_version);
        }
        assert!(kubo.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn explicit_marker_returns_405_headers_without_range_or_sse_c() {
        let (state, kubo) = versioned_read_state().await;
        publish_versioned_read_object(
            &state,
            "marker-source",
            "marker-explicit.txt",
            "QmMarkerSource",
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        let marker_version = install_current_marker(&state, "marker-explicit.txt").await;
        let mut malformed_sse_c = http::HeaderMap::new();
        malformed_sse_c.insert(
            "x-amz-server-side-encryption-customer-algorithm",
            http::HeaderValue::from_static("AES256"),
        );
        let mut get_request = get_object_request(
            "marker-explicit.txt",
            Some(&marker_version),
            malformed_sse_c.clone(),
        );
        get_request.input.range = Some(Range::Int {
            first: 5,
            last: Some(4),
        });
        let mut head_request = head_object_request(
            "marker-explicit.txt",
            Some(&marker_version),
            malformed_sse_c,
        );
        head_request.input.range = Some(Range::Int {
            first: 5,
            last: Some(4),
        });

        for error in [
            get_object(&state, get_request)
                .await
                .expect_err("explicit-marker GetObject must fail before range or SSE-C"),
            head_object(&state, head_request)
                .await
                .expect_err("explicit-marker HeadObject must fail before range or SSE-C"),
        ] {
            assert_eq!(error.code().as_str(), "MethodNotAllowed");
            assert_eq!(
                error.status_code(),
                Some(http::StatusCode::METHOD_NOT_ALLOWED)
            );
            let headers = error.headers().expect("explicit-marker headers");
            assert_eq!(headers["x-amz-delete-marker"], "true");
            assert_eq!(headers["x-amz-version-id"], marker_version);
            assert!(headers.get(http::header::LAST_MODIFIED).is_some());
        }
        assert!(kubo.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn unknown_exact_is_no_such_version() {
        let (state, _kubo) = versioned_read_state().await;
        publish_versioned_read_object(
            &state,
            "unknown-source",
            "unknown.txt",
            "QmUnknown",
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        let unknown = uuid::Uuid::new_v4().to_string();

        for error in [
            get_object(
                &state,
                get_object_request("unknown.txt", Some(&unknown), http::HeaderMap::new()),
            )
            .await
            .expect_err("unknown GetObject version must fail"),
            head_object(
                &state,
                head_object_request("unknown.txt", Some(&unknown), http::HeaderMap::new()),
            )
            .await
            .expect_err("unknown HeadObject version must fail"),
        ] {
            assert_eq!(error.code().as_str(), "NoSuchVersion");
        }
    }

    #[tokio::test]
    async fn unversioned_exact_is_invalid_argument() {
        let kubo = kubo_server("unused").await;
        let state = pinning_state(kubo.uri(), "request", "one", "").await;
        publish_versioned_read_object(
            &state,
            "unversioned-source",
            "unversioned.txt",
            "QmUnversioned",
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        let supplied = uuid::Uuid::new_v4().to_string();

        for error in [
            get_object(
                &state,
                get_object_request("unversioned.txt", Some(&supplied), http::HeaderMap::new()),
            )
            .await
            .expect_err("unversioned GetObject version must fail"),
            head_object(
                &state,
                head_object_request("unversioned.txt", Some(&supplied), http::HeaderMap::new()),
            )
            .await
            .expect_err("unversioned HeadObject version must fail"),
        ] {
            assert_eq!(error.code().as_str(), "InvalidArgument");
        }
    }

    #[tokio::test]
    async fn copy_source_exact_version_uses_selected_content_and_tags() {
        let (state, _kubo) = versioned_read_state().await;
        let source_tags = vec![crate::pinning::tags::ObjectTag::new("team", "historical")];
        let source_old = publish_versioned_read_object(
            &state,
            "copy-old",
            "copy-source.txt",
            "QmCopyOld",
            false,
            None,
            None,
            source_tags.clone(),
        )
        .await;
        publish_versioned_read_object(
            &state,
            "copy-new",
            "copy-source.txt",
            "QmCopyNew",
            false,
            None,
            None,
            vec![crate::pinning::tags::ObjectTag::new("team", "current")],
        )
        .await;
        let source_version = source_old.version_id.expect("historical source version");

        let copy = copy_object(
            &state,
            copy_request_version(
                "copy-source.txt",
                Some(&source_version),
                "copy-destination.txt",
                http::HeaderMap::new(),
            ),
        )
        .await
        .expect("historical CopyObject");
        assert_eq!(
            copy.output
                .copy_object_result
                .as_ref()
                .and_then(|result| result.e_tag.as_ref())
                .map(ETag::value),
            Some("QmCopyOld")
        );
        assert_eq!(
            copy.output.copy_source_version_id.as_deref(),
            Some(source_version.as_str())
        );
        let destination =
            crate::store::object::get_latest(state.store.db(), "bucket", "copy-destination.txt")
                .await
                .unwrap();
        assert_eq!(destination.cid, "QmCopyOld");
        assert_eq!(
            crate::store::pinning::tags::list_object_tags(state.store.db(), &destination.id)
                .await
                .unwrap(),
            source_tags
        );
    }

    #[tokio::test]
    async fn copy_source_marker_uses_current_404_or_explicit_405() {
        let (state, kubo) = versioned_read_state().await;
        publish_versioned_read_object(
            &state,
            "copy-marker-source",
            "copy-marker.txt",
            "QmCopyMarker",
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        let marker_version = install_current_marker(&state, "copy-marker.txt").await;
        let mut malformed_copy_source_sse_c = http::HeaderMap::new();
        malformed_copy_source_sse_c.insert(
            "x-amz-copy-source-server-side-encryption-customer-algorithm",
            http::HeaderValue::from_static("AES256"),
        );

        for (version_id, code, status) in [
            (None, "NoSuchKey", http::StatusCode::NOT_FOUND),
            (
                Some(marker_version.as_str()),
                "MethodNotAllowed",
                http::StatusCode::METHOD_NOT_ALLOWED,
            ),
        ] {
            let error = copy_object(
                &state,
                copy_request_version(
                    "copy-marker.txt",
                    version_id,
                    "copy-marker-destination.txt",
                    malformed_copy_source_sse_c.clone(),
                ),
            )
            .await
            .expect_err("delete-marker CopyObject must fail");
            assert_eq!(error.code().as_str(), code);
            assert_eq!(error.status_code(), Some(status));
            let headers = error.headers().expect("delete-marker headers");
            assert_eq!(headers["x-amz-delete-marker"], "true");
            assert_eq!(headers["x-amz-version-id"], marker_version);
        }
        assert!(kubo.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn copy_destination_gets_independent_object_and_public_ids() {
        let (state, _kubo) = versioned_read_state().await;
        let source = publish_versioned_read_object(
            &state,
            "copy-independent-source",
            "copy-independent-source.txt",
            "QmCopyIndependent",
            false,
            None,
            None,
            Vec::new(),
        )
        .await;
        let source_version = source.version_id.expect("source version");

        let copy = copy_object(
            &state,
            copy_request_version(
                "copy-independent-source.txt",
                None,
                "copy-independent-destination.txt",
                http::HeaderMap::new(),
            ),
        )
        .await
        .expect("CopyObject");
        let destination_version = copy.output.version_id.expect("destination version");
        assert_eq!(
            copy.output.copy_source_version_id.as_deref(),
            Some(source_version.as_str())
        );
        assert_ne!(destination_version, source_version);

        let source = crate::store::object::get_latest(
            state.store.db(),
            "bucket",
            "copy-independent-source.txt",
        )
        .await
        .unwrap();
        let destination = crate::store::object::get_latest(
            state.store.db(),
            "bucket",
            "copy-independent-destination.txt",
        )
        .await
        .unwrap();
        assert_ne!(destination.id, source.id);
        assert_eq!(destination.cid, source.cid);
    }

    fn object_model(key: &str) -> object::Model {
        object::Model {
            id: "id".to_string(),
            bucket: "bucket".to_string(),
            key: key.to_string(),
            cid: "QmTest".to_string(),
            size: 0,
            content_type: None,
            etag: "etag".to_string(),
            metadata: None,
            encrypted: false,
            key_wrap: None,
            sse_c_key_fingerprint: None,
            multipart: false,
            is_latest: true,
            created_at: Utc::now(),
        }
    }

    async fn list_state_with_keys(keys: &[&str]) -> Arc<AppState> {
        use sea_orm::Database;

        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();

        for (idx, key) in keys.iter().enumerate() {
            let cid = format!("Qm{idx:08x}");
            let etag = format!("etag-{idx:08x}");
            crate::store::object::upsert(
                &db,
                &format!("id-{idx:08x}"),
                "bucket",
                key,
                &cid,
                0,
                None,
                &etag,
                None,
                false,
                None,
                None,
                false,
            )
            .await
            .unwrap();
        }

        let kubo = crate::kubo::KuboClient::new("http://127.0.0.1:5001".to_string());
        let store = crate::store::Store::new(db);
        let credentials = std::collections::HashMap::new();
        let master_key = crate::crypto::key::MasterKey::from_hex(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .unwrap();

        Arc::new(AppState {
            kubo,
            cold_kubo: None,
            store,
            credentials,
            master_key,
            pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
        })
    }

    fn list_v2_request(input: ListObjectsV2Input) -> S3Request<ListObjectsV2Input> {
        use http::{HeaderMap, Method, Uri};

        S3Request {
            input,
            method: Method::GET,
            uri: Uri::from_static("/bucket?list-type=2"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn list_v1_request(input: ListObjectsInput) -> S3Request<ListObjectsInput> {
        S3Request {
            input,
            method: http::Method::GET,
            uri: http::Uri::from_static("/bucket"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn delete_objects_request(keys: &[&str], quiet: bool) -> S3Request<DeleteObjectsInput> {
        delete_objects_version_request(
            keys.iter().map(|key| ((*key).to_owned(), None)).collect(),
            quiet,
        )
    }

    fn delete_objects_version_request(
        objects: Vec<(String, Option<String>)>,
        quiet: bool,
    ) -> S3Request<DeleteObjectsInput> {
        S3Request {
            input: DeleteObjectsInput {
                bucket: "bucket".to_owned(),
                bypass_governance_retention: None,
                checksum_algorithm: None,
                delete: Delete {
                    objects: objects
                        .into_iter()
                        .map(|(key, version_id)| ObjectIdentifier {
                            key,
                            version_id,
                            ..Default::default()
                        })
                        .collect(),
                    quiet: Some(quiet),
                },
                expected_bucket_owner: None,
                mfa: None,
                request_payer: None,
            },
            method: http::Method::POST,
            uri: http::Uri::from_static("/bucket?delete"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn pinning_put_automatic_commits_outbox_without_provider_request() {
        let kubo = kubo_server("bafy-put").await;
        let state = pinning_state(kubo.uri(), "always", "all", "").await;
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            crate::store::object_version::BucketVersioningState::Enabled,
        )
        .await
        .unwrap();

        let response = put_object(&state, put_request("automatic", None))
            .await
            .unwrap();

        assert_eq!(
            response.output.e_tag.as_ref().map(ETag::value),
            Some("bafy-put")
        );
        uuid::Uuid::parse_str(response.output.version_id.as_deref().unwrap()).unwrap();
        assert_eq!(
            lease_sources_for_latest(&state, "automatic").await,
            vec!["automatic"]
        );
        assert_eq!(pending_operations(&state).await, vec!["submit", "submit"]);
        let requests = kubo.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path() == "/api/v0/add")
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path() == "/api/v0/pin/add")
                .count(),
            1
        );
        assert!(
            requests
                .iter()
                .all(|request| request.url.path().starts_with("/api/v0/")),
            "the response path must not call a remote pinning provider"
        );
    }

    #[tokio::test]
    async fn cancelled_streaming_put_releases_guard_and_never_publishes() {
        use http_body_util::BodyExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri = format!("http://{}", listener.local_addr().unwrap());
        // Unlike Wiremock's full-body collector, this streaming endpoint treats
        // request cancellation as ordinary EOF/error rather than a mock panic.
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new().fallback(|mut body: axum::body::Body| async move {
                    while let Some(frame) = body.frame().await {
                        if frame.is_err() {
                            break;
                        }
                    }
                    http::StatusCode::BAD_REQUEST
                }),
            )
            .await
            .unwrap();
        });
        let state = pinning_state(uri, "request", "one", "").await;
        let (started, ready) = tokio::sync::oneshot::channel();
        let body = async_stream::stream! {
            started.send(()).unwrap();
            std::future::pending::<()>().await;
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"never sent"));
        };
        let mut req = put_request("cancelled", None);
        req.input.body = Some(StreamingBlob::wrap(body));
        let task_state = state.clone();
        let task = tokio::spawn(async move { put_object(&task_state, req).await });
        ready.await.unwrap();
        assert!(
            crate::store::import::ownership::try_admit_lifecycle_mutation(
                state.store.db(),
                "bucket",
                "cancelled",
                "recovery",
                1,
                chrono::Utc::now(),
            )
            .await
            .unwrap()
            .is_none()
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if crate::store::import::ownership::try_admit_lifecycle_mutation(
                    state.store.db(),
                    "bucket",
                    "cancelled",
                    "recovery",
                    1,
                    chrono::Utc::now(),
                )
                .await
                .unwrap()
                .is_some()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            crate::store::object::get_latest(state.store.db(), "bucket", "cancelled").await,
            Err(AppError::NoSuchKey(_))
        ));
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn put_object_pin_add_failure_never_removes_the_uploaded_cid() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"bafy-shared-pin-failure\",\"Size\":\"4\"}\n"),
            )
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_string("kubo-body-marker-do-not-leak http://127.0.0.1:5001"),
            )
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/rm"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&kubo)
            .await;

        let state = pinning_state(kubo.uri(), "request", "one", "").await;
        let error = put_object(&state, put_request("pin-add-failure", None))
            .await
            .expect_err("a Kubo pin-add failure must fail PutObject");

        assert_eq!(error.code().as_str(), "InternalError");
        assert_eq!(error.message(), Some("internal storage backend error"));
        assert!(
            crate::store::import::ownership::try_admit_lifecycle_mutation(
                state.store.db(),
                "bucket",
                "pin-add-failure",
                "recovery",
                1,
                chrono::Utc::now(),
            )
            .await
            .unwrap()
            .is_some(),
            "failed streaming PUT must release its ownership"
        );
        assert!(
            !error.to_string().contains("kubo-body-marker-do-not-leak")
                && !error.to_string().contains("127.0.0.1"),
            "PutObject must not expose Kubo body or endpoint: {error}"
        );
        assert!(
            kubo.received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() != "/api/v0/pin/rm"),
            "standard PutObject must never unpin a CID after pin-add failure"
        );
    }

    #[tokio::test]
    async fn put_object_publication_failure_never_removes_the_pinned_cid() {
        use sea_orm::ConnectionTrait;

        let kubo = kubo_server("bafy-shared-publication-failure").await;
        let state = pinning_state(kubo.uri(), "request", "one", "").await;
        state
            .store
            .db()
            .execute_unprepared("DROP TABLE objects")
            .await
            .unwrap();

        let error = put_object(&state, put_request("publication-failure", None))
            .await
            .expect_err("a publication database failure must fail PutObject");

        assert_eq!(error.code().as_str(), "InternalError");
        let requests = kubo.received_requests().await.unwrap();
        assert!(
            requests
                .iter()
                .any(|request| request.url.path() == "/api/v0/pin/add"),
            "the fixture must reach the successful local pin before publication fails"
        );
        assert!(
            requests
                .iter()
                .all(|request| request.url.path() != "/api/v0/pin/rm"),
            "standard PutObject must never unpin a CID after publication failure"
        );
    }

    #[tokio::test]
    async fn pinning_put_request_manual_and_always_manual_create_expected_leases() {
        let request_kubo = kubo_server("bafy-request").await;
        let request_state = pinning_state(request_kubo.uri(), "request", "one", "").await;

        put_object(&request_state, put_request("ordinary", None))
            .await
            .unwrap();
        assert!(
            lease_sources_for_latest(&request_state, "ordinary")
                .await
                .is_empty()
        );
        put_object(
            &request_state,
            put_request("manual", Some("ipfs-s3%3Apin=true&ipfs-s3%3Aduration=2h")),
        )
        .await
        .unwrap();
        assert_eq!(
            lease_sources_for_latest(&request_state, "manual").await,
            vec!["manual"]
        );

        let always_kubo = kubo_server("bafy-always").await;
        let always_state = pinning_state(always_kubo.uri(), "always", "one", "").await;
        put_object(
            &always_state,
            put_request(
                "combined",
                Some("team=storage&ipfs-s3%3Apin=true&ipfs-s3%3Aduration=2h"),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            lease_sources_for_latest(&always_state, "combined").await,
            vec!["automatic", "manual"]
        );
        assert_eq!(
            pin_lease::Entity::find()
                .filter(
                    pin_lease::Column::OwnerObjectId.eq(crate::store::object::get_latest(
                        always_state.store.db(),
                        "bucket",
                        "combined",
                    )
                    .await
                    .unwrap()
                    .id,)
                )
                .count(always_state.store.db())
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn pinning_put_invalid_tag_control_and_policy_fail_before_kubo_add() {
        let kubo = kubo_server("bafy-never-added").await;
        let state = pinning_state(kubo.uri(), "request", "one", "matched/").await;

        for request in [
            put_request("matched/malformed", Some("team=%GG")),
            put_request("matched/control", Some("ipfs-s3%3Aduration=1h")),
            put_request("unmatched", Some("ipfs-s3%3Apin=true")),
        ] {
            let error = put_object(&state, request).await.unwrap_err();
            assert_eq!(error.code().as_str(), "InvalidArgument");
        }

        assert!(
            kubo.received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() != "/api/v0/add")
        );
    }

    #[tokio::test]
    async fn pinning_put_rejects_duplicate_tagging_before_kubo() {
        let kubo = kubo_server("bafy-never-added").await;
        let state = pinning_state(kubo.uri(), "request", "one", "").await;
        let mut request = put_request("duplicate-tags", Some("team=legal"));
        request
            .headers
            .append("x-amz-tagging", http::HeaderValue::from_static("team=%GG"));

        let error = put_object(&state, request).await.unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidArgument");
        assert_eq!(
            error.message(),
            Some("invalid pinning request: duplicate x-amz-tagging header")
        );
        assert!(kubo.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn pinning_copy_rejects_duplicate_tagging_before_kubo_or_publish() {
        let kubo = kubo_server("unused-add-response").await;
        let state = pinning_state(kubo.uri(), "request", "one", "dest/").await;
        seed_copy_source(&state, "source", "bafy-shared", &[]).await;
        let mut request = copy_request("source", "dest/duplicate-tags", Some("COPY"), Some(""));
        request.headers.append(
            "x-amz-tagging",
            http::HeaderValue::from_static("team=missed"),
        );

        let error = copy_object(&state, request).await.unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidArgument");
        assert_eq!(
            error.message(),
            Some("invalid pinning request: duplicate x-amz-tagging header")
        );
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "dest/duplicate-tags")
                .await
                .is_err()
        );
        assert!(kubo.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn pinning_copy_rejects_duplicate_tagging_directive_before_kubo_or_publish() {
        let kubo = kubo_server("unused-add-response").await;
        let state = pinning_state(kubo.uri(), "request", "one", "dest/").await;
        seed_copy_source(&state, "source", "bafy-shared", &[]).await;
        let mut request = copy_request("source", "dest/duplicate-directive", Some("COPY"), None);
        request.headers.append(
            "x-amz-tagging-directive",
            http::HeaderValue::from_static("REPLACE"),
        );

        let error = copy_object(&state, request).await.unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidArgument");
        assert_eq!(
            error.message(),
            Some("invalid pinning request: duplicate x-amz-tagging-directive header")
        );
        assert!(
            crate::store::object::get_latest(
                state.store.db(),
                "bucket",
                "dest/duplicate-directive"
            )
            .await
            .is_err()
        );
        assert!(kubo.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn pinning_overwrite_ends_old_leases_and_publishes_new_latest_together() {
        let kubo = kubo_server("bafy-new").await;
        let state = pinning_state(kubo.uri(), "always", "one", "").await;
        publish_seed(&state, "old-object", "key", "bafy-old", Vec::new()).await;

        put_object(
            &state,
            put_request("key", Some("ipfs-s3%3Apin=true&ipfs-s3%3Aduration=2h")),
        )
        .await
        .unwrap();

        let old = pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq("old-object"))
            .all(state.store.db())
            .await
            .unwrap();
        assert_eq!(old.len(), 1);
        assert_eq!(old[0].state, "cancelled");
        assert_eq!(old[0].generation, 2);
        let latest = crate::store::object::get_latest(state.store.db(), "bucket", "key")
            .await
            .unwrap();
        assert_eq!(latest.cid, "bafy-new");
        assert_ne!(latest.id, "old-object");
        assert_eq!(
            lease_sources_for_latest(&state, "key").await,
            vec!["automatic", "manual"]
        );
    }

    #[tokio::test]
    async fn pinning_copy_directives_use_source_or_replacement_tags_and_destination_policy() {
        let kubo = kubo_server("unused-add-response").await;
        let state = pinning_state(kubo.uri(), "request", "one", "dest/").await;
        let source_tags = vec![
            crate::pinning::tags::ObjectTag::new("team", "source"),
            crate::pinning::tags::ObjectTag::new("ipfs-s3:pin", "true"),
        ];
        publish_seed(
            &state,
            "source-source",
            "source",
            "bafy-shared",
            vec![crate::pinning::tags::ObjectTag::new("team", "source")],
        )
        .await;
        crate::store::pinning::tags::replace_object_tags(
            state.store.db(),
            "source-source",
            &source_tags,
        )
        .await
        .unwrap();
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            crate::store::object_version::BucketVersioningState::Enabled,
        )
        .await
        .unwrap();

        let first_copy = copy_object(&state, copy_request("source", "dest/default", None, None))
            .await
            .unwrap();
        uuid::Uuid::parse_str(first_copy.output.version_id.as_deref().unwrap()).unwrap();
        copy_object(
            &state,
            copy_request("source", "dest/copy", Some("COPY"), Some("")),
        )
        .await
        .unwrap();
        copy_object(
            &state,
            copy_request(
                "source",
                "dest/replaced",
                Some("REPLACE"),
                Some("team=replaced&ipfs-s3%3Apin=true"),
            ),
        )
        .await
        .unwrap();
        for request in [
            copy_request("source", "dest/reject-tags", None, Some("team=override")),
            copy_request(
                "source",
                "dest/reject-copy-tags",
                Some("COPY"),
                Some("team=override"),
            ),
            copy_request("source", "dest/missing", Some("REPLACE"), None),
            copy_request("source", "dest/invalid", Some("MERGE"), None),
        ] {
            let error = copy_object(&state, request).await.unwrap_err();
            assert_eq!(error.code().as_str(), "InvalidArgument");
        }
        copy_object(
            &state,
            copy_request("source", "dest/empty", Some("REPLACE"), Some("")),
        )
        .await
        .unwrap();

        assert_eq!(
            crate::store::pinning::tags::list_object_tags(
                state.store.db(),
                &crate::store::object::get_latest(state.store.db(), "bucket", "dest/default")
                    .await
                    .unwrap()
                    .id,
            )
            .await
            .unwrap(),
            vec![
                crate::pinning::tags::ObjectTag::new("ipfs-s3:pin", "true"),
                crate::pinning::tags::ObjectTag::new("team", "source"),
            ]
        );
        assert!(
            crate::store::pinning::tags::list_object_tags(
                state.store.db(),
                &crate::store::object::get_latest(state.store.db(), "bucket", "dest/empty")
                    .await
                    .unwrap()
                    .id,
            )
            .await
            .unwrap()
            .is_empty()
        );
        assert_eq!(
            crate::store::pinning::tags::list_object_tags(
                state.store.db(),
                &crate::store::object::get_latest(state.store.db(), "bucket", "dest/replaced")
                    .await
                    .unwrap()
                    .id,
            )
            .await
            .unwrap(),
            vec![
                crate::pinning::tags::ObjectTag::new("ipfs-s3:pin", "true"),
                crate::pinning::tags::ObjectTag::new("team", "replaced"),
            ]
        );
        assert_eq!(
            lease_sources_for_latest(&state, "dest/default").await,
            vec!["manual"]
        );
        assert_eq!(
            lease_sources_for_latest(&state, "dest/copy").await,
            vec!["manual"]
        );
        assert_eq!(
            lease_sources_for_latest(&state, "dest/replaced").await,
            vec!["manual"]
        );
        assert!(
            lease_sources_for_latest(&state, "dest/empty")
                .await
                .is_empty()
        );
        let usage = pin_provider_usage::Entity::find_by_id("alpha")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (4, 1));
        let requests = kubo.received_requests().await.unwrap();
        assert_eq!(requests.len(), 4);
        assert!(
            requests
                .iter()
                .all(|request| request.url.path() == "/api/v0/pin/add"),
            "copy must only touch the local Kubo pin endpoint synchronously"
        );
    }

    #[tokio::test]
    async fn pinning_copy_reuses_pinned_remote_without_submit_or_poll() {
        let kubo = kubo_server("unused-add-response").await;
        let state = pinning_state(kubo.uri(), "request", "one", "dest/").await;
        let source_tags = vec![crate::pinning::tags::ObjectTag::new("ipfs-s3:pin", "true")];
        seed_copy_source(&state, "source", "bafy-pinned-copy", &source_tags).await;
        copy_object(&state, copy_request("source", "dest/first", None, None))
            .await
            .unwrap();
        remote_pin::Entity::update_many()
            .col_expr(remote_pin::Column::Status, "pinned".into())
            .col_expr(
                remote_pin::Column::RequestId,
                Some("remote-request".to_owned()).into(),
            )
            .filter(remote_pin::Column::Provider.eq("alpha"))
            .filter(remote_pin::Column::Cid.eq("bafy-pinned-copy"))
            .exec(state.store.db())
            .await
            .unwrap();
        pin_job::Entity::delete_many()
            .exec(state.store.db())
            .await
            .unwrap();

        copy_object(&state, copy_request("source", "dest/second", None, None))
            .await
            .unwrap();

        assert!(pending_operations(&state).await.is_empty());
        let latest = crate::store::object::get_latest(state.store.db(), "bucket", "dest/second")
            .await
            .unwrap();
        let lease = pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq(latest.id))
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.state, "active");
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq(lease.id))
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "pinned"
        );
    }

    #[tokio::test]
    async fn unversioned_simple_delete_is_idempotent_and_removes_hidden_null() {
        let kubo = kubo_server("unused-add-response").await;
        let state = pinning_state(kubo.uri(), "always", "one", "").await;
        publish_seed(
            &state,
            "unversioned-delete",
            "key",
            "bafy-unversioned",
            vec![crate::pinning::tags::ObjectTag::new("team", "storage")],
        )
        .await;

        let exact_error = delete_object(&state, delete_object_version_request("key", Some("null")))
            .await
            .unwrap_err();
        assert_eq!(exact_error.code().as_str(), "InvalidArgument");
        assert_eq!(
            crate::store::object::get_latest(state.store.db(), "bucket", "key")
                .await
                .unwrap()
                .id,
            "unversioned-delete"
        );

        let first = delete_object(&state, delete_object_request("key"))
            .await
            .unwrap()
            .output;
        assert_eq!(first, DeleteObjectOutput::default());
        let second = delete_object(&state, delete_object_request("key"))
            .await
            .expect("missing unversioned simple delete is idempotent")
            .output;
        assert_eq!(second, DeleteObjectOutput::default());
        assert!(matches!(
            crate::store::object::get_latest(state.store.db(), "bucket", "key").await,
            Err(crate::error::AppError::NoSuchKey(_))
        ));
        assert_eq!(
            object_version::Entity::find()
                .filter(object_version::Column::Bucket.eq("bucket"))
                .filter(object_version::Column::Key.eq("key"))
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            object_tag::Entity::find()
                .filter(object_tag::Column::ObjectId.eq("unversioned-delete"))
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert!(
            pin_lease::Entity::find()
                .filter(pin_lease::Column::OwnerObjectId.eq("unversioned-delete"))
                .all(state.store.db())
                .await
                .unwrap()
                .iter()
                .all(|lease| lease.state == "cancelled")
        );
    }

    #[tokio::test]
    async fn enabled_simple_delete_always_creates_new_opaque_marker() {
        let kubo = kubo_server("unused-add-response").await;
        let state = pinning_state(kubo.uri(), "always", "one", "").await;
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            crate::store::object_version::BucketVersioningState::Enabled,
        )
        .await
        .unwrap();
        publish_seed(&state, "enabled-delete", "key", "bafy-enabled", vec![]).await;

        let first = delete_object(&state, delete_object_request("key"))
            .await
            .unwrap()
            .output;
        assert_eq!(first.delete_marker, Some(true));
        let first_marker = first.version_id.expect("first marker version ID");
        uuid::Uuid::parse_str(&first_marker).unwrap();
        assert!(matches!(
            crate::store::object::get_latest(state.store.db(), "bucket", "key").await,
            Err(crate::error::AppError::NoSuchKey(_))
        ));
        assert!(
            pin_lease::Entity::find()
                .filter(pin_lease::Column::OwnerObjectId.eq("enabled-delete"))
                .all(state.store.db())
                .await
                .unwrap()
                .iter()
                .all(|lease| lease.state == "active")
        );

        let second = delete_object(&state, delete_object_request("key"))
            .await
            .unwrap()
            .output;
        assert_eq!(second.delete_marker, Some(true));
        let second_marker = second.version_id.expect("second marker version ID");
        uuid::Uuid::parse_str(&second_marker).unwrap();
        assert_ne!(first_marker, second_marker);
        let versions = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq("bucket"))
            .filter(object_version::Column::Key.eq("key"))
            .order_by_asc(object_version::Column::Sequence)
            .all(state.store.db())
            .await
            .unwrap();
        assert_eq!(versions.len(), 3);
        assert_eq!(
            versions
                .iter()
                .filter(|version| version.kind == "delete_marker")
                .count(),
            2
        );
        assert_eq!(
            versions
                .iter()
                .find(|version| version.is_latest)
                .and_then(|version| version.version_id.as_deref()),
            Some(second_marker.as_str())
        );
    }

    #[tokio::test]
    async fn suspended_simple_delete_replaces_null_and_releases_only_displaced_null() {
        let kubo = kubo_server("unused-add-response").await;
        let state = pinning_state(kubo.uri(), "always", "one", "").await;
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            crate::store::object_version::BucketVersioningState::Enabled,
        )
        .await
        .unwrap();
        publish_seed(&state, "opaque-old", "key", "bafy-opaque-old", vec![]).await;
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            crate::store::object_version::BucketVersioningState::Suspended,
        )
        .await
        .unwrap();
        publish_seed(
            &state,
            "null-old",
            "key",
            "bafy-null-old",
            vec![crate::pinning::tags::ObjectTag::new("team", "null")],
        )
        .await;
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            crate::store::object_version::BucketVersioningState::Enabled,
        )
        .await
        .unwrap();
        publish_seed(
            &state,
            "opaque-current",
            "key",
            "bafy-opaque-current",
            vec![],
        )
        .await;
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            crate::store::object_version::BucketVersioningState::Suspended,
        )
        .await
        .unwrap();

        let output = delete_object(&state, delete_object_request("key"))
            .await
            .unwrap()
            .output;
        assert_eq!(output.delete_marker, Some(true));
        assert_eq!(output.version_id.as_deref(), Some("null"));
        let versions = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq("bucket"))
            .filter(object_version::Column::Key.eq("key"))
            .order_by_asc(object_version::Column::Sequence)
            .all(state.store.db())
            .await
            .unwrap();
        assert_eq!(versions.len(), 3);
        let null = versions
            .iter()
            .find(|version| version.version_id.is_none())
            .unwrap();
        assert_eq!(null.kind, "delete_marker");
        assert!(null.is_latest);
        assert!(matches!(
            crate::store::object::get_latest(state.store.db(), "bucket", "key").await,
            Err(crate::error::AppError::NoSuchKey(_))
        ));
        for retained in ["opaque-old", "opaque-current"] {
            assert!(
                pin_lease::Entity::find()
                    .filter(pin_lease::Column::OwnerObjectId.eq(retained))
                    .all(state.store.db())
                    .await
                    .unwrap()
                    .iter()
                    .all(|lease| lease.state == "active")
            );
        }
        assert!(
            pin_lease::Entity::find()
                .filter(pin_lease::Column::OwnerObjectId.eq("null-old"))
                .all(state.store.db())
                .await
                .unwrap()
                .iter()
                .all(|lease| lease.state == "cancelled")
        );
        assert_eq!(
            object_tag::Entity::find()
                .filter(object_tag::Column::ObjectId.eq("null-old"))
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );

        let lifecycle_before_marker_replacement = pin_lease::Entity::find()
            .order_by_asc(pin_lease::Column::Id)
            .all(state.store.db())
            .await
            .unwrap();
        let replacement = delete_object(&state, delete_object_request("key"))
            .await
            .unwrap()
            .output;
        assert_eq!(replacement.delete_marker, Some(true));
        assert_eq!(replacement.version_id.as_deref(), Some("null"));
        assert_eq!(
            pin_lease::Entity::find()
                .order_by_asc(pin_lease::Column::Id)
                .all(state.store.db())
                .await
                .unwrap(),
            lifecycle_before_marker_replacement
        );
        assert_eq!(
            object_version::Entity::find()
                .filter(object_version::Column::Bucket.eq("bucket"))
                .filter(object_version::Column::Key.eq("key"))
                .count(state.store.db())
                .await
                .unwrap(),
            3
        );
    }

    #[tokio::test]
    async fn delete_never_calls_pin_rm() {
        let kubo = kubo_server("unused-add-response").await;
        let state = pinning_state(kubo.uri(), "always", "one", "").await;
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            crate::store::object_version::BucketVersioningState::Enabled,
        )
        .await
        .unwrap();
        publish_seed(&state, "never-unpin", "key", "bafy-never-unpin", vec![]).await;
        let content_version = object_version::Entity::find()
            .filter(object_version::Column::ObjectId.eq("never-unpin"))
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap()
            .version_id
            .unwrap();
        let marker = delete_object(&state, delete_object_request("key"))
            .await
            .unwrap()
            .output
            .version_id
            .unwrap();
        delete_object(
            &state,
            delete_object_version_request("key", Some(&content_version)),
        )
        .await
        .unwrap();
        delete_object(&state, delete_object_version_request("key", Some(&marker)))
            .await
            .unwrap();

        assert!(
            kubo.received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() != "/api/v0/pin/rm")
        );
    }

    #[tokio::test]
    async fn pinning_delete_operations_close_only_removed_latest_owner_leases_without_kubo_unpin() {
        let kubo = kubo_server("unused-add-response").await;
        let state = pinning_state(kubo.uri(), "always", "one", "").await;
        publish_seed(&state, "delete-a", "a", "bafy-a", Vec::new()).await;
        publish_seed(&state, "delete-b", "b", "bafy-b", Vec::new()).await;

        delete_object(&state, delete_object_request("a"))
            .await
            .unwrap();
        delete_object(&state, delete_object_request("missing"))
            .await
            .expect("missing unversioned simple delete is idempotent");
        assert_eq!(
            pin_lease::Entity::find()
                .filter(pin_lease::Column::OwnerObjectId.eq("delete-a"))
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "cancelled"
        );
        assert_eq!(
            pin_lease::Entity::find()
                .filter(pin_lease::Column::OwnerObjectId.eq("delete-b"))
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "active"
        );

        let output = delete_objects(
            &state,
            delete_objects_request(&["missing", "b", "b"], false),
        )
        .await
        .unwrap()
        .output;
        assert_eq!(
            output
                .deleted
                .unwrap()
                .into_iter()
                .map(|object| object.key.unwrap())
                .collect::<Vec<_>>(),
            vec!["missing", "b", "b"]
        );
        assert_eq!(
            pin_lease::Entity::find()
                .filter(pin_lease::Column::OwnerObjectId.eq("delete-b"))
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "cancelled"
        );
        assert!(
            kubo.received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() != "/api/v0/pin/rm")
        );
    }

    #[tokio::test]
    async fn add_plain_object_stream_counts_pins_and_returns_cid() {
        use futures_util::stream;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmPlain\",\"Size\":\"5\"}\n"),
            )
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmPlain\"]}"))
            .mount(&kubo)
            .await;

        let state = test_state(kubo.uri()).await;
        let stored = add_plain_object_stream(
            &state,
            stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
                b"hello",
            ))]),
        )
        .await
        .unwrap();

        assert_eq!(stored.cid, "QmPlain");
        assert_eq!(stored.size, 5);

        let requests = kubo.received_requests().await.unwrap();
        assert!(
            requests
                .iter()
                .any(|request| request.url.path() == "/api/v0/add")
        );
        assert!(requests.iter().any(|request| {
            request.url.path() == "/api/v0/pin/add" && request.url.query() == Some("arg=QmPlain")
        }));
    }

    #[tokio::test]
    async fn publish_plain_object_writes_latest_metadata() {
        use futures_util::stream;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmPlain\",\"Size\":\"5\"}\n"),
            )
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmPlain\"]}"))
            .mount(&kubo)
            .await;

        let state = test_state(kubo.uri()).await;
        crate::store::bucket::create(state.store.db(), "test-bucket", None)
            .await
            .unwrap();
        let stored = add_plain_object_stream(
            &state,
            stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
                b"hello",
            ))]),
        )
        .await
        .unwrap();

        publish_plain_object(
            &state,
            "test-bucket",
            "prefix/file.txt",
            None,
            None,
            &stored,
            false,
        )
        .await
        .unwrap();

        let obj =
            crate::store::object::get_latest(state.store.db(), "test-bucket", "prefix/file.txt")
                .await
                .unwrap();
        assert_eq!(obj.cid, "QmPlain");
        assert_eq!(obj.size, 5);
        assert_eq!(obj.etag, "QmPlain");
        assert!(!obj.encrypted);
        assert!(obj.key_wrap.is_none());
    }

    #[tokio::test]
    async fn pin_add_error_does_not_remove_a_possibly_shared_cid() {
        use futures_util::stream;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmShared\",\"Size\":\"5\"}\n"),
            )
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(500).set_body_string("pin failed"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/rm"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&kubo)
            .await;

        let state = test_state(kubo.uri()).await;
        let err: s3s::S3Error = add_plain_object_stream(
            &state,
            stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
                b"hello",
            ))]),
        )
        .await
        .unwrap_err()
        .into();

        assert_eq!(err.code().as_str(), "InternalError");
        let requests = kubo.received_requests().await.unwrap();
        assert!(!requests.iter().any(|request| {
            request.url.path() == "/api/v0/pin/rm" && request.url.query() == Some("arg=QmShared")
        }));
    }

    #[tokio::test]
    async fn publish_failure_keeps_the_successfully_pinned_cid() {
        use futures_util::stream;
        use sea_orm::ConnectionTrait;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmShared\",\"Size\":\"5\"}\n"),
            )
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmShared\"]}"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/rm"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&kubo)
            .await;

        let state = test_state(kubo.uri()).await;
        let stored = add_plain_object_stream(
            &state,
            stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
                b"hello",
            ))]),
        )
        .await
        .unwrap();

        state
            .store
            .db()
            .execute_unprepared("DROP TABLE objects")
            .await
            .unwrap();
        let result = publish_plain_object(
            &state,
            "test-bucket",
            "prefix/file.txt",
            None,
            None,
            &stored,
            false,
        )
        .await;

        assert!(result.is_err());
        let requests = kubo.received_requests().await.unwrap();
        assert!(!requests.iter().any(|request| {
            request.url.path() == "/api/v0/pin/rm" && request.url.query() == Some("arg=QmShared")
        }));
    }

    #[tokio::test]
    async fn delete_objects_preserves_duplicates_order_quiet_and_per_item_errors() {
        let kubo = kubo_server("unused-add-response").await;
        let state = pinning_state(kubo.uri(), "always", "one", "").await;
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            crate::store::object_version::BucketVersioningState::Enabled,
        )
        .await
        .unwrap();
        publish_seed(&state, "batch-a", "a", "bafy-a", vec![]).await;
        publish_seed(&state, "batch-b", "b", "bafy-b", vec![]).await;
        let retained_a = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq("bucket"))
            .filter(object_version::Column::Key.eq("a"))
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap()
            .version_id
            .unwrap();
        let unknown = "00000000-0000-0000-0000-000000000001".to_owned();

        let output = delete_objects(
            &state,
            delete_objects_version_request(
                vec![
                    ("a".to_owned(), None),
                    ("a".to_owned(), None),
                    ("a".to_owned(), Some(retained_a.clone())),
                    ("bad".to_owned(), Some("not-a-version".to_owned())),
                    ("missing".to_owned(), Some(unknown.clone())),
                    ("b".to_owned(), None),
                ],
                false,
            ),
        )
        .await
        .unwrap()
        .output;
        let deleted = output.deleted.unwrap();
        assert_eq!(
            deleted
                .iter()
                .map(|deleted| deleted.key.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("a"), Some("a"), Some("b")]
        );
        assert_eq!(deleted[0].delete_marker, Some(true));
        assert_eq!(deleted[1].delete_marker, Some(true));
        assert_ne!(
            deleted[0].delete_marker_version_id,
            deleted[1].delete_marker_version_id
        );
        assert_eq!(deleted[0].version_id, None);
        assert_eq!(deleted[1].version_id, None);
        assert_eq!(deleted[2].version_id.as_deref(), Some(retained_a.as_str()));
        assert_eq!(deleted[2].delete_marker, None);
        assert_eq!(deleted[3].delete_marker, Some(true));
        assert_eq!(
            output
                .errors
                .as_ref()
                .unwrap()
                .iter()
                .map(|error| {
                    (
                        error.code.as_deref(),
                        error.key.as_deref(),
                        error.version_id.as_deref(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (Some("InvalidArgument"), Some("bad"), Some("not-a-version")),
                (
                    Some("NoSuchVersion"),
                    Some("missing"),
                    Some(unknown.as_str())
                ),
            ]
        );

        let before_quiet = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq("bucket"))
            .filter(object_version::Column::Key.eq("a"))
            .count(state.store.db())
            .await
            .unwrap();
        let quiet = delete_objects(
            &state,
            delete_objects_version_request(
                vec![
                    ("a".to_owned(), None),
                    ("a".to_owned(), None),
                    ("bad".to_owned(), Some("still-not-a-version".to_owned())),
                ],
                true,
            ),
        )
        .await
        .unwrap()
        .output;
        assert_eq!(quiet.deleted, None);
        assert_eq!(
            quiet.errors.as_ref().unwrap()[0].code.as_deref(),
            Some("InvalidArgument")
        );
        assert_eq!(
            object_version::Entity::find()
                .filter(object_version::Column::Bucket.eq("bucket"))
                .filter(object_version::Column::Key.eq("a"))
                .count(state.store.db())
                .await
                .unwrap(),
            before_quiet + 2
        );
        assert_eq!(
            delete_objects_item_error(&AppError::StaleContentMutation),
            (
                "OperationAborted".to_owned(),
                "content mutation was superseded by a newer operation".to_owned()
            )
        );
    }

    #[tokio::test]
    async fn delete_objects_nonquiet_is_idempotent_and_preserves_request_order() {
        let state = list_state_with_keys(&["a", "b"]).await;

        let output = delete_objects(
            &state,
            delete_objects_request(&["a", "missing", "a", "b"], false),
        )
        .await
        .unwrap()
        .output;

        assert_eq!(
            output
                .deleted
                .as_ref()
                .unwrap()
                .iter()
                .map(|object| object.key.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("missing"), Some("a"), Some("b")]
        );
        assert_eq!(output.errors, None);
        for key in ["a", "b"] {
            assert!(matches!(
                crate::store::object::get_latest(state.store.db(), "bucket", key).await,
                Err(crate::error::AppError::NoSuchKey(_))
            ));
        }
    }

    #[tokio::test]
    async fn delete_objects_quiet_executes_deletes_without_deleted_output() {
        let state = list_state_with_keys(&["a", "b"]).await;

        let output = delete_objects(&state, delete_objects_request(&["a", "missing", "b"], true))
            .await
            .unwrap()
            .output;

        assert_eq!(output.deleted, None);
        assert_eq!(output.errors, None);
        for key in ["a", "b"] {
            assert!(matches!(
                crate::store::object::get_latest(state.store.db(), "bucket", key).await,
                Err(crate::error::AppError::NoSuchKey(_))
            ));
        }
    }

    #[tokio::test]
    async fn delete_objects_missing_bucket_returns_no_such_bucket() {
        let state = test_state("http://127.0.0.1:5001".to_owned()).await;

        let error = delete_objects(&state, delete_objects_request(&["a"], false))
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "NoSuchBucket");
    }

    #[tokio::test]
    async fn delete_objects_returns_per_key_errors_and_continues_after_database_errors() {
        use sea_orm::ConnectionTrait;

        let state = list_state_with_keys(&["a", "b"]).await;
        state
            .store
            .db()
            .execute_unprepared("DROP TABLE objects")
            .await
            .unwrap();

        let output = delete_objects(&state, delete_objects_request(&["a", "b"], false))
            .await
            .unwrap()
            .output;

        assert_eq!(output.deleted, None);
        assert_eq!(
            output
                .errors
                .as_ref()
                .unwrap()
                .iter()
                .map(|error| {
                    (
                        error.code.as_deref(),
                        error.key.as_deref(),
                        error.message.as_deref(),
                        error.version_id.as_deref(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (
                    Some("InternalError"),
                    Some("a"),
                    Some("failed to delete object"),
                    None,
                ),
                (
                    Some("InternalError"),
                    Some("b"),
                    Some("failed to delete object"),
                    None,
                ),
            ]
        );
    }

    #[tokio::test]
    async fn list_objects_v1_marker_is_exclusive_and_fields_are_echoed() {
        let state = list_state_with_keys(&["a", "b", "c"]).await;
        let output = list_objects(
            &state,
            list_v1_request(ListObjectsInput {
                bucket: "bucket".to_owned(),
                delimiter: Some(String::new()),
                encoding_type: Some(EncodingType::from_static(EncodingType::URL)),
                marker: Some("a".to_owned()),
                max_keys: Some(1),
                prefix: Some(String::new()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .output;

        assert_eq!(
            output
                .contents
                .as_ref()
                .unwrap()
                .iter()
                .filter_map(|object| object.key.as_deref())
                .collect::<Vec<_>>(),
            vec!["b"]
        );
        assert_eq!(output.name.as_deref(), Some("bucket"));
        assert_eq!(output.prefix.as_deref(), Some(""));
        assert_eq!(output.delimiter.as_deref(), Some(""));
        assert_eq!(output.marker.as_deref(), Some("a"));
        assert_eq!(output.max_keys, Some(1));
        assert_eq!(
            output.encoding_type.as_ref().map(EncodingType::as_str),
            Some("url")
        );
        assert_eq!(output.is_truncated, Some(true));
        assert_eq!(output.next_marker.as_deref(), Some("b"));
    }

    #[test]
    fn rfc3986_url_encoding_uses_utf8_uppercase_hex_and_unreserved_passthrough() {
        assert_eq!(
            rfc3986_url_encode("AZaz09-._~/ %()é"),
            "AZaz09-._~%2F%20%25%28%29%C3%A9"
        );
    }

    #[tokio::test]
    async fn list_objects_url_encoding_projects_wire_fields_without_changing_raw_cursors() {
        let raw_prefix = "prefix/";
        let raw_object = "prefix/a%2F(é)";
        let raw_common_key = "prefix/dir%2F(é)/one";
        let raw_start_after = "ignored/%2F(é)";
        let state = list_state_with_keys(&[raw_object, raw_common_key, "prefix/z"]).await;

        let first_v1 = list_objects(
            &state,
            list_v1_request(ListObjectsInput {
                bucket: "bucket".to_owned(),
                prefix: Some(raw_prefix.to_owned()),
                delimiter: Some("/".to_owned()),
                marker: Some(raw_prefix.to_owned()),
                max_keys: Some(2),
                encoding_type: Some(EncodingType::from_static(EncodingType::URL)),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .output;

        assert_eq!(first_v1.name.as_deref(), Some("bucket"));
        assert_eq!(first_v1.prefix.as_deref(), Some("prefix%2F"));
        assert_eq!(first_v1.delimiter.as_deref(), Some("%2F"));
        assert_eq!(first_v1.marker.as_deref(), Some("prefix%2F"));
        assert_eq!(
            first_v1
                .contents
                .as_ref()
                .unwrap()
                .iter()
                .filter_map(|object| object.key.as_deref())
                .collect::<Vec<_>>(),
            vec!["prefix%2Fa%252F%28%C3%A9%29"]
        );
        assert_eq!(
            first_v1
                .common_prefixes
                .as_ref()
                .unwrap()
                .iter()
                .filter_map(|prefix| prefix.prefix.as_deref())
                .collect::<Vec<_>>(),
            vec!["prefix%2Fdir%252F%28%C3%A9%29%2F"]
        );
        assert_eq!(
            first_v1.next_marker.as_deref(),
            Some("prefix%2Fdir%252F%28%C3%A9%29%2Fone")
        );

        let second_v1 = list_objects(
            &state,
            list_v1_request(ListObjectsInput {
                bucket: "bucket".to_owned(),
                prefix: Some(raw_prefix.to_owned()),
                delimiter: Some("/".to_owned()),
                marker: Some(raw_common_key.to_owned()),
                max_keys: Some(2),
                encoding_type: Some(EncodingType::from_static(EncodingType::URL)),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .output;
        assert_eq!(
            second_v1.marker.as_deref(),
            Some("prefix%2Fdir%252F%28%C3%A9%29%2Fone")
        );
        assert_eq!(
            second_v1
                .contents
                .as_ref()
                .unwrap()
                .iter()
                .filter_map(|object| object.key.as_deref())
                .collect::<Vec<_>>(),
            vec!["prefix%2Fz"]
        );
        assert!(second_v1.common_prefixes.is_none());
        assert_eq!(second_v1.next_marker, None);

        let v2 = list_objects_v2(
            &state,
            list_v2_request(ListObjectsV2Input {
                bucket: "bucket".to_owned(),
                prefix: Some(raw_prefix.to_owned()),
                delimiter: Some("/".to_owned()),
                continuation_token: Some(raw_prefix.to_owned()),
                start_after: Some(raw_start_after.to_owned()),
                max_keys: Some(2),
                encoding_type: Some(EncodingType::from_static(EncodingType::URL)),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .output;

        assert_eq!(v2.name.as_deref(), Some("bucket"));
        assert_eq!(v2.prefix.as_deref(), Some("prefix%2F"));
        assert_eq!(v2.delimiter.as_deref(), Some("%2F"));
        assert_eq!(
            v2.start_after.as_deref(),
            Some("ignored%2F%252F%28%C3%A9%29")
        );
        assert_eq!(v2.continuation_token.as_deref(), Some(raw_prefix));
        assert_eq!(v2.next_continuation_token.as_deref(), Some(raw_common_key));
        assert_eq!(
            v2.contents
                .as_ref()
                .unwrap()
                .iter()
                .filter_map(|object| object.key.as_deref())
                .collect::<Vec<_>>(),
            vec!["prefix%2Fa%252F%28%C3%A9%29"]
        );
        assert_eq!(
            v2.common_prefixes
                .as_ref()
                .unwrap()
                .iter()
                .filter_map(|prefix| prefix.prefix.as_deref())
                .collect::<Vec<_>>(),
            vec!["prefix%2Fdir%252F%28%C3%A9%29%2F"]
        );
    }

    #[tokio::test]
    async fn list_objects_v1_untruncated_page_omits_next_marker() {
        let state = list_state_with_keys(&["a", "b"]).await;
        let output = list_objects(
            &state,
            list_v1_request(ListObjectsInput {
                bucket: "bucket".to_owned(),
                marker: Some("a".to_owned()),
                max_keys: Some(1000),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .output;

        assert_eq!(output.is_truncated, Some(false));
        assert_eq!(output.next_marker, None);
        assert_eq!(
            output
                .contents
                .unwrap()
                .into_iter()
                .filter_map(|object| object.key)
                .collect::<Vec<_>>(),
            vec!["b"]
        );
    }

    #[tokio::test]
    async fn list_objects_v1_delimiter_next_marker_tracks_last_consumed_row() {
        let state = list_state_with_keys(&["a", "photos/1", "photos/2", "videos/1"]).await;
        let first = list_objects(
            &state,
            list_v1_request(ListObjectsInput {
                bucket: "bucket".to_owned(),
                delimiter: Some("/".to_owned()),
                max_keys: Some(2),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .output;

        assert_eq!(
            first
                .contents
                .as_ref()
                .unwrap()
                .iter()
                .filter_map(|object| object.key.as_deref())
                .collect::<Vec<_>>(),
            vec!["a"]
        );
        assert_eq!(
            first
                .common_prefixes
                .as_ref()
                .unwrap()
                .iter()
                .filter_map(|prefix| prefix.prefix.as_deref())
                .collect::<Vec<_>>(),
            vec!["photos/"]
        );
        assert_eq!(first.is_truncated, Some(true));
        assert_eq!(first.next_marker.as_deref(), Some("photos/2"));

        let second = list_objects(
            &state,
            list_v1_request(ListObjectsInput {
                bucket: "bucket".to_owned(),
                delimiter: Some("/".to_owned()),
                marker: first.next_marker,
                max_keys: Some(2),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .output;

        assert_eq!(
            second
                .common_prefixes
                .unwrap()
                .into_iter()
                .filter_map(|prefix| prefix.prefix)
                .collect::<Vec<_>>(),
            vec!["videos/"]
        );
        assert_eq!(second.is_truncated, Some(false));
        assert_eq!(second.next_marker, None);
    }

    #[tokio::test]
    async fn list_objects_v2_continuation_token_still_precedes_start_after() {
        let state = list_state_with_keys(&["a", "b", "c", "d"]).await;
        let output = list_objects_v2(
            &state,
            list_v2_request(ListObjectsV2Input {
                bucket: "bucket".to_owned(),
                continuation_token: Some("b".to_owned()),
                start_after: Some("c".to_owned()),
                max_keys: Some(1000),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .output;

        assert_eq!(
            output
                .contents
                .unwrap()
                .into_iter()
                .filter_map(|object| object.key)
                .collect::<Vec<_>>(),
            vec!["c", "d"]
        );
        assert_eq!(output.continuation_token.as_deref(), Some("b"));
        assert_eq!(output.start_after.as_deref(), Some("c"));
    }

    #[tokio::test]
    async fn list_objects_v2_sets_common_prefixes_when_delimiter_is_present() {
        let state = list_state_with_keys(&[
            "a.txt",
            "photos/cat.jpg",
            "photos/dog.jpg",
            "videos/clip.mp4",
        ])
        .await;
        let input = ListObjectsV2Input {
            bucket: "bucket".to_string(),
            prefix: Some("".to_string()),
            delimiter: Some("/".to_string()),
            max_keys: Some(1000),
            ..Default::default()
        };
        let resp = list_objects_v2(&state, list_v2_request(input))
            .await
            .unwrap()
            .output;

        let keys: Vec<_> = resp
            .contents
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|o| o.key.as_deref())
            .collect();
        let prefixes: Vec<_> = resp
            .common_prefixes
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|p| p.prefix.as_deref())
            .collect();

        assert_eq!(keys, vec!["a.txt"]);
        assert_eq!(prefixes, vec!["photos/", "videos/"]);
        assert_eq!(resp.key_count, Some(3));
        assert_eq!(resp.prefix, Some("".to_string()));
        assert_eq!(resp.delimiter, Some("/".to_string()));
        assert_eq!(resp.is_truncated, Some(false));
    }

    #[tokio::test]
    async fn list_objects_v2_uses_start_after_when_no_continuation_token_exists() {
        let state = list_state_with_keys(&["a.txt", "b.txt", "c.txt"]).await;
        let input = ListObjectsV2Input {
            bucket: "bucket".to_string(),
            start_after: Some("a.txt".to_string()),
            max_keys: Some(1000),
            ..Default::default()
        };
        let resp = list_objects_v2(&state, list_v2_request(input))
            .await
            .unwrap()
            .output;

        let keys: Vec<_> = resp
            .contents
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|o| o.key.as_deref())
            .collect();
        assert_eq!(keys, vec!["b.txt", "c.txt"]);
        assert_eq!(resp.is_truncated, Some(false));
    }

    #[tokio::test]
    async fn list_objects_v2_scans_past_duplicate_prefix_rows_to_detect_truncation() {
        let state = list_state_with_keys(&[
            "a.txt",
            "photos/cat.jpg",
            "photos/dog.jpg",
            "videos/clip.mp4",
        ])
        .await;

        let first_input = ListObjectsV2Input {
            bucket: "bucket".to_string(),
            prefix: Some("".to_string()),
            delimiter: Some("/".to_string()),
            max_keys: Some(2),
            ..Default::default()
        };
        let first = list_objects_v2(&state, list_v2_request(first_input))
            .await
            .unwrap()
            .output;

        let keys: Vec<_> = first
            .contents
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|o| o.key.as_deref())
            .collect();
        let prefixes: Vec<_> = first
            .common_prefixes
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|p| p.prefix.as_deref())
            .collect();

        assert_eq!(first.key_count, Some(2));
        assert_eq!(keys, vec!["a.txt"]);
        assert_eq!(prefixes, vec!["photos/"]);
        assert_eq!(first.is_truncated, Some(true));
        let next_token = first.next_continuation_token.clone().unwrap();
        assert_eq!(next_token, "photos/dog.jpg");

        let second_input = ListObjectsV2Input {
            bucket: "bucket".to_string(),
            prefix: Some("".to_string()),
            delimiter: Some("/".to_string()),
            continuation_token: Some(next_token),
            max_keys: Some(2),
            ..Default::default()
        };
        let second = list_objects_v2(&state, list_v2_request(second_input))
            .await
            .unwrap()
            .output;

        let second_prefixes: Vec<_> = second
            .common_prefixes
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|p| p.prefix.as_deref())
            .collect();
        assert_eq!(second_prefixes, vec!["videos/"]);
        assert_eq!(second.is_truncated, Some(false));
    }

    #[tokio::test]
    async fn ordinary_lists_hide_noncurrent_and_markers() {
        use crate::store::object_version::BucketVersioningState;

        let state = list_state_with_keys(&[]).await;
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            BucketVersioningState::Enabled,
        )
        .await
        .unwrap();

        async fn publish(state: &Arc<AppState>, id: &str, key: &str, cid: &str) -> String {
            crate::store::object::upsert(
                state.store.db(),
                id,
                "bucket",
                key,
                cid,
                1,
                None,
                cid,
                None,
                false,
                None,
                None,
                false,
            )
            .await
            .unwrap();
            let object = crate::store::object::get_by_id(state.store.db(), id)
                .await
                .unwrap();
            state
                .store
                .db()
                .transaction(move |txn| {
                    Box::pin(async move {
                        crate::store::object_version::install_content_version(
                            txn,
                            BucketVersioningState::Enabled,
                            &object,
                            Utc::now(),
                        )
                        .await
                    })
                })
                .await
                .unwrap()
        }

        publish(&state, "visible-old", "visible", "QmVisibleOld").await;
        publish(&state, "visible-new", "visible", "QmVisibleNew").await;
        publish(&state, "deleted-content", "deleted", "QmDeleted").await;
        state
            .store
            .db()
            .transaction(|txn| {
                Box::pin(async move {
                    crate::store::object_version::install_delete_marker(
                        txn,
                        BucketVersioningState::Enabled,
                        "bucket",
                        "deleted",
                        Utc::now(),
                    )
                    .await
                })
            })
            .await
            .unwrap();

        let v1 = list_objects(
            &state,
            list_v1_request(ListObjectsInput {
                bucket: "bucket".to_owned(),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .output;
        assert_eq!(
            v1.contents
                .unwrap()
                .into_iter()
                .filter_map(|object| object.key)
                .collect::<Vec<_>>(),
            vec!["visible"]
        );

        let v2 = list_objects_v2(
            &state,
            list_v2_request(ListObjectsV2Input {
                bucket: "bucket".to_owned(),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .output;
        assert_eq!(
            v2.contents
                .unwrap()
                .into_iter()
                .filter_map(|object| object.key)
                .collect::<Vec<_>>(),
            vec!["visible"]
        );
        assert_eq!(v2.key_count, Some(1));
    }

    #[test]
    fn listing_fold_without_delimiter_returns_flat_objects() {
        let rows = vec![
            object_model("a.txt"),
            object_model("b.txt"),
            object_model("c.txt"),
        ];
        let page = fold_listing_rows(rows, "", None, 1000);
        assert_eq!(page.object_keys(), vec!["a.txt", "b.txt", "c.txt"]);
        assert!(page.common_prefixes().is_empty());
        assert!(!page.is_truncated);
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn listing_fold_with_delimiter_returns_objects_and_common_prefixes() {
        let rows = vec![
            object_model("a.txt"),
            object_model("photos/cat.jpg"),
            object_model("photos/dog.jpg"),
            object_model("b.txt"),
        ];
        let page = fold_listing_rows(rows, "", Some("/"), 1000);
        assert_eq!(page.object_keys(), vec!["a.txt", "b.txt"]);
        assert_eq!(page.common_prefixes(), vec!["photos/"]);
        assert!(!page.is_truncated);
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn listing_fold_with_prefix_and_delimiter_scopes_common_prefixes() {
        let rows = vec![
            object_model("photos/2024/jan.jpg"),
            object_model("photos/2024/feb.jpg"),
            object_model("photos/2025/mar.jpg"),
        ];
        let page = fold_listing_rows(rows, "photos/", Some("/"), 1000);
        assert!(page.object_keys().is_empty());
        assert_eq!(page.common_prefixes(), vec!["photos/2024/", "photos/2025/"]);
        assert!(!page.is_truncated);
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn listing_fold_counts_prefix_once_and_tracks_last_consumed_row() {
        let rows = vec![
            object_model("a.txt"),
            object_model("photos/cat.jpg"),
            object_model("photos/dog.jpg"),
            object_model("videos/clip.mp4"),
        ];
        let page = fold_listing_rows(rows, "", Some("/"), 2);
        assert_eq!(page.object_keys(), vec!["a.txt"]);
        assert_eq!(page.common_prefixes(), vec!["photos/"]);
        assert!(page.is_truncated);
        assert_eq!(page.next_cursor.as_deref(), Some("photos/dog.jpg"));
    }

    #[test]
    fn test_resolve_range_none_returns_full_object() {
        let total = 1000u64;
        let (start, end) = resolve_range(None, total).unwrap();
        assert_eq!(start, 0);
        assert_eq!(end, total);
    }

    #[test]
    fn test_resolve_range_explicit() {
        // bytes=100-199 → half-open [100, 200)
        let range = Range::Int {
            first: 100,
            last: Some(199),
        };
        let total = 1000u64;
        let (start, end) = resolve_range(Some(&range), total).unwrap();
        assert_eq!(start, 100);
        assert_eq!(end, 200);
    }

    #[test]
    fn test_resolve_range_suffix() {
        // bytes=-50 → last 50 bytes → [950, 1000)
        let range = Range::Suffix { length: 50 };
        let total = 1000u64;
        let (start, end) = resolve_range(Some(&range), total).unwrap();
        assert_eq!(start, 950);
        assert_eq!(end, 1000);
    }
}
