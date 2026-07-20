use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use bytes::Bytes;
use futures_util::{Stream, StreamExt, TryStreamExt};
use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};

use crate::crypto::EncryptionMode;
use crate::state::AppState;

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
) -> S3Result<StoredObject>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    let (counter, count_handle) = ByteCounter::new();
    let counted = counter.wrap(stream);
    let cid = crate::kubo::add::stream_add(&state.kubo, counted, 1)
        .await
        .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?;

    if let Err(e) = crate::kubo::pin::pin_add(&state.kubo, &cid).await {
        return Err(s3s::s3_error!(InternalError, "pin: {e}"));
    }

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
) -> S3Result<()> {
    let object_id = uuid::Uuid::new_v4().to_string();
    if let Err(e) = crate::store::object::upsert(
        state.store.db(),
        &object_id,
        bucket,
        key,
        &stored.cid,
        stored.size,
        content_type,
        &stored.cid,
        metadata,
        false,
        None,
        None,
        multipart,
    )
    .await
    {
        return Err(e.into());
    }
    Ok(())
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
    legacy_plaintext: Option<Vec<u8>>,
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

async fn collect_legacy_sse_c_plaintext(
    state: &Arc<AppState>,
    obj: &crate::store::entities::object::Model,
    key: Arc<crate::crypto::ObjectKey>,
    collect_plaintext: bool,
) -> S3Result<Option<Vec<u8>>> {
    let cat = crate::kubo::cat::stream_cat(&state.kubo, &obj.cid, None)
        .await
        .map_err(|e| s3s::s3_error!(InternalError, "cat: {e}"))?;
    let decrypted = crate::crypto::chunker::decrypt_chunk_stream(cat, key);
    tokio::pin!(decrypted);
    let mut observed = 0_i64;
    let mut plaintext = collect_plaintext.then(Vec::new);
    while let Some(chunk) = decrypted.next().await {
        let chunk = chunk.map_err(|error| match error {
            crate::error::AppError::Crypto(_) => {
                s3s::s3_error!(AccessDenied, "SSE-C object authentication failed")
            }
            other => s3s::s3_error!(InternalError, "decrypt: {other}"),
        })?;
        let len = i64::try_from(chunk.len())
            .map_err(|_| s3s::s3_error!(AccessDenied, "SSE-C object size mismatch"))?;
        observed = observed
            .checked_add(len)
            .ok_or_else(|| s3s::s3_error!(AccessDenied, "SSE-C object size mismatch"))?;
        if let Some(plaintext) = plaintext.as_mut() {
            plaintext.extend_from_slice(&chunk);
        }
    }
    if observed == 0 || observed != obj.size {
        return Err(s3s::s3_error!(AccessDenied, "SSE-C object size mismatch"));
    }
    Ok(plaintext)
}

async fn authenticate_sse_c_object(
    state: &Arc<AppState>,
    obj: &crate::store::entities::object::Model,
    headers: ValidatedSseCHeaders,
    collect_legacy_plaintext: bool,
) -> S3Result<AuthenticatedSseCObject> {
    if let Some(fingerprint) = obj.sse_c_key_fingerprint.as_deref() {
        verify_object_sse_c_fingerprint(state, fingerprint, &headers.key)?;
        return Ok(AuthenticatedSseCObject {
            key: Arc::new(headers.key),
            key_md5: headers.key_md5,
            fingerprint: fingerprint.to_owned(),
            legacy_plaintext: None,
        });
    }

    let key = Arc::new(headers.key);
    let plaintext =
        collect_legacy_sse_c_plaintext(state, obj, key.clone(), collect_legacy_plaintext).await?;
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
        legacy_plaintext: plaintext,
    })
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

pub async fn put_object(
    state: &Arc<AppState>,
    req: S3Request<PutObjectInput>,
) -> S3Result<S3Response<PutObjectOutput>> {
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let content_type = req.input.content_type.clone();
    let db = state.store.db();

    // Validate the bucket exists.
    let exists = crate::store::bucket::exists(db, bucket).await?;
    if !exists {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {}", bucket));
    }

    let enc_mode = determine_encryption_mode(&req.headers)?;
    let metadata = extract_custom_metadata(&req.headers);
    let object_id = uuid::Uuid::new_v4().to_string();

    // Extract the body stream (Option<StreamingBlob>).
    let body = req
        .input
        .body
        .ok_or_else(|| s3s::s3_error!(IncompleteBody, "request body is missing"))?;

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
            let cid = crate::kubo::add::stream_add(&state.kubo, stream, 1)
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?;
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
            let cid = crate::kubo::add::stream_add(&state.kubo, encrypted_stream, 1)
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?;
            (cid, true, Some(wrapped), None)
        }
        EncryptionMode::SseC => {
            let validated = extract_sse_c_headers(&req.headers)?;
            let fingerprint = state.master_key.sse_c_key_fingerprint(&validated.key);
            let pinned = Box::pin(stream);
            let encrypted_stream =
                crate::crypto::chunker::encrypt_chunk_stream(pinned, Arc::new(validated.key));
            let cid = crate::kubo::add::stream_add(&state.kubo, encrypted_stream, 1)
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?;
            (cid, true, None, Some(fingerprint))
        }
    };

    let size = count_handle.load(Ordering::Relaxed) as i64;

    // Pin the CID. If pin fails, the CID is already in Kubo (from stream_add)
    // but unpinned; best-effort clean up by pin::rm (which is a no-op if not
    // pinned) so it can be GC'd later.
    if let Err(e) = crate::kubo::pin::pin_add(&state.kubo, &cid).await {
        let _ = crate::kubo::pin::pin_rm(&state.kubo, &cid).await;
        return Err(s3s::s3_error!(InternalError, "pin: {e}"));
    }

    // Store metadata. If DB fails, unpin so the CID can be GC'd.
    if let Err(e) = crate::store::object::upsert(
        db,
        &object_id,
        bucket,
        key,
        &cid,
        size,
        content_type.as_deref(),
        &cid,
        metadata,
        encrypted,
        key_wrap.as_deref(),
        sse_c_key_fingerprint.as_deref(),
        false,
    )
    .await
    {
        let _ = crate::kubo::pin::pin_rm(&state.kubo, &cid).await;
        return Err(e.into());
    }

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
            ..Default::default()
        },
        headers,
    ))
}

pub async fn get_object(
    state: &Arc<AppState>,
    req: S3Request<GetObjectInput>,
) -> S3Result<S3Response<GetObjectOutput>> {
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let db = state.store.db();

    let obj = crate::store::object::get_latest(db, bucket, key).await?;

    let has_range = req.input.range.is_some();
    let is_sse_c = obj.encrypted && obj.key_wrap.is_none();
    let mut sse_c_auth = if is_sse_c {
        Some(
            authenticate_sse_c_object(state, &obj, extract_sse_c_headers(&req.headers)?, has_range)
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
                state,
                start,
                end,
                has_range,
                sse_customer_key_md5,
            )
            .await;
        };

        let ok_arc = Arc::new(ok);

        if has_range {
            // Encrypted objects are chunked, so we cannot ask Kubo for a byte
            // range directly. Collect the decrypted plaintext and slice it.
            // (MVP trade-off: v0.9 will optimize to chunk-level Range.)
            let cat_stream = crate::kubo::cat::stream_cat(&state.kubo, &obj.cid, None)
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "cat: {e}"))?;
            let decrypted = crate::crypto::chunker::decrypt_chunk_stream(cat_stream, ok_arc);
            let chunks: Vec<Bytes> = decrypted.try_collect().await.map_err(|e| match e {
                crate::error::AppError::Crypto(_) => s3s::s3_error!(
                    AccessDenied,
                    "decryption failed — SSE-C key may not match the key used during upload"
                ),
                other => s3s::s3_error!(InternalError, "decrypt: {other}"),
            })?;
            let mut collected = Vec::with_capacity(chunks.iter().map(Bytes::len).sum());
            for chunk in chunks {
                collected.extend_from_slice(&chunk);
            }

            let s = start as usize;
            let e = (end as usize).min(collected.len());
            if s > e {
                return Err(s3s::s3_error!(
                    InvalidRange,
                    "requested range exceeds available decrypted data"
                ));
            }
            let sliced = collected[s..e].to_vec();
            let stream =
                futures_util::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(sliced))]);
            StreamingBlob::wrap(stream)
        } else {
            // No Range: stream decrypted plaintext directly without collecting.
            // Clone KuboClient + cid + ObjectKey into the 'static stream.
            let kubo = state.kubo.clone();
            let cid = obj.cid.clone();
            let ok_clone = ok_arc.clone();
            let stream = async_stream::stream! {
                let cat = crate::kubo::cat::stream_cat(&kubo, &cid, None)
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                let decrypted = crate::crypto::chunker::decrypt_chunk_stream(cat, ok_clone);
                let mut s = Box::pin(decrypted);
                while let Some(chunk) = s.next().await {
                    match chunk {
                        Ok(b) => yield Ok(b),
                        Err(crate::error::AppError::Crypto(_)) => {
                            yield Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "decryption failed — SSE-C key mismatch"));
                        }
                        Err(e) => {
                            yield Err(std::io::Error::other(e.to_string()));
                        }
                    }
                }
            };
            StreamingBlob::wrap(stream)
        }
    } else {
        // Plaintext: stream directly from Kubo without collecting into memory.
        let kubo = state.kubo.clone();
        let cid = obj.cid.clone();
        let kubo_range = if has_range { Some((start, end)) } else { None };
        let stream = async_stream::stream! {
            let cat = crate::kubo::cat::stream_cat(&kubo, &cid, kubo_range)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            tokio::pin!(cat);
            while let Some(chunk) = cat.next().await {
                yield chunk;
            }
        };
        StreamingBlob::wrap(stream)
    };

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
        ..Default::default()
    }))
}

async fn build_sse_c_get_response(
    obj: &crate::store::entities::object::Model,
    mut auth: AuthenticatedSseCObject,
    state: &Arc<AppState>,
    start: u64,
    end: u64,
    has_range: bool,
    sse_customer_key_md5: Option<String>,
) -> S3Result<S3Response<GetObjectOutput>> {
    let body = if has_range {
        let plaintext = if let Some(plaintext) = auth.legacy_plaintext.take() {
            plaintext
        } else {
            let cat = crate::kubo::cat::stream_cat(&state.kubo, &obj.cid, None)
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "cat: {e}"))?;
            let decrypted = crate::crypto::chunker::decrypt_chunk_stream(cat, auth.key.clone());
            let chunks: Vec<Bytes> =
                decrypted.try_collect().await.map_err(|error| match error {
                    crate::error::AppError::Crypto(_) => {
                        s3s::s3_error!(AccessDenied, "SSE-C object authentication failed")
                    }
                    other => s3s::s3_error!(InternalError, "decrypt: {other}"),
                })?;
            let mut plaintext = Vec::with_capacity(chunks.iter().map(Bytes::len).sum());
            for chunk in chunks {
                plaintext.extend_from_slice(&chunk);
            }
            plaintext
        };
        let start = usize::try_from(start)
            .map_err(|_| s3s::s3_error!(InvalidRange, "range start is too large"))?;
        let end = usize::try_from(end)
            .map_err(|_| s3s::s3_error!(InvalidRange, "range end is too large"))?;
        if end > plaintext.len() || start > end {
            return Err(s3s::s3_error!(
                InvalidRange,
                "requested range exceeds available decrypted data"
            ));
        }
        StreamingBlob::wrap(futures_util::stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::copy_from_slice(&plaintext[start..end])),
        ]))
    } else {
        // A legacy object's first request authenticated one complete cat above;
        // use a second cat here so the successful no-range response stays streaming.
        let kubo = state.kubo.clone();
        let cid = obj.cid.clone();
        let key = auth.key;
        let stream = async_stream::stream! {
            let cat = crate::kubo::cat::stream_cat(&kubo, &cid, None)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let decrypted = crate::crypto::chunker::decrypt_chunk_stream(cat, key);
            tokio::pin!(decrypted);
            while let Some(chunk) = decrypted.next().await {
                match chunk {
                    Ok(bytes) => yield Ok(bytes),
                    Err(crate::error::AppError::Crypto(_)) => {
                        yield Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "SSE-C object authentication failed",
                        ));
                        return;
                    }
                    Err(error) => yield Err(std::io::Error::other(error.to_string())),
                }
            }
        };
        StreamingBlob::wrap(stream)
    };

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
        ..Default::default()
    }))
}

pub async fn head_object(
    state: &Arc<AppState>,
    req: S3Request<HeadObjectInput>,
) -> S3Result<S3Response<HeadObjectOutput>> {
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let db = state.store.db();

    let obj = crate::store::object::get_latest(db, bucket, key).await?;
    let sse_c_auth = if obj.encrypted && obj.key_wrap.is_none() {
        Some(
            authenticate_sse_c_object(state, &obj, extract_sse_c_headers(&req.headers)?, false)
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

    crate::store::object::delete_latest(db, bucket, key).await?;

    Ok(S3Response::new(DeleteObjectOutput::default()))
}

pub async fn delete_objects(
    state: &Arc<AppState>,
    req: S3Request<DeleteObjectsInput>,
) -> S3Result<S3Response<DeleteObjectsOutput>> {
    let DeleteObjectsInput { bucket, delete, .. } = req.input;
    let db = state.store.db();

    if !crate::store::bucket::exists(db, &bucket).await? {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {}", bucket));
    }

    let quiet = delete.quiet.unwrap_or(false);
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for object in delete.objects {
        // v0.2 has no versioning; ObjectIdentifier::version_id is deliberately ignored.
        let key = object.key;
        match crate::store::object::delete_latest_if_present(db, &bucket, &key).await {
            Ok(_) if !quiet => deleted.push(DeletedObject {
                key: Some(key),
                ..Default::default()
            }),
            Ok(_) => {}
            Err(error) => {
                tracing::error!(%bucket, %key, %error, "failed to delete object");
                errors.push(Error {
                    code: Some("InternalError".to_owned()),
                    key: Some(key),
                    message: Some("failed to delete object".to_owned()),
                    version_id: None,
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
    let dst_bucket = &req.input.bucket;
    let dst_key = &req.input.key;
    let db = state.store.db();

    let (src_bucket, src_key) = match req.input.copy_source {
        CopySource::Bucket {
            ref bucket,
            ref key,
            ..
        } => (bucket.to_string(), key.to_string()),
        _ => {
            return Err(s3s::s3_error!(InvalidArgument, "unsupported copy source"));
        }
    };

    let src_obj = crate::store::object::get_latest(db, &src_bucket, &src_key).await?;
    let source_sse_c_headers = extract_copy_source_sse_c_headers(&req.headers)?;
    let verified_source_fingerprint = if src_obj.encrypted && src_obj.key_wrap.is_none() {
        let headers = source_sse_c_headers.ok_or_else(|| {
            s3s::s3_error!(
                InvalidArgument,
                "complete copy-source SSE-C headers are required"
            )
        })?;
        Some(
            authenticate_sse_c_object(state, &src_obj, headers, false)
                .await?
                .fingerprint,
        )
    } else {
        if source_sse_c_headers.is_some() {
            return Err(s3s::s3_error!(
                InvalidArgument,
                "copy-source SSE-C headers were provided for a non-SSE-C object"
            ));
        }
        None
    };

    // Validate destination bucket exists.
    let dst_exists = crate::store::bucket::exists(db, dst_bucket).await?;
    if !dst_exists {
        return Err(s3s::s3_error!(
            NoSuchBucket,
            "bucket not found: {}",
            dst_bucket
        ));
    }

    // Re-pin the (content-addressed) CID so the copy is independently pinned.
    crate::kubo::pin::pin_add(&state.kubo, &src_obj.cid)
        .await
        .map_err(|e| s3s::s3_error!(InternalError, "pin: {e}"))?;

    let new_id = uuid::Uuid::new_v4().to_string();

    crate::store::object::upsert(
        db,
        &new_id,
        dst_bucket,
        dst_key,
        &src_obj.cid,
        src_obj.size,
        src_obj.content_type.as_deref(),
        &src_obj.etag,
        src_obj.metadata.clone(),
        src_obj.encrypted,
        src_obj.key_wrap.as_deref(),
        verified_source_fingerprint.as_deref(),
        src_obj.multipart,
    )
    .await?;

    Ok(S3Response::new(CopyObjectOutput {
        copy_object_result: Some(CopyObjectResult {
            e_tag: Some(ETag::Strong(src_obj.etag.clone())),
            last_modified: Some(Timestamp::from(SystemTime::from(chrono::Utc::now()))),
            ..Default::default()
        }),
        ..Default::default()
    }))
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

fn normalized_max_keys(value: Option<i32>) -> usize {
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

fn url_encoding_requested(encoding_type: Option<&EncodingType>) -> bool {
    encoding_type.is_some_and(|encoding_type| encoding_type.as_str() == EncodingType::URL)
}

fn project_listing_field(value: &str, url_encode: bool) -> String {
    if url_encode {
        rfc3986_url_encode(value)
    } else {
        value.to_owned()
    }
}

fn project_optional_listing_field(value: Option<String>, url_encode: bool) -> Option<String> {
    value.map(|value| project_listing_field(&value, url_encode))
}

fn listing_dtos(entries: &[ListingEntry], url_encode: bool) -> (Vec<Object>, Vec<CommonPrefix>) {
    let mut contents = Vec::new();
    let mut common_prefixes = Vec::new();

    for entry in entries {
        match entry {
            ListingEntry::Object(model) => contents.push(Object {
                key: Some(project_listing_field(&model.key, url_encode)),
                size: Some(model.size),
                e_tag: Some(ETag::Strong(model.etag.clone())),
                last_modified: Some(Timestamp::from(SystemTime::from(model.created_at))),
                ..Default::default()
            }),
            ListingEntry::CommonPrefix { prefix, .. } => common_prefixes.push(CommonPrefix {
                prefix: Some(project_listing_field(prefix, url_encode)),
            }),
        }
    }

    (contents, common_prefixes)
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
    let (contents, common_prefixes) = listing_dtos(&page.entries, url_encode);

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
    let (contents, common_prefixes) = listing_dtos(&page.entries, url_encode);

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
    use crate::store::entities::object;
    use chrono::Utc;

    async fn test_state(kubo_uri: String) -> Arc<AppState> {
        use sea_orm::Database;

        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();

        Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new(kubo_uri),
            store: crate::store::Store::new(db),
            credentials: HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        })
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
            store,
            credentials,
            master_key,
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
        S3Request {
            input: DeleteObjectsInput {
                bucket: "bucket".to_owned(),
                bypass_governance_retention: None,
                checksum_algorithm: None,
                delete: Delete {
                    objects: keys
                        .iter()
                        .map(|key| ObjectIdentifier {
                            key: (*key).to_owned(),
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
        let err = add_plain_object_stream(
            &state,
            stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
                b"hello",
            ))]),
        )
        .await
        .unwrap_err();

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
