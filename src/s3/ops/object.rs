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

/// Determine the requested server-side encryption mode from request headers.
pub fn determine_encryption_mode(headers: &http::HeaderMap) -> S3Result<EncryptionMode> {
    if let Some(val) = headers.get("x-amz-server-side-encryption-customer-algorithm") {
        // SSE-C: customer provides key. Algorithm must be AES256.
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

/// Decode the SSE-C customer key from the `x-amz-server-side-encryption-customer-key`
/// header (base64-encoded, 32 bytes) and validate it against the
/// `x-amz-server-side-encryption-customer-key-MD5` header.
///
/// S3 requires the client to send the MD5 of the raw key so the server can
/// detect transmission corruption. We use constant-time comparison to avoid
/// timing side-channels.
pub fn extract_sse_c_key(headers: &http::HeaderMap) -> S3Result<crate::crypto::ObjectKey> {
    use base64::Engine;

    let key_b64 = headers
        .get("x-amz-server-side-encryption-customer-key")
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
        .get("x-amz-server-side-encryption-customer-key-MD5")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing SSE-C customer key MD5"))?;
    {
        let client_md5 = base64::engine::general_purpose::STANDARD
            .decode(md5_b64)
            .map_err(|e| s3s::s3_error!(InvalidArgument, "invalid SSE-C key-MD5: {e}"))?;
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
    Ok(crate::crypto::ObjectKey { bytes: ok_arr })
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

    let (cid, encrypted, key_wrap): (String, bool, Option<String>) = match enc_mode {
        EncryptionMode::None => {
            let cid = crate::kubo::add::stream_add(&state.kubo, stream, 1)
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?;
            (cid, false, None)
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
            let encrypted_stream = crate::crypto::chunker::encrypt_chunk_stream(
                pinned,
                Arc::new(ok),
                object_id.clone(),
                0, // single-object upload, part_number = 0
            );
            let cid = crate::kubo::add::stream_add(&state.kubo, encrypted_stream, 1)
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?;
            (cid, true, Some(wrapped))
        }
        EncryptionMode::SseC => {
            let ok = extract_sse_c_key(&req.headers)?;
            let pinned = Box::pin(stream);
            let encrypted_stream = crate::crypto::chunker::encrypt_chunk_stream(
                pinned,
                Arc::new(ok),
                object_id.clone(),
                0, // single-object upload, part_number = 0
            );
            let cid = crate::kubo::add::stream_add(&state.kubo, encrypted_stream, 1)
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?;
            (cid, true, None)
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
        false,
    )
    .await
    {
        let _ = crate::kubo::pin::pin_rm(&state.kubo, &cid).await;
        return Err(e.into());
    }

    Ok(S3Response::new(PutObjectOutput {
        e_tag: Some(ETag::Strong(cid.clone())),
        ..Default::default()
    }))
}

pub async fn get_object(
    state: &Arc<AppState>,
    req: S3Request<GetObjectInput>,
) -> S3Result<S3Response<GetObjectOutput>> {
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let db = state.store.db();

    let obj = crate::store::object::get_latest(db, bucket, key).await?;

    let range_ref = req.input.range.as_ref();
    let (start, end) = resolve_range(range_ref, obj.size as u64)?;
    let has_range = range_ref.is_some();

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
            // SSE-C: key provided by the caller.
            extract_sse_c_key(&req.headers)?
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

pub async fn head_object(
    state: &Arc<AppState>,
    req: S3Request<HeadObjectInput>,
) -> S3Result<S3Response<HeadObjectOutput>> {
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let db = state.store.db();

    let obj = crate::store::object::get_latest(db, bucket, key).await?;

    let server_side_encryption = if obj.encrypted && obj.key_wrap.is_some() {
        Some(ServerSideEncryption::from_static("AES256"))
    } else {
        None
    };

    Ok(S3Response::new(HeadObjectOutput {
        content_length: Some(obj.size),
        content_type: obj.content_type.clone(),
        e_tag: Some(ETag::Strong(obj.etag.clone())),
        last_modified: Some(Timestamp::from(SystemTime::from(obj.created_at))),
        server_side_encryption,
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

pub async fn list_objects_v2(
    state: &Arc<AppState>,
    req: S3Request<ListObjectsV2Input>,
) -> S3Result<S3Response<ListObjectsV2Output>> {
    let bucket = &req.input.bucket;
    let prefix = req.input.prefix.clone();
    let delimiter = req.input.delimiter.clone();
    let encoding_type = req.input.encoding_type.clone();
    let start_after = req.input.start_after.clone();
    let continuation_token = req.input.continuation_token.clone();
    let max_keys = req.input.max_keys.unwrap_or(1000).clamp(1, 1000) as u64;
    let db = state.store.db();

    let exists = crate::store::bucket::exists(db, bucket).await?;
    if !exists {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {}", bucket));
    }

    // Internal cursor: explicit continuation token takes priority over
    // StartAfter. Both are exclusive via `store::object::list`'s key > token
    // filter.
    let mut cursor = continuation_token
        .clone()
        .filter(|t| !t.is_empty())
        .or_else(|| start_after.clone().filter(|s| !s.is_empty()));
    let batch_limit = (max_keys + 1).max(1000);

    let mut builder = ListObjectsV2PageBuilder::new(
        prefix.as_deref().unwrap_or(""),
        delimiter.as_deref(),
        max_keys as usize,
    );

    loop {
        let rows = crate::store::object::list(
            db,
            bucket,
            prefix.as_deref(),
            cursor.as_deref(),
            batch_limit,
        )
        .await?;

        let exhausted = rows.len() < batch_limit as usize;
        for row in rows {
            cursor = Some(row.key.clone());
            if builder.push_row(row) == PushListEntryResult::PageComplete {
                break;
            }
        }

        if exhausted || builder.is_truncated {
            break;
        }
    }

    let page = builder.finish();

    let contents: Vec<Object> = page
        .entries
        .iter()
        .filter_map(|e| match e {
            ListObjectsV2Entry::Object(m) => Some(Object {
                key: Some(m.key.clone()),
                size: Some(m.size),
                e_tag: Some(ETag::Strong(m.etag.clone())),
                last_modified: Some(Timestamp::from(SystemTime::from(m.created_at))),
                ..Default::default()
            }),
            ListObjectsV2Entry::CommonPrefix { .. } => None,
        })
        .collect();

    let common_prefixes: Vec<CommonPrefix> = page
        .entries
        .iter()
        .filter_map(|e| match e {
            ListObjectsV2Entry::CommonPrefix { prefix, .. } => Some(CommonPrefix {
                prefix: Some(prefix.clone()),
            }),
            ListObjectsV2Entry::Object(_) => None,
        })
        .collect();

    Ok(S3Response::new(ListObjectsV2Output {
        contents: Some(contents),
        common_prefixes: (!common_prefixes.is_empty()).then_some(common_prefixes),
        is_truncated: Some(page.is_truncated),
        continuation_token: continuation_token.clone(),
        next_continuation_token: page.next_token,
        key_count: Some(page.entries.len() as i32),
        max_keys: Some(max_keys as i32),
        name: Some(bucket.clone()),
        prefix: Some(prefix.unwrap_or_default()),
        delimiter,
        encoding_type,
        start_after,
        ..Default::default()
    }))
}

// ---------------------------------------------------------------------------
// ListObjectsV2 delimiter / CommonPrefixes folding model
// ---------------------------------------------------------------------------

use crate::store::entities::object;

/// A single entry that may be returned by ListObjectsV2: either a concrete
/// object or a folded common prefix.
#[derive(Clone, Debug)]
enum ListObjectsV2Entry {
    Object(object::Model),
    CommonPrefix {
        prefix: String,
        /// The key of the most recent object row that contributed to this
        /// common prefix. Used to build the continuation token for the next
        /// page so pagination can resume after all consumed rows.
        continuation_key: String,
    },
}

impl ListObjectsV2Entry {
    #[cfg(test)]
    #[allow(dead_code)]
    fn key(&self) -> &str {
        match self {
            ListObjectsV2Entry::Object(m) => &m.key,
            ListObjectsV2Entry::CommonPrefix { prefix, .. } => prefix,
        }
    }
}

/// Result of folding one page of object rows.
#[derive(Clone, Debug)]
struct ListObjectsV2Page {
    entries: Vec<ListObjectsV2Entry>,
    is_truncated: bool,
    next_token: Option<String>,
}

#[cfg(test)]
impl ListObjectsV2Page {
    fn object_keys(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter_map(|e| match e {
                ListObjectsV2Entry::Object(m) => Some(m.key.as_str()),
                ListObjectsV2Entry::CommonPrefix { .. } => None,
            })
            .collect()
    }

    fn common_prefixes(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter_map(|e| match e {
                ListObjectsV2Entry::Object(_) => None,
                ListObjectsV2Entry::CommonPrefix { prefix, .. } => Some(prefix.as_str()),
            })
            .collect()
    }
}

/// Determine the common prefix for `key` under `prefix` and `delimiter`.
///
/// Returns `None` when there is no delimiter, when the key does not start with
/// `prefix`, or when the remaining suffix contains no delimiter.
fn common_prefix_for_key(key: &str, prefix: &str, delimiter: Option<&str>) -> Option<String> {
    let delimiter = delimiter?;
    let rest = key.strip_prefix(prefix)?;
    let idx = rest.find(delimiter)?;
    let end = idx + delimiter.len();
    Some(format!("{}{}", prefix, &rest[..end]))
}

/// Result of pushing a row into the page builder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PushListEntryResult {
    /// The row was consumed (added or merged) and the page can accept more.
    Continue,
    /// The row was not added because the page is full.
    PageComplete,
}

/// Builds a single ListObjectsV2 page with delimiter folding and max-keys
/// accounting.
struct ListObjectsV2PageBuilder {
    prefix: String,
    delimiter: Option<String>,
    max_keys: usize,
    entries: Vec<ListObjectsV2Entry>,
    /// Maps a common prefix string to its position in `entries`.
    common_prefix_positions: HashMap<String, usize>,
    /// The key of the last row that was returned/merged into this page.
    last_returned_token: Option<String>,
    /// Set to true once we determine that at least one additional row exists
    /// after `max_keys` entries have been returned.
    is_truncated: bool,
}

impl ListObjectsV2PageBuilder {
    fn new(prefix: &str, delimiter: Option<&str>, max_keys: usize) -> Self {
        Self {
            prefix: prefix.to_string(),
            delimiter: delimiter.map(|s| s.to_string()),
            max_keys,
            entries: Vec::new(),
            common_prefix_positions: HashMap::new(),
            last_returned_token: None,
            is_truncated: false,
        }
    }

    /// Fold a single object row into the page.
    fn push_row(&mut self, row: object::Model) -> PushListEntryResult {
        let key = row.key.clone();

        if let Some(common_prefix) =
            common_prefix_for_key(&key, &self.prefix, self.delimiter.as_deref())
        {
            if let Some(&pos) = self.common_prefix_positions.get(&common_prefix) {
                // Row merges into an already-returned common prefix: update
                // its continuation key and our last consumed token, but do
                // not increase the visible entry count.
                if let ListObjectsV2Entry::CommonPrefix {
                    ref mut continuation_key,
                    ..
                } = self.entries[pos]
                {
                    *continuation_key = key.clone();
                }
                self.last_returned_token = Some(key);
                return PushListEntryResult::Continue;
            }

            if self.entries.len() >= self.max_keys {
                self.is_truncated = true;
                return PushListEntryResult::PageComplete;
            }

            let pos = self.entries.len();
            self.entries.push(ListObjectsV2Entry::CommonPrefix {
                prefix: common_prefix.clone(),
                continuation_key: key.clone(),
            });
            self.common_prefix_positions.insert(common_prefix, pos);
            self.last_returned_token = Some(key);
            return PushListEntryResult::Continue;
        }

        if self.entries.len() >= self.max_keys {
            self.is_truncated = true;
            return PushListEntryResult::PageComplete;
        }

        self.entries.push(ListObjectsV2Entry::Object(row));
        self.last_returned_token = Some(key);
        PushListEntryResult::Continue
    }

    fn finish(mut self) -> ListObjectsV2Page {
        let next_token = if self.is_truncated {
            self.last_returned_token.take()
        } else {
            None
        };
        ListObjectsV2Page {
            entries: self.entries,
            is_truncated: self.is_truncated,
            next_token,
        }
    }
}

#[cfg(test)]
fn fold_list_objects_v2_rows(
    rows: Vec<object::Model>,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: usize,
) -> ListObjectsV2Page {
    let mut builder = ListObjectsV2PageBuilder::new(prefix, delimiter, max_keys);
    for row in rows {
        if builder.push_row(row) == PushListEntryResult::PageComplete {
            break;
        }
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::entities::object;
    use chrono::Utc;

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
    fn list_v2_fold_without_delimiter_returns_flat_objects() {
        let rows = vec![
            object_model("a.txt"),
            object_model("b.txt"),
            object_model("c.txt"),
        ];
        let page = fold_list_objects_v2_rows(rows, "", None, 1000);
        assert_eq!(page.object_keys(), vec!["a.txt", "b.txt", "c.txt"]);
        assert!(page.common_prefixes().is_empty());
        assert!(!page.is_truncated);
        assert!(page.next_token.is_none());
    }

    #[test]
    fn list_v2_fold_with_delimiter_returns_direct_objects_and_common_prefixes() {
        let rows = vec![
            object_model("a.txt"),
            object_model("photos/cat.jpg"),
            object_model("photos/dog.jpg"),
            object_model("b.txt"),
        ];
        let page = fold_list_objects_v2_rows(rows, "", Some("/"), 1000);
        assert_eq!(page.object_keys(), vec!["a.txt", "b.txt"]);
        assert_eq!(page.common_prefixes(), vec!["photos/"]);
        assert!(!page.is_truncated);
        assert!(page.next_token.is_none());
    }

    #[test]
    fn list_v2_fold_with_prefix_and_delimiter_scopes_common_prefixes() {
        let rows = vec![
            object_model("photos/2024/jan.jpg"),
            object_model("photos/2024/feb.jpg"),
            object_model("photos/2025/mar.jpg"),
        ];
        let page = fold_list_objects_v2_rows(rows, "photos/", Some("/"), 1000);
        assert!(page.object_keys().is_empty());
        assert_eq!(page.common_prefixes(), vec!["photos/2024/", "photos/2025/"]);
        assert!(!page.is_truncated);
        assert!(page.next_token.is_none());
    }

    #[test]
    fn list_v2_fold_max_keys_counts_common_prefixes_and_tracks_consumed_prefix_rows() {
        let rows = vec![
            object_model("a.txt"),
            object_model("photos/cat.jpg"),
            object_model("photos/dog.jpg"),
            object_model("videos/clip.mp4"),
        ];
        let page = fold_list_objects_v2_rows(rows, "", Some("/"), 2);
        assert_eq!(page.object_keys(), vec!["a.txt"]);
        assert_eq!(page.common_prefixes(), vec!["photos/"]);
        assert!(page.is_truncated);
        assert_eq!(page.next_token, Some("photos/dog.jpg".to_string()));
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
