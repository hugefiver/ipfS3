use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use futures_util::StreamExt;
use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};

use crate::crypto::EncryptionMode;
use crate::state::AppState;

use super::object::{
    ByteCounter, determine_encryption_mode, extract_custom_metadata, extract_sse_c_key,
};

fn parse_decompress_upload_options(uri: &http::Uri) -> S3Result<(Option<String>, bool)> {
    let mut raw_target = None;
    let mut result = true;
    for (name, value) in crate::s3::query::decoded_query_pairs(uri)? {
        match name.as_str() {
            "decompress-zip" => {
                raw_target = Some(value);
            }
            "decompress-zip-result" => result = value != "false",
            _ => {}
        }
    }
    let target = raw_target
        .as_deref()
        .map(crate::zip::sanitize::normalize_target_prefix)
        .transpose()?;
    Ok((target, result))
}

/// Initiate a multipart upload.
///
/// Allocates a fresh `object_id` and `upload_id`, records the upload metadata
/// (encryption mode, wrapped key, content-type, user metadata) in the store,
/// and returns the `upload_id` to the client.
pub async fn create_multipart_upload(
    state: &Arc<AppState>,
    req: S3Request<CreateMultipartUploadInput>,
) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let content_type = req.input.content_type.clone();
    let db = state.store.db();

    // Validate the bucket exists.
    let exists = crate::store::bucket::exists(db, bucket).await?;
    if !exists {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {}", bucket));
    }

    let (decompress_zip_target, decompress_zip_result) = parse_decompress_upload_options(&req.uri)?;
    if decompress_zip_target.is_some()
        && [
            "x-amz-server-side-encryption",
            "x-amz-server-side-encryption-customer-algorithm",
            "x-amz-server-side-encryption-customer-key",
            "x-amz-server-side-encryption-customer-key-md5",
        ]
        .iter()
        .any(|name| req.headers.contains_key(*name))
    {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "decompress-zip cannot be combined with server-side encryption"
        ));
    }
    let enc_mode = determine_encryption_mode(&req.headers)?;
    let metadata = extract_custom_metadata(&req.headers);
    let object_id = uuid::Uuid::new_v4().to_string();
    let upload_id = uuid::Uuid::new_v4().to_string();

    // For SSE-S3 we generate a per-object key now and persist its wrapped form
    // so the same key can be reused for every part. SSE-C keys are supplied
    // per-request and are never stored; only their HMAC fingerprint is kept.
    let (key_wrap, sse_c_key_fingerprint) = match enc_mode {
        EncryptionMode::SseS3 => {
            let ok = state.master_key.generate_object_key();
            let wrapped = state
                .master_key
                .wrap(&ok)
                .map_err(|e| s3s::s3_error!(InternalError, "key wrap: {e}"))?;
            (Some(wrapped), None)
        }
        EncryptionMode::SseC => {
            let key = extract_sse_c_key(&req.headers)?;
            (None, Some(state.master_key.sse_c_key_fingerprint(&key)))
        }
        EncryptionMode::None => (None, None),
    };

    crate::store::multipart::create_upload(
        db,
        &upload_id,
        &object_id,
        bucket,
        key,
        enc_mode.as_str(),
        key_wrap.as_deref(),
        sse_c_key_fingerprint.as_deref(),
        content_type.as_deref(),
        metadata,
        decompress_zip_target.as_deref(),
        decompress_zip_result,
    )
    .await?;

    let server_side_encryption = if enc_mode == EncryptionMode::SseS3 {
        Some(ServerSideEncryption::from_static("AES256"))
    } else {
        None
    };

    Ok(S3Response::new(CreateMultipartUploadOutput {
        bucket: Some(bucket.clone()),
        key: Some(key.clone()),
        upload_id: Some(upload_id),
        server_side_encryption,
        ..Default::default()
    }))
}

async fn validate_sse_c_upload_key(
    state: &Arc<AppState>,
    upload_id: &str,
    headers: &http::HeaderMap,
) -> S3Result<crate::crypto::ObjectKey> {
    if determine_encryption_mode(headers)? != EncryptionMode::SseC {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "complete, unmixed SSE-C headers are required for this upload"
        ));
    }

    let key = extract_sse_c_key(headers)?;
    let candidate = state.master_key.sse_c_key_fingerprint(&key);
    let upload = crate::store::multipart::claim_sse_c_key_fingerprint(
        state.store.db(),
        upload_id,
        &candidate,
    )
    .await?;
    let persisted = upload.sse_c_key_fingerprint.as_deref().ok_or_else(|| {
        s3s::s3_error!(
            InternalError,
            "missing persisted SSE-C key fingerprint for multipart upload"
        )
    })?;
    let matches = state
        .master_key
        .verify_sse_c_key_fingerprint(persisted, &key)
        .map_err(|e| {
            s3s::s3_error!(
                InternalError,
                "invalid persisted SSE-C key fingerprint: {e}"
            )
        })?;
    if !matches {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "SSE-C customer key does not match multipart upload"
        ));
    }

    Ok(key)
}

/// Authenticate every SSE-C part before starting the root-object add request.
///
/// This keeps a corrupt part (or a legacy upload claimed by a key that did not
/// encrypt its existing parts) retryable without creating an unpinned root
/// object. Kubo content is addressed by CID, so the normal streaming complete
/// path can safely fetch the authenticated bytes again for re-encryption.
async fn authenticate_sse_c_parts(
    kubo: &crate::kubo::KuboClient,
    parts: &[(String, i64)],
    key: Arc<crate::crypto::ObjectKey>,
) -> S3Result<()> {
    preflight_sse_c_part_sizes(parts)?;

    for (cid, expected_size) in parts {
        let part_stream = crate::kubo::cat::stream_cat(kubo, cid, None)
            .await
            .map_err(|e| s3s::s3_error!(InternalError, "cat: {e}"))?;
        let decrypted = crate::crypto::chunker::decrypt_chunk_stream(part_stream, key.clone());
        tokio::pin!(decrypted);
        let mut observed_size = 0_i64;
        while let Some(chunk) = decrypted.next().await {
            match chunk {
                Ok(chunk) => {
                    observed_size = checked_part_plaintext_size(observed_size, chunk.len())?;
                    if observed_size > *expected_size {
                        return Err(s3s::s3_error!(
                            InvalidPart,
                            "decrypted part exceeds recorded plaintext size"
                        ));
                    }
                }
                Err(crate::error::AppError::Crypto(_)) => {
                    return Err(s3s::s3_error!(
                        InvalidPart,
                        "failed to decrypt part during complete — SSE-C key may not match the key used to upload parts"
                    ));
                }
                Err(error) => {
                    return Err(s3s::s3_error!(
                        InternalError,
                        "failed to stream encrypted multipart part: {error}"
                    ));
                }
            }
        }
        if observed_size != *expected_size {
            return Err(s3s::s3_error!(
                InvalidPart,
                "decrypted part size {observed_size} does not match recorded size {expected_size}"
            ));
        }
    }

    Ok(())
}

fn preflight_sse_c_part_sizes(parts: &[(String, i64)]) -> S3Result<()> {
    if parts.iter().any(|(_, expected_size)| *expected_size < 0) {
        return Err(s3s::s3_error!(
            InvalidPart,
            "multipart part has a negative recorded plaintext size"
        ));
    }
    Ok(())
}

fn checked_part_plaintext_size(observed: i64, chunk_size: usize) -> S3Result<i64> {
    let chunk_size = i64::try_from(chunk_size)
        .map_err(|_| s3s::s3_error!(InvalidPart, "multipart part plaintext size overflow"))?;
    observed
        .checked_add(chunk_size)
        .ok_or_else(|| s3s::s3_error!(InvalidPart, "multipart part plaintext size overflow"))
}

/// Upload a single part of an initiated multipart upload.
///
/// The part bytes are streamed into Kubo (optionally encrypted first), the
/// resulting content-addressed CID is pinned and recorded as the part's ETag.
pub async fn upload_part(
    state: &Arc<AppState>,
    req: S3Request<UploadPartInput>,
) -> S3Result<S3Response<UploadPartOutput>> {
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let upload_id = &req.input.upload_id;
    let part_number = req.input.part_number;

    // S3 requires part_number ∈ [1, 10000].
    if !(1..=10000).contains(&part_number) {
        return Err(s3s::s3_error!(
            InvalidPart,
            "part_number must be in [1, 10000], got {part_number}"
        ));
    }

    let db = state.store.db();

    let upload = crate::store::multipart::get_upload(db, upload_id).await?;

    if upload.bucket != *bucket || upload.key != *key {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "bucket/key mismatch for upload_id"
        ));
    }

    let enc_mode = EncryptionMode::parse(&upload.encryption_mode);
    let sse_c_key = match enc_mode {
        EncryptionMode::SseC => {
            Some(validate_sse_c_upload_key(state, upload_id, &req.headers).await?)
        }
        EncryptionMode::None | EncryptionMode::SseS3 => None,
    };

    let body = req
        .input
        .body
        .ok_or_else(|| s3s::s3_error!(IncompleteBody, "request body is missing"))?;

    let (counter, count_handle) = ByteCounter::new();
    let stream = counter.wrap(body);

    let cid: String = match enc_mode {
        EncryptionMode::None => crate::kubo::add::stream_add(&state.kubo, stream, 1)
            .await
            .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?,
        EncryptionMode::SseS3 => {
            let wrapped = upload.key_wrap.as_ref().ok_or_else(|| {
                s3s::s3_error!(InternalError, "missing wrapped key for SSE-S3 upload")
            })?;
            let ok = state
                .master_key
                .unwrap(wrapped)
                .map_err(|e| s3s::s3_error!(InternalError, "key unwrap: {e}"))?;
            let pinned = Box::pin(stream);
            let encrypted_stream =
                crate::crypto::chunker::encrypt_chunk_stream(pinned, Arc::new(ok));
            crate::kubo::add::stream_add(&state.kubo, encrypted_stream, 1)
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?
        }
        EncryptionMode::SseC => {
            let ok = sse_c_key.ok_or_else(|| {
                s3s::s3_error!(
                    InternalError,
                    "missing validated SSE-C key for multipart upload"
                )
            })?;
            let pinned = Box::pin(stream);
            let encrypted_stream =
                crate::crypto::chunker::encrypt_chunk_stream(pinned, Arc::new(ok));
            crate::kubo::add::stream_add(&state.kubo, encrypted_stream, 1)
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?
        }
    };

    let part_size = count_handle.load(Ordering::Relaxed) as i64;

    crate::kubo::pin::pin_add(&state.kubo, &cid)
        .await
        .map_err(|e| s3s::s3_error!(InternalError, "pin: {e}"))?;

    crate::store::multipart::upsert_part(db, upload_id, part_number, &cid, part_size, &cid).await?;

    let server_side_encryption = if enc_mode == EncryptionMode::SseS3 {
        Some(ServerSideEncryption::from_static("AES256"))
    } else {
        None
    };

    Ok(S3Response::new(UploadPartOutput {
        e_tag: Some(ETag::Strong(cid.clone())),
        server_side_encryption,
        ..Default::default()
    }))
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompletedMultipartArchive {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub encryption_object_id: String,
    pub completion_attempt_id: String,
    pub root_cid: String,
    pub total_size: i64,
    pub content_type: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub encrypted: bool,
    pub key_wrap: Option<String>,
    pub sse_c_key_fingerprint: Option<String>,
    pub decompress_zip_target: Option<String>,
    pub decompress_zip_result: bool,
    pub server_side_encryption: Option<ServerSideEncryption>,
}

#[async_trait::async_trait]
pub(crate) trait CompletedUploadFinalizerStore: Send + Sync {
    async fn commit(
        &self,
        upload_id: &str,
        attempt: crate::store::object::LatestObjectRow,
    ) -> Result<(), crate::store::multipart::CommitCompletedUploadError>;

    async fn reconcile(
        &self,
        upload_id: &str,
        expected_attempt: &crate::store::object::LatestObjectRow,
    ) -> crate::store::multipart::ReconciledCommitOutcome;
}

struct DatabaseCompletedUploadFinalizer<'a> {
    db: &'a sea_orm::DatabaseConnection,
}

#[async_trait::async_trait]
impl CompletedUploadFinalizerStore for DatabaseCompletedUploadFinalizer<'_> {
    async fn commit(
        &self,
        upload_id: &str,
        attempt: crate::store::object::LatestObjectRow,
    ) -> Result<(), crate::store::multipart::CommitCompletedUploadError> {
        crate::store::multipart::commit_completed_upload(self.db, upload_id, attempt).await
    }

    async fn reconcile(
        &self,
        upload_id: &str,
        expected_attempt: &crate::store::object::LatestObjectRow,
    ) -> crate::store::multipart::ReconciledCommitOutcome {
        crate::store::multipart::reconcile_completion_attempt(self.db, upload_id, expected_attempt)
            .await
    }
}

pub async fn finalize_completed_multipart_archive(
    state: &Arc<AppState>,
    completed: &CompletedMultipartArchive,
) -> S3Result<()> {
    let store = DatabaseCompletedUploadFinalizer {
        db: state.store.db(),
    };
    finalize_completed_multipart_archive_with_store(completed, &store).await
}

async fn finalize_completed_multipart_archive_with_store<
    S: CompletedUploadFinalizerStore + ?Sized,
>(
    completed: &CompletedMultipartArchive,
    store: &S,
) -> S3Result<()> {
    let attempt = crate::store::object::LatestObjectRow {
        id: completed.completion_attempt_id.clone(),
        bucket: completed.bucket.clone(),
        key: completed.key.clone(),
        cid: completed.root_cid.clone(),
        size: completed.total_size,
        content_type: completed.content_type.clone(),
        etag: completed.root_cid.clone(),
        metadata: completed.metadata.clone(),
        encrypted: completed.encrypted,
        key_wrap: completed.key_wrap.clone(),
        sse_c_key_fingerprint: completed.sse_c_key_fingerprint.clone(),
        multipart: true,
        created_at: chrono::Utc::now(),
    };

    match store.commit(&completed.upload_id, attempt.clone()).await {
        Ok(()) => Ok(()),
        Err(crate::store::multipart::CommitCompletedUploadError::RolledBack {
            completion_attempt_id,
            source,
        }) => {
            if completion_attempt_id != completed.completion_attempt_id {
                return Err(s3s::s3_error!(InternalError, "completion attempt mismatch"));
            }
            Err(source.into())
        }
        Err(crate::store::multipart::CommitCompletedUploadError::OutcomeUnknown {
            completion_attempt_id,
            source,
        }) => {
            if completion_attempt_id != completed.completion_attempt_id {
                return Err(s3s::s3_error!(InternalError, "completion attempt mismatch"));
            }
            match store.reconcile(&completed.upload_id, &attempt).await {
                crate::store::multipart::ReconciledCommitOutcome::Committed => Ok(()),
                crate::store::multipart::ReconciledCommitOutcome::NotCommitted => {
                    Err(source.into())
                }
                crate::store::multipart::ReconciledCommitOutcome::Unknown(reconcile_error) => {
                    Err(s3s::s3_error!(
                        InternalError,
                        "commit outcome unknown ({source}); reconciliation failed ({reconcile_error})"
                    ))
                }
            }
        }
    }
}

/// Complete a multipart upload by concatenating the parts into a single object.
///
/// The client-supplied part list is validated against the recorded parts
/// (matching ETags, ascending order), then each part's CID is streamed from
/// Kubo in order and re-added as a single content-addressed object. The
/// resulting root CID becomes the object's CID and ETag.
pub async fn complete_multipart_upload(
    state: &Arc<AppState>,
    req: S3Request<CompleteMultipartUploadInput>,
) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
    let completed = complete_multipart_upload_inner(state, req).await?;
    finalize_completed_multipart_archive(state, &completed).await?;

    Ok(S3Response::new(CompleteMultipartUploadOutput {
        bucket: Some(completed.bucket),
        key: Some(completed.key),
        e_tag: Some(ETag::Strong(completed.root_cid)),
        server_side_encryption: completed.server_side_encryption,
        ..Default::default()
    }))
}

pub async fn complete_multipart_upload_inner(
    state: &Arc<AppState>,
    req: S3Request<CompleteMultipartUploadInput>,
) -> S3Result<CompletedMultipartArchive> {
    let completion_attempt_id = uuid::Uuid::new_v4().to_string();
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let upload_id = &req.input.upload_id;
    let db = state.store.db();

    // s3s parses the XML body into `multipart_upload` for us; each
    // `CompletedPart.e_tag` is already an `ETag` with surrounding quotes
    // stripped by the s3s XML deserializer.
    let completed = req
        .input
        .multipart_upload
        .as_ref()
        .and_then(|m| m.parts.as_ref())
        .ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing multipart upload parts"))?;

    if completed.is_empty() {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "part list must not be empty"
        ));
    }

    // Validate ascending part-number order (gaps allowed). S3 requires the
    // client to present parts in ascending order. Each part must include
    // a part_number.
    let mut last_pn = 0i32;
    for cp in completed.iter() {
        let pn = cp.part_number.ok_or_else(|| {
            s3s::s3_error!(InvalidArgument, "part_number is required for each part")
        })?;
        if pn <= last_pn {
            return Err(s3s::s3_error!(
                InvalidPartOrder,
                "parts must be in ascending order"
            ));
        }
        last_pn = pn;
    }

    let upload = crate::store::multipart::get_upload(db, upload_id).await?;
    let encryption_object_id = upload.object_id.clone();

    if upload.bucket != *bucket || upload.key != *key {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "bucket/key mismatch for upload_id"
        ));
    }

    let enc_mode = EncryptionMode::parse(&upload.encryption_mode);
    let sse_c_key = match enc_mode {
        EncryptionMode::SseC => {
            Some(validate_sse_c_upload_key(state, upload_id, &req.headers).await?)
        }
        EncryptionMode::None | EncryptionMode::SseS3 => None,
    };

    // Resolve each client-declared part to its stored record, verifying the
    // ETag matches. Collect the (cid, size) pairs needed to build the concat
    // stream.
    let mut parts_to_concat: Vec<(String, i64)> = Vec::with_capacity(completed.len());
    for cp in completed.iter() {
        let pn = cp.part_number.ok_or_else(|| {
            s3s::s3_error!(InvalidArgument, "part_number is required for each part")
        })?;
        let part = crate::store::multipart::get_part(db, upload_id, pn).await?;

        if let Some(ref client_etag) = cp.e_tag
            && client_etag.value() != part.etag
        {
            return Err(s3s::s3_error!(InvalidPart, "etag mismatch for part {pn}"));
        }

        parts_to_concat.push((part.cid.clone(), part.size));
    }

    if enc_mode == EncryptionMode::SseC {
        preflight_sse_c_part_sizes(&parts_to_concat)?;
    }

    // S3 requires every part except the last to be at least 5 MiB.
    // (The last part has no minimum size.)
    const MIN_PART_SIZE: i64 = 5 * 1024 * 1024;
    let part_count = parts_to_concat.len();
    for (i, (_, size)) in parts_to_concat.iter().enumerate() {
        if i < part_count - 1 && *size < MIN_PART_SIZE {
            let pn = completed[i]
                .part_number
                .expect("part_number validated above");
            return Err(s3s::s3_error!(
                EntityTooSmall,
                "part {pn} is {size} bytes; minimum is {MIN_PART_SIZE} bytes for all but the last part"
            ));
        }
    }

    // Concatenate every part's bytes (in order) and re-add as a single object.
    // The concat stream must be `'static` for `stream_add`.
    let total_size = parts_to_concat.iter().try_fold(0_i64, |total, (_, size)| {
        total
            .checked_add(*size)
            .ok_or_else(|| s3s::s3_error!(InvalidPart, "multipart object size overflow"))
    })?;
    let part_cids: Vec<String> = parts_to_concat.iter().map(|(c, _)| c.clone()).collect();

    let kubo = state.kubo.clone();

    let root_cid = match enc_mode {
        EncryptionMode::None => {
            // Plaintext: concatenate ciphertext (= plaintext here) directly.
            let concat_stream = async_stream::stream! {
                for cid in &part_cids {
                    let part_stream = crate::kubo::cat::stream_cat(&kubo, cid, None)
                        .await
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    tokio::pin!(part_stream);
                    while let Some(chunk) = part_stream.next().await {
                        yield chunk;
                    }
                }
            };
            crate::kubo::add::stream_add(&state.kubo, concat_stream, 1)
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "add: {e}"))?
        }
        EncryptionMode::SseS3 | EncryptionMode::SseC => {
            // Encrypted: each part was independently encrypted with its own
            // chunk boundaries. Concatenating ciphertexts and then decrypting
            // with fixed CIPHER_CHUNK splitting would fail at part boundaries
            // (the last chunk of each part may be smaller than CIPHER_CHUNK).
            //
            // Fix: decrypt each part to plaintext, concatenate the plaintext
            // streams, then re-encrypt the whole as a single object.
            let ok = match enc_mode {
                EncryptionMode::SseS3 => {
                    let wrapped = upload.key_wrap.as_ref().ok_or_else(|| {
                        s3s::s3_error!(InternalError, "missing key_wrap for SSE-S3 upload")
                    })?;
                    state
                        .master_key
                        .unwrap(wrapped)
                        .map_err(|e| s3s::s3_error!(InternalError, "key unwrap: {e}"))?
                }
                EncryptionMode::SseC => sse_c_key.ok_or_else(|| {
                    s3s::s3_error!(
                        InternalError,
                        "missing validated SSE-C key for multipart complete"
                    )
                })?,
                _ => unreachable!(),
            };
            let ok_arc = Arc::new(ok);
            if enc_mode == EncryptionMode::SseC {
                authenticate_sse_c_parts(&kubo, &parts_to_concat, ok_arc.clone()).await?;
            }
            // Step 1: Build a plaintext concat stream by decrypting each part.
            // We use an out-of-band flag to capture decryption failures,
            // because the error gets buried inside reqwest's body stream and
            // its Display does not include the source chain.
            let decrypt_err: Arc<std::sync::Mutex<Option<crate::error::AppError>>> =
                Arc::new(std::sync::Mutex::new(None));
            let decrypt_kubo = kubo.clone();
            let decrypt_ok = ok_arc.clone();
            let err_flag = decrypt_err.clone();
            let plaintext_concat = async_stream::stream! {
                for cid in &part_cids {
                    let part_stream = crate::kubo::cat::stream_cat(&decrypt_kubo, cid, None)
                        .await
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    let decrypted = crate::crypto::chunker::decrypt_chunk_stream(
                        part_stream,
                        decrypt_ok.clone(),
                    );
                    let mut s = Box::pin(decrypted);
                    while let Some(chunk) = s.next().await {
                        match chunk {
                            Ok(b) => yield Ok(b),
                            Err(crate::error::AppError::Crypto(msg)) => {
                                // Capture the error out-of-band, then terminate
                                // the stream so stream_add fails fast.
                                *err_flag.lock().unwrap() =
                                    Some(crate::error::AppError::Crypto(msg));
                                yield Err(std::io::Error::other(
                                    "decryption failed",
                                ));
                                return;
                            }
                            Err(e) => {
                                yield Err(std::io::Error::other(
                                    e.to_string(),
                                ));
                            }
                        }
                    }
                }
            };

            // Step 2: Re-encrypt the concatenated plaintext as a single object.
            let encrypted_stream =
                crate::crypto::chunker::encrypt_chunk_stream(Box::pin(plaintext_concat), ok_arc);

            let root_result = crate::kubo::add::stream_add(&state.kubo, encrypted_stream, 1).await;

            // Check the out-of-band decryption error flag FIRST — if the
            // decrypt failed, the stream_add error is just a side effect.
            if decrypt_err.lock().unwrap().take().is_some() {
                return Err(s3s::s3_error!(
                    InvalidPart,
                    "failed to decrypt part during complete — SSE-C key may not match the key used to upload parts"
                ));
            }

            root_result.map_err(|e| s3s::s3_error!(InternalError, "add: {e}"))?
        }
    };

    crate::kubo::pin::pin_add(&state.kubo, &root_cid)
        .await
        .map_err(|e| s3s::s3_error!(InternalError, "pin: {e}"))?;

    let (encrypted, key_wrap): (bool, Option<String>) = match enc_mode {
        EncryptionMode::None => (false, None),
        EncryptionMode::SseS3 => (true, upload.key_wrap.clone()),
        EncryptionMode::SseC => (true, None),
    };

    let server_side_encryption = if encrypted && key_wrap.is_some() {
        Some(ServerSideEncryption::from_static("AES256"))
    } else {
        None
    };

    Ok(CompletedMultipartArchive {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        encryption_object_id,
        completion_attempt_id,
        root_cid,
        total_size,
        content_type: upload.content_type,
        metadata: upload.metadata,
        encrypted,
        key_wrap,
        sse_c_key_fingerprint: upload.sse_c_key_fingerprint,
        decompress_zip_target: upload.decompress_zip_target,
        decompress_zip_result: upload.decompress_zip_result,
        server_side_encryption,
    })
}

/// Abort a multipart upload, discarding its database records.
pub async fn abort_multipart_upload(
    state: &Arc<AppState>,
    req: S3Request<AbortMultipartUploadInput>,
) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let upload_id = &req.input.upload_id;
    let db = state.store.db();

    // Verify bucket/key match the upload record.
    let upload = crate::store::multipart::get_upload(db, upload_id).await?;
    if upload.bucket != *bucket || upload.key != *key {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "bucket/key mismatch for upload_id"
        ));
    }

    // Delete the upload record; ON DELETE CASCADE removes parts.
    crate::store::multipart::delete_upload(db, upload_id).await?;

    Ok(S3Response::new(AbortMultipartUploadOutput::default()))
}

/// List the parts already uploaded for a multipart upload.
pub async fn list_parts(
    state: &Arc<AppState>,
    req: S3Request<ListPartsInput>,
) -> S3Result<S3Response<ListPartsOutput>> {
    let bucket = &req.input.bucket;
    let key = &req.input.key;
    let upload_id = &req.input.upload_id;
    let db = state.store.db();

    let upload = crate::store::multipart::get_upload(db, upload_id).await?;

    if upload.bucket != *bucket || upload.key != *key {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "bucket/key mismatch for upload_id"
        ));
    }

    let parts = crate::store::multipart::list_parts(db, upload_id).await?;

    let parts_dto: Vec<Part> = parts
        .into_iter()
        .map(|p| Part {
            part_number: Some(p.part_number),
            e_tag: Some(ETag::Strong(p.etag)),
            size: Some(p.size),
            last_modified: Some(Timestamp::from(SystemTime::from(p.uploaded_at))),
            ..Default::default()
        })
        .collect();

    Ok(S3Response::new(ListPartsOutput {
        bucket: Some(bucket.clone()),
        key: Some(key.clone()),
        upload_id: Some(upload_id.clone()),
        parts: Some(parts_dto),
        ..Default::default()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use sea_orm::{ConnectionTrait, DatabaseBackend, EntityTrait, Statement};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    enum FakeCommitResult {
        Ok,
        RolledBack(String),
        OutcomeUnknown(String),
    }

    enum FakeReconcileResult {
        Committed,
        NotCommitted,
        Unknown,
    }

    struct FakeFinalizerStore {
        commit: FakeCommitResult,
        reconcile: FakeReconcileResult,
        reconcile_calls: AtomicUsize,
    }

    struct BlockingUnknownFinalizerStore {
        db: sea_orm::DatabaseConnection,
        commit_signal: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        reconcile_signal: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release_reconcile: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    }

    #[async_trait::async_trait]
    impl CompletedUploadFinalizerStore for FakeFinalizerStore {
        async fn commit(
            &self,
            _upload_id: &str,
            _attempt: crate::store::object::LatestObjectRow,
        ) -> Result<(), crate::store::multipart::CommitCompletedUploadError> {
            match &self.commit {
                FakeCommitResult::Ok => Ok(()),
                FakeCommitResult::RolledBack(completion_attempt_id) => Err(
                    crate::store::multipart::CommitCompletedUploadError::RolledBack {
                        completion_attempt_id: completion_attempt_id.clone(),
                        source: crate::error::AppError::Internal("forced rollback".to_owned()),
                    },
                ),
                FakeCommitResult::OutcomeUnknown(completion_attempt_id) => Err(
                    crate::store::multipart::CommitCompletedUploadError::OutcomeUnknown {
                        completion_attempt_id: completion_attempt_id.clone(),
                        source: crate::error::AppError::Internal(
                            "forced unknown commit outcome".to_owned(),
                        ),
                    },
                ),
            }
        }

        async fn reconcile(
            &self,
            _upload_id: &str,
            _expected_attempt: &crate::store::object::LatestObjectRow,
        ) -> crate::store::multipart::ReconciledCommitOutcome {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            match self.reconcile {
                FakeReconcileResult::Committed => {
                    crate::store::multipart::ReconciledCommitOutcome::Committed
                }
                FakeReconcileResult::NotCommitted => {
                    crate::store::multipart::ReconciledCommitOutcome::NotCommitted
                }
                FakeReconcileResult::Unknown => {
                    crate::store::multipart::ReconciledCommitOutcome::Unknown(
                        crate::error::AppError::Internal("forced query failure".to_owned()),
                    )
                }
            }
        }
    }

    #[async_trait::async_trait]
    impl CompletedUploadFinalizerStore for BlockingUnknownFinalizerStore {
        async fn commit(
            &self,
            _upload_id: &str,
            attempt: crate::store::object::LatestObjectRow,
        ) -> Result<(), crate::store::multipart::CommitCompletedUploadError> {
            if let Some(signal) = self.commit_signal.lock().await.take() {
                let _ = signal.send(());
            }
            Err(
                crate::store::multipart::CommitCompletedUploadError::OutcomeUnknown {
                    completion_attempt_id: attempt.id,
                    source: crate::error::AppError::Internal(
                        "forced unknown commit outcome".to_owned(),
                    ),
                },
            )
        }

        async fn reconcile(
            &self,
            upload_id: &str,
            expected_attempt: &crate::store::object::LatestObjectRow,
        ) -> crate::store::multipart::ReconciledCommitOutcome {
            if let Some(signal) = self.reconcile_signal.lock().await.take() {
                let _ = signal.send(());
            }
            if let Some(release) = self.release_reconcile.lock().await.take() {
                let _ = release.await;
            }
            crate::store::multipart::reconcile_completion_attempt(
                &self.db,
                upload_id,
                expected_attempt,
            )
            .await
        }
    }

    async fn test_state_with_bucket_and_kubo(name: &str, kubo_uri: String) -> Arc<AppState> {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, name, None).await.unwrap();
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

    async fn test_state_with_bucket(name: &str) -> Arc<AppState> {
        test_state_with_bucket_and_kubo(name, "http://127.0.0.1:5001".to_owned()).await
    }

    async fn file_backed_test_state_with_bucket_and_kubo(
        name: &str,
        kubo_uri: String,
        directory: &tempfile::TempDir,
    ) -> Arc<AppState> {
        let database_path = directory.path().join("multipart-concurrency.sqlite");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let mut options = sea_orm::ConnectOptions::new(database_url);
        options.max_connections(4).min_connections(2);
        let db = sea_orm::Database::connect(options).await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, name, None).await.unwrap();
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

    async fn multipart_kubo(add_cids: &[&str]) -> MockServer {
        let kubo = MockServer::start().await;
        let responses: Arc<Vec<String>> = Arc::new(
            add_cids
                .iter()
                .map(|cid| format!("{{\"Hash\":\"{cid}\",\"Size\":\"5\"}}\n"))
                .collect(),
        );
        let response_index = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with({
                let responses = responses.clone();
                let response_index = response_index.clone();
                move |_: &wiremock::Request| {
                    let index = response_index.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(200).set_body_string(responses[index].clone())
                }
            })
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[]}"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/rm"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[]}"))
            .mount(&kubo)
            .await;
        kubo
    }

    async fn seed_plain_upload(state: &Arc<AppState>, upload_id: &str) {
        crate::store::multipart::create_upload(
            state.store.db(),
            upload_id,
            "encryption-object-1",
            "test-bucket",
            "archive.zip",
            "none",
            None,
            None,
            Some("application/zip"),
            None,
            None,
            true,
        )
        .await
        .unwrap();
    }

    fn upload_part_request(upload_id: &str, bytes: &'static [u8]) -> S3Request<UploadPartInput> {
        S3Request {
            input: UploadPartInput {
                bucket: "test-bucket".to_owned(),
                key: "archive.zip".to_owned(),
                part_number: 1,
                upload_id: upload_id.to_owned(),
                body: Some(StreamingBlob::from(s3s::Body::from(Bytes::from_static(
                    bytes,
                )))),
                ..Default::default()
            },
            method: http::Method::PUT,
            uri: format!("/test-bucket/archive.zip?partNumber=1&uploadId={upload_id}")
                .parse()
                .unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn complete_request(upload_id: &str, etag: &str) -> S3Request<CompleteMultipartUploadInput> {
        S3Request {
            input: CompleteMultipartUploadInput {
                bucket: "test-bucket".to_owned(),
                key: "archive.zip".to_owned(),
                upload_id: upload_id.to_owned(),
                multipart_upload: Some(CompletedMultipartUpload {
                    parts: Some(vec![CompletedPart {
                        e_tag: Some(ETag::Strong(etag.to_owned())),
                        part_number: Some(1),
                        ..Default::default()
                    }]),
                }),
                ..Default::default()
            },
            method: http::Method::POST,
            uri: format!("/test-bucket/archive.zip?uploadId={upload_id}")
                .parse()
                .unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn abort_request(upload_id: &str) -> S3Request<AbortMultipartUploadInput> {
        S3Request {
            input: AbortMultipartUploadInput {
                bucket: "test-bucket".to_owned(),
                key: "archive.zip".to_owned(),
                upload_id: upload_id.to_owned(),
                ..Default::default()
            },
            method: http::Method::DELETE,
            uri: format!("/test-bucket/archive.zip?uploadId={upload_id}")
                .parse()
                .unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn get_object_request(key: &str) -> S3Request<GetObjectInput> {
        S3Request {
            input: GetObjectInput {
                bucket: "test-bucket".to_owned(),
                key: key.to_owned(),
                ..Default::default()
            },
            method: http::Method::GET,
            uri: format!("/test-bucket/{key}").parse().unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    async fn read_object_through_get(state: &Arc<AppState>, key: &str) -> Vec<u8> {
        let response = crate::s3::ops::object::get_object(state, get_object_request(key))
            .await
            .unwrap();
        let mut body = response.output.body.unwrap();
        let mut bytes = Vec::new();
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
        }
        bytes
    }

    async fn seed_ordinary_shared_object(state: &Arc<AppState>, cid: &str, size: i64) {
        crate::store::object::upsert(
            state.store.db(),
            "ordinary-object",
            "test-bucket",
            "ordinary.txt",
            cid,
            size,
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
    }

    fn completed_archive(completion_attempt_id: &str) -> CompletedMultipartArchive {
        CompletedMultipartArchive {
            bucket: "test-bucket".to_owned(),
            key: "archive.zip".to_owned(),
            upload_id: "upload-1".to_owned(),
            encryption_object_id: "encryption-object-1".to_owned(),
            completion_attempt_id: completion_attempt_id.to_owned(),
            root_cid: "QmRoot".to_owned(),
            total_size: 9,
            content_type: Some("application/zip".to_owned()),
            metadata: Some(serde_json::json!({"source": "multipart"})),
            encrypted: false,
            key_wrap: None,
            sse_c_key_fingerprint: None,
            decompress_zip_target: None,
            decompress_zip_result: true,
            server_side_encryption: None,
        }
    }

    fn latest_attempt_for_archive(
        archive: &CompletedMultipartArchive,
    ) -> crate::store::object::LatestObjectRow {
        crate::store::object::LatestObjectRow {
            id: archive.completion_attempt_id.clone(),
            bucket: archive.bucket.clone(),
            key: archive.key.clone(),
            cid: archive.root_cid.clone(),
            size: archive.total_size,
            content_type: archive.content_type.clone(),
            etag: archive.root_cid.clone(),
            metadata: archive.metadata.clone(),
            encrypted: archive.encrypted,
            key_wrap: archive.key_wrap.clone(),
            sse_c_key_fingerprint: archive.sse_c_key_fingerprint.clone(),
            multipart: true,
            created_at: chrono::Utc::now(),
        }
    }

    async fn assert_pin_add_count(kubo: &MockServer, cid: &str, expected: usize) {
        let expected_query = format!("arg={cid}");
        let requests = kubo.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| {
                    request.url.path() == "/api/v0/pin/add"
                        && request.url.query() == Some(expected_query.as_str())
                })
                .count(),
            expected,
            "unexpected pin/add count for {cid}"
        );
    }

    async fn assert_no_pin_removes(kubo: &MockServer, cids: &[&str]) {
        let requests = kubo.received_requests().await.unwrap();
        for cid in cids {
            let expected_query = format!("arg={cid}");
            assert!(
                !requests.iter().any(|request| {
                    request.url.path() == "/api/v0/pin/rm"
                        && request.url.query() == Some(expected_query.as_str())
                }),
                "must not remove pin for {cid}"
            );
        }
    }

    fn multipart_create_request(bucket: &str, key: &str) -> S3Request<CreateMultipartUploadInput> {
        S3Request {
            input: CreateMultipartUploadInput {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                ..Default::default()
            },
            method: http::Method::POST,
            uri: format!("/{bucket}/{key}?uploads").parse().unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
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

    async fn authentication_kubo(bodies: HashMap<String, Vec<u8>>) -> MockServer {
        let kubo = MockServer::start().await;
        let bodies = Arc::new(bodies);
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(move |request: &wiremock::Request| {
                let cid = request
                    .url
                    .query_pairs()
                    .find(|(name, _)| name == "arg")
                    .map(|(_, value)| value.into_owned())
                    .expect("cat CID");
                match bodies.get(&cid) {
                    Some(body) => ResponseTemplate::new(200).set_body_bytes(body.clone()),
                    None => ResponseTemplate::new(404).set_body_string("unknown CID"),
                }
            })
            .mount(&kubo)
            .await;
        kubo
    }

    fn fixed_encrypted_chunk(
        key: &crate::crypto::ObjectKey,
        nonce_byte: u8,
        plaintext: &[u8],
    ) -> Vec<u8> {
        crate::crypto::aes_gcm::encrypt_chunk(key, &[nonce_byte; 12], plaintext)
            .unwrap()
            .to_vec()
    }

    #[tokio::test]
    async fn authenticate_sse_c_parts_preflights_all_negative_sizes_before_cat() {
        let kubo = authentication_kubo(HashMap::from([
            ("valid".to_owned(), Vec::new()),
            ("negative".to_owned(), Vec::new()),
        ]))
        .await;
        let parts = vec![("valid".to_owned(), 0), ("negative".to_owned(), -1)];

        let error = authenticate_sse_c_parts(
            &crate::kubo::KuboClient::new(kubo.uri()),
            &parts,
            Arc::new(crate::crypto::ObjectKey { bytes: [7; 32] }),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidPart");
        assert!(kubo.received_requests().await.unwrap().is_empty());
    }

    #[test]
    fn authenticate_sse_c_parts_checked_size_overflow_is_invalid_part() {
        let error = checked_part_plaintext_size(i64::MAX, 1).unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidPart");
    }

    #[tokio::test]
    async fn authenticate_sse_c_parts_enforces_exact_recorded_plaintext_size() {
        let key = crate::crypto::ObjectKey { bytes: [7; 32] };
        let full = vec![0x41; crate::crypto::chunker::CHUNK_SIZE];
        let tail = b"tail";
        let mut exact_multi = fixed_encrypted_chunk(&key, 1, &full);
        exact_multi.extend_from_slice(&fixed_encrypted_chunk(&key, 2, tail));
        let full_boundary = fixed_encrypted_chunk(&key, 3, &full);
        let mut truncated = fixed_encrypted_chunk(&key, 4, b"truncated");
        truncated.pop();
        let mut invalid_tag = fixed_encrypted_chunk(&key, 5, b"tag");
        *invalid_tag.last_mut().unwrap() ^= 0x80;
        let kubo = authentication_kubo(HashMap::from([
            ("empty".to_owned(), Vec::new()),
            ("truncated".to_owned(), truncated),
            ("boundary".to_owned(), full_boundary),
            ("invalid-tag".to_owned(), invalid_tag),
            ("exact".to_owned(), exact_multi),
        ]))
        .await;
        let client = crate::kubo::KuboClient::new(kubo.uri());
        let key = Arc::new(key);

        authenticate_sse_c_parts(&client, &[("empty".to_owned(), 0)], key.clone())
            .await
            .unwrap();

        for (cid, expected) in [
            ("empty", 1_i64),
            ("truncated", 9),
            (
                "boundary",
                i64::try_from(crate::crypto::chunker::CHUNK_SIZE).unwrap() - 1,
            ),
            ("invalid-tag", 3),
        ] {
            let error =
                authenticate_sse_c_parts(&client, &[(cid.to_owned(), expected)], key.clone())
                    .await
                    .unwrap_err();
            assert_eq!(error.code().as_str(), "InvalidPart", "case {cid}");
        }

        authenticate_sse_c_parts(
            &client,
            &[(
                "exact".to_owned(),
                i64::try_from(crate::crypto::chunker::CHUNK_SIZE + tail.len()).unwrap(),
            )],
            key,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn create_sse_c_persists_exact_fingerprint() {
        let state = test_state_with_bucket("test-bucket").await;
        let mut req = multipart_create_request("test-bucket", "archive.zip");
        req.headers = valid_sse_c_headers();
        let expected_fingerprint = state
            .master_key
            .sse_c_key_fingerprint(&extract_sse_c_key(&req.headers).unwrap());

        let response = create_multipart_upload(&state, req).await.unwrap();
        let upload_id = response.output.upload_id.unwrap();
        let upload = crate::store::entities::multipart_upload::Entity::find_by_id(upload_id)
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(upload.encryption_mode, "sse_c");
        assert!(upload.key_wrap.is_none());
        assert_eq!(
            upload.sse_c_key_fingerprint.as_deref(),
            Some(expected_fingerprint.as_str())
        );
    }

    #[tokio::test]
    async fn create_sse_c_rejects_invalid_or_partial_headers_before_insert() {
        use base64::Engine;

        let state = test_state_with_bucket("test-bucket").await;
        let mut malformed_headers = Vec::new();

        let mut wrong_md5 = valid_sse_c_headers();
        wrong_md5.insert(
            "x-amz-server-side-encryption-customer-key-md5",
            base64::engine::general_purpose::STANDARD
                .encode([0; 16])
                .parse()
                .unwrap(),
        );
        malformed_headers.push(wrong_md5);

        for missing_header in [
            "x-amz-server-side-encryption-customer-algorithm",
            "x-amz-server-side-encryption-customer-key",
            "x-amz-server-side-encryption-customer-key-md5",
        ] {
            let mut headers = valid_sse_c_headers();
            headers.remove(missing_header);
            malformed_headers.push(headers);
        }

        let mut mixed_headers = valid_sse_c_headers();
        mixed_headers.insert(
            "x-amz-server-side-encryption",
            http::HeaderValue::from_static("AES256"),
        );
        malformed_headers.push(mixed_headers);

        for headers in malformed_headers {
            let mut req = multipart_create_request("test-bucket", "archive.zip");
            req.headers = headers;
            assert_eq!(
                create_multipart_upload(&state, req)
                    .await
                    .unwrap_err()
                    .code()
                    .as_str(),
                "InvalidArgument"
            );
            assert!(
                crate::store::entities::multipart_upload::Entity::find()
                    .all(state.store.db())
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn create_plain_and_sse_s3_store_no_sse_c_fingerprint() {
        let state = test_state_with_bucket("test-bucket").await;

        let plain =
            create_multipart_upload(&state, multipart_create_request("test-bucket", "plain.zip"))
                .await
                .unwrap();
        let mut sse_s3_req = multipart_create_request("test-bucket", "sse-s3.zip");
        sse_s3_req.headers.insert(
            "x-amz-server-side-encryption",
            http::HeaderValue::from_static("AES256"),
        );
        let sse_s3 = create_multipart_upload(&state, sse_s3_req).await.unwrap();

        for upload_id in [
            plain.output.upload_id.unwrap(),
            sse_s3.output.upload_id.unwrap(),
        ] {
            let upload = crate::store::entities::multipart_upload::Entity::find_by_id(upload_id)
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap();
            assert!(upload.sse_c_key_fingerprint.is_none());
        }
    }

    #[tokio::test]
    async fn create_multipart_upload_decodes_shared_query_once() {
        let state = test_state_with_bucket("test-bucket").await;
        let mut req = multipart_create_request("test-bucket", "archive.zip");
        req.uri = "/test-bucket/archive.zip?uploads=&decompress-zip=prefix%2Fnested%2F&decompress-zip-result=false"
            .parse()
            .unwrap();

        create_multipart_upload(&state, req).await.unwrap();

        let upload = crate::store::entities::multipart_upload::Entity::find()
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            upload.decompress_zip_target.as_deref(),
            Some("prefix/nested/")
        );
        assert!(!upload.decompress_zip_result);
    }

    #[tokio::test]
    async fn create_multipart_upload_rejects_invalid_query_utf8() {
        let state = test_state_with_bucket("test-bucket").await;
        let mut req = multipart_create_request("test-bucket", "archive.zip");
        req.uri = "/test-bucket/archive.zip?uploads=&decompress-zip=%FF"
            .parse()
            .unwrap();

        assert_eq!(
            create_multipart_upload(&state, req)
                .await
                .unwrap_err()
                .code()
                .as_str(),
            "InvalidArgument"
        );
    }

    #[tokio::test]
    async fn create_multipart_upload_rejects_decompress_sse() {
        let state = test_state_with_bucket("test-bucket").await;
        let mut req = multipart_create_request("test-bucket", "archive.zip");
        req.uri = "/test-bucket/archive.zip?uploads=&decompress-zip=prefix%2F"
            .parse()
            .unwrap();
        req.headers.insert(
            "x-amz-server-side-encryption",
            http::HeaderValue::from_static("AES256"),
        );

        assert_eq!(
            create_multipart_upload(&state, req)
                .await
                .unwrap_err()
                .code()
                .as_str(),
            "InvalidArgument"
        );
    }

    #[test]
    fn decompress_upload_options_use_last_values_and_exact_false_only() {
        let uri = "/test-bucket/archive.zip?uploads=&decompress-zip=first%2F&decompress-zip-result=false&decompress-zip=last%2Fnested%2F&decompress-zip-result=False"
            .parse::<http::Uri>()
            .unwrap();
        assert_eq!(
            parse_decompress_upload_options(&uri).unwrap(),
            (Some("last/nested/".to_owned()), true)
        );
        let defaults = "/test-bucket/archive.zip?uploads"
            .parse::<http::Uri>()
            .unwrap();
        assert_eq!(
            parse_decompress_upload_options(&defaults).unwrap(),
            (None, true)
        );
    }

    #[test]
    fn decompress_upload_options_normalize_only_the_final_target_value() {
        let valid_final =
            "/test-bucket/archive.zip?uploads&decompress-zip=../bad&decompress-zip=prefix%2F"
                .parse::<http::Uri>()
                .unwrap();
        assert_eq!(
            parse_decompress_upload_options(&valid_final).unwrap(),
            (Some("prefix/".to_owned()), true)
        );

        let invalid_final =
            "/test-bucket/archive.zip?uploads&decompress-zip=prefix%2F&decompress-zip=../bad"
                .parse::<http::Uri>()
                .unwrap();
        assert_eq!(
            parse_decompress_upload_options(&invalid_final)
                .unwrap_err()
                .code()
                .as_str(),
            "InvalidArgument"
        );
    }

    #[tokio::test]
    async fn create_multipart_upload_rejects_decompress_with_any_sse_header() {
        for header in [
            "x-amz-server-side-encryption",
            "x-amz-server-side-encryption-customer-algorithm",
            "x-amz-server-side-encryption-customer-key",
            "x-amz-server-side-encryption-customer-key-md5",
        ] {
            let state = test_state_with_bucket("test-bucket").await;
            let mut req = multipart_create_request("test-bucket", "archive.zip");
            req.uri = "/test-bucket/archive.zip?uploads=&decompress-zip=prefix%2F"
                .parse()
                .unwrap();
            req.headers.insert(
                http::header::HeaderName::from_static(header),
                http::HeaderValue::from_static("bogus"),
            );

            assert_eq!(
                create_multipart_upload(&state, req)
                    .await
                    .unwrap_err()
                    .code()
                    .as_str(),
                "InvalidArgument",
                "header {header} must be rejected before encryption parsing"
            );
        }
    }

    #[tokio::test]
    async fn upload_part_replacement_keeps_old_and_new_part_pins() {
        let kubo = multipart_kubo(&["QmOldPart", "QmNewPart"]).await;
        let state = test_state_with_bucket_and_kubo("test-bucket", kubo.uri()).await;
        seed_plain_upload(&state, "upload-1").await;

        upload_part(&state, upload_part_request("upload-1", b"old"))
            .await
            .unwrap();
        upload_part(&state, upload_part_request("upload-1", b"new data"))
            .await
            .unwrap();

        let part = crate::store::multipart::get_part(state.store.db(), "upload-1", 1)
            .await
            .unwrap();
        assert_eq!(part.cid, "QmNewPart");
        assert_eq!(part.size, 8);
        assert_pin_add_count(&kubo, "QmOldPart", 1).await;
        assert_pin_add_count(&kubo, "QmNewPart", 1).await;
        assert_no_pin_removes(&kubo, &["QmOldPart", "QmNewPart"]).await;
    }

    #[tokio::test]
    async fn upload_part_initial_db_failure_keeps_new_pin() {
        let kubo = multipart_kubo(&["QmNewPart"]).await;
        let state = test_state_with_bucket_and_kubo("test-bucket", kubo.uri()).await;
        seed_plain_upload(&state, "upload-1").await;
        state
            .store
            .db()
            .execute(Statement::from_string(
                DatabaseBackend::Sqlite,
                "CREATE TRIGGER fail_part_insert BEFORE INSERT ON multipart_parts \
                 BEGIN SELECT RAISE(FAIL, 'forced part insert failure'); END;",
            ))
            .await
            .unwrap();

        let error = upload_part(&state, upload_part_request("upload-1", b"new"))
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InternalError");
        assert!(
            crate::store::multipart::list_parts(state.store.db(), "upload-1")
                .await
                .unwrap()
                .is_empty()
        );
        assert_pin_add_count(&kubo, "QmNewPart", 1).await;
        assert_no_pin_removes(&kubo, &["QmNewPart"]).await;
    }

    #[tokio::test]
    async fn upload_part_db_failure_keeps_new_pin_and_old_row() {
        let kubo = multipart_kubo(&["QmNewPart"]).await;
        let state = test_state_with_bucket_and_kubo("test-bucket", kubo.uri()).await;
        seed_plain_upload(&state, "upload-1").await;
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-1",
            1,
            "QmOldPart",
            3,
            "QmOldPart",
        )
        .await
        .unwrap();
        state
            .store
            .db()
            .execute(Statement::from_string(
                DatabaseBackend::Sqlite,
                "CREATE TRIGGER fail_part_update BEFORE UPDATE ON multipart_parts \
                 BEGIN SELECT RAISE(FAIL, 'forced part update failure'); END;",
            ))
            .await
            .unwrap();

        let error = upload_part(&state, upload_part_request("upload-1", b"new"))
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InternalError");
        let part = crate::store::multipart::get_part(state.store.db(), "upload-1", 1)
            .await
            .unwrap();
        assert_eq!(part.cid, "QmOldPart");
        assert_pin_add_count(&kubo, "QmNewPart", 1).await;
        assert_no_pin_removes(&kubo, &["QmOldPart", "QmNewPart"]).await;
    }

    #[tokio::test]
    async fn complete_inner_returns_archive_with_distinct_encryption_and_attempt_identities() {
        let kubo = multipart_kubo(&["QmRoot", "QmRoot"]).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"part data"))
            .mount(&kubo)
            .await;
        let state = test_state_with_bucket_and_kubo("test-bucket", kubo.uri()).await;
        crate::store::multipart::create_upload(
            state.store.db(),
            "upload-1",
            "encryption-object-1",
            "test-bucket",
            "archive.zip",
            "none",
            None,
            None,
            Some("application/zip"),
            Some(serde_json::json!({"source": "multipart"})),
            Some("prefix/"),
            false,
        )
        .await
        .unwrap();
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-1",
            1,
            "QmPart",
            9,
            "QmPart",
        )
        .await
        .unwrap();

        let first = complete_multipart_upload_inner(&state, complete_request("upload-1", "QmPart"))
            .await
            .unwrap();
        let second =
            complete_multipart_upload_inner(&state, complete_request("upload-1", "QmPart"))
                .await
                .unwrap();

        assert_eq!(first.bucket, "test-bucket");
        assert_eq!(first.key, "archive.zip");
        assert_eq!(first.upload_id, "upload-1");
        assert_eq!(first.encryption_object_id, "encryption-object-1");
        assert_eq!(second.encryption_object_id, "encryption-object-1");
        uuid::Uuid::parse_str(&first.completion_attempt_id).unwrap();
        uuid::Uuid::parse_str(&second.completion_attempt_id).unwrap();
        assert_ne!(first.completion_attempt_id, second.completion_attempt_id);
        assert_eq!(first.root_cid, "QmRoot");
        assert_eq!(second.root_cid, "QmRoot");
        assert_eq!(first.total_size, 9);
        assert_eq!(first.content_type.as_deref(), Some("application/zip"));
        assert_eq!(
            first.metadata,
            Some(serde_json::json!({"source": "multipart"}))
        );
        assert!(!first.encrypted);
        assert!(first.key_wrap.is_none());
        assert_eq!(first.decompress_zip_target.as_deref(), Some("prefix/"));
        assert!(!first.decompress_zip_result);
        assert!(first.server_side_encryption.is_none());
        assert!(
            crate::store::entities::object::Entity::find_by_id(&first.completion_attempt_id)
                .one(state.store.db())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            crate::store::entities::object::Entity::find_by_id(&second.completion_attempt_id)
                .one(state.store.db())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            crate::store::multipart::get_upload(state.store.db(), "upload-1")
                .await
                .is_ok()
        );
        assert!(
            crate::store::multipart::get_part(state.store.db(), "upload-1", 1)
                .await
                .is_ok()
        );
        assert_pin_add_count(&kubo, "QmRoot", 2).await;
        assert_no_pin_removes(&kubo, &["QmRoot", "QmPart"]).await;
    }

    #[tokio::test]
    async fn complete_inner_sse_reencrypts_with_an_embedded_random_nonce() {
        let object_key = crate::crypto::key::ObjectKey { bytes: [7; 32] };
        let master_key = crate::crypto::key::MasterKey::from_hex(
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();
        let wrapped_key = master_key.wrap(&object_key).unwrap();
        let part_nonce = [0x21; 12];
        let encrypted_part =
            crate::crypto::aes_gcm::encrypt_chunk(&object_key, &part_nonce, b"part data").unwrap();
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmRoot\",\"Size\":\"9\"}\n"),
            )
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[]}"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(encrypted_part))
            .mount(&kubo)
            .await;
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "test-bucket", None)
            .await
            .unwrap();
        let state = Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new(kubo.uri()),
            store: crate::store::Store::new(db),
            credentials: HashMap::new(),
            master_key,
        });
        crate::store::multipart::create_upload(
            state.store.db(),
            "upload-1",
            "encryption-object-1",
            "test-bucket",
            "archive.zip",
            "sse_s3",
            Some(&wrapped_key),
            None,
            Some("application/zip"),
            None,
            None,
            true,
        )
        .await
        .unwrap();
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-1",
            1,
            "QmEncryptedPart",
            9,
            "QmEncryptedPart",
        )
        .await
        .unwrap();

        let completed = complete_multipart_upload_inner(
            &state,
            complete_request("upload-1", "QmEncryptedPart"),
        )
        .await
        .unwrap();

        assert_eq!(completed.encryption_object_id, "encryption-object-1");
        assert!(completed.encrypted);
        assert_eq!(completed.key_wrap.as_deref(), Some(wrapped_key.as_str()));
        let requests = kubo.received_requests().await.unwrap();
        let add_body = &requests
            .iter()
            .find(|request| request.url.path() == "/api/v0/add")
            .unwrap()
            .body;
        let frame_len = b"part data".len() + 12 + 16;
        assert!(add_body.windows(frame_len).any(|window| {
            crate::crypto::aes_gcm::decrypt_chunk(&object_key, window)
                .is_ok_and(|plaintext| plaintext.as_ref() == b"part data")
        }));
    }

    #[tokio::test]
    async fn shared_part_cid_survives_upload_part_replacement() {
        let kubo = multipart_kubo(&["QmNewPart"]).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"shared bytes"))
            .mount(&kubo)
            .await;
        let state = test_state_with_bucket_and_kubo("test-bucket", kubo.uri()).await;
        seed_ordinary_shared_object(&state, "QmSharedPart", 12).await;
        seed_plain_upload(&state, "upload-1").await;
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-1",
            1,
            "QmSharedPart",
            12,
            "QmSharedPart",
        )
        .await
        .unwrap();

        upload_part(&state, upload_part_request("upload-1", b"replacement"))
            .await
            .unwrap();

        assert_eq!(
            read_object_through_get(&state, "ordinary.txt").await,
            b"shared bytes"
        );
        assert_no_pin_removes(&kubo, &["QmSharedPart", "QmNewPart"]).await;
    }

    #[tokio::test]
    async fn shared_part_cid_survives_abort() {
        let kubo = multipart_kubo(&[]).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"shared bytes"))
            .mount(&kubo)
            .await;
        let state = test_state_with_bucket_and_kubo("test-bucket", kubo.uri()).await;
        seed_ordinary_shared_object(&state, "QmSharedPart", 12).await;
        seed_plain_upload(&state, "upload-1").await;
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-1",
            1,
            "QmSharedPart",
            12,
            "QmSharedPart",
        )
        .await
        .unwrap();

        abort_multipart_upload(&state, abort_request("upload-1"))
            .await
            .unwrap();

        assert_eq!(
            read_object_through_get(&state, "ordinary.txt").await,
            b"shared bytes"
        );
        assert!(
            crate::store::multipart::get_upload(state.store.db(), "upload-1")
                .await
                .is_err()
        );
        assert!(
            crate::store::multipart::list_parts(state.store.db(), "upload-1")
                .await
                .unwrap()
                .is_empty()
        );
        assert_no_pin_removes(&kubo, &["QmSharedPart"]).await;
    }

    #[tokio::test]
    async fn shared_part_cid_survives_complete() {
        let kubo = multipart_kubo(&["QmRoot"]).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"shared bytes"))
            .mount(&kubo)
            .await;
        let state = test_state_with_bucket_and_kubo("test-bucket", kubo.uri()).await;
        seed_ordinary_shared_object(&state, "QmSharedPart", 12).await;
        seed_plain_upload(&state, "upload-1").await;
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-1",
            1,
            "QmSharedPart",
            12,
            "QmSharedPart",
        )
        .await
        .unwrap();

        complete_multipart_upload(&state, complete_request("upload-1", "QmSharedPart"))
            .await
            .unwrap();

        assert_eq!(
            read_object_through_get(&state, "ordinary.txt").await,
            b"shared bytes"
        );
        assert_no_pin_removes(&kubo, &["QmSharedPart", "QmRoot"]).await;
    }

    #[tokio::test]
    async fn complete_single_part_equal_root_keeps_readable_cid() {
        let kubo = multipart_kubo(&["QmPart"]).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"part data"))
            .mount(&kubo)
            .await;
        let state = test_state_with_bucket_and_kubo("test-bucket", kubo.uri()).await;
        seed_plain_upload(&state, "upload-1").await;
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-1",
            1,
            "QmPart",
            9,
            "QmPart",
        )
        .await
        .unwrap();

        complete_multipart_upload(&state, complete_request("upload-1", "QmPart"))
            .await
            .unwrap();

        let latest =
            crate::store::object::get_latest(state.store.db(), "test-bucket", "archive.zip")
                .await
                .unwrap();
        assert_eq!(latest.cid, "QmPart");
        assert!(
            crate::store::multipart::get_upload(state.store.db(), "upload-1")
                .await
                .is_err()
        );
        assert!(
            crate::store::multipart::list_parts(state.store.db(), "upload-1")
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            read_object_through_get(&state, "archive.zip").await,
            b"part data"
        );
        assert_no_pin_removes(&kubo, &["QmPart"]).await;
    }

    #[tokio::test]
    async fn outcome_unknown_committed_reconciliation_succeeds_without_pin_removal() {
        let kubo = MockServer::start().await;
        let archive = completed_archive("attempt-1");
        let store = FakeFinalizerStore {
            commit: FakeCommitResult::OutcomeUnknown("attempt-1".to_owned()),
            reconcile: FakeReconcileResult::Committed,
            reconcile_calls: AtomicUsize::new(0),
        };

        finalize_completed_multipart_archive_with_store(&archive, &store)
            .await
            .unwrap();

        assert_eq!(store.reconcile_calls.load(Ordering::SeqCst), 1);
        assert_no_pin_removes(&kubo, &["QmRoot", "QmPart"]).await;
    }

    #[tokio::test]
    async fn finalizer_direct_commit_and_rollback_do_not_reconcile_or_remove_pins() {
        let kubo = MockServer::start().await;
        let archive = completed_archive("attempt-1");
        let committed = FakeFinalizerStore {
            commit: FakeCommitResult::Ok,
            reconcile: FakeReconcileResult::Unknown,
            reconcile_calls: AtomicUsize::new(0),
        };
        finalize_completed_multipart_archive_with_store(&archive, &committed)
            .await
            .unwrap();
        assert_eq!(committed.reconcile_calls.load(Ordering::SeqCst), 0);

        let rolled_back = FakeFinalizerStore {
            commit: FakeCommitResult::RolledBack("attempt-1".to_owned()),
            reconcile: FakeReconcileResult::Unknown,
            reconcile_calls: AtomicUsize::new(0),
        };
        let error = finalize_completed_multipart_archive_with_store(&archive, &rolled_back)
            .await
            .unwrap_err();
        assert_eq!(error.code().as_str(), "InternalError");
        assert_eq!(rolled_back.reconcile_calls.load(Ordering::SeqCst), 0);
        assert_no_pin_removes(&kubo, &["QmRoot", "QmPart"]).await;
    }

    #[tokio::test]
    async fn outcome_unknown_not_committed_returns_error_without_pin_removal() {
        let kubo = MockServer::start().await;
        let archive = completed_archive("attempt-1");
        let store = FakeFinalizerStore {
            commit: FakeCommitResult::OutcomeUnknown("attempt-1".to_owned()),
            reconcile: FakeReconcileResult::NotCommitted,
            reconcile_calls: AtomicUsize::new(0),
        };

        let error = finalize_completed_multipart_archive_with_store(&archive, &store)
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InternalError");
        assert_eq!(store.reconcile_calls.load(Ordering::SeqCst), 1);
        assert_no_pin_removes(&kubo, &["QmRoot", "QmPart"]).await;
    }

    #[tokio::test]
    async fn outcome_unknown_query_failure_returns_error_without_pin_removal() {
        let kubo = MockServer::start().await;
        let archive = completed_archive("attempt-1");
        let store = FakeFinalizerStore {
            commit: FakeCommitResult::OutcomeUnknown("attempt-1".to_owned()),
            reconcile: FakeReconcileResult::Unknown,
            reconcile_calls: AtomicUsize::new(0),
        };

        let error = finalize_completed_multipart_archive_with_store(&archive, &store)
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InternalError");
        assert!(error.to_string().contains("reconciliation failed"));
        assert_eq!(store.reconcile_calls.load(Ordering::SeqCst), 1);
        assert_no_pin_removes(&kubo, &["QmRoot", "QmPart"]).await;
    }

    #[tokio::test]
    async fn finalizer_rejects_error_for_different_completion_attempt() {
        let kubo = MockServer::start().await;
        let archive = completed_archive("attempt-1");
        let store = FakeFinalizerStore {
            commit: FakeCommitResult::OutcomeUnknown("other-attempt".to_owned()),
            reconcile: FakeReconcileResult::Committed,
            reconcile_calls: AtomicUsize::new(0),
        };

        let error = finalize_completed_multipart_archive_with_store(&archive, &store)
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InternalError");
        assert_eq!(store.reconcile_calls.load(Ordering::SeqCst), 0);
        assert_no_pin_removes(&kubo, &["QmRoot", "QmPart"]).await;
    }

    #[tokio::test]
    async fn finalize_delete_trigger_reports_rolled_back_and_keeps_all_pins() {
        let kubo = multipart_kubo(&["QmRootFirst", "QmRootRetry"]).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"part data"))
            .mount(&kubo)
            .await;
        let state = test_state_with_bucket_and_kubo("test-bucket", kubo.uri()).await;
        crate::store::object::upsert(
            state.store.db(),
            "old-object",
            "test-bucket",
            "archive.zip",
            "QmOld",
            3,
            Some("text/plain"),
            "QmOld",
            None,
            false,
            None,
            None,
            false,
        )
        .await
        .unwrap();
        seed_plain_upload(&state, "upload-1").await;
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-1",
            1,
            "QmPart",
            9,
            "QmPart",
        )
        .await
        .unwrap();

        let first = complete_multipart_upload_inner(&state, complete_request("upload-1", "QmPart"))
            .await
            .unwrap();
        state
            .store
            .db()
            .execute(Statement::from_string(
                DatabaseBackend::Sqlite,
                "CREATE TRIGGER fail_multipart_upload_delete \
                 BEFORE DELETE ON multipart_uploads \
                 BEGIN SELECT RAISE(FAIL, 'forced multipart delete failure'); END;",
            ))
            .await
            .unwrap();

        let store_error = crate::store::multipart::commit_completed_upload(
            state.store.db(),
            "upload-1",
            latest_attempt_for_archive(&first),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            store_error,
            crate::store::multipart::CommitCompletedUploadError::RolledBack {
                ref completion_attempt_id,
                ..
            } if completion_attempt_id == &first.completion_attempt_id
        ));
        let finalize_error = finalize_completed_multipart_archive(&state, &first)
            .await
            .unwrap_err();
        assert_eq!(finalize_error.code().as_str(), "InternalError");
        assert_eq!(
            crate::store::object::get_latest(state.store.db(), "test-bucket", "archive.zip",)
                .await
                .unwrap()
                .id,
            "old-object"
        );
        assert!(
            crate::store::entities::object::Entity::find_by_id(&first.completion_attempt_id)
                .one(state.store.db())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            crate::store::multipart::get_upload(state.store.db(), "upload-1")
                .await
                .is_ok()
        );
        assert!(
            crate::store::multipart::get_part(state.store.db(), "upload-1", 1)
                .await
                .is_ok()
        );
        assert_no_pin_removes(&kubo, &["QmRootFirst", "QmPart"]).await;

        state
            .store
            .db()
            .execute(Statement::from_string(
                DatabaseBackend::Sqlite,
                "DROP TRIGGER fail_multipart_upload_delete",
            ))
            .await
            .unwrap();
        let second =
            complete_multipart_upload_inner(&state, complete_request("upload-1", "QmPart"))
                .await
                .unwrap();
        finalize_completed_multipart_archive(&state, &second)
            .await
            .unwrap();

        let latest =
            crate::store::object::get_latest(state.store.db(), "test-bucket", "archive.zip")
                .await
                .unwrap();
        assert_eq!(latest.id, second.completion_attempt_id);
        let old = crate::store::entities::object::Entity::find_by_id("old-object")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert!(!old.is_latest);
        assert!(
            crate::store::multipart::get_upload(state.store.db(), "upload-1")
                .await
                .is_err()
        );
        assert!(
            crate::store::multipart::list_parts(state.store.db(), "upload-1")
                .await
                .unwrap()
                .is_empty()
        );
        assert_no_pin_removes(&kubo, &["QmRootFirst", "QmRootRetry", "QmPart"]).await;
    }

    #[tokio::test]
    async fn outcome_unknown_attempt_a_does_not_adopt_attempt_b_commit() {
        let kubo = MockServer::start().await;
        let state = test_state_with_bucket_and_kubo("test-bucket", kubo.uri()).await;
        seed_plain_upload(&state, "upload-1").await;
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-1",
            1,
            "QmPart",
            9,
            "QmPart",
        )
        .await
        .unwrap();
        let archive_a = completed_archive("attempt-a");
        let archive_b = completed_archive("attempt-b");
        let (commit_tx, commit_rx) = tokio::sync::oneshot::channel();
        let (reconcile_tx, reconcile_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let a_store = Arc::new(BlockingUnknownFinalizerStore {
            db: state.store.db().clone(),
            commit_signal: tokio::sync::Mutex::new(Some(commit_tx)),
            reconcile_signal: tokio::sync::Mutex::new(Some(reconcile_tx)),
            release_reconcile: tokio::sync::Mutex::new(Some(release_rx)),
        });

        let a_task = tokio::spawn({
            let store = a_store.clone();
            async move {
                finalize_completed_multipart_archive_with_store(&archive_a, store.as_ref()).await
            }
        });
        commit_rx.await.unwrap();
        reconcile_rx.await.unwrap();

        let b_result = finalize_completed_multipart_archive(&state, &archive_b).await;
        release_tx.send(()).unwrap();
        let a_result = a_task.await.unwrap();

        assert!(a_result.is_err());
        assert!(b_result.is_ok());
        assert!(
            crate::store::entities::object::Entity::find_by_id("attempt-a")
                .one(state.store.db())
                .await
                .unwrap()
                .is_none()
        );
        let winner = crate::store::entities::object::Entity::find_by_id("attempt-b")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert!(winner.is_latest);
        assert_eq!(winner.cid, "QmRoot");
        assert!(
            crate::store::multipart::get_upload(state.store.db(), "upload-1")
                .await
                .is_err()
        );
        assert_no_pin_removes(&kubo, &["QmRoot", "QmPart"]).await;
    }

    #[tokio::test]
    async fn concurrent_complete_same_upload_uses_distinct_attempt_ids() {
        let kubo = multipart_kubo(&["QmRoot", "QmRoot"]).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"part data"))
            .mount(&kubo)
            .await;
        let directory = tempfile::tempdir().unwrap();
        let state =
            file_backed_test_state_with_bucket_and_kubo("test-bucket", kubo.uri(), &directory)
                .await;
        seed_plain_upload(&state, "upload-1").await;
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-1",
            1,
            "QmPart",
            9,
            "QmPart",
        )
        .await
        .unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(3));

        let spawn_complete = |state: Arc<AppState>, barrier: Arc<tokio::sync::Barrier>| {
            tokio::spawn(async move {
                let completed =
                    complete_multipart_upload_inner(&state, complete_request("upload-1", "QmPart"))
                        .await
                        .unwrap();
                let attempt_id = completed.completion_attempt_id.clone();
                let encryption_object_id = completed.encryption_object_id.clone();
                barrier.wait().await;
                let result = finalize_completed_multipart_archive(&state, &completed).await;
                (attempt_id, encryption_object_id, result)
            })
        };
        let first = spawn_complete(state.clone(), barrier.clone());
        let second = spawn_complete(state.clone(), barrier.clone());
        barrier.wait().await;

        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert_ne!(first.0, second.0);
        assert_eq!(first.1, "encryption-object-1");
        assert_eq!(second.1, "encryption-object-1");
        assert_ne!(first.2.is_ok(), second.2.is_ok());
        let (winner_attempt_id, loser_attempt_id) = if first.2.is_ok() {
            (&first.0, &second.0)
        } else {
            (&second.0, &first.0)
        };

        let latest =
            crate::store::object::get_latest(state.store.db(), "test-bucket", "archive.zip")
                .await
                .unwrap();
        assert_eq!(&latest.id, winner_attempt_id);
        assert_eq!(latest.cid, "QmRoot");
        assert!(
            crate::store::entities::object::Entity::find_by_id(loser_attempt_id)
                .one(state.store.db())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            crate::store::multipart::get_upload(state.store.db(), "upload-1")
                .await
                .is_err()
        );
        assert!(
            crate::store::multipart::list_parts(state.store.db(), "upload-1")
                .await
                .unwrap()
                .is_empty()
        );
        assert_pin_add_count(&kubo, "QmRoot", 2).await;
        assert_no_pin_removes(&kubo, &["QmRoot", "QmPart"]).await;
    }
}
