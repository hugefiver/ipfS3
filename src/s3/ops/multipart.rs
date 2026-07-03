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

    let enc_mode = determine_encryption_mode(&req.headers)?;
    let metadata = extract_custom_metadata(&req.headers);
    let object_id = uuid::Uuid::new_v4().to_string();
    let upload_id = uuid::Uuid::new_v4().to_string();

    // For SSE-S3 we generate a per-object key now and persist its wrapped form
    // so the same key can be reused for every part. SSE-C keys are supplied
    // per-request and are never stored.
    let key_wrap: Option<String> = match enc_mode {
        EncryptionMode::SseS3 => {
            let ok = state.master_key.generate_object_key();
            let wrapped = state
                .master_key
                .wrap(&ok)
                .map_err(|e| s3s::s3_error!(InternalError, "key wrap: {e}"))?;
            Some(wrapped)
        }
        _ => None,
    };

    crate::store::multipart::create_upload(
        db,
        &upload_id,
        &object_id,
        bucket,
        key,
        enc_mode.as_str(),
        key_wrap.as_deref(),
        content_type.as_deref(),
        metadata,
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

    let body = req
        .input
        .body
        .ok_or_else(|| s3s::s3_error!(IncompleteBody, "request body is missing"))?;

    let (counter, count_handle) = ByteCounter::new();
    let stream = counter.wrap(body);

    let enc_mode = EncryptionMode::parse(&upload.encryption_mode);

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
            let encrypted_stream = crate::crypto::chunker::encrypt_chunk_stream(
                pinned,
                Arc::new(ok),
                upload.object_id.clone(),
                part_number as u32,
            );
            crate::kubo::add::stream_add(&state.kubo, encrypted_stream, 1)
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?
        }
        EncryptionMode::SseC => {
            let ok = extract_sse_c_key(&req.headers)?;
            let pinned = Box::pin(stream);
            let encrypted_stream = crate::crypto::chunker::encrypt_chunk_stream(
                pinned,
                Arc::new(ok),
                upload.object_id.clone(),
                part_number as u32,
            );
            crate::kubo::add::stream_add(&state.kubo, encrypted_stream, 1)
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?
        }
    };

    let part_size = count_handle.load(Ordering::Relaxed) as i64;

    // S3 allows re-uploading a part with the same part_number to replace a
    // previous upload. If a part with this number already exists, unpin its
    // old CID and delete the record before inserting the new one.
    if let Ok(old_part) = crate::store::multipart::get_part(db, upload_id, part_number).await {
        let _ = crate::kubo::pin::pin_rm(&state.kubo, &old_part.cid).await;
        let _ = crate::store::multipart::delete_part(db, upload_id, part_number).await;
    }

    // Pin the part before recording it; if the DB insert fails we unpin so the
    // CID does not linger as garbage in the IPFS node.
    crate::kubo::pin::pin_add(&state.kubo, &cid)
        .await
        .map_err(|e| s3s::s3_error!(InternalError, "pin: {e}"))?;

    if let Err(e) =
        crate::store::multipart::insert_part(db, upload_id, part_number, &cid, part_size, &cid)
            .await
    {
        // Best-effort cleanup of the pinned part on DB failure.
        let _ = crate::kubo::pin::pin_rm(&state.kubo, &cid).await;
        return Err(e.into());
    }

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

    if upload.bucket != *bucket || upload.key != *key {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "bucket/key mismatch for upload_id"
        ));
    }

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
    let total_size: i64 = parts_to_concat.iter().map(|(_, s)| s).sum();
    let part_cids: Vec<String> = parts_to_concat.iter().map(|(c, _)| c.clone()).collect();

    let enc_mode = EncryptionMode::parse(&upload.encryption_mode);
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
                EncryptionMode::SseC => {
                    // SSE-C: the customer key must be provided on Complete.
                    // We need it to decrypt the parts.
                    super::object::extract_sse_c_key(&req.headers)?
                }
                _ => unreachable!(),
            };
            let ok_arc = Arc::new(ok);
            let object_id = upload.object_id.clone();

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
            let encrypted_stream = crate::crypto::chunker::encrypt_chunk_stream(
                Box::pin(plaintext_concat),
                ok_arc,
                object_id,
                0, // single object, part_number = 0
            );

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

    // Store metadata. If DB fails, unpin root_cid so it can be GC'd
    // (consistent with PutObject's cleanup logic).
    if let Err(e) = crate::store::object::upsert(
        db,
        &upload.object_id,
        bucket,
        key,
        &root_cid,
        total_size,
        upload.content_type.as_deref(),
        &root_cid,
        upload.metadata.clone(),
        encrypted,
        key_wrap.as_deref(),
        true,
    )
    .await
    {
        let _ = crate::kubo::pin::pin_rm(&state.kubo, &root_cid).await;
        return Err(e.into());
    }

    // The part CIDs are now subsumed by the root object; unpin them so they
    // can be garbage-collected by the IPFS node.
    for (part_cid, _) in &parts_to_concat {
        let _ = crate::kubo::pin::pin_rm(&state.kubo, part_cid).await;
    }

    // Delete the upload record; ON DELETE CASCADE automatically removes parts.
    crate::store::multipart::delete_upload(db, upload_id).await?;

    let server_side_encryption = if encrypted && key_wrap.is_some() {
        Some(ServerSideEncryption::from_static("AES256"))
    } else {
        None
    };

    Ok(S3Response::new(CompleteMultipartUploadOutput {
        bucket: Some(bucket.clone()),
        key: Some(key.clone()),
        e_tag: Some(ETag::Strong(root_cid.clone())),
        server_side_encryption,
        ..Default::default()
    }))
}

/// Abort a multipart upload, discarding all uploaded parts.
///
/// Best-effort: part pins and DB rows are removed even if some pin-rm calls
/// fail, then the upload record itself is deleted.
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

    // List parts for cleanup. Distinguish NoSuchUpload (safe to ignore —
    // nothing to clean up) from other DB errors (must propagate).
    let parts = match crate::store::multipart::list_parts(db, upload_id).await {
        Ok(p) => p,
        Err(crate::error::AppError::NoSuchUpload(_)) => Vec::new(),
        Err(e) => return Err(e.into()),
    };

    for part in &parts {
        let _ = crate::kubo::pin::pin_rm(&state.kubo, &part.cid).await;
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
