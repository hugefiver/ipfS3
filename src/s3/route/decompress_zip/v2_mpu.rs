//! ZIP v2 multipart Complete: publish only the captured source/output selection.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use http::{HeaderMap, StatusCode};
use s3s::{Body, S3Request, S3Response, S3Result, dto::CompleteMultipartUploadInput};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    error::AppError,
    state::AppState,
    store::{
        self,
        import::ownership::StandardMutationGuard,
        multipart::v2_zip::{CompleteReceipt, CompleteState, Record},
        pinning::publication::{
            CapturedZipPublication, PublicationObject, ZipExecutionSettlement,
            ZipPublicationAuthority, application::publish_captured_zip,
        },
        zip::{self, execution},
    },
    zip::{
        batch::{FinalZipManifest, ManifestFailure, ManifestFile},
        options::ZipTargets,
    },
};

const LEASE_SECONDS: i64 = 60;

fn conflict() -> s3s::S3Error {
    let mut error = s3s::s3_error!(
        OperationAborted,
        "ZIP v2 multipart Complete is fenced or conflicts"
    );
    error.set_status_code(StatusCode::CONFLICT);
    error
}

fn store_error(error: AppError) -> s3s::S3Error {
    match error {
        AppError::InvalidZipParameter(_)
        | AppError::ZipIdempotencyConflict
        | AppError::StaleContentMutation => conflict(),
        other => other.into(),
    }
}

fn receipt_response(receipt: CompleteReceipt) -> S3Result<S3Response<Body>> {
    let zero_success = matches!(
        receipt
            .headers
            .get("x-ipfs-s3-zip-batch-status")
            .map(String::as_str),
        Some("failed" | "empty")
    );
    if zero_success
        && (receipt.headers.contains_key("etag")
            || receipt.headers.contains_key("x-amz-version-id")
            || !receipt
                .xml
                .contains("<SourcePublished>false</SourcePublished>"))
    {
        return Err(conflict());
    }
    let mut headers = HeaderMap::new();
    for (name, value) in receipt.headers {
        headers.insert(
            http::header::HeaderName::from_bytes(name.as_bytes()).map_err(|_| conflict())?,
            http::HeaderValue::from_str(&value).map_err(|_| conflict())?,
        );
    }
    if zero_success {
        if !headers.contains_key(http::header::DATE) {
            return Err(conflict());
        }
        // Both first Complete and exact replay return this S3 error *after*
        // the batch/receipt commit. Date and batch handle are frozen headers;
        // transport must not replace them with a fresh replay timestamp.
        let mut response = S3Response::with_status(
            Body::from(concat!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                "<Error><Code>InvalidRequest</Code>",
                "<Message>ZIP v2 multipart batch published no files; query its status with the original UploadId</Message></Error>"
            ).to_owned()),
            StatusCode::BAD_REQUEST,
        );
        response.headers = headers;
        return Ok(response);
    }
    Ok(S3Response::with_headers(Body::from(receipt.xml), headers))
}

struct Lease {
    cancelled: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Lease {
    fn start(state: Arc<AppState>, claim: execution::Claim) -> Self {
        let cancelled = CancellationToken::new();
        let stop = cancelled.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {
                        match store::import::ownership::renew_zip_v2_direct_group(state.store.db(), &claim, LEASE_SECONDS).await {
                            Ok(true) => {},
                            Ok(false) | Err(_) => { stop.cancel(); break; }
                        }
                    }
                }
            }
        });
        Self { cancelled, task }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.cancelled.cancel();
        self.task.abort();
    }
}

async fn retained_manifest(
    state: &AppState,
    intake: &Record,
    claim: &execution::Claim,
) -> S3Result<(FinalZipManifest, Option<StandardMutationGuard>)> {
    let execution = execution::read(state.store.db(), &claim.batch_id)
        .await
        .map_err(store_error)?
        .ok_or_else(conflict)?;
    let retained = zip::snapshot(state.store.db(), &claim.batch_id)
        .await?
        .ok_or_else(conflict)?;
    if execution.state != "admitted"
        || execution.epoch != claim.epoch
        || execution.worker.as_deref() != Some(&claim.worker)
        || retained.batch.state != "open"
        || !retained.batch.manifest_prepared
        || retained.batch.source != "mpu"
        || retained.batch.owner != intake.owner
        || retained.batch.token != intake.token
        || retained.batch.bucket != intake.bucket
        || retained.batch.archive_key != intake.archive_key
        || retained.batch.captured_options != intake.captured_options
        || retained.batch.source_published
    {
        return Err(conflict());
    }
    let options = intake.options().map_err(|_| conflict())?;
    let source_guard = if options.publish_source {
        let generation = retained
            .batch
            .input_identity
            .strip_prefix("zip-v2-source-gen:")
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value > 0)
            .ok_or_else(conflict)?;
        Some(StandardMutationGuard {
            bucket: intake.bucket.clone(),
            key: intake.archive_key.clone(),
            mutation_id: format!("zip-v2-source:{}", claim.batch_id),
            expected_generation: generation,
            mutation_prefix: None,
        })
    } else {
        if retained.batch.input_identity != "pending" {
            return Err(conflict());
        }
        None
    };
    let mut manifest = FinalZipManifest::default();
    for entry in retained.entries {
        if entry.version_row_id.is_some() {
            return Err(conflict());
        }
        match (entry.object_key, entry.cid, entry.size, entry.error_code) {
            (Some(key), Some(cid), Some(size), None) if size >= 0 => {
                if key.strip_prefix(&intake.target_prefix) != Some(entry.path.as_str()) {
                    manifest.root_error = Some("invalid_manifest");
                }
                manifest.successful.push(ManifestFile {
                    relative_path: entry.path,
                    object_key: key,
                    cid,
                    size,
                });
            }
            (None, None, None, Some(code)) => {
                let code = match code.as_str() {
                    "entry_read_failed" => "entry_read_failed",
                    "entry_upload_failed" => "entry_upload_failed",
                    "entry_failed" => "entry_failed",
                    _ => return Err(conflict()),
                };
                manifest.failed.push(ManifestFailure {
                    path: entry.path,
                    code,
                });
            }
            _ => return Err(conflict()),
        }
    }
    Ok((manifest, source_guard))
}

async fn admit_manifest(
    state: &AppState,
    intake: &Record,
    claim: &execution::Claim,
    manifest: &FinalZipManifest,
    publish_source: bool,
) -> S3Result<Option<StandardMutationGuard>> {
    let work = store::pinning::management::records::SharedDecodeBudget::default();
    let tx = state
        .store
        .begin_runtime(store::pinning::management::records::DecodeWork::shared(
            &work,
        ))
        .await?;
    store::import::ownership::lock_bucket_for_ownership(&tx, &intake.bucket).await?;
    let items = manifest
        .successful
        .iter()
        .map(|file| execution::ManifestItem::Success {
            path: file.relative_path.clone(),
            object_key: file.object_key.clone(),
            cid: file.cid.clone(),
            size: file.size,
        })
        .chain(
            manifest
                .failed
                .iter()
                .map(|failure| execution::ManifestItem::Failure {
                    path: failure.path.clone(),
                    code: failure.code.into(),
                }),
        )
        .collect::<Vec<_>>();
    let ids = manifest
        .successful
        .iter()
        .map(|file| (file.object_key.clone(), uuid::Uuid::new_v4().to_string()))
        .collect::<BTreeMap<_, _>>();
    let source_guard = if publish_source {
        Some(
            store::import::ownership::admit_zip_v2_source_in_transaction(
                &tx,
                &intake.bucket,
                &intake.archive_key,
                &ids.keys().cloned().collect::<BTreeSet<_>>(),
                &format!("zip-v2-source:{}", claim.batch_id),
                chrono::Utc::now(),
            )
            .await
            .map_err(store_error)?,
        )
    } else {
        None
    };
    execution::admit_manifest_in_transaction(&tx, claim, &items, &ids)
        .await
        .map_err(store_error)?;
    zip::BatchAdmission::admit_runtime_mirror(
        &tx,
        &zip::BatchAdmission {
            id: claim.batch_id.clone(),
            owner: intake.owner.clone(),
            source: "mpu".into(),
            token: intake.token.clone(),
            fingerprint: "pending".into(),
            bucket: intake.bucket.clone(),
            archive_key: intake.archive_key.clone(),
            input_identity: "pending".into(),
            captured_options: intake.captured_options.clone(),
        },
    )
    .await
    .map_err(store_error)?;
    zip::ManifestItem::prepare_manifest_in_transaction(
        &tx,
        &claim.batch_id,
        &manifest.manifest_items(),
    )
    .await
    .map_err(store_error)?;
    if let Some(guard) = &source_guard {
        store::import::ownership::capture_zip_v2_source_guard_in_transaction(&tx, claim, guard)
            .await
            .map_err(store_error)?;
    }
    tx.commit().await?;
    Ok(source_guard)
}

pub(super) async fn complete(
    state: &Arc<AppState>,
    req: S3Request<CompleteMultipartUploadInput>,
    intake: Record,
    principal: &str,
) -> S3Result<S3Response<Body>> {
    let id = &req.input.upload_id;
    if principal != intake.owner
        || id != &intake.original_upload_id
        || req.input.bucket != intake.bucket
        || req.input.key != intake.archive_key
        || intake.execution_id != *id
    {
        return Err(conflict());
    }
    let historical =
        crate::zip::options::decode_historical_zip_options(&intake.captured_options, None)
            .map_err(|_| conflict())?;
    let options = intake.options().map_err(|_| conflict())?;
    if (!options.publish_source && !options.publish_extracted)
        || options.result_version != 2
        || options.token != intake.token
        || (matches!(options.targets, ZipTargets::Source | ZipTargets::Both)
            && !options.publish_source)
        || (matches!(options.targets, ZipTargets::Extracted | ZipTargets::Both)
            && !options.publish_extracted)
    {
        return Err(conflict());
    }
    super::replay_controls::verify(
        &req.headers,
        &crate::s3::query::decoded_query_pairs(&req.uri)?,
        &crate::zip::options::ZipOptions::V2(options.clone()),
        &intake.target_prefix,
        Some(true),
        options.root_enabled,
    )?;
    let admitted = execution::read(state.store.db(), &intake.execution_id)
        .await
        .map_err(store_error)?
        .ok_or_else(conflict)?;
    if admitted.source != "mpu"
        || admitted.owner != principal
        || admitted.bucket != req.input.bucket
        || admitted.source_key != req.input.key
        || admitted.token != intake.token
        || admitted.captured_options != intake.captured_options
        || admitted.request_fingerprint != intake.request_fingerprint
        || admitted.request_fingerprint
            != hex::encode(Sha256::digest(admitted.request_contract.as_bytes()))
    {
        return Err(conflict());
    }
    if historical.expected_sha256.as_ref().is_some_and(|expected| {
        admitted
            .input_sha256
            .as_ref()
            .is_some_and(|sha| sha != expected)
    }) {
        return Err(conflict());
    }
    if req.headers.contains_key("x-amz-tagging") {
        let mut tags = super::parse_publication_tags(&req.headers)?;
        tags.sort_by(|left, right| left.key.cmp(&right.key));
        let contract: serde_json::Value =
            serde_json::from_str(&admitted.request_contract).map_err(|_| conflict())?;
        if contract.get("tags") != Some(&serde_json::json!(tags)) {
            return Err(conflict());
        }
    }
    if super::has_sse_header(&req.headers) {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "ZIP multipart cannot use server-side encryption"
        ));
    }
    let intent = intake.zip_intent().map_err(|_| conflict())?;
    if let Some(intent) = &intent
        && (intent.bucket() != intake.bucket
            || intent.source_key() != intake.archive_key
            || intent.target_prefix() != intake.target_prefix
            || intent.origin().principal_id != principal
            || intent.origin().request_id != *id
            || intent.v2_options().as_ref() != Some(&options))
    {
        return Err(conflict());
    }
    let parts = req
        .input
        .multipart_upload
        .as_ref()
        .and_then(|body| body.parts.as_ref())
        .ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing multipart Complete parts"))?;
    let contract =
        store::multipart::v2_zip::complete_request_contract(parts).map_err(store_error)?;
    let db = state.store.db();
    if admitted.state == "pending"
        && let Some(intent) = &intent
    {
        store::pinning::publication::preflight_captured_zip_routes(
            &state.store,
            intent,
            state.pinning.effective_config(),
            None,
        )
        .await?;
    }
    match store::multipart::v2_zip::capture_complete(
        db,
        &intake,
        principal,
        &req.input.bucket,
        &req.input.key,
        parts,
    )
    .await
    .map_err(store_error)?
    {
        CompleteState::Completed(receipt) => return receipt_response(receipt),
        CompleteState::Pending => {}
    }
    let intent = intent.ok_or_else(|| {
        s3s::s3_error!(
            OperationAborted,
            "ZIP capture needs_attention: missing original multipart intent"
        )
    })?;
    if admitted.state != "admitted" && super::v2::current_io_denied(state, &intent, None) {
        return Err(conflict());
    }
    let claim = execution::claim(db, id, &uuid::Uuid::new_v4().to_string(), LEASE_SECONDS)
        .await
        .map_err(store_error)?
        .ok_or_else(conflict)?;
    let lease = Lease::start(state.clone(), claim.clone());
    let snapshot = execution::read(db, id)
        .await
        .map_err(store_error)?
        .ok_or_else(conflict)?;
    if snapshot.epoch != claim.epoch || snapshot.worker.as_deref() != Some(&claim.worker) {
        return Err(conflict());
    }
    let (manifest, source_guard) = if snapshot.state == "admitted" {
        retained_manifest(state, &intake, &claim).await?
    } else {
        // The assembler validates upload identity, exact stored part ETags and
        // checksums, size and complete CAT EOF before returning a SHA/CID.
        let archive = tokio::select! {
            result = crate::s3::ops::multipart::assemble_zip_v2_archive(state, &req, &intake) => result?,
            _ = lease.cancelled.cancelled() => return Err(conflict()),
        };
        execution::bind_clean_input(
            db,
            &claim,
            &archive.input_sha256,
            &archive.archive_cid,
            archive.archive_size,
        )
        .await
        .map_err(store_error)?;
        let manifest = if options.publish_extracted {
            let stream =
                crate::kubo::cat::stream_cat(&state.kubo, &archive.archive_cid, None).await?;
            let outcome = tokio::select! {
                result = crate::zip::extract::extract_zip_stream_with_limits(state, &intake.target_prefix,
                    stream, super::zip_limits_for_call(state, crate::zip::extract::MAX_DECOMPRESSED_ARCHIVE_BYTES)) => result?,
                _ = lease.cancelled.cancelled() => return Err(conflict()),
            };
            super::reject_archive_key_collision(&intake.archive_key, &outcome.entries)?;
            crate::zip::batch::final_zip_manifest(
                &outcome.entries,
                &outcome.failures,
                &intake.target_prefix,
            )
        } else {
            crate::zip::batch::final_zip_manifest(&[], &[], &intake.target_prefix)
        };
        let guard =
            admit_manifest(state, &intake, &claim, &manifest, options.publish_source).await?;
        (manifest, guard)
    };
    let bound = execution::read(db, id)
        .await
        .map_err(store_error)?
        .ok_or_else(conflict)?;
    if bound.epoch != claim.epoch
        || bound.worker.as_deref() != Some(&claim.worker)
        || bound.state != "admitted"
        || bound.input_art_cid.is_none()
        || bound.input_art_size.is_none()
        || options.publish_source != source_guard.is_some()
    {
        return Err(conflict());
    }
    let source_cid = bound.input_art_cid.as_ref().ok_or_else(conflict)?;
    let upload = if options.publish_source {
        Some(
            store::multipart::get_upload(db, id)
                .await
                .map_err(|_| conflict())?,
        )
    } else {
        None
    };
    if !store::import::ownership::renew_zip_v2_direct_group(db, &claim, LEASE_SECONDS)
        .await
        .map_err(store_error)?
    {
        return Err(conflict());
    }
    let guards = execution::read_targets(db, &claim)
        .await
        .map_err(store_error)?;
    let keys = manifest
        .successful
        .iter()
        .map(|file| file.object_key.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if keys.len() != manifest.successful.len()
        || keys != guards.iter().map(|guard| guard.key.as_str()).collect()
    {
        return Err(conflict());
    }
    store::pinning::publication::preflight_captured_zip_routes(
        &state.store,
        &intent,
        state.pinning.effective_config(),
        Some(&manifest),
    )
    .await?;
    if snapshot.state == "admitted" && super::v2::current_io_denied(state, &intent, Some(&manifest))
    {
        return Err(conflict());
    }
    let root_outcome =
        super::v2::build_zip_root(state, id, &manifest, options.build_root(), &lease.cancelled)
            .await?;
    let successes = manifest
        .successful
        .iter()
        .map(|file| {
            PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                &intake.bucket,
                &file.object_key,
                file.cid.clone(),
                file.size,
                None,
                None,
                false,
                None,
                None,
                chrono::Utc::now(),
            )
        })
        .collect();
    let bound = execution::read(db, id)
        .await
        .map_err(store_error)?
        .ok_or_else(conflict)?;
    if bound.epoch != claim.epoch
        || bound.worker.as_deref() != Some(&claim.worker)
        || bound.state != "admitted"
    {
        return Err(conflict());
    }
    let status = if manifest.successful.is_empty() && !options.publish_source {
        if manifest.failed.is_empty() {
            "empty"
        } else {
            "failed"
        }
    } else {
        "completed"
    };
    let terminal = serde_json::json!({"input_sha256": bound.input_sha256.ok_or_else(conflict)?,
        "published_count": manifest.successful.len(), "failed_count": manifest.failed.len(), "status": status}).to_string();
    let request = CapturedZipPublication {
        batch_id: id.clone(),
        source: upload.as_ref().map(|upload| {
            let mut object = PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                &intake.bucket,
                &intake.archive_key,
                source_cid.clone(),
                bound.input_art_size.expect("validated above"),
                upload.content_type.clone(),
                upload.metadata.clone(),
                false,
                None,
                None,
                chrono::Utc::now(),
            );
            object.multipart = true;
            object
        }),
        source_tags: upload
            .as_ref()
            .map(|upload| {
                store::pinning::tags::tags_from_json(&upload.tags_json).map_err(|_| conflict())
            })
            .transpose()?
            .unwrap_or_default(),
        successes,
        manifest,
        capture: intent,
        authority: ZipPublicationAuthority::Execution {
            claim: claim.clone(),
            source_guard,
            settlement: ZipExecutionSettlement::Mpu { contract },
        },
        root_outcome,
        terminal_result: terminal,
    };
    if lease.cancelled.is_cancelled() {
        return Err(conflict());
    }
    let publication = publish_captured_zip(&state.store, request).await;
    if matches!(publication, Err(AppError::StaleContentMutation)) {
        let fenced = if options.publish_source {
            execution::fence(db, &claim).await
        } else {
            execution::fence_if_lost(db, &claim).await
        };
        if let Err(error) = fenced {
            tracing::warn!(%error, "ZIP multipart guard reconciliation deferred");
        }
    }
    // Read the committed receipt, never infer an unknown commit result from a
    // returned CID or from a later current object projection.
    let parts = req
        .input
        .multipart_upload
        .as_ref()
        .and_then(|body| body.parts.as_ref())
        .ok_or_else(conflict)?;
    match store::multipart::v2_zip::capture_complete(
        db,
        &intake,
        principal,
        &intake.bucket,
        &intake.archive_key,
        parts,
    )
    .await
    .map_err(store_error)?
    {
        CompleteState::Completed(receipt) => receipt_response(receipt),
        CompleteState::Pending => match publication {
            Err(error) => Err(store_error(error)),
            Ok(_) => Err(conflict()),
        },
    }
}
