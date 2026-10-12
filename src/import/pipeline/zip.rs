use crate::{
    import::{
        ImportClaim, ImportExecutionError, ImportFailure, ImportFailureCode,
        decompress::decompress_import_with_context,
        execution_error::{ensure_active, map_publication_error, terminal_failure},
        pipeline::{ImportArtifact, ImportCoordinator, JobCancellation},
        progress::ProgressReporter,
        publication::zip::recover_prepared_zip_import,
        v2_source::{V2SourceError, bound::BoundCatError, fetch_zip_source_with_progress},
        zip_capture::legacy_capture,
    },
    state::AppState,
    store::{
        entities::import_job,
        import::ownership,
        pinning::publication::{
            bind_legacy_import_input, decode_zip_capture_json, read_legacy_import_input,
        },
        zip::{self, BatchAdmission},
    },
};
use chrono::Utc;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(super) async fn execute_zip_job(
    coordinator: &Arc<ImportCoordinator>,
    state: &Arc<AppState>,
    job: &import_job::Model,
    claim: &ImportClaim,
    cancellation: &JobCancellation,
    network_cancel: &CancellationToken,
    reporter: &ProgressReporter,
) -> Result<ImportArtifact, ImportExecutionError> {
    let db = state.store.db();
    let proof = read_legacy_import_input(db, &job.id)
        .await
        .map_err(|error| map_publication_error(error, cancellation))?;
    let capture = legacy_capture(job)?;
    let batch = zip::read(db, &job.id)
        .await
        .map_err(|error| map_publication_error(error, cancellation))?;
    if let Some(batch) = &batch {
        if batch.source != "import"
            || batch.token != job.id
            || batch.owner != capture.origin().principal_id
            || batch.bucket != job.bucket
            || batch.archive_key != job.key
            || batch.input_identity != job.id
            || batch.fingerprint != job.request_fingerprint
            || batch.state != "open"
            || batch.source_published
            || decode_zip_capture_json(&batch.captured_options)
                .ok()
                .as_ref()
                != Some(&capture)
        {
            return Err(terminal_failure(
                ImportFailureCode::PublicationFailed,
                "ZIP batch identity needs_attention",
            ));
        }
    } else {
        zip::admit(db, &BatchAdmission { id: job.id.clone(), owner: capture.origin().principal_id.clone(),
            source: "import".into(), token: job.id.clone(), fingerprint: job.request_fingerprint.clone(),
            bucket: job.bucket.clone(), archive_key: job.key.clone(), input_identity: job.id.clone(),
            captured_options: serde_json::json!({"zip_intent":capture,"target_prefix":capture.target_prefix(),"root_capture":capture.controls().root}).to_string(),
        }).await.map_err(|error| map_publication_error(error, cancellation))?;
    }
    ensure_active(cancellation)?;
    if let Some(artifact) = recover_prepared_zip_import(
        state,
        job,
        claim,
        cancellation,
        reporter,
        coordinator.execution_observer(),
    )
    .await?
    {
        return Ok(artifact);
    }
    // Pending attempts may re-extract the immutable bound input, never replace it
    // with a fresh URL response. Prepared attempts have already returned above.
    ownership::reset_extracted_targets_for_attempt(db, claim, &job.bucket, Utc::now())
        .await
        .map_err(|error| map_publication_error(error, cancellation))?;
    let artifact = if let Some(proof) = proof {
        ImportArtifact {
            cid: proof.cid().to_owned(),
            logical_size: proof.size() as u64,
            object_content_type: bound_content_type(job, batch.as_ref(), &proof)?,
        }
    } else {
        let descriptor = serde_json::to_string(&(&job.source_type, &job.source_value))
            .expect("source strings serialize");
        let verified = fetch_zip_source_with_progress(
            coordinator,
            state,
            &descriptor,
            None,
            network_cancel.clone(),
            &state.pinning.zip_extraction_limits(),
            Some((reporter, cancellation)),
        )
        .await
        .map_err(|error| source_error(error, cancellation))?;
        ensure_active(cancellation)?;
        let content_type = job
            .object_content_type
            .clone()
            .or(verified.object_content_type.clone());
        let metadata =
            serde_json::json!({"schema_version":1,"kind":"verified_input","cid":verified.cid,
            "size":verified.size,"sha256":verified.sha256,"content_type":content_type})
            .to_string();
        crate::store::zip::import_intake::stage_legacy_input_metadata(
            db,
            claim,
            &job.bucket,
            &metadata,
        )
        .await
        .map_err(|error| map_publication_error(error, cancellation))?;
        bind_legacy_import_input(
            db,
            claim,
            &verified.sha256,
            &verified.cid,
            verified.size as i64,
        )
        .await
        .map_err(|error| map_publication_error(error, cancellation))?;
        ImportArtifact {
            cid: verified.cid,
            logical_size: verified.size,
            object_content_type: content_type,
        }
    };
    reporter
        .logical_size(artifact.logical_size, cancellation)
        .await?;
    decompress_import_with_context(
        state,
        job,
        artifact.clone(),
        claim,
        cancellation,
        reporter,
        crate::zip::extract::MAX_DECOMPRESSED_ARCHIVE_BYTES,
        coordinator.execution_observer(),
    )
    .await?;
    Ok(artifact)
}

fn bound_content_type(
    job: &import_job::Model,
    batch: Option<&crate::store::entities::zip_batch::Model>,
    proof: &crate::store::pinning::publication::LegacyZipInputProof,
) -> Result<Option<String>, ImportExecutionError> {
    let invalid = || {
        terminal_failure(
            ImportFailureCode::PublicationFailed,
            "bound input metadata needs_attention",
        )
    };
    let options = batch
        .map(|b| serde_json::from_str::<serde_json::Value>(&b.captured_options))
        .transpose()
        .map_err(|_| invalid())?;
    let Some(value) = options
        .as_ref()
        .and_then(|o| o.get("legacy_input_metadata"))
    else {
        // Historical proofs have no source-header capture. Never fetch headers
        // again; only the original explicit object override is available.
        return Ok(job.object_content_type.clone());
    };
    if value.as_object().map(serde_json::Map::len) != Some(6)
        || value
            .get("schema_version")
            .and_then(serde_json::Value::as_u64)
            != Some(1)
        || value.get("kind").and_then(serde_json::Value::as_str) != Some("verified_input")
        || value.get("cid").and_then(serde_json::Value::as_str) != Some(proof.cid())
        || value.get("sha256").and_then(serde_json::Value::as_str) != Some(proof.sha256())
        || value.get("size").and_then(serde_json::Value::as_i64) != Some(proof.size())
    {
        return Err(invalid());
    }
    match value.get("content_type") {
        Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) => Ok(Some(value.clone())),
        _ => Err(invalid()),
    }
}

fn source_error(error: V2SourceError, cancellation: &JobCancellation) -> ImportExecutionError {
    if let Err(error) = ensure_active(cancellation) {
        return error;
    }
    let code = match &error {
        V2SourceError::SourceRedirected => ImportFailureCode::SourceRedirected,
        V2SourceError::SourceHttpError => ImportFailureCode::SourceHttpError,
        V2SourceError::SourceTooLarge => ImportFailureCode::SourceTooLarge,
        V2SourceError::SourceStalled => ImportFailureCode::SourceStalled,
        V2SourceError::CidUnavailable => ImportFailureCode::CidNotFound,
        V2SourceError::DeadlineExceeded => ImportFailureCode::JobDeadlineExceeded,
        _ => ImportFailureCode::SourceUnreachable,
    };
    let failure = ImportFailure {
        code,
        message: error.code().to_owned(),
        retryable: error.retryable(),
    };
    if failure.retryable {
        ImportExecutionError::Retryable(failure)
    } else {
        ImportExecutionError::Terminal(failure)
    }
}

pub(crate) fn bound_error(
    error: BoundCatError,
    cancellation: &JobCancellation,
) -> ImportExecutionError {
    if let Err(error) = ensure_active(cancellation) {
        return error;
    }
    match error {
        BoundCatError::Unavailable => terminal_failure(
            ImportFailureCode::PublicationFailed,
            "bound_artifact_unavailable",
        ),
        BoundCatError::Mismatch => terminal_failure(
            ImportFailureCode::PublicationFailed,
            "bound_artifact_mismatch",
        ),
        BoundCatError::Canceled => ImportExecutionError::Interrupted,
        BoundCatError::Transport => ImportExecutionError::Retryable(ImportFailure {
            code: ImportFailureCode::CidNotFile,
            message: "bound_artifact_transport".into(),
            retryable: true,
        }),
    }
}
