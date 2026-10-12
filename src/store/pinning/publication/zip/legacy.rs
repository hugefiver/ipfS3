use super::super::{
    DecidedSnapshot, PublicationObject, PublicationRequest, ZipBatchPublication, ZipCompletedReplay,
};
use super::{CapturedZipPublication, ZipPublicationAuthority, capture::read_capture, invalid};
use crate::{
    error::{AppError, AppResult},
    lifecycle::model::MultipartUploadTargetIdentity,
    store::{
        entities::{import_job, import_job_result, multipart_upload, zip_batch},
        import::ownership::{self, ImportPublicationGuard, StandardMutationGuard},
        multipart::AbortExactIncompleteUploadResult,
        object_version::PublicationResult,
    },
    zip::batch::{FinalZipManifest, ManifestFile},
};
use chrono::{DateTime, Utc};
use sea_orm::{DatabaseTransaction, EntityTrait, Set};

mod replay;

#[derive(Debug, Clone)]
pub struct LegacyZipAuthority {
    pub standard_guard: Option<StandardMutationGuard>,
    pub upload_target: Option<MultipartUploadTargetIdentity>,
    pub import_guard: Option<ImportPublicationGuard>,
    pub result_rows: Vec<import_job_result::ActiveModel>,
    pub replay: Option<ZipCompletedReplay>,
}

pub(super) async fn verify_ownership(
    tx: &DatabaseTransaction,
    request: &CapturedZipPublication,
    authority: &LegacyZipAuthority,
) -> AppResult<()> {
    let source = request.source.as_ref().ok_or_else(invalid)?;
    if authority.standard_guard.is_none() && authority.import_guard.is_none() {
        return Err(super::super::invalid_publication(
            "missing ZIP publication ownership guard; needs_attention",
        ));
    }
    if (authority.standard_guard.is_some() && authority.import_guard.is_some())
        || (authority.upload_target.is_some() && authority.import_guard.is_some())
        || (authority.upload_target.is_some() != authority.replay.is_some())
    {
        return Err(invalid());
    }
    if let Some(guard) = &authority.standard_guard {
        ownership::verify_standard_mutation_guard(
            tx,
            guard,
            &source.bucket,
            &source.key,
            &request
                .successes
                .iter()
                .map(|s| s.key.clone())
                .collect::<Vec<_>>(),
        )
        .await?;
        if authority.upload_target.is_none()
            && request.capture.origin().request_id != guard.mutation_id
        {
            return Err(invalid());
        }
    }
    if let Some(target) = &authority.upload_target {
        if target.bucket != source.bucket || target.key != source.key {
            return Err(AppError::NoSuchUpload(target.upload_id.clone()));
        }
        if request.capture.origin().request_id != target.upload_id {
            return Err(invalid());
        }
        let upload = multipart_upload::Entity::find_by_id(&target.upload_id)
            .one(tx)
            .await?
            .ok_or_else(|| AppError::NoSuchUpload(target.upload_id.clone()))?;
        if upload.bucket != target.bucket
            || upload.key != target.key
            || upload.created_at != target.initiated_at
        {
            return Err(AppError::NoSuchUpload(target.upload_id.clone()));
        }
        if let Some(json) = upload.zip_intent_json {
            if read_capture(&json)? != request.capture {
                return Err(invalid());
            }
        } else if serde_json::to_value(&request.capture)
            .map_err(|_| invalid())?
            .get("semantics")
            .is_some_and(|v| v == "current")
        {
            return Err(invalid());
        }
    }
    if let Some(guard) = &authority.import_guard {
        if request.capture.origin().request_id != guard.job_id {
            return Err(invalid());
        }
        let locations = request
            .source
            .iter()
            .chain(&request.successes)
            .map(|o| (o.bucket.clone(), o.key.clone()))
            .collect::<Vec<_>>();
        ownership::verify_publication_guard(tx, guard, &source.bucket, &locations).await?;
        let job = import_job::Entity::find_by_id(&guard.job_id)
            .one(tx)
            .await?
            .ok_or_else(invalid)?;
        if let Some(json) = job.zip_intent_json {
            if read_capture(&json)? != request.capture {
                return Err(invalid());
            }
        } else if serde_json::to_value(&request.capture)
            .map_err(|_| invalid())?
            .get("semantics")
            .is_some_and(|v| v == "current")
        {
            return Err(invalid());
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(in crate::store::pinning::publication) async fn publish_legacy_in_transaction(
    tx: &DatabaseTransaction,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    batch: &ZipBatchPublication,
    upload_target: Option<&MultipartUploadTargetIdentity>,
    standard_guard: Option<&StandardMutationGuard>,
    import_guard: Option<&ImportPublicationGuard>,
    result_rows: Vec<import_job_result::ActiveModel>,
    decided: Option<&DecidedSnapshot>,
    replay: Option<&ZipCompletedReplay>,
) -> AppResult<PublicationResult> {
    // Read only after acquiring bucket, even for this compatibility adapter.
    ownership::lock_bucket_for_ownership(tx, &request.object.bucket).await?;
    let row = zip_batch::Entity::find_by_id(&batch.batch_id)
        .one(tx)
        .await?
        .ok_or_else(invalid)?;
    let capture = read_capture(&row.captured_options)?;
    if decided.is_some_and(|s| s.decision != *capture.source_decision()) {
        return Err(invalid());
    }
    capture
        .source_decision()
        .validate_policy(&request.policy)
        .map_err(|_| invalid())?;
    let by_id = entries
        .iter()
        .map(|o| (&o.id, o))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut successful = Vec::new();
    for binding in &batch.entries {
        let object = by_id.get(&binding.object_id).ok_or_else(invalid)?;
        successful.push(ManifestFile {
            relative_path: binding.path.clone(),
            object_key: object.key.clone(),
            cid: object.cid.clone(),
            size: object.logical_size,
        });
    }
    let source_id = request.object.id.clone();
    let results = super::publish_in_transaction(
        tx,
        &CapturedZipPublication {
            batch_id: batch.batch_id.clone(),
            source: Some(request.object),
            source_tags: request.tags,
            successes: entries,
            manifest: FinalZipManifest {
                successful,
                ..Default::default()
            },
            capture,
            authority: ZipPublicationAuthority::Legacy(Box::new(LegacyZipAuthority {
                standard_guard: standard_guard.cloned(),
                upload_target: upload_target.cloned(),
                import_guard: import_guard.cloned(),
                result_rows,
                replay: replay.cloned(),
            })),
            root_outcome: batch.root_outcome.clone(),
            terminal_result: batch.terminal_result.clone(),
        },
    )
    .await?;
    results
        .into_iter()
        .find(|(_, r)| r.object_id == source_id)
        .map(|(_, r)| r)
        .ok_or_else(invalid)
}

pub(super) async fn settle(
    tx: &DatabaseTransaction,
    request: &CapturedZipPublication,
    authority: &LegacyZipAuthority,
    results: &[(String, PublicationResult)],
    now: DateTime<Utc>,
) -> AppResult<()> {
    let source = request.source.as_ref().ok_or_else(invalid)?;
    if let Some(target) = &authority.upload_target {
        match crate::store::multipart::abort_exact_incomplete_upload_in_transaction(tx, target)
            .await?
        {
            AbortExactIncompleteUploadResult::Applied => {}
            _ => return Err(AppError::NoSuchUpload(target.upload_id.clone())),
        }
    }
    if let Some(replay) = &authority.replay {
        let result = results
            .iter()
            .find(|(_, r)| r.object_id == source.id)
            .ok_or_else(invalid)?;
        replay::seal(
            tx,
            request,
            authority.upload_target.as_ref().ok_or_else(invalid)?,
            &result.1,
            replay,
        )
        .await?;
    }
    if let Some(guard) = &authority.standard_guard {
        ownership::complete_standard_mutation_in_transaction(tx, guard, now).await?;
    }
    if let Some(guard) = &authority.import_guard {
        let mut rows = authority.result_rows.clone();
        for row in &mut rows {
            row.job_id = Set(guard.job_id.clone());
        }
        if !rows.is_empty() {
            import_job_result::Entity::insert_many(rows)
                .exec(tx)
                .await?;
        }
        #[cfg(test)]
        super::super::pause_before_import_completion_for_test(&guard.job_id).await;
        ownership::complete_publication_in_transaction(
            tx,
            guard,
            &source.cid,
            source.logical_size,
            now,
        )
        .await?;
    }
    Ok(())
}
