use std::time::Duration;

use chrono::Utc;
use sea_orm::EntityTrait;

use crate::{
    error::AppError,
    import::{
        ImportClaim, ImportExecutionError, ImportFailureCode,
        execution_error::{ensure_active, map_publication_error, terminal_failure},
        pipeline::{ImportArtifact, JobCancellation},
    },
    state::AppState,
    store::{
        entities::{import_job, import_job_target},
        import::{
            ownership::{ExpectedImportTarget, ImportPublicationGuard},
            results::{self, ZipResultRecord},
        },
        pinning::publication::{
            CapturedZipPublication, LegacyZipAuthority, PublicationObject, PublicationResult,
            ZipPublicationAuthority, publish_captured_zip,
        },
    },
    zip::{batch::FinalZipManifest, response::ExtractedEntry},
};

use crate::store::zip::{RootClaim, RootOutcome};
mod prepared;

// An initial failure has no root epoch. Once claim_root succeeds, even a
// failed directory build must carry its epoch into the publication fence.
enum RootResult {
    Unclaimed(RootOutcome),
    ClaimedFailure {
        claim: RootClaim,
        code: &'static str,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct ZipPublicationEntry {
    pub entry: ExtractedEntry,
    pub target: ExpectedImportTarget,
}

pub(crate) struct ZipPublicationData<'a> {
    pub entries: &'a [ZipPublicationEntry],
    pub records: &'a [ZipResultRecord],
    pub manifest: Option<&'a FinalZipManifest>,
}

pub(crate) async fn publish_zip_import(
    state: &AppState,
    job: &import_job::Model,
    claim: &ImportClaim,
    artifact: &ImportArtifact,
    data: ZipPublicationData<'_>,
    guard: ImportPublicationGuard,
    cancellation: &JobCancellation,
) -> Result<PublicationResult, ImportExecutionError> {
    let ZipPublicationData {
        entries,
        records,
        manifest,
    } = data;
    ensure_active(cancellation)?;
    let capture = crate::import::zip_capture::legacy_capture(job)?;
    let metadata: serde_json::Value = serde_json::from_str(&job.metadata_json).map_err(|_| {
        terminal_failure(
            ImportFailureCode::PublicationFailed,
            "persisted import metadata is invalid",
        )
    })?;
    let archive_size = i64::try_from(artifact.logical_size).map_err(|_| {
        terminal_failure(
            ImportFailureCode::PublicationFailed,
            "import archive size is outside the supported range",
        )
    })?;
    let now = Utc::now();
    let archive_object = PublicationObject::from_put(
        uuid::Uuid::new_v4().to_string(),
        &job.bucket,
        &job.key,
        artifact.cid.clone(),
        archive_size,
        artifact.object_content_type.clone(),
        Some(metadata),
        false,
        None,
        None,
        now,
    );
    let successes = entries
        .iter()
        .map(|entry| {
            PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                &job.bucket,
                &entry.entry.key,
                entry.entry.cid.clone(),
                entry.entry.size,
                None,
                None,
                false,
                None,
                None,
                now,
            )
        })
        .collect::<Vec<_>>();
    let result_rows =
        results::zip_publication_rows(&job.id, &job.key, &artifact.cid, archive_size, records)
            .map_err(|error| map_publication_error(error, cancellation))?;
    let manifest = manifest.ok_or_else(|| {
        terminal_failure(
            ImportFailureCode::PublicationFailed,
            "ZIP manifest needs_attention",
        )
    })?;
    let root_result = prepare_and_build_root(
        state,
        job,
        claim,
        manifest,
        capture.controls().root,
        cancellation,
    )
    .await?;
    let root_outcome = match root_result {
        RootResult::Unclaimed(outcome) => outcome,
        RootResult::ClaimedFailure { claim, code } => RootOutcome::ClaimedFailed { claim, code },
    };
    let source_tags = serde_json::from_str(&job.tags_json).map_err(|_| {
        terminal_failure(
            ImportFailureCode::PublicationFailed,
            "persisted ZIP tags are invalid",
        )
    })?;
    let root_enabled = capture.controls().root.enabled();
    let published = publish_captured_zip(state.store.db(), CapturedZipPublication {
        batch_id: job.id.clone(), source: Some(archive_object), source_tags, successes,
        manifest: manifest.clone(), capture,
        authority: ZipPublicationAuthority::Legacy(Box::new(LegacyZipAuthority {
            standard_guard: None, upload_target: None, import_guard: Some(guard), result_rows, replay: None,
        })), root_outcome,
        terminal_result: serde_json::json!({"batch_id": job.id, "archive_cid": artifact.cid, "root_enabled": root_enabled}).to_string(),
    }).await.map_err(|error| map_publication_error(error, cancellation))?;
    published
        .into_iter()
        .find(|(key, _)| key == &job.key)
        .map(|(_, result)| result)
        .ok_or_else(|| {
            terminal_failure(
                ImportFailureCode::PublicationFailed,
                "missing archive publication receipt",
            )
        })
}

async fn publication_guard(
    state: &AppState,
    job: &import_job::Model,
    claim: &ImportClaim,
    entries: &[ZipPublicationEntry],
    cancellation: &JobCancellation,
) -> Result<ImportPublicationGuard, ImportExecutionError> {
    let archive_target = import_job_target::Entity::find_by_id((
        job.id.clone(),
        job.bucket.clone(),
        job.key.clone(),
    ))
    .one(state.store.db())
    .await
    .map_err(AppError::from)
    .map_err(|error| map_publication_error(error, cancellation))?
    .filter(|target| target.kind == "archive")
    .ok_or(ImportExecutionError::Superseded)?;

    let mut targets = Vec::with_capacity(entries.len().saturating_add(1));
    targets.push(ExpectedImportTarget {
        bucket: archive_target.bucket,
        key: archive_target.key,
        generation: archive_target.expected_generation,
    });
    targets.extend(entries.iter().map(|entry| entry.target.clone()));
    targets.sort_by(|left, right| {
        left.bucket
            .cmp(&right.bucket)
            .then_with(|| left.key.cmp(&right.key))
    });
    Ok(ImportPublicationGuard {
        job_id: claim.job_id.clone(),
        worker_id: claim.worker_id.clone(),
        claim_epoch: claim.claim_epoch,
        targets,
    })
}

async fn freeze_with_guard(
    state: &AppState,
    job: &import_job::Model,
    artifact: &ImportArtifact,
    data: &ZipPublicationData<'_>,
    guard: &ImportPublicationGuard,
    cancellation: &JobCancellation,
) -> Result<(), ImportExecutionError> {
    ensure_active(cancellation)?;
    let proof =
        crate::store::pinning::publication::read_legacy_import_input(state.store.db(), &job.id)
            .await
            .map_err(|error| map_publication_error(error, cancellation))?
            .ok_or_else(|| {
                terminal_failure(
                    ImportFailureCode::PublicationFailed,
                    "ZIP input proof needs_attention",
                )
            })?;
    if proof.cid() != artifact.cid || proof.size() as u64 != artifact.logical_size {
        return Err(terminal_failure(
            ImportFailureCode::PublicationFailed,
            "ZIP artifact differs from bound input",
        ));
    }
    let json = prepared::PreparedImport::capture(artifact, guard, data)?.json()?;
    let manifest = data.manifest.expect("prepared capture requires manifest");
    let locations = std::iter::once((job.bucket.clone(), job.key.clone()))
        .chain(
            data.entries
                .iter()
                .map(|e| (job.bucket.clone(), e.entry.key.clone())),
        )
        .collect::<Vec<_>>();
    crate::store::zip::import_intake::freeze_legacy_manifest(
        state.store.db(),
        guard,
        &job.bucket,
        &locations,
        &manifest.manifest_items(),
        &json,
    )
    .await
    .map_err(|error| map_publication_error(error, cancellation))?;
    ensure_active(cancellation)
}

pub(crate) async fn freeze_zip_import(
    state: &AppState,
    job: &import_job::Model,
    claim: &ImportClaim,
    artifact: &ImportArtifact,
    data: ZipPublicationData<'_>,
    cancellation: &JobCancellation,
) -> Result<ImportPublicationGuard, ImportExecutionError> {
    let guard = publication_guard(state, job, claim, data.entries, cancellation).await?;
    freeze_with_guard(state, job, artifact, &data, &guard, cancellation).await?;
    Ok(guard)
}

pub(crate) async fn recover_prepared_zip_import(
    state: &AppState,
    job: &import_job::Model,
    claim: &ImportClaim,
    cancellation: &JobCancellation,
    reporter: &crate::import::progress::ProgressReporter,
    observer: &dyn crate::import::pipeline::ImportExecutionObserver,
) -> Result<Option<ImportArtifact>, ImportExecutionError> {
    let batch = crate::store::zip::read(state.store.db(), &job.id)
        .await
        .map_err(|error| map_publication_error(error, cancellation))?;
    let Some(batch) = batch.filter(|b| b.manifest_prepared) else {
        return Ok(None);
    };
    let missing = || {
        terminal_failure(
            ImportFailureCode::PublicationFailed,
            "prepared ZIP response needs_attention",
        )
    };
    let options: serde_json::Value =
        serde_json::from_str(&batch.captured_options).map_err(|_| missing())?;
    let json = options
        .get("legacy_prepared_import")
        .ok_or_else(missing)?
        .to_string();
    let recovered = prepared::PreparedImport::decode(&json)?.restore(job, claim)?;
    let data = ZipPublicationData {
        entries: &recovered.entries,
        records: &recovered.records,
        manifest: Some(&recovered.manifest),
    };
    freeze_with_guard(
        state,
        job,
        &recovered.artifact,
        &data,
        &recovered.guard,
        cancellation,
    )
    .await?;
    tokio::select! {
        biased;
        _ = cancellation.shutdown.cancelled() => return Err(ImportExecutionError::Interrupted),
        _ = cancellation.ownership_lost.cancelled() => return Err(ImportExecutionError::Superseded),
        _ = observer.before_publication(&job.id) => {}
    }
    reporter
        .phase(crate::import::ImportPhase::Publishing, cancellation)
        .await?;
    publish_zip_import(
        state,
        job,
        claim,
        &recovered.artifact,
        data,
        recovered.guard,
        cancellation,
    )
    .await?;
    Ok(Some(recovered.artifact))
}

#[allow(clippy::too_many_arguments)]
async fn prepare_and_build_root(
    state: &AppState,
    job: &import_job::Model,
    claim: &ImportClaim,
    manifest: &FinalZipManifest,
    capture: crate::pinning::tags::ZipRootCapture,
    cancellation: &JobCancellation,
) -> Result<RootResult, ImportExecutionError> {
    ensure_active(cancellation)?;
    let db = state.store.db();
    let admitted = crate::store::zip::read(db, &job.id)
        .await
        .map_err(|error| map_publication_error(error, cancellation))?
        .ok_or(ImportExecutionError::Superseded)?;
    if admitted.state != "open" || !admitted.manifest_prepared {
        return Err(ImportExecutionError::Superseded);
    }
    let items = manifest.manifest_items();
    let snapshot = crate::store::zip::snapshot(db, &job.id)
        .await
        .map_err(|error| map_publication_error(error, cancellation))?
        .ok_or(ImportExecutionError::Superseded)?;
    if !results::matches_zip_manifest(&snapshot.entries, &items) {
        return Err(terminal_failure(
            ImportFailureCode::PublicationFailed,
            "ZIP import manifest changed on worker recovery",
        ));
    }
    ensure_active(cancellation)?;
    if !capture.enabled() {
        return Ok(RootResult::Unclaimed(RootOutcome::Disabled));
    }
    if manifest.successful.is_empty() {
        return Ok(RootResult::Unclaimed(RootOutcome::Empty));
    }
    if let Some(code) = manifest.root_error {
        return Ok(RootResult::Unclaimed(RootOutcome::Failed { code }));
    }

    // A crash after receipt verification but before the atomic publication
    // must not rebuild the same root just because the import claim changed.
    // The batch's current, unexpired root epoch remains the durable authority;
    // the final import publication still fences the *new* import claim.
    if admitted.root_revision > 0 {
        let snapshot = crate::store::zip::snapshot(db, &job.id)
            .await
            .map_err(|error| map_publication_error(error, cancellation))?
            .ok_or(ImportExecutionError::Superseded)?;
        if snapshot.batch.state == "open"
            && let Some(build) = snapshot.builds.iter().find(|build| {
                build.revision == snapshot.batch.root_revision
                    && build.epoch == snapshot.batch.root_epoch
                    && build.status == "verified"
            })
            && let Some(reference) = snapshot.references.iter().find(|reference| {
                reference.revision == build.revision
                    && reference.epoch == build.epoch
                    && reference.state == "retained"
                    && reference.verification_receipt.is_some()
            })
        {
            let root_claim = crate::store::zip::RootClaim {
                batch_id: job.id.clone(),
                revision: build.revision,
                epoch: build.epoch,
                worker: build.worker.clone(),
            };
            match crate::store::zip::renew_claim(db, &root_claim, 45).await {
                Ok(()) => {
                    return Ok(RootResult::Unclaimed(RootOutcome::Verified {
                        claim: root_claim,
                        node_identity: reference.node_identity.clone(),
                        tier: reference.tier.clone(),
                        cid: reference.cid.clone(),
                    }));
                }
                Err(AppError::Internal(message))
                    if message == "stale ZIP batch ownership or root claim" => {}
                Err(error) => return Err(map_publication_error(error, cancellation)),
            }
        }
    }

    // Root intent is durable before Kubo I/O. A superseded epoch can retain a
    // candidate but cannot verify it or adopt it in the import publication.
    let worker = format!("{}:{}", claim.worker_id, claim.claim_epoch);
    let root_claim = loop {
        ensure_active(cancellation)?;
        match crate::store::zip::claim_root(db, &job.id, &worker, 45).await {
            Ok(root_claim) => break root_claim,
            Err(crate::error::AppError::Internal(message))
                if message == "stale ZIP batch ownership or root claim" =>
            {
                let snapshot = crate::store::zip::read(db, &job.id)
                    .await
                    .map_err(|error| map_publication_error(error, cancellation))?
                    .ok_or(ImportExecutionError::Superseded)?;
                if snapshot.state != "open" {
                    return Err(ImportExecutionError::Superseded);
                }
                tokio::select! {
                    biased;
                    _ = cancellation.shutdown.cancelled() => return Err(ImportExecutionError::Interrupted),
                    _ = cancellation.ownership_lost.cancelled() => return Err(ImportExecutionError::Superseded),
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {},
                }
            }
            Err(error) => return Err(map_publication_error(error, cancellation)),
        }
    };
    crate::store::zip::mark_invoked(db, &root_claim)
        .await
        .map_err(|error| map_publication_error(error, cancellation))?;
    if let Err(interruption) = ensure_active(cancellation) {
        let _ = crate::store::zip::mark_unknown(db, &root_claim).await;
        return Err(interruption);
    }
    let root_cancel = tokio_util::sync::CancellationToken::new();
    let files = manifest.directory_files();
    let build = crate::kubo::directory::build_directory(&state.kubo, &files, &root_cancel);
    tokio::pin!(build);
    let mut renewal = tokio::time::interval(Duration::from_secs(10));
    renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    renewal.tick().await;
    let (built, interruption) = loop {
        tokio::select! {
            biased;
            _ = cancellation.shutdown.cancelled() => {
                root_cancel.cancel();
                break (build.await, Some(ImportExecutionError::Interrupted));
            }
            _ = cancellation.ownership_lost.cancelled() => {
                root_cancel.cancel();
                break (build.await, Some(ImportExecutionError::Superseded));
            }
            _ = renewal.tick() => {
                if let Err(error) = crate::store::zip::renew_claim(db, &root_claim, 45).await {
                    root_cancel.cancel();
                    let built = build.await;
                    retain_build_candidate(db, &root_claim, &built)
                        .await
                        .map_err(|failure| map_publication_error(failure, cancellation))?;
                    let _ = crate::store::zip::mark_unknown(db, &root_claim).await;
                    return Err(map_publication_error(error, cancellation));
                }
            }
            result = &mut build => break (result, None),
        }
    };
    retain_build_candidate(db, &root_claim, &built)
        .await
        .map_err(|error| map_publication_error(error, cancellation))?;
    if let Some(interruption) = interruption {
        let _ = crate::store::zip::mark_unknown(db, &root_claim).await;
        return Err(interruption);
    }
    let root = match built {
        Ok(Some(root)) => root,
        Ok(None) => {
            return Ok(RootResult::ClaimedFailure {
                claim: root_claim,
                code: "invalid_manifest",
            });
        }
        Err(error) => {
            let code = match error.reason() {
                crate::kubo::directory::DirectoryBuildError::PathConflict => "path_conflict",
                crate::kubo::directory::DirectoryBuildError::InvalidManifest => "invalid_manifest",
                crate::kubo::directory::DirectoryBuildError::BlockTooLarge => {
                    "directory_block_too_large"
                }
                crate::kubo::directory::DirectoryBuildError::HashCollision => {
                    "directory_hash_collision"
                }
                _ => "directory_build_failed",
            };
            return Ok(RootResult::ClaimedFailure {
                claim: root_claim,
                code,
            });
        }
    };
    if let Err(interruption) = ensure_active(cancellation) {
        let _ = crate::store::zip::mark_unknown(db, &root_claim).await;
        return Err(interruption);
    }
    let node = root.local_residency.node_identity;
    let cid = root.cid;
    let receipt = serde_json::json!({"node_identity": node, "cid": cid}).to_string();
    if let Err(error) =
        crate::store::zip::verify_root(db, &root_claim, &node, "hot", &cid, &receipt).await
    {
        let _ = crate::store::zip::mark_unknown(db, &root_claim).await;
        return Err(map_publication_error(error, cancellation));
    }
    ensure_active(cancellation)?;
    Ok(RootResult::Unclaimed(RootOutcome::Verified {
        claim: root_claim,
        node_identity: node,
        tier: "hot".into(),
        cid,
    }))
}

async fn retain_build_candidate(
    db: &sea_orm::DatabaseConnection,
    claim: &RootClaim,
    built: &Result<
        Option<crate::kubo::directory::DirectoryRoot>,
        crate::kubo::directory::DirectoryBuildError,
    >,
) -> Result<(), AppError> {
    let candidate = match built {
        Ok(Some(root)) => Some((
            root.local_residency.node_identity.as_str(),
            root.cid.as_str(),
        )),
        Err(error) => error
            .candidate()
            .map(|candidate| (candidate.node_identity.as_str(), candidate.cid.as_str())),
        Ok(None) => None,
    };
    if let Some((node, cid)) = candidate {
        crate::store::zip::retain_candidate(db, claim, node, "hot", cid).await?;
    }
    Ok(())
}
