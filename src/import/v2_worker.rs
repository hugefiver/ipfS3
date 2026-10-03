//! ZIP v2 import worker. The signed intake is durable before any source
//! I/O; every subsequent effect is owned by the same execution epoch.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use futures_util::{StreamExt, stream::BoxStream};
use sea_orm::{DatabaseConnection, TransactionTrait};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    error::{AppError, AppResult},
    import::{
        pipeline::ImportCoordinator,
        v2_source::{V2SourceError, fetch_v2_zip_source},
    },
    kubo::directory::{
        DirectoryBuildError, DirectoryCandidate, DirectoryFile, build_directory_capturing_candidate,
    },
    pinning::{policy::PublicationPolicy, zip_policy::ZipPublishedOutput},
    state::AppState,
    store::{
        import::ownership,
        pinning::publication::{
            PublicationObject, ZipV2Publication, ZipV2PublicationResult, ZipV2Success,
        },
        zip::{self, execution, import_intake},
    },
    zip::{
        batch::final_zip_manifest,
        extract::{
            ExtractionObserver, ObservedExtractionError, extract_zip_stream_observed_with_limits,
        },
        options::{ZipTargets, ZipV2Options},
        response::{ExtractFailure, ExtractedEntry},
        sanitize::normalize_target_prefix,
    },
};

const PAGE: u64 = 32;
const MAX_PAGES_PER_POLL: usize = 4;
const CANDIDATE_RETENTION_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) async fn execute_with_heartbeat(
    coordinator: Arc<ImportCoordinator>,
    state: Arc<AppState>,
    claim: execution::Claim,
    shutdown: CancellationToken,
    lease_seconds: i64,
) {
    let cancel = shutdown.child_token();
    let root_claim = tokio::sync::Mutex::new(None);
    let mut candidate = None;
    let root_lease = root_lease_seconds(&coordinator);
    let period =
        Duration::from_millis(((lease_seconds.min(root_lease) as u64) * 1_000 / 3).max(100));
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    // These slots outlive work. Cancellation still drops all business I/O;
    // afterward only claim-fenced candidate retention is allowed to finish.
    {
        let work = execute_claim(
            &coordinator,
            &state,
            &claim,
            &cancel,
            &root_claim,
            &mut candidate,
        );
        tokio::pin!(work);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => { cancel.cancel(); break; }
                _ = interval.tick() => {
                     let renewed = import_intake::renew(state.store.db(), &claim, lease_seconds).await;
                    if !matches!(renewed, Ok(true)) {
                        cancel.cancel();
                        break;
                    }
                    if let Some(root) = root_claim.lock().await.clone()
                        && zip::renew_claim(state.store.db(), &root, root_lease).await.is_err() {
                            cancel.cancel();
                            break;
                        }
                }
                _ = &mut work => break,
            }
        }
    }
    retain_pending_candidate(state.store.db(), &root_claim, &mut candidate).await;
}

async fn retain_pending_candidate(
    db: &DatabaseConnection,
    current: &tokio::sync::Mutex<Option<zip::RootClaim>>,
    candidate: &mut Option<DirectoryCandidate>,
) {
    let Some(known) = candidate.as_ref() else {
        return;
    };
    let retention = async {
        let root = current
            .lock()
            .await
            .clone()
            .ok_or(AppError::StaleContentMutation)?;
        zip::retain_candidate(db, &root, &known.node_identity, "hot", &known.cid).await
    };
    if matches!(
        tokio::time::timeout(CANDIDATE_RETENTION_TIMEOUT, retention).await,
        Ok(Ok(()))
    ) {
        // Keep the slot borrowed until commit: shutdown/renewal loss can drop
        // this timeout cleanup too, and the heartbeat must still see evidence.
        *candidate = None;
    } else {
        tracing::warn!(
            "ZIP import candidate retention failed or timed out; original root intent remains recoverable"
        );
    }
}

fn root_lease_seconds(coordinator: &ImportCoordinator) -> i64 {
    ((coordinator.config().raw.job_timeout_secs / 3) as i64).clamp(1, 60)
}

/// Keyset cursor is per process, not a claim. A missed or contended ID is
/// rediscovered after wraparound; only the DB-clock due filter can start work.
pub(super) async fn claim_due(
    db: &DatabaseConnection,
    worker: &str,
    lease_seconds: i64,
    capacity: usize,
    after: &mut String,
) -> AppResult<Vec<execution::Claim>> {
    let mut claimed = Vec::new();
    for _ in 0..MAX_PAGES_PER_POLL {
        let page = import_intake::pending_ids(db, after, PAGE).await?;
        if page.is_empty() {
            after.clear();
            break;
        }
        for id in &page {
            *after = id.clone();
            if let Some(claim) = execution::claim(db, id, worker, lease_seconds).await? {
                claimed.push(claim);
                if claimed.len() >= capacity {
                    return Ok(claimed);
                }
            }
        }
        if page.len() < PAGE as usize {
            after.clear();
            break;
        }
    }
    Ok(claimed)
}

#[derive(Debug)]
enum RunError {
    Lost,
    Retry,
    InvalidZip,
    DigestMismatch(String),
    Fenced,
}

/// The caller joins this future with its heartbeat and shutdown token. No
/// background child outlives the shared import worker.
pub(super) async fn execute_claim(
    coordinator: &ImportCoordinator,
    state: &Arc<AppState>,
    claim: &execution::Claim,
    cancel: &CancellationToken,
    root_claim: &tokio::sync::Mutex<Option<zip::RootClaim>>,
    candidate: &mut Option<DirectoryCandidate>,
) {
    // A peer can keep a stream non-idle forever with trickle bytes. Bound the
    // entire attempt, not only each network idle interval; timeout remains a
    // retryable DB-clock event and is eventually fenced by the epoch budget.
    let outcome = tokio::time::timeout(
        Duration::from_secs(coordinator.config().raw.job_timeout_secs),
        execute_inner(coordinator, state, claim, cancel, root_claim, candidate),
    )
    .await
    .unwrap_or(Err(RunError::Retry));
    // timeout has already dropped execute_inner: never resume publication,
    // verification or root RPCs while preserving its pending candidate.
    retain_pending_candidate(state.store.db(), root_claim, candidate).await;
    if cancel.is_cancelled() {
        return;
    }
    match outcome {
        Ok(()) | Err(RunError::Lost) => {}
        Err(RunError::Fenced) => {
            let _ = execution::fence(state.store.db(), claim).await;
        }
        Err(RunError::DigestMismatch(measured)) => {
            // The measured digest came from this first, complete source read,
            // not a second URL GET or a CID-derived guess. Never log it.
            let _ = import_intake::fail_if_owned(
                state.store.db(),
                claim,
                import_intake::ImportFailure::ExpectedSha256Mismatch {
                    measured_sha256: &measured,
                },
            )
            .await;
        }
        Err(RunError::InvalidZip) => {
            let _ = import_intake::fail_if_owned(
                state.store.db(),
                claim,
                import_intake::ImportFailure::InvalidZip,
            )
            .await;
        }
        Err(RunError::Retry) => {
            // Epoch is durable across process restarts. A failed claim retains
            // its DB-clock lease; renewal sets the bounded next retry time.
            let budget = coordinator.config().raw.max_attempts as i64;
            if claim.epoch >= budget {
                let _ = execution::fence(state.store.db(), claim).await;
            } else {
                let delay = (1_i64 << (claim.epoch.max(1) - 1).min(5)).min(60);
                let _ = import_intake::renew(state.store.db(), claim, delay).await;
            }
        }
    }
}

async fn execute_inner(
    coordinator: &ImportCoordinator,
    state: &Arc<AppState>,
    claim: &execution::Claim,
    cancel: &CancellationToken,
    root_claim: &tokio::sync::Mutex<Option<zip::RootClaim>>,
    candidate: &mut Option<DirectoryCandidate>,
) -> Result<(), RunError> {
    let db = state.store.db();
    let snapshot = execution::read(db, &claim.batch_id)
        .await
        .map_err(|_| RunError::Retry)?
        .ok_or(RunError::Lost)?;
    if snapshot.epoch != claim.epoch
        || snapshot.worker.as_deref() != Some(&claim.worker)
        || snapshot.source != "import"
        || !matches!(snapshot.state.as_str(), "pending" | "admitted")
    {
        return Err(RunError::Lost);
    }
    let source = import_intake::claimed_source(db, claim)
        .await
        .map_err(|_| RunError::Retry)?
        .ok_or(RunError::Lost)?;
    let prefix = normalize_target_prefix(&source.prefix).map_err(|_| RunError::InvalidZip)?;
    if prefix != source.prefix {
        return Err(RunError::InvalidZip);
    }
    let capture: serde_json::Value =
        serde_json::from_str(&snapshot.captured_options).map_err(|_| RunError::Fenced)?;
    let options: ZipV2Options =
        serde_json::from_value(capture.get("options").cloned().ok_or(RunError::Fenced)?)
            .map_err(|_| RunError::Fenced)?;
    if (!options.publish_source && !options.publish_extracted)
        || options.result_version != 2
        || options.token != snapshot.token
        || (matches!(options.targets, ZipTargets::Source | ZipTargets::Both)
            && !options.publish_source)
        || (matches!(options.targets, ZipTargets::Extracted | ZipTargets::Both)
            && !options.publish_extracted)
    {
        return Err(RunError::Fenced);
    }
    let revision = capture
        .get("rule_revision")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    if options.targets != ZipTargets::None
        && revision.as_deref() != Some(state.pinning.zip_output_rules().revision())
    {
        return Err(RunError::Fenced);
    }
    let limits = state.pinning.zip_extraction_limits();
    if snapshot.state == "pending" {
        let (cid, digest, size) = if let (Some(cid), Some(sha), Some(size)) = (
            snapshot.input_art_cid.clone(),
            snapshot.input_sha256.clone(),
            snapshot.input_art_size,
        ) {
            (cid, sha, size)
        } else {
            let artifact = fetch_v2_zip_source(
                coordinator,
                state,
                &source.source_descriptor,
                source.expected_sha256.as_deref(),
                cancel.clone(),
                &limits,
            )
            .await
            .map_err(|error| classify_source(&error))?;
            let size = i64::try_from(artifact.size).map_err(|_| RunError::InvalidZip)?;
            import_intake::bind_verified_input(db, claim, &artifact.sha256, &artifact.cid, size)
                .await
                .map_err(|_| RunError::Retry)?;
            (artifact.cid, artifact.sha256, size)
        };
        if cancel.is_cancelled() {
            return Err(RunError::Lost);
        }
        let (entries, failures) = if options.publish_extracted {
            let outcome =
                extract_artifact(state, claim, &prefix, &cid, &digest, size, cancel).await?;
            (outcome.entries, outcome.failures)
        } else {
            // Source-only attests the complete input bytes, but never parses
            // entries, uploads children or invents an empty directory root.
            (Vec::new(), Vec::new())
        };
        let manifest = final_zip_manifest(&entries, &failures, &prefix);
        let items = manifest.manifest_items();
        let v2_items = items
            .iter()
            .map(|item| match item {
                zip::ManifestItem::Success {
                    path,
                    object_key,
                    cid,
                    size,
                } => execution::ManifestItem::Success {
                    path: path.clone(),
                    object_key: object_key.clone(),
                    cid: cid.clone(),
                    size: *size,
                },
                zip::ManifestItem::Failure { path, code } => execution::ManifestItem::Failure {
                    path: path.clone(),
                    code: code.clone(),
                },
            })
            .collect::<Vec<_>>();
        let mutation_ids = manifest
            .successful
            .iter()
            .map(|file| {
                (
                    file.object_key.clone(),
                    format!(
                        "zip-v2:{}:{}:{}",
                        claim.batch_id,
                        claim.epoch,
                        uuid::Uuid::new_v4()
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let tx = db.begin().await.map_err(|_| RunError::Retry)?;
        ownership::lock_bucket_for_ownership(&tx, &snapshot.bucket)
            .await
            .map_err(|_| RunError::Retry)?;
        let source_guard = if options.publish_source {
            Some(
                ownership::admit_zip_v2_source_in_transaction(
                    &tx,
                    &snapshot.bucket,
                    &snapshot.source_key,
                    &mutation_ids.keys().cloned().collect(),
                    &format!("zip-v2-source:{}", claim.batch_id),
                    crate::store::database_clock::database_now(&tx)
                        .await
                        .map_err(|_| RunError::Retry)?,
                )
                .await
                .map_err(|error| match error {
                    AppError::StaleContentMutation => RunError::Fenced,
                    _ => RunError::Retry,
                })?,
            )
        } else {
            None
        };
        // Keep the legacy root mirror, both immutable manifests, and every
        // source-safe output guard inside this *one* short bucket-first txn.
        let mirror = zip::BatchAdmission {
            id: snapshot.id.clone(),
            owner: snapshot.owner.clone(),
            source: snapshot.source.clone(),
            token: snapshot.token.clone(),
            // Publication verifies these mirror-only sentinel values against
            // the authoritative execution request fingerprint and bound SHA.
            fingerprint: "pending".into(),
            bucket: snapshot.bucket.clone(),
            archive_key: snapshot.source_key.clone(),
            input_identity: "pending".into(),
            captured_options: snapshot.captured_options.clone(),
        };
        zip::BatchAdmission::admit_in_transaction(&tx, &mirror)
            .await
            .map_err(|_| RunError::Retry)?;
        execution::admit_manifest_in_transaction(&tx, claim, &v2_items, &mutation_ids)
            .await
            .map_err(|error| match error {
                AppError::StaleContentMutation => RunError::Fenced,
                AppError::InvalidZipParameter(_) => RunError::InvalidZip,
                _ => RunError::Retry,
            })?;
        zip::ManifestItem::prepare_manifest_in_transaction(&tx, &claim.batch_id, &items)
            .await
            .map_err(|_| RunError::Retry)?;
        if let Some(guard) = &source_guard {
            ownership::capture_zip_v2_source_guard_in_transaction(&tx, claim, guard)
                .await
                .map_err(|_| RunError::Retry)?;
        }
        tx.commit().await.map_err(|_| RunError::Retry)?;
    }
    // The first hint predates the clean-input bind. Use the durable admitted
    // row for the measured digest and final publication identity.
    let snapshot = execution::read(db, &claim.batch_id)
        .await
        .map_err(|_| RunError::Retry)?
        .ok_or(RunError::Lost)?;
    if snapshot.state != "admitted"
        || snapshot.epoch != claim.epoch
        || snapshot.worker.as_deref() != Some(&claim.worker)
    {
        return Err(RunError::Lost);
    }
    // After admission, recovery uses only the immutable manifest and rotated
    // original guards. Never CAT, fetch, extract or re-admit a source again.
    let mirror = zip::snapshot(db, &claim.batch_id)
        .await
        .map_err(|_| RunError::Retry)?
        .ok_or(RunError::Retry)?;
    if mirror.batch.state != "open"
        || !mirror.batch.manifest_prepared
        || mirror.batch.source_published
    {
        return Err(RunError::Lost);
    }
    let source_guard =
        import_intake::source_guard(&snapshot, &mirror.batch).map_err(|_| RunError::Fenced)?;
    if options.publish_source != source_guard.is_some()
        || (!options.publish_extracted && !mirror.entries.is_empty())
    {
        return Err(RunError::Fenced);
    }
    let source_object = if options.publish_source {
        let content_type = capture
            .get("source_content_type")
            .filter(|value| !value.is_null())
            .map(|value| value.as_str().map(str::to_owned).ok_or(RunError::Fenced))
            .transpose()?;
        let metadata = capture
            .get("source_metadata")
            .filter(|value| value.as_object().is_some_and(|map| !map.is_empty()))
            .cloned();
        Some(PublicationObject::from_put(
            uuid::Uuid::new_v4().to_string(),
            &snapshot.bucket,
            &snapshot.source_key,
            snapshot.input_art_cid.clone().ok_or(RunError::Lost)?,
            snapshot.input_art_size.ok_or(RunError::Lost)?,
            content_type,
            metadata,
            false,
            None,
            None,
            chrono::Utc::now(),
        ))
    } else {
        None
    };
    let mut source_policy = if options.publish_source {
        Some(PublicationPolicy {
            tags: serde_json::from_value(
                capture
                    .get("source_tags")
                    .cloned()
                    .ok_or(RunError::Fenced)?,
            )
            .map_err(|_| RunError::Fenced)?,
            // Source tags are metadata, not authority to inherit manual intent.
            leases: Vec::new(),
        })
    } else {
        None
    };
    let mut successes = Vec::new();
    let mut files = Vec::new();
    let mut failed = 0_usize;
    let mut root_error = None;
    let paths = mirror
        .entries
        .iter()
        .filter(|item| item.object_key.is_some())
        .map(|item| item.path.as_str())
        .collect::<BTreeSet<_>>();
    for entry in &mirror.entries {
        if let (Some(key), Some(cid), Some(size)) = (&entry.object_key, &entry.cid, entry.size) {
            if key != &format!("{prefix}{}", entry.path) {
                root_error = Some("invalid_manifest");
            } else if entry
                .path
                .match_indices('/')
                .any(|(i, _)| paths.contains(&entry.path[..i]))
                && root_error.is_none()
            {
                root_error = Some("path_conflict");
            }
            files.push(DirectoryFile {
                path: entry.path.clone(),
                cid: cid.clone(),
            });
            successes.push(ZipV2Success {
                object: PublicationObject::from_put(
                    uuid::Uuid::new_v4().to_string(),
                    &snapshot.bucket,
                    key,
                    cid.clone(),
                    size,
                    None,
                    None,
                    false,
                    None,
                    None,
                    chrono::Utc::now(),
                ),
                path: entry.path.clone(),
                object_key: key.clone(),
                cid: cid.clone(),
                size,
                policy: PublicationPolicy {
                    tags: Vec::new(),
                    leases: Vec::new(),
                },
            });
        } else if entry.error_code.is_some() {
            failed += 1;
        } else {
            return Err(RunError::Lost);
        }
    }
    if options.targets != ZipTargets::None {
        let outputs = successes
            .iter()
            .map(|success| ZipPublishedOutput {
                bucket: snapshot.bucket.clone(),
                key: success.object_key.clone(),
                version_id: "not-yet-published".into(),
                cid: success.cid.clone(),
            })
            .collect::<Vec<_>>();
        let plan = state
            .pinning
            .zip_output_rules()
            .plan(
                crate::pinning::zip_policy::ZipTargets {
                    source: matches!(options.targets, ZipTargets::Source | ZipTargets::Both),
                    extracted: matches!(options.targets, ZipTargets::Extracted | ZipTargets::Both),
                },
                source_object.as_ref().map(|object| ZipPublishedOutput {
                    bucket: object.bucket.clone(),
                    key: object.key.clone(),
                    version_id: "not-yet-published".into(),
                    cid: object.cid.clone(),
                }),
                &outputs,
            )
            .map_err(|_| RunError::Fenced)?;
        for decision in plan.outputs {
            let policy = if decision.kind == crate::pinning::zip_policy::ZipOutputKind::Source {
                source_policy.as_mut().ok_or(RunError::Lost)?
            } else {
                &mut successes
                    .iter_mut()
                    .find(|output| output.object_key == decision.output.key)
                    .ok_or(RunError::Lost)?
                    .policy
            };
            policy.leases = decision
                .intents
                .into_iter()
                .map(|intent| intent.intent)
                .collect();
        }
    }
    if !import_intake::renew(db, claim, 60)
        .await
        .map_err(|_| RunError::Retry)?
    {
        return Err(RunError::Lost);
    }
    let root = if !options.build_root() {
        zip::RootOutcome::Disabled
    } else if successes.is_empty() {
        zip::RootOutcome::Empty
    } else if let Some(code) = root_error {
        zip::RootOutcome::Failed { code }
    } else {
        build_root(
            state,
            claim,
            &mirror,
            &files,
            cancel,
            root_lease_seconds(coordinator),
            RootBuildCapture {
                current: root_claim,
                candidate,
            },
        )
        .await?
    };
    if cancel.is_cancelled() {
        return Err(RunError::Lost);
    }
    tokio::select! {
        _ = cancel.cancelled() => return Err(RunError::Lost),
        _ = coordinator.execution_observer().before_publication(&claim.batch_id) => {}
    }
    let digest = snapshot.input_sha256.as_deref().ok_or(RunError::Lost)?;
    let terminal = serde_json::json!({
        "input_sha256": digest, "published_count": successes.len(), "failed_count": failed,
        "status": if successes.is_empty() && !options.publish_source { "failed" } else { "completed" },
    })
    .to_string();
    let request = ZipV2Publication {
        claim: claim.clone(),
        source: source_object,
        source_policy,
        source_guard,
        successes,
        targets: options.targets,
        captured_rule_revision: revision,
        root_outcome: root,
        terminal_result: terminal,
    };
    match import_intake::publish_import_zip_v2(
        db,
        request,
        state.pinning.zip_output_rules(),
        state.pinning.effective_config(),
        state.pinning.provider_limits(),
    )
    .await
    {
        Ok(ZipV2PublicationResult::Published(_)) => Ok(()),
        Ok(ZipV2PublicationResult::Fenced) => Err(RunError::Lost),
        Err(_) => {
            // An unknown DB commit outcome is not proof of rollback. The next
            // due claim reconciles the committed receipt or retries the exact
            // admitted manifest; never write a synthetic failure receipt.
            let status = import_intake::read_for_path(
                db,
                &claim.batch_id,
                &snapshot.owner,
                &snapshot.bucket,
                &snapshot.source_key,
            )
            .await;
            if matches!(
                status,
                Ok(Some(import_intake::Status { state: "ready", .. }))
            ) {
                Ok(())
            } else {
                let _ = execution::fence_if_lost(db, claim).await;
                Err(RunError::Retry)
            }
        }
    }
}

fn classify_source(error: &V2SourceError) -> RunError {
    if let Some(measured) = error.measured_sha256() {
        return RunError::DigestMismatch(measured.to_owned());
    }
    if error.retryable()
        || matches!(
            error,
            V2SourceError::Canceled | V2SourceError::DeadlineExceeded
        )
    {
        RunError::Retry
    } else {
        RunError::Fenced
    }
}

struct UniqueOutputs {
    seen: BTreeSet<String>,
    source_key: String,
}

#[async_trait::async_trait]
impl ExtractionObserver for UniqueOutputs {
    type Error = std::io::Error;

    async fn entry_started(&mut self, key: &str) -> Result<(), Self::Error> {
        if key == self.source_key || !self.seen.insert(key.to_owned()) {
            return Err(std::io::Error::other("duplicate ZIP output"));
        }
        Ok(())
    }
    async fn entry_finished(&mut self, _: &ExtractedEntry) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn entry_failed(&mut self, _: &str, _: &ExtractFailure) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn bytes_processed(&mut self, _: u64) -> Result<(), Self::Error> {
        Ok(())
    }
}

async fn extract_artifact(
    state: &Arc<AppState>,
    claim: &execution::Claim,
    prefix: &str,
    cid: &str,
    digest: &str,
    size: i64,
    cancel: &CancellationToken,
) -> Result<crate::zip::extract::ExtractOutcome, RunError> {
    let stream = crate::kubo::cat::stream_cat(&state.kubo, cid, None)
        .await
        .map_err(|_| RunError::Retry)?;
    let shared = Arc::new(tokio::sync::Mutex::new((
        Box::pin(stream) as BoxStream<'static, Result<bytes::Bytes, std::io::Error>>,
        Sha256::new(),
        0_u64,
    )));
    let for_extract = shared.clone();
    let input = async_stream::stream! {
        loop {
            let next = {
                let mut source = for_extract.lock().await;
                let next = source.0.next().await;
                if let Some(Ok(bytes)) = &next {
                    source.1.update(bytes);
                    source.2 += bytes.len() as u64;
                }
                next
            };
            match next { Some(frame) => yield frame, None => break }
        }
    };
    let mut observer = UniqueOutputs {
        seen: BTreeSet::new(),
        source_key: execution::read(state.store.db(), &claim.batch_id)
            .await
            .map_err(|_| RunError::Retry)?
            .ok_or(RunError::Lost)?
            .source_key,
    };
    let result = extract_zip_stream_observed_with_limits(
        state,
        prefix,
        Box::pin(input),
        state.pinning.zip_extraction_limits(),
        &mut observer,
    )
    .await
    .map_err(|error| match error {
        ObservedExtractionError::Observer(_) | ObservedExtractionError::Limit(_) => {
            RunError::InvalidZip
        }
        ObservedExtractionError::Archive(error) if error.code().as_str() == "InternalError" => {
            RunError::Retry
        }
        ObservedExtractionError::Archive(_) => RunError::InvalidZip,
    })?;
    // The streaming ZIP parser stops after the last local entry. Drain the
    // remaining CAT body to EOF, then verify the complete byte identity
    // against the first bound source read. A bytes_stream is not evidence of
    // HTTP trailer verification; that requires a frame-aware transport.
    let mut source = shared.lock().await;
    while let Some(chunk) = source.0.next().await {
        let chunk = chunk.map_err(|_| RunError::Retry)?;
        source.1.update(&chunk);
        source.2 += chunk.len() as u64;
        if source.2 > size as u64 {
            return Err(RunError::Retry);
        }
    }
    if source.2 != size as u64 || hex::encode(source.1.clone().finalize()) != digest {
        return Err(RunError::Retry);
    }
    if cancel.is_cancelled() {
        return Err(RunError::Lost);
    }
    Ok(result)
}

/// The caller-owned evidence that survives dropping the initial root build.
struct RootBuildCapture<'a> {
    current: &'a tokio::sync::Mutex<Option<zip::RootClaim>>,
    candidate: &'a mut Option<DirectoryCandidate>,
}

async fn build_root(
    state: &Arc<AppState>,
    claim: &execution::Claim,
    mirror: &zip::BatchSnapshot,
    files: &[DirectoryFile],
    cancel: &CancellationToken,
    root_lease: i64,
    capture: RootBuildCapture<'_>,
) -> Result<zip::RootOutcome, RunError> {
    let RootBuildCapture { current, candidate } = capture;
    let db = state.store.db();
    if let Some(previous) = mirror.builds.iter().find(|build| {
        build.revision == mirror.batch.root_revision && build.epoch == mirror.batch.root_epoch
    }) {
        // The previous process may have crashed after starting root I/O. Wait
        // for its DB-clock lease rather than burning every retry epoch against
        // a root claim that is not yet available. The execution heartbeat
        // remains active throughout this cancellable wait.
        let now = crate::store::database_clock::database_now(db)
            .await
            .map_err(|_| RunError::Retry)?;
        if previous.lease_until > now {
            let remaining = (previous.lease_until - now)
                .to_std()
                .map_err(|_| RunError::Retry)?;
            tokio::select! {
                _ = cancel.cancelled() => return Err(RunError::Lost),
                _ = tokio::time::sleep(remaining.min(Duration::from_secs(60))) => {}
            }
        }
    }
    let root = zip::claim_root(db, &claim.batch_id, &claim.worker, root_lease)
        .await
        .map_err(|_| RunError::Retry)?;
    *current.lock().await = Some(root.clone());
    zip::mark_invoked(db, &root)
        .await
        .map_err(|_| RunError::Retry)?;
    match build_directory_capturing_candidate(&state.kubo, files, cancel, candidate).await {
        Ok(Some(verified)) => {
            let node = verified.local_residency.node_identity.clone();
            zip::retain_candidate(db, &root, &node, "hot", &verified.cid)
                .await
                .map_err(|_| RunError::Retry)?;
            *candidate = None;
            let receipt =
                serde_json::to_string(&verified.local_residency).map_err(|_| RunError::Retry)?;
            zip::verify_root(db, &root, &node, "hot", &verified.cid, &receipt)
                .await
                .map_err(|_| RunError::Retry)?;
            Ok(zip::RootOutcome::Verified {
                claim: root,
                node_identity: node,
                tier: "hot".into(),
                cid: verified.cid,
            })
        }
        Ok(None) => Err(RunError::Retry),
        Err(error) => {
            if let Some(known) = error.candidate() {
                zip::retain_candidate(db, &root, &known.node_identity, "hot", &known.cid)
                    .await
                    .map_err(|_| RunError::Retry)?;
                *candidate = None;
            }
            if matches!(error.reason(), DirectoryBuildError::Canceled) || cancel.is_cancelled() {
                return Err(RunError::Lost);
            }
            // Failed root is observable, but never a substitute for a lost
            // execution guard; the publisher rechecks the complete fence.
            Ok(zip::RootOutcome::ClaimedFailed {
                claim: root,
                code: "root_build_failed",
            })
        }
    }
}
