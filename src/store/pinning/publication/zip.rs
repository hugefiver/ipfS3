//! One locked transaction algorithm for legacy and negotiated ZIP publications.
use std::collections::{BTreeMap, BTreeSet};

use sea_orm::{
    ConnectionTrait, DatabaseConnection, DatabaseTransaction, TransactionError, TransactionTrait,
};

use super::{PublicationObject, invalid_publication, ledger};
use crate::{
    error::{AppError, AppResult},
    pinning::{
        tags::ObjectTag,
        zip_policy::{CapturedZipIntent, ZipBoundOutput},
    },
    store::{
        object_version::{BucketVersioningState, PublicationResult},
        zip::{self as storage, VersionBinding, publication_lease::PublicationLease},
    },
    zip::batch::FinalZipManifest,
};

pub(super) mod allocation;
mod capture;
mod execution;
mod input;
mod keys;
mod legacy;
mod objects;
mod pins;
mod sql;
mod work;

pub use execution::ZipExecutionSettlement;
pub use input::{LegacyZipInputProof, bind_legacy_import_input, read_legacy_import_input};
pub use legacy::LegacyZipAuthority;

#[derive(Debug, Clone)]
pub enum ZipPublicationAuthority {
    Legacy(Box<LegacyZipAuthority>),
    Execution {
        claim: storage::execution::Claim,
        source_guard: Option<crate::store::import::ownership::StandardMutationGuard>,
        settlement: ZipExecutionSettlement,
    },
}

/// No per-output pre-authorized policy is accepted here. The capture must equal
/// admission storage; successful objects must equal the prepared final manifest.
#[derive(Debug, Clone)]
pub struct CapturedZipPublication {
    pub batch_id: String,
    pub source: Option<PublicationObject>,
    pub source_tags: Vec<ObjectTag>,
    pub successes: Vec<PublicationObject>,
    pub manifest: FinalZipManifest,
    pub capture: CapturedZipIntent,
    pub authority: ZipPublicationAuthority,
    pub root_outcome: storage::RootOutcome,
    pub terminal_result: String,
}

pub fn serialize_zip_capture(capture: &CapturedZipIntent) -> AppResult<String> {
    let json = serde_json::to_string(capture).map_err(|_| invalid())?;
    CapturedZipIntent::decode_json(&json).map_err(|_| invalid())?;
    Ok(json)
}

fn invalid() -> AppError {
    invalid_publication(
        "ZIP publication capture, exact manifest or admission mismatch; needs_attention",
    )
}

pub async fn publish_captured_zip(
    db: &DatabaseConnection,
    request: CapturedZipPublication,
    config: &crate::pinning::config::ValidatedPinningConfig,
    rules: &crate::pinning::zip_policy::ValidatedZipOutputRules,
    mode: crate::config::OptionalPinControlMode,
) -> AppResult<Vec<(String, PublicationResult)>> {
    let selection = request
        .capture
        .resolve_current_with_rules(config, rules, mode)
        .map_err(|error| AppError::InvalidPinningRequest(error.to_string()))?;
    publish_captured_zip_with_limits(db, request, Some(&config.provider_limits), &selection).await
}

pub(super) async fn publish_captured_zip_with_limits(
    db: &DatabaseConnection,
    request: CapturedZipPublication,
    current_limits: Option<&crate::pinning::config::ProviderLimitMap>,
    selection: &CapturedZipIntent,
) -> AppResult<Vec<(String, PublicationResult)>> {
    let selection = std::sync::Arc::new(selection.clone());
    let current_limits = current_limits.cloned().map(std::sync::Arc::new);
    let request = std::sync::Arc::new(request);
    let session = work::session(&request, &selection)?;
    for retry in 0..=super::MAX_TRANSACTION_RETRIES {
        work::begin_attempt(&session)?;
        let request = request.clone();
        let session = session.clone();
        let current_limits = current_limits.clone();
        let selection = selection.clone();
        match db
            .transaction(|tx| {
                Box::pin(async move {
                    publish_with_reservations(
                        &mut super::reservations::BorrowedWithWork {
                            db: tx,
                            work: &session,
                        },
                        &request,
                        current_limits.as_deref(),
                        &selection,
                    )
                    .await
                })
            })
            .await
        {
            Ok(result) => return Ok(result),
            Err(TransactionError::Transaction(error))
                if super::is_retryable_transaction_conflict(&error)
                    && retry < super::MAX_TRANSACTION_RETRIES =>
            {
                super::publication_retry_delay(retry).await
            }
            // Connection/commit errors never qualify as known rollback retries.
            Err(error) => return Err(super::transaction_error_into_app(error)),
        }
    }
    unreachable!("ZIP retry loop always returns")
}

pub(crate) async fn publish_captured_zip_store(
    store: &crate::store::Store,
    request: CapturedZipPublication,
    config: &crate::pinning::config::ValidatedPinningConfig,
    rules: &crate::pinning::zip_policy::ValidatedZipOutputRules,
    mode: crate::config::OptionalPinControlMode,
) -> AppResult<Vec<(String, PublicationResult)>> {
    let selection = request
        .capture
        .resolve_current_with_rules(config, rules, mode)
        .map_err(|error| AppError::InvalidPinningRequest(error.to_string()))?;
    let current_limits = &config.provider_limits;
    let session = work::session(&request, &selection)?;
    for retry in 0..=super::MAX_TRANSACTION_RETRIES {
        work::begin_attempt(&session)?;
        let tx = store
            .begin_publication_runtime(super::super::management::records::DecodeWork::shared(
                &session,
            ))
            .await
            .map_err(super::transaction_error_into_app)?;
        let mut owner = super::PublicationTransaction::Runtime(tx, session.clone());
        let result = Box::pin(publish_with_reservations(
            &mut owner,
            &request,
            Some(current_limits),
            &selection,
        ))
        .await;
        let result = match result {
            Ok(result) => {
                owner.into_db().commit().await.map_err(|error| {
                    super::transaction_error_into_app(TransactionError::Connection(error))
                })?;
                return Ok(result);
            }
            Err(error) => {
                owner.into_db().rollback().await?;
                error
            }
        };
        if super::is_retryable_transaction_conflict(&result)
            && retry < super::MAX_TRANSACTION_RETRIES
        {
            super::publication_retry_delay(retry).await;
        } else {
            return Err(result);
        }
    }
    unreachable!("ZIP retry loop always returns")
}

fn objects(request: &CapturedZipPublication) -> Vec<&PublicationObject> {
    let mut objects = request
        .source
        .iter()
        .chain(&request.successes)
        .collect::<Vec<_>>();
    objects.sort_by(|a, b| (&a.bucket, &a.key).cmp(&(&b.bucket, &b.key)));
    objects
}

fn validate(request: &CapturedZipPublication) -> AppResult<()> {
    serialize_zip_capture(&request.capture)?;
    if request.batch_id.is_empty()
        || request.source.is_some() != request.capture.controls().publish_source
        || request.terminal_result.is_empty()
        || request.terminal_result.len() > 1_048_576
        || serde_json::from_str::<serde_json::Value>(&request.terminal_result).is_err()
        || request.successes.len() > crate::zip::extract::MAX_ARCHIVE_ENTRIES as usize
    {
        return Err(invalid());
    }
    let build_root =
        request.capture.controls().root.enabled() && request.capture.controls().publish_extracted;
    match (
        build_root,
        request.successes.is_empty(),
        &request.root_outcome,
    ) {
        (false, _, storage::RootOutcome::Disabled) | (true, true, storage::RootOutcome::Empty) => {}
        (
            true,
            false,
            storage::RootOutcome::Failed { .. }
            | storage::RootOutcome::ClaimedFailed { .. }
            | storage::RootOutcome::Verified { .. },
        ) => {}
        _ => return Err(invalid()),
    }
    if let Some(source) = &request.source {
        if source.key != request.capture.source_key() {
            return Err(invalid());
        }
        request
            .capture
            .source_decision()
            .replay_policy(request.source_tags.clone())
            .map_err(|_| invalid())?;
    } else if !request.source_tags.is_empty() {
        return Err(invalid());
    }
    let mut ids = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for object in objects(request) {
        if object.bucket != request.capture.bucket()
            || object.logical_size < 0
            || object.cid.is_empty()
            || object.id.is_empty()
            || !ids.insert(&object.id)
            || !keys.insert(&object.key)
        {
            return Err(invalid());
        }
    }
    let successes = request
        .successes
        .iter()
        .map(|o| (&o.key, (&o.cid, o.logical_size)))
        .collect::<BTreeMap<_, _>>();
    let mut paths = BTreeSet::new();
    if successes.len() != request.manifest.successful.len() {
        return Err(invalid());
    }
    for file in &request.manifest.successful {
        if !paths.insert(&file.relative_path)
            || successes.get(&file.object_key) != Some(&(&file.cid, file.size))
            || file.object_key == request.capture.source_key()
        {
            return Err(invalid());
        }
    }
    Ok(())
}

pub(super) async fn lock_version_frontier<C: ConnectionTrait>(
    tx: &C,
    objects: &[&PublicationObject],
    state: BucketVersioningState,
    mut maintenance: Option<&mut PublicationLease<'_>>,
) -> AppResult<Vec<String>> {
    let locations = objects
        .iter()
        .map(|o| (&o.bucket, &o.key))
        .collect::<BTreeSet<_>>();
    let mut owners = BTreeSet::new();
    for (bucket, key) in locations {
        if let Some(lease) = maintenance.as_deref_mut() {
            lease.checkpoint(tx).await?;
        }
        crate::store::object_version::lock_current_row(tx, bucket, key).await?;
        if let Some(lease) = maintenance.as_deref_mut() {
            lease.checkpoint(tx).await?;
        }
        crate::store::object_version::allocate_next_sequence(tx, bucket, key).await?;
        if let Some(lease) = maintenance.as_deref_mut() {
            lease.checkpoint(tx).await?;
        }
        let current = super::lock_current_object_projection(tx, bucket, key).await?;
        match state {
            BucketVersioningState::Unversioned => {
                if let Some(current) = current {
                    owners.insert(current.id);
                }
            }
            BucketVersioningState::Enabled => {}
            BucketVersioningState::Suspended => {
                if let Some(id) = super::lock_null_object_id(tx, bucket, key).await? {
                    super::lock_object_by_id(tx, &id).await?;
                    owners.insert(id);
                }
            }
        }
    }
    Ok(owners.into_iter().collect())
}

fn candidate_binding(object: &PublicationObject) -> ZipBoundOutput {
    ZipBoundOutput {
        object_id: object.id.clone(),
        bucket: object.bucket.clone(),
        key: object.key.clone(),
        version_id: format!("candidate:{}", object.id),
        cid: object.cid.clone(),
        logical_size: object.logical_size,
    }
}

pub(super) async fn publish_in_transaction(
    tx: &DatabaseTransaction,
    request: &CapturedZipPublication,
    current_limits: &crate::pinning::config::ProviderLimitMap,
    selection: &CapturedZipIntent,
) -> AppResult<Vec<(String, PublicationResult)>> {
    publish_with_reservations(
        &mut super::reservations::Borrowed(tx),
        request,
        Some(current_limits),
        selection,
    )
    .await
}

#[tracing::instrument(name = "zip_publication", skip_all, fields(batch_id = %request.batch_id, outputs = request.successes.len()))]
async fn publish_with_reservations<
    R: super::reservations::Reservations<Connection = DatabaseTransaction>,
>(
    reservations: &mut R,
    request: &CapturedZipPublication,
    current_limits: Option<&crate::pinning::config::ProviderLimitMap>,
    selection: &CapturedZipIntent,
) -> AppResult<Vec<(String, PublicationResult)>> {
    let tx = reservations.db();
    validate(request)?;
    crate::store::import::ownership::lock_bucket_for_ownership(tx, request.capture.bucket())
        .await?;
    let session = match reservations.runtime_work() {
        Some(session) => session.clone(),
        None => work::session(request, selection)?,
    };
    let work = super::super::management::records::DecodeWork::shared(&session);
    work.with(|session| session.check())
        .map_err(super::super::management::admission::work_error)?;
    // Admission readers below run only after the bucket write/fence, before any frontier locks.
    let admission = match &request.authority {
        ZipPublicationAuthority::Legacy(authority) => {
            legacy::verify_ownership(tx, request, authority).await?;
            None
        }
        ZipPublicationAuthority::Execution {
            claim,
            source_guard,
            settlement,
        } => Some(
            execution::verify_ownership(tx, request, claim, source_guard.as_ref(), settlement)
                .await?,
        ),
    };
    let maintained_guard = match &request.authority {
        ZipPublicationAuthority::Legacy(authority) if authority.upload_target.is_some() => {
            authority.standard_guard.as_ref()
        }
        _ => None,
    };
    let mut maintenance = PublicationLease::start(tx, maintained_guard, &request.batch_id).await?;
    let ordered = objects(request);
    let state = crate::store::bucket::lock_versioning_state(tx, request.capture.bucket()).await?;
    let locked_objects =
        objects::LockedObjects::load(tx, request, work, Some(&mut maintenance)).await?;
    let owners = locked_objects.owners(state);
    maintenance.checkpoint(tx).await?;
    let batch_before = capture::verify_batch(tx, request).await?;
    if let storage::RootOutcome::Verified { claim, .. } = &request.root_outcome {
        maintenance
            .root_locked(tx, &request.batch_id, claim)
            .await?;
    }
    maintenance.checkpoint(tx).await?;
    input::verify_publication(tx, request).await?;
    if let Some(snapshot) = &admission {
        execution::verify_manifest(tx, request, snapshot).await?;
    }
    let manifest_before = capture::verify_manifest(tx, request).await?;
    let manifest_bytes = manifest_before.iter().try_fold(0usize, |total, row| {
        total
            .checked_add(row.batch_id.len())
            .and_then(|n| n.checked_add(row.path.len()))
            .and_then(|n| n.checked_add(row.object_key.as_ref().map_or(0, String::len)))
            .and_then(|n| n.checked_add(row.cid.as_ref().map_or(0, String::len)))
            .and_then(|n| n.checked_add(row.error_code.as_ref().map_or(0, String::len)))
            .and_then(|n| n.checked_add(256))
            .ok_or(AppError::ManagementBudget)
    })?;
    let _manifest_resident = sql::retain_copy(work, manifest_bytes)?;

    // This candidate selection is lock budgeting only. Real immutable row IDs
    // replace all placeholders before any decision/lease is written.
    let candidate_inputs_resident = sql::retain_copy(work, work::candidate_bytes(request)?)?;
    let source_candidate = request.source.as_ref().map(candidate_binding);
    let candidates = request
        .successes
        .iter()
        .map(candidate_binding)
        .collect::<Vec<_>>();
    let (candidate, _candidate_resident) = work::select(
        selection,
        &request.manifest,
        source_candidate.as_ref(),
        &candidates,
        work,
    )?;
    drop(source_candidate);
    drop(candidates);
    drop(candidate_inputs_resident);
    let allocation_keys = keys::AllocationKeys::load(
        tx,
        selection,
        &candidate,
        current_limits,
        work,
        Some(&mut maintenance),
    )
    .await?;
    let routes = &allocation_keys.routes;
    let allocatable = &allocation_keys.enabled;
    maintenance.checkpoint(tx).await?;
    let now = crate::store::database_clock::database_now(tx).await?;
    let pin_snapshot = pins::LockedPins::load(
        reservations,
        &allocation_keys,
        &candidate,
        &owners,
        now,
        work,
        Some(&mut maintenance),
    )
    .await?;
    let shape_bytes = work
        .with(|session| allocation::selection_shape_bytes(&candidate, session))
        .map_err(super::super::management::admission::work_error)?;
    let candidate_shape_resident = sql::retain_copy(work, shape_bytes)?;
    let candidate_shape = allocation::selection_shape(&candidate);
    drop(candidate);
    drop(_candidate_resident);
    maintenance.checkpoint(reservations.db()).await?;
    let tx = reservations.db();
    allocation_keys
        .verify_aliases(tx, work, Some(&mut maintenance))
        .await?;
    if let Some(owner) = reservations.runtime() {
        ledger::verify_runtime_birth_routes(owner, routes, allocatable, true, work).await?;
    } else if allocatable.len() == routes.len() {
        ledger::verify_selected_routes(tx, routes, true).await?;
    } else {
        ledger::verify_birth_candidate_routes(tx, routes, allocatable, true).await?;
    }
    maintenance.checkpoint(tx).await?;
    let physical_frontier = objects::prepare_physical(
        tx,
        ordered.iter().map(|object| object.cid.clone()).collect(),
        now,
        work,
        Some(&mut maintenance),
    )
    .await?;
    maintenance.checkpoint(tx).await?;
    pin_snapshot
        .lock_tail(tx, work, Some(&mut maintenance))
        .await?;
    let mut object_writes = locked_objects.plan(request, state, now, work)?;
    let (selected, _selected_resident) = work::select(
        selection,
        &request.manifest,
        object_writes.source.as_ref(),
        &object_writes.outputs,
        work,
    )?;
    allocation::verify_candidate(&candidate_shape, &selected)?;
    object_writes.decisions(request, &selected, work)?;
    let pin_writes = pin_snapshot.plan(&selected, &allocation_keys, &owners, request, now, work)?;
    drop(selected);
    drop(_selected_resident);
    drop(candidate_shape);
    drop(candidate_shape_resident);
    let results = &object_writes.results;
    let bindings = &object_writes.bindings;
    object_writes
        .persist(reservations.db(), work, &mut maintenance)
        .await?;
    pin_writes
        .persist(reservations.db(), work, &mut maintenance)
        .await?;
    let tx = reservations.db();
    maintenance.checkpoint(tx).await?;
    let terminal = execution::terminal_result(tx, request, results).await?;
    match &request.root_outcome {
        storage::RootOutcome::ClaimedFailed { claim, code } => {
            storage::publish_with_maintenance(
                tx,
                &claim.batch_id,
                bindings,
                request.source.is_some(),
                &terminal,
                storage::RootOutcome::Failed { code },
                Some(claim),
                Some(&mut maintenance),
            )
            .await?
        }
        outcome => {
            storage::publish_with_maintenance(
                tx,
                &request.batch_id,
                bindings,
                request.source.is_some(),
                &terminal,
                outcome.clone(),
                None,
                Some(&mut maintenance),
            )
            .await?
        }
    }
    maintenance.checkpoint(tx).await?;
    let settlement_bytes = match &request.authority {
        ZipPublicationAuthority::Legacy(authority) => authority.result_rows.len().checked_mul(2048),
        ZipPublicationAuthority::Execution { .. } => Some(0),
    }
    .and_then(|bytes| bytes.checked_add(terminal.len()))
    .ok_or(AppError::ManagementBudget)?;
    let _settlement_resident = sql::retain_copy(work, settlement_bytes)?;
    match &request.authority {
        ZipPublicationAuthority::Legacy(authority) => {
            legacy::settle(tx, request, authority, results, now).await?
        }
        ZipPublicationAuthority::Execution {
            claim,
            source_guard,
            settlement,
        } => {
            execution::settle(
                tx,
                request,
                admission.as_ref().ok_or_else(invalid)?,
                claim,
                source_guard.as_ref(),
                settlement,
                results,
                &terminal,
                now,
            )
            .await?
        }
    }
    storage::verify_published(
        tx,
        &batch_before,
        &manifest_before,
        bindings,
        request.source.is_some(),
        &terminal,
        &request.root_outcome,
    )
    .await?;
    object_writes.verify(tx, work).await?;
    physical_frontier.verify(tx, work, None).await?;
    pin_writes.verify(reservations, work).await?;
    if let Some(owner) = reservations.runtime() {
        ledger::verify_runtime_birth_routes(owner, routes, allocatable, false, work).await?;
    } else {
        ledger::verify_birth_candidate_routes(tx, routes, allocatable, false).await?;
    }
    allocation_keys.verify_aliases(tx, work, None).await?;
    work.with(|session| session.check())
        .map_err(super::super::management::admission::work_error)?;
    Ok(object_writes.results)
}

pub(super) use capture::read_capture;
pub(super) use legacy::publish_legacy_in_transaction;

pub fn decode_zip_capture_json(json: &str) -> AppResult<CapturedZipIntent> {
    capture::read_capture(json)
}
