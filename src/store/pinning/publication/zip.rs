//! One locked transaction algorithm for legacy and negotiated ZIP publications.
use std::collections::{BTreeMap, BTreeSet};

use sea_orm::{
    ConnectionTrait, DatabaseConnection, DatabaseTransaction, TransactionError, TransactionTrait,
};

use super::{PublicationObject, invalid_publication, leases, ledger, quota};
use crate::{
    error::{AppError, AppResult},
    pinning::{
        tags::ObjectTag,
        zip_policy::{CapturedZipIntent, ZipBoundOutput, select_zip_intents},
    },
    store::{
        object_version::{BucketVersioningState, PublicationResult},
        zip::{self as storage, VersionBinding},
    },
    zip::batch::FinalZipManifest,
};

mod allocation;
mod capture;
mod execution;
mod input;
mod legacy;

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
) -> AppResult<Vec<(String, PublicationResult)>> {
    for retry in 0..=super::MAX_TRANSACTION_RETRIES {
        let request = request.clone();
        match db
            .transaction(|tx| Box::pin(async move { publish_in_transaction(tx, &request).await }))
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
) -> AppResult<Vec<String>> {
    let locations = objects
        .iter()
        .map(|o| (&o.bucket, &o.key))
        .collect::<BTreeSet<_>>();
    let mut owners = BTreeSet::new();
    for (bucket, key) in locations {
        crate::store::object_version::lock_current_row(tx, bucket, key).await?;
        crate::store::object_version::allocate_next_sequence(tx, bucket, key).await?;
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

async fn bound(
    tx: &DatabaseTransaction,
    object: &PublicationObject,
    path: &str,
) -> AppResult<(ZipBoundOutput, VersionBinding)> {
    let binding = storage::binding_for_published_object(tx, path, &object.id).await?;
    let mut bound = candidate_binding(object);
    bound.version_id = binding.version_row_id.clone();
    Ok((bound, binding))
}

pub(super) async fn publish_in_transaction(
    tx: &DatabaseTransaction,
    request: &CapturedZipPublication,
) -> AppResult<Vec<(String, PublicationResult)>> {
    validate(request)?;
    crate::store::import::ownership::lock_bucket_for_ownership(tx, request.capture.bucket())
        .await?;
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
    let ordered = objects(request);
    let state = crate::store::bucket::lock_versioning_state(tx, request.capture.bucket()).await?;
    let owners = lock_version_frontier(tx, &ordered, state).await?;
    capture::verify_batch(tx, request).await?;
    input::verify_publication(tx, request).await?;
    if let Some(snapshot) = &admission {
        execution::verify_manifest(tx, request, snapshot).await?;
    }
    capture::verify_manifest(tx, request).await?;

    // This candidate selection is lock budgeting only. Real immutable row IDs
    // replace all placeholders before any decision/lease is written.
    let source_candidate = request.source.as_ref().map(candidate_binding);
    let candidates = request
        .successes
        .iter()
        .map(candidate_binding)
        .collect::<Vec<_>>();
    let candidate = select_zip_intents(
        &request.capture,
        &request.manifest,
        source_candidate.as_ref(),
        &candidates,
    )
    .map_err(|_| invalid())?;
    let (limits, routes, allocatable) =
        allocation::frozen_allocation(&request.capture, &candidate)?;
    let pairs = allocation::attachment_pairs(tx, &candidate, &limits).await?;
    leases::lock_publication_lifecycle_frontier(tx, &owners, &pairs).await?;
    quota::lock_publication_usage_rows(
        tx,
        &pairs.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>(),
    )
    .await?;
    if allocatable.len() == routes.len() {
        ledger::verify_selected_routes(tx, &routes, true).await?;
    } else {
        ledger::verify_birth_candidate_routes(tx, &routes, &allocatable, true).await?;
    }
    crate::store::residency::prepare_hot_publication_frontier(
        tx,
        ordered.iter().map(|o| o.cid.clone()).collect(),
    )
    .await?;
    let now = crate::store::database_clock::database_now(tx).await?;
    let mut results = Vec::with_capacity(ordered.len());
    for object in ordered {
        results.push((
            object.key.clone(),
            super::write_object_version_and_update_lifecycle(tx, object, state, now).await?,
        ));
    }
    let source_bound = request.source.as_ref().map(|o| bound(tx, o, "source"));
    let source_bound = match source_bound {
        Some(future) => Some(future.await?.0),
        None => None,
    };
    let by_key = request
        .successes
        .iter()
        .map(|o| (&o.key, o))
        .collect::<BTreeMap<_, _>>();
    let mut successful = Vec::new();
    let mut bindings = Vec::new();
    for file in &request.manifest.successful {
        let (output, binding) = bound(tx, by_key[&file.object_key], &file.relative_path).await?;
        successful.push(output);
        bindings.push(binding);
    }
    let selected = select_zip_intents(
        &request.capture,
        &request.manifest,
        source_bound.as_ref(),
        &successful,
    )
    .map_err(|_| invalid())?;
    allocation::verify_candidate(&candidate, &selected)?;
    if let (Some(source), Some(decision)) = (&request.source, &selected.source_decision) {
        super::tags::replace_object_tags(tx, &source.id, &request.source_tags).await?;
        crate::store::pinning::decision::write_for_object_in_transaction(tx, &source.id, decision)
            .await?;
    }
    for (output, decision) in &selected.output_decisions {
        // Each real output is a new control/version owner, not a reuse of the
        // source's unique control revision. Authorization remains frozen.
        let mut decision = decision.clone();
        decision.control_revision = uuid::Uuid::new_v4().to_string();
        crate::store::pinning::decision::write_for_object_in_transaction(
            tx,
            &output.object_id,
            &decision,
        )
        .await?;
    }
    allocation::create_leases(tx, &selected, &limits, now).await?;
    let terminal = execution::terminal_result(tx, request, &results).await?;
    match &request.root_outcome {
        storage::RootOutcome::ClaimedFailed { claim, code } => {
            claim
                .publish_failed(tx, &bindings, request.source.is_some(), &terminal, code)
                .await?
        }
        outcome => {
            storage::publish(
                tx,
                &request.batch_id,
                &bindings,
                request.source.is_some(),
                &terminal,
                outcome.clone(),
            )
            .await?
        }
    }
    match &request.authority {
        ZipPublicationAuthority::Legacy(authority) => {
            legacy::settle(tx, request, authority, &results, now).await?
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
                &results,
                &terminal,
                now,
            )
            .await?
        }
    }
    Ok(results)
}

pub(super) use capture::read_capture;
pub(super) use legacy::publish_legacy_in_transaction;

pub fn decode_zip_capture_json(json: &str) -> AppResult<CapturedZipIntent> {
    capture::read_capture(json)
}
