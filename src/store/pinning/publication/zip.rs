//! One locked transaction algorithm for legacy and negotiated ZIP publications.
use std::collections::{BTreeMap, BTreeSet};

use sea_orm::{DatabaseConnection, DatabaseTransaction, TransactionError, TransactionTrait};

use super::{PinTargetSpec, PublicationObject, invalid_publication};
use crate::{
    error::{AppError, AppResult},
    pinning::{tags::ObjectTag, zip_policy::CapturedZipIntent},
    store::{
        object_version::PublicationResult,
        zip::{self as storage, publication_lease::PublicationLease},
    },
    zip::batch::FinalZipManifest,
};

mod capture;
mod execution;
mod input;
mod legacy;
mod objects;

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
    publish_captured_zip_with_limits(db, request, &config.provider_limits, &selection).await
}

pub(super) async fn publish_captured_zip_with_limits(
    db: &DatabaseConnection,
    request: CapturedZipPublication,
    current_limits: &crate::pinning::config::ProviderLimitMap,
    selection: &CapturedZipIntent,
) -> AppResult<Vec<(String, PublicationResult)>> {
    let selection = std::sync::Arc::new(selection.clone());
    let current_limits = std::sync::Arc::new(current_limits.clone());
    let request = std::sync::Arc::new(request);
    for retry in 0..=super::MAX_TRANSACTION_RETRIES {
        let request = request.clone();
        let current_limits = current_limits.clone();
        let selection = selection.clone();
        match db
            .transaction(|tx| {
                Box::pin(async move {
                    publish_in_transaction(tx, &request, &current_limits, &selection).await
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

#[tracing::instrument(name = "zip_publication", skip_all, fields(batch_id = %request.batch_id, outputs = request.successes.len()))]
pub(super) async fn publish_in_transaction(
    tx: &DatabaseTransaction,
    request: &CapturedZipPublication,
    current_limits: &crate::pinning::config::ProviderLimitMap,
    selection: &CapturedZipIntent,
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
    let maintained_guard = match &request.authority {
        ZipPublicationAuthority::Legacy(authority) if authority.upload_target.is_some() => {
            authority.standard_guard.as_ref()
        }
        _ => None,
    };
    let mut maintenance = PublicationLease::start(tx, maintained_guard, &request.batch_id).await?;
    let state = crate::store::bucket::lock_versioning_state(tx, request.capture.bucket()).await?;
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

    let now = crate::store::database_clock::database_now(tx).await?;
    let (results, bindings, leases) =
        objects::publish_objects(tx, request, state, now, &mut maintenance, selection).await?;
    let references = leases
        .into_iter()
        .map(|lease| {
            (
                lease.owner.object_id,
                PinTargetSpec {
                    cid: lease.owner.cid,
                    logical_size: lease.owner.logical_size,
                },
                lease.intent,
            )
        })
        .collect::<Vec<_>>();
    super::resources::attach_many(tx, &references, current_limits, now).await?;
    maintenance.checkpoint(tx).await?;
    let terminal = execution::terminal_result(tx, request, &results).await?;
    match &request.root_outcome {
        storage::RootOutcome::ClaimedFailed { claim, code } => {
            storage::publish_with_maintenance(
                tx,
                &claim.batch_id,
                &bindings,
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
                &bindings,
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
    storage::verify_published(
        tx,
        &batch_before,
        &manifest_before,
        &bindings,
        request.source.is_some(),
        &terminal,
        &request.root_outcome,
    )
    .await?;
    Ok(results)
}

pub(super) use capture::read_capture;
pub(super) use legacy::publish_legacy_in_transaction;

pub fn decode_zip_capture_json(json: &str) -> AppResult<CapturedZipIntent> {
    capture::read_capture(json)
}
