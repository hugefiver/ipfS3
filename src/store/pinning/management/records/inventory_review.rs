//! Non-importable proof that a complete reducer review produced this capture.
use super::*;
use crate::pinning::management::{
    Capture, PlanEntry,
    retirement::{RetirementFence, RetirementReview, RunLimits},
};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BoundInventory {
    scope: String,
    identity: String,
    principal: String,
    plan: String,
    entry: PlanEntry,
}

mod persistence;
pub(super) use persistence::bind_canonical_archive;
pub(super) use persistence::check_checkpoint_confirmation;
pub use persistence::copy_original_inventory;
pub(crate) use persistence::copy_original_inventory_document;
pub(crate) use persistence::copy_prepared_output_document;
pub(crate) use persistence::finish_apply;
pub use persistence::load_original_inventory;
pub(crate) use persistence::prepare_apply;
pub(crate) use persistence::prepared_output_reference;
pub(crate) use persistence::publish as stage_canonical_inventory;
pub(crate) use persistence::publish_output as stage_canonical_output;
pub(super) use persistence::{hydrate, publish_transition, seal_confirmed};

/// Prepared from full original inventory outside the final-write transaction.
/// No spool or archive reader survives into the publication lock boundary.
pub struct PreparedRetirementReview {
    bound: BoundInventory,
    fence: RetirementFence,
}

/// Reconstruct the selected capture against the complete reducer and originals.
/// This performs disk IO and must be called before the final-write transaction.
pub async fn prepare_retirement_review(
    authorization: &LocalAuthorization,
    review: &RetirementReview<'_>,
    request: &PlanRequest,
    entry: &PlanEntry,
    session: &SharedDecodeBudget,
) -> Result<PreparedRetirementReview> {
    crate::pinning::management::validate_request(request)
        .map_err(|_| ManagementError::Confirmation)?;
    let Some(Capture::Provider(capture)) = &entry.capture else {
        return Err(ManagementError::InvalidEvidence);
    };
    let checkpoint = review.checkpoint();
    if !request.items.contains(&entry.request)
        || !checkpoint.complete()
        || checkpoint.database_scope != authorization.database_scope
        || checkpoint.database_identity != authorization.database_identity
        || review.reference() != &capture.inventory
        || review.totals() != &capture.totals
    {
        return Err(ManagementError::Confirmation);
    }
    DecodeWork::shared(session).copy(&capture.mutation_scope)?;
    let reconstructed = review
        .capture(&entry.request, capture.mutation_scope.clone())
        .await?;
    if reconstructed != **capture
        || entry.step.as_ref()
            != Some(&crate::pinning::management::PlannedStep::Retire(
                review.step(),
            ))
    {
        return Err(ManagementError::Confirmation);
    }
    DecodeWork::shared(session).copy(&(
        &authorization.database_scope,
        &authorization.database_identity,
        &authorization.principal,
        &request.plan_id,
        entry,
    ))?;
    Ok(PreparedRetirementReview {
        bound: BoundInventory {
            scope: authorization.database_scope.clone(),
            identity: authorization.database_identity.clone(),
            principal: authorization.principal.clone(),
            plan: request.plan_id.clone(),
            entry: entry.clone(),
        },
        fence: review.prepare_fence()?,
    })
}

/// Install the prepared proof after ordered business locks and before source
/// mutations. Only bounded database checks run here, never spool reads/reduction.
pub async fn bind_sealed_retirement_review(
    tx: &DatabaseTransaction,
    authorization: &LocalAuthorization,
    prepared: PreparedRetirementReview,
    session: &SharedDecodeBudget,
    limits: &RunLimits,
) -> Result<()> {
    authorization.check(tx).await?;
    let bound = prepared.bound;
    if bound.scope != authorization.database_scope
        || bound.identity != authorization.database_identity
        || bound.principal != authorization.principal
    {
        return Err(ManagementError::Unauthorized);
    }
    prepared.fence.seal(tx, limits).await?;
    let mut work = session
        .lock()
        .map_err(|_| ManagementError::InvalidEvidence)?;
    work.check()?;
    if !work.retirement_reviews.iter().any(|existing| {
        existing.scope == bound.scope
            && existing.identity == bound.identity
            && existing.principal == bound.principal
            && existing.plan == bound.plan
            && existing.entry == bound.entry
    }) {
        work.retirement_reviews.push(bound);
    }
    Ok(())
}

pub(super) fn require(
    work: DecodeWork<'_>,
    scope: &str,
    identity: &str,
    principal: &str,
    plan: &str,
    entry: &PlanEntry,
) -> Result<()> {
    work.with(|work| {
        work.check()?;
        if work.retirement_reviews.iter().any(|bound| {
            bound.scope == scope
                && bound.identity == identity
                && bound.principal == principal
                && bound.plan == plan
                && bound.entry == *entry
        }) {
            Ok(())
        } else {
            Err(ManagementError::InvalidEvidence)
        }
    })
}

pub(crate) fn validate_entry(
    work: &SharedDecodeBudget,
    scope: &str,
    identity: &str,
    plan: &str,
    entry: &PlanEntry,
) -> Result<()> {
    let work = work.lock().map_err(|_| ManagementError::InvalidEvidence)?;
    work.check()?;
    if work.retirement_reviews.iter().any(|bound| {
        bound.scope == scope
            && bound.identity == identity
            && bound.plan == plan
            && bound.entry == *entry
    }) {
        Ok(())
    } else {
        Err(ManagementError::InvalidEvidence)
    }
}
