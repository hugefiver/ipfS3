//! Exact public documents are matched to local frozen reviews, never promoted.
use super::*;
use crate::pinning::management::{self as management, PlanStatus, wire::ImportedPlan};

pub async fn load_review_from_imported<C: ConnectionTrait>(
    db: &C,
    auth: &LocalAuthorization,
    imported: &ImportedPlan,
) -> Result<ReviewHandle> {
    auth.check(db).await?;
    let source = evidence::Evidence::load_with_work(
        db,
        &auth.database_scope,
        &auth.database_identity,
        &[imported.request().plan_id.clone()],
        imported.session(),
    )
    .await?;
    let first = source
        .archive_members(&auth.database_scope, &imported.request().plan_id)
        .next()
        .ok_or(ManagementError::Confirmation)?;
    let review = archive::load_review(&source, first, &mut *source.budget()?)?;
    if review.scope() != auth.database_scope
        || review.identity() != auth.database_identity
        || review.principal() != auth.principal
    {
        return Err(ManagementError::Unauthorized);
    }
    validate_reviewed(&review, imported)?;
    if review.candidate_item_ids() != imported.confirmation() {
        return Err(ManagementError::Confirmation);
    }
    Ok(review)
}

/// Restoration does not revalidate today's TTL or configuration. Apply and the
/// worker independently resolve local durable authority at their own boundaries.
pub async fn load_confirmed_from_imported<C: ConnectionTrait>(
    db: &C,
    auth: &LocalAuthorization,
    imported: &ImportedPlan,
) -> Result<ConfirmedBinding> {
    let review = load_review_from_imported(db, auth, imported).await?;
    if review.candidate_item_ids().is_none() {
        return Err(ManagementError::Confirmation);
    }
    let source = evidence::Evidence::load_with_work(
        db,
        &auth.database_scope,
        &auth.database_identity,
        &[imported.request().plan_id.clone()],
        imported.session(),
    )
    .await?;
    let first = source
        .archive_members(&auth.database_scope, &imported.request().plan_id)
        .next()
        .ok_or(ManagementError::Confirmation)?;
    decode_binding_from_row(&source, first)
}

pub(super) fn validate_reviewed(review: &ReviewHandle, imported: &ImportedPlan) -> Result<()> {
    if !imported.untrusted_applied_claims().is_empty() {
        return Err(ManagementError::Confirmation);
    }
    let acks = imported
        .confirmation()
        .ok_or(ManagementError::Confirmation)?;
    let expected = review.planning_view();
    let supplied = imported.snapshot_without_applied();
    if review.request() != imported.request()
        || review.entries() != imported.plan().items()
        || review.summary() != imported.plan().summary()
        || review.migration_points() != imported.migration_points()
        || expected.observed_at != supplied.observed_at
        || expected.database_now != supplied.database_now
        || expected.configuration != &supplied.configuration
        || expected.objects != supplied.objects
        || expected.routes != supplied.routes
        || expected.targets != supplied.targets
        || expected.remotes != supplied.remotes
        || expected.historical_remotes != supplied.historical_remotes
        || expected.unknown_resources != supplied.unknown_resources
        || expected.ledger_domains != supplied.ledger_domains
        || expected.inventories != supplied.inventories
        || expected.root_references != supplied.root_references
        || expected.applied != supplied.applied
        || supplied
            .native_frame
            .as_ref()
            .is_none_or(|frame| !management::wire::native_equal(review, frame))
    {
        return Err(ManagementError::Confirmation);
    }
    if acks.iter().enumerate().any(|(index, id)| {
        acks[..index].contains(id)
            || !review
                .entries()
                .iter()
                .any(|entry| entry.status == PlanStatus::Candidate && entry.request.item_id == *id)
    }) || review.entries().iter().any(|entry| {
        matches!(entry.status, PlanStatus::Blocked(_))
            || entry.status == PlanStatus::Candidate && !acks.contains(&entry.request.item_id)
    }) {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}
