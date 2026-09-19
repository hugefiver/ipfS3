//! The transition worker uses the same immutable target and rule evaluator as expiration.
use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, PaginatorTrait, QueryFilter};

use crate::{
    error::AppResult,
    lifecycle::{
        actions::{
            action_matches_expected, lock_lifecycle_configuration, revalidate_candidate,
            same_action_definition,
        },
        config::from_canonical_json,
        evaluator::{LifecycleEvaluationContext, evaluate_candidate},
        model::{
            ClaimedLifecycleAction, LifecycleCandidate, LifecycleTargetIdentity,
            VersionLifecycleCandidate,
        },
    },
    store::{
        database_clock::database_now,
        entities::object_version,
        lifecycle_action::{action_kind_from_db, lock_claim_for_execution, target_from_action},
        pinning::tags::list_object_tags,
    },
};

/// Caller holds the action and bucket ownership fences. Every invocation reloads
/// configuration, immutable version, tags, role/count and residency facts.
pub(crate) async fn revalidate_transition(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
) -> AppResult<Option<VersionLifecycleCandidate>> {
    let Some(action) = lock_claim_for_execution(txn, claim).await? else {
        return Ok(None);
    };
    if !same_action_definition(&action, &claim.action) {
        return Ok(None);
    }
    let LifecycleTargetIdentity::Version(target) = target_from_action(&action)? else {
        return Ok(None);
    };
    let Some(row) = lock_lifecycle_configuration(txn, &target.bucket).await? else {
        return Ok(None);
    };
    if row.revision != action.config_revision {
        return Ok(None);
    }
    let Some(json) = row.canonical_json else {
        return Ok(None);
    };
    let configuration = from_canonical_json(&json)?;
    let Some(candidate) =
        revalidate_candidate(txn, &target, action_kind_from_db(&action.action_kind)?).await?
    else {
        return Ok(None);
    };
    let tags = match target.object_id.as_deref() {
        Some(id) => list_object_tags(txn, id).await?,
        None => Vec::new(),
    };
    let versions = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq(&target.bucket))
        .filter(object_version::Column::Key.eq(&target.key));
    let public_version_count = versions.clone().count(txn).await?;
    let newer_noncurrent_count = versions
        .filter(object_version::Column::IsLatest.eq(false))
        .filter(object_version::Column::Sequence.gt(target.sequence))
        .count(txn)
        .await?;
    let expected = evaluate_candidate(&LifecycleEvaluationContext {
        candidate: &candidate,
        tags: &tags,
        public_version_count,
        newer_noncurrent_count,
        config_revision: row.revision,
        configuration: &configuration,
        database_now: database_now(txn).await?,
    })?;
    if !expected
        .as_ref()
        .is_some_and(|expected| action_matches_expected(&action, expected))
    {
        return Ok(None);
    }
    match candidate {
        LifecycleCandidate::Version(candidate) => Ok(Some(candidate)),
        LifecycleCandidate::MultipartUpload(_) => Ok(None),
    }
}
