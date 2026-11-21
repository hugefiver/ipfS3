mod selection;
pub(crate) mod status;

use std::sync::Arc;

use chrono::{DateTime, Utc};
use s3s::{S3Request, S3Response, S3Result, dto::*};
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction,
    IsolationLevel, TransactionTrait,
};

use crate::{
    error::{AppError, AppResult},
    iam::Action,
    pinning::{
        decision::{DecisionEffect, DecisionOrigin, ExtensionDecision, WarningCode},
        policy::{ManualLeaseMutation, PublicationContext, PublicationPolicy},
        tags::{ObjectTag, PinControl, validate_tag_set},
    },
    state::AppState,
    store::{
        entities::object,
        object_version::{
            BucketVersioningState, PublicVersionId, ResolvedVersion, VersionKind, VersionSelector,
        },
        pinning::{decision, resources, tags},
    },
};

const INVALID_TAG_SET: &str = "invalid object tag set";

async fn begin_tag_snapshot(db: &DatabaseConnection) -> AppResult<DatabaseTransaction> {
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        Ok(db.begin().await?)
    } else {
        Ok(db
            .begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await?)
    }
}

pub async fn get_object_tagging(
    state: &Arc<AppState>,
    req: S3Request<GetObjectTaggingInput>,
) -> S3Result<S3Response<GetObjectTaggingOutput>> {
    let principal = super::object::principal_id(state, &req)?;
    let input = req.input;
    let selector = version_selector(input.version_id.as_deref())?;
    let txn = begin_tag_snapshot(state.store.db()).await?;
    let (resolved, stored) = selection::authorized_tags(
        state,
        &txn,
        (&input.bucket, &input.key),
        &principal,
        Action::GetObjectTagging,
        &selector,
        None,
    )
    .await?;
    selected_content_owner(&resolved, matches!(selector, VersionSelector::Current))?;
    let version_id = response_version_id(&txn, &input.bucket, &resolved).await?;
    txn.commit().await.map_err(AppError::from)?;
    let tag_set = stored
        .into_iter()
        .map(|tag| Tag {
            key: Some(tag.key),
            value: Some(tag.value),
        })
        .collect();

    Ok(S3Response::new(GetObjectTaggingOutput {
        tag_set,
        version_id,
    }))
}

pub async fn put_object_tagging(
    state: &Arc<AppState>,
    req: S3Request<PutObjectTaggingInput>,
) -> S3Result<S3Response<PutObjectTaggingOutput>> {
    put_object_tagging_at(state, req, Utc::now()).await
}

pub(crate) async fn put_object_tagging_at(
    state: &Arc<AppState>,
    req: S3Request<PutObjectTaggingInput>,
    now: DateTime<Utc>,
) -> S3Result<S3Response<PutObjectTaggingOutput>> {
    let principal = super::object::principal_id(state, &req)?;
    let input = req.input;
    let selector = version_selector(input.version_id.as_deref())?;
    let replacement = dto_to_object_tags(input.tagging.tag_set).map_err(s3s::S3Error::from)?;
    validate_replacement(&replacement).map_err(s3s::S3Error::from)?;
    let (version_id, warning) = replace_tag_set(
        state,
        input.bucket,
        input.key,
        selector,
        Some(replacement),
        principal,
        now,
    )
    .await
    .map_err(s3s::S3Error::from)?;
    Ok(S3Response::with_headers(
        PutObjectTaggingOutput { version_id },
        super::object::pin_warning_headers(warning),
    ))
}

pub async fn delete_object_tagging(
    state: &Arc<AppState>,
    req: S3Request<DeleteObjectTaggingInput>,
) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
    delete_object_tagging_at(state, req, Utc::now()).await
}

pub(crate) async fn delete_object_tagging_at(
    state: &Arc<AppState>,
    req: S3Request<DeleteObjectTaggingInput>,
    now: DateTime<Utc>,
) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
    let principal = super::object::principal_id(state, &req)?;
    let input = req.input;
    let selector = version_selector(input.version_id.as_deref())?;
    let (version_id, _) = replace_tag_set(
        state,
        input.bucket,
        input.key,
        selector,
        None,
        principal,
        now,
    )
    .await
    .map_err(s3s::S3Error::from)?;
    Ok(S3Response::new(DeleteObjectTaggingOutput { version_id }))
}

fn dto_to_object_tags(tag_set: Vec<Tag>) -> AppResult<Vec<ObjectTag>> {
    tag_set
        .into_iter()
        .map(|tag| {
            let key = tag
                .key
                .ok_or_else(|| invalid_pinning_request(INVALID_TAG_SET))?;
            let value = tag
                .value
                .ok_or_else(|| invalid_pinning_request(INVALID_TAG_SET))?;
            Ok(ObjectTag::new(key, value))
        })
        .collect()
}

fn validate_replacement(replacement: &[ObjectTag]) -> AppResult<()> {
    validate_tag_set(replacement).map_err(|_| invalid_pinning_request(INVALID_TAG_SET))?;
    PinControl::from_tags(replacement).map_err(|_| invalid_pinning_request(INVALID_TAG_SET))?;
    Ok(())
}

fn capture_tag_decision(
    state: &AppState,
    (bucket, key): (&str, &str),
    replacement: &[ObjectTag],
    current_tags: &[ObjectTag],
    previous: Option<&ExtensionDecision>,
    has_manual_lease: bool,
    principal: &str,
) -> AppResult<ExtensionDecision> {
    let control =
        PinControl::from_tags(replacement).map_err(|_| invalid_pinning_request(INVALID_TAG_SET))?;
    let origin = DecisionOrigin::new(principal, uuid::Uuid::new_v4().to_string());
    let policy = PublicationPolicy {
        tags: replacement.to_vec(),
        // Replacing tags is not a publication. Existing automatic work persists
        // independently, but no new lease/job/quota is admitted here.
        leases: previous.map_or_else(Vec::new, |decision| {
            decision
                .effective_intents
                .iter()
                .filter(|intent| intent.source == crate::pinning::policy::LeaseSource::Automatic)
                .cloned()
                .collect()
        }),
    };
    if !has_manual_lease && !matches!(control, PinControl::Absent) {
        let old_control = match previous {
            Some(previous) if previous.legacy_unknown => Some(previous.control.clone()),
            None => Some(
                PinControl::from_tags(current_tags)
                    .map_err(|_| invalid_pinning_request(INVALID_TAG_SET))?,
            ),
            _ => None,
        };
        if let Some(old_control) = old_control.filter(|old| !matches!(old, PinControl::Absent)) {
            if old_control == control {
                return ExtensionDecision::capture_legacy_unknown(origin, control, &policy)
                    .map_err(invalid_pinning_request);
            }
            if matches!(control, PinControl::Request { .. }) {
                return Err(invalid_pinning_request(
                    "an inherited control cannot be reapplied by tagging",
                ));
            }
        }
    }
    let warning = match (&control, previous) {
        (PinControl::Request { .. }, Some(previous))
            if previous.effect == DecisionEffect::Skipped =>
        {
            if previous.control != control {
                return Err(invalid_pinning_request(
                    "a skipped control cannot be reapplied by tagging",
                ));
            }
            previous.warning
        }
        (PinControl::Request { .. }, _) if !has_manual_lease => {
            let (_, attempted) = state
                .pinning
                .policy()
                .evaluate_publication_decision(
                    PublicationContext {
                        bucket,
                        key,
                        tags: replacement,
                        is_decompress_zip: false,
                    },
                    origin.clone(),
                )
                .map_err(AppError::from)?;
            (attempted.effect == DecisionEffect::Skipped)
                .then_some(attempted.warning)
                .flatten()
        }
        _ => None,
    };
    let effect = if warning.is_some() {
        DecisionEffect::Skipped
    } else {
        DecisionEffect::NoIntent
    };
    let decision = ExtensionDecision::capture(origin, effect, warning, control, &policy)
        .map_err(invalid_pinning_request)?;
    Ok(decision)
}

fn version_selector(version_id: Option<&str>) -> AppResult<VersionSelector> {
    match version_id {
        Some(version_id) => Ok(VersionSelector::Exact(PublicVersionId::parse_s3(
            version_id,
        )?)),
        None => Ok(VersionSelector::Current),
    }
}

fn selected_content_owner(resolved: &ResolvedVersion, current: bool) -> AppResult<&object::Model> {
    match resolved.kind {
        VersionKind::DeleteMarker => Err(AppError::DeleteMarker {
            version_id: resolved.public_version_id.clone(),
            created_at: resolved.created_at,
            current,
        }),
        VersionKind::Object => resolved.object.as_ref().ok_or_else(|| {
            AppError::Internal("object version index is missing its object".to_owned())
        }),
    }
}

async fn response_version_id<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    resolved: &ResolvedVersion,
) -> AppResult<Option<String>> {
    Ok(
        (crate::store::bucket::get_versioning_state(db, bucket).await?
            != BucketVersioningState::Unversioned)
            .then_some(resolved.public_version_id.clone()),
    )
}

async fn replace_tag_set(
    state: &Arc<AppState>,
    bucket: String,
    key: String,
    selector: VersionSelector,
    replacement: Option<Vec<ObjectTag>>,
    principal: String,
    now: DateTime<Utc>,
) -> AppResult<(Option<String>, Option<WarningCode>)> {
    let db = state.store.db();
    let txn = db.begin().await?;
    let action = if replacement.is_some() {
        Action::PutObjectTagging
    } else {
        Action::DeleteObjectTagging
    };
    let result: AppResult<_> = async {
        let (selected, current_tags) = selection::authorized_tags(
            state,
            &txn,
            (&bucket, &key),
            &principal,
            action,
            &selector,
            replacement.as_deref(),
        )
        .await?;
        let replacement = replacement.unwrap_or_default();
        let owner =
            selected_content_owner(&selected, matches!(selector, VersionSelector::Current))?;
        let version_id = response_version_id(&txn, &bucket, &selected).await?;
        let previous = decision::read_for_version(&txn, &selected.id).await?;
        let manual = resources::manual(&txn, &owner.id, now).await?;
        let decision = capture_tag_decision(
            state,
            (&bucket, &key),
            &replacement,
            &current_tags,
            previous.as_ref(),
            manual.is_some(),
            &principal,
        )?;
        let mutation = match state.pinning.policy().evaluate_tag_replacement(
            &bucket,
            &key,
            manual.as_ref(),
            &replacement,
            now,
        ) {
            Ok(mutation) => mutation,
            Err(_)
                if manual.is_none()
                    && (decision.effect == DecisionEffect::Skipped || decision.legacy_unknown) =>
            {
                ManualLeaseMutation::Keep
            }
            Err(error) => return Err(error.into()),
        };
        resources::replace_manual(&txn, &owner.id, &mutation, now).await?;
        tags::replace_object_tags(&txn, &owner.id, &replacement).await?;
        decision::replace_for_version_with_tags_in_transaction(
            &txn,
            &selected.id,
            &owner.id,
            &decision,
            replacement,
        )
        .await?;
        Ok((version_id, decision.warning))
    }
    .await;
    match result {
        Ok(result) => {
            txn.commit().await?;
            Ok(result)
        }
        Err(error) => {
            txn.rollback().await?;
            // A SQLite snapshot can see a deleted version until the write upgrade fails.
            if matches!(selector, VersionSelector::Exact(_))
                && matches!(error, AppError::Database(_))
                && let Err(missing @ AppError::NoSuchVersion { .. }) =
                    crate::store::object_version::resolve_version(db, &bucket, &key, &selector)
                        .await
            {
                return Err(missing);
            }
            Err(error)
        }
    }
}

fn invalid_pinning_request(message: &str) -> AppError {
    AppError::InvalidPinningRequest(message.to_owned())
}

#[cfg(test)]
mod tests;
