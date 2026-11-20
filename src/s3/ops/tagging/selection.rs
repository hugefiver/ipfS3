use sea_orm::DatabaseTransaction;

use crate::{
    error::{AppError, AppResult},
    iam::{Action, Context, Resource},
    pinning::tags::ObjectTag,
    state::AppState,
    store::{
        object_version::{
            ResolvedVersion, VersionSelector, lock_resolved_version, resolve_version,
        },
        pinning::tags,
    },
};

pub(super) async fn authorized_tags(
    state: &AppState,
    txn: &DatabaseTransaction,
    (bucket, key): (&str, &str),
    principal: &str,
    action: Action,
    selector: &VersionSelector,
    request_tags: Option<&[ObjectTag]>,
) -> AppResult<(ResolvedVersion, Vec<ObjectTag>)> {
    let requested_version = match selector {
        VersionSelector::Current => None,
        VersionSelector::Exact(version) => Some(version.as_s3_str()),
    };
    let context = Context {
        action,
        resource: Resource::Object(key),
        version: requested_version,
        request_tags,
        existing_tags: None,
    };
    let scope = state.credentials.policies.scope(principal, Some(bucket));
    let needs_tags = scope.needs_existing_tags(&context);
    let denied = || AppError::AccessDenied("access denied by policy".into());
    if !needs_tags && !scope.allows(&context) {
        return Err(denied());
    }

    let finish = |stored: &[ObjectTag]| -> AppResult<()> {
        if needs_tags {
            if !scope.allows(&Context {
                existing_tags: Some(stored),
                ..context
            }) {
                return Err(denied());
            }
        }
        Ok(())
    };

    let selected = if matches!(
        action,
        Action::PutObjectTagging | Action::DeleteObjectTagging
    ) {
        lock_resolved_version(txn, bucket, key, selector).await
    } else {
        resolve_version(txn, bucket, key, selector).await
    };
    let selected = match selected {
        Ok(selected) => selected,
        Err(error) => {
            if matches!(
                error,
                AppError::NoSuchKey(_)
                    | AppError::NoSuchVersion { .. }
                    | AppError::NoSuchBucket(_)
                    | AppError::InvalidArgument(_)
            ) {
                finish(&[])?;
            }
            return Err(error);
        }
    };
    let stored = match selected.object.as_ref() {
        Some(owner) if action != Action::GetPinningStatus || needs_tags => {
            tags::list_object_tags(txn, &owner.id).await?
        }
        _ => Vec::new(),
    };
    finish(&stored)?;
    Ok((selected, stored))
}
