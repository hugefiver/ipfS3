//! Decision storage is opt-in: absent legacy rows never imply executable intent.
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, QueryFilter, QuerySelect, Set,
};

use crate::{
    error::{AppError, AppResult},
    pinning::decision::{DecisionEffect, ExtensionDecision},
    store::entities::{object_version, pin_extension_decision},
};

/// Caller must hold the publication/tag transaction. Neither this nor read_for_version
/// reparses raw object tags. Fails closed if object_id does not name exactly one version.
pub async fn write_for_object_in_transaction<C: ConnectionTrait>(
    db: &C,
    object_id: &str,
    decision: &ExtensionDecision,
) -> AppResult<String> {
    let current_tags = super::tags::list_object_tags(db, object_id).await?;
    decision
        .replay_policy(current_tags)
        .map_err(|_| AppError::Internal("extension decision does not match version tags".into()))?;
    let rows = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(object_id))
        .all(db)
        .await?;
    let [row] = rows.as_slice() else {
        return Err(AppError::Internal(
            "extension decision requires exactly one internal object version".into(),
        ));
    };
    if row.kind != "object" {
        return Err(AppError::Internal(
            "decision target must be a content version".into(),
        ));
    }
    let snapshot = serde_json::to_string(decision)
        .map_err(|_| AppError::Internal("extension decision serialization failed".into()))?;
    // A repeated control_revision is a conflict, not a license to attach an old
    // captured request to a different version. Tag workers should capture anew.
    pin_extension_decision::Entity::insert(pin_extension_decision::ActiveModel {
        version_row_id: Set(row.id.clone()),
        object_id: Set(object_id.to_owned()),
        control_revision: Set(decision.control_revision.clone()),
        config_revision: Set(decision.config_revision.clone()),
        effect: Set(match decision.effect {
            DecisionEffect::Accepted => "accepted",
            DecisionEffect::Skipped => "skipped",
            DecisionEffect::NoIntent => "no_intent",
        }
        .to_owned()),
        snapshot: Set(snapshot),
    })
    .exec(db)
    .await?;
    Ok(row.id.clone())
}

/// For an explicit tag revision on the *same* object version; call in the tag
/// replacement transaction. Legacy absent rows remain absent until opt-in capture.
pub async fn replace_for_version_in_transaction<C: ConnectionTrait>(
    db: &C,
    version_row_id: &str,
    object_id: &str,
    decision: &ExtensionDecision,
) -> AppResult<()> {
    let query = object_version::Entity::find_by_id(version_row_id.to_owned());
    let row = if db.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(db).await?
    } else {
        query.one(db).await?
    };
    let Some(row) =
        row.filter(|row| row.object_id.as_deref() == Some(object_id) && row.kind == "object")
    else {
        return Err(AppError::Internal(
            "extension decision target version mismatch".into(),
        ));
    };
    pin_extension_decision::Entity::delete_by_id(row.id.clone())
        .exec(db)
        .await?;
    write_for_object_in_transaction(db, object_id, decision).await?;
    Ok(())
}

/// `None` is **legacy/unknown**, never infer Accepted from raw object tags.
pub async fn read_for_version<C: ConnectionTrait>(
    db: &C,
    version_row_id: &str,
) -> AppResult<Option<ExtensionDecision>> {
    let Some(row) = pin_extension_decision::Entity::find_by_id(version_row_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let decision: ExtensionDecision = serde_json::from_str(&row.snapshot)
        .map_err(|_| AppError::Internal("invalid stored extension decision".into()))?;
    decision
        .validate_snapshot()
        .map_err(|_| AppError::Internal("invalid stored extension decision".into()))?;
    let effect = match row.effect.as_str() {
        "accepted" => DecisionEffect::Accepted,
        "skipped" => DecisionEffect::Skipped,
        "no_intent" => DecisionEffect::NoIntent,
        _ => return Err(AppError::Internal("invalid stored extension effect".into())),
    };
    if effect != decision.effect
        || row.config_revision != decision.config_revision
        || row.control_revision != decision.control_revision
    {
        return Err(AppError::Internal(
            "stored extension decision metadata mismatch".into(),
        ));
    }
    let version = object_version::Entity::find_by_id(version_row_id.to_owned())
        .one(db)
        .await?;
    if !version.is_some_and(|version| {
        version.kind == "object" && version.object_id.as_deref() == Some(row.object_id.as_str())
    }) {
        return Err(AppError::Internal(
            "stored decision version owner mismatch".into(),
        ));
    }
    Ok(Some(decision))
}
