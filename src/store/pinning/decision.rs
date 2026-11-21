use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, sea_query::Expr,
};

use crate::{
    error::{AppError, AppResult},
    pinning::{decision::ExtensionDecision, tags::ObjectTag},
    store::entities::object_version,
};

pub async fn write_for_object_in_transaction<C: ConnectionTrait>(
    db: &C,
    object_id: &str,
    decision: &ExtensionDecision,
) -> AppResult<String> {
    let versions: Vec<String> = object_version::Entity::find()
        .select_only()
        .column(object_version::Column::Id)
        .filter(object_version::Column::ObjectId.eq(object_id))
        .limit(2)
        .into_tuple()
        .all(db)
        .await?;
    let [version_id] = versions.as_slice() else {
        return Err(AppError::Internal(
            "extension decision requires exactly one content version".into(),
        ));
    };
    replace_for_version_in_transaction(db, version_id, object_id, decision).await?;
    Ok(version_id.clone())
}

pub async fn replace_for_version_in_transaction<C: ConnectionTrait>(
    db: &C,
    version_row_id: &str,
    object_id: &str,
    decision: &ExtensionDecision,
) -> AppResult<()> {
    let tags = super::tags::list_object_tags(db, object_id).await?;
    replace_for_version_with_tags_in_transaction(db, version_row_id, object_id, decision, tags)
        .await
}

pub(crate) async fn replace_for_version_with_tags_in_transaction<C: ConnectionTrait>(
    db: &C,
    version_row_id: &str,
    object_id: &str,
    decision: &ExtensionDecision,
    tags: Vec<ObjectTag>,
) -> AppResult<()> {
    decision
        .replay_policy(tags)
        .map_err(|_| AppError::Internal("extension decision does not match version tags".into()))?;
    let snapshot = serde_json::to_string(decision)
        .map_err(|_| AppError::Internal("extension decision serialization failed".into()))?;
    let result = object_version::Entity::update_many()
        .col_expr(object_version::Column::PinDecision, Expr::value(snapshot))
        .filter(object_version::Column::Id.eq(version_row_id))
        .filter(object_version::Column::ObjectId.eq(object_id))
        .filter(object_version::Column::Kind.eq("object"))
        .exec(db)
        .await?;
    if result.rows_affected != 1 {
        return Err(AppError::Internal(
            "extension decision target version mismatch".into(),
        ));
    }
    Ok(())
}

/// Absent decisions never authorize replay of raw pin control tags.
pub async fn read_for_version<C: ConnectionTrait>(
    db: &C,
    version_row_id: &str,
) -> AppResult<Option<ExtensionDecision>> {
    let snapshot: Option<Option<String>> = object_version::Entity::find_by_id(version_row_id)
        .select_only()
        .column(object_version::Column::PinDecision)
        .into_tuple()
        .one(db)
        .await?;
    snapshot
        .flatten()
        .map(|json| {
            let invalid = || AppError::Internal("invalid stored extension decision".into());
            let decision: ExtensionDecision = serde_json::from_str(&json).map_err(|_| invalid())?;
            decision.validate_snapshot().map_err(|_| invalid())?;
            Ok(decision)
        })
        .transpose()
}
