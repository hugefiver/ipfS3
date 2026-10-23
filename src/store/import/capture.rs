//! Frozen publication inputs without source descriptors, metadata, or tags.
use super::{jobs, requests};
use crate::{
    error::AppResult,
    pinning::decision::ExtensionDecision,
    store::entities::import_job::{self, Column},
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, FromQueryResult, QueryFilter, QuerySelect,
    sea_query::Expr,
};

#[derive(FromQueryResult)]
struct Location {
    bucket: String,
    key: String,
}

pub(crate) struct CapturedImport {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub decompress_prefix: Option<String>,
    pub root_capture_json: Option<String>,
    pub zip_intent_json: Option<String>,
    pub pin_decision_json: Option<String>,
}

impl CapturedImport {
    pub(crate) fn decision(&self) -> AppResult<Option<ExtensionDecision>> {
        jobs::decode_decision(
            (&self.id, &self.bucket, &self.key),
            jobs::DecisionFields {
                prefix: self.decompress_prefix.as_deref(),
                root: self.root_capture_json.as_deref(),
                zip: self.zip_intent_json.as_deref(),
                pin: self.pin_decision_json.as_deref(),
            },
        )
    }
}

/// The caller owns the transaction and its ownership fence. Locks follow the
/// claim order: hot state first, frozen inputs second.
pub(crate) async fn load<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> AppResult<Option<CapturedImport>> {
    let mut query = import_job::Entity::find_state().select_only();
    for (column, name) in [(Column::Bucket, "bucket"), (Column::Key, "key")] {
        let bytes = if db.get_database_backend() == DatabaseBackend::Sqlite {
            format!("length(CAST(\"{name}\" AS BLOB))")
        } else {
            format!("octet_length(\"{name}\")")
        };
        query = query.column_as(
            Expr::cust(format!("CASE WHEN {bytes}<=65536 THEN \"{name}\" END")),
            column,
        );
    }
    query = query.filter(Column::Id.eq(id));
    if db.get_database_backend() == DatabaseBackend::Postgres {
        query = query.lock_shared();
    }
    let Some(location) = query.into_model::<Location>().one(db).await? else {
        return Ok(None);
    };
    let mut rows = requests::read_cells(
        db,
        &[id.to_owned()],
        &[
            Column::DecompressPrefix,
            Column::RootCaptureJson,
            Column::ZipIntentJson,
            Column::PinDecisionJson,
        ],
    )
    .await?;
    let row = rows
        .remove(id)
        .ok_or(crate::error::AppError::NoSuchImportJob)?;
    let [
        decompress_prefix,
        root_capture_json,
        zip_intent_json,
        pin_decision_json,
    ]: [Option<String>; 4] = row
        .try_into()
        .map_err(|_| crate::error::AppError::InvalidImportRequest)?;
    Ok(Some(CapturedImport {
        id: id.to_owned(),
        bucket: location.bucket,
        key: location.key,
        decompress_prefix,
        root_capture_json,
        zip_intent_json,
        pin_decision_json,
    }))
}
