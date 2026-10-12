use super::{CapturedZipPublication, ZipPublicationAuthority, invalid};
use crate::{
    error::{AppError, AppResult},
    pinning::zip_policy::{CapturedZipIntent, decode_legacy_v1_intent, decode_legacy_v2_intent},
    store::entities::{zip_batch, zip_manifest_entry},
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseTransaction, EntityTrait, QueryFilter,
    QuerySelect,
};
use std::collections::BTreeSet;

/// Historical decoders require their original evidence, never current rules.
pub(in crate::store::pinning::publication) fn read_capture(
    json: &str,
) -> AppResult<CapturedZipIntent> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|_| invalid())?;
    if let Some(capture) = value.get("zip_intent") {
        return CapturedZipIntent::decode_json(&capture.to_string()).map_err(|_| invalid());
    }
    if value.get("schema_version").is_some() {
        return CapturedZipIntent::decode_json(json).map_err(|_| invalid());
    }
    let old = value.get("legacy_zip_intent").unwrap_or(&value).to_string();
    decode_legacy_v1_intent(&old)
        .or_else(|_| decode_legacy_v2_intent(&old))
        .map_err(|_| invalid())
}

pub(super) async fn verify_batch(
    tx: &DatabaseTransaction,
    request: &CapturedZipPublication,
) -> AppResult<()> {
    let query = zip_batch::Entity::find_by_id(&request.batch_id);
    let row = if tx.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(tx).await?
    } else {
        query.one(tx).await?
    }
    .ok_or_else(invalid)?;
    if row.state != "open"
        || !row.manifest_prepared
        || row.source_published
        || row.bucket != request.capture.bucket()
        || row.archive_key != request.capture.source_key()
        || row.owner != request.capture.origin().principal_id
        || read_capture(&row.captured_options)? != request.capture
    {
        return Err(invalid());
    }
    match &request.authority {
        ZipPublicationAuthority::Legacy(authority) => {
            let source = if authority.upload_target.is_some() {
                "mpu"
            } else if authority.import_guard.is_some() {
                "import"
            } else {
                "direct"
            };
            if row.source != source
                || authority
                    .upload_target
                    .as_ref()
                    .is_some_and(|t| row.token != t.upload_id || row.id != t.upload_id)
                || authority
                    .import_guard
                    .as_ref()
                    .is_some_and(|g| row.token != g.job_id)
            {
                return Err(invalid());
            }
        }
        ZipPublicationAuthority::Execution { claim, .. } => {
            if row.id != claim.batch_id
                || row.fingerprint != "pending"
                || row.token != request.capture.controls().admission_token
                || (row.input_identity != "pending"
                    && !row.input_identity.starts_with("zip-v2-source-gen:"))
            {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

pub(super) async fn verify_manifest(
    tx: &DatabaseTransaction,
    request: &CapturedZipPublication,
) -> AppResult<()> {
    let query = zip_manifest_entry::Entity::find()
        .filter(zip_manifest_entry::Column::BatchId.eq(&request.batch_id));
    let rows = if tx.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_shared().all(tx).await?
    } else {
        query.all(tx).await?
    };
    let mut successes = BTreeSet::new();
    for row in rows {
        if row.version_row_id.is_some() {
            return Err(invalid());
        }
        match (row.object_key, row.cid, row.size, row.error_code) {
            (Some(key), Some(cid), Some(size), None) => {
                successes.insert((row.path, key, cid, size));
            }
            (None, None, None, Some(_)) => {}
            _ => return Err(invalid()),
        }
    }
    let expected = request
        .manifest
        .successful
        .iter()
        .map(|f| {
            (
                f.relative_path.clone(),
                f.object_key.clone(),
                f.cid.clone(),
                f.size,
            )
        })
        .collect::<BTreeSet<_>>();
    if successes != expected {
        return Err(AppError::Internal(
            "stale ZIP batch manifest binding".into(),
        ));
    }
    Ok(())
}
