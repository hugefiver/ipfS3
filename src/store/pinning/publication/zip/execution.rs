use super::{CapturedZipPublication, ZipPublicationAuthority, capture::read_capture, invalid};
use crate::{
    error::AppResult,
    store::{
        entities::zip_batch,
        import::ownership::{self, StandardMutationGuard},
        object_version::PublicationResult,
        zip::{execution, import_intake},
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseTransaction, EntityTrait, QueryFilter,
    Statement,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub enum ZipExecutionSettlement {
    Direct,
    Import,
    Mpu { contract: String },
}

pub(super) async fn verify_ownership(
    tx: &DatabaseTransaction,
    request: &CapturedZipPublication,
    claim: &execution::Claim,
    source_guard: Option<&StandardMutationGuard>,
    settlement: &ZipExecutionSettlement,
) -> AppResult<execution::Snapshot> {
    let snapshot = execution::read(tx, &claim.batch_id)
        .await?
        .ok_or_else(invalid)?;
    if snapshot.id != request.batch_id
        || snapshot.owner != request.capture.origin().principal_id
        || snapshot.bucket != request.capture.bucket()
        || snapshot.source_key != request.capture.source_key()
        || snapshot.token != request.capture.controls().admission_token
        || snapshot.state != "admitted"
        || snapshot.input_sha256.is_none()
        || read_capture(&snapshot.captured_options)? != request.capture
        || (snapshot.source == "import") != matches!(settlement, ZipExecutionSettlement::Import)
        || (snapshot.source == "mpu") != matches!(settlement, ZipExecutionSettlement::Mpu { .. })
        || request.source.is_some() != source_guard.is_some()
    {
        return Err(invalid());
    }
    // Authenticated admission ID (not a caller-selected arbitrary policy origin).
    let origin = &request.capture.origin().request_id;
    if origin != &snapshot.id && origin != &format!("zip-v2-source:{}", snapshot.id) {
        return Err(invalid());
    }
    if let Some(source) = &request.source
        && (Some(source.cid.as_str()) != snapshot.input_art_cid.as_deref()
            || Some(source.logical_size) != snapshot.input_art_size
            || source.encrypted
            || source.multipart != (snapshot.source == "mpu"))
    {
        return Err(invalid());
    }
    let guards = execution::read_targets(tx, claim).await?;
    let keys = request
        .successes
        .iter()
        .map(|s| s.key.clone())
        .collect::<BTreeSet<_>>();
    if guards
        .iter()
        .map(|g| g.key.clone())
        .collect::<BTreeSet<_>>()
        != keys
    {
        return Err(invalid());
    }
    ownership::verify_zip_output_guards_in_transaction(
        tx,
        &snapshot.bucket,
        &snapshot.source_key,
        &guards.iter().map(|g| g.key.clone()).collect::<Vec<_>>(),
        &guards,
    )
    .await?;
    if let Some(guard) = source_guard {
        ownership::verify_standard_mutation_guard(
            tx,
            guard,
            &snapshot.bucket,
            &snapshot.source_key,
            &[],
        )
        .await?;
    }
    if matches!(settlement, ZipExecutionSettlement::Import) {
        let outputs = request
            .manifest
            .successful
            .iter()
            .map(|f| (f.relative_path.as_str(), f.object_key.as_str()))
            .collect::<Vec<_>>();
        import_intake::verify_publication_in_transaction(tx, &snapshot, &outputs).await?;
    }
    if let ZipExecutionSettlement::Mpu { contract } = settlement {
        let intake = crate::store::multipart::v2_zip::read_by_upload(tx, &snapshot.id)
            .await?
            .ok_or_else(invalid)?;
        if intake.active_upload_id.as_deref() != Some(snapshot.id.as_str())
            || intake.execution_id != snapshot.id
            || intake.owner != snapshot.owner
            || intake.token != snapshot.token
            || intake.bucket != snapshot.bucket
            || intake.archive_key != snapshot.source_key
            || intake.captured_options != snapshot.captured_options
            || intake.request_fingerprint != snapshot.request_fingerprint
            || contract.is_empty()
        {
            return Err(invalid());
        }
    }
    Ok(snapshot)
}

pub(super) async fn verify_manifest(
    tx: &DatabaseTransaction,
    request: &CapturedZipPublication,
    snapshot: &execution::Snapshot,
) -> AppResult<()> {
    let row = zip_batch::Entity::find_by_id(&request.batch_id)
        .one(tx)
        .await?
        .ok_or_else(invalid)?;
    if row.source != snapshot.source
        || row.token != snapshot.token
        || row.captured_options != snapshot.captured_options
    {
        return Err(invalid());
    }
    let stored_guard =
        if let Some(generation) = row.input_identity.strip_prefix("zip-v2-source-gen:") {
            Some(StandardMutationGuard {
                bucket: snapshot.bucket.clone(),
                key: snapshot.source_key.clone(),
                mutation_id: format!("zip-v2-source:{}", snapshot.id),
                expected_generation: generation.parse().map_err(|_| invalid())?,
                mutation_prefix: None,
            })
        } else {
            None
        };
    let ZipPublicationAuthority::Execution {
        claim,
        source_guard,
        ..
    } = &request.authority
    else {
        return Err(invalid());
    };
    if &stored_guard != source_guard {
        return Err(invalid());
    }
    let placeholder = if tx.get_database_backend() == DatabaseBackend::Postgres {
        "$1"
    } else {
        "?"
    };
    let rows = tx.query_all(Statement::from_sql_and_values(tx.get_database_backend(),
        format!("SELECT path,object_key,cid,size,error_code FROM zip_v2_manifest WHERE batch_id={placeholder}"),
        [request.batch_id.clone().into()])).await?;
    let mut successes = BTreeSet::new();
    let mut legacy = BTreeMap::new();
    for row in crate::store::entities::zip_manifest_entry::Entity::find()
        .filter(crate::store::entities::zip_manifest_entry::Column::BatchId.eq(&request.batch_id))
        .all(tx)
        .await?
    {
        legacy.insert(row.path.clone(), row);
    }
    if legacy.len() != rows.len() {
        return Err(invalid());
    }
    for row in rows {
        let path: String = row.try_get("", "path")?;
        let key: Option<String> = row.try_get("", "object_key")?;
        let cid: Option<String> = row.try_get("", "cid")?;
        let size: Option<i64> = row.try_get("", "size")?;
        let code: Option<String> = row.try_get("", "error_code")?;
        let mirror = legacy.get(&path).ok_or_else(invalid)?;
        if mirror.object_key != key
            || mirror.cid != cid
            || mirror.size != size
            || mirror.error_code != code
        {
            return Err(invalid());
        }
        if let Some(key) = key {
            successes.insert((
                path,
                key,
                cid.ok_or_else(invalid)?,
                size.ok_or_else(invalid)?,
            ));
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
        .collect();
    if successes != expected {
        return Err(invalid());
    }
    execution::verify_in_transaction(tx, claim).await?;
    Ok(())
}

pub(super) async fn terminal_result(
    tx: &DatabaseTransaction,
    request: &CapturedZipPublication,
    results: &[(String, PublicationResult)],
) -> AppResult<String> {
    if !matches!(request.authority, ZipPublicationAuthority::Execution { .. }) {
        return Ok(request.terminal_result.clone());
    }
    let mut terminal: serde_json::Value =
        serde_json::from_str(&request.terminal_result).map_err(|_| invalid())?;
    if let Some(source) = &request.source {
        let result = results
            .iter()
            .find(|(_, r)| r.object_id == source.id)
            .ok_or_else(invalid)?;
        let binding =
            crate::store::zip::binding_for_published_object(tx, "source", &source.id).await?;
        let map = terminal.as_object_mut().ok_or_else(invalid)?;
        map.insert("source_cid".into(), source.cid.clone().into());
        map.insert("source_size".into(), source.logical_size.into());
        map.insert(
            "source_version_row_id".into(),
            binding.version_row_id.into(),
        );
        map.insert(
            "source_version_id".into(),
            serde_json::to_value(&result.1.version_id).map_err(|_| invalid())?,
        );
        // Kept for old response/recovery readers, not executable authorization.
        let policy = request
            .capture
            .source_decision()
            .replay_policy(request.source_tags.clone())
            .map_err(|_| invalid())?;
        map.insert(
            "source_policy".into(),
            serde_json::json!({"tags": policy.tags, "leases": policy.leases}),
        );
    }
    Ok(terminal.to_string())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn settle(
    tx: &DatabaseTransaction,
    request: &CapturedZipPublication,
    snapshot: &execution::Snapshot,
    claim: &execution::Claim,
    source_guard: Option<&StandardMutationGuard>,
    settlement: &ZipExecutionSettlement,
    results: &[(String, PublicationResult)],
    terminal: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    if let Some(guard) = source_guard {
        ownership::complete_standard_mutation_in_transaction(tx, guard, now).await?;
    }
    execution::complete_in_transaction(tx, claim, terminal).await?;
    match settlement {
        ZipExecutionSettlement::Import => {
            import_intake::complete_publication_in_transaction(tx, snapshot, claim, terminal)
                .await?
        }
        ZipExecutionSettlement::Mpu { contract } => {
            let source_result = request
                .source
                .as_ref()
                .map(|source| {
                    results
                        .iter()
                        .find(|(_, r)| r.object_id == source.id)
                        .map(|(_, result)| (source.cid.as_str(), result))
                        .ok_or_else(invalid)
                })
                .transpose()?;
            crate::store::multipart::v2_zip::finalize_complete_in_transaction(
                tx,
                claim,
                contract,
                source_result,
            )
            .await?;
        }
        ZipExecutionSettlement::Direct => {}
    }
    Ok(())
}
