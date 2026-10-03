use std::collections::{HashMap, HashSet};

use chrono::Utc;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseTransaction,
    EntityTrait, QueryFilter, QuerySelect, Set,
};

use super::{
    invalid, lock_batch, manifest,
    root::{self, RootClaim},
    safe_code, stale,
};
use crate::{
    error::AppResult,
    store::entities::{object, object_version, zip_batch, zip_manifest_entry, zip_root_reference},
};

#[derive(Clone, Debug)]
pub struct VersionBinding {
    pub path: String,
    /// Internal `object_versions.id`, NOT the nullable public S3 VersionId.
    pub version_row_id: String,
}

/// Adapter for the existing publication helper, which returns a newly inserted
/// immutable object ID but only the *public* VersionId. Resolve the private row
/// in the same transaction by exact object ID, never by current key/public null ID.
pub async fn binding_for_published_object(
    tx: &DatabaseTransaction,
    path: &str,
    object_id: &str,
) -> AppResult<VersionBinding> {
    if !super::required(path) || !super::required(object_id) {
        return Err(invalid());
    }
    let query = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(object_id))
        .limit(2);
    let rows = if tx.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_shared().all(tx).await?
    } else {
        query.all(tx).await?
    };
    if rows.len() != 1 || rows[0].kind != "object" {
        return Err(stale());
    }
    Ok(VersionBinding {
        path: path.into(),
        version_row_id: rows[0].id.clone(),
    })
}

#[derive(Clone, Debug)]
pub enum RootOutcome {
    Disabled,
    Empty,
    /// Only for an initial failure before any root claim was created.
    Failed {
        code: &'static str,
    },
    /// A failed build with an existing, still-owned claim. No CID is publishable.
    ClaimedFailed {
        claim: RootClaim,
        code: &'static str,
    },
    /// Only verified local root receipts can become externally visible.
    Verified {
        claim: RootClaim,
        node_identity: String,
        tier: String,
        cid: String,
    },
}

async fn lock_version(tx: &DatabaseTransaction, id: &str) -> AppResult<object_version::Model> {
    let query = object_version::Entity::find_by_id(id);
    let row = if tx.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_shared().one(tx).await?
    } else {
        query.one(tx).await?
    };
    row.ok_or_else(stale)
}

async fn lock_object(tx: &DatabaseTransaction, id: &str) -> AppResult<object::Model> {
    let query = object::Entity::find_by_id(id);
    let row = if tx.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_shared().one(tx).await?
    } else {
        query.one(tx).await?
    };
    row.ok_or_else(stale)
}

async fn bind_versions(
    tx: &DatabaseTransaction,
    batch: &zip_batch::Model,
    bindings: &[VersionBinding],
) -> AppResult<(usize, usize)> {
    let entries = manifest::entries(tx, &batch.id).await?;
    let successes = entries.iter().filter(|e| e.cid.is_some()).count();
    if successes != bindings.len() {
        return Err(invalid());
    }
    let failed = entries.len() - successes;
    let by_path: HashMap<_, _> = entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect();
    let mut seen = HashSet::new();
    let mut resolved = Vec::with_capacity(successes);
    for binding in bindings {
        if !seen.insert(&binding.path) {
            return Err(invalid());
        }
        let entry = by_path.get(binding.path.as_str()).ok_or_else(invalid)?;
        let key = entry.object_key.as_deref().ok_or_else(invalid)?;
        if entry.version_row_id.is_some() || binding.version_row_id.is_empty() {
            return Err(stale());
        }
        // Exact existence at publication; historical manifest keeps its immutable
        // ID/CID/size after lifecycle deletes the version or the current key moves.
        let version = lock_version(tx, &binding.version_row_id).await?;
        if version.kind != "object" || version.bucket != batch.bucket || version.key != key {
            return Err(stale());
        }
        let obj = lock_object(tx, version.object_id.as_deref().ok_or_else(stale)?).await?;
        if obj.bucket != batch.bucket
            || obj.key != key
            || Some(obj.cid.as_str()) != entry.cid.as_deref()
            || Some(obj.size) != entry.size
        {
            return Err(stale());
        }
        resolved.push(((*entry).clone(), binding.version_row_id.clone()));
    }
    // Complete exact-version validation before writing any manifest binding.
    for (entry, version_row_id) in resolved {
        zip_manifest_entry::ActiveModel {
            version_row_id: Set(Some(version_row_id)),
            ..entry.into()
        }
        .update(tx)
        .await?;
    }
    Ok((successes, failed))
}

async fn root_fields(
    tx: &DatabaseTransaction,
    batch: &zip_batch::Model,
    outcome: RootOutcome,
    successes: usize,
    failures: usize,
    failed_claim: Option<&RootClaim>,
) -> AppResult<(String, Option<String>, Option<String>)> {
    match outcome {
        RootOutcome::Disabled if batch.root_revision == 0 => Ok(("disabled".into(), None, None)),
        RootOutcome::Empty if successes == 0 && batch.root_revision == 0 => {
            Ok(("empty".into(), None, None))
        }
        RootOutcome::Failed { code } if successes > 0 && safe_code(code) => {
            if let Some(claim) = failed_claim {
                if claim.batch_id != batch.id {
                    return Err(stale());
                }
                let build = root::current_claim(tx, claim).await?;
                if matches!(build.status.as_str(), "unknown" | "failed") {
                    return Err(stale());
                }
            } else if batch.state != "open" || batch.root_revision != 0 {
                // A different live claimant can block a new MPU attempt. Publish
                // its objects without settling or releasing that claimant's lease.
                if code != "root_intent_failed" || batch.state != "open" || batch.root_revision == 0
                {
                    return Err(stale());
                }
                let build = crate::store::entities::zip_root_build::Entity::find_by_id((
                    batch.id.clone(),
                    batch.root_revision,
                    batch.root_epoch,
                ))
                .one(tx)
                .await?
                .ok_or_else(stale)?;
                if build.lease_until <= crate::store::database_clock::database_now(tx).await?
                    || !matches!(
                        build.status.as_str(),
                        "intent" | "invoked" | "verified" | "reconciling"
                    )
                {
                    return Err(stale());
                }
            }
            Ok(("failed".into(), Some(code.into()), None))
        }
        RootOutcome::ClaimedFailed { claim, code } if successes > 0 && safe_code(code) => {
            if claim.batch_id != batch.id
                || claim.revision != batch.root_revision
                || claim.epoch != batch.root_epoch
            {
                return Err(stale());
            }
            let build = root::current_claim(tx, &claim).await?;
            if matches!(build.status.as_str(), "unknown" | "failed") {
                return Err(stale());
            }
            Ok(("failed".into(), Some(code.into()), None))
        }
        RootOutcome::Verified {
            claim,
            node_identity,
            tier,
            cid,
        } if successes > 0 => {
            if claim.batch_id != batch.id
                || claim.revision != batch.root_revision
                || claim.epoch != batch.root_epoch
            {
                return Err(stale());
            }
            let build = root::current_claim(tx, &claim).await?;
            if build.status != "verified" {
                return Err(stale());
            }
            let found = root::reference(tx, &claim, &node_identity, &tier, &cid).await?;
            if found.state != "retained" || found.verification_receipt.is_none() {
                return Err(stale());
            }
            zip_root_reference::ActiveModel {
                state: Set("adopted".into()),
                updated_at: Set(Utc::now()),
                ..found.into()
            }
            .update(tx)
            .await?;
            let status = if failures > 0 { "partial" } else { "complete" };
            Ok((status.into(), None, Some(cid)))
        }
        _ => Err(invalid()),
    }
}

fn valid_terminal(result: &str) -> bool {
    !result.is_empty()
        && result.len() <= 1_048_576
        && serde_json::from_str::<serde_json::Value>(result).is_ok()
}

async fn verify_archive_binding(
    tx: &DatabaseTransaction,
    batch: &zip_batch::Model,
    terminal_result: &str,
    source_published: bool,
) -> AppResult<()> {
    if batch.source == "mpu" {
        // ZIP v2 MPU binds the verified CAT/add EOF to its execution row. A
        // source publication is authorized only by its admitted exact guard;
        // outputs-only must never turn this into an implicit source write.
        if let Some(intake) = crate::store::multipart::v2_zip::read_by_upload(tx, &batch.id).await?
        {
            let execution = crate::store::zip::execution::read(tx, &batch.id)
                .await?
                .ok_or_else(stale)?;
            let marker: serde_json::Value =
                serde_json::from_str(terminal_result).map_err(|_| stale())?;
            let options: crate::zip::options::ZipV2Options =
                serde_json::from_str(&intake.captured_options).map_err(|_| stale())?;
            if (execution.state == "admitted"
                && (batch.state != "open"
                    || batch.terminal_result.is_some()
                    || batch.source_published))
                || (execution.state == "completed"
                    && (batch.state != "published"
                        || batch.terminal_result.is_none()
                        || batch.source_published != source_published))
                || source_published != options.publish_source
                || (source_published
                    && (batch
                        .input_identity
                        .strip_prefix("zip-v2-source-gen:")
                        .and_then(|value| value.parse::<i64>().ok())
                        .is_none_or(|generation| generation < 1)
                        || marker.get("source_cid").and_then(|value| value.as_str())
                            != execution.input_art_cid.as_deref()
                        || marker.get("source_size").and_then(|value| value.as_i64())
                            != execution.input_art_size
                        || marker.get("source_version_id").is_none()))
                || (!source_published && batch.input_identity != "pending")
                || intake.execution_id != batch.id
                || intake.owner != batch.owner
                || intake.token != batch.token
                || intake.bucket != batch.bucket
                || intake.archive_key != batch.archive_key
                || intake.captured_options != batch.captured_options
                || execution.source != "mpu"
                || execution.owner != intake.owner
                || execution.token != intake.token
                || execution.bucket != intake.bucket
                || execution.source_key != intake.archive_key
                || execution.captured_options != intake.captured_options
                || execution.request_fingerprint != intake.request_fingerprint
                || execution.input_sha256.as_deref()
                    != marker.get("input_sha256").and_then(|value| value.as_str())
                || execution.input_sha256.is_none()
                || execution.input_art_cid.as_deref().is_none_or(str::is_empty)
                || execution.input_art_size.is_none_or(|size| size < 0)
                || !matches!(execution.state.as_str(), "admitted" | "completed")
            {
                return Err(stale());
            }
            if source_published && execution.state == "admitted" {
                let generation = batch
                    .input_identity
                    .strip_prefix("zip-v2-source-gen:")
                    .and_then(|value| value.parse::<i64>().ok())
                    .filter(|generation| *generation > 0)
                    .ok_or_else(stale)?;
                let guard = crate::store::import::ownership::StandardMutationGuard {
                    bucket: batch.bucket.clone(),
                    key: batch.archive_key.clone(),
                    mutation_id: format!("zip-v2-source:{}", batch.id),
                    expected_generation: generation,
                    mutation_prefix: None,
                };
                crate::store::import::ownership::verify_standard_mutation_guard(
                    tx,
                    &guard,
                    &batch.bucket,
                    &batch.archive_key,
                    &[],
                )
                .await?;
                crate::store::multipart::v2_zip::verify_published_source(tx, &execution, &marker)
                    .await?;
            } else if !source_published
                && [
                    "source_version_id",
                    "source_version_row_id",
                    "source_cid",
                    "source_size",
                ]
                .iter()
                .any(|field| marker.get(field).is_some())
            {
                return Err(stale());
            }
            if execution.state == "completed" {
                // Root-only recovery cannot reauthorize a source mutation. Its
                // version may have been deleted and its guard is already done;
                // preserve the immutable binding from the committed batch and
                // execution instead of consulting today's source projection.
                let original: serde_json::Value =
                    serde_json::from_str(batch.terminal_result.as_deref().ok_or_else(stale)?)
                        .map_err(|_| stale())?;
                let completed: serde_json::Value =
                    serde_json::from_str(execution.terminal_result.as_deref().ok_or_else(stale)?)
                        .map_err(|_| stale())?;
                if [
                    "input_sha256",
                    "source_cid",
                    "source_size",
                    "source_version_id",
                    "source_version_row_id",
                    "source_policy",
                    "published_count",
                    "failed_count",
                    "status",
                ]
                .iter()
                .any(|field| {
                    marker.get(field) != original.get(field)
                        || original.get(field) != completed.get(field)
                }) {
                    return Err(stale());
                }
            }
            let backend = tx.get_database_backend();
            let sql = if backend == DatabaseBackend::Postgres {
                "SELECT owner,token,execution_id,bucket,archive_key,part_contract,response_xml FROM zip_v2_mpu_completions WHERE upload_id=$1"
            } else {
                "SELECT owner,token,execution_id,bucket,archive_key,part_contract,response_xml FROM zip_v2_mpu_completions WHERE upload_id=?"
            };
            let row = tx
                .query_one(sea_orm::Statement::from_sql_and_values(
                    backend,
                    sql,
                    [batch.id.clone().into()],
                ))
                .await?
                .ok_or_else(stale)?;
            if row.try_get::<String>("", "owner")? != intake.owner
                || row.try_get::<String>("", "token")? != intake.token
                || row.try_get::<String>("", "execution_id")? != intake.execution_id
                || row.try_get::<String>("", "bucket")? != intake.bucket
                || row.try_get::<String>("", "archive_key")? != intake.archive_key
                || row.try_get::<String>("", "part_contract")?.is_empty()
                || (execution.state == "admitted"
                    && (intake.active_upload_id.as_deref() != Some(batch.id.as_str())
                        || row.try_get::<Option<String>>("", "response_xml")?.is_some()))
                || (execution.state == "completed"
                    && (intake.active_upload_id.is_some()
                        || row.try_get::<Option<String>>("", "response_xml")?.is_none()))
            {
                return Err(stale());
            }
            return Ok(());
        }
        let archive_cid = |value: &str| {
            serde_json::from_str::<serde_json::Value>(value)
                .ok()
                .and_then(|json| json.get("archive_cid")?.as_str().map(str::to_owned))
        };
        let requested = archive_cid(terminal_result).ok_or_else(stale)?;
        if batch
            .terminal_result
            .as_deref()
            .and_then(archive_cid)
            .is_some_and(|original| original != requested)
        {
            return Err(stale());
        }
        super::batch::verify_prepared_archive(tx, &batch.id, &requested).await?;
    }
    Ok(())
}

/// Called INSIDE the caller's existing object/version/pin publication transaction,
/// AFTER its mutation/claim guard and object writes, BEFORE commit. Failure of
/// the root is persisted as `failed` with a safe code; it does not throw away
/// successfully published objects. On uncertain DB commit, call `snapshot` by
/// batch/token before attempting any object publication again. The caller MUST
/// roll back the entire publication transaction on Err, never commit partially
/// completed object/manifest/receipt writes.
pub async fn publish(
    tx: &DatabaseTransaction,
    id: &str,
    bindings: &[VersionBinding],
    source_published: bool,
    terminal_result: &str,
    outcome: RootOutcome,
) -> AppResult<()> {
    publish_with_failed_claim(
        tx,
        id,
        bindings,
        source_published,
        terminal_result,
        outcome,
        None,
    )
    .await
}

async fn publish_with_failed_claim(
    tx: &DatabaseTransaction,
    id: &str,
    bindings: &[VersionBinding],
    source_published: bool,
    terminal_result: &str,
    outcome: RootOutcome,
    failed_claim: Option<&RootClaim>,
) -> AppResult<()> {
    if !valid_terminal(terminal_result) {
        return Err(invalid());
    }
    let batch = super::batch::locked_open_batch(tx, id).await?;
    if !batch.manifest_prepared {
        return Err(stale());
    }
    verify_archive_binding(tx, &batch, terminal_result, source_published).await?;
    let (successes, failures) = bind_versions(tx, &batch, bindings).await?;
    let (status, error, cid) =
        root_fields(tx, &batch, outcome, successes, failures, failed_claim).await?;
    zip_batch::ActiveModel {
        state: Set("published".into()),
        source_published: Set(source_published),
        terminal_result: Set(Some(terminal_result.into())),
        root_status: Set(status),
        root_error_code: Set(error),
        root_cid: Set(cid),
        updated_at: Set(Utc::now()),
        ..batch.into()
    }
    .update(tx)
    .await?;
    Ok(())
}

/// Root-only retry: identical, already-bound manifest; no output version write.
/// Caller supplies the updated terminal representation for subsequent replay.
pub async fn settle_root_retry(
    tx: &DatabaseTransaction,
    id: &str,
    terminal_result: &str,
    outcome: RootOutcome,
) -> AppResult<()> {
    settle_root_retry_with_failed_claim(tx, id, terminal_result, outcome, None).await
}

async fn settle_root_retry_with_failed_claim(
    tx: &DatabaseTransaction,
    id: &str,
    terminal_result: &str,
    outcome: RootOutcome,
    failed_claim: Option<&RootClaim>,
) -> AppResult<()> {
    if !valid_terminal(terminal_result) {
        return Err(invalid());
    }
    let batch = lock_batch(tx, id).await?;
    if batch.state != "published" || batch.root_status != "failed" {
        return Err(stale());
    }
    verify_archive_binding(tx, &batch, terminal_result, batch.source_published).await?;
    let entries = manifest::entries(tx, id).await?;
    let successes = entries.iter().filter(|e| e.cid.is_some()).count();
    if entries
        .iter()
        .any(|e| e.cid.is_some() && e.version_row_id.is_none())
    {
        return Err(stale());
    }
    let failures = entries.len() - successes;
    let (status, error, cid) =
        root_fields(tx, &batch, outcome, successes, failures, failed_claim).await?;
    if !matches!(status.as_str(), "complete" | "partial" | "failed") {
        return Err(invalid());
    }
    zip_batch::ActiveModel {
        terminal_result: Set(Some(terminal_result.into())),
        root_status: Set(status),
        root_error_code: Set(error),
        root_cid: Set(cid),
        updated_at: Set(Utc::now()),
        ..batch.into()
    }
    .update(tx)
    .await?;
    Ok(())
}

impl RootClaim {
    /// Persist a failed initial publication only while this root claim is current.
    /// Call before releasing an uncertain root attempt as `unknown`.
    pub async fn publish_failed(
        &self,
        tx: &DatabaseTransaction,
        bindings: &[VersionBinding],
        source_published: bool,
        terminal_result: &str,
        code: &'static str,
    ) -> AppResult<()> {
        publish_with_failed_claim(
            tx,
            &self.batch_id,
            bindings,
            source_published,
            terminal_result,
            RootOutcome::Failed { code },
            Some(self),
        )
        .await
    }

    /// A failed root-only retry is fenced exactly like a verified retry.
    pub async fn settle_failed_retry(
        &self,
        tx: &DatabaseTransaction,
        terminal_result: &str,
        code: &'static str,
    ) -> AppResult<()> {
        settle_root_retry_with_failed_claim(
            tx,
            &self.batch_id,
            terminal_result,
            RootOutcome::Failed { code },
            Some(self),
        )
        .await
    }
}
