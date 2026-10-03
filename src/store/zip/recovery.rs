//! Bounded root-only retry eligibility and fenced settlement. Never consults
//! current S3 keys/versions: the already published manifest is the snapshot.

use chrono::Duration as ChronoDuration;
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, Set, Statement,
    TransactionTrait,
};

use super::{BatchSnapshot, RootClaim, RootOutcome, lock_batch, root, stale};
use crate::{
    error::{AppError, AppResult},
    store::{database_clock::database_now, entities::zip_root_build},
    zip::options::{ZipTargets, ZipV2Options},
};

pub const PAGE_SIZE: usize = 16;
pub const MAX_REVISIONS: i64 = 5;

/// One indexed, bounded page. Database wall clock decides both eligibility and
/// lease expiry; claim_root rechecks under the batch lock before any Kubo IO.
pub async fn due_page(db: &DatabaseConnection) -> AppResult<Vec<String>> {
    let backend = db.get_database_backend();
    let (initial_due, lease_due, enabled) = match backend {
        DatabaseBackend::Postgres => (
            "b.updated_at <= clock_timestamp() - INTERVAL '30 seconds'",
            "r.lease_until <= clock_timestamp()",
            "((b.captured_options::jsonb ->> 'root_enabled' = 'true' AND (b.captured_options::jsonb -> 'publish_extracted' IS NULL OR b.captured_options::jsonb -> 'publish_extracted' = 'true'::jsonb)) OR b.captured_options::jsonb #>> '{root_capture,configured}' = 'true' OR b.captured_options::jsonb #>> '{root_capture,tagged}' = 'true' OR (b.captured_options::jsonb #>> '{options,root_enabled}' = 'true' AND b.captured_options::jsonb #>> '{options,publish_extracted}' = 'true'))",
        ),
        DatabaseBackend::Sqlite => (
            "julianday(b.updated_at) <= julianday('now', '-30 seconds')",
            "julianday(r.lease_until) <= julianday('now')",
            "((json_extract(b.captured_options,'$.root_enabled')=1 AND (json_type(b.captured_options,'$.publish_extracted') IS NULL OR json_extract(b.captured_options,'$.publish_extracted')=1)) OR json_extract(b.captured_options,'$.root_capture.configured')=1 OR json_extract(b.captured_options,'$.root_capture.tagged')=1 OR (json_extract(b.captured_options,'$.options.root_enabled')=1 AND json_extract(b.captured_options,'$.options.publish_extracted')=1))",
        ),
        _ => {
            return Err(AppError::Internal(
                "ZIP recovery requires SQLite or PostgreSQL".into(),
            ));
        }
    };
    let sql = format!("SELECT b.id FROM zip_batches b
        WHERE b.state='published' AND b.root_status='failed' AND b.manifest_prepared=TRUE
          AND b.root_revision <= {MAX_REVISIONS} AND {enabled}
          AND b.root_error_code NOT IN ('path_conflict','invalid_manifest','disabled','empty',
                                      'directory_block_too_large','directory_hash_collision','needs_attention')
          AND EXISTS (SELECT 1 FROM zip_manifest_entries m
                      WHERE m.batch_id=b.id AND m.cid IS NOT NULL AND m.version_row_id IS NOT NULL)
          AND NOT EXISTS (SELECT 1 FROM zip_manifest_entries m
                          WHERE m.batch_id=b.id AND m.cid IS NOT NULL AND m.version_row_id IS NULL)
          AND ((b.root_revision=0 AND {initial_due}) OR
               (b.root_revision>0 AND EXISTS (SELECT 1 FROM zip_root_builds r
                WHERE r.batch_id=b.id AND r.revision=b.root_revision AND r.epoch=b.root_epoch
                  AND {lease_due})))
        ORDER BY b.id LIMIT {PAGE_SIZE}");
    let rows = db.query_all(Statement::from_string(backend, sql)).await?;
    rows.into_iter()
        .map(|row| row.try_get("", "id").map_err(Into::into))
        .collect()
}

/// Defence-in-depth after claiming: reject stale/incomplete, disabled, or
/// synthetic-path manifests before invoking the builder.
pub fn files(
    snapshot: &BatchSnapshot,
    claim: &RootClaim,
) -> AppResult<Vec<crate::kubo::directory::DirectoryFile>> {
    let batch = &snapshot.batch;
    let options: serde_json::Value =
        serde_json::from_str(&batch.captured_options).map_err(|_| stale())?;
    let prefix = options
        .get("target_prefix")
        .and_then(serde_json::Value::as_str);
    let legacy_enabled = options
        .get("root_enabled")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        || options
            .get("root_capture")
            .and_then(|capture| {
                serde_json::from_value::<crate::pinning::tags::ZipRootCapture>(capture.clone()).ok()
            })
            .is_some_and(crate::pinning::tags::ZipRootCapture::enabled);
    // v2's separately authenticated execution capture has no legacy prefix.
    // Trust the immutable path-to-version binding, never infer it from a key.
    let v2_enabled = options.get("options").is_some_and(|v2| {
        v2.get("root_enabled").and_then(serde_json::Value::as_bool) == Some(true)
            && v2
                .get("publish_extracted")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
    });
    // MPU v2 persists ZipV2Options itself, unlike the nested direct capture.
    // Any flat v2 field selects this contract: malformed captures must never
    // fall back to legacy root_enabled/target_prefix authorization.
    let flat_v2 = [
        "publish_source",
        "publish_extracted",
        "targets",
        "token",
        "root_override",
        "result_version",
    ]
    .iter()
    .any(|field| options.get(field).is_some());
    let enabled = if flat_v2 {
        let captured: ZipV2Options =
            serde_json::from_value(options.clone()).map_err(|_| stale())?;
        if batch.source != "mpu"
            || captured.result_version != 2
            || captured.token != batch.token
            || captured.token.is_empty()
            || captured.token.len() > 128
            || !captured
                .token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._~-".contains(&byte))
            || captured.publish_source != batch.source_published
            || captured
                .root_override
                .is_some_and(|value| value != captured.root_enabled)
            || (matches!(captured.targets, ZipTargets::Source | ZipTargets::Both)
                && !captured.publish_source)
            || (matches!(captured.targets, ZipTargets::Extracted | ZipTargets::Both)
                && !captured.publish_extracted)
            // Require the complete, unambiguous flat snapshot, including the
            // nullable root_override; reject legacy/nested fields mixed in.
            || serde_json::to_value(&captured).map_err(|_| stale())? != options
        {
            return Err(stale());
        }
        captured.build_root()
    } else {
        (prefix.is_some() && legacy_enabled) || v2_enabled
    };
    if !enabled
        || batch.id != claim.batch_id
        || batch.state != "published"
        || batch.root_status != "failed"
        || !batch.manifest_prepared
        || batch.root_revision != claim.revision
        || batch.root_epoch != claim.epoch
        || snapshot.entries.is_empty()
        || snapshot.entries.len() > 10_000
    {
        return Err(stale());
    }
    let mut result = Vec::new();
    for entry in &snapshot.entries {
        if let Some(cid) = &entry.cid {
            if entry.version_row_id.is_none()
                || entry.object_key.as_deref().is_none_or(|key| {
                    prefix
                        .is_some_and(|prefix| key.strip_prefix(prefix) != Some(entry.path.as_str()))
                })
                || entry.size.is_none()
            {
                return Err(stale());
            }
            result.push(crate::kubo::directory::DirectoryFile {
                path: entry.path.clone(),
                cid: cid.clone(),
            });
        }
    }
    if result.is_empty() {
        return Err(stale());
    }
    Ok(result)
}

/// Keep the original result envelope (including archive/MPU/import metadata),
/// changing only root fields; never turn the injected warning into an entry error.
fn terminal(
    snapshot: &BatchSnapshot,
    status: &str,
    code: Option<&str>,
    cid: Option<&str>,
) -> AppResult<String> {
    let mut value: serde_json::Value = serde_json::from_str(
        snapshot
            .batch
            .terminal_result
            .as_deref()
            .ok_or_else(stale)?,
    )
    .map_err(|_| stale())?;
    let object = value.as_object_mut().ok_or_else(stale)?;
    object.insert("root_status".into(), status.into());
    object.insert("root_warning".into(), code.into());
    object.insert("root_cid".into(), cid.into());
    let result = serde_json::to_string(&value).map_err(|_| stale())?;
    if result.len() > 1_048_576 {
        return Err(stale());
    }
    Ok(result)
}

pub async fn settle_verified(
    db: &DatabaseConnection,
    snapshot: &BatchSnapshot,
    claim: RootClaim,
    node: String,
    cid: String,
) -> AppResult<()> {
    let result = terminal(
        snapshot,
        if snapshot.entries.iter().any(|e| e.cid.is_none()) {
            "partial"
        } else {
            "complete"
        },
        None,
        Some(&cid),
    )?;
    let tx = db.begin().await?;
    let id = claim.batch_id.clone();
    super::settle_root_retry(
        &tx,
        &id,
        &result,
        RootOutcome::Verified {
            claim,
            node_identity: node,
            tier: "hot".into(),
            cid,
        },
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn settle_failed(
    db: &DatabaseConnection,
    snapshot: &BatchSnapshot,
    claim: &RootClaim,
    code: &'static str,
) -> AppResult<()> {
    let attention = claim.revision >= MAX_REVISIONS;
    let code = if attention { "needs_attention" } else { code };
    let result = terminal(snapshot, "failed", Some(code), None)?;
    let tx = db.begin().await?;
    let batch = lock_batch(&tx, &claim.batch_id).await?;
    if batch.root_revision != claim.revision || batch.root_epoch != claim.epoch {
        return Err(stale());
    }
    let build = root::current_claim(&tx, claim).await?;
    claim.settle_failed_retry(&tx, &result, code).await?;
    // A failed claim cannot renew. Its future lease_until is the durable retry
    // backoff, so a second gateway cannot immediately claim the same batch.
    let now = database_now(&tx).await?;
    let seconds = 30_i64.saturating_mul(1_i64 << (claim.revision - 1).clamp(0, 5));
    zip_root_build::ActiveModel {
        status: Set("failed".into()),
        error_code: Set(Some(code.into())),
        lease_until: Set(now + ChronoDuration::seconds(seconds)),
        updated_at: Set(now),
        ..build.into()
    }
    .update(&tx)
    .await?;
    tx.commit().await?;
    Ok(())
}
