//! ZIP v2 import intake. A request and its ZIP execution are one bucket-
//! locked transaction; no archive destination or prefix is claimed at intake.
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction, Statement,
    TransactionTrait, Value,
};

use crate::{
    error::{AppError, AppResult},
    pinning::{
        config::{ProviderLimitMap, ValidatedPinningConfig},
        zip_policy::ValidatedZipOutputRules,
    },
    store::{
        import::{jobs, ownership, ownership::StandardMutationGuard},
        pinning::publication::{ZipV2Publication, ZipV2PublicationResult},
        zip::execution,
    },
    zip::options::ZipV2Options,
};

pub struct Request {
    pub admission: execution::Admission,
    pub prefix: String,
    /// Private; URL query strings may contain secrets. Never render or log.
    pub source_descriptor: String,
    pub expected_sha256: Option<String>,
}

pub struct Status {
    pub id: String,
    pub state: &'static str,
    pub expected_sha256: Option<String>,
    pub measured_sha256: Option<String>,
    /// Authenticated store status; the root may be retried independently of
    /// the immutable import receipt and execution terminal.
    pub root_status: Option<String>,
    /// A receipt sealed from an actual source version in its publication txn.
    /// Never reconstructed from whichever object currently occupies the key.
    pub published_source: Option<PublishedSource>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedSource {
    pub cid: String,
    pub size: i64,
    pub version_id: Option<String>,
}

fn sql(db: &impl ConnectionTrait, sqlite: &str, postgres: &str, values: Vec<Value>) -> Statement {
    let backend = db.get_database_backend();
    Statement::from_sql_and_values(
        backend,
        if backend == DatabaseBackend::Postgres {
            postgres
        } else {
            sqlite
        },
        values,
    )
}

fn conflict() -> AppError {
    AppError::ImportIdempotencyConflict
}

/// A database trigger also checks both directions at write time, so a legacy
/// writer between preflight and admission cannot steal the same token.
pub fn map_token_conflict(error: AppError) -> AppError {
    match &error {
        AppError::Database(detail) if detail.contains("zip_v2_import_token_conflict") => conflict(),
        _ => error,
    }
}

fn transient(error: &AppError) -> bool {
    let AppError::Database(detail) = error else {
        return false;
    };
    let detail = detail.to_ascii_lowercase();
    detail.contains("database is locked")
        || detail.contains("database is busy")
        || detail.contains("could not serialize access")
        || detail.contains("deadlock detected")
        || detail.contains("sqlstate 40001")
        || detail.contains("sqlstate 40p01")
        || detail.contains("duplicate key value violates unique constraint")
        || detail.contains("sqlstate 23505")
        || detail.contains("code: 23505")
        || detail.contains("code: 2067")
}

pub async fn legacy_token_exists<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
    token: &str,
) -> AppResult<bool> {
    let row = db.query_one(sql(db,
        "SELECT 1 FROM zip_v2_import_requests WHERE bucket=? AND source_key=? AND client_token=?",
        "SELECT 1 FROM zip_v2_import_requests WHERE bucket=$1 AND source_key=$2 AND client_token=$3",
        vec![bucket.into(), key.into(), token.into()])).await?;
    Ok(row.is_some())
}

async fn existing<C: ConnectionTrait>(db: &C, req: &Request) -> AppResult<Option<Status>> {
    let a = &req.admission;
    let row = db.query_one(sql(db,
        "SELECT r.batch_id,r.owner,r.bucket,r.source_key,r.prefix,r.source_descriptor,r.expected_sha256,r.request_contract,e.state,e.input_sha256,r.job_state,r.receipt_metadata,b.root_status FROM zip_v2_import_requests r JOIN zip_v2_executions e ON e.id=r.batch_id LEFT JOIN zip_batches b ON b.id=r.batch_id WHERE r.owner=? AND r.client_token=?",
        "SELECT r.batch_id,r.owner,r.bucket,r.source_key,r.prefix,r.source_descriptor,r.expected_sha256,r.request_contract,e.state,e.input_sha256,r.job_state,r.receipt_metadata,b.root_status FROM zip_v2_import_requests r JOIN zip_v2_executions e ON e.id=r.batch_id LEFT JOIN zip_batches b ON b.id=r.batch_id WHERE r.owner=$1 AND r.client_token=$2",
        vec![a.owner.clone().into(), a.token.clone().into()])).await?;
    let Some(row) = row else { return Ok(None) };
    let contract: String = row.try_get("", "request_contract")?;
    let descriptor: String = row.try_get("", "source_descriptor")?;
    let prefix: String = row.try_get("", "prefix")?;
    let bucket: String = row.try_get("", "bucket")?;
    let source_key: String = row.try_get("", "source_key")?;
    let expected: Option<String> = row.try_get("", "expected_sha256")?;
    if contract != a.request_contract
        || descriptor != req.source_descriptor
        || prefix != req.prefix
        || bucket != a.bucket
        || source_key != a.source_key
        || expected != req.expected_sha256
    {
        return Err(conflict());
    }
    status_from_row(row).map(Some)
}

fn status_from_row(row: sea_orm::QueryResult) -> AppResult<Status> {
    let execution_state: String = row.try_get("", "state")?;
    let job_state: String = row.try_get("", "job_state")?;
    let receipt: Option<String> = row.try_get("", "receipt_metadata")?;
    let measured: Option<String> = row.try_get("", "input_sha256")?;
    // Do not present an execution transition as published without a durable
    // import receipt. Later worker must atomically bind verified completion.
    let state = match (job_state.as_str(), execution_state.as_str()) {
        ("ready", "completed") if receipt.is_some() && measured.is_some() => "ready",
        ("failed", _) | (_, "fenced") => "failed",
        _ => "pending",
    };
    let published_source = if state == "ready" {
        let value: serde_json::Value =
            serde_json::from_str(receipt.as_deref().ok_or_else(stale)?).map_err(|_| stale())?;
        value
            .get("source")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| stale())?
    } else {
        None
    };
    Ok(Status {
        id: row.try_get("", "batch_id")?,
        state,
        expected_sha256: row.try_get("", "expected_sha256")?,
        measured_sha256: measured,
        root_status: row.try_get("", "root_status")?,
        published_source,
    })
}

pub async fn preflight(db: &DatabaseConnection, req: &Request) -> AppResult<Option<Status>> {
    for attempt in 0..=3 {
        let outcome = preflight_once(db, req).await;
        if !outcome.as_ref().is_err_and(transient) || attempt == 3 {
            return outcome;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10 << attempt)).await;
    }
    unreachable!("bounded intake preflight retries return")
}

async fn preflight_once(db: &DatabaseConnection, req: &Request) -> AppResult<Option<Status>> {
    let tx = db.begin().await?;
    ownership::lock_bucket_for_ownership(&tx, &req.admission.bucket).await?;
    let result = preflight_locked(&tx, req).await?;
    tx.commit().await?;
    Ok(result)
}

async fn preflight_locked(tx: &DatabaseTransaction, req: &Request) -> AppResult<Option<Status>> {
    let a = &req.admission;
    if jobs::find_idempotent(tx, &a.bucket, &a.source_key, &a.token)
        .await?
        .is_some()
    {
        return Err(conflict());
    }
    existing(tx, req).await
}

pub async fn admit(db: &DatabaseConnection, req: &Request) -> AppResult<Status> {
    for attempt in 0..=3 {
        let outcome = admit_once(db, req).await;
        if !outcome.as_ref().is_err_and(transient) || attempt == 3 {
            return outcome;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10 << attempt)).await;
    }
    unreachable!("bounded intake admission retries return")
}

async fn admit_once(db: &DatabaseConnection, req: &Request) -> AppResult<Status> {
    let tx = db.begin().await?;
    ownership::lock_bucket_for_ownership(&tx, &req.admission.bucket).await?;
    if let Some(status) = preflight_locked(&tx, req).await? {
        tx.commit().await?;
        return Ok(status);
    }
    let a = &req.admission;
    // No standalone execution::admit: its own commit would orphan the batch
    // if the request insert fails.
    execution::admit_in_transaction(&tx, a).await?;
    let inserted = tx.execute(sql(&tx,
        "INSERT INTO zip_v2_import_requests (batch_id,owner,bucket,source_key,client_token,prefix,source_descriptor,expected_sha256,request_contract) VALUES (?,?,?,?,?,?,?,?,?)",
        "INSERT INTO zip_v2_import_requests (batch_id,owner,bucket,source_key,client_token,prefix,source_descriptor,expected_sha256,request_contract) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        vec![a.id.clone().into(),a.owner.clone().into(),a.bucket.clone().into(),a.source_key.clone().into(),
            a.token.clone().into(),req.prefix.clone().into(),req.source_descriptor.clone().into(),
            req.expected_sha256.clone().into(),a.request_contract.clone().into()])).await.map_err(|error| map_token_conflict(error.into()))?;
    if inserted.rows_affected() != 1 {
        return Err(conflict());
    }
    tx.commit().await?;
    Ok(Status {
        id: a.id.clone(),
        state: "pending",
        expected_sha256: req.expected_sha256.clone(),
        measured_sha256: None,
        root_status: None,
        published_source: None,
    })
}

/// Safe status: path + authenticated principal must match the captured intake.
pub async fn read_for_path(
    db: &DatabaseConnection,
    id: &str,
    owner: &str,
    bucket: &str,
    key: &str,
) -> AppResult<Option<Status>> {
    let row = db.query_one(sql(db,
        "SELECT r.batch_id,r.expected_sha256,r.job_state,r.receipt_metadata,e.state,e.input_sha256,b.root_status FROM zip_v2_import_requests r JOIN zip_v2_executions e ON e.id=r.batch_id LEFT JOIN zip_batches b ON b.id=r.batch_id WHERE r.batch_id=? AND r.owner=? AND r.bucket=? AND r.source_key=?",
        "SELECT r.batch_id,r.expected_sha256,r.job_state,r.receipt_metadata,e.state,e.input_sha256,b.root_status FROM zip_v2_import_requests r JOIN zip_v2_executions e ON e.id=r.batch_id LEFT JOIN zip_batches b ON b.id=r.batch_id WHERE r.batch_id=$1 AND r.owner=$2 AND r.bucket=$3 AND r.source_key=$4",
        vec![id.into(),owner.into(),bucket.into(),key.into()])).await?;
    row.map(status_from_row).transpose()
}

/// Bounded keyset discovery of due pending and admitted work. The database
/// clock, not the polling process's clock, decides whether a claim is due.
/// Discovery is never authority to fetch a private source or publish outputs.
pub async fn pending_ids(
    db: &DatabaseConnection,
    after: &str,
    limit: u64,
) -> AppResult<Vec<String>> {
    if !(1..=256).contains(&limit) {
        return Err(AppError::InvalidImportRequest);
    }
    let rows = db.query_all(sql(db,
        "SELECT r.batch_id FROM zip_v2_import_requests r JOIN zip_v2_executions e ON e.id=r.batch_id WHERE r.job_state='pending' AND e.state IN ('pending','admitted') AND (e.lease_until IS NULL OR julianday(e.lease_until)<=julianday('now')) AND r.batch_id>? ORDER BY r.batch_id LIMIT ?",
        "SELECT r.batch_id FROM zip_v2_import_requests r JOIN zip_v2_executions e ON e.id=r.batch_id WHERE r.job_state='pending' AND e.state IN ('pending','admitted') AND (e.lease_until IS NULL OR e.lease_until<=clock_timestamp()) AND r.batch_id>$1 ORDER BY r.batch_id LIMIT $2",
        vec![after.into(), (limit as i64).into()])).await?;
    rows.into_iter()
        .map(|row| row.try_get("", "batch_id").map_err(Into::into))
        .collect()
}

pub struct ClaimedSource {
    pub source_descriptor: String,
    pub expected_sha256: Option<String>,
    pub prefix: String,
}

/// Claim first with execution::claim; a pending ID is not an authority to
/// access a potentially credential-bearing URL. Admitted recovery gets only
/// the frozen prefix, never a fetchable descriptor. No data is returned after
/// expiration or epoch takeover.
pub async fn claimed_source(
    db: &DatabaseConnection,
    claim: &execution::Claim,
) -> AppResult<Option<ClaimedSource>> {
    let row = db.query_one(sql(db,
        "SELECT CASE WHEN e.state='pending' THEN r.source_descriptor ELSE '' END AS source_descriptor,r.expected_sha256,r.prefix FROM zip_v2_import_requests r JOIN zip_v2_executions e ON e.id=r.batch_id WHERE r.batch_id=? AND r.job_state='pending' AND e.state IN ('pending','admitted') AND e.epoch=? AND e.worker=? AND julianday(e.lease_until)>julianday('now')",
        "SELECT CASE WHEN e.state='pending' THEN r.source_descriptor ELSE '' END AS source_descriptor,r.expected_sha256,r.prefix FROM zip_v2_import_requests r JOIN zip_v2_executions e ON e.id=r.batch_id WHERE r.batch_id=$1 AND r.job_state='pending' AND e.state IN ('pending','admitted') AND e.epoch=$2 AND e.worker=$3 AND e.lease_until>clock_timestamp()",
        vec![claim.batch_id.clone().into(),claim.epoch.into(),claim.worker.clone().into()])).await?;
    row.map(|row| {
        Ok(ClaimedSource {
            source_descriptor: row.try_get("", "source_descriptor")?,
            expected_sha256: row.try_get("", "expected_sha256")?,
            prefix: row.try_get("", "prefix")?,
        })
    })
    .transpose()
}

/// Only after clean ZIP EOF/trailer verification: compare the signed URL
/// promise before binding measured bytes. CID imports also bind measured SHA;
/// CID identity alone is not a ZIP digest.
pub async fn bind_verified_input(
    db: &DatabaseConnection,
    claim: &execution::Claim,
    sha256: &str,
    art_cid: &str,
    art_size: i64,
) -> AppResult<()> {
    let source = claimed_source(db, claim)
        .await?
        .ok_or(AppError::StaleContentMutation)?;
    if source
        .expected_sha256
        .as_deref()
        .is_some_and(|expected| expected != sha256)
    {
        return Err(AppError::InvalidImportRequest);
    }
    execution::bind_clean_input(db, claim, sha256, art_cid, art_size).await
}

fn captured_options(snapshot: &execution::Snapshot) -> AppResult<ZipV2Options> {
    let capture: serde_json::Value =
        serde_json::from_str(&snapshot.captured_options).map_err(|_| stale())?;
    serde_json::from_value(capture.get("options").cloned().ok_or_else(stale)?).map_err(|_| stale())
}

/// Recover only the exact source identity frozen beside both admitted manifests.
/// This never admits/recreates a missing destination token after takeover.
pub fn source_guard(
    snapshot: &execution::Snapshot,
    batch: &crate::store::entities::zip_batch::Model,
) -> AppResult<Option<StandardMutationGuard>> {
    if snapshot.source != "import"
        || snapshot.state != "admitted"
        || batch.state != "open"
        || !batch.manifest_prepared
        || batch.source_published
        || batch.id != snapshot.id
        || batch.owner != snapshot.owner
        || batch.source != snapshot.source
        || batch.token != snapshot.token
        || batch.bucket != snapshot.bucket
        || batch.archive_key != snapshot.source_key
        || batch.fingerprint != "pending"
        || batch.captured_options != snapshot.captured_options
    {
        return Err(stale());
    }
    if !captured_options(snapshot)?.publish_source {
        return if batch.input_identity == "pending" {
            Ok(None)
        } else {
            Err(stale())
        };
    }
    let generation = batch
        .input_identity
        .strip_prefix("zip-v2-source-gen:")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(stale)?;
    Ok(Some(StandardMutationGuard {
        bucket: snapshot.bucket.clone(),
        key: snapshot.source_key.clone(),
        mutation_id: format!("zip-v2-source:{}", snapshot.id),
        expected_generation: generation,
        mutation_prefix: None,
    }))
}

/// The shared group seam already supports the import capture shape. It renews
/// execution, source and outputs in ONE bucket-first transaction, fencing a
/// missing/expired source guard rather than reviving it. Pending has no guard.
pub async fn renew(
    db: &DatabaseConnection,
    claim: &execution::Claim,
    seconds: i64,
) -> AppResult<bool> {
    let Some(snapshot) = execution::read(db, &claim.batch_id).await? else {
        return Ok(false);
    };
    if snapshot.source != "import" {
        return Err(stale());
    }
    ownership::renew_zip_v2_direct_group(db, claim, seconds).await
}

/// The import worker calls this only after clean EOF, a bound measured SHA,
/// complete manifest admission and preparation of the matching ZIP batch.
/// A transport/commit error is NOT a failure receipt: read the authenticated
/// status before deciding whether to retry publication under a live claim.
/// This schema has no next_attempt/attempt-budget columns: transport retries
/// must be bounded and scheduled by the worker against the database lease clock.
pub async fn publish_import_zip_v2(
    db: &DatabaseConnection,
    request: ZipV2Publication,
    rules: &ValidatedZipOutputRules,
    config: &ValidatedPinningConfig,
    limits: &ProviderLimitMap,
) -> AppResult<ZipV2PublicationResult> {
    request.publish_import(db, rules, config, limits).await
}

fn stale() -> AppError {
    AppError::StaleContentMutation
}

/// Called after the bucket lock, before any publication writes. The request
/// must still be the exact captured import; in particular an output may not
/// escape its frozen prefix or quietly substitute the archive source key.
pub(crate) async fn verify_publication_in_transaction(
    tx: &DatabaseTransaction,
    snapshot: &execution::Snapshot,
    outputs: &[(&str, &str)],
) -> AppResult<()> {
    let row = tx.query_one(sql(tx,
        "SELECT owner,bucket,source_key,client_token,prefix,expected_sha256,request_contract,job_state,receipt_metadata FROM zip_v2_import_requests WHERE batch_id=?",
        "SELECT owner,bucket,source_key,client_token,prefix,expected_sha256,request_contract,job_state,receipt_metadata FROM zip_v2_import_requests WHERE batch_id=$1 FOR UPDATE",
        vec![snapshot.id.clone().into()])).await?.ok_or_else(stale)?;
    let prefix: String = row.try_get("", "prefix")?;
    if snapshot.source != "import"
        || snapshot.state != "admitted"
        || snapshot.input_sha256.is_none()
        || row.try_get::<String>("", "owner")? != snapshot.owner
        || row.try_get::<String>("", "bucket")? != snapshot.bucket
        || row.try_get::<String>("", "source_key")? != snapshot.source_key
        || row.try_get::<String>("", "client_token")? != snapshot.token
        || row.try_get::<String>("", "request_contract")? != snapshot.request_contract
        || row.try_get::<String>("", "job_state")? != "pending"
        || row
            .try_get::<Option<String>>("", "receipt_metadata")?
            .is_some()
        || row
            .try_get::<Option<String>>("", "expected_sha256")?
            .as_ref()
            .is_some_and(|sha| Some(sha) != snapshot.input_sha256.as_ref())
        || outputs
            .iter()
            .any(|(path, key)| key != &format!("{prefix}{path}"))
    {
        return Err(stale());
    }
    Ok(())
}

/// Only a narrow, strictly typed terminal envelope is accepted on import:
/// arbitrary request JSON (URLs, raw errors, fabricated source ETags/versions)
/// must never become either the terminal result or a replayable receipt.
pub(crate) async fn complete_publication_in_transaction(
    tx: &DatabaseTransaction,
    snapshot: &execution::Snapshot,
    claim: &execution::Claim,
    terminal: &str,
) -> AppResult<()> {
    let batch = execution::read(tx, &claim.batch_id)
        .await?
        .ok_or_else(stale)?;
    if batch.state != "completed"
        || batch.source != "import"
        || batch.owner != snapshot.owner
        || batch.bucket != snapshot.bucket
        || batch.source_key != snapshot.source_key
        || batch.token != snapshot.token
        || batch.request_fingerprint != snapshot.request_fingerprint
        || batch.request_contract != snapshot.request_contract
        || batch.epoch != claim.epoch
        || batch.worker.as_deref() != Some(&claim.worker)
        || batch.terminal_result.as_deref() != Some(terminal)
        || batch.input_sha256 != snapshot.input_sha256
    {
        return Err(stale());
    }
    let options = captured_options(snapshot)?;
    let root = tx.query_one(sql(tx,
        "SELECT state,root_status,source_published,terminal_result FROM zip_batches WHERE id=?",
        "SELECT state,root_status,source_published,terminal_result FROM zip_batches WHERE id=$1",
        vec![claim.batch_id.clone().into()])).await?.ok_or_else(stale)?;
    let root_status: String = root.try_get("", "root_status")?;
    if root.try_get::<String>("", "state")? != "published"
        || root.try_get::<bool>("", "source_published")? != options.publish_source
        || root
            .try_get::<Option<String>>("", "terminal_result")?
            .as_deref()
            != Some(terminal)
        || !matches!(
            root_status.as_str(),
            "disabled" | "empty" | "failed" | "complete" | "partial"
        )
    {
        return Err(stale());
    }
    let counts = tx.query_one(sql(tx,
        "SELECT SUM(CASE WHEN object_key IS NOT NULL THEN 1 ELSE 0 END) AS published, SUM(CASE WHEN error_code IS NOT NULL THEN 1 ELSE 0 END) AS failed FROM zip_v2_manifest WHERE batch_id=?",
        "SELECT SUM(CASE WHEN object_key IS NOT NULL THEN 1 ELSE 0 END) AS published, SUM(CASE WHEN error_code IS NOT NULL THEN 1 ELSE 0 END) AS failed FROM zip_v2_manifest WHERE batch_id=$1",
        vec![claim.batch_id.clone().into()])).await?.ok_or_else(stale)?;
    let published = counts.try_get::<Option<i64>>("", "published")?.unwrap_or(0);
    let failed = counts.try_get::<Option<i64>>("", "failed")?.unwrap_or(0);
    let value: serde_json::Value = serde_json::from_str(terminal).map_err(|_| stale())?;
    let envelope = value.as_object().ok_or_else(stale)?;
    let measured = snapshot.input_sha256.as_deref().ok_or_else(stale)?;
    let result_status = if published == 0 && !options.publish_source {
        "failed"
    } else {
        "completed"
    };
    if envelope.len() != if options.publish_source { 9 } else { 4 }
        || (!options.publish_extracted
            && (published != 0 || failed != 0 || root_status != "disabled"))
        || envelope
            .get("input_sha256")
            .and_then(serde_json::Value::as_str)
            != Some(measured)
        || envelope
            .get("published_count")
            .and_then(serde_json::Value::as_i64)
            != Some(published)
        || envelope
            .get("failed_count")
            .and_then(serde_json::Value::as_i64)
            != Some(failed)
        || envelope.get("status").and_then(serde_json::Value::as_str) != Some(result_status)
    {
        return Err(stale());
    }
    let source = if options.publish_source {
        Some(verify_source_receipt(tx, snapshot, envelope).await?)
    } else {
        None
    };
    let mut receipt = serde_json::json!({
        "result_version": 2,
        "input_sha256": measured,
        "published_count": published,
        "failed_count": failed,
        "status": result_status,
        "root_status": root_status,
    });
    if let Some(source) = source {
        receipt["source"] = serde_json::to_value(source).map_err(|_| stale())?;
    }
    let receipt = receipt.to_string();
    if receipt.len() > 1024 {
        return Err(stale());
    }
    let changed = tx.execute(sql(tx,
        "UPDATE zip_v2_import_requests SET job_state='ready',receipt_metadata=? WHERE batch_id=? AND owner=? AND bucket=? AND source_key=? AND client_token=? AND request_contract=? AND job_state='pending' AND receipt_metadata IS NULL",
        "UPDATE zip_v2_import_requests SET job_state='ready',receipt_metadata=$1 WHERE batch_id=$2 AND owner=$3 AND bucket=$4 AND source_key=$5 AND client_token=$6 AND request_contract=$7 AND job_state='pending' AND receipt_metadata IS NULL",
        vec![receipt.into(),claim.batch_id.clone().into(),snapshot.owner.clone().into(),snapshot.bucket.clone().into(),snapshot.source_key.clone().into(),snapshot.token.clone().into(),snapshot.request_contract.clone().into()])).await?;
    if changed.rows_affected() != 1 {
        return Err(stale());
    }
    Ok(())
}

/// Only while the publisher still holds the bucket lock: match its actual
/// newly written current source version. The immutable receipt then survives
/// subsequent overwrite/deletion without consulting the current key again.
async fn verify_source_receipt(
    tx: &DatabaseTransaction,
    snapshot: &execution::Snapshot,
    envelope: &serde_json::Map<String, serde_json::Value>,
) -> AppResult<PublishedSource> {
    let cid = envelope
        .get("source_cid")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(stale)?;
    let size = envelope
        .get("source_size")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(stale)?;
    let version_id: Option<String> = serde_json::from_value(
        envelope
            .get("source_version_id")
            .cloned()
            .ok_or_else(stale)?,
    )
    .map_err(|_| stale())?;
    let version_row_id = envelope
        .get("source_version_row_id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(stale)?;
    if Some(cid) != snapshot.input_art_cid.as_deref() || Some(size) != snapshot.input_art_size {
        return Err(stale());
    }
    let row = tx.query_one(sql(tx,
        "SELECT v.version_id,o.cid,o.size,o.etag,o.encrypted,o.multipart FROM object_versions v JOIN objects o ON o.id=v.object_id WHERE v.bucket=? AND v.key=? AND v.id=? AND v.kind='object' AND v.is_latest=TRUE",
        "SELECT v.version_id,o.cid,o.size,o.etag,o.encrypted,o.multipart FROM object_versions v JOIN objects o ON o.id=v.object_id WHERE v.bucket=$1 AND v.key=$2 AND v.id=$3 AND v.kind='object' AND v.is_latest=TRUE",
        vec![snapshot.bucket.clone().into(),snapshot.source_key.clone().into(),version_row_id.to_owned().into()])).await?.ok_or_else(stale)?;
    let public_version = match row.try_get::<Option<String>>("", "version_id")? {
        Some(id) => Some(id),
        None => match crate::store::bucket::get_versioning_state(tx, &snapshot.bucket).await? {
            crate::store::object_version::BucketVersioningState::Unversioned => None,
            _ => Some(crate::store::object_version::NULL_VERSION_ID.to_owned()),
        },
    };
    if row.try_get::<String>("", "cid")? != cid
        || row.try_get::<String>("", "etag")? != cid
        || row.try_get::<i64>("", "size")? != size
        || row.try_get::<bool>("", "encrypted")?
        || row.try_get::<bool>("", "multipart")?
        || public_version != version_id
    {
        return Err(stale());
    }
    // The publisher inserts this typed policy after validating each exact
    // output's automatic-rule intersection. It must never contain raw errors.
    let policy = envelope
        .get("source_policy")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(stale)?;
    let tags: Vec<crate::pinning::tags::ObjectTag> =
        serde_json::from_value(policy.get("tags").cloned().ok_or_else(stale)?)
            .map_err(|_| stale())?;
    let leases: Vec<crate::pinning::policy::LeaseIntent> =
        serde_json::from_value(policy.get("leases").cloned().ok_or_else(stale)?)
            .map_err(|_| stale())?;
    let capture: serde_json::Value =
        serde_json::from_str(&snapshot.captured_options).map_err(|_| stale())?;
    let captured_tags: Vec<crate::pinning::tags::ObjectTag> =
        serde_json::from_value(capture.get("source_tags").cloned().ok_or_else(stale)?)
            .map_err(|_| stale())?;
    if policy.len() != 2
        || tags != captured_tags
        || leases.iter().any(|lease| {
            lease.source != crate::pinning::policy::LeaseSource::Automatic
                || lease.content_mode != crate::pinning::tags::ContentMode::Object
        })
    {
        return Err(stale());
    }
    Ok(PublishedSource {
        cid: cid.to_owned(),
        size,
        version_id,
    })
}

#[derive(Clone, Debug)]
pub enum ImportFailure<'a> {
    /// The independently measured full-input digest, which must disagree with
    /// the captured promise. Neither digest is a source ETag.
    ExpectedSha256Mismatch {
        measured_sha256: &'a str,
    },
    InvalidZip,
}

impl ImportFailure<'_> {
    fn code(&self) -> &'static str {
        match self {
            Self::ExpectedSha256Mismatch { .. } => "expected_sha256_mismatch",
            Self::InvalidZip => "invalid_zip",
        }
    }
}

/// Permanently fail an invalid input ONLY while the original worker still
/// owns a pending execution. Do not turn DB/transport uncertainty into a
/// failure: this call itself propagates the error for caller reconciliation.
/// No retry scheduling or attempt budget is persisted in this intake schema.
pub async fn fail_if_owned(
    db: &DatabaseConnection,
    claim: &execution::Claim,
    failure: ImportFailure<'_>,
) -> AppResult<bool> {
    let Some(hint) = execution::read(db, &claim.batch_id).await? else {
        return Ok(false);
    };
    if hint.source != "import" {
        return Ok(false);
    }
    let tx = db.begin().await?;
    ownership::lock_bucket_for_ownership(&tx, &hint.bucket).await?;
    let Some(snapshot) = execution::read(&tx, &claim.batch_id).await? else {
        return Ok(false);
    };
    let now = crate::store::database_clock::database_now(&tx).await?;
    if snapshot.source != "import"
        || snapshot.bucket != hint.bucket
        || snapshot.state != "pending"
        || snapshot.epoch != claim.epoch
        || snapshot.worker.as_deref() != Some(&claim.worker)
        || !snapshot.lease_until.is_some_and(|until| until > now)
    {
        return Ok(false);
    }
    if let ImportFailure::ExpectedSha256Mismatch { measured_sha256 } = &failure {
        let row = tx.query_one(sql(&tx,
            "SELECT expected_sha256 FROM zip_v2_import_requests WHERE batch_id=? AND owner=? AND bucket=? AND source_key=? AND client_token=? AND request_contract=? AND job_state='pending'",
            "SELECT expected_sha256 FROM zip_v2_import_requests WHERE batch_id=$1 AND owner=$2 AND bucket=$3 AND source_key=$4 AND client_token=$5 AND request_contract=$6 AND job_state='pending' FOR UPDATE",
            vec![claim.batch_id.clone().into(),snapshot.owner.clone().into(),snapshot.bucket.clone().into(),snapshot.source_key.clone().into(),snapshot.token.clone().into(),snapshot.request_contract.clone().into()])).await?.ok_or_else(stale)?;
        let expected: Option<String> = row.try_get("", "expected_sha256")?;
        if measured_sha256.len() != 64
            || !measured_sha256
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
            || expected
                .as_deref()
                .is_none_or(|sha| sha == *measured_sha256)
        {
            return Err(AppError::InvalidImportRequest);
        }
    }
    let changed = tx.execute(sql(&tx,
        "UPDATE zip_v2_executions SET state='fenced',terminal_result=?,updated_at=? WHERE id=? AND epoch=? AND worker=? AND state='pending'",
        "UPDATE zip_v2_executions SET state='fenced',terminal_result=$1,updated_at=$2 WHERE id=$3 AND epoch=$4 AND worker=$5 AND state='pending'",
        vec![failure.code().into(),now.into(),claim.batch_id.clone().into(),claim.epoch.into(),claim.worker.clone().into()])).await?;
    if changed.rows_affected() != 1 {
        return Err(stale());
    }
    let receipt = serde_json::json!({"code": failure.code()}).to_string();
    let changed = tx.execute(sql(&tx,
        "UPDATE zip_v2_import_requests SET job_state='failed',receipt_metadata=? WHERE batch_id=? AND owner=? AND bucket=? AND source_key=? AND client_token=? AND request_contract=? AND job_state='pending' AND receipt_metadata IS NULL",
        "UPDATE zip_v2_import_requests SET job_state='failed',receipt_metadata=$1 WHERE batch_id=$2 AND owner=$3 AND bucket=$4 AND source_key=$5 AND client_token=$6 AND request_contract=$7 AND job_state='pending' AND receipt_metadata IS NULL",
        vec![receipt.into(),claim.batch_id.clone().into(),snapshot.owner.into(),snapshot.bucket.into(),snapshot.source_key.into(),snapshot.token.into(),snapshot.request_contract.into()])).await?;
    if changed.rows_affected() != 1 {
        return Err(stale());
    }
    tx.commit().await?;
    Ok(true)
}
