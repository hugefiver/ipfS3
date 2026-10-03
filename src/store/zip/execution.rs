//! Independent source=false ZIP v2 execution. This module deliberately does
//! not reuse the legacy ZIP batch terminal state or publish a source object.
//! The authenticated caller freezes the signed token and canonical headers;
//! only the ZIP reader can attest clean EOF/trailer before binding input bytes.
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction, QueryResult,
    Statement, TransactionTrait, Value,
};
use sha2::{Digest, Sha256};

use crate::{
    error::{AppError, AppResult},
    store::{
        database_clock::database_now,
        import::ownership::{self, StandardMutationGuard},
    },
};

#[derive(Clone, Debug)]
pub struct Admission {
    pub id: String,
    pub owner: String,
    /// Authenticated entry kind: direct, mpu, or import. Not the source key.
    pub source: String,
    pub token: String,
    /// SHA-256 of the canonical signed request headers, supplied by auth.
    pub request_fingerprint: String,
    pub request_contract: String,
    pub bucket: String,
    pub source_key: String,
    pub captured_options: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub id: String,
    pub owner: String,
    pub source: String,
    pub token: String,
    pub request_fingerprint: String,
    pub request_contract: String,
    pub bucket: String,
    pub source_key: String,
    pub captured_options: String,
    pub input_sha256: Option<String>,
    pub input_art_cid: Option<String>,
    pub input_art_size: Option<i64>,
    pub state: String,
    pub epoch: i64,
    pub worker: Option<String>,
    pub lease_until: Option<DateTime<Utc>>,
    pub terminal_result: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    pub batch_id: String,
    pub epoch: i64,
    pub worker: String,
}

#[derive(Clone, Debug)]
pub enum ManifestItem {
    Success {
        path: String,
        object_key: String,
        cid: String,
        size: i64,
    },
    Failure {
        path: String,
        code: String,
    },
}

fn invalid() -> AppError {
    AppError::InvalidZipParameter("ZIP v2 execution conflict or invalid input".into())
}
fn stale() -> AppError {
    AppError::StaleContentMutation
}
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}
fn required(value: &str) -> bool {
    !value.is_empty() && !value.chars().any(char::is_control)
}

fn statement(
    db: &impl ConnectionTrait,
    sqlite: &str,
    postgres: &str,
    values: Vec<Value>,
) -> Statement {
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

fn supported(db: &impl ConnectionTrait) -> AppResult<()> {
    if matches!(
        db.get_database_backend(),
        DatabaseBackend::Sqlite | DatabaseBackend::Postgres
    ) {
        Ok(())
    } else {
        Err(AppError::Internal(
            "ZIP v2 execution requires SQLite or PostgreSQL".into(),
        ))
    }
}

fn decode(row: QueryResult) -> AppResult<Snapshot> {
    Ok(Snapshot {
        id: row.try_get("", "id")?,
        owner: row.try_get("", "owner")?,
        source: row.try_get("", "source")?,
        token: row.try_get("", "token")?,
        request_fingerprint: row.try_get("", "request_fingerprint")?,
        request_contract: row.try_get("", "request_contract")?,
        bucket: row.try_get("", "bucket")?,
        source_key: row.try_get("", "source_key")?,
        captured_options: row.try_get("", "captured_options")?,
        input_sha256: row.try_get("", "input_sha256")?,
        input_art_cid: row.try_get("", "input_art_cid")?,
        input_art_size: row.try_get("", "input_art_size")?,
        state: row.try_get("", "state")?,
        epoch: row.try_get("", "epoch")?,
        worker: row.try_get("", "worker")?,
        lease_until: row.try_get("", "lease_until")?,
        terminal_result: row.try_get("", "terminal_result")?,
    })
}

pub async fn read<C: ConnectionTrait>(db: &C, id: &str) -> AppResult<Option<Snapshot>> {
    supported(db)?;
    db.query_one(statement(
        db,
        "SELECT * FROM zip_v2_executions WHERE id=?",
        "SELECT * FROM zip_v2_executions WHERE id=$1",
        vec![id.into()],
    ))
    .await?
    .map(decode)
    .transpose()
}

fn same_request(snapshot: &Snapshot, request: &Admission) -> bool {
    snapshot.owner == request.owner
        && snapshot.source == request.source
        && snapshot.token == request.token
        && snapshot.request_fingerprint == request.request_fingerprint
        && snapshot.request_contract == request.request_contract
        && snapshot.bucket == request.bucket
        && snapshot.source_key == request.source_key
}

/// Token identity is immutable. A pending batch is not a successful replay:
/// the caller must read the complete ZIP then compare its SHA-256 with this row.
pub async fn admit(db: &DatabaseConnection, request: &Admission) -> AppResult<Snapshot> {
    let tx = db.begin().await?;
    let snapshot = admit_in_transaction(&tx, request).await?;
    tx.commit().await?;
    Ok(snapshot)
}

/// Restricted intake entry: caller holds the bucket ownership lock and must
/// commit the execution and its request identity row together in this txn.
pub async fn admit_in_transaction(
    tx: &DatabaseTransaction,
    request: &Admission,
) -> AppResult<Snapshot> {
    supported(tx)?;
    if ![
        &request.id,
        &request.owner,
        &request.token,
        &request.bucket,
        &request.source_key,
        &request.request_contract,
        &request.captured_options,
    ]
    .into_iter()
    .all(|s| required(s))
        || !matches!(request.source.as_str(), "direct" | "mpu" | "import")
        || !digest(&request.request_fingerprint)
        || request.request_fingerprint
            != hex::encode(Sha256::digest(request.request_contract.as_bytes()))
        || request.captured_options.len() > 65536
        || serde_json::from_str::<serde_json::Value>(&request.captured_options).is_err()
    {
        return Err(invalid());
    }
    let now = database_now(tx).await?;
    tx.execute(statement(tx,
        "INSERT INTO zip_v2_executions (id,owner,source,token,request_fingerprint,request_contract,bucket,source_key,captured_options,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(owner,source,token) DO NOTHING",
        "INSERT INTO zip_v2_executions (id,owner,source,token,request_fingerprint,request_contract,bucket,source_key,captured_options,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(owner,source,token) DO NOTHING",
        vec![request.id.clone().into(),request.owner.clone().into(),request.source.clone().into(),
             request.token.clone().into(),request.request_fingerprint.clone().into(),request.request_contract.clone().into(),
             request.bucket.clone().into(),request.source_key.clone().into(),request.captured_options.clone().into(),
             now.into(),now.into()])).await?;
    let row = tx
        .query_one(statement(
            tx,
            "SELECT * FROM zip_v2_executions WHERE owner=? AND source=? AND token=?",
            "SELECT * FROM zip_v2_executions WHERE owner=$1 AND source=$2 AND token=$3",
            vec![
                request.owner.clone().into(),
                request.source.clone().into(),
                request.token.clone().into(),
            ],
        ))
        .await?
        .ok_or_else(stale)?;
    let snapshot = decode(row)?;
    if !same_request(&snapshot, request) {
        return Err(invalid());
    }
    Ok(snapshot)
}

/// Only call after the complete input ZIP has reached clean EOF and passed its
/// trailer check. A CID, request ID, or random identifier is *not* this digest.
/// A competing worker cannot bind bytes under a stale claim.
pub async fn bind_clean_input(
    db: &DatabaseConnection,
    claim: &Claim,
    sha256: &str,
    art_cid: &str,
    art_size: i64,
) -> AppResult<()> {
    if !digest(sha256) || !required(art_cid) || art_size < 0 {
        return Err(invalid());
    }
    let initial = read(db, &claim.batch_id).await?.ok_or_else(stale)?;
    let tx = db.begin().await?;
    ownership::lock_bucket_for_ownership(&tx, &initial.bucket).await?;
    let batch = live(&tx, claim).await?;
    if batch.state != "pending" {
        return Err(invalid());
    }
    if let Some(old) = &batch.input_sha256 {
        if old != sha256
            || batch.input_art_cid.as_deref() != Some(art_cid)
            || batch.input_art_size != Some(art_size)
        {
            return Err(invalid());
        }
    } else {
        let changed = tx.execute(statement(&tx,
            "UPDATE zip_v2_executions SET input_sha256=?,input_art_cid=?,input_art_size=?,updated_at=? WHERE id=? AND epoch=? AND input_sha256 IS NULL AND state='pending'",
            "UPDATE zip_v2_executions SET input_sha256=$1,input_art_cid=$2,input_art_size=$3,updated_at=$4 WHERE id=$5 AND epoch=$6 AND input_sha256 IS NULL AND state='pending'",
            vec![sha256.into(),art_cid.into(),art_size.into(),database_now(&tx).await?.into(),claim.batch_id.clone().into(),claim.epoch.into()])).await?;
        if changed.rows_affected() != 1 {
            return Err(stale());
        }
    }
    tx.commit().await?;
    Ok(())
}

/// Replay validation must happen after independently hashing *this* complete
/// request. Never serve a completed response using only token/header identity.
pub async fn read_for_replay(
    db: &DatabaseConnection,
    request: &Admission,
    sha256: &str,
) -> AppResult<Option<Snapshot>> {
    if !digest(sha256) {
        return Err(invalid());
    }
    let row = admit(db, request).await?;
    if let Some(old) = &row.input_sha256
        && old != sha256
    {
        return Err(invalid());
    }
    Ok((row.input_sha256.is_some()).then_some(row))
}

fn claim_matches(batch: &Snapshot, claim: &Claim, now: DateTime<Utc>) -> bool {
    batch.id == claim.batch_id
        && batch.epoch == claim.epoch
        && batch.worker.as_deref() == Some(&claim.worker)
        && batch.lease_until.is_some_and(|until| until > now)
        && matches!(batch.state.as_str(), "pending" | "admitted")
}

async fn live(tx: &DatabaseTransaction, claim: &Claim) -> AppResult<Snapshot> {
    let batch = read(tx, &claim.batch_id).await?.ok_or_else(stale)?;
    if !claim_matches(&batch, claim, database_now(tx).await?) {
        return Err(stale());
    }
    Ok(batch)
}

async fn manifest_keys<C: ConnectionTrait>(db: &C, id: &str) -> AppResult<BTreeSet<String>> {
    let rows = db
        .query_all(statement(
            db,
            "SELECT object_key FROM zip_v2_manifest WHERE batch_id=? AND object_key IS NOT NULL",
            "SELECT object_key FROM zip_v2_manifest WHERE batch_id=$1 AND object_key IS NOT NULL",
            vec![id.into()],
        ))
        .await?;
    rows.into_iter()
        .map(|row| row.try_get("", "object_key").map_err(Into::into))
        .collect()
}

pub async fn read_targets<C: ConnectionTrait>(
    db: &C,
    claim: &Claim,
) -> AppResult<Vec<StandardMutationGuard>> {
    let batch = read(db, &claim.batch_id).await?.ok_or_else(stale)?;
    if batch.epoch != claim.epoch || batch.worker.as_deref() != Some(&claim.worker) {
        return Err(stale());
    }
    let rows = db.query_all(statement(db,
        "SELECT object_key,mutation_id,expected_generation,epoch FROM zip_v2_targets WHERE batch_id=? ORDER BY object_key",
        "SELECT object_key,mutation_id,expected_generation,epoch FROM zip_v2_targets WHERE batch_id=$1 ORDER BY object_key",
        vec![claim.batch_id.clone().into()])).await?;
    let mut guards = Vec::with_capacity(rows.len());
    for row in rows {
        let epoch: i64 = row.try_get("", "epoch")?;
        if epoch != claim.epoch {
            return Err(stale());
        }
        guards.push(StandardMutationGuard {
            bucket: batch.bucket.clone(),
            key: row.try_get("", "object_key")?,
            mutation_id: row.try_get("", "mutation_id")?,
            expected_generation: row.try_get("", "expected_generation")?,
            mutation_prefix: None,
        });
    }
    let actual = guards
        .iter()
        .map(|g| g.key.clone())
        .collect::<BTreeSet<_>>();
    if actual.len() != guards.len()
        || actual != manifest_keys(db, &claim.batch_id).await?
        || actual.contains(&batch.source_key)
    {
        return Err(stale());
    }
    Ok(guards)
}

/// Caller holds the bucket ownership lock in this transaction. Verifies the
/// entire original guard set; never admits a missing guard a second time.
pub async fn verify_in_transaction(
    tx: &DatabaseTransaction,
    claim: &Claim,
) -> AppResult<Vec<StandardMutationGuard>> {
    let batch = live(tx, claim).await?;
    if batch.state != "admitted" {
        return Err(stale());
    }
    let guards = read_targets(tx, claim).await?;
    let keys = guards.iter().map(|g| g.key.clone()).collect::<Vec<_>>();
    ownership::verify_zip_output_guards_in_transaction(
        tx,
        &batch.bucket,
        &batch.source_key,
        &keys,
        &guards,
    )
    .await?;
    Ok(guards)
}

fn valid_manifest(
    items: &[ManifestItem],
    source_key: &str,
    mutation_ids: &BTreeMap<String, String>,
) -> AppResult<BTreeSet<String>> {
    let mut paths = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for item in items {
        let path = match item {
            ManifestItem::Success { path, .. } | ManifestItem::Failure { path, .. } => path,
        };
        if !required(path)
            || path.starts_with('/')
            || path.contains('\\')
            || path
                .split('/')
                .any(|s| s.is_empty() || s == "." || s == "..")
            || !paths.insert(path)
        {
            return Err(invalid());
        }
        match item {
            ManifestItem::Success {
                object_key,
                cid,
                size,
                ..
            } => {
                if !required(object_key)
                    || object_key == source_key
                    || !required(cid)
                    || *size < 0
                    || !keys.insert(object_key.clone())
                {
                    return Err(invalid());
                }
            }
            ManifestItem::Failure { code, .. } if !required(code) || code.len() > 128 => {
                return Err(invalid());
            }
            _ => {}
        }
    }
    if keys.len() != mutation_ids.len()
        || keys
            .iter()
            .any(|key| mutation_ids.get(key).is_none_or(|id| !required(id)))
    {
        return Err(invalid());
    }
    Ok(keys)
}

/// Call inside one short transaction AFTER lock_bucket_for_ownership. The
/// source-safe admission, complete final manifest, guards and batch transition
/// either commit together or all roll back. Do not commit after an error.
pub async fn admit_manifest_in_transaction(
    tx: &DatabaseTransaction,
    claim: &Claim,
    items: &[ManifestItem],
    mutation_ids: &BTreeMap<String, String>,
) -> AppResult<()> {
    let batch = live(tx, claim).await?;
    if batch.state != "pending" || batch.input_sha256.is_none() {
        return Err(stale());
    }
    let keys = valid_manifest(items, &batch.source_key, mutation_ids)?;
    let now = database_now(tx).await?;
    let guards = ownership::admit_zip_outputs_without_source_in_transaction(
        tx,
        &batch.bucket,
        &batch.source_key,
        &keys,
        mutation_ids,
        now,
    )
    .await?;
    for item in items {
        let (path, key, cid, size, code) = match item {
            ManifestItem::Success {
                path,
                object_key,
                cid,
                size,
            } => (
                path,
                Some(object_key.clone()),
                Some(cid.clone()),
                Some(*size),
                None,
            ),
            ManifestItem::Failure { path, code } => (path, None, None, None, Some(code.clone())),
        };
        tx.execute(statement(tx,
            "INSERT INTO zip_v2_manifest (batch_id,path,object_key,cid,size,error_code,created_at) VALUES (?,?,?,?,?,?,?)",
            "INSERT INTO zip_v2_manifest (batch_id,path,object_key,cid,size,error_code,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)",
            vec![claim.batch_id.clone().into(),path.clone().into(),key.into(),cid.into(),size.into(),code.into(),now.into()])).await?;
    }
    for guard in guards {
        if guard.mutation_prefix.is_some() || guard.key == batch.source_key {
            return Err(stale());
        }
        tx.execute(statement(tx,
            "INSERT INTO zip_v2_targets (batch_id,object_key,mutation_id,expected_generation,epoch) VALUES (?,?,?,?,?)",
            "INSERT INTO zip_v2_targets (batch_id,object_key,mutation_id,expected_generation,epoch) VALUES ($1,$2,$3,$4,$5)",
            vec![claim.batch_id.clone().into(),guard.key.into(),guard.mutation_id.into(),guard.expected_generation.into(),claim.epoch.into()])).await?;
    }
    let changed = tx.execute(statement(tx,
        "UPDATE zip_v2_executions SET state='admitted',updated_at=? WHERE id=? AND epoch=? AND worker=? AND state='pending'",
        "UPDATE zip_v2_executions SET state='admitted',updated_at=$1 WHERE id=$2 AND epoch=$3 AND worker=$4 AND state='pending'",
        vec![now.into(),claim.batch_id.clone().into(),claim.epoch.into(),claim.worker.clone().into()])).await?;
    if changed.rows_affected() != 1 {
        return Err(stale());
    }
    Ok(())
}

async fn mark_fenced(tx: &DatabaseTransaction, batch: &Snapshot, claim: &Claim) -> AppResult<()> {
    let now = database_now(tx).await?;
    let changed = tx.execute(statement(tx,
        "UPDATE zip_v2_executions SET state='fenced',terminal_result='fenced',updated_at=? WHERE id=? AND epoch=? AND worker=? AND state='admitted'",
        "UPDATE zip_v2_executions SET state='fenced',terminal_result='fenced',updated_at=$1 WHERE id=$2 AND epoch=$3 AND worker=$4 AND state='admitted'",
        vec![now.into(),batch.id.clone().into(),claim.epoch.into(),claim.worker.clone().into()])).await?;
    if changed.rows_affected() != 1 {
        return Err(stale());
    }
    Ok(())
}

/// Change the *same* surviving authority to a new token; neither a fresh
/// admission nor a generation bump. Old request-owned cleanup must no longer
/// be able to clear B's guard after A crashed. The bucket lock serializes all
/// three CAS writes; failure rolls the entire transaction back.
async fn rotate_guard(
    tx: &DatabaseTransaction,
    id: &str,
    old: &StandardMutationGuard,
    new_epoch: i64,
    now: DateTime<Utc>,
) -> AppResult<StandardMutationGuard> {
    let new_token = format!("zip-v2:{id}:{new_epoch}:{}", uuid::Uuid::new_v4());
    let destination = tx.execute(statement(tx,
        "UPDATE import_destinations SET mutation_id=?,updated_at=? WHERE bucket=? AND key=? AND mutation_id=? AND generation=? AND mutation_prefix IS NULL",
        "UPDATE import_destinations SET mutation_id=$1,updated_at=$2 WHERE bucket=$3 AND key=$4 AND mutation_id=$5 AND generation=$6 AND mutation_prefix IS NULL",
        vec![new_token.clone().into(),now.into(),old.bucket.clone().into(),old.key.clone().into(),old.mutation_id.clone().into(),old.expected_generation.into()])).await?;
    if destination.rows_affected() != 1 {
        return Err(stale());
    }
    let lease = tx.execute(statement(tx,
        "UPDATE standard_mutation_leases SET mutation_id=?,lease_until=? WHERE bucket=? AND key=? AND mutation_id=? AND generation=? AND julianday(lease_until)>julianday('now')",
        "UPDATE standard_mutation_leases SET mutation_id=$1,lease_until=$2 WHERE bucket=$3 AND key=$4 AND mutation_id=$5 AND generation=$6 AND lease_until>clock_timestamp()",
        vec![new_token.clone().into(),(now+Duration::seconds(ownership::STANDARD_MUTATION_LEASE_SECONDS)).into(),
             old.bucket.clone().into(),old.key.clone().into(),old.mutation_id.clone().into(),old.expected_generation.into()])).await?;
    if lease.rows_affected() != 1 {
        return Err(stale());
    }
    let target = tx.execute(statement(tx,
        "UPDATE zip_v2_targets SET mutation_id=?,epoch=? WHERE batch_id=? AND object_key=? AND mutation_id=? AND expected_generation=?",
        "UPDATE zip_v2_targets SET mutation_id=$1,epoch=$2 WHERE batch_id=$3 AND object_key=$4 AND mutation_id=$5 AND expected_generation=$6",
        vec![new_token.clone().into(),new_epoch.into(),id.into(),old.key.clone().into(),old.mutation_id.clone().into(),old.expected_generation.into()])).await?;
    if target.rows_affected() != 1 {
        return Err(stale());
    }
    Ok(StandardMutationGuard {
        mutation_id: new_token,
        ..old.clone()
    })
}

/// Lease expiry permits takeover only while every exact guard still belongs to
/// the batch. Rotation of epoch/worker and target epochs shares one bucket-first
/// transaction. A lost guard records a terminal fence, never a re-admission.
pub async fn claim(
    db: &DatabaseConnection,
    id: &str,
    worker: &str,
    lease_seconds: i64,
) -> AppResult<Option<Claim>> {
    if !required(worker) || !(1..=60).contains(&lease_seconds) {
        return Err(invalid());
    }
    let Some(initial) = read(db, id).await? else {
        return Ok(None);
    };
    let tx = db.begin().await?;
    ownership::lock_bucket_for_ownership(&tx, &initial.bucket).await?;
    let batch = read(&tx, id).await?.ok_or_else(stale)?;
    let now = database_now(&tx).await?;
    if batch.bucket != initial.bucket
        || !matches!(batch.state.as_str(), "pending" | "admitted")
        || batch.lease_until.is_some_and(|until| until > now)
    {
        return Ok(None);
    }
    if batch.epoch == i64::MAX {
        return Err(stale());
    }
    let old = Claim {
        batch_id: id.into(),
        epoch: batch.epoch,
        worker: batch.worker.clone().unwrap_or_default(),
    };
    let mut surviving_guards = Vec::new();
    if batch.state == "admitted" {
        // Verify against the old epoch before rotating any target. A prefix
        // takeover or even one missing guard permanently fences the batch.
        let guards = read_targets(&tx, &old).await;
        let verified = match guards {
            Ok(guards) => {
                let keys = guards.iter().map(|g| g.key.clone()).collect::<Vec<_>>();
                let verified = ownership::verify_zip_output_guards_in_transaction(
                    &tx,
                    &batch.bucket,
                    &batch.source_key,
                    &keys,
                    &guards,
                )
                .await;
                surviving_guards = guards;
                verified
            }
            Err(AppError::StaleContentMutation) => Err(stale()),
            Err(other) => return Err(other),
        };
        match verified {
            Ok(()) => {}
            Err(AppError::StaleContentMutation) => {
                mark_fenced(&tx, &batch, &old).await?;
                tx.commit().await?;
                return Ok(None);
            }
            Err(other) => return Err(other),
        }
    }
    let epoch = batch.epoch + 1;
    let until = now + Duration::seconds(lease_seconds);
    let mut rotated = Vec::with_capacity(surviving_guards.len());
    for guard in &surviving_guards {
        match rotate_guard(&tx, id, guard, epoch, now).await {
            Ok(rotated_guard) => rotated.push(rotated_guard),
            Err(AppError::StaleContentMutation) => {
                tx.rollback().await?;
                fence_if_lost(db, &old).await?;
                return Ok(None);
            }
            Err(other) => return Err(other),
        }
    }
    if !rotated.is_empty() {
        let keys = rotated.iter().map(|g| g.key.clone()).collect::<Vec<_>>();
        ownership::verify_zip_output_guards_in_transaction(
            &tx,
            &batch.bucket,
            &batch.source_key,
            &keys,
            &rotated,
        )
        .await?;
    }
    let changed = tx.execute(statement(&tx,
        "UPDATE zip_v2_executions SET epoch=?,worker=?,lease_until=?,updated_at=? WHERE id=? AND epoch=? AND state=?",
        "UPDATE zip_v2_executions SET epoch=$1,worker=$2,lease_until=$3,updated_at=$4 WHERE id=$5 AND epoch=$6 AND state=$7",
        vec![epoch.into(),worker.into(),until.into(),now.into(),id.into(),batch.epoch.into(),batch.state.into()])).await?;
    if changed.rows_affected() != 1 {
        return Err(stale());
    }
    tx.commit().await?;
    Ok(Some(Claim {
        batch_id: id.into(),
        epoch,
        worker: worker.into(),
    }))
}

/// Returns false for a stale worker or lost guard; on lost guard persists the
/// fenced terminal state without touching a successor's ownership token.
pub async fn renew(db: &DatabaseConnection, claim: &Claim, lease_seconds: i64) -> AppResult<bool> {
    if !(1..=60).contains(&lease_seconds) {
        return Err(invalid());
    }
    let Some(initial) = read(db, &claim.batch_id).await? else {
        return Ok(false);
    };
    let tx = db.begin().await?;
    ownership::lock_bucket_for_ownership(&tx, &initial.bucket).await?;
    let batch = read(&tx, &claim.batch_id).await?.ok_or_else(stale)?;
    let now = database_now(&tx).await?;
    if !claim_matches(&batch, claim, now) {
        return Ok(false);
    }
    if batch.state == "admitted" {
        let guards = read_targets(&tx, claim).await;
        let renewed = match guards {
            Ok(guards) => {
                let keys = guards.iter().map(|g| g.key.clone()).collect::<Vec<_>>();
                ownership::renew_zip_output_guards_in_transaction(
                    &tx,
                    &batch.bucket,
                    &batch.source_key,
                    &keys,
                    &guards,
                )
                .await
            }
            Err(AppError::StaleContentMutation) => Err(stale()),
            Err(other) => return Err(other),
        };
        match renewed {
            Ok(()) => {}
            Err(AppError::StaleContentMutation) => {
                // Ownership may have returned after partial writes: drop the
                // transaction first, then persist only the batch fence.
                tx.rollback().await?;
                fence_if_lost(db, claim).await?;
                return Ok(false);
            }
            Err(other) => return Err(other),
        }
    }
    let changed = tx.execute(statement(&tx,
        "UPDATE zip_v2_executions SET lease_until=?,updated_at=? WHERE id=? AND epoch=? AND worker=? AND state=?",
        "UPDATE zip_v2_executions SET lease_until=$1,updated_at=$2 WHERE id=$3 AND epoch=$4 AND worker=$5 AND state=$6",
        vec![(now+Duration::seconds(lease_seconds)).into(),now.into(),claim.batch_id.clone().into(),claim.epoch.into(),claim.worker.clone().into(),batch.state.into()])).await?;
    if changed.rows_affected() != 1 {
        return Err(stale());
    }
    tx.commit().await?;
    Ok(true)
}

/// Re-check a failed publication in a clean bucket-first transaction. Fences
/// only if this *same epoch* has actually lost some original guard.
pub async fn fence_if_lost(db: &DatabaseConnection, claim: &Claim) -> AppResult<bool> {
    let Some(initial) = read(db, &claim.batch_id).await? else {
        return Ok(false);
    };
    let tx = db.begin().await?;
    ownership::lock_bucket_for_ownership(&tx, &initial.bucket).await?;
    let batch = read(&tx, &claim.batch_id).await?.ok_or_else(stale)?;
    if batch.epoch != claim.epoch
        || batch.worker.as_deref() != Some(&claim.worker)
        || batch.state != "admitted"
    {
        return Ok(false);
    }
    let guards = read_targets(&tx, claim).await;
    let verified = match guards {
        Ok(guards) => {
            let keys = guards.iter().map(|g| g.key.clone()).collect::<Vec<_>>();
            ownership::verify_zip_output_guards_in_transaction(
                &tx,
                &batch.bucket,
                &batch.source_key,
                &keys,
                &guards,
            )
            .await
        }
        Err(AppError::StaleContentMutation) => Err(stale()),
        Err(other) => return Err(other),
    };
    match verified {
        Ok(()) => return Ok(false),
        Err(AppError::StaleContentMutation) => {}
        Err(other) => return Err(other),
    }
    mark_fenced(&tx, &batch, claim).await?;
    tx.commit().await?;
    Ok(true)
}

/// Call in the same bucket-locked publication transaction, after the exact
/// output object writes. Commit this transaction only if completion succeeds.
/// Clearing all guards is its final authorization write.
pub async fn complete_in_transaction(
    tx: &DatabaseTransaction,
    claim: &Claim,
    terminal_result: &str,
) -> AppResult<()> {
    if !required(terminal_result)
        || serde_json::from_str::<serde_json::Value>(terminal_result).is_err()
    {
        return Err(invalid());
    }
    let batch = live(tx, claim).await?;
    let guards = verify_in_transaction(tx, claim).await?;
    let keys = guards.iter().map(|g| g.key.clone()).collect::<Vec<_>>();
    let now = database_now(tx).await?;
    let changed = tx.execute(statement(tx,
        "UPDATE zip_v2_executions SET state='completed',terminal_result=?,updated_at=? WHERE id=? AND epoch=? AND worker=? AND state='admitted'",
        "UPDATE zip_v2_executions SET state='completed',terminal_result=$1,updated_at=$2 WHERE id=$3 AND epoch=$4 AND worker=$5 AND state='admitted'",
        vec![terminal_result.into(),now.into(),claim.batch_id.clone().into(),claim.epoch.into(),claim.worker.clone().into()])).await?;
    if changed.rows_affected() != 1 {
        return Err(stale());
    }
    ownership::complete_zip_output_guards_in_transaction(
        tx,
        &batch.bucket,
        &batch.source_key,
        &keys,
        &guards,
        now,
    )
    .await?;
    Ok(())
}

/// Convenience for an otherwise empty short transaction; publication callers
/// use complete_in_transaction so object writes and guard cleanup are atomic.
pub async fn complete(db: &DatabaseConnection, claim: &Claim, result: &str) -> AppResult<()> {
    let batch = read(db, &claim.batch_id).await?.ok_or_else(stale)?;
    let tx = db.begin().await?;
    ownership::lock_bucket_for_ownership(&tx, &batch.bucket).await?;
    if let Err(error) = complete_in_transaction(&tx, claim, result).await {
        tx.rollback().await?;
        fence_if_lost(db, claim).await?;
        return Err(error);
    }
    tx.commit().await?;
    Ok(())
}

/// Terminalize a still-owned empty/failed attempt. Does not clear any guard:
/// safe cleanup of an abandoned guard is left to existing lease recovery.
pub async fn fence(db: &DatabaseConnection, claim: &Claim) -> AppResult<bool> {
    let Some(initial) = read(db, &claim.batch_id).await? else {
        return Ok(false);
    };
    let tx = db.begin().await?;
    ownership::lock_bucket_for_ownership(&tx, &initial.bucket).await?;
    let batch = read(&tx, &claim.batch_id).await?.ok_or_else(stale)?;
    if !claim_matches(&batch, claim, database_now(&tx).await?) {
        return Ok(false);
    }
    let changed = tx.execute(statement(&tx,
        "UPDATE zip_v2_executions SET state='fenced',terminal_result='fenced',updated_at=? WHERE id=? AND epoch=? AND worker=? AND state IN ('pending','admitted')",
        "UPDATE zip_v2_executions SET state='fenced',terminal_result='fenced',updated_at=$1 WHERE id=$2 AND epoch=$3 AND worker=$4 AND state IN ('pending','admitted')",
        vec![database_now(&tx).await?.into(),claim.batch_id.clone().into(),claim.epoch.into(),claim.worker.clone().into()])).await?;
    if changed.rows_affected() != 1 {
        return Err(stale());
    }
    tx.commit().await?;
    Ok(true)
}
