use std::collections::BTreeMap;

use chrono::Utc;
use sea_orm::{
    AccessMode, ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection,
    DatabaseTransaction, EntityTrait, IsolationLevel, QueryFilter, QueryOrder, Set, Statement,
    TransactionTrait, Value, sea_query::OnConflict,
};
use sha2::{Digest, Sha256};

use super::{BatchSnapshot, invalid, lock_batch, required, supported};
use crate::error::{AppError, AppResult};
use crate::store::entities::{
    object, object_version, zip_batch, zip_manifest_entry, zip_root_build, zip_root_reference,
};

#[derive(Clone, Debug)]
pub struct BatchAdmission {
    pub id: String,
    pub owner: String,
    /// direct, mpu or import. MPU upload ID / import job ID / signed direct v2 token.
    pub source: String,
    pub token: String,
    /// For a v2 mirror use the literal "pending", not a guessed ZIP digest.
    /// The v2 execution owns the signed request contract and binds the actual
    /// input SHA-256 only after the reader reaches clean EOF.
    pub fingerprint: String,
    pub bucket: String,
    pub archive_key: String,
    /// For a v2 mirror use the literal "pending" until v2 binds input bytes.
    pub input_identity: String,
    /// Captured options/policy as an immutable serialized snapshot.
    pub captured_options: String,
}

/// This is the parsed CompleteMultipartUpload body, not the set of stored parts.
/// ETag presence, strong/weak form and each checksum are part of the contract.
pub fn complete_request_contract(parts: &[s3s::dto::CompletedPart]) -> AppResult<String> {
    if parts.is_empty() || parts.len() > 10_000 {
        return Err(invalid());
    }
    let mut last = 0;
    let mut contract = Vec::with_capacity(parts.len());
    for part in parts {
        let number = part.part_number.ok_or_else(invalid)?;
        if number <= last || number > 10_000 {
            return Err(invalid());
        }
        last = number;
        let etag = match &part.e_tag {
            Some(s3s::dto::ETag::Strong(value)) => serde_json::json!(["strong", value]),
            Some(s3s::dto::ETag::Weak(value)) => serde_json::json!(["weak", value]),
            None => serde_json::Value::Null,
        };
        contract.push(serde_json::json!({
            "part_number": number, "etag": etag,
            "crc32": part.checksum_crc32, "crc32c": part.checksum_crc32c,
            "crc64nvme": part.checksum_crc64nvme, "sha1": part.checksum_sha1,
            "sha256": part.checksum_sha256,
        }));
    }
    serde_json::to_string(&contract).map_err(|_| invalid())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedUploadResult {
    pub archive_cid: String,
    pub archive_size: i64,
    /// Public S3 VersionId, not the private version-row ID. None is valid.
    pub public_version_id: Option<String>,
    pub server_side_encryption: Option<String>,
    /// Exact response XML produced from the actually published manifest.
    pub response_xml: String,
    /// Snapshot of the first successful response, independent of later root retries.
    pub response_headers: BTreeMap<String, String>,
}

impl CompletedUploadResult {
    pub fn into_response_parts(self) -> (BTreeMap<String, String>, String) {
        (self.response_headers, self.response_xml)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayLookup {
    Missing,
    Pending,
    Completed(CompletedUploadResult),
}

/// Batch identity is checked against authenticated principal before inspecting
/// any result. The initiator's captured ZIP options remain immutable in the batch.
pub struct CompleteRequest<'a> {
    pub owner: &'a str,
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
    pub parts: &'a [s3s::dto::CompletedPart],
}

fn replay_conflict() -> AppError {
    AppError::InvalidZipParameter("ZIP multipart Complete request conflict".into())
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

fn verify_complete_batch(batch: &zip_batch::Model, request: &CompleteRequest<'_>) -> AppResult<()> {
    if batch.source != "mpu"
        || batch.id != request.upload_id
        || batch.token != request.upload_id
        || batch.input_identity != request.upload_id
        || batch.owner != request.owner
        || batch.bucket != request.bucket
        || batch.archive_key != request.key
        || serde_json::from_str::<serde_json::Value>(&batch.captured_options).is_err()
    {
        return Err(replay_conflict());
    }
    Ok(())
}

fn decode_result(row: &sea_orm::QueryResult) -> AppResult<Option<CompletedUploadResult>> {
    let prepared: Option<String> = row.try_get("", "prepared_archive_cid")?;
    let cid: Option<String> = row.try_get("", "archive_cid")?;
    let size: Option<i64> = row.try_get("", "archive_size")?;
    let version: Option<String> = row.try_get("", "public_version_id")?;
    let sse: Option<String> = row.try_get("", "server_side_encryption")?;
    let xml: Option<String> = row.try_get("", "response_xml")?;
    let headers: Option<String> = row.try_get("", "response_headers_json")?;
    match (cid, size, xml, headers) {
        (None, None, None, None) if version.is_none() && sse.is_none() => Ok(None),
        (Some(archive_cid), Some(archive_size), Some(response_xml), Some(headers))
            if !archive_cid.is_empty()
                && prepared.as_deref() == Some(archive_cid.as_str())
                && archive_size >= 0
                && !response_xml.is_empty()
                && version.as_deref().is_none_or(|v| !v.is_empty())
                && sse.as_deref().is_none_or(|v| v == "AES256") =>
        {
            Ok(Some(CompletedUploadResult {
                archive_cid,
                archive_size,
                public_version_id: version,
                server_side_encryption: sse,
                response_xml,
                response_headers: serde_json::from_str(&headers).map_err(|_| {
                    AppError::Internal("invalid persisted ZIP MPU response headers".into())
                })?,
            }))
        }
        _ => Err(AppError::Internal(
            "invalid persisted ZIP MPU completion shape".into(),
        )),
    }
}

/// The original Complete request owns the first archive CID even if a later
/// object-publication transaction rolls back. The ZIP batch stays open/NULL.
pub async fn capture_prepared_archive(
    db: &DatabaseConnection,
    request: &CompleteRequest<'_>,
    cid: &str,
) -> AppResult<()> {
    supported(db)?;
    if !required(cid) {
        return Err(invalid());
    }
    let contract = complete_request_contract(request.parts)?;
    let digest = hex::encode(Sha256::digest(contract.as_bytes()));
    let tx = db.begin().await?;
    let batch = lock_batch(&tx, request.upload_id).await?;
    verify_complete_batch(&batch, request)?;
    if batch.state != "open" {
        return Err(replay_conflict());
    }
    let row = tx.query_one(statement(&tx,
        "SELECT request_fingerprint,request_contract,prepared_archive_cid FROM zip_mpu_replays WHERE batch_id=?",
        "SELECT request_fingerprint,request_contract,prepared_archive_cid FROM zip_mpu_replays WHERE batch_id=$1",
        vec![request.upload_id.into()],
    )).await?.ok_or_else(super::stale)?;
    if row.try_get::<String>("", "request_fingerprint")? != digest
        || row.try_get::<String>("", "request_contract")? != contract
    {
        return Err(replay_conflict());
    }
    let prepared: Option<String> = row.try_get("", "prepared_archive_cid")?;
    if let Some(prepared) = prepared {
        if prepared != cid {
            return Err(replay_conflict());
        }
    } else {
        let changed = tx.execute(statement(&tx,
            "UPDATE zip_mpu_replays SET prepared_archive_cid=? WHERE batch_id=? AND prepared_archive_cid IS NULL AND archive_cid IS NULL",
            "UPDATE zip_mpu_replays SET prepared_archive_cid=$1 WHERE batch_id=$2 AND prepared_archive_cid IS NULL AND archive_cid IS NULL",
            vec![cid.into(), request.upload_id.into()],
        )).await?;
        if changed.rows_affected() != 1 {
            return Err(super::stale());
        }
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn verify_prepared_archive(
    tx: &DatabaseTransaction,
    id: &str,
    cid: &str,
) -> AppResult<()> {
    let row = tx
        .query_one(statement(
            tx,
            "SELECT prepared_archive_cid FROM zip_mpu_replays WHERE batch_id=?",
            "SELECT prepared_archive_cid FROM zip_mpu_replays WHERE batch_id=$1",
            vec![id.into()],
        ))
        .await?
        .ok_or_else(super::stale)?;
    if row
        .try_get::<Option<String>>("", "prepared_archive_cid")?
        .as_deref()
        != Some(cid)
    {
        return Err(super::stale());
    }
    Ok(())
}

/// Capture before cat/add and before prepare_manifest, after validating the
/// upload's actual parts. A changed request never replaces the first contract.
pub async fn capture_complete_request(
    db: &DatabaseConnection,
    request: &CompleteRequest<'_>,
) -> AppResult<()> {
    supported(db)?;
    let contract = complete_request_contract(request.parts)?;
    let digest = hex::encode(Sha256::digest(contract.as_bytes()));
    let tx = db.begin().await?;
    let batch = lock_batch(&tx, request.upload_id).await?;
    verify_complete_batch(&batch, request)?;
    if batch.state != "open" {
        return Err(replay_conflict());
    }
    tx.execute(statement(&tx,
        "INSERT INTO zip_mpu_replays (batch_id,request_fingerprint,request_contract) VALUES (?,?,?) ON CONFLICT (batch_id) DO NOTHING",
        "INSERT INTO zip_mpu_replays (batch_id,request_fingerprint,request_contract) VALUES ($1,$2,$3) ON CONFLICT (batch_id) DO NOTHING",
        vec![request.upload_id.into(), digest.clone().into(), contract.clone().into()],
    )).await?;
    let found = tx
        .query_one(statement(
            &tx,
            "SELECT request_fingerprint,request_contract FROM zip_mpu_replays WHERE batch_id=?",
            "SELECT request_fingerprint,request_contract FROM zip_mpu_replays WHERE batch_id=$1",
            vec![request.upload_id.into()],
        ))
        .await?
        .ok_or_else(super::stale)?;
    if found.try_get::<String>("", "request_fingerprint")? != digest
        || found.try_get::<String>("", "request_contract")? != contract
    {
        return Err(replay_conflict());
    }
    tx.commit().await?;
    Ok(())
}

/// Safe to call after get_upload returns NoSuchUpload. Never resolves the
/// current object key and never accepts upload_id without owner + full contract.
pub async fn replay_lookup(
    db: &DatabaseConnection,
    request: &CompleteRequest<'_>,
) -> AppResult<ReplayLookup> {
    supported(db)?;
    let tx = if db.get_database_backend() == DatabaseBackend::Postgres {
        db.begin_with_config(
            Some(IsolationLevel::RepeatableRead),
            Some(AccessMode::ReadOnly),
        )
        .await?
    } else {
        db.begin().await?
    };
    let Some(batch) = zip_batch::Entity::find_by_id(request.upload_id)
        .one(&tx)
        .await?
    else {
        tx.commit().await?;
        return Ok(ReplayLookup::Missing);
    };
    let contract = complete_request_contract(request.parts)?;
    let digest = hex::encode(Sha256::digest(contract.as_bytes()));
    verify_complete_batch(&batch, request)?;
    let row = tx.query_one(statement(&tx,
        "SELECT request_fingerprint,request_contract,prepared_archive_cid,archive_cid,archive_size,public_version_id,server_side_encryption,response_xml,response_headers_json FROM zip_mpu_replays WHERE batch_id=?",
        "SELECT request_fingerprint,request_contract,prepared_archive_cid,archive_cid,archive_size,public_version_id,server_side_encryption,response_xml,response_headers_json FROM zip_mpu_replays WHERE batch_id=$1",
        vec![request.upload_id.into()],
    )).await?;
    tx.commit().await?;
    let Some(row) = row else {
        return Ok(ReplayLookup::Pending);
    };
    if row.try_get::<String>("", "request_fingerprint")? != digest
        || row.try_get::<String>("", "request_contract")? != contract
    {
        return Err(replay_conflict());
    }
    let result = decode_result(&row)?;
    match (batch.state.as_str(), result) {
        ("published", Some(result)) if batch.source_published => {
            Ok(ReplayLookup::Completed(result))
        }
        ("open", None) | ("published", None) => Ok(ReplayLookup::Pending),
        _ => Err(AppError::Internal(
            "inconsistent ZIP MPU replay publication".into(),
        )),
    }
}

/// Publication worker: call inside the same object/version/batch transaction
/// after `zip::publish` and before COMMIT. Pass the actual archive receipt and
/// response XML built from the published manifest, never the current key.
pub async fn completed_upload_result(
    tx: &DatabaseTransaction,
    request: &CompleteRequest<'_>,
    archive_object_id: &str,
    result: &CompletedUploadResult,
) -> AppResult<()> {
    supported(tx)?;
    let contract = complete_request_contract(request.parts)?;
    let digest = hex::encode(Sha256::digest(contract.as_bytes()));
    let batch = lock_batch(tx, request.upload_id).await?;
    verify_complete_batch(&batch, request)?;
    if batch.state != "published"
        || !batch.source_published
        || result.archive_cid.is_empty()
        || result.archive_size < 0
        || result.response_xml.is_empty()
        || result.response_headers.is_empty()
        || result
            .public_version_id
            .as_deref()
            .is_some_and(str::is_empty)
        || !matches!(
            result.server_side_encryption.as_deref(),
            None | Some("AES256")
        )
    {
        return Err(replay_conflict());
    }
    verify_prepared_archive(tx, request.upload_id, &result.archive_cid).await?;
    // The result must be tied to the archive version that this very
    // publication transaction wrote, not a later current-key read.
    let archive = object::Entity::find_by_id(archive_object_id)
        .one(tx)
        .await?
        .ok_or_else(super::stale)?;
    let versions = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(archive_object_id))
        .all(tx)
        .await?;
    if archive.bucket != request.bucket
        || archive.key != request.key
        || archive.cid != result.archive_cid
        || archive.size != result.archive_size
        || !archive.multipart
        || versions.len() != 1
        || versions[0].kind != "object"
        || versions[0].bucket != request.bucket
        || versions[0].key != request.key
        || result.server_side_encryption.as_deref()
            != (archive.encrypted && archive.key_wrap.is_some()).then_some("AES256")
    {
        return Err(replay_conflict());
    }
    let expected_public_version =
        match crate::store::bucket::get_versioning_state(tx, request.bucket).await? {
            crate::store::object_version::BucketVersioningState::Unversioned => None,
            crate::store::object_version::BucketVersioningState::Enabled => {
                versions[0].version_id.as_deref()
            }
            crate::store::object_version::BucketVersioningState::Suspended => Some("null"),
        };
    if result.public_version_id.as_deref() != expected_public_version {
        return Err(replay_conflict());
    }
    let row = tx.query_one(statement(tx,
        "SELECT request_fingerprint,request_contract,prepared_archive_cid,archive_cid,archive_size,public_version_id,server_side_encryption,response_xml,response_headers_json FROM zip_mpu_replays WHERE batch_id=?",
        "SELECT request_fingerprint,request_contract,prepared_archive_cid,archive_cid,archive_size,public_version_id,server_side_encryption,response_xml,response_headers_json FROM zip_mpu_replays WHERE batch_id=$1",
        vec![request.upload_id.into()],
    )).await?.ok_or_else(super::stale)?;
    if row.try_get::<String>("", "request_fingerprint")? != digest
        || row.try_get::<String>("", "request_contract")? != contract
    {
        return Err(replay_conflict());
    }
    if let Some(existing) = decode_result(&row)? {
        return if existing == *result {
            Ok(())
        } else {
            Err(replay_conflict())
        };
    }
    let changed = tx.execute(statement(tx,
        "UPDATE zip_mpu_replays SET archive_cid=?,archive_size=?,public_version_id=?,server_side_encryption=?,response_xml=?,response_headers_json=? WHERE batch_id=? AND archive_cid IS NULL AND prepared_archive_cid=?",
        "UPDATE zip_mpu_replays SET archive_cid=$1,archive_size=$2,public_version_id=$3,server_side_encryption=$4,response_xml=$5,response_headers_json=$6 WHERE batch_id=$7 AND archive_cid IS NULL AND prepared_archive_cid=$8",
        vec![result.archive_cid.clone().into(), result.archive_size.into(), result.public_version_id.clone().into(),
              result.server_side_encryption.clone().into(), result.response_xml.clone().into(),
              serde_json::to_string(&result.response_headers).map_err(|_| invalid())?.into(),
              request.upload_id.into(), result.archive_cid.clone().into()],
    )).await?;
    if changed.rows_affected() != 1 {
        return Err(super::stale());
    }
    Ok(())
}

// `zip::BatchAdmission` is already publicly re-exported; these associated
// functions expose the store seam without editing the concurrently-owned mod.rs.
impl BatchAdmission {
    pub async fn capture_prepared_archive(
        db: &DatabaseConnection,
        owner: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[s3s::dto::CompletedPart],
        cid: &str,
    ) -> AppResult<()> {
        capture_prepared_archive(
            db,
            &CompleteRequest {
                owner,
                bucket,
                key,
                upload_id,
                parts,
            },
            cid,
        )
        .await
    }
    pub async fn capture_mpu_complete(
        db: &DatabaseConnection,
        owner: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[s3s::dto::CompletedPart],
    ) -> AppResult<()> {
        capture_complete_request(
            db,
            &CompleteRequest {
                owner,
                bucket,
                key,
                upload_id,
                parts,
            },
        )
        .await
    }
    pub async fn replay_lookup(
        db: &DatabaseConnection,
        owner: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[s3s::dto::CompletedPart],
    ) -> AppResult<ReplayLookup> {
        replay_lookup(
            db,
            &CompleteRequest {
                owner,
                bucket,
                key,
                upload_id,
                parts,
            },
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn completed_upload_result(
        tx: &DatabaseTransaction,
        owner: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[s3s::dto::CompletedPart],
        archive_object_id: &str,
        archive_cid: &str,
        archive_size: i64,
        public_version_id: Option<&str>,
        server_side_encryption: Option<&str>,
        response_xml: &str,
        response_headers: BTreeMap<String, String>,
    ) -> AppResult<()> {
        completed_upload_result(
            tx,
            &CompleteRequest {
                owner,
                bucket,
                key,
                upload_id,
                parts,
            },
            archive_object_id,
            &CompletedUploadResult {
                archive_cid: archive_cid.into(),
                archive_size,
                public_version_id: public_version_id.map(str::to_owned),
                server_side_encryption: server_side_encryption.map(str::to_owned),
                response_xml: response_xml.into(),
                response_headers,
            },
        )
        .await
    }
}

impl ReplayLookup {
    pub fn completed(self) -> Option<CompletedUploadResult> {
        match self {
            Self::Completed(result) => Some(result),
            Self::Missing | Self::Pending => None,
        }
    }

    pub fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }
}

/// Stable token identity. A replay returns the original snapshot, never replacing
/// captured options after a config change; fingerprint mismatch is a conflict.
pub async fn admit(
    db: &DatabaseConnection,
    request: &BatchAdmission,
) -> AppResult<zip_batch::Model> {
    supported(db)?;
    validate_admission(request)?;
    let tx = db.begin().await?;
    let found = insert_or_find(&tx, request, false).await?;
    tx.commit().await?;
    Ok(found)
}

impl BatchAdmission {
    /// Caller holds the bucket ownership lock. Never commit on an error: the
    /// v2 guard admission and both manifests share this transaction. The v2
    /// execution alone attests the signed contract and clean input SHA-256.
    /// Associated with the re-exported type so callers need no new mod.rs API.
    pub(crate) async fn admit_in_transaction(
        tx: &DatabaseTransaction,
        request: &BatchAdmission,
    ) -> AppResult<zip_batch::Model> {
        supported(tx)?;
        validate_admission(request)?;
        if request.fingerprint != "pending" || request.input_identity != "pending" {
            return Err(invalid());
        }
        insert_or_find(tx, request, true).await
    }
}

fn validate_admission(request: &BatchAdmission) -> AppResult<()> {
    if ![
        &request.id,
        &request.owner,
        &request.token,
        &request.fingerprint,
        &request.bucket,
        &request.archive_key,
        &request.input_identity,
    ]
    .into_iter()
    .all(|v| required(v))
        || request.captured_options.is_empty()
        || request.captured_options.len() > 65536
        || serde_json::from_str::<serde_json::Value>(&request.captured_options).is_err()
        || !matches!(request.source.as_str(), "direct" | "mpu" | "import")
    {
        return Err(invalid());
    }
    Ok(())
}

async fn insert_or_find(
    tx: &DatabaseTransaction,
    request: &BatchAdmission,
    exact_mirror: bool,
) -> AppResult<zip_batch::Model> {
    let now = Utc::now();
    zip_batch::Entity::insert(zip_batch::ActiveModel {
        id: Set(request.id.clone()),
        owner: Set(request.owner.clone()),
        source: Set(request.source.clone()),
        token: Set(request.token.clone()),
        fingerprint: Set(request.fingerprint.clone()),
        bucket: Set(request.bucket.clone()),
        archive_key: Set(request.archive_key.clone()),
        input_identity: Set(request.input_identity.clone()),
        captured_options: Set(request.captured_options.clone()),
        state: Set("open".into()),
        manifest_prepared: Set(false),
        source_published: Set(false),
        terminal_result: Set(None),
        root_status: Set("pending".into()),
        root_error_code: Set(None),
        root_cid: Set(None),
        root_revision: Set(0),
        root_epoch: Set(0),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::columns([
            zip_batch::Column::Owner,
            zip_batch::Column::Source,
            zip_batch::Column::Token,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(tx)
    .await?;
    let found = zip_batch::Entity::find()
        .filter(zip_batch::Column::Owner.eq(&request.owner))
        .filter(zip_batch::Column::Source.eq(&request.source))
        .filter(zip_batch::Column::Token.eq(&request.token))
        .one(tx)
        .await?
        .ok_or_else(|| AppError::Internal("ZIP batch admission lost its identity".into()))?;
    if found.fingerprint != request.fingerprint
        || found.bucket != request.bucket
        || found.archive_key != request.archive_key
        || found.input_identity != request.input_identity
    {
        return Err(AppError::InvalidZipParameter(
            "ZIP idempotency token conflict".into(),
        ));
    }
    // Unlike the legacy public replay lookup, a new v2 mirror must never
    // borrow a terminal/source-published row (nor a root claim in progress).
    if exact_mirror
        && (found.id != request.id
            || found.state != "open"
            || found.source_published
            || found.terminal_result.is_some()
            || found.root_status != "pending"
            || found.root_revision != 0
            || found.root_epoch != 0)
    {
        return Err(AppError::InvalidZipParameter(
            "ZIP idempotency token conflict".into(),
        ));
    }
    Ok(found)
}

pub async fn read(db: &DatabaseConnection, id: &str) -> AppResult<Option<zip_batch::Model>> {
    Ok(zip_batch::Entity::find_by_id(id).one(db).await?)
}

/// Read a consistent recovery/outcome snapshot; no current-key lookup is used.
pub async fn snapshot(db: &DatabaseConnection, id: &str) -> AppResult<Option<BatchSnapshot>> {
    supported(db)?;
    use sea_orm::{AccessMode, DatabaseBackend, IsolationLevel};
    let tx = if db.get_database_backend() == DatabaseBackend::Postgres {
        db.begin_with_config(
            Some(IsolationLevel::RepeatableRead),
            Some(AccessMode::ReadOnly),
        )
        .await?
    } else {
        db.begin().await?
    };
    let Some(batch) = zip_batch::Entity::find_by_id(id).one(&tx).await? else {
        tx.commit().await?;
        return Ok(None);
    };
    let entries = zip_manifest_entry::Entity::find()
        .filter(zip_manifest_entry::Column::BatchId.eq(id))
        .order_by_asc(zip_manifest_entry::Column::Path)
        .all(&tx)
        .await?;
    let builds = zip_root_build::Entity::find()
        .filter(zip_root_build::Column::BatchId.eq(id))
        .order_by_asc(zip_root_build::Column::Revision)
        .order_by_asc(zip_root_build::Column::Epoch)
        .all(&tx)
        .await?;
    let references = zip_root_reference::Entity::find()
        .filter(zip_root_reference::Column::BatchId.eq(id))
        .all(&tx)
        .await?;
    tx.commit().await?;
    Ok(Some(BatchSnapshot {
        batch,
        entries,
        builds,
        references,
    }))
}

pub(super) async fn locked_open_batch(
    tx: &sea_orm::DatabaseTransaction,
    id: &str,
) -> AppResult<zip_batch::Model> {
    let batch = lock_batch(tx, id).await?;
    if batch.state != "open" {
        return Err(super::stale());
    }
    Ok(batch)
}
