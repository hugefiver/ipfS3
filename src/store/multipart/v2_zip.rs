use std::collections::BTreeMap;

use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction,
    EntityTrait, QueryFilter, QuerySelect, Statement, TransactionTrait, Value,
};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};
use crate::pinning::tags::ObjectTag;
use crate::zip::options::ZipV2Options;

use super::create_upload_with_decision;

#[derive(Clone, Debug)]
pub struct Intake {
    pub upload_id: String,
    pub object_id: String,
    pub owner: String,
    pub token: String,
    pub bucket: String,
    pub key: String,
    pub target_prefix: String,
    pub request_contract: String,
    pub options: ZipV2Options,
    pub captured_config: String,
    pub rule_revision: String,
    pub content_type: Option<String>,
    pub metadata: Option<JsonValue>,
    pub tags: Vec<ObjectTag>,
}

#[derive(Clone, Debug)]
pub struct Record {
    pub original_upload_id: String,
    pub active_upload_id: Option<String>,
    pub execution_id: String,
    pub owner: String,
    pub token: String,
    pub bucket: String,
    pub archive_key: String,
    pub target_prefix: String,
    pub request_fingerprint: String,
    pub captured_options: String,
    pub captured_config: String,
    pub rule_revision: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteReceipt {
    pub xml: String,
    pub headers: BTreeMap<String, String>,
}

pub enum CompleteState {
    Pending,
    Completed(CompleteReceipt),
}

pub fn complete_request_contract(parts: &[s3s::dto::CompletedPart]) -> AppResult<String> {
    if parts.is_empty() || parts.len() > 10_000 {
        return Err(conflict());
    }
    let mut previous = 0;
    let mut contract = Vec::with_capacity(parts.len());
    for part in parts {
        let number = part.part_number.ok_or_else(conflict)?;
        if number <= previous || number > 10_000 {
            return Err(conflict());
        }
        previous = number;
        let etag = match &part.e_tag {
            Some(s3s::dto::ETag::Strong(value)) => serde_json::json!(["strong", value]),
            Some(s3s::dto::ETag::Weak(value)) => serde_json::json!(["weak", value]),
            None => JsonValue::Null,
        };
        contract.push(serde_json::json!({
            "part_number": number, "etag": etag,
            "crc32": part.checksum_crc32, "crc32c": part.checksum_crc32c,
            "crc64nvme": part.checksum_crc64nvme, "sha1": part.checksum_sha1,
            "sha256": part.checksum_sha256,
        }));
    }
    serde_json::to_string(&contract).map_err(|_| conflict())
}

/// Freeze the parsed, ordered Complete body before any CAT/add. The legacy
/// canonicalizer includes ETag absence/strength and every checksum field.
/// The intake's original owner, token, and active upload must all still match.
pub async fn capture_complete(
    db: &DatabaseConnection,
    record: &Record,
    owner: &str,
    bucket: &str,
    key: &str,
    parts: &[s3s::dto::CompletedPart],
) -> AppResult<CompleteState> {
    let contract = complete_request_contract(parts)?;
    let tx = db.begin().await?;
    crate::store::import::ownership::lock_bucket_for_ownership(&tx, bucket).await?;
    let current = read_by_upload(&tx, &record.original_upload_id)
        .await?
        .ok_or_else(conflict)?;
    if current.owner != owner
        || current.bucket != bucket
        || current.archive_key != key
        || current.token != record.token
        || current.execution_id != record.execution_id
        || current.request_fingerprint != record.request_fingerprint
        || current.captured_options != record.captured_options
        || current.captured_config != record.captured_config
        || current.rule_revision != record.rule_revision
        || current.target_prefix != record.target_prefix
    {
        return Err(conflict());
    }
    let execution = crate::store::zip::execution::read(&tx, &record.execution_id)
        .await?
        .ok_or_else(conflict)?;
    if execution.source != "mpu"
        || execution.owner != owner
        || execution.token != record.token
        || execution.bucket != bucket
        || execution.source_key != key
        || execution.captured_options != record.captured_options
        || execution.request_fingerprint != record.request_fingerprint
    {
        return Err(conflict());
    }
    if current.active_upload_id.as_deref() == Some(record.original_upload_id.as_str())
        && execution.state == "pending"
    {
        super::get_upload(&tx, &record.original_upload_id)
            .await
            .map_err(|_| conflict())?;
        tx.execute(query(&tx,
            "INSERT INTO zip_v2_mpu_completions (upload_id,owner,token,execution_id,bucket,archive_key,part_contract) VALUES (?,?,?,?,?,?,?) ON CONFLICT(upload_id) DO NOTHING",
            "INSERT INTO zip_v2_mpu_completions (upload_id,owner,token,execution_id,bucket,archive_key,part_contract) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(upload_id) DO NOTHING",
            vec![record.original_upload_id.clone().into(),owner.into(),record.token.clone().into(),
                record.execution_id.clone().into(),bucket.into(),key.into(),contract.clone().into()]
        )).await?;
    }
    let row = tx
        .query_one(query(
            &tx,
            "SELECT * FROM zip_v2_mpu_completions WHERE upload_id=?",
            "SELECT * FROM zip_v2_mpu_completions WHERE upload_id=$1",
            vec![record.original_upload_id.clone().into()],
        ))
        .await?
        .ok_or_else(conflict)?;
    if row.try_get::<String>("", "owner")? != owner
        || row.try_get::<String>("", "token")? != record.token
        || row.try_get::<String>("", "execution_id")? != record.execution_id
        || row.try_get::<String>("", "bucket")? != bucket
        || row.try_get::<String>("", "archive_key")? != key
        || row.try_get::<String>("", "part_contract")? != contract
    {
        return Err(conflict());
    }
    let xml: Option<String> = row.try_get("", "response_xml")?;
    let headers: Option<String> = row.try_get("", "response_headers")?;
    let state = match (xml, headers) {
        (Some(xml), Some(headers))
            if execution.state == "completed" && current.active_upload_id.is_none() =>
        {
            let mut headers: BTreeMap<String, String> =
                serde_json::from_str(&headers).map_err(|_| conflict())?;
            if matches!(
                headers
                    .get("x-ipfs-s3-zip-batch-status")
                    .map(String::as_str),
                Some("failed" | "empty")
            ) && !headers.contains_key("date")
            {
                // Pre-upgrade completed receipts have no HTTP Date. Derive it
                // from the immutable published root, not the time of replay.
                let root =
                    crate::store::entities::zip_batch::Entity::find_by_id(&record.execution_id)
                        .one(&tx)
                        .await?
                        .ok_or_else(conflict)?;
                if root.state != "published" || root.source_published {
                    return Err(conflict());
                }
                headers.insert(
                    "date".into(),
                    root.updated_at
                        .format("%a, %d %b %Y %H:%M:%S GMT")
                        .to_string(),
                );
            }
            CompleteState::Completed(CompleteReceipt { xml, headers })
        }
        (None, None)
            if current.active_upload_id.as_deref() == Some(record.original_upload_id.as_str())
                && matches!(execution.state.as_str(), "pending" | "admitted") =>
        {
            CompleteState::Pending
        }
        _ => return Err(conflict()),
    };
    tx.commit().await?;
    Ok(state)
}

/// Only invoked inside the v2 publication transaction, after versions, root
/// adoption and execution completion. Failure rolls them all back together.
/// Verify the exact newly published source, including nullable public VersionId.
/// Never resolve it through the mutable current-key projection or a marker CID.
pub(crate) async fn verify_published_source(
    tx: &DatabaseTransaction,
    execution: &crate::store::zip::execution::Snapshot,
    terminal: &JsonValue,
) -> AppResult<String> {
    use crate::store::entities::{object, object_version};
    let row_id = terminal["source_version_row_id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or_else(conflict)?;
    let query = object_version::Entity::find_by_id(row_id);
    let version = if tx.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_shared().one(tx).await?
    } else {
        query.one(tx).await?
    }
    .ok_or_else(conflict)?;
    let object_id = version.object_id.as_deref().ok_or_else(conflict)?;
    let query = object::Entity::find_by_id(object_id);
    let source = if tx.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_shared().one(tx).await?
    } else {
        query.one(tx).await?
    }
    .ok_or_else(conflict)?;
    let state = crate::store::bucket::get_versioning_state(tx, &execution.bucket).await?;
    let public_id = crate::store::object_version::public_version_id(&version)?;
    let expected = if state == crate::store::object_version::BucketVersioningState::Unversioned {
        JsonValue::Null
    } else {
        JsonValue::String(public_id.as_s3_str().to_owned())
    };
    if version.bucket != execution.bucket
        || version.key != execution.source_key
        || version.kind != "object"
        || (state == crate::store::object_version::BucketVersioningState::Unversioned
            && version.version_id.is_some())
        || !version.is_latest
        || source.bucket != execution.bucket
        || source.key != execution.source_key
        || !source.is_latest
        || execution.input_art_cid.as_deref() != Some(source.cid.as_str())
        || execution.input_art_size != Some(source.size)
        || source.etag != source.cid
        || source.encrypted
        || source.key_wrap.is_some()
        || source.sse_c_key_fingerprint.is_some()
        || !source.multipart
        || terminal.get("source_version_id") != Some(&expected)
    {
        return Err(conflict());
    }
    Ok(source.id)
}

pub async fn finalize_complete_in_transaction(
    tx: &DatabaseTransaction,
    claim: &crate::store::zip::execution::Claim,
    contract: &str,
    source_result: Option<(&str, &crate::store::object_version::PublicationResult)>,
) -> AppResult<()> {
    let record = read_by_upload(tx, &claim.batch_id)
        .await?
        .ok_or_else(conflict)?;
    if record.original_upload_id != record.execution_id
        || record.active_upload_id.as_deref() != Some(claim.batch_id.as_str())
    {
        return Err(conflict());
    }
    let row = tx
        .query_one(query(
            tx,
            "SELECT * FROM zip_v2_mpu_completions WHERE upload_id=?",
            "SELECT * FROM zip_v2_mpu_completions WHERE upload_id=$1",
            vec![claim.batch_id.clone().into()],
        ))
        .await?
        .ok_or_else(conflict)?;
    if row.try_get::<String>("", "owner")? != record.owner
        || row.try_get::<String>("", "token")? != record.token
        || row.try_get::<String>("", "execution_id")? != record.execution_id
        || row.try_get::<String>("", "bucket")? != record.bucket
        || row.try_get::<String>("", "archive_key")? != record.archive_key
        || row.try_get::<String>("", "part_contract")? != contract
        || row.try_get::<Option<String>>("", "response_xml")?.is_some()
    {
        return Err(conflict());
    }
    let execution = crate::store::zip::execution::read(tx, &claim.batch_id)
        .await?
        .ok_or_else(conflict)?;
    use crate::store::entities::{
        zip_batch, zip_manifest_entry, zip_root_build, zip_root_reference,
    };
    let root = zip_batch::Entity::find_by_id(&claim.batch_id)
        .one(tx)
        .await?
        .ok_or_else(conflict)?;
    let options: ZipV2Options =
        serde_json::from_str(&record.captured_options).map_err(|_| conflict())?;
    let entries = zip_manifest_entry::Entity::find()
        .filter(zip_manifest_entry::Column::BatchId.eq(&claim.batch_id))
        .all(tx)
        .await?;
    let references = zip_root_reference::Entity::find()
        .filter(zip_root_reference::Column::BatchId.eq(&claim.batch_id))
        .all(tx)
        .await?;
    let builds = zip_root_build::Entity::find()
        .filter(zip_root_build::Column::BatchId.eq(&claim.batch_id))
        .all(tx)
        .await?;
    if execution.state != "completed"
        || execution.owner != record.owner
        || execution.token != record.token
        || execution.source != "mpu"
        || execution.input_sha256.is_none()
        || root.state != "published"
        || root.source_published != options.publish_source
        || options.publish_source != source_result.is_some()
        || root.owner != record.owner
        || root.bucket != record.bucket
        || root.archive_key != record.archive_key
    {
        return Err(conflict());
    }
    let published = entries
        .iter()
        .filter(|e| e.cid.is_some() && e.version_row_id.is_some())
        .count();
    let failed = entries.iter().filter(|e| e.error_code.is_some()).count();
    let terminal: JsonValue =
        serde_json::from_str(execution.terminal_result.as_deref().ok_or_else(conflict)?)
            .map_err(|_| conflict())?;
    let sha = execution.input_sha256.as_deref().ok_or_else(conflict)?;
    if let Some((cid, result)) = source_result {
        if execution.input_art_cid.as_deref() != Some(cid)
            || verify_published_source(tx, &execution, &terminal).await? != result.object_id
            || root
                .input_identity
                .strip_prefix("zip-v2-source-gen:")
                .is_none()
            || terminal["source_cid"] != cid
            || terminal["source_size"] != execution.input_art_size.unwrap_or(-1)
            || terminal["source_version_id"] != serde_json::json!(result.version_id)
        {
            return Err(conflict());
        }
    } else if root.input_identity != "pending"
        || [
            "source_version_id",
            "source_version_row_id",
            "source_cid",
            "source_size",
        ]
        .iter()
        .any(|field| terminal.get(field).is_some())
    {
        return Err(conflict());
    }
    if terminal["input_sha256"] != sha
        || terminal["published_count"] != published
        || terminal["failed_count"] != failed
        || terminal["status"]
            != if published == 0 && !options.publish_source {
                if failed == 0 { "empty" } else { "failed" }
            } else {
                "completed"
            }
    {
        return Err(conflict());
    }
    let root_status = root.root_status.as_str();
    let mut headers = BTreeMap::from([
        ("content-type".to_owned(), "application/xml".to_owned()),
        ("x-ipfs-s3-zip-batch-id".to_owned(), claim.batch_id.clone()),
        (
            "x-ipfs-s3-zip-root-status".to_owned(),
            root_status.to_owned(),
        ),
        (
            "x-ipfs-s3-zip-batch-status".to_owned(),
            terminal["status"].as_str().ok_or_else(conflict)?.to_owned(),
        ),
    ]);
    if published == 0 && !options.publish_source {
        headers.insert(
            "date".into(),
            root.updated_at
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string(),
        );
    }
    if let Some(cid) = root
        .root_cid
        .as_ref()
        .filter(|_| matches!(root_status, "complete" | "partial"))
    {
        if !references.iter().any(|reference| {
            reference.state == "adopted"
                && reference.verification_receipt.is_some()
                && reference.cid == *cid
                && reference.revision == root.root_revision
                && reference.epoch == root.root_epoch
                && builds.iter().any(|build| {
                    build.revision == reference.revision
                        && build.epoch == reference.epoch
                        && build.status == "verified"
                })
        }) {
            return Err(conflict());
        }
        headers.insert("x-ipfs-s3-zip-root-cid".into(), cid.clone());
    }
    let source_xml = if let Some((cid, result)) = source_result {
        headers.insert("etag".into(), format!("\"{cid}\""));
        let version_xml = if let Some(version) = &result.version_id {
            headers.insert("x-amz-version-id".into(), version.clone());
            format!(
                "<VersionId>{}</VersionId>",
                quick_xml::escape::escape(version)
            )
        } else {
            String::new()
        };
        format!(
            "<ETag>\"{}\"</ETag>{version_xml}",
            quick_xml::escape::escape(cid)
        )
    } else {
        String::new()
    };
    let xml = format!(
        "<ZipBatchResult><BatchId>{}</BatchId><SourcePublished>{}</SourcePublished>{source_xml}<InputSHA256>{sha}</InputSHA256><BatchStatus>{}</BatchStatus><PublishedCount>{published}</PublishedCount><FailedCount>{failed}</FailedCount><RootStatus>{root_status}</RootStatus></ZipBatchResult>",
        claim.batch_id,
        root.source_published,
        terminal["status"].as_str().ok_or_else(conflict)?,
    );
    let changed = tx.execute(query(tx,
        "UPDATE zip_v2_mpu_completions SET response_xml=?,response_headers=? WHERE upload_id=? AND response_xml IS NULL AND part_contract=?",
        "UPDATE zip_v2_mpu_completions SET response_xml=$1,response_headers=$2 WHERE upload_id=$3 AND response_xml IS NULL AND part_contract=$4",
        vec![xml.into(),serde_json::to_string(&headers).map_err(|_| conflict())?.into(),claim.batch_id.clone().into(),contract.into()],
    )).await?;
    if changed.rows_affected() != 1 {
        return Err(conflict());
    }
    super::delete_upload(tx, &claim.batch_id).await?;
    Ok(())
}

fn query(db: &impl ConnectionTrait, sqlite: &str, postgres: &str, values: Vec<Value>) -> Statement {
    Statement::from_sql_and_values(
        db.get_database_backend(),
        if db.get_database_backend() == DatabaseBackend::Postgres {
            postgres
        } else {
            sqlite
        },
        values,
    )
}

fn conflict() -> AppError {
    AppError::ZipIdempotencyConflict
}

pub async fn read_by_upload<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
) -> AppResult<Option<Record>> {
    let row = db
        .query_one(query(
            db,
            "SELECT * FROM zip_v2_mpu_intakes WHERE original_upload_id=?",
            "SELECT * FROM zip_v2_mpu_intakes WHERE original_upload_id=$1",
            vec![upload_id.into()],
        ))
        .await?;
    row.map(|row| {
        Ok(Record {
            original_upload_id: row.try_get("", "original_upload_id")?,
            active_upload_id: row.try_get("", "active_upload_id")?,
            execution_id: row.try_get("", "execution_id")?,
            owner: row.try_get("", "owner")?,
            token: row.try_get("", "token")?,
            bucket: row.try_get("", "bucket")?,
            archive_key: row.try_get("", "archive_key")?,
            target_prefix: row.try_get("", "target_prefix")?,
            request_fingerprint: row.try_get("", "request_fingerprint")?,
            captured_options: row.try_get("", "captured_options")?,
            captured_config: row.try_get("", "captured_config")?,
            rule_revision: row.try_get("", "rule_revision")?,
        })
    })
    .transpose()
}

async fn replay(
    tx: &DatabaseTransaction,
    existing_id: &str,
    request: &Intake,
) -> AppResult<String> {
    let record = read_by_upload(tx, existing_id)
        .await?
        .ok_or_else(conflict)?;
    if record.owner != request.owner
        || record.bucket != request.bucket
        || record.archive_key != request.key
        || record.target_prefix != request.target_prefix
        || record.token != request.token
        || record.active_upload_id.as_deref() != Some(existing_id)
    {
        return Err(conflict());
    }
    super::get_upload(tx, existing_id)
        .await
        .map_err(|_| conflict())?;
    Ok(existing_id.to_owned())
}

pub async fn create_or_replay(db: &DatabaseConnection, request: &Intake) -> AppResult<String> {
    if (!request.options.publish_extracted && !request.options.publish_source)
        || request.options.token != request.token
        || request.rule_revision.is_empty()
    {
        return Err(AppError::InvalidZipParameter(
            "invalid ZIP v2 upload intake".into(),
        ));
    }
    let captured_options = serde_json::to_string(&request.options)
        .map_err(|_| AppError::Internal("invalid ZIP v2 options".into()))?;
    let request_fingerprint = hex::encode(Sha256::digest(request.request_contract.as_bytes()));
    let tx = db.begin().await?;
    crate::store::import::ownership::lock_bucket_for_ownership(&tx, &request.bucket).await?;
    let execution = crate::store::zip::execution::admit_in_transaction(
        &tx,
        &crate::store::zip::execution::Admission {
            id: request.upload_id.clone(),
            owner: request.owner.clone(),
            source: "mpu".into(),
            token: request.token.clone(),
            request_fingerprint: request_fingerprint.clone(),
            request_contract: request.request_contract.clone(),
            bucket: request.bucket.clone(),
            source_key: request.key.clone(),
            captured_options: captured_options.clone(),
        },
    )
    .await
    .map_err(|error| match error {
        AppError::InvalidZipParameter(_) => conflict(),
        other => other,
    })?;
    if execution.id != request.upload_id {
        let old = replay(&tx, &execution.id, request).await?;
        tx.commit().await?;
        return Ok(old);
    }
    create_upload_with_decision(
        &tx,
        &request.upload_id,
        &request.object_id,
        &request.bucket,
        &request.key,
        "none",
        None,
        None,
        request.content_type.as_deref(),
        request.metadata.clone(),
        &request.tags,
        Some(&request.target_prefix),
        true,
        None,
    )
    .await?;
    let now = crate::store::database_clock::database_now(&tx).await?;
    tx.execute(query(&tx,
        "INSERT INTO zip_v2_mpu_intakes (original_upload_id,active_upload_id,execution_id,owner,token,bucket,archive_key,target_prefix,request_fingerprint,captured_options,captured_config,rule_revision,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        "INSERT INTO zip_v2_mpu_intakes (original_upload_id,active_upload_id,execution_id,owner,token,bucket,archive_key,target_prefix,request_fingerprint,captured_options,captured_config,rule_revision,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
        vec![request.upload_id.clone().into(),request.upload_id.clone().into(),execution.id.into(),
             request.owner.clone().into(),request.token.clone().into(),request.bucket.clone().into(),
             request.key.clone().into(),request.target_prefix.clone().into(),request_fingerprint.into(),
             captured_options.into(),request.captured_config.clone().into(),request.rule_revision.clone().into(),now.into()]
    )).await?;
    tx.commit().await?;
    Ok(request.upload_id.clone())
}
