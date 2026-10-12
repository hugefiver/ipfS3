use super::{CapturedZipPublication, ZipPublicationAuthority, invalid};
use crate::{
    error::AppResult,
    import::ImportClaim,
    store::{
        database_clock::database_now,
        entities::{import_job, zip_batch},
        import::ownership,
    },
};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, QuerySelect, Statement,
    TransactionTrait,
};
use serde::{Deserialize, Serialize};

/// Server-measured complete input, not a digest derived from its CID or URL.
/// Only the claim-fenced binder writes this evidence after clean input EOF.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyZipInputProof {
    schema_version: u8,
    sha256: String,
    cid: String,
    size: i64,
}

impl LegacyZipInputProof {
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
    pub fn cid(&self) -> &str {
        &self.cid
    }
    pub fn size(&self) -> i64 {
        self.size
    }

    fn validate(&self) -> AppResult<()> {
        if self.schema_version != 1
            || !complete_input(Some(&self.sha256), Some(&self.cid), Some(self.size))
        {
            return Err(invalid());
        }
        Ok(())
    }
}

pub(super) fn complete_input(sha256: Option<&str>, cid: Option<&str>, size: Option<i64>) -> bool {
    sha256.is_some_and(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
        && cid.is_some_and(|s| !s.is_empty())
        && size.is_some_and(|s| s >= 0)
}

fn decode(json: &str) -> AppResult<LegacyZipInputProof> {
    let proof: LegacyZipInputProof = serde_json::from_str(json).map_err(|_| invalid())?;
    proof.validate()?;
    Ok(proof)
}

/// Workers check this BEFORE fetching the original URL on every retry/restart.
/// NULL means no complete bound evidence, not permission to infer SHA from CID.
/// The job/path/token join prevents borrowing another import's proof.
pub async fn read_legacy_import_input<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
) -> AppResult<Option<LegacyZipInputProof>> {
    let placeholder = if db.get_database_backend() == DatabaseBackend::Postgres {
        "$1"
    } else {
        "?"
    };
    let row = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
        format!("SELECT b.legacy_input_proof_json,b.source,b.token,b.bucket,b.archive_key,j.bucket AS job_bucket,j.key AS job_key,j.decompress_prefix FROM zip_batches b LEFT JOIN import_jobs j ON j.id=b.id WHERE b.id={placeholder}"),
        [job_id.into()])).await?;
    let Some(row) = row else { return Ok(None) };
    if row.try_get::<String>("", "source")? != "import"
        || row.try_get::<String>("", "token")? != job_id
        || Some(row.try_get::<String>("", "bucket")?)
            != row.try_get::<Option<String>>("", "job_bucket")?
        || Some(row.try_get::<String>("", "archive_key")?)
            != row.try_get::<Option<String>>("", "job_key")?
        || row
            .try_get::<Option<String>>("", "decompress_prefix")?
            .is_none()
    {
        return Err(invalid());
    }
    row.try_get::<Option<String>>("", "legacy_input_proof_json")?
        .map(|json| decode(&json))
        .transpose()
}

/// Call only after server hashing/counting the entire source through clean EOF,
/// and obtaining the corresponding complete Kubo artifact. No client digest is
/// needed. On commit uncertainty, read the bound proof before any URL retry.
pub async fn bind_legacy_import_input(
    db: &DatabaseConnection,
    claim: &ImportClaim,
    sha256: &str,
    cid: &str,
    size: i64,
) -> AppResult<LegacyZipInputProof> {
    let proof = LegacyZipInputProof {
        schema_version: 1,
        sha256: sha256.to_owned(),
        cid: cid.to_owned(),
        size,
    };
    proof.validate()?;
    let job = import_job::Entity::find_by_id(&claim.job_id)
        .one(db)
        .await?
        .ok_or_else(invalid)?;
    if job.decompress_prefix.is_none() {
        return Err(invalid());
    }
    let tx = db.begin().await?;
    ownership::lock_bucket_for_ownership(&tx, &job.bucket).await?;
    ownership::verify_active_claim(&tx, claim, &job.bucket).await?;
    let query = zip_batch::Entity::find_by_id(&claim.job_id);
    let batch = if tx.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(&tx).await?
    } else {
        query.one(&tx).await?
    }
    .ok_or_else(invalid)?;
    if batch.source != "import"
        || batch.token != job.id
        || batch.bucket != job.bucket
        || batch.archive_key != job.key
        || batch.state != "open"
        || batch.source_published
    {
        return Err(invalid());
    }
    if let Some(old) = read_legacy_import_input(&tx, &claim.job_id).await? {
        if old != proof {
            return Err(invalid());
        }
    } else {
        let json = serde_json::to_string(&proof).map_err(|_| invalid())?;
        let sql = if tx.get_database_backend() == DatabaseBackend::Postgres {
            "UPDATE zip_batches SET legacy_input_proof_json=$1,updated_at=$2 WHERE id=$3 AND legacy_input_proof_json IS NULL"
        } else {
            "UPDATE zip_batches SET legacy_input_proof_json=?,updated_at=? WHERE id=? AND legacy_input_proof_json IS NULL"
        };
        let changed = tx
            .execute(Statement::from_sql_and_values(
                tx.get_database_backend(),
                sql,
                [
                    json.into(),
                    database_now(&tx).await?.into(),
                    claim.job_id.clone().into(),
                ],
            ))
            .await?;
        if changed.rows_affected() != 1 {
            return Err(invalid());
        }
    }
    tx.commit().await?;
    Ok(proof)
}

pub(super) async fn verify_publication<C: ConnectionTrait>(
    db: &C,
    request: &CapturedZipPublication,
) -> AppResult<()> {
    if let ZipPublicationAuthority::Legacy(authority) = &request.authority
        && let Some(guard) = &authority.import_guard
    {
        if request.batch_id != guard.job_id {
            return Err(invalid());
        }
        let proof = read_legacy_import_input(db, &guard.job_id)
            .await?
            .ok_or_else(invalid)?;
        let source = request.source.as_ref().ok_or_else(invalid)?;
        if proof.cid != source.cid || proof.size != source.logical_size {
            return Err(invalid());
        }
    }
    Ok(())
}
