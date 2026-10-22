//! Immutable byte archives. Header and body are not, by themselves, authority.
use super::*;
use native::meter::{Budget, CELL_SLICE, PAGE_BYTES};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
pub(crate) mod progress;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveHeader {
    pub database_scope: String,
    pub database_identity: String,
    pub principal: String,
    pub catalog: Vec<String>,
    pub request: String,
    pub configuration: String,
    pub acknowledgements: Vec<String>,
    /// Original SQL text, not a reissued timestamp.
    pub created_at: String,
    pub expires_at: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRef {
    pub archive_id: String,
    pub digest: String,
    pub bytes: i64,
    pub chunks: i64,
}

/// Staging and sealing occur in the caller transaction. A failed write must be
/// rolled back, not committed as an applied prefix. Chunks are always <=64 KiB.
pub async fn write_archive<R: Read>(
    tx: &DatabaseTransaction,
    id: &str,
    header: &ArchiveHeader,
    input: &mut R,
    budget: &mut Budget,
) -> Result<ArchiveRef> {
    let raw = serde_json::to_string(header).map_err(|_| ManagementError::InvalidEvidence)?;
    if raw.len() > PAGE_BYTES || id.is_empty() || id.len() > 1024 {
        return Err(ManagementError::Budget);
    }
    let _resident = budget.reserve(raw.len() + CELL_SLICE * 2)?;
    budget.charge(1, 1, 1, raw.len() as u64)?;
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"INSERT INTO pin_management_archive(archive_id,format,database_scope,native_identity,header,state) VALUES ($1,'S75A1',$2,$3,$4,'staging')",[id.into(),header.database_scope.clone().into(),header.database_identity.clone().into(),raw.clone().into()])).await.map_err(|_|ManagementError::Database)?;
    let mut digest = Sha256::new();
    digest.update(b"S75A1\0");
    digest.update((raw.len() as u64).to_be_bytes());
    digest.update(raw.as_bytes());
    let mut buffer = vec![0; CELL_SLICE];
    let mut chunks = 0i64;
    let mut bytes = 0i64;
    loop {
        budget.check()?;
        let n = input
            .read(&mut buffer)
            .map_err(|_| ManagementError::InvalidEvidence)?;
        if n == 0 {
            break;
        }
        budget.charge(1, 1, 1, n as u64)?;
        digest.update((chunks as u64).to_be_bytes());
        digest.update((n as u64).to_be_bytes());
        digest.update(&buffer[..n]);
        tx.execute(Statement::from_sql_and_values(
            tx.get_database_backend(),
            "INSERT INTO pin_management_archive_chunks VALUES ($1,$2,$3)",
            [id.into(), chunks.into(), buffer[..n].to_vec().into()],
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
        chunks = chunks.checked_add(1).ok_or(ManagementError::Budget)?;
        bytes = bytes.checked_add(n as i64).ok_or(ManagementError::Budget)?;
    }
    let digest = hex::encode(digest.finalize());
    budget.charge(1, 0, 0, 0)?;
    let changed = tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_management_archive SET state='sealed',chunks=$1,bytes=$2,digest=$3 WHERE archive_id=$4 AND state='staging'",[chunks.into(),bytes.into(),digest.clone().into(),id.into()])).await.map_err(|_|ManagementError::Database)?;
    if changed.rows_affected() != 1 {
        return Err(ManagementError::Changed);
    }
    Ok(ArchiveRef {
        archive_id: id.into(),
        digest,
        bytes,
        chunks,
    })
}

/// The sink must remain unpublished until this function verifies the trailer.
pub async fn read_archive<C: ConnectionTrait, W: Write>(
    db: &C,
    reference: &ArchiveRef,
    output: &mut W,
    budget: &mut Budget,
) -> Result<ArchiveHeader> {
    let raw = read_archive_raw(db, reference, output, budget).await?;
    serde_json::from_str(&raw).map_err(|_| ManagementError::InvalidEvidence)
}

/// Verify transport before the caller decodes the bounded header in its own
/// operation-wide read session.
pub(crate) async fn read_archive_raw<C: ConnectionTrait, W: Write>(
    db: &C,
    reference: &ArchiveRef,
    output: &mut W,
    budget: &mut Budget,
) -> Result<String> {
    budget.charge(1, 1, 0, 0)?;
    let length = if db.get_database_backend() == sea_orm::DatabaseBackend::Sqlite {
        "length(CAST(header AS BLOB))"
    } else {
        "CAST(OCTET_LENGTH(header) AS BIGINT)"
    };
    let row = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),format!("SELECT {length} AS header_bytes,format,state,chunks,bytes,digest FROM pin_management_archive WHERE archive_id=$1"),[reference.archive_id.clone().into()])).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
    let header_bytes = usize::try_from(
        row.try_get::<i64>("", "header_bytes")
            .map_err(|_| ManagementError::InvalidEvidence)?,
    )
    .map_err(|_| ManagementError::InvalidEvidence)?;
    if header_bytes > native::meter::PAGE_BYTES {
        return Err(ManagementError::Budget);
    }
    budget.charge(1, 1, 1, header_bytes as u64)?;
    let _resident = budget.reserve(header_bytes * 2 + CELL_SLICE * 2)?;
    let header=db.query_one(Statement::from_sql_and_values(db.get_database_backend(),format!("SELECT CASE WHEN {length}=$2 THEN header END AS header FROM pin_management_archive WHERE archive_id=$1"),[reference.archive_id.clone().into(),(header_bytes as i64).into()])).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
    let raw: String = header
        .try_get("", "header")
        .map_err(|_| ManagementError::Changed)?;
    if row
        .try_get::<String>("", "format")
        .map_err(|_| ManagementError::InvalidEvidence)?
        != "S75A1"
        || row
            .try_get::<String>("", "state")
            .map_err(|_| ManagementError::InvalidEvidence)?
            != "sealed"
        || row
            .try_get::<i64>("", "chunks")
            .map_err(|_| ManagementError::InvalidEvidence)?
            != reference.chunks
        || row
            .try_get::<i64>("", "bytes")
            .map_err(|_| ManagementError::InvalidEvidence)?
            != reference.bytes
        || row
            .try_get::<String>("", "digest")
            .map_err(|_| ManagementError::InvalidEvidence)?
            != reference.digest
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let mut digest = Sha256::new();
    digest.update(b"S75A1\0");
    digest.update((raw.len() as u64).to_be_bytes());
    digest.update(raw.as_bytes());
    let mut bytes = 0i64;
    budget.charge(
        0,
        0,
        0,
        u64::try_from(reference.bytes).map_err(|_| ManagementError::InvalidEvidence)?,
    )?;
    budget.work.output_bytes = budget
        .work
        .output_bytes
        .checked_add(reference.bytes as u64)
        .ok_or(ManagementError::Budget)?;
    budget.check()?;
    for ordinal in 0..reference.chunks {
        budget.charge(1, 1, 1, 0)?;
        let row = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),"SELECT CASE WHEN length(payload)<=65536 AND length(payload)<=$3 THEN payload END AS payload FROM pin_management_archive_chunks WHERE archive_id=$1 AND ordinal=$2",[reference.archive_id.clone().into(),ordinal.into(),(reference.bytes-bytes).into()])).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
        let chunk: Vec<u8> = row
            .try_get("", "payload")
            .map_err(|_| ManagementError::InvalidEvidence)?;
        if chunk.is_empty() || chunk.len() > CELL_SLICE {
            return Err(ManagementError::InvalidEvidence);
        }
        if bytes
            .checked_add(chunk.len() as i64)
            .is_none_or(|used| used > reference.bytes)
        {
            return Err(ManagementError::InvalidEvidence);
        }
        digest.update((ordinal as u64).to_be_bytes());
        digest.update((chunk.len() as u64).to_be_bytes());
        digest.update(&chunk);
        output
            .write_all(&chunk)
            .map_err(|_| ManagementError::Database)?;
        bytes = bytes
            .checked_add(chunk.len() as i64)
            .ok_or(ManagementError::Budget)?;
    }
    budget.charge(1, 0, 0, 0)?;
    if db.query_one(Statement::from_sql_and_values(db.get_database_backend(),"SELECT 1 FROM pin_management_archive_chunks WHERE archive_id=$1 AND ordinal>=$2 LIMIT 1",[reference.archive_id.clone().into(),reference.chunks.into()])).await.map_err(|_|ManagementError::Database)?.is_some() || bytes != reference.bytes || hex::encode(digest.finalize()) != reference.digest { return Err(ManagementError::InvalidEvidence); }
    Ok(raw)
}
