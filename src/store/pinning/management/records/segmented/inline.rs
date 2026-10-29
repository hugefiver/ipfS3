//! Small immutable documents have one root read and no chunk lookup.
use super::*;

pub(crate) fn digest(header: &str, payload: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"S75I2\0inline\0");
    digest.update((header.len() as u64).to_be_bytes());
    digest.update(header.as_bytes());
    digest.update((payload.len() as u64).to_be_bytes());
    digest.update(payload);
    hex::encode(digest.finalize())
}

pub(super) async fn write(
    tx: &impl TransactionConnection,
    id: &str,
    header: &ArchiveHeader,
    raw: String,
    payload: Vec<u8>,
    budget: &mut Budget,
) -> Result<ArchiveRef> {
    if id.is_empty() || id.len() > 1024 {
        return Err(ManagementError::Budget);
    }
    let _resident = budget.reserve(raw.len())?;
    let bytes = payload.len() as i64;
    let digest = digest(&raw, &payload);
    budget.charge(1, 1, 1, (raw.len() + payload.len()) as u64)?;
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
        "INSERT INTO pin_management_inline_archive(archive_id,format,database_scope,native_identity,header,bytes,digest,payload) VALUES($1,'S75I2',$2,$3,$4,$5,$6,$7)",
        [id.into(), header.database_scope.clone().into(), header.database_identity.clone().into(), raw.into(), bytes.into(), digest.clone().into(), payload.into()]))
        .await.map_err(|_| ManagementError::Database)?;
    Ok(ArchiveRef {
        archive_id: id.into(),
        digest,
        bytes,
        chunks: 0,
    })
}

pub(crate) async fn reference<C: ConnectionTrait>(db: &C, id: &str) -> Result<Option<ArchiveRef>> {
    let rows = db.query_all(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT CASE WHEN length(digest)=64 THEN digest END AS digest,bytes,chunks FROM pin_management_archive WHERE archive_id=$1 AND state='sealed' UNION ALL SELECT CASE WHEN length(digest)=64 THEN digest END AS digest,bytes,CAST(0 AS BIGINT) AS chunks FROM pin_management_inline_archive WHERE archive_id=$1",
        [id.into()])).await.map_err(|_| ManagementError::Database)?;
    if rows.len() > 1 {
        return Err(ManagementError::InvalidEvidence);
    }
    rows.first()
        .map(|row| {
            Ok(ArchiveRef {
                archive_id: id.into(),
                digest: row
                    .try_get("", "digest")
                    .map_err(|_| ManagementError::InvalidEvidence)?,
                bytes: row
                    .try_get("", "bytes")
                    .map_err(|_| ManagementError::InvalidEvidence)?,
                chunks: row
                    .try_get("", "chunks")
                    .map_err(|_| ManagementError::InvalidEvidence)?,
            })
        })
        .transpose()
}

pub(super) async fn read<C: ConnectionTrait, W: Write>(
    db: &C,
    reference: &ArchiveRef,
    output: &mut W,
    budget: &mut Budget,
) -> Result<Option<String>> {
    // A segmented reference cannot name an inline document.
    if reference.chunks != 0 {
        return Ok(None);
    }
    budget.charge(1, 1, 0, 0)?;
    let length = if db.get_database_backend() == sea_orm::DatabaseBackend::Sqlite {
        "length(CAST(header AS BLOB))"
    } else {
        "octet_length(header)"
    };
    let _resident = budget.reserve(PAGE_BYTES + CELL_SLICE)?;
    let row = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
        format!("SELECT format,bytes,CASE WHEN length(digest)=64 THEN digest END AS digest,CASE WHEN {length}+length(payload)<=1048576 THEN header END AS header,CASE WHEN length(payload)<=65536 AND {length}+length(payload)<=1048576 THEN payload END AS payload FROM pin_management_inline_archive WHERE archive_id=$1"),
        [reference.archive_id.clone().into()])).await.map_err(|_| ManagementError::Database)?;
    let Some(row) = row else { return Ok(None) };
    let raw: String = row
        .try_get("", "header")
        .map_err(|_| ManagementError::Budget)?;
    let payload: Vec<u8> = row
        .try_get("", "payload")
        .map_err(|_| ManagementError::Budget)?;
    if row
        .try_get::<String>("", "format")
        .map_err(|_| ManagementError::InvalidEvidence)?
        != "S75I2"
        || row
            .try_get::<i64>("", "bytes")
            .map_err(|_| ManagementError::InvalidEvidence)?
            != reference.bytes
        || row
            .try_get::<String>("", "digest")
            .map_err(|_| ManagementError::InvalidEvidence)?
            != reference.digest
        || payload.len() as i64 != reference.bytes
        || digest(&raw, &payload) != reference.digest
    {
        return Err(ManagementError::InvalidEvidence);
    }
    budget.charge(0, 0, 1, (raw.len() + payload.len()) as u64)?;
    budget.work.output_bytes = budget
        .work
        .output_bytes
        .checked_add(payload.len() as u64)
        .ok_or(ManagementError::Budget)?;
    budget.check()?;
    output
        .write_all(&payload)
        .map_err(|_| ManagementError::Database)?;
    Ok(Some(raw))
}
