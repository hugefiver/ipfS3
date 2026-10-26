use super::*;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
#[cfg(test)]
mod tests;

const BATCH_SQL: &str = "SELECT ordinal,CASE WHEN length(CAST(payload AS BLOB)) BETWEEN 1 AND 65536 THEN payload END AS payload FROM pin_management_archive_chunks WHERE archive_id=$1 AND ordinal>=$2 AND ordinal<$3 ORDER BY ordinal";

/// Only the owner-private prepared S75A1 output uses this bounded transfer.
/// The sink remains unpublished until both archive and wire EOF are verified.
pub(super) async fn copy<W: Write>(
    db: &DatabaseConnection,
    reference: &segmented::ArchiveRef,
    raw: &str,
    output: &mut W,
    budget: &mut Budget,
) -> Result<()> {
    budget.charge(1, 1, 1, raw.len() as u64)?;
    let row = spool::one(db,
        "SELECT 1 AS valid FROM pin_management_archive WHERE archive_id=$1 AND format='S75A1' AND state='sealed' AND header=$2 AND chunks=$3 AND bytes=$4 AND digest=$5",
        vec![reference.archive_id.clone().into(), raw.into(), reference.chunks.into(), reference.bytes.into(), reference.digest.clone().into()],
    ).await?;
    if row.is_none() || reference.chunks < 0 || reference.bytes < 0 {
        return Err(ManagementError::InvalidEvidence);
    }
    let _memory = budget.reserve(2 * v3::FRAME_BYTES)?;
    budget.charge(0, 0, 0, reference.bytes as u64)?;
    budget.work.output_bytes = budget
        .work
        .output_bytes
        .checked_add(reference.bytes as u64)
        .ok_or(ManagementError::Budget)?;
    budget.check()?;
    let mut hash = Sha256::new_with_prefix(b"S75A1\0");
    hash.update((raw.len() as u64).to_be_bytes());
    hash.update(raw.as_bytes());
    let mut bytes = 0i64;
    let mut ordinal = 0i64;
    while ordinal < reference.chunks {
        let end = ordinal.saturating_add(16).min(reference.chunks);
        budget.charge(1, (end - ordinal) as u64, 2 * (end - ordinal) as u64, 0)?;
        let rows = db
            .query_all(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                BATCH_SQL,
                [
                    reference.archive_id.clone().into(),
                    ordinal.into(),
                    end.into(),
                ],
            ))
            .await
            .map_err(|_| ManagementError::Database)?;
        if rows.len() != (end - ordinal) as usize {
            return Err(ManagementError::InvalidEvidence);
        }
        for row in rows {
            if spool::get::<i64>(&row, "ordinal")? != ordinal {
                return Err(ManagementError::InvalidEvidence);
            }
            let chunk: Vec<u8> = spool::get(&row, "payload")?;
            bytes = bytes
                .checked_add(chunk.len() as i64)
                .ok_or(ManagementError::Budget)?;
            if chunk.is_empty() || chunk.len() > v3::CELL_BYTES || bytes > reference.bytes {
                return Err(ManagementError::InvalidEvidence);
            }
            hash.update((ordinal as u64).to_be_bytes());
            hash.update((chunk.len() as u64).to_be_bytes());
            hash.update(&chunk);
            output
                .write_all(&chunk)
                .map_err(|_| ManagementError::Database)?;
            ordinal += 1;
        }
    }
    budget.charge(1, 0, 0, 0)?;
    let extra = spool::one(
        db,
        "SELECT 1 FROM pin_management_archive_chunks WHERE archive_id=$1 AND ordinal>=$2 LIMIT 1",
        vec![reference.archive_id.clone().into(), reference.chunks.into()],
    )
    .await?;
    if extra.is_some()
        || bytes != reference.bytes
        || hex::encode(hash.finalize()) != reference.digest
    {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(())
}
