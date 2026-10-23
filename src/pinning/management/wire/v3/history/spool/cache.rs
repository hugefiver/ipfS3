use super::*;
use std::io::{Read, Seek, SeekFrom};
impl Spool {
    /// Cache is task work storage, never part of the business/source scan.
    pub async fn store_wire(&self, file: &mut File, budget: &mut Budget) -> Result<()> {
        file.seek(SeekFrom::Start(0)).map_err(invalid)?;
        let _memory = budget
            .reserve(super::super::super::CELL_BYTES)
            .map_err(storage)?;
        let tx = self.db.begin().await.map_err(invalid)?;
        let result = async {
            exec(&tx, "DELETE FROM sealed_wire", vec![], budget).await?;
            let mut bytes = vec![0; super::super::super::CELL_BYTES];
            let mut ordinal = 0i64;
            loop {
                budget.check().map_err(storage)?;
                let n = file.read(&mut bytes).map_err(invalid)?;
                if n == 0 {
                    break;
                }
                budget.charge(0, 0, 0, n as u64).map_err(storage)?;
                exec(
                    &tx,
                    "INSERT INTO sealed_wire VALUES($1,$2)",
                    vec![ordinal.into(), bytes[..n].to_vec().into()],
                    budget,
                )
                .await?;
                budget.work.copied_bytes = budget
                    .work
                    .copied_bytes
                    .checked_add(n as u64)
                    .ok_or(SafeError::LimitExceeded)?;
                ordinal = ordinal.checked_add(1).ok_or(SafeError::LimitExceeded)?;
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            tx.rollback().await.map_err(invalid)?;
            return Err(e);
        }
        tx.commit().await.map_err(invalid)
    }
    pub async fn load_wire(&self, budget: &mut Budget) -> Result<File> {
        use crate::store::pinning::management::native::meter::PAGE_BYTES;

        const PAGE_CHUNKS: usize = PAGE_BYTES / super::super::super::CELL_BYTES;
        // Reserve the driver page and decoded payloads before querying the spool.
        let _memory = budget.reserve(2 * PAGE_BYTES).map_err(storage)?;
        let mut file = files::stream::temporary().map_err(invalid)?;
        let mut ordinal = 0i64;
        loop {
            budget.charge(1, 0, 0, 0).map_err(storage)?;
            let rows = self.db.query_all(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT ordinal,CASE WHEN length(payload)<=65536 THEN payload END AS payload FROM sealed_wire WHERE ordinal>=$1 ORDER BY ordinal LIMIT $2",
                vec![ordinal.into(), (PAGE_CHUNKS as i64).into()],
            )).await.map_err(invalid)?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                if get::<i64>(&row, "ordinal")? != ordinal {
                    return Err(SafeError::InvalidInput);
                }
                let bytes: Vec<u8> = get(&row, "payload")?;
                if bytes.is_empty() {
                    return Err(SafeError::InvalidInput);
                }
                budget
                    .charge(0, 1, 1, bytes.len() as u64)
                    .map_err(storage)?;
                file.write_all(&bytes).map_err(invalid)?;
                budget.work.copied_bytes = budget
                    .work
                    .copied_bytes
                    .checked_add(bytes.len() as u64)
                    .ok_or(SafeError::LimitExceeded)?;
                ordinal = ordinal.checked_add(1).ok_or(SafeError::LimitExceeded)?;
            }
        }
        file.seek(SeekFrom::Start(0)).map_err(invalid)?;
        Ok(file)
    }
}

#[cfg(test)]
mod tests;
