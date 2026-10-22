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
        let _memory = budget
            .reserve(super::super::super::CELL_BYTES)
            .map_err(storage)?;
        let mut file = files::stream::temporary().map_err(invalid)?;
        let mut ordinal = 0i64;
        while let Some(row)=one(&self.db,"SELECT ordinal,CASE WHEN length(payload)<=65536 THEN payload END AS payload FROM sealed_wire WHERE ordinal>=$1 ORDER BY ordinal LIMIT 1",vec![ordinal.into()],budget).await? {
            if get::<i64>(&row,"ordinal")?!=ordinal {return Err(SafeError::InvalidInput)}
            let bytes:Vec<u8>=get(&row,"payload")?;
            if bytes.is_empty() {return Err(SafeError::InvalidInput)}
            budget.charge(0,0,0,bytes.len() as u64).map_err(storage)?;
            file.write_all(&bytes).map_err(invalid)?;
            budget.work.copied_bytes=budget.work.copied_bytes.checked_add(bytes.len() as u64).ok_or(SafeError::LimitExceeded)?;
            ordinal=ordinal.checked_add(1).ok_or(SafeError::LimitExceeded)?;
        }
        file.seek(SeekFrom::Start(0)).map_err(invalid)?;
        Ok(file)
    }
}
