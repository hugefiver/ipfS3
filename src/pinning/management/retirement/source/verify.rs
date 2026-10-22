//! Revalidate a retained archive against a bounded live pass, without copying it.
use super::*;

impl InventorySpool {
    /// Finish scan first. Reopening never trusts file-supplied EOF, revisions or
    /// reducer facts. This pass compares original bytes, ordered members and wire
    /// coordinates. Existing derived claims remain for canonical comparison.
    pub async fn verify_source(
        &mut self,
        source: &DatabaseConnection,
        limits: &RunLimits,
    ) -> Result<Progress> {
        let mut run = super::super::spool::task_run(&self.db, limits).await?;
        let original = self.checkpoint().await?;
        if !original.complete() {
            return Err(ManagementError::InvalidEvidence);
        }
        loop {
            if !run.page(0, 0)? {
                return Ok(Progress::Pending);
            }
            let checkpoint = self
                .verification
                .source
                .clone()
                .unwrap_or_else(|| original.rewind());
            let first = self.verification.source.is_none();
            let mut tx =
                crate::store::pinning::management::transaction::ManagementTransaction::begin(
                    source,
                    run.remaining(),
                    limits.cancellation.clone(),
                )
                .await
                .map_err(page::transaction_error)?;
            let control = page::Control {
                cancel: limits.cancellation.clone(),
                total_rows: run.total_rows,
                total_bytes: run.total_bytes,
                ordered_cells: false,
            };
            let read = tx
                .run(move |tx, _| {
                    Box::pin(async move {
                        if first {
                            let sql = if tx.get_database_backend() == DatabaseBackend::Sqlite {
                                "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now') AS now"
                            } else {
                                "SELECT CAST(clock_timestamp() AS TEXT) AS now"
                            };
                            let now = tx
                                .query_one(Statement::from_string(tx.get_database_backend(), sql))
                                .await
                                .map_err(|_| ManagementError::Database)?
                                .ok_or(ManagementError::Database)?;
                            if checkpoint.observed_at
                                > native::parse_time(&get::<String>(&now, "now")?)?
                            {
                                return Err(ManagementError::InvalidEvidence);
                            }
                        }
                        page::read(tx, checkpoint, 0, 0, control).await
                    })
                })
                .await
                .map_err(page::transaction_error)?;
            tx.rollback().await.map_err(page::transaction_error)?;
            let metering = self
                .db
                .begin()
                .await
                .map_err(|_| ManagementError::Database)?;
            super::super::spool::budget::charge(
                &metering,
                &run,
                read.rows,
                read.work.keyset.bytes + read.work.cells.bytes,
            )
            .await?;
            super::super::spool::budget::record_queries(
                &metering,
                "revalidation_keyset",
                &read.work.keyset,
            )
            .await?;
            super::super::spool::budget::record_queries(
                &metering,
                "revalidation_cells",
                &read.work.cells,
            )
            .await?;
            metering
                .commit()
                .await
                .map_err(|_| ManagementError::Database)?;
            let stored = self.db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                "SELECT ordinal,table_name,CASE WHEN page_bytes<=8388608 THEN row_key END AS row_key,column_name,offset,length,CASE WHEN page_bytes<=8388608 THEN bytes END AS bytes FROM (SELECT *,SUM(payload_bytes) OVER (ORDER BY ordinal ROWS UNBOUNDED PRECEDING) AS page_bytes FROM (SELECT ordinal,CASE WHEN length(table_name)<=256 THEN table_name END AS table_name,CASE WHEN length(CAST(row_key AS BLOB))<=65536 THEN row_key END AS row_key,CASE WHEN length(column_name)<=256 THEN column_name END AS column_name,offset,length,CASE WHEN length(bytes)<=65536 THEN bytes END AS bytes,length(bytes)+length(CAST(row_key AS BLOB)) AS payload_bytes FROM parts WHERE ordinal>$1 ORDER BY ordinal LIMIT $2)) ORDER BY ordinal",
                vec![self.verification.source_ordinal.into(),(read.parts.len() as i64).into()]
            )).await.map_err(|_|ManagementError::Database)?;
            if stored.len() != read.parts.len() {
                return Err(ManagementError::Changed);
            }
            let mut ordinal = self.verification.source_ordinal;
            for (row, part) in stored.into_iter().zip(&read.parts) {
                ordinal += 1;
                if get::<i64>(&row, "ordinal")? != ordinal
                    || get::<String>(&row, "table_name")? != part.table
                    || get::<String>(&row, "row_key")? != encode(&part.key)?
                    || get::<String>(&row, "column_name")? != part.column
                    || get::<i64>(&row, "offset")? != part.offset as i64
                    || get::<Option<i64>>(&row, "length")? != part.length.map(|n| n as i64)
                    || get::<Vec<u8>>(&row, "bytes")? != part.bytes
                {
                    return Err(ManagementError::Changed);
                }
            }
            let stored = self.db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                "SELECT m.table_name,m.row_key,m.position,k.sort_key FROM members m LEFT JOIN wire_keys k ON k.table_name=m.table_name AND k.row_key=m.row_key WHERE position>$1 ORDER BY position LIMIT $2",
                vec![self.verification.source_position.into(),(read.members.len() as i64).into()]
            )).await.map_err(|_|ManagementError::Database)?;
            if stored.len() != read.members.len() {
                return Err(ManagementError::Changed);
            }
            let mut position = self.verification.source_position;
            for (row, (key, _)) in stored.into_iter().zip(&read.members) {
                position += 1;
                if get::<i64>(&row, "position")? != position
                    || get::<String>(&row, "table_name")? != key.table
                    || get::<String>(&row, "row_key")? != encode(key)?
                    || get::<Vec<u8>>(&row, "sort_key")? != super::super::spool::wire_key(key)
                {
                    return Err(ManagementError::Changed);
                }
            }
            for table in &read.eof {
                if one(
                    &self.db,
                    "SELECT table_name FROM eof WHERE table_name=$1",
                    vec![table.clone().into()],
                )
                .await?
                .is_none()
                {
                    return Err(ManagementError::Changed);
                }
            }
            run.check()?;
            let complete = read.checkpoint.complete();
            self.verification.source = Some(read.checkpoint);
            self.verification.source_ordinal = ordinal;
            self.verification.source_position = position;
            if complete {
                if one(&self.db,"SELECT k.table_name FROM wire_keys k WHERE NOT EXISTS(SELECT 1 FROM members m WHERE m.table_name=k.table_name AND m.row_key=k.row_key) LIMIT 1",vec![]).await?.is_some() {return Err(ManagementError::Changed);}
                if one(
                    &self.db,
                    "SELECT ordinal FROM parts WHERE ordinal>$1 OR ordinal<1 LIMIT 1",
                    vec![ordinal.into()],
                )
                .await?
                .is_some()
                    || one(
                        &self.db,
                        "SELECT position FROM members WHERE position>$1 OR position<1 LIMIT 1",
                        vec![position.into()],
                    )
                    .await?
                    .is_some()
                {
                    return Err(ManagementError::Changed);
                }
                self.source_scanned = true;
                return Ok(Progress::Archived);
            }
        }
    }
}
