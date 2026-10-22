//! One bounded source read. File staging happens after acknowledged source rollback.
use super::*;
use crate::store::pinning::management::transaction::{
    CancellationToken, StopCause, TransactionFailure,
};

pub(super) struct Control {
    pub cancel: CancellationToken,
    pub total_rows: u64,
    pub total_bytes: u64,
    pub ordered_cells: bool,
}
pub(super) struct Page {
    pub checkpoint: SourceCheckpoint,
    pub rows: u64,
    pub bytes: u64,
    pub parts: Vec<ArchivePart>,
    pub members: Vec<(NativeKey, u64)>,
    pub eof: Vec<String>,
    pub work: ReadWork,
}
pub(super) fn transaction_error(error: TransactionFailure) -> ManagementError {
    if !error.rollback_confirmed {
        return ManagementError::Database;
    }
    match error.cause {
        StopCause::Cancelled | StopCause::Deadline => ManagementError::Budget,
        _ => error.management_error(),
    }
}

pub(super) fn interrupted(error: TransactionFailure, staged_page: bool) -> Result<Progress> {
    if staged_page && error.rollback_confirmed && error.cause == StopCause::Deadline {
        Ok(Progress::Pending)
    } else {
        Err(transaction_error(error))
    }
}

pub(super) async fn read(
    tx: &DatabaseTransaction,
    checkpoint: SourceCheckpoint,
    rows: u64,
    bytes: u64,
    control: Control,
) -> Result<Page> {
    checkpoint.check(tx).await?;
    let mut page = Page {
        checkpoint,
        rows,
        bytes,
        parts: Vec::new(),
        members: Vec::new(),
        eof: Vec::new(),
        work: ReadWork::default(),
    };
    let initial_bytes = bytes;
    while page.parts.len() < 256 {
        if control.cancel.is_cancelled() {
            return Err(ManagementError::Budget);
        }
        let cp = &mut page.checkpoint;
        if let Some(table) = native::current_catalog().get(cp.table) {
            if cp.row.is_none() && page.parts.is_empty() {
                let batch = batch::read(
                    tx,
                    table,
                    cp.after.as_ref(),
                    control.ordered_cells.then_some(256),
                    &mut page.work,
                )
                .await?;
                if !batch.is_empty() {
                    for (key, parts) in batch {
                        page.rows = page.rows.checked_add(1).ok_or(ManagementError::Budget)?;
                        for part in parts {
                            page.bytes = page
                                .bytes
                                .checked_add(part.bytes.len() as u64)
                                .ok_or(ManagementError::Budget)?;
                            page.parts.push(part);
                        }
                        page.members.push((key.clone(), page.rows));
                        cp.after = Some(key);
                    }
                    if page.rows > control.total_rows || page.bytes > control.total_bytes {
                        return Err(ManagementError::Budget);
                    }
                    break;
                }
            }
            if cp.row.is_none() {
                cp.row = next_key(tx, table, cp.after.as_ref(), &mut page.work.keyset).await?;
            }
            if let Some(key) = cp.row.as_ref() {
                let values = cells::read(
                    tx,
                    table,
                    key,
                    (cp.column, cp.offset),
                    256 - page.parts.len(),
                    1024 * 1024 - (page.bytes - initial_bytes),
                    &mut page.work.cells,
                )
                .await?;
                if values.is_empty() {
                    break;
                }
                for value in values {
                    page.bytes = page
                        .bytes
                        .checked_add(value.bytes.len() as u64)
                        .ok_or(ManagementError::Budget)?;
                    if page.bytes > control.total_bytes {
                        return Err(ManagementError::Budget);
                    }
                    cp.offset += value.bytes.len() as u64;
                    if value.length.is_none_or(|length| cp.offset == length) {
                        cp.column += 1;
                        cp.offset = 0;
                    }
                    page.parts.push(value);
                }
                if cp.column == table.columns.len() {
                    page.rows = page.rows.checked_add(1).ok_or(ManagementError::Budget)?;
                    if page.rows > control.total_rows {
                        return Err(ManagementError::Budget);
                    }
                    page.members.push((key.clone(), page.rows));
                    cp.after = cp.row.take();
                    cp.column = 0;
                }
            } else {
                page.eof.push(table.table.clone());
                cp.table += 1;
                cp.after = None;
            }
        }
        if cp.complete() || page.bytes - initial_bytes >= 1024 * 1024 {
            break;
        }
    }
    page.checkpoint.check(tx).await?;
    Ok(page)
}

impl InventorySpool {
    pub(super) async fn stage_page(
        &self,
        expected: &str,
        page: Page,
        run: &Run<'_>,
    ) -> Result<bool> {
        run.check()?;
        let tx = self
            .db
            .begin()
            .await
            .map_err(|_| ManagementError::Database)?;
        let result=async {
            let mut staged=QueryWork::default();
            stage::parts(&tx, &page.parts, run,&mut staged).await?;
            stage::members(&tx, &page.members, run,&mut staged).await?;
            super::super::spool::budget::copy_work(&tx,run,usize::try_from(staged.bytes).map_err(|_|ManagementError::Budget)?).await?;
            super::super::spool::budget::record_queries(&tx,"source_keyset",&page.work.keyset).await?;
            super::super::spool::budget::record_queries(&tx,"source_cells",&page.work.cells).await?;
            super::super::spool::budget::record_queries(&tx,"spool_chunks",&staged).await?;
            for table in &page.eof {
                run.check()?;
                if one(&tx,"SELECT table_name FROM eof WHERE table_name=$1",vec![table.clone().into()]).await?.is_none() {exec(&tx,"INSERT INTO eof VALUES($1)",vec![table.clone().into()]).await?;}
            }
            let changed=tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"UPDATE state SET checkpoint=$1,rows=$2,bytes=$3,phase=$4 WHERE id=1 AND phase=0 AND checkpoint=$5",vec![encode(&page.checkpoint)?.into(),i64::try_from(page.rows).map_err(|_|ManagementError::Budget)?.into(),i64::try_from(page.bytes).map_err(|_|ManagementError::Budget)?.into(),i64::from(page.checkpoint.complete()).into(),expected.into()])).await.map_err(|_|ManagementError::Database)?;
            if changed.rows_affected()!=1 {return Err(ManagementError::Changed)}
            run.check()
        }.await;
        if let Err(error) = result {
            tx.rollback().await.map_err(|_| ManagementError::Database)?;
            return Err(error);
        }
        tx.commit().await.map_err(|_| ManagementError::Database)?;
        Ok(page.checkpoint.complete())
    }
}

#[cfg(test)]
#[path = "page_tests.rs"]
mod sqlite_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_acknowledged_slice_deadline_after_progress_can_resume() {
        let failure = TransactionFailure {
            cause: StopCause::Deadline,
            rollback_confirmed: true,
        };
        assert_eq!(interrupted(failure, true), Ok(Progress::Pending));
        assert_eq!(interrupted(failure, false), Err(ManagementError::Budget));
        assert_eq!(
            interrupted(
                TransactionFailure {
                    rollback_confirmed: false,
                    ..failure
                },
                true
            ),
            Err(ManagementError::Database)
        );
        assert_eq!(
            interrupted(
                TransactionFailure {
                    cause: StopCause::Cancelled,
                    ..failure
                },
                true
            ),
            Err(ManagementError::Budget)
        );
        assert_eq!(
            interrupted(
                TransactionFailure {
                    cause: StopCause::Management(ManagementError::Budget),
                    ..failure
                },
                true
            ),
            Err(ManagementError::Budget)
        );
    }
}
