//! Frozen reviews are rebuilt from original chunks, not trusted cached totals.
use super::{
    spool::{exec, get, one, put_part},
    *,
};
use crate::store::pinning::management::native;
use sea_orm::TransactionTrait;

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreCursor {
    ordinal: i64,
    rows: i64,
    bytes: u64,
    column: usize,
    offset: u64,
    key: Option<NativeKey>,
    length: Option<u64>,
}
impl InventorySpool {
    /// Reconstructs a frozen archive in a fresh task spool. Only original cells,
    /// membership, checkpoint and frozen configuration are imported; all facts
    /// and totals must be reduced again. This never obtains execution authority.
    pub async fn reconstruct(
        &mut self,
        original: &InventorySpool,
        limits: &RunLimits,
    ) -> Result<Progress> {
        self.source_scanned = false;
        let checkpoint = original.checkpoint().await?;
        if !checkpoint.complete()
            || !crate::store::migrations::history::final_checkpoint(
                &checkpoint.catalog_revision,
                &checkpoint.catalog,
            )
        {
            return Err(ManagementError::InvalidEvidence);
        }
        let mut run = super::spool::task_run(&self.db, limits).await?;
        loop {
            let state = one(&self.db, "SELECT phase FROM state WHERE id=1", vec![])
                .await?
                .ok_or(ManagementError::InvalidEvidence)?;
            if get::<i64>(&state, "phase")? > 0 {
                return Ok(Progress::Archived);
            }
            let mut cursor: RestoreCursor = if let Some(row) = one(
                &self.db,
                "SELECT payload FROM restore_progress WHERE id=1",
                vec![],
            )
            .await?
            {
                decode(&get::<String>(&row, "payload")?)?
            } else {
                RestoreCursor::default()
            };
            if !run.page(cursor.rows as u64, cursor.bytes)? {
                return Ok(Progress::Pending);
            }
            let mut source = ArchiveCursor {
                ordinal: cursor.ordinal,
            };
            let tx = self
                .db
                .begin()
                .await
                .map_err(|_| ManagementError::Database)?;
            let mut eof = false;
            for _ in 0..16 {
                run.check()?;
                let Some(part) = original.read_part(&mut source).await? else {
                    eof = true;
                    break;
                };
                if cursor.key.is_none() {
                    let row = one(
                        &original.db,
                        "SELECT row_key FROM members WHERE position=$1",
                        vec![(cursor.rows + 1).into()],
                    )
                    .await?
                    .ok_or(ManagementError::InvalidEvidence)?;
                    let key: NativeKey = decode(&get::<String>(&row, "row_key")?)?;
                    if key != part.key {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    cursor.key = Some(key);
                }
                let key = cursor
                    .key
                    .as_ref()
                    .ok_or(ManagementError::InvalidEvidence)?;
                let table = native::current_catalog()
                    .iter()
                    .find(|t| t.table == key.table)
                    .ok_or(ManagementError::InvalidEvidence)?;
                if key != &part.key
                    || part.table != key.table
                    || table.columns.get(cursor.column) != Some(&part.column)
                    || part.offset != cursor.offset
                {
                    return Err(ManagementError::InvalidEvidence);
                }
                if cursor.offset == 0 {
                    cursor.length = part.length;
                }
                if cursor.length != part.length
                    || part.length.is_some_and(|n| {
                        part.offset > n
                            || part.bytes.len() as u64
                                != (n - part.offset).min(native::meter::CELL_SLICE as u64)
                    })
                {
                    return Err(ManagementError::InvalidEvidence);
                }
                cursor.bytes = cursor
                    .bytes
                    .checked_add(part.bytes.len() as u64)
                    .ok_or(ManagementError::Budget)?;
                if cursor.bytes > run.total_bytes {
                    return Err(ManagementError::Budget);
                }
                put_part(&tx, &part).await?;
                cursor.offset += part.bytes.len() as u64;
                if part.length.is_none_or(|n| cursor.offset == n) {
                    cursor.column += 1;
                    cursor.offset = 0;
                }
                if cursor.column == table.columns.len() {
                    cursor.rows += 1;
                    if cursor.rows as u64 > run.total_rows {
                        return Err(ManagementError::Budget);
                    }
                    exec(
                        &tx,
                        "INSERT INTO members VALUES($1,$2,$3)",
                        vec![
                            key.table.clone().into(),
                            encode(key)?.into(),
                            cursor.rows.into(),
                        ],
                    )
                    .await?;
                    super::spool::index_wire_key(&tx, key).await?;
                    cursor.key = None;
                    cursor.column = 0;
                }
                cursor.ordinal = source.ordinal;
            }
            if eof {
                if cursor.key.is_some()
                    || one(
                        &original.db,
                        "SELECT position FROM members WHERE position>$1 LIMIT 1",
                        vec![cursor.rows.into()],
                    )
                    .await?
                    .is_some()
                {
                    return Err(ManagementError::InvalidEvidence);
                }
                for table in native::current_catalog() {
                    if one(
                        &original.db,
                        "SELECT table_name FROM eof WHERE table_name=$1",
                        vec![table.table.clone().into()],
                    )
                    .await?
                    .is_none()
                    {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    exec(
                        &tx,
                        "INSERT INTO eof VALUES($1)",
                        vec![table.table.clone().into()],
                    )
                    .await?;
                }
                let configuration: ConfigurationBinding = original
                    .lookup("configuration", "inventory")
                    .await?
                    .ok_or(ManagementError::InvalidEvidence)?;
                super::spool::fact(&tx, "configuration", "inventory", &configuration).await?;
                exec(&tx,"UPDATE state SET reference=$1,checkpoint=$2,phase=1,rows=$3,bytes=$4 WHERE id=1",vec![encode(original.reference())?.into(),encode(&checkpoint)?.into(),cursor.rows.into(),(cursor.bytes as i64).into()]).await?;
            }
            if one(&tx, "SELECT id FROM restore_progress WHERE id=1", vec![])
                .await?
                .is_some()
            {
                exec(
                    &tx,
                    "UPDATE restore_progress SET payload=$1 WHERE id=1",
                    vec![encode(&cursor)?.into()],
                )
                .await?;
            } else {
                exec(
                    &tx,
                    "INSERT INTO restore_progress VALUES(1,$1)",
                    vec![encode(&cursor)?.into()],
                )
                .await?;
            }
            run.check()?;
            tx.commit().await.map_err(|_| ManagementError::Database)?;
            if eof {
                self.reference = original.reference().clone();
                return Ok(Progress::Archived);
            }
        }
    }
}
