use super::*;

impl<S: Read + Seek> FrozenRetirementImport<S> {
    pub(super) async fn finish_originals(&mut self, run: &mut Run<'_>) -> Result<bool> {
        let reference: RetirementInventoryRef = self.metadata("reference").await?;
        let membership: RetirementInventoryRef = self.metadata("membership_reference").await?;
        let checkpoint: SourceCheckpoint = self.metadata("source_descriptor").await?;
        let (scope, identity, catalog) = self
            .header
            .as_ref()
            .ok_or(ManagementError::InvalidEvidence)?;
        if reference != self.expected
            || membership.reducer_version != REDUCER_VERSION
            || !checkpoint.complete()
            || checkpoint.catalog_revision != crate::store::pinning::management::schema::VERSION
            || &checkpoint.database_scope != scope
            || &checkpoint.database_identity != identity
            || &checkpoint.catalog != catalog
        {
            return Err(ManagementError::InvalidEvidence);
        }
        while self.position < self.rows {
            let tx = self
                .spool
                .db
                .begin()
                .await
                .map_err(|_| ManagementError::Database)?;
            let mut pending = false;
            let result = async {
                for _ in 0..16 {
                    if self.position == self.rows {
                        break;
                    }
                    if !run.page(self.rows, self.wire_bytes)? {
                        pending = true;
                        break;
                    }
                    self.cursor.budget_mut().charge(9, 0, 0, 0)?;
                    let member = one(
                &tx,
                "SELECT row_key,table_name FROM import_members WHERE inventory=$1 AND position=$2",
                vec![
                    membership.inventory_id.clone().into(),
                    i64::try_from(self.position + 1)
                        .map_err(|_| ManagementError::Budget)?
                        .into(),
                ],
            )
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
                    let encoded: String = get(&member, "row_key")?;
                    let key: NativeKey = decode(&encoded)?;
                    if key.table != get::<String>(&member, "table_name")?
                        || one(
                            &tx,
                            "SELECT row_key FROM import_rows WHERE row_key=$1",
                            vec![encoded.clone().into()],
                        )
                        .await?
                        .is_none()
                    {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    let table = native::current_catalog()
                        .iter()
                        .find(|t| t.table == key.table)
                        .ok_or(ManagementError::InvalidEvidence)?;
                    let column = table
                        .columns
                        .get(self.part_column)
                        .ok_or(ManagementError::InvalidEvidence)?;
                    let cell = one(
                        &tx,
                        "SELECT length FROM import_columns WHERE row_key=$1 AND column_name=$2",
                        vec![encoded.clone().into(), column.clone().into()],
                    )
                    .await?
                    .ok_or(ManagementError::InvalidEvidence)?;
                    let length = get::<Option<i64>>(&cell, "length")?
                        .map(|n| u64::try_from(n).map_err(|_| ManagementError::InvalidEvidence))
                        .transpose()?;
                    let chunk = one(
                &tx,
                "SELECT bytes FROM import_chunks WHERE row_key=$1 AND column_name=$2 AND offset=$3",
                vec![
                    encoded.clone().into(),
                    column.clone().into(),
                    i64::try_from(self.part_offset)
                        .map_err(|_| ManagementError::Budget)?
                        .into(),
                ],
            )
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
                    let bytes: Vec<u8> = get(&chunk, "bytes")?;
                    if bytes.len() as u64
                        != length
                            .unwrap_or(0)
                            .checked_sub(self.part_offset)
                            .ok_or(ManagementError::InvalidEvidence)?
                            .min(CELL_SLICE as u64)
                    {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    let next_offset = self.part_offset + bytes.len() as u64;
                    put_part(
                        &tx,
                        &ArchivePart {
                            table: key.table.clone(),
                            key: key.clone(),
                            column: column.clone(),
                            offset: self.part_offset,
                            length,
                            bytes,
                        },
                    )
                    .await?;
                    let complete = length.is_none_or(|n| n == next_offset);
                    if complete && self.part_column + 1 == table.columns.len() {
                        exec(
                            &tx,
                            "INSERT INTO members VALUES($1,$2,$3)",
                            vec![
                                key.table.clone().into(),
                                encoded.into(),
                                i64::try_from(self.position + 1)
                                    .map_err(|_| ManagementError::Budget)?
                                    .into(),
                            ],
                        )
                        .await?;
                        super::super::spool::index_wire_key(&tx, &key).await?;
                    }
                    if complete {
                        self.part_column += 1;
                        self.part_offset = 0;
                    } else {
                        self.part_offset = next_offset;
                    }
                    if self.part_column == table.columns.len() {
                        self.position += 1;
                        self.part_column = 0;
                    }
                }
                run.check()
            }
            .await;
            if let Err(error) = result {
                tx.rollback().await.map_err(|_| ManagementError::Database)?;
                return Err(error);
            }
            tx.commit().await.map_err(|_| ManagementError::Database)?;
            if pending {
                return Ok(false);
            }
        }
        if one(
            &self.spool.db,
            "SELECT position FROM import_members WHERE inventory=$1 AND position>$2 LIMIT 1",
            vec![
                membership.inventory_id.clone().into(),
                i64::try_from(self.rows)
                    .map_err(|_| ManagementError::Budget)?
                    .into(),
            ],
        )
        .await?
        .is_some()
        {
            return Err(ManagementError::InvalidEvidence);
        }
        while let Some(table) = native::current_catalog().get(self.eof_table) {
            if !run.page(self.rows, self.wire_bytes)? {
                return Ok(false);
            }
            self.cursor.budget_mut().charge(2, 0, 0, 0)?;
            if one(
                &self.spool.db,
                "SELECT table_name FROM import_eof WHERE inventory=$1 AND table_name=$2",
                vec![
                    membership.inventory_id.clone().into(),
                    table.table.clone().into(),
                ],
            )
            .await?
            .is_none()
            {
                return Err(ManagementError::InvalidEvidence);
            }
            exec(
                &self.spool.db,
                "INSERT INTO eof VALUES($1)",
                vec![table.table.clone().into()],
            )
            .await?;
            self.eof_table += 1;
        }
        let config = self
            .configuration
            .as_ref()
            .ok_or(ManagementError::InvalidEvidence)?;
        let tx = self
            .spool
            .db
            .begin()
            .await
            .map_err(|_| ManagementError::Database)?;
        super::super::spool::fact(&tx, "configuration", "inventory", config).await?;
        exec(&tx,"UPDATE state SET reference=$1,checkpoint=$2,phase=1,rows=$3,bytes=$4 WHERE id=1 AND phase=0",vec![encode(&reference)?.into(),encode(&checkpoint)?.into(),i64::try_from(self.rows).map_err(|_|ManagementError::Budget)?.into(),i64::try_from(self.bytes).map_err(|_|ManagementError::Budget)?.into()]).await?;
        run.check()?;
        tx.commit().await.map_err(|_| ManagementError::Database)?;
        self.spool.reference = reference;
        Ok(true)
    }
}
