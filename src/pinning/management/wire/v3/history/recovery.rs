use super::spool::{Spool, exec, one, sort_key};
use super::*;
use crate::store::pinning::management::records::segmented::{ArchiveHeader, ArchiveRef};
use sea_orm::TransactionTrait;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::io::{Seek, SeekFrom};
mod confirmation;

/// All identities and applied results here remain untrusted historical claims.
/// There is no method to bind this task DB as a local execution database.
pub struct Recovery {
    pub head: super::super::document::DocumentHead,
    spool: Spool,
    budget: Budget,
    original: File,
}
impl Recovery {
    pub fn budget(&self) -> &Budget {
        &self.budget
    }
    pub async fn close(self) -> Result<()> {
        self.spool.close().await
    }
    /// Recreate the complete, reverified archive at a NEW protected path. This
    /// retains the same work/deadline/cancellation budget and grants no execution.
    pub async fn export_private(self, path: &str) -> Result<Budget> {
        let Self {
            spool,
            budget,
            original,
            head,
        } = self;
        spool.close().await?;
        drop(head);
        let cursor = super::super::archive_cursor::stage_import(
            original,
            Projection::PrivateRecovery,
            budget,
        )?
        .into_cursor();
        files::stream::export_archive(cursor, path, Projection::PrivateRecovery).map_err(invalid)
    }
    /// Exact converted generation as historical data, never a local source seal
    /// or committed replay capability. All dependent ArchiveRefs remain claims.
    pub async fn converted_source(
        &mut self,
        key: &NativeKey,
        generation: i64,
    ) -> Result<crate::store::initialization::history::ConvertedSource> {
        let id = crate::store::initialization::history::archive_id(
            &self.head.database_scope,
            &self.head.database_identity,
            key,
            generation,
        )
        .map_err(storage)?;
        let reference = self.archive(&id).await?;
        let source: crate::store::initialization::history::ConvertedSource =
            self.read_part(&reference).await?;
        if source.source != *key || source.source_generation != generation {
            return Err(SafeError::InvalidInput);
        }
        Ok(source)
    }
    /// Restore the exact row's frozen review projection, not its current row's
    /// later confirmation. Imported ACKs and identities still grant no authority.
    pub async fn frozen_review(
        &mut self,
        row: &crate::store::initialization::history::RowReference,
    ) -> Result<Option<crate::store::initialization::history::ReviewIndex>> {
        let Some(reference) = &row.review else {
            return Ok(None);
        };
        let mut review: crate::store::initialization::history::ReviewIndex =
            self.read_part(&reference.version).await?;
        if review.anchor.as_ref() != Some(&reference.anchor)
            || review.core_identity.as_ref() != Some(&reference.core_identity)
            || review.database_identity != reference.database_identity
            || review.principal != reference.principal
            || row.key.values.first() != Some(&review.database_scope)
        {
            return Err(SafeError::InvalidInput);
        }
        review.confirmed_at = reference.confirmed_at;
        review.acknowledgements = reference.acknowledgements.clone();
        Ok(Some(review))
    }
    pub async fn next_key(
        &mut self,
        table: &str,
        after: Option<&NativeKey>,
    ) -> Result<Option<NativeKey>> {
        if !source::tables().iter().any(|t| t.name == table) {
            return Err(SafeError::InvalidInput);
        }
        self.spool.next_key(table, after, &mut self.budget).await
    }
    /// Streams original UTF-8/NULL. For archive_chunks.payload this exposes the
    /// documented hex representation; use read_archive for original binary bytes.
    pub async fn cell<W: Write>(
        &mut self,
        key: &NativeKey,
        column: &str,
        output: &mut W,
    ) -> Result<Option<u64>> {
        self.spool.cell(key, column, output, &mut self.budget).await
    }
    pub async fn archive(&mut self, id: &str) -> Result<ArchiveRef> {
        ArchiveCells {
            spool: &self.spool,
            budget: &mut self.budget,
        }
        .archive(id)
        .await
    }
    /// The sink remains unpublished until original storage integrity succeeds.
    pub async fn read_archive<W: Write>(
        &mut self,
        reference: &ArchiveRef,
        output: &mut W,
    ) -> Result<ArchiveHeader> {
        ArchiveCells {
            spool: &self.spool,
            budget: &mut self.budget,
        }
        .read_archive(reference, output)
        .await
    }
}

pub(super) struct ArchiveCells<'a> {
    pub spool: &'a Spool,
    pub budget: &'a mut Budget,
}
impl ArchiveCells<'_> {
    async fn text(
        &mut self,
        key: &NativeKey,
        column: &str,
        limit: usize,
    ) -> Result<Option<String>> {
        let _resident = self.budget.reserve(limit * 2).map_err(storage)?;
        let mut output = Bounded {
            bytes: Vec::new(),
            limit,
        };
        self.spool
            .cell(key, column, &mut output, self.budget)
            .await?
            .map(|_| String::from_utf8(output.bytes).map_err(invalid))
            .transpose()
    }
    async fn required(&mut self, key: &NativeKey, column: &str, limit: usize) -> Result<String> {
        self.text(key, column, limit)
            .await?
            .ok_or(SafeError::InvalidInput)
    }
    async fn archive_key(&mut self, id: &str) -> Result<NativeKey> {
        let row = one(&self.spool.db, "SELECT MIN(table_name) AS table_name,COUNT(*) AS roots FROM rows WHERE table_name IN ('pin_management_archive','pin_management_inline_archive') AND archive_id=$1", vec![id.into()], self.budget).await?.ok_or(SafeError::InvalidInput)?;
        if row.try_get::<i64>("", "roots").map_err(invalid)? != 1 {
            return Err(SafeError::InvalidInput);
        }
        Ok(NativeKey {
            table: row.try_get("", "table_name").map_err(invalid)?,
            values: vec![id.into()],
        })
    }
    pub async fn archive(&mut self, id: &str) -> Result<ArchiveRef> {
        let key = self.archive_key(id).await?;
        let inline = key.table == "pin_management_inline_archive";
        if self.required(&key, "format", 16).await? != if inline { "S75I2" } else { "S75A1" }
            || !inline && self.required(&key, "state", 16).await? != "sealed"
        {
            return Err(SafeError::UnknownEvidence);
        }
        if !inline && confirmation::declared(self, &key).await? {
            return Err(SafeError::UnknownEvidence);
        }
        Ok(ArchiveRef {
            archive_id: id.into(),
            digest: self.required(&key, "digest", 64).await?,
            bytes: self
                .required(&key, "bytes", 32)
                .await?
                .parse()
                .map_err(invalid)?,
            chunks: if inline {
                0
            } else {
                self.required(&key, "chunks", 32)
                    .await?
                    .parse()
                    .map_err(invalid)?
            },
        })
    }
    /// Verifies exact stored header text, all chunk coordinates, lengths and
    /// digest before success. The sink must remain unpublished until success.
    pub async fn read_archive<W: Write>(
        &mut self,
        reference: &ArchiveRef,
        output: &mut W,
    ) -> Result<ArchiveHeader> {
        if reference.bytes < 0
            || reference.chunks < 0
            || reference.digest.len() != 64
            || self.archive(&reference.archive_id).await? != *reference
        {
            return Err(SafeError::InvalidInput);
        }
        let key = self.archive_key(&reference.archive_id).await?;
        let raw = self
            .required(&key, "header", super::super::FRAME_BYTES)
            .await?;
        let _resident = self
            .budget
            .reserve(raw.len() * 3 + 4 * super::super::CELL_BYTES)
            .map_err(storage)?;
        let header: ArchiveHeader = serde_json::from_str(&raw).map_err(invalid)?;
        if self.required(&key, "database_scope", 4096).await? != header.database_scope
            || self.required(&key, "native_identity", 4096).await? != header.database_identity
        {
            return Err(SafeError::InvalidInput);
        }
        if key.table == "pin_management_inline_archive" {
            let encoded = self
                .required(&key, "payload", 2 * super::super::CELL_BYTES)
                .await?;
            if !encoded
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(SafeError::InvalidInput);
            }
            let payload = hex::decode(encoded).map_err(invalid)?;
            if payload.len() as i64 != reference.bytes
                || crate::store::pinning::management::records::segmented::inline_digest(
                    &raw, &payload,
                ) != reference.digest
            {
                return Err(SafeError::InvalidInput);
            }
            self.budget
                .charge(0, 0, 0, payload.len() as u64)
                .map_err(storage)?;
            super::super::codec::write_counted(output, &mut self.budget.work, &payload)?;
            return Ok(header);
        }
        let mut digest = Sha256::new_with_prefix(b"S75A1\0");
        digest.update((raw.len() as u64).to_be_bytes());
        digest.update(raw.as_bytes());
        let mut total = 0i64;
        for ordinal in 0..reference.chunks {
            let key = NativeKey {
                table: "pin_management_archive_chunks".into(),
                values: vec![reference.archive_id.clone(), ordinal.to_string()],
            };
            let encoded = self
                .required(&key, "payload", 2 * super::super::CELL_BYTES)
                .await?;
            if encoded.is_empty()
                || !encoded
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(SafeError::InvalidInput);
            }
            let bytes = hex::decode(encoded).map_err(invalid)?;
            digest.update((ordinal as u64).to_be_bytes());
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(&bytes);
            self.budget
                .charge(0, 0, 0, bytes.len() as u64)
                .map_err(storage)?;
            super::super::codec::write_counted(output, &mut self.budget.work, &bytes)?;
            total = total
                .checked_add(bytes.len() as i64)
                .ok_or(SafeError::LimitExceeded)?;
        }
        if one(&self.spool.db,"SELECT 1 FROM rows WHERE table_name='pin_management_archive_chunks' AND archive_id=$1 AND ordinal>=$2 LIMIT 1",vec![reference.archive_id.clone().into(),reference.chunks.into()],self.budget).await?.is_some() || total!=reference.bytes || hex::encode(digest.finalize())!=reference.digest {return Err(SafeError::InvalidInput)}
        Ok(header)
    }
    pub(super) async fn verify_archives(&mut self) -> Result<()> {
        if one(&self.spool.db,"SELECT 1 FROM rows c WHERE c.table_name='pin_management_archive_chunks' AND NOT EXISTS (SELECT 1 FROM rows h WHERE h.table_name='pin_management_archive' AND h.archive_id=c.archive_id) LIMIT 1",vec![],self.budget).await?.is_some() {return Err(SafeError::InvalidInput)}
        let mut after = None;
        while let Some(key) = self
            .spool
            .next_key("pin_management_archive", after.as_ref(), self.budget)
            .await?
        {
            if confirmation::declared(self, &key).await? {
                confirmation::verify(self, &key).await?;
            } else if self.required(&key, "state", 16).await? == "sealed" {
                let reference = self.archive(&key.values[0]).await?;
                self.read_archive(&reference, &mut std::io::sink()).await?;
            }
            after = Some(key);
        }
        let mut after = None;
        while let Some(key) = self
            .spool
            .next_key("pin_management_inline_archive", after.as_ref(), self.budget)
            .await?
        {
            let reference = self.archive(&key.values[0]).await?;
            self.read_archive(&reference, &mut std::io::sink()).await?;
            after = Some(key);
        }
        Ok(())
    }
}
impl Recovery {
    /// Bounded typed historical parts, e.g. ConvertedSource, ReviewIndex,
    /// NativeRow or original results. Large opaque parts use read_archive instead.
    pub async fn read_part<T: DeserializeOwned>(&mut self, reference: &ArchiveRef) -> Result<T> {
        let bytes = usize::try_from(reference.bytes).map_err(invalid)?;
        let _resident = self
            .budget
            .reserve(bytes.checked_mul(3).ok_or(SafeError::LimitExceeded)?)
            .map_err(storage)?;
        let mut file = files::stream::temporary().map_err(invalid)?;
        self.read_archive(reference, &mut file).await?;
        file.seek(SeekFrom::Start(0)).map_err(invalid)?;
        serde_json::from_reader(std::io::BufReader::with_capacity(
            super::super::CELL_BYTES,
            file,
        ))
        .map_err(invalid)
    }
}
struct Bounded {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("cell budget"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) async fn restore(path: &str, budget: Budget) -> Result<Recovery> {
    let staged = files::stream::stage_temporary(path, Projection::PrivateRecovery, budget)
        .map_err(invalid)?;
    let (head, staged) = staged.inspect().map_err(invalid)?;
    if head.request.is_some()
        || head.configuration.is_some()
        || head.acknowledgements.is_some()
        || !head.items.is_empty()
        || head.applied_claims
    {
        return Err(SafeError::InvalidInput);
    }
    let mut cursor = staged.cursor().map_err(invalid)?;
    let spool = Spool::create(cursor.budget_mut()).await?;
    let result = stage(&spool, &mut cursor).await;
    if let Err(e) = result {
        spool.close().await?;
        return Err(e);
    }
    let (original, budget) = cursor.into_verified()?;
    let mut recovery = Recovery {
        head,
        spool,
        budget,
        original,
    };
    if let Err(e) = (ArchiveCells {
        spool: &recovery.spool,
        budget: &mut recovery.budget,
    })
    .verify_archives()
    .await
    {
        recovery.close().await?;
        return Err(e);
    }
    Ok(recovery)
}
async fn stage(
    spool: &Spool,
    cursor: &mut super::super::archive_cursor::ReviewCursor<File>,
) -> Result<()> {
    let mut row = None;
    let mut column = String::new();
    let mut offset = 0u64;
    let mut column_index = 0;
    let mut key_bytes = Vec::new();
    let mut tx = spool.db.begin().await.map_err(invalid)?;
    let mut events = 0;
    while let Some(event) = cursor.next_event()? {
        let mut budget = cursor.take_budget();
        let result=async {
            match event {
                Event::Frame(Frame::Row {section:Section::Native,key,cells})=> {
                    let table=source::tables().iter().find(|t|t.name==key.table).ok_or(SafeError::InvalidInput)?;
                    if key.values.len()!=table.keys.len() || cells as usize!=table.columns.len() {return Err(SafeError::InvalidInput)}
                    row=Some(key);column_index=0;
                },
                Event::Frame(Frame::Row {..})=>return Err(SafeError::InvalidInput),
                Event::Frame(Frame::Cell {column:next})=> {
                    let key=row.as_ref().ok_or(SafeError::InvalidInput)?;
                    let table=source::tables().iter().find(|t|t.name==key.table).ok_or(SafeError::InvalidInput)?;
                    if table.columns.get(column_index)!=Some(&next) {return Err(SafeError::InvalidInput)}
                    column=next;offset=0;key_bytes.clear();
                    spool.part(&tx,&source::Part {key:key.clone(),column:column.clone(),offset:0,length:Some(0),bytes:Vec::new()},&mut budget).await?;
                },
                Event::Chunk(bytes)=> {
                    let key=row.as_ref().ok_or(SafeError::InvalidInput)?;
                    let table=source::tables().iter().find(|t|t.name==key.table).ok_or(SafeError::InvalidInput)?;
                    if table.keys.contains(&column) {
                        if bytes.len()>1024usize.saturating_sub(key_bytes.len()) {return Err(SafeError::InvalidInput)}
                        key_bytes.extend_from_slice(&bytes);
                    }
                    budget.charge(0,0,0,bytes.len() as u64).map_err(storage)?;
                    exec(&tx,"INSERT INTO parts VALUES($1,$2,$3,$4,$5)",vec![key.table.clone().into(),sort_key(key).into(),column.clone().into(),i64::try_from(offset).map_err(invalid)?.into(),bytes.clone().into()],&mut budget).await?;
                    offset=offset.checked_add(bytes.len() as u64).ok_or(SafeError::LimitExceeded)?;
                },
                Event::Frame(Frame::CellEnd {value})=> {
                    let key=row.as_ref().ok_or(SafeError::InvalidInput)?;
                    let table=source::tables().iter().find(|t|t.name==key.table).ok_or(SafeError::InvalidInput)?;
                    if let Some(index)=table.keys.iter().position(|c|c==&column)
                        && (value!=CellValue::Text || key.values[index].as_bytes()!=key_bytes) {return Err(SafeError::InvalidInput)}
                    let length=match value {CellValue::Null=>None,CellValue::Text=>Some(i64::try_from(offset).map_err(invalid)?),CellValue::Redacted=>return Err(SafeError::InvalidInput)};
                    exec(&tx,"UPDATE cells SET length=$1 WHERE table_name=$2 AND sort_key=$3 AND column_name=$4",vec![length.into(),key.table.clone().into(),sort_key(key).into(),column.clone().into()],&mut budget).await?;
                    column_index+=1;
                },
                Event::Frame(Frame::RowEnd)=> {row=None;},
                _=>{},
            }
            Ok(())
        }.await;
        cursor.restore_budget(budget);
        if let Err(e) = result {
            tx.rollback().await.map_err(invalid)?;
            return Err(e);
        }
        events += 1;
        if events == 256 {
            tx.commit().await.map_err(invalid)?;
            tx = spool.db.begin().await.map_err(invalid)?;
            events = 0;
        }
    }
    tx.commit().await.map_err(invalid)
}
