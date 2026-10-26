//! Bounded asynchronous storage reads; consumers synchronously borrow each page.
use super::*;
use native::meter::ResidentGuard;

pub(crate) struct Pages {
    reference: ArchiveRef,
    digest: Sha256,
    ordinal: i64,
    bytes: i64,
    inline: Option<Vec<u8>>,
    inline_done: bool,
    done: bool,
    _memory: ResidentGuard,
}
pub(crate) struct Page {
    pub(crate) chunks: Vec<Vec<u8>>,
    _memory: ResidentGuard,
}

pub(super) async fn read_page<C: ConnectionTrait>(
    db: &C,
    archive: &str,
    first: i64,
    count: i64,
    budget: &mut Budget,
) -> Result<Page> {
    let _metadata = budget.reserve(8192)?;
    let capacity = count as u64 + 1;
    budget.charge(1, capacity, 2 * capacity, 0)?;
    let (ordinal, length) = if db.get_database_backend() == sea_orm::DatabaseBackend::Sqlite {
        (
            "CASE WHEN typeof(ordinal)='integer' THEN ordinal END",
            "length(CAST(payload AS BLOB))",
        )
    } else {
        ("ordinal", "octet_length(payload)")
    };
    // Scalar lookahead proves the complete range, including fractional ordinals
    // beyond the last payload row, without materializing an extra payload.
    let metadata = db.query_all(Statement::from_sql_and_values(db.get_database_backend(),
        format!("SELECT {ordinal} AS ordinal,CASE WHEN {length} BETWEEN 1 AND 65536 THEN CAST({length} AS BIGINT) END AS payload_bytes FROM pin_management_archive_chunks WHERE archive_id=$1 AND ordinal>=$2 AND ordinal<$3 ORDER BY pin_management_archive_chunks.ordinal LIMIT {capacity}"),
        [archive.into(), first.into(), (first+count).into()])).await.map_err(|_| ManagementError::Database)?;
    if metadata.len() != count as usize {
        return Err(ManagementError::InvalidEvidence);
    }
    let mut sizes = Vec::with_capacity(metadata.len());
    let mut bytes = 0usize;
    for (offset, row) in metadata.into_iter().enumerate() {
        let ordinal: i64 = row
            .try_get("", "ordinal")
            .map_err(|_| ManagementError::InvalidEvidence)?;
        if ordinal != first + offset as i64 {
            return Err(ManagementError::InvalidEvidence);
        }
        let size = usize::try_from(
            row.try_get::<i64>("", "payload_bytes")
                .map_err(|_| ManagementError::InvalidEvidence)?,
        )
        .map_err(|_| ManagementError::InvalidEvidence)?;
        bytes += size;
        sizes.push(size);
    }
    let memory = budget.reserve(2 * bytes + 4096)?;
    budget.charge(1, count as u64, count as u64, bytes as u64)?;
    let expected = sizes
        .iter()
        .enumerate()
        .map(|(offset, size)| format!("WHEN {} THEN {size}", first + offset as i64))
        .collect::<Vec<_>>()
        .join(" ");
    // Length predicates prevent changed storage from exceeding the prepaid sum.
    let rows = db.query_all(Statement::from_sql_and_values(db.get_database_backend(),
        format!("SELECT {ordinal} AS ordinal,CASE WHEN {length}=CASE pin_management_archive_chunks.ordinal {expected} END THEN payload END AS payload FROM pin_management_archive_chunks WHERE archive_id=$1 AND ordinal>=$2 AND ordinal<$3 ORDER BY pin_management_archive_chunks.ordinal LIMIT {count}"),
        [archive.into(), first.into(), (first+count).into()])).await.map_err(|_| ManagementError::Database)?;
    if rows.len() != count as usize {
        return Err(ManagementError::InvalidEvidence);
    }
    let mut chunks = Vec::with_capacity(rows.len());
    for (offset, row) in rows.into_iter().enumerate() {
        let ordinal: i64 = row
            .try_get("", "ordinal")
            .map_err(|_| ManagementError::InvalidEvidence)?;
        if ordinal != first + offset as i64 {
            return Err(ManagementError::InvalidEvidence);
        }
        let chunk: Vec<u8> = row
            .try_get("", "payload")
            .map_err(|_| ManagementError::InvalidEvidence)?;
        if chunk.len() != sizes[offset] {
            return Err(ManagementError::InvalidEvidence);
        }
        chunks.push(chunk);
    }
    budget.check()?;
    // Only the unused scalar lookahead is released; payload and failed-read work stay.
    budget.work.rows -= 1;
    budget.work.cells -= 2;
    Ok(Page {
        chunks,
        _memory: memory,
    })
}

pub(super) async fn check_end<C: ConnectionTrait>(
    db: &C,
    reference: &ArchiveRef,
    budget: &mut Budget,
) -> Result<()> {
    budget.charge(1, 1, 0, 0)?;
    let extra = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT 1 FROM pin_management_archive_chunks WHERE archive_id=$1 AND ordinal<0 UNION ALL SELECT 1 FROM pin_management_archive_chunks WHERE archive_id=$1 AND ordinal>=$2 UNION ALL SELECT 1 FROM pin_management_archive_chunks WHERE archive_id=$1 AND ordinal IS NULL LIMIT 1",
        [reference.archive_id.clone().into(), reference.chunks.into()])).await.map_err(|_| ManagementError::Database)?;
    if extra.is_some() {
        return Err(ManagementError::InvalidEvidence);
    }
    budget.check()?;
    budget.work.rows -= 1;
    Ok(())
}
#[cfg(test)]
mod tests;
impl Pages {
    pub(crate) async fn open<C: ConnectionTrait>(
        db: &C,
        reference: &ArchiveRef,
        budget: &mut Budget,
    ) -> Result<(header::DecodedHeader, Self)> {
        if reference.archive_id.is_empty()
            || reference.archive_id.len() > 1024
            || reference.bytes < 0
            || reference.chunks < 0
            || reference.digest.len() != 64
            || !reference
                .digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(ManagementError::InvalidEvidence);
        }
        let memory = budget.reserve(3 * PAGE_BYTES + CELL_SLICE)?;
        let mut payload = Vec::new();
        // Inline reads are bounded to one cell; the ordinary reader verifies its
        // storage digest before this buffer is exposed.
        if let Some(raw) = inline::read(db, reference, &mut payload, budget).await? {
            budget.prepay_reader(0, payload.len() as u64)?;
            let header = header::DecodedHeader::decode(&raw, budget)?;
            return Ok((
                header,
                Self {
                    reference: reference.clone(),
                    digest: Sha256::new(),
                    ordinal: 0,
                    bytes: reference.bytes,
                    inline: Some(payload),
                    inline_done: false,
                    done: false,
                    _memory: memory,
                },
            ));
        }
        budget.charge(1, 1, 0, 0)?;
        let length = if db.get_database_backend() == sea_orm::DatabaseBackend::Sqlite {
            "length(CAST(header AS BLOB))"
        } else {
            "CAST(OCTET_LENGTH(header) AS BIGINT)"
        };
        let row = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
            format!("SELECT CASE WHEN {length}<=1048576 THEN header END AS header,format,state,chunks,bytes,digest FROM pin_management_archive WHERE archive_id=$1"),
            [reference.archive_id.clone().into()])).await.map_err(|_| ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
        let raw: String = row
            .try_get("", "header")
            .map_err(|_| ManagementError::Budget)?;
        budget.charge(0, 0, 1, raw.len() as u64)?;
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
        drop(row);
        let header = header::DecodedHeader::decode(&raw, budget)?;
        let mut digest = Sha256::new_with_prefix(b"S75A1\0");
        digest.update((raw.len() as u64).to_be_bytes());
        digest.update(raw.as_bytes());
        Ok((
            header,
            Self {
                reference: reference.clone(),
                digest,
                ordinal: 0,
                bytes: 0,
                inline: None,
                inline_done: false,
                done: false,
                _memory: memory,
            },
        ))
    }
    pub(crate) async fn next<C: ConnectionTrait>(
        &mut self,
        db: &C,
        budget: &mut Budget,
    ) -> Result<Option<Page>> {
        budget.check()?;
        if self.done {
            return Ok(None);
        }
        if let Some(payload) = self.inline.take() {
            self.inline_done = true;
            return Ok(Some(Page {
                chunks: vec![payload],
                _memory: budget.reserve(CELL_SLICE)?,
            }));
        }
        if self.inline_done {
            self.done = true;
            return Ok(None);
        }
        if self.ordinal == self.reference.chunks {
            check_end(db, &self.reference, budget).await?;
            if self.bytes != self.reference.bytes
                || hex::encode(self.digest.clone().finalize()) != self.reference.digest
            {
                return Err(ManagementError::InvalidEvidence);
            }
            budget.check()?;
            self.done = true;
            return Ok(None);
        }
        let count = (self.reference.chunks - self.ordinal).min((PAGE_BYTES / CELL_SLICE) as i64);
        let page = read_page(db, &self.reference.archive_id, self.ordinal, count, budget).await?;
        for chunk in &page.chunks {
            if self
                .bytes
                .checked_add(chunk.len() as i64)
                .is_none_or(|n| n > self.reference.bytes)
            {
                return Err(ManagementError::InvalidEvidence);
            }
            self.digest.update((self.ordinal as u64).to_be_bytes());
            self.digest.update((chunk.len() as u64).to_be_bytes());
            self.digest.update(chunk);
            self.bytes += chunk.len() as i64;
            self.ordinal += 1;
        }
        budget.check()?;
        Ok(Some(page))
    }
}
