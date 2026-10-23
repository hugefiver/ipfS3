//! Bounded asynchronous storage reads; consumers synchronously borrow each page.
use super::*;
use native::meter::ResidentGuard;

pub(crate) struct Pages {
    reference: ArchiveRef,
    pub(crate) header: ArchiveHeader,
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
impl Pages {
    pub(crate) async fn open<C: ConnectionTrait>(db: &C, reference: &ArchiveRef, budget: &mut Budget) -> Result<Self> {
        if reference.archive_id.is_empty() || reference.archive_id.len() > 1024
            || reference.bytes < 0 || reference.chunks < 0 || reference.digest.len() != 64
            || !reference.digest.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
            return Err(ManagementError::InvalidEvidence);
        }
        let memory = budget.reserve(3 * PAGE_BYTES + CELL_SLICE)?;
        let mut payload = Vec::new();
        // Inline reads are bounded to one cell; the ordinary reader verifies its
        // storage digest before this buffer is exposed.
        if let Some(raw) = inline::read(db, reference, &mut payload, budget).await? {
            budget.prepay_reader(0, payload.len() as u64)?;
            let header = serde_json::from_str(&raw).map_err(|_| ManagementError::InvalidEvidence)?;
            return Ok(Self { reference: reference.clone(), header, digest: Sha256::new(), ordinal: 0, bytes: reference.bytes,
                inline: Some(payload), inline_done: false, done: false, _memory: memory });
        }
        budget.charge(1, 1, 0, 0)?;
        let length = if db.get_database_backend() == sea_orm::DatabaseBackend::Sqlite {
            "length(CAST(header AS BLOB))"
        } else { "CAST(OCTET_LENGTH(header) AS BIGINT)" };
        let row = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
            format!("SELECT CASE WHEN {length}<=1048576 THEN header END AS header,format,state,chunks,bytes,digest FROM pin_management_archive WHERE archive_id=$1"),
            [reference.archive_id.clone().into()])).await.map_err(|_| ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
        let raw: String = row.try_get("", "header").map_err(|_| ManagementError::Budget)?;
        budget.charge(0, 0, 1, raw.len() as u64)?;
        if row.try_get::<String>("", "format").map_err(|_| ManagementError::InvalidEvidence)? != "S75A1"
            || row.try_get::<String>("", "state").map_err(|_| ManagementError::InvalidEvidence)? != "sealed"
            || row.try_get::<i64>("", "chunks").map_err(|_| ManagementError::InvalidEvidence)? != reference.chunks
            || row.try_get::<i64>("", "bytes").map_err(|_| ManagementError::InvalidEvidence)? != reference.bytes
            || row.try_get::<String>("", "digest").map_err(|_| ManagementError::InvalidEvidence)? != reference.digest {
            return Err(ManagementError::InvalidEvidence);
        }
        let header = serde_json::from_str(&raw).map_err(|_| ManagementError::InvalidEvidence)?;
        let mut digest = Sha256::new_with_prefix(b"S75A1\0");
        digest.update((raw.len() as u64).to_be_bytes()); digest.update(raw.as_bytes());
        Ok(Self { reference: reference.clone(), header, digest, ordinal: 0, bytes: 0,
            inline: None, inline_done: false, done: false, _memory: memory })
    }
    pub(crate) async fn next<C: ConnectionTrait>(&mut self, db: &C, budget: &mut Budget) -> Result<Option<Page>> {
        budget.check()?;
        if self.done { return Ok(None); }
        if let Some(payload) = self.inline.take() {
            self.inline_done = true;
            return Ok(Some(Page { chunks: vec![payload], _memory: budget.reserve(CELL_SLICE)? }));
        }
        if self.inline_done { self.done = true; return Ok(None); }
        if self.ordinal == self.reference.chunks {
            budget.charge(1, 0, 0, 0)?;
            let extra = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
                "SELECT 1 FROM pin_management_archive_chunks WHERE archive_id=$1 AND (ordinal<0 OR ordinal>=$2) LIMIT 1",
                [self.reference.archive_id.clone().into(), self.reference.chunks.into()])).await.map_err(|_| ManagementError::Database)?;
            if extra.is_some() || self.bytes != self.reference.bytes
                || hex::encode(self.digest.clone().finalize()) != self.reference.digest {
                return Err(ManagementError::InvalidEvidence);
            }
            budget.check()?; self.done = true; return Ok(None);
        }
        let count = (self.reference.chunks - self.ordinal).min((PAGE_BYTES / CELL_SLICE) as i64);
        // The query caps each physical cell and the row count before decoding.
        let memory = budget.reserve(2 * PAGE_BYTES + 4096)?;
        budget.charge(1, count as u64, count as u64, 0)?;
        let rows = db.query_all(Statement::from_sql_and_values(db.get_database_backend(),
            "SELECT ordinal,CASE WHEN length(payload) BETWEEN 1 AND 65536 THEN payload END AS payload FROM pin_management_archive_chunks WHERE archive_id=$1 AND ordinal>=$2 AND ordinal<$3 ORDER BY ordinal LIMIT 16",
            [self.reference.archive_id.clone().into(), self.ordinal.into(), (self.ordinal+count).into()])).await.map_err(|_| ManagementError::Database)?;
        if rows.len() != count as usize { return Err(ManagementError::InvalidEvidence); }
        let mut chunks = Vec::with_capacity(rows.len());
        for row in rows {
            let ordinal: i64 = row.try_get("", "ordinal").map_err(|_| ManagementError::InvalidEvidence)?;
            let chunk: Vec<u8> = row.try_get("", "payload").map_err(|_| ManagementError::InvalidEvidence)?;
            budget.charge(0, 0, 0, chunk.len() as u64)?;
            if ordinal != self.ordinal || chunk.is_empty() || chunk.len() > CELL_SLICE
                || self.bytes.checked_add(chunk.len() as i64).is_none_or(|n| n > self.reference.bytes) {
                return Err(ManagementError::InvalidEvidence);
            }
            self.digest.update((ordinal as u64).to_be_bytes());
            self.digest.update((chunk.len() as u64).to_be_bytes()); self.digest.update(&chunk);
            self.bytes += chunk.len() as i64; self.ordinal += 1; chunks.push(chunk);
        }
        budget.work.pages = budget.work.pages.checked_add(1).ok_or(ManagementError::Budget)?;
        budget.check()?;
        Ok(Some(Page { chunks, _memory: memory }))
    }
}
