//! Exact comparisons for untrusted resume and shared source originals.
use super::{
    spool::{get, one},
    *,
};
use sea_orm::TransactionTrait;
mod shared;
pub(super) use shared::restore as restore_shared;

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct VerificationCursor {
    stage: usize,
    raw: ArchiveCursor,
    membership: MembershipCursor,
    after: String,
    second: String,
    rows: u64,
    bytes: u64,
    live: Option<RetirementInventoryRef>,
    pub(super) source: Option<SourceCheckpoint>,
    pub(super) source_ordinal: i64,
    pub(super) source_position: i64,
}
impl InventorySpool {
    /// Proves two provider reducers consumed precisely the same source originals
    /// before their bounded evidence roots share one canonical plan archive.
    pub async fn verify_shared_source(
        &mut self,
        primary: &InventorySpool,
        limits: &RunLimits,
    ) -> Result<Progress> {
        if self.shared_route.is_some() && self.shared_source.as_ref() == Some(primary.reference()) {
            return Ok(Progress::Reduced);
        }
        let a = self.checkpoint().await?;
        let b = primary.checkpoint().await?;
        if !a.complete()
            || !b.complete()
            || a.database_scope != b.database_scope
            || a.database_identity != b.database_identity
            || a.catalog != b.catalog
            || a.revisions != b.revisions
        {
            return Err(ManagementError::Changed);
        }
        if self
            .shared_verification
            .live
            .as_ref()
            .is_some_and(|id| id != primary.reference())
        {
            return Err(ManagementError::Changed);
        }
        self.shared_verification.live = Some(primary.reference().clone());
        let mut run = super::spool::task_run(&self.db, limits).await?;
        loop {
            if self.shared_verification.stage == 2 {
                run.check()?;
                self.shared_source = Some(primary.reference().clone());
                return Ok(Progress::Reduced);
            }
            if !run.page(
                self.shared_verification.rows,
                self.shared_verification.bytes,
            )? {
                return Ok(Progress::Pending);
            }
            let started = std::time::Instant::now();
            let mut cursor = self.shared_verification.clone();
            let (mut rows, mut bytes) = (0u64, 0u64);
            match cursor.stage {
                0 => {
                    let original = shared::parts(&self.db, cursor.raw.ordinal).await?;
                    let common = shared::parts(&primary.db, cursor.raw.ordinal).await?;
                    if original != common {
                        return Err(ManagementError::Changed);
                    }
                    if let Some((last, _)) = original.last() {
                        cursor.raw.ordinal = *last;
                        bytes = original.iter().try_fold(0u64, |sum, (_, part)| {
                            Ok::<_, ManagementError>(
                                sum + (encode(&part.key)?.len()
                                    + part.table.len()
                                    + part.column.len()
                                    + part.bytes.len())
                                    as u64
                                    * 2,
                            )
                        })?;
                    } else {
                        cursor.stage = 1;
                    }
                }
                1 => {
                    let original = shared::members(&self.db, cursor.source_position).await?;
                    let common = shared::members(&primary.db, cursor.source_position).await?;
                    if original != common {
                        return Err(ManagementError::Changed);
                    }
                    if let Some((last, _)) = original.last() {
                        cursor.source_position = *last;
                        rows = original.len() as u64;
                        bytes = original.iter().try_fold(0u64, |sum, (_, key)| {
                            encode(key).map(|key| sum + key.len() as u64 * 2)
                        })?;
                    } else {
                        shared::eof(&self.db).await?;
                        shared::eof(&primary.db).await?;
                        cursor.stage = 2;
                    }
                }
                _ => {
                    return Err(ManagementError::InvalidEvidence);
                }
            }
            cursor.rows = cursor
                .rows
                .checked_add(rows)
                .ok_or(ManagementError::Budget)?;
            cursor.bytes = cursor
                .bytes
                .checked_add(bytes)
                .ok_or(ManagementError::Budget)?;
            let tx = self
                .db
                .begin()
                .await
                .map_err(|_| ManagementError::Database)?;
            super::spool::budget::charge(&tx, &run, rows, bytes).await?;
            shared::save(&tx, &cursor).await?;
            super::spool::budget::timed(&tx, "shared_source", started.elapsed()).await?;
            run.check()?;
            tx.commit().await.map_err(|_| ManagementError::Database)?;
            self.shared_verification = cursor;
        }
    }
    /// `live` is scanned from the original checkpoint's rewind and reduced with
    /// its frozen configuration. Full bytes/membership/derivations, not a digest
    /// or a file-supplied completion marker, establish resumable local evidence.
    /// The caller still fences the current source vector before publication.
    pub async fn verify_resume_against(
        &mut self,
        live: &InventorySpool,
        limits: &RunLimits,
    ) -> Result<Progress> {
        if !live.source_scanned
            || !live.reduced_here.load(std::sync::atomic::Ordering::Acquire)
            || self.checkpoint().await? != live.checkpoint().await?
        {
            return Err(ManagementError::InvalidEvidence);
        }
        if self
            .verification
            .live
            .as_ref()
            .is_some_and(|id| id != live.reference())
        {
            return Err(ManagementError::Changed);
        }
        self.verification.live = Some(live.reference().clone());
        let mut run = super::spool::task_run(&self.db, limits).await?;
        loop {
            if !run.page(self.verification.rows, self.verification.bytes)? {
                return Ok(Progress::Pending);
            }
            match self.verification.stage {
                0 => {
                    let mut left = self.verification.raw.clone();
                    let mut right = left.clone();
                    let original = self.read_part(&mut left).await?;
                    let current = live.read_part(&mut right).await?;
                    if original != current {
                        return Err(ManagementError::Changed);
                    }
                    if let Some(part) = original {
                        self.verification.bytes = self
                            .verification
                            .bytes
                            .checked_add(part.bytes.len() as u64)
                            .ok_or(ManagementError::Budget)?;
                        self.verification.raw = left;
                    } else {
                        self.verification.stage += 1;
                    }
                }
                1 => {
                    let mut left = self.verification.membership.clone();
                    let mut right = left.clone();
                    let original = self.read_membership(&mut left).await?;
                    let current = live.read_membership(&mut right).await?;
                    if original != current {
                        return Err(ManagementError::Changed);
                    }
                    if let Some(InventoryMembership::Member(key)) = &original {
                        let row = one(
                            &self.db,
                            "SELECT sort_key FROM wire_keys WHERE table_name=$1 AND row_key=$2",
                            vec![key.table.clone().into(), encode(key)?.into()],
                        )
                        .await?
                        .ok_or(ManagementError::InvalidEvidence)?;
                        if get::<Vec<u8>>(&row, "sort_key")? != super::spool::wire_key(key) {
                            return Err(ManagementError::InvalidEvidence);
                        }
                    }
                    if original.is_some() {
                        self.verification.membership = left;
                        self.verification.rows += 1;
                    } else {
                        self.verification.stage += 1;
                    }
                }
                2 => {
                    let sql = "SELECT kind,identity,payload FROM facts WHERE (kind,identity)>($1,$2) ORDER BY kind,identity LIMIT 1";
                    let args = vec![
                        self.verification.after.clone().into(),
                        self.verification.second.clone().into(),
                    ];
                    let left = one(&self.db, sql, args.clone()).await?;
                    let right = one(&live.db, sql, args).await?;
                    let project = |row: sea_orm::QueryResult| -> Result<(String, String, String)> {
                        Ok((
                            get(&row, "kind")?,
                            get(&row, "identity")?,
                            get(&row, "payload")?,
                        ))
                    };
                    let left = left.map(project).transpose()?;
                    let right = right.map(project).transpose()?;
                    if left != right {
                        return Err(ManagementError::Changed);
                    }
                    if let Some((kind, id, payload)) = left {
                        self.verification.after = kind;
                        self.verification.second = id;
                        self.verification.rows += 1;
                        self.verification.bytes += payload.len() as u64;
                    } else {
                        self.verification.stage += 1;
                        self.verification.after.clear();
                        self.verification.second.clear();
                    }
                }
                3..=6 => {
                    let (table, key, columns) = match self.verification.stage {
                        3 => (
                            "quotes",
                            "identity",
                            &[
                                "identity",
                                "remote_key",
                                "cid",
                                "bytes",
                                "unknown",
                                "inflight",
                                "cleanup",
                                "retained",
                            ][..],
                        ),
                        4 => (
                            "target_edges",
                            "target_id",
                            &[
                                "target_id",
                                "lease_id",
                                "cid",
                                "allocation",
                                "logical_bytes",
                                "active",
                                "referenced",
                            ][..],
                        ),
                        5 => (
                            "jobs",
                            "identity",
                            &["identity", "allocation", "cid", "inflight", "cleanup"][..],
                        ),
                        _ => ("cids", "cid", &["cid"][..]),
                    };
                    let projection = columns
                        .iter()
                        .map(|c| format!("CAST({c} AS TEXT) AS {c}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    let sql = format!(
                        "SELECT {projection} FROM {table} WHERE {key}>$1 ORDER BY {key} LIMIT 1"
                    );
                    let args = vec![self.verification.after.clone().into()];
                    let left = one(&self.db, &sql, args.clone()).await?;
                    let right = one(&live.db, &sql, args).await?;
                    let project = |row: sea_orm::QueryResult| -> Result<Vec<String>> {
                        columns.iter().map(|c| get(&row, c)).collect()
                    };
                    let left = left.map(project).transpose()?;
                    let right = right.map(project).transpose()?;
                    if left != right {
                        return Err(ManagementError::Changed);
                    }
                    if let Some(values) = left {
                        self.verification.after = values[0].clone();
                        self.verification.rows += 1;
                        self.verification.bytes +=
                            values.iter().map(|s| s.len() as u64).sum::<u64>();
                    } else {
                        self.verification.stage += 1;
                        self.verification.after.clear();
                    }
                }
                _ => {
                    run.check()?;
                    self.source_scanned = true;
                    self.reduced_here
                        .store(true, std::sync::atomic::Ordering::Release);
                    return Ok(Progress::Reduced);
                }
            }
        }
    }
}
