//! Operation-local raw and typed arenas with one cumulative allocation ledger.
use super::*;
use std::{collections::BTreeSet, sync::Arc};

#[derive(Default)]
pub struct ReadSession {
    storage: native::meter::Budget,
    task_limit: Option<(u64, u64)>,
    task_cancel: Option<super::super::transaction::CancellationToken>,
    failed: bool,
    pub(super) frozen_sources: Vec<evidence::FrozenSourceIndex>,
    pub(super) bytes: usize,
    pub(super) nodes: usize,
    pub(super) authority_path: BTreeSet<native::NativeKey>,
    pub(super) predecessor_depth: usize,
    pub(super) archive: archive::ReadBatch,
    pub(super) results: Vec<(Arc<native::NativeRow>, Arc<results::DecodedResult>)>,
    pub(super) prefixes: Vec<results::ValidatedPrefix>,
    pub(super) retirement_reviews: Vec<inventory_review::BoundInventory>,
    typed: Vec<(
        String,
        std::any::TypeId,
        Arc<dyn std::any::Any + Send + Sync>,
    )>,
}

impl ReadSession {
    pub(crate) fn for_task(
        storage: native::meter::Budget,
        rows: u64,
        bytes: u64,
        cancel: super::super::transaction::CancellationToken,
    ) -> Self {
        Self {
            storage,
            task_limit: Some((rows, bytes)),
            task_cancel: Some(cancel),
            ..Self::default()
        }
    }
    pub(crate) fn task_usage(&self) -> Result<native::meter::WorkMeter> {
        let mut work = self.storage_usage();
        work.rows = work
            .rows
            .checked_add(self.nodes as u64)
            .ok_or(ManagementError::Budget)?;
        work.raw_bytes = work
            .raw_bytes
            .checked_add(work.copied_bytes)
            .and_then(|bytes| bytes.checked_add(work.output_bytes))
            .ok_or(ManagementError::Budget)?;
        work.raw_bytes = work
            .raw_bytes
            .checked_add(self.bytes as u64)
            .ok_or(ManagementError::Budget)?;
        Ok(work)
    }
    pub(crate) fn storage_budget(&self) -> Result<native::meter::Budget> {
        self.check()?;
        let mut budget = self.storage.remaining_budget()?;
        if let Some((rows, bytes)) = self.task_limit {
            let used = self.task_usage()?;
            budget.restrict_remaining(
                rows.checked_sub(used.rows).ok_or(ManagementError::Budget)?,
                bytes
                    .checked_sub(used.raw_bytes)
                    .ok_or(ManagementError::Budget)?,
                self.storage.remaining()?,
            )?;
            budget.restrict_work_bytes(
                bytes
                    .checked_sub(used.raw_bytes)
                    .ok_or(ManagementError::Budget)?,
            )?;
        }
        Ok(budget)
    }
    pub(crate) fn shared<T: Send + Sync + 'static>(
        &mut self,
        input: &str,
        decode: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<Arc<T>> {
        self.check()?;
        if let Some((_, _, value)) = self
            .typed
            .iter()
            .find(|(raw, kind, _)| raw == input && *kind == std::any::TypeId::of::<T>())
        {
            return value
                .clone()
                .downcast::<T>()
                .map_err(|_| ManagementError::InvalidEvidence);
        }
        let value = match decode(self) {
            Err(ManagementError::Budget) => {
                self.failed = true;
                return Err(ManagementError::Budget);
            }
            result => result?,
        };
        self.nodes(3)?;
        self.bytes(input.len())?;
        let value = Arc::new(value);
        self.typed
            .push((input.to_owned(), std::any::TypeId::of::<T>(), value.clone()));
        Ok(value)
    }
    pub(crate) fn typed_shared<T: serde::de::DeserializeOwned + Send + Sync + 'static>(
        &mut self,
        input: &str,
    ) -> Result<Arc<T>> {
        self.shared(input, |work| work.typed(input))
    }
    pub(crate) fn charge_json_scan(&mut self, input: &str) -> Result<()> {
        let result = codec::charge_json_scan(input, self);
        if result == Err(ManagementError::Budget) {
            self.failed = true;
        }
        result
    }
    pub(crate) fn charge_json_owned(&mut self, input: &str) -> Result<()> {
        let result = codec::charge_json_owned(input, self);
        if result == Err(ManagementError::Budget) {
            self.failed = true;
        }
        result
    }
    pub fn usage(&self) -> (usize, usize) {
        (self.nodes, self.bytes)
    }
    pub fn storage_usage(&self) -> native::meter::WorkMeter {
        self.storage.work
    }
    pub(crate) fn finish_storage(&mut self, budget: &native::meter::Budget) -> Result<()> {
        let accounted = self.charge_storage(budget.work);
        let checked = budget.check();
        self.failed |= checked.is_err();
        accounted.and(checked)
    }
    pub(crate) fn charge_storage(&mut self, used: native::meter::WorkMeter) -> Result<()> {
        let checked = self.check();
        let result = (|| {
            let add = |a: u64, b: u64| a.checked_add(b).ok_or(ManagementError::Budget);
            self.storage.work.sql = add(self.storage.work.sql, used.sql)?;
            self.storage.work.rows = add(self.storage.work.rows, used.rows)?;
            self.storage.work.cells = add(self.storage.work.cells, used.cells)?;
            self.storage.work.raw_bytes = add(self.storage.work.raw_bytes, used.raw_bytes)?;
            self.storage.work.copied_bytes = self
                .storage
                .work
                .copied_bytes
                .checked_add(used.copied_bytes)
                .ok_or(ManagementError::Budget)?;
            self.storage.work.output_bytes = self
                .storage
                .work
                .output_bytes
                .checked_add(used.output_bytes)
                .ok_or(ManagementError::Budget)?;
            self.storage.work.pages = self
                .storage
                .work
                .pages
                .checked_add(used.pages)
                .ok_or(ManagementError::Budget)?;
            let limit = native::meter::Limits::default().bytes;
            if self.storage.work.copied_bytes > limit || self.storage.work.output_bytes > limit {
                return Err(ManagementError::Budget);
            }
            checked?;
            self.storage.charge(0, 0, 0, 0)?;
            self.check()
        })();
        self.failed |= result.is_err();
        result
    }
    #[cfg(test)]
    pub(crate) fn consume(&mut self, nodes: usize, bytes: usize) -> Result<()> {
        self.nodes(nodes)?;
        self.bytes(bytes)
    }
    #[track_caller]
    pub(crate) fn typed<T: serde::de::DeserializeOwned>(&mut self, input: &str) -> Result<T> {
        let result = codec::decode_json(input, self);
        if matches!(&result, Err(ManagementError::Budget)) {
            self.failed = true;
        }
        result
    }
    #[track_caller]
    pub(crate) fn nodes(&mut self, amount: usize) -> Result<()> {
        self.check()?;
        self.nodes = match self.nodes.checked_add(amount) {
            Some(value) => value,
            None => {
                self.failed = true;
                return Err(ManagementError::Budget);
            }
        };
        if self.nodes > crate::pinning::management::wire::MAX_WIRE_NODES {
            self.failed = true;
            return Err(ManagementError::Budget);
        }
        self.check()
    }
    pub(crate) fn check(&self) -> Result<()> {
        self.storage.check()?;
        if self
            .task_cancel
            .as_ref()
            .is_some_and(|cancel| cancel.is_cancelled())
        {
            return Err(ManagementError::Budget);
        }
        if let Some((rows, bytes)) = self.task_limit {
            let used = self.task_usage()?;
            if used.rows > rows || used.raw_bytes > bytes {
                return Err(ManagementError::Budget);
            }
        }
        if self.failed
            || self.nodes > crate::pinning::management::wire::MAX_WIRE_NODES
            || self.bytes > crate::pinning::management::wire::MAX_WIRE_BYTES
        {
            return Err(ManagementError::Budget);
        }
        Ok(())
    }
    #[track_caller]
    pub(crate) fn copy<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let (nodes, bytes) = codec::measure(value)?;
        self.nodes(nodes)?;
        self.bytes(bytes)
    }
    pub(super) fn index_copy<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.copy(value)
    }
    pub(super) fn index_cells(&mut self, cells: usize, bytes: usize) -> Result<()> {
        self.nodes(cells.checked_add(1).ok_or(ManagementError::Budget)?)?;
        self.bytes(bytes)
    }
    pub(super) fn key(&mut self, row: &native::NativeRow) -> Result<native::NativeKey> {
        #[derive(Serialize)]
        struct Key<'a> {
            table: &'a str,
            values: [Option<&'a str>; 4],
        }
        self.index_copy(&Key {
            table: super::super::schema::RECORDS,
            values: ["database_scope", "plan_id", "item_id", "kind"]
                .map(|column| native::value(row, column)),
        })?;
        evidence::key(row)
    }
    pub(super) fn result(
        &mut self,
        row: &native::NativeRow,
    ) -> Result<Arc<results::DecodedResult>> {
        self.check()?;
        let raw = native::value(row, "result").ok_or(ManagementError::InvalidEvidence)?;
        if let Some((_, parsed)) = self.results.iter().find(|(original, _)| {
            [
                "database_scope",
                "native_identity",
                "principal",
                "plan_id",
                "item_id",
                "kind",
                "result",
            ]
            .iter()
            .all(|column| original.get(*column) == row.get(*column))
        }) {
            return Ok(parsed.clone());
        }
        self.nodes(2)?;
        let result = Arc::new(results::decode_facts(
            raw.strip_prefix("S7R1\n")
                .ok_or(ManagementError::InvalidEvidence)?,
            self,
        )?);
        let row = archive::retain_row_ref(row, self)?;
        self.results.push((row, result.clone()));
        Ok(result)
    }
    #[track_caller]
    pub(crate) fn bytes(&mut self, amount: usize) -> Result<()> {
        self.check()?;
        self.bytes = match self.bytes.checked_add(amount) {
            Some(value) => value,
            None => {
                self.failed = true;
                return Err(ManagementError::Budget);
            }
        };
        if self.bytes > crate::pinning::management::wire::MAX_WIRE_BYTES {
            self.failed = true;
            return Err(ManagementError::Budget);
        }
        self.check()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn task_storage_and_decode_share_remaining_allowance_without_expanding_defaults() {
        let mut session =
            ReadSession::for_task(native::meter::Budget::default(), 5, 100, Default::default());
        session.index_cells(1, 40).unwrap();
        let mut read = session.storage_budget().unwrap();
        assert_eq!(read.charge(1, 4, 0, 1), Err(ManagementError::Budget));
        assert_eq!(
            session.charge_storage(read.work),
            Err(ManagementError::Budget)
        );
        assert_eq!(session.task_usage().unwrap().rows, 6);
        let session = ReadSession::for_task(
            native::meter::Budget::default(),
            u64::MAX,
            u64::MAX,
            Default::default(),
        );
        let mut read = session.storage_budget().unwrap();
        assert_eq!(
            read.charge(0, native::meter::Limits::default().rows + 1, 0, 0),
            Err(ManagementError::Budget)
        );
        let mut read = session.storage_budget().unwrap();
        assert!(matches!(
            read.reserve(native::meter::Limits::default().resident + 1),
            Err(ManagementError::Budget)
        ));
    }

    #[test]
    fn borrowed_storage_keeps_operation_cancellation() {
        let storage = native::meter::Budget::default();
        let cancel = storage.cancellation();
        let session = ReadSession::for_task(storage, 100, 1000, Default::default());
        let read = session.storage_budget().unwrap();
        cancel.store(true, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(read.check(), Err(ManagementError::Budget));
        assert_eq!(session.check(), Err(ManagementError::Budget));
    }
    #[test]
    fn storage_work_limit_failure_cannot_reuse_cached_data() {
        let mut work = ReadSession::default();
        work.typed_shared::<Vec<u8>>("[]").unwrap();
        let limit = native::meter::Limits::default().sql;
        work.charge_storage(native::meter::WorkMeter {
            sql: limit / 2,
            ..Default::default()
        })
        .unwrap();
        work.charge_storage(native::meter::WorkMeter {
            sql: limit - limit / 2,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(work.storage_usage().sql, limit);
        assert_eq!(
            work.charge_storage(native::meter::WorkMeter {
                sql: 1,
                ..Default::default()
            }),
            Err(ManagementError::Budget)
        );
        assert!(matches!(
            work.typed_shared::<Vec<u8>>("[]"),
            Err(ManagementError::Budget)
        ));
    }
    #[test]
    fn source_limit_failure_cannot_reuse_cached_data() {
        let mut work = ReadSession::default();
        work.typed_shared::<Vec<u8>>("[]").unwrap();
        let oversized = format!(
            "[{}0]",
            "0,".repeat(crate::pinning::management::wire::MAX_WIRE_NODES)
        );
        assert!(matches!(
            work.typed::<Vec<u8>>(&oversized),
            Err(ManagementError::Budget)
        ));
        assert!(matches!(
            work.typed_shared::<Vec<u8>>("[]"),
            Err(ManagementError::Budget)
        ));
    }
    #[test]
    fn arithmetic_failure_poisoning_cannot_be_bypassed() {
        let mut work = ReadSession::default();
        work.typed_shared::<Vec<u8>>("[]").unwrap();
        assert_eq!(work.consume(usize::MAX, 0), Err(ManagementError::Budget));
        assert!(matches!(
            work.typed_shared::<Vec<u8>>("[]"),
            Err(ManagementError::Budget)
        ));
    }
}
