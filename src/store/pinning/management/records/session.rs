//! Operation-local raw and typed arenas with one cumulative allocation ledger.
use super::*;
use std::{collections::BTreeSet, sync::Arc};

#[derive(Default)]
pub struct ReadSession {
    failed: bool,
    pub(super) frozen_sources: Vec<evidence::FrozenSourceIndex>,
    pub(super) bytes: usize,
    pub(super) nodes: usize,
    pub(super) authority_path: BTreeSet<native::NativeKey>,
    pub(super) predecessor_depth: usize,
    pub(super) archive: archive::ReadBatch,
    pub(super) results: Vec<(Arc<native::NativeRow>, Arc<results::DecodedResult>)>,
    pub(super) prefixes: Vec<results::ValidatedPrefix>,
    typed: Vec<(
        String,
        std::any::TypeId,
        Arc<dyn std::any::Any + Send + Sync>,
    )>,
}

impl ReadSession {
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
        Ok(())
    }
    pub(crate) fn check(&self) -> Result<()> {
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
