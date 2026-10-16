//! Borrowed table membership at an item boundary. Outputs include tombstones.
use super::*;
use facts::Key;
use records::RowView;

#[derive(Clone, Copy)]
pub(super) enum RowKey<'a> {
    Stored(&'a Key),
    Native(&'a native::NativeTable, RowView<'a>),
}
impl<'a> RowKey<'a> {
    pub fn table(self) -> &'a str {
        match self {
            Self::Stored(key) => &key.0,
            Self::Native(table, _) => &table.table,
        }
    }
    pub fn value(self, index: usize) -> &'a str {
        match self {
            Self::Stored(key) => &key.1[index],
            Self::Native(table, row) => row
                .cell(&table.primary_key[index])
                .expect("validated primary key"),
        }
    }
    pub fn matches(self, key: &Key) -> bool {
        self.table() == key.0
            && match self {
                Self::Stored(original) => original == key,
                Self::Native(table, row) => {
                    table.primary_key.len() == key.1.len()
                        && table
                            .primary_key
                            .iter()
                            .zip(&key.1)
                            .all(|(column, value)| row.cell(column) == Some(value))
                }
            }
    }
    pub fn copy_out(self, binding: records::BindingView<'_>) -> Result<Key> {
        match self {
            Self::Stored(key) => {
                binding.copy(key)?;
                Ok(key.clone())
            }
            Self::Native(table, row) => {
                binding.copy(&table.table)?;
                binding.with_decode_work(|work| work.nodes(2))?;
                for column in &table.primary_key {
                    binding.copy(&cell(row, column)?)?;
                }
                Ok(Key(
                    table.table.clone(),
                    table
                        .primary_key
                        .iter()
                        .map(|column| cell(row, column).map(str::to_owned))
                        .collect::<Result<_>>()?,
                ))
            }
        }
    }
}

pub(super) trait RowSet {
    fn iter(&self) -> impl Iterator<Item = (RowKey<'_>, RowView<'_>)>;
    fn get(&self, key: &Key) -> Option<RowView<'_>>;
    fn contains_key(&self, key: &Key) -> bool {
        self.get(key).is_some()
    }
}
impl RowSet for capture::Rows {
    fn iter(&self) -> impl Iterator<Item = (RowKey<'_>, RowView<'_>)> {
        self.iter()
            .map(|(key, row)| (RowKey::Stored(key), RowView::Native(row)))
    }
    fn get(&self, key: &Key) -> Option<RowView<'_>> {
        self.get(key).map(RowView::Native)
    }
}

pub(super) struct CapturedRows<'a, T> {
    pub before: &'a T,
    pub captured: &'a capture::Rows,
}
impl<T: RowSet> RowSet for CapturedRows<'_, T> {
    fn iter(&self) -> impl Iterator<Item = (RowKey<'_>, RowView<'_>)> {
        self.before
            .iter()
            .filter(|(key, _)| !self.captured.keys().any(|candidate| key.matches(candidate)))
            .chain(
                self.captured
                    .iter()
                    .map(|(key, row)| (RowKey::Stored(key), RowView::Native(row))),
            )
    }
    fn get(&self, key: &Key) -> Option<RowView<'_>> {
        self.captured
            .get(key)
            .map(RowView::Native)
            .or_else(|| self.before.get(key))
    }
}

pub(super) struct LatestRows<'a> {
    source: records::FrozenRows<'a>,
    cursor: &'a Cursor,
    output: Option<&'a OperationRead>,
}
impl<'a> LatestRows<'a> {
    pub fn before(binding: records::BindingView<'a>, cursor: &'a Cursor) -> Result<Self> {
        Ok(Self {
            source: binding.frozen_rows()?,
            cursor,
            output: None,
        })
    }
    pub fn after(&self, output: &'a OperationRead) -> Self {
        Self {
            output: Some(output),
            ..*self
        }
    }
    fn replaced(&self, key: RowKey<'_>) -> bool {
        self.output
            .is_some_and(|op| op.facts.2.iter().any(|out| key.matches(&out.0)))
    }
}
impl RowSet for LatestRows<'_> {
    fn iter(&self) -> impl Iterator<Item = (RowKey<'_>, RowView<'_>)> {
        let original = self
            .source
            .all_rows()
            .map(|(table, row)| (RowKey::Native(table, row), row))
            .filter(|(key, _)| {
                !self
                    .cursor
                    .entries()
                    .any(|(changed, _)| key.matches(changed))
                    && !self.replaced(*key)
            });
        let latest = self
            .cursor
            .entries()
            .filter(|(key, _)| !self.replaced(RowKey::Stored(key)))
            .filter_map(|(key, row)| row.map(|row| (RowKey::Stored(key), row)));
        let outputs = self
            .output
            .into_iter()
            .flat_map(|op| op.output_rows())
            .filter_map(|(key, row)| row.map(|row| (RowKey::Stored(key), row)));
        original.chain(latest).chain(outputs)
    }
    fn get(&self, key: &Key) -> Option<RowView<'_>> {
        if let Some(op) = self.output
            && let Some((_, row)) = op.output_rows().find(|(candidate, _)| *candidate == key)
        {
            return row;
        }
        if let Some((_, row)) = self
            .cursor
            .entries()
            .find(|(candidate, _)| *candidate == key)
        {
            return row;
        }
        self.source.row_key(&key.0, &key.1)
    }
}
