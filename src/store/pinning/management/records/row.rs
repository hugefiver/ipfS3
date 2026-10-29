//! Stable row owners; views borrow original cells, including SQL NULL.
use super::*;
use crate::store::pinning::management::actions::execution::facts::ItemFacts;
use crate::store::pinning::management::schema;
use std::sync::Arc;

pub(crate) enum RowHandle {
    Sql(Arc<native::NativeRow>),
    Archive {
        table: &'static native::NativeTable,
        leaf: Arc<archive::Leaf>,
        terminals: Vec<(usize, Arc<native::NativeRow>)>,
    },
    Output {
        table: &'static native::NativeTable,
        facts: Arc<ItemFacts>,
        slot: usize,
    },
}
impl RowHandle {
    pub(crate) fn get(&self, column: &str) -> Option<&Option<String>> {
        match self {
            Self::Sql(row) => row.get(column),
            Self::Archive {
                table,
                leaf,
                terminals,
            } => {
                let slot = table
                    .columns
                    .iter()
                    .position(|candidate| candidate == column)?;
                match &leaf.2[slot] {
                    archive::Cell::Text(value) => Some(value),
                    archive::Cell::Before(..) => terminals
                        .iter()
                        .find(|(index, _)| *index == slot)?
                        .1
                        .get("native_before"),
                }
            }
            Self::Output { table, facts, slot } => facts.2[*slot].2.as_ref()?.get(
                table
                    .columns
                    .iter()
                    .position(|candidate| candidate == column)?,
            ),
        }
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &Option<String>)> {
        let (native, columns) = match self {
            Self::Sql(row) => (Some(row), None),
            Self::Archive { table, .. } | Self::Output { table, .. } => {
                (None, Some(&table.columns))
            }
        };
        native.into_iter().flat_map(|row| row.iter()).chain(
            columns
                .into_iter()
                .flatten()
                .map(|column| (column, self.get(column).expect("validated row layout"))),
        )
    }
}
impl native::RawCells for RowHandle {
    fn raw_cell(&self, column: &str) -> Option<&Option<String>> {
        self.get(column)
    }
}
impl PartialEq for RowHandle {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl Eq for RowHandle {}
impl PartialEq<native::NativeRow> for RowHandle {
    fn eq(&self, other: &native::NativeRow) -> bool {
        self.iter().eq(other.iter())
    }
}
impl PartialEq<RowHandle> for native::NativeRow {
    fn eq(&self, other: &RowHandle) -> bool {
        other == self
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RowView<'a> {
    Native(&'a native::NativeRow),
    Original {
        public: &'a native::NativeRow,
        private: &'a native::NativeRow,
    },
    Handle(&'a RowHandle),
}
impl<'a> RowView<'a> {
    pub(crate) fn get(self, column: &str) -> Option<&'a Option<String>> {
        match self {
            Self::Native(row) => row.get(column),
            Self::Original { public, private } => {
                private.get(column).or_else(|| public.get(column))
            }
            Self::Handle(row) => row.get(column),
        }
    }
    pub(crate) fn cell(self, column: &str) -> Option<&'a str> {
        self.get(column).and_then(Option::as_deref)
    }
    pub(crate) fn iter(self) -> impl Iterator<Item = (&'a String, &'a Option<String>)> {
        let (native, handle) = match self {
            Self::Native(row) | Self::Original { public: row, .. } => (Some(row), None),
            Self::Handle(row) => (None, Some(row)),
        };
        native
            .into_iter()
            .flat_map(|row| row.iter())
            .map(move |(column, _)| (column, self.get(column).expect("validated original cell")))
            .chain(handle.into_iter().flat_map(|row| row.iter()))
    }
}
impl native::RawCells for RowView<'_> {
    fn raw_cell(&self, column: &str) -> Option<&Option<String>> {
        self.get(column)
    }
}
impl<'a> native::RowValue<'a> for RowView<'a> {
    fn value(self, column: &str) -> Option<&'a str> {
        self.cell(column)
    }
}
impl PartialEq for RowView<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl Eq for RowView<'_> {}
impl Serialize for RowView<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        for (column, value) in self.iter() {
            map.serialize_entry(column, value)?;
        }
        map.end()
    }
}
impl PartialEq<native::NativeRow> for RowView<'_> {
    fn eq(&self, other: &native::NativeRow) -> bool {
        self.iter().eq(other.iter())
    }
}
impl PartialEq<RowView<'_>> for native::NativeRow {
    fn eq(&self, other: &RowView<'_>) -> bool {
        other == self
    }
}

/// Original cells at a frozen boundary. No table or row is materialized to read it.
#[derive(Clone, Copy)]
pub(crate) enum FrozenRows<'a> {
    Archive(&'a ArchivedRows),
    Frame(&'a NativeFrame),
    Execution(&'a ExecutionRows<'a>),
}
/// Actual per-item boundary produced by the validated same-plan cursor. It is
/// never serialized and never substitutes for the original confirmation.
pub(crate) struct ExecutionRows<'a> {
    pub scope: &'a str,
    pub identity: &'a str,
    pub rows: Vec<(&'a native::NativeTable, RowView<'a>)>,
}
pub(crate) struct FrozenTable<'a> {
    pub table: &'a String,
    pub primary_key: &'a Vec<String>,
    pub columns: &'a Vec<String>,
    pub rows: FrozenTableRows<'a>,
}
pub(crate) struct FrozenTableRows<'a> {
    source: FrozenRows<'a>,
    table: &'a str,
}
fn original_view<'a>(
    frame: &'a NativeFrame,
    table: &str,
    row: &'a native::NativeRow,
) -> RowView<'a> {
    let layout = frame.table(table).expect("registered native table");
    match frame.private_rows().iter().find(|(key, _)| {
        key.table == table
            && key.values.len() == layout.primary_key.len()
            && layout
                .primary_key
                .iter()
                .zip(&key.values)
                .all(|(column, value)| native::value(row, column) == Some(value))
    }) {
        Some((_, private)) => RowView::Original {
            public: row,
            private,
        },
        None => RowView::Native(row),
    }
}
impl<'a> FrozenTableRows<'a> {
    pub(crate) fn iter(&self) -> impl Iterator<Item = RowView<'a>> + use<'a> {
        let (archive, frame, execution) = match self.source {
            FrozenRows::Archive(rows) => (Some(rows), None, None),
            FrozenRows::Frame(frame) => (None, Some(frame), None),
            FrozenRows::Execution(rows) => (None, None, Some(rows)),
        };
        let table = self.table;
        archive
            .into_iter()
            .flat_map(|rows| rows.rows())
            .filter(move |(layout, _, _)| layout.table == table)
            .map(|(_, _, row)| RowView::Handle(row))
            .chain(frame.into_iter().flat_map(move |frame| {
                frame.table(table).into_iter().flat_map(move |layout| {
                    layout
                        .rows
                        .iter()
                        .map(move |row| original_view(frame, table, row))
                })
            }))
            .chain(execution.into_iter().flat_map(move |rows| {
                rows.rows
                    .iter()
                    .filter(move |(layout, _)| layout.table == table)
                    .map(|(_, row)| *row)
            }))
    }
}
impl<'a> FrozenRows<'a> {
    pub(crate) fn row_key(self, table: &str, key: &[String]) -> Option<RowView<'a>> {
        self.all_rows()
            .find(|(layout, row)| {
                layout.table == table
                    && layout.primary_key.len() == key.len()
                    && layout
                        .primary_key
                        .iter()
                        .zip(key)
                        .all(|(column, value)| row.cell(column) == Some(value))
            })
            .map(|(_, row)| row)
    }
    pub(crate) fn validate_current(self) -> Result<()> {
        match self {
            Self::Frame(frame) => frame.validate_current(),
            Self::Execution(_) => Ok(()),
            Self::Archive(rows) => {
                if rows.schema_versions() == [schema::VERSION]
                    && rows.catalog() == native::current_catalog()
                {
                    Ok(())
                } else {
                    Err(ManagementError::Schema)
                }
            }
        }
    }
    pub(crate) fn validate_final_layout(self) -> Result<()> {
        let versions = match self {
            Self::Frame(frame) => {
                frame.validate()?;
                frame.schema_versions.as_slice()
            }
            Self::Archive(rows) => rows.schema_versions(),
            Self::Execution(_) => return Ok(()),
        };
        if crate::store::migrations::history::has_final_layout(versions) {
            Ok(())
        } else {
            Err(ManagementError::Schema)
        }
    }
    pub(crate) fn all_rows(self) -> impl Iterator<Item = (&'a native::NativeTable, RowView<'a>)> {
        let (archive, frame, execution) = match self {
            Self::Archive(rows) => (Some(rows), None, None),
            Self::Frame(frame) => (None, Some(frame), None),
            Self::Execution(rows) => (None, None, Some(rows)),
        };
        archive
            .into_iter()
            .flat_map(|rows| rows.rows())
            .map(|(table, _, row)| (table, RowView::Handle(row)))
            .chain(frame.into_iter().flat_map(|frame| {
                frame.tables.iter().flat_map(move |table| {
                    table
                        .rows
                        .iter()
                        .map(move |row| (table, original_view(frame, &table.table, row)))
                })
            }))
            .chain(
                execution
                    .into_iter()
                    .flat_map(|rows| rows.rows.iter().copied()),
            )
    }
    pub(crate) fn scope(self) -> &'a str {
        match self {
            Self::Archive(rows) => rows.scope(),
            Self::Frame(frame) => &frame.database_scope,
            Self::Execution(rows) => rows.scope,
        }
    }
    pub(crate) fn identity(self) -> &'a str {
        match self {
            Self::Archive(rows) => rows.identity(),
            Self::Frame(frame) => &frame.database_identity,
            Self::Execution(rows) => rows.identity,
        }
    }
    pub(crate) fn original_row_matches(
        self,
        table: &str,
        key: &[&str],
        row: &native::NativeRow,
    ) -> bool {
        match self {
            Self::Archive(_) | Self::Execution(_) => self
                .row(table, key)
                .is_some_and(|original| original == *row),
            Self::Frame(frame) => frame.original_row_matches(table, key, row),
        }
    }
    pub(crate) fn projected_row_matches(
        self,
        table: &str,
        key: &[&str],
        expected: &native::NativeRow,
    ) -> bool {
        self.row(table, key).is_some_and(|row| {
            row.iter().count() == expected.len()
                && row.iter().all(|(column, value)| {
                    expected.get(column)
                        == Some(if native::confidential(table, column) {
                            &None
                        } else {
                            value
                        })
                })
        })
    }
    pub(crate) fn row(self, table: &str, key: &[&str]) -> Option<RowView<'a>> {
        match self {
            Self::Execution(rows) => rows
                .rows
                .iter()
                .find(|(layout, row)| {
                    layout.table == table
                        && layout.primary_key.len() == key.len()
                        && layout
                            .primary_key
                            .iter()
                            .zip(key)
                            .all(|(column, value)| row.cell(column) == Some(*value))
                })
                .map(|(_, row)| *row),
            Self::Archive(rows) => rows.row_ref(table, key).map(|row| RowView::Handle(row)),
            Self::Frame(frame) => frame
                .row(table, key)
                .map(|row| original_view(frame, table, row)),
        }
    }
    pub(crate) fn original_value(self, table: &str, key: &[&str], column: &str) -> Option<&'a str> {
        match self {
            Self::Archive(_) | Self::Execution(_) => self.row(table, key)?.cell(column),
            Self::Frame(frame) => frame.original_value(table, key, column),
        }
    }
    pub(crate) fn table(self, name: &str) -> Option<FrozenTable<'a>> {
        let layout = match self {
            Self::Archive(rows) => rows.catalog().iter().find(|table| table.table == name)?,
            Self::Frame(frame) => frame.table(name)?,
            Self::Execution(_) => native::current_catalog()
                .iter()
                .find(|table| table.table == name)?,
        };
        Some(FrozenTable {
            table: &layout.table,
            primary_key: &layout.primary_key,
            columns: &layout.columns,
            rows: FrozenTableRows {
                source: self,
                table: &layout.table,
            },
        })
    }
}
impl<'a> From<&'a NativeFrame> for FrozenRows<'a> {
    fn from(frame: &'a NativeFrame) -> Self {
        Self::Frame(frame)
    }
}
impl<'a> From<&&'a NativeFrame> for FrozenRows<'a> {
    fn from(frame: &&'a NativeFrame) -> Self {
        Self::Frame(frame)
    }
}
