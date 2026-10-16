//! Direct public projection over frozen original cells; no native tree is rebuilt.
use super::*;
use crate::store::pinning::management::{
    native,
    records::{ArchivedRows, FrozenRows, ReadSession, ReviewHandle, RowView},
};
use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};

struct PublicRow<'a>(&'a str, RowView<'a>);
impl Serialize for PublicRow<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(None)?;
        for (column, value) in self.1.iter() {
            map.serialize_entry(
                column,
                if native::confidential(self.0, column) {
                    &None
                } else {
                    value
                },
            )?;
        }
        map.end()
    }
}
struct TableRows<'a>(FrozenRows<'a>, &'a str);
impl Serialize for TableRows<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut sequence = s.serialize_seq(None)?;
        for (table, row) in self.0.all_rows().filter(|(table, _)| table.table == self.1) {
            sequence.serialize_element(&PublicRow(&table.table, row))?;
        }
        sequence.end()
    }
}
struct Tables<'a>(FrozenRows<'a>, &'a [native::NativeTable]);
impl Serialize for Tables<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Table<'a> {
            table: &'a str,
            columns: &'a [String],
            primary_key: &'a [String],
            rows: TableRows<'a>,
            definitions: &'a [crate::store::pinning::management::catalog::NativeColumn],
        }
        let mut sequence = s.serialize_seq(Some(self.1.len()))?;
        for table in self.1 {
            sequence.serialize_element(&Table {
                table: &table.table,
                columns: &table.columns,
                primary_key: &table.primary_key,
                rows: TableRows(self.0, &table.table),
                definitions: &table.definitions,
            })?;
        }
        sequence.end()
    }
}
struct Frontier<'a>(FrozenRows<'a>);
impl Serialize for Frontier<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        struct Values<'a>(&'a native::NativeTable, RowView<'a>);
        impl Serialize for Values<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                let mut seq = s.serialize_seq(Some(self.0.primary_key.len()))?;
                for column in &self.0.primary_key {
                    seq.serialize_element(self.1.cell(column).ok_or_else(|| {
                        <S::Error as serde::ser::Error>::custom("invalid frozen key")
                    })?)?;
                }
                seq.end()
            }
        }
        #[derive(Serialize)]
        struct Key<'a> {
            table: &'a str,
            values: Values<'a>,
        }
        let mut sequence = s.serialize_seq(None)?;
        for (table, row) in self.0.all_rows() {
            sequence.serialize_element(&Key {
                table: &table.table,
                values: Values(table, row),
            })?;
        }
        sequence.end()
    }
}
pub(super) struct NativeProjection<'a> {
    source: FrozenRows<'a>,
    versions: &'a [String],
    catalog: &'a [native::NativeTable],
}
impl<'a> NativeProjection<'a> {
    fn archive(rows: &'a ArchivedRows) -> Self {
        Self {
            source: FrozenRows::Archive(rows),
            versions: rows.schema_versions(),
            catalog: rows.catalog(),
        }
    }
    pub(super) fn frame(frame: &'a NativeFrame) -> Self {
        Self {
            source: FrozenRows::Frame(frame),
            versions: &frame.schema_versions,
            catalog: &frame.tables,
        }
    }
}
impl Serialize for NativeProjection<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut state = s.serialize_struct("NativeFrame", 5)?;
        state.serialize_field("schema_versions", self.versions)?;
        state.serialize_field("database_scope", self.source.scope())?;
        state.serialize_field("database_identity", self.source.identity())?;
        state.serialize_field("tables", &Tables(self.source, self.catalog))?;
        state.serialize_field("frontier", &Frontier(self.source))?;
        state.end()
    }
}

/// A reviewed file carries proposal data and ACKs, never local authority.
pub fn encode_review(
    session: &mut ReadSession,
    review: &ReviewHandle,
    confirmation: Option<&[String]>,
    output: &mut impl std::io::Write,
) -> Result<(), SafeError> {
    let candidates = review
        .entries()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate);
    if let Some(acks) = confirmation
        && (acks.len() > super::super::MAX_ITEMS
            || acks.iter().enumerate().any(|(index, id)| {
                acks[..index].contains(id)
                    || !review.entries().iter().any(|entry| {
                        entry.status == PlanStatus::Candidate && entry.request.item_id == *id
                    })
            })
            || candidates.count() != acks.len())
    {
        return Err(SafeError::ConfirmationMismatch);
    }
    #[derive(Serialize)]
    struct Plan<'a> {
        items: &'a [PlanEntry],
        summary: &'a Summary,
    }
    #[derive(Serialize)]
    struct Document<'a, T: Serialize> {
        schema_version: u32,
        request: &'a PlanRequest,
        snapshot: &'a T,
        native: NativeProjection<'a>,
        plan: Plan<'a>,
        migration_points: &'a [crate::store::pinning::management::records::MigrationPoint],
        confirmation: Option<&'a [String]>,
        applied_trust: AppliedTrust,
    }
    let document = Document {
        schema_version: WIRE_SCHEMA_VERSION,
        request: review.request(),
        snapshot: review.snapshot(),
        native: NativeProjection::archive(review.archive()),
        plan: Plan {
            items: review.entries(),
            summary: review.summary(),
        },
        migration_points: review.migration_points(),
        confirmation,
        applied_trust: AppliedTrust::UntrustedClaimsOnly,
    };
    session
        .copy(&document)
        .map_err(|_| SafeError::LimitExceeded)?;
    struct Bounded<'a, W> {
        output: &'a mut W,
        bytes: usize,
    }
    impl<W: std::io::Write> std::io::Write for Bounded<'_, W> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > MAX_WIRE_BYTES.saturating_sub(self.bytes) {
                return Err(std::io::Error::other("document limit"));
            }
            let written = self.output.write(bytes)?;
            self.bytes += written;
            Ok(written)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.output.flush()
        }
    }
    serde_json::to_writer(Bounded { output, bytes: 0 }, &document)
        .map_err(|_| SafeError::LimitExceeded)
}

pub(crate) fn native_equal(review: &ReviewHandle, frame: &NativeFrame) -> bool {
    let rows = review.archive();
    if rows.scope() != frame.database_scope
        || rows.identity() != frame.database_identity
        || rows.schema_versions() != frame.schema_versions
        || rows.catalog().len() != frame.tables.len()
    {
        return false;
    }
    if !rows
        .catalog()
        .iter()
        .zip(&frame.tables)
        .all(|(left, right)| {
            left.table == right.table
                && left.columns == right.columns
                && left.primary_key == right.primary_key
                && left.definitions == right.definitions
        })
    {
        return false;
    }
    if !rows
        .rows()
        .map(|(table, key, _)| (&table.table, key))
        .eq(frame
            .frontier
            .iter()
            .map(|key| (&key.table, key.values.as_slice())))
    {
        return false;
    }
    let projection = FrozenRows::Frame(frame);
    rows.rows().all(|(table, key, row)| {
        projection.row_key(&table.table, key).is_some_and(|public| {
            public.iter().count() == table.columns.len()
                && row.iter().all(|(column, value)| {
                    public.get(column)
                        == Some(if native::confidential(&table.table, column) {
                            &None
                        } else {
                            value
                        })
                })
        })
    })
}
