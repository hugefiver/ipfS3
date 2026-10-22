use super::super::{CellValue, Event, Frame, Projection, Section, state::private_cell};

pub(super) struct ProjectionState {
    mode: Projection,
    section: Section,
    table: String,
    private: bool,
}
impl ProjectionState {
    pub fn new(mode: Projection) -> Self {
        Self {
            mode,
            section: Section::Native,
            table: String::new(),
            private: false,
        }
    }
    pub fn apply(&mut self, mut event: Event) -> Option<Event> {
        match &mut event {
            Event::Frame(Frame::Header { projection, .. }) => *projection = self.mode,
            _ if self.mode == Projection::PrivateRecovery => {}
            Event::Frame(Frame::Row { section, key, .. }) => {
                self.section = *section;
                self.table.clone_from(&key.table);
                self.private = false;
            }
            Event::Frame(Frame::Cell { column }) => {
                self.private = private_cell(self.section, &self.table, column);
            }
            Event::Chunk(_) if self.private => return None,
            Event::Frame(Frame::CellEnd { value }) => {
                if self.private && *value == CellValue::Text {
                    *value = CellValue::Redacted;
                }
                self.private = false;
            }
            Event::Frame(Frame::RowEnd) => {
                self.table.clear();
                self.private = false;
            }
            _ => {}
        }
        Some(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pinning::management::{NativeKey, wire::v3::AppliedTrust};

    fn header(projection: Projection) -> Frame {
        Frame::Header {
            schema_version: 3,
            projection,
            database_scope: "scope".into(),
            database_identity: "identity".into(),
            principal: "principal".into(),
            applied_trust: AppliedTrust::UntrustedClaimsOnly,
            catalog: vec!["catalog".into()],
            created_at: "time".into(),
            expires_at: None,
        }
    }

    #[test]
    fn headers_follow_requested_projection() {
        for mode in [Projection::Review, Projection::PrivateRecovery] {
            for source in [Projection::Review, Projection::PrivateRecovery] {
                let mut state = ProjectionState::new(mode);
                assert!(matches!(
                    state.apply(Event::Frame(header(source))),
                    Some(Event::Frame(frame)) if frame == header(mode)
                ));
            }
        }
    }

    #[test]
    fn cell_classification_follows_columns_rows_and_mode() {
        use CellValue::{Null, Redacted, Text};
        type Row<'a> = (Section, &'a str, &'a [(&'a str, CellValue, bool)]);
        let rows: &[Row<'_>] = &[
            (
                Section::Native,
                "objects",
                &[
                    ("key", Text, false),
                    ("metadata", Text, true),
                    ("size", Text, false),
                    ("sse_c_key_fingerprint", Null, true),
                    ("unknown_column", Text, true),
                ],
            ),
            (Section::Native, "objects", &[("key", Text, false)]),
            (
                Section::Native,
                "pin_management_archive",
                &[("archive_id", Text, true)],
            ),
            (
                Section::Native,
                "pin_management_archive_chunks",
                &[("archive_id", Text, true)],
            ),
            (Section::Native, "unknown_table", &[("key", Text, true)]),
            (Section::Membership, "objects", &[("key", Text, true)]),
            (Section::Inventory, "objects", &[("key", Text, true)]),
            (Section::Result, "objects", &[("key", Text, true)]),
            (Section::AppliedClaim, "objects", &[("key", Text, true)]),
        ];
        for mode in [Projection::Review, Projection::PrivateRecovery] {
            let mut state = ProjectionState::new(mode);
            for (index, (section, table, cells)) in rows.iter().enumerate() {
                assert!(matches!(
                    state.apply(Event::Frame(Frame::Row {
                        section: *section,
                        key: NativeKey {
                            table: (*table).into(),
                            values: vec![index.to_string()],
                        },
                        cells: cells.len() as u32,
                    })),
                    Some(Event::Frame(Frame::Row { .. }))
                ));
                for (column, value, private) in *cells {
                    assert!(matches!(
                        state.apply(Event::Frame(Frame::Cell {
                            column: (*column).into(),
                        })),
                        Some(Event::Frame(Frame::Cell { column: actual })) if actual == *column
                    ));
                    let suppress = mode == Projection::Review && *private;
                    if *value == Text {
                        for bytes in [b"first".as_slice(), b"second".as_slice()] {
                            let event = state.apply(Event::Chunk(bytes.to_vec()));
                            if suppress {
                                assert!(event.is_none());
                            } else {
                                assert!(matches!(
                                    event,
                                    Some(Event::Chunk(actual)) if actual == bytes
                                ));
                            }
                        }
                    }
                    let expected = if suppress && *value == Text {
                        Redacted
                    } else {
                        *value
                    };
                    assert!(matches!(
                        state.apply(Event::Frame(Frame::CellEnd { value: *value })),
                        Some(Event::Frame(Frame::CellEnd { value: actual })) if actual == expected
                    ));
                }
                assert!(matches!(
                    state.apply(Event::Frame(Frame::RowEnd)),
                    Some(Event::Frame(Frame::RowEnd))
                ));
            }
        }
    }
}
