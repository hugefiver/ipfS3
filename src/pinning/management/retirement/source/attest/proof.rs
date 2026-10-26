use super::*;
use serde::ser::SerializeSeq;

/// Consecutive cells share their original table/key; every cell retains its
/// column, offset, NULL distinction and complete original byte slice.
pub(super) fn serialize_parts<S: serde::Serializer>(
    mut parts: &[Part],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    let mut rows = serializer.serialize_seq(None)?;
    while let Some(first) = parts.first() {
        let count = parts
            .iter()
            .take_while(|part| part.table == first.table && part.key == first.key)
            .count();
        rows.serialize_element(&(first.table, first.key.as_ref(), Cells(&parts[..count])))?;
        parts = &parts[count..];
    }
    rows.end()
}

struct Cells<'a>(&'a [Part]);
impl Serialize for Cells<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut cells = serializer.serialize_seq(Some(self.0.len()))?;
        for part in self.0 {
            cells.serialize_element(&(
                &part.column,
                part.offset,
                part.length,
                part.bytes.as_slice(),
            ))?;
        }
        cells.end()
    }
}

#[cfg(test)]
pub(super) fn assert_round_trip(accepted: &AcceptedPage) {
    type ProofCell = (String, u64, Option<u64>, Vec<u8>);

    #[derive(Deserialize)]
    struct Proof {
        format: String,
        before: Cursor,
        after: Cursor,
        parts: Vec<(String, NativeKey, Vec<ProofCell>)>,
        members: Vec<NativeKey>,
        eof: Vec<String>,
    }
    let payload = serde_json::to_vec(accepted).unwrap();
    let restored: Proof = serde_json::from_slice(&payload).unwrap();
    assert_eq!(restored.format, "S75-source-rows-1");
    assert!(restored.before == accepted.before && restored.after == accepted.after);
    let parts = restored
        .parts
        .into_iter()
        .flat_map(|(table, key, cells)| {
            cells
                .into_iter()
                .map(move |(column, offset, length, bytes)| ArchivePart {
                    table: table.clone(),
                    key: key.clone(),
                    column,
                    offset,
                    length,
                    bytes,
                })
        })
        .collect::<Vec<_>>();
    assert_eq!(parts.len(), accepted.parts.len());
    assert!(
        accepted
            .parts
            .iter()
            .zip(parts)
            .all(|(part, restored)| *part == restored)
    );
    assert!(restored.members == accepted.members && restored.eof == accepted.eof);
}

#[cfg(test)]
#[tokio::test]
async fn grouped_proof_retains_null_empty_private_fragments_and_row_order() {
    let fixture =
        crate::store::pinning::management::actions::tests::fixture("source-proof-rows").await;
    let tx = fixture.db.begin().await.unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot()),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let key = NativeKey {
        table: "pin_resource_history".into(),
        values: vec!["foreign-provider".into(), "秘密🔒".into(), "3".into()],
    };
    let mut parts = Vec::new();
    for (column, offset, length, bytes) in [
        ("provider", 0, Some(16), b"foreign-provider".to_vec()),
        ("ledger", 0, None, Vec::new()),
        ("ledger", 0, Some(0), Vec::new()),
        ("ledger", 7, Some(10), vec![0xe7, 0x95]),
        ("ledger", 9, Some(10), vec![0x8c]),
    ] {
        parts.push(Part {
            table: "pin_resource_history",
            key: std::sync::Arc::new(key.clone()),
            column,
            offset,
            length,
            bytes,
        });
    }
    let next = NativeKey {
        table: "objects".into(),
        values: vec!["next".into()],
    };
    parts.push(Part {
        table: "objects",
        key: std::sync::Arc::new(next.clone()),
        column: "key",
        offset: 0,
        length: Some(4),
        bytes: b"next".to_vec(),
    });
    parts.push(parts[0].clone());
    let before = Cursor::new(checkpoint);
    let mut after = before.clone();
    after.partial = 7;
    after.ordinal = parts.len() as i64 - 1;
    after.position = 1;
    let accepted = AcceptedPage {
        format: "S75-source-rows-1",
        before,
        after,
        parts,
        members: vec![key, next],
        eof: vec!["pin_resource_history".into()],
        read_bytes: 31,
    };
    assert_round_trip(&accepted);
}
