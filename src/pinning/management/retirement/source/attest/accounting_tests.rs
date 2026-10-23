use super::*;
use crate::store::pinning::management::actions::tests::fixture;

fn key_bytes(key: &NativeKey) -> u64 {
    (key.table.len() + key.values.iter().map(String::len).sum::<usize>()) as u64
}

fn stored_parts(parts: Vec<Part>) -> Vec<ArchivePart> {
    parts
        .into_iter()
        .map(|part| ArchivePart {
            table: part.table.into(),
            key: part.key.as_ref().clone(),
            column: part.column.into(),
            offset: part.offset,
            length: part.length,
            bytes: part.bytes,
        })
        .collect()
}

async fn fence_bytes(tx: &DatabaseTransaction, checkpoint: &SourceCheckpoint) -> u64 {
    let row = tx
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT file AS identity FROM pragma_database_list WHERE name='main'",
        ))
        .await
        .unwrap()
        .unwrap();
    let identity: String = row.try_get("", "identity").unwrap();
    checkpoint.database_scope.len() as u64
        + identity.len() as u64
        + checkpoint
            .revisions
            .keys()
            .map(|table| table.len() as u64 + 8)
            .sum::<u64>()
}

#[tokio::test]
async fn source_batch_accounting_includes_rejected_metadata_and_temporary_key_encoding() {
    let fixture = fixture("source-batch-accounting").await;
    let short = "{\"private\":\"秘密🔒\"}";
    let long = "x".repeat(CELL_SLICE + 1);
    for (epoch, ledger) in [(2, short), (10, long.as_str()), (100, short)] {
        exec(&fixture.db, "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES('meter-source','cid',$1,$2)", vec![epoch.into(), ledger.into()]).await.unwrap();
    }
    let tx = fixture.db.begin().await.unwrap();
    let mut checkpoint = SourceCheckpoint::start(
        &tx,
        RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot()),
    )
    .await
    .unwrap();
    let catalog = native::current_catalog();
    checkpoint.table = catalog
        .iter()
        .position(|table| table.table == "pin_resource_history")
        .unwrap();
    let table = &catalog[checkpoint.table];
    let read = page::read(
        &tx,
        checkpoint.clone(),
        0,
        0,
        page::Control {
            cancel: Default::default(),
            total_rows: 256,
            total_bytes: 1024 * 1024,
            ordered_cells: true,
        },
    )
    .await
    .unwrap();
    assert_eq!(read.members.len(), 1);
    assert_eq!(read.members[0].0.values, ["meter-source", "cid", "2"]);
    let keys = [2, 10, 100].map(|epoch| NativeKey {
        table: table.table.clone(),
        values: vec!["meter-source".into(), "cid".into(), epoch.to_string()],
    });
    let metadata_keys = keys
        .iter()
        .flat_map(|key| &key.values)
        .map(|value| value.len() as u64)
        .sum::<u64>();
    let selected_keys = keys[0]
        .values
        .iter()
        .map(|value| value.len() as u64)
        .sum::<u64>();
    let returned = metadata_keys
        + 3 * table.columns.len() as u64 * 8
        + selected_keys
        + read
            .parts
            .iter()
            .map(|part| part.bytes.len() as u64)
            .sum::<u64>()
        + 2 * fence_bytes(&tx, &checkpoint).await;
    let copied = 3 * table.table.len() as u64
        + encode(&keys[0]).unwrap().len() as u64
        + encode(&keys[1]).unwrap().len() as u64
        + 2 * key_bytes(&keys[0]);
    let before = Cursor {
        source: checkpoint,
        ordinal: 0,
        partial: 0,
        position: 0,
        eof: 0,
    };
    let accepted = compare(
        &tx,
        before,
        LocalPage {
            parts: stored_parts(read.parts),
            members: read.members.into_iter().map(|(key, _)| key).collect(),
            eof: read.eof,
            bytes: 0,
        },
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(accepted.read_bytes, returned + copied);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn source_fragment_accounting_includes_every_length_probe_and_shared_key_copy() {
    let fixture = fixture("source-fragment-accounting").await;
    let original = "界".repeat(CELL_SLICE * 12);
    exec(&fixture.db, "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES('meter-source','cid',1,$1)", vec![original.into()]).await.unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let mut checkpoint = SourceCheckpoint::start(
        &tx,
        RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot()),
    )
    .await
    .unwrap();
    let catalog = native::current_catalog();
    checkpoint.table = catalog
        .iter()
        .position(|table| table.table == "pin_resource_history")
        .unwrap();
    let table = &catalog[checkpoint.table];
    checkpoint.row = Some(NativeKey {
        table: table.table.clone(),
        values: vec!["meter-source".into(), "cid".into(), "1".into()],
    });
    checkpoint.column = table
        .columns
        .iter()
        .position(|column| column == "ledger")
        .unwrap();
    checkpoint.offset = CELL_SLICE as u64;
    let read = page::read(
        &tx,
        checkpoint.clone(),
        0,
        0,
        page::Control {
            cancel: Default::default(),
            total_rows: 256,
            total_bytes: 1024 * 1024,
            ordered_cells: true,
        },
    )
    .await
    .unwrap();
    assert!(read.members.is_empty());
    assert_eq!(read.bytes, 1024 * 1024);
    let returned = read.bytes
        + read.parts.len() as u64 * (table.columns.len() - checkpoint.column) as u64 * 8
        + 2 * fence_bytes(&tx, &checkpoint).await;
    let copied = read.parts.len() as u64 * key_bytes(checkpoint.row.as_ref().unwrap());
    let before = Cursor {
        source: checkpoint,
        ordinal: 0,
        partial: 0,
        position: 0,
        eof: 0,
    };
    let accepted = compare(
        &tx,
        before,
        LocalPage {
            parts: stored_parts(read.parts),
            members: Vec::new(),
            eof: read.eof,
            bytes: 0,
        },
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(accepted.read_bytes, returned + copied);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn source_fence_accounting_uses_the_returned_identity_projection() {
    let fixture = fixture("source-fence-accounting").await;
    let tx = fixture.db.begin().await.unwrap();
    let read = super::super::check_read::CheckRead::new(&tx);
    let identity = format!("source/{}", "秘密🔒".repeat(40));
    let row = read
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT $1 AS identity",
            [identity.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<String>("", "identity").unwrap(), identity);
    assert_eq!(read.bytes(), identity.len() as u64);
    assert!(
        read.query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT 'unaccounted' AS unexpected_projection"
        ))
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}
