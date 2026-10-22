use super::*;
use crate::store::pinning::management::actions::tests::{Fixture, fixture};

async fn checkpoint(tx: &DatabaseTransaction, fixture: &Fixture, name: &str) -> SourceCheckpoint {
    let mut checkpoint = SourceCheckpoint::start(
        tx,
        RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot()),
    )
    .await
    .unwrap();
    checkpoint.table = native::current_catalog()
        .iter()
        .position(|table| table.table == name)
        .unwrap();
    checkpoint
}

fn ordered() -> Control {
    Control {
        cancel: Default::default(),
        total_rows: u64::MAX,
        total_bytes: u64::MAX,
        ordered_cells: true,
    }
}

#[tokio::test]
async fn ordered_batches_page_native_composite_keys_and_keep_null_and_empty_cells() {
    let fixture = fixture("ordered-source-pages").await;
    fixture.db.execute_unprepared("INSERT INTO zip_batches(id,owner,source,token,fingerprint,bucket,archive_key,input_identity,captured_options,created_at,updated_at) VALUES('batch','owner','direct','token','fingerprint','bucket','archive','input','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    fixture.db.execute_unprepared("WITH RECURSIVE revisions(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM revisions WHERE n<70), epochs(n) AS (SELECT 10 UNION ALL SELECT 2) INSERT INTO zip_root_builds(batch_id,revision,epoch,worker,lease_until,status,created_at,updated_at) SELECT 'batch',revisions.n,epochs.n,'秘密🔒',CURRENT_TIMESTAMP,'verified',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM revisions CROSS JOIN epochs").await.unwrap();
    fixture.db.execute_unprepared("INSERT INTO objects(id,bucket,key,cid,size,etag,encrypted,multipart,is_latest,created_at,content_type) VALUES('empty-source','bucket','empty-source','QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE',0,'etag',0,0,1,CURRENT_TIMESTAMP,''),('unicode-source','bucket','unicode-source','QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE',0,'etag',0,0,1,CURRENT_TIMESTAMP,'秘密🔒')").await.unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let mut cp = checkpoint(&tx, &fixture, "zip_root_builds").await;
    let table = &native::current_catalog()[cp.table];
    let per_page = 256 / table.columns.len();
    let mut rows = 0;
    let mut bytes = 0;
    let mut keys = Vec::new();
    while rows < 140 {
        let page = read(&tx, cp, rows, bytes, ordered()).await.unwrap();
        let count = per_page.min(140 - rows as usize);
        assert_eq!(page.parts.len(), count * table.columns.len());
        assert!(page.parts.len() <= 256);
        assert_eq!(page.members.len(), count);
        assert_eq!(page.work.keyset.calls, 1);
        assert_eq!(page.work.cells.calls, 1);
        assert_eq!(page.work.keyset.rows, count as u64);
        assert_eq!(page.work.cells.rows, count as u64);
        for ((key, position), parts) in page
            .members
            .iter()
            .zip(page.parts.chunks(table.columns.len()))
        {
            assert_eq!(*position, keys.len() as u64 + 1);
            assert!(
                parts
                    .iter()
                    .map(|part| &part.column)
                    .eq(table.columns.iter())
            );
            assert!(
                parts
                    .iter()
                    .all(|part| part.key == *key && part.offset == 0)
            );
            let error = parts
                .iter()
                .find(|part| part.column == "error_code")
                .unwrap();
            assert_eq!(error.length, None);
            assert!(error.bytes.is_empty());
            let worker = parts.iter().find(|part| part.column == "worker").unwrap();
            assert_eq!(worker.length, Some("秘密🔒".len() as u64));
            assert_eq!(worker.bytes, "秘密🔒".as_bytes());
            keys.push(key.clone());
        }
        assert_eq!(page.rows, rows + count as u64);
        assert_eq!(
            page.bytes,
            bytes
                + page
                    .parts
                    .iter()
                    .map(|part| part.bytes.len() as u64)
                    .sum::<u64>()
        );
        assert_eq!(page.checkpoint.after.as_ref(), keys.last());
        assert!(page.checkpoint.row.is_none());
        rows = page.rows;
        bytes = page.bytes;
        cp = page.checkpoint;
    }
    let expected = (1..=70)
        .flat_map(|revision| {
            [2, 10].map(move |epoch| NativeKey {
                table: table.table.clone(),
                values: vec!["batch".into(), revision.to_string(), epoch.to_string()],
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(keys, expected);
    let mut work = ReadWork::default();
    assert!(
        batch::read(&tx, table, None, Some(table.columns.len() - 1), &mut work)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(work.keyset.calls, 0);
    assert_eq!(work.cells.calls, 0);
    let ordinary = read(
        &tx,
        checkpoint(&tx, &fixture, &table.table).await,
        0,
        0,
        Control {
            ordered_cells: false,
            ..ordered()
        },
    )
    .await
    .unwrap();
    assert_eq!(ordinary.members.len(), 140);
    assert_eq!(ordinary.parts.len(), 140 * table.columns.len());
    assert_eq!(ordinary.work.keyset.calls, 1);
    assert_eq!(ordinary.work.cells.calls, 1);
    let objects = read(
        &tx,
        checkpoint(&tx, &fixture, "objects").await,
        0,
        0,
        ordered(),
    )
    .await
    .unwrap();
    let types = objects
        .parts
        .iter()
        .filter(|part| part.column == "content_type")
        .collect::<Vec<_>>();
    assert_eq!(types.len(), 3);
    assert!(
        types
            .iter()
            .any(|part| part.length.is_none() && part.bytes.is_empty())
    );
    let empty = types
        .iter()
        .find(|part| part.key.values[0] == "empty-source")
        .unwrap();
    assert_eq!(empty.length, Some(0));
    assert!(empty.bytes.is_empty());
    let unicode = types
        .iter()
        .find(|part| part.key.values[0] == "unicode-source")
        .unwrap();
    assert_eq!(unicode.length, Some("秘密🔒".len() as u64));
    assert_eq!(unicode.bytes, "秘密🔒".as_bytes());
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn long_private_cell_stops_the_batch_prefix_and_resumes_column_and_offset() {
    let fixture = fixture("ordered-source-long-cell").await;
    let short = "{\"private\":\"秘密🔒\"}";
    let long = format!("{{\"private\":\"秘密{}🔒\"}}", "x".repeat(CELL_SLICE * 20));
    for (epoch, ledger) in [(100, short), (10, long.as_str()), (2, short)] {
        exec(&fixture.db, "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES('source','cid',$1,$2)", vec![epoch.into(), ledger.into()]).await.unwrap();
    }
    let tx = fixture.db.begin().await.unwrap();
    let cp = checkpoint(&tx, &fixture, "pin_resource_history").await;
    let table_index = cp.table;
    let prefix = read(&tx, cp, 0, 0, ordered()).await.unwrap();
    assert_eq!(prefix.members.len(), 1);
    assert_eq!(prefix.members[0].0.values, ["source", "cid", "2"]);
    assert_eq!(prefix.work.keyset.calls, 1);
    assert_eq!(prefix.work.cells.calls, 1);
    let ledger = prefix
        .parts
        .iter()
        .find(|part| part.column == "ledger")
        .unwrap();
    assert_eq!(ledger.bytes, short.as_bytes());
    let middle = read(&tx, prefix.checkpoint, prefix.rows, prefix.bytes, ordered())
        .await
        .unwrap();
    assert!(middle.members.is_empty());
    assert_eq!(middle.checkpoint.table, table_index);
    assert_eq!(
        middle.checkpoint.row.as_ref().unwrap().values,
        ["source", "cid", "10"]
    );
    assert_eq!(
        native::current_catalog()[table_index].columns[middle.checkpoint.column],
        "ledger"
    );
    assert!(middle.checkpoint.offset > 0);
    assert_eq!(middle.checkpoint.offset % CELL_SLICE as u64, 0);
    assert!(middle.parts.iter().all(|part| part.key.values[2] == "10"));
    let offset = middle.checkpoint.offset;
    let mut ledger_bytes = middle
        .parts
        .iter()
        .filter(|part| part.column == "ledger")
        .flat_map(|part| part.bytes.iter().copied())
        .collect::<Vec<_>>();
    let resumed = read(&tx, middle.checkpoint, middle.rows, middle.bytes, ordered())
        .await
        .unwrap();
    assert_eq!(resumed.parts[0].column, "ledger");
    assert_eq!(resumed.parts[0].offset, offset);
    ledger_bytes.extend(
        resumed
            .parts
            .iter()
            .filter(|part| {
                part.table == "pin_resource_history"
                    && part.key.values[2] == "10"
                    && part.column == "ledger"
            })
            .flat_map(|part| part.bytes.iter().copied()),
    );
    assert_eq!(ledger_bytes, long.as_bytes());
    let completed = resumed
        .members
        .iter()
        .filter(|(key, _)| key.table == "pin_resource_history")
        .map(|(key, _)| key.values[2].as_str())
        .collect::<Vec<_>>();
    assert_eq!(completed, ["10", "100"]);
    let tail = resumed
        .parts
        .iter()
        .find(|part| {
            part.table == "pin_resource_history"
                && part.key.values[2] == "100"
                && part.column == "ledger"
        })
        .unwrap();
    assert_eq!(tail.bytes, short.as_bytes());
    tx.rollback().await.unwrap();
}
