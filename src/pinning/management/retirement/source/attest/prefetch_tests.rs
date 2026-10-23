use super::*;
use crate::store::pinning::management::actions::tests::fixture;

#[test]
fn incomplete_final_cell_does_not_admit_a_completed_member() {
    let table = native::current_catalog()
        .iter()
        .find(|table| table.table == "objects")
        .unwrap();
    let mut part = ArchivePart {
        table: table.table.clone(),
        key: NativeKey {
            table: table.table.clone(),
            values: vec!["object".into()],
        },
        column: table.columns.last().unwrap().clone(),
        offset: 0,
        length: Some((2 * CELL_SLICE) as u64),
        bytes: vec![b'x'; CELL_SLICE],
    };
    assert_eq!(member_limit(std::slice::from_ref(&part)).unwrap(), 1);
    part.offset = CELL_SLICE as u64;
    assert_eq!(member_limit(std::slice::from_ref(&part)).unwrap(), 2);
    part.column = table.columns[0].clone();
    assert_eq!(member_limit(std::slice::from_ref(&part)).unwrap(), 1);
    part.column = table.columns.last().unwrap().clone();
    part.offset = 0;
    part.length = None;
    part.bytes.clear();
    assert_eq!(member_limit(std::slice::from_ref(&part)).unwrap(), 2);
}

#[tokio::test]
async fn member_prefix_keeps_cross_page_private_bytes_and_the_empty_tail_fence() {
    let fixture = fixture("source-attestation-large-row").await;
    let raw = format!(
        "{{\"private\":\"{}\"}}",
        "界".repeat(2 * 1024 * 1024 / 3 + 1)
    );
    fixture.db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES('large-source','cid',1,$1)",
        [raw.clone().into()])).await.unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot()),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spool = InventorySpool::create(&path, file, &checkpoint)
        .await
        .unwrap();
    let mut limits = RunLimits::default();
    limits.total_bytes = crate::pinning::management::wire::v3::files::stream::limits().bytes;
    while spool.scan(&fixture.db, &limits).await.unwrap() == Progress::Pending {}
    let run = super::super::super::spool::task_run(&spool.db, &limits)
        .await
        .unwrap();
    let mut cursor = Cursor::new(checkpoint);
    let mut original = Vec::new();
    let mut members = 0;
    let mut partial_row = false;
    while !cursor.source.complete() {
        let stored = local(&spool, &cursor, &run).await.unwrap();
        let fetched_members = stored.members.len();
        let tx = fixture.db.begin().await.unwrap();
        let accepted = compare(&tx, cursor, stored, Default::default())
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        super::proof::assert_round_trip(&accepted);
        for part in &accepted.parts {
            if part.table == "pin_resource_history"
                && part.column == "ledger"
                && part.key.values[0] == "large-source"
            {
                original.extend_from_slice(&part.bytes);
            }
        }
        let completed = accepted
            .members
            .iter()
            .filter(|key| key.table == "pin_resource_history" && key.values[0] == "large-source")
            .count();
        if accepted.after.source.row.as_ref().is_some_and(|key| {
            key.table == "pin_resource_history" && key.values[0] == "large-source"
        }) {
            assert_eq!(completed, 0);
            assert!(fetched_members >= 1);
            partial_row = true;
        }
        members += completed;
        cursor = accepted.after;
    }
    assert!(partial_row);
    assert_eq!(members, 1);
    assert_eq!(original, raw.as_bytes());
    let before_tail = spool.task_work().await.unwrap();
    let tail = local(&spool, &cursor, &run).await.unwrap();
    assert!(tail.parts.is_empty() && tail.members.is_empty());
    let after_tail = spool.task_work().await.unwrap();
    let eof_bytes = tail.eof.iter().map(|table| table.len() as u64).sum::<u64>();
    assert!(eof_bytes > 0);
    assert_eq!(after_tail.bytes - before_tail.bytes, eof_bytes);
    let extra = NativeKey {
        table: "objects".into(),
        values: vec!["unseen-tail".into()],
    };
    exec(
        &spool.db,
        "INSERT INTO members VALUES($1,$2,$3)",
        vec![
            extra.table.clone().into(),
            encode(&extra).unwrap().into(),
            (cursor.position + 1).into(),
        ],
    )
    .await
    .unwrap();
    exec(
        &spool.db,
        "INSERT INTO wire_keys VALUES($1,$2,$3)",
        vec![
            extra.table.clone().into(),
            super::super::super::spool::wire_key(&extra).into(),
            encode(&extra).unwrap().into(),
        ],
    )
    .await
    .unwrap();
    assert!(matches!(
        local(&spool, &cursor, &run).await,
        Err(ManagementError::Changed)
    ));
    spool.close().await.unwrap();
    workspace.cleanup().unwrap();
}

#[tokio::test]
async fn local_materialized_reads_keep_failure_debt_and_charge_a_successful_retry() {
    let fixture = fixture("source-attestation-read-accounting").await;
    let tx = fixture.db.begin().await.unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot()),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spool = InventorySpool::create(&path, file, &checkpoint)
        .await
        .unwrap();
    let limits = RunLimits::default();
    while spool.scan(&fixture.db, &limits).await.unwrap() == Progress::Pending {}
    let run = super::super::super::spool::task_run(&spool.db, &limits)
        .await
        .unwrap();
    let cursor = Cursor::new(checkpoint);
    let before = spool.task_work().await.unwrap();
    let loaded = local(&spool, &cursor, &run).await.unwrap();
    let part_bytes = loaded
        .parts
        .iter()
        .enumerate()
        .map(|(index, part)| {
            (part.table.len()
                + encode(&part.key).unwrap().len()
                + part.column.len()
                + part.bytes.len()
                + 64
                + 32
                + if index == 0 || loaded.parts[index - 1].key != part.key {
                    encode(&part.key).unwrap().len()
                } else {
                    0
                }) as u64
        })
        .sum::<u64>();
    let member_bytes = loaded
        .members
        .iter()
        .map(|key| {
            (2 * (key.table.len()
                + encode(key).unwrap().len()
                + super::super::super::spool::wire_key(key).len())
                + 8) as u64
        })
        .sum::<u64>();
    let paid = spool.task_work().await.unwrap();
    let eof_bytes = loaded
        .eof
        .iter()
        .map(|table| table.len() as u64)
        .sum::<u64>();
    assert_eq!(
        paid.bytes - before.bytes,
        part_bytes + member_bytes + eof_bytes
    );
    assert_eq!(paid.rows - before.rows, loaded.parts.len() as u64);
    assert!(loaded.bytes > part_bytes);
    let checksum = loaded.parts[0].checksum().unwrap();
    exec(
        &spool.db,
        "UPDATE parts SET checksum=$1 WHERE ordinal=1",
        vec!["0".repeat(64).into()],
    )
    .await
    .unwrap();
    assert!(matches!(
        local(&spool, &cursor, &run).await,
        Err(ManagementError::Changed)
    ));
    let failed = spool.task_work().await.unwrap();
    assert_eq!(failed.bytes - paid.bytes, loaded.bytes);
    exec(
        &spool.db,
        "UPDATE parts SET checksum=$1 WHERE ordinal=1",
        vec![checksum.into()],
    )
    .await
    .unwrap();
    spool.close().await.unwrap();
    let spool = InventorySpool::resume(&path).await.unwrap();
    assert_eq!(spool.task_work().await.unwrap(), failed);
    let retried = local(&spool, &cursor, &run).await.unwrap();
    let complete = spool.task_work().await.unwrap();
    assert_eq!(
        complete.bytes,
        failed.bytes + part_bytes + member_bytes + eof_bytes
    );
    assert!(retried.parts == loaded.parts && retried.members == loaded.members);
    let tx = fixture.db.begin().await.unwrap();
    let accepted = compare(&tx, cursor, retried, Default::default())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(accepted.read_bytes > 0);
    assert!(
        accepted.read_bytes
            >= accepted
                .parts
                .iter()
                .map(|part| part.bytes.len() as u64)
                .sum::<u64>()
    );
    super::proof::assert_round_trip(&accepted);
    spool.close().await.unwrap();
    workspace.cleanup().unwrap();
}
