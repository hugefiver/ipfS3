use super::*;

#[tokio::test]
async fn completed_wire_page_settles_only_its_unused_local_read_allowance() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
    let limits = RunLimits::default();
    let run = spool::task_run(&spool.db, &limits).await.unwrap();
    let mut expected_bytes = 0;
    for (index, id) in ["first", "秘密🔒", "third"].into_iter().enumerate() {
        let key = NativeKey {
            table: "objects".into(),
            values: vec![id.into()],
        };
        let raw = encode(&key).unwrap();
        let order = spool::wire_key(&key);
        expected_bytes += 2 * (key.table.len() + order.len() + raw.len()) as u64;
        exec(
            &spool.db,
            "INSERT INTO members VALUES($1,$2,$3)",
            vec![
                key.table.clone().into(),
                raw.clone().into(),
                ((index + 1) as i64).into(),
            ],
        )
        .await
        .unwrap();
        exec(
            &spool.db,
            "INSERT INTO wire_keys VALUES($1,$2,$3)",
            vec![key.table.into(), order.into(), raw.into()],
        )
        .await
        .unwrap();
    }
    spool::budget::charge(&spool.db, &run, 7, 1024)
        .await
        .unwrap();
    let before = coordinates(&spool).await.unwrap();
    spool::budget::charge(&spool.db, &run, 512, 2 * 1024 * 1024)
        .await
        .unwrap();
    let (after, count, payload, done, used) = source::attest::wire(&spool, None).await.unwrap();
    assert_eq!(count, 3);
    assert!(!done);
    assert_eq!(used.rows, 6);
    assert_eq!(used.bytes, expected_bytes);
    settle_local_read(&spool, 512, 2 * 1024 * 1024, used)
        .await
        .unwrap();
    spool::budget::copy_work(&spool.db, &run, payload.len())
        .await
        .unwrap();
    let paid = coordinates(&spool).await.unwrap();
    assert_eq!(paid.rows, before.rows + 6);
    assert_eq!(
        paid.bytes,
        before.bytes + expected_bytes as i64 + payload.len() as i64
    );
    assert_eq!(paid.pages, before.pages + 1);
    let (_, count, _, done, used) = source::attest::wire(&spool, after.as_ref()).await.unwrap();
    assert_eq!(count, 0);
    assert!(done);
    assert_eq!((used.rows, used.bytes), (0, 0));
    spool.close().await.unwrap();
    workspace.cleanup().unwrap();
}

#[tokio::test]
async fn completed_retry_does_not_refund_the_previous_interrupted_read() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
    let limits = RunLimits::default();
    let run = spool::task_run(&spool.db, &limits).await.unwrap();
    spool::budget::charge(&spool.db, &run, 512, 2 * 1024 * 1024)
        .await
        .unwrap();
    let failed = coordinates(&spool).await.unwrap();
    spool.close().await.unwrap();
    let spool = InventorySpool::resume(&path).await.unwrap();
    spool::budget::charge(&spool.db, &run, 512, 2 * 1024 * 1024)
        .await
        .unwrap();
    settle_local_read(
        &spool,
        512,
        2 * 1024 * 1024,
        TaskWork {
            rows: 2,
            bytes: 1234,
            pages: 0,
        },
    )
    .await
    .unwrap();
    let paid = coordinates(&spool).await.unwrap();
    assert_eq!(paid.rows, failed.rows + 2);
    assert_eq!(paid.bytes, failed.bytes + 1234);
    assert_eq!(paid.pages, failed.pages + 1);
    spool.close().await.unwrap();
    workspace.cleanup().unwrap();
}

#[tokio::test]
async fn reopening_after_page_commit_finishes_interrupted_local_accounting_once() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
    let limits = RunLimits::default();
    let run = spool::task_run(&spool.db, &limits).await.unwrap();
    let before = coordinates(&spool).await.unwrap();
    let pending = encode(&(
        "database before cursor",
        before.clone(),
        512u64,
        16 * 1024 * 1024u64,
    ))
    .unwrap();
    let tx = spool.db.begin().await.unwrap();
    spool::budget::charge(&tx, &run, 512, 16 * 1024 * 1024)
        .await
        .unwrap();
    exec(
        &tx,
        "UPDATE task_budget SET confirmation_pending=$1 WHERE id=1",
        vec![pending.into()],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    // The accepted target page carries these consumption coordinates. No local
    // reconciliation was committed before this task handle was lost.
    let accepted = TaskCoordinates {
        rows: before.rows + 7,
        bytes: before.bytes + 4096,
        pages: before.pages + 1,
    };
    spool.close().await.unwrap();
    let reopened = InventorySpool::resume(&path).await.unwrap();
    reconcile_coordinates(&reopened, Some(&before), Some(&accepted))
        .await
        .unwrap();
    assert!(coordinates(&reopened).await.unwrap() == accepted);
    reconcile_coordinates(&reopened, Some(&before), Some(&accepted))
        .await
        .unwrap();
    assert!(coordinates(&reopened).await.unwrap() == accepted);
    reopened.close().await.unwrap();
    workspace.cleanup().unwrap();
}

#[tokio::test]
async fn uncommitted_page_keeps_failed_attempt_cost_across_reopen() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
    let limits = RunLimits::default();
    let run = spool::task_run(&spool.db, &limits).await.unwrap();
    let before = coordinates(&spool).await.unwrap();
    let pending = encode(&("database before cursor", before, 32u64, 1024u64)).unwrap();
    let tx = spool.db.begin().await.unwrap();
    spool::budget::charge(&tx, &run, 32, 1024).await.unwrap();
    exec(
        &tx,
        "UPDATE task_budget SET confirmation_pending=$1 WHERE id=1",
        vec![pending.into()],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let failed = coordinates(&spool).await.unwrap();
    spool.close().await.unwrap();
    let reopened = InventorySpool::resume(&path).await.unwrap();
    reconcile_coordinates(&reopened, None, None).await.unwrap();
    assert!(coordinates(&reopened).await.unwrap() == failed);
    reopened.close().await.unwrap();
    workspace.cleanup().unwrap();
}
