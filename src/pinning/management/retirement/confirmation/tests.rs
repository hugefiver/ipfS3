use super::*;

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
