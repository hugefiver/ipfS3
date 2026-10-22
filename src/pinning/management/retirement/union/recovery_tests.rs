use super::*;

#[tokio::test]
async fn rejected_fact_page_keeps_reads_when_derived_changes_roll_back() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&path, file).await.unwrap();
    union.db.execute_unprepared("CREATE TABLE facts(kind TEXT NOT NULL,identity TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(kind,identity)); INSERT INTO facts VALUES('current','remote','not-json')").await.unwrap();
    let limits = RunLimits::default();
    let run = super::super::spool::task_run(&union.db, &limits)
        .await
        .unwrap();
    let rows = work::facts(&union.db, &union.db, "current", "", &run)
        .await
        .unwrap();
    let consumed = union.task_work().await.unwrap();
    assert!(consumed.rows > 0 && consumed.bytes > 0 && consumed.pages > 0);
    let tx = union.db.begin().await.unwrap();
    exec(&tx, "INSERT INTO cids VALUES('partial')", vec![])
        .await
        .unwrap();
    let outcome =
        decode::<RemoteSnapshot>(&get::<String>(&rows[0], "payload").unwrap()).map(|_| ());
    assert!(matches!(
        work::commit(tx, outcome).await,
        Err(ManagementError::InvalidEvidence)
    ));
    assert!(
        one(&union.db, "SELECT cid FROM cids", vec![])
            .await
            .unwrap()
            .is_none()
    );
    union.close().await.unwrap();
    let union = PlanUnion::resume_task(&path).await.unwrap();
    assert_eq!(union.task_work().await.unwrap(), consumed);
    union.close().await.unwrap();
    workspace.cleanup().unwrap();
}

#[tokio::test]
async fn failed_output_page_keeps_consumed_work_after_reopening() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&path, file).await.unwrap();
    exec(
        &union.db,
        "INSERT INTO quotes VALUES('remote','key','cid','not-a-number',0,0,0,0)",
        vec![],
    )
    .await
    .unwrap();
    super::super::cache_guard::install(&union.db, &["quotes", "output"])
        .await
        .unwrap();
    let limits = RunLimits::default();
    assert!(matches!(
        union.finish(Summary::default(), &limits).await,
        Err(ManagementError::InvalidEvidence)
    ));
    let failed = union.task_work().await.unwrap();
    assert!(failed.rows > 0 && failed.bytes > 0 && failed.pages > 0);
    let state = one(
        &union.db,
        "SELECT after_key,stage FROM output WHERE id=1",
        vec![],
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(get::<String>(&state, "after_key").unwrap(), "");
    assert_eq!(get::<i64>(&state, "stage").unwrap(), 0);
    union.close().await.unwrap();
    let union = PlanUnion::resume_task(&path).await.unwrap();
    assert_eq!(union.task_work().await.unwrap(), failed);
    assert!(matches!(
        union
            .finish(Summary::default(), &RunLimits::default())
            .await,
        Err(ManagementError::InvalidEvidence)
    ));
    let retried = union.task_work().await.unwrap();
    assert!(
        retried.rows > failed.rows && retried.bytes > failed.bytes && retried.pages > failed.pages
    );
    exec(&union.db, "UPDATE quotes SET bytes='99'", vec![])
        .await
        .unwrap();
    assert!(matches!(
        union
            .finish(Summary::default(), &RunLimits::default())
            .await,
        Err(ManagementError::Changed)
    ));
    assert!(matches!(
        super::super::cache_guard::check(&union.db).await,
        Err(ManagementError::Changed)
    ));
    union.close().await.unwrap();
    workspace.cleanup().unwrap();
}

#[tokio::test]
async fn larger_resume_limits_do_not_expand_the_canonical_union_envelope() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spools = vec![InventorySpool::allocate(&path, file, None).await.unwrap()];
    let mut small = RunLimits::default();
    small.total_rows = 8;
    small.total_bytes = 2048;
    let run = super::super::spool::task_run(&spools[0].db, &small)
        .await
        .unwrap();
    super::super::spool::budget::charge(&spools[0].db, &run, 5, 0)
        .await
        .unwrap();
    let (original_path, file) = workspace.reserve().await.unwrap();
    let original = PlanUnion::create(&original_path, file).await.unwrap();
    original.start_task(&small).await.unwrap();
    let task = super::super::task_budget::TaskBudget::new(&spools, &original, (8, 2048));
    let (canonical_path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&canonical_path, file).await.unwrap();
    exec(
        &union.db,
        "ATTACH DATABASE $1 AS original",
        vec![path.to_str().unwrap().into()],
    )
    .await
    .unwrap();
    let large = RunLimits::default();
    union
        .start_task(&task.envelope_limits(&large))
        .await
        .unwrap();
    exec(&union.db, "INSERT INTO cids VALUES('cid')", vec![])
        .await
        .unwrap();
    let caps = task.union_caps(&union, &large).await.unwrap();
    assert_eq!(caps.total_rows, 3);
    assert!(matches!(
        union.finish(Summary::default(), &caps).await,
        Err(ManagementError::Budget)
    ));
    let consumed = union.task_work().await.unwrap();
    assert!(consumed.rows <= 3);
    super::super::task_budget::TaskBudget::forward_cache(&union.db, true)
        .await
        .unwrap();
    let total = task.total().await.unwrap();
    assert_eq!(total.rows, 5 + consumed.rows);
    assert!(total.rows <= 8 && total.bytes <= 2048);
    union.close().await.unwrap();
    let union = PlanUnion::resume_task(&canonical_path).await.unwrap();
    exec(
        &union.db,
        "ATTACH DATABASE $1 AS original",
        vec![path.to_str().unwrap().into()],
    )
    .await
    .unwrap();
    super::super::task_budget::TaskBudget::forward_cache(&union.db, true)
        .await
        .unwrap();
    assert_eq!(task.total().await.unwrap(), total);
    union.close().await.unwrap();
    original.close().await.unwrap();
    spools[0].close().await.unwrap();
    workspace.cleanup().unwrap();
}
