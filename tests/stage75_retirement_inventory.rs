use ipfs_s3_gateway::{
    pinning::{
        identity::{CleanupMode, ProviderRouteSnapshot},
        management::{
            self,
            retirement::{ArchiveCursor, InventorySpool, Progress, RunLimits, SourceCheckpoint},
        },
    },
    store::{self, pinning::management::ManagementError},
};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement, TransactionTrait};
use std::{collections::BTreeMap, time::Duration};

fn route(provider: &str, scope: &str) -> ProviderRouteSnapshot {
    ProviderRouteSnapshot {
        provider_id: provider.into(),
        backend: "noop".into(),
        scope: scope.into(),
        storage_domain: "remote".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: Some("env:PRIVATE".into()),
        api_profile: "noop".into(),
        strategy: "cid".into(),
        cleanup: CleanupMode::Retain,
        confirmation: None,
    }
}
fn config(route: ProviderRouteSnapshot) -> management::ConfigurationBinding {
    management::ConfigurationBinding {
        routes: vec![route],
        policies: vec![],
        limits: BTreeMap::new(),
        zip_rules: None,
        source_node: None,
    }
}
fn limits() -> RunLimits {
    let mut limits = RunLimits::default();
    limits.pages = 512;
    limits.elapsed = Duration::from_secs(120);
    limits
}
fn reserve(path: &std::path::Path) -> std::fs::File {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).unwrap()
}

async fn checkpoint_snapshot(
    db: &sea_orm::DatabaseConnection,
    selected: &ProviderRouteSnapshot,
) -> management::Snapshot {
    use store::pinning::management::native::{
        meter::Budget,
        scope::{ScopeSpec, Selector, collect_scope},
    };
    let tx = db.begin().await.unwrap();
    let mut budget = Budget::default();
    let read = collect_scope(
        &tx,
        &ScopeSpec::new(vec![Selector::Exact(management::NativeKey {
            table: "pin_provider_routes".into(),
            values: vec!["selected".into()],
        })])
        .unwrap(),
        &mut budget,
    )
    .await
    .unwrap();
    let frame = read.into_native_frame(&mut budget).unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        management::RouteIdentity::from_original(selected.clone()),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    management::Snapshot {
        observed_at: checkpoint.observed_at.timestamp(),
        database_now: Some(checkpoint.observed_at),
        native_frame: Some(frame),
        configuration: Some(config(selected.clone())),
        ..Default::default()
    }
}

fn checkpoint_request(selected: &ProviderRouteSnapshot) -> management::PlanRequest {
    management::PlanRequest {
        plan_id: "retained-planning".into(),
        items: vec![management::ItemRequest {
            item_id: "retire".into(),
            action: management::Action::Retire,
            subject: management::Subject::Provider {
                route: management::RouteIdentity::from_original(selected.clone()),
            },
        }],
    }
}
fn planning_slice() -> RunLimits {
    let mut limits = RunLimits::default();
    limits.pages = 1;
    limits
}

#[tokio::test]
async fn shared_inventories_reject_mismatched_originals() {
    use management::retirement::{ArchiveCursor, Workspace};
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let selected = route("selected", "selected");
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES('selected',$1,'selected',0)",
        [serde_json::to_string(&selected).unwrap().into()],
    ))
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        management::RouteIdentity::from_original(selected.clone()),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let mut workspace = Workspace::create().unwrap();
    let (left_path, file) = workspace.reserve().await.unwrap();
    let left = InventorySpool::create(&left_path, file, &checkpoint)
        .await
        .unwrap();
    let (right_path, file) = workspace.reserve().await.unwrap();
    let mut right = InventorySpool::create(&right_path, file, &checkpoint)
        .await
        .unwrap();
    for spool in [&left, &right] {
        while spool.scan(&db, &RunLimits::default()).await.unwrap() == Progress::Pending {}
        while spool
            .reduce(&config(selected.clone()), &RunLimits::default())
            .await
            .unwrap()
            == Progress::Pending
        {}
        spool.frozen_review().await.unwrap();
    }
    assert!(left.checkpoint().await.unwrap() == right.checkpoint().await.unwrap());
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&right_path)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Delete),
        )
        .await
        .unwrap();
    let editing = sea_orm::SqlxSqliteConnector::from_sqlx_sqlite_pool(pool);
    let changed=editing.execute_unprepared("UPDATE parts SET bytes=CAST('changed' AS BLOB) WHERE ordinal=(SELECT MIN(ordinal) FROM parts)").await.unwrap();
    assert_eq!(changed.rows_affected(), 1);
    editing.close().await.unwrap();
    assert!(
        left.read_part(&mut ArchiveCursor::default()).await.unwrap()
            != right
                .read_part(&mut ArchiveCursor::default())
                .await
                .unwrap()
    );
    let result = right
        .verify_shared_source(&left, &RunLimits::default())
        .await;
    left.close().await.unwrap();
    right.close().await.unwrap();
    workspace.cleanup().unwrap();
    assert_eq!(result, Err(ManagementError::Changed));
}

#[tokio::test]
async fn planning_checkpoint_reopens_source_and_task_with_frozen_clocks_and_no_authority_prefix() {
    use management::retirement::{PlanningCheckpoint, PlanningProgress};
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.sqlite");
    let mut db = Database::connect(format!("sqlite:{}?mode=rwc", source.display()))
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    let selected = route("selected", "selected");
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES('selected',$1,'selected',0)",
        [serde_json::to_string(&selected).unwrap().into()],
    ))
    .await
    .unwrap();
    db.close().await.unwrap();
    db = Database::connect(format!("sqlite:{}?mode=ro", source.display()))
        .await
        .unwrap();
    let local = checkpoint_snapshot(&db, &selected).await;
    let request = checkpoint_request(&selected);
    let mut future = local.clone();
    future.database_now = future
        .database_now
        .map(|now| now + chrono::Duration::hours(1));
    future.observed_at = future.database_now.unwrap().timestamp();
    assert!(matches!(
        PlanningCheckpoint::create(
            &directory.path().join("future-clock"),
            &db,
            "operator",
            &request,
            &future,
            &RunLimits::default()
        )
        .await,
        Err(ManagementError::InvalidEvidence)
    ));
    let path = directory.path().join("planning-task");
    let mut task = PlanningCheckpoint::create(
        &path,
        &db,
        "operator",
        &request,
        &local,
        &RunLimits::default(),
    )
    .await
    .unwrap();
    let full_path = task.path().unwrap().to_owned();
    assert!(full_path.is_absolute());
    let frozen_now = task.snapshot().database_now;
    let mut resumes = 0;
    let prepared =
        loop {
            let one = planning_slice();
            match task.advance(&db, &one).await.unwrap() {
                PlanningProgress::Complete(prepared) => break prepared,
                PlanningProgress::Incomplete => {
                    let before = task.task_work().await.unwrap();
                    assert_eq!(task.snapshot().database_now, frozen_now);
                    assert_eq!(task.close().await.unwrap(), full_path);
                    db.close().await.unwrap();
                    db = Database::connect(format!("sqlite:{}?mode=ro", source.display()))
                        .await
                        .unwrap();
                    let input = PlanningCheckpoint::input(&full_path).await.unwrap();
                    assert!(input.request == request);
                    assert!(input.configuration == config(selected.clone()));
                    let fresh = checkpoint_snapshot(&db, &selected).await;
                    task = PlanningCheckpoint::resume(
                        &full_path,
                        &db,
                        "operator",
                        &request,
                        &fresh,
                        &RunLimits::default(),
                    )
                    .await
                    .unwrap();
                    let after = task.task_work().await.unwrap();
                    assert!(after.iter().zip(&before).all(|(a, b)| a.rows >= b.rows
                        && a.bytes >= b.bytes
                        && a.pages >= b.pages));
                    resumes += 1;
                    assert!(
                        resumes < 128,
                        "checkpoint must make bounded cross-object progress"
                    );
                }
            }
        };
    assert!(resumes > 1);
    assert_eq!(prepared.checkpoint_path(), Some(full_path.as_path()));
    assert!(
        prepared
            .plan
            .items()
            .iter()
            .all(|item| item.status == management::PlanStatus::Ready)
    );
    let plan = prepared.plan.clone();
    prepared.close().await.unwrap();
    assert!(PlanningCheckpoint::input(&full_path).await.is_ok());
    let fresh = checkpoint_snapshot(&db, &selected).await;
    let mut task = PlanningCheckpoint::resume(
        &full_path,
        &db,
        "operator",
        &request,
        &fresh,
        &RunLimits::default(),
    )
    .await
    .unwrap();
    let prepared = match task.advance(&db, &RunLimits::default()).await.unwrap() {
        PlanningProgress::Complete(prepared) => prepared,
        PlanningProgress::Incomplete => {
            panic!("completed retained task must recover the original plan")
        }
    };
    assert!(prepared.plan == plan);
    prepared.cleanup().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn planning_checkpoint_shared_originals_resume_across_single_page_invocations() {
    use management::retirement::{PlanningCheckpoint, PlanningProgress};
    let directory = tempfile::tempdir().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let selected = route("selected", "selected");
    let other = route("other", "other");
    for original in [&selected, &other] {
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO pin_provider_routes VALUES($1,$2,$1,0)",
            [
                original.provider_id.clone().into(),
                serde_json::to_string(original).unwrap().into(),
            ],
        ))
        .await
        .unwrap();
    }
    for index in 0..300 {
        let original = route(&format!("foreign-{index:04}"), "foreign");
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO pin_provider_routes VALUES($1,$2,$1,0)",
            [
                original.provider_id.clone().into(),
                serde_json::to_string(&original).unwrap().into(),
            ],
        ))
        .await
        .unwrap();
    }
    let mut request = checkpoint_request(&selected);
    request.items.push(management::ItemRequest {
        item_id: "other".into(),
        action: management::Action::Retire,
        subject: management::Subject::Provider {
            route: management::RouteIdentity::from_original(other.clone()),
        },
    });
    let mut local = checkpoint_snapshot(&db, &selected).await;
    local.configuration.as_mut().unwrap().routes.push(other);
    let path = directory.path().join("shared-task");
    let mut task = PlanningCheckpoint::create(
        &path,
        &db,
        "operator",
        &request,
        &local,
        &RunLimits::default(),
    )
    .await
    .unwrap();
    let mut resumes = 0;
    loop {
        match task.advance(&db, &planning_slice()).await.unwrap() {
            PlanningProgress::Complete(prepared) => {
                assert_eq!(prepared.plan.items().len(), 2);
                prepared.cleanup().await.unwrap();
                break;
            }
            PlanningProgress::Incomplete => {
                let path = task.close().await.unwrap();
                let mut fresh = checkpoint_snapshot(&db, &selected).await;
                fresh.configuration = local.configuration.clone();
                task = PlanningCheckpoint::resume(
                    &path,
                    &db,
                    "operator",
                    &request,
                    &fresh,
                    &RunLimits::default(),
                )
                .await
                .unwrap();
                resumes += 1;
                assert!(
                    resumes < 256,
                    "shared raw/member comparison must retain its bounded progress"
                );
            }
        }
    }
    assert!(resumes > 2);
}

#[tokio::test]
async fn planning_checkpoint_cancellation_drift_and_unknown_files_never_publish_or_delete() {
    use management::retirement::{PlanningCheckpoint, PlanningProgress};
    let directory = tempfile::tempdir().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let selected = route("selected", "selected");
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES('selected',$1,'selected',0)",
        [serde_json::to_string(&selected).unwrap().into()],
    ))
    .await
    .unwrap();
    let request = checkpoint_request(&selected);
    let local = checkpoint_snapshot(&db, &selected).await;
    let path = directory.path().join("planning-task");
    let mut task = PlanningCheckpoint::create(
        &path,
        &db,
        "operator",
        &request,
        &local,
        &RunLimits::default(),
    )
    .await
    .unwrap();
    let cancelled = RunLimits::default();
    cancelled.cancellation.cancel();
    assert!(matches!(
        task.advance(&db, &cancelled).await,
        Err(ManagementError::Budget)
    ));
    assert!(matches!(
        task.advance(&db, &planning_slice()).await.unwrap(),
        PlanningProgress::Incomplete
    ));
    db.execute_unprepared(
        "UPDATE pin_provider_routes SET display_name='changed' WHERE provider='selected'",
    )
    .await
    .unwrap();
    assert!(matches!(
        task.advance(&db, &RunLimits::default()).await,
        Err(ManagementError::Changed)
    ));
    let full_path = task.close().await.unwrap();
    let fresh = checkpoint_snapshot(&db, &selected).await;
    assert!(matches!(
        PlanningCheckpoint::resume(
            &full_path,
            &db,
            "operator",
            &request,
            &fresh,
            &RunLimits::default()
        )
        .await,
        Err(ManagementError::Changed)
    ));
    assert!(PlanningCheckpoint::input(&full_path).await.is_ok());
    let second = directory.path().join("cleanup-task");
    let existing = directory.path().join("existing");
    std::fs::create_dir(&existing).unwrap();
    let canary = existing.join("canary");
    std::fs::write(&canary, b"existing operator content").unwrap();
    assert!(
        PlanningCheckpoint::create(
            &existing,
            &db,
            "operator",
            &request,
            &fresh,
            &RunLimits::default()
        )
        .await
        .is_err()
    );
    assert_eq!(
        std::fs::read(&canary).unwrap(),
        b"existing operator content"
    );
    let missing = directory.path().join("missing-parent");
    assert!(
        PlanningCheckpoint::create(
            &missing.join("task"),
            &db,
            "operator",
            &request,
            &fresh,
            &RunLimits::default()
        )
        .await
        .is_err()
    );
    assert!(!missing.exists());
    let task = PlanningCheckpoint::create(
        &second,
        &db,
        "operator",
        &request,
        &fresh,
        &RunLimits::default(),
    )
    .await
    .unwrap();
    let unknown = second.join("operator-note");
    std::fs::write(&unknown, b"preserve this unrelated file").unwrap();
    assert!(matches!(
        task.cleanup().await,
        Err(ManagementError::InvalidEvidence)
    ));
    assert_eq!(
        std::fs::read(&unknown).unwrap(),
        b"preserve this unrelated file"
    );
    assert!(PlanningCheckpoint::input(&second).await.is_ok());
}

#[tokio::test]
async fn planning_checkpoint_task_budget_survives_reopen_with_larger_invocation_limits() {
    use management::retirement::{PlanningCheckpoint, PlanningProgress};
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("quota-source.sqlite");
    let mut db = Database::connect(format!("sqlite:{}?mode=rwc", source.display()))
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    let selected = route("selected", "selected");
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES('selected',$1,'selected',0)",
        [serde_json::to_string(&selected).unwrap().into()],
    ))
    .await
    .unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES('opaque',$1,'opaque',0)",
        ["x".repeat(2 * 1024 * 1024).into()],
    ))
    .await
    .unwrap();
    let request = checkpoint_request(&selected);
    let local = checkpoint_snapshot(&db, &selected).await;
    let mut quota = planning_slice();
    quota.total_bytes = 1536 * 1024;
    let path = directory.path().join("quota-task");
    let mut task = PlanningCheckpoint::create(&path, &db, "operator", &request, &local, &quota)
        .await
        .unwrap();
    loop {
        match task.advance(&db, &planning_slice()).await {
            Ok(PlanningProgress::Incomplete) => continue,
            Err(ManagementError::Budget) => break,
            _ => panic!("an incomplete over-quota task must not publish a plan"),
        }
    }
    let before = task.task_work().await.unwrap();
    let full_path = task.close().await.unwrap();
    db.close().await.unwrap();
    db = Database::connect(format!("sqlite:{}?mode=rw", source.display()))
        .await
        .unwrap();
    let fresh = checkpoint_snapshot(&db, &selected).await;
    let mut task = PlanningCheckpoint::resume(
        &full_path,
        &db,
        "operator",
        &request,
        &fresh,
        &RunLimits::default(),
    )
    .await
    .unwrap();
    let after = task.task_work().await.unwrap();
    assert!(after.iter().zip(before).all(|(a, b)| a.bytes >= b.bytes));
    assert!(matches!(
        task.advance(&db, &RunLimits::default()).await,
        Err(ManagementError::Budget)
    ));
    task.cleanup().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn current_retirement_status_rebuilds_after_source_changes() {
    use management::retirement::CurrentRetirementScan;
    let directory = tempfile::tempdir().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let selected = route("selected", "selected");
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES ('selected',$1,'selected',0)",
        [serde_json::to_string(&selected).unwrap().into()],
    ))
    .await
    .unwrap();
    let identity = management::RouteIdentity::from_original(selected.clone());
    let path = directory.path().join("status-before.sqlite");
    let mut scan = CurrentRetirementScan::start(
        &db,
        identity.clone(),
        config(selected.clone()),
        &path,
        reserve(&path),
        &limits(),
    )
    .await
    .unwrap();
    let before = loop {
        if let Some(status) = scan.advance(&db, &limits()).await.unwrap() {
            break status;
        }
    };
    assert_eq!(before.responsibility.unknown, 0);
    db.execute_unprepared("INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES('selected','bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku',10,'pinning',1,0,CURRENT_TIMESTAMP)").await.unwrap();
    assert!(matches!(
        scan.advance(&db, &limits()).await,
        Err(ManagementError::InvalidEvidence)
    ));
    let path = directory.path().join("status-after.sqlite");
    let mut scan = CurrentRetirementScan::start(
        &db,
        identity,
        config(selected),
        &path,
        reserve(&path),
        &limits(),
    )
    .await
    .unwrap();
    let after = loop {
        if let Some(status) = scan.advance(&db, &limits()).await.unwrap() {
            break status;
        }
    };
    assert!(after.responsibility.needs_attention);
    assert!(after.responsibility.unknown > before.responsibility.unknown);
    assert!(after.observed_at >= before.observed_at);
}

#[tokio::test]
async fn retirement_self_transition_requires_exact_rows_and_revision_deltas() {
    use management::retirement::SourceTransition;
    use store::pinning::management::{
        native::{
            meter::Budget,
            scope::{ScopeSpec, Selector, collect_scope},
        },
        transaction::ManagementTransaction,
    };
    let directory = tempfile::tempdir().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let selected = route("selected", "selected");
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES ('selected',$1,'selected',0)",
        [serde_json::to_string(&selected).unwrap().into()],
    ))
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        management::RouteIdentity::from_original(selected.clone()),
    )
    .await
    .unwrap();
    let key = management::NativeKey {
        table: "pin_provider_routes".into(),
        values: vec!["selected".into()],
    };
    let rows = collect_scope(
        &tx,
        &ScopeSpec::new(vec![Selector::Exact(key.clone())]).unwrap(),
        &mut Budget::default(),
    )
    .await
    .unwrap();
    let before = rows.rows().get(&key).unwrap().clone();
    tx.rollback().await.unwrap();
    let path = directory.path().join("transition.sqlite");
    let spool = InventorySpool::create(&path, reserve(&path), &checkpoint)
        .await
        .unwrap();
    while spool.scan(&db, &limits()).await.unwrap() == Progress::Pending {}
    let configuration = config(selected);
    while spool.reduce(&configuration, &limits()).await.unwrap() == Progress::Pending {}
    let review = spool.frozen_review().await.unwrap();
    for extra_write in [false, true] {
        let change = SourceTransition {
            key: key.clone(),
            before: Some(before.clone()),
            after: Some(before.clone()),
            writes: 1,
        };
        let fence = review.prepare_fence().unwrap();
        let mut guarded =
            ManagementTransaction::begin(&db, Duration::from_secs(5), Default::default())
                .await
                .unwrap();
        let result = guarded.run(move |tx, _| Box::pin(async move {
            let limits = limits();
            let mut budget = Budget::default();
            let transition = fence.begin_transition(tx, vec![change], &mut budget, &limits).await?;
            tx.execute_unprepared("UPDATE pin_provider_routes SET display_name=display_name WHERE provider='selected'").await.map_err(|_| ManagementError::Database)?;
            if extra_write {
                tx.execute_unprepared("INSERT INTO pin_provider_routes VALUES ('extra-same-plan','{}','extra',0)").await.map_err(|_| ManagementError::Database)?;
            }
            transition.finish(tx, &mut budget, &limits).await
        })).await;
        if extra_write {
            let failure = result.err().unwrap();
            assert_eq!(failure.management_error(), ManagementError::Changed);
            assert!(failure.rollback_confirmed);
        } else {
            let next = result.unwrap();
            assert_eq!(
                next.revisions["pin_provider_routes"],
                checkpoint.revisions["pin_provider_routes"] + 1
            );
            guarded.rollback().await.unwrap();
        }
    }
    let tx = db.begin().await.unwrap();
    let change = SourceTransition {
        key,
        before: Some(before.clone()),
        after: Some(before),
        writes: 1,
    };
    assert!(matches!(
        review
            .prepare_fence()
            .unwrap()
            .begin_transition(
                &tx,
                vec![change.clone(), change],
                &mut Budget::default(),
                &limits()
            )
            .await,
        Err(ManagementError::InvalidEvidence)
    ));
    tx.rollback().await.unwrap();
    let tx = db.begin().await.unwrap();
    review.seal(&tx, &limits()).await.unwrap();
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn retirement_archives_more_than_4096_rows_and_preserves_long_private_originals() {
    let directory = tempfile::tempdir().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let selected = route("selected", "selected");
    let other = route("other", "other");
    let long = format!("  {}  ", "私密".repeat(40000));
    let mut private = other.clone();
    private.secret_ref = Some(long.clone());
    let original = serde_json::to_string_pretty(&private).unwrap();
    let tx = db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES ('selected',$1,'selected',0)",
        [serde_json::to_string(&selected).unwrap().into()],
    ))
    .await
    .unwrap();
    for n in 0..4100 {
        tx.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO pin_provider_routes VALUES ($1,$2,$3,0)",
            vec![
                format!("other-{n:05}").into(),
                if n == 0 {
                    original.clone()
                } else {
                    serde_json::to_string(&selected).unwrap()
                }
                .into(),
                String::new().into(),
            ],
        ))
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    let tx = db.begin().await.unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        management::RouteIdentity::from_original(selected.clone()),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let path = directory.path().join("inventory.sqlite");
    let spool = InventorySpool::create(&path, reserve(&path), &checkpoint)
        .await
        .unwrap();
    while spool.scan(&db, &limits()).await.unwrap() == Progress::Pending {}
    let mut cursor = ArchiveCursor::default();
    let mut restored = Vec::new();
    let mut saw_empty = false;
    let mut saw_null = false;
    let mut rows = 0usize;
    while let Some(part) = spool.read_part(&mut cursor).await.unwrap() {
        assert!(part.bytes.len() <= 64 * 1024);
        if part.length.is_none() {
            assert!(part.bytes.is_empty());
            saw_null = true;
        }
        if part.table == "pin_provider_routes" && part.column == "provider" {
            rows += 1;
        }
        if part.table == "pin_provider_routes"
            && part.column == "snapshot"
            && part.key.values == ["other-00000"]
        {
            restored.extend(part.bytes);
        } else if part.table == "pin_provider_routes"
            && part.column == "display_name"
            && part.key.values == ["other-00001"]
        {
            saw_empty = part.length == Some(0) && part.bytes.is_empty();
        }
    }
    assert_eq!(rows, 4101);
    assert_eq!(restored, original.as_bytes());
    assert!(saw_empty);
    assert!(saw_null);
    while spool
        .reduce(&config(selected.clone()), &limits())
        .await
        .unwrap()
        == Progress::Pending
    {}
    let request = management::PlanRequest {
        plan_id: "frozen-retirement".into(),
        items: vec![management::ItemRequest {
            item_id: "retire".into(),
            action: management::Action::Retire,
            subject: management::Subject::Provider {
                route: management::RouteIdentity::from_original(selected.clone()),
            },
        }],
    };
    let local = management::Snapshot {
        observed_at: checkpoint.observed_at.timestamp(),
        database_now: Some(checkpoint.observed_at),
        configuration: Some(config(selected.clone())),
        ..Default::default()
    };
    let union_path = directory.path().join("union.sqlite");
    let union = management::retirement::PlanUnion::create(&union_path, reserve(&union_path))
        .await
        .unwrap();
    let plan = loop {
        if let Some(plan) = management::dry_run_retirement(
            &request,
            &local,
            &[spool.frozen_review().await.unwrap()],
            &[],
            &union,
            &limits(),
        )
        .await
        .unwrap()
        {
            break plan;
        }
    };
    let refs = spool
        .frozen_review()
        .await
        .unwrap()
        .stage_evidence(plan.plan_id(), &plan.items()[0])
        .await
        .unwrap();
    let canonical = directory.path().join("canonical.v3");
    write_canonical(
        &spool,
        &checkpoint,
        reserve(&canonical),
        management::wire::v3::Projection::PrivateRecovery,
        &plan,
    )
    .await;
    verify_canonical(&canonical, original.as_bytes());
    let cursor = management::wire::v3::archive_cursor::stage_import(
        std::fs::File::open(&canonical).unwrap(),
        management::wire::v3::Projection::PrivateRecovery,
        archive_budget(),
    )
    .unwrap()
    .into_cursor();
    let imported_path = directory.path().join("wire-restored.sqlite");
    let mut importer = management::retirement::FrozenRetirementImport::create(
        cursor,
        &imported_path,
        reserve(&imported_path),
        spool.reference().clone(),
    )
    .await
    .unwrap();
    while importer.advance(&limits()).await.unwrap() == Progress::Pending {}
    let (imported, _canonical, _budget) = importer.into_parts().unwrap();
    let capture = imported
        .restore_capture(plan.plan_id(), &request.items[0], &refs)
        .await
        .unwrap();
    assert!(
        plan.items()[0].capture.as_ref() == Some(&management::Capture::Provider(Box::new(capture)))
    );
    assert!(
        imported.frozen_review().await.unwrap().totals()
            == spool.frozen_review().await.unwrap().totals()
    );
    let (mut before, mut after) = (ArchiveCursor::default(), ArchiveCursor::default());
    loop {
        let expected = spool.read_part(&mut before).await.unwrap();
        let actual = imported.read_part(&mut after).await.unwrap();
        assert!(expected == actual);
        if expected.is_none() {
            break;
        }
    }
    let imported_review = imported.frozen_review().await.unwrap();
    let tx = db.begin().await.unwrap();
    assert!(matches!(
        imported_review.seal(&tx, &limits()).await,
        Err(ManagementError::InvalidEvidence)
    ));
    tx.rollback().await.unwrap();
    let public = directory.path().join("review.v3");
    write_canonical(
        &spool,
        &checkpoint,
        reserve(&public),
        management::wire::v3::Projection::Review,
        &plan,
    )
    .await;
    verify_review(&public);
    let review = spool.frozen_review().await.unwrap();
    assert_eq!(review.totals().current_resources, 0);
    assert!(!review.step().needs_attention);
    let retirement = review
        .capture(
            &management::ItemRequest {
                item_id: "retire".into(),
                subject: management::Subject::Provider {
                    route: management::RouteIdentity::from_original(selected.clone()),
                },
                action: management::Action::Retire,
            },
            management::retirement::RetirementMutationScope::default(),
        )
        .await
        .unwrap();
    assert_eq!(retirement.mutation_scope.routes.len(), 1);
    let tx = db.begin().await.unwrap();
    review.seal(&tx, &limits()).await.unwrap();
    let cancelled = limits();
    cancelled.cancellation.cancel();
    assert!(matches!(
        review.seal(&tx, &cancelled).await,
        Err(ManagementError::Budget)
    ));
    assert_eq!(
        spool.scan(&db, &cancelled).await.unwrap_err(),
        ManagementError::Budget
    );
    assert_eq!(
        spool
            .reduce(&config(selected.clone()), &cancelled)
            .await
            .unwrap_err(),
        ManagementError::Budget
    );
    tx.rollback().await.unwrap();
    db.execute_unprepared("UPDATE pin_provider_routes SET retired=1 WHERE provider='selected'")
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    assert!(matches!(
        review.seal(&tx, &limits()).await,
        Err(ManagementError::Changed)
    ));
    tx.rollback().await.unwrap();
    // A fresh mutation must not rewrite the original frozen review.
    assert!(!spool.frozen_review().await.unwrap().step().needs_attention);
    let path = directory.path().join("frozen-rebuild.sqlite");
    let mut restored = InventorySpool::create(&path, reserve(&path), &checkpoint.rewind())
        .await
        .unwrap();
    while restored.reconstruct(&spool, &limits()).await.unwrap() == Progress::Pending {}
    while restored
        .reduce(&config(selected.clone()), &limits())
        .await
        .unwrap()
        == Progress::Pending
    {}
    assert!(
        restored.frozen_review().await.unwrap().totals()
            == spool.frozen_review().await.unwrap().totals()
    );
    let tx = db.begin().await.unwrap();
    assert!(matches!(
        restored
            .frozen_review()
            .await
            .unwrap()
            .seal(&tx, &limits())
            .await,
        Err(ManagementError::InvalidEvidence)
    ));
    tx.rollback().await.unwrap();
}

fn archive_budget() -> store::pinning::management::native::meter::Budget {
    use store::pinning::management::native::meter::{Budget, Limits};
    Budget::new(Limits {
        rows: 100_000,
        bytes: 128 * 1024 * 1024,
        sql: 1_000_000,
        resident: 16 * 1024 * 1024,
        elapsed: Duration::from_secs(120),
    })
}

#[tokio::test]
async fn frozen_m23_inventory_import_and_reconstruction_do_not_renew_checkpoint() {
    use management::wire::v3::{self, Projection};
    use store::migrations::history;
    let directory = tempfile::tempdir().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let selected = route("selected", "selected");
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES('selected',$1,'selected',0)",
        [serde_json::to_string(&selected).unwrap().into()],
    ))
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        management::RouteIdentity::from_original(selected.clone()),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let path = directory.path().join("m23.sqlite");
    let spool = InventorySpool::create(&path, reserve(&path), &checkpoint)
        .await
        .unwrap();
    while spool.scan(&db, &limits()).await.unwrap() == Progress::Pending {}
    // Construct a historical-layout fixture before producing any archive. Cells
    // are unchanged: old m23 and baseline share this complete physical catalog.
    let mut old = spool.checkpoint().await.unwrap();
    old.catalog_revision = history::M23.into();
    old.catalog = history::VERSIONS.iter().map(|s| (*s).into()).collect();
    let editing = Database::connect(format!(
        "sqlite://{}?mode=rw",
        path.to_string_lossy().replace('\\', "/")
    ))
    .await
    .unwrap();
    editing
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE state SET checkpoint=$1 WHERE id=1",
            [serde_json::to_string(&old).unwrap().into()],
        ))
        .await
        .unwrap();
    editing.close().await.unwrap();
    while spool
        .reduce(&config(selected.clone()), &limits())
        .await
        .unwrap()
        == Progress::Pending
    {}
    let rebuilt_path = directory.path().join("rebuilt.sqlite");
    let mut rebuilt = InventorySpool::create(&rebuilt_path, reserve(&rebuilt_path), &old.rewind())
        .await
        .unwrap();
    while rebuilt.reconstruct(&spool, &limits()).await.unwrap() == Progress::Pending {}
    assert!(rebuilt.checkpoint().await.unwrap() == old);
    let request = checkpoint_request(&selected);
    let local = management::Snapshot {
        observed_at: old.observed_at.timestamp(),
        database_now: Some(old.observed_at),
        configuration: Some(config(selected)),
        ..Default::default()
    };
    let union_path = directory.path().join("union.sqlite");
    let union = management::retirement::PlanUnion::create(&union_path, reserve(&union_path))
        .await
        .unwrap();
    let plan = loop {
        if let Some(plan) = management::dry_run_retirement(
            &request,
            &local,
            &[spool.frozen_review().await.unwrap()],
            &[],
            &union,
            &limits(),
        )
        .await
        .unwrap()
        {
            break plan;
        }
    };
    let archive = directory.path().join("historical.v3");
    write_canonical(
        &spool,
        &old,
        reserve(&archive),
        Projection::PrivateRecovery,
        &plan,
    )
    .await;
    let cursor = v3::archive_cursor::stage_import(
        std::fs::File::open(&archive).unwrap(),
        Projection::PrivateRecovery,
        archive_budget(),
    )
    .unwrap()
    .into_cursor();
    let imported_path = directory.path().join("imported.sqlite");
    let mut importer = management::retirement::FrozenRetirementImport::create(
        cursor,
        &imported_path,
        reserve(&imported_path),
        spool.reference().clone(),
    )
    .await
    .unwrap();
    while importer.advance(&limits()).await.unwrap() == Progress::Pending {}
    let (imported, _, _) = importer.into_parts().unwrap();
    assert!(imported.checkpoint().await.unwrap() == old);
    let tx = db.begin().await.unwrap();
    assert_eq!(old.check(&tx).await, Err(ManagementError::Changed));
    assert!(
        imported
            .frozen_review()
            .await
            .unwrap()
            .seal(&tx, &limits())
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    imported.close().await.unwrap();
    rebuilt.close().await.unwrap();
    spool.close().await.unwrap();
    union.close().await.unwrap();
}
async fn write_canonical(
    spool: &InventorySpool,
    checkpoint: &SourceCheckpoint,
    file: std::fs::File,
    projection: management::wire::v3::Projection,
    plan: &management::DryRunPlan,
) {
    use management::wire::v3::{AppliedTrust, Frame, SCHEMA_VERSION, Writer};
    let mut writer = Writer::new(file, projection, archive_budget()).unwrap();
    writer
        .frame(&Frame::Header {
            schema_version: SCHEMA_VERSION,
            projection,
            database_scope: checkpoint.database_scope.clone(),
            database_identity: checkpoint.database_identity.clone(),
            principal: "operator".into(),
            applied_trust: AppliedTrust::UntrustedClaimsOnly,
            catalog: checkpoint.catalog.clone(),
            created_at: checkpoint.observed_at.to_rfc3339(),
            expires_at: None,
        })
        .unwrap();
    writer
        .frame(&Frame::Request {
            request: Some(management::PlanRequest {
                plan_id: plan.plan_id().into(),
                items: plan
                    .items()
                    .iter()
                    .map(|entry| entry.request.clone())
                    .collect(),
            }),
        })
        .unwrap();
    writer
        .frame(&Frame::Configuration {
            configuration: Some(
                spool
                    .frozen_review()
                    .await
                    .unwrap()
                    .configuration()
                    .await
                    .unwrap(),
            ),
        })
        .unwrap();
    writer
        .frame(&Frame::Acknowledgements {
            item_ids: Some(vec![]),
        })
        .unwrap();
    for entry in plan.items() {
        writer
            .frame(&Frame::Item {
                entry: entry.into(),
            })
            .unwrap();
    }
    writer
        .frame(&Frame::Summary {
            summary: *plan.summary(),
        })
        .unwrap();
    spool
        .write_archive_sections(&mut writer, &limits())
        .await
        .unwrap();
    let (file, budget) = writer.finish_with_budget().unwrap();
    assert!(
        budget.work.sql <= budget.work.rows + checkpoint.revisions.len() as u64 * 4,
        "inventory export must page keys and amortize original-cell SQL: rows={} sql={}",
        budget.work.rows,
        budget.work.sql,
    );
    file.sync_all().unwrap();
}
fn verify_canonical(path: &std::path::Path, expected: &[u8]) {
    use management::wire::v3::{Event, Frame, Projection, Reader, Section};
    let mut reader = Reader::new(
        std::fs::File::open(path).unwrap(),
        Projection::PrivateRecovery,
        archive_budget(),
    )
    .unwrap();
    let mut selected = false;
    let mut cell = false;
    let mut bytes = Vec::new();
    let mut members = 0;
    let mut eof = 0;
    while let Some(event) = reader.next_frame().unwrap() {
        match event {
            Event::Frame(Frame::Row { section, key, .. }) => {
                selected = section == Section::Native
                    && key.table == "pin_provider_routes"
                    && key.values == ["other-00000"];
                if section == Section::Membership {
                    match key.values.get(2).map(String::as_str) {
                        Some("member") => members += 1,
                        Some("eof") => eof += 1,
                        _ => panic!("invalid membership coordinate"),
                    }
                }
            }
            Event::Frame(Frame::Cell { column }) => cell = selected && column == "snapshot",
            Event::Chunk(chunk) if cell => bytes.extend(chunk),
            Event::Frame(Frame::RowEnd) => {
                cell = false;
                selected = false;
            }
            _ => {}
        }
    }
    assert!(reader.verified());
    assert_eq!(bytes, expected);
    assert!(members >= 4101);
    assert!(eof > 0);
}

fn verify_review(path: &std::path::Path) {
    use management::wire::v3::{CellValue, Event, Frame, Projection, Reader};
    let mut reader = Reader::new(
        std::fs::File::open(path).unwrap(),
        Projection::Review,
        archive_budget(),
    )
    .unwrap();
    let mut redacted = 0;
    while let Some(event) = reader.next_frame().unwrap() {
        match event {
            Event::Frame(Frame::CellEnd {
                value: CellValue::Redacted,
            }) => redacted += 1,
            Event::Chunk(bytes) => assert!(!String::from_utf8_lossy(&bytes).contains("私密")),
            _ => {}
        }
    }
    assert!(reader.verified());
    assert!(redacted >= 4101);
}

#[tokio::test]
async fn cleanup_keeps_shared_alias_and_equivalent_cid_references() {
    use management::{
        Action, ItemRequest, RetirementResource, RetirementSelection, RouteIdentity, Subject,
        retirement::RetirementMutationScope,
    };
    let directory = tempfile::tempdir().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    let mut a = route("provider-a", "shared");
    a.api_profile = "psa".into();
    a.cleanup = CleanupMode::Managed;
    let mut b = a.clone();
    b.provider_id = "provider-b".into();
    for (allocation, original) in [("alias-a", &a), ("alias-b", &b)] {
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO pin_provider_routes VALUES($1,$2,$1,0)",
            vec![
                allocation.into(),
                serde_json::to_string(original).unwrap().into(),
            ],
        ))
        .await
        .unwrap();
    }
    const V0: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
    let cid = ipfs_s3_gateway::pinning::provider::canonical_resource_cid(V0).unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO objects(id,bucket,key,cid,size,etag,encrypted,multipart,is_latest,created_at) VALUES('owner','bucket','key',$1,10,'etag',0,0,1,CURRENT_TIMESTAMP)",[V0.into()])).await.unwrap();
    db.execute_unprepared("INSERT INTO pin_leases(id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state) VALUES('lease','owner','automatic','policy','all','object',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'2099-01-01 00:00:00+00:00',1,'active')").await.unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO pin_lease_targets(id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at) VALUES('target','lease',$1,10,'alias-b','pinned',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",[V0.into()])).await.unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO remote_pins(provider,cid,request_id,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES('alias-a',$1,'resource-a',10,'pinned',1,0,CURRENT_TIMESTAMP)",[cid.clone().into()])).await.unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES('alias-a',$1,$2,'application_created','confirmed')",vec![cid.clone().into(),serde_json::to_string(&a).unwrap().into()])).await.unwrap();
    let opaque = "private error text ".repeat(100_000);
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE remote_pins SET last_error_text=$1 WHERE provider='alias-a'",
        [opaque.clone().into()],
    ))
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    let route = RouteIdentity::from_original(a.clone());
    let cp = SourceCheckpoint::start(&tx, route.clone()).await.unwrap();
    tx.rollback().await.unwrap();
    let path = directory.path().join("shared.sqlite");
    let spool = InventorySpool::create(&path, reserve(&path), &cp)
        .await
        .unwrap();
    while spool.scan(&db, &limits()).await.unwrap() == Progress::Pending {}
    let mut configuration = config(a);
    configuration.routes.push(b);
    while spool.reduce(&configuration, &limits()).await.unwrap() == Progress::Pending {}
    let review = spool.frozen_review().await.unwrap();
    assert_eq!(review.totals().current_resources, 1);
    assert!(review.totals().attribution_complete);
    assert_eq!(review.totals().unknown, 0);
    let request = ItemRequest {
        item_id: "cleanup".into(),
        subject: Subject::Provider {
            route: route.clone(),
        },
        action: Action::RetireWith {
            selection: RetirementSelection::ControlledCleanup {
                resources: vec![RetirementResource {
                    key: route.remote_key(&cid),
                    route,
                    epoch: 1,
                    resource_id: "resource-a".into(),
                }],
            },
        },
    };
    assert!(
        review
            .capture(&request, RetirementMutationScope::default())
            .await
            .is_err()
    );
    let tx = db.begin().await.unwrap();
    review.seal(&tx, &limits()).await.unwrap();
    tx.rollback().await.unwrap();
    let mut cursor = ArchiveCursor::default();
    let mut bytes = 0;
    while let Some(part) = spool.read_part(&mut cursor).await.unwrap() {
        if part.table == "remote_pins" && part.column == "last_error_text" {
            bytes += part.bytes.len();
        }
    }
    assert_eq!(bytes, opaque.len());
}

#[tokio::test]
async fn cancelled_and_drifted_passes_never_produce_a_seal() {
    use std::time::Instant;

    const SOURCE_ROWS: u64 = 256 + 1;

    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.sqlite");
    let source_url = format!("sqlite://{}", source.to_string_lossy().replace('\\', "/"));
    let db = Database::connect(format!("{source_url}?mode=rwc"))
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    let selected = route("selected", "selected");
    let snapshot = serde_json::to_string(&selected).unwrap();
    let seed = db.begin().await.unwrap();
    for i in 1..=SOURCE_ROWS {
        seed.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO pin_provider_routes VALUES($1,$2,'original',0)",
            [format!("route-{i:04}").into(), snapshot.clone().into()],
        ))
        .await
        .unwrap();
    }
    seed.commit().await.unwrap();
    let tx = db.begin().await.unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        management::RouteIdentity::from_original(selected.clone()),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let path = directory.path().join("cancel.sqlite");
    let spool = InventorySpool::create(&path, reserve(&path), &checkpoint)
        .await
        .unwrap();
    let slice = || {
        let mut limits = RunLimits::default();
        limits.pages = 1;
        limits
    };
    let cancelled = slice();
    cancelled.cancellation.cancel();
    assert_eq!(
        spool.scan(&db, &cancelled).await.unwrap_err(),
        ManagementError::Budget
    );
    assert!(matches!(
        spool.frozen_review().await,
        Err(ManagementError::InvalidEvidence)
    ));

    let scan_started = Instant::now();
    loop {
        assert_eq!(spool.scan(&db, &slice()).await.unwrap(), Progress::Pending);
        if spool.task_work().await.unwrap().rows >= SOURCE_ROWS - 1 {
            break;
        }
    }
    let initial_scan_elapsed = scan_started.elapsed();
    let accepted = spool.checkpoint().await.unwrap();
    assert!(!accepted.complete());
    let discarded = spool.task_work().await.unwrap();
    let mut cursor = ArchiveCursor::default();
    loop {
        let part = spool.read_part(&mut cursor).await.unwrap().unwrap();
        if part.key.table == "pin_provider_routes"
            && part.key.values == ["route-0001"]
            && part.column == "display_name"
        {
            assert_eq!(part.bytes, b"original");
            break;
        }
    }

    let competitor = Database::connect(format!("{source_url}?mode=rw"))
        .await
        .unwrap();
    let write_started = Instant::now();
    let writer = competitor.begin().await.unwrap();
    let writes = writer
        .execute_unprepared(
            "UPDATE pin_provider_routes SET display_name='committed-change' WHERE provider='route-0001'",
        )
        .await
        .unwrap()
        .rows_affected();
    assert_eq!(writes, 1);
    writer.commit().await.unwrap();
    let write_elapsed = write_started.elapsed();
    let revision = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT revision FROM pin_management_revision WHERE source_table='pin_provider_routes'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "revision")
        .unwrap();
    assert_eq!(revision, checkpoint.revisions["pin_provider_routes"] + 1);

    let rejection_started = Instant::now();
    assert_eq!(
        spool.scan(&db, &slice()).await.unwrap_err(),
        ManagementError::Changed
    );
    let rejection_elapsed = rejection_started.elapsed();
    assert!(spool.checkpoint().await.unwrap() == accepted);
    assert_eq!(spool.task_work().await.unwrap(), discarded);
    assert!(matches!(
        spool.frozen_review().await,
        Err(ManagementError::InvalidEvidence)
    ));
    let tx = db.begin().await.unwrap();
    assert_eq!(accepted.fence(&tx).await, Err(ManagementError::Changed));
    tx.rollback().await.unwrap();
    spool.close().await.unwrap();
    drop(spool);
    let spool = InventorySpool::resume(&path).await.unwrap();
    assert_eq!(spool.task_work().await.unwrap(), discarded);
    assert_eq!(
        spool.scan(&db, &slice()).await,
        Err(ManagementError::Changed)
    );
    assert_eq!(spool.task_work().await.unwrap(), discarded);
    spool.close().await.unwrap();
    drop(spool);

    let tx = db.begin().await.unwrap();
    let fresh = SourceCheckpoint::start(
        &tx,
        management::RouteIdentity::from_original(selected.clone()),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(fresh.revisions["pin_provider_routes"], revision);
    let fresh_path = directory.path().join("fresh-pass.sqlite");
    let spool = InventorySpool::create(&fresh_path, reserve(&fresh_path), &fresh)
        .await
        .unwrap();
    let envelope = RunLimits::default();
    let remaining = || {
        let mut limits = slice();
        limits.total_rows = envelope.total_rows.checked_sub(discarded.rows).unwrap();
        limits.total_bytes = envelope.total_bytes.checked_sub(discarded.bytes).unwrap();
        limits
    };
    let mut scan_elapsed = Duration::ZERO;
    let mut previous: Option<management::retirement::TaskWork> = None;
    loop {
        let started = Instant::now();
        let progress = spool.scan(&db, &remaining()).await.unwrap();
        scan_elapsed += started.elapsed();
        let work = spool.task_work().await.unwrap();
        if let Some(previous) = previous {
            assert!(work.rows >= previous.rows);
            assert!(work.bytes >= previous.bytes);
            assert!(work.pages >= previous.pages);
        }
        assert!(work.rows + discarded.rows <= envelope.total_rows);
        assert!(work.bytes + discarded.bytes <= envelope.total_bytes);
        previous = Some(work);
        if progress == Progress::Archived {
            break;
        }
        assert_eq!(progress, Progress::Pending);
    }
    let rescanned = spool.task_work().await.unwrap();
    assert!(rescanned.rows >= SOURCE_ROWS);
    let mut cursor = ArchiveCursor::default();
    loop {
        let part = spool.read_part(&mut cursor).await.unwrap().unwrap();
        if part.key.table == "pin_provider_routes"
            && part.key.values == ["route-0001"]
            && part.column == "display_name"
        {
            assert_eq!(part.bytes, b"committed-change");
            break;
        }
    }
    let reduction_started = Instant::now();
    while spool
        .reduce(&config(selected.clone()), &remaining())
        .await
        .unwrap()
        == Progress::Pending
    {}
    let reduction_elapsed = reduction_started.elapsed();
    let review = spool.frozen_review().await.unwrap();
    assert!(review.checkpoint().revisions == fresh.revisions);
    assert!(review.totals().attribution_complete);
    assert_eq!(review.totals().unknown, 0);
    let tx = db.begin().await.unwrap();
    review.seal(&tx, &remaining()).await.unwrap();
    tx.rollback().await.unwrap();
    let work = spool.task_work().await.unwrap();
    assert!(work.rows + discarded.rows <= envelope.total_rows);
    assert!(work.bytes + discarded.bytes <= envelope.total_bytes);
    eprintln!(
        "sqlite retirement source drift committed_writes={writes} revision_before={} revision_after={revision} invalidated_passes=1 restarted_passes=1 initial_scan={initial_scan_elapsed:?} write={write_elapsed:?} changed_rejection={rejection_elapsed:?} discarded_work={discarded:?} rescan={scan_elapsed:?} rescan_work={rescanned:?} reduce={reduction_elapsed:?} fresh_total_work={work:?} fresh_phases={:?} fresh_SQL_points={:?}",
        checkpoint.revisions["pin_provider_routes"],
        spool.phase_work().await.unwrap(),
        spool.query_work().await.unwrap(),
    );
    spool.close().await.unwrap();
    competitor.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn reduction_cannot_reset_the_scan_task_quota_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let selected = route("selected", "selected");
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES('selected',$1,'selected',0)",
        [serde_json::to_string(&selected).unwrap().into()],
    ))
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        management::RouteIdentity::from_original(selected.clone()),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let path = directory.path().join("measured.sqlite");
    let measured = InventorySpool::create(&path, reserve(&path), &checkpoint)
        .await
        .unwrap();
    while measured.scan(&db, &RunLimits::default()).await.unwrap() == Progress::Pending {}
    let scanned = measured.task_work().await.unwrap();
    measured.close().await.unwrap();
    let path = directory.path().join("quota-reduction.sqlite");
    let spool = InventorySpool::create(&path, reserve(&path), &checkpoint)
        .await
        .unwrap();
    let mut quota = RunLimits::default();
    quota.total_rows = scanned.rows;
    while spool.scan(&db, &quota).await.unwrap() == Progress::Pending {}
    assert!(matches!(
        spool
            .reduce(&config(selected.clone()), &RunLimits::default())
            .await,
        Err(ManagementError::Budget)
    ));
    let accepted = spool.task_work().await.unwrap();
    spool.close().await.unwrap();
    let spool = InventorySpool::resume(&path).await.unwrap();
    assert_eq!(spool.task_work().await.unwrap(), accepted);
    assert!(matches!(
        spool.reduce(&config(selected), &RunLimits::default()).await,
        Err(ManagementError::Budget)
    ));
    assert!(matches!(
        spool.frozen_review().await,
        Err(ManagementError::InvalidEvidence)
    ));
    spool.close().await.unwrap();
}

#[tokio::test]
async fn retained_inventory_revalidates_originals_before_rebuilding_its_review() {
    let directory = tempfile::tempdir().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let selected = route("selected", "selected");
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES('selected',$1,'selected',0)",
        [serde_json::to_string(&selected).unwrap().into()],
    ))
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        management::RouteIdentity::from_original(selected.clone()),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let path = directory.path().join("retained.sqlite");
    let spool = InventorySpool::create(&path, reserve(&path), &checkpoint)
        .await
        .unwrap();
    while spool.scan(&db, &RunLimits::default()).await.unwrap() == Progress::Pending {}
    while spool
        .reduce(&config(selected.clone()), &RunLimits::default())
        .await
        .unwrap()
        == Progress::Pending
    {}
    let before = spool.task_work().await.unwrap();
    let original_totals = spool.frozen_review().await.unwrap().totals().clone();
    spool.close().await.unwrap();
    let mut spool = InventorySpool::resume(&path).await.unwrap();
    assert!(matches!(
        spool.frozen_review().await,
        Err(ManagementError::InvalidEvidence)
    ));
    while spool
        .verify_source(&db, &RunLimits::default())
        .await
        .unwrap()
        == Progress::Pending
    {}
    while spool
        .reduce(&config(selected.clone()), &RunLimits::default())
        .await
        .unwrap()
        == Progress::Pending
    {}
    let after = spool.task_work().await.unwrap();
    assert!(after.rows > before.rows && after.bytes > before.bytes);
    let review = spool.frozen_review().await.unwrap();
    assert!(review.totals().routes_available);
    assert!(review.totals() == &original_totals);
    assert_eq!(review.checkpoint().observed_at, checkpoint.observed_at);
    let tx = db.begin().await.unwrap();
    review.seal(&tx, &RunLimits::default()).await.unwrap();
    tx.rollback().await.unwrap();
    spool.close().await.unwrap();
    let corrupt = Database::connect(format!("sqlite:{}?mode=rw", path.display()))
        .await
        .unwrap();
    corrupt.execute_unprepared("UPDATE parts SET bytes=X'666F72676564' WHERE table_name='pin_provider_routes' AND column_name='display_name'").await.unwrap();
    corrupt.close().await.unwrap();
    let mut spool = InventorySpool::resume(&path).await.unwrap();
    assert_eq!(
        spool.verify_source(&db, &RunLimits::default()).await,
        Err(ManagementError::Changed)
    );
    assert!(matches!(
        spool.frozen_review().await,
        Err(ManagementError::InvalidEvidence)
    ));
    spool.close().await.unwrap();
}

#[tokio::test]
async fn resumed_scan_keeps_its_original_task_byte_quota() {
    let directory = tempfile::tempdir().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES ('opaque',$1,'opaque',0)",
        ["x".repeat(2 * 1024 * 1024).into()],
    ))
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        management::RouteIdentity::from_original(route("selected", "selected")),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let path = directory.path().join("quota.sqlite");
    let spool = InventorySpool::create(&path, reserve(&path), &checkpoint)
        .await
        .unwrap();
    let quota = || {
        let mut limits = RunLimits::default();
        limits.pages = 1;
        limits.total_bytes = 1536 * 1024;
        limits
    };
    assert_eq!(spool.scan(&db, &quota()).await.unwrap(), Progress::Pending);
    let accepted = loop {
        let accepted = spool.checkpoint().await.unwrap();
        match spool.scan(&db, &quota()).await {
            Ok(Progress::Pending) => continue,
            Err(ManagementError::Budget) => break accepted,
            other => panic!("quota must stop the incomplete archive: {other:?}"),
        }
    };
    assert!(spool.checkpoint().await.unwrap() == accepted);
    spool.close().await.unwrap();
    drop(spool);
    let spool = InventorySpool::resume(&path).await.unwrap();
    let result = spool.scan(&db, &RunLimits::default()).await;
    let current = spool.checkpoint().await.unwrap();
    spool.close().await.unwrap();
    assert_eq!(result, Err(ManagementError::Budget));
    assert!(current == accepted);
    let spool = InventorySpool::resume(&path).await.unwrap();
    let larger = RunLimits::default();
    spool
        .increase_task_budget(larger.total_rows, larger.total_bytes)
        .await
        .unwrap();
    assert!(spool.checkpoint().await.unwrap() == accepted);
    let completed = spool.scan(&db, &larger).await;
    let end = spool.checkpoint().await.unwrap();
    spool.close().await.unwrap();
    assert_eq!(completed, Ok(Progress::Archived));
    assert!(end.complete());
}

#[tokio::test]
async fn known_resource_quotes_union_aliases_and_epochs_but_keep_unresolved_epochs() {
    let directory = tempfile::tempdir().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let a = route("provider-a", "shared");
    let mut b = a.clone();
    b.provider_id = "provider-b".into();
    let cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    for (allocation, route, bytes, epoch, effect) in [
        ("alias-a", &a, 10i64, 3i64, "retained"),
        ("alias-b", &b, 20i64, 4i64, "cleanup_pending"),
    ] {
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO pin_provider_routes VALUES($1,$2,$1,0)",
            vec![
                allocation.into(),
                serde_json::to_string(route).unwrap().into(),
            ],
        ))
        .await
        .unwrap();
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO remote_pins(provider,cid,request_id,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES($1,$2,'shared-resource',$3,'pinned',$4,0,CURRENT_TIMESTAMP)",vec![allocation.into(),cid.into(),bytes.into(),epoch.into()])).await.unwrap();
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES($1,$2,$3,'application_created',$4)",vec![allocation.into(),cid.into(),serde_json::to_string(route).unwrap().into(),effect.into()])).await.unwrap();
    }
    for (epoch, resource) in [(1, Some("shared-resource")), (2, None), (5, None)] {
        let ledger = serde_json::json!({"provider":"alias-a","cid":cid,"route":serde_json::to_string(&a).unwrap(),"ownership":"application_created","effect":"confirmed","first_observed_at":null,"last_observed_at":null,"remote_pinned_at":null,"gateway_verified_at":null,"content_verified_at":null,"first_error":null,"last_error":null});
        let envelope =
            serde_json::json!({"ledger":ledger,"request_id":resource,"confirmed_absent":false});
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO pin_resource_history VALUES('alias-a',$1,$2,$3)",
            vec![cid.into(), epoch.into(), envelope.to_string().into()],
        ))
        .await
        .unwrap();
    }
    let tx = db.begin().await.unwrap();
    let cp = SourceCheckpoint::start(&tx, management::RouteIdentity::from_original(a.clone()))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let path = directory.path().join("quotes.sqlite");
    let spool = InventorySpool::create(&path, reserve(&path), &cp)
        .await
        .unwrap();
    while spool.scan(&db, &limits()).await.unwrap() == Progress::Pending {}
    let mut configuration = config(a);
    configuration.routes.push(b);
    while spool.reduce(&configuration, &limits()).await.unwrap() == Progress::Pending {}
    let review = spool.frozen_review().await.unwrap();
    assert_eq!(review.totals().current_resources, 2);
    assert_eq!(review.totals().historical_resources, 3);
    assert_eq!(review.totals().unique_resources, 3);
    assert_eq!(review.totals().accounted_bytes, 20);
    assert_eq!(review.totals().unknown, 3);
    assert_eq!(review.totals().retained, 1);
    assert_eq!(review.totals().cleanup_pending, 1);
    let fence = review.prepare_fence().unwrap();
    let mut guarded = store::pinning::management::transaction::ManagementTransaction::begin(
        &db,
        Duration::from_secs(2),
        Default::default(),
    )
    .await
    .unwrap();
    let final_limits = limits();
    guarded
        .run(move |tx, _| Box::pin(async move { fence.seal(tx, &final_limits).await }))
        .await
        .unwrap();
    guarded.rollback().await.unwrap();
    let request = management::PlanRequest {
        plan_id: "quote-retire".into(),
        items: vec![management::ItemRequest {
            item_id: "retire".into(),
            action: management::Action::Retire,
            subject: management::Subject::Provider {
                route: cp.route.clone(),
            },
        }],
    };
    let local = management::Snapshot {
        observed_at: cp.observed_at.timestamp(),
        database_now: Some(cp.observed_at),
        configuration: Some(configuration),
        ..Default::default()
    };
    let union_path = directory.path().join("quote-union.sqlite");
    let union = management::retirement::PlanUnion::create(&union_path, reserve(&union_path))
        .await
        .unwrap();
    let plan = loop {
        if let Some(plan) = management::dry_run_retirement(
            &request,
            &local,
            &[spool.frozen_review().await.unwrap()],
            &[],
            &union,
            &limits(),
        )
        .await
        .unwrap()
        {
            break plan;
        }
    };
    assert_eq!(plan.summary().accounted_remote_bytes, 20);
    assert_eq!(plan.summary().physical_release_bytes, 0);
    let refs = review
        .stage_evidence(plan.plan_id(), &plan.items()[0])
        .await
        .unwrap();
    let canonical = directory.path().join("quote.v3");
    write_canonical(
        &spool,
        &cp,
        reserve(&canonical),
        management::wire::v3::Projection::PrivateRecovery,
        &plan,
    )
    .await;
    let cursor = management::wire::v3::archive_cursor::stage_import(
        std::fs::File::open(&canonical).unwrap(),
        management::wire::v3::Projection::PrivateRecovery,
        archive_budget(),
    )
    .unwrap()
    .into_cursor();
    let restored_path = directory.path().join("quote-restored.sqlite");
    let mut importer = management::retirement::FrozenRetirementImport::create(
        cursor,
        &restored_path,
        reserve(&restored_path),
        spool.reference().clone(),
    )
    .await
    .unwrap();
    while importer.advance(&limits()).await.unwrap() == Progress::Pending {}
    let (restored, _original, _budget) = importer.into_parts().unwrap();
    let capture = restored
        .restore_capture(plan.plan_id(), &request.items[0], &refs)
        .await
        .unwrap();
    assert!(
        plan.items()[0].capture.as_ref() == Some(&management::Capture::Provider(Box::new(capture)))
    );
}
