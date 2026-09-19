use super::*;
use crate::pinning::policy::PublicationPolicy;
use crate::store::{
    self,
    pinning::publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
};
use sea_orm::Database;

async fn setup() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    db
}

async fn admit(db: &DatabaseConnection) -> StandardMutationGuard {
    admit_content_mutation(
        db,
        "bucket",
        "key",
        None,
        SupersedeReason::PutObject,
        Utc::now(),
    )
    .await
    .unwrap()
}

async fn lifecycle(db: &DatabaseConnection) -> Option<StandardMutationGuard> {
    try_admit_lifecycle_mutation(db, "bucket", "key", "action", 1, Utc::now())
        .await
        .unwrap()
}

fn request() -> PublicationRequest {
    PublicationRequest {
        object: PublicationObject::from_put(
            "object".into(),
            "bucket",
            "key",
            "cid".into(),
            -1,
            None,
            None,
            false,
            None,
            None,
            Utc::now(),
        ),
        tags: vec![],
        policy: PublicationPolicy {
            tags: vec![],
            leases: vec![],
        },
        object_target: PinTargetSpec {
            cid: "cid".into(),
            logical_size: -1,
        },
    }
}

#[tokio::test]
async fn failed_publication_releases_ordinary_writer() {
    let db = setup().await;
    let guard = admit(&db).await;
    assert!(
        publication::publish_standard_object(&db, request(), guard, &Default::default())
            .await
            .is_err()
    );
    assert!(
        lifecycle(&db).await.is_some(),
        "failed publication stranded ordinary mutation_id"
    );
}

#[tokio::test]
async fn active_writer_blocks_lifecycle() {
    let db = setup().await;
    let _guard = admit(&db).await;
    assert!(lifecycle(&db).await.is_none());
}

#[tokio::test]
async fn disappeared_writer_expires_and_fences_old_publication() {
    let db = setup().await;
    let old = admit(&db).await;
    db.execute_unprepared(
        "UPDATE standard_mutation_leases SET lease_until = '2000-01-01T00:00:00Z'",
    )
    .await
    .expect("ordinary writers need a durable bounded lease");
    let new = lifecycle(&db)
        .await
        .expect("expired writer must not block lifecycle");
    assert!(new.expected_generation > old.expected_generation);
    let txn = db.begin().await.unwrap();
    lock_bucket_for_ownership(&txn, "bucket").await.unwrap();
    assert!(matches!(
        verify_standard_mutation_guard(&txn, &old, "bucket", "key", &[]).await,
        Err(AppError::StaleContentMutation)
    ));
    txn.rollback().await.unwrap();
}

async fn expire(db: &DatabaseConnection) {
    db.execute_unprepared(
        "UPDATE standard_mutation_leases SET lease_until = '2000-01-01T00:00:00Z'",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn committed_publication_is_not_cancelled_before_response_returns() {
    let db = setup().await;
    let guard = admit(&db).await;
    let lease = std::sync::Arc::new(MutationLease::start_with_interval(
        &db,
        &guard,
        Duration::from_millis(10),
    ));
    let gate = std::sync::Arc::new(super::mutation_lease::RenewalGate::default());
    *lease.renewal_gate.lock().unwrap() = Some(gate.clone());
    let (committed, after_commit) = tokio::sync::oneshot::channel();
    let (finish_commit, commit_gate) = tokio::sync::oneshot::channel();
    let (respond, response_gate) = tokio::sync::oneshot::channel();
    let work_lease = lease.clone();
    let work_db = db.clone();
    let work = tokio::spawn(async move {
        work_lease
            .run(async {
                let mut valid = request();
                valid.object.logical_size = 0;
                valid.object_target.logical_size = 0;
                let published = work_lease
                    .commit(async {
                        let published = publication::publish_standard_object(
                            &work_db,
                            valid,
                            guard,
                            &Default::default(),
                        )
                        .await?;
                        committed.send(()).unwrap();
                        commit_gate.await.unwrap();
                        AppResult::Ok(published)
                    })
                    .await?;
                response_gate.await.unwrap();
                AppResult::Ok(published)
            })
            .await
    });
    after_commit.await.unwrap();
    gate.entered.notified().await;
    gate.proceed.notify_one();
    finish_commit.send(()).unwrap();
    lease.renewal_observed.notified().await;
    let _ = respond.send(());
    assert!(
        work.await.unwrap().is_ok(),
        "successful commit must survive a queued renewal before response return"
    );
    lease.finish().await;
    assert_eq!(
        store::entities::object::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn superseded_running_work_is_cancelled_not_misclassified_as_completed() {
    let db = setup().await;
    let old = admit(&db).await;
    let lease = MutationLease::start_with_interval(&db, &old, Duration::from_millis(10));
    let new = admit(&db).await;
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        lease.run(std::future::pending::<AppResult<()>>()),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(AppError::StaleContentMutation)));
    assert!(matches!(
        lease.commit(async { AppResult::Ok(()) }).await,
        Err(AppError::StaleContentMutation)
    ));
    lease.finish().await;
    renew_standard_mutation(&db, &new).await.unwrap();
}

#[tokio::test]
async fn expired_guard_cannot_renew_publish_or_complete() {
    let db = setup().await;
    let guard = admit(&db).await;
    expire(&db).await;
    assert!(matches!(
        renew_standard_mutation(&db, &guard).await,
        Err(AppError::StaleContentMutation)
    ));
    let mut valid = request();
    valid.object.logical_size = 0;
    valid.object_target.logical_size = 0;
    assert!(matches!(
        publication::publish_standard_object(&db, valid, guard.clone(), &Default::default()).await,
        Err(AppError::StaleContentMutation)
    ));
    assert!(
        store::entities::object::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .is_empty()
    );
    let txn = db.begin().await.unwrap();
    lock_bucket_for_ownership(&txn, "bucket").await.unwrap();
    assert!(matches!(
        complete_standard_mutation_in_transaction(&txn, &guard, Utc::now()).await,
        Err(AppError::StaleContentMutation)
    ));
    txn.rollback().await.unwrap();
}

#[tokio::test]
async fn completion_rechecks_lease_after_initial_verification() {
    let db = setup().await;
    let guard = admit(&db).await;
    let txn = db.begin().await.unwrap();
    lock_bucket_for_ownership(&txn, "bucket").await.unwrap();
    verify_standard_mutation_guard(&txn, &guard, "bucket", "key", &[])
        .await
        .unwrap();
    txn.execute_unprepared(
        "UPDATE standard_mutation_leases SET lease_until = '2000-01-01T00:00:00Z'",
    )
    .await
    .unwrap();
    assert!(matches!(
        complete_standard_mutation_in_transaction(&txn, &guard, Utc::now()).await,
        Err(AppError::StaleContentMutation)
    ));
    txn.rollback().await.unwrap();
}

#[tokio::test]
async fn late_release_cannot_clear_new_guard_or_other_key() {
    let db = setup().await;
    let old = admit(&db).await;
    let new = admit(&db).await;
    let others = admit_content_mutations(
        &db,
        "bucket",
        &["other".into(), "other".into(), "third".into()],
        None,
        SupersedeReason::DeleteObject,
        Utc::now(),
    )
    .await
    .unwrap();
    assert_eq!(others.len(), 2);
    assert!(!release_standard_mutation(&db, &old).await.unwrap());
    renew_standard_mutation(&db, &new).await.unwrap();
    assert!(lifecycle(&db).await.is_none());
    assert!(release_standard_mutation(&db, &new).await.unwrap());
    for guard in others {
        renew_standard_mutation(&db, &guard).await.unwrap();
        assert!(release_standard_mutation(&db, &guard).await.unwrap());
    }
    assert!(lifecycle(&db).await.is_some());
}

#[tokio::test]
async fn renewal_loss_cancels_in_progress_work() {
    let db = setup().await;
    let guard = admit(&db).await;
    let lease = MutationLease::start_with_interval(&db, &guard, Duration::from_millis(10));
    expire(&db).await;
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        lease.run(std::future::pending::<AppResult<()>>()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(error, AppError::StaleContentMutation));
    lease.finish().await;
    assert!(lifecycle(&db).await.is_some());
}

#[tokio::test]
async fn prefix_recovery_is_literal_and_never_clears_a_live_replacement() {
    let db = setup().await;
    let old = admit_content_and_prefix_mutation(
        &db,
        "bucket",
        "archive",
        "A%_/",
        SupersedeReason::DecompressZip,
        Utc::now(),
    )
    .await
    .unwrap();
    assert!(
        try_admit_lifecycle_mutation(&db, "bucket", "A%_/child", "action", 1, Utc::now())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        try_admit_lifecycle_mutation(&db, "bucket", "Ax_/child", "other", 1, Utc::now())
            .await
            .unwrap()
            .is_some()
    );
    expire(&db).await;
    let replacement = admit_content_and_prefix_mutation(
        &db,
        "bucket",
        "new-archive",
        "A%_/",
        SupersedeReason::DecompressZip,
        Utc::now(),
    )
    .await
    .unwrap();
    assert!(
        try_admit_lifecycle_mutation(&db, "bucket", "A%_/child", "action", 1, Utc::now())
            .await
            .unwrap()
            .is_none()
    );
    assert!(!release_standard_mutation(&db, &old).await.unwrap());
    renew_standard_mutation(&db, &replacement).await.unwrap();
    release_standard_mutation(&db, &replacement).await.unwrap();
    assert!(
        try_admit_lifecycle_mutation(&db, "bucket", "A%_/child", "action", 1, Utc::now())
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn cancellation_cleans_exact_guard_without_waiting_for_expiry() {
    let db = setup().await;
    let guard = admit(&db).await;
    let (started, ready) = tokio::sync::oneshot::channel();
    let task_db = db.clone();
    let task = tokio::spawn(async move {
        run_mutation(&task_db, &guard, |_| async {
            started.send(()).unwrap();
            std::future::pending::<AppResult<()>>().await
        })
        .await
    });
    ready.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        while lifecycle(&db).await.is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled request must signal cleanup immediately");
}

#[tokio::test]
async fn two_instances_renew_a_long_writer_then_recover_disappearance() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path()
            .join("mutation.db")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let first = store::connect_database(&url).await.unwrap();
    store::run_migrations(&first).await.unwrap();
    store::bucket::create(&first, "bucket", None).await.unwrap();
    let second = store::connect_database(&url).await.unwrap();
    let guard = admit_content_mutation(
        &first,
        "bucket",
        "key",
        None,
        SupersedeReason::PutObject,
        Utc::now() - chrono::Duration::days(99),
    )
    .await
    .unwrap();
    // Shorten only the fixture deadline. Renewal still derives its bounded
    // extension from the database, not the deliberately skewed caller timestamp.
    let shortened =
        store::database_clock::database_now(&first).await.unwrap() + chrono::Duration::seconds(60);
    standard_mutation_lease::Entity::update_many()
        .col_expr(
            standard_mutation_lease::Column::LeaseUntil,
            Expr::value(shortened),
        )
        .exec(&first)
        .await
        .unwrap();
    let lease = MutationLease::start_with_interval(&first, &guard, Duration::from_millis(10));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = standard_mutation_lease::Entity::find_by_id((
                "bucket".to_owned(),
                "key".to_owned(),
            ))
            .one(&second)
            .await
            .unwrap()
            .unwrap();
            if row.lease_until > shortened {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(lifecycle(&second).await.is_none());
    lease.finish().await;
    let abandoned = admit(&first).await;
    first.close().await.unwrap();
    expire(&second).await;
    let replacement = lifecycle(&second).await.unwrap();
    assert!(replacement.expected_generation > abandoned.expected_generation);
    assert!(
        !release_standard_mutation(&second, &abandoned)
            .await
            .unwrap()
    );
    second.close().await.unwrap();
}

#[tokio::test]
async fn legacy_migration_preserves_fences_then_recovers_after_quarantine() {
    use sea_orm_migration::{MigrationTrait, SchemaManager};
    let db = setup().await;
    let guard = admit(&db).await;
    let migration = store::migrations::m20260919_000001_standard_mutation_lease::Migration;
    migration.down(&SchemaManager::new(&db)).await.unwrap();
    let before = store::database_clock::database_now(&db).await.unwrap();
    migration.up(&SchemaManager::new(&db)).await.unwrap();
    let row = standard_mutation_lease::Entity::find_by_id(("bucket".to_owned(), "key".to_owned()))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.mutation_id, guard.mutation_id);
    assert_eq!(row.generation, guard.expected_generation);
    assert!(row.lease_until >= before + chrono::Duration::seconds(120));
    assert!(lifecycle(&db).await.is_none());
    expire(&db).await;
    assert!(lifecycle(&db).await.is_some());
}
