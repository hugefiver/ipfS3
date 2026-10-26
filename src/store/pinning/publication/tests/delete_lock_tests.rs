use std::{collections::BTreeMap, sync::Mutex, time::Instant};

use super::*;

#[derive(Default)]
struct SqlWork {
    count: usize,
    elapsed: Duration,
    statements: BTreeMap<String, usize>,
}

#[derive(Clone, Default)]
struct QueryMeter(Arc<Mutex<Option<SqlWork>>>);

impl QueryMeter {
    fn install(db: &mut DatabaseConnection) -> Self {
        let meter = Self::default();
        let state = meter.0.clone();
        db.set_metric_callback(move |info| {
            if let Some(work) = state.lock().unwrap().as_mut() {
                work.count += 1;
                work.elapsed += info.elapsed;
                *work
                    .statements
                    .entry(info.statement.sql.clone())
                    .or_default() += 1;
                assert!(work.statements.len() <= 64, "unbounded delete query shapes");
            }
        });
        meter
    }

    fn begin(&self) {
        *self.0.lock().unwrap() = Some(SqlWork::default());
    }

    fn finish(&self) -> SqlWork {
        self.0.lock().unwrap().take().unwrap()
    }
}

async fn ownership_fence_sql(db: &DatabaseConnection, meter: &QueryMeter) -> String {
    let txn = db.begin().await.unwrap();
    meter.begin();
    lock_bucket_for_ownership(&txn, "bucket").await.unwrap();
    let work = meter.finish();
    txn.rollback().await.unwrap();
    assert_eq!(work.count, 1);
    work.statements.into_keys().next().unwrap()
}

#[tokio::test]
async fn delete_attempt_uses_one_bucket_fence_and_rejects_stale_guard() {
    let mut db = setup().await;
    set_versioning(&db, BucketVersioningState::Enabled).await;
    let now = Utc::now();
    let stale = admit_content_mutation(
        &db,
        "bucket",
        "key",
        None,
        SupersedeReason::DeleteObject,
        now,
    )
    .await
    .unwrap();
    let winner = admit_content_mutation(
        &db,
        "bucket",
        "key",
        None,
        SupersedeReason::DeleteObject,
        now,
    )
    .await
    .unwrap();
    let meter = QueryMeter::install(&mut db);
    let fence_sql = ownership_fence_sql(&db, &meter).await;

    meter.begin();
    let error = delete_version_attempt(
        (&db).into(),
        "bucket",
        "key",
        VersionSelector::Current,
        stale,
        now,
    )
    .await
    .unwrap_err();
    let stale_work = meter.finish();
    assert!(matches!(
        error,
        sea_orm::TransactionError::Transaction(AppError::StaleContentMutation)
    ));
    assert_eq!(stale_work.statements.get(&fence_sql), Some(&1));
    assert!(versions_for(&db, "key").await.is_empty());

    meter.begin();
    let deleted = delete_version_attempt(
        (&db).into(),
        "bucket",
        "key",
        VersionSelector::Current,
        winner,
        now,
    )
    .await
    .unwrap();
    let work = meter.finish();
    assert_eq!(work.statements.get(&fence_sql), Some(&1));
    assert!(deleted.created_delete_marker);
    let versions = versions_for(&db, "key").await;
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].kind, "delete_marker");
    assert!(versions[0].is_latest);
}

#[tokio::test]
async fn delete_attempt_waits_for_versioning_update_and_reads_committed_state() {
    let (_directory, first, second) =
        setup_independent_file_backed("delete-versioning-lock.sqlite").await;
    set_versioning(&first, BucketVersioningState::Enabled).await;
    let now = Utc::now();
    let guard = admit_content_mutation(
        &second,
        "bucket",
        "key",
        None,
        SupersedeReason::DeleteObject,
        now,
    )
    .await
    .unwrap();

    let versioning = first.begin().await.unwrap();
    crate::store::bucket::lock_versioning_state(&versioning, "bucket")
        .await
        .unwrap();
    versioning
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE buckets SET versioning_status = ? WHERE name = ?",
            ["Suspended".into(), "bucket".into()],
        ))
        .await
        .unwrap();

    let (started, waiting) = tokio::sync::oneshot::channel();
    let mut deleting = tokio::spawn(async move {
        started.send(()).unwrap();
        delete_version_attempt(
            (&second).into(),
            "bucket",
            "key",
            VersionSelector::Current,
            guard,
            now,
        )
        .await
    });
    waiting.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut deleting)
            .await
            .is_err(),
        "delete must wait for the bucket versioning transaction"
    );
    versioning.commit().await.unwrap();

    let deleted = tokio::time::timeout(Duration::from_secs(10), deleting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(deleted.created_delete_marker);
    assert_eq!(deleted.version_id.as_deref(), Some("null"));
    let versions = versions_for(&first, "key").await;
    assert_eq!(versions.len(), 1);
    assert!(versions[0].version_id.is_none());
    assert_eq!(versions[0].kind, "delete_marker");
    assert!(versions[0].is_latest);
}

#[tokio::test]
#[ignore = "file-backed SQLite delete-attempt cost measurement"]
async fn delete_attempt_file_sqlite_cost() {
    for item_count in [1, 1_000] {
        for sample in 0..5 {
            let (_directory, mut db) = setup_file_backed("delete-cost.sqlite").await;
            for index in 0..item_count {
                publish_object(
                    &db,
                    request(
                        object(
                            &format!("object-{index}"),
                            &format!("key-{index}"),
                            "bafy-cost",
                            7,
                        ),
                        vec![],
                        vec![],
                    ),
                    &limits(),
                )
                .await
                .unwrap();
            }
            let meter = QueryMeter::install(&mut db);
            let fence_sql = ownership_fence_sql(&db, &meter).await;
            let mut work = SqlWork::default();
            let mut wall = Duration::ZERO;
            for index in 0..item_count {
                let key = format!("key-{index}");
                let now = Utc::now();
                let guard = admit_content_mutation(
                    &db,
                    "bucket",
                    &key,
                    None,
                    SupersedeReason::DeleteObject,
                    now,
                )
                .await
                .unwrap();
                meter.begin();
                let started = Instant::now();
                delete_version_attempt(
                    (&db).into(),
                    "bucket",
                    &key,
                    VersionSelector::Current,
                    guard,
                    now,
                )
                .await
                .unwrap();
                wall += started.elapsed();
                let item = meter.finish();
                work.count += item.count;
                work.elapsed += item.elapsed;
                for (sql, count) in item.statements {
                    *work.statements.entry(sql).or_default() += count;
                }
            }
            eprintln!(
                "delete_attempt_sqlite items={item_count} sample={sample} completed_transactions={item_count} statements={} bucket_fences={} sql_us={} wall_us={}",
                work.count,
                work.statements.get(&fence_sql).copied().unwrap_or_default(),
                work.elapsed.as_micros(),
                wall.as_micros(),
            );
        }
    }
}
