use super::*;
use sea_orm::{Database, DatabaseConnection};
use std::{sync::Arc, time::Duration};
use tokio::sync::Barrier;

tokio::task_local! {
    static READ_BARRIER: (&'static str, Arc<Barrier>);
}

pub(super) async fn checkpoint(point: &str) {
    if let Ok(Some(barrier)) =
        READ_BARRIER.try_with(|(target, barrier)| (*target == point).then(|| barrier.clone()))
    {
        tokio::time::timeout(Duration::from_secs(10), async {
            barrier.wait().await;
            barrier.wait().await;
        })
        .await
        .expect("snapshot barrier must be released");
    }
}

async fn publish(db: &DatabaseConnection, bucket: &str, cid: &str) {
    use crate::store::pinning::publication::*;
    let object = PublicationObject::from_put(
        uuid::Uuid::new_v4().to_string(),
        bucket,
        "key",
        cid.to_owned(),
        3,
        None,
        None,
        false,
        None,
        None,
        Utc::now(),
    );
    publish_object(
        db,
        PublicationRequest {
            object,
            tags: vec![],
            policy: crate::pinning::policy::PublicationPolicy {
                tags: vec![],
                leases: vec![],
            },
            object_target: PinTargetSpec {
                cid: cid.to_owned(),
                logical_size: 3,
            },
        },
        &std::collections::BTreeMap::new(),
    )
    .await
    .unwrap();
}

async fn exercise_interleavings(db: &DatabaseConnection) {
    let bucket = format!("snapshot-{}", uuid::Uuid::new_v4());
    crate::store::bucket::create(db, &bucket, None)
        .await
        .unwrap();
    for point in ["version", "object"] {
        for delete in [false, true] {
            publish(db, &bucket, "old").await;
            let barrier = Arc::new(Barrier::new(2));
            let read_db = db.clone();
            let read_bucket = bucket.clone();
            let read_barrier = barrier.clone();
            let read = tokio::spawn(READ_BARRIER.scope((point, read_barrier), async move {
                read_snapshot(&read_db, &read_bucket, "key", &VersionSelector::Current).await
            }));
            barrier.wait().await;
            if delete {
                let guard = crate::store::import::ownership::admit_content_mutation(
                    db,
                    &bucket,
                    "key",
                    None,
                    crate::import::SupersedeReason::DeleteObject,
                    Utc::now(),
                )
                .await
                .unwrap();
                crate::store::pinning::publication::delete_version_with_leases_guarded(
                    db,
                    &bucket,
                    "key",
                    VersionSelector::Current,
                    guard,
                    Utc::now(),
                )
                .await
                .unwrap();
            } else {
                publish(db, &bucket, "new").await;
            }
            barrier.wait().await;
            let selected = read
                .await
                .unwrap()
                .expect("concurrent publication must not look corrupt");
            assert_eq!(selected.version.object.unwrap().cid, "old");
            assert_eq!(selected.residency.unwrap().identity.cid, "old");
            let current = read_snapshot(db, &bucket, "key", &VersionSelector::Current).await;
            if delete {
                assert!(matches!(current, Err(AppError::NoSuchKey(_))));
            } else {
                assert_eq!(current.unwrap().version.object.unwrap().cid, "new");
            }
        }
    }
}

#[tokio::test]
async fn sqlite_read_snapshot_survives_publication_between_selects() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("snapshot.sqlite").display()
    );
    let mut options = sea_orm::ConnectOptions::new(url);
    options.max_connections(4);
    let db = Database::connect(options).await.unwrap();
    db.execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    tokio::time::timeout(Duration::from_secs(15), exercise_interleavings(&db))
        .await
        .unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires coordinator-provided IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_read_snapshot_survives_publication_between_selects() {
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").unwrap();
    let admin = Database::connect(&url).await.unwrap();
    let schema = format!("snapshot_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let mut options = sea_orm::ConnectOptions::new(url);
    options.set_schema_search_path(schema.clone());
    let db = Database::connect(options).await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    let reader_db = db.clone();
    let result = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(15), exercise_interleavings(&reader_db))
            .await
            .unwrap();
    })
    .await;
    db.close().await.unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    result.unwrap();
}
