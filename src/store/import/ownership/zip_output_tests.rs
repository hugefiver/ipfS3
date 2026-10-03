use super::*;
use crate::{
    import::ImportSource,
    pinning::tags::ObjectTag,
    store::{
        self,
        entities::{object, object_tag, object_version},
        import::jobs::NewImportJob,
    },
};
use std::collections::HashMap;

use sea_orm::{ConnectOptions, Database};

async fn setup() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    db
}

async fn file_setup() -> (tempfile::TempDir, DatabaseConnection, DatabaseConnection) {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path()
            .join("zip.sqlite")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let connect = async |url: &str| -> DatabaseConnection {
        let mut options = ConnectOptions::new(url.to_owned());
        options.max_connections(1).min_connections(1);
        store::apply_sqlite_busy_timeout(&mut options);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        db
    };
    let first = connect(&url).await;
    store::run_migrations(&first).await.unwrap();
    store::bucket::create(&first, "bucket", None).await.unwrap();
    let second = connect(&url).await;
    (dir, first, second)
}

fn request(id: &str, key: &str, prefix: Option<&str>) -> NewImportJob {
    NewImportJob {
        id: id.into(),
        bucket: "bucket".into(),
        key: key.into(),
        source: ImportSource::Cid(
            "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".into(),
        ),
        request_fingerprint: format!("sha256:{id}"),
        client_token: None,
        object_content_type: None,
        metadata: HashMap::new(),
        tags: vec![ObjectTag::new("fixture", "true")],
        decompress_prefix: prefix.map(str::to_owned),
    }
}

async fn admit(
    db: &DatabaseConnection,
    source: &str,
    keys: &[&str],
) -> AppResult<Vec<StandardMutationGuard>> {
    let keys = keys
        .iter()
        .map(|key| (*key).to_owned())
        .collect::<BTreeSet<_>>();
    let ids = keys
        .iter()
        .map(|key| (key.clone(), uuid::Uuid::new_v4().to_string()))
        .collect::<BTreeMap<_, _>>();
    let source = source.to_owned();
    db.transaction(move |txn| {
        Box::pin(async move {
            lock_bucket_for_ownership(txn, "bucket").await?;
            admit_zip_outputs_without_source_in_transaction(
                txn,
                "bucket",
                &source,
                &keys,
                &ids,
                Utc::now(),
            )
            .await
        })
    })
    .await
    .map_err(transaction_error_into_app)
}

async fn destination(db: &DatabaseConnection, key: &str) -> Option<import_destination::Model> {
    import_destination::Entity::find_by_id(("bucket".to_owned(), key.to_owned()))
        .one(db)
        .await
        .unwrap()
}

async fn state(db: &DatabaseConnection, id: &str) -> String {
    import_job::Entity::find_by_id(id)
        .one(db)
        .await
        .unwrap()
        .unwrap()
        .state
}

async fn seed_source_object(db: &DatabaseConnection) {
    db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('source-object','bucket','source.zip','source-cid',1,'source-cid')")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('source-version','bucket','source.zip','object','source-object',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO object_tags (object_id,key,value) VALUES ('source-object','original','untouched')")
        .await
        .unwrap();
}

async fn source_rows(
    db: &DatabaseConnection,
) -> (object::Model, object_version::Model, object_tag::Model) {
    (
        object::Entity::find_by_id("source-object")
            .one(db)
            .await
            .unwrap()
            .unwrap(),
        object_version::Entity::find_by_id("source-version")
            .one(db)
            .await
            .unwrap()
            .unwrap(),
        object_tag::Entity::find_by_id(("source-object".to_owned(), "original".to_owned()))
            .one(db)
            .await
            .unwrap()
            .unwrap(),
    )
}

#[tokio::test]
async fn source_import_prefix_covering_output_is_rejected_without_superseding_source() {
    let db = setup().await;
    seed_source_object(&db).await;
    submit(
        &db,
        request("source-job", "source.zip", Some("out/")),
        Utc::now(),
    )
    .await
    .unwrap();
    let before = destination(&db, "source.zip").await;
    let rows = source_rows(&db).await;
    assert!(matches!(
        admit(&db, "source.zip", &["out/entry"]).await,
        Err(AppError::StaleContentMutation)
    ));
    assert_eq!(destination(&db, "source.zip").await, before);
    assert_eq!(source_rows(&db).await, rows);
    assert!(destination(&db, "out/entry").await.is_none());
    assert_eq!(state(&db, "source-job").await, STATE_QUEUED);
}

#[tokio::test]
async fn source_prefix_mutation_covering_output_is_rejected_with_guard_unchanged() {
    let db = setup().await;
    let source = admit_content_and_prefix_mutation(
        &db,
        "bucket",
        "source.zip",
        "out/",
        SupersedeReason::DecompressZip,
        Utc::now(),
    )
    .await
    .unwrap();
    let before = destination(&db, "source.zip").await;
    assert!(matches!(
        admit(&db, "source.zip", &["out/entry"]).await,
        Err(AppError::StaleContentMutation)
    ));
    assert_eq!(destination(&db, "source.zip").await, before);
    assert!(destination(&db, "out/entry").await.is_none());
    renew_standard_mutation(&db, &source).await.unwrap();
}

#[tokio::test]
async fn unrelated_outputs_and_legitimate_overwrite_get_exact_renewable_guards() {
    let db = setup().await;
    seed_source_object(&db).await;
    let rows = source_rows(&db).await;
    let source = admit_content_mutation(
        &db,
        "bucket",
        "source.zip",
        None,
        SupersedeReason::PutObject,
        Utc::now(),
    )
    .await
    .unwrap();
    submit(&db, request("old-output", "out/existing", None), Utc::now())
        .await
        .unwrap();
    let guards = admit(&db, "source.zip", &["out/new", "out/existing", "out/new"])
        .await
        .unwrap();
    assert_eq!(
        guards.iter().map(|g| g.key.as_str()).collect::<Vec<_>>(),
        ["out/existing", "out/new"]
    );
    assert!(guards.iter().all(|g| g.mutation_prefix.is_none()));
    assert_eq!(state(&db, "old-output").await, STATE_SUPERSEDED);
    renew_standard_mutation(&db, &source).await.unwrap();
    for guard in &guards {
        renew_standard_mutation(&db, guard).await.unwrap();
    }
    let txn = db.begin().await.unwrap();
    lock_bucket_for_ownership(&txn, "bucket").await.unwrap();
    let keys = ["out/existing".into(), "out/new".into()];
    verify_zip_output_guards_in_transaction(&txn, "bucket", "source.zip", &keys, &guards)
        .await
        .unwrap();
    renew_zip_output_guards_in_transaction(&txn, "bucket", "source.zip", &keys, &guards)
        .await
        .unwrap();
    complete_zip_output_guards_in_transaction(
        &txn,
        "bucket",
        "source.zip",
        &keys,
        &guards,
        Utc::now(),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(
        destination(&db, "source.zip")
            .await
            .unwrap()
            .mutation_id
            .as_deref(),
        Some(source.mutation_id.as_str())
    );
    assert!(
        destination(&db, "out/existing")
            .await
            .unwrap()
            .mutation_id
            .is_none()
    );
    assert_eq!(source_rows(&db).await, rows);
}

#[tokio::test]
async fn old_exact_admission_still_supersedes_overlapping_source_job() {
    let db = setup().await;
    submit(
        &db,
        request("source-job", "source.zip", Some("out/")),
        Utc::now(),
    )
    .await
    .unwrap();
    admit_content_mutation(
        &db,
        "bucket",
        "out/entry",
        None,
        SupersedeReason::PutObject,
        Utc::now(),
    )
    .await
    .unwrap();
    assert_eq!(state(&db, "source-job").await, STATE_SUPERSEDED);
    assert!(
        destination(&db, "source.zip")
            .await
            .unwrap()
            .owner_job_id
            .is_none()
    );
}

#[tokio::test]
async fn source_covered_by_other_anchor_prefix_is_not_implicitly_invalidated() {
    let db = setup().await;
    let other = admit_content_and_prefix_mutation(
        &db,
        "bucket",
        "unrelated-anchor",
        "",
        SupersedeReason::DecompressZip,
        Utc::now(),
    )
    .await
    .unwrap();
    let before = destination(&db, "unrelated-anchor").await;
    assert!(matches!(
        admit(&db, "source.zip", &["out/entry"]).await,
        Err(AppError::StaleContentMutation)
    ));
    assert_eq!(destination(&db, "unrelated-anchor").await, before);
    renew_standard_mutation(&db, &other).await.unwrap();
}

#[tokio::test]
async fn other_import_owner_must_not_lose_a_prefix_covering_source() {
    let db = setup().await;
    submit(
        &db,
        request("other-owner", "elsewhere.zip", Some("")),
        Utc::now(),
    )
    .await
    .unwrap();
    let before = destination(&db, "elsewhere.zip").await;
    assert!(matches!(
        admit(&db, "source.zip", &["out/entry"]).await,
        Err(AppError::StaleContentMutation)
    ));
    assert_eq!(destination(&db, "elsewhere.zip").await, before);
    assert_eq!(state(&db, "other-owner").await, STATE_QUEUED);
}

#[tokio::test]
async fn unrelated_import_prefix_is_legitimately_superseded_for_exact_output() {
    let db = setup().await;
    submit(
        &db,
        request("other-owner", "elsewhere.zip", Some("out/")),
        Utc::now(),
    )
    .await
    .unwrap();
    let guards = admit(&db, "source.zip", &["out/entry"]).await.unwrap();
    assert_eq!(guards.len(), 1);
    assert_eq!(guards[0].key, "out/entry");
    assert_eq!(state(&db, "other-owner").await, STATE_SUPERSEDED);
    assert!(destination(&db, "source.zip").await.is_none());
}

#[tokio::test]
async fn source_in_outputs_or_second_conflicting_output_rolls_back_all_guards() {
    let db = setup().await;
    assert!(matches!(
        admit(&db, "source.zip", &["source.zip"]).await,
        Err(AppError::StaleContentMutation)
    ));
    submit(
        &db,
        request("source-job", "source.zip", Some("blocked/")),
        Utc::now(),
    )
    .await
    .unwrap();
    assert!(matches!(
        admit(&db, "source.zip", &["ok/one", "blocked/two"]).await,
        Err(AppError::StaleContentMutation)
    ));
    assert!(destination(&db, "ok/one").await.is_none());
    assert!(destination(&db, "blocked/two").await.is_none());
    assert_eq!(state(&db, "source-job").await, STATE_QUEUED);
}

#[tokio::test]
async fn failed_second_output_insert_rolls_back_first_guard_and_lease() {
    let db = setup().await;
    db.execute_unprepared(
        "CREATE TRIGGER reject_second_zip_output BEFORE INSERT ON import_destinations \
         WHEN NEW.key = 'z/fail' BEGIN SELECT RAISE(FAIL, 'injected'); END;",
    )
    .await
    .unwrap();
    assert!(
        admit(&db, "source.zip", &["a/first", "z/fail"])
            .await
            .is_err()
    );
    assert!(destination(&db, "a/first").await.is_none());
    assert!(destination(&db, "z/fail").await.is_none());
    assert!(
        standard_mutation_lease::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn displaced_output_blocks_whole_publication_and_renewal() {
    let db = setup().await;
    let guards = admit(&db, "source.zip", &["out/first", "out/second"])
        .await
        .unwrap();
    admit_content_mutation(
        &db,
        "bucket",
        "out/second",
        None,
        SupersedeReason::PutObject,
        Utc::now(),
    )
    .await
    .unwrap();
    let keys = ["out/first".into(), "out/second".into()];
    let txn = db.begin().await.unwrap();
    lock_bucket_for_ownership(&txn, "bucket").await.unwrap();
    assert!(matches!(
        verify_zip_output_guards_in_transaction(&txn, "bucket", "source.zip", &keys, &guards).await,
        Err(AppError::StaleContentMutation)
    ));
    assert!(matches!(
        renew_zip_output_guards_in_transaction(&txn, "bucket", "source.zip", &keys, &guards).await,
        Err(AppError::StaleContentMutation)
    ));
    assert!(matches!(
        complete_zip_output_guards_in_transaction(
            &txn,
            "bucket",
            "source.zip",
            &keys,
            &guards,
            Utc::now(),
        )
        .await,
        Err(AppError::StaleContentMutation)
    ));
    txn.rollback().await.unwrap();
    assert_eq!(
        destination(&db, "out/first")
            .await
            .unwrap()
            .mutation_id
            .as_deref(),
        Some(guards[0].mutation_id.as_str())
    );
}

#[tokio::test]
async fn sqlite_bucket_lock_serializes_source_overlap_before_zip_output_admission() {
    let (_dir, first, second) = file_setup().await;
    let txn = first.begin().await.unwrap();
    lock_bucket_for_ownership(&txn, "bucket").await.unwrap();
    submit_in_transaction(
        &txn,
        request("source-job", "source.zip", Some("out/")),
        Utc::now(),
    )
    .await
    .unwrap();
    let (attempted, started) = tokio::sync::oneshot::channel();
    let mut competing = tokio::spawn(async move {
        attempted.send(()).unwrap();
        admit(&second, "source.zip", &["out/entry"]).await
    });
    started.await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), &mut competing)
            .await
            .is_err()
    );
    txn.commit().await.unwrap();
    assert!(matches!(
        competing.await.unwrap(),
        Err(AppError::StaleContentMutation)
    ));
    assert!(destination(&first, "out/entry").await.is_none());
    assert_eq!(state(&first, "source-job").await, STATE_QUEUED);
}
