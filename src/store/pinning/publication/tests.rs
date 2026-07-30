use std::{collections::HashMap, time::Duration};

use chrono::Utc;
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, Statement, TransactionTrait,
};

use super::*;
use crate::{
    error::AppError,
    import::{ImportSource, SupersedeReason},
    pinning::{
        config::{LeaseDuration, ProviderLimitMap, ProviderLimits, ProviderMode},
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::{ContentMode, ObjectTag},
    },
    store::{
        entities::{
            import_destination, import_job, import_job_result, import_job_target, object,
            object_tag, pin_job, pin_lease, pin_lease_target, pin_provider_usage, remote_pin,
        },
        import::{
            jobs::{NewImportJob, claim_due},
            ownership::{
                ExpectedImportTarget, ImportPublicationGuard, admit_content_mutation,
                admit_content_mutations, claim_extracted_target, submit,
            },
        },
        multipart::{self, CommitCompletedUploadError, ReconciledCommitOutcome},
    },
};

fn limits() -> ProviderLimitMap {
    ProviderLimitMap::from([
        (
            "pinata".to_owned(),
            ProviderLimits {
                priority: 1,
                max_bytes: 1_000,
                max_pins: 100,
                enabled: true,
            },
        ),
        (
            "filebase".to_owned(),
            ProviderLimits {
                priority: 2,
                max_bytes: 1_000,
                max_pins: 100,
                enabled: true,
            },
        ),
    ])
}

#[test]
fn zip_object_writes_use_location_order_even_when_entries_are_reversed() {
    let archive = object("archive", "z/archive.zip", "bafy-archive", 7);
    let entries = vec![
        object("entry-z", "z/file.txt", "bafy-z", 1),
        object("entry-a", "a/file.txt", "bafy-a", 1),
    ];

    assert_eq!(
        ordered_publication_objects(&archive, &entries)
            .into_iter()
            .map(|object| object.id.as_str())
            .collect::<Vec<_>>(),
        vec!["entry-a", "archive", "entry-z"]
    );
}

#[tokio::test]
async fn zip_duplicate_entry_location_preserves_last_entry_wins() {
    let db = setup().await;
    let archive = request(
        object("archive", "archive.zip", "bafy-archive", 7),
        vec![],
        vec![],
    );

    publish_zip(
        &db,
        ZipPublicationRequest {
            archive,
            entries: vec![
                object("z-first", "same.txt", "bafy-first", 1),
                object("a-second", "same.txt", "bafy-second", 2),
            ],
        },
        &limits(),
    )
    .await
    .unwrap();

    let latest = crate::store::object::get_latest(&db, "bucket", "same.txt")
        .await
        .unwrap();
    assert_eq!(
        (latest.id.as_str(), latest.cid.as_str()),
        ("a-second", "bafy-second")
    );
}

fn duration() -> LeaseDuration {
    LeaseDuration::parse("1h").unwrap()
}

fn tag(key: &str, value: &str) -> ObjectTag {
    ObjectTag::new(key, value)
}

fn intent(
    source: LeaseSource,
    provider_mode: ProviderMode,
    providers: &[&str],
    content_mode: ContentMode,
) -> LeaseIntent {
    LeaseIntent {
        source,
        policy_id: "policy:test".to_owned(),
        provider_mode,
        providers: providers
            .iter()
            .map(|provider| (*provider).to_owned())
            .collect(),
        content_mode,
        duration: duration(),
    }
}

fn object(id: &str, key: &str, cid: &str, logical_size: i64) -> PublicationObject {
    PublicationObject::from_put(
        id.to_owned(),
        "bucket",
        key,
        cid.to_owned(),
        logical_size,
        Some("application/octet-stream".to_owned()),
        Some(serde_json::json!({"fixture": true})),
        false,
        None,
        None,
        Utc::now(),
    )
}

fn request(
    object: PublicationObject,
    tags: Vec<ObjectTag>,
    leases: Vec<LeaseIntent>,
) -> PublicationRequest {
    let object_target = PinTargetSpec {
        cid: object.cid.clone(),
        logical_size: object.logical_size,
    };
    PublicationRequest {
        object,
        tags: tags.clone(),
        policy: PublicationPolicy { tags, leases },
        object_target,
    }
}

fn automatic_and_manual_request(id: &str, key: &str, cid: &str) -> PublicationRequest {
    request(
        object(id, key, cid, 7),
        vec![
            tag("team", "storage"),
            tag("ipfs-s3:pin", "true"),
            tag("ipfs-s3:duration", "1h"),
        ],
        vec![
            intent(
                LeaseSource::Automatic,
                ProviderMode::All,
                &["filebase", "pinata"],
                ContentMode::Object,
            ),
            intent(
                LeaseSource::Manual,
                ProviderMode::One,
                &["filebase", "pinata"],
                ContentMode::Object,
            ),
        ],
    )
}

async fn setup() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    db
}

async fn setup_file_backed(name: &str) -> (tempfile::TempDir, DatabaseConnection) {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join(name);
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        database_path.display().to_string().replace('\\', "/")
    );
    let mut options = ConnectOptions::new(database_url);
    options.max_connections(4).min_connections(2);
    crate::store::apply_sqlite_busy_timeout(&mut options);
    let db = Database::connect(options).await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    (directory, db)
}

#[tokio::test]
async fn sqlite_publication_waits_for_existing_writer_before_starting_read_snapshot() {
    let (_directory, db) = setup_file_backed("publication-write-intent.sqlite").await;
    let blocker = db.begin().await.unwrap();
    blocker
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "UPDATE buckets SET owner = owner WHERE name = 'bucket'",
        ))
        .await
        .unwrap();

    let publication = request(
        object(
            "write-intent-publication",
            "write-intent-key",
            "bafy-write-intent",
            7,
        ),
        vec![],
        vec![],
    );
    let provider_limits = limits();
    let (published, ()) = tokio::join!(publish_object(&db, publication, &provider_limits), async {
        tokio::time::sleep(Duration::from_millis(250)).await;
        blocker.rollback().await.unwrap();
    },);
    assert!(
        published.is_ok(),
        "publication should wait for the existing writer: {published:?}"
    );

    let latest = crate::store::object::get_latest(&db, "bucket", "write-intent-key")
        .await
        .unwrap();
    assert_eq!(
        (latest.id.as_str(), latest.cid.as_str(), latest.is_latest),
        ("write-intent-publication", "bafy-write-intent", true)
    );
}

async fn seed_upload(db: &DatabaseConnection, upload_id: &str, key: &str) {
    multipart::create_upload(
        db,
        upload_id,
        &format!("encryption-{upload_id}"),
        "bucket",
        key,
        "none",
        None,
        None,
        Some("application/octet-stream"),
        None,
        &[],
        None,
        true,
    )
    .await
    .unwrap();
    multipart::upsert_part(db, upload_id, 1, "bafy-part", 3, "bafy-part")
        .await
        .unwrap();
}

async fn rows<T>(db: &DatabaseConnection) -> u64
where
    T: EntityTrait + 'static,
    T::Model: Send + Sync,
{
    T::find().count(db).await.unwrap()
}

async fn jobs(db: &DatabaseConnection) -> Vec<pin_job::Model> {
    pin_job::Entity::find()
        .order_by_asc(pin_job::Column::Id)
        .all(db)
        .await
        .unwrap()
}

async fn assert_no_publication_rows(db: &DatabaseConnection) {
    assert_eq!(rows::<object::Entity>(db).await, 0);
    assert_eq!(rows::<object_tag::Entity>(db).await, 0);
    assert_eq!(rows::<pin_lease::Entity>(db).await, 0);
    assert_eq!(rows::<pin_lease_target::Entity>(db).await, 0);
    assert_eq!(rows::<remote_pin::Entity>(db).await, 0);
    assert_eq!(rows::<pin_provider_usage::Entity>(db).await, 0);
    assert_eq!(rows::<pin_job::Entity>(db).await, 0);
}

fn import_request(id: &str, key: &str, decompress_prefix: Option<&str>) -> NewImportJob {
    NewImportJob {
        id: id.to_owned(),
        bucket: "bucket".to_owned(),
        key: key.to_owned(),
        source: ImportSource::Cid(
            "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".to_owned(),
        ),
        request_fingerprint: format!("sha256:{id}"),
        client_token: None,
        object_content_type: Some("application/octet-stream".to_owned()),
        metadata: HashMap::new(),
        tags: vec![tag("fixture", "true")],
        decompress_prefix: decompress_prefix.map(str::to_owned),
    }
}

async fn claimed_import(
    db: &DatabaseConnection,
    id: &str,
    key: &str,
    now: chrono::DateTime<Utc>,
) -> crate::import::ImportClaim {
    submit(db, import_request(id, key, None), now)
        .await
        .unwrap();
    claim_due(
        db,
        "import-worker",
        now,
        now + chrono::Duration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap()
    .claim
}

async fn publish_import_winner(
    db: &DatabaseConnection,
    job_id: &str,
    key: &str,
    cid: &str,
    now: chrono::DateTime<Utc>,
) {
    let claim = claimed_import(db, job_id, key, now).await;
    let destination = import_destination::Entity::find_by_id(("bucket".to_owned(), key.to_owned()))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    publish_import_object(
        db,
        request(
            object(&format!("{job_id}-object"), key, cid, 7),
            vec![],
            vec![],
        ),
        ImportPublicationGuard {
            job_id: claim.job_id,
            worker_id: claim.worker_id,
            claim_epoch: claim.claim_epoch,
            targets: vec![ExpectedImportTarget {
                bucket: "bucket".to_owned(),
                key: key.to_owned(),
                generation: destination.generation,
            }],
        },
        vec![],
        now,
        &limits(),
    )
    .await
    .unwrap();
}

async fn publication_row_counts(db: &DatabaseConnection) -> [u64; 7] {
    [
        rows::<object::Entity>(db).await,
        rows::<object_tag::Entity>(db).await,
        rows::<pin_lease::Entity>(db).await,
        rows::<pin_lease_target::Entity>(db).await,
        rows::<remote_pin::Entity>(db).await,
        rows::<pin_provider_usage::Entity>(db).await,
        rows::<pin_job::Entity>(db).await,
    ]
}

#[tokio::test]
async fn newer_completed_import_fences_every_older_exact_standard_publication() {
    for (key, reason) in [
        ("put-key", SupersedeReason::PutObject),
        ("copy-key", SupersedeReason::CopyObject),
        ("complete-key", SupersedeReason::CompleteMultipartUpload),
    ] {
        let db = setup().await;
        let now = Utc::now();
        let guard = admit_content_mutation(&db, "bucket", key, None, reason, now)
            .await
            .unwrap();
        publish_import_winner(
            &db,
            &format!("newer-{key}"),
            key,
            &format!("bafy-newer-{key}"),
            now,
        )
        .await;
        let rows_after_import = publication_row_counts(&db).await;

        let error = publish_standard_object(
            &db,
            automatic_and_manual_request(
                &format!("stale-{key}"),
                key,
                &format!("bafy-stale-{key}"),
            ),
            guard,
            &limits(),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, AppError::StaleContentMutation));
        assert_eq!(publication_row_counts(&db).await, rows_after_import);
        assert_eq!(
            crate::store::object::get_latest(&db, "bucket", key)
                .await
                .unwrap()
                .cid,
            format!("bafy-newer-{key}")
        );
    }
}

#[tokio::test]
async fn newer_completed_import_fences_older_delete_and_delete_objects_keys() {
    for (key, label, uses_batch_admission) in [
        ("delete-key", "delete", false),
        ("batch-key", "delete-objects", true),
    ] {
        let db = setup().await;
        let now = Utc::now();
        publish_object(
            &db,
            request(
                object(&format!("old-{label}"), key, "bafy-old", 7),
                vec![],
                vec![],
            ),
            &limits(),
        )
        .await
        .unwrap();
        let guard = if uses_batch_admission {
            let mut guards = admit_content_mutations(
                &db,
                "bucket",
                &[key.to_owned()],
                None,
                SupersedeReason::DeleteObject,
                now,
            )
            .await
            .unwrap();
            assert_eq!(guards.len(), 1);
            guards.pop().unwrap()
        } else {
            admit_content_mutation(&db, "bucket", key, None, SupersedeReason::DeleteObject, now)
                .await
                .unwrap()
        };
        publish_import_winner(
            &db,
            &format!("newer-{label}"),
            key,
            &format!("bafy-newer-{label}"),
            now,
        )
        .await;
        let rows_after_import = publication_row_counts(&db).await;

        let error = delete_latest_with_leases_guarded(&db, "bucket", key, guard, now)
            .await
            .unwrap_err();

        assert!(matches!(error, AppError::StaleContentMutation));
        assert_eq!(publication_row_counts(&db).await, rows_after_import);
        assert_eq!(
            crate::store::object::get_latest(&db, "bucket", key)
                .await
                .unwrap()
                .cid,
            format!("bafy-newer-{label}")
        );
    }
}

#[tokio::test]
async fn newer_exact_import_fences_older_prefix_publication_without_side_effects() {
    let db = setup().await;
    let now = Utc::now();
    let guard = crate::store::import::ownership::admit_content_and_prefix_mutation(
        &db,
        "bucket",
        "archive.zip",
        "literal%_/Case/",
        SupersedeReason::DecompressZip,
        now,
    )
    .await
    .unwrap();
    publish_import_winner(
        &db,
        "newer-prefix-winner",
        "literal%_/Case/file.txt",
        "bafy-newer-prefix",
        now,
    )
    .await;
    let rows_after_import = publication_row_counts(&db).await;

    let error = publish_standard_zip(
        &db,
        ZipPublicationRequest {
            archive: automatic_and_manual_request(
                "stale-archive",
                "archive.zip",
                "bafy-stale-archive",
            ),
            entries: vec![object(
                "stale-entry",
                "literal%_/Case/file.txt",
                "bafy-stale-entry",
                3,
            )],
        },
        guard,
        &limits(),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, AppError::StaleContentMutation));
    assert_eq!(publication_row_counts(&db).await, rows_after_import);
    assert!(
        crate::store::object::get_latest(&db, "bucket", "archive.zip")
            .await
            .is_err()
    );
    assert_eq!(
        crate::store::object::get_latest(&db, "bucket", "literal%_/Case/file.txt")
            .await
            .unwrap()
            .cid,
        "bafy-newer-prefix"
    );
}

#[tokio::test]
async fn unrelated_exact_import_does_not_fence_older_prefix_publication() {
    let db = setup().await;
    let now = Utc::now();
    let guard = crate::store::import::ownership::admit_content_and_prefix_mutation(
        &db,
        "bucket",
        "archive.zip",
        "outputs/",
        SupersedeReason::DecompressZip,
        now,
    )
    .await
    .unwrap();
    publish_import_winner(
        &db,
        "unrelated-winner",
        "outside/file.txt",
        "bafy-unrelated",
        now,
    )
    .await;

    publish_standard_zip(
        &db,
        ZipPublicationRequest {
            archive: request(
                object("archive", "archive.zip", "bafy-archive", 7),
                vec![],
                vec![],
            ),
            entries: vec![object("entry", "outputs/file.txt", "bafy-entry", 3)],
        },
        guard,
        &limits(),
    )
    .await
    .unwrap();

    assert_eq!(
        crate::store::object::get_latest(&db, "bucket", "outputs/file.txt")
            .await
            .unwrap()
            .cid,
        "bafy-entry"
    );
    assert_eq!(
        crate::store::object::get_latest(&db, "bucket", "outside/file.txt")
            .await
            .unwrap()
            .cid,
        "bafy-unrelated"
    );
}

#[tokio::test]
async fn newer_overlapping_prefix_fences_older_exact_standard_publication() {
    let db = setup().await;
    let now = Utc::now();
    let old_guard = admit_content_mutation(
        &db,
        "bucket",
        "outputs/file.txt",
        None,
        SupersedeReason::PutObject,
        now,
    )
    .await
    .unwrap();
    let _new_guard = crate::store::import::ownership::admit_content_and_prefix_mutation(
        &db,
        "bucket",
        "newer.zip",
        "outputs/",
        SupersedeReason::DecompressZip,
        now,
    )
    .await
    .unwrap();

    let error = publish_standard_object(
        &db,
        request(
            object("stale-exact", "outputs/file.txt", "bafy-stale", 7),
            vec![],
            vec![],
        ),
        old_guard,
        &limits(),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, AppError::StaleContentMutation));
    assert_no_publication_rows(&db).await;
}

#[tokio::test]
async fn newer_overlapping_prefix_fences_older_prefix_even_if_newer_never_publishes() {
    let db = setup().await;
    let now = Utc::now();
    let old_guard = crate::store::import::ownership::admit_content_and_prefix_mutation(
        &db,
        "bucket",
        "older.zip",
        "literal%_/Case/",
        SupersedeReason::DecompressZip,
        now,
    )
    .await
    .unwrap();
    let _new_guard = crate::store::import::ownership::admit_content_and_prefix_mutation(
        &db,
        "bucket",
        "newer.zip",
        "literal%_/Case/nested/",
        SupersedeReason::DecompressZip,
        now,
    )
    .await
    .unwrap();

    let error = publish_standard_zip(
        &db,
        ZipPublicationRequest {
            archive: request(
                object("stale-archive", "older.zip", "bafy-stale-archive", 7),
                vec![],
                vec![],
            ),
            entries: vec![object(
                "stale-entry",
                "literal%_/Case/file.txt",
                "bafy-stale-entry",
                3,
            )],
        },
        old_guard,
        &limits(),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, AppError::StaleContentMutation));
    assert_no_publication_rows(&db).await;
}

#[tokio::test]
async fn stale_import_guard_rolls_back_everything() {
    let db = setup().await;
    let now = Utc::now();
    let claim = claimed_import(&db, "job", "key", now).await;
    let guard = ImportPublicationGuard {
        job_id: claim.job_id.clone(),
        worker_id: claim.worker_id.clone(),
        claim_epoch: claim.claim_epoch,
        targets: vec![ExpectedImportTarget {
            bucket: "bucket".to_owned(),
            key: "key".to_owned(),
            generation: 2,
        }],
    };
    let rows = vec![import_job_result::ActiveModel {
        job_id: Set("job".to_owned()),
        sequence: Set(0),
        key: Set("key".to_owned()),
        cid: Set(Some("bafy-result".to_owned())),
        size: Set(Some(7)),
        error_code: Set(None),
        error_message: Set(None),
    }];

    let error = publish_import_object(
        &db,
        automatic_and_manual_request("published", "key", "bafy-published"),
        guard,
        rows,
        now,
        &limits(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, AppError::StaleImportOwnership));
    assert_no_publication_rows(&db).await;
    assert_eq!(
        import_job_result::Entity::find().count(&db).await.unwrap(),
        0
    );
    let job = import_job::Entity::find_by_id("job".to_owned())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (job.state.as_str(), job.locked_by.as_deref()),
        ("running", Some("import-worker"))
    );
    assert_eq!(
        import_job_target::Entity::find()
            .filter(import_job_target::Column::JobId.eq("job"))
            .count(&db)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn guarded_import_publication_completes_and_releases_ownership() {
    let db = setup().await;
    let now = Utc::now();
    let claim = claimed_import(&db, "job", "key", now).await;
    let result = publish_import_object(
        &db,
        request(
            object("published", "key", "bafy-published", 7),
            vec![],
            vec![],
        ),
        ImportPublicationGuard {
            job_id: claim.job_id.clone(),
            worker_id: claim.worker_id.clone(),
            claim_epoch: claim.claim_epoch,
            targets: vec![ExpectedImportTarget {
                bucket: "bucket".to_owned(),
                key: "key".to_owned(),
                generation: 1,
            }],
        },
        vec![import_job_result::ActiveModel {
            job_id: Set("wrong-job-id-is-overwritten".to_owned()),
            sequence: Set(0),
            key: Set("key".to_owned()),
            cid: Set(Some("bafy-published".to_owned())),
            size: Set(Some(7)),
            error_code: Set(None),
            error_message: Set(None),
        }],
        now,
        &limits(),
    )
    .await
    .unwrap();
    assert_eq!(result.object_id, "published");
    let job = import_job::Entity::find_by_id("job".to_owned())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            job.state.as_str(),
            job.final_cid.as_deref(),
            job.logical_size
        ),
        ("completed", Some("bafy-published"), Some(7))
    );
    let destination = crate::store::entities::import_destination::Entity::find_by_id((
        "bucket".to_owned(),
        "key".to_owned(),
    ))
    .one(&db)
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        (destination.generation, destination.owner_job_id),
        (1, None)
    );
    assert_eq!(
        import_job_target::Entity::find()
            .filter(import_job_target::Column::JobId.eq("job"))
            .count(&db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        import_job_result::Entity::find()
            .filter(import_job_result::Column::JobId.eq("job"))
            .count(&db)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn guarded_import_waiting_for_bucket_lock_rechecks_expired_lease_with_fresh_time() {
    let (_directory, db) = setup_file_backed("import-fresh-guard-time.sqlite").await;
    let submitted_at = Utc::now();
    submit(
        &db,
        import_request("fresh-time-job", "fresh-time-key", None),
        submitted_at,
    )
    .await
    .unwrap();
    let claim = claim_due(
        &db,
        "import-worker",
        submitted_at,
        submitted_at + chrono::Duration::milliseconds(200),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap()
    .claim;
    let blocker = db.begin().await.unwrap();
    blocker
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "UPDATE buckets SET owner = owner WHERE name = 'bucket'",
        ))
        .await
        .unwrap();

    let worker_db = db.clone();
    let publication = tokio::spawn(async move {
        publish_import_object(
            &worker_db,
            request(
                object("fresh-time-object", "fresh-time-key", "bafy-fresh-time", 7),
                vec![],
                vec![],
            ),
            ImportPublicationGuard {
                job_id: claim.job_id,
                worker_id: claim.worker_id,
                claim_epoch: claim.claim_epoch,
                targets: vec![ExpectedImportTarget {
                    bucket: "bucket".to_owned(),
                    key: "fresh-time-key".to_owned(),
                    generation: 1,
                }],
            },
            vec![import_job_result::ActiveModel {
                job_id: sea_orm::Set("fresh-time-job".to_owned()),
                sequence: sea_orm::Set(0),
                key: sea_orm::Set("fresh-time-key".to_owned()),
                cid: sea_orm::Set(Some("bafy-fresh-time".to_owned())),
                size: sea_orm::Set(Some(7)),
                error_code: sea_orm::Set(None),
                error_message: sea_orm::Set(None),
            }],
            submitted_at,
            &limits(),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(350)).await;
    blocker.rollback().await.unwrap();

    let error = publication.await.unwrap().unwrap_err();
    assert!(matches!(error, AppError::StaleImportOwnership));
    assert_no_publication_rows(&db).await;
    assert_eq!(
        import_job_result::Entity::find().count(&db).await.unwrap(),
        0
    );
    let job = import_job::Entity::find_by_id("fresh-time-job")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((job.state.as_str(), job.final_cid), ("running", None));
    assert_eq!(
        import_job_target::Entity::find()
            .filter(import_job_target::Column::JobId.eq("fresh-time-job"))
            .count(&db)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn guarded_import_completion_after_capped_job_deadline_rolls_back() {
    let _test_lock = test_gates::IMPORT_COMPLETION_TEST_LOCK.lock().await;
    let db = setup().await;
    let submitted_at = Utc::now();
    submit(
        &db,
        import_request("completion-deadline-job", "completion-deadline-key", None),
        submitted_at,
    )
    .await
    .unwrap();
    let claim = claim_due(
        &db,
        "import-worker",
        submitted_at,
        submitted_at + chrono::Duration::milliseconds(250),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap()
    .claim;
    let gate = std::sync::Arc::new(test_gates::ImportCompletionGate {
        job_id: "completion-deadline-job".to_owned(),
        arrived: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    *test_gates::IMPORT_BEFORE_COMPLETION.lock().await = Some(gate.clone());

    let worker_db = db.clone();
    let publication = tokio::spawn(async move {
        publish_import_object(
            &worker_db,
            request(
                object(
                    "completion-deadline-object",
                    "completion-deadline-key",
                    "bafy-completion-deadline",
                    7,
                ),
                vec![],
                vec![],
            ),
            ImportPublicationGuard {
                job_id: claim.job_id,
                worker_id: claim.worker_id,
                claim_epoch: claim.claim_epoch,
                targets: vec![ExpectedImportTarget {
                    bucket: "bucket".to_owned(),
                    key: "completion-deadline-key".to_owned(),
                    generation: 1,
                }],
            },
            vec![],
            submitted_at,
            &limits(),
        )
        .await
    });
    gate.arrived.notified().await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    gate.resume.notify_one();
    let result = publication.await.unwrap();
    *test_gates::IMPORT_BEFORE_COMPLETION.lock().await = None;

    assert!(matches!(result, Err(AppError::StaleImportOwnership)));
    assert_no_publication_rows(&db).await;
    assert_eq!(
        import_job_result::Entity::find().count(&db).await.unwrap(),
        0
    );
    let job = import_job::Entity::find_by_id("completion-deadline-job")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((job.state.as_str(), job.final_cid), ("running", None));
}

#[tokio::test]
async fn lost_import_zip_target_rolls_back_archive_entries_results_and_leases() {
    let db = setup().await;
    let now = Utc::now();
    let claim = claimed_import(&db, "zip-job", "archive.zip", now).await;
    let entry_generation = claim_extracted_target(&db, &claim, "bucket", "out/file.txt", now)
        .await
        .unwrap();
    import_job_target::Entity::delete_by_id((
        "zip-job".to_owned(),
        "bucket".to_owned(),
        "out/file.txt".to_owned(),
    ))
    .exec(&db)
    .await
    .unwrap();
    let guard = ImportPublicationGuard {
        job_id: claim.job_id.clone(),
        worker_id: claim.worker_id.clone(),
        claim_epoch: claim.claim_epoch,
        targets: vec![
            ExpectedImportTarget {
                bucket: "bucket".to_owned(),
                key: "archive.zip".to_owned(),
                generation: 1,
            },
            ExpectedImportTarget {
                bucket: "bucket".to_owned(),
                key: "out/file.txt".to_owned(),
                generation: entry_generation,
            },
        ],
    };
    let archive = automatic_and_manual_request("archive-object", "archive.zip", "bafy-archive");
    let error = publish_import_zip(
        &db,
        ZipPublicationRequest {
            archive,
            entries: vec![object("entry-object", "out/file.txt", "bafy-entry", 3)],
        },
        guard,
        vec![import_job_result::ActiveModel {
            job_id: Set("zip-job".to_owned()),
            sequence: Set(0),
            key: Set("out/file.txt".to_owned()),
            cid: Set(Some("bafy-entry".to_owned())),
            size: Set(Some(3)),
            error_code: Set(None),
            error_message: Set(None),
        }],
        now,
        &limits(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, AppError::StaleImportOwnership));
    assert_no_publication_rows(&db).await;
    assert_eq!(
        import_job_result::Entity::find().count(&db).await.unwrap(),
        0
    );
}

#[tokio::test]
async fn expired_attempt_cannot_publish_after_reclaim() {
    let db = setup().await;
    let now = Utc::now();
    let first = claimed_import(&db, "job", "key", now).await;
    let second = claim_due(
        &db,
        "replacement-worker",
        now + chrono::Duration::seconds(30),
        now + chrono::Duration::seconds(60),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap()
    .claim;
    assert_eq!(second.claim_epoch, first.claim_epoch + 1);
    let stale_guard = ImportPublicationGuard {
        job_id: first.job_id.clone(),
        worker_id: first.worker_id.clone(),
        claim_epoch: first.claim_epoch,
        targets: vec![ExpectedImportTarget {
            bucket: "bucket".to_owned(),
            key: "key".to_owned(),
            generation: 1,
        }],
    };
    let error = publish_import_object(
        &db,
        request(
            object("published", "key", "bafy-published", 7),
            vec![],
            vec![],
        ),
        stale_guard,
        vec![],
        now + chrono::Duration::seconds(30),
        &limits(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, AppError::StaleImportOwnership));
    assert_no_publication_rows(&db).await;
    let job = import_job::Entity::find_by_id("job".to_owned())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            job.state.as_str(),
            job.locked_by.as_deref(),
            job.claim_epoch
        ),
        ("running", Some("replacement-worker"), second.claim_epoch)
    );
}

#[tokio::test]
async fn stale_worker_released_at_prepublication_barrier_cannot_write() {
    let db = setup().await;
    let now = Utc::now();
    let claim = claimed_import(&db, "job", "key", now).await;
    let guard = ImportPublicationGuard {
        job_id: claim.job_id.clone(),
        worker_id: claim.worker_id.clone(),
        claim_epoch: claim.claim_epoch,
        targets: vec![ExpectedImportTarget {
            bucket: "bucket".to_owned(),
            key: "key".to_owned(),
            generation: 1,
        }],
    };
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let worker_barrier = barrier.clone();
    let worker_db = db.clone();
    let (release_worker, wait_for_release) = tokio::sync::oneshot::channel();
    let worker = tokio::spawn(async move {
        worker_barrier.wait().await;
        wait_for_release.await.unwrap();
        publish_import_object(
            &worker_db,
            request(
                object("published", "key", "bafy-published", 7),
                vec![],
                vec![],
            ),
            guard,
            vec![],
            now,
            &limits(),
        )
        .await
    });
    barrier.wait().await;
    admit_content_mutation(
        &db,
        "bucket",
        "key",
        None,
        crate::import::SupersedeReason::PutObject,
        now,
    )
    .await
    .unwrap();
    release_worker.send(()).unwrap();
    let error = worker.await.unwrap().unwrap_err();
    assert!(matches!(error, AppError::StaleImportOwnership));
    assert_no_publication_rows(&db).await;
}

#[test]
fn transaction_conflict_classification_is_narrow_and_explicit() {
    for message in [
        "stale publication attachment prelock compare-and-set",
        "Execution Error: (code: 5) database is locked",
        "Execution Error: database is busy",
        "duplicate key value violates unique constraint objects_latest",
        "error returned from database: deadlock detected (SQLSTATE 40P01)",
        "could not serialize access due to concurrent update (SQLSTATE 40001)",
    ] {
        assert!(
            is_retryable_transaction_conflict(&AppError::Database(message.to_owned())),
            "expected retryable conflict: {message}"
        );
    }
    for error in [
        AppError::InvalidPinningRequest("constraint requested by user".to_owned()),
        AppError::NoSuchUpload("upload".to_owned()),
        AppError::Database("constraint configuration is unavailable".to_owned()),
        AppError::Database("ordinary database failure".to_owned()),
    ] {
        assert!(!is_retryable_transaction_conflict(&error), "{error}");
    }
}

#[tokio::test]
async fn empty_persisted_tags_with_manual_policy_are_rejected_before_any_mutation() {
    let db = setup().await;
    let control = tag("ipfs-s3:pin", "true");

    let mut empty_persisted = request(
        object("split-empty", "split-empty", "bafy-split-empty", 7),
        vec![control.clone()],
        vec![intent(
            LeaseSource::Manual,
            ProviderMode::One,
            &["pinata"],
            ContentMode::Object,
        )],
    );
    empty_persisted.tags.clear();
    let error = publish_object(&db, empty_persisted, &limits())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        AppError::InvalidPinningRequest(ref message)
            if message == "publication tags do not match evaluated policy tags"
    ));
    assert!(!error.to_string().contains("ipfs-s3:pin"));
    assert_no_publication_rows(&db).await;
}

#[tokio::test]
async fn persisted_pin_control_without_policy_tags_is_rejected_before_any_mutation() {
    let db = setup().await;
    let mut empty_policy = request(
        object("split-policy", "split-policy", "bafy-split-policy", 7),
        vec![tag("ipfs-s3:pin", "true")],
        vec![],
    );
    empty_policy.policy.tags.clear();
    let error = publish_object(&db, empty_policy, &limits())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        AppError::InvalidPinningRequest(ref message)
            if message == "publication tags do not match evaluated policy tags"
    ));
    assert!(!error.to_string().contains("ipfs-s3:pin"));
    assert_no_publication_rows(&db).await;
}

async fn remote(db: &DatabaseConnection, provider: &str, cid: &str) -> remote_pin::Model {
    remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await
        .unwrap()
        .unwrap()
}

async fn targets_for_object(
    db: &DatabaseConnection,
    object_id: &str,
) -> Vec<pin_lease_target::Model> {
    let lease_ids = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq(object_id))
        .all(db)
        .await
        .unwrap()
        .into_iter()
        .map(|lease| lease.id)
        .collect::<Vec<_>>();
    pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.is_in(lease_ids))
        .order_by_asc(pin_lease_target::Column::Provider)
        .order_by_asc(pin_lease_target::Column::Cid)
        .all(db)
        .await
        .unwrap()
}

async fn manual_lease(db: &DatabaseConnection, object_id: &str) -> pin_lease::Model {
    pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq(object_id))
        .filter(pin_lease::Column::Source.eq("manual"))
        .one(db)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn publication_atomically_commits_object_tags_two_leases_targets_usage_and_jobs() {
    let db = setup().await;
    let result = publish_object(
        &db,
        automatic_and_manual_request("object-1", "key", "bafy-one"),
        &limits(),
    )
    .await
    .unwrap();

    assert_eq!(result.object_id, "object-1");
    assert_eq!(
        crate::store::object::get_latest(&db, "bucket", "key")
            .await
            .unwrap()
            .id,
        result.object_id
    );
    assert_eq!(
        object_tag::Entity::find()
            .filter(object_tag::Column::ObjectId.eq(&result.object_id))
            .count(&db)
            .await
            .unwrap(),
        3
    );
    let leases = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq(&result.object_id))
        .order_by_asc(pin_lease::Column::Source)
        .all(&db)
        .await
        .unwrap();
    assert_eq!(leases.len(), 2);
    assert!(leases.iter().all(|lease| {
        lease.state == "active"
            && lease.generation == 1
            && lease.last_touched_at == lease.created_at
            && lease.expires_at > lease.created_at
    }));
    assert_eq!(targets_for_object(&db, &result.object_id).await.len(), 3);
    for provider in ["pinata", "filebase"] {
        let usage = pin_provider_usage::Entity::find_by_id(provider)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (7, 1));
    }
    let jobs = jobs(&db).await;
    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(|job| {
        job.operation == "submit"
            && job.state == "pending"
            && job.submit_phase.as_deref() == Some("ready")
            && job.lease_id.is_some()
            && job.target_id.is_some()
            && job.expected_generation == Some(1)
            && job.expected_remote_epoch.is_none()
    }));
}

#[tokio::test]
async fn forced_outbox_failure_rolls_back_object_tags_leases_targets_remotes_and_usage() {
    let db = setup().await;
    db.execute(Statement::from_string(
        DatabaseBackend::Sqlite,
        "CREATE TRIGGER fail_pin_job_insert BEFORE INSERT ON pin_jobs \
         BEGIN SELECT RAISE(FAIL, 'forced pin job insert failure'); END;",
    ))
    .await
    .unwrap();
    let publication = request(
        object("object-fail", "key", "bafy-fail", 7),
        vec![tag("ipfs-s3:pin", "true")],
        vec![intent(
            LeaseSource::Manual,
            ProviderMode::One,
            &["pinata"],
            ContentMode::Object,
        )],
    );

    let error = publish_object(&db, publication, &limits())
        .await
        .unwrap_err();

    assert!(matches!(error, AppError::Database(_)));
    assert!(matches!(
        crate::store::object::get_latest(&db, "bucket", "key").await,
        Err(AppError::NoSuchKey(_))
    ));
    assert_eq!(rows::<object::Entity>(&db).await, 0);
    assert_eq!(rows::<object_tag::Entity>(&db).await, 0);
    assert_eq!(rows::<pin_lease::Entity>(&db).await, 0);
    assert_eq!(rows::<pin_lease_target::Entity>(&db).await, 0);
    assert_eq!(rows::<remote_pin::Entity>(&db).await, 0);
    assert_eq!(rows::<pin_provider_usage::Entity>(&db).await, 0);
    assert_eq!(rows::<pin_job::Entity>(&db).await, 0);
}

#[tokio::test]
async fn overwrite_ends_old_leases_once_per_remote_and_enqueues_only_remote_scoped_work() {
    let db = setup().await;
    publish_object(
        &db,
        automatic_and_manual_request("old-object", "key", "bafy-old"),
        &limits(),
    )
    .await
    .unwrap();
    let before_pinata = remote(&db, "pinata", "bafy-old").await.epoch;
    let before_filebase = remote(&db, "filebase", "bafy-old").await.epoch;

    publish_object(
        &db,
        request(object("new-object", "key", "bafy-new", 9), vec![], vec![]),
        &limits(),
    )
    .await
    .unwrap();

    let old_leases = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq("old-object"))
        .all(&db)
        .await
        .unwrap();
    assert!(
        old_leases
            .iter()
            .all(|lease| lease.state == "cancelled" && lease.generation == 2)
    );
    assert!(
        targets_for_object(&db, "old-object")
            .await
            .iter()
            .all(|target| target.state == "released")
    );
    assert_eq!(
        remote(&db, "pinata", "bafy-old").await.epoch,
        before_pinata + 1
    );
    assert_eq!(
        remote(&db, "filebase", "bafy-old").await.epoch,
        before_filebase + 1
    );
    let cleanup = jobs(&db)
        .await
        .into_iter()
        .filter(|job| matches!(job.operation.as_str(), "unpin" | "reconcile"))
        .collect::<Vec<_>>();
    assert_eq!(cleanup.len(), 2);
    assert!(cleanup.iter().all(|job| {
        job.operation == "unpin"
            && job.lease_id.is_none()
            && job.target_id.is_none()
            && job.expected_generation.is_none()
            && job.expected_remote_epoch.is_some()
            && job.submit_phase.is_none()
    }));
}

#[tokio::test]
async fn delete_marks_latest_false_and_ends_leases_without_deleting_rows() {
    let db = setup().await;
    publish_object(
        &db,
        automatic_and_manual_request("object-delete", "delete-key", "bafy-delete"),
        &limits(),
    )
    .await
    .unwrap();
    let before = remote(&db, "pinata", "bafy-delete").await.epoch;

    assert!(
        delete_latest_with_leases(&db, "bucket", "delete-key", Utc::now())
            .await
            .unwrap()
    );
    assert!(
        !delete_latest_with_leases(&db, "bucket", "delete-key", Utc::now())
            .await
            .unwrap()
    );

    let stored = object::Entity::find_by_id("object-delete")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(!stored.is_latest);
    assert_eq!(remote(&db, "pinata", "bafy-delete").await.epoch, before + 1);
    assert!(
        pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq("object-delete"))
            .all(&db)
            .await
            .unwrap()
            .iter()
            .all(|lease| lease.state == "cancelled")
    );
}

#[tokio::test]
async fn multipart_zip_failure_points_preserve_upload_parts_prior_latest_and_rollback_new_rows() {
    for failure in ["archive", "entry", "lease", "job"] {
        let db = setup().await;
        crate::store::object::upsert(
            &db,
            "prior-object",
            "bucket",
            "archive.zip",
            "bafy-prior",
            5,
            Some("application/zip"),
            "bafy-prior",
            None,
            false,
            None,
            None,
            false,
        )
        .await
        .unwrap();
        crate::store::pinning::tags::replace_object_tags(
            &db,
            "prior-object",
            &[tag("prior", "tag")],
        )
        .await
        .unwrap();
        seed_upload(&db, "upload-zip", "archive.zip").await;
        let trigger = match failure {
            "archive" => {
                "CREATE TRIGGER fail_publication BEFORE INSERT ON objects WHEN NEW.id = 'archive-new' BEGIN SELECT RAISE(FAIL, 'archive'); END;"
            }
            "entry" => {
                "CREATE TRIGGER fail_publication BEFORE INSERT ON objects WHEN NEW.id = 'entry-a' BEGIN SELECT RAISE(FAIL, 'entry'); END;"
            }
            "lease" => {
                "CREATE TRIGGER fail_publication BEFORE INSERT ON pin_leases BEGIN SELECT RAISE(FAIL, 'lease'); END;"
            }
            "job" => {
                "CREATE TRIGGER fail_publication BEFORE INSERT ON pin_jobs BEGIN SELECT RAISE(FAIL, 'job'); END;"
            }
            _ => unreachable!(),
        };
        db.execute_unprepared(trigger).await.unwrap();
        let archive = automatic_and_manual_request("archive-new", "archive.zip", "bafy-archive");
        let publication = ZipPublicationRequest {
            archive,
            entries: vec![
                object("entry-a", "unzipped/a.txt", "bafy-entry-a", 3),
                object("entry-b", "unzipped/b.txt", "bafy-entry-b", 4),
            ],
        };

        let error = publish_completed_zip(&db, "upload-zip", publication, &limits())
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            CommitCompletedUploadError::RolledBack {
                ref completion_attempt_id,
                ..
            } if completion_attempt_id == "archive-new"
        ));
        assert!(multipart::get_upload(&db, "upload-zip").await.is_ok());
        assert_eq!(
            multipart::list_parts(&db, "upload-zip")
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            crate::store::object::get_latest(&db, "bucket", "archive.zip")
                .await
                .unwrap()
                .id,
            "prior-object",
            "failure point {failure}"
        );
        assert_eq!(
            rows::<object::Entity>(&db).await,
            1,
            "failure point {failure}"
        );
        assert_eq!(
            rows::<object_tag::Entity>(&db).await,
            1,
            "failure point {failure}"
        );
        assert_eq!(
            rows::<pin_lease::Entity>(&db).await,
            0,
            "failure point {failure}"
        );
        assert_eq!(
            rows::<pin_lease_target::Entity>(&db).await,
            0,
            "failure point {failure}"
        );
        assert_eq!(
            rows::<remote_pin::Entity>(&db).await,
            0,
            "failure point {failure}"
        );
        assert_eq!(
            rows::<pin_provider_usage::Entity>(&db).await,
            0,
            "failure point {failure}"
        );
        assert_eq!(
            rows::<pin_job::Entity>(&db).await,
            0,
            "failure point {failure}"
        );
    }
}

#[tokio::test]
async fn multipart_non_zip_and_zip_success_publish_and_remove_upload_parts_atomically() {
    let db = setup().await;
    seed_upload(&db, "upload-object", "root.bin").await;
    let mut root = object("complete-object", "root.bin", "bafy-root", 7);
    root.multipart = true;
    let root = request(
        root,
        vec![tag("ipfs-s3:pin", "true")],
        vec![intent(
            LeaseSource::Manual,
            ProviderMode::One,
            &["pinata"],
            ContentMode::Object,
        )],
    );
    publish_completed_upload(&db, "upload-object", root, &limits())
        .await
        .unwrap();
    assert!(multipart::get_upload(&db, "upload-object").await.is_err());
    assert!(
        multipart::list_parts(&db, "upload-object")
            .await
            .unwrap()
            .is_empty()
    );
    let completed_root = object::Entity::find_by_id("complete-object")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(completed_root.multipart);
    assert_eq!(
        object_tag::Entity::find()
            .filter(object_tag::Column::ObjectId.eq("complete-object"))
            .count(&db)
            .await
            .unwrap(),
        1
    );
    assert_eq!(targets_for_object(&db, "complete-object").await.len(), 1);

    seed_upload(&db, "upload-zip", "archive.zip").await;
    let mut archive_object = object("complete-zip", "archive.zip", "bafy-archive", 7);
    archive_object.multipart = true;
    let archive = request(
        archive_object,
        vec![tag("ipfs-s3:pin", "true")],
        vec![intent(
            LeaseSource::Manual,
            ProviderMode::All,
            &["pinata", "filebase"],
            ContentMode::Decompressed,
        )],
    );
    publish_completed_zip(
        &db,
        "upload-zip",
        ZipPublicationRequest {
            archive,
            entries: vec![
                object("zip-entry-a", "out/a", "bafy-a", 3),
                object("zip-entry-b", "out/b", "bafy-b", 4),
            ],
        },
        &limits(),
    )
    .await
    .unwrap();
    assert!(multipart::get_upload(&db, "upload-zip").await.is_err());
    assert!(
        multipart::list_parts(&db, "upload-zip")
            .await
            .unwrap()
            .is_empty()
    );
    let cids = object::Entity::find()
        .filter(object::Column::Id.is_in(["complete-zip", "zip-entry-a", "zip-entry-b"]))
        .order_by_asc(object::Column::Cid)
        .all(&db)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.cid)
        .collect::<Vec<_>>();
    assert_eq!(cids, vec!["bafy-a", "bafy-archive", "bafy-b"]);
    let completed_zip = object::Entity::find_by_id("complete-zip")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(completed_zip.multipart);
    assert_eq!(
        object_tag::Entity::find()
            .filter(object_tag::Column::ObjectId.eq("complete-zip"))
            .count(&db)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq("complete-object"))
            .count(&db)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq("complete-zip"))
            .count(&db)
            .await
            .unwrap(),
        1
    );
    assert_eq!(targets_for_object(&db, "complete-zip").await.len(), 4);
    let pinata_usage = pin_provider_usage::Entity::find_by_id("pinata")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (pinata_usage.reserved_bytes, pinata_usage.reserved_pins),
        (14, 3)
    );
    let filebase_usage = pin_provider_usage::Entity::find_by_id("filebase")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (filebase_usage.reserved_bytes, filebase_usage.reserved_pins),
        (7, 2)
    );
    let canonical_jobs = jobs(&db).await;
    assert_eq!(canonical_jobs.len(), 5);
    assert!(canonical_jobs.iter().all(|job| {
        job.operation == "submit"
            && job.state == "pending"
            && job.submit_phase.as_deref() == Some("ready")
            && job.expected_generation == Some(1)
            && job.expected_remote_epoch.is_none()
            && job.lease_id.is_some()
            && job.target_id.is_some()
    }));
}

#[tokio::test]
async fn repeated_cid_reuse_keeps_unique_usage_and_advances_epoch_per_new_target() {
    let db = setup().await;
    let manual = |id: &str, key: &str| {
        request(
            object(id, key, "bafy-shared", 7),
            vec![tag("ipfs-s3:pin", "true")],
            vec![intent(
                LeaseSource::Manual,
                ProviderMode::One,
                &["pinata"],
                ContentMode::Object,
            )],
        )
    };
    publish_object(&db, manual("shared-a", "a"), &limits())
        .await
        .unwrap();
    assert_eq!(remote(&db, "pinata", "bafy-shared").await.epoch, 1);
    publish_object(&db, manual("shared-b", "b"), &limits())
        .await
        .unwrap();

    assert_eq!(remote(&db, "pinata", "bafy-shared").await.epoch, 2);
    let usage = pin_provider_usage::Entity::find_by_id("pinata")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (7, 1));
    assert_eq!(
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::Provider.eq("pinata"))
            .filter(pin_lease_target::Column::Cid.eq("bafy-shared"))
            .count(&db)
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn concurrent_different_keys_sharing_a_remote_retry_rolled_back_sqlite_conflicts() {
    let (_directory, db) = setup_file_backed("publication-shared-cid.sqlite").await;
    let publication = |id: &str, key: &str| {
        request(
            object(id, key, "bafy-concurrent-shared", 7),
            vec![],
            vec![intent(
                LeaseSource::Automatic,
                ProviderMode::One,
                &["pinata"],
                ContentMode::Object,
            )],
        )
    };

    let provider_limits = limits();
    let (first, second) = tokio::join!(
        publish_object(&db, publication("concurrent-a", "key-a"), &provider_limits,),
        publish_object(&db, publication("concurrent-b", "key-b"), &provider_limits,),
    );
    assert!(first.is_ok(), "first publication failed: {first:?}");
    assert!(second.is_ok(), "second publication failed: {second:?}");

    let usage = pin_provider_usage::Entity::find_by_id("pinata")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (7, 1));
    assert_eq!(
        remote(&db, "pinata", "bafy-concurrent-shared").await.epoch,
        2
    );
    assert_eq!(rows::<pin_lease_target::Entity>(&db).await, 2);
    assert_eq!(
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::Cid.eq("bafy-concurrent-shared"))
            .filter(pin_lease_target::Column::State.eq("waiting"))
            .count(&db)
            .await
            .unwrap(),
        2
    );
    let jobs = jobs(&db).await;
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        jobs.iter()
            .filter(|job| job.operation == "submit" && job.state != "done")
            .count(),
        1
    );
}

#[tokio::test]
async fn concurrent_same_key_overwrites_retry_and_leave_one_consistent_lifecycle() {
    let (_directory, db) = setup_file_backed("publication-same-key.sqlite").await;
    let publication = |id: &str| {
        request(
            object(id, "shared-key", "bafy-concurrent-overwrite", 7),
            vec![],
            vec![intent(
                LeaseSource::Automatic,
                ProviderMode::One,
                &["pinata"],
                ContentMode::Object,
            )],
        )
    };
    publish_object(&db, publication("original"), &limits())
        .await
        .unwrap();
    remote_pin::Entity::update_many()
        .col_expr(remote_pin::Column::Status, "pinned".into())
        .filter(remote_pin::Column::Provider.eq("pinata"))
        .filter(remote_pin::Column::Cid.eq("bafy-concurrent-overwrite"))
        .exec(&db)
        .await
        .unwrap();
    pin_lease_target::Entity::update_many()
        .col_expr(pin_lease_target::Column::State, "pinned".into())
        .exec(&db)
        .await
        .unwrap();
    pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, "done".into())
        .exec(&db)
        .await
        .unwrap();

    let provider_limits = limits();
    let (first, second) = tokio::join!(
        publish_object(&db, publication("overwrite-a"), &provider_limits),
        publish_object(&db, publication("overwrite-b"), &provider_limits),
    );
    assert!(first.is_ok(), "first overwrite failed: {first:?}");
    assert!(second.is_ok(), "second overwrite failed: {second:?}");

    let objects = object::Entity::find()
        .filter(object::Column::Key.eq("shared-key"))
        .all(&db)
        .await
        .unwrap();
    assert_eq!(objects.len(), 3);
    let latest = objects
        .iter()
        .filter(|object| object.is_latest)
        .collect::<Vec<_>>();
    assert_eq!(latest.len(), 1);

    let leases = pin_lease::Entity::find().all(&db).await.unwrap();
    assert_eq!(leases.len(), 3);
    assert_eq!(
        leases
            .iter()
            .filter(|lease| lease.state == "active")
            .count(),
        1
    );
    assert_eq!(
        leases
            .iter()
            .filter(|lease| lease.state == "cancelled")
            .count(),
        2
    );
    assert!(
        leases
            .iter()
            .filter(|lease| lease.state == "cancelled")
            .all(|lease| lease.generation == 2)
    );
    assert_eq!(
        leases
            .iter()
            .find(|lease| lease.state == "active")
            .map(|lease| lease.owner_object_id.as_str()),
        Some(latest[0].id.as_str())
    );

    let targets = pin_lease_target::Entity::find().all(&db).await.unwrap();
    assert_eq!(
        targets
            .iter()
            .filter(|target| target.state == "pinned")
            .count(),
        1
    );
    assert_eq!(
        targets
            .iter()
            .filter(|target| target.state == "released")
            .count(),
        2
    );
    let usage = pin_provider_usage::Entity::find_by_id("pinata")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (7, 1));
    assert_eq!(
        remote(&db, "pinata", "bafy-concurrent-overwrite")
            .await
            .epoch,
        5
    );
    let pending_unpins = jobs(&db)
        .await
        .into_iter()
        .filter(|job| job.operation == "unpin" && job.state == "pending")
        .collect::<Vec<_>>();
    assert_eq!(pending_unpins.len(), 2);
    assert_eq!(
        pending_unpins
            .iter()
            .map(|job| job.expected_remote_epoch.unwrap())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([2, 4])
    );
    assert!(pending_unpins.iter().all(|job| {
        job.lease_id.is_none() && job.target_id.is_none() && job.expected_generation.is_none()
    }));
}

#[tokio::test]
async fn one_selects_first_reservable_provider_all_fans_out_and_exhaustion_persists_first() {
    let db = setup().await;
    let one = request(
        object("one", "one", "bafy-one-provider", 7),
        vec![],
        vec![intent(
            LeaseSource::Automatic,
            ProviderMode::One,
            &["filebase", "pinata"],
            ContentMode::Object,
        )],
    );
    publish_object(&db, one, &limits()).await.unwrap();
    let one_targets = targets_for_object(&db, "one").await;
    assert_eq!(one_targets.len(), 1);
    assert_eq!(one_targets[0].provider, "pinata");

    let all = request(
        object("all", "all", "bafy-all-provider", 7),
        vec![],
        vec![intent(
            LeaseSource::Automatic,
            ProviderMode::All,
            &["filebase", "pinata"],
            ContentMode::Object,
        )],
    );
    publish_object(&db, all, &limits()).await.unwrap();
    assert_eq!(
        targets_for_object(&db, "all")
            .await
            .into_iter()
            .map(|target| target.provider)
            .collect::<Vec<_>>(),
        vec!["filebase", "pinata"]
    );

    let blocked_limits = ProviderLimitMap::from([
        (
            "pinata".to_owned(),
            ProviderLimits {
                priority: 1,
                max_bytes: 1,
                max_pins: 1,
                enabled: true,
            },
        ),
        (
            "filebase".to_owned(),
            ProviderLimits {
                priority: 2,
                max_bytes: 1,
                max_pins: 1,
                enabled: true,
            },
        ),
    ]);
    publish_object(
        &db,
        request(
            object("blocked", "blocked", "bafy-blocked", 7),
            vec![],
            vec![intent(
                LeaseSource::Automatic,
                ProviderMode::One,
                &["filebase", "pinata"],
                ContentMode::Object,
            )],
        ),
        &blocked_limits,
    )
    .await
    .unwrap();
    let blocked = targets_for_object(&db, "blocked").await;
    assert_eq!(blocked.len(), 1);
    assert_eq!(
        (blocked[0].provider.as_str(), blocked[0].state.as_str()),
        ("pinata", "quota_blocked")
    );
    assert!(
        delete_latest_with_leases(&db, "bucket", "blocked", Utc::now())
            .await
            .unwrap()
    );
    assert_eq!(
        pin_lease_target::Entity::find_by_id(&blocked[0].id)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .state,
        "released"
    );
}

#[tokio::test]
async fn overwrite_same_cid_does_not_create_a_second_submit_while_old_call_is_ambiguous() {
    let db = setup().await;
    let publication = |id: &str| {
        request(
            object(id, "same-key", "bafy-ambiguous", 7),
            vec![],
            vec![intent(
                LeaseSource::Automatic,
                ProviderMode::One,
                &["pinata"],
                ContentMode::Object,
            )],
        )
    };
    publish_object(&db, publication("ambiguous-old"), &limits())
        .await
        .unwrap();
    let old_submit = jobs(&db)
        .await
        .into_iter()
        .find(|job| job.operation == "submit")
        .unwrap();
    let locked_until = Utc::now() + chrono::TimeDelta::minutes(1);
    pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, "running".into())
        .col_expr(pin_job::Column::SubmitPhase, Some("calling").into())
        .col_expr(pin_job::Column::LockedUntil, Some(locked_until).into())
        .filter(pin_job::Column::Id.eq(&old_submit.id))
        .exec(&db)
        .await
        .unwrap();

    publish_object(&db, publication("ambiguous-new"), &limits())
        .await
        .unwrap();

    let submit_jobs = jobs(&db)
        .await
        .into_iter()
        .filter(|job| job.operation == "submit" && job.state != "done")
        .collect::<Vec<_>>();
    assert_eq!(submit_jobs.len(), 1);
    assert_eq!(submit_jobs[0].id, old_submit.id);
    let current_epoch = remote(&db, "pinata", "bafy-ambiguous").await.epoch;
    assert_eq!(
        jobs(&db)
            .await
            .into_iter()
            .filter(|job| {
                job.operation == "reconcile"
                    && job.expected_remote_epoch == Some(current_epoch)
                    && job.lease_id.is_none()
                    && job.target_id.is_none()
            })
            .count(),
        1
    );

    let lease = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq("ambiguous-new"))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let target = targets_for_object(&db, "ambiguous-new").await.remove(0);
    let crate::store::pinning::jobs::NewPinJob::Target(job) =
        crate::store::pinning::jobs::submit_job(
            "pinata",
            "bafy-ambiguous",
            &lease.id,
            &target.id,
            lease.generation,
            Utc::now(),
        )
    else {
        unreachable!()
    };
    crate::store::pinning::jobs::ensure_or_reactivate_submit_job(&db, job, Utc::now())
        .await
        .unwrap();
    assert_eq!(
        jobs(&db)
            .await
            .into_iter()
            .filter(|job| job.operation == "submit" && job.state != "done")
            .count(),
        1,
        "the shared canonical ensure API must also preserve ambiguous ownership"
    );
}

#[tokio::test]
async fn new_one_mode_target_on_failed_remote_gets_current_epoch_reconcile() {
    let db = setup().await;
    let publication = |id: &str, key: &str| {
        request(
            object(id, key, "bafy-one-failed", 7),
            vec![],
            vec![intent(
                LeaseSource::Automatic,
                ProviderMode::One,
                &["pinata"],
                ContentMode::Object,
            )],
        )
    };
    publish_object(&db, publication("one-failed-a", "a"), &limits())
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE remote_pins SET status = 'failed', request_id = 'failed-request', \
         failure_attempts = 8, next_retry_at = NULL WHERE provider = 'pinata' AND cid = 'bafy-one-failed'; \
         DELETE FROM pin_jobs;",
    )
    .await
    .unwrap();

    publish_object(&db, publication("one-failed-b", "b"), &limits())
        .await
        .unwrap();

    let remote = remote(&db, "pinata", "bafy-one-failed").await;
    assert_eq!(remote.failure_attempts, 0);
    assert_eq!(
        jobs(&db)
            .await
            .into_iter()
            .filter(|job| {
                job.operation == "reconcile" && job.expected_remote_epoch == Some(remote.epoch)
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn zip_decompressed_manual_targets_only_successful_entries_while_automatic_targets_archive() {
    let db = setup().await;
    let archive = request(
        object("zip", "archive.zip", "bafy-archive", 10),
        vec![tag("ipfs-s3:pin", "true")],
        vec![
            intent(
                LeaseSource::Automatic,
                ProviderMode::One,
                &["pinata"],
                ContentMode::Object,
            ),
            intent(
                LeaseSource::Manual,
                ProviderMode::One,
                &["pinata"],
                ContentMode::Decompressed,
            ),
        ],
    );
    publish_zip(
        &db,
        ZipPublicationRequest {
            archive,
            entries: vec![
                object("entry-ok-a", "out/a", "bafy-entry-a", 3),
                object("entry-ok-b", "out/b", "bafy-entry-b", 4),
            ],
        },
        &limits(),
    )
    .await
    .unwrap();

    let leases = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq("zip"))
        .all(&db)
        .await
        .unwrap();
    assert_eq!(leases.len(), 2);
    let automatic_id = leases
        .iter()
        .find(|lease| lease.source == "automatic")
        .unwrap()
        .id
        .clone();
    let manual_id = leases
        .iter()
        .find(|lease| lease.source == "manual")
        .unwrap()
        .id
        .clone();
    let automatic_cids = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(automatic_id))
        .all(&db)
        .await
        .unwrap()
        .into_iter()
        .map(|target| target.cid)
        .collect::<Vec<_>>();
    let mut manual_cids = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(manual_id))
        .all(&db)
        .await
        .unwrap()
        .into_iter()
        .map(|target| target.cid)
        .collect::<Vec<_>>();
    manual_cids.sort();
    assert_eq!(automatic_cids, vec!["bafy-archive"]);
    assert_eq!(manual_cids, vec!["bafy-entry-a", "bafy-entry-b"]);
    assert!(!manual_cids.contains(&"bafy-archive".to_owned()));
    assert_eq!(
        pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.is_in(["entry-ok-a", "entry-ok-b"]))
            .count(&db)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn shared_reserved_automatic_manual_and_copy_targets_keep_one_canonical_submit() {
    let db = setup().await;
    publish_object(
        &db,
        automatic_and_manual_request("shared-root", "root", "bafy-shared"),
        &limits(),
    )
    .await
    .unwrap();
    publish_object(
        &db,
        request(
            object("shared-copy", "copy", "bafy-shared", 7),
            vec![tag("ipfs-s3:pin", "true")],
            vec![intent(
                LeaseSource::Manual,
                ProviderMode::One,
                &["pinata"],
                ContentMode::Object,
            )],
        ),
        &limits(),
    )
    .await
    .unwrap();

    let pinata_submits = jobs(&db)
        .await
        .into_iter()
        .filter(|job| {
            job.provider == "pinata" && job.cid == "bafy-shared" && job.operation == "submit"
        })
        .collect::<Vec<_>>();
    assert_eq!(pinata_submits.len(), 1);
    assert_eq!(
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::Provider.eq("pinata"))
            .filter(pin_lease_target::Column::Cid.eq("bafy-shared"))
            .filter(pin_lease_target::Column::State.eq("waiting"))
            .count(&db)
            .await
            .unwrap(),
        3
    );
}

#[tokio::test]
async fn pinned_reuse_projects_all_targets_pinned_without_new_job() {
    let db = setup().await;
    publish_object(
        &db,
        request(
            object("pinned-a", "pinned-a", "bafy-pinned", 7),
            vec![],
            vec![intent(
                LeaseSource::Automatic,
                ProviderMode::One,
                &["pinata"],
                ContentMode::Object,
            )],
        ),
        &limits(),
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE remote_pins SET status = 'pinned', request_id = 'request-pinned' \
         WHERE provider = 'pinata' AND cid = 'bafy-pinned'; DELETE FROM pin_jobs;",
    )
    .await
    .unwrap();
    publish_object(
        &db,
        request(
            object("pinned-b", "pinned-b", "bafy-pinned", 7),
            vec![],
            vec![intent(
                LeaseSource::Automatic,
                ProviderMode::One,
                &["pinata"],
                ContentMode::Object,
            )],
        ),
        &limits(),
    )
    .await
    .unwrap();

    assert!(jobs(&db).await.is_empty());
    let states = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Cid.eq("bafy-pinned"))
        .all(&db)
        .await
        .unwrap();
    assert_eq!(states.len(), 2);
    assert!(states.iter().all(|target| target.state == "pinned"));
}

#[tokio::test]
async fn queued_and_pinning_reuse_preserve_one_canonical_poll() {
    for status in ["queued", "pinning"] {
        let db = setup().await;
        publish_object(
            &db,
            request(
                object("queued-a", "a", "bafy-queued", 7),
                vec![],
                vec![intent(
                    LeaseSource::Automatic,
                    ProviderMode::One,
                    &["pinata"],
                    ContentMode::Object,
                )],
            ),
            &limits(),
        )
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "UPDATE remote_pins SET status = '{status}', request_id = 'request-queued' \
             WHERE provider = 'pinata' AND cid = 'bafy-queued'; DELETE FROM pin_jobs;"
        ))
        .await
        .unwrap();
        publish_object(
            &db,
            request(
                object("queued-b", "b", "bafy-queued", 7),
                vec![],
                vec![intent(
                    LeaseSource::Automatic,
                    ProviderMode::One,
                    &["pinata"],
                    ContentMode::Object,
                )],
            ),
            &limits(),
        )
        .await
        .unwrap();

        let poll_jobs = jobs(&db)
            .await
            .into_iter()
            .filter(|job| job.operation == "poll")
            .collect::<Vec<_>>();
        assert_eq!(poll_jobs.len(), 1, "status {status}");
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::Cid.eq("bafy-queued"))
                .filter(pin_lease_target::Column::State.eq("submitted"))
                .count(&db)
                .await
                .unwrap(),
            2,
            "status {status}"
        );
    }
}

#[tokio::test]
async fn genuinely_new_failed_all_target_resets_once_while_passive_equal_and_tags_do_not() {
    let db = setup().await;
    let all_request = |id: &str, key: &str| {
        request(
            object(id, key, "bafy-failed", 7),
            vec![tag("ipfs-s3:pin", "true")],
            vec![intent(
                LeaseSource::Manual,
                ProviderMode::All,
                &["pinata"],
                ContentMode::Object,
            )],
        )
    };
    publish_object(&db, all_request("failed-a", "a"), &limits())
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE remote_pins SET status = 'failed', request_id = 'request-failed', \
         failure_attempts = 8, next_retry_at = NULL WHERE provider = 'pinata' AND cid = 'bafy-failed'; \
         DELETE FROM pin_jobs;",
    )
    .await
    .unwrap();

    publish_object(&db, all_request("failed-b", "b"), &limits())
        .await
        .unwrap();

    let reset = remote(&db, "pinata", "bafy-failed").await;
    assert_eq!(reset.epoch, 2);
    assert_eq!(reset.failure_attempts, 0);
    assert!(reset.next_retry_at.is_some());
    assert_eq!(
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::Cid.eq("bafy-failed"))
            .filter(pin_lease_target::Column::State.eq("degraded"))
            .count(&db)
            .await
            .unwrap(),
        2
    );
    let reconciles = jobs(&db)
        .await
        .into_iter()
        .filter(|job| job.operation == "reconcile" && job.expected_remote_epoch == Some(2))
        .collect::<Vec<_>>();
    assert_eq!(reconciles.len(), 1);

    db.execute_unprepared(
        "UPDATE remote_pins SET failure_attempts = 8, next_retry_at = NULL \
         WHERE provider = 'pinata' AND cid = 'bafy-failed'; DELETE FROM pin_jobs;",
    )
    .await
    .unwrap();
    crate::store::pinning::tags::replace_object_tags(
        &db,
        "failed-b",
        &[tag("ipfs-s3:pin", "true"), tag("team", "replacement")],
    )
    .await
    .unwrap();
    let lease = manual_lease(&db, "failed-b").await;
    let target = targets_for_object(&db, "failed-b").await.remove(0);
    crate::store::pinning::leases::project_target_from_remote(&db, &target.id, Utc::now())
        .await
        .unwrap();
    let kept = crate::store::pinning::leases::renew_manual_lease(
        &db,
        "failed-b",
        &lease.id,
        lease.expires_at,
        Utc::now(),
    )
    .await
    .unwrap();
    assert!(matches!(
        kept,
        crate::store::pinning::leases::ManualLeaseRenewalOutcome::Kept { generation: 1 }
    ));
    assert_eq!(
        remote(&db, "pinata", "bafy-failed").await.failure_attempts,
        8
    );
}

#[tokio::test]
async fn completed_publication_reconciliation_uses_exact_attempt_and_upload_absence() {
    let db = setup().await;
    seed_upload(&db, "upload-reconcile", "root.bin").await;
    let expected = object("attempt-exact", "root.bin", "bafy-exact", 7);
    publish_completed_upload(
        &db,
        "upload-reconcile",
        request(expected.clone(), vec![], vec![]),
        &limits(),
    )
    .await
    .unwrap();
    assert!(matches!(
        reconcile_completed_publication(&db, "upload-reconcile", &expected).await,
        ReconciledCommitOutcome::Committed
    ));

    let absent = object("attempt-absent", "root.bin", "bafy-exact", 7);
    assert!(matches!(
        reconcile_completed_publication(&db, "upload-reconcile", &absent).await,
        ReconciledCommitOutcome::NotCommitted
    ));

    let mut mismatched = expected.clone();
    mismatched.cid = "bafy-other".to_owned();
    assert!(matches!(
        reconcile_completed_publication(&db, "upload-reconcile", &mismatched).await,
        ReconciledCommitOutcome::Unknown(AppError::Internal(_))
    ));
}
