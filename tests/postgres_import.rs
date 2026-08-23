use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use ipfs_s3_gateway::{
    error::{AppError, AppResult},
    import::ImportSource,
    pinning::{config::ProviderLimitMap, policy::PublicationPolicy, tags::ObjectTag},
    store::{
        self,
        entities::{
            bucket, import_destination, import_job, import_job_target, import_prefix_claim, object,
        },
        import::{
            jobs::{NewImportJob, claim_due},
            ownership::{
                ExpectedImportTarget, ImportPublicationGuard, admit_content_mutation,
                admit_prefix_mutation, claim_extracted_target, install_prefix_claim,
                lock_bucket_for_ownership, submit, supersede_bucket,
            },
        },
        multipart,
        pinning::publication::{
            PinTargetSpec, PublicationObject, PublicationRequest, publish_import_object,
            publish_object, publish_standard_object,
        },
    },
};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    EntityTrait, PaginatorTrait, QueryFilter, Statement, TransactionError, TransactionTrait,
};
use sea_orm_migration::{MigrationTrait, MigratorTrait};
use tokio::{sync::oneshot, task::JoinHandle};

use ipfs_s3_gateway::store::migrations::{
    m20250701_000001_init, m20260707_000001_decompress_zip, m20260720_000001_sse_c_key_fingerprint,
    m20260721_000001_multi_provider_pinning, m20260729_000001_ipfs3_import,
    m20260729_000002_postgres_utc_timestamps, m20260730_000001_standard_mutation_fence,
    m20260813_000001_postgres_json_columns,
};

static POSTGRES_MIGRATIONS: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
static POSTGRES_TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct PreJsonCompatibilityMigrator;

impl MigratorTrait for PreJsonCompatibilityMigrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20250701_000001_init::Migration),
            Box::new(m20260707_000001_decompress_zip::Migration),
            Box::new(m20260720_000001_sse_c_key_fingerprint::Migration),
            Box::new(m20260721_000001_multi_provider_pinning::Migration),
            Box::new(m20260729_000001_ipfs3_import::Migration),
            Box::new(m20260729_000002_postgres_utc_timestamps::Migration),
            Box::new(m20260730_000001_standard_mutation_fence::Migration),
        ]
    }
}

struct CurrentJsonCompatibilityMigrator;

impl MigratorTrait for CurrentJsonCompatibilityMigrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20250701_000001_init::Migration),
            Box::new(m20260707_000001_decompress_zip::Migration),
            Box::new(m20260720_000001_sse_c_key_fingerprint::Migration),
            Box::new(m20260721_000001_multi_provider_pinning::Migration),
            Box::new(m20260729_000001_ipfs3_import::Migration),
            Box::new(m20260729_000002_postgres_utc_timestamps::Migration),
            Box::new(m20260730_000001_standard_mutation_fence::Migration),
            Box::new(m20260813_000001_postgres_json_columns::Migration),
        ]
    }
}

fn request(id: &str, bucket: &str, key: &str, prefix: Option<&str>) -> NewImportJob {
    NewImportJob {
        id: id.to_owned(),
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        source: ImportSource::Cid(
            "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".to_owned(),
        ),
        request_fingerprint: format!("sha256:{id}"),
        client_token: None,
        object_content_type: Some("application/octet-stream".to_owned()),
        metadata: HashMap::new(),
        tags: vec![ObjectTag::new("fixture", "true")],
        decompress_prefix: prefix.map(str::to_owned),
    }
}

async fn connect_single(url: &str) -> DatabaseConnection {
    let mut options = ConnectOptions::new(url.to_owned());
    options.max_connections(1).min_connections(1);
    Database::connect(options).await.unwrap()
}

async fn connect_triplet(
    url: &str,
    bucket_name: &str,
) -> (DatabaseConnection, DatabaseConnection, DatabaseConnection) {
    let holder = connect_single(url).await;
    POSTGRES_MIGRATIONS
        .get_or_init(|| async {
            store::run_migrations(&holder).await.unwrap();
        })
        .await;
    assert_postgres_utc_timestamp_columns(&holder).await;
    crate_bucket(&holder, bucket_name).await;
    let contender = connect_single(url).await;
    let observer = connect_single(url).await;
    (holder, contender, observer)
}

async fn assert_postgres_utc_timestamp_columns(db: &DatabaseConnection) {
    let rows = db
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT table_name, column_name, data_type \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() \
               AND (table_name, column_name) IN ( \
                    ('buckets', 'created_at'), \
                    ('objects', 'created_at'), \
                    ('multipart_uploads', 'created_at'), \
                    ('multipart_parts', 'uploaded_at') \
               ) \
             ORDER BY table_name, column_name",
        ))
        .await
        .unwrap();
    let columns = rows
        .iter()
        .map(|row| {
            (
                row.try_get::<String>("", "table_name").unwrap(),
                row.try_get::<String>("", "column_name").unwrap(),
                row.try_get::<String>("", "data_type").unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        columns,
        [
            (
                "buckets".to_owned(),
                "created_at".to_owned(),
                "timestamp with time zone".to_owned(),
            ),
            (
                "multipart_parts".to_owned(),
                "uploaded_at".to_owned(),
                "timestamp with time zone".to_owned(),
            ),
            (
                "multipart_uploads".to_owned(),
                "created_at".to_owned(),
                "timestamp with time zone".to_owned(),
            ),
            (
                "objects".to_owned(),
                "created_at".to_owned(),
                "timestamp with time zone".to_owned(),
            ),
        ]
    );
}

async fn crate_bucket(db: &DatabaseConnection, bucket_name: &str) {
    ipfs_s3_gateway::store::bucket::create(db, bucket_name, None)
        .await
        .unwrap();
}

async fn job(db: &DatabaseConnection, id: &str) -> import_job::Model {
    import_job::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .unwrap()
        .unwrap()
}

async fn destination(
    db: &DatabaseConnection,
    bucket_name: &str,
    key: &str,
) -> import_destination::Model {
    import_destination::Entity::find_by_id((bucket_name.to_owned(), key.to_owned()))
        .one(db)
        .await
        .unwrap()
        .unwrap()
}

async fn assert_no_destination(db: &DatabaseConnection, bucket_name: &str, key: &str) {
    assert!(
        import_destination::Entity::find_by_id((bucket_name.to_owned(), key.to_owned()))
            .one(db)
            .await
            .unwrap()
            .is_none()
    );
}

async fn assert_no_targets(db: &DatabaseConnection, job_id: &str) {
    assert!(
        import_job_target::Entity::find()
            .filter(import_job_target::Column::JobId.eq(job_id))
            .one(db)
            .await
            .unwrap()
            .is_none()
    );
}

async fn assert_target(
    db: &DatabaseConnection,
    job_id: &str,
    bucket_name: &str,
    key: &str,
    generation: i64,
) {
    let target = import_job_target::Entity::find_by_id((
        job_id.to_owned(),
        bucket_name.to_owned(),
        key.to_owned(),
    ))
    .one(db)
    .await
    .unwrap()
    .unwrap();
    assert_eq!(target.expected_generation, generation);
}

async fn prefix_count(db: &DatabaseConnection, bucket_name: &str) -> u64 {
    import_prefix_claim::Entity::find()
        .filter(import_prefix_claim::Column::Bucket.eq(bucket_name))
        .count(db)
        .await
        .unwrap()
}

async fn delete_bucket_seam(
    db: &DatabaseConnection,
    bucket_name: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let bucket_name = bucket_name.to_owned();
    db.transaction(|txn| {
        Box::pin(async move {
            lock_bucket_for_ownership(txn, &bucket_name).await?;
            supersede_bucket(txn, &bucket_name, now).await?;
            bucket::Entity::delete_by_id(bucket_name).exec(txn).await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| match error {
        TransactionError::Transaction(error) => error,
        TransactionError::Connection(error) => error.into(),
    })
}

async fn supersede_active_test_jobs(
    db: &DatabaseConnection,
    bucket_name: &str,
    now: DateTime<Utc>,
) {
    let bucket_name = bucket_name.to_owned();
    db.transaction(|txn| {
        Box::pin(async move {
            lock_bucket_for_ownership(txn, &bucket_name).await?;
            supersede_bucket(txn, &bucket_name, now).await?;
            Ok::<_, AppError>(())
        })
    })
    .await
    .unwrap();
}

async fn backend_pid(db: &DatabaseConnection) -> i32 {
    db.query_one(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT pg_backend_pid() AS pid",
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "pid")
    .unwrap()
}

async fn assert_blocked_by_holder(
    observer: &DatabaseConnection,
    holder_pid: i32,
    contender_pid: i32,
) {
    tokio::time::timeout(Duration::seconds(5).to_std().unwrap(), async {
        loop {
            let row = observer
                .query_one(Statement::from_string(
                    DatabaseBackend::Postgres,
                    format!(
                        "SELECT {holder_pid} = ANY(pg_blocking_pids({contender_pid})) AS blocked"
                    ),
                ))
                .await
                .unwrap()
                .unwrap();
            if row.try_get::<bool>("", "blocked").unwrap() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("contender did not visibly wait for the already-held bucket fence");
}

async fn release_after_observed_bucket_wait<T>(
    operation: &'static str,
    holder_transaction: sea_orm::DatabaseTransaction,
    observer: &DatabaseConnection,
    holder_pid: i32,
    contender_pid: i32,
    started: oneshot::Receiver<()>,
    task: JoinHandle<AppResult<T>>,
) -> T
where
    T: Send + 'static,
{
    started.await.unwrap();
    assert_blocked_by_holder(observer, holder_pid, contender_pid).await;
    holder_transaction.commit().await.unwrap();
    task.await
        .unwrap()
        .unwrap_or_else(|error| panic!("{operation} failed after bucket fence release: {error:?}"))
}

async fn lock_job_for_update(transaction: &sea_orm::DatabaseTransaction, job_id: &str) {
    transaction
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT id FROM import_jobs WHERE id = $1 FOR UPDATE",
            [job_id.into()],
        ))
        .await
        .unwrap()
        .expect("fixture import job must exist");
}

async fn wait_for_claim_and_assert_empty(
    operation: &str,
    task: JoinHandle<AppResult<Vec<ipfs_s3_gateway::store::import::jobs::ClaimedImportJob>>>,
) {
    let claimed = tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap_or_else(|_| panic!("{operation} claimant did not finish after holder commit"))
        .unwrap()
        .unwrap_or_else(|error| panic!("{operation} claimant failed: {error:?}"));
    assert!(
        claimed.is_empty(),
        "{operation} returned stale claimed work"
    );
}

fn publication_request(
    id: &str,
    bucket_name: &str,
    key: &str,
    cid: &str,
    now: DateTime<Utc>,
) -> PublicationRequest {
    let object = PublicationObject::from_put(
        id.to_owned(),
        bucket_name,
        key,
        cid.to_owned(),
        7,
        Some("application/octet-stream".to_owned()),
        None,
        false,
        None,
        None,
        now,
    );
    PublicationRequest {
        object_target: PinTargetSpec {
            cid: object.cid.clone(),
            logical_size: object.logical_size,
        },
        object,
        tags: Vec::new(),
        policy: PublicationPolicy {
            tags: Vec::new(),
            leases: Vec::new(),
        },
    }
}

async fn acquire_advisory_lock(db: &DatabaseConnection, key: i64) {
    db.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT pg_advisory_lock($1)",
        [key.into()],
    ))
    .await
    .unwrap()
    .expect("advisory lock query must return a row");
}

async fn release_advisory_lock(db: &DatabaseConnection, key: i64) {
    let row = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT pg_advisory_unlock($1) AS unlocked",
            [key.into()],
        ))
        .await
        .unwrap()
        .expect("advisory unlock query must return a row");
    assert!(row.try_get::<bool>("", "unlocked").unwrap());
}

async fn assert_publication_lock_chain(
    observer: &DatabaseConnection,
    gate_pid: i32,
    standard_pid: i32,
    import_pid: i32,
) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let row = observer
                .query_one(Statement::from_string(
                    DatabaseBackend::Postgres,
                    format!(
                        "SELECT \
                         {gate_pid} = ANY(pg_blocking_pids({standard_pid})) AS standard_at_insert_gate, \
                         {standard_pid} = ANY(pg_blocking_pids({import_pid})) AS import_waits_for_standard"
                    ),
                ))
                .await
                .unwrap()
                .unwrap();
            if row
                .try_get::<bool>("", "standard_at_insert_gate")
                .unwrap()
                && row
                    .try_get::<bool>("", "import_waits_for_standard")
                    .unwrap()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("standard publication never passed the bucket FK while import waited on its object");
}

async fn backend_transaction_identity(
    observer: &DatabaseConnection,
    pid: i32,
) -> (DateTime<Utc>, Option<String>) {
    let row = observer
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            format!(
                "SELECT xact_start, backend_xid::text AS backend_xid \
                 FROM pg_stat_activity WHERE pid = {pid}"
            ),
        ))
        .await
        .unwrap()
        .expect("publication backend must be visible");
    (
        row.try_get("", "xact_start").unwrap(),
        row.try_get("", "backend_xid").unwrap(),
    )
}

#[tokio::test]
async fn postgres_standard_publication_insert_is_compatible_with_guarded_import_bucket_fence() {
    let Ok(url) = std::env::var("IPFS_S3_TEST_POSTGRES_URL") else {
        eprintln!("skipping PostgreSQL publication lock test: IPFS_S3_TEST_POSTGRES_URL is unset");
        return;
    };
    let _serial = POSTGRES_TEST_SERIAL.lock().await;
    let bucket_name = format!("pg-import-publication-lock-{}", uuid::Uuid::new_v4());
    let suffix = uuid::Uuid::new_v4();
    let old_id = format!("old-publication-{suffix}");
    let standard_id = format!("standard-publication-{suffix}");
    let import_job_id = format!("import-job-{suffix}");
    let import_object_id = format!("import-publication-{suffix}");
    let key = "same-key";
    let before_insert_gate = 8_104_201_i64;
    let after_insert_gate = 8_104_202_i64;
    let claim_now = Utc::now();
    let (fixture, standard_db, observer) = connect_triplet(&url, &bucket_name).await;
    let import_db = connect_single(&url).await;
    let gate_db = connect_single(&url).await;

    store::object::upsert(
        &fixture,
        &old_id,
        &bucket_name,
        key,
        "bafy-old",
        7,
        Some("application/octet-stream"),
        "bafy-old",
        None,
        false,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    submit(
        &fixture,
        request(&import_job_id, &bucket_name, key, None),
        claim_now,
    )
    .await
    .unwrap();
    let claim = claim_due(
        &fixture,
        "publication-import-worker",
        claim_now + Duration::seconds(1),
        claim_now + Duration::seconds(31),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap()
    .claim;
    assert_eq!(claim.job_id, import_job_id);
    let guard = ImportPublicationGuard {
        job_id: claim.job_id.clone(),
        worker_id: claim.worker_id.clone(),
        claim_epoch: claim.claim_epoch,
        targets: vec![ExpectedImportTarget {
            bucket: bucket_name.clone(),
            key: key.to_owned(),
            generation: 1,
        }],
    };

    fixture
        .execute_unprepared(&format!(
            "CREATE OR REPLACE FUNCTION ipfs3_pause_standard_before_insert() \
             RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN \
               IF OLD.id = '{old_id}' AND NEW.is_latest = FALSE THEN \
                 PERFORM pg_advisory_xact_lock({before_insert_gate}); \
               END IF; \
               RETURN NEW; \
             END $$"
        ))
        .await
        .unwrap();
    fixture
        .execute_unprepared("DROP TRIGGER IF EXISTS ipfs3_pause_standard_before_insert ON objects")
        .await
        .unwrap();
    fixture
        .execute_unprepared(
            "CREATE TRIGGER ipfs3_pause_standard_before_insert \
             BEFORE UPDATE OF is_latest ON objects FOR EACH ROW \
             EXECUTE FUNCTION ipfs3_pause_standard_before_insert()",
        )
        .await
        .unwrap();
    fixture
        .execute_unprepared(&format!(
            "CREATE OR REPLACE FUNCTION ipfs3_pause_standard_after_insert() \
             RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN \
               IF NEW.id = '{standard_id}' THEN \
                 PERFORM pg_advisory_xact_lock({after_insert_gate}); \
               END IF; \
               RETURN NEW; \
             END $$"
        ))
        .await
        .unwrap();
    fixture
        .execute_unprepared("DROP TRIGGER IF EXISTS ipfs3_pause_standard_after_insert ON objects")
        .await
        .unwrap();
    fixture
        .execute_unprepared(
            "CREATE TRIGGER ipfs3_pause_standard_after_insert \
             AFTER INSERT ON objects FOR EACH ROW \
             EXECUTE FUNCTION ipfs3_pause_standard_after_insert()",
        )
        .await
        .unwrap();

    acquire_advisory_lock(&gate_db, before_insert_gate).await;
    acquire_advisory_lock(&gate_db, after_insert_gate).await;
    let gate_pid = backend_pid(&gate_db).await;
    let standard_pid = backend_pid(&standard_db).await;
    let import_pid = backend_pid(&import_db).await;
    let standard_bucket = bucket_name.clone();
    let standard_id_for_task = standard_id.clone();
    let standard = tokio::spawn(async move {
        publish_object(
            &standard_db,
            publication_request(
                &standard_id_for_task,
                &standard_bucket,
                key,
                "bafy-standard",
                Utc::now(),
            ),
            &ProviderLimitMap::new(),
        )
        .await
    });
    assert_blocked_by_holder(&observer, gate_pid, standard_pid).await;

    let import_bucket = bucket_name.clone();
    let import_object_id_for_task = import_object_id.clone();
    let import_publication = tokio::spawn(async move {
        publish_import_object(
            &import_db,
            publication_request(
                &import_object_id_for_task,
                &import_bucket,
                key,
                "bafy-import",
                claim_now + Duration::seconds(2),
            ),
            guard,
            Vec::new(),
            claim_now + Duration::seconds(2),
            &ProviderLimitMap::new(),
        )
        .await
    });
    assert_blocked_by_holder(&observer, standard_pid, import_pid).await;
    let standard_transaction = backend_transaction_identity(&observer, standard_pid).await;
    let import_transaction = backend_transaction_identity(&observer, import_pid).await;

    release_advisory_lock(&gate_db, before_insert_gate).await;
    assert_publication_lock_chain(&observer, gate_pid, standard_pid, import_pid).await;
    assert_eq!(
        backend_transaction_identity(&observer, standard_pid).await,
        standard_transaction,
        "standard publication retried instead of passing the bucket FK in its original transaction"
    );
    assert_eq!(
        backend_transaction_identity(&observer, import_pid).await,
        import_transaction,
        "guarded import retried after a bucket/object deadlock"
    );
    release_advisory_lock(&gate_db, after_insert_gate).await;

    let standard_result = tokio::time::timeout(std::time::Duration::from_secs(5), standard)
        .await
        .expect("standard publication did not finish")
        .unwrap()
        .unwrap();
    assert_eq!(standard_result.object_id, standard_id);
    let import_result = tokio::time::timeout(std::time::Duration::from_secs(5), import_publication)
        .await
        .expect("guarded import publication did not finish")
        .unwrap()
        .unwrap();
    assert_eq!(import_result.object_id, import_object_id);

    let rows = object::Entity::find()
        .filter(object::Column::Bucket.eq(&bucket_name))
        .filter(object::Column::Key.eq(key))
        .all(&fixture)
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    let latest = rows.iter().filter(|row| row.is_latest).collect::<Vec<_>>();
    assert_eq!(latest.len(), 1);
    assert_eq!(
        (latest[0].id.as_str(), latest[0].cid.as_str()),
        (import_object_id.as_str(), "bafy-import")
    );
    assert_eq!(job(&fixture, &import_job_id).await.state, "completed");
    let owned = destination(&fixture, &bucket_name, key).await;
    assert_eq!((owned.generation, owned.owner_job_id), (1, None));
}

#[tokio::test]
async fn postgres_newer_import_completion_fences_older_standard_publication() {
    let Ok(url) = std::env::var("IPFS_S3_TEST_POSTGRES_URL") else {
        eprintln!(
            "skipping PostgreSQL standard mutation fence test: IPFS_S3_TEST_POSTGRES_URL is unset"
        );
        return;
    };
    let _serial = POSTGRES_TEST_SERIAL.lock().await;
    let suffix = uuid::Uuid::new_v4();
    let bucket_name = format!("pg-standard-mutation-fence-{suffix}");
    let key = "same-key";
    let standard_object_id = format!("stale-standard-{suffix}");
    let import_job_id = format!("winning-import-job-{suffix}");
    let import_object_id = format!("winning-import-object-{suffix}");
    let admitted_at = Utc::now();
    let (fixture, standard_db, _) = connect_triplet(&url, &bucket_name).await;

    let stale_guard = admit_content_mutation(
        &fixture,
        &bucket_name,
        key,
        None,
        ipfs_s3_gateway::import::SupersedeReason::PutObject,
        admitted_at,
    )
    .await
    .unwrap();
    assert_eq!(stale_guard.expected_generation, 1);

    submit(
        &fixture,
        request(&import_job_id, &bucket_name, key, None),
        admitted_at + Duration::seconds(1),
    )
    .await
    .unwrap();
    let claim = claim_due(
        &fixture,
        "winning-import-worker",
        admitted_at + Duration::seconds(2),
        admitted_at + Duration::seconds(32),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap()
    .claim;
    let import_guard = ImportPublicationGuard {
        job_id: claim.job_id,
        worker_id: claim.worker_id,
        claim_epoch: claim.claim_epoch,
        targets: vec![ExpectedImportTarget {
            bucket: bucket_name.clone(),
            key: key.to_owned(),
            generation: 2,
        }],
    };
    publish_import_object(
        &fixture,
        publication_request(
            &import_object_id,
            &bucket_name,
            key,
            "bafy-winning-import",
            admitted_at + Duration::seconds(3),
        ),
        import_guard,
        Vec::new(),
        admitted_at + Duration::seconds(3),
        &ProviderLimitMap::new(),
    )
    .await
    .unwrap();

    let error = publish_standard_object(
        &standard_db,
        publication_request(
            &standard_object_id,
            &bucket_name,
            key,
            "bafy-stale-standard",
            admitted_at + Duration::seconds(4),
        ),
        stale_guard,
        &ProviderLimitMap::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, AppError::StaleContentMutation));

    assert!(
        object::Entity::find_by_id(&standard_object_id)
            .one(&fixture)
            .await
            .unwrap()
            .is_none()
    );
    let latest = store::object::get_latest(&fixture, &bucket_name, key)
        .await
        .unwrap();
    assert_eq!(
        (latest.id.as_str(), latest.cid.as_str()),
        (import_object_id.as_str(), "bafy-winning-import")
    );
    let destination = destination(&fixture, &bucket_name, key).await;
    assert_eq!(destination.generation, 2);
    assert!(destination.owner_job_id.is_none());
    assert!(destination.mutation_id.is_none());
    assert!(destination.mutation_prefix.is_none());
}

#[tokio::test]
async fn postgres_claim_due_contends_with_prefix_and_bucket_supersession_without_deadlock() {
    let Ok(url) = std::env::var("IPFS_S3_TEST_POSTGRES_URL") else {
        eprintln!("skipping PostgreSQL lock-order test: IPFS_S3_TEST_POSTGRES_URL is unset");
        return;
    };
    let _serial = POSTGRES_TEST_SERIAL.lock().await;
    let base = Utc::now() - Duration::days(3650);

    let prefix_bucket = format!("pg-import-lock-prefix-{}", uuid::Uuid::new_v4());
    let prefix_suffix = uuid::Uuid::new_v4();
    let prefix_a = format!("a-prefix-{prefix_suffix}");
    let prefix_z = format!("z-prefix-{prefix_suffix}");
    let prefix_trigger = format!("trigger-prefix-{prefix_suffix}");
    let (holder, contender, observer) = connect_triplet(&url, &prefix_bucket).await;
    submit(
        &holder,
        request(&prefix_z, &prefix_bucket, "out/z", None),
        base,
    )
    .await
    .unwrap();
    submit(
        &holder,
        request(&prefix_a, &prefix_bucket, "out/a", None),
        base + Duration::seconds(1),
    )
    .await
    .unwrap();
    submit(
        &holder,
        request(&prefix_trigger, &prefix_bucket, "archive", None),
        Utc::now() + Duration::hours(1),
    )
    .await
    .unwrap();
    let holder_pid = backend_pid(&holder).await;
    let contender_pid = backend_pid(&contender).await;
    let holder_transaction = holder.begin().await.unwrap();
    lock_bucket_for_ownership(&holder_transaction, &prefix_bucket)
        .await
        .unwrap();
    lock_job_for_update(&holder_transaction, &prefix_a).await;
    let (started_tx, started_rx) = oneshot::channel();
    let claim_now = base + Duration::seconds(2);
    let claim_task = tokio::spawn(async move {
        started_tx.send(()).unwrap();
        claim_due(
            &contender,
            "prefix-claimant",
            claim_now,
            claim_now + Duration::seconds(30),
            2,
        )
        .await
    });
    started_rx.await.unwrap();
    assert_blocked_by_holder(&observer, holder_pid, contender_pid).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        install_prefix_claim(
            &holder_transaction,
            &prefix_trigger,
            &prefix_bucket,
            "out/",
            claim_now,
        ),
    )
    .await
    .expect("prefix supersession deadlocked with claim_due")
    .unwrap();
    holder_transaction.commit().await.unwrap();
    wait_for_claim_and_assert_empty("prefix supersession", claim_task).await;
    assert_eq!(job(&holder, &prefix_a).await.state, "superseded");
    assert_eq!(job(&holder, &prefix_z).await.state, "superseded");
    assert_eq!(job(&holder, &prefix_trigger).await.state, "queued");
    for key in ["out/a", "out/z"] {
        let row = destination(&holder, &prefix_bucket, key).await;
        assert_eq!((row.generation, row.owner_job_id), (2, None));
    }
    assert_eq!(prefix_count(&holder, &prefix_bucket).await, 1);

    let bucket_name = format!("pg-import-lock-bucket-{}", uuid::Uuid::new_v4());
    let bucket_suffix = uuid::Uuid::new_v4();
    let bucket_a = format!("a-bucket-{bucket_suffix}");
    let bucket_z = format!("z-bucket-{bucket_suffix}");
    let (holder, contender, observer) = connect_triplet(&url, &bucket_name).await;
    submit(
        &holder,
        request(&bucket_z, &bucket_name, "key-a", None),
        base + Duration::seconds(10),
    )
    .await
    .unwrap();
    submit(
        &holder,
        request(&bucket_a, &bucket_name, "key-z", None),
        base + Duration::seconds(11),
    )
    .await
    .unwrap();
    let holder_pid = backend_pid(&holder).await;
    let contender_pid = backend_pid(&contender).await;
    let holder_transaction = holder.begin().await.unwrap();
    lock_bucket_for_ownership(&holder_transaction, &bucket_name)
        .await
        .unwrap();
    lock_job_for_update(&holder_transaction, &bucket_a).await;
    let (started_tx, started_rx) = oneshot::channel();
    let claim_now = base + Duration::seconds(12);
    let claim_task = tokio::spawn(async move {
        started_tx.send(()).unwrap();
        claim_due(
            &contender,
            "bucket-claimant",
            claim_now,
            claim_now + Duration::seconds(30),
            2,
        )
        .await
    });
    started_rx.await.unwrap();
    assert_blocked_by_holder(&observer, holder_pid, contender_pid).await;
    let superseded = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        supersede_bucket(&holder_transaction, &bucket_name, claim_now),
    )
    .await
    .expect("bucket supersession deadlocked with claim_due")
    .unwrap();
    assert_eq!(superseded, 2);
    holder_transaction.commit().await.unwrap();
    wait_for_claim_and_assert_empty("bucket supersession", claim_task).await;
    assert_eq!(job(&holder, &bucket_a).await.state, "superseded");
    assert_eq!(job(&holder, &bucket_z).await.state, "superseded");
    for key in ["key-a", "key-z"] {
        let row = destination(&holder, &bucket_name, key).await;
        assert_eq!((row.generation, row.owner_job_id), (1, None));
    }
    assert_eq!(prefix_count(&holder, &bucket_name).await, 0);

    supersede_active_test_jobs(&holder, &prefix_bucket, Utc::now()).await;
}

#[tokio::test]
async fn postgres_bucket_first_ownership_serializes_all_task_four_races() {
    let Ok(url) = std::env::var("IPFS_S3_TEST_POSTGRES_URL") else {
        eprintln!("skipping PostgreSQL import ownership test: IPFS_S3_TEST_POSTGRES_URL is unset");
        return;
    };
    let _serial = POSTGRES_TEST_SERIAL.lock().await;
    let now = Utc::now();

    let bucket_exact_submit = format!("pg-import-exact-submit-{}", uuid::Uuid::new_v4());
    let (exact_holder, exact_contender, exact_observer) =
        connect_triplet(&url, &bucket_exact_submit).await;
    submit(
        &exact_holder,
        request("exact-old", &bucket_exact_submit, "key", None),
        now,
    )
    .await
    .unwrap();
    let holder_pid = backend_pid(&exact_holder).await;
    let contender_pid = backend_pid(&exact_contender).await;
    let holder_transaction = exact_holder.begin().await.unwrap();
    lock_bucket_for_ownership(&holder_transaction, &bucket_exact_submit)
        .await
        .unwrap();
    let (started, started_at_lock) = oneshot::channel();
    let contender_bucket = bucket_exact_submit.clone();
    let submit_task = tokio::spawn(async move {
        started.send(()).unwrap();
        submit(
            &exact_contender,
            request("exact-new", &contender_bucket, "key", None),
            now,
        )
        .await
    });
    release_after_observed_bucket_wait(
        "submit-first exact",
        holder_transaction,
        &exact_observer,
        holder_pid,
        contender_pid,
        started_at_lock,
        submit_task,
    )
    .await;
    admit_content_mutation(
        &exact_holder,
        &bucket_exact_submit,
        "key",
        None,
        ipfs_s3_gateway::import::SupersedeReason::PutObject,
        now,
    )
    .await
    .unwrap();
    let exact = destination(&exact_holder, &bucket_exact_submit, "key").await;
    assert_eq!(job(&exact_holder, "exact-old").await.state, "superseded");
    assert_eq!(job(&exact_holder, "exact-new").await.state, "superseded");
    assert_eq!((exact.generation, exact.owner_job_id), (3, None));
    assert_no_targets(&exact_holder, "exact-old").await;
    assert_no_targets(&exact_holder, "exact-new").await;
    assert_eq!(prefix_count(&exact_holder, &bucket_exact_submit).await, 0);

    let bucket_exact_admit = format!("pg-import-exact-admit-{}", uuid::Uuid::new_v4());
    let (exact_holder, exact_contender, exact_observer) =
        connect_triplet(&url, &bucket_exact_admit).await;
    submit(
        &exact_holder,
        request("exact-admit-old", &bucket_exact_admit, "key", None),
        now,
    )
    .await
    .unwrap();
    let holder_pid = backend_pid(&exact_holder).await;
    let contender_pid = backend_pid(&exact_contender).await;
    let holder_transaction = exact_holder.begin().await.unwrap();
    lock_bucket_for_ownership(&holder_transaction, &bucket_exact_admit)
        .await
        .unwrap();
    let (started, started_at_lock) = oneshot::channel();
    let contender_bucket = bucket_exact_admit.clone();
    let admit_task = tokio::spawn(async move {
        started.send(()).unwrap();
        admit_content_mutation(
            &exact_contender,
            &contender_bucket,
            "key",
            None,
            ipfs_s3_gateway::import::SupersedeReason::PutObject,
            now,
        )
        .await
    });
    release_after_observed_bucket_wait(
        "admission-first exact",
        holder_transaction,
        &exact_observer,
        holder_pid,
        contender_pid,
        started_at_lock,
        admit_task,
    )
    .await;
    submit(
        &exact_holder,
        request("exact-admit-new", &bucket_exact_admit, "key", None),
        now,
    )
    .await
    .unwrap();
    let exact = destination(&exact_holder, &bucket_exact_admit, "key").await;
    assert_eq!(
        job(&exact_holder, "exact-admit-old").await.state,
        "superseded"
    );
    assert_eq!(job(&exact_holder, "exact-admit-new").await.state, "queued");
    assert_eq!(
        (exact.generation, exact.owner_job_id.as_deref()),
        (3, Some("exact-admit-new"))
    );
    assert_no_targets(&exact_holder, "exact-admit-old").await;
    assert_target(
        &exact_holder,
        "exact-admit-new",
        &bucket_exact_admit,
        "key",
        3,
    )
    .await;
    assert_eq!(prefix_count(&exact_holder, &bucket_exact_admit).await, 0);

    let bucket_prefix_submit = format!("pg-import-prefix-submit-{}", uuid::Uuid::new_v4());
    let (prefix_holder, prefix_contender, prefix_observer) =
        connect_triplet(&url, &bucket_prefix_submit).await;
    submit(
        &prefix_holder,
        request(
            "prefix-old",
            &bucket_prefix_submit,
            "archive-old",
            Some("out/a/"),
        ),
        now,
    )
    .await
    .unwrap();
    let holder_pid = backend_pid(&prefix_holder).await;
    let contender_pid = backend_pid(&prefix_contender).await;
    let holder_transaction = prefix_holder.begin().await.unwrap();
    lock_bucket_for_ownership(&holder_transaction, &bucket_prefix_submit)
        .await
        .unwrap();
    let (started, started_at_lock) = oneshot::channel();
    let contender_bucket = bucket_prefix_submit.clone();
    let prefix_submit_task = tokio::spawn(async move {
        started.send(()).unwrap();
        submit(
            &prefix_contender,
            request("prefix-new", &contender_bucket, "archive-new", Some("out/")),
            now,
        )
        .await
    });
    release_after_observed_bucket_wait(
        "submit-first overlapping prefix",
        holder_transaction,
        &prefix_observer,
        holder_pid,
        contender_pid,
        started_at_lock,
        prefix_submit_task,
    )
    .await;
    admit_prefix_mutation(&prefix_holder, &bucket_prefix_submit, "out/a/", now)
        .await
        .unwrap();
    assert_eq!(job(&prefix_holder, "prefix-old").await.state, "superseded");
    assert_eq!(job(&prefix_holder, "prefix-new").await.state, "superseded");
    assert_eq!(
        (
            destination(&prefix_holder, &bucket_prefix_submit, "archive-new")
                .await
                .generation,
            destination(&prefix_holder, &bucket_prefix_submit, "archive-new")
                .await
                .owner_job_id,
        ),
        (1, None)
    );
    assert_eq!(prefix_count(&prefix_holder, &bucket_prefix_submit).await, 0);
    assert_no_targets(&prefix_holder, "prefix-old").await;
    assert_no_targets(&prefix_holder, "prefix-new").await;

    let bucket_prefix_admit = format!("pg-import-prefix-admit-{}", uuid::Uuid::new_v4());
    let (prefix_holder, prefix_contender, prefix_observer) =
        connect_triplet(&url, &bucket_prefix_admit).await;
    submit(
        &prefix_holder,
        request(
            "prefix-admit-old",
            &bucket_prefix_admit,
            "archive-old",
            Some("out/a/"),
        ),
        now,
    )
    .await
    .unwrap();
    let holder_pid = backend_pid(&prefix_holder).await;
    let contender_pid = backend_pid(&prefix_contender).await;
    let holder_transaction = prefix_holder.begin().await.unwrap();
    lock_bucket_for_ownership(&holder_transaction, &bucket_prefix_admit)
        .await
        .unwrap();
    let (started, started_at_lock) = oneshot::channel();
    let contender_bucket = bucket_prefix_admit.clone();
    let prefix_admit_task = tokio::spawn(async move {
        started.send(()).unwrap();
        admit_prefix_mutation(&prefix_contender, &contender_bucket, "out/a/", now).await
    });
    release_after_observed_bucket_wait(
        "admission-first overlapping prefix",
        holder_transaction,
        &prefix_observer,
        holder_pid,
        contender_pid,
        started_at_lock,
        prefix_admit_task,
    )
    .await;
    submit(
        &prefix_holder,
        request(
            "prefix-admit-new",
            &bucket_prefix_admit,
            "archive-new",
            Some("out/"),
        ),
        now,
    )
    .await
    .unwrap();
    let archive_old = destination(&prefix_holder, &bucket_prefix_admit, "archive-old").await;
    let archive_new = destination(&prefix_holder, &bucket_prefix_admit, "archive-new").await;
    assert_eq!(
        job(&prefix_holder, "prefix-admit-old").await.state,
        "superseded"
    );
    assert_eq!(
        job(&prefix_holder, "prefix-admit-new").await.state,
        "queued"
    );
    assert_eq!(
        (archive_old.generation, archive_old.owner_job_id),
        (1, None)
    );
    assert_eq!(
        (archive_new.generation, archive_new.owner_job_id.as_deref()),
        (1, Some("prefix-admit-new"))
    );
    assert_eq!(prefix_count(&prefix_holder, &bucket_prefix_admit).await, 1);
    assert_no_targets(&prefix_holder, "prefix-admit-old").await;
    assert_target(
        &prefix_holder,
        "prefix-admit-new",
        &bucket_prefix_admit,
        "archive-new",
        1,
    )
    .await;

    let bucket_empty_admit = format!("pg-import-empty-admit-{}", uuid::Uuid::new_v4());
    let (empty_holder, empty_contender, empty_observer) =
        connect_triplet(&url, &bucket_empty_admit).await;
    submit(
        &empty_holder,
        request("empty-old", &bucket_empty_admit, "archive", Some("")),
        now,
    )
    .await
    .unwrap();
    let holder_pid = backend_pid(&empty_holder).await;
    let contender_pid = backend_pid(&empty_contender).await;
    let holder_transaction = empty_holder.begin().await.unwrap();
    lock_bucket_for_ownership(&holder_transaction, &bucket_empty_admit)
        .await
        .unwrap();
    let (started, started_at_lock) = oneshot::channel();
    let contender_bucket = bucket_empty_admit.clone();
    let empty_admit_task = tokio::spawn(async move {
        started.send(()).unwrap();
        admit_prefix_mutation(&empty_contender, &contender_bucket, "", now).await
    });
    release_after_observed_bucket_wait(
        "admission-first empty prefix",
        holder_transaction,
        &empty_observer,
        holder_pid,
        contender_pid,
        started_at_lock,
        empty_admit_task,
    )
    .await;
    submit(
        &empty_holder,
        request("empty-new", &bucket_empty_admit, "whole/key", None),
        now,
    )
    .await
    .unwrap();
    let empty = destination(&empty_holder, &bucket_empty_admit, "whole/key").await;
    assert_eq!(job(&empty_holder, "empty-old").await.state, "superseded");
    assert_eq!(job(&empty_holder, "empty-new").await.state, "queued");
    assert_eq!(
        (empty.generation, empty.owner_job_id.as_deref()),
        (1, Some("empty-new"))
    );
    assert_eq!(prefix_count(&empty_holder, &bucket_empty_admit).await, 0);
    assert_no_targets(&empty_holder, "empty-old").await;
    assert_target(
        &empty_holder,
        "empty-new",
        &bucket_empty_admit,
        "whole/key",
        1,
    )
    .await;

    let bucket_empty_submit = format!("pg-import-empty-submit-{}", uuid::Uuid::new_v4());
    let (empty_holder, empty_contender, empty_observer) =
        connect_triplet(&url, &bucket_empty_submit).await;
    submit(
        &empty_holder,
        request(
            "empty-submit-old",
            &bucket_empty_submit,
            "archive",
            Some(""),
        ),
        now,
    )
    .await
    .unwrap();
    let holder_pid = backend_pid(&empty_holder).await;
    let contender_pid = backend_pid(&empty_contender).await;
    let holder_transaction = empty_holder.begin().await.unwrap();
    lock_bucket_for_ownership(&holder_transaction, &bucket_empty_submit)
        .await
        .unwrap();
    let (started, started_at_lock) = oneshot::channel();
    let contender_bucket = bucket_empty_submit.clone();
    let empty_submit_task = tokio::spawn(async move {
        started.send(()).unwrap();
        submit(
            &empty_contender,
            request("empty-submit-new", &contender_bucket, "whole/key", None),
            now,
        )
        .await
    });
    release_after_observed_bucket_wait(
        "submit-first empty prefix",
        holder_transaction,
        &empty_observer,
        holder_pid,
        contender_pid,
        started_at_lock,
        empty_submit_task,
    )
    .await;
    admit_prefix_mutation(&empty_holder, &bucket_empty_submit, "", now)
        .await
        .unwrap();
    let empty = destination(&empty_holder, &bucket_empty_submit, "whole/key").await;
    assert_eq!(
        job(&empty_holder, "empty-submit-old").await.state,
        "superseded"
    );
    assert_eq!(
        job(&empty_holder, "empty-submit-new").await.state,
        "superseded"
    );
    assert_eq!((empty.generation, empty.owner_job_id), (2, None));
    assert_eq!(prefix_count(&empty_holder, &bucket_empty_submit).await, 0);
    assert_no_targets(&empty_holder, "empty-submit-old").await;
    assert_no_targets(&empty_holder, "empty-submit-new").await;

    let bucket_extracted_claim = format!("pg-import-extracted-claim-{}", uuid::Uuid::new_v4());
    let (extracted_holder, extracted_contender, extracted_observer) =
        connect_triplet(&url, &bucket_extracted_claim).await;
    submit(
        &extracted_holder,
        request(
            "extracted",
            &bucket_extracted_claim,
            "archive.zip",
            Some("out/"),
        ),
        now - Duration::seconds(60),
    )
    .await
    .unwrap();
    let claimed = claim_due(
        &extracted_holder,
        "worker",
        now,
        now + Duration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert_eq!(claimed.claim.job_id, "extracted");
    let claim = claimed.claim;
    let holder_pid = backend_pid(&extracted_holder).await;
    let contender_pid = backend_pid(&extracted_contender).await;
    let holder_transaction = extracted_holder.begin().await.unwrap();
    lock_bucket_for_ownership(&holder_transaction, &bucket_extracted_claim)
        .await
        .unwrap();
    let (started, started_at_lock) = oneshot::channel();
    let contender_bucket = bucket_extracted_claim.clone();
    let claim_task = tokio::spawn(async move {
        started.send(()).unwrap();
        claim_extracted_target(
            &extracted_contender,
            &claim,
            &contender_bucket,
            "out/file",
            now,
        )
        .await
    });
    assert_eq!(
        release_after_observed_bucket_wait(
            "claim-first extracted target",
            holder_transaction,
            &extracted_observer,
            holder_pid,
            contender_pid,
            started_at_lock,
            claim_task,
        )
        .await,
        1
    );
    admit_prefix_mutation(&extracted_holder, &bucket_extracted_claim, "out/", now)
        .await
        .unwrap();
    let extracted = destination(&extracted_holder, &bucket_extracted_claim, "out/file").await;
    assert_eq!(
        job(&extracted_holder, "extracted").await.state,
        "superseded"
    );
    assert_eq!((extracted.generation, extracted.owner_job_id), (2, None));
    assert_no_targets(&extracted_holder, "extracted").await;
    assert_eq!(
        prefix_count(&extracted_holder, &bucket_extracted_claim).await,
        0
    );

    let bucket_extracted_admit = format!("pg-import-extracted-admit-{}", uuid::Uuid::new_v4());
    let (extracted_holder, extracted_contender, extracted_observer) =
        connect_triplet(&url, &bucket_extracted_admit).await;
    submit(
        &extracted_holder,
        request(
            "extracted-admit",
            &bucket_extracted_admit,
            "archive.zip",
            Some("out/"),
        ),
        now - Duration::seconds(60),
    )
    .await
    .unwrap();
    let claimed = claim_due(
        &extracted_holder,
        "worker",
        now,
        now + Duration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert_eq!(claimed.claim.job_id, "extracted-admit");
    let claim = claimed.claim;
    let holder_pid = backend_pid(&extracted_holder).await;
    let contender_pid = backend_pid(&extracted_contender).await;
    let holder_transaction = extracted_holder.begin().await.unwrap();
    lock_bucket_for_ownership(&holder_transaction, &bucket_extracted_admit)
        .await
        .unwrap();
    let (started, started_at_lock) = oneshot::channel();
    let contender_bucket = bucket_extracted_admit.clone();
    let prefix_task = tokio::spawn(async move {
        started.send(()).unwrap();
        admit_prefix_mutation(&extracted_contender, &contender_bucket, "out/", now).await
    });
    release_after_observed_bucket_wait(
        "admission-first extracted target",
        holder_transaction,
        &extracted_observer,
        holder_pid,
        contender_pid,
        started_at_lock,
        prefix_task,
    )
    .await;
    assert!(matches!(
        claim_extracted_target(
            &extracted_holder,
            &claim,
            &bucket_extracted_admit,
            "out/file",
            now,
        )
        .await,
        Err(AppError::StaleImportOwnership)
    ));
    assert_eq!(
        job(&extracted_holder, "extracted-admit").await.state,
        "superseded"
    );
    assert_no_destination(&extracted_holder, &bucket_extracted_admit, "out/file").await;
    assert_no_targets(&extracted_holder, "extracted-admit").await;
    assert_eq!(
        prefix_count(&extracted_holder, &bucket_extracted_admit).await,
        0
    );

    let bucket_delete_submit = format!("pg-import-delete-submit-{}", uuid::Uuid::new_v4());
    let (delete_holder, delete_contender, delete_observer) =
        connect_triplet(&url, &bucket_delete_submit).await;
    submit(
        &delete_holder,
        request("delete-old", &bucket_delete_submit, "old", None),
        now,
    )
    .await
    .unwrap();
    let holder_pid = backend_pid(&delete_holder).await;
    let contender_pid = backend_pid(&delete_contender).await;
    let holder_transaction = delete_holder.begin().await.unwrap();
    lock_bucket_for_ownership(&holder_transaction, &bucket_delete_submit)
        .await
        .unwrap();
    let (started, started_at_lock) = oneshot::channel();
    let contender_bucket = bucket_delete_submit.clone();
    let submit_task = tokio::spawn(async move {
        started.send(()).unwrap();
        submit(
            &delete_contender,
            request("delete-new", &contender_bucket, "new", None),
            now,
        )
        .await
    });
    release_after_observed_bucket_wait(
        "submit-first bucket deletion",
        holder_transaction,
        &delete_observer,
        holder_pid,
        contender_pid,
        started_at_lock,
        submit_task,
    )
    .await;
    delete_bucket_seam(&delete_holder, &bucket_delete_submit, now)
        .await
        .unwrap();
    assert!(
        bucket::Entity::find_by_id(bucket_delete_submit.clone())
            .one(&delete_holder)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(job(&delete_holder, "delete-old").await.state, "superseded");
    assert_eq!(job(&delete_holder, "delete-new").await.state, "superseded");
    assert!(
        import_destination::Entity::find()
            .filter(import_destination::Column::Bucket.eq(&bucket_delete_submit))
            .one(&delete_holder)
            .await
            .unwrap()
            .is_none()
    );
    assert_no_targets(&delete_holder, "delete-old").await;
    assert_no_targets(&delete_holder, "delete-new").await;
    assert_eq!(prefix_count(&delete_holder, &bucket_delete_submit).await, 0);

    let bucket_delete_first = format!("pg-import-delete-first-{}", uuid::Uuid::new_v4());
    let (delete_holder, delete_contender, delete_observer) =
        connect_triplet(&url, &bucket_delete_first).await;
    submit(
        &delete_holder,
        request("delete-first-old", &bucket_delete_first, "old", None),
        now,
    )
    .await
    .unwrap();
    let holder_pid = backend_pid(&delete_holder).await;
    let contender_pid = backend_pid(&delete_contender).await;
    let holder_transaction = delete_holder.begin().await.unwrap();
    lock_bucket_for_ownership(&holder_transaction, &bucket_delete_first)
        .await
        .unwrap();
    let (started, started_at_lock) = oneshot::channel();
    let contender_bucket = bucket_delete_first.clone();
    let delete_task = tokio::spawn(async move {
        started.send(()).unwrap();
        delete_bucket_seam(&delete_contender, &contender_bucket, now).await
    });
    release_after_observed_bucket_wait(
        "delete-first bucket deletion",
        holder_transaction,
        &delete_observer,
        holder_pid,
        contender_pid,
        started_at_lock,
        delete_task,
    )
    .await;
    assert!(matches!(
        submit(
            &delete_holder,
            request("delete-first-new", &bucket_delete_first, "new", None),
            now,
        )
        .await,
        Err(AppError::NoSuchBucket(name)) if name == bucket_delete_first
    ));
    assert_eq!(
        job(&delete_holder, "delete-first-old").await.state,
        "superseded"
    );
    assert!(
        import_job::Entity::find_by_id("delete-first-new".to_owned())
            .one(&delete_holder)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        import_destination::Entity::find()
            .filter(import_destination::Column::Bucket.eq(&bucket_delete_first))
            .one(&delete_holder)
            .await
            .unwrap()
            .is_none()
    );
    assert_no_targets(&delete_holder, "delete-first-old").await;
    assert_eq!(prefix_count(&delete_holder, &bucket_delete_first).await, 0);

    for bucket_name in [
        &bucket_exact_admit,
        &bucket_prefix_admit,
        &bucket_empty_admit,
    ] {
        supersede_active_test_jobs(&delete_holder, bucket_name, Utc::now()).await;
    }
}

#[tokio::test]
async fn postgres_json_columns_require_compatibility_migration() {
    let Ok(url) = std::env::var("IPFS_S3_TEST_POSTGRES_URL") else {
        eprintln!(
            "skipping PostgreSQL JSON compatibility test: IPFS_S3_TEST_POSTGRES_URL is unset"
        );
        return;
    };
    let _serial = POSTGRES_TEST_SERIAL.lock().await;
    let schema = format!("json_{}", uuid::Uuid::new_v4().simple());
    let suffix = uuid::Uuid::new_v4();
    let bucket_name = format!("json-bucket-{suffix}");
    let upload_id = format!("json-upload-{suffix}");
    let upload_object_id = format!("json-upload-object-{suffix}");
    let invalid_object_id = format!("json-invalid-object-{suffix}");
    let invalid_key = format!("json-invalid-key-{suffix}");
    let fixture = connect_single(&url).await;

    fixture
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    fixture
        .execute_unprepared(&format!("SET search_path TO {schema}"))
        .await
        .unwrap();
    PreJsonCompatibilityMigrator::up(&fixture, None)
        .await
        .unwrap();
    crate_bucket(&fixture, &bucket_name).await;
    multipart::create_upload(
        &fixture,
        &upload_id,
        &upload_object_id,
        &bucket_name,
        "object",
        "none",
        None,
        None,
        None,
        None,
        &[],
        None,
        false,
    )
    .await
    .unwrap();

    let decode_error = multipart::get_upload(&fixture, &upload_id)
        .await
        .unwrap_err();
    let decode_diagnostic = format!("{decode_error:?}").to_lowercase();
    for expected in [
        "error occurred while decoding column",
        "mismatched types",
        "json",
        "text",
    ] {
        assert!(
            decode_diagnostic.contains(expected),
            "missing PostgreSQL JSON/TEXT decode diagnostic: {expected}"
        );
    }
    assert!(
        !decode_diagnostic.contains("[]"),
        "decode diagnostic must not leak the stored tags value"
    );

    fixture
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "INSERT INTO objects (id, bucket, key, cid, size, etag, metadata, encrypted, multipart, is_latest) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, FALSE, TRUE)",
            [
                invalid_object_id.clone().into(),
                bucket_name.clone().into(),
                invalid_key.clone().into(),
                "bafy-invalid-json".into(),
                1_i64.into(),
                "bafy-invalid-json".into(),
                "not-json".into(),
            ],
        ))
        .await
        .unwrap();

    let invalid_json_error = CurrentJsonCompatibilityMigrator::up(&fixture, None)
        .await
        .unwrap_err();
    let invalid_json_diagnostic = format!("{invalid_json_error:?}");
    assert!(
        invalid_json_diagnostic.contains("invalid JSON in objects.metadata"),
        "migration must fail with the sanitized invalid-JSON message"
    );
    assert!(
        !invalid_json_diagnostic.contains("not-json"),
        "migration diagnostic must not leak invalid stored content"
    );

    let marker_count: i64 = fixture
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT COUNT(*) AS count FROM seaql_migrations \
             WHERE version = 'm20260813_000001_postgres_json_columns'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "count")
        .unwrap();
    assert_eq!(
        marker_count, 0,
        "failed migration must not record its marker"
    );

    let text_types = fixture
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT table_name || '.' || column_name AS name, data_type \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() \
               AND (table_name, column_name) IN ( \
                    ('multipart_uploads', 'metadata'), \
                    ('multipart_uploads', 'tags_json'), \
                    ('objects', 'metadata') \
               ) \
             ORDER BY table_name, column_name",
        ))
        .await
        .unwrap()
        .iter()
        .map(|row| {
            (
                row.try_get::<String>("", "name").unwrap(),
                row.try_get::<String>("", "data_type").unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        text_types,
        [
            ("multipart_uploads.metadata".to_owned(), "text".to_owned()),
            ("multipart_uploads.tags_json".to_owned(), "text".to_owned()),
            ("objects.metadata".to_owned(), "text".to_owned()),
        ],
        "failed migration must roll back all column-type changes"
    );
    fixture
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "DELETE FROM objects WHERE id = $1",
            [invalid_object_id.into()],
        ))
        .await
        .unwrap();

    CurrentJsonCompatibilityMigrator::up(&fixture, None)
        .await
        .unwrap();
    let marker: String = fixture
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT version FROM seaql_migrations ORDER BY version DESC LIMIT 1",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "version")
        .unwrap();
    assert_eq!(marker, "m20260813_000001_postgres_json_columns");

    let json_columns = fixture
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT table_name || '.' || column_name AS name, data_type, is_nullable, column_default \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() \
               AND (table_name, column_name) IN ( \
                    ('multipart_uploads', 'metadata'), \
                    ('multipart_uploads', 'tags_json'), \
                    ('objects', 'metadata') \
               ) \
             ORDER BY table_name, column_name",
        ))
        .await
        .unwrap()
        .iter()
        .map(|row| {
            (
                row.try_get::<String>("", "name").unwrap(),
                row.try_get::<String>("", "data_type").unwrap(),
                row.try_get::<String>("", "is_nullable").unwrap(),
                row.try_get::<Option<String>>("", "column_default").unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        json_columns,
        [
            (
                "multipart_uploads.metadata".to_owned(),
                "jsonb".to_owned(),
                "YES".to_owned(),
                None,
            ),
            (
                "multipart_uploads.tags_json".to_owned(),
                "jsonb".to_owned(),
                "NO".to_owned(),
                Some("'[]'::jsonb".to_owned()),
            ),
            (
                "objects.metadata".to_owned(),
                "jsonb".to_owned(),
                "YES".to_owned(),
                None,
            ),
        ]
    );

    fixture.close().await.unwrap();
    let fixture = connect_single(&url).await;
    fixture
        .execute_unprepared(&format!("SET search_path TO {schema}"))
        .await
        .unwrap();
    let upload = multipart::get_upload(&fixture, &upload_id).await.unwrap();
    assert!(upload.metadata.is_none());
    assert_eq!(upload.tags_json, serde_json::json!([]));
    multipart::upsert_part(
        &fixture,
        &upload_id,
        1,
        "bafy-six-mib-part",
        6_291_456,
        "bafy-six-mib-part",
    )
    .await
    .unwrap();
    let part = multipart::get_part(&fixture, &upload_id, 1).await.unwrap();
    assert_eq!(part.size, 6_291_456);

    fixture.close().await.unwrap();
    let cleanup = connect_single(&url).await;
    cleanup
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    cleanup.close().await.unwrap();
}
