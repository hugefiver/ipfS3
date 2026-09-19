use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration as StdDuration,
};

use chrono::Utc;
use ipfs_s3_gateway::{
    state::AppState,
    store::{
        self, Store,
        entities::{object, object_version, residency_reference, version_residency},
    },
};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseConnection, EntityTrait,
    QueryFilter, Set, TransactionTrait, sea_query::Expr,
};
use tokio::sync::oneshot;
use wiremock::{Mock, ResponseTemplate, matchers};

use super::*;

const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(10);
const CONCURRENT_KEY: &str = "report/concurrent.bin";
const CONCURRENT_CID: &str = COLD_VERSION_CID;
const CONCURRENT_VERSION: &str = "00000000-0000-0000-0000-000000000401";

#[derive(Clone, Copy, Debug)]
enum ConcurrentListOperation {
    V1,
    V2,
    Versions,
}

impl ConcurrentListOperation {
    fn query(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::V1 => &[("prefix", CONCURRENT_KEY)],
            Self::V2 => &[("list-type", "2"), ("prefix", CONCURRENT_KEY)],
            Self::Versions => &[("versions", ""), ("prefix", CONCURRENT_KEY)],
        }
    }

    fn assert_class(self, xml: &str, storage_class: &str) {
        match self {
            Self::V1 | Self::V2 => {
                assert_listed_object(xml, CONCURRENT_KEY, storage_class);
            }
            Self::Versions => assert_listed_version(
                xml,
                CONCURRENT_KEY,
                CONCURRENT_VERSION,
                CONCURRENT_CID,
                storage_class,
            ),
        }
    }
}

struct QueryPause {
    released: Mutex<bool>,
    release_changed: Condvar,
    timed_out: AtomicBool,
}

impl QueryPause {
    fn new() -> Self {
        Self {
            released: Mutex::new(false),
            release_changed: Condvar::new(),
            timed_out: AtomicBool::new(false),
        }
    }

    fn release(&self) {
        let mut released = self
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *released = true;
        self.release_changed.notify_all();
    }

    fn wait_for_release(&self) {
        let released = self
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (released, result) = self
            .release_changed
            .wait_timeout_while(released, TEST_TIMEOUT, |released| !*released)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if result.timed_out() && !*released {
            self.timed_out.store(true, Ordering::SeqCst);
        }
    }
}

struct QueryPauseGuard(Arc<QueryPause>);

impl Drop for QueryPauseGuard {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct ImmutableMetadata {
    object: object::Model,
    version: object_version::Model,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_objects_v1_keeps_one_storage_class_snapshot_during_publication() {
    assert_concurrent_list_snapshot(ConcurrentListOperation::V1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_objects_v2_keeps_one_storage_class_snapshot_during_publication() {
    assert_concurrent_list_snapshot(ConcurrentListOperation::V2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_object_versions_keeps_one_storage_class_snapshot_during_publication() {
    assert_concurrent_list_snapshot(ConcurrentListOperation::Versions).await;
}

async fn assert_concurrent_list_snapshot(operation: ConcurrentListOperation) {
    let database_directory = tempfile::tempdir().expect("create reporting concurrency directory");
    let database_path = database_directory
        .path()
        .join("reporting-concurrency.sqlite3");
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        database_path.display().to_string().replace('\\', "/")
    );

    let mut reader_db = connect_sqlite(&database_url, 4).await;
    reader_db
        .execute_unprepared("PRAGMA journal_mode = WAL")
        .await
        .expect("enable reporting concurrency WAL");
    store::run_migrations(&reader_db)
        .await
        .expect("run reporting concurrency migrations");
    store::bucket::create(&reader_db, BUCKET, Some("tier-read-owner"))
        .await
        .expect("create reporting concurrency bucket");
    store::bucket::set_versioning_state(&reader_db, BUCKET, BucketVersioningState::Enabled)
        .await
        .expect("enable reporting concurrency versioning");

    let writer_db = connect_sqlite(&database_url, 2).await;
    let now = Utc::now();
    seed_physical(&reader_db, "hot", CONCURRENT_CID, false, now).await;
    seed_physical(&reader_db, "cold", CONCURRENT_CID, true, now).await;
    let seeded = seed_content_version(
        &reader_db,
        CONCURRENT_KEY,
        CONCURRENT_CID,
        Some(CONCURRENT_VERSION),
        1,
        true,
        "hot",
        "STANDARD",
        23,
        now,
        None,
        EncryptionEnvelope::plain(),
    )
    .await;
    let before = immutable_metadata(&reader_db, &seeded).await;

    let pause = Arc::new(QueryPause::new());
    let _release_on_unwind = QueryPauseGuard(pause.clone());
    let triggered = Arc::new(AtomicBool::new(false));
    let (reached_tx, reached_rx) = oneshot::channel();
    let reached_tx = Arc::new(Mutex::new(Some(reached_tx)));
    reader_db.set_metric_callback({
        let pause = pause.clone();
        let triggered = triggered.clone();
        let reached_tx = reached_tx.clone();
        move |info| {
            if is_first_residency_lookup(&info.statement.sql)
                && triggered
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                if let Some(sender) = reached_tx
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    let _ = sender.send(());
                }
                pause.wait_for_release();
            }
        }
    });

    let harness = start_reporting_harness_with_db(reader_db).await;
    let endpoint = harness.endpoint.clone();
    let request =
        tokio::spawn(async move { signed_bucket_get(&endpoint, operation.query()).await });

    tokio::time::timeout(TEST_TIMEOUT, reached_rx)
        .await
        .unwrap_or_else(|_| panic!("{operation:?} never reached object_id residency lookup"))
        .unwrap_or_else(|_| panic!("{operation:?} residency lookup signal was dropped"));

    tokio::time::timeout(TEST_TIMEOUT, publish_standard_ia(&writer_db, &seeded))
        .await
        .unwrap_or_else(|_| panic!("{operation:?} concurrent STANDARD_IA publication timed out"));
    pause.release();

    let response = tokio::time::timeout(TEST_TIMEOUT, request)
        .await
        .unwrap_or_else(|_| panic!("{operation:?} in-flight signed request timed out"))
        .unwrap_or_else(|error| panic!("{operation:?} request task failed: {error}"));
    let status = response.status();
    let xml = response.text().await.expect("read in-flight list XML");
    assert_eq!(
        status,
        StatusCode::OK,
        "{operation:?} mixed the pre-publication object_id lookup with the post-publication version_row_id lookup: {xml}"
    );
    operation.assert_class(&xml, "STANDARD");

    let subsequent = signed_bucket_get(&harness.endpoint, operation.query()).await;
    assert_eq!(
        subsequent.status(),
        StatusCode::OK,
        "subsequent {operation:?}"
    );
    let subsequent_xml = subsequent.text().await.expect("read subsequent list XML");
    operation.assert_class(&subsequent_xml, "STANDARD_IA");

    let after = immutable_metadata(harness.state.store.db(), &seeded).await;
    assert_eq!(
        after.object, before.object,
        "immutable object metadata changed"
    );
    assert_eq!(
        after.version, before.version,
        "immutable version metadata changed"
    );
    assert!(
        !pause.timed_out.load(Ordering::SeqCst),
        "{operation:?} query callback escaped only through its safety timeout"
    );
    assert!(
        harness._kubo.received_requests().await.unwrap().is_empty(),
        "{operation:?} storage-class reporting must not call Kubo"
    );

    tokio::time::timeout(TEST_TIMEOUT, harness.server.shutdown())
        .await
        .expect("shut down reporting concurrency server");
}

async fn connect_sqlite(database_url: &str, max_connections: u32) -> DatabaseConnection {
    let mut options = ConnectOptions::new(database_url.to_owned());
    options
        .max_connections(max_connections)
        .min_connections(1)
        .sqlx_logging(false);
    store::apply_sqlite_busy_timeout(&mut options);
    let db = Database::connect(options)
        .await
        .expect("connect reporting concurrency SQLite database");
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .expect("enable reporting concurrency foreign keys");
    db
}

fn is_first_residency_lookup(sql: &str) -> bool {
    let sql = sql.to_ascii_lowercase().replace(['"', '`'], "");
    sql.contains("select")
        && sql.contains("from version_residencies")
        && sql.contains("version_residencies.object_id")
        && sql.contains(" in ")
}

async fn publish_standard_ia(db: &DatabaseConnection, seeded: &SeededRow) {
    let txn = db
        .begin()
        .await
        .expect("begin seeded STANDARD_IA publication");
    let now = Utc::now();
    residency_reference::Entity::insert(residency_reference::ActiveModel {
        owner_kind: Set("version".to_owned()),
        owner_id: Set(seeded.version_row_id.clone()),
        reason: Set("retained_version".to_owned()),
        version_row_id: Set(seeded.version_row_id.clone()),
        object_id: Set(seeded.object_id.clone()),
        tier: Set("cold".to_owned()),
        cid: Set(CONCURRENT_CID.to_owned()),
        created_at: Set(now),
    })
    .exec(&txn)
    .await
    .expect("insert seeded cold retained reference");
    let removed = residency_reference::Entity::delete_many()
        .filter(residency_reference::Column::OwnerKind.eq("version"))
        .filter(residency_reference::Column::OwnerId.eq(&seeded.version_row_id))
        .filter(residency_reference::Column::Reason.eq("retained_version"))
        .filter(residency_reference::Column::VersionRowId.eq(&seeded.version_row_id))
        .filter(residency_reference::Column::ObjectId.eq(&seeded.object_id))
        .filter(residency_reference::Column::Tier.eq("hot"))
        .filter(residency_reference::Column::Cid.eq(CONCURRENT_CID))
        .exec(&txn)
        .await
        .expect("delete seeded hot retained reference");
    assert_eq!(removed.rows_affected, 1, "seeded publication hot reference");
    let moved = version_residency::Entity::update_many()
        .col_expr(version_residency::Column::PrimaryTier, Expr::value("cold"))
        .col_expr(
            version_residency::Column::StorageClass,
            Expr::value("STANDARD_IA"),
        )
        .col_expr(version_residency::Column::Revision, Expr::value(2))
        .col_expr(version_residency::Column::UpdatedAt, Expr::value(now))
        .filter(version_residency::Column::VersionRowId.eq(&seeded.version_row_id))
        .filter(version_residency::Column::ObjectId.eq(&seeded.object_id))
        .filter(version_residency::Column::Cid.eq(CONCURRENT_CID))
        .filter(version_residency::Column::PrimaryTier.eq("hot"))
        .filter(version_residency::Column::StorageClass.eq("STANDARD"))
        .filter(version_residency::Column::Revision.eq(1))
        .exec(&txn)
        .await
        .expect("move seeded residency to STANDARD_IA");
    assert_eq!(moved.rows_affected, 1, "seeded publication residency fence");
    txn.commit()
        .await
        .expect("commit seeded atomic STANDARD_IA publication");
}

async fn immutable_metadata(db: &DatabaseConnection, seeded: &SeededRow) -> ImmutableMetadata {
    ImmutableMetadata {
        object: object::Entity::find_by_id(&seeded.object_id)
            .one(db)
            .await
            .expect("query immutable object metadata")
            .expect("immutable object metadata"),
        version: object_version::Entity::find_by_id(&seeded.version_row_id)
            .one(db)
            .await
            .expect("query immutable version metadata")
            .expect("immutable version metadata"),
    }
}

async fn start_reporting_harness_with_db(db: DatabaseConnection) -> ReportingHarness {
    let kubo = start_kubo_harness(KuboScript {
        add_replies: Vec::new(),
        cat_bodies: HashMap::new(),
    })
    .await
    .server;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v0/id"))
        .and(matchers::query_param("peerid-base", "b58mh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ID": COLD_NODE_ID
        })))
        .mount(&kubo)
        .await;

    let kubo_uri = kubo.uri();
    let state = Arc::new(AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo_uri.clone()),
        cold_kubo: Some(ipfs_s3_gateway::kubo::KuboClient::new(kubo_uri)),
        store: Store::new(db),
        credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::MasterKey::from_hex(&"0".repeat(64))
            .expect("reporting concurrency master key"),
        pinning: ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    let server =
        start_s3_server(state.clone(), Arc::new(tokio::sync::Mutex::new(Vec::new()))).await;

    ReportingHarness {
        endpoint: server.endpoint.clone(),
        state,
        _kubo: kubo,
        server,
    }
}
