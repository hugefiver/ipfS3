use std::time::Duration;

use chrono::Utc;
use ipfs_s3_gateway::{
    import::SupersedeReason,
    pinning::{
        config::{LeaseDuration, ProviderLimitMap, ProviderLimits, ProviderMode},
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::ContentMode,
    },
    store::{
        self,
        entities::object_version,
        import::ownership::admit_content_mutation,
        object_version::{BucketVersioningState, PublicVersionId, VersionSelector},
        pinning::publication::{
            PinTargetSpec, PublicationObject, PublicationRequest,
            delete_version_with_leases_guarded, publish_object, publish_standard_object,
        },
    },
};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    EntityTrait, QueryFilter, Statement,
};

const POSTGRES_ENV: &str = "IPFS_S3_TEST_POSTGRES_URL";

pub struct PgResidencyFixture {
    pub db: DatabaseConnection,
    url: String,
    schema: String,
    cleanup: SchemaCleanup,
}

impl PgResidencyFixture {
    pub async fn new() -> Self {
        let url = std::env::var(POSTGRES_ENV)
            .expect("IPFS_S3_TEST_POSTGRES_URL must be set for ignored PostgreSQL tests");
        let schema = format!("residency_concurrency_{}", uuid::Uuid::new_v4().simple());
        let cleanup = SchemaCleanup::new(url.clone(), schema.clone());
        let db = connect(&url, None).await;
        db.execute_unprepared(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap_or_else(|_| panic!("create isolated PostgreSQL residency schema"));
        db.execute_unprepared(&format!("SET search_path TO {schema}"))
            .await
            .unwrap_or_else(|_| panic!("select isolated PostgreSQL residency schema"));
        store::run_migrations(&db)
            .await
            .unwrap_or_else(|_| panic!("migrate isolated PostgreSQL residency schema"));
        Self {
            db,
            url,
            schema,
            cleanup,
        }
    }

    pub async fn connection(&self) -> DatabaseConnection {
        connect(&self.url, Some(&self.schema)).await
    }

    pub async fn cleanup(mut self) {
        self.db
            .execute_unprepared(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap_or_else(|_| panic!("drop isolated PostgreSQL residency schema"));
        self.cleanup.disarm();
        self.db
            .close()
            .await
            .unwrap_or_else(|_| panic!("close PostgreSQL residency fixture"));
    }
}

struct SchemaCleanup {
    url: String,
    schema: Option<String>,
}

impl SchemaCleanup {
    fn new(url: String, schema: String) -> Self {
        assert!(valid_schema(&schema));
        Self {
            url,
            schema: Some(schema),
        }
    }

    fn disarm(&mut self) {
        self.schema = None;
    }
}

impl Drop for SchemaCleanup {
    fn drop(&mut self) {
        let Some(schema) = self.schema.take() else {
            return;
        };
        if !valid_schema(&schema) {
            return;
        }
        let url = self.url.clone();
        if let Ok(thread) = std::thread::Builder::new()
            .name("postgres-residency-concurrency-cleanup".to_owned())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                runtime.block_on(async move {
                    let mut options = ConnectOptions::new(url);
                    options.max_connections(1).min_connections(1);
                    let Ok(Ok(db)) =
                        tokio::time::timeout(Duration::from_secs(10), Database::connect(options))
                            .await
                    else {
                        return;
                    };
                    let _ = tokio::time::timeout(
                        Duration::from_secs(10),
                        db.execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE")),
                    )
                    .await;
                    let _ = tokio::time::timeout(Duration::from_secs(10), db.close()).await;
                });
            })
        {
            let _ = thread.join();
        }
    }
}

fn valid_schema(schema: &str) -> bool {
    schema
        .strip_prefix("residency_concurrency_")
        .is_some_and(|suffix| {
            suffix.len() == 32
                && suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}

async fn connect(url: &str, schema: Option<&str>) -> DatabaseConnection {
    let mut options = ConnectOptions::new(url.to_owned());
    options.max_connections(1).min_connections(1);
    let db = Database::connect(options)
        .await
        .unwrap_or_else(|_| panic!("connect to PostgreSQL residency test database"));
    if let Some(schema) = schema {
        assert!(valid_schema(schema));
        db.execute_unprepared(&format!("SET search_path TO {schema}"))
            .await
            .unwrap_or_else(|_| panic!("select isolated PostgreSQL residency schema"));
    }
    db
}

pub async fn backend_pid(db: &DatabaseConnection) -> i32 {
    db.query_one(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT pg_backend_pid() AS pid",
    ))
    .await
    .expect("query PostgreSQL backend PID")
    .expect("PostgreSQL backend PID row")
    .try_get("", "pid")
    .expect("decode PostgreSQL backend PID")
}

pub async fn wait_until_blocked_by(
    observer: &DatabaseConnection,
    contender_pid: i32,
    blocker_pid: i32,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row = observer
                .query_one(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "SELECT $2 = ANY(pg_blocking_pids($1)) AS blocked",
                    [contender_pid.into(), blocker_pid.into()],
                ))
                .await
                .expect("observe PostgreSQL blocking relationship")
                .expect("PostgreSQL blocking relationship row");
            if row
                .try_get::<bool>("", "blocked")
                .expect("decode PostgreSQL blocking relationship")
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("contender must demonstrably wait on the verification transaction");
}

pub fn provider_limits() -> ProviderLimitMap {
    ProviderLimitMap::from([(
        "pinata".to_owned(),
        ProviderLimits {
            priority: 1,
            max_bytes: 10_000,
            max_pins: 100,
            enabled: true,
        },
    )])
}

pub fn publication_request(
    object_id: &str,
    bucket: &str,
    key: &str,
    cid: &str,
    with_lease: bool,
) -> PublicationRequest {
    let object = PublicationObject::from_put(
        object_id.to_owned(),
        bucket,
        key,
        cid.to_owned(),
        7,
        Some("application/octet-stream".to_owned()),
        None,
        false,
        None,
        None,
        Utc::now(),
    );
    let leases = with_lease
        .then(|| LeaseIntent {
            source: LeaseSource::Manual,
            policy_id: "policy:postgres-residency-concurrency".to_owned(),
            provider_mode: ProviderMode::One,
            providers: vec!["pinata".to_owned()],
            content_mode: ContentMode::Object,
            duration: LeaseDuration::parse("1h").expect("valid fixture lease duration"),
        })
        .into_iter()
        .collect();
    PublicationRequest {
        object_target: PinTargetSpec {
            cid: object.cid.clone(),
            logical_size: object.logical_size,
        },
        object,
        tags: Vec::new(),
        policy: PublicationPolicy {
            tags: Vec::new(),
            leases,
        },
    }
}

pub async fn create_bucket(db: &DatabaseConnection, bucket: &str, state: BucketVersioningState) {
    store::bucket::create(db, bucket, None)
        .await
        .expect("create fixture bucket");
    if state != BucketVersioningState::Unversioned {
        store::bucket::set_versioning_state(db, bucket, state)
            .await
            .expect("set fixture bucket versioning");
    }
}

pub async fn publish(
    db: &DatabaseConnection,
    object_id: &str,
    bucket: &str,
    key: &str,
    cid: &str,
    with_lease: bool,
) -> store::object_version::PublicationResult {
    publish_object(
        db,
        publication_request(object_id, bucket, key, cid, with_lease),
        &provider_limits(),
    )
    .await
    .expect("publish fixture object")
}

pub async fn guarded_publish(
    db: &DatabaseConnection,
    object_id: &str,
    bucket: &str,
    key: &str,
    cid: &str,
) -> store::object_version::PublicationResult {
    let now = Utc::now();
    let guard = admit_content_mutation(db, bucket, key, None, SupersedeReason::PutObject, now)
        .await
        .expect("admit guarded PUT overwrite");
    publish_standard_object(
        db,
        publication_request(object_id, bucket, key, cid, false),
        guard,
        &provider_limits(),
    )
    .await
    .expect("publish guarded PUT overwrite")
}

pub async fn guarded_exact_delete(
    db: &DatabaseConnection,
    bucket: &str,
    key: &str,
    public_version_id: &str,
) -> store::object_version::DeleteVersionResult {
    let now = Utc::now();
    let guard = admit_content_mutation(db, bucket, key, None, SupersedeReason::DeleteObject, now)
        .await
        .expect("admit guarded exact delete");
    delete_version_with_leases_guarded(
        db,
        bucket,
        key,
        VersionSelector::Exact(
            PublicVersionId::parse_s3(public_version_id).expect("valid public version ID"),
        ),
        guard,
        now,
    )
    .await
    .expect("execute guarded exact delete")
}

pub async fn version_for_object(db: &DatabaseConnection, object_id: &str) -> object_version::Model {
    object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(object_id))
        .one(db)
        .await
        .expect("load object version")
        .unwrap_or_else(|| panic!("object must retain its fixture version"))
}
