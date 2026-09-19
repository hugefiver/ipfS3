#[allow(dead_code)]
mod support {
    #[path = "cors.rs"]
    pub mod cors;
    #[path = "decompress.rs"]
    pub mod decompress;
    #[path = "sigv4.rs"]
    pub mod sigv4;
}

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use chrono::Utc;
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::store::{
    self,
    entities::{object, object_tag, object_version, pin_lease},
    import::ownership::admit_content_mutation,
    migrations::{
        m20250701_000001_init, m20260707_000001_decompress_zip,
        m20260720_000001_sse_c_key_fingerprint, m20260721_000001_multi_provider_pinning,
        m20260729_000001_ipfs3_import, m20260729_000002_postgres_utc_timestamps,
        m20260730_000001_standard_mutation_fence, m20260813_000001_postgres_json_columns,
        m20260825_000001_object_versioning,
    },
    object_version::{BucketVersioningState, PublicVersionId, VersionSelector},
    pinning::publication::{
        PinTargetSpec, PublicationObject, PublicationRequest, delete_version_with_leases_guarded,
        publish_object, publish_standard_object,
    },
};
use ipfs_s3_gateway::{
    crypto::key::MasterKey,
    error::AppError,
    import::SupersedeReason,
    kubo::KuboClient,
    pinning::{
        config::{LeaseDuration, ProviderLimitMap, ProviderLimits, ProviderMode},
        coordinator::PinningCoordinator,
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::{ContentMode, ObjectTag},
    },
    state::AppState,
    store::Store,
};
use s3s::{
    S3Request,
    dto::{PutObjectTaggingInput, Tag, Tagging},
};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    DbErr, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, Statement,
};
use sea_orm_migration::{MigrationTrait, MigratorTrait, SchemaManager};
use support::{
    decompress::{AddReply, KuboScript, start_kubo_harness, start_s3_server},
    sigv4::send_sigv4,
};

const BACKFILL_COUNT_MISMATCH: &str = "object_versions backfill count mismatch";
const PUBLIC_STATE_DOWN_REFUSAL: &str = "object versioning schema contains public state";

struct PreObjectVersioningMigrator;

impl MigratorTrait for PreObjectVersioningMigrator {
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

struct PgFixture {
    db: DatabaseConnection,
    schema: String,
    url: String,
    cleanup_guard: OwnedPgSchemaCleanup,
}

struct OwnedPgSchemaCleanup {
    url: String,
    schema: Option<String>,
}

impl OwnedPgSchemaCleanup {
    fn new(url: String, schema: String) -> Self {
        assert!(is_owned_versioning_schema(&schema));
        Self {
            url,
            schema: Some(schema),
        }
    }

    fn disarm(&mut self) {
        self.schema = None;
    }
}

impl Drop for OwnedPgSchemaCleanup {
    fn drop(&mut self) {
        let Some(schema) = self.schema.take() else {
            return;
        };
        if !is_owned_versioning_schema(&schema) {
            return;
        }
        let url = self.url.clone();
        if let Ok(thread) = std::thread::Builder::new()
            .name("postgres-versioning-schema-cleanup".to_owned())
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

fn is_owned_versioning_schema(schema: &str) -> bool {
    schema.strip_prefix("versioning_").is_some_and(|suffix| {
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

impl PgFixture {
    async fn independent_connection(&self) -> DatabaseConnection {
        let mut options = ConnectOptions::new(self.url.clone());
        options.max_connections(1).min_connections(1);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared(&format!("SET search_path TO {}", self.schema))
            .await
            .unwrap();
        db
    }

    async fn cleanup(mut self) {
        self.db
            .execute_unprepared(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
        self.cleanup_guard.disarm();
    }
}

async fn legacy_fixture() -> Option<PgFixture> {
    let Ok(url) = std::env::var("IPFS_S3_TEST_POSTGRES_URL") else {
        eprintln!("skipping PostgreSQL object-versioning test: IPFS_S3_TEST_POSTGRES_URL is unset");
        return None;
    };
    let mut options = ConnectOptions::new(url.clone());
    options.max_connections(1).min_connections(1);
    let db = Database::connect(options).await.unwrap();
    let schema = format!("versioning_{}", uuid::Uuid::new_v4().simple());
    let cleanup_guard = OwnedPgSchemaCleanup::new(url.clone(), schema.clone());
    db.execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    db.execute_unprepared(&format!("SET search_path TO {schema}"))
        .await
        .unwrap();
    PreObjectVersioningMigrator::up(&db, None).await.unwrap();
    db.execute_unprepared("INSERT INTO buckets (name) VALUES ('versioning-bucket')")
        .await
        .unwrap();
    for statement in [
        "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
         VALUES ('legacy-a', 'versioning-bucket', 'alpha', 'QmLegacyA', 1, 'QmLegacyA', FALSE, \
                 '2026-08-25 00:00:00+00:00')",
        "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
         VALUES ('current-a', 'versioning-bucket', 'alpha', 'QmCurrentA', 2, 'QmCurrentA', TRUE, \
                 '2026-08-25 00:01:00+00:00')",
        "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
         VALUES ('current-b', 'versioning-bucket', 'beta', 'QmCurrentB', 3, 'QmCurrentB', TRUE, \
                 '2026-08-25 00:02:00+00:00')",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    Some(PgFixture {
        db,
        schema,
        url,
        cleanup_guard,
    })
}

async fn migrated_fixture() -> Option<PgFixture> {
    let fixture = legacy_fixture().await?;
    store::run_migrations(&fixture.db).await.unwrap();
    Some(fixture)
}

async fn assert_rejected(db: &DatabaseConnection, statement: &str) {
    assert!(
        db.execute_unprepared(statement).await.is_err(),
        "PostgreSQL accepted invalid object-versioning input: {statement}"
    );
}

async fn table_exists(db: &DatabaseConnection, table: &str) -> bool {
    db.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT 1 FROM information_schema.tables \
         WHERE table_schema = current_schema() AND table_name = $1",
        [table.to_owned().into()],
    ))
    .await
    .unwrap()
    .is_some()
}

async fn signed_put_bucket_versioning(endpoint: &str, status: &str) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        endpoint,
        "versioning-bucket",
        "",
        &[("versioning", "")],
        format!(
            "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>{status}</Status></VersioningConfiguration>"
        )
        .into_bytes(),
        headers,
        "test",
    )
    .await
}

async fn signed_put_object(endpoint: &str, key: &str, body: &[u8]) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::PUT,
        endpoint,
        "versioning-bucket",
        key,
        &[],
        body.to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn signed_delete_object(
    endpoint: &str,
    key: &str,
    version_id: Option<&str>,
) -> reqwest::Response {
    let query = version_id
        .map(|version_id| vec![("versionId", version_id)])
        .unwrap_or_default();
    send_sigv4(
        reqwest::Method::DELETE,
        endpoint,
        "versioning-bucket",
        key,
        &query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn signed_get_object(
    endpoint: &str,
    key: &str,
    version_id: Option<&str>,
) -> reqwest::Response {
    let query = version_id
        .map(|version_id| vec![("versionId", version_id)])
        .unwrap_or_default();
    send_sigv4(
        reqwest::Method::GET,
        endpoint,
        "versioning-bucket",
        key,
        &query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn signed_head_object(
    endpoint: &str,
    key: &str,
    version_id: Option<&str>,
) -> reqwest::Response {
    let query = version_id
        .map(|version_id| vec![("versionId", version_id)])
        .unwrap_or_default();
    send_sigv4(
        reqwest::Method::HEAD,
        endpoint,
        "versioning-bucket",
        key,
        &query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn signed_list_object_versions(endpoint: &str, prefix: &str) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        endpoint,
        "versioning-bucket",
        "",
        &[("versions", ""), ("prefix", prefix)],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

fn response_version_id(response: &reqwest::Response, operation: &str) -> String {
    response
        .headers()
        .get("x-amz-version-id")
        .unwrap_or_else(|| panic!("{operation}: missing x-amz-version-id"))
        .to_str()
        .unwrap_or_else(|_| panic!("{operation}: x-amz-version-id is not text"))
        .to_owned()
}

fn contention_limits() -> ProviderLimitMap {
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

fn contention_request(id: &str, key: &str, cid: &str) -> PublicationRequest {
    let tags = vec![
        ObjectTag::new("owner", id),
        ObjectTag::new("ipfs-s3:pin", "true"),
        ObjectTag::new("ipfs-s3:duration", "1h"),
    ];
    let object = PublicationObject::from_put(
        id.to_owned(),
        "versioning-bucket",
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
    PublicationRequest {
        object: object.clone(),
        tags: tags.clone(),
        policy: PublicationPolicy {
            tags,
            leases: vec![LeaseIntent {
                source: LeaseSource::Manual,
                policy_id: "policy:postgres-contention".to_owned(),
                provider_mode: ProviderMode::One,
                providers: vec!["pinata".to_owned()],
                content_mode: ContentMode::Object,
                duration: LeaseDuration::parse("1h").unwrap(),
            }],
        },
        object_target: PinTargetSpec {
            cid: object.cid,
            logical_size: object.logical_size,
        },
    }
}

fn exact_put_object_tagging_request(
    bucket: &str,
    key: &str,
    version_id: &str,
) -> S3Request<PutObjectTaggingInput> {
    S3Request {
        input: PutObjectTaggingInput {
            bucket: bucket.to_owned(),
            checksum_algorithm: None,
            content_md5: None,
            expected_bucket_owner: None,
            key: key.to_owned(),
            request_payer: None,
            tagging: Tagging {
                tag_set: vec![
                    Tag {
                        key: Some("classification".to_owned()),
                        value: Some("historical".to_owned()),
                    },
                    Tag {
                        key: Some("race".to_owned()),
                        value: Some("public-operation".to_owned()),
                    },
                ],
            },
            version_id: Some(version_id.to_owned()),
        },
        method: http::Method::PUT,
        uri: format!("/{bucket}/{key}?tagging").parse().unwrap(),
        headers: HeaderMap::new(),
        extensions: http::Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

async fn parallel_publish(
    first: DatabaseConnection,
    second: DatabaseConnection,
    key: &'static str,
    first_id: &'static str,
    second_id: &'static str,
) {
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let first_barrier = barrier.clone();
    let first_publish = tokio::spawn(async move {
        first_barrier.wait().await;
        publish_object(
            &first,
            contention_request(first_id, key, &format!("bafy-{first_id}")),
            &contention_limits(),
        )
        .await
    });
    let second_barrier = barrier.clone();
    let second_publish = tokio::spawn(async move {
        second_barrier.wait().await;
        publish_object(
            &second,
            contention_request(second_id, key, &format!("bafy-{second_id}")),
            &contention_limits(),
        )
        .await
    });
    barrier.wait().await;
    tokio::time::timeout(Duration::from_secs(15), async {
        first_publish.await.unwrap().unwrap();
        second_publish.await.unwrap().unwrap();
    })
    .await
    .expect("parallel PostgreSQL publications deadlocked");
}

#[tokio::test]
async fn postgres_object_versioning_migration_adds_status_table_indexes_and_checks() {
    let Some(fixture) = migrated_fixture().await else {
        return;
    };
    let columns = fixture
        .db
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT table_name, column_name, data_type \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() \
                AND (table_name = 'buckets' AND column_name = 'versioning_status' \
                     OR (table_name = 'object_versions' \
                         AND column_name NOT IN ('lifecycle_age_started_at', 'became_noncurrent_at'))) \
             ORDER BY table_name, ordinal_position",
        ))
        .await
        .unwrap()
        .into_iter()
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
                "versioning_status".to_owned(),
                "text".to_owned()
            ),
            (
                "object_versions".to_owned(),
                "id".to_owned(),
                "text".to_owned()
            ),
            (
                "object_versions".to_owned(),
                "bucket".to_owned(),
                "text".to_owned()
            ),
            (
                "object_versions".to_owned(),
                "key".to_owned(),
                "text".to_owned()
            ),
            (
                "object_versions".to_owned(),
                "version_id".to_owned(),
                "text".to_owned()
            ),
            (
                "object_versions".to_owned(),
                "kind".to_owned(),
                "text".to_owned()
            ),
            (
                "object_versions".to_owned(),
                "object_id".to_owned(),
                "text".to_owned()
            ),
            (
                "object_versions".to_owned(),
                "sequence".to_owned(),
                "bigint".to_owned()
            ),
            (
                "object_versions".to_owned(),
                "is_latest".to_owned(),
                "boolean".to_owned()
            ),
            (
                "object_versions".to_owned(),
                "created_at".to_owned(),
                "timestamp with time zone".to_owned(),
            ),
            (
                "object_versions".to_owned(),
                "updated_at".to_owned(),
                "timestamp with time zone".to_owned(),
            ),
        ]
    );
    let indexes = fixture
        .db
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT indexname FROM pg_indexes \
             WHERE schemaname = current_schema() AND tablename = 'object_versions'",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "indexname").unwrap())
        .collect::<Vec<_>>();
    for index in [
        "uq_object_versions_latest",
        "uq_object_versions_sequence",
        "uq_object_versions_null_slot",
        "uq_object_versions_public_id",
        "idx_object_versions_exact",
        "idx_object_versions_key_order",
        "idx_object_versions_bucket_order",
    ] {
        assert!(
            indexes.contains(&index.to_owned()),
            "missing PostgreSQL index {index}"
        );
    }
    assert_rejected(
        &fixture.db,
        "UPDATE buckets SET versioning_status = 'enabled' WHERE name = 'versioning-bucket'",
    )
    .await;
    assert_rejected(
        &fixture.db,
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at, lifecycle_age_started_at) \
         VALUES ('invalid-marker', 'versioning-bucket', 'invalid', NULL, 'delete_marker', \
                 'current-a', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_object_versioning_backfills_only_legacy_latest_as_hidden_null() {
    let Some(fixture) = migrated_fixture().await else {
        return;
    };
    let rows = fixture
        .db
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT key, version_id, kind, object_id, sequence, is_latest \
             FROM object_versions ORDER BY key ASC",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.try_get::<String>("", "key").unwrap(),
                row.try_get::<Option<String>>("", "version_id").unwrap(),
                row.try_get::<String>("", "kind").unwrap(),
                row.try_get::<Option<String>>("", "object_id").unwrap(),
                row.try_get::<i64>("", "sequence").unwrap(),
                row.try_get::<bool>("", "is_latest").unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            (
                "alpha".to_owned(),
                None,
                "object".to_owned(),
                Some("current-a".to_owned()),
                1,
                true,
            ),
            (
                "beta".to_owned(),
                None,
                "object".to_owned(),
                Some("current-b".to_owned()),
                1,
                true,
            ),
        ]
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_object_versioning_backfill_count_must_match() {
    let Some(fixture) = migrated_fixture().await else {
        return;
    };
    let selected: i64 = fixture
        .db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT COUNT(*) AS count FROM objects WHERE is_latest = TRUE",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "count")
        .unwrap();
    let inserted: i64 = fixture
        .db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT COUNT(*) AS count FROM object_versions",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "count")
        .unwrap();
    assert_eq!(selected, inserted, "{BACKFILL_COUNT_MISMATCH}");
    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_object_versioning_down_allows_only_hidden_null_rows() {
    // Exercise this historical migration at its own schema boundary, not beneath
    // later residency foreign keys. This is not a downgrade of current schema.
    let Some(fixture) = legacy_fixture().await else {
        return;
    };
    m20260825_000001_object_versioning::Migration
        .up(&SchemaManager::new(&fixture.db))
        .await
        .unwrap();
    m20260825_000001_object_versioning::Migration
        .down(&SchemaManager::new(&fixture.db))
        .await
        .unwrap();
    assert!(!table_exists(&fixture.db, "object_versions").await);
    let status_column = fixture
        .db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT 1 FROM information_schema.columns \
             WHERE table_schema = current_schema() \
               AND table_name = 'buckets' \
               AND column_name = 'versioning_status'",
        ))
        .await
        .unwrap();
    assert!(status_column.is_none());
    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_object_versioning_down_refuses_public_state_versions_and_markers() {
    for public_state_statement in [
        "UPDATE buckets SET versioning_status = 'Enabled' WHERE name = 'versioning-bucket'",
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at, lifecycle_age_started_at) \
         VALUES ('public-version', 'versioning-bucket', 'public', 'opaque-version', 'object', \
                 'current-a', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at, lifecycle_age_started_at) \
         VALUES ('public-marker', 'versioning-bucket', 'marker', NULL, 'delete_marker', NULL, \
                 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ] {
        let Some(fixture) = migrated_fixture().await else {
            return;
        };
        fixture
            .db
            .execute_unprepared(public_state_statement)
            .await
            .unwrap();
        let result = m20260825_000001_object_versioning::Migration
            .down(&SchemaManager::new(&fixture.db))
            .await;
        assert!(matches!(
            result,
            Err(DbErr::Migration(message)) if message == PUBLIC_STATE_DOWN_REFUSAL
        ));
        assert!(table_exists(&fixture.db, "object_versions").await);
        fixture.cleanup().await;
    }
}

#[tokio::test]
async fn postgres_parallel_publish_delete_tag_serializes_without_deadlock() {
    let Some(fixture) = migrated_fixture().await else {
        return;
    };
    let second = fixture.independent_connection().await;
    store::bucket::set_versioning_state(
        &fixture.db,
        "versioning-bucket",
        BucketVersioningState::Enabled,
    )
    .await
    .unwrap();

    parallel_publish(
        fixture.db.clone(),
        second.clone(),
        "enabled-race",
        "pg-enabled-first",
        "pg-enabled-second",
    )
    .await;
    let enabled = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq("versioning-bucket"))
        .filter(object_version::Column::Key.eq("enabled-race"))
        .order_by_asc(object_version::Column::Sequence)
        .all(&fixture.db)
        .await
        .unwrap();
    assert_eq!(enabled.len(), 2);
    assert_eq!(
        enabled
            .iter()
            .map(|version| version.sequence)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        enabled.iter().filter(|version| version.is_latest).count(),
        1
    );
    assert!(enabled.iter().all(|version| version.version_id.is_some()));
    for owner in ["pg-enabled-first", "pg-enabled-second"] {
        assert_eq!(
            object_tag::Entity::find()
                .filter(object_tag::Column::ObjectId.eq(owner))
                .count(&fixture.db)
                .await
                .unwrap(),
            3
        );
        assert!(
            pin_lease::Entity::find()
                .filter(pin_lease::Column::OwnerObjectId.eq(owner))
                .all(&fixture.db)
                .await
                .unwrap()
                .iter()
                .all(|lease| lease.state == "active")
        );
        let owner_tags = store::pinning::tags::list_object_tags(&fixture.db, owner)
            .await
            .unwrap();
        assert!(
            owner_tags
                .iter()
                .all(|tag| tag.key != "classification" && tag.key != "race")
        );
    }

    let current = enabled.iter().find(|version| version.is_latest).unwrap();
    let historical = enabled.iter().find(|version| !version.is_latest).unwrap();
    let current_owner = current.object_id.as_deref().unwrap();
    let historical_owner = historical.object_id.as_deref().unwrap();
    store::pinning::tags::replace_object_tags(
        &second,
        current_owner,
        &[
            ObjectTag::new("owner", "retagged-current"),
            ObjectTag::new("ipfs-s3:pin", "true"),
            ObjectTag::new("ipfs-s3:duration", "1h"),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        store::pinning::tags::list_object_tags(&fixture.db, current_owner)
            .await
            .unwrap(),
        [
            ObjectTag::new("ipfs-s3:duration", "1h"),
            ObjectTag::new("ipfs-s3:pin", "true"),
            ObjectTag::new("owner", "retagged-current"),
        ]
    );
    assert_eq!(
        store::pinning::tags::list_object_tags(&fixture.db, historical_owner)
            .await
            .unwrap()
            .iter()
            .find(|tag| tag.key == "owner")
            .map(|tag| tag.value.as_str()),
        Some(historical_owner)
    );

    let exact_guard = admit_content_mutation(
        &fixture.db,
        "versioning-bucket",
        "enabled-race",
        None,
        SupersedeReason::DeleteObject,
        Utc::now(),
    )
    .await
    .unwrap();
    delete_version_with_leases_guarded(
        &second,
        "versioning-bucket",
        "enabled-race",
        VersionSelector::Exact(
            PublicVersionId::parse_s3(historical.version_id.as_deref().unwrap()).unwrap(),
        ),
        exact_guard,
        Utc::now(),
    )
    .await
    .unwrap();
    assert_eq!(
        object_tag::Entity::find()
            .filter(object_tag::Column::ObjectId.eq(historical_owner))
            .count(&fixture.db)
            .await
            .unwrap(),
        0
    );
    assert!(
        pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq(historical_owner))
            .all(&fixture.db)
            .await
            .unwrap()
            .iter()
            .all(|lease| lease.state == "cancelled")
    );
    assert!(
        pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq(current_owner))
            .all(&fixture.db)
            .await
            .unwrap()
            .iter()
            .all(|lease| lease.state == "active")
    );

    let simple_guard = admit_content_mutation(
        &fixture.db,
        "versioning-bucket",
        "enabled-race",
        None,
        SupersedeReason::DeleteObject,
        Utc::now(),
    )
    .await
    .unwrap();
    let simple = delete_version_with_leases_guarded(
        &second,
        "versioning-bucket",
        "enabled-race",
        VersionSelector::Current,
        simple_guard,
        Utc::now(),
    )
    .await
    .unwrap();
    assert!(simple.created_delete_marker);

    store::bucket::set_versioning_state(
        &fixture.db,
        "versioning-bucket",
        BucketVersioningState::Suspended,
    )
    .await
    .unwrap();
    parallel_publish(
        fixture.db.clone(),
        second.clone(),
        "suspended-race",
        "pg-suspended-first",
        "pg-suspended-second",
    )
    .await;
    let suspended = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq("versioning-bucket"))
        .filter(object_version::Column::Key.eq("suspended-race"))
        .all(&fixture.db)
        .await
        .unwrap();
    assert_eq!(suspended.len(), 1);
    assert_eq!(suspended[0].sequence, 2);
    assert!(suspended[0].version_id.is_none());
    assert!(suspended[0].is_latest);

    let now = Utc::now();
    let stale_guard = admit_content_mutation(
        &fixture.db,
        "versioning-bucket",
        "stale-race",
        None,
        SupersedeReason::PutObject,
        now,
    )
    .await
    .unwrap();
    let winner_guard = admit_content_mutation(
        &second,
        "versioning-bucket",
        "stale-race",
        None,
        SupersedeReason::PutObject,
        now,
    )
    .await
    .unwrap();
    let stale = publish_standard_object(
        &fixture.db,
        contention_request("pg-stale-loser", "stale-race", "bafy-pg-stale"),
        stale_guard,
        &contention_limits(),
    )
    .await
    .unwrap_err();
    assert!(matches!(stale, AppError::StaleContentMutation));
    publish_standard_object(
        &second,
        contention_request("pg-stale-winner", "stale-race", "bafy-pg-winner"),
        winner_guard,
        &contention_limits(),
    )
    .await
    .unwrap();
    assert_eq!(
        object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq("versioning-bucket"))
            .filter(object_version::Column::Key.eq("stale-race"))
            .count(&fixture.db)
            .await
            .unwrap(),
        1
    );

    drop(second);
    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_synchronized_publish_exact_delete_tag_serializes_without_deadlock() {
    let Some(fixture) = migrated_fixture().await else {
        return;
    };
    let publisher = fixture.independent_connection().await;
    let deleter = fixture.independent_connection().await;
    let tagger = fixture.independent_connection().await;
    let key = "synchronized-publish-delete-tag";
    store::bucket::set_versioning_state(
        &fixture.db,
        "versioning-bucket",
        BucketVersioningState::Enabled,
    )
    .await
    .unwrap();
    publish_object(
        &fixture.db,
        contention_request("pg-sync-first", key, "bafy-pg-sync-first"),
        &contention_limits(),
    )
    .await
    .unwrap();
    publish_object(
        &fixture.db,
        contention_request("pg-sync-second", key, "bafy-pg-sync-second"),
        &contention_limits(),
    )
    .await
    .unwrap();
    let seeded = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq("versioning-bucket"))
        .filter(object_version::Column::Key.eq(key))
        .order_by_asc(object_version::Column::Sequence)
        .all(&fixture.db)
        .await
        .unwrap();
    assert_eq!(seeded.len(), 2);
    let historical = seeded
        .iter()
        .find(|version| !version.is_latest)
        .expect("seeded historical version must exist");
    let historical_version_id = historical
        .version_id
        .clone()
        .expect("enabled historical version must have an opaque ID");
    let historical_owner = historical
        .object_id
        .clone()
        .expect("historical object version must retain its owner");
    let tagging_state = Arc::new(AppState {
        kubo: KuboClient::new("http://127.0.0.1:5001".to_owned()),
        cold_kubo: None,
        store: Store::new(tagger),
        credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
        master_key: MasterKey::from_hex(&"0".repeat(64)).expect("zero test master key"),
        pinning: PinningCoordinator::disabled_for_test(),
    });

    let publication_guard = admit_content_mutation(
        &publisher,
        "versioning-bucket",
        key,
        None,
        SupersedeReason::PutObject,
        Utc::now(),
    )
    .await
    .unwrap();
    let delete_guard = admit_content_mutation(
        &deleter,
        "versioning-bucket",
        key,
        None,
        SupersedeReason::DeleteObject,
        Utc::now(),
    )
    .await
    .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(4));
    let publish_barrier = barrier.clone();
    let publish_key = key.to_owned();
    let publication = tokio::spawn(async move {
        publish_barrier.wait().await;
        publish_standard_object(
            &publisher,
            contention_request(
                "pg-sync-race-publication",
                &publish_key,
                "bafy-pg-sync-race-publication",
            ),
            publication_guard,
            &contention_limits(),
        )
        .await
    });
    let delete_barrier = barrier.clone();
    let delete_key = key.to_owned();
    let tagging_version_id = historical_version_id.clone();
    let deletion = tokio::spawn(async move {
        delete_barrier.wait().await;
        delete_version_with_leases_guarded(
            &deleter,
            "versioning-bucket",
            &delete_key,
            VersionSelector::Exact(PublicVersionId::parse_s3(&historical_version_id).unwrap()),
            delete_guard,
            Utc::now(),
        )
        .await
    });
    let tag_barrier = barrier.clone();
    let tag_state = tagging_state.clone();
    let tag_request =
        exact_put_object_tagging_request("versioning-bucket", key, &tagging_version_id);
    let tag = tokio::spawn(async move {
        tag_barrier.wait().await;
        ipfs_s3_gateway::s3::ops::tagging::put_object_tagging(&tag_state, tag_request).await
    });
    barrier.wait().await;
    let (publication_result, deletion_result, tag_result) =
        tokio::time::timeout(Duration::from_secs(15), async {
            (
                publication.await.unwrap(),
                deletion.await.unwrap(),
                tag.await.unwrap(),
            )
        })
        .await
        .expect("synchronized PostgreSQL publication/delete/tag race deadlocked");

    let publication_succeeded = publication_result.is_ok();
    let publication_stale = matches!(publication_result, Err(AppError::StaleContentMutation));
    let deletion_succeeded = deletion_result.is_ok();
    let deletion_stale = matches!(deletion_result, Err(AppError::StaleContentMutation));
    assert!(publication_succeeded || publication_stale);
    assert!(deletion_succeeded || deletion_stale);
    assert_ne!(publication_succeeded, deletion_succeeded);
    let tag_succeeded = tag_result.is_ok();
    let tag_no_such_version = tag_result
        .as_ref()
        .err()
        .is_some_and(|error| error.code().as_str() == "NoSuchVersion");
    assert!(tag_succeeded || tag_no_such_version);
    assert!(!tag_no_such_version || deletion_succeeded);
    if let Ok(response) = &tag_result {
        assert_eq!(
            response.output.version_id.as_deref(),
            Some(tagging_version_id.as_str())
        );
    }

    let versions = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq("versioning-bucket"))
        .filter(object_version::Column::Key.eq(key))
        .order_by_asc(object_version::Column::Sequence)
        .all(&fixture.db)
        .await
        .unwrap();
    assert_eq!(
        versions.iter().filter(|version| version.is_latest).count(),
        1
    );
    assert!(
        versions
            .iter()
            .filter(|version| version.version_id.is_none())
            .count()
            <= 1
    );
    let sequences = versions
        .iter()
        .map(|version| version.sequence)
        .collect::<HashSet<_>>();
    assert_eq!(sequences.len(), versions.len());
    let latest = versions
        .iter()
        .find(|version| version.is_latest)
        .expect("serialized race must retain one latest version");
    let projections = object::Entity::find()
        .filter(object::Column::Bucket.eq("versioning-bucket"))
        .filter(object::Column::Key.eq(key))
        .filter(object::Column::IsLatest.eq(true))
        .all(&fixture.db)
        .await
        .unwrap();
    assert_eq!(projections.len(), 1);
    assert_eq!(
        projections[0].id,
        latest
            .object_id
            .as_deref()
            .expect("latest object version must retain an owner")
    );
    let raced_object = object::Entity::find_by_id("pg-sync-race-publication")
        .one(&fixture.db)
        .await
        .unwrap();
    if publication_succeeded {
        assert!(raced_object.is_some());
    } else {
        assert!(
            raced_object.is_none(),
            "stale publication resurrected an object"
        );
    }

    if deletion_succeeded {
        assert!(
            versions
                .iter()
                .all(|version| version.object_id.as_deref() != Some(historical_owner.as_str()))
        );
        assert_eq!(
            object_tag::Entity::find()
                .filter(object_tag::Column::ObjectId.eq(&historical_owner))
                .count(&fixture.db)
                .await
                .unwrap(),
            0
        );
        assert!(
            pin_lease::Entity::find()
                .filter(pin_lease::Column::OwnerObjectId.eq(&historical_owner))
                .all(&fixture.db)
                .await
                .unwrap()
                .iter()
                .all(|lease| lease.state == "cancelled")
        );
    } else {
        assert!(tag_succeeded);
        assert_eq!(
            store::pinning::tags::list_object_tags(&fixture.db, &historical_owner)
                .await
                .unwrap(),
            [
                ObjectTag::new("classification", "historical"),
                ObjectTag::new("race", "public-operation"),
            ]
        );
        assert!(
            pin_lease::Entity::find()
                .filter(pin_lease::Column::OwnerObjectId.eq(&historical_owner))
                .all(&fixture.db)
                .await
                .unwrap()
                .iter()
                .all(|lease| lease.state == "cancelled")
        );
    }
    for owner in versions
        .iter()
        .filter_map(|version| version.object_id.as_deref())
    {
        if owner == historical_owner.as_str() {
            assert!(deletion_stale && tag_succeeded);
            continue;
        }
        assert!(
            pin_lease::Entity::find()
                .filter(pin_lease::Column::OwnerObjectId.eq(owner))
                .all(&fixture.db)
                .await
                .unwrap()
                .iter()
                .all(|lease| lease.state == "active")
        );
    }
    let current_tags = store::pinning::tags::list_object_tags(
        &fixture.db,
        latest
            .object_id
            .as_deref()
            .expect("latest version must retain its internal owner"),
    )
    .await
    .unwrap();
    assert!(
        current_tags
            .iter()
            .all(|tag| tag.key != "classification" && tag.key != "race")
    );

    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_suspended_null_replacement_preserves_opaque_versions() {
    let Some(fixture) = migrated_fixture().await else {
        return;
    };
    let key = "suspended-aws-parity";
    let opaque_one_cid = "bafy-pg-aws-enabled-first";
    let shared_cid = "bafy-pg-aws-enabled-second";
    let null_replacement_cid = "bafy-pg-aws-suspended-first";
    store::bucket::set_versioning_state(
        &fixture.db,
        "versioning-bucket",
        BucketVersioningState::Enabled,
    )
    .await
    .unwrap();

    let first_opaque = publish_object(
        &fixture.db,
        contention_request("pg-aws-enabled-first", key, opaque_one_cid),
        &contention_limits(),
    )
    .await
    .unwrap()
    .version_id
    .expect("enabled publication must return an opaque public version ID");
    let second_opaque = publish_object(
        &fixture.db,
        contention_request("pg-aws-enabled-second", key, shared_cid),
        &contention_limits(),
    )
    .await
    .unwrap()
    .version_id
    .expect("enabled publication must return an opaque public version ID");
    assert!(matches!(
        PublicVersionId::parse_s3(&first_opaque),
        Ok(PublicVersionId::Opaque(_))
    ));
    assert!(matches!(
        PublicVersionId::parse_s3(&second_opaque),
        Ok(PublicVersionId::Opaque(_))
    ));
    assert_ne!(first_opaque, second_opaque);

    let marker_created_at = Utc::now();
    let marker_guard = admit_content_mutation(
        &fixture.db,
        "versioning-bucket",
        key,
        None,
        SupersedeReason::DeleteObject,
        marker_created_at,
    )
    .await
    .unwrap();
    let marker = delete_version_with_leases_guarded(
        &fixture.db,
        "versioning-bucket",
        key,
        VersionSelector::Current,
        marker_guard,
        marker_created_at,
    )
    .await
    .unwrap();
    assert!(marker.created_delete_marker);
    assert!(!marker.deleted_delete_marker);
    let marker_version_id = marker
        .version_id
        .expect("enabled simple delete must return an opaque marker version ID");
    let marker_selector = PublicVersionId::parse_s3(&marker_version_id).unwrap();
    assert!(matches!(&marker_selector, PublicVersionId::Opaque(_)));
    let marker_row = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq("versioning-bucket"))
        .filter(object_version::Column::Key.eq(key))
        .filter(object_version::Column::VersionId.eq(&marker_version_id))
        .one(&fixture.db)
        .await
        .unwrap()
        .expect("created marker must be indexed");
    assert_eq!(marker_row.kind, "delete_marker");
    assert!(marker_row.is_latest);

    let marker_deleted_at = marker_created_at + chrono::Duration::milliseconds(1);
    let marker_delete_guard = admit_content_mutation(
        &fixture.db,
        "versioning-bucket",
        key,
        None,
        SupersedeReason::DeleteObject,
        marker_deleted_at,
    )
    .await
    .unwrap();
    let marker_delete = delete_version_with_leases_guarded(
        &fixture.db,
        "versioning-bucket",
        key,
        VersionSelector::Exact(marker_selector),
        marker_delete_guard,
        marker_deleted_at,
    )
    .await
    .unwrap();
    assert!(marker_delete.deleted_delete_marker);
    assert!(!marker_delete.created_delete_marker);
    assert_eq!(
        marker_delete.version_id.as_deref(),
        Some(marker_version_id.as_str())
    );
    let restored = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq("versioning-bucket"))
        .filter(object_version::Column::Key.eq(key))
        .filter(object_version::Column::IsLatest.eq(true))
        .one(&fixture.db)
        .await
        .unwrap()
        .expect("exact marker deletion must restore the prior opaque version");
    assert_eq!(restored.version_id.as_deref(), Some(second_opaque.as_str()));
    let first_row = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq("versioning-bucket"))
        .filter(object_version::Column::Key.eq(key))
        .filter(object_version::Column::VersionId.eq(&first_opaque))
        .one(&fixture.db)
        .await
        .unwrap()
        .expect("first opaque version must remain indexed after marker deletion");
    assert!(!first_row.is_latest);

    store::bucket::set_versioning_state(
        &fixture.db,
        "versioning-bucket",
        BucketVersioningState::Suspended,
    )
    .await
    .unwrap();
    for (id, cid) in [
        ("pg-aws-suspended-first", null_replacement_cid),
        ("pg-aws-suspended-second", shared_cid),
    ] {
        let version_id = publish_object(
            &fixture.db,
            contention_request(id, key, cid),
            &contention_limits(),
        )
        .await
        .unwrap()
        .version_id;
        assert!(matches!(
            version_id
                .as_deref()
                .and_then(|id| PublicVersionId::parse_s3(id).ok()),
            Some(PublicVersionId::Null)
        ));
    }

    let rows = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq("versioning-bucket"))
        .filter(object_version::Column::Key.eq(key))
        .order_by_asc(object_version::Column::Sequence)
        .all(&fixture.db)
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.iter().map(|row| row.sequence).collect::<Vec<_>>(),
        [1, 2, 4],
        "the second suspended publication replaces the null slot after allocating its sequence"
    );
    assert_eq!(
        rows.iter()
            .filter_map(|row| row.version_id.as_deref())
            .filter(|version_id| *version_id == first_opaque)
            .count(),
        1
    );
    assert_eq!(
        rows.iter()
            .filter_map(|row| row.version_id.as_deref())
            .filter(|version_id| *version_id == second_opaque)
            .count(),
        1
    );
    assert_eq!(
        rows.iter().filter(|row| row.version_id.is_none()).count(),
        1
    );
    assert_eq!(rows.iter().filter(|row| row.is_latest).count(), 1);
    let latest = rows.iter().find(|row| row.is_latest).unwrap();
    assert!(latest.version_id.is_none());
    let first_opaque_object = object::Entity::find_by_id("pg-aws-enabled-first")
        .one(&fixture.db)
        .await
        .unwrap()
        .expect("first opaque object row must be retained");
    assert_eq!(first_opaque_object.cid, opaque_one_cid);
    let second_opaque_object = object::Entity::find_by_id("pg-aws-enabled-second")
        .one(&fixture.db)
        .await
        .unwrap()
        .expect("second opaque object row must be retained");
    assert_eq!(second_opaque_object.cid, shared_cid);
    assert_ne!(first_opaque_object.cid, second_opaque_object.cid);
    let latest_object_id = latest
        .object_id
        .as_deref()
        .expect("latest null version must retain its object ID");
    assert_eq!(latest_object_id, "pg-aws-suspended-second");
    let latest_null_object = object::Entity::find_by_id(latest_object_id)
        .one(&fixture.db)
        .await
        .unwrap()
        .expect("latest null object row must be retained");
    assert_eq!(latest_null_object.cid, shared_cid);
    assert_ne!(second_opaque_object.id, latest_null_object.id);
    assert_eq!(second_opaque_object.cid, latest_null_object.cid);
    assert!(rows.iter().any(|row| {
        row.version_id.as_deref() == Some(second_opaque.as_str())
            && row.object_id.as_deref() == Some(second_opaque_object.id.as_str())
    }));

    let visible =
        store::object_version::scan_versions(&fixture.db, "versioning-bucket", key, None, 10)
            .await
            .unwrap();
    assert_eq!(visible.len(), 3);
    assert_eq!(
        visible
            .iter()
            .map(|version| {
                (
                    version.sequence,
                    version.public_version_id.as_str(),
                    version.is_latest,
                    version.object.as_ref().map(|object| object.id.as_str()),
                )
            })
            .collect::<Vec<_>>(),
        rows.iter()
            .rev()
            .map(|row| {
                (
                    row.sequence,
                    row.version_id.as_deref().unwrap_or("null"),
                    row.is_latest,
                    row.object_id.as_deref(),
                )
            })
            .collect::<Vec<_>>()
    );

    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_http_marker_restore_then_suspended_replacement_preserves_all_versions() {
    const KEY: &str = "marker-restore-suspended.txt";
    const CID_ONE: &str = "QmPgHttpMarkerRestoreOne";
    const CID_TWO: &str = "QmPgHttpMarkerRestoreTwo";
    const CID_THREE: &str = "QmPgHttpMarkerRestoreThree";

    let Some(fixture) = migrated_fixture().await else {
        return;
    };
    let kubo = start_kubo_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok(CID_ONE),
            AddReply::Ok(CID_TWO),
            AddReply::Ok(CID_THREE),
            AddReply::Ok(CID_TWO),
        ],
        cat_bodies: HashMap::from([
            (CID_ONE.to_owned(), b"version one".to_vec()),
            (CID_TWO.to_owned(), b"version two".to_vec()),
        ]),
    })
    .await;
    let state = Arc::new(AppState {
        kubo: KuboClient::new(kubo.server.uri()),
        cold_kubo: None,
        store: Store::new(fixture.db.clone()),
        credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
        master_key: MasterKey::from_hex(&"0".repeat(64)).expect("zero test master key"),
        pinning: PinningCoordinator::disabled_for_test(),
    });
    let server =
        start_s3_server(state.clone(), Arc::new(tokio::sync::Mutex::new(Vec::new()))).await;
    let endpoint = server.endpoint.clone();
    let db = fixture.db.clone();
    let test_state = state.clone();

    let result = tokio::spawn(async move {
        let enabled = signed_put_bucket_versioning(&endpoint, "Enabled").await;
        assert_eq!(
            enabled.status(),
            StatusCode::OK,
            "enable versioning before signed object sequence"
        );

        let first_put = signed_put_object(&endpoint, KEY, b"version one").await;
        assert_eq!(first_put.status(), StatusCode::OK, "enabled signed PUT1");
        let version_one = response_version_id(&first_put, "enabled signed PUT1");
        assert!(
            version_one != "null",
            "enabled signed PUT1 must return an opaque version ID"
        );

        let second_put = signed_put_object(&endpoint, KEY, b"version two").await;
        assert_eq!(second_put.status(), StatusCode::OK, "enabled signed PUT2");
        let version_two = response_version_id(&second_put, "enabled signed PUT2");
        assert!(
            version_two != "null" && version_two != version_one,
            "enabled signed PUT2 must return a distinct opaque version ID"
        );

        let marker_delete = signed_delete_object(&endpoint, KEY, None).await;
        assert_eq!(
            marker_delete.status(),
            StatusCode::NO_CONTENT,
            "enabled simple signed delete"
        );
        let marker_version = response_version_id(&marker_delete, "enabled simple signed delete");
        assert!(
            marker_version != "null"
                && marker_version != version_one
                && marker_version != version_two,
            "enabled simple delete must return a distinct opaque marker ID"
        );

        let historical_get = signed_get_object(&endpoint, KEY, Some(&version_one)).await;
        assert_eq!(
            historical_get.status(),
            StatusCode::OK,
            "historical signed GET while delete marker exists"
        );
        assert!(
            historical_get
                .headers()
                .get("x-amz-version-id")
                .is_some_and(|header| header == version_one.as_str()),
            "historical signed GET must select PUT1"
        );
        assert!(
            historical_get
                .bytes()
                .await
                .expect("historical signed GET body")
                .as_ref()
                == b"version one",
            "historical signed GET body"
        );

        assert_eq!(
            signed_get_object(&endpoint, KEY, None).await.status(),
            StatusCode::NOT_FOUND,
            "current signed GET while delete marker exists"
        );
        assert_eq!(
            signed_head_object(&endpoint, KEY, None).await.status(),
            StatusCode::NOT_FOUND,
            "current signed HEAD while delete marker exists"
        );
        assert_eq!(
            signed_head_object(&endpoint, KEY, Some(&marker_version))
                .await
                .status(),
            StatusCode::METHOD_NOT_ALLOWED,
            "explicit delete-marker signed HEAD"
        );

        let marker_remove = signed_delete_object(&endpoint, KEY, Some(&marker_version)).await;
        assert_eq!(
            marker_remove.status(),
            StatusCode::NO_CONTENT,
            "exact signed delete of marker"
        );

        let current_get = signed_get_object(&endpoint, KEY, None).await;
        assert_eq!(
            current_get.status(),
            StatusCode::OK,
            "current signed GET after marker removal"
        );
        assert!(
            current_get
                .headers()
                .get("x-amz-version-id")
                .is_some_and(|header| header == version_two.as_str()),
            "current signed GET must select PUT2"
        );
        assert!(
            current_get
                .bytes()
                .await
                .expect("current signed GET body")
                .as_ref()
                == b"version two",
            "current signed GET body"
        );

        let suspended = signed_put_bucket_versioning(&endpoint, "Suspended").await;
        assert_eq!(
            suspended.status(),
            StatusCode::OK,
            "suspend versioning before null replacement"
        );
        for (operation, body) in [
            ("suspended signed null PUT3", b"version three".as_slice()),
            ("suspended signed null PUT2", b"version two".as_slice()),
        ] {
            let put = signed_put_object(&endpoint, KEY, body).await;
            assert_eq!(put.status(), StatusCode::OK, "{operation}");
            assert!(
                response_version_id(&put, operation) == "null",
                "{operation}: expected literal null version ID"
            );
        }

        let list = signed_list_object_versions(&endpoint, KEY).await;
        assert_eq!(
            list.status(),
            StatusCode::OK,
            "signed ListObjectVersions after null replacement"
        );
        let xml = list
            .text()
            .await
            .expect("signed ListObjectVersions response body");
        assert_eq!(
            xml.matches("<Version>").count(),
            3,
            "ListObjectVersions Version entry count"
        );
        assert_eq!(
            xml.matches("<DeleteMarker>").count(),
            0,
            "ListObjectVersions delete-marker entry count"
        );
        assert_eq!(
            xml.matches("<VersionId>null</VersionId>").count(),
            1,
            "ListObjectVersions null-version entry count"
        );
        assert_eq!(
            xml.matches(&format!("<VersionId>{version_one}</VersionId>"))
                .count(),
            1,
            "ListObjectVersions PUT1 entry count"
        );
        assert_eq!(
            xml.matches(&format!("<VersionId>{version_two}</VersionId>"))
                .count(),
            1,
            "ListObjectVersions PUT2 entry count"
        );
        assert_eq!(
            xml.split("<Version>")
                .skip(1)
                .filter_map(|entry| entry.split_once("</Version>").map(|(entry, _)| entry))
                .filter(|entry| {
                    entry.contains("<IsLatest>true</IsLatest>")
                        && entry.contains("<VersionId>null</VersionId>")
                })
                .count(),
            1,
            "ListObjectVersions latest entry must be null"
        );

        let rows = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq("versioning-bucket"))
            .filter(object_version::Column::Key.eq(KEY))
            .order_by_asc(object_version::Column::Sequence)
            .all(&db)
            .await
            .expect("query PostgreSQL object_versions");
        assert_eq!(rows.len(), 3, "PostgreSQL object_versions row count");
        let version_one_row = rows
            .iter()
            .find(|row| row.version_id.as_deref() == Some(version_one.as_str()))
            .expect("PostgreSQL PUT1 version row");
        let version_two_row = rows
            .iter()
            .find(|row| row.version_id.as_deref() == Some(version_two.as_str()))
            .expect("PostgreSQL PUT2 version row");
        let null_row = rows
            .iter()
            .find(|row| row.version_id.is_none())
            .expect("PostgreSQL null version row");
        assert!(
            version_one_row.kind == "object"
                && version_one_row.sequence == 1
                && !version_one_row.is_latest,
            "PostgreSQL PUT1 version linkage"
        );
        assert!(
            version_two_row.kind == "object"
                && version_two_row.sequence == 2
                && !version_two_row.is_latest,
            "PostgreSQL PUT2 version linkage"
        );
        assert!(
            null_row.kind == "object" && null_row.sequence == 4 && null_row.is_latest,
            "PostgreSQL latest null version linkage"
        );
        let object_one_id = version_one_row
            .object_id
            .as_deref()
            .expect("PostgreSQL PUT1 object linkage");
        let object_two_id = version_two_row
            .object_id
            .as_deref()
            .expect("PostgreSQL PUT2 object linkage");
        let object_four_id = null_row
            .object_id
            .as_deref()
            .expect("PostgreSQL latest null object linkage");
        assert!(
            object_one_id != object_two_id
                && object_one_id != object_four_id
                && object_two_id != object_four_id,
            "PostgreSQL version rows must link distinct immutable objects"
        );

        let objects = object::Entity::find()
            .filter(object::Column::Bucket.eq("versioning-bucket"))
            .filter(object::Column::Key.eq(KEY))
            .all(&db)
            .await
            .expect("query PostgreSQL immutable object rows");
        assert_eq!(objects.len(), 4, "PostgreSQL immutable object row count");
        assert_eq!(
            objects
                .iter()
                .filter(|object| object.cid == CID_ONE)
                .count(),
            1,
            "PostgreSQL PUT1 CID count"
        );
        assert_eq!(
            objects
                .iter()
                .filter(|object| object.cid == CID_TWO)
                .count(),
            2,
            "PostgreSQL shared PUT2 CID count"
        );
        assert_eq!(
            objects
                .iter()
                .filter(|object| object.cid == CID_THREE)
                .count(),
            1,
            "PostgreSQL suspended PUT3 CID count"
        );
        let object_one = objects
            .iter()
            .find(|object| object.id == object_one_id)
            .expect("retained internal object O1");
        let object_two = objects
            .iter()
            .find(|object| object.id == object_two_id)
            .expect("retained internal object O2");
        let object_four = objects
            .iter()
            .find(|object| object.id == object_four_id)
            .expect("retained internal object O4");
        let object_three = objects
            .iter()
            .find(|object| object.cid == CID_THREE)
            .expect("retained internal object O3");
        assert!(
            object_one.cid == CID_ONE
                && object_two.cid == CID_TWO
                && object_four.cid == CID_TWO
                && object_three.cid == CID_THREE,
            "PostgreSQL immutable object CIDs"
        );
        assert!(
            object_two.id != object_four.id && object_two.cid == object_four.cid,
            "O2 and O4 must be distinct rows with the same CID"
        );
        assert!(
            !object_one.is_latest
                && !object_two.is_latest
                && !object_three.is_latest
                && object_four.is_latest,
            "only O4 must be the current immutable object"
        );
        let referenced_object_ids = rows
            .iter()
            .filter_map(|row| row.object_id.as_deref())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            referenced_object_ids.len(),
            3,
            "PostgreSQL indexed object linkage count"
        );
        assert!(
            referenced_object_ids.contains(object_one_id)
                && referenced_object_ids.contains(object_two_id)
                && referenced_object_ids.contains(object_four_id)
                && !referenced_object_ids.contains(object_three.id.as_str()),
            "PostgreSQL object-version linkages"
        );

        let scan = store::object_version::scan_versions(
            test_state.store.db(),
            "versioning-bucket",
            KEY,
            None,
            10,
        )
        .await
        .expect("production version scan");
        assert_eq!(scan.len(), 3, "production version scan count");
        assert!(
            scan[0].public_version_id == "null"
                && scan[0].is_latest
                && scan[0]
                    .object
                    .as_ref()
                    .is_some_and(|object| object.id == object_four_id),
            "production scan latest null linkage"
        );
        assert!(
            scan[1].public_version_id == version_two
                && !scan[1].is_latest
                && scan[1]
                    .object
                    .as_ref()
                    .is_some_and(|object| object.id == object_two_id),
            "production scan PUT2 linkage"
        );
        assert!(
            scan[2].public_version_id == version_one
                && !scan[2].is_latest
                && scan[2]
                    .object
                    .as_ref()
                    .is_some_and(|object| object.id == object_one_id),
            "production scan PUT1 linkage"
        );
    })
    .await;

    server.shutdown().await;
    drop(state);
    drop(kubo);
    fixture.cleanup().await;
    result.expect("PostgreSQL HTTP marker restore regression task");
}
