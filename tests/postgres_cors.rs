use std::{sync::Arc, time::Duration};

use ipfs_s3_gateway::store::{
    self,
    cors_config::{delete_configuration, get_optional_configuration, put_configuration},
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
};
use tokio::sync::Barrier;

const BUCKET: &str = "cors-bucket";
const FIRST: &str = r#"{"rules":[{"allowed_origins":["https://first.example"],"allowed_methods":["GET"],"allowed_headers":[],"expose_headers":[],"id":"first","max_age_seconds":null}]}"#;
const SECOND: &str = r#"{"rules":[{"allowed_origins":["https://second.example"],"allowed_methods":["PUT"],"allowed_headers":["x-second-*"],"expose_headers":["x-second-response"],"id":"second","max_age_seconds":30}]}"#;
const RACE_TIMEOUT: Duration = Duration::from_secs(10);

struct OwnedPgSchemaCleanup {
    url: String,
    schema: Option<String>,
}

impl OwnedPgSchemaCleanup {
    fn new(url: String, schema: String) -> Self {
        assert!(
            is_owned_cors_schema(&schema),
            "CORS test cleanup must own its schema"
        );
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
        if !is_owned_cors_schema(&schema) {
            return;
        }
        let url = self.url.clone();
        if let Ok(thread) = std::thread::Builder::new()
            .name("postgres-cors-schema-cleanup".to_owned())
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

struct PgFixture {
    db: DatabaseConnection,
    schema: String,
    url: String,
    cleanup_guard: OwnedPgSchemaCleanup,
}

impl PgFixture {
    async fn independent_connection(&self) -> DatabaseConnection {
        let mut options = ConnectOptions::new(self.url.clone());
        options.max_connections(1).min_connections(1);
        let db = Database::connect(options)
            .await
            .expect("independent PostgreSQL connection must open");
        db.execute_unprepared(&format!("SET search_path TO {}", self.schema))
            .await
            .expect("independent PostgreSQL connection must use its CORS schema");
        db
    }

    async fn cleanup(mut self) {
        assert!(
            is_owned_cors_schema(&self.schema),
            "CORS test cleanup must only drop its owned schema"
        );
        self.db
            .execute_unprepared(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .expect("CORS PostgreSQL schema must drop during explicit cleanup");
        self.cleanup_guard.disarm();
        self.db
            .close()
            .await
            .expect("CORS PostgreSQL fixture connection must close");
    }
}

fn is_owned_cors_schema(schema: &str) -> bool {
    schema.strip_prefix("cors_").is_some_and(|suffix| {
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

async fn cors_fixture() -> Option<PgFixture> {
    let Ok(url) = std::env::var("IPFS_S3_TEST_POSTGRES_URL") else {
        eprintln!("skipping PostgreSQL CORS tests: IPFS_S3_TEST_POSTGRES_URL is unset");
        return None;
    };
    let mut options = ConnectOptions::new(url.clone());
    options.max_connections(1).min_connections(1);
    let db = Database::connect(options)
        .await
        .expect("PostgreSQL CORS fixture connection must open");
    let schema = format!("cors_{}", uuid::Uuid::new_v4().simple());
    let cleanup_guard = OwnedPgSchemaCleanup::new(url.clone(), schema.clone());
    db.execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("PostgreSQL CORS fixture schema must create");
    db.execute_unprepared(&format!("SET search_path TO {schema}"))
        .await
        .expect("PostgreSQL CORS fixture connection must use its schema");
    assert_supported_postgres_version(&db).await;
    store::run_migrations(&db)
        .await
        .expect("all PostgreSQL migrations must apply to the CORS fixture");
    store::bucket::create(&db, BUCKET, Some("owner"))
        .await
        .expect("CORS fixture bucket must seed");
    Some(PgFixture {
        db,
        schema,
        url,
        cleanup_guard,
    })
}

async fn assert_supported_postgres_version(db: &DatabaseConnection) {
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SHOW server_version_num",
        ))
        .await
        .expect("PostgreSQL server version query must succeed")
        .expect("PostgreSQL server version query must return a row");
    let version: String = row
        .try_get("", "server_version_num")
        .expect("PostgreSQL server version must be text");
    let version: i32 = version
        .parse()
        .expect("PostgreSQL server version must be numeric");
    assert!(
        (170000..180000).contains(&version),
        "PostgreSQL CORS tests require PostgreSQL 17"
    );
}

async fn cors_columns(db: &DatabaseConnection) -> Vec<(String, String, String)> {
    db.query_all(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT column_name, data_type, is_nullable \
         FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = $1 \
         ORDER BY ordinal_position",
        ["bucket_cors_configs".to_owned().into()],
    ))
    .await
    .expect("CORS column metadata query must succeed")
    .into_iter()
    .map(|row| {
        (
            row.try_get("", "column_name")
                .expect("CORS column name must be present"),
            row.try_get("", "data_type")
                .expect("CORS column type must be present"),
            row.try_get("", "is_nullable")
                .expect("CORS column nullability must be present"),
        )
    })
    .collect()
}

async fn cors_primary_key_columns(db: &DatabaseConnection) -> Vec<String> {
    db.query_all(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT kcu.column_name \
         FROM information_schema.table_constraints AS tc \
         JOIN information_schema.key_column_usage AS kcu \
           ON tc.constraint_name = kcu.constraint_name \
          AND tc.constraint_schema = kcu.constraint_schema \
          AND tc.table_schema = kcu.table_schema \
         WHERE tc.table_schema = current_schema() \
           AND tc.table_name = $1 \
           AND tc.constraint_type = 'PRIMARY KEY' \
         ORDER BY kcu.ordinal_position",
        ["bucket_cors_configs".to_owned().into()],
    ))
    .await
    .expect("CORS primary-key metadata query must succeed")
    .into_iter()
    .map(|row| {
        row.try_get("", "column_name")
            .expect("CORS primary-key column must be present")
    })
    .collect()
}

async fn cors_bucket_foreign_keys(
    db: &DatabaseConnection,
) -> Vec<(String, String, String, String)> {
    db.query_all(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT kcu.column_name AS source_column, \
                ccu.table_name AS referenced_table, \
                ccu.column_name AS referenced_column, \
                rc.delete_rule \
         FROM information_schema.table_constraints AS tc \
         JOIN information_schema.key_column_usage AS kcu \
           ON tc.constraint_name = kcu.constraint_name \
          AND tc.constraint_schema = kcu.constraint_schema \
          AND tc.table_schema = kcu.table_schema \
         JOIN information_schema.referential_constraints AS rc \
           ON tc.constraint_name = rc.constraint_name \
          AND tc.constraint_schema = rc.constraint_schema \
         JOIN information_schema.constraint_column_usage AS ccu \
           ON rc.unique_constraint_name = ccu.constraint_name \
          AND rc.unique_constraint_schema = ccu.constraint_schema \
         WHERE tc.table_schema = current_schema() \
           AND tc.table_name = $1 \
           AND tc.constraint_type = 'FOREIGN KEY'",
        ["bucket_cors_configs".to_owned().into()],
    ))
    .await
    .expect("CORS foreign-key metadata query must succeed")
    .into_iter()
    .map(|row| {
        (
            row.try_get("", "source_column")
                .expect("CORS foreign-key source column must be present"),
            row.try_get("", "referenced_table")
                .expect("CORS foreign-key table must be present"),
            row.try_get("", "referenced_column")
                .expect("CORS foreign-key target column must be present"),
            row.try_get("", "delete_rule")
                .expect("CORS foreign-key delete rule must be present"),
        )
    })
    .collect()
}

async fn observe_during_race(db: DatabaseConnection, start: Arc<Barrier>) -> Vec<Option<String>> {
    start.wait().await;
    let mut snapshots = Vec::new();
    for _ in 0..16 {
        snapshots.push(
            get_optional_configuration(&db, BUCKET)
                .await
                .expect("CORS race observation must succeed"),
        );
        tokio::task::yield_now().await;
    }
    snapshots
}

fn assert_complete_snapshot(snapshot: Option<String>) {
    let Some(snapshot) = snapshot else {
        return;
    };
    assert!(
        serde_json::from_str::<serde_json::Value>(&snapshot).is_ok(),
        "a present CORS race observation must be valid JSON"
    );
    assert!(
        snapshot == FIRST || snapshot == SECOND,
        "a CORS race observation must be one complete expected configuration"
    );
}

#[tokio::test]
async fn postgres_cors_migration_schema_timestamps_and_cascade_are_correct() {
    let Some(fixture) = cors_fixture().await else {
        return;
    };

    assert_eq!(
        cors_columns(&fixture.db).await,
        vec![
            ("bucket".to_owned(), "text".to_owned(), "NO".to_owned()),
            (
                "canonical_json".to_owned(),
                "text".to_owned(),
                "NO".to_owned()
            ),
            (
                "created_at".to_owned(),
                "timestamp with time zone".to_owned(),
                "NO".to_owned(),
            ),
            (
                "updated_at".to_owned(),
                "timestamp with time zone".to_owned(),
                "NO".to_owned(),
            ),
        ],
        "CORS table must have exactly the required columns and types"
    );
    assert_eq!(
        cors_primary_key_columns(&fixture.db).await,
        vec!["bucket".to_owned()],
        "CORS table must use bucket as its only primary-key column"
    );
    assert_eq!(
        cors_bucket_foreign_keys(&fixture.db).await,
        vec![(
            "bucket".to_owned(),
            "buckets".to_owned(),
            "name".to_owned(),
            "CASCADE".to_owned(),
        )],
        "CORS table must cascade from buckets(name)"
    );

    put_configuration(&fixture.db, BUCKET, FIRST)
        .await
        .expect("CORS configuration must store");
    let row = fixture
        .db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT created_at, updated_at FROM bucket_cors_configs WHERE bucket = $1",
            [BUCKET.to_owned().into()],
        ))
        .await
        .expect("CORS timestamp query must succeed")
        .expect("stored CORS configuration must have timestamps");
    let created_at: chrono::DateTime<chrono::Utc> = row
        .try_get("", "created_at")
        .expect("CORS created timestamp must be readable");
    let updated_at: chrono::DateTime<chrono::Utc> = row
        .try_get("", "updated_at")
        .expect("CORS updated timestamp must be readable");
    assert!(
        updated_at >= created_at,
        "CORS updated timestamp must not precede creation"
    );

    fixture
        .db
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "DELETE FROM buckets WHERE name = $1",
            [BUCKET.to_owned().into()],
        ))
        .await
        .expect("CORS fixture bucket deletion must succeed");
    assert!(
        get_optional_configuration(&fixture.db, BUCKET)
            .await
            .expect("CORS configuration read after cascade must succeed")
            .is_none(),
        "bucket deletion must cascade to the CORS configuration"
    );

    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_cors_put_put_race_exposes_only_complete_snapshots() {
    let Some(fixture) = cors_fixture().await else {
        return;
    };
    let first_db = fixture.independent_connection().await;
    let second_db = fixture.independent_connection().await;
    let reader_db = fixture.independent_connection().await;
    let start = Arc::new(Barrier::new(3));

    let first_start = start.clone();
    let first = tokio::spawn(async move {
        first_start.wait().await;
        put_configuration(&first_db, BUCKET, FIRST).await
    });
    let second_start = start.clone();
    let second = tokio::spawn(async move {
        second_start.wait().await;
        put_configuration(&second_db, BUCKET, SECOND).await
    });
    let reader = tokio::spawn(observe_during_race(reader_db, start));

    let (first, second, mut snapshots) = tokio::time::timeout(RACE_TIMEOUT, async {
        (
            first.await.expect("first PUT task must join"),
            second.await.expect("second PUT task must join"),
            reader.await.expect("PUT/PUT reader task must join"),
        )
    })
    .await
    .expect("PUT/PUT race must complete within the bounded timeout");
    first.expect("first PUT must succeed");
    second.expect("second PUT must succeed");
    snapshots.push(
        get_optional_configuration(&fixture.db, BUCKET)
            .await
            .expect("PUT/PUT final CORS read must succeed"),
    );
    for snapshot in snapshots {
        assert_complete_snapshot(snapshot);
    }
    assert!(
        matches!(
            get_optional_configuration(&fixture.db, BUCKET)
                .await
                .expect("PUT/PUT final CORS read must succeed")
                .as_deref(),
            Some(FIRST) | Some(SECOND)
        ),
        "PUT/PUT race must retain one complete configuration"
    );

    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_cors_put_delete_race_exposes_only_complete_snapshots() {
    let Some(fixture) = cors_fixture().await else {
        return;
    };
    put_configuration(&fixture.db, BUCKET, SECOND)
        .await
        .expect("PUT/DELETE race seed must store");
    let put_db = fixture.independent_connection().await;
    let delete_db = fixture.independent_connection().await;
    let reader_db = fixture.independent_connection().await;
    let start = Arc::new(Barrier::new(3));

    let put_start = start.clone();
    let put = tokio::spawn(async move {
        put_start.wait().await;
        put_configuration(&put_db, BUCKET, FIRST).await
    });
    let delete_start = start.clone();
    let delete = tokio::spawn(async move {
        delete_start.wait().await;
        delete_configuration(&delete_db, BUCKET).await
    });
    let reader = tokio::spawn(observe_during_race(reader_db, start));

    let (put, delete, mut snapshots) = tokio::time::timeout(RACE_TIMEOUT, async {
        (
            put.await.expect("PUT task must join"),
            delete.await.expect("DELETE task must join"),
            reader.await.expect("PUT/DELETE reader task must join"),
        )
    })
    .await
    .expect("PUT/DELETE race must complete within the bounded timeout");
    put.expect("PUT must succeed");
    delete.expect("DELETE must succeed");
    snapshots.push(
        get_optional_configuration(&fixture.db, BUCKET)
            .await
            .expect("PUT/DELETE final CORS read must succeed"),
    );
    for snapshot in snapshots {
        assert_complete_snapshot(snapshot);
    }
    assert!(
        matches!(
            get_optional_configuration(&fixture.db, BUCKET)
                .await
                .expect("PUT/DELETE final CORS read must succeed")
                .as_deref(),
            None | Some(FIRST)
        ),
        "PUT/DELETE race must end with absence or one complete configuration"
    );

    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_cors_delete_put_race_exposes_only_complete_snapshots() {
    let Some(fixture) = cors_fixture().await else {
        return;
    };
    put_configuration(&fixture.db, BUCKET, SECOND)
        .await
        .expect("DELETE/PUT race seed must store");
    let delete_db = fixture.independent_connection().await;
    let put_db = fixture.independent_connection().await;
    let reader_db = fixture.independent_connection().await;
    let start = Arc::new(Barrier::new(3));

    let delete_start = start.clone();
    let delete = tokio::spawn(async move {
        delete_start.wait().await;
        delete_configuration(&delete_db, BUCKET).await
    });
    let put_start = start.clone();
    let put = tokio::spawn(async move {
        put_start.wait().await;
        put_configuration(&put_db, BUCKET, FIRST).await
    });
    let reader = tokio::spawn(observe_during_race(reader_db, start));

    let (delete, put, mut snapshots) = tokio::time::timeout(RACE_TIMEOUT, async {
        (
            delete.await.expect("DELETE task must join"),
            put.await.expect("PUT task must join"),
            reader.await.expect("DELETE/PUT reader task must join"),
        )
    })
    .await
    .expect("DELETE/PUT race must complete within the bounded timeout");
    delete.expect("DELETE must succeed");
    put.expect("PUT must succeed");
    snapshots.push(
        get_optional_configuration(&fixture.db, BUCKET)
            .await
            .expect("DELETE/PUT final CORS read must succeed"),
    );
    for snapshot in snapshots {
        assert_complete_snapshot(snapshot);
    }
    assert!(
        matches!(
            get_optional_configuration(&fixture.db, BUCKET)
                .await
                .expect("DELETE/PUT final CORS read must succeed")
                .as_deref(),
            None | Some(FIRST)
        ),
        "DELETE/PUT race must end with absence or one complete configuration"
    );

    fixture.cleanup().await;
}
