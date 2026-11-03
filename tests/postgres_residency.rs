use std::time::Duration;

use ipfs_s3_gateway::store::{self, entities::object_version, migrations::*};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, EntityTrait, QueryOrder,
    Statement, TransactionTrait,
};
use sea_orm_migration::MigrationTrait;

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
            .name("postgres-residency-schema-cleanup".to_owned())
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
    schema.strip_prefix("residency_").is_some_and(|suffix| {
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

async fn connection(url: &str, schema: Option<&str>) -> DatabaseConnection {
    let mut options = ConnectOptions::new(url.to_owned());
    options.max_connections(1).min_connections(1);
    let db = Database::connect(options).await.unwrap();
    if let Some(schema) = schema {
        db.execute_unprepared(&format!("SET search_path TO {schema}"))
            .await
            .unwrap();
    }
    db
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; run explicitly with --ignored"]
async fn postgres_down_waits_for_verification_then_refuses_without_losing_data() {
    use ipfs_s3_gateway::residency::{PhysicalVerification, VersionResidencyIdentity};
    use sea_orm::{DatabaseBackend, QuerySelect};
    use sea_orm_migration::SchemaManager;

    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("IPFS_S3_TEST_POSTGRES_URL must be set for ignored PostgreSQL tests");
    let schema = format!("residency_{}", uuid::Uuid::new_v4().simple());
    let mut cleanup = SchemaCleanup::new(url.clone(), schema.clone());
    let db = connection(&url, None).await;
    db.execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    db.execute_unprepared(&format!("SET search_path TO {schema}"))
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
         VALUES ('object', 'bucket', 'key', 'QmDownRace', 7, 'QmDownRace', TRUE, CURRENT_TIMESTAMP)",
    ).await.unwrap();
    db.execute_unprepared(
        "INSERT INTO object_versions \
         (id, bucket, key, kind, object_id, sequence, is_latest, lifecycle_age_started_at, \
          created_at, updated_at) VALUES \
         ('version', 'bucket', 'key', 'object', 'object', 1, TRUE, CURRENT_TIMESTAMP, \
          CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .await
    .unwrap();
    let identity = VersionResidencyIdentity::new("version", "object", "QmDownRace");
    let txn = db.begin().await.unwrap();
    store::residency::attach_hot_in_transaction(&txn, &identity, &PhysicalVerification::Pending)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let down_db = connection(&url, Some(&schema)).await;
    let down_pid: i32 = down_db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT pg_backend_pid() AS pid",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "pid")
        .unwrap();
    // Keep verification uncommitted while down starts. The old implementation
    // checked pending state, blocked only at DROP, then lost this receipt.
    let writer = db.begin().await.unwrap();
    store::residency::mark_hot_verified_in_transaction(
        &writer,
        &identity,
        "hot-node",
        "verified-before-down",
    )
    .await
    .unwrap();
    let verified = store::residency::resolve_version_residency(&writer, "version")
        .await
        .unwrap();
    let mut down = tokio::spawn(async move {
        let result = m20260912_000001_residency_references::Migration
            .down(&SchemaManager::new(&down_db))
            .await;
        down_db.close().await.unwrap();
        result
    });
    let blocked_query = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            writer
                .query_one(Statement::from_string(
                    DatabaseBackend::Postgres,
                    "SELECT pg_stat_clear_snapshot()",
                ))
                .await
                .unwrap();
            let row = writer
                .query_one(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "SELECT query FROM pg_stat_activity WHERE pid = $1 \
                 AND pg_backend_pid() = ANY(pg_blocking_pids(pid))",
                    [down_pid.into()],
                ))
                .await
                .unwrap();
            if let Some(row) = row {
                break row.try_get::<String>("", "query").unwrap();
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    writer.commit().await.unwrap();
    let result = match tokio::time::timeout(Duration::from_secs(10), &mut down).await {
        Ok(result) => result.unwrap(),
        Err(_) => {
            down.abort();
            let _ = down.await;
            panic!("down must finish after the writer commits");
        }
    };
    let blocked_query = blocked_query.expect("down must block on the existing verification writer");
    assert!(
        result.is_err(),
        "down must reject newly committed verification"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("non-reconstructable")
    );
    assert!(
        blocked_query.starts_with("LOCK TABLE"),
        "down must wait before the state check, not at DROP"
    );
    assert_eq!(
        store::residency::resolve_version_residency(&db, "version")
            .await
            .unwrap(),
        verified
    );
    // Every table remains available after the refused down transaction rolls back.
    assert_eq!(
        store::entities::residency_reference::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        store::entities::residency_backfill::Entity::find()
            .limit(1)
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(object_version_snapshot(&db).await.len(), 1);
    db.execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    cleanup.disarm();
    db.close().await.unwrap();
}

async fn object_version_snapshot(db: &DatabaseConnection) -> Vec<object_version::Model> {
    object_version::Entity::find()
        .order_by_asc(object_version::Column::Id)
        .all(db)
        .await
        .unwrap()
}
