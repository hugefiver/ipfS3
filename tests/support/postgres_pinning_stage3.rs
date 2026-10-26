use std::{future::Future, panic::AssertUnwindSafe};

use crate::store;
use futures_util::FutureExt;
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
};

pub(crate) async fn isolated<F, Fut>(migrate: bool, body: F)
where
    F: FnOnce(DatabaseConnection) -> Fut,
    Fut: Future<Output = ()>,
{
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("set IPFS_S3_TEST_POSTGRES_URL to an authorized, isolated PostgreSQL 17 endpoint");
    let admin = Database::connect(&url)
        .await
        .expect("test PostgreSQL must be reachable");
    let version = admin
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SHOW server_version",
        ))
        .await
        .unwrap()
        .unwrap();
    let version: String = version.try_get("", "server_version").unwrap();
    assert!(
        version.starts_with("17."),
        "requires PostgreSQL 17, got {version}"
    );

    let schema = format!("stage3_evidence_{}", uuid::Uuid::new_v4().simple());
    assert!(
        schema.starts_with("stage3_evidence_")
            && schema
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_')
    );
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let result = AssertUnwindSafe(async {
        let mut options = ConnectOptions::new(url);
        options.min_connections(1).max_connections(1);
        options.set_schema_search_path(schema.clone());
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared("SET statement_timeout TO '15s'")
            .await
            .unwrap();
        if migrate {
            store::run_migrations(&db).await.unwrap();
        }
        body(db.clone()).await;
        db.close().await.unwrap();
    })
    .catch_unwind()
    .await;
    // Only this test's generated schema is removed, including on assertion failure.
    admin
        .execute_unprepared("SET statement_timeout TO '15s'")
        .await
        .unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
