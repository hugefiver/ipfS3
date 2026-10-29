use futures_util::FutureExt;
use sea_orm::{ConnectionTrait, Database, DatabaseConnection};

pub async fn with_schema<F, W>(work: F)
where
    F: FnOnce(DatabaseConnection) -> W,
    W: std::future::Future<Output = ()>,
{
    let url =
        std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect("authorized PostgreSQL fixture URL");
    let admin = Database::connect(&url).await.unwrap();
    let name = format!("baseline_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA \"{name}\""))
        .await
        .unwrap();
    let mut options = sea_orm::ConnectOptions::new(&url);
    options
        .max_connections(4)
        .set_schema_search_path(name.clone());
    let db = Database::connect(options).await.unwrap();
    let outcome = std::panic::AssertUnwindSafe(work(db.clone()))
        .catch_unwind()
        .await;
    db.close().await.unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA \"{name}\" CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
