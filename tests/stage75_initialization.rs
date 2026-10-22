use ipfs_s3_gateway::{
    config::Config,
    state::AppState,
    store::{self, initialization::SchemaState, pinning::management::native},
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};

fn configuration(url: &str) -> Config {
    let mut config: Config = toml::from_str("").unwrap();
    config.storage.database_url = url.to_owned();
    config.crypto.master_key = "7".repeat(64);
    config.kubo.rpc_url = "http://127.0.0.1:1".into();
    config
}

async fn exclusive_postgres_fixture(url: &str) -> DatabaseConnection {
    let mut options = sea_orm::ConnectOptions::new(url);
    options
        .min_connections(1)
        .max_connections(1)
        .sqlx_logging(false);
    let db = sea_orm::Database::connect(options).await.unwrap();
    let row = db.query_one(Statement::from_string(db.get_database_backend(),
        "SELECT current_schema() AS name, NOT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace) AND NOT EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace=current_schema()::regnamespace) AS empty, pg_try_advisory_lock(hashtextextended(current_database() || ':' || current_schema(),0)) AS claimed"))
        .await.unwrap().unwrap();
    assert!(
        row.try_get::<String>("", "name")
            .unwrap()
            .starts_with("stage7_")
    );
    assert!(row.try_get::<bool>("", "empty").unwrap());
    assert!(row.try_get::<bool>("", "claimed").unwrap());
    db
}

async fn table_names(db: &DatabaseConnection) -> Vec<String> {
    let sql = match db.get_database_backend() {
        DatabaseBackend::Sqlite => {
            "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name"
        }
        DatabaseBackend::Postgres => {
            "SELECT table_name AS name FROM information_schema.tables WHERE table_schema=current_schema() ORDER BY table_name"
        }
        _ => unreachable!(),
    };
    db.query_all(Statement::from_string(db.get_database_backend(), sql))
        .await
        .unwrap()
        .iter()
        .map(|row| row.try_get("", "name").unwrap())
        .collect()
}

async fn concurrent_initialization_and_backfill_refusal(url: &str) {
    let config = configuration(url);
    let (first, second) = tokio::join!(AppState::new(&config), AppState::new(&config));
    let first = first.unwrap();
    let second = second.unwrap();
    let db = first.store.db();
    assert_eq!(
        store::initialization::classify(db).await.unwrap(),
        SchemaState::Current
    );
    let binding = native::scope::binding(db).await.unwrap();
    assert_eq!(
        native::scope::binding(second.store.db()).await.unwrap(),
        binding
    );
    db.execute_unprepared("UPDATE pin_management_readiness SET state='backfill'")
        .await
        .unwrap();
    let tables = table_names(db).await;
    assert!(AppState::new(&config).await.is_err());
    assert_eq!(table_names(db).await, tables);
    assert_eq!(native::scope::binding(db).await.unwrap(), binding);
    let row = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT state FROM pin_management_readiness",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<String>("", "state").unwrap(), "backfill");
    second.store.db().clone().close().await.unwrap();
    first.store.db().clone().close().await.unwrap();
}

async fn partial_schema_refusal(url: &str) {
    let db = store::connect_database(url).await.unwrap();
    db.execute_unprepared("CREATE TABLE objects(id TEXT PRIMARY KEY, marker TEXT NOT NULL)")
        .await
        .unwrap();
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO objects(id,marker) VALUES($1,$2)",
        ["existing-object".into(), "preserve-existing-content".into()],
    ))
    .await
    .unwrap();
    let tables = table_names(&db).await;
    assert_eq!(
        store::initialization::classify(&db).await.unwrap(),
        SchemaState::UpgradeRequired
    );
    assert!(AppState::new(&configuration(url)).await.is_err());
    assert_eq!(table_names(&db).await, tables);
    let row = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT marker FROM objects WHERE id='existing-object'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.try_get::<String>("", "marker").unwrap(),
        "preserve-existing-content"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_concurrent_appstate_initializes_once_and_refuses_backfill() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("empty.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    concurrent_initialization_and_backfill_refusal(&url).await;
}

#[tokio::test]
async fn sqlite_appstate_does_not_initialize_partial_schema_without_migration_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("partial.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    partial_schema_refusal(&url).await;
}

#[tokio::test]
#[ignore = "requires parent-selected empty exclusive Stage7 PostgreSQL schema"]
async fn postgres_concurrent_appstate_initializes_once_and_refuses_backfill() {
    let url = std::env::var("IPFS_S3_STAGE75_HISTORY_DATABASE_URL").unwrap();
    let guard = exclusive_postgres_fixture(&url).await;
    concurrent_initialization_and_backfill_refusal(&url).await;
    guard.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires parent-selected empty exclusive Stage7 PostgreSQL schema"]
async fn postgres_appstate_does_not_initialize_partial_schema_without_migration_metadata() {
    let url = std::env::var("IPFS_S3_STAGE75_HISTORY_DATABASE_URL").unwrap();
    let guard = exclusive_postgres_fixture(&url).await;
    partial_schema_refusal(&url).await;
    guard.close().await.unwrap();
}
