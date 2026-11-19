use ipfs_s3_gateway::{config::Config, state::AppState, store};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};

#[path = "support/baseline_postgres.rs"]
mod postgres;

fn configuration(url: &str) -> Config {
    let mut config: Config = toml::from_str("").unwrap();
    config.storage.database_url = url.to_owned();
    config.crypto.master_key = "7".repeat(64);
    config.kubo.rpc_url = "http://127.0.0.1:1".into();
    config
}

async fn concurrent_open(url: &str) {
    let config = configuration(url);
    let (first, second) = tokio::join!(AppState::new(&config), AppState::new(&config));
    let first = first.unwrap();
    let second = second.unwrap();
    store::bucket::create(first.store.db(), "shared-initialization", None)
        .await
        .unwrap();
    assert!(
        store::bucket::exists(second.store.db(), "shared-initialization")
            .await
            .unwrap()
    );
    second.store.db().clone().close().await.unwrap();
    first.store.db().clone().close().await.unwrap();
}

async fn partial_schema_refusal(url: &str) {
    let db = store::connect_database(url).await.unwrap();
    db.execute_unprepared("CREATE TABLE objects(id TEXT PRIMARY KEY, marker TEXT NOT NULL); INSERT INTO objects VALUES ('existing-object','preserve-existing-content')").await.unwrap();
    assert!(AppState::new(&configuration(url)).await.is_err());
    let sql = match db.get_database_backend() {
        DatabaseBackend::Sqlite => "SELECT name FROM sqlite_schema WHERE type='table'",
        DatabaseBackend::Postgres => {
            "SELECT table_name AS name FROM information_schema.tables WHERE table_schema=current_schema()"
        }
        _ => unreachable!(),
    };
    let tables = db
        .query_all(Statement::from_string(db.get_database_backend(), sql))
        .await
        .unwrap();
    assert_eq!(tables.len(), 1);
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

fn sqlite_url(dir: &tempfile::TempDir) -> String {
    format!(
        "sqlite://{}?mode=rwc",
        dir.path()
            .join("db.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    )
}

async fn schema_url(db: &DatabaseConnection) -> url::Url {
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT current_schema() AS name",
        ))
        .await
        .unwrap()
        .unwrap();
    let schema: String = row.try_get("", "name").unwrap();
    let mut url = url::Url::parse(&std::env::var("IPFS_S3_TEST_POSTGRES_URL").unwrap()).unwrap();
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    url
}

#[tokio::test]
async fn sqlite_concurrent_appstate_initializes_once() {
    let directory = tempfile::tempdir().unwrap();
    concurrent_open(&sqlite_url(&directory)).await;
}

#[tokio::test]
async fn sqlite_appstate_preserves_unknown_partial_schema() {
    let directory = tempfile::tempdir().unwrap();
    partial_schema_refusal(&sqlite_url(&directory)).await;
}

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_concurrent_appstate_initializes_once() {
    postgres::with_schema(
        |db| async move { concurrent_open(schema_url(&db).await.as_str()).await },
    )
    .await;
}

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_appstate_preserves_unknown_partial_schema() {
    postgres::with_schema(|db| async move {
        partial_schema_refusal(schema_url(&db).await.as_str()).await
    })
    .await;
}
