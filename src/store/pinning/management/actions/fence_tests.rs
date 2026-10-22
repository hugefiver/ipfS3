use sea_orm::{ConnectOptions, ConnectionTrait, Database, DatabaseConnection, TransactionTrait};

async fn connection(url: &str) -> DatabaseConnection {
    let mut options = ConnectOptions::new(url);
    options.max_connections(1).sqlx_logging(false);
    let db = Database::connect(options).await.unwrap();
    db.execute_unprepared("PRAGMA busy_timeout=0")
        .await
        .unwrap();
    db
}

#[tokio::test]
async fn writer_intent_holds_until_transaction_end_without_source_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("writer.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let first = connection(&url).await;
    first
        .execute_unprepared(
            "CREATE TABLE pin_provider_routes(provider TEXT PRIMARY KEY); \
         CREATE TABLE pin_management_records(database_scope TEXT,kind TEXT); \
         INSERT INTO pin_management_records VALUES('identity','database_scope'); \
         CREATE TABLE mutations(n INTEGER); INSERT INTO mutations VALUES(0); \
         CREATE TRIGGER record_mutation AFTER UPDATE ON pin_management_records \
         BEGIN UPDATE mutations SET n=n+1; END; \
         CREATE TRIGGER route_mutation AFTER UPDATE ON pin_provider_routes \
         BEGIN UPDATE mutations SET n=n+1; END;",
        )
        .await
        .unwrap();
    let second = connection(&url).await;
    // Exercise both an empty route table and a populated one, and both ways of
    // ending the caller-owned transaction. The competing write must not leak in.
    for commit in [false, true] {
        let insert = format!(
            "INSERT INTO pin_provider_routes VALUES('competitor-{}')",
            u8::from(commit)
        );
        let tx = first.begin().await.unwrap();
        super::write_intent(&tx).await.unwrap();
        assert!(second.execute_unprepared(&insert).await.is_err());
        if commit {
            tx.commit().await.unwrap();
        } else {
            tx.rollback().await.unwrap();
        }
        second.execute_unprepared(&insert).await.unwrap();
    }
    let row = first
        .query_one(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT n FROM mutations",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
    first.close().await.unwrap();
    second.close().await.unwrap();
}

#[tokio::test]
async fn ordinary_source_discovery_retains_the_caller_writer_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("source.sqlite")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let first = connection(&url).await;
    crate::store::run_migrations(&first).await.unwrap();
    let second = connection(&url).await;
    let now = chrono::Utc::now();
    let job = crate::store::entities::pin_job::Model {
        id: "ordinary-source-discovery".into(),
        operation: "submit".into(),
        provider: "ordinary".into(),
        cid: "cid".into(),
        lease_id: None,
        target_id: None,
        expected_generation: None,
        expected_remote_epoch: None,
        state: "pending".into(),
        attempts: 0,
        next_attempt_at: now,
        locked_until: None,
        submit_phase: None,
        last_error: None,
        created_at: now,
        updated_at: now,
    };
    let tx = first.begin().await.unwrap();
    super::super::job_authority::lock_source_in_transaction_with_work(
        &tx,
        &job,
        super::super::records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    let insert = "INSERT INTO pin_provider_routes VALUES('competing-source','{}','private',0)";
    assert!(second.execute_unprepared(insert).await.is_err());
    tx.rollback().await.unwrap();
    second.execute_unprepared(insert).await.unwrap();
    first.close().await.unwrap();
    second.close().await.unwrap();
}
