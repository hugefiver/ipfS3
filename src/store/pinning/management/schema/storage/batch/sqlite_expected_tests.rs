use super::*;
use sea_orm::{ConnectOptions, Database, DatabaseConnection, TransactionTrait};

async fn fixture() -> (DatabaseConnection, SqliteCatalog) {
    let mut options = ConnectOptions::new("sqlite::memory:");
    options.max_connections(1);
    let db = Database::connect(options).await.unwrap();
    db.execute_unprepared(
        "CREATE TABLE pin_management_readiness (id INTEGER,format TEXT,state TEXT);\
         INSERT INTO pin_management_readiness VALUES (1,'S75A1','ready')",
    )
    .await
    .unwrap();
    let mut objects = BTreeMap::new();
    for (name, kind, ddl) in [
        ("sample", "table", "CREATE TABLE sample (value TEXT UNIQUE)"),
        (
            "sample_index",
            "index",
            "CREATE INDEX sample_index ON sample(value)",
        ),
        (
            "sample_guard",
            "trigger",
            "CREATE TRIGGER sample_guard BEFORE INSERT ON sample BEGIN SELECT 1; END",
        ),
    ] {
        db.execute_unprepared(ddl).await.unwrap();
        objects.insert(
            name.into(),
            SqliteObject::new(kind, "sample", ddl.into(), false),
        );
    }
    let cap = objects.len() + 1;
    let query = catalog_query(&objects, &["sample"], &["sample"], cap);
    (
        db,
        SqliteCatalog {
            objects,
            query,
            cap,
        },
    )
}

async fn validate<C: ConnectionTrait>(db: &C, catalog: &SqliteCatalog) -> Result<()> {
    let rows = db
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            catalog.query.clone(),
            ["ready".into()],
        ))
        .await
        .map_err(|_| ManagementError::Schema)?;
    super::super::sqlite_rows(&rows, catalog)
}

#[tokio::test]
async fn sqlite_catalog_rowid_reads_preserve_identity_and_normalization() {
    let (db, catalog) = fixture().await;
    // The UNIQUE column supplies a legitimate implicit index with no SQL text.
    validate(&db, &catalog).await.unwrap();
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared(
        "DROP INDEX sample_index; CREATE INDEX sample_index on sample(\nvalue\n)",
    )
    .await
    .unwrap();
    validate(&tx, &catalog).await.unwrap();
    tx.rollback().await.unwrap();

    for ddl in [
        "DROP INDEX sample_index; CREATE INDEX sample_index ON sample(value DESC)",
        "DROP INDEX sample_index; CREATE TABLE elsewhere(value TEXT); CREATE INDEX sample_index ON elsewhere(value)",
        "DROP TRIGGER sample_guard; CREATE INDEX sample_guard ON sample(value)",
    ] {
        let tx = db.begin().await.unwrap();
        tx.execute_unprepared(ddl).await.unwrap();
        assert!(validate(&tx, &catalog).await.is_err(), "{ddl}");
        tx.rollback().await.unwrap();
    }
    validate(&db, &catalog).await.unwrap();
}

#[tokio::test]
async fn sqlite_catalog_rejects_duplicate_exact_definitions() {
    let (db, catalog) = fixture().await;
    validate(&db, &catalog).await.unwrap();
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared(
        "PRAGMA writable_schema=ON;\
         INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql) \
         SELECT type,name,tbl_name,rootpage,sql FROM sqlite_schema WHERE name='sample_index';\
         PRAGMA writable_schema=OFF",
    )
    .await
    .unwrap();
    assert!(validate(&tx, &catalog).await.is_err());
    tx.rollback().await.unwrap();
    validate(&db, &catalog).await.unwrap();
}
