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
            control_query: format!("WITH {} {}", shadow_query(&["sample"]), CONTROL),
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
    let (db, mut catalog) = fixture().await;
    let ddl = "CREATE TABLE headroom (value TEXT)";
    db.execute_unprepared(ddl).await.unwrap();
    catalog.objects.insert(
        "headroom".into(),
        SqliteObject::new("table", "headroom", ddl.into(), false),
    );
    // Each scoped table permits an implicit index; headroom needs none itself.
    catalog.cap = catalog.objects.len() + 2;
    catalog.query = catalog_query(
        &catalog.objects,
        &["sample", "headroom"],
        &["sample", "headroom"],
        catalog.cap,
    );
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
    let actual: i64 = tx
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS n FROM sqlite_schema WHERE tbl_name IN ('sample','headroom')",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert!(actual <= catalog.cap as i64);
    assert!(validate(&tx, &catalog).await.is_err());
    tx.rollback().await.unwrap();
    validate(&db, &catalog).await.unwrap();
}

#[tokio::test]
async fn sqlite_catalog_binds_distinct_trigger_definitions_to_names() {
    let (db, mut catalog) = fixture().await;
    for (name, ddl) in [
        (
            "sample_guard",
            "CREATE TRIGGER sample_guard BEFORE INSERT ON sample WHEN NEW.value='first''s value' BEGIN SELECT RAISE(ABORT,'cannot insert'); END",
        ),
        (
            "sample_guard_other",
            "CREATE TRIGGER sample_guard_other BEFORE INSERT ON sample WHEN NEW.value='other''s value' BEGIN SELECT RAISE(ABORT,'cannot insert'); END",
        ),
    ] {
        db.execute_unprepared(&format!("DROP TRIGGER IF EXISTS {name}; {ddl}"))
            .await
            .unwrap();
        catalog.objects.insert(
            name.into(),
            SqliteObject::new("trigger", "sample", ddl.into(), false),
        );
    }
    catalog.cap = catalog.objects.len() + 1;
    catalog.query = catalog_query(&catalog.objects, &["sample"], &["sample"], catalog.cap);
    validate(&db, &catalog).await.unwrap();
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared(
        "DROP TRIGGER sample_guard_other; \
         CREATE TRIGGER sample_guard_other BEFORE INSERT ON sample WHEN NEW.value='first''s value' BEGIN SELECT RAISE(ABORT,'cannot insert'); END",
    )
    .await
    .unwrap();
    assert!(validate(&tx, &catalog).await.is_err());
    tx.rollback().await.unwrap();
    validate(&db, &catalog).await.unwrap();
}
