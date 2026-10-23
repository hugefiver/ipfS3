use super::*;
use sea_orm::{DbErr, ExecResult, QueryResult, TransactionTrait};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Observed<'a, C> {
    inner: &'a C,
    queries: AtomicUsize,
    payload_columns: Vec<String>,
    returned_payload_bytes: AtomicUsize,
}

impl<'a, C> Observed<'a, C> {
    fn new(inner: &'a C, payload_columns: Vec<String>) -> Self {
        Self {
            inner,
            queries: AtomicUsize::new(0),
            payload_columns,
            returned_payload_bytes: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl<C: ConnectionTrait> ConnectionTrait for Observed<'_, C> {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.inner.get_database_backend()
    }

    async fn execute(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        self.inner.execute(statement).await
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        self.inner.execute_unprepared(sql).await
    }

    async fn query_one(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.queries.fetch_add(1, Ordering::Relaxed);
        self.inner.query_one(statement).await
    }

    async fn query_all(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        self.queries.fetch_add(1, Ordering::Relaxed);
        let rows = self.inner.query_all(statement).await?;
        for row in &rows {
            for column in &self.payload_columns {
                if let Ok(Some(value)) = row.try_get::<Option<String>>("", column) {
                    self.returned_payload_bytes
                        .fetch_add(value.len(), Ordering::Relaxed);
                }
            }
        }
        Ok(rows)
    }
}

#[tokio::test]
async fn native_images_batch_catalog_queries_and_recheck_schema_and_rows() {
    let db = crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    let txn = db.begin().await.unwrap();
    let versions = schema(&txn, true).await.unwrap();
    crate::store::pinning::legacy_migration::fence::lock_before_routes(&txn, &versions)
        .await
        .unwrap();
    crate::store::pinning::legacy_migration::fence::lock_from_routes(&txn, &versions)
        .await
        .unwrap();
    let observed = Observed::new(&txn, vec![]);
    let table_count = tables(&versions).len();
    let before = read(&observed, &versions).await.unwrap();
    assert_eq!(observed.queries.swap(0, Ordering::Relaxed), table_count + 2);
    txn.execute_unprepared(
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('old',97,3)",
    )
    .await
    .unwrap();
    let after = read(&observed, &versions).await.unwrap();
    assert_eq!(observed.queries.swap(0, Ordering::Relaxed), table_count + 2);
    assert_eq!(after, read(&txn, &versions).await.unwrap());
    assert_ne!(before, after);
    let usage = after
        .iter()
        .find(|image| image.table == "pin_provider_usage")
        .unwrap();
    assert_eq!(usage.rows.len(), 1);
    assert_eq!(
        super::super::model::field(&usage.rows[0], "reserved_bytes"),
        Some("97")
    );
    txn.execute_unprepared("ALTER TABLE pin_resource_history ADD COLUMN extra_evidence TEXT")
        .await
        .unwrap();
    assert!(read(&observed, &versions).await.is_err());
    txn.rollback().await.unwrap();
}

#[test]
fn normalization_preserves_composite_native_key_order_and_full_rows() {
    let row = |provider: Option<&str>, cid: Option<Option<&str>>, evidence: &str| {
        let mut row = super::super::NativeRow::from([
            ("provider".into(), provider.map(str::to_owned)),
            ("evidence".into(), Some(evidence.into())),
        ]);
        if let Some(cid) = cid {
            row.insert("cid".into(), cid.map(str::to_owned));
        }
        row
    };
    let mut images = vec![TableImage {
        table: "native".into(),
        columns: vec!["provider".into(), "cid".into(), "evidence".into()],
        primary_key: vec!["provider".into(), "cid".into()],
        rows: vec![
            row(Some("z"), Some(Some("1")), "z"),
            row(Some("a"), Some(Some("2")), "a2"),
            row(Some("a"), Some(None), "null"),
            row(Some("a"), None, "missing"),
            row(None, Some(Some("1")), "null-provider"),
            row(Some("a"), Some(Some("10")), "a10"),
        ],
    }];
    let mut expected = images[0].rows.clone();
    expected.sort_by_key(|row| {
        images[0]
            .primary_key
            .iter()
            .map(|k| row.get(k).cloned())
            .collect::<Vec<_>>()
    });
    normalize(&mut images);
    assert_eq!(images[0].rows, expected);
}

async fn exercise_payload_budget<C: ConnectionTrait>(db: &C) {
    db.execute_unprepared(
        "CREATE TEMP TABLE r26_reader_budget(id TEXT PRIMARY KEY,payload TEXT,nullable TEXT)",
    )
    .await
    .unwrap();
    let shape = raw("r26_reader_budget", "id,payload,nullable", "id");
    let names = shape.columns.iter().map(String::len).sum::<usize>();
    let prefix = if db.get_database_backend() == DatabaseBackend::Postgres {
        "é💠"
    } else {
        "é💠\0"
    };
    let large = "x".repeat(MAX_BYTES - 2 * (names + 1) - prefix.len());
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO r26_reader_budget(id,payload,nullable) VALUES ('b',$1,''),('a',$2,NULL)",
        [large.clone().into(), prefix.into()],
    ))
    .await
    .unwrap();
    let observed = Observed::new(db, shape.columns.clone());
    let (rows, bytes) = read_rows(&observed, &shape, MAX_ROWS, MAX_BYTES)
        .await
        .unwrap();
    assert_eq!(observed.queries.swap(0, Ordering::Relaxed), 1);
    assert_eq!(bytes, MAX_BYTES);
    assert_eq!(
        observed.returned_payload_bytes.swap(0, Ordering::Relaxed),
        MAX_BYTES - 2 * names
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].try_get::<String>("", "id").unwrap(), "a");
    assert_eq!(rows[0].try_get::<String>("", "payload").unwrap(), prefix);
    assert_eq!(
        rows[0].try_get::<Option<String>>("", "nullable").unwrap(),
        None
    );
    assert_eq!(rows[1].try_get::<String>("", "id").unwrap(), "b");
    assert_eq!(rows[1].try_get::<String>("", "payload").unwrap(), large);
    assert_eq!(
        rows[1].try_get::<Option<String>>("", "nullable").unwrap(),
        Some(String::new())
    );
    drop(rows);

    // Both cells individually fit; their complete table image exceeds the budget.
    db.execute_unprepared("UPDATE r26_reader_budget SET payload=payload || 'x' WHERE id='a'")
        .await
        .unwrap();
    assert!(
        read_rows(&observed, &shape, MAX_ROWS, MAX_BYTES)
            .await
            .is_err()
    );
    assert_eq!(observed.queries.swap(0, Ordering::Relaxed), 1);
    assert_eq!(
        observed.returned_payload_bytes.swap(0, Ordering::Relaxed),
        0
    );

    db.execute_unprepared("UPDATE r26_reader_budget SET payload=payload || payload WHERE id='b'")
        .await
        .unwrap();
    assert!(
        read_rows(&observed, &shape, MAX_ROWS, MAX_BYTES)
            .await
            .is_err()
    );
    assert_eq!(observed.queries.swap(0, Ordering::Relaxed), 1);
    assert_eq!(
        observed.returned_payload_bytes.swap(0, Ordering::Relaxed),
        0
    );

    assert!(read_rows(&observed, &shape, 0, MAX_BYTES).await.is_err());
    assert_eq!(
        observed.returned_payload_bytes.swap(0, Ordering::Relaxed),
        0
    );
    db.execute_unprepared("DELETE FROM r26_reader_budget")
        .await
        .unwrap();
    let (rows, bytes) = read_rows(&observed, &shape, 0, 0).await.unwrap();
    assert!(rows.is_empty());
    assert_eq!(bytes, 0);
}

#[tokio::test]
async fn sqlite_payload_budget_rejects_large_cells_and_aggregates_before_returning_text() {
    let db = crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap();
    exercise_payload_budget(&db).await;
}

#[tokio::test]
async fn sqlite_non_utf8_encoding_is_refused_before_inventory_payloads() {
    let db = crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap();
    db.execute_unprepared("PRAGMA encoding='UTF-16le'")
        .await
        .unwrap();
    db.execute_unprepared("CREATE TABLE original(payload TEXT)")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO original VALUES('ࠀé💠')")
        .await
        .unwrap();
    assert!(check_encoding(&db).await.is_err());
    let observed = Observed::new(&db, vec!["payload".into()]);
    assert!(read(&observed, &[]).await.is_err());
    assert_eq!(observed.queries.load(Ordering::Relaxed), 1);
    assert_eq!(observed.returned_payload_bytes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn frozen_import_request_is_in_the_complete_budgeted_snapshot() {
    use crate::{
        import::ImportSource,
        store::{
            self,
            import::jobs::{self, NewImportJob},
        },
    };
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "snapshot-bucket", None)
        .await
        .unwrap();
    let versions = schema(&db, false).await.unwrap();
    for (id, metadata) in [
        ("small", "é💠".to_owned()),
        ("large", "x".repeat(MAX_BYTES)),
    ] {
        let metadata = std::collections::HashMap::from([("original".into(), metadata)]);
        let txn = db.begin().await.unwrap();
        jobs::insert_queued(
            &txn,
            NewImportJob {
                id: id.into(),
                bucket: "snapshot-bucket".into(),
                key: id.into(),
                source: ImportSource::Url("https://example.invalid/source".parse().unwrap()),
                request_fingerprint: id.into(),
                client_token: None,
                object_content_type: None,
                metadata: metadata.clone(),
                tags: vec![],
                decompress_prefix: None,
            },
            store::database_clock::database_now(&txn).await.unwrap(),
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        let observed = Observed::new(&db, vec!["metadata_json".into()]);
        let result = read(&observed, &versions).await;
        if id == "small" {
            let images = result.unwrap();
            let frozen = images
                .iter()
                .find(|image| image.table == "import_job_requests")
                .unwrap();
            assert_eq!(frozen.rows.len(), 1);
            let original: std::collections::HashMap<String, String> = serde_json::from_str(
                super::super::model::field(&frozen.rows[0], "metadata_json").unwrap(),
            )
            .unwrap();
            assert_eq!(original, metadata);
        } else {
            assert!(result.is_err());
            assert_eq!(observed.returned_payload_bytes.load(Ordering::Relaxed), 0);
        }
    }
}

#[tokio::test]
async fn concurrent_writer_does_not_change_the_reader_snapshot_or_its_budget() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("snapshot.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let reader = crate::store::connect_database(&url).await.unwrap();
    reader
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let writer = crate::store::connect_database(&url).await.unwrap();
    reader
        .execute_unprepared(
            "CREATE TABLE r26_reader_budget(id TEXT PRIMARY KEY,payload TEXT,nullable TEXT)",
        )
        .await
        .unwrap();
    reader
        .execute_unprepared("INSERT INTO r26_reader_budget(id,payload) VALUES ('a','é💠')")
        .await
        .unwrap();
    let shape = raw("r26_reader_budget", "id,payload,nullable", "id");
    let txn = reader.begin().await.unwrap();
    let (before, before_bytes) = read_rows(&txn, &shape, MAX_ROWS, MAX_BYTES).await.unwrap();
    writer
        .execute_unprepared(&format!(
            "UPDATE r26_reader_budget SET payload=CAST(zeroblob({}) AS TEXT)",
            MAX_BYTES + 1
        ))
        .await
        .unwrap();
    let (after, after_bytes) = read_rows(&txn, &shape, MAX_ROWS, MAX_BYTES).await.unwrap();
    assert_eq!(after_bytes, before_bytes);
    assert_eq!(before[0].try_get::<String>("", "payload").unwrap(), "é💠");
    assert_eq!(after[0].try_get::<String>("", "payload").unwrap(), "é💠");
    txn.commit().await.unwrap();
    let observed = Observed::new(&reader, shape.columns.clone());
    assert!(
        read_rows(&observed, &shape, MAX_ROWS, MAX_BYTES)
            .await
            .is_err()
    );
    assert_eq!(observed.returned_payload_bytes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
#[ignore = "parent owned PostgreSQL fixture required"]
async fn postgres_payload_budget_preserves_native_text_and_refuses_oversized_payloads() {
    assert_eq!(
        std::env::var("IPFS_S3_R26_PG_OWNED").as_deref(),
        Ok("legacy-r26-fixture-only")
    );
    let url =
        std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect("authorized isolated PG URL required");
    let mut options = sea_orm::ConnectOptions::new(url);
    options.max_connections(1).min_connections(1);
    let db = sea_orm::Database::connect(options).await.unwrap();
    db.execute_unprepared("SET TIME ZONE 'UTC'").await.unwrap();
    db.execute_unprepared("SET statement_timeout='8s'")
        .await
        .unwrap();
    exercise_payload_budget(&db).await;
    db.execute_unprepared("CREATE TEMP TABLE r26_reader_scalars(id BIGINT PRIMARY KEY,amount NUMERIC,observed_at TIMESTAMPTZ)").await.unwrap();
    db.execute_unprepared("INSERT INTO r26_reader_scalars VALUES (10,123456789.123456789,'2026-10-07T00:00:00.123456Z')").await.unwrap();
    let original = db.query_one(Statement::from_string(db.get_database_backend(), "SELECT amount::text AS amount,observed_at::text AS observed_at FROM r26_reader_scalars")).await.unwrap().unwrap();
    let shape = raw("r26_reader_scalars", "id,amount,observed_at", "id");
    let (rows, _) = read_rows(&db, &shape, MAX_ROWS, MAX_BYTES).await.unwrap();
    for column in ["amount", "observed_at"] {
        assert_eq!(
            rows[0].try_get::<String>("", column).unwrap(),
            original.try_get::<String>("", column).unwrap()
        );
    }
    db.close().await.unwrap();
}
