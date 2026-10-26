//! Native storage checks use a parent-owned, empty PostgreSQL schema only.
use ipfs_s3_gateway::store::{
    self,
    pinning::management::{
        native::{
            NativeKey, cells,
            meter::Budget,
            scan,
            scope::{self, ScopeSpec, Selector},
        },
        records::segmented::{self, ArchiveHeader},
    },
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
    TransactionTrait,
};
use std::{
    io::{Cursor, Read},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

struct CancelAfterFirstChunk {
    input: Cursor<Vec<u8>>,
    cancelled: Arc<AtomicBool>,
}

#[tokio::test]
#[ignore = "requires parent-selected initialized exclusive Stage7 PostgreSQL schema"]
async fn postgres_immutable_guards_are_validated() {
    use ipfs_s3_gateway::store::pinning::management::schema;
    let url = std::env::var("IPFS_S3_STAGE75_STORAGE_DATABASE_URL").unwrap();
    let db = connection(&url).await;
    let name: String = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT current_schema() AS name",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "name")
        .unwrap();
    assert!(name.starts_with("stage7_"));
    let claimed: bool = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT pg_try_advisory_lock(hashtextextended(current_database() || ':' || current_schema(), 0)) AS claimed"))
        .await.unwrap().unwrap().try_get("", "claimed").unwrap();
    assert!(claimed);
    schema::validate(&db).await.unwrap();
    // A repeated name on a different table is not one of the fourteen guard pairs.
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared("CREATE TRIGGER s75_inline_immutable BEFORE UPDATE ON pin_management_records FOR EACH ROW EXECUTE FUNCTION s75_immutable()")
        .await.unwrap();
    schema::storage::validate(&tx).await.unwrap();
    tx.rollback().await.unwrap();
    let mut damages = [
        "DROP TRIGGER s75_chunk_sealed ON pin_management_archive_chunks",
        "ALTER TABLE pin_management_archive DISABLE TRIGGER s75_archive_no_update",
        "CREATE OR REPLACE FUNCTION s75_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN OLD; END $$",
        "CREATE OR REPLACE FUNCTION s75_chunk_guard() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE status TEXT; BEGIN SELECT state INTO STRICT status FROM pin_management_archive WHERE archive_id=NEW.archive_id FOR SHARE NOWAIT; IF status='SEALED' THEN RAISE EXCEPTION 'immutable archive'; END IF; RETURN NEW; END $$",
        "CREATE OR REPLACE FUNCTION s75_chunk_guard() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE status TEXT; BEGIN SELECT state INTO STRICT status FROM pin_management_archive WHERE archive_id=NEW.archive_id FOR SHARE NOWAIT; IF status='sealed ' THEN RAISE EXCEPTION 'immutable archive'; END IF; RETURN NEW; END $$",
        "DROP TRIGGER s75_applied_no_update ON pin_management_records; CREATE TRIGGER s75_applied_no_update BEFORE UPDATE ON pin_management_records FOR EACH ROW WHEN (OLD.state='APPLIED') EXECUTE FUNCTION s75_immutable()",
        "DROP TRIGGER s75_pin_provider_routes_update ON pin_provider_routes; CREATE TRIGGER s75_pin_provider_routes_update AFTER UPDATE ON pin_provider_routes FOR EACH ROW WHEN (false) EXECUTE FUNCTION s75_pin_provider_routes_update()",
        "DROP TRIGGER s75_pin_provider_routes_update ON pin_provider_routes; CREATE TRIGGER s75_pin_provider_routes_update AFTER UPDATE OF display_name ON pin_provider_routes FOR EACH ROW EXECUTE FUNCTION s75_pin_provider_routes_update()",
        "CREATE TRIGGER unexpected_archive_guard BEFORE UPDATE ON pin_management_archive FOR EACH ROW EXECUTE FUNCTION s75_immutable()",
        "DROP TRIGGER s75_applied_no_delete ON pin_management_records",
        "ALTER TABLE pin_management_records DISABLE TRIGGER s75_applied_no_update",
        "DROP TRIGGER s75_archive_no_delete ON pin_management_archive; CREATE TRIGGER s75_archive_no_delete BEFORE DELETE ON pin_management_pending_scope FOR EACH ROW EXECUTE FUNCTION s75_immutable()",
        "ALTER FUNCTION s75_immutable() SECURITY DEFINER",
        "ALTER FUNCTION s75_immutable() SET search_path TO public",
        "ALTER TABLE pin_management_archive ENABLE ALWAYS TRIGGER s75_archive_no_update",
    ].map(str::to_owned).to_vec();
    damages.extend([
        format!("CREATE OR REPLACE FUNCTION s75_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN /*{}*/ RAISE EXCEPTION 'immutable archive'; END $$", "x".repeat(65537)),
        format!("DROP TRIGGER s75_applied_no_update ON pin_management_records; CREATE TRIGGER s75_applied_no_update BEFORE UPDATE ON pin_management_records FOR EACH ROW WHEN (OLD.state='{}') EXECUTE FUNCTION s75_immutable()", "x".repeat(65537)),
    ]);
    for damage in damages {
        let tx = db.begin().await.unwrap();
        tx.execute_unprepared(&damage).await.unwrap();
        let rejected = schema::validate(&tx).await.is_err();
        tx.rollback().await.unwrap();
        assert!(
            rejected,
            "damaged immutable storage guard must reject authority"
        );
        schema::validate(&db).await.unwrap();
    }
    db.close().await.unwrap();
}

impl Read for CancelAfterFirstChunk {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.input.position() != 0 {
            self.cancelled.store(true, Ordering::Relaxed);
        }
        self.input.read(buffer)
    }
}

async fn connection(url: &str) -> DatabaseConnection {
    let mut options = ConnectOptions::new(url);
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    Database::connect(options).await.unwrap()
}

#[tokio::test]
#[ignore = "requires parent-selected empty exclusive Stage7 PostgreSQL schema"]
async fn postgres_scoped_storage_fences_chunks_and_revision() {
    let url = std::env::var("IPFS_S3_STAGE75_STORAGE_DATABASE_URL")
        .expect("parent must supply an empty exclusive PostgreSQL schema");
    let parsed = url::Url::parse(&url).unwrap();
    assert!(matches!(parsed.scheme(), "postgres" | "postgresql"));
    let db = connection(&url).await;
    let row = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT current_schema() AS name, NOT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace) AND NOT EXISTS (SELECT 1 FROM pg_proc WHERE pronamespace=current_schema()::regnamespace) AS empty"))
        .await.unwrap().unwrap();
    let schema: String = row.try_get("", "name").unwrap();
    assert!(schema.starts_with("stage7_"));
    assert!(
        row.try_get::<bool>("", "empty").unwrap(),
        "occupied schema must be preserved"
    );
    let claimed: bool = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT pg_try_advisory_lock(hashtextextended(current_database() || ':' || current_schema(), 0)) AS claimed"))
        .await.unwrap().unwrap().try_get("", "claimed").unwrap();
    assert!(claimed);
    store::run_migrations(&db).await.unwrap();
    let writer = connection(&url).await;
    writer
        .execute_unprepared("SET lock_timeout = '100ms'")
        .await
        .unwrap();
    writer
        .execute_unprepared("SET statement_timeout = '3s'")
        .await
        .unwrap();

    let spec = ScopeSpec::new(vec![Selector::Route {
        provider: "selected".into(),
    }])
    .unwrap();
    let tx = db.begin().await.unwrap();
    let reviewed = scope::collect_scope(&tx, &spec, &mut Budget::default())
        .await
        .unwrap();
    assert!(reviewed.rows().is_empty());
    tx.commit().await.unwrap();
    writer
        .execute_unprepared(
            "INSERT INTO pin_provider_routes VALUES ('unrelated','{}','private',FALSE)",
        )
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    scope::revalidate_scope(&tx, &reviewed, &mut Budget::default())
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    // An empty membership is protected against insertion until the caller ends
    // its transaction. The identical INSERT succeeds only after rollback.
    let tx = db.begin().await.unwrap();
    scope::fence_memberships(&tx, &spec).await.unwrap();
    let insert = "INSERT INTO pin_provider_routes VALUES ('selected','{  \"private\": null }','private',FALSE)";
    let started = Instant::now();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), writer.execute_unprepared(insert))
            .await
            .expect("server-side lock timeout must finish")
            .is_err()
    );
    eprintln!("pg phantom lock conflict elapsed={:?}", started.elapsed());
    tx.rollback().await.unwrap();
    writer.execute_unprepared(insert).await.unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        scope::revalidate_scope(&tx, &reviewed, &mut Budget::default())
            .await
            .is_err()
    );
    let current = scope::collect_scope(&tx, &spec, &mut Budget::default())
        .await
        .unwrap();
    assert_eq!(
        current.rows().values().next().unwrap()["snapshot"].as_deref(),
        Some("{  \"private\": null }")
    );
    tx.rollback().await.unwrap();

    for mutation in [
        "UPDATE pin_provider_routes SET snapshot='concurrent update' WHERE provider='selected'",
        "DELETE FROM pin_provider_routes WHERE provider='selected'",
    ] {
        let tx = db.begin().await.unwrap();
        scope::fence_memberships(&tx, &spec).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), writer.execute_unprepared(mutation))
                .await
                .expect("update/delete must finish at the server lock timeout")
                .is_err()
        );
        tx.rollback().await.unwrap();
        writer.execute_unprepared(mutation).await.unwrap();
        let tx = db.begin().await.unwrap();
        assert!(
            scope::revalidate_scope(&tx, &current, &mut Budget::default())
                .await
                .is_err(),
            "same-key mutation or deletion must invalidate the reviewed original"
        );
        tx.rollback().await.unwrap();
        writer.execute_unprepared("INSERT INTO pin_provider_routes VALUES ('selected','{  \"private\": null }','private',FALSE) ON CONFLICT (provider) DO UPDATE SET snapshot=EXCLUDED.snapshot").await.unwrap();
        let tx = db.begin().await.unwrap();
        scope::revalidate_scope(&tx, &current, &mut Budget::default())
            .await
            .unwrap();
        tx.rollback().await.unwrap();
    }

    let tx = db.begin().await.unwrap();
    let checkpoint = scan::start_scan(&tx, spec.clone(), &mut Budget::default())
        .await
        .unwrap();
    let page = scan::scan_page(&tx, &checkpoint, &mut Budget::default())
        .await
        .unwrap();
    assert!(page.complete);
    tx.commit().await.unwrap();
    let tx = writer.begin().await.unwrap();
    tx.execute_unprepared(
        "UPDATE pin_provider_routes SET snapshot='rollback' WHERE provider='selected'",
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let tx = db.begin().await.unwrap();
    scan::seal_scan(&tx, &page.checkpoint, &mut Budget::default())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    writer
        .execute_unprepared(
            "UPDATE pin_provider_routes SET snapshot='changed' WHERE provider='selected'",
        )
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        scan::seal_scan(&tx, &page.checkpoint, &mut Budget::default())
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();

    // PostgreSQL text cannot contain NUL, but exact UTF-8 and NULL/empty byte
    // distinctions remain preserved by cell streaming and the opaque archive.
    let original = "私密-original\n".repeat(200_000);
    writer
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE pin_provider_routes SET snapshot=$1 WHERE provider='selected'",
            [original.clone().into()],
        ))
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    let mut output = Vec::new();
    let mut budget = Budget::default();
    let key = NativeKey {
        table: "pin_provider_routes".into(),
        values: vec!["selected".into()],
    };
    assert_eq!(
        cells::copy_cell(&tx, &key, "snapshot", &mut output, &mut budget)
            .await
            .unwrap(),
        Some(original.len() as u64)
    );
    assert_eq!(output, original.as_bytes());
    assert!(budget.peak_resident() <= 128 * 1024);
    tx.rollback().await.unwrap();
    let header = ArchiveHeader {
        database_scope: "original".into(),
        database_identity: "physical".into(),
        principal: "operator".into(),
        catalog: vec!["original".into()],
        request: "{  \"null\":null,\"empty\":\"\" }".into(),
        configuration: "private".into(),
        acknowledgements: vec!["item".into()],
        created_at: "2026-01-01 01:02:03.123456".into(),
        expires_at: None,
    };
    let tx = db.begin().await.unwrap();
    let archive = segmented::write_archive(
        &tx,
        "postgres-private",
        &header,
        &mut original.as_bytes(),
        &mut Budget::default(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let mut restored = Vec::new();
    let restored_header =
        segmented::read_archive(&db, &archive, &mut restored, &mut Budget::default())
            .await
            .unwrap();
    assert!(restored_header == header);
    assert_eq!(restored, original.as_bytes());
    for truncate in [
        "TRUNCATE pin_management_archive_chunks",
        "TRUNCATE pin_management_archive CASCADE",
    ] {
        let tx = writer.begin().await.unwrap();
        let rejected = tx.execute_unprepared(truncate).await.is_err();
        tx.rollback().await.unwrap();
        assert!(rejected, "sealed archive must survive {truncate}");
        let mut after = Vec::new();
        let after_header =
            segmented::read_archive(&db, &archive, &mut after, &mut Budget::default())
                .await
                .unwrap();
        assert!(after_header == header);
        assert_eq!(after, original.as_bytes());
    }
    assert!(
        writer
            .execute_unprepared(
                "DELETE FROM pin_management_archive_chunks WHERE archive_id='postgres-private'"
            )
            .await
            .is_err()
    );
    let tx = db.begin().await.unwrap();
    let mut cancelled_budget = Budget::default();
    let mut input = CancelAfterFirstChunk {
        input: Cursor::new(vec![42; 200_000]),
        cancelled: cancelled_budget.cancellation(),
    };
    assert!(
        segmented::write_archive(
            &tx,
            "cancelled-private",
            &header,
            &mut input,
            &mut cancelled_budget,
        )
        .await
        .is_err()
    );
    let staged: i64 = tx.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT COUNT(*) AS n FROM pin_management_archive_chunks WHERE archive_id='cancelled-private'"))
        .await.unwrap().unwrap().try_get("", "n").unwrap();
    assert_eq!(staged, 1, "exercise cancellation after a real chunk write");
    tx.rollback().await.unwrap();
    let remaining: i64 = writer
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT COUNT(*) AS n FROM pin_management_archive WHERE archive_id='cancelled-private'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(remaining, 0);
    let tx = db.begin().await.unwrap();
    segmented::write_archive(
        &tx,
        "cancelled-private",
        &header,
        &mut original.as_bytes(),
        &mut Budget::default(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    writer.close().await.unwrap();
    db.close().await.unwrap();
}
