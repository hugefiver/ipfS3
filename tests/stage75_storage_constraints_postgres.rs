//! Transactional damage probes against a parent-owned, initialized exclusive fixture.
use ipfs_s3_gateway::store::pinning::management::schema;
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, Statement, TransactionTrait,
};

#[tokio::test]
#[ignore = "requires initialized exclusive IPFS_S3_STAGE75_STORAGE_DATABASE_URL"]
async fn postgres_storage_constraints_are_required_and_exact() {
    let url = std::env::var("IPFS_S3_STAGE75_STORAGE_DATABASE_URL")
        .expect("parent must supply an initialized exclusive Stage7 PostgreSQL fixture");
    let parsed = url::Url::parse(&url).unwrap();
    assert!(matches!(parsed.scheme(), "postgres" | "postgresql"));
    let mut options = ConnectOptions::new(url);
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    let db = Database::connect(options).await.unwrap();
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT current_schema()::text AS name",
        ))
        .await
        .unwrap()
        .unwrap();
    assert!(
        row.try_get::<String>("", "name")
            .unwrap()
            .starts_with("stage7_")
    );
    let claimed: bool = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT pg_try_advisory_lock(hashtextextended(current_database() || ':' || current_schema(), 0)) AS claimed"))
        .await.unwrap().unwrap().try_get("", "claimed").unwrap();
    assert!(claimed, "fixture must be exclusively owned");
    // The public validator requires the existing readiness row and complete schema.
    schema::validate(&db).await.unwrap();

    let mut damage = Vec::new();
    damage.push("SET LOCAL session_replication_role='replica'".into());
    let enforcement = db.query_all(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT format('%I.%I',tn.nspname,tc.relname) AS relation, quote_ident(t.tgname) AS trigger_name FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_trigger t ON t.tgconstraint=k.oid JOIN pg_class tc ON tc.oid=t.tgrelid JOIN pg_namespace tn ON tn.oid=tc.relnamespace WHERE n.nspname=current_schema() AND k.contype='f' AND c.relname IN ('pin_management_locator','pin_management_archive_chunks') ORDER BY k.oid,t.oid"))
        .await.unwrap();
    assert_eq!(enforcement.len(), 8);
    for trigger in enforcement {
        let relation: String = trigger.try_get("", "relation").unwrap();
        let name: String = trigger.try_get("", "trigger_name").unwrap();
        damage.push(format!("ALTER TABLE {relation} DISABLE TRIGGER {name}"));
    }
    // These preserve the FK definitions, including on the referenced side.
    for table in [
        "pin_management_source_control",
        "pin_management_archive",
        "pin_management_locator",
        "pin_management_archive_chunks",
    ] {
        damage.push(format!("ALTER TABLE {table} DISABLE TRIGGER ALL"));
    }
    for (table, constraints) in [
        (
            "pin_management_readiness",
            &["id_check", "format_check", "state_check"][..],
        ),
        (
            "pin_management_source_control",
            &["source_generation_check", "locator_generation_check"][..],
        ),
        ("pin_management_locator", &["source_key_fkey"][..]),
        ("pin_management_revision", &["revision_check"][..]),
        (
            "pin_management_archive",
            &["format_check", "state_check"][..],
        ),
        (
            "pin_management_archive_chunks",
            &["ordinal_check", "payload_check", "archive_id_fkey"][..],
        ),
    ] {
        for suffix in constraints {
            damage.push(format!(
                "ALTER TABLE {table} DROP CONSTRAINT {table}_{suffix}"
            ));
        }
    }
    let chunks = "ALTER TABLE pin_management_archive_chunks";
    let drop_payload =
        format!("{chunks} DROP CONSTRAINT pin_management_archive_chunks_payload_check;");
    let drop_fk =
        format!("{chunks} DROP CONSTRAINT pin_management_archive_chunks_archive_id_fkey;");
    damage.extend([
        format!("{drop_payload} {chunks} ADD CHECK(length(payload)<=65536) NOT VALID"),
        format!("{drop_payload} {chunks} ADD CHECK(length(payload)<=65537)"),
        format!("{drop_payload} {chunks} ADD CHECK(length(payload)<=65536) NO INHERIT"),
        format!("{drop_fk} {chunks} ADD FOREIGN KEY(archive_id) REFERENCES pin_management_archive(archive_id) NOT VALID"),
        format!("{drop_fk} {chunks} ADD FOREIGN KEY(archive_id) REFERENCES pin_management_archive(archive_id) DEFERRABLE INITIALLY DEFERRED"),
        format!("{drop_fk} {chunks} ADD FOREIGN KEY(archive_id) REFERENCES pin_management_archive(archive_id) ON DELETE CASCADE"),
        format!("{drop_fk} {chunks} ADD FOREIGN KEY(archive_id) REFERENCES pin_management_source_control(source_key) NOT VALID"),
        "ALTER TABLE pin_management_readiness DROP CONSTRAINT pin_management_readiness_format_check; ALTER TABLE pin_management_readiness ADD CHECK(format IN ('S75A1','s75a1'))".into(),
        "ALTER TABLE pin_management_source_control DROP CONSTRAINT pin_management_source_control_source_generation_check; ALTER TABLE pin_management_source_control ADD CHECK(source_generation>=0 AND source_generation<=9223372036854775807)".into(),
        "ALTER TABLE pin_management_pending_scope ADD CHECK(source_generation>=0) NOT VALID".into(),
        "ALTER TABLE pin_management_archive ALTER COLUMN chunks SET DEFAULT 1".into(),
        "ALTER TABLE pin_management_readiness ALTER COLUMN format SET DEFAULT 'S75A1'".into(),
    ]);
    for (table, column) in [
        ("pin_management_source_control", "source_generation"),
        ("pin_management_source_control", "locator_generation"),
        ("pin_management_archive", "chunks"),
        ("pin_management_archive", "bytes"),
    ] {
        damage.push(format!(
            "ALTER TABLE {table} ALTER COLUMN {column} DROP DEFAULT"
        ));
    }
    for sql in damage {
        let tx = db.begin().await.unwrap();
        tx.execute_unprepared("SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='15s'")
            .await
            .unwrap();
        tx.execute_unprepared(&sql).await.unwrap();
        let rejected = schema::validate(&tx).await.is_err();
        tx.rollback().await.unwrap();
        assert!(rejected, "damaged storage contract must be rejected: {sql}");
        schema::validate(&db).await.unwrap();
    }
    db.close().await.unwrap();
}
