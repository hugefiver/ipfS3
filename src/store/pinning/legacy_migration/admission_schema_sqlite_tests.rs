use super::*;
use sea_orm::DatabaseConnection;

async fn database() -> DatabaseConnection {
    crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap()
}

async fn projected(db: &DatabaseConnection, sql: &str) -> AppResult<Metadata> {
    decode(
        db.query_all(Statement::from_string(DatabaseBackend::Sqlite, sql))
            .await?,
    )
}

#[tokio::test]
async fn facts_decoder_preserves_versions_and_rejects_malformed_rows() {
    let db = database().await;
    let empty = projected(&db, "SELECT 1 AS kind,NULL AS version,0 AS facts")
        .await
        .unwrap();
    assert!(empty.versions.is_empty());
    assert_facts(&empty.facts, 0);
    let metadata = projected(&db, "SELECT 0 AS kind,'b' AS version,NULL AS facts UNION ALL SELECT 0,'',NULL UNION ALL SELECT 0,'b',NULL UNION ALL SELECT 1,NULL,127").await.unwrap();
    assert_eq!(metadata.versions, ["b", "", "b"]);
    assert!(metadata.facts.management_shadow && metadata.facts.management_objects);
    assert!(metadata.facts.receipt_objects && metadata.facts.route_shadow);
    assert!(
        metadata.facts.route_main
            && metadata.facts.receipt_shadow
            && metadata.facts.receipt_partial
    );
    for sql in [
        "SELECT 1 AS kind,NULL AS version,0 AS facts WHERE 0",
        "SELECT 0 AS kind,'a' AS version,NULL AS facts",
        "SELECT 1 AS kind,NULL AS version,0 AS facts UNION ALL SELECT 1,NULL,0",
        "SELECT 2 AS kind,NULL AS version,0 AS facts",
        "SELECT -1 AS kind,NULL AS version,0 AS facts",
        "SELECT 4294967296 AS kind,'a' AS version,NULL AS facts UNION ALL SELECT 1,NULL,0",
        "SELECT 4294967297 AS kind,NULL AS version,0 AS facts",
        "SELECT 1.0 AS kind,NULL AS version,0 AS facts",
        "SELECT NULL AS kind,NULL AS version,0 AS facts",
        "SELECT '1' AS kind,NULL AS version,0 AS facts",
        "SELECT 1 AS kind,NULL AS version,NULL AS facts",
        "SELECT 1 AS kind,NULL AS version,'1' AS facts",
        "SELECT 1 AS kind,NULL AS version,1.5 AS facts",
        "SELECT 1 AS kind,NULL AS version,-1 AS facts",
        "SELECT 1 AS kind,NULL AS version,128 AS facts",
        "SELECT 1 AS kind,NULL AS version,4294967296 AS facts",
        "SELECT 1 AS kind,'a' AS version,0 AS facts",
        "SELECT 0 AS kind,NULL AS version,NULL AS facts UNION ALL SELECT 1,NULL,0",
        "SELECT 0 AS kind,17 AS version,NULL AS facts UNION ALL SELECT 1,NULL,0",
        "SELECT 0 AS kind,'a' AS version,0 AS facts UNION ALL SELECT 1,NULL,0",
    ] {
        assert!(projected(&db, sql).await.is_err(), "{sql}");
    }
}

#[tokio::test]
async fn facts_follow_real_catalog_predicates_including_overlapping_shadows() {
    let db = database().await;
    db.execute_unprepared("CREATE TABLE seaql_migrations(version TEXT)")
        .await
        .unwrap();
    let mut expected = 0;
    for (ddl, bits) in [
        ("CREATE TEMP TABLE pin_management_records(x)", 1 | 32),
        ("CREATE TEMP TABLE seaql_migrations(version TEXT)", 1 | 8),
        ("CREATE TABLE pin_management_retirements(x)", 2 | 64),
        ("CREATE VIEW pin_legacy_migrations AS SELECT 1", 4),
        ("CREATE TABLE pin_provider_routes(x)", 16),
    ] {
        let before = decode(
            db.query_all(Statement::from_string(DatabaseBackend::Sqlite, METADATA))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_facts(&before.facts, expected);
        db.execute_unprepared(ddl).await.unwrap();
        expected |= bits;
    }
    let after = decode(
        db.query_all(Statement::from_string(DatabaseBackend::Sqlite, METADATA))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_facts(&after.facts, 127);
}

fn assert_facts(facts: &Facts, bits: u8) {
    for (actual, mask) in [
        (facts.management_shadow, 1),
        (facts.management_objects, 2),
        (facts.receipt_objects, 4),
        (facts.route_shadow, 8),
        (facts.route_main, 16),
        (facts.receipt_shadow, 32),
        (facts.receipt_partial, 64),
    ] {
        assert_eq!(actual, bits & mask != 0);
    }
}
