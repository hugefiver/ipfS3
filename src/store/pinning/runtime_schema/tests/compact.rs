use super::*;

async fn equivalent(tx: &RuntimeTransaction, expected: bool) {
    let generic = management::schema::read_versions(tx.db()).await;
    let work = SharedDecodeBudget::default();
    let compact = tx.schema_versions(DecodeWork::shared(&work)).await;
    assert_eq!(generic.is_ok(), expected, "generic: {generic:?}");
    assert_eq!(compact.is_ok(), expected, "compact: {compact:?}");
    if let (Ok(generic), Ok(compact)) = (generic, compact) {
        assert_eq!(compact, generic);
    }
}

async fn loose_versions(tx: &RuntimeTransaction, collation: &str) {
    tx.db()
        .execute_unprepared(&format!(
            "ALTER TABLE seaql_migrations RENAME TO saved_versions; \
         CREATE TABLE seaql_migrations(version COLLATE {collation},applied_at); \
         INSERT INTO seaql_migrations SELECT version,applied_at FROM saved_versions"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn compact_versions_match_full_ordered_decoder_for_live_corruption() {
    let store = fixture().await;
    qualify(&store).await.unwrap();
    let middle = "(SELECT version FROM saved_versions ORDER BY version LIMIT 1 OFFSET 10)";
    let mutations = [
        format!("DELETE FROM seaql_migrations WHERE version={middle}"),
        format!("UPDATE seaql_migrations SET version='unknown' WHERE version={middle}"),
        "INSERT INTO seaql_migrations VALUES('unknown',0)".into(),
        format!("UPDATE seaql_migrations SET version=(SELECT min(version) FROM saved_versions) WHERE version={middle}"),
        format!("UPDATE seaql_migrations SET version=NULL WHERE version={middle}"),
        format!("UPDATE seaql_migrations SET version=7 WHERE version={middle}"),
        format!("UPDATE seaql_migrations SET version=0.5 WHERE version={middle}"),
        format!("UPDATE seaql_migrations SET version=CAST(version AS BLOB) WHERE version={middle}"),
        format!("UPDATE seaql_migrations SET version=CAST(X'FF' AS TEXT) WHERE version={middle}"),
        format!("UPDATE seaql_migrations SET version=version||hex(zeroblob(1000)) WHERE version={middle}"),
        format!("UPDATE seaql_migrations SET version=upper(version) WHERE version={middle}"),
        format!("UPDATE seaql_migrations SET version=version||' ' WHERE version={middle}"),
        "DELETE FROM seaql_migrations".into(),
        "INSERT INTO seaql_migrations SELECT 'extra-'||value,0 FROM json_each('[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30]')".into(),
    ];
    for mutation in mutations {
        let work = SharedDecodeBudget::default();
        let tx = store
            .begin_runtime(DecodeWork::shared(&work))
            .await
            .unwrap();
        loose_versions(&tx, "BINARY").await;
        equivalent(&tx, true).await;
        tx.db().execute_unprepared(&mutation).await.unwrap();
        equivalent(&tx, false).await;
        tx.rollback().await.unwrap();
    }
    let work = SharedDecodeBudget::default();
    let tx = store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    equivalent(&tx, true).await;
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn source_column_custom_reverse_order_is_not_treated_as_a_set() {
    let store = custom_collation("runtime_reverse", |left, right| right.cmp(left)).await;
    qualify(&store).await.unwrap();
    let work = SharedDecodeBudget::default();
    let tx = store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    loose_versions(&tx, "runtime_reverse").await;
    equivalent(&tx, false).await;
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn overridden_binary_collation_cannot_make_version_bytes_equal() {
    let store = custom_collation("BINARY", |left, right| {
        left.to_ascii_lowercase().cmp(&right.to_ascii_lowercase())
    })
    .await;
    let equality = store
        .db()
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT 'a'='A' COLLATE BINARY AS equal",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(equality.try_get::<i64>("", "equal").unwrap(), 1);
    qualify(&store).await.unwrap();
    let work = SharedDecodeBudget::default();
    let tx = store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    equivalent(&tx, true).await;
    tx.db()
        .execute_unprepared(
            "UPDATE seaql_migrations SET version=upper(version) \
         WHERE version=(SELECT version FROM seaql_migrations ORDER BY version LIMIT 1 OFFSET 10)",
        )
        .await
        .unwrap();
    equivalent(&tx, false).await;
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn live_case_short_circuits_failing_main_control_expressions() {
    let store = fixture().await;
    qualify(&store).await.unwrap();
    for shadow in [false, true] {
        let work = SharedDecodeBudget::default();
        let tx = store
            .begin_runtime(DecodeWork::shared(&work))
            .await
            .unwrap();
        tx.db()
            .execute_unprepared(
                "ALTER TABLE seaql_migrations RENAME TO saved_versions; \
             CREATE VIEW seaql_migrations AS SELECT json_extract('invalid','$') AS version",
            )
            .await
            .unwrap();
        if shadow {
            tx.db()
                .execute_unprepared(
                    "ALTER TABLE pin_management_readiness RENAME TO saved_readiness; \
                 CREATE VIEW pin_management_readiness AS SELECT json_extract('invalid','$') AS id, \
                 'S75A1' AS format,'ready' AS state; \
                 CREATE TEMP VIEW ObJeCtS AS SELECT json_extract('invalid','$') AS invalid",
                )
                .await
                .unwrap();
        } else {
            tx.db()
                .execute_unprepared(
                    "UPDATE pin_management_readiness SET state='backfill' WHERE id=1",
                )
                .await
                .unwrap();
        }
        let before = work.lock().unwrap().storage_usage();
        assert!(matches!(
            tx.check_schema(DecodeWork::shared(&work)).await,
            Err(AppError::InvalidPinningRequest(_))
        ));
        let after = work.lock().unwrap().storage_usage();
        assert_eq!(after.sql - before.sql, 1);
        assert_eq!(after.rows - before.rows, 1);
        tx.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn warm_preparation_failure_is_charged_and_cannot_publish_versions() {
    let store = fixture().await;
    qualify(&store).await.unwrap();
    let work = SharedDecodeBudget::default();
    let tx = store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    tx.db()
        .execute_unprepared(
            "ALTER TABLE seaql_migrations RENAME TO saved_versions; \
         CREATE VIEW seaql_migrations AS SELECT 1 AS absent_version",
        )
        .await
        .unwrap();
    let before = work.lock().unwrap().storage_usage();
    assert!(tx.schema_versions(DecodeWork::shared(&work)).await.is_err());
    let after = work.lock().unwrap().storage_usage();
    assert_eq!(after.sql - before.sql, 1);
    assert_eq!(after.rows - before.rows, 0);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn warm_guard_obeys_row_budget_and_charges_the_returned_failure() {
    let store = fixture().await;
    qualify(&store).await.unwrap();
    let work = std::sync::Arc::new(std::sync::Mutex::new(
        management::records::ReadSession::for_task(
            native::meter::Budget::new(native::meter::Limits {
                rows: 1,
                ..Default::default()
            }),
            u64::MAX,
            u64::MAX,
            management::transaction::CancellationToken::new(),
        ),
    ));
    let tx = store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    assert!(matches!(
        tx.check_schema(DecodeWork::shared(&work)).await,
        Err(AppError::ManagementBudget)
    ));
    let used = work.lock().unwrap().storage_usage();
    assert_eq!(used.rows, 2);
    assert_eq!(used.cells, 4);
    assert_eq!(used.raw_bytes, 32);
    tx.rollback().await.unwrap();
}
