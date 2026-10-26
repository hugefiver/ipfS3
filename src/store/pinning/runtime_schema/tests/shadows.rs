use super::*;
use crate::store::pinning::ledger;

#[tokio::test]
async fn custom_nocase_cannot_hide_mixed_case_shadows_on_a_warm_clone() {
    let store = custom_collation("NOCASE", str::cmp).await;
    let equality = store
        .db()
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT 'a'='A' COLLATE NOCASE AS equal",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(equality.try_get::<i64>("", "equal").unwrap(), 0);
    qualify(&store).await.unwrap();
    let clone = store.clone();
    for (kind, name) in [("TABLE", "PiN_PrOvIdEr_RoUtEs"), ("VIEW", "ObJeCtS")] {
        let work = SharedDecodeBudget::default();
        let tx = clone
            .begin_runtime(DecodeWork::shared(&work))
            .await
            .unwrap();
        let definition = if kind == "TABLE" {
            "(invalid TEXT)"
        } else {
            "AS SELECT json_extract('invalid','$') AS invalid"
        };
        tx.db()
            .execute_unprepared(&format!("CREATE TEMP {kind} {name} {definition}"))
            .await
            .unwrap();
        assert!(matches!(
            tx.check_schema(DecodeWork::shared(&work)).await,
            Err(AppError::InvalidPinningRequest(_))
        ));
        assert!(matches!(
            ledger::prepare_runtime_unpin_effect(tx, "provider", "cid", DecodeWork::shared(&work))
                .await,
            Err(AppError::InvalidPinningRequest(_))
        ));

        // The failed operation rolls its TEMP DDL back and releases checkout.
        let work = SharedDecodeBudget::default();
        let tx = clone
            .begin_runtime(DecodeWork::shared(&work))
            .await
            .unwrap();
        tx.db()
            .execute_unprepared("CREATE TEMP TABLE unrelated(value TEXT)")
            .await
            .unwrap();
        tx.check_schema(DecodeWork::shared(&work)).await.unwrap();
        let (tx, _) =
            ledger::prepare_runtime_unpin_effect(tx, "provider", "cid", DecodeWork::shared(&work))
                .await
                .unwrap();
        tx.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn registered_binary_cannot_change_shadow_identifier_byte_comparison() {
    let store = custom_collation("BINARY", |left, right| {
        left.to_ascii_lowercase().cmp(&right.to_ascii_lowercase())
    })
    .await;
    qualify(&store).await.unwrap();
    let work = SharedDecodeBudget::default();
    let tx = store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    tx.db()
        .execute_unprepared("CREATE TEMP TABLE unrelated(value TEXT)")
        .await
        .unwrap();
    tx.check_schema(DecodeWork::shared(&work)).await.unwrap();
    tx.db()
        .execute_unprepared("CREATE TEMP VIEW ObJeCtS AS SELECT 1 AS invalid")
        .await
        .unwrap();
    assert!(matches!(
        tx.check_schema(DecodeWork::shared(&work)).await,
        Err(AppError::InvalidPinningRequest(_))
    ));
    tx.db()
        .execute_unprepared("DROP VIEW temp.ObJeCtS")
        .await
        .unwrap();
    tx.check_schema(DecodeWork::shared(&work)).await.unwrap();
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn custom_nocase_rejects_cold_shadows_and_removal_restores_qualification() {
    for ddl in [
        "CREATE TEMP TABLE PiN_PrOvIdEr_RoUtEs(invalid TEXT)",
        "CREATE TEMP VIEW ObJeCtS AS SELECT json_extract('invalid','$') AS invalid",
    ] {
        let store = custom_collation("NOCASE", str::cmp).await;
        store.db().execute_unprepared(ddl).await.unwrap();
        assert!(matches!(
            qualify(&store).await,
            Err(AppError::InvalidPinningRequest(_))
        ));
        let drop = if ddl.contains("TABLE") {
            "DROP TABLE temp.PiN_PrOvIdEr_RoUtEs"
        } else {
            "DROP VIEW temp.ObJeCtS"
        };
        store.db().execute_unprepared(drop).await.unwrap();
        store
            .db()
            .execute_unprepared("CREATE TEMP TABLE unrelated(value TEXT)")
            .await
            .unwrap();
        qualify(&store).await.unwrap();
    }
}

#[tokio::test]
async fn mixed_case_shadow_short_circuits_main_controls_with_custom_nocase() {
    let store = custom_collation("NOCASE", str::cmp).await;
    qualify(&store).await.unwrap();
    let work = SharedDecodeBudget::default();
    let tx = store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    tx.db()
        .execute_unprepared(
            "ALTER TABLE seaql_migrations RENAME TO saved_versions; \
         CREATE VIEW seaql_migrations AS SELECT json_extract('invalid','$') AS version; \
         ALTER TABLE pin_management_readiness RENAME TO saved_readiness; \
         CREATE VIEW pin_management_readiness AS SELECT json_extract('invalid','$') AS id, \
         'S75A1' AS format,'ready' AS state; \
         CREATE TEMP VIEW ObJeCtS AS SELECT json_extract('invalid','$') AS invalid",
        )
        .await
        .unwrap();
    assert!(matches!(
        tx.check_schema(DecodeWork::shared(&work)).await,
        Err(AppError::InvalidPinningRequest(_))
    ));
    tx.rollback().await.unwrap();
}
