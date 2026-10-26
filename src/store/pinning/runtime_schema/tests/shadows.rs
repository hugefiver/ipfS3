use super::*;

#[tokio::test]
async fn cold_store_rejects_shadows_independent_of_registered_collations() {
    for collation in ["NOCASE", "BINARY"] {
        for ddl in [
            "CREATE TEMP TABLE PiN_PrOvIdEr_RoUtEs(invalid TEXT)",
            "CREATE TEMP VIEW ObJeCtS AS SELECT json_extract('invalid','$') AS invalid",
            "CREATE TEMP VIEW seaql_migrations AS SELECT 'invalid' AS version",
        ] {
            let store = custom_collation(collation, str::cmp).await;
            store.db().execute_unprepared(ddl).await.unwrap();
            assert!(qualify(&store).await.is_err());
            assert!(store.inner.qualification.get().is_none());
        }
    }
}
