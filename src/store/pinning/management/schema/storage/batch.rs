//! Catalog reads scoped to one schema validation.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

mod postgres;
mod postgres_tables;
mod sqlite_expected;
pub(super) use postgres::validate as postgres;
pub(super) use postgres::validate_bounds as postgres_bounds;
pub(super) use postgres_tables::validate as postgres_tables;

// Bounds apply before catalog text crosses the database connection. Neither is
// derived from a data decode budget or retained beyond this validation.
pub(super) const SQL_BYTES: usize = 64 * 1024;
const NAME_BYTES: usize = 256;

fn parameters(count: usize) -> String {
    (1..=count)
        .map(|n| format!("${n}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) async fn reject_shadows<C: ConnectionTrait>(
    db: &C,
    tables: &[(&str, String)],
    records: bool,
) -> Result<()> {
    let mut names = tables.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    if records {
        names.extend([RECORDS, RETIREMENTS, "seaql_migrations"]);
    }
    let backend = db.get_database_backend();
    let source = if backend == DatabaseBackend::Sqlite {
        "sqlite_temp_schema WHERE type IN ('table','view') AND name"
    } else {
        "pg_class WHERE relnamespace=pg_my_temp_schema() AND relname"
    };
    if db
        .query_one(Statement::from_sql_and_values(
            backend,
            format!(
                "SELECT 1 FROM {source} IN ({}) LIMIT 1",
                parameters(names.len())
            ),
            names.into_iter().map(sea_orm::Value::from),
        ))
        .await
        .map_err(|_| ManagementError::Schema)?
        .is_some()
    {
        return Err(ManagementError::Schema);
    }
    Ok(())
}

pub(super) async fn sqlite<C: ConnectionTrait>(
    db: &C,
    records: bool,
    expected_state: &str,
) -> Result<()> {
    let backend = DatabaseBackend::Sqlite;
    let catalog = sqlite_expected::expected(records);
    let rows = db
        .query_all(Statement::from_sql_and_values(
            backend,
            catalog.query.clone(),
            [expected_state.into()],
        ))
        .await
        .map_err(|_| ManagementError::Schema)?;
    sqlite_rows(&rows, catalog)
}

fn sqlite_rows(
    rows: &[sea_orm::QueryResult],
    catalog: &sqlite_expected::SqliteCatalog,
) -> Result<()> {
    let expected = &catalog.objects;
    let mut control_seen = false;
    let mut catalog_count = 0;
    let mut seen = BTreeSet::new();
    for row in rows {
        match row
            .try_get::<String>("", "row_kind")
            .map_err(|_| ManagementError::Schema)?
            .as_str()
        {
            "control" => {
                let shadow: i64 = row
                    .try_get("", "shadow")
                    .map_err(|_| ManagementError::Schema)?;
                let ready: i64 = row
                    .try_get("", "ready")
                    .map_err(|_| ManagementError::Schema)?;
                if control_seen || shadow != 0 || ready != 1 {
                    return Err(ManagementError::Schema);
                }
                control_seen = true;
                continue;
            }
            "catalog" => {
                catalog_count += 1;
                if catalog_count > catalog.cap {
                    return Err(ManagementError::Schema);
                }
            }
            _ => return Err(ManagementError::Schema),
        }
        if !row
            .try_get::<bool>("", "bounded")
            .map_err(|_| ManagementError::Schema)?
        {
            return Err(ManagementError::Schema);
        }
        let name: String = row
            .try_get("", "name")
            .map_err(|_| ManagementError::Schema)?;
        let kind: String = row
            .try_get("", "type")
            .map_err(|_| ManagementError::Schema)?;
        let table: String = row
            .try_get("", "tbl_name")
            .map_err(|_| ManagementError::Schema)?;
        let sql: Option<String> = row
            .try_get("", "sql")
            .map_err(|_| ManagementError::Schema)?;
        if let Some(object) = expected.get(&name) {
            if !seen.insert(name) || kind != object.kind || table != object.table {
                return Err(ManagementError::Schema);
            }
            let sql = sql.ok_or(ManagementError::Schema)?;
            if !object.matches(&sql) {
                return Err(ManagementError::Schema);
            }
        } else {
            return Err(ManagementError::Schema);
        }
    }
    if !control_seen {
        return Err(ManagementError::Schema);
    }
    Ok(())
}

pub(in crate::store::pinning) async fn observed_control(
    window: &crate::store::pinning::management::admission::worker_observation::WorkerReadWindow,
) -> Result<Vec<String>> {
    window_state(window.db(), window.versions()).await
}

pub(in crate::store::pinning) async fn quota_control(
    window: &crate::store::pinning::quota::production::QuotaReadWindow<'_>,
) -> Result<Vec<String>> {
    window_state(window.db(), window.versions()).await
}

async fn window_state<C: ConnectionTrait>(
    db: &C,
    qualified_versions: &[String],
) -> Result<Vec<String>> {
    let catalog = sqlite_expected::expected(true);
    let bound = qualified_versions
        .iter()
        .map(String::len)
        .max()
        .unwrap_or(0);
    let sql = format!(
        "{} UNION ALL SELECT 'version',CASE WHEN length(CAST(version AS BLOB))<={bound} THEN version END,NULL,NULL,NULL,NULL,NULL,NULL FROM (SELECT version FROM seaql_migrations ORDER BY version LIMIT 65) ORDER BY row_kind,name",
        catalog.control_query
    );
    let rows = db
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            sql,
            ["ready".into()],
        ))
        .await
        .map_err(|_| ManagementError::Schema)?;
    let mut versions = Vec::new();
    let mut controls = Vec::new();
    for row in rows {
        if row
            .try_get::<String>("", "row_kind")
            .map_err(|_| ManagementError::Schema)?
            == "version"
        {
            versions.push(
                row.try_get::<String>("", "name")
                    .map_err(|_| ManagementError::Schema)?,
            );
        } else {
            controls.push(row);
        }
    }
    if versions != qualified_versions {
        return Err(ManagementError::Schema);
    }
    sqlite_rows(&controls, catalog)?;
    Ok(versions)
}

#[cfg(test)]
mod tests {
    use crate::store::{self, pinning::management::schema};
    use sea_orm::{ConnectionTrait, Database, TransactionTrait};

    #[tokio::test]
    async fn sqlite_shadow_skips_runtime_failing_readiness() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        let tx = db.begin().await.unwrap();
        tx.execute_unprepared("CREATE TEMP VIEW pin_management_readiness AS SELECT 1 AS id,'S75A1' AS format,json_extract('not json','$') AS state")
            .await.unwrap();
        let backend = sea_orm::DatabaseBackend::Sqlite;
        assert!(tx.query_all(sea_orm::Statement::from_string(backend,
            "SELECT 1 FROM pin_management_readiness WHERE id=1 AND format='S75A1' AND state='ready'"))
            .await.is_err());
        let catalog = super::sqlite_expected::expected(false);
        for query in [&catalog.query, &catalog.control_query] {
            let rows = tx
                .query_all(sea_orm::Statement::from_sql_and_values(
                    backend,
                    query.clone(),
                    ["ready".into()],
                ))
                .await
                .expect("shadow must short-circuit the runtime-failing readiness view");
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].try_get::<String>("", "row_kind").unwrap(),
                "control"
            );
            assert_eq!(rows[0].try_get::<i64>("", "shadow").unwrap(), 1);
            assert_eq!(rows[0].try_get::<i64>("", "ready").unwrap(), 0);
        }
        assert!(matches!(
            schema::storage::validate(&tx).await,
            Err(crate::store::pinning::management::ManagementError::Schema)
        ));
        tx.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_control_rows_fail_closed() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let catalog = super::sqlite_expected::expected(false);
        for (sql, valid) in [
            ("SELECT 'control' AS row_kind,0 AS shadow,1 AS ready", true),
            (
                "SELECT 'control' AS row_kind,0 AS shadow,1 AS ready WHERE 0",
                false,
            ),
            (
                "SELECT 'control' AS row_kind,0 AS shadow,1 AS ready UNION ALL SELECT 'control',0,1",
                false,
            ),
            ("SELECT 'unknown' AS row_kind,0 AS shadow,1 AS ready", false),
            ("SELECT NULL AS row_kind,0 AS shadow,1 AS ready", false),
            (
                "SELECT 'control' AS row_kind,NULL AS shadow,1 AS ready",
                false,
            ),
            (
                "SELECT 'control' AS row_kind,0 AS shadow,NULL AS ready",
                false,
            ),
            ("SELECT 'control' AS row_kind,2 AS shadow,1 AS ready", false),
            (
                "SELECT 'control' AS row_kind,-1 AS shadow,1 AS ready",
                false,
            ),
            ("SELECT 'control' AS row_kind,0 AS shadow,2 AS ready", false),
            (
                "SELECT 'control' AS row_kind,0 AS shadow,-1 AS ready",
                false,
            ),
            (
                "SELECT 'control' AS row_kind,0.5 AS shadow,1 AS ready",
                false,
            ),
            (
                "SELECT 'control' AS row_kind,0 AS shadow,1.5 AS ready",
                false,
            ),
        ] {
            let rows = db
                .query_all(sea_orm::Statement::from_string(
                    sea_orm::DatabaseBackend::Sqlite,
                    sql,
                ))
                .await
                .unwrap();
            assert_eq!(super::sqlite_rows(&rows, catalog).is_ok(), valid, "{sql}");
        }
    }

    #[tokio::test]
    async fn sqlite_storage_keeps_records_protections_and_backfill_fresh() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        schema::storage::validate(&db).await.unwrap();
        let tx = db.begin().await.unwrap();
        for ddl in [
            "CREATE TEMP VIEW pin_management_records AS SELECT * FROM main.pin_management_records",
            "CREATE TEMP VIEW pin_management_retirements AS SELECT * FROM main.pin_management_retirements",
            "CREATE TEMP VIEW seaql_migrations AS SELECT * FROM main.seaql_migrations",
        ] {
            tx.execute_unprepared(ddl).await.unwrap();
        }
        schema::storage::validate(&tx).await.unwrap();
        assert!(schema::storage::validate_schema(&tx, false).await.is_err());
        tx.rollback().await.unwrap();
        for ddl in [
            "DROP TRIGGER s75_applied_no_delete",
            "DROP INDEX s75_record_kind",
        ] {
            let tx = db.begin().await.unwrap();
            tx.execute_unprepared(ddl).await.unwrap();
            assert!(schema::storage::validate(&tx).await.is_err());
            tx.rollback().await.unwrap();
        }
        db.execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
            .await
            .unwrap();
        assert!(schema::storage::validate(&db).await.is_err());
        schema::storage::validate_state(&db, true).await.unwrap();
        db.execute_unprepared("UPDATE pin_management_readiness SET state='ready' WHERE id=1")
            .await
            .unwrap();
        assert!(schema::storage::validate_state(&db, true).await.is_err());
        schema::storage::validate(&db).await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_catalog_rechecks_mutations_and_temp_shadows() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        schema::validate(&db).await.unwrap();
        for ddl in [
            "CREATE TRIGGER unexpected_record AFTER INSERT ON pin_management_records BEGIN SELECT 1; END",
            "CREATE TRIGGER unexpected_locator AFTER INSERT ON pin_management_locator BEGIN SELECT 1; END",
            "CREATE INDEX unexpected_locator_index ON pin_management_locator(source_key)",
            "CREATE TEMP VIEW pin_management_locator AS SELECT 1 AS source_key",
            "CREATE TEMP VIEW seaql_migrations AS SELECT version FROM main.seaql_migrations",
            "DROP TRIGGER s75_locator_delete",
            "DROP INDEX s75_jobs_resource",
            "DROP TABLE pin_management_pending_scope",
            "UPDATE pin_management_readiness SET state='backfill' WHERE id=1",
        ] {
            let tx = db.begin().await.unwrap();
            tx.execute_unprepared(ddl).await.unwrap();
            assert!(schema::validate(&tx).await.is_err());
            tx.rollback().await.unwrap();
        }
        schema::validate(&db).await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_catalog_rejects_oversized_trigger_definition() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        let tx = db.begin().await.unwrap();
        tx.execute_unprepared("DROP TRIGGER s75_applied_no_delete")
            .await
            .unwrap();
        tx.execute_unprepared(&format!(
            "CREATE TRIGGER s75_applied_no_delete {} BEFORE DELETE ON pin_management_records WHEN OLD.state='applied' BEGIN SELECT RAISE(ABORT,'immutable applied record'); END",
            " ".repeat(128 * 1024)
        ))
        .await
        .unwrap();
        assert!(schema::validate(&tx).await.is_err());
        tx.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_catalog_rechecks_definitions_on_independent_connections() {
        let first = Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&first).await.unwrap();
        schema::storage::validate(&first).await.unwrap();
        schema::validate(&first).await.unwrap();
        let second = Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&second).await.unwrap();
        for (state, valid) in [("applied", true), ("APPLIED", false)] {
            let tx = second.begin().await.unwrap();
            tx.execute_unprepared("DROP TRIGGER s75_applied_no_delete")
                .await
                .unwrap();
            tx.execute_unprepared(&format!(
                "CREATE TRIGGER s75_applied_no_delete before delete ON pin_management_records\nWHEN OLD.state='{state}' BEGIN SELECT RAISE(ABORT,'immutable applied record'); END"
            )).await.unwrap();
            assert_eq!(schema::storage::validate(&tx).await.is_ok(), valid);
            assert_eq!(schema::validate(&tx).await.is_ok(), valid);
            tx.rollback().await.unwrap();
        }
        schema::validate(&first).await.unwrap();
    }
}
