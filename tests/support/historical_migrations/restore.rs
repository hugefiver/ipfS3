//! Rebuild a test database from independently executed historical DDL and SQL cells.
use super::Historical;
use crate::store::migrations::history;
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, DatabaseTransaction, Statement};
use sea_orm_migration::MigratorTrait;

pub async fn replace_sqlite_layout(tx: &DatabaseTransaction, endpoint: &str) {
    assert_eq!(tx.get_database_backend(), DatabaseBackend::Sqlite);
    let reference = Database::connect("sqlite::memory:").await.unwrap();
    Historical::up(
        &reference,
        Some(history::prefix(endpoint).unwrap().len() as u32),
    )
    .await
    .unwrap();
    let objects=reference.query_all(Statement::from_string(DatabaseBackend::Sqlite,
        "SELECT type,name,sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 ELSE 2 END,rowid")).await.unwrap();
    let mut inserts = Vec::new();
    for object in &objects {
        if object.try_get::<String>("", "type").unwrap() != "table" {
            continue;
        }
        let table: String = object.try_get("", "name").unwrap();
        let columns = reference
            .query_all(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT name FROM pragma_table_info($1) ORDER BY cid",
                [table.clone().into()],
            ))
            .await
            .unwrap()
            .into_iter()
            .map(|r| format!("\"{}\"", r.try_get::<String>("", "name").unwrap()))
            .collect::<Vec<_>>();
        let cells = columns
            .iter()
            .map(|c| format!("quote({c})"))
            .collect::<Vec<_>>()
            .join(" || ',' || ");
        let sql = format!(
            "SELECT 'INSERT INTO \"{table}\" ({}) VALUES (' || {cells} || ')' AS sql FROM \"{table}\"",
            columns.join(",")
        );
        let source: &dyn ConnectionTrait = if table == "seaql_migrations" {
            &reference
        } else {
            tx
        };
        inserts.extend(
            source
                .query_all(Statement::from_string(DatabaseBackend::Sqlite, sql))
                .await
                .unwrap()
                .into_iter()
                .map(|r| r.try_get::<String>("", "sql").unwrap()),
        );
    }
    let current=tx.query_all(Statement::from_string(DatabaseBackend::Sqlite,"SELECT type,name FROM sqlite_schema WHERE type IN ('table','trigger') AND name NOT LIKE 'sqlite_%' ORDER BY CASE type WHEN 'trigger' THEN 0 ELSE 1 END,rowid DESC")).await.unwrap();
    tx.execute_unprepared("PRAGMA defer_foreign_keys=ON")
        .await
        .unwrap();
    for row in current {
        tx.execute_unprepared(&format!(
            "DROP {} \"{}\"",
            row.try_get::<String>("", "type").unwrap(),
            row.try_get::<String>("", "name").unwrap()
        ))
        .await
        .unwrap();
    }
    for object in objects
        .iter()
        .filter(|r| r.try_get::<String>("", "type").unwrap() != "trigger")
    {
        tx.execute_unprepared(&object.try_get::<String>("", "sql").unwrap())
            .await
            .unwrap();
    }
    for insert in inserts {
        tx.execute_unprepared(&insert).await.unwrap();
    }
    for object in objects
        .iter()
        .filter(|r| r.try_get::<String>("", "type").unwrap() == "trigger")
    {
        tx.execute_unprepared(&object.try_get::<String>("", "sql").unwrap())
            .await
            .unwrap();
    }
    assert!(
        tx.query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA foreign_key_check"
        ))
        .await
        .unwrap()
        .is_empty()
    );
    reference.close().await.unwrap();
}
