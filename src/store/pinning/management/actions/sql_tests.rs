use super::*;
use sea_orm::{Database, Statement};

#[tokio::test]
async fn exact_native_predicates_bind_composite_integer_keys() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared(
        "CREATE TABLE sample(provider TEXT,cid TEXT,epoch BIGINT,PRIMARY KEY(provider,cid,epoch)); \
         INSERT INTO sample VALUES ('p','c',7),('p','c',8),('other','c',7);",
    )
    .await
    .unwrap();
    let table = native::current_catalog()
        .iter()
        .find(|table| table.table == "pin_resource_history")
        .unwrap();
    let (predicate, values) = super::sql::exact_key(table, &["p", "c", "7"]).unwrap();
    let rows = db
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            format!("SELECT epoch FROM sample WHERE {predicate}"),
            values,
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].try_get::<i64>("", "epoch").unwrap(), 7);
    assert!(super::sql::exact_key(table, &["p", "c", "not-an-integer"]).is_err());
    assert!(super::sql::exact_key(table, &["p", "c"]).is_err());
}
