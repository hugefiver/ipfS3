use super::invalid;
use crate::{
    error::AppResult,
    store::migrations::m20261007_000001_legacy_pinning_migration::schema as expected,
};
use sea_orm::{ConnectionTrait, Statement};

fn normalize(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(super) async fn validate<C: ConnectionTrait>(db: &C) -> AppResult<()> {
    for (table, ddl) in [
        (expected::OPERATIONS, expected::OPERATIONS_SQL),
        (expected::ITEMS, expected::ITEMS_SQL),
    ] {
        let row = db
            .query_one(Statement::from_sql_and_values(
                db.get_database_backend(),
                "SELECT sql FROM sqlite_schema WHERE type='table' AND name=$1",
                [table.into()],
            ))
            .await?
            .ok_or_else(invalid)?;
        let actual: String = row.try_get("", "sql")?;
        // This includes every CHECK, UNIQUE, FK and deferrability clause. Equivalent
        // hand-written DDL is deliberately unknown, not proof of formal upgrade.
        if normalize(&actual) != normalize(ddl) {
            return Err(invalid());
        }
        columns(db, table).await?;
        indexes(db, table).await?;
        foreign_key(db, table).await?;
        for operation in ["UPDATE", "DELETE"] {
            trigger(
                db,
                &format!("{table}_no_{}", operation.to_ascii_lowercase()),
                table,
                &expected::sqlite_immutable(table, operation),
            )
            .await?;
        }
    }
    trigger(
        db,
        "pin_legacy_item_bound",
        expected::ITEMS,
        expected::SQLITE_BOUND,
    )
    .await
}

async fn columns<C: ConnectionTrait>(db: &C, table: &str) -> AppResult<()> {
    let rows=db.query_all(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT name,type,\"notnull\",dflt_value,pk,hidden FROM pragma_table_xinfo($1) ORDER BY cid LIMIT 12",[table.into()])).await?;
    let expected = expected::columns(table);
    if rows.len() != expected.len() {
        return Err(invalid());
    }
    for (row, (name, ty, nonnull, pk)) in rows.iter().zip(expected) {
        if row.try_get::<String>("", "name")? != name
            || row.try_get::<String>("", "type")? != ty
            || row.try_get::<i32>("", "notnull")? != i32::from(nonnull)
            || row.try_get::<i32>("", "pk")? != pk
            || row.try_get::<Option<String>>("", "dflt_value")?.is_some()
            || row.try_get::<i32>("", "hidden")? != 0
        {
            return Err(invalid());
        }
    }
    Ok(())
}

async fn indexes<C: ConnectionTrait>(db: &C, table: &str) -> AppResult<()> {
    let rows = db
        .query_all(Statement::from_sql_and_values(
            db.get_database_backend(),
            "SELECT name,\"unique\",origin,partial FROM pragma_index_list($1) LIMIT 3",
            [table.into()],
        ))
        .await?;
    if rows.len() != 2 {
        return Err(invalid());
    }
    let mut keys = vec![];
    let mut origins = vec![];
    for row in rows {
        if row.try_get::<i32>("", "unique")? != 1 || row.try_get::<i32>("", "partial")? != 0 {
            return Err(invalid());
        }
        origins.push(row.try_get::<String>("", "origin")?);
        let name: String = row.try_get("", "name")?;
        let fields = db
            .query_all(Statement::from_sql_and_values(
                db.get_database_backend(),
                "SELECT name FROM pragma_index_info($1) ORDER BY seqno LIMIT 4",
                [name.into()],
            ))
            .await?;
        keys.push(
            fields
                .iter()
                .map(|r| r.try_get::<String>("", "name"))
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    keys.sort();
    origins.sort();
    let mut expected: Vec<Vec<String>> = expected::unique_keys(table)
        .into_iter()
        .map(|v| v.into_iter().map(str::to_owned).collect())
        .collect();
    expected.sort();
    if keys != expected || origins != ["pk", "u"] {
        return Err(invalid());
    }
    Ok(())
}

async fn foreign_key<C: ConnectionTrait>(db: &C, table: &str) -> AppResult<()> {
    let rows=db.query_all(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT \"table\",\"from\",\"to\",on_update,on_delete,\"match\" FROM pragma_foreign_key_list($1) LIMIT 2",[table.into()])).await?;
    if rows.len() != 1 {
        return Err(invalid());
    }
    let row = &rows[0];
    let from = if table == expected::OPERATIONS {
        "reverses"
    } else {
        "operation_id"
    };
    for (column, value) in [
        ("table", expected::OPERATIONS),
        ("from", from),
        ("to", "id"),
        ("on_update", "NO ACTION"),
        ("on_delete", "RESTRICT"),
        ("match", "NONE"),
    ] {
        if row.try_get::<String>("", column)? != value {
            return Err(invalid());
        }
    }
    Ok(())
}

async fn trigger<C: ConnectionTrait>(db: &C, name: &str, table: &str, ddl: &str) -> AppResult<()> {
    let row = db
        .query_one(Statement::from_sql_and_values(
            db.get_database_backend(),
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=$1 AND tbl_name=$2",
            [name.into(), table.into()],
        ))
        .await?
        .ok_or_else(invalid)?;
    if normalize(&row.try_get::<String>("", "sql")?) != normalize(ddl) {
        return Err(invalid());
    }
    Ok(())
}
