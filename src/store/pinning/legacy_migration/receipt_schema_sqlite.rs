use super::invalid;
use crate::{
    error::AppResult,
    store::migrations::m20261007_000001_legacy_pinning_migration::schema as expected,
};
use sea_orm::{ConnectionTrait, QueryResult};

#[path = "receipt_schema_sqlite_read.rs"]
mod read;
#[cfg(test)]
#[path = "receipt_schema_sqlite_tests.rs"]
mod tests;

fn normalize(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(super) async fn validate<C: ConnectionTrait>(db: &C) -> AppResult<()> {
    // Each branch is bounded before UNION ALL: at most 52 metadata rows,
    // followed by at most 16 key rows. Neither bound caps DDL bytes or SQLite work.
    let rows = db
        .query_all(read::metadata_statement(db.get_database_backend()))
        .await?;
    let indexes = validate_metadata(&rows)?;
    let rows = db
        .query_all(read::keys_statement(db.get_database_backend(), &indexes))
        .await?;
    validate_keys(&rows, &indexes)
}

struct RegularIndex {
    table: &'static str,
    name: String,
}

fn validate_metadata(rows: &[QueryResult]) -> AppResult<Vec<RegularIndex>> {
    let mut tables = [read::Table::default(), read::Table::default()];
    for row in rows {
        let owner: String = row.try_get("", "owner")?;
        let table = match owner.as_str() {
            expected::OPERATIONS => &mut tables[0],
            expected::ITEMS => &mut tables[1],
            _ => return Err(invalid()),
        };
        table.push(row)?;
    }
    let mut indexes = Vec::with_capacity(4);
    for (mut actual, (table, ddl)) in tables.into_iter().zip([
        (expected::OPERATIONS, expected::OPERATIONS_SQL),
        (expected::ITEMS, expected::ITEMS_SQL),
    ]) {
        // Every CHECK, UNIQUE, FK and deferrability clause remains part of the
        // proof. Only whitespace is normalized, not hand-written equivalents.
        if actual.ddl.len() != 1 || normalize(&actual.ddl[0]) != normalize(ddl) {
            return Err(invalid());
        }
        columns(&mut actual.columns, table)?;
        foreign_key(&actual.foreign_keys, table)?;
        triggers(&actual.triggers, table)?;
        indexes.extend(regular_indexes(actual.indexes, &actual.special, table)?);
    }
    Ok(indexes)
}

fn columns(rows: &mut [read::Column], table: &str) -> AppResult<()> {
    let expected = expected::columns(table);
    if rows.len() != expected.len() {
        return Err(invalid());
    }
    rows.sort_by_key(|row| row.cid);
    if rows.windows(2).any(|pair| pair[0].cid == pair[1].cid) {
        return Err(invalid());
    }
    for (row, (name, ty, nonnull, pk)) in rows.iter().zip(expected) {
        if row.name != name
            || row.ty != ty
            || row.nonnull != i32::from(nonnull)
            || row.pk != pk
            || row.default.is_some()
            || row.hidden != 0
        {
            return Err(invalid());
        }
    }
    Ok(())
}

fn regular_indexes(
    rows: Vec<read::Index>,
    special: &[String],
    table: &'static str,
) -> AppResult<Vec<RegularIndex>> {
    if !(2..=3).contains(&rows.len())
        || rows
            .iter()
            .enumerate()
            .any(|(i, row)| rows[..i].iter().any(|other| other.name == row.name))
    {
        return Err(invalid());
    }
    let mut indexes = Vec::with_capacity(2);
    let mut origins = Vec::with_capacity(2);
    let mut has_special = false;
    for row in rows {
        if table == expected::OPERATIONS && row.name == "s75_legacy_reverses" {
            if row.unique != 0 || row.partial != 0 || row.origin != "c" {
                return Err(invalid());
            }
            has_special = true;
        } else {
            if row.unique != 1 || row.partial != 0 {
                return Err(invalid());
            }
            origins.push(row.origin);
            indexes.push(RegularIndex {
                table,
                name: row.name,
            });
        }
    }
    origins.sort();
    if origins != ["pk", "u"] {
        return Err(invalid());
    }
    if has_special {
        let registered = crate::store::pinning::management::schema::storage::indexes()
            .into_iter()
            .find(|(name, _)| *name == "s75_legacy_reverses")
            .ok_or_else(invalid)?;
        if special.len() != 1 || normalize(&special[0]) != normalize(registered.1) {
            return Err(invalid());
        }
    } else if !special.is_empty() {
        return Err(invalid());
    }
    Ok(indexes)
}

fn validate_keys(rows: &[QueryResult], indexes: &[RegularIndex]) -> AppResult<()> {
    let mut groups: Vec<Vec<(i32, String)>> = indexes.iter().map(|_| Vec::new()).collect();
    for row in rows {
        if row.try_get::<String>("", "kind")? != "key" {
            return Err(invalid());
        }
        let owner: String = row.try_get("", "owner")?;
        let name: String = row.try_get("", "index_name")?;
        let group = indexes
            .iter()
            .position(|index| index.table == owner && index.name == name)
            .ok_or_else(invalid)?;
        groups[group].push((row.try_get("", "key_seqno")?, row.try_get("", "key_name")?));
    }
    let mut keys = [Vec::new(), Vec::new()];
    for (mut fields, index) in groups.into_iter().zip(indexes) {
        if fields.is_empty() || fields.len() > 3 {
            return Err(invalid());
        }
        fields.sort_by_key(|(seqno, _)| *seqno);
        if fields.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(invalid());
        }
        let group = usize::from(index.table == expected::ITEMS);
        keys[group].push(fields.into_iter().map(|(_, name)| name).collect::<Vec<_>>());
    }
    for (mut actual, table) in keys
        .into_iter()
        .zip([expected::OPERATIONS, expected::ITEMS])
    {
        let mut expected: Vec<Vec<String>> = expected::unique_keys(table)
            .into_iter()
            .map(|v| v.into_iter().map(str::to_owned).collect())
            .collect();
        actual.sort();
        expected.sort();
        if actual != expected {
            return Err(invalid());
        }
    }
    Ok(())
}

fn foreign_key(rows: &[[String; 6]], table: &str) -> AppResult<()> {
    if rows.len() != 1 {
        return Err(invalid());
    }
    let from = if table == expected::OPERATIONS {
        "reverses"
    } else {
        "operation_id"
    };
    if rows[0]
        != [
            expected::OPERATIONS,
            from,
            "id",
            "NO ACTION",
            "RESTRICT",
            "NONE",
        ]
    {
        return Err(invalid());
    }
    Ok(())
}

fn triggers(rows: &[(String, String)], table: &str) -> AppResult<()> {
    if rows.len() != if table == expected::ITEMS { 3 } else { 2 } {
        return Err(invalid());
    }
    let check = |name: &str, ddl: &str| -> AppResult<()> {
        let mut matches = rows.iter().filter(|(actual, _)| actual == name);
        let (_, actual) = matches.next().ok_or_else(invalid)?;
        if matches.next().is_some() || normalize(actual) != normalize(ddl) {
            return Err(invalid());
        }
        Ok(())
    };
    for operation in ["UPDATE", "DELETE"] {
        check(
            &format!("{table}_no_{}", operation.to_ascii_lowercase()),
            &expected::sqlite_immutable(table, operation),
        )?;
    }
    if table == expected::ITEMS {
        check("pin_legacy_item_bound", expected::SQLITE_BOUND)?;
    }
    Ok(())
}
