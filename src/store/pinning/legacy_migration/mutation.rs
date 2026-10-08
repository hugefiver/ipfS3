//! Only provider keys, declared missing route, source retirement and usage move.
use super::{
    Declaration, TableImage,
    model::{field, key},
    refused,
};
use crate::error::AppResult;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

pub(super) async fn write_changes<C: ConnectionTrait>(
    db: &C,
    before: &[TableImage],
    after: &[TableImage],
    decl: &Declaration,
    reverse: bool,
) -> AppResult<()> {
    for image in before {
        if !matches!(
            image.table.as_str(),
            "remote_pins"
                | "remote_pin_ledger"
                | "pin_lease_targets"
                | "pin_jobs"
                | "pin_provider_routes"
                | "pin_provider_usage"
        ) {
            continue;
        }
        let destination = after
            .iter()
            .find(|i| i.table == image.table)
            .ok_or_else(|| refused("incomplete post-image"))?;
        if image.table == "pin_provider_usage" {
            usage_changes(db, image, destination).await?;
            continue;
        }
        for row in &image.rows {
            let old_key = key(image, row);
            let mut new_key = old_key.clone();
            let provider = field(row, "provider").unwrap_or("");
            if image.table != "pin_provider_routes" {
                for ns in &decl.namespaces {
                    let (from, to) = if reverse {
                        (&ns.target, &ns.source)
                    } else {
                        (&ns.source, &ns.target)
                    };
                    if provider == from
                        && (!reverse
                            || ns.bindings.iter().any(|b| {
                                b.table == image.table
                                    && b.pointer.is_none()
                                    && b.key
                                        .iter()
                                        .all(|(k, v)| k == "provider" || field(row, k) == Some(v))
                            }))
                        && new_key.contains_key("provider")
                    {
                        new_key.insert("provider".into(), to.clone());
                    }
                }
            }
            let target = destination
                .rows
                .iter()
                .find(|r| key(destination, r) == new_key)
                .ok_or_else(|| refused("operational row correspondence missing"))?;
            let changed: Vec<_> = image
                .columns
                .iter()
                .filter(|c| row.get(*c) != target.get(*c))
                .collect();
            if changed.is_empty() {
                continue;
            }
            if changed
                .iter()
                .any(|c| !matches!(c.as_str(), "provider" | "route" | "retired"))
            {
                return Err(refused("migration attempted non-key/evidence mutation"));
            }
            let mut values = vec![];
            let sets = changed
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    values.push(target.get(*c).cloned().flatten().into());
                    if c.as_str() == "retired" {
                        format!("\"{c}\"=CAST(${} AS BOOLEAN)", i + 1)
                    } else {
                        format!("\"{c}\"=${}", i + 1)
                    }
                })
                .collect::<Vec<_>>()
                .join(",");
            let predicate = old_key
                .iter()
                .map(|(k, v)| {
                    values.push(v.clone().into());
                    format!("\"{k}\"=${}", values.len())
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            let result = db
                .execute(Statement::from_sql_and_values(
                    db.get_database_backend(),
                    format!("UPDATE {} SET {sets} WHERE {predicate}", image.table),
                    values,
                ))
                .await?;
            if result.rows_affected() != 1 {
                return Err(refused("last row CAS failed"));
            }
        }
    }
    Ok(())
}

async fn usage_changes<C: ConnectionTrait>(
    db: &C,
    before: &TableImage,
    after: &TableImage,
) -> AppResult<()> {
    for row in &before.rows {
        let provider = field(row, "provider").ok_or_else(|| refused("usage identity missing"))?;
        if !after
            .rows
            .iter()
            .any(|r| field(r, "provider") == Some(provider))
        {
            let result = db
                .execute(Statement::from_sql_and_values(
                    db.get_database_backend(),
                    "DELETE FROM pin_provider_usage WHERE provider=$1",
                    [provider.into()],
                ))
                .await?;
            if result.rows_affected() != 1 {
                return Err(refused("usage delete CAS failed"));
            }
        }
    }
    for row in &after.rows {
        if before.rows.contains(row) {
            continue;
        }
        let columns = &after.columns;
        let time = if db.get_database_backend() == DatabaseBackend::Postgres {
            "TIMESTAMPTZ"
        } else {
            "TEXT"
        };
        let terms = columns
            .iter()
            .enumerate()
            .map(|(i, c)| match c.as_str() {
                "reserved_bytes" | "reserved_pins" | "observed_bytes" | "observed_pins" => {
                    format!("CAST(${} AS BIGINT)", i + 1)
                }
                "observed_at" => format!("CAST(${} AS {time})", i + 1),
                _ => format!("${}", i + 1),
            })
            .collect::<Vec<_>>()
            .join(",");
        let sets = columns
            .iter()
            .filter(|c| c.as_str() != "provider")
            .map(|c| format!("{c}=excluded.{c}"))
            .collect::<Vec<_>>()
            .join(",");
        let values: Vec<sea_orm::Value> = columns
            .iter()
            .map(|c| row.get(c).cloned().flatten().into())
            .collect();
        db.execute(Statement::from_sql_and_values(db.get_database_backend(),format!("INSERT INTO pin_provider_usage ({}) VALUES ({terms}) ON CONFLICT(provider) DO UPDATE SET {sets}",columns.join(",")),values)).await?;
    }
    Ok(())
}
